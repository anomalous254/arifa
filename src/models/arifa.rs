use super::event::WsMessage;
use crate::metrics::Metrics;
use crate::routing_table::RoutingTable;
use crate::session::WsSession;
use futures_util::StreamExt;
use redis::{AsyncCommands, Client, aio::ConnectionManager};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use uuid::Uuid;

const ONLINE_USERS_KEY: &str = "arifa:online_users";
const USER_SESSIONS_PREFIX: &str = "arifa:user_sessions";
const SESSION_USER_PREFIX: &str = "arifa:session_user";

/// Buffer size for each session's forwarding channel. A slow client can
/// have this many messages queued before new ones start getting dropped
/// (see `RoutingTable::route`) — this bounds per-session memory instead
/// of letting one stuck client grow without limit.
const SESSION_CHANNEL_CAPACITY: usize = 256;

/// Initial and max backoff when the shared Redis pub/sub connection
/// drops and needs to be reestablished.
const RECONNECT_INITIAL_BACKOFF: Duration = Duration::from_millis(200);
const RECONNECT_MAX_BACKOFF: Duration = Duration::from_secs(30);

/// Commands sent from `Arifa::subscribe`/`unsubscribe` to the single
/// background router task that owns the real Redis pub/sub connection.
enum RouterCommand {
    Subscribe {
        channel: String,
        session_id: String,
        sender: mpsc::Sender<WsMessage>,
    },
    Unsubscribe {
        channel: String,
        session_id: String,
    },
}

/// Core realtime pub/sub engine built on Redis.
///
/// A single background "router" task per node owns one shared Redis
/// pub/sub connection; sessions register interest via an in-memory
/// [`RoutingTable`] rather than each opening their own Redis connection.
/// The router automatically reconnects (with backoff) and resubscribes
/// to every channel still in use if the connection drops.
#[derive(Clone)]
pub struct Arifa {
    manager: ConnectionManager,
    node_id: String,
    command_tx: mpsc::UnboundedSender<RouterCommand>,
    /// Every currently-live forwarding task, keyed by session_id, so
    /// `shutdown()` can drain/abort them all without the caller needing
    /// to track handles itself.
    sessions: Arc<Mutex<HashMap<String, JoinHandle<()>>>>,
    shutdown_tx: watch::Sender<bool>,
    pub metrics: Metrics,
}

impl Arifa {
    /// Creates a new `Arifa` instance connected to Redis and spawns the
    /// router task that owns the shared pub/sub connection for this node.
    pub async fn new(
        redis_url: impl AsRef<str>,
        node_id: impl Into<String>,
    ) -> redis::RedisResult<Self> {
        let redis_url = redis_url.as_ref().to_string();
        let client = Client::open(redis_url.as_str())?;
        let manager = client.get_connection_manager().await?;
        let node_id = node_id.into();
        let metrics = Metrics::default();

        let (command_tx, command_rx) = mpsc::unbounded_channel();
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        {
            let router_node_id = node_id.clone();
            let router_metrics = metrics.clone();
            tokio::spawn(async move {
                run_router(
                    redis_url,
                    router_node_id,
                    command_rx,
                    shutdown_rx,
                    router_metrics,
                )
                .await;
            });
        }

        Ok(Self {
            manager,
            node_id,
            command_tx,
            sessions: Arc::new(Mutex::new(HashMap::new())),
            shutdown_tx,
            metrics,
        })
    }

    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    /// Subscribes a session to one or more channels and registers it in
    /// the local session registry (for `shutdown()`).
    ///
    /// Returns the generated session_id. Pass it, plus the same
    /// `channels`, to [`Arifa::unsubscribe`] when the session ends.
    pub fn subscribe<C, S>(
        &self,
        channels: impl IntoIterator<Item = S>,
        session: C,
        user_id: impl Into<String>,
    ) -> String
    where
        C: WsSession + 'static,
        S: Into<String>,
    {
        let channels = channels.into_iter().map(Into::into).collect::<Vec<_>>();
        let session = Arc::new(session);
        let arifa = self.clone();
        let session_id = Uuid::new_v4().to_string();
        let user_id = user_id.into();

        let (tx, mut rx) = mpsc::channel::<WsMessage>(SESSION_CHANNEL_CAPACITY);

        for channel in &channels {
            let _ = self.command_tx.send(RouterCommand::Subscribe {
                channel: channel.clone(),
                session_id: session_id.clone(),
                sender: tx.clone(),
            });
        }

        self.metrics.session_started();

        let task_session_id = session_id.clone();
        let metrics = self.metrics.clone();
        let handle = tokio::spawn(async move {
            if let Err(err) = arifa.add_online_user(&task_session_id, &user_id).await {
                eprintln!("Failed to mark session '{task_session_id}' online: {err}");
            }

            while let Some(event) = rx.recv().await {
                if session.send(event).await.is_err() {
                    break;
                }
            }

            metrics.session_ended();
            println!("Forwarder for session '{task_session_id}' stopped.");
        });

        self.sessions
            .lock()
            .unwrap()
            .insert(session_id.clone(), handle);

        session_id
    }

    /// Tears down a session: tells the router to drop it from each
    /// channel's routing table, removes it from the local session
    /// registry, and aborts the forwarding task.
    /// and subs 1 from the tracked metrics sessions
    pub fn unsubscribe(&self, session_id: &str, channels: &[String]) {
        for channel in channels {
            let _ = self.command_tx.send(RouterCommand::Unsubscribe {
                channel: channel.clone(),
                session_id: session_id.to_string(),
            });
        }

        if let Some(handle) = self.sessions.lock().unwrap().remove(session_id) {
            handle.abort();
        }
        self.metrics.session_ended();
    }

    /// Publishes a message to a Redis channel via the regular
    /// `ConnectionManager` (not the pub/sub connection) — publishing
    /// scales independently of subscriber count.
    pub async fn publish(
        &self,
        channel: impl AsRef<str>,
        event: &WsMessage,
    ) -> redis::RedisResult<()> {
        let payload = serde_json::to_string(event).expect("WsMessage serialization failed");

        let mut conn = self.manager.clone();

        let _: usize = conn.publish(channel.as_ref(), payload).await?;

        Ok(())
    }

    /// Removes a session from the online set and, if found via the
    /// reverse lookup, from its owning user's session set too. Call this
    /// directly when a session's connection closes (e.g. from the
    /// message-stream loop in your ws handler on `Close`/error).
    pub async fn remove_online_user(&self, session_id: &str) -> redis::RedisResult<()> {
        let mut conn = self.manager.clone();

        let user_id: Option<String> = conn
            .get(format!("{SESSION_USER_PREFIX}:{session_id}"))
            .await?;

        let _: usize = conn.srem(ONLINE_USERS_KEY, session_id).await?;

        if let Some(user_id) = user_id {
            let _: usize = conn
                .srem(format!("{USER_SESSIONS_PREFIX}:{user_id}"), session_id)
                .await?;

            let _: usize = conn
                .del(format!("{SESSION_USER_PREFIX}:{session_id}"))
                .await?;
        }

        Ok(())
    }

    pub async fn add_online_user(&self, session_id: &str, user_id: &str) -> redis::RedisResult<()> {
        let mut conn = self.manager.clone();

        let _: usize = conn.sadd(ONLINE_USERS_KEY, session_id).await?;

        let _: () = conn
            .set(format!("{SESSION_USER_PREFIX}:{session_id}"), user_id)
            .await?;

        let _: usize = conn
            .sadd(format!("{USER_SESSIONS_PREFIX}:{user_id}"), session_id)
            .await?;

        Ok(())
    }

    pub async fn is_user_online(&self, user_id: &str) -> redis::RedisResult<bool> {
        let mut conn = self.manager.clone();
        let count: u64 = conn
            .scard(format!("{USER_SESSIONS_PREFIX}:{user_id}"))
            .await?;
        Ok(count > 0)
    }

    pub async fn online_users(&self) -> redis::RedisResult<u64> {
        let mut conn = self.manager.clone();
        conn.scard(ONLINE_USERS_KEY).await
    }

    /// Gracefully shuts this node down: aborts every registered
    /// forwarding task (so clients get a clean close rather than a
    /// silent hang) and signals the router task to stop. Call this from
    /// your SIGTERM/shutdown handler before the process exits.
    pub fn shutdown(&self) {
        let mut sessions = self.sessions.lock().unwrap();
        for (_, handle) in sessions.drain() {
            handle.abort();
        }
        drop(sessions);

        let _ = self.shutdown_tx.send(true);
    }
}

/// The single background task (per node) that owns the real Redis
/// pub/sub connection. Reconnects with backoff if the connection drops,
/// resubscribing to every channel still in `routing` afterward.
async fn run_router(
    redis_url: String,
    node_id: String,
    mut cmd_rx: mpsc::UnboundedReceiver<RouterCommand>,
    mut shutdown_rx: watch::Receiver<bool>,
    metrics: Metrics,
) {
    let client = match Client::open(redis_url.as_str()) {
        Ok(c) => c,
        Err(err) => {
            eprintln!("Arifa router: invalid redis url: {err}");
            return;
        }
    };

    let mut routing = RoutingTable::new();
    let mut backoff = RECONNECT_INITIAL_BACKOFF;

    'connection: loop {
        if *shutdown_rx.borrow() {
            break;
        }

        let mut pubsub = match client.get_async_pubsub().await {
            Ok(p) => p,
            Err(err) => {
                eprintln!(
                    "Arifa router: failed to open pub/sub connection: {err} (retrying in {backoff:?})"
                );
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(RECONNECT_MAX_BACKOFF);
                continue 'connection;
            }
        };

        // Fresh connection — resubscribe to everything already in the
        // routing table (non-empty only after a reconnect, since we
        // start empty on first boot).
        for channel in routing.channels() {
            if let Err(err) = pubsub.subscribe(&channel).await {
                eprintln!("Router failed to resubscribe to '{channel}' after reconnect: {err}");
            }
        }
        backoff = RECONNECT_INITIAL_BACKOFF; // reset after a successful (re)connect

        enum Action {
            Command(Option<RouterCommand>),
            Message(Option<redis::Msg>),
            Shutdown,
        }

        loop {
            // `on_message()` borrows `pubsub` mutably for the stream's
            // lifetime, so it's scoped tightly: build it, select, then
            // let it drop before touching `pubsub` again (subscribe/
            // unsubscribe below need `&mut pubsub` with no live borrow).
            let action = {
                let mut msg_stream = pubsub.on_message();
                tokio::select! {
                    cmd = cmd_rx.recv() => Action::Command(cmd),
                    msg = msg_stream.next() => Action::Message(msg),
                    _ = shutdown_rx.changed() => Action::Shutdown,
                }
            };

            match action {
                Action::Shutdown => {
                    println!("Arifa router for node '{node_id}' shutting down.");
                    break 'connection;
                }
                Action::Command(None) => {
                    // Every Arifa handle was dropped.
                    break 'connection;
                }
                Action::Command(Some(RouterCommand::Subscribe {
                    channel,
                    session_id,
                    sender,
                })) => {
                    let is_new_channel = routing.subscribe(&channel, &session_id, sender);
                    if is_new_channel {
                        if let Err(err) = pubsub.subscribe(&channel).await {
                            eprintln!("Router failed to subscribe to '{channel}': {err}");
                            // Treat as a dead connection and force a reconnect
                            // rather than silently never receiving this channel.
                            metrics.record_reconnect();
                            continue 'connection;
                        }
                    }
                }
                Action::Command(Some(RouterCommand::Unsubscribe {
                    channel,
                    session_id,
                })) => {
                    let should_unsubscribe = routing.unsubscribe(&channel, &session_id);
                    if should_unsubscribe {
                        if let Err(err) = pubsub.unsubscribe(&channel).await {
                            eprintln!("Router failed to unsubscribe from '{channel}': {err}");
                        }
                    }
                }
                Action::Message(None) => {
                    eprintln!(
                        "Arifa router: pub/sub stream ended unexpectedly, reconnecting in {backoff:?}"
                    );
                    metrics.record_reconnect();
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(RECONNECT_MAX_BACKOFF);
                    continue 'connection;
                }
                Action::Message(Some(msg)) => {
                    let channel = msg.get_channel_name().to_string();

                    let payload = match msg.get_payload::<String>() {
                        Ok(p) => p,
                        Err(err) => {
                            eprintln!("Invalid payload on '{channel}': {err}");
                            continue;
                        }
                    };

                    let event: WsMessage = match serde_json::from_str(&payload) {
                        Ok(e) => e,
                        Err(err) => {
                            eprintln!("Invalid JSON on '{channel}': {err}");
                            continue;
                        }
                    };

                    if let Some(target_node) = &event.node_id
                        && target_node != &node_id
                    {
                        continue;
                    }

                    let (delivered, dropped) = routing.route(&channel, event);
                    metrics.record_routed(delivered as u64, dropped as u64);
                }
            }
        }
    }

    println!("Arifa router for node '{node_id}' stopped.");
}
