use super::event::WsMessage;
use crate::metrics::Metrics;
use crate::router::{RouterCommand, run_router};
use crate::session::WsSession;
use redis::{AsyncCommands, Client, aio::ConnectionManager};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use uuid::Uuid;

const ONLINE_USERS_KEY: &str = "arifa:online_users";
const USER_SESSIONS_PREFIX: &str = "arifa:user_sessions";
const SESSION_USER_PREFIX: &str = "arifa:session_user";
const USER_NODE_PREFIX: &str = "arifa:user_node";

/// Buffer size for each session's forwarding channel. A slow client can
/// have this many messages queued before new ones start getting dropped
/// (see `RoutingTable::route`) — this bounds per-session memory instead
/// of letting one stuck client grow without limit.
const SESSION_CHANNEL_CAPACITY: usize = 256;

/// Core realtime pub/sub engine built on Redis.
///
/// A single background "router" task per node owns one shared Redis
/// pub/sub connection; sessions register interest via an in-memory
/// [`RoutingTable`] rather than each opening their own Redis connection.
/// The router automatically reconnects (with backoff) and resubscribes
/// to every channel still in use if the connection drops.
#[derive(Clone)]
pub struct Arifa {
    /// Dedicated connection for `publish()` — the hot path. Kept separate
    /// from `presence_manager` so publish latency can never queue behind
    /// bursts of online-user bookkeeping writes (SADD/SET/SREM), and vice
    /// versa. Each `ConnectionManager` multiplexes exactly one physical
    /// connection, so two managers means two real, independent
    /// connections to Redis.
    manager: ConnectionManager,
    /// Dedicated connection for online-user presence tracking
    /// (`add_online_user` / `remove_online_user` / `is_user_online` /
    /// `online_users`). Separate from `manager` for the same reason: a
    /// burst of presence writes (e.g. many sessions connecting at once)
    /// must never delay message delivery on the publish path, and a
    /// burst of publishes must never delay presence bookkeeping.
    presence_manager: ConnectionManager,
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

        // Two independent physical connections from the same client: one
        // dedicated to publish() traffic, one to presence bookkeeping.
        // This is on top of the router's own separate pub/sub connection
        // (opened inside run_router), so each node holds three Redis
        // connections total — a small, fixed cost in exchange for
        // guaranteeing none of the three workloads can queue behind
        // another under load.
        let manager = client.get_connection_manager().await?;
        let presence_manager = client.get_connection_manager().await?;

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
            presence_manager,
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
    pub async fn subscribe<C, S>(
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
            let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();
            let _ = self.command_tx.send(RouterCommand::Subscribe {
                channel: channel.clone(),
                session_id: session_id.clone(),
                sender: tx.clone(),
                ack: ack_tx,
            });
            let _ = ack_rx.await; // don't return until the router has actually registered this channel
        }

        self.metrics.session_started();

        let task_session_id = session_id.clone();
        let metrics = self.metrics.clone();
        let handle = tokio::spawn(async move {
            // Now safe to re-enable: this goes over `presence_manager`,
            // a connection dedicated to bookkeeping traffic, so it can
            // no longer queue behind (or block) publish() traffic on
            // `manager`.
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
            self.metrics.session_ended();
            handle.abort();
        }
    }

    /// Publishes a message to a Redis channel via the dedicated publish
    /// `ConnectionManager` (not the pub/sub connection, and not the
    /// presence connection) — publishing scales independently of both
    /// subscriber count and presence-bookkeeping load.
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
        let mut conn = self.presence_manager.clone();

        let user_id: Option<String> = conn
            .get(format!("{SESSION_USER_PREFIX}:{session_id}"))
            .await?;

        let _: usize = conn.srem(ONLINE_USERS_KEY, session_id).await?;

        if let Some(user_id) = user_id {
            let user_sessions_key = format!("{USER_SESSIONS_PREFIX}:{user_id}");

            let _: usize = conn.srem(&user_sessions_key, session_id).await?;

            let _: usize = conn
                .del(format!("{SESSION_USER_PREFIX}:{session_id}"))
                .await?;

            // If this was the last session, remove the node mapping too.
            let remaining: u64 = conn.scard(&user_sessions_key).await?;

            if remaining == 0 {
                let _: usize = conn.del(format!("{USER_NODE_PREFIX}:{user_id}")).await?;

                let _: usize = conn.del(user_sessions_key).await?;
            }
        }

        Ok(())
    }

    pub async fn get_user_node_id(&self, user_id: &str) -> redis::RedisResult<Option<String>> {
        let mut conn = self.presence_manager.clone();

        conn.get(format!("{USER_NODE_PREFIX}:{user_id}")).await
    }

    pub async fn add_online_user(&self, session_id: &str, user_id: &str) -> redis::RedisResult<()> {
        let mut conn = self.presence_manager.clone();

        // session is online
        let _: usize = conn.sadd(ONLINE_USERS_KEY, session_id).await?;

        // session -> user
        let _: () = conn
            .set(format!("{SESSION_USER_PREFIX}:{session_id}"), user_id)
            .await?;

        // user -> sessions
        let _: usize = conn
            .sadd(format!("{USER_SESSIONS_PREFIX}:{user_id}"), session_id)
            .await?;

        // user -> node
        let _: () = conn
            .set(format!("{USER_NODE_PREFIX}:{user_id}"), &self.node_id)
            .await?;

        Ok(())
    }

    pub async fn is_user_online(&self, user_id: &str) -> redis::RedisResult<bool> {
        let mut conn = self.presence_manager.clone();
        let count: u64 = conn
            .scard(format!("{USER_SESSIONS_PREFIX}:{user_id}"))
            .await?;
        Ok(count > 0)
    }

    pub async fn online_users(&self) -> redis::RedisResult<u64> {
        let mut conn = self.presence_manager.clone();
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
