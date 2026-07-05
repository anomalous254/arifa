use super::event::WsMessage;
use crate::session::WsSession;
use futures_util::StreamExt;
use redis::{AsyncCommands, Client, aio::ConnectionManager};
use std::sync::Arc;
use tokio::task::JoinHandle;
use uuid::Uuid;

const ONLINE_USERS_KEY: &str = "arifa:online_users";

/// Core realtime pub/sub engine built on Redis.
///
/// `Arifa` provides:
/// - Redis Pub/Sub subscriptions per WebSocket session
/// - Message routing across channels
/// - Multi-node filtering via `node_id`
/// - Online user tracking
///
/// Each subscription runs in a separate Tokio task.
#[derive(Clone)]
pub struct Arifa {
    client: Client,
    manager: ConnectionManager,
    node_id: String,
}

impl Arifa {
    /// Creates a new `Arifa` instance connected to Redis.
    ///
    /// # Arguments
    /// - `redis_url`: Redis connection string (e.g. `redis://localhost`)
    /// - `node_id`: Unique identifier for this server instance (used for routing)
    ///
    /// # Errors
    /// Returns a Redis error if connection fails.
    pub async fn new(
        redis_url: impl AsRef<str>,
        node_id: impl Into<String>,
    ) -> redis::RedisResult<Self> {
        let client = Client::open(redis_url.as_ref())?;
        let manager = client.get_connection_manager().await?;

        Ok(Self {
            client,
            manager,
            node_id: node_id.into(),
        })
    }

    /// Returns the node identifier of this Arifa instance.
    ///
    /// Used for multi-node message filtering.
    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    /// Subscribes a session to one or more Redis channels.
    ///
    /// This spawns a background task that:
    /// - Listens to Redis Pub/Sub messages
    /// - Deserializes them into `WsMessage`
    /// - Forwards them to the provided `WsSession`
    /// - Filters messages based on `node_id`
    ///
    /// # Arguments
    /// - `channels`: list of Redis channels to subscribe to
    /// - `session`: WebSocket session implementing `WsSession`
    ///
    /// # Returns
    /// A `JoinHandle` representing the background subscription task.
    /// This can be aborted to stop the subscription.
    pub fn subscribe<C, S>(
        &self,
        channels: impl IntoIterator<Item = S>,
        session: C,
    ) -> JoinHandle<()>
    where
        C: WsSession + 'static,
        S: Into<String>,
    {
        let channels = channels.into_iter().map(Into::into).collect::<Vec<_>>();
        let session = Arc::new(session);
        let arifa = self.clone();
        let session_id = Uuid::new_v4().to_string();

        tokio::spawn(async move {
            arifa.subscription_loop(channels, session, session_id).await;
        })
    }

    /// Internal subscription loop that processes Redis messages.
    ///
    /// This function:
    /// - Connects to Redis Pub/Sub
    /// - Subscribes to provided channels
    /// - Streams incoming messages
    /// - Deserializes payload into `WsMessage`
    /// - Sends messages to the session
    /// - Stops when session disconnects or fails
    async fn subscription_loop<C>(&self, channels: Vec<String>, session: Arc<C>, session_id: String)
    where
        C: WsSession,
    {
        let mut pubsub = match self.client.get_async_pubsub().await {
            Ok(pubsub) => pubsub,
            Err(err) => {
                eprintln!("Failed to create PubSub connection: {err}");
                return;
            }
        };

        for channel in &channels {
            if let Err(err) = pubsub.subscribe(channel).await {
                eprintln!("Failed to subscribe to '{channel}': {err}");
                return;
            }
        }

        let _ = self.add_online_user(&session_id).await;

        println!("Node '{}' subscribed to {:?}", self.node_id, channels);

        let mut stream = pubsub.on_message();

        while let Some(message) = stream.next().await {
            let payload = match message.get_payload::<String>() {
                Ok(payload) => payload,
                Err(err) => {
                    eprintln!("Invalid payload: {err}");
                    continue;
                }
            };

            let event: WsMessage = match serde_json::from_str(&payload) {
                Ok(event) => event,
                Err(err) => {
                    eprintln!("Invalid JSON: {err}");
                    continue;
                }
            };

            // Skip messages targeted at another node.
            if let Some(target_node) = &event.node_id
                && target_node != &self.node_id
            {
                continue;
            }

            if session.send(event).await.is_err() {
                break;
            }
        }

        let _ = self.remove_online_user(&session_id).await;

        println!("Subscription closed.");
    }

    /// Publishes a message to a Redis channel.
    ///
    /// # Arguments
    /// - `channel`: Redis channel name
    /// - `event`: message payload to broadcast
    pub async fn publish(
        &self,
        channel: impl AsRef<str>,
        event: &WsMessage,
    ) -> redis::RedisResult<()> {
        let payload = serde_json::to_string(event).expect("WsMessage serialization failed");

        let mut conn = self.manager.clone();

        conn.publish(channel.as_ref(), payload).await
    }

    /// Marks a session as online in Redis.
    pub async fn add_online_user(&self, session_id: &str) -> redis::RedisResult<()> {
        let mut conn = self.manager.clone();

        conn.sadd(ONLINE_USERS_KEY, session_id).await
    }

    /// Removes a session from the online users set.
    pub async fn remove_online_user(&self, session_id: &str) -> redis::RedisResult<()> {
        let mut conn = self.manager.clone();

        conn.srem(ONLINE_USERS_KEY, session_id).await
    }

    /// Returns the number of currently online users.
    pub async fn online_users(&self) -> redis::RedisResult<u64> {
        let mut conn = self.manager.clone();

        conn.scard(ONLINE_USERS_KEY).await
    }

    /// Aborts an active subscription task.
    ///
    /// This immediately stops the Redis subscription loop associated
    /// with the given `JoinHandle`.
    ///
    /// # Note
    /// This is a hard stop (not graceful shutdown).
    pub fn unsubscribe(&self, handle: JoinHandle<()>) {
        handle.abort();
    }
}
