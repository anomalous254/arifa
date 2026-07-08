# Arifa

Arifa is a lightweight, Redis-based realtime pub/sub engine for Rust applications.

It provides a simple abstraction over Redis Pub/Sub and WebSocket sessions, making it easy to build scalable realtime systems that work across multiple application nodes.

## Features

- Redis Pub/Sub
- Shared Redis Pub/Sub connection per node
- Automatic reconnect with exponential backoff
- Automatic channel resubscription
- Tokio async runtime
- WebSocket session abstraction
- Channel-based subscriptions
- Multi-node message routing
- Online user tracking
- Lock-free metrics
- Framework agnostic (`WsSession` trait)

## Benchmarks

| Benchmark | Result |
|---|---|
| Subscribe → unsubscribe (round trip) | ~1.1 ms |
| Message serialization | ~0.18 µs |
| Cross-node publish → receive | ~375 µs |
| Delivery throughput @ 100 subscribers | ~185,000 msg/sec |
| Delivery throughput @ 1,000 subscribers | ~260,000 msg/sec |
| Delivery throughput @ 10,000 subscribers | ~270,000 msg/sec |
| Fan-out: 1 channel × 2,000 subs (1 publish) | ~17 ms |
| Fan-out: 2,000 channels × 1 sub each (2,000 publishes) | ~490 ms |
| Messages dropped / Redis reconnects | 0 / 0 |

Delivery throughput holds steady from 100 to 10,000 concurrent subscribers on a single channel — total time to fan out scales with `sessions × messages`, not with a drop in per-message throughput.

Benchmarks live in their own crate at [`benchmarks/`](./benchmarks) and use [Criterion](https://github.com/bheisler/criterion.rs) against a local Redis instance. See [`benchmarks/README.md`](./benchmarks/README.md) for how to run them yourself and what each one measures.


## Installation

```bash
cargo add arifa
```

## Creating an Arifa Instance

```rust
use arifa::prelude::*;

#[tokio::main]
async fn main() {
    let arifa = Arifa::new(
        "redis://127.0.0.1/",
        "node-1",
    )
    .await
    .unwrap();
}
```

## Defining a WebSocket Session

Arifa is transport agnostic. Implement the `WsSession` trait for your WebSocket framework.

```rust
use arifa::prelude::*;
use async_trait::async_trait;

struct MySession;

#[async_trait]
impl WsSession for MySession {
    type Error = anyhow::Error;

    async fn send(&self, event: WsMessage) -> Result<(), Self::Error> {
        println!("{:?}", event);
        Ok(())
    }
}
```

## Subscribing

`subscribe()` registers the session, marks it online, and returns a generated session id.

```rust
let session = MySession;

let channels = vec![
    "Location::891e2040897ffff".to_string(),
    "User::42".to_string(),
];

let session_id = arifa.subscribe(
    channels.clone(),
    session,
    "42",
).await;
```

The returned session id is used when removing the connection from the online users set and unsubscribing.

## Publishing Messages

```rust
use arifa::prelude::*;
use serde_json::json;

let message = WsMessage {
    scope: MessageScope::Broadcast,
    kind: MessageKind::Feeds,
    node_id: None,
    payload: json!({
        "message": "Hello world"
    }),
};

arifa
    .publish("Location::891e2040897ffff", &message)
    .await?;
```

## Unsubscribing

When a WebSocket disconnects, remove the session from the online users set before unsubscribing.

```rust
let _ = arifa.remove_online_user(&session_id).await;
arifa.unsubscribe(&session_id, &channels);
```

## Message Model

```rust
pub struct WsMessage {
    pub scope: MessageScope,
    pub kind: MessageKind,
    pub node_id: Option<String>,
    pub payload: serde_json::Value,
}
```

### MessageScope

```rust
pub enum MessageScope {
    Broadcast,
    Private,
}
```

### MessageKind

```rust
pub enum MessageKind {
    Feeds,
    DirectMessage,
    Event,
}
```

## Online Users

A connection is automatically marked online inside `subscribe()`.

Query the current number of active sessions:

```rust
let users = arifa.online_users().await?;
println!("{users}");
```

Remove a session when its WebSocket closes:

```rust
let _ = arifa.remove_online_user(&session_id).await;
```

Check whether a user currently has any active sessions:

```rust
let online = arifa.is_user_online("42").await?;
```

> **Note:** The online count reflects active sessions, not unique users. Multiple browser tabs or devices for the same user count as multiple sessions.

## Multi-node Routing

To target only a specific application node, set the `node_id` field.

```rust
let message = WsMessage {
    scope: MessageScope::Private,
    kind: MessageKind::Event,
    node_id: Some("node-2".into()),
    payload: serde_json::json!({
        "status": "updated"
    }),
};

arifa.publish("User::42", &message).await?;
```

If `node_id` is `None`, every subscribed node receives the message.

## Metrics

Arifa exposes lightweight runtime metrics.

```rust
let metrics = arifa.metrics.snapshot();

println!("Active sessions: {}", metrics.sessions_active);
println!("Messages routed: {}", metrics.messages_routed);
println!("Messages dropped: {}", metrics.messages_dropped);
println!("Redis reconnects: {}", metrics.redis_reconnects);
```

## Graceful Shutdown

Before shutting down your application, stop Arifa's background router and forwarding tasks.

```rust
arifa.shutdown();
```

## Example: Actix Web

```rust
#[get("/ws/connect")]
pub async fn notification_channel(
    req: HttpRequest,
    body: web::Payload,
    state: web::Data<AppState>,
    query: web::Query<ConnectRequestQuery>,
) -> Result<HttpResponse, Error> {
    let (resp, session, mut stream) = actix_ws::handle(&req, body)?;

    let location = format!("Location::{}", query.get_cell()?);
    let user = format!("User::{}", query.user_id);

    let channels = vec![location.clone(), user.clone()];

    let ws = ActixWsSession::new(session);

    let session_id = state.arifa.subscribe(
        channels.clone(),
        ws,
        query.user_id.to_string(),
    );

    let arifa = state.arifa.clone();

    actix_web::rt::spawn(async move {
        while let Some(message) = stream.next().await {
            match message {
                Ok(actix_ws::Message::Close(_)) => break,
                Err(_) => break,
                _ => {}
            }
        }

        let _ = arifa.remove_online_user(&session_id).await;
        arifa.unsubscribe(&session_id, &channels);
    });

    Ok(resp)
}
```

## Architecture

```
                   WebSocket Client
                           │
                           ▼
                      WsSession
                           │
                           ▼
                 Per-session forwarder
                           ▲
                           │
                    Routing Table
                           │
                           ▼
          Shared Redis Pub/Sub Router
                (one per application node)
                           │
                           ▼
                     Redis Pub/Sub
                           │
                           ▼
                      Other Nodes
```

Each Arifa instance maintains a single Redis Pub/Sub connection that is shared by all subscriptions on that node. Incoming messages are routed to subscribed sessions through an in-memory routing table. If the Redis connection drops, Arifa automatically reconnects and resubscribes to all active channels.

## Use Cases

- Chat applications
- Notifications
- Live location updates
- Multiplayer games
- Live dashboards
- Social feeds
- Collaborative applications

## Notes

- One shared Redis Pub/Sub connection is used per application node.
- Each subscribed session runs in its own forwarding task.
- Session message queues are bounded to prevent slow clients from consuming unbounded memory.
- Redis reconnects and channel resubscriptions happen automatically.
- Call `remove_online_user()` before `unsubscribe()` when a client disconnects.

## License

Apache-2.0
