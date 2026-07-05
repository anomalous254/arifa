# Arifa

Arifa is a lightweight, Redis-based realtime pub/sub engine for Rust applications.

It provides a simple abstraction over Redis Pub/Sub and WebSocket sessions, making it easy to build scalable realtime systems that work across multiple application nodes.

## Features

- Redis Pub/Sub
- Tokio async runtime
- WebSocket session abstraction
- Channel-based subscriptions
- Multi-node message routing
- Online user tracking
- Framework agnostic (`WsSession` trait)

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

`subscribe()` returns a tuple containing the spawned task handle and the generated session id.

```rust
let session = MySession;

let (handle, session_id) = arifa.subscribe(
    vec![
        "Location::891e2040897ffff",
        "User::42",
    ],
    session,
);
```

The session id can be used to remove the connection from the online users set when the client disconnects.

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

`unsubscribe()` aborts the subscription's Tokio task via `JoinHandle::abort()`. This is a hard stop, so any code that would otherwise run after the subscription loop — including online-user cleanup — is skipped. Call `remove_online_user` yourself when the WebSocket closes:

```rust
let _ = arifa.remove_online_user(&session_id).await;
arifa.unsubscribe(handle);
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

### NotificationScope

```rust
pub enum MessageScope {
    Broadcast,
    Private,
}
```

### NotificationKind

```rust
pub enum MessageKind {
    Feeds,
    DirectMessage,
    Event,
}
```

## Online Users

A connection is marked online automatically inside `subscribe()`.

Query the current online count:

```rust
let users = arifa.online_users().await?;
println!("{users}");
```

Remove a user when their connection closes:

```rust
let _ = arifa.remove_online_user(&session_id).await;
```

Note: this count reflects active subscriptions, not unique users — a client with multiple concurrent subscriptions (e.g. multiple open tabs) is counted once per subscription.

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

    let ws = ActixWsSession::new(session);

    let (handle, session_id) =
        state.arifa.subscribe(vec![&location, &user], ws);

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
        arifa.unsubscribe(handle);
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
Arifa
        │
        ▼
Redis Pub/Sub
        │
        ▼
Other Nodes
```

Each subscription owns its own Redis Pub/Sub connection and runs inside a dedicated Tokio task.

## Use Cases

- Chat applications
- Notifications
- Live location updates
- Multiplayer games
- Live dashboards
- Social feeds
- Collaborative applications

## Notes

- `unsubscribe()` uses `JoinHandle::abort()` (hard stop) — callers are responsible for calling `remove_online_user` themselves.
- Each subscription spawns a dedicated Tokio task and generates its own session id.
- A Redis Pub/Sub connection is created per subscription.

## License

Apache-2.0
