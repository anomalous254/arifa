# Arifa

**Arifa** is a lightweight, Redis-based realtime pub/sub engine designed for WebSocket applications in Rust.

It provides a simple abstraction over Redis Pub/Sub and WebSocket sessions, enabling scalable real-time messaging across multiple nodes.

---

## ✨ Features

- Redis Pub/Sub integration
- Multi-node support via `node_id`
- WebSocket session abstraction (`WsSession`)
- Channel-based message routing
- Online user tracking
- Built on Tokio async runtime
- Easy integration with Actix Web or custom WS frameworks

---

##  Installation

Add Arifa to your `Cargo.toml`:

```toml
arifa = "0.1.0"
```

##  Quick Start

### 1. Create Arifa instance

```rust
use arifa::prelude::*;

#[tokio::main]
async fn main() {
    let arifa = Arifa::new(
        "redis://127.0.0.1/",
        "node-1"
    )
    .await
    .expect("Failed to connect to Redis");
}
```

### 2. Define a WebSocket session

Implement the `WsSession` trait:

```rust
use arifa::prelude::*;
use async_trait::async_trait;

struct MySession;

#[async_trait]
impl WsSession for MySession {
    type Error = anyhow::Error;

    async fn send(&self, event: WsMessage) -> Result<(), Self::Error> {
        println!("Received: {:?}", event);
        Ok(())
    }
}
```

### 3. Subscribe to channels

```rust
let session = MySession;

let handle = arifa.subscribe(
    vec!["Location::123", "User::42"],
    session,
);
```

### 4. Publish messages

```rust
use arifa::prelude::*;
use serde_json::json;

let msg = WsMessage {
    scope: NotificationScope::Broadcast,
    kind: NotificationKind::Event,
    node_id: None,
    payload: json!({"hello": "world"}),
};

arifa.publish("Location::123", &msg).await?;
```

### 5. Unsubscribe (stop subscription)

```rust
arifa.unsubscribe(handle);
```

## 🌍 Message Model

```rust
pub struct WsMessage {
    pub scope: NotificationScope,
    pub kind: NotificationKind,
    pub node_id: Option<String>,
    pub payload: serde_json::Value,
}
```

### Scopes

- `Broadcast` → sent to all subscribers
- `Private` → targeted messages

### Kinds

- `Feeds`
- `DirectMessage`
- `Event`

## 🧠 Architecture

Arifa works as follows:

```
WebSocket Client
      ↓
WsSession trait
      ↓
Arifa subscription task (Tokio)
      ↓
Redis Pub/Sub
      ↓
Multi-node routing (node_id filtering)
```

Each WebSocket connection runs in its own lightweight async task.

## 🧩 Example Use Cases

- Real-time chat systems
- Live location tracking
- Notifications system
- Multiplayer game events
- Social feed updates

## ⚠️ Notes

- `unsubscribe()` uses `JoinHandle::abort()` (hard stop)
- Each subscription spawns a Tokio task
- Redis Pub/Sub connection is per subscription

## 📦 License

Apache-2.0
