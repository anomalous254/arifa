# axum-arifa

A realtime pub/sub demo project using [Arifa](https://crates.io/crates/arifa) (Redis-based WebSocket pub/sub), showing how to integrate it with two different Rust web frameworks:

- **`axum`** — built with the [Axum](https://github.com/tokio-rs/axum) web framework
- **`actix_web`** — built with [Actix Web](https://actix.rs/)

Each binary is a standalone, framework-specific example. They expose the same routes and behavior, but are tested and run independently below.

## Prerequisites

- Rust toolchain (edition 2024 support — run `rustup update` if `cargo build` complains about the edition)
- Redis running locally

```bash
redis-server &
```

- [wscat](https://github.com/websockets/wscat) for WebSocket testing

```bash
npm install -g wscat
```

## Project Structure

```
arifa-example/
├── Cargo.toml
└── src/
    └── bin/
        ├── main_axum.rs
        └── main_actix.rs
```

## Routes (same shape in both examples)

| Method | Path          | Description                                      |
|--------|---------------|---------------------------------------------------|
| GET    | `/ws/connect` | Upgrade to WebSocket, subscribe to channels       |
| GET    | `/publish`    | Trigger a test broadcast message via Redis        |
| GET    | `/online`     | Return current online subscription count          |

`/ws/connect` and `/publish` both take query params:

- `user_id` — subscribes to / publishes on `User::<user_id>`
- `location` — subscribes to / publishes on `Location::<location>`

---

## Axum Example

### Build

```bash
cargo build --bin axum
```

### Run

```bash
cargo run --bin axum
```

You should see:

```
Server running on http://0.0.0.0:3000
```

### Test with wscat + curl

**1. Connect a WebSocket client**

```bash
wscat -c "ws://localhost:3000/ws/connect?user_id=42&location=891e2040897ffff"
```

Expected:

```
Connected (press CTRL+C to quit)
```

**2. Trigger a broadcast (in a second terminal)**

```bash
curl "http://localhost:3000/publish?user_id=42&location=891e2040897ffff"
```

Expected:

```json
{"status":"sent"}
```

The wscat terminal should immediately show:

```json
{"scope":"Broadcast","kind":"Feeds","node_id":null,"payload":{"message":"Hello from /publish!"}}
```

**3. Check online user count**

```bash
curl "http://localhost:3000/online"
```

```json
{"online":1}
```

**4. Test multiple subscribers**

Open a third terminal:

```bash
wscat -c "ws://localhost:3000/ws/connect?user_id=99&location=891e2040897ffff"
```

Re-run the `/publish` curl command — both wscat sessions should receive the broadcast.

**5. Test disconnect cleanup**

Press `Ctrl+C` in a wscat terminal, then:

```bash
curl "http://localhost:3000/online"
```

The count should have decremented.

### Axum Testing Checklist

- [ ] `cargo run --bin axum` prints `Server running on http://0.0.0.0:3000`
- [ ] `wscat` connects to `/ws/connect`
- [ ] `curl /publish` returns `{"status":"sent"}`
- [ ] Message appears in the wscat session
- [ ] `curl /online` reflects the correct count
- [ ] Multiple wscat clients on the same `location` all receive a broadcast
- [ ] Disconnecting a wscat client decrements the online count

---

## Actix Web Example

### Build

```bash
cargo build --bin actix_web
```

### Run

```bash
cargo run --bin actix_web
```

You should see:

```
Server running on http://0.0.0.0:8080
```

### Test with wscat + curl

**1. Connect a WebSocket client**

```bash
wscat -c "ws://localhost:8080/ws/connect?user_id=7&location=891e2040897ffff"
```

Expected:

```
Connected (press CTRL+C to quit)
```

**2. Trigger a broadcast (in a second terminal)**

```bash
curl "http://localhost:8080/publish?user_id=7&location=891e2040897ffff"
```

Expected:

```json
{"status":"sent"}
```

The wscat terminal should immediately show:

```json
{"scope":"Broadcast","kind":"Feeds","node_id":null,"payload":{"message":"Hello from /publish!"}}
```

**3. Check online user count**

```bash
curl "http://localhost:8080/online"
```

```json
{"online":1}
```

**4. Test multiple subscribers**

Open a third terminal:

```bash
wscat -c "ws://localhost:8080/ws/connect?user_id=8&location=891e2040897ffff"
```

Re-run the `/publish` curl command — both wscat sessions should receive the broadcast.

**5. Test disconnect cleanup**

Press `Ctrl+C` in a wscat terminal, then:

```bash
curl "http://localhost:8080/online"
```

The count should have decremented.

### Actix Web Testing Checklist

- [ ] `cargo run --bin actix_web` prints `Server running on http://0.0.0.0:8080`
- [ ] `wscat` connects to `/ws/connect`
- [ ] `curl /publish` returns `{"status":"sent"}`
- [ ] Message appears in the wscat session
- [ ] `curl /online` reflects the correct count
- [ ] Multiple wscat clients on the same `location` all receive a broadcast
- [ ] Disconnecting a wscat client decrements the online count

---

## Notes


- Since `unsubscribe()` in Arifa is a hard `abort()`, both implementations explicitly call `remove_online_user` **before** `unsubscribe`, so cleanup isn't skipped.
- Make sure Redis is running (`redis-server`) before starting either binary — both will panic on startup if the Redis connection fails.

