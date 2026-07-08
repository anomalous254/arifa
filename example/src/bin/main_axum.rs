use axum::{
    Json, Router,
    extract::{
        Query, State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    response::IntoResponse,
    routing::get,
};
use futures::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::json;
use std::sync::Arc;
use tokio::sync::Mutex;

use arifa::prelude::*;
use async_trait::async_trait;

// ---- Shared app state ----
#[derive(Clone)]
struct AppState {
    arifa: Arc<Arifa>,
}

// ---- WsSession implementation for Axum's WebSocket ----
struct AxumWsSession {
    sender: Arc<Mutex<futures::stream::SplitSink<WebSocket, Message>>>,
}

#[async_trait]
impl WsSession for AxumWsSession {
    type Error = axum::Error;

    async fn send(&self, event: WsMessage) -> Result<(), Self::Error> {
        let payload = serde_json::to_string(&event).map_err(axum::Error::new)?;
        let mut sender = self.sender.lock().await;
        sender.send(Message::Text(payload)).await
    }
}

// ---- Query params for the ws connect route ----
#[derive(Deserialize)]
struct ConnectQuery {
    user_id: String,
    location: String,
}

#[tokio::main]
async fn main() {
    // Set up Arifa connected to Redis, node id "node-1"
    let arifa = Arifa::new("redis://127.0.0.1/", "node-1")
        .await
        .expect("failed to connect Arifa to Redis");

    let state = AppState {
        arifa: Arc::new(arifa),
    };

    let app = Router::new()
        .route("/", get(root))
        .route("/ws/connect", get(ws_connect))
        .route("/publish", get(publish_test_message))
        .route("/online", get(online_users))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();

    println!("Server running on http://0.0.0.0:3000");
    axum::serve(listener, app).await.unwrap();
}

async fn root() -> &'static str {
    "Arifa + Axum realtime server"
}

// ---- WebSocket upgrade handler ----
async fn ws_connect(
    ws: WebSocketUpgrade,
    Query(params): Query<ConnectQuery>,
    State(state): State<AppState>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, state, params))
}

async fn handle_socket(socket: WebSocket, state: AppState, params: ConnectQuery) {
    let (sender, mut receiver) = socket.split();
    let sender = Arc::new(Mutex::new(sender));

    let session = AxumWsSession {
        sender: sender.clone(),
    };

    let location_channel = format!("Location::{}", params.location);
    let user_channel = format!("User::{}", params.user_id);

    let session_id = state
        .arifa
        .subscribe(
            vec![&location_channel, &user_channel],
            session,
            params.user_id,
        )
        .await;

    let arifa = state.arifa.clone();

    // Read loop: waits for client disconnect / close frame
    while let Some(msg) = receiver.next().await {
        match msg {
            Ok(Message::Close(_)) => break,
            Err(_) => break,
            _ => {} // ignore other incoming frames (or handle client->server messages here)
        }
    }

    // Cleanup on disconnect
    let _ = arifa.remove_online_user(&session_id).await;
    arifa.unsubscribe(&session_id, vec![&location_channel, &user_channel]);
}

// ---- Simple HTTP route to publish a test broadcast message ----
async fn publish_test_message(
    State(state): State<AppState>,
    Query(params): Query<ConnectQuery>,
) -> impl IntoResponse {
    let location_channel = format!("Location::{}", params.location);

    let message = WsMessage {
        scope: MessageScope::Broadcast,
        kind: MessageKind::Feeds,
        node_id: None,
        payload: json!({ "message": "Hello from /publish!" }),
    };

    match state.arifa.publish(&location_channel, &message).await {
        Ok(_) => Json(json!({ "status": "sent" })),
        Err(e) => Json(json!({ "status": "error", "detail": e.to_string() })),
    }
}

// ---- Check online user count ----
async fn online_users(State(state): State<AppState>) -> impl IntoResponse {
    match state.arifa.online_users().await {
        Ok(count) => Json(json!({ "online": count })),
        Err(e) => Json(json!({ "status": "error", "detail": e.to_string() })),
    }
}
