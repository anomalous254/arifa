use actix_web::{App, Error, HttpRequest, HttpResponse, HttpServer, get, web};
use async_trait::async_trait;
use futures_util::StreamExt;
use serde::Deserialize;
use serde_json::json;

use arifa::prelude::*;

// ---- Shared app state ----
struct AppState {
    arifa: Arifa,
}

// ---- WsSession implementation wrapping actix_ws::Session ----
struct ActixWsSession {
    session: actix_ws::Session,
}

#[async_trait]
impl WsSession for ActixWsSession {
    type Error = actix_ws::Closed;

    async fn send(&self, event: WsMessage) -> Result<(), Self::Error> {
        let payload = serde_json::to_string(&event).unwrap_or_default();
        let mut session = self.session.clone();
        session.text(payload).await
    }
}

// ---- Query params for the ws connect route ----
#[derive(Deserialize)]
struct ConnectRequestQuery {
    user_id: String,
    location: String,
}

// ---- WebSocket connect endpoint ----
#[get("/ws/connect")]
async fn notification_channel(
    req: HttpRequest,
    body: web::Payload,
    state: web::Data<AppState>,
    query: web::Query<ConnectRequestQuery>,
) -> Result<HttpResponse, Error> {
    let (resp, session, mut stream) = actix_ws::handle(&req, body)?;

    let location = format!("Location::{}", query.location);
    let user = format!("User::{}", query.user_id);

    let ws = ActixWsSession { session };

    let session_id = state
        .arifa
        .subscribe(vec![&location, &user], ws, query.user_id)
        .await;

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
        arifa.unsubscribe(&session_id, vec![&location, &user]);
    });

    Ok(resp)
}

// ---- HTTP route to trigger a test broadcast ----
#[get("/publish")]
async fn publish_test_message(
    state: web::Data<AppState>,
    query: web::Query<ConnectRequestQuery>,
) -> HttpResponse {
    let location_channel = format!("Location::{}", query.location);

    let message = WsMessage {
        scope: MessageScope::Broadcast,
        kind: MessageKind::Feeds,
        node_id: None,
        payload: json!({ "message": "Hello from /publish!" }),
    };

    match state.arifa.publish(&location_channel, &message).await {
        Ok(_) => HttpResponse::Ok().json(json!({ "status": "sent" })),
        Err(e) => HttpResponse::InternalServerError()
            .json(json!({ "status": "error", "detail": e.to_string() })),
    }
}

// ---- Check online user count ----
#[get("/online")]
async fn online_users(state: web::Data<AppState>) -> HttpResponse {
    match state.arifa.online_users().await {
        Ok(count) => HttpResponse::Ok().json(json!({ "online": count })),
        Err(e) => HttpResponse::InternalServerError()
            .json(json!({ "status": "error", "detail": e.to_string() })),
    }
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let arifa = Arifa::new("redis://127.0.0.1/", "node-1")
        .await
        .expect("failed to connect Arifa to Redis");

    let state = web::Data::new(AppState { arifa });

    println!("Server running on http://0.0.0.0:8080");

    HttpServer::new(move || {
        App::new()
            .app_data(state.clone())
            .service(notification_channel)
            .service(publish_test_message)
            .service(online_users)
    })
    .bind(("0.0.0.0", 8080))?
    .run()
    .await
}
