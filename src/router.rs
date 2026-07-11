use crate::metrics::Metrics;
use crate::models::WsMessage;
use crate::routing_table::RoutingTable;
use futures_util::StreamExt;
use redis::Client;
use std::time::Duration;
use tokio::sync::{mpsc, watch};
use tracing::{info, warn};

/// Initial and max backoff when the shared Redis pub/sub connection
/// drops and needs to be reestablished.
const RECONNECT_INITIAL_BACKOFF: Duration = Duration::from_millis(200);
const RECONNECT_MAX_BACKOFF: Duration = Duration::from_secs(30);

/// Commands sent from `Arifa::subscribe`/`unsubscribe` to the single
/// background router task that owns the real Redis pub/sub connection.
pub enum RouterCommand {
    Subscribe {
        channel: String,
        session_id: String,
        sender: mpsc::Sender<WsMessage>,
        ack: tokio::sync::oneshot::Sender<()>,
    },
    Unsubscribe {
        channel: String,
        session_id: String,
    },
}

/// The single background task (per node) that owns the real Redis
/// pub/sub connection. Reconnects with backoff if the connection drops,
/// resubscribing to every channel still in `routing` afterward.
pub async fn run_router(
    redis_url: String,
    node_id: String,
    mut cmd_rx: mpsc::UnboundedReceiver<RouterCommand>,
    mut shutdown_rx: watch::Receiver<bool>,
    metrics: Metrics,
) {
    let client = match Client::open(redis_url.as_str()) {
        Ok(c) => c,
        Err(err) => {
            warn!("Arifa router: invalid redis url: {err}");
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
                warn!("Arifa router: failed to open pub/sub connection: {err} (retrying in {backoff:?})");
                
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
                warn!("Router failed to resubscribe to '{channel}' after reconnect: {err}");
                
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
                    info!("Arifa router for node '{node_id}' shutting down.");
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
                    ack,
                })) => {
                    let is_new_channel = routing.subscribe(&channel, &session_id, sender);
                    if is_new_channel {
                        if let Err(err) = pubsub.subscribe(&channel).await {
                            warn!("Router failed to subscribe to '{channel}': {err}");
                            
                            // Treat as a dead connection and force a reconnect
                            // rather than silently never receiving this channel.
                            metrics.record_reconnect();
                            continue 'connection;
                        }
                    }
                    let _ = ack.send(());
                }
                Action::Command(Some(RouterCommand::Unsubscribe {
                    channel,
                    session_id,
                })) => {
                    let should_unsubscribe = routing.unsubscribe(&channel, &session_id);
                    if should_unsubscribe {
                        if let Err(err) = pubsub.unsubscribe(&channel).await {
                            warn!("Router failed to unsubscribe from '{channel}': {err}");
                        }
                    }
                }
                Action::Message(None) => {
                    warn!(
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
                            warn!("Invalid payload on '{channel}': {err}");
                            continue;
                        }
                    };

                    let event: WsMessage = match serde_json::from_str(&payload) {
                        Ok(e) => e,
                        Err(err) => {
                            warn!("Invalid JSON on '{channel}': {err}");
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

    info!("Arifa router for node '{node_id}' stopped.");
}
