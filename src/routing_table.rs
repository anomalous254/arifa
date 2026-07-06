use crate::WsMessage;
use std::collections::HashMap;
use tokio::sync::mpsc;

/// Pure in-memory bookkeeping for "which sessions want which channels".
/// Deliberately has zero Redis I/O so it can be unit tested directly,
/// without spinning up a real Redis instance.
#[derive(Default)]
pub struct RoutingTable {
    // channel -> (session_id -> sender)
    routes: HashMap<String, HashMap<String, mpsc::Sender<WsMessage>>>,
}

impl RoutingTable {
    pub fn new() -> Self {
        Self {
            routes: HashMap::new(),
        }
    }

    /// Registers a session's interest in a channel.
    /// Returns `true` if this is the *first* session on this channel —
    /// the caller should then issue a real Redis `SUBSCRIBE`.
    pub fn subscribe(
        &mut self,
        channel: &str,
        session_id: &str,
        sender: mpsc::Sender<WsMessage>,
    ) -> bool {
        let is_new = !self.routes.contains_key(channel);
        self.routes
            .entry(channel.to_string())
            .or_default()
            .insert(session_id.to_string(), sender);
        is_new
    }

    /// Removes a session's interest in a channel.
    /// Returns `true` if this was the *last* session on this channel —
    /// the caller should then issue a real Redis `UNSUBSCRIBE`.
    pub fn unsubscribe(&mut self, channel: &str, session_id: &str) -> bool {
        if let Some(sessions) = self.routes.get_mut(channel) {
            sessions.remove(session_id);
            if sessions.is_empty() {
                self.routes.remove(channel);
                return true;
            }
        }
        false
    }

    /// Delivers `event` to every session registered on `channel`.
    ///
    /// A session with a full buffer (a stalled/slow client) has the
    /// message dropped rather than being pruned or blocking every other
    /// session on the same channel — a momentarily-full buffer isn't
    /// fatal. A session whose receiver has been dropped entirely
    /// (forwarding task ended) IS pruned.
    ///
    /// Returns `(delivered_count, dropped_count)`.
    pub fn route(&mut self, channel: &str, event: WsMessage) -> (usize, usize) {
        let Some(sessions) = self.routes.get_mut(channel) else {
            return (0, 0);
        };

        let mut delivered = 0usize;
        let mut dropped = 0usize;

        sessions.retain(|_, sender| match sender.try_send(event.clone()) {
            Ok(()) => {
                delivered += 1;
                true
            }
            Err(mpsc::error::TrySendError::Full(_)) => {
                dropped += 1;
                true
            }
            Err(mpsc::error::TrySendError::Closed(_)) => false,
        });

        if sessions.is_empty() {
            self.routes.remove(channel);
        }

        (delivered, dropped)
    }

    /// Every channel currently subscribed to — used to resubscribe all
    /// of them after a Redis reconnect.
    pub fn channels(&self) -> Vec<String> {
        self.routes.keys().cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{MessageKind, MessageScope};

    fn channel_pair(capacity: usize) -> (mpsc::Sender<WsMessage>, mpsc::Receiver<WsMessage>) {
        mpsc::channel(capacity)
    }

    fn sample_event() -> WsMessage {
        WsMessage {
            scope: MessageScope::Broadcast,
            kind: MessageKind::Event,
            node_id: None,
            payload: serde_json::json!({ "test": true }),
        }
    }

    #[test]
    fn first_subscriber_triggers_real_subscribe() {
        let mut table = RoutingTable::new();
        let (tx, _rx) = channel_pair(4);
        assert!(table.subscribe("chan", "session-1", tx));
    }

    #[test]
    fn second_subscriber_does_not_resubscribe() {
        let mut table = RoutingTable::new();
        let (tx1, _rx1) = channel_pair(4);
        let (tx2, _rx2) = channel_pair(4);
        assert!(table.subscribe("chan", "session-1", tx1));
        assert!(!table.subscribe("chan", "session-2", tx2));
    }

    #[test]
    fn last_unsubscriber_triggers_real_unsubscribe() {
        let mut table = RoutingTable::new();
        let (tx1, _rx1) = channel_pair(4);
        let (tx2, _rx2) = channel_pair(4);
        table.subscribe("chan", "session-1", tx1);
        table.subscribe("chan", "session-2", tx2);

        assert!(!table.unsubscribe("chan", "session-1"));
        assert!(table.unsubscribe("chan", "session-2"));
    }

    #[test]
    fn route_delivers_to_all_subscribers() {
        let mut table = RoutingTable::new();
        let (tx1, mut rx1) = channel_pair(4);
        let (tx2, mut rx2) = channel_pair(4);
        table.subscribe("chan", "session-1", tx1);
        table.subscribe("chan", "session-2", tx2);

        let (delivered, dropped) = table.route("chan", sample_event());
        assert_eq!(delivered, 2);
        assert_eq!(dropped, 0);
        assert!(rx1.try_recv().is_ok());
        assert!(rx2.try_recv().is_ok());
    }

    #[test]
    fn route_prunes_dropped_receiver() {
        let mut table = RoutingTable::new();
        let (tx, rx) = channel_pair(4);
        table.subscribe("chan", "session-1", tx);
        drop(rx);

        let (delivered, dropped) = table.route("chan", sample_event());
        assert_eq!(delivered, 0);
        assert_eq!(dropped, 0);
        assert!(table.channels().is_empty());
    }

    #[test]
    fn route_drops_but_keeps_slow_session() {
        let mut table = RoutingTable::new();
        let (tx, _rx) = channel_pair(1); // capacity 1, receiver never drains
        table.subscribe("chan", "session-1", tx);

        let (d1, dropped1) = table.route("chan", sample_event()); // fills the buffer
        let (d2, dropped2) = table.route("chan", sample_event()); // buffer now full

        assert_eq!((d1, dropped1), (1, 0));
        assert_eq!((d2, dropped2), (0, 1));
        // Session is still registered — a full buffer isn't fatal.
        assert_eq!(table.channels(), vec!["chan".to_string()]);
    }
}
