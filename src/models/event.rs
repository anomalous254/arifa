use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NotificationScope {
    Broadcast,
    Private,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NotificationKind {
    Feeds,
    DirectMessage,
    Event,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WsMessage {
    pub scope: NotificationScope,
    pub kind: NotificationKind,
    pub node_id: Option<String>,
    pub payload: serde_json::Value,
}
