use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MessageScope {
    Broadcast,
    Private,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MessageKind {
    Feeds,
    DirectMessage,
    Event,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WsMessage {
    pub scope: MessageScope,
    pub kind: MessageKind,
    pub node_id: Option<String>,
    pub payload: serde_json::Value,
}
