use crate::models::WsMessage;
use async_trait::async_trait;

#[async_trait]
pub trait WsSession: Send + Sync + 'static {
    type Error: std::error::Error + Send + Sync + 'static;

    async fn send(&self, event: WsMessage) -> Result<(), Self::Error>;
}
