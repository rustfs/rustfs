#[async_trait::async_trait]
pub trait Coordinator: Send + Sync {
    async fn ping();
    async fn create_bucket() -> Result<>
}