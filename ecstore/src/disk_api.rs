use std::fmt::Debug;

use anyhow::Result;
use bytes::Bytes;

#[async_trait::async_trait]
pub trait DiskAPI: Debug + Send + Sync + 'static {
    fn is_local(&self) -> bool;

    async fn read_all(&self, volume: &str, path: &str) -> Result<Bytes>;
    async fn write_all(&self, volume: &str, path: &str, data: Bytes) -> Result<()>;
    async fn rename_file(
        &self,
        src_volume: &str,
        src_path: &str,
        dst_volume: &str,
        dst_path: &str,
    ) -> Result<()>;

    async fn make_volumes(&self, volume: Vec<&str>) -> Result<()>;
    async fn make_volume(&self, volume: &str) -> Result<()>;
}
