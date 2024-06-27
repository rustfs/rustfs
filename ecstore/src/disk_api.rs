use std::fmt::Debug;

use anyhow::Error;
use bytes::Bytes;

#[async_trait::async_trait]
pub trait DiskAPI: Debug + Send + Sync + 'static {
    async fn read_all(&self, volume: &str, path: &str) -> Result<Bytes, Error>;
    async fn write_all(&self, volume: &str, path: &str, data: Bytes) -> Result<(), Error>;
    async fn rename_file(
        &self,
        src_volume: &str,
        src_path: &str,
        dst_volume: &str,
        dst_path: &str,
    ) -> Result<(), Error>;

    async fn make_volumes(&self, volume: Vec<&str>) -> Result<(), Error>;
    async fn make_volume(&self, volume: &str) -> Result<(), Error>;
}
