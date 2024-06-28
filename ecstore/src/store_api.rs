use anyhow::Error;

pub trait StorageAPI {
    async fn put_object(&self, bucket: &str, objcet: &str) -> Result<(), Error>;
}
