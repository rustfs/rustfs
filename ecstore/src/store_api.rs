use anyhow::Result;

pub struct MakeBucketOptions {}

pub trait StorageAPI {
    async fn make_bucket(&self, bucket: &str, opts: &MakeBucketOptions) -> Result<()>;

    async fn put_object(&self, bucket: &str, objcet: &str) -> Result<()>;
}
