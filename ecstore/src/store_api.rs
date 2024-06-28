use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;
use futures::Stream;

use crate::stream::DynByteStream;

pub struct MakeBucketOptions {
    pub force_create: bool,
}

pub struct PutObjReader {
    // pub stream: Box<dyn Stream<Item = Bytes>>,
}

// impl PutObjReader {
//     pub fn new<S: Stream>(stream: S) -> Self {
//         PutObjReader { stream }
//     }
// }

pub struct ObjectOptions {
    // Use the maximum parity (N/2), used when saving server configuration files
    pub max_parity: bool,
}

#[async_trait::async_trait]
pub trait StorageAPI {
    async fn make_bucket(&self, bucket: &str, opts: &MakeBucketOptions) -> Result<()>;

    async fn put_object(
        &self,
        bucket: &str,
        object: &str,
        data: &PutObjReader,
        opts: &ObjectOptions,
    ) -> Result<()>;
}
