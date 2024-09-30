use std::{collections::HashMap, sync::Arc};

use crate::error::{Error, Result};
use crate::store::ECStore;
use lazy_static::lazy_static;
use time::OffsetDateTime;
use tokio::sync::RwLock;

use super::metadata::{load_bucket_metadata, BucketMetadata};
use super::tags;

lazy_static! {
    pub static ref GLOBAL_BucketMetadataSys: Arc<Option<BucketMetadataSys>> = Arc::new(Some(BucketMetadataSys::new()));
}

pub fn get_bucket_metadata_sys() -> Arc<Option<BucketMetadataSys>> {
    GLOBAL_BucketMetadataSys.clone()
}

#[derive(Debug, Default)]
pub struct BucketMetadataSys {
    metadata_map: RwLock<HashMap<String, BucketMetadata>>,
    api: Option<Arc<ECStore>>,
    initialized: RwLock<bool>,
}

impl BucketMetadataSys {
    fn new() -> Self {
        Self { ..Default::default() }
    }

    pub fn init(&mut self, api: Arc<ECStore>, buckets: Vec<String>) {
        self.api = Some(api);

        unimplemented!()
    }

    async fn reset(&mut self) {
        let mut map = self.metadata_map.write().await;
        map.clear();
    }

    pub async fn get_config(&self, bucket: String) -> Result<(BucketMetadata, bool)> {
        if let Some(api) = self.api.as_ref() {
            let has_bm = {
                let map = self.metadata_map.read().await;
                if let Some(bm) = map.get(&bucket) {
                    Some(bm.clone())
                } else {
                    None
                }
            };

            if let Some(bm) = has_bm {
                return Ok((bm, false));
            } else {
                let bm = match load_bucket_metadata(&api, bucket.as_str()).await {
                    Ok(res) => res,
                    Err(err) => {
                        if *self.initialized.read().await {
                            return Err(Error::msg("errBucketMetadataNotInitialized"));
                        } else {
                            return Err(err);
                        }
                    }
                };

                let mut map = self.metadata_map.write().await;

                map.insert(bucket, bm.clone());

                Ok((bm, true))
            }
        } else {
            Err(Error::msg("errBucketMetadataNotInitialized"))
        }
    }

    pub async fn get_tagging_config(&self, bucket: String) -> Result<(tags::Tags, Option<OffsetDateTime>)> {
        unimplemented!()
    }
}
