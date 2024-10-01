use std::{collections::HashMap, sync::Arc};

use crate::bucket::error::BucketMetadataError;
use crate::bucket::metadata::load_bucket_metadata_parse;
use crate::bucket::utils::is_meta_bucketname;
use crate::config;
use crate::config::error::ConfigError;
use crate::disk::error::DiskError;
use crate::error::{Error, Result};
use crate::global::{is_dist_erasure, is_erasure, new_object_layer_fn};
use crate::store::ECStore;
use futures::future::join_all;
use lazy_static::lazy_static;
use time::OffsetDateTime;
use tokio::sync::RwLock;

use super::metadata::{load_bucket_metadata, BucketMetadata};
use super::tags;

lazy_static! {
    static ref GLOBAL_BucketMetadataSys: Arc<BucketMetadataSys> = Arc::new(BucketMetadataSys::new());
}

pub fn get_bucket_metadata_sys() -> Arc<BucketMetadataSys> {
    GLOBAL_BucketMetadataSys.clone()
}

#[derive(Debug, Default)]
pub struct BucketMetadataSys {
    metadata_map: RwLock<HashMap<String, BucketMetadata>>,
    api: Option<ECStore>,
    initialized: RwLock<bool>,
}

impl BucketMetadataSys {
    fn new() -> Self {
        Self::default()
    }

    pub async fn init(&mut self, api: Option<ECStore>, buckets: Vec<&str>) -> Result<()> {
        if api.is_none() {
            return Err(Error::msg("errServerNotInitialized"));
        }
        self.api = api;

        let _ = self.init_internal(buckets).await;

        Ok(())
    }
    async fn init_internal(&self, buckets: Vec<&str>) -> Result<()> {
        if self.api.is_none() {
            return Err(Error::msg("errServerNotInitialized"));
        }
        let mut futures = Vec::new();
        let mut errs = Vec::new();
        let mut ress = Vec::new();

        for &bucket in buckets.iter() {
            futures.push(load_bucket_metadata(self.api.as_ref().unwrap(), bucket));
        }

        let results = join_all(futures).await;

        for res in results {
            match res {
                Ok(entrys) => {
                    ress.push(Some(entrys));
                    errs.push(None);
                }
                Err(e) => {
                    ress.push(None);
                    errs.push(Some(e));
                }
            }
        }
        unimplemented!()
    }

    async fn concurrent_load(&self, buckets: Vec<&str>) -> Result<Vec<&str>> {
        unimplemented!()
    }

    pub async fn get(&self, bucket: &str) -> Result<BucketMetadata> {
        if is_meta_bucketname(bucket) {
            return Err(Error::new(ConfigError::NotFound));
        }

        let map = self.metadata_map.read().await;
        if let Some(bm) = map.get(bucket) {
            Ok(bm.clone())
        } else {
            Err(Error::new(ConfigError::NotFound))
        }
    }

    pub async fn set(&self, bucket: &str, bm: BucketMetadata) {
        if !is_meta_bucketname(bucket) {
            let mut map = self.metadata_map.write().await;
            map.insert(bucket.to_string(), bm);
        }
    }

    async fn reset(&mut self) {
        let mut map = self.metadata_map.write().await;
        map.clear();
    }

    pub async fn update(&mut self, bucket: &str, config_file: &str, data: Vec<u8>) -> Result<(OffsetDateTime)> {
        self.update_and_parse(bucket, config_file, data, true).await
    }

    async fn update_and_parse(&mut self, bucket: &str, config_file: &str, data: Vec<u8>, parse: bool) -> Result<OffsetDateTime> {
        let layer = new_object_layer_fn();
        let lock = layer.read().await;
        let store = match lock.as_ref() {
            Some(s) => s,
            None => return Err(Error::msg("errServerNotInitialized")),
        };

        if is_meta_bucketname(&bucket) {
            return Err(Error::msg("errInvalidArgument"));
        }

        let mut bm = match load_bucket_metadata_parse(store, &bucket, parse).await {
            Ok(res) => res,
            Err(err) => {
                if !is_erasure().await && !is_dist_erasure().await && DiskError::VolumeNotFound.is(&err) {
                    BucketMetadata::new(&bucket)
                } else {
                    return Err(err);
                }
            }
        };

        let updated = bm.update_config(config_file, data)?;

        bm.save(store).await?;

        Ok(updated)
    }

    pub async fn get_config(&self, bucket: &str) -> Result<(BucketMetadata, bool)> {
        if let Some(api) = self.api.as_ref() {
            let has_bm = {
                let map = self.metadata_map.read().await;
                if let Some(bm) = map.get(&bucket.to_string()) {
                    Some(bm.clone())
                } else {
                    None
                }
            };

            if let Some(bm) = has_bm {
                return Ok((bm, false));
            } else {
                let bm = match load_bucket_metadata(&api, bucket).await {
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

                map.insert(bucket.to_string(), bm.clone());

                Ok((bm, true))
            }
        } else {
            Err(Error::msg("errBucketMetadataNotInitialized"))
        }
    }

    pub async fn get_tagging_config(&self, bucket: &str) -> Result<(tags::Tags, OffsetDateTime)> {
        let bm = match self.get_config(bucket).await {
            Ok((res, _)) => res,
            Err(err) => {
                if config::error::is_not_found(&err) {
                    return Err(Error::new(BucketMetadataError::TaggingNotFound));
                } else {
                    return Err(err);
                }
            }
        };

        if let Some(config) = bm.tagging_config {
            Ok((config, bm.tagging_config_updated_at))
        } else {
            Err(Error::new(BucketMetadataError::TaggingNotFound))
        }
    }
}
