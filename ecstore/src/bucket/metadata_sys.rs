use std::collections::HashSet;
use std::{collections::HashMap, sync::Arc};

use crate::bucket::error::BucketMetadataError;
use crate::bucket::metadata::load_bucket_metadata_parse;
use crate::bucket::utils::is_meta_bucketname;
use crate::config;
use crate::config::error::ConfigError;
use crate::disk::error::DiskError;
use crate::error::{Error, Result};
use crate::global::{is_dist_erasure, is_erasure, new_object_layer_fn, GLOBAL_Endpoints};
use crate::store::ECStore;
use futures::future::join_all;
use lazy_static::lazy_static;
use time::OffsetDateTime;
use tokio::sync::RwLock;
use tracing::{error, warn};

use super::metadata::{load_bucket_metadata, BucketMetadata};
use super::tags;

lazy_static! {
    static ref GLOBAL_BucketMetadataSys: Arc<RwLock<BucketMetadataSys>> = Arc::new(RwLock::new(BucketMetadataSys::new()));
}

pub async fn init_bucket_metadata_sys(api: ECStore, buckets: Vec<String>) {
    let mut sys = GLOBAL_BucketMetadataSys.write().await;
    sys.init(api, buckets).await
}
pub async fn get_bucket_metadata_sys() -> Arc<RwLock<BucketMetadataSys>> {
    GLOBAL_BucketMetadataSys.clone()
}

pub async fn bucket_metadata_sys_set(bucket: String, bm: BucketMetadata) {
    let sys = GLOBAL_BucketMetadataSys.write().await;
    sys.set(bucket, bm).await
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

    pub async fn init(&mut self, api: ECStore, buckets: Vec<String>) {
        // if api.is_none() {
        //     return Err(Error::msg("errServerNotInitialized"));
        // }
        self.api = Some(api);

        let _ = self.init_internal(buckets).await;
    }
    async fn init_internal(&self, buckets: Vec<String>) -> Result<()> {
        let count = {
            let endpoints = GLOBAL_Endpoints.read().await;
            endpoints.es_count() * 10
        };

        let mut failed_buckets: HashSet<String> = HashSet::new();
        let mut buckets = buckets.as_slice();

        loop {
            if buckets.len() < count {
                self.concurrent_load(buckets, &mut failed_buckets).await;
                break;
            }

            self.concurrent_load(&buckets[..count], &mut failed_buckets).await;

            buckets = &buckets[count..]
        }

        let mut initialized = self.initialized.write().await;
        *initialized = true;

        if is_dist_erasure().await {
            // TODO: refresh_buckets_metadata_loop
        }

        Ok(())
    }

    async fn concurrent_load(&self, buckets: &[String], failed_buckets: &mut HashSet<String>) {
        let mut futures = Vec::new();

        for bucket in buckets.iter() {
            futures.push(load_bucket_metadata(self.api.as_ref().unwrap(), bucket.as_str()));
        }

        let results = join_all(futures).await;

        let mut idx = 0;

        let mut mp = self.metadata_map.write().await;

        for res in results {
            match res {
                Ok(res) => {
                    if let Some(bucket) = buckets.get(idx) {
                        mp.insert(bucket.clone(), res);
                    }
                }
                Err(e) => {
                    error!("Unable to load bucket metadata, will be retried: {:?}", e);
                    if let Some(bucket) = buckets.get(idx) {
                        failed_buckets.insert(bucket.clone());
                    }
                }
            }

            idx += 1;
        }
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

    pub async fn set(&self, bucket: String, bm: BucketMetadata) {
        if !is_meta_bucketname(&bucket) {
            let mut map = self.metadata_map.write().await;
            map.insert(bucket, bm);
        }
    }

    // async fn reset(&mut self) {
    //     let mut map = self.metadata_map.write().await;
    //     map.clear();
    // }

    pub async fn update(&mut self, bucket: &str, config_file: &str, data: Vec<u8>) -> Result<OffsetDateTime> {
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

        self.save(&mut bm).await?;

        Ok(updated)
    }

    async fn save(&self, bm: &mut BucketMetadata) -> Result<()> {
        let layer = new_object_layer_fn();
        let lock = layer.read().await;
        let store = match lock.as_ref() {
            Some(s) => s,
            None => return Err(Error::msg("errServerNotInitialized")),
        };

        if is_meta_bucketname(&bm.name) {
            return Err(Error::msg("errInvalidArgument"));
        }

        bm.save(store).await?;

        self.set(bm.name.clone(), bm.clone()).await;

        Ok(())
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
                warn!("get_tagging_config err {:?}", &err);
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
