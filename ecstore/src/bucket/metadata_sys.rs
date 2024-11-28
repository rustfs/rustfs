use std::collections::HashSet;
use std::sync::OnceLock;
use std::{collections::HashMap, sync::Arc};

use crate::bucket::error::BucketMetadataError;
use crate::bucket::metadata::{load_bucket_metadata_parse, BUCKET_LIFECYCLE_CONFIG};
use crate::bucket::utils::is_meta_bucketname;
use crate::config;
use crate::config::error::ConfigError;
use crate::disk::error::DiskError;
use crate::error::{Error, Result};
use crate::global::{is_dist_erasure, is_erasure, new_object_layer_fn, GLOBAL_Endpoints};
use crate::store::ECStore;
use crate::utils::xml::deserialize;
use futures::future::join_all;
use s3s::dto::{
    BucketLifecycleConfiguration, NotificationConfiguration, ObjectLockConfiguration, ReplicationConfiguration,
    ServerSideEncryptionConfiguration, Tagging, VersioningConfiguration,
};
use time::OffsetDateTime;
use tokio::sync::RwLock;
use tracing::{error, warn};

use super::metadata::{load_bucket_metadata, BucketMetadata};
use super::policy::bucket_policy::BucketPolicy;
use super::quota::BucketQuota;
use super::target::BucketTargets;

use lazy_static::lazy_static;

lazy_static! {
    pub static ref GLOBAL_BucketMetadataSys: OnceLock<Arc<RwLock<BucketMetadataSys>>> = OnceLock::new();
}

pub async fn init_bucket_metadata_sys(api: Arc<ECStore>, buckets: Vec<String>) {
    let mut sys = BucketMetadataSys::new(api);
    sys.init(buckets).await;

    let sys = Arc::new(RwLock::new(sys));

    GLOBAL_BucketMetadataSys.set(sys).unwrap();
}

// panic if not init
pub(super) fn get_bucket_metadata_sys() -> Arc<RwLock<BucketMetadataSys>> {
    GLOBAL_BucketMetadataSys.get().unwrap().clone()
}

pub async fn set_bucket_metadata(bucket: String, bm: BucketMetadata) {
    let sys = get_bucket_metadata_sys();
    let lock = sys.write().await;
    lock.set(bucket, Arc::new(bm)).await;
}

pub(crate) async fn get(bucket: &str) -> Result<Arc<BucketMetadata>> {
    let sys = get_bucket_metadata_sys();
    let lock = sys.read().await;
    lock.get(bucket).await
}

pub async fn update(bucket: &str, config_file: &str, data: Vec<u8>) -> Result<OffsetDateTime> {
    let bucket_meta_sys_lock = get_bucket_metadata_sys();
    let mut bucket_meta_sys = bucket_meta_sys_lock.write().await;

    bucket_meta_sys.update(bucket, config_file, data).await
}

pub async fn delete(bucket: &str, config_file: &str) -> Result<OffsetDateTime> {
    let bucket_meta_sys_lock = get_bucket_metadata_sys();
    let mut bucket_meta_sys = bucket_meta_sys_lock.write().await;

    bucket_meta_sys.delete(bucket, config_file).await
}

pub async fn get_tagging_config(bucket: &str) -> Result<(Tagging, OffsetDateTime)> {
    let bucket_meta_sys_lock = get_bucket_metadata_sys();
    let bucket_meta_sys = bucket_meta_sys_lock.read().await;

    bucket_meta_sys.get_tagging_config(bucket).await
}

pub async fn get_lifecycle_config(bucket: &str) -> Result<(BucketLifecycleConfiguration, OffsetDateTime)> {
    let bucket_meta_sys_lock = get_bucket_metadata_sys();
    let bucket_meta_sys = bucket_meta_sys_lock.read().await;

    bucket_meta_sys.get_lifecycle_config(bucket).await
}

pub async fn get_sse_config(bucket: &str) -> Result<(ServerSideEncryptionConfiguration, OffsetDateTime)> {
    let bucket_meta_sys_lock = get_bucket_metadata_sys();
    let bucket_meta_sys = bucket_meta_sys_lock.read().await;

    bucket_meta_sys.get_sse_config(bucket).await
}

pub async fn get_object_lock_config(bucket: &str) -> Result<(ObjectLockConfiguration, OffsetDateTime)> {
    let bucket_meta_sys_lock = get_bucket_metadata_sys();
    let bucket_meta_sys = bucket_meta_sys_lock.read().await;

    bucket_meta_sys.get_object_lock_config(bucket).await
}

pub async fn get_replication_config(bucket: &str) -> Result<(ReplicationConfiguration, OffsetDateTime)> {
    let bucket_meta_sys_lock = get_bucket_metadata_sys();
    let bucket_meta_sys = bucket_meta_sys_lock.read().await;

    bucket_meta_sys.get_replication_config(bucket).await
}

pub async fn get_notification_config(bucket: &str) -> Result<Option<NotificationConfiguration>> {
    let bucket_meta_sys_lock = get_bucket_metadata_sys();
    let bucket_meta_sys = bucket_meta_sys_lock.read().await;

    bucket_meta_sys.get_notification_config(bucket).await
}

pub async fn get_versioning_config(bucket: &str) -> Result<(VersioningConfiguration, OffsetDateTime)> {
    let bucket_meta_sys_lock = get_bucket_metadata_sys();
    let bucket_meta_sys = bucket_meta_sys_lock.read().await;

    bucket_meta_sys.get_versioning_config(bucket).await
}

pub async fn get_config_from_disk(bucket: &str) -> Result<BucketMetadata> {
    let bucket_meta_sys_lock = get_bucket_metadata_sys();
    let bucket_meta_sys = bucket_meta_sys_lock.read().await;

    bucket_meta_sys.get_config_from_disk(bucket).await
}

pub async fn created_at(bucket: &str) -> Result<OffsetDateTime> {
    let bucket_meta_sys_lock = get_bucket_metadata_sys();
    let bucket_meta_sys = bucket_meta_sys_lock.read().await;

    bucket_meta_sys.created_at(bucket).await
}

#[derive(Debug)]
pub struct BucketMetadataSys {
    metadata_map: RwLock<HashMap<String, Arc<BucketMetadata>>>,
    api: Arc<ECStore>,
    initialized: RwLock<bool>,
}

impl BucketMetadataSys {
    pub fn new(api: Arc<ECStore>) -> Self {
        Self {
            metadata_map: RwLock::new(HashMap::new()),
            api,
            initialized: RwLock::new(false),
        }
    }

    pub async fn init(&mut self, buckets: Vec<String>) {
        let _ = self.init_internal(buckets).await;
    }
    async fn init_internal(&self, buckets: Vec<String>) -> Result<()> {
        let count = {
            if let Some(endpoints) = GLOBAL_Endpoints.get() {
                endpoints.es_count() * 10
            } else {
                return Err(Error::msg("GLOBAL_Endpoints not init"));
            }
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
            futures.push(load_bucket_metadata(self.api.clone(), bucket.as_str()));
        }

        let results = join_all(futures).await;

        let mut idx = 0;

        let mut mp = self.metadata_map.write().await;

        for res in results {
            match res {
                Ok(res) => {
                    if let Some(bucket) = buckets.get(idx) {
                        mp.insert(bucket.clone(), Arc::new(res));
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

    pub async fn get(&self, bucket: &str) -> Result<Arc<BucketMetadata>> {
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

    pub async fn set(&self, bucket: String, bm: Arc<BucketMetadata>) {
        if !is_meta_bucketname(&bucket) {
            let mut map = self.metadata_map.write().await;
            map.insert(bucket, bm);
        }
    }

    async fn _reset(&mut self) {
        let mut map = self.metadata_map.write().await;
        map.clear();
    }

    pub async fn update(&mut self, bucket: &str, config_file: &str, data: Vec<u8>) -> Result<OffsetDateTime> {
        self.update_and_parse(bucket, config_file, data, true).await
    }

    pub async fn delete(&mut self, bucket: &str, config_file: &str) -> Result<OffsetDateTime> {
        if config_file == BUCKET_LIFECYCLE_CONFIG {
            let meta = match self.get_config_from_disk(bucket).await {
                Ok(res) => res,
                Err(err) => {
                    if !config::error::is_not_found(&err) {
                        return Err(err);
                    } else {
                        BucketMetadata::new(bucket)
                    }
                }
            };

            if !meta.lifecycle_config_xml.is_empty() {
                let cfg = deserialize::<BucketLifecycleConfiguration>(&meta.lifecycle_config_xml)?;
                // TODO: FIXME:
                // for _v in cfg.rules.iter() {
                //     break;
                // }
                if let Some(_v) = cfg.rules.first() {}
            }

            // TODO: other lifecycle handle
        }

        self.update_and_parse(bucket, config_file, Vec::new(), false).await
    }

    async fn update_and_parse(&mut self, bucket: &str, config_file: &str, data: Vec<u8>, parse: bool) -> Result<OffsetDateTime> {
        let Some(store) = new_object_layer_fn() else { return Err(Error::msg("errServerNotInitialized")) };

        if is_meta_bucketname(bucket) {
            return Err(Error::msg("errInvalidArgument"));
        }

        let mut bm = match load_bucket_metadata_parse(store, bucket, parse).await {
            Ok(res) => res,
            Err(err) => {
                if !is_erasure().await && !is_dist_erasure().await && DiskError::VolumeNotFound.is(&err) {
                    BucketMetadata::new(bucket)
                } else {
                    return Err(err);
                }
            }
        };

        let updated = bm.update_config(config_file, data)?;

        self.save(bm).await?;

        Ok(updated)
    }

    async fn save(&self, bm: BucketMetadata) -> Result<()> {
        if is_meta_bucketname(&bm.name) {
            return Err(Error::msg("errInvalidArgument"));
        }

        let mut bm = bm;

        bm.save().await?;

        self.set(bm.name.clone(), Arc::new(bm)).await;

        Ok(())
    }

    pub async fn get_config_from_disk(&self, bucket: &str) -> Result<BucketMetadata> {
        if is_meta_bucketname(bucket) {
            return Err(Error::msg("errInvalidArgument"));
        }

        load_bucket_metadata(self.api.clone(), bucket).await
    }

    pub async fn get_config(&self, bucket: &str) -> Result<(Arc<BucketMetadata>, bool)> {
        let has_bm = {
            let map = self.metadata_map.read().await;
            map.get(&bucket.to_string()).cloned()
        };

        if let Some(bm) = has_bm {
            Ok((bm, false))
        } else {
            let bm = match load_bucket_metadata(self.api.clone(), bucket).await {
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

            let bm = Arc::new(bm);
            map.insert(bucket.to_string(), bm.clone());

            Ok((bm, true))
        }
    }

    pub async fn get_versioning_config(&self, bucket: &str) -> Result<(VersioningConfiguration, OffsetDateTime)> {
        let bm = match self.get_config(bucket).await {
            Ok((res, _)) => res,
            Err(err) => {
                warn!("get_versioning_config err {:?}", &err);
                if config::error::is_not_found(&err) {
                    return Ok((VersioningConfiguration::default(), OffsetDateTime::UNIX_EPOCH));
                } else {
                    return Err(err);
                }
            }
        };

        if let Some(config) = &bm.versioning_config {
            Ok((config.clone(), bm.versioning_config_updated_at))
        } else {
            Ok((VersioningConfiguration::default(), bm.versioning_config_updated_at))
        }
    }

    pub async fn get_bucket_policy(&self, bucket: &str) -> Result<(BucketPolicy, OffsetDateTime)> {
        let bm = match self.get_config(bucket).await {
            Ok((res, _)) => res,
            Err(err) => {
                warn!("get_bucket_policy err {:?}", &err);
                if config::error::is_not_found(&err) {
                    return Err(Error::new(BucketMetadataError::BucketPolicyNotFound));
                } else {
                    return Err(err);
                }
            }
        };

        if let Some(config) = &bm.policy_config {
            Ok((config.clone(), bm.policy_config_updated_at))
        } else {
            Err(Error::new(BucketMetadataError::BucketPolicyNotFound))
        }
    }

    pub async fn get_tagging_config(&self, bucket: &str) -> Result<(Tagging, OffsetDateTime)> {
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

        if let Some(config) = &bm.tagging_config {
            Ok((config.clone(), bm.tagging_config_updated_at))
        } else {
            Err(Error::new(BucketMetadataError::TaggingNotFound))
        }
    }

    pub async fn get_object_lock_config(&self, bucket: &str) -> Result<(ObjectLockConfiguration, OffsetDateTime)> {
        let bm = match self.get_config(bucket).await {
            Ok((res, _)) => res,
            Err(err) => {
                warn!("get_object_lock_config err {:?}", &err);
                if config::error::is_not_found(&err) {
                    return Err(Error::new(BucketMetadataError::BucketObjectLockConfigNotFound));
                } else {
                    return Err(err);
                }
            }
        };

        if let Some(config) = &bm.object_lock_config {
            Ok((config.clone(), bm.object_lock_config_updated_at))
        } else {
            Err(Error::new(BucketMetadataError::BucketObjectLockConfigNotFound))
        }
    }

    pub async fn get_lifecycle_config(&self, bucket: &str) -> Result<(BucketLifecycleConfiguration, OffsetDateTime)> {
        let bm = match self.get_config(bucket).await {
            Ok((res, _)) => res,
            Err(err) => {
                warn!("get_lifecycle_config err {:?}", &err);
                if config::error::is_not_found(&err) {
                    return Err(Error::new(BucketMetadataError::BucketLifecycleNotFound));
                } else {
                    return Err(err);
                }
            }
        };

        if let Some(config) = &bm.lifecycle_config {
            if config.rules.is_empty() {
                Err(Error::new(BucketMetadataError::BucketLifecycleNotFound))
            } else {
                Ok((config.clone(), bm.lifecycle_config_updated_at))
            }
        } else {
            Err(Error::new(BucketMetadataError::BucketLifecycleNotFound))
        }
    }

    pub async fn get_notification_config(&self, bucket: &str) -> Result<Option<NotificationConfiguration>> {
        let bm = match self.get_config(bucket).await {
            Ok((bm, _)) => bm.notification_config.clone(),
            Err(err) => {
                warn!("get_notification_config err {:?}", &err);
                if config::error::is_not_found(&err) {
                    None
                } else {
                    return Err(err);
                }
            }
        };

        Ok(bm)
    }

    pub async fn get_sse_config(&self, bucket: &str) -> Result<(ServerSideEncryptionConfiguration, OffsetDateTime)> {
        let bm = match self.get_config(bucket).await {
            Ok((res, _)) => res,
            Err(err) => {
                warn!("get_sse_config err {:?}", &err);
                if config::error::is_not_found(&err) {
                    return Err(Error::new(BucketMetadataError::BucketSSEConfigNotFound));
                } else {
                    return Err(err);
                }
            }
        };

        if let Some(config) = &bm.sse_config {
            Ok((config.clone(), bm.encryption_config_updated_at))
        } else {
            Err(Error::new(BucketMetadataError::BucketSSEConfigNotFound))
        }
    }

    pub async fn created_at(&self, bucket: &str) -> Result<OffsetDateTime> {
        let bm = match self.get_config(bucket).await {
            Ok((bm, _)) => bm.created,
            Err(err) => {
                return Err(err);
            }
        };

        Ok(bm)
    }

    pub async fn get_quota_config(&self, bucket: &str) -> Result<(BucketQuota, OffsetDateTime)> {
        let bm = match self.get_config(bucket).await {
            Ok((res, _)) => res,
            Err(err) => {
                warn!("get_quota_config err {:?}", &err);
                if config::error::is_not_found(&err) {
                    return Err(Error::new(BucketMetadataError::BucketQuotaConfigNotFound));
                } else {
                    return Err(err);
                }
            }
        };

        if let Some(config) = &bm.quota_config {
            Ok((config.clone(), bm.quota_config_updated_at))
        } else {
            Err(Error::new(BucketMetadataError::BucketQuotaConfigNotFound))
        }
    }

    pub async fn get_replication_config(&self, bucket: &str) -> Result<(ReplicationConfiguration, OffsetDateTime)> {
        let (bm, reload) = match self.get_config(bucket).await {
            Ok(res) => res,
            Err(err) => {
                warn!("get_replication_config err {:?}", &err);
                if config::error::is_not_found(&err) {
                    return Err(Error::new(BucketMetadataError::BucketReplicationConfigNotFound));
                } else {
                    return Err(err);
                }
            }
        };

        if let Some(config) = &bm.replication_config {
            if reload {
                // TODO: globalBucketTargetSys
            }

            Ok((config.clone(), bm.replication_config_updated_at))
        } else {
            Err(Error::new(BucketMetadataError::BucketReplicationConfigNotFound))
        }
    }

    pub async fn get_bucket_targets_config(&self, bucket: &str) -> Result<BucketTargets> {
        let (bm, reload) = match self.get_config(bucket).await {
            Ok(res) => res,
            Err(err) => {
                warn!("get_replication_config err {:?}", &err);
                if config::error::is_not_found(&err) {
                    return Err(Error::new(BucketMetadataError::BucketRemoteTargetNotFound));
                } else {
                    return Err(err);
                }
            }
        };

        if let Some(config) = &bm.bucket_target_config {
            if reload {
                // TODO: globalBucketTargetSys
            }

            Ok(config.clone())
        } else {
            Err(Error::new(BucketMetadataError::BucketRemoteTargetNotFound))
        }
    }
}
