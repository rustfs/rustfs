// Copyright 2024 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use super::metadata::{BucketMetadata, load_bucket_metadata};
use super::quota::BucketQuota;
use super::target::BucketTargets;
use crate::bucket::bucket_target_sys::BucketTargetSys;
use crate::bucket::metadata::load_bucket_metadata_parse;
use crate::bucket::utils::is_meta_bucketname;
use crate::error::{Error, Result, is_err_bucket_not_found};
use crate::runtime::sources as runtime_sources;
use crate::storage_api_contracts::heal::HealOperations as _;
use crate::store::ECStore;
use futures::future::join_all;
use lazy_static::lazy_static;
use rustfs_common::heal_channel::HealOpts;
use rustfs_policy::policy::BucketPolicy;
use s3s::dto::ReplicationConfiguration;
use s3s::dto::{
    AccelerateConfiguration, BucketLifecycleConfiguration, BucketLoggingStatus, CORSConfiguration, NotificationConfiguration,
    ObjectLockConfiguration, PublicAccessBlockConfiguration, RequestPaymentConfiguration, ServerSideEncryptionConfiguration,
    Tagging, VersioningConfiguration, WebsiteConfiguration,
};
use std::collections::HashSet;
use std::sync::OnceLock;
use std::time::Duration;
use std::{collections::HashMap, sync::Arc};
use time::OffsetDateTime;
use tokio::sync::RwLock;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tracing::{error, warn};

lazy_static! {
    pub static ref GLOBAL_BUCKET_METADATA_SYS: OnceLock<Arc<RwLock<BucketMetadataSys>>> = OnceLock::new();
}

const BUCKET_METADATA_REFRESH_INTERVAL: Duration = Duration::from_secs(15 * 60);

pub async fn init_bucket_metadata_sys(api: Arc<ECStore>, buckets: Vec<String>) {
    let mut sys = BucketMetadataSys::new(api);
    sys.init(buckets).await;

    let sys = Arc::new(RwLock::new(sys));

    GLOBAL_BUCKET_METADATA_SYS.set(sys.clone()).unwrap();

    if runtime_sources::setup_is_dist_erasure().await {
        start_refresh_buckets_metadata_loop(sys);
    }
}

pub fn get_global_bucket_metadata_sys() -> Option<Arc<RwLock<BucketMetadataSys>>> {
    GLOBAL_BUCKET_METADATA_SYS.get().cloned()
}

// panic if not init
pub(super) fn get_bucket_metadata_sys() -> Result<Arc<RwLock<BucketMetadataSys>>> {
    if let Some(sys) = GLOBAL_BUCKET_METADATA_SYS.get() {
        Ok(sys.clone())
    } else {
        Err(Error::other("GLOBAL_BUCKET_METADATA_SYS not init"))
    }
}

pub async fn set_bucket_metadata(bucket: String, bm: BucketMetadata) -> Result<()> {
    let sys = get_bucket_metadata_sys()?;
    let lock = sys.write().await;
    lock.set(bucket, Arc::new(bm)).await;
    Ok(())
}

/// Drop a bucket's cached metadata from the in-memory map.
///
/// This is the counterpart to [`set_bucket_metadata`] and is invoked when a
/// bucket is deleted so peers stop serving stale cached configuration for it.
/// Returns `true` if an entry was present.
pub async fn remove_bucket_metadata(bucket: &str) -> Result<bool> {
    let sys = get_bucket_metadata_sys()?;
    let lock = sys.read().await;
    Ok(lock.remove(bucket).await)
}

fn start_refresh_buckets_metadata_loop(sys: Arc<RwLock<BucketMetadataSys>>) {
    let Some(cancel_token) = runtime_sources::background_services_cancel_token().cloned() else {
        warn!("bucket metadata refresh loop skipped because background cancellation token is not initialized");
        return;
    };

    tokio::spawn(async move {
        refresh_buckets_metadata_loop(sys, cancel_token).await;
    });
}

async fn refresh_buckets_metadata_loop(sys: Arc<RwLock<BucketMetadataSys>>, cancel_token: CancellationToken) {
    loop {
        if !wait_refresh_interval_or_cancel(&cancel_token, BUCKET_METADATA_REFRESH_INTERVAL).await {
            break;
        }
        refresh_buckets_metadata_once(sys.clone()).await;
    }
}

async fn wait_refresh_interval_or_cancel(cancel_token: &CancellationToken, interval: Duration) -> bool {
    tokio::select! {
        _ = cancel_token.cancelled() => false,
        _ = sleep(interval) => true,
    }
}

async fn refresh_buckets_metadata_once(sys: Arc<RwLock<BucketMetadataSys>>) {
    let buckets = {
        let sys = sys.read().await;
        sys.bucket_names().await
    };
    if buckets.is_empty() {
        return;
    }

    let count = runtime_sources::endpoint_erasure_set_count()
        .map(|count| count * 10)
        .unwrap_or(10)
        .max(1);
    let mut failed_buckets = HashSet::new();

    for chunk in buckets.chunks(count) {
        let sys = sys.read().await;
        sys.concurrent_load(chunk, &mut failed_buckets).await;
    }

    if !failed_buckets.is_empty() {
        warn!(
            failed_bucket_count = failed_buckets.len(),
            "bucket metadata refresh loop left buckets queued for retry"
        );
    }
}

async fn sync_bucket_target_sys(bucket: &str, bm: &BucketMetadata) {
    BucketTargetSys::get()
        .update_all_targets(bucket, bm.bucket_target_config.as_ref())
        .await;
}

pub async fn get(bucket: &str) -> Result<Arc<BucketMetadata>> {
    let sys = get_bucket_metadata_sys()?;
    let lock = sys.read().await;
    lock.get(bucket).await
}

pub async fn update(bucket: &str, config_file: &str, data: Vec<u8>) -> Result<OffsetDateTime> {
    let bucket_meta_sys_lock = get_bucket_metadata_sys()?;
    let mut bucket_meta_sys = bucket_meta_sys_lock.write().await;

    bucket_meta_sys.update(bucket, config_file, data).await
}

pub async fn delete(bucket: &str, config_file: &str) -> Result<OffsetDateTime> {
    let bucket_meta_sys_lock = get_bucket_metadata_sys()?;
    let mut bucket_meta_sys = bucket_meta_sys_lock.write().await;

    bucket_meta_sys.delete(bucket, config_file).await
}

pub async fn get_bucket_policy(bucket: &str) -> Result<(BucketPolicy, OffsetDateTime)> {
    let bucket_meta_sys_lock = get_bucket_metadata_sys()?;
    let bucket_meta_sys = bucket_meta_sys_lock.read().await;

    bucket_meta_sys.get_bucket_policy(bucket).await
}

/// Returns the raw JSON string of the bucket policy as originally stored.
/// This preserves the exact format of the policy document as it was PUT.
pub async fn get_bucket_policy_raw(bucket: &str) -> Result<(String, OffsetDateTime)> {
    let bucket_meta_sys_lock = get_bucket_metadata_sys()?;
    let bucket_meta_sys = bucket_meta_sys_lock.read().await;

    bucket_meta_sys.get_bucket_policy_raw(bucket).await
}

pub async fn get_bucket_acl_config(bucket: &str) -> Result<(String, OffsetDateTime)> {
    let bucket_meta_sys_lock = get_bucket_metadata_sys()?;
    let bucket_meta_sys = bucket_meta_sys_lock.read().await;

    bucket_meta_sys.get_bucket_acl_config(bucket).await
}

pub async fn get_quota_config(bucket: &str) -> Result<(BucketQuota, OffsetDateTime)> {
    let bucket_meta_sys_lock = get_bucket_metadata_sys()?;
    let bucket_meta_sys = bucket_meta_sys_lock.read().await;

    bucket_meta_sys.get_quota_config(bucket).await
}

pub async fn get_bucket_targets_config(bucket: &str) -> Result<BucketTargets> {
    let bucket_meta_sys_lock = get_bucket_metadata_sys()?;
    let bucket_meta_sys = bucket_meta_sys_lock.read().await;

    bucket_meta_sys.get_bucket_targets_config(bucket).await
}

pub async fn get_cors_config(bucket: &str) -> Result<(CORSConfiguration, OffsetDateTime)> {
    let bucket_meta_sys_lock = get_bucket_metadata_sys()?;
    let bucket_meta_sys = bucket_meta_sys_lock.read().await;

    bucket_meta_sys.get_cors_config(bucket).await
}

pub async fn get_tagging_config(bucket: &str) -> Result<(Tagging, OffsetDateTime)> {
    let bucket_meta_sys_lock = get_bucket_metadata_sys()?;
    let bucket_meta_sys = bucket_meta_sys_lock.read().await;

    bucket_meta_sys.get_tagging_config(bucket).await
}

pub async fn get_public_access_block_config(bucket: &str) -> Result<(PublicAccessBlockConfiguration, OffsetDateTime)> {
    let bucket_meta_sys_lock = get_bucket_metadata_sys()?;
    let bucket_meta_sys = bucket_meta_sys_lock.read().await;

    bucket_meta_sys.get_public_access_block_config(bucket).await
}

pub async fn get_lifecycle_config(bucket: &str) -> Result<(BucketLifecycleConfiguration, OffsetDateTime)> {
    let bucket_meta_sys_lock = get_bucket_metadata_sys()?;
    let bucket_meta_sys = bucket_meta_sys_lock.read().await;

    bucket_meta_sys.get_lifecycle_config(bucket).await
}

pub async fn get_sse_config(bucket: &str) -> Result<(ServerSideEncryptionConfiguration, OffsetDateTime)> {
    let bucket_meta_sys_lock = get_bucket_metadata_sys()?;
    let bucket_meta_sys = bucket_meta_sys_lock.read().await;

    bucket_meta_sys.get_sse_config(bucket).await
}

pub async fn get_object_lock_config(bucket: &str) -> Result<(ObjectLockConfiguration, OffsetDateTime)> {
    let bucket_meta_sys_lock = get_bucket_metadata_sys()?;
    let bucket_meta_sys = bucket_meta_sys_lock.read().await;

    bucket_meta_sys.get_object_lock_config(bucket).await
}

pub async fn get_replication_config(bucket: &str) -> Result<(ReplicationConfiguration, OffsetDateTime)> {
    let bucket_meta_sys_lock = get_bucket_metadata_sys()?;
    let bucket_meta_sys = bucket_meta_sys_lock.read().await;

    bucket_meta_sys.get_replication_config(bucket).await
}

pub async fn get_notification_config(bucket: &str) -> Result<Option<NotificationConfiguration>> {
    let bucket_meta_sys_lock = get_bucket_metadata_sys()?;
    let bucket_meta_sys = bucket_meta_sys_lock.read().await;

    bucket_meta_sys.get_notification_config(bucket).await
}

pub async fn get_versioning_config(bucket: &str) -> Result<(VersioningConfiguration, OffsetDateTime)> {
    let bucket_meta_sys_lock = get_bucket_metadata_sys()?;
    let bucket_meta_sys = bucket_meta_sys_lock.read().await;

    bucket_meta_sys.get_versioning_config(bucket).await
}

pub async fn get_website_config(bucket: &str) -> Result<(WebsiteConfiguration, OffsetDateTime)> {
    let bucket_meta_sys_lock = get_bucket_metadata_sys()?;
    let bucket_meta_sys = bucket_meta_sys_lock.read().await;

    bucket_meta_sys.get_website_config(bucket).await
}

pub async fn get_logging_config(bucket: &str) -> Result<(BucketLoggingStatus, OffsetDateTime)> {
    let bucket_meta_sys_lock = get_bucket_metadata_sys()?;
    let bucket_meta_sys = bucket_meta_sys_lock.read().await;

    bucket_meta_sys.get_logging_config(bucket).await
}

pub async fn get_accelerate_config(bucket: &str) -> Result<(AccelerateConfiguration, OffsetDateTime)> {
    let bucket_meta_sys_lock = get_bucket_metadata_sys()?;
    let bucket_meta_sys = bucket_meta_sys_lock.read().await;

    bucket_meta_sys.get_accelerate_config(bucket).await
}

pub async fn get_request_payment_config(bucket: &str) -> Result<(RequestPaymentConfiguration, OffsetDateTime)> {
    let bucket_meta_sys_lock = get_bucket_metadata_sys()?;
    let bucket_meta_sys = bucket_meta_sys_lock.read().await;

    bucket_meta_sys.get_request_payment_config(bucket).await
}

pub async fn get_config_from_disk(bucket: &str) -> Result<BucketMetadata> {
    let bucket_meta_sys_lock = get_bucket_metadata_sys()?;
    let bucket_meta_sys = bucket_meta_sys_lock.read().await;

    bucket_meta_sys.get_config_from_disk(bucket).await
}

pub async fn created_at(bucket: &str) -> Result<OffsetDateTime> {
    let bucket_meta_sys_lock = get_bucket_metadata_sys()?;
    let bucket_meta_sys = bucket_meta_sys_lock.read().await;

    bucket_meta_sys.created_at(bucket).await
}

pub async fn list_bucket_targets(bucket: &str) -> Result<BucketTargets> {
    let bucket_meta_sys_lock = get_bucket_metadata_sys()?;
    let bucket_meta_sys = bucket_meta_sys_lock.read().await;

    bucket_meta_sys.get_bucket_targets_config(bucket).await
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
        let count = runtime_sources::endpoint_erasure_set_count()
            .map(|count| count * 10)
            .ok_or_else(|| Error::other("endpoint pools not initialized"))?;

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

        Ok(())
    }

    async fn concurrent_load(&self, buckets: &[String], failed_buckets: &mut HashSet<String>) {
        let mut futures = Vec::new();

        for bucket in buckets.iter() {
            let api = self.api.clone();
            let bucket = bucket.clone();
            futures.push(async move {
                sleep(Duration::from_millis(30)).await;
                let _ = api
                    .heal_bucket(
                        &bucket,
                        &HealOpts {
                            recreate: true,
                            ..Default::default()
                        },
                    )
                    .await;
                load_bucket_metadata(self.api.clone(), bucket.as_str()).await
            });
        }

        let results = join_all(futures).await;

        for (idx, res) in results.into_iter().enumerate() {
            match res {
                Ok(res) => {
                    if let Some(bucket) = buckets.get(idx) {
                        self.set(bucket.clone(), Arc::new(res)).await;
                    }
                }
                Err(e) => {
                    error!("Unable to load bucket metadata, will be retried: {:?}", e);
                    if let Some(bucket) = buckets.get(idx) {
                        failed_buckets.insert(bucket.clone());
                    }
                }
            }
        }
    }

    pub async fn get(&self, bucket: &str) -> Result<Arc<BucketMetadata>> {
        if is_meta_bucketname(bucket) {
            return Err(Error::ConfigNotFound);
        }

        let map = self.metadata_map.read().await;
        if let Some(bm) = map.get(bucket) {
            Ok(bm.clone())
        } else {
            Err(Error::ConfigNotFound)
        }
    }

    pub async fn set(&self, bucket: String, bm: Arc<BucketMetadata>) {
        if !is_meta_bucketname(&bucket) {
            let mut map = self.metadata_map.write().await;
            map.insert(bucket.clone(), bm.clone());
            drop(map);
            sync_bucket_target_sys(&bucket, &bm).await;
        }
    }

    /// Remove a bucket's cached metadata from the in-memory map.
    ///
    /// Returns `true` if an entry was present. Reserved meta buckets are ignored.
    pub async fn remove(&self, bucket: &str) -> bool {
        if is_meta_bucketname(bucket) {
            return false;
        }
        let mut map = self.metadata_map.write().await;
        let removed = map.remove(bucket).is_some();
        drop(map);
        if removed {
            BucketTargetSys::get().delete(bucket).await;
        }
        removed
    }

    async fn _reset(&mut self) {
        let mut map = self.metadata_map.write().await;
        map.clear();
    }

    pub async fn update(&mut self, bucket: &str, config_file: &str, data: Vec<u8>) -> Result<OffsetDateTime> {
        self.update_and_parse(bucket, config_file, data, true).await
    }

    pub async fn delete(&mut self, bucket: &str, config_file: &str) -> Result<OffsetDateTime> {
        self.update_and_parse(bucket, config_file, Vec::new(), false).await
    }

    async fn update_and_parse(&mut self, bucket: &str, config_file: &str, data: Vec<u8>, parse: bool) -> Result<OffsetDateTime> {
        let Some(store) = runtime_sources::object_store_handle() else {
            return Err(Error::other("errServerNotInitialized"));
        };

        if is_meta_bucketname(bucket) {
            return Err(Error::other("errInvalidArgument"));
        }

        let mut bm = match load_bucket_metadata_parse(store, bucket, parse).await {
            Ok(res) => res,
            Err(err) => {
                if !runtime_sources::setup_is_erasure().await
                    && !runtime_sources::setup_is_dist_erasure().await
                    && is_err_bucket_not_found(&err)
                {
                    BucketMetadata::new(bucket)
                } else {
                    error!("load bucket metadata failed: {}", err);
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
            return Err(Error::other("errInvalidArgument"));
        }

        let mut bm = bm;

        bm.save().await?;

        self.set(bm.name.clone(), Arc::new(bm)).await;

        Ok(())
    }

    async fn bucket_names(&self) -> Vec<String> {
        self.metadata_map.read().await.keys().cloned().collect()
    }

    pub async fn get_config_from_disk(&self, bucket: &str) -> Result<BucketMetadata> {
        if is_meta_bucketname(bucket) {
            return Err(Error::other("errInvalidArgument"));
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
                    return if *self.initialized.read().await {
                        Err(Error::other("errBucketMetadataNotInitialized"))
                    } else {
                        Err(err)
                    };
                }
            };

            let mut map = self.metadata_map.write().await;

            let bm = Arc::new(bm);
            map.insert(bucket.to_string(), bm.clone());
            drop(map);
            sync_bucket_target_sys(bucket, &bm).await;

            Ok((bm, true))
        }
    }

    pub async fn get_versioning_config(&self, bucket: &str) -> Result<(VersioningConfiguration, OffsetDateTime)> {
        let bm = match self.get_config(bucket).await {
            Ok((res, _)) => res,
            Err(err) => {
                return if err == Error::ConfigNotFound {
                    Ok((VersioningConfiguration::default(), OffsetDateTime::UNIX_EPOCH))
                } else {
                    Err(err)
                };
            }
        };

        if let Some(config) = &bm.versioning_config {
            Ok((config.clone(), bm.versioning_config_updated_at))
        } else {
            Ok((VersioningConfiguration::default(), bm.versioning_config_updated_at))
        }
    }

    pub async fn get_bucket_policy(&self, bucket: &str) -> Result<(BucketPolicy, OffsetDateTime)> {
        let (bm, _) = self.get_config(bucket).await?;

        if let Some(config) = &bm.policy_config {
            Ok((config.clone(), bm.policy_config_updated_at))
        } else {
            Err(Error::ConfigNotFound)
        }
    }

    /// Returns the raw JSON string of the bucket policy as originally stored.
    /// This preserves the exact format of the policy document as it was PUT.
    pub async fn get_bucket_policy_raw(&self, bucket: &str) -> Result<(String, OffsetDateTime)> {
        let (bm, _) = self.get_config(bucket).await?;

        if bm.policy_config_json.is_empty() {
            Err(Error::ConfigNotFound)
        } else {
            let policy_str = String::from_utf8(bm.policy_config_json.clone())
                .map_err(|e| Error::other(format!("invalid UTF-8 in policy JSON: {}", e)))?;
            Ok((policy_str, bm.policy_config_updated_at))
        }
    }

    pub async fn get_bucket_acl_config(&self, bucket: &str) -> Result<(String, OffsetDateTime)> {
        let (bm, _) = self.get_config(bucket).await?;

        if let Some(config) = &bm.bucket_acl_config {
            Ok((config.clone(), bm.bucket_acl_config_updated_at))
        } else {
            Err(Error::ConfigNotFound)
        }
    }

    pub async fn get_tagging_config(&self, bucket: &str) -> Result<(Tagging, OffsetDateTime)> {
        let (bm, _) = self.get_config(bucket).await?;

        if let Some(config) = &bm.tagging_config {
            Ok((config.clone(), bm.tagging_config_updated_at))
        } else {
            Err(Error::ConfigNotFound)
        }
    }

    pub async fn get_public_access_block_config(&self, bucket: &str) -> Result<(PublicAccessBlockConfiguration, OffsetDateTime)> {
        let (bm, _) = self.get_config(bucket).await?;

        if let Some(config) = &bm.public_access_block_config {
            Ok((config.clone(), bm.public_access_block_config_updated_at))
        } else {
            Err(Error::ConfigNotFound)
        }
    }

    pub async fn get_object_lock_config(&self, bucket: &str) -> Result<(ObjectLockConfiguration, OffsetDateTime)> {
        let (bm, _) = self.get_config(bucket).await?;

        if let Some(config) = &bm.object_lock_config {
            Ok((config.clone(), bm.object_lock_config_updated_at))
        } else {
            Err(Error::ConfigNotFound)
        }
    }

    pub async fn get_lifecycle_config(&self, bucket: &str) -> Result<(BucketLifecycleConfiguration, OffsetDateTime)> {
        let (bm, _) = self.get_config(bucket).await?;

        if let Some(config) = &bm.lifecycle_config {
            if config.rules.is_empty() {
                Err(Error::ConfigNotFound)
            } else {
                Ok((config.clone(), bm.lifecycle_config_updated_at))
            }
        } else {
            Err(Error::ConfigNotFound)
        }
    }

    pub async fn get_notification_config(&self, bucket: &str) -> Result<Option<NotificationConfiguration>> {
        let bm = match self.get_config(bucket).await {
            Ok((bm, _)) => bm.notification_config.clone(),
            Err(err) => {
                if err == Error::ConfigNotFound {
                    None
                } else {
                    return Err(err);
                }
            }
        };

        Ok(bm)
    }

    pub async fn get_sse_config(&self, bucket: &str) -> Result<(ServerSideEncryptionConfiguration, OffsetDateTime)> {
        let (bm, _) = self.get_config(bucket).await?;

        if let Some(config) = &bm.sse_config {
            Ok((config.clone(), bm.encryption_config_updated_at))
        } else {
            Err(Error::ConfigNotFound)
        }
    }

    pub async fn get_cors_config(&self, bucket: &str) -> Result<(CORSConfiguration, OffsetDateTime)> {
        let (bm, _) = self.get_config(bucket).await?;

        if let Some(config) = &bm.cors_config {
            Ok((config.clone(), bm.cors_config_updated_at))
        } else {
            Err(Error::ConfigNotFound)
        }
    }

    pub async fn get_website_config(&self, bucket: &str) -> Result<(WebsiteConfiguration, OffsetDateTime)> {
        let (bm, _) = self.get_config(bucket).await?;

        if let Some(config) = &bm.website_config {
            Ok((config.clone(), bm.website_config_updated_at))
        } else {
            Err(Error::ConfigNotFound)
        }
    }

    pub async fn get_logging_config(&self, bucket: &str) -> Result<(BucketLoggingStatus, OffsetDateTime)> {
        let (bm, _) = self.get_config(bucket).await?;

        if let Some(config) = &bm.logging_config {
            Ok((config.clone(), bm.logging_config_updated_at))
        } else {
            Err(Error::ConfigNotFound)
        }
    }

    pub async fn get_accelerate_config(&self, bucket: &str) -> Result<(AccelerateConfiguration, OffsetDateTime)> {
        let (bm, _) = self.get_config(bucket).await?;

        if let Some(config) = &bm.accelerate_config {
            Ok((config.clone(), bm.accelerate_config_updated_at))
        } else {
            Err(Error::ConfigNotFound)
        }
    }

    pub async fn get_request_payment_config(&self, bucket: &str) -> Result<(RequestPaymentConfiguration, OffsetDateTime)> {
        let (bm, _) = self.get_config(bucket).await?;

        if let Some(config) = &bm.request_payment_config {
            Ok((config.clone(), bm.request_payment_config_updated_at))
        } else {
            Err(Error::ConfigNotFound)
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
        let (bm, _) = self.get_config(bucket).await?;

        if let Some(config) = &bm.quota_config {
            Ok((config.clone(), bm.quota_config_updated_at))
        } else {
            Err(Error::ConfigNotFound)
        }
    }

    pub async fn get_replication_config(&self, bucket: &str) -> Result<(ReplicationConfiguration, OffsetDateTime)> {
        let (bm, _) = self.get_config(bucket).await?;

        if let Some(config) = &bm.replication_config {
            Ok((config.clone(), bm.replication_config_updated_at))
        } else {
            Err(Error::ConfigNotFound)
        }
    }

    pub async fn get_bucket_targets_config(&self, bucket: &str) -> Result<BucketTargets> {
        let (bm, _) = self.get_config(bucket).await?;

        if let Some(config) = &bm.bucket_target_config {
            Ok(config.clone())
        } else {
            Err(Error::ConfigNotFound)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bucket::target::{BucketTarget, BucketTargetType, Credentials};
    use serial_test::serial;
    use tokio::time::timeout;

    fn target(bucket: &str, id: &str) -> BucketTarget {
        BucketTarget {
            source_bucket: bucket.to_string(),
            endpoint: format!("{id}.example.com:9000"),
            credentials: Some(Credentials {
                access_key: "access".to_string(),
                secret_key: "secret".to_string(),
                ..Default::default()
            }),
            target_bucket: format!("{bucket}-{id}"),
            arn: format!("arn:rustfs:replication:us-east-1:{bucket}:{id}"),
            target_type: BucketTargetType::ReplicationService,
            region: "us-east-1".to_string(),
            ..Default::default()
        }
    }

    #[tokio::test]
    #[serial]
    async fn metadata_reload_syncs_bucket_target_sys() {
        let bucket = "metadata-reload-targets";
        let target_sys = BucketTargetSys::get();
        target_sys.delete(bucket).await;

        let mut bm = BucketMetadata::new(bucket);
        bm.bucket_target_config = Some(BucketTargets {
            targets: vec![target(bucket, "fresh")],
        });

        sync_bucket_target_sys(bucket, &bm).await;

        let targets = target_sys
            .list_bucket_targets(bucket)
            .await
            .expect("target sync should publish bucket targets");
        assert_eq!(targets.targets.len(), 1);
        assert_eq!(targets.targets[0].arn, format!("arn:rustfs:replication:us-east-1:{bucket}:fresh"));

        target_sys.delete(bucket).await;
    }

    #[tokio::test]
    #[serial]
    async fn metadata_reload_clears_stale_bucket_targets_when_config_is_removed() {
        let bucket = "metadata-clear-targets";
        let target_sys = BucketTargetSys::get();
        target_sys.delete(bucket).await;
        target_sys
            .targets_map
            .write()
            .await
            .insert(bucket.to_string(), vec![target(bucket, "stale")]);

        let bm = BucketMetadata::new(bucket);
        sync_bucket_target_sys(bucket, &bm).await;

        assert!(target_sys.list_bucket_targets(bucket).await.is_err());
        target_sys.delete(bucket).await;
    }

    #[tokio::test]
    async fn refresh_wait_exits_when_cancelled() {
        let cancel_token = CancellationToken::new();
        cancel_token.cancel();

        let should_refresh = timeout(
            Duration::from_millis(100),
            wait_refresh_interval_or_cancel(&cancel_token, Duration::from_secs(60)),
        )
        .await
        .expect("cancelled refresh wait should not sleep until the interval");

        assert!(!should_refresh);
    }
}
