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

#![cfg_attr(docsrs, feature(doc_auto_cfg))]
#![warn(
    // missing_docs,
    rustdoc::missing_crate_level_docs,
    unreachable_pub,
    rust_2018_idioms
)]

use http::HeaderMap;
use rustfs_ecstore::api::bucket::bucket_target_sys::BucketTargetSys as EcstoreBucketTargetSys;
use rustfs_ecstore::api::bucket::lifecycle::bucket_lifecycle_audit::LcEventSrc as EcstoreLcEventSrc;
use rustfs_ecstore::api::bucket::lifecycle::bucket_lifecycle_ops::{
    GLOBAL_ExpiryState as ECSTORE_GLOBAL_EXPIRY_STATE, apply_expiry_rule as ecstore_apply_expiry_rule,
    apply_transition_rule as ecstore_apply_transition_rule,
};
use rustfs_ecstore::api::bucket::lifecycle::evaluator::Evaluator as EcstoreEvaluator;
use rustfs_ecstore::api::bucket::lifecycle::lifecycle::{
    Event as EcstoreEvent, Lifecycle as EcstoreLifecycle, ObjectOpts as EcstoreObjectOpts,
    TRANSITION_COMPLETE as ECSTORE_TRANSITION_COMPLETE,
};
use rustfs_ecstore::api::bucket::metadata_sys::{
    get_lifecycle_config as ecstore_get_lifecycle_config, get_object_lock_config as ecstore_get_object_lock_config,
    get_replication_config as ecstore_get_replication_config,
};
use rustfs_ecstore::api::bucket::replication::{
    ReplicationConfig as EcstoreReplicationConfig, ReplicationConfigurationExt as EcstoreReplicationConfigurationExt,
    ReplicationHealQueueResult as EcstoreReplicationHealQueueResult,
    ReplicationQueueAdmission as EcstoreReplicationQueueAdmission,
    queue_replication_heal_internal as ecstore_queue_replication_heal_internal,
};
use rustfs_ecstore::api::bucket::versioning::VersioningApi as EcstoreVersioningApi;
use rustfs_ecstore::api::bucket::versioning_sys::BucketVersioningSys as EcstoreBucketVersioningSys;
use rustfs_ecstore::api::cache::{ListPathRawOptions as EcstoreListPathRawOptions, list_path_raw as ecstore_list_path_raw};
use rustfs_ecstore::api::capacity::{
    is_reserved_or_invalid_bucket as ecstore_is_reserved_or_invalid_bucket, path2_bucket_object as ecstore_path2_bucket_object,
    path2_bucket_object_with_base_path as ecstore_path2_bucket_object_with_base_path,
};
use rustfs_ecstore::api::config::com::{read_config as ecstore_read_config, save_config as ecstore_save_config};
#[cfg(test)]
use rustfs_ecstore::api::config::init as ecstore_config_init;
use rustfs_ecstore::api::config::storageclass::{RRS as ECSTORE_STORAGECLASS_RRS, STANDARD as ECSTORE_STORAGECLASS_STANDARD};
use rustfs_ecstore::api::data_usage::replace_bucket_usage_memory_from_info as ecstore_replace_bucket_usage_memory_from_info;
#[cfg(test)]
use rustfs_ecstore::api::disk::endpoint::Endpoint as EcstoreEndpoint;
use rustfs_ecstore::api::disk::error::{DiskError as EcstoreDiskError, Result as EcstoreDiskResult};
use rustfs_ecstore::api::disk::{
    BUCKET_META_PREFIX as ECSTORE_BUCKET_META_PREFIX, Bytes as EcstoreDiskBytes, Disk as EcstoreDisk, DiskAPI as EcstoreDiskAPI,
    DiskInfo as EcstoreDiskInfo, DiskInfoOptions as EcstoreDiskInfoOptions, DiskLocation as EcstoreDiskLocation,
    RUSTFS_META_BUCKET as ECSTORE_RUSTFS_META_BUCKET, STORAGE_FORMAT_FILE as ECSTORE_STORAGE_FORMAT_FILE,
    ScanGuard as EcstoreScanGuard,
};
#[cfg(test)]
use rustfs_ecstore::api::disk::{DiskOption as EcstoreDiskOption, DiskStore as EcstoreDiskStore, new_disk as ecstore_new_disk};
use rustfs_ecstore::api::error::{Error as EcstoreErrorType, Result as EcstoreResultType, StorageError as EcstoreStorageError};
use rustfs_ecstore::api::global::{
    GLOBAL_TierConfigMgr as ECSTORE_GLOBAL_TIER_CONFIG_MGR, is_erasure as ecstore_is_erasure,
    is_erasure_sd as ecstore_is_erasure_sd, resolve_object_store_handle as ecstore_resolve_object_store_handle,
};
use rustfs_ecstore::api::set_disk::SetDisks as EcstoreSetDisks;
use rustfs_ecstore::api::storage::ECStore as EcstoreStore;
use rustfs_ecstore::api::tier::tier_config::TierConfig as EcstoreTierConfig;
use rustfs_storage_api::{HTTPRangeSpec, ObjectIO, ObjectToDelete};
use std::path::PathBuf;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

pub mod data_usage_define;
pub mod error;
pub mod runtime_config;
pub mod scanner;
pub mod scanner_budget;
pub mod scanner_folder;
pub mod scanner_io;
pub mod sleeper;

pub use data_usage_define::*;
pub use error::ScannerError;
pub use runtime_config::{apply_scanner_runtime_config, scanner_runtime_config_status, validate_scanner_runtime_config};
pub use rustfs_common::last_minute;
pub use scanner::init_data_scanner;
pub use scanner_io::{clear_dirty_usage_bucket, record_dirty_usage_bucket};
pub use sleeper::{DynamicSleeper, SCANNER_IDLE_MODE, SCANNER_SLEEPER};
use std::sync::atomic::{AtomicU64, Ordering};

static SCANNER_ACTIVE_WORK_UNITS: AtomicU64 = AtomicU64::new(0);

pub fn current_scanner_activity() -> u64 {
    SCANNER_ACTIVE_WORK_UNITS.load(Ordering::Relaxed)
}

pub(crate) struct ScannerActivityGuard;

impl ScannerActivityGuard {
    pub(crate) fn new() -> Self {
        SCANNER_ACTIVE_WORK_UNITS.fetch_add(1, Ordering::Relaxed);
        Self
    }
}

impl Drop for ScannerActivityGuard {
    fn drop(&mut self) {
        let _ = SCANNER_ACTIVE_WORK_UNITS
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| Some(current.saturating_sub(1)));
    }
}

pub(crate) const BUCKET_META_PREFIX: &str = ECSTORE_BUCKET_META_PREFIX;
pub(crate) const RUSTFS_META_BUCKET: &str = ECSTORE_RUSTFS_META_BUCKET;
pub(crate) const STORAGE_FORMAT_FILE: &str = ECSTORE_STORAGE_FORMAT_FILE;
pub(crate) const TRANSITION_COMPLETE: &str = ECSTORE_TRANSITION_COMPLETE;

pub(crate) type Disk = EcstoreDisk;
#[cfg(test)]
pub(crate) type DiskStore = EcstoreDiskStore;
pub(crate) type DiskLocation = EcstoreDiskLocation;
pub(crate) type DiskError = EcstoreDiskError;
pub(crate) type DiskResult<T> = EcstoreDiskResult<T>;
pub(crate) type ECStore = EcstoreStore;
pub(crate) type EcstoreError = EcstoreErrorType;
pub(crate) type EcstoreResult<T> = EcstoreResultType<T>;
pub(crate) type ListPathRawOptions = EcstoreListPathRawOptions;
pub(crate) type BucketTargetSys = EcstoreBucketTargetSys;
pub(crate) type BucketVersioningSys = EcstoreBucketVersioningSys;
pub(crate) type DiskInfo = EcstoreDiskInfo;
pub(crate) type DiskInfoOptions = EcstoreDiskInfoOptions;
pub(crate) type DiskBytes = EcstoreDiskBytes;
pub(crate) type Evaluator = EcstoreEvaluator;
pub(crate) type Event = EcstoreEvent;
pub(crate) type LcEventSrc = EcstoreLcEventSrc;
pub(crate) type ObjectOpts = EcstoreObjectOpts;
pub(crate) type ReplicationConfig = EcstoreReplicationConfig;
pub(crate) type ReplicationHealQueueResult = EcstoreReplicationHealQueueResult;
pub(crate) type ReplicationQueueAdmission = EcstoreReplicationQueueAdmission;
pub(crate) type ScanGuard = EcstoreScanGuard;
pub(crate) type SetDisks = EcstoreSetDisks;
pub(crate) type StorageError = EcstoreStorageError;

pub type ScannerGetObjectReader = <ECStore as ObjectIO>::GetObjectReader;
pub type ScannerObjectInfo = <ECStore as rustfs_storage_api::ObjectOperations>::ObjectInfo;
pub type ScannerObjectOptions = <ECStore as rustfs_storage_api::ObjectOperations>::ObjectOptions;
pub type ScannerObjectToDelete = ObjectToDelete;
pub type ScannerPutObjReader = <ECStore as ObjectIO>::PutObjectReader;

pub(crate) mod storageclass {
    use super::{ECSTORE_STORAGECLASS_RRS, ECSTORE_STORAGECLASS_STANDARD};

    pub(crate) const RRS: &str = ECSTORE_STORAGECLASS_RRS;
    pub(crate) const STANDARD: &str = ECSTORE_STORAGECLASS_STANDARD;
}

#[cfg(test)]
pub(crate) fn init_ecstore_config_for_scanner_tests() {
    ecstore_config_init();
}

#[cfg(test)]
pub(crate) type DiskOption = EcstoreDiskOption;
#[cfg(test)]
pub(crate) type Endpoint = EcstoreEndpoint;

#[cfg(test)]
pub(crate) async fn new_disk(ep: &Endpoint, opt: &DiskOption) -> DiskResult<DiskStore> {
    ecstore_new_disk(ep, opt).await
}

pub(crate) async fn get_lifecycle_config(
    bucket: &str,
) -> EcstoreResult<(s3s::dto::BucketLifecycleConfiguration, time::OffsetDateTime)> {
    ecstore_get_lifecycle_config(bucket).await
}

pub(crate) async fn get_object_lock_config(
    bucket: &str,
) -> EcstoreResult<(s3s::dto::ObjectLockConfiguration, time::OffsetDateTime)> {
    ecstore_get_object_lock_config(bucket).await
}

pub(crate) async fn get_replication_config(
    bucket: &str,
) -> EcstoreResult<(s3s::dto::ReplicationConfiguration, time::OffsetDateTime)> {
    ecstore_get_replication_config(bucket).await
}

pub(crate) trait ScannerLifecycleConfigExt {
    fn has_active_rules(&self, prefix: &str) -> bool;
}

impl ScannerLifecycleConfigExt for s3s::dto::BucketLifecycleConfiguration {
    fn has_active_rules(&self, prefix: &str) -> bool {
        <s3s::dto::BucketLifecycleConfiguration as EcstoreLifecycle>::has_active_rules(self, prefix)
    }
}

pub(crate) trait ScannerReplicationConfigExt {
    fn has_active_rules(&self, prefix: &str, recursive: bool) -> bool;
}

impl ScannerReplicationConfigExt for s3s::dto::ReplicationConfiguration {
    fn has_active_rules(&self, prefix: &str, recursive: bool) -> bool {
        <s3s::dto::ReplicationConfiguration as EcstoreReplicationConfigurationExt>::has_active_rules(self, prefix, recursive)
    }
}

pub(crate) trait ScannerVersioningConfigExt {
    fn prefix_enabled(&self, prefix: &str) -> bool;
    fn versioned(&self, prefix: &str) -> bool;
}

impl ScannerVersioningConfigExt for s3s::dto::VersioningConfiguration {
    fn prefix_enabled(&self, prefix: &str) -> bool {
        <s3s::dto::VersioningConfiguration as EcstoreVersioningApi>::prefix_enabled(self, prefix)
    }

    fn versioned(&self, prefix: &str) -> bool {
        <s3s::dto::VersioningConfiguration as EcstoreVersioningApi>::versioned(self, prefix)
    }
}

pub(crate) trait ScannerDiskExt {
    async fn disk_info(&self, opts: &DiskInfoOptions) -> DiskResult<DiskInfo>;
    async fn read_metadata(&self, volume: &str, path: &str) -> DiskResult<DiskBytes>;
    fn path(&self) -> PathBuf;
    fn get_disk_location(&self) -> DiskLocation;
    fn start_scan(&self) -> ScanGuard;
}

impl<T> ScannerDiskExt for T
where
    T: EcstoreDiskAPI,
{
    async fn disk_info(&self, opts: &DiskInfoOptions) -> DiskResult<DiskInfo> {
        EcstoreDiskAPI::disk_info(self, opts).await
    }

    async fn read_metadata(&self, volume: &str, path: &str) -> DiskResult<DiskBytes> {
        EcstoreDiskAPI::read_metadata(self, volume, path).await
    }

    fn path(&self) -> PathBuf {
        EcstoreDiskAPI::path(self)
    }

    fn get_disk_location(&self) -> DiskLocation {
        EcstoreDiskAPI::get_disk_location(self)
    }

    fn start_scan(&self) -> ScanGuard {
        EcstoreDiskAPI::start_scan(self)
    }
}

pub(crate) async fn apply_transition_rule(event: &Event, src: &LcEventSrc, oi: &ScannerObjectInfo) -> bool {
    ecstore_apply_transition_rule(event, src, oi).await
}

pub(crate) async fn apply_expiry_rule(event: &Event, src: &LcEventSrc, oi: &ScannerObjectInfo) -> bool {
    ecstore_apply_expiry_rule(event, src, oi).await
}

pub(crate) async fn list_global_tiers() -> Vec<EcstoreTierConfig> {
    ECSTORE_GLOBAL_TIER_CONFIG_MGR.read().await.list_tiers()
}

pub(crate) async fn enqueue_global_free_version(oi: ScannerObjectInfo) {
    ECSTORE_GLOBAL_EXPIRY_STATE.write().await.enqueue_free_version(oi).await;
}

pub(crate) async fn enqueue_global_newer_noncurrent(
    bucket: &str,
    to_delete_objs: Vec<ObjectToDelete>,
    event: Event,
    src: &LcEventSrc,
) -> bool {
    ECSTORE_GLOBAL_EXPIRY_STATE
        .write()
        .await
        .enqueue_by_newer_noncurrent(bucket, to_delete_objs, event, src)
        .await
}

pub(crate) async fn queue_replication_heal_internal(
    bucket: &str,
    oi: ScannerObjectInfo,
    rcfg: ReplicationConfig,
    retry_count: u32,
) -> ReplicationHealQueueResult {
    ecstore_queue_replication_heal_internal(bucket, oi, rcfg, retry_count).await
}

pub(crate) fn resolve_scanner_object_store_handle() -> Option<Arc<ECStore>> {
    ecstore_resolve_object_store_handle()
}

pub(crate) fn is_reserved_or_invalid_bucket(bucket: &str, strict: bool) -> bool {
    ecstore_is_reserved_or_invalid_bucket(bucket, strict)
}

pub(crate) fn path2_bucket_object(name: &str) -> (String, String) {
    ecstore_path2_bucket_object(name)
}

pub(crate) fn path2_bucket_object_with_base_path(base_path: &str, path: &str) -> (String, String) {
    ecstore_path2_bucket_object_with_base_path(base_path, path)
}

pub(crate) async fn is_erasure() -> bool {
    ecstore_is_erasure().await
}

pub(crate) async fn is_erasure_sd() -> bool {
    ecstore_is_erasure_sd().await
}

pub(crate) async fn read_config<S>(api: Arc<S>, file: &str) -> EcstoreResult<Vec<u8>>
where
    S: ScannerObjectIO,
{
    ecstore_read_config(api, file).await
}

pub(crate) async fn save_config<S>(api: Arc<S>, file: &str, data: Vec<u8>) -> EcstoreResult<()>
where
    S: ScannerObjectIO,
{
    ecstore_save_config(api, file, data).await
}

pub(crate) async fn list_path_raw(rx: CancellationToken, opts: ListPathRawOptions) -> std::result::Result<(), DiskError> {
    ecstore_list_path_raw(rx, opts).await
}

pub(crate) async fn replace_bucket_usage_memory_from_info(data_usage_info: &rustfs_data_usage::DataUsageInfo) {
    ecstore_replace_bucket_usage_memory_from_info(data_usage_info).await;
}

pub trait ScannerObjectIO:
    ObjectIO<
        Error = EcstoreError,
        RangeSpec = HTTPRangeSpec,
        HeaderMap = HeaderMap,
        ObjectOptions = ScannerObjectOptions,
        ObjectInfo = ScannerObjectInfo,
        GetObjectReader = ScannerGetObjectReader,
        PutObjectReader = ScannerPutObjReader,
    >
{
}

impl<T> ScannerObjectIO for T where
    T: ObjectIO<
            Error = EcstoreError,
            RangeSpec = HTTPRangeSpec,
            HeaderMap = HeaderMap,
            ObjectOptions = ScannerObjectOptions,
            ObjectInfo = ScannerObjectInfo,
            GetObjectReader = ScannerGetObjectReader,
            PutObjectReader = ScannerPutObjReader,
        >
{
}
