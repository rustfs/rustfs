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
use rustfs_config::server_config::{Config as ServerConfig, get_global_server_config as config_get_global_server_config};
use std::path::PathBuf;
use std::sync::Arc;
use storage_api::owner::{
    ECSTORE_BUCKET_META_PREFIX, ECSTORE_RUSTFS_META_BUCKET, ECSTORE_STORAGE_FORMAT_FILE, ECSTORE_STORAGECLASS_RRS,
    ECSTORE_STORAGECLASS_STANDARD, ECSTORE_TRANSITION_COMPLETE, EcstoreBucketTargetSys, EcstoreBucketVersioningSys, EcstoreDisk,
    EcstoreDiskAPI, EcstoreDiskBytes, EcstoreDiskError, EcstoreDiskInfo, EcstoreDiskInfoOptions, EcstoreDiskLocation,
    EcstoreDiskResult, EcstoreErrorType, EcstoreEvaluator, EcstoreEvent, EcstoreLcEventSrc, EcstoreLifecycle,
    EcstoreListPathRawOptions, EcstoreObjectOpts, EcstoreReplicationConfigurationExt, EcstoreReplicationScannerBridge,
    EcstoreResultType, EcstoreScanGuard, EcstoreSetDisks, EcstoreStorageError, EcstoreStore, EcstoreTierConfig,
    EcstoreVersioningApi, HTTPRangeSpec, ObjectIO, ObjectOperations, ObjectToDelete, ScannerReplicationHealObject,
    ScannerReplicationHealResult, ScannerReplicationQueueAdmission, ecstore_apply_expiry_rule, ecstore_apply_transition_rule,
    ecstore_expiry_state_handle, ecstore_get_global_tier_config_mgr, ecstore_get_lifecycle_config,
    ecstore_get_object_lock_config, ecstore_get_replication_config, ecstore_is_erasure, ecstore_is_erasure_sd,
    ecstore_is_reserved_or_invalid_bucket, ecstore_list_path_raw, ecstore_path2_bucket_object,
    ecstore_path2_bucket_object_with_base_path, ecstore_read_config, ecstore_replace_bucket_usage_memory_from_info,
    ecstore_resolve_object_store_handle, ecstore_save_config, scanner_replication_config_for_lifecycle_eval,
};
#[cfg(test)]
use storage_api::owner::{EcstoreDiskOption, EcstoreDiskStore, EcstoreEndpoint, ecstore_config_init, ecstore_new_disk};
use tokio_util::sync::CancellationToken;

pub mod data_usage_define;
pub mod error;
pub mod runtime_config;
pub mod scanner;
pub mod scanner_budget;
pub mod scanner_folder;
pub mod scanner_io;
pub mod sleeper;
pub(crate) mod storage_api;

pub use data_usage_define::*;
pub use error::ScannerError;
pub use runtime_config::{apply_scanner_runtime_config, scanner_runtime_config_status, validate_scanner_runtime_config};
pub use rustfs_common::last_minute;
pub use scanner::init_data_scanner;
pub use scanner_io::{clear_dirty_usage_bucket, record_dirty_usage_bucket};
pub use sleeper::{DynamicSleeper, SCANNER_IDLE_MODE, SCANNER_SLEEPER};
use std::sync::atomic::{AtomicU64, Ordering};
pub use storage_api::ScannerReplicationConfig as ReplicationConfig;

static SCANNER_ACTIVE_WORK_UNITS: AtomicU64 = AtomicU64::new(0);
static SCANNER_FOREGROUND_READ_ACTIVITY: AtomicU64 = AtomicU64::new(0);
static SCANNER_FOREGROUND_STREAM_READS: AtomicU64 = AtomicU64::new(0);

pub fn current_scanner_activity() -> u64 {
    SCANNER_ACTIVE_WORK_UNITS.load(Ordering::Relaxed)
}

pub fn set_foreground_read_activity(active: usize) {
    let active = u64::try_from(active).unwrap_or(u64::MAX);
    SCANNER_FOREGROUND_READ_ACTIVITY.store(active, Ordering::Relaxed);
}

pub fn current_foreground_read_activity() -> u64 {
    SCANNER_FOREGROUND_READ_ACTIVITY
        .load(Ordering::Relaxed)
        .max(SCANNER_FOREGROUND_STREAM_READS.load(Ordering::Relaxed))
}

#[derive(Debug)]
pub struct ForegroundReadGuard;

impl ForegroundReadGuard {
    pub fn new() -> Self {
        SCANNER_FOREGROUND_STREAM_READS.fetch_add(1, Ordering::Relaxed);
        Self
    }
}

impl Default for ForegroundReadGuard {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for ForegroundReadGuard {
    fn drop(&mut self) {
        let _ =
            SCANNER_FOREGROUND_STREAM_READS.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| current.checked_sub(1));
    }
}

#[cfg(test)]
pub(crate) fn reset_foreground_read_activity_for_test() {
    SCANNER_FOREGROUND_READ_ACTIVITY.store(0, Ordering::Relaxed);
    SCANNER_FOREGROUND_STREAM_READS.store(0, Ordering::Relaxed);
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
pub(crate) type ReplicationHealObject = ScannerReplicationHealObject;
pub(crate) type ReplicationHealQueueResult = ScannerReplicationHealResult;
pub(crate) type ReplicationQueueAdmission = ScannerReplicationQueueAdmission;
pub(crate) type ReplicationStatusType = storage_api::ReplicationStatusType;
pub(crate) type ScanGuard = EcstoreScanGuard;
pub(crate) type SetDisks = EcstoreSetDisks;
pub(crate) type StorageError = EcstoreStorageError;

pub type ScannerGetObjectReader = <ECStore as ObjectIO>::GetObjectReader;
pub type ScannerObjectInfo = <ECStore as ObjectOperations>::ObjectInfo;
pub type ScannerObjectOptions = <ECStore as ObjectOperations>::ObjectOptions;
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

pub(crate) fn resolve_scanner_server_config() -> Option<ServerConfig> {
    config_get_global_server_config()
}

pub(crate) async fn list_runtime_tiers() -> Vec<EcstoreTierConfig> {
    ecstore_get_global_tier_config_mgr().read().await.list_tiers()
}

pub(crate) async fn enqueue_runtime_free_version(oi: ScannerObjectInfo) {
    ecstore_expiry_state_handle().write().await.enqueue_free_version(oi);
}

pub(crate) async fn enqueue_runtime_newer_noncurrent(
    bucket: &str,
    to_delete_objs: Vec<ObjectToDelete>,
    event: Event,
    src: &LcEventSrc,
) -> bool {
    ecstore_expiry_state_handle()
        .write()
        .await
        .enqueue_by_newer_noncurrent(bucket, to_delete_objs, event, src)
}

pub(crate) async fn queue_replication_heal(
    bucket: &str,
    oi: ScannerObjectInfo,
    rcfg: ReplicationConfig,
    retry_count: u32,
) -> ReplicationHealQueueResult {
    EcstoreReplicationScannerBridge::queue_heal(bucket, oi, rcfg.into_ecstore(), retry_count)
        .await
        .into()
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

pub(crate) async fn scanner_is_erasure() -> bool {
    ecstore_is_erasure().await
}

pub(crate) async fn scanner_is_erasure_sd() -> bool {
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

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;

    #[test]
    #[serial]
    fn foreground_read_guard_tracks_stream_lifetime() {
        reset_foreground_read_activity_for_test();
        assert_eq!(current_foreground_read_activity(), 0);

        {
            let _guard = ForegroundReadGuard::new();
            assert_eq!(current_foreground_read_activity(), 1);
        }

        assert_eq!(current_foreground_read_activity(), 0);
    }

    #[test]
    #[serial]
    fn foreground_read_activity_keeps_larger_signal() {
        reset_foreground_read_activity_for_test();
        let _guard = ForegroundReadGuard::new();

        set_foreground_read_activity(3);
        assert_eq!(current_foreground_read_activity(), 3);

        set_foreground_read_activity(0);
        assert_eq!(current_foreground_read_activity(), 1);
    }
}
