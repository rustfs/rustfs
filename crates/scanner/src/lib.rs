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

#![recursion_limit = "256"]
#![cfg_attr(docsrs, feature(doc_auto_cfg))]
#![warn(
    // missing_docs,
    rustdoc::missing_crate_level_docs,
    unreachable_pub,
    rust_2018_idioms
)]

use bytes::Bytes;
use http::HeaderMap;
use rustfs_config::server_config::{Config as ServerConfig, get_global_server_config as config_get_global_server_config};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::RwLock;
use std::time::{Duration, Instant};
use storage_api::owner::{
    ECSTORE_BUCKET_META_PREFIX, ECSTORE_RUSTFS_META_BUCKET, ECSTORE_STORAGE_FORMAT_FILE, ECSTORE_STORAGECLASS_RRS,
    ECSTORE_STORAGECLASS_STANDARD, ECSTORE_TRANSITION_COMPLETE, EcstoreBucketTargetSys, EcstoreBucketVersioningSys, EcstoreDisk,
    EcstoreDiskAPI, EcstoreDiskBytes, EcstoreDiskError, EcstoreDiskInfo, EcstoreDiskInfoOptions, EcstoreDiskLocation,
    EcstoreDiskResult, EcstoreErrorType, EcstoreEvaluator, EcstoreEvent, EcstoreLcEventSrc, EcstoreLifecycle,
    EcstoreListPathRawOptions, EcstoreNsScannerOpenRequest, EcstoreObjectOpts, EcstoreReplicationConfigurationExt,
    EcstoreReplicationScannerBridge, EcstoreResultType, EcstoreScanGuard, EcstoreSetDisks, EcstoreStorageError, EcstoreStore,
    EcstoreVersioningApi, HTTPPreconditions, HTTPRangeSpec, ObjectIO, ObjectOperations, ObjectToDelete,
    ScannerReplicationHealObject, ScannerReplicationHealResult, ScannerReplicationQueueAdmission, ecstore_apply_expiry_rule,
    ecstore_apply_transition_rule, ecstore_expiry_state_handle, ecstore_get_global_tier_config_mgr, ecstore_get_lifecycle_config,
    ecstore_get_object_lock_config, ecstore_get_replication_config, ecstore_invalidate_admin_data_usage_snapshot_cache,
    ecstore_invalidate_data_usage_snapshot_cache, ecstore_is_erasure, ecstore_is_erasure_sd,
    ecstore_is_reserved_or_invalid_bucket, ecstore_list_path_raw, ecstore_object_opts_from_object_info,
    ecstore_path2_bucket_object, ecstore_path2_bucket_object_with_base_path, ecstore_read_config,
    ecstore_replace_bucket_usage_memory_from_info, ecstore_resolve_object_store_handle, ecstore_save_config,
    scanner_replication_config_for_lifecycle_eval,
};
#[cfg(test)]
use storage_api::owner::{
    EcstoreDiskOption, EcstoreDiskStore, EcstoreEndpoint, EcstoreEndpointServerPools, EcstoreEndpoints, EcstoreInstanceContext,
    EcstorePoolEndpoints, ecstore_config_init, ecstore_init_bucket_metadata_sys, ecstore_init_local_disks_with_instance_ctx,
    ecstore_new_disk,
};
use tokio_util::sync::CancellationToken;

pub mod data_usage_define;
pub mod error;
pub mod prefix_usage;
mod remote_scanner;
pub mod runtime_config;
pub mod scanner;
pub mod scanner_budget;
pub mod scanner_folder;
#[cfg(test)]
mod scanner_heal_admission_baseline;
pub mod scanner_io;
pub mod sleeper;
pub(crate) mod storage_api;

pub use data_usage_define::*;
pub use error::ScannerError;
pub use prefix_usage::{BucketPrefixUsageResponse, bucket_prefix_usage, invalidate_prefix_usage_cache};
pub use remote_scanner::{
    NS_SCANNER_MAX_REQUEST_BODY_SIZE, RemoteScannerAdmission, RemoteScannerRequest, admit_remote_scanner_request,
    claim_remote_scanner_request, decode_remote_scanner_request, preflight_remote_scanner_request,
    remote_scanner_request_matches_envelope, serve_remote_scanner_request, validate_remote_scanner_request_fence,
};
pub use runtime_config::{apply_scanner_runtime_config, scanner_runtime_config_status, validate_scanner_runtime_config};
pub use rustfs_common::last_minute;
pub use scanner::{
    ScannerCycleRecoveryMarker, ScannerCycleRecoveryStatus, ScannerCycleScheduleStatus, init_data_scanner,
    reset_scanner_cycle_recovery, scanner_cycle_recovery_status, scanner_cycle_schedule_status, scanner_topology_digest,
};
pub use scanner_io::{
    ScannerDirtyUsageAckError, ScannerDirtyUsageState, acknowledge_dirty_usage_generation, clear_dirty_usage_bucket,
    record_dirty_usage_bucket, record_scanner_maintenance_change, scanner_activity_epoch, scanner_dirty_usage_state,
    scanner_maintenance_generation,
};
pub use sleeper::{DynamicSleeper, SCANNER_IDLE_MODE, SCANNER_SLEEPER};
use std::sync::atomic::{AtomicU64, Ordering};
pub use storage_api::ScannerReplicationConfig as ReplicationConfig;
pub use storage_api::scan::SCANNER_ACTIVITY_PROTOCOL_VERSION;

static SCANNER_ACTIVE_WORK_UNITS: AtomicU64 = AtomicU64::new(0);
static SCANNER_RUNTIME_INSTANCES: AtomicU64 = AtomicU64::new(0);
static SCANNER_FOREGROUND_READ_ACTIVITY: AtomicU64 = AtomicU64::new(0);
static SCANNER_FOREGROUND_STREAM_READS: AtomicU64 = AtomicU64::new(0);

pub fn current_scanner_activity() -> u64 {
    SCANNER_ACTIVE_WORK_UNITS.load(Ordering::Relaxed)
}

pub fn scanner_runtime_initialized() -> bool {
    SCANNER_RUNTIME_INSTANCES.load(Ordering::Relaxed) > 0
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

pub(crate) struct ScannerRuntimeGuard;

impl ScannerRuntimeGuard {
    pub(crate) fn new() -> Self {
        SCANNER_RUNTIME_INSTANCES.fetch_add(1, Ordering::Relaxed);
        Self
    }
}

impl Drop for ScannerRuntimeGuard {
    fn drop(&mut self) {
        let _ = SCANNER_RUNTIME_INSTANCES.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| current.checked_sub(1));
    }
}

#[cfg(test)]
fn reset_scanner_runtime_instances_for_test() {
    SCANNER_RUNTIME_INSTANCES.store(0, Ordering::Relaxed);
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
pub(crate) type NsScannerOpenRequest = EcstoreNsScannerOpenRequest;
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
pub(crate) type EndpointServerPools = EcstoreEndpointServerPools;
#[cfg(test)]
pub(crate) type Endpoints = EcstoreEndpoints;
#[cfg(test)]
pub(crate) type InstanceContext = EcstoreInstanceContext;
#[cfg(test)]
pub(crate) type PoolEndpoints = EcstorePoolEndpoints;

#[cfg(test)]
pub(crate) async fn init_local_disks_with_instance_ctx(
    ctx: &Arc<InstanceContext>,
    pools: EndpointServerPools,
) -> EcstoreResult<()> {
    ecstore_init_local_disks_with_instance_ctx(ctx, pools).await
}

#[cfg(test)]
pub(crate) async fn init_bucket_metadata_sys_for_scanner_tests(store: Arc<ECStore>) {
    ecstore_init_bucket_metadata_sys(store, Vec::new()).await;
}

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
    fn is_local(&self) -> bool;
    fn host_name(&self) -> String;
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

    fn is_local(&self) -> bool {
        EcstoreDiskAPI::is_local(self)
    }

    fn host_name(&self) -> String {
        EcstoreDiskAPI::host_name(self)
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

/// How long the scanner caches the runtime tier-name list before re-reading
/// the tier configuration manager.
const TIER_NAME_CACHE_TTL: Duration = Duration::from_secs(30);

/// Process-wide TTL cache of runtime tier names.
///
/// The scan hot path only needs tier *names* to seed `SizeSummary::tier_stats`
/// per object, but every `list_tiers()` call clones each full `TierConfig`
/// (endpoints, credentials, prefixes) from the global manager. Caching just
/// the names keeps the per-object cost at an `Arc` clone.
///
/// Staleness bounds: a newly added tier starts showing up in scans at most
/// `TIER_NAME_CACHE_TTL` later; a removed tier can leave an all-zero
/// `TierStats` seed behind for one cache generation, which merges harmlessly
/// by key in per-object accounting and disappears on the next refresh.
static TIER_NAME_CACHE: RwLock<Option<(Instant, Arc<[String]>)>> = RwLock::new(None);

/// Tier names currently registered in the tier configuration, cached for
/// `TIER_NAME_CACHE_TTL`.
pub(crate) async fn runtime_tier_names() -> Arc<[String]> {
    {
        let cached = TIER_NAME_CACHE.read().unwrap_or_else(|err| err.into_inner()).clone();
        if let Some((refreshed_at, names)) = cached
            && refreshed_at.elapsed() < TIER_NAME_CACHE_TTL
        {
            return names;
        }
    }

    let tiers = ecstore_get_global_tier_config_mgr().read().await.list_tiers();
    let names: Arc<[String]> = tiers.iter().map(|tier| tier.name.clone()).collect::<Vec<_>>().into();
    *TIER_NAME_CACHE.write().unwrap_or_else(|err| err.into_inner()) = Some((Instant::now(), Arc::clone(&names)));
    names
}

/// Test-only cache reset; the production cache has no invalidation hook
/// because the TTL is its only refresh path.
#[cfg(test)]
fn reset_tier_name_cache_for_test() {
    *TIER_NAME_CACHE.write().unwrap_or_else(|err| err.into_inner()) = None;
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
    let Some(store) = ecstore_resolve_object_store_handle() else {
        return false;
    };
    let Ok(bucket_incarnation_id) = store.bucket_incarnation_id(bucket).await else {
        return false;
    };
    ecstore_expiry_state_handle().write().await.enqueue_by_newer_noncurrent(
        bucket,
        to_delete_objs,
        event,
        src,
        bucket_incarnation_id,
    )
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

pub(crate) async fn scanner_disk_is_online(disk: &Disk) -> bool {
    EcstoreDiskAPI::is_online(disk).await
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

pub(crate) async fn save_config_with_preconditions<S>(
    api: Arc<S>,
    file: &str,
    data: Vec<u8>,
    preconditions: HTTPPreconditions,
) -> EcstoreResult<ScannerObjectInfo>
where
    S: ScannerObjectIO,
{
    let mut reader = ScannerPutObjReader::from_vec(data);
    api.put_object(
        RUSTFS_META_BUCKET,
        file,
        &mut reader,
        &ScannerObjectOptions {
            max_parity: true,
            http_preconditions: Some(preconditions),
            ..Default::default()
        },
    )
    .await
}

pub(crate) async fn save_config_with_publication_admission_for_epoch<S>(
    api: Arc<S>,
    file: &str,
    data: Vec<u8>,
    preconditions: HTTPPreconditions,
    expected_epoch: u64,
) -> EcstoreResult<ScannerObjectInfo>
where
    S: ScannerObjectIO + ScannerConfigObjectDelete,
{
    let Some(_admission) = scanner_publication_admission_for_epoch(api.clone(), expected_epoch).await else {
        return Err(EcstoreError::other(SCANNER_PUBLICATION_EPOCH_CHANGED));
    };
    save_config_with_preconditions(api, file, data, preconditions).await
}

pub(crate) const SCANNER_PUBLICATION_EPOCH_CHANGED: &str = "scanner publication epoch changed before commit";

pub(crate) fn scanner_publication_epoch_changed(error: &EcstoreError) -> bool {
    matches!(
        error,
        EcstoreError::Io(io_error) if io_error.to_string() == SCANNER_PUBLICATION_EPOCH_CHANGED
    )
}

pub(crate) async fn delete_config_with_publication_admission_for_epoch<S>(
    api: Arc<S>,
    bucket: &str,
    object: &str,
    opts: ScannerObjectOptions,
    expected_epoch: u64,
) -> EcstoreResult<ScannerObjectInfo>
where
    S: ScannerObjectIO + ScannerConfigObjectDelete,
{
    let Some(_admission) = scanner_publication_admission_for_epoch(api.clone(), expected_epoch).await else {
        return Err(EcstoreError::other(SCANNER_PUBLICATION_EPOCH_CHANGED));
    };
    api.delete_config_object(bucket, object, opts).await
}

/// Capture the storage-owned publication epoch without retaining the read
/// guard across a potentially slow metadata read. Callers must compare this
/// token with a fresh admission immediately before their conditional write.
pub(crate) async fn scanner_publication_epoch<S>(api: Arc<S>) -> Option<u64>
where
    S: ScannerConfigObjectDelete,
{
    let admission = api.scanner_data_usage_publication_admission().await?;
    Some(admission.epoch())
}

/// Re-admit a publication only when the storage-owned movement epoch is still
/// the one observed before the caller's metadata read. The returned guard
/// remains held through the caller's short conditional commit.
pub(crate) async fn scanner_publication_admission_for_epoch<S>(
    api: Arc<S>,
    expected_epoch: u64,
) -> Option<ScannerDataUsagePublicationAdmission>
where
    S: ScannerConfigObjectDelete,
{
    let admission = api.scanner_data_usage_publication_admission().await?;
    if admission.epoch() != expected_epoch {
        return None;
    }
    Some(admission)
}

pub(crate) async fn save_config_shared_with_preconditions<S>(
    api: Arc<S>,
    file: &str,
    data: Bytes,
    sha256hex: Option<String>,
    preconditions: HTTPPreconditions,
) -> EcstoreResult<ScannerObjectInfo>
where
    S: ScannerObjectIO,
{
    let mut reader = ScannerPutObjReader::from_prehashed_bytes(data, sha256hex)?;
    api.put_object(
        RUSTFS_META_BUCKET,
        file,
        &mut reader,
        &ScannerObjectOptions {
            max_parity: true,
            http_preconditions: Some(preconditions),
            ..Default::default()
        },
    )
    .await
}

pub(crate) async fn list_path_raw(rx: CancellationToken, opts: ListPathRawOptions) -> std::result::Result<(), DiskError> {
    ecstore_list_path_raw(rx, opts).await
}

pub(crate) async fn replace_bucket_usage_memory_from_info(data_usage_info: &rustfs_data_usage::DataUsageInfo) {
    ecstore_replace_bucket_usage_memory_from_info(data_usage_info).await;
}

pub(crate) async fn invalidate_data_usage_snapshot_cache() {
    ecstore_invalidate_data_usage_snapshot_cache().await;
}

pub(crate) async fn invalidate_admin_data_usage_snapshot_cache() {
    ecstore_invalidate_admin_data_usage_snapshot_cache().await;
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

#[async_trait::async_trait]
pub trait ScannerConfigObjectDelete: Send + Sync + std::fmt::Debug + 'static {
    async fn delete_config_object(
        &self,
        bucket: &str,
        object: &str,
        opts: ScannerObjectOptions,
    ) -> EcstoreResult<ScannerObjectInfo>;

    /// Acquire storage-owned admission for one short data-usage publication
    /// commit. Implementations without a storage-owned movement owner fail
    /// closed; test fixtures opt into the explicit unfenced helper.
    async fn scanner_data_usage_publication_admission(&self) -> Option<ScannerDataUsagePublicationAdmission> {
        None
    }
}

pub struct ScannerDataUsagePublicationAdmission {
    epoch: u64,
    _read_guard: Option<tokio::sync::OwnedRwLockReadGuard<()>>,
}

impl ScannerDataUsagePublicationAdmission {
    #[cfg(test)]
    pub(crate) fn unfenced() -> Self {
        Self {
            epoch: 0,
            _read_guard: None,
        }
    }

    fn fenced(read_guard: tokio::sync::OwnedRwLockReadGuard<()>, epoch: u64) -> Self {
        Self {
            epoch,
            _read_guard: Some(read_guard),
        }
    }

    pub(crate) fn epoch(&self) -> u64 {
        self.epoch
    }
}

#[async_trait::async_trait]
impl ScannerConfigObjectDelete for ECStore {
    async fn delete_config_object(
        &self,
        bucket: &str,
        object: &str,
        opts: ScannerObjectOptions,
    ) -> EcstoreResult<ScannerObjectInfo> {
        ObjectOperations::delete_object(self, bucket, object, opts).await
    }

    async fn scanner_data_usage_publication_admission(&self) -> Option<ScannerDataUsagePublicationAdmission> {
        let (read_guard, epoch) = self.scanner_data_usage_publication_admission_guard().await?;
        Some(ScannerDataUsagePublicationAdmission::fenced(read_guard, epoch))
    }
}

#[async_trait::async_trait]
impl ScannerConfigObjectDelete for SetDisks {
    async fn delete_config_object(
        &self,
        bucket: &str,
        object: &str,
        opts: ScannerObjectOptions,
    ) -> EcstoreResult<ScannerObjectInfo> {
        ObjectOperations::delete_object(self, bucket, object, opts).await
    }

    async fn scanner_data_usage_publication_admission(&self) -> Option<ScannerDataUsagePublicationAdmission> {
        let (read_guard, epoch) = self.scanner_data_usage_publication_admission_guard().await?;
        Some(ScannerDataUsagePublicationAdmission::fenced(read_guard, epoch))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn runtime_tier_names_serves_cached_arc_within_ttl() {
        reset_tier_name_cache_for_test();
        // The tier config manager is unconfigured in unit tests, so the
        // first call populates the cache from an empty tier list...
        let first = runtime_tier_names().await;
        assert!(first.is_empty());
        // ...and a second call within the TTL must return the cached Arc
        // (pointer-equal) without re-reading the manager.
        let second = runtime_tier_names().await;
        assert!(Arc::ptr_eq(&first, &second));
    }

    #[test]
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
    fn foreground_read_activity_keeps_larger_signal() {
        reset_foreground_read_activity_for_test();
        let _guard = ForegroundReadGuard::new();

        set_foreground_read_activity(3);
        assert_eq!(current_foreground_read_activity(), 3);

        set_foreground_read_activity(0);
        assert_eq!(current_foreground_read_activity(), 1);
    }

    #[test]
    fn scanner_runtime_guard_tracks_runtime_lifetime() {
        reset_scanner_runtime_instances_for_test();
        assert!(!scanner_runtime_initialized());

        {
            let _guard = ScannerRuntimeGuard::new();
            assert!(scanner_runtime_initialized());
        }

        assert!(!scanner_runtime_initialized());
    }
}
