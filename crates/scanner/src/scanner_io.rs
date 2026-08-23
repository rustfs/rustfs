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

use crate::data_usage_define::DATA_USAGE_CACHE_KEY_FORMAT;
use crate::scanner_budget::ScannerCycleBudget;
use crate::scanner_folder::{ScannerItem, scan_data_folder};
use crate::sleeper::SCANNER_SLEEPER;
use crate::{
    DATA_USAGE_CACHE_NAME, DATA_USAGE_ROOT, DataUsageCache, DataUsageCacheInfo, DataUsageCachePrepareOutcome,
    DataUsageCacheSource, DataUsageEntry, DataUsageEntryInfo, DataUsageInfo, DataUsageScanPlanDigest, DataUsageSnapshotSetState,
    ScannerError, SizeSummary, TierStats,
};
use futures::future::join_all;
use metrics::counter;
use rand::seq::SliceRandom as _;
use rustfs_common::heal_channel::HealScanMode;
use rustfs_common::metrics::{Metric, Metrics, emit_scan_bucket_drive_complete, emit_scan_bucket_drive_partial, global_metrics};
#[cfg(test)]
use rustfs_config::{ENV_SCANNER_MAX_CONCURRENT_DISK_SCANS, ENV_SCANNER_MAX_CONCURRENT_SET_SCANS};
use rustfs_data_usage::{BucketTargetUsageInfo, BucketUsageInfo};
use rustfs_filemeta::FileMeta;
use rustfs_lock::{LockError, NamespaceLockGuard};
use rustfs_utils::path::path_join_buf;
use s3s::dto::{
    BucketLifecycleConfiguration, ObjectLockConfiguration, ObjectLockEnabled, ReplicationConfiguration, VersioningConfiguration,
};
use sha2::{Digest as _, Sha256};
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{LazyLock, Mutex as StdMutex, MutexGuard};
use std::time::{Instant, SystemTime};
use std::{fmt::Debug, sync::Arc};
use time::OffsetDateTime;
use tokio::sync::{Mutex, Notify, Semaphore, mpsc};
use tokio::time::Duration;
use tokio_util::sync::CancellationToken;
use tokio_util::task::AbortOnDropHandle;
use tracing::{debug, error, warn};

use crate::ScannerObjectInfo as ObjectInfo;
use crate::storage_api::scan::NamespaceLocking as _;
use crate::storage_api::scanner_io::{BucketInfo, BucketOptions};
use crate::{
    BucketTargetSys, BucketVersioningSys, Disk, DiskError, ECStore, EcstoreError as Error, EcstoreResult as Result,
    RUSTFS_META_BUCKET, ReplicationConfig, STORAGE_FORMAT_FILE, ScannerConfigObjectDelete as _, ScannerDiskExt as _,
    ScannerLifecycleConfigExt as _, ScannerReplicationConfigExt as _, ScannerVersioningConfigExt as _, SetDisks, StorageError,
    enqueue_runtime_free_version, get_lifecycle_config, get_object_lock_config, get_replication_config, runtime_tier_names,
    scanner_publication_admission_for_epoch, scanner_publication_epoch, storageclass,
};

pub(crate) const SCANNER_SKIP_FILE_ERROR: &str = "skip file";
pub(crate) const SCANNER_METADATA_CORRUPT_ERROR: &str = "scanner metadata corrupt";
pub(crate) const SCANNER_METADATA_TRANSIENT_ERROR: &str = "scanner metadata transient";
const LOG_COMPONENT_SCANNER: &str = "scanner";
const LOG_SUBSYSTEM_IO: &str = "io";
// Mirrors `scanner_folder.rs` so the versioning-lookup fallback warn keeps its
// historical `rustfs::scanner::folder` lifecycle event identity after the
// lookup moved into `get_size`.
const LOG_SUBSYSTEM_LIFECYCLE: &str = "lifecycle";
const EVENT_SCANNER_LIFECYCLE_ACTION: &str = "scanner_lifecycle_action";
const EVENT_SCANNER_DISK_BUCKET_STATE: &str = "scanner_disk_bucket_state";
const EVENT_SCANNER_DATA_USAGE_STREAM: &str = "scanner_data_usage_stream";
const EVENT_SCANNER_CACHE_PERSIST_STATE: &str = "scanner_cache_persist_state";
const EVENT_SCANNER_SET_STATE: &str = "scanner_set_state";
const SCANNER_CACHE_LOCK_SUFFIX: &str = ".scanner-cycle.lock";
const SCANNER_CACHE_LOCK_POLL_INTERVAL: Duration = Duration::from_millis(250);
#[cfg(not(test))]
const SCANNER_CACHE_LOCK_LOSS_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(30);
#[cfg(test)]
const SCANNER_CACHE_LOCK_LOSS_SHUTDOWN_TIMEOUT: Duration = Duration::from_millis(50);

const METRIC_SCANNER_SET_SCAN_CONCURRENCY_LIMIT: &str = "rustfs_scanner_set_scan_concurrency_limit";
const METRIC_SCANNER_DISK_SCAN_CONCURRENCY_LIMIT: &str = "rustfs_scanner_disk_scan_concurrency_limit";
const METRIC_SCANNER_SET_SCAN_WAIT_SECONDS: &str = "rustfs_scanner_set_scan_wait_seconds";
const METRIC_SCANNER_DISK_SCAN_WAIT_SECONDS: &str = "rustfs_scanner_disk_scan_wait_seconds";
const METRIC_SCANNER_SET_SCANS_ACTIVE: &str = "rustfs_scanner_set_scans_active";
const METRIC_SCANNER_SET_SCANS_QUEUED: &str = "rustfs_scanner_set_scans_queued";
const METRIC_SCANNER_DISK_BUCKET_SCANS_ACTIVE: &str = "rustfs_scanner_disk_bucket_scans_active";
const METRIC_SCANNER_DISK_BUCKET_SCANS_QUEUED: &str = "rustfs_scanner_disk_bucket_scans_queued";

pub type DirtyUsageBuckets = HashMap<String, u64>;

#[derive(Clone, Debug)]
struct DirtyUsageSnapshot {
    buckets: Arc<DirtyUsageBuckets>,
    generation: u64,
    covers_all_pending: bool,
}

pub(crate) fn is_scanner_metadata_corrupt_error(err: &StorageError) -> bool {
    matches!(err, StorageError::Io(io) if io.to_string().starts_with(SCANNER_METADATA_CORRUPT_ERROR))
}

pub(crate) fn is_scanner_metadata_transient_error(err: &StorageError) -> bool {
    matches!(err, StorageError::Io(io) if io.to_string().starts_with(SCANNER_METADATA_TRANSIENT_ERROR))
}

fn scanner_metadata_corrupt_error(reason: impl std::fmt::Display, bucket: &str, object_path: &str) -> StorageError {
    StorageError::other(format!(
        "{SCANNER_METADATA_CORRUPT_ERROR}: {reason}, bucket={bucket}, object_path={object_path}"
    ))
}

fn scanner_metadata_transient_error(reason: impl std::fmt::Display, bucket: &str, object_path: &str) -> StorageError {
    StorageError::other(format!(
        "{SCANNER_METADATA_TRANSIENT_ERROR}: {reason}, bucket={bucket}, object_path={object_path}"
    ))
}

async fn object_lock_config_for_scanner_item(item: &ScannerItem) -> Option<Arc<ObjectLockConfiguration>> {
    if let Some(config) = item.object_lock.clone() {
        return Some(config);
    }

    get_object_lock_config(&item.bucket)
        .await
        .ok()
        .map(|(config, _)| Arc::new(config))
}

fn object_lock_config_enabled(config: &ObjectLockConfiguration) -> bool {
    config
        .object_lock_enabled
        .as_ref()
        .is_some_and(|enabled| enabled.as_str() == ObjectLockEnabled::ENABLED)
}

pub struct ScannerBucketScanPlan {
    buckets: Vec<BucketInfo>,
    all_buckets: Arc<Vec<BucketInfo>>,
    digest: DataUsageScanPlanDigest,
    leader_epoch: u64,
    /// Epoch captured once for the whole scanner cycle.  `None` is retained
    /// for unfenced test implementations; production plans always carry the
    /// admission token captured before bucket enumeration.
    publication_epoch: Option<u64>,
    dirty_usage_buckets: Arc<DirtyUsageBuckets>,
    bucket_failures: ScannerBucketFailureState,
    pending_maintenance_work: Arc<AtomicBool>,
    cache_cycle_floor: Arc<AtomicU64>,
}

#[derive(Clone, Default)]
struct ScannerBucketFailureState {
    hard: Arc<Mutex<HashSet<String>>>,
    partial: Arc<Mutex<HashSet<String>>>,
    namespace_not_found: Arc<Mutex<HashSet<String>>>,
}

fn scanner_bucket_plan_digest(buckets: &[BucketInfo], activity_digest: [u8; 32]) -> DataUsageScanPlanDigest {
    let mut buckets = buckets.iter().collect::<Vec<_>>();
    buckets.sort_unstable_by(|left, right| left.name.cmp(&right.name));

    let mut hasher = Sha256::new();
    hasher.update(activity_digest);
    hasher.update(u64::try_from(buckets.len()).unwrap_or(u64::MAX).to_be_bytes());
    for bucket in buckets {
        let name = bucket.name.as_bytes();
        hasher.update(u64::try_from(name.len()).unwrap_or(u64::MAX).to_be_bytes());
        hasher.update(name);
        match bucket.created {
            Some(created) => {
                hasher.update([1]);
                hasher.update(created.unix_timestamp_nanos().to_be_bytes());
            }
            None => hasher.update([0]),
        }
    }
    DataUsageScanPlanDigest(hasher.finalize().into())
}

fn scanner_bucket_cache_digest(
    scan_plan_digest: DataUsageScanPlanDigest,
    dirty_generation: Option<u64>,
) -> DataUsageScanPlanDigest {
    let Some(dirty_generation) = dirty_generation else {
        return scan_plan_digest;
    };

    let mut hasher = Sha256::new();
    hasher.update(scan_plan_digest.0);
    hasher.update(dirty_generation.to_be_bytes());
    DataUsageScanPlanDigest(hasher.finalize().into())
}

fn finalize_nsscanner_result(results: &[DataUsageCache], first_err: Option<Error>) -> Result<()> {
    if results.iter().any(|result| result.info.last_update.is_some()) {
        return Ok(());
    }

    if let Some(err) = first_err {
        return Err(err);
    }

    Ok(())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ScannerBucketScanStatus {
    Complete,
    Failed,
    Partial,
    NamespaceNotFound,
}

fn scanner_bucket_scan_status(has_failed: bool, has_partial: bool, has_namespace_not_found: bool) -> ScannerBucketScanStatus {
    if has_failed {
        ScannerBucketScanStatus::Failed
    } else if has_partial {
        ScannerBucketScanStatus::Partial
    } else if has_namespace_not_found {
        ScannerBucketScanStatus::NamespaceNotFound
    } else {
        ScannerBucketScanStatus::Complete
    }
}

fn classify_nsscanner_cycle(
    completed_all_sets: bool,
    budget_elapsed: bool,
    cancelled: bool,
    bucket_scan_status: ScannerBucketScanStatus,
    dirty_usage_status: DirtyUsageSnapshotStatus,
    activity_status: ScannerCycleActivityStatus,
) -> ScannerCycleStatus {
    if budget_elapsed
        || cancelled
        || !matches!(bucket_scan_status, ScannerBucketScanStatus::Complete)
        || dirty_usage_status == DirtyUsageSnapshotStatus::Unverified
    {
        return ScannerCycleStatus::Incomplete;
    }
    if !completed_all_sets {
        return ScannerCycleStatus::Incomplete;
    }

    match (activity_status, dirty_usage_status) {
        (ScannerCycleActivityStatus::Unchanged, DirtyUsageSnapshotStatus::Current) => ScannerCycleStatus::Complete,
        (ScannerCycleActivityStatus::Unverified, _) => ScannerCycleStatus::Incomplete,
        _ => ScannerCycleStatus::Superseded,
    }
}

fn should_publish_usage_snapshot(status: ScannerCycleStatus) -> bool {
    matches!(status, ScannerCycleStatus::Complete | ScannerCycleStatus::Superseded)
}

fn prepare_usage_snapshot_for_publication(
    status: ScannerCycleStatus,
    mut data_usage_info: DataUsageInfo,
) -> Option<DataUsageInfo> {
    if !should_publish_usage_snapshot(status) {
        return None;
    }

    data_usage_info.usage_snapshot_converged = Some(status == ScannerCycleStatus::Complete);
    Some(data_usage_info)
}

async fn publish_usage_snapshot(
    updates: &mpsc::Sender<DataUsageInfo>,
    status: ScannerCycleStatus,
    data_usage_info: DataUsageInfo,
) -> Result<bool> {
    let Some(data_usage_info) = prepare_usage_snapshot_for_publication(status, data_usage_info) else {
        return Ok(false);
    };
    send_data_usage_update(updates, data_usage_info).await?;
    Ok(true)
}

async fn publish_observational_snapshot(
    updates: &mpsc::Sender<DataUsageInfo>,
    mut data_usage_info: DataUsageInfo,
) -> Result<bool> {
    data_usage_info.usage_snapshot_complete = false;
    data_usage_info.usage_snapshot_partial = true;
    data_usage_info.usage_snapshot_converged = Some(false);
    send_data_usage_update(updates, data_usage_info).await?;
    Ok(true)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ScannerCycleActivityStatus {
    Unchanged,
    Changed,
    Unverified,
}

async fn scanner_cycle_activity_status(
    store: &ECStore,
    distributed: bool,
    before: &crate::scanner::ScannerActivitySnapshot,
) -> ScannerCycleActivityStatus {
    match crate::scanner::probe_scanner_activity(store, distributed).await {
        Ok(after) if after == *before => ScannerCycleActivityStatus::Unchanged,
        Ok(_) => ScannerCycleActivityStatus::Changed,
        Err(err) => {
            warn!(
                target: "rustfs::scanner::io",
                event = EVENT_SCANNER_SET_STATE,
                component = LOG_COMPONENT_SCANNER,
                subsystem = LOG_SUBSYSTEM_IO,
                state = "cycle_activity_probe_failed",
                error = %err,
                "Scanner cycle activity verification failed"
            );
            ScannerCycleActivityStatus::Unverified
        }
    }
}

fn scanner_results_have_pending_maintenance_work(results: &[DataUsageCache]) -> bool {
    results.iter().any(|result| !result.info.pending_heals.is_empty())
}

fn pending_maintenance_work_for_cycle(pending: &AtomicBool, results: &[DataUsageCache]) -> bool {
    pending.load(Ordering::Acquire) || scanner_results_have_pending_maintenance_work(results)
}

fn record_bucket_pending_maintenance_work(cache: &DataUsageCache, pending: &AtomicBool) {
    if !cache.info.pending_heals.is_empty() {
        pending.store(true, Ordering::Release);
    }
}

fn is_xl_meta_path(path: &str) -> bool {
    Path::new(path)
        .file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| name == STORAGE_FORMAT_FILE)
}

pub(crate) fn cache_root_entry_info(cache: &DataUsageCache) -> std::result::Result<DataUsageEntryInfo, ScannerError> {
    if cache.info.name.is_empty() {
        return Err(ScannerError::Other("scanner cache root name is empty".to_string()));
    }
    let entry = cache
        .checked_flatten_complete_scope(&cache.info.name)
        .ok_or_else(|| ScannerError::Other(format!("scanner cache root is missing or corrupt: {}", cache.info.name)))?;

    Ok(DataUsageEntryInfo {
        name: cache.info.name.clone(),
        parent: DATA_USAGE_ROOT.to_string(),
        entry,
    })
}

fn apply_bucket_result_to_cache(cache: &mut DataUsageCache, result: DataUsageEntryInfo, update_time: SystemTime) {
    cache.replace(&result.name, &result.parent, result.entry);
    cache.info.last_update = Some(update_time);
}

fn should_publish_completed_snapshot(completed_count: usize, total_count: usize, budget_elapsed: bool, cancelled: bool) -> bool {
    completed_count == total_count && !budget_elapsed && !cancelled
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum NamespaceScannerWorkerMode {
    Coordinator,
    RemoteV4(uuid::Uuid),
}

fn namespace_scanner_workers<T>(
    coordinator_disks: Vec<T>,
    remote_disks: Vec<(T, uuid::Uuid)>,
) -> Vec<(T, NamespaceScannerWorkerMode)> {
    let mut workers = Vec::with_capacity(coordinator_disks.len() + remote_disks.len());
    workers.extend(
        coordinator_disks
            .into_iter()
            .map(|disk| (disk, NamespaceScannerWorkerMode::Coordinator)),
    );
    workers.extend(
        remote_disks
            .into_iter()
            .map(|(disk, server_epoch)| (disk, NamespaceScannerWorkerMode::RemoteV4(server_epoch))),
    );
    workers
}

fn group_remote_disks_by_peer<T>(disks: Vec<T>, peer_key: impl Fn(&T) -> String) -> Vec<Vec<T>> {
    let mut groups = HashMap::<String, Vec<T>>::new();
    for disk in disks {
        groups.entry(peer_key(&disk)).or_default().push(disk);
    }
    groups.into_values().collect()
}

fn scanner_results_match_scan_scope(results: &[DataUsageCache], expected_sources: &HashSet<DataUsageCacheSource>) -> bool {
    if results.is_empty() {
        return false;
    }

    let sources_match_topology = results
        .iter()
        .map(|result| result.info.source)
        .collect::<Option<HashSet<_>>>()
        .is_some_and(|sources| sources.len() == results.len() && sources == *expected_sources);
    let plan_digests = results
        .iter()
        .map(|result| result.info.scan_plan_digest)
        .collect::<Option<HashSet<_>>>();
    let cycles = results.iter().map(|result| result.info.next_cycle).collect::<HashSet<_>>();
    let leader_epochs = results.iter().map(|result| result.info.leader_epoch).collect::<HashSet<_>>();

    sources_match_topology
        && plan_digests.is_some_and(|digests| digests.len() == 1)
        && cycles.len() == 1
        && leader_epochs.len() == 1
}

fn scanner_results_form_complete_snapshot(results: &[DataUsageCache], expected_sources: &HashSet<DataUsageCacheSource>) -> bool {
    results
        .iter()
        .all(|result| result.info.last_update.is_some() && result.info.snapshot_complete)
        && scanner_results_match_scan_scope(results, expected_sources)
}

fn checked_bucket_usage_info(entry: &DataUsageEntry) -> Option<BucketUsageInfo> {
    let mut usage = BucketUsageInfo {
        size: u64::try_from(entry.size).ok()?,
        versions_count: u64::try_from(entry.versions).ok()?,
        objects_count: u64::try_from(entry.objects).ok()?,
        delete_markers_count: u64::try_from(entry.delete_markers).ok()?,
        object_size_histogram: entry.obj_sizes.to_map(),
        object_versions_histogram: entry.obj_versions.to_map(),
        ..Default::default()
    };

    if let Some(replication) = &entry.replication_stats {
        usage.replica_size = replication.replica_size;
        usage.replica_count = replication.replica_count;
        for (target, stats) in &replication.targets {
            usage.replication_info.insert(
                target.clone(),
                BucketTargetUsageInfo {
                    replication_pending_size: stats.pending_size,
                    replicated_size: stats.replicated_size,
                    replication_failed_size: stats.failed_size,
                    replication_pending_count: stats.pending_count,
                    replication_failed_count: stats.failed_count,
                    replicated_count: stats.replicated_count,
                    ..Default::default()
                },
            );
        }
    }
    Some(usage)
}

#[async_trait::async_trait]
pub trait ScannerIO: Send + Sync + Debug + 'static {
    async fn nsscanner(
        &self,
        ctx: CancellationToken,
        budget: Arc<ScannerCycleBudget>,
        updates: mpsc::Sender<DataUsageInfo>,
        want_cycle: u64,
        scan_mode: HealScanMode,
    ) -> Result<()>;
}

#[async_trait::async_trait]
pub(crate) trait ScannerIOCycle: Send + Sync + Debug + 'static {
    async fn nsscanner_with_status(
        &self,
        ctx: CancellationToken,
        budget: Arc<ScannerCycleBudget>,
        updates: mpsc::Sender<DataUsageInfo>,
        want_cycle: u64,
        leader_epoch: u64,
        scan_mode: HealScanMode,
    ) -> Result<ScannerCycleResult>;
}

#[async_trait::async_trait]
pub trait ScannerIOCache: Send + Sync + Debug + 'static {
    async fn nsscanner_cache(
        self: Arc<Self>,
        ctx: CancellationToken,
        budget: Arc<ScannerCycleBudget>,
        scan_plan: ScannerBucketScanPlan,
        updates: mpsc::Sender<DataUsageCache>,
        want_cycle: u64,
        scan_mode: HealScanMode,
    ) -> Result<()>;
}

#[async_trait::async_trait]
pub trait ScannerIODisk: Send + Sync + Debug + 'static {
    async fn nsscanner_disk(
        self: Arc<Self>,
        ctx: CancellationToken,
        budget: Arc<ScannerCycleBudget>,
        set_disks: Vec<Arc<Disk>>,
        cache: DataUsageCache,
        updates: Option<mpsc::Sender<DataUsageEntry>>,
        scan_mode: HealScanMode,
    ) -> Result<ScannerDiskScanOutcome>;

    async fn get_size(&self, item: ScannerItem) -> Result<SizeSummary>;
}

#[derive(Debug)]
pub enum ScannerDiskScanOutcome {
    Complete(DataUsageCache),
    Partial(DataUsageCache),
    NamespaceNotFound(DataUsageCache),
}

pub(crate) async fn scanner_set_disk_inventory(set: &SetDisks) -> Vec<Arc<Disk>> {
    let membership = set.drive_membership_snapshot().await;
    let capacity = membership
        .online
        .len()
        .saturating_add(membership.suspect.len())
        .saturating_add(membership.returning.len())
        .saturating_add(membership.offline.len());
    let mut disks = Vec::with_capacity(capacity);
    disks.extend(membership.online);
    disks.extend(membership.suspect);
    disks.extend(membership.returning);
    disks.extend(membership.offline);
    disks
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ScannerCycleDeferReason {
    ActivityBaselineUnavailable,
    DataMovement,
}

impl ScannerCycleDeferReason {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::ActivityBaselineUnavailable => "activity_baseline_unavailable",
            Self::DataMovement => "data_movement",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ScannerCycleStatus {
    Complete,
    Incomplete,
    Superseded,
    Deferred(ScannerCycleDeferReason),
}

enum ScannerActivityPreflight {
    Ready(crate::scanner::ScannerActivitySnapshot),
    ActivityBaselineUnavailable(String),
    DataMovement,
}

fn scanner_activity_preflight(
    activity: std::result::Result<crate::scanner::ScannerActivitySnapshot, String>,
) -> ScannerActivityPreflight {
    match activity {
        Err(error) => ScannerActivityPreflight::ActivityBaselineUnavailable(error),
        Ok(snapshot) if !crate::scanner::scanner_activity_allows_usage_publication(&snapshot) => {
            ScannerActivityPreflight::DataMovement
        }
        Ok(snapshot) => ScannerActivityPreflight::Ready(snapshot),
    }
}

#[derive(Debug)]
pub(crate) struct ScannerCycleResult {
    pub(crate) status: ScannerCycleStatus,
    publication_epoch: Option<u64>,
    dirty_usage_clear: Option<DirtyUsageBuckets>,
    remote_dirty_usage_acknowledgements: Vec<crate::scanner::ScannerDirtyUsageAcknowledgement>,
    failed_dirty_usage: bool,
    pending_maintenance_work: bool,
    required_cycle_floor: Option<u64>,
}

impl ScannerCycleResult {
    pub(crate) fn new(status: ScannerCycleStatus, dirty_usage_clear: Option<DirtyUsageBuckets>) -> Self {
        Self {
            status,
            publication_epoch: None,
            dirty_usage_clear,
            remote_dirty_usage_acknowledgements: Vec::new(),
            failed_dirty_usage: false,
            pending_maintenance_work: false,
            required_cycle_floor: None,
        }
    }

    pub(crate) fn with_publication_epoch(mut self, publication_epoch: Option<u64>) -> Self {
        self.publication_epoch = publication_epoch;
        self
    }

    pub(crate) fn publication_epoch(&self) -> Option<u64> {
        self.publication_epoch
    }

    fn with_failed_dirty_usage(mut self, failed_dirty_usage: bool) -> Self {
        self.failed_dirty_usage = failed_dirty_usage;
        self
    }

    fn with_pending_maintenance_work(mut self, pending_maintenance_work: bool) -> Self {
        self.pending_maintenance_work = pending_maintenance_work;
        self
    }

    fn with_required_cycle_floor(mut self, required_cycle_floor: Option<u64>) -> Self {
        self.required_cycle_floor = required_cycle_floor;
        self
    }

    pub(crate) fn with_remote_dirty_usage_acknowledgements(
        mut self,
        acknowledgements: Vec<crate::scanner::ScannerDirtyUsageAcknowledgement>,
    ) -> Self {
        self.remote_dirty_usage_acknowledgements = acknowledgements;
        self
    }

    pub(crate) fn acknowledge_durable_usage(self) -> Vec<crate::scanner::ScannerDirtyUsageAcknowledgement> {
        if let Some(snapshot) = self.dirty_usage_clear {
            clear_dirty_usage_buckets(&snapshot);
        }
        self.remote_dirty_usage_acknowledgements
    }

    pub(crate) fn has_dirty_usage_to_acknowledge(&self) -> bool {
        self.dirty_usage_clear.as_ref().is_some_and(|snapshot| !snapshot.is_empty())
            || !self.remote_dirty_usage_acknowledgements.is_empty()
    }

    pub(crate) fn has_failed_dirty_usage(&self) -> bool {
        self.failed_dirty_usage
    }

    pub(crate) fn has_pending_maintenance_work(&self) -> bool {
        self.pending_maintenance_work
    }

    pub(crate) fn required_cycle_floor(&self) -> Option<u64> {
        self.required_cycle_floor
    }
}

mod cache;
mod dirty_usage;
mod guards;
mod io_cache;
mod io_cycle;
mod io_disk;
#[cfg(test)]
mod publish_gate_tests;
#[cfg(test)]
mod tests;

use cache::*;
use dirty_usage::*;
use guards::*;

pub(crate) use cache::{DataUsageCacheScanState, acquire_scanner_cache_locks, current_cache_root_or_prepare};
pub use dirty_usage::{
    ScannerDirtyUsageAckError, ScannerDirtyUsageState, acknowledge_dirty_usage_generation, clear_dirty_usage_bucket,
    record_dirty_usage_bucket, record_scanner_maintenance_change, scanner_activity_epoch, scanner_dirty_usage_state,
    scanner_maintenance_generation,
};
#[cfg(test)]
pub(crate) use dirty_usage::{clear_dirty_usage_buckets_for_tests, dirty_usage_buckets_for_tests};
pub(crate) use dirty_usage::{
    dirty_usage_bucket_notified, dirty_usage_buckets_pending, dirty_usage_generation, scanner_maintenance_changed,
};
