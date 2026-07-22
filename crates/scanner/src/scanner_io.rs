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

use crate::scanner_budget::ScannerCycleBudget;
use crate::scanner_folder::{ScannerItem, scan_data_folder};
use crate::sleeper::SCANNER_SLEEPER;
use crate::{
    DATA_USAGE_CACHE_NAME, DATA_USAGE_ROOT, DataUsageCache, DataUsageCacheInfo, DataUsageCachePrepareOutcome,
    DataUsageCacheSource, DataUsageEntry, DataUsageEntryInfo, DataUsageInfo, DataUsageScanPlanDigest, ScannerError, SizeSummary,
    TierStats,
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
use rustfs_utils::path::path_join_buf;
use s3s::dto::{BucketLifecycleConfiguration, ObjectLockConfiguration, ObjectLockEnabled, ReplicationConfiguration};
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
use tracing::{debug, error, warn};

use crate::ScannerObjectInfo as ObjectInfo;
use crate::storage_api::scan::NamespaceLocking as _;
use crate::storage_api::scanner_io::{BucketInfo, BucketOptions};
use crate::{
    BucketTargetSys, BucketVersioningSys, Disk, DiskError, ECStore, EcstoreError as Error, EcstoreResult as Result,
    RUSTFS_META_BUCKET, ReplicationConfig, STORAGE_FORMAT_FILE, ScannerDiskExt as _, ScannerLifecycleConfigExt as _,
    ScannerReplicationConfigExt as _, ScannerVersioningConfigExt as _, SetDisks, StorageError, enqueue_runtime_free_version,
    get_lifecycle_config, get_object_lock_config, get_replication_config, list_runtime_tiers, storageclass,
};

pub(crate) const SCANNER_SKIP_FILE_ERROR: &str = "skip file";
pub(crate) const SCANNER_METADATA_CORRUPT_ERROR: &str = "scanner metadata corrupt";
pub(crate) const SCANNER_METADATA_TRANSIENT_ERROR: &str = "scanner metadata transient";
const LOG_COMPONENT_SCANNER: &str = "scanner";
const LOG_SUBSYSTEM_IO: &str = "io";
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

static DIRTY_USAGE_BUCKET_GENERATION: AtomicU64 = AtomicU64::new(0);
static DIRTY_USAGE_BUCKETS: LazyLock<StdMutex<DirtyUsageBuckets>> = LazyLock::new(|| StdMutex::new(HashMap::new()));
static DIRTY_USAGE_BUCKET_NOTIFY: LazyLock<Notify> = LazyLock::new(Notify::new);
static SCANNER_ACTIVITY_EPOCH: LazyLock<String> = LazyLock::new(|| format!("{:032x}", rand::random::<u128>()));
static SCANNER_MAINTENANCE_GENERATION: AtomicU64 = AtomicU64::new(0);
static SCANNER_MAINTENANCE_NOTIFY: LazyLock<Notify> = LazyLock::new(Notify::new);

fn dirty_usage_buckets() -> MutexGuard<'static, DirtyUsageBuckets> {
    DIRTY_USAGE_BUCKETS.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn usize_to_u64_saturated(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

fn advance_generation(generation: &AtomicU64) -> u64 {
    generation
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| Some(current.saturating_add(1)))
        .map_or_else(|current| current, |previous| previous.saturating_add(1))
}

pub fn record_dirty_usage_bucket(bucket: &str) {
    if bucket.is_empty() {
        return;
    }

    let pending_buckets = {
        let mut dirty_buckets = dirty_usage_buckets();
        let generation = advance_generation(&DIRTY_USAGE_BUCKET_GENERATION);
        dirty_buckets.insert(bucket.to_string(), generation);
        dirty_buckets.len()
    };
    global_metrics().record_scanner_dirty_usage_pending(usize_to_u64_saturated(pending_buckets));
    DIRTY_USAGE_BUCKET_NOTIFY.notify_one();
}

pub fn record_scanner_maintenance_change(bucket: &str) {
    if bucket.is_empty() {
        return;
    }

    advance_generation(&SCANNER_MAINTENANCE_GENERATION);
    SCANNER_MAINTENANCE_NOTIFY.notify_one();
    record_dirty_usage_bucket(bucket);
}

pub fn scanner_maintenance_generation() -> u64 {
    SCANNER_MAINTENANCE_GENERATION.load(Ordering::Acquire)
}

pub(crate) async fn scanner_maintenance_changed() {
    SCANNER_MAINTENANCE_NOTIFY.notified().await;
}

pub fn scanner_activity_epoch() -> &'static str {
    SCANNER_ACTIVITY_EPOCH.as_str()
}

pub(crate) fn dirty_usage_generation() -> u64 {
    DIRTY_USAGE_BUCKET_GENERATION.load(Ordering::Acquire)
}

pub fn clear_dirty_usage_bucket(bucket: &str) {
    if bucket.is_empty() {
        return;
    }

    let pending_buckets = {
        let mut dirty_buckets = dirty_usage_buckets();
        dirty_buckets.remove(bucket);
        advance_generation(&DIRTY_USAGE_BUCKET_GENERATION);
        dirty_buckets.len()
    };
    global_metrics().record_scanner_dirty_usage_clear(usize_to_u64_saturated(pending_buckets));
}

fn snapshot_dirty_usage_buckets(buckets: &[BucketInfo], absent_generation_cutoff: u64) -> DirtyUsageSnapshot {
    let (snapshot, generation, covers_all_pending) = {
        let dirty_buckets = dirty_usage_buckets();
        let listed_buckets = dirty_buckets
            .values()
            .any(|generation| *generation > absent_generation_cutoff)
            .then(|| buckets.iter().map(|bucket| bucket.name.as_str()).collect::<HashSet<_>>());
        let snapshot = dirty_buckets
            .iter()
            .filter(|(bucket, generation)| {
                **generation <= absent_generation_cutoff
                    || listed_buckets
                        .as_ref()
                        .is_some_and(|listed_buckets| listed_buckets.contains(bucket.as_str()))
            })
            .map(|(bucket, generation)| (bucket.clone(), *generation))
            .collect::<DirtyUsageBuckets>();
        let generation = DIRTY_USAGE_BUCKET_GENERATION.load(Ordering::Acquire);
        let covers_all_pending = generation == absent_generation_cutoff && snapshot.len() == dirty_buckets.len();
        (snapshot, generation, covers_all_pending)
    };
    global_metrics().record_scanner_dirty_usage_cycle_snapshot(usize_to_u64_saturated(snapshot.len()));
    DirtyUsageSnapshot {
        buckets: Arc::new(snapshot),
        generation,
        covers_all_pending,
    }
}

pub(crate) fn dirty_usage_buckets_pending() -> bool {
    !dirty_usage_buckets().is_empty()
}

pub(crate) async fn dirty_usage_bucket_notified() {
    DIRTY_USAGE_BUCKET_NOTIFY.notified().await;
}

fn clear_dirty_usage_buckets(snapshot: &DirtyUsageBuckets) {
    let (cleared_buckets, pending_buckets) = {
        let mut dirty_buckets = dirty_usage_buckets();
        let mut cleared_buckets = 0usize;
        for (bucket, generation) in snapshot {
            if dirty_buckets.get(bucket).is_some_and(|current| current == generation) {
                dirty_buckets.remove(bucket);
                cleared_buckets += 1;
            }
        }
        if cleared_buckets > 0 {
            advance_generation(&DIRTY_USAGE_BUCKET_GENERATION);
        }
        (cleared_buckets, dirty_buckets.len())
    };
    global_metrics()
        .record_scanner_dirty_usage_cycle_clear(usize_to_u64_saturated(cleared_buckets), usize_to_u64_saturated(pending_buckets));
}

fn dirty_usage_buckets_excluding_failed(snapshot: &DirtyUsageBuckets, failed_buckets: &HashSet<String>) -> DirtyUsageBuckets {
    snapshot
        .iter()
        .filter(|(bucket, _)| !failed_buckets.contains(*bucket))
        .map(|(bucket, generation)| (bucket.clone(), *generation))
        .collect()
}

fn should_clear_dirty_usage_snapshot(
    result_ok: bool,
    completed_all_sets: bool,
    budget_elapsed: bool,
    activity_and_generation_current: bool,
    dirty_buckets: &DirtyUsageBuckets,
    failed_buckets: &HashSet<String>,
) -> Option<DirtyUsageBuckets> {
    if result_ok && completed_all_sets && !budget_elapsed && activity_and_generation_current {
        return Some(dirty_usage_buckets_excluding_failed(dirty_buckets, failed_buckets));
    }

    None
}

async fn record_failed_dirty_bucket(failed_buckets: &Arc<Mutex<HashSet<String>>>, bucket: &str) {
    failed_buckets.lock().await.insert(bucket.to_string());
}

async fn record_partial_dirty_bucket(partial_buckets: &Arc<Mutex<HashSet<String>>>, bucket: &str) {
    partial_buckets.lock().await.insert(bucket.to_string());
}

async fn requeue_bucket_work(
    bucket_tx: &mpsc::Sender<BucketInfo>,
    bucket: &BucketInfo,
    work_guard: &mut BucketWorkGuard,
) -> bool {
    if bucket_tx.send(bucket.clone()).await.is_err() {
        return false;
    }

    work_guard.mark_requeued();
    true
}

async fn mark_unprocessed_bucket_work_failed(
    bucket_rx: &Mutex<mpsc::Receiver<BucketInfo>>,
    remaining: &Arc<AtomicUsize>,
    complete: &CancellationToken,
    failed_buckets: &Arc<Mutex<HashSet<String>>>,
) -> usize {
    let mut failed_count = 0;
    let mut receiver = bucket_rx.lock().await;
    while let Some(bucket) = receiver.recv().await {
        record_failed_dirty_bucket(failed_buckets, &bucket.name).await;
        drop(BucketWorkGuard::new(remaining.clone(), complete.clone()));
        failed_count += 1;
    }
    failed_count
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DirtyUsageSnapshotStatus {
    Current,
    Changed,
    Unverified,
}

fn dirty_usage_snapshot_status(snapshot: &DirtyUsageSnapshot) -> DirtyUsageSnapshotStatus {
    let generation = DIRTY_USAGE_BUCKET_GENERATION.load(Ordering::Acquire);
    if generation == u64::MAX {
        DirtyUsageSnapshotStatus::Unverified
    } else if snapshot.covers_all_pending && generation == snapshot.generation {
        DirtyUsageSnapshotStatus::Current
    } else {
        DirtyUsageSnapshotStatus::Changed
    }
}

#[cfg(test)]
fn dirty_usage_bucket_count() -> usize {
    dirty_usage_buckets().len()
}

#[cfg(test)]
pub(crate) fn clear_dirty_usage_buckets_for_tests() {
    dirty_usage_buckets().clear();
}

#[cfg(test)]
pub(crate) fn dirty_usage_buckets_for_tests() -> DirtyUsageBuckets {
    dirty_usage_buckets().clone()
}

fn bucket_usage_scan_order(
    buckets: &[BucketInfo],
    old_cache: &DataUsageCache,
    dirty_buckets: &DirtyUsageBuckets,
) -> Vec<BucketInfo> {
    let mut ordered = Vec::with_capacity(buckets.len());

    for bucket in buckets {
        if dirty_buckets.contains_key(&bucket.name) {
            ordered.push(bucket.clone());
        }
    }

    for bucket in buckets {
        if !dirty_buckets.contains_key(&bucket.name) && old_cache.find(&bucket.name).is_none() {
            ordered.push(bucket.clone());
        }
    }

    for bucket in buckets {
        if !dirty_buckets.contains_key(&bucket.name) && old_cache.find(&bucket.name).is_some() {
            ordered.push(bucket.clone());
        }
    }

    ordered
}

fn record_set_scan_concurrency_limit(limit: usize) {
    metrics::gauge!(METRIC_SCANNER_SET_SCAN_CONCURRENCY_LIMIT).set(limit as f64);
    global_metrics().record_scanner_set_scan_state(Some(limit), None, None);
}

fn record_set_scans_queued(count: usize) {
    metrics::gauge!(METRIC_SCANNER_SET_SCANS_QUEUED).set(count as f64);
    global_metrics().record_scanner_set_scan_state(None, Some(count), None);
}

fn record_set_scans_active(count: usize) {
    metrics::gauge!(METRIC_SCANNER_SET_SCANS_ACTIVE).set(count as f64);
    global_metrics().record_scanner_set_scan_state(None, None, Some(count));
}

fn record_disk_scan_concurrency_limit(pool: &str, set: &str, limit: usize) {
    metrics::gauge!(
        METRIC_SCANNER_DISK_SCAN_CONCURRENCY_LIMIT,
        "pool" => pool.to_owned(),
        "set" => set.to_owned()
    )
    .set(limit as f64);
    global_metrics().record_scanner_disk_bucket_scan_state(pool, set, Some(limit), None, None);
}

fn record_disk_bucket_scans_active(count: usize, pool: &str, set: &str) {
    metrics::gauge!(
        METRIC_SCANNER_DISK_BUCKET_SCANS_ACTIVE,
        "pool" => pool.to_owned(),
        "set" => set.to_owned()
    )
    .set(count as f64);
    global_metrics().record_scanner_disk_bucket_scan_state(pool, set, None, None, Some(count));
}

struct SetScanActiveGuard {
    active: Arc<AtomicUsize>,
}

impl SetScanActiveGuard {
    fn new(active: Arc<AtomicUsize>) -> Self {
        let active_count = active.fetch_add(1, Ordering::Relaxed) + 1;
        record_set_scans_active(active_count);
        Self { active }
    }
}

impl Drop for SetScanActiveGuard {
    fn drop(&mut self) {
        let active_count = decrement_atomic_usize(&self.active);
        record_set_scans_active(active_count);
    }
}

struct DiskBucketScanActiveGuard {
    active: Arc<AtomicUsize>,
    pool: String,
    set: String,
}

struct BucketWorkGuard {
    remaining: Arc<AtomicUsize>,
    complete: CancellationToken,
    requeued: bool,
}

impl BucketWorkGuard {
    fn new(remaining: Arc<AtomicUsize>, complete: CancellationToken) -> Self {
        Self {
            remaining,
            complete,
            requeued: false,
        }
    }

    fn mark_requeued(&mut self) {
        self.requeued = true;
    }
}

impl Drop for BucketWorkGuard {
    fn drop(&mut self) {
        if !self.requeued && self.remaining.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.complete.cancel();
        }
    }
}

impl DiskBucketScanActiveGuard {
    fn new(active: Arc<AtomicUsize>, pool: String, set: String) -> Self {
        let active_count = active.fetch_add(1, Ordering::Relaxed) + 1;
        record_disk_bucket_scans_active(active_count, &pool, &set);
        Self { active, pool, set }
    }
}

impl Drop for DiskBucketScanActiveGuard {
    fn drop(&mut self) {
        let active_count = decrement_atomic_usize(&self.active);
        record_disk_bucket_scans_active(active_count, &self.pool, &self.set);
    }
}

struct BucketDriveFailureGuard {
    failed: bool,
}

impl BucketDriveFailureGuard {
    fn new() -> Self {
        Self { failed: true }
    }

    fn mark_not_failed(&mut self) {
        self.failed = false;
    }
}

impl Drop for BucketDriveFailureGuard {
    fn drop(&mut self) {
        if self.failed {
            global_metrics().record_scan_bucket_drive_failure();
        }
    }
}

struct DiskBucketScanGaugeReset {
    pool: String,
    set: String,
}

impl DiskBucketScanGaugeReset {
    fn new(pool: String, set: String) -> Self {
        Self { pool, set }
    }
}

impl Drop for DiskBucketScanGaugeReset {
    fn drop(&mut self) {
        reset_disk_bucket_scan_gauges(&self.pool, &self.set);
    }
}

fn decrement_atomic_usize(counter: &AtomicUsize) -> usize {
    counter
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| Some(current.saturating_sub(1)))
        .map(|previous| previous.saturating_sub(1))
        .unwrap_or_else(|current| current)
}

fn increment_atomic_usize(counter: &AtomicUsize) -> usize {
    counter
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| Some(current.saturating_add(1)))
        .map(|previous| previous.saturating_add(1))
        .unwrap_or_else(|current| current)
}

fn record_disk_bucket_scans_queued(count: usize, pool: &str, set: &str) {
    metrics::gauge!(
        METRIC_SCANNER_DISK_BUCKET_SCANS_QUEUED,
        "pool" => pool.to_owned(),
        "set" => set.to_owned()
    )
    .set(count as f64);
    global_metrics().record_scanner_disk_bucket_scan_state(pool, set, None, Some(count), None);
}

fn decrement_disk_bucket_scans_queued(counter: &AtomicUsize, pool: &str, set: &str) {
    let queued_count = decrement_atomic_usize(counter);
    record_disk_bucket_scans_queued(queued_count, pool, set);
}

fn increment_disk_bucket_scans_queued(counter: &AtomicUsize, pool: &str, set: &str) {
    let queued_count = increment_atomic_usize(counter);
    record_disk_bucket_scans_queued(queued_count, pool, set);
}

fn reset_set_scan_gauges() {
    record_set_scan_concurrency_limit(0);
    record_set_scans_queued(0);
    record_set_scans_active(0);
    global_metrics().reset_scanner_set_scan_state();
}

fn reset_disk_bucket_scan_gauges(pool: &str, set: &str) {
    record_disk_scan_concurrency_limit(pool, set, 0);
    record_disk_bucket_scans_queued(0, pool, set);
    record_disk_bucket_scans_active(0, pool, set);
}

fn scanner_concurrency_limit(configured: usize, available: usize) -> usize {
    if available == 0 {
        return 0;
    }

    if crate::current_foreground_read_activity() > 0 {
        return 1;
    }

    if configured == 0 {
        available
    } else {
        configured.min(available).max(1)
    }
}

fn scanner_max_concurrent_set_scans(available: usize) -> usize {
    scanner_concurrency_limit(crate::runtime_config::scanner_max_concurrent_set_scans_configured(), available)
}

fn scanner_max_concurrent_disk_scans(available: usize) -> usize {
    scanner_concurrency_limit(crate::runtime_config::scanner_max_concurrent_disk_scans_configured(), available)
}

fn scanner_budgeted_concurrency_limit(configured_limit: usize, requires_serial_progress_accounting: bool) -> usize {
    if requires_serial_progress_accounting {
        1
    } else {
        configured_limit
    }
}

fn record_set_scan_failure(first_err: &mut Option<Error>, err: Error) {
    if first_err.is_none() {
        *first_err = Some(err);
    }
}

fn scanner_task_join_error(stage: &str, err: tokio::task::JoinError) -> Error {
    Error::other(format!("{stage} task join failed: {err}"))
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
        .checked_flatten(&cache.info.name)
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
    total_count > 0 && completed_count == total_count && !budget_elapsed && !cancelled
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum NamespaceScannerWorkerMode {
    Coordinator,
    RemoteV3(uuid::Uuid),
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
            .map(|(disk, server_epoch)| (disk, NamespaceScannerWorkerMode::RemoteV3(server_epoch))),
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

pub(crate) fn scanner_cache_lock_resource(cache_name: &str) -> String {
    path_join_buf(&[crate::BUCKET_META_PREFIX, cache_name, SCANNER_CACHE_LOCK_SUFFIX])
}

pub(crate) fn scanner_cache_lock_timeout() -> Duration {
    Duration::from_secs(rustfs_utils::get_env_u64("RUSTFS_LOCK_ACQUIRE_TIMEOUT", 5))
}

async fn await_scanner_disk_shutdown<F>(scan: Pin<&mut F>)
where
    F: Future,
{
    let _ = tokio::time::timeout(SCANNER_CACHE_LOCK_LOSS_SHUTDOWN_TIMEOUT, scan).await;
}

pub(crate) fn cache_snapshot_is_current(
    cache: &DataUsageCache,
    name: &str,
    source: DataUsageCacheSource,
    next_cycle: u64,
    leader_epoch: u64,
    scan_plan_digest: DataUsageScanPlanDigest,
) -> bool {
    cache.info.name == name
        && cache.info.source == Some(source)
        && cache.info.snapshot_complete
        && cache.info.scan_plan_digest == Some(scan_plan_digest)
        && cache.info.last_update.is_some()
        && cache.info.next_cycle == next_cycle
        && cache.info.leader_epoch == leader_epoch
}

fn completed_data_usage_info(
    results: &[DataUsageCache],
    expected_sources: &HashSet<DataUsageCacheSource>,
    all_buckets: &[String],
    bucket_plan_complete: bool,
    budget_elapsed: bool,
    cancelled: bool,
) -> Option<(DataUsageInfo, SystemTime)> {
    if !bucket_plan_complete {
        return None;
    }
    let completed_set_count = results.iter().filter(|result| result.info.last_update.is_some()).count();
    if !should_publish_completed_snapshot(completed_set_count, results.len(), budget_elapsed, cancelled) {
        return None;
    }
    if !scanner_results_form_complete_snapshot(results, expected_sources) {
        return None;
    }

    if results.iter().any(|result| result.root().is_none()) {
        return None;
    }

    let mut total = DataUsageEntry::default();
    let mut buckets_usage = HashMap::with_capacity(all_buckets.len());
    for bucket in all_buckets {
        let mut merged = DataUsageEntry::default();
        for result in results {
            let entry = result.checked_flatten(bucket)?;
            if !merged.checked_merge(&entry) {
                return None;
            }
        }
        if !total.checked_merge(&merged) {
            return None;
        }
        buckets_usage.insert(bucket.clone(), checked_bucket_usage_info(&merged)?);
    }

    let merged_last_update = results.iter().filter_map(|result| result.info.last_update).max()?;
    let data_usage_info = DataUsageInfo {
        last_update: Some(merged_last_update),
        scanner_cycle: Some(results.first()?.info.next_cycle),
        objects_total_count: u64::try_from(total.objects).ok()?,
        versions_total_count: u64::try_from(total.versions).ok()?,
        delete_markers_total_count: u64::try_from(total.delete_markers).ok()?,
        objects_total_size: u64::try_from(total.size).ok()?,
        buckets_count: u64::try_from(all_buckets.len()).ok()?,
        buckets_usage,
        ..Default::default()
    };
    Some((data_usage_info, merged_last_update))
}

#[cfg(test)]
mod publish_gate_tests {
    use super::*;
    use rustfs_data_usage::{ReplicationAllStats, ReplicationStats};

    const TEST_PLAN_DIGEST: DataUsageScanPlanDigest = DataUsageScanPlanDigest([7; 32]);

    #[test]
    fn should_publish_completed_snapshot_requires_full_clean_cycle() {
        assert!(should_publish_completed_snapshot(3, 3, false, false));
        assert!(!should_publish_completed_snapshot(2, 3, false, false));
        assert!(!should_publish_completed_snapshot(3, 3, true, false));
        assert!(!should_publish_completed_snapshot(3, 3, false, true));
        assert!(!should_publish_completed_snapshot(0, 0, false, false));
    }

    fn incomplete_scope_cache(source: DataUsageCacheSource) -> DataUsageCache {
        DataUsageCache {
            info: DataUsageCacheInfo {
                name: DATA_USAGE_ROOT.to_string(),
                source: Some(source),
                snapshot_complete: false,
                scan_plan_digest: Some(TEST_PLAN_DIGEST),
                ..Default::default()
            },
            ..Default::default()
        }
    }

    #[test]
    fn incomplete_scan_scope_requires_every_expected_set_marker() {
        let first_source = DataUsageCacheSource::new(0, 0);
        let second_source = DataUsageCacheSource::new(1, 0);
        let expected_sources = HashSet::from([first_source, second_source]);
        let first = incomplete_scope_cache(first_source);
        let second = incomplete_scope_cache(second_source);

        assert!(scanner_results_match_scan_scope(&[first.clone(), second], &expected_sources));
        assert!(!scanner_results_form_complete_snapshot(
            &[first.clone(), incomplete_scope_cache(second_source)],
            &expected_sources
        ));
        assert!(!scanner_results_match_scan_scope(
            &[first.clone(), DataUsageCache::default()],
            &expected_sources
        ));
        assert!(!scanner_results_match_scan_scope(
            &[first.clone(), incomplete_scope_cache(first_source)],
            &expected_sources
        ));

        let mut mismatched_plan = incomplete_scope_cache(second_source);
        mismatched_plan.info.scan_plan_digest = Some(DataUsageScanPlanDigest([8; 32]));
        assert!(!scanner_results_match_scan_scope(&[first, mismatched_plan], &expected_sources));
    }

    fn completed_root_cache(bucket: &str, objects: usize, update_secs: u64, source: DataUsageCacheSource) -> DataUsageCache {
        let mut cache = DataUsageCache {
            info: DataUsageCacheInfo {
                name: DATA_USAGE_ROOT.to_string(),
                last_update: Some(SystemTime::UNIX_EPOCH + Duration::from_secs(update_secs)),
                source: Some(source),
                snapshot_complete: true,
                scan_plan_digest: Some(TEST_PLAN_DIGEST),
                ..Default::default()
            },
            ..Default::default()
        };
        cache.replace(
            bucket,
            DATA_USAGE_ROOT,
            DataUsageEntry {
                objects,
                size: objects.saturating_mul(10),
                ..Default::default()
            },
        );
        cache
    }

    fn completed_data_usage_info_for_test(
        results: &[DataUsageCache],
        all_buckets: &[String],
        budget_elapsed: bool,
        cancelled: bool,
    ) -> Option<(DataUsageInfo, SystemTime)> {
        let expected_sources = results.iter().filter_map(|result| result.info.source).collect::<HashSet<_>>();
        completed_data_usage_info(results, &expected_sources, all_buckets, true, budget_elapsed, cancelled)
    }

    #[test]
    fn completed_data_usage_info_requires_every_set_before_publish() {
        let all_buckets = vec!["bucket-a".to_string(), "bucket-b".to_string()];
        let mut first_set = completed_root_cache("bucket-a", 1, 10, DataUsageCacheSource::new(0, 0));
        first_set.replace("bucket-b", DATA_USAGE_ROOT, DataUsageEntry::default());
        let mut second_set = completed_root_cache("bucket-b", 2, 20, DataUsageCacheSource::new(1, 0));
        second_set.replace("bucket-a", DATA_USAGE_ROOT, DataUsageEntry::default());

        assert!(
            completed_data_usage_info_for_test(&[first_set.clone(), DataUsageCache::default()], &all_buckets, false, false)
                .is_none()
        );
        assert!(
            completed_data_usage_info_for_test(&[first_set.clone(), second_set.clone()], &all_buckets, true, false).is_none()
        );
        assert!(
            completed_data_usage_info_for_test(&[first_set.clone(), second_set.clone()], &all_buckets, false, true).is_none()
        );

        let (data_usage_info, last_update) =
            completed_data_usage_info_for_test(&[first_set, second_set], &all_buckets, false, false)
                .expect("all completed sets should produce a publishable data usage snapshot");
        assert_eq!(last_update, SystemTime::UNIX_EPOCH + Duration::from_secs(20));
        assert_eq!(data_usage_info.scanner_cycle, Some(0));
        assert_eq!(data_usage_info.objects_total_count, 3);
        assert_eq!(data_usage_info.buckets_usage.len(), 2);
    }

    #[test]
    fn complete_usage_candidate_with_changed_generation_is_superseded() {
        let all_buckets = vec!["bucket".to_string()];
        let completed_set = completed_root_cache("bucket", 2, 10, DataUsageCacheSource::new(0, 0));
        let completed_usage = completed_data_usage_info_for_test(&[completed_set], &all_buckets, false, false);

        assert!(completed_usage.is_some());
        assert_eq!(
            classify_nsscanner_cycle(
                completed_usage.is_some(),
                false,
                false,
                ScannerBucketScanStatus::Complete,
                DirtyUsageSnapshotStatus::Changed,
                ScannerCycleActivityStatus::Unchanged,
            ),
            ScannerCycleStatus::Superseded
        );
    }

    #[test]
    fn completed_data_usage_info_adds_same_bucket_across_unique_sets() {
        let all_buckets = vec!["bucket".to_string()];
        let first_set = completed_root_cache("bucket", 2, 10, DataUsageCacheSource::new(0, 0));
        let mut second_set = completed_root_cache("bucket", 3, 20, DataUsageCacheSource::new(1, 0));
        let second_entry = second_set.find("bucket").cloned().expect("second set bucket entry");
        second_set.replace(
            "bucket",
            DATA_USAGE_ROOT,
            DataUsageEntry {
                versions: 4,
                delete_markers: 1,
                ..second_entry
            },
        );

        let (data_usage_info, last_update) =
            completed_data_usage_info_for_test(&[first_set, second_set], &all_buckets, false, false)
                .expect("unique completed set snapshots should be aggregated");
        let bucket = data_usage_info.buckets_usage.get("bucket").expect("merged bucket usage");

        assert_eq!(last_update, SystemTime::UNIX_EPOCH + Duration::from_secs(20));
        assert_eq!(data_usage_info.objects_total_count, 5);
        assert_eq!(data_usage_info.objects_total_size, 50);
        assert_eq!(bucket.objects_count, 5);
        assert_eq!(bucket.size, 50);
        assert_eq!(bucket.versions_count, 4);
        assert_eq!(bucket.delete_markers_count, 1);
    }

    #[test]
    fn completed_data_usage_info_flattens_nested_bucket_entries() {
        let all_buckets = vec!["bucket".to_string()];
        let mut first_set = completed_root_cache("bucket", 1, 10, DataUsageCacheSource::new(0, 0));
        let mut nested = DataUsageEntry {
            objects: 2,
            versions: 3,
            size: 2048,
            replication_stats: Some(ReplicationAllStats {
                targets: HashMap::from([(
                    "arn:target".to_string(),
                    ReplicationStats {
                        replicated_size: 2048,
                        replicated_count: 2,
                        ..Default::default()
                    },
                )]),
                replica_size: 2048,
                replica_count: 2,
            }),
            ..Default::default()
        };
        nested.obj_sizes.add(2048);
        nested.obj_versions.add(3);
        first_set.replace("bucket/prefix", "bucket", nested);
        let second_set = completed_root_cache("bucket", 3, 20, DataUsageCacheSource::new(1, 0));

        let (data_usage_info, _) = completed_data_usage_info_for_test(&[first_set, second_set], &all_buckets, false, false)
            .expect("nested bucket entries should be flattened before aggregation");
        let bucket = data_usage_info.buckets_usage.get("bucket").expect("merged bucket usage");

        assert_eq!(data_usage_info.objects_total_count, 6);
        assert_eq!(data_usage_info.objects_total_size, 2088);
        assert_eq!(bucket.objects_count, 6);
        assert_eq!(bucket.versions_count, 3);
        assert_eq!(bucket.object_size_histogram["BETWEEN_1024_B_AND_64_KB"], 1);
        assert_eq!(bucket.object_versions_histogram["BETWEEN_2_AND_10"], 1);
        assert_eq!(bucket.replica_size, 2048);
        assert_eq!(bucket.replica_count, 2);
        assert_eq!(bucket.replication_info["arn:target"].replicated_size, 2048);
        assert_eq!(bucket.replication_info["arn:target"].replicated_count, 2);
    }

    #[test]
    fn completed_data_usage_info_rejects_cyclic_bucket_entries() {
        let all_buckets = vec!["bucket".to_string()];
        let mut cache = completed_root_cache("bucket", 1, 10, DataUsageCacheSource::new(0, 0));
        cache.replace(
            "bucket/prefix",
            "bucket",
            DataUsageEntry {
                objects: 1,
                size: 10,
                ..Default::default()
            },
        );
        cache
            .cache
            .get_mut(&crate::hash_path("bucket/prefix").key())
            .expect("nested entry")
            .add_child(&crate::hash_path("bucket"));

        assert!(completed_data_usage_info_for_test(&[cache], &all_buckets, false, false).is_none());
    }

    #[test]
    fn completed_data_usage_info_rejects_duplicate_or_incomplete_set_sources() {
        let all_buckets = vec!["bucket".to_string()];
        let source = DataUsageCacheSource::new(0, 0);
        let first_set = completed_root_cache("bucket", 2, 10, source);
        let duplicate_set = completed_root_cache("bucket", 3, 20, source);

        assert!(completed_data_usage_info_for_test(&[first_set.clone(), duplicate_set], &all_buckets, false, false).is_none());

        let mut incomplete_set = completed_root_cache("bucket", 3, 20, DataUsageCacheSource::new(1, 0));
        incomplete_set.info.snapshot_complete = false;
        assert!(completed_data_usage_info_for_test(&[first_set, incomplete_set], &all_buckets, false, false).is_none());
    }

    #[test]
    fn completed_data_usage_info_requires_exact_topology_sources() {
        let all_buckets = vec!["bucket".to_string()];
        let first_set = completed_root_cache("bucket", 2, 10, DataUsageCacheSource::new(0, 0));
        let unexpected_set = completed_root_cache("bucket", 3, 20, DataUsageCacheSource::new(99, 99));
        let expected_sources = HashSet::from([DataUsageCacheSource::new(0, 0), DataUsageCacheSource::new(1, 0)]);

        assert!(
            completed_data_usage_info(&[first_set, unexpected_set], &expected_sources, &all_buckets, true, false, false)
                .is_none()
        );
    }

    #[test]
    fn completed_data_usage_info_rejects_incomplete_bucket_plan() {
        let all_buckets = vec!["bucket".to_string()];
        let set = completed_root_cache("bucket", 2, 10, DataUsageCacheSource::new(0, 0));
        let expected_sources = HashSet::from([DataUsageCacheSource::new(0, 0)]);

        assert!(completed_data_usage_info(&[set], &expected_sources, &all_buckets, false, false, false).is_none());
    }

    #[test]
    fn completed_data_usage_info_rejects_missing_bucket_from_any_set() {
        let all_buckets = vec!["bucket-a".to_string(), "bucket-b".to_string()];
        let mut complete_set = completed_root_cache("bucket-a", 2, 10, DataUsageCacheSource::new(0, 0));
        complete_set.replace("bucket-b", DATA_USAGE_ROOT, DataUsageEntry::default());
        let missing_bucket_set = completed_root_cache("bucket-a", 3, 20, DataUsageCacheSource::new(1, 0));

        assert!(completed_data_usage_info_for_test(&[complete_set, missing_bucket_set], &all_buckets, false, false).is_none());
    }

    #[test]
    fn completed_data_usage_info_rejects_mixed_scan_plans() {
        let all_buckets = vec!["bucket".to_string()];
        let first_set = completed_root_cache("bucket", 2, 10, DataUsageCacheSource::new(0, 0));
        let mut second_set = completed_root_cache("bucket", 3, 20, DataUsageCacheSource::new(1, 0));
        second_set.info.scan_plan_digest = Some(DataUsageScanPlanDigest([8; 32]));

        assert!(completed_data_usage_info_for_test(&[first_set, second_set], &all_buckets, false, false).is_none());
    }

    #[test]
    fn completed_data_usage_info_rejects_mixed_scanner_cycles() {
        let all_buckets = vec!["bucket".to_string()];
        let mut first_set = completed_root_cache("bucket", 2, 10, DataUsageCacheSource::new(0, 0));
        let mut second_set = completed_root_cache("bucket", 3, 20, DataUsageCacheSource::new(1, 0));
        first_set.info.next_cycle = 12;
        second_set.info.next_cycle = 13;

        assert!(completed_data_usage_info_for_test(&[first_set, second_set], &all_buckets, false, false).is_none());
    }

    #[test]
    fn completed_data_usage_info_rejects_mixed_leader_epochs() {
        let all_buckets = vec!["bucket".to_string()];
        let mut first_set = completed_root_cache("bucket", 2, 10, DataUsageCacheSource::new(0, 0));
        let mut second_set = completed_root_cache("bucket", 3, 20, DataUsageCacheSource::new(1, 0));
        first_set.info.leader_epoch = 11;
        second_set.info.leader_epoch = 12;

        assert!(completed_data_usage_info_for_test(&[first_set, second_set], &all_buckets, false, false).is_none());
    }

    #[test]
    fn completed_data_usage_info_rejects_counter_overflow() {
        let all_buckets = vec!["bucket".to_string()];
        let first_set = completed_root_cache("bucket", usize::MAX, 10, DataUsageCacheSource::new(0, 0));
        let second_set = completed_root_cache("bucket", 1, 20, DataUsageCacheSource::new(1, 0));

        assert!(completed_data_usage_info_for_test(&[first_set, second_set], &all_buckets, false, false).is_none());
    }

    #[test]
    fn current_cache_snapshot_requires_matching_complete_source_and_cycle() {
        let source = DataUsageCacheSource::new(1, 2);
        let mut cache = completed_root_cache("bucket", 1, 10, source);
        cache.info.next_cycle = 10;

        assert!(cache_snapshot_is_current(&cache, DATA_USAGE_ROOT, source, 10, 0, TEST_PLAN_DIGEST));
        assert!(!cache_snapshot_is_current(&cache, "bucket", source, 10, 0, TEST_PLAN_DIGEST));
        assert!(!cache_snapshot_is_current(
            &cache,
            DATA_USAGE_ROOT,
            DataUsageCacheSource::new(2, 1),
            10,
            0,
            TEST_PLAN_DIGEST
        ));
        assert!(!cache_snapshot_is_current(&cache, DATA_USAGE_ROOT, source, 11, 0, TEST_PLAN_DIGEST));
        assert!(!cache_snapshot_is_current(
            &cache,
            DATA_USAGE_ROOT,
            source,
            10,
            0,
            DataUsageScanPlanDigest([8; 32])
        ));
        cache.info.leader_epoch = 2;
        assert!(!cache_snapshot_is_current(&cache, DATA_USAGE_ROOT, source, 10, 1, TEST_PLAN_DIGEST));
        assert!(cache_snapshot_is_current(&cache, DATA_USAGE_ROOT, source, 10, 2, TEST_PLAN_DIGEST));
        cache.info.next_cycle = 11;
        assert!(!cache_snapshot_is_current(&cache, DATA_USAGE_ROOT, source, 10, 2, TEST_PLAN_DIGEST));
    }

    #[test]
    fn namespace_scanner_worker_selection_keeps_coordinator_fallback_disks() {
        let server_epoch = uuid::Uuid::new_v4();
        let workers = namespace_scanner_workers(vec!["local", "legacy-remote"], vec![("v3", server_epoch)]);

        assert_eq!(
            workers,
            vec![
                ("local", NamespaceScannerWorkerMode::Coordinator),
                ("legacy-remote", NamespaceScannerWorkerMode::Coordinator),
                ("v3", NamespaceScannerWorkerMode::RemoteV3(server_epoch)),
            ]
        );
        assert!(namespace_scanner_workers::<()>(Vec::new(), Vec::new()).is_empty());
    }

    #[test]
    fn remote_scanner_capability_probes_are_grouped_by_peer() {
        let mut groups = group_remote_disks_by_peer(vec![("node-a", 0), ("node-b", 0), ("node-a", 1), ("node-b", 1)], |disk| {
            disk.0.to_string()
        });
        groups.sort_by_key(|group| group[0].0);

        assert_eq!(groups.len(), 2);
        assert_eq!(groups[0], vec![("node-a", 0), ("node-a", 1)]);
        assert_eq!(groups[1], vec![("node-b", 0), ("node-b", 1)]);
    }

    #[test]
    fn scanner_bucket_plan_digest_is_order_independent_and_membership_sensitive() {
        let activity_digest = [4; 32];
        let buckets = vec![
            BucketInfo {
                name: "photos".to_string(),
                ..Default::default()
            },
            BucketInfo {
                name: "videos".to_string(),
                ..Default::default()
            },
        ];
        let reversed = vec![buckets[1].clone(), buckets[0].clone()];
        let changed = vec![
            buckets[0].clone(),
            BucketInfo {
                name: "archives".to_string(),
                ..Default::default()
            },
        ];
        let mut regenerated = buckets.clone();
        regenerated[0].created = Some(OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(1));

        assert_eq!(
            scanner_bucket_plan_digest(&buckets, activity_digest),
            scanner_bucket_plan_digest(&reversed, activity_digest)
        );
        assert_ne!(
            scanner_bucket_plan_digest(&buckets, activity_digest),
            scanner_bucket_plan_digest(&changed, activity_digest)
        );
        assert_ne!(
            scanner_bucket_plan_digest(&buckets, activity_digest),
            scanner_bucket_plan_digest(&regenerated, activity_digest)
        );
        assert_ne!(
            scanner_bucket_plan_digest(&buckets, activity_digest),
            scanner_bucket_plan_digest(&buckets, [5; 32])
        );
    }

    #[test]
    fn dirty_bucket_cache_digest_changes_with_generation() {
        let source = DataUsageCacheSource::new(0, 0);
        let plan = DataUsageScanPlanDigest([9; 32]);
        let first = scanner_bucket_cache_digest(plan, Some(7));
        let second = scanner_bucket_cache_digest(plan, Some(8));
        let mut cache = DataUsageCache {
            info: DataUsageCacheInfo {
                name: "photos".to_string(),
                next_cycle: 11,
                last_update: Some(SystemTime::now()),
                source: Some(source),
                snapshot_complete: true,
                scan_plan_digest: Some(first),
                ..Default::default()
            },
            ..Default::default()
        };
        cache.replace("photos", "", DataUsageEntry::default());

        assert_eq!(scanner_bucket_cache_digest(plan, None), plan);
        assert!(cache_snapshot_is_current(&cache, "photos", source, 11, 0, first));
        assert!(!cache_snapshot_is_current(&cache, "photos", source, 11, 0, second));
    }

    #[test]
    fn count_budget_serializes_set_and_disk_work() {
        assert_eq!(scanner_budgeted_concurrency_limit(8, true), 1);
        assert_eq!(scanner_budgeted_concurrency_limit(8, false), 8);
    }

    #[test]
    fn requeued_bucket_work_is_only_completed_after_retry() {
        let remaining = Arc::new(AtomicUsize::new(1));
        let complete = CancellationToken::new();
        let mut first = BucketWorkGuard::new(remaining.clone(), complete.clone());
        first.mark_requeued();
        drop(first);
        assert_eq!(remaining.load(Ordering::Acquire), 1);
        assert!(!complete.is_cancelled());

        drop(BucketWorkGuard::new(remaining.clone(), complete.clone()));
        assert_eq!(remaining.load(Ordering::Acquire), 0);
        assert!(complete.is_cancelled());
    }

    #[tokio::test]
    async fn exhausted_workers_mark_all_queued_bucket_work_failed() {
        let (tx, rx) = mpsc::channel(2);
        tx.send(BucketInfo {
            name: "photos".to_string(),
            ..Default::default()
        })
        .await
        .expect("queue photos");
        tx.send(BucketInfo {
            name: "videos".to_string(),
            ..Default::default()
        })
        .await
        .expect("queue videos");
        drop(tx);

        let receiver = Mutex::new(rx);
        let remaining = Arc::new(AtomicUsize::new(2));
        let complete = CancellationToken::new();
        let failed = Arc::new(Mutex::new(HashSet::new()));

        let failed_count = mark_unprocessed_bucket_work_failed(&receiver, &remaining, &complete, &failed).await;

        assert_eq!(failed_count, 2);
        assert_eq!(remaining.load(Ordering::Acquire), 0);
        assert!(complete.is_cancelled());
        assert_eq!(*failed.lock().await, HashSet::from(["photos".to_string(), "videos".to_string()]));
    }

    #[tokio::test]
    async fn requeued_bucket_remains_pending_for_another_worker() {
        let (tx, mut rx) = mpsc::channel(1);
        let remaining = Arc::new(AtomicUsize::new(1));
        let complete = CancellationToken::new();
        let mut guard = BucketWorkGuard::new(remaining.clone(), complete.clone());
        let bucket = BucketInfo {
            name: "photos".to_string(),
            ..Default::default()
        };

        assert!(requeue_bucket_work(&tx, &bucket, &mut guard).await);
        drop(guard);

        assert_eq!(remaining.load(Ordering::Acquire), 1);
        assert!(!complete.is_cancelled());
        assert_eq!(rx.recv().await.expect("requeued bucket").name, "photos");
    }
}

async fn send_cache_root_entry_info(
    bucket_result_tx: &mpsc::Sender<DataUsageEntryInfo>,
    cache: &DataUsageCache,
    pending_maintenance_work: &AtomicBool,
) -> std::result::Result<(), ScannerError> {
    let root = cache_root_entry_info(cache)?;
    record_bucket_pending_maintenance_work(cache, pending_maintenance_work);
    bucket_result_tx
        .send(root)
        .await
        .map_err(|err| ScannerError::Other(format!("scanner cache root channel closed: {err}")))
}

async fn persist_and_publish_cache_snapshot(
    store: Arc<SetDisks>,
    updates: &mpsc::Sender<DataUsageCache>,
    mut cache_snapshot: DataUsageCache,
    cache_cycle_floor: &AtomicU64,
) -> Option<SystemTime> {
    let source = cache_snapshot.info.source?;
    let lock_resource = scanner_cache_lock_resource(DATA_USAGE_CACHE_NAME);
    let ns_lock = match store.new_ns_lock(RUSTFS_META_BUCKET, &lock_resource).await {
        Ok(lock) => lock,
        Err(err) => {
            error!(
                target: "rustfs::scanner::io",
                event = EVENT_SCANNER_CACHE_PERSIST_STATE,
                component = LOG_COMPONENT_SCANNER,
                subsystem = LOG_SUBSYSTEM_IO,
                cache_name = DATA_USAGE_CACHE_NAME,
                state = "lock_create_failed",
                error = %err,
                "Scanner cache snapshot lock creation failed"
            );
            return None;
        }
    };
    let guard = match ns_lock.get_write_lock_quiet(scanner_cache_lock_timeout()).await {
        Ok(guard) => guard,
        Err(err) => {
            error!(
                target: "rustfs::scanner::io",
                event = EVENT_SCANNER_CACHE_PERSIST_STATE,
                component = LOG_COMPONENT_SCANNER,
                subsystem = LOG_SUBSYSTEM_IO,
                cache_name = DATA_USAGE_CACHE_NAME,
                state = "lock_acquire_failed",
                error = %err,
                "Scanner cache snapshot lock acquisition failed"
            );
            return None;
        }
    };

    let mut persisted = DataUsageCache::default();
    let revisions = match persisted.load_with_revisions(store.clone(), DATA_USAGE_CACHE_NAME).await {
        Ok(revisions) => revisions,
        Err(err) => {
            error!(
                target: "rustfs::scanner::io",
                event = EVENT_SCANNER_CACHE_PERSIST_STATE,
                component = LOG_COMPONENT_SCANNER,
                subsystem = LOG_SUBSYSTEM_IO,
                cache_name = DATA_USAGE_CACHE_NAME,
                state = "load_or_revision_lookup_failed",
                error = %err,
                "Scanner cache snapshot load or revision lookup failed"
            );
            return None;
        }
    };
    let scan_plan_digest = cache_snapshot.info.scan_plan_digest?;
    if persisted.info.next_cycle > cache_snapshot.info.next_cycle {
        cache_cycle_floor.fetch_max(persisted.info.next_cycle, Ordering::AcqRel);
        warn!(
            target: "rustfs::scanner::io",
            event = EVENT_SCANNER_CACHE_PERSIST_STATE,
            component = LOG_COMPONENT_SCANNER,
            subsystem = LOG_SUBSYSTEM_IO,
            cache_name = DATA_USAGE_CACHE_NAME,
            requested_cycle = cache_snapshot.info.next_cycle,
            persisted_cycle = persisted.info.next_cycle,
            state = "stale_cycle_rejected",
            "Scanner rejected a set cache cycle regression"
        );
        return None;
    }
    if persisted.info.leader_epoch > cache_snapshot.info.leader_epoch {
        error!(
            target: "rustfs::scanner::io",
            event = EVENT_SCANNER_CACHE_PERSIST_STATE,
            component = LOG_COMPONENT_SCANNER,
            subsystem = LOG_SUBSYSTEM_IO,
            cache_name = DATA_USAGE_CACHE_NAME,
            requested_epoch = cache_snapshot.info.leader_epoch,
            persisted_epoch = persisted.info.leader_epoch,
            state = "stale_leader_rejected",
            "Scanner rejected a set cache snapshot from an older leader epoch"
        );
        return None;
    }
    if cache_snapshot_is_current(
        &persisted,
        DATA_USAGE_ROOT,
        source,
        cache_snapshot.info.next_cycle,
        cache_snapshot.info.leader_epoch,
        scan_plan_digest,
    ) {
        cache_snapshot = persisted;
    } else {
        if guard.is_lock_lost() {
            error!(
                target: "rustfs::scanner::io",
                event = EVENT_SCANNER_CACHE_PERSIST_STATE,
                component = LOG_COMPONENT_SCANNER,
                subsystem = LOG_SUBSYSTEM_IO,
                cache_name = DATA_USAGE_CACHE_NAME,
                state = "lock_lost",
                "Scanner cache snapshot save skipped after lock loss"
            );
            return None;
        }

        let done_save = Metrics::time(Metric::SaveUsage);
        if let Err(e) = cache_snapshot
            .save_with_revisions(store, DATA_USAGE_CACHE_NAME, &revisions)
            .await
        {
            error!(
                target: "rustfs::scanner::io",
                event = EVENT_SCANNER_CACHE_PERSIST_STATE,
                component = LOG_COMPONENT_SCANNER,
                subsystem = LOG_SUBSYSTEM_IO,
                cache_name = DATA_USAGE_CACHE_NAME,
                state = "save_failed",
                error = %e,
                "Scanner cache snapshot persistence failed"
            );
            done_save();
            return None;
        }
        done_save();
    }
    if guard.is_lock_lost() {
        error!(
            target: "rustfs::scanner::io",
            event = EVENT_SCANNER_CACHE_PERSIST_STATE,
            component = LOG_COMPONENT_SCANNER,
            subsystem = LOG_SUBSYSTEM_IO,
            cache_name = DATA_USAGE_CACHE_NAME,
            state = "lock_lost_after_save",
            "Scanner cache snapshot publish skipped after lock loss"
        );
        return None;
    }
    drop(guard);
    let last_update = cache_snapshot.info.last_update;

    if let Err(e) = updates.send(cache_snapshot).await {
        error!(
            target: "rustfs::scanner::io",
            event = EVENT_SCANNER_CACHE_PERSIST_STATE,
            component = LOG_COMPONENT_SCANNER,
            subsystem = LOG_SUBSYSTEM_IO,
            cache_name = DATA_USAGE_CACHE_NAME,
            state = "publish_failed",
            error = %e,
            "Scanner cache snapshot publish failed"
        );
    }

    last_update
}

async fn send_data_usage_update(updates: &mpsc::Sender<DataUsageInfo>, data_usage_info: DataUsageInfo) -> Result<()> {
    updates.send(data_usage_info).await.map_err(|e| {
        error!(
            target: "rustfs::scanner::io",
            event = EVENT_SCANNER_DATA_USAGE_STREAM,
            component = LOG_COMPONENT_SCANNER,
            subsystem = LOG_SUBSYSTEM_IO,
            state = "send_failed",
            error = %e,
            "Scanner data usage publish failed"
        );
        StorageError::other("scanner data usage receiver closed before update delivery")
    })
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
pub(crate) enum ScannerCycleStatus {
    Complete,
    Incomplete,
    Superseded,
}

#[derive(Debug)]
pub(crate) struct ScannerCycleResult {
    pub(crate) status: ScannerCycleStatus,
    dirty_usage_clear: Option<DirtyUsageBuckets>,
    failed_dirty_usage: bool,
    pending_maintenance_work: bool,
    required_cycle_floor: Option<u64>,
}

impl ScannerCycleResult {
    pub(crate) fn new(status: ScannerCycleStatus, dirty_usage_clear: Option<DirtyUsageBuckets>) -> Self {
        Self {
            status,
            dirty_usage_clear,
            failed_dirty_usage: false,
            pending_maintenance_work: false,
            required_cycle_floor: None,
        }
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

    pub(crate) fn acknowledge_durable_usage(self) {
        if let Some(snapshot) = self.dirty_usage_clear {
            clear_dirty_usage_buckets(&snapshot);
        }
    }

    pub(crate) fn has_dirty_usage_to_acknowledge(&self) -> bool {
        self.dirty_usage_clear.as_ref().is_some_and(|snapshot| !snapshot.is_empty())
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

#[async_trait::async_trait]
impl ScannerIO for ECStore {
    async fn nsscanner(
        &self,
        ctx: CancellationToken,
        budget: Arc<ScannerCycleBudget>,
        updates: mpsc::Sender<DataUsageInfo>,
        want_cycle: u64,
        scan_mode: HealScanMode,
    ) -> Result<()> {
        // This public path can prove delivery to the receiver, but not that
        // the receiver persisted the update. Keep dirty usage pending unless
        // the main scanner confirms durability through nsscanner_with_status.
        let leader_epoch = crate::scanner::current_scanner_leader_epoch()
            .await
            .map_err(|err| StorageError::other(format!("failed to resolve scanner leader epoch: {err}")))?;
        ScannerIOCycle::nsscanner_with_status(self, ctx, budget, updates, want_cycle, leader_epoch, scan_mode).await?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl ScannerIOCycle for ECStore {
    #[tracing::instrument(skip(self, budget, updates))]
    async fn nsscanner_with_status(
        &self,
        ctx: CancellationToken,
        budget: Arc<ScannerCycleBudget>,
        updates: mpsc::Sender<DataUsageInfo>,
        want_cycle: u64,
        leader_epoch: u64,
        scan_mode: HealScanMode,
    ) -> Result<ScannerCycleResult> {
        let child_token = ctx.child_token();

        let distributed = self.setup_is_dist_erasure().await;
        let activity_before = match crate::scanner::probe_scanner_activity(self, distributed).await {
            Ok(snapshot) => snapshot,
            Err(err) => {
                warn!(
                    target: "rustfs::scanner::io",
                    event = EVENT_SCANNER_SET_STATE,
                    component = LOG_COMPONENT_SCANNER,
                    subsystem = LOG_SUBSYSTEM_IO,
                    state = "cycle_activity_baseline_failed",
                    error = %err,
                    "Scanner cycle skipped because cluster activity could not be baselined"
                );
                return Ok(ScannerCycleResult::new(ScannerCycleStatus::Incomplete, None));
            }
        };
        if !crate::scanner::scanner_activity_allows_usage_publication(&activity_before) {
            debug!(
                target: "rustfs::scanner::io",
                event = EVENT_SCANNER_SET_STATE,
                component = LOG_COMPONENT_SCANNER,
                subsystem = LOG_SUBSYSTEM_IO,
                state = "cycle_data_movement_active",
                "Scanner cycle deferred while rebalance or decommission data movement is active"
            );
            return Ok(ScannerCycleResult::new(ScannerCycleStatus::Incomplete, None));
        }
        let dirty_generation_before_bucket_list = dirty_usage_generation();
        let bucket_listing = self.list_bucket_for_scanner(&BucketOptions::default()).await?;
        let mut bucket_plan_complete = bucket_listing.topology_complete;
        let all_buckets = Arc::new(bucket_listing.buckets);
        let expected_sources = Arc::new(
            self.pools
                .iter()
                .flat_map(|pool| {
                    pool.disk_set
                        .iter()
                        .map(|set| DataUsageCacheSource::new(set.pool_index, set.set_index))
                })
                .collect::<HashSet<_>>(),
        );
        let mut buckets_by_source = HashMap::with_capacity(bucket_listing.set_buckets.len());
        for scope in bucket_listing.set_buckets {
            let source = DataUsageCacheSource::new(scope.pool_index, scope.set_index);
            if buckets_by_source.insert(source, scope.buckets).is_some() {
                bucket_plan_complete = false;
            }
        }
        bucket_plan_complete &= buckets_by_source.keys().copied().collect::<HashSet<_>>() == *expected_sources;
        let scan_plan_digest =
            scanner_bucket_plan_digest(&all_buckets, crate::scanner::scanner_activity_snapshot_digest(&activity_before));
        let dirty_usage_snapshot = Arc::new(snapshot_dirty_usage_buckets(&all_buckets, dirty_generation_before_bucket_list));
        let cache_cycle_floor = Arc::new(AtomicU64::new(want_cycle));

        if all_buckets.is_empty() {
            reset_set_scan_gauges();
            if !bucket_plan_complete {
                return Ok(ScannerCycleResult::new(ScannerCycleStatus::Incomplete, None));
            }
            let activity_status = scanner_cycle_activity_status(self, distributed, &activity_before).await;
            let dirty_usage_status = dirty_usage_snapshot_status(&dirty_usage_snapshot);
            let status = classify_nsscanner_cycle(
                true,
                false,
                ctx.is_cancelled(),
                ScannerBucketScanStatus::Complete,
                dirty_usage_status,
                activity_status,
            );
            if status != ScannerCycleStatus::Complete {
                return Ok(ScannerCycleResult::new(status, None));
            }
            let empty_usage = DataUsageInfo {
                last_update: Some(SystemTime::now()),
                scanner_cycle: Some(want_cycle),
                ..Default::default()
            };
            send_data_usage_update(&updates, empty_usage).await?;
            let dirty_usage_clear = Some(dirty_usage_snapshot.buckets.as_ref().clone());
            return Ok(ScannerCycleResult::new(status, dirty_usage_clear));
        }

        let total_results = expected_sources.len();
        if total_results == 0 {
            warn!(
                target: "rustfs::scanner::io",
                event = EVENT_SCANNER_SET_STATE,
                component = LOG_COMPONENT_SCANNER,
                subsystem = LOG_SUBSYSTEM_IO,
                bucket_count = all_buckets.len(),
                state = "no_disk_sets",
                "Scanner set state update detected missing disk sets"
            );
            reset_set_scan_gauges();
            return Ok(ScannerCycleResult::new(ScannerCycleStatus::Incomplete, None));
        }

        let set_scan_limit = scanner_budgeted_concurrency_limit(
            scanner_max_concurrent_set_scans(total_results),
            budget.requires_serial_progress_accounting(),
        );
        let bucket_failures = ScannerBucketFailureState::default();
        let pending_maintenance_work = Arc::new(AtomicBool::new(false));
        record_set_scan_concurrency_limit(set_scan_limit);
        debug!(
            target: "rustfs::scanner::io",
            event = EVENT_SCANNER_SET_STATE,
            component = LOG_COMPONENT_SCANNER,
            subsystem = LOG_SUBSYSTEM_IO,
            total_sets = total_results,
            concurrency_limit = set_scan_limit,
            state = "concurrency_budget",
            "Scanner set concurrency budget resolved"
        );
        let set_scan_semaphore = Arc::new(Semaphore::new(set_scan_limit));
        let queued_set_scans = Arc::new(AtomicUsize::new(total_results));
        let active_set_scans = Arc::new(AtomicUsize::new(0));
        record_set_scans_queued(total_results);
        record_set_scans_active(0);

        let results = vec![DataUsageCache::default(); total_results];
        let results_mutex: Arc<Mutex<Vec<DataUsageCache>>> = Arc::new(Mutex::new(results));
        let first_err_mutex: Arc<Mutex<Option<Error>>> = Arc::new(Mutex::new(None));
        let mut results_index = 0usize;
        let mut wait_futs = Vec::new();

        for pool in self.pools.iter() {
            for set in pool.disk_set.iter() {
                let results_index_clone = results_index;
                results_index += 1;
                // Clone the Arc to move it into the spawned task
                let set_clone: Arc<SetDisks> = Arc::clone(set);
                let source = DataUsageCacheSource::new(set.pool_index, set.set_index);
                let set_buckets = buckets_by_source.remove(&source).unwrap_or_default();
                let pool_label = set.pool_index.to_string();
                let set_label = set.set_index.to_string();

                let child_token_clone = child_token.clone();
                let budget_clone = budget.clone();
                let want_cycle_clone = want_cycle;
                let scan_mode_clone = scan_mode;
                let results_mutex_clone = results_mutex.clone();
                let first_err_mutex_clone = first_err_mutex.clone();
                let set_scan_semaphore_clone = set_scan_semaphore.clone();
                let queued_set_scans_clone = queued_set_scans.clone();
                let active_set_scans_clone = active_set_scans.clone();

                let (tx, mut rx) = mpsc::channel::<DataUsageCache>(1);

                // Spawn task to receive and store results
                let receiver_fut = tokio::spawn(async move {
                    while let Some(result) = rx.recv().await {
                        let mut results = results_mutex_clone.lock().await;
                        results[results_index_clone] = result;
                    }
                });
                wait_futs.push(receiver_fut);

                let scan_plan = ScannerBucketScanPlan {
                    buckets: set_buckets,
                    all_buckets: Arc::clone(&all_buckets),
                    digest: scan_plan_digest,
                    leader_epoch,
                    dirty_usage_buckets: dirty_usage_snapshot.buckets.clone(),
                    bucket_failures: bucket_failures.clone(),
                    pending_maintenance_work: pending_maintenance_work.clone(),
                    cache_cycle_floor: cache_cycle_floor.clone(),
                };
                // Spawn task to run the scanner
                let scanner_fut = tokio::spawn(async move {
                    let permit_wait = child_token_clone.clone();
                    let permit_wait_start = Instant::now();
                    let _permit = tokio::select! {
                        permit = set_scan_semaphore_clone.acquire_owned() => match permit {
                            Ok(permit) => permit,
                            Err(_) => return,
                        },
                        _ = permit_wait.cancelled() => return,
                    };
                    metrics::histogram!(
                        METRIC_SCANNER_SET_SCAN_WAIT_SECONDS,
                        "pool" => pool_label.clone(),
                        "set" => set_label.clone()
                    )
                    .record(permit_wait_start.elapsed().as_secs_f64());
                    let queued_count = decrement_atomic_usize(&queued_set_scans_clone);
                    record_set_scans_queued(queued_count);
                    let _active_guard = SetScanActiveGuard::new(active_set_scans_clone);

                    if let Err(e) = set_clone
                        .nsscanner_cache(
                            child_token_clone.clone(),
                            budget_clone,
                            scan_plan,
                            tx,
                            want_cycle_clone,
                            scan_mode_clone,
                        )
                        .await
                    {
                        if child_token_clone.is_cancelled() {
                            debug!(
                                pool = %pool_label,
                                set = %set_label,
                                error = %e,
                                "Scanner set scan stopped after cancellation"
                            );
                            return;
                        }

                        counter!(
                            "rustfs_scanner_set_failure_total",
                            "pool" => pool_label.clone(),
                            "set" => set_label.clone(),
                            "stage" => "nsscanner_cache".to_string()
                        )
                        .increment(1);
                        error!(
                            target: "rustfs::scanner::io",
                            event = EVENT_SCANNER_SET_STATE,
                            component = LOG_COMPONENT_SCANNER,
                            subsystem = LOG_SUBSYSTEM_IO,
                            pool = %pool_label,
                            set = %set_label,
                            error = %e,
                            state = "set_scan_failed",
                            "Scanner set scan failed; continuing cycle"
                        );
                        let mut first_err = first_err_mutex_clone.lock().await;
                        record_set_scan_failure(&mut first_err, e);
                    }
                });
                wait_futs.push(scanner_fut);
            }
        }

        for join_result in join_all(wait_futs).await {
            if let Err(err) = join_result {
                error!(
                    target: "rustfs::scanner::io",
                    event = EVENT_SCANNER_SET_STATE,
                    component = LOG_COMPONENT_SCANNER,
                    subsystem = LOG_SUBSYSTEM_IO,
                    state = "set_task_join_failed",
                    error = %err,
                    "Scanner set task join failed"
                );
                let mut first_err = first_err_mutex.lock().await;
                record_set_scan_failure(&mut first_err, scanner_task_join_error("scanner set", err));
            }
        }
        record_set_scan_concurrency_limit(0);
        record_set_scans_queued(0);
        record_set_scans_active(0);

        let first_err = first_err_mutex.lock().await.take();
        let results = results_mutex.lock().await.clone();
        let completed_all_sets = bucket_plan_complete && scanner_results_form_complete_snapshot(&results, &expected_sources);
        let result = finalize_nsscanner_result(&results, first_err);
        let failed_buckets = bucket_failures.hard.lock().await.clone();
        let partial_buckets = bucket_failures.partial.lock().await.clone();
        let namespace_not_found_buckets = bucket_failures.namespace_not_found.lock().await.clone();
        let scan_scope_matches = scanner_results_match_scan_scope(&results, &expected_sources);
        let bucket_scan_status = scanner_bucket_scan_status(
            !failed_buckets.is_empty(),
            scan_scope_matches && !partial_buckets.is_empty(),
            scan_scope_matches && !namespace_not_found_buckets.is_empty(),
        );
        let pending_maintenance_work = pending_maintenance_work_for_cycle(&pending_maintenance_work, &results);
        let observed_cycle_floor = cache_cycle_floor.load(Ordering::Acquire);
        let required_cycle_floor = (observed_cycle_floor > want_cycle).then_some(observed_cycle_floor);
        let budget_elapsed = budget.budget_elapsed();
        let dirty_usage_status = dirty_usage_snapshot_status(&dirty_usage_snapshot);
        let dirty_usage_current = dirty_usage_status == DirtyUsageSnapshotStatus::Current;
        let activity_status = scanner_cycle_activity_status(self, distributed, &activity_before).await;
        let all_bucket_names = all_buckets.iter().map(|bucket| bucket.name.clone()).collect::<Vec<_>>();
        let completed_usage = completed_data_usage_info(
            &results,
            &expected_sources,
            &all_bucket_names,
            bucket_plan_complete,
            budget_elapsed,
            ctx.is_cancelled(),
        );
        let structurally_complete_snapshot = result.is_ok() && completed_all_sets && completed_usage.is_some();
        let cycle_status = classify_nsscanner_cycle(
            structurally_complete_snapshot,
            budget_elapsed,
            ctx.is_cancelled(),
            bucket_scan_status,
            dirty_usage_status,
            activity_status,
        );
        if cycle_status == ScannerCycleStatus::Complete
            && let Some((data_usage_info, _)) = completed_usage
        {
            send_data_usage_update(&updates, data_usage_info).await?;
        }
        let dirty_usage_clear = should_clear_dirty_usage_snapshot(
            result.is_ok(),
            structurally_complete_snapshot,
            budget_elapsed,
            activity_status == ScannerCycleActivityStatus::Unchanged && dirty_usage_current,
            &dirty_usage_snapshot.buckets,
            &failed_buckets,
        );
        result?;
        Ok(ScannerCycleResult::new(cycle_status, dirty_usage_clear)
            .with_failed_dirty_usage(!failed_buckets.is_empty())
            .with_pending_maintenance_work(pending_maintenance_work)
            .with_required_cycle_floor(required_cycle_floor))
    }
}

#[async_trait::async_trait]
impl ScannerIOCache for SetDisks {
    #[tracing::instrument(skip(self, budget, scan_plan, updates))]
    async fn nsscanner_cache(
        self: Arc<Self>,
        ctx: CancellationToken,
        budget: Arc<ScannerCycleBudget>,
        scan_plan: ScannerBucketScanPlan,
        updates: mpsc::Sender<DataUsageCache>,
        want_cycle: u64,
        scan_mode: HealScanMode,
    ) -> Result<()> {
        let ScannerBucketScanPlan {
            buckets,
            all_buckets,
            digest: scan_plan_digest,
            leader_epoch,
            dirty_usage_buckets,
            bucket_failures,
            pending_maintenance_work,
            cache_cycle_floor,
        } = scan_plan;
        let pool_label = self.pool_index.to_string();
        let set_label = self.set_index.to_string();

        let source = DataUsageCacheSource::new(self.pool_index, self.set_index);
        if buckets.is_empty() {
            let now = SystemTime::now();
            let mut cache = DataUsageCache {
                info: DataUsageCacheInfo {
                    name: DATA_USAGE_ROOT.to_string(),
                    next_cycle: want_cycle,
                    last_update: Some(now),
                    leader_epoch,
                    source: Some(source),
                    snapshot_complete: true,
                    scan_plan_digest: Some(scan_plan_digest),
                    ..Default::default()
                },
                cache: HashMap::new(),
            };
            cache.replace(DATA_USAGE_ROOT, "", DataUsageEntry::default());
            for bucket in all_buckets.iter() {
                cache.replace(&bucket.name, DATA_USAGE_ROOT, DataUsageEntry::default());
            }
            reset_disk_bucket_scan_gauges(&pool_label, &set_label);
            return persist_and_publish_cache_snapshot(self, &updates, cache, cache_cycle_floor.as_ref())
                .await
                .map(|_| ())
                .ok_or_else(|| StorageError::other("failed to persist empty scanner set scope"));
        }

        let (disks, healing) = self.get_online_disks_with_healing(false).await;
        if disks.is_empty() {
            debug!(
                target: "rustfs::scanner::io",
                event = EVENT_SCANNER_SET_STATE,
                component = LOG_COMPONENT_SCANNER,
                subsystem = LOG_SUBSYSTEM_IO,
                pool = self.pool_index,
                set = self.set_index,
                state = "no_online_disks",
                "Scanner set state found no online disks"
            );
            reset_disk_bucket_scan_gauges(&pool_label, &set_label);
            return Ok(());
        }
        // Preserve the original set topology across capability filtering. During
        // rolling upgrades, an old remote peer must not make a distributed set
        // look local and allow an unscoped legacy cache to be adopted.
        let require_cache_source = disks.iter().any(|disk| !disk.is_local());
        let mut coordinator_disks = Vec::new();
        let mut remote_candidates = Vec::new();
        for disk in disks {
            if disk.is_local() {
                coordinator_disks.push(disk);
            } else {
                remote_candidates.push(disk);
            }
        }
        let remote_groups = group_remote_disks_by_peer(remote_candidates, |disk| disk.host_name());
        let capability_results = join_all(remote_groups.into_iter().map(|disks| async move {
            let server_epoch = match disks.first() {
                Some(disk) => disk.ns_scanner_server_epoch().await,
                None => Ok(None),
            };
            (disks, server_epoch)
        }))
        .await;
        let mut remote_disks = Vec::new();
        let mut unsupported_remote_disks = 0_usize;
        for (disks, server_epoch) in capability_results {
            match server_epoch {
                Ok(Some(server_epoch)) => {
                    remote_disks.extend(disks.into_iter().map(|disk| (disk, server_epoch)));
                }
                Ok(None) => {
                    let disk_count = disks.len();
                    let peer = disks.first().map(|disk| disk.host_name()).unwrap_or_default();
                    debug!(
                        target: "rustfs::scanner::io",
                        event = EVENT_SCANNER_SET_STATE,
                        component = LOG_COMPONENT_SCANNER,
                        subsystem = LOG_SUBSYSTEM_IO,
                        pool = self.pool_index,
                        set = self.set_index,
                        peer = %peer,
                        disk_count,
                        state = "remote_scanner_unsupported",
                        "Scanner found a peer without remote namespace scanner support"
                    );
                    unsupported_remote_disks = unsupported_remote_disks.saturating_add(disk_count);
                    coordinator_disks.extend(disks);
                }
                Err(err) => {
                    let peer = disks.first().map(|disk| disk.host_name()).unwrap_or_default();
                    let disk_count = disks.len();
                    debug!(
                        target: "rustfs::scanner::io",
                        event = EVENT_SCANNER_SET_STATE,
                        component = LOG_COMPONENT_SCANNER,
                        subsystem = LOG_SUBSYSTEM_IO,
                        pool = self.pool_index,
                        set = self.set_index,
                        peer = %peer,
                        disk_count,
                        state = "remote_scanner_probe_failed",
                        error = %err,
                        "Scanner skipped a peer whose remote namespace scanner capability could not be confirmed"
                    );
                }
            }
        }
        let remote_disk_count = remote_disks.len();
        let workers = namespace_scanner_workers(coordinator_disks, remote_disks);
        if workers.is_empty() {
            debug!(
                target: "rustfs::scanner::io",
                event = EVENT_SCANNER_SET_STATE,
                component = LOG_COMPONENT_SCANNER,
                subsystem = LOG_SUBSYSTEM_IO,
                pool = self.pool_index,
                set = self.set_index,
                state = "no_compatible_disks",
                "Scanner set state found no usable namespace scanner disks"
            );
            reset_disk_bucket_scan_gauges(&pool_label, &set_label);
            return Ok(());
        }
        let set_disk_inventory = Arc::new(scanner_set_disk_inventory(self.as_ref()).await);
        if unsupported_remote_disks > 0 {
            debug!(
                target: "rustfs::scanner::io",
                event = EVENT_SCANNER_SET_STATE,
                component = LOG_COMPONENT_SCANNER,
                subsystem = LOG_SUBSYSTEM_IO,
                pool = self.pool_index,
                set = self.set_index,
                v3_disks = remote_disk_count,
                unsupported_remote_disks,
                state = "unsupported_remote_disks_using_coordinator",
                "Scanner set assigned remote disks without namespace scanner support to coordinator-driven workers"
            );
        }
        let disk_scan_limit = scanner_budgeted_concurrency_limit(
            scanner_max_concurrent_disk_scans(workers.len()),
            budget.requires_serial_progress_accounting(),
        );
        record_disk_scan_concurrency_limit(&pool_label, &set_label, disk_scan_limit);
        debug!(
            target: "rustfs::scanner::io",
            event = EVENT_SCANNER_SET_STATE,
            component = LOG_COMPONENT_SCANNER,
            subsystem = LOG_SUBSYSTEM_IO,
            pool = self.pool_index,
            set = self.set_index,
            online_disks = workers.len(),
            concurrency_limit = disk_scan_limit,
            state = "disk_concurrency_budget",
            "Scanner disk concurrency budget resolved"
        );
        let disk_scan_semaphore = Arc::new(Semaphore::new(disk_scan_limit));
        let queued_disk_bucket_scans = Arc::new(AtomicUsize::new(buckets.len()));
        let active_disk_bucket_scans = Arc::new(AtomicUsize::new(0));
        record_disk_bucket_scans_queued(buckets.len(), &pool_label, &set_label);
        record_disk_bucket_scans_active(0, &pool_label, &set_label);
        let _reset_disk_bucket_scan_gauges = DiskBucketScanGaugeReset::new(pool_label.clone(), set_label.clone());

        let mut old_cache = DataUsageCache::default();
        if let Err(e) = old_cache.load(self.clone(), DATA_USAGE_CACHE_NAME).await {
            warn!(
                target: "rustfs::scanner::io",
                event = EVENT_SCANNER_CACHE_PERSIST_STATE,
                component = LOG_COMPONENT_SCANNER,
                subsystem = LOG_SUBSYSTEM_IO,
                pool = self.pool_index,
                set = self.set_index,
                cache_name = DATA_USAGE_CACHE_NAME,
                state = "old_cache_load_failed",
                error = %e,
                "Scanner old data usage cache load failed; rebuilding from bucket caches"
            );
        }
        match old_cache.prepare_for_scan(
            DATA_USAGE_ROOT,
            want_cycle,
            leader_epoch,
            source,
            scan_plan_digest,
            require_cache_source,
        ) {
            DataUsageCachePrepareOutcome::RejectedNewerCycle => {
                cache_cycle_floor.fetch_max(old_cache.info.next_cycle, Ordering::AcqRel);
                warn!(
                    target: "rustfs::scanner::io",
                    event = EVENT_SCANNER_CACHE_PERSIST_STATE,
                    component = LOG_COMPONENT_SCANNER,
                    subsystem = LOG_SUBSYSTEM_IO,
                    pool = self.pool_index,
                    set = self.set_index,
                    cache_name = DATA_USAGE_CACHE_NAME,
                    requested_cycle = want_cycle,
                    cached_cycle = old_cache.info.next_cycle,
                    state = "stale_cycle_rejected",
                    "Scanner rejected a set cache cycle regression"
                );
                return Ok(());
            }
            DataUsageCachePrepareOutcome::RejectedNewerLeader => {
                warn!(
                    target: "rustfs::scanner::io",
                    event = EVENT_SCANNER_CACHE_PERSIST_STATE,
                    component = LOG_COMPONENT_SCANNER,
                    subsystem = LOG_SUBSYSTEM_IO,
                    pool = self.pool_index,
                    set = self.set_index,
                    cache_name = DATA_USAGE_CACHE_NAME,
                    requested_epoch = leader_epoch,
                    cached_epoch = old_cache.info.leader_epoch,
                    state = "stale_leader_rejected",
                    "Scanner rejected work from an older leader epoch"
                );
                return Ok(());
            }
            DataUsageCachePrepareOutcome::Reused | DataUsageCachePrepareOutcome::Reset => {}
        }

        let mut cache = DataUsageCache {
            info: DataUsageCacheInfo {
                name: DATA_USAGE_ROOT.to_string(),
                next_cycle: want_cycle,
                leader_epoch,
                source: Some(source),
                snapshot_complete: false,
                scan_plan_digest: Some(scan_plan_digest),
                ..Default::default()
            },
            cache: HashMap::new(),
        };
        cache.replace(DATA_USAGE_ROOT, "", DataUsageEntry::default());
        for bucket in all_buckets.iter() {
            cache.replace(&bucket.name, DATA_USAGE_ROOT, DataUsageEntry::default());
        }

        let (bucket_tx, bucket_rx) = mpsc::channel::<BucketInfo>(buckets.len());

        let mut permutes = buckets.clone();
        permutes.shuffle(&mut rand::rng());
        let scan_order = bucket_usage_scan_order(&permutes, &old_cache, &dirty_usage_buckets);

        for bucket in scan_order.iter() {
            if let Some(c) = old_cache.find(&bucket.name) {
                cache.replace(&bucket.name, DATA_USAGE_ROOT, c.clone());
            }

            if let Err(e) = bucket_tx.send(bucket.clone()).await {
                record_failed_dirty_bucket(&bucket_failures.hard, &bucket.name).await;
                error!(
                    target: "rustfs::scanner::io",
                    event = EVENT_SCANNER_SET_STATE,
                    component = LOG_COMPONENT_SCANNER,
                    subsystem = LOG_SUBSYSTEM_IO,
                    bucket = %bucket.name,
                    state = "send_bucket_failed",
                    error = %e,
                    "Scanner bucket dispatch failed"
                );
            }
        }

        let cache_mutex: Arc<Mutex<DataUsageCache>> = Arc::new(Mutex::new(cache));

        let (bucket_result_tx, mut bucket_result_rx) = mpsc::channel::<DataUsageEntryInfo>(workers.len());

        let cache_mutex_clone = cache_mutex.clone();
        let ctx_clone = ctx.clone();
        let completed_bucket_count = Arc::new(AtomicUsize::new(0));
        let completed_bucket_count_clone = completed_bucket_count.clone();
        let collect_bucket_results_fut = tokio::spawn(async move {
            let mut cancelled = false;

            loop {
                tokio::select! {
                    _ = ctx_clone.cancelled(), if !cancelled => {
                        cancelled = true;
                    }
                    result = bucket_result_rx.recv() => {
                        let Some(result) = result else {
                            return;
                        };

                        let mut cache = cache_mutex_clone.lock().await;
                        apply_bucket_result_to_cache(&mut cache, result, SystemTime::now());
                        completed_bucket_count_clone.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        });

        let mut futs = Vec::new();

        let bucket_rx_mutex: Arc<Mutex<mpsc::Receiver<BucketInfo>>> = Arc::new(Mutex::new(bucket_rx));
        let remaining_bucket_work = Arc::new(AtomicUsize::new(buckets.len()));
        let bucket_work_complete = CancellationToken::new();
        for (disk, worker_mode) in workers {
            let bucket_rx_mutex_clone = bucket_rx_mutex.clone();
            let bucket_tx_clone = bucket_tx.clone();
            let remaining_bucket_work_clone = remaining_bucket_work.clone();
            let bucket_work_complete_clone = bucket_work_complete.clone();
            let ctx_clone = ctx.clone();
            let budget_clone = budget.clone();
            let store_clone_clone = self.clone();
            let bucket_result_tx_clone = bucket_result_tx.clone();
            let disk_clone = disk.clone();
            let set_disk_inventory_clone = set_disk_inventory.clone();
            let disk_scan_semaphore_clone = disk_scan_semaphore.clone();
            let queued_disk_bucket_scans_clone = queued_disk_bucket_scans.clone();
            let active_disk_bucket_scans_clone = active_disk_bucket_scans.clone();
            let pool_label_clone = pool_label.clone();
            let set_label_clone = set_label.clone();
            let failed_dirty_buckets_clone = bucket_failures.hard.clone();
            let partial_dirty_buckets_clone = bucket_failures.partial.clone();
            let pending_maintenance_work_clone = pending_maintenance_work.clone();
            let dirty_usage_buckets_clone = dirty_usage_buckets.clone();
            let cache_cycle_floor_clone = cache_cycle_floor.clone();
            let remote_server_epoch = match worker_mode {
                NamespaceScannerWorkerMode::RemoteV3(server_epoch) => Some(server_epoch),
                NamespaceScannerWorkerMode::Coordinator => None,
            };
            futs.push(tokio::spawn(async move {
                let remote_session_id = uuid::Uuid::new_v4();
                let mut remote_session_sequence = 0_u64;
                loop {
                    let bucket = tokio::select! {
                        _ = bucket_work_complete_clone.cancelled() => break,
                        _ = ctx_clone.cancelled() => break,
                        bucket = async { bucket_rx_mutex_clone.lock().await.recv().await } => {
                            let Some(bucket) = bucket else {
                                break;
                            };
                            bucket
                        }
                    };
                    let mut work_guard =
                        BucketWorkGuard::new(remaining_bucket_work_clone.clone(), bucket_work_complete_clone.clone());

                    let permit_wait = ctx_clone.clone();
                    let permit_wait_start = Instant::now();
                    let _permit = tokio::select! {
                        permit = disk_scan_semaphore_clone.clone().acquire_owned() => match permit {
                            Ok(permit) => permit,
                            Err(_) => {
                                decrement_disk_bucket_scans_queued(
                                    &queued_disk_bucket_scans_clone,
                                    &pool_label_clone,
                                    &set_label_clone,
                                );
                                break;
                            },
                        },
                        _ = permit_wait.cancelled() => {
                            decrement_disk_bucket_scans_queued(
                                &queued_disk_bucket_scans_clone,
                                &pool_label_clone,
                                &set_label_clone,
                            );
                            break;
                        },
                    };
                    metrics::histogram!(
                        METRIC_SCANNER_DISK_SCAN_WAIT_SECONDS,
                        "pool" => pool_label_clone.clone(),
                        "set" => set_label_clone.clone()
                    )
                    .record(permit_wait_start.elapsed().as_secs_f64());
                    decrement_disk_bucket_scans_queued(&queued_disk_bucket_scans_clone, &pool_label_clone, &set_label_clone);
                    let _active_guard = DiskBucketScanActiveGuard::new(
                        active_disk_bucket_scans_clone.clone(),
                        pool_label_clone.clone(),
                        set_label_clone.clone(),
                    );

                    debug!(
                        target: "rustfs::scanner::io",
                        event = EVENT_SCANNER_DISK_BUCKET_STATE,
                        component = LOG_COMPONENT_SCANNER,
                        subsystem = LOG_SUBSYSTEM_IO,
                        bucket = %bucket.name,
                        state = "scan_started",
                        "Scanner disk bucket scan started"
                    );

                    let cache_name = path_join_buf(&[&bucket.name, DATA_USAGE_CACHE_NAME]);
                    let bucket_scan_plan_digest =
                        scanner_bucket_cache_digest(scan_plan_digest, dirty_usage_buckets_clone.get(&bucket.name).copied());

                    if let Some(server_epoch) = remote_server_epoch {
                        let request_sequence = remote_session_sequence;
                        let Some(next_sequence) = remote_session_sequence.checked_add(1) else {
                            record_failed_dirty_bucket(&failed_dirty_buckets_clone, &bucket.name).await;
                            error!(
                                target: "rustfs::scanner::io",
                                event = EVENT_SCANNER_DISK_BUCKET_STATE,
                                component = LOG_COMPONENT_SCANNER,
                                subsystem = LOG_SUBSYSTEM_IO,
                                bucket = %bucket.name,
                                state = "remote_session_sequence_exhausted",
                                "Remote scanner session sequence exhausted"
                            );
                            break;
                        };
                        remote_session_sequence = next_sequence;
                        let remote_outcome = crate::remote_scanner::scan_remote_bucket(
                            &disk_clone,
                            ctx_clone.clone(),
                            budget_clone.clone(),
                            crate::remote_scanner::RemoteScannerScanSpec {
                                bucket: &bucket.name,
                                next_cycle: want_cycle,
                                leader_epoch,
                                server_epoch,
                                session_id: remote_session_id,
                                session_sequence: request_sequence,
                                scan_plan_digest: bucket_scan_plan_digest,
                                skip_healing: healing,
                                scan_mode,
                            },
                        )
                        .await;
                        match remote_outcome {
                            Ok(crate::remote_scanner::RemoteScannerOutcome::Complete {
                                usage,
                                pending_maintenance_work,
                            }) => {
                                if pending_maintenance_work {
                                    pending_maintenance_work_clone.store(true, Ordering::Release);
                                }
                                if let Err(e) = bucket_result_tx_clone.send(*usage).await {
                                    record_failed_dirty_bucket(&failed_dirty_buckets_clone, &bucket.name).await;
                                    error!(
                                        target: "rustfs::scanner::io",
                                        event = EVENT_SCANNER_DATA_USAGE_STREAM,
                                        component = LOG_COMPONENT_SCANNER,
                                        subsystem = LOG_SUBSYSTEM_IO,
                                        bucket = %bucket.name,
                                        state = "send_remote_root_failed",
                                        error = %e,
                                        "Remote scanner root entry publish failed"
                                    );
                                }
                            }
                            Ok(crate::remote_scanner::RemoteScannerOutcome::Partial) => {
                                record_partial_dirty_bucket(&partial_dirty_buckets_clone, &bucket.name).await;
                            }
                            Ok(crate::remote_scanner::RemoteScannerOutcome::NamespaceNotFound) => {
                                if requeue_bucket_work(&bucket_tx_clone, &bucket, &mut work_guard).await {
                                    increment_disk_bucket_scans_queued(
                                        &queued_disk_bucket_scans_clone,
                                        &pool_label_clone,
                                        &set_label_clone,
                                    );
                                } else {
                                    record_failed_dirty_bucket(&failed_dirty_buckets_clone, &bucket.name).await;
                                }
                                debug!(
                                    target: "rustfs::scanner::io",
                                    event = EVENT_SCANNER_DISK_BUCKET_STATE,
                                    component = LOG_COMPONENT_SCANNER,
                                    subsystem = LOG_SUBSYSTEM_IO,
                                    bucket = %bucket.name,
                                    state = "remote_namespace_missing_requeued",
                                    "Remote scanner requeued a bucket missing from this disk"
                                );
                                break;
                            }
                            Ok(crate::remote_scanner::RemoteScannerOutcome::CycleAhead(required_cycle)) => {
                                cache_cycle_floor_clone.fetch_max(required_cycle, Ordering::AcqRel);
                                record_failed_dirty_bucket(&failed_dirty_buckets_clone, &bucket.name).await;
                                warn!(
                                    target: "rustfs::scanner::io",
                                    event = EVENT_SCANNER_CACHE_PERSIST_STATE,
                                    component = LOG_COMPONENT_SCANNER,
                                    subsystem = LOG_SUBSYSTEM_IO,
                                    bucket = %bucket.name,
                                    requested_cycle = want_cycle,
                                    cached_cycle = required_cycle,
                                    state = "remote_stale_cycle_rejected",
                                    "Remote scanner rejected a bucket cache cycle regression"
                                );
                            }
                            Err(e) => {
                                if e.retry_bucket_work() {
                                    if requeue_bucket_work(&bucket_tx_clone, &bucket, &mut work_guard).await {
                                        increment_disk_bucket_scans_queued(
                                            &queued_disk_bucket_scans_clone,
                                            &pool_label_clone,
                                            &set_label_clone,
                                        );
                                    } else {
                                        record_failed_dirty_bucket(&failed_dirty_buckets_clone, &bucket.name).await;
                                    }
                                    debug!(
                                        target: "rustfs::scanner::io",
                                        event = EVENT_SCANNER_DISK_BUCKET_STATE,
                                        component = LOG_COMPONENT_SCANNER,
                                        subsystem = LOG_SUBSYSTEM_IO,
                                        bucket = %bucket.name,
                                        state = "remote_zero_progress_requeued",
                                        error = %e,
                                        "Remote scanner requeued bucket rejected before execution"
                                    );
                                    break;
                                }
                                record_failed_dirty_bucket(&failed_dirty_buckets_clone, &bucket.name).await;
                                if ctx_clone.is_cancelled() {
                                    debug!(
                                        target: "rustfs::scanner::io",
                                        event = EVENT_SCANNER_DISK_BUCKET_STATE,
                                        component = LOG_COMPONENT_SCANNER,
                                        subsystem = LOG_SUBSYSTEM_IO,
                                        bucket = %bucket.name,
                                        state = "remote_cancelled",
                                        error = %e,
                                        "Remote scanner bucket scan cancelled"
                                    );
                                } else {
                                    error!(
                                        target: "rustfs::scanner::io",
                                        event = EVENT_SCANNER_DISK_BUCKET_STATE,
                                        component = LOG_COMPONENT_SCANNER,
                                        subsystem = LOG_SUBSYSTEM_IO,
                                        bucket = %bucket.name,
                                        state = "remote_scan_failed",
                                        error = %e,
                                        "Remote scanner bucket scan failed"
                                    );
                                }
                                if e.retire_worker() {
                                    break;
                                }
                            }
                        }
                        continue;
                    }

                    let _local_admission = if disk_clone.is_local() {
                        match crate::remote_scanner::try_admit_remote_scanner(&disk_clone) {
                            Ok(admission) => Some(admission),
                            Err(e) => {
                                if requeue_bucket_work(&bucket_tx_clone, &bucket, &mut work_guard).await {
                                    increment_disk_bucket_scans_queued(
                                        &queued_disk_bucket_scans_clone,
                                        &pool_label_clone,
                                        &set_label_clone,
                                    );
                                } else {
                                    record_failed_dirty_bucket(&failed_dirty_buckets_clone, &bucket.name).await;
                                }
                                debug!(
                                    target: "rustfs::scanner::io",
                                    event = EVENT_SCANNER_DISK_BUCKET_STATE,
                                    component = LOG_COMPONENT_SCANNER,
                                    subsystem = LOG_SUBSYSTEM_IO,
                                    bucket = %bucket.name,
                                    state = "local_disk_busy",
                                    error = %e,
                                    "Scanner local disk is already serving namespace scanner work"
                                );
                                break;
                            }
                        }
                    } else {
                        None
                    };

                    // Lock order: scanner leader fence -> per-bucket cache lock ->
                    // cache object read/write. The cache lock stays outermost for
                    // local and rolling-upgrade workers so leader failover cannot
                    // execute the same bucket concurrently.
                    let lock_resource = scanner_cache_lock_resource(&cache_name);
                    let ns_lock = match store_clone_clone.new_ns_lock(RUSTFS_META_BUCKET, &lock_resource).await {
                        Ok(lock) => lock,
                        Err(e) => {
                            record_failed_dirty_bucket(&failed_dirty_buckets_clone, &bucket.name).await;
                            error!(
                                target: "rustfs::scanner::io",
                                event = EVENT_SCANNER_CACHE_PERSIST_STATE,
                                component = LOG_COMPONENT_SCANNER,
                                subsystem = LOG_SUBSYSTEM_IO,
                                bucket = %bucket.name,
                                cache_name = %cache_name,
                                state = "lock_create_failed",
                                error = %e,
                                "Scanner bucket cache lock creation failed"
                            );
                            continue;
                        }
                    };
                    let cache_guard = match ns_lock.get_write_lock_quiet(scanner_cache_lock_timeout()).await {
                        Ok(guard) => guard,
                        Err(e) => {
                            record_failed_dirty_bucket(&failed_dirty_buckets_clone, &bucket.name).await;
                            error!(
                                target: "rustfs::scanner::io",
                                event = EVENT_SCANNER_CACHE_PERSIST_STATE,
                                component = LOG_COMPONENT_SCANNER,
                                subsystem = LOG_SUBSYSTEM_IO,
                                bucket = %bucket.name,
                                cache_name = %cache_name,
                                state = "lock_acquire_failed",
                                error = %e,
                                "Scanner bucket cache lock acquisition failed"
                            );
                            continue;
                        }
                    };

                    let mut cache = DataUsageCache::default();
                    let revisions = match cache.load_with_revisions(store_clone_clone.clone(), &cache_name).await {
                        Ok(revisions) => revisions,
                        Err(e) => {
                            record_failed_dirty_bucket(&failed_dirty_buckets_clone, &bucket.name).await;
                            error!(
                                target: "rustfs::scanner::io",
                                event = EVENT_SCANNER_CACHE_PERSIST_STATE,
                                component = LOG_COMPONENT_SCANNER,
                                subsystem = LOG_SUBSYSTEM_IO,
                                bucket = %bucket.name,
                                cache_name = %cache_name,
                                state = "load_or_revision_lookup_failed",
                                error = %e,
                                "Scanner bucket cache load or revision lookup failed"
                            );
                            continue;
                        }
                    };
                    if cache_snapshot_is_current(&cache, &bucket.name, source, want_cycle, leader_epoch, bucket_scan_plan_digest)
                    {
                        if cache_guard.is_lock_lost() {
                            record_failed_dirty_bucket(&failed_dirty_buckets_clone, &bucket.name).await;
                            error!(
                                target: "rustfs::scanner::io",
                                event = EVENT_SCANNER_CACHE_PERSIST_STATE,
                                component = LOG_COMPONENT_SCANNER,
                                subsystem = LOG_SUBSYSTEM_IO,
                                bucket = %bucket.name,
                                cache_name = %cache_name,
                                state = "lock_lost_before_reuse",
                                "Current scanner bucket cache root publish skipped after lock loss"
                            );
                            continue;
                        }
                        if let Err(e) =
                            send_cache_root_entry_info(&bucket_result_tx_clone, &cache, &pending_maintenance_work_clone).await
                        {
                            record_failed_dirty_bucket(&failed_dirty_buckets_clone, &bucket.name).await;
                            error!(
                                target: "rustfs::scanner::io",
                                event = EVENT_SCANNER_DATA_USAGE_STREAM,
                                component = LOG_COMPONENT_SCANNER,
                                subsystem = LOG_SUBSYSTEM_IO,
                                bucket = %bucket.name,
                                state = "send_current_root_failed",
                                error = %e,
                                "Current scanner bucket cache root entry publish failed"
                            );
                        }
                        continue;
                    }

                    match cache.prepare_for_scan(
                        &bucket.name,
                        want_cycle,
                        leader_epoch,
                        source,
                        bucket_scan_plan_digest,
                        require_cache_source,
                    ) {
                        DataUsageCachePrepareOutcome::RejectedNewerCycle => {
                            cache_cycle_floor_clone.fetch_max(cache.info.next_cycle, Ordering::AcqRel);
                            record_failed_dirty_bucket(&failed_dirty_buckets_clone, &bucket.name).await;
                            error!(
                                target: "rustfs::scanner::io",
                                event = EVENT_SCANNER_CACHE_PERSIST_STATE,
                                component = LOG_COMPONENT_SCANNER,
                                subsystem = LOG_SUBSYSTEM_IO,
                                bucket = %bucket.name,
                                cache_name = %cache_name,
                                requested_cycle = want_cycle,
                                cached_cycle = cache.info.next_cycle,
                                state = "stale_cycle_rejected",
                                "Scanner rejected a bucket cache cycle regression"
                            );
                            continue;
                        }
                        DataUsageCachePrepareOutcome::RejectedNewerLeader => {
                            record_failed_dirty_bucket(&failed_dirty_buckets_clone, &bucket.name).await;
                            error!(
                                target: "rustfs::scanner::io",
                                event = EVENT_SCANNER_CACHE_PERSIST_STATE,
                                component = LOG_COMPONENT_SCANNER,
                                subsystem = LOG_SUBSYSTEM_IO,
                                bucket = %bucket.name,
                                cache_name = %cache_name,
                                requested_epoch = leader_epoch,
                                cached_epoch = cache.info.leader_epoch,
                                state = "stale_leader_rejected",
                                "Scanner rejected bucket work from an older leader epoch"
                            );
                            continue;
                        }
                        DataUsageCachePrepareOutcome::Reused | DataUsageCachePrepareOutcome::Reset => {}
                    }
                    cache.info.skip_healing = healing;

                    debug!(
                        target: "rustfs::scanner::io",
                        event = EVENT_SCANNER_DISK_BUCKET_STATE,
                        component = LOG_COMPONENT_SCANNER,
                        subsystem = LOG_SUBSYSTEM_IO,
                        bucket = %bucket.name,
                        cache_name = ?cache.info.name,
                        state = "cache_ready",
                        "Scanner disk bucket cache ready"
                    );

                    let before = cache.info.last_update;

                    let scan_ctx = ctx_clone.child_token();
                    let scan = disk_clone.clone().nsscanner_disk(
                        scan_ctx.clone(),
                        budget_clone.clone(),
                        set_disk_inventory_clone.as_ref().clone(),
                        cache.clone(),
                        None,
                        scan_mode,
                    );
                    tokio::pin!(scan);
                    let mut lock_watch = tokio::time::interval(SCANNER_CACHE_LOCK_POLL_INTERVAL);
                    lock_watch.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                    let scan_result = loop {
                        tokio::select! {
                            result = &mut scan => break result,
                            _ = lock_watch.tick() => {
                                if cache_guard.is_lock_lost() {
                                    scan_ctx.cancel();
                                    await_scanner_disk_shutdown(scan.as_mut()).await;
                                    break Err(Error::other("scanner bucket cache lock was lost during bucket scan"));
                                }
                            }
                        }
                    };
                    let scan_outcome = match scan_result {
                        Ok(scan_outcome) => scan_outcome,
                        Err(e) => {
                            record_failed_dirty_bucket(&failed_dirty_buckets_clone, &bucket.name).await;
                            if ctx_clone.is_cancelled() {
                                debug!(
                                    target: "rustfs::scanner::io",
                                    event = EVENT_SCANNER_DISK_BUCKET_STATE,
                                    component = LOG_COMPONENT_SCANNER,
                                    subsystem = LOG_SUBSYSTEM_IO,
                                    bucket = %bucket.name,
                                    state = "cancelled",
                                    error = %e,
                                    "Scanner disk bucket scan cancelled"
                                );
                            } else {
                                error!(
                                    target: "rustfs::scanner::io",
                                    event = EVENT_SCANNER_DISK_BUCKET_STATE,
                                    component = LOG_COMPONENT_SCANNER,
                                    subsystem = LOG_SUBSYSTEM_IO,
                                    bucket = %bucket.name,
                                    state = "scan_failed",
                                    error = %e,
                                    "Scanner disk bucket scan failed"
                                );
                            }

                            if !cache_guard.is_lock_lost()
                                && let (Some(last_update), Some(before_update)) = (cache.info.last_update, before)
                                && last_update > before_update
                            {
                                let done_save = Metrics::time(Metric::SaveUsage);
                                if let Err(e) = cache
                                    .save_with_revisions(store_clone_clone.clone(), cache_name.as_str(), &revisions)
                                    .await
                                {
                                    error!(
                                        target: "rustfs::scanner::io",
                                        event = EVENT_SCANNER_CACHE_PERSIST_STATE,
                                        component = LOG_COMPONENT_SCANNER,
                                        subsystem = LOG_SUBSYSTEM_IO,
                                        bucket = %bucket.name,
                                        cache_name = %cache_name,
                                        state = "save_failed",
                                        error = %e,
                                        "Scanner bucket cache save failed"
                                    );
                                }
                                done_save();
                            }

                            continue;
                        }
                    };

                    let partial = match scan_outcome {
                        ScannerDiskScanOutcome::Complete(completed_cache) => {
                            cache = completed_cache;
                            None
                        }
                        ScannerDiskScanOutcome::Partial(partial_cache) => Some((partial_cache, &partial_dirty_buckets_clone)),
                        ScannerDiskScanOutcome::NamespaceNotFound(_) => {
                            if requeue_bucket_work(&bucket_tx_clone, &bucket, &mut work_guard).await {
                                increment_disk_bucket_scans_queued(
                                    &queued_disk_bucket_scans_clone,
                                    &pool_label_clone,
                                    &set_label_clone,
                                );
                            } else {
                                record_failed_dirty_bucket(&failed_dirty_buckets_clone, &bucket.name).await;
                            }
                            debug!(
                                target: "rustfs::scanner::io",
                                event = EVENT_SCANNER_DISK_BUCKET_STATE,
                                component = LOG_COMPONENT_SCANNER,
                                subsystem = LOG_SUBSYSTEM_IO,
                                bucket = %bucket.name,
                                state = "local_namespace_missing_requeued",
                                "Scanner requeued a bucket missing from this local disk"
                            );
                            break;
                        }
                    };
                    if let Some((partial_cache, failure_buckets)) = partial {
                        record_partial_dirty_bucket(failure_buckets, &bucket.name).await;
                        let done_save = Metrics::time(Metric::SaveUsage);
                        let partial_saved = if cache_guard.is_lock_lost() {
                            false
                        } else {
                            match partial_cache
                                .save_with_revisions(store_clone_clone.clone(), cache_name.as_str(), &revisions)
                                .await
                            {
                                Ok(()) => true,
                                Err(e) => {
                                    error!(
                                        target: "rustfs::scanner::io",
                                        event = EVENT_SCANNER_CACHE_PERSIST_STATE,
                                        component = LOG_COMPONENT_SCANNER,
                                        subsystem = LOG_SUBSYSTEM_IO,
                                        bucket = %bucket.name,
                                        cache_name = %cache_name,
                                        state = "partial_save_failed",
                                        error = %e,
                                        "Scanner partial bucket cache save failed"
                                    );
                                    false
                                }
                            }
                        };
                        done_save();
                        if !partial_saved {
                            record_failed_dirty_bucket(&failed_dirty_buckets_clone, &bucket.name).await;
                        }
                        if partial_saved {
                            debug!(
                                target: "rustfs::scanner::io",
                                event = EVENT_SCANNER_CACHE_PERSIST_STATE,
                                component = LOG_COMPONENT_SCANNER,
                                subsystem = LOG_SUBSYSTEM_IO,
                                bucket = %bucket.name,
                                cache_name = %cache_name,
                                state = "partial_saved_not_published",
                                "Scanner partial bucket cache saved without publishing usage aggregate"
                            );
                        }

                        continue;
                    }
                    debug!(
                        target: "rustfs::scanner::io",
                        event = EVENT_SCANNER_DISK_BUCKET_STATE,
                        component = LOG_COMPONENT_SCANNER,
                        subsystem = LOG_SUBSYSTEM_IO,
                        bucket = %bucket.name,
                        cache_name = %cache.info.name,
                        state = "scan_completed",
                        "Scanner disk bucket scan completed"
                    );

                    if ctx_clone.is_cancelled() {
                        break;
                    }

                    if cache_guard.is_lock_lost() {
                        record_failed_dirty_bucket(&failed_dirty_buckets_clone, &bucket.name).await;
                        error!(
                            target: "rustfs::scanner::io",
                            event = EVENT_SCANNER_CACHE_PERSIST_STATE,
                            component = LOG_COMPONENT_SCANNER,
                            subsystem = LOG_SUBSYSTEM_IO,
                            bucket = %bucket.name,
                            cache_name = %cache_name,
                            state = "lock_lost",
                            "Scanner bucket cache save skipped after lock loss"
                        );
                        continue;
                    }

                    let done_save = Metrics::time(Metric::SaveUsage);
                    if let Err(e) = cache
                        .save_with_revisions(store_clone_clone.clone(), &cache_name, &revisions)
                        .await
                    {
                        done_save();
                        record_failed_dirty_bucket(&failed_dirty_buckets_clone, &bucket.name).await;
                        error!(
                            target: "rustfs::scanner::io",
                            event = EVENT_SCANNER_CACHE_PERSIST_STATE,
                            component = LOG_COMPONENT_SCANNER,
                            subsystem = LOG_SUBSYSTEM_IO,
                            bucket = %bucket.name,
                            cache_name = %cache_name,
                            state = "save_failed",
                            error = %e,
                            "Scanner bucket cache save failed"
                        );
                        continue;
                    }
                    done_save();

                    if cache_guard.is_lock_lost() {
                        record_failed_dirty_bucket(&failed_dirty_buckets_clone, &bucket.name).await;
                        error!(
                            target: "rustfs::scanner::io",
                            event = EVENT_SCANNER_CACHE_PERSIST_STATE,
                            component = LOG_COMPONENT_SCANNER,
                            subsystem = LOG_SUBSYSTEM_IO,
                            bucket = %bucket.name,
                            cache_name = %cache_name,
                            state = "lock_lost_after_save",
                            "Scanner bucket cache root publish skipped after lock loss"
                        );
                        continue;
                    }

                    debug!(
                        target: "rustfs::scanner::io",
                        event = EVENT_SCANNER_DATA_USAGE_STREAM,
                        component = LOG_COMPONENT_SCANNER,
                        subsystem = LOG_SUBSYSTEM_IO,
                        bucket = %bucket.name,
                        cache_name = %cache.info.name,
                        state = "send_root_entry",
                        "Scanner root entry publish started"
                    );

                    if let Err(e) =
                        send_cache_root_entry_info(&bucket_result_tx_clone, &cache, &pending_maintenance_work_clone).await
                    {
                        record_failed_dirty_bucket(&failed_dirty_buckets_clone, &bucket.name).await;
                        error!(
                            target: "rustfs::scanner::io",
                            event = EVENT_SCANNER_DATA_USAGE_STREAM,
                            component = LOG_COMPONENT_SCANNER,
                            subsystem = LOG_SUBSYSTEM_IO,
                            bucket = %bucket.name,
                            state = "send_root_failed",
                            error = %e,
                            "Scanner root entry publish failed"
                        );
                    }
                }
            }));
        }
        drop(bucket_tx);
        drop(bucket_result_tx);

        let mut first_join_err = None;
        for join_result in join_all(futs).await {
            if let Err(err) = join_result {
                error!(
                    target: "rustfs::scanner::io",
                    event = EVENT_SCANNER_DISK_BUCKET_STATE,
                    component = LOG_COMPONENT_SCANNER,
                    subsystem = LOG_SUBSYSTEM_IO,
                    pool = self.pool_index,
                    set = self.set_index,
                    state = "disk_bucket_task_join_failed",
                    error = %err,
                    "Scanner disk bucket task join failed"
                );
                record_set_scan_failure(&mut first_join_err, scanner_task_join_error("scanner disk bucket", err));
            }
        }
        let unprocessed_buckets = mark_unprocessed_bucket_work_failed(
            bucket_rx_mutex.as_ref(),
            &remaining_bucket_work,
            &bucket_work_complete,
            &bucket_failures.hard,
        )
        .await;
        if unprocessed_buckets > 0 {
            warn!(
                target: "rustfs::scanner::io",
                event = EVENT_SCANNER_SET_STATE,
                component = LOG_COMPONENT_SCANNER,
                subsystem = LOG_SUBSYSTEM_IO,
                pool = self.pool_index,
                set = self.set_index,
                unprocessed_buckets,
                state = "workers_exhausted",
                "Scanner marked queued bucket work failed after all disk workers exited"
            );
        }
        record_disk_scan_concurrency_limit(&pool_label, &set_label, 0);
        record_disk_bucket_scans_queued(0, &pool_label, &set_label);
        record_disk_bucket_scans_active(0, &pool_label, &set_label);

        if let Err(err) = collect_bucket_results_fut.await {
            return Err(scanner_task_join_error("scanner bucket result collector", err));
        }

        if let Some(err) = first_join_err {
            return Err(err);
        }

        let completed_count = completed_bucket_count.load(Ordering::Relaxed);
        if should_publish_completed_snapshot(completed_count, buckets.len(), budget.budget_elapsed(), ctx.is_cancelled()) {
            let cache_snapshot = {
                let mut cache = cache_mutex.lock().await;
                cache.info.next_cycle = want_cycle;
                cache.info.last_update.get_or_insert_with(SystemTime::now);
                cache.info.snapshot_complete = true;
                cache.clone()
            };
            let _ = persist_and_publish_cache_snapshot(self.clone(), &updates, cache_snapshot, cache_cycle_floor.as_ref()).await;
        } else {
            let incomplete_scope = DataUsageCache {
                info: DataUsageCacheInfo {
                    name: DATA_USAGE_ROOT.to_string(),
                    next_cycle: want_cycle,
                    leader_epoch,
                    source: Some(source),
                    snapshot_complete: false,
                    scan_plan_digest: Some(scan_plan_digest),
                    ..Default::default()
                },
                cache: HashMap::new(),
            };
            if let Err(e) = updates.send(incomplete_scope).await {
                error!(
                    target: "rustfs::scanner::io",
                    event = EVENT_SCANNER_CACHE_PERSIST_STATE,
                    component = LOG_COMPONENT_SCANNER,
                    subsystem = LOG_SUBSYSTEM_IO,
                    pool = self.pool_index,
                    set = self.set_index,
                    state = "incomplete_scope_publish_failed",
                    error = %e,
                    "Scanner incomplete set scope publish failed"
                );
            }
            debug!(
                target: "rustfs::scanner::io",
                event = EVENT_SCANNER_CACHE_PERSIST_STATE,
                component = LOG_COMPONENT_SCANNER,
                subsystem = LOG_SUBSYSTEM_IO,
                completed_buckets = completed_count,
                total_buckets = buckets.len(),
                budget_elapsed = budget.budget_elapsed(),
                cancelled = ctx.is_cancelled(),
                state = "set_cache_publish_skipped",
                "Scanner set cache publish skipped because cycle did not complete cleanly"
            );
        }

        debug!(
            target: "rustfs::scanner::io",
            event = EVENT_SCANNER_DISK_BUCKET_STATE,
            component = LOG_COMPONENT_SCANNER,
            subsystem = LOG_SUBSYSTEM_IO,
            state = "set_scan_completed",
            "Scanner set scan completed"
        );

        Ok(())
    }
}

#[async_trait::async_trait]
impl ScannerIODisk for Disk {
    async fn get_size(&self, mut item: ScannerItem) -> Result<SizeSummary> {
        let done_object = Metrics::time(Metric::ScanObject);

        if !is_xl_meta_path(&item.path) {
            return Err(StorageError::other(SCANNER_SKIP_FILE_ERROR.to_string()));
        }

        let data = match self.read_metadata(&item.bucket, &item.object_path()).await {
            Ok(data) => data,
            Err(e) if DiskError::is_err_object_not_found(&e) || DiskError::is_err_version_not_found(&e) => {
                return Err(StorageError::other(SCANNER_SKIP_FILE_ERROR.to_string()));
            }
            Err(e) => {
                return Err(scanner_metadata_transient_error(
                    format!("failed to read metadata: {e}"),
                    &item.bucket,
                    &item.object_path(),
                ));
            }
        };

        item.transform_meta_dir();

        let meta = FileMeta::load(&data).map_err(|e| {
            scanner_metadata_corrupt_error(format!("failed to load metadata: {e}"), &item.bucket, &item.object_path())
        })?;
        let fivs = match meta.get_file_info_versions(item.bucket.as_str(), item.object_path().as_str(), false) {
            Ok(versions) => versions,
            Err(e) => {
                error!(
                    target: "rustfs::scanner::io",
                    event = EVENT_SCANNER_DISK_BUCKET_STATE,
                    component = LOG_COMPONENT_SCANNER,
                    subsystem = LOG_SUBSYSTEM_IO,
                    bucket = %item.bucket,
                    object = %item.object_path(),
                    state = "file_info_versions_failed",
                    error = %e,
                    "Scanner disk bucket failed to resolve file info versions"
                );
                return Err(scanner_metadata_corrupt_error(
                    format!("failed to resolve file info versions: {e}"),
                    &item.bucket,
                    &item.object_path(),
                ));
            }
        };

        let versioned = BucketVersioningSys::get(&item.bucket)
            .await
            .map(|v| v.versioned(&item.object_path()))
            .unwrap_or(false);

        let object_infos = fivs
            .versions
            .iter()
            .map(|v| ObjectInfo::from_file_info(v, item.bucket.as_str(), item.object_path().as_str(), versioned))
            .collect::<Vec<ObjectInfo>>();
        let free_version_infos = fivs
            .free_versions
            .iter()
            .map(|v| ObjectInfo::from_file_info(v, item.bucket.as_str(), item.object_path().as_str(), versioned))
            .collect::<Vec<ObjectInfo>>();

        let mut size_summary = SizeSummary::default();

        let tiers = list_runtime_tiers().await;

        for tier in tiers.iter() {
            size_summary.tier_stats.insert(tier.name.clone(), TierStats::default());
        }
        if !size_summary.tier_stats.is_empty() {
            size_summary
                .tier_stats
                .insert(storageclass::STANDARD.to_string(), TierStats::default());
            size_summary
                .tier_stats
                .insert(storageclass::RRS.to_string(), TierStats::default());
        }

        let lock_config = object_lock_config_for_scanner_item(&item).await;

        // Count every version this object contributes to the scan, independent
        // of any lifecycle configuration, so scan-coverage metrics stay honest
        // on clusters without ILM rules. Recorded before `apply_actions` moves
        // `object_infos`.
        global_metrics().record_scanner_versions_scanned(object_infos.len() as u64);

        item.apply_actions(object_infos, lock_config, &mut size_summary).await;

        if !free_version_infos.is_empty() {
            for oi in free_version_infos {
                enqueue_runtime_free_version(oi).await;
            }
        }

        done_object();

        Ok(size_summary)
    }

    #[tracing::instrument(skip(self, budget, updates, cache))]
    async fn nsscanner_disk(
        self: Arc<Self>,
        ctx: CancellationToken,
        budget: Arc<ScannerCycleBudget>,
        set_disks: Vec<Arc<Disk>>,
        cache: DataUsageCache,
        updates: Option<mpsc::Sender<DataUsageEntry>>,
        scan_mode: HealScanMode,
    ) -> Result<ScannerDiskScanOutcome> {
        let done_drive = Metrics::time(Metric::ScanBucketDrive);
        let drive_start = std::time::Instant::now();
        let bucket = cache.info.name.clone();
        let disk_path = self.path().to_string_lossy().to_string();
        global_metrics().record_scan_bucket_drive_start();
        let mut failure_guard = BucketDriveFailureGuard::new();
        let _guard = self.start_scan();

        let mut cache = cache;

        let (lifecycle_config, _) = get_lifecycle_config(&cache.info.name)
            .await
            .unwrap_or_else(|_| (BucketLifecycleConfiguration::default(), OffsetDateTime::now_utc()));

        if lifecycle_config.has_active_rules("") {
            cache.info.lifecycle = Some(Arc::new(lifecycle_config));
        }

        let (replication_config, _) = get_replication_config(&cache.info.name).await.unwrap_or((
            ReplicationConfiguration {
                role: "".to_string(),
                rules: vec![],
            },
            OffsetDateTime::now_utc(),
        ));

        if replication_config.has_active_rules("", true)
            && let Ok(targets) = BucketTargetSys::get().list_bucket_targets(&cache.info.name).await
        {
            cache.info.replication = Some(Arc::new(ReplicationConfig::new(Some(replication_config), Some(targets))));
        }

        if let Ok((object_lock_config, _)) = get_object_lock_config(&cache.info.name).await
            && object_lock_config_enabled(&object_lock_config)
        {
            cache.info.object_lock = Some(Arc::new(object_lock_config));
        }

        let result = scan_data_folder(
            ctx.clone(),
            budget,
            set_disks,
            self.clone(),
            cache,
            updates,
            scan_mode,
            SCANNER_SLEEPER.clone(),
        )
        .await;

        match result {
            Ok(mut data_usage_info) => {
                done_drive();
                emit_scan_bucket_drive_complete(true, &bucket, &disk_path, drive_start.elapsed());
                data_usage_info.info.last_update = Some(SystemTime::now());
                failure_guard.mark_not_failed();
                Ok(ScannerDiskScanOutcome::Complete(data_usage_info))
            }
            Err(ScannerError::PartialCache(mut partial_cache)) => {
                done_drive();
                emit_scan_bucket_drive_partial(&bucket, &disk_path, drive_start.elapsed());
                partial_cache.info.last_update.get_or_insert_with(SystemTime::now);
                failure_guard.mark_not_failed();
                Ok(ScannerDiskScanOutcome::Partial(*partial_cache))
            }
            Err(ScannerError::NamespaceNotFoundCache(mut partial_cache)) => {
                done_drive();
                emit_scan_bucket_drive_partial(&bucket, &disk_path, drive_start.elapsed());
                partial_cache.info.last_update.get_or_insert_with(SystemTime::now);
                failure_guard.mark_not_failed();
                Ok(ScannerDiskScanOutcome::NamespaceNotFound(*partial_cache))
            }
            Err(e) => {
                if ctx.is_cancelled() {
                    emit_scan_bucket_drive_partial(&bucket, &disk_path, drive_start.elapsed());
                    failure_guard.mark_not_failed();
                } else {
                    done_drive();
                    emit_scan_bucket_drive_complete(false, &bucket, &disk_path, drive_start.elapsed());
                }
                Err(StorageError::other(format!("Failed to scan data folder: {e}")))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scanner_budget::ScannerCycleBudgetConfig;
    use crate::scanner_folder::ScannerItem;
    use crate::{
        DiskOption, ECStore, Endpoint, EndpointServerPools, Endpoints, InstanceContext, PoolEndpoints, ScannerObjectOptions,
        ScannerPutObjReader, init_bucket_metadata_sys_for_scanner_tests, init_ecstore_config_for_scanner_tests,
        init_local_disks_with_instance_ctx, new_disk, path2_bucket_object_with_base_path,
    };
    use rustfs_filemeta::FileInfo;
    use rustfs_storage_api::{BucketOperations as _, MakeBucketOptions, ObjectIO as _};
    use serial_test::serial;
    use temp_env::with_var;
    use time::OffsetDateTime;
    use uuid::Uuid;

    fn bucket_info(name: &str) -> BucketInfo {
        BucketInfo {
            name: name.to_string(),
            created: None,
            deleted: None,
            versioning: false,
            object_locking: false,
        }
    }

    async fn setup_two_pool_scanner_store() -> (tempfile::TempDir, Arc<ECStore>) {
        init_ecstore_config_for_scanner_tests();
        let temp_dir = tempfile::tempdir().expect("multi-pool scanner test directory should be created");
        let mut pools = Vec::new();
        for pool_index in 0..2 {
            let mut endpoints = Vec::new();
            for disk_index in 0..4 {
                let disk_path = temp_dir.path().join(format!("pool{pool_index}-disk{disk_index}"));
                tokio::fs::create_dir_all(&disk_path)
                    .await
                    .expect("multi-pool scanner test disk should be created");
                let mut endpoint =
                    Endpoint::try_from(disk_path.to_str().expect("disk path should be utf8")).expect("endpoint should parse");
                endpoint.set_pool_index(pool_index);
                endpoint.set_set_index(0);
                endpoint.set_disk_index(disk_index);
                endpoints.push(endpoint);
            }
            pools.push(PoolEndpoints {
                legacy: false,
                set_count: 1,
                drives_per_set: 4,
                endpoints: Endpoints::from(endpoints),
                cmd_line: format!("scanner-cycle-pool-{pool_index}"),
                platform: format!("OS: {} | Arch: {}", std::env::consts::OS, std::env::consts::ARCH),
            });
        }

        let endpoint_pools = EndpointServerPools::from(pools);
        let instance_ctx = Arc::new(InstanceContext::new());
        init_local_disks_with_instance_ctx(&instance_ctx, endpoint_pools.clone())
            .await
            .expect("multi-pool local disks should initialize");
        let store = ECStore::new_with_instance_ctx(
            "127.0.0.1:0".parse().expect("test address should parse"),
            endpoint_pools,
            CancellationToken::new(),
            instance_ctx,
        )
        .await
        .expect("multi-pool ECStore should initialize");
        init_bucket_metadata_sys_for_scanner_tests(store.clone()).await;

        (temp_dir, store)
    }

    #[tokio::test]
    async fn data_usage_publish_fails_when_receiver_is_closed() {
        let (updates, receiver) = mpsc::channel(1);
        drop(receiver);

        let err = send_data_usage_update(&updates, DataUsageInfo::default())
            .await
            .expect_err("closed usage receiver must reject the scanner update");

        assert!(err.to_string().contains("receiver closed"));
    }

    #[tokio::test]
    #[serial]
    async fn multi_pool_scanner_cycle_publishes_combined_usage() {
        let (_temp_dir, store) = setup_two_pool_scanner_store().await;
        let bucket = format!("scanner-union-{}", Uuid::new_v4().simple());
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("bucket should be created across both pools");

        for (pool_index, (object, body)) in [("pool-a", b"first".as_slice()), ("pool-b", b"second".as_slice())]
            .into_iter()
            .enumerate()
        {
            let mut reader = ScannerPutObjReader::from_vec(body.to_vec());
            store.pools[pool_index].disk_set[0]
                .put_object(&bucket, object, &mut reader, &ScannerObjectOptions::default())
                .await
                .expect("object should be written to its selected pool");
        }

        let ctx = CancellationToken::new();
        let budget = ScannerCycleBudget::new(&ctx, ScannerCycleBudgetConfig::default());
        let (updates, mut receiver) = mpsc::channel(1);
        let result = tokio::time::timeout(
            Duration::from_secs(30),
            ScannerIOCycle::nsscanner_with_status(store.as_ref(), ctx, budget, updates, 1, 1, HealScanMode::Normal),
        )
        .await
        .expect("multi-pool scanner cycle should finish")
        .expect("multi-pool scanner cycle should succeed");

        assert_eq!(result.status, ScannerCycleStatus::Complete);
        let usage = receiver.recv().await.expect("complete scanner cycle should publish usage");
        let bucket_usage = usage
            .buckets_usage
            .get(&bucket)
            .expect("combined bucket usage should be present");
        assert_eq!(bucket_usage.objects_count, 2);
        assert_eq!(bucket_usage.size, 11);
        assert_eq!(usage.objects_total_count, 2);
        assert_eq!(usage.objects_total_size, 11);
    }

    #[tokio::test]
    #[serial]
    async fn multi_pool_scanner_cycle_zero_fills_bucket_absent_from_first_pool() {
        let (_temp_dir, store) = setup_two_pool_scanner_store().await;
        let bucket = format!("scanner-second-pool-{}", Uuid::new_v4().simple());
        store.pools[1].disk_set[0]
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("bucket should be created only in the second pool");
        let body = b"second-only";
        let mut reader = ScannerPutObjReader::from_vec(body.to_vec());
        store.pools[1].disk_set[0]
            .put_object(&bucket, "pool-b", &mut reader, &ScannerObjectOptions::default())
            .await
            .expect("object should be written only to the second pool");

        let ctx = CancellationToken::new();
        let budget = ScannerCycleBudget::new(&ctx, ScannerCycleBudgetConfig::default());
        let (updates, mut receiver) = mpsc::channel(1);
        let result = tokio::time::timeout(
            Duration::from_secs(30),
            ScannerIOCycle::nsscanner_with_status(store.as_ref(), ctx, budget, updates, 1, 1, HealScanMode::Normal),
        )
        .await
        .expect("second-pool-only scanner cycle should finish")
        .expect("second-pool-only scanner cycle should succeed");

        assert_eq!(result.status, ScannerCycleStatus::Complete);
        let usage = receiver.recv().await.expect("complete scanner cycle should publish usage");
        let bucket_usage = usage
            .buckets_usage
            .get(&bucket)
            .expect("second-pool-only bucket usage should be present");
        assert_eq!(bucket_usage.objects_count, 1);
        assert_eq!(bucket_usage.size, u64::try_from(body.len()).expect("test body length should fit u64"));
        assert_eq!(usage.objects_total_count, 1);
        assert_eq!(
            usage.objects_total_size,
            u64::try_from(body.len()).expect("test body length should fit u64")
        );
    }

    #[tokio::test]
    async fn scanner_item_object_lock_uses_cached_config() {
        let temp_dir = std::env::temp_dir();
        let cached = Arc::new(ObjectLockConfiguration {
            object_lock_enabled: Some(ObjectLockEnabled::from_static(ObjectLockEnabled::ENABLED)),
            ..Default::default()
        });
        let item = ScannerItem {
            path: temp_dir.join("object").to_string_lossy().to_string(),
            bucket: "bucket".to_string(),
            prefix: String::new(),
            object_name: "object".to_string(),
            file_type: std::fs::metadata(&temp_dir)
                .expect("temp dir metadata should be readable")
                .file_type(),
            lifecycle: None,
            object_lock: Some(cached.clone()),
            replication: None,
            heal_enabled: false,
            heal_bitrot: false,
            debug: false,
        };

        let resolved = object_lock_config_for_scanner_item(&item)
            .await
            .expect("cached object-lock config should resolve");

        assert!(Arc::ptr_eq(&resolved, &cached));
    }

    #[test]
    fn object_lock_config_enabled_accepts_enabled_only() {
        let enabled = ObjectLockConfiguration {
            object_lock_enabled: Some(ObjectLockEnabled::from_static(ObjectLockEnabled::ENABLED)),
            ..Default::default()
        };

        assert!(object_lock_config_enabled(&enabled));
        assert!(!object_lock_config_enabled(&ObjectLockConfiguration::default()));
    }

    #[test]
    #[serial]
    fn dirty_usage_snapshot_clear_preserves_newer_generation() {
        clear_dirty_usage_buckets_for_tests();
        record_dirty_usage_bucket("photos");
        let buckets = vec![bucket_info("photos")];
        let snapshot = snapshot_dirty_usage_buckets(&buckets, dirty_usage_generation());

        record_dirty_usage_bucket("photos");
        clear_dirty_usage_buckets(&snapshot.buckets);

        assert_eq!(dirty_usage_bucket_count(), 1);
        clear_dirty_usage_buckets_for_tests();
    }

    #[test]
    #[serial]
    fn dirty_usage_snapshot_detects_uncovered_generation() {
        clear_dirty_usage_buckets_for_tests();
        record_dirty_usage_bucket("photos");
        let buckets = vec![bucket_info("photos")];
        let snapshot = snapshot_dirty_usage_buckets(&buckets, dirty_usage_generation());

        assert_eq!(dirty_usage_snapshot_status(&snapshot), DirtyUsageSnapshotStatus::Current);

        record_dirty_usage_bucket("photos");

        assert_eq!(dirty_usage_snapshot_status(&snapshot), DirtyUsageSnapshotStatus::Changed);
        clear_dirty_usage_buckets_for_tests();
    }

    #[test]
    fn generation_saturates_instead_of_wrapping() {
        let generation = AtomicU64::new(u64::MAX - 1);

        assert_eq!(advance_generation(&generation), u64::MAX);
        assert_eq!(advance_generation(&generation), u64::MAX);
        assert_eq!(generation.load(Ordering::Acquire), u64::MAX);
    }

    #[test]
    #[serial]
    fn dirty_usage_snapshot_clears_a_stably_absent_bucket_after_durable_save() {
        clear_dirty_usage_buckets_for_tests();
        record_dirty_usage_bucket("photos");
        record_dirty_usage_bucket("temporarily-omitted");
        let generation_before_bucket_list = dirty_usage_generation();

        let snapshot = snapshot_dirty_usage_buckets(&[bucket_info("photos")], generation_before_bucket_list);

        assert!(snapshot.buckets.contains_key("photos"));
        assert!(snapshot.buckets.contains_key("temporarily-omitted"));
        assert!(dirty_usage_buckets().contains_key("temporarily-omitted"));
        assert_eq!(dirty_usage_snapshot_status(&snapshot), DirtyUsageSnapshotStatus::Current);

        ScannerCycleResult::new(ScannerCycleStatus::Complete, Some(snapshot.buckets.as_ref().clone()))
            .acknowledge_durable_usage();
        assert!(!dirty_usage_buckets().contains_key("temporarily-omitted"));
        clear_dirty_usage_buckets_for_tests();
    }

    #[test]
    #[serial]
    fn dirty_usage_snapshot_preserves_an_absent_bucket_recorded_after_listing_started() {
        clear_dirty_usage_buckets_for_tests();
        let generation_before_bucket_list = dirty_usage_generation();
        record_dirty_usage_bucket("new-or-racing-bucket");

        let snapshot = snapshot_dirty_usage_buckets(&[], generation_before_bucket_list);

        assert!(!snapshot.buckets.contains_key("new-or-racing-bucket"));
        assert_eq!(dirty_usage_snapshot_status(&snapshot), DirtyUsageSnapshotStatus::Changed);
        assert!(dirty_usage_buckets().contains_key("new-or-racing-bucket"));
        clear_dirty_usage_buckets_for_tests();
    }

    #[test]
    #[serial]
    fn deleting_a_clean_bucket_invalidates_an_inflight_usage_snapshot() {
        clear_dirty_usage_buckets_for_tests();
        let snapshot = snapshot_dirty_usage_buckets(&[bucket_info("photos")], dirty_usage_generation());
        assert_eq!(dirty_usage_snapshot_status(&snapshot), DirtyUsageSnapshotStatus::Current);

        record_dirty_usage_bucket("photos");

        assert_eq!(dirty_usage_snapshot_status(&snapshot), DirtyUsageSnapshotStatus::Changed);
        assert!(dirty_usage_buckets().contains_key("photos"));
        clear_dirty_usage_buckets_for_tests();
    }

    #[test]
    #[serial]
    fn deleting_a_bucket_during_listing_invalidates_the_resulting_usage_snapshot() {
        clear_dirty_usage_buckets_for_tests();
        let generation_before_bucket_list = dirty_usage_generation();

        record_dirty_usage_bucket("photos");
        let snapshot = snapshot_dirty_usage_buckets(&[bucket_info("photos")], generation_before_bucket_list);

        assert_eq!(dirty_usage_snapshot_status(&snapshot), DirtyUsageSnapshotStatus::Changed);
        assert!(dirty_usage_buckets().contains_key("photos"));
        clear_dirty_usage_buckets_for_tests();
    }

    #[test]
    #[serial]
    fn scanner_maintenance_change_advances_generation_and_marks_usage_dirty() {
        clear_dirty_usage_buckets_for_tests();
        let generation = scanner_maintenance_generation();

        record_scanner_maintenance_change("photos");

        assert!(scanner_maintenance_generation() > generation);
        assert!(dirty_usage_buckets().contains_key("photos"));
        clear_dirty_usage_buckets_for_tests();
    }

    #[test]
    #[serial]
    fn dirty_usage_clear_excludes_failed_buckets() {
        clear_dirty_usage_buckets_for_tests();
        record_dirty_usage_bucket("photos");
        record_dirty_usage_bucket("videos");
        let buckets = vec![bucket_info("photos"), bucket_info("videos")];
        let snapshot = snapshot_dirty_usage_buckets(&buckets, dirty_usage_generation());
        let failed_buckets = HashSet::from(["videos".to_string()]);
        let clear_snapshot = dirty_usage_buckets_excluding_failed(&snapshot.buckets, &failed_buckets);

        clear_dirty_usage_buckets(&clear_snapshot);

        let dirty_buckets = dirty_usage_buckets();
        assert!(!dirty_buckets.contains_key("photos"));
        assert!(dirty_buckets.contains_key("videos"));
        drop(dirty_buckets);
        clear_dirty_usage_buckets_for_tests();
    }

    #[test]
    fn dirty_usage_clear_plan_excludes_cache_save_failures() {
        let snapshot = DirtyUsageBuckets::from([("photos".to_string(), 1), ("videos".to_string(), 2)]);
        let failed_buckets = HashSet::from(["videos".to_string()]);

        let clear_snapshot = should_clear_dirty_usage_snapshot(true, true, false, true, &snapshot, &failed_buckets)
            .expect("successful completed cycle should produce a clear snapshot");

        assert!(clear_snapshot.contains_key("photos"));
        assert!(!clear_snapshot.contains_key("videos"));
    }

    #[test]
    #[serial]
    fn dirty_usage_is_acknowledged_only_after_durable_usage_confirmation() {
        clear_dirty_usage_buckets_for_tests();
        record_dirty_usage_bucket("photos");
        let snapshot = snapshot_dirty_usage_buckets(&[bucket_info("photos")], dirty_usage_generation());

        let unconfirmed = ScannerCycleResult::new(ScannerCycleStatus::Complete, Some(snapshot.buckets.as_ref().clone()));
        drop(unconfirmed);
        assert!(dirty_usage_buckets().contains_key("photos"));

        let confirmed = ScannerCycleResult::new(ScannerCycleStatus::Complete, Some(snapshot.buckets.as_ref().clone()));
        confirmed.acknowledge_durable_usage();
        assert!(!dirty_usage_buckets().contains_key("photos"));
        clear_dirty_usage_buckets_for_tests();
    }

    #[test]
    #[serial]
    fn clear_dirty_usage_bucket_removes_deleted_bucket_marker() {
        clear_dirty_usage_buckets_for_tests();
        record_dirty_usage_bucket("photos");
        record_dirty_usage_bucket("videos");

        clear_dirty_usage_bucket("photos");

        let buckets = vec![bucket_info("photos"), bucket_info("videos")];
        let snapshot = snapshot_dirty_usage_buckets(&buckets, dirty_usage_generation());
        assert!(!snapshot.buckets.contains_key("photos"));
        assert!(snapshot.buckets.contains_key("videos"));
        assert_eq!(dirty_usage_bucket_count(), 1);
        clear_dirty_usage_buckets_for_tests();
    }

    #[test]
    fn bucket_usage_scan_order_prioritizes_dirty_buckets() {
        let buckets = vec![bucket_info("missing"), bucket_info("cached"), bucket_info("dirty")];
        let mut old_cache = DataUsageCache {
            info: DataUsageCacheInfo {
                name: DATA_USAGE_ROOT.to_string(),
                ..Default::default()
            },
            ..Default::default()
        };
        old_cache.replace("cached", DATA_USAGE_ROOT, DataUsageEntry::default());
        old_cache.replace("dirty", DATA_USAGE_ROOT, DataUsageEntry::default());

        let dirty_buckets = HashMap::from([("dirty".to_string(), 1)]);
        let ordered = bucket_usage_scan_order(&buckets, &old_cache, &dirty_buckets);
        let names = ordered.iter().map(|bucket| bucket.name.as_str()).collect::<Vec<_>>();

        assert_eq!(names, vec!["dirty", "missing", "cached"]);
    }

    #[test]
    fn record_set_scan_failure_preserves_first_error() {
        let mut first = None;
        record_set_scan_failure(&mut first, Error::other("first"));
        record_set_scan_failure(&mut first, Error::other("second"));

        let first = first.expect("first error should be recorded");
        assert!(first.to_string().contains("first"));
    }

    #[tokio::test]
    async fn scanner_task_join_error_includes_stage() {
        let handle = tokio::spawn(async {
            tokio::time::sleep(Duration::from_secs(60)).await;
        });
        handle.abort();

        let join_err = handle.await.expect_err("aborted task should return a join error");
        let err = scanner_task_join_error("scanner set", join_err);

        assert!(err.to_string().contains("scanner set task join failed"));
    }

    #[test]
    fn finalize_nsscanner_result_returns_ok_when_any_set_succeeds() {
        let mut results = vec![DataUsageCache::default(), DataUsageCache::default()];
        results[1].info.last_update = Some(SystemTime::now());

        let result = finalize_nsscanner_result(&results, Some(Error::other("set failed")));
        assert!(result.is_ok());
    }

    #[test]
    fn finalize_nsscanner_result_returns_first_error_when_all_sets_fail() {
        let results = vec![DataUsageCache::default(), DataUsageCache::default()];

        let err = finalize_nsscanner_result(&results, Some(Error::other("set failed")))
            .expect_err("all failed sets should bubble first error");
        assert!(err.to_string().contains("set failed"));
    }

    #[test]
    fn scanner_cycle_status_requires_a_clean_complete_snapshot() {
        assert_eq!(
            classify_nsscanner_cycle(
                true,
                false,
                false,
                ScannerBucketScanStatus::Complete,
                DirtyUsageSnapshotStatus::Current,
                ScannerCycleActivityStatus::Unchanged,
            ),
            ScannerCycleStatus::Complete
        );
        assert_eq!(
            classify_nsscanner_cycle(
                true,
                false,
                false,
                ScannerBucketScanStatus::Complete,
                DirtyUsageSnapshotStatus::Changed,
                ScannerCycleActivityStatus::Unchanged,
            ),
            ScannerCycleStatus::Superseded
        );
        assert_eq!(
            classify_nsscanner_cycle(
                true,
                false,
                false,
                ScannerBucketScanStatus::Complete,
                DirtyUsageSnapshotStatus::Current,
                ScannerCycleActivityStatus::Changed,
            ),
            ScannerCycleStatus::Superseded
        );
        assert_eq!(
            classify_nsscanner_cycle(
                true,
                false,
                false,
                ScannerBucketScanStatus::Complete,
                DirtyUsageSnapshotStatus::Current,
                ScannerCycleActivityStatus::Unverified,
            ),
            ScannerCycleStatus::Incomplete
        );

        for status in [
            classify_nsscanner_cycle(
                false,
                false,
                false,
                ScannerBucketScanStatus::Complete,
                DirtyUsageSnapshotStatus::Current,
                ScannerCycleActivityStatus::Unchanged,
            ),
            classify_nsscanner_cycle(
                true,
                true,
                false,
                ScannerBucketScanStatus::Complete,
                DirtyUsageSnapshotStatus::Current,
                ScannerCycleActivityStatus::Unchanged,
            ),
            classify_nsscanner_cycle(
                true,
                false,
                true,
                ScannerBucketScanStatus::Complete,
                DirtyUsageSnapshotStatus::Current,
                ScannerCycleActivityStatus::Unchanged,
            ),
            classify_nsscanner_cycle(
                true,
                false,
                false,
                ScannerBucketScanStatus::Failed,
                DirtyUsageSnapshotStatus::Current,
                ScannerCycleActivityStatus::Changed,
            ),
            classify_nsscanner_cycle(
                false,
                false,
                false,
                ScannerBucketScanStatus::Partial,
                DirtyUsageSnapshotStatus::Changed,
                ScannerCycleActivityStatus::Changed,
            ),
        ] {
            assert_eq!(status, ScannerCycleStatus::Incomplete);
        }
    }

    #[test]
    fn scanner_cycle_fails_closed_for_namespace_disappearance() {
        for activity_status in [
            ScannerCycleActivityStatus::Changed,
            ScannerCycleActivityStatus::Unchanged,
            ScannerCycleActivityStatus::Unverified,
        ] {
            assert_eq!(
                classify_nsscanner_cycle(
                    false,
                    false,
                    false,
                    ScannerBucketScanStatus::NamespaceNotFound,
                    DirtyUsageSnapshotStatus::Changed,
                    activity_status,
                ),
                ScannerCycleStatus::Incomplete
            );
        }
        assert_eq!(
            classify_nsscanner_cycle(
                true,
                true,
                false,
                ScannerBucketScanStatus::NamespaceNotFound,
                DirtyUsageSnapshotStatus::Changed,
                ScannerCycleActivityStatus::Changed,
            ),
            ScannerCycleStatus::Incomplete
        );
    }

    #[test]
    fn scanner_cycle_fails_closed_when_dirty_generation_is_unverified() {
        assert_eq!(
            classify_nsscanner_cycle(
                true,
                false,
                false,
                ScannerBucketScanStatus::Complete,
                DirtyUsageSnapshotStatus::Unverified,
                ScannerCycleActivityStatus::Unchanged,
            ),
            ScannerCycleStatus::Incomplete
        );
    }

    #[test]
    fn scanner_bucket_failure_status_preserves_the_strongest_failure() {
        assert_eq!(scanner_bucket_scan_status(false, false, false), ScannerBucketScanStatus::Complete);
        assert_eq!(scanner_bucket_scan_status(false, false, true), ScannerBucketScanStatus::NamespaceNotFound);
        assert_eq!(scanner_bucket_scan_status(false, true, true), ScannerBucketScanStatus::Partial);
        assert_eq!(scanner_bucket_scan_status(true, true, true), ScannerBucketScanStatus::Failed);
    }

    #[test]
    fn scanner_cycle_surfaces_persisted_pending_heal_work() {
        let clean = DataUsageCache::default();
        assert!(!scanner_results_have_pending_maintenance_work(std::slice::from_ref(&clean)));

        let mut pending = clean;
        pending.info.pending_heals.push(crate::PendingScannerHeal {
            kind: crate::PendingScannerHealKind::Object,
            bucket: "photos".to_string(),
            object: Some("image.jpg".to_string()),
            version_id: None,
            scan_mode: HealScanMode::Normal,
            first_seen: 1,
            last_attempt: 1,
            attempts: 1,
            last_admission_result: "queue_full".to_string(),
            last_admission_reason: "capacity".to_string(),
        });

        assert!(scanner_results_have_pending_maintenance_work(&[pending]));
    }

    #[tokio::test]
    async fn bucket_cache_pending_heal_reaches_cycle_maintenance_state() {
        let pending_maintenance_work = Arc::new(AtomicBool::new(false));
        let mut bucket_cache = DataUsageCache {
            info: DataUsageCacheInfo {
                name: "photos".to_string(),
                ..Default::default()
            },
            ..Default::default()
        };
        bucket_cache.replace("photos", DATA_USAGE_ROOT, DataUsageEntry::default());
        bucket_cache.info.pending_heals.push(crate::PendingScannerHeal {
            kind: crate::PendingScannerHealKind::Object,
            bucket: "photos".to_string(),
            object: Some("image.jpg".to_string()),
            version_id: None,
            scan_mode: HealScanMode::Normal,
            first_seen: 1,
            last_attempt: 1,
            attempts: 1,
            last_admission_result: "queue_full".to_string(),
            last_admission_reason: "capacity".to_string(),
        });
        let (sender, mut receiver) = mpsc::channel(1);

        send_cache_root_entry_info(&sender, &bucket_cache, &pending_maintenance_work)
            .await
            .expect("bucket result should send");

        let cycle_pending = pending_maintenance_work_for_cycle(&pending_maintenance_work, &[]);
        assert!(cycle_pending);
        assert_eq!(
            crate::scanner::scanner_cycle_outcome_with_pending_maintenance(
                crate::scanner::ScannerCycleOutcome::Completed,
                cycle_pending,
            ),
            crate::scanner::ScannerCycleOutcome::CompletedWithPendingMaintenance
        );
        assert!(receiver.recv().await.is_some());
    }

    #[test]
    #[serial]
    fn scanner_concurrency_limit_preserves_available_when_unconfigured() {
        crate::reset_foreground_read_activity_for_test();
        assert_eq!(scanner_concurrency_limit(0, 4), 4);
    }

    #[test]
    #[serial]
    fn scanner_concurrency_limit_caps_to_configured_value() {
        crate::reset_foreground_read_activity_for_test();
        assert_eq!(scanner_concurrency_limit(2, 4), 2);
    }

    #[test]
    #[serial]
    fn scanner_concurrency_limit_never_exceeds_available_work() {
        crate::reset_foreground_read_activity_for_test();
        assert_eq!(scanner_concurrency_limit(8, 4), 4);
    }

    #[test]
    #[serial]
    fn scanner_concurrency_limit_handles_no_available_work() {
        crate::reset_foreground_read_activity_for_test();
        assert_eq!(scanner_concurrency_limit(2, 0), 0);
    }

    #[test]
    #[serial]
    fn scanner_concurrency_limit_yields_to_foreground_reads() {
        crate::reset_foreground_read_activity_for_test();
        crate::set_foreground_read_activity(8);
        assert_eq!(scanner_concurrency_limit(0, 4), 1);
        assert_eq!(scanner_concurrency_limit(3, 4), 1);
        crate::reset_foreground_read_activity_for_test();
    }

    #[test]
    #[serial]
    fn scanner_concurrency_limit_yields_to_streaming_reads() {
        crate::reset_foreground_read_activity_for_test();
        let _guard = crate::ForegroundReadGuard::new();

        assert_eq!(scanner_concurrency_limit(0, 4), 1);
        assert_eq!(scanner_concurrency_limit(3, 4), 1);
    }

    #[test]
    fn decrement_atomic_usize_saturates_at_zero() {
        let counter = AtomicUsize::new(1);
        assert_eq!(decrement_atomic_usize(&counter), 0);
        assert_eq!(decrement_atomic_usize(&counter), 0);
    }

    #[test]
    fn increment_atomic_usize_saturates_at_max() {
        let counter = AtomicUsize::new(usize::MAX);
        assert_eq!(increment_atomic_usize(&counter), usize::MAX);
        assert_eq!(counter.load(Ordering::Relaxed), usize::MAX);
    }

    #[test]
    #[serial]
    fn scanner_max_concurrent_set_scans_uses_env_cap() {
        with_var(ENV_SCANNER_MAX_CONCURRENT_SET_SCANS, Some("2"), || {
            crate::runtime_config::refresh_scanner_runtime_config_for_tests();
            assert_eq!(scanner_max_concurrent_set_scans(4), 2);
        });
        crate::runtime_config::refresh_scanner_runtime_config_for_tests();
    }

    #[test]
    #[serial]
    fn scanner_max_concurrent_disk_scans_uses_env_cap() {
        with_var(ENV_SCANNER_MAX_CONCURRENT_DISK_SCANS, Some("1"), || {
            crate::runtime_config::refresh_scanner_runtime_config_for_tests();
            assert_eq!(scanner_max_concurrent_disk_scans(4), 1);
        });
        crate::runtime_config::refresh_scanner_runtime_config_for_tests();
    }

    #[test]
    #[cfg(windows)]
    fn is_xl_meta_path_accepts_windows_separator() {
        assert!(is_xl_meta_path("D:\\data\\bucket\\object\\xl.meta"));
    }

    #[test]
    fn is_xl_meta_path_accepts_forward_separator() {
        assert!(is_xl_meta_path("/data/bucket/object/xl.meta"));
    }

    #[tokio::test]
    async fn get_size_treats_missing_metadata_as_skip_file() {
        let temp_dir = std::env::temp_dir().join(format!("rustfs-scanner-missing-meta-{}", Uuid::new_v4()));
        let bucket = "bucket";
        let object = "object";
        let object_dir = temp_dir.join(bucket).join(object);
        let metadata_path = object_dir.join(STORAGE_FORMAT_FILE);

        tokio::fs::create_dir_all(&object_dir)
            .await
            .expect("failed to create object directory");
        tokio::fs::write(&metadata_path, [])
            .await
            .expect("failed to create metadata placeholder");

        let endpoint = Endpoint::try_from(temp_dir.to_string_lossy().as_ref()).expect("failed to create endpoint");
        let disk = new_disk(
            &endpoint,
            &DiskOption {
                cleanup: false,
                health_check: false,
            },
        )
        .await
        .expect("failed to open local disk");

        let relative_path = metadata_path.to_string_lossy().to_string();
        let (_, scanner_path) = path2_bucket_object_with_base_path(temp_dir.to_string_lossy().as_ref(), relative_path.as_str());
        let file_type = tokio::fs::metadata(&metadata_path)
            .await
            .expect("failed to stat metadata placeholder")
            .file_type();

        tokio::fs::remove_dir_all(&object_dir)
            .await
            .expect("failed to remove object directory");

        let item = ScannerItem {
            path: scanner_path,
            bucket: bucket.to_string(),
            prefix: object.to_string(),
            object_name: STORAGE_FORMAT_FILE.to_string(),
            file_type,
            lifecycle: None,
            object_lock: None,
            replication: None,
            heal_enabled: false,
            heal_bitrot: false,
            debug: false,
        };

        let err = disk
            .get_size(item)
            .await
            .expect_err("missing metadata should be skipped instead of reported as a scanner failure");
        assert!(matches!(err, StorageError::Io(ref io) if io.to_string() == SCANNER_SKIP_FILE_ERROR));

        let _ = tokio::fs::remove_dir_all(&temp_dir).await;
    }

    #[tokio::test]
    async fn get_size_marks_corrupt_metadata_for_heal() {
        let temp_dir = std::env::temp_dir().join(format!("rustfs-scanner-corrupt-meta-{}", Uuid::new_v4()));
        let bucket = "bucket";
        let object = "object";
        let object_dir = temp_dir.join(bucket).join(object);
        let metadata_path = object_dir.join(STORAGE_FORMAT_FILE);

        tokio::fs::create_dir_all(&object_dir)
            .await
            .expect("failed to create object directory");
        tokio::fs::write(&metadata_path, b"not-valid-filemeta")
            .await
            .expect("failed to write corrupt metadata");

        let endpoint = Endpoint::try_from(temp_dir.to_string_lossy().as_ref()).expect("failed to create endpoint");
        let disk = new_disk(
            &endpoint,
            &DiskOption {
                cleanup: false,
                health_check: false,
            },
        )
        .await
        .expect("failed to open local disk");

        let relative_path = metadata_path.to_string_lossy().to_string();
        let (_, scanner_path) = path2_bucket_object_with_base_path(temp_dir.to_string_lossy().as_ref(), relative_path.as_str());
        let file_type = tokio::fs::metadata(&metadata_path)
            .await
            .expect("failed to stat metadata")
            .file_type();

        let item = ScannerItem {
            path: scanner_path,
            bucket: bucket.to_string(),
            prefix: object.to_string(),
            object_name: STORAGE_FORMAT_FILE.to_string(),
            file_type,
            lifecycle: None,
            object_lock: None,
            replication: None,
            heal_enabled: false,
            heal_bitrot: false,
            debug: false,
        };

        let err = disk
            .get_size(item)
            .await
            .expect_err("corrupt metadata should be surfaced as scanner-heal work");
        assert!(is_scanner_metadata_corrupt_error(&err));

        let _ = tokio::fs::remove_dir_all(&temp_dir).await;
    }

    #[tokio::test]
    async fn get_size_counts_delete_markers_separately_from_versions() {
        let temp_dir = std::env::temp_dir().join(format!("rustfs-scanner-versioned-usage-{}", Uuid::new_v4()));
        let bucket = "bucket";
        let object = "object";
        let object_dir = temp_dir.join(bucket).join(object);
        let metadata_path = object_dir.join(STORAGE_FORMAT_FILE);

        tokio::fs::create_dir_all(&object_dir)
            .await
            .expect("failed to create object directory");

        let mut meta = FileMeta::new();
        for (size, timestamp) in [(10, 10), (20, 20)] {
            let mut fi = FileInfo::new(object, 1, 1);
            fi.version_id = Some(Uuid::new_v4());
            fi.mod_time = Some(OffsetDateTime::from_unix_timestamp(timestamp).expect("timestamp should be valid"));
            fi.size = size;
            meta.add_version(fi).expect("object version should be added");
        }

        // A real delete marker carries no erasure geometry (delete paths build it as
        // `FileInfo { deleted: true, .. }`). Construct it that way so it classifies as a
        // storage delete marker rather than a purge-pending payload object.
        let delete_marker = FileInfo {
            name: object.to_string(),
            version_id: Some(Uuid::new_v4()),
            mod_time: Some(OffsetDateTime::from_unix_timestamp(30).expect("timestamp should be valid")),
            deleted: true,
            ..Default::default()
        };
        meta.add_version(delete_marker).expect("delete marker should be added");

        tokio::fs::write(&metadata_path, meta.marshal_msg().expect("metadata should marshal"))
            .await
            .expect("failed to write metadata");

        let endpoint = Endpoint::try_from(temp_dir.to_string_lossy().as_ref()).expect("failed to create endpoint");
        let disk = new_disk(
            &endpoint,
            &DiskOption {
                cleanup: false,
                health_check: false,
            },
        )
        .await
        .expect("failed to open local disk");

        let relative_path = metadata_path.to_string_lossy().to_string();
        let (_, scanner_path) = path2_bucket_object_with_base_path(temp_dir.to_string_lossy().as_ref(), relative_path.as_str());
        let file_type = tokio::fs::metadata(&metadata_path)
            .await
            .expect("failed to stat metadata")
            .file_type();
        let item = ScannerItem {
            path: scanner_path,
            bucket: bucket.to_string(),
            prefix: object.to_string(),
            object_name: STORAGE_FORMAT_FILE.to_string(),
            file_type,
            lifecycle: None,
            object_lock: None,
            replication: None,
            heal_enabled: false,
            heal_bitrot: false,
            debug: false,
        };

        let summary = disk.get_size(item).await.expect("scanner should read versioned metadata");

        assert_eq!(summary.versions, 2);
        assert_eq!(summary.delete_markers, 1);
        assert_eq!(summary.total_size, 30);

        let _ = tokio::fs::remove_dir_all(&temp_dir).await;
    }

    #[test]
    fn cache_root_entry_info_flattens_bucket_children() {
        let mut cache = DataUsageCache {
            info: DataUsageCacheInfo {
                name: "bucket".to_string(),
                ..Default::default()
            },
            ..Default::default()
        };
        cache.replace(
            "bucket",
            DATA_USAGE_ROOT,
            DataUsageEntry {
                size: 10,
                objects: 1,
                ..Default::default()
            },
        );
        cache.replace(
            "bucket/prefix",
            "bucket",
            DataUsageEntry {
                size: 20,
                objects: 2,
                ..Default::default()
            },
        );

        let info = cache_root_entry_info(&cache).expect("valid cache should flatten");

        assert_eq!(info.name, "bucket");
        assert_eq!(info.parent, DATA_USAGE_ROOT);
        assert_eq!(info.entry.size, 30);
        assert_eq!(info.entry.objects, 3);
        assert!(info.entry.children.is_empty());
    }

    #[test]
    fn cache_root_entry_info_rejects_missing_or_dangling_roots() {
        let missing_root = DataUsageCache {
            info: DataUsageCacheInfo {
                name: "bucket".to_string(),
                ..Default::default()
            },
            ..Default::default()
        };
        assert!(cache_root_entry_info(&missing_root).is_err());

        let mut dangling = missing_root;
        let mut root = DataUsageEntry::default();
        root.add_child(&crate::hash_path("bucket/missing"));
        dangling.replace("bucket", DATA_USAGE_ROOT, root);
        assert!(cache_root_entry_info(&dangling).is_err());
    }

    #[test]
    fn apply_bucket_result_to_cache_updates_bucket_entry() {
        let mut cache = DataUsageCache {
            info: DataUsageCacheInfo {
                name: DATA_USAGE_ROOT.to_string(),
                ..Default::default()
            },
            ..Default::default()
        };
        cache.replace(
            "bucket",
            DATA_USAGE_ROOT,
            DataUsageEntry {
                size: 5,
                objects: 1,
                ..Default::default()
            },
        );

        let update_time = SystemTime::now();
        apply_bucket_result_to_cache(
            &mut cache,
            DataUsageEntryInfo {
                name: "bucket".to_string(),
                parent: DATA_USAGE_ROOT.to_string(),
                entry: DataUsageEntry {
                    size: 10,
                    objects: 2,
                    ..Default::default()
                },
            },
            update_time,
        );

        assert_eq!(cache.info.last_update, Some(update_time));
        let entry = cache.find("bucket").expect("bucket entry should remain present");
        assert_eq!(entry.size, 10);
        assert_eq!(entry.objects, 2);
    }
}
