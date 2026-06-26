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
    DATA_USAGE_CACHE_NAME, DATA_USAGE_ROOT, DataUsageCache, DataUsageCacheInfo, DataUsageEntry, DataUsageEntryInfo,
    DataUsageInfo, ScannerError, ScannerObjectIO, SizeSummary, TierStats,
};
use futures::future::join_all;
use metrics::counter;
use rand::seq::SliceRandom as _;
use rustfs_common::heal_channel::HealScanMode;
use rustfs_common::metrics::{Metric, Metrics, emit_scan_bucket_drive_complete, emit_scan_bucket_drive_partial, global_metrics};
#[cfg(test)]
use rustfs_config::{ENV_SCANNER_MAX_CONCURRENT_DISK_SCANS, ENV_SCANNER_MAX_CONCURRENT_SET_SCANS};
use rustfs_filemeta::FileMeta;
use rustfs_utils::path::path_join_buf;
use s3s::dto::{BucketLifecycleConfiguration, ReplicationConfiguration};
use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{LazyLock, Mutex as StdMutex, MutexGuard};
use std::time::{Instant, SystemTime};
use std::{fmt::Debug, sync::Arc};
use time::OffsetDateTime;
use tokio::sync::{Mutex, Notify, Semaphore, mpsc};
use tokio::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, warn};

use crate::ScannerObjectInfo as ObjectInfo;
use crate::storage_api::scanner_io::{BucketInfo, BucketOperations, BucketOptions, DiskSetSelector, StorageAdminApi};
use crate::{
    BucketTargetSys, BucketVersioningSys, Disk, DiskError, ECStore, EcstoreError as Error, EcstoreResult as Result,
    ReplicationConfig, STORAGE_FORMAT_FILE, ScannerDiskExt as _, ScannerLifecycleConfigExt as _,
    ScannerReplicationConfigExt as _, ScannerVersioningConfigExt as _, SetDisks, StorageError, enqueue_runtime_free_version,
    get_lifecycle_config, get_object_lock_config, get_replication_config, list_runtime_tiers,
    resolve_scanner_object_store_handle, storageclass,
};

pub(crate) const SCANNER_SKIP_FILE_ERROR: &str = "skip file";
const LOG_COMPONENT_SCANNER: &str = "scanner";
const LOG_SUBSYSTEM_IO: &str = "io";
const EVENT_SCANNER_DISK_BUCKET_STATE: &str = "scanner_disk_bucket_state";
const EVENT_SCANNER_DATA_USAGE_STREAM: &str = "scanner_data_usage_stream";
const EVENT_SCANNER_CACHE_PERSIST_STATE: &str = "scanner_cache_persist_state";
const EVENT_SCANNER_SET_STATE: &str = "scanner_set_state";

const METRIC_SCANNER_SET_SCAN_CONCURRENCY_LIMIT: &str = "rustfs_scanner_set_scan_concurrency_limit";
const METRIC_SCANNER_DISK_SCAN_CONCURRENCY_LIMIT: &str = "rustfs_scanner_disk_scan_concurrency_limit";
const METRIC_SCANNER_SET_SCAN_WAIT_SECONDS: &str = "rustfs_scanner_set_scan_wait_seconds";
const METRIC_SCANNER_DISK_SCAN_WAIT_SECONDS: &str = "rustfs_scanner_disk_scan_wait_seconds";
const METRIC_SCANNER_SET_SCANS_ACTIVE: &str = "rustfs_scanner_set_scans_active";
const METRIC_SCANNER_SET_SCANS_QUEUED: &str = "rustfs_scanner_set_scans_queued";
const METRIC_SCANNER_DISK_BUCKET_SCANS_ACTIVE: &str = "rustfs_scanner_disk_bucket_scans_active";
const METRIC_SCANNER_DISK_BUCKET_SCANS_QUEUED: &str = "rustfs_scanner_disk_bucket_scans_queued";

pub type DirtyUsageBuckets = HashMap<String, u64>;

#[derive(Clone)]
pub struct ScannerBucketScanPlan {
    buckets: Vec<BucketInfo>,
    dirty_usage_buckets: Arc<DirtyUsageBuckets>,
}

impl ScannerBucketScanPlan {
    fn new(buckets: Vec<BucketInfo>, dirty_usage_buckets: Arc<DirtyUsageBuckets>) -> Self {
        Self {
            buckets,
            dirty_usage_buckets,
        }
    }
}

static DIRTY_USAGE_BUCKET_GENERATION: AtomicU64 = AtomicU64::new(0);
static DIRTY_USAGE_BUCKETS: LazyLock<StdMutex<DirtyUsageBuckets>> = LazyLock::new(|| StdMutex::new(HashMap::new()));
static DIRTY_USAGE_BUCKET_NOTIFY: LazyLock<Notify> = LazyLock::new(Notify::new);

fn dirty_usage_buckets() -> MutexGuard<'static, DirtyUsageBuckets> {
    DIRTY_USAGE_BUCKETS.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn usize_to_u64_saturated(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

pub fn record_dirty_usage_bucket(bucket: &str) {
    if bucket.is_empty() {
        return;
    }

    let generation = DIRTY_USAGE_BUCKET_GENERATION.fetch_add(1, Ordering::AcqRel) + 1;
    let pending_buckets = {
        let mut dirty_buckets = dirty_usage_buckets();
        dirty_buckets.insert(bucket.to_string(), generation);
        dirty_buckets.len()
    };
    global_metrics().record_scanner_dirty_usage_pending(usize_to_u64_saturated(pending_buckets));
    DIRTY_USAGE_BUCKET_NOTIFY.notify_one();
}

pub fn clear_dirty_usage_bucket(bucket: &str) {
    if bucket.is_empty() {
        return;
    }

    let pending_buckets = {
        let mut dirty_buckets = dirty_usage_buckets();
        dirty_buckets.remove(bucket);
        dirty_buckets.len()
    };
    global_metrics().record_scanner_dirty_usage_clear(usize_to_u64_saturated(pending_buckets));
}

fn snapshot_dirty_usage_buckets(buckets: &[BucketInfo]) -> DirtyUsageBuckets {
    let snapshot = {
        let dirty_buckets = dirty_usage_buckets();
        buckets
            .iter()
            .filter_map(|bucket| {
                dirty_buckets
                    .get(&bucket.name)
                    .map(|generation| (bucket.name.clone(), *generation))
            })
            .collect::<DirtyUsageBuckets>()
    };
    global_metrics().record_scanner_dirty_usage_cycle_snapshot(usize_to_u64_saturated(snapshot.len()));
    snapshot
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
        (cleared_buckets, dirty_buckets.len())
    };
    global_metrics()
        .record_scanner_dirty_usage_cycle_clear(usize_to_u64_saturated(cleared_buckets), usize_to_u64_saturated(pending_buckets));
}

fn dirty_usage_snapshot_covers_current(snapshot: &DirtyUsageBuckets) -> bool {
    let dirty_buckets = dirty_usage_buckets();
    dirty_buckets.iter().all(|(bucket, generation)| {
        snapshot
            .get(bucket)
            .is_some_and(|snapshot_generation| snapshot_generation == generation)
    })
}

#[cfg(test)]
fn dirty_usage_bucket_count() -> usize {
    dirty_usage_buckets().len()
}

#[cfg(test)]
fn clear_dirty_usage_buckets_for_tests() {
    dirty_usage_buckets().clear();
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

fn record_set_scan_failure(first_err: &mut Option<Error>, err: Error) {
    if first_err.is_none() {
        *first_err = Some(err);
    }
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

fn is_xl_meta_path(path: &str) -> bool {
    Path::new(path)
        .file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| name == STORAGE_FORMAT_FILE)
}

fn cache_root_entry_info(cache: &DataUsageCache) -> DataUsageEntryInfo {
    let entry = cache.root().map(|root| cache.flatten(&root)).unwrap_or_default();

    DataUsageEntryInfo {
        name: cache.info.name.clone(),
        parent: DATA_USAGE_ROOT.to_string(),
        entry,
    }
}

fn apply_bucket_result_to_cache(cache: &mut DataUsageCache, result: DataUsageEntryInfo, update_time: SystemTime) {
    cache.replace(&result.name, &result.parent, result.entry);
    cache.info.last_update = Some(update_time);
}

fn should_publish_completed_snapshot(completed_count: usize, total_count: usize, budget_elapsed: bool, cancelled: bool) -> bool {
    total_count > 0 && completed_count == total_count && !budget_elapsed && !cancelled
}

fn completed_data_usage_info(
    results: &[DataUsageCache],
    all_buckets: &[String],
    budget_elapsed: bool,
    cancelled: bool,
    dirty_usage_current: bool,
) -> Option<(DataUsageInfo, SystemTime)> {
    let completed_set_count = results.iter().filter(|result| result.info.last_update.is_some()).count();
    if !should_publish_completed_snapshot(completed_set_count, results.len(), budget_elapsed, cancelled) || !dirty_usage_current {
        return None;
    }

    let mut all_merged = DataUsageCache::default();
    for result in results.iter() {
        all_merged.merge(result);
    }

    let merged_last_update = all_merged.info.last_update.unwrap_or(SystemTime::UNIX_EPOCH);
    all_merged.root()?;

    Some((all_merged.dui(&all_merged.info.name, all_buckets), merged_last_update))
}

#[cfg(test)]
mod publish_gate_tests {
    use super::*;

    #[test]
    fn should_publish_completed_snapshot_requires_full_clean_cycle() {
        assert!(should_publish_completed_snapshot(3, 3, false, false));
        assert!(!should_publish_completed_snapshot(2, 3, false, false));
        assert!(!should_publish_completed_snapshot(3, 3, true, false));
        assert!(!should_publish_completed_snapshot(3, 3, false, true));
        assert!(!should_publish_completed_snapshot(0, 0, false, false));
    }

    fn completed_root_cache(bucket: &str, objects: usize, update_secs: u64) -> DataUsageCache {
        let mut cache = DataUsageCache {
            info: DataUsageCacheInfo {
                name: DATA_USAGE_ROOT.to_string(),
                last_update: Some(SystemTime::UNIX_EPOCH + Duration::from_secs(update_secs)),
                ..Default::default()
            },
            ..Default::default()
        };
        cache.replace(
            bucket,
            DATA_USAGE_ROOT,
            DataUsageEntry {
                objects,
                size: objects * 10,
                ..Default::default()
            },
        );
        cache
    }

    #[test]
    fn completed_data_usage_info_requires_every_set_before_publish() {
        let all_buckets = vec!["bucket-a".to_string(), "bucket-b".to_string()];
        let first_set = completed_root_cache("bucket-a", 1, 10);
        let second_set = completed_root_cache("bucket-b", 2, 20);

        assert!(
            completed_data_usage_info(&[first_set.clone(), DataUsageCache::default()], &all_buckets, false, false, true)
                .is_none()
        );
        assert!(completed_data_usage_info(&[first_set.clone(), second_set.clone()], &all_buckets, true, false, true).is_none());
        assert!(completed_data_usage_info(&[first_set.clone(), second_set.clone()], &all_buckets, false, true, true).is_none());
        assert!(completed_data_usage_info(&[first_set.clone(), second_set.clone()], &all_buckets, false, false, false).is_none());

        let (data_usage_info, last_update) =
            completed_data_usage_info(&[first_set, second_set], &all_buckets, false, false, true)
                .expect("all completed sets should produce a publishable data usage snapshot");
        assert_eq!(last_update, SystemTime::UNIX_EPOCH + Duration::from_secs(20));
        assert_eq!(data_usage_info.objects_total_count, 3);
        assert_eq!(data_usage_info.buckets_usage.len(), 2);
    }
}

async fn send_cache_root_entry_info(
    bucket_result_tx: &Arc<Mutex<mpsc::Sender<DataUsageEntryInfo>>>,
    cache: &DataUsageCache,
) -> std::result::Result<(), mpsc::error::SendError<DataUsageEntryInfo>> {
    bucket_result_tx.lock().await.send(cache_root_entry_info(cache)).await
}

async fn persist_and_publish_cache_snapshot<S: ScannerObjectIO>(
    store: Arc<S>,
    updates: &mpsc::Sender<DataUsageCache>,
    cache_snapshot: DataUsageCache,
) -> Option<SystemTime> {
    let last_update = cache_snapshot.info.last_update;

    let done_save = Metrics::time(Metric::SaveUsage);
    if let Err(e) = cache_snapshot.save(store, DATA_USAGE_CACHE_NAME).await {
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
    }
    done_save();

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

async fn send_merged_data_usage_update(updates: &mpsc::Sender<DataUsageInfo>, data_usage_info: DataUsageInfo) {
    if let Err(e) = updates.send(data_usage_info).await {
        error!(
            target: "rustfs::scanner::io",
            event = EVENT_SCANNER_DATA_USAGE_STREAM,
            component = LOG_COMPONENT_SCANNER,
            subsystem = LOG_SUBSYSTEM_IO,
            state = "send_merged_failed",
            error = %e,
            "Scanner merged data usage publish failed"
        );
    }
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
        &self,
        ctx: CancellationToken,
        budget: Arc<ScannerCycleBudget>,
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
}

#[async_trait::async_trait]
impl ScannerIO for ECStore {
    #[tracing::instrument(skip(self, budget, updates))]
    async fn nsscanner(
        &self,
        ctx: CancellationToken,
        budget: Arc<ScannerCycleBudget>,
        updates: mpsc::Sender<DataUsageInfo>,
        want_cycle: u64,
        scan_mode: HealScanMode,
    ) -> Result<()> {
        let child_token = ctx.child_token();

        let all_buckets = self.list_bucket(&BucketOptions::default()).await?;

        if all_buckets.is_empty() {
            reset_set_scan_gauges();
            if let Err(e) = updates.send(DataUsageInfo::default()).await {
                error!(
                    target: "rustfs::scanner::io",
                    event = EVENT_SCANNER_SET_STATE,
                    component = LOG_COMPONENT_SCANNER,
                    subsystem = LOG_SUBSYSTEM_IO,
                    state = "empty_bucket_publish_failed",
                    error = %e,
                    "Scanner set state update failed"
                );
            }
            return Ok(());
        }

        let mut total_results = 0;
        for pool in self.pools.iter() {
            total_results += pool.disk_set.len();
        }
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
            return Ok(());
        }

        let set_scan_limit = scanner_max_concurrent_set_scans(total_results);
        let dirty_usage_buckets = Arc::new(snapshot_dirty_usage_buckets(&all_buckets));
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

                let scan_plan = ScannerBucketScanPlan::new(all_buckets.clone(), dirty_usage_buckets.clone());
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

        let (update_tx, mut update_rx) = tokio::sync::oneshot::channel::<()>();

        let all_buckets_clone = all_buckets.iter().map(|b| b.name.clone()).collect::<Vec<String>>();
        let results_mutex_for_updates = results_mutex.clone();
        let budget_for_updates = budget.clone();
        let child_token_for_updates = child_token.clone();
        let dirty_usage_buckets_for_updates = dirty_usage_buckets.clone();
        tokio::spawn(async move {
            let mut last_update = SystemTime::UNIX_EPOCH;
            let mut has_sent_once = false;

            let mut ticker = tokio::time::interval(Duration::from_secs(30));
            loop {
                tokio::select! {
                    _ = child_token_for_updates.cancelled() => {
                        break;
                    }
                    res = &mut update_rx => {
                        if res.is_err() {
                            break;
                        }

                        let data_usage_update = {
                            let results = results_mutex_for_updates.lock().await;
                            completed_data_usage_info(
                                &results,
                                &all_buckets_clone,
                                budget_for_updates.budget_elapsed(),
                                child_token_for_updates.is_cancelled(),
                                dirty_usage_snapshot_covers_current(dirty_usage_buckets_for_updates.as_ref()),
                            )
                        };

                        if let Some((data_usage_info, merged_last_update)) = data_usage_update
                            && (!has_sent_once || merged_last_update > last_update)
                        {
                            send_merged_data_usage_update(&updates, data_usage_info).await;
                        }
                        break;
                    }
                    _ = ticker.tick() => {
                        let data_usage_update = {
                            let results = results_mutex_for_updates.lock().await;
                            completed_data_usage_info(
                                &results,
                                &all_buckets_clone,
                                budget_for_updates.budget_elapsed(),
                                child_token_for_updates.is_cancelled(),
                                dirty_usage_snapshot_covers_current(dirty_usage_buckets_for_updates.as_ref()),
                            )
                        };

                        if let Some((data_usage_info, merged_last_update)) = data_usage_update
                            && (!has_sent_once || merged_last_update > last_update)
                        {
                            send_merged_data_usage_update(&updates, data_usage_info).await;
                            has_sent_once = true;
                            last_update = merged_last_update;
                        }
                    }
                }
            }
        });

        let _ = join_all(wait_futs).await;
        record_set_scan_concurrency_limit(0);
        record_set_scans_queued(0);
        record_set_scans_active(0);

        let _ = update_tx.send(());

        let first_err = first_err_mutex.lock().await.take();
        let results = results_mutex.lock().await.clone();
        let completed_all_sets = results.iter().all(|result| result.info.last_update.is_some());
        let result = finalize_nsscanner_result(&results, first_err);
        if result.is_ok() && completed_all_sets && !budget.budget_elapsed() {
            clear_dirty_usage_buckets(&dirty_usage_buckets);
        }
        result
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
            dirty_usage_buckets,
        } = scan_plan;
        let pool_label = self.pool_index.to_string();
        let set_label = self.set_index.to_string();

        if buckets.is_empty() {
            reset_disk_bucket_scan_gauges(&pool_label, &set_label);
            return Ok(());
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
        let disk_scan_limit = scanner_max_concurrent_disk_scans(disks.len());
        record_disk_scan_concurrency_limit(&pool_label, &set_label, disk_scan_limit);
        debug!(
            target: "rustfs::scanner::io",
            event = EVENT_SCANNER_SET_STATE,
            component = LOG_COMPONENT_SCANNER,
            subsystem = LOG_SUBSYSTEM_IO,
            pool = self.pool_index,
            set = self.set_index,
            online_disks = disks.len(),
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
        old_cache.load(self.clone(), DATA_USAGE_CACHE_NAME).await?;

        let mut cache = DataUsageCache {
            info: DataUsageCacheInfo {
                name: DATA_USAGE_ROOT.to_string(),
                next_cycle: old_cache.info.next_cycle,
                ..Default::default()
            },
            cache: HashMap::new(),
        };

        let (bucket_tx, bucket_rx) = mpsc::channel::<BucketInfo>(buckets.len());

        let mut permutes = buckets.clone();
        permutes.shuffle(&mut rand::rng());
        let scan_order = bucket_usage_scan_order(&permutes, &old_cache, &dirty_usage_buckets);

        for bucket in scan_order.iter() {
            if let Some(c) = old_cache.find(&bucket.name) {
                cache.replace(&bucket.name, DATA_USAGE_ROOT, c.clone());
            }

            if let Err(e) = bucket_tx.send(bucket.clone()).await {
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

        drop(bucket_tx);

        let cache_mutex: Arc<Mutex<DataUsageCache>> = Arc::new(Mutex::new(cache));

        let (bucket_result_tx, mut bucket_result_rx) = mpsc::channel::<DataUsageEntryInfo>(disks.len());

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
        let bucket_result_tx_clone: Arc<Mutex<mpsc::Sender<DataUsageEntryInfo>>> = Arc::new(Mutex::new(bucket_result_tx));
        for disk in disks.into_iter() {
            let bucket_rx_mutex_clone = bucket_rx_mutex.clone();
            let ctx_clone = ctx.clone();
            let budget_clone = budget.clone();
            let store_clone_clone = self.clone();
            let bucket_result_tx_clone_clone = bucket_result_tx_clone.clone();
            let disk_clone = disk.clone();
            let disk_scan_semaphore_clone = disk_scan_semaphore.clone();
            let queued_disk_bucket_scans_clone = queued_disk_bucket_scans.clone();
            let active_disk_bucket_scans_clone = active_disk_bucket_scans.clone();
            let pool_label_clone = pool_label.clone();
            let set_label_clone = set_label.clone();
            futs.push(tokio::spawn(async move {
                loop {
                    let Some(bucket) = bucket_rx_mutex_clone.lock().await.recv().await else {
                        break;
                    };

                    if ctx_clone.is_cancelled() {
                        decrement_disk_bucket_scans_queued(&queued_disk_bucket_scans_clone, &pool_label_clone, &set_label_clone);
                        break;
                    }

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

                    let mut cache = DataUsageCache::default();
                    if let Err(e) = cache.load(store_clone_clone.clone(), &cache_name).await {
                        error!(
                            target: "rustfs::scanner::io",
                            event = EVENT_SCANNER_DISK_BUCKET_STATE,
                                component = LOG_COMPONENT_SCANNER,
                                subsystem = LOG_SUBSYSTEM_IO,
                                bucket = %bucket.name,
                                cache_name = %cache_name,
                                state = "cache_load_failed",
                                error = %e,
                                "Scanner disk bucket cache load failed"
                        );
                    }

                    if cache.info.name.is_empty() {
                        cache.info.name = bucket.name.clone();
                    }

                    cache.info.skip_healing = healing;
                    cache.info.next_cycle = want_cycle;
                    if cache.info.name != bucket.name {
                        cache.info = DataUsageCacheInfo {
                            name: bucket.name.clone(),
                            next_cycle: want_cycle,
                            ..Default::default()
                        };
                    }

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

                    let scan_outcome = match disk_clone
                        .nsscanner_disk(ctx_clone.clone(), budget_clone.clone(), cache.clone(), None, scan_mode)
                        .await
                    {
                        Ok(scan_outcome) => scan_outcome,
                        Err(e) => {
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

                            if let (Some(last_update), Some(before_update)) = (cache.info.last_update, before)
                                && last_update > before_update
                            {
                                let done_save = Metrics::time(Metric::SaveUsage);
                                if let Err(e) = cache.save(store_clone_clone.clone(), cache_name.as_str()).await {
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

                    cache = match scan_outcome {
                        ScannerDiskScanOutcome::Complete(cache) => cache,
                        ScannerDiskScanOutcome::Partial(cache) => {
                            let done_save = Metrics::time(Metric::SaveUsage);
                            let partial_saved = match cache.save(store_clone_clone.clone(), cache_name.as_str()).await {
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
                            };
                            done_save();
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
                    };

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

                    if let Err(e) = send_cache_root_entry_info(&bucket_result_tx_clone_clone, &cache).await {
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

                    let done_save = Metrics::time(Metric::SaveUsage);
                    if let Err(e) = cache.save(store_clone_clone.clone(), &cache_name).await {
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
            }));
        }

        let _ = join_all(futs).await;
        record_disk_scan_concurrency_limit(&pool_label, &set_label, 0);
        record_disk_bucket_scans_queued(0, &pool_label, &set_label);
        record_disk_bucket_scans_active(0, &pool_label, &set_label);

        drop(bucket_result_tx_clone);

        collect_bucket_results_fut.await?;

        let completed_count = completed_bucket_count.load(Ordering::Relaxed);
        if should_publish_completed_snapshot(completed_count, buckets.len(), budget.budget_elapsed(), ctx.is_cancelled()) {
            let cache_snapshot = {
                let mut cache = cache_mutex.lock().await;
                cache.info.next_cycle = want_cycle;
                cache.info.last_update.get_or_insert_with(SystemTime::now);
                cache.clone()
            };
            let _ = persist_and_publish_cache_snapshot(self.clone(), &updates, cache_snapshot).await;
        } else {
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
                return Err(StorageError::other(format!(
                    "failed to read metadata: {e}, bucket={}, object_path={}",
                    &item.bucket,
                    &item.object_path()
                )));
            }
        };

        item.transform_meta_dir();

        let meta = FileMeta::load(&data)?;
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
                return Err(StorageError::other(SCANNER_SKIP_FILE_ERROR.to_string()));
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

        let lock_config = match get_object_lock_config(&item.bucket).await {
            Ok((cfg, _)) => Some(Arc::new(cfg)),
            Err(_) => None,
        };

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
        &self,
        ctx: CancellationToken,
        budget: Arc<ScannerCycleBudget>,
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
            cache.info.replication = Some(Arc::new(ReplicationConfig {
                config: Some(replication_config),
                remotes: Some(targets),
            }));
        }

        // TODO: object lock

        let Some(ecstore) = resolve_scanner_object_store_handle() else {
            error!(
                target: "rustfs::scanner::io",
                event = EVENT_SCANNER_DISK_BUCKET_STATE,
                component = LOG_COMPONENT_SCANNER,
                subsystem = LOG_SUBSYSTEM_IO,
                bucket = %bucket,
                state = "ecstore_unavailable",
                "Scanner disk bucket missing object layer"
            );
            return Err(StorageError::other("ECStore not available".to_string()));
        };

        let disk_location = self.get_disk_location();

        let (Some(pool_idx), Some(set_idx)) = (disk_location.pool_idx, disk_location.set_idx) else {
            error!(
                target: "rustfs::scanner::io",
                event = EVENT_SCANNER_DISK_BUCKET_STATE,
                component = LOG_COMPONENT_SCANNER,
                subsystem = LOG_SUBSYSTEM_IO,
                bucket = %bucket,
                state = "disk_location_unavailable",
                "Scanner disk bucket missing disk location"
            );
            return Err(StorageError::other("Disk location not available".to_string()));
        };

        let disks_result = StorageAdminApi::disk_set_inventory(ecstore.as_ref(), DiskSetSelector::new(pool_idx, set_idx)).await?;

        let Some(disk_idx) = disk_location.disk_idx else {
            error!(
                target: "rustfs::scanner::io",
                event = EVENT_SCANNER_DISK_BUCKET_STATE,
                component = LOG_COMPONENT_SCANNER,
                subsystem = LOG_SUBSYSTEM_IO,
                bucket = %bucket,
                state = "disk_index_unavailable",
                "Scanner disk bucket missing disk index"
            );
            return Err(StorageError::other("Disk index not available".to_string()));
        };

        let local_disk = if let Some(Some(local_disk)) = disks_result.get(disk_idx) {
            local_disk.clone()
        } else {
            error!(
                target: "rustfs::scanner::io",
                event = EVENT_SCANNER_DISK_BUCKET_STATE,
                component = LOG_COMPONENT_SCANNER,
                subsystem = LOG_SUBSYSTEM_IO,
                bucket = %bucket,
                state = "local_disk_unavailable",
                "Scanner disk bucket missing local disk"
            );
            return Err(StorageError::other("Local disk not available".to_string()));
        };

        let disks = disks_result.into_iter().flatten().collect::<Vec<Arc<Disk>>>();

        let result =
            scan_data_folder(ctx.clone(), budget, disks, local_disk, cache, updates, scan_mode, SCANNER_SLEEPER.clone()).await;

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
    use crate::scanner_folder::ScannerItem;
    use crate::{DiskOption, Endpoint, new_disk, path2_bucket_object_with_base_path};
    use serial_test::serial;
    use temp_env::with_var;
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

    #[test]
    #[serial]
    fn dirty_usage_snapshot_clear_preserves_newer_generation() {
        clear_dirty_usage_buckets_for_tests();
        record_dirty_usage_bucket("photos");
        let buckets = vec![bucket_info("photos")];
        let snapshot = snapshot_dirty_usage_buckets(&buckets);

        record_dirty_usage_bucket("photos");
        clear_dirty_usage_buckets(&snapshot);

        assert_eq!(dirty_usage_bucket_count(), 1);
        clear_dirty_usage_buckets_for_tests();
    }

    #[test]
    #[serial]
    fn dirty_usage_snapshot_detects_uncovered_generation() {
        clear_dirty_usage_buckets_for_tests();
        record_dirty_usage_bucket("photos");
        let buckets = vec![bucket_info("photos")];
        let snapshot = snapshot_dirty_usage_buckets(&buckets);

        assert!(dirty_usage_snapshot_covers_current(&snapshot));

        record_dirty_usage_bucket("photos");

        assert!(!dirty_usage_snapshot_covers_current(&snapshot));
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
        let snapshot = snapshot_dirty_usage_buckets(&buckets);
        assert!(!snapshot.contains_key("photos"));
        assert!(snapshot.contains_key("videos"));
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
    fn scanner_concurrency_limit_preserves_available_when_unconfigured() {
        assert_eq!(scanner_concurrency_limit(0, 4), 4);
    }

    #[test]
    fn scanner_concurrency_limit_caps_to_configured_value() {
        assert_eq!(scanner_concurrency_limit(2, 4), 2);
    }

    #[test]
    fn scanner_concurrency_limit_never_exceeds_available_work() {
        assert_eq!(scanner_concurrency_limit(8, 4), 4);
    }

    #[test]
    fn scanner_concurrency_limit_handles_no_available_work() {
        assert_eq!(scanner_concurrency_limit(2, 0), 0);
    }

    #[test]
    fn decrement_atomic_usize_saturates_at_zero() {
        let counter = AtomicUsize::new(1);
        assert_eq!(decrement_atomic_usize(&counter), 0);
        assert_eq!(decrement_atomic_usize(&counter), 0);
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

        let info = cache_root_entry_info(&cache);

        assert_eq!(info.name, "bucket");
        assert_eq!(info.parent, DATA_USAGE_ROOT);
        assert_eq!(info.entry.size, 30);
        assert_eq!(info.entry.objects, 3);
        assert!(info.entry.children.is_empty());
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
