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

use crate::scanner_folder::{ScannerItem, scan_data_folder};
use crate::sleeper::SCANNER_SLEEPER;
use crate::{
    DATA_USAGE_CACHE_NAME, DATA_USAGE_ROOT, DataUsageCache, DataUsageCacheInfo, DataUsageEntry, DataUsageEntryInfo,
    DataUsageInfo, SizeSummary, TierStats,
};
use futures::future::join_all;
use metrics::counter;
use rand::seq::SliceRandom as _;
use rustfs_common::heal_channel::HealScanMode;
use rustfs_common::metrics::{Metric, Metrics, emit_scan_bucket_drive_complete};
use rustfs_config::{
    DEFAULT_SCANNER_MAX_CONCURRENT_DISK_SCANS, DEFAULT_SCANNER_MAX_CONCURRENT_SET_SCANS, ENV_SCANNER_MAX_CONCURRENT_DISK_SCANS,
    ENV_SCANNER_MAX_CONCURRENT_SET_SCANS,
};
use rustfs_ecstore::bucket::bucket_target_sys::BucketTargetSys;
use rustfs_ecstore::bucket::lifecycle::bucket_lifecycle_ops::GLOBAL_ExpiryState;
use rustfs_ecstore::bucket::lifecycle::lifecycle::Lifecycle;
use rustfs_ecstore::bucket::metadata_sys::{get_lifecycle_config, get_object_lock_config, get_replication_config};
use rustfs_ecstore::bucket::replication::{ReplicationConfig, ReplicationConfigurationExt};
use rustfs_ecstore::bucket::versioning::VersioningApi as _;
use rustfs_ecstore::bucket::versioning_sys::BucketVersioningSys;
use rustfs_ecstore::config::storageclass;
use rustfs_ecstore::disk::STORAGE_FORMAT_FILE;
use rustfs_ecstore::disk::{Disk, DiskAPI};
use rustfs_ecstore::error::{Error, StorageError};
use rustfs_ecstore::global::GLOBAL_TierConfigMgr;
use rustfs_ecstore::new_object_layer_fn;
use rustfs_ecstore::set_disk::SetDisks;
use rustfs_ecstore::store_api::{BucketInfo, BucketOperations, BucketOptions, ObjectInfo};
use rustfs_ecstore::{StorageAPI, error::Result, store::ECStore};
use rustfs_filemeta::FileMeta;
use rustfs_utils::path::path_join_buf;
use s3s::dto::{BucketLifecycleConfiguration, ReplicationConfiguration};
use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Instant, SystemTime};
use std::{fmt::Debug, sync::Arc};
use time::OffsetDateTime;
use tokio::sync::{Mutex, Semaphore, mpsc};
use tokio::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, warn};

const METRIC_SCANNER_SET_SCAN_CONCURRENCY_LIMIT: &str = "rustfs_scanner_set_scan_concurrency_limit";
const METRIC_SCANNER_DISK_SCAN_CONCURRENCY_LIMIT: &str = "rustfs_scanner_disk_scan_concurrency_limit";
const METRIC_SCANNER_SET_SCAN_WAIT_SECONDS: &str = "rustfs_scanner_set_scan_wait_seconds";
const METRIC_SCANNER_DISK_SCAN_WAIT_SECONDS: &str = "rustfs_scanner_disk_scan_wait_seconds";
const METRIC_SCANNER_SET_SCANS_ACTIVE: &str = "rustfs_scanner_set_scans_active";
const METRIC_SCANNER_SET_SCANS_QUEUED: &str = "rustfs_scanner_set_scans_queued";
const METRIC_SCANNER_DISK_BUCKET_SCANS_ACTIVE: &str = "rustfs_scanner_disk_bucket_scans_active";
const METRIC_SCANNER_DISK_BUCKET_SCANS_QUEUED: &str = "rustfs_scanner_disk_bucket_scans_queued";

struct SetScanActiveGuard {
    active: Arc<AtomicUsize>,
}

impl SetScanActiveGuard {
    fn new(active: Arc<AtomicUsize>) -> Self {
        let active_count = active.fetch_add(1, Ordering::Relaxed) + 1;
        metrics::gauge!(METRIC_SCANNER_SET_SCANS_ACTIVE).set(active_count as f64);
        Self { active }
    }
}

impl Drop for SetScanActiveGuard {
    fn drop(&mut self) {
        let active_count = decrement_atomic_usize(&self.active);
        metrics::gauge!(METRIC_SCANNER_SET_SCANS_ACTIVE).set(active_count as f64);
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
        metrics::gauge!(
            METRIC_SCANNER_DISK_BUCKET_SCANS_ACTIVE,
            "pool" => pool.clone(),
            "set" => set.clone()
        )
        .set(active_count as f64);
        Self { active, pool, set }
    }
}

impl Drop for DiskBucketScanActiveGuard {
    fn drop(&mut self) {
        let active_count = decrement_atomic_usize(&self.active);
        metrics::gauge!(
            METRIC_SCANNER_DISK_BUCKET_SCANS_ACTIVE,
            "pool" => self.pool.clone(),
            "set" => self.set.clone()
        )
        .set(active_count as f64);
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
}

fn decrement_disk_bucket_scans_queued(counter: &AtomicUsize, pool: &str, set: &str) {
    let queued_count = decrement_atomic_usize(counter);
    record_disk_bucket_scans_queued(queued_count, pool, set);
}

fn reset_set_scan_gauges() {
    metrics::gauge!(METRIC_SCANNER_SET_SCAN_CONCURRENCY_LIMIT).set(0.0);
    metrics::gauge!(METRIC_SCANNER_SET_SCANS_QUEUED).set(0.0);
    metrics::gauge!(METRIC_SCANNER_SET_SCANS_ACTIVE).set(0.0);
}

fn reset_disk_bucket_scan_gauges(pool: &str, set: &str) {
    metrics::gauge!(
        METRIC_SCANNER_DISK_SCAN_CONCURRENCY_LIMIT,
        "pool" => pool.to_owned(),
        "set" => set.to_owned()
    )
    .set(0.0);
    record_disk_bucket_scans_queued(0, pool, set);
    metrics::gauge!(
        METRIC_SCANNER_DISK_BUCKET_SCANS_ACTIVE,
        "pool" => pool.to_owned(),
        "set" => set.to_owned()
    )
    .set(0.0);
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
    scanner_concurrency_limit(
        rustfs_utils::get_env_usize(ENV_SCANNER_MAX_CONCURRENT_SET_SCANS, DEFAULT_SCANNER_MAX_CONCURRENT_SET_SCANS),
        available,
    )
}

fn scanner_max_concurrent_disk_scans(available: usize) -> usize {
    scanner_concurrency_limit(
        rustfs_utils::get_env_usize(ENV_SCANNER_MAX_CONCURRENT_DISK_SCANS, DEFAULT_SCANNER_MAX_CONCURRENT_DISK_SCANS),
        available,
    )
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

async fn persist_and_publish_cache_snapshot<S: StorageAPI>(
    store: Arc<S>,
    updates: &mpsc::Sender<DataUsageCache>,
    cache_snapshot: DataUsageCache,
) -> Option<SystemTime> {
    let last_update = cache_snapshot.info.last_update;

    if let Err(e) = cache_snapshot.save(store, DATA_USAGE_CACHE_NAME).await {
        error!("Failed to save data usage cache: {}", e);
    }

    if let Err(e) = updates.send(cache_snapshot).await {
        error!("Failed to send data usage cache: {}", e);
    }

    last_update
}

#[async_trait::async_trait]
pub trait ScannerIO: Send + Sync + Debug + 'static {
    async fn nsscanner(
        &self,
        ctx: CancellationToken,
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
        buckets: Vec<BucketInfo>,
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
        cache: DataUsageCache,
        updates: Option<mpsc::Sender<DataUsageEntry>>,
        scan_mode: HealScanMode,
    ) -> Result<DataUsageCache>;

    async fn get_size(&self, item: ScannerItem) -> Result<SizeSummary>;
}

#[async_trait::async_trait]
impl ScannerIO for ECStore {
    #[tracing::instrument(skip(self, updates))]
    async fn nsscanner(
        &self,
        ctx: CancellationToken,
        updates: mpsc::Sender<DataUsageInfo>,
        want_cycle: u64,
        scan_mode: HealScanMode,
    ) -> Result<()> {
        let child_token = ctx.child_token();

        let all_buckets = self.list_bucket(&BucketOptions::default()).await?;

        if all_buckets.is_empty() {
            reset_set_scan_gauges();
            if let Err(e) = updates.send(DataUsageInfo::default()).await {
                error!("Failed to send data usage info: {}", e);
            }
            return Ok(());
        }

        let mut total_results = 0;
        for pool in self.pools.iter() {
            total_results += pool.disk_set.len();
        }
        if total_results == 0 {
            warn!("nsscanner: no disk sets available for non-empty bucket list");
            reset_set_scan_gauges();
            return Ok(());
        }

        let set_scan_limit = scanner_max_concurrent_set_scans(total_results);
        metrics::gauge!(METRIC_SCANNER_SET_SCAN_CONCURRENCY_LIMIT).set(set_scan_limit as f64);
        debug!(
            total_sets = total_results,
            concurrency_limit = set_scan_limit,
            "nsscanner: scanner set concurrency budget"
        );
        let set_scan_semaphore = Arc::new(Semaphore::new(set_scan_limit));
        let queued_set_scans = Arc::new(AtomicUsize::new(total_results));
        let active_set_scans = Arc::new(AtomicUsize::new(0));
        metrics::gauge!(METRIC_SCANNER_SET_SCANS_QUEUED).set(total_results as f64);
        metrics::gauge!(METRIC_SCANNER_SET_SCANS_ACTIVE).set(0.0);

        let results = vec![DataUsageCache::default(); total_results];
        let results_mutex: Arc<Mutex<Vec<DataUsageCache>>> = Arc::new(Mutex::new(results));
        let first_err_mutex: Arc<Mutex<Option<Error>>> = Arc::new(Mutex::new(None));
        let mut results_index: i32 = -1_i32;
        let mut wait_futs = Vec::new();

        for pool in self.pools.iter() {
            for set in pool.disk_set.iter() {
                results_index += 1;

                let results_index_clone = results_index as usize;
                // Clone the Arc to move it into the spawned task
                let set_clone: Arc<SetDisks> = Arc::clone(set);
                let pool_label = set.pool_index.to_string();
                let set_label = set.set_index.to_string();

                let child_token_clone = child_token.clone();
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

                let all_buckets_clone = all_buckets.clone();
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
                    metrics::gauge!(METRIC_SCANNER_SET_SCANS_QUEUED).set(queued_count as f64);
                    let _active_guard = SetScanActiveGuard::new(active_set_scans_clone);

                    if let Err(e) = set_clone
                        .nsscanner_cache(child_token_clone.clone(), all_buckets_clone, tx, want_cycle_clone, scan_mode_clone)
                        .await
                    {
                        counter!(
                            "rustfs_scanner_set_failure_total",
                            "pool" => pool_label.clone(),
                            "set" => set_label.clone(),
                            "stage" => "nsscanner_cache".to_string()
                        )
                        .increment(1);
                        error!(
                            pool = %pool_label,
                            set = %set_label,
                            error = %e,
                            "Failed to scan set; continuing scanner cycle"
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
        tokio::spawn(async move {
            let mut last_update = SystemTime::UNIX_EPOCH;
            let mut has_sent_once = false;

            let mut ticker = tokio::time::interval(Duration::from_secs(30));
            loop {
                tokio::select! {
                    _ = child_token.cancelled() => {
                        break;
                    }
                    res = &mut update_rx => {
                        if res.is_err() {
                            break;
                        }

                        let results = results_mutex_for_updates.lock().await;
                        let mut all_merged = DataUsageCache::default();
                        for result in results.iter() {
                            if result.info.last_update.is_none() {
                                continue;
                            }
                            all_merged.merge(result);
                        }

                        let merged_last_update = all_merged.info.last_update.unwrap_or(SystemTime::UNIX_EPOCH);
                        if all_merged.root().is_some() && (!has_sent_once || merged_last_update > last_update) {
                            let dui = all_merged.dui(&all_merged.info.name, &all_buckets_clone);
                            if let Err(e) = updates.send(dui).await {
                                error!("Failed to send data usage info: {}", e);
                            }
                        }
                        break;
                    }
                    _ = ticker.tick() => {
                        let results = results_mutex_for_updates.lock().await;
                        let mut all_merged = DataUsageCache::default();
                        for result in results.iter() {
                            if result.info.last_update.is_none() {
                                continue;
                            }
                            all_merged.merge(result);
                        }

                        let merged_last_update = all_merged.info.last_update.unwrap_or(SystemTime::UNIX_EPOCH);
                        if all_merged.root().is_some() && (!has_sent_once || merged_last_update > last_update) {
                            let dui = all_merged.dui(&all_merged.info.name, &all_buckets_clone);
                            if let Err(e) = updates.send(dui).await {
                                error!("Failed to send data usage info: {}", e);
                            }
                            has_sent_once = true;
                            last_update = merged_last_update;
                        }
                    }
                }
            }
        });

        let _ = join_all(wait_futs).await;
        metrics::gauge!(METRIC_SCANNER_SET_SCANS_QUEUED).set(0.0);
        metrics::gauge!(METRIC_SCANNER_SET_SCANS_ACTIVE).set(0.0);

        let _ = update_tx.send(());

        let first_err = first_err_mutex.lock().await.take();
        let results = results_mutex.lock().await.clone();
        finalize_nsscanner_result(&results, first_err)
    }
}

#[async_trait::async_trait]
impl ScannerIOCache for SetDisks {
    #[tracing::instrument(skip(self, updates))]
    async fn nsscanner_cache(
        self: Arc<Self>,
        ctx: CancellationToken,
        buckets: Vec<BucketInfo>,
        updates: mpsc::Sender<DataUsageCache>,
        want_cycle: u64,
        scan_mode: HealScanMode,
    ) -> Result<()> {
        let pool_label = self.pool_index.to_string();
        let set_label = self.set_index.to_string();

        if buckets.is_empty() {
            reset_disk_bucket_scan_gauges(&pool_label, &set_label);
            return Ok(());
        }

        let (disks, healing) = self.get_online_disks_with_healing(false).await;
        if disks.is_empty() {
            debug!("nsscanner_cache: no online disks available for set");
            reset_disk_bucket_scan_gauges(&pool_label, &set_label);
            return Ok(());
        }
        let disk_scan_limit = scanner_max_concurrent_disk_scans(disks.len());
        metrics::gauge!(
            METRIC_SCANNER_DISK_SCAN_CONCURRENCY_LIMIT,
            "pool" => self.pool_index.to_string(),
            "set" => self.set_index.to_string()
        )
        .set(disk_scan_limit as f64);
        debug!(
            pool = self.pool_index,
            set = self.set_index,
            online_disks = disks.len(),
            concurrency_limit = disk_scan_limit,
            "nsscanner_cache: scanner disk concurrency budget"
        );
        let disk_scan_semaphore = Arc::new(Semaphore::new(disk_scan_limit));
        let queued_disk_bucket_scans = Arc::new(AtomicUsize::new(buckets.len()));
        let active_disk_bucket_scans = Arc::new(AtomicUsize::new(0));
        record_disk_bucket_scans_queued(buckets.len(), &pool_label, &set_label);
        metrics::gauge!(
            METRIC_SCANNER_DISK_BUCKET_SCANS_ACTIVE,
            "pool" => pool_label.clone(),
            "set" => set_label.clone()
        )
        .set(0.0);

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

        for bucket in permutes.iter() {
            if old_cache.find(&bucket.name).is_none()
                && let Err(e) = bucket_tx.send(bucket.clone()).await
            {
                error!("Failed to send bucket info: {}", e);
            }
        }

        for bucket in permutes.iter() {
            if let Some(c) = old_cache.find(&bucket.name) {
                cache.replace(&bucket.name, DATA_USAGE_ROOT, c.clone());

                if let Err(e) = bucket_tx.send(bucket.clone()).await {
                    error!("Failed to send bucket info: {}", e);
                }
            }
        }

        drop(bucket_tx);

        let cache_mutex: Arc<Mutex<DataUsageCache>> = Arc::new(Mutex::new(cache));

        let (bucket_result_tx, mut bucket_result_rx) = mpsc::channel::<DataUsageEntryInfo>(disks.len());

        let cache_mutex_clone = cache_mutex.clone();
        let store_clone = self.clone();
        let ctx_clone = ctx.clone();
        let send_update_fut = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_secs(3 + rand::random::<u64>() % 10));

            let mut last_update = None;

            loop {
                tokio::select! {
                    _ = ctx_clone.cancelled() => {
                        break;
                    }
                    _ = ticker.tick() => {
                        let cache_snapshot = {
                            let cache = cache_mutex_clone.lock().await;
                            if cache.info.last_update == last_update {
                                None
                            } else {
                                Some(cache.clone())
                            }
                        };

                        let Some(cache_snapshot) = cache_snapshot else {
                            continue;
                        };
                        last_update =
                            persist_and_publish_cache_snapshot(store_clone.clone(), &updates, cache_snapshot).await;
                    }
                    res =  bucket_result_rx.recv() => {
                        if let Some(result) = res {
                            let mut cache = cache_mutex_clone.lock().await;
                            cache.replace(&result.name, &result.parent, result.entry);
                            cache.info.last_update = Some(SystemTime::now());

                        } else {
                            let cache_snapshot = {
                                let mut cache = cache_mutex_clone.lock().await;
                                cache.info.next_cycle = want_cycle;
                                cache.info.last_update = Some(SystemTime::now());
                                cache.clone()
                            };
                            let _ = persist_and_publish_cache_snapshot(store_clone.clone(), &updates, cache_snapshot).await;

                            return;
                        }
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

                    debug!("nsscanner_disk: got bucket: {}", bucket.name);

                    let cache_name = path_join_buf(&[&bucket.name, DATA_USAGE_CACHE_NAME]);

                    let mut cache = DataUsageCache::default();
                    if let Err(e) = cache.load(store_clone_clone.clone(), &cache_name).await {
                        error!("Failed to load data usage cache: {}", e);
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

                    debug!("nsscanner_disk: cache.info.name: {:?}", cache.info.name);

                    let (updates_tx, mut updates_rx) = mpsc::channel::<DataUsageEntry>(1);

                    let ctx_clone_clone = ctx_clone.clone();
                    let bucket_name_clone = bucket.name.clone();
                    let bucket_result_tx_clone_clone_clone = bucket_result_tx_clone_clone.clone();
                    let update_fut = tokio::spawn(async move {
                        while let Some(result) = updates_rx.recv().await {
                            if ctx_clone_clone.is_cancelled() {
                                break;
                            }

                            if let Err(e) = bucket_result_tx_clone_clone_clone
                                .lock()
                                .await
                                .send(DataUsageEntryInfo {
                                    name: bucket_name_clone.clone(),
                                    parent: DATA_USAGE_ROOT.to_string(),
                                    entry: result,
                                })
                                .await
                            {
                                error!("Failed to send data usage entry info: {}", e);
                            }
                        }
                    });

                    let before = cache.info.last_update;

                    cache = match disk_clone
                        .nsscanner_disk(ctx_clone.clone(), cache.clone(), Some(updates_tx), scan_mode)
                        .await
                    {
                        Ok(cache) => cache,
                        Err(e) => {
                            error!("Failed to scan disk: {}", e);

                            if let (Some(last_update), Some(before_update)) = (cache.info.last_update, before)
                                && last_update > before_update
                                && let Err(e) = cache.save(store_clone_clone.clone(), cache_name.as_str()).await
                            {
                                error!("Failed to save data usage cache: {}", e);
                            }

                            if let Err(e) = update_fut.await {
                                error!("Failed to update data usage cache: {}", e);
                            }
                            continue;
                        }
                    };

                    debug!("nsscanner_disk: got cache: {}", cache.info.name);

                    if let Err(e) = update_fut.await {
                        error!("nsscanner_disk: Failed to update data usage cache: {}", e);
                    }

                    let root = if let Some(r) = cache.root() {
                        cache.flatten(&r)
                    } else {
                        DataUsageEntry::default()
                    };

                    if ctx_clone.is_cancelled() {
                        break;
                    }

                    debug!("nsscanner_disk: sending data usage entry info: {}", cache.info.name);

                    if let Err(e) = bucket_result_tx_clone_clone
                        .lock()
                        .await
                        .send(DataUsageEntryInfo {
                            name: cache.info.name.clone(),
                            parent: DATA_USAGE_ROOT.to_string(),
                            entry: root,
                        })
                        .await
                    {
                        error!("nsscanner_disk: Failed to send data usage entry info: {}", e);
                    }

                    if let Err(e) = cache.save(store_clone_clone.clone(), &cache_name).await {
                        error!("nsscanner_disk: Failed to save data usage cache: {}", e);
                    }
                }
            }));
        }

        let _ = join_all(futs).await;
        record_disk_bucket_scans_queued(0, &pool_label, &set_label);
        metrics::gauge!(
            METRIC_SCANNER_DISK_BUCKET_SCANS_ACTIVE,
            "pool" => pool_label.clone(),
            "set" => set_label.clone()
        )
        .set(0.0);

        drop(bucket_result_tx_clone);

        send_update_fut.await?;

        debug!("nsscanner_cache: done");

        Ok(())
    }
}

#[async_trait::async_trait]
impl ScannerIODisk for Disk {
    async fn get_size(&self, mut item: ScannerItem) -> Result<SizeSummary> {
        let done_object = Metrics::time(Metric::ScanObject);

        if !is_xl_meta_path(&item.path) {
            return Err(StorageError::other("skip file".to_string()));
        }

        let data = match self.read_metadata(&item.bucket, &item.object_path()).await {
            Ok(data) => data,
            Err(e) => {
                warn!(
                    "Failed to read metadata: {e}, bucket={}, object_path={}",
                    &item.bucket,
                    &item.object_path()
                );

                return Err(StorageError::other("failed to read metadata".to_string()));
            }
        };

        item.transform_meta_dir();

        let meta = FileMeta::load(&data)?;
        let fivs = match meta.get_file_info_versions(item.bucket.as_str(), item.object_path().as_str(), false) {
            Ok(versions) => versions,
            Err(e) => {
                error!("Failed to get file info versions: {}/{}, err: {e}", item.bucket, item.object_path());
                return Err(StorageError::other("skip file".to_string()));
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

        let tiers = {
            let tier_config_mgr = GLOBAL_TierConfigMgr.read().await;
            tier_config_mgr.list_tiers()
        };

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
            let mut expiry_state = GLOBAL_ExpiryState.write().await;
            for oi in free_version_infos {
                expiry_state.enqueue_free_version(oi).await;
            }
        }

        done_object();

        Ok(size_summary)
    }

    #[tracing::instrument(skip(self, updates, cache))]
    async fn nsscanner_disk(
        &self,
        ctx: CancellationToken,
        cache: DataUsageCache,
        updates: Option<mpsc::Sender<DataUsageEntry>>,
        scan_mode: HealScanMode,
    ) -> Result<DataUsageCache> {
        let done_drive = Metrics::time(Metric::ScanBucketDrive);
        let drive_start = std::time::Instant::now();
        let bucket = cache.info.name.clone();
        let disk_path = self.path().to_string_lossy().to_string();
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

        let Some(ecstore) = new_object_layer_fn() else {
            error!("ECStore not available");
            return Err(StorageError::other("ECStore not available".to_string()));
        };

        let disk_location = self.get_disk_location();

        let (Some(pool_idx), Some(set_idx)) = (disk_location.pool_idx, disk_location.set_idx) else {
            error!("Disk location not available");
            return Err(StorageError::other("Disk location not available".to_string()));
        };

        let disks_result = ecstore.get_disks(pool_idx, set_idx).await?;

        let Some(disk_idx) = disk_location.disk_idx else {
            error!("Disk index not available");
            return Err(StorageError::other("Disk index not available".to_string()));
        };

        let local_disk = if let Some(Some(local_disk)) = disks_result.get(disk_idx) {
            local_disk.clone()
        } else {
            error!("Local disk not available");
            return Err(StorageError::other("Local disk not available".to_string()));
        };

        let disks = disks_result.into_iter().flatten().collect::<Vec<Arc<Disk>>>();

        let result = scan_data_folder(ctx, disks, local_disk, cache, updates, scan_mode, SCANNER_SLEEPER.clone()).await;

        match result {
            Ok(mut data_usage_info) => {
                done_drive();
                emit_scan_bucket_drive_complete(true, &bucket, &disk_path, drive_start.elapsed());
                data_usage_info.info.last_update = Some(SystemTime::now());
                Ok(data_usage_info)
            }
            Err(e) => {
                emit_scan_bucket_drive_complete(false, &bucket, &disk_path, drive_start.elapsed());
                Err(StorageError::other(format!("Failed to scan data folder: {e}")))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;
    use temp_env::with_var;

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
            assert_eq!(scanner_max_concurrent_set_scans(4), 2);
        });
    }

    #[test]
    #[serial]
    fn scanner_max_concurrent_disk_scans_uses_env_cap() {
        with_var(ENV_SCANNER_MAX_CONCURRENT_DISK_SCANS, Some("1"), || {
            assert_eq!(scanner_max_concurrent_disk_scans(4), 1);
        });
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
}
