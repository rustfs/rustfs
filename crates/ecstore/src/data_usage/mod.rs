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

// #730: scanner/data-usage state is partially migrated and still owns staged cache helpers.
#![allow(dead_code)]

pub mod local_snapshot;

use crate::storage_api_contracts::{
    bucket::{BucketOperations as _, BucketOptions},
    list::ListOperations as _,
    object::ObjectIO as _,
};
use crate::{
    bucket::{metadata_sys::get_replication_config, versioning::VersioningApi as _, versioning_sys::BucketVersioningSys},
    config::com::read_config,
    disk::DiskAPI,
    error::{Error, classify_system_path_failure_reason},
    runtime::sources as runtime_sources,
    store::ECStore,
};
pub use local_snapshot::{LocalUsageSnapshot, read_snapshot as read_local_snapshot, snapshot_path};
use rustfs_data_usage::{
    BucketTargetUsageInfo, BucketUsageInfo, CompressionTotalInfo, DataUsageCache, DataUsageEntry, DataUsageInfo, DiskUsageStatus,
    SizeSummary,
};
use rustfs_io_metrics::record_system_path_failure;
use rustfs_utils::path::SLASH_SEPARATOR;
use std::{
    collections::{HashMap, HashSet, hash_map::Entry},
    sync::{Arc, LazyLock, OnceLock},
    time::{Duration, SystemTime},
};
use tokio::fs;
use tokio::sync::RwLock;
use tracing::{debug, error, info, instrument};

// Data usage storage constants
pub const DATA_USAGE_ROOT: &str = SLASH_SEPARATOR;
const DATA_USAGE_OBJ_NAME: &str = ".usage.json";
const DATA_COMPRESSION_TOTAL_NAME: &str = ".compression.json";
const DATA_USAGE_BLOOM_NAME: &str = ".bloomcycle.bin";
pub const DATA_USAGE_CACHE_NAME: &str = ".usage-cache.bin";
const DATA_USAGE_CACHE_TTL_SECS: u64 = 30;

#[derive(Debug, Clone)]
struct CachedBucketUsage {
    usage: BucketUsageInfo,
    refreshed_at: SystemTime,
    usage_updated_at: SystemTime,
    // Set by request-path mutations until a scanner snapshot catches up to the same core counts.
    dirty: bool,
    // Set when a newer scanner snapshot was observed but did not include the dirty counts yet.
    stale_snapshot_pending: bool,
}

type UsageMemoryCache = Arc<RwLock<HashMap<String, CachedBucketUsage>>>;
type CacheUpdating = Arc<RwLock<bool>>;

static USAGE_MEMORY_CACHE: OnceLock<UsageMemoryCache> = OnceLock::new();
static USAGE_CACHE_UPDATING: OnceLock<CacheUpdating> = OnceLock::new();

/// Deferred persist thresholds for compression totals: persist after this many
/// operations recorded, but no more often than the min interval.
const COMPRESSION_PERSIST_BATCH_SIZE: u64 = 100;
const COMPRESSION_PERSIST_MIN_INTERVAL: Duration = Duration::from_secs(30);

/// In-memory compression accumulator with debounced persistence state.
#[derive(Debug, Clone)]
struct CompressionTotalState {
    info: CompressionTotalInfo,
    /// Operations recorded since the last persist attempt.
    ops_since_persist: u64,
    /// When the last persist was attempted (used for min-interval gating).
    last_persist: tokio::time::Instant,
    /// When `true`, recording is skipped (embedded mode without observability).
    inited: bool,
}

impl Default for CompressionTotalState {
    fn default() -> Self {
        Self {
            info: CompressionTotalInfo::default(),
            ops_since_persist: 0,
            last_persist: tokio::time::Instant::now(),
            inited: false,
        }
    }
}

static COMPRESSION_TOTAL_MEMORY_CACHE: LazyLock<Arc<RwLock<Option<CompressionTotalState>>>> =
    LazyLock::new(|| Arc::new(RwLock::new(Some(CompressionTotalState::default()))));

fn memory_cache() -> &'static UsageMemoryCache {
    USAGE_MEMORY_CACHE.get_or_init(|| Arc::new(RwLock::new(HashMap::new())))
}

fn cache_updating() -> &'static CacheUpdating {
    USAGE_CACHE_UPDATING.get_or_init(|| Arc::new(RwLock::new(false)))
}

// Data usage storage paths
lazy_static::lazy_static! {
    pub static ref DATA_USAGE_BUCKET: String = format!("{}{}{}",
        crate::disk::RUSTFS_META_BUCKET,
        SLASH_SEPARATOR,
        crate::disk::BUCKET_META_PREFIX
    );
    pub static ref DATA_USAGE_OBJ_NAME_PATH: String = format!("{}{}{}",
        crate::disk::BUCKET_META_PREFIX,
        SLASH_SEPARATOR,
        DATA_USAGE_OBJ_NAME
    );
    pub static ref DATA_USAGE_BLOOM_NAME_PATH: String = format!("{}{}{}",
        crate::disk::BUCKET_META_PREFIX,
        SLASH_SEPARATOR,
        DATA_USAGE_BLOOM_NAME
    );
    pub static ref DATA_COMPRESSION_TOTAL_NAME_PATH: String = format!("{}{}{}",
        crate::disk::BUCKET_META_PREFIX,
        SLASH_SEPARATOR,
        DATA_COMPRESSION_TOTAL_NAME
    );
}

/// Store data usage info to backend storage
#[instrument(skip(store))]
pub async fn store_data_usage_in_backend(data_usage_info: DataUsageInfo, store: Arc<ECStore>) -> Result<(), Error> {
    // Prevent older data from overwriting newer persisted stats
    if let Ok(buf) = read_config(store.clone(), &DATA_USAGE_OBJ_NAME_PATH).await
        && let Ok(existing) = serde_json::from_slice::<DataUsageInfo>(&buf)
        && let (Some(new_ts), Some(existing_ts)) = (data_usage_info.last_update, existing.last_update)
        && new_ts <= existing_ts
    {
        info!(
            "Skip persisting data usage: incoming last_update {:?} <= existing {:?}",
            new_ts, existing_ts
        );
        return Ok(());
    }

    save_data_usage_in_backend(data_usage_info, store).await
}

async fn save_data_usage_in_backend(data_usage_info: DataUsageInfo, store: Arc<ECStore>) -> Result<(), Error> {
    let data =
        serde_json::to_vec(&data_usage_info).map_err(|e| Error::other(format!("Failed to serialize data usage info: {e}")))?;

    // Save to backend using the same mechanism as original code
    crate::config::com::save_config(store, &DATA_USAGE_OBJ_NAME_PATH, data)
        .await
        .map_err(Error::other)?;

    Ok(())
}

fn set_buckets_count_from_usage(data_usage_info: &mut DataUsageInfo) {
    data_usage_info.buckets_count = u64::try_from(data_usage_info.buckets_usage.len()).unwrap_or(u64::MAX);
}

fn remove_bucket_usage_from_info(data_usage_info: &mut DataUsageInfo, bucket: &str) -> bool {
    if bucket.is_empty() {
        return false;
    }

    let removed_usage = data_usage_info.buckets_usage.remove(bucket).is_some();
    let removed_size = data_usage_info.bucket_sizes.remove(bucket).is_some();

    if !removed_usage && !removed_size {
        return false;
    }

    set_buckets_count_from_usage(data_usage_info);
    data_usage_info.calculate_totals();
    true
}

fn merge_bucket_usage_removal(candidate: DataUsageInfo, existing: Option<DataUsageInfo>, bucket: &str) -> Option<DataUsageInfo> {
    let mut data_usage_info = match existing {
        Some(existing) if data_usage_info_updated_at(&existing) >= data_usage_info_updated_at(&candidate) => existing,
        _ => candidate,
    };

    if remove_bucket_usage_from_info(&mut data_usage_info, bucket) {
        Some(data_usage_info)
    } else {
        None
    }
}

async fn clear_bucket_usage_memory(bucket: &str) {
    if bucket.is_empty() {
        return;
    }

    memory_cache().write().await.remove(bucket);
}

pub async fn remove_bucket_usage_from_backend(store: Arc<ECStore>, bucket: &str) -> Result<(), Error> {
    clear_bucket_usage_memory(bucket).await;

    let data_usage_info = load_data_usage_from_backend(store.clone()).await?;
    let existing = load_data_usage_from_backend(store.clone()).await.ok();

    if let Some(data_usage_info) = merge_bucket_usage_removal(data_usage_info, existing, bucket) {
        save_data_usage_in_backend(data_usage_info, store).await?;
    }

    Ok(())
}

/// Load data usage info from backend storage
#[instrument(skip(store))]
pub async fn load_data_usage_from_backend(store: Arc<ECStore>) -> Result<DataUsageInfo, Error> {
    let buf: Vec<u8> = match read_config(store.clone(), &DATA_USAGE_OBJ_NAME_PATH).await {
        Ok(data) => data,
        Err(e) => {
            let reason = classify_system_path_failure_reason(&e);
            record_system_path_failure("data_usage", "read_primary", reason);
            error!(
                path_kind = "data_usage",
                operation = "read_primary",
                reason,
                object = %DATA_USAGE_OBJ_NAME_PATH.as_str(),
                error = %e,
                "system path read failed"
            );

            match read_config(store.clone(), format!("{}.bkp", DATA_USAGE_OBJ_NAME_PATH.as_str()).as_str()).await {
                Ok(data) => data,
                Err(e) => {
                    if e == Error::ConfigNotFound {
                        return Ok(DataUsageInfo::default());
                    }
                    let reason = classify_system_path_failure_reason(&e);
                    record_system_path_failure("data_usage", "read_backup", reason);
                    error!(
                        path_kind = "data_usage",
                        operation = "read_backup",
                        reason,
                        object = %format!("{}.bkp", DATA_USAGE_OBJ_NAME_PATH.as_str()),
                        error = %e,
                        "system path read failed"
                    );
                    return Err(Error::other(e));
                }
            }
        }
    };
    let mut data_usage_info: DataUsageInfo =
        serde_json::from_slice(&buf).map_err(|e| Error::other(format!("Failed to deserialize data usage info: {e}")))?;

    info!("Loaded data usage info from backend with {} buckets", data_usage_info.buckets_count);

    // Handle backward compatibility
    if data_usage_info.buckets_usage.is_empty() {
        data_usage_info.buckets_usage = data_usage_info
            .bucket_sizes
            .iter()
            .map(|(bucket, &size)| {
                (
                    bucket.clone(),
                    BucketUsageInfo {
                        size,
                        ..Default::default()
                    },
                )
            })
            .collect();
    }

    if data_usage_info.bucket_sizes.is_empty() {
        data_usage_info.bucket_sizes = data_usage_info
            .buckets_usage
            .iter()
            .map(|(bucket, bui)| (bucket.clone(), bui.size))
            .collect();
    }

    // Handle replication info
    for (bucket, bui) in &data_usage_info.buckets_usage {
        if (bui.replicated_size_v1 > 0
            || bui.replication_failed_count_v1 > 0
            || bui.replication_failed_size_v1 > 0
            || bui.replication_pending_count_v1 > 0)
            && let Ok((cfg, _)) = get_replication_config(bucket).await
            && !cfg.role.is_empty()
        {
            data_usage_info.replication_info.insert(
                cfg.role.clone(),
                BucketTargetUsageInfo {
                    replication_failed_size: bui.replication_failed_size_v1,
                    replication_failed_count: bui.replication_failed_count_v1,
                    replicated_size: bui.replicated_size_v1,
                    replication_pending_count: bui.replication_pending_count_v1,
                    replication_pending_size: bui.replication_pending_size_v1,
                    ..Default::default()
                },
            );
        }
    }

    Ok(data_usage_info)
}

/// Aggregate usage information from local disk snapshots.
fn merge_snapshot(aggregated: &mut DataUsageInfo, mut snapshot: LocalUsageSnapshot, latest_update: &mut Option<SystemTime>) {
    if let Some(update) = snapshot.last_update
        && latest_update.is_none_or(|current| update > current)
    {
        *latest_update = Some(update);
    }

    snapshot.recompute_totals();

    aggregated.objects_total_count = aggregated.objects_total_count.saturating_add(snapshot.objects_total_count);
    aggregated.versions_total_count = aggregated.versions_total_count.saturating_add(snapshot.versions_total_count);
    aggregated.delete_markers_total_count = aggregated
        .delete_markers_total_count
        .saturating_add(snapshot.delete_markers_total_count);
    aggregated.objects_total_size = aggregated.objects_total_size.saturating_add(snapshot.objects_total_size);

    for (bucket, usage) in snapshot.buckets_usage.into_iter() {
        let bucket_size = usage.size;
        match aggregated.buckets_usage.entry(bucket.clone()) {
            Entry::Occupied(mut entry) => entry.get_mut().merge(&usage),
            Entry::Vacant(entry) => {
                entry.insert(usage.clone());
            }
        }

        aggregated
            .bucket_sizes
            .entry(bucket)
            .and_modify(|size| *size = size.saturating_add(bucket_size))
            .or_insert(bucket_size);
    }
}

pub async fn aggregate_local_snapshots(store: Arc<ECStore>) -> Result<(Vec<DiskUsageStatus>, DataUsageInfo), Error> {
    let mut aggregated = DataUsageInfo::default();
    let mut latest_update: Option<SystemTime> = None;
    let mut statuses: Vec<DiskUsageStatus> = Vec::new();
    let mut processed_disks: HashSet<String> = HashSet::new();

    for (pool_idx, pool) in store.pools.iter().enumerate() {
        for set_disks in pool.disk_set.iter() {
            let disk_entries = {
                let guard = set_disks.disks.read().await;
                guard.clone()
            };

            for (disk_index, disk_opt) in disk_entries.into_iter().enumerate() {
                let Some(disk) = disk_opt else {
                    continue;
                };

                if !disk.is_local() {
                    continue;
                }

                let disk_id = match disk.get_disk_id().await.map_err(Error::from)? {
                    Some(id) => id.to_string(),
                    None => continue,
                };

                let root = disk.path();
                let disk_key = format!("{}|{}", disk.endpoint(), root.display());

                // Skip if we've already processed this physical disk
                if !processed_disks.insert(disk_key.clone()) {
                    continue;
                }

                let mut status = DiskUsageStatus {
                    disk_id: disk_id.clone(),
                    pool_index: Some(pool_idx),
                    set_index: Some(set_disks.set_index),
                    disk_index: Some(disk_index),
                    last_update: None,
                    snapshot_exists: false,
                };

                let snapshot_result = read_local_snapshot(root.as_path(), &disk_id).await;

                // If a snapshot is corrupted or unreadable, skip it but keep processing others
                if let Err(err) = &snapshot_result {
                    info!(
                        "Failed to read data usage snapshot for disk {} (pool {}, set {}, disk {}): {}",
                        disk_id, pool_idx, set_disks.set_index, disk_index, err
                    );
                    // Best-effort cleanup so next scan can rebuild a fresh snapshot instead of repeatedly failing
                    let snapshot_file = snapshot_path(root.as_path(), &disk_id);
                    if let Err(remove_err) = fs::remove_file(&snapshot_file).await
                        && remove_err.kind() != std::io::ErrorKind::NotFound
                    {
                        info!("Failed to remove corrupted snapshot {:?}: {}", snapshot_file, remove_err);
                    }
                }

                if let Ok(Some(mut snapshot)) = snapshot_result {
                    status.last_update = snapshot.last_update;
                    status.snapshot_exists = true;

                    if snapshot.meta.disk_id.is_empty() {
                        snapshot.meta.disk_id = disk_id.clone();
                    }
                    if snapshot.meta.pool_index.is_none() {
                        snapshot.meta.pool_index = Some(pool_idx);
                    }
                    if snapshot.meta.set_index.is_none() {
                        snapshot.meta.set_index = Some(set_disks.set_index);
                    }
                    if snapshot.meta.disk_index.is_none() {
                        snapshot.meta.disk_index = Some(disk_index);
                    }

                    merge_snapshot(&mut aggregated, snapshot, &mut latest_update);
                }

                statuses.push(status);
            }
        }
    }

    aggregated.buckets_count = aggregated.buckets_usage.len() as u64;
    aggregated.last_update = latest_update;
    aggregated.disk_usage_status = statuses.clone();

    Ok((statuses, aggregated))
}

/// Calculate accurate bucket usage statistics by enumerating objects through the object layer.
pub async fn compute_bucket_usage(store: Arc<ECStore>, bucket_name: &str) -> Result<BucketUsageInfo, Error> {
    let mut marker: Option<String> = None;
    let mut version_marker: Option<String> = None;
    let mut object_names: HashSet<String> = HashSet::new();
    let mut versions_count: u64 = 0;
    let mut total_size: u64 = 0;
    let mut delete_markers: u64 = 0;

    loop {
        let result = store
            .clone()
            .list_object_versions(
                bucket_name,
                "", // prefix
                marker.clone(),
                version_marker.clone(),
                None, // delimiter
                1000, // max_keys
            )
            .await?;

        for object in result.objects.iter() {
            if object.is_dir {
                continue;
            }

            if object.delete_marker {
                delete_markers = delete_markers.saturating_add(1);
                continue;
            }

            let object_size = object.size.max(0) as u64;
            object_names.insert(object.name.clone());
            total_size = total_size.saturating_add(object_size);
            versions_count = versions_count.saturating_add(1);
        }

        if !result.is_truncated {
            break;
        }

        marker = result.next_marker.clone();
        version_marker = result.next_version_idmarker.clone();
        if marker.is_none() {
            info!(
                "Bucket {} version listing marked truncated but no marker returned; stopping early",
                bucket_name
            );
            break;
        }
    }

    let objects_count = object_names.len() as u64;

    let usage = BucketUsageInfo {
        size: total_size,
        objects_count,
        versions_count,
        delete_markers_count: delete_markers,
        ..Default::default()
    };

    Ok(usage)
}

pub async fn refresh_versioned_bucket_usage_from_object_layer(store: Arc<ECStore>, data_usage_info: &mut DataUsageInfo) {
    let listed_bucket_names = match store
        .list_bucket(&BucketOptions {
            no_metadata: true,
            ..Default::default()
        })
        .await
    {
        Ok(buckets) => buckets.into_iter().map(|bucket| bucket.name).collect::<Vec<_>>(),
        Err(err) => {
            debug!(error = %err, "failed to list buckets while refreshing versioned bucket usage");
            Vec::new()
        }
    };
    let buckets = bucket_names_for_versioned_refresh(data_usage_info, listed_bucket_names);
    let mut changed = false;

    for bucket in buckets {
        let Ok(versioning) = BucketVersioningSys::get(&bucket).await else {
            continue;
        };

        if !versioning.enabled() && !versioning.suspended() {
            continue;
        }

        let refresh_started_at = SystemTime::now();
        let usage = match compute_bucket_usage(store.clone(), &bucket).await {
            Ok(usage) => usage,
            Err(err) => {
                debug!(
                    bucket = %bucket,
                    error = %err,
                    "failed to refresh versioned bucket usage from object layer"
                );
                continue;
            }
        };

        replace_bucket_usage_memory_from_authoritative(&bucket, usage.clone(), refresh_started_at).await;
        data_usage_info.bucket_sizes.insert(bucket.clone(), usage.size);
        data_usage_info.buckets_usage.insert(bucket, usage);
        changed = true;
    }

    if changed {
        set_buckets_count_from_usage(data_usage_info);
        data_usage_info.calculate_totals();
    }
}

fn bucket_names_for_versioned_refresh(
    data_usage_info: &DataUsageInfo,
    listed_bucket_names: impl IntoIterator<Item = String>,
) -> Vec<String> {
    let mut buckets = data_usage_info.buckets_usage.keys().cloned().collect::<HashSet<String>>();
    buckets.extend(listed_bucket_names.into_iter().filter(|bucket| !bucket.is_empty()));

    let mut buckets = buckets.into_iter().collect::<Vec<_>>();
    buckets.sort();
    buckets
}

async fn ensure_bucket_usage_cached(bucket: &str) {
    let cache = memory_cache().read().await;
    if cache.contains_key(bucket) {
        return;
    }
    drop(cache);

    update_usage_cache_if_needed().await;
}

fn cached_bucket_usage_from_backend(usage: BucketUsageInfo, updated_at: SystemTime) -> CachedBucketUsage {
    CachedBucketUsage {
        usage,
        refreshed_at: SystemTime::now(),
        usage_updated_at: updated_at,
        dirty: false,
        stale_snapshot_pending: false,
    }
}

fn cached_bucket_usage_now(usage: BucketUsageInfo) -> CachedBucketUsage {
    let now = SystemTime::now();
    CachedBucketUsage {
        usage,
        refreshed_at: now,
        usage_updated_at: now,
        dirty: false,
        stale_snapshot_pending: false,
    }
}

fn data_usage_info_updated_at(data_usage_info: &DataUsageInfo) -> SystemTime {
    data_usage_info.last_update.unwrap_or(SystemTime::UNIX_EPOCH)
}

fn bucket_usage_counts_match(left: &BucketUsageInfo, right: &BucketUsageInfo) -> bool {
    left.size == right.size
        && left.objects_count == right.objects_count
        && left.versions_count == right.versions_count
        && left.delete_markers_count == right.delete_markers_count
}

async fn replace_bucket_usage_memory_from_authoritative(bucket: &str, usage: BucketUsageInfo, refresh_started_at: SystemTime) {
    let mut cache = memory_cache().write().await;
    if let Some(existing) = cache.get(bucket)
        && existing.usage_updated_at > refresh_started_at
    {
        return;
    }

    cache.insert(bucket.to_string(), cached_bucket_usage_from_backend(usage, refresh_started_at));
}

/// Fast in-memory update for immediate quota and admin usage consistency.
pub async fn record_bucket_object_write_memory(bucket: &str, previous_current_size: Option<u64>, new_size: u64) {
    record_bucket_object_write_memory_inner(bucket, previous_current_size, new_size, false).await;
}

/// Fast in-memory update for versioned object writes.
pub async fn record_bucket_object_version_write_memory(bucket: &str, previous_current_size: Option<u64>, new_size: u64) {
    record_bucket_object_write_memory_inner(bucket, previous_current_size, new_size, true).await;
}

async fn record_bucket_object_write_memory_inner(
    bucket: &str,
    previous_current_size: Option<u64>,
    new_size: u64,
    creates_new_version: bool,
) {
    ensure_bucket_usage_cached(bucket).await;

    let mut cache = memory_cache().write().await;
    let entry = cache
        .entry(bucket.to_string())
        .or_insert_with(|| cached_bucket_usage_now(BucketUsageInfo::default()));

    if creates_new_version {
        entry.usage.size = entry.usage.size.saturating_add(new_size);
        if previous_current_size.is_none() {
            entry.usage.objects_count = entry.usage.objects_count.saturating_add(1);
        }
        entry.usage.versions_count = entry.usage.versions_count.saturating_add(1);
    } else {
        match previous_current_size {
            Some(previous_size) => {
                entry.usage.size = entry.usage.size.saturating_sub(previous_size).saturating_add(new_size);
            }
            None => {
                entry.usage.size = entry.usage.size.saturating_add(new_size);
                entry.usage.objects_count = entry.usage.objects_count.saturating_add(1);
                entry.usage.versions_count = entry.usage.versions_count.saturating_add(1);
            }
        }
    }

    let now = SystemTime::now();
    entry.refreshed_at = now;
    entry.usage_updated_at = now;
    entry.dirty = true;
    entry.stale_snapshot_pending = false;
}

/// Fast in-memory increment for immediate quota consistency.
pub async fn increment_bucket_usage_memory(bucket: &str, size_increment: u64) {
    record_bucket_object_write_memory(bucket, None, size_increment).await;
}

/// Fast in-memory update for successful object deletes.
pub async fn record_bucket_object_delete_memory(bucket: &str, deleted_size: u64, removed_current_object: bool) {
    ensure_bucket_usage_cached(bucket).await;

    let mut cache = memory_cache().write().await;
    let entry = cache
        .entry(bucket.to_string())
        .or_insert_with(|| cached_bucket_usage_now(BucketUsageInfo::default()));

    entry.usage.size = entry.usage.size.saturating_sub(deleted_size);
    if removed_current_object {
        entry.usage.objects_count = entry.usage.objects_count.saturating_sub(1);
        entry.usage.versions_count = entry.usage.versions_count.saturating_sub(1);
    }

    let now = SystemTime::now();
    entry.refreshed_at = now;
    entry.usage_updated_at = now;
    entry.dirty = true;
    entry.stale_snapshot_pending = false;
}

/// Fast in-memory update for successful delete marker creation.
pub async fn record_bucket_delete_marker_memory(bucket: &str) {
    ensure_bucket_usage_cached(bucket).await;

    let mut cache = memory_cache().write().await;
    let entry = cache
        .entry(bucket.to_string())
        .or_insert_with(|| cached_bucket_usage_now(BucketUsageInfo::default()));

    entry.usage.delete_markers_count = entry.usage.delete_markers_count.saturating_add(1);

    let now = SystemTime::now();
    entry.refreshed_at = now;
    entry.usage_updated_at = now;
    entry.dirty = true;
    entry.stale_snapshot_pending = false;
}

/// Fast in-memory decrement for immediate quota consistency
pub async fn decrement_bucket_usage_memory(bucket: &str, size_decrement: u64) {
    record_bucket_object_delete_memory(bucket, size_decrement, size_decrement > 0).await;
}

/// Get bucket usage from in-memory cache
pub async fn get_bucket_usage_memory(bucket: &str) -> Option<u64> {
    update_usage_cache_if_needed().await;

    let cache = memory_cache().read().await;
    cache.get(bucket).map(|cached| cached.usage.size)
}

async fn update_usage_cache_if_needed() {
    let ttl = Duration::from_secs(DATA_USAGE_CACHE_TTL_SECS);
    let double_ttl = ttl * 2;
    let now = SystemTime::now();

    let cache = memory_cache().read().await;
    let earliest_timestamp = cache.values().map(|cached| cached.refreshed_at).min();
    drop(cache);

    let age = match earliest_timestamp {
        Some(ts) => now.duration_since(ts).unwrap_or_default(),
        None => double_ttl,
    };

    if age < ttl {
        return;
    }

    let mut updating = cache_updating().write().await;
    if age < double_ttl {
        if *updating {
            return;
        }
        *updating = true;
        drop(updating);

        let updating_clone = (*cache_updating()).clone();
        tokio::spawn(async move {
            if let Some(store) = runtime_sources::object_store_handle()
                && let Ok(data_usage_info) = load_data_usage_from_backend(store.clone()).await
            {
                replace_bucket_usage_memory_from_info(&data_usage_info).await;
            }
            let mut updating = updating_clone.write().await;
            *updating = false;
        });
        return;
    }

    for retry in 0..10 {
        if !*updating {
            break;
        }
        drop(updating);
        let delay = Duration::from_millis(1 << retry);
        tokio::time::sleep(delay).await;
        updating = cache_updating().write().await;
    }

    *updating = true;
    drop(updating);

    if let Some(store) = runtime_sources::object_store_handle()
        && let Ok(data_usage_info) = load_data_usage_from_backend(store.clone()).await
    {
        replace_bucket_usage_memory_from_info(&data_usage_info).await;
    }

    let mut updating = cache_updating().write().await;
    *updating = false;
}

pub async fn replace_bucket_usage_memory_from_info(data_usage_info: &DataUsageInfo) {
    let usage_updated_at = data_usage_info_updated_at(data_usage_info);
    let mut cache = memory_cache().write().await;
    let mut next_cache = HashMap::new();

    for (bucket, bucket_usage) in data_usage_info.buckets_usage.iter() {
        if let Some(existing) = cache.get(bucket) {
            if existing.usage_updated_at > usage_updated_at {
                next_cache.insert(bucket.clone(), existing.clone());
                continue;
            }

            if existing.dirty && !bucket_usage_counts_match(&existing.usage, bucket_usage) {
                // A scanner snapshot can be saved after newer writes but still miss them if it listed the bucket earlier.
                let mut preserved = existing.clone();
                preserved.stale_snapshot_pending = true;
                next_cache.insert(bucket.clone(), preserved);
                continue;
            }
        }

        next_cache.insert(bucket.clone(), cached_bucket_usage_from_backend(bucket_usage.clone(), usage_updated_at));
    }

    for (bucket, existing) in cache.iter() {
        if !data_usage_info.buckets_usage.contains_key(bucket) {
            if existing.usage_updated_at > usage_updated_at {
                next_cache.insert(bucket.clone(), existing.clone());
                continue;
            }

            if existing.dirty {
                let mut preserved = existing.clone();
                preserved.stale_snapshot_pending = true;
                next_cache.insert(bucket.clone(), preserved);
            }
        }
    }

    *cache = next_cache;
}

pub async fn apply_bucket_usage_memory_overlay(data_usage_info: &mut DataUsageInfo) {
    let cache = memory_cache().read().await;
    if cache.is_empty() {
        return;
    }

    let persisted_update = data_usage_info.last_update;
    let mut changed = false;

    for (bucket, cached) in cache.iter() {
        if !cached.stale_snapshot_pending && persisted_update.is_some_and(|persisted| cached.usage_updated_at <= persisted) {
            continue;
        }

        data_usage_info.buckets_usage.insert(bucket.clone(), cached.usage.clone());
        data_usage_info.bucket_sizes.insert(bucket.clone(), cached.usage.size);
        changed = true;
    }

    if changed {
        data_usage_info.buckets_count = data_usage_info.buckets_usage.len() as u64;
        data_usage_info.calculate_totals();
    }
}

/// Sync memory cache with backend data (called by scanner)
pub async fn sync_memory_cache_with_backend() -> Result<(), Error> {
    if let Some(store) = runtime_sources::object_store_handle() {
        match load_data_usage_from_backend(store.clone()).await {
            Ok(data_usage_info) => {
                replace_bucket_usage_memory_from_info(&data_usage_info).await;
            }
            Err(e) => {
                debug!("Failed to sync memory cache with backend: {}", e);
            }
        }
    }
    Ok(())
}

/// Create a data usage cache entry from size summary
pub fn create_cache_entry_from_summary(summary: &SizeSummary) -> DataUsageEntry {
    let mut entry = DataUsageEntry::default();
    entry.add_sizes(summary);
    entry
}

/// Convert data usage cache to DataUsageInfo
pub fn cache_to_data_usage_info(
    cache: &DataUsageCache,
    path: &str,
    buckets: &[crate::storage_api_contracts::bucket::BucketInfo],
) -> DataUsageInfo {
    let e = match cache.find(path) {
        Some(e) => e,
        None => return DataUsageInfo::default(),
    };
    let flat = cache.flatten(&e);

    let mut buckets_usage = HashMap::new();
    for bucket in buckets.iter() {
        let e = match cache.find(&bucket.name) {
            Some(e) => e,
            None => continue,
        };
        let flat = cache.flatten(&e);
        let mut bui = BucketUsageInfo {
            size: flat.size as u64,
            versions_count: flat.versions as u64,
            objects_count: flat.objects as u64,
            delete_markers_count: flat.delete_markers as u64,
            object_size_histogram: flat.obj_sizes.to_map(),
            object_versions_histogram: flat.obj_versions.to_map(),
            ..Default::default()
        };

        if let Some(rs) = &flat.replication_stats {
            bui.replica_size = rs.replica_size;
            bui.replica_count = rs.replica_count;

            for (arn, stat) in rs.targets.iter() {
                bui.replication_info.insert(
                    arn.clone(),
                    BucketTargetUsageInfo {
                        replication_pending_size: stat.pending_size,
                        replicated_size: stat.replicated_size,
                        replication_failed_size: stat.failed_size,
                        replication_pending_count: stat.pending_count,
                        replication_failed_count: stat.failed_count,
                        replicated_count: stat.replicated_count,
                        ..Default::default()
                    },
                );
            }
        }
        buckets_usage.insert(bucket.name.clone(), bui);
    }

    DataUsageInfo {
        last_update: cache.info.last_update,
        objects_total_count: flat.objects as u64,
        versions_total_count: flat.versions as u64,
        delete_markers_total_count: flat.delete_markers as u64,
        objects_total_size: flat.size as u64,
        buckets_count: e.children.len() as u64,
        buckets_usage,
        ..Default::default()
    }
}

// Helper functions for DataUsageCache operations
pub async fn load_data_usage_cache(store: &crate::set_disk::SetDisks, name: &str) -> crate::error::Result<DataUsageCache> {
    use crate::disk::{BUCKET_META_PREFIX, RUSTFS_META_BUCKET};
    use crate::object_api::ObjectOptions;
    use http::HeaderMap;
    use rand::RngExt;
    use std::path::Path;
    use std::time::Duration;
    use tokio::time::sleep;

    let mut d = DataUsageCache::default();
    let mut retries = 0;
    while retries < 5 {
        let path = Path::new(BUCKET_META_PREFIX).join(name);
        match store
            .get_object_reader(
                RUSTFS_META_BUCKET,
                path.to_str().unwrap(),
                None,
                HeaderMap::new(),
                &ObjectOptions {
                    no_lock: true,
                    ..Default::default()
                },
            )
            .await
        {
            Ok(mut reader) => {
                if let Ok(info) = DataUsageCache::unmarshal(&reader.read_all().await?) {
                    d = info
                }
                break;
            }
            Err(err) => match err {
                Error::FileNotFound | Error::VolumeNotFound => {
                    match store
                        .get_object_reader(
                            RUSTFS_META_BUCKET,
                            name,
                            None,
                            HeaderMap::new(),
                            &ObjectOptions {
                                no_lock: true,
                                ..Default::default()
                            },
                        )
                        .await
                    {
                        Ok(mut reader) => {
                            if let Ok(info) = DataUsageCache::unmarshal(&reader.read_all().await?) {
                                d = info
                            }
                            break;
                        }
                        Err(_) => match err {
                            Error::FileNotFound | Error::VolumeNotFound => {
                                break;
                            }
                            _ => {}
                        },
                    }
                }
                _ => {
                    break;
                }
            },
        }
        retries += 1;
        let dur = {
            let mut rng = rand::rng();
            rng.random_range(0..1_000)
        };
        sleep(Duration::from_millis(dur)).await;
    }
    Ok(d)
}

#[instrument(skip(cache))]
pub async fn save_data_usage_cache(cache: &DataUsageCache, name: &str) -> crate::error::Result<()> {
    use crate::config::com::save_config;
    use crate::disk::BUCKET_META_PREFIX;
    use std::path::Path;

    let Some(store) = runtime_sources::object_store_handle() else {
        return Err(Error::other("errServerNotInitialized"));
    };
    let buf = cache.marshal_msg().map_err(Error::other)?;
    let buf_clone = buf.clone();

    let store_clone = store.clone();

    let name = Path::new(BUCKET_META_PREFIX).join(name).to_string_lossy().to_string();

    let name_clone = name.clone();
    tokio::spawn(async move {
        let _ = save_config(store_clone, &format!("{}{}", &name_clone, ".bkp"), buf_clone).await;
    });
    save_config(store, &name, buf).await?;
    Ok(())
}

/// Persist the current in-memory compression total to the backend.
/// Resets the debounce counter so the next auto-persist won't fire
/// immediately after this manual flush (intended for shutdown paths).
/// Storage will not be triggered when uninitialized.
pub async fn store_compression_total_in_backend() {
    let snapshot = {
        let mut guard = COMPRESSION_TOTAL_MEMORY_CACHE.write().await;
        let Some(state) = guard.as_mut() else {
            return;
        };
        if !state.inited {
            return;
        }
        state.ops_since_persist = 0;
        state.last_persist = tokio::time::Instant::now();
        state.info.clone()
    };
    try_flush_compression_total(&snapshot).await;
}

/// Load compression total info from backend storage
#[instrument(skip(store))]
pub async fn load_compression_total_from_backend(store: Arc<ECStore>) -> Result<CompressionTotalInfo, Error> {
    let buf: Vec<u8> = match read_config(store.clone(), &DATA_COMPRESSION_TOTAL_NAME_PATH).await {
        Ok(data) => data,
        Err(e) => {
            if e == Error::ConfigNotFound {
                return Ok(CompressionTotalInfo::default());
            }
            let reason = classify_system_path_failure_reason(&e);
            record_system_path_failure("compression_total", "read_primary", reason);
            error!(
                path_kind = "compression_total",
                operation = "read_primary",
                reason,
                object = %DATA_COMPRESSION_TOTAL_NAME_PATH.as_str(),
                error = %e,
                "system path read failed"
            );
            return Err(Error::other(e));
        }
    };
    let compression_total: CompressionTotalInfo =
        serde_json::from_slice(&buf).map_err(|e| Error::other(format!("Failed to deserialize compression total info: {e}")))?;

    info!(
        "Loaded compression total info: original={}, compressed={}, operations={}",
        compression_total.original_bytes_total,
        compression_total.compressed_bytes_total,
        compression_total.compression_operations_total
    );

    Ok(compression_total)
}

pub async fn load_compression_total_from_memory() -> Option<CompressionTotalInfo> {
    COMPRESSION_TOTAL_MEMORY_CACHE.read().await.as_ref().map(|s| s.info.clone())
}

/// Record a compression operation. Accumulates totals in memory and triggers a
/// background persist to backend after every `COMPRESSION_PERSIST_BATCH_SIZE`
/// operations, gated by a minimum interval to avoid excessive IO under high
/// throughput.
pub async fn record_compression_total_memory(original_size: u64, compressed_size: u64) {
    let mut guard = COMPRESSION_TOTAL_MEMORY_CACHE.write().await;
    let Some(state) = guard.as_mut() else {
        error!("compression total memory cache not initialized, discarding record");
        return;
    };
    // Compression totals are only recorded while this cache is initialized.
    if !state.inited {
        return;
    }
    state.info.original_bytes_total = state.info.original_bytes_total.saturating_add(original_size);
    state.info.compressed_bytes_total = state.info.compressed_bytes_total.saturating_add(compressed_size);
    state.info.compression_operations_total = state.info.compression_operations_total.saturating_add(1);
    state.ops_since_persist = state.ops_since_persist.saturating_add(1);
    if state.ops_since_persist >= COMPRESSION_PERSIST_BATCH_SIZE
        || state.last_persist.elapsed() >= COMPRESSION_PERSIST_MIN_INTERVAL
    {
        state.ops_since_persist = 0;
        state.last_persist = tokio::time::Instant::now();
        // Fire-and-forget: hold no lock during IO
        let info = state.info.clone();
        drop(guard);
        tokio::spawn(async move {
            try_flush_compression_total(&info).await;
        });
    }
}

/// Persist the given `CompressionTotalInfo` snapshot to backend storage.
/// Used by both the debounce path and the manual flush path.
async fn try_flush_compression_total(info: &CompressionTotalInfo) {
    let Some(store) = runtime_sources::object_store_handle() else {
        error!("object store not initialized, skipping compression total persist");
        return;
    };
    match serde_json::to_vec(info) {
        Ok(data) => {
            if let Err(e) = crate::config::com::save_config(store.clone(), &DATA_COMPRESSION_TOTAL_NAME_PATH, data).await {
                error!("Failed to persist compression total to backend: {}", e);
            }
        }
        Err(e) => {
            error!("Failed to serialize compression total info: {}", e);
        }
    }
}

/// Initialize the compression total memory cache from backend storage.
/// Should be called at startup after the store is available if compression is enabled.
/// Compression totals are only recorded while this cache is initialized, so this must
/// be called before any compression operations are recorded.
#[instrument(skip(store))]
pub async fn init_compression_total_memory_from_backend(store: Arc<ECStore>) {
    let info = match load_compression_total_from_backend(store).await {
        Ok(info) => info,
        Err(e) => {
            // If load fails (e.g. ConfigNotFound or corrupt backup), start from zero.
            info!("Failed to init compression total from backend, starting from zero: {}", e);
            CompressionTotalInfo::default()
        }
    };
    *COMPRESSION_TOTAL_MEMORY_CACHE.write().await = Some(CompressionTotalState {
        info,
        ops_since_persist: 0,
        last_persist: tokio::time::Instant::now(),
        inited: true,
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustfs_data_usage::BucketUsageInfo;
    use serial_test::serial;

    async fn clear_usage_memory_cache_for_test() {
        memory_cache().write().await.clear();
        *cache_updating().write().await = false;
    }

    fn data_usage_info_for_test(bucket: &str, objects_count: u64, size: u64, last_update: SystemTime) -> DataUsageInfo {
        let mut info = DataUsageInfo {
            last_update: Some(last_update),
            ..Default::default()
        };
        info.buckets_usage.insert(
            bucket.to_string(),
            BucketUsageInfo {
                objects_count,
                versions_count: objects_count,
                size,
                ..Default::default()
            },
        );
        info.bucket_sizes.insert(bucket.to_string(), size);
        info.buckets_count = info.buckets_usage.len() as u64;
        info.calculate_totals();
        info
    }

    fn aggregate_for_test(
        inputs: Vec<(DiskUsageStatus, Result<Option<LocalUsageSnapshot>, Error>)>,
    ) -> (Vec<DiskUsageStatus>, DataUsageInfo) {
        let mut aggregated = DataUsageInfo::default();
        let mut latest_update: Option<SystemTime> = None;
        let mut statuses = Vec::new();

        for (mut status, snapshot_result) in inputs {
            if let Ok(Some(snapshot)) = snapshot_result {
                status.snapshot_exists = true;
                status.last_update = snapshot.last_update;
                merge_snapshot(&mut aggregated, snapshot, &mut latest_update);
            }
            statuses.push(status);
        }

        aggregated.buckets_count = aggregated.buckets_usage.len() as u64;
        aggregated.last_update = latest_update;
        aggregated.disk_usage_status = statuses.clone();

        (statuses, aggregated)
    }

    #[test]
    fn aggregate_skips_corrupted_snapshot_and_preserves_other_disks() {
        let mut good_snapshot = LocalUsageSnapshot::new(local_snapshot::LocalUsageSnapshotMeta {
            disk_id: "good-disk".to_string(),
            pool_index: Some(0),
            set_index: Some(0),
            disk_index: Some(0),
        });
        good_snapshot.last_update = Some(SystemTime::now());
        good_snapshot.buckets_usage.insert(
            "bucket-a".to_string(),
            BucketUsageInfo {
                objects_count: 3,
                versions_count: 3,
                size: 42,
                ..Default::default()
            },
        );
        good_snapshot.recompute_totals();

        let bad_snapshot_err: Result<Option<LocalUsageSnapshot>, Error> = Err(Error::other("corrupted snapshot payload"));

        let inputs = vec![
            (
                DiskUsageStatus {
                    disk_id: "bad-disk".to_string(),
                    pool_index: Some(0),
                    set_index: Some(0),
                    disk_index: Some(1),
                    last_update: None,
                    snapshot_exists: false,
                },
                bad_snapshot_err,
            ),
            (
                DiskUsageStatus {
                    disk_id: "good-disk".to_string(),
                    pool_index: Some(0),
                    set_index: Some(0),
                    disk_index: Some(0),
                    last_update: None,
                    snapshot_exists: false,
                },
                Ok(Some(good_snapshot)),
            ),
        ];

        let (statuses, aggregated) = aggregate_for_test(inputs);

        // Bad disk stays non-existent, good disk is marked present
        let bad_status = statuses.iter().find(|s| s.disk_id == "bad-disk").unwrap();
        assert!(!bad_status.snapshot_exists);
        let good_status = statuses.iter().find(|s| s.disk_id == "good-disk").unwrap();
        assert!(good_status.snapshot_exists);

        // Aggregated data is from good snapshot only
        assert_eq!(aggregated.objects_total_count, 3);
        assert_eq!(aggregated.objects_total_size, 42);
        assert_eq!(aggregated.buckets_count, 1);
        assert_eq!(aggregated.buckets_usage.get("bucket-a").map(|b| (b.objects_count, b.size)), Some((3, 42)));
    }

    #[tokio::test]
    #[serial]
    async fn memory_overlay_reflects_recent_delete_before_scanner_persists() {
        clear_usage_memory_cache_for_test().await;

        let persisted = data_usage_info_for_test("bucket-a", 1, 42, SystemTime::now() - Duration::from_secs(10));
        replace_bucket_usage_memory_from_info(&persisted).await;
        record_bucket_object_delete_memory("bucket-a", 42, true).await;

        let mut response = persisted.clone();
        apply_bucket_usage_memory_overlay(&mut response).await;

        assert_eq!(response.objects_total_count, 0);
        assert_eq!(response.objects_total_size, 0);
        assert_eq!(
            response
                .buckets_usage
                .get("bucket-a")
                .map(|usage| (usage.objects_count, usage.size)),
            Some((0, 0))
        );
    }

    #[test]
    fn remove_bucket_usage_from_info_drops_bucket_and_recomputes_totals() {
        let last_update = SystemTime::now();
        let mut info = data_usage_info_for_test("bucket-a", 2, 84, last_update);
        info.buckets_usage.insert(
            "bucket-b".to_string(),
            BucketUsageInfo {
                objects_count: 3,
                versions_count: 3,
                size: 126,
                ..Default::default()
            },
        );
        info.bucket_sizes.insert("bucket-b".to_string(), 126);
        info.buckets_count = 2;
        info.calculate_totals();

        assert!(remove_bucket_usage_from_info(&mut info, "bucket-a"));

        assert_eq!(info.buckets_count, 1);
        assert_eq!(info.objects_total_count, 3);
        assert_eq!(info.objects_total_size, 126);
        assert_eq!(info.last_update, Some(last_update));
        assert!(!info.buckets_usage.contains_key("bucket-a"));
        assert!(!info.bucket_sizes.contains_key("bucket-a"));
        assert_eq!(
            info.buckets_usage
                .get("bucket-b")
                .map(|usage| (usage.objects_count, usage.size)),
            Some((3, 126))
        );
    }

    #[test]
    fn merge_bucket_usage_removal_preserves_current_snapshot() {
        let now = SystemTime::now();
        let candidate = data_usage_info_for_test("bucket-a", 2, 84, now - Duration::from_secs(10));
        let mut existing = data_usage_info_for_test("bucket-a", 4, 168, now);
        existing.buckets_usage.insert(
            "bucket-c".to_string(),
            BucketUsageInfo {
                objects_count: 5,
                versions_count: 5,
                size: 210,
                ..Default::default()
            },
        );
        existing.bucket_sizes.insert("bucket-c".to_string(), 210);
        existing.buckets_count = 2;
        existing.calculate_totals();

        let merged = merge_bucket_usage_removal(candidate, Some(existing), "bucket-a")
            .expect("bucket-a should be removed from the current snapshot");

        assert_eq!(merged.last_update, Some(now));
        assert_eq!(merged.buckets_count, 1);
        assert_eq!(merged.objects_total_count, 5);
        assert_eq!(merged.objects_total_size, 210);
        assert!(!merged.buckets_usage.contains_key("bucket-a"));
        assert_eq!(
            merged
                .buckets_usage
                .get("bucket-c")
                .map(|usage| (usage.objects_count, usage.size)),
            Some((5, 210))
        );
    }

    #[tokio::test]
    #[serial]
    async fn clear_bucket_usage_memory_prevents_deleted_bucket_overlay() {
        clear_usage_memory_cache_for_test().await;

        let persisted = data_usage_info_for_test("bucket-a", 1, 42, SystemTime::now() - Duration::from_secs(10));
        replace_bucket_usage_memory_from_info(&persisted).await;
        record_bucket_object_delete_memory("bucket-a", 42, true).await;
        clear_bucket_usage_memory("bucket-a").await;

        let mut response = DataUsageInfo {
            last_update: Some(SystemTime::now()),
            ..Default::default()
        };
        apply_bucket_usage_memory_overlay(&mut response).await;

        assert_eq!(response.buckets_count, 0);
        assert_eq!(response.objects_total_count, 0);
        assert!(!response.buckets_usage.contains_key("bucket-a"));
        assert!(!response.bucket_sizes.contains_key("bucket-a"));
    }

    #[tokio::test]
    #[serial]
    async fn memory_overlay_preserves_object_count_for_overwrite() {
        clear_usage_memory_cache_for_test().await;

        let persisted = data_usage_info_for_test("bucket-a", 1, 10, SystemTime::now() - Duration::from_secs(10));
        replace_bucket_usage_memory_from_info(&persisted).await;
        record_bucket_object_write_memory("bucket-a", Some(10), 20).await;

        let mut response = persisted.clone();
        apply_bucket_usage_memory_overlay(&mut response).await;

        assert_eq!(response.objects_total_count, 1);
        assert_eq!(response.objects_total_size, 20);
        assert_eq!(
            response
                .buckets_usage
                .get("bucket-a")
                .map(|usage| (usage.objects_count, usage.size)),
            Some((1, 20))
        );
    }

    #[tokio::test]
    #[serial]
    async fn memory_overlay_counts_versioned_overwrite_as_new_version() {
        clear_usage_memory_cache_for_test().await;

        let persisted = data_usage_info_for_test("bucket-a", 1, 10, SystemTime::now() - Duration::from_secs(10));
        replace_bucket_usage_memory_from_info(&persisted).await;
        record_bucket_object_version_write_memory("bucket-a", Some(10), 20).await;

        let mut response = persisted.clone();
        apply_bucket_usage_memory_overlay(&mut response).await;

        assert_eq!(response.objects_total_count, 1);
        assert_eq!(response.versions_total_count, 2);
        assert_eq!(response.objects_total_size, 30);
        assert_eq!(
            response
                .buckets_usage
                .get("bucket-a")
                .map(|usage| (usage.objects_count, usage.versions_count, usage.size)),
            Some((1, 2, 30))
        );
    }

    #[tokio::test]
    #[serial]
    async fn memory_overlay_records_delete_marker_without_removing_versions() {
        clear_usage_memory_cache_for_test().await;

        let persisted = data_usage_info_for_test("bucket-a", 2, 30, SystemTime::now() - Duration::from_secs(10));
        replace_bucket_usage_memory_from_info(&persisted).await;
        record_bucket_delete_marker_memory("bucket-a").await;

        let mut response = persisted.clone();
        apply_bucket_usage_memory_overlay(&mut response).await;

        assert_eq!(response.objects_total_count, 2);
        assert_eq!(response.versions_total_count, 2);
        assert_eq!(response.delete_markers_total_count, 1);
        assert_eq!(response.objects_total_size, 30);
        assert_eq!(
            response
                .buckets_usage
                .get("bucket-a")
                .map(|usage| { (usage.objects_count, usage.versions_count, usage.delete_markers_count, usage.size,) }),
            Some((2, 2, 1, 30))
        );
    }

    #[test]
    fn versioned_refresh_bucket_names_include_live_buckets_without_usage_snapshot() {
        let mut persisted = DataUsageInfo::default();
        persisted.buckets_usage.insert(
            "bucket-a".to_string(),
            BucketUsageInfo {
                objects_count: 1,
                versions_count: 1,
                size: 10,
                ..Default::default()
            },
        );

        let buckets = bucket_names_for_versioned_refresh(&persisted, vec!["bucket-b".to_string(), "bucket-a".to_string()]);

        assert_eq!(buckets, vec!["bucket-a".to_string(), "bucket-b".to_string()]);
    }

    #[tokio::test]
    #[serial]
    async fn authoritative_versioned_refresh_replaces_stale_dirty_memory() {
        clear_usage_memory_cache_for_test().await;

        let old_persisted = data_usage_info_for_test("bucket-a", 0, 0, SystemTime::now() - Duration::from_secs(10));
        replace_bucket_usage_memory_from_info(&old_persisted).await;
        record_bucket_object_write_memory("bucket-a", None, 15).await;

        let authoritative = BucketUsageInfo {
            objects_count: 1,
            versions_count: 1,
            delete_markers_count: 1,
            size: 10,
            ..Default::default()
        };
        replace_bucket_usage_memory_from_authoritative("bucket-a", authoritative.clone(), SystemTime::now()).await;

        let mut response = old_persisted.clone();
        response.buckets_usage.insert("bucket-a".to_string(), authoritative);
        response.bucket_sizes.insert("bucket-a".to_string(), 10);
        response.calculate_totals();

        apply_bucket_usage_memory_overlay(&mut response).await;

        assert_eq!(response.objects_total_count, 1);
        assert_eq!(response.versions_total_count, 1);
        assert_eq!(response.delete_markers_total_count, 1);
        assert_eq!(response.objects_total_size, 10);
        assert_eq!(
            response.buckets_usage.get("bucket-a").map(|usage| (
                usage.objects_count,
                usage.versions_count,
                usage.delete_markers_count,
                usage.size
            )),
            Some((1, 1, 1, 10))
        );
    }

    #[tokio::test]
    #[serial]
    async fn authoritative_versioned_refresh_preserves_newer_dirty_memory() {
        clear_usage_memory_cache_for_test().await;

        let old_persisted = data_usage_info_for_test("bucket-a", 0, 0, SystemTime::now() - Duration::from_secs(10));
        replace_bucket_usage_memory_from_info(&old_persisted).await;
        let refresh_started = SystemTime::now() - Duration::from_secs(1);
        record_bucket_object_write_memory("bucket-a", None, 15).await;

        replace_bucket_usage_memory_from_authoritative(
            "bucket-a",
            BucketUsageInfo {
                objects_count: 1,
                versions_count: 1,
                delete_markers_count: 1,
                size: 10,
                ..Default::default()
            },
            refresh_started,
        )
        .await;

        let mut response = old_persisted.clone();
        apply_bucket_usage_memory_overlay(&mut response).await;

        assert_eq!(response.objects_total_count, 1);
        assert_eq!(response.versions_total_count, 1);
        assert_eq!(response.delete_markers_total_count, 0);
        assert_eq!(response.objects_total_size, 15);
        assert_eq!(
            response.buckets_usage.get("bucket-a").map(|usage| (
                usage.objects_count,
                usage.versions_count,
                usage.delete_markers_count,
                usage.size
            )),
            Some((1, 1, 0, 15))
        );
    }

    #[tokio::test]
    #[serial]
    async fn scanner_sync_preserves_dirty_delete_marker_with_later_snapshot() {
        clear_usage_memory_cache_for_test().await;

        let now = SystemTime::now();
        let old_persisted = data_usage_info_for_test("bucket-a", 2, 30, now - Duration::from_secs(10));
        replace_bucket_usage_memory_from_info(&old_persisted).await;
        record_bucket_delete_marker_memory("bucket-a").await;

        let scanner_without_marker = data_usage_info_for_test("bucket-a", 2, 30, now + Duration::from_secs(10));
        replace_bucket_usage_memory_from_info(&scanner_without_marker).await;

        let mut response = scanner_without_marker.clone();
        apply_bucket_usage_memory_overlay(&mut response).await;

        assert_eq!(response.objects_total_count, 2);
        assert_eq!(response.versions_total_count, 2);
        assert_eq!(response.delete_markers_total_count, 1);
        assert_eq!(
            response
                .buckets_usage
                .get("bucket-a")
                .map(|usage| { (usage.objects_count, usage.versions_count, usage.delete_markers_count, usage.size,) }),
            Some((2, 2, 1, 30))
        );
    }

    #[tokio::test]
    #[serial]
    async fn memory_overlay_does_not_replace_newer_persisted_usage() {
        clear_usage_memory_cache_for_test().await;

        let now = SystemTime::now();
        let old_persisted = data_usage_info_for_test("bucket-a", 1, 42, now - Duration::from_secs(10));
        replace_bucket_usage_memory_from_info(&old_persisted).await;
        record_bucket_object_delete_memory("bucket-a", 42, true).await;

        let mut newer_persisted = data_usage_info_for_test("bucket-a", 2, 84, now + Duration::from_secs(10));
        apply_bucket_usage_memory_overlay(&mut newer_persisted).await;

        assert_eq!(newer_persisted.objects_total_count, 2);
        assert_eq!(newer_persisted.objects_total_size, 84);
        assert_eq!(
            newer_persisted
                .buckets_usage
                .get("bucket-a")
                .map(|usage| (usage.objects_count, usage.size)),
            Some((2, 84))
        );
    }

    #[tokio::test]
    #[serial]
    async fn scanner_sync_preserves_newer_memory_update() {
        clear_usage_memory_cache_for_test().await;

        let now = SystemTime::now();
        let old_persisted = data_usage_info_for_test("bucket-a", 1, 42, now - Duration::from_secs(10));
        replace_bucket_usage_memory_from_info(&old_persisted).await;
        record_bucket_object_delete_memory("bucket-a", 42, true).await;

        let scanner_snapshot = data_usage_info_for_test("bucket-a", 1, 42, now - Duration::from_secs(5));
        replace_bucket_usage_memory_from_info(&scanner_snapshot).await;

        let mut response = scanner_snapshot.clone();
        apply_bucket_usage_memory_overlay(&mut response).await;

        assert_eq!(response.objects_total_count, 0);
        assert_eq!(response.objects_total_size, 0);
        assert_eq!(
            response
                .buckets_usage
                .get("bucket-a")
                .map(|usage| (usage.objects_count, usage.size)),
            Some((0, 0))
        );
    }

    #[tokio::test]
    #[serial]
    async fn scanner_sync_preserves_dirty_memory_update_with_later_partial_snapshot() {
        clear_usage_memory_cache_for_test().await;

        let now = SystemTime::now();
        let old_persisted = data_usage_info_for_test("bucket-a", 900, 9_000, now - Duration::from_secs(10));
        replace_bucket_usage_memory_from_info(&old_persisted).await;

        for _ in 0..100 {
            record_bucket_object_write_memory("bucket-a", None, 10).await;
        }

        let scanner_partial = data_usage_info_for_test("bucket-a", 950, 9_500, now + Duration::from_secs(10));
        replace_bucket_usage_memory_from_info(&scanner_partial).await;

        let mut response = scanner_partial.clone();
        apply_bucket_usage_memory_overlay(&mut response).await;

        assert_eq!(response.objects_total_count, 1000);
        assert_eq!(response.objects_total_size, 10_000);
        assert_eq!(
            response
                .buckets_usage
                .get("bucket-a")
                .map(|usage| (usage.objects_count, usage.size)),
            Some((1000, 10_000))
        );
    }

    // --- CompressionTotalState tests ---

    /// Reset the compression total cache to a known state for isolated tests.
    async fn reset_compression_cache(state: CompressionTotalState) {
        *COMPRESSION_TOTAL_MEMORY_CACHE.write().await = Some(state);
    }

    #[test]
    fn compression_state_default_values() {
        let state = CompressionTotalState::default();
        assert!(!state.inited, "default state should not be inited");
        assert_eq!(state.ops_since_persist, 0);
        assert_eq!(state.info.original_bytes_total, 0);
        assert_eq!(state.info.compressed_bytes_total, 0);
        assert_eq!(state.info.compression_operations_total, 0);
    }

    #[tokio::test]
    #[serial]
    async fn record_compression_skips_when_not_inited() {
        let mut pre_state = CompressionTotalState::default();
        // Set known values that should remain unchanged when inited=false
        pre_state.info.original_bytes_total = 42;
        pre_state.info.compressed_bytes_total = 21;
        pre_state.info.compression_operations_total = 7;
        pre_state.ops_since_persist = 5;
        // inited is already false from default()

        reset_compression_cache(pre_state.clone()).await;

        // Call with sizes that would be accumulated if inited were true
        record_compression_total_memory(100, 50).await;

        let guard = COMPRESSION_TOTAL_MEMORY_CACHE.read().await;
        let state = guard.as_ref().expect("cache should contain a state");
        assert!(!state.inited);
        assert_eq!(state.info.original_bytes_total, 42, "original should be unchanged");
        assert_eq!(state.info.compressed_bytes_total, 21, "compressed should be unchanged");
        assert_eq!(state.info.compression_operations_total, 7, "operations should be unchanged");
        assert_eq!(state.ops_since_persist, 5, "ops_since_persist should be unchanged");
    }

    #[tokio::test]
    #[serial]
    async fn record_compression_accumulates_totals() {
        let state = CompressionTotalState {
            info: CompressionTotalInfo::default(),
            ops_since_persist: 0,
            last_persist: tokio::time::Instant::now(),
            inited: true,
        };

        reset_compression_cache(state).await;

        for _ in 0..3 {
            record_compression_total_memory(100, 50).await;
        }

        let guard = COMPRESSION_TOTAL_MEMORY_CACHE.read().await;
        let state = guard.as_ref().expect("cache should contain a state");
        assert!(state.inited);
        assert_eq!(state.info.original_bytes_total, 300);
        assert_eq!(state.info.compressed_bytes_total, 150);
        assert_eq!(state.info.compression_operations_total, 3);
        assert_eq!(state.ops_since_persist, 3);
    }

    #[tokio::test]
    #[serial]
    async fn record_compression_triggers_persist_on_batch_full() {
        // Simulate 99 previous records, so the next (100th) hits the batch threshold.
        let state = CompressionTotalState {
            info: CompressionTotalInfo {
                original_bytes_total: 9900,
                compressed_bytes_total: 4950,
                compression_operations_total: 99,
            },
            ops_since_persist: 99,
            last_persist: tokio::time::Instant::now(),
            inited: true,
        };

        reset_compression_cache(state).await;

        // 100th record — should trigger the debounce flush.
        record_compression_total_memory(100, 50).await;

        let guard = COMPRESSION_TOTAL_MEMORY_CACHE.read().await;
        let state = guard.as_ref().expect("cache should contain a state");

        // Totals are correctly accumulated (not reset by the flush).
        assert_eq!(state.info.original_bytes_total, 10_000); // 9900 + 100
        assert_eq!(state.info.compressed_bytes_total, 5_000); // 4950 + 50
        assert_eq!(state.info.compression_operations_total, 100); // 99 + 1

        // Debounce counter is reset after the persist trigger.
        assert_eq!(state.ops_since_persist, 0);

        // last_persist was refreshed; it should be very recent.
        assert!(
            state.last_persist.elapsed() < Duration::from_secs(3),
            "last_persist should have been refreshed to now"
        );

        // try_flush_compression_total is spawned asynchronously here.
        // In a unit test, runtime_sources::object_store_handle() returns None,
        // so the spawned task will hit the "object store not initialized" error
        // path and return gracefully. The state mutation (counter reset + totals
        // accumulation) is the critical behaviour verified above.
    }
}
