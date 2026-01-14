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

pub mod local_snapshot;

use crate::{
    bucket::metadata_sys::get_replication_config, config::com::read_config, disk::DiskAPI, error::Error, store::ECStore,
    store_api::StorageAPI,
};
pub use local_snapshot::{
    DATA_USAGE_DIR, DATA_USAGE_STATE_DIR, LOCAL_USAGE_SNAPSHOT_VERSION, LocalUsageSnapshot, LocalUsageSnapshotMeta,
    data_usage_dir, data_usage_state_dir, ensure_data_usage_layout, read_snapshot as read_local_snapshot, snapshot_file_name,
    snapshot_object_path, snapshot_path, write_snapshot as write_local_snapshot,
};
use rustfs_common::data_usage::{
    BucketTargetUsageInfo, BucketUsageInfo, DataUsageCache, DataUsageEntry, DataUsageInfo, DiskUsageStatus, SizeSummary,
};
use rustfs_utils::path::SLASH_SEPARATOR_STR;
use std::{
    collections::{HashMap, hash_map::Entry},
    sync::{Arc, OnceLock},
    time::{Duration, SystemTime},
};
use tokio::fs;
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

// Data usage storage constants
pub const DATA_USAGE_ROOT: &str = SLASH_SEPARATOR_STR;
const DATA_USAGE_OBJ_NAME: &str = ".usage.json";
const DATA_USAGE_BLOOM_NAME: &str = ".bloomcycle.bin";
pub const DATA_USAGE_CACHE_NAME: &str = ".usage-cache.bin";
const DATA_USAGE_CACHE_TTL_SECS: u64 = 30;

type UsageMemoryCache = Arc<RwLock<HashMap<String, (u64, SystemTime)>>>;
type CacheUpdating = Arc<RwLock<bool>>;

static USAGE_MEMORY_CACHE: OnceLock<UsageMemoryCache> = OnceLock::new();
static USAGE_CACHE_UPDATING: OnceLock<CacheUpdating> = OnceLock::new();

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
        SLASH_SEPARATOR_STR,
        crate::disk::BUCKET_META_PREFIX
    );
    pub static ref DATA_USAGE_OBJ_NAME_PATH: String = format!("{}{}{}",
        crate::disk::BUCKET_META_PREFIX,
        SLASH_SEPARATOR_STR,
        DATA_USAGE_OBJ_NAME
    );
    pub static ref DATA_USAGE_BLOOM_NAME_PATH: String = format!("{}{}{}",
        crate::disk::BUCKET_META_PREFIX,
        SLASH_SEPARATOR_STR,
        DATA_USAGE_BLOOM_NAME
    );
}

/// Store data usage info to backend storage
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

    let data =
        serde_json::to_vec(&data_usage_info).map_err(|e| Error::other(format!("Failed to serialize data usage info: {e}")))?;

    // Save to backend using the same mechanism as original code
    crate::config::com::save_config(store, &DATA_USAGE_OBJ_NAME_PATH, data)
        .await
        .map_err(Error::other)?;

    Ok(())
}

/// Load data usage info from backend storage
pub async fn load_data_usage_from_backend(store: Arc<ECStore>) -> Result<DataUsageInfo, Error> {
    let buf: Vec<u8> = match read_config(store.clone(), &DATA_USAGE_OBJ_NAME_PATH).await {
        Ok(data) => data,
        Err(e) => {
            error!("Failed to read data usage info from backend: {}", e);
            if e == Error::ConfigNotFound {
                info!("Data usage config not found, building basic statistics");
                return build_basic_data_usage_info(store).await;
            }
            return Err(Error::other(e));
        }
    };

    let mut data_usage_info: DataUsageInfo =
        serde_json::from_slice(&buf).map_err(|e| Error::other(format!("Failed to deserialize data usage info: {e}")))?;

    info!("Loaded data usage info from backend with {} buckets", data_usage_info.buckets_count);

    // Validate data and supplement if empty
    if data_usage_info.buckets_count == 0 || data_usage_info.buckets_usage.is_empty() {
        warn!("Loaded data is empty, supplementing with basic statistics");
        if let Ok(basic_info) = build_basic_data_usage_info(store.clone()).await {
            data_usage_info.buckets_count = basic_info.buckets_count;
            data_usage_info.buckets_usage = basic_info.buckets_usage;
            data_usage_info.bucket_sizes = basic_info.bucket_sizes;
            data_usage_info.objects_total_count = basic_info.objects_total_count;
            data_usage_info.objects_total_size = basic_info.objects_total_size;
            data_usage_info.last_update = basic_info.last_update;
        }
    }

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
    let mut continuation: Option<String> = None;
    let mut objects_count: u64 = 0;
    let mut versions_count: u64 = 0;
    let mut total_size: u64 = 0;
    let mut delete_markers: u64 = 0;

    loop {
        let result = store
            .clone()
            .list_objects_v2(
                bucket_name,
                "", // prefix
                continuation.clone(),
                None,  // delimiter
                1000,  // max_keys
                false, // fetch_owner
                None,  // start_after
                false, // incl_deleted
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
            objects_count = objects_count.saturating_add(1);
            total_size = total_size.saturating_add(object_size);

            let detected_versions = if object.num_versions > 0 {
                object.num_versions as u64
            } else {
                1
            };
            versions_count = versions_count.saturating_add(detected_versions);
        }

        if !result.is_truncated {
            break;
        }

        continuation = result.next_continuation_token.clone();
        if continuation.is_none() {
            info!(
                "Bucket {} listing marked truncated but no continuation token returned; stopping early",
                bucket_name
            );
            break;
        }
    }

    if versions_count == 0 {
        versions_count = objects_count;
    }

    let usage = BucketUsageInfo {
        size: total_size,
        objects_count,
        versions_count,
        delete_markers_count: delete_markers,
        ..Default::default()
    };

    Ok(usage)
}

/// Fast in-memory increment for immediate quota consistency
pub async fn increment_bucket_usage_memory(bucket: &str, size_increment: u64) {
    let mut cache = memory_cache().write().await;
    let current = cache.entry(bucket.to_string()).or_insert_with(|| (0, SystemTime::now()));
    current.0 += size_increment;
    current.1 = SystemTime::now();
}

/// Fast in-memory decrement for immediate quota consistency
pub async fn decrement_bucket_usage_memory(bucket: &str, size_decrement: u64) {
    let mut cache = memory_cache().write().await;
    if let Some(current) = cache.get_mut(bucket) {
        current.0 = current.0.saturating_sub(size_decrement);
        current.1 = SystemTime::now();
    }
}

/// Get bucket usage from in-memory cache
pub async fn get_bucket_usage_memory(bucket: &str) -> Option<u64> {
    update_usage_cache_if_needed().await;

    let cache = memory_cache().read().await;
    cache.get(bucket).map(|(usage, _)| *usage)
}

async fn update_usage_cache_if_needed() {
    let ttl = Duration::from_secs(DATA_USAGE_CACHE_TTL_SECS);
    let double_ttl = ttl * 2;
    let now = SystemTime::now();

    let cache = memory_cache().read().await;
    let earliest_timestamp = cache.values().map(|(_, ts)| *ts).min();
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

        let cache_clone = (*memory_cache()).clone();
        let updating_clone = (*cache_updating()).clone();
        tokio::spawn(async move {
            if let Some(store) = crate::global::GLOBAL_OBJECT_API.get()
                && let Ok(data_usage_info) = load_data_usage_from_backend(store.clone()).await
            {
                let mut cache = cache_clone.write().await;
                for (bucket_name, bucket_usage) in data_usage_info.buckets_usage.iter() {
                    cache.insert(bucket_name.clone(), (bucket_usage.size, SystemTime::now()));
                }
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

    if let Some(store) = crate::global::GLOBAL_OBJECT_API.get()
        && let Ok(data_usage_info) = load_data_usage_from_backend(store.clone()).await
    {
        let mut cache = memory_cache().write().await;
        for (bucket_name, bucket_usage) in data_usage_info.buckets_usage.iter() {
            cache.insert(bucket_name.clone(), (bucket_usage.size, SystemTime::now()));
        }
    }

    let mut updating = cache_updating().write().await;
    *updating = false;
}

/// Sync memory cache with backend data (called by scanner)
pub async fn sync_memory_cache_with_backend() -> Result<(), Error> {
    if let Some(store) = crate::global::GLOBAL_OBJECT_API.get() {
        match load_data_usage_from_backend(store.clone()).await {
            Ok(data_usage_info) => {
                let mut cache = memory_cache().write().await;
                for (bucket, bucket_usage) in data_usage_info.buckets_usage.iter() {
                    cache.insert(bucket.clone(), (bucket_usage.size, SystemTime::now()));
                }
            }
            Err(e) => {
                debug!("Failed to sync memory cache with backend: {}", e);
            }
        }
    }
    Ok(())
}

/// Build basic data usage info with real object counts
pub async fn build_basic_data_usage_info(store: Arc<ECStore>) -> Result<DataUsageInfo, Error> {
    let mut data_usage_info = DataUsageInfo::default();

    // Get bucket list
    match store.list_bucket(&crate::store_api::BucketOptions::default()).await {
        Ok(buckets) => {
            data_usage_info.buckets_count = buckets.len() as u64;
            data_usage_info.last_update = Some(SystemTime::now());

            let mut total_objects = 0u64;
            let mut total_versions = 0u64;
            let mut total_size = 0u64;
            let mut total_delete_markers = 0u64;

            for bucket_info in buckets {
                if bucket_info.name.starts_with('.') {
                    continue; // Skip system buckets
                }

                match compute_bucket_usage(store.clone(), &bucket_info.name).await {
                    Ok(bucket_usage) => {
                        total_objects = total_objects.saturating_add(bucket_usage.objects_count);
                        total_versions = total_versions.saturating_add(bucket_usage.versions_count);
                        total_size = total_size.saturating_add(bucket_usage.size);
                        total_delete_markers = total_delete_markers.saturating_add(bucket_usage.delete_markers_count);

                        data_usage_info
                            .buckets_usage
                            .insert(bucket_info.name.clone(), bucket_usage.clone());
                        data_usage_info.bucket_sizes.insert(bucket_info.name, bucket_usage.size);
                    }
                    Err(e) => {
                        warn!("Failed to compute bucket usage for {}: {}", bucket_info.name, e);
                    }
                }
            }

            data_usage_info.objects_total_count = total_objects;
            data_usage_info.versions_total_count = total_versions;
            data_usage_info.objects_total_size = total_size;
            data_usage_info.delete_markers_total_count = total_delete_markers;
        }
        Err(e) => {
            warn!("Failed to list buckets for basic data usage info: {}", e);
        }
    }

    Ok(data_usage_info)
}

/// Create a data usage cache entry from size summary
pub fn create_cache_entry_from_summary(summary: &SizeSummary) -> DataUsageEntry {
    let mut entry = DataUsageEntry::default();
    entry.add_sizes(summary);
    entry
}

/// Convert data usage cache to DataUsageInfo
pub fn cache_to_data_usage_info(cache: &DataUsageCache, path: &str, buckets: &[crate::store_api::BucketInfo]) -> DataUsageInfo {
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
    use crate::store_api::{ObjectIO, ObjectOptions};
    use http::HeaderMap;
    use rand::Rng;
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

pub async fn save_data_usage_cache(cache: &DataUsageCache, name: &str) -> crate::error::Result<()> {
    use crate::config::com::save_config;
    use crate::disk::BUCKET_META_PREFIX;
    use crate::new_object_layer_fn;
    use std::path::Path;

    let Some(store) = new_object_layer_fn() else {
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

#[cfg(test)]
mod tests {
    use super::*;
    use rustfs_common::data_usage::BucketUsageInfo;

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
        let mut good_snapshot = LocalUsageSnapshot::new(LocalUsageSnapshotMeta {
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
}
