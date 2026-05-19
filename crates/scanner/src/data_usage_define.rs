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

use s3s::dto::BucketLifecycleConfiguration;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    future::Future,
    sync::{Arc, LazyLock, Once},
    time::SystemTime,
};

use http::HeaderMap;
use metrics::{counter, describe_counter, describe_histogram, histogram};
use rustfs_config::ENV_SCANNER_CACHE_SAVE_TIMEOUT_SECS;
pub use rustfs_data_usage::{
    BucketTargetUsageInfo, BucketUsageInfo, DataUsageEntry, DataUsageHash, DataUsageHashMap, DataUsageInfo, hash_path,
};
use rustfs_data_usage::{DataUsageCache as SharedDataUsageCache, DataUsageCacheInfo as SharedDataUsageCacheInfo};
use rustfs_ecstore::{
    StorageAPI,
    bucket::{lifecycle::lifecycle::TRANSITION_COMPLETE, replication::ReplicationConfig},
    config::{com::save_config, storageclass},
    disk::{BUCKET_META_PREFIX, RUSTFS_META_BUCKET},
    error::{Error, Result as StorageResult, StorageError},
    store_api::{ObjectInfo, ObjectOptions},
};
use rustfs_utils::path::{SLASH_SEPARATOR, path_join_buf};
use tokio::time::{Duration, Instant, sleep, timeout};
use tracing::warn;

// Data usage constants
pub const DATA_USAGE_ROOT: &str = SLASH_SEPARATOR;

const DATA_USAGE_OBJ_NAME: &str = ".usage.json";

const DATA_USAGE_BLOOM_NAME: &str = ".bloomcycle.bin";

pub const DATA_USAGE_CACHE_NAME: &str = ".usage-cache.bin";
const DATA_USAGE_CACHE_SAVE_TIMEOUT_SECS_DEFAULT: u64 = 30;
const DATA_USAGE_CACHE_SAVE_RETRIES: u32 = 2;
const DATA_USAGE_CACHE_BACKUP_SAVE_TIMEOUT_SECS_MAX: u64 = 5;
const DATA_USAGE_CACHE_BACKUP_SAVE_RETRIES: u32 = 0;
const METRIC_CACHE_SAVE_ATTEMPT_TOTAL: &str = "rustfs_scanner_cache_save_attempt_total";
const METRIC_CACHE_SAVE_TIMEOUT_TOTAL: &str = "rustfs_scanner_cache_save_timeout_total";
const METRIC_CACHE_SAVE_RETRY_TOTAL: &str = "rustfs_scanner_cache_save_retry_total";
const METRIC_CACHE_SAVE_DURATION_SECONDS: &str = "rustfs_scanner_cache_save_duration_seconds";
static CACHE_SAVE_METRICS_ONCE: Once = Once::new();

// Data usage paths (computed at runtime)
pub static DATA_USAGE_BUCKET: LazyLock<String> =
    LazyLock::new(|| format!("{RUSTFS_META_BUCKET}{SLASH_SEPARATOR}{BUCKET_META_PREFIX}"));

pub static DATA_USAGE_OBJ_NAME_PATH: LazyLock<String> =
    LazyLock::new(|| format!("{BUCKET_META_PREFIX}{SLASH_SEPARATOR}{DATA_USAGE_OBJ_NAME}"));

pub static DATA_USAGE_BLOOM_NAME_PATH: LazyLock<String> =
    LazyLock::new(|| format!("{BUCKET_META_PREFIX}{SLASH_SEPARATOR}{DATA_USAGE_BLOOM_NAME}"));

pub static BACKGROUND_HEAL_INFO_PATH: LazyLock<String> =
    LazyLock::new(|| format!("{BUCKET_META_PREFIX}{SLASH_SEPARATOR}.background-heal.json"));

#[derive(Clone, Copy, Default, Debug, Serialize, Deserialize, PartialEq)]
pub struct TierStats {
    pub total_size: u64,
    pub num_versions: i32,
    pub num_objects: i32,
}

impl TierStats {
    pub fn add(&self, u: &TierStats) -> TierStats {
        TierStats {
            total_size: self.total_size + u.total_size,
            num_versions: self.num_versions + u.num_versions,
            num_objects: self.num_objects + u.num_objects,
        }
    }

    pub fn from_object_info(oi: &ObjectInfo) -> Self {
        TierStats {
            total_size: oi.size as u64,
            num_versions: 1,
            num_objects: if oi.is_latest { 1 } else { 0 },
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct AllTierStats {
    pub tiers: HashMap<String, TierStats>,
}

impl AllTierStats {
    pub fn new() -> Self {
        Self { tiers: HashMap::new() }
    }

    pub fn add_sizes(&mut self, tiers: HashMap<String, TierStats>) {
        for (tier, st) in tiers {
            self.tiers
                .insert(tier.clone(), self.tiers.get(&tier).copied().unwrap_or_default().add(&st));
        }
    }

    pub fn merge(&mut self, other: AllTierStats) {
        for (tier, st) in other.tiers {
            self.tiers
                .insert(tier.clone(), self.tiers.get(&tier).copied().unwrap_or_default().add(&st));
        }
    }

    pub fn populate_stats(&self, stats: &mut HashMap<String, TierStats>) {
        for (tier, st) in &self.tiers {
            stats.insert(
                tier.clone(),
                TierStats {
                    total_size: st.total_size,
                    num_versions: st.num_versions,
                    num_objects: st.num_objects,
                },
            );
        }
    }
}

/// Size summary for a single object or group of objects
#[derive(Debug, Default, Clone)]
pub struct SizeSummary {
    /// Total size
    pub total_size: usize,
    /// Number of versions
    pub versions: usize,
    /// Number of delete markers
    pub delete_markers: usize,
    /// Replicated size
    pub replicated_size: i64,
    /// Replicated count
    pub replicated_count: usize,
    /// Pending size
    pub pending_size: i64,
    /// Failed size
    pub failed_size: i64,
    /// Replica size
    pub replica_size: i64,
    /// Replica count
    pub replica_count: usize,
    /// Pending count
    pub pending_count: usize,
    /// Failed count
    pub failed_count: usize,
    /// Replication target stats
    pub repl_target_stats: HashMap<String, ReplTargetSizeSummary>,
    pub tier_stats: HashMap<String, TierStats>,
}

impl SizeSummary {
    pub fn actions_accounting(&mut self, oi: &ObjectInfo, size: i64, actual_size: i64) {
        if oi.delete_marker {
            self.delete_markers += 1;
        }

        if oi.version_id.is_some_and(|v| !v.is_nil()) && size == actual_size {
            self.versions += 1;
        }

        self.total_size += if size > 0 { size as usize } else { 0 };

        if oi.delete_marker || oi.transitioned_object.free_version {
            return;
        }

        let mut tier = oi.storage_class.clone().unwrap_or_else(|| storageclass::STANDARD.to_string());
        if oi.transitioned_object.status == TRANSITION_COMPLETE {
            tier = oi.transitioned_object.tier.clone();
        }

        if let Some(tier_stats) = self.tier_stats.get_mut(&tier) {
            tier_stats.add(&TierStats::from_object_info(oi));
        }
    }
}

/// Replication target size summary
#[derive(Debug, Default, Clone)]
pub struct ReplTargetSizeSummary {
    /// Replicated size
    pub replicated_size: i64,
    /// Replicated count
    pub replicated_count: usize,
    /// Pending size
    pub pending_size: i64,
    /// Failed size
    pub failed_size: i64,
    /// Pending count
    pub pending_count: usize,
    /// Failed count
    pub failed_count: usize,
}

// ===== Cache-related data structures =====

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct DataUsageEntryInfo {
    pub name: String,
    pub parent: String,
    pub entry: DataUsageEntry,
}

/// Data usage cache info
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct DataUsageCacheInfo {
    pub name: String,
    pub next_cycle: u64,
    pub last_update: Option<SystemTime>,
    pub skip_healing: bool,
    pub lifecycle: Option<Arc<BucketLifecycleConfiguration>>,
    pub replication: Option<Arc<ReplicationConfig>>,
    #[serde(default)]
    pub failed_objects: HashMap<String, u64>,
}

/// Data usage cache
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct DataUsageCache {
    pub info: DataUsageCacheInfo,
    pub cache: HashMap<String, DataUsageEntry>,
}

impl DataUsageCache {
    fn as_shared(&self) -> SharedDataUsageCache {
        SharedDataUsageCache {
            info: SharedDataUsageCacheInfo {
                name: self.info.name.clone(),
                next_cycle: self.info.next_cycle,
                last_update: self.info.last_update,
                skip_healing: self.info.skip_healing,
                failed_objects: self.info.failed_objects.clone(),
            },
            cache: self.cache.clone(),
        }
    }

    fn apply_shared_state(&mut self, shared: SharedDataUsageCache) {
        self.info.name = shared.info.name;
        self.info.next_cycle = shared.info.next_cycle;
        self.info.last_update = shared.info.last_update;
        self.info.skip_healing = shared.info.skip_healing;
        self.info.failed_objects = shared.info.failed_objects;
        self.cache = shared.cache;
    }

    fn ensure_cache_save_metrics_registered() {
        CACHE_SAVE_METRICS_ONCE.call_once(|| {
            describe_counter!(
                METRIC_CACHE_SAVE_ATTEMPT_TOTAL,
                "Total scanner data usage cache save attempts by result and cache type."
            );
            describe_counter!(
                METRIC_CACHE_SAVE_TIMEOUT_TOTAL,
                "Total scanner data usage cache save timeouts by cache type."
            );
            describe_counter!(
                METRIC_CACHE_SAVE_RETRY_TOTAL,
                "Total scanner data usage cache save retries by cache type."
            );
            describe_histogram!(
                METRIC_CACHE_SAVE_DURATION_SECONDS,
                "Duration of scanner data usage cache save attempts in seconds."
            );
        });
    }

    fn cache_path_type(path: &str) -> &'static str {
        if path.ends_with(".bkp") { "backup" } else { "main" }
    }

    pub fn replace(&mut self, path: &str, parent: &str, e: DataUsageEntry) {
        let mut shared = self.as_shared();
        shared.replace(path, parent, e);
        self.apply_shared_state(shared);
    }

    pub fn replace_hashed(&mut self, hash: &DataUsageHash, parent: &Option<DataUsageHash>, e: &DataUsageEntry) {
        let mut shared = self.as_shared();
        shared.replace_hashed(hash, parent, e);
        self.apply_shared_state(shared);
    }

    pub fn find(&self, path: &str) -> Option<&DataUsageEntry> {
        self.cache.get(&hash_path(path).key())
    }

    pub fn find_children_copy(&mut self, h: DataUsageHash) -> DataUsageHashMap {
        let mut shared = self.as_shared();
        let children = shared.find_children_copy(h);
        self.apply_shared_state(shared);
        children
    }

    pub fn flatten(&self, root: &DataUsageEntry) -> DataUsageEntry {
        self.as_shared().flatten(root)
    }

    pub fn copy_with_children(&mut self, src: &DataUsageCache, hash: &DataUsageHash, parent: &Option<DataUsageHash>) {
        let mut shared = self.as_shared();
        shared.copy_with_children(&src.as_shared(), hash, parent);
        self.apply_shared_state(shared);
    }

    pub fn delete_recursive(&mut self, hash: &DataUsageHash) {
        let mut shared = self.as_shared();
        shared.delete_recursive(hash);
        self.apply_shared_state(shared);
    }

    pub fn size_recursive(&self, path: &str) -> Option<DataUsageEntry> {
        self.as_shared().size_recursive(path)
    }

    pub fn search_parent(&self, hash: &DataUsageHash) -> Option<DataUsageHash> {
        self.as_shared().search_parent(hash)
    }

    pub fn is_compacted(&self, hash: &DataUsageHash) -> bool {
        self.as_shared().is_compacted(hash)
    }

    pub fn force_compact(&mut self, limit: usize) {
        let mut shared = self.as_shared();
        shared.force_compact(limit);
        self.apply_shared_state(shared);
    }

    pub fn reduce_children_of(&mut self, path: &DataUsageHash, limit: usize, compact_self: bool) {
        let mut shared = self.as_shared();
        shared.reduce_children_of(path, limit, compact_self);
        self.apply_shared_state(shared);
    }

    pub fn total_children_rec(&self, path: &str) -> usize {
        self.as_shared().total_children_rec(path)
    }

    pub fn merge(&mut self, o: &DataUsageCache) {
        let mut shared = self.as_shared();
        shared.merge(&o.as_shared());
        self.apply_shared_state(shared);
    }

    pub fn root_hash(&self) -> DataUsageHash {
        self.as_shared().root_hash()
    }

    pub fn root(&self) -> Option<DataUsageEntry> {
        self.as_shared().root()
    }

    /// Convert cache to DataUsageInfo for a specific path
    pub fn dui(&self, path: &str, buckets: &[String]) -> DataUsageInfo {
        self.as_shared().dui(path, buckets)
    }

    pub fn marshal_msg(&self) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
        let mut buf = Vec::new();
        self.serialize(&mut rmp_serde::Serializer::new(&mut buf))?;
        Ok(buf)
    }

    pub fn unmarshal(buf: &[u8]) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let t: Self = rmp_serde::from_slice(buf)?;
        Ok(t)
    }

    /// Only backend errors are returned as errors.
    /// The loader is optimistic and has no locking, but tries 5 times before giving up.
    /// If the object is not found, a nil error with empty data usage cache is returned.
    pub async fn load<S: StorageAPI>(&mut self, store: Arc<S>, name: &str) -> StorageResult<()> {
        // By default, empty data usage cache
        *self = DataUsageCache::default();

        // Caches are read+written without locks
        let mut retries = 0;
        while retries < 5 {
            let (should_retry, cache_opt, result) = Self::try_load_inner(store.clone(), name, Duration::from_secs(60)).await;
            result?;
            if let Some(cache) = cache_opt {
                *self = cache;
                return Ok(());
            }
            if !should_retry {
                break;
            }

            // Try backup file
            let backup_name = format!("{name}.bkp");
            let (backup_retry, backup_cache_opt, backup_result) =
                Self::try_load_inner(store.clone(), &backup_name, Duration::from_secs(30)).await;
            if backup_result.is_err() {
                // Error loading backup, continue retry
            } else if let Some(cache) = backup_cache_opt {
                // Only return when we have valid data from the backup
                *self = cache;
                return Ok(());
            } else if !backup_retry {
                // Backup not found and not retryable
                break;
            }

            retries += 1;
            // Random sleep between 0 and 1 second
            let sleep_ms: u64 = rand::random::<u64>() % 1000;
            sleep(Duration::from_millis(sleep_ms)).await;
        }

        if retries == 5 {
            warn!("maximum retry reached to load the data usage cache `{}`", name);
        }

        Ok(())
    }
    // Inner load function that attempts to load from a specific path
    // Returns (should_retry, cache_option, error_option)
    async fn try_load_inner<S: StorageAPI>(
        store: Arc<S>,
        load_name: &str,
        timeout_duration: Duration,
    ) -> (bool, Option<DataUsageCache>, StorageResult<()>) {
        // Abandon if more than time.Minute, so we don't hold up scanner.
        // drive timeout by default is 2 minutes, we do not need to wait longer.
        let load_fut = async {
            // First try: RUSTFS_META_BUCKET + BUCKET_META_PREFIX/name
            let path = path_join_buf(&[BUCKET_META_PREFIX, load_name]);
            match store
                .get_object_reader(
                    RUSTFS_META_BUCKET,
                    &path,
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
                    match reader.read_all().await {
                        Ok(data) => {
                            match DataUsageCache::unmarshal(&data) {
                                Ok(cache) => Ok(Some(cache)),
                                Err(_) => {
                                    // Deserialization failed, but we got data
                                    Ok(None)
                                }
                            }
                        }
                        Err(e) => {
                            // Read error
                            Err(e)
                        }
                    }
                }
                Err(err) => {
                    match err {
                        Error::FileNotFound | Error::VolumeNotFound | Error::ObjectNotFound(_, _) | Error::BucketNotFound(_) => {
                            // Try second location: DATA_USAGE_BUCKET/name
                            match store
                                .get_object_reader(
                                    &DATA_USAGE_BUCKET,
                                    load_name,
                                    None,
                                    HeaderMap::new(),
                                    &ObjectOptions {
                                        no_lock: true,
                                        ..Default::default()
                                    },
                                )
                                .await
                            {
                                Ok(mut reader) => match reader.read_all().await {
                                    Ok(data) => match DataUsageCache::unmarshal(&data) {
                                        Ok(cache) => Ok(Some(cache)),
                                        Err(_) => Ok(None),
                                    },
                                    Err(e) => Err(e),
                                },
                                Err(inner_err) => match inner_err {
                                    Error::FileNotFound
                                    | Error::VolumeNotFound
                                    | Error::ObjectNotFound(_, _)
                                    | Error::BucketNotFound(_) => {
                                        // Object not found in both locations
                                        Ok(None)
                                    }
                                    Error::ErasureReadQuorum => {
                                        // InsufficientReadQuorum - retry
                                        Ok(None)
                                    }
                                    _ => {
                                        // Other storage errors - retry
                                        if matches!(
                                            inner_err,
                                            Error::FaultyDisk | Error::DiskFull | Error::StorageFull | Error::SlowDown
                                        ) {
                                            return Ok(None);
                                        }
                                        Err(inner_err)
                                    }
                                },
                            }
                        }
                        Error::ErasureReadQuorum => {
                            // InsufficientReadQuorum - retry
                            Ok(None)
                        }
                        _ => {
                            // Other storage errors - retry
                            if matches!(err, Error::FaultyDisk | Error::DiskFull | Error::StorageFull | Error::SlowDown) {
                                return Ok(None);
                            }
                            Err(err)
                        }
                    }
                }
            }
        };

        match timeout(timeout_duration, load_fut).await {
            Ok(result) => match result {
                Ok(Some(cache)) => (false, Some(cache), Ok(())),
                Ok(None) => {
                    // Not found or deserialization failed - check if we should retry
                    // For now, we don't retry on not found
                    (false, None, Ok(()))
                }
                Err(e) => {
                    // Check if it's a retryable error
                    if matches!(
                        e,
                        Error::ErasureReadQuorum | Error::FaultyDisk | Error::DiskFull | Error::StorageFull | Error::SlowDown
                    ) {
                        (true, None, Ok(()))
                    } else {
                        (false, None, Err(e))
                    }
                }
            },
            Err(_) => {
                // Timeout - retry
                (true, None, Ok(()))
            }
        }
    }

    fn cache_save_timeout() -> Duration {
        Duration::from_secs(
            rustfs_utils::get_env_u64(ENV_SCANNER_CACHE_SAVE_TIMEOUT_SECS, DATA_USAGE_CACHE_SAVE_TIMEOUT_SECS_DEFAULT).max(1),
        )
    }

    fn backup_cache_save_timeout(timeout_duration: Duration) -> Duration {
        timeout_duration.min(Duration::from_secs(DATA_USAGE_CACHE_BACKUP_SAVE_TIMEOUT_SECS_MAX))
    }

    fn record_save_attempt(path_type: &'static str, result: &'static str, duration: Duration) {
        histogram!(METRIC_CACHE_SAVE_DURATION_SECONDS, "cache" => path_type).record(duration.as_secs_f64());
        counter!(
            METRIC_CACHE_SAVE_ATTEMPT_TOTAL,
            "cache" => path_type,
            "result" => result
        )
        .increment(1);
        if result == "timeout" {
            counter!(METRIC_CACHE_SAVE_TIMEOUT_TOTAL, "cache" => path_type).increment(1);
        }
    }

    async fn retry_save_op<F, Fut>(
        path_type: &'static str,
        timeout_duration: Duration,
        max_retries: u32,
        mut save_op: F,
    ) -> StorageResult<()>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = StorageResult<()>>,
    {
        let mut last_err: Option<StorageError> = None;

        for attempt in 0..=max_retries {
            let attempt_start = Instant::now();
            let timeout_res = timeout(timeout_duration, save_op()).await;
            let duration = attempt_start.elapsed();

            match timeout_res {
                Ok(Ok(())) => {
                    Self::record_save_attempt(path_type, "success", duration);
                    return Ok(());
                }
                Err(e) => {
                    Self::record_save_attempt(path_type, "timeout", duration);
                    last_err = Some(StorageError::other(format!("{e} after {timeout_duration:?}")));
                }
                Ok(Err(e)) => {
                    Self::record_save_attempt(path_type, "error", duration);
                    last_err = Some(e);
                }
            }

            if last_err.is_some() && attempt < max_retries {
                counter!(METRIC_CACHE_SAVE_RETRY_TOTAL, "cache" => path_type).increment(1);
                let backoff_ms = 50_u64 * (1_u64 << attempt) + (rand::random::<u64>() % 100);
                sleep(Duration::from_millis(backoff_ms)).await;
            }
        }

        Err(last_err.unwrap_or_else(|| StorageError::other("Failed to save data usage cache".to_string())))
    }

    async fn save_path_with_retry<S: StorageAPI>(
        store: Arc<S>,
        path: &str,
        buf: &[u8],
        timeout_duration: Duration,
        max_retries: u32,
    ) -> StorageResult<()> {
        Self::ensure_cache_save_metrics_registered();
        let path_type = Self::cache_path_type(path);
        let path = path.to_string();

        Self::retry_save_op(path_type, timeout_duration, max_retries, move || {
            let store_clone = store.clone();
            let path_clone = path.clone();
            let buf_clone = buf.to_vec();
            async move {
                save_config(store_clone, &path_clone, buf_clone).await?;
                Ok::<(), StorageError>(())
            }
        })
        .await
    }

    pub async fn save<S: StorageAPI>(&self, store: Arc<S>, name: &str) -> StorageResult<()> {
        let mut buf = Vec::new();
        self.serialize(&mut rmp_serde::Serializer::new(&mut buf))?;
        let timeout_duration = Self::cache_save_timeout();

        let path = path_join_buf(&[BUCKET_META_PREFIX, name]);
        Self::save_path_with_retry(store.clone(), &path, &buf, timeout_duration, DATA_USAGE_CACHE_SAVE_RETRIES).await?;

        let backup_name = format!("{name}.bkp");
        let backup_path = path_join_buf(&[BUCKET_META_PREFIX, &backup_name]);
        let backup_timeout_duration = Self::backup_cache_save_timeout(timeout_duration);
        if let Err(e) =
            Self::save_path_with_retry(store, &backup_path, &buf, backup_timeout_duration, DATA_USAGE_CACHE_BACKUP_SAVE_RETRIES)
                .await
        {
            warn!("Failed to save data usage cache backup: {e}");
        }
        Ok(())
    }
}

/// Trait for storage-specific operations on DataUsageCache
#[async_trait::async_trait]
pub trait DataUsageCacheStorage {
    /// Load data usage cache from backend storage
    async fn load(store: &dyn std::any::Any, name: &str) -> Result<Self, Box<dyn std::error::Error + Send + Sync>>
    where
        Self: Sized;

    /// Save data usage cache to backend storage
    async fn save(&self, name: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
}

impl SizeSummary {
    /// Create a new SizeSummary
    pub fn new() -> Self {
        Self::default()
    }

    /// Add another SizeSummary to this one
    pub fn add(&mut self, other: &SizeSummary) {
        self.total_size += other.total_size;
        self.versions += other.versions;
        self.delete_markers += other.delete_markers;
        self.replicated_size += other.replicated_size;
        self.replicated_count += other.replicated_count;
        self.pending_size += other.pending_size;
        self.failed_size += other.failed_size;
        self.replica_size += other.replica_size;
        self.replica_count += other.replica_count;
        self.pending_count += other.pending_count;
        self.failed_count += other.failed_count;

        // Merge replication target stats
        for (target, stats) in &other.repl_target_stats {
            let entry = self.repl_target_stats.entry(target.clone()).or_default();
            entry.replicated_size += stats.replicated_size;
            entry.replicated_count += stats.replicated_count;
            entry.pending_size += stats.pending_size;
            entry.failed_size += stats.failed_size;
            entry.pending_count += stats.pending_count;
            entry.failed_count += stats.failed_count;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use temp_env::{with_var, with_var_unset};

    #[test]
    fn test_data_usage_info_creation() {
        let mut info = DataUsageInfo::new();
        info.update_capacity(1000, 500, 500);

        assert_eq!(info.total_capacity, 1000);
        assert_eq!(info.total_used_capacity, 500);
        assert_eq!(info.total_free_capacity, 500);
        assert!(info.last_update.is_some());
    }

    #[test]
    fn test_bucket_usage_info_merge() {
        let mut usage1 = BucketUsageInfo::new();
        usage1.size = 100;
        usage1.objects_count = 10;
        usage1.versions_count = 5;

        let mut usage2 = BucketUsageInfo::new();
        usage2.size = 200;
        usage2.objects_count = 20;
        usage2.versions_count = 10;

        usage1.merge(&usage2);

        assert_eq!(usage1.size, 300);
        assert_eq!(usage1.objects_count, 30);
        assert_eq!(usage1.versions_count, 15);
    }

    #[test]
    fn test_size_summary_add() {
        let mut summary1 = SizeSummary::new();
        summary1.total_size = 100;
        summary1.versions = 5;

        let mut summary2 = SizeSummary::new();
        summary2.total_size = 200;
        summary2.versions = 10;

        summary1.add(&summary2);

        assert_eq!(summary1.total_size, 300);
        assert_eq!(summary1.versions, 15);
    }

    #[test]
    fn test_data_usage_entry_merge_sums_failed_objects() {
        let mut left = DataUsageEntry {
            failed_objects: 2,
            ..Default::default()
        };

        let right = DataUsageEntry {
            failed_objects: 3,
            ..Default::default()
        };

        left.merge(&right);

        assert_eq!(left.failed_objects, 5);
    }

    #[test]
    fn test_data_usage_entry_deserialize_defaults_failed_objects() {
        let entry = DataUsageEntry::default();
        let mut value = serde_json::to_value(&entry).expect("Failed to serialize entry");

        let Value::Object(ref mut map) = value else {
            panic!("Expected entry to serialize into an object");
        };

        map.remove("failed_objects");

        let decoded: DataUsageEntry = serde_json::from_value(value).expect("Failed to deserialize entry");
        assert_eq!(decoded.failed_objects, 0);
    }

    #[test]
    fn test_cache_path_type_distinguishes_main_and_backup() {
        assert_eq!(DataUsageCache::cache_path_type("buckets/.usage-cache.bin"), "main");
        assert_eq!(DataUsageCache::cache_path_type("buckets/.usage-cache.bin.bkp"), "backup");
    }

    #[test]
    fn test_cache_save_timeout_uses_default_when_env_missing() {
        with_var_unset(ENV_SCANNER_CACHE_SAVE_TIMEOUT_SECS, || {
            assert_eq!(
                DataUsageCache::cache_save_timeout(),
                Duration::from_secs(DATA_USAGE_CACHE_SAVE_TIMEOUT_SECS_DEFAULT)
            );
        });
    }

    #[test]
    fn test_cache_save_timeout_respects_env_and_minimum_bound() {
        with_var(ENV_SCANNER_CACHE_SAVE_TIMEOUT_SECS, Some("7"), || {
            assert_eq!(DataUsageCache::cache_save_timeout(), Duration::from_secs(7));
        });

        with_var(ENV_SCANNER_CACHE_SAVE_TIMEOUT_SECS, Some("0"), || {
            assert_eq!(DataUsageCache::cache_save_timeout(), Duration::from_secs(1));
        });
    }

    #[tokio::test]
    async fn test_retry_save_op_retries_on_error_then_succeeds() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let attempts_clone = attempts.clone();

        let result =
            DataUsageCache::retry_save_op("main", Duration::from_millis(200), DATA_USAGE_CACHE_SAVE_RETRIES, move || {
                let attempts = attempts_clone.clone();
                async move {
                    let current = attempts.fetch_add(1, Ordering::SeqCst);
                    if current < 2 {
                        return Err(StorageError::other("transient".to_string()));
                    }
                    Ok(())
                }
            })
            .await;

        assert!(result.is_ok());
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn test_retry_save_op_times_out_and_returns_error_after_retries() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let attempts_clone = attempts.clone();

        let result = DataUsageCache::retry_save_op("main", Duration::from_millis(10), DATA_USAGE_CACHE_SAVE_RETRIES, move || {
            let attempts = attempts_clone.clone();
            async move {
                attempts.fetch_add(1, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(50)).await;
                Ok(())
            }
        })
        .await;

        assert!(result.is_err());
        assert_eq!(attempts.load(Ordering::SeqCst), (DATA_USAGE_CACHE_SAVE_RETRIES + 1) as usize);
    }
}
