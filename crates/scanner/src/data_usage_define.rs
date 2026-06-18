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
    collections::{HashMap, HashSet},
    future::Future,
    sync::{Arc, LazyLock, Once},
    time::SystemTime,
};

use http::HeaderMap;
use metrics::{counter, describe_counter, describe_histogram, histogram};
#[cfg(test)]
use rustfs_config::ENV_SCANNER_CACHE_SAVE_TIMEOUT_SECS;
pub use rustfs_data_usage::{
    BucketTargetUsageInfo, BucketUsageInfo, DataUsageEntry, DataUsageHash, DataUsageHashMap, DataUsageInfo, hash_path,
};
use rustfs_ecstore::{
    bucket::{lifecycle::lifecycle::TRANSITION_COMPLETE, replication::ReplicationConfig},
    config::{com::save_config, storageclass},
    disk::{BUCKET_META_PREFIX, RUSTFS_META_BUCKET},
    error::{Error, Result as StorageResult, StorageError},
};
use rustfs_utils::path::{SLASH_SEPARATOR, path_join_buf};
use tokio::time::{Duration, Instant, sleep, timeout};
use tracing::warn;

pub use crate::storage_compat::{
    ScannerGetObjectReader, ScannerObjectIO, ScannerObjectInfo, ScannerObjectOptions, ScannerObjectToDelete, ScannerPutObjReader,
};
use crate::storage_compat::{ScannerObjectInfo as ObjectInfo, ScannerObjectOptions as ObjectOptions};

// Data usage constants
pub const DATA_USAGE_ROOT: &str = SLASH_SEPARATOR;

const DATA_USAGE_OBJ_NAME: &str = ".usage.json";

const DATA_USAGE_BLOOM_NAME: &str = ".bloomcycle.bin";

pub const DATA_USAGE_CACHE_NAME: &str = ".usage-cache.bin";
const DATA_USAGE_CACHE_SAVE_RETRIES: u32 = 2;
const DATA_USAGE_CACHE_BACKUP_SAVE_TIMEOUT_SECS_MAX: u64 = 5;
const DATA_USAGE_CACHE_BACKUP_SAVE_RETRIES: u32 = 0;
const METRIC_CACHE_SAVE_ATTEMPT_TOTAL: &str = "rustfs_scanner_cache_save_attempt_total";
const METRIC_CACHE_SAVE_TIMEOUT_TOTAL: &str = "rustfs_scanner_cache_save_timeout_total";
const METRIC_CACHE_SAVE_RETRY_TOTAL: &str = "rustfs_scanner_cache_save_retry_total";
const METRIC_CACHE_SAVE_DURATION_SECONDS: &str = "rustfs_scanner_cache_save_duration_seconds";
const LOG_COMPONENT_SCANNER: &str = "scanner";
const LOG_SUBSYSTEM_CACHE: &str = "cache";
const EVENT_SCANNER_CACHE_LOAD_STATE: &str = "scanner_cache_load_state";
const EVENT_SCANNER_CACHE_SAVE_STATE: &str = "scanner_cache_save_state";
static CACHE_SAVE_METRICS_ONCE: Once = Once::new();

pub const DATA_USAGE_SCAN_CHECKPOINT_VERSION: u16 = 1;

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

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DataUsageScanCheckpointReason {
    Runtime,
    Objects,
    Directories,
    Unknown,
}

impl DataUsageScanCheckpointReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Runtime => "runtime",
            Self::Objects => "objects",
            Self::Directories => "directories",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct DataUsageScanCheckpoint {
    pub version: u16,
    pub resume_after: String,
    pub reason: DataUsageScanCheckpointReason,
}

impl DataUsageScanCheckpoint {
    pub fn new(resume_after: String, reason: DataUsageScanCheckpointReason) -> Self {
        Self {
            version: DATA_USAGE_SCAN_CHECKPOINT_VERSION,
            resume_after,
            reason,
        }
    }
}

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
    #[serde(default)]
    pub scan_resume_after: Option<String>,
    #[serde(default)]
    pub scan_checkpoint: Option<DataUsageScanCheckpoint>,
}

/// Data usage cache
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct DataUsageCache {
    pub info: DataUsageCacheInfo,
    pub cache: HashMap<String, DataUsageEntry>,
}

impl DataUsageCache {
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
        let hash = hash_path(path);
        self.cache.insert(hash.key(), e);
        if !parent.is_empty() {
            let parent_hash = hash_path(parent);
            self.cache.entry(parent_hash.key()).or_default().add_child(&hash);
        }
    }

    pub fn replace_hashed(&mut self, hash: &DataUsageHash, parent: &Option<DataUsageHash>, e: &DataUsageEntry) {
        self.cache.insert(hash.key(), e.clone());
        if let Some(parent) = parent {
            self.cache.entry(parent.key()).or_default().add_child(hash);
        }
    }

    pub fn find(&self, path: &str) -> Option<&DataUsageEntry> {
        self.cache.get(&hash_path(path).key())
    }

    pub fn find_children_copy(&mut self, h: DataUsageHash) -> DataUsageHashMap {
        self.cache.entry(h.string()).or_default().children.clone()
    }

    pub fn flatten(&self, root: &DataUsageEntry) -> DataUsageEntry {
        let mut root = root.clone();
        for id in root.children.clone().iter() {
            if let Some(e) = self.cache.get(id) {
                let mut e = e.clone();
                if !e.children.is_empty() {
                    e = self.flatten(&e);
                }
                root.merge(&e);
            }
        }
        root.children.clear();
        root
    }

    pub fn copy_with_children(&mut self, src: &DataUsageCache, hash: &DataUsageHash, parent: &Option<DataUsageHash>) {
        if let Some(e) = src.cache.get(&hash.string()) {
            self.cache.insert(hash.key(), e.clone());
            for ch in e.children.iter() {
                if *ch == hash.key() {
                    return;
                }
                self.copy_with_children(src, &DataUsageHash(ch.to_string()), &Some(hash.clone()));
            }
            if let Some(parent) = parent {
                self.cache.entry(parent.key()).or_default().add_child(hash);
            }
        }
    }

    pub fn delete_recursive(&mut self, hash: &DataUsageHash) {
        let mut need_remove = Vec::new();
        if let Some(v) = self.cache.get(&hash.string()) {
            for child in v.children.iter() {
                need_remove.push(child.clone());
            }
        }
        self.cache.remove(&hash.string());
        for child in need_remove {
            self.delete_recursive(&DataUsageHash(child));
        }
    }

    pub fn size_recursive(&self, path: &str) -> Option<DataUsageEntry> {
        match self.find(path) {
            Some(root) => {
                if root.children.is_empty() {
                    return Some(root.clone());
                }
                let mut flat = self.flatten(root);
                if flat.replication_stats.as_ref().is_some_and(|stats| stats.empty()) {
                    flat.replication_stats = None;
                }
                Some(flat)
            }
            None => None,
        }
    }

    pub fn search_parent(&self, hash: &DataUsageHash) -> Option<DataUsageHash> {
        let want = hash.key();
        if let Some(last_index) = want.rfind('/')
            && let Some(v) = self.find(&want[0..last_index])
            && v.children.contains(&want)
        {
            return Some(hash_path(&want[0..last_index]));
        }

        for (k, v) in self.cache.iter() {
            if v.children.contains(&want) {
                return Some(DataUsageHash(k.clone()));
            }
        }
        None
    }

    pub fn is_compacted(&self, hash: &DataUsageHash) -> bool {
        self.cache.get(&hash.key()).is_some_and(|due| due.compacted)
    }

    pub fn force_compact(&mut self, limit: usize) {
        if self.cache.len() < limit {
            return;
        }
        let top = hash_path(&self.info.name).key();
        let Some(top_e) = self.find(&top).cloned() else {
            return;
        };

        if top_e.children.len() > 250_000 {
            self.reduce_children_of(&hash_path(&self.info.name), limit, true);
        }
        if self.cache.len() <= limit {
            return;
        }

        let mut found = HashSet::new();
        found.insert(top);
        mark(self, &top_e, &mut found);
        self.cache.retain(|k, _| found.contains(k));
    }

    pub fn reduce_children_of(&mut self, path: &DataUsageHash, limit: usize, compact_self: bool) {
        let Some(e) = self.cache.get(&path.key()).cloned() else {
            return;
        };

        if e.compacted {
            return;
        }

        if e.children.len() > limit && compact_self {
            let mut flat = self.size_recursive(&path.key()).unwrap_or_default();
            flat.compacted = true;
            self.delete_recursive(path);
            self.replace_hashed(path, &None, &flat);
            return;
        }

        let total = self.total_children_rec(&path.key());
        if total < limit {
            return;
        }

        let mut candidates = Vec::new();
        let mut remove = total - limit;
        add(self, path, &mut candidates);
        candidates.sort_by_key(|a| a.objects);

        let mut candidate_index = 0;
        while remove > 0 && candidate_index < candidates.len() {
            let e = &candidates[candidate_index];
            let candidate = e.path.clone();
            if candidate == *path && !compact_self {
                break;
            }
            let removing = self.total_children_rec(&candidate.key());
            let mut flat = match self.size_recursive(&candidate.key()) {
                Some(flat) => flat,
                None => {
                    candidate_index += 1;
                    continue;
                }
            };

            flat.compacted = true;
            self.delete_recursive(&candidate);
            self.replace_hashed(&candidate, &None, &flat);

            remove = remove.saturating_sub(removing);
            candidate_index += 1;
        }
    }

    pub fn total_children_rec(&self, path: &str) -> usize {
        let Some(root) = self.find(path) else {
            return 0;
        };
        if root.children.is_empty() {
            return 0;
        }

        let mut n = root.children.len();
        for ch in root.children.iter() {
            n += self.total_children_rec(ch);
        }
        n
    }

    pub fn merge(&mut self, o: &DataUsageCache) {
        let Some(mut existing_root) = self.root() else {
            if o.root().is_none() {
                return;
            }
            *self = o.clone();
            return;
        };

        let Some(other_root) = o.root() else {
            return;
        };

        if o.info.last_update > self.info.last_update {
            self.info.last_update = o.info.last_update;
        }

        existing_root.merge(&other_root);
        self.cache.insert(hash_path(&self.info.name).key(), existing_root);

        let root_hash = self.root_hash();
        for key in other_root.children.iter() {
            let Some(entry) = o.cache.get(key) else {
                continue;
            };
            let flat = o.flatten(entry);
            if let Some(existing) = self.cache.get_mut(key) {
                existing.merge(&flat);
            } else {
                self.replace_hashed(&DataUsageHash(key.clone()), &Some(root_hash.clone()), &flat);
            }
        }
    }

    pub fn root_hash(&self) -> DataUsageHash {
        hash_path(&self.info.name)
    }

    pub fn root(&self) -> Option<DataUsageEntry> {
        self.find(&self.info.name).cloned()
    }

    /// Convert cache to DataUsageInfo for a specific path
    pub fn dui(&self, path: &str, buckets: &[String]) -> DataUsageInfo {
        let e = match self.find(path) {
            Some(e) => e,
            None => return DataUsageInfo::default(),
        };
        let flat = self.flatten(e);

        let mut buckets_usage = HashMap::new();
        for bucket_name in buckets.iter() {
            let e = match self.find(bucket_name) {
                Some(e) => e,
                None => continue,
            };
            let flat = self.flatten(e);
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
            buckets_usage.insert(bucket_name.clone(), bui);
        }

        DataUsageInfo {
            last_update: self.info.last_update,
            objects_total_count: flat.objects as u64,
            versions_total_count: flat.versions as u64,
            delete_markers_total_count: flat.delete_markers as u64,
            objects_total_size: flat.size as u64,
            buckets_count: e.children.len() as u64,
            buckets_usage,
            ..Default::default()
        }
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
    pub async fn load<S: ScannerObjectIO>(&mut self, store: Arc<S>, name: &str) -> StorageResult<()> {
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
            warn!(
                target: "rustfs::scanner::data_usage",
                event = EVENT_SCANNER_CACHE_LOAD_STATE,
                component = LOG_COMPONENT_SCANNER,
                subsystem = LOG_SUBSYSTEM_CACHE,
                cache_name = %name,
                retries,
                state = "max_retries_reached",
                "Scanner cache load reached retry limit"
            );
        }

        Ok(())
    }
    // Inner load function that attempts to load from a specific path
    // Returns (should_retry, cache_option, error_option)
    async fn try_load_inner<S: ScannerObjectIO>(
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
        crate::runtime_config::scanner_cache_save_timeout()
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

    async fn save_path_with_retry<S: ScannerObjectIO>(
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

    pub async fn save<S: ScannerObjectIO>(&self, store: Arc<S>, name: &str) -> StorageResult<()> {
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
            warn!(
                target: "rustfs::scanner::data_usage",
                event = EVENT_SCANNER_CACHE_SAVE_STATE,
                component = LOG_COMPONENT_SCANNER,
                subsystem = LOG_SUBSYSTEM_CACHE,
                cache_name = %name,
                backup_path = %backup_path,
                state = "backup_save_failed",
                error = %e,
                "Scanner cache backup save failed"
            );
        }
        Ok(())
    }
}

#[derive(Default, Clone)]
struct Inner {
    objects: usize,
    path: DataUsageHash,
}

fn add(data_usage_cache: &DataUsageCache, path: &DataUsageHash, candidates: &mut Vec<Inner>) -> usize {
    let e = match data_usage_cache.cache.get(&path.key()) {
        Some(e) => e,
        None => return 0,
    };
    let mut objects = e.objects;
    for ch in e.children.iter() {
        objects += add(data_usage_cache, &DataUsageHash(ch.clone()), candidates);
    }
    // Collect internal nodes (with children) as compaction candidates.
    // Leaf nodes have no children to remove, so compacting them is a no-op —
    // total_children_rec returns 0 for leaves, so `remove` would never decrement.
    if !e.children.is_empty() {
        candidates.push(Inner {
            objects,
            path: path.clone(),
        });
    }
    objects
}

fn mark(duc: &DataUsageCache, entry: &DataUsageEntry, found: &mut HashSet<String>) {
    for k in entry.children.iter() {
        found.insert(k.to_string());
        if let Some(ch) = duc.cache.get(k) {
            mark(duc, ch, found);
        }
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
    fn test_data_usage_cache_info_deserialize_defaults_scan_resume_after() {
        let value = serde_json::json!({
            "name": "bucket",
            "next_cycle": 7,
            "last_update": null,
            "skip_healing": false,
            "lifecycle": null,
            "replication": null,
            "failed_objects": {}
        });

        let decoded: DataUsageCacheInfo = serde_json::from_value(value).expect("Failed to deserialize cache info");

        assert_eq!(decoded.name, "bucket");
        assert_eq!(decoded.next_cycle, 7);
        assert!(decoded.scan_resume_after.is_none());
        assert!(decoded.scan_checkpoint.is_none());
    }

    #[test]
    fn test_data_usage_cache_info_unmarshal_old_msgpack_defaults_scan_resume_after() {
        #[derive(Serialize)]
        struct OldDataUsageCacheInfo {
            name: String,
            next_cycle: u64,
            last_update: Option<SystemTime>,
            skip_healing: bool,
            lifecycle: Option<Arc<BucketLifecycleConfiguration>>,
            replication: Option<Arc<ReplicationConfig>>,
            failed_objects: HashMap<String, u64>,
        }

        let old_info = OldDataUsageCacheInfo {
            name: "bucket".to_string(),
            next_cycle: 7,
            last_update: None,
            skip_healing: true,
            lifecycle: None,
            replication: None,
            failed_objects: HashMap::from([("bad-object".to_string(), 11)]),
        };
        let mut buf = Vec::new();
        old_info
            .serialize(&mut rmp_serde::Serializer::new(&mut buf))
            .expect("Failed to serialize old cache info");

        let decoded: DataUsageCacheInfo = rmp_serde::from_slice(&buf).expect("Failed to deserialize old cache info");

        assert_eq!(decoded.name, "bucket");
        assert_eq!(decoded.next_cycle, 7);
        assert!(decoded.skip_healing);
        assert_eq!(decoded.failed_objects.get("bad-object"), Some(&11));
        assert!(decoded.scan_resume_after.is_none());
        assert!(decoded.scan_checkpoint.is_none());
    }

    #[test]
    fn test_data_usage_cache_mutations_update_in_place() {
        let mut cache = DataUsageCache {
            info: DataUsageCacheInfo {
                name: "bucket".to_string(),
                failed_objects: HashMap::from([("bad-object".to_string(), 7)]),
                ..Default::default()
            },
            ..Default::default()
        };

        let root_hash = hash_path("bucket");
        let child_hash = hash_path("bucket/a");
        let grandchild_hash = hash_path("bucket/a/b");

        cache.replace_hashed(&root_hash, &None, &DataUsageEntry::default());
        cache.replace_hashed(
            &child_hash,
            &Some(root_hash.clone()),
            &DataUsageEntry {
                objects: 2,
                size: 20,
                ..Default::default()
            },
        );
        cache.replace_hashed(
            &grandchild_hash,
            &Some(child_hash.clone()),
            &DataUsageEntry {
                objects: 3,
                size: 30,
                ..Default::default()
            },
        );

        assert!(cache.find("bucket").unwrap().children.contains(&child_hash.key()));
        assert!(cache.find("bucket/a").unwrap().children.contains(&grandchild_hash.key()));
        assert_eq!(cache.search_parent(&grandchild_hash), Some(child_hash.clone()));
        assert_eq!(cache.info.failed_objects.get("bad-object"), Some(&7));

        let flat = cache.size_recursive("bucket").unwrap();
        assert_eq!(flat.objects, 5);
        assert_eq!(flat.size, 50);
        assert!(flat.children.is_empty());
    }

    #[test]
    fn test_data_usage_cache_copy_and_delete_recursive() {
        let root_hash = hash_path("bucket");
        let child_hash = hash_path("bucket/a");
        let grandchild_hash = hash_path("bucket/a/b");

        let mut src = DataUsageCache {
            info: DataUsageCacheInfo {
                name: "bucket".to_string(),
                ..Default::default()
            },
            ..Default::default()
        };
        src.replace_hashed(&root_hash, &None, &DataUsageEntry::default());
        src.replace_hashed(
            &child_hash,
            &Some(root_hash.clone()),
            &DataUsageEntry {
                objects: 1,
                ..Default::default()
            },
        );
        src.replace_hashed(
            &grandchild_hash,
            &Some(child_hash.clone()),
            &DataUsageEntry {
                objects: 1,
                ..Default::default()
            },
        );

        let mut dst = DataUsageCache {
            info: src.info.clone(),
            ..Default::default()
        };
        dst.replace_hashed(&root_hash, &None, &DataUsageEntry::default());
        dst.copy_with_children(&src, &child_hash, &Some(root_hash.clone()));

        assert!(dst.cache.contains_key(&child_hash.key()));
        assert!(dst.cache.contains_key(&grandchild_hash.key()));
        assert!(dst.find("bucket").unwrap().children.contains(&child_hash.key()));
        assert!(dst.find("bucket/a").unwrap().children.contains(&grandchild_hash.key()));

        dst.delete_recursive(&child_hash);

        assert!(!dst.cache.contains_key(&child_hash.key()));
        assert!(!dst.cache.contains_key(&grandchild_hash.key()));
        assert!(dst.cache.contains_key(&root_hash.key()));
    }

    #[test]
    fn test_find_children_copy_preserves_missing_entry_behavior() {
        let mut cache = DataUsageCache::default();
        let missing_hash = hash_path("missing");

        assert!(cache.find_children_copy(missing_hash.clone()).is_empty());
        assert!(cache.cache.contains_key(&missing_hash.key()));
    }

    #[test]
    fn test_cache_path_type_distinguishes_main_and_backup() {
        assert_eq!(DataUsageCache::cache_path_type("buckets/.usage-cache.bin"), "main");
        assert_eq!(DataUsageCache::cache_path_type("buckets/.usage-cache.bin.bkp"), "backup");
    }

    #[test]
    fn test_cache_save_timeout_uses_default_when_env_missing() {
        with_var_unset(ENV_SCANNER_CACHE_SAVE_TIMEOUT_SECS, || {
            crate::runtime_config::refresh_scanner_runtime_config_for_tests();
            assert_eq!(
                DataUsageCache::cache_save_timeout(),
                Duration::from_secs(rustfs_config::DEFAULT_SCANNER_CACHE_SAVE_TIMEOUT_SECS)
            );
        });
        crate::runtime_config::refresh_scanner_runtime_config_for_tests();
    }

    #[test]
    fn test_cache_save_timeout_respects_env_and_minimum_bound() {
        with_var(ENV_SCANNER_CACHE_SAVE_TIMEOUT_SECS, Some("7"), || {
            crate::runtime_config::refresh_scanner_runtime_config_for_tests();
            assert_eq!(DataUsageCache::cache_save_timeout(), Duration::from_secs(7));
        });

        with_var(ENV_SCANNER_CACHE_SAVE_TIMEOUT_SECS, Some("0"), || {
            crate::runtime_config::refresh_scanner_runtime_config_for_tests();
            assert_eq!(DataUsageCache::cache_save_timeout(), Duration::from_secs(1));
        });
        crate::runtime_config::refresh_scanner_runtime_config_for_tests();
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

    // --- Tests for `add` function (bug #1: logic inversion) ---

    /// Build a small tree: root -> child1 (leaf), child2 -> grandchild (leaf).
    /// Returns (cache, root_hash).
    fn build_test_tree() -> (DataUsageCache, DataUsageHash) {
        let root = hash_path("bucket");
        let c1 = hash_path("bucket/a");
        let c2 = hash_path("bucket/b");
        let gc = hash_path("bucket/b/c");

        let mut cache = DataUsageCache::default();
        cache.replace_hashed(&root, &None, &DataUsageEntry::default());
        cache.replace_hashed(
            &c1,
            &Some(root.clone()),
            &DataUsageEntry {
                objects: 1,
                size: 10,
                ..Default::default()
            },
        );
        cache.replace_hashed(
            &c2,
            &Some(root.clone()),
            &DataUsageEntry {
                objects: 2,
                size: 20,
                ..Default::default()
            },
        );
        cache.replace_hashed(
            &gc,
            &Some(c2.clone()),
            &DataUsageEntry {
                objects: 3,
                size: 30,
                ..Default::default()
            },
        );
        (cache, root)
    }

    fn build_underflow_test_tree() -> (DataUsageCache, DataUsageHash) {
        let root = hash_path("bucket");
        let small = hash_path("bucket/small");
        let small_a = hash_path("bucket/small/a");
        let small_b = hash_path("bucket/small/b");
        let large = hash_path("bucket/large");
        let large_a = hash_path("bucket/large/a");
        let large_b = hash_path("bucket/large/b");

        let mut cache = DataUsageCache::default();
        cache.replace_hashed(
            &root,
            &None,
            &DataUsageEntry {
                objects: 100,
                ..Default::default()
            },
        );
        cache.replace_hashed(&small, &Some(root.clone()), &DataUsageEntry::default());
        cache.replace_hashed(
            &small_a,
            &Some(small.clone()),
            &DataUsageEntry {
                objects: 1,
                ..Default::default()
            },
        );
        cache.replace_hashed(
            &small_b,
            &Some(small.clone()),
            &DataUsageEntry {
                objects: 1,
                ..Default::default()
            },
        );
        cache.replace_hashed(&large, &Some(root.clone()), &DataUsageEntry::default());
        cache.replace_hashed(
            &large_a,
            &Some(large.clone()),
            &DataUsageEntry {
                objects: 10,
                ..Default::default()
            },
        );
        cache.replace_hashed(
            &large_b,
            &Some(large.clone()),
            &DataUsageEntry {
                objects: 10,
                ..Default::default()
            },
        );
        (cache, root)
    }

    #[test]
    fn test_add_collects_internal_nodes_as_compaction_candidates() {
        // `add()` should collect internal nodes (with children) as compaction candidates.
        // Leaf nodes have no children to remove, so compacting them is a no-op.
        let (cache, root) = build_test_tree();
        let mut candidates = Vec::new();
        add(&cache, &root, &mut candidates);

        let mut paths: Vec<String> = candidates.iter().map(|l| l.path.key()).collect();
        paths.sort();
        // Internal nodes: "bucket" (children: [a, b]) and "bucket/b" (children: [c]).
        // Leaf nodes "bucket/a" and "bucket/b/c" are NOT collected.
        assert_eq!(paths.len(), 2, "add() should find internal nodes with children");
        assert!(paths.contains(&hash_path("bucket").key()));
        assert!(paths.contains(&hash_path("bucket/b").key()));
    }

    #[test]
    fn test_add_returns_empty_for_missing_path() {
        let cache = DataUsageCache::default();
        let mut candidates = Vec::new();
        add(&cache, &hash_path("nonexistent"), &mut candidates);
        assert!(candidates.is_empty());
    }

    #[test]
    fn test_add_skips_leaf_node() {
        // A leaf node (no children) is not a valid compaction candidate —
        // total_children_rec returns 0 for leaves, so compacting them has no effect.
        let mut cache = DataUsageCache::default();
        let h = hash_path("single-leaf");
        cache.replace_hashed(
            &h,
            &None,
            &DataUsageEntry {
                objects: 5,
                size: 50,
                ..Default::default()
            },
        );

        let mut candidates = Vec::new();
        add(&cache, &h, &mut candidates);
        assert!(candidates.is_empty(), "leaf node should not be a compaction candidate");
    }

    // --- Tests for `reduce_children_of` (bug #2: usize underflow) ---

    #[test]
    fn test_reduce_children_of_compacts_internal_node() {
        // Build tree: root -> c1(leaf), c2 -> gc(leaf). total=3, limit=2, remove=1.
        // Internal nodes (compaction candidates): root, c2.
        // compact_self=false skips root; c2 (objects=2, 1 child) is the only candidate.
        // Compacting c2 removes gc (removing=1), satisfying remove=1.
        let (mut cache, root) = build_test_tree();
        cache.reduce_children_of(&root, 2, false);

        // "bucket/b" should be compacted (its child gc removed).
        let entry_c2 = cache.find("bucket/b").unwrap();
        assert!(entry_c2.compacted, "internal node 'bucket/b' should be compacted");
        // "bucket/a" (leaf, not a candidate) should remain unchanged.
        let entry_c1 = cache.find("bucket/a").unwrap();
        assert!(!entry_c1.compacted, "leaf 'bucket/a' should not be compacted");
        // "bucket/b/c" was deleted when its parent was compacted.
        assert!(cache.find("bucket/b/c").is_none(), "grandchild should be removed after parent compaction");
    }

    #[test]
    fn test_reduce_children_of_no_op_when_under_limit() {
        let (mut cache, root) = build_test_tree();
        let before = cache.cache.len();
        // limit=10 > total children => no compaction
        cache.reduce_children_of(&root, 10, false);
        assert_eq!(cache.cache.len(), before);
    }

    #[test]
    fn test_reduce_children_of_usize_underflow_saturates() {
        let (mut cache, root) = build_underflow_test_tree();

        // total children=6, limit=5, remove=1. The smallest candidate removes
        // two descendants, so plain subtraction would underflow and compact the
        // next candidate too.
        cache.reduce_children_of(&root, 5, false);

        assert!(cache.find("bucket/small").is_some_and(|entry| entry.compacted));
        assert!(cache.find("bucket/small/a").is_none());
        assert!(cache.find("bucket/small/b").is_none());
        assert!(cache.find("bucket/large").is_some_and(|entry| !entry.compacted));
        assert!(cache.find("bucket/large/a").is_some());
        assert!(cache.find("bucket/large/b").is_some());
    }

    #[tokio::test]
    async fn test_retry_save_op_times_out_and_returns_error_after_retries() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let attempts_clone = attempts.clone();

        let result = DataUsageCache::retry_save_op("main", Duration::from_millis(10), DATA_USAGE_CACHE_SAVE_RETRIES, move || {
            let attempts = attempts_clone.clone();
            async move {
                attempts.fetch_add(1, Ordering::SeqCst);
                std::future::pending::<StorageResult<()>>().await
            }
        })
        .await;

        assert!(result.is_err());
        assert_eq!(attempts.load(Ordering::SeqCst), (DATA_USAGE_CACHE_SAVE_RETRIES + 1) as usize);
    }
}
