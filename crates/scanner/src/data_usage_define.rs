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

use s3s::dto::{BucketLifecycleConfiguration, ObjectLockConfiguration};
use serde::{Deserialize, Serialize, ser::SerializeMap};
use std::{
    collections::{HashMap, HashSet},
    future::Future,
    sync::{Arc, LazyLock, Once},
    time::SystemTime,
};

use http::HeaderMap;
use metrics::{counter, describe_counter, describe_histogram, histogram};
use rustfs_common::heal_channel::HealScanMode;
#[cfg(test)]
use rustfs_config::ENV_SCANNER_CACHE_SAVE_TIMEOUT_SECS;
pub use rustfs_data_usage::{
    BucketTargetUsageInfo, BucketUsageInfo, DATA_USAGE_OBJECT_NAME, DataUsageEntry, DataUsageHash, DataUsageHashMap,
    DataUsageInfo, LEGACY_DATA_USAGE_OBJECT_NAME, hash_path,
};
use rustfs_utils::path::{SLASH_SEPARATOR, path_join_buf};
use tokio::time::{Duration, Instant, sleep, timeout};
use tracing::{debug, warn};

use crate::ScannerObjectIO;
use crate::storage_api::owner::HTTPPreconditions;
use crate::{
    BUCKET_META_PREFIX, EcstoreError as Error, EcstoreResult as StorageResult, RUSTFS_META_BUCKET, ReplicationConfig,
    ScannerObjectInfo as ObjectInfo, ScannerObjectOptions as ObjectOptions, StorageError, TRANSITION_COMPLETE, save_config,
    save_config_with_preconditions, storageclass,
};

// Data usage constants
pub const DATA_USAGE_ROOT: &str = SLASH_SEPARATOR;

const DATA_USAGE_BLOOM_NAME: &str = ".bloomcycle.bin";

pub const DATA_USAGE_CACHE_NAME: &str = ".usage-cache.bin";
const DATA_USAGE_CACHE_SAVE_RETRIES: u32 = 2;
const DATA_USAGE_CACHE_BACKUP_SAVE_TIMEOUT_SECS_MAX: u64 = 5;
const DATA_USAGE_CACHE_BACKUP_SAVE_RETRIES: u32 = 0;
const DATA_USAGE_CACHE_SAVE_RETRY_BACKOFF_MAX: Duration = Duration::from_millis(350);
const DATA_USAGE_CACHE_PERSISTENCE_MARGIN: Duration = Duration::from_secs(5);
const METRIC_CACHE_SAVE_ATTEMPT_TOTAL: &str = "rustfs_scanner_cache_save_attempt_total";
const METRIC_CACHE_SAVE_TIMEOUT_TOTAL: &str = "rustfs_scanner_cache_save_timeout_total";
const METRIC_CACHE_SAVE_RETRY_TOTAL: &str = "rustfs_scanner_cache_save_retry_total";
const METRIC_CACHE_SAVE_DURATION_SECONDS: &str = "rustfs_scanner_cache_save_duration_seconds";
const METRIC_CACHE_BACKUP_REVISION_FAILURE_TOTAL: &str = "rustfs_scanner_cache_backup_revision_failure_total";
const LOG_COMPONENT_SCANNER: &str = "scanner";
const LOG_SUBSYSTEM_CACHE: &str = "cache";
const EVENT_SCANNER_CACHE_LOAD_STATE: &str = "scanner_cache_load_state";
const EVENT_SCANNER_CACHE_SAVE_STATE: &str = "scanner_cache_save_state";
static CACHE_SAVE_METRICS_ONCE: Once = Once::new();

pub const DATA_USAGE_SCAN_CHECKPOINT_VERSION: u16 = 1;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum DataUsageCacheRevision {
    Missing,
    Etag(String),
}

impl DataUsageCacheRevision {
    pub(crate) fn preconditions(&self) -> HTTPPreconditions {
        match self {
            Self::Missing => HTTPPreconditions {
                if_none_match: Some("*".to_string()),
                ..Default::default()
            },
            Self::Etag(etag) => HTTPPreconditions {
                if_match: Some(etag.clone()),
                ..Default::default()
            },
        }
    }
}

pub(crate) async fn read_config_with_revision<S: ScannerObjectIO>(
    store: Arc<S>,
    path: &str,
) -> StorageResult<(Option<Vec<u8>>, DataUsageCacheRevision)> {
    match store
        .get_object_reader(
            RUSTFS_META_BUCKET,
            path,
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
            let revision = reader
                .object_info
                .etag
                .as_ref()
                .filter(|etag| !etag.is_empty())
                .cloned()
                .map(DataUsageCacheRevision::Etag)
                .ok_or_else(|| StorageError::other(format!("scanner config object {path} has no ETag")))?;
            Ok((Some(reader.read_all().await?), revision))
        }
        Err(Error::FileNotFound | Error::VolumeNotFound | Error::ObjectNotFound(_, _) | Error::BucketNotFound(_)) => {
            Ok((None, DataUsageCacheRevision::Missing))
        }
        Err(err) => Err(err),
    }
}

#[derive(Clone, Debug)]
pub(crate) struct DataUsageCacheRevisions {
    main: DataUsageCacheRevision,
    backup: Option<DataUsageCacheRevision>,
}

enum DataUsageCacheLoadAttempt {
    Loaded {
        cache: Box<DataUsageCache>,
        revision: Option<DataUsageCacheRevision>,
    },
    Missing {
        revision: Option<DataUsageCacheRevision>,
    },
    Corrupt {
        revision: Option<DataUsageCacheRevision>,
    },
    Retryable(Error),
}

struct DataUsageCacheLoadResult {
    cache: DataUsageCache,
    main_revision: DataUsageCacheRevision,
    backup_revision: Option<DataUsageCacheRevision>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DataUsageCacheLoadState {
    Loaded,
    Missing,
    Corrupt,
    Retryable,
}

impl DataUsageCacheLoadAttempt {
    fn revision(&self) -> Option<DataUsageCacheRevision> {
        match self {
            Self::Loaded { revision, .. } | Self::Missing { revision } | Self::Corrupt { revision } => revision.clone(),
            Self::Retryable(_) => None,
        }
    }
}

// Data usage paths (computed at runtime)
pub static DATA_USAGE_BUCKET: LazyLock<String> =
    LazyLock::new(|| format!("{RUSTFS_META_BUCKET}{SLASH_SEPARATOR}{BUCKET_META_PREFIX}"));

pub static DATA_USAGE_OBJ_NAME_PATH: LazyLock<String> =
    LazyLock::new(|| format!("{BUCKET_META_PREFIX}{SLASH_SEPARATOR}{DATA_USAGE_OBJECT_NAME}"));

pub static LEGACY_DATA_USAGE_OBJ_NAME_PATH: LazyLock<String> =
    LazyLock::new(|| format!("{BUCKET_META_PREFIX}{SLASH_SEPARATOR}{LEGACY_DATA_USAGE_OBJECT_NAME}"));

pub static DATA_USAGE_BLOOM_NAME_PATH: LazyLock<String> =
    LazyLock::new(|| format!("{BUCKET_META_PREFIX}{SLASH_SEPARATOR}{DATA_USAGE_BLOOM_NAME}"));

pub static BACKGROUND_HEAL_INFO_PATH: LazyLock<String> =
    LazyLock::new(|| format!("{BUCKET_META_PREFIX}{SLASH_SEPARATOR}.background-heal.json"));

const MAX_DATA_USAGE_CACHE_DEPTH: usize = 1024;

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
            self.delete_markers = self.delete_markers.saturating_add(1);
            return;
        }

        if oi.version_id.is_some_and(|v| !v.is_nil()) && size == actual_size {
            self.versions = self.versions.saturating_add(1);
        }

        let size = usize::try_from(size.max(0)).unwrap_or(usize::MAX);
        self.total_size = self.total_size.saturating_add(size);

        if oi.transitioned_object.free_version {
            return;
        }

        let mut tier = oi.storage_class.clone().unwrap_or_else(|| storageclass::STANDARD.to_string());
        if oi.transitioned_object.status == TRANSITION_COMPLETE {
            tier = oi.transitioned_object.tier.clone();
        }

        if let Some(tier_stats) = self.tier_stats.get_mut(&tier) {
            *tier_stats = tier_stats.add(&TierStats::from_object_info(oi));
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

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(deny_unknown_fields)]
pub struct DataUsageCacheSource {
    pub pool_index: usize,
    pub set_index: usize,
}

impl DataUsageCacheSource {
    pub const fn new(pool_index: usize, set_index: usize) -> Self {
        Self { pool_index, set_index }
    }
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(transparent)]
pub struct DataUsageScanPlanDigest(pub [u8; 32]);

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PendingScannerHealKind {
    Bucket,
    Object,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct PendingScannerHeal {
    pub kind: PendingScannerHealKind,
    pub bucket: String,
    #[serde(default)]
    pub object: Option<String>,
    #[serde(default)]
    pub version_id: Option<String>,
    pub scan_mode: HealScanMode,
    pub first_seen: u64,
    pub last_attempt: u64,
    pub attempts: u32,
    #[serde(default)]
    pub last_admission_result: String,
    #[serde(default)]
    pub last_admission_reason: String,
}

/// Data usage cache info
#[derive(Clone, Debug, Default, Deserialize)]
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
    #[serde(default)]
    pub pending_heals: Vec<PendingScannerHeal>,
    #[serde(default)]
    pub object_lock: Option<Arc<ObjectLockConfiguration>>,
    #[serde(default)]
    pub leader_epoch: u64,
    #[serde(default)]
    pub source: Option<DataUsageCacheSource>,
    #[serde(default)]
    pub snapshot_complete: bool,
    #[serde(default)]
    pub scan_plan_digest: Option<DataUsageScanPlanDigest>,
}

impl Serialize for DataUsageCacheInfo {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        // Keep this metadata map-encoded so older readers can ignore fields
        // appended by newer scanner versions during rolling upgrades.
        let mut state = serializer.serialize_map(Some(15))?;
        state.serialize_entry("name", &self.name)?;
        state.serialize_entry("next_cycle", &self.next_cycle)?;
        state.serialize_entry("leader_epoch", &self.leader_epoch)?;
        state.serialize_entry("last_update", &self.last_update)?;
        state.serialize_entry("skip_healing", &self.skip_healing)?;
        state.serialize_entry("lifecycle", &self.lifecycle)?;
        state.serialize_entry("replication", &self.replication)?;
        state.serialize_entry("failed_objects", &self.failed_objects)?;
        state.serialize_entry("scan_resume_after", &self.scan_resume_after)?;
        state.serialize_entry("scan_checkpoint", &self.scan_checkpoint)?;
        state.serialize_entry("pending_heals", &self.pending_heals)?;
        state.serialize_entry("object_lock", &self.object_lock)?;
        state.serialize_entry("source", &self.source)?;
        state.serialize_entry("snapshot_complete", &self.snapshot_complete)?;
        state.serialize_entry("scan_plan_digest", &self.scan_plan_digest)?;
        state.end()
    }
}

/// Data usage cache
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct DataUsageCache {
    pub info: DataUsageCacheInfo,
    pub cache: HashMap<String, DataUsageEntry>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DataUsageCachePrepareOutcome {
    Reused,
    Reset,
    RejectedNewerCycle,
    RejectedNewerLeader,
}

impl DataUsageCache {
    pub(crate) fn prepare_for_scan(
        &mut self,
        name: &str,
        next_cycle: u64,
        leader_epoch: u64,
        source: DataUsageCacheSource,
        scan_plan_digest: DataUsageScanPlanDigest,
        require_source: bool,
    ) -> DataUsageCachePrepareOutcome {
        if self.info.next_cycle > next_cycle {
            return DataUsageCachePrepareOutcome::RejectedNewerCycle;
        }
        if self.info.leader_epoch > leader_epoch {
            return DataUsageCachePrepareOutcome::RejectedNewerLeader;
        }

        let source_matches = self.info.source == Some(source);
        let plan_matches = self.info.scan_plan_digest == Some(scan_plan_digest);
        let reusable = self.info.name == name
            && self.info.leader_epoch == leader_epoch
            && plan_matches
            && (source_matches || (!require_source && self.info.source.is_none()));
        if !reusable {
            *self = Self::default();
            self.info.name = name.to_string();
        }

        self.info.next_cycle = next_cycle;
        self.info.leader_epoch = leader_epoch;
        self.info.source = Some(source);
        self.info.scan_plan_digest = Some(scan_plan_digest);
        self.info.snapshot_complete = false;
        if reusable {
            DataUsageCachePrepareOutcome::Reused
        } else {
            DataUsageCachePrepareOutcome::Reset
        }
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
        let mut visited = HashSet::new();
        self.flatten_with_guard(root, &mut visited, 0)
    }

    pub(crate) fn checked_flatten(&self, path: &str) -> Option<DataUsageEntry> {
        let root_key = hash_path(path).key();
        let root = self.cache.get(&root_key)?;
        let mut visited = HashSet::from([root_key]);
        let mut pending = root.children.iter().map(|child| (child.clone(), 1usize)).collect::<Vec<_>>();
        let mut flattened = DataUsageEntry::default();
        let mut root_entry = root.clone();
        root_entry.children.clear();
        if !flattened.checked_merge(&root_entry) {
            return None;
        }
        flattened.compacted = root.compacted;

        while let Some((key, depth)) = pending.pop() {
            if depth > MAX_DATA_USAGE_CACHE_DEPTH || !visited.insert(key.clone()) {
                return None;
            }
            let entry = self.cache.get(&key)?;
            if depth == MAX_DATA_USAGE_CACHE_DEPTH && !entry.children.is_empty() {
                return None;
            }
            pending.extend(entry.children.iter().map(|child| (child.clone(), depth + 1)));

            let mut child_entry = entry.clone();
            child_entry.children.clear();
            if !flattened.checked_merge(&child_entry) {
                return None;
            }
        }

        Some(flattened)
    }

    fn flatten_with_guard(&self, root: &DataUsageEntry, visited: &mut HashSet<String>, depth: usize) -> DataUsageEntry {
        let mut root = root.clone();
        if depth >= MAX_DATA_USAGE_CACHE_DEPTH {
            root.children.clear();
            return root;
        }

        for id in root.children.clone().iter() {
            if !visited.insert(id.clone()) {
                continue;
            }
            if let Some(e) = self.cache.get(id) {
                let mut e = e.clone();
                if !e.children.is_empty() {
                    e = self.flatten_with_guard(&e, visited, depth + 1);
                }
                root.merge(&e);
            }
        }
        root.children.clear();
        root
    }

    pub fn copy_with_children(&mut self, src: &DataUsageCache, hash: &DataUsageHash, parent: &Option<DataUsageHash>) {
        let mut visited = HashSet::new();
        self.copy_with_children_guard(src, hash, parent, &mut visited, 0);
    }

    fn copy_with_children_guard(
        &mut self,
        src: &DataUsageCache,
        hash: &DataUsageHash,
        parent: &Option<DataUsageHash>,
        visited: &mut HashSet<String>,
        depth: usize,
    ) {
        if !visited.insert(hash.key()) {
            return;
        }

        if let Some(e) = src.cache.get(&hash.string()) {
            self.cache.insert(hash.key(), e.clone());
            if depth < MAX_DATA_USAGE_CACHE_DEPTH {
                for ch in e.children.iter() {
                    if *ch == hash.key() {
                        continue;
                    }
                    self.copy_with_children_guard(src, &DataUsageHash(ch.to_string()), &Some(hash.clone()), visited, depth + 1);
                }
            }
            if let Some(parent) = parent {
                self.cache.entry(parent.key()).or_default().add_child(hash);
            }
        }
    }

    pub fn delete_recursive(&mut self, hash: &DataUsageHash) {
        let mut visited = HashSet::new();
        self.delete_recursive_guard(hash, &mut visited, 0);
    }

    fn delete_recursive_guard(&mut self, hash: &DataUsageHash, visited: &mut HashSet<String>, depth: usize) {
        if !visited.insert(hash.key()) {
            return;
        }

        let mut need_remove = Vec::new();
        if let Some(v) = self.cache.get(&hash.string()) {
            for child in v.children.iter() {
                need_remove.push(child.clone());
            }
        }
        self.cache.remove(&hash.string());
        if depth >= MAX_DATA_USAGE_CACHE_DEPTH {
            return;
        }
        for child in need_remove {
            self.delete_recursive_guard(&DataUsageHash(child), visited, depth + 1);
        }
    }

    pub fn size_recursive(&self, path: &str) -> Option<DataUsageEntry> {
        match self.find(path) {
            Some(root) => {
                if root.children.is_empty() {
                    return Some(root.clone());
                }
                let mut visited = HashSet::new();
                visited.insert(hash_path(path).key());
                let mut flat = self.flatten_with_guard(root, &mut visited, 0);
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
        let mut visited = HashSet::new();
        visited.insert(hash_path(path).key());
        self.total_children_rec_guard(path, &mut visited, 0)
    }

    fn total_children_rec_guard(&self, path: &str, visited: &mut HashSet<String>, depth: usize) -> usize {
        let Some(root) = self.find(path) else {
            return 0;
        };
        if root.children.is_empty() || depth >= MAX_DATA_USAGE_CACHE_DEPTH {
            return 0;
        }

        let mut n = 0;
        for ch in root.children.iter() {
            if visited.insert(ch.clone()) {
                n += 1 + self.total_children_rec_guard(ch, visited, depth + 1);
            }
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
            buckets_count: u64::try_from(buckets.len()).unwrap_or(u64::MAX),
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
        let loaded = Self::load_cache(store, name).await?;
        *self = loaded.cache;
        Ok(())
    }

    pub(crate) async fn load_with_revisions<S: ScannerObjectIO>(
        &mut self,
        store: Arc<S>,
        name: &str,
    ) -> StorageResult<DataUsageCacheRevisions> {
        let backup_name = format!("{name}.bkp");
        let backup_path = path_join_buf(&[BUCKET_META_PREFIX, &backup_name]);
        let loaded = Self::load_cache(store.clone(), name).await?;
        let backup = match loaded.backup_revision {
            Some(revision) => Some(revision),
            None => match Self::revision_for_path(store, &backup_path).await {
                Ok(revision) => Some(revision),
                Err(err) => {
                    counter!(METRIC_CACHE_BACKUP_REVISION_FAILURE_TOTAL).increment(1);
                    debug!(
                        target: "rustfs::scanner::data_usage",
                        event = EVENT_SCANNER_CACHE_LOAD_STATE,
                        component = LOG_COMPONENT_SCANNER,
                        subsystem = LOG_SUBSYSTEM_CACHE,
                        cache_name = %name,
                        backup_path = %backup_path,
                        state = "backup_revision_unavailable",
                        error = %err,
                        "Scanner cache backup revision lookup failed"
                    );
                    None
                }
            },
        };
        let main = loaded.main_revision;
        *self = loaded.cache;

        Ok(DataUsageCacheRevisions { main, backup })
    }

    async fn load_cache<S: ScannerObjectIO>(store: Arc<S>, name: &str) -> StorageResult<DataUsageCacheLoadResult> {
        let mut last_retryable = None;

        for attempt in 0..5 {
            let main_attempt = Self::try_load_inner(store.clone(), name, Duration::from_secs(60)).await?;
            let main_revision = main_attempt.revision();
            let main_state = match main_attempt {
                DataUsageCacheLoadAttempt::Loaded {
                    cache,
                    revision: Some(main_revision),
                } => {
                    return Ok(DataUsageCacheLoadResult {
                        cache: *cache,
                        main_revision,
                        backup_revision: None,
                    });
                }
                DataUsageCacheLoadAttempt::Loaded { revision: None, .. } => {
                    last_retryable = Some(Error::other(format!("scanner cache object has no revision: {name}")));
                    DataUsageCacheLoadState::Retryable
                }
                DataUsageCacheLoadAttempt::Missing { .. } => DataUsageCacheLoadState::Missing,
                DataUsageCacheLoadAttempt::Corrupt { .. } => DataUsageCacheLoadState::Corrupt,
                DataUsageCacheLoadAttempt::Retryable(err) => {
                    last_retryable = Some(err);
                    DataUsageCacheLoadState::Retryable
                }
            };
            if main_state == DataUsageCacheLoadState::Retryable {
                if attempt < 4 {
                    let sleep_ms: u64 = rand::random::<u64>() % 1000;
                    sleep(Duration::from_millis(sleep_ms)).await;
                }
                continue;
            }

            let backup_name = format!("{name}.bkp");
            let backup_attempt = Self::try_load_inner(store.clone(), &backup_name, Duration::from_secs(30)).await?;
            let backup_revision = backup_attempt.revision();
            let backup_state = match backup_attempt {
                DataUsageCacheLoadAttempt::Loaded {
                    cache,
                    revision: Some(backup_revision),
                } => {
                    if matches!(main_state, DataUsageCacheLoadState::Missing | DataUsageCacheLoadState::Corrupt) {
                        let main_revision = main_revision.ok_or_else(|| {
                            Error::other(format!("scanner cache main revision is unavailable while loading backup: {name}"))
                        })?;
                        return Ok(DataUsageCacheLoadResult {
                            cache: *cache,
                            main_revision,
                            backup_revision: Some(backup_revision),
                        });
                    }
                    DataUsageCacheLoadState::Loaded
                }
                DataUsageCacheLoadAttempt::Loaded { revision: None, .. } => {
                    last_retryable = Some(Error::other(format!("scanner cache backup object has no revision: {backup_name}")));
                    DataUsageCacheLoadState::Retryable
                }
                DataUsageCacheLoadAttempt::Missing { .. } => DataUsageCacheLoadState::Missing,
                DataUsageCacheLoadAttempt::Corrupt { .. } => DataUsageCacheLoadState::Corrupt,
                DataUsageCacheLoadAttempt::Retryable(err) => {
                    last_retryable = Some(err);
                    DataUsageCacheLoadState::Retryable
                }
            };

            match (main_state, backup_state) {
                (DataUsageCacheLoadState::Missing, DataUsageCacheLoadState::Missing) => {
                    return Ok(DataUsageCacheLoadResult {
                        cache: DataUsageCache::default(),
                        main_revision: main_revision
                            .ok_or_else(|| Error::other(format!("scanner cache missing state has no revision: {name}")))?,
                        backup_revision,
                    });
                }
                (DataUsageCacheLoadState::Corrupt, DataUsageCacheLoadState::Missing)
                | (DataUsageCacheLoadState::Missing, DataUsageCacheLoadState::Corrupt)
                | (DataUsageCacheLoadState::Corrupt, DataUsageCacheLoadState::Corrupt) => {
                    warn!(
                        target: "rustfs::scanner::data_usage",
                        event = EVENT_SCANNER_CACHE_LOAD_STATE,
                        component = LOG_COMPONENT_SCANNER,
                        subsystem = LOG_SUBSYSTEM_CACHE,
                        cache_name = %name,
                        state = "corrupt_cache_rebuild",
                        "Scanner cache is corrupt and will be rebuilt"
                    );
                    return Ok(DataUsageCacheLoadResult {
                        cache: DataUsageCache::default(),
                        main_revision: main_revision
                            .ok_or_else(|| Error::other(format!("scanner cache corrupt state has no revision: {name}")))?,
                        backup_revision,
                    });
                }
                _ => {}
            }

            if attempt < 4 {
                let sleep_ms: u64 = rand::random::<u64>() % 1000;
                sleep(Duration::from_millis(sleep_ms)).await;
            }
        }

        warn!(
            target: "rustfs::scanner::data_usage",
            event = EVENT_SCANNER_CACHE_LOAD_STATE,
            component = LOG_COMPONENT_SCANNER,
            subsystem = LOG_SUBSYSTEM_CACHE,
            cache_name = %name,
            retries = 5,
            state = "max_retries_reached",
            "Scanner cache load reached retry limit"
        );
        Err(last_retryable.unwrap_or_else(|| Error::other(format!("scanner cache could not be loaded: {name}"))))
    }

    async fn try_load_inner<S: ScannerObjectIO>(
        store: Arc<S>,
        load_name: &str,
        timeout_duration: Duration,
    ) -> StorageResult<DataUsageCacheLoadAttempt> {
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
                    let revision = reader
                        .object_info
                        .etag
                        .as_ref()
                        .filter(|etag| !etag.is_empty())
                        .cloned()
                        .map(DataUsageCacheRevision::Etag);
                    match reader.read_all().await {
                        Ok(data) => match DataUsageCache::unmarshal(&data) {
                            Ok(cache) => Ok((Some(cache), revision, false)),
                            Err(_) => Ok((None, revision, true)),
                        },
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
                                        Ok(cache) => Ok((Some(cache), Some(DataUsageCacheRevision::Missing), false)),
                                        Err(_) => Ok((None, Some(DataUsageCacheRevision::Missing), true)),
                                    },
                                    Err(e) => Err(e),
                                },
                                Err(inner_err) => match inner_err {
                                    Error::FileNotFound
                                    | Error::VolumeNotFound
                                    | Error::ObjectNotFound(_, _)
                                    | Error::BucketNotFound(_) => {
                                        // Object not found in both locations
                                        Ok((None, Some(DataUsageCacheRevision::Missing), false))
                                    }
                                    Error::ErasureReadQuorum => {
                                        // InsufficientReadQuorum - retry
                                        Err(Error::ErasureReadQuorum)
                                    }
                                    _ => {
                                        // Other storage errors - retry
                                        if matches!(
                                            inner_err,
                                            Error::FaultyDisk | Error::DiskFull | Error::StorageFull | Error::SlowDown
                                        ) {
                                            return Err(inner_err);
                                        }
                                        Err(inner_err)
                                    }
                                },
                            }
                        }
                        Error::ErasureReadQuorum => {
                            // InsufficientReadQuorum - retry
                            Err(Error::ErasureReadQuorum)
                        }
                        _ => {
                            // Other storage errors - retry
                            if matches!(err, Error::FaultyDisk | Error::DiskFull | Error::StorageFull | Error::SlowDown) {
                                return Err(err);
                            }
                            Err(err)
                        }
                    }
                }
            }
        };

        match timeout(timeout_duration, load_fut).await {
            Ok(Ok((Some(cache), revision, _))) => Ok(DataUsageCacheLoadAttempt::Loaded {
                cache: Box::new(cache),
                revision,
            }),
            Ok(Ok((None, revision, true))) => Ok(DataUsageCacheLoadAttempt::Corrupt { revision }),
            Ok(Ok((None, revision, false))) => Ok(DataUsageCacheLoadAttempt::Missing { revision }),
            Ok(Err(err)) => Ok(DataUsageCacheLoadAttempt::Retryable(err)),
            Err(_) => Ok(DataUsageCacheLoadAttempt::Retryable(Error::other("scanner cache load timed out"))),
        }
    }

    async fn revision_for_path<S: ScannerObjectIO>(store: Arc<S>, path: &str) -> StorageResult<DataUsageCacheRevision> {
        match store
            .get_object_reader(
                RUSTFS_META_BUCKET,
                path,
                None,
                HeaderMap::new(),
                &ObjectOptions {
                    no_lock: true,
                    ..Default::default()
                },
            )
            .await
        {
            Ok(reader) => reader
                .object_info
                .etag
                .filter(|etag| !etag.is_empty())
                .map(DataUsageCacheRevision::Etag)
                .ok_or_else(|| StorageError::other(format!("scanner cache object {path} has no ETag"))),
            Err(Error::FileNotFound | Error::VolumeNotFound | Error::ObjectNotFound(_, _) | Error::BucketNotFound(_)) => {
                Ok(DataUsageCacheRevision::Missing)
            }
            Err(err) => Err(err),
        }
    }

    fn cache_save_timeout() -> Duration {
        crate::runtime_config::scanner_cache_save_timeout()
    }

    pub(crate) fn persistence_timeout() -> Duration {
        Self::cache_save_timeout()
            .saturating_mul(DATA_USAGE_CACHE_SAVE_RETRIES + 1)
            .saturating_add(DATA_USAGE_CACHE_SAVE_RETRY_BACKOFF_MAX)
            .saturating_add(Self::backup_cache_save_timeout(Self::cache_save_timeout()))
            .saturating_add(DATA_USAGE_CACHE_PERSISTENCE_MARGIN)
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

    fn should_retry_save_error(err: &StorageError) -> bool {
        // Usage-cache files are best-effort scanner checkpoints. Retrying namespace
        // lock failures immediately only adds more lock traffic to the same hot object.
        !matches!(
            err,
            StorageError::Lock(_)
                | StorageError::NamespaceLockQuorumUnavailable { .. }
                | StorageError::PreconditionFailed
                | StorageError::ObjectNotFound(_, _)
        )
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
                    let should_retry = Self::should_retry_save_error(&e);
                    Self::record_save_attempt(path_type, if should_retry { "error" } else { "lock_error" }, duration);
                    last_err = Some(e);
                    if !should_retry {
                        break;
                    }
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
        revision: Option<DataUsageCacheRevision>,
    ) -> StorageResult<()> {
        Self::ensure_cache_save_metrics_registered();
        let path_type = Self::cache_path_type(path);
        let path = path.to_string();

        let save_result = Self::retry_save_op(path_type, timeout_duration, max_retries, || {
            let store_clone = store.clone();
            let path_clone = path.clone();
            let buf_clone = buf.to_vec();
            let revision = revision.clone();
            async move {
                if let Some(revision) = revision {
                    save_config_with_preconditions(store_clone, &path_clone, buf_clone, revision.preconditions()).await?;
                } else {
                    save_config(store_clone, &path_clone, buf_clone).await?;
                }
                Ok::<(), StorageError>(())
            }
        })
        .await;
        let Err(save_err) = save_result else {
            return Ok(());
        };

        for attempt in 0..=max_retries {
            let reconcile = timeout(timeout_duration, async {
                let mut reader = store
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
                    .await?;
                Ok::<bool, StorageError>(reader.read_all().await? == buf)
            })
            .await;
            if matches!(reconcile, Ok(Ok(true))) {
                Self::record_save_attempt(path_type, "reconciled", Duration::ZERO);
                return Ok(());
            }
            if matches!(reconcile, Ok(Ok(false))) {
                break;
            }
            if attempt < max_retries {
                sleep(Duration::from_millis(50_u64 * (u64::from(attempt) + 1))).await;
            }
        }

        Err(save_err)
    }

    pub async fn save<S: ScannerObjectIO>(&self, store: Arc<S>, name: &str) -> StorageResult<()> {
        self.save_inner(store, name, None).await
    }

    pub(crate) async fn save_with_revisions<S: ScannerObjectIO>(
        &self,
        store: Arc<S>,
        name: &str,
        revisions: &DataUsageCacheRevisions,
    ) -> StorageResult<()> {
        self.save_inner(store, name, Some(revisions)).await
    }

    async fn save_inner<S: ScannerObjectIO>(
        &self,
        store: Arc<S>,
        name: &str,
        revisions: Option<&DataUsageCacheRevisions>,
    ) -> StorageResult<()> {
        let mut buf = Vec::new();
        self.serialize(&mut rmp_serde::Serializer::new(&mut buf))?;
        let timeout_duration = Self::cache_save_timeout();

        let path = path_join_buf(&[BUCKET_META_PREFIX, name]);
        Self::save_path_with_retry(
            store.clone(),
            &path,
            &buf,
            timeout_duration,
            DATA_USAGE_CACHE_SAVE_RETRIES,
            revisions.map(|revisions| revisions.main.clone()),
        )
        .await?;

        let backup_name = format!("{name}.bkp");
        let backup_path = path_join_buf(&[BUCKET_META_PREFIX, &backup_name]);
        let backup_timeout_duration = Self::backup_cache_save_timeout(timeout_duration);
        let backup_revision = revisions.and_then(|revisions| revisions.backup.clone());
        if revisions.is_some() && backup_revision.is_none() {
            return Ok(());
        }
        if let Err(e) = Self::save_path_with_retry(
            store,
            &backup_path,
            &buf,
            backup_timeout_duration,
            DATA_USAGE_CACHE_BACKUP_SAVE_RETRIES,
            backup_revision,
        )
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
    let mut visited = HashSet::new();
    visited.insert(path.key());
    add_with_guard(data_usage_cache, path, candidates, &mut visited, 0)
}

fn add_with_guard(
    data_usage_cache: &DataUsageCache,
    path: &DataUsageHash,
    candidates: &mut Vec<Inner>,
    visited: &mut HashSet<String>,
    depth: usize,
) -> usize {
    let e = match data_usage_cache.cache.get(&path.key()) {
        Some(e) => e,
        None => return 0,
    };
    let mut objects = e.objects;
    if depth < MAX_DATA_USAGE_CACHE_DEPTH {
        for ch in e.children.iter() {
            if visited.insert(ch.clone()) {
                objects += add_with_guard(data_usage_cache, &DataUsageHash(ch.clone()), candidates, visited, depth + 1);
            }
        }
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
    mark_with_depth(duc, entry, found, 0);
}

fn mark_with_depth(duc: &DataUsageCache, entry: &DataUsageEntry, found: &mut HashSet<String>, depth: usize) {
    if depth >= MAX_DATA_USAGE_CACHE_DEPTH {
        return;
    }

    for k in entry.children.iter() {
        if !found.insert(k.to_string()) {
            continue;
        }
        if let Some(ch) = duc.cache.get(k) {
            mark_with_depth(duc, ch, found, depth + 1);
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
        self.total_size = self.total_size.saturating_add(other.total_size);
        self.versions = self.versions.saturating_add(other.versions);
        self.delete_markers = self.delete_markers.saturating_add(other.delete_markers);
        self.replicated_size = self.replicated_size.saturating_add(other.replicated_size);
        self.replicated_count = self.replicated_count.saturating_add(other.replicated_count);
        self.pending_size = self.pending_size.saturating_add(other.pending_size);
        self.failed_size = self.failed_size.saturating_add(other.failed_size);
        self.replica_size = self.replica_size.saturating_add(other.replica_size);
        self.replica_count = self.replica_count.saturating_add(other.replica_count);
        self.pending_count = self.pending_count.saturating_add(other.pending_count);
        self.failed_count = self.failed_count.saturating_add(other.failed_count);

        // Merge replication target stats
        for (target, stats) in &other.repl_target_stats {
            let entry = self.repl_target_stats.entry(target.clone()).or_default();
            entry.replicated_size = entry.replicated_size.saturating_add(stats.replicated_size);
            entry.replicated_count = entry.replicated_count.saturating_add(stats.replicated_count);
            entry.pending_size = entry.pending_size.saturating_add(stats.pending_size);
            entry.failed_size = entry.failed_size.saturating_add(stats.failed_size);
            entry.pending_count = entry.pending_count.saturating_add(stats.pending_count);
            entry.failed_count = entry.failed_count.saturating_add(stats.failed_count);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage_api::scanner_io::{HTTPRangeSpec, ObjectIO};
    use crate::{ScannerGetObjectReader, ScannerPutObjReader};
    use serde_json::Value;
    use std::io::Cursor;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::task::{Context, Poll};
    use temp_env::{with_var, with_var_unset};
    use tokio::io::{AsyncRead, AsyncReadExt, ReadBuf};
    use tokio::sync::Mutex;

    const TEST_PLAN_DIGEST: DataUsageScanPlanDigest = DataUsageScanPlanDigest([3; 32]);

    #[derive(Debug, PartialEq, Eq)]
    struct CachePutRecord {
        object: String,
        if_match: Option<String>,
        if_none_match: Option<String>,
    }

    #[derive(Debug)]
    struct BackupFallbackStore {
        backup: Vec<u8>,
        recovered_main: Vec<u8>,
        main_reads: AtomicUsize,
        recover_main_revision: AtomicBool,
        backup_reads: Mutex<usize>,
        puts: Mutex<Vec<CachePutRecord>>,
    }

    impl BackupFallbackStore {
        fn new(backup: Vec<u8>, recover_main_revision: bool) -> Self {
            Self {
                backup,
                recovered_main: Vec::new(),
                main_reads: AtomicUsize::new(0),
                recover_main_revision: AtomicBool::new(recover_main_revision),
                backup_reads: Mutex::new(0),
                puts: Mutex::new(Vec::new()),
            }
        }

        fn with_recovered_main(backup: Vec<u8>, recovered_main: Vec<u8>) -> Self {
            Self {
                backup,
                recovered_main,
                main_reads: AtomicUsize::new(0),
                recover_main_revision: AtomicBool::new(true),
                backup_reads: Mutex::new(0),
                puts: Mutex::new(Vec::new()),
            }
        }

        fn reader(data: Vec<u8>, etag: &str) -> ScannerGetObjectReader {
            ScannerGetObjectReader {
                stream: Box::new(Cursor::new(data)),
                object_info: ObjectInfo {
                    etag: Some(etag.to_string()),
                    ..Default::default()
                },
                buffered_body: None,
                body_source: Default::default(),
            }
        }
    }

    #[derive(Clone, Debug)]
    enum CacheReadBody {
        Bytes(Vec<u8>),
        PrefixThenError(Vec<u8>),
    }

    #[derive(Debug)]
    struct PrefixThenErrorReader {
        prefix: Cursor<Vec<u8>>,
        failed: bool,
    }

    impl AsyncRead for PrefixThenErrorReader {
        fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
            if self.prefix.position() < u64::try_from(self.prefix.get_ref().len()).unwrap_or(u64::MAX) {
                return Pin::new(&mut self.prefix).poll_read(cx, buf);
            }
            if !self.failed {
                self.failed = true;
                return Poll::Ready(Err(std::io::Error::other("injected cache body read failure")));
            }
            Poll::Ready(Ok(()))
        }
    }

    #[derive(Debug)]
    struct CacheReadStore {
        main: CacheReadBody,
        backup: Option<Vec<u8>>,
        puts: AtomicUsize,
    }

    #[derive(Debug, Default)]
    struct AmbiguousCacheCommitStore {
        data: Mutex<Option<Vec<u8>>>,
        puts: AtomicUsize,
    }

    impl CacheReadStore {
        fn new(main: CacheReadBody, backup: Option<Vec<u8>>) -> Self {
            Self {
                main,
                backup,
                puts: AtomicUsize::new(0),
            }
        }

        fn reader(body: CacheReadBody, etag: &str) -> ScannerGetObjectReader {
            let stream: Box<dyn AsyncRead + Unpin + Send + Sync> = match body {
                CacheReadBody::Bytes(data) => Box::new(Cursor::new(data)),
                CacheReadBody::PrefixThenError(prefix) => Box::new(PrefixThenErrorReader {
                    prefix: Cursor::new(prefix),
                    failed: false,
                }),
            };
            ScannerGetObjectReader {
                stream,
                object_info: ObjectInfo {
                    etag: Some(etag.to_string()),
                    ..Default::default()
                },
                buffered_body: None,
                body_source: Default::default(),
            }
        }
    }

    #[async_trait::async_trait]
    impl ObjectIO for CacheReadStore {
        type Error = Error;
        type RangeSpec = HTTPRangeSpec;
        type HeaderMap = HeaderMap;
        type ObjectOptions = ObjectOptions;
        type ObjectInfo = ObjectInfo;
        type GetObjectReader = ScannerGetObjectReader;
        type PutObjectReader = ScannerPutObjReader;

        async fn get_object_reader(
            &self,
            bucket: &str,
            object: &str,
            _range: Option<Self::RangeSpec>,
            _h: Self::HeaderMap,
            _opts: &Self::ObjectOptions,
        ) -> StorageResult<Self::GetObjectReader> {
            if bucket != RUSTFS_META_BUCKET {
                return Err(Error::FileNotFound);
            }

            let main_path = path_join_buf(&[BUCKET_META_PREFIX, DATA_USAGE_CACHE_NAME]);
            let backup_path = format!("{main_path}.bkp");
            if object == main_path {
                return Ok(Self::reader(self.main.clone(), "main-etag"));
            }
            if object == backup_path {
                return self
                    .backup
                    .clone()
                    .map(|data| Self::reader(CacheReadBody::Bytes(data), "backup-etag"))
                    .ok_or(Error::FileNotFound);
            }
            Err(Error::FileNotFound)
        }

        async fn put_object(
            &self,
            _bucket: &str,
            _object: &str,
            _data: &mut Self::PutObjectReader,
            _opts: &Self::ObjectOptions,
        ) -> StorageResult<Self::ObjectInfo> {
            self.puts.fetch_add(1, Ordering::SeqCst);
            Ok(ObjectInfo::default())
        }
    }

    #[async_trait::async_trait]
    impl ObjectIO for AmbiguousCacheCommitStore {
        type Error = Error;
        type RangeSpec = HTTPRangeSpec;
        type HeaderMap = HeaderMap;
        type ObjectOptions = ObjectOptions;
        type ObjectInfo = ObjectInfo;
        type GetObjectReader = ScannerGetObjectReader;
        type PutObjectReader = ScannerPutObjReader;

        async fn get_object_reader(
            &self,
            bucket: &str,
            object: &str,
            _range: Option<Self::RangeSpec>,
            _h: Self::HeaderMap,
            _opts: &Self::ObjectOptions,
        ) -> StorageResult<Self::GetObjectReader> {
            let expected_path = path_join_buf(&[BUCKET_META_PREFIX, DATA_USAGE_CACHE_NAME]);
            if bucket != RUSTFS_META_BUCKET || object != expected_path {
                return Err(Error::FileNotFound);
            }
            let data = self.data.lock().await.clone().ok_or(Error::FileNotFound)?;
            Ok(CacheReadStore::reader(CacheReadBody::Bytes(data), "committed-etag"))
        }

        async fn put_object(
            &self,
            bucket: &str,
            object: &str,
            data: &mut Self::PutObjectReader,
            _opts: &Self::ObjectOptions,
        ) -> StorageResult<Self::ObjectInfo> {
            let expected_path = path_join_buf(&[BUCKET_META_PREFIX, DATA_USAGE_CACHE_NAME]);
            if bucket != RUSTFS_META_BUCKET || object != expected_path {
                return Err(Error::FileNotFound);
            }
            let mut bytes = Vec::new();
            data.stream.read_to_end(&mut bytes).await?;
            *self.data.lock().await = Some(bytes);
            self.puts.fetch_add(1, Ordering::SeqCst);
            Err(StorageError::PreconditionFailed)
        }
    }

    #[async_trait::async_trait]
    impl ObjectIO for BackupFallbackStore {
        type Error = Error;
        type RangeSpec = HTTPRangeSpec;
        type HeaderMap = HeaderMap;
        type ObjectOptions = ObjectOptions;
        type ObjectInfo = ObjectInfo;
        type GetObjectReader = ScannerGetObjectReader;
        type PutObjectReader = ScannerPutObjReader;

        async fn get_object_reader(
            &self,
            bucket: &str,
            object: &str,
            _range: Option<Self::RangeSpec>,
            _h: Self::HeaderMap,
            _opts: &Self::ObjectOptions,
        ) -> StorageResult<Self::GetObjectReader> {
            if bucket != RUSTFS_META_BUCKET {
                return Err(Error::FileNotFound);
            }

            let main_path = path_join_buf(&[BUCKET_META_PREFIX, DATA_USAGE_CACHE_NAME]);
            let backup_path = format!("{main_path}.bkp");
            if object == main_path {
                let read = self.main_reads.fetch_add(1, Ordering::SeqCst);
                if read == 0 || !self.recover_main_revision.load(Ordering::SeqCst) {
                    return Err(Error::ErasureReadQuorum);
                }
                return Ok(Self::reader(self.recovered_main.clone(), "main-etag"));
            }
            if object == backup_path {
                *self.backup_reads.lock().await += 1;
                return Ok(Self::reader(self.backup.clone(), "backup-etag"));
            }

            Err(Error::FileNotFound)
        }

        async fn put_object(
            &self,
            bucket: &str,
            object: &str,
            _data: &mut Self::PutObjectReader,
            opts: &Self::ObjectOptions,
        ) -> StorageResult<Self::ObjectInfo> {
            if bucket != RUSTFS_META_BUCKET {
                return Err(Error::FileNotFound);
            }
            let if_match = opts
                .http_preconditions
                .as_ref()
                .and_then(HTTPPreconditions::if_match_value)
                .map(str::to_owned);
            let if_none_match = opts
                .http_preconditions
                .as_ref()
                .and_then(HTTPPreconditions::if_none_match_value)
                .map(str::to_owned);
            self.puts.lock().await.push(CachePutRecord {
                object: object.to_string(),
                if_match,
                if_none_match,
            });
            Ok(ObjectInfo {
                etag: Some(format!("saved-{object}")),
                ..Default::default()
            })
        }
    }

    #[test]
    fn cache_revisions_map_to_compare_and_swap_preconditions() {
        let missing = DataUsageCacheRevision::Missing.preconditions();
        let existing = DataUsageCacheRevision::Etag("etag-1".to_string()).preconditions();

        assert_eq!(missing.if_none_match_value(), Some("*"));
        assert!(missing.if_match_value().is_none());
        assert_eq!(existing.if_match_value(), Some("etag-1"));
        assert!(existing.if_none_match_value().is_none());
    }

    #[tokio::test]
    async fn backup_cache_load_recovers_main_revision_before_cas_save() {
        let mut expected = DataUsageCache::default();
        expected.info.name = "bucket".to_string();
        let store = Arc::new(BackupFallbackStore::new(expected.marshal_msg().expect("serialize backup cache"), true));
        let mut loaded = DataUsageCache::default();

        let revisions = loaded
            .load_with_revisions(store.clone(), DATA_USAGE_CACHE_NAME)
            .await
            .expect("backup cache should load after the main revision recovers");

        assert_eq!(loaded.info.name, "bucket");
        assert_eq!(store.main_reads.load(Ordering::SeqCst), 2);
        assert!(matches!(revisions.main, DataUsageCacheRevision::Etag(ref etag) if etag == "main-etag"));
        assert!(matches!(
            revisions.backup,
            Some(DataUsageCacheRevision::Etag(ref etag)) if etag == "backup-etag"
        ));
        assert_eq!(*store.backup_reads.lock().await, 1);

        loaded
            .save_with_revisions(store.clone(), DATA_USAGE_CACHE_NAME, &revisions)
            .await
            .expect("recovered revisions should protect both cache writes");
        let puts = store.puts.lock().await;
        assert_eq!(
            *puts,
            vec![
                CachePutRecord {
                    object: path_join_buf(&[BUCKET_META_PREFIX, DATA_USAGE_CACHE_NAME]),
                    if_match: Some("main-etag".to_string()),
                    if_none_match: None,
                },
                CachePutRecord {
                    object: path_join_buf(&[BUCKET_META_PREFIX, &format!("{DATA_USAGE_CACHE_NAME}.bkp")]),
                    if_match: Some("backup-etag".to_string()),
                    if_none_match: None,
                },
            ]
        );
    }

    #[tokio::test]
    async fn recovered_main_cache_wins_over_stale_backup() {
        let mut main = DataUsageCache::default();
        main.info.name = "current-main".to_string();
        let mut backup = DataUsageCache::default();
        backup.info.name = "stale-backup".to_string();
        let store = Arc::new(BackupFallbackStore::with_recovered_main(
            backup.marshal_msg().expect("serialize stale backup cache"),
            main.marshal_msg().expect("serialize recovered main cache"),
        ));
        let mut loaded = DataUsageCache::default();

        let revisions = loaded
            .load_with_revisions(store.clone(), DATA_USAGE_CACHE_NAME)
            .await
            .expect("a recovered valid main cache should supersede the fallback backup");

        assert_eq!(loaded.info.name, "current-main");
        assert_eq!(store.main_reads.load(Ordering::SeqCst), 2);
        assert_eq!(*store.backup_reads.lock().await, 1);
        assert!(matches!(revisions.main, DataUsageCacheRevision::Etag(ref etag) if etag == "main-etag"));
        assert!(matches!(
            revisions.backup,
            Some(DataUsageCacheRevision::Etag(ref etag)) if etag == "backup-etag"
        ));
    }

    #[tokio::test]
    async fn cache_save_reconciles_an_ambiguous_committed_write() {
        let store = Arc::new(AmbiguousCacheCommitStore::default());
        let mut cache = DataUsageCache::default();
        cache.info.name = "bucket".to_string();
        cache.replace("bucket", "", DataUsageEntry::default());
        let revisions = DataUsageCacheRevisions {
            main: DataUsageCacheRevision::Missing,
            backup: None,
        };

        cache
            .save_with_revisions(store.clone(), DATA_USAGE_CACHE_NAME, &revisions)
            .await
            .expect("read-after-error reconciliation should recognize the committed cache");

        assert_eq!(store.puts.load(Ordering::SeqCst), 1);
        let persisted = store.data.lock().await.clone().expect("cache should be committed");
        assert_eq!(
            DataUsageCache::unmarshal(&persisted)
                .expect("committed cache should decode")
                .info
                .name,
            "bucket"
        );
    }

    #[tokio::test]
    async fn backup_cache_load_fails_closed_without_main_revision_quorum() {
        let backup = DataUsageCache::default().marshal_msg().expect("serialize backup cache");
        let store = Arc::new(BackupFallbackStore::new(backup, false));
        let mut loaded = DataUsageCache::default();

        let error = loaded
            .load_with_revisions(store.clone(), DATA_USAGE_CACHE_NAME)
            .await
            .expect_err("missing main revision quorum must prevent a CAS save");

        assert!(matches!(error, Error::ErasureReadQuorum));
        assert_eq!(store.main_reads.load(Ordering::SeqCst), 5);
        assert_eq!(*store.backup_reads.lock().await, 0);
    }

    #[tokio::test]
    async fn corrupt_primary_cache_loads_valid_backup() {
        let mut expected = DataUsageCache::default();
        expected.info.name = "recovered".to_string();
        let store = Arc::new(CacheReadStore::new(
            CacheReadBody::Bytes(b"not-msgpack".to_vec()),
            Some(expected.marshal_msg().expect("serialize backup cache")),
        ));
        let mut loaded = DataUsageCache::default();

        loaded
            .load(store.clone(), DATA_USAGE_CACHE_NAME)
            .await
            .expect("valid backup must recover a corrupt primary cache");

        assert_eq!(loaded.info.name, "recovered");
        assert_eq!(store.puts.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn corrupt_cache_without_valid_backup_rebuilds_with_cas_revisions() {
        for backup in [None, Some(b"also-not-msgpack".to_vec())] {
            let store = Arc::new(CacheReadStore::new(CacheReadBody::Bytes(b"not-msgpack".to_vec()), backup));
            let mut loaded = DataUsageCache::default();

            let revisions = loaded
                .load_with_revisions(store.clone(), DATA_USAGE_CACHE_NAME)
                .await
                .expect("corrupt scanner caches should be discarded for a full rebuild");

            assert!(loaded.info.name.is_empty());
            assert!(loaded.cache.is_empty());
            assert!(matches!(
                revisions.main,
                DataUsageCacheRevision::Etag(ref etag) if etag == "main-etag"
            ));
            assert!(matches!(
                revisions.backup,
                Some(DataUsageCacheRevision::Missing | DataUsageCacheRevision::Etag(_))
            ));

            loaded
                .save_with_revisions(store.clone(), DATA_USAGE_CACHE_NAME, &revisions)
                .await
                .expect("rebuilt cache should replace corrupt cache objects with CAS protection");
            assert_eq!(store.puts.load(Ordering::SeqCst), 2);
        }
    }

    #[tokio::test]
    async fn partial_cache_body_read_is_retryable_and_does_not_save() {
        let store = Arc::new(CacheReadStore::new(CacheReadBody::PrefixThenError(vec![0x81, 0xa4, b'n', b'a']), None));

        let attempt = DataUsageCache::try_load_inner(store.clone(), DATA_USAGE_CACHE_NAME, Duration::from_secs(1))
            .await
            .expect("body read failures should remain recoverable load attempts");

        assert!(matches!(attempt, DataUsageCacheLoadAttempt::Retryable(_)));
        assert_eq!(store.puts.load(Ordering::SeqCst), 0);
    }

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
    fn size_summary_add_saturates_all_usage_counters() {
        let target = "arn:minio:replication::target".to_string();
        let mut summary = SizeSummary {
            total_size: usize::MAX,
            versions: usize::MAX,
            delete_markers: usize::MAX,
            replicated_size: i64::MAX,
            replicated_count: usize::MAX,
            pending_size: i64::MAX,
            failed_size: i64::MAX,
            replica_size: i64::MAX,
            replica_count: usize::MAX,
            pending_count: usize::MAX,
            failed_count: usize::MAX,
            ..Default::default()
        };
        summary.repl_target_stats.insert(
            target.clone(),
            ReplTargetSizeSummary {
                replicated_size: i64::MAX,
                replicated_count: usize::MAX,
                pending_size: i64::MAX,
                failed_size: i64::MAX,
                pending_count: usize::MAX,
                failed_count: usize::MAX,
            },
        );

        let mut increment = SizeSummary {
            total_size: 1,
            versions: 1,
            delete_markers: 1,
            replicated_size: 1,
            replicated_count: 1,
            pending_size: 1,
            failed_size: 1,
            replica_size: 1,
            replica_count: 1,
            pending_count: 1,
            failed_count: 1,
            ..Default::default()
        };
        increment.repl_target_stats.insert(
            target.clone(),
            ReplTargetSizeSummary {
                replicated_size: 1,
                replicated_count: 1,
                pending_size: 1,
                failed_size: 1,
                pending_count: 1,
                failed_count: 1,
            },
        );

        summary.add(&increment);

        assert_eq!(summary.total_size, usize::MAX);
        assert_eq!(summary.versions, usize::MAX);
        assert_eq!(summary.delete_markers, usize::MAX);
        assert_eq!(summary.replicated_size, i64::MAX);
        assert_eq!(summary.replicated_count, usize::MAX);
        assert_eq!(summary.pending_size, i64::MAX);
        assert_eq!(summary.failed_size, i64::MAX);
        assert_eq!(summary.replica_size, i64::MAX);
        assert_eq!(summary.replica_count, usize::MAX);
        assert_eq!(summary.pending_count, usize::MAX);
        assert_eq!(summary.failed_count, usize::MAX);

        let target_summary = summary
            .repl_target_stats
            .get(&target)
            .expect("replication target summary should remain present");
        assert_eq!(target_summary.replicated_size, i64::MAX);
        assert_eq!(target_summary.replicated_count, usize::MAX);
        assert_eq!(target_summary.pending_size, i64::MAX);
        assert_eq!(target_summary.failed_size, i64::MAX);
        assert_eq!(target_summary.pending_count, usize::MAX);
        assert_eq!(target_summary.failed_count, usize::MAX);
    }

    #[test]
    fn size_summary_counts_delete_markers_separately_from_versions() {
        let mut summary = SizeSummary::new();
        let marker = ObjectInfo {
            delete_marker: true,
            version_id: Some(uuid::Uuid::new_v4()),
            ..Default::default()
        };

        summary.actions_accounting(&marker, 0, 0);

        assert_eq!(summary.delete_markers, 1);
        assert_eq!(summary.versions, 0);
        assert_eq!(summary.total_size, 0);
    }

    #[test]
    fn size_summary_actions_accounting_accumulates_tier_stats() {
        let mut summary = SizeSummary::new();
        summary
            .tier_stats
            .insert(storageclass::STANDARD.to_string(), TierStats::default());

        let object = ObjectInfo {
            storage_class: Some(storageclass::STANDARD.to_string()),
            size: 10,
            is_latest: true,
            ..Default::default()
        };

        summary.actions_accounting(&object, 10, 10);
        summary.actions_accounting(&object, 10, 10);

        let stats = summary
            .tier_stats
            .get(storageclass::STANDARD)
            .expect("standard tier stats should remain present");
        assert_eq!(
            *stats,
            TierStats {
                total_size: 20,
                num_versions: 2,
                num_objects: 2,
            }
        );
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
        assert_eq!(decoded.leader_epoch, 0);
        assert!(decoded.scan_resume_after.is_none());
        assert!(decoded.scan_checkpoint.is_none());
        assert!(decoded.object_lock.is_none());
        assert!(decoded.pending_heals.is_empty());
        assert!(decoded.source.is_none());
        assert!(!decoded.snapshot_complete);
        assert!(decoded.scan_plan_digest.is_none());
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
        assert!(decoded.pending_heals.is_empty());
        assert!(decoded.source.is_none());
        assert!(!decoded.snapshot_complete);
        assert!(decoded.scan_plan_digest.is_none());
    }

    #[test]
    fn test_new_data_usage_cache_msgpack_round_trips_and_supports_old_reader() {
        #[derive(Deserialize)]
        struct OldDataUsageCacheInfo {
            name: String,
            next_cycle: u64,
            last_update: Option<SystemTime>,
            skip_healing: bool,
            lifecycle: Option<Arc<BucketLifecycleConfiguration>>,
            replication: Option<Arc<ReplicationConfig>>,
            failed_objects: HashMap<String, u64>,
            scan_resume_after: Option<String>,
            scan_checkpoint: Option<DataUsageScanCheckpoint>,
            pending_heals: Vec<PendingScannerHeal>,
            object_lock: Option<Arc<ObjectLockConfiguration>>,
        }

        #[derive(Deserialize)]
        struct OldDataUsageCache {
            info: OldDataUsageCacheInfo,
            cache: HashMap<String, DataUsageEntry>,
        }

        let mut cache = DataUsageCache {
            info: DataUsageCacheInfo {
                name: "bucket".to_string(),
                next_cycle: 7,
                leader_epoch: 9,
                skip_healing: true,
                failed_objects: HashMap::from([("bad-object".to_string(), 11)]),
                source: Some(DataUsageCacheSource::new(1, 2)),
                snapshot_complete: true,
                scan_plan_digest: Some(TEST_PLAN_DIGEST),
                ..Default::default()
            },
            ..Default::default()
        };
        cache.replace(
            "bucket",
            "",
            DataUsageEntry {
                objects: 3,
                ..Default::default()
            },
        );
        let buf = cache.marshal_msg().expect("Failed to serialize new cache");
        let current = DataUsageCache::unmarshal(&buf).expect("Current reader failed to deserialize new cache");
        assert_eq!(current.info.leader_epoch, 9);
        assert_eq!(current.info.source, Some(DataUsageCacheSource::new(1, 2)));
        assert!(current.info.snapshot_complete);
        assert_eq!(current.info.scan_plan_digest, Some(TEST_PLAN_DIGEST));
        assert_eq!(current.find("bucket").map(|entry| entry.objects), Some(3));

        let decoded: OldDataUsageCache = rmp_serde::from_slice(&buf).expect("Old reader failed to deserialize new cache");

        assert_eq!(decoded.info.name, "bucket");
        assert_eq!(decoded.info.next_cycle, 7);
        assert!(decoded.info.last_update.is_none());
        assert!(decoded.info.skip_healing);
        assert!(decoded.info.lifecycle.is_none());
        assert!(decoded.info.replication.is_none());
        assert_eq!(decoded.info.failed_objects.get("bad-object"), Some(&11));
        assert!(decoded.info.scan_resume_after.is_none());
        assert!(decoded.info.scan_checkpoint.is_none());
        assert!(decoded.info.pending_heals.is_empty());
        assert!(decoded.info.object_lock.is_none());
        assert_eq!(decoded.cache.get("bucket").map(|entry| entry.objects), Some(3));
    }

    #[test]
    fn data_usage_cache_prepare_for_scan_rejects_unscoped_distributed_cache() {
        let mut cache = DataUsageCache {
            info: DataUsageCacheInfo {
                name: "bucket".to_string(),
                next_cycle: 7,
                scan_resume_after: Some("bucket/prefix".to_string()),
                ..Default::default()
            },
            ..Default::default()
        };
        cache.replace(
            "bucket",
            "",
            DataUsageEntry {
                objects: 3,
                ..Default::default()
            },
        );

        let reused = cache.prepare_for_scan("bucket", 8, 0, DataUsageCacheSource::new(1, 0), TEST_PLAN_DIGEST, true);

        assert_eq!(reused, DataUsageCachePrepareOutcome::Reset);
        assert_eq!(cache.info.name, "bucket");
        assert_eq!(cache.info.next_cycle, 8);
        assert_eq!(cache.info.source, Some(DataUsageCacheSource::new(1, 0)));
        assert_eq!(cache.info.scan_plan_digest, Some(TEST_PLAN_DIGEST));
        assert!(!cache.info.snapshot_complete);
        assert!(cache.info.scan_resume_after.is_none());
        assert!(cache.cache.is_empty());
    }

    #[test]
    fn data_usage_cache_prepare_for_scan_preserves_matching_partial_progress() {
        let source = DataUsageCacheSource::new(1, 0);
        let mut cache = DataUsageCache {
            info: DataUsageCacheInfo {
                name: "bucket".to_string(),
                next_cycle: 7,
                scan_resume_after: Some("bucket/prefix".to_string()),
                source: Some(source),
                snapshot_complete: false,
                scan_plan_digest: Some(TEST_PLAN_DIGEST),
                ..Default::default()
            },
            ..Default::default()
        };
        cache.replace(
            "bucket",
            "",
            DataUsageEntry {
                objects: 3,
                ..Default::default()
            },
        );

        let reused = cache.prepare_for_scan("bucket", 8, 0, source, TEST_PLAN_DIGEST, true);

        assert_eq!(reused, DataUsageCachePrepareOutcome::Reused);
        assert_eq!(cache.info.scan_resume_after.as_deref(), Some("bucket/prefix"));
        assert_eq!(cache.find("bucket").map(|entry| entry.objects), Some(3));
        assert_eq!(cache.info.next_cycle, 8);
        assert_eq!(cache.info.source, Some(source));
        assert_eq!(cache.info.scan_plan_digest, Some(TEST_PLAN_DIGEST));
        assert!(!cache.info.snapshot_complete);
    }

    #[test]
    fn data_usage_cache_prepare_for_scan_rejects_legacy_cache_without_a_bucket_plan() {
        let source = DataUsageCacheSource::new(0, 0);
        let mut cache = DataUsageCache {
            info: DataUsageCacheInfo {
                name: "bucket".to_string(),
                next_cycle: 7,
                ..Default::default()
            },
            ..Default::default()
        };
        cache.replace(
            "bucket",
            "",
            DataUsageEntry {
                objects: 3,
                ..Default::default()
            },
        );

        let reused = cache.prepare_for_scan("bucket", 8, 0, source, TEST_PLAN_DIGEST, false);

        assert_eq!(reused, DataUsageCachePrepareOutcome::Reset);
        assert!(cache.find("bucket").is_none());
        assert_eq!(cache.info.source, Some(source));
        assert_eq!(cache.info.scan_plan_digest, Some(TEST_PLAN_DIGEST));
        assert!(!cache.info.snapshot_complete);
    }

    #[test]
    fn data_usage_cache_prepare_for_scan_rejects_a_different_bucket_plan() {
        let source = DataUsageCacheSource::new(1, 0);
        let mut cache = DataUsageCache {
            info: DataUsageCacheInfo {
                name: "bucket".to_string(),
                next_cycle: 7,
                source: Some(source),
                scan_plan_digest: Some(TEST_PLAN_DIGEST),
                scan_resume_after: Some("bucket/prefix".to_string()),
                ..Default::default()
            },
            ..Default::default()
        };
        cache.replace(
            "bucket",
            "",
            DataUsageEntry {
                objects: 3,
                ..Default::default()
            },
        );

        let next_plan = DataUsageScanPlanDigest([4; 32]);
        let reused = cache.prepare_for_scan("bucket", 8, 0, source, next_plan, true);

        assert_eq!(reused, DataUsageCachePrepareOutcome::Reset);
        assert_eq!(cache.info.scan_plan_digest, Some(next_plan));
        assert!(cache.info.scan_resume_after.is_none());
        assert!(cache.cache.is_empty());
    }

    #[test]
    fn data_usage_cache_prepare_for_scan_rejects_cycle_regression_without_mutation() {
        let source = DataUsageCacheSource::new(1, 0);
        let mut cache = DataUsageCache {
            info: DataUsageCacheInfo {
                name: "bucket".to_string(),
                next_cycle: 8,
                source: Some(source),
                snapshot_complete: true,
                scan_plan_digest: Some(TEST_PLAN_DIGEST),
                ..Default::default()
            },
            ..Default::default()
        };
        cache.replace(
            "bucket",
            "",
            DataUsageEntry {
                objects: 3,
                ..Default::default()
            },
        );

        let outcome = cache.prepare_for_scan("bucket", 7, 0, source, DataUsageScanPlanDigest([4; 32]), true);

        assert_eq!(outcome, DataUsageCachePrepareOutcome::RejectedNewerCycle);
        assert_eq!(cache.info.next_cycle, 8);
        assert_eq!(cache.info.source, Some(source));
        assert_eq!(cache.info.scan_plan_digest, Some(TEST_PLAN_DIGEST));
        assert!(cache.info.snapshot_complete);
        assert_eq!(cache.find("bucket").map(|entry| entry.objects), Some(3));
    }

    #[test]
    fn data_usage_cache_prepare_for_scan_fences_leader_epochs() {
        let source = DataUsageCacheSource::new(1, 0);
        let mut cache = DataUsageCache {
            info: DataUsageCacheInfo {
                name: "bucket".to_string(),
                next_cycle: 8,
                leader_epoch: 11,
                source: Some(source),
                snapshot_complete: true,
                scan_plan_digest: Some(TEST_PLAN_DIGEST),
                ..Default::default()
            },
            ..Default::default()
        };
        cache.replace("bucket", "", DataUsageEntry::default());

        let stale = cache.prepare_for_scan("bucket", 8, 10, source, TEST_PLAN_DIGEST, true);
        assert_eq!(stale, DataUsageCachePrepareOutcome::RejectedNewerLeader);
        assert_eq!(cache.info.leader_epoch, 11);
        assert!(cache.info.snapshot_complete);

        let replacement = cache.prepare_for_scan("bucket", 8, 12, source, TEST_PLAN_DIGEST, true);
        assert_eq!(replacement, DataUsageCachePrepareOutcome::Reset);
        assert_eq!(cache.info.leader_epoch, 12);
        assert!(!cache.info.snapshot_complete);
        assert!(cache.cache.is_empty());
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
    fn test_data_usage_cache_recursive_helpers_tolerate_cycles() {
        let root_hash = hash_path("bucket");
        let child_hash = hash_path("bucket/a");

        let mut cache = DataUsageCache {
            info: DataUsageCacheInfo {
                name: "bucket".to_string(),
                ..Default::default()
            },
            ..Default::default()
        };
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
        cache.cache.entry(child_hash.key()).or_default().add_child(&root_hash);

        assert_eq!(cache.total_children_rec("bucket"), 1);

        let flat = cache.size_recursive("bucket").expect("cyclic cache should still flatten");
        assert_eq!(flat.objects, 2);
        assert_eq!(flat.size, 20);
        assert!(flat.children.is_empty());

        let mut copied = DataUsageCache {
            info: cache.info.clone(),
            ..Default::default()
        };
        copied.copy_with_children(&cache, &root_hash, &None);
        assert!(copied.cache.contains_key(&root_hash.key()));
        assert!(copied.cache.contains_key(&child_hash.key()));

        copied.delete_recursive(&root_hash);
        assert!(!copied.cache.contains_key(&root_hash.key()));
        assert!(!copied.cache.contains_key(&child_hash.key()));
    }

    #[test]
    fn test_data_usage_cache_flatten_does_not_count_root_twice_in_cycle() {
        let root_hash = hash_path("bucket");
        let child_hash = hash_path("bucket/a");

        let mut cache = DataUsageCache {
            info: DataUsageCacheInfo {
                name: "bucket".to_string(),
                ..Default::default()
            },
            ..Default::default()
        };
        cache.replace_hashed(
            &root_hash,
            &None,
            &DataUsageEntry {
                objects: 1,
                size: 10,
                ..Default::default()
            },
        );
        cache.replace_hashed(
            &child_hash,
            &Some(root_hash.clone()),
            &DataUsageEntry {
                objects: 2,
                size: 20,
                ..Default::default()
            },
        );
        cache.cache.entry(child_hash.key()).or_default().add_child(&root_hash);

        let flat = cache.size_recursive("bucket").expect("cyclic cache should still flatten");

        assert_eq!(flat.objects, 3);
        assert_eq!(flat.size, 30);
        assert!(flat.children.is_empty());
    }

    #[test]
    fn checked_flatten_rejects_dangling_child() {
        let root_key = hash_path("bucket").key();
        let mut cache = DataUsageCache::default();
        cache.cache.insert(
            root_key,
            DataUsageEntry {
                objects: 1,
                children: HashSet::from(["missing-child".to_string()]),
                ..Default::default()
            },
        );

        assert!(
            cache.checked_flatten("bucket").is_none(),
            "a missing child must invalidate an exact usage snapshot"
        );
    }

    #[test]
    fn checked_flatten_accepts_depth_limit_and_rejects_deeper_tree() {
        let root_key = hash_path("bucket").key();
        let mut cache = DataUsageCache::default();
        cache.cache.insert(
            root_key.clone(),
            DataUsageEntry {
                objects: 1,
                ..Default::default()
            },
        );

        let mut parent = root_key;
        for depth in 1..=MAX_DATA_USAGE_CACHE_DEPTH {
            let child = format!("depth-{depth}");
            cache
                .cache
                .get_mut(&parent)
                .expect("parent should exist")
                .children
                .insert(child.clone());
            cache.cache.insert(
                child.clone(),
                DataUsageEntry {
                    objects: 1,
                    ..Default::default()
                },
            );
            parent = child;
        }

        let flattened = cache
            .checked_flatten("bucket")
            .expect("a tree ending at the configured depth limit should be valid");
        assert_eq!(flattened.objects, MAX_DATA_USAGE_CACHE_DEPTH + 1);

        let too_deep = "depth-too-deep".to_string();
        cache
            .cache
            .get_mut(&parent)
            .expect("last valid node should exist")
            .children
            .insert(too_deep.clone());
        cache.cache.insert(
            too_deep,
            DataUsageEntry {
                objects: 1,
                ..Default::default()
            },
        );

        assert!(
            cache.checked_flatten("bucket").is_none(),
            "a tree deeper than the configured limit must be rejected"
        );
    }

    #[test]
    fn test_find_children_copy_preserves_missing_entry_behavior() {
        let mut cache = DataUsageCache::default();
        let missing_hash = hash_path("missing");

        assert!(cache.find_children_copy(missing_hash.clone()).is_empty());
        assert!(cache.cache.contains_key(&missing_hash.key()));
    }

    #[test]
    fn test_dui_bucket_count_uses_bucket_list_after_compaction() {
        let root_hash = hash_path("root");
        let mut cache = DataUsageCache {
            info: DataUsageCacheInfo {
                name: "root".to_string(),
                ..Default::default()
            },
            ..Default::default()
        };
        cache.replace_hashed(
            &root_hash,
            &None,
            &DataUsageEntry {
                compacted: true,
                objects: 3,
                ..Default::default()
            },
        );

        let buckets = vec!["bucket-a".to_string(), "bucket-b".to_string()];
        let info = cache.dui("root", &buckets);

        assert_eq!(info.buckets_count, 2);
        assert!(info.buckets_usage.is_empty());
        assert_eq!(info.objects_total_count, 3);
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

    #[test]
    fn test_cache_persistence_timeout_covers_all_save_attempts() {
        with_var(ENV_SCANNER_CACHE_SAVE_TIMEOUT_SECS, Some("7"), || {
            crate::runtime_config::refresh_scanner_runtime_config_for_tests();
            assert_eq!(DataUsageCache::persistence_timeout(), Duration::from_millis(31_350));
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

    #[tokio::test]
    async fn test_retry_save_op_does_not_retry_namespace_lock_errors() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let attempts_clone = attempts.clone();

        let result =
            DataUsageCache::retry_save_op("main", Duration::from_millis(200), DATA_USAGE_CACHE_SAVE_RETRIES, move || {
                let attempts = attempts_clone.clone();
                async move {
                    attempts.fetch_add(1, Ordering::SeqCst);
                    Err(StorageError::NamespaceLockQuorumUnavailable {
                        mode: "write",
                        bucket: RUSTFS_META_BUCKET.to_string(),
                        object: "buckets/.usage-cache.bin".to_string(),
                        required: 2,
                        achieved: 1,
                    })
                }
            })
            .await;

        assert!(matches!(result, Err(StorageError::NamespaceLockQuorumUnavailable { .. })));
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
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
