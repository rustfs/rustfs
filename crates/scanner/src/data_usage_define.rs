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
    AllTierStats, BucketTargetUsageInfo, BucketUsageInfo, DATA_USAGE_OBJECT_NAME, DATA_USAGE_OBSERVED_OBJECT_NAME,
    DataUsageEntry, DataUsageHash, DataUsageHashMap, DataUsageInfo, DataUsageSnapshotSetState, LEGACY_DATA_USAGE_OBJECT_NAME,
    PrefixUsageEntry, PrefixUsageQuery, PrefixUsageSummary, ReplTargetSizeSummary, SizeReconciliationEntry,
    SizeReconciliationScope, SizeSummary, TierStats, hash_path, prefix_usage_in_cache,
};
use rustfs_utils::path::{SLASH_SEPARATOR, path_join_buf};
use tokio::time::{Duration, Instant, sleep, timeout};
use tracing::{debug, warn};

use crate::storage_api::owner::HTTPPreconditions;
use crate::{
    BUCKET_META_PREFIX, EcstoreError as Error, EcstoreResult as StorageResult, RUSTFS_META_BUCKET, ReplicationConfig,
    SCANNER_PUBLICATION_EPOCH_CHANGED, ScannerObjectInfo as ObjectInfo, ScannerObjectOptions as ObjectOptions, StorageError,
    TRANSITION_COMPLETE, save_config, save_config_with_preconditions, scanner_publication_admission_for_epoch, storageclass,
};
use crate::{ScannerConfigObjectDelete, ScannerObjectIO};

// Data usage constants
pub const DATA_USAGE_ROOT: &str = SLASH_SEPARATOR;

const DATA_USAGE_BLOOM_NAME: &str = ".bloomcycle.bin";

pub const DATA_USAGE_CACHE_NAME: &str = ".usage-cache.bin";
pub(crate) const DATA_USAGE_CACHE_KEY_FORMAT: u16 = 1;

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
        Err(
            Error::ConfigNotFound
            | Error::FileNotFound
            | Error::VolumeNotFound
            | Error::ObjectNotFound(_, _)
            | Error::BucketNotFound(_),
        ) => Ok((None, DataUsageCacheRevision::Missing)),
        Err(err) => Err(err),
    }
}

/// Read only the object revision without materializing its body.
pub(crate) async fn read_config_revision<S: ScannerObjectIO>(store: Arc<S>, path: &str) -> StorageResult<DataUsageCacheRevision> {
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
            .ok_or_else(|| StorageError::other(format!("scanner config object {path} has no ETag"))),
        Err(
            Error::ConfigNotFound
            | Error::FileNotFound
            | Error::VolumeNotFound
            | Error::ObjectNotFound(_, _)
            | Error::BucketNotFound(_),
        ) => Ok(DataUsageCacheRevision::Missing),
        Err(err) => Err(err),
    }
}

#[derive(Clone, Debug)]
pub(crate) struct DataUsageCacheRevisions {
    main: DataUsageCacheRevision,
    backup: Option<DataUsageCacheRevision>,
}

pub static DATA_USAGE_BUCKET: LazyLock<String> =
    LazyLock::new(|| format!("{RUSTFS_META_BUCKET}{SLASH_SEPARATOR}{BUCKET_META_PREFIX}"));

pub static DATA_USAGE_OBJ_NAME_PATH: LazyLock<String> =
    LazyLock::new(|| format!("{BUCKET_META_PREFIX}{SLASH_SEPARATOR}{DATA_USAGE_OBJECT_NAME}"));

pub static DATA_USAGE_OBSERVED_OBJ_NAME_PATH: LazyLock<String> =
    LazyLock::new(|| format!("{BUCKET_META_PREFIX}{SLASH_SEPARATOR}{DATA_USAGE_OBSERVED_OBJECT_NAME}"));

pub static LEGACY_DATA_USAGE_OBJ_NAME_PATH: LazyLock<String> =
    LazyLock::new(|| format!("{BUCKET_META_PREFIX}{SLASH_SEPARATOR}{LEGACY_DATA_USAGE_OBJECT_NAME}"));

pub static DATA_USAGE_BLOOM_NAME_PATH: LazyLock<String> =
    LazyLock::new(|| format!("{BUCKET_META_PREFIX}{SLASH_SEPARATOR}{DATA_USAGE_BLOOM_NAME}"));

/// Durable companion object for a cycle-state object which cannot be decoded.
/// The primary object is deliberately never replaced or deleted by recovery.
pub static DATA_USAGE_BLOOM_RECOVERY_PATH: LazyLock<String> =
    LazyLock::new(|| format!("{}.recovery-required.json", DATA_USAGE_BLOOM_NAME_PATH.as_str()));

pub static BACKGROUND_HEAL_INFO_PATH: LazyLock<String> =
    LazyLock::new(|| format!("{BUCKET_META_PREFIX}{SLASH_SEPARATOR}.background-heal.json"));

const MAX_DATA_USAGE_CACHE_DEPTH: usize = 1024;

/// Scanner-side accounting on the shared [`SizeSummary`].
///
/// The type itself lives in `rustfs-data-usage`, which sits below the storage
/// layer and cannot see `ObjectInfo`, so this stays an extension trait rather
/// than an inherent method (backlog#1828).
pub trait ScannerSizeSummaryExt {
    /// Fold one object's contribution into the summary, including its tier.
    fn actions_accounting(&mut self, oi: &ObjectInfo, size: i64, actual_size: i64);
    /// Fold counters and physical tier usage for an object whose metadata is
    /// valid but whose logical size is currently unavailable. Logical totals
    /// stay unchanged.
    fn actions_accounting_unknown(&mut self, oi: &ObjectInfo);
}

impl ScannerSizeSummaryExt for SizeSummary {
    fn actions_accounting(&mut self, oi: &ObjectInfo, size: i64, actual_size: i64) {
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
            *tier_stats = tier_stats.add(&TierStats {
                total_size: u64::try_from(oi.size).unwrap_or(0),
                num_versions: 1,
                num_objects: u64::from(oi.is_latest),
            });
        }
    }

    fn actions_accounting_unknown(&mut self, oi: &ObjectInfo) {
        if oi.delete_marker {
            self.delete_markers = self.delete_markers.saturating_add(1);
            return;
        }

        if oi.version_id.is_some_and(|v| !v.is_nil()) {
            self.versions = self.versions.saturating_add(1);
        }

        if oi.transitioned_object.free_version {
            return;
        }

        let tier = if oi.transitioned_object.status == TRANSITION_COMPLETE {
            oi.transitioned_object.tier.clone()
        } else {
            oi.storage_class.clone().unwrap_or_else(|| storageclass::STANDARD.to_string())
        };
        if let Some(tier_stats) = self.tier_stats.get_mut(&tier) {
            *tier_stats = tier_stats.add(&TierStats {
                total_size: u64::try_from(oi.size).unwrap_or(0),
                num_versions: 1,
                num_objects: u64::from(oi.is_latest),
            });
        }
    }
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
    #[serde(default)]
    pub cache_key_format: u16,
    /// Bounded durable debts for versions whose logical size was not trusted.
    /// The map key is an identity key, never a user-controlled metric label.
    #[serde(default)]
    pub size_reconciliation: HashMap<String, SizeReconciliationEntry>,
    /// Whether the entries retained while a set scan was incomplete come
    /// from a prior complete set snapshot.  This is observational input only.
    #[serde(default)]
    pub lkg_snapshot_complete: bool,
    #[serde(default)]
    pub lkg_next_cycle: Option<u64>,
    #[serde(default)]
    pub lkg_last_update: Option<SystemTime>,
    #[serde(default)]
    pub lkg_leader_epoch: Option<u64>,
    #[serde(default)]
    pub lkg_scan_plan_digest: Option<DataUsageScanPlanDigest>,
}

impl Serialize for DataUsageCacheInfo {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        // Keep this metadata map-encoded so older readers can ignore fields
        // appended by newer scanner versions during rolling upgrades.
        let field_count = 21 + usize::from(!self.size_reconciliation.is_empty());
        let mut state = serializer.serialize_map(Some(field_count))?;
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
        state.serialize_entry("cache_key_format", &self.cache_key_format)?;
        if !self.size_reconciliation.is_empty() {
            state.serialize_entry("size_reconciliation", &self.size_reconciliation)?;
        }
        state.serialize_entry("lkg_snapshot_complete", &self.lkg_snapshot_complete)?;
        state.serialize_entry("lkg_next_cycle", &self.lkg_next_cycle)?;
        state.serialize_entry("lkg_last_update", &self.lkg_last_update)?;
        state.serialize_entry("lkg_leader_epoch", &self.lkg_leader_epoch)?;
        state.serialize_entry("lkg_scan_plan_digest", &self.lkg_scan_plan_digest)?;
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
    /// Prefix-level usage query over this (writer-side) cache; see
    /// [`prefix_usage_in_cache`] for the semantics
    /// (rustfs/backlog#1872).
    pub fn prefix_usage(&self, bucket: &str, prefix: &str, max_entries: usize) -> Option<PrefixUsageQuery> {
        prefix_usage_in_cache(&self.cache, bucket, prefix, max_entries)
    }

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
        let metadata_is_reusable = self.info.name == name
            && self.info.leader_epoch == leader_epoch
            && plan_matches
            && (source_matches || (!require_source && self.info.source.is_none()))
            && self.info.cache_key_format == DATA_USAGE_CACHE_KEY_FORMAT;
        let reusable = metadata_is_reusable
            && (self.cache.is_empty()
                || if name == DATA_USAGE_ROOT || self.info.snapshot_complete {
                    self.checked_flatten_complete_scope(name).is_some()
                } else {
                    self.checked_flatten(name).is_some()
                });
        if !reusable {
            let (pending_heals, size_reconciliation) = if self.info.name == name {
                (
                    std::mem::take(&mut self.info.pending_heals),
                    std::mem::take(&mut self.info.size_reconciliation),
                )
            } else {
                (Vec::new(), HashMap::new())
            };
            *self = Self::default();
            self.info.name = name.to_string();
            self.info.pending_heals = pending_heals;
            self.info.size_reconciliation = size_reconciliation;
        }

        self.info.next_cycle = next_cycle;
        self.info.leader_epoch = leader_epoch;
        self.info.source = Some(source);
        self.info.scan_plan_digest = Some(scan_plan_digest);
        self.info.cache_key_format = DATA_USAGE_CACHE_KEY_FORMAT;
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
        self.checked_flatten_inner(path).map(|(entry, _)| entry)
    }

    pub(crate) fn checked_flatten_complete(&self, path: &str) -> Option<DataUsageEntry> {
        self.checked_flatten_inner(path)
            .filter(|(_, visited)| *visited == self.cache.len())
            .map(|(entry, _)| entry)
    }

    pub(crate) fn checked_flatten_complete_scope(&self, path: &str) -> Option<DataUsageEntry> {
        if path == DATA_USAGE_ROOT {
            return self.checked_flatten_complete(path);
        }
        let (entry, visited) = self.checked_flatten_inner(path)?;
        let root_parent_only = {
            let path_key = hash_path(path).key();
            self.cache
                .get(DATA_USAGE_ROOT)
                .is_some_and(|root| root_is_parent_only(root, &path_key))
        };
        let expected_entries = self.cache.len().saturating_sub(usize::from(root_parent_only));
        (visited == expected_entries).then_some(entry)
    }

    fn checked_flatten_inner(&self, path: &str) -> Option<(DataUsageEntry, usize)> {
        let root_key = hash_path(path).key();
        let (root_key, root) = self.cache.get_key_value(&root_key)?;
        if root.compacted && !root.children.is_empty() {
            return None;
        }
        let mut visited = HashSet::from([root_key.as_str()]);
        let mut pending = root.children.iter().map(|child| (child.as_str(), 1usize)).collect::<Vec<_>>();
        let mut flattened = DataUsageEntry::default();
        if !flattened.checked_merge(root) {
            return None;
        }
        flattened.compacted = root.compacted;

        while let Some((key, depth)) = pending.pop() {
            if depth > MAX_DATA_USAGE_CACHE_DEPTH || !visited.insert(key) {
                return None;
            }
            let entry = self.cache.get(key)?;
            if (entry.compacted || depth == MAX_DATA_USAGE_CACHE_DEPTH) && !entry.children.is_empty() {
                return None;
            }
            pending.extend(entry.children.iter().map(|child| (child.as_str(), depth + 1)));

            if !flattened.checked_merge(entry) {
                return None;
            }
        }

        Some((flattened, visited.len()))
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
                if flat.replication_stats.as_ref().is_some_and(|stats| stats.is_empty()) {
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
            tier_stats: flat.all_tier_stats.filter(|tiers| !tiers.is_empty()),
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
}

mod persistence;

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

fn root_is_parent_only(root: &DataUsageEntry, child: &str) -> bool {
    root.children.len() == 1
        && root.children.contains(child)
        && root.size == 0
        && root.objects == 0
        && root.versions == 0
        && root.delete_markers == 0
        && root.replication_stats.is_none()
        && !root.compacted
        && root.failed_objects == 0
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

#[cfg(test)]
mod tests;
