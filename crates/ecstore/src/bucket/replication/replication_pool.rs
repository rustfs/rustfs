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

use super::replication_config_store::ReplicationConfigStore;
use super::replication_error_boundary::{Error as EcstoreError, is_err_object_not_found, is_err_version_not_found};
use super::replication_filemeta_boundary::{
    MrfOpKind, MrfReplicateEntry, REPLICATE_HEAL_DELETE, ReplicateDecision, ReplicateObjectInfo, ReplicatedTargetInfo,
    ReplicationStatusType, ReplicationType, ReplicationWorkerOperation, ResyncDecision, replicate_decision_for_admitted_targets,
    replication_statuses_map, version_purge_statuses_map,
};
use super::replication_lock_boundary::ReplicationLockTiming;
use super::replication_logging::{EVENT_REPLICATION_CONFIG_LOOKUP_SKIPPED, LOG_COMPONENT_ECSTORE, LOG_SUBSYSTEM_REPLICATION};
use super::replication_metadata_boundary::ReplicationMetadataStore;
use super::replication_object_config::{ReplicationConfig, check_replicate_delete_strict_with_availability, must_replicate};
use super::replication_object_decision_boundary::MustReplicateOptions;
use super::replication_queue_boundary::{
    DeletedObjectReplicationInfo, LARGE_WORKER_COUNT, ReplicationBackpressureRecommendation, ReplicationBackpressureState,
    ReplicationBatchAdmission, ReplicationHealQueueAction, ReplicationHealQueueResult, ReplicationHealResyncDeletes,
    ReplicationOperation, ReplicationPoolOpts, ReplicationPriority, ReplicationQueueAdmission, ReplicationWorkerQueue,
    WORKER_MAX_LIMIT, initial_worker_counts, large_worker_backpressure_resize, mrf_worker_size_to_count,
    replication_backpressure_recommendation, replication_heal_queue_action, resized_worker_counts, should_queue_large_object,
    worker_queue_for_replication_type,
};
use super::replication_resync_boundary::ResyncStatusType;
use super::replication_resync_boundary::{
    BucketReplicationResyncStatus, ResyncOpts, TargetReplicationResyncStatus, decode_mrf_file, decode_resync_file,
    encode_mrf_file, should_auto_resume_resync,
};
use super::replication_resyncer::{
    ReplicationResyncer, get_heal_replicate_object_info, replicate_delete, replicate_object, save_resync_status,
};
use super::replication_state::{ReplicationStats, normalized_target_arns};
use super::replication_storage_boundary::{
    ObjectInfo, ObjectOptions, ObjectToDelete, ReplicationDeletedObject, ReplicationObjectIO, ReplicationStorage,
};
use super::replication_target_boundary::{ReplicationTargetStore, replication_object_is_ssec_encrypted};
use super::replication_versioning_boundary::ReplicationVersioningStore;
use super::runtime_boundary as runtime_sources;
use futures_util::stream::{self, StreamExt};
use metrics::{counter, histogram};
use rustfs_utils::http::{SUFFIX_REPLICATION_TIMESTAMP, get_str};
use sha2::{Digest, Sha256};
use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::sync::LazyLock;
use std::sync::Mutex as StdMutex;
use std::sync::RwLock as StdRwLock;
use std::sync::atomic::Ordering;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU64};
use std::time::Instant;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use tokio::sync::Mutex;
use tokio::sync::RwLock;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::Sender;
use tokio::sync::oneshot;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinHandle;
use tokio::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, instrument, warn};

const EVENT_REPLICATION_WORKER_RESIZE_SKIPPED: &str = "replication_worker_resize_skipped";
const EVENT_REPLICATION_WORKER_RESIZED: &str = "replication_worker_resized";
const EVENT_REPLICATION_BACKPRESSURE: &str = "replication_backpressure";
const EVENT_REPLICATION_RESYNC_LOAD_SKIPPED: &str = "replication_resync_load_skipped";
const EVENT_REPLICATION_RESYNC_RECOVERED: &str = "replication_resync_recovered";
const EVENT_REPLICATION_MRF_QUEUE_UNAVAILABLE: &str = "replication_mrf_queue_unavailable";
const DELETE_BATCH_ADMISSION_CONCURRENCY: usize = 16;
const METRIC_DELETE_BATCH_ITEMS_TOTAL: &str = "rustfs_replication_delete_batch_items_total";
const METRIC_DELETE_BATCH_SIZE: &str = "rustfs_replication_delete_batch_size";
const MRF_PENDING_CAP: usize = 200_000;
const MRF_REPLAY_BATCH_SIZE: usize = 256;
const MRF_REPLAY_CONCURRENCY: usize = 10;
const MRF_ACK_SAVE_ATTEMPTS: usize = 4;
const MRF_SAVE_CHANNEL_CAP: usize = 1_024;
const MRF_SAVE_REQUEST_CAP: usize = 1_000;
const MAX_MRF_TARGET_FIELD_LEN: usize = 1_024;
const MRF_ADMISSION_BATCH_DELAY: Duration = Duration::from_millis(10);

tokio::task_local! {
    static MRF_REPLAY_RETRIES: Arc<StdMutex<Vec<MrfReplicateEntry>>>;
}

#[derive(Debug)]
struct MrfSaveRequest {
    file: &'static str,
    entries: Vec<MrfReplicateEntry>,
    persisted: oneshot::Sender<bool>,
    permit: OwnedSemaphorePermit,
}

#[derive(Default)]
struct PendingMrfFile {
    entries: Vec<MrfReplicateEntry>,
    persisted: Vec<oneshot::Sender<bool>>,
    permits: Vec<OwnedSemaphorePermit>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct MrfMergeOutcome {
    duration_millis: u64,
    additions_persisted: bool,
}

#[derive(Debug, Default)]
pub struct DurableMrfBacklog {
    pub available: bool,
    pub entries: Vec<MrfReplicateEntry>,
}

#[derive(Debug, Clone)]
struct OrderedMrfEntry {
    file: &'static str,
    file_index: usize,
    entry: MrfReplicateEntry,
}

fn ordered_mrf_entries(legacy: Vec<MrfReplicateEntry>, targeted: Vec<MrfReplicateEntry>) -> Vec<OrderedMrfEntry> {
    let mut entries = legacy
        .into_iter()
        .enumerate()
        .map(|(file_index, entry)| OrderedMrfEntry {
            file: ReplicationMetadataStore::MRF_REPLICATION_FILE,
            file_index,
            entry,
        })
        .chain(targeted.into_iter().enumerate().map(|(file_index, entry)| OrderedMrfEntry {
            file: ReplicationMetadataStore::TARGETED_MRF_REPLICATION_FILE,
            file_index,
            entry,
        }))
        .collect::<Vec<_>>();
    entries.sort_by(|left, right| {
        let left_legacy = left.entry.source_mod_time.is_none() && left.entry.enqueued_order.is_none();
        let right_legacy = right.entry.source_mod_time.is_none() && right.entry.enqueued_order.is_none();
        let file_rank = |file| usize::from(file == ReplicationMetadataStore::TARGETED_MRF_REPLICATION_FILE);
        left.entry
            .bucket
            .cmp(&right.entry.bucket)
            .then_with(|| left.entry.object.cmp(&right.entry.object))
            .then_with(|| left_legacy.cmp(&right_legacy).reverse())
            .then_with(|| {
                if left_legacy && right_legacy {
                    return file_rank(left.file)
                        .cmp(&file_rank(right.file))
                        .then_with(|| left.file_index.cmp(&right.file_index));
                }
                let source_order = |entry: &MrfReplicateEntry| {
                    entry
                        .source_mod_time
                        .map(i128::from)
                        .or_else(|| entry.enqueued_order.map(i128::from))
                        .unwrap_or(i128::MAX)
                };
                source_order(&left.entry)
                    .cmp(&source_order(&right.entry))
                    .then_with(|| {
                        left.entry
                            .enqueued_order
                            .unwrap_or(u64::MAX)
                            .cmp(&right.entry.enqueued_order.unwrap_or(u64::MAX))
                    })
                    .then_with(|| file_rank(left.file).cmp(&file_rank(right.file)))
                    .then_with(|| left.file_index.cmp(&right.file_index))
            })
    });
    entries
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DurableMrfBucketBacklog {
    pub bucket: String,
    pub count: u64,
    pub bytes: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DurableMrfTargetBacklog {
    pub bucket: String,
    pub target_arn: String,
    pub count: u64,
    pub bytes: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DurableMrfBacklogSummary {
    pub available: bool,
    pub buckets: Vec<DurableMrfBucketBacklog>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct DurableMrfBacklogSnapshot {
    summary: DurableMrfBacklogSummary,
    targets: Vec<DurableMrfTargetBacklog>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MrfBucketBacklogObservability {
    pub bucket: String,
    pub pending_count: u64,
    pub pending_bytes: u64,
    pub dropped_count: u64,
    pub missed_count: u64,
    pub flush_failure_count: u64,
    pub last_flush_duration_millis: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MrfBacklogObservabilitySummary {
    pub buckets: Vec<MrfBucketBacklogObservability>,
}

static DURABLE_MRF_BACKLOG_SUMMARY: LazyLock<StdRwLock<DurableMrfBacklogSummary>> =
    LazyLock::new(|| StdRwLock::new(DurableMrfBacklogSummary::default()));
static DURABLE_MRF_TARGET_BACKLOG: LazyLock<StdRwLock<Vec<DurableMrfTargetBacklog>>> =
    LazyLock::new(|| StdRwLock::new(Vec::new()));
static MRF_BACKLOG_OBSERVABILITY: LazyLock<StdRwLock<MrfBacklogObservabilityTracker>> =
    LazyLock::new(|| StdRwLock::new(MrfBacklogObservabilityTracker::default()));

fn should_replay_force_delete_intent(entry: &MrfReplicateEntry) -> bool {
    entry.force_delete_id.is_some() && entry.force_delete_local_commit && !entry.target_arns.is_empty()
}

#[derive(Debug, Clone, Default)]
struct DurableMrfBacklogTracker {
    available: bool,
    buckets: HashMap<String, DurableMrfBucketBacklog>,
    targets: HashMap<(String, String), DurableMrfTargetBacklog>,
}

impl DurableMrfBacklogTracker {
    fn add_entry(&mut self, entry: &MrfReplicateEntry) {
        let Ok(size) = u64::try_from(entry.size) else {
            self.available = false;
            self.buckets.clear();
            self.targets.clear();
            return;
        };

        if !self.available {
            return;
        }

        let bucket_name = entry.bucket.clone();
        let bucket = match self.buckets.entry(bucket_name) {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => {
                let bucket = entry.key().clone();
                entry.insert(DurableMrfBucketBacklog {
                    bucket,
                    ..Default::default()
                })
            }
        };
        bucket.count = bucket.count.saturating_add(1);
        bucket.bytes = bucket.bytes.saturating_add(size);

        for target_arn in &entry.target_arns {
            if target_arn.is_empty() {
                continue;
            }
            let key = (entry.bucket.clone(), target_arn.clone());
            let target = match self.targets.entry(key) {
                Entry::Occupied(entry) => entry.into_mut(),
                Entry::Vacant(entry) => {
                    let (bucket, target_arn) = entry.key().clone();
                    entry.insert(DurableMrfTargetBacklog {
                        bucket,
                        target_arn,
                        ..Default::default()
                    })
                }
            };
            target.count = target.count.saturating_add(1);
            target.bytes = target.bytes.saturating_add(size);
        }
    }

    fn into_snapshot(self) -> DurableMrfBacklogSnapshot {
        if !self.available {
            return DurableMrfBacklogSnapshot::default();
        }

        DurableMrfBacklogSnapshot {
            summary: DurableMrfBacklogSummary {
                available: true,
                buckets: self.buckets.into_values().collect(),
            },
            targets: self.targets.into_values().collect(),
        }
    }
}

#[derive(Debug, Clone, Default)]
struct MrfBacklogObservabilityTracker {
    buckets: HashMap<String, MrfBucketBacklogObservability>,
}

impl MrfBacklogObservabilityTracker {
    fn bucket_mut(&mut self, bucket_name: &str) -> &mut MrfBucketBacklogObservability {
        match self.buckets.entry(bucket_name.to_string()) {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => entry.insert(MrfBucketBacklogObservability {
                bucket: bucket_name.to_string(),
                ..Default::default()
            }),
        }
    }

    fn add_pending(&mut self, entry: &MrfReplicateEntry) {
        let Ok(size) = u64::try_from(entry.size) else {
            return;
        };
        let bucket = self.bucket_mut(&entry.bucket);
        bucket.pending_count = bucket.pending_count.saturating_add(1);
        bucket.pending_bytes = bucket.pending_bytes.saturating_add(size);
    }

    fn flush_pending_entries<'a>(&mut self, entries: impl IntoIterator<Item = &'a MrfReplicateEntry>, duration_millis: u64) {
        for entry in entries {
            let Ok(size) = u64::try_from(entry.size) else {
                continue;
            };
            let bucket = self.bucket_mut(&entry.bucket);
            bucket.pending_count = bucket.pending_count.saturating_sub(1);
            bucket.pending_bytes = bucket.pending_bytes.saturating_sub(size);
            bucket.last_flush_duration_millis = duration_millis;
        }
    }

    fn record_drop(&mut self, entry: &MrfReplicateEntry) {
        let bucket = self.bucket_mut(&entry.bucket);
        bucket.dropped_count = bucket.dropped_count.saturating_add(1);
    }

    fn record_missed(&mut self, bucket_name: &str) {
        let bucket = self.bucket_mut(bucket_name);
        bucket.missed_count = bucket.missed_count.saturating_add(1);
    }

    fn record_flush_failure(&mut self, duration_millis: u64) {
        for bucket in self.buckets.values_mut().filter(|bucket| bucket.pending_count > 0) {
            bucket.flush_failure_count = bucket.flush_failure_count.saturating_add(1);
            bucket.last_flush_duration_millis = duration_millis;
        }
    }

    fn snapshot(&self) -> MrfBacklogObservabilitySummary {
        MrfBacklogObservabilitySummary {
            buckets: self.buckets.values().cloned().collect(),
        }
    }
}

fn durable_mrf_backlog_summary_from_entries<'a>(
    entries: impl IntoIterator<Item = &'a MrfReplicateEntry>,
) -> DurableMrfBacklogSnapshot {
    let mut tracker = DurableMrfBacklogTracker {
        available: true,
        ..Default::default()
    };
    for entry in entries {
        tracker.add_entry(entry);
    }
    tracker.into_snapshot()
}

#[cfg(test)]
fn durable_mrf_backlog_summary_from_sizes<I>(entries: I) -> DurableMrfBacklogSnapshot
where
    I: IntoIterator<Item = (String, i64)>,
{
    let mut tracker = DurableMrfBacklogTracker {
        available: true,
        ..Default::default()
    };
    for (bucket_name, entry_size) in entries {
        tracker.add_entry(&MrfReplicateEntry {
            bucket: bucket_name,
            object: String::new(),
            version_id: None,
            retry_count: 0,
            size: entry_size,
            op: MrfOpKind::Object,
            force_delete: false,
            delete_marker_version_id: None,
            delete_marker: false,
            delete_marker_mtime: None,
            target_arns: Vec::new(),
            ..Default::default()
        });
    }
    tracker.into_snapshot()
}

fn set_durable_mrf_backlog_snapshot(snapshot: DurableMrfBacklogSnapshot) {
    match DURABLE_MRF_BACKLOG_SUMMARY.write() {
        Ok(mut guard) => *guard = snapshot.summary,
        Err(poisoned) => *poisoned.into_inner() = snapshot.summary,
    }
    match DURABLE_MRF_TARGET_BACKLOG.write() {
        Ok(mut guard) => *guard = snapshot.targets,
        Err(poisoned) => *poisoned.into_inner() = snapshot.targets,
    }
}

pub fn durable_mrf_backlog_summary_snapshot() -> DurableMrfBacklogSummary {
    match DURABLE_MRF_BACKLOG_SUMMARY.read() {
        Ok(guard) => guard.clone(),
        Err(poisoned) => poisoned.into_inner().clone(),
    }
}

pub fn durable_mrf_target_backlog_snapshot() -> Vec<DurableMrfTargetBacklog> {
    match DURABLE_MRF_TARGET_BACKLOG.read() {
        Ok(guard) => guard.clone(),
        Err(poisoned) => poisoned.into_inner().clone(),
    }
}

pub fn mrf_backlog_observability_snapshot() -> MrfBacklogObservabilitySummary {
    match MRF_BACKLOG_OBSERVABILITY.read() {
        Ok(guard) => guard.snapshot(),
        Err(poisoned) => poisoned.into_inner().snapshot(),
    }
}

fn update_mrf_backlog_observability(mut update: impl FnMut(&mut MrfBacklogObservabilityTracker)) {
    match MRF_BACKLOG_OBSERVABILITY.write() {
        Ok(mut guard) => update(&mut guard),
        Err(poisoned) => update(&mut poisoned.into_inner()),
    }
}

fn observe_mrf_pending(entry: &MrfReplicateEntry) {
    update_mrf_backlog_observability(|tracker| tracker.add_pending(entry));
}

fn observe_mrf_pending_flushed(entries: &[MrfReplicateEntry], duration_millis: u64) {
    update_mrf_backlog_observability(|tracker| tracker.flush_pending_entries(entries, duration_millis));
}

fn observe_mrf_drop(entry: &MrfReplicateEntry) {
    update_mrf_backlog_observability(|tracker| tracker.record_drop(entry));
}

fn observe_mrf_missed(bucket: &str) {
    update_mrf_backlog_observability(|tracker| tracker.record_missed(bucket));
}

fn observe_mrf_flush_failure(duration_millis: u64) {
    update_mrf_backlog_observability(|tracker| tracker.record_flush_failure(duration_millis));
}

fn durable_mrf_backlog_from_read(result: Result<Vec<u8>, EcstoreError>) -> DurableMrfBacklog {
    match result {
        Ok(data) => match decode_mrf_file(&data) {
            Ok(entries) if entries.iter().all(|entry| entry.size >= 0) => DurableMrfBacklog {
                available: true,
                entries,
            },
            Ok(_) | Err(_) => DurableMrfBacklog::default(),
        },
        Err(EcstoreError::ConfigNotFound) => DurableMrfBacklog {
            available: true,
            entries: Vec::new(),
        },
        Err(_) => DurableMrfBacklog::default(),
    }
}

pub async fn read_durable_mrf_backlog<S: ReplicationObjectIO>(storage: Arc<S>) -> DurableMrfBacklog {
    let mut backlog = durable_mrf_backlog_from_read(
        ReplicationConfigStore::read_preserve_empty(storage.clone(), ReplicationMetadataStore::MRF_REPLICATION_FILE).await,
    );
    let mut targeted = durable_mrf_backlog_from_read(
        ReplicationConfigStore::read_preserve_empty(storage, ReplicationMetadataStore::TARGETED_MRF_REPLICATION_FILE).await,
    );
    if targeted.available && !targeted.entries.iter().all(targeted_mrf_entry_is_valid) {
        targeted = DurableMrfBacklog::default();
    }
    backlog.available &= targeted.available;
    backlog.entries = ordered_mrf_entries(backlog.entries, targeted.entries)
        .into_iter()
        .map(|entry| entry.entry)
        .collect();
    backlog
}

fn mrf_file_for_entry(entry: &MrfReplicateEntry) -> &'static str {
    if entry.op == MrfOpKind::Delete && entry.target_arns.iter().any(|arn| !arn.is_empty()) {
        ReplicationMetadataStore::TARGETED_MRF_REPLICATION_FILE
    } else {
        ReplicationMetadataStore::MRF_REPLICATION_FILE
    }
}

/// An MRF source lookup that reports the object or version as gone is terminal:
/// the source no longer exists, so replaying it would achieve nothing. Anything
/// else is transient and must be retried, or a flaky disk silently drops the
/// backlog entry. Named helper (from #5659) so the classification is pinned by
/// its own tests rather than being an inline guard.
fn should_retry_mrf_source_lookup(error: &EcstoreError) -> bool {
    !is_err_object_not_found(error) && !is_err_version_not_found(error)
}

fn targeted_mrf_entry_is_valid(entry: &MrfReplicateEntry) -> bool {
    let targets = normalized_target_arns(&entry.target_arns);
    entry.op == MrfOpKind::Delete
        && targets.len() == 1
        && targets[0].len() <= MAX_MRF_TARGET_FIELD_LEN
        && (!entry.delete_marker || entry.delete_marker_version_id.is_some())
        && (entry.delete_marker || entry.version_id.is_some() || entry.delete_marker_version_id.is_some())
        && entry
            .target_delete_marker_version_id
            .as_ref()
            .is_none_or(|version_id| !version_id.is_empty() && version_id.len() <= MAX_MRF_TARGET_FIELD_LEN)
}

fn encoded_mrf_entry(entry: &MrfReplicateEntry) -> Result<Vec<u8>, EcstoreError> {
    encode_mrf_file(std::slice::from_ref(entry)).map_err(|error| EcstoreError::other(error.to_string()))
}

fn encoded_mrf_entry_identity(entry: &MrfReplicateEntry) -> Result<Vec<u8>, EcstoreError> {
    let mut identity = entry.clone();
    identity.enqueued_order = None;
    encoded_mrf_entry(&identity)
}

fn mrf_entry_has_stable_identity(entry: &MrfReplicateEntry) -> bool {
    entry.version_id.is_some() || entry.delete_marker_version_id.is_some()
}

fn recovered_mrf_delete_infos(
    entry: &MrfReplicateEntry,
    delete_object: ReplicationDeletedObject,
) -> Vec<DeletedObjectReplicationInfo> {
    let target_arns = normalized_target_arns(&entry.target_arns);
    let base = DeletedObjectReplicationInfo {
        delete_object,
        bucket: entry.bucket.clone(),
        event_type: REPLICATE_HEAL_DELETE.to_string(),
        op_type: ReplicationType::Heal,
        blocked_delete_marker_version_state: entry.blocked_delete_marker_version_state(),
        target_delete_marker_version_id: (target_arns.len() == 1)
            .then(|| entry.target_delete_marker_version_id.clone())
            .flatten(),
        ..Default::default()
    };
    let Some((last, preceding)) = target_arns.split_last() else {
        return vec![base];
    };

    preceding
        .iter()
        .map(|target_arn| DeletedObjectReplicationInfo {
            target_arn: (**target_arn).to_string(),
            ..base.clone()
        })
        .chain(std::iter::once(DeletedObjectReplicationInfo {
            target_arn: (*last).to_string(),
            ..base.clone()
        }))
        .collect()
}

enum RecoveredMrfOperations {
    TerminalSkip,
    Retry,
    Work(Vec<ReplicationOperation>),
}

enum MrfReplayResult {
    Retain,
    Acknowledge(Vec<MrfReplicateEntry>),
}

struct MrfReplayAcknowledgement {
    original: Vec<u8>,
    replacements: Vec<MrfReplicateEntry>,
}

struct MrfFileContents {
    entries: Vec<MrfReplicateEntry>,
    expected_etag: Option<String>,
}

type PendingMrfAcknowledgement = (Vec<u8>, Vec<MrfReplicateEntry>, Vec<MrfReplicateEntry>);

fn mrf_claim_key(file: &str, encoded: &[u8]) -> String {
    let digest = hex_simd::encode_to_string(Sha256::digest(encoded), hex_simd::AsciiCase::Lower);
    format!("{file}.claims/{digest}")
}

async fn recovered_mrf_operations<S: ReplicationStorage>(entry: &MrfReplicateEntry, storage: &Arc<S>) -> RecoveredMrfOperations {
    match entry.op {
        MrfOpKind::Delete => {
            // Force-delete intents are owned by `start_force_delete_processor`,
            // which replays them from FORCE_DELETE_REPLICATION_FILE (#5641).
            // Replaying them here as ordinary deletes would double-process the
            // intent and lose its target set, so skip them terminally.
            if entry.force_delete_id.is_some() {
                return RecoveredMrfOperations::TerminalSkip;
            }
            let (versioned, version_suspended) =
                match ReplicationVersioningStore::prefix_state(&entry.bucket, &entry.object).await {
                    Ok(state) => state,
                    Err(_) => return RecoveredMrfOperations::Retry,
                };
            let oi = ObjectInfo {
                bucket: entry.bucket.clone(),
                name: entry.object.clone(),
                version_id: entry.version_id,
                delete_marker: entry.delete_marker,
                ..Default::default()
            };
            let (dsc, missing_required_client) = match check_replicate_delete_strict_with_availability(
                &entry.bucket,
                &ObjectToDelete {
                    object_name: entry.object.clone(),
                    version_id: entry.version_id,
                    ..Default::default()
                },
                &oi,
                &ObjectOptions {
                    versioned,
                    version_suspended,
                    ..Default::default()
                },
                None,
            )
            .await
            {
                Ok(dsc) => dsc,
                Err(_) => return RecoveredMrfOperations::Retry,
            };
            let stored_targets = normalized_target_arns(&entry.target_arns);
            if stored_targets.is_empty() && missing_required_client {
                return RecoveredMrfOperations::Retry;
            }
            if !stored_targets.is_empty()
                && stored_targets
                    .iter()
                    .any(|target| !dsc.targets_map.get(*target).is_some_and(|decision| decision.replicate))
            {
                return RecoveredMrfOperations::Retry;
            }
            if dsc.targets_map.is_empty() {
                return RecoveredMrfOperations::TerminalSkip;
            }
            let mut rstate = oi.replication_state();
            rstate.replicate_decision_str = dsc.to_string();
            let delete_marker_mtime = entry
                .delete_marker_mtime
                .and_then(|nanos| OffsetDateTime::from_unix_timestamp_nanos(nanos as i128).ok());
            let delete_object = ReplicationDeletedObject {
                object_name: entry.object.clone(),
                version_id: entry.version_id,
                delete_marker_version_id: entry.delete_marker_version_id,
                delete_marker: entry.delete_marker,
                delete_marker_mtime,
                replication_state: Some(rstate),
                ..Default::default()
            };

            RecoveredMrfOperations::Work(
                recovered_mrf_delete_infos(entry, delete_object)
                    .into_iter()
                    .map(|delete| ReplicationOperation::Delete(Box::new(delete)))
                    .collect(),
            )
        }
        MrfOpKind::Object | MrfOpKind::Metadata | MrfOpKind::Heal | MrfOpKind::ExistingObject => {
            let opts = ObjectOptions {
                version_id: entry.version_id.map(|version_id| version_id.to_string()),
                ..Default::default()
            };
            let oi = match storage.get_object_info(&entry.bucket, &entry.object, &opts).await {
                Ok(oi) => oi,
                Err(error) if !should_retry_mrf_source_lookup(&error) => {
                    return RecoveredMrfOperations::TerminalSkip;
                }
                Err(_) => return RecoveredMrfOperations::Retry,
            };
            if entry.op == MrfOpKind::Metadata {
                let mut dsc = must_replicate(
                    &entry.bucket,
                    &entry.object,
                    MustReplicateOptions::new(&oi.user_defined, (*oi.user_tags).clone(), ReplicationType::Metadata, false)
                        .with_replication_status(oi.replication_status.clone()),
                )
                .await;
                let stored_targets = normalized_target_arns(&entry.target_arns);
                if !stored_targets.is_empty() {
                    if stored_targets
                        .iter()
                        .any(|target| !dsc.targets_map.get(*target).is_some_and(|decision| decision.replicate))
                    {
                        return RecoveredMrfOperations::Retry;
                    }
                    dsc.targets_map.retain(|target, _| stored_targets.contains(&target.as_str()));
                }
                if !dsc.replicate_any() {
                    return RecoveredMrfOperations::TerminalSkip;
                }
                let mut object = replicate_object_info_from_object_info(oi, dsc, ReplicationType::Metadata);
                object.retry_count = u32::try_from(entry.retry_count).unwrap_or_default();
                return RecoveredMrfOperations::Work(vec![ReplicationOperation::Object(Box::new(object))]);
            }

            let stored_targets = normalized_target_arns(&entry.target_arns);
            if !stored_targets.is_empty() {
                let dsc = replicate_decision_for_admitted_targets(&entry.target_arns);
                let mut object = replicate_object_info_from_object_info(oi, dsc, entry.op.replication_type());
                object.retry_count = u32::try_from(entry.retry_count).unwrap_or_default();
                return RecoveredMrfOperations::Work(vec![ReplicationOperation::Object(Box::new(object))]);
            }

            let (config, _) = match ReplicationMetadataStore::replication_config(&entry.bucket).await {
                Ok(config) => config,
                Err(EcstoreError::ConfigNotFound) => return RecoveredMrfOperations::TerminalSkip,
                Err(_) => return RecoveredMrfOperations::Retry,
            };
            let Ok(targets) = ReplicationTargetStore::list_bucket_targets(&entry.bucket).await else {
                return RecoveredMrfOperations::Retry;
            };
            let rcfg = ReplicationConfig::new(Some(config), Some(targets));
            let Ok(mut object) = get_heal_replicate_object_info(&oi, &rcfg).await else {
                return RecoveredMrfOperations::Retry;
            };
            object.retry_count = u32::try_from(entry.retry_count).unwrap_or_default();

            match replication_heal_queue_action(&mut object) {
                ReplicationHealQueueAction::Skip => RecoveredMrfOperations::TerminalSkip,
                ReplicationHealQueueAction::QueueObject => {
                    RecoveredMrfOperations::Work(vec![ReplicationOperation::Object(Box::new(object))])
                }
                ReplicationHealQueueAction::QueueDelete(delete) => {
                    RecoveredMrfOperations::Work(vec![ReplicationOperation::Delete(Box::new(delete))])
                }
                ReplicationHealQueueAction::QueueResyncDeletes(batch) => RecoveredMrfOperations::Work(
                    batch
                        .target_delete_infos()
                        .map(|delete| ReplicationOperation::Delete(Box::new(delete)))
                        .collect(),
                ),
            }
        }
    }
}

async fn process_recovered_mrf_entry<S: ReplicationStorage>(entry: &MrfReplicateEntry, storage: Arc<S>) -> MrfReplayResult {
    let operations = match recovered_mrf_operations(entry, &storage).await {
        RecoveredMrfOperations::TerminalSkip => return MrfReplayResult::Acknowledge(Vec::new()),
        RecoveredMrfOperations::Retry => return MrfReplayResult::Retain,
        RecoveredMrfOperations::Work(operations) => operations,
    };
    execute_recovered_mrf_operations(operations, storage).await
}

async fn execute_recovered_mrf_operations<S: ReplicationStorage>(
    operations: Vec<ReplicationOperation>,
    storage: Arc<S>,
) -> MrfReplayResult {
    let retries = Arc::new(StdMutex::new(Vec::new()));
    let acknowledged = MRF_REPLAY_RETRIES
        .scope(retries.clone(), async move {
            let mut acknowledged = true;
            for operation in operations {
                match operation {
                    ReplicationOperation::Object(object) => {
                        acknowledged &= replicate_object(*object, storage.clone()).await.1;
                    }
                    ReplicationOperation::Delete(delete) => {
                        acknowledged &= replicate_delete(*delete, storage.clone()).await;
                    }
                }
            }
            acknowledged
        })
        .await;
    if !acknowledged {
        return MrfReplayResult::Retain;
    }

    MrfReplayResult::Acknowledge(match retries.lock() {
        Ok(mut retries) => std::mem::take(&mut *retries),
        Err(poisoned) => std::mem::take(&mut *poisoned.into_inner()),
    })
}

async fn read_mrf_snapshot<S: ReplicationStorage>(file: &str, storage: &Arc<S>) -> Option<Vec<MrfReplicateEntry>> {
    let lock = storage
        .new_ns_lock(ReplicationMetadataStore::rustfs_meta_bucket(), file)
        .await
        .ok()?;
    let guard = lock.get_write_lock(ReplicationLockTiming::acquire_timeout()).await.ok()?;
    let contents = read_mrf_entries_no_lock(file, storage).await.ok()?;
    (!guard.is_lock_lost()).then_some(contents.entries)
}

async fn acknowledge_mrf_batch<S: ReplicationStorage>(
    file: &str,
    acknowledgements: Vec<MrfReplayAcknowledgement>,
    storage: &Arc<S>,
    replay_lock_lost_signal: Option<Arc<rustfs_lock::distributed_lock::LockLostSignal>>,
) -> Option<usize> {
    if acknowledgements.is_empty() {
        return Some(0);
    }
    let replacements = acknowledgements
        .into_iter()
        .map(|ack| (ack.original, ack.replacements))
        .collect::<HashMap<_, _>>();
    for _ in 0..MRF_ACK_SAVE_ATTEMPTS {
        if replay_lock_lost_signal.as_ref().is_some_and(|signal| signal.is_lost()) {
            return None;
        }
        let lock = storage
            .new_ns_lock(ReplicationMetadataStore::rustfs_meta_bucket(), file)
            .await
            .ok()?;
        let guard = lock.get_write_lock(ReplicationLockTiming::acquire_timeout()).await.ok()?;
        let contents = read_mrf_entries_no_lock(file, storage).await.ok()?;
        let entries = contents.entries;
        let mut pending_replacements = replacements.clone();
        let mut matched = 0usize;
        let mut remaining = Vec::with_capacity(entries.len());
        for entry in entries {
            let encoded = encoded_mrf_entry(&entry).ok()?;
            if let Some(retry_entries) = pending_replacements.remove(&encoded) {
                matched = matched.saturating_add(1);
                remaining.extend(retry_entries);
            } else {
                remaining.push(entry);
            }
        }
        if matched == 0 {
            return Some(0);
        }
        if remaining.len() > MRF_PENDING_CAP {
            return None;
        }
        let encoded = encode_mrf_file(&remaining).ok()?;
        if guard.is_lock_lost() || replay_lock_lost_signal.as_ref().is_some_and(|signal| signal.is_lost()) {
            return None;
        }
        drop(guard);
        if ReplicationConfigStore::save_conditional(
            storage.clone(),
            file,
            encoded,
            contents.expected_etag,
            replay_lock_lost_signal.iter().cloned().collect(),
        )
        .await
        .is_ok()
        {
            return (!replay_lock_lost_signal.as_ref().is_some_and(|signal| signal.is_lost())).then_some(matched);
        }
    }
    None
}

async fn commit_mrf_replay_batch<S: ReplicationStorage>(
    file: &str,
    pending: Vec<PendingMrfAcknowledgement>,
    storage: &Arc<S>,
    replay_lock_lost_signal: Option<Arc<rustfs_lock::distributed_lock::LockLostSignal>>,
) -> Option<usize> {
    let external = pending
        .iter()
        .flat_map(|(_, _, entries)| entries.iter().cloned())
        .collect::<Vec<_>>();
    let external_persisted = external.is_empty()
        || (external.iter().all(targeted_mrf_entry_is_valid)
            && merge_mrf_entries_to_disk(ReplicationMetadataStore::TARGETED_MRF_REPLICATION_FILE, &external, storage)
                .await
                .is_some_and(|outcome| outcome.additions_persisted));
    let acknowledgements = pending
        .into_iter()
        .filter(|(_, _, external)| external.is_empty() || external_persisted)
        .map(|(original, replacements, _)| MrfReplayAcknowledgement { original, replacements })
        .collect();
    acknowledge_mrf_batch(file, acknowledgements, storage, replay_lock_lost_signal).await
}

async fn process_mrf_backlog<S: ReplicationStorage>(storage: Arc<S>) -> usize {
    let replay_key = format!("{}.replay-all", ReplicationMetadataStore::MRF_REPLICATION_FILE);
    let Ok(replay_lock) = storage
        .new_ns_lock(ReplicationMetadataStore::rustfs_meta_bucket(), &replay_key)
        .await
    else {
        return 0;
    };
    let Ok(replay_guard) = replay_lock.get_write_lock(ReplicationLockTiming::acquire_timeout()).await else {
        return 0;
    };
    let Some(legacy) = read_mrf_snapshot(ReplicationMetadataStore::MRF_REPLICATION_FILE, &storage).await else {
        return 0;
    };
    let Some(targeted) = read_mrf_snapshot(ReplicationMetadataStore::TARGETED_MRF_REPLICATION_FILE, &storage).await else {
        return 0;
    };
    let snapshot = ordered_mrf_entries(legacy, targeted);
    let mut pending_acks = HashMap::<&'static str, Vec<PendingMrfAcknowledgement>>::new();
    let mut visited = HashSet::new();
    let mut blocked_keys = HashSet::new();
    let replay_concurrency = Arc::new(Semaphore::new(MRF_REPLAY_CONCURRENCY));

    for batch in snapshot.chunks(MRF_REPLAY_BATCH_SIZE) {
        if replay_guard.is_lock_lost() {
            return 0;
        }
        let candidates = batch
            .iter()
            .filter_map(|ordered| {
                encoded_mrf_entry(&ordered.entry)
                    .ok()
                    .map(|encoded| (ordered.file, ordered.entry.clone(), encoded))
            })
            .filter(|(file, _, encoded)| visited.insert((*file, encoded.clone())))
            .collect::<Vec<_>>();
        let mut key_groups = BTreeMap::<(String, String), Vec<(&'static str, MrfReplicateEntry, Vec<u8>)>>::new();
        for (file, entry, encoded) in candidates {
            key_groups
                .entry((entry.bucket.clone(), entry.object.clone()))
                .or_default()
                .push((file, entry, encoded));
        }

        for (key, entries) in key_groups {
            if blocked_keys.contains(&key) {
                continue;
            }
            if replay_guard.is_lock_lost() {
                return 0;
            }
            let storage = storage.clone();
            let replay_lock_lost_signal = replay_guard.lock_lost_signal();
            let replay_concurrency = replay_concurrency.clone();
            let result = tokio::spawn(async move {
                let _permit = replay_concurrency.acquire_owned().await.ok()?;
                let mut acknowledgements = Vec::new();
                let mut completed = true;
                for (file, entry, encoded) in entries {
                    let claim_key = mrf_claim_key(file, &encoded);
                    let claim_lock = storage
                        .new_ns_lock(ReplicationMetadataStore::rustfs_meta_bucket(), &claim_key)
                        .await
                        .ok()?;
                    let claim_guard = claim_lock
                        .get_write_lock(ReplicationLockTiming::acquire_timeout())
                        .await
                        .ok()?;
                    if claim_guard.is_lock_lost() || replay_lock_lost_signal.as_ref().is_some_and(|signal| signal.is_lost()) {
                        return None;
                    }
                    let MrfReplayResult::Acknowledge(retries) = process_recovered_mrf_entry(&entry, storage.clone()).await else {
                        completed = false;
                        break;
                    };
                    if claim_guard.is_lock_lost() || replay_lock_lost_signal.as_ref().is_some_and(|signal| signal.is_lost()) {
                        return None;
                    }
                    let mut local = Vec::new();
                    let mut external = Vec::new();
                    for mut retry in retries {
                        retry.source_mod_time = retry.source_mod_time.or(entry.source_mod_time);
                        retry.enqueued_order = retry.enqueued_order.or(entry.enqueued_order);
                        if mrf_file_for_entry(&retry) == file {
                            local.push(retry);
                        } else {
                            external.push(retry);
                        }
                    }
                    acknowledgements.push((file, encoded, local, external));
                }
                Some((acknowledgements, completed))
            })
            .await
            .ok()
            .flatten();
            match result {
                Some((acknowledgements, completed)) => {
                    for (file, original, replacements, external) in acknowledgements {
                        pending_acks.entry(file).or_default().push((original, replacements, external));
                    }
                    if !completed {
                        blocked_keys.insert(key);
                    }
                }
                None => {
                    blocked_keys.insert(key);
                }
            }
        }
    }

    if replay_guard.is_lock_lost() {
        return 0;
    }
    let mut recovered = 0usize;
    for file in [
        ReplicationMetadataStore::MRF_REPLICATION_FILE,
        ReplicationMetadataStore::TARGETED_MRF_REPLICATION_FILE,
    ] {
        recovered = recovered.saturating_add(
            commit_mrf_replay_batch(
                file,
                pending_acks.remove(file).unwrap_or_default(),
                &storage,
                replay_guard.lock_lost_signal(),
            )
            .await
            .unwrap_or_default(),
        );
    }
    recovered
}

#[cfg(test)]
async fn process_mrf_file<S: ReplicationStorage>(_file: &str, storage: Arc<S>) -> usize {
    process_mrf_backlog(storage).await
}

pub async fn persist_force_delete_intent<S: ReplicationStorage>(
    storage: Arc<S>,
    mut entry: MrfReplicateEntry,
) -> Result<(), EcstoreError> {
    entry.force_delete_local_commit = false;
    let file = ReplicationMetadataStore::FORCE_DELETE_REPLICATION_FILE;
    let lock = storage
        .new_ns_lock(ReplicationMetadataStore::rustfs_meta_bucket(), file)
        .await?;
    let _guard = lock.get_write_lock(ReplicationLockTiming::acquire_timeout()).await?;

    let mut entries = match ReplicationConfigStore::read_no_lock(storage.clone(), file).await {
        Ok(data) => decode_mrf_file(&data)?,
        Err(EcstoreError::ConfigNotFound) => Vec::new(),
        Err(err) => return Err(err),
    };

    if entries
        .iter()
        .any(|existing| existing.force_delete_id == entry.force_delete_id)
    {
        return Ok(());
    }

    entries.push(entry);
    let data = encode_mrf_file(&entries)?;
    ReplicationConfigStore::save_no_lock(storage, file, data).await
}

pub async fn commit_force_delete_intent<S: ReplicationStorage>(
    storage: Arc<S>,
    operation_id: uuid::Uuid,
) -> Result<(), EcstoreError> {
    let file = ReplicationMetadataStore::FORCE_DELETE_REPLICATION_FILE;
    let lock = storage
        .new_ns_lock(ReplicationMetadataStore::rustfs_meta_bucket(), file)
        .await?;
    let _guard = lock.get_write_lock(ReplicationLockTiming::acquire_timeout()).await?;

    let data = ReplicationConfigStore::read_no_lock(storage.clone(), file).await?;
    let mut entries = decode_mrf_file(&data)?;
    let Some(entry) = entries.iter_mut().find(|entry| entry.force_delete_id == Some(operation_id)) else {
        return Err(EcstoreError::ConfigNotFound);
    };
    if entry.force_delete_local_commit {
        return Ok(());
    }
    entry.force_delete_local_commit = true;
    ReplicationConfigStore::save_no_lock(storage, file, encode_mrf_file(&entries)?).await
}

pub async fn complete_force_delete_intent<S: ReplicationStorage>(
    storage: Arc<S>,
    operation_id: uuid::Uuid,
) -> Result<(), EcstoreError> {
    let file = ReplicationMetadataStore::FORCE_DELETE_REPLICATION_FILE;
    let lock = storage
        .new_ns_lock(ReplicationMetadataStore::rustfs_meta_bucket(), file)
        .await?;
    let _guard = lock.get_write_lock(ReplicationLockTiming::acquire_timeout()).await?;

    let data = match ReplicationConfigStore::read_no_lock(storage.clone(), file).await {
        Ok(data) => data,
        Err(EcstoreError::ConfigNotFound) => return Ok(()),
        Err(err) => return Err(err),
    };
    let mut entries = decode_mrf_file(&data)?;
    let original_len = entries.len();
    entries.retain(|entry| entry.force_delete_id != Some(operation_id));
    if entries.len() == original_len {
        return Ok(());
    }

    ReplicationConfigStore::save_no_lock(storage, file, encode_mrf_file(&entries)?).await
}

#[derive(Debug, thiserror::Error)]
#[error("replication resync {active_resync_id} is already active for {bucket}/{arn}")]
struct ResyncActiveConflictError {
    bucket: String,
    arn: String,
    active_resync_id: String,
}

pub fn resync_start_conflict_id(error: &EcstoreError) -> Option<&str> {
    match error {
        EcstoreError::Io(io_error) => io_error
            .get_ref()?
            .downcast_ref::<ResyncActiveConflictError>()
            .map(|conflict| conflict.active_resync_id.as_str()),
        _ => None,
    }
}

/// Main replication pool structure
#[derive(Debug)]
pub struct ReplicationPool<S: ReplicationStorage> {
    // Atomic counters for active workers
    active_workers: Arc<AtomicI32>,
    active_lrg_workers: Arc<AtomicI32>,
    active_mrf_workers: Arc<AtomicI32>,

    storage: Arc<S>,

    // Configuration
    priority: RwLock<ReplicationPriority>,
    max_workers: RwLock<usize>,
    max_l_workers: RwLock<usize>,

    // Statistics
    stats: Arc<ReplicationStats>,

    // Worker channels
    workers: RwLock<Vec<Sender<ReplicationOperation>>>,
    lrg_workers: RwLock<Vec<Sender<ReplicationOperation>>>,

    // MRF (Most Recent Failures) channels
    mrf_replica_tx: Sender<ReplicationOperation>,
    // Shared among N MRF workers; Arc allows spawning more than one worker.
    mrf_replica_rx: Arc<Mutex<Receiver<ReplicationOperation>>>,
    mrf_save_tx: Sender<MrfSaveRequest>,
    mrf_save_rx: Mutex<Option<Receiver<MrfSaveRequest>>>,
    mrf_save_permits: Arc<Semaphore>,
    mrf_enqueued_order: AtomicU64,

    // Control channels
    mrf_worker_kill_tx: Sender<()>,
    mrf_stop_tx: Sender<()>,

    // Worker size tracking
    mrf_worker_size: AtomicI32,

    // Task handles for cleanup
    task_handles: Mutex<Vec<JoinHandle<()>>>,
    mrf_processor_started: AtomicBool,

    // Replication resyncer for handling bucket resync operations
    resyncer: Arc<ReplicationResyncer>,
}

impl<S: ReplicationStorage> ReplicationPool<S> {
    /// Creates a new replication pool with specified options
    pub async fn new(opts: ReplicationPoolOpts, stats: Arc<ReplicationStats>, storage: Arc<S>) -> Arc<Self> {
        let worker_counts = initial_worker_counts(&opts);
        let max_workers = opts.max_workers.unwrap_or(WORKER_MAX_LIMIT);
        let max_l_workers = opts.max_l_workers.unwrap_or(LARGE_WORKER_COUNT);

        // Create MRF channels
        let (mrf_replica_tx, mrf_replica_rx) = mpsc::channel(100000);
        let (mrf_save_tx, mrf_save_rx) = mpsc::channel(MRF_SAVE_CHANNEL_CAP);
        let (mrf_worker_kill_tx, _mrf_worker_kill_rx) = mpsc::channel(worker_counts.mrf_workers);
        let (mrf_stop_tx, _mrf_stop_rx) = mpsc::channel(1);
        let mrf_enqueued_order = read_durable_mrf_backlog(storage.clone())
            .await
            .entries
            .into_iter()
            .filter_map(|entry| entry.enqueued_order)
            .max()
            .unwrap_or_default();

        let pool = Arc::new(Self {
            active_workers: Arc::new(AtomicI32::new(0)),
            active_lrg_workers: Arc::new(AtomicI32::new(0)),
            active_mrf_workers: Arc::new(AtomicI32::new(0)),
            priority: RwLock::new(opts.priority),
            max_workers: RwLock::new(max_workers),
            max_l_workers: RwLock::new(max_l_workers),
            stats,
            storage,
            workers: RwLock::new(Vec::new()),
            lrg_workers: RwLock::new(Vec::new()),
            mrf_replica_tx,
            mrf_replica_rx: Arc::new(Mutex::new(mrf_replica_rx)),
            mrf_save_tx,
            mrf_save_rx: Mutex::new(Some(mrf_save_rx)),
            mrf_save_permits: Arc::new(Semaphore::new(MRF_PENDING_CAP)),
            mrf_enqueued_order: AtomicU64::new(mrf_enqueued_order),
            mrf_worker_kill_tx,
            mrf_stop_tx,
            mrf_worker_size: AtomicI32::new(0),
            task_handles: Mutex::new(Vec::new()),
            mrf_processor_started: AtomicBool::new(false),
            resyncer: Arc::new(ReplicationResyncer::new().await),
        });

        // Initialize workers
        pool.resize_lrg_workers(max_l_workers, 0).await;
        pool.resize_workers(worker_counts.workers, 0).await;
        pool.resize_failed_workers(worker_counts.mrf_workers_i32()).await;

        // Start the persister immediately. MRF recovery is started only after the
        // pool has been published in the instance context; the force-delete
        // processor keeps main's construction-time placement (#5641).
        pool.start_force_delete_processor().await;
        pool.start_mrf_persister().await;

        pool
    }

    /// Returns the number of active workers handling replication traffic
    pub fn active_workers(&self) -> i32 {
        self.active_workers.load(Ordering::SeqCst)
    }

    /// Returns the number of active workers handling replication failures
    pub fn active_mrf_workers(&self) -> i32 {
        self.active_mrf_workers.load(Ordering::SeqCst)
    }

    /// Returns the number of active workers handling traffic > 128MiB object size
    pub fn active_lrg_workers(&self) -> i32 {
        self.active_lrg_workers.load(Ordering::SeqCst)
    }

    /// Resizes the large workers pool
    pub async fn resize_lrg_workers(&self, n: usize, check_old: usize) {
        let mut lrg_workers = self.lrg_workers.write().await;

        if (check_old > 0 && lrg_workers.len() != check_old) || n == lrg_workers.len() || n < 1 {
            return;
        }

        // Add workers if needed
        while lrg_workers.len() < n {
            let (tx, rx) = mpsc::channel(100000);
            lrg_workers.push(tx);

            let active_counter = self.active_lrg_workers.clone();
            let storage = self.storage.clone();
            let stats = self.stats.clone();

            let handle = tokio::spawn(async move {
                let mut rx = rx;
                while let Some(operation) = rx.recv().await {
                    let _active = ActiveWorkerGuard::new(active_counter.clone());
                    process_replication_operation(operation, stats.clone(), storage.clone()).await;
                }
            });

            self.task_handles.lock().await.push(handle);
        }

        // Remove workers if needed
        while lrg_workers.len() > n {
            if let Some(worker) = lrg_workers.pop() {
                drop(worker); // Closing the channel will terminate the worker
            }
        }
    }

    /// Resizes the regular workers pool
    pub async fn resize_workers(&self, n: usize, check_old: usize) {
        let mut workers = self.workers.write().await;

        if (check_old > 0 && workers.len() != check_old) || n == workers.len() || n < 1 {
            debug!(
                event = EVENT_REPLICATION_WORKER_RESIZE_SKIPPED,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_REPLICATION,
                check_old_mismatch = check_old > 0 && workers.len() != check_old,
                same_size = n == workers.len(),
                invalid_target_size = n < 1,
                current_workers = workers.len(),
                target_workers = n,
                "Skipped replication worker resize"
            );
            return;
        }

        // Add workers if needed
        if workers.len() < n {
            info!(
                event = EVENT_REPLICATION_WORKER_RESIZED,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_REPLICATION,
                action = "increase",
                from_workers = workers.len(),
                to_workers = n,
                "Resized replication workers"
            );
        }

        while workers.len() < n {
            let (tx, rx) = mpsc::channel(10000);
            workers.push(tx);

            let active_counter = self.active_workers.clone();
            let stats = self.stats.clone();
            let storage = self.storage.clone();

            let handle = tokio::spawn(async move {
                let mut rx = rx;
                while let Some(operation) = rx.recv().await {
                    let _active = ActiveWorkerGuard::new(active_counter.clone());
                    process_replication_operation(operation, stats.clone(), storage.clone()).await;
                }
            });

            self.task_handles.lock().await.push(handle);
        }

        // Remove workers if needed
        if workers.len() > n {
            info!(
                event = EVENT_REPLICATION_WORKER_RESIZED,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_REPLICATION,
                action = "decrease",
                from_workers = workers.len(),
                to_workers = n,
                "Resized replication workers"
            );
        }

        while workers.len() > n {
            if let Some(worker) = workers.pop() {
                drop(worker); // Closing the channel will terminate the worker
            }
        }
    }

    /// Resizes the failed workers pool
    pub async fn resize_failed_workers(&self, n: i32) {
        // Spawn workers up to n.  Each worker shares the receiver via Arc<Mutex<...>>.
        // The mutex is held only while calling recv() — released before processing — so
        // all workers process entries concurrently (the dequeue step is serialised but
        // the replication I/O is not).
        while self.mrf_worker_size.load(Ordering::SeqCst) < n {
            self.mrf_worker_size.fetch_add(1, Ordering::SeqCst);

            let active_counter = self.active_mrf_workers.clone();
            let stats = self.stats.clone();
            let storage = self.storage.clone();
            let mrf_rx = Arc::clone(&self.mrf_replica_rx);

            let handle = tokio::spawn(async move {
                loop {
                    let operation = { mrf_rx.lock().await.recv().await };
                    let Some(operation) = operation else { break };

                    let _active = ActiveWorkerGuard::new(active_counter.clone());
                    process_replication_operation(operation, stats.clone(), storage.clone()).await;
                }
            });
            self.task_handles.lock().await.push(handle);
        }

        // Remove workers if needed
        while self.mrf_worker_size.load(Ordering::SeqCst) > n {
            self.mrf_worker_size.fetch_sub(1, Ordering::SeqCst);
            let _ = self.mrf_worker_kill_tx.try_send(());
        }
    }

    /// Resizes worker priority and counts
    pub async fn resize_worker_priority(
        &self,
        pri: ReplicationPriority,
        max_workers: Option<usize>,
        max_l_workers: Option<usize>,
    ) {
        let current_workers = self.workers.read().await.len();
        let current_mrf = mrf_worker_size_to_count(self.mrf_worker_size.load(Ordering::SeqCst));
        let worker_counts = resized_worker_counts(&pri, max_workers, current_workers, current_mrf);

        if let Some(max_w) = max_workers {
            *self.max_workers.write().await = max_w;
        }

        let max_l_workers_val = max_l_workers.unwrap_or(LARGE_WORKER_COUNT);
        *self.max_l_workers.write().await = max_l_workers_val;
        *self.priority.write().await = pri;

        self.resize_workers(worker_counts.workers, 0).await;
        self.resize_failed_workers(worker_counts.mrf_workers_i32()).await;
        self.resize_lrg_workers(max_l_workers_val, 0).await;
    }

    /// Gets a worker channel deterministically based on bucket and object names
    async fn get_worker_ch(&self, bucket: &str, object: &str, _size: i64) -> Option<Sender<ReplicationOperation>> {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        format!("{bucket}{object}").hash(&mut hasher);
        let hash = hasher.finish();

        let workers = self.workers.read().await;
        if workers.is_empty() {
            return None;
        }

        let index = (hash as usize) % workers.len();
        workers.get(index).cloned()
    }

    async fn worker_queue_channel(
        &self,
        op_type: &ReplicationType,
        bucket: &str,
        object: &str,
        size: i64,
    ) -> Option<Sender<ReplicationOperation>> {
        match worker_queue_for_replication_type(op_type) {
            ReplicationWorkerQueue::Mrf => Some(self.mrf_replica_tx.clone()),
            ReplicationWorkerQueue::Regular => self.get_worker_ch(bucket, object, size).await,
        }
    }

    async fn apply_queue_backpressure(&self, queue_type: &'static str, include_mrf_workers: bool, message: &'static str) {
        let priority = self.priority.read().await.clone();
        let max_workers = *self.max_workers.read().await;
        let current_workers = self.workers.read().await.len();
        let current_mrf_workers = self.mrf_worker_size.load(Ordering::SeqCst);
        let recommendation = replication_backpressure_recommendation(
            &priority,
            ReplicationBackpressureState {
                current_workers,
                active_workers: self.active_workers(),
                current_mrf_workers,
                active_mrf_workers: self.active_mrf_workers(),
                max_workers,
                include_mrf_workers,
            },
        );

        match recommendation {
            ReplicationBackpressureRecommendation::KeepFast => {
                debug!(
                    event = EVENT_REPLICATION_BACKPRESSURE,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_REPLICATION,
                    queue_type,
                    priority = "fast",
                    recommendation = "none",
                    "{message}"
                );
            }
            ReplicationBackpressureRecommendation::SetPriorityAuto => {
                debug!(
                    event = EVENT_REPLICATION_BACKPRESSURE,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_REPLICATION,
                    queue_type,
                    priority = "slow",
                    recommendation = "set_priority_auto",
                    "{message}"
                );
            }
            ReplicationBackpressureRecommendation::Resize(resize) => {
                if let Some(regular_workers) = resize.regular_workers {
                    self.resize_workers(regular_workers.new_count, regular_workers.existing_count)
                        .await;
                }

                if let Some(mrf_workers) = resize.mrf_workers {
                    self.resize_failed_workers(mrf_workers).await;
                }
            }
            ReplicationBackpressureRecommendation::Noop => {}
        }
    }

    /// Queues a replica task
    pub async fn queue_replica_task(&self, ri: ReplicateObjectInfo) -> ReplicationQueueAdmission {
        let target_arns = ri.dsc.replicate_target_arns();
        // If object is large, queue it to a static set of large workers
        if should_queue_large_object(ri.size) {
            use std::collections::hash_map::DefaultHasher;
            use std::hash::{Hash, Hasher};

            let mut hasher = DefaultHasher::new();
            format!("{}{}", ri.bucket, ri.name).hash(&mut hasher);
            let hash = hasher.finish();

            let lrg_workers = self.lrg_workers.read().await;

            if !lrg_workers.is_empty() {
                let index = (hash as usize) % lrg_workers.len();

                if let Some(worker) = lrg_workers.get(index) {
                    self.stats.inc_q(&ri.bucket, ri.size, ri.delete_marker, ri.op_type);
                    self.stats.inc_target_q(&ri.bucket, &target_arns, ri.size);
                    if worker.try_send(ReplicationOperation::Object(Box::new(ri.clone()))).is_ok() {
                        return ReplicationQueueAdmission::Queued;
                    }
                    self.stats.dec_q(&ri.bucket, ri.size, ri.delete_marker, ri.op_type);
                    self.stats.dec_target_q(&ri.bucket, &target_arns, ri.size);

                    // Try to add more workers if possible
                    let max_l_workers = *self.max_l_workers.read().await;
                    let existing = lrg_workers.len();
                    let resize = large_worker_backpressure_resize(existing, self.active_lrg_workers(), max_l_workers);
                    drop(lrg_workers);

                    // Queue to MRF if worker is busy.
                    let admission = self.queue_mrf_save_admission(ri.to_mrf_entry(), "large_object").await;

                    if let Some(resize) = resize {
                        self.resize_lrg_workers(resize.new_count, resize.existing_count).await;
                    }
                    return admission;
                }
            }
            return ReplicationQueueAdmission::Missed;
        }

        // Handle regular sized objects

        let ch = self.worker_queue_channel(&ri.op_type, &ri.bucket, &ri.name, ri.size).await;

        let Some(channel) = ch else {
            return ReplicationQueueAdmission::Missed;
        };

        self.stats.inc_q(&ri.bucket, ri.size, ri.delete_marker, ri.op_type);
        self.stats.inc_target_q(&ri.bucket, &target_arns, ri.size);
        if channel.try_send(ReplicationOperation::Object(Box::new(ri.clone()))).is_ok() {
            return ReplicationQueueAdmission::Queued;
        }
        self.stats.dec_q(&ri.bucket, ri.size, ri.delete_marker, ri.op_type);
        self.stats.dec_target_q(&ri.bucket, &target_arns, ri.size);

        // Queue to MRF if all workers are busy.
        let admission = self.queue_mrf_save_admission(ri.to_mrf_entry(), "object").await;

        // Try to scale up workers based on priority
        self.apply_queue_backpressure("object", true, "Replication queue is backpressured")
            .await;

        admission
    }

    /// Queues a replica delete task
    pub async fn queue_replica_delete_task(&self, doi: DeletedObjectReplicationInfo) -> ReplicationQueueAdmission {
        let target_arns = doi.admitted_target_arns();
        let ch = self
            .worker_queue_channel(&doi.op_type, &doi.bucket, &doi.delete_object.object_name, 0)
            .await;

        let Some(channel) = ch else {
            return ReplicationQueueAdmission::Missed;
        };

        self.stats.inc_q(&doi.bucket, 0, true, doi.op_type);
        self.stats.inc_target_q(&doi.bucket, &target_arns, 0);
        if channel.try_send(ReplicationOperation::Delete(Box::new(doi.clone()))).is_ok() {
            return ReplicationQueueAdmission::Queued;
        }
        self.stats.dec_q(&doi.bucket, 0, true, doi.op_type);
        self.stats.dec_target_q(&doi.bucket, &target_arns, 0);

        if doi.delete_object.force_delete {
            self.stats.inc_q(&doi.bucket, 0, true, doi.op_type);
            self.stats.inc_target_q(&doi.bucket, &target_arns, 0);
            if channel
                .send(ReplicationOperation::Delete(Box::new(doi.clone())))
                .await
                .is_ok()
            {
                return ReplicationQueueAdmission::Queued;
            }
            self.stats.dec_q(&doi.bucket, 0, true, doi.op_type);
            self.stats.dec_target_q(&doi.bucket, &target_arns, 0);
            return ReplicationQueueAdmission::Missed;
        }

        let admission = self.queue_mrf_save_admission(doi.to_mrf_entry(), "delete").await;

        self.apply_queue_backpressure("delete", false, "Replication delete queue is backpressured")
            .await;

        admission
    }

    /// Queues a DeleteObjects replication tail with a fixed concurrency window.
    /// Each item retains the existing regular-worker to MRF fallback contract.
    pub async fn queue_replica_delete_batch(&self, deletes: &[DeletedObjectReplicationInfo]) -> ReplicationBatchAdmission {
        let mut summary = ReplicationBatchAdmission::default();
        let mut admissions = stream::iter(
            deletes
                .iter()
                .cloned()
                .map(|delete| async move { self.queue_replica_delete_task(delete).await }),
        )
        .buffer_unordered(DELETE_BATCH_ADMISSION_CONCURRENCY);

        while let Some(admission) = admissions.next().await {
            summary.record(admission);
        }

        let outcome = summary.outcome();
        let total = u64::try_from(summary.total).unwrap_or(u64::MAX);
        let queued = u64::try_from(summary.queued).unwrap_or(u64::MAX);
        let missed = u64::try_from(summary.missed).unwrap_or(u64::MAX);
        histogram!(METRIC_DELETE_BATCH_SIZE).record(total as f64);
        counter!(METRIC_DELETE_BATCH_ITEMS_TOTAL, "outcome" => outcome, "state" => "queued").increment(queued);
        counter!(METRIC_DELETE_BATCH_ITEMS_TOTAL, "outcome" => outcome, "state" => "missed").increment(missed);
        debug!(
            event = "replication_delete_batch_admission",
            batch_size = summary.total,
            queued = summary.queued,
            missed = summary.missed,
            outcome,
            "Admitted DeleteObjects replication batch"
        );
        summary
    }

    /// Queues an MRF save operation
    async fn queue_mrf_save(&self, entry: MrfReplicateEntry) {
        let _ = self.queue_mrf_save_admission(entry, "mrf_worker").await;
    }

    pub async fn queue_mrf_delete_task(&self, doi: DeletedObjectReplicationInfo) -> ReplicationQueueAdmission {
        self.queue_mrf_delete_tasks(vec![doi]).await
    }

    pub async fn queue_mrf_delete_tasks(&self, tasks: Vec<DeletedObjectReplicationInfo>) -> ReplicationQueueAdmission {
        if tasks.is_empty() {
            return ReplicationQueueAdmission::Skipped;
        }
        if tasks.iter().any(|task| {
            task.target_arn.is_empty()
                || task.target_arn.len() > MAX_MRF_TARGET_FIELD_LEN
                || task
                    .target_delete_marker_version_id
                    .as_ref()
                    .is_some_and(|version_id| version_id.is_empty() || version_id.len() > MAX_MRF_TARGET_FIELD_LEN)
        }) {
            for task in &tasks {
                observe_mrf_missed(&task.bucket);
            }
            return ReplicationQueueAdmission::Missed;
        }
        let entries = tasks.into_iter().map(|task| task.to_mrf_entry()).collect::<Vec<_>>();
        if !entries.iter().all(targeted_mrf_entry_is_valid) {
            return ReplicationQueueAdmission::Missed;
        }
        if MRF_REPLAY_RETRIES
            .try_with(|replay| match replay.lock() {
                Ok(mut retries) => retries.extend(entries.iter().cloned()),
                Err(poisoned) => poisoned.into_inner().extend(entries.iter().cloned()),
            })
            .is_ok()
        {
            return ReplicationQueueAdmission::Queued;
        }
        self.queue_mrf_entries_admission(entries, "target_delete_failure").await
    }

    async fn queue_mrf_save_admission(&self, entry: MrfReplicateEntry, queue_type: &'static str) -> ReplicationQueueAdmission {
        self.queue_mrf_entries_admission(vec![entry], queue_type).await
    }

    async fn queue_mrf_entries_admission(
        &self,
        mut entries: Vec<MrfReplicateEntry>,
        queue_type: &'static str,
    ) -> ReplicationQueueAdmission {
        if entries.is_empty() {
            return ReplicationQueueAdmission::Skipped;
        }
        if entries.len() > MRF_SAVE_REQUEST_CAP {
            return ReplicationQueueAdmission::Missed;
        }
        for entry in &mut entries {
            entry.source_mod_time = entry.source_mod_time.or(entry.delete_marker_mtime);
            if entry.enqueued_order.is_none() {
                entry.enqueued_order = Some(self.next_mrf_enqueued_order());
            }
        }
        let file = mrf_file_for_entry(&entries[0]);
        if entries.iter().any(|entry| mrf_file_for_entry(entry) != file) {
            return ReplicationQueueAdmission::Missed;
        }
        if file == ReplicationMetadataStore::TARGETED_MRF_REPLICATION_FILE && !entries.iter().all(targeted_mrf_entry_is_valid) {
            for entry in &entries {
                observe_mrf_missed(&entry.bucket);
            }
            return ReplicationQueueAdmission::Missed;
        }
        let Ok(permit_count) = u32::try_from(entries.len()) else {
            return ReplicationQueueAdmission::Missed;
        };
        let Ok(permit) = self.mrf_save_permits.clone().try_acquire_many_owned(permit_count) else {
            for entry in &entries {
                observe_mrf_drop(entry);
            }
            return ReplicationQueueAdmission::Missed;
        };
        let stats_entries = entries.clone();
        let (persisted, completion) = oneshot::channel();
        for entry in &stats_entries {
            self.stats
                .inc_q(&entry.bucket, entry.size, matches!(entry.op, MrfOpKind::Delete), ReplicationType::Heal);
            self.stats.inc_target_q(&entry.bucket, &entry.target_arns, entry.size);
        }
        if let Err(error) = self
            .mrf_save_tx
            .send(MrfSaveRequest {
                file,
                entries,
                persisted,
                permit,
            })
            .await
        {
            dec_mrf_entries(self.stats.as_ref(), &stats_entries);
            for entry in &error.0.entries {
                observe_mrf_missed(&entry.bucket);
            }
            return ReplicationQueueAdmission::Missed;
        }
        match completion.await {
            Ok(true) => ReplicationQueueAdmission::Queued,
            Ok(false) => ReplicationQueueAdmission::Missed,
            Err(_) => {
                dec_mrf_entries(self.stats.as_ref(), &stats_entries);
                for entry in &stats_entries {
                    observe_mrf_missed(&entry.bucket);
                }
                warn!(
                    event = EVENT_REPLICATION_MRF_QUEUE_UNAVAILABLE,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_REPLICATION,
                    queue_type,
                    "MRF persistence task stopped before durable acknowledgement"
                );
                ReplicationQueueAdmission::Missed
            }
        }
    }

    fn next_mrf_enqueued_order(&self) -> u64 {
        let now = u64::try_from(OffsetDateTime::now_utc().unix_timestamp_nanos()).unwrap_or_default();
        let mut current = self.mrf_enqueued_order.load(Ordering::Acquire);
        loop {
            let next = current.max(now).saturating_add(1);
            match self
                .mrf_enqueued_order
                .compare_exchange_weak(current, next, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => return next,
                Err(observed) => current = observed,
            }
        }
    }

    /// Starts the MRF processor once, after the pool is published.
    async fn start_mrf_processor(&self) {
        if self
            .mrf_processor_started
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }

        let storage = self.storage.clone();

        let handle = tokio::spawn(async move {
            loop {
                let recovered = process_mrf_backlog(storage.clone()).await;
                refresh_durable_mrf_backlog_snapshot(storage.clone()).await;
                if recovered > 0 {
                    info!(
                        component = LOG_COMPONENT_ECSTORE,
                        subsystem = LOG_SUBSYSTEM_REPLICATION,
                        recovered,
                        "Recovered MRF entries from disk"
                    );
                }
                tokio::time::sleep(super::replication_timing::mrf_flush_interval()).await;
            }
        });
        self.task_handles.lock().await.push(handle);
    }

    async fn start_force_delete_processor(&self) {
        let storage = self.storage.clone();
        let handle =
            tokio::spawn(async move {
                let data =
                    match ReplicationConfigStore::read(storage.clone(), ReplicationMetadataStore::FORCE_DELETE_REPLICATION_FILE)
                        .await
                    {
                        Ok(data) => data,
                        Err(EcstoreError::ConfigNotFound) => return,
                        Err(error) => {
                            warn!(
                                component = LOG_COMPONENT_ECSTORE,
                                subsystem = LOG_SUBSYSTEM_REPLICATION,
                                error = %error,
                                "Failed to load durable force-delete intents"
                            );
                            return;
                        }
                    };

                let entries = match decode_mrf_file(&data) {
                    Ok(entries) => entries,
                    Err(error) => {
                        warn!(
                            component = LOG_COMPONENT_ECSTORE,
                            subsystem = LOG_SUBSYSTEM_REPLICATION,
                            error = %error,
                            "Failed to decode durable force-delete intents"
                        );
                        return;
                    }
                };

                for entry in entries {
                    if !should_replay_force_delete_intent(&entry) {
                        continue;
                    }
                    let Some(operation_id) = entry.force_delete_id else {
                        continue;
                    };
                    schedule_replication_delete(DeletedObjectReplicationInfo {
                        delete_object: ReplicationDeletedObject {
                            object_name: entry.object,
                            force_delete: true,
                            force_delete_id: Some(operation_id),
                            force_delete_target_arns: entry.target_arns,
                            force_delete_generation: entry.force_delete_generation,
                            ..Default::default()
                        },
                        bucket: entry.bucket,
                        op_type: ReplicationType::Heal,
                        event_type: REPLICATE_HEAL_DELETE.to_string(),
                        ..Default::default()
                    })
                    .await;
                }
            });
        self.task_handles.lock().await.push(handle);
    }

    /// Starts the MRF persister — ongoing background task.
    ///
    /// Drains `mrf_save_rx` (entries that overflowed the normal worker channels) and
    /// coalesces newly admitted requests for 10ms before flushing, and retries failed
    /// persistence on the configured interval. Each flush merges with the durable
    /// backlog while holding the MRF config object's distributed write lock.
    async fn start_mrf_persister(&self) {
        let Some(mut rx) = self.mrf_save_rx.lock().await.take() else {
            return;
        };
        let storage = self.storage.clone();
        let stats = self.stats.clone();

        let handle = tokio::spawn(async move {
            let mut legacy_pending = PendingMrfFile::default();
            let mut targeted_pending = PendingMrfFile::default();
            // Flush interval: `RUSTFS_REPL_MRF_FLUSH_INTERVAL_MS` (default 10000ms,
            // clamped to >=10ms), read once when the persister task starts.
            let mut interval = tokio::time::interval(super::replication_timing::mrf_flush_interval());
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                tokio::select! {
                    request = rx.recv() => match request {
                        Some(request) => {
                            {
                                let mut enqueue = |request: MrfSaveRequest| {
                                    let pending = if request.file == ReplicationMetadataStore::TARGETED_MRF_REPLICATION_FILE {
                                        &mut targeted_pending
                                    } else {
                                        &mut legacy_pending
                                    };
                                    for entry in &request.entries {
                                        observe_mrf_pending(entry);
                                    }
                                    pending.entries.extend(request.entries);
                                    pending.persisted.push(request.persisted);
                                    pending.permits.push(request.permit);
                                };
                                enqueue(request);
                                tokio::time::sleep(MRF_ADMISSION_BATCH_DELAY).await;
                                while let Ok(request) = rx.try_recv() {
                                    enqueue(request);
                                }
                            }
                            let legacy_flushed = !legacy_pending.entries.is_empty()
                                && flush_pending_mrf_file(
                                    ReplicationMetadataStore::MRF_REPLICATION_FILE,
                                    &mut legacy_pending,
                                    &storage,
                                    stats.as_ref(),
                                )
                                .await;
                            let targeted_flushed = !targeted_pending.entries.is_empty()
                                && flush_pending_mrf_file(
                                    ReplicationMetadataStore::TARGETED_MRF_REPLICATION_FILE,
                                    &mut targeted_pending,
                                    &storage,
                                    stats.as_ref(),
                                )
                                .await;
                            if legacy_flushed || targeted_flushed {
                                refresh_durable_mrf_backlog_snapshot(storage.clone()).await;
                            }
                        }
                        None => {
                            let legacy_flushed = legacy_pending.entries.is_empty()
                                || flush_pending_mrf_file(
                                    ReplicationMetadataStore::MRF_REPLICATION_FILE,
                                    &mut legacy_pending,
                                    &storage,
                                    stats.as_ref(),
                                )
                                .await;
                            let targeted_flushed = targeted_pending.entries.is_empty()
                                || flush_pending_mrf_file(
                                    ReplicationMetadataStore::TARGETED_MRF_REPLICATION_FILE,
                                    &mut targeted_pending,
                                    &storage,
                                    stats.as_ref(),
                                )
                                .await;
                            if legacy_flushed || targeted_flushed {
                                refresh_durable_mrf_backlog_snapshot(storage.clone()).await;
                            }
                            break;
                        }
                    },
                    _ = interval.tick() => {
                        let legacy_flushed = !legacy_pending.entries.is_empty()
                            && flush_pending_mrf_file(
                                ReplicationMetadataStore::MRF_REPLICATION_FILE,
                                &mut legacy_pending,
                                &storage,
                                stats.as_ref(),
                            )
                            .await;
                        let targeted_flushed = !targeted_pending.entries.is_empty()
                            && flush_pending_mrf_file(
                                ReplicationMetadataStore::TARGETED_MRF_REPLICATION_FILE,
                                &mut targeted_pending,
                                &storage,
                                stats.as_ref(),
                            )
                            .await;
                        if legacy_flushed || targeted_flushed {
                            refresh_durable_mrf_backlog_snapshot(storage.clone()).await;
                        }
                    }
                }
            }
        });
        self.task_handles.lock().await.push(handle);
    }

    /// Worker function for handling regular replication operations
    async fn add_worker(
        &self,
        mut rx: Receiver<ReplicationOperation>,
        active_counter: Arc<AtomicI32>,
        stats: Arc<ReplicationStats>,
    ) {
        while let Some(operation) = rx.recv().await {
            let _active = ActiveWorkerGuard::new(active_counter.clone());
            process_replication_operation(operation, stats.clone(), self.storage.clone()).await;
        }
    }

    /// Worker function for handling large object replication operations
    async fn add_large_worker(
        &self,
        mut rx: Receiver<ReplicationOperation>,
        active_counter: Arc<AtomicI32>,
        stats: Arc<ReplicationStats>,
        storage: Arc<S>,
    ) {
        while let Some(operation) = rx.recv().await {
            let _active = ActiveWorkerGuard::new(active_counter.clone());
            process_replication_operation(operation, stats.clone(), storage.clone()).await;
        }
    }

    /// Worker function for handling MRF (Most Recent Failures) operations
    async fn add_mrf_worker(
        &self,
        mut rx: Receiver<ReplicationOperation>,
        active_counter: Arc<AtomicI32>,
        stats: Arc<ReplicationStats>,
    ) {
        while let Some(operation) = rx.recv().await {
            let _active = ActiveWorkerGuard::new(active_counter.clone());
            process_replication_operation(operation, stats.clone(), self.storage.clone()).await;
        }
    }

    /// Delete resync metadata from replication resync state in memory
    pub async fn delete_resync_metadata(&self, bucket: &str) {
        let mut status_map = self.resyncer.status_map.write().await;
        status_map.remove(bucket);
        // Note: global site resync metrics deletion would be handled here
        // global_site_resync_metrics.delete_bucket(bucket);
    }

    /// Initialize bucket replication resync for all buckets
    pub async fn init_resync_internal(
        self: Arc<Self>,
        cancellation_token: CancellationToken,
        buckets: Vec<String>,
    ) -> Result<(), EcstoreError> {
        // Load bucket metadata system in background
        let pool_clone = self;

        tokio::spawn(async move {
            pool_clone.start_resync_routine(buckets, cancellation_token).await;
        });

        Ok(())
    }

    pub async fn get_bucket_resync_status(&self, bucket: &str) -> Result<BucketReplicationResyncStatus, EcstoreError> {
        if let Some(status) = self.resyncer.status_map.read().await.get(bucket).cloned() {
            return Ok(status);
        }

        let status = load_bucket_resync_metadata(bucket, self.storage.clone()).await?;
        self.resyncer
            .status_map
            .write()
            .await
            .insert(bucket.to_string(), status.clone());
        Ok(status)
    }

    pub async fn cancel_bucket_resync(&self, opts: ResyncOpts) -> Result<(), EcstoreError> {
        self.resyncer.cancel(&opts).await;
        self.resyncer
            .mark_status(ResyncStatusType::ResyncCanceled, opts, self.storage.clone())
            .await?;
        Ok(())
    }

    pub async fn start_bucket_resync(self: Arc<Self>, opts: ResyncOpts) -> Result<(), EcstoreError> {
        let new_run = self.clone().admit_bucket_resync(opts.clone()).await?;
        self.activate_bucket_resync(opts, !new_run).await
    }

    pub async fn admit_bucket_resync(self: Arc<Self>, opts: ResyncOpts) -> Result<bool, EcstoreError> {
        tokio::spawn(async move { self.admit_bucket_resync_transaction(opts).await })
            .await
            .map_err(|error| EcstoreError::other(format!("replication resync admission task failed: {error}")))?
    }

    async fn admit_bucket_resync_transaction(self: Arc<Self>, opts: ResyncOpts) -> Result<bool, EcstoreError> {
        let admission_lock_key = ReplicationMetadataStore::resync_admission_lock_key(&opts.bucket);
        let admission_lock = self
            .storage
            .new_ns_lock(ReplicationMetadataStore::rustfs_meta_bucket(), &admission_lock_key)
            .await?;
        // Lock order: bucket resync admission lock -> resync status config-object lock.
        let _admission_guard = match admission_lock.get_write_lock(ReplicationLockTiming::acquire_timeout()).await {
            Ok(guard) => guard,
            Err(lock_error) => {
                if let Ok(status) = load_bucket_resync_metadata(&opts.bucket, self.storage.clone()).await {
                    self.resyncer.status_map.write().await.insert(opts.bucket.clone(), status);
                }
                return Err(EcstoreError::from(lock_error));
            }
        };

        let mut bucket_status = load_bucket_resync_metadata(&opts.bucket, self.storage.clone()).await?;
        if let Some(active) = bucket_status.targets_map.get(&opts.arn) {
            if active.resync_id == opts.resync_id {
                self.resyncer
                    .status_map
                    .write()
                    .await
                    .insert(opts.bucket.clone(), bucket_status);
                return Ok(false);
            }
            if should_auto_resume_resync(active.resync_status) {
                let active_resync_id = active.resync_id.clone();
                self.resyncer
                    .status_map
                    .write()
                    .await
                    .insert(opts.bucket.clone(), bucket_status);
                return Err(EcstoreError::other(ResyncActiveConflictError {
                    bucket: opts.bucket.clone(),
                    arn: opts.arn.clone(),
                    active_resync_id,
                }));
            }
        }

        let now = OffsetDateTime::now_utc();
        bucket_status.last_update = Some(now);
        bucket_status.targets_map.insert(
            opts.arn.clone(),
            TargetReplicationResyncStatus {
                start_time: Some(now),
                last_update: Some(now),
                resync_id: opts.resync_id.clone(),
                resync_before_date: opts.resync_before,
                resync_status: ResyncStatusType::ResyncPending,
                failed_size: 0,
                failed_count: 0,
                replicated_size: 0,
                replicated_count: 0,
                bucket: opts.bucket.clone(),
                object: String::new(),
                error: None,
            },
        );

        save_resync_status(&opts.bucket, &bucket_status, self.storage.clone()).await?;
        self.resyncer
            .status_map
            .write()
            .await
            .insert(opts.bucket.clone(), bucket_status);

        Ok(true)
    }

    pub async fn activate_bucket_resync(self: Arc<Self>, opts: ResyncOpts, recovering: bool) -> Result<(), EcstoreError> {
        let bucket_status = load_bucket_resync_metadata(&opts.bucket, self.storage.clone()).await?;
        let Some(target_status) = bucket_status.targets_map.get(&opts.arn) else {
            return Err(EcstoreError::other("replication resync admission is missing"));
        };
        if target_status.resync_id != opts.resync_id {
            return Err(EcstoreError::other(ResyncActiveConflictError {
                bucket: opts.bucket.clone(),
                arn: opts.arn.clone(),
                active_resync_id: target_status.resync_id.clone(),
            }));
        }
        if !should_auto_resume_resync(target_status.resync_status) {
            return Ok(());
        }
        self.resyncer
            .status_map
            .write()
            .await
            .insert(opts.bucket.clone(), bucket_status);

        let resyncer = self.resyncer.clone();
        let storage = self.storage.clone();
        let cancel_token = CancellationToken::new();
        if resyncer.register_cancel_token(&opts, cancel_token.clone()).await {
            tokio::spawn(async move {
                Box::pin(
                    resyncer
                        .clone()
                        .resync_bucket(cancel_token, storage, recovering, opts.clone()),
                )
                .await;
                resyncer.clear_cancel_token(&opts).await;
            });
        }

        Ok(())
    }

    /// Start the resync routine that runs in a loop
    async fn start_resync_routine(self: Arc<Self>, buckets: Vec<String>, cancellation_token: CancellationToken) {
        // Retry-poll sleep upper bound: `RUSTFS_REPL_RESYNC_POLL_MAX_MS`
        // (default 60000ms, clamped to >=10ms), read once when this routine
        // starts. The anti-busy-spin floor is min(1s, max) so the default
        // keeps the historical "sleep at least one second" behavior while
        // short test overrides stay short.
        let max_sleep = super::replication_timing::resync_poll_max_sleep();
        let max_sleep_ms = u64::try_from(max_sleep.as_millis()).unwrap_or(u64::MAX).max(1);
        let floor_sleep = Duration::from_secs(1).min(max_sleep);
        // Run the replication resync in a loop
        loop {
            let self_clone = self.clone();
            let ctx = cancellation_token.clone();
            tokio::select! {
                _ = cancellation_token.cancelled() => {
                    return;
                }
                result = self_clone.load_resync(&buckets, ctx) => {
                    if result.is_ok() {
                        return;
                    }
                }
            }

            // Generate a random duration between 0 and `max_sleep` (default 1 minute)
            use rand::RngExt;
            let duration_millis = rand::rng().random_range(0..max_sleep_ms);
            let mut duration = Duration::from_millis(duration_millis);

            // Make sure to sleep at least `floor_sleep` to avoid high CPU ticks
            if duration < floor_sleep {
                duration = floor_sleep;
            }

            tokio::time::sleep(duration).await;
        }
    }

    /// Load bucket replication resync statuses into memory
    #[instrument(skip(_cancellation_token))]
    async fn load_resync(
        self: Arc<Self>,
        buckets: &[String],
        _cancellation_token: CancellationToken,
    ) -> Result<(), EcstoreError> {
        let load_resync_lock = match self
            .storage
            .new_ns_lock(ReplicationMetadataStore::rustfs_meta_bucket(), "replication/resync/load-resync.lock")
            .await
        {
            Ok(lock) => lock,
            Err(err) => {
                warn!(
                    event = EVENT_REPLICATION_RESYNC_LOAD_SKIPPED,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_REPLICATION,
                    error = ?err,
                    reason = "leader_lock_create_failed",
                    "Skipped replication resync metadata load"
                );
                return Ok(());
            }
        };
        let _load_resync_guard = match load_resync_lock
            .get_write_lock(ReplicationLockTiming::acquire_timeout())
            .await
        {
            Ok(guard) => guard,
            Err(_) => {
                debug!(
                    event = EVENT_REPLICATION_RESYNC_LOAD_SKIPPED,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_REPLICATION,
                    reason = "leader_lock_held_by_another_node",
                    "Another node is already loading replication resync metadata"
                );
                return Ok(());
            }
        };

        let mut recovered_statuses = Vec::new();
        let mut restart_opts = Vec::new();
        let mut recovered_bucket_count = 0usize;
        let mut skipped_failed_target_count = 0usize;

        for bucket in buckets {
            let meta = match load_bucket_resync_metadata(bucket, self.storage.clone()).await {
                Ok(meta) => meta,
                Err(err) => {
                    if !matches!(err, EcstoreError::VolumeNotFound) {
                        debug!(
                            event = EVENT_REPLICATION_RESYNC_LOAD_SKIPPED,
                            component = LOG_COMPONENT_ECSTORE,
                            subsystem = LOG_SUBSYSTEM_REPLICATION,
                            bucket,
                            error = ?err,
                            reason = "metadata_load_failed",
                            "Skipped replication resync metadata load"
                        );
                    }
                    continue;
                }
            };

            if meta.targets_map.is_empty() {
                continue;
            }

            recovered_bucket_count += 1;
            for (arn, stats) in &meta.targets_map {
                if should_auto_resume_resync(stats.resync_status) {
                    restart_opts.push(ResyncOpts {
                        bucket: bucket.clone(),
                        arn: arn.clone(),
                        resync_id: stats.resync_id.clone(),
                        resync_before: stats.resync_before_date,
                    });
                } else if stats.resync_status == ResyncStatusType::ResyncFailed {
                    skipped_failed_target_count += 1;
                }
            }

            recovered_statuses.push((bucket.clone(), meta));
        }

        if !recovered_statuses.is_empty() {
            let mut status_map = self.resyncer.status_map.write().await;
            status_map.extend(recovered_statuses);
        }

        if !restart_opts.is_empty() || skipped_failed_target_count > 0 {
            info!(
                event = EVENT_REPLICATION_RESYNC_RECOVERED,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_REPLICATION,
                recovered_buckets = recovered_bucket_count,
                resumed_targets = restart_opts.len(),
                skipped_failed_targets = skipped_failed_target_count,
                "Recovered replication resync state from persisted metadata; failed targets require manual resync restart"
            );
        }

        for opts in restart_opts {
            let ctx = CancellationToken::new();
            let resync = self.resyncer.clone();
            let storage = self.storage.clone();
            tokio::spawn(async move {
                if resync.register_cancel_token(&opts, ctx.clone()).await {
                    Box::pin(resync.clone().resync_bucket(ctx, storage, true, opts.clone())).await;
                    resync.clear_cancel_token(&opts).await;
                }
            });
        }

        Ok(())
    }
}

struct ActiveWorkerGuard {
    counter: Arc<AtomicI32>,
}

impl ActiveWorkerGuard {
    fn new(counter: Arc<AtomicI32>) -> Self {
        counter.fetch_add(1, Ordering::SeqCst);
        Self { counter }
    }
}

impl Drop for ActiveWorkerGuard {
    fn drop(&mut self) {
        self.counter.fetch_sub(1, Ordering::SeqCst);
    }
}

struct ReplicationBacklogGuard {
    stats: Arc<ReplicationStats>,
    bucket: String,
    size: i64,
    is_delete_marker: bool,
    op_type: ReplicationType,
    target_arns: Vec<String>,
}

impl ReplicationBacklogGuard {
    fn for_object(stats: Arc<ReplicationStats>, object: &ReplicateObjectInfo) -> Self {
        Self {
            stats,
            bucket: object.bucket.clone(),
            size: object.size,
            is_delete_marker: object.delete_marker,
            op_type: object.op_type,
            target_arns: object.dsc.replicate_target_arns(),
        }
    }

    fn for_delete(stats: Arc<ReplicationStats>, delete: &DeletedObjectReplicationInfo) -> Self {
        Self {
            stats,
            bucket: delete.bucket.clone(),
            size: 0,
            is_delete_marker: true,
            op_type: delete.op_type,
            target_arns: delete.admitted_target_arns(),
        }
    }
}

impl Drop for ReplicationBacklogGuard {
    fn drop(&mut self) {
        self.stats.dec_q(&self.bucket, self.size, self.is_delete_marker, self.op_type);
        self.stats.dec_target_q(&self.bucket, &self.target_arns, self.size);
    }
}

async fn process_replication_operation<S: ReplicationStorage>(
    operation: ReplicationOperation,
    stats: Arc<ReplicationStats>,
    storage: Arc<S>,
) {
    match operation {
        ReplicationOperation::Object(obj_info) => {
            let _backlog = ReplicationBacklogGuard::for_object(stats, obj_info.as_ref());
            replicate_object(*obj_info, storage).await;
        }
        ReplicationOperation::Delete(del_info) => {
            let _backlog = ReplicationBacklogGuard::for_delete(stats, del_info.as_ref());
            replicate_delete(*del_info, storage).await;
        }
    }
}

fn dec_mrf_entries(stats: &ReplicationStats, entries: &[MrfReplicateEntry]) {
    for entry in entries {
        stats.dec_q(&entry.bucket, entry.size, matches!(entry.op, MrfOpKind::Delete), ReplicationType::Heal);
        stats.dec_target_q(&entry.bucket, &entry.target_arns, entry.size);
    }
}

async fn read_mrf_entries_no_lock<S: ReplicationObjectIO>(file: &str, storage: &Arc<S>) -> Result<MrfFileContents, EcstoreError> {
    match ReplicationConfigStore::read_no_lock_with_etag(storage.clone(), file).await {
        Ok((data, etag)) => {
            let entries = decode_mrf_file(&data).map_err(|error| EcstoreError::other(error.to_string()))?;
            if file == ReplicationMetadataStore::TARGETED_MRF_REPLICATION_FILE && !entries.iter().all(targeted_mrf_entry_is_valid)
            {
                return Err(EcstoreError::other("invalid targeted MRF entry"));
            }
            Ok(MrfFileContents {
                entries,
                expected_etag: Some(etag),
            })
        }
        Err(EcstoreError::ConfigNotFound) => Ok(MrfFileContents {
            entries: Vec::new(),
            expected_etag: None,
        }),
        Err(error) => Err(error),
    }
}

fn merge_mrf_entries_with_cap(
    entries: &mut Vec<MrfReplicateEntry>,
    additions: &[MrfReplicateEntry],
    cap: usize,
) -> Result<bool, EcstoreError> {
    let original_len = entries.len();
    let mut fingerprints = HashMap::<[u8; 32], Vec<usize>>::new();
    for (index, entry) in entries.iter().enumerate() {
        let encoded = encoded_mrf_entry_identity(entry)?;
        fingerprints.entry(Sha256::digest(encoded).into()).or_default().push(index);
    }
    for addition in additions {
        let encoded = encoded_mrf_entry_identity(addition)?;
        let fingerprint = Sha256::digest(&encoded).into();
        let duplicate = mrf_entry_has_stable_identity(addition)
            && fingerprints.get(&fingerprint).is_some_and(|indices| {
                indices
                    .iter()
                    .any(|index| encoded_mrf_entry_identity(&entries[*index]).is_ok_and(|existing| existing == encoded))
            });
        if duplicate {
            continue;
        }
        if entries.len() >= cap {
            entries.truncate(original_len);
            return Ok(false);
        }
        fingerprints.entry(fingerprint).or_default().push(entries.len());
        entries.push(addition.clone());
    }
    Ok(true)
}

async fn merge_mrf_entries_to_disk<S: ReplicationStorage>(
    file: &str,
    additions: &[MrfReplicateEntry],
    storage: &Arc<S>,
) -> Option<MrfMergeOutcome> {
    if file == ReplicationMetadataStore::TARGETED_MRF_REPLICATION_FILE && !additions.iter().all(targeted_mrf_entry_is_valid) {
        return None;
    }
    let started = Instant::now();
    let mrf_lock = match storage
        .new_ns_lock(ReplicationMetadataStore::rustfs_meta_bucket(), file)
        .await
    {
        Ok(lock) => lock,
        Err(error) => {
            observe_mrf_flush_failure(duration_millis_u64(started.elapsed()));
            warn!(
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_REPLICATION,
                error = %error,
                "Failed to create MRF persistence lock"
            );
            return None;
        }
    };
    let guard = match mrf_lock.get_write_lock(ReplicationLockTiming::acquire_timeout()).await {
        Ok(guard) => guard,
        Err(error) => {
            observe_mrf_flush_failure(duration_millis_u64(started.elapsed()));
            warn!(
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_REPLICATION,
                error = %error,
                "Failed to acquire MRF persistence lock"
            );
            return None;
        }
    };
    let contents = match read_mrf_entries_no_lock(file, storage).await {
        Ok(contents) => contents,
        Err(error) => {
            observe_mrf_flush_failure(duration_millis_u64(started.elapsed()));
            warn!(
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_REPLICATION,
                error = %error,
                "Failed to read MRF entries before merge"
            );
            return None;
        }
    };
    let mut entries = contents.entries;
    let dropped_existing = (entries.len() > MRF_PENDING_CAP).then(|| entries.split_off(MRF_PENDING_CAP));
    let additions_persisted = match merge_mrf_entries_with_cap(&mut entries, additions, MRF_PENDING_CAP) {
        Ok(persisted) => persisted,
        Err(_) => {
            observe_mrf_flush_failure(duration_millis_u64(started.elapsed()));
            return None;
        }
    };
    if !additions_persisted {
        for entry in additions {
            observe_mrf_drop(entry);
        }
        if dropped_existing.is_none() {
            drop(guard);
            return Some(MrfMergeOutcome {
                duration_millis: duration_millis_u64(started.elapsed()),
                additions_persisted: false,
            });
        }
    }
    let data = match encode_mrf_file(&entries) {
        Ok(data) => data,
        Err(error) => {
            observe_mrf_flush_failure(duration_millis_u64(started.elapsed()));
            warn!(
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_REPLICATION,
                error = %error,
                "Failed to encode merged MRF entries"
            );
            return None;
        }
    };
    if guard.is_lock_lost() {
        observe_mrf_flush_failure(duration_millis_u64(started.elapsed()));
        return None;
    }
    drop(guard);
    if let Err(error) =
        ReplicationConfigStore::save_conditional(storage.clone(), file, data, contents.expected_etag, Vec::new()).await
    {
        observe_mrf_flush_failure(duration_millis_u64(started.elapsed()));
        warn!(
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_REPLICATION,
            error = %error,
            "Failed to save merged MRF entries"
        );
        return None;
    }
    if let Some(dropped_existing) = dropped_existing {
        for entry in &dropped_existing {
            observe_mrf_drop(entry);
        }
    }
    Some(MrfMergeOutcome {
        duration_millis: duration_millis_u64(started.elapsed()),
        additions_persisted,
    })
}

async fn flush_pending_mrf_file<S: ReplicationStorage>(
    file: &str,
    pending: &mut PendingMrfFile,
    storage: &Arc<S>,
    stats: &ReplicationStats,
) -> bool {
    let Some(outcome) = merge_mrf_entries_to_disk(file, &pending.entries, storage).await else {
        return false;
    };
    observe_mrf_pending_flushed(&pending.entries, outcome.duration_millis);
    dec_mrf_entries(stats, &pending.entries);
    pending.entries.clear();
    pending.permits.clear();
    for persisted in pending.persisted.drain(..) {
        let _ = persisted.send(outcome.additions_persisted);
    }
    true
}

async fn refresh_durable_mrf_backlog_snapshot<S: ReplicationStorage>(storage: Arc<S>) {
    let backlog = read_durable_mrf_backlog(storage).await;
    if backlog.available {
        set_durable_mrf_backlog_snapshot(durable_mrf_backlog_summary_from_entries(&backlog.entries));
    }
}

fn duration_millis_u64(duration: std::time::Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

/// Load bucket resync metadata from disk
async fn load_bucket_resync_metadata<S: ReplicationObjectIO>(
    bucket: &str,
    obj_api: Arc<S>,
) -> Result<BucketReplicationResyncStatus, EcstoreError> {
    let mut brs = BucketReplicationResyncStatus::new();

    let resync_file_path = ReplicationMetadataStore::bucket_resync_file_path(bucket);

    let data = match ReplicationConfigStore::read(obj_api, &resync_file_path).await {
        Ok(data) => data,
        Err(EcstoreError::ConfigNotFound) => return Ok(brs),
        Err(err) => return Err(err),
    };

    if data.is_empty() {
        // Seems to be empty
        return Ok(brs);
    }

    brs = decode_resync_file(&data)?;

    Ok(brs)
}

// Define a trait object type for the replication pool
pub type DynReplicationPool = dyn ReplicationPoolTrait + Send + Sync;

/// Trait that abstracts the replication pool operations
#[async_trait::async_trait]
pub trait ReplicationPoolTrait: std::fmt::Debug {
    fn active_workers(&self) -> i32;
    fn active_mrf_workers(&self) -> i32;
    fn active_lrg_workers(&self) -> i32;
    async fn queue_replica_task(&self, ri: ReplicateObjectInfo) -> ReplicationQueueAdmission;
    async fn queue_replica_delete_task(&self, ri: DeletedObjectReplicationInfo) -> ReplicationQueueAdmission;
    async fn queue_replica_delete_batch(&self, deletes: &[DeletedObjectReplicationInfo]) -> ReplicationBatchAdmission;
    async fn queue_mrf_delete_task(&self, ri: DeletedObjectReplicationInfo) -> ReplicationQueueAdmission;
    async fn queue_mrf_delete_tasks(&self, tasks: Vec<DeletedObjectReplicationInfo>) -> ReplicationQueueAdmission;
    async fn start_mrf_processor(&self);
    async fn resize(&self, priority: ReplicationPriority, max_workers: usize, max_l_workers: usize);
    async fn get_bucket_resync_status(&self, bucket: &str) -> Result<BucketReplicationResyncStatus, EcstoreError>;
    async fn cancel_bucket_resync(&self, opts: ResyncOpts) -> Result<(), EcstoreError>;
    async fn admit_bucket_resync(self: Arc<Self>, opts: ResyncOpts) -> Result<bool, EcstoreError>;
    async fn activate_bucket_resync(self: Arc<Self>, opts: ResyncOpts, recovering: bool) -> Result<(), EcstoreError>;
    async fn start_bucket_resync(self: Arc<Self>, opts: ResyncOpts) -> Result<(), EcstoreError>;
    async fn init_resync(
        self: Arc<Self>,
        cancellation_token: CancellationToken,
        buckets: Vec<String>,
    ) -> Result<(), EcstoreError>;
}

// Implement the trait for ReplicationPool
#[async_trait::async_trait]
impl<S: ReplicationStorage> ReplicationPoolTrait for ReplicationPool<S> {
    fn active_workers(&self) -> i32 {
        ReplicationPool::<S>::active_workers(self)
    }

    fn active_mrf_workers(&self) -> i32 {
        ReplicationPool::<S>::active_mrf_workers(self)
    }

    fn active_lrg_workers(&self) -> i32 {
        ReplicationPool::<S>::active_lrg_workers(self)
    }

    async fn queue_replica_task(&self, ri: ReplicateObjectInfo) -> ReplicationQueueAdmission {
        self.queue_replica_task(ri).await
    }

    async fn queue_replica_delete_task(&self, ri: DeletedObjectReplicationInfo) -> ReplicationQueueAdmission {
        self.queue_replica_delete_task(ri).await
    }

    async fn queue_replica_delete_batch(&self, deletes: &[DeletedObjectReplicationInfo]) -> ReplicationBatchAdmission {
        self.queue_replica_delete_batch(deletes).await
    }

    async fn queue_mrf_delete_task(&self, ri: DeletedObjectReplicationInfo) -> ReplicationQueueAdmission {
        self.queue_mrf_delete_task(ri).await
    }

    async fn queue_mrf_delete_tasks(&self, tasks: Vec<DeletedObjectReplicationInfo>) -> ReplicationQueueAdmission {
        self.queue_mrf_delete_tasks(tasks).await
    }

    async fn start_mrf_processor(&self) {
        self.start_mrf_processor().await;
    }

    async fn resize(&self, priority: ReplicationPriority, max_workers: usize, max_l_workers: usize) {
        self.resize(priority, max_workers, max_l_workers).await;
    }

    async fn get_bucket_resync_status(&self, bucket: &str) -> Result<BucketReplicationResyncStatus, EcstoreError> {
        self.get_bucket_resync_status(bucket).await
    }

    async fn cancel_bucket_resync(&self, opts: ResyncOpts) -> Result<(), EcstoreError> {
        self.cancel_bucket_resync(opts).await
    }

    async fn admit_bucket_resync(self: Arc<Self>, opts: ResyncOpts) -> Result<bool, EcstoreError> {
        self.admit_bucket_resync(opts).await
    }

    async fn activate_bucket_resync(self: Arc<Self>, opts: ResyncOpts, recovering: bool) -> Result<(), EcstoreError> {
        self.activate_bucket_resync(opts, recovering).await
    }

    async fn start_bucket_resync(self: Arc<Self>, opts: ResyncOpts) -> Result<(), EcstoreError> {
        self.start_bucket_resync(opts).await
    }

    async fn init_resync(
        self: Arc<Self>,
        cancellation_token: CancellationToken,
        buckets: Vec<String>,
    ) -> Result<(), EcstoreError> {
        self.init_resync_internal(cancellation_token, buckets).await
    }
}

/// Initializes background replication with the given options.
///
/// Phase 5 (backlog#939): the replication stats/pool moved into the per-instance
/// `InstanceContext`; this owner initializes the current instance's cells
/// (lazily, once — single-instance behavior is unchanged).
pub async fn init_background_replication<S: ReplicationStorage>(storage: Arc<S>) {
    let ctx = crate::runtime::global::current_ctx();

    let stats = ctx
        .replication_stats_cell()
        .get_or_init(|| async {
            let stats = Arc::new(ReplicationStats::new());
            stats.start_background_tasks().await;
            stats
        })
        .await;

    let pool = ctx
        .replication_pool_cell()
        .get_or_init(|| async {
            let pool = ReplicationPool::new(ReplicationPoolOpts::default(), stats.clone(), storage).await;
            pool as Arc<DynReplicationPool>
        })
        .await;
    pool.start_mrf_processor().await;

    assert!(runtime_sources::replication_runtime_initialized());
}

pub fn get_global_replication_pool() -> Option<Arc<DynReplicationPool>> {
    runtime_sources::replication_pool()
}

pub fn get_global_replication_stats() -> Option<Arc<ReplicationStats>> {
    runtime_sources::replication_stats()
}

pub(crate) async fn schedule_replication<S: ReplicationStorage>(
    oi: ObjectInfo,
    o: Arc<S>,
    dsc: ReplicateDecision,
    op_type: ReplicationType,
) {
    let (synchronous, asynchronous) = dsc.partition_by_sync();
    let mut async_oi = oi;

    if synchronous.replicate_any() {
        let ri = replicate_object_info_from_object_info(async_oi.clone(), synchronous, op_type);
        let (state, _) = replicate_object(ri, o.clone()).await;
        async_oi.replication_status_internal = state.replication_status_internal;
        async_oi.version_purge_status_internal = state.version_purge_status_internal;
    }

    if asynchronous.replicate_any()
        && let Some(pool) = runtime_sources::replication_pool()
    {
        let ri = replicate_object_info_from_object_info(async_oi, asynchronous, op_type);
        let _ = pool.queue_replica_task(ri).await;
    }
}

fn replicate_object_info_from_object_info(
    oi: ObjectInfo,
    dsc: ReplicateDecision,
    op_type: ReplicationType,
) -> ReplicateObjectInfo {
    let tgt_statuses = replication_statuses_map(&oi.replication_status_internal.clone().unwrap_or_default());
    let purge_statuses = version_purge_statuses_map(&oi.version_purge_status_internal.clone().unwrap_or_default());
    let tm = get_str(&oi.user_defined, SUFFIX_REPLICATION_TIMESTAMP)
        .map(|v| OffsetDateTime::parse(&v, &Rfc3339).unwrap_or(OffsetDateTime::UNIX_EPOCH));
    let mut rstate = oi.replication_state();
    rstate.replicate_decision_str = dsc.to_string();
    let asz = oi.get_actual_size().unwrap_or_default();
    let ssec = replication_object_is_ssec_encrypted(&oi.user_defined);
    let checksum = if ssec { oi.checksum.clone() } else { None };

    ReplicateObjectInfo {
        name: oi.name,
        size: oi.size,
        actual_size: asz,
        bucket: oi.bucket,
        version_id: oi.version_id,
        etag: oi.etag,
        mod_time: oi.mod_time,
        replication_status: oi.replication_status,
        replication_status_internal: oi.replication_status_internal,
        delete_marker: oi.delete_marker,
        version_purge_status_internal: oi.version_purge_status_internal,
        version_purge_status: oi.version_purge_status,

        replication_state: Some(rstate),
        op_type,
        dsc,
        target_statuses: tgt_statuses,
        target_purge_statuses: purge_statuses,
        replication_timestamp: tm,
        user_tags: (*oi.user_tags).clone(),
        checksum,
        retry_count: 0,
        event_type: "".to_string(),
        existing_obj_resync: ResyncDecision::default(),
        ssec,
    }
}

pub(crate) async fn schedule_replication_delete(dv: DeletedObjectReplicationInfo) {
    if let Some(pool) = runtime_sources::replication_pool() {
        let _ = pool.queue_replica_delete_task(dv.clone()).await;
    }

    if let Some(stats) = runtime_sources::replication_stats() {
        let target_arns = dv.admitted_target_arns();
        if let Some(rs) = dv.delete_object.replication_state.as_ref() {
            for k in target_arns
                .iter()
                .filter(|target_arn| rs.targets.contains_key(*target_arn) || rs.purge_targets.contains_key(*target_arn))
            {
                let ri = ReplicatedTargetInfo {
                    arn: k.clone(),
                    size: 0,
                    duration: Duration::default(),
                    op_type: ReplicationType::Delete,
                    ..Default::default()
                };
                stats
                    .update(&dv.bucket, &ri, ReplicationStatusType::Pending, ReplicationStatusType::Empty)
                    .await;
            }
        }
    }
}

/// QueueReplicationHeal is a wrapper for queue_replication_heal_internal
pub async fn queue_replication_heal(bucket: &str, oi: ObjectInfo, retry_count: u32) {
    // ignore modtime zero objects
    if oi.mod_time.is_none() || oi.mod_time == Some(OffsetDateTime::UNIX_EPOCH) {
        return;
    }

    let rcfg = match ReplicationMetadataStore::replication_config(bucket).await {
        Ok((config, _)) => config,
        Err(err) => {
            debug!(
                event = EVENT_REPLICATION_CONFIG_LOOKUP_SKIPPED,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_REPLICATION,
                bucket,
                error = %err,
                reason = "config_lookup_failed",
                "Skipped replication heal queue due to missing replication config"
            );

            return;
        }
    };

    let tgts = match ReplicationTargetStore::list_bucket_targets(bucket).await {
        Ok(targets) => Some(targets),
        Err(err) => {
            debug!(
                event = EVENT_REPLICATION_CONFIG_LOOKUP_SKIPPED,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_REPLICATION,
                bucket,
                error = %err,
                reason = "target_list_failed",
                "Skipped bucket target list during replication heal queue setup"
            );
            None
        }
    };

    let rcfg_wrapper = ReplicationConfig::new(Some(rcfg), tgts);
    queue_replication_heal_internal(bucket, oi, rcfg_wrapper, retry_count).await;
}

pub async fn queue_replication_metadata(bucket: &str, oi: ObjectInfo, retry_count: u32) {
    let dsc = must_replicate(
        bucket,
        &oi.name,
        MustReplicateOptions::new(&oi.user_defined, (*oi.user_tags).clone(), ReplicationType::Metadata, false)
            .with_replication_status(oi.replication_status.clone()),
    )
    .await;

    if !dsc.replicate_any() {
        return;
    }

    let mut roi = replicate_object_info_from_object_info(oi, dsc, ReplicationType::Metadata);
    roi.retry_count = retry_count;
    if let Some(pool) = runtime_sources::replication_pool() {
        let _ = pool.queue_replica_task(roi).await;
    }
}

/// queue_replication_heal_internal enqueues objects that failed replication OR eligible for resyncing through
/// an ongoing resync operation or via existing objects replication configuration setting.
pub(crate) async fn queue_replication_heal_internal(
    _bucket: &str,
    oi: ObjectInfo,
    rcfg: ReplicationConfig,
    retry_count: u32,
) -> ReplicationHealQueueResult {
    let mut roi = ReplicateObjectInfo::default();

    // ignore modtime zero objects
    if oi.mod_time.is_none() || oi.mod_time == Some(OffsetDateTime::UNIX_EPOCH) {
        return ReplicationHealQueueResult {
            object_info: roi,
            admission: ReplicationQueueAdmission::Skipped,
        };
    }

    if rcfg.config.is_none() || rcfg.remotes.is_none() {
        return ReplicationHealQueueResult {
            object_info: roi,
            admission: ReplicationQueueAdmission::Skipped,
        };
    }

    roi = match get_heal_replicate_object_info(&oi, &rcfg).await {
        Ok(roi) => roi,
        Err(err) => {
            warn!(
                event = EVENT_REPLICATION_CONFIG_LOOKUP_SKIPPED,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_REPLICATION,
                bucket = %oi.bucket,
                object = %oi.name,
                error = %err,
                "Failed to classify object for replication heal"
            );
            return ReplicationHealQueueResult {
                object_info: roi,
                admission: ReplicationQueueAdmission::Missed,
            };
        }
    };
    roi.retry_count = retry_count;

    match replication_heal_queue_action(&mut roi) {
        ReplicationHealQueueAction::Skip => ReplicationHealQueueResult {
            object_info: roi,
            admission: ReplicationQueueAdmission::Skipped,
        },
        ReplicationHealQueueAction::QueueObject => {
            let admission = if let Some(pool) = runtime_sources::replication_pool() {
                pool.queue_replica_task(roi.clone()).await
            } else {
                ReplicationQueueAdmission::Missed
            };
            ReplicationHealQueueResult {
                object_info: roi,
                admission,
            }
        }
        ReplicationHealQueueAction::QueueDelete(dv) => {
            let admission = if let Some(pool) = runtime_sources::replication_pool() {
                pool.queue_replica_delete_task(dv).await
            } else {
                ReplicationQueueAdmission::Missed
            };
            ReplicationHealQueueResult {
                object_info: roi,
                admission,
            }
        }
        ReplicationHealQueueAction::QueueResyncDeletes(batch) => {
            let admission = queue_replicate_deletes(batch).await;
            ReplicationHealQueueResult {
                object_info: roi,
                admission,
            }
        }
    }
}

async fn queue_replicate_deletes(batch: ReplicationHealResyncDeletes) -> ReplicationQueueAdmission {
    let mut admission = ReplicationQueueAdmission::Skipped;
    for dv in batch.target_delete_infos() {
        let target_admission = if let Some(pool) = runtime_sources::replication_pool() {
            pool.queue_replica_delete_task(dv).await
        } else {
            ReplicationQueueAdmission::Missed
        };
        admission.merge(target_admission);
    }
    admission
}

#[cfg(test)]
mod tests {
    use super::super::replication_filemeta_boundary::ReplicateTargetDecision;
    use super::super::replication_resync_boundary::{decode_mrf_file, encode_mrf_file, encode_resync_file};
    use super::super::replication_resyncer::DeleteReplicationSourceCheckProbe;
    use super::super::replication_storage_boundary::{
        DeletedObject, FileInfo, GetObjectReader, HTTPRangeSpec, ListOperations, ObjectIO, ObjectOperations, PutObjReader,
        StorageListObjectVersionsInfo, StorageListObjectsV2Info, StorageNamespaceLocking, StorageObjectInfoOrErr, WalkOptions,
    };
    use super::*;
    use std::collections::{HashMap, HashSet};
    use std::fmt::{Debug, Formatter};
    use std::io::Cursor;
    use std::sync::Mutex as StdMutex;
    use std::sync::atomic::{AtomicBool, AtomicUsize};
    use tokio::io::AsyncReadExt;
    use tokio::sync::Notify;
    use uuid::Uuid;

    type TestListObjectsV2Info = StorageListObjectsV2Info<ObjectInfo>;
    type TestListObjectVersionsInfo = StorageListObjectVersionsInfo<ObjectInfo>;
    type TestObjectInfoOrErr = StorageObjectInfoOrErr<ObjectInfo, EcstoreError>;

    struct LoadResyncSharedState {
        data: StdMutex<Vec<u8>>,
        targeted_mrf_data: StdMutex<Vec<u8>>,
        lock_manager: Arc<rustfs_lock::GlobalLockManager>,
        first_read_started: Notify,
        delay_first_read: AtomicBool,
        read_count: AtomicUsize,
        write_count: AtomicUsize,
        fail_next_write: AtomicBool,
        block_next_write: AtomicBool,
        write_started: Notify,
        allow_write: Notify,
        object_info_count: AtomicUsize,
        delete_object_count: AtomicUsize,
        block_next_object_info: AtomicBool,
        object_info_started: Notify,
        allow_object_info: Notify,
    }

    struct LoadResyncNodeStore {
        owner: String,
        shared: Arc<LoadResyncSharedState>,
    }

    impl LoadResyncNodeStore {
        fn new(owner: &str, shared: Arc<LoadResyncSharedState>) -> Self {
            Self {
                owner: owner.to_string(),
                shared,
            }
        }
    }

    impl Debug for LoadResyncNodeStore {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("LoadResyncNodeStore").field("owner", &self.owner).finish()
        }
    }

    #[async_trait::async_trait]
    impl ObjectIO for LoadResyncNodeStore {
        type Error = EcstoreError;
        type RangeSpec = HTTPRangeSpec;
        type HeaderMap = http::HeaderMap;
        type ObjectOptions = ObjectOptions;
        type ObjectInfo = ObjectInfo;
        type GetObjectReader = GetObjectReader;
        type PutObjectReader = PutObjReader;

        async fn get_object_reader(
            &self,
            _bucket: &str,
            object: &str,
            _range: Option<Self::RangeSpec>,
            _h: Self::HeaderMap,
            _opts: &Self::ObjectOptions,
        ) -> Result<Self::GetObjectReader, Self::Error> {
            if !object.ends_with("/.replication/resync.bin")
                && !object.ends_with("config/replication/force-delete.bin")
                && object != ReplicationMetadataStore::MRF_REPLICATION_FILE
                && object != ReplicationMetadataStore::TARGETED_MRF_REPLICATION_FILE
            {
                return Err(EcstoreError::FileNotFound);
            }

            let read_index = self.shared.read_count.fetch_add(1, Ordering::SeqCst);
            if read_index == 0 && self.shared.delay_first_read.load(Ordering::SeqCst) {
                self.shared.first_read_started.notify_waiters();
                tokio::time::sleep(Duration::from_millis(1_500)).await;
            }

            let data = if object == ReplicationMetadataStore::TARGETED_MRF_REPLICATION_FILE {
                self.shared
                    .targeted_mrf_data
                    .lock()
                    .expect("test targeted MRF data lock should not be poisoned")
                    .clone()
            } else {
                self.shared
                    .data
                    .lock()
                    .expect("test data lock should not be poisoned")
                    .clone()
            };
            if data.is_empty() {
                return Err(EcstoreError::FileNotFound);
            }
            let size = i64::try_from(data.len()).expect("test metadata length should fit i64");
            let etag = hex_simd::encode_to_string(Sha256::digest(&data), hex_simd::AsciiCase::Lower);
            Ok(Self::GetObjectReader {
                stream: Box::new(Cursor::new(data)),
                object_info: ObjectInfo {
                    size,
                    actual_size: size,
                    etag: Some(etag),
                    ..Default::default()
                },
                buffered_body: None,
                body_source: Default::default(),
            })
        }

        async fn put_object(
            &self,
            _bucket: &str,
            object: &str,
            data: &mut Self::PutObjectReader,
            opts: &Self::ObjectOptions,
        ) -> Result<Self::ObjectInfo, Self::Error> {
            if self.shared.fail_next_write.swap(false, Ordering::SeqCst) {
                return Err(EcstoreError::Unexpected);
            }
            if self.shared.block_next_write.swap(false, Ordering::SeqCst) {
                self.shared.write_started.notify_one();
                self.shared.allow_write.notified().await;
            }
            let mut encoded = Vec::new();
            data.stream.read_to_end(&mut encoded).await.map_err(EcstoreError::from)?;
            let current = if object == ReplicationMetadataStore::TARGETED_MRF_REPLICATION_FILE {
                self.shared
                    .targeted_mrf_data
                    .lock()
                    .expect("test targeted MRF data lock should not be poisoned")
                    .clone()
            } else {
                self.shared
                    .data
                    .lock()
                    .expect("test data lock should not be poisoned")
                    .clone()
            };
            let current_etag =
                (!current.is_empty()).then(|| hex_simd::encode_to_string(Sha256::digest(&current), hex_simd::AsciiCase::Lower));
            if let Some(preconditions) = &opts.http_preconditions
                && (preconditions.if_match_value() != current_etag.as_deref() && preconditions.if_match_value().is_some()
                    || (current_etag.is_some() && preconditions.if_none_match_value() == Some("*")))
            {
                return Err(EcstoreError::Unexpected);
            }
            if object == ReplicationMetadataStore::TARGETED_MRF_REPLICATION_FILE {
                *self
                    .shared
                    .targeted_mrf_data
                    .lock()
                    .expect("test targeted MRF data lock should not be poisoned") = encoded;
            } else {
                *self.shared.data.lock().expect("test data lock should not be poisoned") = encoded;
            }
            self.shared.write_count.fetch_add(1, Ordering::SeqCst);
            Ok(ObjectInfo::default())
        }
    }

    #[async_trait::async_trait]
    impl ObjectOperations for LoadResyncNodeStore {
        type Error = EcstoreError;
        type ObjectInfo = ObjectInfo;
        type ObjectOptions = ObjectOptions;
        type FileInfo = FileInfo;
        type ObjectToDelete = ObjectToDelete;
        type DeletedObject = DeletedObject;

        async fn get_object_info(
            &self,
            _bucket: &str,
            object: &str,
            _opts: &Self::ObjectOptions,
        ) -> Result<Self::ObjectInfo, Self::Error> {
            self.shared.object_info_count.fetch_add(1, Ordering::SeqCst);
            if object == "panic" {
                panic!("injected MRF replay panic");
            }
            if self.shared.block_next_object_info.swap(false, Ordering::SeqCst) {
                self.shared.object_info_started.notify_one();
                self.shared.allow_object_info.notified().await;
            }
            if object == "terminal" {
                return Err(EcstoreError::FileNotFound);
            }
            Err(EcstoreError::NotImplemented)
        }

        async fn verify_object_integrity(
            &self,
            _bucket: &str,
            _object: &str,
            _opts: &Self::ObjectOptions,
        ) -> Result<(), Self::Error> {
            Err(EcstoreError::NotImplemented)
        }

        async fn copy_object(
            &self,
            _src_bucket: &str,
            _src_object: &str,
            _dst_bucket: &str,
            _dst_object: &str,
            _src_info: &mut Self::ObjectInfo,
            _src_opts: &Self::ObjectOptions,
            _dst_opts: &Self::ObjectOptions,
        ) -> Result<Self::ObjectInfo, Self::Error> {
            Err(EcstoreError::NotImplemented)
        }

        async fn delete_object_version(
            &self,
            _bucket: &str,
            _object: &str,
            _fi: &Self::FileInfo,
            _force_del_marker: bool,
        ) -> Result<(), Self::Error> {
            Err(EcstoreError::NotImplemented)
        }

        async fn delete_object(
            &self,
            _bucket: &str,
            _object: &str,
            _opts: Self::ObjectOptions,
        ) -> Result<Self::ObjectInfo, Self::Error> {
            self.shared.delete_object_count.fetch_add(1, Ordering::SeqCst);
            Err(EcstoreError::NotImplemented)
        }

        async fn delete_objects(
            &self,
            _bucket: &str,
            _objects: Vec<Self::ObjectToDelete>,
            _opts: Self::ObjectOptions,
        ) -> (Vec<Self::DeletedObject>, Vec<Option<Self::Error>>) {
            (Vec::new(), vec![Some(EcstoreError::NotImplemented)])
        }

        async fn put_object_metadata(
            &self,
            _bucket: &str,
            _object: &str,
            _opts: &Self::ObjectOptions,
        ) -> Result<Self::ObjectInfo, Self::Error> {
            Err(EcstoreError::NotImplemented)
        }

        async fn get_object_tags(
            &self,
            _bucket: &str,
            _object: &str,
            _opts: &Self::ObjectOptions,
        ) -> Result<String, Self::Error> {
            Err(EcstoreError::NotImplemented)
        }

        async fn put_object_tags(
            &self,
            _bucket: &str,
            _object: &str,
            _tags: &str,
            _opts: &Self::ObjectOptions,
        ) -> Result<Self::ObjectInfo, Self::Error> {
            Err(EcstoreError::NotImplemented)
        }

        async fn delete_object_tags(
            &self,
            _bucket: &str,
            _object: &str,
            _opts: &Self::ObjectOptions,
        ) -> Result<Self::ObjectInfo, Self::Error> {
            Err(EcstoreError::NotImplemented)
        }

        async fn add_partial(&self, _bucket: &str, _object: &str, _version_id: &str) -> Result<(), Self::Error> {
            Err(EcstoreError::NotImplemented)
        }

        async fn transition_object(&self, _bucket: &str, _object: &str, _opts: &Self::ObjectOptions) -> Result<(), Self::Error> {
            Err(EcstoreError::NotImplemented)
        }

        async fn restore_transitioned_object(
            self: Arc<Self>,
            _bucket: &str,
            _object: &str,
            _opts: &Self::ObjectOptions,
        ) -> Result<(), Self::Error> {
            Err(EcstoreError::NotImplemented)
        }
    }

    #[async_trait::async_trait]
    impl ListOperations for LoadResyncNodeStore {
        type Error = EcstoreError;
        type ListObjectsV2Info = TestListObjectsV2Info;
        type ListObjectVersionsInfo = TestListObjectVersionsInfo;
        type ObjectInfoOrErr = TestObjectInfoOrErr;
        type WalkOptions = WalkOptions;
        type WalkCancellation = CancellationToken;
        type WalkResultSender = Sender<TestObjectInfoOrErr>;

        async fn list_objects_v2(
            self: Arc<Self>,
            _bucket: &str,
            _prefix: &str,
            _continuation_token: Option<String>,
            _delimiter: Option<String>,
            _max_keys: i32,
            _fetch_owner: bool,
            _start_after: Option<String>,
            _incl_deleted: bool,
        ) -> Result<Self::ListObjectsV2Info, Self::Error> {
            Err(EcstoreError::NotImplemented)
        }

        async fn list_object_versions(
            self: Arc<Self>,
            _bucket: &str,
            _prefix: &str,
            _marker: Option<String>,
            _version_marker: Option<String>,
            _delimiter: Option<String>,
            _max_keys: i32,
        ) -> Result<Self::ListObjectVersionsInfo, Self::Error> {
            Err(EcstoreError::NotImplemented)
        }

        async fn walk(
            self: Arc<Self>,
            _rx: Self::WalkCancellation,
            _bucket: &str,
            _prefix: &str,
            _result: Self::WalkResultSender,
            _opts: Self::WalkOptions,
        ) -> Result<(), Self::Error> {
            Ok(())
        }
    }

    #[async_trait::async_trait]
    impl StorageNamespaceLocking for LoadResyncNodeStore {
        type Error = EcstoreError;
        type NamespaceLock = rustfs_lock::NamespaceLockWrapper;

        async fn new_ns_lock(&self, bucket: &str, object: &str) -> Result<Self::NamespaceLock, Self::Error> {
            let lock =
                rustfs_lock::NamespaceLock::with_local_manager("load-resync-test".to_string(), self.shared.lock_manager.clone());
            Ok(rustfs_lock::NamespaceLockWrapper::new(
                lock,
                rustfs_lock::ObjectKey::new(bucket.to_string(), object.to_string()),
                self.owner.clone(),
            ))
        }
    }

    async fn new_test_replication_pool(storage: Arc<LoadResyncNodeStore>) -> Arc<ReplicationPool<LoadResyncNodeStore>> {
        let (mrf_replica_tx, mrf_replica_rx) = mpsc::channel(1);
        let (mrf_save_tx, mrf_save_rx) = mpsc::channel(1);
        let (mrf_worker_kill_tx, _) = mpsc::channel(1);
        let (mrf_stop_tx, _) = mpsc::channel(1);

        Arc::new(ReplicationPool {
            active_workers: Arc::new(AtomicI32::new(0)),
            active_lrg_workers: Arc::new(AtomicI32::new(0)),
            active_mrf_workers: Arc::new(AtomicI32::new(0)),
            storage,
            priority: RwLock::new(ReplicationPoolOpts::default().priority),
            max_workers: RwLock::new(WORKER_MAX_LIMIT),
            max_l_workers: RwLock::new(LARGE_WORKER_COUNT),
            stats: Arc::new(ReplicationStats::new()),
            workers: RwLock::new(Vec::new()),
            lrg_workers: RwLock::new(Vec::new()),
            mrf_replica_tx,
            mrf_replica_rx: Arc::new(Mutex::new(mrf_replica_rx)),
            mrf_save_tx,
            mrf_save_rx: Mutex::new(Some(mrf_save_rx)),
            mrf_save_permits: Arc::new(Semaphore::new(MRF_PENDING_CAP)),
            mrf_enqueued_order: AtomicU64::new(0),
            mrf_worker_kill_tx,
            mrf_stop_tx,
            mrf_worker_size: AtomicI32::new(0),
            task_handles: Mutex::new(Vec::new()),
            mrf_processor_started: AtomicBool::new(false),
            resyncer: Arc::new(ReplicationResyncer::new().await),
        })
    }

    async fn current_queue(pool: &ReplicationPool<LoadResyncNodeStore>, bucket: &str) -> (i64, i64) {
        let stats = pool.stats.get_latest_replication_stats(bucket).await;
        (stats.replication_stats.q_stat.curr.count, stats.replication_stats.q_stat.curr.bytes)
    }

    fn current_target_queue(pool: &ReplicationPool<LoadResyncNodeStore>, bucket: &str, target_arn: &str) -> Option<(u64, u64)> {
        pool.stats
            .runtime_target_backlog_snapshot()
            .into_iter()
            .find(|target| target.bucket == bucket && target.target_arn == target_arn)
            .map(|target| (target.count, target.bytes))
    }

    fn test_replicate_decision(target_arns: &[&str]) -> ReplicateDecision {
        let mut decision = ReplicateDecision::default();
        for target_arn in target_arns {
            decision.set(ReplicateTargetDecision::new((*target_arn).to_string(), true, false));
        }
        decision
    }

    async fn wait_for_current_queue(pool: &ReplicationPool<LoadResyncNodeStore>, bucket: &str, expected: (i64, i64)) {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if current_queue(pool, bucket).await == expected {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("replication queue should reach the expected state");
    }

    #[tokio::test]
    async fn regular_worker_admission_counts_channel_backlog_before_receive() {
        let pool = new_test_replication_pool(Arc::new(LoadResyncNodeStore::new("node-a", empty_resync_shared_state()))).await;
        let (tx, _rx) = mpsc::channel(1);
        pool.workers.write().await.push(tx);

        let admission = pool
            .queue_replica_task(ReplicateObjectInfo {
                bucket: "admission-bucket".to_string(),
                name: "object".to_string(),
                size: 4096,
                op_type: ReplicationType::Object,
                ..Default::default()
            })
            .await;

        assert_eq!(admission, ReplicationQueueAdmission::Queued);
        assert_eq!(current_queue(&pool, "admission-bucket").await, (1, 4096));
    }

    #[tokio::test]
    async fn regular_worker_admission_counts_target_backlog_before_receive() {
        let pool = new_test_replication_pool(Arc::new(LoadResyncNodeStore::new("node-a", empty_resync_shared_state()))).await;
        let (tx, _rx) = mpsc::channel(1);
        pool.workers.write().await.push(tx);

        let admission = pool
            .queue_replica_task(ReplicateObjectInfo {
                bucket: "target-admission-bucket".to_string(),
                name: "object".to_string(),
                size: 4096,
                op_type: ReplicationType::Object,
                dsc: test_replicate_decision(&["arn:rustfs:replication:target-b", "arn:rustfs:replication:target-a"]),
                ..Default::default()
            })
            .await;

        assert_eq!(admission, ReplicationQueueAdmission::Queued);
        assert_eq!(current_queue(&pool, "target-admission-bucket").await, (1, 4096));
        assert_eq!(
            current_target_queue(&pool, "target-admission-bucket", "arn:rustfs:replication:target-a"),
            Some((1, 4096))
        );
        assert_eq!(
            current_target_queue(&pool, "target-admission-bucket", "arn:rustfs:replication:target-b"),
            Some((1, 4096))
        );
    }

    #[tokio::test]
    async fn large_worker_admission_counts_channel_backlog_before_receive() {
        let pool = new_test_replication_pool(Arc::new(LoadResyncNodeStore::new("node-a", empty_resync_shared_state()))).await;
        let (tx, _rx) = mpsc::channel(1);
        pool.lrg_workers.write().await.push(tx);
        let size = 128 * 1024 * 1024;

        let admission = pool
            .queue_replica_task(ReplicateObjectInfo {
                bucket: "large-admission-bucket".to_string(),
                name: "large-object".to_string(),
                size,
                op_type: ReplicationType::Object,
                ..Default::default()
            })
            .await;

        assert_eq!(admission, ReplicationQueueAdmission::Queued);
        assert_eq!(current_queue(&pool, "large-admission-bucket").await, (1, size));
    }

    #[tokio::test]
    async fn delete_admission_counts_channel_backlog_before_receive() {
        let pool = new_test_replication_pool(Arc::new(LoadResyncNodeStore::new("node-a", empty_resync_shared_state()))).await;
        let (tx, _rx) = mpsc::channel(1);
        pool.workers.write().await.push(tx);

        let admission = pool
            .queue_replica_delete_task(DeletedObjectReplicationInfo {
                bucket: "delete-admission-bucket".to_string(),
                delete_object: ReplicationDeletedObject {
                    object_name: "deleted-object".to_string(),
                    ..Default::default()
                },
                op_type: ReplicationType::Delete,
                ..Default::default()
            })
            .await;

        assert_eq!(admission, ReplicationQueueAdmission::Queued);
        assert_eq!(current_queue(&pool, "delete-admission-bucket").await, (1, 0));
    }

    #[tokio::test]
    async fn force_delete_backpressures_in_memory_instead_of_losing_prefix_semantics_to_mrf() {
        let shared = empty_resync_shared_state();
        let pool = new_test_replication_pool(Arc::new(LoadResyncNodeStore::new("node-a", shared.clone()))).await;
        let (tx, mut rx) = mpsc::channel(1);
        tx.try_send(ReplicationOperation::Delete(Box::new(DeletedObjectReplicationInfo {
            bucket: "source".to_string(),
            delete_object: ReplicationDeletedObject {
                object_name: "already-buffered".to_string(),
                ..Default::default()
            },
            ..Default::default()
        })))
        .expect("test setup should fill the delete worker channel");
        pool.workers.write().await.push(tx);

        let queued_pool = pool.clone();
        let mut admission = tokio::spawn(async move {
            queued_pool
                .queue_replica_delete_task(DeletedObjectReplicationInfo {
                    bucket: "source".to_string(),
                    delete_object: ReplicationDeletedObject {
                        object_name: "prefix/".to_string(),
                        force_delete: true,
                        ..Default::default()
                    },
                    ..Default::default()
                })
                .await
        });

        assert!(
            tokio::time::timeout(Duration::from_millis(25), &mut admission).await.is_err(),
            "force delete must wait for worker capacity instead of being serialized as a normal delete"
        );
        let _ = rx.recv().await.expect("buffered delete should be released first");
        assert_eq!(
            admission.await.expect("force-delete admission task should finish"),
            ReplicationQueueAdmission::Queued
        );
        let ReplicationOperation::Delete(queued) = rx.recv().await.expect("force delete should reach the worker") else {
            panic!("force delete must remain a delete operation");
        };
        assert!(queued.delete_object.force_delete);
        assert!(shared.data.lock().expect("legacy MRF lock should not poison").is_empty());
    }

    #[tokio::test]
    async fn delete_admission_counts_target_backlog_before_receive() {
        let pool = new_test_replication_pool(Arc::new(LoadResyncNodeStore::new("node-a", empty_resync_shared_state()))).await;
        let (tx, _rx) = mpsc::channel(1);
        pool.workers.write().await.push(tx);

        let admission = pool
            .queue_replica_delete_task(DeletedObjectReplicationInfo {
                bucket: "delete-target-admission-bucket".to_string(),
                target_arn: "arn:rustfs:replication:target-a".to_string(),
                delete_object: ReplicationDeletedObject {
                    object_name: "deleted-object".to_string(),
                    ..Default::default()
                },
                op_type: ReplicationType::Delete,
                ..Default::default()
            })
            .await;

        assert_eq!(admission, ReplicationQueueAdmission::Queued);
        assert_eq!(current_queue(&pool, "delete-target-admission-bucket").await, (1, 0));
        assert_eq!(
            current_target_queue(&pool, "delete-target-admission-bucket", "arn:rustfs:replication:target-a"),
            Some((1, 0))
        );
    }

    #[tokio::test]
    async fn regular_worker_drains_current_backlog_after_processing() {
        let pool = new_test_replication_pool(Arc::new(LoadResyncNodeStore::new("node-a", empty_resync_shared_state()))).await;
        pool.resize_workers(1, 0).await;

        let admission = pool
            .queue_replica_task(ReplicateObjectInfo {
                bucket: "regular-drain-bucket".to_string(),
                name: "object".to_string(),
                size: 4096,
                op_type: ReplicationType::Object,
                ..Default::default()
            })
            .await;

        assert_eq!(admission, ReplicationQueueAdmission::Queued);
        wait_for_current_queue(&pool, "regular-drain-bucket", (0, 0)).await;
    }

    #[tokio::test]
    async fn large_worker_drains_current_backlog_after_processing() {
        let pool = new_test_replication_pool(Arc::new(LoadResyncNodeStore::new("node-a", empty_resync_shared_state()))).await;
        pool.resize_lrg_workers(1, 0).await;
        let size = 128 * 1024 * 1024;

        let admission = pool
            .queue_replica_task(ReplicateObjectInfo {
                bucket: "large-drain-bucket".to_string(),
                name: "large-object".to_string(),
                size,
                op_type: ReplicationType::Object,
                ..Default::default()
            })
            .await;

        assert_eq!(admission, ReplicationQueueAdmission::Queued);
        wait_for_current_queue(&pool, "large-drain-bucket", (0, 0)).await;
    }

    #[tokio::test]
    async fn regular_delete_worker_drains_current_backlog_after_processing() {
        let pool = new_test_replication_pool(Arc::new(LoadResyncNodeStore::new("node-a", empty_resync_shared_state()))).await;
        pool.resize_workers(1, 0).await;

        let admission = pool
            .queue_replica_delete_task(DeletedObjectReplicationInfo {
                bucket: "delete-drain-bucket".to_string(),
                delete_object: ReplicationDeletedObject {
                    object_name: "deleted-object".to_string(),
                    ..Default::default()
                },
                op_type: ReplicationType::Delete,
                ..Default::default()
            })
            .await;

        assert_eq!(admission, ReplicationQueueAdmission::Queued);
        wait_for_current_queue(&pool, "delete-drain-bucket", (0, 0)).await;
    }

    fn load_resync_test_metadata() -> Vec<u8> {
        let mut status = BucketReplicationResyncStatus::new();
        status.targets_map.insert(
            "arn:test".to_string(),
            TargetReplicationResyncStatus {
                bucket: "load-resync-lock".to_string(),
                resync_status: ResyncStatusType::ResyncCompleted,
                ..Default::default()
            },
        );
        encode_resync_file(&status).expect("test resync metadata should encode")
    }

    fn empty_resync_shared_state() -> Arc<LoadResyncSharedState> {
        Arc::new(LoadResyncSharedState {
            data: StdMutex::new(Vec::new()),
            targeted_mrf_data: StdMutex::new(Vec::new()),
            lock_manager: Arc::new(rustfs_lock::GlobalLockManager::new()),
            first_read_started: Notify::new(),
            delay_first_read: AtomicBool::new(false),
            read_count: AtomicUsize::new(0),
            write_count: AtomicUsize::new(0),
            fail_next_write: AtomicBool::new(false),
            block_next_write: AtomicBool::new(false),
            write_started: Notify::new(),
            allow_write: Notify::new(),
            object_info_count: AtomicUsize::new(0),
            delete_object_count: AtomicUsize::new(0),
            block_next_object_info: AtomicBool::new(false),
            object_info_started: Notify::new(),
            allow_object_info: Notify::new(),
        })
    }

    async fn hold_resync_runtime_lock(
        shared: &Arc<LoadResyncSharedState>,
        bucket: &str,
        arn: &str,
    ) -> rustfs_lock::NamespaceLockGuard {
        let lock =
            rustfs_lock::NamespaceLock::with_local_manager("resync-start-blocker".to_string(), shared.lock_manager.clone());
        let lock = rustfs_lock::NamespaceLockWrapper::new(
            lock,
            rustfs_lock::ObjectKey::new(
                ReplicationMetadataStore::rustfs_meta_bucket().to_string(),
                ReplicationMetadataStore::resync_lock_key(bucket, arn),
            ),
            "blocker".to_string(),
        );
        lock.get_write_lock(Duration::from_secs(1))
            .await
            .expect("test should hold the runtime resync lock")
    }

    fn test_resync_opts(bucket: &str, arn: &str, id: &str) -> ResyncOpts {
        ResyncOpts {
            bucket: bucket.to_string(),
            arn: arn.to_string(),
            resync_id: id.to_string(),
            resync_before: Some(OffsetDateTime::UNIX_EPOCH),
        }
    }

    #[tokio::test]
    async fn concurrent_resync_starts_accept_one_id_and_reject_the_other() {
        let shared = empty_resync_shared_state();
        let first_pool = new_test_replication_pool(Arc::new(LoadResyncNodeStore::new("node-a", shared.clone()))).await;
        let second_pool = new_test_replication_pool(Arc::new(LoadResyncNodeStore::new("node-b", shared.clone()))).await;
        let _runtime_guard = hold_resync_runtime_lock(&shared, "atomic-start", "arn:test").await;

        let first = first_pool
            .clone()
            .start_bucket_resync(test_resync_opts("atomic-start", "arn:test", "run-a"));
        let second = second_pool
            .clone()
            .start_bucket_resync(test_resync_opts("atomic-start", "arn:test", "run-b"));
        let (first, second) = tokio::join!(first, second);

        let (accepted_id, conflict) = match (first, second) {
            (Ok(()), Err(conflict)) => ("run-a", conflict),
            (Err(conflict), Ok(())) => ("run-b", conflict),
            outcome => panic!("exactly one concurrent start should be accepted: {outcome:?}"),
        };
        assert_eq!(resync_start_conflict_id(&conflict), Some(accepted_id));

        let persisted = decode_resync_file(&shared.data.lock().expect("test data lock should not be poisoned"))
            .expect("accepted status should be persisted");
        assert_eq!(persisted.targets_map["arn:test"].resync_id, accepted_id);
        assert_eq!(persisted.targets_map["arn:test"].resync_status, ResyncStatusType::ResyncPending);
        assert_eq!(
            first_pool.resyncer.status_map.read().await["atomic-start"].targets_map["arn:test"].resync_id,
            accepted_id
        );
        assert_eq!(
            second_pool.resyncer.status_map.read().await["atomic-start"].targets_map["arn:test"].resync_id,
            accepted_id
        );
    }

    #[tokio::test]
    async fn same_resync_id_retry_is_idempotent_without_rewriting_status() {
        let shared = empty_resync_shared_state();
        let pool = new_test_replication_pool(Arc::new(LoadResyncNodeStore::new("node-a", shared.clone()))).await;
        let _runtime_guard = hold_resync_runtime_lock(&shared, "same-id", "arn:test").await;
        let opts = test_resync_opts("same-id", "arn:test", "run-a");

        pool.clone()
            .start_bucket_resync(opts.clone())
            .await
            .expect("first start should be accepted");
        let first_status = pool
            .resyncer
            .status_map
            .read()
            .await
            .get("same-id")
            .expect("accepted status should be published")
            .targets_map["arn:test"]
            .clone();

        pool.clone()
            .start_bucket_resync(opts)
            .await
            .expect("same ID retry should be accepted idempotently");
        let retried_status = pool
            .resyncer
            .status_map
            .read()
            .await
            .get("same-id")
            .expect("retried status should remain published")
            .targets_map["arn:test"]
            .clone();

        assert_eq!(shared.write_count.load(Ordering::SeqCst), 1);
        assert_eq!(retried_status.resync_id, first_status.resync_id);
        assert_eq!(retried_status.start_time, first_status.start_time);
        assert_eq!(retried_status.resync_status, ResyncStatusType::ResyncPending);
        assert_eq!(pool.resyncer.cancel_tokens.read().await.len(), 1);
    }

    #[tokio::test]
    async fn admitted_resync_waits_for_target_metadata_commit_before_activation() {
        let shared = empty_resync_shared_state();
        let pool = new_test_replication_pool(Arc::new(LoadResyncNodeStore::new("node-a", shared.clone()))).await;
        let _runtime_guard = hold_resync_runtime_lock(&shared, "two-phase-start", "arn:test").await;
        let opts = test_resync_opts("two-phase-start", "arn:test", "run-a");

        let new_run = pool
            .clone()
            .admit_bucket_resync(opts.clone())
            .await
            .expect("admission should persist the intent");
        assert!(new_run);
        assert!(pool.resyncer.cancel_tokens.read().await.is_empty());
        assert_eq!(shared.write_count.load(Ordering::SeqCst), 1);

        pool.clone()
            .activate_bucket_resync(opts, false)
            .await
            .expect("activation should start the admitted run");
        assert_eq!(pool.resyncer.cancel_tokens.read().await.len(), 1);
    }

    #[tokio::test]
    async fn same_id_retry_after_restart_recreates_missing_runtime_task() {
        let shared = empty_resync_shared_state();
        let mut persisted = BucketReplicationResyncStatus::new();
        persisted.targets_map.insert(
            "arn:test".to_string(),
            TargetReplicationResyncStatus {
                bucket: "restart-retry".to_string(),
                resync_id: "run-a".to_string(),
                resync_status: ResyncStatusType::ResyncPending,
                ..Default::default()
            },
        );
        *shared.data.lock().expect("test data lock should not be poisoned") =
            encode_resync_file(&persisted).expect("restart status should encode");
        let pool = new_test_replication_pool(Arc::new(LoadResyncNodeStore::new("node-a", shared.clone()))).await;
        let _runtime_guard = hold_resync_runtime_lock(&shared, "restart-retry", "arn:test").await;

        pool.clone()
            .start_bucket_resync(test_resync_opts("restart-retry", "arn:test", "run-a"))
            .await
            .expect("same ID retry should recover an accepted run");

        assert_eq!(shared.write_count.load(Ordering::SeqCst), 0);
        assert_eq!(pool.resyncer.cancel_tokens.read().await.len(), 1);
        assert_eq!(
            pool.resyncer.status_map.read().await["restart-retry"].targets_map["arn:test"].resync_id,
            "run-a"
        );
    }

    #[tokio::test]
    async fn same_completed_resync_id_retry_does_not_restart_work() {
        let shared = empty_resync_shared_state();
        let mut persisted = BucketReplicationResyncStatus::new();
        persisted.targets_map.insert(
            "arn:test".to_string(),
            TargetReplicationResyncStatus {
                bucket: "completed-retry".to_string(),
                resync_id: "run-a".to_string(),
                resync_status: ResyncStatusType::ResyncCompleted,
                ..Default::default()
            },
        );
        *shared.data.lock().expect("test data lock should not be poisoned") =
            encode_resync_file(&persisted).expect("completed status should encode");
        let pool = new_test_replication_pool(Arc::new(LoadResyncNodeStore::new("node-a", shared.clone()))).await;

        pool.clone()
            .start_bucket_resync(test_resync_opts("completed-retry", "arn:test", "run-a"))
            .await
            .expect("completed same ID retry should remain idempotent");

        assert_eq!(shared.write_count.load(Ordering::SeqCst), 0);
        assert!(pool.resyncer.cancel_tokens.read().await.is_empty());
        assert_eq!(
            pool.resyncer.status_map.read().await["completed-retry"].targets_map["arn:test"].resync_status,
            ResyncStatusType::ResyncCompleted
        );
    }

    #[tokio::test]
    async fn start_failure_does_not_publish_or_persist_requested_id() {
        let shared = empty_resync_shared_state();
        shared.fail_next_write.store(true, Ordering::SeqCst);
        let pool = new_test_replication_pool(Arc::new(LoadResyncNodeStore::new("node-a", shared.clone()))).await;

        let error = pool
            .clone()
            .start_bucket_resync(test_resync_opts("failed-start", "arn:test", "run-a"))
            .await
            .expect_err("metadata save failure should reject the start");

        assert!(matches!(error, EcstoreError::Unexpected));
        assert!(shared.data.lock().expect("test data lock should not be poisoned").is_empty());
        assert!(!pool.resyncer.status_map.read().await.contains_key("failed-start"));
        assert_eq!(shared.write_count.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn canceled_start_request_finishes_accepted_transaction() {
        let shared = empty_resync_shared_state();
        shared.block_next_write.store(true, Ordering::SeqCst);
        let pool = new_test_replication_pool(Arc::new(LoadResyncNodeStore::new("node-a", shared.clone()))).await;
        let _runtime_guard = hold_resync_runtime_lock(&shared, "canceled-start", "arn:test").await;

        let start_pool = pool.clone();
        let start = tokio::spawn(async move {
            start_pool
                .start_bucket_resync(test_resync_opts("canceled-start", "arn:test", "run-a"))
                .await
        });
        tokio::time::timeout(Duration::from_secs(10), shared.write_started.notified())
            .await
            .expect("start transaction should reach the durable write");
        start.abort();
        assert!(start.await.expect_err("caller task should be canceled").is_cancelled());
        shared.allow_write.notify_one();

        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if pool.resyncer.status_map.read().await.contains_key("canceled-start") {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("detached admission transaction should finish after caller cancellation");
        assert_eq!(shared.write_count.load(Ordering::SeqCst), 1);
        assert_eq!(
            pool.resyncer.status_map.read().await["canceled-start"].targets_map["arn:test"].resync_id,
            "run-a"
        );
    }

    #[test]
    fn replication_queue_admission_combines_target_results() {
        let mut admission = ReplicationQueueAdmission::Skipped;

        admission.merge(ReplicationQueueAdmission::Queued);
        assert_eq!(admission, ReplicationQueueAdmission::Queued);

        admission.merge(ReplicationQueueAdmission::Missed);
        assert_eq!(admission, ReplicationQueueAdmission::Missed);
    }

    #[tokio::test]
    async fn heal_queue_marks_missing_versioning_state_as_missed() {
        use super::super::replication_target_boundary::BucketTargets;
        use s3s::dto::{
            DeleteReplication, DeleteReplicationStatus, Destination, ReplicationConfiguration, ReplicationRule,
            ReplicationRuleStatus,
        };

        let arn = "arn:rustfs:replication:us-east-1:target:bucket";
        let bucket = format!("missing-versioning-state-{}", Uuid::new_v4());
        let result = queue_replication_heal_internal(
            &bucket,
            ObjectInfo {
                bucket: bucket.clone(),
                name: "object".to_string(),
                version_id: Some(Uuid::new_v4()),
                version_purge_status: super::super::replication_filemeta_boundary::VersionPurgeStatusType::Pending,
                mod_time: Some(OffsetDateTime::now_utc()),
                ..Default::default()
            },
            ReplicationConfig::new(
                Some(ReplicationConfiguration {
                    role: String::new(),
                    rules: vec![ReplicationRule {
                        delete_marker_replication: None,
                        delete_replication: Some(DeleteReplication {
                            status: DeleteReplicationStatus::from_static(DeleteReplicationStatus::ENABLED),
                        }),
                        destination: Destination {
                            bucket: arn.to_string(),
                            ..Default::default()
                        },
                        existing_object_replication: None,
                        filter: None,
                        id: Some("delete".to_string()),
                        prefix: Some(String::new()),
                        priority: Some(1),
                        source_selection_criteria: None,
                        status: ReplicationRuleStatus::from_static(ReplicationRuleStatus::ENABLED),
                    }],
                }),
                Some(BucketTargets::default()),
            ),
            0,
        )
        .await;

        assert_eq!(result.admission, ReplicationQueueAdmission::Missed);
    }

    #[tokio::test]
    async fn queue_replica_task_waits_for_durable_mrf_when_worker_queue_is_full() {
        temp_env::async_with_vars([("RUSTFS_REPL_MRF_FLUSH_INTERVAL_MS", Some("10"))], async {
            let shared = empty_resync_shared_state();
            let pool = new_test_replication_pool(Arc::new(LoadResyncNodeStore::new("node-a", shared.clone()))).await;
            pool.start_mrf_persister().await;
            let (tx, _rx) = mpsc::channel(1);
            tx.try_send(ReplicationOperation::Object(Box::new(ReplicateObjectInfo {
                bucket: "runtime-backlog".to_string(),
                name: "already-buffered".to_string(),
                size: 1,
                op_type: ReplicationType::Object,
                ..Default::default()
            })))
            .expect("test setup should fill the worker queue");
            pool.workers.write().await.push(tx);

            let admission = pool
                .queue_replica_task(ReplicateObjectInfo {
                    bucket: "runtime-backlog".to_string(),
                    name: "fallback-object".to_string(),
                    size: 2048,
                    op_type: ReplicationType::Object,
                    dsc: test_replicate_decision(&["arn:rustfs:replication:target-a"]),
                    ..Default::default()
                })
                .await;

            assert_eq!(admission, ReplicationQueueAdmission::Queued);
            assert_eq!(current_queue(&pool, "runtime-backlog").await, (0, 0));
            let durable = shared.data.lock().expect("durable MRF data lock should not poison").clone();
            let entries = decode_mrf_file(&durable).expect("durably admitted MRF entry should decode");
            assert_eq!(entries.len(), 1);
            assert_eq!(entries[0].object, "fallback-object");
        })
        .await;
    }

    #[test]
    fn replicate_object_info_from_object_info_preserves_ssec_checksum() {
        let checksum = bytes::Bytes::from_static(b"ssec-checksum");
        let oi = ObjectInfo {
            bucket: "source".to_string(),
            name: "object".to_string(),
            user_defined: Arc::new(HashMap::from([(
                rustfs_utils::http::SSEC_ALGORITHM_HEADER.to_string(),
                "AES256".to_string(),
            )])),
            checksum: Some(checksum.clone()),
            ..Default::default()
        };

        let ri = replicate_object_info_from_object_info(oi, ReplicateDecision::default(), ReplicationType::Object);

        assert!(ri.ssec);
        assert_eq!(ri.checksum, Some(checksum));
    }

    #[tokio::test]
    async fn mrf_save_admission_waits_for_capacity_instead_of_dropping() {
        let (tx, mut rx) = mpsc::channel(1);
        let permits = Arc::new(Semaphore::new(2));
        let first = MrfReplicateEntry {
            bucket: "bucket".to_string(),
            object: "first".to_string(),
            version_id: None,
            retry_count: 1,
            size: 1,
            op: MrfOpKind::Object,
            force_delete: false,
            delete_marker_version_id: None,
            delete_marker: false,
            delete_marker_mtime: None,
            target_arns: Vec::new(),
            ..Default::default()
        };
        let second = MrfReplicateEntry {
            object: "second".to_string(),
            ..first.clone()
        };

        let (first_persisted, _first_completion) = oneshot::channel();
        tx.try_send(MrfSaveRequest {
            file: ReplicationMetadataStore::MRF_REPLICATION_FILE,
            entries: vec![first],
            persisted: first_persisted,
            permit: permits
                .clone()
                .acquire_owned()
                .await
                .expect("first MRF permit should be available"),
        })
        .expect("first MRF request should fill the test channel");
        let (second_persisted, _second_completion) = oneshot::channel();
        let admission = tx.send(MrfSaveRequest {
            file: ReplicationMetadataStore::MRF_REPLICATION_FILE,
            entries: vec![second],
            persisted: second_persisted,
            permit: permits.acquire_owned().await.expect("second MRF permit should be available"),
        });
        tokio::pin!(admission);

        assert!(
            tokio::time::timeout(Duration::from_millis(25), &mut admission).await.is_err(),
            "full MRF channel should apply backpressure instead of returning Missed"
        );

        let received = rx.recv().await.expect("first MRF request should still be queued");
        assert_eq!(received.entries[0].object, "first");

        tokio::time::timeout(Duration::from_secs(1), &mut admission)
            .await
            .expect("MRF admission should finish once capacity is available")
            .expect("MRF request channel should remain open");

        let received = rx
            .recv()
            .await
            .expect("second MRF entry should be queued after capacity opens");
        assert_eq!(received.entries[0].object, "second");
    }

    #[tokio::test]
    async fn delete_batch_admission_reports_durable_mrf_fallback_items() {
        let pool = new_test_replication_pool(Arc::new(LoadResyncNodeStore::new("node-a", empty_resync_shared_state()))).await;
        let (worker_tx, worker_rx) = mpsc::channel(1);
        worker_tx
            .try_send(ReplicationOperation::Delete(Box::new(DeletedObjectReplicationInfo {
                bucket: "batch-backpressure".to_string(),
                delete_object: ReplicationDeletedObject {
                    object_name: "already-queued".to_string(),
                    ..Default::default()
                },
                op_type: ReplicationType::Delete,
                ..Default::default()
            })))
            .expect("test worker channel should be full");
        pool.workers.write().await.push(worker_tx);

        let mut mrf_rx = pool
            .mrf_save_rx
            .lock()
            .await
            .take()
            .expect("test should own the MRF save receiver");
        let deletes = vec![DeletedObjectReplicationInfo {
            bucket: "batch-backpressure".to_string(),
            delete_object: ReplicationDeletedObject {
                object_name: "object-0".to_string(),
                ..Default::default()
            },
            op_type: ReplicationType::Delete,
            ..Default::default()
        }];
        let admission = pool.queue_replica_delete_batch(&deletes);
        tokio::pin!(admission);
        assert!(
            tokio::time::timeout(Duration::from_millis(25), &mut admission).await.is_err(),
            "batch admission should wait for durable MRF persistence"
        );
        let request = mrf_rx.recv().await.expect("MRF fallback request should be queued");
        assert_eq!(request.entries[0].object, "object-0");
        request
            .persisted
            .send(true)
            .expect("batch admission should still await persistence");

        let summary = tokio::time::timeout(Duration::from_secs(1), &mut admission)
            .await
            .expect("batch admission should finish after MRF persistence");
        assert_eq!(summary.total, 1);
        assert_eq!(summary.queued, 1);
        assert_eq!(summary.missed, 0);
        assert_eq!(summary.outcome(), "all_queued");
        drop(worker_rx);
    }

    #[tokio::test]
    async fn mrf_flush_failure_keeps_pending_backlog_observable() {
        let shared = empty_resync_shared_state();
        shared.fail_next_write.store(true, Ordering::SeqCst);
        let storage = Arc::new(LoadResyncNodeStore::new("mrf-flush-failure", shared));
        let entry = MrfReplicateEntry {
            bucket: "mrf-flush-failure-bucket".to_string(),
            object: "pending".to_string(),
            version_id: None,
            retry_count: 1,
            size: 2048,
            op: MrfOpKind::Object,
            force_delete: false,
            delete_marker_version_id: None,
            delete_marker: false,
            delete_marker_mtime: None,
            target_arns: Vec::new(),
            ..Default::default()
        };
        observe_mrf_pending(&entry);

        let result =
            merge_mrf_entries_to_disk(ReplicationMetadataStore::MRF_REPLICATION_FILE, std::slice::from_ref(&entry), &storage)
                .await;

        assert_eq!(result, None);
        let snapshot = mrf_backlog_observability_snapshot();
        let bucket = snapshot
            .buckets
            .iter()
            .find(|stats| stats.bucket == "mrf-flush-failure-bucket")
            .expect("failed MRF flush should keep the bucket observable");
        assert_eq!(bucket.pending_count, 1);
        assert_eq!(bucket.pending_bytes, 2048);
        assert_eq!(bucket.flush_failure_count, 1);
    }

    #[tokio::test]
    async fn delete_replay_apply_failure_is_not_acknowledged() {
        let shared = empty_resync_shared_state();
        let storage = Arc::new(LoadResyncNodeStore::new("node-a", shared.clone()));
        let outcome = execute_recovered_mrf_operations(
            vec![ReplicationOperation::Delete(Box::new(DeletedObjectReplicationInfo {
                bucket: "source".to_string(),
                delete_object: ReplicationDeletedObject {
                    object_name: "delete".to_string(),
                    version_id: Some(Uuid::new_v4()),
                    replication_state: Some(Default::default()),
                    ..Default::default()
                },
                ..Default::default()
            }))],
            storage,
        )
        .await;

        assert!(matches!(outcome, MrfReplayResult::Retain));
        assert_eq!(shared.delete_object_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn source_absent_delete_replay_is_acknowledged_after_reconciliation() {
        let _probe = DeleteReplicationSourceCheckProbe::install("source", "gone", vec![false], None).await;
        let shared = empty_resync_shared_state();
        let storage = Arc::new(LoadResyncNodeStore::new("node-a", shared.clone()));

        let outcome = execute_recovered_mrf_operations(
            vec![ReplicationOperation::Delete(Box::new(DeletedObjectReplicationInfo {
                bucket: "source".to_string(),
                delete_object: ReplicationDeletedObject {
                    object_name: "gone".to_string(),
                    delete_marker: true,
                    delete_marker_version_id: Some(Uuid::from_u128(9)),
                    replication_state: Some(Default::default()),
                    ..Default::default()
                },
                ..Default::default()
            }))],
            storage,
        )
        .await;

        assert!(matches!(outcome, MrfReplayResult::Acknowledge(ref retries) if retries.is_empty()));
        assert_eq!(shared.delete_object_count.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn targeted_mrf_queue_rejects_empty_target() {
        let pool = new_test_replication_pool(Arc::new(LoadResyncNodeStore::new("node-a", empty_resync_shared_state()))).await;

        let admission = pool
            .queue_mrf_delete_task(DeletedObjectReplicationInfo {
                bucket: "source".to_string(),
                delete_object: ReplicationDeletedObject {
                    object_name: "delete".to_string(),
                    delete_marker: true,
                    ..Default::default()
                },
                ..Default::default()
            })
            .await;

        assert_eq!(admission, ReplicationQueueAdmission::Missed);
    }

    #[tokio::test]
    async fn targeted_mrf_queue_persists_directly_to_v2() {
        temp_env::async_with_vars([("RUSTFS_REPL_MRF_FLUSH_INTERVAL_MS", Some("10"))], async {
            let shared = empty_resync_shared_state();
            let pool = new_test_replication_pool(Arc::new(LoadResyncNodeStore::new("node-a", shared.clone()))).await;
            pool.start_mrf_persister().await;
            let target_version_id = "opaque-target-marker";

            let admission = pool
                .queue_mrf_delete_task(DeletedObjectReplicationInfo {
                    bucket: "source".to_string(),
                    target_arn: "arn:target-a".to_string(),
                    target_delete_marker_version_id: Some(target_version_id.to_string()),
                    delete_object: ReplicationDeletedObject {
                        object_name: "delete".to_string(),
                        delete_marker: true,
                        delete_marker_version_id: Some(Uuid::new_v4()),
                        ..Default::default()
                    },
                    ..Default::default()
                })
                .await;

            assert_eq!(admission, ReplicationQueueAdmission::Queued);
            assert!(
                shared
                    .data
                    .lock()
                    .expect("test legacy MRF lock should not be poisoned")
                    .is_empty()
            );
            let targeted = shared
                .targeted_mrf_data
                .lock()
                .expect("test targeted MRF lock should not be poisoned")
                .clone();
            let entries = decode_mrf_file(&targeted).expect("targeted MRF should decode");
            assert_eq!(entries.len(), 1);
            assert_eq!(entries[0].target_arns, vec!["arn:target-a".to_string()]);
            assert_eq!(entries[0].target_delete_marker_version_id.as_deref(), Some(target_version_id));
        })
        .await;
    }

    #[tokio::test]
    async fn targeted_mrf_batch_admission_is_all_or_nothing_and_deduplicated() {
        temp_env::async_with_vars([("RUSTFS_REPL_MRF_FLUSH_INTERVAL_MS", Some("10"))], async {
            let shared = empty_resync_shared_state();
            let pool = new_test_replication_pool(Arc::new(LoadResyncNodeStore::new("node-a", shared.clone()))).await;
            let task = |target: &str, target_version: &str| DeletedObjectReplicationInfo {
                bucket: "source".to_string(),
                target_arn: target.to_string(),
                target_delete_marker_version_id: Some(target_version.to_string()),
                delete_object: ReplicationDeletedObject {
                    object_name: "delete".to_string(),
                    delete_marker: true,
                    delete_marker_version_id: Some(Uuid::from_u128(7)),
                    ..Default::default()
                },
                ..Default::default()
            };
            let valid = vec![task("arn:target-a", "marker-a"), task("arn:target-b", "marker-b")];
            let mut invalid = valid.clone();
            invalid[1].target_delete_marker_version_id = Some(String::new());

            assert_eq!(pool.queue_mrf_delete_tasks(invalid).await, ReplicationQueueAdmission::Missed);
            assert!(
                shared
                    .targeted_mrf_data
                    .lock()
                    .expect("test targeted MRF data lock should not be poisoned")
                    .is_empty()
            );

            pool.start_mrf_persister().await;
            assert_eq!(pool.queue_mrf_delete_tasks(valid.clone()).await, ReplicationQueueAdmission::Queued);
            assert_eq!(pool.queue_mrf_delete_tasks(valid).await, ReplicationQueueAdmission::Queued);

            let data = shared
                .targeted_mrf_data
                .lock()
                .expect("test targeted MRF data lock should not be poisoned")
                .clone();
            let entries = decode_mrf_file(&data).expect("targeted MRF batch should decode");
            assert_eq!(entries.len(), 2);
            assert_eq!(
                entries
                    .iter()
                    .map(|entry| (entry.target_arns[0].as_str(), entry.target_delete_marker_version_id.as_deref()))
                    .collect::<HashSet<_>>(),
                HashSet::from([("arn:target-a", Some("marker-a")), ("arn:target-b", Some("marker-b"))])
            );
        })
        .await;
    }

    #[tokio::test]
    async fn targeted_mrf_queue_waits_for_durable_retry_without_changing_legacy_file() {
        temp_env::async_with_vars([("RUSTFS_REPL_MRF_FLUSH_INTERVAL_MS", Some("10"))], async {
            let shared = empty_resync_shared_state();
            let legacy = MrfReplicateEntry {
                bucket: "source".to_string(),
                object: "legacy".to_string(),
                version_id: None,
                retry_count: 1,
                size: 1,
                op: MrfOpKind::Object,
                force_delete: false,
                delete_marker_version_id: None,
                delete_marker: false,
                delete_marker_mtime: None,
                target_arns: Vec::new(),
                target_delete_marker_version_id: None,
                source_mod_time: None,
                enqueued_order: None,
                ..Default::default()
            };
            let legacy_data = encode_mrf_file(std::slice::from_ref(&legacy)).expect("legacy MRF should encode");
            *shared.data.lock().expect("test legacy MRF lock should not be poisoned") = legacy_data.clone();
            shared.fail_next_write.store(true, Ordering::SeqCst);
            let pool = new_test_replication_pool(Arc::new(LoadResyncNodeStore::new("node-a", shared.clone()))).await;
            pool.start_mrf_persister().await;

            let admission = pool
                .queue_mrf_delete_task(DeletedObjectReplicationInfo {
                    bucket: "source".to_string(),
                    target_arn: "arn:target-b".to_string(),
                    target_delete_marker_version_id: Some("opaque-target-marker".to_string()),
                    delete_object: ReplicationDeletedObject {
                        object_name: "delete".to_string(),
                        delete_marker: true,
                        delete_marker_version_id: Some(Uuid::new_v4()),
                        ..Default::default()
                    },
                    ..Default::default()
                })
                .await;

            assert_eq!(admission, ReplicationQueueAdmission::Queued);
            assert_eq!(*shared.data.lock().expect("test legacy MRF lock should not be poisoned"), legacy_data);
            let targeted = shared
                .targeted_mrf_data
                .lock()
                .expect("test targeted MRF lock should not be poisoned")
                .clone();
            let queued = decode_mrf_file(&targeted).expect("durably acknowledged targeted MRF should decode");
            assert_eq!(queued[0].object, "delete");
            assert_eq!(queued[0].target_arns, vec!["arn:target-b".to_string()]);
        })
        .await;
    }

    #[tokio::test]
    async fn durable_mrf_backlog_combines_v1_and_v2() {
        let shared = empty_resync_shared_state();
        let legacy = MrfReplicateEntry {
            bucket: "source".to_string(),
            object: "legacy".to_string(),
            version_id: None,
            retry_count: 1,
            size: 1,
            op: MrfOpKind::Object,
            force_delete: false,
            delete_marker_version_id: None,
            delete_marker: false,
            delete_marker_mtime: None,
            target_arns: Vec::new(),
            target_delete_marker_version_id: None,
            source_mod_time: None,
            enqueued_order: None,
            ..Default::default()
        };
        let targeted = MrfReplicateEntry {
            object: "targeted".to_string(),
            op: MrfOpKind::Delete,
            delete_marker: true,
            delete_marker_version_id: Some(Uuid::from_u128(1)),
            target_arns: vec!["arn:target-a".to_string()],
            ..legacy.clone()
        };
        *shared.data.lock().expect("test legacy MRF lock should not be poisoned") =
            encode_mrf_file(std::slice::from_ref(&legacy)).expect("legacy MRF should encode");
        *shared
            .targeted_mrf_data
            .lock()
            .expect("test targeted MRF lock should not be poisoned") =
            encode_mrf_file(std::slice::from_ref(&targeted)).expect("targeted MRF should encode");
        let storage = Arc::new(LoadResyncNodeStore::new("node-a", shared.clone()));

        let backlog = read_durable_mrf_backlog(storage.clone()).await;

        assert!(backlog.available);
        assert_eq!(
            backlog
                .entries
                .iter()
                .map(|entry| entry.object.as_str())
                .collect::<HashSet<_>>(),
            HashSet::from(["legacy", "targeted"])
        );

        *shared
            .targeted_mrf_data
            .lock()
            .expect("test targeted MRF lock should not be poisoned") = vec![0xde, 0xad, 0xbe, 0xef];
        assert!(!read_durable_mrf_backlog(storage).await.available);
    }

    #[tokio::test]
    async fn concurrent_mrf_merges_preserve_both_entries() {
        let shared = empty_resync_shared_state();
        let node_a = Arc::new(LoadResyncNodeStore::new("node-a", shared.clone()));
        let node_b = Arc::new(LoadResyncNodeStore::new("node-b", shared.clone()));
        let first = MrfReplicateEntry {
            bucket: "source".to_string(),
            object: "first".to_string(),
            version_id: None,
            retry_count: 0,
            size: 1,
            op: MrfOpKind::Object,
            force_delete: false,
            delete_marker_version_id: None,
            delete_marker: false,
            delete_marker_mtime: None,
            target_arns: Vec::new(),
            target_delete_marker_version_id: None,
            source_mod_time: None,
            enqueued_order: None,
            ..Default::default()
        };
        let second = MrfReplicateEntry {
            object: "second".to_string(),
            ..first.clone()
        };

        let first_entries = [first];
        let second_entries = [second];
        let (first_result, second_result) = tokio::join!(
            merge_mrf_entries_to_disk(ReplicationMetadataStore::MRF_REPLICATION_FILE, &first_entries, &node_a),
            merge_mrf_entries_to_disk(ReplicationMetadataStore::MRF_REPLICATION_FILE, &second_entries, &node_b),
        );

        assert!(first_result.is_some());
        assert!(second_result.is_some());
        let encoded = shared.data.lock().expect("test MRF data lock should not be poisoned").clone();
        let entries = decode_mrf_file(&encoded).expect("merged MRF file should decode");
        assert_eq!(entries.len(), 2);
        assert_eq!(
            entries.iter().map(|entry| entry.object.as_str()).collect::<HashSet<_>>(),
            HashSet::from(["first", "second"])
        );
    }

    #[test]
    fn persisted_mrf_cap_rejects_a_batch_without_partial_append() {
        let base = MrfReplicateEntry {
            bucket: "source".to_string(),
            object: "base".to_string(),
            version_id: Some(Uuid::from_u128(1)),
            retry_count: 0,
            size: 1,
            op: MrfOpKind::Object,
            force_delete: false,
            delete_marker_version_id: None,
            delete_marker: false,
            delete_marker_mtime: None,
            target_arns: Vec::new(),
            target_delete_marker_version_id: None,
            source_mod_time: None,
            enqueued_order: None,
            ..Default::default()
        };
        let first = MrfReplicateEntry {
            object: "first".to_string(),
            version_id: Some(Uuid::from_u128(2)),
            ..base.clone()
        };
        let second = MrfReplicateEntry {
            object: "second".to_string(),
            version_id: Some(Uuid::from_u128(3)),
            ..base.clone()
        };
        let mut entries = vec![base.clone()];

        assert!(!merge_mrf_entries_with_cap(&mut entries, &[first, second], 2).expect("MRF entries should encode"));
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].object, base.object);
        assert_eq!(entries[0].version_id, base.version_id);
        assert!(
            merge_mrf_entries_with_cap(&mut entries, std::slice::from_ref(&base), 1).expect("duplicate MRF entry should encode")
        );
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].object, base.object);
        assert_eq!(entries[0].version_id, base.version_id);
    }

    #[tokio::test]
    async fn conditional_merge_retries_after_stale_snapshot_without_overwrite() {
        let shared = empty_resync_shared_state();
        let node_a = Arc::new(LoadResyncNodeStore::new("node-a", shared.clone()));
        let node_b = Arc::new(LoadResyncNodeStore::new("node-b", shared.clone()));
        let base = MrfReplicateEntry {
            bucket: "source".to_string(),
            object: "base".to_string(),
            version_id: Some(Uuid::from_u128(1)),
            retry_count: 0,
            size: 1,
            op: MrfOpKind::Object,
            force_delete: false,
            delete_marker_version_id: None,
            delete_marker: false,
            delete_marker_mtime: None,
            target_arns: Vec::new(),
            target_delete_marker_version_id: None,
            source_mod_time: None,
            enqueued_order: None,
            ..Default::default()
        };
        *shared.data.lock().expect("test MRF data lock should not poison") =
            encode_mrf_file(std::slice::from_ref(&base)).expect("base MRF should encode");
        let first = MrfReplicateEntry {
            object: "first".to_string(),
            version_id: Some(Uuid::from_u128(2)),
            ..base.clone()
        };
        let second = MrfReplicateEntry {
            object: "second".to_string(),
            version_id: Some(Uuid::from_u128(3)),
            ..base
        };
        shared.block_next_write.store(true, Ordering::SeqCst);
        let first_entry = first.clone();
        let first_merge = tokio::spawn(async move {
            merge_mrf_entries_to_disk(
                ReplicationMetadataStore::MRF_REPLICATION_FILE,
                std::slice::from_ref(&first_entry),
                &node_a,
            )
            .await
        });
        tokio::time::timeout(Duration::from_secs(1), shared.write_started.notified())
            .await
            .expect("first conditional write should pause after its snapshot");

        assert!(
            merge_mrf_entries_to_disk(ReplicationMetadataStore::MRF_REPLICATION_FILE, std::slice::from_ref(&second), &node_b,)
                .await
                .is_some()
        );
        shared.allow_write.notify_one();
        assert!(
            first_merge.await.expect("first merge task should finish").is_none(),
            "stale If-Match must fail instead of overwriting the concurrent entry"
        );
        assert!(
            merge_mrf_entries_to_disk(
                ReplicationMetadataStore::MRF_REPLICATION_FILE,
                std::slice::from_ref(&first),
                &Arc::new(LoadResyncNodeStore::new("node-a-retry", shared.clone())),
            )
            .await
            .is_some()
        );

        let data = shared.data.lock().expect("test MRF data lock should not poison").clone();
        assert_eq!(
            decode_mrf_file(&data)
                .expect("merged MRF should decode")
                .iter()
                .map(|entry| entry.object.as_str())
                .collect::<HashSet<_>>(),
            HashSet::from(["base", "first", "second"])
        );
    }

    #[tokio::test]
    async fn batch_ack_preserves_unmatched_and_concurrent_entries() {
        let shared = empty_resync_shared_state();
        let storage = Arc::new(LoadResyncNodeStore::new("node-a", shared.clone()));
        let acknowledged = MrfReplicateEntry {
            bucket: "source".to_string(),
            object: "acknowledged".to_string(),
            version_id: None,
            retry_count: 0,
            size: 1,
            op: MrfOpKind::Object,
            force_delete: false,
            delete_marker_version_id: None,
            delete_marker: false,
            delete_marker_mtime: None,
            target_arns: Vec::new(),
            target_delete_marker_version_id: None,
            source_mod_time: None,
            enqueued_order: None,
            ..Default::default()
        };
        let retained = MrfReplicateEntry {
            object: "retained".to_string(),
            ..acknowledged.clone()
        };
        let concurrent = MrfReplicateEntry {
            object: "concurrent".to_string(),
            ..acknowledged.clone()
        };
        *shared.data.lock().expect("test MRF data lock should not be poisoned") =
            encode_mrf_file(&[acknowledged.clone(), retained]).expect("test MRF entries should encode");
        assert!(
            merge_mrf_entries_to_disk(
                ReplicationMetadataStore::MRF_REPLICATION_FILE,
                std::slice::from_ref(&concurrent),
                &storage,
            )
            .await
            .is_some()
        );

        let acknowledgement = MrfReplayAcknowledgement {
            original: encoded_mrf_entry(&acknowledged).expect("acknowledged entry should encode"),
            replacements: Vec::new(),
        };
        let matched = acknowledge_mrf_batch(
            ReplicationMetadataStore::MRF_REPLICATION_FILE,
            vec![MrfReplayAcknowledgement {
                original: acknowledgement.original.clone(),
                replacements: Vec::new(),
            }],
            &storage,
            None,
        )
        .await;

        assert_eq!(matched, Some(1));
        let data = shared.data.lock().expect("test MRF data lock should not be poisoned").clone();
        let entries = decode_mrf_file(&data).expect("acknowledged MRF should decode");
        assert_eq!(
            entries.iter().map(|entry| entry.object.as_str()).collect::<HashSet<_>>(),
            HashSet::from(["retained", "concurrent"])
        );

        assert_eq!(
            acknowledge_mrf_batch(ReplicationMetadataStore::MRF_REPLICATION_FILE, vec![acknowledgement], &storage, None,).await,
            Some(0),
            "a repeated acknowledgement must not remove a different entry"
        );
    }

    #[tokio::test]
    async fn batch_ack_retries_cas_conflict_without_replaying_work() {
        let shared = empty_resync_shared_state();
        let node_a = Arc::new(LoadResyncNodeStore::new("node-a", shared.clone()));
        let node_b = Arc::new(LoadResyncNodeStore::new("node-b", shared.clone()));
        let acknowledged = MrfReplicateEntry {
            bucket: "source".to_string(),
            object: "acknowledged".to_string(),
            version_id: Some(Uuid::from_u128(1)),
            retry_count: 0,
            size: 1,
            op: MrfOpKind::Object,
            force_delete: false,
            delete_marker_version_id: None,
            delete_marker: false,
            delete_marker_mtime: None,
            target_arns: Vec::new(),
            target_delete_marker_version_id: None,
            source_mod_time: None,
            enqueued_order: None,
            ..Default::default()
        };
        let concurrent = MrfReplicateEntry {
            object: "concurrent".to_string(),
            version_id: Some(Uuid::from_u128(2)),
            ..acknowledged.clone()
        };
        *shared.data.lock().expect("test MRF data lock should not be poisoned") =
            encode_mrf_file(std::slice::from_ref(&acknowledged)).expect("test MRF entry should encode");
        shared.block_next_write.store(true, Ordering::SeqCst);
        let acknowledgement = MrfReplayAcknowledgement {
            original: encoded_mrf_entry(&acknowledged).expect("acknowledged entry should encode"),
            replacements: Vec::new(),
        };
        let acknowledge = tokio::spawn(async move {
            acknowledge_mrf_batch(ReplicationMetadataStore::MRF_REPLICATION_FILE, vec![acknowledgement], &node_a, None).await
        });
        tokio::time::timeout(Duration::from_secs(1), shared.write_started.notified())
            .await
            .expect("acknowledgement write should pause after its snapshot");

        assert!(
            merge_mrf_entries_to_disk(
                ReplicationMetadataStore::MRF_REPLICATION_FILE,
                std::slice::from_ref(&concurrent),
                &node_b,
            )
            .await
            .is_some()
        );
        shared.allow_write.notify_one();

        assert_eq!(acknowledge.await.expect("acknowledgement task should finish"), Some(1));
        let data = shared.data.lock().expect("test MRF data lock should not be poisoned").clone();
        let entries = decode_mrf_file(&data).expect("acknowledged MRF should decode");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].object, "concurrent");
    }

    #[tokio::test]
    async fn generationless_repeated_entry_survives_one_acknowledgement() {
        let shared = empty_resync_shared_state();
        let storage = Arc::new(LoadResyncNodeStore::new("node-a", shared.clone()));
        let entry = MrfReplicateEntry {
            bucket: "source".to_string(),
            object: "unversioned".to_string(),
            version_id: None,
            retry_count: 0,
            size: 1,
            op: MrfOpKind::Object,
            force_delete: false,
            delete_marker_version_id: None,
            delete_marker: false,
            delete_marker_mtime: None,
            target_arns: Vec::new(),
            target_delete_marker_version_id: None,
            source_mod_time: None,
            enqueued_order: None,
            ..Default::default()
        };
        assert!(
            merge_mrf_entries_to_disk(ReplicationMetadataStore::MRF_REPLICATION_FILE, std::slice::from_ref(&entry), &storage,)
                .await
                .is_some()
        );
        assert!(
            merge_mrf_entries_to_disk(ReplicationMetadataStore::MRF_REPLICATION_FILE, std::slice::from_ref(&entry), &storage,)
                .await
                .is_some()
        );
        let before = shared.data.lock().expect("test MRF data lock should not poison").clone();
        assert_eq!(decode_mrf_file(&before).expect("repeated MRF should decode").len(), 2);

        assert_eq!(
            acknowledge_mrf_batch(
                ReplicationMetadataStore::MRF_REPLICATION_FILE,
                vec![MrfReplayAcknowledgement {
                    original: encoded_mrf_entry(&entry).expect("generationless MRF entry should encode"),
                    replacements: Vec::new(),
                }],
                &storage,
                None,
            )
            .await,
            Some(1)
        );
        let after = shared.data.lock().expect("test MRF data lock should not poison").clone();
        assert_eq!(decode_mrf_file(&after).expect("remaining MRF should decode").len(), 1);
    }

    #[tokio::test]
    async fn legacy_and_targeted_mrf_files_persist_independently() {
        let shared = empty_resync_shared_state();
        let storage = Arc::new(LoadResyncNodeStore::new("node-a", shared.clone()));
        let legacy = MrfReplicateEntry {
            bucket: "source".to_string(),
            object: "legacy".to_string(),
            version_id: None,
            retry_count: 0,
            size: 1,
            op: MrfOpKind::Object,
            force_delete: false,
            delete_marker_version_id: None,
            delete_marker: false,
            delete_marker_mtime: None,
            target_arns: Vec::new(),
            target_delete_marker_version_id: None,
            source_mod_time: None,
            enqueued_order: None,
            ..Default::default()
        };
        let targeted = MrfReplicateEntry {
            object: "targeted".to_string(),
            op: MrfOpKind::Delete,
            delete_marker: true,
            delete_marker_version_id: Some(Uuid::from_u128(1)),
            target_arns: vec!["arn:target-a".to_string()],
            ..legacy.clone()
        };

        let legacy_entries = [legacy];
        let targeted_entries = [targeted];
        let (legacy_result, targeted_result) = tokio::join!(
            merge_mrf_entries_to_disk(ReplicationMetadataStore::MRF_REPLICATION_FILE, &legacy_entries, &storage,),
            merge_mrf_entries_to_disk(ReplicationMetadataStore::TARGETED_MRF_REPLICATION_FILE, &targeted_entries, &storage,),
        );

        assert!(legacy_result.is_some());
        assert!(targeted_result.is_some());
        let legacy_data = shared
            .data
            .lock()
            .expect("test legacy MRF lock should not be poisoned")
            .clone();
        let targeted_data = shared
            .targeted_mrf_data
            .lock()
            .expect("test targeted MRF lock should not be poisoned")
            .clone();
        assert_eq!(decode_mrf_file(&legacy_data).expect("legacy MRF should decode")[0].object, "legacy");
        assert_eq!(decode_mrf_file(&targeted_data).expect("targeted MRF should decode")[0].object, "targeted");
    }

    #[tokio::test]
    async fn v1_ack_waits_for_v2_retry_persistence() {
        let shared = empty_resync_shared_state();
        let storage = Arc::new(LoadResyncNodeStore::new("node-a", shared.clone()));
        let source = MrfReplicateEntry {
            bucket: "source".to_string(),
            object: "delete".to_string(),
            version_id: None,
            retry_count: 0,
            size: 0,
            op: MrfOpKind::Delete,
            force_delete: false,
            delete_marker_version_id: Some(Uuid::from_u128(11)),
            delete_marker: true,
            delete_marker_mtime: None,
            target_arns: Vec::new(),
            target_delete_marker_version_id: None,
            source_mod_time: None,
            enqueued_order: None,
            ..Default::default()
        };
        let targeted_retry = MrfReplicateEntry {
            target_arns: vec!["arn:target-a".to_string()],
            target_delete_marker_version_id: Some("opaque-marker".to_string()),
            ..source.clone()
        };
        *shared.data.lock().expect("legacy MRF lock should not poison") =
            encode_mrf_file(std::slice::from_ref(&source)).expect("legacy MRF should encode");
        shared.fail_next_write.store(true, Ordering::SeqCst);
        let pending = || {
            vec![(
                encoded_mrf_entry(&source).expect("legacy MRF entry should encode"),
                Vec::new(),
                vec![targeted_retry.clone()],
            )]
        };

        assert_eq!(
            commit_mrf_replay_batch(ReplicationMetadataStore::MRF_REPLICATION_FILE, pending(), &storage, None,).await,
            Some(0)
        );
        let legacy_after_failure = shared.data.lock().expect("legacy MRF lock should not poison").clone();
        assert_eq!(
            decode_mrf_file(&legacy_after_failure)
                .expect("legacy MRF should decode")
                .len(),
            1
        );
        assert!(
            shared
                .targeted_mrf_data
                .lock()
                .expect("targeted MRF lock should not poison")
                .is_empty()
        );

        assert_eq!(
            commit_mrf_replay_batch(ReplicationMetadataStore::MRF_REPLICATION_FILE, pending(), &storage, None,).await,
            Some(1)
        );
        let legacy = shared.data.lock().expect("legacy MRF lock should not poison").clone();
        assert!(decode_mrf_file(&legacy).expect("legacy MRF should decode").is_empty());
        let targeted = shared
            .targeted_mrf_data
            .lock()
            .expect("targeted MRF lock should not poison")
            .clone();
        let targeted = decode_mrf_file(&targeted).expect("targeted MRF should decode");
        assert_eq!(targeted.len(), 1);
        assert_eq!(targeted[0].target_arns, targeted_retry.target_arns);
        assert_eq!(
            targeted[0].target_delete_marker_version_id,
            targeted_retry.target_delete_marker_version_id
        );
    }

    #[tokio::test]
    async fn transient_recovery_failure_keeps_durable_entry() {
        let shared = empty_resync_shared_state();
        let entry = MrfReplicateEntry {
            bucket: "source".to_string(),
            object: "retry".to_string(),
            version_id: None,
            retry_count: 1,
            size: 1,
            op: MrfOpKind::Object,
            force_delete: false,
            delete_marker_version_id: None,
            delete_marker: false,
            delete_marker_mtime: None,
            target_arns: Vec::new(),
            target_delete_marker_version_id: None,
            source_mod_time: None,
            enqueued_order: None,
            ..Default::default()
        };
        *shared.data.lock().expect("test MRF data lock should not be poisoned") =
            encode_mrf_file(std::slice::from_ref(&entry)).expect("test MRF entry should encode");
        let storage = Arc::new(LoadResyncNodeStore::new("node-a", shared.clone()));

        let recovered = process_mrf_file(ReplicationMetadataStore::MRF_REPLICATION_FILE, storage).await;

        assert_eq!(recovered, 0);
        let encoded = shared.data.lock().expect("test MRF data lock should not be poisoned").clone();
        let retained = decode_mrf_file(&encoded).expect("retained MRF should decode");
        assert_eq!(retained.len(), 1);
        assert_eq!(retained[0].object, entry.object);
    }

    #[tokio::test]
    async fn replay_panic_keeps_durable_entry() {
        let shared = empty_resync_shared_state();
        let panic_entry = MrfReplicateEntry {
            bucket: "source".to_string(),
            object: "panic".to_string(),
            version_id: None,
            retry_count: 1,
            size: 1,
            op: MrfOpKind::Object,
            force_delete: false,
            delete_marker_version_id: None,
            delete_marker: false,
            delete_marker_mtime: None,
            target_arns: Vec::new(),
            target_delete_marker_version_id: None,
            source_mod_time: None,
            enqueued_order: None,
            ..Default::default()
        };
        let terminal_entry = MrfReplicateEntry {
            object: "terminal".to_string(),
            ..panic_entry.clone()
        };
        *shared.data.lock().expect("test MRF data lock should not be poisoned") =
            encode_mrf_file(&[panic_entry.clone(), terminal_entry]).expect("test MRF entries should encode");
        let storage = Arc::new(LoadResyncNodeStore::new("node-a", shared.clone()));

        let recovered = process_mrf_file(ReplicationMetadataStore::MRF_REPLICATION_FILE, storage).await;

        assert_eq!(recovered, 1, "another entry in the same batch must survive a worker panic");
        let encoded = shared.data.lock().expect("test MRF data lock should not be poisoned").clone();
        let retained = decode_mrf_file(&encoded).expect("retained MRF should decode");
        assert_eq!(retained.len(), 1);
        assert_eq!(retained[0].object, panic_entry.object);
    }

    #[tokio::test]
    async fn replay_cancellation_keeps_durable_entry() {
        let shared = empty_resync_shared_state();
        shared.block_next_object_info.store(true, Ordering::SeqCst);
        let entry = MrfReplicateEntry {
            bucket: "source".to_string(),
            object: "cancel".to_string(),
            version_id: None,
            retry_count: 1,
            size: 1,
            op: MrfOpKind::Object,
            force_delete: false,
            delete_marker_version_id: None,
            delete_marker: false,
            delete_marker_mtime: None,
            target_arns: Vec::new(),
            target_delete_marker_version_id: None,
            source_mod_time: None,
            enqueued_order: None,
            ..Default::default()
        };
        *shared.data.lock().expect("test MRF data lock should not be poisoned") =
            encode_mrf_file(std::slice::from_ref(&entry)).expect("test MRF entry should encode");
        let storage = Arc::new(LoadResyncNodeStore::new("node-a", shared.clone()));
        let task = tokio::spawn(process_mrf_file(ReplicationMetadataStore::MRF_REPLICATION_FILE, storage));
        tokio::time::timeout(Duration::from_secs(1), shared.object_info_started.notified())
            .await
            .expect("replay should reach the blocked worker");

        task.abort();
        assert!(task.await.expect_err("aborted replay should be cancelled").is_cancelled());

        let encoded = shared.data.lock().expect("test MRF data lock should not be poisoned").clone();
        assert_eq!(decode_mrf_file(&encoded).expect("retained MRF should decode").len(), 1);
    }

    #[tokio::test]
    async fn two_nodes_execute_one_durable_entry_once() {
        let shared = empty_resync_shared_state();
        shared.block_next_object_info.store(true, Ordering::SeqCst);
        let entry = MrfReplicateEntry {
            bucket: "source".to_string(),
            object: "terminal".to_string(),
            version_id: None,
            retry_count: 1,
            size: 1,
            op: MrfOpKind::Object,
            force_delete: false,
            delete_marker_version_id: None,
            delete_marker: false,
            delete_marker_mtime: None,
            target_arns: Vec::new(),
            target_delete_marker_version_id: None,
            source_mod_time: None,
            enqueued_order: None,
            ..Default::default()
        };
        *shared.data.lock().expect("test MRF data lock should not be poisoned") =
            encode_mrf_file(std::slice::from_ref(&entry)).expect("test MRF entry should encode");
        let node_a = Arc::new(LoadResyncNodeStore::new("node-a", shared.clone()));
        let node_b = Arc::new(LoadResyncNodeStore::new("node-b", shared.clone()));
        let first = tokio::spawn(process_mrf_file(ReplicationMetadataStore::MRF_REPLICATION_FILE, node_a));
        tokio::time::timeout(Duration::from_secs(1), shared.object_info_started.notified())
            .await
            .expect("first node should start replay work");
        let second = tokio::spawn(process_mrf_file(ReplicationMetadataStore::MRF_REPLICATION_FILE, node_b));

        shared.allow_object_info.notify_one();
        let (first, second) = tokio::join!(first, second);

        assert_eq!(
            first.expect("first replay task should finish") + second.expect("second replay task should finish"),
            1
        );
        assert_eq!(shared.object_info_count.load(Ordering::SeqCst), 1);
        let encoded = shared.data.lock().expect("test MRF data lock should not be poisoned").clone();
        assert!(decode_mrf_file(&encoded).expect("acknowledged MRF should decode").is_empty());
    }

    /// Ported unchanged from #5659: these pin the guarantee, not the
    /// implementation, so they keep holding across this branch's MRF rewrite.
    #[test]
    fn mrf_object_replay_source_lookup_discards_missing_objects_and_retries_transient_errors() {
        assert!(!should_retry_mrf_source_lookup(&EcstoreError::FileNotFound));
        assert!(!should_retry_mrf_source_lookup(&EcstoreError::FileVersionNotFound));
        assert!(!should_retry_mrf_source_lookup(&EcstoreError::VersionNotFound(
            "bucket".to_string(),
            "object".to_string(),
            "version".to_string(),
        )));
        assert!(should_retry_mrf_source_lookup(&EcstoreError::Unexpected));
    }

    #[test]
    fn mrf_metadata_replay_source_lookup_discards_missing_objects_and_retries_transient_errors() {
        for error in [EcstoreError::FileNotFound, EcstoreError::FileVersionNotFound] {
            assert!(!should_retry_mrf_source_lookup(&error));
        }
        assert!(should_retry_mrf_source_lookup(&EcstoreError::Unexpected));
    }

    #[tokio::test]
    async fn corrupt_mrf_is_not_overwritten_by_merge() {
        let shared = empty_resync_shared_state();
        let corrupt = vec![0xde, 0xad, 0xbe, 0xef];
        *shared.data.lock().expect("test MRF data lock should not be poisoned") = corrupt.clone();
        let storage = Arc::new(LoadResyncNodeStore::new("node-a", shared.clone()));
        let entry = MrfReplicateEntry {
            bucket: "source".to_string(),
            object: "new".to_string(),
            version_id: None,
            retry_count: 1,
            size: 1,
            op: MrfOpKind::Object,
            force_delete: false,
            delete_marker_version_id: None,
            delete_marker: false,
            delete_marker_mtime: None,
            target_arns: Vec::new(),
            target_delete_marker_version_id: None,
            source_mod_time: None,
            enqueued_order: None,
            ..Default::default()
        };

        let result =
            merge_mrf_entries_to_disk(ReplicationMetadataStore::MRF_REPLICATION_FILE, std::slice::from_ref(&entry), &storage)
                .await;

        assert!(result.is_none());
        assert_eq!(*shared.data.lock().expect("test MRF data lock should not be poisoned"), corrupt);
        assert_eq!(shared.write_count.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn malformed_targeted_mrf_is_not_overwritten_by_merge() {
        let shared = empty_resync_shared_state();
        let malformed = MrfReplicateEntry {
            bucket: "source".to_string(),
            object: "malformed".to_string(),
            version_id: None,
            retry_count: 1,
            size: 1,
            op: MrfOpKind::Delete,
            force_delete: false,
            delete_marker_version_id: None,
            delete_marker: true,
            delete_marker_mtime: None,
            target_arns: vec!["arn:target-a".to_string(), "arn:target-b".to_string()],
            target_delete_marker_version_id: Some("opaque-target-marker".to_string()),
            source_mod_time: None,
            enqueued_order: None,
            ..Default::default()
        };
        let malformed_data = encode_mrf_file(std::slice::from_ref(&malformed)).expect("malformed entry should encode");
        *shared
            .targeted_mrf_data
            .lock()
            .expect("test targeted MRF data lock should not be poisoned") = malformed_data.clone();
        let storage = Arc::new(LoadResyncNodeStore::new("node-a", shared.clone()));
        let valid = MrfReplicateEntry {
            object: "valid".to_string(),
            delete_marker_version_id: Some(Uuid::from_u128(1)),
            target_arns: vec!["arn:target-a".to_string()],
            target_delete_marker_version_id: Some("opaque-target-marker".to_string()),
            ..malformed
        };

        let result = merge_mrf_entries_to_disk(
            ReplicationMetadataStore::TARGETED_MRF_REPLICATION_FILE,
            std::slice::from_ref(&valid),
            &storage,
        )
        .await;

        assert!(result.is_none());
        assert_eq!(
            *shared
                .targeted_mrf_data
                .lock()
                .expect("test targeted MRF data lock should not be poisoned"),
            malformed_data
        );
        assert_eq!(shared.write_count.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn mrf_processor_start_is_explicit_and_once_only() {
        let pool = new_test_replication_pool(Arc::new(LoadResyncNodeStore::new("node-a", empty_resync_shared_state()))).await;
        assert!(!pool.mrf_processor_started.load(Ordering::Acquire));

        pool.start_mrf_processor().await;
        pool.start_mrf_processor().await;

        assert!(pool.mrf_processor_started.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn replication_backlog_guard_decrements_on_drop() {
        let stats = Arc::new(ReplicationStats::new());
        stats.inc_q("guard-bucket", 256, false, ReplicationType::Object);
        stats.inc_target_q("guard-bucket", &["arn:rustfs:replication:target-a".to_string()], 256);

        {
            let object = ReplicateObjectInfo {
                bucket: "guard-bucket".to_string(),
                size: 256,
                op_type: ReplicationType::Object,
                dsc: test_replicate_decision(&["arn:rustfs:replication:target-a"]),
                ..Default::default()
            };
            let _guard = ReplicationBacklogGuard::for_object(stats.clone(), &object);
        }

        let queued = stats.get_latest_replication_stats("guard-bucket").await;
        assert_eq!(queued.replication_stats.q_stat.curr.count, 0);
        assert_eq!(queued.replication_stats.q_stat.curr.bytes, 0);
        assert!(stats.runtime_target_backlog_snapshot().is_empty());
    }

    #[test]
    fn dec_mrf_entries_decrements_target_backlog() {
        let stats = ReplicationStats::new();
        let entry = MrfReplicateEntry {
            bucket: "mrf-target-drain-bucket".to_string(),
            object: "object".to_string(),
            version_id: None,
            retry_count: 1,
            size: 1024,
            op: MrfOpKind::Object,
            force_delete: false,
            delete_marker_version_id: None,
            delete_marker: false,
            delete_marker_mtime: None,
            target_arns: vec!["arn:rustfs:replication:target-a".to_string()],
            ..Default::default()
        };

        stats.inc_q(&entry.bucket, entry.size, false, ReplicationType::Heal);
        stats.inc_target_q(&entry.bucket, &entry.target_arns, entry.size);
        dec_mrf_entries(&stats, std::slice::from_ref(&entry));

        assert!(stats.runtime_target_backlog_snapshot().is_empty());
    }

    #[test]
    fn mrf_observability_tracker_separates_pending_drop_miss_and_flush_failure() {
        let first = MrfReplicateEntry {
            bucket: "tracker-bucket".to_string(),
            object: "first".to_string(),
            version_id: None,
            retry_count: 1,
            size: 1024,
            op: MrfOpKind::Object,
            force_delete: false,
            delete_marker_version_id: None,
            delete_marker: false,
            delete_marker_mtime: None,
            target_arns: Vec::new(),
            ..Default::default()
        };
        let second = MrfReplicateEntry {
            object: "second".to_string(),
            size: 512,
            ..first.clone()
        };
        let mut tracker = MrfBacklogObservabilityTracker::default();

        tracker.add_pending(&first);
        tracker.add_pending(&second);
        tracker.record_drop(&second);
        tracker.record_missed("tracker-bucket");
        tracker.record_flush_failure(7);
        tracker.flush_pending_entries([&first], 11);

        let snapshot = tracker.snapshot();
        let bucket = snapshot
            .buckets
            .iter()
            .find(|stats| stats.bucket == "tracker-bucket")
            .expect("tracker bucket should be present");
        assert_eq!(bucket.pending_count, 1);
        assert_eq!(bucket.pending_bytes, 512);
        assert_eq!(bucket.dropped_count, 1);
        assert_eq!(bucket.missed_count, 1);
        assert_eq!(bucket.flush_failure_count, 1);
        assert_eq!(bucket.last_flush_duration_millis, 11);
    }

    #[test]
    fn auto_resume_resync_only_for_inflight_states() {
        assert!(should_auto_resume_resync(ResyncStatusType::ResyncPending));
        assert!(should_auto_resume_resync(ResyncStatusType::ResyncStarted));
        assert!(!should_auto_resume_resync(ResyncStatusType::NoResync));
        assert!(!should_auto_resume_resync(ResyncStatusType::ResyncCanceled));
        assert!(!should_auto_resume_resync(ResyncStatusType::ResyncCompleted));
        assert!(!should_auto_resume_resync(ResyncStatusType::ResyncFailed));
    }

    #[tokio::test]
    async fn load_resync_leader_lock_allows_only_one_startup_recovery() {
        temp_env::async_with_vars([(rustfs_config::ENV_OBJECT_LOCK_ACQUIRE_TIMEOUT, Some("1"))], async {
            let shared = Arc::new(LoadResyncSharedState {
                data: StdMutex::new(load_resync_test_metadata()),
                targeted_mrf_data: StdMutex::new(Vec::new()),
                lock_manager: Arc::new(rustfs_lock::GlobalLockManager::new()),
                first_read_started: Notify::new(),
                delay_first_read: AtomicBool::new(true),
                read_count: AtomicUsize::new(0),
                write_count: AtomicUsize::new(0),
                fail_next_write: AtomicBool::new(false),
                block_next_write: AtomicBool::new(false),
                write_started: Notify::new(),
                allow_write: Notify::new(),
                object_info_count: AtomicUsize::new(0),
                delete_object_count: AtomicUsize::new(0),
                block_next_object_info: AtomicBool::new(false),
                object_info_started: Notify::new(),
                allow_object_info: Notify::new(),
            });
            let leader_pool = new_test_replication_pool(Arc::new(LoadResyncNodeStore::new("node-a", shared.clone()))).await;
            let skipped_pool = new_test_replication_pool(Arc::new(LoadResyncNodeStore::new("node-b", shared.clone()))).await;

            let leader = leader_pool.clone();
            let leader_task = tokio::spawn(async move {
                let buckets = vec!["load-resync-lock".to_string()];
                leader.load_resync(&buckets, CancellationToken::new()).await
            });

            tokio::time::timeout(Duration::from_secs(1), shared.first_read_started.notified())
                .await
                .expect("leader should start reading persisted resync metadata");

            let buckets = vec!["load-resync-lock".to_string()];
            skipped_pool
                .clone()
                .load_resync(&buckets, CancellationToken::new())
                .await
                .expect("contended load_resync should skip without failing startup");

            leader_task
                .await
                .expect("leader load_resync task should not panic")
                .expect("leader load_resync should succeed");

            assert_eq!(
                shared.read_count.load(Ordering::SeqCst),
                1,
                "only the leader node should read persisted resync metadata"
            );
            assert!(
                leader_pool.resyncer.status_map.read().await.contains_key("load-resync-lock"),
                "leader node should recover persisted resync status"
            );
            assert!(
                skipped_pool.resyncer.status_map.read().await.is_empty(),
                "node that does not hold the leader lock must not populate status_map"
            );
        })
        .await;
    }

    // ── MrfReplicateEntry encode/decode roundtrips ────────────────────────────

    #[test]
    fn mrf_entry_object_roundtrip() {
        let vid = Uuid::new_v4();
        let entry = MrfReplicateEntry {
            bucket: "my-bucket".to_string(),
            object: "path/to/obj".to_string(),
            version_id: Some(vid),
            retry_count: 3,
            size: 1024,
            op: MrfOpKind::Object,
            force_delete: false,
            delete_marker_version_id: None,
            delete_marker: false,
            delete_marker_mtime: None,
            target_arns: Vec::new(),
            ..Default::default()
        };

        let encoded = encode_mrf_file(std::slice::from_ref(&entry)).expect("encode");
        let decoded = decode_mrf_file(&encoded).expect("decode");

        assert_eq!(decoded.len(), 1);
        let got = &decoded[0];
        assert_eq!(got.bucket, "my-bucket");
        assert_eq!(got.object, "path/to/obj");
        assert_eq!(got.version_id, Some(vid));
        assert_eq!(got.retry_count, 3);
        assert_eq!(got.size, 1024);
        assert_eq!(got.op, MrfOpKind::Object);
        assert_eq!(got.delete_marker_version_id, None);
        assert!(!got.delete_marker);
    }

    #[test]
    fn mrf_entry_delete_marker_roundtrip() {
        let dm_vid = Uuid::new_v4();
        // A specific, non-now() nanosecond timestamp: replay must preserve this exact value
        // instead of stamping the replica with the current time (backlog#867).
        let mtime_nanos = 1_705_312_200_123_456_789i64;
        let entry = MrfReplicateEntry {
            bucket: "del-bucket".to_string(),
            object: "key".to_string(),
            version_id: None,
            retry_count: 0,
            size: 0,
            op: MrfOpKind::Delete,
            force_delete: false,
            delete_marker_version_id: Some(dm_vid),
            delete_marker: true,
            delete_marker_mtime: Some(mtime_nanos),
            target_arns: Vec::new(),
            ..Default::default()
        };

        let encoded = encode_mrf_file(std::slice::from_ref(&entry)).expect("encode");
        let decoded = decode_mrf_file(&encoded).expect("decode");

        assert_eq!(decoded.len(), 1);
        let got = &decoded[0];
        assert_eq!(got.bucket, "del-bucket");
        assert_eq!(got.object, "key");
        assert_eq!(got.version_id, None);
        assert_eq!(got.op, MrfOpKind::Delete);
        assert_eq!(got.delete_marker_version_id, Some(dm_vid));
        assert!(got.delete_marker);
        assert_eq!(
            got.delete_marker_mtime,
            Some(mtime_nanos),
            "delete-marker mtime must survive the MRF disk round-trip"
        );
    }

    #[test]
    fn mrf_entry_versioned_delete_roundtrip() {
        let vid = Uuid::new_v4();
        let entry = MrfReplicateEntry {
            bucket: "ver-bucket".to_string(),
            object: "versioned-key".to_string(),
            version_id: Some(vid),
            retry_count: 0,
            size: 0,
            op: MrfOpKind::Delete,
            force_delete: false,
            delete_marker_version_id: None,
            delete_marker: false,
            delete_marker_mtime: None,
            target_arns: Vec::new(),
            ..Default::default()
        };

        let encoded = encode_mrf_file(&[entry]).expect("encode");
        let decoded = decode_mrf_file(&encoded).expect("decode");

        assert_eq!(decoded.len(), 1);
        let got = &decoded[0];
        assert_eq!(got.op, MrfOpKind::Delete);
        assert_eq!(got.version_id, Some(vid));
        assert_eq!(got.delete_marker_version_id, None);
        assert!(!got.delete_marker);
    }

    #[test]
    fn mrf_entry_mixed_batch_roundtrip() {
        let obj_vid = Uuid::new_v4();
        let del_dm_vid = Uuid::new_v4();
        let entries = vec![
            MrfReplicateEntry {
                bucket: "b".to_string(),
                object: "obj".to_string(),
                version_id: Some(obj_vid),
                retry_count: 1,
                size: 512,
                op: MrfOpKind::Object,
                force_delete: false,
                delete_marker_version_id: None,
                delete_marker: false,
                delete_marker_mtime: None,
                target_arns: Vec::new(),
                ..Default::default()
            },
            MrfReplicateEntry {
                bucket: "b".to_string(),
                object: "del".to_string(),
                version_id: None,
                retry_count: 0,
                size: 0,
                op: MrfOpKind::Delete,
                force_delete: false,
                delete_marker_version_id: Some(del_dm_vid),
                delete_marker: true,
                delete_marker_mtime: None,
                target_arns: Vec::new(),
                ..Default::default()
            },
        ];

        let encoded = encode_mrf_file(&entries).expect("encode");
        let decoded = decode_mrf_file(&encoded).expect("decode");

        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[0].op, MrfOpKind::Object);
        assert_eq!(decoded[0].version_id, Some(obj_vid));
        assert_eq!(decoded[1].op, MrfOpKind::Delete);
        assert_eq!(decoded[1].delete_marker_version_id, Some(del_dm_vid));
        assert!(decoded[1].delete_marker);
    }

    // ── Recovery replay routing ───────────────────────────────────────────────

    #[test]
    fn targeted_mrf_delete_recovery_stays_target_specific_across_replays() {
        let delete_marker_version_id = Uuid::new_v4();
        let target_arn = "arn:rustfs:replication:target-b";
        let target_delete_marker_version_id = "opaque-target-marker";
        let entry = MrfReplicateEntry {
            bucket: "source".to_string(),
            object: "object".to_string(),
            version_id: None,
            retry_count: 0,
            size: 0,
            op: MrfOpKind::Delete,
            force_delete: false,
            delete_marker_version_id: Some(delete_marker_version_id),
            delete_marker: true,
            delete_marker_mtime: None,
            target_arns: vec![target_arn.to_string()],
            target_delete_marker_version_id: Some(target_delete_marker_version_id.to_string()),
            source_mod_time: None,
            enqueued_order: None,
            ..Default::default()
        };
        let persisted = encode_mrf_file(std::slice::from_ref(&entry)).expect("targeted MRF delete should encode");
        let recovered = decode_mrf_file(&persisted).expect("targeted MRF delete should decode");
        let delete_object = ReplicationDeletedObject {
            object_name: entry.object.clone(),
            delete_marker_version_id: entry.delete_marker_version_id,
            delete_marker: true,
            ..Default::default()
        };

        let first_replay = recovered_mrf_delete_infos(&recovered[0], delete_object.clone());
        assert_eq!(first_replay.len(), 1);
        assert_eq!(first_replay[0].target_arn, target_arn);
        assert_eq!(
            first_replay[0].target_delete_marker_version_id.as_deref(),
            Some(target_delete_marker_version_id)
        );

        let retry_entry = first_replay[0].to_mrf_entry();
        let second_replay = recovered_mrf_delete_infos(&retry_entry, delete_object);
        assert_eq!(second_replay.len(), 1);
        assert_eq!(second_replay[0].target_arn, target_arn);
        assert_eq!(second_replay[0].delete_object.delete_marker_version_id, Some(delete_marker_version_id));
        assert_eq!(
            second_replay[0].target_delete_marker_version_id.as_deref(),
            Some(target_delete_marker_version_id)
        );
    }

    #[test]
    fn targeted_mrf_delete_recovery_normalizes_targets() {
        let entry = MrfReplicateEntry {
            bucket: "source".to_string(),
            object: "object".to_string(),
            version_id: None,
            retry_count: 0,
            size: 0,
            op: MrfOpKind::Delete,
            force_delete: false,
            delete_marker_version_id: None,
            delete_marker: true,
            delete_marker_mtime: None,
            target_arns: vec![
                "arn:target-b".to_string(),
                String::new(),
                "arn:target-a".to_string(),
                "arn:target-b".to_string(),
            ],
            target_delete_marker_version_id: None,
            source_mod_time: None,
            enqueued_order: None,
            ..Default::default()
        };

        let replay = recovered_mrf_delete_infos(
            &entry,
            ReplicationDeletedObject {
                object_name: entry.object.clone(),
                delete_marker: true,
                ..Default::default()
            },
        );

        assert_eq!(
            replay.iter().map(|delete| delete.target_arn.as_str()).collect::<Vec<_>>(),
            vec!["arn:target-a", "arn:target-b"]
        );
        let retried = recovered_mrf_delete_infos(&replay[1].to_mrf_entry(), replay[1].delete_object.clone());
        assert_eq!(retried.len(), 1);
        assert_eq!(retried[0].target_arn, "arn:target-b");
    }

    #[test]
    fn targeted_delete_mrf_isolated_from_legacy_file() {
        let targeted = MrfReplicateEntry {
            bucket: "source".to_string(),
            object: "object".to_string(),
            version_id: None,
            retry_count: 0,
            size: 0,
            op: MrfOpKind::Delete,
            force_delete: false,
            delete_marker_version_id: Some(Uuid::from_u128(1)),
            delete_marker: true,
            delete_marker_mtime: None,
            target_arns: vec!["arn:target-a".to_string()],
            target_delete_marker_version_id: None,
            source_mod_time: None,
            enqueued_order: None,
            ..Default::default()
        };
        let legacy = MrfReplicateEntry {
            target_arns: Vec::new(),
            ..targeted.clone()
        };

        assert_eq!(mrf_file_for_entry(&targeted), ReplicationMetadataStore::TARGETED_MRF_REPLICATION_FILE);
        assert_eq!(mrf_file_for_entry(&legacy), ReplicationMetadataStore::MRF_REPLICATION_FILE);
        assert!(targeted_mrf_entry_is_valid(&targeted));

        let ambiguous = MrfReplicateEntry {
            target_arns: vec!["arn:target-a".to_string(), "arn:target-b".to_string()],
            target_delete_marker_version_id: Some("opaque-target-marker".to_string()),
            ..targeted.clone()
        };
        assert!(!targeted_mrf_entry_is_valid(&ambiguous));

        let oversized_target = MrfReplicateEntry {
            target_arns: vec!["a".repeat(MAX_MRF_TARGET_FIELD_LEN + 1)],
            target_delete_marker_version_id: None,
            ..ambiguous
        };
        assert!(!targeted_mrf_entry_is_valid(&oversized_target));

        let missing_source_marker_id = MrfReplicateEntry {
            delete_marker_version_id: None,
            ..targeted.clone()
        };
        assert!(!targeted_mrf_entry_is_valid(&missing_source_marker_id));

        let bare_delete = MrfReplicateEntry {
            delete_marker: false,
            delete_marker_version_id: None,
            ..targeted.clone()
        };
        assert!(!targeted_mrf_entry_is_valid(&bare_delete));

        let empty_target_version = MrfReplicateEntry {
            target_delete_marker_version_id: Some(String::new()),
            ..targeted.clone()
        };
        assert!(!targeted_mrf_entry_is_valid(&empty_target_version));

        let oversized_target_version = MrfReplicateEntry {
            target_delete_marker_version_id: Some("v".repeat(MAX_MRF_TARGET_FIELD_LEN + 1)),
            ..targeted
        };
        assert!(!targeted_mrf_entry_is_valid(&oversized_target_version));
    }

    #[test]
    fn legacy_mrf_delete_recovery_keeps_live_config_fallback() {
        let entry = MrfReplicateEntry {
            bucket: "source".to_string(),
            object: "object".to_string(),
            version_id: None,
            retry_count: 0,
            size: 0,
            op: MrfOpKind::Delete,
            force_delete: false,
            delete_marker_version_id: Some(Uuid::new_v4()),
            delete_marker: true,
            delete_marker_mtime: None,
            target_arns: Vec::new(),
            target_delete_marker_version_id: None,
            source_mod_time: None,
            enqueued_order: None,
            ..Default::default()
        };

        let replay = recovered_mrf_delete_infos(
            &entry,
            ReplicationDeletedObject {
                object_name: entry.object.clone(),
                delete_marker_version_id: entry.delete_marker_version_id,
                delete_marker: true,
                ..Default::default()
            },
        );

        assert_eq!(replay.len(), 1);
        assert!(replay[0].target_arn.is_empty());
    }

    #[test]
    fn mrf_entry_op_routes_correctly() {
        // Object entries must have op=Object so the processor calls get_object_info + heal.
        let obj_entry = MrfReplicateEntry {
            bucket: "b".to_string(),
            object: "o".to_string(),
            version_id: None,
            retry_count: 0,
            size: 0,
            op: MrfOpKind::Object,
            force_delete: false,
            delete_marker_version_id: None,
            delete_marker: false,
            delete_marker_mtime: None,
            target_arns: Vec::new(),
            ..Default::default()
        };
        assert_eq!(obj_entry.op, MrfOpKind::Object);

        // Delete entries must have op=Delete so the processor calls schedule_replication_delete.
        let del_entry = MrfReplicateEntry {
            bucket: "b".to_string(),
            object: "o".to_string(),
            version_id: None,
            retry_count: 0,
            size: 0,
            op: MrfOpKind::Delete,
            force_delete: false,
            delete_marker_version_id: Some(Uuid::new_v4()),
            delete_marker: true,
            delete_marker_mtime: None,
            target_arns: Vec::new(),
            ..Default::default()
        };
        assert_eq!(del_entry.op, MrfOpKind::Delete);

        // Entries written by old code (before the op field existed) must deserialise as Object
        // so existing recovery behaviour is preserved.
        let legacy_entry = MrfReplicateEntry {
            bucket: "b".to_string(),
            object: "o".to_string(),
            version_id: None,
            retry_count: 0,
            size: 0,
            op: MrfOpKind::default(),
            force_delete: false,
            delete_marker_version_id: None,
            delete_marker: false,
            delete_marker_mtime: None,
            target_arns: Vec::new(),
            ..Default::default()
        };
        assert_eq!(legacy_entry.op, MrfOpKind::Object, "legacy default must be Object");
    }

    #[test]
    fn mrf_legacy_file_without_op_field_decoded_as_object() {
        // Hand-build the exact bytes a pre-MrfOpKind binary would have written to disk.
        // The old MrfReplicateEntry had only 4 persisted keys (versionID is omitted when
        // None due to skip_serializing_if): bucket, object, retryCount, size.
        // There is no "op", "deleteMarker", or "deleteMarkerVersionID" key.
        //
        // This proves that #[serde(default)] on the `op` field carries real weight:
        // if you remove that attribute, rmp_serde will return an error on this payload
        // and the test will fail.
        let mut msgpack = Vec::new();
        // Outer: array of 1 (the Vec<MrfReplicateEntry>)
        rmp::encode::write_array_len(&mut msgpack, 1).unwrap();
        // Inner: named map with the 4 original fields only — no "op", no "deleteMarker*"
        rmp::encode::write_map_len(&mut msgpack, 4).unwrap();
        rmp::encode::write_str(&mut msgpack, "bucket").unwrap();
        rmp::encode::write_str(&mut msgpack, "old-bucket").unwrap();
        rmp::encode::write_str(&mut msgpack, "object").unwrap();
        rmp::encode::write_str(&mut msgpack, "old-key").unwrap();
        rmp::encode::write_str(&mut msgpack, "retryCount").unwrap();
        rmp::encode::write_i32(&mut msgpack, 2).unwrap();
        rmp::encode::write_str(&mut msgpack, "size").unwrap();
        rmp::encode::write_i64(&mut msgpack, 100).unwrap();

        // Prepend the MRF file header: format=1 (LE u16) || version=1 (LE u16)
        let mut data = Vec::with_capacity(4 + msgpack.len());
        data.extend_from_slice(&1u16.to_le_bytes()); // MRF_META_FORMAT
        data.extend_from_slice(&1u16.to_le_bytes()); // MRF_META_VERSION
        data.extend_from_slice(&msgpack);

        let decoded = decode_mrf_file(&data).expect("legacy payload must decode without error");
        assert_eq!(decoded.len(), 1);
        let entry = &decoded[0];
        assert_eq!(entry.bucket, "old-bucket");
        assert_eq!(entry.object, "old-key");
        assert_eq!(entry.retry_count, 2);
        assert_eq!(entry.size, 100);
        assert_eq!(entry.version_id, None);
        // The "op" key was absent — #[serde(default)] must fill in MrfOpKind::Object.
        assert_eq!(entry.op, MrfOpKind::Object, "missing op key must default to Object");
        assert!(!entry.delete_marker);
        assert_eq!(entry.delete_marker_version_id, None);
        // The "deleteMarkerMtime" key was absent in old files — #[serde(default)] must fill in
        // None so replay falls back to the current time (backlog#867 backward compatibility).
        assert_eq!(entry.delete_marker_mtime, None, "missing deleteMarkerMtime key must default to None");
        assert!(entry.target_arns.is_empty(), "old MRF entries must not be attributed to a target");
    }

    #[test]
    fn durable_mrf_snapshot_reads_restart_backlog_and_valid_empty_state() {
        let entries = vec![MrfReplicateEntry {
            bucket: "restart-bucket".to_string(),
            object: "object".to_string(),
            version_id: None,
            retry_count: 1,
            size: 512,
            op: MrfOpKind::Object,
            force_delete: false,
            delete_marker_version_id: None,
            delete_marker: false,
            delete_marker_mtime: None,
            target_arns: Vec::new(),
            ..Default::default()
        }];
        let encoded = encode_mrf_file(&entries).expect("durable MRF backlog should encode");

        let recovered = durable_mrf_backlog_from_read(Ok(encoded));
        assert!(recovered.available);
        assert_eq!(recovered.entries.len(), 1);
        assert_eq!(recovered.entries[0].bucket, "restart-bucket");
        assert_eq!(recovered.entries[0].size, 512);

        let missing_file = durable_mrf_backlog_from_read(Err(EcstoreError::ConfigNotFound));
        assert!(missing_file.available);
        assert!(missing_file.entries.is_empty());
    }

    #[test]
    fn durable_mrf_summary_aggregates_entries_by_bucket_for_obs() {
        let snapshot =
            durable_mrf_backlog_summary_from_sizes([("b1".to_string(), 1024), ("b1".to_string(), 512), ("b2".to_string(), 0)]);

        let summary = snapshot.summary;
        assert!(summary.available);
        let buckets = summary
            .buckets
            .into_iter()
            .map(|bucket| (bucket.bucket.clone(), bucket))
            .collect::<HashMap<_, _>>();
        assert_eq!(buckets["b1"].count, 2);
        assert_eq!(buckets["b1"].bytes, 1536);
        assert_eq!(buckets["b2"].count, 1);
        assert_eq!(buckets["b2"].bytes, 0);
        assert!(snapshot.targets.is_empty());
    }

    #[test]
    fn durable_mrf_summary_aggregates_target_backlog_without_attributing_legacy_entries() {
        let entries = vec![
            MrfReplicateEntry {
                bucket: "b1".to_string(),
                object: "object-a".to_string(),
                version_id: None,
                retry_count: 0,
                size: 1024,
                op: MrfOpKind::Object,
                force_delete: false,
                delete_marker_version_id: None,
                delete_marker: false,
                delete_marker_mtime: None,
                target_arns: vec!["arn:target-a".to_string(), "arn:target-b".to_string()],
                ..Default::default()
            },
            MrfReplicateEntry {
                bucket: "b1".to_string(),
                object: "object-b".to_string(),
                version_id: None,
                retry_count: 0,
                size: 512,
                op: MrfOpKind::Object,
                force_delete: false,
                delete_marker_version_id: None,
                delete_marker: false,
                delete_marker_mtime: None,
                target_arns: vec!["arn:target-a".to_string()],
                ..Default::default()
            },
            MrfReplicateEntry {
                bucket: "b1".to_string(),
                object: "legacy-object".to_string(),
                version_id: None,
                retry_count: 0,
                size: 256,
                op: MrfOpKind::Object,
                force_delete: false,
                delete_marker_version_id: None,
                delete_marker: false,
                delete_marker_mtime: None,
                target_arns: Vec::new(),
                ..Default::default()
            },
        ];

        let snapshot = durable_mrf_backlog_summary_from_entries(&entries);

        let summary = snapshot.summary;
        assert!(summary.available);
        let buckets = summary
            .buckets
            .into_iter()
            .map(|bucket| (bucket.bucket.clone(), bucket))
            .collect::<HashMap<_, _>>();
        assert_eq!(buckets["b1"].count, 3);
        assert_eq!(buckets["b1"].bytes, 1792);

        let targets = snapshot
            .targets
            .into_iter()
            .map(|target| ((target.bucket.clone(), target.target_arn.clone()), target))
            .collect::<HashMap<_, _>>();
        let target_a = &targets[&("b1".to_string(), "arn:target-a".to_string())];
        assert_eq!(target_a.count, 2);
        assert_eq!(target_a.bytes, 1536);
        let target_b = &targets[&("b1".to_string(), "arn:target-b".to_string())];
        assert_eq!(target_b.count, 1);
        assert_eq!(target_b.bytes, 1024);
    }

    #[test]
    fn durable_mrf_summary_marks_invalid_sizes_unavailable() {
        let invalid = durable_mrf_backlog_summary_from_sizes([("bucket".to_string(), -1)]);
        let summary = invalid.summary;
        assert!(!summary.available);
        assert!(summary.buckets.is_empty());
        assert!(invalid.targets.is_empty());
    }

    #[test]
    fn durable_mrf_snapshot_marks_corrupt_or_invalid_data_unavailable() {
        let corrupt = durable_mrf_backlog_from_read(Ok(vec![0, 1, 2]));
        assert!(!corrupt.available);
        assert!(corrupt.entries.is_empty());

        let negative = encode_mrf_file(&[MrfReplicateEntry {
            bucket: "bucket".to_string(),
            object: "object".to_string(),
            version_id: None,
            retry_count: 0,
            size: -1,
            op: MrfOpKind::Object,
            force_delete: false,
            delete_marker_version_id: None,
            delete_marker: false,
            delete_marker_mtime: None,
            target_arns: Vec::new(),
            ..Default::default()
        }])
        .expect("invalid persisted entry should still encode for boundary testing");
        let invalid = durable_mrf_backlog_from_read(Ok(negative));
        assert!(!invalid.available);
        assert!(invalid.entries.is_empty());
    }

    #[test]
    fn force_delete_replay_requires_local_commit_and_keeps_persisted_targets() {
        let operation_id = Uuid::new_v4();
        let pending = MrfReplicateEntry {
            bucket: "source".to_string(),
            object: "logs/".to_string(),
            target_arns: vec!["arn:target:old-generation".to_string()],
            force_delete_id: Some(operation_id),
            force_delete_generation: Some(11),
            force_delete_local_commit: false,
            op: MrfOpKind::Delete,
            ..Default::default()
        };
        assert!(!should_replay_force_delete_intent(&pending));

        let mut committed = pending;
        committed.force_delete_local_commit = true;
        let recovered = decode_mrf_file(&encode_mrf_file(&[committed.clone()]).expect("force-delete intent should encode"))
            .expect("force-delete intent should decode");
        assert!(should_replay_force_delete_intent(&recovered[0]));
        assert_eq!(recovered[0].target_arns, vec!["arn:target:old-generation"]);
        assert_eq!(recovered[0].force_delete_generation, Some(11));

        committed.target_arns.clear();
        assert!(!should_replay_force_delete_intent(&committed));
    }

    #[tokio::test]
    async fn force_delete_intent_append_commit_and_cleanup_are_idempotent() {
        let shared = empty_resync_shared_state();
        let storage = Arc::new(LoadResyncNodeStore::new("force-delete-journal", shared));
        let operation_id = Uuid::new_v4();
        let entry = MrfReplicateEntry {
            bucket: "source".to_string(),
            object: "logs/".to_string(),
            target_arns: vec!["arn:target:stable".to_string()],
            force_delete_id: Some(operation_id),
            force_delete_generation: Some(12),
            op: MrfOpKind::Delete,
            ..Default::default()
        };

        persist_force_delete_intent(storage.clone(), entry.clone())
            .await
            .expect("first journal append should succeed");
        persist_force_delete_intent(storage.clone(), entry)
            .await
            .expect("duplicate journal append should be a no-op");

        let data = ReplicationConfigStore::read(storage.clone(), ReplicationMetadataStore::FORCE_DELETE_REPLICATION_FILE)
            .await
            .expect("journal should be readable");
        let entries = decode_mrf_file(&data).expect("journal should decode");
        assert_eq!(entries.len(), 1);
        assert!(!entries[0].force_delete_local_commit);
        assert!(!should_replay_force_delete_intent(&entries[0]));

        commit_force_delete_intent(storage.clone(), operation_id)
            .await
            .expect("commit marker should persist");
        commit_force_delete_intent(storage.clone(), operation_id)
            .await
            .expect("duplicate commit marker should be a no-op");
        let data = ReplicationConfigStore::read(storage.clone(), ReplicationMetadataStore::FORCE_DELETE_REPLICATION_FILE)
            .await
            .expect("committed journal should be readable");
        let entries = decode_mrf_file(&data).expect("committed journal should decode");
        assert!(should_replay_force_delete_intent(&entries[0]));
        assert_eq!(entries[0].target_arns, vec!["arn:target:stable"]);

        complete_force_delete_intent(storage.clone(), operation_id)
            .await
            .expect("journal cleanup should succeed");
        complete_force_delete_intent(storage, operation_id)
            .await
            .expect("duplicate journal cleanup should be a no-op");
    }
}
