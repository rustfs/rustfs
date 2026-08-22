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
use super::replication_object_config::{ReplicationConfig, check_replicate_delete_strict, must_replicate};
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
    BucketReplicationResyncStatus, RESYNC_FILE_MAX_BYTES, ResyncOpts, TargetReplicationResyncStatus, decode_mrf_file,
    decode_resync_file, encode_mrf_file, should_auto_resume_resync,
};
use super::replication_resyncer::{
    ReplicationResyncer, get_heal_replicate_object_info, replicate_delete, replicate_delete_with_outcome, replicate_object,
    replicate_object_with_outcome, save_resync_status,
};
use super::replication_state::ReplicationStats;
use super::replication_storage_boundary::{
    HTTPPreconditions, ObjectInfo, ObjectOptions, ObjectToDelete, ReplicationDeletedObject, ReplicationObjectIO,
    ReplicationStorage,
};
use super::replication_target_boundary::{ReplicationTargetStore, replication_object_is_ssec_encrypted};
use super::replication_versioning_boundary::ReplicationVersioningStore;
use super::runtime_boundary as runtime_sources;
use futures_util::stream::{self, StreamExt};
use metrics::{counter, histogram};
use rustfs_utils::hash::HashAlgorithm;
use rustfs_utils::http::{SUFFIX_REPLICATION_TIMESTAMP, get_str};
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::Arc;
use std::sync::LazyLock;
use std::sync::RwLock as StdRwLock;
use std::sync::atomic::AtomicI32;
use std::sync::atomic::Ordering;
use std::time::Instant;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use tokio::sync::Mutex;
use tokio::sync::RwLock;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::Sender;
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
const MRF_CORRUPT_FILE_PREFIX: &str = "config/replication/mrf.corrupt";
const MRF_PENDING_CAP: usize = 200_000;
const MRF_RETRY_INITIAL_DELAY: Duration = Duration::from_millis(100);
const MRF_RETRY_MAX_DELAY: Duration = Duration::from_secs(5);

#[derive(Debug, Default)]
pub struct DurableMrfBacklog {
    pub available: bool,
    pub entries: Vec<MrfReplicateEntry>,
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

#[allow(
    dead_code,
    reason = "MinIO-parity replication surface with no caller in this port (backlog#1823)"
)]
fn durable_mrf_backlog_tracker_from_entries(entries: &[MrfReplicateEntry]) -> DurableMrfBacklogTracker {
    let mut tracker = DurableMrfBacklogTracker {
        available: true,
        ..Default::default()
    };
    for entry in entries {
        tracker.add_entry(entry);
    }
    tracker
}

#[derive(Debug)]
struct PendingMrfAppend {
    digest: [u8; 32],
    entry_count: usize,
}

#[derive(Debug)]
struct MrfAppendResult {
    duration_millis: u64,
    backlog: DurableMrfBacklogSnapshot,
}

fn mrf_payload_digest(data: &[u8]) -> [u8; 32] {
    let encoded = HashAlgorithm::SHA256.hash_encode(data);
    let mut digest = [0; 32];
    digest.copy_from_slice(encoded.as_ref());
    digest
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

    #[cfg(test)]
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

fn set_durable_mrf_backlog_summary(summary: DurableMrfBacklogSummary) {
    set_durable_mrf_backlog_snapshot(DurableMrfBacklogSnapshot {
        summary,
        targets: Vec::new(),
    });
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
    durable_mrf_backlog_from_read(ReplicationConfigStore::read(storage, ReplicationMetadataStore::MRF_REPLICATION_FILE).await)
}

pub async fn persist_force_delete_intent<S: ReplicationStorage>(
    storage: Arc<S>,
    mut entry: MrfReplicateEntry,
) -> Result<(), EcstoreError> {
    entry.force_delete_local_commit = false;
    update_force_delete_intents(storage, move |entries, _exists| {
        if entries
            .iter()
            .any(|existing| existing.force_delete_id == entry.force_delete_id)
        {
            return Ok(false);
        }
        entries.push(entry.clone());
        Ok(true)
    })
    .await
}

pub async fn commit_force_delete_intent<S: ReplicationStorage>(
    storage: Arc<S>,
    operation_id: uuid::Uuid,
) -> Result<(), EcstoreError> {
    update_force_delete_intents(storage, move |entries, exists| {
        if !exists {
            return Err(EcstoreError::ConfigNotFound);
        }
        let Some(entry) = entries.iter_mut().find(|entry| entry.force_delete_id == Some(operation_id)) else {
            return Err(EcstoreError::ConfigNotFound);
        };
        if entry.force_delete_local_commit {
            return Ok(false);
        }
        entry.force_delete_local_commit = true;
        Ok(true)
    })
    .await
}

pub async fn complete_force_delete_intent<S: ReplicationStorage>(
    storage: Arc<S>,
    operation_id: uuid::Uuid,
) -> Result<(), EcstoreError> {
    update_force_delete_intents(storage, move |entries, exists| {
        if !exists {
            return Ok(false);
        }
        let original_len = entries.len();
        entries.retain(|entry| entry.force_delete_id != Some(operation_id));
        Ok(entries.len() != original_len)
    })
    .await
}

const FORCE_DELETE_INTENT_CAS_RETRIES: usize = 3;

fn is_retryable_force_delete_error(error: &EcstoreError) -> bool {
    matches!(error, EcstoreError::PreconditionFailed) || error.to_string().contains("force-delete journal lock lost")
}

async fn update_force_delete_intents<S, F>(storage: Arc<S>, mut update: F) -> Result<(), EcstoreError>
where
    S: ReplicationStorage,
    F: FnMut(&mut Vec<MrfReplicateEntry>, bool) -> Result<bool, EcstoreError>,
{
    let file = ReplicationMetadataStore::FORCE_DELETE_REPLICATION_FILE;
    for attempt in 0..=FORCE_DELETE_INTENT_CAS_RETRIES {
        let result = {
            let lock = storage
                .new_ns_lock(
                    ReplicationMetadataStore::rustfs_meta_bucket(),
                    ReplicationMetadataStore::FORCE_DELETE_REPLICATION_TRANSACTION_LOCK,
                )
                .await?;
            // Lock order is transaction namespace lock -> force-delete journal object lock.
            // Keep the transaction guard alive through the conditional write so legacy
            // writers cannot interleave a read-modify-write transition within this process.
            let guard = lock.get_write_lock(ReplicationLockTiming::acquire_timeout()).await?;
            let (mut entries, preconditions, exists) = read_force_delete_intents(storage.clone(), file).await?;
            if !update(&mut entries, exists)? {
                return Ok(());
            }
            save_force_delete_intents(storage.clone(), file, &guard, entries, preconditions).await
        };

        match result {
            Err(error) if is_retryable_force_delete_error(&error) && attempt < FORCE_DELETE_INTENT_CAS_RETRIES => {
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
            result => return result,
        }
    }
    Err(EcstoreError::other("force-delete journal update retries exhausted"))
}

async fn read_force_delete_intents<S: ReplicationObjectIO>(
    storage: Arc<S>,
    file: &str,
) -> Result<(Vec<MrfReplicateEntry>, HTTPPreconditions, bool), EcstoreError> {
    match ReplicationConfigStore::read_no_lock_with_metadata(storage, file).await {
        Ok((data, object_info)) => {
            let etag = object_info
                .etag
                .filter(|etag| !etag.trim().is_empty())
                .ok_or_else(|| EcstoreError::other("force-delete journal has no ETag for conditional update"))?;
            Ok((
                decode_mrf_file(&data)?,
                HTTPPreconditions {
                    if_match: Some(etag),
                    ..Default::default()
                },
                true,
            ))
        }
        Err(EcstoreError::ConfigNotFound) => Ok((
            Vec::new(),
            HTTPPreconditions {
                if_none_match: Some("*".to_string()),
                ..Default::default()
            },
            false,
        )),
        Err(err) => Err(err),
    }
}

async fn save_force_delete_intents<S: ReplicationStorage>(
    storage: Arc<S>,
    file: &str,
    guard: &rustfs_lock::NamespaceLockGuard,
    entries: Vec<MrfReplicateEntry>,
    preconditions: HTTPPreconditions,
) -> Result<(), EcstoreError> {
    ensure_force_delete_journal_lock_held(guard.is_lock_lost())?;
    ReplicationConfigStore::save_conditional(storage, file, encode_mrf_file(&entries)?, preconditions).await
}

fn ensure_force_delete_journal_lock_held(lock_lost: bool) -> Result<(), EcstoreError> {
    if lock_lost {
        return Err(EcstoreError::other("force-delete journal lock lost before conditional update"));
    }
    Ok(())
}

fn ensure_mrf_journal_lock_held(lock_lost: bool) -> Result<(), EcstoreError> {
    if lock_lost {
        return Err(EcstoreError::other("MRF journal lock lost before conditional update"));
    }
    Ok(())
}

fn mrf_journal_preconditions(etag: Option<&str>, exists: bool) -> Option<HTTPPreconditions> {
    if exists {
        etag.filter(|value| !value.trim().is_empty()).map(|etag| HTTPPreconditions {
            if_match: Some(etag.to_string()),
            ..Default::default()
        })
    } else {
        Some(HTTPPreconditions {
            if_none_match: Some("*".to_string()),
            ..Default::default()
        })
    }
}

fn mrf_prefix_matches(current: &[MrfReplicateEntry], prefix: &[MrfReplicateEntry]) -> bool {
    current.starts_with(prefix)
}

async fn read_mrf_entries<S: ReplicationObjectIO>(storage: Arc<S>) -> Result<Vec<MrfReplicateEntry>, EcstoreError> {
    match ReplicationConfigStore::read(storage, ReplicationMetadataStore::MRF_REPLICATION_FILE).await {
        Ok(data) if data.is_empty() => Ok(Vec::new()),
        Ok(data) => decode_mrf_file(&data),
        Err(EcstoreError::ConfigNotFound) => Ok(Vec::new()),
        Err(error) => Err(error),
    }
}

/// Acknowledge only the generation read by the recovery leader. Entries appended
/// after that generation are retained as a suffix and replayed on the next startup.
/// Lock order: recovery leader lock -> MRF journal object lock.
async fn acknowledge_mrf_recovery<S: ReplicationStorage>(
    storage: Arc<S>,
    recovery_guard: &rustfs_lock::NamespaceLockGuard,
    replayed_prefix: &[MrfReplicateEntry],
    retry_entries: &[MrfReplicateEntry],
) -> Result<Vec<MrfReplicateEntry>, EcstoreError> {
    let file = ReplicationMetadataStore::MRF_REPLICATION_FILE;
    for _attempt in 0..=FORCE_DELETE_INTENT_CAS_RETRIES {
        let lock = storage
            .new_ns_lock(ReplicationMetadataStore::rustfs_meta_bucket(), file)
            .await?;
        let guard = lock.get_write_lock(ReplicationLockTiming::acquire_timeout()).await?;
        let current = ReplicationConfigStore::read_no_lock_with_metadata_preserve_empty(storage.clone(), file).await;
        let (current_data, current_etag, current_exists) = match current {
            Ok((data, object_info)) => (data, object_info.etag, true),
            Err(EcstoreError::ConfigNotFound) => (Vec::new(), None, false),
            Err(error) => return Err(error),
        };
        let current_entries = if current_data.is_empty() {
            Vec::new()
        } else {
            decode_mrf_file(&current_data)?
        };
        if !mrf_prefix_matches(&current_entries, replayed_prefix) {
            return Err(EcstoreError::other("MRF recovery prefix changed before acknowledgement"));
        }

        let mut retained = Vec::with_capacity(retry_entries.len() + current_entries.len().saturating_sub(replayed_prefix.len()));
        retained.extend_from_slice(retry_entries);
        retained.extend_from_slice(&current_entries[replayed_prefix.len()..]);
        if recovery_guard.is_lock_lost() || guard.is_lock_lost() {
            return Err(EcstoreError::other("MRF recovery lock lost before acknowledgement"));
        }
        let Some(preconditions) = mrf_journal_preconditions(current_etag.as_deref(), current_exists) else {
            return Err(EcstoreError::other("MRF journal has no ETag for recovery acknowledgement"));
        };
        let data = if retained.is_empty() {
            Vec::new()
        } else {
            encode_mrf_file(&retained)?
        };
        match ReplicationConfigStore::save_conditional_no_lock(storage.clone(), file, data, preconditions).await {
            Ok(()) => return Ok(retained),
            Err(EcstoreError::PreconditionFailed) => continue,
            Err(error) => return Err(error),
        }
    }
    Err(EcstoreError::PreconditionFailed)
}

/// Acquires the MRF recovery leader lock for the startup replay.
/// Returns `None` (after logging) when the lock cannot be created or another
/// node is already processing the backlog.
async fn acquire_mrf_recovery_guard<S: ReplicationStorage>(storage: &Arc<S>) -> Option<rustfs_lock::NamespaceLockGuard> {
    let recovery_lock = match storage
        .new_ns_lock(
            ReplicationMetadataStore::rustfs_meta_bucket(),
            ReplicationMetadataStore::MRF_REPLICATION_RECOVERY_LOCK,
        )
        .await
    {
        Ok(lock) => lock,
        Err(error) => {
            warn!(
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_REPLICATION,
                error = %error,
                "Failed to create the MRF recovery leader lock"
            );
            return None;
        }
    };
    match recovery_lock
        .get_write_lock_quiet(ReplicationLockTiming::acquire_timeout())
        .await
    {
        Ok(guard) => Some(guard),
        Err(_) => {
            debug!(
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_REPLICATION,
                "Another node is already processing the MRF recovery backlog"
            );
            None
        }
    }
}

/// Reads and decodes the on-disk MRF recovery file.
/// Returns `None` when there is nothing to replay: missing file (publishes an
/// empty available summary), read failure, or corrupt data (quarantined).
async fn load_mrf_recovery_entries<S: ReplicationStorage>(storage: &Arc<S>) -> Option<Vec<MrfReplicateEntry>> {
    let data = match ReplicationConfigStore::read(storage.clone(), ReplicationMetadataStore::MRF_REPLICATION_FILE).await {
        Ok(d) => d,
        Err(EcstoreError::ConfigNotFound) => {
            set_durable_mrf_backlog_summary(DurableMrfBacklogSummary {
                available: true,
                buckets: Vec::new(),
            });
            return None;
        }
        Err(e) => {
            warn!(
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_REPLICATION,
                error = %e,
                "Failed to load MRF recovery file"
            );
            return None;
        }
    };

    match decode_mrf_file(&data) {
        Ok(v) => Some(v),
        Err(e) => {
            warn!(
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_REPLICATION,
                error = %e,
                "Failed to decode MRF recovery file — preserving corrupt data"
            );
            quarantine_mrf_file(storage, &data).await;
            None
        }
    }
}

/// Replays one MRF recovery entry by operation kind.
/// Returns `None` when the entry is skipped entirely (no admission outcome);
/// entries that must be retried later are pushed onto `retry_entries`.
async fn replay_mrf_entry<S: ReplicationStorage>(
    entry: &MrfReplicateEntry,
    storage: &Arc<S>,
    retry_entries: &mut Vec<MrfReplicateEntry>,
) -> Option<ReplicationQueueAdmission> {
    match entry.op {
        MrfOpKind::Delete => replay_mrf_delete_entry(entry, storage, retry_entries).await,
        MrfOpKind::Object | MrfOpKind::Heal | MrfOpKind::ExistingObject => {
            replay_mrf_object_entry(entry, storage, retry_entries).await
        }
        MrfOpKind::Metadata => replay_mrf_metadata_entry(entry, storage, retry_entries).await,
    }
}

/// Replays a delete-kind MRF entry: force-delete intents replay directly,
/// stale force-delete generations are skipped, and plain deletes are
/// reconstructed as heal deletes.
async fn replay_mrf_delete_entry<S: ReplicationStorage>(
    entry: &MrfReplicateEntry,
    storage: &Arc<S>,
    retry_entries: &mut Vec<MrfReplicateEntry>,
) -> Option<ReplicationQueueAdmission> {
    if should_replay_force_delete_intent(entry) {
        let operation_id = entry.force_delete_id?;
        let delete = force_delete_heal_replication_info(entry, operation_id);
        if replicate_delete_with_outcome(delete, storage.clone()).await {
            Some(ReplicationQueueAdmission::Queued)
        } else {
            Some(ReplicationQueueAdmission::Missed)
        }
    } else if entry.force_delete_id.is_some() {
        Some(ReplicationQueueAdmission::Skipped)
    } else {
        replay_mrf_reconstructed_delete(entry, storage, retry_entries).await
    }
}

/// Pure DTO construction: heal replication info for a replayed force-delete intent.
fn force_delete_heal_replication_info(entry: &MrfReplicateEntry, operation_id: uuid::Uuid) -> DeletedObjectReplicationInfo {
    DeletedObjectReplicationInfo {
        delete_object: ReplicationDeletedObject {
            object_name: entry.object.clone(),
            force_delete: true,
            force_delete_id: Some(operation_id),
            force_delete_target_arns: entry.target_arns.clone(),
            force_delete_generation: entry.force_delete_generation,
            ..Default::default()
        },
        bucket: entry.bucket.clone(),
        op_type: ReplicationType::Heal,
        event_type: REPLICATE_HEAL_DELETE.to_string(),
        ..Default::default()
    }
}

/// Reconstruct a heal delete and re-queue it.  We do NOT call
/// get_object_info here because the delete-marker or version may
/// already be absent from the local store — that is expected.
async fn replay_mrf_reconstructed_delete<S: ReplicationStorage>(
    entry: &MrfReplicateEntry,
    storage: &Arc<S>,
    retry_entries: &mut Vec<MrfReplicateEntry>,
) -> Option<ReplicationQueueAdmission> {
    let versioned = ReplicationVersioningStore::prefix_enabled(&entry.bucket, &entry.object).await;
    let oi = ObjectInfo {
        bucket: entry.bucket.clone(),
        name: entry.object.clone(),
        version_id: entry.version_id,
        delete_marker: entry.delete_marker,
        ..Default::default()
    };
    let dsc = resolve_mrf_delete_replicate_decision(entry, &oi, versioned, retry_entries).await?;
    let dv = reconstructed_heal_delete_info(entry, &oi, &dsc);
    if replicate_delete_with_outcome(dv, storage.clone()).await {
        Some(ReplicationQueueAdmission::Queued)
    } else {
        Some(ReplicationQueueAdmission::Missed)
    }
}

/// The MRF entry does not persist the replication decision and the
/// source object is gone, so re-derive the decision from the live
/// bucket config (mirroring get_heal_replicate_object_info) and set
/// it on the reconstructed delete. Without this the decision string
/// is empty and the delete replicates to zero targets — a silent
/// no-op that leaves replicas diverged (backlog#858 / #799 B9).
async fn resolve_mrf_delete_replicate_decision(
    entry: &MrfReplicateEntry,
    oi: &ObjectInfo,
    versioned: bool,
    retry_entries: &mut Vec<MrfReplicateEntry>,
) -> Option<ReplicateDecision> {
    if entry.target_arns.is_empty() {
        match ReplicationMetadataStore::optional_replication_config(&entry.bucket).await {
            Ok(None) => None,
            Err(_) => {
                retry_entries.push(entry.clone());
                None
            }
            Ok(Some(_)) => match check_replicate_delete_strict(
                &entry.bucket,
                &ObjectToDelete {
                    object_name: entry.object.clone(),
                    version_id: entry.version_id,
                    ..Default::default()
                },
                oi,
                &ObjectOptions {
                    versioned,
                    ..Default::default()
                },
                None,
            )
            .await
            {
                Ok(dsc) => Some(dsc),
                Err(_) => {
                    retry_entries.push(entry.clone());
                    None
                }
            },
        }
    } else {
        Some(replicate_decision_for_admitted_targets(&entry.target_arns))
    }
}

/// Pure DTO construction: reconstructed heal delete carrying the re-derived
/// replication decision.
fn reconstructed_heal_delete_info(
    entry: &MrfReplicateEntry,
    oi: &ObjectInfo,
    dsc: &ReplicateDecision,
) -> DeletedObjectReplicationInfo {
    let mut rstate = oi.replication_state();
    rstate.replicate_decision_str = dsc.to_string();

    let delete_marker_mtime = entry
        .delete_marker_mtime
        .and_then(|nanos| OffsetDateTime::from_unix_timestamp_nanos(i128::from(nanos)).ok());

    DeletedObjectReplicationInfo {
        delete_object: ReplicationDeletedObject {
            object_name: entry.object.clone(),
            version_id: entry.version_id,
            delete_marker_version_id: entry.delete_marker_version_id,
            delete_marker: entry.delete_marker,
            delete_marker_mtime,
            force_delete: entry.force_delete,
            replication_state: Some(rstate),
            ..Default::default()
        },
        bucket: entry.bucket.clone(),
        op_type: ReplicationType::Heal,
        event_type: REPLICATE_HEAL_DELETE.to_string(),
        ..Default::default()
    }
}

/// Replays an Object/Heal/ExistingObject MRF entry against the live source object.
async fn replay_mrf_object_entry<S: ReplicationStorage>(
    entry: &MrfReplicateEntry,
    storage: &Arc<S>,
    retry_entries: &mut Vec<MrfReplicateEntry>,
) -> Option<ReplicationQueueAdmission> {
    let opts = ObjectOptions {
        version_id: entry.version_id.map(|u| u.to_string()),
        ..Default::default()
    };
    let oi = match storage.get_object_info(&entry.bucket, &entry.object, &opts).await {
        Ok(oi) => oi,
        Err(e) => {
            debug!(
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_REPLICATION,
                bucket = %entry.bucket,
                object = %entry.object,
                error = %e,
                "MRF recovery: source object lookup failed"
            );
            if should_retry_mrf_source_lookup(&e) {
                retry_entries.push(entry.clone());
            }
            return None;
        }
    };
    if entry.target_arns.is_empty() {
        // Legacy entries predate target admission persistence. They cannot
        // be safely attributed, so retain the old live-config fallback.
        Some(queue_replication_heal(&entry.bucket, oi, entry.retry_count.max(0) as u32).await)
    } else {
        let roi = admitted_mrf_replicate_object(oi, entry, entry.op.replication_type());
        if replicate_object_with_outcome(roi, storage.clone()).await.1 {
            Some(ReplicationQueueAdmission::Queued)
        } else {
            Some(ReplicationQueueAdmission::Missed)
        }
    }
}

/// Replays a metadata-kind MRF entry against the live source object.
async fn replay_mrf_metadata_entry<S: ReplicationStorage>(
    entry: &MrfReplicateEntry,
    storage: &Arc<S>,
    retry_entries: &mut Vec<MrfReplicateEntry>,
) -> Option<ReplicationQueueAdmission> {
    let opts = ObjectOptions {
        version_id: entry.version_id.map(|u| u.to_string()),
        ..Default::default()
    };
    let oi = match storage.get_object_info(&entry.bucket, &entry.object, &opts).await {
        Ok(oi) => oi,
        Err(e) => {
            debug!(
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_REPLICATION,
                bucket = %entry.bucket,
                object = %entry.object,
                error = %e,
                "MRF metadata recovery: source object lookup failed"
            );
            if should_retry_mrf_source_lookup(&e) {
                retry_entries.push(entry.clone());
            }
            return None;
        }
    };
    if entry.target_arns.is_empty() {
        Some(queue_replication_metadata(&entry.bucket, oi, entry.retry_count.max(0) as u32).await)
    } else {
        let roi = admitted_mrf_replicate_object(oi, entry, ReplicationType::Metadata);
        if replicate_object_with_outcome(roi, storage.clone()).await.1 {
            Some(ReplicationQueueAdmission::Queued)
        } else {
            Some(ReplicationQueueAdmission::Missed)
        }
    }
}

/// Pure DTO construction: replicate-object info for an entry with persisted
/// admitted targets, carrying over the entry's retry count.
fn admitted_mrf_replicate_object(oi: ObjectInfo, entry: &MrfReplicateEntry, op_type: ReplicationType) -> ReplicateObjectInfo {
    let dsc = replicate_decision_for_admitted_targets(&entry.target_arns);
    let mut roi = replicate_object_info_from_object_info(oi, dsc, op_type);
    roi.retry_count = entry.retry_count.max(0) as u32;
    roi
}

/// Acknowledges the replayed MRF prefix and returns the retained backlog.
/// On acknowledgement failure the backlog is preserved for the next startup and
/// re-read (falling back to the replayed snapshot) so the published summary stays accurate.
async fn resolve_retained_mrf_entries<S: ReplicationStorage>(
    storage: &Arc<S>,
    recovery_guard: &rustfs_lock::NamespaceLockGuard,
    entries: &[MrfReplicateEntry],
    retry_entries: &[MrfReplicateEntry],
) -> Vec<MrfReplicateEntry> {
    match acknowledge_mrf_recovery(storage.clone(), recovery_guard, entries, retry_entries).await {
        Ok(retained) => retained,
        Err(error) => {
            warn!(
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_REPLICATION,
                error = %error,
                "Failed to acknowledge the MRF recovery prefix; preserving it for the next startup"
            );
            match read_mrf_entries(storage.clone()).await {
                Ok(current) => current,
                Err(read_error) => {
                    warn!(
                        component = LOG_COMPONENT_ECSTORE,
                        subsystem = LOG_SUBSYSTEM_REPLICATION,
                        error = %read_error,
                        "Failed to refresh the MRF backlog after acknowledgement failure"
                    );
                    entries.to_vec()
                }
            }
        }
    }
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
    mrf_save_tx: Sender<MrfReplicateEntry>,
    mrf_save_rx: Mutex<Option<Receiver<MrfReplicateEntry>>>,

    // MRF worker lifecycle
    mrf_worker_cancellations: Mutex<Vec<CancellationToken>>,
    #[allow(
        dead_code,
        reason = "MinIO-parity replication surface with no caller in this port (backlog#1823)"
    )]
    mrf_stop_tx: Sender<()>,

    // Worker size tracking
    mrf_worker_size: AtomicI32,

    // Task handles for cleanup
    task_handles: Mutex<Vec<JoinHandle<()>>>,

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
        let (mrf_save_tx, mrf_save_rx) = mpsc::channel(100000);
        let (mrf_stop_tx, _mrf_stop_rx) = mpsc::channel(1);

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
            mrf_worker_cancellations: Mutex::new(Vec::with_capacity(worker_counts.mrf_workers)),
            mrf_stop_tx,
            mrf_worker_size: AtomicI32::new(0),
            task_handles: Mutex::new(Vec::new()),
            resyncer: Arc::new(ReplicationResyncer::new().await),
        });

        // Initialize workers
        pool.resize_lrg_workers(max_l_workers, 0).await;
        pool.resize_workers(worker_counts.workers, 0).await;
        pool.resize_failed_workers(worker_counts.mrf_workers_i32()).await;

        // Start background tasks
        pool.start_mrf_persister().await;
        pool.start_mrf_processor().await;
        pool.start_force_delete_processor().await;

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
        let target = mrf_worker_size_to_count(n);
        let mut cancellations = self.mrf_worker_cancellations.lock().await;

        while cancellations.len() < target {
            let cancellation = CancellationToken::new();
            cancellations.push(cancellation.clone());

            let active_counter = self.active_mrf_workers.clone();
            let stats = self.stats.clone();
            let storage = self.storage.clone();
            let mrf_rx = Arc::clone(&self.mrf_replica_rx);

            let handle = tokio::spawn(async move {
                loop {
                    let operation = tokio::select! {
                        biased;
                        operation = async {
                            let mut receiver = mrf_rx.lock().await;
                            tokio::select! {
                                biased;
                                operation = receiver.recv() => operation,
                                _ = cancellation.cancelled() => None,
                            }
                        } => operation,
                        _ = cancellation.cancelled() => break,
                    };
                    let Some(operation) = operation else { break };

                    let _active = ActiveWorkerGuard::new(active_counter.clone());
                    process_replication_operation(operation, stats.clone(), storage.clone()).await;
                }
            });
            self.task_handles.lock().await.push(handle);
        }

        while cancellations.len() > target {
            if let Some(cancellation) = cancellations.pop() {
                cancellation.cancel();
            }
        }

        self.mrf_worker_size.store(n.max(0), Ordering::SeqCst);
    }

    /// Resizes worker priority and counts
    #[allow(
        dead_code,
        reason = "MinIO-parity replication surface with no caller in this port (backlog#1823)"
    )]
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
    #[allow(
        dead_code,
        reason = "MinIO-parity replication surface with no caller in this port (backlog#1823)"
    )]
    async fn queue_mrf_save(&self, entry: MrfReplicateEntry) {
        let _ = self.queue_mrf_save_admission(entry, "mrf_worker").await;
    }

    async fn queue_mrf_save_admission(&self, entry: MrfReplicateEntry, queue_type: &'static str) -> ReplicationQueueAdmission {
        let bucket = entry.bucket.clone();
        let size = entry.size;
        let is_delete = matches!(entry.op, MrfOpKind::Delete);
        let target_arns = entry.target_arns.clone();
        let admission = queue_mrf_save_entry(&self.mrf_save_tx, entry, queue_type).await;
        if admission == ReplicationQueueAdmission::Queued {
            self.stats.inc_q(&bucket, size, is_delete, ReplicationType::Heal);
            self.stats.inc_target_q(&bucket, &target_arns, size);
        }
        admission
    }

    /// Starts the MRF processor — one-shot at startup.
    ///
    /// Reads the on-disk MRF file, re-injects admitted entries as Heal operations, and
    /// rewrites any entries that could not be admitted for a later startup retry.
    async fn start_mrf_processor(&self) {
        let storage = self.storage.clone();

        let handle = tokio::spawn(async move {
            let Some(recovery_guard) = acquire_mrf_recovery_guard(&storage).await else {
                return;
            };

            let Some(entries) = load_mrf_recovery_entries(&storage).await else {
                return;
            };
            set_durable_mrf_backlog_snapshot(durable_mrf_backlog_summary_from_entries(&entries));

            let total = entries.len();
            let mut queued_count = 0usize;
            let mut retry_entries = Vec::new();

            for entry in entries.iter() {
                let Some(admission) = replay_mrf_entry(entry, &storage, &mut retry_entries).await else {
                    continue;
                };

                if admission == ReplicationQueueAdmission::Missed {
                    retry_entries.push(entry.clone());
                } else if admission == ReplicationQueueAdmission::Queued {
                    queued_count += 1;
                }
            }

            let retained = resolve_retained_mrf_entries(&storage, &recovery_guard, &entries, &retry_entries).await;
            let retained_count = retained.len();
            set_durable_mrf_backlog_snapshot(durable_mrf_backlog_summary_from_entries(&retained));

            if queued_count > 0 {
                info!(
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_REPLICATION,
                    recovered = queued_count,
                    total,
                    retained = retained_count,
                    "Replayed MRF entries admitted for retry"
                );
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
    /// writes them to the on-disk MRF file every flush interval (default 10s,
    /// overridable via `RUSTFS_REPL_MRF_FLUSH_INTERVAL_MS`) or when 1 000 new
    /// entries accumulate. Runtime writers append under the journal lock; only the
    /// startup recovery leader may remove an acknowledged prefix.
    async fn start_mrf_persister(&self) {
        let Some(mut rx) = self.mrf_save_rx.lock().await.take() else {
            return;
        };
        let storage = self.storage.clone();
        let stats = self.stats.clone();

        let handle = tokio::spawn(async move {
            let mut pending = Vec::new();
            let mut pending_payload = None;
            let mut channel_closed = false;
            let mut capped = false;
            // Flush interval: `RUSTFS_REPL_MRF_FLUSH_INTERVAL_MS` (default 10000ms,
            // clamped to >=10ms), read once when the persister task starts.
            let mut interval = tokio::time::interval(super::replication_timing::mrf_flush_interval());
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                if !channel_closed && rx.is_closed() && rx.is_empty() {
                    channel_closed = true;
                }
                if channel_closed && pending.is_empty() {
                    break;
                }
                let flush_requested = if channel_closed || pending.len() >= MRF_PENDING_CAP || pending_payload.is_some() {
                    true
                } else {
                    tokio::select! {
                        entry = rx.recv() => match entry {
                            Some(entry) => {
                                observe_mrf_pending(&entry);
                                pending.push(entry);
                                pending.len() >= 1000
                            }
                            None => {
                                channel_closed = true;
                                true
                            }
                        },
                        _ = interval.tick() => !pending.is_empty(),
                    }
                };

                if !flush_requested || pending.is_empty() {
                    continue;
                }
                if pending.len() >= MRF_PENDING_CAP && !capped {
                    capped = true;
                    warn!(
                        component = LOG_COMPONENT_ECSTORE,
                        subsystem = LOG_SUBSYSTEM_REPLICATION,
                        cap = MRF_PENDING_CAP,
                        "MRF pending backlog reached capacity — applying backpressure"
                    );
                }

                match flush_mrf_to_disk(&pending, &storage, &mut pending_payload).await {
                    Some(result) => {
                        set_durable_mrf_backlog_snapshot(result.backlog);
                        observe_mrf_pending_flushed(&pending, result.duration_millis);
                        dec_mrf_entries(stats.as_ref(), &pending);
                        pending.clear();
                        pending_payload = None;
                        capped = false;
                    }
                    None => {
                        interval.tick().await;
                    }
                }
            }
        });
        self.task_handles.lock().await.push(handle);
    }

    /// Worker function for handling regular replication operations
    #[allow(
        dead_code,
        reason = "MinIO-parity replication surface with no caller in this port (backlog#1823)"
    )]
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
    #[allow(
        dead_code,
        reason = "MinIO-parity replication surface with no caller in this port (backlog#1823)"
    )]
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
    #[allow(
        dead_code,
        reason = "MinIO-parity replication surface with no caller in this port (backlog#1823)"
    )]
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
    #[allow(
        dead_code,
        reason = "MinIO-parity replication surface with no caller in this port (backlog#1823)"
    )]
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

async fn queue_mrf_save_entry(
    tx: &Sender<MrfReplicateEntry>,
    entry: MrfReplicateEntry,
    queue_type: &'static str,
) -> ReplicationQueueAdmission {
    let Err(error) = tx.send(entry).await else {
        return ReplicationQueueAdmission::Queued;
    };
    let entry = error.0;

    warn!(
        event = EVENT_REPLICATION_MRF_QUEUE_UNAVAILABLE,
        component = LOG_COMPONENT_ECSTORE,
        subsystem = LOG_SUBSYSTEM_REPLICATION,
        bucket = %entry.bucket,
        object = %entry.object,
        queue_type = queue_type,
        "MRF save channel unavailable — replication failure entry could not be persisted for retry"
    );
    observe_mrf_missed(&entry.bucket);
    ReplicationQueueAdmission::Missed
}

async fn quarantine_mrf_file<S: ReplicationStorage>(storage: &Arc<S>, data: &[u8]) {
    let quarantine_file = format!("{MRF_CORRUPT_FILE_PREFIX}.{}.bin", OffsetDateTime::now_utc().unix_timestamp_nanos());
    let payload = data.to_vec();
    let mut retry_delay = MRF_RETRY_INITIAL_DELAY;
    loop {
        match ReplicationConfigStore::save(storage.clone(), &quarantine_file, payload.clone()).await {
            Ok(()) => {
                warn!(
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_REPLICATION,
                    file = %quarantine_file,
                    "Quarantined corrupt MRF recovery file"
                );
                break;
            }
            Err(error) => warn!(
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_REPLICATION,
                file = %quarantine_file,
                error = %error,
                "Failed to quarantine corrupt MRF recovery file; retrying without overwriting it"
            ),
        }
        tokio::time::sleep(retry_delay).await;
        retry_delay = retry_delay.saturating_mul(2).min(MRF_RETRY_MAX_DELAY);
    }

    // Clear the active path only if it still contains the bytes that were
    // quarantined. The write lock closes the read/clear race with another node
    // replacing the active generation while this node retries quarantine.
    retry_delay = MRF_RETRY_INITIAL_DELAY;
    loop {
        let lock = match storage
            .new_ns_lock(
                ReplicationMetadataStore::rustfs_meta_bucket(),
                ReplicationMetadataStore::MRF_REPLICATION_FILE,
            )
            .await
        {
            Ok(lock) => lock,
            Err(error) => {
                warn!(
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_REPLICATION,
                    error = %error,
                    "Failed to acquire the MRF recovery lock before clearing quarantine source; retrying"
                );
                tokio::time::sleep(retry_delay).await;
                retry_delay = retry_delay.saturating_mul(2).min(MRF_RETRY_MAX_DELAY);
                continue;
            }
        };
        let guard = match lock.get_write_lock(ReplicationLockTiming::acquire_timeout()).await {
            Ok(guard) => guard,
            Err(error) => {
                warn!(
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_REPLICATION,
                    error = %error,
                    "Failed to acquire the MRF recovery lock before clearing quarantine source; retrying"
                );
                tokio::time::sleep(retry_delay).await;
                retry_delay = retry_delay.saturating_mul(2).min(MRF_RETRY_MAX_DELAY);
                continue;
            }
        };
        match ReplicationConfigStore::read_no_lock_with_metadata_preserve_empty(
            storage.clone(),
            ReplicationMetadataStore::MRF_REPLICATION_FILE,
        )
        .await
        {
            Err(EcstoreError::ConfigNotFound) => return,
            Ok((current, _)) if current != data => return,
            Ok((_, object_info)) => {
                let Some(preconditions) = mrf_journal_preconditions(object_info.etag.as_deref(), true) else {
                    warn!(
                        component = LOG_COMPONENT_ECSTORE,
                        subsystem = LOG_SUBSYSTEM_REPLICATION,
                        "Cannot clear the corrupt MRF recovery path without an ETag; retrying"
                    );
                    drop(guard);
                    tokio::time::sleep(retry_delay).await;
                    retry_delay = retry_delay.saturating_mul(2).min(MRF_RETRY_MAX_DELAY);
                    continue;
                };
                if let Err(error) = ensure_mrf_journal_lock_held(guard.is_lock_lost()) {
                    warn!(
                        component = LOG_COMPONENT_ECSTORE,
                        subsystem = LOG_SUBSYSTEM_REPLICATION,
                        error = %error,
                        "MRF journal lock was lost before clearing the corrupt recovery path; retrying"
                    );
                    drop(guard);
                    tokio::time::sleep(retry_delay).await;
                    retry_delay = retry_delay.saturating_mul(2).min(MRF_RETRY_MAX_DELAY);
                    continue;
                }
                match ReplicationConfigStore::save_conditional_no_lock(
                    storage.clone(),
                    ReplicationMetadataStore::MRF_REPLICATION_FILE,
                    Vec::new(),
                    preconditions,
                )
                .await
                {
                    Ok(()) => return,
                    Err(error) => warn!(
                        component = LOG_COMPONENT_ECSTORE,
                        subsystem = LOG_SUBSYSTEM_REPLICATION,
                        error = %error,
                        "Failed to clear the corrupt MRF recovery path after quarantine; retrying"
                    ),
                }
            }
            Err(error) => warn!(
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_REPLICATION,
                error = %error,
                "Failed to verify the corrupt MRF recovery path before clearing; retrying"
            ),
        }
        drop(guard);
        tokio::time::sleep(retry_delay).await;
        retry_delay = retry_delay.saturating_mul(2).min(MRF_RETRY_MAX_DELAY);
    }
}

fn should_retry_mrf_source_lookup(error: &EcstoreError) -> bool {
    !is_err_object_not_found(error) && !is_err_version_not_found(error)
}

fn dec_mrf_entries(stats: &ReplicationStats, entries: &[MrfReplicateEntry]) {
    for entry in entries {
        stats.dec_q(&entry.bucket, entry.size, matches!(entry.op, MrfOpKind::Delete), ReplicationType::Heal);
        stats.dec_target_q(&entry.bucket, &entry.target_arns, entry.size);
    }
}

/// Appends `entries` to the MRF persistence file.
/// Returns the committed backlog and flush duration on success; on failure logs
/// the error and returns `None`.
/// Callers must NOT clear their in-memory buffer on `None` so the next tick
/// can retry — otherwise a transient storage error permanently drops the batch.
async fn flush_mrf_to_disk<S: ReplicationStorage>(
    entries: &[MrfReplicateEntry],
    storage: &Arc<S>,
    pending_payload: &mut Option<PendingMrfAppend>,
) -> Option<MrfAppendResult> {
    append_mrf_entries_to_disk(entries, storage, pending_payload, &[]).await
}

async fn recover_corrupt_mrf_generation<S: ReplicationStorage>(
    corrupt_generation: &[u8],
    entries: &[MrfReplicateEntry],
    preconditions: Option<HTTPPreconditions>,
    storage: &Arc<S>,
    guard: &rustfs_lock::NamespaceLockGuard,
    pending_payload: &mut Option<PendingMrfAppend>,
    started: Instant,
) -> Option<MrfAppendResult> {
    let quarantine_file = format!("{MRF_CORRUPT_FILE_PREFIX}.{}.bin", OffsetDateTime::now_utc().unix_timestamp_nanos());
    if let Err(error) = ReplicationConfigStore::save_no_lock(storage.clone(), &quarantine_file, corrupt_generation.to_vec()).await
    {
        warn!(
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_REPLICATION,
            file = %quarantine_file,
            error = %error,
            "Failed to quarantine a corrupt active MRF generation before rebuilding it"
        );
        return None;
    }

    let data = match encode_mrf_file(entries) {
        Ok(data) => data,
        Err(error) => {
            observe_mrf_flush_failure(0);
            warn!(
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_REPLICATION,
                count = entries.len(),
                error = %error,
                "Failed to rebuild the active MRF generation after quarantining corruption"
            );
            return None;
        }
    };
    *pending_payload = Some(PendingMrfAppend {
        digest: mrf_payload_digest(&data),
        entry_count: entries.len(),
    });
    let Some(preconditions) = preconditions else {
        observe_mrf_flush_failure(duration_millis_u64(started.elapsed()));
        warn!(
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_REPLICATION,
            count = entries.len(),
            "Failed to rebuild the active MRF generation because its ETag is unavailable"
        );
        return None;
    };
    if let Err(error) = ensure_mrf_journal_lock_held(guard.is_lock_lost()) {
        observe_mrf_flush_failure(duration_millis_u64(started.elapsed()));
        warn!(
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_REPLICATION,
            count = entries.len(),
            error = %error,
            "Failed to replace the active MRF generation after losing its namespace lock"
        );
        return None;
    }
    if let Err(error) = ReplicationConfigStore::save_conditional_no_lock(
        storage.clone(),
        ReplicationMetadataStore::MRF_REPLICATION_FILE,
        data,
        preconditions,
    )
    .await
    {
        observe_mrf_flush_failure(duration_millis_u64(started.elapsed()));
        warn!(
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_REPLICATION,
            count = entries.len(),
            error = %error,
            "Failed to replace the active MRF generation after quarantining corruption"
        );
        return None;
    }
    Some(MrfAppendResult {
        duration_millis: duration_millis_u64(started.elapsed()),
        backlog: durable_mrf_backlog_summary_from_entries(entries),
    })
}

async fn append_mrf_entries_to_disk<S: ReplicationStorage>(
    entries_to_append: &[MrfReplicateEntry],
    storage: &Arc<S>,
    pending_payload: &mut Option<PendingMrfAppend>,
    known_pending: &[MrfReplicateEntry],
) -> Option<MrfAppendResult> {
    if entries_to_append.is_empty() {
        return None;
    }
    let started = Instant::now();
    let lock = match storage
        .new_ns_lock(
            ReplicationMetadataStore::rustfs_meta_bucket(),
            ReplicationMetadataStore::MRF_REPLICATION_FILE,
        )
        .await
    {
        Ok(lock) => lock,
        Err(error) => {
            warn!(
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_REPLICATION,
                error = %error,
                "Failed to acquire the MRF lock before appending entries"
            );
            return None;
        }
    };
    let guard = match lock.get_write_lock(ReplicationLockTiming::acquire_timeout()).await {
        Ok(guard) => guard,
        Err(error) => {
            warn!(
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_REPLICATION,
                error = %error,
                "Failed to acquire the MRF write lock before appending entries"
            );
            return None;
        }
    };

    let (current, current_etag, current_exists) = match ReplicationConfigStore::read_no_lock_with_metadata_preserve_empty(
        storage.clone(),
        ReplicationMetadataStore::MRF_REPLICATION_FILE,
    )
    .await
    {
        Ok((data, object_info)) => (data, object_info.etag, true),
        Err(EcstoreError::ConfigNotFound) => (Vec::new(), None, false),
        Err(error) => {
            warn!(
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_REPLICATION,
                error = %error,
                "Failed to read MRF backlog before appending entries"
            );
            return None;
        }
    };

    let mut entries = match decode_mrf_file(&current) {
        Ok(entries) => entries,
        Err(_) if current.is_empty() => Vec::new(),
        Err(error) => {
            warn!(
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_REPLICATION,
                error = %error,
                "Failed to decode MRF backlog before appending entries"
            );
            let mut recovery_entries = Vec::with_capacity(known_pending.len().saturating_add(entries_to_append.len()));
            recovery_entries.extend_from_slice(known_pending);
            recovery_entries.extend_from_slice(entries_to_append);
            return recover_corrupt_mrf_generation(
                &current,
                &recovery_entries,
                mrf_journal_preconditions(current_etag.as_deref(), current_exists),
                storage,
                &guard,
                pending_payload,
                started,
            )
            .await;
        }
    };
    if let Some(pending) = pending_payload.as_ref()
        && entries.len() >= pending.entry_count
    {
        match encode_mrf_file(&entries[..pending.entry_count]) {
            Ok(prefix) if mrf_payload_digest(&prefix) == pending.digest => {
                return Some(MrfAppendResult {
                    duration_millis: duration_millis_u64(started.elapsed()),
                    backlog: durable_mrf_backlog_summary_from_entries(&entries),
                });
            }
            Ok(_) => {}
            Err(error) => {
                observe_mrf_flush_failure(0);
                warn!(
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_REPLICATION,
                    error = %error,
                    "Failed to verify an MRF append after an ambiguous save error"
                );
                return None;
            }
        }
    }
    entries.extend_from_slice(entries_to_append);
    let data = match encode_mrf_file(&entries) {
        Ok(data) => data,
        Err(error) => {
            observe_mrf_flush_failure(0);
            warn!(
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_REPLICATION,
                count = entries.len(),
                error = %error,
                "Failed to encode MRF entries for disk append"
            );
            return None;
        }
    };
    *pending_payload = Some(PendingMrfAppend {
        digest: mrf_payload_digest(&data),
        entry_count: entries.len(),
    });
    let Some(preconditions) = mrf_journal_preconditions(current_etag.as_deref(), current_exists) else {
        let duration_millis = duration_millis_u64(started.elapsed());
        observe_mrf_flush_failure(duration_millis);
        warn!(
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_REPLICATION,
            count = entries.len(),
            "Failed to append MRF entries because the current generation has no ETag"
        );
        return None;
    };
    if let Err(error) = ensure_mrf_journal_lock_held(guard.is_lock_lost()) {
        let duration_millis = duration_millis_u64(started.elapsed());
        observe_mrf_flush_failure(duration_millis);
        warn!(
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_REPLICATION,
            count = entries.len(),
            error = %error,
            "Failed to append MRF entries after losing the namespace lock"
        );
        return None;
    }
    if let Err(error) = ReplicationConfigStore::save_conditional_no_lock(
        storage.clone(),
        ReplicationMetadataStore::MRF_REPLICATION_FILE,
        data,
        preconditions,
    )
    .await
    {
        let duration_millis = duration_millis_u64(started.elapsed());
        observe_mrf_flush_failure(duration_millis);
        warn!(
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_REPLICATION,
            count = entries.len(),
            error = %error,
            "Failed to append MRF entries to disk"
        );
        return None;
    }
    Some(MrfAppendResult {
        duration_millis: duration_millis_u64(started.elapsed()),
        backlog: durable_mrf_backlog_summary_from_entries(&entries),
    })
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

    let data = match ReplicationConfigStore::read_limited(obj_api, &resync_file_path, RESYNC_FILE_MAX_BYTES).await {
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
    /// Persist one entry straight to the durable MRF journal, bypassing the
    /// live worker queues. For failures whose source state is already gone —
    /// e.g. exhausted delete-marker purges — where only a startup replay can
    /// retry, and live re-dispatch would loop unboundedly against a down
    /// target.
    async fn persist_mrf_entry(&self, entry: MrfReplicateEntry) -> ReplicationQueueAdmission;
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

    async fn persist_mrf_entry(&self, entry: MrfReplicateEntry) -> ReplicationQueueAdmission {
        self.queue_mrf_save_admission(entry, "delete_marker_purge").await
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

    let _pool = ctx
        .replication_pool_cell()
        .get_or_init(|| async {
            let pool = ReplicationPool::new(ReplicationPoolOpts::default(), stats.clone(), storage).await;
            pool as Arc<DynReplicationPool>
        })
        .await;

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
        let state = replicate_object(ri, o.clone()).await;
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
    let asz = oi.get_actual_size_or_physical();
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

pub(crate) async fn schedule_replication_delete(dv: DeletedObjectReplicationInfo) -> ReplicationQueueAdmission {
    let admission = if let Some(pool) = runtime_sources::replication_pool() {
        pool.queue_replica_delete_task(dv.clone()).await
    } else {
        ReplicationQueueAdmission::Missed
    };

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

    admission
}

/// QueueReplicationHeal is a wrapper for queue_replication_heal_internal
pub async fn queue_replication_heal(bucket: &str, oi: ObjectInfo, retry_count: u32) -> ReplicationQueueAdmission {
    // ignore modtime zero objects
    if oi.mod_time.is_none() || oi.mod_time == Some(OffsetDateTime::UNIX_EPOCH) {
        return ReplicationQueueAdmission::Skipped;
    }

    let rcfg = match ReplicationMetadataStore::optional_replication_config(bucket).await {
        Ok(Some(config)) => config,
        Ok(None) => return ReplicationQueueAdmission::Skipped,
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

            return ReplicationQueueAdmission::Missed;
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
    queue_replication_heal_internal(bucket, oi, rcfg_wrapper, retry_count)
        .await
        .admission
}

pub async fn queue_replication_metadata(bucket: &str, oi: ObjectInfo, retry_count: u32) -> ReplicationQueueAdmission {
    let dsc = must_replicate(
        bucket,
        &oi.name,
        MustReplicateOptions::new(&oi.user_defined, (*oi.user_tags).clone(), ReplicationType::Metadata, false)
            .with_replication_status(oi.replication_status.clone()),
    )
    .await;

    if !dsc.replicate_any() {
        return ReplicationQueueAdmission::Skipped;
    }

    let mut roi = replicate_object_info_from_object_info(oi, dsc, ReplicationType::Metadata);
    roi.retry_count = retry_count;
    if let Some(pool) = runtime_sources::replication_pool() {
        pool.queue_replica_task(roi).await
    } else {
        ReplicationQueueAdmission::Missed
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
    use super::super::replication_storage_boundary::{
        DeletedObject, FileInfo, GetObjectReader, HTTPRangeSpec, ListOperations, ObjectIO, ObjectOperations, PutObjReader,
        StorageListObjectVersionsInfo, StorageListObjectsV2Info, StorageNamespaceLocking, StorageObjectInfoOrErr, WalkOptions,
    };
    use super::*;
    use std::collections::{HashMap, VecDeque};
    use std::fmt::{Debug, Formatter};
    use std::io::{self, Cursor};
    use std::pin::Pin;
    use std::sync::Mutex as StdMutex;
    use std::sync::atomic::{AtomicBool, AtomicUsize};
    use std::task::{Context, Poll};
    use tokio::io::{AsyncRead, AsyncReadExt, ReadBuf};
    use tokio::sync::Notify;
    use uuid::Uuid;

    type TestListObjectsV2Info = StorageListObjectsV2Info<ObjectInfo>;
    type TestListObjectVersionsInfo = StorageListObjectVersionsInfo<ObjectInfo>;
    type TestObjectInfoOrErr = StorageObjectInfoOrErr<ObjectInfo, EcstoreError>;

    struct LoadResyncSharedState {
        data: StdMutex<Vec<u8>>,
        empty_object_exists: AtomicBool,
        etag_revision: AtomicUsize,
        last_put_preconditions: StdMutex<Option<HTTPPreconditions>>,
        last_put_no_lock: AtomicBool,
        omit_etag: AtomicBool,
        conditional_write_replacements: StdMutex<VecDeque<Vec<u8>>>,
        writes: StdMutex<Vec<(String, Vec<u8>)>>,
        lock_manager: Arc<rustfs_lock::GlobalLockManager>,
        first_read_started: Notify,
        delay_first_read: AtomicBool,
        hold_first_read: AtomicBool,
        allow_first_read: Notify,
        read_count: AtomicUsize,
        reported_size: StdMutex<Option<i64>>,
        stream_read_bytes: Arc<AtomicUsize>,
        write_count: AtomicUsize,
        fail_next_write: AtomicBool,
        fail_after_write: AtomicBool,
        block_next_write: AtomicBool,
        write_started: Notify,
        allow_write: Notify,
    }

    struct LoadResyncNodeStore {
        owner: String,
        shared: Arc<LoadResyncSharedState>,
    }

    struct CountingReader {
        inner: Cursor<Vec<u8>>,
        bytes_read: Arc<AtomicUsize>,
    }

    impl AsyncRead for CountingReader {
        fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
            let filled_before = buf.filled().len();
            match Pin::new(&mut self.inner).poll_read(cx, buf) {
                Poll::Ready(Ok(())) => {
                    self.bytes_read
                        .fetch_add(buf.filled().len().saturating_sub(filled_before), Ordering::SeqCst);
                    Poll::Ready(Ok(()))
                }
                other => other,
            }
        }
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
                && !object.ends_with("config/replication/mrf.bin")
                && !object.ends_with("config/replication/force-delete.bin")
            {
                return Err(EcstoreError::FileNotFound);
            }

            let read_index = self.shared.read_count.fetch_add(1, Ordering::SeqCst);
            if read_index == 0 && self.shared.delay_first_read.load(Ordering::SeqCst) {
                self.shared.first_read_started.notify_waiters();
                if self.shared.hold_first_read.load(Ordering::SeqCst) {
                    self.shared.allow_first_read.notified().await;
                } else {
                    tokio::time::sleep(Duration::from_millis(1_500)).await;
                }
            }

            let data = self
                .shared
                .data
                .lock()
                .expect("test data lock should not be poisoned")
                .clone();
            if data.is_empty() && !self.shared.empty_object_exists.load(Ordering::SeqCst) {
                return Err(EcstoreError::FileNotFound);
            }
            let actual_size = i64::try_from(data.len()).expect("test metadata length should fit i64");
            let size = self
                .shared
                .reported_size
                .lock()
                .expect("test reported size lock should not be poisoned")
                .unwrap_or(actual_size);
            Ok(Self::GetObjectReader {
                stream: Box::new(CountingReader {
                    inner: Cursor::new(data),
                    bytes_read: self.shared.stream_read_bytes.clone(),
                }),
                object_info: ObjectInfo {
                    size,
                    actual_size,
                    etag: (!self.shared.omit_etag.load(Ordering::SeqCst))
                        .then(|| format!("mrf-{}", self.shared.etag_revision.load(Ordering::SeqCst))),
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
            if opts.http_preconditions.is_some()
                && let Some(replacement) = self
                    .shared
                    .conditional_write_replacements
                    .lock()
                    .expect("test replacement lock should not be poisoned")
                    .pop_front()
            {
                *self.shared.data.lock().expect("test data lock should not be poisoned") = replacement;
                self.shared.etag_revision.fetch_add(1, Ordering::SeqCst);
            }
            let current_etag = if self
                .shared
                .data
                .lock()
                .expect("test data lock should not be poisoned")
                .is_empty()
                && !self.shared.empty_object_exists.load(Ordering::SeqCst)
            {
                None
            } else {
                Some(format!("mrf-{}", self.shared.etag_revision.load(Ordering::SeqCst)))
            };
            if opts.http_preconditions.as_ref().is_some_and(|preconditions| {
                preconditions.if_none_match_value() == Some("*") && current_etag.is_some()
                    || preconditions
                        .if_match_value()
                        .is_some_and(|expected| current_etag.as_deref() != Some(expected))
            }) {
                return Err(EcstoreError::PreconditionFailed);
            }
            *self
                .shared
                .last_put_preconditions
                .lock()
                .expect("test preconditions lock should not be poisoned") = opts.http_preconditions.clone();
            self.shared.last_put_no_lock.store(opts.no_lock, Ordering::SeqCst);
            if self.shared.fail_next_write.swap(false, Ordering::SeqCst) {
                return Err(EcstoreError::Unexpected);
            }
            if self.shared.block_next_write.swap(false, Ordering::SeqCst) {
                self.shared.write_started.notify_one();
                self.shared.allow_write.notified().await;
            }
            let mut encoded = Vec::new();
            data.stream.read_to_end(&mut encoded).await.map_err(EcstoreError::from)?;
            self.shared
                .writes
                .lock()
                .expect("test writes lock should not be poisoned")
                .push((object.to_string(), encoded.clone()));
            if !object.starts_with(MRF_CORRUPT_FILE_PREFIX) {
                *self.shared.data.lock().expect("test data lock should not be poisoned") = encoded;
                self.shared.etag_revision.fetch_add(1, Ordering::SeqCst);
            }
            self.shared.write_count.fetch_add(1, Ordering::SeqCst);
            if self.shared.fail_after_write.swap(false, Ordering::SeqCst) {
                return Err(EcstoreError::Unexpected);
            }
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
            _object: &str,
            _opts: &Self::ObjectOptions,
        ) -> Result<Self::ObjectInfo, Self::Error> {
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
        new_test_replication_pool_with_mrf_capacity(storage, 1).await
    }

    async fn new_test_replication_pool_with_mrf_capacity(
        storage: Arc<LoadResyncNodeStore>,
        mrf_save_capacity: usize,
    ) -> Arc<ReplicationPool<LoadResyncNodeStore>> {
        let (mrf_replica_tx, mrf_replica_rx) = mpsc::channel(1);
        let (mrf_save_tx, mrf_save_rx) = mpsc::channel(mrf_save_capacity);
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
            mrf_worker_cancellations: Mutex::new(Vec::new()),
            mrf_stop_tx,
            mrf_worker_size: AtomicI32::new(0),
            task_handles: Mutex::new(Vec::new()),
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
            empty_object_exists: AtomicBool::new(false),
            etag_revision: AtomicUsize::new(0),
            last_put_preconditions: StdMutex::new(None),
            last_put_no_lock: AtomicBool::new(false),
            omit_etag: AtomicBool::new(false),
            conditional_write_replacements: StdMutex::new(VecDeque::new()),
            writes: StdMutex::new(Vec::new()),
            lock_manager: Arc::new(rustfs_lock::GlobalLockManager::new()),
            first_read_started: Notify::new(),
            delay_first_read: AtomicBool::new(false),
            hold_first_read: AtomicBool::new(false),
            allow_first_read: Notify::new(),
            read_count: AtomicUsize::new(0),
            reported_size: StdMutex::new(None),
            stream_read_bytes: Arc::new(AtomicUsize::new(0)),
            write_count: AtomicUsize::new(0),
            fail_next_write: AtomicBool::new(false),
            fail_after_write: AtomicBool::new(false),
            block_next_write: AtomicBool::new(false),
            write_started: Notify::new(),
            allow_write: Notify::new(),
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
        let result = queue_replication_heal_internal(
            "missing-versioning-state",
            ObjectInfo {
                bucket: "missing-versioning-state".to_string(),
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
    async fn queue_replica_task_counts_mrf_pending_backlog_when_worker_queue_is_full() {
        let shared = empty_resync_shared_state();
        let pool = new_test_replication_pool(Arc::new(LoadResyncNodeStore::new("node-a", shared))).await;
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
        let queued = pool.stats.get_latest_replication_stats("runtime-backlog").await;
        assert_eq!(queued.replication_stats.q_stat.curr.count, 1);
        assert_eq!(queued.replication_stats.q_stat.curr.bytes, 2048);
        assert_eq!(
            current_target_queue(&pool, "runtime-backlog", "arn:rustfs:replication:target-a"),
            Some((1, 2048))
        );
    }

    #[tokio::test]
    async fn resize_failed_workers_cancels_idle_workers() {
        let shared = empty_resync_shared_state();
        let pool = new_test_replication_pool(Arc::new(LoadResyncNodeStore::new("mrf-resize", shared))).await;

        pool.resize_failed_workers(4).await;
        assert_eq!(pool.mrf_worker_cancellations.lock().await.len(), 4);
        assert_eq!(pool.mrf_worker_size.load(Ordering::SeqCst), 4);

        pool.resize_failed_workers(1).await;

        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let finished = pool
                    .task_handles
                    .lock()
                    .await
                    .iter()
                    .filter(|handle| handle.is_finished())
                    .count();
                if finished == 3 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("canceled MRF workers should exit while the shared queue is idle");

        assert_eq!(pool.mrf_worker_cancellations.lock().await.len(), 1);
        assert_eq!(pool.mrf_worker_size.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn resize_failed_workers_is_idempotent_across_growth_and_shrink() {
        let shared = empty_resync_shared_state();
        let pool = new_test_replication_pool(Arc::new(LoadResyncNodeStore::new("mrf-resize-repeat", shared))).await;

        for target in [2, 4, 1, 4, 4] {
            pool.resize_failed_workers(target).await;
            assert_eq!(
                pool.mrf_worker_cancellations.lock().await.len(),
                usize::try_from(target).expect("test worker count should fit usize")
            );
            assert_eq!(pool.mrf_worker_size.load(Ordering::SeqCst), target);
        }
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

        tx.try_send(first).expect("first MRF entry should fill the test channel");

        let admission = queue_mrf_save_entry(&tx, second, "test");
        tokio::pin!(admission);

        assert!(
            tokio::time::timeout(Duration::from_millis(25), &mut admission).await.is_err(),
            "full MRF channel should apply backpressure instead of returning Missed"
        );

        let received = rx.recv().await.expect("first MRF entry should still be queued");
        assert_eq!(received.object, "first");

        let admission = tokio::time::timeout(Duration::from_secs(1), &mut admission)
            .await
            .expect("MRF admission should finish once capacity is available");
        assert_eq!(admission, ReplicationQueueAdmission::Queued);

        let received = rx
            .recv()
            .await
            .expect("second MRF entry should be queued after capacity opens");
        assert_eq!(received.object, "second");
    }

    #[tokio::test]
    async fn delete_batch_admission_reports_mrf_fallback_items() {
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
        let deletes = (0..1)
            .map(|index| DeletedObjectReplicationInfo {
                bucket: "batch-backpressure".to_string(),
                delete_object: ReplicationDeletedObject {
                    object_name: format!("object-{index}"),
                    ..Default::default()
                },
                op_type: ReplicationType::Delete,
                ..Default::default()
            })
            .collect::<Vec<_>>();
        let summary = pool.queue_replica_delete_batch(&deletes).await;
        let entry = mrf_rx
            .recv()
            .await
            .expect("MRF fallback entry should be queued after batch admission");
        assert_eq!(entry.object, "object-0");
        assert_eq!(summary.total, 1);
        assert_eq!(summary.queued, 1);
        assert_eq!(summary.missed, 0);
        assert_eq!(summary.outcome(), "all_queued");
        drop(worker_rx);
    }

    #[tokio::test]
    async fn mrf_save_admission_records_missed_when_channel_is_closed() {
        let (tx, rx) = mpsc::channel(1);
        drop(rx);
        let bucket = "mrf-missed-hook-bucket";

        let admission = queue_mrf_save_entry(
            &tx,
            MrfReplicateEntry {
                bucket: bucket.to_string(),
                object: "missed".to_string(),
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
            },
            "test",
        )
        .await;

        assert_eq!(admission, ReplicationQueueAdmission::Missed);
        let snapshot = mrf_backlog_observability_snapshot();
        let bucket = snapshot
            .buckets
            .iter()
            .find(|stats| stats.bucket == "mrf-missed-hook-bucket")
            .expect("missed MRF admission should be observable");
        assert_eq!(bucket.missed_count, 1);
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
        let mut pending_payload = None;

        let result = flush_mrf_to_disk(std::slice::from_ref(&entry), &storage, &mut pending_payload).await;

        assert!(result.is_none());
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
    async fn bounded_replication_config_read_accepts_exact_limit_and_caps_underreported_stream() {
        const TEST_LIMIT: usize = 32;

        let shared = empty_resync_shared_state();
        *shared.data.lock().expect("test data lock should not be poisoned") = vec![0xaa; TEST_LIMIT];
        let storage = Arc::new(LoadResyncNodeStore::new("node-a", shared.clone()));
        let file = ReplicationMetadataStore::bucket_resync_file_path("bounded-read");

        let data = ReplicationConfigStore::read_limited(storage.clone(), &file, TEST_LIMIT)
            .await
            .expect("payload ending at the read limit should succeed");
        assert_eq!(data.len(), TEST_LIMIT);
        assert_eq!(shared.stream_read_bytes.load(Ordering::SeqCst), TEST_LIMIT);

        *shared.data.lock().expect("test data lock should not be poisoned") = vec![0xbb; TEST_LIMIT * 2];
        *shared
            .reported_size
            .lock()
            .expect("test reported size lock should not be poisoned") =
            Some(i64::try_from(TEST_LIMIT).expect("test limit should fit i64"));
        shared.stream_read_bytes.store(0, Ordering::SeqCst);

        let error = ReplicationConfigStore::read_limited(storage, &file, TEST_LIMIT)
            .await
            .expect_err("an underreported oversized payload should fail");
        assert!(matches!(error, EcstoreError::CorruptedFormat));
        assert_eq!(shared.stream_read_bytes.load(Ordering::SeqCst), TEST_LIMIT + 1);
    }

    #[tokio::test]
    async fn load_bucket_resync_metadata_rejects_declared_oversize_before_body_read() {
        let shared = empty_resync_shared_state();
        *shared.data.lock().expect("test data lock should not be poisoned") = load_resync_test_metadata();
        *shared
            .reported_size
            .lock()
            .expect("test reported size lock should not be poisoned") =
            Some(i64::try_from(RESYNC_FILE_MAX_BYTES + 1).expect("resync limit should fit i64"));
        let storage = Arc::new(LoadResyncNodeStore::new("node-a", shared.clone()));

        let error = load_bucket_resync_metadata("bounded-read", storage)
            .await
            .expect_err("declared oversized resync metadata should fail");

        assert!(matches!(error, EcstoreError::CorruptedFormat));
        assert_eq!(shared.stream_read_bytes.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn load_resync_leader_lock_allows_only_one_startup_recovery() {
        temp_env::async_with_vars([(rustfs_config::ENV_OBJECT_LOCK_ACQUIRE_TIMEOUT, Some("1"))], async {
            let shared = Arc::new(LoadResyncSharedState {
                data: StdMutex::new(load_resync_test_metadata()),
                empty_object_exists: AtomicBool::new(false),
                etag_revision: AtomicUsize::new(1),
                last_put_preconditions: StdMutex::new(None),
                last_put_no_lock: AtomicBool::new(false),
                omit_etag: AtomicBool::new(false),
                conditional_write_replacements: StdMutex::new(VecDeque::new()),
                writes: StdMutex::new(Vec::new()),
                lock_manager: Arc::new(rustfs_lock::GlobalLockManager::new()),
                first_read_started: Notify::new(),
                delay_first_read: AtomicBool::new(true),
                hold_first_read: AtomicBool::new(false),
                allow_first_read: Notify::new(),
                read_count: AtomicUsize::new(0),
                reported_size: StdMutex::new(None),
                stream_read_bytes: Arc::new(AtomicUsize::new(0)),
                write_count: AtomicUsize::new(0),
                fail_next_write: AtomicBool::new(false),
                fail_after_write: AtomicBool::new(false),
                block_next_write: AtomicBool::new(false),
                write_started: Notify::new(),
                allow_write: Notify::new(),
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
    async fn corrupt_mrf_file_is_quarantined_without_overwriting_recovery_data() {
        let shared = empty_resync_shared_state();
        let corrupt = vec![0xde, 0xad, 0xbe, 0xef];
        *shared.data.lock().expect("test data lock should not be poisoned") = corrupt.clone();
        let pool = new_test_replication_pool(Arc::new(LoadResyncNodeStore::new("mrf-corrupt", shared.clone()))).await;

        pool.start_mrf_processor().await;
        let handle = pool
            .task_handles
            .lock()
            .await
            .pop()
            .expect("MRF processor task should be registered");
        handle.await.expect("MRF processor should not panic");

        let writes = shared.writes.lock().expect("test writes lock should not be poisoned");
        let (file, data) = writes.first().expect("corrupt MRF data should be quarantined");
        assert!(file.starts_with(MRF_CORRUPT_FILE_PREFIX));
        assert_eq!(data, &corrupt);
        let marker = writes
            .iter()
            .find(|(file, _)| file == ReplicationMetadataStore::MRF_REPLICATION_FILE)
            .expect("active MRF path should be cleared after quarantine");
        assert!(marker.1.is_empty(), "the active MRF path should be marked absent");
        let preconditions = shared
            .last_put_preconditions
            .lock()
            .expect("test preconditions lock should not be poisoned")
            .clone()
            .expect("active MRF cleanup should be conditional");
        assert_eq!(preconditions.if_match_value(), Some("mrf-0"));
        assert_eq!(preconditions.if_none_match_value(), None);
        assert!(shared.last_put_no_lock.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn corrupt_mrf_quarantine_preserves_a_concurrently_replaced_generation() {
        let shared = empty_resync_shared_state();
        let corrupt = vec![0xde, 0xad, 0xbe, 0xef];
        let replacement = encode_mrf_file(&[MrfReplicateEntry {
            bucket: "mrf-replacement".to_string(),
            object: "new-generation".to_string(),
            op: MrfOpKind::Object,
            ..Default::default()
        }])
        .expect("replacement MRF generation should encode");
        *shared.data.lock().expect("test data lock should not be poisoned") = corrupt;
        shared.block_next_write.store(true, Ordering::SeqCst);
        let pool = new_test_replication_pool(Arc::new(LoadResyncNodeStore::new("mrf-corrupt-race", shared.clone()))).await;
        let write_started = shared.write_started.notified();

        pool.start_mrf_processor().await;
        tokio::time::timeout(Duration::from_secs(2), write_started)
            .await
            .expect("quarantine write should block before its payload is persisted");
        *shared.data.lock().expect("test data lock should not be poisoned") = replacement.clone();
        shared.allow_write.notify_one();

        let handle = pool
            .task_handles
            .lock()
            .await
            .pop()
            .expect("MRF processor task should be registered");
        handle.await.expect("MRF processor should not panic");

        assert_eq!(
            *shared.data.lock().expect("test data lock should not be poisoned"),
            replacement,
            "quarantine cleanup must not clear a newer active MRF generation"
        );
        assert!(
            !shared
                .writes
                .lock()
                .expect("test writes lock should not be poisoned")
                .iter()
                .any(|(file, data)| file == ReplicationMetadataStore::MRF_REPLICATION_FILE && data.is_empty()),
            "the newer active generation must not be replaced with the empty marker"
        );
    }

    #[tokio::test]
    async fn corrupt_mrf_quarantine_retries_without_blocking_new_failures() {
        temp_env::async_with_vars([("RUSTFS_REPL_MRF_FLUSH_INTERVAL_MS", Some("10"))], async {
            let shared = empty_resync_shared_state();
            shared.fail_next_write.store(true, Ordering::SeqCst);
            *shared.data.lock().expect("test data lock should not be poisoned") = vec![0xde, 0xad, 0xbe, 0xef];
            let pool = new_test_replication_pool(Arc::new(LoadResyncNodeStore::new("mrf-corrupt-retry", shared.clone()))).await;

            pool.start_mrf_persister().await;
            pool.start_mrf_processor().await;
            pool.mrf_save_tx
                .send(MrfReplicateEntry {
                    bucket: "mrf-corrupt-retry".to_string(),
                    object: "new-failure".to_string(),
                    op: MrfOpKind::Object,
                    ..Default::default()
                })
                .await
                .expect("new failure should be staged during corrupt-file recovery");

            pool.mrf_save_tx
                .send(MrfReplicateEntry {
                    bucket: "mrf-corrupt-retry".to_string(),
                    object: "second-failure".to_string(),
                    op: MrfOpKind::Object,
                    ..Default::default()
                })
                .await
                .expect("persister should drain the first staged failure before recovery completes");
            let processor_handle = pool
                .task_handles
                .lock()
                .await
                .pop()
                .expect("MRF processor task should be registered");
            processor_handle.await.expect("MRF processor should retry quarantine writes");

            tokio::time::timeout(Duration::from_secs(2), async {
                loop {
                    let quarantined = {
                        let writes = shared.writes.lock().expect("test writes lock should not be poisoned");
                        writes
                            .iter()
                            .any(|(file, data)| file.starts_with(MRF_CORRUPT_FILE_PREFIX) && data == &[0xde, 0xad, 0xbe, 0xef])
                    };
                    if quarantined {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("quarantine should retry after the injected write failure");

            tokio::time::timeout(Duration::from_secs(2), async {
                loop {
                    let data = shared.data.lock().expect("test data lock should not be poisoned").clone();
                    if decode_mrf_file(&data).is_ok_and(|entries| {
                        ["new-failure", "second-failure"]
                            .iter()
                            .all(|object| entries.iter().any(|entry| entry.object == *object))
                    }) {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("staged failures should flush after quarantine recovery");

            let persister_handle = pool
                .task_handles
                .lock()
                .await
                .pop()
                .expect("MRF persister task should be registered");
            persister_handle.abort();
        })
        .await;
    }

    #[tokio::test]
    async fn mrf_persister_appends_without_waiting_for_recovery() {
        temp_env::async_with_vars([("RUSTFS_REPL_MRF_FLUSH_INTERVAL_MS", Some("10"))], async {
            let shared = empty_resync_shared_state();
            let seed = MrfReplicateEntry {
                bucket: "mrf-append-only".to_string(),
                object: "seed".to_string(),
                op: MrfOpKind::Object,
                ..Default::default()
            };
            *shared.data.lock().expect("test data lock should not be poisoned") =
                encode_mrf_file(std::slice::from_ref(&seed)).expect("seed MRF backlog should encode");
            let pool = new_test_replication_pool(Arc::new(LoadResyncNodeStore::new("mrf-append-only", shared.clone()))).await;

            pool.start_mrf_persister().await;
            pool.mrf_save_tx
                .send(MrfReplicateEntry {
                    bucket: seed.bucket.clone(),
                    object: "new-failure".to_string(),
                    op: MrfOpKind::Object,
                    ..Default::default()
                })
                .await
                .expect("new MRF failure should be accepted");
            tokio::time::timeout(Duration::from_secs(3), async {
                loop {
                    let data = shared.data.lock().expect("test data lock should not be poisoned").clone();
                    if decode_mrf_file(&data).is_ok_and(|entries| {
                        entries.len() == 2 && entries[0].object == seed.object && entries[1].object == "new-failure"
                    }) {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("persister should append without a recovery handoff");
            assert!(shared.last_put_no_lock.load(Ordering::SeqCst));

            let handle = pool
                .task_handles
                .lock()
                .await
                .pop()
                .expect("MRF persister task should be registered");
            handle.abort();
        })
        .await;
    }

    #[tokio::test]
    async fn mrf_recovery_acknowledgement_preserves_concurrent_suffix() {
        let shared = empty_resync_shared_state();
        let completed = MrfReplicateEntry {
            bucket: "mrf-recovery-prefix".to_string(),
            object: "completed".to_string(),
            op: MrfOpKind::Delete,
            force_delete_id: Some(Uuid::new_v4()),
            ..Default::default()
        };
        let retry = MrfReplicateEntry {
            object: "retry".to_string(),
            ..completed.clone()
        };
        let suffix = MrfReplicateEntry {
            object: "concurrent-suffix".to_string(),
            ..completed.clone()
        };
        let prefix = vec![completed, retry.clone()];
        *shared.data.lock().expect("test data lock should not be poisoned") =
            encode_mrf_file(&prefix).expect("MRF recovery prefix should encode");
        let storage = Arc::new(LoadResyncNodeStore::new("mrf-recovery-prefix", shared.clone()));
        let recovery_lock = storage
            .new_ns_lock(
                ReplicationMetadataStore::rustfs_meta_bucket(),
                ReplicationMetadataStore::MRF_REPLICATION_RECOVERY_LOCK,
            )
            .await
            .expect("recovery leader lock should be created");
        let recovery_guard = recovery_lock
            .get_write_lock(Duration::from_secs(1))
            .await
            .expect("recovery leader lock should be acquired");
        let mut pending_payload = None;
        assert!(
            append_mrf_entries_to_disk(std::slice::from_ref(&suffix), &storage, &mut pending_payload, &[])
                .await
                .is_some()
        );

        let retained = acknowledge_mrf_recovery(storage, &recovery_guard, &prefix, std::slice::from_ref(&retry))
            .await
            .expect("recovery acknowledgement should preserve the suffix");

        assert_eq!(retained.len(), 2);
        assert_eq!(retained[0].object, retry.object);
        assert_eq!(retained[1].object, suffix.object);
    }

    #[tokio::test]
    async fn mrf_persister_seeds_retained_startup_entries() {
        assert!(
            runtime_sources::replication_pool().is_none(),
            "test requires the runtime replication pool to be unavailable"
        );
        temp_env::async_with_vars([("RUSTFS_REPL_MRF_FLUSH_INTERVAL_MS", Some("10"))], async {
            let shared = empty_resync_shared_state();
            let retained = MrfReplicateEntry {
                bucket: "mrf-replay-seed".to_string(),
                object: "retained-delete".to_string(),
                op: MrfOpKind::Delete,
                target_arns: vec!["arn:rustfs:replication:target-a".to_string()],
                ..Default::default()
            };
            *shared.data.lock().expect("test data lock should not be poisoned") =
                encode_mrf_file(std::slice::from_ref(&retained)).expect("MRF entry should encode");
            let pool = new_test_replication_pool(Arc::new(LoadResyncNodeStore::new("mrf-seed", shared.clone()))).await;

            pool.start_mrf_persister().await;
            pool.start_mrf_processor().await;
            let processor_handle = pool
                .task_handles
                .lock()
                .await
                .pop()
                .expect("MRF processor task should be registered");
            processor_handle.await.expect("MRF processor should not panic");
            pool.mrf_save_tx
                .send(MrfReplicateEntry {
                    bucket: "mrf-replay-seed".to_string(),
                    object: "new-failure".to_string(),
                    op: MrfOpKind::Object,
                    ..Default::default()
                })
                .await
                .expect("new MRF failure should be accepted");

            tokio::time::timeout(Duration::from_secs(2), async {
                loop {
                    let persisted = {
                        let writes = shared.writes.lock().expect("test writes lock should not be poisoned");
                        writes
                            .iter()
                            .rev()
                            .find(|(file, _)| file == ReplicationMetadataStore::MRF_REPLICATION_FILE)
                            .map(|(_, data)| decode_mrf_file(data).expect("persisted MRF data should decode"))
                    };
                    if let Some(entries) = persisted
                        && entries.iter().any(|entry| entry.object == retained.object)
                        && entries.iter().any(|entry| entry.object == "new-failure")
                    {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("persister flush should retain startup entries");

            let persister_handle = pool
                .task_handles
                .lock()
                .await
                .pop()
                .expect("MRF persister task should be registered");
            persister_handle.abort();
        })
        .await;
    }

    #[tokio::test]
    async fn mrf_capped_append_retries_and_retains_existing_backlog() {
        assert!(
            runtime_sources::replication_pool().is_none(),
            "test requires the runtime replication pool to be unavailable"
        );
        temp_env::async_with_vars([("RUSTFS_REPL_MRF_FLUSH_INTERVAL_MS", Some("10"))], async {
            let shared = empty_resync_shared_state();
            let retained = (0..MRF_PENDING_CAP)
                .map(|index| MrfReplicateEntry {
                    bucket: "mrf-capped-retry".to_string(),
                    object: format!("retained-{index}"),
                    op: MrfOpKind::Object,
                    ..Default::default()
                })
                .collect::<Vec<_>>();
            *shared.data.lock().expect("test data lock should not be poisoned") =
                encode_mrf_file(&retained).expect("MRF backlog should encode");
            let pool = new_test_replication_pool(Arc::new(LoadResyncNodeStore::new("mrf-capped-retry", shared.clone()))).await;

            pool.start_mrf_persister().await;
            pool.start_mrf_processor().await;
            let processor_handle = pool
                .task_handles
                .lock()
                .await
                .pop()
                .expect("MRF processor task should be registered");
            processor_handle.await.expect("MRF processor should not panic");

            tokio::time::timeout(Duration::from_secs(30), async {
                loop {
                    if shared.write_count.load(Ordering::SeqCst) > 0 {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("startup backlog should be flushed before appending a capped batch");
            shared.fail_next_write.store(true, Ordering::SeqCst);
            pool.mrf_save_tx
                .send(MrfReplicateEntry {
                    bucket: "mrf-capped-retry".to_string(),
                    object: "new-capped-failure".to_string(),
                    op: MrfOpKind::Object,
                    ..Default::default()
                })
                .await
                .expect("new capped failure should be accepted");

            tokio::time::timeout(Duration::from_secs(30), async {
                loop {
                    let data = shared.data.lock().expect("test data lock should not be poisoned").clone();
                    if decode_mrf_file(&data).is_ok_and(|entries| {
                        entries.len() == MRF_PENDING_CAP + 1
                            && entries.first().is_some_and(|entry| entry.object == "retained-0")
                            && entries.last().is_some_and(|entry| entry.object == "new-capped-failure")
                    }) {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("a failed capped append should be retried without dropping either batch");

            let persister_handle = pool
                .task_handles
                .lock()
                .await
                .pop()
                .expect("MRF persister task should be registered");
            persister_handle.abort();
        })
        .await;
    }

    #[tokio::test]
    async fn mrf_capped_append_recognizes_a_post_commit_error_after_a_concurrent_append() {
        let shared = empty_resync_shared_state();
        let initial = MrfReplicateEntry {
            bucket: "mrf-append-idempotency".to_string(),
            object: "retained".to_string(),
            op: MrfOpKind::Object,
            ..Default::default()
        };
        let appended = MrfReplicateEntry {
            object: "new-failure".to_string(),
            ..initial.clone()
        };
        *shared.data.lock().expect("test data lock should not be poisoned") =
            encode_mrf_file(std::slice::from_ref(&initial)).expect("initial MRF backlog should encode");
        shared.fail_after_write.store(true, Ordering::SeqCst);
        let storage = Arc::new(LoadResyncNodeStore::new("mrf-append-idempotency", shared.clone()));
        let mut pending_payload = None;

        assert!(
            append_mrf_entries_to_disk(std::slice::from_ref(&appended), &storage, &mut pending_payload, &[])
                .await
                .is_none(),
            "the injected post-commit error should leave the capped batch pending"
        );
        let concurrent = MrfReplicateEntry {
            object: "concurrent-failure".to_string(),
            ..initial.clone()
        };
        let mut concurrent_payload = None;
        assert!(
            append_mrf_entries_to_disk(std::slice::from_ref(&concurrent), &storage, &mut concurrent_payload, &[])
                .await
                .is_some(),
            "a concurrent node should be able to append after the ambiguous save"
        );
        assert!(
            append_mrf_entries_to_disk(std::slice::from_ref(&appended), &storage, &mut pending_payload, &[])
                .await
                .is_some(),
            "retry should recognize the already-committed payload"
        );

        let entries = decode_mrf_file(&shared.data.lock().expect("test data lock should not be poisoned"))
            .expect("persisted MRF backlog should decode");
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].bucket, initial.bucket);
        assert_eq!(entries[0].object, initial.object);
        assert_eq!(entries[1].bucket, appended.bucket);
        assert_eq!(entries[1].object, appended.object);
        assert_eq!(entries[2].bucket, concurrent.bucket);
        assert_eq!(entries[2].object, concurrent.object);
        assert_eq!(
            shared.write_count.load(Ordering::SeqCst),
            2,
            "retry must not write the appended MRF batch twice"
        );
    }

    #[tokio::test]
    async fn mrf_capped_append_retries_after_a_conditional_generation_conflict() {
        let shared = empty_resync_shared_state();
        let initial = MrfReplicateEntry {
            bucket: "mrf-append-cas".to_string(),
            object: "retained".to_string(),
            op: MrfOpKind::Object,
            ..Default::default()
        };
        let concurrent = MrfReplicateEntry {
            object: "concurrent".to_string(),
            ..initial.clone()
        };
        let appended = MrfReplicateEntry {
            object: "new-failure".to_string(),
            ..initial.clone()
        };
        *shared.data.lock().expect("test data lock should not be poisoned") =
            encode_mrf_file(std::slice::from_ref(&initial)).expect("initial MRF backlog should encode");
        shared
            .conditional_write_replacements
            .lock()
            .expect("test replacement lock should not be poisoned")
            .push_back(encode_mrf_file(&[initial.clone(), concurrent.clone()]).expect("replacement should encode"));
        let storage = Arc::new(LoadResyncNodeStore::new("mrf-append-cas", shared.clone()));
        let mut pending_payload = None;

        assert!(
            append_mrf_entries_to_disk(std::slice::from_ref(&appended), &storage, &mut pending_payload, &[])
                .await
                .is_none(),
            "a stale capped append must not overwrite a concurrent generation"
        );
        assert!(
            append_mrf_entries_to_disk(std::slice::from_ref(&appended), &storage, &mut pending_payload, &[])
                .await
                .is_some(),
            "the capped append should retry against the concurrent generation"
        );

        let entries = decode_mrf_file(&shared.data.lock().expect("test data lock should not be poisoned"))
            .expect("persisted MRF backlog should decode");
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].object, initial.object);
        assert_eq!(entries[1].object, concurrent.object);
        assert_eq!(entries[2].object, appended.object);
    }

    #[tokio::test]
    async fn mrf_capped_append_rejects_an_existing_generation_without_an_etag() {
        let shared = empty_resync_shared_state();
        let initial = MrfReplicateEntry {
            bucket: "mrf-append-no-etag".to_string(),
            object: "retained".to_string(),
            op: MrfOpKind::Object,
            ..Default::default()
        };
        *shared.data.lock().expect("test data lock should not be poisoned") =
            encode_mrf_file(std::slice::from_ref(&initial)).expect("initial MRF backlog should encode");
        shared.omit_etag.store(true, Ordering::SeqCst);
        let storage = Arc::new(LoadResyncNodeStore::new("mrf-append-no-etag", shared.clone()));
        let appended = MrfReplicateEntry {
            object: "new-failure".to_string(),
            ..initial.clone()
        };
        let mut pending_payload = None;

        assert!(
            append_mrf_entries_to_disk(std::slice::from_ref(&appended), &storage, &mut pending_payload, &[])
                .await
                .is_none(),
            "an existing MRF generation without an ETag must fail closed"
        );
        assert!(
            shared
                .writes
                .lock()
                .expect("test writes lock should not be poisoned")
                .is_empty(),
            "the un-fenced capped append must not write"
        );
        let entries = decode_mrf_file(&shared.data.lock().expect("test data lock should not be poisoned"))
            .expect("the original MRF backlog should remain readable");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].object, initial.object);
    }

    #[tokio::test]
    async fn mrf_capped_append_replaces_an_existing_empty_generation_conditionally() {
        let shared = empty_resync_shared_state();
        shared.empty_object_exists.store(true, Ordering::SeqCst);
        let storage = Arc::new(LoadResyncNodeStore::new("mrf-append-empty", shared.clone()));
        let appended = MrfReplicateEntry {
            bucket: "mrf-append-empty".to_string(),
            object: "new-failure".to_string(),
            op: MrfOpKind::Object,
            ..Default::default()
        };
        let mut pending_payload = None;

        assert!(
            append_mrf_entries_to_disk(std::slice::from_ref(&appended), &storage, &mut pending_payload, &[])
                .await
                .is_some(),
            "an existing empty MRF generation should be replaced"
        );
        let preconditions = shared
            .last_put_preconditions
            .lock()
            .expect("test preconditions lock should not be poisoned")
            .clone()
            .expect("the empty generation replacement should be conditional");
        assert_eq!(preconditions.if_match_value(), Some("mrf-0"));
        assert_eq!(preconditions.if_none_match_value(), None);
        assert!(shared.last_put_no_lock.load(Ordering::SeqCst));
        let entries = decode_mrf_file(&shared.data.lock().expect("test data lock should not be poisoned"))
            .expect("the replaced MRF backlog should decode");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].object, appended.object);
    }

    #[tokio::test]
    async fn mrf_capped_append_recovers_a_late_corrupt_generation() {
        let shared = empty_resync_shared_state();
        let retained = MrfReplicateEntry {
            bucket: "mrf-late-corruption".to_string(),
            object: "retained".to_string(),
            op: MrfOpKind::Object,
            ..Default::default()
        };
        let appended = MrfReplicateEntry {
            bucket: retained.bucket.clone(),
            object: "appended-after-corruption".to_string(),
            op: MrfOpKind::Object,
            ..Default::default()
        };
        *shared.data.lock().expect("test data lock should not be poisoned") = vec![0xde, 0xad, 0xbe, 0xef];
        let storage = Arc::new(LoadResyncNodeStore::new("mrf-late-corruption", shared.clone()));
        let mut pending_payload = None;

        assert!(
            append_mrf_entries_to_disk(
                std::slice::from_ref(&appended),
                &storage,
                &mut pending_payload,
                std::slice::from_ref(&retained),
            )
            .await
            .is_some(),
            "a corrupt active generation should be quarantined and rebuilt"
        );

        let writes = shared.writes.lock().expect("test writes lock should not be poisoned");
        assert!(
            writes
                .iter()
                .any(|(file, data)| file.starts_with(MRF_CORRUPT_FILE_PREFIX) && data == &[0xde, 0xad, 0xbe, 0xef]),
            "the corrupt active generation should be retained in quarantine"
        );
        let recovered = decode_mrf_file(&shared.data.lock().expect("test data lock should not be poisoned"))
            .expect("the active MRF generation should be rebuilt");
        assert_eq!(recovered.len(), 2);
        assert_eq!(recovered[0].object, retained.object);
        assert_eq!(recovered[1].object, appended.object);
    }

    #[tokio::test]
    async fn mrf_recovery_leader_lock_allows_only_one_processor() {
        assert!(
            runtime_sources::replication_pool().is_none(),
            "test requires the runtime replication pool to be unavailable"
        );
        temp_env::async_with_vars([(rustfs_config::ENV_OBJECT_LOCK_ACQUIRE_TIMEOUT, Some("1"))], async {
            let shared = empty_resync_shared_state();
            let entry = MrfReplicateEntry {
                bucket: "mrf-recovery-leader".to_string(),
                object: "pending".to_string(),
                op: MrfOpKind::Object,
                ..Default::default()
            };
            *shared.data.lock().expect("test data lock should not be poisoned") =
                encode_mrf_file(std::slice::from_ref(&entry)).expect("MRF entry should encode");
            shared.delay_first_read.store(true, Ordering::SeqCst);
            let leader_pool = new_test_replication_pool(Arc::new(LoadResyncNodeStore::new("mrf-leader-a", shared.clone()))).await;
            let skipped_pool =
                new_test_replication_pool(Arc::new(LoadResyncNodeStore::new("mrf-leader-b", shared.clone()))).await;

            leader_pool.start_mrf_processor().await;
            tokio::time::timeout(Duration::from_secs(2), shared.first_read_started.notified())
                .await
                .expect("the leader should start reading the MRF backlog");
            skipped_pool.start_mrf_processor().await;

            let skipped_handle = skipped_pool
                .task_handles
                .lock()
                .await
                .pop()
                .expect("MRF processor task should be registered for skipped node");
            skipped_handle.await.expect("skipped MRF processor should not panic");

            let leader_handle = leader_pool
                .task_handles
                .lock()
                .await
                .pop()
                .expect("MRF processor task should be registered for leader");
            leader_handle.await.expect("leader MRF processor should not panic");

            assert_eq!(
                shared.read_count.load(Ordering::SeqCst),
                2,
                "only the leader should read the backlog: once for replay and once for acknowledgement"
            );
        })
        .await;
    }

    #[tokio::test]
    async fn mrf_recovery_acknowledgement_rejects_changed_prefix() {
        let shared = empty_resync_shared_state();
        let completed = MrfReplicateEntry {
            bucket: "mrf-prefix-changed".to_string(),
            object: "completed".to_string(),
            retry_count: 1,
            op: MrfOpKind::Object,
            ..Default::default()
        };
        let retry = MrfReplicateEntry {
            object: "retry".to_string(),
            ..completed.clone()
        };
        let mut changed = completed.clone();
        changed.retry_count = 2;
        let prefix = vec![completed, retry.clone()];
        let current = vec![changed, retry];
        *shared.data.lock().expect("test data lock should not be poisoned") =
            encode_mrf_file(&current).expect("changed MRF prefix should encode");
        let storage = Arc::new(LoadResyncNodeStore::new("mrf-prefix-changed", shared.clone()));
        let recovery_lock = storage
            .new_ns_lock(
                ReplicationMetadataStore::rustfs_meta_bucket(),
                ReplicationMetadataStore::MRF_REPLICATION_RECOVERY_LOCK,
            )
            .await
            .expect("recovery leader lock should be created");
        let recovery_guard = recovery_lock
            .get_write_lock(Duration::from_secs(1))
            .await
            .expect("recovery leader lock should be acquired");

        let error = acknowledge_mrf_recovery(storage.clone(), &recovery_guard, &prefix, &[])
            .await
            .expect_err("acknowledgement must reject a changed recovery prefix");

        assert!(error.to_string().contains("prefix changed"));
        let persisted = decode_mrf_file(&shared.data.lock().expect("test data lock should not be poisoned"))
            .expect("changed MRF prefix should remain readable");
        assert_eq!(persisted, current);
    }

    #[tokio::test]
    async fn mrf_recovery_acknowledgement_write_failure_preserves_prefix() {
        let shared = empty_resync_shared_state();
        let entry = MrfReplicateEntry {
            bucket: "mrf-ack-write-failure".to_string(),
            object: "completed".to_string(),
            op: MrfOpKind::Object,
            ..Default::default()
        };
        let prefix = vec![entry.clone()];
        *shared.data.lock().expect("test data lock should not be poisoned") =
            encode_mrf_file(&prefix).expect("MRF prefix should encode");
        shared.fail_next_write.store(true, Ordering::SeqCst);
        let storage = Arc::new(LoadResyncNodeStore::new("mrf-ack-write-failure", shared.clone()));
        let recovery_lock = storage
            .new_ns_lock(
                ReplicationMetadataStore::rustfs_meta_bucket(),
                ReplicationMetadataStore::MRF_REPLICATION_RECOVERY_LOCK,
            )
            .await
            .expect("recovery leader lock should be created");
        let recovery_guard = recovery_lock
            .get_write_lock(Duration::from_secs(1))
            .await
            .expect("recovery leader lock should be acquired");

        assert!(
            acknowledge_mrf_recovery(storage, &recovery_guard, &prefix, &[])
                .await
                .is_err(),
            "acknowledgement must fail closed when the conditional save fails"
        );
        let persisted = decode_mrf_file(&shared.data.lock().expect("test data lock should not be poisoned"))
            .expect("the original MRF prefix should remain readable");
        assert_eq!(persisted, prefix);
    }

    #[tokio::test]
    async fn mrf_appenders_accumulate_without_overwriting_each_other() {
        let shared = empty_resync_shared_state();
        let first = MrfReplicateEntry {
            bucket: "mrf-multi-writer".to_string(),
            object: "first".to_string(),
            op: MrfOpKind::Object,
            ..Default::default()
        };
        let second = MrfReplicateEntry {
            object: "second".to_string(),
            ..first.clone()
        };
        let first_storage = Arc::new(LoadResyncNodeStore::new("mrf-writer-a", shared.clone()));
        let second_storage = Arc::new(LoadResyncNodeStore::new("mrf-writer-b", shared.clone()));
        let mut first_payload = None;
        let mut second_payload = None;

        assert!(
            append_mrf_entries_to_disk(std::slice::from_ref(&first), &first_storage, &mut first_payload, &[])
                .await
                .is_some()
        );
        assert!(
            append_mrf_entries_to_disk(std::slice::from_ref(&second), &second_storage, &mut second_payload, &[])
                .await
                .is_some()
        );

        let persisted = decode_mrf_file(&shared.data.lock().expect("test data lock should not be poisoned"))
            .expect("combined MRF backlog should decode");
        assert_eq!(persisted, vec![first, second]);
    }

    #[test]
    fn mrf_recovery_prefix_matching_checks_all_persisted_fields() {
        let original = MrfReplicateEntry {
            bucket: "mrf-prefix".to_string(),
            object: "object".to_string(),
            retry_count: 1,
            size: 10,
            op: MrfOpKind::Object,
            ..Default::default()
        };
        let mut changed = original.clone();
        changed.retry_count = 2;
        assert!(!mrf_prefix_matches(&[changed], std::slice::from_ref(&original)));

        let mut suffix = original.clone();
        suffix.object = "suffix".to_string();
        assert!(mrf_prefix_matches(&[original.clone(), suffix], &[original]));
    }

    #[tokio::test]
    async fn mrf_delete_replay_retry_is_retained_on_disk_when_runtime_pool_is_unavailable() {
        assert!(
            runtime_sources::replication_pool().is_none(),
            "test requires the runtime replication pool to be unavailable"
        );
        let shared = empty_resync_shared_state();
        let entry = MrfReplicateEntry {
            bucket: "mrf-replay-retry".to_string(),
            object: "destructive-delete".to_string(),
            op: MrfOpKind::Delete,
            target_arns: vec!["arn:rustfs:replication:target-a".to_string()],
            ..Default::default()
        };
        *shared.data.lock().expect("test data lock should not be poisoned") =
            encode_mrf_file(std::slice::from_ref(&entry)).expect("MRF entry should encode");
        let pool = new_test_replication_pool(Arc::new(LoadResyncNodeStore::new("mrf-retry", shared.clone()))).await;

        pool.start_mrf_processor().await;
        let handle = pool
            .task_handles
            .lock()
            .await
            .pop()
            .expect("MRF processor task should be registered");
        handle.await.expect("MRF processor should not panic");
        let retained = decode_mrf_file(&shared.data.lock().expect("test data lock should not be poisoned"))
            .expect("processor should keep retry entries readable")
            .pop()
            .expect("the unavailable runtime pool should retain the entry on disk");
        assert_eq!(retained.bucket, entry.bucket);
        assert_eq!(retained.object, entry.object);
        assert_eq!(retained.version_id, entry.version_id);
        assert_eq!(retained.target_arns, entry.target_arns);
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
        let storage = Arc::new(LoadResyncNodeStore::new("force-delete-journal", shared.clone()));
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
        let preconditions = shared
            .last_put_preconditions
            .lock()
            .expect("test preconditions lock should not be poisoned")
            .clone()
            .expect("first journal append should be conditional");
        assert_eq!(preconditions.if_none_match_value(), Some("*"));
        assert_eq!(preconditions.if_match_value(), None);
        assert!(!shared.last_put_no_lock.load(Ordering::SeqCst));
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
        let preconditions = shared
            .last_put_preconditions
            .lock()
            .expect("test preconditions lock should not be poisoned")
            .clone()
            .expect("commit marker should be conditional");
        assert_eq!(preconditions.if_none_match_value(), None);
        assert_eq!(preconditions.if_match_value(), Some("mrf-1"));
        assert!(!shared.last_put_no_lock.load(Ordering::SeqCst));
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

    #[tokio::test]
    async fn force_delete_intent_cleanup_retries_after_a_stale_journal_snapshot() {
        let shared = empty_resync_shared_state();
        let storage = Arc::new(LoadResyncNodeStore::new("force-delete-journal", shared.clone()));
        let operation_id = Uuid::new_v4();
        let mut entry = MrfReplicateEntry {
            bucket: "source".to_string(),
            object: "original".to_string(),
            force_delete_id: Some(operation_id),
            op: MrfOpKind::Delete,
            ..Default::default()
        };
        persist_force_delete_intent(storage.clone(), entry.clone())
            .await
            .expect("journal append should succeed");
        commit_force_delete_intent(storage.clone(), operation_id)
            .await
            .expect("journal commit should succeed");
        entry.force_delete_local_commit = true;
        let concurrent = MrfReplicateEntry {
            bucket: "source".to_string(),
            object: "concurrent".to_string(),
            force_delete_id: Some(Uuid::new_v4()),
            op: MrfOpKind::Delete,
            ..Default::default()
        };
        shared
            .conditional_write_replacements
            .lock()
            .expect("test replacement lock should not be poisoned")
            .push_back(encode_mrf_file(&[entry, concurrent.clone()]).expect("concurrent journal entries should encode"));

        complete_force_delete_intent(storage.clone(), operation_id)
            .await
            .expect("cleanup should retry after a concurrent journal update");

        let data = ReplicationConfigStore::read(storage, ReplicationMetadataStore::FORCE_DELETE_REPLICATION_FILE)
            .await
            .expect("journal should remain readable");
        let entries = decode_mrf_file(&data).expect("journal should decode");
        assert_eq!(entries.len(), 1, "cleanup must preserve only the concurrent journal entry");
        assert_eq!(entries[0].force_delete_id, concurrent.force_delete_id);
        assert_eq!(entries[0].object, concurrent.object);
    }

    #[tokio::test]
    async fn force_delete_intent_commit_retries_past_the_bounded_cas_conflict_limit() {
        let shared = empty_resync_shared_state();
        let storage = Arc::new(LoadResyncNodeStore::new("force-delete-journal", shared.clone()));
        let operation_id = Uuid::new_v4();
        let entry = MrfReplicateEntry {
            bucket: "source".to_string(),
            object: "original".to_string(),
            force_delete_id: Some(operation_id),
            op: MrfOpKind::Delete,
            ..Default::default()
        };
        persist_force_delete_intent(storage.clone(), entry.clone())
            .await
            .expect("journal append should succeed");
        {
            let mut replacements = shared
                .conditional_write_replacements
                .lock()
                .expect("test replacement lock should not be poisoned");
            for object in ["first", "second", "third"] {
                let mut replacement = entry.clone();
                replacement.object = object.to_string();
                replacements.push_back(encode_mrf_file(&[replacement]).expect("concurrent journal entry should encode"));
            }
        }

        commit_force_delete_intent(storage.clone(), operation_id)
            .await
            .expect("commit marker must retry until it is durable");

        let data = ReplicationConfigStore::read(storage, ReplicationMetadataStore::FORCE_DELETE_REPLICATION_FILE)
            .await
            .expect("journal should remain readable");
        let entries = decode_mrf_file(&data).expect("journal should decode");
        assert_eq!(entries.len(), 1);
        assert!(entries[0].force_delete_local_commit);
        assert_eq!(entries[0].force_delete_id, Some(operation_id));
    }

    #[tokio::test]
    async fn force_delete_intent_rejects_existing_journal_without_an_etag() {
        let shared = empty_resync_shared_state();
        let storage = Arc::new(LoadResyncNodeStore::new("force-delete-journal", shared.clone()));
        let operation_id = Uuid::new_v4();
        let entry = MrfReplicateEntry {
            bucket: "source".to_string(),
            object: "original".to_string(),
            force_delete_id: Some(operation_id),
            op: MrfOpKind::Delete,
            ..Default::default()
        };
        persist_force_delete_intent(storage.clone(), entry)
            .await
            .expect("journal append should succeed");
        let writes_before = shared.write_count.load(Ordering::SeqCst);
        shared.omit_etag.store(true, Ordering::SeqCst);

        let err = commit_force_delete_intent(storage.clone(), operation_id)
            .await
            .expect_err("missing ETag must reject journal mutation");
        assert!(err.to_string().contains("no ETag"));
        assert_eq!(shared.write_count.load(Ordering::SeqCst), writes_before);
        let data = ReplicationConfigStore::read(storage, ReplicationMetadataStore::FORCE_DELETE_REPLICATION_FILE)
            .await
            .expect("journal should remain readable");
        let entries = decode_mrf_file(&data).expect("journal should decode");
        assert!(!entries[0].force_delete_local_commit);
    }

    #[test]
    fn force_delete_journal_rejects_a_lost_transaction_lease() {
        let err = ensure_force_delete_journal_lock_held(true).expect_err("lost transaction lease must fence the journal write");

        assert!(err.to_string().contains("lock lost"));
    }

    #[test]
    fn mrf_journal_rejects_a_lost_transaction_lease() {
        let err = ensure_mrf_journal_lock_held(true).expect_err("lost MRF lease must fence the journal write");

        assert!(err.to_string().contains("lock lost"));
    }

    #[tokio::test]
    async fn force_delete_journal_rejects_a_stale_conditional_write() {
        let shared = empty_resync_shared_state();
        let storage = Arc::new(LoadResyncNodeStore::new("force-delete-journal", shared));
        let file = ReplicationMetadataStore::FORCE_DELETE_REPLICATION_FILE;
        let original = MrfReplicateEntry {
            bucket: "source".to_string(),
            object: "original".to_string(),
            force_delete_id: Some(Uuid::new_v4()),
            op: MrfOpKind::Delete,
            ..Default::default()
        };
        ReplicationConfigStore::save(
            storage.clone(),
            file,
            encode_mrf_file(&[original]).expect("initial journal entry should encode"),
        )
        .await
        .expect("initial journal write should succeed");

        let (_, object_info) = ReplicationConfigStore::read_no_lock_with_metadata(storage.clone(), file)
            .await
            .expect("journal snapshot should include an ETag");
        let stale_preconditions = HTTPPreconditions {
            if_match: object_info.etag,
            ..Default::default()
        };
        let replacement = MrfReplicateEntry {
            bucket: "source".to_string(),
            object: "replacement".to_string(),
            force_delete_id: Some(Uuid::new_v4()),
            op: MrfOpKind::Delete,
            ..Default::default()
        };
        let replacement_data = encode_mrf_file(&[replacement]).expect("replacement journal entry should encode");
        ReplicationConfigStore::save(storage.clone(), file, replacement_data.clone())
            .await
            .expect("concurrent journal write should succeed");

        let err = ReplicationConfigStore::save_conditional(
            storage.clone(),
            file,
            encode_mrf_file(&[]).expect("empty journal should encode"),
            stale_preconditions,
        )
        .await
        .expect_err("stale journal snapshot must not overwrite newer data");
        assert_eq!(err, EcstoreError::PreconditionFailed);
        assert_eq!(
            ReplicationConfigStore::read(storage, file)
                .await
                .expect("newer journal data should remain readable"),
            replacement_data
        );
    }
}
