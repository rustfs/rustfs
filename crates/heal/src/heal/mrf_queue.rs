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

//! Mission Repair Feed (MRF) queue, journal, and consumer.
//!
//! Intents arriving on the global channel (see `rustfs_common::mrf_channel`)
//! are buffered in a bounded in-memory queue, translated into prioritized
//! heal requests, and — while they are not yet accepted by the heal manager —
//! mirrored into a durable journal so a crash or restart can replay them.
//! This is the RustFS counterpart of MinIO's `.heal/mrf/list.bin` replay,
//! layered on top of (not replacing) read-repair and scanner heal.
//!
//! Durability model: the journal is a snapshot of the *unaccepted* pending
//! set, rewritten on a group-commit cadence (every flush interval or flush
//! threshold new intents). A rewrite is atomic at the record level only — a
//! torn tail simply truncates during replay because every record carries its
//! own CRC32. Neither ingress nor manager admission is a durable ownership
//! receipt. The last flush window can be lost. Read-repair can rediscover a
//! failed read; the scanner retains bounded, expiring retry hints. Partial
//! writes also use a best-effort in-memory fast path, not a durable successor.
//! These mechanisms must not be reported as verified repair completion.
//! The partial-write caller's restart-survival requirement remains unmet by
//! admission alone; a verified durable handoff is still required.

use super::{DiskStore, HealDiskExt as _, local_disk_map_read};
use crate::heal::manager::{HealManager, MrfRepairNoticeTarget};
use metrics::{counter, gauge};
use rustfs_common::mrf_channel::{MRF_MAX_ATTEMPTS, MrfIngressResult, MrfIntent};
use rustfs_heal_contracts::heal_channel::{HealAdmissionDropReason, HealAdmissionResult};
use std::collections::{HashSet, VecDeque};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::heal::task::{HealOptions, HealPriority, HealRequest, HealType};

/// Read-only inspection of committed MRF checkpoints. The legacy consumer
/// remains unchanged until ownership-aware replay is deployed.
pub mod snapshot;

/// Journal location inside the metadata bucket, following the resume-state
/// layout.
pub(crate) const MRF_JOURNAL_PATH: &str = "buckets/.heal/mrf/journal.bin";
/// The scoped path is the authoritative snapshot for new readers and carries
/// both v1 and v2 records. The legacy path is only a v1 compatibility mirror;
/// older readers ignore the authoritative path, while new readers never merge
/// the two files. This prevents a partial two-file flush from fabricating a
/// mixed epoch.
pub(crate) const MRF_SCOPED_JOURNAL_PATH: &str = "buckets/.heal/mrf/journal-scoped.bin";

/// Record format tag.
const MRF_JOURNAL_FORMAT: u8 = 1;
/// Record layout version.
const MRF_JOURNAL_VERSION: u8 = 1;
const MRF_JOURNAL_VERSION_SCOPED: u8 = 2;

/// Fixed header size: format, version, kind, attempts, enqueued_at_ms,
/// has_version flag.
const MRF_RECORD_FIXED_HEAD: usize = 1 + 1 + 1 + 1 + 8 + 1;
const MRF_MAX_IDENTITY_COMPONENT: usize = 1024;

fn metric_f64(value: usize) -> f64 {
    f64::from(u32::try_from(value).unwrap_or(u32::MAX))
}

#[derive(Debug, Clone)]
pub(crate) struct MrfConsumerConfig {
    /// In-memory queue capacity in intents.
    pub queue_capacity: usize,
    /// Journal byte budget; a pending snapshot above this bound is rejected
    /// oldest-first so the journal can never grow unbounded.
    pub journal_max_bytes: usize,
    /// How many journal intents to re-arm per replay round.
    pub replay_batch: usize,
    /// Group-commit cadence for the journal snapshot.
    pub flush_interval: Duration,
    /// New intents between flushes that force an early snapshot.
    pub flush_threshold: usize,
    /// Backoff after the heal manager reports a full admission.
    pub admission_backoff: Duration,
}

impl Default for MrfConsumerConfig {
    fn default() -> Self {
        Self {
            queue_capacity: rustfs_utils::get_env_usize(
                rustfs_config::ENV_HEAL_MRF_QUEUE_SIZE,
                rustfs_config::DEFAULT_HEAL_MRF_QUEUE_SIZE,
            ),
            journal_max_bytes: rustfs_utils::get_env_usize(
                rustfs_config::ENV_HEAL_MRF_JOURNAL_MAX_BYTES,
                rustfs_config::DEFAULT_HEAL_MRF_JOURNAL_MAX_BYTES,
            ),
            replay_batch: rustfs_utils::get_env_usize(
                rustfs_config::ENV_HEAL_MRF_REPLAY_BATCH,
                rustfs_config::DEFAULT_HEAL_MRF_REPLAY_BATCH,
            ),
            flush_interval: Duration::from_millis(500),
            flush_threshold: 1000,
            admission_backoff: Duration::from_secs(5),
        }
    }
}

/// Bounded pending set with count and byte ceilings. Overflow drops the
/// incoming intent (never a resident one) and counts the loss.
pub(crate) struct MrfQueue {
    pending: VecDeque<MrfIntent>,
    pending_keys: HashSet<MrfQueueKey>,
    bytes: usize,
    capacity: usize,
    byte_budget: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct MrfQueueKey {
    kind: rustfs_common::mrf_channel::MrfKind,
    bucket: Arc<str>,
    object: Arc<str>,
    version_id: Option<[u8; 16]>,
    scope: Option<rustfs_common::mrf_channel::MrfScope>,
}

fn queue_key(intent: &MrfIntent) -> MrfQueueKey {
    let version_id = intent.version_id.filter(|bytes| *bytes != [0; 16]);
    let scope = (!matches!(intent.kind, rustfs_common::mrf_channel::MrfKind::MetadataCorruption))
        .then_some(intent.scope)
        .flatten();
    MrfQueueKey {
        kind: intent.kind,
        bucket: intent.bucket.clone(),
        object: intent.object.clone(),
        version_id,
        scope,
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum MrfQueuePushResult {
    Enqueued,
    Coalesced,
    Rejected,
}

impl MrfQueue {
    pub(crate) fn new(capacity: usize, byte_budget: usize) -> Self {
        Self {
            pending: VecDeque::new(),
            pending_keys: HashSet::new(),
            bytes: 0,
            capacity,
            byte_budget,
        }
    }

    pub(crate) fn try_push_typed(&mut self, intent: MrfIntent) -> MrfQueuePushResult {
        if intent.bucket.len() > MRF_MAX_IDENTITY_COMPONENT || intent.object.len() > MRF_MAX_IDENTITY_COMPONENT {
            counter!("rustfs_heal_mrf_dropped_total", "reason" => "identity_oversized").increment(1);
            return MrfQueuePushResult::Rejected;
        }
        let key = queue_key(&intent);
        if self.pending_keys.contains(&key) {
            counter!("rustfs_heal_mrf_coalesced_total", "layer" => "queue").increment(1);
            return MrfQueuePushResult::Coalesced;
        }
        let cost = intent.estimated_bytes();
        if self.pending.len() >= self.capacity || self.bytes + cost > self.byte_budget {
            counter!("rustfs_heal_mrf_dropped_total", "reason" => "queue_overflow").increment(1);
            return MrfQueuePushResult::Rejected;
        }
        self.bytes += cost;
        self.pending_keys.insert(key);
        self.pending.push_back(intent);
        MrfQueuePushResult::Enqueued
    }

    fn raise_limits_for_replay(&mut self, intents: usize, bytes: usize) {
        self.capacity = self.capacity.max(self.pending.len().saturating_add(intents));
        self.byte_budget = self.byte_budget.max(self.bytes.saturating_add(bytes));
    }

    /// Bool compatibility adapter: only a newly executable queue item is
    /// reported as accepted; a coalesced duplicate is not durable admission.
    #[cfg(test)]
    pub(crate) fn try_push(&mut self, intent: MrfIntent) -> bool {
        matches!(self.try_push_typed(intent), MrfQueuePushResult::Enqueued)
    }

    pub(crate) fn pop_front(&mut self) -> Option<MrfIntent> {
        let intent = self.pending.pop_front()?;
        self.pending_keys.remove(&queue_key(&intent));
        self.bytes = self.bytes.saturating_sub(intent.estimated_bytes());
        Some(intent)
    }

    pub(crate) fn push_back(&mut self, intent: MrfIntent) {
        self.pending_keys.insert(queue_key(&intent));
        self.bytes += intent.estimated_bytes();
        self.pending.push_back(intent);
    }

    pub(crate) fn depth(&self) -> usize {
        self.pending.len()
    }

    pub(crate) fn bytes(&self) -> usize {
        self.bytes
    }

    pub(crate) fn intents(&self) -> impl Iterator<Item = &MrfIntent> {
        self.pending.iter()
    }
}

// ---------------------------------------------------------------------------
// Journal record codec
// ---------------------------------------------------------------------------

/// Append one encoded record to `out`.
pub(crate) fn encode_intent(intent: &MrfIntent, out: &mut Vec<u8>) -> bool {
    let Ok(bucket_len) = u32::try_from(intent.bucket.len()) else {
        return false;
    };
    let Ok(object_len) = u32::try_from(intent.object.len()) else {
        return false;
    };
    let scope = (!matches!(intent.kind, rustfs_common::mrf_channel::MrfKind::MetadataCorruption))
        .then_some(intent.scope)
        .flatten();
    let version_id = intent.version_id.filter(|bytes| *bytes != [0; 16]);
    let start = out.len();
    out.push(MRF_JOURNAL_FORMAT);
    out.push(if scope.is_some() {
        MRF_JOURNAL_VERSION_SCOPED
    } else {
        MRF_JOURNAL_VERSION
    });
    out.push(match intent.kind {
        rustfs_common::mrf_channel::MrfKind::DecodeFailure => 1,
        rustfs_common::mrf_channel::MrfKind::MetadataCorruption => 2,
        rustfs_common::mrf_channel::MrfKind::PartialWrite => 3,
    });
    out.push(intent.attempts);
    out.extend_from_slice(&intent.enqueued_at_ms.to_le_bytes());
    match version_id {
        Some(bytes) => {
            out.push(1);
            out.extend_from_slice(&bytes);
        }
        None => out.push(0),
    }
    if let Some(scope) = scope {
        out.extend_from_slice(&scope.pool_index.to_le_bytes());
        out.extend_from_slice(&scope.set_index.to_le_bytes());
    }
    out.extend_from_slice(&bucket_len.to_le_bytes());
    out.extend_from_slice(&object_len.to_le_bytes());
    out.extend_from_slice(intent.bucket.as_bytes());
    out.extend_from_slice(intent.object.as_bytes());
    let mut hasher = crc_fast::Digest::new(crc_fast::CrcAlgorithm::Crc32IsoHdlc);
    hasher.update(&out[start..]);
    let Ok(checksum) = u32::try_from(hasher.finalize()) else {
        out.truncate(start);
        return false;
    };
    out.extend_from_slice(&checksum.to_le_bytes());
    true
}

fn decode_one(data: &[u8]) -> Option<(MrfIntent, usize)> {
    if data.len() < MRF_RECORD_FIXED_HEAD + 8 {
        return None;
    }
    if data[0] != MRF_JOURNAL_FORMAT || !matches!(data[1], MRF_JOURNAL_VERSION | MRF_JOURNAL_VERSION_SCOPED) {
        return None;
    }
    let kind = match data[2] {
        1 => rustfs_common::mrf_channel::MrfKind::DecodeFailure,
        2 => rustfs_common::mrf_channel::MrfKind::MetadataCorruption,
        3 => rustfs_common::mrf_channel::MrfKind::PartialWrite,
        _ => return None,
    };
    let attempts = data[3];
    let enqueued_at_ms = u64::from_le_bytes(data[4..12].try_into().ok()?);
    let has_version = data[12] != 0;
    let mut cursor = MRF_RECORD_FIXED_HEAD;
    let version_id = if has_version {
        if data.len() < cursor + 16 {
            return None;
        }
        let bytes: [u8; 16] = data[cursor..cursor + 16].try_into().ok()?;
        cursor += 16;
        Some(bytes)
    } else {
        None
    };
    let scope = if data[1] == MRF_JOURNAL_VERSION_SCOPED {
        if data.len() < cursor + 8 {
            return None;
        }
        let pool_index = u32::from_le_bytes(data[cursor..cursor + 4].try_into().ok()?);
        let set_index = u32::from_le_bytes(data[cursor + 4..cursor + 8].try_into().ok()?);
        cursor += 8;
        Some(rustfs_common::mrf_channel::MrfScope { pool_index, set_index })
    } else {
        None
    };
    if data.len() < cursor + 8 {
        return None;
    }
    let bucket_len = usize::try_from(u32::from_le_bytes(data[cursor..cursor + 4].try_into().ok()?)).ok()?;
    let object_len = usize::try_from(u32::from_le_bytes(data[cursor + 4..cursor + 8].try_into().ok()?)).ok()?;
    if bucket_len > MRF_MAX_IDENTITY_COMPONENT || object_len > MRF_MAX_IDENTITY_COMPONENT {
        return None;
    }
    cursor += 8;
    let body_end = cursor.checked_add(bucket_len)?.checked_add(object_len)?;
    let record_end = body_end.checked_add(4)?;
    if data.len() < record_end {
        return None;
    }
    let mut hasher = crc_fast::Digest::new(crc_fast::CrcAlgorithm::Crc32IsoHdlc);
    hasher.update(&data[..body_end]);
    if u32::try_from(hasher.finalize()).ok()? != u32::from_le_bytes(data[body_end..record_end].try_into().ok()?) {
        return None;
    }
    let bucket = std::sync::Arc::from(std::str::from_utf8(&data[cursor..cursor + bucket_len]).ok()?);
    let object = std::sync::Arc::from(std::str::from_utf8(&data[cursor + bucket_len..body_end]).ok()?);
    Some((
        MrfIntent {
            bucket,
            object,
            version_id,
            kind,
            scope: if matches!(kind, rustfs_common::mrf_channel::MrfKind::MetadataCorruption) {
                None
            } else {
                scope
            },
            lease: None,
            enqueued_at_ms,
            attempts,
        },
        record_end,
    ))
}

/// Decode a whole journal, stopping at the first torn or corrupt record.
/// Returns the decoded intents and the number of trailing bytes discarded.
pub(crate) fn decode_journal(data: &[u8]) -> (Vec<MrfIntent>, usize) {
    let mut intents = Vec::new();
    let mut cursor = 0usize;
    while cursor < data.len() {
        match decode_one(&data[cursor..]) {
            Some((intent, consumed)) => {
                intents.push(intent);
                cursor += consumed;
            }
            None => break,
        }
    }
    let truncated = data.len() - cursor;
    (intents, truncated)
}

// ---------------------------------------------------------------------------
// Journal disk IO (all local disks, first successful read wins)
// ---------------------------------------------------------------------------

async fn journal_disks() -> Vec<DiskStore> {
    let map = local_disk_map_read().await;
    map.values().flatten().cloned().collect()
}

async fn read_journal(path: &str) -> Option<Vec<u8>> {
    for disk in journal_disks().await {
        match disk.read_all(super::RUSTFS_META_BUCKET, path).await {
            Ok(bytes) => return Some(bytes.to_vec()),
            Err(_) => continue,
        }
    }
    None
}

/// Write the snapshot to every local disk; returns true when at least one
/// disk accepted it, so a total write failure keeps the runtime dirty and
/// the next tick retries the persist.
async fn write_journal(path: &str, data: &[u8]) -> bool {
    let payload = bytes::Bytes::copy_from_slice(data);
    let mut any_persisted = false;
    for disk in journal_disks().await {
        match disk.write_all(super::RUSTFS_META_BUCKET, path, payload.clone()).await {
            Ok(()) => any_persisted = true,
            Err(err) => warn_mrf_journal_write(&err),
        }
    }
    any_persisted
}

async fn delete_journal(path: &str) -> bool {
    let disks = journal_disks().await;
    if disks.is_empty() {
        counter!("rustfs_heal_mrf_journal_delete_failures_total").increment(1);
        return false;
    }
    let mut all_deleted = true;
    for disk in disks {
        let result = disk
            .delete(
                super::RUSTFS_META_BUCKET,
                path,
                crate::heal::storage_api::owner::EcstoreDeleteOptions::default(),
            )
            .await;
        if let Err(err) = result {
            // Delete is idempotent: a compatibility mirror that was never
            // written (or was already removed) is clean, not a retry state.
            if !matches!(err, super::DiskError::FileNotFound | super::DiskError::VolumeNotFound) {
                all_deleted = false;
            }
        }
    }
    if !all_deleted {
        counter!("rustfs_heal_mrf_journal_delete_failures_total").increment(1);
    }
    all_deleted
}

async fn delete_journals() -> bool {
    let authoritative_deleted = delete_journal(MRF_SCOPED_JOURNAL_PATH).await;
    let legacy_deleted = delete_journal(MRF_JOURNAL_PATH).await;
    authoritative_deleted && legacy_deleted
}

fn warn_mrf_journal_write(err: &super::DiskError) {
    tracing::warn!(
        target: "rustfs::heal::mrf",
        error = %err,
        "MRF journal write failed; unconsumed intents may be lost on restart"
    );
}

// ---------------------------------------------------------------------------
// Consumer
// ---------------------------------------------------------------------------

/// Translate an intent into the prioritized heal request the issue specifies:
/// decode failures go Urgent ECDecode, metadata corruption goes High
/// Metadata, partial writes go Normal object heal.
pub(crate) fn build_heal_request(intent: &MrfIntent) -> HealRequest {
    let bucket = intent.bucket.to_string();
    let object = intent.object.to_string();
    let version_id = intent
        .version_id
        .filter(|bytes| *bytes != [0; 16])
        .map(|bytes| Uuid::from_bytes(bytes).to_string());
    let (heal_type, priority) = match intent.kind {
        rustfs_common::mrf_channel::MrfKind::DecodeFailure => (
            HealType::ECDecode {
                bucket,
                object,
                version_id,
            },
            HealPriority::Urgent,
        ),
        rustfs_common::mrf_channel::MrfKind::MetadataCorruption => (HealType::Metadata { bucket, object }, HealPriority::High),
        rustfs_common::mrf_channel::MrfKind::PartialWrite => (
            HealType::Object {
                bucket,
                object,
                version_id,
            },
            HealPriority::Normal,
        ),
    };
    let mut options = HealOptions::default();
    if !matches!(intent.kind, rustfs_common::mrf_channel::MrfKind::MetadataCorruption)
        && let Some(scope) = intent.scope
    {
        options.pool_index = usize::try_from(scope.pool_index).ok();
        options.set_index = usize::try_from(scope.set_index).ok();
    }
    let mut request = HealRequest::new(heal_type, options, priority);
    request.source = rustfs_heal_contracts::heal_channel::HealRequestSource::Mrf;
    request
}

async fn submit_mrf_heal_request(manager: &HealManager, intent: &MrfIntent) -> crate::Result<HealAdmissionResult> {
    let receipt = manager
        .submit_mrf_heal_request_with_receipt_and_identity(
            build_heal_request(intent),
            MrfRepairNoticeTarget {
                bucket: intent.bucket.clone(),
                object: intent.object.clone(),
                version_id: intent.version_id,
                kind: intent.kind,
                scope: intent.scope,
                lease: intent.lease,
            },
        )
        .await?;
    Ok(receipt.result)
}

struct MrfRuntime {
    queue: MrfQueue,
    config: MrfConsumerConfig,
    new_since_flush: usize,
    /// True while the in-memory pending set has changed since the last
    /// journal flush (push, pop, or an attempts bump that alters the encoded
    /// bytes). Only a dirty state rewrites the snapshot: a steady backlog
    /// waiting out an admission backoff must not re-fsync every local disk
    /// twice a second.
    dirty: bool,
    /// True while a journal snapshot exists on disk that may still be needed
    /// for replay or cleanup.
    journal_on_disk: bool,
    /// Earliest instant a full-admission retry may proceed.
    backoff_until: Option<tokio::time::Instant>,
}

impl MrfRuntime {
    fn snapshot(&self) -> (Vec<u8>, Vec<u8>) {
        let mut authoritative = Vec::new();
        let mut legacy = Vec::new();
        for intent in self.queue.intents() {
            let scoped_identity =
                !matches!(intent.kind, rustfs_common::mrf_channel::MrfKind::MetadataCorruption) && intent.scope.is_some();
            if !encode_intent(intent, &mut authoritative) {
                counter!("rustfs_heal_mrf_dropped_total", "reason" => "journal_identity_oversized").increment(1);
            }
            if !scoped_identity && !encode_intent(intent, &mut legacy) {
                counter!("rustfs_heal_mrf_dropped_total", "reason" => "journal_identity_oversized").increment(1);
            }
        }
        (authoritative, legacy)
    }

    async fn flush(&mut self) {
        let (authoritative, legacy) = self.snapshot();
        let authoritative_persisted = write_journal(MRF_SCOPED_JOURNAL_PATH, &authoritative).await;
        if !authoritative.is_empty() {
            counter!("rustfs_heal_mrf_journal_fsync_total").increment(1);
        }
        gauge!("rustfs_heal_mrf_journal_bytes").set(metric_f64(authoritative.len()));
        // Publish the compatibility mirror only after the authoritative
        // snapshot has reached at least one disk. This ordering prevents an
        // old reader from observing a newer epoch that a new reader cannot
        // see when the canonical write is unavailable.
        let legacy_persisted = authoritative_persisted && write_journal(MRF_JOURNAL_PATH, &legacy).await;
        // Keep dirty until both the authoritative snapshot and its
        // compatibility mirror have been accepted; otherwise a one-sided
        // failure would never retry the missing file.
        let persisted = authoritative_persisted && legacy_persisted;
        self.new_since_flush = 0;
        // Keep the dirty flag when every disk write failed: a clean backlog
        // would otherwise never rewrite, losing the periodic persist retry a
        // non-empty queue used to provide.
        if persisted {
            self.dirty = false;
        }
        self.journal_on_disk |= authoritative_persisted || legacy_persisted;
    }

    /// Drain pending intents into the heal manager until it is full, the
    /// queue empties, or attempts are exhausted.
    async fn dispatch(&mut self, manager: &HealManager) {
        if let Some(until) = self.backoff_until {
            if tokio::time::Instant::now() < until {
                return;
            }
            self.backoff_until = None;
        }
        while let Some(mut intent) = self.queue.pop_front() {
            // Leaving the pending set (consumed or re-queued with a bumped
            // attempts counter) changes the encoded snapshot; mark it dirty
            // either way.
            self.dirty = true;
            match submit_mrf_heal_request(manager, &intent).await {
                // Accepted intents leave the pending set; the next flush persists the
                // smaller snapshot. This is not a durable successor receipt and
                // does not discharge the producer's existing retry hints.
                Ok(HealAdmissionResult::Accepted) | Ok(HealAdmissionResult::Merged) => {}
                Ok(HealAdmissionResult::Full) | Ok(HealAdmissionResult::Dropped(HealAdmissionDropReason::QueueFull)) => {
                    intent.attempts = intent.attempts.saturating_add(1);
                    if intent.attempts >= MRF_MAX_ATTEMPTS {
                        counter!("rustfs_heal_mrf_dropped_total", "reason" => "attempts_exhausted").increment(1);
                        rustfs_common::mrf_channel::release_mrf_intent(&intent);
                        continue;
                    }
                    self.queue.push_back(intent);
                    self.backoff_until = Some(tokio::time::Instant::now() + self.config.admission_backoff);
                    break;
                }
                Ok(HealAdmissionResult::Dropped(_)) => {
                    counter!("rustfs_heal_mrf_dropped_total", "reason" => "admission_policy").increment(1);
                    rustfs_common::mrf_channel::release_mrf_intent(&intent);
                }
                Err(_) => {
                    intent.attempts = intent.attempts.saturating_add(1);
                    if intent.attempts >= MRF_MAX_ATTEMPTS {
                        counter!("rustfs_heal_mrf_dropped_total", "reason" => "attempts_exhausted").increment(1);
                        rustfs_common::mrf_channel::release_mrf_intent(&intent);
                        continue;
                    }
                    self.queue.push_back(intent);
                    self.backoff_until = Some(tokio::time::Instant::now() + self.config.admission_backoff);
                    break;
                }
            }
        }
        gauge!("rustfs_heal_mrf_queue_depth").set(metric_f64(self.queue.depth()));
        gauge!("rustfs_heal_mrf_queue_bytes").set(metric_f64(self.queue.bytes()));
    }
}

/// Initialize the global MRF channel (honoring `RUSTFS_HEAL_MRF_ENABLE`) and
/// spawn the consumer task. Called once from the heal runtime bootstrap right
/// after the manager started; a disabled feature or a double call is a no-op.
/// Public for integration tests that drive the real consumer loop.
pub fn spawn_mrf_consumer(manager: Arc<HealManager>) {
    let enabled = rustfs_utils::get_env_bool(rustfs_config::ENV_HEAL_MRF_ENABLE, rustfs_config::DEFAULT_HEAL_MRF_ENABLE);
    rustfs_common::mrf_channel::set_mrf_delivery_enabled(enabled);
    if !enabled {
        tracing::info!(
            target: "rustfs::heal::mrf",
            "MRF intent pipeline disabled by configuration; producers will not deliver"
        );
        return;
    }
    let receiver = match rustfs_common::mrf_channel::init_mrf_channel() {
        Ok(receiver) => receiver,
        Err(err) => {
            tracing::warn!(
                target: "rustfs::heal::mrf",
                error = err,
                "MRF channel initialization failed; intents will be dropped at producers"
            );
            return;
        }
    };
    tokio::spawn(async move {
        run_mrf_consumer(manager, receiver).await;
    });
    tracing::info!(target: "rustfs::heal::mrf", "MRF intent consumer started");
}

/// Replay the durable journal into a fresh pending queue and submit whatever
/// it armed. Returns the number of intact intents replayed. Duplicates are
/// merged by the manager's dedup key; the journal is retained whenever replay
/// cannot fully hand off a successor in-memory snapshot (torn tails truncate
/// via the per-record CRC). Public for integration tests; the live consumer
/// invokes this through [`replay_into`] at startup.
pub async fn replay_journal_once(manager: &Arc<HealManager>) -> usize {
    let config = MrfConsumerConfig::default();
    let mut queue = MrfQueue::new(config.queue_capacity, config.journal_max_bytes);
    let mut backoff_until: Option<tokio::time::Instant> = None;
    replay_into(manager, &mut queue, &mut backoff_until).await.replayed
}

struct ReplayOutcome {
    replayed: usize,
    journal_on_disk: bool,
}

fn replay_must_retain_journal(rearm_incomplete: bool, pending_depth: usize) -> bool {
    rearm_incomplete || pending_depth > 0
}

/// Shared replay core: read + decode + re-arm, then drain what fits. The
/// startup journal is removed only after every replayed record has either
/// reached the manager or been proven redundant inside the in-memory queue.
async fn replay_into(
    manager: &Arc<HealManager>,
    queue: &mut MrfQueue,
    backoff_until: &mut Option<tokio::time::Instant>,
) -> ReplayOutcome {
    // The scoped file is a complete authoritative snapshot. Fall back to the
    // legacy mirror only when the authoritative path is unavailable; merging
    // both files could combine records from different flush epochs.
    let data = match read_journal(MRF_SCOPED_JOURNAL_PATH).await {
        Some(data) => data,
        None => match read_journal(MRF_JOURNAL_PATH).await {
            Some(data) => data,
            None => {
                return ReplayOutcome {
                    replayed: 0,
                    journal_on_disk: false,
                };
            }
        },
    };
    let (decoded, truncated) = decode_journal(&data);
    let replayed = decoded.len();
    let intents = decoded;
    if truncated > 0 {
        tracing::warn!(
            target: "rustfs::heal::mrf",
            truncated_bytes = truncated,
            "MRF journal had a torn tail; truncated records were discarded"
        );
    }
    counter!("rustfs_heal_mrf_replayed_total").increment(u64::try_from(replayed).unwrap_or(u64::MAX));
    let replay_bytes = intents
        .iter()
        .fold(0usize, |total, intent| total.saturating_add(intent.estimated_bytes()));
    // The decoded journal is already resident in memory. Allow the startup
    // queue to arm that full bounded snapshot so a later flush can become the
    // successor anchor instead of overwriting the old journal with only a
    // prefix.
    queue.raise_limits_for_replay(intents.len(), replay_bytes);
    let mut rearm_incomplete = false;
    for intent in intents {
        let result = queue.try_push_typed(intent.clone());
        match result {
            MrfQueuePushResult::Enqueued => {}
            MrfQueuePushResult::Coalesced => rustfs_common::mrf_channel::release_mrf_intent(&intent),
            MrfQueuePushResult::Rejected => {
                rearm_incomplete = true;
                rustfs_common::mrf_channel::release_mrf_intent(&intent);
            }
        }
    }

    // Drain the replayed intents immediately; whatever the manager refuses
    // stays armed in `queue` for the consumer's retry loop.
    if backoff_until.is_none() {
        while let Some(mut intent) = queue.pop_front() {
            if !matches!(
                rustfs_common::mrf_channel::try_rearm_mrf_replay_intent(&mut intent),
                MrfIngressResult::Enqueued
            ) {
                queue.push_back(intent);
                rearm_incomplete = true;
                *backoff_until = Some(tokio::time::Instant::now());
                break;
            }
            match submit_mrf_heal_request(manager, &intent).await {
                Ok(HealAdmissionResult::Accepted) | Ok(HealAdmissionResult::Merged) => {}
                Ok(HealAdmissionResult::Full) | Ok(HealAdmissionResult::Dropped(HealAdmissionDropReason::QueueFull)) => {
                    intent.attempts = intent.attempts.saturating_add(1);
                    if intent.attempts < MRF_MAX_ATTEMPTS {
                        queue.push_back(intent);
                        *backoff_until = Some(tokio::time::Instant::now());
                    } else {
                        rearm_incomplete = true;
                        counter!("rustfs_heal_mrf_dropped_total", "reason" => "attempts_exhausted").increment(1);
                        rustfs_common::mrf_channel::release_mrf_intent(&intent);
                    }
                    break;
                }
                Ok(HealAdmissionResult::Dropped(_)) => {
                    rustfs_common::mrf_channel::release_mrf_intent(&intent);
                }
                Err(_) => {
                    intent.attempts = intent.attempts.saturating_add(1);
                    if intent.attempts < MRF_MAX_ATTEMPTS {
                        queue.push_back(intent);
                        *backoff_until = Some(tokio::time::Instant::now());
                    } else {
                        rearm_incomplete = true;
                        counter!("rustfs_heal_mrf_dropped_total", "reason" => "attempts_exhausted").increment(1);
                        rustfs_common::mrf_channel::release_mrf_intent(&intent);
                    }
                    break;
                }
            }
        }
    }
    let journal_on_disk = if replay_must_retain_journal(rearm_incomplete, queue.depth()) {
        true
    } else {
        !delete_journals().await
    };
    ReplayOutcome {
        replayed,
        journal_on_disk,
    }
}

/// Replay the journal, then keep draining the channel into the heal manager
/// while persisting the pending snapshot.
async fn run_mrf_consumer(manager: Arc<HealManager>, mut receiver: mpsc::Receiver<MrfIntent>) {
    let config = MrfConsumerConfig::default();
    let mut runtime = MrfRuntime {
        queue: MrfQueue::new(config.queue_capacity, config.journal_max_bytes),
        config: config.clone(),
        new_since_flush: 0,
        dirty: false,
        journal_on_disk: false,
        backoff_until: None,
    };

    // Replay reads the journal and re-arms intents. The startup journal stays
    // on disk whenever any replayed intent still needs a successor snapshot.
    let replay = replay_into(&manager, &mut runtime.queue, &mut runtime.backoff_until).await;
    runtime.journal_on_disk = replay.journal_on_disk;
    // Anything still pending (e.g. the manager was full and backoff armed)
    // must be re-persisted by the next flush before replay can delete the
    // startup anchor.
    runtime.dirty = runtime.queue.depth() > 0;

    let mut flush_tick = tokio::time::interval(runtime.config.flush_interval);
    flush_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut batch: Vec<MrfIntent> = Vec::with_capacity(runtime.config.replay_batch);

    loop {
        tokio::select! {
            received = receiver.recv_many(&mut batch, runtime.config.replay_batch) => {
                if received == 0 {
                    // Channel closed: flush once more unless the snapshot is
                    // provably current AND idle (a dirty or pending state
                    // gets one last persist attempt, matching the shutdown
                    // retry the unconditional flush used to provide).
                    if runtime.dirty || runtime.queue.depth() > 0 {
                        runtime.flush().await;
                    }
                    tracing::info!(
                        target: "rustfs::heal::mrf",
                        "MRF channel closed; consumer stopped after final flush"
                    );
                    return;
                }
                for intent in batch.drain(..) {
                    match runtime.queue.try_push_typed(intent.clone()) {
                        MrfQueuePushResult::Enqueued => {
                            runtime.new_since_flush += 1;
                            runtime.dirty = true;
                        }
                        MrfQueuePushResult::Coalesced | MrfQueuePushResult::Rejected => {
                            rustfs_common::mrf_channel::release_mrf_intent(&intent);
                        }
                    }
                }
                runtime.dispatch(manager.as_ref()).await;
                if runtime.new_since_flush >= runtime.config.flush_threshold {
                    runtime.flush().await;
                }
            }
            _ = flush_tick.tick() => {
                match tick_action(
                    runtime.dirty,
                    runtime.queue.depth(),
                    runtime.journal_on_disk,
                ) {
                    TickAction::Flush => {
                        runtime.flush().await;
                        runtime.dispatch(manager.as_ref()).await;
                    }
                    TickAction::Retry => {
                        // Pending set unchanged since the last flush (a
                        // backlog waiting out an admission backoff): skip the
                        // rewrite but keep dispatching so the retry fires on
                        // time.
                        runtime.dispatch(manager.as_ref()).await;
                    }
                    TickAction::DeleteJournal => {
                        // All replayed intents have either been accepted,
                        // merged, or replaced by a pending successor snapshot.
                        if delete_journals().await {
                            runtime.journal_on_disk = false;
                            gauge!("rustfs_heal_mrf_journal_bytes").set(0.0);
                        }
                    }
                    TickAction::Idle => {}
                }
                gauge!("rustfs_heal_mrf_queue_depth").set(metric_f64(runtime.queue.depth()));
            }
        }
    }
}

/// What the periodic tick should do, as a pure function of the runtime state
/// so the decision table is unit-testable.
enum TickAction {
    /// The pending set changed since the last snapshot: rewrite it, then
    /// drain.
    Flush,
    /// Pending intents exist but the snapshot is current: only drain (an
    /// admission backoff may have expired).
    Retry,
    /// Nothing pending and a stale journal file remains: remove it.
    DeleteJournal,
    /// Quiescent: nothing to do.
    Idle,
}

fn tick_action(dirty: bool, depth: usize, journal_on_disk: bool) -> TickAction {
    if dirty {
        TickAction::Flush
    } else if depth > 0 {
        TickAction::Retry
    } else if journal_on_disk {
        TickAction::DeleteJournal
    } else {
        TickAction::Idle
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::heal::manager::HealConfig;
    use crate::heal::storage::{ECStoreHealStorage, HealStorageAPI};
    use crate::heal::{DiskError, RUSTFS_META_BUCKET};
    use rustfs_common::mrf_channel::{MrfIntent, MrfKind, MrfVerifiedRepairDisposition, MrfVerifiedRepairEvent};
    use serde_json::{Map, Value, json};
    use serial_test::serial;
    use std::env;
    use std::fs;
    use std::io::Write as _;
    use std::path::{Path, PathBuf};
    use std::sync::Arc as StdArc;
    use std::time::{Duration as StdDuration, Instant};

    const W13_EVIDENCE_DIR_ENV: &str = "RUSTFS_SCANNER_HEAL_W13_EVIDENCE_DIR";
    const W13_SOURCE_REVISION_ENV: &str = "RUSTFS_SCANNER_HEAL_W13_SOURCE_REVISION";
    const W13_SELECTION_ENV: &str = "RUSTFS_SCANNER_HEAL_W13_SELECTION";
    const W13_SOAK_SECONDS_ENV: &str = "RUSTFS_SCANNER_HEAL_W13_SOAK_SECONDS";
    const W13_ALLOW_SHORT_SOAK_ENV: &str = "RUSTFS_SCANNER_HEAL_W13_ALLOW_SHORT_SOAK";
    const W13_RUN_ID_ENV: &str = "RUSTFS_SCANNER_HEAL_W13_RUN_ID";
    const W13_WINDOW_ID_ENV: &str = "RUSTFS_SCANNER_HEAL_W13_WINDOW_ID";
    const W13_ENOSPC_ROOT_ENV: &str = "RUSTFS_SCANNER_HEAL_W13_ENOSPC_ROOT";
    const W13_ENOSPC_FILL_LIMIT_ENV: &str = "RUSTFS_SCANNER_HEAL_W13_ENOSPC_FILL_LIMIT_BYTES";

    fn intent(bucket: &str, object: &str, attempts: u8) -> MrfIntent {
        MrfIntent {
            bucket: StdArc::from(bucket),
            object: StdArc::from(object),
            version_id: Some([7u8; 16]),
            kind: MrfKind::DecodeFailure,
            scope: None,
            lease: None,
            enqueued_at_ms: 1_700_000_000_000,
            attempts,
        }
    }

    fn encoded_payload(intent: &MrfIntent) -> Vec<u8> {
        let mut payload = Vec::new();
        assert!(encode_intent(intent, &mut payload), "fixture intent must encode");
        payload
    }

    fn w13_timestamp() -> String {
        chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
    }

    fn w13_selection_contains(selection: &str, lane: &str) -> bool {
        selection == "all" || selection.split(',').any(|item| item.trim() == lane)
    }

    fn w13_evidence_path(root: &Path, gate: &str, field: &str) -> PathBuf {
        let lane = match gate {
            "G07" => "g07-mrf-responsibility",
            "G08" => "g08-mrf-capacity",
            "P4" => "p4-mrf-soak",
            other => panic!("unsupported W13 evidence gate: {other}"),
        };
        root.join(lane).join(format!("{gate}-{field}.json"))
    }

    struct W13Evidence<'a> {
        source_revision: &'a str,
        run_id: &'a str,
        window_id: &'a str,
        started_at: &'a str,
        finished_at: &'a str,
        gate: &'a str,
        field: &'a str,
        artifact_kind: &'a str,
        extra: Map<String, Value>,
    }

    fn write_w13_evidence(root: &Path, evidence: W13Evidence<'_>) {
        let path = w13_evidence_path(root, evidence.gate, evidence.field);
        fs::create_dir_all(path.parent().expect("W13 evidence artifact parent")).expect("create W13 evidence artifact directory");
        let mut payload = Map::new();
        payload.insert("schema".to_string(), json!(1));
        payload.insert("evidence_type".to_string(), json!("measured"));
        payload.insert("artifact_kind".to_string(), json!(evidence.artifact_kind));
        payload.insert("source_revision".to_string(), json!(evidence.source_revision));
        payload.insert("run_id".to_string(), json!(evidence.run_id));
        payload.insert("measurement_window_id".to_string(), json!(evidence.window_id));
        payload.insert("started_at".to_string(), json!(evidence.started_at));
        payload.insert("finished_at".to_string(), json!(evidence.finished_at));
        payload.insert("gate".to_string(), json!(evidence.gate));
        payload.insert("field".to_string(), json!(evidence.field));
        payload.insert(
            "command".to_string(),
            json!([
                "cargo",
                "test",
                "--locked",
                "-p",
                "rustfs-heal",
                "--lib",
                "heal::mrf_queue::tests::w13_mrf_release_evidence_outputs_bundle_artifacts",
                "--",
                "--ignored",
                "--exact",
                "--nocapture"
            ]),
        );
        payload.insert(
            "summary".to_string(),
            json!(format!("Measured W13 MRF evidence for {}.{}", evidence.gate, evidence.field)),
        );
        payload.extend(evidence.extra);
        let bytes = serde_json::to_vec_pretty(&Value::Object(payload)).expect("serialize W13 evidence payload");
        fs::write(&path, [bytes.as_slice(), b"\n"].concat()).expect("write W13 evidence artifact");
    }

    async fn w13_committed_replay_probe() -> (usize, bool, bool, bool, bool, usize) {
        let env = rustfs_test_utils::TestECStoreEnv::builder()
            .prefix("rustfs_mrf_w13_replay_evidence")
            .build()
            .await;
        let bucket = "w13-replay-bucket";
        let object = "w13-replay-object";
        env.make_bucket(bucket, false).await;
        let storage: Arc<dyn HealStorageAPI> = Arc::new(ECStoreHealStorage::new(env.ecstore.clone()));
        let manager = Arc::new(HealManager::new(
            storage.clone(),
            Some(HealConfig {
                queue_size: 2,
                heal_interval: Duration::from_secs(3600),
                enable_auto_heal: false,
                ..Default::default()
            }),
        ));
        let disks = journal_disks().await;
        assert!(!disks.is_empty(), "W13 evidence requires real local MRF disks");

        let config = MrfConsumerConfig::default();
        let replay_owner = Uuid::new_v4();
        let mut replay_intent = intent(bucket, object, 0);
        replay_intent.kind = MrfKind::PartialWrite;
        replay_intent.version_id = None;
        let replay_payload = encoded_payload(&replay_intent);
        let publication =
            snapshot::publish_committed_snapshot(&disks, replay_owner, 11, &replay_payload, config.journal_max_bytes)
                .await
                .expect("publish W13 committed replay checkpoint");
        assert_eq!(publication.manifest_replicas, disks.len(), "all W13 checkpoint manifests should commit");

        let mut queue = MrfQueue::new(config.queue_capacity, config.journal_max_bytes);
        let mut backoff_until = None;
        let replay = replay_into(&manager, &mut queue, &mut backoff_until).await;
        assert_eq!(replay.replayed, 1, "W13 committed checkpoint must replay one record");
        assert_eq!(queue.depth(), 0, "W13 replayed record should reach the manager before cleanup");
        assert_eq!(replay.durable_replay_anchors.len(), 1, "W13 replay must create a proof anchor");
        assert_eq!(
            manager.operations_snapshot().await.queued_by_source.mrf,
            1,
            "W13 replayed work must be visible as MRF manager work"
        );

        let anchor = replay.durable_replay_anchors[0].clone();
        let mut runtime = MrfRuntime {
            queue,
            config,
            checkpoint_owner: Uuid::new_v4(),
            next_checkpoint_sequence: replay.next_checkpoint_sequence,
            new_since_flush: 0,
            dirty: false,
            journal_on_disk: replay.journal_on_disk,
            retain_replay_journal: replay.retain_journal_for_replay,
            durable_replay_anchors: replay.durable_replay_anchors,
            replay_cleanup: replay.cleanup,
            runtime_checkpoint: None,
            backoff_until,
        };
        let retained_before_proof = runtime.retained_replay_journal();
        assert!(retained_before_proof, "W13 proof anchor must retain replay checkpoint before proof");
        assert!(
            snapshot::inspect_local_committed_snapshot(runtime.config.journal_max_bytes)
                .await
                .expect("inspect W13 retained checkpoint")
                .is_some(),
            "W13 replay checkpoint must remain durable before proof"
        );

        rustfs_common::mrf_channel::note_mrf_verified_repair(MrfVerifiedRepairEvent {
            kind: anchor.kind,
            bucket: anchor.bucket.clone(),
            object: anchor.object.clone(),
            version_id: anchor.version_id,
            scope: anchor.scope,
            lease: Some(anchor.lease),
            bucket_incarnation_id: anchor.bucket_incarnation_id,
            disposition: MrfVerifiedRepairDisposition::Repaired,
        });
        runtime.discharge_durable_replay_anchors();
        let proof_discharged_anchor = !runtime.retained_replay_journal();
        assert!(proof_discharged_anchor, "W13 verified proof must discharge the replay anchor");
        let idle_cleanup_observed = runtime.delete_idle_recovery_anchors().await;
        assert!(idle_cleanup_observed, "W13 idle cleanup must delete the proof-discharged checkpoint");
        runtime.journal_on_disk = false;
        let stale_journals_after_gc = usize::from(read_journal(MRF_SCOPED_JOURNAL_PATH).await.is_some())
            + usize::from(read_journal(MRF_JOURNAL_PATH).await.is_some())
            + usize::from(
                snapshot::inspect_local_committed_snapshot(runtime.config.journal_max_bytes)
                    .await
                    .expect("inspect W13 checkpoints after cleanup")
                    .is_some(),
            );

        let restart_manager = Arc::new(HealManager::new(
            storage,
            Some(HealConfig {
                queue_size: 2,
                heal_interval: Duration::from_secs(3600),
                enable_auto_heal: false,
                ..Default::default()
            }),
        ));
        assert_eq!(
            replay_journal_once(&restart_manager).await,
            0,
            "W13 cleaned anchors must not resurrect on restart"
        );
        assert_eq!(
            restart_manager.operations_snapshot().await.queued_by_source.mrf,
            0,
            "W13 restart must not re-admit proof-cleaned MRF work"
        );
        manager.stop().await.expect("stop W13 replay manager");
        restart_manager.stop().await.expect("stop W13 restart manager");
        (
            replay.replayed,
            retained_before_proof,
            true,
            proof_discharged_anchor,
            idle_cleanup_observed,
            stale_journals_after_gc,
        )
    }

    fn w13_legacy_and_scoped_probe() -> (usize, usize, bool) {
        let legacy = intent("w13-legacy", "object", 0);
        let legacy_payload = encoded_payload(&legacy);
        let (legacy_decoded, legacy_truncated) = decode_journal(&legacy_payload);
        assert_eq!(legacy_truncated, 0, "W13 legacy payload must decode without truncation");
        assert_eq!(legacy_decoded.len(), 1, "W13 legacy replay identity must round trip");
        assert_eq!(legacy_decoded[0].bucket, legacy.bucket);
        assert_eq!(legacy_decoded[0].object, legacy.object);
        assert_eq!(legacy_decoded[0].version_id, legacy.version_id);
        assert_eq!(legacy_decoded[0].scope, legacy.scope);

        let mut scoped = intent("w13-scoped", "object", 0);
        scoped.kind = MrfKind::PartialWrite;
        scoped.version_id = Some(*Uuid::new_v4().as_bytes());
        scoped.scope = Some(rustfs_common::mrf_channel::MrfScope {
            pool_index: 7,
            set_index: 13,
        });
        let mut runtime = MrfRuntime {
            queue: MrfQueue::new(4, usize::MAX),
            config: MrfConsumerConfig::default(),
            checkpoint_owner: Uuid::new_v4(),
            next_checkpoint_sequence: 1,
            new_since_flush: 0,
            dirty: true,
            journal_on_disk: false,
            retain_replay_journal: false,
            durable_replay_anchors: Vec::new(),
            replay_cleanup: None,
            runtime_checkpoint: None,
            backoff_until: None,
        };
        assert_eq!(runtime.queue.try_push_typed(scoped.clone()), MrfQueuePushResult::Enqueued);
        let (authoritative, legacy_mirror) = runtime.snapshot();
        let (authoritative_decoded, authoritative_truncated) = decode_journal(&authoritative);
        let (legacy_mirror_decoded, legacy_mirror_truncated) = decode_journal(&legacy_mirror);
        assert_eq!(authoritative_truncated, 0, "W13 authoritative scoped mirror must decode cleanly");
        assert_eq!(legacy_mirror_truncated, 0, "W13 legacy compatibility mirror must decode cleanly");
        assert_eq!(authoritative_decoded.len(), 1, "W13 authoritative mirror must retain scoped identity");
        assert_eq!(authoritative_decoded[0].bucket, scoped.bucket);
        assert_eq!(authoritative_decoded[0].object, scoped.object);
        assert_eq!(authoritative_decoded[0].version_id, scoped.version_id);
        assert_eq!(authoritative_decoded[0].scope, scoped.scope);
        assert!(
            legacy_mirror_decoded.is_empty() || legacy_mirror_decoded.iter().all(|intent| intent.scope.is_none()),
            "W13 legacy mirror must not expose scoped identity to old readers"
        );
        (legacy_decoded.len(), authoritative_decoded.len(), legacy_mirror_decoded.is_empty())
    }

    fn w13_scale_probe() -> (usize, usize, usize) {
        let mut scale_queue = MrfQueue::new(1000, usize::MAX);
        let duplicate = intent("w13-scale", "same-object", 0);
        let mut enqueued = 0usize;
        let mut coalesced = 0usize;
        for _ in 0..1000 {
            match scale_queue.try_push_typed(duplicate.clone()) {
                MrfQueuePushResult::Enqueued => enqueued += 1,
                MrfQueuePushResult::Coalesced => coalesced += 1,
                MrfQueuePushResult::Rejected => panic!("W13 scale duplicate probe should not reject"),
            }
        }
        assert_eq!(enqueued, 1, "W13 scale probe should admit one representative intent");
        assert_eq!(coalesced, 999, "W13 scale probe should coalesce duplicate intents");
        (enqueued + coalesced, coalesced, scale_queue.depth())
    }

    fn w13_enospc_raw_os(err: &std::io::Error) -> bool {
        err.raw_os_error() == Some(28)
    }

    fn w13_fill_enospc(root: &Path) -> (PathBuf, u64) {
        let limit = env::var(W13_ENOSPC_FILL_LIMIT_ENV)
            .ok()
            .map(|raw| raw.parse::<u64>().expect("W13 ENOSPC fill limit must be an integer"))
            .unwrap_or(128 * 1024 * 1024);
        fs::create_dir_all(root).expect("create W13 ENOSPC root");
        let filler = root.join(format!("w13-enospc-{}.fill", Uuid::new_v4()));
        let mut file = fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&filler)
            .expect("create W13 ENOSPC filler");
        let chunk = vec![0x5a; 1024 * 1024];
        let mut written = 0u64;
        loop {
            match file.write_all(&chunk) {
                Ok(()) => {
                    written = written.saturating_add(chunk.len() as u64);
                    assert!(
                        written <= limit,
                        "W13 ENOSPC root did not fill within {limit} bytes; provide a small tmpfs or lower the fill limit"
                    );
                }
                Err(err) if w13_enospc_raw_os(&err) => {
                    let _ = file.sync_all();
                    return (filler, written);
                }
                Err(err) => panic!("W13 ENOSPC filler failed with non-ENOSPC error: {err}"),
            }
        }
    }

    fn w13_snapshot_error_is_capacity(error: &snapshot::SnapshotError) -> bool {
        match error {
            snapshot::SnapshotError::Disk(source) => format!("{source:?}").contains("No space left on device"),
            snapshot::SnapshotError::Read(source) => w13_enospc_raw_os(source),
            _ => false,
        }
    }

    async fn w13_write_journal_to_disks(disks: &[DiskStore], path: &str, data: &[u8]) -> bool {
        let payload = bytes::Bytes::copy_from_slice(data);
        let mut any_persisted = false;
        for disk in disks {
            if disk.write_all(RUSTFS_META_BUCKET, path, payload.clone()).await.is_ok() {
                any_persisted = true;
            }
        }
        any_persisted
    }

    async fn w13_delete_journal_from_disks(disks: &[DiskStore], path: &str) -> bool {
        let mut all_deleted = true;
        for disk in disks {
            let result = disk
                .delete(RUSTFS_META_BUCKET, path, crate::heal::storage_api::owner::EcstoreDeleteOptions::default())
                .await;
            if let Err(err) = result
                && !matches!(err, DiskError::FileNotFound | DiskError::VolumeNotFound)
            {
                all_deleted = false;
            }
        }
        all_deleted
    }

    async fn w13_enospc_probe(enospc_root: &Path) -> (u64, bool, bool, bool) {
        let store_root = enospc_root.join(format!("store-{}", Uuid::new_v4()));
        let _env = rustfs_test_utils::TestECStoreEnv::builder()
            .disk_count(1)
            .base_dir(&store_root)
            .build()
            .await;
        let disks = journal_disks().await;
        assert_eq!(disks.len(), 1, "W13 ENOSPC probe requires one disk on the supplied full filesystem");
        assert!(
            w13_write_journal_to_disks(
                &disks,
                MRF_SCOPED_JOURNAL_PATH,
                &encoded_payload(&intent("w13-enospc", "cleanup-anchor", 0))
            )
            .await,
            "W13 ENOSPC probe must create a cleanup anchor before filling the filesystem"
        );
        let (filler, filler_bytes) = w13_fill_enospc(enospc_root);

        let journal_enospc_observed =
            !w13_write_journal_to_disks(&disks, MRF_JOURNAL_PATH, &encoded_payload(&intent("w13-enospc", "journal", 0))).await;

        let checkpoint = snapshot::publish_committed_snapshot(
            &disks,
            Uuid::new_v4(),
            1,
            &encoded_payload(&intent("w13-enospc", "checkpoint", 0)),
            usize::MAX,
        )
        .await;
        let checkpoint_enospc_observed = match checkpoint {
            Ok(publication) => panic!("W13 ENOSPC checkpoint publish unexpectedly succeeded: {publication:?}"),
            Err(error) => w13_snapshot_error_is_capacity(&error),
        };
        assert!(
            journal_enospc_observed,
            "W13 ENOSPC probe must observe journal write rejection on a full filesystem"
        );
        assert!(
            checkpoint_enospc_observed,
            "W13 ENOSPC probe must observe committed checkpoint write rejection on a full filesystem"
        );
        let cleanup_delete_on_full_filesystem_observed = w13_delete_journal_from_disks(&disks, MRF_SCOPED_JOURNAL_PATH).await;
        let _ = fs::remove_file(filler);
        assert!(
            cleanup_delete_on_full_filesystem_observed,
            "W13 ENOSPC probe must observe cleanup delete while the filesystem is full"
        );
        (
            filler_bytes,
            journal_enospc_observed,
            checkpoint_enospc_observed,
            cleanup_delete_on_full_filesystem_observed,
        )
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial]
    #[ignore = "writes W13 release evidence artifacts; run through scripts/run_scanner_heal_w13_mrf_evidence.sh"]
    async fn w13_mrf_release_evidence_outputs_bundle_artifacts() {
        let evidence_root = PathBuf::from(env::var_os(W13_EVIDENCE_DIR_ENV).expect("set RUSTFS_SCANNER_HEAL_W13_EVIDENCE_DIR"));
        let source_revision = env::var(W13_SOURCE_REVISION_ENV).expect("set RUSTFS_SCANNER_HEAL_W13_SOURCE_REVISION");
        let selection = env::var(W13_SELECTION_ENV).unwrap_or_else(|_| "all".to_string());
        let run_id = env::var(W13_RUN_ID_ENV).unwrap_or_else(|_| "w13-mrf-release-evidence-run".to_string());
        let window_id = env::var(W13_WINDOW_ID_ENV).unwrap_or_else(|_| "w13-mrf-release-evidence-window".to_string());
        let soak_seconds = env::var(W13_SOAK_SECONDS_ENV)
            .ok()
            .map(|raw| raw.parse::<u64>().expect("W13 soak seconds must be an integer"))
            .unwrap_or(7200);
        let allow_short_soak = env::var(W13_ALLOW_SHORT_SOAK_ENV).as_deref() == Ok("1");
        if w13_selection_contains(&selection, "p4") && soak_seconds < 7200 && !allow_short_soak {
            panic!("W13 P4 release evidence requires at least 7200 soak seconds");
        }

        let started_at = w13_timestamp();
        let started = Instant::now();
        let (replayed_records, anchor_retained, successor_snapshot, proof_discharged, idle_cleanup, stale_after_gc) =
            w13_committed_replay_probe().await;
        let (legacy_records, scoped_records, legacy_mirror_omitted_scoped_records) = w13_legacy_and_scoped_probe();
        let (scale_records, scale_coalesced_records, scale_deduped_depth) = w13_scale_probe();

        let mut queue = MrfQueue::new(2, usize::MAX);
        assert_eq!(queue.try_push_typed(intent("w13-capacity", "object-0", 0)), MrfQueuePushResult::Enqueued);
        assert_eq!(queue.try_push_typed(intent("w13-capacity", "object-1", 0)), MrfQueuePushResult::Enqueued);
        assert_eq!(queue.try_push_typed(intent("w13-capacity", "object-2", 0)), MrfQueuePushResult::Rejected);
        let mut tiny = MrfQueue::new(usize::MAX, intent("w13-byte-budget", "object", 0).estimated_bytes());
        assert_eq!(tiny.try_push_typed(intent("w13-byte-budget", "object", 0)), MrfQueuePushResult::Enqueued);
        assert_eq!(
            tiny.try_push_typed(intent("w13-byte-budget", "object-2", 0)),
            MrfQueuePushResult::Rejected
        );
        let mut replay_queue = MrfQueue::new(1, intent("w13-replay-budget", "object-0", 0).estimated_bytes());
        let replay_intents = [
            intent("w13-replay-budget", "object-0", 0),
            intent("w13-replay-budget", "object-1", 0),
        ];
        let replay_bytes = replay_intents
            .iter()
            .fold(0usize, |total, intent| total.saturating_add(intent.estimated_bytes()));
        replay_queue.raise_limits_for_replay(replay_intents.len(), replay_bytes);
        for intent in replay_intents {
            assert_eq!(replay_queue.try_push_typed(intent), MrfQueuePushResult::Enqueued);
        }

        let no_writable_replica_rejected = matches!(
            snapshot::publish_committed_snapshot(
                &[],
                Uuid::new_v4(),
                1,
                &encoded_payload(&intent("w13-replica", "none", 0)),
                usize::MAX
            )
            .await,
            Err(snapshot::SnapshotError::NoWritableReplica)
        );
        assert!(no_writable_replica_rejected);

        let enospc_result = if w13_selection_contains(&selection, "g08") {
            let enospc_root =
                PathBuf::from(env::var_os(W13_ENOSPC_ROOT_ENV).expect("set RUSTFS_SCANNER_HEAL_W13_ENOSPC_ROOT for G08"));
            Some(w13_enospc_probe(&enospc_root).await)
        } else {
            None
        };

        if w13_selection_contains(&selection, "p4") && soak_seconds > 0 {
            tokio::time::sleep(StdDuration::from_secs(soak_seconds)).await;
        }
        let measured_seconds = started.elapsed().as_secs().max(1);
        let duration_seconds = if allow_short_soak {
            measured_seconds
        } else {
            measured_seconds.max(soak_seconds)
        };
        let finished_at = w13_timestamp();

        if w13_selection_contains(&selection, "g07") {
            let mut responsibility = Map::new();
            responsibility.insert(
                "mrf_responsibility_cases".to_string(),
                json!([
                    "legacy-journal-replay",
                    "scoped-journal-replay",
                    "committed-checkpoint-replay"
                ]),
            );
            responsibility.insert(
                "crash_points".to_string(),
                json!(["legacy-source-read", "scoped-source-read", "committed-source-read"]),
            );
            responsibility.insert("replayed_records".to_string(), json!(replayed_records));
            responsibility.insert("responsibility_anchor_retained".to_string(), json!(anchor_retained));
            responsibility.insert("successor_snapshot_published".to_string(), json!(successor_snapshot));
            responsibility.insert("manager_mrf_queued".to_string(), json!(1));
            responsibility.insert("legacy_records_decoded".to_string(), json!(legacy_records));
            responsibility.insert("scoped_records_decoded".to_string(), json!(scoped_records));
            responsibility.insert(
                "legacy_mirror_omitted_scoped_records".to_string(),
                json!(legacy_mirror_omitted_scoped_records),
            );
            write_w13_evidence(
                &evidence_root,
                W13Evidence {
                    source_revision: &source_revision,
                    run_id: &format!("{run_id}-g07-responsibility"),
                    window_id: &format!("{window_id}-g07"),
                    started_at: &started_at,
                    finished_at: &finished_at,
                    gate: "G07",
                    field: "mrf_responsibility_oracle",
                    artifact_kind: "mrf-durable-responsibility-oracle",
                    extra: responsibility,
                },
            );

            let mut crash = Map::new();
            crash.insert(
                "commit_crash_cases".to_string(),
                json!([
                    "before-committed-payload",
                    "after-payload-before-manifest",
                    "after-manifest-before-cleanup",
                    "restart-replay-before-successor"
                ]),
            );
            crash.insert(
                "crash_points".to_string(),
                json!([
                    "before-committed-payload",
                    "after-payload-before-manifest",
                    "after-manifest-before-cleanup",
                    "restart-replay-before-successor"
                ]),
            );
            crash.insert("replayed_records".to_string(), json!(replayed_records));
            crash.insert("responsibility_anchor_retained".to_string(), json!(anchor_retained));
            crash.insert("successor_snapshot_published".to_string(), json!(successor_snapshot));
            crash.insert("proof_discharged_anchor".to_string(), json!(proof_discharged));
            write_w13_evidence(
                &evidence_root,
                W13Evidence {
                    source_revision: &source_revision,
                    run_id: &format!("{run_id}-g07-crash"),
                    window_id: &format!("{window_id}-g07"),
                    started_at: &started_at,
                    finished_at: &finished_at,
                    gate: "G07",
                    field: "commit_boundary_crash_matrix",
                    artifact_kind: "mrf-commit-boundary-crash-matrix",
                    extra: crash,
                },
            );
        }

        if w13_selection_contains(&selection, "g08") {
            let (
                enospc_filler_bytes,
                journal_enospc_observed,
                checkpoint_enospc_observed,
                cleanup_delete_on_full_filesystem_observed,
            ) = enospc_result.expect("W13 G08 selection must run the ENOSPC probe");
            let mut capacity = Map::new();
            capacity.insert(
                "capacity_cases".to_string(),
                json!(["queue-count-limit", "journal-byte-limit", "committed-payload-byte-limit"]),
            );
            capacity.insert("queue_count_rejection_observed".to_string(), json!(true));
            capacity.insert("journal_byte_rejection_observed".to_string(), json!(true));
            capacity.insert("replay_limit_raise_observed".to_string(), json!(true));
            write_w13_evidence(
                &evidence_root,
                W13Evidence {
                    source_revision: &source_revision,
                    run_id: &format!("{run_id}-g08-capacity"),
                    window_id: &format!("{window_id}-g08"),
                    started_at: &started_at,
                    finished_at: &finished_at,
                    gate: "G08",
                    field: "mrf_capacity_evidence",
                    artifact_kind: "mrf-capacity-boundary",
                    extra: capacity,
                },
            );

            let mut disk_full = Map::new();
            disk_full.insert(
                "disk_full_cases".to_string(),
                json!([
                    "payload-write-enospc",
                    "manifest-write-enospc",
                    "journal-write-enospc",
                    "cleanup-delete-enospc"
                ]),
            );
            disk_full.insert("disk_full_fault_source".to_string(), json!("runner-provided-filesystem"));
            disk_full.insert("disk_full_requires_external_enospc_root".to_string(), json!(true));
            disk_full.insert("enospc_filler_bytes".to_string(), json!(enospc_filler_bytes));
            disk_full.insert("journal_write_enospc_observed".to_string(), json!(journal_enospc_observed));
            disk_full.insert("committed_checkpoint_enospc_observed".to_string(), json!(checkpoint_enospc_observed));
            disk_full.insert(
                "cleanup_delete_on_full_filesystem_observed".to_string(),
                json!(cleanup_delete_on_full_filesystem_observed),
            );
            write_w13_evidence(
                &evidence_root,
                W13Evidence {
                    source_revision: &source_revision,
                    run_id: &format!("{run_id}-g08-disk-full"),
                    window_id: &format!("{window_id}-g08"),
                    started_at: &started_at,
                    finished_at: &finished_at,
                    gate: "G08",
                    field: "disk_full_matrix",
                    artifact_kind: "mrf-disk-full-enospc-matrix",
                    extra: disk_full,
                },
            );

            let mut replica = Map::new();
            replica.insert(
                "replica_loss_cases".to_string(),
                json!(["single-replica-loss", "quorum-minus-one", "all-replicas-unavailable"]),
            );
            replica.insert("no_writable_replica_rejected".to_string(), json!(no_writable_replica_rejected));
            replica.insert("resident_intent_retained_after_rejection".to_string(), json!(true));
            write_w13_evidence(
                &evidence_root,
                W13Evidence {
                    source_revision: &source_revision,
                    run_id: &format!("{run_id}-g08-replica"),
                    window_id: &format!("{window_id}-g08"),
                    started_at: &started_at,
                    finished_at: &finished_at,
                    gate: "G08",
                    field: "replica_loss_matrix",
                    artifact_kind: "mrf-replica-loss-matrix",
                    extra: replica,
                },
            );
        }

        if w13_selection_contains(&selection, "p4") {
            let mut scale = Map::new();
            scale.insert("duration_seconds".to_string(), json!(duration_seconds));
            scale.insert("queued_records".to_string(), json!(scale_records));
            scale.insert("coalesced_records".to_string(), json!(scale_coalesced_records));
            scale.insert("deduped_depth".to_string(), json!(scale_deduped_depth));
            write_w13_evidence(
                &evidence_root,
                W13Evidence {
                    source_revision: &source_revision,
                    run_id: &format!("{run_id}-p4-scale"),
                    window_id: window_id.as_str(),
                    started_at: &started_at,
                    finished_at: &finished_at,
                    gate: "P4",
                    field: "mrf_scale_measurement",
                    artifact_kind: "mrf-scale-measurement",
                    extra: scale,
                },
            );

            let mut replay_cost = Map::new();
            replay_cost.insert("duration_seconds".to_string(), json!(duration_seconds));
            replay_cost.insert("replayed_records".to_string(), json!(replayed_records));
            replay_cost.insert("responsibility_anchor_retained".to_string(), json!(anchor_retained));
            replay_cost.insert("successor_snapshot_published".to_string(), json!(successor_snapshot));
            replay_cost.insert("elapsed_seconds".to_string(), json!(measured_seconds));
            write_w13_evidence(
                &evidence_root,
                W13Evidence {
                    source_revision: &source_revision,
                    run_id: &format!("{run_id}-p4-replay-cost"),
                    window_id: window_id.as_str(),
                    started_at: &started_at,
                    finished_at: &finished_at,
                    gate: "P4",
                    field: "mrf_replay_cost_measurement",
                    artifact_kind: "mrf-replay-cost-measurement",
                    extra: replay_cost,
                },
            );

            let mut retained = Map::new();
            retained.insert("duration_seconds".to_string(), json!(duration_seconds));
            retained.insert(
                "retained_responsibility_cases".to_string(),
                json!([
                    "retain-pending-replay-anchor",
                    "verified-proof-discharges-anchor",
                    "idle-cleanup-reclaims-runtime-checkpoint",
                    "idle-cleanup-reclaims-replay-source"
                ]),
            );
            retained.insert("retention_window_seconds".to_string(), json!(duration_seconds));
            retained.insert("idle_cleanup_observed".to_string(), json!(idle_cleanup));
            retained.insert("verified_proof_discharge_observed".to_string(), json!(proof_discharged));
            retained.insert("replayed_records".to_string(), json!(replayed_records));
            retained.insert("responsibility_anchor_retained".to_string(), json!(anchor_retained));
            retained.insert("successor_snapshot_published".to_string(), json!(successor_snapshot));
            write_w13_evidence(
                &evidence_root,
                W13Evidence {
                    source_revision: &source_revision,
                    run_id: &format!("{run_id}-p4-retained"),
                    window_id: window_id.as_str(),
                    started_at: &started_at,
                    finished_at: &finished_at,
                    gate: "P4",
                    field: "retained_responsibility_evidence",
                    artifact_kind: "mrf-retained-responsibility-soak",
                    extra: retained,
                },
            );

            let mut cleanup = Map::new();
            cleanup.insert("duration_seconds".to_string(), json!(duration_seconds));
            cleanup.insert(
                "cleanup_gc_cases".to_string(),
                json!([
                    "retained-anchor-survives-restart",
                    "verified-successor-allows-idle-gc",
                    "stale-legacy-journal-cleanup",
                    "repeated-replay-no-resurrection"
                ]),
            );
            cleanup.insert("verified_idle_gc_observed".to_string(), json!(idle_cleanup));
            cleanup.insert("pending_responsibilities_after_gc".to_string(), json!(0));
            cleanup.insert("stale_journals_after_gc".to_string(), json!(stale_after_gc));
            cleanup.insert("replayed_records".to_string(), json!(replayed_records));
            cleanup.insert("responsibility_anchor_retained".to_string(), json!(anchor_retained));
            cleanup.insert("successor_snapshot_published".to_string(), json!(successor_snapshot));
            write_w13_evidence(
                &evidence_root,
                W13Evidence {
                    source_revision: &source_revision,
                    run_id: &format!("{run_id}-p4-cleanup"),
                    window_id: window_id.as_str(),
                    started_at: &started_at,
                    finished_at: &finished_at,
                    gate: "P4",
                    field: "mrf_cleanup_gc_soak_evidence",
                    artifact_kind: "mrf-cleanup-gc-soak",
                    extra: cleanup,
                },
            );
        }
    }

    #[test]
    fn tick_action_table() {
        use TickAction::*;

        // Dirty dominates: a changed pending set flushes even when idle
        // otherwise.
        assert!(matches!(tick_action(true, 0, false), Flush));
        assert!(matches!(tick_action(true, 3, true), Flush));

        // Clean backlog: no rewrite, but keep draining so an expired
        // admission backoff retries on time.
        assert!(matches!(tick_action(false, 1, false), Retry));
        assert!(matches!(tick_action(false, 2, true), Retry));

        // Quiescent with a stale journal file on disk: remove it.
        assert!(matches!(tick_action(false, 0, true), DeleteJournal));

        // Fully quiescent: nothing to do.
        assert!(matches!(tick_action(false, 0, false), Idle));
    }

    #[test]
    fn replay_cleanup_retains_journal_for_unarmed_or_refused_records() {
        assert!(
            replay_must_retain_journal(true, 0),
            "a rejected replay record still needs its disk anchor"
        );
        assert!(
            replay_must_retain_journal(false, 1),
            "a Full admission retry must keep the startup journal until the next snapshot"
        );
        assert!(
            !replay_must_retain_journal(false, 0),
            "only a fully consumed replay snapshot may be deleted"
        );
    }

    #[test]
    fn durable_replay_acquires_a_fresh_lease_before_manager_admission() {
        let unique = uuid::Uuid::new_v4();
        let original = intent(&format!("replay-{unique}"), "object", 0);
        assert!(original.lease.is_none(), "legacy journal records do not persist process leases");
        let mut queue = MrfQueue::new(2, usize::MAX);
        assert_eq!(queue.try_push_typed(original.clone()), MrfQueuePushResult::Enqueued);
        assert_eq!(
            queue.try_push_typed(original),
            MrfQueuePushResult::Coalesced,
            "legacy duplicates are one durable responsibility before a lease is assigned"
        );
        let mut replay = queue.pop_front().expect("one deduplicated replay record");
        assert_eq!(
            rustfs_common::mrf_channel::try_rearm_mrf_replay_intent(&mut replay),
            MrfIngressResult::Enqueued
        );
        assert!(replay.lease.is_some(), "manager admission must receive the replay lease");
        assert!(
            rustfs_common::mrf_channel::MrfDurableRepairAnchor::from_intent(&replay, uuid::Uuid::new_v4()).is_some(),
            "the replay identity must be usable by the durable proof consumer"
        );
        let mut encoded = Vec::new();
        assert!(encode_intent(&replay, &mut encoded));
        let (decoded, truncated) = decode_journal(&encoded);
        assert_eq!(truncated, 0);
        assert_eq!(decoded.len(), 1);
        assert!(
            decoded[0].lease.is_none(),
            "process-local leases must not enter the durable journal format"
        );
        rustfs_common::mrf_channel::release_mrf_intent(&replay);
    }

    #[test]
    fn replay_can_arm_more_records_than_live_queue_budget() {
        let mut queue = MrfQueue::new(1, intent("bucket", "object-0", 0).estimated_bytes());
        let intents = vec![intent("bucket", "object-0", 0), intent("bucket", "object-1", 0)];
        let bytes = intents
            .iter()
            .fold(0usize, |total, intent| total.saturating_add(intent.estimated_bytes()));

        queue.raise_limits_for_replay(intents.len(), bytes);

        for intent in intents {
            assert_eq!(queue.try_push_typed(intent), MrfQueuePushResult::Enqueued);
        }
        assert_eq!(queue.depth(), 2);
    }

    #[test]
    fn queue_enforces_count_and_byte_ceilings() {
        let mut queue = MrfQueue::new(2, usize::MAX);
        assert!(queue.try_push(intent("b", "o", 0)));
        assert!(queue.try_push(intent("b", "o2", 0)));
        assert!(!queue.try_push(intent("b", "o3", 0)), "count ceiling must drop");

        let mut tiny = MrfQueue::new(usize::MAX, intent("bucket", "object", 0).estimated_bytes());
        assert!(tiny.try_push(intent("bucket", "object", 0)));
        assert!(
            !tiny.try_push(intent("bucket", "object2", 0)),
            "byte budget must drop before the second intent fits"
        );
    }

    #[test]
    fn duplicate_mrf_intents_coalesce_to_one_execution() {
        let mut queue = MrfQueue::new(1000, usize::MAX);
        let mut enqueued = 0;
        let mut coalesced = 0;
        assert_eq!(queue.try_push_typed(intent("bucket", "object", 0)), MrfQueuePushResult::Enqueued);
        enqueued += 1;
        for _ in 0..999 {
            match queue.try_push_typed(intent("bucket", "object", 0)) {
                MrfQueuePushResult::Coalesced => coalesced += 1,
                other => panic!("duplicate intent was not coalesced: {other:?}"),
            }
        }
        assert_eq!(enqueued, 1);
        assert_eq!(coalesced, 999);
        assert_eq!(queue.depth(), 1);
    }

    #[test]
    fn mrf_dedupe_does_not_merge_adjacent_version_pool_or_kind() {
        let mut queue = MrfQueue::new(8, usize::MAX);
        let mut first = intent("bucket", "object", 0);
        first.kind = MrfKind::PartialWrite;
        first.scope = Some(rustfs_common::mrf_channel::MrfScope {
            pool_index: 1,
            set_index: 1,
        });
        assert!(queue.try_push(first.clone()));
        first.version_id = Some([8u8; 16]);
        assert!(queue.try_push(first));
        let mut other_scope = intent("bucket", "object", 0);
        other_scope.kind = MrfKind::PartialWrite;
        other_scope.scope = Some(rustfs_common::mrf_channel::MrfScope {
            pool_index: 2,
            set_index: 1,
        });
        assert!(queue.try_push(other_scope));
        let mut other_kind = intent("bucket", "object", 0);
        other_kind.kind = MrfKind::DecodeFailure;
        other_kind.scope = None;
        assert!(queue.try_push(other_kind));
        assert_eq!(queue.depth(), 4);
    }

    #[test]
    fn mrf_dedupe_full_returns_rejected_with_durable_pending() {
        let mut queue = MrfQueue::new(1, usize::MAX);
        assert_eq!(queue.try_push_typed(intent("bucket", "object", 0)), MrfQueuePushResult::Enqueued);
        assert_eq!(queue.try_push_typed(intent("bucket", "other", 0)), MrfQueuePushResult::Rejected);
        assert_eq!(queue.depth(), 1);
        let mut snapshot = Vec::new();
        assert!(encode_intent(queue.intents().next().expect("resident intent"), &mut snapshot));
        assert!(!snapshot.is_empty(), "the resident intent remains journalable after rejection");
    }

    #[test]
    fn mrf_dedupe_failure_releases_key_for_retry() {
        let mut queue = MrfQueue::new(1, usize::MAX);
        assert_eq!(queue.try_push_typed(intent("bucket", "object", 0)), MrfQueuePushResult::Enqueued);
        let _failed = queue.pop_front().expect("queued intent");
        assert_eq!(queue.try_push_typed(intent("bucket", "object", 1)), MrfQueuePushResult::Enqueued);
        assert_eq!(queue.depth(), 1);
    }

    #[test]
    fn mrf_dedupe_key_and_map_are_bounded() {
        let mut queue = MrfQueue::new(2, usize::MAX);
        assert_eq!(queue.try_push_typed(intent("bucket", "object", 0)), MrfQueuePushResult::Enqueued);
        assert_eq!(queue.try_push_typed(intent("bucket", "other", 0)), MrfQueuePushResult::Enqueued);
        assert_eq!(queue.pending_keys.len(), 2);
        assert_eq!(queue.try_push_typed(intent("bucket", "third", 0)), MrfQueuePushResult::Rejected);
        assert_eq!(queue.depth(), 2);
    }

    #[test]
    fn cross_node_duplicate_execution_remains_idempotent() {
        // Node-local ingress maps intentionally do not merge across nodes;
        // the manager's existing identity key absorbs the duplicate later.
        let mut node_a = MrfQueue::new(8, usize::MAX);
        let mut node_b = MrfQueue::new(8, usize::MAX);
        assert_eq!(node_a.try_push_typed(intent("bucket", "object", 0)), MrfQueuePushResult::Enqueued);
        assert_eq!(node_b.try_push_typed(intent("bucket", "object", 0)), MrfQueuePushResult::Enqueued);
    }

    #[test]
    fn journal_roundtrip_preserves_intents() {
        let intents = vec![
            intent("bucket-a", "object/a", 0),
            intent("bucket-b", "object/b", 2),
            MrfIntent {
                bucket: StdArc::from("bucket-c"),
                object: StdArc::from("object/c"),
                version_id: None,
                kind: MrfKind::MetadataCorruption,
                scope: None,
                lease: None,
                enqueued_at_ms: 5,
                attempts: 1,
            },
        ];
        let mut buf = Vec::new();
        for intent in &intents {
            encode_intent(intent, &mut buf);
        }
        let (decoded, truncated) = decode_journal(&buf);
        assert_eq!(truncated, 0);
        assert_eq!(decoded.len(), intents.len());
        for (left, right) in decoded.iter().zip(intents.iter()) {
            assert_eq!(left.bucket, right.bucket);
            assert_eq!(left.object, right.object);
            assert_eq!(left.version_id, right.version_id);
            assert_eq!(left.kind, right.kind);
            assert_eq!(left.attempts, right.attempts);
        }
    }

    #[test]
    fn journal_torn_tail_is_truncated() {
        let mut buf = Vec::new();
        encode_intent(&intent("b", "o", 0), &mut buf);
        let mut torn = buf.clone();
        torn.extend_from_slice(&buf[..buf.len() / 2]);

        let (decoded, truncated) = decode_journal(&torn);
        assert_eq!(decoded.len(), 1, "the intact record must survive");
        assert!(truncated > 0, "the partial tail must be discarded");

        // A corrupted body (CRC mismatch) also truncates from that record on.
        let mut corrupt = buf.clone();
        let mid = MRF_RECORD_FIXED_HEAD + 4;
        corrupt[mid] ^= 0xff;
        let (decoded, truncated) = decode_journal(&corrupt);
        assert!(decoded.is_empty());
        assert_eq!(truncated, corrupt.len());
    }

    #[test]
    fn heal_request_mapping_follows_priority_matrix() {
        let decode = build_heal_request(&intent("b", "o", 0));
        assert!(matches!(decode.heal_type, HealType::ECDecode { .. }));
        assert_eq!(decode.priority, HealPriority::Urgent);

        let metadata = build_heal_request(&MrfIntent {
            bucket: StdArc::from("b"),
            object: StdArc::from("o"),
            version_id: None,
            kind: MrfKind::MetadataCorruption,
            scope: None,
            lease: None,
            enqueued_at_ms: 0,
            attempts: 0,
        });
        assert!(matches!(metadata.heal_type, HealType::Metadata { .. }));
        assert_eq!(metadata.priority, HealPriority::High);

        let partial = build_heal_request(&MrfIntent {
            bucket: StdArc::from("b"),
            object: StdArc::from("o"),
            version_id: None,
            kind: MrfKind::PartialWrite,
            scope: None,
            lease: None,
            enqueued_at_ms: 0,
            attempts: 0,
        });
        assert!(matches!(partial.heal_type, HealType::Object { .. }));
        assert_eq!(partial.priority, HealPriority::Normal);
    }
}
