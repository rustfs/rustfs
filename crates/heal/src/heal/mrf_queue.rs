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
//! own CRC32. Losing the last flush window (≤500 ms) is acceptable because
//! every producer keeps its own safety net: read-repair re-detects on the
//! next failing read, and the scanner's corrupt-metadata branch leaves a
//! pending-ledger entry behind even when its MRF intent is accepted
//! (backlog#1894 axis A), so a lost intent is retried by the ledger rather
//! than waiting for the failed-object TTL to re-scan the path.

use super::{DiskStore, HealDiskExt as _, local_disk_map_read};
use crate::heal::manager::HealManager;
use metrics::{counter, gauge};
use rustfs_common::heal_channel::{HealAdmissionDropReason, HealAdmissionResult};
use rustfs_common::mrf_channel::{MRF_MAX_ATTEMPTS, MrfIntent};
use std::collections::{HashSet, VecDeque};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::heal::task::{HealOptions, HealPriority, HealRequest, HealType};

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
    request.source = rustfs_common::heal_channel::HealRequestSource::Mrf;
    request
}

async fn submit_mrf_heal_request(manager: &HealManager, intent: &MrfIntent) -> crate::Result<HealAdmissionResult> {
    let receipt = manager
        .submit_mrf_heal_request_with_receipt_and_identity(
            build_heal_request(intent),
            intent.bucket.clone(),
            intent.object.clone(),
            intent.version_id,
            intent.kind,
            intent.scope,
            intent.lease,
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
    /// True while a journal snapshot exists on disk that no longer reflects
    /// an all-consumed pending set; the next idle tick removes it (MinIO
    /// deletes its `list.bin` after replay for the same reason).
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
                // smaller snapshot. The scanner ledger is cleared later, when the
                // canonical heal task reaches a successful terminal completion.
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
/// merged by the manager's dedup key; the journal file is removed once read
/// (torn tails truncate via the per-record CRC). Public for integration tests;
/// the live consumer invokes this through [`replay_into`] at startup.
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

/// Shared replay core: read + decode + re-arm + delete, then drain what fits.
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
    let mut intents = Vec::new();
    let (decoded, truncated) = decode_journal(&data);
    intents.extend(decoded);
    if truncated > 0 {
        tracing::warn!(
            target: "rustfs::heal::mrf",
            truncated_bytes = truncated,
            "MRF journal had a torn tail; truncated records were discarded"
        );
    }
    counter!("rustfs_heal_mrf_replayed_total").increment(u64::try_from(intents.len()).unwrap_or(u64::MAX));
    let replayed = intents.len();
    for intent in intents {
        let result = queue.try_push_typed(intent.clone());
        if !matches!(result, MrfQueuePushResult::Enqueued) {
            rustfs_common::mrf_channel::release_mrf_intent(&intent);
        }
    }
    let journal_on_disk = !delete_journals().await;

    // Drain the replayed intents immediately; whatever the manager refuses
    // stays armed in `queue` for the consumer's retry loop.
    if backoff_until.is_none() {
        while let Some(mut intent) = queue.pop_front() {
            match submit_mrf_heal_request(manager, &intent).await {
                Ok(HealAdmissionResult::Accepted) | Ok(HealAdmissionResult::Merged) => {}
                Ok(HealAdmissionResult::Full) | Ok(HealAdmissionResult::Dropped(HealAdmissionDropReason::QueueFull)) => {
                    intent.attempts = intent.attempts.saturating_add(1);
                    if intent.attempts < MRF_MAX_ATTEMPTS {
                        queue.push_back(intent);
                        *backoff_until = Some(tokio::time::Instant::now());
                    }
                    break;
                }
                Ok(HealAdmissionResult::Dropped(_)) | Err(_) => {}
            }
        }
    }
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

    // Replay: read the journal, re-arm intents (duplicates are merged by the
    // manager's dedup key), then drop the file so the next flush starts clean.
    let replay = replay_into(&manager, &mut runtime.queue, &mut runtime.backoff_until).await;
    runtime.journal_on_disk = replay.journal_on_disk;
    // The replay deleted the journal file; anything still pending (e.g. the
    // manager was full and backoff armed) must be re-persisted by the next
    // flush or a crash before it would lose those intents.
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
                match tick_action(runtime.dirty, runtime.queue.depth(), runtime.journal_on_disk) {
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
                        // All intents consumed: remove the journal so a restart
                        // replays nothing (mirrors MinIO's post-replay unlink).
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
    use rustfs_common::mrf_channel::{MrfIntent, MrfKind};
    use std::sync::Arc as StdArc;

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
