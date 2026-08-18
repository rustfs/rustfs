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
//! own CRC32. Losing the last flush window (≤500 ms) is acceptable: replayed
//! duplicates are merged by the manager's dedup key, and read-repair remains
//! the safety net.

use super::{DiskStore, HealDiskExt as _, local_disk_map_read};
use crate::heal::manager::HealManager;
use metrics::{counter, gauge};
use rustfs_common::heal_channel::{HealAdmissionDropReason, HealAdmissionResult};
use rustfs_common::mrf_channel::{MRF_MAX_ATTEMPTS, MrfIntent};
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::heal::task::{HealOptions, HealPriority, HealRequest, HealType};

/// Journal location inside the metadata bucket, following the resume-state
/// layout.
pub(crate) const MRF_JOURNAL_PATH: &str = "buckets/.heal/mrf/journal.bin";

/// Record format tag.
const MRF_JOURNAL_FORMAT: u8 = 1;
/// Record layout version.
const MRF_JOURNAL_VERSION: u8 = 1;

/// Fixed header size: format, version, kind, attempts, enqueued_at_ms,
/// has_version flag.
const MRF_RECORD_FIXED_HEAD: usize = 1 + 1 + 1 + 1 + 8 + 1;

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
    bytes: usize,
    capacity: usize,
    byte_budget: usize,
}

impl MrfQueue {
    pub(crate) fn new(capacity: usize, byte_budget: usize) -> Self {
        Self {
            pending: VecDeque::new(),
            bytes: 0,
            capacity,
            byte_budget,
        }
    }

    /// Returns `false` (after counting) when either ceiling would be crossed.
    pub(crate) fn try_push(&mut self, intent: MrfIntent) -> bool {
        let cost = intent.estimated_bytes();
        if self.pending.len() >= self.capacity || self.bytes + cost > self.byte_budget {
            counter!("rustfs_heal_mrf_dropped_total", "reason" => "queue_overflow").increment(1);
            return false;
        }
        self.bytes += cost;
        self.pending.push_back(intent);
        true
    }

    pub(crate) fn pop_front(&mut self) -> Option<MrfIntent> {
        let intent = self.pending.pop_front()?;
        self.bytes = self.bytes.saturating_sub(intent.estimated_bytes());
        Some(intent)
    }

    pub(crate) fn push_back(&mut self, intent: MrfIntent) {
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
pub(crate) fn encode_intent(intent: &MrfIntent, out: &mut Vec<u8>) {
    let start = out.len();
    out.push(MRF_JOURNAL_FORMAT);
    out.push(MRF_JOURNAL_VERSION);
    out.push(match intent.kind {
        rustfs_common::mrf_channel::MrfKind::DecodeFailure => 1,
        rustfs_common::mrf_channel::MrfKind::MetadataCorruption => 2,
        rustfs_common::mrf_channel::MrfKind::PartialWrite => 3,
    });
    out.push(intent.attempts);
    out.extend_from_slice(&intent.enqueued_at_ms.to_le_bytes());
    match intent.version_id {
        Some(bytes) => {
            out.push(1);
            out.extend_from_slice(&bytes);
        }
        None => out.push(0),
    }
    out.extend_from_slice(&(intent.bucket.len() as u32).to_le_bytes());
    out.extend_from_slice(&(intent.object.len() as u32).to_le_bytes());
    out.extend_from_slice(intent.bucket.as_bytes());
    out.extend_from_slice(intent.object.as_bytes());
    let mut hasher = crc_fast::Digest::new(crc_fast::CrcAlgorithm::Crc32IsoHdlc);
    hasher.update(&out[start..]);
    out.extend_from_slice(&(hasher.finalize() as u32).to_le_bytes());
}

fn decode_one(data: &[u8]) -> Option<(MrfIntent, usize)> {
    if data.len() < MRF_RECORD_FIXED_HEAD + 8 {
        return None;
    }
    if data[0] != MRF_JOURNAL_FORMAT || data[1] != MRF_JOURNAL_VERSION {
        return None;
    }
    let kind = match data[2] {
        1 => rustfs_common::mrf_channel::MrfKind::DecodeFailure,
        2 => rustfs_common::mrf_channel::MrfKind::MetadataCorruption,
        3 => rustfs_common::mrf_channel::MrfKind::PartialWrite,
        _ => return None,
    };
    let attempts = data[3];
    let enqueued_at_ms = u64::from_le_bytes(data[4..12].try_into().expect("slice length checked"));
    let has_version = data[12] != 0;
    let mut cursor = MRF_RECORD_FIXED_HEAD;
    let version_id = if has_version {
        if data.len() < cursor + 16 {
            return None;
        }
        let bytes: [u8; 16] = data[cursor..cursor + 16].try_into().expect("slice length checked");
        cursor += 16;
        Some(bytes)
    } else {
        None
    };
    if data.len() < cursor + 8 {
        return None;
    }
    let bucket_len = u32::from_le_bytes(data[cursor..cursor + 4].try_into().expect("slice length checked")) as usize;
    let object_len = u32::from_le_bytes(data[cursor + 4..cursor + 8].try_into().expect("slice length checked")) as usize;
    cursor += 8;
    let body_end = cursor.checked_add(bucket_len)?.checked_add(object_len)?;
    let record_end = body_end.checked_add(4)?;
    if data.len() < record_end {
        return None;
    }
    let mut hasher = crc_fast::Digest::new(crc_fast::CrcAlgorithm::Crc32IsoHdlc);
    hasher.update(&data[..body_end]);
    if (hasher.finalize() as u32) != u32::from_le_bytes(data[body_end..record_end].try_into().expect("slice length checked")) {
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

async fn read_journal() -> Option<Vec<u8>> {
    for disk in journal_disks().await {
        match disk.read_all(super::RUSTFS_META_BUCKET, MRF_JOURNAL_PATH).await {
            Ok(bytes) => return Some(bytes.to_vec()),
            Err(_) => continue,
        }
    }
    None
}

async fn write_journal(data: &[u8]) {
    let payload = bytes::Bytes::copy_from_slice(data);
    for disk in journal_disks().await {
        if let Err(err) = disk
            .write_all(super::RUSTFS_META_BUCKET, MRF_JOURNAL_PATH, payload.clone())
            .await
        {
            warn_mrf_journal_write(&err);
        }
    }
    if !data.is_empty() {
        counter!("rustfs_heal_mrf_journal_fsync_total").increment(1);
    }
    gauge!("rustfs_heal_mrf_journal_bytes").set(data.len() as f64);
}

async fn delete_journal() {
    for disk in journal_disks().await {
        let _ = disk
            .delete(
                super::RUSTFS_META_BUCKET,
                MRF_JOURNAL_PATH,
                crate::heal::storage_api::owner::EcstoreDeleteOptions::default(),
            )
            .await;
    }
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
    let version_id = intent.version_id.map(|bytes| Uuid::from_bytes(bytes).to_string());
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
    let mut request = HealRequest::new(heal_type, HealOptions::default(), priority);
    request.source = rustfs_common::heal_channel::HealRequestSource::Mrf;
    request
}

struct MrfRuntime {
    queue: MrfQueue,
    config: MrfConsumerConfig,
    new_since_flush: usize,
    /// True while a journal snapshot exists on disk that no longer reflects
    /// an all-consumed pending set; the next idle tick removes it (MinIO
    /// deletes its `list.bin` after replay for the same reason).
    journal_on_disk: bool,
    /// Earliest instant a full-admission retry may proceed.
    backoff_until: Option<tokio::time::Instant>,
}

impl MrfRuntime {
    fn record_accept(&mut self) {
        // Accepted intents leave the pending set; the next flush persists the
        // smaller snapshot, which is the journal's compaction.
    }

    fn snapshot(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        for intent in self.queue.intents() {
            encode_intent(intent, &mut buf);
        }
        buf
    }

    async fn flush(&mut self) {
        write_journal(&self.snapshot()).await;
        self.new_since_flush = 0;
        self.journal_on_disk = true;
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
            let request = build_heal_request(&intent);
            match manager.submit_heal_request(request).await {
                Ok(HealAdmissionResult::Accepted) | Ok(HealAdmissionResult::Merged) => self.record_accept(),
                Ok(HealAdmissionResult::Full) | Ok(HealAdmissionResult::Dropped(HealAdmissionDropReason::QueueFull)) => {
                    intent.attempts = intent.attempts.saturating_add(1);
                    if intent.attempts >= MRF_MAX_ATTEMPTS {
                        counter!("rustfs_heal_mrf_dropped_total", "reason" => "attempts_exhausted").increment(1);
                        continue;
                    }
                    self.queue.push_back(intent);
                    self.backoff_until = Some(tokio::time::Instant::now() + self.config.admission_backoff);
                    break;
                }
                Ok(HealAdmissionResult::Dropped(_)) => {
                    counter!("rustfs_heal_mrf_dropped_total", "reason" => "admission_policy").increment(1);
                }
                Err(_) => {
                    intent.attempts = intent.attempts.saturating_add(1);
                    if intent.attempts >= MRF_MAX_ATTEMPTS {
                        counter!("rustfs_heal_mrf_dropped_total", "reason" => "attempts_exhausted").increment(1);
                        continue;
                    }
                    self.queue.push_back(intent);
                    self.backoff_until = Some(tokio::time::Instant::now() + self.config.admission_backoff);
                    break;
                }
            }
        }
        gauge!("rustfs_heal_mrf_queue_depth").set(self.queue.depth() as f64);
        gauge!("rustfs_heal_mrf_queue_bytes").set(self.queue.bytes() as f64);
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
    replay_into(manager, &mut queue, &mut backoff_until).await
}

/// Shared replay core: read + decode + re-arm + delete, then drain what fits.
async fn replay_into(
    manager: &Arc<HealManager>,
    queue: &mut MrfQueue,
    backoff_until: &mut Option<tokio::time::Instant>,
) -> usize {
    let Some(data) = read_journal().await else {
        return 0;
    };
    let (intents, truncated) = decode_journal(&data);
    if truncated > 0 {
        tracing::warn!(
            target: "rustfs::heal::mrf",
            truncated_bytes = truncated,
            "MRF journal had a torn tail; truncated records were discarded"
        );
    }
    counter!("rustfs_heal_mrf_replayed_total").increment(intents.len() as u64);
    let replayed = intents.len();
    for intent in intents {
        queue.try_push(intent);
    }
    delete_journal().await;

    // Drain the replayed intents immediately; whatever the manager refuses
    // stays armed in `queue` for the consumer's retry loop.
    if backoff_until.is_none() {
        while let Some(mut intent) = queue.pop_front() {
            let request = build_heal_request(&intent);
            match manager.submit_heal_request(request).await {
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
    replayed
}

/// Replay the journal, then keep draining the channel into the heal manager
/// while persisting the pending snapshot.
async fn run_mrf_consumer(manager: Arc<HealManager>, mut receiver: mpsc::Receiver<MrfIntent>) {
    let config = MrfConsumerConfig::default();
    let mut runtime = MrfRuntime {
        queue: MrfQueue::new(config.queue_capacity, config.journal_max_bytes),
        config: config.clone(),
        new_since_flush: 0,
        journal_on_disk: false,
        backoff_until: None,
    };

    // Replay: read the journal, re-arm intents (duplicates are merged by the
    // manager's dedup key), then drop the file so the next flush starts clean.
    replay_into(&manager, &mut runtime.queue, &mut runtime.backoff_until).await;

    let mut flush_tick = tokio::time::interval(runtime.config.flush_interval);
    flush_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut batch: Vec<MrfIntent> = Vec::with_capacity(runtime.config.replay_batch);

    loop {
        tokio::select! {
            received = receiver.recv_many(&mut batch, runtime.config.replay_batch) => {
                if received == 0 {
                    // Channel closed: flush once more and stop.
                    runtime.flush().await;
                    tracing::info!(
                        target: "rustfs::heal::mrf",
                        "MRF channel closed; consumer stopped after final flush"
                    );
                    return;
                }
                for intent in batch.drain(..) {
                    runtime.queue.try_push(intent);
                    runtime.new_since_flush += 1;
                }
                runtime.dispatch(manager.as_ref()).await;
                if runtime.new_since_flush >= runtime.config.flush_threshold {
                    runtime.flush().await;
                }
            }
            _ = flush_tick.tick() => {
                if runtime.new_since_flush > 0 || runtime.queue.depth() > 0 {
                    runtime.flush().await;
                    runtime.dispatch(manager.as_ref()).await;
                } else if runtime.journal_on_disk {
                    // All intents consumed: remove the journal so a restart
                    // replays nothing (mirrors MinIO's post-replay unlink).
                    delete_journal().await;
                    runtime.journal_on_disk = false;
                    gauge!("rustfs_heal_mrf_journal_bytes").set(0.0);
                }
                gauge!("rustfs_heal_mrf_queue_depth").set(runtime.queue.depth() as f64);
            }
        }
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
            enqueued_at_ms: 1_700_000_000_000,
            attempts,
        }
    }

    #[test]
    fn queue_enforces_count_and_byte_ceilings() {
        let mut queue = MrfQueue::new(2, usize::MAX);
        assert!(queue.try_push(intent("b", "o", 0)));
        assert!(queue.try_push(intent("b", "o", 0)));
        assert!(!queue.try_push(intent("b", "o", 0)), "count ceiling must drop");

        let mut tiny = MrfQueue::new(usize::MAX, intent("bucket", "object", 0).estimated_bytes());
        assert!(tiny.try_push(intent("bucket", "object", 0)));
        assert!(
            !tiny.try_push(intent("bucket", "object", 0)),
            "byte budget must drop before the second intent fits"
        );
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
            enqueued_at_ms: 0,
            attempts: 0,
        });
        assert!(matches!(partial.heal_type, HealType::Object { .. }));
        assert_eq!(partial.priority, HealPriority::Normal);
    }
}
