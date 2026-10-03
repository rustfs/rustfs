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

use super::{ECStore, Error, ecstore_config};
use rustfs_scanner::{
    ScannerDirtyUsageBucket, ScannerDurableDirtyUsageReplayEntry, ScannerDurableDirtyUsageReplayRecord,
    ScannerDurableDirtyUsageReplayScope, SegmentInvalidationProducerIdentity,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;
use tokio::sync::Notify;
use tracing::{debug, warn};

const LOG_COMPONENT_STORAGE: &str = "storage";
const LOG_SUBSYSTEM_SCANNER: &str = "scanner";
const EVENT_SCANNER_DIRTY_USAGE_JOURNAL: &str = "scanner_dirty_usage_journal";
const DURABLE_DIRTY_USAGE_REPLAY_PREFIX: &str = "scanner/durable-dirty-producer-replay";
const DURABLE_DIRTY_USAGE_REPLAY_MAX_BYTES: usize = 64 * 1024;
const DURABLE_DIRTY_USAGE_REPLAY_MAX_ENTRIES: usize = 1024;
const DURABLE_DIRTY_USAGE_REPLAY_MAX_TOP_LEVEL_ENTRIES: usize = 128;
const DURABLE_DIRTY_USAGE_JOURNAL_FLUSH_INTERVAL: Duration = Duration::from_secs(1);
const DURABLE_DIRTY_USAGE_JOURNAL_RETRY_INTERVAL: Duration = Duration::from_secs(5);

#[derive(Clone)]
pub(crate) struct DurableDirtyUsageJournal {
    shared: Option<Arc<SharedJournal>>,
}

struct SharedJournal {
    pending: Mutex<PendingJournal>,
    changed: Arc<Notify>,
    #[cfg(test)]
    failed_attempts: std::sync::atomic::AtomicU64,
}

#[derive(Default)]
struct PendingJournal {
    state: JournalState,
    revision: u64,
    persisted_revision: u64,
}

impl PendingJournal {
    fn changed(&mut self) {
        self.revision = self.revision.saturating_add(1);
    }

    fn confirm_flush(&mut self, revision: u64) {
        // A save cannot acknowledge mutations received after its snapshot.
        if revision == self.revision && revision != u64::MAX {
            self.persisted_revision = revision;
        }
    }
}

impl DurableDirtyUsageJournal {
    pub(crate) fn record_committed_mutation(&self, bucket: &str, object: &str, producer: SegmentInvalidationProducerIdentity) {
        let Some(shared) = &self.shared else { return };
        let generation = rustfs_scanner::scanner_dirty_usage_bucket_generation(bucket).unwrap_or(0);
        {
            // Release the scanner dirty-map lock first; no journal guard spans I/O.
            let mut pending = shared.pending.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            pending
                .state
                .record_mutation(bucket.to_string(), object, generation, producer);
            pending.changed();
        }
        shared.changed.notify_one();
    }

    pub(crate) fn clear_confirmed_buckets(&self, cleared: Vec<ScannerDirtyUsageBucket>) {
        let Some(shared) = &self.shared else { return };
        {
            let mut pending = shared.pending.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            pending.state.clear_buckets(&cleared);
            // Observer callbacks run after scanner locks are released. Recheck under
            // the journal lock so an intervening unknown mutation cannot lose its marker.
            if pending.state.buckets.is_empty() && !rustfs_scanner::scanner_dirty_usage_state().pending {
                pending.state.invalidated = false;
            }
            pending.changed();
        }
        shared.changed.notify_one();
    }
}

impl Drop for DurableDirtyUsageJournal {
    fn drop(&mut self) {
        if let Some(shared) = &self.shared {
            // An idle worker owns only a Weak reference and exits after its owner drops.
            shared.changed.notify_one();
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct OwnedReplayRecord {
    owner: String,
    replay: Option<ScannerDurableDirtyUsageReplayRecord>,
}

fn durable_dirty_usage_owner(node_name: &str) -> Option<String> {
    (!node_name.is_empty()).then(|| {
        let mut hash = Sha256::new();
        hash.update(b"rustfs/scanner/dirty-journal/owner/v1\0");
        hash.update(node_name.as_bytes());
        hex_simd::encode_to_string(hash.finalize(), hex_simd::AsciiCase::Lower)
    })
}

fn durable_dirty_usage_replay_object(owner: &str) -> String {
    format!("{DURABLE_DIRTY_USAGE_REPLAY_PREFIX}/{owner}.json")
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct JournalState {
    buckets: BTreeMap<String, JournalBucketState>,
    scope_bytes: usize,
    invalidated: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct JournalBucketState {
    generation: u64,
    scope: JournalScope,
    producers: BTreeSet<SegmentInvalidationProducerIdentity>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum JournalScope {
    WholeBucket,
    TopLevelEntries(BTreeSet<String>),
}

impl JournalState {
    fn record_mutation(&mut self, bucket: String, object: &str, generation: u64, producer: SegmentInvalidationProducerIdentity) {
        if bucket.is_empty() || generation == 0 || generation == u64::MAX || !durable_dirty_usage_producer_is_supported(producer)
        {
            self.buckets.clear();
            self.scope_bytes = 0;
            self.invalidated = true;
            return;
        }

        let state = self.buckets.entry(bucket).or_insert_with(|| JournalBucketState {
            generation,
            scope: JournalScope::TopLevelEntries(BTreeSet::new()),
            producers: BTreeSet::new(),
        });
        self.scope_bytes = self.scope_bytes.saturating_sub(journal_scope_bytes(&state.scope));
        state.generation = state.generation.max(generation);
        merge_journal_scope(&mut state.scope, durable_dirty_usage_journal_scope(object));
        state.producers.insert(producer);
        let bytes = journal_scope_bytes(&state.scope);
        if self.scope_bytes.saturating_add(bytes) > DURABLE_DIRTY_USAGE_REPLAY_MAX_BYTES {
            state.scope = JournalScope::WholeBucket;
        } else {
            self.scope_bytes += bytes;
        }
        if self.buckets.len() > DURABLE_DIRTY_USAGE_REPLAY_MAX_ENTRIES {
            self.buckets.clear();
            self.scope_bytes = 0;
            self.invalidated = true;
        }
    }

    fn clear_buckets(&mut self, cleared: &[ScannerDirtyUsageBucket]) {
        for entry in cleared {
            if self
                .buckets
                .get(&entry.bucket)
                .is_some_and(|state| state.generation <= entry.generation)
                && let Some(removed) = self.buckets.remove(&entry.bucket)
            {
                self.scope_bytes = self.scope_bytes.saturating_sub(journal_scope_bytes(&removed.scope));
            }
        }
    }

    fn replay_entries(&self) -> Option<Vec<ScannerDurableDirtyUsageReplayEntry>> {
        if self.invalidated || self.buckets.is_empty() || self.buckets.len() > DURABLE_DIRTY_USAGE_REPLAY_MAX_ENTRIES {
            return None;
        }
        let mut entries = Vec::with_capacity(self.buckets.len());
        for (bucket, state) in &self.buckets {
            if state.producers.is_empty() {
                return None;
            }
            let scope = match &state.scope {
                JournalScope::WholeBucket => ScannerDurableDirtyUsageReplayScope::WholeBucket,
                JournalScope::TopLevelEntries(entries) => {
                    if entries.is_empty() || entries.len() > DURABLE_DIRTY_USAGE_REPLAY_MAX_TOP_LEVEL_ENTRIES {
                        return None;
                    }
                    ScannerDurableDirtyUsageReplayScope::TopLevelEntries {
                        entries: entries.clone(),
                    }
                }
            };
            entries.push(ScannerDurableDirtyUsageReplayEntry {
                bucket: bucket.clone(),
                generation: state.generation,
                scope,
                producers: state.producers.clone(),
            });
        }
        Some(entries)
    }

    fn from_replay_record(record: ScannerDurableDirtyUsageReplayRecord) -> Option<Self> {
        if record.entries.is_empty() || record.entries.len() > DURABLE_DIRTY_USAGE_REPLAY_MAX_ENTRIES {
            return None;
        }
        let mut state = JournalState::default();
        for entry in record.entries {
            if entry.bucket.is_empty()
                || entry.bucket.contains(['/', '\\', '\0'])
                || entry.bucket == "."
                || entry.bucket == ".."
                || entry.generation == 0
                || entry.generation == u64::MAX
                || entry.producers.is_empty()
                || entry
                    .producers
                    .iter()
                    .any(|producer| !durable_dirty_usage_producer_is_supported(*producer))
            {
                return None;
            }
            let scope = match entry.scope {
                ScannerDurableDirtyUsageReplayScope::WholeBucket => JournalScope::WholeBucket,
                ScannerDurableDirtyUsageReplayScope::TopLevelEntries { entries } => {
                    if entries.is_empty() || entries.len() > DURABLE_DIRTY_USAGE_REPLAY_MAX_TOP_LEVEL_ENTRIES {
                        return None;
                    }
                    if entries
                        .iter()
                        .any(|entry| durable_dirty_usage_top_level_entry(entry).as_deref() != Some(entry.as_str()))
                    {
                        return None;
                    }
                    JournalScope::TopLevelEntries(entries)
                }
            };
            state.scope_bytes += journal_scope_bytes(&scope);
            if state
                .buckets
                .insert(
                    entry.bucket,
                    JournalBucketState {
                        generation: entry.generation,
                        scope,
                        producers: entry.producers,
                    },
                )
                .is_some()
            {
                return None;
            }
        }
        Some(state)
    }
}

pub(crate) async fn start_durable_dirty_usage_journal(store: Arc<ECStore>) -> DurableDirtyUsageJournal {
    // ECStore initializes this stable endpoint authority before AppContext exists.
    let node_name = rustfs_common::get_global_local_node_name().await;
    let Some(owner) = durable_dirty_usage_owner(&node_name) else {
        warn!(
            event = EVENT_SCANNER_DIRTY_USAGE_JOURNAL,
            component = LOG_COMPONENT_STORAGE,
            subsystem = LOG_SUBSYSTEM_SCANNER,
            state = "owner_unavailable",
            "Scanner dirty journal owner is unavailable"
        );
        return DurableDirtyUsageJournal { shared: None };
    };
    let state = replay_durable_dirty_usage_journal(store.clone(), &owner).await;
    let changed = Arc::new(Notify::new());
    let shared = Arc::new(SharedJournal {
        pending: Mutex::new(PendingJournal {
            state,
            ..Default::default()
        }),
        changed: changed.clone(),
        #[cfg(test)]
        failed_attempts: std::sync::atomic::AtomicU64::new(0),
    });
    tokio::spawn(run_durable_dirty_usage_journal(store, owner, Arc::downgrade(&shared), changed));
    DurableDirtyUsageJournal { shared: Some(shared) }
}

async fn replay_durable_dirty_usage_journal(store: Arc<ECStore>, owner: &str) -> JournalState {
    let path = durable_dirty_usage_replay_object(owner);
    let bytes = match super::read_config(store, &path).await {
        Ok(bytes) if !bytes.is_empty() => bytes,
        Ok(_) | Err(Error::ConfigNotFound) => return JournalState::default(),
        Err(err) => {
            warn!(
                event = EVENT_SCANNER_DIRTY_USAGE_JOURNAL,
                component = LOG_COMPONENT_STORAGE,
                subsystem = LOG_SUBSYSTEM_SCANNER,
                state = "read_failed",
                error = ?err,
                "Scanner dirty journal could not be read"
            );
            return JournalState {
                invalidated: true,
                ..Default::default()
            };
        }
    };
    let Some((state, replay)) = decode_owned_replay_record(owner, &bytes) else {
        warn!(
            event = EVENT_SCANNER_DIRTY_USAGE_JOURNAL,
            component = LOG_COMPONENT_STORAGE,
            subsystem = LOG_SUBSYSTEM_SCANNER,
            state = "replay_unverified",
            "Scanner dirty journal requires a full scan"
        );
        return JournalState {
            invalidated: true,
            ..Default::default()
        };
    };
    match rustfs_scanner::replay_durable_dirty_usage_producer_record(&replay) {
        Ok(replayed) => {
            debug!(
                event = EVENT_SCANNER_DIRTY_USAGE_JOURNAL,
                component = LOG_COMPONENT_STORAGE,
                subsystem = LOG_SUBSYSTEM_SCANNER,
                state = "replayed",
                generation = replayed.generation,
                pending = replayed.pending,
                "Scanner dirty journal replayed"
            );
            state
        }
        Err(err) => {
            warn!(
                event = EVENT_SCANNER_DIRTY_USAGE_JOURNAL,
                component = LOG_COMPONENT_STORAGE,
                subsystem = LOG_SUBSYSTEM_SCANNER,
                state = "replay_rejected",
                error = ?err,
                "Scanner dirty journal was rejected"
            );
            JournalState {
                invalidated: true,
                ..Default::default()
            }
        }
    }
}

fn decode_owned_replay_record(owner: &str, bytes: &[u8]) -> Option<(JournalState, Vec<u8>)> {
    if bytes.len() > DURABLE_DIRTY_USAGE_REPLAY_MAX_BYTES {
        return None;
    }
    let record: OwnedReplayRecord = serde_json::from_slice(bytes).ok()?;
    if record.owner != owner {
        return None;
    }
    let replay = record.replay?;
    let bytes = serde_json::to_vec(&replay).ok()?;
    let state = JournalState::from_replay_record(replay)?;
    Some((state, bytes))
}

fn encode_owned_replay_record(owner: &str, state: &JournalState) -> Result<Vec<u8>, serde_json::Error> {
    let mut entries = state.replay_entries();
    for compact in [false, true] {
        if let Some(entries) = &mut entries {
            if compact {
                for entry in entries.iter_mut() {
                    entry.scope = ScannerDurableDirtyUsageReplayScope::WholeBucket;
                }
            }
            if let Ok(replay) = rustfs_scanner::encode_durable_dirty_usage_producer_replay_record(entries.clone())
                && let Ok(replay) = serde_json::from_slice(&replay)
            {
                let bytes = serde_json::to_vec(&OwnedReplayRecord {
                    owner: owner.to_string(),
                    replay: Some(replay),
                })?;
                if bytes.len() <= DURABLE_DIRTY_USAGE_REPLAY_MAX_BYTES {
                    return Ok(bytes);
                }
            }
        }
    }
    // Never label a truncated record as complete producer coverage.
    serde_json::to_vec(&OwnedReplayRecord {
        owner: owner.to_string(),
        replay: None,
    })
}

async fn run_durable_dirty_usage_journal(store: Arc<ECStore>, owner: String, shared: Weak<SharedJournal>, changed: Arc<Notify>) {
    let mut next_flush = tokio::time::Instant::now() + DURABLE_DIRTY_USAGE_JOURNAL_FLUSH_INTERVAL;
    loop {
        changed.notified().await;
        if shared.strong_count() == 0 {
            break;
        }
        tokio::time::sleep_until(next_flush).await;
        let Some(current) = shared.upgrade() else { break };
        let snapshot = {
            let pending = current.pending.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            (pending.revision != pending.persisted_revision).then(|| (pending.state.clone(), pending.revision))
        };
        drop(current);
        let Some((snapshot, revision)) = snapshot else { continue };
        let saved = flush_durable_dirty_usage_journal(store.clone(), &owner, &snapshot).await;
        next_flush = tokio::time::Instant::now()
            + if saved {
                DURABLE_DIRTY_USAGE_JOURNAL_FLUSH_INTERVAL
            } else {
                DURABLE_DIRTY_USAGE_JOURNAL_RETRY_INTERVAL
            };
        let Some(current) = shared.upgrade() else { break };
        #[cfg(test)]
        if !saved {
            current.failed_attempts.fetch_add(1, std::sync::atomic::Ordering::Release);
        }
        let mut pending = current.pending.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if saved {
            pending.confirm_flush(revision);
        }
        if pending.revision != pending.persisted_revision {
            changed.notify_one();
        }
    }
}

async fn flush_durable_dirty_usage_journal(store: Arc<ECStore>, owner: &str, state: &JournalState) -> bool {
    let path = durable_dirty_usage_replay_object(owner);
    let result = if state.buckets.is_empty() && !state.invalidated {
        ecstore_config::com::delete_config(store, &path).await
    } else {
        let bytes = match encode_owned_replay_record(owner, state) {
            Ok(bytes) => bytes,
            Err(err) => {
                warn!(
                    event = EVENT_SCANNER_DIRTY_USAGE_JOURNAL,
                    component = LOG_COMPONENT_STORAGE,
                    subsystem = LOG_SUBSYSTEM_SCANNER,
                    state = "encode_failed",
                    error = ?err,
                    "Scanner dirty journal could not be encoded"
                );
                return false;
            }
        };
        ecstore_config::com::save_config(store, &path, bytes).await
    };
    match result {
        Ok(()) | Err(Error::ConfigNotFound) => true,
        Err(err) => {
            warn!(
                event = EVENT_SCANNER_DIRTY_USAGE_JOURNAL,
                component = LOG_COMPONENT_STORAGE,
                subsystem = LOG_SUBSYSTEM_SCANNER,
                state = "write_failed",
                error = ?err,
                "Scanner dirty journal persistence failed"
            );
            false
        }
    }
}

fn journal_scope_bytes(scope: &JournalScope) -> usize {
    match scope {
        JournalScope::WholeBucket => 0,
        JournalScope::TopLevelEntries(entries) => entries.iter().map(String::len).sum(),
    }
}

fn durable_dirty_usage_journal_scope(object: &str) -> JournalScope {
    match durable_dirty_usage_top_level_entry(object) {
        Some(entry) => JournalScope::TopLevelEntries(BTreeSet::from([entry])),
        None => JournalScope::WholeBucket,
    }
}

fn merge_journal_scope(current: &mut JournalScope, incoming: JournalScope) {
    let overflowed = match (&mut *current, incoming) {
        (JournalScope::WholeBucket, _) | (_, JournalScope::WholeBucket) => {
            *current = JournalScope::WholeBucket;
            false
        }
        (JournalScope::TopLevelEntries(entries), JournalScope::TopLevelEntries(incoming)) => {
            entries.extend(incoming);
            entries.len() > DURABLE_DIRTY_USAGE_REPLAY_MAX_TOP_LEVEL_ENTRIES
        }
    };
    if overflowed {
        *current = JournalScope::WholeBucket;
    }
}

fn durable_dirty_usage_producer_is_supported(producer: SegmentInvalidationProducerIdentity) -> bool {
    matches!(
        producer,
        SegmentInvalidationProducerIdentity::PutObject
            | SegmentInvalidationProducerIdentity::DeleteObject
            | SegmentInvalidationProducerIdentity::DeleteMarker
            | SegmentInvalidationProducerIdentity::CompleteMultipartUpload
            | SegmentInvalidationProducerIdentity::AbortMultipartUpload
            | SegmentInvalidationProducerIdentity::ObjectMetadata
            | SegmentInvalidationProducerIdentity::BucketMetadata
            | SegmentInvalidationProducerIdentity::Replication
            | SegmentInvalidationProducerIdentity::TierTransition
            | SegmentInvalidationProducerIdentity::TierExpiration
            | SegmentInvalidationProducerIdentity::DirectoryObject
    )
}

fn durable_dirty_usage_top_level_entry(object: &str) -> Option<String> {
    let (top_level_entry, _) = object.split_once('/').unwrap_or((object, ""));
    (!top_level_entry.is_empty()
        && top_level_entry != "."
        && top_level_entry != ".."
        && !object.starts_with('/')
        && !top_level_entry.contains(['\\', '\0']))
    .then(|| top_level_entry.to_string())
}

#[cfg(test)]
mod tests {
    use super::{
        DURABLE_DIRTY_USAGE_REPLAY_MAX_BYTES, DURABLE_DIRTY_USAGE_REPLAY_MAX_TOP_LEVEL_ENTRIES, DurableDirtyUsageJournal,
        JournalState, OwnedReplayRecord, PendingJournal, SharedJournal, decode_owned_replay_record, durable_dirty_usage_owner,
        durable_dirty_usage_replay_object, encode_owned_replay_record, flush_durable_dirty_usage_journal,
        replay_durable_dirty_usage_journal, run_durable_dirty_usage_journal,
    };
    use rustfs_scanner::{
        ScannerDirtyUsageBucket, ScannerDurableDirtyUsageReplayEntry, ScannerDurableDirtyUsageReplayRecord,
        ScannerDurableDirtyUsageReplayScope, SegmentInvalidationProducerIdentity,
    };
    use std::collections::BTreeSet;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use tokio::sync::Notify;

    async fn journal_test_env() -> &'static rustfs_test_utils::TestECStoreEnv {
        // Bucket metadata owns a process-global Weak store reference. Keep its
        // fixture alive across serialized journal cases in the same test process.
        static ENV: tokio::sync::OnceCell<rustfs_test_utils::TestECStoreEnv> = tokio::sync::OnceCell::const_new();
        ENV.get_or_init(|| rustfs_test_utils::TestECStoreEnv::builder().build()).await
    }

    #[test]
    fn journal_owner_is_stable_and_never_defaults_to_a_shared_identity() {
        assert!(durable_dirty_usage_owner("").is_none());
        let first = durable_dirty_usage_owner("node-a:9000").expect("configured owner");
        assert_eq!(Some(first.clone()), durable_dirty_usage_owner("node-a:9000"));
        assert_ne!(Some(first), durable_dirty_usage_owner("node-b:9000"));
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn owned_journals_keep_peer_records_after_interleaved_writes_and_clear() {
        let env = journal_test_env().await;
        let owner_a = durable_dirty_usage_owner("node-a:9000").expect("owner a");
        let owner_b = durable_dirty_usage_owner("node-b:9000").expect("owner b");
        let mut state_a = JournalState::default();
        let mut state_b = JournalState::default();
        state_a.record_mutation("bucket-a".to_string(), "a/object", 7, SegmentInvalidationProducerIdentity::PutObject);
        state_b.record_mutation("bucket-b".to_string(), "b/object", 3, SegmentInvalidationProducerIdentity::PutObject);

        let legacy_path = "scanner/durable-dirty-producer-replay.json";
        let legacy =
            rustfs_scanner::encode_durable_dirty_usage_producer_replay_record(state_a.replay_entries().expect("legacy entries"))
                .expect("legacy bytes");
        super::ecstore_config::com::save_config(env.ecstore.clone(), legacy_path, legacy.clone())
            .await
            .expect("legacy save");
        assert!(flush_durable_dirty_usage_journal(env.ecstore.clone(), &owner_a, &state_a).await);
        assert!(flush_durable_dirty_usage_journal(env.ecstore.clone(), &owner_b, &state_b).await);
        let path_b = durable_dirty_usage_replay_object(&owner_b);
        let bytes_b = super::super::read_config(env.ecstore.clone(), &path_b)
            .await
            .expect("read owner b");
        assert!(decode_owned_replay_record(&owner_a, &bytes_b).is_none());
        assert!(decode_owned_replay_record(&owner_a, &legacy).is_none());

        state_a.clear_buckets(&[ScannerDirtyUsageBucket {
            bucket: "bucket-a".to_string(),
            generation: 7,
        }]);
        assert!(flush_durable_dirty_usage_journal(env.ecstore.clone(), &owner_a, &state_a).await);
        assert_eq!(
            super::super::read_config(env.ecstore.clone(), &path_b)
                .await
                .expect("peer survives"),
            bytes_b
        );
        assert_eq!(
            super::super::read_config(env.ecstore.clone(), legacy_path)
                .await
                .expect("legacy retained"),
            legacy
        );
        let replayed = replay_durable_dirty_usage_journal(env.ecstore.clone(), &owner_b).await;
        assert_eq!(replayed, state_b);
        assert!(matches!(
            super::super::read_config(env.ecstore.clone(), &durable_dirty_usage_replay_object(&owner_a)).await,
            Err(super::Error::ConfigNotFound)
        ));
        let dirty = rustfs_scanner::scanner_dirty_usage_state();
        rustfs_scanner::acknowledge_dirty_usage_generation(rustfs_scanner::scanner_activity_epoch(), dirty.generation)
            .expect("clear test replay");
    }

    #[test]
    fn owned_journal_budget_compacts_scopes_or_records_unverified_coverage() {
        let owner = durable_dirty_usage_owner("node-a:9000").expect("owner");
        let mut state = JournalState::default();
        for bucket in 0..3 {
            for entry in 0..128 {
                state.record_mutation(
                    format!("bucket-{bucket}"),
                    &format!("{entry:03}{}/object", "\"".repeat(197)),
                    7,
                    SegmentInvalidationProducerIdentity::PutObject,
                );
            }
        }
        assert!(state.scope_bytes <= DURABLE_DIRTY_USAGE_REPLAY_MAX_BYTES);
        let bytes = encode_owned_replay_record(&owner, &state).expect("bounded record");
        assert!(bytes.len() <= DURABLE_DIRTY_USAGE_REPLAY_MAX_BYTES);
        let (decoded, _) = decode_owned_replay_record(&owner, &bytes).expect("compacted replay");
        assert_eq!(decoded.buckets.len(), 3);
        assert!(
            decoded
                .replay_entries()
                .expect("entries")
                .iter()
                .all(|entry| entry.scope == ScannerDurableDirtyUsageReplayScope::WholeBucket)
        );
        state = JournalState::default();
        for bucket in 0..1024 {
            for producer in SegmentInvalidationProducerIdentity::REQUIRED_PRODUCTION {
                state.record_mutation(format!("bucket-{bucket:04}-{}", "x".repeat(48)), "object", 9, producer);
            }
        }
        assert!(!state.invalidated);
        assert_eq!(state.buckets.len(), 1024);
        let bytes = encode_owned_replay_record(&owner, &state).expect("unverified marker");
        assert!(bytes.len() <= DURABLE_DIRTY_USAGE_REPLAY_MAX_BYTES);
        let record: OwnedReplayRecord = serde_json::from_slice(&bytes).expect("marker");
        assert!(record.replay.is_none());
        assert!(decode_owned_replay_record(&owner, &bytes).is_none());
    }

    #[test]
    fn an_older_flush_cannot_confirm_a_new_mutation_or_exhausted_revision() {
        let mut pending = PendingJournal::default();
        pending.changed();
        let saving = pending.revision;
        pending.changed();
        pending.confirm_flush(saving);
        assert_ne!(pending.revision, pending.persisted_revision);
        pending.confirm_flush(pending.revision);
        assert_eq!(pending.revision, pending.persisted_revision);
        pending.revision = u64::MAX;
        pending.changed();
        pending.confirm_flush(u64::MAX);
        assert_ne!(pending.revision, pending.persisted_revision);
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn clear_callback_keeps_unknown_coverage_until_all_pending_work_is_confirmed() {
        let env = journal_test_env().await;
        let owner = durable_dirty_usage_owner("clear:9000").expect("owner");
        let shared = Arc::new(SharedJournal {
            pending: Mutex::new(PendingJournal::default()),
            changed: Arc::new(Notify::new()),
            failed_attempts: std::sync::atomic::AtomicU64::new(0),
        });
        let handle = DurableDirtyUsageJournal {
            shared: Some(shared.clone()),
        };
        rustfs_scanner::record_dirty_usage_bucket("unidentified");
        handle.record_committed_mutation("unidentified", "", SegmentInvalidationProducerIdentity::Unknown);
        rustfs_scanner::record_dirty_usage_object_from_producer(
            "identified",
            "prefix/object",
            SegmentInvalidationProducerIdentity::PutObject,
        );
        handle.record_committed_mutation("identified", "prefix/object", SegmentInvalidationProducerIdentity::PutObject);
        let old = rustfs_scanner::scanner_dirty_usage_bucket_generation("identified").expect("old generation");
        let snapshot = shared.pending.lock().expect("state").state.clone();
        assert!(snapshot.invalidated);
        assert!(snapshot.scope_bytes > 0);
        assert!(flush_durable_dirty_usage_journal(env.ecstore.clone(), &owner, &snapshot).await);

        rustfs_scanner::clear_dirty_usage_bucket("identified");
        handle.clear_confirmed_buckets(vec![ScannerDirtyUsageBucket {
            bucket: "identified".to_string(),
            generation: old,
        }]);
        {
            let pending = shared.pending.lock().expect("partial clear");
            assert!(pending.state.invalidated, "the unknown bucket still has pending work");
            assert_eq!(pending.state.scope_bytes, 0);
        }
        rustfs_scanner::record_dirty_usage_object_from_producer(
            "identified",
            "new/object",
            SegmentInvalidationProducerIdentity::PutObject,
        );
        handle.record_committed_mutation("identified", "new/object", SegmentInvalidationProducerIdentity::PutObject);
        handle.clear_confirmed_buckets(vec![ScannerDirtyUsageBucket {
            bucket: "identified".to_string(),
            generation: old,
        }]);
        let latest = rustfs_scanner::scanner_dirty_usage_bucket_generation("identified").expect("latest generation");
        assert!(latest > old);
        {
            let pending = shared.pending.lock().expect("stale clear");
            assert!(pending.state.invalidated);
            assert_eq!(pending.state.buckets["identified"].generation, latest);
            assert!(pending.state.scope_bytes > 0);
        }
        let dirty = rustfs_scanner::scanner_dirty_usage_state();
        rustfs_scanner::acknowledge_dirty_usage_generation(rustfs_scanner::scanner_activity_epoch(), dirty.generation)
            .expect("verified full clear");
        handle.clear_confirmed_buckets(vec![ScannerDirtyUsageBucket {
            bucket: "identified".to_string(),
            generation: latest,
        }]);
        let cleared = shared.pending.lock().expect("full clear").state.clone();
        assert!(!cleared.invalidated);
        assert!(cleared.buckets.is_empty());
        assert_eq!(cleared.scope_bytes, 0);
        assert!(flush_durable_dirty_usage_journal(env.ecstore.clone(), &owner, &cleared).await);
        assert!(matches!(
            super::super::read_config(env.ecstore.clone(), &durable_dirty_usage_replay_object(&owner)).await,
            Err(super::Error::ConfigNotFound)
        ));
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn journal_worker_retries_failed_storage_without_a_new_mutation() {
        let env = journal_test_env().await;
        let owner = durable_dirty_usage_owner("retry:9000").expect("owner");
        let mut blocked = Vec::new();
        for disk in &env.disk_paths {
            let path = disk.join(".rustfs.sys/scanner/durable-dirty-producer-replay");
            std::fs::create_dir_all(path.parent().expect("parent")).expect("scanner directory");
            let backup = path.with_extension("retry-backup");
            let existed = path.exists();
            if existed {
                std::fs::rename(&path, &backup).expect("retain other journal records during fault injection");
            }
            std::fs::write(&path, b"not a directory").expect("block journal writes");
            blocked.push((path, backup, existed));
        }
        let mut pending = PendingJournal::default();
        pending
            .state
            .record_mutation("retry".to_string(), "prefix/object", 7, SegmentInvalidationProducerIdentity::PutObject);
        pending.changed();
        let changed = Arc::new(Notify::new());
        let shared = Arc::new(SharedJournal {
            pending: Mutex::new(pending),
            changed: changed.clone(),
            failed_attempts: std::sync::atomic::AtomicU64::new(0),
        });
        let handle = DurableDirtyUsageJournal {
            shared: Some(shared.clone()),
        };
        let worker = tokio::spawn(run_durable_dirty_usage_journal(
            env.ecstore.clone(),
            owner.clone(),
            Arc::downgrade(&shared),
            changed.clone(),
        ));
        changed.notify_one();
        tokio::time::timeout(Duration::from_secs(20), async {
            while shared.failed_attempts.load(std::sync::atomic::Ordering::Acquire) == 0 {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("real storage failure must be observed");
        assert_eq!(shared.pending.lock().expect("failed state").persisted_revision, 0);
        for (path, backup, existed) in blocked {
            std::fs::remove_file(&path).expect("restore journal writes");
            if existed {
                std::fs::rename(backup, path).expect("restore other journal records");
            }
        }
        tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                let confirmed = {
                    let pending = shared.pending.lock().expect("retry state");
                    pending.persisted_revision == pending.revision
                };
                if confirmed {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("retry must persist the original mutation without another notification");
        let bytes = super::super::read_config(env.ecstore.clone(), &durable_dirty_usage_replay_object(&owner))
            .await
            .expect("retry bytes");
        let (saved, _) = decode_owned_replay_record(&owner, &bytes).expect("retry replay");
        assert_eq!(saved.buckets["retry"].generation, 7);
        drop(shared);
        drop(handle);
        tokio::time::timeout(Duration::from_secs(5), worker)
            .await
            .expect("worker stops")
            .expect("worker task");
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn journal_worker_coalesces_mutations_and_releases_its_store() {
        let env = journal_test_env().await;
        let owner = durable_dirty_usage_owner("worker:9000").expect("owner");
        let changed = Arc::new(Notify::new());
        let shared = Arc::new(SharedJournal {
            pending: Mutex::new(PendingJournal::default()),
            changed: changed.clone(),
            failed_attempts: std::sync::atomic::AtomicU64::new(0),
        });
        let handle = DurableDirtyUsageJournal {
            shared: Some(shared.clone()),
        };
        let worker = tokio::spawn(run_durable_dirty_usage_journal(
            env.ecstore.clone(),
            owner.clone(),
            Arc::downgrade(&shared),
            changed,
        ));
        for entry in 0..1000 {
            let object = format!("prefix/object-{entry}");
            rustfs_scanner::record_dirty_usage_object_from_producer(
                "coalesced",
                &object,
                SegmentInvalidationProducerIdentity::PutObject,
            );
            handle.record_committed_mutation("coalesced", &object, SegmentInvalidationProducerIdentity::PutObject);
        }
        let expected = rustfs_scanner::scanner_dirty_usage_bucket_generation("coalesced").expect("generation");
        let path = durable_dirty_usage_replay_object(&owner);
        tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                if let Ok(bytes) = super::super::read_config(env.ecstore.clone(), &path).await
                    && let Some((state, _)) = decode_owned_replay_record(&owner, &bytes)
                    && state
                        .buckets
                        .get("coalesced")
                        .is_some_and(|bucket| bucket.generation == expected)
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("latest coalesced mutation should persist");
        drop(shared);
        drop(handle);
        tokio::time::timeout(Duration::from_secs(5), worker)
            .await
            .expect("worker should stop")
            .expect("worker task");
        let dirty = rustfs_scanner::scanner_dirty_usage_state();
        rustfs_scanner::acknowledge_dirty_usage_generation(rustfs_scanner::scanner_activity_epoch(), dirty.generation)
            .expect("clear worker test");
    }

    #[test]
    fn journal_state_merges_scopes_and_clears_only_confirmed_generations() {
        let mut state = JournalState::default();
        state.record_mutation("photos".to_string(), "2026/object-a", 7, SegmentInvalidationProducerIdentity::Replication);
        state.record_mutation(
            "photos".to_string(),
            "archive/object-b",
            9,
            SegmentInvalidationProducerIdentity::TierExpiration,
        );

        let entries = state.replay_entries().expect("journal should encode a bounded bucket");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].generation, 9);
        assert_eq!(
            entries[0].producers,
            BTreeSet::from([
                SegmentInvalidationProducerIdentity::Replication,
                SegmentInvalidationProducerIdentity::TierExpiration
            ])
        );
        assert_eq!(
            entries[0].scope,
            ScannerDurableDirtyUsageReplayScope::TopLevelEntries {
                entries: BTreeSet::from(["2026".to_string(), "archive".to_string()])
            }
        );

        state.clear_buckets(&[ScannerDirtyUsageBucket {
            bucket: "photos".to_string(),
            generation: 7,
        }]);
        assert!(state.buckets.contains_key("photos"));
        state.clear_buckets(&[ScannerDirtyUsageBucket {
            bucket: "photos".to_string(),
            generation: 9,
        }]);
        assert!(state.buckets.is_empty());
    }

    #[test]
    fn journal_state_expands_to_whole_bucket_for_ambiguous_or_overflowing_scopes() {
        let mut ambiguous = JournalState::default();
        ambiguous.record_mutation("photos".to_string(), "../bad", 7, SegmentInvalidationProducerIdentity::Replication);
        assert_eq!(
            ambiguous.replay_entries().expect("ambiguous object should still encode")[0].scope,
            ScannerDurableDirtyUsageReplayScope::WholeBucket
        );

        let mut overflow = JournalState::default();
        for index in 0..=DURABLE_DIRTY_USAGE_REPLAY_MAX_TOP_LEVEL_ENTRIES {
            overflow.record_mutation(
                "photos".to_string(),
                &format!("prefix-{index}/object"),
                u64::try_from(index + 1).expect("test index fits in u64"),
                SegmentInvalidationProducerIdentity::Replication,
            );
        }
        assert_eq!(
            overflow.replay_entries().expect("overflow should encode as whole bucket")[0].scope,
            ScannerDurableDirtyUsageReplayScope::WholeBucket
        );
    }

    #[test]
    fn journal_state_hydrates_replayed_records_before_later_flushes() {
        let record = ScannerDurableDirtyUsageReplayRecord {
            schema: 1,
            cache_key_format: 1,
            writer_epoch: "writer".to_string(),
            entries: vec![ScannerDurableDirtyUsageReplayEntry {
                bucket: "photos".to_string(),
                generation: 7,
                scope: ScannerDurableDirtyUsageReplayScope::TopLevelEntries {
                    entries: BTreeSet::from(["2026".to_string()]),
                },
                producers: BTreeSet::from([SegmentInvalidationProducerIdentity::Replication]),
            }],
        };
        let mut state = JournalState::from_replay_record(record).expect("valid replay record should hydrate");
        state.record_mutation(
            "videos".to_string(),
            "clips/object",
            8,
            SegmentInvalidationProducerIdentity::TierExpiration,
        );

        let entries = state.replay_entries().expect("hydrated state should remain encodable");
        assert_eq!(entries.len(), 2);
        assert!(entries.iter().any(|entry| entry.bucket == "photos" && entry.generation == 7));
        assert!(entries.iter().any(|entry| entry.bucket == "videos" && entry.generation == 8));
    }

    #[test]
    fn journal_state_hydration_preserves_replayed_bucket_scope_and_producers() {
        let record = ScannerDurableDirtyUsageReplayRecord {
            schema: 1,
            cache_key_format: 1,
            writer_epoch: "writer".to_string(),
            entries: vec![ScannerDurableDirtyUsageReplayEntry {
                bucket: "photos".to_string(),
                generation: 7,
                scope: ScannerDurableDirtyUsageReplayScope::WholeBucket,
                producers: BTreeSet::from([SegmentInvalidationProducerIdentity::Replication]),
            }],
        };
        let mut state = JournalState::from_replay_record(record).expect("valid replay record should hydrate");
        state.record_mutation(
            "photos".to_string(),
            "2026/object",
            8,
            SegmentInvalidationProducerIdentity::TierExpiration,
        );

        let entries = state.replay_entries().expect("hydrated state should remain encodable");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].bucket, "photos");
        assert_eq!(entries[0].generation, 8);
        assert_eq!(entries[0].scope, ScannerDurableDirtyUsageReplayScope::WholeBucket);
        assert_eq!(
            entries[0].producers,
            BTreeSet::from([
                SegmentInvalidationProducerIdentity::Replication,
                SegmentInvalidationProducerIdentity::TierExpiration
            ])
        );
    }

    #[test]
    fn journal_state_drops_invalid_generation_or_non_production_identity_fail_closed() {
        let mut state = JournalState::default();
        state.record_mutation("photos".to_string(), "2026/object", 7, SegmentInvalidationProducerIdentity::Replication);
        state.record_mutation(
            "videos".to_string(),
            "clip/object",
            u64::MAX,
            SegmentInvalidationProducerIdentity::Replication,
        );
        assert!(state.buckets.is_empty());

        state.record_mutation("photos".to_string(), "2026/object", 7, SegmentInvalidationProducerIdentity::Unknown);
        assert!(state.buckets.is_empty());
    }
}
