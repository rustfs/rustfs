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
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{debug, warn};

const LOG_COMPONENT_STORAGE: &str = "storage";
const LOG_SUBSYSTEM_SCANNER: &str = "scanner";
const EVENT_SCANNER_DIRTY_USAGE_JOURNAL: &str = "scanner_dirty_usage_journal";
const DURABLE_DIRTY_USAGE_REPLAY_OBJECT: &str = "scanner/durable-dirty-producer-replay.json";
const DURABLE_DIRTY_USAGE_REPLAY_MAX_BYTES: usize = 64 * 1024;
const DURABLE_DIRTY_USAGE_REPLAY_MAX_ENTRIES: usize = 1024;
const DURABLE_DIRTY_USAGE_REPLAY_MAX_TOP_LEVEL_ENTRIES: usize = 128;
const DURABLE_DIRTY_USAGE_JOURNAL_CHANNEL_DEPTH: usize = 256;
const DURABLE_DIRTY_USAGE_JOURNAL_RETRY_INTERVAL: Duration = Duration::from_secs(5);

#[derive(Clone)]
pub(crate) struct DurableDirtyUsageJournal {
    sender: mpsc::Sender<JournalCommand>,
}

impl DurableDirtyUsageJournal {
    pub(crate) fn record_committed_mutation(&self, bucket: &str, object: &str, producer: SegmentInvalidationProducerIdentity) {
        let snapshot = rustfs_scanner::scanner_dirty_usage_snapshot(DURABLE_DIRTY_USAGE_REPLAY_MAX_ENTRIES);
        let generation = snapshot
            .buckets
            .into_iter()
            .find(|entry| entry.bucket == bucket)
            .map(|entry| entry.generation)
            .unwrap_or(0);
        self.dispatch(JournalCommand::RecordMutation {
            bucket: bucket.to_string(),
            object: object.to_string(),
            generation,
            producer,
        });
    }

    pub(crate) fn clear_confirmed_buckets(&self, cleared: Vec<ScannerDirtyUsageBucket>) {
        self.dispatch(JournalCommand::ClearBuckets { cleared });
    }

    fn dispatch(&self, command: JournalCommand) {
        match self.sender.try_send(command) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(command)) => {
                let sender = self.sender.clone();
                tokio::spawn(async move {
                    let _ = sender.send(command).await;
                });
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {}
        }
    }
}

#[derive(Debug)]
enum JournalCommand {
    RecordMutation {
        bucket: String,
        object: String,
        generation: u64,
        producer: SegmentInvalidationProducerIdentity,
    },
    ClearBuckets {
        cleared: Vec<ScannerDirtyUsageBucket>,
    },
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct JournalState {
    buckets: BTreeMap<String, JournalBucketState>,
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
            return;
        }

        let event_scope = durable_dirty_usage_journal_scope(object);
        self.buckets
            .entry(bucket)
            .and_modify(|state| {
                state.generation = state.generation.max(generation);
                merge_journal_scope(&mut state.scope, event_scope.clone());
                state.producers.insert(producer);
            })
            .or_insert_with(|| JournalBucketState {
                generation,
                scope: event_scope,
                producers: BTreeSet::from([producer]),
            });
    }

    fn clear_buckets(&mut self, cleared: &[ScannerDirtyUsageBucket]) {
        for entry in cleared {
            if self
                .buckets
                .get(&entry.bucket)
                .is_some_and(|state| state.generation <= entry.generation)
            {
                self.buckets.remove(&entry.bucket);
            }
        }
    }

    fn replay_entries(&self) -> Option<Vec<ScannerDurableDirtyUsageReplayEntry>> {
        if self.buckets.is_empty() || self.buckets.len() > DURABLE_DIRTY_USAGE_REPLAY_MAX_ENTRIES {
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
    let initial_state = replay_durable_dirty_usage_journal(store.clone()).await;
    let (sender, receiver) = mpsc::channel(DURABLE_DIRTY_USAGE_JOURNAL_CHANNEL_DEPTH);
    tokio::spawn(run_durable_dirty_usage_journal(store, receiver, initial_state));
    DurableDirtyUsageJournal { sender }
}

async fn replay_durable_dirty_usage_journal(store: Arc<ECStore>) -> JournalState {
    let bytes = match super::read_config(store, DURABLE_DIRTY_USAGE_REPLAY_OBJECT).await {
        Ok(bytes) if !bytes.is_empty() => bytes,
        Ok(_) | Err(Error::ConfigNotFound) => return JournalState::default(),
        Err(err) => {
            warn!(
                event = EVENT_SCANNER_DIRTY_USAGE_JOURNAL,
                component = LOG_COMPONENT_STORAGE,
                subsystem = LOG_SUBSYSTEM_SCANNER,
                state = "read_failed",
                error = ?err,
                "Durable scanner dirty usage journal could not be read"
            );
            return JournalState::default();
        }
    };
    if bytes.len() > DURABLE_DIRTY_USAGE_REPLAY_MAX_BYTES {
        warn!(
            event = EVENT_SCANNER_DIRTY_USAGE_JOURNAL,
            component = LOG_COMPONENT_STORAGE,
            subsystem = LOG_SUBSYSTEM_SCANNER,
            state = "oversized",
            "Durable scanner dirty usage journal exceeds replay size limit"
        );
        return JournalState::default();
    }
    match rustfs_scanner::replay_durable_dirty_usage_producer_record(&bytes) {
        Ok(state) => {
            debug!(
                event = EVENT_SCANNER_DIRTY_USAGE_JOURNAL,
                component = LOG_COMPONENT_STORAGE,
                subsystem = LOG_SUBSYSTEM_SCANNER,
                state = "replayed",
                generation = state.generation,
                pending = state.pending,
                "Durable scanner dirty usage journal replayed"
            );
            match serde_json::from_slice::<ScannerDurableDirtyUsageReplayRecord>(&bytes)
                .ok()
                .and_then(JournalState::from_replay_record)
            {
                Some(state) => state,
                None => {
                    warn!(
                        event = EVENT_SCANNER_DIRTY_USAGE_JOURNAL,
                        component = LOG_COMPONENT_STORAGE,
                        subsystem = LOG_SUBSYSTEM_SCANNER,
                        state = "hydrate_rejected",
                        "Durable scanner dirty usage journal could not hydrate writer state"
                    );
                    JournalState::default()
                }
            }
        }
        Err(err) => {
            warn!(
                event = EVENT_SCANNER_DIRTY_USAGE_JOURNAL,
                component = LOG_COMPONENT_STORAGE,
                subsystem = LOG_SUBSYSTEM_SCANNER,
                state = "replay_rejected",
                error = ?err,
                "Durable scanner dirty usage journal was rejected"
            );
            JournalState::default()
        }
    }
}

async fn run_durable_dirty_usage_journal(
    store: Arc<ECStore>,
    mut receiver: mpsc::Receiver<JournalCommand>,
    initial_state: JournalState,
) {
    let mut state = initial_state;
    let mut dirty = false;
    let mut retry = tokio::time::interval(DURABLE_DIRTY_USAGE_JOURNAL_RETRY_INTERVAL);
    retry.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            command = receiver.recv() => {
                let Some(command) = command else {
                    break;
                };
                apply_journal_command(&mut state, command);
                while let Ok(command) = receiver.try_recv() {
                    apply_journal_command(&mut state, command);
                }
                dirty = true;
            }
            _ = retry.tick(), if dirty => {}
        }

        if dirty && flush_durable_dirty_usage_journal(store.clone(), &state).await {
            dirty = false;
        }
    }
}

fn apply_journal_command(state: &mut JournalState, command: JournalCommand) {
    match command {
        JournalCommand::RecordMutation {
            bucket,
            object,
            generation,
            producer,
        } => state.record_mutation(bucket, &object, generation, producer),
        JournalCommand::ClearBuckets { cleared } => state.clear_buckets(&cleared),
    }
}

async fn flush_durable_dirty_usage_journal(store: Arc<ECStore>, state: &JournalState) -> bool {
    let Some(entries) = state.replay_entries() else {
        return delete_durable_dirty_usage_journal(store).await;
    };
    let bytes = match rustfs_scanner::encode_durable_dirty_usage_producer_replay_record(entries) {
        Ok(bytes) => bytes,
        Err(err) => {
            warn!(
                event = EVENT_SCANNER_DIRTY_USAGE_JOURNAL,
                component = LOG_COMPONENT_STORAGE,
                subsystem = LOG_SUBSYSTEM_SCANNER,
                state = "encode_rejected",
                error = ?err,
                "Durable scanner dirty usage journal could not be encoded"
            );
            return delete_durable_dirty_usage_journal(store).await;
        }
    };
    match ecstore_config::com::save_config(store, DURABLE_DIRTY_USAGE_REPLAY_OBJECT, bytes).await {
        Ok(()) => true,
        Err(err) => {
            warn!(
                event = EVENT_SCANNER_DIRTY_USAGE_JOURNAL,
                component = LOG_COMPONENT_STORAGE,
                subsystem = LOG_SUBSYSTEM_SCANNER,
                state = "write_failed",
                error = ?err,
                "Durable scanner dirty usage journal write failed"
            );
            false
        }
    }
}

async fn delete_durable_dirty_usage_journal(store: Arc<ECStore>) -> bool {
    match ecstore_config::com::delete_config(store, DURABLE_DIRTY_USAGE_REPLAY_OBJECT).await {
        Ok(()) | Err(Error::ConfigNotFound) => true,
        Err(err) => {
            warn!(
                event = EVENT_SCANNER_DIRTY_USAGE_JOURNAL,
                component = LOG_COMPONENT_STORAGE,
                subsystem = LOG_SUBSYSTEM_SCANNER,
                state = "delete_failed",
                error = ?err,
                "Durable scanner dirty usage journal delete failed"
            );
            false
        }
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
    use super::{DURABLE_DIRTY_USAGE_REPLAY_MAX_TOP_LEVEL_ENTRIES, JournalState};
    use rustfs_scanner::{
        ScannerDirtyUsageBucket, ScannerDurableDirtyUsageReplayEntry, ScannerDurableDirtyUsageReplayRecord,
        ScannerDurableDirtyUsageReplayScope, SegmentInvalidationProducerIdentity,
    };
    use std::collections::BTreeSet;

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
