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
/// process-wide dirty-usage invalidation state, its acknowledgment protocol, and snapshot helpers.
use super::*;
use std::collections::BTreeSet;
use std::sync::RwLock as StdRwLock;

pub(super) static DIRTY_USAGE_BUCKET_GENERATION: AtomicU64 = AtomicU64::new(0);
pub(super) static DIRTY_USAGE_BUCKETS: LazyLock<StdMutex<DirtyUsageBuckets>> = LazyLock::new(|| StdMutex::new(HashMap::new()));
// Lock order when dirty usage state is updated is `DIRTY_USAGE_BUCKETS`,
// `DIRTY_USAGE_BUCKET_SCOPES`, then `DIRTY_USAGE_PRODUCER_IDENTITIES`. All
// guards are held only for synchronous map updates, so no scanner task can
// observe a bucket generation without its matching scope and producer evidence.
pub(super) static DIRTY_USAGE_BUCKET_SCOPES: LazyLock<StdMutex<DirtyUsageBucketScopes>> =
    LazyLock::new(|| StdMutex::new(HashMap::new()));
// Non-authoritative process-local producer coverage. Segment reuse binds this
// per-bucket suffix to an earlier durable proof from the same process epoch.
pub(super) static DIRTY_USAGE_PRODUCER_IDENTITIES: LazyLock<StdMutex<DirtyUsageProducerIdentities>> =
    LazyLock::new(|| StdMutex::new(BTreeMap::new()));
static DIRTY_USAGE_CLEAR_OBSERVER: LazyLock<StdRwLock<Option<ScannerDirtyUsageClearObserver>>> =
    LazyLock::new(|| StdRwLock::new(None));
pub(super) static DIRTY_USAGE_PRODUCER_COVERAGE: AtomicU64 = AtomicU64::new(0);
pub(super) static DIRTY_USAGE_BUCKET_NOTIFY: LazyLock<Notify> = LazyLock::new(Notify::new);
pub(super) static SCANNER_ACTIVITY_EPOCH: LazyLock<String> = LazyLock::new(|| format!("{:032x}", rand::random::<u128>()));
pub(super) static SCANNER_MAINTENANCE_GENERATION: AtomicU64 = AtomicU64::new(0);
pub(super) static SCANNER_MAINTENANCE_NOTIFY: LazyLock<Notify> = LazyLock::new(Notify::new);
const SCANNER_DURABLE_DIRTY_USAGE_REPLAY_SCHEMA: u16 = 1;
const SCANNER_DURABLE_DIRTY_USAGE_REPLAY_MAX_BYTES: usize = 64 * 1024;
const SCANNER_DURABLE_DIRTY_USAGE_REPLAY_MAX_ENTRIES: usize = 1024;
const SCANNER_DURABLE_DIRTY_USAGE_REPLAY_MAX_TOP_LEVEL_ENTRIES: usize = 128;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ScannerDirtyUsageState {
    pub generation: u64,
    pub pending: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScannerDirtyUsageBucket {
    pub bucket: String,
    pub generation: u64,
}

pub type ScannerDirtyUsageClearObserver = Arc<dyn Fn(Vec<ScannerDirtyUsageBucket>) + Send + Sync + 'static>;

/// A non-durable optimization hint for a dirty bucket.
///
/// A whole-bucket marker always wins over narrow path hints. The scanner never
/// publishes a prefix-only result as authoritative usage; this only controls
/// whether a complete per-bucket cache can reuse known-clean direct children.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum DirtyUsageBucketScope {
    WholeBucket,
    TopLevelEntries(HashSet<String>),
}

pub(super) type DirtyUsageBucketScopes = HashMap<String, DirtyUsageBucketScope>;

pub(super) const MAX_DIRTY_USAGE_TOP_LEVEL_ENTRIES_PER_BUCKET: usize = 128;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct DirtyUsageProducerIdentityState {
    first_generation: u64,
    last_generation: u64,
    fully_identified: bool,
    durable_replayed: bool,
}

pub(super) type DirtyUsageProducerIdentities = BTreeMap<String, DirtyUsageProducerIdentityState>;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) struct DirtyUsageProducerEvidence {
    pub(super) producer_identity_coverage_complete: bool,
    pub(super) durable_producer_identity: bool,
    pub(super) restart_gap_absent: bool,
    pub(super) generation_window_bound: bool,
    pub(super) generation_start: u64,
    pub(super) generation_end: u64,
}

impl DirtyUsageProducerEvidence {
    pub(super) fn segment_invalidation_proof(self) -> Option<crate::DataUsageSegmentInvalidationProof> {
        (self.generation_window_bound && self.producer_identity_coverage_complete).then(|| {
            crate::DataUsageSegmentInvalidationProof {
                process_epoch: scanner_activity_epoch().to_string(),
                generation_start: self.generation_start,
                generation_end: self.generation_end,
                producer_identity_coverage_complete: true,
                cold_zero_walk_oracle: false,
            }
        })
    }
}

pub fn set_scanner_dirty_usage_clear_observer(
    observer: Option<ScannerDirtyUsageClearObserver>,
) -> Option<ScannerDirtyUsageClearObserver> {
    let mut slot = DIRTY_USAGE_CLEAR_OBSERVER
        .write()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    std::mem::replace(&mut *slot, observer)
}

fn notify_dirty_usage_clear(cleared: Vec<ScannerDirtyUsageBucket>) {
    if cleared.is_empty() {
        return;
    }
    let observer = DIRTY_USAGE_CLEAR_OBSERVER
        .read()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone();
    if let Some(observer) = observer {
        observer(cleared);
    }
}

/// A point-in-time view of the local dirty bucket generations.
///
/// `complete == false` is an all-or-nothing overflow signal: `buckets` is
/// empty and callers must fall back to the global dirty generation rather than
/// treating a bounded prefix as authoritative.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScannerDirtyUsageSnapshot {
    pub generation: u64,
    pub pending_bucket_count: u64,
    pub complete: bool,
    pub buckets: Vec<ScannerDirtyUsageBucket>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum ScannerDirtyUsageAckError {
    #[error("scanner process instance changed before dirty usage acknowledgement")]
    ProcessChanged,
    #[error("scanner dirty usage generation cannot be acknowledged")]
    InvalidGeneration,
    #[error("scanner dirty usage bucket incarnation fence is unavailable")]
    IncarnationUnavailable,
}

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum ScannerDurableDirtyUsageReplayError {
    #[error("scanner durable dirty usage replay record exceeds size limit")]
    ByteLimit,
    #[error("scanner durable dirty usage replay record cannot be decoded")]
    InvalidJson,
    #[error("scanner durable dirty usage replay schema is unsupported")]
    UnsupportedSchema,
    #[error("scanner durable dirty usage replay record is empty or over entry limit")]
    EntryLimit,
    #[error("scanner durable dirty usage replay record contains invalid generation")]
    InvalidGeneration,
    #[error("scanner durable dirty usage replay record contains an invalid bucket or scope")]
    InvalidScope,
    #[error("scanner durable dirty usage replay record contains an invalid producer identity")]
    InvalidProducer,
    #[error("scanner durable dirty usage replay record contains duplicate buckets")]
    DuplicateBucket,
    #[error("scanner durable dirty usage replay record belongs to a different cache key format")]
    MixedVersion,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScannerDurableDirtyUsageReplayRecord {
    pub schema: u16,
    pub cache_key_format: u16,
    pub writer_epoch: String,
    pub entries: Vec<ScannerDurableDirtyUsageReplayEntry>,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScannerDurableDirtyUsageReplayEntry {
    pub bucket: String,
    pub generation: u64,
    pub scope: ScannerDurableDirtyUsageReplayScope,
    pub producers: BTreeSet<crate::segment_invalidation::SegmentInvalidationProducerIdentity>,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ScannerDurableDirtyUsageReplayScope {
    WholeBucket,
    TopLevelEntries { entries: BTreeSet<String> },
}

pub fn encode_durable_dirty_usage_producer_replay_record(
    entries: Vec<ScannerDurableDirtyUsageReplayEntry>,
) -> std::result::Result<Vec<u8>, ScannerDurableDirtyUsageReplayError> {
    let record = ScannerDurableDirtyUsageReplayRecord {
        schema: SCANNER_DURABLE_DIRTY_USAGE_REPLAY_SCHEMA,
        cache_key_format: crate::DATA_USAGE_CACHE_KEY_FORMAT,
        writer_epoch: scanner_activity_epoch().to_string(),
        entries,
    };
    validate_durable_dirty_usage_replay_record(&record)?;
    serde_json::to_vec(&record).map_err(|_| ScannerDurableDirtyUsageReplayError::InvalidJson)
}

pub fn replay_durable_dirty_usage_producer_record(
    bytes: &[u8],
) -> std::result::Result<ScannerDirtyUsageState, ScannerDurableDirtyUsageReplayError> {
    if bytes.len() > SCANNER_DURABLE_DIRTY_USAGE_REPLAY_MAX_BYTES {
        return Err(ScannerDurableDirtyUsageReplayError::ByteLimit);
    }
    let record: ScannerDurableDirtyUsageReplayRecord =
        serde_json::from_slice(bytes).map_err(|_| ScannerDurableDirtyUsageReplayError::InvalidJson)?;
    let replay = validate_durable_dirty_usage_replay_record(&record)?;
    let max_generation = replay.iter().map(|entry| entry.generation).max().unwrap_or(0);
    set_dirty_usage_generation_floor(max_generation)?;
    let pending = {
        let mut dirty_buckets = dirty_usage_buckets();
        let mut dirty_scopes = dirty_usage_bucket_scopes();
        let mut producer_identities = dirty_usage_producer_identities();
        let mut coverage = 0_u64;
        for entry in replay {
            dirty_buckets.insert(entry.bucket.clone(), entry.generation);
            dirty_scopes.insert(entry.bucket.clone(), entry.scope);
            coverage |= entry.coverage;
            producer_identities.insert(
                entry.bucket,
                DirtyUsageProducerIdentityState {
                    first_generation: entry.generation,
                    last_generation: entry.generation,
                    fully_identified: true,
                    durable_replayed: true,
                },
            );
        }
        DIRTY_USAGE_PRODUCER_COVERAGE.fetch_or(coverage, Ordering::AcqRel);
        dirty_buckets.len()
    };
    global_metrics().record_scanner_dirty_usage_pending(usize_to_u64_saturated(pending));
    DIRTY_USAGE_BUCKET_NOTIFY.notify_one();
    Ok(scanner_dirty_usage_state())
}

#[derive(Debug)]
struct ValidatedDurableDirtyUsageReplayEntry {
    bucket: String,
    generation: u64,
    scope: DirtyUsageBucketScope,
    coverage: u64,
}

fn validate_durable_dirty_usage_replay_record(
    record: &ScannerDurableDirtyUsageReplayRecord,
) -> std::result::Result<Vec<ValidatedDurableDirtyUsageReplayEntry>, ScannerDurableDirtyUsageReplayError> {
    if record.schema != SCANNER_DURABLE_DIRTY_USAGE_REPLAY_SCHEMA {
        return Err(ScannerDurableDirtyUsageReplayError::UnsupportedSchema);
    }
    if record.cache_key_format != crate::DATA_USAGE_CACHE_KEY_FORMAT {
        return Err(ScannerDurableDirtyUsageReplayError::MixedVersion);
    }
    if record.writer_epoch.is_empty() || record.writer_epoch.contains('\0') {
        return Err(ScannerDurableDirtyUsageReplayError::InvalidScope);
    }
    if record.entries.is_empty() || record.entries.len() > SCANNER_DURABLE_DIRTY_USAGE_REPLAY_MAX_ENTRIES {
        return Err(ScannerDurableDirtyUsageReplayError::EntryLimit);
    }
    let mut buckets = HashSet::with_capacity(record.entries.len());
    let mut replay = Vec::with_capacity(record.entries.len());
    for entry in &record.entries {
        if !valid_bucket_for_durable_dirty_usage_replay(&entry.bucket) {
            return Err(ScannerDurableDirtyUsageReplayError::InvalidScope);
        }
        if !buckets.insert(entry.bucket.clone()) {
            return Err(ScannerDurableDirtyUsageReplayError::DuplicateBucket);
        }
        if entry.generation == 0 || entry.generation == u64::MAX {
            return Err(ScannerDurableDirtyUsageReplayError::InvalidGeneration);
        }
        let scope = validate_durable_dirty_usage_replay_scope(&entry.scope)?;
        let mut coverage = 0_u64;
        for producer in &entry.producers {
            let Some(bit) = producer.production_coverage_bit() else {
                return Err(ScannerDurableDirtyUsageReplayError::InvalidProducer);
            };
            coverage |= bit;
        }
        if coverage == 0 {
            return Err(ScannerDurableDirtyUsageReplayError::InvalidProducer);
        }
        replay.push(ValidatedDurableDirtyUsageReplayEntry {
            bucket: entry.bucket.clone(),
            generation: entry.generation,
            scope,
            coverage,
        });
    }
    Ok(replay)
}

fn valid_bucket_for_durable_dirty_usage_replay(bucket: &str) -> bool {
    !bucket.is_empty() && !bucket.contains(['/', '\\', '\0']) && bucket != "." && bucket != ".."
}

fn validate_durable_dirty_usage_replay_scope(
    scope: &ScannerDurableDirtyUsageReplayScope,
) -> std::result::Result<DirtyUsageBucketScope, ScannerDurableDirtyUsageReplayError> {
    match scope {
        ScannerDurableDirtyUsageReplayScope::WholeBucket => Ok(DirtyUsageBucketScope::WholeBucket),
        ScannerDurableDirtyUsageReplayScope::TopLevelEntries { entries } => {
            if entries.is_empty() || entries.len() > SCANNER_DURABLE_DIRTY_USAGE_REPLAY_MAX_TOP_LEVEL_ENTRIES {
                return Err(ScannerDurableDirtyUsageReplayError::InvalidScope);
            }
            let mut validated = HashSet::with_capacity(entries.len());
            for entry in entries {
                if dirty_usage_top_level_entry(entry).as_deref() != Some(entry.as_str()) {
                    return Err(ScannerDurableDirtyUsageReplayError::InvalidScope);
                }
                validated.insert(entry.clone());
            }
            Ok(DirtyUsageBucketScope::TopLevelEntries(validated))
        }
    }
}

/// A scoped ACK requires storage-owned lifecycle and incarnation fences.
/// Callers must only send ACKs backed by durable per-bucket publication.
pub fn acknowledge_scoped_dirty_usage(
    instance_id: &str,
    entries: &[(&crate::storage_api::EcstoreBucketMetadataMutationGuard, u64)],
    probe_only: bool,
) -> std::result::Result<u64, ScannerDirtyUsageAckError> {
    // Lock order: sorted bucket lifecycle/metadata fences (caller), then dirty map.
    // No await or storage operation occurs while the dirty map is locked.
    let (cleared, pending, cleared_buckets) = {
        let mut dirty = dirty_usage_buckets();
        let mut dirty_scopes = dirty_usage_bucket_scopes();
        let mut producer_identities = dirty_usage_producer_identities();
        let checked = entries
            .iter()
            .map(|(guard, generation)| {
                guard
                    .checked_bucket_incarnation()
                    .map(|(bucket, _)| (bucket, *generation))
                    .map_err(|_| ScannerDirtyUsageAckError::IncarnationUnavailable)
            })
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let cleared_buckets = if probe_only {
            Vec::new()
        } else {
            checked
                .iter()
                .filter(|(bucket, generation)| dirty.get(*bucket) == Some(generation))
                .map(|(bucket, generation)| ScannerDirtyUsageBucket {
                    bucket: (*bucket).to_string(),
                    generation: *generation,
                })
                .collect::<Vec<_>>()
        };
        let cleared = apply_scoped_dirty_usage_ack(
            instance_id,
            scanner_activity_epoch(),
            DIRTY_USAGE_BUCKET_GENERATION.load(Ordering::Acquire),
            &mut dirty,
            &mut dirty_scopes,
            &checked,
            probe_only,
        )?;
        if cleared > 0 {
            producer_identities.retain(|bucket, _| dirty.contains_key(bucket));
            advance_generation(&DIRTY_USAGE_BUCKET_GENERATION);
        }
        (cleared, dirty.len(), cleared_buckets)
    };
    notify_dirty_usage_clear(cleared_buckets);
    if !probe_only {
        global_metrics().record_scanner_dirty_usage_cycle_clear(usize_to_u64_saturated(cleared), usize_to_u64_saturated(pending));
    }
    Ok(usize_to_u64_saturated(cleared))
}

fn apply_scoped_dirty_usage_ack(
    instance_id: &str,
    current_instance: &str,
    current_generation: u64,
    dirty: &mut DirtyUsageBuckets,
    dirty_scopes: &mut DirtyUsageBucketScopes,
    entries: &[(&str, u64)],
    probe_only: bool,
) -> std::result::Result<usize, ScannerDirtyUsageAckError> {
    if instance_id != current_instance {
        return Err(ScannerDirtyUsageAckError::ProcessChanged);
    }
    if current_generation == u64::MAX
        || entries
            .iter()
            .any(|(_, generation)| *generation == 0 || *generation == u64::MAX || *generation > current_generation)
    {
        return Err(ScannerDirtyUsageAckError::InvalidGeneration);
    }
    let mut cleared = 0;
    if !probe_only {
        for (bucket, generation) in entries {
            if dirty.get(*bucket) == Some(generation) {
                dirty.remove(*bucket);
                dirty_scopes.remove(*bucket);
                cleared += 1;
            }
        }
    }
    Ok(cleared)
}

#[cfg(test)]
mod scoped_dirty_usage_tests {
    use super::*;
    use crate::segment_invalidation::SegmentInvalidationProducerIdentity;
    use serial_test::serial;

    #[test]
    fn scoped_dirty_usage_preserves_uncovered_newer_and_replayed_generations() {
        let mut dirty = HashMap::from([("hot".to_string(), 7), ("cold".to_string(), 8)]);
        let mut scopes = HashMap::from([
            ("hot".to_string(), DirtyUsageBucketScope::WholeBucket),
            (
                "cold".to_string(),
                DirtyUsageBucketScope::TopLevelEntries(HashSet::from(["first".to_string()])),
            ),
        ]);
        assert_eq!(
            apply_scoped_dirty_usage_ack("p", "p", 8, &mut dirty, &mut scopes, &[("cold", 8)], true),
            Ok(0)
        );
        assert_eq!(dirty.len(), 2);
        assert!(scopes.contains_key("cold"));
        assert_eq!(
            apply_scoped_dirty_usage_ack("p", "p", 8, &mut dirty, &mut scopes, &[("cold", 8)], false),
            Ok(1)
        );
        assert_eq!(dirty.get("hot"), Some(&7));
        assert!(!scopes.contains_key("cold"));
        assert_eq!(
            apply_scoped_dirty_usage_ack("p", "p", 8, &mut dirty, &mut scopes, &[("cold", 8)], false),
            Ok(0)
        );
        dirty.insert("cold".to_string(), 9);
        scopes.insert("cold".to_string(), DirtyUsageBucketScope::WholeBucket);
        assert_eq!(
            apply_scoped_dirty_usage_ack("p", "p", 9, &mut dirty, &mut scopes, &[("cold", 8)], false),
            Ok(0)
        );
        assert_eq!(dirty.get("cold"), Some(&9));
        assert!(scopes.contains_key("cold"));
    }

    #[test]
    fn scoped_dirty_usage_rejects_restart_and_invalid_batch_before_clearing() {
        let original = HashMap::from([("hot".to_string(), 7), ("cold".to_string(), 8)]);
        let mut dirty = original.clone();
        let original_scopes = HashMap::from([
            ("hot".to_string(), DirtyUsageBucketScope::WholeBucket),
            ("cold".to_string(), DirtyUsageBucketScope::WholeBucket),
        ]);
        let mut scopes = original_scopes.clone();
        assert_eq!(
            apply_scoped_dirty_usage_ack("old", "new", 8, &mut dirty, &mut scopes, &[("cold", 8)], false),
            Err(ScannerDirtyUsageAckError::ProcessChanged)
        );
        assert_eq!(scopes, original_scopes);
        for generation in [0, 9, u64::MAX] {
            assert_eq!(
                apply_scoped_dirty_usage_ack("p", "p", 8, &mut dirty, &mut scopes, &[("cold", 8), ("hot", generation)], false,),
                Err(ScannerDirtyUsageAckError::InvalidGeneration)
            );
            assert_eq!(dirty, original);
            assert_eq!(scopes, original_scopes);
        }
    }

    #[test]
    #[serial]
    fn dirty_usage_tracks_known_segment_producer_identities_without_authorizing_unknown_sources() {
        clear_dirty_usage_buckets_for_tests();
        record_dirty_usage_object_from_producer("photos", "hot/object", SegmentInvalidationProducerIdentity::PutObject);
        record_dirty_usage_object_from_producer("photos", "archive/object", SegmentInvalidationProducerIdentity::DeleteObject);
        record_dirty_usage_bucket_from_producer("photos", SegmentInvalidationProducerIdentity::Unknown);
        record_dirty_usage_bucket_from_producers(
            "photos",
            [
                SegmentInvalidationProducerIdentity::DeleteMarker,
                SegmentInvalidationProducerIdentity::AbortMultipartUpload,
            ],
        );

        assert_eq!(
            dirty_usage_producer_identities_for_tests(),
            BTreeSet::from([
                SegmentInvalidationProducerIdentity::PutObject,
                SegmentInvalidationProducerIdentity::DeleteObject,
                SegmentInvalidationProducerIdentity::DeleteMarker,
                SegmentInvalidationProducerIdentity::AbortMultipartUpload
            ])
        );
        assert_eq!(
            dirty_usage_bucket_scopes_for_tests().get("photos"),
            Some(&DirtyUsageBucketScope::WholeBucket),
            "an unknown producer keeps the bucket dirty but must not count as producer coverage"
        );
        let snapshot = snapshot_dirty_usage_buckets(
            &[BucketInfo {
                name: "photos".to_string(),
                created: None,
                deleted: None,
                versioning: false,
                object_locking: false,
            }],
            dirty_usage_generation(),
        );
        let evidence = dirty_usage_producer_evidence(&snapshot);
        assert!(!evidence.producer_identity_coverage_complete);
        assert!(!evidence.generation_window_bound);

        clear_dirty_usage_buckets_for_tests();
        assert!(dirty_usage_producer_identities_for_tests().is_empty());
    }

    #[test]
    #[serial]
    fn durable_dirty_usage_replay_restores_producer_identity_authority() {
        clear_dirty_usage_buckets_for_tests();
        let bytes = encode_durable_dirty_usage_producer_replay_record(vec![ScannerDurableDirtyUsageReplayEntry {
            bucket: "photos".to_string(),
            generation: 7,
            scope: ScannerDurableDirtyUsageReplayScope::TopLevelEntries {
                entries: BTreeSet::from(["2026".to_string(), "archive".to_string()]),
            },
            producers: SegmentInvalidationProducerIdentity::REQUIRED_PRODUCTION.into_iter().collect(),
        }])
        .expect("durable replay record should encode");

        let state = replay_durable_dirty_usage_producer_record(&bytes).expect("durable replay should be accepted");
        assert!(state.pending);
        assert!(state.generation >= 7);
        assert_eq!(
            dirty_usage_bucket_scopes_for_tests().get("photos"),
            Some(&DirtyUsageBucketScope::TopLevelEntries(HashSet::from([
                "2026".to_string(),
                "archive".to_string()
            ])))
        );

        let snapshot = snapshot_dirty_usage_buckets(
            &[BucketInfo {
                name: "photos".to_string(),
                created: None,
                deleted: None,
                versioning: false,
                object_locking: false,
            }],
            dirty_usage_generation(),
        );
        let evidence = dirty_usage_producer_evidence(&snapshot);
        assert!(evidence.producer_identity_coverage_complete);
        assert!(evidence.durable_producer_identity);
        assert!(evidence.restart_gap_absent);
        assert_eq!(evidence.generation_start, 7);
        assert_eq!(evidence.generation_end, 7);

        record_dirty_usage_object_from_producer("photos", "new/object", SegmentInvalidationProducerIdentity::PutObject);
        let changed = snapshot_dirty_usage_buckets(
            &[BucketInfo {
                name: "photos".to_string(),
                created: None,
                deleted: None,
                versioning: false,
                object_locking: false,
            }],
            dirty_usage_generation(),
        );
        let changed_evidence = dirty_usage_producer_evidence(&changed);
        assert!(!changed_evidence.durable_producer_identity);
        assert!(!changed_evidence.restart_gap_absent);
        clear_dirty_usage_buckets_for_tests();
    }

    #[test]
    #[serial]
    fn durable_dirty_usage_replay_rejects_invalid_records_without_partial_state() {
        let valid_entry = ScannerDurableDirtyUsageReplayEntry {
            bucket: "photos".to_string(),
            generation: 7,
            scope: ScannerDurableDirtyUsageReplayScope::WholeBucket,
            producers: BTreeSet::from([SegmentInvalidationProducerIdentity::PutObject]),
        };
        let valid_record = || ScannerDurableDirtyUsageReplayRecord {
            schema: SCANNER_DURABLE_DIRTY_USAGE_REPLAY_SCHEMA,
            cache_key_format: crate::DATA_USAGE_CACHE_KEY_FORMAT,
            writer_epoch: "writer".to_string(),
            entries: vec![valid_entry.clone()],
        };

        let invalid_cases = [
            {
                let mut record = valid_record();
                record.schema = SCANNER_DURABLE_DIRTY_USAGE_REPLAY_SCHEMA + 1;
                (
                    serde_json::to_vec(&record).expect("invalid schema record should encode"),
                    ScannerDurableDirtyUsageReplayError::UnsupportedSchema,
                )
            },
            {
                let mut record = valid_record();
                record.cache_key_format = crate::DATA_USAGE_CACHE_KEY_FORMAT + 1;
                (
                    serde_json::to_vec(&record).expect("mixed key format record should encode"),
                    ScannerDurableDirtyUsageReplayError::MixedVersion,
                )
            },
            {
                let mut record = valid_record();
                record.entries[0].producers = BTreeSet::from([SegmentInvalidationProducerIdentity::Unknown]);
                (
                    serde_json::to_vec(&record).expect("unknown producer record should encode"),
                    ScannerDurableDirtyUsageReplayError::InvalidProducer,
                )
            },
            {
                let mut record = valid_record();
                record.entries.push(valid_entry.clone());
                (
                    serde_json::to_vec(&record).expect("duplicate bucket record should encode"),
                    ScannerDurableDirtyUsageReplayError::DuplicateBucket,
                )
            },
            {
                let mut record = valid_record();
                record.entries[0].scope = ScannerDurableDirtyUsageReplayScope::TopLevelEntries {
                    entries: BTreeSet::from(["bad/child".to_string()]),
                };
                (
                    serde_json::to_vec(&record).expect("invalid scope record should encode"),
                    ScannerDurableDirtyUsageReplayError::InvalidScope,
                )
            },
            {
                let mut record = valid_record();
                record.entries[0].generation = u64::MAX;
                (
                    serde_json::to_vec(&record).expect("invalid generation record should encode"),
                    ScannerDurableDirtyUsageReplayError::InvalidGeneration,
                )
            },
        ];

        for (bytes, expected_error) in invalid_cases {
            clear_dirty_usage_buckets_for_tests();
            assert_eq!(replay_durable_dirty_usage_producer_record(&bytes), Err(expected_error));
            assert!(dirty_usage_buckets_for_tests().is_empty());
            assert!(dirty_usage_bucket_scopes_for_tests().is_empty());
        }

        clear_dirty_usage_buckets_for_tests();
        let oversized = vec![b' '; SCANNER_DURABLE_DIRTY_USAGE_REPLAY_MAX_BYTES + 1];
        assert_eq!(
            replay_durable_dirty_usage_producer_record(&oversized),
            Err(ScannerDurableDirtyUsageReplayError::ByteLimit)
        );
        assert!(dirty_usage_buckets_for_tests().is_empty());
    }
}

pub(super) fn dirty_usage_buckets() -> MutexGuard<'static, DirtyUsageBuckets> {
    DIRTY_USAGE_BUCKETS.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn dirty_usage_bucket_scopes() -> MutexGuard<'static, DirtyUsageBucketScopes> {
    DIRTY_USAGE_BUCKET_SCOPES
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn dirty_usage_producer_identities() -> MutexGuard<'static, DirtyUsageProducerIdentities> {
    DIRTY_USAGE_PRODUCER_IDENTITIES
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

pub(super) fn usize_to_u64_saturated(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

pub(super) fn advance_generation(generation: &AtomicU64) -> u64 {
    generation
        .try_update(Ordering::AcqRel, Ordering::Acquire, |current| Some(current.saturating_add(1)))
        .map_or_else(|current| current, |previous| previous.saturating_add(1))
}

fn set_dirty_usage_generation_floor(floor: u64) -> std::result::Result<(), ScannerDurableDirtyUsageReplayError> {
    if floor == 0 || floor == u64::MAX {
        return Err(ScannerDurableDirtyUsageReplayError::InvalidGeneration);
    }
    DIRTY_USAGE_BUCKET_GENERATION
        .try_update(Ordering::AcqRel, Ordering::Acquire, |current| {
            (current != u64::MAX).then_some(current.max(floor))
        })
        .map(|_| ())
        .map_err(|_| ScannerDurableDirtyUsageReplayError::InvalidGeneration)
}

pub fn record_dirty_usage_bucket(bucket: &str) {
    if bucket.is_empty() {
        return;
    }

    record_dirty_usage_bucket_inner(bucket, std::iter::empty());
}

pub fn record_dirty_usage_bucket_from_producer(
    bucket: &str,
    producer: crate::segment_invalidation::SegmentInvalidationProducerIdentity,
) {
    if bucket.is_empty() {
        return;
    }

    record_dirty_usage_bucket_inner(bucket, [producer]);
}

pub fn record_dirty_usage_bucket_from_producers<I>(bucket: &str, producers: I)
where
    I: IntoIterator<Item = crate::segment_invalidation::SegmentInvalidationProducerIdentity>,
{
    if bucket.is_empty() {
        return;
    }

    record_dirty_usage_bucket_inner(bucket, producers);
}

fn record_dirty_usage_bucket_inner<I>(bucket: &str, producers: I)
where
    I: IntoIterator<Item = crate::segment_invalidation::SegmentInvalidationProducerIdentity>,
{
    let pending_buckets = {
        let mut dirty_buckets = dirty_usage_buckets();
        let mut dirty_scopes = dirty_usage_bucket_scopes();
        let mut producer_identities = dirty_usage_producer_identities();
        let generation = advance_generation(&DIRTY_USAGE_BUCKET_GENERATION);
        dirty_buckets.insert(bucket.to_string(), generation);
        dirty_scopes.insert(bucket.to_string(), DirtyUsageBucketScope::WholeBucket);
        record_segment_invalidation_producer_identities_for_generation(&mut producer_identities, bucket, generation, producers);
        dirty_buckets.len()
    };
    global_metrics().record_scanner_dirty_usage_pending(usize_to_u64_saturated(pending_buckets));
    // A write invalidates this bucket's prefix-usage answers on the spot so
    // admin/console consumers never ride the full TTL after a change
    // (rustfs/backlog#1872).
    crate::prefix_usage::invalidate_prefix_usage_cache(bucket);
    DIRTY_USAGE_BUCKET_NOTIFY.notify_one();
}

/// Record a mutation whose affected top-level namespace entry is known.
///
/// Object names that cannot be represented as one safe direct child retain the
/// conservative whole-bucket marker. This journal is intentionally process
/// local: after restart or any unverified distributed path the scanner falls
/// back to its ordinary bucket scan.
pub fn record_dirty_usage_object(bucket: &str, object: &str) {
    record_dirty_usage_object_inner(bucket, object, std::iter::empty());
}

pub fn record_dirty_usage_object_from_producer(
    bucket: &str,
    object: &str,
    producer: crate::segment_invalidation::SegmentInvalidationProducerIdentity,
) {
    if bucket.is_empty() {
        return;
    }

    record_dirty_usage_object_inner(bucket, object, [producer]);
}

fn record_dirty_usage_object_inner<I>(bucket: &str, object: &str, producers: I)
where
    I: IntoIterator<Item = crate::segment_invalidation::SegmentInvalidationProducerIdentity>,
{
    let producers = producers.into_iter().collect::<Vec<_>>();
    let Some(top_level_entry) = dirty_usage_top_level_entry(object) else {
        record_dirty_usage_bucket_inner(bucket, producers);
        return;
    };
    if bucket.is_empty() {
        return;
    }

    let pending_buckets = {
        let mut dirty_buckets = dirty_usage_buckets();
        let mut dirty_scopes = dirty_usage_bucket_scopes();
        let mut producer_identities = dirty_usage_producer_identities();
        let generation = advance_generation(&DIRTY_USAGE_BUCKET_GENERATION);
        dirty_buckets.insert(bucket.to_string(), generation);
        let scope = dirty_scopes
            .entry(bucket.to_string())
            .or_insert_with(|| DirtyUsageBucketScope::TopLevelEntries(HashSet::new()));
        let overflowed = match scope {
            DirtyUsageBucketScope::WholeBucket => false,
            DirtyUsageBucketScope::TopLevelEntries(entries) => {
                entries.insert(top_level_entry);
                entries.len() > MAX_DIRTY_USAGE_TOP_LEVEL_ENTRIES_PER_BUCKET
            }
        };
        if overflowed {
            *scope = DirtyUsageBucketScope::WholeBucket;
        }
        record_segment_invalidation_producer_identities_for_generation(&mut producer_identities, bucket, generation, producers);
        dirty_buckets.len()
    };
    global_metrics().record_scanner_dirty_usage_pending(usize_to_u64_saturated(pending_buckets));
    crate::prefix_usage::invalidate_prefix_usage_cache(bucket);
    DIRTY_USAGE_BUCKET_NOTIFY.notify_one();
}

fn record_segment_invalidation_producer_identities_for_generation<I>(
    identities: &mut DirtyUsageProducerIdentities,
    bucket: &str,
    generation: u64,
    producers: I,
) where
    I: IntoIterator<Item = crate::segment_invalidation::SegmentInvalidationProducerIdentity>,
{
    let mut event_coverage = 0_u64;
    let mut fully_identified = true;
    for producer in producers {
        if let Some(coverage_bit) = producer.production_coverage_bit() {
            event_coverage |= coverage_bit;
        } else {
            fully_identified = false;
        }
    }
    fully_identified &= event_coverage != 0;
    DIRTY_USAGE_PRODUCER_COVERAGE.fetch_or(event_coverage, Ordering::AcqRel);

    match identities.get_mut(bucket) {
        Some(state) => {
            state.last_generation = state.last_generation.max(generation);
            state.fully_identified &= fully_identified;
            state.durable_replayed = false;
        }
        None => {
            identities.insert(
                bucket.to_string(),
                DirtyUsageProducerIdentityState {
                    first_generation: generation,
                    last_generation: generation,
                    fully_identified,
                    durable_replayed: false,
                },
            );
        }
    }
}

#[cfg(test)]
fn dirty_usage_producer_identities_for_tests() -> BTreeSet<crate::segment_invalidation::SegmentInvalidationProducerIdentity> {
    let coverage = DIRTY_USAGE_PRODUCER_COVERAGE.load(Ordering::Acquire);
    crate::segment_invalidation::SegmentInvalidationProducerIdentity::REQUIRED_PRODUCTION
        .into_iter()
        .filter(|identity| identity.production_coverage_bit().is_some_and(|bit| coverage & bit != 0))
        .collect()
}

fn dirty_usage_top_level_entry(object: &str) -> Option<String> {
    let (top_level_entry, _) = object.split_once('/').unwrap_or((object, ""));
    (!top_level_entry.is_empty()
        && top_level_entry != "."
        && top_level_entry != ".."
        && !object.starts_with('/')
        && !top_level_entry.contains(['\\', '\0']))
    .then(|| top_level_entry.to_string())
}

pub fn record_scanner_maintenance_change(bucket: &str) {
    if bucket.is_empty() {
        return;
    }

    advance_generation(&SCANNER_MAINTENANCE_GENERATION);
    SCANNER_MAINTENANCE_NOTIFY.notify_one();
    record_dirty_usage_bucket(bucket);
}

pub fn scanner_maintenance_generation() -> u64 {
    SCANNER_MAINTENANCE_GENERATION.load(Ordering::Acquire)
}

pub(crate) async fn scanner_maintenance_changed() {
    SCANNER_MAINTENANCE_NOTIFY.notified().await;
}

pub fn scanner_activity_epoch() -> &'static str {
    SCANNER_ACTIVITY_EPOCH.as_str()
}

pub fn scanner_dirty_usage_state() -> ScannerDirtyUsageState {
    let dirty_buckets = dirty_usage_buckets();
    ScannerDirtyUsageState {
        generation: DIRTY_USAGE_BUCKET_GENERATION.load(Ordering::Acquire),
        pending: !dirty_buckets.is_empty(),
    }
}

pub fn scanner_dirty_usage_snapshot(max_entries: usize) -> ScannerDirtyUsageSnapshot {
    let (generation, pending_bucket_count, complete, mut buckets) = {
        let dirty_buckets = dirty_usage_buckets();
        let generation = DIRTY_USAGE_BUCKET_GENERATION.load(Ordering::Acquire);
        let pending_bucket_count = usize_to_u64_saturated(dirty_buckets.len());
        let complete = dirty_buckets.len() <= max_entries;
        let buckets = if complete {
            dirty_buckets
                .iter()
                .map(|(bucket, generation)| ScannerDirtyUsageBucket {
                    bucket: bucket.clone(),
                    generation: *generation,
                })
                .collect::<Vec<_>>()
        } else {
            Vec::new()
        };
        (generation, pending_bucket_count, complete, buckets)
    };
    buckets.sort_unstable_by(|left, right| left.bucket.cmp(&right.bucket));

    ScannerDirtyUsageSnapshot {
        generation,
        pending_bucket_count,
        complete,
        buckets,
    }
}

pub fn acknowledge_dirty_usage_generation(
    instance_id: &str,
    generation: u64,
) -> std::result::Result<(), ScannerDirtyUsageAckError> {
    if instance_id != scanner_activity_epoch() {
        return Err(ScannerDirtyUsageAckError::ProcessChanged);
    }

    let (cleared_buckets, pending_buckets, cleared) = {
        let mut dirty_buckets = dirty_usage_buckets();
        let mut dirty_scopes = dirty_usage_bucket_scopes();
        let mut producer_identities = dirty_usage_producer_identities();
        let current_generation = DIRTY_USAGE_BUCKET_GENERATION.load(Ordering::Acquire);
        if generation == 0 || generation == u64::MAX || current_generation == u64::MAX || generation > current_generation {
            return Err(ScannerDirtyUsageAckError::InvalidGeneration);
        }

        let before = dirty_buckets.len();
        let cleared = dirty_buckets
            .iter()
            .filter(|(_, dirty_generation)| **dirty_generation <= generation)
            .map(|(bucket, dirty_generation)| ScannerDirtyUsageBucket {
                bucket: bucket.clone(),
                generation: *dirty_generation,
            })
            .collect::<Vec<_>>();
        dirty_buckets.retain(|_, dirty_generation| *dirty_generation > generation);
        dirty_scopes.retain(|bucket, _| dirty_buckets.contains_key(bucket));
        producer_identities.retain(|bucket, _| dirty_buckets.contains_key(bucket));
        let cleared_buckets = before.saturating_sub(dirty_buckets.len());
        if cleared_buckets > 0 {
            advance_generation(&DIRTY_USAGE_BUCKET_GENERATION);
        }
        (cleared_buckets, dirty_buckets.len(), cleared)
    };
    notify_dirty_usage_clear(cleared);
    global_metrics()
        .record_scanner_dirty_usage_cycle_clear(usize_to_u64_saturated(cleared_buckets), usize_to_u64_saturated(pending_buckets));
    Ok(())
}

pub(crate) fn dirty_usage_generation() -> u64 {
    DIRTY_USAGE_BUCKET_GENERATION.load(Ordering::Acquire)
}

pub fn clear_dirty_usage_bucket(bucket: &str) {
    if bucket.is_empty() {
        return;
    }

    let (pending_buckets, cleared) = {
        let mut dirty_buckets = dirty_usage_buckets();
        let mut dirty_scopes = dirty_usage_bucket_scopes();
        let mut producer_identities = dirty_usage_producer_identities();
        let cleared = dirty_buckets.remove(bucket).map(|generation| ScannerDirtyUsageBucket {
            bucket: bucket.to_string(),
            generation,
        });
        dirty_scopes.remove(bucket);
        producer_identities.remove(bucket);
        DIRTY_USAGE_PRODUCER_COVERAGE.store(0, Ordering::Release);
        advance_generation(&DIRTY_USAGE_BUCKET_GENERATION);
        (dirty_buckets.len(), cleared)
    };
    if let Some(cleared) = cleared {
        notify_dirty_usage_clear(vec![cleared]);
    }
    global_metrics().record_scanner_dirty_usage_clear(usize_to_u64_saturated(pending_buckets));
}

pub(super) fn snapshot_dirty_usage_buckets(buckets: &[BucketInfo], absent_generation_cutoff: u64) -> DirtyUsageSnapshot {
    let (snapshot, scopes, generation, covers_all_pending) = {
        let dirty_buckets = dirty_usage_buckets();
        let dirty_scopes = dirty_usage_bucket_scopes();
        let listed_buckets = dirty_buckets
            .values()
            .any(|generation| *generation > absent_generation_cutoff)
            .then(|| buckets.iter().map(|bucket| bucket.name.as_str()).collect::<HashSet<_>>());
        let snapshot = dirty_buckets
            .iter()
            .filter(|(bucket, generation)| {
                **generation <= absent_generation_cutoff
                    || listed_buckets
                        .as_ref()
                        .is_some_and(|listed_buckets| listed_buckets.contains(bucket.as_str()))
            })
            .map(|(bucket, generation)| (bucket.clone(), *generation))
            .collect::<DirtyUsageBuckets>();
        let scopes = snapshot
            .keys()
            .map(|bucket| {
                (
                    bucket.clone(),
                    dirty_scopes
                        .get(bucket)
                        .cloned()
                        .unwrap_or(DirtyUsageBucketScope::WholeBucket),
                )
            })
            .collect::<DirtyUsageBucketScopes>();
        let generation = DIRTY_USAGE_BUCKET_GENERATION.load(Ordering::Acquire);
        let covers_all_pending = generation == absent_generation_cutoff && snapshot.len() == dirty_buckets.len();
        (snapshot, scopes, generation, covers_all_pending)
    };
    global_metrics().record_scanner_dirty_usage_cycle_snapshot(usize_to_u64_saturated(snapshot.len()));
    DirtyUsageSnapshot {
        buckets: Arc::new(snapshot),
        scopes: Arc::new(scopes),
        generation,
        covers_all_pending,
    }
}

pub(crate) fn dirty_usage_buckets_pending() -> bool {
    !dirty_usage_buckets().is_empty()
}

pub(crate) async fn dirty_usage_bucket_notified() {
    DIRTY_USAGE_BUCKET_NOTIFY.notified().await;
}

pub(super) fn clear_dirty_usage_buckets(snapshot: &DirtyUsageBuckets) {
    let (cleared_buckets, pending_buckets, cleared) = {
        let mut dirty_buckets = dirty_usage_buckets();
        let mut dirty_scopes = dirty_usage_bucket_scopes();
        let mut producer_identities = dirty_usage_producer_identities();
        let mut cleared_buckets = 0usize;
        let mut cleared = Vec::new();
        for (bucket, generation) in snapshot {
            if dirty_buckets.get(bucket).is_some_and(|current| current == generation) {
                dirty_buckets.remove(bucket);
                dirty_scopes.remove(bucket);
                cleared_buckets += 1;
                cleared.push(ScannerDirtyUsageBucket {
                    bucket: bucket.clone(),
                    generation: *generation,
                });
            }
        }
        if cleared_buckets > 0 {
            producer_identities.retain(|bucket, _| dirty_buckets.contains_key(bucket));
            advance_generation(&DIRTY_USAGE_BUCKET_GENERATION);
        }
        (cleared_buckets, dirty_buckets.len(), cleared)
    };
    notify_dirty_usage_clear(cleared);
    global_metrics()
        .record_scanner_dirty_usage_cycle_clear(usize_to_u64_saturated(cleared_buckets), usize_to_u64_saturated(pending_buckets));
}

pub(super) fn dirty_usage_buckets_excluding_failed(
    snapshot: &DirtyUsageBuckets,
    failed_buckets: &HashSet<String>,
) -> DirtyUsageBuckets {
    snapshot
        .iter()
        .filter(|(bucket, _)| !failed_buckets.contains(*bucket))
        .map(|(bucket, generation)| (bucket.clone(), *generation))
        .collect()
}

pub(super) fn should_clear_dirty_usage_snapshot(
    result_ok: bool,
    completed_all_sets: bool,
    budget_elapsed: bool,
    activity_and_generation_current: bool,
    dirty_buckets: &DirtyUsageBuckets,
    failed_buckets: &HashSet<String>,
) -> Option<DirtyUsageBuckets> {
    if result_ok && completed_all_sets && !budget_elapsed && activity_and_generation_current {
        return Some(dirty_usage_buckets_excluding_failed(dirty_buckets, failed_buckets));
    }

    None
}

pub(super) async fn record_failed_dirty_bucket(failed_buckets: &Arc<Mutex<HashSet<String>>>, bucket: &str) {
    failed_buckets.lock().await.insert(bucket.to_string());
}

pub(super) async fn record_partial_dirty_bucket(partial_buckets: &Arc<Mutex<HashSet<String>>>, bucket: &str) {
    partial_buckets.lock().await.insert(bucket.to_string());
}

pub(super) async fn requeue_bucket_work(
    bucket_tx: &mpsc::Sender<BucketInfo>,
    bucket: &BucketInfo,
    work_guard: &mut BucketWorkGuard,
) -> bool {
    if bucket_tx.send(bucket.clone()).await.is_err() {
        return false;
    }

    work_guard.mark_requeued();
    true
}

pub(super) async fn mark_unprocessed_bucket_work_failed(
    bucket_rx: &Mutex<mpsc::Receiver<BucketInfo>>,
    remaining: &Arc<AtomicUsize>,
    complete: &CancellationToken,
    failed_buckets: &Arc<Mutex<HashSet<String>>>,
) -> usize {
    let mut failed_count = 0;
    let mut receiver = bucket_rx.lock().await;
    while let Some(bucket) = receiver.recv().await {
        record_failed_dirty_bucket(failed_buckets, &bucket.name).await;
        drop(BucketWorkGuard::new(remaining.clone(), complete.clone()));
        failed_count += 1;
    }
    failed_count
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum DirtyUsageSnapshotStatus {
    Current,
    Changed,
    Unverified,
}

pub(super) fn dirty_usage_snapshot_status(snapshot: &DirtyUsageSnapshot) -> DirtyUsageSnapshotStatus {
    let generation = DIRTY_USAGE_BUCKET_GENERATION.load(Ordering::Acquire);
    if generation == u64::MAX {
        DirtyUsageSnapshotStatus::Unverified
    } else if snapshot.covers_all_pending && generation == snapshot.generation {
        DirtyUsageSnapshotStatus::Current
    } else {
        DirtyUsageSnapshotStatus::Changed
    }
}

pub(super) fn dirty_usage_producer_evidence(snapshot: &DirtyUsageSnapshot) -> DirtyUsageProducerEvidence {
    let snapshot_current = dirty_usage_snapshot_status(snapshot) == DirtyUsageSnapshotStatus::Current
        && snapshot.generation != 0
        && snapshot.generation != u64::MAX
        && !snapshot.buckets.is_empty();
    let producer_identities = dirty_usage_producer_identities();
    let mut generation_start = u64::MAX;
    let mut generation_end = 0;
    let producer_identity_coverage_complete = snapshot_current
        && snapshot.buckets.iter().all(|(bucket, generation)| {
            producer_identities.get(bucket).is_some_and(|state| {
                generation_start = generation_start.min(state.first_generation);
                generation_end = generation_end.max(state.last_generation);
                state.fully_identified && state.last_generation == *generation
            })
        })
        && DIRTY_USAGE_PRODUCER_COVERAGE.load(Ordering::Acquire)
            & crate::segment_invalidation::SegmentInvalidationProducerIdentity::REQUIRED_PRODUCTION_COVERAGE_MASK
            == crate::segment_invalidation::SegmentInvalidationProducerIdentity::REQUIRED_PRODUCTION_COVERAGE_MASK;
    let durable_producer_identity = producer_identity_coverage_complete
        && snapshot
            .buckets
            .keys()
            .all(|bucket| producer_identities.get(bucket).is_some_and(|state| state.durable_replayed));
    let generation_window_bound = producer_identity_coverage_complete
        && generation_start != u64::MAX
        && generation_end >= generation_start
        && generation_end <= snapshot.generation;

    DirtyUsageProducerEvidence {
        producer_identity_coverage_complete,
        durable_producer_identity,
        restart_gap_absent: durable_producer_identity,
        generation_window_bound,
        generation_start: if generation_window_bound { generation_start } else { 0 },
        generation_end: if generation_window_bound { generation_end } else { 0 },
    }
}

#[cfg(test)]
pub(super) fn dirty_usage_bucket_count() -> usize {
    dirty_usage_buckets().len()
}

#[cfg(test)]
pub(crate) fn clear_dirty_usage_buckets_for_tests() {
    dirty_usage_buckets().clear();
    dirty_usage_bucket_scopes().clear();
    dirty_usage_producer_identities().clear();
    DIRTY_USAGE_PRODUCER_COVERAGE.store(0, Ordering::Release);
}

#[cfg(test)]
pub(crate) fn dirty_usage_buckets_for_tests() -> DirtyUsageBuckets {
    dirty_usage_buckets().clone()
}

#[cfg(test)]
pub(crate) fn dirty_usage_bucket_scopes_for_tests() -> DirtyUsageBucketScopes {
    dirty_usage_bucket_scopes().clone()
}
