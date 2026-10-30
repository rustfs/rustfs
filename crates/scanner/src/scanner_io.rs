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

use crate::data_usage_define::clone_cache_value;
use crate::data_usage_define::{DATA_USAGE_CACHE_KEY_FORMAT, DataUsageCacheRevisions};
use crate::scanner_budget::ScannerCycleBudget;
use crate::scanner_folder::{ScannerBucketPrefixScanScope, ScannerItem, scan_data_folder_scoped};
use crate::sleeper::SCANNER_SLEEPER;
use crate::{
    DATA_USAGE_CACHE_NAME, DATA_USAGE_ROOT, DataUsageCache, DataUsageCacheInfo, DataUsageCachePrepareOutcome,
    DataUsageCacheSource, DataUsageEntry, DataUsageEntryInfo, DataUsageInfo, DataUsageScanPlanDigest, DataUsageSnapshotSetState,
    ScannerError, SizeSummary, TierStats,
};
use bytes::Bytes;
use futures::future::join_all;
use metrics::counter;
use rand::seq::SliceRandom as _;
#[cfg(test)]
use rustfs_config::{ENV_SCANNER_MAX_CONCURRENT_DISK_SCANS, ENV_SCANNER_MAX_CONCURRENT_SET_SCANS, ENV_SCANNER_WALKERS_PER_DISK};
use rustfs_data_usage::{BucketTargetUsageInfo, BucketUsageInfo, observed_data_usage_is_newer};
use rustfs_filemeta::FileMeta;
use rustfs_gateway_types::persistence::PersistedLifecycleConfiguration as BucketLifecycleConfiguration;
use rustfs_gateway_types::persistence::PersistedReplicationConfiguration;
use rustfs_gateway_types::persistence::{
    PersistedObjectLockConfiguration as ObjectLockConfiguration, PersistedVersioningConfiguration as VersioningConfiguration,
};
use rustfs_heal_contracts::heal_channel::HealScanMode;
use rustfs_lock::{LockError, NamespaceLockGuard};
use rustfs_scanner_metrics::metrics::{
    Metric, Metrics, emit_scan_bucket_drive_complete, emit_scan_bucket_drive_partial, global_metrics,
};
use rustfs_utils::path::path_join_buf;
use sha2::{Digest as _, Sha256};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{LazyLock, Mutex as StdMutex, MutexGuard};
use std::time::{Instant, SystemTime};
use std::{fmt::Debug, sync::Arc};
use time::OffsetDateTime;
use tokio::sync::{Mutex, Notify, Semaphore, mpsc};
use tokio::time::Duration;
use tokio_util::sync::CancellationToken;
use tokio_util::task::AbortOnDropHandle;
use tracing::{debug, error, warn};

use crate::ScannerObjectInfo as ObjectInfo;
use crate::storage_api::EcstoreScannerPeerDirtyUsageSnapshot;
use crate::storage_api::ScannerStorage;
use crate::storage_api::scan::NamespaceLocking as _;
use crate::storage_api::scanner_io::{BucketInfo, BucketOptions};
use crate::{
    BucketTargetSys, BucketVersioningSys, Disk, DiskError, ECStore, EcstoreError as Error, EcstoreResult as Result,
    RUSTFS_META_BUCKET, ReplicationConfig, STORAGE_FORMAT_FILE, ScannerDiskExt as _, ScannerLifecycleConfigExt as _,
    ScannerReplicationConfigExt as _, ScannerVersioningConfigExt as _, SetDisks, StorageError, begin_tier_registry_cycle,
    complete_tier_registry_cycle, enqueue_runtime_free_version, get_lifecycle_config, get_object_lock_config,
    get_replication_config, runtime_tier_names, runtime_tier_registry_for_cycle, scanner_publication_admission_for_epoch,
    scanner_publication_epoch, storageclass,
};

pub(crate) const SCANNER_SKIP_FILE_ERROR: &str = "skip file";
pub(crate) const SCANNER_METADATA_CORRUPT_ERROR: &str = "scanner metadata corrupt";
pub(crate) const SCANNER_METADATA_TRANSIENT_ERROR: &str = "scanner metadata transient";
const LOG_COMPONENT_SCANNER: &str = "scanner";
const LOG_SUBSYSTEM_IO: &str = "io";
// Mirrors `scanner_folder.rs` so the versioning-lookup fallback warn keeps its
// historical `rustfs::scanner::folder` lifecycle event identity after the
// lookup moved into `get_size`.
const LOG_SUBSYSTEM_LIFECYCLE: &str = "lifecycle";
const EVENT_SCANNER_LIFECYCLE_ACTION: &str = "scanner_lifecycle_action";
const EVENT_SCANNER_DISK_BUCKET_STATE: &str = "scanner_disk_bucket_state";
const EVENT_SCANNER_DATA_USAGE_STREAM: &str = "scanner_data_usage_stream";
const EVENT_SCANNER_CACHE_PERSIST_STATE: &str = "scanner_cache_persist_state";
const EVENT_SCANNER_SET_STATE: &str = "scanner_set_state";
const SCANNER_CACHE_LOCK_SUFFIX: &str = ".scanner-cycle.lock";
const SCANNER_CACHE_LOCK_POLL_INTERVAL: Duration = Duration::from_millis(250);
#[cfg(not(test))]
const SCANNER_CACHE_LOCK_LOSS_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(30);
#[cfg(test)]
const SCANNER_CACHE_LOCK_LOSS_SHUTDOWN_TIMEOUT: Duration = Duration::from_millis(50);

const METRIC_SCANNER_SET_SCAN_CONCURRENCY_LIMIT: &str = "rustfs_scanner_set_scan_concurrency_limit";
const METRIC_SCANNER_DISK_SCAN_CONCURRENCY_LIMIT: &str = "rustfs_scanner_disk_scan_concurrency_limit";
const METRIC_SCANNER_SET_SCAN_WAIT_SECONDS: &str = "rustfs_scanner_set_scan_wait_seconds";
const METRIC_SCANNER_DISK_SCAN_WAIT_SECONDS: &str = "rustfs_scanner_disk_scan_wait_seconds";
const METRIC_SCANNER_SET_SCANS_ACTIVE: &str = "rustfs_scanner_set_scans_active";
const METRIC_SCANNER_SET_SCANS_QUEUED: &str = "rustfs_scanner_set_scans_queued";
const METRIC_SCANNER_DISK_BUCKET_SCANS_ACTIVE: &str = "rustfs_scanner_disk_bucket_scans_active";
const METRIC_SCANNER_DISK_BUCKET_SCANS_QUEUED: &str = "rustfs_scanner_disk_bucket_scans_queued";

pub(crate) const SCANNER_SEGMENT_ACTIVATION_PROOF_INPUTS: [&str; 9] = [
    "source",
    "bucket_incarnation",
    "key_format",
    "baseline_scan_plan_digest",
    "process_epoch",
    "generation_window",
    "producer_identities",
    "durable_dirty_producer_journal",
    "ack_generation_guard",
];
pub(crate) const SCANNER_SEGMENT_ACTIVATION_FAIL_CLOSED_CHECKS: [&str; 8] = [
    "missing_producer_identity",
    "missing_durable_journal_replay",
    "restart_gap",
    "generation_gap",
    "overflow",
    "stale_ack_generation",
    "missing_cold_zero_walk_oracle",
    "distributed_without_peer_invalidation",
];

pub type DirtyUsageBuckets = HashMap<String, u64>;

#[derive(Clone, Debug)]
struct DirtyUsageSnapshot {
    buckets: Arc<DirtyUsageBuckets>,
    scopes: Arc<DirtyUsageBucketScopes>,
    generation: u64,
    /// Nothing moved in the dirty map during the bucket listing.
    covers_all_pending: bool,
    /// Every pending bucket this cycle listed is in the snapshot; see
    /// `snapshot_dirty_usage_buckets_after_listing`. Admits a scoped walk.
    covers_listed_pending: bool,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct ScannerBucketScanScope {
    selected_buckets: Option<Arc<HashSet<String>>>,
    selected_bucket_prefixes: Option<Arc<HashMap<String, ScannerBucketPrefixScanScope>>>,
    baseline_scan_plan_digest: Option<DataUsageScanPlanDigest>,
}

impl ScannerBucketScanScope {
    #[cfg(test)]
    pub(crate) fn selected_buckets_for_tests(&self) -> Option<&HashSet<String>> {
        self.selected_buckets.as_deref()
    }

    fn is_default(&self) -> bool {
        self.selected_buckets.is_none() && self.selected_bucket_prefixes.is_none() && self.baseline_scan_plan_digest.is_none()
    }

    fn from_dirty_buckets(
        selected_buckets: HashSet<String>,
        selected_bucket_prefixes: HashMap<String, ScannerBucketPrefixScanScope>,
        baseline_scan_plan_digest: DataUsageScanPlanDigest,
    ) -> Self {
        Self {
            selected_buckets: Some(Arc::new(selected_buckets)),
            selected_bucket_prefixes: (!selected_bucket_prefixes.is_empty()).then(|| Arc::new(selected_bucket_prefixes)),
            baseline_scan_plan_digest: Some(baseline_scan_plan_digest),
        }
    }

    pub(crate) fn prefix_scope_for(&self, bucket: &str) -> Option<ScannerBucketPrefixScanScope> {
        self.selected_bucket_prefixes.as_ref()?.get(bucket).cloned()
    }
}

#[derive(Clone, Copy)]
pub(super) struct ScannerCacheBaselineProof<'a> {
    pub(super) authoritative_data: Option<&'a Bytes>,
    pub(super) observed_candidate_data: Option<&'a Bytes>,
    pub(super) expected_sources: &'a HashSet<DataUsageCacheSource>,
    pub(super) leader_epoch: u64,
    pub(super) want_cycle: u64,
    pub(super) scan_plan_digest: DataUsageScanPlanDigest,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ScannerPeerDirtyUsageExpectation {
    instance_id: String,
    generation: u64,
    pending: bool,
}

#[derive(Debug, PartialEq, Eq)]
struct VerifiedRemoteDirtyUsage {
    dirty_buckets: HashSet<String>,
    acknowledgements: Vec<crate::scanner::ScannerDirtyUsageAcknowledgement>,
    peer_count: usize,
    dirty_peer_count: usize,
}

struct ScannerBucketScopeResolutionResult {
    scope: ScannerBucketScanScope,
    remote_dirty_usage_acknowledgements: Vec<crate::scanner::ScannerDirtyUsageAcknowledgement>,
    distributed_segment_invalidation_evidence: Option<DistributedSegmentInvalidationEvidence>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct DistributedSegmentInvalidationEvidence {
    pub(crate) invalidation_domain: crate::segment_invalidation::SegmentInvalidationDomain,
    pub(crate) distributed_ec_invalidation: bool,
    pub(crate) peer_count: usize,
    pub(crate) dirty_peer_count: usize,
    pub(crate) same_window_remote_proof: bool,
    pub(crate) all_peers_bound_to_generation_window: bool,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct ScannerSegmentReuseActivationProof {
    pub(crate) production_activation: bool,
    pub(crate) producer_identity_coverage_complete: bool,
    pub(crate) durable_producer_identity: bool,
    pub(crate) durable_dirty_producer_journal: bool,
    pub(crate) restart_gap_absent: bool,
    pub(crate) generation_window_bound: bool,
    pub(crate) overflow_absent: bool,
    pub(crate) ack_generation_guard: bool,
    pub(crate) cold_zero_walk_oracle: bool,
    pub(crate) distributed_peer_invalidation: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ScannerSegmentReuseActivationPreflight {
    pub(crate) production_activation: bool,
    pub(crate) scanner_segment_reuse_activated: bool,
    pub(crate) proof_inputs: &'static [&'static str],
    pub(crate) fail_closed_checks: &'static [&'static str],
    pub(crate) fail_closed_blockers: [Option<&'static str>; 8],
}

impl ScannerSegmentReuseActivationPreflight {
    #[cfg(test)]
    pub(crate) fn fail_closed_blockers(&self) -> impl Iterator<Item = &'static str> + '_ {
        self.fail_closed_blockers.iter().filter_map(|blocker| *blocker)
    }
}

fn verified_remote_dirty_usage(
    expected_peers: &HashMap<String, ScannerPeerDirtyUsageExpectation>,
    peer_snapshots: Vec<(String, EcstoreScannerPeerDirtyUsageSnapshot)>,
) -> Option<VerifiedRemoteDirtyUsage> {
    if expected_peers.is_empty() || peer_snapshots.len() != expected_peers.len() {
        return None;
    }

    let mut received_peers = HashSet::with_capacity(peer_snapshots.len());
    let mut dirty_buckets = HashSet::new();
    let mut acknowledgements = Vec::new();
    for (host, snapshot) in peer_snapshots {
        let expected = expected_peers.get(&host)?;
        if !received_peers.insert(host.clone())
            || snapshot.instance_id != expected.instance_id
            || snapshot.generation != expected.generation
            || snapshot.generation == u64::MAX
            || snapshot.protocol_version != crate::SCANNER_DIRTY_USAGE_SNAPSHOT_PROTOCOL_VERSION
            || !snapshot.complete
            || snapshot.pending_bucket_count != u64::try_from(snapshot.buckets.len()).unwrap_or(u64::MAX)
            || (expected.pending && snapshot.pending_bucket_count == 0)
            || (!expected.pending && snapshot.pending_bucket_count != 0)
        {
            return None;
        }
        let entries = snapshot
            .buckets
            .iter()
            .map(|(bucket, state)| crate::storage_api::EcstoreScannerScopedDirtyUsageAckEntry {
                bucket: bucket.clone(),
                bucket_incarnation: state.bucket_incarnation,
                generation: state.generation,
            })
            .collect::<Vec<_>>();
        dirty_buckets.extend(snapshot.buckets.keys().cloned());
        if !entries.is_empty() {
            acknowledgements.push(crate::scanner::ScannerDirtyUsageAcknowledgement {
                host,
                instance_id: snapshot.instance_id,
                kind: crate::scanner::ScannerDirtyUsageAcknowledgementKind::Scoped {
                    owner_id: snapshot.owner_id,
                    entries,
                },
            });
        }
    }

    let peer_count = received_peers.len();
    let dirty_peer_count = acknowledgements.len();
    (received_peers.len() == expected_peers.len()).then_some(VerifiedRemoteDirtyUsage {
        dirty_buckets,
        acknowledgements,
        peer_count,
        dirty_peer_count,
    })
}

fn scanner_scoped_dirty_usage_ack_exceeds_cost_threshold(
    acknowledgements: &[crate::scanner::ScannerDirtyUsageAcknowledgement],
) -> bool {
    acknowledgements.iter().any(|acknowledgement| {
        matches!(
            &acknowledgement.kind,
            crate::scanner::ScannerDirtyUsageAcknowledgementKind::Scoped { entries, .. }
                if entries.len() > crate::SCANNER_SCOPED_DIRTY_USAGE_ACK_MAX_ENTRIES
        )
    })
}

fn resolve_remote_dirty_usage_scope(
    requested_scope: ScannerBucketScanScope,
    dirty_usage_snapshot: &DirtyUsageSnapshot,
    remote_dirty_usage: VerifiedRemoteDirtyUsage,
    all_buckets: &[BucketInfo],
    baseline_proof: ScannerCacheBaselineProof<'_>,
) -> ScannerBucketScopeResolutionResult {
    let default_result = |scope: ScannerBucketScanScope| ScannerBucketScopeResolutionResult {
        scope,
        remote_dirty_usage_acknowledgements: Vec::new(),
        distributed_segment_invalidation_evidence: None,
    };

    let peer_count = remote_dirty_usage.peer_count;
    let dirty_peer_count = remote_dirty_usage.dirty_peer_count;
    let remote_dirty_buckets = remote_dirty_usage.dirty_buckets.clone();
    let mut dirty_buckets = dirty_usage_snapshot.buckets.keys().cloned().collect::<HashSet<_>>();
    dirty_buckets.extend(remote_dirty_usage.dirty_buckets);
    // Peer snapshots contribute bucket names only; the local prefix scopes
    // would narrow a bucket a peer dirtied elsewhere, so the merged scope
    // stays at bucket granularity (same rule as the local fallthrough).
    let scope = scoped_scan_scope_from_dirty_buckets(
        requested_scope.clone(),
        dirty_buckets.clone(),
        None,
        true,
        false,
        all_buckets,
        baseline_proof,
    );
    if scope.is_default() {
        return default_result(scope);
    }
    let Some(selected_buckets) = scope.selected_buckets.as_ref() else {
        return default_result(scope);
    };
    let mut scoped_acknowledgements = Vec::with_capacity(remote_dirty_usage.acknowledgements.len());
    for acknowledgement in remote_dirty_usage.acknowledgements {
        let crate::scanner::ScannerDirtyUsageAcknowledgement {
            host,
            instance_id,
            kind: crate::scanner::ScannerDirtyUsageAcknowledgementKind::Scoped { owner_id, entries },
        } = acknowledgement
        else {
            return default_result(scope);
        };
        let entries = entries
            .into_iter()
            .filter(|entry| selected_buckets.contains(&entry.bucket))
            .collect::<Vec<_>>();
        if !entries.is_empty() {
            scoped_acknowledgements.push(crate::scanner::ScannerDirtyUsageAcknowledgement {
                host,
                instance_id,
                kind: crate::scanner::ScannerDirtyUsageAcknowledgementKind::Scoped { owner_id, entries },
            });
        }
    }
    if scanner_scoped_dirty_usage_ack_exceeds_cost_threshold(&scoped_acknowledgements) {
        return default_result(ScannerBucketScanScope::default());
    }
    let has_scoped_acknowledgements = !scoped_acknowledgements.is_empty();

    let distributed_segment_invalidation_evidence =
        (dirty_peer_count > 0 && has_scoped_acknowledgements).then_some(DistributedSegmentInvalidationEvidence {
            invalidation_domain: crate::segment_invalidation::SegmentInvalidationDomain::DistributedEc,
            distributed_ec_invalidation: true,
            peer_count,
            dirty_peer_count,
            same_window_remote_proof: true,
            all_peers_bound_to_generation_window: true,
        });
    let segment_reuse_activation_preflight = scanner_segment_reuse_activation_preflight_for_baseline_with_evidence(
        dirty_usage_snapshot,
        true,
        baseline_proof,
        distributed_segment_invalidation_evidence,
    );
    let scope = if segment_reuse_activation_preflight.scanner_segment_reuse_activated {
        let local_only_scopes = dirty_usage_snapshot
            .scopes
            .iter()
            .filter(|(bucket, _)| !remote_dirty_buckets.contains(bucket.as_str()))
            .map(|(bucket, scope)| (bucket.clone(), scope.clone()))
            .collect::<DirtyUsageBucketScopes>();
        scoped_scan_scope_from_dirty_buckets(
            requested_scope,
            dirty_buckets,
            (!local_only_scopes.is_empty()).then_some(&local_only_scopes),
            true,
            true,
            all_buckets,
            baseline_proof,
        )
    } else {
        scope
    };

    ScannerBucketScopeResolutionResult {
        scope,
        remote_dirty_usage_acknowledgements: scoped_acknowledgements,
        distributed_segment_invalidation_evidence,
    }
}

/// `expected_converged` is `None` for the authoritative object: a `Complete`
/// cycle with pending buckets publishes there unconverged and is still a
/// structural baseline, because the buckets it could not certify stay in the
/// dirty map and are rescanned rather than reused. The convergence marker
/// must still be present; a snapshot without one was not written by a
/// certifying scanner.
fn complete_scanner_cache_snapshot_plan_digest(
    snapshot: &DataUsageInfo,
    proof: ScannerCacheBaselineProof<'_>,
    expected_converged: Option<bool>,
) -> Option<DataUsageScanPlanDigest> {
    // The observational mark disqualifies only the authoritative slot: an
    // observed candidate carries it by construction.
    let marks_accepted = match expected_converged {
        Some(expected) => snapshot.usage_snapshot_converged == Some(expected),
        None => snapshot.usage_snapshot_converged.is_some() && !snapshot.usage_snapshot_observational,
    };
    if !snapshot.is_complete_bucket_usage_snapshot()
        || snapshot.usage_snapshot_partial
        || !marks_accepted
        || snapshot.scanner_epoch != Some(proof.leader_epoch)
        || snapshot.usage_snapshot_set_states.len() != proof.expected_sources.len()
    {
        return None;
    }

    // Completed maintenance also covers ordinary usage. Keep its exact stored
    // proof for cache reuse, and reject mixtures of different set work proofs.
    let baseline_plan_digest = DataUsageScanPlanDigest(snapshot.usage_snapshot_set_states.first()?.scan_plan_digest?);
    if !scanner_plan_digest_is_work_variant(proof.scan_plan_digest, baseline_plan_digest) {
        return None;
    }
    let mut states = HashSet::with_capacity(snapshot.usage_snapshot_set_states.len());
    for state in &snapshot.usage_snapshot_set_states {
        let source = DataUsageCacheSource::new(usize::try_from(state.pool_index).ok()?, usize::try_from(state.set_index).ok()?);
        if !proof.expected_sources.contains(&source)
            || !states.insert(source)
            || !state.complete
            || state.tombstone
            || state.scanner_epoch != Some(proof.leader_epoch)
            || state.scanner_cycle.is_none_or(|cycle| cycle > proof.want_cycle)
            || state.scan_plan_digest != Some(baseline_plan_digest.0)
        {
            return None;
        }
    }

    (states == *proof.expected_sources).then_some(baseline_plan_digest)
}

/// Whether `digest` is `structural` under any work flag: the same bucket
/// list, topology and movement state walked as a scoped, full or Deep sweep.
/// The flag changes what a walk must visit, not the namespace it describes.
fn scanner_plan_digest_is_work_variant(structural: DataUsageScanPlanDigest, digest: DataUsageScanPlanDigest) -> bool {
    [
        structural,
        scanner_bucket_work_digest(structural, HealScanMode::Normal, true),
        scanner_bucket_work_digest(structural, HealScanMode::Deep, true),
    ]
    .contains(&digest)
}

/// Buckets the last authoritative publication left pending. A `Complete`
/// cycle names every bucket whose value it could not certify, including a
/// partial walk that published the bucket's last certified value, so the
/// next scoped cycle must walk them even when the dirty map no longer names
/// them. Over-inclusion only costs a walk; a missing baseline contributes
/// nothing and the dirty map decides alone.
fn scanner_baseline_pending_buckets(proof: &ScannerCacheBaselineProof<'_>) -> Vec<String> {
    proof
        .authoritative_data
        .and_then(|data| serde_json::from_slice::<DataUsageInfo>(data).ok())
        .map(|snapshot| snapshot.usage_snapshot_pending_buckets)
        .unwrap_or_default()
}

fn complete_scanner_cache_baseline_plan_digest(proof: ScannerCacheBaselineProof<'_>) -> Option<DataUsageScanPlanDigest> {
    let authoritative = serde_json::from_slice::<DataUsageInfo>(proof.authoritative_data?).ok()?;
    if let Some(validated_digest) = complete_scanner_cache_snapshot_plan_digest(&authoritative, proof, None) {
        return Some(validated_digest);
    }

    // A complete but superseded observation may reuse its per-set cache only
    // when it was explicitly tied to the durable authoritative baseline. It
    // remains observational: this proof grants bucket-scope reuse only and
    // never changes authoritative usage publication or dirty acknowledgement.
    let authoritative_has_identity = (crate::scanner::data_usage_info_has_persisted_baseline_identity(&authoritative)
        && !authoritative.usage_snapshot_observational)
        || crate::scanner::data_usage_info_is_bootstrap_pending(&authoritative);
    if !authoritative_has_identity {
        return None;
    }
    let observed = serde_json::from_slice::<DataUsageInfo>(proof.observed_candidate_data?).ok()?;
    if !observed_data_usage_is_newer(&observed, &authoritative) {
        return None;
    }

    complete_scanner_cache_snapshot_plan_digest(&observed, proof, Some(false))
}

fn scanner_segment_reuse_baseline_producer_evidence(
    dirty_usage_snapshot: &DirtyUsageSnapshot,
    baseline_proof: ScannerCacheBaselineProof<'_>,
) -> (DirtyUsageProducerEvidence, bool) {
    let mut evidence = dirty_usage_producer_evidence(dirty_usage_snapshot);
    if !evidence.generation_window_bound || !evidence.producer_identity_coverage_complete {
        return (evidence, false);
    }

    let Some(authoritative_data) = baseline_proof.authoritative_data else {
        return (evidence, false);
    };
    let Ok(authoritative) = serde_json::from_slice::<DataUsageInfo>(authoritative_data) else {
        return (evidence, false);
    };
    // Segment (prefix) reuse keeps requiring a converged baseline: its
    // producer evidence is proven against a dirty snapshot that was current.
    if complete_scanner_cache_snapshot_plan_digest(&authoritative, baseline_proof, Some(true)).is_none() {
        return (evidence, false);
    }
    let Some(cold_zero_walk_oracle) =
        scanner_snapshot_set_states_segment_reuse_proof(&authoritative, &evidence, baseline_proof.expected_sources)
    else {
        return (evidence, false);
    };

    evidence.durable_producer_identity = true;
    evidence.restart_gap_absent = true;
    (evidence, cold_zero_walk_oracle)
}

fn scanner_snapshot_set_states_segment_reuse_proof(
    snapshot: &DataUsageInfo,
    evidence: &DirtyUsageProducerEvidence,
    expected_sources: &HashSet<DataUsageCacheSource>,
) -> Option<bool> {
    let mut covered_sources = HashSet::with_capacity(expected_sources.len());
    let mut cold_zero_walk_oracle = true;
    let all_sets_proved = snapshot.usage_snapshot_set_states.iter().all(|state| {
        let (Ok(pool_index), Ok(set_index)) = (usize::try_from(state.pool_index), usize::try_from(state.set_index)) else {
            return false;
        };
        let source = DataUsageCacheSource::new(pool_index, set_index);
        let proved = state.complete
            && !state.tombstone
            && expected_sources.contains(&source)
            && covered_sources.insert(source)
            && scanner_segment_invalidation_baseline_proof_matches(state.segment_invalidation_proof.as_ref(), evidence);
        if proved {
            cold_zero_walk_oracle &= state
                .segment_invalidation_proof
                .as_ref()
                .is_some_and(|proof| proof.cold_zero_walk_oracle);
        }
        proved
    });
    (all_sets_proved && covered_sources.len() == expected_sources.len()).then_some(cold_zero_walk_oracle)
}

fn scoped_scan_scope_from_dirty_buckets(
    requested_scope: ScannerBucketScanScope,
    dirty_buckets: HashSet<String>,
    dirty_scopes: Option<&DirtyUsageBucketScopes>,
    dirty_snapshot_complete: bool,
    segment_reuse_activated: bool,
    all_buckets: &[BucketInfo],
    baseline_proof: ScannerCacheBaselineProof<'_>,
) -> ScannerBucketScanScope {
    if !requested_scope.is_default() || !dirty_snapshot_complete {
        return requested_scope;
    }

    let current_buckets = all_buckets.iter().map(|bucket| bucket.name.as_str()).collect::<HashSet<_>>();
    let selected_buckets = dirty_buckets
        .into_iter()
        .chain(scanner_baseline_pending_buckets(&baseline_proof))
        .filter(|bucket| current_buckets.contains(bucket.as_str()))
        .collect::<HashSet<_>>();
    if selected_buckets.is_empty() {
        return requested_scope;
    }

    let Some(baseline_scan_plan_digest) = complete_scanner_cache_baseline_plan_digest(baseline_proof) else {
        return requested_scope;
    };

    let selected_bucket_prefixes = if segment_reuse_activated {
        dirty_scopes
            .into_iter()
            .flat_map(|dirty_scopes| {
                selected_buckets
                    .iter()
                    .filter_map(|bucket| dirty_scopes.get(bucket).map(|scope| (bucket.clone(), scope)))
            })
            .filter_map(|(bucket, scope)| match scope {
                DirtyUsageBucketScope::WholeBucket => None,
                DirtyUsageBucketScope::TopLevelEntries(entries) => {
                    ScannerBucketPrefixScanScope::from_dirty_top_level_entries(entries.clone()).map(|scope| (bucket, scope))
                }
            })
            .collect()
    } else {
        HashMap::new()
    };

    ScannerBucketScanScope::from_dirty_buckets(selected_buckets, selected_bucket_prefixes, baseline_scan_plan_digest)
}

/// Why a cycle eligible for a dirty-bucket scope walked the whole namespace.
///
/// Only cycles that were not required to walk everything are classified, so
/// scheduled maintenance, deep scans, and the startup cycle never report one.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ScannerScopeFallbackReason {
    /// A dirty entry was removed or replaced during the bucket listing.
    DirtySnapshotIncomplete,
    /// The dirty-usage generation is exhausted and cannot be verified.
    DirtyGenerationExhausted,
    /// More buckets are dirty than one scoped cycle may select.
    DirtyBucketOverflow,
    /// No dirty bucket is in the listing, so there is nothing to scope to.
    NoListedDirtyBuckets,
    /// No complete authoritative or observed baseline matches the plan.
    BaselineUnavailable,
    /// Peer dirty usage could not be verified or scoped acknowledgements
    /// were rejected.
    PeerScopeUnavailable,
}

impl ScannerScopeFallbackReason {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::DirtySnapshotIncomplete => "dirty_snapshot_incomplete",
            Self::DirtyGenerationExhausted => "dirty_generation_exhausted",
            Self::DirtyBucketOverflow => "dirty_bucket_overflow",
            Self::NoListedDirtyBuckets => "no_listed_dirty_buckets",
            Self::BaselineUnavailable => "baseline_unavailable",
            Self::PeerScopeUnavailable => "peer_scope_unavailable",
        }
    }
}

/// Classify a resolved full-namespace scope against the preconditions
/// `resolve_scanner_bucket_scan_scope` checks, in the same order. Returns
/// `None` when the cycle was required to walk everything or was scoped.
fn scanner_scope_fallback_reason(
    scope_eligible: bool,
    resolved_scope: &ScannerBucketScanScope,
    dirty_usage_snapshot: &DirtyUsageSnapshot,
    all_buckets: &[BucketInfo],
    baseline_proof: ScannerCacheBaselineProof<'_>,
    distributed: bool,
) -> Option<ScannerScopeFallbackReason> {
    if !scope_eligible || !resolved_scope.is_default() {
        return None;
    }
    if dirty_usage_snapshot.generation == u64::MAX {
        return Some(ScannerScopeFallbackReason::DirtyGenerationExhausted);
    }
    if !dirty_usage_snapshot.covers_listed_pending {
        return Some(ScannerScopeFallbackReason::DirtySnapshotIncomplete);
    }
    if dirty_usage_snapshot.buckets.len() > crate::SCANNER_DIRTY_USAGE_SNAPSHOT_MAX_ENTRIES {
        return Some(ScannerScopeFallbackReason::DirtyBucketOverflow);
    }
    // Peers may contribute dirty buckets the local snapshot does not hold.
    if !distributed {
        let listed = all_buckets.iter().map(|bucket| bucket.name.as_str()).collect::<HashSet<_>>();
        if !dirty_usage_snapshot
            .buckets
            .keys()
            .any(|bucket| listed.contains(bucket.as_str()))
        {
            return Some(ScannerScopeFallbackReason::NoListedDirtyBuckets);
        }
    }
    if complete_scanner_cache_baseline_plan_digest(baseline_proof).is_none() {
        return Some(ScannerScopeFallbackReason::BaselineUnavailable);
    }
    distributed.then_some(ScannerScopeFallbackReason::PeerScopeUnavailable)
}

fn scanner_segment_reuse_activation_preflight() -> ScannerSegmentReuseActivationPreflight {
    scanner_segment_reuse_activation_preflight_from_proof(ScannerSegmentReuseActivationProof {
        production_activation: true,
        ..Default::default()
    })
}

fn scanner_segment_reuse_activation_preflight_from_proof(
    proof: ScannerSegmentReuseActivationProof,
) -> ScannerSegmentReuseActivationPreflight {
    ScannerSegmentReuseActivationPreflight {
        production_activation: proof.production_activation,
        scanner_segment_reuse_activated: proof.production_activation
            && proof.producer_identity_coverage_complete
            && proof.durable_producer_identity
            && proof.durable_dirty_producer_journal
            && proof.restart_gap_absent
            && proof.generation_window_bound
            && proof.overflow_absent
            && proof.ack_generation_guard
            && proof.cold_zero_walk_oracle
            && proof.distributed_peer_invalidation,
        proof_inputs: &SCANNER_SEGMENT_ACTIVATION_PROOF_INPUTS,
        fail_closed_checks: &SCANNER_SEGMENT_ACTIVATION_FAIL_CLOSED_CHECKS,
        fail_closed_blockers: [
            (!proof.producer_identity_coverage_complete || !proof.durable_producer_identity)
                .then_some("missing_producer_identity"),
            (!proof.durable_dirty_producer_journal).then_some("missing_durable_journal_replay"),
            (!proof.restart_gap_absent).then_some("restart_gap"),
            (!proof.generation_window_bound).then_some("generation_gap"),
            (!proof.overflow_absent).then_some("overflow"),
            (!proof.ack_generation_guard).then_some("stale_ack_generation"),
            (!proof.cold_zero_walk_oracle).then_some("missing_cold_zero_walk_oracle"),
            (!proof.distributed_peer_invalidation).then_some("distributed_without_peer_invalidation"),
        ],
    }
}

fn scanner_segment_reuse_activation_preflight_for_cycle(
    dirty_usage_snapshot: &DirtyUsageSnapshot,
    dirty_usage_producer_evidence: DirtyUsageProducerEvidence,
    distributed: bool,
    distributed_segment_invalidation_evidence: Option<DistributedSegmentInvalidationEvidence>,
    cold_zero_walk_oracle: bool,
) -> ScannerSegmentReuseActivationPreflight {
    scanner_segment_reuse_activation_preflight_from_proof(ScannerSegmentReuseActivationProof {
        production_activation: true,
        producer_identity_coverage_complete: dirty_usage_producer_evidence.producer_identity_coverage_complete,
        durable_producer_identity: dirty_usage_producer_evidence.durable_producer_identity,
        durable_dirty_producer_journal: dirty_usage_producer_evidence.durable_dirty_producer_journal,
        restart_gap_absent: dirty_usage_producer_evidence.restart_gap_absent,
        generation_window_bound: dirty_usage_snapshot.covers_all_pending
            && dirty_usage_snapshot.generation != 0
            && dirty_usage_snapshot.generation != u64::MAX
            && dirty_usage_producer_evidence.generation_window_bound,
        overflow_absent: dirty_usage_snapshot.covers_all_pending,
        ack_generation_guard: true,
        cold_zero_walk_oracle,
        distributed_peer_invalidation: scanner_distributed_segment_invalidation_admitted(
            distributed,
            distributed_segment_invalidation_evidence,
        ),
    })
}

fn scanner_segment_reuse_activation_preflight_for_baseline(
    dirty_usage_snapshot: &DirtyUsageSnapshot,
    distributed: bool,
    baseline_proof: ScannerCacheBaselineProof<'_>,
) -> ScannerSegmentReuseActivationPreflight {
    scanner_segment_reuse_activation_preflight_for_baseline_with_evidence(dirty_usage_snapshot, distributed, baseline_proof, None)
}

fn scanner_segment_reuse_activation_preflight_for_baseline_with_evidence(
    dirty_usage_snapshot: &DirtyUsageSnapshot,
    distributed: bool,
    baseline_proof: ScannerCacheBaselineProof<'_>,
    distributed_segment_invalidation_evidence: Option<DistributedSegmentInvalidationEvidence>,
) -> ScannerSegmentReuseActivationPreflight {
    let (dirty_usage_producer_evidence, cold_zero_walk_oracle) =
        scanner_segment_reuse_baseline_producer_evidence(dirty_usage_snapshot, baseline_proof);
    scanner_segment_reuse_activation_preflight_for_cycle(
        dirty_usage_snapshot,
        dirty_usage_producer_evidence,
        distributed,
        distributed_segment_invalidation_evidence,
        cold_zero_walk_oracle,
    )
}

fn scanner_distributed_segment_invalidation_admitted(
    distributed: bool,
    evidence: Option<DistributedSegmentInvalidationEvidence>,
) -> bool {
    if !distributed {
        return true;
    }
    evidence.is_some_and(|evidence| {
        evidence.invalidation_domain == crate::segment_invalidation::SegmentInvalidationDomain::DistributedEc
            && evidence.distributed_ec_invalidation
            && evidence.same_window_remote_proof
            && evidence.all_peers_bound_to_generation_window
            && evidence.dirty_peer_count > 0
            && evidence.dirty_peer_count <= evidence.peer_count
    })
}

fn scanner_durable_segment_invalidation_evidence(
    dirty_usage_snapshot: &DirtyUsageSnapshot,
    results: &[DataUsageCache],
    expected_sources: &HashSet<DataUsageCacheSource>,
) -> DirtyUsageProducerEvidence {
    let mut evidence = dirty_usage_producer_evidence(dirty_usage_snapshot);
    if !evidence.generation_window_bound
        || !evidence.producer_identity_coverage_complete
        || !scanner_results_form_complete_snapshot(results, expected_sources)
    {
        return evidence;
    }

    let mut covered_sources = HashSet::with_capacity(expected_sources.len());
    let all_sets_proved = results.iter().all(|result| {
        let Some(source) = result.info.source else {
            return false;
        };
        expected_sources.contains(&source)
            && covered_sources.insert(source)
            && scanner_segment_invalidation_proof_matches(result.info.segment_invalidation_proof.as_ref(), &evidence)
    });
    if all_sets_proved && covered_sources.len() == expected_sources.len() {
        evidence.durable_producer_identity = true;
        evidence.restart_gap_absent = true;
    }
    evidence
}

fn scanner_segment_invalidation_proof_matches(
    proof: Option<&crate::DataUsageSegmentInvalidationProof>,
    evidence: &DirtyUsageProducerEvidence,
) -> bool {
    proof.is_some_and(|proof| {
        scanner_segment_invalidation_proof_is_well_formed(proof)
            && proof.generation_start == evidence.generation_start
            && proof.generation_end == evidence.generation_end
    })
}

fn scanner_segment_invalidation_baseline_proof_matches(
    proof: Option<&crate::DataUsageSegmentInvalidationProof>,
    evidence: &DirtyUsageProducerEvidence,
) -> bool {
    proof.is_some_and(|proof| {
        scanner_segment_invalidation_proof_is_well_formed(proof)
            && ((proof.generation_start == evidence.generation_start && proof.generation_end == evidence.generation_end)
                || proof.generation_end < evidence.generation_start)
    })
}

fn scanner_segment_invalidation_proof_is_well_formed(proof: &crate::DataUsageSegmentInvalidationProof) -> bool {
    proof.process_epoch == scanner_activity_epoch()
        && proof.generation_start != 0
        && proof.generation_end >= proof.generation_start
        && proof.producer_identity_coverage_complete
}

fn scanner_completed_set_segment_invalidation_proof(
    proof: &Option<crate::DataUsageSegmentInvalidationProof>,
    cold_zero_walk_reuse_candidate: bool,
) -> Option<crate::DataUsageSegmentInvalidationProof> {
    clone_cache_value(proof).map(|mut proof| {
        proof.cold_zero_walk_oracle = cold_zero_walk_reuse_candidate;
        proof
    })
}

#[cfg(test)]
fn scanner_segment_reuse_activated() -> bool {
    scanner_segment_reuse_activation_preflight().scanner_segment_reuse_activated
}

pub(crate) fn is_scanner_metadata_corrupt_error(err: &StorageError) -> bool {
    matches!(err, StorageError::Io(io) if io.to_string().starts_with(SCANNER_METADATA_CORRUPT_ERROR))
}

pub(crate) fn is_scanner_metadata_transient_error(err: &StorageError) -> bool {
    matches!(err, StorageError::Io(io) if io.to_string().starts_with(SCANNER_METADATA_TRANSIENT_ERROR))
}

fn scanner_metadata_corrupt_error(reason: impl std::fmt::Display, bucket: &str, object_path: &str) -> StorageError {
    StorageError::other(format!(
        "{SCANNER_METADATA_CORRUPT_ERROR}: {reason}, bucket={bucket}, object_path={object_path}"
    ))
}

fn scanner_metadata_transient_error(reason: impl std::fmt::Display, bucket: &str, object_path: &str) -> StorageError {
    StorageError::other(format!(
        "{SCANNER_METADATA_TRANSIENT_ERROR}: {reason}, bucket={bucket}, object_path={object_path}"
    ))
}

async fn object_lock_config_for_scanner_item(item: &ScannerItem) -> Option<Arc<ObjectLockConfiguration>> {
    if let Some(config) = item.object_lock.clone() {
        return Some(config);
    }

    get_object_lock_config(&item.bucket)
        .await
        .ok()
        .map(|(config, _)| Arc::new(config))
}

fn object_lock_config_enabled(config: &ObjectLockConfiguration) -> bool {
    config
        .object_lock_enabled
        .as_ref()
        .is_some_and(|enabled| enabled == "Enabled")
}

pub struct ScannerBucketScanPlan {
    buckets: Vec<BucketInfo>,
    all_buckets: Arc<Vec<BucketInfo>>,
    scope: ScannerBucketScanScope,
    digest: DataUsageScanPlanDigest,
    /// Includes mutation generations even when the set planner uses a structural digest.
    bucket_coverage_digest: DataUsageScanPlanDigest,
    /// The plan a bucket sweep is started and resumed under: the coverage
    /// identity (`scanner_activity_coverage_digest`) with the bucket list.
    /// It leaves out what `bucket_coverage_digest` binds for reuse, so a
    /// coordinator restart or a maintenance publication does not turn a
    /// resumed sweep into mixed coverage.
    checkpoint_coverage_digest: DataUsageScanPlanDigest,
    requires_full_scan: bool,
    service_cohort: Option<Arc<StdMutex<ScannerServiceCohort>>>,
    // Cache work must invalidate on namespace completion even when its scoped baseline remains reusable.
    execution_digest: DataUsageScanPlanDigest,
    leader_epoch: u64,
    tier_registry_generation: u64,
    /// Epoch captured once for the whole scanner cycle.  `None` is retained
    /// for unfenced test implementations; production plans always carry the
    /// admission token captured before bucket enumeration.
    publication_epoch: Option<u64>,
    dirty_usage_buckets: Arc<DirtyUsageBuckets>,
    bucket_failures: ScannerBucketFailureState,
    bucket_completions: Option<Arc<Mutex<ScannerBucketCompletions>>>,
    pending_maintenance_work: Arc<AtomicBool>,
    cache_cycle_floor: Arc<AtomicU64>,
    cold_zero_walk_reuse_observed: Arc<AtomicBool>,
    segment_invalidation_proof: Option<crate::DataUsageSegmentInvalidationProof>,
}

#[derive(Clone, Default)]
struct ScannerBucketFailureState {
    hard: Arc<Mutex<HashSet<String>>>,
    partial: Arc<Mutex<HashSet<String>>>,
    /// Partial buckets whose set scan published the bucket's last certified
    /// value in place of the unfinished walk; a subset of `partial`.
    partial_carried: Arc<Mutex<HashSet<String>>>,
    namespace_not_found: Arc<Mutex<HashSet<String>>>,
}

pub(crate) fn scanner_bucket_plan_digest(buckets: &[BucketInfo], activity_digest: [u8; 32]) -> DataUsageScanPlanDigest {
    let mut buckets = buckets.iter().collect::<Vec<_>>();
    buckets.sort_unstable_by(|left, right| left.name.cmp(&right.name));

    let mut hasher = Sha256::new();
    hasher.update(activity_digest);
    hasher.update(u64::try_from(buckets.len()).unwrap_or(u64::MAX).to_be_bytes());
    for bucket in buckets {
        let name = bucket.name.as_bytes();
        hasher.update(u64::try_from(name.len()).unwrap_or(u64::MAX).to_be_bytes());
        hasher.update(name);
        match bucket.created {
            Some(created) => {
                hasher.update([1]);
                hasher.update(created.unix_timestamp_nanos().to_be_bytes());
            }
            None => hasher.update([0]),
        }
    }
    DataUsageScanPlanDigest(hasher.finalize().into())
}

fn scanner_bucket_inventory_is_complete(
    all_buckets: &[BucketInfo],
    buckets_by_source: &HashMap<DataUsageCacheSource, Vec<BucketInfo>>,
) -> bool {
    let inventory = all_buckets
        .iter()
        .map(|bucket| (bucket.name.as_str(), bucket.created))
        .collect::<HashMap<_, _>>();
    if inventory.len() != all_buckets.len() || inventory.keys().any(|name| name.is_empty() || *name == DATA_USAGE_ROOT) {
        return false;
    }
    let mut covered = HashSet::with_capacity(inventory.len());
    for buckets in buckets_by_source.values() {
        let mut set_names = HashSet::with_capacity(buckets.len());
        for bucket in buckets {
            if !set_names.insert(bucket.name.as_str()) || inventory.get(bucket.name.as_str()) != Some(&bucket.created) {
                return false;
            }
            covered.insert(bucket.name.as_str());
        }
    }
    covered.len() == inventory.len()
}

// Bind known work requirements before both local and remote cache admission.
// Matching requirements remain reusable for the same intent; this is not a
// new deadline or a durable generation for newly due maintenance.
fn scanner_bucket_work_digest(
    scan_plan_digest: DataUsageScanPlanDigest,
    scan_mode: HealScanMode,
    requires_full_scan: bool,
) -> DataUsageScanPlanDigest {
    if scan_mode == HealScanMode::Normal && !requires_full_scan {
        return scan_plan_digest;
    }
    let mut hasher = Sha256::new();
    hasher.update(b"scanner-bucket-work-v1");
    hasher.update(scan_plan_digest.0);
    hasher.update([match scan_mode {
        HealScanMode::Unknown => 0,
        HealScanMode::Normal => 1,
        HealScanMode::Deep => 2,
    }]);
    hasher.update([u8::from(requires_full_scan || scan_mode == HealScanMode::Deep)]);
    DataUsageScanPlanDigest(hasher.finalize().into())
}

fn scanner_bucket_cache_digest(
    scan_plan_digest: DataUsageScanPlanDigest,
    dirty_generation: Option<u64>,
) -> DataUsageScanPlanDigest {
    let Some(dirty_generation) = dirty_generation else {
        return scan_plan_digest;
    };

    let mut hasher = Sha256::new();
    hasher.update(scan_plan_digest.0);
    hasher.update(dirty_generation.to_be_bytes());
    DataUsageScanPlanDigest(hasher.finalize().into())
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct ScannerBucketPlanDigests {
    pub(crate) reuse: DataUsageScanPlanDigest,
    pub(crate) coverage: DataUsageScanPlanDigest,
}

impl ScannerBucketPlanDigests {
    pub(crate) fn from_work_plans(
        reuse: DataUsageScanPlanDigest,
        coverage: DataUsageScanPlanDigest,
        dirty_generation: Option<u64>,
    ) -> Self {
        Self {
            reuse: scanner_bucket_cache_digest(reuse, dirty_generation),
            coverage: scanner_bucket_cache_digest(coverage, dirty_generation),
        }
    }
}

#[cfg(test)]
pub(crate) fn checkpoint_fixture_bucket_digest(
    scan_plan_digest: DataUsageScanPlanDigest,
    dirty_generation: Option<u64>,
) -> DataUsageScanPlanDigest {
    scanner_bucket_cache_digest(scan_plan_digest, dirty_generation)
}

fn finalize_nsscanner_result(results: &[DataUsageCache], first_err: Option<Error>) -> Result<()> {
    if results.iter().any(|result| result.info.last_update.is_some()) {
        return Ok(());
    }

    if let Some(err) = first_err {
        return Err(err);
    }

    Ok(())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ScannerBucketScanStatus {
    Complete,
    Failed,
    /// At least one bucket walk stopped early and no certified value could
    /// stand in for it on some set.
    Partial,
    /// Every bucket walk that stopped early was covered by that bucket's
    /// last certified set value, so the cycle may complete with those
    /// buckets pending. Necessary, not sufficient: `completed_all_sets`
    /// still proves that every set published.
    PartialCarried,
    NamespaceNotFound,
}

fn scanner_bucket_scan_status(
    has_failed: bool,
    has_uncarried_partial: bool,
    has_namespace_not_found: bool,
    has_carried_partial: bool,
) -> ScannerBucketScanStatus {
    if has_failed {
        ScannerBucketScanStatus::Failed
    } else if has_uncarried_partial {
        ScannerBucketScanStatus::Partial
    } else if has_namespace_not_found {
        ScannerBucketScanStatus::NamespaceNotFound
    } else if has_carried_partial {
        ScannerBucketScanStatus::PartialCarried
    } else {
        ScannerBucketScanStatus::Complete
    }
}

fn scanner_partial_progress_is_usable(
    bucket_plan_complete: bool,
    scan_scope_matches: bool,
    bucket_status: ScannerBucketScanStatus,
    has_set_failure: bool,
    has_missing_namespace: bool,
) -> bool {
    bucket_plan_complete
        && scan_scope_matches
        && matches!(bucket_status, ScannerBucketScanStatus::Partial | ScannerBucketScanStatus::PartialCarried)
        && !has_set_failure
        && !has_missing_namespace
}

fn scanner_cycle_cold_zero_walk_oracle(
    scan_scope: &ScannerBucketScanScope,
    all_buckets: &[BucketInfo],
    completed_all_sets: bool,
    scan_scope_matches: bool,
    bucket_scan_status: ScannerBucketScanStatus,
    cold_zero_walk_reuse_observed: bool,
) -> bool {
    let Some(selected_buckets) = scan_scope.selected_buckets.as_deref() else {
        return false;
    };
    cold_zero_walk_reuse_observed
        && !selected_buckets.is_empty()
        && completed_all_sets
        && scan_scope_matches
        && bucket_scan_status == ScannerBucketScanStatus::Complete
        && all_buckets.iter().any(|bucket| !selected_buckets.contains(&bucket.name))
}

fn classify_nsscanner_cycle(
    completed_all_sets: bool,
    budget_elapsed: bool,
    cancelled: bool,
    bucket_scan_status: ScannerBucketScanStatus,
    dirty_usage_status: DirtyUsageSnapshotStatus,
    activity_status: ScannerCycleActivityStatus,
) -> ScannerCycleStatus {
    // The post-scan activity proof is required regardless of why the scan was
    // incomplete.  Returning Incomplete first would apply the long ordinary
    // retry/backoff path to an unverifiable publication and could acknowledge
    // a cycle without a movement-generation proof.
    if activity_status == ScannerCycleActivityStatus::Unverified {
        return ScannerCycleStatus::Deferred(ScannerCycleDeferReason::ActivityBaselineUnavailable);
    }
    // A bucket walk that stopped early keeps the cycle incomplete only when
    // no certified value stands in for it (`Partial`). `PartialCarried`
    // means every such bucket published its last certified value and is
    // reported pending; `completed_all_sets` proves the sets agreed.
    if budget_elapsed
        || cancelled
        || !matches!(
            bucket_scan_status,
            ScannerBucketScanStatus::Complete | ScannerBucketScanStatus::PartialCarried
        )
        || dirty_usage_status == DirtyUsageSnapshotStatus::Unverified
    {
        return ScannerCycleStatus::Incomplete;
    }
    if !completed_all_sets {
        return ScannerCycleStatus::Incomplete;
    }

    // Termination is snapshot-at-the-beginning: a structurally complete walk
    // that the guardrail did not veto is `Complete`. Buckets whose dirty
    // generation moved during the walk, and buckets whose walk stopped early
    // behind a certified value, stay pending for the next cycle and are
    // reported in the publication; they no longer supersede the cycle.
    match activity_status {
        ScannerCycleActivityStatus::Unchanged => ScannerCycleStatus::Complete,
        ScannerCycleActivityStatus::Changed | ScannerCycleActivityStatus::Unverified => ScannerCycleStatus::Superseded,
    }
}

fn should_publish_usage_snapshot(status: ScannerCycleStatus) -> bool {
    matches!(status, ScannerCycleStatus::Complete | ScannerCycleStatus::Superseded)
}

fn should_publish_observational_snapshot(status: ScannerCycleStatus) -> bool {
    matches!(status, ScannerCycleStatus::Deferred(ScannerCycleDeferReason::ActivityBaselineUnavailable))
}

/// Pending buckets of a cycle, sorted so the publication is deterministic.
pub(crate) type ScannerCyclePendingBuckets = BTreeSet<String>;

/// Stamp the certification outcome of a cycle on its usage snapshot. A
/// `Complete` cycle publishes an authoritative snapshot that is converged
/// only when no bucket is pending; any other publishable status is an
/// observational snapshot routed to the observed object. The marks are part
/// of the bytes the publication expectation digests, so every writer applies
/// exactly this function.
fn apply_scanner_cycle_publication_marks(
    data_usage_info: &mut DataUsageInfo,
    status: ScannerCycleStatus,
    pending_buckets: &ScannerCyclePendingBuckets,
) {
    let complete = status == ScannerCycleStatus::Complete;
    data_usage_info.usage_snapshot_observational = !complete;
    data_usage_info.usage_snapshot_converged = Some(complete && pending_buckets.is_empty());
    if complete {
        data_usage_info.usage_snapshot_pending_buckets = pending_buckets.iter().cloned().collect();
        data_usage_info.usage_snapshot_pending_bucket_count = u64::try_from(pending_buckets.len()).unwrap_or(u64::MAX);
    } else {
        data_usage_info.usage_snapshot_pending_buckets.clear();
        data_usage_info.usage_snapshot_pending_bucket_count = 0;
    }
}

fn prepare_usage_snapshot_for_publication(
    status: ScannerCycleStatus,
    pending_buckets: &ScannerCyclePendingBuckets,
    mut data_usage_info: DataUsageInfo,
) -> Option<DataUsageInfo> {
    if !should_publish_usage_snapshot(status) {
        return None;
    }

    apply_scanner_cycle_publication_marks(&mut data_usage_info, status, pending_buckets);
    Some(data_usage_info)
}

async fn publish_usage_snapshot(
    updates: &mpsc::Sender<DataUsageInfo>,
    status: ScannerCycleStatus,
    pending_buckets: &ScannerCyclePendingBuckets,
    data_usage_info: DataUsageInfo,
) -> Result<bool> {
    let Some(data_usage_info) = prepare_usage_snapshot_for_publication(status, pending_buckets, data_usage_info) else {
        return Ok(false);
    };
    send_data_usage_update(updates, data_usage_info).await?;
    Ok(true)
}

async fn publish_observational_snapshot(
    updates: &mpsc::Sender<DataUsageInfo>,
    mut data_usage_info: DataUsageInfo,
) -> Result<bool> {
    data_usage_info.usage_snapshot_complete = false;
    data_usage_info.usage_snapshot_partial = true;
    data_usage_info.usage_snapshot_converged = Some(false);
    data_usage_info.usage_snapshot_observational = true;
    data_usage_info.usage_snapshot_pending_buckets.clear();
    data_usage_info.usage_snapshot_pending_bucket_count = 0;
    send_data_usage_update(updates, data_usage_info).await?;
    Ok(true)
}

/// Post-walk verdict of the cluster activity guardrail. `Changed` is an
/// attributed veto (or, for peers, the phase-2 dirty-usage fallback); local
/// namespace and dirty-usage generation movement is not a veto and is
/// certified per bucket instead.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ScannerCycleActivityStatus {
    Unchanged,
    Changed,
    Unverified,
}

fn scanner_cycle_activity_status(
    before: &crate::scanner::ScannerActivitySnapshot,
    observed: std::result::Result<crate::scanner::ScannerActivitySnapshot, String>,
    publication_blocked: bool,
    certification: Option<&ScannerRemoteDirtyUsageCertification>,
    certification_required: bool,
) -> (ScannerCycleActivityStatus, Vec<(String, String, u64)>) {
    match observed {
        Ok(after) => {
            let veto = if publication_blocked {
                Some(crate::scanner::ScannerActivityGuardrailVeto::new(
                    crate::scanner::LOCAL_SCANNER_ACTIVITY_NODE,
                    "publication_blocked",
                ))
            } else {
                crate::scanner::scanner_activity_guardrail_veto(before, &after)
            };
            let status = if let Some(veto) = veto {
                warn!(
                    target: "rustfs::scanner::io",
                    event = EVENT_SCANNER_SET_STATE,
                    component = LOG_COMPONENT_SCANNER,
                    subsystem = LOG_SUBSYSTEM_IO,
                    state = "cycle_guardrail_veto",
                    stage = "post_scan_probe",
                    field = veto.field,
                    host = %veto.host,
                    "Scanner cycle publication vetoed by the cluster activity guardrail"
                );
                global_metrics().record_scanner_guardrail_veto(veto.reason());
                ScannerCycleActivityStatus::Changed
            } else if certification_required && certification.is_none()
                || certification.is_some_and(|proof| !proof.matches_activity(&after))
            {
                ScannerCycleActivityStatus::Unverified
            } else if let Some(moved) =
                crate::scanner::scanner_peer_dirty_usage_moved(before, &after).filter(|_| certification.is_none())
            {
                // Without a complete producer proof, retain the original
                // whole-cycle fallback for a moving peer.
                tracing::info!(
                    target: "rustfs::scanner::io",
                    event = EVENT_SCANNER_SET_STATE,
                    component = LOG_COMPONENT_SCANNER,
                    subsystem = LOG_SUBSYSTEM_IO,
                    state = "cycle_peer_dirty_usage_moved",
                    stage = "post_scan_probe",
                    field = moved.field,
                    host = %moved.host,
                    "Scanner cycle superseded because a peer's dirty usage moved during the walk"
                );
                global_metrics().record_scanner_guardrail_veto(moved.reason());
                ScannerCycleActivityStatus::Changed
            } else {
                ScannerCycleActivityStatus::Unchanged
            };
            (status, crate::scanner::scanner_activity_publication_lease_targets(&after))
        }
        Err(err) => {
            warn!(
                target: "rustfs::scanner::io",
                event = EVENT_SCANNER_SET_STATE,
                component = LOG_COMPONENT_SCANNER,
                subsystem = LOG_SUBSYSTEM_IO,
                state = "cycle_activity_probe_failed",
                stage = "post_scan_probe",
                error = %err,
                "Scanner cycle activity verification failed"
            );
            (ScannerCycleActivityStatus::Unverified, Vec::new())
        }
    }
}

fn scanner_results_have_pending_maintenance_work(results: &[DataUsageCache]) -> bool {
    results.iter().any(|result| !result.info.pending_heals.is_empty())
}

fn pending_maintenance_work_for_cycle(pending: &AtomicBool, results: &[DataUsageCache]) -> bool {
    pending.load(Ordering::Acquire) || scanner_results_have_pending_maintenance_work(results)
}

fn record_bucket_pending_maintenance_work(cache: &DataUsageCache, pending: &AtomicBool) {
    if !cache.info.pending_heals.is_empty() {
        pending.store(true, Ordering::Release);
    }
}

fn is_xl_meta_path(path: &str) -> bool {
    Path::new(path)
        .file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| name == STORAGE_FORMAT_FILE)
}

pub(crate) fn cache_root_entry_info(cache: &DataUsageCache) -> std::result::Result<DataUsageEntryInfo, ScannerError> {
    if cache.info.name.is_empty() {
        return Err(ScannerError::Other("scanner cache root name is empty".to_string()));
    }
    let entry = cache
        .checked_flatten_complete_scope(&cache.info.name)
        .ok_or_else(|| ScannerError::Other(format!("scanner cache root is missing or corrupt: {}", cache.info.name)))?;

    Ok(DataUsageEntryInfo {
        name: clone_cache_value(&cache.info.name),
        parent: DATA_USAGE_ROOT.to_string(),
        entry,
        bucket_incarnation: cache
            .info
            .scan_identity
            .map(|identity| identity.bucket_incarnation)
            .filter(|incarnation| !incarnation.is_nil()),
        tier_registry_generation: cache.info.tier_registry_generation,
    })
}

fn apply_bucket_result_to_cache(
    cache: &mut DataUsageCache,
    result: DataUsageEntryInfo,
    update_time: SystemTime,
) -> Option<String> {
    if cache.info.tier_registry_generation != result.tier_registry_generation {
        // A result from another registry generation must never be folded into
        // this cycle. Leaving it unapplied makes the cycle incomplete and
        // forces the caller to re-account it under one frozen registry.
        warn!(
            target: "rustfs::scanner::io",
            event = EVENT_SCANNER_CACHE_PERSIST_STATE,
            component = LOG_COMPONENT_SCANNER,
            subsystem = LOG_SUBSYSTEM_IO,
            state = "bucket_result_rejected",
            bucket = %result.name,
            pool = cache.info.source.map(|source| source.pool_index),
            set = cache.info.source.map(|source| source.set_index),
            cycle = cache.info.next_cycle,
            expected_tier_registry_generation = cache.info.tier_registry_generation,
            result_tier_registry_generation = result.tier_registry_generation,
            "Scanner bucket result rejected because the tier registry generation differs"
        );
        return None;
    }
    match result.bucket_incarnation {
        Some(incarnation) if !incarnation.is_nil() => {
            cache
                .info
                .scan_bucket_incarnations
                .insert(clone_cache_value(&result.name), incarnation);
        }
        _ => {
            cache.info.scan_bucket_incarnations.remove(&result.name);
        }
    }
    cache.replace(&result.name, &result.parent, result.entry);
    cache.info.last_update = Some(update_time);
    Some(result.name)
}

fn should_publish_completed_snapshot(completed_count: usize, total_count: usize, budget_elapsed: bool, cancelled: bool) -> bool {
    completed_count == total_count && !budget_elapsed && !cancelled
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum NamespaceScannerWorkerMode {
    Coordinator,
    RemoteV4(uuid::Uuid),
}

fn namespace_scanner_workers<T>(
    coordinator_disks: Vec<T>,
    remote_disks: Vec<(T, uuid::Uuid)>,
) -> Vec<(T, NamespaceScannerWorkerMode)> {
    let mut workers = Vec::with_capacity(coordinator_disks.len() + remote_disks.len());
    workers.extend(
        coordinator_disks
            .into_iter()
            .map(|disk| (disk, NamespaceScannerWorkerMode::Coordinator)),
    );
    workers.extend(
        remote_disks
            .into_iter()
            .map(|(disk, server_epoch)| (disk, NamespaceScannerWorkerMode::RemoteV4(server_epoch))),
    );
    workers
}

struct NamespaceScannerWalker<T> {
    disk: T,
    mode: NamespaceScannerWorkerMode,
    /// Shared by every walker of one local disk; `None` for disks the
    /// coordinator reaches over the network, which keep today's one walker.
    local_admission: Option<Arc<crate::remote_scanner::LocalDiskAdmissionShare>>,
}

/// Expands each worker into its bucket walkers. A local coordinator disk gets
/// `walkers_per_disk` walkers sharing one disk admission; every other worker
/// keeps a single walker, so the walker count never drops below the disk count.
fn namespace_scanner_walkers<T: Clone>(
    workers: Vec<(T, NamespaceScannerWorkerMode)>,
    walkers_per_disk: usize,
    is_local: impl Fn(&T) -> bool,
) -> Vec<NamespaceScannerWalker<T>> {
    let mut walkers = Vec::with_capacity(workers.len());
    for (disk, mode) in workers {
        if mode == NamespaceScannerWorkerMode::Coordinator && is_local(&disk) {
            let local_admission = crate::remote_scanner::LocalDiskAdmissionShare::new();
            walkers.extend((0..walkers_per_disk.max(1)).map(|_| NamespaceScannerWalker {
                disk: disk.clone(),
                mode,
                local_admission: Some(local_admission.clone()),
            }));
        } else {
            walkers.push(NamespaceScannerWalker {
                disk,
                mode,
                local_admission: None,
            });
        }
    }
    walkers
}

fn group_remote_disks_by_peer<T>(disks: Vec<T>, peer_key: impl Fn(&T) -> String) -> Vec<Vec<T>> {
    let mut groups = HashMap::<String, Vec<T>>::new();
    for disk in disks {
        groups.entry(peer_key(&disk)).or_default().push(disk);
    }
    groups.into_values().collect()
}

fn scanner_results_match_scan_scope(results: &[DataUsageCache], expected_sources: &HashSet<DataUsageCacheSource>) -> bool {
    if results.is_empty() {
        return false;
    }

    let sources_match_topology = results
        .iter()
        .map(|result| result.info.source)
        .collect::<Option<HashSet<_>>>()
        .is_some_and(|sources| sources.len() == results.len() && sources == *expected_sources);
    let plan_digests = results
        .iter()
        .map(|result| result.info.scan_plan_digest)
        .collect::<Option<HashSet<_>>>();
    let cycles = results.iter().map(|result| result.info.next_cycle).collect::<HashSet<_>>();
    let leader_epochs = results.iter().map(|result| result.info.leader_epoch).collect::<HashSet<_>>();

    sources_match_topology
        && plan_digests.is_some_and(|digests| digests.len() == 1)
        && cycles.len() == 1
        && leader_epochs.len() == 1
}

fn scanner_results_form_complete_snapshot(results: &[DataUsageCache], expected_sources: &HashSet<DataUsageCacheSource>) -> bool {
    results
        .iter()
        .all(|result| result.info.last_update.is_some() && result.info.snapshot_complete)
        && scanner_results_match_scan_scope(results, expected_sources)
}

fn checked_bucket_usage_info(entry: DataUsageEntry) -> Option<BucketUsageInfo> {
    let mut usage = BucketUsageInfo {
        size: u64::try_from(entry.size).ok()?,
        versions_count: u64::try_from(entry.versions).ok()?,
        objects_count: u64::try_from(entry.objects).ok()?,
        delete_markers_count: u64::try_from(entry.delete_markers).ok()?,
        object_size_histogram: entry.obj_sizes.to_map(),
        object_versions_histogram: entry.obj_versions.to_map(),
        ..Default::default()
    };

    if let Some(replication) = entry.replication_stats {
        usage.replica_size = replication.replica_size;
        usage.replica_count = replication.replica_count;
        for (target, stats) in replication.targets {
            usage.replication_info.insert(
                target,
                BucketTargetUsageInfo {
                    replication_pending_size: stats.pending_size,
                    replicated_size: stats.replicated_size,
                    replication_failed_size: stats.failed_size,
                    replication_pending_count: stats.pending_count,
                    replication_failed_count: stats.failed_count,
                    replicated_count: stats.replicated_count,
                    ..Default::default()
                },
            );
        }
    }
    Some(usage)
}

#[async_trait::async_trait]
pub trait ScannerIO: Send + Sync + Debug + 'static {
    async fn nsscanner(
        &self,
        ctx: CancellationToken,
        budget: Arc<ScannerCycleBudget>,
        updates: mpsc::Sender<DataUsageInfo>,
        want_cycle: u64,
        scan_mode: HealScanMode,
    ) -> Result<()>;
}

#[async_trait::async_trait]
pub(crate) trait ScannerIOCycle: Send + Sync + Debug + 'static {
    async fn nsscanner_with_status(
        &self,
        ctx: CancellationToken,
        budget: Arc<ScannerCycleBudget>,
        updates: mpsc::Sender<DataUsageInfo>,
        want_cycle: u64,
        leader_epoch: u64,
        scan_mode: HealScanMode,
    ) -> Result<ScannerCycleResult>;
}

#[async_trait::async_trait]
pub trait ScannerIOCache: Send + Sync + Debug + 'static {
    async fn nsscanner_cache(
        self: Arc<Self>,
        ctx: CancellationToken,
        budget: Arc<ScannerCycleBudget>,
        scan_plan: ScannerBucketScanPlan,
        updates: mpsc::Sender<DataUsageCache>,
        want_cycle: u64,
        scan_mode: HealScanMode,
    ) -> Result<()>;
}

#[derive(Debug)]
pub struct ScannerDiskScanOptions {
    pub scan_mode: HealScanMode,
    pub prefix_scan_scope: Option<ScannerBucketPrefixScanScope>,
    pub checkpoint_tx: Option<mpsc::Sender<DataUsageCache>>,
}

#[async_trait::async_trait]
pub trait ScannerIODisk: Send + Sync + Debug + 'static {
    async fn nsscanner_disk(
        self: Arc<Self>,
        ctx: CancellationToken,
        budget: Arc<ScannerCycleBudget>,
        set_disks: Vec<Arc<Disk>>,
        cache: DataUsageCache,
        updates: Option<mpsc::Sender<DataUsageEntry>>,
        options: ScannerDiskScanOptions,
    ) -> Result<ScannerDiskScanOutcome>;

    async fn get_size(&self, item: ScannerItem) -> Result<SizeSummary>;

    /// Read one object using a registry snapshot captured at scan start.
    async fn get_size_with_tier_names(&self, item: ScannerItem, tier_names: &[String]) -> Result<SizeSummary>;
}

#[derive(Debug)]
pub enum ScannerDiskScanOutcome {
    Complete(DataUsageCache),
    Partial(DataUsageCache),
    NamespaceNotFound(DataUsageCache),
}

pub(crate) async fn scanner_set_disk_inventory(set: &SetDisks) -> Vec<Arc<Disk>> {
    let membership = set.drive_membership_snapshot().await;
    let capacity = membership
        .online
        .len()
        .saturating_add(membership.suspect.len())
        .saturating_add(membership.returning.len())
        .saturating_add(membership.offline.len());
    let mut disks = Vec::with_capacity(capacity);
    disks.extend(membership.online);
    disks.extend(membership.suspect);
    disks.extend(membership.returning);
    disks.extend(membership.offline);
    disks
}

pub(crate) async fn scanner_bucket_checkpoint_identity(
    set: &SetDisks,
    bucket: &str,
    publication_epoch: u64,
    tier_registry_generation: u64,
    scan_mode: HealScanMode,
) -> Result<Option<crate::DataUsageScanIdentity>> {
    let Some(bucket_incarnation) = set.find_bucket_incarnation_id_from_disk(bucket).await? else {
        return Ok(None);
    };
    let disks = set
        .format
        .erasure
        .sets
        .get(set.set_index)
        .filter(|disks| !disks.is_empty())
        .ok_or_else(|| Error::other("scanner checkpoint set layout is absent"))?;
    if set.format.id.is_nil() || disks.iter().any(uuid::Uuid::is_nil) {
        return Err(Error::other("scanner checkpoint set layout has a nil identity"));
    }
    let mut digest = Sha256::new();
    digest.update(set.format.id.as_bytes());
    for disk in disks {
        digest.update(disk.as_bytes());
    }
    Ok(Some(crate::DataUsageScanIdentity {
        version: 1,
        bucket_incarnation,
        set_layout: crate::DataUsageScanPlanDigest(digest.finalize().into()),
        publication_epoch,
        tier_registry_generation,
        scan_mode,
    }))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ScannerCycleDeferReason {
    ActivityBaselineUnavailable,
    DataMovement,
    /// A granted lease's absolute deadline cannot cover the persistence
    /// operation. This can occur even when the configured budget fits the
    /// nominal TTL because lease acquisition consumed part of the window.
    PublicationLeaseDeadlineExceeded,
    /// A remote lease could not be released after the persistence attempt.
    /// Keep the cycle deferred because the peer may still admit movement.
    PublicationLeaseReleaseFailed,
}

impl ScannerCycleDeferReason {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::ActivityBaselineUnavailable => "activity_baseline_unavailable",
            Self::DataMovement => "data_movement",
            Self::PublicationLeaseDeadlineExceeded => "publication_lease_deadline_exceeded",
            Self::PublicationLeaseReleaseFailed => "publication_lease_release_failed",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ScannerCycleStatus {
    Complete,
    Incomplete,
    Superseded,
    Deferred(ScannerCycleDeferReason),
}

enum ScannerActivityPreflight {
    Ready(crate::scanner::ScannerActivitySnapshot),
    ActivityBaselineUnavailable(String),
    DataMovement,
}

fn scanner_activity_preflight(
    activity: std::result::Result<crate::scanner::ScannerActivitySnapshot, String>,
) -> ScannerActivityPreflight {
    match activity {
        Err(error) => ScannerActivityPreflight::ActivityBaselineUnavailable(error),
        Ok(snapshot) if !crate::scanner::scanner_activity_allows_usage_publication(&snapshot) => {
            ScannerActivityPreflight::DataMovement
        }
        Ok(snapshot) => ScannerActivityPreflight::Ready(snapshot),
    }
}

#[derive(Debug)]
pub(crate) struct ScannerCycleResult {
    pub(crate) status: ScannerCycleStatus,
    publication_epoch: Option<u64>,
    activity_digest: Option<[u8; 32]>,
    activity_baseline: Option<Arc<crate::scanner::ScannerActivitySnapshot>>,
    pending_dirty_usage_buckets: usize,
    usage_update_emitted: bool,
    partial_bucket_progress: bool,
    dirty_usage_clear: Option<DirtyUsageBuckets>,
    remote_dirty_usage_acknowledgements: Vec<crate::scanner::ScannerDirtyUsageAcknowledgement>,
    remote_dirty_usage_certification: Option<ScannerRemoteDirtyUsageCertification>,
    remote_dirty_usage_pending: bool,
    distributed_segment_invalidation_evidence: Option<DistributedSegmentInvalidationEvidence>,
    segment_reuse_activation_preflight: ScannerSegmentReuseActivationPreflight,
    remote_publication_lease_targets: Vec<(String, String, u64)>,
    failed_dirty_usage: bool,
    pending_maintenance_work: bool,
    required_cycle_floor: Option<u64>,
    scope_fallback: Option<ScannerScopeFallbackReason>,
    publication_expectation: Option<ScannerPublicationExpectation>,
}

impl ScannerCycleResult {
    pub(crate) fn new(status: ScannerCycleStatus, dirty_usage_clear: Option<DirtyUsageBuckets>) -> Self {
        Self {
            status,
            publication_epoch: None,
            activity_digest: None,
            activity_baseline: None,
            pending_dirty_usage_buckets: 0,
            usage_update_emitted: false,
            partial_bucket_progress: false,
            dirty_usage_clear,
            remote_dirty_usage_acknowledgements: Vec::new(),
            remote_dirty_usage_certification: None,
            remote_dirty_usage_pending: false,
            distributed_segment_invalidation_evidence: None,
            segment_reuse_activation_preflight: scanner_segment_reuse_activation_preflight(),
            remote_publication_lease_targets: Vec::new(),
            failed_dirty_usage: false,
            pending_maintenance_work: false,
            required_cycle_floor: None,
            scope_fallback: None,
            publication_expectation: None,
        }
    }

    pub(crate) fn with_publication_epoch(mut self, publication_epoch: Option<u64>) -> Self {
        self.publication_expectation = None;
        self.publication_epoch = publication_epoch;
        self
    }

    pub(crate) fn publication_epoch(&self) -> Option<u64> {
        self.publication_epoch
    }

    fn with_activity_digest(mut self, activity_digest: [u8; 32]) -> Self {
        self.publication_expectation = None;
        self.activity_digest = Some(activity_digest);
        self
    }

    #[cfg(test)]
    pub(crate) fn activity_digest(&self) -> Option<[u8; 32]> {
        self.activity_digest
    }

    fn with_activity_baseline(mut self, baseline: Arc<crate::scanner::ScannerActivitySnapshot>) -> Self {
        self.publication_expectation = None;
        self.activity_baseline = Some(baseline);
        self
    }

    /// The pre-walk activity probe the post-lease guardrail recheck compares
    /// against.
    pub(crate) fn activity_baseline(&self) -> Option<&crate::scanner::ScannerActivitySnapshot> {
        self.activity_baseline.as_deref()
    }

    fn with_pending_dirty_usage_buckets(mut self, pending: usize) -> Self {
        self.publication_expectation = None;
        self.pending_dirty_usage_buckets = pending;
        self
    }

    /// Buckets a `Complete` cycle left pending for the next cycle.
    pub(crate) fn pending_dirty_usage_buckets(&self) -> usize {
        self.pending_dirty_usage_buckets
    }

    pub(crate) fn with_usage_update_emitted(mut self, emitted: bool) -> Self {
        self.publication_expectation = None;
        self.usage_update_emitted = emitted;
        self
    }

    pub(crate) fn has_usage_update(&self) -> bool {
        self.usage_update_emitted
    }

    pub(crate) fn with_partial_bucket_progress(mut self, partial_bucket_progress: bool) -> Self {
        self.publication_expectation = None;
        self.partial_bucket_progress = partial_bucket_progress;
        self
    }

    pub(crate) fn has_partial_bucket_progress(&self) -> bool {
        self.partial_bucket_progress
    }

    fn with_failed_dirty_usage(mut self, failed_dirty_usage: bool) -> Self {
        self.publication_expectation = None;
        self.failed_dirty_usage = failed_dirty_usage;
        self
    }

    fn with_pending_maintenance_work(mut self, pending_maintenance_work: bool) -> Self {
        self.publication_expectation = None;
        self.pending_maintenance_work = pending_maintenance_work;
        self
    }

    fn with_scope_fallback(mut self, scope_fallback: Option<ScannerScopeFallbackReason>) -> Self {
        self.publication_expectation = None;
        self.scope_fallback = scope_fallback;
        self
    }

    /// Why this cycle walked the whole namespace although a dirty-bucket
    /// scope was eligible.
    #[cfg(test)]
    pub(crate) fn scope_fallback(&self) -> Option<ScannerScopeFallbackReason> {
        self.scope_fallback
    }

    /// A `Complete` full-namespace fallback: while dirty buckets remain, the
    /// next cycle must not run under the scoped cycle's immediate wake.
    pub(crate) fn completed_scope_fallback(&self) -> Option<ScannerScopeFallbackReason> {
        (self.status == ScannerCycleStatus::Complete)
            .then_some(self.scope_fallback)
            .flatten()
    }

    fn with_required_cycle_floor(mut self, required_cycle_floor: Option<u64>) -> Self {
        self.publication_expectation = None;
        self.required_cycle_floor = required_cycle_floor;
        self
    }

    pub(crate) fn with_remote_dirty_usage_acknowledgements(
        mut self,
        acknowledgements: Vec<crate::scanner::ScannerDirtyUsageAcknowledgement>,
    ) -> Self {
        self.publication_expectation = None;
        self.remote_dirty_usage_acknowledgements = acknowledgements;
        self
    }

    fn with_remote_dirty_usage_certification(mut self, certification: Option<ScannerRemoteDirtyUsageCertification>) -> Self {
        self.publication_expectation = None;
        self.remote_dirty_usage_certification = certification;
        self
    }

    pub(crate) fn remote_dirty_usage_certification(&self) -> Option<&ScannerRemoteDirtyUsageCertification> {
        self.remote_dirty_usage_certification.as_ref()
    }

    pub(crate) fn with_remote_dirty_usage_pending(mut self, pending: bool) -> Self {
        self.publication_expectation = None;
        self.remote_dirty_usage_pending = pending;
        self
    }

    pub(crate) fn has_remote_dirty_usage_pending(&self) -> bool {
        self.remote_dirty_usage_pending
    }

    fn with_distributed_segment_invalidation_evidence(
        mut self,
        evidence: Option<DistributedSegmentInvalidationEvidence>,
    ) -> Self {
        self.publication_expectation = None;
        self.distributed_segment_invalidation_evidence = evidence;
        self
    }

    fn with_segment_reuse_activation_preflight(mut self, preflight: ScannerSegmentReuseActivationPreflight) -> Self {
        self.publication_expectation = None;
        self.segment_reuse_activation_preflight = preflight;
        self
    }

    pub(crate) fn with_remote_publication_lease_targets(mut self, targets: Vec<(String, String, u64)>) -> Self {
        self.publication_expectation = None;
        self.remote_publication_lease_targets = targets;
        self
    }

    pub(crate) fn remote_publication_lease_targets(&self) -> &[(String, String, u64)] {
        &self.remote_publication_lease_targets
    }

    pub(crate) fn publication_expectation(&self) -> Option<ScannerPublicationExpectation> {
        self.publication_expectation.clone()
    }

    fn with_publication_expectation(mut self, expectation: Option<ScannerPublicationExpectation>) -> Self {
        // Seal only after all coverage and acknowledgement inputs are final.
        self.publication_expectation = expectation;
        self
    }

    pub(crate) fn acknowledge_durable_usage(
        self,
        proof: &crate::scanner::RootPublicationProof,
    ) -> Vec<crate::scanner::ScannerDirtyUsageAcknowledgement> {
        if self.status != ScannerCycleStatus::Complete
            || self
                .publication_expectation
                .as_ref()
                .is_none_or(|expected| proof.verified_version_for(expected).is_none())
        {
            return Vec::new();
        }
        self.clear_verified_usage()
    }

    fn clear_verified_usage(self) -> Vec<crate::scanner::ScannerDirtyUsageAcknowledgement> {
        if let Some(snapshot) = self.dirty_usage_clear {
            clear_dirty_usage_buckets(&snapshot);
        }
        self.remote_dirty_usage_acknowledgements
    }

    pub(crate) fn has_dirty_usage_to_acknowledge(&self) -> bool {
        self.dirty_usage_clear.as_ref().is_some_and(|snapshot| !snapshot.is_empty())
            || !self.remote_dirty_usage_acknowledgements.is_empty()
    }

    pub(crate) fn has_failed_dirty_usage(&self) -> bool {
        self.failed_dirty_usage
    }

    pub(crate) fn has_pending_maintenance_work(&self) -> bool {
        self.pending_maintenance_work
    }

    pub(crate) fn required_cycle_floor(&self) -> Option<u64> {
        self.required_cycle_floor
    }
}

mod cache;
mod dirty_usage;
mod guards;
pub(crate) use guards::ScannerServiceCohort;
mod io_cache;
mod io_cycle;
mod peer_certification;
#[cfg(test)]
use io_cache::{ScannerSetCacheGeneration, prepare_scoped_set_scan};
pub(crate) use io_cycle::{ScannerCycleRequest, nsscanner_with_storage_status_scoped};
pub(crate) use peer_certification::ScannerRemoteDirtyUsageCertification;
use peer_certification::{ScannerBucketCompletions, ScannerRemoteDirtyUsageWindow};
mod io_disk;
#[cfg(test)]
mod publish_gate_tests;
#[cfg(test)]
mod tests;

pub(crate) use cache::ScannerPublicationExpectation;
use cache::*;
use dirty_usage::*;
use guards::*;

pub(crate) use cache::{
    DataUsageCacheReuseOptions, DataUsageCacheScanState, ScannerCheckpointPersistContext, ScannerCheckpointPersistResult,
    acquire_scanner_cache_locks, current_cache_root_or_prepare_with_generation, log_saved_partial_bucket_cache,
    persist_scanner_checkpoint,
};
pub use dirty_usage::{
    ScannerDirtyUsageAckError, ScannerDirtyUsageBucket, ScannerDirtyUsageClearObserver, ScannerDirtyUsageMutationObserver,
    ScannerDirtyUsageSnapshot, ScannerDirtyUsageState, ScannerDurableDirtyUsageReplayEntry, ScannerDurableDirtyUsageReplayError,
    ScannerDurableDirtyUsageReplayRecord, ScannerDurableDirtyUsageReplayScope, acknowledge_dirty_usage_generation,
    acknowledge_scoped_dirty_usage, clear_dirty_usage_bucket, encode_durable_dirty_usage_producer_replay_record,
    record_dirty_usage_bucket, record_dirty_usage_bucket_from_producer, record_dirty_usage_bucket_from_producers,
    record_dirty_usage_object, record_dirty_usage_object_from_producer, record_scanner_maintenance_change,
    record_status_class_mutation, replay_durable_dirty_usage_producer_record, scanner_activity_epoch,
    scanner_dirty_usage_bucket_generation, scanner_dirty_usage_snapshot, scanner_dirty_usage_state,
    scanner_maintenance_generation, set_scanner_dirty_usage_clear_observer, set_scanner_dirty_usage_mutation_observer,
};
#[cfg(test)]
pub(crate) use dirty_usage::{clear_dirty_usage_buckets_for_tests, dirty_usage_buckets_for_tests};
pub(crate) use dirty_usage::{
    dirty_usage_bucket_notified, dirty_usage_buckets_pending, dirty_usage_generation, scanner_maintenance_changed,
};
