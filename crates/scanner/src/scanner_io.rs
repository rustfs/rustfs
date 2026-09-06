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
use rustfs_config::{ENV_SCANNER_MAX_CONCURRENT_DISK_SCANS, ENV_SCANNER_MAX_CONCURRENT_SET_SCANS};
use rustfs_data_usage::{BucketTargetUsageInfo, BucketUsageInfo, observed_data_usage_is_newer};
use rustfs_filemeta::FileMeta;
use rustfs_heal_contracts::heal_channel::HealScanMode;
use rustfs_lock::{LockError, NamespaceLockGuard};
use rustfs_scanner_metrics::metrics::{
    Metric, Metrics, emit_scan_bucket_drive_complete, emit_scan_bucket_drive_partial, global_metrics,
};
use rustfs_utils::path::path_join_buf;
use s3s::dto::{
    BucketLifecycleConfiguration, ObjectLockConfiguration, ObjectLockEnabled, ReplicationConfiguration, VersioningConfiguration,
};
use sha2::{Digest as _, Sha256};
use std::collections::{HashMap, HashSet};
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

pub type DirtyUsageBuckets = HashMap<String, u64>;

#[derive(Clone, Debug)]
struct DirtyUsageSnapshot {
    buckets: Arc<DirtyUsageBuckets>,
    scopes: Arc<DirtyUsageBucketScopes>,
    generation: u64,
    covers_all_pending: bool,
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

    (received_peers.len() == expected_peers.len()).then_some(VerifiedRemoteDirtyUsage {
        dirty_buckets,
        acknowledgements,
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

fn complete_scanner_cache_snapshot_plan_digest(
    snapshot: &DataUsageInfo,
    proof: ScannerCacheBaselineProof<'_>,
    expected_converged: bool,
) -> Option<DataUsageScanPlanDigest> {
    if !snapshot.is_complete_bucket_usage_snapshot()
        || snapshot.usage_snapshot_partial
        || snapshot.usage_snapshot_converged != Some(expected_converged)
        || snapshot.scanner_epoch != Some(proof.leader_epoch)
        || snapshot.usage_snapshot_set_states.len() != proof.expected_sources.len()
    {
        return None;
    }

    // Completed maintenance also covers ordinary usage. Keep its exact stored
    // proof for cache reuse, and reject mixtures of different set work proofs.
    let baseline_plan_digest = DataUsageScanPlanDigest(snapshot.usage_snapshot_set_states.first()?.scan_plan_digest?);
    if ![
        proof.scan_plan_digest,
        scanner_bucket_work_digest(proof.scan_plan_digest, HealScanMode::Normal, true),
        scanner_bucket_work_digest(proof.scan_plan_digest, HealScanMode::Deep, true),
    ]
    .contains(&baseline_plan_digest)
    {
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

fn complete_scanner_cache_baseline_plan_digest(proof: ScannerCacheBaselineProof<'_>) -> Option<DataUsageScanPlanDigest> {
    let authoritative = serde_json::from_slice::<DataUsageInfo>(proof.authoritative_data?).ok()?;
    if let Some(validated_digest) = complete_scanner_cache_snapshot_plan_digest(&authoritative, proof, true) {
        return Some(validated_digest);
    }

    // A complete but superseded observation may reuse its per-set cache only
    // when it was explicitly tied to the durable authoritative baseline. It
    // remains observational: this proof grants bucket-scope reuse only and
    // never changes authoritative usage publication or dirty acknowledgement.
    let authoritative_has_identity = (crate::scanner::data_usage_info_has_persisted_baseline_identity(&authoritative)
        && authoritative.usage_snapshot_converged != Some(false))
        || crate::scanner::data_usage_info_is_bootstrap_pending(&authoritative);
    if !authoritative_has_identity {
        return None;
    }
    let observed = serde_json::from_slice::<DataUsageInfo>(proof.observed_candidate_data?).ok()?;
    if !observed_data_usage_is_newer(&observed, &authoritative) {
        return None;
    }

    complete_scanner_cache_snapshot_plan_digest(&observed, proof, false)
}

fn scoped_scan_scope_from_dirty_buckets(
    requested_scope: ScannerBucketScanScope,
    dirty_buckets: HashSet<String>,
    dirty_scopes: Option<&DirtyUsageBucketScopes>,
    dirty_snapshot_complete: bool,
    all_buckets: &[BucketInfo],
    baseline_proof: ScannerCacheBaselineProof<'_>,
) -> ScannerBucketScanScope {
    if !requested_scope.is_default() || !dirty_snapshot_complete {
        return requested_scope;
    }

    let current_buckets = all_buckets.iter().map(|bucket| bucket.name.as_str()).collect::<HashSet<_>>();
    let selected_buckets = dirty_buckets
        .into_iter()
        .filter(|bucket| current_buckets.contains(bucket.as_str()))
        .collect::<HashSet<_>>();
    if selected_buckets.is_empty() {
        return requested_scope;
    }

    let Some(baseline_scan_plan_digest) = complete_scanner_cache_baseline_plan_digest(baseline_proof) else {
        return requested_scope;
    };

    let selected_bucket_prefixes = dirty_scopes
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
        .collect();

    ScannerBucketScanScope::from_dirty_buckets(selected_buckets, selected_bucket_prefixes, baseline_scan_plan_digest)
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
        .is_some_and(|enabled| enabled.as_str() == ObjectLockEnabled::ENABLED)
}

pub struct ScannerBucketScanPlan {
    buckets: Vec<BucketInfo>,
    all_buckets: Arc<Vec<BucketInfo>>,
    scope: ScannerBucketScanScope,
    digest: DataUsageScanPlanDigest,
    /// Includes mutation generations even when the set planner uses a structural digest.
    bucket_coverage_digest: DataUsageScanPlanDigest,
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
    pending_maintenance_work: Arc<AtomicBool>,
    cache_cycle_floor: Arc<AtomicU64>,
}

#[derive(Clone, Default)]
struct ScannerBucketFailureState {
    hard: Arc<Mutex<HashSet<String>>>,
    partial: Arc<Mutex<HashSet<String>>>,
    namespace_not_found: Arc<Mutex<HashSet<String>>>,
}

fn scanner_bucket_plan_digest(buckets: &[BucketInfo], activity_digest: [u8; 32]) -> DataUsageScanPlanDigest {
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
    Partial,
    NamespaceNotFound,
}

fn scanner_bucket_scan_status(has_failed: bool, has_partial: bool, has_namespace_not_found: bool) -> ScannerBucketScanStatus {
    if has_failed {
        ScannerBucketScanStatus::Failed
    } else if has_partial {
        ScannerBucketScanStatus::Partial
    } else if has_namespace_not_found {
        ScannerBucketScanStatus::NamespaceNotFound
    } else {
        ScannerBucketScanStatus::Complete
    }
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
    if budget_elapsed
        || cancelled
        || !matches!(bucket_scan_status, ScannerBucketScanStatus::Complete)
        || dirty_usage_status == DirtyUsageSnapshotStatus::Unverified
    {
        return ScannerCycleStatus::Incomplete;
    }
    if !completed_all_sets {
        return ScannerCycleStatus::Incomplete;
    }

    match (activity_status, dirty_usage_status) {
        (ScannerCycleActivityStatus::Unchanged, DirtyUsageSnapshotStatus::Current) => ScannerCycleStatus::Complete,
        _ => ScannerCycleStatus::Superseded,
    }
}

fn should_publish_usage_snapshot(status: ScannerCycleStatus) -> bool {
    matches!(status, ScannerCycleStatus::Complete | ScannerCycleStatus::Superseded)
}

fn should_publish_observational_snapshot(status: ScannerCycleStatus) -> bool {
    matches!(status, ScannerCycleStatus::Deferred(ScannerCycleDeferReason::ActivityBaselineUnavailable))
}

fn prepare_usage_snapshot_for_publication(
    status: ScannerCycleStatus,
    mut data_usage_info: DataUsageInfo,
) -> Option<DataUsageInfo> {
    if !should_publish_usage_snapshot(status) {
        return None;
    }

    data_usage_info.usage_snapshot_converged = Some(status == ScannerCycleStatus::Complete);
    Some(data_usage_info)
}

async fn publish_usage_snapshot(
    updates: &mpsc::Sender<DataUsageInfo>,
    status: ScannerCycleStatus,
    data_usage_info: DataUsageInfo,
) -> Result<bool> {
    let Some(data_usage_info) = prepare_usage_snapshot_for_publication(status, data_usage_info) else {
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
    send_data_usage_update(updates, data_usage_info).await?;
    Ok(true)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ScannerCycleActivityStatus {
    Unchanged,
    Changed,
    Unverified,
}

async fn scanner_cycle_activity_status<S>(
    store: &S,
    distributed: bool,
    before: &crate::scanner::ScannerActivitySnapshot,
) -> (ScannerCycleActivityStatus, Vec<(String, String, u64)>)
where
    S: ScannerStorage,
{
    // Read the pending-commit barrier before sampling its completion generation.
    // A tail that drains during this await must invalidate the earlier baseline.
    let publication_blocked = store.scanner_data_usage_publication_blocked().await;
    match crate::scanner::probe_scanner_activity(store, distributed).await {
        Ok(after) => {
            let status = if !publication_blocked && after == *before {
                ScannerCycleActivityStatus::Unchanged
            } else {
                ScannerCycleActivityStatus::Changed
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
        name: cache.info.name.clone(),
        parent: DATA_USAGE_ROOT.to_string(),
        entry,
        tier_registry_generation: cache.info.tier_registry_generation,
    })
}

fn apply_bucket_result_to_cache(cache: &mut DataUsageCache, result: DataUsageEntryInfo, update_time: SystemTime) -> bool {
    if cache.info.tier_registry_generation != result.tier_registry_generation {
        // A result from another registry generation must never be folded into
        // this cycle. Leaving it unapplied makes the cycle incomplete and
        // forces the caller to re-account it under one frozen registry.
        return false;
    }
    cache.replace(&result.name, &result.parent, result.entry);
    cache.info.last_update = Some(update_time);
    true
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

fn checked_bucket_usage_info(entry: &DataUsageEntry) -> Option<BucketUsageInfo> {
    let mut usage = BucketUsageInfo {
        size: u64::try_from(entry.size).ok()?,
        versions_count: u64::try_from(entry.versions).ok()?,
        objects_count: u64::try_from(entry.objects).ok()?,
        delete_markers_count: u64::try_from(entry.delete_markers).ok()?,
        object_size_histogram: entry.obj_sizes.to_map(),
        object_versions_histogram: entry.obj_versions.to_map(),
        ..Default::default()
    };

    if let Some(replication) = &entry.replication_stats {
        usage.replica_size = replication.replica_size;
        usage.replica_count = replication.replica_count;
        for (target, stats) in &replication.targets {
            usage.replication_info.insert(
                target.clone(),
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
) -> Result<crate::DataUsageScanIdentity> {
    let bucket_incarnation = set.bucket_incarnation_id_from_disk(bucket).await?;
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
    Ok(crate::DataUsageScanIdentity {
        version: 1,
        bucket_incarnation,
        set_layout: crate::DataUsageScanPlanDigest(digest.finalize().into()),
        publication_epoch,
        tier_registry_generation,
        scan_mode,
    })
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
    observational_snapshot_published: bool,
    dirty_usage_clear: Option<DirtyUsageBuckets>,
    remote_dirty_usage_acknowledgements: Vec<crate::scanner::ScannerDirtyUsageAcknowledgement>,
    remote_publication_lease_targets: Vec<(String, String, u64)>,
    failed_dirty_usage: bool,
    pending_maintenance_work: bool,
    required_cycle_floor: Option<u64>,
    publication_expectation: Option<ScannerPublicationExpectation>,
}

impl ScannerCycleResult {
    pub(crate) fn new(status: ScannerCycleStatus, dirty_usage_clear: Option<DirtyUsageBuckets>) -> Self {
        Self {
            status,
            publication_epoch: None,
            activity_digest: None,
            observational_snapshot_published: false,
            dirty_usage_clear,
            remote_dirty_usage_acknowledgements: Vec::new(),
            remote_publication_lease_targets: Vec::new(),
            failed_dirty_usage: false,
            pending_maintenance_work: false,
            required_cycle_floor: None,
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

    pub(crate) fn activity_digest(&self) -> Option<[u8; 32]> {
        self.activity_digest
    }

    pub(crate) fn with_observational_snapshot_published(mut self, published: bool) -> Self {
        self.publication_expectation = None;
        self.observational_snapshot_published = published;
        self
    }

    pub(crate) fn has_observational_snapshot(&self) -> bool {
        self.observational_snapshot_published
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
#[cfg(test)]
use io_cache::{ScannerSetCacheGeneration, prepare_scoped_set_scan};
pub(crate) use io_cycle::{ScannerCycleRequest, nsscanner_with_storage_status_scoped};
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
    DataUsageCacheReuseOptions, DataUsageCacheScanState, acquire_scanner_cache_locks,
    current_cache_root_or_prepare_with_generation,
};
pub use dirty_usage::{
    ScannerDirtyUsageAckError, ScannerDirtyUsageBucket, ScannerDirtyUsageSnapshot, ScannerDirtyUsageState,
    acknowledge_dirty_usage_generation, acknowledge_scoped_dirty_usage, clear_dirty_usage_bucket, record_dirty_usage_bucket,
    record_dirty_usage_object, record_scanner_maintenance_change, scanner_activity_epoch, scanner_dirty_usage_snapshot,
    scanner_dirty_usage_state, scanner_maintenance_generation,
};
#[cfg(test)]
pub(crate) use dirty_usage::{clear_dirty_usage_buckets_for_tests, dirty_usage_buckets_for_tests};
pub(crate) use dirty_usage::{
    dirty_usage_bucket_notified, dirty_usage_buckets_pending, dirty_usage_generation, scanner_maintenance_changed,
};
