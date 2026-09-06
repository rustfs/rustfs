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

use crate::bucket::lifecycle::tier_last_day_stats::DailyAllTierStats;
use crate::cluster::rpc::{
    PeerRestClient, ScannerDirtyUsageAcknowledgement, ScannerPeerActivity, ScannerPeerDirtyUsageSnapshot,
    ScannerPublicationLease, TierConfigReloadOutcome,
};
use crate::diagnostics::admin_server_info::get_commit_id;
use crate::disk::DiskAPI;
use crate::error::{Error, Result};
use crate::layout::endpoints::EndpointServerPools;
use crate::runtime::sources as runtime_sources;
use crate::services::metrics_realtime::{CollectMetricsOpts, MetricType};
use crate::services::rebalance::RebalSaveOpt;
use crate::storage_api_contracts::admin::StorageAdminApi;
use bytes::Bytes;
use futures::future::join_all;
use lazy_static::lazy_static;
use rustfs_madmin::health::{Cpus, MemInfo, OsInfo, Partitions, ProcInfo, SysConfig, SysErrors, SysServices};
use rustfs_madmin::metrics::RealtimeMetrics;
use rustfs_madmin::net::NetInfo;
use rustfs_madmin::{ItemState, ServerProperties, StorageInfo};
use rustfs_utils::XHost;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet, HashMap, hash_map::DefaultHasher};
use std::future::Future;
use std::hash::{Hash, Hasher};
use std::sync::{
    Arc, LazyLock, Mutex, OnceLock,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::time::{Duration, Instant, SystemTime};
use tokio::time::{sleep, timeout};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

/// After this many consecutive admin-call failures, mark the peer as offline.
const CONSECUTIVE_FAILURE_THRESHOLD: u32 = 3;
const LOG_COMPONENT_ECSTORE: &str = "ecstore";
const LOG_SUBSYSTEM_NOTIFICATION: &str = "notification";
const EVENT_NOTIFICATION_PEER_PROPAGATION: &str = "notification_peer_propagation";
const EVENT_NOTIFICATION_CAPABILITY_PROBE: &str = "notification_capability_probe";
const SCANNER_ACTIVITY_PROBE_TIMEOUT: Duration = Duration::from_secs(5);
const TIER_DAILY_STATS_PROBE_TIMEOUT: Duration = Duration::from_secs(5);
const TIER_CONFIG_RELOAD_RETRY_BASE: Duration = Duration::from_millis(100);
const TIER_CONFIG_RELOAD_RETRY_CAP: Duration = Duration::from_secs(5);
const REMOTE_VERSION_STATE_PROBE_INTERVAL: Duration = Duration::from_secs(10);
const REMOTE_VERSION_STATE_PROBE_TIMEOUT: Duration = Duration::from_secs(5);
const REMOTE_VERSION_STATE_PROOF_TTL: Duration = Duration::from_secs(30);
const CROSS_POOL_FENCE_SUPPORTED_VERSION: u32 = 2;
const TIER_DELETE_JOURNAL_POLICY_SUPPORTED_VERSION: u32 = 3;
const DECOMMISSION_TARGET_FENCE_POLICY_SUPPORTED_VERSION: u32 = 4;
// Keep this synchronized with the version served by node_service. Including
// the local member in the minimum prevents an older coordinator from
// self-authorizing a policy implemented only by newer remote peers.
const LOCAL_CROSS_POOL_FENCE_POLICY_SUPPORTED_VERSION: u32 = 4;
/// Version 5 is reserved for a fleet whose every metadata writer preserves
/// explicit transition version state and destination identity, and implements
/// conditional per-generation `xl.meta` writes with strong readback. The node
/// service must not advertise this version until the conditional writer from
/// rustfs/backlog#684 is available.
const LEGACY_TRANSITION_STATE_RECONCILE_POLICY_SUPPORTED_VERSION: u32 = 5;

fn resolve_admin_peer_probe_timeout_secs(configured: Option<u64>) -> u64 {
    configured
        .filter(|seconds| *seconds > 0)
        .unwrap_or(rustfs_config::DEFAULT_ADMIN_PEER_PROBE_TIMEOUT_SECS)
        .min(rustfs_config::MAX_ADMIN_PEER_PROBE_TIMEOUT_SECS)
}

fn admin_peer_probe_timeout() -> Duration {
    let configured = rustfs_utils::get_env_opt_u64_with_aliases(rustfs_config::ENV_ADMIN_PEER_PROBE_TIMEOUT_SECS, &[]);
    let seconds = resolve_admin_peer_probe_timeout_secs(configured);
    Duration::from_secs(seconds)
}

fn remaining_admin_peer_probe_timeout(deadline: Instant) -> Option<Duration> {
    remaining_admin_peer_probe_timeout_at(deadline, Instant::now())
}

fn remaining_admin_peer_probe_timeout_at(deadline: Instant, now: Instant) -> Option<Duration> {
    let remaining = deadline.saturating_duration_since(now);
    (!remaining.is_zero()).then_some(remaining)
}

type CrossPoolFencePolicyResult = Result<BTreeMap<String, Uuid>>;

fn cross_pool_fence_policy_results(
    peer_epochs: BTreeMap<String, Uuid>,
    minimum_version: u32,
) -> (
    CrossPoolFencePolicyResult,
    CrossPoolFencePolicyResult,
    CrossPoolFencePolicyResult,
    CrossPoolFencePolicyResult,
) {
    let journal_result = if minimum_version >= TIER_DELETE_JOURNAL_POLICY_SUPPORTED_VERSION {
        Ok(peer_epochs.clone())
    } else {
        Err(Error::other("tier delete journal v6 policy capability version is unsupported"))
    };
    let decommission_target_fence_result = if minimum_version >= DECOMMISSION_TARGET_FENCE_POLICY_SUPPORTED_VERSION {
        Ok(peer_epochs.clone())
    } else {
        Err(Error::other("decommission target fence policy capability version is unsupported"))
    };
    let legacy_transition_state_reconcile_result =
        if minimum_version >= LEGACY_TRANSITION_STATE_RECONCILE_POLICY_SUPPORTED_VERSION {
            Ok(peer_epochs.clone())
        } else {
            Err(Error::other("legacy transition state reconcile policy capability version is unsupported"))
        };
    (
        Ok(peer_epochs),
        journal_result,
        decommission_target_fence_result,
        legacy_transition_state_reconcile_result,
    )
}

#[derive(Clone, Debug)]
pub struct ScannerPublicationLeaseGrant {
    pub host: String,
    pub lease: ScannerPublicationLease,
}

/// Cached result from the last successful admin call to a peer.
struct PeerAdminCache {
    last_storage_info: Option<StorageInfo>,
    last_server_info: Option<ServerProperties>,
    storage_failures: u32,
    server_failures: u32,
    /// When the last successful server_info probe landed. Used to stop a stale
    /// cached `online` snapshot from being served indefinitely while a peer is
    /// actually down (rustfs/backlog#1049 P2).
    last_server_success: Option<SystemTime>,
}

#[derive(Default)]
struct TierConfigReloadWorkers {
    peers: HashMap<String, bool>,
}

enum TierConfigReloadFinish {
    Completed,
    Pending,
}

impl PeerAdminCache {
    fn new() -> Self {
        Self {
            last_storage_info: None,
            last_server_info: None,
            storage_failures: 0,
            server_failures: 0,
            last_server_success: None,
        }
    }
}

/// A cached `online` snapshot older than this is no longer trusted on a probe
/// failure: rather than reporting a stale `online`, the member falls through to
/// the live unknown/degraded/offline classification (rustfs/backlog#1049 P2).
const SERVER_INFO_CACHE_MAX_AGE: Duration = Duration::from_secs(60);

lazy_static! {
    pub static ref GLOBAL_NOTIFICATION_SYS: OnceLock<Arc<NotificationSys>> = OnceLock::new();
}

#[derive(Clone)]
struct FleetCapabilityProof {
    topology_fingerprint: String,
    peer_epochs: Arc<BTreeMap<String, Uuid>>,
    expires_at: Instant,
    generation: Arc<FleetCapabilityProofGeneration>,
}

impl FleetCapabilityProof {
    fn new(topology_fingerprint: String, peer_epochs: Arc<BTreeMap<String, Uuid>>, expires_at: Instant) -> Self {
        Self {
            topology_fingerprint,
            peer_epochs,
            expires_at,
            generation: FleetCapabilityProofGeneration::fresh(),
        }
    }

    fn token(&self) -> FleetCapabilityProofToken {
        FleetCapabilityProofToken {
            topology_fingerprint: self.topology_fingerprint.clone(),
            peer_epochs: self.peer_epochs.clone(),
        }
    }

    #[cfg(any(test, feature = "test-util"))]
    fn with_fresh_generation(&self) -> Self {
        Self::new(self.topology_fingerprint.clone(), Arc::clone(&self.peer_epochs), self.expires_at)
    }
}

/// Admission generation for effects that must not straddle a fleet-proof
/// replacement. Revocation is deliberately non-blocking: it closes admission
/// immediately, while the proof slot withholds the successor generation until
/// every admitted operation has drained.
#[derive(Default)]
struct FleetCapabilityProofGeneration {
    accepting: AtomicBool,
    active: AtomicUsize,
}

impl FleetCapabilityProofGeneration {
    fn fresh() -> Arc<Self> {
        Arc::new(Self {
            accepting: AtomicBool::new(true),
            active: AtomicUsize::new(0),
        })
    }

    fn try_acquire(self: &Arc<Self>) -> Option<FleetCapabilityProofPermit> {
        if !self.accepting.load(Ordering::Acquire) {
            return None;
        }
        self.active.fetch_add(1, Ordering::AcqRel);
        if self.accepting.load(Ordering::Acquire) {
            Some(FleetCapabilityProofPermit {
                generation: Arc::clone(self),
            })
        } else {
            self.release();
            None
        }
    }

    fn revoke(&self) {
        self.accepting.store(false, Ordering::Release);
    }

    fn is_accepting(&self) -> bool {
        self.accepting.load(Ordering::Acquire)
    }

    fn is_drained(&self) -> bool {
        self.active.load(Ordering::Acquire) == 0
    }

    fn release(&self) {
        let previous = self.active.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(previous > 0, "fleet capability permit count underflow");
    }
}

struct FleetCapabilityProofPermit {
    generation: Arc<FleetCapabilityProofGeneration>,
}

impl Drop for FleetCapabilityProofPermit {
    fn drop(&mut self) {
        self.generation.release();
    }
}

#[derive(Clone, PartialEq, Eq)]
struct FleetCapabilityProofToken {
    topology_fingerprint: String,
    peer_epochs: Arc<BTreeMap<String, Uuid>>,
}

#[derive(Default)]
struct FleetCapabilityProofState {
    proof: Option<FleetCapabilityProof>,
    draining_generation: Option<Arc<FleetCapabilityProofGeneration>>,
    topology_conflict: bool,
}

#[derive(Clone, PartialEq, Eq)]
pub(crate) struct RemoteVersionStateFleetProofToken(FleetCapabilityProofToken);

#[derive(Clone, PartialEq, Eq)]
pub struct CrossPoolFenceFleetProofToken(FleetCapabilityProofToken);

#[derive(Clone, PartialEq, Eq)]
pub(crate) struct DecommissionTargetFenceFleetProofToken(FleetCapabilityProofToken);

/// A point-in-time proof that every current storage member implements the v6
/// dispatch-manifest policy. It intentionally has no `Clone` implementation:
/// one acquisition authorizes one manifest construction attempt.
pub(crate) struct TierDeleteJournalFleetProofToken {
    token: FleetCapabilityProofToken,
    _permit: FleetCapabilityProofPermit,
}

/// Effect-window authority for one legacy transition-state reconciliation.
///
/// The token intentionally cannot be cloned. Its permit keeps the admitted
/// fleet generation alive until the caller finishes the final strong
/// readback, while revocation makes every later validation fail immediately.
pub struct LegacyTransitionStateReconcileFleetProofToken {
    token: FleetCapabilityProofToken,
    _permit: FleetCapabilityProofPermit,
}

/// Effect-window authority for one immutable ILM recovery export.
pub struct IlmRecoveryExportFleetProofToken {
    token: FleetCapabilityProofToken,
    _permit: FleetCapabilityProofPermit,
}

/// Effect-window authority for emitting the compact transition-transaction
/// state sequence. The generation permit prevents a successor proof from
/// being published until the admitted writer has finished.
pub(crate) struct TransitionTransactionCompactionFleetProofToken {
    token: FleetCapabilityProofToken,
    _permit: FleetCapabilityProofPermit,
}

static REMOTE_VERSION_STATE_FLEET_PROOF: OnceLock<std::sync::RwLock<FleetCapabilityProofState>> = OnceLock::new();
static CROSS_POOL_FENCE_FLEET_PROOF: OnceLock<std::sync::RwLock<FleetCapabilityProofState>> = OnceLock::new();
static TIER_DELETE_JOURNAL_FLEET_PROOF: OnceLock<std::sync::RwLock<FleetCapabilityProofState>> = OnceLock::new();
static DECOMMISSION_TARGET_FENCE_FLEET_PROOF: OnceLock<std::sync::RwLock<FleetCapabilityProofState>> = OnceLock::new();
static LEGACY_TRANSITION_STATE_RECONCILE_FLEET_PROOF: OnceLock<std::sync::RwLock<FleetCapabilityProofState>> = OnceLock::new();
static ILM_RECOVERY_EXPORT_FLEET_PROOF: OnceLock<std::sync::RwLock<FleetCapabilityProofState>> = OnceLock::new();
static TRANSITION_TRANSACTION_COMPACTION_FLEET_PROOF: OnceLock<std::sync::RwLock<FleetCapabilityProofState>> = OnceLock::new();
static REMOTE_VERSION_STATE_PROBE_TOPOLOGY: OnceLock<String> = OnceLock::new();
static ILM_RECOVERY_EXPORT_LOCAL_PROCESS_EPOCH: LazyLock<Uuid> = LazyLock::new(Uuid::new_v4);

fn cross_pool_fence_fleet_proof_slot() -> &'static std::sync::RwLock<FleetCapabilityProofState> {
    CROSS_POOL_FENCE_FLEET_PROOF.get_or_init(|| std::sync::RwLock::new(FleetCapabilityProofState::default()))
}

fn remote_version_state_fleet_proof_slot() -> &'static std::sync::RwLock<FleetCapabilityProofState> {
    REMOTE_VERSION_STATE_FLEET_PROOF.get_or_init(|| std::sync::RwLock::new(FleetCapabilityProofState::default()))
}

fn tier_delete_journal_fleet_proof_slot() -> &'static std::sync::RwLock<FleetCapabilityProofState> {
    TIER_DELETE_JOURNAL_FLEET_PROOF.get_or_init(|| std::sync::RwLock::new(FleetCapabilityProofState::default()))
}

fn decommission_target_fence_fleet_proof_slot() -> &'static std::sync::RwLock<FleetCapabilityProofState> {
    DECOMMISSION_TARGET_FENCE_FLEET_PROOF.get_or_init(|| std::sync::RwLock::new(FleetCapabilityProofState::default()))
}

fn legacy_transition_state_reconcile_fleet_proof_slot() -> &'static std::sync::RwLock<FleetCapabilityProofState> {
    LEGACY_TRANSITION_STATE_RECONCILE_FLEET_PROOF.get_or_init(|| std::sync::RwLock::new(FleetCapabilityProofState::default()))
}

fn ilm_recovery_export_fleet_proof_slot() -> &'static std::sync::RwLock<FleetCapabilityProofState> {
    ILM_RECOVERY_EXPORT_FLEET_PROOF.get_or_init(|| std::sync::RwLock::new(FleetCapabilityProofState::default()))
}

fn transition_transaction_compaction_fleet_proof_slot() -> &'static std::sync::RwLock<FleetCapabilityProofState> {
    TRANSITION_TRANSACTION_COMPACTION_FLEET_PROOF.get_or_init(|| std::sync::RwLock::new(FleetCapabilityProofState::default()))
}

fn revoke_fleet_capability_proof_state(state: &mut FleetCapabilityProofState) {
    if let Some(proof) = state.proof.take() {
        proof.generation.revoke();
        if !proof.generation.is_drained() {
            state.draining_generation = Some(proof.generation);
        }
    }
    if state
        .draining_generation
        .as_ref()
        .is_some_and(|generation| generation.is_drained())
    {
        state.draining_generation = None;
    }
}

fn revoke_fleet_capability_proof(slot: &std::sync::RwLock<FleetCapabilityProofState>) {
    let mut state = slot.write().unwrap_or_else(std::sync::PoisonError::into_inner);
    revoke_fleet_capability_proof_state(&mut state);
}

fn mark_fleet_capability_topology_conflict(slot: &std::sync::RwLock<FleetCapabilityProofState>) {
    let mut state = slot.write().unwrap_or_else(std::sync::PoisonError::into_inner);
    state.topology_conflict = true;
    revoke_fleet_capability_proof_state(&mut state);
}

fn publish_fleet_capability_probe_result(
    slot: &std::sync::RwLock<FleetCapabilityProofState>,
    topology_fingerprint: &str,
    result: Result<BTreeMap<String, Uuid>>,
    observed_at: Instant,
) -> Option<Error> {
    match result {
        Ok(peer_epochs) => {
            let mut state = slot.write().unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(current) = state
                .proof
                .as_mut()
                .filter(|proof| proof.topology_fingerprint == topology_fingerprint && proof.peer_epochs.as_ref() == &peer_epochs)
            {
                current.expires_at = observed_at + REMOTE_VERSION_STATE_PROOF_TTL;
                return None;
            }

            if let Some(previous) = state.proof.take() {
                previous.generation.revoke();
                if !previous.generation.is_drained() {
                    state.draining_generation = Some(previous.generation);
                }
            }
            if state
                .draining_generation
                .as_ref()
                .is_some_and(|generation| generation.is_drained())
            {
                state.draining_generation = None;
            }
            if state.draining_generation.is_some() {
                return Some(Error::other(
                    "fleet capability proof successor waits for the previous generation to drain",
                ));
            }
            state.proof = Some(FleetCapabilityProof::new(
                topology_fingerprint.to_string(),
                Arc::new(peer_epochs),
                observed_at + REMOTE_VERSION_STATE_PROOF_TTL,
            ));
            None
        }
        Err(err) => {
            revoke_fleet_capability_proof(slot);
            Some(err)
        }
    }
}

pub(crate) fn acquire_remote_version_state_fleet_proof() -> Option<RemoteVersionStateFleetProofToken> {
    let expected_topology = REMOTE_VERSION_STATE_PROBE_TOPOLOGY.get()?;
    let state = remote_version_state_fleet_proof_slot()
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    acquire_fleet_capability_proof_from(&state, expected_topology, Instant::now()).map(RemoteVersionStateFleetProofToken)
}

fn acquire_fleet_capability_proof_from(
    state: &FleetCapabilityProofState,
    expected_topology: &str,
    now: Instant,
) -> Option<FleetCapabilityProofToken> {
    if state.topology_conflict || !fleet_capability_proof_valid_at(state.proof.as_ref(), expected_topology, now) {
        return None;
    }
    state.proof.as_ref().map(FleetCapabilityProof::token)
}

pub(crate) fn remote_version_state_fleet_proof_matches(proof: &RemoteVersionStateFleetProofToken) -> bool {
    fleet_capability_proof_matches(remote_version_state_fleet_proof_slot(), &proof.0)
}

pub(crate) fn acquire_transition_transaction_compaction_fleet_proof() -> Option<TransitionTransactionCompactionFleetProofToken> {
    let expected_topology = REMOTE_VERSION_STATE_PROBE_TOPOLOGY.get()?;
    let state = transition_transaction_compaction_fleet_proof_slot()
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let token = acquire_fleet_capability_proof_from(&state, expected_topology, Instant::now())?;
    let permit = state.proof.as_ref()?.generation.try_acquire()?;
    Some(TransitionTransactionCompactionFleetProofToken { token, _permit: permit })
}

pub(crate) fn transition_transaction_compaction_fleet_proof_matches(
    proof: &TransitionTransactionCompactionFleetProofToken,
) -> bool {
    let Some(expected_topology) = REMOTE_VERSION_STATE_PROBE_TOPOLOGY.get() else {
        return false;
    };
    let state = transition_transaction_compaction_fleet_proof_slot()
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    proof._permit.generation.is_accepting()
        && fleet_capability_proof_matches_at(&state, &proof.token, expected_topology, Instant::now())
        && state
            .proof
            .as_ref()
            .is_some_and(|current| Arc::ptr_eq(&current.generation, &proof._permit.generation))
}

pub fn acquire_cross_pool_fence_fleet_proof() -> Option<CrossPoolFenceFleetProofToken> {
    let expected_topology = REMOTE_VERSION_STATE_PROBE_TOPOLOGY.get()?;
    let state = cross_pool_fence_fleet_proof_slot()
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    acquire_fleet_capability_proof_from(&state, expected_topology, Instant::now()).map(CrossPoolFenceFleetProofToken)
}

pub fn cross_pool_fence_fleet_proof_matches(proof: &CrossPoolFenceFleetProofToken) -> bool {
    fleet_capability_proof_matches(cross_pool_fence_fleet_proof_slot(), &proof.0)
}

pub(crate) fn acquire_decommission_target_fence_fleet_proof() -> Option<DecommissionTargetFenceFleetProofToken> {
    let expected_topology = REMOTE_VERSION_STATE_PROBE_TOPOLOGY.get()?;
    let state = decommission_target_fence_fleet_proof_slot()
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    acquire_fleet_capability_proof_from(&state, expected_topology, Instant::now()).map(DecommissionTargetFenceFleetProofToken)
}

pub(crate) fn decommission_target_fence_fleet_proof_matches(proof: &DecommissionTargetFenceFleetProofToken) -> bool {
    fleet_capability_proof_matches(decommission_target_fence_fleet_proof_slot(), &proof.0)
}

pub(crate) fn acquire_tier_delete_journal_fleet_proof() -> Option<TierDeleteJournalFleetProofToken> {
    let expected_topology = REMOTE_VERSION_STATE_PROBE_TOPOLOGY.get()?;
    let state = tier_delete_journal_fleet_proof_slot()
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    acquire_tier_delete_journal_fleet_proof_from(&state, expected_topology, Instant::now())
}

fn acquire_tier_delete_journal_fleet_proof_from(
    state: &FleetCapabilityProofState,
    expected_topology: &str,
    now: Instant,
) -> Option<TierDeleteJournalFleetProofToken> {
    let token = acquire_fleet_capability_proof_from(state, expected_topology, now)?;
    let permit = state.proof.as_ref()?.generation.try_acquire()?;
    Some(TierDeleteJournalFleetProofToken { token, _permit: permit })
}

pub(crate) fn tier_delete_journal_fleet_proof_matches(proof: &TierDeleteJournalFleetProofToken) -> bool {
    let Some(expected_topology) = REMOTE_VERSION_STATE_PROBE_TOPOLOGY.get() else {
        return false;
    };
    let state = tier_delete_journal_fleet_proof_slot()
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    tier_delete_journal_fleet_proof_matches_at(&state, proof, expected_topology, Instant::now())
}

fn tier_delete_journal_fleet_proof_matches_at(
    state: &FleetCapabilityProofState,
    proof: &TierDeleteJournalFleetProofToken,
    expected_topology: &str,
    now: Instant,
) -> bool {
    proof._permit.generation.is_accepting()
        && fleet_capability_proof_matches_at(state, &proof.token, expected_topology, now)
        && state
            .proof
            .as_ref()
            .is_some_and(|current| Arc::ptr_eq(&current.generation, &proof._permit.generation))
}

pub(crate) fn tier_delete_journal_topology_generation(proof: &TierDeleteJournalFleetProofToken) -> String {
    stable_tier_delete_journal_topology_generation(&proof.token.topology_fingerprint)
}

/// Acquire one non-cloneable authority that must span the complete reconcile
/// effect window, including its final strong readback.
pub async fn acquire_legacy_transition_state_reconcile_fleet_proof() -> Option<LegacyTransitionStateReconcileFleetProofToken> {
    let expected_topology = REMOTE_VERSION_STATE_PROBE_TOPOLOGY.get()?;
    let proof = {
        let state = legacy_transition_state_reconcile_fleet_proof_slot()
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        acquire_legacy_transition_state_reconcile_fleet_proof_from(&state, expected_topology, Instant::now())?
    };
    let observed_peer_epochs = observe_legacy_transition_state_reconcile_fleet(expected_topology).await?;
    let state = legacy_transition_state_reconcile_fleet_proof_slot()
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    legacy_transition_state_reconcile_fleet_proof_matches_observation_at(
        &state,
        &proof,
        expected_topology,
        &observed_peer_epochs,
        Instant::now(),
    )
    .then_some(proof)
}

fn acquire_legacy_transition_state_reconcile_fleet_proof_from(
    state: &FleetCapabilityProofState,
    expected_topology: &str,
    now: Instant,
) -> Option<LegacyTransitionStateReconcileFleetProofToken> {
    let token = acquire_fleet_capability_proof_from(state, expected_topology, now)?;
    let permit = state.proof.as_ref()?.generation.try_acquire()?;
    Some(LegacyTransitionStateReconcileFleetProofToken { token, _permit: permit })
}

async fn observe_legacy_transition_state_reconcile_fleet(expected_topology: &str) -> Option<BTreeMap<String, Uuid>> {
    let notification_sys = get_global_notification_sys()?;
    let (peer_epochs, minimum_version) = timeout(
        REMOTE_VERSION_STATE_PROBE_TIMEOUT,
        notification_sys.probe_cross_pool_fence_fleet(expected_topology),
    )
    .await
    .ok()?
    .ok()?;
    let (_, _, _, reconcile_result) = cross_pool_fence_policy_results(peer_epochs, minimum_version);
    reconcile_result.ok()
}

/// Revalidate the exact fleet generation captured by a reconcile token with a
/// fresh synchronous observation. Callers must await this before each
/// conditional metadata write and after the final strong readback.
pub async fn legacy_transition_state_reconcile_fleet_proof_matches(
    proof: &LegacyTransitionStateReconcileFleetProofToken,
) -> bool {
    let Some(expected_topology) = REMOTE_VERSION_STATE_PROBE_TOPOLOGY.get() else {
        return false;
    };
    legacy_transition_state_reconcile_fleet_proof_matches_with_observer(
        legacy_transition_state_reconcile_fleet_proof_slot(),
        proof,
        expected_topology,
        || observe_legacy_transition_state_reconcile_fleet(expected_topology),
    )
    .await
}

pub async fn acquire_ilm_recovery_export_fleet_proof() -> Option<IlmRecoveryExportFleetProofToken> {
    let expected_topology = REMOTE_VERSION_STATE_PROBE_TOPOLOGY.get()?;
    let proof = {
        let state = ilm_recovery_export_fleet_proof_slot()
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        acquire_ilm_recovery_export_fleet_proof_from(&state, expected_topology, Instant::now())?
    };
    let observed = observe_ilm_recovery_export_fleet(expected_topology).await?;
    let state = ilm_recovery_export_fleet_proof_slot()
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    ilm_recovery_export_fleet_proof_matches_observation_at(&state, &proof, expected_topology, &observed, Instant::now())
        .then_some(proof)
}

fn acquire_ilm_recovery_export_fleet_proof_from(
    state: &FleetCapabilityProofState,
    expected_topology: &str,
    now: Instant,
) -> Option<IlmRecoveryExportFleetProofToken> {
    let token = acquire_fleet_capability_proof_from(state, expected_topology, now)?;
    let permit = state.proof.as_ref()?.generation.try_acquire()?;
    Some(IlmRecoveryExportFleetProofToken { token, _permit: permit })
}

pub async fn ilm_recovery_export_fleet_proof_matches(proof: &IlmRecoveryExportFleetProofToken) -> bool {
    let Some(expected_topology) = REMOTE_VERSION_STATE_PROBE_TOPOLOGY.get() else {
        return false;
    };
    {
        let state = ilm_recovery_export_fleet_proof_slot()
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !ilm_recovery_export_fleet_proof_matches_at(&state, proof, expected_topology, Instant::now()) {
            return false;
        }
    }
    let Some(observed) = observe_ilm_recovery_export_fleet(expected_topology).await else {
        return false;
    };
    let state = ilm_recovery_export_fleet_proof_slot()
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    ilm_recovery_export_fleet_proof_matches_observation_at(&state, proof, expected_topology, &observed, Instant::now())
}

pub fn ilm_recovery_export_topology_generation(proof: &IlmRecoveryExportFleetProofToken) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"rustfs-ilm-recovery-export-topology-v1\0");
    hasher.update(proof.token.topology_fingerprint.as_bytes());
    rustfs_utils::crypto::hex(hasher.finalize().as_slice())
}

pub fn ilm_recovery_export_member_epochs_sha256(proof: &IlmRecoveryExportFleetProofToken) -> String {
    let encoded = serde_json::to_vec(proof.token.peer_epochs.as_ref()).expect("member epoch map is JSON encodable");
    let mut hasher = Sha256::new();
    hasher.update(b"rustfs-ilm-recovery-export-members-v1\0");
    hasher.update(encoded);
    rustfs_utils::crypto::hex(hasher.finalize().as_slice())
}

pub fn ilm_recovery_export_local_process_epoch() -> Uuid {
    *ILM_RECOVERY_EXPORT_LOCAL_PROCESS_EPOCH
}

fn ilm_recovery_export_fleet_proof_matches_at(
    state: &FleetCapabilityProofState,
    proof: &IlmRecoveryExportFleetProofToken,
    expected_topology: &str,
    now: Instant,
) -> bool {
    proof._permit.generation.is_accepting()
        && fleet_capability_proof_matches_at(state, &proof.token, expected_topology, now)
        && state
            .proof
            .as_ref()
            .is_some_and(|current| Arc::ptr_eq(&current.generation, &proof._permit.generation))
}

fn ilm_recovery_export_fleet_proof_matches_observation_at(
    state: &FleetCapabilityProofState,
    proof: &IlmRecoveryExportFleetProofToken,
    expected_topology: &str,
    observed: &BTreeMap<String, Uuid>,
    now: Instant,
) -> bool {
    ilm_recovery_export_fleet_proof_matches_at(state, proof, expected_topology, now)
        && proof.token.peer_epochs.as_ref() == observed
}

async fn observe_ilm_recovery_export_fleet(expected_topology: &str) -> Option<BTreeMap<String, Uuid>> {
    #[cfg(test)]
    {
        let state = ilm_recovery_export_fleet_proof_slot()
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if fleet_capability_proof_valid_at(state.proof.as_ref(), expected_topology, Instant::now()) {
            return state.proof.as_ref().map(|proof| proof.peer_epochs.as_ref().clone());
        }
    }
    let notification_sys = get_global_notification_sys()?;
    timeout(
        REMOTE_VERSION_STATE_PROBE_TIMEOUT,
        notification_sys.probe_ilm_recovery_export_fleet(expected_topology),
    )
    .await
    .ok()?
    .ok()
}

async fn legacy_transition_state_reconcile_fleet_proof_matches_with_observer<F, Fut>(
    slot: &std::sync::RwLock<FleetCapabilityProofState>,
    proof: &LegacyTransitionStateReconcileFleetProofToken,
    expected_topology: &str,
    observe: F,
) -> bool
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = Option<BTreeMap<String, Uuid>>>,
{
    {
        let state = slot.read().unwrap_or_else(std::sync::PoisonError::into_inner);
        if !legacy_transition_state_reconcile_fleet_proof_matches_at(&state, proof, expected_topology, Instant::now()) {
            return false;
        }
    }
    let Some(observed_peer_epochs) = observe().await else {
        return false;
    };
    let state = slot.read().unwrap_or_else(std::sync::PoisonError::into_inner);
    legacy_transition_state_reconcile_fleet_proof_matches_observation_at(
        &state,
        proof,
        expected_topology,
        &observed_peer_epochs,
        Instant::now(),
    )
}

fn legacy_transition_state_reconcile_fleet_proof_matches_at(
    state: &FleetCapabilityProofState,
    proof: &LegacyTransitionStateReconcileFleetProofToken,
    expected_topology: &str,
    now: Instant,
) -> bool {
    proof._permit.generation.is_accepting()
        && fleet_capability_proof_matches_at(state, &proof.token, expected_topology, now)
        && state
            .proof
            .as_ref()
            .is_some_and(|current| Arc::ptr_eq(&current.generation, &proof._permit.generation))
}

fn legacy_transition_state_reconcile_fleet_proof_matches_observation_at(
    state: &FleetCapabilityProofState,
    proof: &LegacyTransitionStateReconcileFleetProofToken,
    expected_topology: &str,
    observed_peer_epochs: &BTreeMap<String, Uuid>,
    now: Instant,
) -> bool {
    legacy_transition_state_reconcile_fleet_proof_matches_at(state, proof, expected_topology, now)
        && proof.token.peer_epochs.as_ref() == observed_peer_epochs
}

#[cfg(all(test, feature = "test-util"))]
pub(crate) fn tier_delete_journal_fleet_proof_has_inflight_for_test() -> bool {
    let state = tier_delete_journal_fleet_proof_slot()
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    state.proof.as_ref().is_some_and(|proof| !proof.generation.is_drained())
        || state
            .draining_generation
            .as_ref()
            .is_some_and(|generation| !generation.is_drained())
}

fn stable_tier_delete_journal_topology_generation(topology_fingerprint: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"rustfs-tier-delete-journal-topology-v1\0");
    hasher.update(topology_fingerprint.as_bytes());
    rustfs_utils::crypto::hex(hasher.finalize().as_slice())
}

#[cfg(any(test, feature = "test-util"))]
pub(crate) fn install_cross_pool_fence_fleet_proof_for_test() {
    let topology = REMOTE_VERSION_STATE_PROBE_TOPOLOGY
        .get()
        .cloned()
        .unwrap_or_else(|| "pool-activation-test-topology".to_string());
    let _ = REMOTE_VERSION_STATE_PROBE_TOPOLOGY.set(topology.clone());
    let mut state = cross_pool_fence_fleet_proof_slot()
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let now = Instant::now();
    let proof = if !state.topology_conflict && fleet_capability_proof_valid_at(state.proof.as_ref(), &topology, now) {
        state.proof.clone()
    } else {
        Some(FleetCapabilityProof::new(
            topology.clone(),
            Arc::new(BTreeMap::new()),
            now + Duration::from_secs(60 * 60),
        ))
    };
    state.topology_conflict = false;
    state.proof = proof.clone();
    drop(state);
    let mut journal_state = tier_delete_journal_fleet_proof_slot()
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    debug_assert!(
        journal_state
            .proof
            .as_ref()
            .is_none_or(|current| current.generation.is_drained())
    );
    journal_state.topology_conflict = false;
    journal_state.draining_generation = None;
    journal_state.proof = proof.as_ref().map(FleetCapabilityProof::with_fresh_generation);
    drop(journal_state);
    let mut decommission_state = decommission_target_fence_fleet_proof_slot()
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    debug_assert!(
        decommission_state
            .proof
            .as_ref()
            .is_none_or(|current| current.generation.is_drained())
    );
    decommission_state.topology_conflict = false;
    decommission_state.draining_generation = None;
    decommission_state.proof = proof.as_ref().map(FleetCapabilityProof::with_fresh_generation);
    drop(decommission_state);
    let mut export_state = ilm_recovery_export_fleet_proof_slot()
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if !fleet_capability_proof_valid_at(export_state.proof.as_ref(), &topology, now) {
        debug_assert!(
            export_state
                .proof
                .as_ref()
                .is_none_or(|current| current.generation.is_drained())
        );
        export_state.topology_conflict = false;
        export_state.draining_generation = None;
        export_state.proof = proof.as_ref().map(FleetCapabilityProof::with_fresh_generation);
    }
}

#[cfg(test)]
pub(crate) struct CrossPoolFenceFleetProofGuard {
    previous_proof: Option<FleetCapabilityProof>,
    previous_topology_conflict: bool,
    previous_journal_proof: Option<FleetCapabilityProof>,
    previous_journal_topology_conflict: bool,
    previous_decommission_proof: Option<FleetCapabilityProof>,
    previous_decommission_topology_conflict: bool,
}

#[cfg(test)]
impl Drop for CrossPoolFenceFleetProofGuard {
    fn drop(&mut self) {
        let mut state = cross_pool_fence_fleet_proof_slot()
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.proof = self
            .previous_proof
            .take()
            .as_ref()
            .map(FleetCapabilityProof::with_fresh_generation);
        state.draining_generation = None;
        state.topology_conflict = self.previous_topology_conflict;
        drop(state);
        let mut journal_state = tier_delete_journal_fleet_proof_slot()
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        journal_state.proof = self
            .previous_journal_proof
            .take()
            .as_ref()
            .map(FleetCapabilityProof::with_fresh_generation);
        journal_state.draining_generation = None;
        journal_state.topology_conflict = self.previous_journal_topology_conflict;
        drop(journal_state);
        let mut decommission_state = decommission_target_fence_fleet_proof_slot()
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        decommission_state.proof = self
            .previous_decommission_proof
            .take()
            .as_ref()
            .map(FleetCapabilityProof::with_fresh_generation);
        decommission_state.draining_generation = None;
        decommission_state.topology_conflict = self.previous_decommission_topology_conflict;
    }
}

/// Temporarily revoke the test proof so activation paths can exercise their
/// fail-closed behavior without changing the process-wide topology binding.
#[cfg(test)]
pub(crate) fn without_cross_pool_fence_fleet_proof_for_test() -> CrossPoolFenceFleetProofGuard {
    let mut state = cross_pool_fence_fleet_proof_slot()
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let mut journal_state = tier_delete_journal_fleet_proof_slot()
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let mut decommission_state = decommission_target_fence_fleet_proof_slot()
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let guard = CrossPoolFenceFleetProofGuard {
        previous_proof: state.proof.clone(),
        previous_topology_conflict: state.topology_conflict,
        previous_journal_proof: journal_state.proof.clone(),
        previous_journal_topology_conflict: journal_state.topology_conflict,
        previous_decommission_proof: decommission_state.proof.clone(),
        previous_decommission_topology_conflict: decommission_state.topology_conflict,
    };
    if let Some(proof) = state.proof.take() {
        proof.generation.revoke();
        if !proof.generation.is_drained() {
            state.draining_generation = Some(proof.generation);
        }
    }
    state.topology_conflict = true;
    if let Some(proof) = journal_state.proof.take() {
        proof.generation.revoke();
        if !proof.generation.is_drained() {
            journal_state.draining_generation = Some(proof.generation);
        }
    }
    journal_state.topology_conflict = true;
    if let Some(proof) = decommission_state.proof.take() {
        proof.generation.revoke();
        if !proof.generation.is_drained() {
            decommission_state.draining_generation = Some(proof.generation);
        }
    }
    decommission_state.topology_conflict = true;
    guard
}

#[cfg(test)]
pub(crate) struct DecommissionTargetFenceFleetProofGuard {
    previous_proof: Option<FleetCapabilityProof>,
    previous_topology_conflict: bool,
}

#[cfg(test)]
impl Drop for DecommissionTargetFenceFleetProofGuard {
    fn drop(&mut self) {
        let mut state = decommission_target_fence_fleet_proof_slot()
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.proof = self
            .previous_proof
            .take()
            .as_ref()
            .map(FleetCapabilityProof::with_fresh_generation);
        state.draining_generation = None;
        state.topology_conflict = self.previous_topology_conflict;
    }
}

#[cfg(test)]
pub(crate) fn without_decommission_target_fence_fleet_proof_for_test() -> DecommissionTargetFenceFleetProofGuard {
    let mut state = decommission_target_fence_fleet_proof_slot()
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let guard = DecommissionTargetFenceFleetProofGuard {
        previous_proof: state.proof.clone(),
        previous_topology_conflict: state.topology_conflict,
    };
    revoke_fleet_capability_proof_state(&mut state);
    state.topology_conflict = true;
    guard
}

#[cfg(any(test, feature = "test-util"))]
pub fn rotate_cross_pool_fence_fleet_proof_for_test() -> bool {
    let mut state = cross_pool_fence_fleet_proof_slot()
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let Some(current) = state.proof.as_ref() else {
        return false;
    };
    let proof = FleetCapabilityProof::new(
        current.topology_fingerprint.clone(),
        Arc::new(current.peer_epochs.as_ref().clone()),
        current.expires_at,
    );
    state.proof = Some(proof.clone());
    drop(state);
    let mut journal_state = tier_delete_journal_fleet_proof_slot()
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    journal_state.topology_conflict = false;
    if let Some(previous) = journal_state.proof.take() {
        previous.generation.revoke();
        if !previous.generation.is_drained() {
            journal_state.draining_generation = Some(previous.generation);
        }
    }
    if journal_state
        .draining_generation
        .as_ref()
        .is_some_and(|generation| generation.is_drained())
    {
        journal_state.draining_generation = None;
    }
    if journal_state.draining_generation.is_none() {
        journal_state.proof = Some(proof.with_fresh_generation());
    }
    drop(journal_state);
    let mut decommission_state = decommission_target_fence_fleet_proof_slot()
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    decommission_state.topology_conflict = false;
    revoke_fleet_capability_proof_state(&mut decommission_state);
    if decommission_state.draining_generation.is_none() {
        decommission_state.proof = Some(proof.with_fresh_generation());
    }
    true
}

fn fleet_capability_proof_matches(
    slot: &std::sync::RwLock<FleetCapabilityProofState>,
    proof: &FleetCapabilityProofToken,
) -> bool {
    let Some(expected_topology) = REMOTE_VERSION_STATE_PROBE_TOPOLOGY.get() else {
        return false;
    };
    let state = slot.read().unwrap_or_else(std::sync::PoisonError::into_inner);
    fleet_capability_proof_matches_at(&state, proof, expected_topology, Instant::now())
}

fn fleet_capability_proof_matches_at(
    state: &FleetCapabilityProofState,
    proof: &FleetCapabilityProofToken,
    expected_topology: &str,
    now: Instant,
) -> bool {
    !state.topology_conflict
        && state.proof.as_ref().is_some_and(|current| {
            current.topology_fingerprint == expected_topology
                && current.topology_fingerprint == proof.topology_fingerprint
                && Arc::ptr_eq(&current.peer_epochs, &proof.peer_epochs)
                && now < current.expires_at
        })
}

fn fleet_capability_proof_valid_at(proof: Option<&FleetCapabilityProof>, expected_topology: &str, now: Instant) -> bool {
    proof.is_some_and(|proof| proof.topology_fingerprint == expected_topology && now < proof.expires_at)
}

#[cfg(test)]
pub(crate) struct RemoteVersionStateFleetProofGuard;

#[cfg(test)]
impl Drop for RemoteVersionStateFleetProofGuard {
    fn drop(&mut self) {
        revoke_fleet_capability_proof(remote_version_state_fleet_proof_slot());
    }
}

#[cfg(test)]
pub(crate) fn install_remote_version_state_fleet_proof_for_test(topology_fingerprint: &str) -> RemoteVersionStateFleetProofGuard {
    match REMOTE_VERSION_STATE_PROBE_TOPOLOGY.set(topology_fingerprint.to_string()) {
        Ok(()) => {}
        Err(_)
            if REMOTE_VERSION_STATE_PROBE_TOPOLOGY
                .get()
                .is_some_and(|current| current == topology_fingerprint) => {}
        Err(_) => panic!("remote version state test topology is already bound to another fingerprint"),
    }
    let peer_epochs = BTreeMap::new();
    if let Some(err) = publish_fleet_capability_probe_result(
        remote_version_state_fleet_proof_slot(),
        topology_fingerprint,
        Ok(peer_epochs),
        Instant::now(),
    ) {
        panic!("test proof installation must not fail: {err}");
    }
    RemoteVersionStateFleetProofGuard
}

#[cfg(all(test, feature = "test-util"))]
pub(crate) struct TransitionTransactionCompactionFleetProofGuard;

#[cfg(all(test, feature = "test-util"))]
impl Drop for TransitionTransactionCompactionFleetProofGuard {
    fn drop(&mut self) {
        revoke_fleet_capability_proof(transition_transaction_compaction_fleet_proof_slot());
    }
}

#[cfg(all(test, feature = "test-util"))]
pub(crate) fn install_transition_transaction_compaction_fleet_proof_for_test(
    topology_fingerprint: &str,
) -> TransitionTransactionCompactionFleetProofGuard {
    let _ = REMOTE_VERSION_STATE_PROBE_TOPOLOGY.set(topology_fingerprint.to_string());
    let effective_topology = REMOTE_VERSION_STATE_PROBE_TOPOLOGY
        .get()
        .expect("transition transaction compaction test topology should be initialized");
    if let Some(err) = publish_fleet_capability_probe_result(
        transition_transaction_compaction_fleet_proof_slot(),
        effective_topology,
        Ok(BTreeMap::new()),
        Instant::now(),
    ) {
        panic!("test proof installation must not fail: {err}");
    }
    TransitionTransactionCompactionFleetProofGuard
}

fn insert_remote_version_state_peer(peer_epochs: &mut BTreeMap<String, Uuid>, peer: String, epoch: Uuid) -> Result<()> {
    if epoch.is_nil() || peer_epochs.values().any(|existing| *existing == epoch) || peer_epochs.insert(peer, epoch).is_some() {
        return Err(Error::other("remote version state capability peer identity is invalid"));
    }
    Ok(())
}

pub fn start_remote_version_state_fleet_probe(topology_fingerprint: String) {
    if REMOTE_VERSION_STATE_PROBE_TOPOLOGY.set(topology_fingerprint.clone()).is_err() {
        if REMOTE_VERSION_STATE_PROBE_TOPOLOGY.get() != Some(&topology_fingerprint) {
            for slot in [
                remote_version_state_fleet_proof_slot(),
                cross_pool_fence_fleet_proof_slot(),
                tier_delete_journal_fleet_proof_slot(),
                decommission_target_fence_fleet_proof_slot(),
                legacy_transition_state_reconcile_fleet_proof_slot(),
                ilm_recovery_export_fleet_proof_slot(),
                transition_transaction_compaction_fleet_proof_slot(),
            ] {
                mark_fleet_capability_topology_conflict(slot);
            }
        }
        return;
    }

    tokio::spawn(async move {
        loop {
            let notification_sys = get_global_notification_sys();
            let remote_version_state_probe = async {
                match notification_sys.as_ref() {
                    Some(notification_sys) => timeout(
                        REMOTE_VERSION_STATE_PROBE_TIMEOUT,
                        notification_sys.probe_remote_version_state_fleet(&topology_fingerprint),
                    )
                    .await
                    .unwrap_or_else(|_| Err(Error::other("remote version state fleet capability probe timed out"))),
                    None => Err(Error::other("remote version state fleet capability notification system is unavailable")),
                }
            };
            let cross_pool_fence_probe = async {
                match notification_sys.as_ref() {
                    Some(notification_sys) => timeout(
                        REMOTE_VERSION_STATE_PROBE_TIMEOUT,
                        notification_sys.probe_cross_pool_fence_fleet(&topology_fingerprint),
                    )
                    .await
                    .unwrap_or_else(|_| Err(Error::other("cross-pool fence fleet capability probe timed out"))),
                    None => Err(Error::other("cross-pool fence fleet capability notification system is unavailable")),
                }
            };
            let recovery_export_probe = async {
                match notification_sys.as_ref() {
                    Some(notification_sys) => timeout(
                        REMOTE_VERSION_STATE_PROBE_TIMEOUT,
                        notification_sys.probe_ilm_recovery_export_fleet(&topology_fingerprint),
                    )
                    .await
                    .unwrap_or_else(|_| Err(Error::other("ILM recovery export fleet capability probe timed out"))),
                    None => Err(Error::other("ILM recovery export fleet capability notification system is unavailable")),
                }
            };
            let transition_transaction_compaction_probe = async {
                match notification_sys.as_ref() {
                    Some(notification_sys) => timeout(
                        REMOTE_VERSION_STATE_PROBE_TIMEOUT,
                        notification_sys.probe_transition_transaction_compaction_fleet(&topology_fingerprint),
                    )
                    .await
                    .unwrap_or_else(|_| Err(Error::other("transition transaction compaction fleet capability probe timed out"))),
                    None => Err(Error::other(
                        "transition transaction compaction fleet capability notification system is unavailable",
                    )),
                }
            };
            let (result, fence_probe, recovery_export_result, transition_transaction_compaction_result) = tokio::join!(
                remote_version_state_probe,
                cross_pool_fence_probe,
                recovery_export_probe,
                transition_transaction_compaction_probe
            );
            let (fence_result, journal_result, decommission_target_fence_result, reconcile_result) = match fence_probe {
                Ok((peer_epochs, minimum_version)) => cross_pool_fence_policy_results(peer_epochs, minimum_version),
                Err(err) => {
                    let message = err.to_string();
                    (
                        Err(Error::other(message.clone())),
                        Err(Error::other(message.clone())),
                        Err(Error::other(message.clone())),
                        Err(Error::other(message)),
                    )
                }
            };
            let topology_conflict = remote_version_state_fleet_proof_slot()
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .topology_conflict;
            if topology_conflict {
                revoke_fleet_capability_proof(remote_version_state_fleet_proof_slot());
                revoke_fleet_capability_proof(cross_pool_fence_fleet_proof_slot());
                revoke_fleet_capability_proof(tier_delete_journal_fleet_proof_slot());
                revoke_fleet_capability_proof(decommission_target_fence_fleet_proof_slot());
                revoke_fleet_capability_proof(legacy_transition_state_reconcile_fleet_proof_slot());
                revoke_fleet_capability_proof(ilm_recovery_export_fleet_proof_slot());
                revoke_fleet_capability_proof(transition_transaction_compaction_fleet_proof_slot());
            } else if let Some(err) = publish_fleet_capability_probe_result(
                remote_version_state_fleet_proof_slot(),
                &topology_fingerprint,
                result,
                Instant::now(),
            ) {
                debug!(error = %err, "remote version state fleet capability probe failed closed");
            }
            if !topology_conflict
                && let Some(err) = publish_fleet_capability_probe_result(
                    cross_pool_fence_fleet_proof_slot(),
                    &topology_fingerprint,
                    fence_result,
                    Instant::now(),
                )
            {
                debug!(
                    event = EVENT_NOTIFICATION_CAPABILITY_PROBE,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_NOTIFICATION,
                    capability = "cross_pool_fence",
                    state = "failed_closed",
                    error = %err,
                    "notification capability probe"
                );
            }
            if !topology_conflict
                && let Some(err) = publish_fleet_capability_probe_result(
                    ilm_recovery_export_fleet_proof_slot(),
                    &topology_fingerprint,
                    recovery_export_result,
                    Instant::now(),
                )
            {
                debug!(
                    event = EVENT_NOTIFICATION_CAPABILITY_PROBE,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_NOTIFICATION,
                    capability = "ilm_recovery_export_v1",
                    state = "failed_closed",
                    error = %err,
                    "notification capability probe"
                );
            }
            if !topology_conflict
                && let Some(err) = publish_fleet_capability_probe_result(
                    transition_transaction_compaction_fleet_proof_slot(),
                    &topology_fingerprint,
                    transition_transaction_compaction_result,
                    Instant::now(),
                )
            {
                debug!(
                    event = EVENT_NOTIFICATION_CAPABILITY_PROBE,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_NOTIFICATION,
                    capability = "transition_transaction_compaction_v1",
                    state = "failed_closed",
                    error = %err,
                    "notification capability probe"
                );
            }
            if !topology_conflict
                && let Some(err) = publish_fleet_capability_probe_result(
                    tier_delete_journal_fleet_proof_slot(),
                    &topology_fingerprint,
                    journal_result,
                    Instant::now(),
                )
            {
                debug!(
                    event = EVENT_NOTIFICATION_CAPABILITY_PROBE,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_NOTIFICATION,
                    capability = "tier_delete_journal_v6_policy",
                    state = "failed_closed",
                    error = %err,
                    "notification capability probe"
                );
            }
            if !topology_conflict
                && let Some(err) = publish_fleet_capability_probe_result(
                    decommission_target_fence_fleet_proof_slot(),
                    &topology_fingerprint,
                    decommission_target_fence_result,
                    Instant::now(),
                )
            {
                debug!(
                    event = EVENT_NOTIFICATION_CAPABILITY_PROBE,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_NOTIFICATION,
                    capability = "decommission_target_fence_v2",
                    state = "failed_closed",
                    error = %err,
                    "notification capability probe"
                );
            }
            if !topology_conflict
                && let Some(err) = publish_fleet_capability_probe_result(
                    legacy_transition_state_reconcile_fleet_proof_slot(),
                    &topology_fingerprint,
                    reconcile_result,
                    Instant::now(),
                )
            {
                debug!(
                    event = EVENT_NOTIFICATION_CAPABILITY_PROBE,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_NOTIFICATION,
                    capability = "legacy_transition_state_reconcile_v1",
                    state = "failed_closed",
                    error = %err,
                    "notification capability probe"
                );
            }
            sleep(REMOTE_VERSION_STATE_PROBE_INTERVAL).await;
        }
    });
}

pub async fn new_global_notification_sys(eps: EndpointServerPools) -> Result<()> {
    let _ = GLOBAL_NOTIFICATION_SYS
        .set(Arc::new(NotificationSys::new(eps).await))
        .map_err(|_| Error::other("init notification_sys fail"));
    Ok(())
}

// Owned handle rather than `&'static` (backlog#1052 S3): per-server contexts
// need to hold their own notification system, which a process-lifetime
// borrow cannot express.
pub fn get_global_notification_sys() -> Option<Arc<NotificationSys>> {
    GLOBAL_NOTIFICATION_SYS.get().cloned()
}

pub struct NotificationSys {
    pub peer_clients: Vec<Option<PeerRestClient>>,
    pub all_peer_clients: Vec<Option<PeerRestClient>>,
    peer_topology_hosts: Vec<String>,
    peer_admin_caches: Vec<Mutex<PeerAdminCache>>,
    tier_config_reload_workers: Arc<Mutex<TierConfigReloadWorkers>>,
}

impl NotificationSys {
    pub async fn new(eps: EndpointServerPools) -> Self {
        let expected_remote_hosts = eps
            .peer_grid_host_slots_sorted()
            .into_iter()
            .filter_map(|(peer, _, is_local)| (!is_local).then_some(peer))
            .collect::<Vec<_>>();
        let (peer_clients, all_peer_clients, peer_topology_hosts) = PeerRestClient::new_clients_with_topology(eps).await;
        let peer_topology_hosts = if peer_topology_hosts.is_empty() {
            expected_remote_hosts
        } else {
            peer_topology_hosts
        };
        let peer_admin_caches = (0..peer_clients.len()).map(|_| Mutex::new(PeerAdminCache::new())).collect();
        Self {
            peer_clients,
            all_peer_clients,
            peer_topology_hosts,
            peer_admin_caches,
            tier_config_reload_workers: Default::default(),
        }
    }

    async fn probe_remote_version_state_fleet(&self, topology_fingerprint: &str) -> Result<BTreeMap<String, Uuid>> {
        if self.peer_clients.len() != self.peer_topology_hosts.len() {
            return Err(Error::other("remote version state capability fleet membership is incomplete"));
        }
        let probes = self.peer_clients.iter().map(|client| async {
            let client = client
                .as_ref()
                .ok_or_else(|| Error::other("remote version state capability peer is unreachable"))?;
            client.probe_remote_version_state(topology_fingerprint.to_string()).await
        });
        let mut peer_epochs = BTreeMap::new();
        for result in join_all(probes).await {
            let (peer, epoch) = result?;
            insert_remote_version_state_peer(&mut peer_epochs, peer, epoch)?;
        }
        Ok(peer_epochs)
    }

    async fn probe_transition_transaction_compaction_fleet(&self, topology_fingerprint: &str) -> Result<BTreeMap<String, Uuid>> {
        if self.peer_clients.len() != self.peer_topology_hosts.len() {
            return Err(Error::other(
                "transition transaction compaction capability fleet membership is incomplete",
            ));
        }
        let probes = self.peer_clients.iter().map(|client| async {
            let client = client
                .as_ref()
                .ok_or_else(|| Error::other("transition transaction compaction capability peer is unreachable"))?;
            client
                .probe_transition_transaction_compaction(topology_fingerprint.to_string())
                .await
        });
        let mut peer_epochs = BTreeMap::new();
        for result in join_all(probes).await {
            let (peer, epoch) = result?;
            insert_remote_version_state_peer(&mut peer_epochs, peer, epoch)?;
        }
        Ok(peer_epochs)
    }

    async fn probe_cross_pool_fence_fleet(&self, topology_fingerprint: &str) -> Result<(BTreeMap<String, Uuid>, u32)> {
        if self.peer_clients.len() != self.peer_topology_hosts.len() {
            return Err(Error::other("cross-pool fence capability fleet membership is incomplete"));
        }
        let probes = self.peer_clients.iter().map(|client| async {
            let client = client
                .as_ref()
                .ok_or_else(|| Error::other("cross-pool fence capability peer is unreachable"))?;
            client.probe_cross_pool_fence(topology_fingerprint.to_string()).await
        });
        let mut peer_epochs = BTreeMap::new();
        let mut minimum_version = LOCAL_CROSS_POOL_FENCE_POLICY_SUPPORTED_VERSION;
        for result in join_all(probes).await {
            let (peer, version, epoch) = result?;
            if version < CROSS_POOL_FENCE_SUPPORTED_VERSION {
                return Err(Error::other("cross-pool fence capability version is unsupported"));
            }
            minimum_version = minimum_version.min(version);
            insert_remote_version_state_peer(&mut peer_epochs, peer, epoch)?;
        }
        Ok((peer_epochs, minimum_version))
    }

    async fn probe_ilm_recovery_export_fleet(&self, topology_fingerprint: &str) -> Result<BTreeMap<String, Uuid>> {
        if self.peer_clients.len() != self.peer_topology_hosts.len() {
            return Err(Error::other("ILM recovery export capability fleet membership is incomplete"));
        }
        let local_member = runtime_sources::local_node_name().await;
        if local_member.trim().is_empty() {
            return Err(Error::other("ILM recovery export local member identity is unavailable"));
        }
        let mut peer_epochs = BTreeMap::new();
        insert_remote_version_state_peer(&mut peer_epochs, local_member.clone(), ilm_recovery_export_local_process_epoch())?;
        let probes = self.peer_clients.iter().map(|client| async {
            let client = client
                .as_ref()
                .ok_or_else(|| Error::other("ILM recovery export capability peer is unreachable"))?;
            client.probe_ilm_recovery_export(topology_fingerprint.to_string()).await
        });
        for result in join_all(probes).await {
            let (peer, epoch) = result?;
            insert_remote_version_state_peer(&mut peer_epochs, peer, epoch)?;
        }
        validate_ilm_recovery_export_members(&self.peer_topology_hosts, &local_member, &peer_epochs)?;
        Ok(peer_epochs)
    }
}

fn validate_ilm_recovery_export_members(
    expected_remote_members: &[String],
    local_member: &str,
    observed: &BTreeMap<String, Uuid>,
) -> Result<()> {
    let expected = expected_remote_members
        .iter()
        .cloned()
        .chain(std::iter::once(local_member.to_string()))
        .collect::<BTreeSet<_>>();
    if expected.len() != expected_remote_members.len().saturating_add(1) || observed.keys().ne(expected.iter()) {
        return Err(Error::other("ILM recovery export capability fleet membership does not match topology"));
    }
    Ok(())
}

/// Rolling tier activity summed over every cluster member that answered, with
/// the reporting coverage behind the sum.
///
/// The coverage is part of the result rather than a log line: a sum over a
/// subset of the cluster is not a cluster total, and a caller that renders it
/// as one is the defect this type exists to prevent.
pub struct ClusterTierDailyStats {
    pub stats: DailyAllTierStats,
    /// Members whose rolling day is included, always at least this node.
    pub nodes_reporting: usize,
    /// Members this deployment expects to hear from, including this node.
    pub nodes_expected: usize,
    /// Members that could not be asked, could not answer, or answered with a
    /// ring this build refuses to merge. Sorted, and named by grid host.
    pub unavailable_nodes: Vec<String>,
}

impl ClusterTierDailyStats {
    pub fn is_complete(&self) -> bool {
        self.unavailable_nodes.is_empty() && self.nodes_reporting == self.nodes_expected
    }
}

/// Fold one member's rolling day into the running cluster total.
///
/// Merging (rather than adding totals) ages each member's ring to the newer
/// clock first, so a member that stopped transitioning yesterday contributes
/// only the hours still inside the rolling day.
fn merge_tier_daily_stats(into: &mut DailyAllTierStats, from: DailyAllTierStats) {
    for (tier, stats) in from {
        match into.remove(&tier) {
            Some(existing) => {
                into.insert(tier, existing.merge(stats));
            }
            None => {
                into.insert(tier, stats);
            }
        }
    }
}

impl NotificationSys {
    /// Sum this node's rolling tier activity with every reachable peer's.
    ///
    /// Each node records only the transitions it completed itself, so the sum
    /// is a cluster total and a retried transition is counted once, by the
    /// node that finally committed it. Peers are probed concurrently under a
    /// per-peer deadline so one black-holed member cannot hold the admin
    /// request open.
    pub async fn tier_daily_stats(&self, local: DailyAllTierStats) -> ClusterTierDailyStats {
        let mut stats = local;
        let nodes_expected = self.peer_clients.len() + 1;
        let mut nodes_reporting = 1;
        let mut unavailable_nodes = Vec::new();

        let mut probes = Vec::with_capacity(self.peer_clients.len());
        for (idx, client) in self.peer_clients.iter().enumerate() {
            let host = self.tier_daily_stats_peer_host(idx, client.as_ref());
            probes.push(async move {
                let Some(client) = client.as_ref() else {
                    return (host, Err(Error::other("peer is not reachable")));
                };
                // The peer is already named by the caller's `peer` log field
                // and by `unavailable_nodes`, so the deadline is reported as
                // the typed variant rather than a formatted fragment.
                let result = timeout(TIER_DAILY_STATS_PROBE_TIMEOUT, client.tier_daily_stats())
                    .await
                    .unwrap_or(Err(Error::Timeout));
                (host, result)
            });
        }

        for (host, result) in join_all(probes).await {
            match result {
                Ok(peer_stats) => {
                    nodes_reporting += 1;
                    merge_tier_daily_stats(&mut stats, peer_stats);
                }
                Err(err) => {
                    warn!(
                        event = EVENT_NOTIFICATION_CAPABILITY_PROBE,
                        component = LOG_COMPONENT_ECSTORE,
                        subsystem = LOG_SUBSYSTEM_NOTIFICATION,
                        peer = host,
                        error = %err,
                        "tier daily stats peer did not report"
                    );
                    unavailable_nodes.push(host);
                }
            }
        }

        unavailable_nodes.sort();
        ClusterTierDailyStats {
            stats,
            nodes_reporting,
            nodes_expected,
            unavailable_nodes,
        }
    }

    /// Name a peer slot even when no client was ever built for it, so an
    /// unreachable member is reported by host instead of disappearing.
    fn tier_daily_stats_peer_host(&self, idx: usize, client: Option<&PeerRestClient>) -> String {
        if let Some(client) = client {
            return client.grid_host.clone();
        }
        self.peer_topology_hosts
            .get(idx)
            .cloned()
            .unwrap_or_else(|| format!("peer[{idx}]"))
    }
}

pub struct NotificationPeerErr {
    pub host: String,
    pub err: Option<Error>,
}

/// One peer's answer to a KMS configuration fingerprint probe.
pub struct PeerKmsConfigFingerprint {
    pub host: String,
    /// `None` when the peer has no KMS configuration of its own, or could not
    /// be asked at all, in which case `err` carries the reason.
    pub fingerprint: Option<String>,
    pub err: Option<Error>,
}

fn notification_peer_result<T>(host: String, result: Result<T>) -> NotificationPeerErr {
    NotificationPeerErr { host, err: result.err() }
}

fn unreachable_notification_peer_err() -> NotificationPeerErr {
    NotificationPeerErr {
        host: String::new(),
        err: Some(Error::other("peer is not reachable")),
    }
}

impl NotificationSys {
    pub fn rest_client_from_hash(&self, s: &str) -> Option<PeerRestClient> {
        if self.all_peer_clients.is_empty() {
            return None;
        }
        let mut hasher = DefaultHasher::new();
        s.hash(&mut hasher);
        let idx = (hasher.finish() as usize) % self.all_peer_clients.len();
        self.all_peer_clients[idx].clone()
    }

    pub fn peer_client_for_grid_host(&self, grid_host: &str) -> Option<PeerRestClient> {
        self.all_peer_clients
            .iter()
            .flatten()
            .find(|client| client.grid_host == grid_host)
            .cloned()
    }

    pub async fn delete_policy(&self, policy_name: &str) -> Vec<NotificationPeerErr> {
        let mut futures = Vec::with_capacity(self.peer_clients.len());
        for client in self.peer_clients.iter() {
            let policy = policy_name.to_string();
            futures.push(async move {
                if let Some(client) = client {
                    match client.delete_policy(&policy).await {
                        Ok(_) => NotificationPeerErr {
                            host: client.host.to_string(),
                            err: None,
                        },
                        Err(e) => NotificationPeerErr {
                            host: client.host.to_string(),
                            err: Some(e),
                        },
                    }
                } else {
                    NotificationPeerErr {
                        host: "".to_string(),
                        err: Some(Error::other("peer is not reachable")),
                    }
                }
            });
        }
        join_all(futures).await
    }

    pub async fn load_policy(&self, policy_name: &str) -> Vec<NotificationPeerErr> {
        let mut futures = Vec::with_capacity(self.peer_clients.len());
        for client in self.peer_clients.iter() {
            let policy = policy_name.to_string();
            futures.push(async move {
                if let Some(client) = client {
                    match client.load_policy(&policy).await {
                        Ok(_) => NotificationPeerErr {
                            host: client.host.to_string(),
                            err: None,
                        },
                        Err(e) => NotificationPeerErr {
                            host: client.host.to_string(),
                            err: Some(e),
                        },
                    }
                } else {
                    NotificationPeerErr {
                        host: "".to_string(),
                        err: Some(Error::other("peer is not reachable")),
                    }
                }
            });
        }
        join_all(futures).await
    }

    pub async fn load_policy_mapping(&self, user_or_group: &str, user_type: u64, is_group: bool) -> Vec<NotificationPeerErr> {
        let mut futures = Vec::with_capacity(self.peer_clients.len());
        for client in self.peer_clients.iter() {
            let uog = user_or_group.to_string();
            futures.push(async move {
                if let Some(client) = client {
                    match client.load_policy_mapping(&uog, user_type, is_group).await {
                        Ok(_) => NotificationPeerErr {
                            host: client.host.to_string(),
                            err: None,
                        },
                        Err(e) => NotificationPeerErr {
                            host: client.host.to_string(),
                            err: Some(e),
                        },
                    }
                } else {
                    NotificationPeerErr {
                        host: "".to_string(),
                        err: Some(Error::other("peer is not reachable")),
                    }
                }
            });
        }
        join_all(futures).await
    }

    pub async fn delete_user(&self, access_key: &str) -> Vec<NotificationPeerErr> {
        let mut futures = Vec::with_capacity(self.peer_clients.len());
        for client in self.peer_clients.iter() {
            let ak = access_key.to_string();
            futures.push(async move {
                if let Some(client) = client {
                    match client.delete_user(&ak).await {
                        Ok(_) => NotificationPeerErr {
                            host: client.host.to_string(),
                            err: None,
                        },
                        Err(e) => NotificationPeerErr {
                            host: client.host.to_string(),
                            err: Some(e),
                        },
                    }
                } else {
                    NotificationPeerErr {
                        host: "".to_string(),
                        err: Some(Error::other("peer is not reachable")),
                    }
                }
            });
        }
        join_all(futures).await
    }

    async fn signal_dynamic_config(&self, sub_sys: &str, dry_run: bool) -> Vec<NotificationPeerErr> {
        let mut futures = Vec::with_capacity(self.peer_clients.len());
        for client in self.peer_clients.iter() {
            let sub_sys = sub_sys.to_string();
            futures.push(async move {
                if let Some(client) = client {
                    match client
                        .signal_service(
                            crate::cluster::rpc::SERVICE_SIGNAL_RELOAD_DYNAMIC,
                            &sub_sys,
                            dry_run,
                            SystemTime::UNIX_EPOCH,
                        )
                        .await
                    {
                        Ok(_) => NotificationPeerErr {
                            host: client.host.to_string(),
                            err: None,
                        },
                        Err(e) => NotificationPeerErr {
                            host: client.host.to_string(),
                            err: Some(e),
                        },
                    }
                } else {
                    NotificationPeerErr {
                        host: "".to_string(),
                        err: Some(Error::other("peer is not reachable")),
                    }
                }
            });
        }
        join_all(futures).await
    }

    pub async fn preflight_dynamic_config(&self, sub_sys: &str) -> Vec<NotificationPeerErr> {
        self.signal_dynamic_config(sub_sys, true).await
    }

    pub async fn reload_dynamic_config(&self, sub_sys: &str) -> Vec<NotificationPeerErr> {
        self.signal_dynamic_config(sub_sys, false).await
    }

    /// Ask every peer to re-read the cluster-persisted KMS configuration.
    ///
    /// Best-effort by contract: the caller has already switched locally, so a
    /// peer that fails is reported rather than rolled back. Peers built before
    /// the KMS subsystem existed reject the signal with an explicit error.
    pub async fn reload_kms_config(&self) -> Vec<NotificationPeerErr> {
        self.reload_dynamic_config(crate::cluster::rpc::KMS_SIGNAL_SUBSYSTEM).await
    }

    /// Collect the KMS configuration fingerprint each peer is running.
    ///
    /// A peer whose build predates the KMS subsystem rejects the probe, so it
    /// is reported as an error rather than silently agreeing with this node.
    pub async fn kms_config_fingerprints(&self) -> Vec<PeerKmsConfigFingerprint> {
        let mut futures = Vec::with_capacity(self.peer_clients.len());
        for client in self.peer_clients.iter() {
            futures.push(async move {
                let Some(client) = client else {
                    return PeerKmsConfigFingerprint {
                        host: String::new(),
                        fingerprint: None,
                        err: Some(Error::other("peer is not reachable")),
                    };
                };
                match client.kms_config_fingerprint().await {
                    Ok(fingerprint) => PeerKmsConfigFingerprint {
                        host: client.host.to_string(),
                        fingerprint,
                        err: None,
                    },
                    Err(e) => PeerKmsConfigFingerprint {
                        host: client.host.to_string(),
                        fingerprint: None,
                        err: Some(e),
                    },
                }
            });
        }
        join_all(futures).await
    }

    pub async fn refresh_config_snapshot(&self) -> Vec<NotificationPeerErr> {
        let mut futures = Vec::with_capacity(self.peer_clients.len());
        for client in self.peer_clients.iter() {
            futures.push(async move {
                if let Some(client) = client {
                    match client
                        .signal_service(crate::cluster::rpc::SERVICE_SIGNAL_REFRESH_CONFIG, "", false, SystemTime::UNIX_EPOCH)
                        .await
                    {
                        Ok(_) => NotificationPeerErr {
                            host: client.host.to_string(),
                            err: None,
                        },
                        Err(e) => NotificationPeerErr {
                            host: client.host.to_string(),
                            err: Some(e),
                        },
                    }
                } else {
                    NotificationPeerErr {
                        host: "".to_string(),
                        err: Some(Error::other("peer is not reachable")),
                    }
                }
            });
        }
        join_all(futures).await
    }

    pub async fn storage_info<S>(&self, api: &S) -> rustfs_madmin::StorageInfo
    where
        S: StorageAdminApi<BackendInfo = rustfs_madmin::BackendInfo, StorageInfo = rustfs_madmin::StorageInfo>,
    {
        let mut futures = Vec::with_capacity(self.peer_clients.len());
        let endpoints = runtime_sources::endpoint_pools().unwrap_or_else(|| Vec::new().into());
        let peer_timeout = admin_peer_probe_timeout();

        for (idx, client) in self.peer_clients.iter().enumerate() {
            let endpoints = endpoints.clone();
            let cache = self.peer_admin_caches.get(idx);
            futures.push(async move {
                if let Some(client) = client {
                    let host = client.host.to_string();
                    let deadline = Instant::now() + peer_timeout;
                    let probe_timeout = remaining_admin_peer_probe_timeout(deadline).unwrap_or_default();
                    match timeout(probe_timeout, client.local_storage_info()).await {
                        Ok(Ok(mut info)) => {
                            normalize_and_cache_peer_storage_info(cache, &host, &mut info);
                            Some(info)
                        }
                        Ok(Err(err)) => {
                            warn!("peer {} storage_info failed: {}", host, err);
                            handle_peer_failure(cache, &host, &endpoints)
                        }
                        Err(_) => {
                            warn!("peer {} storage_info timed out after {:?}", host, peer_timeout);
                            handle_peer_failure(cache, &host, &endpoints)
                        }
                    }
                } else {
                    None
                }
            });
        }

        let mut replies = join_all(futures).await;

        replies.push(Some(StorageAdminApi::local_storage_info(api).await));

        let mut disks = Vec::new();
        for info in replies.into_iter().flatten() {
            disks.extend(info.disks);
        }

        let backend = StorageAdminApi::backend_info(api).await;
        rustfs_madmin::StorageInfo { disks, backend }
    }

    pub async fn server_info(&self) -> Vec<ServerProperties> {
        let mut futures = Vec::with_capacity(self.peer_clients.len());
        let endpoints = runtime_sources::endpoint_pools().unwrap_or_else(|| Vec::new().into());
        let peer_timeout = admin_peer_probe_timeout();

        for (idx, client) in self.peer_clients.iter().enumerate() {
            let host = self
                .peer_topology_hosts
                .get(idx)
                .cloned()
                .or_else(|| client.as_ref().map(|client| client.host.to_string()))
                .unwrap_or_default();
            futures.push(async move {
                let Some(client) = client else {
                    return PeerServerInfoProbe {
                        host,
                        result: Err(PeerServerInfoProbeFailure::NoClient),
                    };
                };

                let deadline = Instant::now() + peer_timeout;
                let Some(first_timeout) = remaining_admin_peer_probe_timeout(deadline) else {
                    let health = peer_disk_health_with_deadline(&host, deadline).await;
                    return PeerServerInfoProbe {
                        host,
                        result: Err(PeerServerInfoProbeFailure::Rpc { health }),
                    };
                };

                // First attempt. A single evicted or half-open internode channel
                // is enough to fail one probe and, before retrying, would drop
                // the member to unknown/offline for this whole snapshot. On a
                // quick failure we evict the channel and re-dial once before
                // falling back (rustfs/backlog#1049, P1-B). A slow attempt
                // consumes the round budget and therefore does not trigger a
                // second full wait or an asynchronous eviction side effect.
                match timeout(first_timeout, client.server_info()).await {
                    Ok(Ok(info)) => {
                        return PeerServerInfoProbe { host, result: Ok(info) };
                    }
                    Ok(Err(err)) => debug!("peer {host} server_info failed (attempt 1/2): {err}"),
                    Err(_) => debug!("peer {host} server_info timed out (attempt 1/2) after {peer_timeout:?}"),
                }

                // Drop the suspect channel AND clear the offline gate so the
                // retry actually re-dials. A network-like first failure runs
                // through `finalize_result`, which sets the offline gate; a bare
                // `evict_connection` would leave that gate up and the retry would
                // fast-fail with "temporarily offline" instead of reconnecting
                // (rustfs/backlog#1049 P1-B).
                let Some(retry_budget) = remaining_admin_peer_probe_timeout(deadline) else {
                    let health = peer_disk_health_with_deadline(&host, deadline).await;
                    return PeerServerInfoProbe {
                        host,
                        result: Err(PeerServerInfoProbeFailure::Rpc { health }),
                    };
                };
                // Bound connection-cache cleanup too. The helper clears the offline gate even
                // when eviction itself times out, so cancellation cannot strand this peer in
                // fast-fail mode.
                if !client.prepare_retry_with_timeout(retry_budget).await {
                    let health = peer_disk_health_with_deadline(&host, deadline).await;
                    return PeerServerInfoProbe {
                        host,
                        result: Err(PeerServerInfoProbeFailure::Rpc { health }),
                    };
                }

                // Second and final attempt on the fresh channel.
                let Some(retry_timeout) = remaining_admin_peer_probe_timeout(deadline) else {
                    let health = peer_disk_health_with_deadline(&host, deadline).await;
                    return PeerServerInfoProbe {
                        host,
                        result: Err(PeerServerInfoProbeFailure::Rpc { health }),
                    };
                };
                match timeout(retry_timeout, client.server_info()).await {
                    Ok(Ok(info)) => PeerServerInfoProbe { host, result: Ok(info) },
                    Ok(Err(err)) => {
                        warn!("peer {host} server_info failed after retry: {err}");
                        let health = peer_disk_health_with_deadline(&host, deadline).await;
                        PeerServerInfoProbe {
                            host,
                            result: Err(PeerServerInfoProbeFailure::Rpc { health }),
                        }
                    }
                    Err(_) => {
                        warn!("peer {host} server_info timed out after retry ({peer_timeout:?})");
                        let health = peer_disk_health_with_deadline(&host, deadline).await;
                        PeerServerInfoProbe {
                            host,
                            result: Err(PeerServerInfoProbeFailure::Rpc { health }),
                        }
                    }
                }
            });
        }

        publish_server_info_probe_round(&self.peer_admin_caches, &endpoints, join_all(futures).await)
    }

    pub async fn load_user(&self, access_key: &str, temp: bool) -> Vec<NotificationPeerErr> {
        let mut futures = Vec::with_capacity(self.peer_clients.len());
        for client in self.peer_clients.iter() {
            let ak = access_key.to_string();
            futures.push(async move {
                if let Some(client) = client {
                    match client.load_user(&ak, temp).await {
                        Ok(_) => NotificationPeerErr {
                            host: client.host.to_string(),
                            err: None,
                        },
                        Err(e) => NotificationPeerErr {
                            host: client.host.to_string(),
                            err: Some(e),
                        },
                    }
                } else {
                    NotificationPeerErr {
                        host: "".to_string(),
                        err: Some(Error::other("peer is not reachable")),
                    }
                }
            });
        }
        join_all(futures).await
    }

    pub async fn load_group(&self, group: &str) -> Vec<NotificationPeerErr> {
        let mut futures = Vec::with_capacity(self.peer_clients.len());
        for client in self.peer_clients.iter() {
            let gname = group.to_string();
            futures.push(async move {
                if let Some(client) = client {
                    match client.load_group(&gname).await {
                        Ok(_) => NotificationPeerErr {
                            host: client.host.to_string(),
                            err: None,
                        },
                        Err(e) => NotificationPeerErr {
                            host: client.host.to_string(),
                            err: Some(e),
                        },
                    }
                } else {
                    NotificationPeerErr {
                        host: "".to_string(),
                        err: Some(Error::other("peer is not reachable")),
                    }
                }
            });
        }
        join_all(futures).await
    }

    pub async fn delete_service_account(&self, access_key: &str) -> Vec<NotificationPeerErr> {
        let mut futures = Vec::with_capacity(self.peer_clients.len());
        for client in self.peer_clients.iter() {
            let ak = access_key.to_string();
            futures.push(async move {
                if let Some(client) = client {
                    match client.delete_service_account(&ak).await {
                        Ok(_) => NotificationPeerErr {
                            host: client.host.to_string(),
                            err: None,
                        },
                        Err(e) => NotificationPeerErr {
                            host: client.host.to_string(),
                            err: Some(e),
                        },
                    }
                } else {
                    NotificationPeerErr {
                        host: "".to_string(),
                        err: Some(Error::other("peer is not reachable")),
                    }
                }
            });
        }
        join_all(futures).await
    }

    pub async fn load_service_account(&self, access_key: &str) -> Vec<NotificationPeerErr> {
        let mut futures = Vec::with_capacity(self.peer_clients.len());
        for client in self.peer_clients.iter() {
            let ak = access_key.to_string();
            futures.push(async move {
                if let Some(client) = client {
                    match client.load_service_account(&ak).await {
                        Ok(_) => NotificationPeerErr {
                            host: client.host.to_string(),
                            err: None,
                        },
                        Err(e) => NotificationPeerErr {
                            host: client.host.to_string(),
                            err: Some(e),
                        },
                    }
                } else {
                    NotificationPeerErr {
                        host: "".to_string(),
                        err: Some(Error::other("peer is not reachable")),
                    }
                }
            });
        }
        join_all(futures).await
    }

    pub async fn reload_pool_meta(&self) -> Result<()> {
        let mut failures = Vec::new();
        let mut futures = Vec::with_capacity(self.peer_clients.len());
        for (idx, client) in self.peer_clients.iter().enumerate() {
            if let Some(client) = client {
                let host = client.grid_host.clone();
                futures.push(async move { client.reload_pool_meta().await.map_err(|err| (host, err)) });
            } else {
                warn!(
                    event = EVENT_NOTIFICATION_PEER_PROPAGATION,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_NOTIFICATION,
                    action = "reload_pool_meta",
                    result = "peer_unreachable",
                    peer_index = idx,
                    "notification peer propagation"
                );
                failures.push(format!("peer[{idx}] reload_pool_meta failed: peer is not reachable"));
            }
        }

        for result in join_all(futures).await {
            if let Err((host, err)) = result {
                let failure = format!("peer {host} reload_pool_meta failed: {err}");
                error!(
                    event = EVENT_NOTIFICATION_PEER_PROPAGATION,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_NOTIFICATION,
                    action = "reload_pool_meta",
                    result = "peer_failed",
                    peer = %host,
                    error = %err,
                    "notification peer propagation"
                );
                failures.push(failure);
            }
        }

        aggregate_notification_failures("reload_pool_meta", failures)
    }

    #[tracing::instrument(skip(self))]
    pub async fn load_rebalance_meta(&self, start: bool) -> Result<()> {
        let failures = self.load_rebalance_meta_failures(start).await?;
        aggregate_notification_failures("load_rebalance_meta", failures)
    }

    #[tracing::instrument(skip(self))]
    pub async fn load_rebalance_meta_failures(&self, start: bool) -> Result<Vec<String>> {
        let operation = format!("load_rebalance_meta(start={start})");
        let mut failures = Vec::new();
        let mut futures = Vec::with_capacity(self.peer_clients.len());
        for (idx, client) in self.peer_clients.iter().enumerate() {
            if let Some(client) = client {
                let host = client.grid_host.clone();
                futures.push(async move {
                    let result = client.load_rebalance_meta(start).await;
                    (host, result)
                });
            } else {
                warn!(
                    event = EVENT_NOTIFICATION_PEER_PROPAGATION,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_NOTIFICATION,
                    action = "load_rebalance_meta",
                    result = "peer_unreachable",
                    peer_index = idx,
                    start_rebalance = start,
                    "notification peer propagation"
                );
                failures.push(format!("peer[{idx}] {operation} failed: peer is not reachable"));
            }
        }

        for (host, result) in join_all(futures).await {
            if let Err(err) = result {
                let failure = format!("peer {host} {operation} failed: {err}");
                error!(
                    event = EVENT_NOTIFICATION_PEER_PROPAGATION,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_NOTIFICATION,
                    action = "load_rebalance_meta",
                    result = "peer_failed",
                    peer = %host,
                    start_rebalance = start,
                    error = %err,
                    "notification peer propagation"
                );
                failures.push(failure);
            } else {
                debug!(
                    event = EVENT_NOTIFICATION_PEER_PROPAGATION,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_NOTIFICATION,
                    action = "load_rebalance_meta",
                    result = "peer_success",
                    peer = %host,
                    start_rebalance = start,
                    "notification peer propagation"
                );
            }
        }

        Ok(failures)
    }

    pub async fn stop_rebalance(&self, expected_rebalance_id: Option<&str>) -> Result<()> {
        let failures = self.stop_rebalance_failures(expected_rebalance_id).await?;
        aggregate_notification_failures("stop_rebalance", failures)
    }

    pub async fn stop_rebalance_failures(&self, expected_rebalance_id: Option<&str>) -> Result<Vec<String>> {
        info!(
            event = EVENT_NOTIFICATION_PEER_PROPAGATION,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_NOTIFICATION,
            action = "stop_rebalance",
            state = "started",
            "notification peer propagation"
        );
        let Some(store) = runtime_sources::object_store_handle() else {
            error!(
                event = EVENT_NOTIFICATION_PEER_PROPAGATION,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_NOTIFICATION,
                action = "stop_rebalance",
                result = "failed",
                reason = "object_layer_not_initialized",
                "notification peer propagation"
            );
            return Err(Error::other("stop_rebalance: object layer not initialized"));
        };

        let mut failures = Vec::new();

        let mut futures = Vec::with_capacity(self.peer_clients.len());
        for (idx, client) in self.peer_clients.iter().enumerate() {
            if let Some(client) = client {
                let host = client.grid_host.clone();
                futures.push(async move {
                    let result = client.stop_rebalance(expected_rebalance_id).await;
                    (host, result)
                });
            } else {
                warn!(
                    event = EVENT_NOTIFICATION_PEER_PROPAGATION,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_NOTIFICATION,
                    action = "stop_rebalance",
                    result = "peer_unreachable",
                    peer_index = idx,
                    "notification peer propagation"
                );
                failures.push(format!("peer[{idx}] stop_rebalance failed: peer is not reachable"));
            }
        }

        for (host, result) in join_all(futures).await {
            if let Err(err) = result {
                let failure = format!("peer {host} stop_rebalance failed: {err}");
                error!(
                    event = EVENT_NOTIFICATION_PEER_PROPAGATION,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_NOTIFICATION,
                    action = "stop_rebalance",
                    result = "peer_failed",
                    peer = %host,
                    error = %err,
                    "notification peer propagation"
                );
                failures.push(failure);
            } else {
                debug!(
                    event = EVENT_NOTIFICATION_PEER_PROPAGATION,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_NOTIFICATION,
                    action = "stop_rebalance",
                    result = "peer_success",
                    peer = %host,
                    "notification peer propagation"
                );
            }
        }

        let local_rebalance_id = match expected_rebalance_id {
            Some(expected_id) => Some(expected_id.to_owned()),
            None => store.current_rebalance_id().await,
        };
        match store.stop_rebalance_for_id(local_rebalance_id.as_deref()).await {
            Ok(_) => {
                let save_result = match local_rebalance_id.as_deref() {
                    Some(expected_id) => {
                        store
                            .save_rebalance_stats_for_id(usize::MAX, RebalSaveOpt::StoppedAt, expected_id)
                            .await
                    }
                    None => Ok(()),
                };
                if let Err(err) = save_result {
                    error!(
                        event = EVENT_NOTIFICATION_PEER_PROPAGATION,
                        component = LOG_COMPONENT_ECSTORE,
                        subsystem = LOG_SUBSYSTEM_NOTIFICATION,
                        action = "stop_rebalance",
                        result = "local_save_failed",
                        error = %err,
                        "notification peer propagation"
                    );
                    return Err(Error::other(format!(
                        "local stop_rebalance save_rebalance_stats(stopped_at) failed: {err}"
                    )));
                }
            }
            Err(err) => {
                error!(
                    event = EVENT_NOTIFICATION_PEER_PROPAGATION,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_NOTIFICATION,
                    action = "stop_rebalance",
                    result = "local_stop_failed",
                    error = %err,
                    "notification peer propagation"
                );
                return Err(Error::other(format!("local stop_rebalance stop failed: {err}")));
            }
        }

        info!(
            event = EVENT_NOTIFICATION_PEER_PROPAGATION,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_NOTIFICATION,
            action = "stop_rebalance",
            result = if failures.is_empty() { "success" } else { "partial_failure" },
            "notification peer propagation"
        );
        Ok(failures)
    }

    pub async fn load_bucket_metadata(&self, bucket: &str) -> Result<()> {
        self.load_bucket_metadata_with_scanner_maintenance(bucket, false).await
    }

    pub async fn load_bucket_metadata_for_scanner_maintenance(&self, bucket: &str) -> Result<()> {
        self.load_bucket_metadata_with_scanner_maintenance(bucket, true).await
    }

    async fn load_bucket_metadata_with_scanner_maintenance(&self, bucket: &str, scanner_maintenance_change: bool) -> Result<()> {
        let operation = format!("load_bucket_metadata({bucket})");
        let mut failures = Vec::new();
        let mut futures = Vec::with_capacity(self.peer_clients.len());
        for (idx, client) in self.peer_clients.iter().enumerate() {
            if let Some(client) = client {
                let host = client.host.to_string();
                let b = bucket.to_string();
                futures.push(async move {
                    client
                        .load_bucket_metadata(&b, scanner_maintenance_change)
                        .await
                        .map_err(|err| (host, err))
                });
            } else {
                failures.push(format!("peer[{idx}] {operation} failed: peer is not reachable"));
            }
        }

        for result in join_all(futures).await {
            if let Err((host, err)) = result {
                let failure = format!("peer {host} {operation} failed: {err}");
                error!("notification {operation} err {failure}");
                failures.push(failure);
            }
        }

        aggregate_notification_failures(&operation, failures)
    }

    pub async fn delete_bucket_metadata(&self, bucket: &str) -> Vec<NotificationPeerErr> {
        let mut futures = Vec::with_capacity(self.peer_clients.len());
        for client in self.peer_clients.iter() {
            let b = bucket.to_string();
            futures.push(async move {
                if let Some(client) = client {
                    match client.delete_bucket_metadata(&b).await {
                        Ok(_) => NotificationPeerErr {
                            host: client.host.to_string(),
                            err: None,
                        },
                        Err(e) => NotificationPeerErr {
                            host: client.host.to_string(),
                            err: Some(e),
                        },
                    }
                } else {
                    NotificationPeerErr {
                        host: "".to_string(),
                        err: Some(Error::other("peer is not reachable")),
                    }
                }
            });
        }
        join_all(futures).await
    }

    pub async fn start_profiling(&self, profiler: &str) -> Vec<NotificationPeerErr> {
        let mut futures = Vec::with_capacity(self.peer_clients.len());
        for client in self.peer_clients.iter() {
            let pf = profiler.to_string();
            futures.push(async move {
                if let Some(client) = client {
                    match client.start_profiling(&pf).await {
                        Ok(_) => NotificationPeerErr {
                            host: client.host.to_string(),
                            err: None,
                        },
                        Err(e) => NotificationPeerErr {
                            host: client.host.to_string(),
                            err: Some(e),
                        },
                    }
                } else {
                    NotificationPeerErr {
                        host: "".to_string(),
                        err: Some(Error::other("peer is not reachable")),
                    }
                }
            });
        }
        join_all(futures).await
    }

    pub async fn get_cpus(&self) -> Vec<Cpus> {
        let mut futures = Vec::with_capacity(self.peer_clients.len());
        for client in self.peer_clients.iter().cloned() {
            futures.push(async move {
                if let Some(client) = client {
                    client.get_cpus().await.unwrap_or_default()
                } else {
                    Cpus::default()
                }
            });
        }
        join_all(futures).await
    }

    pub async fn get_net_info(&self) -> Vec<NetInfo> {
        let mut futures = Vec::with_capacity(self.peer_clients.len());
        for client in self.peer_clients.iter().cloned() {
            futures.push(async move {
                if let Some(client) = client {
                    client.get_net_info().await.unwrap_or_default()
                } else {
                    NetInfo::default()
                }
            });
        }
        join_all(futures).await
    }

    pub async fn get_partitions(&self) -> Vec<Partitions> {
        let mut futures = Vec::with_capacity(self.peer_clients.len());
        for client in self.peer_clients.iter().cloned() {
            futures.push(async move {
                if let Some(client) = client {
                    client.get_partitions().await.unwrap_or_default()
                } else {
                    Partitions::default()
                }
            });
        }
        join_all(futures).await
    }

    pub async fn get_os_info(&self) -> Vec<OsInfo> {
        let mut futures = Vec::with_capacity(self.peer_clients.len());
        for client in self.peer_clients.iter().cloned() {
            futures.push(async move {
                if let Some(client) = client {
                    client.get_os_info().await.unwrap_or_default()
                } else {
                    OsInfo::default()
                }
            });
        }
        join_all(futures).await
    }

    pub async fn get_sys_services(&self) -> Vec<SysServices> {
        let mut futures = Vec::with_capacity(self.peer_clients.len());
        for client in self.peer_clients.iter().cloned() {
            futures.push(async move {
                if let Some(client) = client {
                    client.get_se_linux_info().await.unwrap_or_default()
                } else {
                    SysServices::default()
                }
            });
        }
        join_all(futures).await
    }

    pub async fn get_sys_config(&self) -> Vec<SysConfig> {
        let mut futures = Vec::with_capacity(self.peer_clients.len());
        for client in self.peer_clients.iter().cloned() {
            futures.push(async move {
                if let Some(client) = client {
                    client.get_sys_config().await.unwrap_or_default()
                } else {
                    SysConfig::default()
                }
            });
        }
        join_all(futures).await
    }

    pub async fn get_sys_errors(&self) -> Vec<SysErrors> {
        let mut futures = Vec::with_capacity(self.peer_clients.len());
        for client in self.peer_clients.iter().cloned() {
            futures.push(async move {
                if let Some(client) = client {
                    client.get_sys_errors().await.unwrap_or_default()
                } else {
                    SysErrors::default()
                }
            });
        }
        join_all(futures).await
    }

    pub async fn get_mem_info(&self) -> Vec<MemInfo> {
        let mut futures = Vec::with_capacity(self.peer_clients.len());
        for client in self.peer_clients.iter().cloned() {
            futures.push(async move {
                if let Some(client) = client {
                    client.get_mem_info().await.unwrap_or_default()
                } else {
                    MemInfo::default()
                }
            });
        }
        join_all(futures).await
    }

    pub async fn get_proc_info(&self) -> Vec<ProcInfo> {
        let mut futures = Vec::with_capacity(self.peer_clients.len());
        for client in self.peer_clients.iter().cloned() {
            futures.push(async move {
                if let Some(client) = client {
                    client.get_proc_info().await.unwrap_or_default()
                } else {
                    ProcInfo::default()
                }
            });
        }
        join_all(futures).await
    }

    pub async fn get_metrics(&self, t: MetricType, opts: &CollectMetricsOpts) -> Vec<RealtimeMetrics> {
        let mut futures = Vec::with_capacity(self.peer_clients.len());
        for client in self.peer_clients.iter().cloned() {
            let t_clone = t;
            let opts_clone = opts;
            futures.push(async move {
                if let Some(client) = client {
                    client.get_metrics(t_clone, opts_clone).await.unwrap_or_default()
                } else {
                    RealtimeMetrics::default()
                }
            });
        }
        join_all(futures).await
    }

    pub async fn scanner_activity_snapshots(&self) -> Result<Vec<(String, ScannerPeerActivity)>> {
        if self.peer_clients.is_empty() {
            return Err(Error::other("scanner activity probe has no remote peers"));
        }
        if self.all_peer_clients.len() != self.peer_clients.len() + 1 {
            return Err(Error::other(format!(
                "scanner activity peer topology is incomplete: {} remote peers for {} cluster members",
                self.peer_clients.len(),
                self.all_peer_clients.len()
            )));
        }

        let mut futures = Vec::with_capacity(self.peer_clients.len());
        for (idx, client) in self.peer_clients.iter().cloned().enumerate() {
            futures.push(async move {
                let client = client.ok_or_else(|| Error::other(format!("scanner activity peer[{idx}] is unreachable")))?;
                let host = client.grid_host.clone();
                scanner_activity_with_retry(&client, &host)
                    .await
                    .map(|activity| (host, activity))
            });
        }

        let mut generations = Vec::with_capacity(futures.len());
        for result in join_all(futures).await {
            generations.push(result?);
        }
        Ok(generations)
    }

    pub async fn scanner_dirty_usage_snapshots(&self) -> Result<Vec<(String, ScannerPeerDirtyUsageSnapshot)>> {
        if self.peer_clients.is_empty() {
            return Err(Error::other("scanner dirty usage snapshot probe has no remote peers"));
        }
        if self.all_peer_clients.len() != self.peer_clients.len() + 1 {
            return Err(Error::other("scanner dirty usage snapshot peer topology is incomplete"));
        }

        let mut futures = Vec::with_capacity(self.peer_clients.len());
        for client in self.peer_clients.iter().cloned() {
            futures.push(async move {
                let client = client.ok_or_else(|| Error::other("scanner dirty usage snapshot peer is unreachable"))?;
                let host = client.grid_host.clone();
                scanner_dirty_usage_snapshot_with_retry(&client, &host)
                    .await
                    .map(|snapshot| (host, snapshot))
            });
        }

        let mut snapshots = Vec::with_capacity(futures.len());
        for result in join_all(futures).await {
            snapshots.push(result?);
        }
        Ok(snapshots)
    }

    pub async fn scanner_scoped_dirty_usage_capabilities(
        &self,
        acknowledgements: Vec<ScannerDirtyUsageAcknowledgement>,
    ) -> Result<bool> {
        let mut by_host = HashMap::with_capacity(acknowledgements.len());
        for acknowledgement in acknowledgements {
            let host = match &acknowledgement {
                ScannerDirtyUsageAcknowledgement::Scoped { host, .. } => host.clone(),
                ScannerDirtyUsageAcknowledgement::Generation { .. } => {
                    return Err(Error::other("scanner scoped dirty usage capability requires scoped acknowledgements"));
                }
            };
            if by_host.insert(host.clone(), acknowledgement).is_some() {
                return Err(Error::other("duplicate scanner dirty usage acknowledgement target"));
            }
        }

        let clients = self
            .peer_clients
            .iter()
            .flatten()
            .map(|client| (client.grid_host.clone(), client.clone()))
            .collect::<HashMap<_, _>>();
        let mut futures = Vec::with_capacity(by_host.len());
        for (host, acknowledgement) in by_host {
            let Some(client) = clients.get(&host).cloned() else {
                return Err(Error::other("scanner scoped dirty usage capability failed: peer is not reachable"));
            };
            futures.push(async move {
                let ScannerDirtyUsageAcknowledgement::Scoped {
                    owner_id,
                    instance_id,
                    entries,
                    ..
                } = acknowledgement
                else {
                    unreachable!("scoped acknowledgement was validated before probing");
                };
                timeout(
                    SCANNER_ACTIVITY_PROBE_TIMEOUT,
                    client.scanner_scoped_dirty_usage_capability(owner_id, instance_id, entries),
                )
                .await
                .map_err(|_| Error::other("scanner scoped dirty usage capability timed out"))?
            });
        }

        for result in join_all(futures).await {
            if !result? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    pub async fn acknowledge_scanner_dirty_usage(&self, acknowledgements: Vec<ScannerDirtyUsageAcknowledgement>) -> Result<bool> {
        let mut by_host = HashMap::with_capacity(acknowledgements.len());
        for acknowledgement in acknowledgements {
            let host = match &acknowledgement {
                ScannerDirtyUsageAcknowledgement::Generation { host, .. }
                | ScannerDirtyUsageAcknowledgement::Scoped { host, .. } => host.clone(),
            };
            if by_host.insert(host.clone(), acknowledgement).is_some() {
                return Err(Error::other("duplicate scanner dirty usage acknowledgement target"));
            }
        }

        let clients = self
            .peer_clients
            .iter()
            .flatten()
            .map(|client| (client.grid_host.clone(), client.clone()))
            .collect::<HashMap<_, _>>();
        let mut failures = Vec::new();
        let mut futures = Vec::with_capacity(by_host.len());
        for (host, acknowledgement) in by_host {
            let Some(client) = clients.get(&host).cloned() else {
                failures.push(format!("peer {host} scanner dirty usage acknowledgement failed: peer is not reachable"));
                continue;
            };
            futures.push(async move {
                let result = match acknowledgement {
                    ScannerDirtyUsageAcknowledgement::Generation {
                        instance_id, generation, ..
                    } => {
                        scanner_activity_with_timeout(
                            SCANNER_ACTIVITY_PROBE_TIMEOUT,
                            &host,
                            client.acknowledge_scanner_dirty_usage(instance_id, generation),
                        )
                        .await
                    }
                    ScannerDirtyUsageAcknowledgement::Scoped {
                        owner_id,
                        instance_id,
                        entries,
                        ..
                    } => {
                        client
                            .acknowledge_scanner_scoped_dirty_usage(owner_id, instance_id, entries)
                            .await
                    }
                };
                (host, result)
            });
        }
        aggregate_scanner_dirty_usage_acknowledgement_results(join_all(futures).await, failures)
    }

    /// Acquire remote publication leases in a deterministic host order. A
    /// missing/legacy peer is a hard publication deferral; already acquired
    /// leases are released before returning so a partial acquisition cannot
    /// pin movement on one peer.
    pub async fn acquire_scanner_publication_leases(
        &self,
        mut targets: Vec<(String, String, u64)>,
    ) -> Result<Vec<ScannerPublicationLeaseGrant>> {
        targets.sort_by(|left, right| left.0.cmp(&right.0));
        for pair in targets.windows(2) {
            if pair[0].0 == pair[1].0 {
                return Err(Error::other(format!("duplicate scanner publication lease target: {}", pair[0].0)));
            }
        }

        let mut grants = Vec::with_capacity(targets.len());
        for (host, session_id, generation) in targets {
            let Some(client) = self
                .peer_clients
                .iter()
                .flatten()
                .find(|client| client.grid_host == host)
                .cloned()
            else {
                let _ = self.release_scanner_publication_leases(grants).await;
                return Err(Error::other(format!("scanner publication lease peer {host} is unavailable")));
            };
            match client.acquire_scanner_publication_lease(&session_id, generation).await {
                Ok(lease) => grants.push(ScannerPublicationLeaseGrant { host, lease }),
                Err(err) => {
                    let _ = self.release_scanner_publication_leases(grants).await;
                    return Err(Error::other(format!("scanner publication lease acquisition failed: {err}")));
                }
            }
        }
        Ok(grants)
    }

    pub async fn release_scanner_publication_leases(&self, mut grants: Vec<ScannerPublicationLeaseGrant>) -> Result<()> {
        grants.sort_by(|left, right| right.host.cmp(&left.host));
        let mut failures = Vec::new();
        for grant in grants {
            let Some(client) = self
                .peer_clients
                .iter()
                .flatten()
                .find(|client| client.grid_host == grant.host)
            else {
                failures.push(format!("peer {} is unavailable", grant.host));
                continue;
            };
            if let Err(err) = client.release_scanner_publication_lease(&grant.lease).await {
                failures.push(format!("peer {} release failed: {err}", grant.host));
            }
        }
        if failures.is_empty() {
            Ok(())
        } else {
            Err(Error::other(format!(
                "scanner publication lease release failures: {}",
                failures.join("; ")
            )))
        }
    }

    /// Revalidate every remote lease in deterministic host order immediately
    /// before a final scanner metadata write.  A peer restart removes its
    /// process-owned token table and changes its activity session, so an old
    /// generation cannot pass this proof even when the numeric generation is
    /// reused.
    pub async fn validate_scanner_publication_leases(&self, grants: &[ScannerPublicationLeaseGrant]) -> Result<()> {
        let mut grants = grants.to_vec();
        grants.sort_by(|left, right| left.host.cmp(&right.host));
        for pair in grants.windows(2) {
            if pair[0].host == pair[1].host {
                return Err(Error::other(format!("duplicate scanner publication lease target: {}", pair[0].host)));
            }
        }
        for grant in grants {
            let Some(client) = self
                .peer_clients
                .iter()
                .flatten()
                .find(|client| client.grid_host == grant.host)
                .cloned()
            else {
                return Err(Error::other(format!("scanner publication lease peer {} is unavailable", grant.host)));
            };
            client
                .validate_scanner_publication_lease(&grant.lease)
                .await
                .map_err(|err| Error::other(format!("scanner publication lease validation failed for {}: {err}", grant.host)))?;
        }
        Ok(())
    }

    pub async fn reload_site_replication_config(&self) -> Vec<NotificationPeerErr> {
        let mut futures = Vec::with_capacity(self.peer_clients.len());
        for client in self.peer_clients.iter() {
            futures.push(async move {
                if let Some(client) = client {
                    match client.reload_site_replication_config().await {
                        Ok(_) => NotificationPeerErr {
                            host: client.host.to_string(),
                            err: None,
                        },
                        Err(e) => NotificationPeerErr {
                            host: client.host.to_string(),
                            err: Some(e),
                        },
                    }
                } else {
                    NotificationPeerErr {
                        host: "".to_string(),
                        err: Some(Error::other("peer is not reachable")),
                    }
                }
            });
        }
        join_all(futures).await
    }

    pub async fn load_transition_tier_config(&self) -> Vec<NotificationPeerErr> {
        let mut futures = Vec::with_capacity(self.peer_clients.len());
        for client in self.peer_clients.iter() {
            futures.push(async move {
                if let Some(client) = client {
                    match client.load_transition_tier_config().await {
                        Ok(_) => NotificationPeerErr {
                            host: client.host.to_string(),
                            err: None,
                        },
                        Err(e) => NotificationPeerErr {
                            host: client.host.to_string(),
                            err: Some(e),
                        },
                    }
                } else {
                    NotificationPeerErr {
                        host: "".to_string(),
                        err: Some(Error::other("peer is not reachable")),
                    }
                }
            });
        }
        join_all(futures).await
    }

    /// Starts one immediate configuration reload worker per peer. Concurrent
    /// tier mutations share the existing worker for that peer.
    pub fn spawn_transition_tier_config_reload_workers(self: &Arc<Self>) {
        self.spawn_transition_tier_config_reload_workers_with_cancel_token(runtime_sources::background_services_cancel_token());
    }

    fn spawn_transition_tier_config_reload_workers_with_cancel_token(self: &Arc<Self>, cancel_token: Option<CancellationToken>) {
        let Some(cancel_token) = cancel_token else {
            warn!(
                event = EVENT_NOTIFICATION_PEER_PROPAGATION,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_NOTIFICATION,
                action = "reload_transition_tier_config",
                result = "background_service_unavailable",
                "notification peer propagation"
            );
            return;
        };
        for (peer_index, client) in self.peer_clients.iter().enumerate() {
            let Some(client) = client.clone() else {
                warn!(
                    event = EVENT_NOTIFICATION_PEER_PROPAGATION,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_NOTIFICATION,
                    action = "reload_transition_tier_config",
                    peer_index,
                    result = "peer_unreachable",
                    "notification peer propagation"
                );
                continue;
            };
            let host = client.grid_host.clone();
            if !self.reserve_tier_config_reload_worker(&host) {
                debug!(
                    event = EVENT_NOTIFICATION_PEER_PROPAGATION,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_NOTIFICATION,
                    action = "reload_transition_tier_config",
                    host,
                    result = "coalesced",
                    "notification peer propagation"
                );
                continue;
            }
            let sys = Arc::clone(self);
            let cancel_token = cancel_token.clone();
            tokio::spawn(async move {
                run_tier_config_reload_worker(sys, host, cancel_token, move || {
                    let client = client.clone();
                    async move { client.load_transition_tier_config_single_attempt_outcome().await }
                })
                .await;
            });
        }
    }

    fn reserve_tier_config_reload_worker(&self, host: &str) -> bool {
        let mut workers = self
            .tier_config_reload_workers
            .lock()
            .expect("tier config reload worker state must not be poisoned");
        match workers.peers.get_mut(host) {
            Some(pending) => {
                *pending = true;
                false
            }
            None => {
                workers.peers.insert(host.to_string(), false);
                true
            }
        }
    }

    fn take_tier_config_reload_pending(&self, host: &str) -> bool {
        let mut workers = self
            .tier_config_reload_workers
            .lock()
            .expect("tier config reload worker state must not be poisoned");
        let Some(pending) = workers.peers.get_mut(host) else {
            return false;
        };
        let pending_reload = *pending;
        *pending = false;
        pending_reload
    }

    fn finish_tier_config_reload_worker(&self, host: &str) -> TierConfigReloadFinish {
        let mut workers = self
            .tier_config_reload_workers
            .lock()
            .expect("tier config reload worker state must not be poisoned");
        let Some(pending) = workers.peers.get_mut(host) else {
            return TierConfigReloadFinish::Completed;
        };
        if *pending {
            *pending = false;
            return TierConfigReloadFinish::Pending;
        }
        workers.peers.remove(host);
        TierConfigReloadFinish::Completed
    }

    fn cancel_tier_config_reload_worker(&self, host: &str) {
        let mut workers = self
            .tier_config_reload_workers
            .lock()
            .expect("tier config reload worker state must not be poisoned");
        workers.peers.remove(host);
    }

    #[allow(dead_code, reason = "asserted by this file's tests (backlog#1823)")]
    fn tier_config_reload_worker_active(&self, host: &str) -> bool {
        self.tier_config_reload_workers
            .lock()
            .expect("tier config reload worker state must not be poisoned")
            .peers
            .contains_key(host)
    }

    pub async fn prepare_tier_mutation(&self, mutation_id: Uuid, canonical_payload: Bytes) -> Vec<NotificationPeerErr> {
        let mut futures = Vec::with_capacity(self.peer_clients.len());
        for client in self.peer_clients.iter().cloned() {
            let payload = canonical_payload.clone();
            futures.push(async move {
                if let Some(client) = client {
                    notification_peer_result(client.host.to_string(), client.prepare_tier_mutation(mutation_id, payload).await)
                } else {
                    unreachable_notification_peer_err()
                }
            });
        }
        join_all(futures).await
    }

    pub async fn commit_tier_mutation(&self, mutation_id: Uuid, canonical_payload: Bytes) -> Vec<NotificationPeerErr> {
        let mut futures = Vec::with_capacity(self.peer_clients.len());
        for client in self.peer_clients.iter().cloned() {
            let payload = canonical_payload.clone();
            futures.push(async move {
                if let Some(client) = client {
                    notification_peer_result(client.host.to_string(), client.commit_tier_mutation(mutation_id, payload).await)
                } else {
                    unreachable_notification_peer_err()
                }
            });
        }
        join_all(futures).await
    }

    pub async fn abort_tier_mutation(&self, mutation_id: Uuid, canonical_prepare_payload: Bytes) -> Vec<NotificationPeerErr> {
        let mut futures = Vec::with_capacity(self.peer_clients.len());
        for client in self.peer_clients.iter().cloned() {
            let payload = canonical_prepare_payload.clone();
            futures.push(async move {
                if let Some(client) = client {
                    notification_peer_result(client.host.to_string(), client.abort_tier_mutation(mutation_id, payload).await)
                } else {
                    unreachable_notification_peer_err()
                }
            });
        }
        join_all(futures).await
    }
}

async fn run_tier_config_reload_worker<F, Fut>(
    sys: Arc<NotificationSys>,
    host: String,
    cancel_token: CancellationToken,
    mut reload: F,
) where
    F: FnMut() -> Fut,
    Fut: Future<Output = TierConfigReloadOutcome>,
{
    let mut retry_attempt = 0;
    loop {
        if cancel_token.is_cancelled() {
            sys.cancel_tier_config_reload_worker(&host);
            return;
        }
        let result = tokio::select! {
            _ = cancel_token.cancelled() => {
                sys.cancel_tier_config_reload_worker(&host);
                return;
            }
            result = reload() => result,
        };

        match result {
            TierConfigReloadOutcome::Success => match sys.finish_tier_config_reload_worker(&host) {
                TierConfigReloadFinish::Completed => {
                    debug!(
                        event = EVENT_NOTIFICATION_PEER_PROPAGATION,
                        component = LOG_COMPONENT_ECSTORE,
                        subsystem = LOG_SUBSYSTEM_NOTIFICATION,
                        action = "reload_transition_tier_config",
                        host,
                        result = "success",
                        "notification peer propagation"
                    );
                    return;
                }
                TierConfigReloadFinish::Pending => retry_attempt = 0,
            },
            TierConfigReloadOutcome::Terminal(err) => match sys.finish_tier_config_reload_worker(&host) {
                TierConfigReloadFinish::Completed => {
                    // This peer keeps the previous tier configuration for good, so record
                    // why. Dropping the error here hides the only evidence of a divergent
                    // node behind an outcome label that cannot be acted on.
                    warn!(
                        event = EVENT_NOTIFICATION_PEER_PROPAGATION,
                        component = LOG_COMPONENT_ECSTORE,
                        subsystem = LOG_SUBSYSTEM_NOTIFICATION,
                        action = "reload_transition_tier_config",
                        host,
                        outcome = "terminal",
                        error = ?err,
                        "tier configuration reload stopped after a terminal outcome"
                    );
                    return;
                }
                TierConfigReloadFinish::Pending => retry_attempt = 0,
            },
            TierConfigReloadOutcome::TransientReconnect(_) | TierConfigReloadOutcome::TransientRetrySameChannel(_) => {
                let delay = tier_config_reload_retry_delay(retry_attempt);
                retry_attempt = retry_attempt.saturating_add(1);
                if sys.take_tier_config_reload_pending(&host) {
                    retry_attempt = 0;
                    continue;
                }
                if retry_attempt == 1 {
                    warn!(
                        event = EVENT_NOTIFICATION_PEER_PROPAGATION,
                        component = LOG_COMPONENT_ECSTORE,
                        subsystem = LOG_SUBSYSTEM_NOTIFICATION,
                        action = "reload_transition_tier_config",
                        host,
                        retry_attempt,
                        retry_delay_ms = delay.as_millis(),
                        outcome = "transient",
                        "tier configuration reload failed; retrying"
                    );
                } else if retry_attempt.is_power_of_two() {
                    debug!(
                        event = EVENT_NOTIFICATION_PEER_PROPAGATION,
                        component = LOG_COMPONENT_ECSTORE,
                        subsystem = LOG_SUBSYSTEM_NOTIFICATION,
                        action = "reload_transition_tier_config",
                        host,
                        retry_attempt,
                        retry_delay_ms = delay.as_millis(),
                        outcome = "transient",
                        "tier configuration reload retry failed"
                    );
                }

                tokio::select! {
                    _ = cancel_token.cancelled() => {
                        sys.cancel_tier_config_reload_worker(&host);
                        return;
                    }
                    _ = sleep(delay) => {}
                }
            }
        }
    }
}

fn tier_config_reload_retry_delay(retry_attempt: u32) -> Duration {
    let multiplier = 1_u32 << retry_attempt.min(6);
    TIER_CONFIG_RELOAD_RETRY_BASE
        .checked_mul(multiplier)
        .unwrap_or(TIER_CONFIG_RELOAD_RETRY_CAP)
        .min(TIER_CONFIG_RELOAD_RETRY_CAP)
}

async fn scanner_activity_with_timeout<F>(timeout_duration: Duration, host: &str, activity: F) -> Result<ScannerPeerActivity>
where
    F: Future<Output = Result<ScannerPeerActivity>>,
{
    timeout(timeout_duration, activity)
        .await
        .map_err(|_| Error::other(format!("scanner activity peer {host} timed out after {timeout_duration:?}")))?
}

/// Classify transport-only activity failures without treating an answered
/// peer's application error as an outage.
pub fn scanner_peer_transport_error_message_is_retryable(error: &str) -> bool {
    crate::cluster::rpc::client::message_has_network_needle(error)
}

fn scanner_activity_should_retry(first_error: Option<&Error>, timed_out: bool) -> bool {
    timed_out || first_error.is_some_and(PeerRestClient::is_network_like_error)
}

/// Retry one activity probe after a bounded reconnect when the first attempt
/// failed at the transport boundary.  A peer that answered with an invalid or
/// incompatible activity response is not retried here: it must remain a hard
/// fail-closed result for the all-peer publication proof.
async fn scanner_activity_with_retry(client: &PeerRestClient, host: &str) -> Result<ScannerPeerActivity> {
    let first = timeout(SCANNER_ACTIVITY_PROBE_TIMEOUT, client.scanner_activity()).await;
    let should_retry = match &first {
        Ok(Ok(_)) => false,
        Ok(Err(err)) => scanner_activity_should_retry(Some(err), false),
        Err(_) => scanner_activity_should_retry(None, true),
    };

    match first {
        Ok(Ok(activity)) => return Ok(activity),
        Ok(Err(err)) if !should_retry => return Err(err),
        Ok(Err(err)) => {
            debug!(peer = host, error = %err, "scanner activity probe failed on first transport attempt; reconnecting");
            client.prepare_retry().await;
        }
        Err(_) => {
            debug!(peer = host, timeout = ?SCANNER_ACTIVITY_PROBE_TIMEOUT, "scanner activity probe timed out on first attempt; reconnecting");
            client.prepare_retry().await;
        }
    }

    match timeout(SCANNER_ACTIVITY_PROBE_TIMEOUT, client.scanner_activity()).await {
        Ok(result) => result,
        Err(_) => {
            client.evict_connection().await;
            Err(Error::Timeout)
        }
    }
}

async fn scanner_dirty_usage_snapshot_with_retry(client: &PeerRestClient, host: &str) -> Result<ScannerPeerDirtyUsageSnapshot> {
    let first = timeout(SCANNER_ACTIVITY_PROBE_TIMEOUT, client.scanner_dirty_usage_snapshot()).await;
    let should_retry = match &first {
        Ok(Ok(_)) => false,
        Ok(Err(err)) => scanner_activity_should_retry(Some(err), false),
        Err(_) => scanner_activity_should_retry(None, true),
    };

    match first {
        Ok(Ok(snapshot)) => return Ok(snapshot),
        Ok(Err(err)) if !should_retry => return Err(err),
        Ok(Err(err)) => {
            debug!(
                event = EVENT_NOTIFICATION_CAPABILITY_PROBE,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_NOTIFICATION,
                result = "retrying",
                capability = "scanner_dirty_usage_snapshot",
                peer = host,
                error = %err,
                "notification capability probe retrying"
            );
            client.prepare_retry().await;
        }
        Err(_) => {
            debug!(
                event = EVENT_NOTIFICATION_CAPABILITY_PROBE,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_NOTIFICATION,
                result = "retrying",
                capability = "scanner_dirty_usage_snapshot",
                peer = host,
                timeout = ?SCANNER_ACTIVITY_PROBE_TIMEOUT,
                "notification capability probe retrying"
            );
            client.prepare_retry().await;
        }
    }

    match timeout(SCANNER_ACTIVITY_PROBE_TIMEOUT, client.scanner_dirty_usage_snapshot()).await {
        Ok(result) => result,
        Err(_) => {
            client.evict_connection().await;
            Err(Error::Timeout)
        }
    }
}

#[allow(dead_code, reason = "asserted by this file's tests (backlog#1823)")]
async fn call_peer_with_timeout<F, Fut>(
    timeout_dur: Duration,
    host_label: &str,
    op: F,
    fallback: impl FnOnce() -> ServerProperties,
) -> ServerProperties
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = Result<ServerProperties>> + Send,
{
    match timeout(timeout_dur, op()).await {
        Ok(Ok(info)) => info,
        Ok(Err(err)) => {
            warn!("peer {host_label} server_info failed: {err}");
            fallback()
        }
        Err(_) => {
            warn!("peer {host_label} server_info timed out after {:?}", timeout_dur);
            fallback()
        }
    }
}

/// Handle a peer failure for storage_info: return cached data if available,
/// or mark offline only after consecutive failures exceed the threshold.
fn handle_peer_failure(
    cache: Option<&Mutex<PeerAdminCache>>,
    host: &str,
    endpoints: &EndpointServerPools,
) -> Option<StorageInfo> {
    let cache = cache?;

    let mut c = match cache.lock() {
        Ok(cache) => cache,
        Err(poisoned) => {
            warn!("peer {host} storage_info cache mutex poisoned");
            poisoned.into_inner()
        }
    };
    c.storage_failures += 1;

    if let Some(ref cached) = c.last_storage_info
        && c.storage_failures < CONSECUTIVE_FAILURE_THRESHOLD
    {
        debug!(
            event = "peer_probe_failure",
            peer = host,
            probe = "storage_info",
            consecutive_failures = c.storage_failures,
            threshold = CONSECUTIVE_FAILURE_THRESHOLD,
            "peer storage_info probe failed; returning cached state until the offline threshold is reached"
        );
        return Some(cached.clone());
    }

    if c.storage_failures >= CONSECUTIVE_FAILURE_THRESHOLD {
        if c.storage_failures == CONSECUTIVE_FAILURE_THRESHOLD {
            warn!(
                event = "peer_marked_offline",
                peer = host,
                probe = "storage_info",
                consecutive_failures = c.storage_failures,
                threshold = CONSECUTIVE_FAILURE_THRESHOLD,
                "reporting peer disks offline after consecutive storage_info failures"
            );
        }
        return Some(StorageInfo {
            disks: synthesized_disks(host, endpoints, ItemState::Offline),
            ..Default::default()
        });
    }

    None
}

fn normalize_and_cache_peer_storage_info(cache: Option<&Mutex<PeerAdminCache>>, host: &str, info: &mut StorageInfo) {
    // `Disk::local` is relative to this aggregator, not to the peer that
    // produced the response.
    for disk in &mut info.disks {
        disk.local = false;
    }

    let Some(cache) = cache else {
        return;
    };

    let mut c = match cache.lock() {
        Ok(cache) => cache,
        Err(poisoned) => {
            warn!("peer {host} storage_info cache mutex poisoned");
            poisoned.into_inner()
        }
    };
    if c.storage_failures >= CONSECUTIVE_FAILURE_THRESHOLD {
        info!(
            event = "peer_recovered_online",
            peer = host,
            probe = "storage_info",
            consecutive_failures = c.storage_failures,
            "peer storage_info probe succeeded again; peer disks reported online"
        );
    }
    c.last_storage_info = Some(info.clone());
    c.storage_failures = 0;
}

/// Independent liveness evidence for a peer, gathered from the local disk-health
/// heartbeat rather than the admin RPC path. `any_online` is true when at least
/// one of the peer's drives is still answering the ~15s health check; `disks`
/// carries a per-drive entry (state `"ok"` when online, `"offline"` when the
/// heartbeat marks it faulty) so a `degraded` member's drives are counted for
/// real. See rustfs/backlog#1049 (P0-B).
struct PeerDiskHealth {
    any_online: bool,
    disks: Vec<rustfs_madmin::Disk>,
}

struct PeerServerInfoProbe {
    host: String,
    result: std::result::Result<ServerProperties, PeerServerInfoProbeFailure>,
}

enum PeerServerInfoProbeFailure {
    Rpc { health: Option<PeerDiskHealth> },
    NoClient,
}

/// Consult the local disk-health state for `host` without issuing any RPC.
///
/// On the aggregating node a peer's drives are remote-disk handles whose
/// `is_online()` is a pure atomic read of the heartbeat tracker (independent of
/// the admin `server_info` RPC that just failed). Returns `None` when the store
/// is not initialized or the host owns no drives in the topology.
async fn peer_disk_health(host: &str) -> Option<PeerDiskHealth> {
    let store = runtime_sources::object_store_handle()?;

    let mut disks = Vec::new();
    let mut any_online = false;
    for sets in store.pools.iter() {
        for set in sets.disk_set.iter() {
            let guard = set.disks.read().await;
            for (idx, slot) in guard.iter().enumerate() {
                let Some(ep) = set.set_endpoints.get(idx) else {
                    continue;
                };
                if !endpoint_host_matches(host, &ep.host_port()) {
                    continue;
                }
                let online = match slot {
                    Some(disk) => disk.is_online().await,
                    None => false,
                };
                any_online |= online;
                // A live drive is counted online via the DriveState "ok" string;
                // a faulty one is counted offline. This keeps a degraded member's
                // drives in the real online/offline buckets.
                disks.push(rustfs_madmin::Disk {
                    endpoint: ep.to_string(),
                    state: if online {
                        rustfs_heal_contracts::heal_channel::DriveState::Ok.to_string()
                    } else {
                        ItemState::Offline.to_string().to_owned()
                    },
                    pool_index: ep.pool_idx,
                    set_index: ep.set_idx,
                    disk_index: ep.disk_idx,
                    ..Default::default()
                });
            }
        }
    }

    if disks.is_empty() {
        None
    } else {
        Some(PeerDiskHealth { any_online, disks })
    }
}

async fn peer_disk_health_with_deadline(host: &str, deadline: Instant) -> Option<PeerDiskHealth> {
    let remaining = remaining_admin_peer_probe_timeout(deadline)?;
    timeout(remaining, peer_disk_health(host)).await.ok().flatten()
}

/// Handle a peer failure for server_info: return cached data if available, or
/// classify the member as `unknown` / `degraded` / `offline` depending on how
/// many consecutive probes have failed and whether the peer's drives are still
/// answering the local disk-health heartbeat.
///
/// - Below the failure threshold with no cached snapshot: `unknown` (probe
///   missed this cycle but the member is not confirmed down).
/// - At/after the threshold with drives still online: `degraded` (the admin RPC
///   is stuck but the node is alive and serving data) — this is what stops a
///   healthy node from rotating through a false `offline` (rustfs/backlog#1049).
/// - At/after the threshold with drives also offline: `offline` (confirmed).
///
/// Synthesized entries always carry one drive per endpoint so the pool's drive
/// totals stay balanced.
fn handle_server_info_failure(
    cache: Option<&Mutex<PeerAdminCache>>,
    host: &str,
    endpoints: &EndpointServerPools,
    peer_health: Option<&PeerDiskHealth>,
) -> ServerProperties {
    let Some(cache) = cache else {
        return unknown_server_properties(host, endpoints);
    };

    let mut c = match cache.lock() {
        Ok(cache) => cache,
        Err(poisoned) => {
            warn!("peer {host} server_info cache mutex poisoned");
            poisoned.into_inner()
        }
    };
    c.server_failures += 1;

    if let Some(ref cached) = c.last_server_info
        && c.server_failures < CONSECUTIVE_FAILURE_THRESHOLD
    {
        if cached_snapshot_is_fresh(c.last_server_success) {
            debug!(
                event = "peer_probe_failure",
                peer = host,
                consecutive_failures = c.server_failures,
                threshold = CONSECUTIVE_FAILURE_THRESHOLD,
                "peer server_info probe failed; returning cached state until the offline threshold is reached"
            );
            return cached.clone();
        }
        // The cached snapshot is too old to keep reporting as `online`; fall
        // through to the live unknown/degraded/offline classification below
        // instead of masking a down peer with a stale success (P2).
        debug!(
            event = "peer_cache_stale",
            peer = host,
            max_age_secs = SERVER_INFO_CACHE_MAX_AGE.as_secs(),
            "cached server_info snapshot is stale; reclassifying from live signals instead of reporting stale online"
        );
    }

    if c.server_failures >= CONSECUTIVE_FAILURE_THRESHOLD {
        // Drives still answering the heartbeat: the node is alive, only its
        // admin surface is unreachable — report `degraded`, not `offline`, so a
        // stuck admin path does not read as an ejected node.
        if let Some(health) = peer_health.filter(|h| h.any_online) {
            if c.server_failures == CONSECUTIVE_FAILURE_THRESHOLD {
                warn!(
                    event = "peer_marked_degraded",
                    peer = host,
                    consecutive_failures = c.server_failures,
                    threshold = CONSECUTIVE_FAILURE_THRESHOLD,
                    "peer admin server_info keeps failing but its drives are online; reporting degraded (not offline)"
                );
            } else {
                debug!(
                    event = "peer_still_degraded",
                    peer = host,
                    consecutive_failures = c.server_failures,
                    "peer admin server_info still failing while its drives remain online"
                );
            }
            return degraded_server_properties(host, &health.disks);
        }

        // Log the transition exactly once (at the crossing) so the console's
        // "node offline" verdict has a matching WARN in the observer's logs
        // (rustfs/backlog#888: nodes were marked offline with no log naming
        // the transition). Later failures while already offline stay at DEBUG
        // to avoid repeating the warning every probe cycle.
        if c.server_failures == CONSECUTIVE_FAILURE_THRESHOLD {
            warn!(
                event = "peer_marked_offline",
                peer = host,
                consecutive_failures = c.server_failures,
                threshold = CONSECUTIVE_FAILURE_THRESHOLD,
                "marking peer offline for admin/console reporting after consecutive server_info failures; \
                 a background recovery probe will restore it automatically once reachable"
            );
        } else {
            debug!(
                event = "peer_still_offline",
                peer = host,
                consecutive_failures = c.server_failures,
                "peer server_info probe failed while peer is already reported offline"
            );
        }
        return offline_server_properties(host, endpoints);
    }

    unknown_server_properties(host, endpoints)
}

fn publish_server_info_probe_round(
    caches: &[Mutex<PeerAdminCache>],
    endpoints: &EndpointServerPools,
    probes: Vec<PeerServerInfoProbe>,
) -> Vec<ServerProperties> {
    probes
        .into_iter()
        .enumerate()
        .map(|(idx, probe)| {
            let cache = caches.get(idx);
            match probe.result {
                Ok(info) => {
                    update_server_info_cache(cache, &probe.host, &info);
                    info
                }
                Err(PeerServerInfoProbeFailure::Rpc { health }) => {
                    handle_server_info_failure(cache, &probe.host, endpoints, health.as_ref())
                }
                Err(PeerServerInfoProbeFailure::NoClient) => unknown_server_properties(&probe.host, endpoints),
            }
        })
        .collect()
}

fn update_server_info_cache(cache: Option<&Mutex<PeerAdminCache>>, host: &str, info: &ServerProperties) {
    let Some(cache) = cache else {
        return;
    };

    let mut c = match cache.lock() {
        Ok(cache) => cache,
        Err(poisoned) => {
            warn!("peer {host} server_info cache mutex poisoned");
            poisoned.into_inner()
        }
    };
    if c.server_failures >= CONSECUTIVE_FAILURE_THRESHOLD {
        info!(
            event = "peer_recovered_online",
            peer = host,
            consecutive_failures = c.server_failures,
            "peer server_info probe succeeded again; peer is back online for admin/console reporting"
        );
    }
    c.last_server_info = Some(info.clone());
    c.last_server_success = Some(SystemTime::now());
    c.server_failures = 0;
}

/// Whether a cached server_info snapshot is recent enough to still report as
/// `online` on a probe failure. A missing timestamp means no age information is
/// available (e.g. a snapshot set without going through the success path in a
/// test); such a snapshot is treated as fresh to preserve the prior behavior,
/// while a clock that went backwards is treated as stale. See P2 in
/// rustfs/backlog#1049.
fn cached_snapshot_is_fresh(last_success: Option<SystemTime>) -> bool {
    match last_success {
        Some(at) => at.elapsed().map(|age| age < SERVER_INFO_CACHE_MAX_AGE).unwrap_or(false),
        None => true,
    }
}

/// A member that could not be probed this cycle and is not confirmed down.
/// Carries the endpoint's drives (marked `unknown`) so the pool's drive totals
/// stay balanced instead of the member's drives vanishing from the summary.
fn unknown_server_properties(host: &str, endpoints: &EndpointServerPools) -> ServerProperties {
    ServerProperties {
        endpoint: host.to_string(),
        state: ItemState::Unknown.to_string().to_owned(),
        disks: synthesized_disks(host, endpoints, ItemState::Unknown),
        ..Default::default()
    }
}

fn offline_server_properties(host: &str, endpoints: &EndpointServerPools) -> ServerProperties {
    ServerProperties {
        uptime: runtime_sources::boot_uptime_secs(),
        version: get_commit_id(),
        endpoint: host.to_string(),
        state: ItemState::Offline.to_string().to_owned(),
        disks: synthesized_disks(host, endpoints, ItemState::Offline),
        ..Default::default()
    }
}

/// A member whose admin RPC is unreachable but whose drives are still online.
/// Carries the per-drive health observed from the local heartbeat so the drives
/// land in the real online/offline buckets while the member reads as degraded.
fn degraded_server_properties(host: &str, disks: &[rustfs_madmin::Disk]) -> ServerProperties {
    ServerProperties {
        uptime: runtime_sources::boot_uptime_secs(),
        version: get_commit_id(),
        endpoint: host.to_string(),
        state: ItemState::Degraded.to_string().to_owned(),
        disks: disks.to_vec(),
        ..Default::default()
    }
}

/// Enumerate the drives a host owns from the pool topology, tagged with the
/// given member state. Used to synthesize drive entries for a member whose
/// properties RPC could not be answered, so summary counters stay complete.
fn synthesized_disks(host: &str, endpoints: &EndpointServerPools, state: ItemState) -> Vec<rustfs_madmin::Disk> {
    let mut disks = Vec::new();

    for pool in endpoints.as_ref() {
        for ep in pool.endpoints.as_ref() {
            if (host.is_empty() && ep.is_local) || endpoint_host_matches(host, &ep.host_port()) {
                disks.push(rustfs_madmin::Disk {
                    endpoint: ep.to_string(),
                    state: state.to_string().to_owned(),
                    pool_index: ep.pool_idx,
                    set_index: ep.set_idx,
                    disk_index: ep.disk_idx,
                    ..Default::default()
                });
            }
        }
    }

    disks
}

/// Whether `peer_host` refers to the same node as an endpoint whose
/// `host_port()` is `ep_host_port`.
///
/// Current topology clients preserve the endpoint `hostname:port`, so the
/// direct comparison is the normal path. The resolution fallback keeps
/// compatibility with older or manually constructed clients whose `XHost`
/// contains a resolved `IP:port` (rustfs/rustfs#4607 follow-up).
fn endpoint_host_matches(peer_host: &str, ep_host_port: &str) -> bool {
    if peer_host == ep_host_port {
        return true;
    }
    XHost::try_from(ep_host_port.to_string())
        .map(|resolved| resolved.to_string() == peer_host)
        .unwrap_or(false)
}

fn aggregate_notification_failures(operation: &str, failures: Vec<String>) -> Result<()> {
    if failures.is_empty() {
        return Ok(());
    }

    Err(Error::other(format!(
        "{operation} encountered {} failure(s): {}",
        failures.len(),
        failures.join(" | ")
    )))
}

fn aggregate_scanner_dirty_usage_acknowledgement_results(
    results: Vec<(String, Result<ScannerPeerActivity>)>,
    mut failures: Vec<String>,
) -> Result<bool> {
    let mut dirty_usage_pending = false;
    for (host, result) in results {
        match result {
            Ok(activity) => {
                dirty_usage_pending |= activity.dirty_usage_pending != Some(false);
            }
            Err(err) => failures.push(format!("peer {host} scanner dirty usage acknowledgement failed: {err}")),
        }
    }
    aggregate_notification_failures("acknowledge_scanner_dirty_usage", failures)?;
    Ok(dirty_usage_pending)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bucket::lifecycle::tier_last_day_stats::LastDayTierStats;
    use rustfs_data_usage::TierStats;

    fn ring(total_size: u64) -> LastDayTierStats {
        let mut stats = LastDayTierStats::default();
        stats.add_stats(TierStats {
            total_size,
            num_versions: 1,
            num_objects: 1,
        });
        stats
    }

    #[test]
    fn merging_peer_rings_sums_each_tier_without_double_counting() {
        let mut cluster = DailyAllTierStats::from([("WARM".to_string(), ring(10))]);

        merge_tier_daily_stats(
            &mut cluster,
            DailyAllTierStats::from([("WARM".to_string(), ring(20)), ("COLD".to_string(), ring(5))]),
        );

        assert_eq!(
            cluster.get("WARM").expect("the shared tier must survive the merge").total(),
            TierStats {
                total_size: 30,
                num_versions: 2,
                num_objects: 2,
            },
            "both nodes' completions belong in the cluster total"
        );
        assert_eq!(
            cluster.get("COLD").expect("a tier only one node saw must be kept").total(),
            TierStats {
                total_size: 5,
                num_versions: 1,
                num_objects: 1,
            }
        );
    }

    #[test]
    fn a_single_member_result_is_complete_and_a_missing_peer_is_not() {
        let complete = ClusterTierDailyStats {
            stats: DailyAllTierStats::new(),
            nodes_reporting: 1,
            nodes_expected: 1,
            unavailable_nodes: Vec::new(),
        };
        assert!(complete.is_complete(), "a single-member deployment reports its whole cluster");

        let partial = ClusterTierDailyStats {
            stats: DailyAllTierStats::new(),
            nodes_reporting: 1,
            nodes_expected: 2,
            unavailable_nodes: vec!["node-b:9000".to_string()],
        };
        assert!(!partial.is_complete(), "a silent member must make the sum partial");
    }

    #[test]
    fn cross_pool_policy_versions_authorize_only_their_supported_protocols() {
        let peers = BTreeMap::from([("node-b:9000".to_string(), Uuid::new_v4())]);
        let (generic_v2, journal_v2, decommission_v2, reconcile_v2) = cross_pool_fence_policy_results(peers.clone(), 2);
        assert!(generic_v2.is_ok(), "v2 remains valid for existing cross-pool fencing");
        assert!(journal_v2.is_err(), "a mixed v2/v3 fleet must fail closed for journal-v6 deletion");
        assert!(decommission_v2.is_err(), "v2 cannot authorize the sticky per-target decommission fence");
        assert!(reconcile_v2.is_err(), "v2 cannot authorize legacy transition-state reconciliation");

        let (generic_v3, journal_v3, decommission_v3, reconcile_v3) = cross_pool_fence_policy_results(peers.clone(), 3);
        assert!(generic_v3.is_ok());
        assert!(journal_v3.is_ok(), "an all-v3 fleet may authorize journal-v6 deletion");
        assert!(decommission_v3.is_err(), "v3 members do not understand the per-target decommission fence");
        assert!(reconcile_v3.is_err());

        let (generic_v4, journal_v4, decommission_v4, reconcile_v4) =
            cross_pool_fence_policy_results(peers.clone(), LOCAL_CROSS_POOL_FENCE_POLICY_SUPPORTED_VERSION);
        assert!(generic_v4.is_ok());
        assert!(journal_v4.is_ok());
        assert!(decommission_v4.is_ok(), "an all-v4 fleet may create sticky per-target reservations");
        assert!(
            reconcile_v4.is_err(),
            "the current local policy lacks the conditional xl.meta writer required by reconcile"
        );

        let (generic_v5, journal_v5, decommission_v5, reconcile_v5) = cross_pool_fence_policy_results(peers, 5);
        assert!(generic_v5.is_ok());
        assert!(journal_v5.is_ok());
        assert!(decommission_v5.is_ok());
        assert!(
            reconcile_v5.is_ok(),
            "only an all-v5 fleet preserves destination identity and conditional reconcile writes"
        );
    }

    #[test]
    fn remote_version_state_fleet_proof_rejects_stale_or_mismatched_membership() {
        let now = Instant::now();
        let mut peer_epochs = BTreeMap::new();
        peer_epochs.insert("peer-a".to_string(), Uuid::new_v4());
        let proof = FleetCapabilityProof::new("topology-a".to_string(), Arc::new(peer_epochs), now + Duration::from_secs(1));

        assert!(fleet_capability_proof_valid_at(Some(&proof), "topology-a", now));
        assert!(!fleet_capability_proof_valid_at(Some(&proof), "topology-b", now));
        assert!(!fleet_capability_proof_valid_at(Some(&proof), "topology-a", proof.expires_at));
        assert!(!fleet_capability_proof_valid_at(None, "topology-a", now));
    }

    #[test]
    fn remote_version_state_fleet_proof_rejects_nil_process_epoch() {
        let mut peer_epochs = BTreeMap::new();

        assert!(insert_remote_version_state_peer(&mut peer_epochs, "peer-a".to_string(), Uuid::nil()).is_err());
        assert!(peer_epochs.is_empty());
    }

    #[test]
    fn remote_version_state_fleet_proof_accepts_single_node_membership() {
        let now = Instant::now();
        let proof = FleetCapabilityProof::new("topology-a".to_string(), Arc::new(BTreeMap::new()), now + Duration::from_secs(1));

        assert!(fleet_capability_proof_valid_at(Some(&proof), "topology-a", now));
    }

    #[test]
    fn remote_version_state_fleet_proof_token_changes_with_process_epoch() {
        let now = Instant::now();
        let proof = FleetCapabilityProof::new(
            "topology-a".to_string(),
            Arc::new(BTreeMap::from([("peer-a".to_string(), Uuid::new_v4())])),
            now + Duration::from_secs(1),
        );
        let captured = proof.token();
        let restarted = FleetCapabilityProof::new(
            proof.topology_fingerprint.clone(),
            Arc::new(BTreeMap::from([("peer-a".to_string(), Uuid::new_v4())])),
            proof.expires_at,
        );

        assert!(captured != restarted.token());
    }

    #[test]
    fn ilm_recovery_export_member_digest_is_order_independent_and_epoch_bound() {
        let now = Instant::now();
        let local_epoch = ilm_recovery_export_local_process_epoch();
        assert!(!local_epoch.is_nil());
        assert_eq!(local_epoch, ilm_recovery_export_local_process_epoch());
        let remote_epoch = Uuid::new_v4();
        let slot = std::sync::RwLock::new(FleetCapabilityProofState::default());
        let peers = BTreeMap::from([("node-b".to_string(), remote_epoch), ("node-a".to_string(), local_epoch)]);
        assert!(publish_fleet_capability_probe_result(&slot, "topology-a", Ok(peers), now).is_none());
        let proof = {
            let state = slot.read().expect("export proof slot should not poison");
            acquire_ilm_recovery_export_fleet_proof_from(&state, "topology-a", now).expect("complete fleet should admit export")
        };
        let digest = ilm_recovery_export_member_epochs_sha256(&proof);

        let changed_slot = std::sync::RwLock::new(FleetCapabilityProofState::default());
        let changed = BTreeMap::from([("node-a".to_string(), local_epoch), ("node-b".to_string(), Uuid::new_v4())]);
        assert!(publish_fleet_capability_probe_result(&changed_slot, "topology-a", Ok(changed), now).is_none());
        let changed_proof = {
            let state = changed_slot.read().expect("export proof slot should not poison");
            acquire_ilm_recovery_export_fleet_proof_from(&state, "topology-a", now).expect("complete fleet should admit export")
        };
        assert_ne!(digest, ilm_recovery_export_member_epochs_sha256(&changed_proof));
    }

    #[test]
    fn ilm_recovery_export_members_must_match_the_exact_topology() {
        let expected_remote = vec!["node-b".to_string()];
        let local = "node-a";
        let complete = BTreeMap::from([
            (local.to_string(), Uuid::new_v4()),
            (expected_remote[0].clone(), Uuid::new_v4()),
        ]);
        assert!(validate_ilm_recovery_export_members(&expected_remote, local, &complete).is_ok());

        let unexpected = BTreeMap::from([(local.to_string(), Uuid::new_v4()), ("node-c".to_string(), Uuid::new_v4())]);
        assert!(validate_ilm_recovery_export_members(&expected_remote, local, &unexpected).is_err());
        assert!(
            validate_ilm_recovery_export_members(&[local.to_string()], local, &complete).is_err(),
            "the configured remote set cannot repeat the local member"
        );
    }

    #[test]
    fn ilm_recovery_export_restart_revokes_authority_until_permit_drains() {
        let slot = std::sync::RwLock::new(FleetCapabilityProofState::default());
        let now = Instant::now();
        let original = BTreeMap::from([("node-a".to_string(), Uuid::new_v4())]);
        assert!(publish_fleet_capability_probe_result(&slot, "topology-a", Ok(original), now).is_none());
        let admitted = {
            let state = slot.read().expect("export proof slot should not poison");
            acquire_ilm_recovery_export_fleet_proof_from(&state, "topology-a", now).expect("fresh fleet should admit export")
        };

        let restarted = BTreeMap::from([("node-a".to_string(), Uuid::new_v4())]);
        let draining = publish_fleet_capability_probe_result(&slot, "topology-a", Ok(restarted.clone()), now)
            .expect("restart must wait for the admitted export effect window");
        assert!(draining.to_string().contains("previous generation to drain"));
        {
            let state = slot.read().expect("export proof slot should not poison");
            assert!(!ilm_recovery_export_fleet_proof_matches_at(&state, &admitted, "topology-a", now));
            assert!(
                acquire_ilm_recovery_export_fleet_proof_from(&state, "topology-a", now).is_none(),
                "successor authority must wait for the old effect window to drain"
            );
        }
        drop(admitted);
        assert!(
            publish_fleet_capability_probe_result(&slot, "topology-a", Ok(restarted), now + Duration::from_millis(1)).is_none()
        );
        let state = slot.read().expect("export proof slot should not poison");
        assert!(acquire_ilm_recovery_export_fleet_proof_from(&state, "topology-a", now).is_some());
    }

    #[test]
    fn tier_delete_journal_generation_is_stable_across_members_and_process_restarts() {
        let topology = "topology-a";
        let now = Instant::now();
        let node_a_view = FleetCapabilityProof::new(
            topology.to_string(),
            Arc::new(BTreeMap::from([("node-b".to_string(), Uuid::new_v4())])),
            now + Duration::from_secs(1),
        );
        let node_b_view = FleetCapabilityProof::new(
            topology.to_string(),
            Arc::new(BTreeMap::from([("node-a".to_string(), Uuid::new_v4())])),
            now + Duration::from_secs(1),
        );
        let restarted_node_a_view = FleetCapabilityProof::new(
            topology.to_string(),
            Arc::new(BTreeMap::from([("node-b".to_string(), Uuid::new_v4())])),
            now + Duration::from_secs(1),
        );

        let generations = [&node_a_view, &node_b_view, &restarted_node_a_view]
            .map(|proof| stable_tier_delete_journal_topology_generation(&proof.token().topology_fingerprint));
        assert_eq!(generations[0], generations[1]);
        assert_eq!(generations[0], generations[2]);
        assert_ne!(
            generations[0],
            stable_tier_delete_journal_topology_generation("topology-b"),
            "a real topology change must produce a different durable generation"
        );
    }

    #[test]
    fn tier_delete_journal_restart_revokes_old_token_but_fresh_token_recovers_same_generation() {
        let slot = std::sync::RwLock::new(FleetCapabilityProofState::default());
        let now = Instant::now();
        let original_peers = BTreeMap::from([("node-b".to_string(), Uuid::new_v4())]);
        assert!(publish_fleet_capability_probe_result(&slot, "topology-a", Ok(original_peers), now).is_none());
        let original = slot
            .read()
            .expect("proof slot should not poison")
            .proof
            .as_ref()
            .expect("successful probe should publish proof")
            .token();
        let original_generation = stable_tier_delete_journal_topology_generation(&original.topology_fingerprint);

        let restarted_peers = BTreeMap::from([("node-b".to_string(), Uuid::new_v4())]);
        assert!(
            publish_fleet_capability_probe_result(&slot, "topology-a", Ok(restarted_peers), now + Duration::from_millis(1))
                .is_none()
        );
        let state = slot.read().expect("proof slot should not poison");
        let fresh = state
            .proof
            .as_ref()
            .expect("restart probe should publish a fresh proof")
            .token();

        assert!(!fleet_capability_proof_matches_at(
            &state,
            &original,
            "topology-a",
            now + Duration::from_millis(2)
        ));
        assert!(fleet_capability_proof_matches_at(
            &state,
            &fresh,
            "topology-a",
            now + Duration::from_millis(2)
        ));
        assert_eq!(
            original_generation,
            stable_tier_delete_journal_topology_generation(&fresh.topology_fingerprint)
        );
    }

    #[test]
    fn remote_version_state_fleet_proof_renewal_preserves_only_same_epoch_token() {
        let slot = std::sync::RwLock::new(FleetCapabilityProofState::default());
        let now = Instant::now();
        let epoch = Uuid::new_v4();
        let peers = BTreeMap::from([("peer-a".to_string(), epoch)]);
        assert!(publish_fleet_capability_probe_result(&slot, "topology-a", Ok(peers.clone()), now).is_none());
        let original = slot
            .read()
            .expect("proof slot should not poison")
            .proof
            .as_ref()
            .expect("successful probe should publish proof")
            .token();

        assert!(publish_fleet_capability_probe_result(&slot, "topology-a", Ok(peers), now + Duration::from_millis(1)).is_none());
        let renewed = slot
            .read()
            .expect("proof slot should not poison")
            .proof
            .as_ref()
            .expect("renewal should retain proof")
            .token();
        assert!(Arc::ptr_eq(&original.peer_epochs, &renewed.peer_epochs));

        let restarted = BTreeMap::from([("peer-a".to_string(), Uuid::new_v4())]);
        assert!(
            publish_fleet_capability_probe_result(&slot, "topology-a", Ok(restarted), now + Duration::from_millis(2)).is_none()
        );
        let replaced = slot
            .read()
            .expect("proof slot should not poison")
            .proof
            .as_ref()
            .expect("restarted peer should publish a new proof")
            .token();
        assert!(!Arc::ptr_eq(&original.peer_epochs, &replaced.peer_epochs));
    }

    #[test]
    fn tier_delete_journal_successor_waits_for_inflight_generation_to_drain() {
        let slot = std::sync::RwLock::new(FleetCapabilityProofState::default());
        let now = Instant::now();
        let original_peers = BTreeMap::from([("peer-a".to_string(), Uuid::new_v4())]);
        assert!(publish_fleet_capability_probe_result(&slot, "topology-a", Ok(original_peers), now).is_none());

        let admitted = {
            let state = slot.read().expect("proof slot should not poison");
            acquire_tier_delete_journal_fleet_proof_from(&state, "topology-a", now)
                .expect("a fresh proof should admit one journal operation")
        };
        {
            let state = slot.read().expect("proof slot should not poison");
            assert!(
                tier_delete_journal_fleet_proof_matches_at(&state, &admitted, "topology-a", now),
                "a freshly admitted journal proof must remain current"
            );
            assert!(
                acquire_tier_delete_journal_fleet_proof_from(&state, "topology-a", now + REMOTE_VERSION_STATE_PROOF_TTL,)
                    .is_none(),
                "TTL expiry must stop new admission"
            );
            assert!(
                !tier_delete_journal_fleet_proof_matches_at(
                    &state,
                    &admitted,
                    "topology-a",
                    now + REMOTE_VERSION_STATE_PROOF_TTL,
                ),
                "TTL expiry must also stop an admitted proof at its next durable fence"
            );
            assert!(!admitted._permit.generation.is_drained());
        }

        let restarted_peers = BTreeMap::from([("peer-a".to_string(), Uuid::new_v4())]);
        let blocked = publish_fleet_capability_probe_result(
            &slot,
            "topology-a",
            Ok(restarted_peers.clone()),
            now + Duration::from_millis(1),
        )
        .expect("a successor proof must wait for the admitted generation");
        assert!(blocked.to_string().contains("previous generation to drain"));
        {
            let state = slot.read().expect("proof slot should not poison");
            assert!(state.proof.is_none(), "new operations must remain closed while the predecessor drains");
            assert!(state.draining_generation.is_some());
            assert!(
                !tier_delete_journal_fleet_proof_matches_at(&state, &admitted, "topology-a", now + Duration::from_millis(1),),
                "a restarted peer must revoke an admitted proof before its next durable fence"
            );
        }

        drop(admitted);
        assert!(
            publish_fleet_capability_probe_result(&slot, "topology-a", Ok(restarted_peers), now + Duration::from_millis(2),)
                .is_none(),
            "the successor may publish after the in-flight operation releases its permit"
        );
        let state = slot.read().expect("proof slot should not poison");
        assert!(state.proof.is_some());
        assert!(state.draining_generation.is_none());
    }

    #[test]
    fn tier_delete_journal_topology_conflict_revokes_admitted_generation() {
        let slot = std::sync::RwLock::new(FleetCapabilityProofState::default());
        let now = Instant::now();
        let peers = BTreeMap::from([("peer-a".to_string(), Uuid::new_v4())]);
        assert!(publish_fleet_capability_probe_result(&slot, "topology-a", Ok(peers), now).is_none());
        let admitted = {
            let state = slot.read().expect("proof slot should not poison");
            acquire_tier_delete_journal_fleet_proof_from(&state, "topology-a", now)
                .expect("a fresh proof should admit one journal operation")
        };

        mark_fleet_capability_topology_conflict(&slot);

        let state = slot.read().expect("proof slot should not poison");
        assert!(state.topology_conflict);
        assert!(state.proof.is_none());
        assert!(state.draining_generation.is_some());
        assert!(!admitted._permit.generation.is_accepting());
        assert!(
            !tier_delete_journal_fleet_proof_matches_at(&state, &admitted, "topology-a", now),
            "topology conflict must revoke an already admitted journal proof"
        );
    }

    #[test]
    fn legacy_transition_state_reconcile_admits_only_compatible_single_and_multi_node_fleets() {
        let now = Instant::now();
        for peers in [
            BTreeMap::new(),
            BTreeMap::from([("peer-a".to_string(), Uuid::new_v4()), ("peer-b".to_string(), Uuid::new_v4())]),
        ] {
            let slot = std::sync::RwLock::new(FleetCapabilityProofState::default());
            let (_, _, _, result) =
                cross_pool_fence_policy_results(peers, LEGACY_TRANSITION_STATE_RECONCILE_POLICY_SUPPORTED_VERSION);
            assert!(publish_fleet_capability_probe_result(&slot, "topology-a", result, now).is_none());

            let admitted = {
                let state = slot.read().expect("reconcile proof slot should not poison");
                acquire_legacy_transition_state_reconcile_fleet_proof_from(&state, "topology-a", now)
                    .expect("an all-compatible fleet should admit reconciliation")
            };
            let state = slot.read().expect("reconcile proof slot should not poison");
            assert!(legacy_transition_state_reconcile_fleet_proof_matches_at(
                &state,
                &admitted,
                "topology-a",
                now,
            ));
        }
    }

    #[test]
    fn legacy_transition_state_reconcile_restart_drains_concurrent_effect_windows() {
        let slot = std::sync::RwLock::new(FleetCapabilityProofState::default());
        let now = Instant::now();
        let original_peers = BTreeMap::from([("peer-a".to_string(), Uuid::new_v4())]);
        let (_, _, _, original_result) =
            cross_pool_fence_policy_results(original_peers, LEGACY_TRANSITION_STATE_RECONCILE_POLICY_SUPPORTED_VERSION);
        assert!(publish_fleet_capability_probe_result(&slot, "topology-a", original_result, now).is_none());

        let (first, second) = {
            let state = slot.read().expect("reconcile proof slot should not poison");
            (
                acquire_legacy_transition_state_reconcile_fleet_proof_from(&state, "topology-a", now)
                    .expect("the first reconcile writer should be admitted"),
                acquire_legacy_transition_state_reconcile_fleet_proof_from(&state, "topology-a", now)
                    .expect("the second reconcile writer should be admitted"),
            )
        };

        let restarted_peers = BTreeMap::from([("peer-a".to_string(), Uuid::new_v4())]);
        let (_, _, _, restarted_result) =
            cross_pool_fence_policy_results(restarted_peers.clone(), LEGACY_TRANSITION_STATE_RECONCILE_POLICY_SUPPORTED_VERSION);
        let blocked =
            publish_fleet_capability_probe_result(&slot, "topology-a", restarted_result, now + Duration::from_millis(1))
                .expect("a restarted member must revoke the old generation and wait for both writers");
        assert!(blocked.to_string().contains("previous generation to drain"));
        {
            let state = slot.read().expect("reconcile proof slot should not poison");
            assert!(state.proof.is_none());
            assert!(state.draining_generation.is_some());
            assert!(!legacy_transition_state_reconcile_fleet_proof_matches_at(
                &state,
                &first,
                "topology-a",
                now + Duration::from_millis(1),
            ));
            assert!(!legacy_transition_state_reconcile_fleet_proof_matches_at(
                &state,
                &second,
                "topology-a",
                now + Duration::from_millis(1),
            ));
        }

        drop(first);
        let (_, _, _, still_blocked_result) =
            cross_pool_fence_policy_results(restarted_peers.clone(), LEGACY_TRANSITION_STATE_RECONCILE_POLICY_SUPPORTED_VERSION);
        assert!(
            publish_fleet_capability_probe_result(&slot, "topology-a", still_blocked_result, now + Duration::from_millis(2),)
                .is_some(),
            "one remaining writer must keep the successor generation closed"
        );

        drop(second);
        let (_, _, _, admitted_result) =
            cross_pool_fence_policy_results(restarted_peers, LEGACY_TRANSITION_STATE_RECONCILE_POLICY_SUPPORTED_VERSION);
        assert!(
            publish_fleet_capability_probe_result(&slot, "topology-a", admitted_result, now + Duration::from_millis(3),)
                .is_none(),
            "the restarted generation may publish only after every old writer drains"
        );
    }

    #[test]
    fn legacy_transition_state_reconcile_fresh_observation_closes_the_polling_window() {
        let slot = std::sync::RwLock::new(FleetCapabilityProofState::default());
        let now = Instant::now();
        let original_peers = BTreeMap::from([("peer-a".to_string(), Uuid::new_v4())]);
        let (_, _, _, original_result) =
            cross_pool_fence_policy_results(original_peers.clone(), LEGACY_TRANSITION_STATE_RECONCILE_POLICY_SUPPORTED_VERSION);
        assert!(publish_fleet_capability_probe_result(&slot, "topology-a", original_result, now).is_none());
        let admitted = {
            let state = slot.read().expect("reconcile proof slot should not poison");
            acquire_legacy_transition_state_reconcile_fleet_proof_from(&state, "topology-a", now)
                .expect("the original fleet should admit reconciliation")
        };

        let restarted_peers = BTreeMap::from([("peer-a".to_string(), Uuid::new_v4())]);
        let state = slot.read().expect("reconcile proof slot should not poison");
        assert!(
            legacy_transition_state_reconcile_fleet_proof_matches_at(&state, &admitted, "topology-a", now),
            "the periodic cache has not observed the restart yet"
        );
        assert!(!legacy_transition_state_reconcile_fleet_proof_matches_observation_at(
            &state,
            &admitted,
            "topology-a",
            &restarted_peers,
            now,
        ));

        let (_, _, _, downgraded) = cross_pool_fence_policy_results(original_peers, 4);
        assert!(
            downgraded.is_err(),
            "a synchronous observation of a downgraded peer must fail before any cached proof can authorize a write"
        );
    }

    #[tokio::test]
    async fn legacy_transition_state_reconcile_invalid_token_skips_fleet_observation() {
        let slot = std::sync::RwLock::new(FleetCapabilityProofState::default());
        let now = Instant::now();
        let peers = BTreeMap::from([("peer-a".to_string(), Uuid::new_v4())]);
        assert!(publish_fleet_capability_probe_result(&slot, "topology-a", Ok(peers), now).is_none());
        let admitted = {
            let state = slot.read().expect("reconcile proof slot should not poison");
            acquire_legacy_transition_state_reconcile_fleet_proof_from(&state, "topology-a", now)
                .expect("the original fleet should admit reconciliation")
        };
        revoke_fleet_capability_proof(&slot);

        assert!(
            !legacy_transition_state_reconcile_fleet_proof_matches_with_observer(&slot, &admitted, "topology-a", || async {
                panic!("an invalid local generation must not trigger a fleet observation");
            },)
            .await
        );
    }

    #[test]
    fn legacy_transition_state_reconcile_membership_and_topology_changes_revoke_authority() {
        let now = Instant::now();
        for replacement in [
            BTreeMap::from([("peer-a".to_string(), Uuid::new_v4()), ("peer-b".to_string(), Uuid::new_v4())]),
            BTreeMap::from([("peer-a".to_string(), Uuid::new_v4())]),
        ] {
            let slot = std::sync::RwLock::new(FleetCapabilityProofState::default());
            let original = BTreeMap::from([("peer-a".to_string(), Uuid::new_v4())]);
            assert!(publish_fleet_capability_probe_result(&slot, "topology-a", Ok(original), now).is_none());
            let admitted = {
                let state = slot.read().expect("reconcile proof slot should not poison");
                acquire_legacy_transition_state_reconcile_fleet_proof_from(&state, "topology-a", now)
                    .expect("the original fleet should admit reconciliation")
            };

            assert!(
                publish_fleet_capability_probe_result(&slot, "topology-a", Ok(replacement), now + Duration::from_millis(1),)
                    .is_some(),
                "membership or process-epoch replacement must wait for the admitted writer"
            );
            let state = slot.read().expect("reconcile proof slot should not poison");
            assert!(!legacy_transition_state_reconcile_fleet_proof_matches_at(
                &state,
                &admitted,
                "topology-a",
                now + Duration::from_millis(1),
            ));
        }

        let slot = std::sync::RwLock::new(FleetCapabilityProofState::default());
        assert!(publish_fleet_capability_probe_result(&slot, "topology-a", Ok(BTreeMap::new()), now).is_none());
        let admitted = {
            let state = slot.read().expect("reconcile proof slot should not poison");
            acquire_legacy_transition_state_reconcile_fleet_proof_from(&state, "topology-a", now)
                .expect("the original topology should admit reconciliation")
        };
        mark_fleet_capability_topology_conflict(&slot);
        let state = slot.read().expect("reconcile proof slot should not poison");
        assert!(state.topology_conflict);
        assert!(!legacy_transition_state_reconcile_fleet_proof_matches_at(
            &state,
            &admitted,
            "topology-a",
            now,
        ));
    }

    #[test]
    fn legacy_transition_state_reconcile_capability_downgrade_fails_closed() {
        let slot = std::sync::RwLock::new(FleetCapabilityProofState::default());
        let now = Instant::now();
        let peers = BTreeMap::from([("peer-a".to_string(), Uuid::new_v4())]);
        let (_, _, _, compatible_result) =
            cross_pool_fence_policy_results(peers.clone(), LEGACY_TRANSITION_STATE_RECONCILE_POLICY_SUPPORTED_VERSION);
        assert!(publish_fleet_capability_probe_result(&slot, "topology-a", compatible_result, now).is_none());
        let admitted = {
            let state = slot.read().expect("reconcile proof slot should not poison");
            acquire_legacy_transition_state_reconcile_fleet_proof_from(&state, "topology-a", now)
                .expect("v5 should admit reconciliation")
        };

        let (_, _, _, downgraded_result) =
            cross_pool_fence_policy_results(peers, LEGACY_TRANSITION_STATE_RECONCILE_POLICY_SUPPORTED_VERSION - 1);
        let err = publish_fleet_capability_probe_result(&slot, "topology-a", downgraded_result, now + Duration::from_millis(1))
            .expect("a v4 member must revoke reconcile authority");
        assert!(err.to_string().contains("reconcile policy capability version is unsupported"));
        let state = slot.read().expect("reconcile proof slot should not poison");
        assert!(state.proof.is_none());
        assert!(!legacy_transition_state_reconcile_fleet_proof_matches_at(
            &state,
            &admitted,
            "topology-a",
            now + Duration::from_millis(1),
        ));
        assert!(
            acquire_legacy_transition_state_reconcile_fleet_proof_from(&state, "topology-a", now + Duration::from_millis(1),)
                .is_none(),
            "a downgraded fleet must remain inspect-only"
        );
    }

    #[test]
    fn remote_version_state_fleet_proof_conflict_revokes_atomic_snapshot() {
        let now = Instant::now();
        let mut state = FleetCapabilityProofState {
            proof: Some(FleetCapabilityProof::new(
                "topology-a".to_string(),
                Arc::new(BTreeMap::new()),
                now + Duration::from_secs(1),
            )),
            draining_generation: None,
            topology_conflict: false,
        };
        assert!(acquire_fleet_capability_proof_from(&state, "topology-a", now).is_some());

        state.topology_conflict = true;
        assert!(acquire_fleet_capability_proof_from(&state, "topology-a", now).is_none());
    }

    #[test]
    fn remote_version_state_fleet_probe_rejects_duplicate_member_or_process_epoch() {
        let epoch = Uuid::new_v4();
        let mut peer_epochs = BTreeMap::new();
        insert_remote_version_state_peer(&mut peer_epochs, "node-a:9000".to_string(), epoch)
            .expect("first member should be admitted");
        assert!(insert_remote_version_state_peer(&mut peer_epochs, "node-b:9000".to_string(), epoch).is_err());
        assert!(insert_remote_version_state_peer(&mut peer_epochs, "node-a:9000".to_string(), Uuid::new_v4()).is_err());
        assert!(insert_remote_version_state_peer(&mut peer_epochs, "node-c:9000".to_string(), Uuid::nil()).is_err());
    }

    #[test]
    fn remote_version_state_fleet_probe_failure_revokes_previous_proof() {
        let slot = std::sync::RwLock::new(FleetCapabilityProofState::default());
        let now = Instant::now();
        let peer_epochs = BTreeMap::from([("node-a:9000".to_string(), Uuid::new_v4())]);
        assert!(publish_fleet_capability_probe_result(&slot, "topology-a", Ok(peer_epochs), now).is_none());
        assert!(slot.read().expect("proof slot should not poison").proof.is_some());

        assert!(
            publish_fleet_capability_probe_result(&slot, "topology-a", Err(Error::other("peer unavailable")), now,).is_some()
        );
        assert!(slot.read().expect("proof slot should not poison").proof.is_none());

        let peer_epochs = BTreeMap::from([("node-a:9000".to_string(), Uuid::new_v4())]);
        assert!(publish_fleet_capability_probe_result(&slot, "topology-a", Ok(peer_epochs), now).is_none());
        assert!(slot.read().expect("proof slot should not poison").proof.is_some());
    }

    #[tokio::test]
    async fn remote_version_state_fleet_probe_rejects_unreachable_member() {
        let notification_sys = NotificationSys {
            peer_clients: vec![None],
            all_peer_clients: vec![None, None],
            peer_topology_hosts: vec!["peer-a".to_string()],
            peer_admin_caches: vec![Mutex::new(PeerAdminCache::new())],
            tier_config_reload_workers: Default::default(),
        };

        let err = notification_sys
            .probe_remote_version_state_fleet("topology-a")
            .await
            .expect_err("an unreachable configured member must fail the fleet proof");
        assert!(err.to_string().contains("unreachable"));
    }

    #[tokio::test]
    async fn remote_version_state_fleet_probe_rejects_missing_member_slot() {
        let notification_sys = NotificationSys {
            peer_clients: Vec::new(),
            all_peer_clients: vec![None],
            peer_topology_hosts: vec!["peer-a".to_string()],
            peer_admin_caches: Vec::new(),
            tier_config_reload_workers: Default::default(),
        };

        let err = notification_sys
            .probe_remote_version_state_fleet("topology-a")
            .await
            .expect_err("a missing configured member slot must fail the fleet proof");
        assert!(err.to_string().contains("incomplete"));
    }

    #[tokio::test]
    async fn legacy_transition_state_reconcile_probe_rejects_missing_or_unreachable_members() {
        let missing = NotificationSys {
            peer_clients: Vec::new(),
            all_peer_clients: vec![None],
            peer_topology_hosts: vec!["peer-a".to_string()],
            peer_admin_caches: Vec::new(),
            tier_config_reload_workers: Default::default(),
        };
        let missing_err = missing
            .probe_cross_pool_fence_fleet("topology-a")
            .await
            .expect_err("a missing member slot must prevent reconcile capability proof");
        assert!(missing_err.to_string().contains("incomplete"));

        let unreachable = NotificationSys {
            peer_clients: vec![None],
            all_peer_clients: vec![None, None],
            peer_topology_hosts: vec!["peer-a".to_string()],
            peer_admin_caches: vec![Mutex::new(PeerAdminCache::new())],
            tier_config_reload_workers: Default::default(),
        };
        let unreachable_err = unreachable
            .probe_cross_pool_fence_fleet("topology-a")
            .await
            .expect_err("an unreachable member must prevent reconcile capability proof");
        assert!(unreachable_err.to_string().contains("unreachable"));
    }

    #[tokio::test]
    async fn legacy_transition_state_reconcile_single_node_stays_closed_before_local_cas_support() {
        let notification_sys = NotificationSys {
            peer_clients: Vec::new(),
            all_peer_clients: vec![None],
            peer_topology_hosts: Vec::new(),
            peer_admin_caches: Vec::new(),
            tier_config_reload_workers: Default::default(),
        };
        let (peers, minimum_version) = notification_sys
            .probe_cross_pool_fence_fleet("topology-a")
            .await
            .expect("a single-node capability probe should complete");
        assert!(peers.is_empty());
        assert_eq!(minimum_version, LOCAL_CROSS_POOL_FENCE_POLICY_SUPPORTED_VERSION);
        let (_, _, _, reconcile_result) = cross_pool_fence_policy_results(peers, minimum_version);
        assert!(
            reconcile_result.is_err(),
            "the current node must not self-authorize reconcile before the conditional writer lands"
        );
    }

    fn build_props(endpoint: &str) -> ServerProperties {
        ServerProperties {
            endpoint: endpoint.to_string(),
            ..Default::default()
        }
    }

    #[test]
    fn admin_peer_probe_timeout_rejects_zero_and_caps_large_values() {
        assert_eq!(
            resolve_admin_peer_probe_timeout_secs(None),
            rustfs_config::DEFAULT_ADMIN_PEER_PROBE_TIMEOUT_SECS
        );
        assert_eq!(
            resolve_admin_peer_probe_timeout_secs(Some(0)),
            rustfs_config::DEFAULT_ADMIN_PEER_PROBE_TIMEOUT_SECS
        );
        assert_eq!(
            resolve_admin_peer_probe_timeout_secs(Some(rustfs_config::MAX_ADMIN_PEER_PROBE_TIMEOUT_SECS + 1)),
            rustfs_config::MAX_ADMIN_PEER_PROBE_TIMEOUT_SECS
        );
        assert_eq!(resolve_admin_peer_probe_timeout_secs(Some(7)), 7);
    }

    #[tokio::test]
    async fn admin_peer_probe_health_fallback_respects_expired_deadline() {
        let deadline = Instant::now();
        assert!(peer_disk_health_with_deadline("peer-1", deadline).await.is_none());
    }

    #[test]
    fn admin_peer_probe_deadline_is_shared_across_attempts() {
        let start = Instant::now();
        let deadline = start + Duration::from_secs(10);
        assert!(remaining_admin_peer_probe_timeout_at(deadline, start + Duration::from_secs(6)).is_some());
        assert!(remaining_admin_peer_probe_timeout_at(deadline, start + Duration::from_secs(10)).is_none());
    }

    #[tokio::test]
    async fn call_peer_with_timeout_returns_value_when_fast() {
        let result = call_peer_with_timeout(
            Duration::from_millis(50),
            "peer-1",
            || async { Ok::<_, Error>(build_props("fast")) },
            || build_props("fallback"),
        )
        .await;

        assert_eq!(result.endpoint, "fast");
    }

    #[tokio::test]
    async fn call_peer_with_timeout_uses_fallback_on_error() {
        let result = call_peer_with_timeout(
            Duration::from_millis(50),
            "peer-2",
            || async { Err::<ServerProperties, _>(Error::other("boom")) },
            || build_props("fallback"),
        )
        .await;

        assert_eq!(result.endpoint, "fallback");
    }

    #[tokio::test]
    async fn call_peer_with_timeout_uses_fallback_on_timeout() {
        let result = call_peer_with_timeout(
            Duration::from_millis(5),
            "peer-3",
            std::future::pending::<Result<ServerProperties>>,
            || build_props("fallback"),
        )
        .await;

        assert_eq!(result.endpoint, "fallback");
    }

    #[test]
    fn aggregate_notification_failures_returns_ok_when_empty() {
        assert!(aggregate_notification_failures("stop_rebalance", Vec::new()).is_ok());
    }

    #[test]
    fn aggregate_notification_failures_returns_joined_error_when_non_empty() {
        let err = aggregate_notification_failures(
            "load_rebalance_meta",
            vec!["peer-1 failed".to_string(), "local save failed".to_string()],
        )
        .expect_err("non-empty failures should return error");

        let msg = err.to_string();
        assert!(msg.contains("load_rebalance_meta"));
        assert!(msg.contains("2 failure(s)"));
        assert!(msg.contains("peer-1 failed"));
        assert!(msg.contains("local save failed"));
    }

    #[test]
    fn peer_client_for_grid_host_matches_exact_grid_host() {
        let sys = NotificationSys {
            peer_clients: Vec::new(),
            all_peer_clients: vec![Some(PeerRestClient::new(
                "127.0.0.1:9000".to_string().try_into().expect("peer host should parse"),
                "http://127.0.0.1:9000".to_string(),
            ))],
            peer_topology_hosts: Vec::new(),
            peer_admin_caches: Vec::new(),
            tier_config_reload_workers: Default::default(),
        };

        let client = sys
            .peer_client_for_grid_host("http://127.0.0.1:9000")
            .expect("matching grid host should return peer client");
        assert_eq!(client.grid_host, "http://127.0.0.1:9000");
        assert!(sys.peer_client_for_grid_host("http://node-b:9000").is_none());
    }

    #[test]
    fn load_rebalance_meta_aggregate_failures_return_error() {
        let err = aggregate_notification_failures(
            "load_rebalance_meta(start=true)",
            vec!["peer[0] load_rebalance_meta failed: peer is not reachable".to_string()],
        )
        .expect_err("load_rebalance_meta peer failures must be returned");

        let msg = err.to_string();
        assert!(msg.contains("load_rebalance_meta(start=true)"));
        assert!(msg.contains("1 failure(s)"));
        assert!(msg.contains("peer[0]"));
    }

    #[test]
    fn stop_rebalance_aggregate_failures_return_error() {
        let err = aggregate_notification_failures(
            "stop_rebalance",
            vec!["peer[0] stop_rebalance failed: peer is not reachable".to_string()],
        )
        .expect_err("stop_rebalance peer failures must be returned");

        let msg = err.to_string();
        assert!(msg.contains("stop_rebalance"));
        assert!(msg.contains("1 failure(s)"));
        assert!(msg.contains("peer[0]"));
    }

    #[tokio::test]
    async fn reload_pool_meta_reports_unreachable_peers() {
        let sys = NotificationSys {
            peer_clients: vec![None],
            all_peer_clients: Vec::new(),
            peer_topology_hosts: vec!["node-a:9000".to_string()],
            peer_admin_caches: vec![Mutex::new(PeerAdminCache::new())],
            tier_config_reload_workers: Default::default(),
        };

        let err = sys
            .reload_pool_meta()
            .await
            .expect_err("unreachable peers should fail pool metadata reload");

        let msg = err.to_string();
        assert!(msg.contains("reload_pool_meta"));
        assert!(msg.contains("1 failure(s)"));
        assert!(msg.contains("peer[0]"));
    }

    #[tokio::test]
    async fn scanner_activity_probe_reports_unreachable_peers() {
        let sys = NotificationSys {
            peer_clients: vec![None],
            all_peer_clients: vec![None, None],
            peer_topology_hosts: vec!["node-a:9000".to_string()],
            peer_admin_caches: vec![Mutex::new(PeerAdminCache::new())],
            tier_config_reload_workers: Default::default(),
        };

        let err = sys
            .scanner_activity_snapshots()
            .await
            .expect_err("unreachable peers must disable scanner idle backoff");

        assert!(err.to_string().contains("scanner activity peer[0] is unreachable"));
    }

    #[tokio::test]
    async fn scanner_activity_probe_rejects_an_empty_peer_set() {
        let sys = NotificationSys {
            peer_clients: Vec::new(),
            all_peer_clients: Vec::new(),
            peer_topology_hosts: Vec::new(),
            peer_admin_caches: Vec::new(),
            tier_config_reload_workers: Default::default(),
        };

        let err = sys
            .scanner_activity_snapshots()
            .await
            .expect_err("a missing peer set must disable scanner idle backoff");

        assert!(err.to_string().contains("no remote peers"));
    }

    #[tokio::test]
    async fn scanner_publication_lease_release_reports_all_unavailable_peers() {
        let sys = NotificationSys {
            peer_clients: Vec::new(),
            all_peer_clients: Vec::new(),
            peer_topology_hosts: Vec::new(),
            peer_admin_caches: Vec::new(),
            tier_config_reload_workers: Default::default(),
        };
        let grants = ["peer-a", "peer-b"]
            .into_iter()
            .map(|host| ScannerPublicationLeaseGrant {
                host: host.to_string(),
                lease: ScannerPublicationLease {
                    token: Uuid::new_v4(),
                    movement_generation: 3,
                    owner_id: Uuid::new_v4().to_string(),
                    session_id: "session-a".to_string(),
                    expires_at: Instant::now() + Duration::from_secs(30),
                },
            })
            .collect();

        let error = sys
            .release_scanner_publication_leases(grants)
            .await
            .expect_err("an unavailable peer must not silently release a remote lease");
        let message = error.to_string();
        assert!(message.contains("peer-a"));
        assert!(message.contains("peer-b"));
    }

    #[tokio::test]
    async fn scanner_activity_probe_rejects_an_incomplete_peer_topology() {
        let client = PeerRestClient::new(
            "127.0.0.1:9000".to_string().try_into().expect("peer host should parse"),
            "http://127.0.0.1:9000".to_string(),
        );
        let sys = NotificationSys {
            peer_clients: vec![Some(client)],
            all_peer_clients: vec![None],
            peer_topology_hosts: vec!["127.0.0.1:9000".to_string()],
            peer_admin_caches: vec![Mutex::new(PeerAdminCache::new())],
            tier_config_reload_workers: Default::default(),
        };

        let err = sys
            .scanner_activity_snapshots()
            .await
            .expect_err("an incomplete peer topology must disable scanner idle backoff");

        assert!(err.to_string().contains("peer topology is incomplete"));
    }

    #[tokio::test]
    async fn scanner_dirty_usage_snapshot_probe_rejects_unusable_peer_topologies() {
        let unreachable = NotificationSys {
            peer_clients: vec![None],
            all_peer_clients: vec![None, None],
            peer_topology_hosts: vec!["node-a:9000".to_string()],
            peer_admin_caches: vec![Mutex::new(PeerAdminCache::new())],
            tier_config_reload_workers: Default::default(),
        };
        let err = unreachable
            .scanner_dirty_usage_snapshots()
            .await
            .expect_err("an unreachable peer must invalidate the distributed dirty usage snapshot");
        assert!(err.to_string().contains("peer is unreachable"));

        let empty = NotificationSys {
            peer_clients: Vec::new(),
            all_peer_clients: Vec::new(),
            peer_topology_hosts: Vec::new(),
            peer_admin_caches: Vec::new(),
            tier_config_reload_workers: Default::default(),
        };
        let err = empty
            .scanner_dirty_usage_snapshots()
            .await
            .expect_err("an empty peer set must not produce a distributed dirty usage snapshot");
        assert!(err.to_string().contains("no remote peers"));

        let client = PeerRestClient::new(
            "127.0.0.1:9000".to_string().try_into().expect("peer host should parse"),
            "http://127.0.0.1:9000".to_string(),
        );
        let incomplete = NotificationSys {
            peer_clients: vec![Some(client)],
            all_peer_clients: vec![None],
            peer_topology_hosts: vec!["127.0.0.1:9000".to_string()],
            peer_admin_caches: vec![Mutex::new(PeerAdminCache::new())],
            tier_config_reload_workers: Default::default(),
        };
        let err = incomplete
            .scanner_dirty_usage_snapshots()
            .await
            .expect_err("an incomplete topology must not produce a distributed dirty usage snapshot");
        assert!(err.to_string().contains("peer topology is incomplete"));
    }

    #[tokio::test]
    async fn server_info_no_client_slot_uses_topology_host_without_counting_rpc_failure() {
        let sys = NotificationSys {
            peer_clients: vec![None],
            all_peer_clients: vec![None, None],
            peer_topology_hosts: vec!["node-a:9000".to_string()],
            peer_admin_caches: vec![Mutex::new(PeerAdminCache::new())],
            tier_config_reload_workers: Default::default(),
        };

        let servers = sys.server_info().await;

        assert_eq!(servers.len(), 1);
        assert_eq!(servers[0].endpoint, "node-a:9000");
        assert_eq!(servers[0].state, ItemState::Unknown.to_string());
        let cache = sys.peer_admin_caches[0].lock().expect("cache mutex should not be poisoned");
        assert_eq!(cache.server_failures, 0, "construction-only missing slots are not failed RPC attempts");
        assert!(cache.last_server_info.is_none());
    }

    #[test]
    fn server_info_failure_cache_stays_aligned_with_topology_slot() {
        let cache_a = Mutex::new(PeerAdminCache {
            last_server_info: Some(build_props("cached-a")),
            last_server_success: Some(SystemTime::now()),
            server_failures: 1,
            storage_failures: 0,
            last_storage_info: None,
        });
        let cache_b = Mutex::new(PeerAdminCache {
            last_server_info: Some(build_props("cached-b")),
            last_server_success: Some(SystemTime::now()),
            server_failures: 1,
            storage_failures: 0,
            last_storage_info: None,
        });
        let caches = [cache_a, cache_b];
        let endpoints = EndpointServerPools::from(Vec::new());

        let rendered = handle_server_info_failure(Some(&caches[1]), "node-b:9000", &endpoints, None);

        assert_eq!(rendered.endpoint, "cached-b");
        assert_eq!(caches[0].lock().expect("cache mutex should not be poisoned").server_failures, 1);
        assert_eq!(caches[1].lock().expect("cache mutex should not be poisoned").server_failures, 2);
    }

    #[tokio::test]
    async fn scanner_activity_probe_times_out() {
        let err = scanner_activity_with_timeout(
            Duration::from_millis(5),
            "peer-1",
            std::future::pending::<Result<ScannerPeerActivity>>(),
        )
        .await
        .expect_err("a stalled peer must not block scanner scheduling");

        assert!(err.to_string().contains("timed out"));
        assert!(err.to_string().contains("peer-1"));
    }

    #[test]
    fn scanner_activity_retry_only_reconnects_transport_failures() {
        assert!(scanner_activity_should_retry(None, true));
        assert!(scanner_activity_should_retry(Some(&Error::other("connection refused")), false));
        assert!(!scanner_activity_should_retry(
            Some(&Error::other("peer returned an invalid scanner activity response proof")),
            false
        ));
        assert!(!scanner_activity_should_retry(
            Some(&Error::from(tonic::Status::internal("peer rejected activity"))),
            false
        ));
    }

    #[tokio::test]
    async fn scanner_dirty_usage_acknowledgement_rejects_missing_and_duplicate_targets() {
        let sys = NotificationSys {
            peer_clients: Vec::new(),
            all_peer_clients: Vec::new(),
            peer_admin_caches: Vec::new(),
            tier_config_reload_workers: Default::default(),
            peer_topology_hosts: Vec::new(),
        };
        let missing = sys
            .acknowledge_scanner_dirty_usage(vec![ScannerDirtyUsageAcknowledgement::Generation {
                host: "peer-1".to_string(),
                instance_id: "0123456789abcdef0123456789abcdef".to_string(),
                generation: 7,
            }])
            .await
            .expect_err("a missing acknowledgement target must remain pending");
        assert!(missing.to_string().contains("peer is not reachable"));

        let duplicate = sys
            .acknowledge_scanner_dirty_usage(vec![
                ScannerDirtyUsageAcknowledgement::Generation {
                    host: "peer-1".to_string(),
                    instance_id: "0123456789abcdef0123456789abcdef".to_string(),
                    generation: 7,
                },
                ScannerDirtyUsageAcknowledgement::Scoped {
                    host: "peer-1".to_string(),
                    owner_id: "11111111-1111-1111-1111-111111111111".to_string(),
                    instance_id: "0123456789abcdef0123456789abcdef".to_string(),
                    entries: Vec::new(),
                },
            ])
            .await
            .expect_err("duplicate acknowledgement targets must be rejected");
        assert!(
            duplicate
                .to_string()
                .contains("duplicate scanner dirty usage acknowledgement target")
        );
    }

    #[test]
    fn scanner_dirty_usage_acknowledgement_preserves_newer_pending_work() {
        let activity = |dirty_usage_pending| ScannerPeerActivity {
            instance_id: "0123456789abcdef0123456789abcdef".to_string(),
            namespace_generation: 1,
            maintenance_generation: 1,
            protocol_version: crate::storage_api_contracts::internode::SCANNER_ACTIVITY_PROTOCOL_VERSION,
            topology_digest: Some([0; 32]),
            data_movement_active: Some(false),
            dirty_usage_generation: Some(2),
            dirty_usage_pending,
            movement_generation: Some(1),
            publication_blocked: Some(false),
        };

        let pending = aggregate_scanner_dirty_usage_acknowledgement_results(
            vec![
                ("peer-1".to_string(), Ok(activity(Some(false)))),
                ("peer-2".to_string(), Ok(activity(Some(true)))),
            ],
            Vec::new(),
        )
        .expect("successful acknowledgements should return their pending state");
        assert!(pending, "new dirty usage reported by an acknowledged peer must remain pending");

        let cleared = aggregate_scanner_dirty_usage_acknowledgement_results(
            vec![("peer-1".to_string(), Ok(activity(Some(false))))],
            Vec::new(),
        )
        .expect("a cleared acknowledgement should succeed");
        assert!(!cleared, "an explicitly cleared peer must not remain pending");

        let unknown =
            aggregate_scanner_dirty_usage_acknowledgement_results(vec![("peer-1".to_string(), Ok(activity(None)))], Vec::new())
                .expect("an acknowledgement without a pending field should remain retryable");
        assert!(unknown, "a peer that cannot prove its dirty state is clear must remain pending");

        let err = aggregate_scanner_dirty_usage_acknowledgement_results(
            vec![("peer-1".to_string(), Err(Error::other("injected acknowledgement failure")))],
            Vec::new(),
        )
        .expect_err("a reachable peer acknowledgement failure must be reported");
        assert!(err.to_string().contains("peer-1"));
        assert!(err.to_string().contains("injected acknowledgement failure"));
    }

    #[tokio::test]
    async fn load_bucket_metadata_reports_unreachable_peers() {
        let sys = NotificationSys {
            peer_clients: vec![None],
            all_peer_clients: Vec::new(),
            peer_topology_hosts: vec!["node-a:9000".to_string()],
            peer_admin_caches: vec![Mutex::new(PeerAdminCache::new())],
            tier_config_reload_workers: Default::default(),
        };

        let err = sys
            .load_bucket_metadata("bucket-a")
            .await
            .expect_err("unreachable peers should fail bucket metadata reload");

        let msg = err.to_string();
        assert!(msg.contains("load_bucket_metadata(bucket-a)"));
        assert!(msg.contains("1 failure(s)"));
        assert!(msg.contains("peer[0]"));
    }

    #[test]
    fn tier_config_reload_retry_delay_is_exponentially_capped() {
        assert_eq!(tier_config_reload_retry_delay(0), Duration::from_millis(100));
        assert_eq!(tier_config_reload_retry_delay(1), Duration::from_millis(200));
        assert_eq!(tier_config_reload_retry_delay(5), Duration::from_millis(3200));
        assert_eq!(tier_config_reload_retry_delay(6), TIER_CONFIG_RELOAD_RETRY_CAP);
        assert_eq!(tier_config_reload_retry_delay(u32::MAX), TIER_CONFIG_RELOAD_RETRY_CAP);
    }

    #[tokio::test]
    async fn tier_config_reload_worker_retries_only_network_failures() {
        let sys = Arc::new(NotificationSys {
            peer_clients: Vec::new(),
            all_peer_clients: Vec::new(),
            peer_topology_hosts: Vec::new(),
            peer_admin_caches: Vec::new(),
            tier_config_reload_workers: Default::default(),
        });
        assert!(sys.reserve_tier_config_reload_worker("node-a:9000"));
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let calls_for_reload = Arc::clone(&calls);

        run_tier_config_reload_worker(Arc::clone(&sys), "node-a:9000".to_string(), CancellationToken::new(), move || {
            let attempt = calls_for_reload.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            async move {
                if attempt == 0 {
                    TierConfigReloadOutcome::TransientReconnect(Error::other("connection refused"))
                } else {
                    TierConfigReloadOutcome::Success
                }
            }
        })
        .await;

        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 2);
        assert!(!sys.tier_config_reload_worker_active("node-a:9000"));
    }

    #[tokio::test]
    async fn tier_config_reload_worker_converges_after_readiness_unknown() {
        let sys = Arc::new(NotificationSys {
            peer_clients: Vec::new(),
            all_peer_clients: Vec::new(),
            peer_topology_hosts: Vec::new(),
            peer_admin_caches: Vec::new(),
            tier_config_reload_workers: Default::default(),
        });
        assert!(sys.reserve_tier_config_reload_worker("node-a:9000"));
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let calls_for_reload = Arc::clone(&calls);

        run_tier_config_reload_worker(Arc::clone(&sys), "node-a:9000".to_string(), CancellationToken::new(), move || {
            let attempt = calls_for_reload.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            async move {
                if attempt == 0 {
                    TierConfigReloadOutcome::TransientRetrySameChannel(Error::other("Service was not ready: test client"))
                } else {
                    TierConfigReloadOutcome::Success
                }
            }
        })
        .await;

        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 2);
        assert!(!sys.tier_config_reload_worker_active("node-a:9000"));
    }

    #[tokio::test]
    async fn tier_config_reload_worker_stops_on_terminal_failure() {
        let sys = Arc::new(NotificationSys {
            peer_clients: Vec::new(),
            all_peer_clients: Vec::new(),
            peer_topology_hosts: Vec::new(),
            peer_admin_caches: Vec::new(),
            tier_config_reload_workers: Default::default(),
        });
        assert!(sys.reserve_tier_config_reload_worker("node-a:9000"));
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let calls_for_reload = Arc::clone(&calls);

        run_tier_config_reload_worker(Arc::clone(&sys), "node-a:9000".to_string(), CancellationToken::new(), move || {
            calls_for_reload.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            async { TierConfigReloadOutcome::Terminal(Error::NotImplemented) }
        })
        .await;

        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert!(!sys.tier_config_reload_worker_active("node-a:9000"));
    }

    #[tokio::test]
    async fn tier_config_reload_worker_reloads_once_after_success_with_pending_mutation() {
        let sys = Arc::new(NotificationSys {
            peer_clients: Vec::new(),
            all_peer_clients: Vec::new(),
            peer_topology_hosts: Vec::new(),
            peer_admin_caches: Vec::new(),
            tier_config_reload_workers: Default::default(),
        });
        assert!(sys.reserve_tier_config_reload_worker("node-a:9000"));
        let sys_for_reload = Arc::clone(&sys);
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let calls_for_reload = Arc::clone(&calls);

        run_tier_config_reload_worker(Arc::clone(&sys), "node-a:9000".to_string(), CancellationToken::new(), move || {
            let attempt = calls_for_reload.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let sys = Arc::clone(&sys_for_reload);
            async move {
                if attempt == 0 {
                    assert!(!sys.reserve_tier_config_reload_worker("node-a:9000"));
                    TierConfigReloadOutcome::Success
                } else {
                    TierConfigReloadOutcome::Success
                }
            }
        })
        .await;

        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 2);
        assert!(!sys.tier_config_reload_worker_active("node-a:9000"));
    }

    #[test]
    fn tier_config_reload_none_peer_does_not_start_a_worker() {
        let sys = Arc::new(NotificationSys {
            peer_clients: vec![None],
            all_peer_clients: Vec::new(),
            peer_topology_hosts: vec!["node-a:9000".to_string()],
            peer_admin_caches: vec![Mutex::new(PeerAdminCache::new())],
            tier_config_reload_workers: Default::default(),
        });

        sys.spawn_transition_tier_config_reload_workers_with_cancel_token(Some(CancellationToken::new()));

        assert!(
            sys.tier_config_reload_workers
                .lock()
                .expect("tier config reload worker state must not be poisoned")
                .peers
                .is_empty()
        );
    }

    #[test]
    fn tier_config_reload_without_background_token_does_not_reserve_a_worker() {
        let client = PeerRestClient::new(
            "127.0.0.1:9000".to_string().try_into().expect("peer host should parse"),
            "http://127.0.0.1:9000".to_string(),
        );
        let sys = Arc::new(NotificationSys {
            peer_clients: vec![Some(client)],
            all_peer_clients: Vec::new(),
            peer_topology_hosts: vec!["127.0.0.1:9000".to_string()],
            peer_admin_caches: vec![Mutex::new(PeerAdminCache::new())],
            tier_config_reload_workers: Default::default(),
        });

        sys.spawn_transition_tier_config_reload_workers_with_cancel_token(None);

        assert!(
            sys.tier_config_reload_workers
                .lock()
                .expect("tier config reload worker state must not be poisoned")
                .peers
                .is_empty()
        );
    }

    #[tokio::test]
    async fn tier_config_reload_cancellation_during_transient_backoff_releases_state() {
        let sys = Arc::new(NotificationSys {
            peer_clients: Vec::new(),
            all_peer_clients: Vec::new(),
            peer_topology_hosts: Vec::new(),
            peer_admin_caches: Vec::new(),
            tier_config_reload_workers: Default::default(),
        });
        assert!(sys.reserve_tier_config_reload_worker("node-a:9000"));
        let cancel_token = CancellationToken::new();
        let cancel_for_reload = cancel_token.clone();
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let calls_for_reload = Arc::clone(&calls);

        run_tier_config_reload_worker(Arc::clone(&sys), "node-a:9000".to_string(), cancel_token, move || {
            let attempt = calls_for_reload.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let cancel_token = cancel_for_reload.clone();
            async move {
                if attempt == 0 {
                    cancel_token.cancel();
                }
                TierConfigReloadOutcome::TransientReconnect(Error::other("connection refused"))
            }
        })
        .await;

        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert!(!sys.tier_config_reload_worker_active("node-a:9000"));
    }

    #[tokio::test]
    async fn load_transition_tier_config_reports_unreachable_peers() {
        let sys = NotificationSys {
            peer_clients: vec![None],
            all_peer_clients: Vec::new(),
            peer_topology_hosts: vec!["node-a:9000".to_string()],
            peer_admin_caches: vec![Mutex::new(PeerAdminCache::new())],
            tier_config_reload_workers: Default::default(),
        };

        let results = sys.load_transition_tier_config().await;
        assert_eq!(results.len(), 1);
        assert!(results[0].host.is_empty());
        assert!(results[0].err.is_some());
        assert!(results[0].err.as_ref().unwrap().to_string().contains("peer is not reachable"));
    }

    #[tokio::test]
    async fn tier_mutation_fanout_reports_unreachable_peers_fail_closed() {
        let sys = NotificationSys {
            peer_clients: vec![None],
            all_peer_clients: Vec::new(),
            peer_topology_hosts: vec!["node-a:9000".to_string()],
            peer_admin_caches: vec![Mutex::new(PeerAdminCache::new())],
            tier_config_reload_workers: Default::default(),
        };
        let mutation_id = Uuid::from_u128(1);

        let prepare = sys.prepare_tier_mutation(mutation_id, Bytes::from_static(b"prepare")).await;
        assert_eq!(prepare.len(), 1);
        assert!(prepare[0].host.is_empty());
        assert!(
            prepare[0]
                .err
                .as_ref()
                .expect("unreachable prepare peer should carry an error")
                .to_string()
                .contains("peer is not reachable")
        );

        let commit = sys.commit_tier_mutation(mutation_id, Bytes::from_static(b"commit")).await;
        assert_eq!(commit.len(), 1);
        assert!(commit[0].err.is_some());

        let abort = sys.abort_tier_mutation(mutation_id, Bytes::from_static(b"prepare")).await;
        assert_eq!(abort.len(), 1);
        assert!(abort[0].err.is_some());
    }

    // --- Tests for handle_peer_failure / handle_server_info_failure caching ---

    #[test]
    fn handle_peer_failure_first_failure_returns_none_when_no_cache() {
        let cache = Mutex::new(PeerAdminCache::new());
        let endpoints = EndpointServerPools::default();

        let result = handle_peer_failure(Some(&cache), "peer-1", &endpoints);
        assert!(result.is_none());
        assert_eq!(cache.lock().unwrap().storage_failures, 1);
    }

    #[test]
    fn handle_peer_failure_returns_cached_data_on_single_failure() {
        let cached_info = StorageInfo {
            disks: vec![rustfs_madmin::Disk {
                endpoint: "disk-0".to_string(),
                state: "ok".to_string(),
                ..Default::default()
            }],
            ..Default::default()
        };

        let cache = Mutex::new(PeerAdminCache {
            last_storage_info: Some(cached_info),
            last_server_info: None,
            storage_failures: 0,
            server_failures: 0,
            last_server_success: None,
        });
        let endpoints = EndpointServerPools::default();

        // First failure: should return cached data
        let result = handle_peer_failure(Some(&cache), "peer-1", &endpoints);
        let info = result.unwrap();
        assert_eq!(info.disks.len(), 1);
        assert_eq!(info.disks[0].state, "ok");
        assert_eq!(cache.lock().unwrap().storage_failures, 1);
    }

    #[test]
    fn normalize_and_cache_peer_storage_info_marks_disks_remote() {
        let cache = Mutex::new(PeerAdminCache::new());
        let mut info = StorageInfo {
            disks: vec![
                rustfs_madmin::Disk {
                    endpoint: "http://node2:9000/media/rustfs-01".to_string(),
                    drive_path: "/media/rustfs-01".to_string(),
                    local: true,
                    ..Default::default()
                },
                rustfs_madmin::Disk {
                    endpoint: "http://node3:9000/media/rustfs-01".to_string(),
                    drive_path: "/media/rustfs-01".to_string(),
                    local: true,
                    ..Default::default()
                },
                rustfs_madmin::Disk {
                    endpoint: "http://node4:9000/media/rustfs-01".to_string(),
                    drive_path: "/media/rustfs-01".to_string(),
                    local: true,
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        normalize_and_cache_peer_storage_info(Some(&cache), "peer-1", &mut info);

        assert!(info.disks.iter().all(|disk| !disk.local));
        let cached = cache.lock().expect("peer cache must remain available");
        assert!(
            cached
                .last_storage_info
                .as_ref()
                .expect("successful peer response must be cached")
                .disks
                .iter()
                .all(|disk| !disk.local)
        );
        drop(cached);

        let degraded = handle_peer_failure(Some(&cache), "peer-1", &EndpointServerPools::default())
            .expect("first peer failure must return the cached snapshot");
        assert!(degraded.disks.iter().all(|disk| !disk.local));
    }

    #[test]
    fn handle_peer_failure_returns_offline_after_threshold_exceeded() {
        let cached_info = StorageInfo {
            disks: vec![rustfs_madmin::Disk {
                endpoint: "disk-0".to_string(),
                state: "ok".to_string(),
                ..Default::default()
            }],
            ..Default::default()
        };

        let cache = Mutex::new(PeerAdminCache {
            last_storage_info: Some(cached_info),
            last_server_info: None,
            storage_failures: CONSECUTIVE_FAILURE_THRESHOLD - 1,
            server_failures: 0,
            last_server_success: None,
        });
        let endpoints = EndpointServerPools::default();

        // This failure pushes us to the threshold => offline
        let result = handle_peer_failure(Some(&cache), "peer-1", &endpoints);
        assert!(result.is_some());
        assert_eq!(cache.lock().unwrap().storage_failures, CONSECUTIVE_FAILURE_THRESHOLD);
    }

    #[test]
    fn handle_server_info_failure_returns_cached_on_single_failure() {
        let cached_props = ServerProperties {
            endpoint: "peer-1".to_string(),
            state: "online".to_string(),
            ..Default::default()
        };

        let cache = Mutex::new(PeerAdminCache {
            last_storage_info: None,
            last_server_info: Some(cached_props),
            storage_failures: 0,
            server_failures: 0,
            last_server_success: None,
        });
        let endpoints = EndpointServerPools::default();

        let result = handle_server_info_failure(Some(&cache), "peer-1", &endpoints, None);
        assert_eq!(result.endpoint, "peer-1");
        assert_eq!(result.state, "online");
        assert_eq!(cache.lock().unwrap().server_failures, 1);
    }

    #[test]
    fn handle_server_info_failure_does_not_serve_stale_cached_online() {
        // A single failure with a cached snapshot would normally return the
        // cached `online`, but if that snapshot is older than the max age we
        // must not keep reporting online — fall through to `unknown` instead of
        // masking a possibly-down peer (rustfs/backlog#1049 P2).
        let cached_props = ServerProperties {
            endpoint: "peer-1".to_string(),
            state: "online".to_string(),
            ..Default::default()
        };
        let stale_at = SystemTime::now()
            .checked_sub(SERVER_INFO_CACHE_MAX_AGE + Duration::from_secs(1))
            .expect("test clock underflow");

        let cache = Mutex::new(PeerAdminCache {
            last_storage_info: None,
            last_server_info: Some(cached_props),
            storage_failures: 0,
            server_failures: 0,
            last_server_success: Some(stale_at),
        });
        let endpoints = EndpointServerPools::default();

        let result = handle_server_info_failure(Some(&cache), "peer-1", &endpoints, None);
        assert_eq!(result.state, ItemState::Unknown.to_string());
        assert_eq!(cache.lock().unwrap().server_failures, 1);
    }

    #[test]
    fn cached_snapshot_freshness_respects_age_and_missing_timestamp() {
        assert!(cached_snapshot_is_fresh(None), "no timestamp is treated as fresh");
        assert!(cached_snapshot_is_fresh(Some(SystemTime::now())), "a just-now success is fresh");
        let stale = SystemTime::now()
            .checked_sub(SERVER_INFO_CACHE_MAX_AGE + Duration::from_secs(1))
            .expect("test clock underflow");
        assert!(!cached_snapshot_is_fresh(Some(stale)), "an old success is stale");
    }

    #[test]
    fn server_info_probe_round_commits_failures_only_when_published() {
        let caches = vec![Mutex::new(PeerAdminCache::new())];
        let endpoints = EndpointServerPools::default();
        let probes = vec![PeerServerInfoProbe {
            host: "peer-1".to_string(),
            result: Err(PeerServerInfoProbeFailure::Rpc { health: None }),
        }];

        assert_eq!(
            caches[0]
                .lock()
                .expect("peer cache should lock before publish")
                .server_failures,
            0
        );

        let replies = publish_server_info_probe_round(&caches, &endpoints, probes);

        assert_eq!(replies.len(), 1);
        assert_eq!(replies[0].endpoint, "peer-1");
        assert_eq!(replies[0].state, ItemState::Unknown.to_string());
        assert_eq!(
            caches[0]
                .lock()
                .expect("peer cache should lock after publish")
                .server_failures,
            1
        );
    }

    #[test]
    fn server_info_probe_round_does_not_count_no_client_slots_as_rpc_failures() {
        let caches = vec![Mutex::new(PeerAdminCache::new())];
        let endpoints = EndpointServerPools::default();
        let probes = vec![PeerServerInfoProbe {
            host: "node-a:9000".to_string(),
            result: Err(PeerServerInfoProbeFailure::NoClient),
        }];

        let replies = publish_server_info_probe_round(&caches, &endpoints, probes);

        assert_eq!(replies.len(), 1);
        assert_eq!(replies[0].endpoint, "node-a:9000");
        assert_eq!(replies[0].state, ItemState::Unknown.to_string());
        assert_eq!(
            caches[0]
                .lock()
                .expect("peer cache should lock after no-client publish")
                .server_failures,
            0
        );
    }

    #[test]
    fn endpoint_host_matches_direct_and_canonicalized() {
        // Direct match (IP deployment): peer host already equals host_port.
        assert!(endpoint_host_matches("10.0.0.12:9000", "10.0.0.12:9000"));
        // Different IPs must not match.
        assert!(!endpoint_host_matches("10.0.0.12:9000", "10.0.0.99:9000"));

        // Hostname deployment: `PeerRestClient::host` is the resolved `IP:port`,
        // the endpoint keeps the raw `hostname:port`. Resolve "localhost" the
        // same way `XHost` does (avoids depending on external DNS) and confirm
        // the canonical compare matches — the regression this fixes is the
        // synthesized/degraded drive list going empty on hostname clusters.
        let resolved = XHost::try_from("localhost:9000".to_string())
            .expect("localhost should resolve")
            .to_string();
        assert!(
            endpoint_host_matches(&resolved, "localhost:9000"),
            "resolved localhost ({resolved}) must match the hostname endpoint"
        );
        // A resolved address that is not localhost must not match.
        assert!(!endpoint_host_matches("203.0.113.1:9000", "localhost:9000"));
    }

    #[test]
    fn handle_server_info_failure_returns_unknown_before_threshold_without_cache() {
        let cache = Mutex::new(PeerAdminCache::new());
        let endpoints = EndpointServerPools::default();

        let result = handle_server_info_failure(Some(&cache), "peer-1", &endpoints, None);
        assert_eq!(result.endpoint, "peer-1");
        // A probe miss below the threshold is "unknown" (not confirmed down,
        // and not the misleading "initializing"): rustfs/backlog#1049.
        assert_eq!(result.state, ItemState::Unknown.to_string());
        // The default (empty) pool has no topology entry for this host, so no
        // drives are synthesized here; the drive-synthesis and counter-balance
        // behavior is exercised by the get_online_offline_disks_stats tests in
        // admin_server_info.
        assert!(result.disks.is_empty());
        assert_eq!(cache.lock().unwrap().server_failures, 1);
    }

    #[test]
    fn handle_server_info_failure_returns_offline_after_threshold() {
        let cached_props = ServerProperties {
            endpoint: "peer-1".to_string(),
            state: "online".to_string(),
            ..Default::default()
        };

        let cache = Mutex::new(PeerAdminCache {
            last_storage_info: None,
            last_server_info: Some(cached_props),
            storage_failures: 0,
            server_failures: CONSECUTIVE_FAILURE_THRESHOLD - 1,
            last_server_success: None,
        });
        let endpoints = EndpointServerPools::default();

        let result = handle_server_info_failure(Some(&cache), "peer-1", &endpoints, None);
        assert_eq!(result.state, ItemState::Offline.to_string());
        assert_eq!(cache.lock().unwrap().server_failures, CONSECUTIVE_FAILURE_THRESHOLD);
    }

    #[test]
    fn handle_server_info_failure_returns_degraded_when_disks_online_past_threshold() {
        // Past the threshold but the peer's drives still answer the heartbeat:
        // the node is alive, only its admin RPC is stuck — report degraded (with
        // the real per-drive health), not offline (rustfs/backlog#1049 P0-B).
        let cache = Mutex::new(PeerAdminCache {
            last_storage_info: None,
            last_server_info: None,
            storage_failures: 0,
            server_failures: CONSECUTIVE_FAILURE_THRESHOLD - 1,
            last_server_success: None,
        });
        let endpoints = EndpointServerPools::default();
        let health = PeerDiskHealth {
            any_online: true,
            disks: vec![rustfs_madmin::Disk {
                endpoint: "http://peer-1:9000/data".to_string(),
                state: "ok".to_string(),
                ..Default::default()
            }],
        };

        let result = handle_server_info_failure(Some(&cache), "peer-1", &endpoints, Some(&health));
        assert_eq!(result.state, ItemState::Degraded.to_string());
        assert_eq!(result.disks.len(), 1);
        assert_eq!(result.disks[0].state, "ok");
        assert_eq!(cache.lock().unwrap().server_failures, CONSECUTIVE_FAILURE_THRESHOLD);
    }

    #[test]
    fn handle_server_info_failure_stays_offline_when_disks_also_offline() {
        // Past the threshold and the heartbeat also reports the drives down:
        // this is a genuine offline, degraded must not mask it.
        let cache = Mutex::new(PeerAdminCache {
            last_storage_info: None,
            last_server_info: None,
            storage_failures: 0,
            server_failures: CONSECUTIVE_FAILURE_THRESHOLD - 1,
            last_server_success: None,
        });
        let endpoints = EndpointServerPools::default();
        let health = PeerDiskHealth {
            any_online: false,
            disks: vec![rustfs_madmin::Disk {
                endpoint: "http://peer-1:9000/data".to_string(),
                state: ItemState::Offline.to_string().to_owned(),
                ..Default::default()
            }],
        };

        let result = handle_server_info_failure(Some(&cache), "peer-1", &endpoints, Some(&health));
        assert_eq!(result.state, ItemState::Offline.to_string());
    }

    #[test]
    fn success_resets_failure_counters_independently() {
        let cache = Mutex::new(PeerAdminCache {
            last_storage_info: None,
            last_server_info: None,
            storage_failures: 2,
            server_failures: 2,
            last_server_success: None,
        });

        {
            let mut c = cache.lock().unwrap();
            c.last_storage_info = Some(StorageInfo::default());
            c.storage_failures = 0;
        }

        let cache = cache.lock().unwrap();
        assert_eq!(cache.storage_failures, 0);
        assert_eq!(cache.server_failures, 2);
    }

    #[test]
    fn storage_failures_do_not_affect_server_failures() {
        let cache = Mutex::new(PeerAdminCache {
            last_storage_info: Some(StorageInfo::default()),
            last_server_info: Some(ServerProperties {
                endpoint: "peer-1".to_string(),
                state: "online".to_string(),
                ..Default::default()
            }),
            storage_failures: CONSECUTIVE_FAILURE_THRESHOLD - 1,
            server_failures: 0,
            last_server_success: None,
        });
        let endpoints = EndpointServerPools::default();

        let storage_result = handle_peer_failure(Some(&cache), "peer-1", &endpoints);
        assert!(storage_result.is_some());

        let server_result = handle_server_info_failure(Some(&cache), "peer-1", &endpoints, None);
        assert_eq!(server_result.state, "online");
        assert_eq!(cache.lock().unwrap().server_failures, 1);
    }

    #[test]
    fn poisoned_admin_cache_mutex_still_returns_fallbacks() {
        let storage_cache = Mutex::new(PeerAdminCache::new());
        let server_cache = Mutex::new(PeerAdminCache::new());
        let endpoints = EndpointServerPools::default();

        let _ = std::panic::catch_unwind(|| {
            let _guard = storage_cache.lock().expect("test: poison storage cache mutex");
            panic!("poison storage cache mutex");
        });
        let _ = std::panic::catch_unwind(|| {
            let _guard = server_cache.lock().expect("test: poison server cache mutex");
            panic!("poison server cache mutex");
        });

        let storage_result = handle_peer_failure(Some(&storage_cache), "peer-1", &endpoints);
        assert!(storage_result.is_none());

        let server_result = handle_server_info_failure(Some(&server_cache), "peer-1", &endpoints, None);
        assert_eq!(server_result.endpoint, "peer-1");
        assert_eq!(server_result.state, ItemState::Unknown.to_string());
    }

    #[test]
    fn poisoned_admin_cache_recovers_on_success_and_resets_failures() {
        let storage_cache = Mutex::new(PeerAdminCache {
            last_storage_info: None,
            last_server_info: None,
            storage_failures: CONSECUTIVE_FAILURE_THRESHOLD - 1,
            server_failures: 0,
            last_server_success: None,
        });
        let server_cache = Mutex::new(PeerAdminCache {
            last_storage_info: None,
            last_server_info: None,
            storage_failures: 0,
            server_failures: CONSECUTIVE_FAILURE_THRESHOLD - 1,
            last_server_success: None,
        });
        let endpoints = EndpointServerPools::default();

        let _ = std::panic::catch_unwind(|| {
            let _guard = storage_cache.lock().expect("test: poison storage cache mutex");
            panic!("poison storage cache mutex");
        });
        let _ = std::panic::catch_unwind(|| {
            let _guard = server_cache.lock().expect("test: poison server cache mutex");
            panic!("poison server cache mutex");
        });

        normalize_and_cache_peer_storage_info(
            Some(&storage_cache),
            "peer-1",
            &mut StorageInfo {
                disks: vec![rustfs_madmin::Disk {
                    endpoint: "disk-0".to_string(),
                    state: "ok".to_string(),
                    ..Default::default()
                }],
                ..Default::default()
            },
        );
        update_server_info_cache(
            Some(&server_cache),
            "peer-1",
            &ServerProperties {
                endpoint: "peer-1".to_string(),
                state: "online".to_string(),
                ..Default::default()
            },
        );

        let storage_result = handle_peer_failure(Some(&storage_cache), "peer-1", &endpoints);
        assert!(storage_result.is_some());
        assert_eq!(storage_result.unwrap().disks[0].state, "ok");

        let server_result = handle_server_info_failure(Some(&server_cache), "peer-1", &endpoints, None);
        assert_eq!(server_result.state, "online");
    }
}
