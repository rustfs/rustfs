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

use crate::cluster::rpc::{PeerRestClient, ScannerPeerActivity, TierConfigReloadOutcome};
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
use std::collections::{BTreeMap, HashMap, hash_map::DefaultHasher};
use std::future::Future;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex, OnceLock};
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
const TIER_CONFIG_RELOAD_RETRY_BASE: Duration = Duration::from_millis(100);
const TIER_CONFIG_RELOAD_RETRY_CAP: Duration = Duration::from_secs(5);
const REMOTE_VERSION_STATE_PROBE_INTERVAL: Duration = Duration::from_secs(10);
const REMOTE_VERSION_STATE_PROBE_TIMEOUT: Duration = Duration::from_secs(5);
const REMOTE_VERSION_STATE_PROOF_TTL: Duration = Duration::from_secs(30);
const CROSS_POOL_FENCE_SUPPORTED_VERSION: u32 = 1;

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
}

impl FleetCapabilityProof {
    fn token(&self) -> FleetCapabilityProofToken {
        FleetCapabilityProofToken {
            topology_fingerprint: self.topology_fingerprint.clone(),
            peer_epochs: self.peer_epochs.clone(),
        }
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
    topology_conflict: bool,
}

#[derive(Clone, PartialEq, Eq)]
pub(crate) struct RemoteVersionStateFleetProofToken(FleetCapabilityProofToken);

#[derive(Clone, PartialEq, Eq)]
pub struct CrossPoolFenceFleetProofToken(FleetCapabilityProofToken);

static REMOTE_VERSION_STATE_FLEET_PROOF: OnceLock<std::sync::RwLock<FleetCapabilityProofState>> = OnceLock::new();
static CROSS_POOL_FENCE_FLEET_PROOF: OnceLock<std::sync::RwLock<FleetCapabilityProofState>> = OnceLock::new();
static REMOTE_VERSION_STATE_PROBE_TOPOLOGY: OnceLock<String> = OnceLock::new();

fn cross_pool_fence_fleet_proof_slot() -> &'static std::sync::RwLock<FleetCapabilityProofState> {
    CROSS_POOL_FENCE_FLEET_PROOF.get_or_init(|| std::sync::RwLock::new(FleetCapabilityProofState::default()))
}

fn remote_version_state_fleet_proof_slot() -> &'static std::sync::RwLock<FleetCapabilityProofState> {
    REMOTE_VERSION_STATE_FLEET_PROOF.get_or_init(|| std::sync::RwLock::new(FleetCapabilityProofState::default()))
}

fn replace_fleet_capability_proof(slot: &std::sync::RwLock<FleetCapabilityProofState>, proof: Option<FleetCapabilityProof>) {
    slot.write().unwrap_or_else(std::sync::PoisonError::into_inner).proof = proof;
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
            let peer_epochs = state
                .proof
                .as_ref()
                .filter(|proof| proof.topology_fingerprint == topology_fingerprint && proof.peer_epochs.as_ref() == &peer_epochs)
                .map(|proof| Arc::clone(&proof.peer_epochs))
                .unwrap_or_else(|| Arc::new(peer_epochs));
            state.proof = Some(FleetCapabilityProof {
                topology_fingerprint: topology_fingerprint.to_string(),
                peer_epochs,
                expires_at: observed_at + REMOTE_VERSION_STATE_PROOF_TTL,
            });
            None
        }
        Err(err) => {
            replace_fleet_capability_proof(slot, None);
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

#[cfg(any(test, feature = "test-util"))]
pub fn rotate_cross_pool_fence_fleet_proof_for_test() -> bool {
    let mut state = cross_pool_fence_fleet_proof_slot()
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let Some(current) = state.proof.as_ref() else {
        return false;
    };
    state.proof = Some(FleetCapabilityProof {
        topology_fingerprint: current.topology_fingerprint.clone(),
        peer_epochs: Arc::new(current.peer_epochs.as_ref().clone()),
        expires_at: current.expires_at,
    });
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
    if state.topology_conflict {
        return false;
    }
    state.proof.as_ref().is_some_and(|current| {
        current.topology_fingerprint == *expected_topology
            && current.topology_fingerprint == proof.topology_fingerprint
            && Arc::ptr_eq(&current.peer_epochs, &proof.peer_epochs)
            && Instant::now() < current.expires_at
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
        replace_fleet_capability_proof(remote_version_state_fleet_proof_slot(), None);
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

fn insert_remote_version_state_peer(peer_epochs: &mut BTreeMap<String, Uuid>, peer: String, epoch: Uuid) -> Result<()> {
    if epoch.is_nil() || peer_epochs.values().any(|existing| *existing == epoch) || peer_epochs.insert(peer, epoch).is_some() {
        return Err(Error::other("remote version state capability peer identity is invalid"));
    }
    Ok(())
}

pub fn start_remote_version_state_fleet_probe(topology_fingerprint: String) {
    if REMOTE_VERSION_STATE_PROBE_TOPOLOGY.set(topology_fingerprint.clone()).is_err() {
        if REMOTE_VERSION_STATE_PROBE_TOPOLOGY.get() != Some(&topology_fingerprint) {
            for slot in [remote_version_state_fleet_proof_slot(), cross_pool_fence_fleet_proof_slot()] {
                let mut state = slot.write().unwrap_or_else(std::sync::PoisonError::into_inner);
                state.topology_conflict = true;
                state.proof = None;
            }
        }
        return;
    }

    tokio::spawn(async move {
        loop {
            let result = match get_global_notification_sys() {
                Some(notification_sys) => {
                    match timeout(
                        REMOTE_VERSION_STATE_PROBE_TIMEOUT,
                        notification_sys.probe_remote_version_state_fleet(&topology_fingerprint),
                    )
                    .await
                    {
                        Ok(result) => result,
                        Err(_) => Err(Error::other("remote version state fleet capability probe timed out")),
                    }
                }
                None => Err(Error::other("remote version state fleet capability notification system is unavailable")),
            };
            let fence_result = match get_global_notification_sys() {
                Some(notification_sys) => timeout(
                    REMOTE_VERSION_STATE_PROBE_TIMEOUT,
                    notification_sys.probe_cross_pool_fence_fleet(&topology_fingerprint),
                )
                .await
                .unwrap_or_else(|_| Err(Error::other("cross-pool fence fleet capability probe timed out"))),
                None => Err(Error::other("cross-pool fence fleet capability notification system is unavailable")),
            };
            let topology_conflict = remote_version_state_fleet_proof_slot()
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .topology_conflict;
            if topology_conflict {
                replace_fleet_capability_proof(remote_version_state_fleet_proof_slot(), None);
                replace_fleet_capability_proof(cross_pool_fence_fleet_proof_slot(), None);
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
                    capability = "cross_pool_fence_v1",
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

    async fn probe_cross_pool_fence_fleet(&self, topology_fingerprint: &str) -> Result<BTreeMap<String, Uuid>> {
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
        for result in join_all(probes).await {
            let (peer, version, epoch) = result?;
            if version < CROSS_POOL_FENCE_SUPPORTED_VERSION {
                return Err(Error::other("cross-pool fence capability version is unsupported"));
            }
            insert_remote_version_state_peer(&mut peer_epochs, peer, epoch)?;
        }
        Ok(peer_epochs)
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
        let peer_timeout = Duration::from_secs(5);

        for (idx, client) in self.peer_clients.iter().enumerate() {
            let endpoints = endpoints.clone();
            let cache = self.peer_admin_caches.get(idx);
            futures.push(async move {
                if let Some(client) = client {
                    let host = client.host.to_string();
                    match timeout(peer_timeout, client.local_storage_info()).await {
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
                            client.evict_connection().await;
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
        let peer_timeout = Duration::from_secs(5);

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

                // First attempt. A single evicted or half-open internode channel
                // is enough to fail one probe and, before retrying, would drop
                // the member to unknown/offline for this whole snapshot. So on any
                // first-attempt failure we evict the channel and re-dial once
                // before falling back (rustfs/backlog#1049, P1-B).
                match timeout(peer_timeout, client.server_info()).await {
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
                client.prepare_retry().await;

                // Second and final attempt on the fresh channel.
                match timeout(peer_timeout, client.server_info()).await {
                    Ok(Ok(info)) => PeerServerInfoProbe { host, result: Ok(info) },
                    Ok(Err(err)) => {
                        warn!("peer {host} server_info failed after retry: {err}");
                        let health = peer_disk_health(&host).await;
                        PeerServerInfoProbe {
                            host,
                            result: Err(PeerServerInfoProbeFailure::Rpc { health }),
                        }
                    }
                    Err(_) => {
                        warn!("peer {host} server_info timed out after retry ({peer_timeout:?})");
                        client.evict_connection().await;
                        let health = peer_disk_health(&host).await;
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

        match store.stop_rebalance_for_id(expected_rebalance_id).await {
            Ok(_) => {
                if let Err(err) = store.save_rebalance_stats(usize::MAX, RebalSaveOpt::StoppedAt).await {
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
                scanner_activity_with_timeout(SCANNER_ACTIVITY_PROBE_TIMEOUT, &host, client.scanner_activity())
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

    pub async fn acknowledge_scanner_dirty_usage(&self, acknowledgements: Vec<(String, String, u64)>) -> Result<bool> {
        let mut by_host = HashMap::with_capacity(acknowledgements.len());
        for (host, instance_id, generation) in acknowledgements {
            if by_host.insert(host.clone(), (instance_id, generation)).is_some() {
                return Err(Error::other(format!("duplicate scanner dirty usage acknowledgement target: {host}")));
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
        for (host, (instance_id, generation)) in by_host {
            let Some(client) = clients.get(&host).cloned() else {
                failures.push(format!("peer {host} scanner dirty usage acknowledgement failed: peer is not reachable"));
                continue;
            };
            futures.push(async move {
                let result = scanner_activity_with_timeout(
                    SCANNER_ACTIVITY_PROBE_TIMEOUT,
                    &host,
                    client.acknowledge_scanner_dirty_usage(instance_id, generation),
                )
                .await;
                (host, result)
            });
        }
        aggregate_scanner_dirty_usage_acknowledgement_results(join_all(futures).await, failures)
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

    pub async fn abort_tier_mutation(&self, mutation_id: Uuid) -> Vec<NotificationPeerErr> {
        let mut futures = Vec::with_capacity(self.peer_clients.len());
        for client in self.peer_clients.iter().cloned() {
            futures.push(async move {
                if let Some(client) = client {
                    notification_peer_result(client.host.to_string(), client.abort_tier_mutation(mutation_id).await)
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
                        rustfs_common::heal_channel::DriveState::Ok.to_string()
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

    #[test]
    fn remote_version_state_fleet_proof_rejects_stale_or_mismatched_membership() {
        let now = Instant::now();
        let mut peer_epochs = BTreeMap::new();
        peer_epochs.insert("peer-a".to_string(), Uuid::new_v4());
        let proof = FleetCapabilityProof {
            topology_fingerprint: "topology-a".to_string(),
            peer_epochs: Arc::new(peer_epochs),
            expires_at: now + Duration::from_secs(1),
        };

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
        let proof = FleetCapabilityProof {
            topology_fingerprint: "topology-a".to_string(),
            peer_epochs: Arc::new(BTreeMap::new()),
            expires_at: now + Duration::from_secs(1),
        };

        assert!(fleet_capability_proof_valid_at(Some(&proof), "topology-a", now));
    }

    #[test]
    fn remote_version_state_fleet_proof_token_changes_with_process_epoch() {
        let now = Instant::now();
        let proof = FleetCapabilityProof {
            topology_fingerprint: "topology-a".to_string(),
            peer_epochs: Arc::new(BTreeMap::from([("peer-a".to_string(), Uuid::new_v4())])),
            expires_at: now + Duration::from_secs(1),
        };
        let captured = proof.token();
        let restarted = FleetCapabilityProof {
            topology_fingerprint: proof.topology_fingerprint.clone(),
            peer_epochs: Arc::new(BTreeMap::from([("peer-a".to_string(), Uuid::new_v4())])),
            expires_at: proof.expires_at,
        };

        assert!(captured != restarted.token());
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
    fn remote_version_state_fleet_proof_conflict_revokes_atomic_snapshot() {
        let now = Instant::now();
        let mut state = FleetCapabilityProofState {
            proof: Some(FleetCapabilityProof {
                topology_fingerprint: "topology-a".to_string(),
                peer_epochs: Arc::new(BTreeMap::new()),
                expires_at: now + Duration::from_secs(1),
            }),
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

    fn build_props(endpoint: &str) -> ServerProperties {
        ServerProperties {
            endpoint: endpoint.to_string(),
            ..Default::default()
        }
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
            .acknowledge_scanner_dirty_usage(vec![("peer-1".to_string(), "0123456789abcdef0123456789abcdef".to_string(), 7)])
            .await
            .expect_err("a missing acknowledgement target must remain pending");
        assert!(missing.to_string().contains("peer is not reachable"));

        let duplicate = sys
            .acknowledge_scanner_dirty_usage(vec![
                ("peer-1".to_string(), "0123456789abcdef0123456789abcdef".to_string(), 7),
                ("peer-1".to_string(), "0123456789abcdef0123456789abcdef".to_string(), 7),
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

        let abort = sys.abort_tier_mutation(mutation_id).await;
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
