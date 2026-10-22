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

//! Startup peer re-admission gate (rustfs#8669).
//!
//! Local startup readiness only proves that *this* node sees storage, IAM and
//! lock quorum. During a one-pod-at-a-time rollout the peers can still count
//! the restarted node's drives as unavailable: their dials keep hitting the
//! previous pod IP until the platform DNS TTL expires, and a recovered remote
//! drive then walks Offline -> Returning -> Online. An orchestrator that
//! advances on this node's Ready can stop the next pod while the survivors are
//! still short of read quorum.
//!
//! The gate asks the peers in every erasure set this node serves how they
//! currently see its drives, over the authenticated internode RPC channel, and
//! holds the startup `FullReady` transition until a strict majority of each
//! set's peers count all of this node's drives in that set as online.
//!
//! The gate is bounded and fails open. It publishes readiness when the startup
//! budget elapses, immediately when older peers cannot answer and the majority
//! is out of reach, and after [`UNREACHABLE_PEER_GRACE`] when unreachable peers
//! keep the majority out of reach. It runs only on the startup transition;
//! steady-state `/health/ready` and the S3 request path never consult it.

use super::{
    NODE_STORAGE_READINESS_TIMEOUT, STARTUP_RUNTIME_READINESS_POLL_INTERVAL, collect_lock_quorum_observation,
    exclusive_lock_quorum, health_cluster_timeout, node_disk_snapshot, runtime_sources,
};
use crate::storage_api::error::StorageError;
use crate::storage_api::server::readiness::contract::admin::{DiskSetSelector, StorageAdminApi};
use crate::storage_api::server::readiness::{
    DiskStore, EndpointServerPools, PeerRestClient, disk_endpoint_snapshot, get_global_notification_sys, is_dist_erasure,
};
use rustfs_madmin::BackendInfo;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fmt;
use std::future::Future;
use std::time::Duration;
use tracing::{debug, info, warn};

const EVENT_STARTUP_PEER_READMISSION: &str = "startup_peer_readmission";
const LOG_COMPONENT_SERVER: &str = "server";
const LOG_SUBSYSTEM_READINESS: &str = "readiness";

/// How long unreachable peers may keep a set's majority out of reach before the
/// gate stops waiting for them. A dead peer must not block a rollout for the
/// whole startup budget.
pub(super) const UNREACHABLE_PEER_GRACE: Duration = Duration::from_secs(15);

fn startup_peer_readmission_enabled() -> bool {
    rustfs_utils::get_env_bool(
        rustfs_config::ENV_STARTUP_PEER_READMISSION_ENABLE,
        rustfs_config::DEFAULT_STARTUP_PEER_READMISSION_ENABLE,
    )
}

// ---------------------------------------------------------------------------
// Peer side: report how this node sees another host's drives.
// ---------------------------------------------------------------------------

/// This node's readiness view of one drive served by another host.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PeerDriveObservation {
    pub(crate) pool_index: i32,
    pub(crate) set_index: i32,
    pub(crate) disk_index: i32,
    /// Same predicate as `/health/ready`: the drive counts towards read quorum.
    pub(crate) online: bool,
    pub(crate) runtime_state: String,
    pub(crate) host_online: bool,
}

#[derive(Debug)]
pub(crate) enum PeerDriveAdmissionError {
    NotInitialized,
    UnknownHost,
    Storage(StorageError),
}

impl PeerDriveAdmissionError {
    pub(crate) fn not_initialized(&self) -> bool {
        matches!(self, Self::NotInitialized)
    }
}

impl fmt::Display for PeerDriveAdmissionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotInitialized => f.write_str("storage layer not initialized"),
            Self::UnknownHost => f.write_str("host is not a remote member of this node's topology"),
            Self::Storage(err) => write!(f, "drive inventory unavailable: {err}"),
        }
    }
}

/// Answer a peer's `PeerDriveAdmission` RPC from local runtime state only.
///
/// Reuses the node readiness inputs (cached lock-client reachability and the
/// in-memory drive health tracker); no disk-info RPC or filesystem probe runs.
pub(crate) async fn observe_peer_drive_admission(host: &str) -> Result<Vec<PeerDriveObservation>, PeerDriveAdmissionError> {
    let (Some(store), Some(endpoint_pools)) = (
        runtime_sources::current_object_store_handle(),
        runtime_sources::current_endpoints_handle(),
    ) else {
        return Err(PeerDriveAdmissionError::NotInitialized);
    };
    if !is_remote_topology_host(&endpoint_pools, host) {
        return Err(PeerDriveAdmissionError::UnknownHost);
    }
    let lock_observation = collect_lock_quorum_observation().await;
    host_drive_observations(store.as_ref(), host, &lock_observation.online_hosts)
        .await
        .map_err(PeerDriveAdmissionError::Storage)
}

fn is_remote_topology_host(endpoint_pools: &EndpointServerPools, host: &str) -> bool {
    !host.is_empty()
        && endpoint_pools
            .as_ref()
            .iter()
            .flat_map(|pool| pool.endpoints.as_ref().iter())
            .any(|endpoint| !endpoint.is_local && endpoint.host_port() == host)
}

pub(super) async fn host_drive_observations<S>(
    store: &S,
    host: &str,
    online_hosts: &HashSet<String>,
) -> Result<Vec<PeerDriveObservation>, StorageError>
where
    S: StorageAdminApi<BackendInfo = BackendInfo, Disk = DiskStore, Error = StorageError>,
{
    tokio::time::timeout(NODE_STORAGE_READINESS_TIMEOUT, async {
        let backend = store.backend_info().await;
        let mut drives = Vec::new();
        for (pool_idx, &set_count) in backend.total_sets.iter().enumerate() {
            for set_idx in 0..set_count {
                for disk in store
                    .disk_set_inventory(DiskSetSelector::new(pool_idx, set_idx))
                    .await?
                    .into_iter()
                    .flatten()
                {
                    let endpoint = disk_endpoint_snapshot(&disk);
                    if endpoint.is_local || endpoint.host_port() != host {
                        continue;
                    }
                    let (pool_index, set_index, disk_index) = (endpoint.pool_idx, endpoint.set_idx, endpoint.disk_idx);
                    let runtime_state = disk.runtime_state().as_str().to_string();
                    let (_, unavailable) = node_disk_snapshot(endpoint, &runtime_state, online_hosts);
                    drives.push(PeerDriveObservation {
                        pool_index,
                        set_index,
                        disk_index,
                        online: unavailable.is_none(),
                        host_online: online_hosts.contains(host),
                        runtime_state,
                    });
                }
            }
        }
        Ok(drives)
    })
    .await
    .map_err(|_| StorageError::Timeout)?
}

// ---------------------------------------------------------------------------
// Restarting node: wait for a majority of each set's peers to re-admit us.
// ---------------------------------------------------------------------------

/// One erasure set that mixes this node's drives with peer drives.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ReadmissionSet {
    pub(super) pool_index: i32,
    pub(super) set_index: i32,
    pub(super) local_disks: BTreeSet<i32>,
    pub(super) peers: BTreeSet<String>,
    pub(super) required_peers: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ReadmissionPlan {
    pub(super) local_host: String,
    pub(super) sets: Vec<ReadmissionSet>,
}

impl ReadmissionPlan {
    fn peers(&self) -> BTreeSet<String> {
        self.sets.iter().flat_map(|set| set.peers.iter().cloned()).collect()
    }
}

/// Build the per-set peer quorum this node must reach before startup readiness.
///
/// Each set requires a strict majority of its distinct peer hosts. A simple
/// half would let a 3-node set publish on one peer's view, and that peer may be
/// the next one the orchestrator stops.
pub(super) fn readmission_plan(endpoint_pools: &EndpointServerPools) -> Result<ReadmissionPlan, &'static str> {
    let mut local_hosts = BTreeSet::new();
    let mut sets: BTreeMap<(i32, i32), (BTreeSet<i32>, BTreeSet<String>)> = BTreeMap::new();
    for endpoint in endpoint_pools.as_ref().iter().flat_map(|pool| pool.endpoints.as_ref().iter()) {
        let host = endpoint.host_port();
        let (local_disks, peers) = sets.entry((endpoint.pool_idx, endpoint.set_idx)).or_default();
        if endpoint.is_local {
            local_disks.insert(endpoint.disk_idx);
            if !host.is_empty() {
                local_hosts.insert(host);
            }
        } else if !host.is_empty() {
            peers.insert(host);
        }
    }

    let mut local_hosts = local_hosts.into_iter();
    let (Some(local_host), None) = (local_hosts.next(), local_hosts.next()) else {
        return Err("local_topology_host_ambiguous");
    };

    let sets = sets
        .into_iter()
        .filter(|(_, (local_disks, peers))| !local_disks.is_empty() && !peers.is_empty())
        .map(|((pool_index, set_index), (local_disks, peers))| ReadmissionSet {
            pool_index,
            set_index,
            required_peers: exclusive_lock_quorum(peers.len()),
            local_disks,
            peers,
        })
        .collect::<Vec<_>>();
    if sets.is_empty() {
        return Err("no_set_shared_with_peers");
    }
    Ok(ReadmissionPlan { local_host, sets })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct PeerDriveView {
    pub(super) pool_index: i32,
    pub(super) set_index: i32,
    pub(super) disk_index: i32,
    pub(super) online: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum PeerProbe {
    /// The peer answered with its view of this node's drives.
    Answered(Vec<PeerDriveView>),
    /// The peer predates the RPC and will never answer.
    Unsupported,
    /// The RPC failed or timed out; the peer may answer later.
    Unreachable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SetState {
    Admitted,
    Waiting,
    /// The majority needs a peer that is currently unreachable.
    NeedsUnreachablePeer,
    /// Even if every unreachable peer answered, the majority is out of reach.
    Unattainable,
}

fn peer_admits_set(drives: &[PeerDriveView], set: &ReadmissionSet) -> bool {
    set.local_disks.iter().all(|disk_index| {
        drives.iter().any(|drive| {
            drive.pool_index == set.pool_index
                && drive.set_index == set.set_index
                && drive.disk_index == *disk_index
                && drive.online
        })
    })
}

fn set_state(set: &ReadmissionSet, probes: &HashMap<String, PeerProbe>) -> SetState {
    let (mut admitted, mut pending, mut unreachable) = (0usize, 0usize, 0usize);
    for peer in &set.peers {
        match probes.get(peer) {
            Some(PeerProbe::Answered(drives)) if peer_admits_set(drives, set) => admitted += 1,
            Some(PeerProbe::Answered(_)) => pending += 1,
            Some(PeerProbe::Unsupported) => {}
            Some(PeerProbe::Unreachable) | None => unreachable += 1,
        }
    }
    if admitted >= set.required_peers {
        SetState::Admitted
    } else if admitted + pending + unreachable < set.required_peers {
        SetState::Unattainable
    } else if admitted + pending < set.required_peers {
        SetState::NeedsUnreachablePeer
    } else {
        SetState::Waiting
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct ReadmissionProgress {
    pub(super) total_sets: usize,
    pub(super) admitted_sets: usize,
    pub(super) waiting_sets: usize,
    pub(super) unreachable_sets: usize,
    pub(super) unattainable_sets: usize,
}

fn evaluate(plan: &ReadmissionPlan, probes: &HashMap<String, PeerProbe>) -> ReadmissionProgress {
    let mut progress = ReadmissionProgress {
        total_sets: plan.sets.len(),
        ..Default::default()
    };
    for set in &plan.sets {
        match set_state(set, probes) {
            SetState::Admitted => progress.admitted_sets += 1,
            SetState::Waiting => progress.waiting_sets += 1,
            SetState::NeedsUnreachablePeer => progress.unreachable_sets += 1,
            SetState::Unattainable => progress.unattainable_sets += 1,
        }
    }
    progress
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum GiveUpReason {
    DeadlineElapsed,
    PeersUnreachable,
    PeersCannotReport,
}

impl GiveUpReason {
    fn as_str(self) -> &'static str {
        match self {
            Self::DeadlineElapsed => "deadline_elapsed",
            Self::PeersUnreachable => "peers_unreachable",
            Self::PeersCannotReport => "peers_cannot_report",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ReadmissionOutcome {
    Admitted {
        waited: Duration,
    },
    Skipped(&'static str),
    GaveUp {
        reason: GiveUpReason,
        waited: Duration,
        progress: ReadmissionProgress,
    },
}

/// Poll `probe` until every planned set is re-admitted or the gate gives up.
///
/// The loop always terminates: by `deadline`, by `unreachable_grace` of
/// continuous unreachable shortfall, or immediately when peers that cannot
/// report make the majority unattainable.
pub(super) async fn wait_for_peer_readmission_with<P, Fut>(
    plan: &ReadmissionPlan,
    deadline: tokio::time::Instant,
    poll_interval: Duration,
    unreachable_grace: Duration,
    mut probe: P,
) -> ReadmissionOutcome
where
    P: FnMut() -> Fut,
    Fut: Future<Output = HashMap<String, PeerProbe>>,
{
    let started = tokio::time::Instant::now();
    let mut unreachable_since: Option<tokio::time::Instant> = None;
    let mut last_logged: Option<ReadmissionProgress> = None;

    loop {
        let probes = probe().await;
        let progress = evaluate(plan, &probes);
        let now = tokio::time::Instant::now();
        let waited = now.saturating_duration_since(started);
        let give_up = |reason| ReadmissionOutcome::GaveUp {
            reason,
            waited,
            progress,
        };

        if progress.admitted_sets == progress.total_sets {
            return ReadmissionOutcome::Admitted { waited };
        }
        if progress.unattainable_sets > 0 {
            return give_up(GiveUpReason::PeersCannotReport);
        }
        if progress.unreachable_sets > 0 {
            let since = *unreachable_since.get_or_insert(now);
            if now.saturating_duration_since(since) >= unreachable_grace {
                return give_up(GiveUpReason::PeersUnreachable);
            }
        } else {
            unreachable_since = None;
        }
        if now >= deadline {
            return give_up(GiveUpReason::DeadlineElapsed);
        }

        if last_logged != Some(progress) {
            info!(
                event = EVENT_STARTUP_PEER_READMISSION,
                component = LOG_COMPONENT_SERVER,
                subsystem = LOG_SUBSYSTEM_READINESS,
                state = "waiting",
                total_sets = progress.total_sets,
                admitted_sets = progress.admitted_sets,
                waiting_sets = progress.waiting_sets,
                unreachable_sets = progress.unreachable_sets,
                "waiting for peers to re-admit this node"
            );
            last_logged = Some(progress);
        }
        tokio::time::sleep(poll_interval.min(deadline.saturating_duration_since(now))).await;
    }
}

async fn probe_peers(clients: &[(String, PeerRestClient)], local_host: &str, timeout: Duration) -> HashMap<String, PeerProbe> {
    futures::future::join_all(clients.iter().map(|(host, client)| async move {
        let probe = match tokio::time::timeout(timeout, client.peer_drive_admission(local_host)).await {
            Ok(Ok(Some(drives))) => PeerProbe::Answered(
                drives
                    .into_iter()
                    .map(|drive| PeerDriveView {
                        pool_index: drive.pool_index,
                        set_index: drive.set_index,
                        disk_index: drive.disk_index,
                        online: drive.online,
                    })
                    .collect(),
            ),
            Ok(Ok(None)) => PeerProbe::Unsupported,
            Ok(Err(err)) => {
                debug!(
                    event = EVENT_STARTUP_PEER_READMISSION,
                    component = LOG_COMPONENT_SERVER,
                    subsystem = LOG_SUBSYSTEM_READINESS,
                    result = "peer_error",
                    peer = %host,
                    error = %err,
                    "peer re-admission probe failed"
                );
                PeerProbe::Unreachable
            }
            Err(_) => PeerProbe::Unreachable,
        };
        (host.clone(), probe)
    }))
    .await
    .into_iter()
    .collect()
}

/// Run the startup re-admission gate against the live topology and peers.
pub(super) async fn wait_for_startup_peer_readmission(deadline: tokio::time::Instant) -> ReadmissionOutcome {
    if !startup_peer_readmission_enabled() {
        return ReadmissionOutcome::Skipped("disabled");
    }
    if !is_dist_erasure().await {
        return ReadmissionOutcome::Skipped("not_distributed");
    }
    let Some(endpoint_pools) = runtime_sources::current_endpoints_handle() else {
        return ReadmissionOutcome::Skipped("topology_unavailable");
    };
    let plan = match readmission_plan(&endpoint_pools) {
        Ok(plan) => plan,
        Err(reason) => return ReadmissionOutcome::Skipped(reason),
    };
    let Some(notification_sys) = get_global_notification_sys() else {
        return ReadmissionOutcome::Skipped("peer_clients_unavailable");
    };
    let peers = plan.peers();
    // A planned peer without a client stays unreachable, so the grace bound applies.
    let clients = notification_sys
        .peer_clients
        .iter()
        .flatten()
        .filter(|client| peers.contains(client.topology_member()))
        .map(|client| (client.topology_member().to_string(), client.clone()))
        .collect::<Vec<_>>();
    let timeout = health_cluster_timeout();
    let local_host = plan.local_host.as_str();
    wait_for_peer_readmission_with(&plan, deadline, STARTUP_RUNTIME_READINESS_POLL_INTERVAL, UNREACHABLE_PEER_GRACE, || {
        probe_peers(&clients, local_host, timeout)
    })
    .await
}

pub(super) fn log_readmission_outcome(outcome: &ReadmissionOutcome) {
    match outcome {
        ReadmissionOutcome::Admitted { waited } => info!(
            event = EVENT_STARTUP_PEER_READMISSION,
            component = LOG_COMPONENT_SERVER,
            subsystem = LOG_SUBSYSTEM_READINESS,
            result = "admitted",
            waited_ms = u64::try_from(waited.as_millis()).unwrap_or(u64::MAX),
            "peers re-admitted this node"
        ),
        // Single-node deployments have no peers; keep their startup log quiet.
        ReadmissionOutcome::Skipped(reason @ "not_distributed") => debug!(
            event = EVENT_STARTUP_PEER_READMISSION,
            component = LOG_COMPONENT_SERVER,
            subsystem = LOG_SUBSYSTEM_READINESS,
            result = "skipped",
            reason = *reason,
            "peer re-admission gate skipped"
        ),
        ReadmissionOutcome::Skipped(reason) => info!(
            event = EVENT_STARTUP_PEER_READMISSION,
            component = LOG_COMPONENT_SERVER,
            subsystem = LOG_SUBSYSTEM_READINESS,
            result = "skipped",
            reason = *reason,
            "peer re-admission gate skipped"
        ),
        ReadmissionOutcome::GaveUp {
            reason,
            waited,
            progress,
        } => warn!(
            event = EVENT_STARTUP_PEER_READMISSION,
            component = LOG_COMPONENT_SERVER,
            subsystem = LOG_SUBSYSTEM_READINESS,
            result = "gave_up",
            reason = reason.as_str(),
            waited_ms = u64::try_from(waited.as_millis()).unwrap_or(u64::MAX),
            total_sets = progress.total_sets,
            admitted_sets = progress.admitted_sets,
            waiting_sets = progress.waiting_sets,
            unreachable_sets = progress.unreachable_sets,
            unattainable_sets = progress.unattainable_sets,
            "publishing startup readiness without peer re-admission"
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage_api::server::readiness::{Endpoint, Endpoints, PoolEndpoints};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn endpoint(host: &str, is_local: bool, set_idx: i32, disk_idx: i32) -> Endpoint {
        Endpoint {
            url: url::Url::parse(&format!("http://{host}:9000/data{disk_idx}")).expect("test endpoint url"),
            is_local,
            pool_idx: 0,
            set_idx,
            disk_idx,
        }
    }

    fn pools(set_count: usize, drives_per_set: usize, endpoints: Vec<Endpoint>) -> EndpointServerPools {
        EndpointServerPools::from(vec![PoolEndpoints {
            legacy: false,
            set_count,
            drives_per_set,
            endpoints: Endpoints::from(endpoints),
            cmd_line: String::new(),
            platform: String::new(),
        }])
    }

    /// Three pods, one drive each, one set: the #8669 topology seen from node-2.
    fn three_node_plan() -> ReadmissionPlan {
        readmission_plan(&pools(
            1,
            3,
            vec![
                endpoint("node-0", false, 0, 0),
                endpoint("node-1", false, 0, 1),
                endpoint("node-2", true, 0, 2),
            ],
        ))
        .expect("three-node plan")
    }

    fn answered(online: bool) -> PeerProbe {
        PeerProbe::Answered(vec![PeerDriveView {
            pool_index: 0,
            set_index: 0,
            disk_index: 2,
            online,
        }])
    }

    fn probes(entries: &[(&str, PeerProbe)]) -> HashMap<String, PeerProbe> {
        entries
            .iter()
            .map(|(host, probe)| (format!("{host}:9000"), probe.clone()))
            .collect()
    }

    #[test]
    fn plan_requires_a_strict_majority_of_each_shared_sets_peers() {
        let plan = three_node_plan();
        assert_eq!(plan.local_host, "node-2:9000");
        assert_eq!(plan.sets.len(), 1);
        assert_eq!(plan.sets[0].local_disks, BTreeSet::from([2]));
        assert_eq!(plan.sets[0].required_peers, 2, "a 3-node set must not publish on one peer's view");

        // Two sets of four drives on four nodes; node-0 serves only set 0.
        let plan = readmission_plan(&pools(
            2,
            4,
            vec![
                endpoint("node-0", true, 0, 0),
                endpoint("node-1", false, 0, 1),
                endpoint("node-2", false, 0, 2),
                endpoint("node-3", false, 0, 3),
                endpoint("node-1", false, 1, 0),
                endpoint("node-2", false, 1, 1),
                endpoint("node-3", false, 1, 2),
                endpoint("node-3", false, 1, 3),
            ],
        ))
        .expect("multi-set plan");
        assert_eq!(plan.sets.len(), 1, "sets without a local drive are not gated");
        assert_eq!(plan.sets[0].peers.len(), 3);
        assert_eq!(plan.sets[0].required_peers, 2);
    }

    #[test]
    fn plan_skips_topologies_without_peers_or_a_single_local_host() {
        assert_eq!(
            readmission_plan(&pools(1, 2, vec![endpoint("node-0", true, 0, 0), endpoint("node-0", true, 0, 1)])),
            Err("no_set_shared_with_peers")
        );
        assert_eq!(
            readmission_plan(&pools(1, 2, vec![endpoint("node-0", false, 0, 0), endpoint("node-1", false, 0, 1)])),
            Err("local_topology_host_ambiguous")
        );
    }

    #[test]
    fn set_state_separates_waiting_unreachable_and_unattainable_shortfalls() {
        let plan = three_node_plan();
        let set = &plan.sets[0];
        let state = |entries: &[(&str, PeerProbe)]| set_state(set, &probes(entries));

        assert_eq!(state(&[("node-0", answered(true)), ("node-1", answered(true))]), SetState::Admitted);
        assert_eq!(state(&[("node-0", answered(true)), ("node-1", answered(false))]), SetState::Waiting);
        assert_eq!(
            state(&[("node-0", answered(true)), ("node-1", PeerProbe::Unreachable)]),
            SetState::NeedsUnreachablePeer
        );
        assert_eq!(
            state(&[("node-0", answered(true))]),
            SetState::NeedsUnreachablePeer,
            "missing probe is unreachable"
        );
        assert_eq!(
            state(&[("node-0", answered(true)), ("node-1", PeerProbe::Unsupported)]),
            SetState::Unattainable
        );
        // A peer that answers without listing our drive has not re-admitted it.
        assert_eq!(
            state(&[("node-0", answered(true)), ("node-1", PeerProbe::Answered(Vec::new()))]),
            SetState::Waiting
        );
    }

    #[tokio::test(start_paused = true)]
    async fn gate_holds_until_peers_count_the_drive_online() {
        let plan = three_node_plan();
        let polls = Arc::new(AtomicUsize::new(0));
        let deadline = tokio::time::Instant::now() + Duration::from_secs(120);
        let outcome = wait_for_peer_readmission_with(&plan, deadline, Duration::from_secs(1), UNREACHABLE_PEER_GRACE, || {
            let poll = polls.fetch_add(1, Ordering::SeqCst);
            async move {
                // node-1 keeps dialing the previous pod IP for five polls.
                probes(&[("node-0", answered(true)), ("node-1", answered(poll >= 5))])
            }
        })
        .await;

        assert_eq!(
            outcome,
            ReadmissionOutcome::Admitted {
                waited: Duration::from_secs(5)
            }
        );
        assert_eq!(polls.load(Ordering::SeqCst), 6);
    }

    #[tokio::test(start_paused = true)]
    async fn gate_gives_up_at_the_startup_deadline() {
        let plan = three_node_plan();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        let outcome = wait_for_peer_readmission_with(&plan, deadline, Duration::from_secs(1), UNREACHABLE_PEER_GRACE, || async {
            probes(&[("node-0", answered(true)), ("node-1", answered(false))])
        })
        .await;

        let ReadmissionOutcome::GaveUp {
            reason,
            waited,
            progress,
        } = outcome
        else {
            panic!("expected the bounded wait to give up, got {outcome:?}");
        };
        assert_eq!(reason, GiveUpReason::DeadlineElapsed);
        assert_eq!(waited, Duration::from_secs(30));
        assert_eq!(progress.waiting_sets, 1);
    }

    #[tokio::test(start_paused = true)]
    async fn dead_peer_releases_the_gate_after_the_unreachable_grace() {
        let plan = three_node_plan();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(120);
        let outcome = wait_for_peer_readmission_with(&plan, deadline, Duration::from_secs(1), UNREACHABLE_PEER_GRACE, || async {
            probes(&[("node-0", answered(true)), ("node-1", PeerProbe::Unreachable)])
        })
        .await;

        let ReadmissionOutcome::GaveUp { reason, waited, .. } = outcome else {
            panic!("expected a dead peer to release the gate, got {outcome:?}");
        };
        assert_eq!(reason, GiveUpReason::PeersUnreachable);
        assert_eq!(waited, UNREACHABLE_PEER_GRACE);
    }

    #[tokio::test(start_paused = true)]
    async fn unreachable_grace_restarts_when_the_peer_comes_back() {
        let plan = three_node_plan();
        let polls = Arc::new(AtomicUsize::new(0));
        let deadline = tokio::time::Instant::now() + Duration::from_secs(120);
        let outcome = wait_for_peer_readmission_with(&plan, deadline, Duration::from_secs(1), UNREACHABLE_PEER_GRACE, || {
            let poll = polls.fetch_add(1, Ordering::SeqCst);
            async move {
                let node_1 = match poll {
                    0..10 => PeerProbe::Unreachable,
                    10..30 => answered(false),
                    _ => answered(true),
                };
                probes(&[("node-0", answered(true)), ("node-1", node_1)])
            }
        })
        .await;

        assert_eq!(
            outcome,
            ReadmissionOutcome::Admitted {
                waited: Duration::from_secs(30)
            }
        );
    }

    #[tokio::test(start_paused = true)]
    async fn older_peers_that_cannot_report_release_the_gate_immediately() {
        let plan = three_node_plan();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(120);
        let outcome = wait_for_peer_readmission_with(&plan, deadline, Duration::from_secs(1), UNREACHABLE_PEER_GRACE, || async {
            probes(&[("node-0", PeerProbe::Unsupported), ("node-1", PeerProbe::Unsupported)])
        })
        .await;

        let ReadmissionOutcome::GaveUp { reason, waited, .. } = outcome else {
            panic!("expected a rolling upgrade from older peers to skip the wait, got {outcome:?}");
        };
        assert_eq!(reason, GiveUpReason::PeersCannotReport);
        assert_eq!(waited, Duration::ZERO);
    }

    #[test]
    fn peer_side_rejects_hosts_outside_its_remote_topology() {
        let topology = pools(
            1,
            3,
            vec![
                endpoint("node-0", true, 0, 0),
                endpoint("node-1", false, 0, 1),
                endpoint("node-2", false, 0, 2),
            ],
        );
        assert!(is_remote_topology_host(&topology, "node-2:9000"));
        assert!(!is_remote_topology_host(&topology, "node-0:9000"), "a node does not answer for itself");
        assert!(!is_remote_topology_host(&topology, "node-9:9000"));
        assert!(!is_remote_topology_host(&topology, ""));
    }
}
