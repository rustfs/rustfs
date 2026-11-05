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
use futures::{Stream, StreamExt, stream::FuturesUnordered};
use rustfs_madmin::BackendInfo;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fmt;
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

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct ReadmissionProgress {
    pub(super) total_sets: usize,
    pub(super) admitted_sets: usize,
    pub(super) waiting_sets: usize,
    pub(super) unreachable_sets: usize,
    pub(super) unattainable_sets: usize,
}

impl ReadmissionProgress {
    fn unobserved(total_sets: usize) -> Self {
        Self {
            total_sets,
            unreachable_sets: total_sets,
            ..Self::default()
        }
    }

    fn count_mut(&mut self, state: SetState) -> &mut usize {
        match state {
            SetState::Admitted => &mut self.admitted_sets,
            SetState::Waiting => &mut self.waiting_sets,
            SetState::NeedsUnreachablePeer => &mut self.unreachable_sets,
            SetState::Unattainable => &mut self.unattainable_sets,
        }
    }

    fn replace(&mut self, before: SetState, after: SetState) {
        if before != after {
            *self.count_mut(before) -= 1;
            *self.count_mut(after) += 1;
        }
    }
}

#[derive(Clone, Copy)]
enum PeerVote {
    Admitted,
    Waiting,
    Unsupported,
    Unreachable,
}

impl PeerVote {
    fn counts(self) -> (usize, usize, usize) {
        match self {
            Self::Admitted => (1, 1, 0),
            Self::Waiting => (0, 1, 0),
            Self::Unsupported => (0, 0, 1),
            Self::Unreachable => (0, 0, 0),
        }
    }
}

#[derive(Clone, Copy, Default)]
struct SetVotes {
    admitted: usize,
    reachable: usize,
    unsupported: usize,
}

impl SetVotes {
    fn state(self, set: &ReadmissionSet) -> SetState {
        if self.admitted >= set.required_peers {
            SetState::Admitted
        } else if set.peers.len() - self.unsupported < set.required_peers {
            SetState::Unattainable
        } else if self.reachable < set.required_peers {
            SetState::NeedsUnreachablePeer
        } else {
            SetState::Waiting
        }
    }

    fn replace(&mut self, before: PeerVote, after: PeerVote) {
        let (admitted_before, reachable_before, unsupported_before) = before.counts();
        let (admitted_after, reachable_after, unsupported_after) = after.counts();
        self.admitted = self.admitted - admitted_before + admitted_after;
        self.reachable = self.reachable - reachable_before + reachable_after;
        self.unsupported = self.unsupported - unsupported_before + unsupported_after;
    }
}

/// A compiled membership index and compact votes for one probe round.
/// Reply payloads are consumed once and discarded; only affected sets change.
struct ReadmissionRound<'a> {
    plan: &'a ReadmissionPlan,
    memberships: HashMap<&'a str, Vec<(usize, PeerVote)>>,
    sets: Vec<SetVotes>,
    progress: ReadmissionProgress,
}

impl<'a> ReadmissionRound<'a> {
    fn new(plan: &'a ReadmissionPlan) -> Self {
        let mut memberships: HashMap<&str, Vec<(usize, PeerVote)>> = HashMap::new();
        for (index, set) in plan.sets.iter().enumerate() {
            for host in &set.peers {
                memberships
                    .entry(host.as_str())
                    .or_default()
                    .push((index, PeerVote::Unreachable));
            }
        }
        Self {
            plan,
            memberships,
            sets: vec![SetVotes::default(); plan.sets.len()],
            progress: ReadmissionProgress::unobserved(plan.sets.len()),
        }
    }

    fn reset(&mut self) {
        for edges in self.memberships.values_mut() {
            for (_, vote) in edges {
                *vote = PeerVote::Unreachable;
            }
        }
        self.sets.fill(SetVotes::default());
        self.progress = ReadmissionProgress::unobserved(self.plan.sets.len());
    }

    fn update_peer(&mut self, host: &str, mut classify: impl FnMut(&ReadmissionSet) -> PeerVote) {
        let Some(edges) = self.memberships.get_mut(host) else { return };
        for (index, vote) in edges {
            // Indices and previous votes come only from the configured plan.
            let set = &self.plan.sets[*index];
            let counts = &mut self.sets[*index];
            let before = counts.state(set);
            let next = classify(set);
            counts.replace(*vote, next);
            *vote = next;
            self.progress.replace(before, counts.state(set));
        }
    }

    fn observe_drives(&mut self, host: &str, drives: impl IntoIterator<Item = PeerDriveView>) {
        if !self.memberships.contains_key(host) {
            return;
        }
        let online = drives
            .into_iter()
            .filter(|drive| drive.online)
            .map(|drive| (drive.pool_index, drive.set_index, drive.disk_index))
            .collect::<HashSet<_>>();
        self.update_peer(host, |set| {
            if set
                .local_disks
                .iter()
                .all(|disk| online.contains(&(set.pool_index, set.set_index, *disk)))
            {
                PeerVote::Admitted
            } else {
                PeerVote::Waiting
            }
        });
    }

    fn observe(&mut self, host: &str, probe: PeerProbe) {
        match probe {
            PeerProbe::Answered(drives) => self.observe_drives(host, drives),
            PeerProbe::Unsupported => self.update_peer(host, |_| PeerVote::Unsupported),
            PeerProbe::Unreachable => self.update_peer(host, |_| PeerVote::Unreachable),
        }
    }
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

/// Poll current-round peer replies until every set is admitted or a bound expires.
///
/// The owner enforces the shared startup deadline and continuous unreachable
/// grace while polling RPCs and sleeping. Pending read-only RPCs are cancelled
/// once all sets have quorum, an unsupported fleet cannot supply it, or a bound
/// expires. Votes are never carried between rounds.
pub(super) async fn wait_for_peer_readmission_with<P, S>(
    plan: &ReadmissionPlan,
    deadline: tokio::time::Instant,
    poll_interval: Duration,
    unreachable_grace: Duration,
    mut probe: P,
) -> ReadmissionOutcome
where
    P: FnMut() -> S,
    S: Stream<Item = (String, PeerProbe)>,
{
    let started = tokio::time::Instant::now();
    let mut unreachable_since: Option<tokio::time::Instant> = None;
    let mut last_logged: Option<ReadmissionProgress> = None;
    let mut votes = ReadmissionRound::new(plan);
    let mut progress = votes.progress;
    let give_up = |reason, progress| ReadmissionOutcome::GaveUp {
        reason,
        waited: tokio::time::Instant::now().saturating_duration_since(started),
        progress,
    };
    let wait_bound = |unreachable_since: Option<tokio::time::Instant>| {
        if let Some(since) = unreachable_since {
            let grace_deadline = since + unreachable_grace;
            if grace_deadline <= deadline {
                return (grace_deadline, GiveUpReason::PeersUnreachable);
            }
        }
        (deadline, GiveUpReason::DeadlineElapsed)
    };

    loop {
        let (bound, reason) = wait_bound(unreachable_since);
        if tokio::time::Instant::now() >= bound {
            return give_up(reason, progress);
        }
        votes.reset();
        let mut round = std::pin::pin!(probe());
        loop {
            let (bound, reason) = wait_bound(unreachable_since);
            let next = tokio::select! {
                biased;
                _ = tokio::time::sleep_until(bound) => return give_up(reason, progress),
                next = round.next() => next,
            };
            let complete = if let Some((host, reply)) = next {
                votes.observe(&host, reply);
                false
            } else {
                true
            };
            progress = votes.progress;
            // A reply poll and its synchronous fold can consume the remaining
            // budget after select polled its timer. Do not accept late work or
            // clear the grace deadline that bounded this observation.
            if tokio::time::Instant::now() >= bound {
                return give_up(reason, progress);
            }
            if progress.admitted_sets == progress.total_sets {
                return ReadmissionOutcome::Admitted {
                    waited: tokio::time::Instant::now().saturating_duration_since(started),
                };
            }
            if progress.unattainable_sets > 0 {
                return give_up(GiveUpReason::PeersCannotReport, progress);
            }
            if progress.unreachable_sets == 0 {
                // Enough reachable replies disprove a previous shortfall even
                // while another, unnecessary peer is still answering.
                unreachable_since = None;
            } else if complete {
                // Pending replies are potential votes, not evidence that their
                // hosts are unreachable. Only a finished round starts grace.
                unreachable_since.get_or_insert_with(tokio::time::Instant::now);
            }
            if complete {
                break;
            }
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
        let (bound, reason) = wait_bound(unreachable_since);
        tokio::select! {
            biased;
            _ = tokio::time::sleep_until(bound) => return give_up(reason, progress),
            _ = tokio::time::sleep(poll_interval) => {}
        }
    }
}

fn probe_peers<'a>(
    clients: &'a [(String, PeerRestClient)],
    local_host: &'a str,
    timeout: Duration,
) -> impl Stream<Item = (String, PeerProbe)> + 'a {
    clients
        .iter()
        .map(|(host, client)| async move {
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
        })
        .collect::<FuturesUnordered<_>>()
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
    use std::future::Future;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn probe_round<F>(round: F) -> impl Stream<Item = (String, PeerProbe)>
    where
        F: Future<Output = HashMap<String, PeerProbe>>,
    {
        futures::stream::once(round).flat_map(futures::stream::iter)
    }

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
        let state = |entries: &[(&str, PeerProbe)]| {
            let mut round = ReadmissionRound::new(&plan);
            for (host, probe) in entries {
                round.observe(&format!("{host}:9000"), probe.clone());
            }
            round.sets[0].state(set)
        };

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
            probe_round(async move {
                // node-1 keeps dialing the previous pod IP for five polls.
                probes(&[("node-0", answered(true)), ("node-1", answered(poll >= 5))])
            })
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
        let outcome = wait_for_peer_readmission_with(&plan, deadline, Duration::from_secs(1), UNREACHABLE_PEER_GRACE, || {
            probe_round(async { probes(&[("node-0", answered(true)), ("node-1", answered(false))]) })
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
        let outcome = wait_for_peer_readmission_with(&plan, deadline, Duration::from_secs(1), UNREACHABLE_PEER_GRACE, || {
            probe_round(async { probes(&[("node-0", answered(true)), ("node-1", PeerProbe::Unreachable)]) })
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
            probe_round(async move {
                let node_1 = match poll {
                    0..10 => PeerProbe::Unreachable,
                    10..30 => answered(false),
                    _ => answered(true),
                };
                probes(&[("node-0", answered(true)), ("node-1", node_1)])
            })
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
        let outcome = wait_for_peer_readmission_with(&plan, deadline, Duration::from_secs(1), UNREACHABLE_PEER_GRACE, || {
            probe_round(async { probes(&[("node-0", PeerProbe::Unsupported), ("node-1", PeerProbe::Unsupported)]) })
        })
        .await;

        let ReadmissionOutcome::GaveUp { reason, waited, .. } = outcome else {
            panic!("expected a rolling upgrade from older peers to skip the wait, got {outcome:?}");
        };
        assert_eq!(reason, GiveUpReason::PeersCannotReport);
        assert_eq!(waited, Duration::ZERO);
    }

    struct ProbeDrop(Arc<AtomicUsize>);

    impl Drop for ProbeDrop {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn gate_expired_budget_does_not_start_a_probe() {
        let plan = three_node_plan();
        let polls = AtomicUsize::new(0);
        let outcome = wait_for_peer_readmission_with(
            &plan,
            tokio::time::Instant::now(),
            Duration::from_secs(1),
            UNREACHABLE_PEER_GRACE,
            || {
                polls.fetch_add(1, Ordering::SeqCst);
                probe_round(async { probes(&[("node-0", answered(true)), ("node-1", answered(false))]) })
            },
        )
        .await;
        assert_eq!(polls.load(Ordering::SeqCst), 0, "an exhausted shared startup budget must start no RPCs");
        assert!(matches!(
            outcome,
            ReadmissionOutcome::GaveUp {
                reason: GiveUpReason::DeadlineElapsed,
                waited: Duration::ZERO,
                progress: ReadmissionProgress { total_sets: 1, .. },
            }
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn gate_deadline_cancels_a_suspended_round() {
        let plan = three_node_plan();
        let dropped = Arc::new(AtomicUsize::new(0));
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        let gate = wait_for_peer_readmission_with(&plan, deadline, Duration::from_secs(1), UNREACHABLE_PEER_GRACE, || {
            let guard = ProbeDrop(Arc::clone(&dropped));
            probe_round(async move {
                let _guard = guard;
                std::future::pending::<HashMap<String, PeerProbe>>().await
            })
        });
        let outcome = tokio::time::timeout(Duration::from_secs(3), gate)
            .await
            .expect("startup deadline must interrupt a suspended round");
        assert!(matches!(outcome, ReadmissionOutcome::GaveUp {
            reason: GiveUpReason::DeadlineElapsed,
            waited,
            progress: ReadmissionProgress { total_sets: 1, .. },
        } if waited == Duration::from_secs(2)));
        assert_eq!(dropped.load(Ordering::SeqCst), 1, "deadline cancels the owned round");
    }

    #[tokio::test(start_paused = true)]
    async fn gate_unreachable_grace_cancels_a_suspended_round() {
        let plan = three_node_plan();
        let polls = AtomicUsize::new(0);
        let dropped = Arc::new(AtomicUsize::new(0));
        let deadline = tokio::time::Instant::now() + Duration::from_secs(120);
        let gate = wait_for_peer_readmission_with(&plan, deadline, Duration::from_secs(1), UNREACHABLE_PEER_GRACE, || {
            let first = polls.fetch_add(1, Ordering::SeqCst) == 0;
            let guard = ProbeDrop(Arc::clone(&dropped));
            probe_round(async move {
                let _guard = guard;
                if first {
                    probes(&[("node-0", answered(true)), ("node-1", PeerProbe::Unreachable)])
                } else {
                    std::future::pending::<HashMap<String, PeerProbe>>().await
                }
            })
        });
        let outcome = tokio::time::timeout(UNREACHABLE_PEER_GRACE + Duration::from_secs(1), gate)
            .await
            .expect("unreachable grace must interrupt the next suspended round");
        assert!(matches!(outcome, ReadmissionOutcome::GaveUp {
            reason: GiveUpReason::PeersUnreachable,
            waited,
            ..
        } if waited == UNREACHABLE_PEER_GRACE));
        assert_eq!(dropped.load(Ordering::SeqCst), 2, "both completed and suspended rounds release ownership");
    }

    fn five_node_plan() -> ReadmissionPlan {
        readmission_plan(&pools(1, 5, (0..5).map(|n| endpoint(&format!("node-{n}"), n == 2, 0, n)).collect()))
            .expect("five-node plan")
    }

    fn peer_reply(host: &str, reply: PeerProbe, delay: Duration) -> futures::future::BoxFuture<'static, (String, PeerProbe)> {
        use futures::FutureExt;
        let host = format!("{host}:9000");
        async move {
            if !delay.is_zero() {
                tokio::time::sleep(delay).await;
            }
            (host, reply)
        }
        .boxed()
    }

    fn suspended_peer(host: &str, dropped: Arc<AtomicUsize>) -> futures::future::BoxFuture<'static, (String, PeerProbe)> {
        use futures::FutureExt;
        let host = format!("{host}:9000");
        let guard = ProbeDrop(dropped);
        async move {
            let _guard = guard;
            let reply = std::future::pending::<PeerProbe>().await;
            (host, reply)
        }
        .boxed()
    }

    #[tokio::test(start_paused = true)]
    async fn gate_five_node_quorum_does_not_wait_for_a_surplus_peer() {
        let plan = five_node_plan();
        assert_eq!(plan.sets[0].required_peers, 3);
        let dropped = Arc::new(AtomicUsize::new(0));
        let outcome = wait_for_peer_readmission_with(
            &plan,
            tokio::time::Instant::now() + Duration::from_secs(30),
            Duration::from_secs(1),
            UNREACHABLE_PEER_GRACE,
            || {
                vec![
                    suspended_peer("node-4", Arc::clone(&dropped)),
                    peer_reply("node-0", answered(true), Duration::ZERO),
                    peer_reply("node-1", answered(true), Duration::ZERO),
                    peer_reply("node-3", answered(true), Duration::ZERO),
                ]
                .into_iter()
                .collect::<FuturesUnordered<_>>()
            },
        )
        .await;
        assert_eq!(outcome, ReadmissionOutcome::Admitted { waited: Duration::ZERO });
        assert_eq!(dropped.load(Ordering::SeqCst), 1, "surplus RPC is cancelled without a detached task");
    }

    #[tokio::test(start_paused = true)]
    async fn gate_three_node_quorum_waits_for_both_peers() {
        let plan = three_node_plan();
        let outcome = wait_for_peer_readmission_with(
            &plan,
            tokio::time::Instant::now() + Duration::from_secs(30),
            Duration::from_secs(1),
            UNREACHABLE_PEER_GRACE,
            || {
                vec![
                    peer_reply("node-0", answered(true), Duration::ZERO),
                    peer_reply("node-1", answered(true), Duration::from_secs(2)),
                ]
                .into_iter()
                .collect::<FuturesUnordered<_>>()
            },
        )
        .await;
        assert_eq!(
            outcome,
            ReadmissionOutcome::Admitted {
                waited: Duration::from_secs(2)
            }
        );
    }

    #[tokio::test(start_paused = true)]
    async fn gate_waits_for_every_shared_set() {
        let plan = readmission_plan(&pools(
            2,
            3,
            vec![
                endpoint("node-0", false, 0, 0),
                endpoint("node-1", false, 0, 1),
                endpoint("node-2", true, 0, 2),
                endpoint("node-3", false, 1, 0),
                endpoint("node-2", true, 1, 1),
                endpoint("node-4", false, 1, 2),
            ],
        ))
        .expect("two shared sets");
        let second_set = || {
            PeerProbe::Answered(vec![PeerDriveView {
                pool_index: 0,
                set_index: 1,
                disk_index: 1,
                online: true,
            }])
        };
        // These peers belong only to set 0; matching foreign drive IDs must
        // never let their replies supply votes for set 1.
        let first_set_with_extra_views = || {
            PeerProbe::Answered(vec![
                PeerDriveView {
                    pool_index: 0,
                    set_index: 0,
                    disk_index: 2,
                    online: true,
                },
                PeerDriveView {
                    pool_index: 0,
                    set_index: 1,
                    disk_index: 1,
                    online: true,
                },
            ])
        };
        let outcome = wait_for_peer_readmission_with(
            &plan,
            tokio::time::Instant::now() + Duration::from_secs(30),
            Duration::from_secs(1),
            UNREACHABLE_PEER_GRACE,
            || {
                vec![
                    peer_reply("node-0", first_set_with_extra_views(), Duration::ZERO),
                    peer_reply("node-1", first_set_with_extra_views(), Duration::ZERO),
                    peer_reply("node-3", second_set(), Duration::ZERO),
                    peer_reply("node-4", second_set(), Duration::from_secs(2)),
                ]
                .into_iter()
                .collect::<FuturesUnordered<_>>()
            },
        )
        .await;
        assert_eq!(
            outcome,
            ReadmissionOutcome::Admitted {
                waited: Duration::from_secs(2)
            }
        );
    }

    #[tokio::test(start_paused = true)]
    async fn gate_partial_reachability_resets_grace_before_round_completion() {
        let plan = five_node_plan();
        let polls = AtomicUsize::new(0);
        let outcome = wait_for_peer_readmission_with(
            &plan,
            tokio::time::Instant::now() + Duration::from_secs(60),
            Duration::from_secs(1),
            UNREACHABLE_PEER_GRACE,
            || {
                let poll = polls.fetch_add(1, Ordering::SeqCst);
                let recovering = poll == 1;
                vec![
                    peer_reply("node-0", answered(true), Duration::ZERO),
                    peer_reply("node-1", answered(true), Duration::ZERO),
                    peer_reply(
                        "node-3",
                        if recovering { answered(false) } else { PeerProbe::Unreachable },
                        Duration::ZERO,
                    ),
                    peer_reply(
                        "node-4",
                        if recovering { answered(false) } else { PeerProbe::Unreachable },
                        if recovering { Duration::from_secs(20) } else { Duration::ZERO },
                    ),
                ]
                .into_iter()
                .collect::<FuturesUnordered<_>>()
            },
        )
        .await;
        assert!(
            matches!(outcome, ReadmissionOutcome::GaveUp {
            reason: GiveUpReason::PeersUnreachable, waited, ..
        } if waited == Duration::from_secs(37)),
            "old grace must reset at 1s; a new shortfall starts at 22s: {outcome:?}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn gate_never_combines_votes_from_different_rounds() {
        let plan = five_node_plan();
        let polls = AtomicUsize::new(0);
        let outcome = wait_for_peer_readmission_with(
            &plan,
            tokio::time::Instant::now() + Duration::from_secs(4),
            Duration::from_secs(1),
            UNREACHABLE_PEER_GRACE,
            || {
                let entries = if polls.fetch_add(1, Ordering::SeqCst) == 0 {
                    probes(&[
                        ("node-0", answered(true)),
                        ("node-1", answered(true)),
                        ("node-3", answered(false)),
                    ])
                } else {
                    probes(&[("node-3", answered(true))])
                };
                futures::stream::iter(entries)
            },
        )
        .await;
        assert!(
            matches!(outcome, ReadmissionOutcome::GaveUp {
            reason: GiveUpReason::DeadlineElapsed, waited, ..
        } if waited == Duration::from_secs(4)),
            "stale affirmative votes must not admit the node: {outcome:?}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn gate_unsupported_quorum_cancels_other_pending_peers() {
        let plan = three_node_plan();
        let dropped = Arc::new(AtomicUsize::new(0));
        let outcome = wait_for_peer_readmission_with(
            &plan,
            tokio::time::Instant::now() + Duration::from_secs(30),
            Duration::from_secs(1),
            UNREACHABLE_PEER_GRACE,
            || {
                vec![
                    suspended_peer("node-1", Arc::clone(&dropped)),
                    peer_reply("node-0", PeerProbe::Unsupported, Duration::ZERO),
                ]
                .into_iter()
                .collect::<FuturesUnordered<_>>()
            },
        )
        .await;
        assert!(matches!(
            outcome,
            ReadmissionOutcome::GaveUp {
                reason: GiveUpReason::PeersCannotReport,
                waited: Duration::ZERO,
                ..
            }
        ));
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn gate_deadline_wins_over_a_simultaneous_quorum_reply() {
        let plan = three_node_plan();
        let outcome = wait_for_peer_readmission_with(
            &plan,
            tokio::time::Instant::now() + Duration::from_secs(2),
            Duration::from_secs(1),
            UNREACHABLE_PEER_GRACE,
            || {
                vec![
                    peer_reply("node-0", answered(true), Duration::ZERO),
                    peer_reply("node-1", answered(true), Duration::from_secs(2)),
                ]
                .into_iter()
                .collect::<FuturesUnordered<_>>()
            },
        )
        .await;
        assert!(matches!(outcome, ReadmissionOutcome::GaveUp {
            reason: GiveUpReason::DeadlineElapsed, waited, ..
        } if waited == Duration::from_secs(2)));
    }

    #[test]
    fn round_replaces_votes_ignores_unknown_hosts_and_resets() {
        let plan = three_node_plan();
        let mut round = ReadmissionRound::new(&plan);
        let unreachable = ReadmissionProgress {
            total_sets: 1,
            unreachable_sets: 1,
            ..Default::default()
        };
        assert_eq!(round.progress, unreachable);
        round.observe("unknown:9000", answered(true));
        assert_eq!(round.progress, unreachable);
        round.observe("node-0:9000", answered(false));
        round.observe("node-0:9000", answered(true));
        assert_eq!(round.progress, unreachable, "one peer's replacements cannot create another vote");
        round.observe("node-1:9000", answered(false));
        assert_eq!(
            round.progress,
            ReadmissionProgress {
                total_sets: 1,
                waiting_sets: 1,
                ..Default::default()
            }
        );
        round.observe("node-1:9000", PeerProbe::Unsupported);
        assert_eq!(
            round.progress,
            ReadmissionProgress {
                total_sets: 1,
                unattainable_sets: 1,
                ..Default::default()
            }
        );
        round.observe("node-1:9000", answered(true));
        assert_eq!(
            round.progress,
            ReadmissionProgress {
                total_sets: 1,
                admitted_sets: 1,
                ..Default::default()
            }
        );
        round.observe("node-0:9000", PeerProbe::Unreachable);
        assert_eq!(round.progress, unreachable);
        round.reset();
        assert_eq!(round.progress, unreachable);
        round.observe("node-1:9000", answered(true));
        assert_eq!(round.progress, unreachable, "a new round cannot keep the prior node-0 vote");
    }

    #[test]
    fn round_matches_all_local_slots_without_duplicate_or_cross_pool_votes() {
        let mut plan = readmission_plan(&pools(
            1,
            4,
            vec![
                endpoint("node-0", false, 0, 0),
                endpoint("node-1", false, 0, 1),
                endpoint("node-2", true, 0, 2),
                endpoint("node-2", true, 0, 3),
            ],
        ))
        .expect("two local slots");
        let mut second = plan.sets[0].clone();
        second.pool_index = 1;
        plan.sets.push(second);
        let mut round = ReadmissionRound::new(&plan);
        let drive = |pool_index, disk_index, online| PeerDriveView {
            pool_index,
            set_index: 0,
            disk_index,
            online,
        };
        round.observe_drives(
            "node-0:9000",
            vec![
                drive(0, 2, true),
                drive(0, 2, false),
                drive(0, 2, true),
                drive(0, 3, false),
                drive(9, 3, true),
            ],
        );
        round.observe_drives(
            "node-1:9000",
            vec![drive(0, 2, true), drive(0, 3, true), drive(1, 2, true), drive(1, 3, true)],
        );
        assert_eq!(
            round.progress,
            ReadmissionProgress {
                total_sets: 2,
                waiting_sets: 2,
                ..Default::default()
            },
            "duplicate and foreign slots cannot replace missing local slots"
        );
        round.observe_drives("node-0:9000", vec![drive(0, 2, false), drive(0, 2, true), drive(0, 3, true)]);
        assert_eq!(
            round.progress,
            ReadmissionProgress {
                total_sets: 2,
                admitted_sets: 1,
                waiting_sets: 1,
                ..Default::default()
            },
            "any online duplicate is valid, but only in its own pool and set"
        );
        round.observe_drives(
            "node-0:9000",
            vec![drive(0, 2, true), drive(0, 3, true), drive(1, 2, true), drive(1, 3, true)],
        );
        assert_eq!(
            round.progress,
            ReadmissionProgress {
                total_sets: 2,
                admitted_sets: 2,
                ..Default::default()
            }
        );
    }

    #[test]
    fn round_visits_each_reply_drive_once() {
        use std::cell::Cell;
        let endpoints = (0..64)
            .flat_map(|set_index| {
                (0..16).map(move |disk_index| {
                    let mut endpoint = endpoint(&format!("node-{disk_index}"), disk_index == 0, set_index, disk_index);
                    endpoint.url.set_path(&format!("/set-{set_index}/disk-{disk_index}"));
                    endpoint
                })
            })
            .collect();
        let plan = readmission_plan(&pools(64, 16, endpoints)).expect("valid 64-set topology");
        let mut round = ReadmissionRound::new(&plan);
        let visited = Cell::new(0);
        for peer in 1..16 {
            let drives = (0..64).map(|set_index| {
                visited.set(visited.get() + 1);
                PeerDriveView {
                    pool_index: 0,
                    set_index,
                    disk_index: 0,
                    online: false,
                }
            });
            round.observe_drives(&format!("node-{peer}:9000"), drives);
        }
        assert_eq!(visited.get(), 15 * 64, "payload consumption is linear in received entries");
        assert_eq!(
            round.progress,
            ReadmissionProgress {
                total_sets: 64,
                waiting_sets: 64,
                ..Default::default()
            }
        );
        round.observe_drives(
            "unknown:9000",
            (0..64).map(|_| panic!("a host outside the topology must not have its payload inspected")),
        );
        assert_eq!(round.progress.waiting_sets, 64);
        round.reset();
        assert_eq!(
            round.progress,
            ReadmissionProgress {
                total_sets: 64,
                unreachable_sets: 64,
                ..Default::default()
            }
        );
    }

    fn round_with_slow_final_poll(reply: PeerProbe) -> impl Stream<Item = (String, PeerProbe)> {
        use std::task::Poll;
        let mut item = 0;
        futures::stream::poll_fn(move |cx| {
            item += 1;
            match item {
                1 => Poll::Ready(Some(("node-0:9000".to_string(), answered(true)))),
                2 => {
                    // Model decoding/processing inside poll without yielding:
                    // the select's timer branch was already polled this turn.
                    let mut work = std::pin::pin!(tokio::time::advance(Duration::from_secs(2)));
                    assert!(work.as_mut().poll(cx).is_pending());
                    Poll::Ready(Some(("node-1:9000".to_string(), reply.clone())))
                }
                _ => Poll::Ready(None),
            }
        })
    }

    #[tokio::test(start_paused = true)]
    async fn gate_rejects_quorum_when_reply_poll_consumes_the_budget() {
        let plan = three_node_plan();
        let outcome = wait_for_peer_readmission_with(
            &plan,
            tokio::time::Instant::now() + Duration::from_secs(2),
            Duration::from_secs(1),
            UNREACHABLE_PEER_GRACE,
            || round_with_slow_final_poll(answered(true)),
        )
        .await;
        assert!(
            matches!(outcome, ReadmissionOutcome::GaveUp {
            reason: GiveUpReason::DeadlineElapsed, waited, ..
        } if waited == Duration::from_secs(2)),
            "a late poll result cannot publish admission: {outcome:?}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn gate_rejects_grace_reset_when_reply_poll_consumes_the_bound() {
        let plan = three_node_plan();
        let rounds = AtomicUsize::new(0);
        let gate = wait_for_peer_readmission_with(
            &plan,
            tokio::time::Instant::now() + Duration::from_secs(120),
            Duration::from_secs(1),
            Duration::from_secs(3),
            || -> futures::stream::BoxStream<'static, (String, PeerProbe)> {
                if rounds.fetch_add(1, Ordering::SeqCst) == 0 {
                    futures::stream::iter(probes(&[("node-0", answered(true)), ("node-1", PeerProbe::Unreachable)])).boxed()
                } else {
                    round_with_slow_final_poll(answered(false)).boxed()
                }
            },
        );
        let outcome = tokio::time::timeout(Duration::from_secs(5), gate)
            .await
            .expect("a reply finishing at the grace bound cannot clear it");
        assert!(
            matches!(outcome, ReadmissionOutcome::GaveUp {
            reason: GiveUpReason::PeersUnreachable, waited, ..
        } if waited == Duration::from_secs(3)),
            "captured grace bound must win: {outcome:?}"
        );
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
