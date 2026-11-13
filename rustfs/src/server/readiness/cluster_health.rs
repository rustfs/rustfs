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

use super::{
    ClusterHealthProbeKind, DependencyReadiness, DependencyReadinessReport, Endpoint, EndpointServerPools, StorageAdminApi,
    StorageWriteReadinessStatus, collect_peer_health_readiness, configured_readiness_topology,
    dependency_readiness_report_from_write_status, disk_is_online_for_readiness, exclusive_lock_quorum, is_dist_erasure,
    pool_metadata_write_readiness, pool_read_quorum, pool_write_quorum, runtime_sources,
};
use futures::{FutureExt, StreamExt, future::BoxFuture, stream::FuturesUnordered};
use rustfs_madmin::{BackendInfo, Disk, StorageInfo};
use std::collections::{HashMap, HashSet};

type DiskSlot = (i32, i32, i32);
type StorageProbes = FuturesUnordered<BoxFuture<'static, StorageObservation>>;
type LockProbes = FuturesUnordered<BoxFuture<'static, (String, bool)>>;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum ObservationMember {
    Local,
    Peer(String),
}

struct StorageObservation {
    member: ObservationMember,
    info: Option<StorageInfo>,
}

struct ConfiguredDiskSlot {
    endpoint: String,
    member: ObservationMember,
    set_index: usize,
    observed: bool,
}

struct StorageQuorum {
    slots: HashMap<DiskSlot, ConfiguredDiskSlot>,
    votes_needed: Vec<usize>,
    unsatisfied_sets: usize,
}

/// Observe fresh storage and lock replies until every configured set has quorum.
/// The caller owns the end-to-end deadline. Dropping either collection drops
/// its remaining probe futures. Transport cancellation retains the existing
/// bounded RPC cleanup. Observations never reuse the administrative caches.
pub(super) async fn collect_cluster_probe_report(kind: ClusterHealthProbeKind) -> DependencyReadinessReport {
    let iam_ready = runtime_sources::current_iam_ready();
    let peer_health_ready = collect_peer_health_readiness();
    let Some(store) = runtime_sources::current_object_store_handle() else {
        return dependency_readiness_report_from_write_status(
            DependencyReadiness {
                iam_ready,
                peer_health_ready,
                ..Default::default()
            },
            StorageWriteReadinessStatus::default(),
        );
    };
    let metadata = match kind {
        ClusterHealthProbeKind::Write => pool_metadata_write_readiness(store.pool_meta_write_status().await),
        ClusterHealthProbeKind::Read => StorageWriteReadinessStatus {
            ready: true,
            pool_metadata_reason: None,
        },
    };
    let Some(endpoints) = runtime_sources::current_endpoints_handle() else {
        return dependency_readiness_report_from_write_status(
            DependencyReadiness {
                iam_ready,
                peer_health_ready,
                ..Default::default()
            },
            metadata,
        );
    };
    let backend = StorageAdminApi::backend_info(store.as_ref()).await;
    let storage_probes = StorageProbes::new();
    storage_probes.push(
        async move {
            StorageObservation {
                member: ObservationMember::Local,
                info: Some(StorageAdminApi::local_storage_info_with_metrics(store.as_ref(), false).await),
            }
        }
        .boxed(),
    );
    if let Some(notification) = crate::runtime_sources::current_notification_system() {
        let mut members = HashSet::new();
        for peer in notification.peer_clients.iter().flatten() {
            let member = peer.topology_member().to_string();
            if !members.insert(member.clone()) {
                continue;
            }
            let peer = peer.clone();
            storage_probes.push(
                async move {
                    StorageObservation {
                        member: ObservationMember::Peer(member),
                        info: peer.local_storage_info_with_metrics(false).await.ok(),
                    }
                }
                .boxed(),
            );
        }
    }

    let (storage_ready, lock_quorum_ready) = tokio::join!(
        async { metadata.ready && collect_storage_quorum(kind, backend, &endpoints, storage_probes).await },
        collect_lock_quorum(&endpoints),
    );
    let readiness = DependencyReadiness {
        storage_ready: metadata.ready && storage_ready,
        iam_ready,
        lock_quorum_ready,
        peer_health_ready,
    };
    dependency_readiness_report_from_write_status(readiness, metadata)
}

fn configured_storage_quorum(
    kind: ClusterHealthProbeKind,
    backend: BackendInfo,
    endpoints: &EndpointServerPools,
) -> Option<StorageQuorum> {
    let shape = StorageInfo {
        backend,
        ..Default::default()
    };
    let (total_sets, drives_per_set) = configured_readiness_topology(&shape)?;
    if endpoints.as_ref().len() != total_sets.len() {
        return None;
    }

    let mut slots = HashMap::new();
    let mut identities = HashSet::new();
    let mut votes_needed = Vec::new();
    for (pool_idx, pool) in endpoints.as_ref().iter().enumerate() {
        let set_count = total_sets[pool_idx];
        let drive_count = drives_per_set[pool_idx];
        if pool.set_count != set_count
            || pool.drives_per_set != drive_count
            || pool.endpoints.as_ref().len() != set_count.checked_mul(drive_count)?
        {
            return None;
        }
        let quorum = match kind {
            ClusterHealthProbeKind::Write => pool_write_quorum(&shape, pool_idx, drive_count),
            ClusterHealthProbeKind::Read => pool_read_quorum(&shape, pool_idx, drive_count),
        }?;
        let set_offset = votes_needed.len();
        votes_needed.resize(set_offset.checked_add(set_count)?, quorum);
        for endpoint in pool.endpoints.as_ref() {
            let set_idx = usize::try_from(endpoint.set_idx).ok()?;
            let host = endpoint.host_port();
            let member = if endpoint.is_local {
                ObservationMember::Local
            } else {
                ObservationMember::Peer(host.clone())
            };
            if usize::try_from(endpoint.pool_idx).ok() != Some(pool_idx)
                || set_idx >= set_count
                || usize::try_from(endpoint.disk_idx)
                    .ok()
                    .is_none_or(|index| index >= drive_count)
                || !identities.insert((host, endpoint.get_file_path()))
                || slots
                    .insert(
                        (endpoint.pool_idx, endpoint.set_idx, endpoint.disk_idx),
                        ConfiguredDiskSlot {
                            endpoint: endpoint.to_string(),
                            member,
                            set_index: set_offset + set_idx,
                            observed: false,
                        },
                    )
                    .is_some()
            {
                return None;
            }
        }
    }
    Some(StorageQuorum {
        unsatisfied_sets: votes_needed.len(),
        votes_needed,
        slots,
    })
}

fn disk_slot(disk: &Disk) -> DiskSlot {
    (disk.pool_index, disk.set_index, disk.disk_index)
}

fn accept_storage_observation(observation: StorageObservation, quorum: &mut StorageQuorum) {
    let Some(info) = observation.info else {
        return;
    };
    let mut reply_slots = HashSet::new();
    if !info.disks.iter().all(|disk| reply_slots.insert(disk_slot(disk))) {
        return;
    }
    for disk in info.disks {
        let slot = disk_slot(&disk);
        let Some(configured) = quorum.slots.get_mut(&slot) else {
            continue;
        };
        // The configured endpoint carries the disk identity and path. The
        // reported mount path can differ inside a peer's container namespace.
        if configured.observed || configured.member != observation.member || disk.endpoint != configured.endpoint {
            continue;
        }
        configured.observed = true;
        if disk_is_online_for_readiness(&disk) {
            count_quorum_vote(&mut quorum.votes_needed, &mut quorum.unsatisfied_sets, configured.set_index);
        }
    }
}

fn count_quorum_vote(votes_needed: &mut [usize], unsatisfied_sets: &mut usize, set_index: usize) {
    let needed = &mut votes_needed[set_index];
    if *needed > 0 {
        *needed -= 1;
        if *needed == 0 {
            *unsatisfied_sets -= 1;
        }
    }
}

async fn collect_storage_quorum(
    kind: ClusterHealthProbeKind,
    backend: BackendInfo,
    endpoints: &EndpointServerPools,
    mut probes: StorageProbes,
) -> bool {
    let Some(mut quorum) = configured_storage_quorum(kind, backend, endpoints) else {
        return false;
    };
    while let Some(observation) = probes.next().await {
        accept_storage_observation(observation, &mut quorum);
        if quorum.unsatisfied_sets == 0 {
            return true;
        }
    }
    false
}

async fn collect_lock_quorum(endpoints: &EndpointServerPools) -> bool {
    if !is_dist_erasure().await {
        return true;
    }
    let Some(clients) = runtime_sources::current_lock_clients_handle() else {
        return false;
    };
    let probes: LockProbes = clients
        .into_iter()
        .map(|(host, client)| async move { (host, client.is_online().await) }.boxed())
        .collect();
    collect_lock_quorum_with(endpoints, probes).await
}

async fn collect_lock_quorum_with(endpoints: &EndpointServerPools, mut probes: LockProbes) -> bool {
    let mut host_sets: HashMap<String, Vec<usize>> = HashMap::new();
    let mut votes_needed = Vec::new();
    for pool in endpoints.as_ref() {
        for set_idx in 0..pool.set_count {
            let hosts = pool
                .endpoints
                .as_ref()
                .iter()
                .filter(|endpoint| usize::try_from(endpoint.set_idx).ok() == Some(set_idx))
                .map(Endpoint::host_port)
                .filter(|host| !host.is_empty())
                .collect::<HashSet<_>>();
            if hosts.is_empty() {
                return false;
            }
            let set_index = votes_needed.len();
            // Cluster read health retains its existing exclusive-lock threshold.
            votes_needed.push(exclusive_lock_quorum(hosts.len()));
            for host in hosts {
                host_sets.entry(host).or_default().push(set_index);
            }
        }
    }
    let mut unsatisfied_sets = votes_needed.len();
    if unsatisfied_sets == 0 {
        return false;
    }
    while let Some((host, online)) = probes.next().await {
        if !online {
            continue;
        }
        let Some(sets) = host_sets.remove(&host) else {
            continue;
        };
        for set_index in sets {
            count_quorum_vote(&mut votes_needed, &mut unsatisfied_sets, set_index);
        }
        if unsatisfied_sets == 0 {
            return true;
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::super::{aggregate_lock_quorum_status, storage_read_ready_from_runtime_state, storage_ready_from_runtime_state};
    use super::*;
    use crate::storage_api::server::readiness::{Endpoints, PoolEndpoints};
    use std::future;
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };

    fn topology(layouts: &[(usize, usize, usize)]) -> (BackendInfo, EndpointServerPools) {
        let mut backend = BackendInfo::default();
        let mut pools = Vec::new();
        for (pool_idx, &(set_count, drive_count, parity)) in layouts.iter().enumerate() {
            backend.total_sets.push(set_count);
            backend.drives_per_set.push(drive_count);
            backend.standard_sc_data.push(drive_count - parity);
            backend.standard_sc_parities.push(parity);
            let mut endpoints = Vec::new();
            for set_idx in 0..set_count {
                for disk_idx in 0..drive_count {
                    endpoints.push(Endpoint {
                        url: url::Url::parse(&format!(
                            "http://node-{}:9000/pool-{pool_idx}-set-{set_idx}-disk-{disk_idx}",
                            disk_idx % 4
                        ))
                        .expect("configured test endpoint"),
                        is_local: disk_idx % 4 == 0,
                        pool_idx: i32::try_from(pool_idx).expect("test pool index"),
                        set_idx: i32::try_from(set_idx).expect("test set index"),
                        disk_idx: i32::try_from(disk_idx).expect("test disk index"),
                    });
                }
            }
            pools.push(PoolEndpoints {
                legacy: false,
                set_count,
                drives_per_set: drive_count,
                endpoints: Endpoints::from(endpoints),
                cmd_line: String::new(),
                platform: String::new(),
            });
        }
        (backend, EndpointServerPools::from(pools))
    }

    fn observation(endpoints: &EndpointServerPools, node: usize) -> StorageObservation {
        let host = format!("node-{node}:9000");
        let disks = endpoints
            .as_ref()
            .iter()
            .flat_map(|pool| pool.endpoints.as_ref())
            .filter(|endpoint| endpoint.host_port() == host)
            .map(|endpoint| Disk {
                endpoint: endpoint.to_string(),
                drive_path: endpoint.get_file_path(),
                pool_index: endpoint.pool_idx,
                set_index: endpoint.set_idx,
                disk_index: endpoint.disk_idx,
                state: "ok".to_string(),
                runtime_state: Some("online".to_string()),
                ..Default::default()
            })
            .collect();
        StorageObservation {
            member: if node == 0 {
                ObservationMember::Local
            } else {
                ObservationMember::Peer(host)
            },
            info: Some(StorageInfo {
                disks,
                ..Default::default()
            }),
        }
    }

    fn ready_probes(endpoints: &EndpointServerPools, nodes: &[usize]) -> StorageProbes {
        nodes
            .iter()
            .map(|&node| future::ready(observation(endpoints, node)).boxed())
            .collect()
    }

    struct DropSignal(Arc<AtomicBool>);

    impl Drop for DropSignal {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn three_fresh_nodes_reach_ec4_quorum_and_cancel_slow_peer() {
        let (backend, endpoints) = topology(&[(1, 16, 4)]);
        let probes = ready_probes(&endpoints, &[0, 1, 2]);
        let dropped = Arc::new(AtomicBool::new(false));
        let signal = DropSignal(dropped.clone());
        probes.push(
            async move {
                let _signal = signal;
                future::pending().await
            }
            .boxed(),
        );
        assert!(
            collect_storage_quorum(ClusterHealthProbeKind::Write, backend, &endpoints, probes)
                .now_or_never()
                .expect("healthy quorum must not wait for a fourth node")
        );
        assert!(dropped.load(Ordering::SeqCst), "the slow probe must be cancelled");
    }

    #[tokio::test]
    async fn insufficient_storage_or_one_insufficient_set_stays_unready() {
        for layouts in [&[(1, 16, 4)][..], &[(2, 16, 4)][..], &[(1, 16, 4), (1, 8, 2)][..]] {
            let (backend, endpoints) = topology(layouts);
            assert!(
                !collect_storage_quorum(ClusterHealthProbeKind::Write, backend, &endpoints, ready_probes(&endpoints, &[0, 1]))
                    .await
            );
        }
        let (backend, endpoints) = topology(&[(2, 16, 4)]);
        let probes = StorageProbes::new();
        for node in 0..4 {
            let mut reply = observation(&endpoints, node);
            reply
                .info
                .as_mut()
                .expect("test inventory")
                .disks
                .retain(|disk| disk.set_index == 0);
            probes.push(future::ready(reply).boxed());
        }
        assert!(!collect_storage_quorum(ClusterHealthProbeKind::Read, backend, &endpoints, probes).await);
    }

    #[tokio::test]
    async fn storage_read_and_write_thresholds_and_legacy_runtime_are_preserved() {
        let (backend, endpoints) = topology(&[(1, 4, 2)]);
        for (kind, expected) in [(ClusterHealthProbeKind::Read, true), (ClusterHealthProbeKind::Write, false)] {
            let probes = StorageProbes::new();
            for node in [0, 1] {
                let mut reply = observation(&endpoints, node);
                reply.info.as_mut().expect("test inventory").disks[0].runtime_state = None;
                probes.push(future::ready(reply).boxed());
            }
            assert_eq!(collect_storage_quorum(kind, backend.clone(), &endpoints, probes).await, expected);
        }
    }

    #[tokio::test]
    async fn returning_disks_keep_compatibility_while_suspect_disks_do_not_vote() {
        let (backend, endpoints) = topology(&[(1, 4, 2)]);
        for (runtime_state, expected) in [("returning", true), ("suspect", false)] {
            let probes = ready_probes(&endpoints, &[0, 1]);
            let mut reply = observation(&endpoints, 2);
            reply.info.as_mut().expect("test inventory").disks[0].runtime_state = Some(runtime_state.to_string());
            probes.push(future::ready(reply).boxed());
            assert_eq!(
                collect_storage_quorum(ClusterHealthProbeKind::Write, backend.clone(), &endpoints, probes).await,
                expected
            );
        }
    }

    #[tokio::test]
    async fn dropping_collector_cancels_pending_storage_work() {
        let (backend, endpoints) = topology(&[(1, 16, 4)]);
        let probes = ready_probes(&endpoints, &[0, 1]);
        let dropped = Arc::new(AtomicBool::new(false));
        let signal = DropSignal(dropped.clone());
        probes.push(
            async move {
                let _signal = signal;
                future::pending().await
            }
            .boxed(),
        );
        let mut collector = collect_storage_quorum(ClusterHealthProbeKind::Write, backend, &endpoints, probes).boxed();
        assert!(collector.as_mut().now_or_never().is_none(), "two nodes must wait for sufficient evidence");
        assert!(!dropped.load(Ordering::SeqCst));
        drop(collector);
        assert!(dropped.load(Ordering::SeqCst), "cancelling the owner must drop the pending probe");
    }

    #[test]
    fn malformed_configured_slots_cannot_supply_quorum() {
        let (backend, endpoints) = topology(&[(1, 4, 2)]);
        for invalid in 0..4 {
            let mut invalid_endpoints = endpoints.clone();
            let configured = invalid_endpoints.as_mut()[0].endpoints.as_mut();
            match invalid {
                0 => configured[1].disk_idx = configured[0].disk_idx,
                1 => configured[1].pool_idx = -1,
                2 => configured[1].url = configured[0].url.clone(),
                _ => {
                    configured.pop();
                }
            }
            assert!(configured_storage_quorum(ClusterHealthProbeKind::Write, backend.clone(), &invalid_endpoints).is_none());
        }
        let mut missing_shape = backend;
        missing_shape.total_sets.clear();
        assert!(configured_storage_quorum(ClusterHealthProbeKind::Write, missing_shape, &endpoints).is_none());
    }

    #[test]
    fn incremental_storage_votes_match_the_inventory_quorum_for_every_subset() {
        for layouts in [
            &[(1, 4, 2)][..],
            &[(2, 4, 2)][..],
            &[(1, 4, 2), (1, 3, 1)][..],
            &[(2, 4, 2), (1, 3, 1)][..],
        ] {
            let (backend, endpoints) = topology(layouts);
            let positions = endpoints
                .as_ref()
                .iter()
                .flat_map(|pool| pool.endpoints.as_ref())
                .enumerate()
                .map(|(position, endpoint)| ((endpoint.pool_idx, endpoint.set_idx, endpoint.disk_idx), position))
                .collect::<HashMap<_, _>>();
            for subset in 0..(1_usize << positions.len()) {
                for kind in [ClusterHealthProbeKind::Read, ClusterHealthProbeKind::Write] {
                    let mut quorum = configured_storage_quorum(kind, backend.clone(), &endpoints).expect("valid test topology");
                    let mut inventory = StorageInfo {
                        backend: backend.clone(),
                        ..Default::default()
                    };
                    for node in 0..4 {
                        let mut reply = observation(&endpoints, node);
                        let disks = &mut reply.info.as_mut().expect("test inventory").disks;
                        for disk in disks.iter_mut() {
                            let position = positions[&disk_slot(disk)];
                            if subset & (1_usize << position) == 0 {
                                disk.runtime_state = Some("suspect".to_string());
                            } else {
                                match position % 3 {
                                    0 => disk.runtime_state = Some("returning".to_string()),
                                    1 => {
                                        disk.state = "unformatted".to_string();
                                        disk.runtime_state = None;
                                    }
                                    _ => disk.state = "ONLINE".to_string(),
                                }
                            }
                        }
                        inventory.disks.extend(disks.iter().cloned());
                        accept_storage_observation(reply, &mut quorum);
                        let expected = match kind {
                            ClusterHealthProbeKind::Read => storage_read_ready_from_runtime_state(&inventory),
                            ClusterHealthProbeKind::Write => storage_ready_from_runtime_state(&inventory),
                        };
                        assert_eq!(
                            quorum.unsatisfied_sets == 0,
                            expected,
                            "layouts={layouts:?}, subset={subset}, kind={kind:?}, node={node}"
                        );
                    }
                }
            }
        }
    }

    #[tokio::test]
    async fn repeated_storage_members_cannot_supply_extra_votes() {
        let (backend, endpoints) = topology(&[(1, 4, 2)]);
        assert!(
            !collect_storage_quorum(ClusterHealthProbeKind::Read, backend, &endpoints, ready_probes(&endpoints, &[0, 0]),).await
        );
    }

    #[tokio::test]
    async fn wrong_member_duplicate_and_out_of_range_slots_do_not_vote() {
        let (backend, endpoints) = topology(&[(1, 4, 2)]);
        for invalid in 0..5 {
            let probes = ready_probes(&endpoints, &[0]);
            let mut reply = observation(&endpoints, 1);
            let disk = &mut reply.info.as_mut().expect("test inventory").disks[0];
            match invalid {
                0 => reply.member = ObservationMember::Peer("node-2:9000".to_string()),
                1 => disk.disk_index = 4,
                2 => disk.set_index = 1,
                3 => disk.endpoint = "http://node-2:9000/other".to_string(),
                _ => {
                    let duplicate = disk.clone();
                    reply.info.as_mut().expect("test inventory").disks.push(duplicate);
                }
            }
            probes.push(future::ready(reply).boxed());
            assert!(!collect_storage_quorum(ClusterHealthProbeKind::Read, backend.clone(), &endpoints, probes).await);
        }
    }

    #[tokio::test]
    async fn lock_quorum_ignores_slow_nonessential_members_but_requires_every_set() {
        let (_, endpoints) = topology(&[(1, 16, 4)]);
        let probes: LockProbes = (0..3)
            .map(|node| future::ready((format!("node-{node}:9000"), true)).boxed())
            .collect();
        let dropped = Arc::new(AtomicBool::new(false));
        let signal = DropSignal(dropped.clone());
        probes.push(
            async move {
                let _signal = signal;
                future::pending().await
            }
            .boxed(),
        );
        assert!(
            collect_lock_quorum_with(&endpoints, probes)
                .now_or_never()
                .expect("three lock peers must satisfy exclusive quorum")
        );
        assert!(dropped.load(Ordering::SeqCst));

        let probes: LockProbes = (0..2)
            .map(|node| future::ready((format!("node-{node}:9000"), true)).boxed())
            .collect();
        assert!(
            !collect_lock_quorum_with(&endpoints, probes).await,
            "cluster read also requires exclusive-lock quorum"
        );

        let (_, mut multiple_sets) = topology(&[(2, 16, 4)]);
        for endpoint in multiple_sets.as_mut()[0]
            .endpoints
            .as_mut()
            .iter_mut()
            .filter(|endpoint| endpoint.set_idx == 1)
        {
            endpoint
                .url
                .set_host(Some(&format!("other-node-{}", endpoint.disk_idx % 4)))
                .expect("second-set host");
            endpoint.is_local = false;
        }
        let probes: LockProbes = (0..4)
            .map(|node| future::ready((format!("node-{node}:9000"), true)).boxed())
            .collect();
        assert!(
            !collect_lock_quorum_with(&multiple_sets, probes).await,
            "one set's quorum cannot substitute for another set"
        );
    }

    #[tokio::test]
    async fn incremental_lock_votes_match_the_existing_quorum_for_every_host_subset() {
        let (_, mut disjoint_sets) = topology(&[(2, 4, 2)]);
        for endpoint in disjoint_sets.as_mut()[0]
            .endpoints
            .as_mut()
            .iter_mut()
            .filter(|endpoint| endpoint.set_idx == 1)
        {
            endpoint
                .url
                .set_host(Some(&format!("other-node-{}", endpoint.disk_idx)))
                .expect("disjoint second-set host");
            endpoint.is_local = false;
        }
        for endpoints in [topology(&[(1, 16, 4)]).1, topology(&[(2, 4, 2), (1, 3, 1)]).1, disjoint_sets] {
            let mut hosts = endpoints
                .as_ref()
                .iter()
                .flat_map(|pool| pool.endpoints.as_ref())
                .map(Endpoint::host_port)
                .collect::<HashSet<_>>()
                .into_iter()
                .collect::<Vec<_>>();
            hosts.sort();
            for subset in 0..(1_usize << hosts.len()) {
                let online_hosts = hosts
                    .iter()
                    .enumerate()
                    .filter(|(position, _)| subset & (1_usize << position) != 0)
                    .map(|(_, host)| host.clone())
                    .collect::<HashSet<_>>();
                let probes = LockProbes::new();
                for host in &hosts {
                    let online = online_hosts.contains(host);
                    probes.push(future::ready((host.clone(), online)).boxed());
                    probes.push(future::ready((host.clone(), online)).boxed());
                }
                probes.push(future::ready(("unknown-node:9000".to_string(), true)).boxed());
                assert_eq!(
                    collect_lock_quorum_with(&endpoints, probes).await,
                    aggregate_lock_quorum_status(&endpoints, &online_hosts).ready,
                    "hosts={hosts:?}, subset={subset}"
                );
            }
        }
    }

    #[tokio::test]
    async fn native_path_endpoints_are_owned_by_the_local_observation() {
        let (backend, mut endpoints) = topology(&[(1, 4, 2)]);
        let mut disks = Vec::new();
        for endpoint in endpoints.as_mut()[0].endpoints.as_mut() {
            endpoint.url = url::Url::from_file_path(format!("/disk-{}", endpoint.disk_idx)).expect("native disk path");
            endpoint.is_local = true;
            disks.push(Disk {
                endpoint: endpoint.to_string(),
                drive_path: endpoint.get_file_path(),
                pool_index: endpoint.pool_idx,
                set_index: endpoint.set_idx,
                disk_index: endpoint.disk_idx,
                state: "ok".to_string(),
                ..Default::default()
            });
        }
        let probes = StorageProbes::new();
        probes.push(
            future::ready(StorageObservation {
                member: ObservationMember::Local,
                info: Some(StorageInfo {
                    disks,
                    ..Default::default()
                }),
            })
            .boxed(),
        );
        assert!(collect_storage_quorum(ClusterHealthProbeKind::Write, backend, &endpoints, probes).await);
    }

    #[tokio::test]
    async fn reported_mount_paths_do_not_override_configured_endpoint_identity() {
        let (backend, endpoints) = topology(&[(1, 4, 2)]);
        let probes = StorageProbes::new();
        for node in 0..3 {
            let mut reply = observation(&endpoints, node);
            reply.info.as_mut().expect("test inventory").disks[0].drive_path = "/container/mount".to_string();
            probes.push(future::ready(reply).boxed());
        }
        assert!(collect_storage_quorum(ClusterHealthProbeKind::Write, backend, &endpoints, probes).await);
    }
}
