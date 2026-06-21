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

use std::collections::{BTreeMap, BTreeSet};

use rustfs_storage_api::{
    CapabilityStatus, DiskCapabilities, TopologyCapabilities, TopologyDisk, TopologyLabels, TopologyPool, TopologySet,
    TopologySnapshot,
};

use crate::{
    endpoints::EndpointServerPools,
    layout::endpoint::{Endpoint, EndpointType},
};

const ENDPOINT_TYPE_LABEL: &str = "endpoint_type";
const LOCAL_ENDPOINT_LABEL: &str = "local";
const LOCAL_NODE_ID: &str = "local";
const STORAGE_MEDIA_NOT_REPORTED: &str = "storage media not reported by endpoints";
const FAILURE_DOMAIN_NOT_REPORTED: &str = "failure domain labels not reported by endpoints";
const NUMA_NOT_WIRED: &str = "NUMA topology not wired into runtime";
const PROFILING_NOT_WIRED: &str = "profiling capability not wired into ECStore";
const PEER_HEALTH_NOT_REPORTED: &str = "peer health not reported by endpoints";

#[derive(Debug, Clone)]
pub struct ClusterControlPlane {
    endpoint_pools: EndpointServerPools,
}

impl ClusterControlPlane {
    pub fn new(endpoint_pools: EndpointServerPools) -> Self {
        Self { endpoint_pools }
    }

    pub fn topology_snapshot(&self) -> TopologySnapshot {
        topology_snapshot_from_endpoint_pools(&self.endpoint_pools)
    }

    pub fn membership_snapshot(&self) -> ClusterMembershipSnapshot {
        membership_snapshot_from_endpoint_pools(&self.endpoint_pools)
    }

    pub fn pool_state_snapshot(&self) -> ClusterPoolStateSnapshot {
        pool_state_snapshot_from_endpoint_pools(&self.endpoint_pools)
    }

    pub fn local_node_storage_snapshot(&self) -> ClusterLocalNodeStorageSnapshot {
        let membership = self.membership_snapshot();
        local_node_storage_snapshot_from_membership(&membership)
    }

    pub fn peer_health_snapshot(&self) -> ClusterPeerHealthSnapshot {
        let membership = self.membership_snapshot();
        peer_health_snapshot_from_membership(&membership)
    }

    pub fn read_snapshot(&self) -> ClusterControlPlaneSnapshot {
        let membership = self.membership_snapshot();

        ClusterControlPlaneSnapshot {
            topology: self.topology_snapshot(),
            pool_state: self.pool_state_snapshot(),
            local_storage: local_node_storage_snapshot_from_membership(&membership),
            peer_health: peer_health_snapshot_from_membership(&membership),
            membership,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterControlPlaneSnapshot {
    pub topology: TopologySnapshot,
    pub pool_state: ClusterPoolStateSnapshot,
    pub local_storage: ClusterLocalNodeStorageSnapshot,
    pub peer_health: ClusterPeerHealthSnapshot,
    pub membership: ClusterMembershipSnapshot,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ClusterMembershipSnapshot {
    pub nodes: Vec<ClusterNodeMembership>,
    pub drives: Vec<ClusterDriveMembership>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterNodeMembership {
    pub node_id: String,
    pub grid_host: String,
    pub is_local: bool,
    pub pools: Vec<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterDriveMembership {
    pub pool_index: usize,
    pub set_index: usize,
    pub disk_index: usize,
    pub node_id: String,
    pub is_local: bool,
    pub endpoint_type: ClusterEndpointType,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ClusterPoolStateSnapshot {
    pub pools: Vec<ClusterPoolState>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterPoolState {
    pub pool_index: usize,
    pub set_count: usize,
    pub drives_per_set: usize,
    pub endpoint_count: usize,
    pub local_drive_count: usize,
    pub remote_drive_count: usize,
    pub legacy: bool,
    pub endpoint_types: Vec<ClusterEndpointType>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ClusterLocalNodeStorageSnapshot {
    pub nodes: Vec<ClusterLocalNodeStorage>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterLocalNodeStorage {
    pub node_id: String,
    pub pools: Vec<usize>,
    pub drive_count: usize,
    pub path_drive_count: usize,
    pub url_drive_count: usize,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ClusterPeerHealthSnapshot {
    pub peers: Vec<ClusterPeerHealth>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterPeerHealth {
    pub node_id: String,
    pub is_local: bool,
    pub status: CapabilityStatus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ClusterEndpointType {
    Path,
    Url,
}

pub fn topology_snapshot_from_endpoint_pools(endpoint_pools: &EndpointServerPools) -> TopologySnapshot {
    topology_snapshot_from_endpoint_pools_with_capabilities(
        endpoint_pools,
        default_topology_capabilities(),
        default_disk_capabilities(),
    )
}

pub fn topology_snapshot_from_endpoint_pools_with_capabilities(
    endpoint_pools: &EndpointServerPools,
    capabilities: TopologyCapabilities,
    disk_capabilities: DiskCapabilities,
) -> TopologySnapshot {
    TopologySnapshot {
        pools: endpoint_pools
            .as_ref()
            .iter()
            .enumerate()
            .map(|(pool_index, pool)| {
                let sets =
                    topology_sets_from_endpoints(pool_index, pool.drives_per_set, pool.endpoints.as_ref(), &disk_capabilities);
                TopologyPool {
                    pool_index,
                    pool_id: None,
                    labels: TopologyLabels::default(),
                    sets,
                }
            })
            .collect(),
        capabilities,
    }
}

pub fn membership_snapshot_from_endpoint_pools(endpoint_pools: &EndpointServerPools) -> ClusterMembershipSnapshot {
    let mut nodes = BTreeMap::<String, ClusterNodeMembership>::new();
    let mut drives = Vec::new();

    for (pool_index, pool) in endpoint_pools.as_ref().iter().enumerate() {
        for (endpoint_index, endpoint) in pool.endpoints.as_ref().iter().enumerate() {
            let (set_index, disk_index) = endpoint_indices(pool_index, endpoint_index, pool.drives_per_set, endpoint);
            let node_id = endpoint_node_id(endpoint);

            match nodes.entry(node_id.clone()) {
                std::collections::btree_map::Entry::Vacant(entry) => {
                    entry.insert(ClusterNodeMembership {
                        node_id: node_id.clone(),
                        grid_host: endpoint.grid_host(),
                        is_local: endpoint.is_local,
                        pools: vec![pool_index],
                    });
                }
                std::collections::btree_map::Entry::Occupied(mut entry) => {
                    let node = entry.get_mut();
                    node.is_local |= endpoint.is_local;
                    if !node.pools.contains(&pool_index) {
                        node.pools.push(pool_index);
                    }
                }
            }

            drives.push(ClusterDriveMembership {
                pool_index,
                set_index,
                disk_index,
                node_id,
                is_local: endpoint.is_local,
                endpoint_type: endpoint_type(endpoint),
            });
        }
    }

    ClusterMembershipSnapshot {
        nodes: nodes.into_values().collect(),
        drives,
    }
}

pub fn pool_state_snapshot_from_endpoint_pools(endpoint_pools: &EndpointServerPools) -> ClusterPoolStateSnapshot {
    ClusterPoolStateSnapshot {
        pools: endpoint_pools
            .as_ref()
            .iter()
            .enumerate()
            .map(|(pool_index, pool)| {
                let mut endpoint_types = BTreeSet::new();
                let mut local_drive_count = 0;

                for endpoint in pool.endpoints.as_ref() {
                    endpoint_types.insert(endpoint_type(endpoint));
                    if endpoint.is_local {
                        local_drive_count += 1;
                    }
                }

                let endpoint_count = pool.endpoints.as_ref().len();
                ClusterPoolState {
                    pool_index,
                    set_count: pool.set_count,
                    drives_per_set: pool.drives_per_set,
                    endpoint_count,
                    local_drive_count,
                    remote_drive_count: endpoint_count.saturating_sub(local_drive_count),
                    legacy: pool.legacy,
                    endpoint_types: endpoint_types.into_iter().collect(),
                }
            })
            .collect(),
    }
}

pub fn local_node_storage_snapshot_from_membership(membership: &ClusterMembershipSnapshot) -> ClusterLocalNodeStorageSnapshot {
    ClusterLocalNodeStorageSnapshot {
        nodes: membership
            .nodes
            .iter()
            .filter(|node| node.is_local)
            .map(|node| {
                let mut path_drive_count = 0;
                let mut url_drive_count = 0;

                for drive in membership.drives.iter().filter(|drive| drive.node_id == node.node_id) {
                    match drive.endpoint_type {
                        ClusterEndpointType::Path => path_drive_count += 1,
                        ClusterEndpointType::Url => url_drive_count += 1,
                    }
                }

                ClusterLocalNodeStorage {
                    node_id: node.node_id.clone(),
                    pools: node.pools.clone(),
                    drive_count: path_drive_count + url_drive_count,
                    path_drive_count,
                    url_drive_count,
                }
            })
            .collect(),
    }
}

pub fn peer_health_snapshot_from_membership(membership: &ClusterMembershipSnapshot) -> ClusterPeerHealthSnapshot {
    ClusterPeerHealthSnapshot {
        peers: membership
            .nodes
            .iter()
            .map(|node| ClusterPeerHealth {
                node_id: node.node_id.clone(),
                is_local: node.is_local,
                status: CapabilityStatus::unknown().with_reason(PEER_HEALTH_NOT_REPORTED),
            })
            .collect(),
    }
}

fn topology_sets_from_endpoints(
    pool_index: usize,
    drives_per_set: usize,
    endpoints: &[Endpoint],
    disk_capabilities: &DiskCapabilities,
) -> Vec<TopologySet> {
    let mut sets = BTreeMap::<usize, Vec<TopologyDisk>>::new();

    for (endpoint_index, endpoint) in endpoints.iter().enumerate() {
        let (set_index, disk_index) = endpoint_indices(pool_index, endpoint_index, drives_per_set, endpoint);
        sets.entry(set_index).or_default().push(topology_disk_from_endpoint(
            pool_index,
            set_index,
            disk_index,
            endpoint,
            disk_capabilities,
        ));
    }

    sets.into_iter()
        .map(|(set_index, mut disks)| {
            disks.sort_by_key(|disk| disk.disk_index);
            TopologySet {
                pool_index,
                set_index,
                set_id: None,
                labels: TopologyLabels::default(),
                disks,
            }
        })
        .collect()
}

fn topology_disk_from_endpoint(
    pool_index: usize,
    set_index: usize,
    disk_index: usize,
    endpoint: &Endpoint,
    disk_capabilities: &DiskCapabilities,
) -> TopologyDisk {
    TopologyDisk {
        pool_index,
        set_index,
        disk_index,
        disk_id: endpoint_disk_id(endpoint),
        labels: endpoint_labels(endpoint),
        capabilities: disk_capabilities.clone(),
    }
}

fn endpoint_indices(pool_index: usize, endpoint_index: usize, drives_per_set: usize, endpoint: &Endpoint) -> (usize, usize) {
    let safe_drives_per_set = drives_per_set.max(1);
    let set_index = non_negative_index(endpoint.set_idx).unwrap_or(endpoint_index / safe_drives_per_set);
    let disk_index = non_negative_index(endpoint.disk_idx).unwrap_or(endpoint_index % safe_drives_per_set);

    debug_assert_eq!(non_negative_index(endpoint.pool_idx).unwrap_or(pool_index), pool_index);
    (set_index, disk_index)
}

fn endpoint_disk_id(endpoint: &Endpoint) -> Option<String> {
    let host_port = endpoint.host_port();
    if host_port.is_empty() { None } else { Some(host_port) }
}

fn endpoint_node_id(endpoint: &Endpoint) -> String {
    endpoint_disk_id(endpoint).unwrap_or_else(|| LOCAL_NODE_ID.to_owned())
}

fn endpoint_labels(endpoint: &Endpoint) -> TopologyLabels {
    let mut additional = BTreeMap::new();
    additional.insert(ENDPOINT_TYPE_LABEL.to_owned(), endpoint_type_label(endpoint).to_owned());
    additional.insert(LOCAL_ENDPOINT_LABEL.to_owned(), endpoint.is_local.to_string());

    TopologyLabels {
        additional,
        ..TopologyLabels::default()
    }
}

fn endpoint_type(endpoint: &Endpoint) -> ClusterEndpointType {
    match endpoint.get_type() {
        EndpointType::Path => ClusterEndpointType::Path,
        EndpointType::Url => ClusterEndpointType::Url,
    }
}

fn endpoint_type_label(endpoint: &Endpoint) -> &'static str {
    match endpoint_type(endpoint) {
        ClusterEndpointType::Path => "path",
        ClusterEndpointType::Url => "url",
    }
}

fn non_negative_index(index: i32) -> Option<usize> {
    usize::try_from(index).ok()
}

fn default_topology_capabilities() -> TopologyCapabilities {
    TopologyCapabilities {
        profiling: CapabilityStatus::unknown().with_reason(PROFILING_NOT_WIRED),
        numa: CapabilityStatus::unsupported().with_reason(NUMA_NOT_WIRED),
        failure_domain_labels: CapabilityStatus::unknown().with_reason(FAILURE_DOMAIN_NOT_REPORTED),
        media_labels: CapabilityStatus::unknown().with_reason(STORAGE_MEDIA_NOT_REPORTED),
    }
}

fn default_disk_capabilities() -> DiskCapabilities {
    DiskCapabilities {
        media_type: CapabilityStatus::unknown().with_reason(STORAGE_MEDIA_NOT_REPORTED),
        failure_domain: CapabilityStatus::unknown().with_reason(FAILURE_DOMAIN_NOT_REPORTED),
        numa: CapabilityStatus::unsupported().with_reason(NUMA_NOT_WIRED),
        profiling: CapabilityStatus::unknown().with_reason(PROFILING_NOT_WIRED),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    use crate::endpoints::{Endpoints, PoolEndpoints};

    #[test]
    fn topology_snapshot_maps_endpoint_sets_without_local_paths() {
        let endpoint_pools = sample_path_endpoint_pools();
        let snapshot = topology_snapshot_from_endpoint_pools(&endpoint_pools);

        assert_eq!(snapshot.pools.len(), 1);
        assert_eq!(snapshot.pools[0].sets.len(), 2);
        assert_eq!(snapshot.pools[0].sets[0].disks.len(), 2);
        assert_eq!(snapshot.pools[0].sets[1].disks.len(), 2);
        assert_eq!(snapshot.pools[0].sets[1].disks[1].disk_index, 1);
        assert_eq!(
            snapshot.pools[0].sets[0].disks[0]
                .labels
                .additional
                .get(ENDPOINT_TYPE_LABEL)
                .map(String::as_str),
            Some("path")
        );

        let encoded = serde_json::to_string(&snapshot).expect("serialize topology snapshot");
        assert!(!encoded.contains("/tmp/rustfs-cluster-control-plane"));
    }

    #[test]
    fn topology_snapshot_uses_url_hosts_as_disk_ids() {
        let endpoint_pools = sample_url_endpoint_pools();
        let snapshot = topology_snapshot_from_endpoint_pools(&endpoint_pools);

        assert_eq!(snapshot.pools[0].sets[0].disks[0].disk_id.as_deref(), Some("node1.example:9000"));
        assert_eq!(
            snapshot.pools[0].sets[0].disks[0]
                .labels
                .additional
                .get(ENDPOINT_TYPE_LABEL)
                .map(String::as_str),
            Some("url")
        );
    }

    #[test]
    fn membership_snapshot_groups_nodes_and_drives() {
        let endpoint_pools = sample_url_endpoint_pools();
        let snapshot = membership_snapshot_from_endpoint_pools(&endpoint_pools);

        assert_eq!(snapshot.nodes.len(), 2);
        assert_eq!(snapshot.drives.len(), 4);
        assert_eq!(snapshot.nodes[0].node_id, "node1.example:9000");
        assert_eq!(snapshot.nodes[0].pools, vec![0]);
        assert_eq!(snapshot.drives[2].node_id, "node2.example:9000");
        assert_eq!(snapshot.drives[2].set_index, 1);
        assert_eq!(snapshot.drives[2].endpoint_type, ClusterEndpointType::Url);
    }

    #[test]
    fn pool_state_snapshot_counts_local_remote_drives_and_endpoint_types() {
        let endpoint_pools = sample_mixed_endpoint_pools();
        let snapshot = pool_state_snapshot_from_endpoint_pools(&endpoint_pools);

        assert_eq!(snapshot.pools.len(), 1);
        assert_eq!(snapshot.pools[0].set_count, 2);
        assert_eq!(snapshot.pools[0].drives_per_set, 2);
        assert_eq!(snapshot.pools[0].endpoint_count, 4);
        assert_eq!(snapshot.pools[0].local_drive_count, 3);
        assert_eq!(snapshot.pools[0].remote_drive_count, 1);
        assert_eq!(
            snapshot.pools[0].endpoint_types,
            vec![ClusterEndpointType::Path, ClusterEndpointType::Url]
        );
    }

    #[test]
    fn local_node_storage_snapshot_keeps_only_local_drive_counts() {
        let membership = membership_snapshot_from_endpoint_pools(&sample_mixed_endpoint_pools());
        let snapshot = local_node_storage_snapshot_from_membership(&membership);

        assert_eq!(snapshot.nodes.len(), 2);
        assert_eq!(snapshot.nodes[0].node_id, LOCAL_NODE_ID);
        assert_eq!(snapshot.nodes[0].drive_count, 2);
        assert_eq!(snapshot.nodes[0].path_drive_count, 2);
        assert_eq!(snapshot.nodes[0].url_drive_count, 0);
        assert_eq!(snapshot.nodes[1].node_id, "node1.example:9000");
        assert_eq!(snapshot.nodes[1].drive_count, 1);
        assert_eq!(snapshot.nodes[1].path_drive_count, 0);
        assert_eq!(snapshot.nodes[1].url_drive_count, 1);
    }

    #[test]
    fn peer_health_snapshot_reports_static_unknown_status() {
        let membership = membership_snapshot_from_endpoint_pools(&sample_url_endpoint_pools());
        let snapshot = peer_health_snapshot_from_membership(&membership);

        assert_eq!(snapshot.peers.len(), 2);
        assert_eq!(snapshot.peers[0].node_id, "node1.example:9000");
        assert!(snapshot.peers[0].is_local);
        assert_eq!(snapshot.peers[0].status.reason.as_deref(), Some(PEER_HEALTH_NOT_REPORTED));
        assert_eq!(snapshot.peers[1].node_id, "node2.example:9000");
        assert!(!snapshot.peers[1].is_local);
    }

    #[test]
    fn control_plane_read_snapshot_combines_topology_and_membership() {
        let control_plane = ClusterControlPlane::new(sample_mixed_endpoint_pools());
        let snapshot = control_plane.read_snapshot();
        let node_ids = snapshot
            .membership
            .nodes
            .iter()
            .map(|node| node.node_id.as_str())
            .collect::<BTreeSet<_>>();

        assert_eq!(snapshot.topology.pools[0].sets.len(), 2);
        assert_eq!(snapshot.pool_state.pools[0].endpoint_count, 4);
        assert_eq!(snapshot.local_storage.nodes.len(), 2);
        assert_eq!(snapshot.peer_health.peers.len(), 3);
        assert_eq!(node_ids, BTreeSet::from([LOCAL_NODE_ID, "node1.example:9000", "node2.example:9000"]));
    }

    fn sample_path_endpoint_pools() -> EndpointServerPools {
        let endpoints = (0..4)
            .map(|index| {
                let mut endpoint =
                    Endpoint::try_from(format!("/tmp/rustfs-cluster-control-plane-{index}").as_str()).expect("local endpoint");
                endpoint.set_pool_index(0);
                endpoint.set_set_index(index / 2);
                endpoint.set_disk_index(index % 2);
                endpoint
            })
            .collect::<Vec<_>>();

        EndpointServerPools::from(vec![PoolEndpoints {
            legacy: false,
            set_count: 2,
            drives_per_set: 2,
            endpoints: Endpoints::from(endpoints),
            cmd_line: "/tmp/rustfs-cluster-control-plane-{0...3}".to_owned(),
            platform: "OS: test | Arch: test".to_owned(),
        }])
    }

    fn sample_url_endpoint_pools() -> EndpointServerPools {
        let endpoints = (0..4)
            .map(|index| {
                let host = if index < 2 { "node1.example" } else { "node2.example" };
                let mut endpoint =
                    Endpoint::try_from(format!("http://{host}:9000/export{index}").as_str()).expect("url endpoint");
                endpoint.set_pool_index(0);
                endpoint.set_set_index(index / 2);
                endpoint.set_disk_index(index % 2);
                endpoint.is_local = index < 2;
                endpoint
            })
            .collect::<Vec<_>>();

        EndpointServerPools::from(vec![PoolEndpoints {
            legacy: false,
            set_count: 2,
            drives_per_set: 2,
            endpoints: Endpoints::from(endpoints),
            cmd_line: "http://node{1...2}.example:9000/export{0...3}".to_owned(),
            platform: "OS: test | Arch: test".to_owned(),
        }])
    }

    fn sample_mixed_endpoint_pools() -> EndpointServerPools {
        let mut endpoints = Vec::new();

        for index in 0..2 {
            let mut endpoint =
                Endpoint::try_from(format!("/tmp/rustfs-cluster-control-plane-{index}").as_str()).expect("local endpoint");
            endpoint.set_pool_index(0);
            endpoint.set_set_index(0);
            endpoint.set_disk_index(index);
            endpoints.push(endpoint);
        }

        for index in 2..4 {
            let host = if index == 2 { "node1.example" } else { "node2.example" };
            let mut endpoint = Endpoint::try_from(format!("http://{host}:9000/export{index}").as_str()).expect("url endpoint");
            endpoint.set_pool_index(0);
            endpoint.set_set_index(1);
            endpoint.set_disk_index(index - 2);
            endpoint.is_local = index == 2;
            endpoints.push(endpoint);
        }

        EndpointServerPools::from(vec![PoolEndpoints {
            legacy: false,
            set_count: 2,
            drives_per_set: 2,
            endpoints: Endpoints::from(endpoints),
            cmd_line: "mixed-test-endpoints".to_owned(),
            platform: "OS: test | Arch: test".to_owned(),
        }])
    }
}
