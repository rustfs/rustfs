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

use crate::runtime_capabilities::runtime_observability_snapshot;
use crate::server::{
    DependencyReadiness, DependencyReadinessReport, ReadinessDegradedReason, snapshot_dependency_readiness_report,
};
use crate::storage_api::cluster::EndpointServerPools;
use crate::storage_api::cluster::contract::observability::ObservabilitySnapshot;
use crate::storage_api::cluster::contract::topology::TopologySnapshot;
use crate::storage_api::cluster::control_plane::{
    ClusterControlPlane, ClusterControlPlaneSnapshot, ClusterLocalNodeStorageSnapshot, ClusterMembershipSnapshot,
    ClusterPeerHealthSnapshot, ClusterPoolStateSnapshot,
};
use crate::workload_admission::workload_admission_registry_snapshot;
use rustfs_concurrency::{AdmissionState, WorkloadAdmissionRegistrySnapshot};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterReadOnlySnapshot {
    pub topology: TopologySnapshot,
    pub membership: ClusterMembershipSnapshot,
    pub pool_state: ClusterPoolStateSnapshot,
    pub local_storage: ClusterLocalNodeStorageSnapshot,
    pub peer_health: ClusterPeerHealthSnapshot,
    pub observability: ObservabilitySnapshot,
    pub workload_admission: WorkloadAdmissionRegistrySnapshot,
    pub runtime_status: ClusterRuntimeStatusSnapshot,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterRuntimeStatusSnapshot {
    pub readiness: DependencyReadiness,
    pub state: ClusterRuntimeReadinessState,
    pub degraded_reasons: Vec<ReadinessDegradedReason>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClusterRuntimeReadinessState {
    Ready,
    Degraded,
    Unknown,
}

impl ClusterRuntimeReadinessState {
    fn from_report(report: &DependencyReadinessReport) -> Self {
        if report.readiness.storage_ready && report.readiness.iam_ready && report.readiness.lock_quorum_ready {
            Self::Ready
        } else if report.degraded_reasons.is_empty() {
            Self::Unknown
        } else {
            Self::Degraded
        }
    }
}

impl ClusterRuntimeStatusSnapshot {
    pub fn from_readiness_report(report: DependencyReadinessReport) -> Self {
        let state = ClusterRuntimeReadinessState::from_report(&report);
        Self {
            readiness: report.readiness,
            state,
            degraded_reasons: report.degraded_reasons,
        }
    }
}

pub fn cluster_read_only_snapshot_from_endpoint_pools(
    endpoint_pools: &EndpointServerPools,
    runtime_status: ClusterRuntimeStatusSnapshot,
) -> ClusterReadOnlySnapshot {
    let control_plane = ClusterControlPlane::new(endpoint_pools.clone());
    let control_plane_snapshot = control_plane.read_snapshot();
    cluster_read_only_snapshot_from_control_plane(control_plane_snapshot, runtime_status)
}

pub fn cluster_read_only_snapshot_from_control_plane(
    control_plane: ClusterControlPlaneSnapshot,
    runtime_status: ClusterRuntimeStatusSnapshot,
) -> ClusterReadOnlySnapshot {
    ClusterReadOnlySnapshot {
        topology: control_plane.topology,
        membership: control_plane.membership,
        pool_state: control_plane.pool_state,
        local_storage: control_plane.local_storage,
        peer_health: control_plane.peer_health,
        observability: runtime_observability_snapshot(),
        workload_admission: workload_admission_registry_snapshot(),
        runtime_status,
    }
}

pub async fn collect_cluster_read_only_snapshot(endpoint_pools: &EndpointServerPools) -> Option<ClusterReadOnlySnapshot> {
    let runtime_status = ClusterRuntimeStatusSnapshot::from_readiness_report(snapshot_dependency_readiness_report().await);
    Some(cluster_read_only_snapshot_from_endpoint_pools(endpoint_pools, runtime_status))
}

pub fn cluster_has_actionable_pressure(snapshot: &ClusterReadOnlySnapshot) -> bool {
    snapshot.runtime_status.state == ClusterRuntimeReadinessState::Degraded
        || snapshot
            .workload_admission
            .entries()
            .iter()
            .any(|entry| entry.state != AdmissionState::Open)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage_api::cluster::contract::capability::{CapabilityState, CapabilityStatus};
    use crate::storage_api::cluster::contract::topology::{DiskCapabilities, TopologyCapabilities};
    use crate::storage_api::cluster::{Endpoint, Endpoints, PoolEndpoints};
    use rustfs_concurrency::{WorkloadAdmissionSnapshot, WorkloadClass};

    #[test]
    fn runtime_status_snapshot_maps_ready_and_degraded_reports() {
        let ready = ClusterRuntimeStatusSnapshot::from_readiness_report(DependencyReadinessReport {
            readiness: DependencyReadiness {
                storage_ready: true,
                iam_ready: true,
                lock_quorum_ready: true,
            },
            degraded_reasons: Vec::new(),
        });
        assert_eq!(ready.state, ClusterRuntimeReadinessState::Ready);

        let degraded = ClusterRuntimeStatusSnapshot::from_readiness_report(DependencyReadinessReport {
            readiness: DependencyReadiness {
                storage_ready: false,
                iam_ready: true,
                lock_quorum_ready: true,
            },
            degraded_reasons: vec![ReadinessDegradedReason::StorageQuorumUnavailable],
        });
        assert_eq!(degraded.state, ClusterRuntimeReadinessState::Degraded);
        assert_eq!(degraded.degraded_reasons, vec![ReadinessDegradedReason::StorageQuorumUnavailable]);
    }

    #[test]
    fn cluster_snapshot_preserves_read_only_sections_and_partial_states() {
        let endpoint_pools = sample_endpoint_pools();
        let runtime_status = ClusterRuntimeStatusSnapshot::from_readiness_report(DependencyReadinessReport {
            readiness: DependencyReadiness {
                storage_ready: false,
                iam_ready: true,
                lock_quorum_ready: false,
            },
            degraded_reasons: vec![ReadinessDegradedReason::StorageAndLockUnavailable],
        });

        let snapshot = cluster_read_only_snapshot_from_endpoint_pools(&endpoint_pools, runtime_status);

        assert_eq!(snapshot.topology.pools.len(), 1);
        assert_eq!(snapshot.membership.nodes.len(), 2);
        assert_eq!(snapshot.pool_state.pools[0].endpoint_count, 4);
        assert_eq!(snapshot.local_storage.nodes.len(), 1);
        assert_eq!(snapshot.peer_health.peers.len(), 2);
        assert_eq!(snapshot.runtime_status.state, ClusterRuntimeReadinessState::Degraded);
        assert_eq!(
            snapshot.runtime_status.degraded_reasons,
            vec![ReadinessDegradedReason::StorageAndLockUnavailable]
        );
        assert_eq!(snapshot.peer_health.peers[0].status.state, CapabilityState::Unknown);
        if cfg!(target_os = "linux") {
            assert_eq!(snapshot.observability.platform.numa.state, CapabilityState::Unknown);
        } else {
            assert_eq!(snapshot.observability.platform.numa.state, CapabilityState::Unsupported);
        }
        assert!(
            snapshot
                .workload_admission
                .entries()
                .iter()
                .any(|entry| entry.state == AdmissionState::Unknown)
        );
    }

    #[test]
    fn cluster_pressure_helper_detects_degraded_runtime_or_non_open_admission() {
        let no_pressure = ClusterReadOnlySnapshot {
            topology: TopologySnapshot::default(),
            membership: ClusterMembershipSnapshot::default(),
            pool_state: ClusterPoolStateSnapshot::default(),
            local_storage: ClusterLocalNodeStorageSnapshot::default(),
            peer_health: ClusterPeerHealthSnapshot::default(),
            observability: ObservabilitySnapshot::default(),
            workload_admission: WorkloadAdmissionRegistrySnapshot::new(vec![WorkloadAdmissionSnapshot::new(
                WorkloadClass::ForegroundRead,
                AdmissionState::Open,
            )]),
            runtime_status: ClusterRuntimeStatusSnapshot {
                readiness: DependencyReadiness {
                    storage_ready: true,
                    iam_ready: true,
                    lock_quorum_ready: true,
                },
                state: ClusterRuntimeReadinessState::Ready,
                degraded_reasons: Vec::new(),
            },
        };
        assert!(!cluster_has_actionable_pressure(&no_pressure));

        let runtime_pressure = ClusterReadOnlySnapshot {
            runtime_status: ClusterRuntimeStatusSnapshot {
                readiness: DependencyReadiness {
                    storage_ready: false,
                    iam_ready: true,
                    lock_quorum_ready: true,
                },
                state: ClusterRuntimeReadinessState::Degraded,
                degraded_reasons: vec![ReadinessDegradedReason::StorageQuorumUnavailable],
            },
            ..no_pressure.clone()
        };
        assert!(cluster_has_actionable_pressure(&runtime_pressure));

        let admission_pressure = ClusterReadOnlySnapshot {
            workload_admission: WorkloadAdmissionRegistrySnapshot::new(vec![WorkloadAdmissionSnapshot::new(
                WorkloadClass::Repair,
                AdmissionState::Unknown,
            )]),
            ..no_pressure
        };
        assert!(cluster_has_actionable_pressure(&admission_pressure));
    }

    #[test]
    fn topology_capability_states_can_remain_unknown_disabled_and_unsupported() {
        let endpoint_pools = sample_endpoint_pools();
        let mut control_plane = ClusterControlPlane::new(endpoint_pools).read_snapshot();
        control_plane.topology.capabilities = TopologyCapabilities {
            profiling: CapabilityStatus::disabled().with_reason("disabled for test"),
            numa: CapabilityStatus::unsupported().with_reason("unsupported for test"),
            failure_domain_labels: CapabilityStatus::unknown().with_reason("unknown for test"),
            media_labels: CapabilityStatus::supported(),
        };

        for pool in &mut control_plane.topology.pools {
            for set in &mut pool.sets {
                for disk in &mut set.disks {
                    disk.capabilities = DiskCapabilities {
                        media_type: CapabilityStatus::unknown().with_reason("media unknown"),
                        failure_domain: CapabilityStatus::unknown().with_reason("fd unknown"),
                        numa: CapabilityStatus::unsupported().with_reason("numa unsupported"),
                        profiling: CapabilityStatus::disabled().with_reason("profiling disabled"),
                    };
                }
            }
        }

        let snapshot = cluster_read_only_snapshot_from_control_plane(
            control_plane,
            ClusterRuntimeStatusSnapshot {
                readiness: DependencyReadiness {
                    storage_ready: true,
                    iam_ready: true,
                    lock_quorum_ready: true,
                },
                state: ClusterRuntimeReadinessState::Ready,
                degraded_reasons: Vec::new(),
            },
        );

        assert_eq!(snapshot.topology.capabilities.profiling.state, CapabilityState::Disabled);
        assert_eq!(snapshot.topology.capabilities.numa.state, CapabilityState::Unsupported);
        assert_eq!(snapshot.topology.capabilities.failure_domain_labels.state, CapabilityState::Unknown);
        assert_eq!(
            snapshot.topology.pools[0].sets[0].disks[0].capabilities.profiling.state,
            CapabilityState::Disabled
        );
    }

    fn sample_endpoint_pools() -> EndpointServerPools {
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
}
