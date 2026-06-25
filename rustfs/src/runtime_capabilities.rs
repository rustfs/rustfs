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

use crate::storage_api::{
    CapabilitySnapshotError, CapabilityStatus, DiskCapabilities, EndpointServerPools, MemorySamplingState, ObservabilitySnapshot,
    ObservabilitySnapshotProvider, PlatformSupport, TopologyCapabilities, TopologySnapshot, TopologySnapshotProvider,
    UserspaceProfilingCapability, topology_snapshot_from_endpoint_pools_with_capabilities,
};

const NOT_WIRED_INTO_RUNTIME: &str = "not wired into runtime";
const EBPF_LINUX_ONLY: &str = "eBPF support is only available on linux targets";
const STORAGE_MEDIA_NOT_REPORTED: &str = "storage media not reported by endpoints";
const FAILURE_DOMAIN_NOT_REPORTED: &str = "failure domain labels not reported by endpoints";
const NUMA_NOT_WIRED: &str = "NUMA topology not wired into runtime";
const NUMA_LINUX_ONLY: &str = "NUMA topology reporting currently targets linux runtimes";

#[derive(Debug, Default, Clone, Copy)]
pub struct RustFsObservabilitySnapshotProvider;

#[async_trait::async_trait]
impl ObservabilitySnapshotProvider for RustFsObservabilitySnapshotProvider {
    async fn observability_snapshot(&self) -> Result<ObservabilitySnapshot, CapabilitySnapshotError> {
        Ok(runtime_observability_snapshot())
    }
}

pub fn runtime_observability_snapshot() -> ObservabilitySnapshot {
    ObservabilitySnapshot {
        runtime_telemetry: runtime_telemetry_status(),
        userspace_profiling: UserspaceProfilingCapability {
            cpu: cpu_profiling_status(),
            memory: memory_profiling_status(),
            continuous_cpu: cpu_profiling_status(),
            periodic_cpu: cpu_profiling_status(),
        },
        memory_sampling: MemorySamplingState {
            process: CapabilityStatus::supported(),
            system: CapabilityStatus::supported(),
            cgroup: cgroup_memory_status(),
        },
        platform: PlatformSupport {
            target_triple: Some(compiled_target_triple()),
            os: Some(std::env::consts::OS.to_owned()),
            arch: Some(std::env::consts::ARCH.to_owned()),
            allocator: CapabilityStatus::supported()
                .with_reason(format!("backend={}", crate::allocator_reclaim::allocator_backend())),
            ebpf: ebpf_status(),
            numa: numa_status(),
        },
    }
}

#[derive(Debug, Clone)]
pub struct EndpointTopologySnapshotProvider {
    endpoint_pools: EndpointServerPools,
}

impl EndpointTopologySnapshotProvider {
    pub fn new(endpoint_pools: EndpointServerPools) -> Self {
        Self { endpoint_pools }
    }
}

#[async_trait::async_trait]
impl TopologySnapshotProvider for EndpointTopologySnapshotProvider {
    async fn topology_snapshot(&self) -> Result<TopologySnapshot, CapabilitySnapshotError> {
        Ok(topology_snapshot_from_endpoint_pools(&self.endpoint_pools))
    }
}

pub fn topology_snapshot_from_endpoint_pools(endpoint_pools: &EndpointServerPools) -> TopologySnapshot {
    topology_snapshot_from_endpoint_pools_with_capabilities(
        endpoint_pools,
        TopologyCapabilities {
            profiling: cpu_profiling_status(),
            numa: CapabilityStatus::unsupported().with_reason(NUMA_NOT_WIRED),
            failure_domain_labels: CapabilityStatus::unknown().with_reason(FAILURE_DOMAIN_NOT_REPORTED),
            media_labels: CapabilityStatus::unknown().with_reason(STORAGE_MEDIA_NOT_REPORTED),
        },
        DiskCapabilities {
            media_type: CapabilityStatus::unknown().with_reason(STORAGE_MEDIA_NOT_REPORTED),
            failure_domain: CapabilityStatus::unknown().with_reason(FAILURE_DOMAIN_NOT_REPORTED),
            numa: CapabilityStatus::unsupported().with_reason(NUMA_NOT_WIRED),
            profiling: cpu_profiling_status(),
        },
    )
}

fn runtime_telemetry_status() -> CapabilityStatus {
    if rustfs_obs::dial9::is_enabled() {
        CapabilityStatus::supported()
    } else {
        CapabilityStatus::disabled()
    }
}

fn compiled_target_triple() -> String {
    let arch = std::env::consts::ARCH;
    let vendor = target_vendor_name();
    let os = std::env::consts::OS;
    let env = target_env_name();

    match env {
        Some(env) => format!("{arch}-{vendor}-{os}-{env}"),
        None => format!("{arch}-{vendor}-{os}"),
    }
}

fn target_vendor_name() -> &'static str {
    if cfg!(target_vendor = "apple") {
        "apple"
    } else if cfg!(target_vendor = "pc") {
        "pc"
    } else {
        "unknown"
    }
}

fn target_env_name() -> Option<&'static str> {
    if cfg!(target_env = "gnu") {
        Some("gnu")
    } else if cfg!(target_env = "musl") {
        Some("musl")
    } else if cfg!(target_env = "msvc") {
        Some("msvc")
    } else if cfg!(target_env = "sgx") {
        Some("sgx")
    } else {
        None
    }
}

fn cpu_profiling_status() -> CapabilityStatus {
    if cfg!(any(target_os = "linux", target_os = "macos")) {
        CapabilityStatus::supported()
    } else {
        CapabilityStatus::unsupported().with_reason("userspace CPU profiling supports linux and macos targets")
    }
}

fn ebpf_status() -> CapabilityStatus {
    if cfg!(target_os = "linux") {
        CapabilityStatus::unknown().with_reason(NOT_WIRED_INTO_RUNTIME)
    } else {
        CapabilityStatus::unsupported().with_reason(EBPF_LINUX_ONLY)
    }
}

fn numa_status() -> CapabilityStatus {
    if cfg!(target_os = "linux") {
        CapabilityStatus::unknown().with_reason(NUMA_NOT_WIRED)
    } else {
        CapabilityStatus::unsupported().with_reason(NUMA_LINUX_ONLY)
    }
}

fn memory_profiling_status() -> CapabilityStatus {
    if cfg!(all(target_os = "linux", target_env = "gnu", target_arch = "x86_64")) {
        CapabilityStatus::supported().with_reason("jemalloc memory profiling target")
    } else {
        CapabilityStatus::unsupported().with_reason("memory profiling supports linux gnu x86_64 targets")
    }
}

fn cgroup_memory_status() -> CapabilityStatus {
    if cfg!(target_os = "linux") {
        CapabilityStatus::supported()
    } else {
        CapabilityStatus::unsupported().with_reason("cgroup memory sampling supports linux targets")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage_api::{
        CapabilityState, Endpoint, Endpoints, ObservabilitySnapshotProvider, PoolEndpoints, TopologySnapshotProvider,
    };

    #[tokio::test]
    async fn observability_provider_returns_platform_snapshot() {
        let provider = RustFsObservabilitySnapshotProvider;
        let snapshot = provider.observability_snapshot().await.expect("observability snapshot");

        assert_eq!(snapshot.platform.os.as_deref(), Some(std::env::consts::OS));
        assert_eq!(snapshot.platform.arch.as_deref(), Some(std::env::consts::ARCH));
        assert_eq!(snapshot.platform.target_triple.as_deref(), Some(compiled_target_triple().as_str()));
        assert!(matches!(
            snapshot.runtime_telemetry.state,
            CapabilityState::Supported | CapabilityState::Disabled
        ));
        assert!(
            snapshot
                .platform
                .allocator
                .reason
                .as_deref()
                .unwrap_or_default()
                .contains("backend=")
        );
        if cfg!(target_os = "linux") {
            assert_eq!(snapshot.platform.ebpf.state, CapabilityState::Unknown);
            assert_eq!(snapshot.platform.numa.state, CapabilityState::Unknown);
        } else {
            assert_eq!(snapshot.platform.ebpf.state, CapabilityState::Unsupported);
            assert_eq!(snapshot.platform.numa.state, CapabilityState::Unsupported);
        }
    }

    #[tokio::test]
    async fn topology_provider_maps_endpoint_pools_to_sets_and_disks() {
        let endpoint_pools = sample_endpoint_pools();
        let provider = EndpointTopologySnapshotProvider::new(endpoint_pools);
        let snapshot = provider.topology_snapshot().await.expect("topology snapshot");

        assert_eq!(snapshot.pools.len(), 1);
        assert_eq!(snapshot.pools[0].sets.len(), 2);
        assert_eq!(snapshot.pools[0].sets[0].disks.len(), 2);
        assert_eq!(snapshot.pools[0].sets[1].disks.len(), 2);
        assert_eq!(snapshot.pools[0].sets[0].disks[1].disk_index, 1);
        assert_eq!(
            snapshot.pools[0].sets[0].disks[0]
                .labels
                .additional
                .get("endpoint_type")
                .map(String::as_str),
            Some("path")
        );
    }

    #[test]
    fn topology_snapshot_does_not_expose_local_paths() {
        let endpoint_pools = sample_endpoint_pools();
        let snapshot = topology_snapshot_from_endpoint_pools(&endpoint_pools);
        let encoded = serde_json::to_string(&snapshot).expect("serialize topology snapshot");

        assert!(!encoded.contains("/tmp/rustfs-runtime-capability"));
    }

    fn sample_endpoint_pools() -> EndpointServerPools {
        let endpoints = (0..4)
            .map(|index| {
                let mut endpoint =
                    Endpoint::try_from(format!("/tmp/rustfs-runtime-capability-{index}").as_str()).expect("local endpoint");
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
            cmd_line: "/tmp/rustfs-runtime-capability-{0...3}".to_owned(),
            platform: "OS: test | Arch: test".to_owned(),
        }])
    }
}
