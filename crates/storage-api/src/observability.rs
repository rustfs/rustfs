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

use std::fmt::Debug;

use serde::{Deserialize, Serialize};

use crate::{CapabilitySnapshotError, CapabilityStatus};

/// Snapshot of observability capabilities and state.
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObservabilitySnapshot {
    /// Runtime telemetry capabilities.
    pub runtime_telemetry: CapabilityStatus,
    /// Userspace profiling capabilities.
    pub userspace_profiling: UserspaceProfilingCapability,
    /// Memory sampling state.
    pub memory_sampling: MemorySamplingState,
    /// Platform support information.
    pub platform: PlatformSupport,
}

/// Capabilities for userspace profiling.
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UserspaceProfilingCapability {
    /// CPU profiling capability.
    pub cpu: CapabilityStatus,
    /// Memory profiling capability.
    pub memory: CapabilityStatus,
    /// Continuous CPU profiling capability.
    pub continuous_cpu: CapabilityStatus,
    /// Periodic CPU profiling capability.
    pub periodic_cpu: CapabilityStatus,
}

/// State of memory sampling capabilities.
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MemorySamplingState {
    /// Process-level memory sampling.
    pub process: CapabilityStatus,
    /// System-level memory sampling.
    pub system: CapabilityStatus,
    /// Cgroup-level memory sampling.
    pub cgroup: CapabilityStatus,
}

/// Platform support information.
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlatformSupport {
    /// Target triple (e.g., x86_64-unknown-linux-gnu).
    pub target_triple: Option<String>,
    /// Operating system.
    pub os: Option<String>,
    /// Architecture.
    pub arch: Option<String>,
    /// Memory allocator capability.
    pub allocator: CapabilityStatus,
    /// eBPF support.
    pub ebpf: CapabilityStatus,
    /// NUMA support.
    pub numa: CapabilityStatus,
}

/// Provider for observability snapshots.
#[async_trait::async_trait]
pub trait ObservabilitySnapshotProvider: Send + Sync + Debug {
    /// Get a snapshot of observability capabilities and state.
    async fn observability_snapshot(&self) -> Result<ObservabilitySnapshot, CapabilitySnapshotError>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::CapabilityState;

    #[test]
    fn observability_snapshot_preserves_unknown_and_unsupported_states() {
        let snapshot = ObservabilitySnapshot {
            runtime_telemetry: CapabilityStatus::unknown(),
            userspace_profiling: UserspaceProfilingCapability {
                cpu: CapabilityStatus::unsupported().with_reason("unsupported target"),
                memory: CapabilityStatus::disabled(),
                continuous_cpu: CapabilityStatus::unknown(),
                periodic_cpu: CapabilityStatus::supported(),
            },
            memory_sampling: MemorySamplingState {
                process: CapabilityStatus::supported(),
                system: CapabilityStatus::supported(),
                cgroup: CapabilityStatus::unknown(),
            },
            platform: PlatformSupport {
                target_triple: Some("x86_64-unknown-linux-gnu".to_owned()),
                os: Some("linux".to_owned()),
                arch: Some("x86_64".to_owned()),
                allocator: CapabilityStatus::supported(),
                ebpf: CapabilityStatus::unknown(),
                numa: CapabilityStatus::unsupported(),
            },
        };

        let encoded = serde_json::to_string(&snapshot).expect("serialize observability snapshot");
        let decoded: ObservabilitySnapshot = serde_json::from_str(&encoded).expect("deserialize observability snapshot");

        assert_eq!(decoded.runtime_telemetry.state, CapabilityState::Unknown);
        assert_eq!(decoded.userspace_profiling.cpu.state, CapabilityState::Unsupported);
        assert_eq!(decoded.userspace_profiling.memory.state, CapabilityState::Disabled);
        assert_eq!(decoded.platform.numa.state, CapabilityState::Unsupported);
        assert_eq!(decoded.platform.ebpf.state, CapabilityState::Unknown);
    }
}
