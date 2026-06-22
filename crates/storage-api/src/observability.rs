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

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObservabilitySnapshot {
    pub runtime_telemetry: CapabilityStatus,
    pub userspace_profiling: UserspaceProfilingCapability,
    pub memory_sampling: MemorySamplingState,
    pub platform: PlatformSupport,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UserspaceProfilingCapability {
    pub cpu: CapabilityStatus,
    pub memory: CapabilityStatus,
    pub continuous_cpu: CapabilityStatus,
    pub periodic_cpu: CapabilityStatus,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MemorySamplingState {
    pub process: CapabilityStatus,
    pub system: CapabilityStatus,
    pub cgroup: CapabilityStatus,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlatformSupport {
    pub target_triple: Option<String>,
    pub os: Option<String>,
    pub arch: Option<String>,
    pub allocator: CapabilityStatus,
    pub ebpf: CapabilityStatus,
    pub numa: CapabilityStatus,
}

#[async_trait::async_trait]
pub trait ObservabilitySnapshotProvider: Send + Sync + Debug {
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
