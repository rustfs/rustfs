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

use std::collections::BTreeMap;
use std::fmt::Debug;

use serde::{Deserialize, Serialize};

use crate::{CapabilitySnapshotError, CapabilityStatus};

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TopologySnapshot {
    pub pools: Vec<TopologyPool>,
    pub capabilities: TopologyCapabilities,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TopologyCapabilities {
    pub profiling: CapabilityStatus,
    pub numa: CapabilityStatus,
    pub failure_domain_labels: CapabilityStatus,
    pub media_labels: CapabilityStatus,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TopologyPool {
    pub pool_index: usize,
    pub pool_id: Option<String>,
    pub labels: TopologyLabels,
    pub sets: Vec<TopologySet>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TopologySet {
    pub pool_index: usize,
    pub set_index: usize,
    pub set_id: Option<String>,
    pub labels: TopologyLabels,
    pub disks: Vec<TopologyDisk>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TopologyDisk {
    pub pool_index: usize,
    pub set_index: usize,
    pub disk_index: usize,
    pub disk_id: Option<String>,
    pub labels: TopologyLabels,
    pub capabilities: DiskCapabilities,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TopologyLabels {
    pub zone: Option<String>,
    pub rack: Option<String>,
    pub node: Option<String>,
    pub media: Option<String>,
    pub numa_node: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub additional: BTreeMap<String, String>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DiskCapabilities {
    pub media_type: CapabilityStatus,
    pub failure_domain: CapabilityStatus,
    pub numa: CapabilityStatus,
    pub profiling: CapabilityStatus,
}

#[async_trait::async_trait]
pub trait TopologySnapshotProvider: Send + Sync + Debug {
    async fn topology_snapshot(&self) -> Result<TopologySnapshot, CapabilitySnapshotError>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::CapabilityState;

    #[test]
    fn topology_snapshot_allows_missing_and_extra_labels() {
        let raw = r#"{
            "pools": [{
                "pool_index": 0,
                "pool_id": null,
                "labels": {
                    "additional": {
                        "room": "a"
                    }
                },
                "sets": [{
                    "pool_index": 0,
                    "set_index": 1,
                    "set_id": null,
                    "labels": {},
                    "disks": [{
                        "pool_index": 0,
                        "set_index": 1,
                        "disk_index": 2,
                        "disk_id": "disk-2",
                        "labels": {
                            "media": "ssd",
                            "additional": {
                                "slot": "nvme0"
                            }
                        },
                        "capabilities": {
                            "media_type": { "state": "supported" },
                            "failure_domain": { "state": "unknown" },
                            "numa": { "state": "unsupported", "reason": "not reported" },
                            "profiling": { "state": "disabled" }
                        }
                    }]
                }]
            }],
            "capabilities": {
                "profiling": { "state": "supported" },
                "numa": { "state": "unknown" },
                "failure_domain_labels": { "state": "supported" },
                "media_labels": { "state": "supported" }
            }
        }"#;

        let snapshot: TopologySnapshot = serde_json::from_str(raw).expect("deserialize topology snapshot");
        let disk = &snapshot.pools[0].sets[0].disks[0];

        assert_eq!(snapshot.pools[0].labels.zone, None);
        assert_eq!(snapshot.pools[0].labels.additional.get("room").map(String::as_str), Some("a"));
        assert_eq!(disk.labels.media.as_deref(), Some("ssd"));
        assert_eq!(disk.labels.additional.get("slot").map(String::as_str), Some("nvme0"));
        assert_eq!(disk.capabilities.numa.state, CapabilityState::Unsupported);
        assert_eq!(disk.capabilities.profiling.state, CapabilityState::Disabled);
    }
}
