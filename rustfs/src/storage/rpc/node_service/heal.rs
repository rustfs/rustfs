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

use crate::startup_background::{heal_enabled_from_env, scanner_enabled_from_env};
use rmp_serde::Deserializer;
use rustfs_common::heal_channel::HealScanMode;
use rustfs_heal::HealOperationsSnapshot;
use rustfs_scanner::scanner::BackgroundHealInfo;
use serde::{Deserialize, Serialize};
use std::io::Cursor;

use super::super::encode_msgpack_map;

const NODE_HEAL_STATUS_VERSION: u8 = 1;
const NODE_HEAL_STATUS_MAX_SIZE: usize = 64 * 1024;

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct NodeHealProgress {
    pub objects_scanned: u64,
    pub objects_healed: u64,
    pub objects_failed: u64,
    pub bytes_processed: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct NodeHealInfo {
    bitrot_start_time: Option<chrono::DateTime<chrono::Utc>>,
    bitrot_start_cycle: u64,
    current_scan_mode: HealScanMode,
}

impl From<BackgroundHealInfo> for NodeHealInfo {
    fn from(info: BackgroundHealInfo) -> Self {
        Self {
            bitrot_start_time: info.bitrot_start_time,
            bitrot_start_cycle: info.bitrot_start_cycle,
            current_scan_mode: info.current_scan_mode,
        }
    }
}

impl From<NodeHealInfo> for BackgroundHealInfo {
    fn from(info: NodeHealInfo) -> Self {
        Self {
            bitrot_start_time: info.bitrot_start_time,
            bitrot_start_cycle: info.bitrot_start_cycle,
            current_scan_mode: info.current_scan_mode,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct NodeHealStatusSnapshot {
    version: u8,
    pub services_enabled: bool,
    pub initialized: bool,
    info: NodeHealInfo,
    pub operations: HealOperationsSnapshot,
    pub progress: Option<NodeHealProgress>,
}

impl NodeHealStatusSnapshot {
    #[cfg(test)]
    pub(crate) fn for_test(
        services_enabled: bool,
        initialized: bool,
        info: BackgroundHealInfo,
        operations: HealOperationsSnapshot,
        progress: Option<NodeHealProgress>,
    ) -> Self {
        Self {
            version: NODE_HEAL_STATUS_VERSION,
            services_enabled,
            initialized,
            info: info.into(),
            operations,
            progress,
        }
    }

    pub(crate) fn info(&self) -> BackgroundHealInfo {
        BackgroundHealInfo {
            bitrot_start_time: self.info.bitrot_start_time,
            bitrot_start_cycle: self.info.bitrot_start_cycle,
            current_scan_mode: self.info.current_scan_mode,
        }
    }
}

pub(crate) async fn capture_node_heal_status(info: BackgroundHealInfo) -> NodeHealStatusSnapshot {
    let progress = rustfs_heal::current_heal_progress_snapshot()
        .await
        .map(|progress| NodeHealProgress {
            objects_scanned: progress.objects_scanned,
            objects_healed: progress.objects_healed,
            objects_failed: progress.objects_failed,
            bytes_processed: progress.bytes_processed,
        });

    NodeHealStatusSnapshot {
        version: NODE_HEAL_STATUS_VERSION,
        services_enabled: scanner_enabled_from_env() || heal_enabled_from_env(),
        initialized: rustfs_heal::heal_runtime_initialized(),
        info: info.into(),
        operations: rustfs_heal::current_heal_operations_snapshot().await,
        progress,
    }
}

pub(crate) fn encode_node_heal_status(snapshot: &NodeHealStatusSnapshot) -> Result<Vec<u8>, String> {
    encode_msgpack_map(snapshot).map_err(|err| format!("failed to encode node heal status: {err}"))
}

pub(crate) fn decode_node_heal_status(data: &[u8]) -> Result<NodeHealStatusSnapshot, String> {
    if data.len() > NODE_HEAL_STATUS_MAX_SIZE {
        return Err("node heal status exceeds size limit".to_string());
    }
    let mut deserializer = Deserializer::new(Cursor::new(data));
    let snapshot = NodeHealStatusSnapshot::deserialize(&mut deserializer)
        .map_err(|err| format!("failed to decode node heal status: {err}"))?;
    if usize::try_from(deserializer.get_ref().position()).ok() != Some(data.len()) {
        return Err("node heal status contains trailing data".to_string());
    }
    if snapshot.version != NODE_HEAL_STATUS_VERSION {
        return Err(format!("unsupported node heal status version: {}", snapshot.version));
    }
    Ok(snapshot)
}

#[cfg(test)]
mod tests {
    use super::{
        NODE_HEAL_STATUS_MAX_SIZE, NODE_HEAL_STATUS_VERSION, NodeHealProgress, NodeHealStatusSnapshot, decode_node_heal_status,
        encode_node_heal_status,
    };
    use rustfs_heal::HealOperationsSnapshot;
    use rustfs_scanner::scanner::BackgroundHealInfo;

    #[test]
    fn node_heal_status_round_trip_is_versioned() {
        let snapshot = NodeHealStatusSnapshot::for_test(
            true,
            true,
            BackgroundHealInfo::default(),
            HealOperationsSnapshot {
                queue_length: 2,
                active_tasks: 1,
                ..Default::default()
            },
            Some(NodeHealProgress {
                objects_scanned: 7,
                objects_healed: 5,
                objects_failed: 1,
                bytes_processed: 1024,
            }),
        );

        let encoded = encode_node_heal_status(&snapshot).expect("snapshot should encode");
        let decoded = decode_node_heal_status(&encoded).expect("snapshot should decode");
        assert_eq!(decoded.version, NODE_HEAL_STATUS_VERSION);
        assert_eq!(decoded.operations.queue_length, 2);
        assert_eq!(decoded.progress, snapshot.progress);
    }

    #[test]
    fn node_heal_status_rejects_unknown_version() {
        let mut snapshot = NodeHealStatusSnapshot::for_test(
            false,
            false,
            BackgroundHealInfo::default(),
            HealOperationsSnapshot::default(),
            None,
        );
        snapshot.version += 1;

        let encoded = encode_node_heal_status(&snapshot).expect("snapshot should encode");
        let err = decode_node_heal_status(&encoded).expect_err("unknown version should fail closed");
        assert!(err.contains("unsupported node heal status version"));
    }

    #[test]
    fn node_heal_status_accepts_fixed_v1_wire_keys() {
        let fixture = serde_json::json!({
            "version": 1,
            "servicesEnabled": true,
            "initialized": true,
            "info": {"bitrotStartTime": null, "bitrotStartCycle": 9, "currentScanMode": 1},
            "operations": {
                "queueLength": 2, "activeTasks": 1, "retryingTasks": 0,
                "queuedByPriority": {"low": 0, "normal": 2, "high": 0, "urgent": 0},
                "activeByPriority": {"low": 0, "normal": 0, "high": 1, "urgent": 0},
                "retryingByPriority": {"low": 0, "normal": 0, "high": 0, "urgent": 0},
                "queuedBySource": {"scanner": 2, "admin": 0, "autoHeal": 0, "internal": 0, "readRepair": 0},
                "activeBySource": {"scanner": 0, "admin": 1, "autoHeal": 0, "internal": 0, "readRepair": 0},
                "retryingBySource": {"scanner": 0, "admin": 0, "autoHeal": 0, "internal": 0, "readRepair": 0}
            },
            "progress": null
        });
        let encoded = rmp_serde::to_vec_named(&fixture).expect("fixture should encode");
        let decoded = decode_node_heal_status(&encoded).expect("fixed v1 fixture should decode");
        assert_eq!(decoded.info().bitrot_start_cycle, 9);
        assert_eq!(decoded.operations.queue_length, 2);
    }

    #[test]
    fn node_heal_status_rejects_nested_unknown_trailing_truncated_and_oversized_data() {
        let mut fixture = serde_json::json!({
            "version": 1, "servicesEnabled": true, "initialized": true,
            "info": {"bitrotStartTime": null, "bitrotStartCycle": 0, "currentScanMode": 0, "unknown": true},
            "operations": HealOperationsSnapshot::default(), "progress": null
        });
        let encoded = rmp_serde::to_vec_named(&fixture).expect("fixture should encode");
        assert!(
            decode_node_heal_status(&encoded)
                .expect_err("nested unknown field must fail")
                .contains("unknown")
        );

        fixture["info"].as_object_mut().expect("info object").remove("unknown");
        let mut encoded = rmp_serde::to_vec_named(&fixture).expect("fixture should encode");
        encoded.push(0);
        assert!(
            decode_node_heal_status(&encoded)
                .expect_err("trailing data must fail")
                .contains("trailing")
        );
        encoded.truncate(encoded.len() / 2);
        assert!(decode_node_heal_status(&encoded).is_err());
        assert!(
            decode_node_heal_status(&vec![0; NODE_HEAL_STATUS_MAX_SIZE + 1])
                .expect_err("oversized status must fail")
                .contains("size limit")
        );
    }
}
