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

pub(crate) use rustfs_replication::{MrfOpKind, MrfReplicateEntry};
pub(crate) use rustfs_replication::{
    REPLICATE_EXISTING, REPLICATE_EXISTING_DELETE, REPLICATE_HEAL_DELETE, ReplicateTargetDecision, ReplicatedInfos,
    ReplicatedTargetInfo, ReplicationAction, ReplicationWorkerOperation, ResyncDecision, get_replication_state,
    parse_replicate_decision, target_reset_header, version_purge_statuses_map,
};
pub use rustfs_replication::{
    REPLICATE_INCOMING_DELETE, ReplicateDecision, ReplicateObjectInfo, ReplicationState, ReplicationStatusType, ReplicationType,
    VersionPurgeStatusType, replication_statuses_map,
};

pub(crate) fn replication_status_from_filemeta(status: rustfs_filemeta::ReplicationStatusType) -> ReplicationStatusType {
    ReplicationStatusType::from(status.as_str())
}

pub(crate) fn version_purge_status_from_filemeta(status: rustfs_filemeta::VersionPurgeStatusType) -> VersionPurgeStatusType {
    VersionPurgeStatusType::from(status.as_str())
}

pub(crate) fn replication_state_from_filemeta(state: &rustfs_filemeta::ReplicationState) -> ReplicationState {
    ReplicationState {
        replica_timestamp: state.replica_timestamp,
        replica_status: replication_status_from_filemeta(state.replica_status.clone()),
        delete_marker: state.delete_marker,
        replication_timestamp: state.replication_timestamp,
        replication_status_internal: state.replication_status_internal.clone(),
        version_purge_status_internal: state.version_purge_status_internal.clone(),
        replicate_decision_str: state.replicate_decision_str.clone(),
        targets: state
            .targets
            .iter()
            .map(|(arn, status)| (arn.clone(), replication_status_from_filemeta(status.clone())))
            .collect(),
        purge_targets: state
            .purge_targets
            .iter()
            .map(|(arn, status)| (arn.clone(), version_purge_status_from_filemeta(status.clone())))
            .collect(),
        reset_statuses_map: state.reset_statuses_map.clone(),
    }
}

pub fn replication_status_to_filemeta(status: ReplicationStatusType) -> rustfs_filemeta::ReplicationStatusType {
    rustfs_filemeta::ReplicationStatusType::from(status.as_str())
}

pub fn version_purge_status_to_filemeta(status: VersionPurgeStatusType) -> rustfs_filemeta::VersionPurgeStatusType {
    rustfs_filemeta::VersionPurgeStatusType::from(status.as_str())
}

pub fn replication_state_to_filemeta(state: &ReplicationState) -> rustfs_filemeta::ReplicationState {
    rustfs_filemeta::ReplicationState {
        replica_timestamp: state.replica_timestamp,
        replica_status: replication_status_to_filemeta(state.replica_status.clone()),
        delete_marker: state.delete_marker,
        replication_timestamp: state.replication_timestamp,
        replication_status_internal: state.replication_status_internal.clone(),
        version_purge_status_internal: state.version_purge_status_internal.clone(),
        replicate_decision_str: state.replicate_decision_str.clone(),
        targets: state
            .targets
            .iter()
            .map(|(arn, status)| (arn.clone(), replication_status_to_filemeta(status.clone())))
            .collect(),
        purge_targets: state
            .purge_targets
            .iter()
            .map(|(arn, status)| (arn.clone(), version_purge_status_to_filemeta(status.clone())))
            .collect(),
        reset_statuses_map: state.reset_statuses_map.clone(),
    }
}
