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
    REPLICATE_EXISTING, REPLICATE_EXISTING_DELETE, REPLICATE_HEAL_DELETE, REPLICATE_INCOMING_DELETE, ReplicateDecision,
    ReplicateTargetDecision, ReplicatedInfos, ReplicatedTargetInfo, ReplicationAction, ReplicationState,
    ReplicationWorkerOperation, ResyncDecision, get_replication_state, parse_replicate_decision, replication_statuses_map,
    target_reset_header, version_purge_statuses_map,
};
pub use rustfs_replication::{ReplicateObjectInfo, ReplicationStatusType, ReplicationType, VersionPurgeStatusType};
