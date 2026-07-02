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

pub mod config;
pub mod delete;
pub mod mrf;
pub mod operation;
pub mod queue;
pub mod resync;
pub mod rule;
pub mod tagging;

pub use config::{ObjectOpts, ReplicationConfigurationExt};
pub use delete::DeletedObjectReplicationInfo;
pub use mrf::{MrfOpKind, MrfReplicateEntry, decode_mrf_file, encode_mrf_file};
pub use operation::{MustReplicateOptions, is_ssec_encrypted};
pub use queue::{ReplicationHealQueueResult, ReplicationOperation, ReplicationPriority, ReplicationQueueAdmission};
pub use resync::{
    BucketReplicationResyncStatus, Error, Result, ResyncOpts, ResyncStatusType, TargetReplicationResyncStatus,
    decode_resync_file, encode_resync_file,
};
pub use rule::ReplicationRuleExt;
pub use rustfs_filemeta::{
    REPLICATE_EXISTING, REPLICATE_EXISTING_DELETE, REPLICATE_HEAL, REPLICATE_HEAL_DELETE, REPLICATE_INCOMING_DELETE,
    ReplicateDecision, ReplicateObjectInfo, ReplicateTargetDecision, ReplicatedInfos, ReplicatedTargetInfo, ReplicationAction,
    ReplicationState, ReplicationStatusType, ReplicationType, ReplicationWorkerOperation, ResyncDecision, ResyncTargetDecision,
    VersionPurgeStatusType, get_replication_state, parse_replicate_decision, replication_statuses_map, target_reset_header,
    version_purge_statuses_map,
};
pub use tagging::{ReplicationTagFilter, decode_tags_to_map};
