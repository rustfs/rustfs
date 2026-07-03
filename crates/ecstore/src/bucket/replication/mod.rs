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

pub mod datatypes;
mod replication_bandwidth_boundary;
mod replication_config_boundary;
mod replication_config_store;
mod replication_error_boundary;
mod replication_event_sink;
mod replication_filemeta_boundary;
mod replication_lifecycle_bridge;
mod replication_lock_boundary;
mod replication_logging;
mod replication_metadata_boundary;
mod replication_migration_bridge;
mod replication_msgp_boundary;
mod replication_object_bridge;
mod replication_object_config;
mod replication_object_decision_boundary;
pub(crate) mod replication_pool;
mod replication_queue_boundary;
mod replication_resync_boundary;
mod replication_resyncer;
mod replication_scanner_bridge;
mod replication_state;
mod replication_stats_boundary;
mod replication_storage_boundary;
mod replication_tagging_boundary;
mod replication_target_boundary;
mod replication_target_config_bridge;
mod replication_versioning_boundary;
mod runtime_boundary;

pub use datatypes::ResyncStatusType;
pub use replication_config_boundary::{ObjectOpts, ReplicationConfigurationExt};
#[cfg(test)]
pub(crate) use replication_filemeta_boundary::ReplicateTargetDecision;
pub(crate) use replication_filemeta_boundary::{
    ReplicateDecision, ReplicationState, ReplicationStatusType, VersionPurgeStatusType, replication_statuses_map,
    version_purge_statuses_map,
};
pub(crate) use replication_lifecycle_bridge::{ReplicationLifecycleBridge, ReplicationLifecycleConfig};
pub(crate) use replication_migration_bridge::ReplicationMigrationBridge;
pub use replication_object_bridge::ReplicationObjectBridge;
pub use replication_object_config::ReplicationConfig;
pub use replication_object_decision_boundary::MustReplicateOptions;
pub use replication_pool::{
    DynReplicationPool, ReplicationPoolTrait, get_global_replication_pool, get_global_replication_stats,
    init_background_replication,
};
pub use replication_queue_boundary::{
    DeletedObjectReplicationInfo, ReplicationHealQueueResult, ReplicationOperation, ReplicationPriority,
    ReplicationQueueAdmission,
};
pub use replication_resync_boundary::{BucketReplicationResyncStatus, ResyncOpts, TargetReplicationResyncStatus};
pub use replication_scanner_bridge::ReplicationScannerBridge;
pub use replication_state::ReplicationStats;
pub use replication_stats_boundary::BucketStats;
pub use replication_storage_boundary::{ReplicationObjectIO, ReplicationStorage};
pub(crate) use replication_target_config_bridge::ReplicationTargetConfigBridge;
