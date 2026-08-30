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
mod replication_proxy;
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
pub(crate) mod replication_timing;
mod replication_versioning_boundary;
mod runtime_boundary;

pub use replication_config_boundary::{
    ObjectOpts, OperatorRuleContract, REMOTE_TARGET_CAPABILITY_CONTRACT_VERSION, REMOTE_TARGET_UNSUPPORTED_FIELDS,
    REMOTE_TARGET_WRITABLE_FIELDS, REPLICATION_CAPABILITY_CONTRACT_VERSION, REPLICATION_READ_ONLY_HISTORICAL_FIELDS,
    REPLICATION_WRITABLE_FIELDS, ReplicationConfigStructureError, ReplicationConfigurationExt, ReplicationTargetValidationError,
    assign_site_replication_rule_priorities, invalid_replication_config_status_field, is_site_replication_role,
    is_site_replication_rule, merge_incoming_replication_config, merge_user_replication_config,
    replication_target_arn_deployment_id, replication_target_arns, should_remove_replication_target,
    site_replication_rule_deployment_id, unsupported_replication_config_field, validate_replication_config_structure,
    validate_replication_config_target_arns,
};
pub(crate) use replication_filemeta_boundary::version_purge_statuses_map;
pub use replication_filemeta_boundary::{
    MrfOpKind, MrfReplicateEntry, REPLICATE_INCOMING_DELETE, ReplicateDecision, ReplicateObjectInfo, ReplicationState,
    ReplicationStatusType, ReplicationType, VersionPurgeStatusType, replication_state_to_filemeta,
    replication_status_to_filemeta, replication_statuses_map, version_purge_status_to_filemeta,
};
pub(crate) use replication_filemeta_boundary::{
    replication_state_from_filemeta, replication_status_from_filemeta, version_purge_status_from_filemeta,
};
pub(crate) use replication_lifecycle_bridge::ReplicationLifecycleBridge;
pub(crate) use replication_migration_bridge::ReplicationMigrationBridge;
pub use replication_object_bridge::ReplicationObjectBridge;
pub use replication_object_config::{DeleteReplicationConfigSnapshot, ReplicationConfig};
pub use replication_object_decision_boundary::{
    MustReplicateOptions, ReplicationDeleteScheduleInput, ReplicationDeleteStateSource, delete_replication_state_from_config,
    delete_replication_version_id, should_schedule_delete_replication, should_use_existing_delete_replication_info,
    should_use_existing_delete_replication_source,
};
pub use replication_pool::{
    DurableMrfBacklog, DynReplicationPool, ReplicationPoolTrait, commit_force_delete_intent, complete_force_delete_intent,
    get_global_replication_pool, get_global_replication_stats, init_background_replication, persist_force_delete_intent,
    read_durable_mrf_backlog, resync_start_conflict_id,
};
pub use replication_proxy::get_proxy_targets;
pub use replication_queue_boundary::{
    DeletedObjectReplicationInfo, ReplicationBatchAdmission, ReplicationHealQueueResult, ReplicationOperation,
    ReplicationPriority, ReplicationQueueAdmission,
};
pub use replication_resync_boundary::ResyncStatusType;
pub use replication_resync_boundary::{BucketReplicationResyncStatus, ResyncOpts, TargetReplicationResyncStatus};
pub use replication_scanner_bridge::ReplicationScannerBridge;
pub use replication_state::{ReplicationStats, RuntimeReplicationTargetBacklog};
pub use replication_stats_boundary::{BucketReplicationStat, BucketReplicationStats, BucketStats, InQueueMetric, XferStats};
pub use replication_storage_boundary::{ReplicationObjectIO, ReplicationStorage};
pub use replication_target_boundary::SsecPassthroughCapability;
pub(crate) use replication_target_config_bridge::ReplicationTargetConfigBridge;
