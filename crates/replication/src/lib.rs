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
mod filemeta;
mod http;
pub mod mrf;
pub mod multipart;
pub mod object;
pub mod operation;
pub mod queue;
pub mod resync;
pub mod rule;
pub mod runtime;
pub mod stats;
mod storage_api;
pub mod tagging;

pub use config::{
    ObjectOpts, ReplicationConfigurationExt, ReplicationTargetValidationError, active_replication_rule_destination_arns,
    replication_target_arns, should_remove_replication_target, validate_replication_config_target_arns,
};
pub use delete::{
    DeletedObjectReplicationInfo, is_retryable_delete_replication_head_error, is_version_delete_replication,
    should_retry_delete_marker_purge,
};
pub use filemeta::{
    REPLICATE_EXISTING, REPLICATE_EXISTING_DELETE, REPLICATE_HEAL, REPLICATE_HEAL_DELETE, REPLICATE_INCOMING,
    REPLICATE_INCOMING_DELETE, REPLICATE_MRF, REPLICATE_QUEUED, REPLICATION_RESET, REPLICATION_STATUS, ReplicateDecision,
    ReplicateObjectInfo, ReplicateTargetDecision, ReplicatedInfos, ReplicatedTargetInfo, ReplicationAction, ReplicationState,
    ReplicationStatusType, ReplicationType, ReplicationWorkerOperation, ResyncDecision, ResyncTargetDecision,
    VersionPurgeStatusType, get_replication_state, parse_replicate_decision, replication_statuses_map, target_reset_header,
    version_purge_statuses_map,
};
pub use mrf::{MrfOpKind, MrfReplicateEntry, decode_mrf_file, encode_mrf_file};
pub use multipart::{
    ReplicationMultipartPartInput, ReplicationMultipartPartPlan, ReplicationMultipartPlanError, ReplicationMultipartRange,
    replication_multipart_complete_actual_size, replication_multipart_part_plan,
};
pub use object::{
    ReplicationSourceObject, ReplicationTargetObject, content_matches_by_etag, replication_action_for_target,
    replication_etags_match, target_is_newer_than_source_null_version,
};
pub use operation::{
    MustReplicateOptions, ReplicationDeleteScheduleInput, ReplicationDeleteSource, ReplicationDeleteStateSource,
    ReplicationResyncTargetObject, delete_replication_missing_source_decision, delete_replication_object_opts,
    delete_replication_state_from_config, delete_replication_version_id, heal_uses_delete_replication_path, is_ssec_encrypted,
    resync_target_for_object, should_schedule_delete_replication, should_use_existing_delete_replication_info,
    should_use_existing_delete_replication_source,
};
pub use queue::{
    ReplicationHealQueueAction, ReplicationHealQueueResult, ReplicationHealResyncDeletes, ReplicationOperation,
    ReplicationPriority, ReplicationQueueAdmission, ReplicationWorkerQueue, mrf_save_admission, replication_heal_queue_action,
    worker_queue_for_replication_type,
};
pub use resync::{
    BucketReplicationResyncStatus, Error, Result, ResyncOpts, ResyncStatusType, TargetReplicationResyncStatus,
    decode_resync_file, encode_resync_file, is_version_id_mismatch, resync_state_accepts_update, should_auto_resume_resync,
    should_count_head_proxy_failure,
};
pub use rule::ReplicationRuleExt;
pub use runtime::{
    LARGE_WORKER_COUNT, MIN_LARGE_OBJ_SIZE, MRF_WORKER_AUTO_DEFAULT, MRF_WORKER_MAX_LIMIT, MRF_WORKER_MIN_LIMIT,
    ReplicationBackpressureRecommendation, ReplicationBackpressureResize, ReplicationBackpressureState, ReplicationPoolOpts,
    ReplicationWorkerCounts, ReplicationWorkerResize, WORKER_AUTO_DEFAULT, WORKER_MAX_LIMIT, WORKER_MIN_LIMIT,
    initial_worker_counts, large_worker_backpressure_resize, mrf_worker_size_to_count, next_large_worker_count,
    next_mrf_worker_count, next_regular_worker_count, replication_backpressure_recommendation, resized_worker_counts,
    should_grow_large_workers, should_queue_large_object, worker_counts_for_priority,
};
pub use stats::{
    ActiveWorkerStat, BucketReplicationStat, BucketReplicationStats, BucketStats, ExponentialMovingAverage, FailStats,
    FailedMetric, InQueueMetric, InQueueStats, LatencyStats, ProxyMetric, ProxyStatsCache, QueueCache, QueueNode, QueueStats,
    SRMetricsSummary, XferStats,
};
pub use storage_api::{DeletedObject, ObjectToDelete};
pub use tagging::{ReplicationTagFilter, decode_tags_to_map};
