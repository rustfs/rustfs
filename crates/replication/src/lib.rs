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
    ObjectOpts, OperatorRuleContract, REMOTE_TARGET_CAPABILITY_CONTRACT_VERSION, REMOTE_TARGET_UNSUPPORTED_FIELDS,
    REMOTE_TARGET_WRITABLE_FIELDS, REPLICATION_CAPABILITY_CONTRACT_VERSION, REPLICATION_READ_ONLY_HISTORICAL_FIELDS,
    REPLICATION_WRITABLE_FIELDS, ReplicationConfigStructureError, ReplicationConfigurationExt, ReplicationTargetValidationError,
    active_replication_rule_destination_arns, assign_site_replication_rule_priorities, invalid_replication_config_status_field,
    is_reconciler_owned_site_replication_rule, is_site_replication_role, is_site_replication_rule,
    merge_incoming_replication_config, merge_user_replication_config, replication_target_arn_deployment_id,
    replication_target_arns, should_remove_replication_target, site_replication_rule_deployment_id,
    unsupported_replication_config_field, validate_replication_config_structure, validate_replication_config_target_arns,
};
pub use delete::{
    DeletedObjectReplicationInfo, delete_marker_purge_mrf_entry, delete_marker_purge_version_id,
    is_retryable_delete_replication_head_error, is_version_delete_replication, replicate_delete_outcome,
    resync_existing_delete_replication_info, should_retry_delete_marker_purge, target_delete_version_id,
};
pub use filemeta::{
    NULL_VERSION_ID, REPLICATE_EXISTING, REPLICATE_EXISTING_DELETE, REPLICATE_HEAL, REPLICATE_HEAL_DELETE, REPLICATE_INCOMING,
    REPLICATE_INCOMING_DELETE, REPLICATE_MRF, REPLICATE_QUEUED, REPLICATION_RESET, REPLICATION_STATUS, ReplicateDecision,
    ReplicateObjectInfo, ReplicateTargetDecision, ReplicatedInfos, ReplicatedTargetInfo, ReplicationAction, ReplicationState,
    ReplicationStatusType, ReplicationType, ReplicationWorkerOperation, ResyncDecision, ResyncTargetDecision,
    VersionPurgeStatusType, get_replication_state, parse_replicate_decision, replicate_decision_for_admitted_targets,
    replication_statuses_map, target_reset_header, version_purge_statuses_map,
};
pub use mrf::{
    MRF_ENVELOPE_FORMAT, MRF_ENVELOPE_VERSION, MRF_V2_FILE, MRF_V2_FORMAT, MRF_V2_NAMESPACE, MRF_V2_VERSION, MrfCapabilities,
    MrfCapability, MrfEnvelope, MrfEnvelopeError, MrfOpKind, MrfProtocolCapabilities, MrfReplicateEntry, MrfV2Capabilities,
    MrfV2Envelope, MrfV2Error, MrfV2Reader, MrfV2Readiness, decode_mrf_file, encode_mrf_file,
};
pub use multipart::{
    ReplicationMultipartPartInput, ReplicationMultipartPartPlan, ReplicationMultipartPlanError, ReplicationMultipartRange,
    replication_multipart_complete_actual_size, replication_multipart_part_plan,
};
pub use object::{
    ReplicationSourceObject, ReplicationTargetObject, SsecPassthroughCapability, SsecPassthroughGate, content_matches_by_etag,
    is_replication_target_offline_error, replication_action_for_target, replication_etags_match,
    ssec_passthrough_evidence_present, ssec_passthrough_gate, target_is_newer_than_source_null_version, version_identity_drifted,
};
pub use operation::{
    MustReplicateOptions, ReplicationDeleteScheduleInput, ReplicationDeleteSource, ReplicationDeleteStateSource,
    ReplicationResyncTargetObject, delete_replication_missing_source_decision, delete_replication_object_opts,
    delete_replication_state_from_config, delete_replication_version_id, heal_uses_delete_replication_path, is_ssec_encrypted,
    resync_target_for_object, should_schedule_delete_replication, should_use_existing_delete_replication_info,
    should_use_existing_delete_replication_source,
};
pub use queue::{
    ReplicationBatchAdmission, ReplicationHealQueueAction, ReplicationHealQueueResult, ReplicationHealResyncDeletes,
    ReplicationOperation, ReplicationPriority, ReplicationQueueAdmission, ReplicationWorkerQueue, mrf_save_admission,
    replication_heal_queue_action, worker_queue_for_replication_type,
};
pub use resync::{
    BucketReplicationResyncStatus, Error, RESYNC_FILE_MAX_BYTES, Result, ResyncOpts, ResyncStatusType,
    TargetReplicationResyncStatus, decode_resync_file, encode_resync_file, is_version_id_mismatch, resync_state_accepts_update,
    resync_status_duration, sanitize_resync_error_detail, should_auto_resume_resync, should_count_head_proxy_failure,
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
    ReplicationMetricScope, SRMetricsSummary, XferStats,
};
pub use storage_api::{DeletedObject, ObjectToDelete};
pub use tagging::{ReplicationTagFilter, decode_tags_to_map};
