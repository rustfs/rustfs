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
pub mod object;
pub mod operation;
pub mod queue;
pub mod resync;
pub mod rule;
pub mod runtime;
pub mod stats;
pub mod tagging;

pub use config::{ObjectOpts, ReplicationConfigurationExt};
pub use delete::{
    DeletedObjectReplicationInfo, is_retryable_delete_replication_head_error, is_version_delete_replication,
    should_retry_delete_marker_purge,
};
pub use mrf::{MrfOpKind, MrfReplicateEntry, decode_mrf_file, encode_mrf_file};
pub use object::{
    ReplicationSourceObject, ReplicationTargetObject, content_matches_by_etag, replication_action_for_target,
    target_is_newer_than_source_null_version,
};
pub use operation::{MustReplicateOptions, is_ssec_encrypted};
pub use queue::{ReplicationHealQueueResult, ReplicationOperation, ReplicationPriority, ReplicationQueueAdmission};
pub use resync::{
    BucketReplicationResyncStatus, Error, Result, ResyncOpts, ResyncStatusType, TargetReplicationResyncStatus,
    decode_resync_file, encode_resync_file, is_version_id_mismatch, resync_state_accepts_update, should_count_head_proxy_failure,
};
pub use rule::ReplicationRuleExt;
pub use runtime::{
    LARGE_WORKER_COUNT, MIN_LARGE_OBJ_SIZE, MRF_WORKER_AUTO_DEFAULT, MRF_WORKER_MAX_LIMIT, MRF_WORKER_MIN_LIMIT,
    ReplicationPoolOpts, ReplicationWorkerCounts, WORKER_AUTO_DEFAULT, WORKER_MAX_LIMIT, WORKER_MIN_LIMIT, initial_worker_counts,
    mrf_worker_size_to_count, next_large_worker_count, next_mrf_worker_count, next_regular_worker_count, resized_worker_counts,
    should_grow_large_workers, should_queue_large_object, worker_counts_for_priority,
};
pub use rustfs_filemeta::{
    REPLICATE_EXISTING, REPLICATE_EXISTING_DELETE, REPLICATE_HEAL, REPLICATE_HEAL_DELETE, REPLICATE_INCOMING_DELETE,
    ReplicateDecision, ReplicateObjectInfo, ReplicateTargetDecision, ReplicatedInfos, ReplicatedTargetInfo, ReplicationAction,
    ReplicationState, ReplicationStatusType, ReplicationType, ReplicationWorkerOperation, ResyncDecision, ResyncTargetDecision,
    VersionPurgeStatusType, get_replication_state, parse_replicate_decision, replication_statuses_map, target_reset_header,
    version_purge_statuses_map,
};
pub use stats::{
    ActiveWorkerStat, BucketReplicationStat, BucketReplicationStats, BucketStats, ExponentialMovingAverage, FailStats,
    FailedMetric, InQueueMetric, InQueueStats, LatencyStats, ProxyMetric, ProxyStatsCache, QueueCache, QueueNode, QueueStats,
    SRMetricsSummary, XferStats,
};
pub use tagging::{ReplicationTagFilter, decode_tags_to_map};
