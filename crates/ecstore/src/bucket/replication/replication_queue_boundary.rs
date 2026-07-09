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

pub use rustfs_replication::{
    DeletedObjectReplicationInfo, ReplicationHealQueueResult, ReplicationOperation, ReplicationPriority,
    ReplicationQueueAdmission,
};
pub(crate) use rustfs_replication::{
    LARGE_WORKER_COUNT, ReplicationBackpressureRecommendation, ReplicationBackpressureState, ReplicationHealQueueAction,
    ReplicationHealResyncDeletes, ReplicationPoolOpts, ReplicationWorkerQueue, WORKER_MAX_LIMIT, initial_worker_counts,
    large_worker_backpressure_resize, mrf_worker_size_to_count, replication_backpressure_recommendation,
    replication_heal_queue_action, resized_worker_counts, should_queue_large_object, worker_queue_for_replication_type,
};
