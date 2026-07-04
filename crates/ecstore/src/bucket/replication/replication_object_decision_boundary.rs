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
    MustReplicateOptions, ReplicationDeleteScheduleInput, ReplicationDeleteStateSource, delete_replication_state_from_config,
    delete_replication_version_id, should_schedule_delete_replication, should_use_existing_delete_replication_info,
    should_use_existing_delete_replication_source,
};
pub(crate) use rustfs_replication::{
    ReplicationDeleteSource, ReplicationMultipartPartInput, ReplicationResyncTargetObject,
    delete_replication_missing_source_decision, delete_replication_object_opts, heal_uses_delete_replication_path,
    is_retryable_delete_replication_head_error, is_version_delete_replication, replication_etags_match,
    replication_multipart_complete_actual_size, replication_multipart_part_plan, resync_target_for_object,
    should_retry_delete_marker_purge,
};
