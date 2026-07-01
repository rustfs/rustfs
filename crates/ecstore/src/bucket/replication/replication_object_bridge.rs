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

use std::{collections::HashMap, sync::Arc};

use super::replication_filemeta_boundary::{ReplicateDecision, ReplicationStatusType, ReplicationType};
use super::replication_pool::{schedule_replication, schedule_replication_delete};
use super::replication_resyncer::{
    DeletedObjectReplicationInfo, MustReplicateOptions, check_replicate_delete, get_must_replicate_options, must_replicate,
};
use super::replication_storage_boundary::{ObjectInfo, ObjectOptions, ObjectToDelete, ReplicationStorage};

pub struct ReplicationObjectBridge;

impl ReplicationObjectBridge {
    pub fn must_replicate_options(
        user_defined: &HashMap<String, String>,
        user_tags: String,
        status: ReplicationStatusType,
        op_type: ReplicationType,
        opts: ObjectOptions,
    ) -> MustReplicateOptions {
        get_must_replicate_options(user_defined, user_tags, status, op_type, opts)
    }

    pub async fn must_replicate(bucket: &str, object: &str, options: MustReplicateOptions) -> ReplicateDecision {
        must_replicate(bucket, object, options).await
    }

    pub async fn check_delete(
        bucket: &str,
        object: &ObjectToDelete,
        source: &ObjectInfo,
        opts: &ObjectOptions,
        get_error: Option<String>,
    ) -> ReplicateDecision {
        check_replicate_delete(bucket, object, source, opts, get_error).await
    }

    pub async fn schedule_object<S: ReplicationStorage>(
        object: ObjectInfo,
        storage: Arc<S>,
        decision: ReplicateDecision,
        op_type: ReplicationType,
    ) {
        schedule_replication(object, storage, decision, op_type).await;
    }

    pub async fn schedule_delete(delete_object: DeletedObjectReplicationInfo) {
        schedule_replication_delete(delete_object).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn object_bridge_builds_operation_specific_options() {
        let user_defined = HashMap::new();

        let metadata = ReplicationObjectBridge::must_replicate_options(
            &user_defined,
            String::new(),
            ReplicationStatusType::Empty,
            ReplicationType::Metadata,
            ObjectOptions::default(),
        );
        assert!(metadata.is_metadata_replication());
        assert!(!metadata.is_existing_object_replication());

        let existing = ReplicationObjectBridge::must_replicate_options(
            &user_defined,
            String::new(),
            ReplicationStatusType::Empty,
            ReplicationType::ExistingObject,
            ObjectOptions::default(),
        );
        assert!(existing.is_existing_object_replication());
        assert!(!existing.is_metadata_replication());
    }
}
