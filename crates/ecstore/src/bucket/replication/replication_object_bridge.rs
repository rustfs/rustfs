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

use super::replication_error_boundary::Result;
use super::replication_filemeta_boundary::{ReplicateDecision, ReplicatedTargetInfo, ReplicationStatusType, ReplicationType};
use super::replication_metadata_boundary::ReplicationInstanceContext;
use super::replication_object_config::{
    DeleteReplicationConfigSnapshot, check_replicate_delete, check_replicate_delete_strict, check_replicate_delete_with_snapshot,
    get_must_replicate_options, load_delete_replication_config_in, load_delete_request_config_in, must_replicate,
};
use super::replication_object_decision_boundary::MustReplicateOptions;
use super::replication_pool::{schedule_replication, schedule_replication_delete};
use super::replication_queue_boundary::DeletedObjectReplicationInfo;
use super::replication_storage_boundary::{
    DeletedObject, ObjectInfo, ObjectOptions, ObjectToDelete, ReplicationObjectStore, ReplicationStorage,
    deleted_object_for_replication,
};

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

    pub async fn check_delete_strict(
        bucket: &str,
        object: &ObjectToDelete,
        source: &ObjectInfo,
        opts: &ObjectOptions,
        get_error: Option<String>,
    ) -> Result<ReplicateDecision> {
        check_replicate_delete_strict(bucket, object, source, opts, get_error).await
    }

    pub async fn delete_request_config(api: &ReplicationObjectStore, bucket: &str) -> Result<DeleteReplicationConfigSnapshot> {
        load_delete_request_config_in(&api.ctx, bucket).await
    }

    pub(crate) async fn delete_request_config_in(
        ctx: &ReplicationInstanceContext,
        bucket: &str,
    ) -> Result<DeleteReplicationConfigSnapshot> {
        load_delete_request_config_in(ctx, bucket).await
    }

    #[allow(
        dead_code,
        reason = "declared boundary surface for the ECStore replication split plan; no caller in this port (backlog#1823)"
    )]
    pub(crate) async fn delete_config_snapshot_in(
        ctx: &ReplicationInstanceContext,
        bucket: &str,
        opts: &ObjectOptions,
    ) -> Result<DeleteReplicationConfigSnapshot> {
        load_delete_replication_config_in(ctx, bucket, opts).await
    }

    pub fn has_active_delete_rule(snapshot: &DeleteReplicationConfigSnapshot, object: &str) -> bool {
        snapshot.has_active_rule(object)
    }

    pub fn force_delete_target_set(
        snapshot: &DeleteReplicationConfigSnapshot,
        prefix: &str,
    ) -> Option<(Vec<String>, time::OffsetDateTime)> {
        snapshot.force_delete_target_set(prefix)
    }

    pub fn check_delete_with_snapshot(
        object: &ObjectToDelete,
        source: &ObjectInfo,
        opts: &ObjectOptions,
        source_error: bool,
        snapshot: &DeleteReplicationConfigSnapshot,
    ) -> ReplicateDecision {
        check_replicate_delete_with_snapshot(object, source, opts, source_error, snapshot)
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

    pub async fn schedule_deletes(delete_objects: &[DeletedObjectReplicationInfo]) {
        if let Some(pool) = super::runtime_boundary::replication_pool() {
            let _ = pool.queue_replica_delete_batch(delete_objects).await;
        }

        if let Some(stats) = super::runtime_boundary::replication_stats() {
            for delete_object in delete_objects {
                if let Some(rs) = &delete_object.delete_object.replication_state {
                    for k in rs.targets.keys() {
                        let ri = ReplicatedTargetInfo {
                            arn: k.clone(),
                            size: 0,
                            duration: std::time::Duration::default(),
                            op_type: ReplicationType::Delete,
                            ..Default::default()
                        };
                        stats
                            .update(&delete_object.bucket, &ri, ReplicationStatusType::Pending, ReplicationStatusType::Empty)
                            .await;
                    }
                }
            }
        }
    }

    pub async fn schedule_storage_delete(delete_object: DeletedObject, bucket: String, event_type: String) {
        Self::schedule_delete(DeletedObjectReplicationInfo {
            delete_object: deleted_object_for_replication(delete_object),
            bucket,
            event_type,
            ..Default::default()
        })
        .await;
    }

    pub async fn schedule_storage_deletes(delete_objects: Vec<DeletedObject>, bucket: String, event_type: String) {
        let delete_objects = delete_objects
            .into_iter()
            .map(|delete_object| DeletedObjectReplicationInfo {
                delete_object: deleted_object_for_replication(delete_object),
                bucket: bucket.clone(),
                event_type: event_type.clone(),
                ..Default::default()
            })
            .collect::<Vec<_>>();
        Self::schedule_deletes(&delete_objects).await;
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
