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

use rustfs_common::metrics::IlmAction;

use crate::bucket::lifecycle::lifecycle::ObjectOpts;
use crate::bucket::replication::ReplicationLifecycleBridge;
pub(crate) use crate::bucket::replication::ReplicationStatusType;
#[cfg(test)]
pub(crate) use crate::bucket::replication::VersionPurgeStatusType;
pub(crate) use crate::bucket::replication::{
    DeleteReplicationConfigSnapshot, ReplicationObjectBridge, replication_state_to_filemeta,
};
use crate::storage_api_contracts::object::DeletedObject;

#[allow(
    dead_code,
    reason = "declared boundary surface for the ECStore replication split plan; no caller in this port (backlog#1823)"
)]
pub(crate) fn has_pending_version_purge(obj: &ObjectOpts) -> bool {
    obj.version_purge_status.is_pending()
}

#[allow(
    dead_code,
    reason = "declared boundary surface for the ECStore replication split plan; no caller in this port (backlog#1823)"
)]
pub(crate) fn has_pending_object_replication(obj: &ObjectOpts) -> bool {
    replication_status_blocks_lifecycle(&obj.replication_status)
}

#[allow(
    dead_code,
    reason = "declared boundary surface for the ECStore replication split plan; no caller in this port (backlog#1823)"
)]
pub(crate) fn has_pending_lifecycle_replication(obj: &ObjectOpts) -> bool {
    has_pending_object_replication(obj) || has_pending_version_purge(obj)
}

pub(crate) fn replication_status_blocks_lifecycle(status: &ReplicationStatusType) -> bool {
    matches!(status, ReplicationStatusType::Pending | ReplicationStatusType::Failed)
}

pub(crate) fn lifecycle_action_waits_for_replication(action: IlmAction) -> bool {
    matches!(
        action,
        IlmAction::DeleteAction
            | IlmAction::DeleteVersionAction
            | IlmAction::DeleteRestoredAction
            | IlmAction::DeleteRestoredVersionAction
            | IlmAction::DeleteAllVersionsAction
            | IlmAction::DelMarkerDeleteAllVersionsAction
            | IlmAction::TransitionAction
            | IlmAction::TransitionVersionAction
    )
}

pub(crate) async fn schedule_delete(bucket: String, delete_object: DeletedObject) {
    ReplicationLifecycleBridge::schedule_delete(bucket, delete_object).await;
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::bucket::replication::{DeleteReplicationConfigSnapshot, ReplicationObjectBridge};
    use crate::object_api::{ObjectInfo, ObjectOptions};
    use crate::storage_api_contracts::object::ObjectToDelete;
    use rustfs_common::metrics::IlmAction;
    use s3s::dto::{
        BucketVersioningStatus, DeleteMarkerReplication, DeleteMarkerReplicationStatus, DeleteReplication,
        DeleteReplicationStatus, Destination, ReplicationConfiguration, ReplicationRule, ReplicationRuleStatus,
        VersioningConfiguration,
    };
    use uuid::Uuid;

    use super::*;

    fn object_opts(version_purge_status: VersionPurgeStatusType) -> ObjectOpts {
        ObjectOpts {
            name: "logs/object".to_string(),
            user_tags: String::new(),
            mod_time: None,
            size: 0,
            version_id: None,
            is_latest: true,
            delete_marker: false,
            num_versions: 1,
            successor_mod_time: None,
            transition_status: String::new(),
            restore_ongoing: false,
            restore_expires: None,
            versioned: true,
            version_suspended: false,
            user_defined: HashMap::new(),
            version_purge_status,
            replication_status: ReplicationStatusType::default(),
        }
    }

    fn object_opts_with_replication_status(replication_status: ReplicationStatusType) -> ObjectOpts {
        ObjectOpts {
            replication_status,
            ..object_opts(VersionPurgeStatusType::default())
        }
    }

    #[test]
    fn pending_version_purge_blocks_lifecycle_actions_from_object_state() {
        assert!(has_pending_version_purge(&object_opts(VersionPurgeStatusType::Pending)));
        assert!(has_pending_version_purge(&object_opts(VersionPurgeStatusType::Failed)));
        assert!(!has_pending_version_purge(&object_opts(VersionPurgeStatusType::Complete)));
        assert!(!has_pending_version_purge(&object_opts(VersionPurgeStatusType::default())));
    }

    #[test]
    fn pending_replication_status_blocks_lifecycle_actions_from_object_state() {
        assert!(has_pending_object_replication(&object_opts_with_replication_status(
            ReplicationStatusType::Pending
        )));
        assert!(has_pending_object_replication(&object_opts_with_replication_status(
            ReplicationStatusType::Failed
        )));
        assert!(!has_pending_object_replication(&object_opts_with_replication_status(
            ReplicationStatusType::Completed
        )));
        assert!(!has_pending_object_replication(&object_opts_with_replication_status(
            ReplicationStatusType::Empty
        )));
    }

    #[test]
    fn lifecycle_action_waits_for_replication_for_expiration_and_transition() {
        assert!(lifecycle_action_waits_for_replication(IlmAction::DeleteAction));
        assert!(lifecycle_action_waits_for_replication(IlmAction::DeleteVersionAction));
        assert!(lifecycle_action_waits_for_replication(IlmAction::TransitionAction));
        assert!(lifecycle_action_waits_for_replication(IlmAction::TransitionVersionAction));
        assert!(!lifecycle_action_waits_for_replication(IlmAction::NoneAction));
    }

    #[test]
    fn lifecycle_delete_admission_uses_marker_and_version_switches_for_all_purges() {
        for marker_enabled in [false, true] {
            for purge_enabled in [false, true] {
                let snapshot = DeleteReplicationConfigSnapshot::from_configs_for_test(
                    VersioningConfiguration {
                        status: Some(BucketVersioningStatus::from_static(BucketVersioningStatus::ENABLED)),
                        ..Default::default()
                    },
                    Some(ReplicationConfiguration {
                        role: String::new(),
                        rules: vec![ReplicationRule {
                            delete_marker_replication: Some(DeleteMarkerReplication {
                                status: Some(if marker_enabled {
                                    DeleteMarkerReplicationStatus::from_static(DeleteMarkerReplicationStatus::ENABLED)
                                } else {
                                    DeleteMarkerReplicationStatus::from_static(DeleteMarkerReplicationStatus::DISABLED)
                                }),
                            }),
                            delete_replication: Some(DeleteReplication {
                                status: if purge_enabled {
                                    DeleteReplicationStatus::from_static(DeleteReplicationStatus::ENABLED)
                                } else {
                                    DeleteReplicationStatus::from_static(DeleteReplicationStatus::DISABLED)
                                },
                            }),
                            destination: Destination {
                                bucket: "arn:rustfs:replication:target".to_string(),
                                ..Default::default()
                            },
                            existing_object_replication: None,
                            filter: None,
                            id: Some("lifecycle-delete-switches".to_string()),
                            prefix: Some(String::new()),
                            priority: Some(1),
                            source_selection_criteria: None,
                            status: ReplicationRuleStatus::from_static(ReplicationRuleStatus::ENABLED),
                        }],
                    }),
                );
                let source = ObjectInfo {
                    bucket: "bucket".to_string(),
                    name: "logs/object".to_string(),
                    ..Default::default()
                };
                let marker = ObjectToDelete {
                    object_name: source.name.clone(),
                    ..Default::default()
                };
                let marker_opts = ObjectOptions {
                    versioned: true,
                    ..Default::default()
                };
                assert_eq!(
                    ReplicationObjectBridge::check_delete_with_snapshot(&marker, &source, &marker_opts, false, &snapshot)
                        .replicate_any(),
                    marker_enabled
                );

                for delete_marker in [false, true] {
                    for version_id in [Uuid::new_v4(), Uuid::nil()] {
                        let purge = ObjectToDelete {
                            object_name: source.name.clone(),
                            version_id: Some(version_id),
                            ..Default::default()
                        };
                        let purge_source = ObjectInfo {
                            delete_marker,
                            ..source.clone()
                        };
                        let purge_opts = ObjectOptions {
                            version_id: Some(version_id.to_string()),
                            versioned: true,
                            ..Default::default()
                        };
                        assert_eq!(
                            ReplicationObjectBridge::check_delete_with_snapshot(
                                &purge,
                                &purge_source,
                                &purge_opts,
                                false,
                                &snapshot,
                            )
                            .replicate_any(),
                            purge_enabled,
                            "delete marker={delete_marker}, version_id={version_id}"
                        );
                    }
                }
            }
        }
    }
}
