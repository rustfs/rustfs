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
use rustfs_replication::{ReplicateDecision, ReplicationStatusType};

use crate::bucket::lifecycle::lifecycle::ObjectOpts;
use crate::bucket::replication::{ReplicationLifecycleBridge, ReplicationLifecycleConfig};
use crate::object_api::{ObjectInfo, ObjectOptions};
use crate::storage_api_contracts::object::{DeletedObject, ObjectToDelete};

pub(crate) type LifecycleReplicationConfig = ReplicationLifecycleConfig;

pub(crate) fn has_pending_version_purge(obj: &ObjectOpts) -> bool {
    obj.version_purge_status.is_pending()
}

pub(crate) fn has_pending_object_replication(obj: &ObjectOpts) -> bool {
    replication_status_blocks_lifecycle(&obj.replication_status)
}

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

pub(crate) async fn check_delete_replication(
    bucket: &str,
    object: ObjectToDelete,
    source: &ObjectInfo,
    opts: &ObjectOptions,
) -> ReplicateDecision {
    ReplicationLifecycleBridge::check_delete_replication(bucket, &object, source, opts).await
}

pub(crate) async fn schedule_delete(bucket: String, delete_object: DeletedObject) {
    ReplicationLifecycleBridge::schedule_delete(bucket, delete_object).await;
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use rustfs_common::metrics::IlmAction;
    use rustfs_replication::{ReplicationStatusType, VersionPurgeStatusType};

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
}
