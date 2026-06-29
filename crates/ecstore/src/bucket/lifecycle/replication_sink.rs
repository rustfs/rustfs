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

use rustfs_filemeta::{REPLICATE_INCOMING_DELETE, ReplicateDecision};
use s3s::dto::ReplicationConfiguration;

use crate::bucket::lifecycle::lifecycle::ObjectOpts;
use crate::bucket::replication::{self, DeletedObjectReplicationInfo, ReplicationConfig, ReplicationConfigurationExt as _};
use crate::object_api::{ObjectInfo, ObjectOptions};
use crate::storage_api_contracts::object::{DeletedObject, ObjectToDelete};

pub(crate) type LifecycleReplicationConfig = ReplicationConfig;

pub(crate) fn new_replication_config(config: ReplicationConfiguration) -> LifecycleReplicationConfig {
    ReplicationConfig::new(Some(config), None)
}

pub(crate) fn has_pending_version_purge(config: &LifecycleReplicationConfig, obj: &ObjectOpts) -> bool {
    config
        .config
        .as_ref()
        .is_some_and(|config| config.has_active_rules(obj.name.as_str(), true))
        && !obj.version_purge_status.is_empty()
}

pub(crate) async fn check_delete_replication(
    bucket: &str,
    object: ObjectToDelete,
    source: &ObjectInfo,
    opts: &ObjectOptions,
) -> ReplicateDecision {
    replication::check_replicate_delete(bucket, &object, source, opts, None).await
}

pub(crate) async fn schedule_delete(bucket: String, delete_object: DeletedObject) {
    replication::schedule_replication_delete(DeletedObjectReplicationInfo {
        delete_object,
        bucket,
        event_type: REPLICATE_INCOMING_DELETE.to_string(),
        ..Default::default()
    })
    .await;
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use rustfs_filemeta::{ReplicationStatusType, VersionPurgeStatusType};
    use s3s::dto::{Destination, ReplicationRule, ReplicationRuleStatus};

    use super::*;

    fn replication_rule() -> ReplicationRule {
        ReplicationRule {
            delete_marker_replication: None,
            delete_replication: None,
            destination: Destination {
                bucket: "arn:aws:s3:::target-bucket".to_string(),
                ..Default::default()
            },
            existing_object_replication: None,
            filter: None,
            id: Some("rule".to_string()),
            prefix: Some(String::new()),
            priority: Some(1),
            source_selection_criteria: None,
            status: ReplicationRuleStatus::from_static(ReplicationRuleStatus::ENABLED),
        }
    }

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

    #[test]
    fn has_pending_version_purge_preserves_replication_active_rule_behavior() {
        let config = new_replication_config(ReplicationConfiguration {
            role: String::new(),
            rules: vec![replication_rule()],
        });

        assert!(has_pending_version_purge(&config, &object_opts(VersionPurgeStatusType::Pending)));
        assert!(!has_pending_version_purge(&config, &object_opts(VersionPurgeStatusType::default())));
    }
}
