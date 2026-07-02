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

use s3s::dto::ReplicationConfiguration;

use super::config::ReplicationConfigurationExt as _;
use super::replication_filemeta_boundary::{
    REPLICATE_INCOMING_DELETE, ReplicateDecision, ReplicationState, version_purge_statuses_map,
};
use super::replication_object_config::{ReplicationConfig, check_replicate_delete};
use super::replication_storage_boundary::{DeletedObject, ObjectInfo, ObjectOptions, ObjectToDelete};
use rustfs_replication::DeletedObjectReplicationInfo;

pub(crate) type ReplicationLifecycleConfig = ReplicationConfig;

pub(crate) struct ReplicationLifecycleBridge;

impl ReplicationLifecycleBridge {
    pub(crate) fn new_config(config: ReplicationConfiguration) -> ReplicationLifecycleConfig {
        ReplicationConfig::new(Some(config), None)
    }

    pub(crate) fn has_pending_version_purge(
        config: &ReplicationLifecycleConfig,
        object_name: &str,
        version_purge_pending: bool,
    ) -> bool {
        version_purge_pending
            && config
                .config
                .as_ref()
                .is_some_and(|config| config.has_active_rules(object_name, true))
    }

    pub(crate) async fn check_delete_replication(
        bucket: &str,
        object: &ObjectToDelete,
        source: &ObjectInfo,
        opts: &ObjectOptions,
    ) -> ReplicateDecision {
        check_replicate_delete(bucket, object, source, opts, None).await
    }

    pub(crate) fn version_delete_replication_state(decision: &ReplicateDecision) -> ReplicationState {
        let pending_status = decision.pending_status();
        ReplicationState {
            replicate_decision_str: decision.to_string(),
            version_purge_status_internal: pending_status.clone(),
            purge_targets: version_purge_statuses_map(pending_status.as_deref().unwrap_or_default()),
            ..Default::default()
        }
    }

    pub(crate) async fn schedule_delete(bucket: String, delete_object: DeletedObject) {
        super::replication_pool::schedule_replication_delete(DeletedObjectReplicationInfo {
            delete_object,
            bucket,
            event_type: REPLICATE_INCOMING_DELETE.to_string(),
            ..Default::default()
        })
        .await;
    }
}

#[cfg(test)]
mod tests {
    use s3s::dto::{Destination, ReplicationRule, ReplicationRuleStatus};

    use super::super::replication_filemeta_boundary::{ReplicateTargetDecision, VersionPurgeStatusType};
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

    #[test]
    fn has_pending_version_purge_preserves_replication_active_rule_behavior() {
        let config = ReplicationLifecycleBridge::new_config(ReplicationConfiguration {
            role: String::new(),
            rules: vec![replication_rule()],
        });

        assert!(ReplicationLifecycleBridge::has_pending_version_purge(&config, "logs/object", true));
        assert!(!ReplicationLifecycleBridge::has_pending_version_purge(&config, "logs/object", false));
    }

    #[test]
    fn version_delete_replication_state_tracks_pending_purge_targets() {
        let target = ReplicateTargetDecision::new("arn:aws:s3:::target".to_string(), true, false);
        let mut decision = ReplicateDecision::new();
        decision.set(target);

        let state = ReplicationLifecycleBridge::version_delete_replication_state(&decision);

        assert_eq!(state.version_purge_status_internal.as_deref(), Some("arn:aws:s3:::target=PENDING;"));
        assert!(state.purge_targets.contains_key("arn:aws:s3:::target"));
        assert_eq!(state.purge_targets["arn:aws:s3:::target"], VersionPurgeStatusType::Pending);
    }
}
