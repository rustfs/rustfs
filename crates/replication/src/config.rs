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

use crate::ReplicationTagFilter;
use crate::ReplicationType;
use crate::rule::ReplicationRuleExt as _;
use s3s::dto::DeleteMarkerReplicationStatus;
use s3s::dto::DeleteReplicationStatus;
use s3s::dto::Destination;
use s3s::dto::{
    ExistingObjectReplicationStatus, ReplicaModificationsStatus, ReplicationConfiguration, ReplicationRule,
    ReplicationRuleStatus, ReplicationRules,
};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use uuid::Uuid;

pub const REPLICATION_CAPABILITY_CONTRACT_VERSION: u32 = 1;

pub const REPLICATION_WRITABLE_FIELDS: &[&str] = &[
    "Role",
    "Rule.ID",
    "Rule.Status",
    "Rule.Priority",
    "Rule.Filter.Prefix",
    "Rule.Filter.Tag",
    "Rule.Filter.And",
    "Rule.Destination.Bucket",
    "Rule.ExistingObjectReplication.Status",
    "Rule.DeleteMarkerReplication.Status",
    "Rule.DeleteReplication.Status",
    "Rule.SourceSelectionCriteria.ReplicaModifications.Status",
];

pub const REPLICATION_READ_ONLY_HISTORICAL_FIELDS: &[&str] = &[
    "SourceSelectionCriteria.SseKmsEncryptedObjects",
    "Destination.EncryptionConfiguration",
    "Destination.Metrics",
    "Destination.ReplicationTime",
];

pub const REMOTE_TARGET_CAPABILITY_CONTRACT_VERSION: u32 = 1;

pub const REMOTE_TARGET_WRITABLE_FIELDS: &[&str] = &[
    "sourcebucket",
    "endpoint",
    "credentials.accessKey",
    "credentials.secretKey",
    "targetbucket",
    "secure",
    "path",
    "api",
    "arn",
    "type",
    "region",
    "bandwidth",
    "replicationSync",
    "storage_class",
    "skipTlsVerify",
    "caCertPem",
];

pub const REMOTE_TARGET_UNSUPPORTED_FIELDS: &[&str] = &["disableProxy", "healthCheckDuration", "edge", "edgeSyncBeforeExpiry"];

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ObjectOpts {
    pub name: String,
    pub user_tags: String,
    pub version_id: Option<Uuid>,
    pub delete_marker: bool,
    pub ssec: bool,
    pub op_type: ReplicationType,
    pub replica: bool,
    pub existing_object: bool,
    pub target_arn: String,
}

pub trait ReplicationConfigurationExt {
    fn replicate(&self, opts: &ObjectOpts) -> bool;
    fn has_existing_object_replication(&self, arn: &str) -> (bool, bool);
    fn filter_actionable_rules(&self, obj: &ObjectOpts) -> ReplicationRules;
    fn get_destination(&self) -> Destination;
    fn has_active_rules(&self, prefix: &str, recursive: bool) -> bool;
    fn filter_target_arns(&self, obj: &ObjectOpts) -> Vec<String>;
    fn filter_target_replication_decisions(&self, obj: &ObjectOpts) -> Vec<(String, bool)> {
        self.filter_target_arns(obj)
            .into_iter()
            .map(|arn| {
                let mut target = obj.clone();
                target.target_arn = arn.clone();
                (arn, self.replicate(&target))
            })
            .collect()
    }
}

fn rule_replicates(rule: &ReplicationRule, obj: &ObjectOpts) -> bool {
    if let Some(status) = &rule.existing_object_replication
        && obj.existing_object
        && status.status == ExistingObjectReplicationStatus::from_static(ExistingObjectReplicationStatus::DISABLED)
    {
        return false;
    }

    if obj.op_type != ReplicationType::Delete {
        return rule.metadata_replicate(obj);
    }

    if !rule.metadata_replicate(obj) {
        return false;
    }

    let version_purge = obj.version_id.is_some();
    if version_purge {
        rule.delete_replication
            .as_ref()
            .is_some_and(|delete| delete.status == DeleteReplicationStatus::from_static(DeleteReplicationStatus::ENABLED))
    } else {
        rule.delete_marker_replication.as_ref().is_some_and(|delete_marker| {
            delete_marker.status == Some(DeleteMarkerReplicationStatus::from_static(DeleteMarkerReplicationStatus::ENABLED))
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicationTargetValidationError {
    RoleWithMultipleDestinations,
    StaleTarget,
}

pub fn unsupported_replication_config_field(config: &ReplicationConfiguration) -> Option<&'static str> {
    for rule in &config.rules {
        if rule
            .source_selection_criteria
            .as_ref()
            .is_some_and(|criteria| criteria.sse_kms_encrypted_objects.is_some())
        {
            return Some("SourceSelectionCriteria.SseKmsEncryptedObjects");
        }
        if rule.destination.encryption_configuration.is_some() {
            return Some("Destination.EncryptionConfiguration");
        }
        if rule.destination.access_control_translation.is_some() {
            return Some("Destination.AccessControlTranslation");
        }
        if rule.destination.account.is_some() {
            return Some("Destination.Account");
        }
        if rule.destination.metrics.is_some() {
            return Some("Destination.Metrics");
        }
        if rule.destination.replication_time.is_some() {
            return Some("Destination.ReplicationTime");
        }
        if rule.destination.storage_class.is_some() {
            return Some("Destination.StorageClass");
        }
    }
    None
}

pub fn invalid_replication_config_status_field(config: &ReplicationConfiguration) -> Option<&'static str> {
    for rule in &config.rules {
        if !matches!(rule.status.as_str(), ReplicationRuleStatus::ENABLED | ReplicationRuleStatus::DISABLED) {
            return Some("Rule.Status");
        }
        if rule.existing_object_replication.as_ref().is_some_and(|existing| {
            !matches!(
                existing.status.as_str(),
                ExistingObjectReplicationStatus::ENABLED | ExistingObjectReplicationStatus::DISABLED
            )
        }) {
            return Some("Rule.ExistingObjectReplication.Status");
        }
        if rule.delete_replication.as_ref().is_some_and(|delete| {
            !matches!(
                delete.status.as_str(),
                DeleteReplicationStatus::ENABLED | DeleteReplicationStatus::DISABLED
            )
        }) {
            return Some("Rule.DeleteReplication.Status");
        }
        if rule
            .delete_marker_replication
            .as_ref()
            .and_then(|delete| delete.status.as_ref())
            .is_some_and(|status| {
                !matches!(
                    status.as_str(),
                    DeleteMarkerReplicationStatus::ENABLED | DeleteMarkerReplicationStatus::DISABLED
                )
            })
        {
            return Some("Rule.DeleteMarkerReplication.Status");
        }
        if rule
            .source_selection_criteria
            .as_ref()
            .and_then(|criteria| criteria.replica_modifications.as_ref())
            .is_some_and(|modifications| {
                !matches!(
                    modifications.status.as_str(),
                    ReplicaModificationsStatus::ENABLED | ReplicaModificationsStatus::DISABLED
                )
            })
        {
            return Some("Rule.SourceSelectionCriteria.ReplicaModifications.Status");
        }
    }

    None
}

pub fn active_replication_rule_destination_arns(config: &ReplicationConfiguration) -> HashSet<String> {
    let mut arns = HashSet::new();

    for rule in &config.rules {
        if rule.status == ReplicationRuleStatus::from_static(ReplicationRuleStatus::DISABLED) {
            continue;
        }

        let arn = rule.destination.bucket.trim();
        if !arn.is_empty() {
            arns.insert(arn.to_string());
        }
    }

    arns
}

pub fn replication_target_arns(config: &ReplicationConfiguration) -> HashSet<String> {
    let role = config.role.trim();
    if !role.is_empty() {
        return HashSet::from([role.to_string()]);
    }

    active_replication_rule_destination_arns(config)
}

pub fn validate_replication_config_target_arns<'a>(
    configured_arns: impl IntoIterator<Item = &'a str>,
    config: &ReplicationConfiguration,
) -> std::result::Result<(), ReplicationTargetValidationError> {
    let configured_arns = configured_arns.into_iter().collect::<HashSet<_>>();

    let role = config.role.trim();
    let destination_arns = active_replication_rule_destination_arns(config);
    if !role.is_empty() && destination_arns.len() > 1 {
        return Err(ReplicationTargetValidationError::RoleWithMultipleDestinations);
    }

    for configured_arn in replication_target_arns(config) {
        if !configured_arns.contains(configured_arn.as_str()) {
            return Err(ReplicationTargetValidationError::StaleTarget);
        }
    }

    Ok(())
}

pub fn should_remove_replication_target(
    target_arn: &str,
    is_replication_target: bool,
    config_target_arns: &HashSet<String>,
) -> bool {
    is_replication_target && config_target_arns.contains(target_arn)
}

impl ReplicationConfigurationExt for ReplicationConfiguration {
    /// Check whether any object-replication rules exist
    fn has_existing_object_replication(&self, arn: &str) -> (bool, bool) {
        let mut has_arn = false;
        let arn = arn.trim();

        for rule in &self.rules {
            if rule.destination.bucket.trim() == arn || self.role.trim() == arn {
                if !has_arn {
                    has_arn = true;
                }
                if let Some(status) = &rule.existing_object_replication
                    && status.status == ExistingObjectReplicationStatus::from_static(ExistingObjectReplicationStatus::ENABLED)
                {
                    return (true, true);
                }
            }
        }
        (has_arn, false)
    }

    fn filter_actionable_rules(&self, obj: &ObjectOpts) -> ReplicationRules {
        if obj.name.is_empty() && obj.op_type != ReplicationType::Resync && obj.op_type != ReplicationType::All {
            return vec![];
        }

        let mut rules = ReplicationRules::default();

        for rule in &self.rules {
            if rule.status == ReplicationRuleStatus::from_static(ReplicationRuleStatus::DISABLED) {
                continue;
            }

            if !obj.target_arn.is_empty()
                && rule.destination.bucket.trim() != obj.target_arn.trim()
                && self.role.trim() != obj.target_arn.trim()
            {
                continue;
            }

            if obj.op_type == ReplicationType::Resync || obj.op_type == ReplicationType::All {
                rules.push(rule.clone());
                continue;
            }

            if let Some(status) = &rule.existing_object_replication
                && obj.existing_object
                && status.status == ExistingObjectReplicationStatus::from_static(ExistingObjectReplicationStatus::DISABLED)
            {
                continue;
            }

            if !obj.name.starts_with(rule.prefix()) {
                continue;
            }

            if let Some(filter) = &rule.filter {
                let object_tags = ReplicationTagFilter::decode_tags_to_map(&obj.user_tags);
                if filter.test_tags(&object_tags) {
                    rules.push(rule.clone());
                }
            } else {
                rules.push(rule.clone());
            }
        }

        rules.sort_by(|a, b| {
            if a.destination == b.destination {
                b.priority.cmp(&a.priority)
            } else {
                std::cmp::Ordering::Equal
            }
        });

        rules
    }

    /// Retrieve the destination configuration
    fn get_destination(&self) -> Destination {
        if !self.rules.is_empty() {
            self.rules[0].destination.clone()
        } else {
            Destination {
                bucket: String::new(),
                ..Default::default()
            }
        }
    }

    /// Determine whether an object should be replicated
    fn replicate(&self, obj: &ObjectOpts) -> bool {
        let rules = self.filter_actionable_rules(obj);
        rules.first().is_some_and(|rule| rule_replicates(rule, obj))
    }

    /// Check for an active rule
    /// Optionally accept a prefix
    /// When recursive is true, return true if any level under the prefix has an active rule
    /// Without a prefix, recursive behaves as true
    fn has_active_rules(&self, prefix: &str, recursive: bool) -> bool {
        if self.rules.is_empty() {
            return false;
        }

        for rule in &self.rules {
            if rule.status == ReplicationRuleStatus::from_static(ReplicationRuleStatus::DISABLED) {
                continue;
            }

            if let Some(filter) = &rule.filter
                && let Some(filter_prefix) = &filter.prefix
            {
                if !prefix.is_empty() && !filter_prefix.is_empty() {
                    // The provided prefix must fall within the rule prefix
                    if !recursive && !prefix.starts_with(filter_prefix) {
                        continue;
                    }
                }

                // When recursive, skip this rule if it does not match the test prefix or hierarchy
                if recursive && !rule.prefix().starts_with(prefix) && !prefix.starts_with(rule.prefix()) {
                    continue;
                }
            }
            return true;
        }
        false
    }

    /// Filter target ARNs and return a slice of the distinct values in the config
    fn filter_target_arns(&self, obj: &ObjectOpts) -> Vec<String> {
        let role = self.role.trim();
        if !role.is_empty() {
            return vec![role.to_string()];
        }

        let mut arns = Vec::new();
        let mut targets_map: HashSet<String> = HashSet::new();
        let rules = self.filter_actionable_rules(obj);

        for rule in rules {
            if rule.status == ReplicationRuleStatus::from_static(ReplicationRuleStatus::DISABLED) {
                continue;
            }

            let arn = rule.destination.bucket.trim();
            if !arn.is_empty() && !targets_map.contains(arn) {
                targets_map.insert(arn.to_string());
            }
        }

        for arn in targets_map {
            arns.push(arn);
        }
        arns
    }

    fn filter_target_replication_decisions(&self, obj: &ObjectOpts) -> Vec<(String, bool)> {
        let rules = self.filter_actionable_rules(obj);
        let role = self.role.trim();
        if !role.is_empty() {
            let mut selected = None;
            for rule in &rules {
                if selected.is_none_or(|current: &ReplicationRule| rule.priority > current.priority) {
                    selected = Some(rule);
                }
            }
            return vec![(role.to_string(), selected.is_some_and(|rule| rule_replicates(rule, obj)))];
        }

        let mut target_indexes: HashMap<&str, usize> = HashMap::new();
        let mut selected_rules: Vec<(&str, &ReplicationRule)> = Vec::new();
        for rule in &rules {
            let arn = rule.destination.bucket.trim();
            if arn.is_empty() {
                continue;
            }
            if let Some(index) = target_indexes.get(arn).copied() {
                if rule.priority > selected_rules[index].1.priority {
                    selected_rules[index].1 = rule;
                }
            } else {
                target_indexes.insert(arn, selected_rules.len());
                selected_rules.push((arn, rule));
            }
        }
        selected_rules
            .into_iter()
            .map(|(arn, rule)| (arn.to_string(), rule_replicates(rule, obj)))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use s3s::dto::{
        DeleteMarkerReplication, DeleteReplication, Destination, EncryptionConfiguration, ExistingObjectReplication, Metrics,
        MetricsStatus, ReplicaModifications, ReplicationRule, ReplicationTime, ReplicationTimeStatus, ReplicationTimeValue,
        SourceSelectionCriteria, SseKmsEncryptedObjects, SseKmsEncryptedObjectsStatus,
    };
    use s3s::xml::{Deserializer, Serializer};

    fn replication_rule(id: &str, arn: &str) -> ReplicationRule {
        ReplicationRule {
            delete_marker_replication: Some(DeleteMarkerReplication::default()),
            delete_replication: None,
            destination: Destination {
                bucket: arn.to_string(),
                ..Default::default()
            },
            existing_object_replication: Some(ExistingObjectReplication {
                status: ExistingObjectReplicationStatus::from_static(ExistingObjectReplicationStatus::ENABLED),
            }),
            filter: None,
            id: Some(id.to_string()),
            prefix: Some(String::new()),
            priority: Some(1),
            source_selection_criteria: None,
            status: ReplicationRuleStatus::from_static(ReplicationRuleStatus::ENABLED),
        }
    }

    #[test]
    fn filter_target_arns_uses_role_when_role_is_present() {
        let config = ReplicationConfiguration {
            role: " arn:legacy:target ".to_string(),
            rules: vec![
                replication_rule("rule-1", "arn:target:a"),
                replication_rule("rule-2", "arn:target:b"),
            ],
        };

        let arns = config.filter_target_arns(&ObjectOpts {
            name: "object".to_string(),
            op_type: ReplicationType::Object,
            ..Default::default()
        });

        assert_eq!(arns, vec!["arn:legacy:target".to_string()]);
    }

    #[test]
    fn filter_target_arns_falls_back_to_role_when_destination_is_empty() {
        let config = ReplicationConfiguration {
            role: "arn:legacy:target".to_string(),
            rules: vec![ReplicationRule {
                delete_marker_replication: Some(DeleteMarkerReplication::default()),
                delete_replication: None,
                destination: Destination {
                    bucket: String::new(),
                    ..Default::default()
                },
                existing_object_replication: Some(ExistingObjectReplication {
                    status: ExistingObjectReplicationStatus::from_static(ExistingObjectReplicationStatus::ENABLED),
                }),
                filter: None,
                id: Some("rule-1".to_string()),
                prefix: Some(String::new()),
                priority: Some(1),
                source_selection_criteria: None,
                status: ReplicationRuleStatus::from_static(ReplicationRuleStatus::ENABLED),
            }],
        };

        let arns = config.filter_target_arns(&ObjectOpts {
            name: "object".to_string(),
            op_type: ReplicationType::Object,
            ..Default::default()
        });

        assert_eq!(arns, vec!["arn:legacy:target".to_string()]);
    }

    fn replication_rule_existing_object_disabled(id: &str, arn: &str) -> ReplicationRule {
        ReplicationRule {
            delete_marker_replication: Some(DeleteMarkerReplication::default()),
            delete_replication: None,
            destination: Destination {
                bucket: arn.to_string(),
                ..Default::default()
            },
            existing_object_replication: Some(ExistingObjectReplication {
                status: ExistingObjectReplicationStatus::from_static(ExistingObjectReplicationStatus::DISABLED),
            }),
            filter: None,
            id: Some(id.to_string()),
            prefix: Some(String::new()),
            priority: Some(1),
            source_selection_criteria: None,
            status: ReplicationRuleStatus::from_static(ReplicationRuleStatus::ENABLED),
        }
    }

    // Regression test for BUG-3: replicate_object was calling filter_target_arns with
    // existing_object:false regardless of op_type, letting ExistingObject resync operations
    // fan out to targets whose rule has ExistingObjectReplicationStatus::DISABLED.
    #[test]
    fn filter_target_arns_excludes_disabled_existing_object_target_for_existing_object_op() {
        let config = ReplicationConfiguration {
            role: String::new(),
            rules: vec![
                replication_rule("rule-enabled", "arn:target:enabled"),
                replication_rule_existing_object_disabled("rule-disabled", "arn:target:disabled"),
            ],
        };

        let arns = config.filter_target_arns(&ObjectOpts {
            name: "object".to_string(),
            op_type: ReplicationType::ExistingObject,
            existing_object: true,
            ..Default::default()
        });

        assert_eq!(arns.len(), 1, "only the ENABLED target should be returned for ExistingObject ops");
        assert!(arns.contains(&"arn:target:enabled".to_string()));
        assert!(!arns.contains(&"arn:target:disabled".to_string()));
    }

    // Heal operations intentionally bypass ExistingObjectReplicationStatus — healing a past
    // failure is not subject to the existing-object opt-out.
    #[test]
    fn filter_target_arns_includes_disabled_existing_object_target_for_heal_op() {
        let config = ReplicationConfiguration {
            role: String::new(),
            rules: vec![
                replication_rule("rule-enabled", "arn:target:enabled"),
                replication_rule_existing_object_disabled("rule-disabled", "arn:target:disabled"),
            ],
        };

        let arns = config.filter_target_arns(&ObjectOpts {
            name: "object".to_string(),
            op_type: ReplicationType::Heal,
            existing_object: false,
            ..Default::default()
        });

        assert_eq!(
            arns.len(),
            2,
            "Heal ops must reach all targets regardless of existing_object_replication setting"
        );
        assert!(arns.contains(&"arn:target:enabled".to_string()));
        assert!(arns.contains(&"arn:target:disabled".to_string()));
    }

    #[test]
    fn replication_target_arns_use_role_when_present() {
        let role = "arn:rustfs:replication:us-east-1:source:bucket";
        let destination = "arn:rustfs:replication:us-east-1:target:bucket";
        let config = ReplicationConfiguration {
            role: format!(" {role} "),
            rules: vec![replication_rule("rule-1", destination)],
        };

        let arns = replication_target_arns(&config);

        assert!(arns.contains(role));
        assert!(!arns.contains(destination));
    }

    #[test]
    fn replication_target_arns_use_rule_destinations_without_role() {
        let destination = "arn:rustfs:replication:us-east-1:target:bucket";
        let config = ReplicationConfiguration {
            role: String::new(),
            rules: vec![replication_rule("rule-1", destination)],
        };

        let arns = replication_target_arns(&config);

        assert!(arns.contains(destination));
    }

    #[test]
    fn validate_replication_config_target_arns_accepts_matching_destination_arns() {
        let arn = "arn:rustfs:replication:us-east-1:target:bucket";
        let config = ReplicationConfiguration {
            role: String::new(),
            rules: vec![replication_rule("rule-1", arn)],
        };

        validate_replication_config_target_arns([arn], &config).expect("matching target should pass validation");
    }

    #[test]
    fn validate_replication_config_target_arns_rejects_stale_destination_arns() {
        let config = ReplicationConfiguration {
            role: String::new(),
            rules: vec![replication_rule("rule-1", "arn:rustfs:replication:us-east-1:target-b:bucket")],
        };

        let err = validate_replication_config_target_arns(["arn:rustfs:replication:us-east-1:target-a:bucket"], &config)
            .expect_err("stale target should fail validation");
        assert_eq!(err, ReplicationTargetValidationError::StaleTarget);
    }

    #[test]
    fn validate_replication_config_target_arns_rejects_role_with_multiple_destinations() {
        let role = "arn:rustfs:replication:us-east-1:role-target:bucket";
        let config = ReplicationConfiguration {
            role: role.to_string(),
            rules: vec![
                replication_rule("rule-a", "arn:rustfs:replication:us-east-1:target-a:bucket"),
                replication_rule("rule-b", "arn:rustfs:replication:us-east-1:target-b:bucket"),
            ],
        };

        let err = validate_replication_config_target_arns([role], &config)
            .expect_err("role plus multiple destinations should be rejected");
        assert_eq!(err, ReplicationTargetValidationError::RoleWithMultipleDestinations);
    }

    #[test]
    fn validate_replication_config_target_arns_ignores_disabled_rules() {
        let mut rule = replication_rule("rule-1", "arn:rustfs:replication:us-east-1:stale:bucket");
        rule.status = ReplicationRuleStatus::from_static(ReplicationRuleStatus::DISABLED);
        let config = ReplicationConfiguration {
            role: String::new(),
            rules: vec![rule],
        };

        validate_replication_config_target_arns(std::iter::empty::<&str>(), &config)
            .expect("disabled rules should not require live targets");
    }

    #[test]
    fn should_remove_replication_target_only_matches_replication_target_arns() {
        let target_arns = HashSet::from(["arn:rustfs:replication:us-east-1:removed:bucket".to_string()]);

        assert!(should_remove_replication_target(
            "arn:rustfs:replication:us-east-1:removed:bucket",
            true,
            &target_arns
        ));
        assert!(!should_remove_replication_target(
            "arn:rustfs:replication:us-east-1:kept:bucket",
            true,
            &target_arns
        ));
        assert!(!should_remove_replication_target(
            "arn:rustfs:replication:us-east-1:removed:bucket",
            false,
            &target_arns
        ));
    }

    fn delete_marker_rule(id: &str, arn: &str, prefix: &str, priority: i32, delete_marker_enabled: bool) -> ReplicationRule {
        let status = if delete_marker_enabled {
            DeleteMarkerReplicationStatus::from_static(DeleteMarkerReplicationStatus::ENABLED)
        } else {
            DeleteMarkerReplicationStatus::from_static(DeleteMarkerReplicationStatus::DISABLED)
        };
        ReplicationRule {
            delete_marker_replication: Some(DeleteMarkerReplication { status: Some(status) }),
            delete_replication: None,
            destination: Destination {
                bucket: arn.to_string(),
                ..Default::default()
            },
            existing_object_replication: Some(ExistingObjectReplication {
                status: ExistingObjectReplicationStatus::from_static(ExistingObjectReplicationStatus::ENABLED),
            }),
            filter: None,
            id: Some(id.to_string()),
            prefix: Some(prefix.to_string()),
            priority: Some(priority),
            source_selection_criteria: None,
            status: ReplicationRuleStatus::from_static(ReplicationRuleStatus::ENABLED),
        }
    }

    // Regression test for backlog#1029: two ENABLED rules to the same destination with
    // overlapping prefixes must be evaluated highest-priority-first (AWS S3 / MinIO precedence).
    // The higher-priority rule disables delete-marker replication, so a delete-marker op must
    // NOT replicate; if the sort were ascending the lower-priority ENABLED rule would win.
    #[test]
    fn replicate_delete_marker_follows_highest_priority_rule() {
        let arn = "arn:rustfs:replication:us-east-1:target:bucket";
        let config = ReplicationConfiguration {
            role: String::new(),
            rules: vec![
                delete_marker_rule("low-priority-enabled", arn, "logs/", 1, true),
                delete_marker_rule("high-priority-disabled", arn, "logs/2026/", 5, false),
            ],
        };

        let opts = ObjectOpts {
            name: "logs/2026/app.log".to_string(),
            op_type: ReplicationType::Delete,
            delete_marker: true,
            version_id: None,
            ..Default::default()
        };

        assert!(
            !config.replicate(&opts),
            "highest-priority rule disables delete-marker replication, so the delete marker must not replicate"
        );
    }

    #[test]
    fn role_delete_decision_follows_highest_priority_rule() {
        let role = "arn:rustfs:replication:us-east-1:role-target:bucket";
        let destination = "arn:rustfs:replication:us-east-1:target:bucket";
        let config = ReplicationConfiguration {
            role: role.to_string(),
            rules: vec![
                delete_marker_rule("low-priority-enabled", destination, "logs/", 1, true),
                delete_marker_rule("high-priority-disabled", destination, "logs/2026/", 5, false),
            ],
        };
        let opts = ObjectOpts {
            name: "logs/2026/app.log".to_string(),
            op_type: ReplicationType::Delete,
            delete_marker: true,
            ..Default::default()
        };

        assert_eq!(
            config.filter_target_replication_decisions(&opts),
            vec![(role.to_string(), false)],
            "the role target must use the highest-priority matching rule"
        );
    }

    #[test]
    fn version_purge_uses_delete_replication_for_object_and_marker_versions() {
        let arn = "arn:rustfs:replication:us-east-1:target:bucket";
        let mut rule = delete_marker_rule("delete-switches", arn, "", 1, true);
        rule.delete_replication = Some(DeleteReplication {
            status: DeleteReplicationStatus::from_static(DeleteReplicationStatus::DISABLED),
        });
        let mut config = ReplicationConfiguration {
            role: String::new(),
            rules: vec![rule],
        };

        for version_id in [Some(Uuid::new_v4()), Some(Uuid::nil())] {
            for delete_marker in [false, true] {
                assert!(!config.replicate(&ObjectOpts {
                    name: "object".to_string(),
                    op_type: ReplicationType::Delete,
                    version_id,
                    delete_marker,
                    ..Default::default()
                }));
            }
        }

        let stored_marker = ObjectOpts {
            name: "object".to_string(),
            op_type: ReplicationType::Delete,
            delete_marker: true,
            ..Default::default()
        };
        assert!(config.replicate(&stored_marker), "stored markers must use DeleteMarkerReplication");
        assert_eq!(config.filter_target_replication_decisions(&stored_marker), vec![(arn.to_string(), true)]);

        config.rules[0].delete_replication = Some(DeleteReplication {
            status: DeleteReplicationStatus::from_static(DeleteReplicationStatus::ENABLED),
        });
        assert!(config.replicate(&ObjectOpts {
            name: "object".to_string(),
            op_type: ReplicationType::Delete,
            version_id: Some(Uuid::nil()),
            delete_marker: true,
            ..Default::default()
        }));
        assert!(config.replicate(&stored_marker));
    }

    #[test]
    fn unsupported_replication_fields_are_reported_before_persistence() {
        let arn = "arn:rustfs:replication:us-east-1:target:bucket";
        let mut config = ReplicationConfiguration {
            role: String::new(),
            rules: vec![replication_rule("unsupported", arn)],
        };

        config.rules[0].source_selection_criteria = Some(SourceSelectionCriteria {
            replica_modifications: None,
            sse_kms_encrypted_objects: Some(SseKmsEncryptedObjects {
                status: SseKmsEncryptedObjectsStatus::from_static(SseKmsEncryptedObjectsStatus::ENABLED),
            }),
        });
        assert_eq!(
            unsupported_replication_config_field(&config),
            Some("SourceSelectionCriteria.SseKmsEncryptedObjects")
        );

        config.rules[0].source_selection_criteria = None;
        config.rules[0].destination.encryption_configuration = Some(EncryptionConfiguration {
            replica_kms_key_id: Some("arn:aws:kms:us-east-1:123456789012:key/opaque-key-id".to_string()),
        });
        assert_eq!(unsupported_replication_config_field(&config), Some("Destination.EncryptionConfiguration"));

        config.rules[0].destination.encryption_configuration = None;
        config.rules[0].destination.access_control_translation = Some(s3s::dto::AccessControlTranslation {
            owner: s3s::dto::OwnerOverride::from_static(s3s::dto::OwnerOverride::DESTINATION),
        });
        assert_eq!(
            unsupported_replication_config_field(&config),
            Some("Destination.AccessControlTranslation")
        );

        config.rules[0].destination.access_control_translation = None;
        config.rules[0].destination.account = Some("123456789012".to_string());
        assert_eq!(unsupported_replication_config_field(&config), Some("Destination.Account"));

        config.rules[0].destination.account = None;
        config.rules[0].destination.metrics = Some(Metrics {
            event_threshold: None,
            status: MetricsStatus::from_static(MetricsStatus::ENABLED),
        });
        assert_eq!(unsupported_replication_config_field(&config), Some("Destination.Metrics"));

        config.rules[0].destination.metrics = None;
        config.rules[0].destination.replication_time = Some(ReplicationTime {
            status: ReplicationTimeStatus::from_static(ReplicationTimeStatus::ENABLED),
            time: ReplicationTimeValue { minutes: Some(15) },
        });
        assert_eq!(unsupported_replication_config_field(&config), Some("Destination.ReplicationTime"));

        config.rules[0].destination.replication_time = None;
        config.rules[0].destination.storage_class =
            Some(s3s::dto::StorageClass::from_static(s3s::dto::StorageClass::STANDARD_IA));
        assert_eq!(unsupported_replication_config_field(&config), Some("Destination.StorageClass"));
    }

    #[test]
    fn historical_destination_fields_survive_the_s3_xml_round_trip() {
        let xml = br#"
            <ReplicationConfiguration>
              <Role></Role>
              <Rule>
                <ID>historical</ID>
                <Status>Enabled</Status>
                <Destination>
                  <Bucket>arn:aws:s3:::destination</Bucket>
                  <Account>123456789012</Account>
                  <AccessControlTranslation><Owner>Destination</Owner></AccessControlTranslation>
                  <StorageClass>STANDARD_IA</StorageClass>
                </Destination>
              </Rule>
            </ReplicationConfiguration>
        "#;
        let mut deserializer = Deserializer::new(xml);
        let config = <ReplicationConfiguration as s3s::xml::Deserialize>::deserialize(&mut deserializer)
            .expect("historical config should parse");
        deserializer
            .expect_eof()
            .expect("historical config should consume the whole body");

        let mut encoded = Vec::new();
        <ReplicationConfiguration as s3s::xml::Serialize>::serialize(&config, &mut Serializer::new(&mut encoded))
            .expect("historical config should serialize");
        let encoded = String::from_utf8(encoded).expect("serialized XML should be UTF-8");
        for field in [
            "<Account>123456789012</Account>",
            "<Owner>Destination</Owner>",
            "<StorageClass>STANDARD_IA</StorageClass>",
        ] {
            assert!(encoded.contains(field), "historical field {field} was lost: {encoded}");
        }
    }

    #[test]
    fn s3_xml_parser_discards_unknown_replication_elements_before_validation() {
        let xml = br#"
            <ReplicationConfiguration>
              <Role></Role>
              <FutureTopLevel>future</FutureTopLevel>
              <Rule>
                <ID>unknown</ID>
                <Status>Enabled</Status>
                <Destination>
                  <Bucket>arn:aws:s3:::destination</Bucket>
                </Destination>
              </Rule>
            </ReplicationConfiguration>
        "#;
        let mut deserializer = Deserializer::new(xml);
        let config = <ReplicationConfiguration as s3s::xml::Deserialize>::deserialize(&mut deserializer)
            .expect("s3s should accept unknown elements");
        deserializer.expect_eof().expect("unknown elements should still be consumed");

        assert!(config.rules[0].destination.encryption_configuration.is_none());
        assert_eq!(unsupported_replication_config_field(&config), None);
    }

    #[test]
    fn capability_fields_match_validator_rejections() {
        let rejected_fields = [
            "SourceSelectionCriteria.SseKmsEncryptedObjects",
            "Destination.EncryptionConfiguration",
            "Destination.Metrics",
            "Destination.ReplicationTime",
        ];

        for field in rejected_fields {
            assert!(
                REPLICATION_READ_ONLY_HISTORICAL_FIELDS.contains(&field),
                "rejected field {field} must be advertised as readable historical data"
            );
            assert!(
                !REPLICATION_WRITABLE_FIELDS.contains(&field),
                "rejected field {field} must not be advertised as writable"
            );
        }

        for field in REPLICATION_WRITABLE_FIELDS {
            assert!(
                !REPLICATION_READ_ONLY_HISTORICAL_FIELDS.contains(field),
                "field {field} cannot be both writable and historical-only"
            );
        }
    }

    #[test]
    fn invalid_replication_status_fields_are_reported_before_persistence() {
        let arn = "arn:rustfs:replication:us-east-1:target:bucket";
        let mut config = ReplicationConfiguration {
            role: String::new(),
            rules: vec![replication_rule("invalid-status", arn)],
        };

        config.rules[0].status = ReplicationRuleStatus::from_static("Invalid");
        assert_eq!(invalid_replication_config_status_field(&config), Some("Rule.Status"));

        config.rules[0] = replication_rule("invalid-status", arn);
        config.rules[0].existing_object_replication = Some(ExistingObjectReplication {
            status: ExistingObjectReplicationStatus::from_static("Invalid"),
        });
        assert_eq!(
            invalid_replication_config_status_field(&config),
            Some("Rule.ExistingObjectReplication.Status")
        );

        config.rules[0] = replication_rule("invalid-status", arn);
        config.rules[0].delete_replication = Some(DeleteReplication {
            status: DeleteReplicationStatus::from_static("Invalid"),
        });
        assert_eq!(invalid_replication_config_status_field(&config), Some("Rule.DeleteReplication.Status"));

        config.rules[0] = replication_rule("invalid-status", arn);
        config.rules[0].delete_marker_replication = Some(DeleteMarkerReplication {
            status: Some(DeleteMarkerReplicationStatus::from_static("Invalid")),
        });
        assert_eq!(
            invalid_replication_config_status_field(&config),
            Some("Rule.DeleteMarkerReplication.Status")
        );

        config.rules[0] = replication_rule("invalid-status", arn);
        config.rules[0].source_selection_criteria = Some(SourceSelectionCriteria {
            replica_modifications: Some(ReplicaModifications {
                status: ReplicaModificationsStatus::from_static("Invalid"),
            }),
            sse_kms_encrypted_objects: None,
        });
        assert_eq!(
            invalid_replication_config_status_field(&config),
            Some("Rule.SourceSelectionCriteria.ReplicaModifications.Status")
        );
    }

    #[test]
    fn target_decisions_choose_highest_priority_rule_per_destination() {
        let target_a = "arn:rustfs:replication:us-east-1:target:a";
        let target_b = "arn:rustfs:replication:us-east-1:target:b";
        let mut a_low = delete_marker_rule("a-low", target_a, "logs/", 1, true);
        let b = delete_marker_rule("b", target_b, "logs/", 2, true);
        let a_high = delete_marker_rule("a-high", target_a, "logs/2026/", 5, false);
        a_low.delete_replication = Some(DeleteReplication {
            status: DeleteReplicationStatus::from_static(DeleteReplicationStatus::ENABLED),
        });
        let config = ReplicationConfiguration {
            role: String::new(),
            rules: vec![a_low, b, a_high],
        };

        let decisions = config.filter_target_replication_decisions(&ObjectOpts {
            name: "logs/2026/app.log".to_string(),
            op_type: ReplicationType::Delete,
            delete_marker: true,
            ..Default::default()
        });

        assert_eq!(decisions, vec![(target_a.to_string(), false), (target_b.to_string(), true)]);
    }
}
