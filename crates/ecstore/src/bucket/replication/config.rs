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

use super::ReplicationRuleExt as _;
use crate::bucket::tagging::decode_tags_to_map;
use rustfs_filemeta::ReplicationType;
use s3s::dto::DeleteMarkerReplicationStatus;
use s3s::dto::DeleteReplicationStatus;
use s3s::dto::Destination;
use s3s::dto::{ExistingObjectReplicationStatus, ReplicationConfiguration, ReplicationRuleStatus, ReplicationRules};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use uuid::Uuid;

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
}

impl ReplicationConfigurationExt for ReplicationConfiguration {
    /// Check whether any object-replication rules exist
    fn has_existing_object_replication(&self, arn: &str) -> (bool, bool) {
        let mut has_arn = false;

        for rule in &self.rules {
            if rule.destination.bucket == arn || self.role == arn {
                if !has_arn {
                    has_arn = true;
                }
                if let Some(status) = &rule.existing_object_replication {
                    if status.status == ExistingObjectReplicationStatus::from_static(ExistingObjectReplicationStatus::ENABLED) {
                        return (true, true);
                    }
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

            if !obj.target_arn.is_empty() && rule.destination.bucket != obj.target_arn && self.role != obj.target_arn {
                continue;
            }

            if obj.op_type == ReplicationType::Resync || obj.op_type == ReplicationType::All {
                rules.push(rule.clone());
                continue;
            }

            if let Some(status) = &rule.existing_object_replication {
                if obj.existing_object
                    && status.status == ExistingObjectReplicationStatus::from_static(ExistingObjectReplicationStatus::DISABLED)
                {
                    continue;
                }
            }

            if !obj.name.starts_with(rule.prefix()) {
                continue;
            }

            if let Some(filter) = &rule.filter {
                let object_tags = decode_tags_to_map(&obj.user_tags);
                if filter.test_tags(&object_tags) {
                    rules.push(rule.clone());
                }
            } else {
                rules.push(rule.clone());
            }
        }

        rules.sort_by(|a, b| {
            if a.destination == b.destination {
                a.priority.cmp(&b.priority)
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
                account: None,
                bucket: "".to_string(),
                encryption_configuration: None,
                metrics: None,
                replication_time: None,
                access_control_translation: None,
                storage_class: None,
            }
        }
    }

    /// Determine whether an object should be replicated
    fn replicate(&self, obj: &ObjectOpts) -> bool {
        let rules = self.filter_actionable_rules(obj);

        for rule in rules.iter() {
            if rule.status == ReplicationRuleStatus::from_static(ReplicationRuleStatus::DISABLED) {
                continue;
            }

            if let Some(status) = &rule.existing_object_replication {
                if obj.existing_object
                    && status.status == ExistingObjectReplicationStatus::from_static(ExistingObjectReplicationStatus::DISABLED)
                {
                    return false;
                }
            }

            if obj.op_type == ReplicationType::Delete {
                if obj.version_id.is_some() {
                    return rule
                        .delete_replication
                        .clone()
                        .is_some_and(|d| d.status == DeleteReplicationStatus::from_static(DeleteReplicationStatus::ENABLED));
                } else {
                    return rule.delete_marker_replication.clone().is_some_and(|d| {
                        d.status == Some(DeleteMarkerReplicationStatus::from_static(DeleteMarkerReplicationStatus::ENABLED))
                    });
                }
            }

            // Regular object/metadata replication
            return rule.metadata_replicate(obj);
        }
        false
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

            if let Some(filter) = &rule.filter {
                if let Some(filter_prefix) = &filter.prefix {
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
            }
            return true;
        }
        false
    }

    /// Filter target ARNs and return a slice of the distinct values in the config
    fn filter_target_arns(&self, obj: &ObjectOpts) -> Vec<String> {
        let mut arns = Vec::new();
        let mut targets_map: HashSet<String> = HashSet::new();
        let rules = self.filter_actionable_rules(obj);

        for rule in rules {
            if rule.status == ReplicationRuleStatus::from_static(ReplicationRuleStatus::DISABLED) {
                continue;
            }

            if !self.role.is_empty() {
                arns.push(self.role.clone()); // Use the legacy RoleArn when present
                return arns;
            }

            if !targets_map.contains(&rule.destination.bucket) {
                targets_map.insert(rule.destination.bucket.clone());
            }
        }

        for arn in targets_map {
            arns.push(arn);
        }
        arns
    }
}
