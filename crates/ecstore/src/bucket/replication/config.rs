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
    /// 检查是否有现有对象复制规则
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

    /// 获取目标配置
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

    /// 判断对象是否应该被复制
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

            // 常规对象/元数据复制
            return rule.metadata_replicate(obj);
        }
        false
    }

    /// 检查是否有活跃的规则
    /// 可选择性地提供前缀
    /// 如果recursive为true，函数还会在前缀下的任何级别有活跃规则时返回true
    /// 如果没有指定前缀，recursive实际上为true
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
                        // 传入的前缀必须在规则前缀中
                        if !recursive && !prefix.starts_with(filter_prefix) {
                            continue;
                        }
                    }

                    // 如果是递归的，我们可以跳过这个规则，如果它不匹配测试前缀或前缀下的级别不匹配
                    if recursive && !rule.prefix().starts_with(prefix) && !prefix.starts_with(rule.prefix()) {
                        continue;
                    }
                }
            }
            return true;
        }
        false
    }

    /// 过滤目标ARN，返回配置中不同目标ARN的切片
    fn filter_target_arns(&self, obj: &ObjectOpts) -> Vec<String> {
        let mut arns = Vec::new();
        let mut targets_map: HashSet<String> = HashSet::new();
        let rules = self.filter_actionable_rules(obj);

        for rule in rules {
            if rule.status == ReplicationRuleStatus::from_static(ReplicationRuleStatus::DISABLED) {
                continue;
            }

            if !self.role.is_empty() {
                arns.push(self.role.clone()); // 如果存在，使用传统的RoleArn
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
