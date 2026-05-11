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

use crate::rules::{RulesMap, TargetIdSet};
use rustfs_s3_common::EventName;
use rustfs_targets::arn::TargetID;
use starshard::AsyncShardedHashMap;
use std::sync::Arc;
use tracing::info;

#[derive(Clone)]
pub struct NotifyRuleEngine {
    bucket_rules_map: Arc<AsyncShardedHashMap<String, RulesMap, rustc_hash::FxBuildHasher>>,
}

impl NotifyRuleEngine {
    pub fn new() -> Self {
        Self {
            bucket_rules_map: Arc::new(AsyncShardedHashMap::new(0)),
        }
    }

    pub async fn is_target_bound_to_any_bucket(&self, target_id: &TargetID) -> bool {
        let items = self.bucket_rules_map.iter().await;
        for (_bucket, rules_map) in items {
            if rules_map.contains_target_id(target_id) {
                return true;
            }
        }
        false
    }

    pub async fn set_bucket_rules(&self, bucket: &str, rules_map: RulesMap) {
        if rules_map.is_empty() {
            self.bucket_rules_map.remove(&bucket.to_string()).await;
        } else {
            self.bucket_rules_map.insert(bucket.to_string(), rules_map).await;
        }
        info!("Updated notification rules for bucket: {}", bucket);
    }

    pub async fn get_bucket_rules(&self, bucket: &str) -> Option<RulesMap> {
        self.bucket_rules_map.get(&bucket.to_string()).await
    }

    pub async fn clear_bucket_rules(&self, bucket: &str) {
        if self.bucket_rules_map.remove(&bucket.to_string()).await.is_some() {
            info!("Removed all notification rules for bucket: {}", bucket);
        }
    }

    pub async fn has_subscriber(&self, bucket: &str, event: &EventName) -> bool {
        self.get_bucket_rules(bucket)
            .await
            .is_some_and(|rules_map| rules_map.has_subscriber(event))
    }

    pub async fn match_targets(&self, bucket: &str, event_name: EventName, object_key: &str) -> TargetIdSet {
        self.get_bucket_rules(bucket)
            .await
            .map_or_else(TargetIdSet::new, |rules_map| rules_map.match_rules(event_name, object_key))
    }
}

impl Default for NotifyRuleEngine {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::NotifyRuleEngine;
    use crate::rules::RulesMap;
    use rustfs_s3_common::EventName;
    use rustfs_targets::arn::TargetID;

    #[tokio::test]
    async fn rule_engine_tracks_bucket_rule_lifecycle() {
        let engine = NotifyRuleEngine::new();
        let target_id = TargetID::new("primary".to_string(), "webhook".to_string());
        let mut rules_map = RulesMap::new();
        rules_map.add_rule_config(&[EventName::ObjectCreatedPut], "*".to_string(), target_id.clone());

        assert!(!engine.has_subscriber("bucket", &EventName::ObjectCreatedPut).await);
        assert!(!engine.is_target_bound_to_any_bucket(&target_id).await);

        engine.set_bucket_rules("bucket", rules_map).await;

        assert!(engine.has_subscriber("bucket", &EventName::ObjectCreatedPut).await);
        assert!(engine.is_target_bound_to_any_bucket(&target_id).await);
        assert_eq!(
            engine
                .match_targets("bucket", EventName::ObjectCreatedPut, "object")
                .await
                .into_iter()
                .collect::<Vec<_>>(),
            vec![target_id.clone()]
        );

        engine.clear_bucket_rules("bucket").await;

        assert!(!engine.has_subscriber("bucket", &EventName::ObjectCreatedPut).await);
        assert!(!engine.is_target_bound_to_any_bucket(&target_id).await);
    }
}
