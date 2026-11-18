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
use percent_encoding::percent_decode_str;
use rustfs_s3_types::EventName;
use rustfs_targets::arn::TargetID;
use starshard::{DEFAULT_SHARDS, ShardedHashMap, SnapshotMode};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use tracing::info;
use uuid::Uuid;

const LOG_COMPONENT_NOTIFY: &str = "notify";
const LOG_SUBSYSTEM_RULE_ENGINE: &str = "rule_engine";
const EVENT_NOTIFY_RULES_UPDATED: &str = "notify_bucket_rules_updated";

fn decoded_object_key_for_matching(object_key: &str) -> Option<String> {
    if !object_key.contains('%') {
        return None;
    }

    let decoded = percent_decode_str(object_key).decode_utf8().ok()?;
    (decoded != object_key).then(|| decoded.into_owned())
}

struct BucketRules {
    incarnation_id: Option<Uuid>,
    rules: RulesMap,
    refresh_requested: Arc<AtomicBool>,
}

#[derive(Clone)]
pub struct NotifyRuleEngine {
    bucket_rules_map: Arc<ShardedHashMap<String, Arc<BucketRules>, rustc_hash::FxBuildHasher>>,
}

impl NotifyRuleEngine {
    pub fn new() -> Self {
        Self {
            bucket_rules_map: Arc::new(ShardedHashMap::with_snapshot_mode(DEFAULT_SHARDS, SnapshotMode::Cached)),
        }
    }

    pub async fn is_target_bound_to_any_bucket(&self, target_id: &TargetID) -> bool {
        self.bucket_rules_map
            .iter()
            .any(|(_, snapshot)| snapshot.rules.contains_target_id(target_id))
    }

    pub async fn set_bucket_rules(&self, bucket: &str, rules_map: RulesMap) {
        self.publish_bucket_rules(bucket, None, rules_map);
    }

    /// Atomically publishes the rules and the identity used by event dispatch.
    pub(crate) fn publish_bucket_rules(&self, bucket: &str, incarnation_id: Option<Uuid>, rules_map: RulesMap) {
        let event_count = rules_map.iter_events().count();
        let bucket_key = bucket.to_owned();
        if rules_map.is_empty() {
            self.bucket_rules_map.remove(&bucket_key);
        } else {
            // A reload that confirms the current incarnation must not re-arm
            // the refresh triggered by delayed events from an older bucket.
            let refresh_requested = self
                .bucket_rules_map
                .get(&bucket_key)
                .filter(|current| current.incarnation_id == incarnation_id)
                .map_or_else(|| Arc::new(AtomicBool::new(false)), |current| current.refresh_requested.clone());
            self.bucket_rules_map.insert(
                bucket_key,
                Arc::new(BucketRules {
                    incarnation_id,
                    rules: rules_map,
                    refresh_requested,
                }),
            );
        }
        info!(
            event = EVENT_NOTIFY_RULES_UPDATED,
            component = LOG_COMPONENT_NOTIFY,
            subsystem = LOG_SUBSYSTEM_RULE_ENGINE,
            bucket = %bucket,
            state = "updated",
            event_count,
            "notify bucket rules state"
        );
    }

    pub async fn get_bucket_rules(&self, bucket: &str) -> Option<RulesMap> {
        self.bucket_rules_map
            .get(&bucket.to_string())
            .map(|snapshot| snapshot.rules.clone())
    }

    pub async fn clear_bucket_rules(&self, bucket: &str) {
        self.withdraw_bucket_rules(bucket);
    }

    pub(crate) fn withdraw_bucket_rules(&self, bucket: &str) {
        if self.bucket_rules_map.remove(&bucket.to_string()).is_some() {
            info!(
                event = EVENT_NOTIFY_RULES_UPDATED,
                component = LOG_COMPONENT_NOTIFY,
                subsystem = LOG_SUBSYSTEM_RULE_ENGINE,
                bucket = %bucket,
                state = "removed",
                "notify bucket rules state"
            );
        }
    }

    pub async fn has_subscriber(&self, bucket: &str, event: &EventName) -> bool {
        self.bucket_rules_map
            .get(&bucket.to_string())
            .is_some_and(|snapshot| snapshot.rules.has_subscriber(event))
    }

    pub async fn match_targets(&self, bucket: &str, event_name: EventName, object_key: &str) -> TargetIdSet {
        self.match_bucket_targets(bucket, None, event_name, object_key)
    }

    /// Allow one targeted reload per installed bucket incarnation. The ordinary
    /// metadata refresh loop can still recover a failed or interrupted reload.
    pub(crate) fn request_incarnation_refresh(&self, bucket: &str, incarnation_id: Uuid) -> bool {
        self.bucket_rules_map.get(&bucket.to_string()).is_some_and(|snapshot| {
            snapshot.incarnation_id.is_some_and(|current| current != incarnation_id)
                && !snapshot.refresh_requested.swap(true, Ordering::Relaxed)
        })
    }

    pub(crate) fn match_bucket_targets(
        &self,
        bucket: &str,
        incarnation_id: Option<Uuid>,
        event_name: EventName,
        object_key: &str,
    ) -> TargetIdSet {
        let Some(snapshot) = self
            .bucket_rules_map
            .get(&bucket.to_string())
            .filter(|snapshot| snapshot.incarnation_id == incarnation_id)
        else {
            return TargetIdSet::new();
        };
        let mut targets = snapshot.rules.match_rules(event_name, object_key);
        if let Some(decoded_key) = decoded_object_key_for_matching(object_key) {
            targets.extend(snapshot.rules.match_rules(event_name, &decoded_key));
        }
        targets
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
    use rustfs_s3_types::EventName;
    use rustfs_targets::arn::TargetID;

    #[test]
    fn bucket_rules_route_events_only_with_the_matching_incarnation() {
        let engine = NotifyRuleEngine::new();
        let old_id = uuid::Uuid::new_v4();
        let new_id = uuid::Uuid::new_v4();
        let old_target = TargetID::new("old".to_string(), "webhook".to_string());
        let new_target = TargetID::new("new".to_string(), "webhook".to_string());
        let rules = |target: TargetID| {
            let mut rules = RulesMap::new();
            rules.add_rule_config(&[EventName::ObjectCreatedPut], "*".to_owned(), target);
            rules
        };
        let matched = |id| engine.match_bucket_targets("bucket", id, EventName::ObjectCreatedPut, "object");

        engine.publish_bucket_rules("bucket", Some(old_id), rules(old_target.clone()));
        assert!(matched(Some(old_id)).contains(&old_target));
        assert!(matched(Some(new_id)).is_empty(), "a peer may still have the old rules");
        assert!(engine.request_incarnation_refresh("bucket", new_id));
        assert!(!engine.request_incarnation_refresh("bucket", new_id));
        assert!(matched(None).is_empty());

        engine.withdraw_bucket_rules("bucket");
        assert!(matched(Some(old_id)).is_empty());
        assert!(matched(Some(new_id)).is_empty());
        engine.publish_bucket_rules("bucket", Some(new_id), rules(new_target.clone()));
        assert!(matched(Some(old_id)).is_empty());
        assert!(matched(Some(new_id)).contains(&new_target));
    }

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
