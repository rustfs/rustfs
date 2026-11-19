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

use crate::rules::xml_config::{FilterRule, S3KeyFilter};
use crate::rules::{RulesMap, TargetIdSet};
use crate::storage_api::notification::{BucketMetadata, NotifyStore};
use percent_encoding::percent_decode_str;
use rustfs_s3_types::EventName;
use rustfs_targets::arn::{ARN, TargetID};
use starshard::{AsyncShardedHashMap, DEFAULT_SHARDS, SnapshotMode};
use std::sync::{
    Arc, RwLock, Weak,
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

#[derive(Clone)]
pub struct NotifyRuleEngine {
    bucket_rules_map: Arc<AsyncShardedHashMap<String, CachedBucketRules, rustc_hash::FxBuildHasher>>,
    // Never hold this short synchronous guard across an await. Metadata cache
    // reads precede the compiled-rules cache, so there is no reverse lock order.
    metadata_source: Arc<RwLock<Option<Weak<NotifyStore>>>>,
}

#[derive(Clone)]
struct CachedBucketRules {
    metadata: Option<Weak<BucketMetadata>>,
    incarnation_id: Option<Uuid>,
    refresh_requested: Arc<AtomicBool>,
    // Cache rejected snapshots too, so corrupt persisted routes are not
    // repeatedly compiled on the event hot path.
    rules: Option<RulesMap>,
}

fn rules_from_metadata(metadata: &BucketMetadata) -> Option<RulesMap> {
    let mut rules = RulesMap::new();
    let Some(config) = metadata.notification_config.as_ref() else {
        // Unreadable metadata is not authority to keep an old subscription.
        return metadata.notification_config_xml.is_empty().then_some(rules);
    };
    let config = config.behavior();
    for route in config
        .queue_routes
        .into_iter()
        .chain(config.topic_routes)
        .chain(config.lambda_routes)
    {
        let target = ARN::parse(&route.destination).ok()?.target_id;
        let events: Vec<_> = route.events.iter().filter_map(|event| EventName::parse(event).ok()).collect();
        let mut filter = S3KeyFilter::default();
        for (name, value) in route.filter_rules.into_iter().flatten() {
            filter.filter_rule_list.push(FilterRule {
                name: name?,
                value: value?,
            });
        }
        filter.validate().ok()?;
        rules.add_rule_config(&events, filter.pattern(), target);
    }
    Some(rules)
}

/// Compiles removal targets from metadata captured by the fenced native delete.
/// The snapshot remains authority after the bucket's metadata has been withdrawn.
pub fn bucket_removed_targets_from_metadata(metadata: &BucketMetadata) -> TargetIdSet {
    rules_from_metadata(metadata)
        .map(|rules| rules.match_rules(EventName::BucketRemoved, ""))
        .unwrap_or_default()
}

impl NotifyRuleEngine {
    pub fn new() -> Self {
        Self {
            bucket_rules_map: Arc::new(AsyncShardedHashMap::with_snapshot_mode(DEFAULT_SHARDS, SnapshotMode::Cached)),
            metadata_source: Arc::new(RwLock::new(None)),
        }
    }

    /// Uses this store's immutable metadata snapshots as rule authority. Once
    /// bound, missing metadata or a dropped store fails closed, never falling
    /// back to the standalone name-only table. The weak handle avoids cycles.
    pub fn bind_bucket_metadata_source(&self, store: &Arc<NotifyStore>) {
        *self.metadata_source.write().unwrap_or_else(|err| err.into_inner()) = Some(Arc::downgrade(store));
    }

    fn metadata_source(&self) -> Option<Weak<NotifyStore>> {
        self.metadata_source.read().unwrap_or_else(|err| err.into_inner()).clone()
    }

    fn uses_bucket_metadata_source(&self) -> bool {
        self.metadata_source().is_some()
    }

    pub async fn is_target_bound_to_any_bucket(&self, target_id: &TargetID) -> bool {
        if let Some(source) = self.metadata_source() {
            let Some(store) = source.upgrade() else { return true };
            let Ok(metadata) = store.cached_bucket_metadata().await else { return true };
            return metadata
                .iter()
                .any(|metadata| rules_from_metadata(metadata).is_none_or(|rules| rules.contains_target_id(target_id)));
        }
        self.bucket_rules_map
            .iter()
            .await
            .into_iter()
            .any(|(_, snapshot)| snapshot.rules.is_some_and(|rules| rules.contains_target_id(target_id)))
    }

    pub async fn set_bucket_rules(&self, bucket: &str, rules_map: RulesMap) {
        self.publish_bucket_rules(bucket, None, rules_map).await;
    }

    /// Publishes standalone rules with their optional originating incarnation.
    /// Store-bound dispatch accepts only the store's metadata snapshots.
    pub(crate) async fn publish_bucket_rules(&self, bucket: &str, incarnation_id: Option<Uuid>, rules_map: RulesMap) {
        if self.uses_bucket_metadata_source() {
            return;
        }
        let event_count = rules_map.iter_events().count();
        let key = bucket.to_owned();
        if rules_map.is_empty() {
            self.bucket_rules_map.remove(&key).await;
        } else {
            let refresh_requested = self
                .bucket_rules_map
                .get(&key)
                .await
                .filter(|current| current.incarnation_id == incarnation_id)
                .map_or_else(|| Arc::new(AtomicBool::new(false)), |current| current.refresh_requested);
            self.bucket_rules_map
                .insert(
                    key,
                    CachedBucketRules {
                        metadata: None,
                        incarnation_id,
                        refresh_requested,
                        rules: Some(rules_map),
                    },
                )
                .await;
        }
        info!(
            event = EVENT_NOTIFY_RULES_UPDATED,
            component = LOG_COMPONENT_NOTIFY,
            subsystem = LOG_SUBSYSTEM_RULE_ENGINE,
            state = "updated",
            bucket = %bucket,
            event_count,
            "notify bucket rules state"
        );
    }

    async fn snapshot_for_bucket(&self, bucket: &str) -> Option<CachedBucketRules> {
        if let Some(source) = self.metadata_source() {
            let metadata = match source.upgrade() {
                Some(store) => store.get_bucket_metadata(bucket).await.ok(),
                None => None,
            };
            return self.snapshot_for_metadata(bucket, metadata).await;
        }
        self.bucket_rules_map.get(&bucket.to_owned()).await
    }

    pub async fn get_bucket_rules(&self, bucket: &str) -> Option<RulesMap> {
        self.snapshot_for_bucket(bucket).await.and_then(|snapshot| snapshot.rules)
    }

    async fn snapshot_for_metadata(&self, bucket: &str, metadata: Option<Arc<BucketMetadata>>) -> Option<CachedBucketRules> {
        let key = bucket.to_owned();
        let Some(metadata) = metadata else {
            self.bucket_rules_map.remove(&key).await;
            return None;
        };
        let current = self.bucket_rules_map.get(&key).await;
        if let Some(cached) = &current
            && cached
                .metadata
                .as_ref()
                .is_some_and(|snapshot| Weak::ptr_eq(snapshot, &Arc::downgrade(&metadata)))
        {
            return current;
        }
        let incarnation_id = (!metadata.bucket_incarnation_id.is_nil()).then_some(metadata.bucket_incarnation_id);
        let refresh_requested = current
            .filter(|cached| cached.incarnation_id == incarnation_id)
            .map_or_else(|| Arc::new(AtomicBool::new(false)), |cached| cached.refresh_requested);
        // The same immutable snapshot supplies both the identity and the rules.
        // A late compilation cannot be reused for a different metadata snapshot.
        let snapshot = CachedBucketRules {
            metadata: Some(Arc::downgrade(&metadata)),
            incarnation_id,
            refresh_requested,
            rules: rules_from_metadata(&metadata),
        };
        self.bucket_rules_map.insert(key, snapshot.clone()).await;
        Some(snapshot)
    }

    #[cfg(test)]
    async fn rules_for_metadata(&self, bucket: &str, metadata: Option<Arc<BucketMetadata>>) -> Option<RulesMap> {
        self.snapshot_for_metadata(bucket, metadata)
            .await
            .and_then(|snapshot| snapshot.rules)
    }

    pub async fn clear_bucket_rules(&self, bucket: &str) {
        self.withdraw_bucket_rules(bucket).await;
    }

    pub(crate) async fn withdraw_bucket_rules(&self, bucket: &str) {
        if self.bucket_rules_map.remove(&bucket.to_owned()).await.is_some() {
            info!(
                event = EVENT_NOTIFY_RULES_UPDATED,
                component = LOG_COMPONENT_NOTIFY,
                subsystem = LOG_SUBSYSTEM_RULE_ENGINE,
                state = "removed",
                bucket = %bucket,
                "notify bucket rules state"
            );
        }
    }

    pub async fn has_subscriber(&self, bucket: &str, event: &EventName) -> bool {
        self.get_bucket_rules(bucket)
            .await
            .is_some_and(|rules| rules.has_subscriber(event))
    }

    /// Queries the current rules. Event dispatch additionally validates its origin.
    pub async fn match_targets(&self, bucket: &str, event_name: EventName, object_key: &str) -> TargetIdSet {
        self.get_bucket_rules(bucket)
            .await
            .map_or_else(TargetIdSet::new, |rules| Self::targets_for_rules(&rules, event_name, object_key))
    }

    fn match_snapshot(
        snapshot: &CachedBucketRules,
        incarnation_id: Option<Uuid>,
        event_name: EventName,
        object_key: &str,
    ) -> TargetIdSet {
        if snapshot.incarnation_id != incarnation_id || (snapshot.metadata.is_some() && incarnation_id.is_none()) {
            return TargetIdSet::new();
        }
        let Some(rules) = &snapshot.rules else { return TargetIdSet::new() };
        Self::targets_for_rules(rules, event_name, object_key)
    }

    fn targets_for_rules(rules: &RulesMap, event_name: EventName, object_key: &str) -> TargetIdSet {
        let mut targets = rules.match_rules(event_name, object_key);
        if let Some(decoded_key) = decoded_object_key_for_matching(object_key) {
            targets.extend(rules.match_rules(event_name, &decoded_key));
        }
        targets
    }

    pub(crate) async fn match_bucket_targets(
        &self,
        bucket: &str,
        incarnation_id: Option<Uuid>,
        event_name: EventName,
        object_key: &str,
    ) -> TargetIdSet {
        self.snapshot_for_bucket(bucket)
            .await
            .map_or_else(TargetIdSet::new, |snapshot| {
                Self::match_snapshot(&snapshot, incarnation_id, event_name, object_key)
            })
    }

    /// Allow one targeted reload per installed incarnation, including revisions.
    pub(crate) async fn request_incarnation_refresh(&self, bucket: &str, incarnation_id: Uuid) -> bool {
        !incarnation_id.is_nil()
            && self.bucket_rules_map.get(&bucket.to_owned()).await.is_some_and(|snapshot| {
                snapshot.incarnation_id.is_some_and(|current| current != incarnation_id)
                    && !snapshot.refresh_requested.swap(true, Ordering::Relaxed)
            })
    }

    pub(crate) async fn refresh_incarnation(&self, bucket: &str, incarnation_id: Uuid) -> bool {
        let Some(store) = self.metadata_source().and_then(|source| source.upgrade()) else { return false };
        if !self.request_incarnation_refresh(bucket, incarnation_id).await {
            return false;
        }
        crate::storage_api::notification::refresh_notify_bucket_metadata(store, bucket).await
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
    use crate::storage_api::notification::{BucketMetadata, deserialize_notification_config};
    use rustfs_s3_types::EventName;
    use rustfs_targets::arn::TargetID;
    use std::sync::{Arc, Weak};

    #[tokio::test]
    async fn bucket_rules_route_events_only_with_the_matching_incarnation() {
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

        engine
            .publish_bucket_rules("bucket", Some(old_id), rules(old_target.clone()))
            .await;
        assert!(
            engine
                .match_bucket_targets("bucket", Some(old_id), EventName::ObjectCreatedPut, "object")
                .await
                .contains(&old_target)
        );
        assert!(
            engine
                .match_bucket_targets("bucket", Some(new_id), EventName::ObjectCreatedPut, "object")
                .await
                .is_empty(),
            "a peer may still have the old rules"
        );
        assert!(engine.request_incarnation_refresh("bucket", new_id).await);
        assert!(!engine.request_incarnation_refresh("bucket", new_id).await);
        assert!(
            engine
                .match_bucket_targets("bucket", None, EventName::ObjectCreatedPut, "object")
                .await
                .is_empty()
        );

        engine.withdraw_bucket_rules("bucket").await;
        assert!(
            engine
                .match_bucket_targets("bucket", Some(old_id), EventName::ObjectCreatedPut, "object")
                .await
                .is_empty()
        );
        assert!(
            engine
                .match_bucket_targets("bucket", Some(new_id), EventName::ObjectCreatedPut, "object")
                .await
                .is_empty()
        );
        engine
            .publish_bucket_rules("bucket", Some(new_id), rules(new_target.clone()))
            .await;
        assert!(
            engine
                .match_bucket_targets("bucket", Some(old_id), EventName::ObjectCreatedPut, "object")
                .await
                .is_empty()
        );
        assert!(
            engine
                .match_bucket_targets("bucket", Some(new_id), EventName::ObjectCreatedPut, "object")
                .await
                .contains(&new_target)
        );
    }

    fn metadata(target: &str) -> Arc<BucketMetadata> {
        let mut metadata = BucketMetadata::new("bucket");
        metadata.notification_config_xml = format!(
            "<NotificationConfiguration><QueueConfiguration><Event>s3:ObjectCreated:Put</Event>\
             <Queue>arn:rustfs:sqs:us-east-1:{target}:webhook</Queue></QueueConfiguration></NotificationConfiguration>"
        )
        .into_bytes();
        metadata.notification_config =
            Some(deserialize_notification_config(&metadata.notification_config_xml).expect("parse inert notification fixture"));
        Arc::new(metadata)
    }

    #[tokio::test]
    async fn metadata_bound_rules_only_match_the_originating_incarnation() {
        let engine = NotifyRuleEngine::new();
        let old = metadata("old");
        let replacement = metadata("replacement");
        let old_id = old.bucket_incarnation_id;
        let new_id = replacement.bucket_incarnation_id;
        for (metadata, accepted, rejected) in [(old, old_id, new_id), (replacement, new_id, old_id)] {
            let snapshot = engine
                .snapshot_for_metadata("bucket", Some(metadata))
                .await
                .expect("compiled snapshot");
            assert_eq!(
                NotifyRuleEngine::match_snapshot(&snapshot, Some(accepted), EventName::ObjectCreatedPut, "object").len(),
                1
            );
            assert!(
                NotifyRuleEngine::match_snapshot(&snapshot, Some(rejected), EventName::ObjectCreatedPut, "object").is_empty()
            );
            assert!(NotifyRuleEngine::match_snapshot(&snapshot, None, EventName::ObjectCreatedPut, "object").is_empty());
        }
        let mut legacy = (*metadata("legacy")).clone();
        legacy.bucket_incarnation_id = uuid::Uuid::nil();
        let snapshot = engine
            .snapshot_for_metadata("bucket", Some(Arc::new(legacy)))
            .await
            .expect("legacy snapshot");
        assert!(NotifyRuleEngine::match_snapshot(&snapshot, None, EventName::ObjectCreatedPut, "object").is_empty());
    }

    #[tokio::test]
    async fn notification_snapshot_cache_never_reuses_replaced_incarnation_or_revision() {
        let engine = NotifyRuleEngine::new();
        let old = metadata("old");
        let mut replacement = (*metadata("new")).clone();
        // Identical timestamps must not make different configurations equal.
        replacement.notification_config_updated_at = old.notification_config_updated_at;
        let replacement = Arc::new(replacement);
        assert_ne!(old.bucket_incarnation_id, replacement.bucket_incarnation_id);
        let old_target = TargetID::new("old".into(), "webhook".into());
        let new_target = TargetID::new("new".into(), "webhook".into());

        for (snapshot, wanted, unwanted) in [
            (old.clone(), &old_target, &new_target),
            (replacement.clone(), &new_target, &old_target),
            // Simulate a late compilation of a captured, older snapshot.
            (old.clone(), &old_target, &new_target),
            (replacement.clone(), &new_target, &old_target),
        ] {
            let rules = engine
                .rules_for_metadata("bucket", Some(snapshot))
                .await
                .expect("readable snapshot");
            let targets = rules.match_rules(EventName::ObjectCreatedPut, "object");
            assert!(targets.contains(wanted));
            assert!(!targets.contains(unwanted));
        }

        let mut next_revision = (*old).clone();
        next_revision.notification_config = replacement.notification_config.clone();
        next_revision.notification_config_xml = replacement.notification_config_xml.clone();
        let rules = engine
            .rules_for_metadata("bucket", Some(Arc::new(next_revision)))
            .await
            .expect("same-incarnation revision");
        assert!(!rules.contains_target_id(&old_target));
        assert!(rules.contains_target_id(&new_target));
    }

    #[tokio::test]
    async fn notification_snapshot_absence_and_corruption_never_fall_back_to_old_rules() {
        let engine = NotifyRuleEngine::new();
        let old = metadata("old");
        assert!(engine.rules_for_metadata("bucket", Some(old.clone())).await.is_some());

        let mut unreadable = (*old).clone();
        unreadable.notification_config = None;
        unreadable.notification_config_xml = b"<broken>".to_vec();
        assert!(
            engine
                .rules_for_metadata("bucket", Some(Arc::new(unreadable)))
                .await
                .is_none()
        );
        assert!(
            engine
                .bucket_rules_map
                .get(&"bucket".to_owned())
                .await
                .expect("cache rejected snapshot")
                .rules
                .is_none()
        );
        assert!(engine.rules_for_metadata("bucket", None).await.is_none());
        assert!(engine.bucket_rules_map.get(&"bucket".to_owned()).await.is_none());

        // A recreated bucket without notifications has no inherited targets.
        let replacement = Arc::new(BucketMetadata::new("bucket"));
        assert!(
            engine
                .rules_for_metadata("bucket", Some(replacement))
                .await
                .expect("empty snapshot")
                .is_empty()
        );
    }

    #[tokio::test]
    async fn notification_snapshot_binding_never_falls_back_after_store_loss() {
        let engine = NotifyRuleEngine::new();
        let target = TargetID::new("old".into(), "webhook".into());
        let mut rules = RulesMap::new();
        rules.add_rule_config(&[EventName::ObjectCreatedPut], "*".into(), target.clone());
        engine.set_bucket_rules("bucket", rules.clone()).await;
        assert!(engine.has_subscriber("bucket", &EventName::ObjectCreatedPut).await);

        *engine.metadata_source.write().expect("metadata source lock") = Some(Weak::new());
        // A delayed validated PUT must not revive the standalone table, even
        // when its owning store is gone. Target removal also fails closed.
        engine.set_bucket_rules("bucket", rules).await;
        assert!(!engine.has_subscriber("bucket", &EventName::ObjectCreatedPut).await);
        assert!(
            engine
                .match_targets("bucket", EventName::ObjectCreatedPut, "object")
                .await
                .is_empty()
        );
        assert!(engine.is_target_bound_to_any_bucket(&target).await);
    }

    #[test]
    fn notification_snapshot_compilation_preserves_destinations_events_and_filters() {
        let mut metadata = (*metadata("queue")).clone();
        metadata.notification_config_xml = concat!(
            "<NotificationConfiguration>",
            "<QueueConfiguration><Event>s3:ObjectCreated:*</Event><Event>s3:Future:Unrecognized</Event>",
            "<Queue>arn:rustfs:sqs:us-east-1:queue:webhook</Queue>",
            "<Filter><S3Key><FilterRule><Name>Prefix</Name><Value>logs/</Value></FilterRule>",
            "<FilterRule><Name>Suffix</Name><Value>.csv</Value></FilterRule></S3Key></Filter></QueueConfiguration>",
            "<TopicConfiguration><Event>s3:ObjectRemoved:*</Event><Topic>arn:rustfs:sqs:us-east-1:topic:webhook</Topic></TopicConfiguration>",
            "<CloudFunctionConfiguration><Event>s3:ObjectCreated:Put</Event>",
            "<CloudFunction>arn:rustfs:sqs:us-east-1:lambda:webhook</CloudFunction></CloudFunctionConfiguration>",
            "</NotificationConfiguration>"
        ).as_bytes().to_vec();
        metadata.notification_config =
            Some(deserialize_notification_config(&metadata.notification_config_xml).expect("parse destinations"));
        let rules = super::rules_from_metadata(&metadata).expect("compile destinations");
        assert_eq!(rules.match_rules(EventName::ObjectCreatedPut, "logs/data.csv").len(), 2);
        assert_eq!(rules.match_rules(EventName::ObjectCreatedPut, "other/data.csv").len(), 1);
        assert_eq!(rules.match_rules(EventName::ObjectCreatedPut, "logs/data.txt").len(), 1);
        let removed = rules.match_rules(EventName::ObjectRemovedDelete, "object");
        assert_eq!(removed.len(), 1);
        assert!(removed.contains(&TargetID::new("topic".into(), "webhook".into())));
    }

    #[test]
    fn notification_snapshot_compilation_rejects_invalid_persisted_routes() {
        for (destination, filter) in [
            ("not-an-arn", ""),
            (
                "arn:rustfs:sqs:us-east-1:inert:webhook",
                "<Filter><S3Key><FilterRule><Value>logs/</Value></FilterRule></S3Key></Filter>",
            ),
            (
                "arn:rustfs:sqs:us-east-1:inert:webhook",
                concat!(
                    "<Filter><S3Key><FilterRule><Name>prefix</Name><Value>logs/</Value></FilterRule>",
                    "<FilterRule><Name>Prefix</Name><Value>private/</Value></FilterRule></S3Key></Filter>"
                ),
            ),
        ] {
            let mut metadata = BucketMetadata::new("bucket");
            metadata.notification_config_xml = format!(
                "<NotificationConfiguration><QueueConfiguration><Event>s3:ObjectCreated:Put</Event>\
                 <Queue>{destination}</Queue>{filter}</QueueConfiguration></NotificationConfiguration>"
            )
            .into_bytes();
            metadata.notification_config = Some(
                deserialize_notification_config(&metadata.notification_config_xml)
                    .expect("parse invalid but well-formed persisted route"),
            );
            assert!(super::rules_from_metadata(&metadata).is_none(), "invalid route must not dispatch");
        }
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
