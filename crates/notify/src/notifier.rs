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

use crate::{error::NotificationError, event::Event, integration::NotificationMetrics, rule_engine::NotifyRuleEngine};
use rustfs_config::notify::{DEFAULT_NOTIFY_SEND_CONCURRENCY, ENV_NOTIFY_SEND_CONCURRENCY};
use rustfs_targets::arn::TargetID;
use rustfs_targets::target::EntityTarget;
use rustfs_targets::{SharedTarget, Target, TargetRuntimeManager};
use std::sync::Arc;
use tokio::sync::{RwLock, Semaphore};
use tracing::{debug, error, info, instrument, warn};

const LOG_COMPONENT_NOTIFY: &str = "notify";
const LOG_SUBSYSTEM_DISPATCH: &str = "dispatch";
const EVENT_NOTIFY_DISPATCH_SKIPPED: &str = "notify_dispatch_skipped";
const EVENT_NOTIFY_DISPATCH_FAILED: &str = "notify_dispatch_failed";
const EVENT_NOTIFY_DISPATCH_STARTED: &str = "notify_dispatch_started";
const EVENT_NOTIFY_DISPATCH_COMPLETED: &str = "notify_dispatch_completed";
const EVENT_NOTIFY_RUNTIME_LIFECYCLE: &str = "notify_runtime_lifecycle";

pub type SharedNotifyTargetList = Arc<RwLock<TargetList>>;

/// Manages event notification to targets based on rules
pub struct EventNotifier {
    metrics: Arc<NotificationMetrics>,
    rule_engine: NotifyRuleEngine,
    target_list: SharedNotifyTargetList,
    send_limiter: Arc<Semaphore>,
}

impl Default for EventNotifier {
    fn default() -> Self {
        Self::new(Arc::new(NotificationMetrics::new()), NotifyRuleEngine::new())
    }
}

impl EventNotifier {
    /// Creates a new EventNotifier
    ///
    /// # Returns
    /// Returns a new instance of EventNotifier.
    pub fn new(metrics: Arc<NotificationMetrics>, rule_engine: NotifyRuleEngine) -> Self {
        let max_inflight = rustfs_utils::get_env_usize(ENV_NOTIFY_SEND_CONCURRENCY, DEFAULT_NOTIFY_SEND_CONCURRENCY);
        EventNotifier {
            metrics,
            rule_engine,
            target_list: Arc::new(RwLock::new(TargetList::new())),
            send_limiter: Arc::new(Semaphore::new(max_inflight)),
        }
    }

    /// Returns a reference to the target list
    /// This method provides access to the target list for external use.
    ///
    /// # Returns
    /// Returns an `Arc<RwLock<TargetList>>` representing the target list.
    pub fn target_list(&self) -> SharedNotifyTargetList {
        Arc::clone(&self.target_list)
    }

    /// Returns a list of ARNs for the registered targets
    ///
    /// # Arguments
    /// * `region` - The region to use for generating the ARNs
    ///
    /// # Returns
    /// Returns a vector of strings representing the ARNs of the registered targets
    pub async fn get_arn_list(&self, region: &str) -> Vec<String> {
        let target_list_guard = self.target_list.read().await;
        target_list_guard
            .keys()
            .iter()
            .map(|target_id| target_id.to_arn(region).to_string())
            .collect()
    }

    /// Removes all targets
    pub async fn remove_all_bucket_targets(&self) {
        let mut target_list_guard = self.target_list.write().await;
        // The logic for sending cancel signals via stream_cancel_senders would be removed.
        // TargetList::clear_targets_only already handles calling target.close().
        target_list_guard.clear_targets_only().await; // Modified clear to not re-cancel
        info!(
            event = EVENT_NOTIFY_RUNTIME_LIFECYCLE,
            component = LOG_COMPONENT_NOTIFY,
            subsystem = LOG_SUBSYSTEM_DISPATCH,
            state = "targets_cleared",
            "notify runtime lifecycle"
        );
    }

    /// Sends an event to the appropriate targets based on the bucket rules
    ///
    /// # Arguments
    /// * `event` - The event to send
    #[instrument(skip_all)]
    pub async fn send(&self, event: Arc<Event>) {
        let bucket_name = &event.s3.bucket.name;
        let object_key = &event.s3.object.key;
        let event_name = event.event_name;

        let target_ids = self.rule_engine.match_targets(bucket_name, event_name, object_key).await;
        if target_ids.is_empty() {
            debug!(
                event = EVENT_NOTIFY_DISPATCH_SKIPPED,
                component = LOG_COMPONENT_NOTIFY,
                subsystem = LOG_SUBSYSTEM_DISPATCH,
                bucket = %bucket_name,
                object = %object_key,
                reason = "no_matching_targets",
                "Skipped notify dispatch"
            );
            self.metrics.increment_skipped();
            return;
        }
        let target_ids_len = target_ids.len();
        let mut handles = vec![];

        // Use scope to limit the borrow scope of target_list
        let target_list_guard = self.target_list.read().await;
        debug!(
            event = EVENT_NOTIFY_DISPATCH_STARTED,
            component = LOG_COMPONENT_NOTIFY,
            subsystem = LOG_SUBSYSTEM_DISPATCH,
            bucket = %bucket_name,
            object = %object_key,
            target_count = target_ids_len,
            "notify dispatch"
        );
        for target_id in target_ids {
            // `get` now returns Option<Arc<dyn Target + Send + Sync>>
            if let Some(target_arc) = target_list_guard.get(&target_id) {
                // Clone an Arc<Box<dyn Target>> (which is where target_list is stored) to move into an asynchronous task
                // target_arc is already Arc, clone it for the async task
                let target_for_task = target_arc.clone();
                if !target_for_task.is_enabled() {
                    debug!(
                        event = EVENT_NOTIFY_DISPATCH_SKIPPED,
                        component = LOG_COMPONENT_NOTIFY,
                        subsystem = LOG_SUBSYSTEM_DISPATCH,
                        target_id = %target_for_task.id(),
                        reason = "target_disabled",
                        "Skipped notify dispatch target"
                    );
                    continue;
                }
                let limiter = self.send_limiter.clone();
                let metrics = self.metrics.clone();
                let event_clone = event.clone();
                let is_deferred = target_for_task.store().is_some();
                let target_name_for_task = target_for_task.name(); // Get the name before generating the task
                debug!(
                    event = EVENT_NOTIFY_DISPATCH_STARTED,
                    component = LOG_COMPONENT_NOTIFY,
                    subsystem = LOG_SUBSYSTEM_DISPATCH,
                    target_id = %target_for_task.id(),
                    deferred = is_deferred,
                    "notify dispatch"
                );
                // Use cloned data in closures to avoid borrowing conflicts
                // Create an EntityTarget from the event
                let entity_target: Arc<EntityTarget<Event>> = Arc::new(EntityTarget {
                    object_name: object_key.to_string(),
                    bucket_name: bucket_name.to_string(),
                    event_name,
                    data: event_clone.as_ref().clone(),
                });
                let handle = tokio::spawn(async move {
                    metrics.increment_processing();
                    let _permit = match limiter.acquire_owned().await {
                        Ok(p) => p,
                        Err(e) => {
                            error!(
                                event = EVENT_NOTIFY_DISPATCH_FAILED,
                                component = LOG_COMPONENT_NOTIFY,
                                subsystem = LOG_SUBSYSTEM_DISPATCH,
                                target_id = %target_name_for_task,
                                error = %e,
                                reason = "permit_acquire_failed",
                                "Failed to acquire notify send permit"
                            );
                            metrics.increment_failed();
                            return;
                        }
                    };
                    if let Err(e) = target_for_task.save(entity_target.clone()).await {
                        metrics.increment_failed();
                        error!(
                            event = EVENT_NOTIFY_DISPATCH_FAILED,
                            component = LOG_COMPONENT_NOTIFY,
                            subsystem = LOG_SUBSYSTEM_DISPATCH,
                            target_id = %target_name_for_task,
                            error = %e,
                            reason = "send_failed",
                            "Failed to dispatch notify event"
                        );
                    } else {
                        if is_deferred {
                            metrics.decrement_processing();
                        } else {
                            metrics.increment_processed();
                        }
                        debug!(
                            event = EVENT_NOTIFY_DISPATCH_COMPLETED,
                            component = LOG_COMPONENT_NOTIFY,
                            subsystem = LOG_SUBSYSTEM_DISPATCH,
                            target_id = %target_name_for_task,
                            deferred = is_deferred,
                            "notify dispatch"
                        );
                    }
                });
                handles.push(handle);
            } else {
                warn!(
                    event = EVENT_NOTIFY_DISPATCH_FAILED,
                    component = LOG_COMPONENT_NOTIFY,
                    subsystem = LOG_SUBSYSTEM_DISPATCH,
                    target_id = %target_id,
                    reason = "target_missing_from_runtime",
                    "Matched notify target is missing from runtime"
                );
                self.metrics.increment_skipped();
            }
        }
        // target_list is automatically released here
        drop(target_list_guard);

        // Wait for all tasks to be completed
        for handle in handles {
            if let Err(e) = handle.await {
                error!(
                    event = EVENT_NOTIFY_DISPATCH_FAILED,
                    component = LOG_COMPONENT_NOTIFY,
                    subsystem = LOG_SUBSYSTEM_DISPATCH,
                    error = %e,
                    reason = "join_failed",
                    "Notify dispatch task failed"
                );
            }
        }
        debug!(
            event = EVENT_NOTIFY_DISPATCH_COMPLETED,
            component = LOG_COMPONENT_NOTIFY,
            subsystem = LOG_SUBSYSTEM_DISPATCH,
            bucket = %bucket_name,
            target_count = target_ids_len,
            "notify dispatch"
        );
    }

    /// Initializes the targets for buckets from shared target handles.
    #[instrument(skip(self, targets_to_init))]
    pub async fn init_bucket_targets_shared(&self, targets_to_init: Vec<SharedTarget<Event>>) -> Result<(), NotificationError> {
        let mut target_list_guard = self.target_list.write().await;
        target_list_guard.clear();

        for target in targets_to_init {
            debug!(
                event = EVENT_NOTIFY_RUNTIME_LIFECYCLE,
                component = LOG_COMPONENT_NOTIFY,
                subsystem = LOG_SUBSYSTEM_DISPATCH,
                state = "target_init",
                target_id = %target.id(),
                "Initializing notify runtime target"
            );
            target_list_guard.add(target)?;
        }

        info!(
            event = EVENT_NOTIFY_RUNTIME_LIFECYCLE,
            component = LOG_COMPONENT_NOTIFY,
            subsystem = LOG_SUBSYSTEM_DISPATCH,
            state = "targets_initialized",
            target_count = target_list_guard.len(),
            "notify runtime lifecycle"
        );
        Ok(())
    }
}

/// A thread-safe list of targets
pub struct TargetList {
    /// Map of TargetID to Target
    runtime: TargetRuntimeManager<Event>,
}

impl Default for TargetList {
    fn default() -> Self {
        Self::new()
    }
}

impl TargetList {
    /// Creates a new TargetList
    pub fn new() -> Self {
        TargetList {
            runtime: TargetRuntimeManager::new(),
        }
    }

    /// Adds a target to the list
    ///
    /// # Arguments
    /// * `target` - The target to add
    ///
    /// # Returns
    /// Returns `Ok(())` if the target was added successfully, or a `NotificationError` if an error occurred.
    pub fn add(&mut self, target: Arc<dyn Target<Event> + Send + Sync>) -> Result<(), NotificationError> {
        let id = target.id();
        if self.runtime.get_by_target_id(&id).is_some() {
            // Potentially update or log a warning/error if replacing an existing target.
            warn!(
                event = EVENT_NOTIFY_RUNTIME_LIFECYCLE,
                component = LOG_COMPONENT_NOTIFY,
                subsystem = LOG_SUBSYSTEM_DISPATCH,
                state = "target_overwrite",
                target_id = %id,
                "Overwriting existing notify runtime target"
            );
        }
        self.runtime.add_arc(target);
        Ok(())
    }

    /// Clears all targets from the list
    pub fn clear(&mut self) {
        self.runtime.clear();
    }

    /// Removes a target by ID. Note: This does not stop its associated event stream.
    /// Stream cancellation should be handled by EventNotifier.
    ///
    /// # Arguments
    /// * `id` - The ID of the target to remove
    ///
    /// # Returns
    /// Returns the removed target if it existed, otherwise `None`.
    pub async fn remove_target_only(&mut self, id: &TargetID) -> Option<SharedTarget<Event>> {
        self.runtime.remove_by_target_id_and_close(id).await
    }

    /// Clears all targets from the list. Note: This does not stop their associated event streams.
    /// Stream cancellation should be handled by EventNotifier.
    pub async fn clear_targets_only(&mut self) {
        self.runtime.clear_and_close().await;
    }

    /// Returns a target by ID
    ///
    /// # Arguments
    /// * `id` - The ID of the target to retrieve
    ///
    /// # Returns
    /// Returns the target if it exists, otherwise `None`.
    pub fn get(&self, id: &TargetID) -> Option<SharedTarget<Event>> {
        self.runtime.get_by_target_id(id)
    }

    /// Returns all target IDs
    pub fn keys(&self) -> Vec<TargetID> {
        self.runtime.target_ids()
    }

    /// Returns all targets in the list
    pub fn values(&self) -> Vec<SharedTarget<Event>> {
        self.runtime.values()
    }

    pub fn runtime_snapshots(&self) -> Vec<rustfs_targets::RuntimeTargetSnapshot> {
        self.runtime.snapshots()
    }

    pub async fn runtime_health_snapshots(&self) -> Vec<rustfs_targets::RuntimeTargetHealthSnapshot> {
        self.runtime.health_snapshots().await
    }

    pub fn runtime_status_snapshot(
        &self,
        replay_workers: &rustfs_targets::ReplayWorkerManager,
    ) -> rustfs_targets::RuntimeStatusSnapshot {
        self.runtime.status_snapshot(replay_workers)
    }

    pub fn runtime_mut(&mut self) -> &mut TargetRuntimeManager<Event> {
        &mut self.runtime
    }

    /// Returns the number of targets
    pub fn len(&self) -> usize {
        self.runtime.len()
    }

    /// is_empty can be derived from len()
    pub fn is_empty(&self) -> bool {
        self.runtime.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{rule_engine::NotifyRuleEngine, rules::RulesMap};
    use async_trait::async_trait;
    use rustfs_s3_types::EventName;
    use rustfs_targets::StoreError;
    use rustfs_targets::{
        TargetError,
        store::{Key, Store},
        target::{EntityTarget, QueuedPayload, QueuedPayloadMeta},
    };
    use serde::{Serialize, de::DeserializeOwned};
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    #[tokio::test]
    async fn encoded_event_key_matches_raw_prefix_suffix_filter() {
        let target_id = TargetID::new("primary".to_string(), "webhook".to_string());
        let mut rules_map = RulesMap::new();
        rules_map.add_rule_config(&[EventName::ObjectCreatedPut], "uploads/*.csv".to_string(), target_id.clone());

        let rule_engine = NotifyRuleEngine::new();
        rule_engine.set_bucket_rules("test-bucket", rules_map).await;

        let targets = rule_engine
            .match_targets("test-bucket", EventName::ObjectCreatedPut, "uploads%2Freport.csv")
            .await;

        assert!(targets.contains(&target_id));
    }

    #[tokio::test]
    async fn encoded_event_key_matches_raw_and_decoded_rule_targets() {
        let raw_target = TargetID::new("raw".to_string(), "webhook".to_string());
        let decoded_target = TargetID::new("decoded".to_string(), "webhook".to_string());
        let mut rules_map = RulesMap::new();
        rules_map.add_rule_config(&[EventName::ObjectCreatedPut], "uploads%2F*.csv".to_string(), raw_target.clone());
        rules_map.add_rule_config(&[EventName::ObjectCreatedPut], "uploads/*.csv".to_string(), decoded_target.clone());

        let rule_engine = NotifyRuleEngine::new();
        rule_engine.set_bucket_rules("test-bucket", rules_map).await;

        let targets = rule_engine
            .match_targets("test-bucket", EventName::ObjectCreatedPut, "uploads%2Freport.csv")
            .await;

        assert_eq!(targets.len(), 2);
        assert!(targets.contains(&raw_target));
        assert!(targets.contains(&decoded_target));
    }

    #[tokio::test]
    async fn encoded_event_key_does_not_bypass_suffix_filter() {
        let target_id = TargetID::new("primary".to_string(), "webhook".to_string());
        let mut rules_map = RulesMap::new();
        rules_map.add_rule_config(&[EventName::ObjectCreatedPut], "uploads/*.csv".to_string(), target_id);

        let rule_engine = NotifyRuleEngine::new();
        rule_engine.set_bucket_rules("test-bucket", rules_map).await;

        let root_targets = rule_engine
            .match_targets("test-bucket", EventName::ObjectCreatedPut, "report.csv")
            .await;
        let suffix_targets = rule_engine
            .match_targets("test-bucket", EventName::ObjectCreatedPut, "uploads%2Freport.txt")
            .await;

        assert!(root_targets.is_empty());
        assert!(suffix_targets.is_empty());
    }

    #[derive(Clone)]
    struct TestTarget {
        id: TargetID,
        enabled: bool,
        save_calls: Arc<AtomicUsize>,
    }

    impl TestTarget {
        fn new(id: &str, name: &str, enabled: bool) -> Self {
            Self {
                id: TargetID::new(id.to_string(), name.to_string()),
                enabled,
                save_calls: Arc::new(AtomicUsize::new(0)),
            }
        }
    }

    #[async_trait]
    impl<E> Target<E> for TestTarget
    where
        E: Send + Sync + 'static + Clone + Serialize + DeserializeOwned,
    {
        fn id(&self) -> TargetID {
            self.id.clone()
        }

        async fn is_active(&self) -> Result<bool, TargetError> {
            Ok(self.enabled)
        }

        async fn save(&self, _event: Arc<EntityTarget<E>>) -> Result<(), TargetError> {
            self.save_calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        async fn send_raw_from_store(&self, _key: Key, _body: Vec<u8>, _meta: QueuedPayloadMeta) -> Result<(), TargetError> {
            Ok(())
        }

        async fn close(&self) -> Result<(), TargetError> {
            Ok(())
        }

        fn store(&self) -> Option<&(dyn Store<QueuedPayload, Error = StoreError, Key = Key> + Send + Sync)> {
            None
        }

        fn clone_dyn(&self) -> Box<dyn Target<E> + Send + Sync> {
            let cloned = self.clone();
            Box::new(cloned)
        }

        async fn init(&self) -> Result<(), TargetError> {
            Ok(())
        }

        fn is_enabled(&self) -> bool {
            self.enabled
        }
    }

    #[tokio::test]
    async fn test_send_event_skips_disabled_target() {
        let rule_engine = NotifyRuleEngine::new();
        let notifier = EventNotifier::new(Arc::new(NotificationMetrics::new()), rule_engine.clone());

        let enabled_target = TestTarget::new("enabled-target", "webhook", true);
        let disabled_target = TestTarget::new("disabled-target", "webhook", false);

        let mut rules_map = RulesMap::new();
        rules_map.add_rule_config(&[EventName::ObjectCreatedPut], "*".to_string(), enabled_target.id.clone());
        rules_map.add_rule_config(&[EventName::ObjectCreatedPut], "*".to_string(), disabled_target.id.clone());

        rule_engine.set_bucket_rules("bucket", rules_map).await;
        notifier
            .target_list()
            .write()
            .await
            .add(Arc::new(enabled_target.clone()))
            .unwrap();
        notifier
            .target_list()
            .write()
            .await
            .add(Arc::new(disabled_target.clone()))
            .unwrap();

        let event = Arc::new(Event::new_test_event("bucket", "object", EventName::ObjectCreatedPut));
        notifier.send(event).await;

        assert_eq!(enabled_target.save_calls.load(Ordering::SeqCst), 1);
        assert_eq!(disabled_target.save_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn send_event_respects_prefix_suffix_filters() {
        let rule_engine = NotifyRuleEngine::new();
        let notifier = EventNotifier::new(Arc::new(NotificationMetrics::new()), rule_engine.clone());
        let target = TestTarget::new("filtered-target", "webhook", true);
        let mut rules_map = RulesMap::new();
        rules_map.add_rule_config(&[EventName::ObjectCreatedPut], "uploads/*.csv".to_string(), target.id.clone());

        rule_engine.set_bucket_rules("bucket", rules_map).await;
        notifier.target_list().write().await.add(Arc::new(target.clone())).unwrap();

        notifier
            .send(Arc::new(Event::new_test_event("bucket", "report.csv", EventName::ObjectCreatedPut)))
            .await;
        notifier
            .send(Arc::new(Event::new_test_event(
                "bucket",
                "uploads/report.txt",
                EventName::ObjectCreatedPut,
            )))
            .await;

        assert_eq!(target.save_calls.load(Ordering::SeqCst), 0);

        notifier
            .send(Arc::new(Event::new_test_event(
                "bucket",
                "uploads/report.csv",
                EventName::ObjectCreatedPut,
            )))
            .await;

        assert_eq!(target.save_calls.load(Ordering::SeqCst), 1);
    }
}
