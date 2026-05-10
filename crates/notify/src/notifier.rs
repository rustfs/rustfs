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

use crate::{error::NotificationError, event::Event, integration::NotificationMetrics, rules::RulesMap};
use rustfs_config::notify::{DEFAULT_NOTIFY_SEND_CONCURRENCY, ENV_NOTIFY_SEND_CONCURRENCY};
use rustfs_s3_common::EventName;
use rustfs_targets::arn::TargetID;
use rustfs_targets::target::EntityTarget;
use rustfs_targets::{SharedTarget, Target, TargetRuntimeManager};
use starshard::AsyncShardedHashMap;
use std::sync::Arc;
use tokio::sync::{RwLock, Semaphore};
use tracing::{debug, error, info, instrument, warn};

/// Manages event notification to targets based on rules
pub struct EventNotifier {
    metrics: Arc<NotificationMetrics>,
    target_list: Arc<RwLock<TargetList>>,
    bucket_rules_map: Arc<AsyncShardedHashMap<String, RulesMap, rustc_hash::FxBuildHasher>>,
    send_limiter: Arc<Semaphore>,
}

impl Default for EventNotifier {
    fn default() -> Self {
        Self::new(Arc::new(NotificationMetrics::new()))
    }
}

impl EventNotifier {
    /// Creates a new EventNotifier
    ///
    /// # Returns
    /// Returns a new instance of EventNotifier.
    pub fn new(metrics: Arc<NotificationMetrics>) -> Self {
        let max_inflight = rustfs_utils::get_env_usize(ENV_NOTIFY_SEND_CONCURRENCY, DEFAULT_NOTIFY_SEND_CONCURRENCY);
        EventNotifier {
            metrics,
            target_list: Arc::new(RwLock::new(TargetList::new())),
            bucket_rules_map: Arc::new(AsyncShardedHashMap::new(0)),
            send_limiter: Arc::new(Semaphore::new(max_inflight)),
        }
    }

    /// Checks whether a TargetID is still referenced by any bucket's rules.
    ///
    /// # Arguments
    /// * `target_id` - The TargetID to check.
    ///
    /// # Returns
    /// Returns `true` if the TargetID is bound to any bucket, otherwise `false`.
    pub async fn is_target_bound_to_any_bucket(&self, target_id: &TargetID) -> bool {
        // `AsyncShardedHashMap::iter()`: Traverse (bucket_name, rules_map)
        let items = self.bucket_rules_map.iter().await;
        for (_bucket, rules_map) in items {
            if rules_map.contains_target_id(target_id) {
                return true;
            }
        }
        false
    }

    /// Returns a reference to the target list
    /// This method provides access to the target list for external use.
    ///
    /// # Returns
    /// Returns an `Arc<RwLock<TargetList>>` representing the target list.
    pub fn target_list(&self) -> Arc<RwLock<TargetList>> {
        Arc::clone(&self.target_list)
    }

    /// Removes all notification rules for a bucket
    ///
    /// # Arguments
    /// * `bucket` - The name of the bucket for which to remove rules
    ///
    /// This method removes all rules associated with the specified bucket name.
    /// It will log a message indicating the removal of rules.
    pub async fn remove_rules_map(&self, bucket: &str) {
        if self.bucket_rules_map.remove(&bucket.to_string()).await.is_some() {
            info!("Removed all notification rules for bucket: {}", bucket);
        }
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

    /// Adds a rules map for a bucket
    ///
    /// # Arguments
    /// * `bucket` - The name of the bucket for which to add the rules map
    /// * `rules_map` - The rules map to add for the bucket
    pub async fn add_rules_map(&self, bucket: &str, rules_map: RulesMap) {
        if rules_map.is_empty() {
            self.bucket_rules_map.remove(&bucket.to_string()).await;
        } else {
            self.bucket_rules_map.insert(bucket.to_string(), rules_map).await;
        }
        info!("Added rules for bucket: {}", bucket);
    }

    /// Gets the rules map for a specific bucket.
    ///
    /// # Arguments
    /// * `bucket` - The name of the bucket for which to get the rules map
    ///
    /// # Returns
    /// Returns `Some(RulesMap)` if rules exist for the bucket, otherwise returns `None`.
    pub async fn get_rules_map(&self, bucket: &str) -> Option<RulesMap> {
        self.bucket_rules_map.get(&bucket.to_string()).await
    }

    /// Removes notification rules for a bucket
    ///
    /// # Arguments
    /// * `bucket` - The name of the bucket for which to remove notification rules
    pub async fn remove_notification(&self, bucket: &str) {
        self.bucket_rules_map.remove(&bucket.to_string()).await;
        info!("Removed notification rules for bucket: {}", bucket);
    }

    /// Removes all targets
    pub async fn remove_all_bucket_targets(&self) {
        let mut target_list_guard = self.target_list.write().await;
        // The logic for sending cancel signals via stream_cancel_senders would be removed.
        // TargetList::clear_targets_only already handles calling target.close().
        target_list_guard.clear_targets_only().await; // Modified clear to not re-cancel
        info!("Removed all targets and their streams");
    }

    /// Checks if there are active subscribers for the given bucket and event name.
    ///
    /// # Parameters
    /// * `bucket_name` - bucket name.
    /// * `event_name` - Event name.
    ///
    /// # Return value
    /// Return `true` if at least one matching notification rule exists.
    pub async fn has_subscriber(&self, bucket_name: &str, event_name: &EventName) -> bool {
        // Rules to check if the bucket exists
        if let Some(rules_map) = self.bucket_rules_map.get(&bucket_name.to_string()).await {
            // A composite event (such as ObjectCreatedAll) is expanded to multiple single events.
            // We need to check whether any of these single events have the rules configured.
            rules_map.has_subscriber(event_name)
        } else {
            // If no bucket is found, no subscribers
            false
        }
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

        let Some(rules) = self.bucket_rules_map.get(bucket_name).await else {
            debug!("No rules found for bucket: {}", bucket_name);
            self.metrics.increment_skipped();
            return;
        };

        let target_ids = rules.match_rules(event_name, object_key);
        if target_ids.is_empty() {
            debug!("No matching targets for event in bucket: {}", bucket_name);
            self.metrics.increment_skipped();
            return;
        }
        let target_ids_len = target_ids.len();
        let mut handles = vec![];

        // Use scope to limit the borrow scope of target_list
        let target_list_guard = self.target_list.read().await;
        info!("Sending event to targets: {:?}", target_ids);
        for target_id in target_ids {
            // `get` now returns Option<Arc<dyn Target + Send + Sync>>
            if let Some(target_arc) = target_list_guard.get(&target_id) {
                // Clone an Arc<Box<dyn Target>> (which is where target_list is stored) to move into an asynchronous task
                // target_arc is already Arc, clone it for the async task
                let target_for_task = target_arc.clone();
                if !target_for_task.is_enabled() {
                    debug!("Skipping disabled target: {}", target_for_task.name());
                    continue;
                }
                let limiter = self.send_limiter.clone();
                let metrics = self.metrics.clone();
                let event_clone = event.clone();
                let is_deferred = target_for_task.store().is_some();
                let target_name_for_task = target_for_task.name(); // Get the name before generating the task
                debug!("Preparing to send event to target: {}", target_name_for_task);
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
                            error!("Failed to acquire send permit for target {}: {}", target_name_for_task, e);
                            metrics.increment_failed();
                            return;
                        }
                    };
                    if let Err(e) = target_for_task.save(entity_target.clone()).await {
                        metrics.increment_failed();
                        error!("Failed to send event to target {}: {}", target_name_for_task, e);
                    } else {
                        if is_deferred {
                            metrics.decrement_processing();
                        } else {
                            metrics.increment_processed();
                        }
                        debug!("Successfully saved event to target {}", target_name_for_task);
                    }
                });
                handles.push(handle);
            } else {
                warn!("Target ID {:?} found in rules but not in target list.", target_id);
                self.metrics.increment_skipped();
            }
        }
        // target_list is automatically released here
        drop(target_list_guard);

        // Wait for all tasks to be completed
        for handle in handles {
            if let Err(e) = handle.await {
                error!("Task for sending/saving event failed: {}", e);
            }
        }
        info!("Event processing initiated for {} targets for bucket: {}", target_ids_len, bucket_name);
    }

    /// Initializes the targets for buckets
    ///
    /// # Arguments
    /// * `targets_to_init` - A vector of boxed targets to initialize
    ///
    /// # Returns
    /// Returns `Ok(())` if initialization is successful, otherwise returns a `NotificationError`.
    #[instrument(skip(self, targets_to_init))]
    pub async fn init_bucket_targets(
        &self,
        targets_to_init: Vec<Box<dyn Target<Event> + Send + Sync>>,
    ) -> Result<(), NotificationError> {
        // Currently active, simpler logic
        let mut target_list_guard = self.target_list.write().await; //Gets a write lock for the TargetList

        // Clear existing targets first - rebuild from scratch to ensure consistency with new configuration
        target_list_guard.clear();

        for target_boxed in targets_to_init {
            // Traverse the incoming Box<dyn Target >
            debug!("init bucket target: {}", target_boxed.name());
            // TargetList::add method expectations Arc<dyn Target + Send + Sync>
            // Therefore, you need to convert Box<dyn Target + Send + Sync> to Arc<dyn Target + Send + Sync>
            let target_arc: Arc<dyn Target<Event> + Send + Sync> = Arc::from(target_boxed);
            target_list_guard.add(target_arc)?; // Add Arc<dyn Target> to the list
        }
        info!(
            "Initialized {} targets, list size: {}", // Clearer logs
            target_list_guard.len(),
            target_list_guard.len()
        );
        Ok(()) // Make sure to return a Result
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
            warn!("Target with ID {} already exists in TargetList. It will be overwritten.", id);
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
    use async_trait::async_trait;
    use rustfs_s3_common::EventName;
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
        let notifier = EventNotifier::new(Arc::new(NotificationMetrics::new()));

        let enabled_target = TestTarget::new("enabled-target", "webhook", true);
        let disabled_target = TestTarget::new("disabled-target", "webhook", false);

        let mut rules_map = RulesMap::new();
        rules_map.add_rule_config(&[EventName::ObjectCreatedPut], "*".to_string(), enabled_target.id.clone());
        rules_map.add_rule_config(&[EventName::ObjectCreatedPut], "*".to_string(), disabled_target.id.clone());

        notifier.add_rules_map("bucket", rules_map).await;
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
}
