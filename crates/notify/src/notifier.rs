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

use crate::{error::NotificationError, event::Event, rules::RulesMap};
use dashmap::DashMap;
use rustfs_targets::EventName;
use rustfs_targets::Target;
use rustfs_targets::arn::TargetID;
use rustfs_targets::target::EntityTarget;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::RwLock;
use tracing::{debug, error, info, instrument, warn};

/// Manages event notification to targets based on rules
pub struct EventNotifier {
    target_list: Arc<RwLock<TargetList>>,
    bucket_rules_map: Arc<DashMap<String, RulesMap>>,
}

impl Default for EventNotifier {
    fn default() -> Self {
        Self::new()
    }
}

impl EventNotifier {
    /// Creates a new EventNotifier
    pub fn new() -> Self {
        EventNotifier {
            target_list: Arc::new(RwLock::new(TargetList::new())),
            bucket_rules_map: Arc::new(DashMap::new()),
        }
    }

    /// Returns a reference to the target list
    /// This method provides access to the target list for external use.
    ///
    pub fn target_list(&self) -> Arc<RwLock<TargetList>> {
        Arc::clone(&self.target_list)
    }

    /// Removes all notification rules for a bucket
    ///
    /// # Arguments
    /// * `bucket_name` - The name of the bucket for which to remove rules
    ///
    /// This method removes all rules associated with the specified bucket name.
    /// It will log a message indicating the removal of rules.
    pub async fn remove_rules_map(&self, bucket_name: &str) {
        if self.bucket_rules_map.remove(bucket_name).is_some() {
            info!("Removed all notification rules for bucket: {}", bucket_name);
        }
    }

    /// Returns a list of ARNs for the registered targets
    pub async fn get_arn_list(&self, region: &str) -> Vec<String> {
        let target_list_guard = self.target_list.read().await;
        target_list_guard
            .keys()
            .iter()
            .map(|target_id| target_id.to_arn(region).to_arn_string())
            .collect()
    }

    /// Adds a rules map for a bucket
    pub async fn add_rules_map(&self, bucket_name: &str, rules_map: RulesMap) {
        if rules_map.is_empty() {
            self.bucket_rules_map.remove(bucket_name);
        } else {
            self.bucket_rules_map.insert(bucket_name.to_string(), rules_map);
        }
        info!("Added rules for bucket: {}", bucket_name);
    }

    /// Gets the rules map for a specific bucket.
    pub fn get_rules_map(&self, bucket_name: &str) -> Option<RulesMap> {
        self.bucket_rules_map.get(bucket_name).map(|r| r.clone())
    }

    /// Removes notification rules for a bucket
    pub async fn remove_notification(&self, bucket_name: &str) {
        self.bucket_rules_map.remove(bucket_name);
        info!("Removed notification rules for bucket: {}", bucket_name);
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
        if let Some(rules_map) = self.bucket_rules_map.get(bucket_name) {
            // A composite event (such as ObjectCreatedAll) is expanded to multiple single events.
            // We need to check whether any of these single events have the rules configured.
            rules_map.has_subscriber(event_name)
        } else {
            // If no bucket is found, no subscribers
            false
        }
    }

    /// Sends an event to the appropriate targets based on the bucket rules
    #[instrument(skip_all)]
    pub async fn send(&self, event: Arc<Event>) {
        let bucket_name = &event.s3.bucket.name;
        let object_key = &event.s3.object.key;
        let event_name = event.event_name;
        if let Some(rules) = self.bucket_rules_map.get(bucket_name) {
            let target_ids = rules.match_rules(event_name, object_key);
            if target_ids.is_empty() {
                debug!("No matching targets for event in bucket: {}", bucket_name);
                return;
            }
            let target_ids_len = target_ids.len();
            let mut handles = vec![];

            // Use scope to limit the borrow scope of target_list
            {
                let target_list_guard = self.target_list.read().await;
                info!("Sending event to targets: {:?}", target_ids);
                for target_id in target_ids {
                    // `get` now returns Option<Arc<dyn Target + Send + Sync>>
                    if let Some(target_arc) = target_list_guard.get(&target_id) {
                        // Clone an Arc<Box<dyn Target>> (which is where target_list is stored) to move into an asynchronous task
                        // target_arc is already Arc, clone it for the async task
                        let cloned_target_for_task = target_arc.clone();
                        let event_clone = event.clone();
                        let target_name_for_task = cloned_target_for_task.name(); // Get the name before generating the task
                        debug!("Preparing to send event to target: {}", target_name_for_task);
                        // Use cloned data in closures to avoid borrowing conflicts
                        // Create an EntityTarget from the event
                        let entity_target: Arc<EntityTarget<Event>> = Arc::new(EntityTarget {
                            object_name: object_key.to_string(),
                            bucket_name: bucket_name.to_string(),
                            event_name,
                            data: event_clone.clone().as_ref().clone(),
                        });
                        let handle = tokio::spawn(async move {
                            if let Err(e) = cloned_target_for_task.save(entity_target.clone()).await {
                                error!("Failed to send event to target {}: {}", target_name_for_task, e);
                            } else {
                                debug!("Successfully saved event to target {}", target_name_for_task);
                            }
                        });
                        handles.push(handle);
                    } else {
                        warn!("Target ID {:?} found in rules but not in target list.", target_id);
                    }
                }
                // target_list is automatically released here
            }

            // Wait for all tasks to be completed
            for handle in handles {
                if let Err(e) = handle.await {
                    error!("Task for sending/saving event failed: {}", e);
                }
            }
            info!("Event processing initiated for {} targets for bucket: {}", target_ids_len, bucket_name);
        } else {
            debug!("No rules found for bucket: {}", bucket_name);
        }
    }

    /// Initializes the targets for buckets
    #[instrument(skip(self, targets_to_init))]
    pub async fn init_bucket_targets(
        &self,
        targets_to_init: Vec<Box<dyn Target<Event> + Send + Sync>>,
    ) -> Result<(), NotificationError> {
        // Currently active, simpler logic
        let mut target_list_guard = self.target_list.write().await; //Gets a write lock for the TargetList
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
    targets: HashMap<TargetID, Arc<dyn Target<Event> + Send + Sync>>,
}

impl Default for TargetList {
    fn default() -> Self {
        Self::new()
    }
}

impl TargetList {
    /// Creates a new TargetList
    pub fn new() -> Self {
        TargetList { targets: HashMap::new() }
    }

    /// Adds a target to the list
    pub fn add(&mut self, target: Arc<dyn Target<Event> + Send + Sync>) -> Result<(), NotificationError> {
        let id = target.id();
        if self.targets.contains_key(&id) {
            // Potentially update or log a warning/error if replacing an existing target.
            warn!("Target with ID {} already exists in TargetList. It will be overwritten.", id);
        }
        self.targets.insert(id, target);
        Ok(())
    }

    /// Removes a target by ID. Note: This does not stop its associated event stream.
    /// Stream cancellation should be handled by EventNotifier.
    pub async fn remove_target_only(&mut self, id: &TargetID) -> Option<Arc<dyn Target<Event> + Send + Sync>> {
        if let Some(target_arc) = self.targets.remove(id) {
            if let Err(e) = target_arc.close().await {
                // Target's own close logic
                error!("Failed to close target {} during removal: {}", id, e);
            }
            Some(target_arc)
        } else {
            None
        }
    }

    /// Clears all targets from the list. Note: This does not stop their associated event streams.
    /// Stream cancellation should be handled by EventNotifier.
    pub async fn clear_targets_only(&mut self) {
        let target_ids_to_clear: Vec<TargetID> = self.targets.keys().cloned().collect();
        for id in target_ids_to_clear {
            if let Some(target_arc) = self.targets.remove(&id) {
                if let Err(e) = target_arc.close().await {
                    error!("Failed to close target {} during clear: {}", id, e);
                }
            }
        }
        self.targets.clear();
    }

    /// Returns a target by ID
    pub fn get(&self, id: &TargetID) -> Option<Arc<dyn Target<Event> + Send + Sync>> {
        self.targets.get(id).cloned()
    }

    /// Returns all target IDs
    pub fn keys(&self) -> Vec<TargetID> {
        self.targets.keys().cloned().collect()
    }

    /// Returns the number of targets
    pub fn len(&self) -> usize {
        self.targets.len()
    }

    // is_empty can be derived from len()
    pub fn is_empty(&self) -> bool {
        self.targets.is_empty()
    }
}
