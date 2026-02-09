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

use crate::notification_system_subscriber::NotificationSystemSubscriberView;
use crate::notifier::TargetList;
use crate::{
    Event, error::NotificationError, notifier::EventNotifier, registry::TargetRegistry, rules::BucketNotificationConfig, stream,
};
use hashbrown::HashMap;
use rustfs_config::notify::{DEFAULT_NOTIFY_TARGET_STREAM_CONCURRENCY, ENV_NOTIFY_TARGET_STREAM_CONCURRENCY};
use rustfs_ecstore::config::{Config, KVS};
use rustfs_targets::EventName;
use rustfs_targets::arn::TargetID;
use rustfs_targets::store::{Key, Store};
use rustfs_targets::target::EntityTarget;
use rustfs_targets::{StoreError, Target};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::{RwLock, Semaphore, mpsc};
use tracing::{debug, error, info, warn};

/// Notify the system of monitoring indicators
pub struct NotificationMetrics {
    /// The number of events currently being processed
    processing_events: AtomicUsize,
    /// Number of events that have been successfully processed
    processed_events: AtomicUsize,
    /// Number of events that failed to handle
    failed_events: AtomicUsize,
    /// System startup time
    start_time: Instant,
}

impl Default for NotificationMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl NotificationMetrics {
    pub fn new() -> Self {
        NotificationMetrics {
            processing_events: AtomicUsize::new(0),
            processed_events: AtomicUsize::new(0),
            failed_events: AtomicUsize::new(0),
            start_time: Instant::now(),
        }
    }

    // Provide public methods to increase count
    pub fn increment_processing(&self) {
        self.processing_events.fetch_add(1, Ordering::Relaxed);
    }

    pub fn increment_processed(&self) {
        self.processing_events.fetch_sub(1, Ordering::Relaxed);
        self.processed_events.fetch_add(1, Ordering::Relaxed);
    }

    pub fn increment_failed(&self) {
        self.processing_events.fetch_sub(1, Ordering::Relaxed);
        self.failed_events.fetch_add(1, Ordering::Relaxed);
    }

    // Provide public methods to get count
    pub fn processing_count(&self) -> usize {
        self.processing_events.load(Ordering::Relaxed)
    }

    pub fn processed_count(&self) -> usize {
        self.processed_events.load(Ordering::Relaxed)
    }

    pub fn failed_count(&self) -> usize {
        self.failed_events.load(Ordering::Relaxed)
    }

    pub fn uptime(&self) -> Duration {
        self.start_time.elapsed()
    }
}

/// The notification system that integrates all components
pub struct NotificationSystem {
    /// The event notifier
    pub notifier: Arc<EventNotifier>,
    /// The target registry
    pub registry: Arc<TargetRegistry>,
    /// The current configuration
    pub config: Arc<RwLock<Config>>,
    /// Cancel sender for managing stream processing tasks
    stream_cancellers: Arc<RwLock<HashMap<TargetID, mpsc::Sender<()>>>>,
    /// Concurrent control signal quantity
    concurrency_limiter: Arc<Semaphore>,
    /// Monitoring indicators
    metrics: Arc<NotificationMetrics>,
    /// Subscriber view
    subscriber_view: NotificationSystemSubscriberView,
}

impl NotificationSystem {
    /// Creates a new NotificationSystem
    pub fn new(config: Config) -> Self {
        let concurrency_limiter =
            rustfs_utils::get_env_usize(ENV_NOTIFY_TARGET_STREAM_CONCURRENCY, DEFAULT_NOTIFY_TARGET_STREAM_CONCURRENCY);
        NotificationSystem {
            subscriber_view: NotificationSystemSubscriberView::new(),
            notifier: Arc::new(EventNotifier::new()),
            registry: Arc::new(TargetRegistry::new()),
            config: Arc::new(RwLock::new(config)),
            stream_cancellers: Arc::new(RwLock::new(HashMap::new())),
            concurrency_limiter: Arc::new(Semaphore::new(concurrency_limiter)), // Limit the maximum number of concurrent processing events to 20
            metrics: Arc::new(NotificationMetrics::new()),
        }
    }

    /// Initializes the notification system
    pub async fn init(&self) -> Result<(), NotificationError> {
        info!("Initialize notification system...");

        let config = self.config.read().await;
        debug!("Initializing notification system with config: {:?}", *config);
        let targets: Vec<Box<dyn Target<Event> + Send + Sync>> = self.registry.create_targets_from_config(&config).await?;

        info!("{} notification targets were created", targets.len());

        // Initiate event stream processing for each storage enabled target
        let mut cancellers = HashMap::new();
        for target in &targets {
            let target_id = target.id();
            info!("Initializing target: {}", target.id());
            // Initialize the target
            if let Err(e) = target.init().await {
                warn!("Target {} Initialization failed:{}", target.id(), e);
                continue;
            }
            debug!("Target {} initialized successfully,enabled:{}", target_id, target.is_enabled());
            // Check if the target is enabled and has storage
            if target.is_enabled() {
                if let Some(store) = target.store() {
                    info!("Start event stream processing for target {}", target.id());

                    // The storage of the cloned target and the target itself
                    let store_clone = store.boxed_clone();
                    let target_box = target.clone_dyn();
                    let target_arc = Arc::from(target_box);

                    // Add a reference to the monitoring metrics
                    let metrics = self.metrics.clone();
                    let semaphore = self.concurrency_limiter.clone();

                    // Encapsulated enhanced version of start_event_stream
                    let cancel_tx = self.enhanced_start_event_stream(store_clone, target_arc, metrics, semaphore);

                    // Start event stream processing and save cancel sender
                    let target_id_clone = target_id.clone();
                    cancellers.insert(target_id, cancel_tx);
                    info!("Event stream processing for target {} is started successfully", target_id_clone);
                } else {
                    info!("Target {} No storage is configured, event stream processing is skipped", target_id);
                }
            } else {
                info!("Target {} is not enabled, event stream processing is skipped", target_id);
            }
        }

        // Update canceler collection
        *self.stream_cancellers.write().await = cancellers;
        // Initialize the bucket target
        self.notifier.init_bucket_targets(targets).await?;
        info!("Notification system initialized");
        Ok(())
    }

    /// Gets a list of Targets for all currently active (initialized).
    ///
    /// # Return
    /// A Vec containing all active Targets `TargetID`.
    pub async fn get_active_targets(&self) -> Vec<TargetID> {
        self.notifier.target_list().read().await.keys()
    }

    /// Gets the complete Target list, including both active and inactive Targets.
    ///
    /// # Return
    /// An `Arc<RwLock<TargetList>>` containing all Targets.
    pub async fn get_all_targets(&self) -> Arc<RwLock<TargetList>> {
        self.notifier.target_list()
    }

    /// Gets all Target values, including both active and inactive Targets.
    ///
    /// # Return
    /// A Vec containing all Targets.
    pub async fn get_target_values(&self) -> Vec<Arc<dyn Target<Event> + Send + Sync>> {
        self.notifier.target_list().read().await.values()
    }

    /// Checks if there are active subscribers for the given bucket and event name.
    pub async fn has_subscriber(&self, bucket: &str, event: &EventName) -> bool {
        if !self.subscriber_view.has_subscriber(bucket, event) {
            return false;
        }
        self.notifier.has_subscriber(bucket, event).await
    }

    async fn update_config_and_reload<F>(&self, mut modifier: F) -> Result<(), NotificationError>
    where
        F: FnMut(&mut Config) -> bool, // The closure returns a boolean value indicating whether the configuration has been changed
    {
        let Some(store) = rustfs_ecstore::global::new_object_layer_fn() else {
            return Err(NotificationError::StorageNotAvailable(
                "Failed to save target configuration: server storage not initialized".to_string(),
            ));
        };

        let mut new_config = rustfs_ecstore::config::com::read_config_without_migrate(store.clone())
            .await
            .map_err(|e| NotificationError::ReadConfig(e.to_string()))?;

        if !modifier(&mut new_config) {
            // If the closure indication has not changed, return in advance
            info!("Configuration not changed, skipping save and reload.");
            return Ok(());
        }

        // Save the modified configuration to storage
        rustfs_ecstore::config::com::save_server_config(store, &new_config)
            .await
            .map_err(|e| NotificationError::SaveConfig(e.to_string()))?;

        info!("Configuration updated. Reloading system...");
        self.reload_config(new_config).await
    }

    /// Accurately remove a Target and its related resources through TargetID.
    ///
    /// This process includes:
    /// 1. Stop the event stream associated with the Target (if present).
    /// 2. Remove the Target instance from the activity list of Notifier.
    /// 3. Remove the configuration item of the Target from the system configuration.
    ///
    /// # Parameters
    /// * `target_id` - The unique identifier of the Target to be removed.
    ///
    /// # return
    /// If successful, return `Ok(())`.
    pub async fn remove_target(&self, target_id: &TargetID, target_type: &str) -> Result<(), NotificationError> {
        info!("Attempting to remove target: {}", target_id);

        let ttype = target_type.to_lowercase();
        let tname = target_id.name.to_lowercase();

        self.update_config_and_reload(|config| {
            let mut changed = false;
            if let Some(targets_of_type) = config.0.get_mut(&ttype) {
                if targets_of_type.remove(&tname).is_some() {
                    info!("Removed target {} from configuration", target_id);
                    changed = true;
                }
                if targets_of_type.is_empty() {
                    config.0.remove(&ttype);
                }
            }
            if !changed {
                warn!("Target {} not found in configuration", target_id);
            }
            changed
        })
        .await
    }

    /// Set or update a Target configuration.
    /// If the configuration is changed, the entire notification system will be automatically reloaded to apply the changes.
    ///
    /// # Arguments
    /// * `target_type` - Target type, such as "notify_webhook" or "notify_mqtt".
    /// * `target_name` - A unique name for a Target, such as "1".
    /// * `kvs` - The full configuration of the Target.
    ///
    /// # Returns
    /// Result<(), NotificationError>
    /// If the target configuration is successfully set, it returns Ok(()).
    /// If the target configuration is invalid, it returns Err(NotificationError::Configuration).
    pub async fn set_target_config(&self, target_type: &str, target_name: &str, kvs: KVS) -> Result<(), NotificationError> {
        info!("Setting config for target {} of type {}", target_name, target_type);
        let ttype = target_type.to_lowercase();
        let tname = target_name.to_lowercase();
        self.update_config_and_reload(|config| {
            config.0.entry(ttype.clone()).or_default().insert(tname.clone(), kvs.clone());
            true // The configuration is always modified
        })
        .await
    }

    /// Removes all notification configurations for a bucket.
    /// If the configuration is successfully removed, the entire notification system will be automatically reloaded.
    ///
    /// # Arguments
    /// * `bucket` - The name of the bucket whose notification configuration is to be removed.
    ///
    pub async fn remove_bucket_notification_config(&self, bucket: &str) {
        self.subscriber_view.clear_bucket(bucket);
        self.notifier.remove_rules_map(bucket).await;
    }

    /// Removes a Target configuration.
    /// If the configuration is successfully removed, the entire notification system will be automatically reloaded.
    ///
    /// # Arguments
    /// * `target_type` - Target type, such as "notify_webhook" or "notify_mqtt".
    /// * `target_name` - A unique name for a Target, such as "1".
    ///
    /// # Returns
    /// Result<(), NotificationError>
    ///
    /// If the target configuration is successfully removed, it returns Ok(()).
    /// If the target configuration does not exist, it returns Ok(()) without making any changes.
    pub async fn remove_target_config(&self, target_type: &str, target_name: &str) -> Result<(), NotificationError> {
        info!("Removing config for target {} of type {}", target_name, target_type);

        let ttype = target_type.to_lowercase();
        let tname = target_name.to_lowercase();

        let target_id = TargetID {
            id: tname.clone(),
            name: ttype.clone(),
        };

        // Deletion is prohibited if bucket rules refer to it
        if self.notifier.is_target_bound_to_any_bucket(&target_id).await {
            return Err(NotificationError::Configuration(format!(
                "Target is still bound to bucket rules and deletion is prohibited: type={} name={}",
                ttype, tname
            )));
        }

        let config_result = self
            .update_config_and_reload(|config| {
                let mut changed = false;
                if let Some(targets) = config.0.get_mut(&ttype) {
                    if targets.remove(&tname).is_some() {
                        changed = true;
                    }
                    if targets.is_empty() {
                        config.0.remove(target_type);
                    }
                }
                if !changed {
                    info!("Target {} of type {} not found, no changes made.", target_name, target_type);
                }
                debug!("Config after remove: {:?}", config);
                changed
            })
            .await;

        if config_result.is_ok() {
            // Remove from target list
            let target_list = self.notifier.target_list();
            let mut target_list_guard = target_list.write().await;
            let _ = target_list_guard.remove_target_only(&target_id).await;
        }

        config_result
    }

    /// Enhanced event stream startup function, including monitoring and concurrency control
    fn enhanced_start_event_stream(
        &self,
        store: Box<dyn Store<EntityTarget<Event>, Error = StoreError, Key = Key> + Send>,
        target: Arc<dyn Target<Event> + Send + Sync>,
        metrics: Arc<NotificationMetrics>,
        semaphore: Arc<Semaphore>,
    ) -> mpsc::Sender<()> {
        stream::start_event_stream_with_batching(store, target, metrics, semaphore)
    }

    /// Update configuration
    async fn update_config(&self, new_config: Config) {
        let mut config = self.config.write().await;
        *config = new_config;
    }

    /// Reloads the configuration
    pub async fn reload_config(&self, new_config: Config) -> Result<(), NotificationError> {
        info!("Reload notification configuration starts");

        // Stop all existing streaming services
        let mut cancellers = self.stream_cancellers.write().await;
        for (target_id, cancel_tx) in cancellers.drain() {
            info!("Stop event stream processing for target {}", target_id);
            let _ = cancel_tx.send(()).await;
        }

        // Clear the target_list and ensure that reload is a replacement reconstruction (solve the target_list len unchanged/residual problem)
        self.notifier.remove_all_bucket_targets().await;

        // Update the config
        self.update_config(new_config.clone()).await;

        // Create a new target from configuration
        // This function will now be responsible for merging env, creating and persisting the final configuration.
        let targets: Vec<Box<dyn Target<Event> + Send + Sync>> = self
            .registry
            .create_targets_from_config(&new_config)
            .await
            .map_err(NotificationError::Target)?;

        info!("{} notification targets were created from the new configuration", targets.len());

        // Start new event stream processing for each storage enabled target
        let mut new_cancellers = HashMap::new();
        for target in &targets {
            let target_id = target.id();

            // Initialize the target
            if let Err(e) = target.init().await {
                error!("Target {} Initialization failed:{}", target_id, e);
                continue;
            }
            // Check if the target is enabled and has storage
            if target.is_enabled() {
                if let Some(store) = target.store() {
                    info!("Start new event stream processing for target {}", target_id);

                    // The storage of the cloned target and the target itself
                    let store_clone = store.boxed_clone();
                    // let target_box = target.clone_dyn();
                    let target_arc = Arc::from(target.clone_dyn());

                    // Encapsulated enhanced version of start_event_stream
                    let cancel_tx = self.enhanced_start_event_stream(
                        store_clone,
                        target_arc,
                        self.metrics.clone(),
                        self.concurrency_limiter.clone(),
                    );

                    // Start event stream processing and save cancel sender
                    // let cancel_tx = start_event_stream(store_clone, target_clone);
                    let target_id_clone = target_id.clone();
                    new_cancellers.insert(target_id, cancel_tx);
                    info!("Event stream processing of target {} is restarted successfully", target_id_clone);
                } else {
                    info!("Target {} No storage is configured, event stream processing is skipped", target_id);
                }
            } else {
                info!("Target {} disabled, event stream processing is skipped", target_id);
            }
        }

        // Update canceler collection
        *cancellers = new_cancellers;

        // Initialize the bucket target
        self.notifier.init_bucket_targets(targets).await?;
        info!("Configuration reloaded end");
        Ok(())
    }

    /// Loads the bucket notification configuration
    pub async fn load_bucket_notification_config(
        &self,
        bucket: &str,
        cfg: &BucketNotificationConfig,
    ) -> Result<(), NotificationError> {
        self.subscriber_view.apply_bucket_config(bucket, cfg);
        let arn_list = self.notifier.get_arn_list(&cfg.region).await;
        if arn_list.is_empty() {
            return Err(NotificationError::Configuration("No targets configured".to_string()));
        }
        info!("Available ARNs: {:?}", arn_list);
        // Validate the configuration against the available ARNs
        if let Err(e) = cfg.validate(&cfg.region, &arn_list) {
            debug!("Bucket notification config validation region:{} failed: {}", &cfg.region, e);
            if !e.to_string().contains("ARN not found") {
                return Err(NotificationError::BucketNotification(e.to_string()));
            } else {
                error!("config validate failed, err: {}", e);
            }
        }

        let rules_map = cfg.get_rules_map();
        self.notifier.add_rules_map(bucket, rules_map.clone()).await;
        info!("Loaded notification config for bucket: {}", bucket);
        Ok(())
    }

    /// Sends an event
    pub async fn send_event(&self, event: Arc<Event>) {
        self.notifier.send(event).await;
    }

    /// Obtain system status information
    pub fn get_status(&self) -> HashMap<String, String> {
        let mut status = HashMap::new();

        status.insert("uptime_seconds".to_string(), self.metrics.uptime().as_secs().to_string());
        status.insert("processing_events".to_string(), self.metrics.processing_count().to_string());
        status.insert("processed_events".to_string(), self.metrics.processed_count().to_string());
        status.insert("failed_events".to_string(), self.metrics.failed_count().to_string());

        status
    }

    // Add a method to shut down the system
    pub async fn shutdown(&self) {
        info!("Turn off the notification system");

        // Get the number of active targets
        let active_targets = self.stream_cancellers.read().await.len();
        info!("Stops {} active event stream processing tasks", active_targets);

        let mut cancellers = self.stream_cancellers.write().await;
        for (target_id, cancel_tx) in cancellers.drain() {
            info!("Stop event stream processing for target {}", target_id);
            let _ = cancel_tx.send(()).await;
        }
        // Wait for a short while to make sure the task has a chance to complete
        tokio::time::sleep(Duration::from_millis(500)).await;

        info!("Notify the system to be shut down completed");
    }
}

impl Drop for NotificationSystem {
    fn drop(&mut self) {
        // Asynchronous operation cannot be used here, but logs can be recorded.
        info!("Notify the system instance to be destroyed");
        let status = self.get_status();
        for (key, value) in status {
            info!("key:{}, value:{}", key, value);
        }

        info!("Notification system status at shutdown:");
    }
}

/// Loads configuration from a file
pub async fn load_config_from_file(path: &str, system: &NotificationSystem) -> Result<(), NotificationError> {
    let config_data = tokio::fs::read(path)
        .await
        .map_err(|e| NotificationError::Configuration(format!("Failed to read config file: {e}")))?;

    let config = Config::unmarshal(config_data.as_slice())
        .map_err(|e| NotificationError::Configuration(format!("Failed to parse config: {e}")))?;
    system.reload_config(config).await
}
