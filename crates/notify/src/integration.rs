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
    Event,
    error::NotificationError,
    notifier::EventNotifier,
    registry::TargetRegistry,
    rules::{BucketNotificationConfig, ParseConfigError},
    stream,
};
use hashbrown::HashMap;
use rustfs_config::notify::{
<<<<<<< HEAD
    DEFAULT_NOTIFY_TARGET_STREAM_CONCURRENCY, ENV_NOTIFY_TARGET_STREAM_CONCURRENCY, ENV_NOTIFY_WEBHOOK_ENABLE,
    ENV_NOTIFY_WEBHOOK_ENDPOINT, NOTIFY_KAFKA_SUB_SYS, NOTIFY_MQTT_SUB_SYS, NOTIFY_MYSQL_SUB_SYS, NOTIFY_NATS_SUB_SYS,
    NOTIFY_POSTGRES_SUB_SYS, NOTIFY_PULSAR_SUB_SYS, NOTIFY_REDIS_SUB_SYS, NOTIFY_WEBHOOK_SUB_SYS,
=======
    DEFAULT_NOTIFY_TARGET_STREAM_CONCURRENCY, ENV_NOTIFY_TARGET_STREAM_CONCURRENCY, NOTIFY_AMQP_SUB_SYS, NOTIFY_KAFKA_SUB_SYS,
    NOTIFY_MQTT_SUB_SYS, NOTIFY_NATS_SUB_SYS, NOTIFY_PULSAR_SUB_SYS, NOTIFY_WEBHOOK_SUB_SYS,
>>>>>>> 56f1dc85 (feat(targets): implement AMQP notification target)
};
use rustfs_config::{ENV_NOTIFY_ENABLE, EVENT_DEFAULT_DIR};
use rustfs_ecstore::config::{Config, KVS};
use rustfs_s3_common::EventName;
use rustfs_targets::arn::TargetID;
use rustfs_targets::store::{Key, Store};
use rustfs_targets::target::QueuedPayload;
use rustfs_targets::{StoreError, Target};
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::{RwLock, Semaphore, broadcast, mpsc};
use tracing::{debug, info, warn};

const MAX_RECENT_LIVE_EVENTS: usize = 1024;

fn notify_configuration_hint() -> String {
    let webhook_enable_primary = format!("{ENV_NOTIFY_WEBHOOK_ENABLE}_PRIMARY");
    let webhook_endpoint_primary = format!("{ENV_NOTIFY_WEBHOOK_ENDPOINT}_PRIMARY");
    format!(
        "No notify targets configured. Check {ENV_NOTIFY_ENABLE}=true and instance-scoped target env vars (for example {webhook_enable_primary} + {webhook_endpoint_primary} for arn:rustfs:sqs::primary:webhook). If using default queue_dir, ensure {EVENT_DEFAULT_DIR} is writable."
    )
}

fn subsystem_target_type(target_type: &str) -> &str {
    match target_type {
        NOTIFY_AMQP_SUB_SYS => "amqp",
        NOTIFY_WEBHOOK_SUB_SYS => "webhook",
        NOTIFY_KAFKA_SUB_SYS => "kafka",
        NOTIFY_MQTT_SUB_SYS => "mqtt",
        NOTIFY_MYSQL_SUB_SYS => "mysql",
        NOTIFY_NATS_SUB_SYS => "nats",
        NOTIFY_POSTGRES_SUB_SYS => "postgres",
        NOTIFY_PULSAR_SUB_SYS => "pulsar",
        NOTIFY_REDIS_SUB_SYS => "redis",
        _ => target_type,
    }
}

fn runtime_target_id_for_subsystem(target_type: &str, target_name: &str) -> TargetID {
    TargetID {
        id: target_name.to_lowercase(),
        name: subsystem_target_type(target_type).to_string(),
    }
}

#[derive(Clone)]
pub struct LiveEventBatch {
    pub events: Vec<Arc<Event>>,
    pub next_sequence: u64,
    pub truncated: bool,
}

#[derive(Default)]
struct LiveEventHistory {
    next_sequence: u64,
    events: VecDeque<(u64, Arc<Event>)>,
}

impl LiveEventHistory {
    fn record(&mut self, event: Arc<Event>) {
        self.next_sequence = self.next_sequence.saturating_add(1);
        self.events.push_back((self.next_sequence, event));
        while self.events.len() > MAX_RECENT_LIVE_EVENTS {
            self.events.pop_front();
        }
    }

    fn snapshot_since(&self, after_sequence: u64, limit: usize) -> LiveEventBatch {
        let mut events = Vec::new();
        let mut next_sequence = after_sequence;
        let mut truncated = false;

        for (sequence, event) in self.events.iter() {
            if *sequence <= after_sequence {
                continue;
            }
            if events.len() >= limit {
                truncated = true;
                break;
            }
            next_sequence = *sequence;
            events.push(event.clone());
        }

        LiveEventBatch {
            events,
            next_sequence,
            truncated,
        }
    }
}

/// Notify the system of monitoring indicators
pub struct NotificationMetrics {
    /// The number of events currently being processed
    processing_events: AtomicUsize,
    /// Number of events that have been successfully processed
    processed_events: AtomicUsize,
    /// Number of events that failed to handle
    failed_events: AtomicUsize,
    /// Number of dispatch attempts skipped before delivery
    skipped_events: AtomicUsize,
    /// System startup time
    start_time: Instant,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct NotificationMetricSnapshot {
    pub current_send_in_progress: u64,
    pub events_errors_total: u64,
    pub events_sent_total: u64,
    pub events_skipped_total: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct NotificationTargetMetricSnapshot {
    pub failed_messages: u64,
    pub queue_length: u64,
    pub target_id: String,
    pub target_type: String,
    pub total_messages: u64,
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
            skipped_events: AtomicUsize::new(0),
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

    pub fn decrement_processing(&self) {
        self.processing_events.fetch_sub(1, Ordering::Relaxed);
    }

    pub fn increment_failed(&self) {
        self.processing_events.fetch_sub(1, Ordering::Relaxed);
        self.failed_events.fetch_add(1, Ordering::Relaxed);
    }

    pub fn increment_skipped(&self) {
        self.skipped_events.fetch_add(1, Ordering::Relaxed);
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

    pub fn skipped_count(&self) -> usize {
        self.skipped_events.load(Ordering::Relaxed)
    }

    pub fn uptime(&self) -> Duration {
        self.start_time.elapsed()
    }

    pub fn snapshot(&self) -> NotificationMetricSnapshot {
        NotificationMetricSnapshot {
            current_send_in_progress: self.processing_count() as u64,
            events_errors_total: self.failed_count() as u64,
            events_sent_total: self.processed_count() as u64,
            events_skipped_total: self.skipped_count() as u64,
        }
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
    /// Live event fan-out for in-process streaming consumers.
    live_event_sender: broadcast::Sender<Arc<Event>>,
    /// Recent live event history for peer fan-in consumers.
    live_event_history: Arc<RwLock<LiveEventHistory>>,
}

impl NotificationSystem {
    /// Creates a new NotificationSystem
    pub fn new(config: Config) -> Self {
        let concurrency_limiter =
            rustfs_utils::get_env_usize(ENV_NOTIFY_TARGET_STREAM_CONCURRENCY, DEFAULT_NOTIFY_TARGET_STREAM_CONCURRENCY);
        let (live_event_sender, _) = broadcast::channel(1024);
        let metrics = Arc::new(NotificationMetrics::new());
        NotificationSystem {
            subscriber_view: NotificationSystemSubscriberView::new(),
            notifier: Arc::new(EventNotifier::new(metrics.clone())),
            registry: Arc::new(TargetRegistry::new()),
            config: Arc::new(RwLock::new(config)),
            stream_cancellers: Arc::new(RwLock::new(HashMap::new())),
            concurrency_limiter: Arc::new(Semaphore::new(concurrency_limiter)), // Limit the maximum number of concurrent processing events to 20
            metrics,
            live_event_sender,
            live_event_history: Arc::new(RwLock::new(LiveEventHistory::default())),
        }
    }

    /// Initializes targets and starts event streams for those with stores.
    /// Returns a map of (target_id -> cancel_sender) for streams that were started.
    async fn init_targets_and_start_streams(
        &self,
        targets: &[Box<dyn Target<Event> + Send + Sync>],
    ) -> HashMap<TargetID, mpsc::Sender<()>> {
        let mut cancellers = HashMap::new();
        for target in targets {
            let target_id = target.id();
            info!("Initializing target: {}", target_id);

            let has_store = target.store().is_some();

            if let Err(e) = target.init().await {
                warn!("Target {} Initialization failed: {}", target_id, e);
                // For targets without a store, init failure is fatal — skip.
                // For store-backed targets, still start the stream so queued events
                // can be drained when connectivity recovers (send_from_store retries).
                if !has_store {
                    continue;
                }
                warn!(
                    "Target {} has a store, starting stream despite init failure — \
                     connectivity will be retried by send_from_store",
                    target_id
                );
            } else {
                debug!("Target {} initialized successfully, enabled: {}", target_id, target.is_enabled());
            }

            if !target.is_enabled() {
                info!("Target {} is not enabled, event stream processing is skipped", target_id);
                continue;
            }

            if let Some(store) = target.store() {
                info!("Start event stream processing for target {}", target_id);

                let store_clone = store.boxed_clone();
                let target_arc = Arc::from(target.clone_dyn());

                let cancel_tx = self.enhanced_start_event_stream(
                    store_clone,
                    target_arc,
                    self.metrics.clone(),
                    self.concurrency_limiter.clone(),
                );

                let target_id_clone = target_id.clone();
                cancellers.insert(target_id, cancel_tx);
                info!("Event stream processing for target {} is started successfully", target_id_clone);
            } else {
                info!("Target {} No storage is configured, event stream processing is skipped", target_id);
            }
        }
        cancellers
    }

    /// Initializes the notification system
    pub async fn init(&self) -> Result<(), NotificationError> {
        info!("Initialize notification system...");

        let config = {
            let guard = self.config.read().await;
            debug!(
                subsystem_count = guard.0.len(),
                "Initializing notification system with configuration summary"
            );
            guard.clone()
        };

        let targets: Vec<Box<dyn Target<Event> + Send + Sync>> = self.registry.create_targets_from_config(&config).await?;

        info!("{} notification targets were created", targets.len());
        if targets.is_empty() {
            warn!("{}", notify_configuration_hint());
        }

        // Initialize targets and start event streams
        let cancellers = self.init_targets_and_start_streams(&targets).await;

        // Update canceller collection
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

    /// Returns true when at least one in-process consumer is subscribed to live events.
    pub fn has_live_listeners(&self) -> bool {
        self.live_event_sender.receiver_count() > 0
    }

    /// Subscribes to the in-process live event stream.
    pub fn subscribe_live_events(&self) -> broadcast::Receiver<Arc<Event>> {
        self.live_event_sender.subscribe()
    }

    pub async fn recent_live_events_since(&self, after_sequence: u64, limit: usize) -> LiveEventBatch {
        let history = self.live_event_history.read().await;
        history.snapshot_since(after_sequence, limit.max(1))
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
        let tname = target_id.id.to_lowercase();

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
        let target_id = runtime_target_id_for_subsystem(&ttype, &tname);

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
                        config.0.remove(&ttype);
                    }
                }
                if !changed {
                    info!("Target {} of type {} not found, no changes made.", target_name, target_type);
                }
                debug!(
                    subsystem_count = config.0.len(),
                    "Target config removal processed and configuration summary updated"
                );
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
        store: Box<dyn Store<QueuedPayload, Error = StoreError, Key = Key> + Send>,
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

        // Clear the target_list and ensure that reload is a replacement reconstruction
        self.notifier.remove_all_bucket_targets().await;

        // Update the config
        self.update_config(new_config.clone()).await;

        // Create new targets from configuration
        let targets: Vec<Box<dyn Target<Event> + Send + Sync>> = self
            .registry
            .create_targets_from_config(&new_config)
            .await
            .map_err(NotificationError::Target)?;

        info!("{} notification targets were created from the new configuration", targets.len());
        if targets.is_empty() {
            warn!("{}", notify_configuration_hint());
        }

        // Initialize targets and start event streams using shared helper
        let new_cancellers = self.init_targets_and_start_streams(&targets).await;

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
        let arn_list = self.notifier.get_arn_list(&cfg.region).await;
        if arn_list.is_empty() {
            return Err(NotificationError::Configuration(notify_configuration_hint()));
        }
        info!("Available ARNs: {:?}", arn_list);
        // Validate the configuration against the available ARNs
        if let Err(e) = cfg.validate(&cfg.region, &arn_list) {
            debug!("Bucket notification config validation region:{} failed: {}", &cfg.region, e);
            if !matches!(e, ParseConfigError::ArnNotFound(_)) {
                return Err(NotificationError::BucketNotification(e.to_string()));
            }
            warn!(
                bucket = %bucket,
                region = %cfg.region,
                error = %e,
                "Bucket notification config references missing target ARN; keeping compatibility and loading remaining rules"
            );
        }

        self.subscriber_view.apply_bucket_config(bucket, cfg);
        let rules_map = cfg.get_rules_map();
        self.notifier.add_rules_map(bucket, rules_map.clone()).await;
        info!("Loaded notification config for bucket: {}", bucket);
        Ok(())
    }

    /// Sends an event
    pub async fn send_event(&self, event: Arc<Event>) {
        self.live_event_history.write().await.record(event.clone());
        let _ = self.live_event_sender.send(event.clone());
        self.notifier.send(event).await;
    }

    /// Obtain system status information
    pub fn get_status(&self) -> HashMap<String, String> {
        let mut status = HashMap::new();

        status.insert("uptime_seconds".to_string(), self.metrics.uptime().as_secs().to_string());
        status.insert("processing_events".to_string(), self.metrics.processing_count().to_string());
        status.insert("processed_events".to_string(), self.metrics.processed_count().to_string());
        status.insert("failed_events".to_string(), self.metrics.failed_count().to_string());
        status.insert("skipped_events".to_string(), self.metrics.skipped_count().to_string());

        status
    }

    pub fn snapshot_metrics(&self) -> NotificationMetricSnapshot {
        self.metrics.snapshot()
    }

    pub async fn snapshot_target_metrics(&self) -> Vec<NotificationTargetMetricSnapshot> {
        let targets = self.notifier.target_list().read().await.values();
        let mut snapshots = Vec::with_capacity(targets.len());

        for target in targets {
            let delivery = target.delivery_snapshot();
            let target_id = target.id();
            snapshots.push(NotificationTargetMetricSnapshot {
                failed_messages: delivery.failed_messages,
                queue_length: delivery.queue_length,
                target_id: target_id.to_string(),
                target_type: target_id.name,
                total_messages: delivery.total_messages,
            });
        }

        snapshots.sort_by(|a, b| a.target_id.cmp(&b.target_id));
        snapshots
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

#[cfg(test)]
mod tests {
    use super::*;
    use rustfs_s3_common::EventName;

    #[test]
    fn live_event_history_snapshots_from_sequence() {
        let mut history = LiveEventHistory::default();
        history.record(Arc::new(Event::new_test_event("bucket", "one", EventName::ObjectCreatedPut)));
        history.record(Arc::new(Event::new_test_event("bucket", "two", EventName::ObjectCreatedPut)));

        let batch = history.snapshot_since(1, 16);

        assert_eq!(batch.next_sequence, 2);
        assert!(!batch.truncated);
        assert_eq!(batch.events.len(), 1);
        assert_eq!(batch.events[0].s3.object.key, "two");
    }

    #[test]
    fn live_event_history_marks_truncation() {
        let mut history = LiveEventHistory::default();
        history.record(Arc::new(Event::new_test_event("bucket", "one", EventName::ObjectCreatedPut)));
        history.record(Arc::new(Event::new_test_event("bucket", "two", EventName::ObjectCreatedPut)));

        let batch = history.snapshot_since(0, 1);

        assert_eq!(batch.next_sequence, 1);
        assert!(batch.truncated);
        assert_eq!(batch.events.len(), 1);
        assert_eq!(batch.events[0].s3.object.key, "one");
    }

    #[test]
    fn runtime_target_id_for_subsystem_maps_notify_webhook_to_runtime_type() {
        let target_id = runtime_target_id_for_subsystem(NOTIFY_WEBHOOK_SUB_SYS, "Primary");
        assert_eq!(target_id.id, "primary");
        assert_eq!(target_id.name, "webhook");
    }

    #[test]
    fn runtime_target_id_for_subsystem_maps_notify_amqp_to_runtime_type() {
        let target_id = runtime_target_id_for_subsystem(NOTIFY_AMQP_SUB_SYS, "Primary");
        assert_eq!(target_id.id, "primary");
        assert_eq!(target_id.name, "amqp");
    }

    #[test]
    fn runtime_target_id_for_subsystem_maps_notify_mqtt_to_runtime_type() {
        let target_id = runtime_target_id_for_subsystem(NOTIFY_MQTT_SUB_SYS, "Analytics");
        assert_eq!(target_id.id, "analytics");
        assert_eq!(target_id.name, "mqtt");
    }

    #[test]
    fn runtime_target_id_for_subsystem_maps_notify_kafka_to_runtime_type() {
        let target_id = runtime_target_id_for_subsystem(NOTIFY_KAFKA_SUB_SYS, "EventBus");
        assert_eq!(target_id.id, "eventbus");
        assert_eq!(target_id.name, "kafka");
    }

    #[test]
    fn runtime_target_id_for_subsystem_maps_notify_nats_to_runtime_type() {
        let target_id = runtime_target_id_for_subsystem(NOTIFY_NATS_SUB_SYS, "Bus");
        assert_eq!(target_id.id, "bus");
        assert_eq!(target_id.name, "nats");
    }

    #[test]
    fn runtime_target_id_for_subsystem_maps_notify_pulsar_to_runtime_type() {
        let target_id = runtime_target_id_for_subsystem(NOTIFY_PULSAR_SUB_SYS, "Ledger");
        assert_eq!(target_id.id, "ledger");
        assert_eq!(target_id.name, "pulsar");
    }

    #[test]
    fn runtime_target_id_for_subsystem_maps_notify_redis_to_runtime_type() {
        let target_id = runtime_target_id_for_subsystem(NOTIFY_REDIS_SUB_SYS, "Primary");
        assert_eq!(target_id.id, "primary");
        assert_eq!(target_id.name, "redis");
    }
    #[test]
    fn runtime_target_id_for_subsystem_maps_notify_postgres_to_runtime_type() {
        let target_id = runtime_target_id_for_subsystem(NOTIFY_POSTGRES_SUB_SYS, "AuditTrail");
        assert_eq!(target_id.id, "audittrail");
        assert_eq!(target_id.name, "postgres");
    }
}
