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
use crate::notifier::{EventNotifier, TargetList};
use crate::services::NotifyServices;
use crate::{
    Event, error::NotificationError, pipeline::LiveEventHistory, registry::TargetRegistry, rules::BucketNotificationConfig,
};
use hashbrown::HashMap;
use rustfs_config::notify::{DEFAULT_NOTIFY_TARGET_STREAM_CONCURRENCY, ENV_NOTIFY_TARGET_STREAM_CONCURRENCY};
use rustfs_ecstore::config::{Config, KVS};
use rustfs_s3_common::EventName;
use rustfs_targets::arn::TargetID;
use rustfs_targets::{ReplayWorkerManager, RuntimeTargetHealthSnapshot, SharedTarget};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::{RwLock, Semaphore, broadcast};
use tracing::info;

#[derive(Clone)]
pub struct LiveEventBatch {
    pub events: Vec<Arc<Event>>,
    pub next_sequence: u64,
    pub truncated: bool,
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
    services: NotifyServices,
}

impl NotificationSystem {
    /// Creates a new NotificationSystem
    pub fn new(config: Config) -> Self {
        let concurrency_limiter =
            rustfs_utils::get_env_usize(ENV_NOTIFY_TARGET_STREAM_CONCURRENCY, DEFAULT_NOTIFY_TARGET_STREAM_CONCURRENCY);
        let (live_event_sender, _) = broadcast::channel(1024);
        let metrics = Arc::new(NotificationMetrics::new());
        let subscriber_view = Arc::new(NotificationSystemSubscriberView::new());
        let notifier = Arc::new(EventNotifier::new(metrics.clone()));
        let target_list = notifier.target_list();
        let registry = Arc::new(TargetRegistry::new());
        let config = Arc::new(RwLock::new(config));
        let stream_cancellers = Arc::new(RwLock::new(ReplayWorkerManager::new()));
        let concurrency_limiter = Arc::new(Semaphore::new(concurrency_limiter)); // Limit the maximum number of concurrent processing events to 20
        let live_event_history = Arc::new(RwLock::new(LiveEventHistory::default()));
        let services = NotifyServices::new(
            notifier.clone(),
            target_list,
            registry.clone(),
            config.clone(),
            stream_cancellers,
            concurrency_limiter,
            metrics,
            subscriber_view,
            live_event_sender,
            live_event_history,
        );

        NotificationSystem {
            notifier,
            registry,
            config,
            services,
        }
    }

    /// Initializes the notification system
    pub async fn init(&self) -> Result<(), NotificationError> {
        self.services.config_manager.init().await
    }

    /// Gets a list of Targets for all currently active (initialized).
    ///
    /// # Return
    /// A Vec containing all active Targets `TargetID`.
    pub async fn get_active_targets(&self) -> Vec<TargetID> {
        self.services.runtime_view.get_active_targets().await
    }

    /// Gets the complete Target list, including both active and inactive Targets.
    ///
    /// # Return
    /// An `Arc<RwLock<TargetList>>` containing all Targets.
    pub async fn get_all_targets(&self) -> Arc<RwLock<TargetList>> {
        self.services.runtime_view.get_all_targets()
    }

    /// Gets all Target values, including both active and inactive Targets.
    ///
    /// # Return
    /// A Vec containing all Targets.
    pub async fn get_target_values(&self) -> Vec<SharedTarget<Event>> {
        self.services.runtime_view.get_target_values().await
    }

    /// Checks if there are active subscribers for the given bucket and event name.
    pub async fn has_subscriber(&self, bucket: &str, event: &EventName) -> bool {
        self.services.bucket_config_manager.has_subscriber(bucket, event).await
    }

    /// Returns true when at least one in-process consumer is subscribed to live events.
    pub fn has_live_listeners(&self) -> bool {
        self.services.pipeline.has_live_listeners()
    }

    /// Subscribes to the in-process live event stream.
    pub fn subscribe_live_events(&self) -> broadcast::Receiver<Arc<Event>> {
        self.services.pipeline.subscribe_live_events()
    }

    pub async fn recent_live_events_since(&self, after_sequence: u64, limit: usize) -> LiveEventBatch {
        self.services.pipeline.recent_live_events_since(after_sequence, limit).await
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
        self.services.config_manager.remove_target(target_id, target_type).await
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
        self.services
            .config_manager
            .set_target_config(target_type, target_name, kvs)
            .await
    }

    /// Removes all notification configurations for a bucket.
    /// If the configuration is successfully removed, the entire notification system will be automatically reloaded.
    ///
    /// # Arguments
    /// * `bucket` - The name of the bucket whose notification configuration is to be removed.
    ///
    pub async fn remove_bucket_notification_config(&self, bucket: &str) {
        self.services
            .bucket_config_manager
            .remove_bucket_notification_config(bucket)
            .await;
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
        self.services
            .config_manager
            .remove_target_config(target_type, target_name)
            .await
    }

    /// Reloads the configuration
    pub async fn reload_config(&self, new_config: Config) -> Result<(), NotificationError> {
        self.services.config_manager.reload_config(new_config).await
    }

    /// Loads the bucket notification configuration
    pub async fn load_bucket_notification_config(
        &self,
        bucket: &str,
        cfg: &BucketNotificationConfig,
    ) -> Result<(), NotificationError> {
        self.services
            .bucket_config_manager
            .load_bucket_notification_config(bucket, cfg)
            .await
    }

    /// Sends an event
    pub async fn send_event(&self, event: Arc<Event>) {
        self.services.pipeline.send_event(event).await;
    }

    /// Obtain system status information
    pub fn get_status(&self) -> HashMap<String, String> {
        self.services.status_view.get_status()
    }

    pub fn snapshot_metrics(&self) -> NotificationMetricSnapshot {
        self.services.status_view.snapshot_metrics()
    }

    pub async fn snapshot_target_metrics(&self) -> Vec<NotificationTargetMetricSnapshot> {
        self.services.runtime_view.snapshot_target_metrics().await
    }

    pub async fn snapshot_target_health(&self) -> Vec<RuntimeTargetHealthSnapshot> {
        self.services.runtime_view.snapshot_target_health().await
    }

    pub async fn runtime_status_snapshot(&self) -> rustfs_targets::RuntimeStatusSnapshot {
        self.services.runtime_view.runtime_status_snapshot().await
    }

    // Add a method to shut down the system
    pub async fn shutdown(&self) {
        self.services.runtime_facade.shutdown().await;
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

    #[tokio::test]
    async fn notification_system_exposes_live_event_pipeline() {
        let system = NotificationSystem::new(Config::default());
        assert!(!system.has_live_listeners());

        let _rx = system.subscribe_live_events();
        assert!(system.has_live_listeners());

        system
            .send_event(Arc::new(Event::new_test_event("bucket", "object", EventName::ObjectCreatedPut)))
            .await;

        let batch = system.recent_live_events_since(0, 16).await;
        assert_eq!(batch.events.len(), 1);
        assert_eq!(batch.events[0].s3.object.key, "object");
        assert_eq!(batch.next_sequence, 1);
        assert!(!batch.truncated);
    }
}
