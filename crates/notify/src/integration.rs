use crate::arn::TargetID;
use crate::store::{Key, Store};
use crate::{
    config::{parse_config, Config}, error::NotificationError, notifier::EventNotifier, registry::TargetRegistry,
    rules::BucketNotificationConfig,
    stream,
    Event,
    StoreError,
    Target,
    KVS,
};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, RwLock, Semaphore};
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

    // 提供公共方法增加计数
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

    // 提供公共方法获取计数
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
}

impl Default for NotificationSystem {
    fn default() -> Self {
        Self::new()
    }
}

impl NotificationSystem {
    /// Creates a new NotificationSystem
    pub fn new() -> Self {
        NotificationSystem {
            notifier: Arc::new(EventNotifier::new()),
            registry: Arc::new(TargetRegistry::new()),
            config: Arc::new(RwLock::new(Config::new())),
            stream_cancellers: Arc::new(RwLock::new(HashMap::new())),
            concurrency_limiter: Arc::new(Semaphore::new(
                std::env::var("RUSTFS_TARGET_STREAM_CONCURRENCY")
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(20),
            )), // Limit the maximum number of concurrent processing events to 20
            metrics: Arc::new(NotificationMetrics::new()),
        }
    }

    /// Initializes the notification system
    pub async fn init(&self) -> Result<(), NotificationError> {
        info!("Initialize notification system...");

        let config = self.config.read().await;
        debug!(
            "Initializing notification system with config: {:?}",
            *config
        );
        let targets: Vec<Box<dyn Target + Send + Sync>> =
            self.registry.create_targets_from_config(&config).await?;

        info!("{} notification targets were created", targets.len());

        // Initiate event stream processing for each storage enabled target
        let mut cancellers = HashMap::new();
        for target in &targets {
            let target_id = target.id();
            info!("Initializing target: {}", target.id());
            // Initialize the target
            if let Err(e) = target.init().await {
                error!("Target {} Initialization failed:{}", target.id(), e);
                continue;
            }
            debug!(
                "Target {} initialized successfully,enabled:{}",
                target_id,
                target.is_enabled()
            );
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
                    let cancel_tx = self.enhanced_start_event_stream(
                        store_clone,
                        target_arc,
                        metrics,
                        semaphore,
                    );

                    // Start event stream processing and save cancel sender
                    let target_id_clone = target_id.clone();
                    cancellers.insert(target_id, cancel_tx);
                    info!(
                        "Event stream processing for target {} is started successfully",
                        target_id_clone
                    );
                } else {
                    info!(
                        "Target {} No storage is configured, event stream processing is skipped",
                        target_id
                    );
                }
            } else {
                info!(
                    "Target {} is not enabled, event stream processing is skipped",
                    target_id
                );
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

    /// 通过 TargetID 精确地移除一个 Target 及其相关资源。
    ///
    /// 这个过程包括：
    /// 1. 停止与该 Target 关联的事件流（如果存在）。
    /// 2. 从 Notifier 的活动列表中移除该 Target 实例。
    /// 3. 从系统配置中移除该 Target 的配置项。
    ///
    /// # 参数
    /// * `target_id` - 要移除的 Target 的唯一标识符。
    ///
    /// # 返回
    /// 如果成功，则返回 `Ok(())`。
    pub async fn remove_target(
        &self,
        target_id: &TargetID,
        target_type: &str,
    ) -> Result<(), NotificationError> {
        info!("Attempting to remove target: {}", target_id);

        // 步骤 1: 停止事件流 (如果存在)
        let mut cancellers_guard = self.stream_cancellers.write().await;
        if let Some(cancel_tx) = cancellers_guard.remove(target_id) {
            info!("Stopping event stream for target {}", target_id);
            // 发送停止信号，即使失败也继续执行，因为接收端可能已经关闭
            if let Err(e) = cancel_tx.send(()).await {
                error!(
                    "Failed to send stop signal to target {} stream: {}",
                    target_id, e
                );
            }
        } else {
            info!(
                "No active event stream found for target {}, skipping stop.",
                target_id
            );
        }
        drop(cancellers_guard);

        // 步骤 2: 从 Notifier 的活动列表中移除 Target 实例
        // TargetList::remove_target_only 会调用 target.close()
        let target_list = self.notifier.target_list();
        let mut target_list_guard = target_list.write().await;
        if target_list_guard
            .remove_target_only(target_id)
            .await
            .is_some()
        {
            info!("Removed target {} from the active list.", target_id);
        } else {
            warn!("Target {} was not found in the active list.", target_id);
        }
        drop(target_list_guard);

        // 步骤 3: 从持久化配置中移除 Target
        let mut config_guard = self.config.write().await;
        let mut changed = false;
        if let Some(targets_of_type) = config_guard.get_mut(target_type) {
            if targets_of_type.remove(&target_id.name).is_some() {
                info!("Removed target {} from the configuration.", target_id);
                changed = true;
            }
            // 如果该类型下已无任何 target，则移除该类型条目
            if targets_of_type.is_empty() {
                config_guard.remove(target_type);
            }
        }

        if !changed {
            warn!("Target {} was not found in the configuration.", target_id);
        }

        Ok(())
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
    pub async fn set_target_config(
        &self,
        target_type: &str,
        target_name: &str,
        kvs: KVS,
    ) -> Result<(), NotificationError> {
        info!(
            "Setting config for target {} of type {}",
            target_name, target_type
        );
        let mut config_guard = self.config.write().await;
        config_guard
            .entry(target_type.to_string())
            .or_default()
            .insert(target_name.to_string(), kvs);

        let new_config = config_guard.clone();
        // Release the lock before calling reload_config
        drop(config_guard);

        self.reload_config(new_config).await
    }

    /// Removes all notification configurations for a bucket.
    pub async fn remove_bucket_notification_config(&self, bucket_name: &str) {
        self.notifier.remove_rules_map(bucket_name).await;
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
    pub async fn remove_target_config(
        &self,
        target_type: &str,
        target_name: &str,
    ) -> Result<(), NotificationError> {
        info!(
            "Removing config for target {} of type {}",
            target_name, target_type
        );
        let mut config_guard = self.config.write().await;
        let mut changed = false;

        if let Some(targets) = config_guard.get_mut(target_type) {
            if targets.remove(target_name).is_some() {
                changed = true;
            }
            if targets.is_empty() {
                config_guard.remove(target_type);
            }
        }

        if changed {
            let new_config = config_guard.clone();
            // Release the lock before calling reload_config
            drop(config_guard);
            self.reload_config(new_config).await
        } else {
            info!(
                "Target {} of type {} not found, no changes made.",
                target_name, target_type
            );
            Ok(())
        }
    }

    /// Enhanced event stream startup function, including monitoring and concurrency control
    fn enhanced_start_event_stream(
        &self,
        store: Box<dyn Store<Event, Error = StoreError, Key = Key> + Send>,
        target: Arc<dyn Target + Send + Sync>,
        metrics: Arc<NotificationMetrics>,
        semaphore: Arc<Semaphore>,
    ) -> mpsc::Sender<()> {
        // Event Stream Processing Using Batch Version
        stream::start_event_stream_with_batching(store, target, metrics, semaphore)
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

        // Update the config
        {
            let mut config = self.config.write().await;
            *config = new_config.clone();
        }

        // Create a new target from configuration
        let targets: Vec<Box<dyn Target + Send + Sync>> = self
            .registry
            .create_targets_from_config(&new_config)
            .await
            .map_err(NotificationError::Target)?;

        info!(
            "{} notification targets were created from the new configuration",
            targets.len()
        );

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
                    let target_box = target.clone_dyn();
                    let target_arc = Arc::from(target_box);

                    // Add a reference to the monitoring metrics
                    let metrics = self.metrics.clone();
                    let semaphore = self.concurrency_limiter.clone();

                    // Encapsulated enhanced version of start_event_stream
                    let cancel_tx = self.enhanced_start_event_stream(
                        store_clone,
                        target_arc,
                        metrics,
                        semaphore,
                    );

                    // Start event stream processing and save cancel sender
                    // let cancel_tx = start_event_stream(store_clone, target_clone);
                    let target_id_clone = target_id.clone();
                    new_cancellers.insert(target_id, cancel_tx);
                    info!(
                        "Event stream processing of target {} is restarted successfully",
                        target_id_clone
                    );
                } else {
                    info!(
                        "Target {} No storage is configured, event stream processing is skipped",
                        target_id
                    );
                }
            } else {
                info!(
                    "Target {} disabled, event stream processing is skipped",
                    target_id
                );
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
        bucket_name: &str,
        config: &BucketNotificationConfig,
    ) -> Result<(), NotificationError> {
        let arn_list = self.notifier.get_arn_list(&config.region).await;
        if arn_list.is_empty() {
            return Err(NotificationError::Configuration(
                "No targets configured".to_string(),
            ));
        }
        info!("Available ARNs: {:?}", arn_list);
        // Validate the configuration against the available ARNs
        if let Err(e) = config.validate(&config.region, &arn_list) {
            debug!(
                "Bucket notification config validation region:{} failed: {}",
                &config.region, e
            );
            if !e.to_string().contains("ARN not found") {
                return Err(NotificationError::BucketNotification(e.to_string()));
            } else {
                error!("{}", e);
            }
        }

        // let rules_map = config.to_rules_map();
        let rules_map = config.get_rules_map();
        self.notifier
            .add_rules_map(bucket_name, rules_map.clone())
            .await;
        info!("Loaded notification config for bucket: {}", bucket_name);
        Ok(())
    }

    /// Sends an event
    pub async fn send_event(
        &self,
        bucket_name: &str,
        event_name: &str,
        object_key: &str,
        event: Event,
    ) {
        self.notifier
            .send(bucket_name, event_name, object_key, event)
            .await;
    }

    /// Obtain system status information
    pub fn get_status(&self) -> HashMap<String, String> {
        let mut status = HashMap::new();

        status.insert(
            "uptime_seconds".to_string(),
            self.metrics.uptime().as_secs().to_string(),
        );
        status.insert(
            "processing_events".to_string(),
            self.metrics.processing_count().to_string(),
        );
        status.insert(
            "processed_events".to_string(),
            self.metrics.processed_count().to_string(),
        );
        status.insert(
            "failed_events".to_string(),
            self.metrics.failed_count().to_string(),
        );

        status
    }

    // Add a method to shut down the system
    pub async fn shutdown(&self) {
        info!("Turn off the notification system");

        // Get the number of active targets
        let active_targets = self.stream_cancellers.read().await.len();
        info!(
            "Stops {} active event stream processing tasks",
            active_targets
        );

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
pub async fn load_config_from_file(
    path: &str,
    system: &NotificationSystem,
) -> Result<(), NotificationError> {
    let config_str = tokio::fs::read_to_string(path).await.map_err(|e| {
        NotificationError::Configuration(format!("Failed to read config file: {}", e))
    })?;

    let config = parse_config(&config_str)
        .map_err(|e| NotificationError::Configuration(format!("Failed to parse config: {}", e)))?;

    system.reload_config(config).await
}
