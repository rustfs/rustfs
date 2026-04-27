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

use crate::heal::{
    progress::{HealProgress, HealStatistics},
    storage::HealStorageAPI,
    task::{HealOptions, HealPriority, HealRequest, HealTask, HealTaskStatus, HealType},
};
use crate::{Error, Result};
use metrics::{counter, gauge};
use rustfs_common::heal_channel::{HealAdmissionDropReason, HealAdmissionResult};
use rustfs_ecstore::disk::DiskAPI;
use rustfs_ecstore::disk::error::DiskError;
use rustfs_ecstore::global::GLOBAL_LOCAL_DISK_MAP;
use std::{
    collections::{BinaryHeap, HashMap, HashSet},
    sync::Arc,
    time::{Duration, SystemTime},
};
use tokio::{
    sync::{Mutex, Notify, RwLock},
    time::interval,
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

/// Priority queue wrapper for heal requests
/// Uses BinaryHeap for priority-based ordering while maintaining FIFO for same-priority items
#[derive(Debug)]
struct PriorityHealQueue {
    /// Heap of (priority, sequence, request) tuples
    heap: BinaryHeap<PriorityQueueItem>,
    /// Sequence counter for FIFO ordering within same priority
    sequence: u64,
    /// Set of request keys to prevent duplicates
    dedup_keys: HashSet<String>,
}

/// Wrapper for heap items to implement proper ordering
#[derive(Debug)]
struct PriorityQueueItem {
    priority: HealPriority,
    sequence: u64,
    request: HealRequest,
}

impl Eq for PriorityQueueItem {}

impl PartialEq for PriorityQueueItem {
    fn eq(&self, other: &Self) -> bool {
        self.priority == other.priority && self.sequence == other.sequence
    }
}

impl Ord for PriorityQueueItem {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // First compare by priority (higher priority first)
        match self.priority.cmp(&other.priority) {
            std::cmp::Ordering::Equal => {
                // If priorities are equal, use sequence for FIFO (lower sequence first)
                other.sequence.cmp(&self.sequence)
            }
            ordering => ordering,
        }
    }
}

impl PartialOrd for PriorityQueueItem {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum QueuePushOutcome {
    Accepted,
    Merged,
}

impl PriorityHealQueue {
    fn new() -> Self {
        Self {
            heap: BinaryHeap::new(),
            sequence: 0,
            dedup_keys: HashSet::new(),
        }
    }

    fn len(&self) -> usize {
        self.heap.len()
    }

    fn pop_next(&mut self) -> Option<HealRequest> {
        self.heap.pop().map(|item| {
            let key = Self::make_dedup_key(&item.request);
            self.dedup_keys.remove(&key);
            item.request
        })
    }

    fn is_empty(&self) -> bool {
        self.heap.is_empty()
    }

    fn push(&mut self, request: HealRequest) -> QueuePushOutcome {
        let key = Self::make_dedup_key(&request);

        // Check for duplicates
        if self.dedup_keys.contains(&key) {
            return QueuePushOutcome::Merged;
        }

        self.dedup_keys.insert(key);
        self.sequence += 1;
        self.heap.push(PriorityQueueItem {
            priority: request.priority,
            sequence: self.sequence,
            request,
        });
        QueuePushOutcome::Accepted
    }

    /// Get statistics about queue contents by priority
    fn get_priority_stats(&self) -> HashMap<HealPriority, usize> {
        let mut stats = HashMap::new();
        for item in &self.heap {
            *stats.entry(item.priority).or_insert(0) += 1;
        }
        stats
    }

    #[cfg(test)]
    fn pop(&mut self) -> Option<HealRequest> {
        self.heap.pop().map(|item| {
            let key = Self::make_dedup_key(&item.request);
            self.dedup_keys.remove(&key);
            item.request
        })
    }

    #[cfg(test)]
    fn pop_runnable<F>(&mut self, can_run: F) -> Option<HealRequest>
    where
        F: Fn(&HealRequest) -> bool,
    {
        self.pop_runnable_with_skips(can_run, |_| None).0
    }

    fn pop_runnable_with_skips<F, G>(&mut self, can_run: F, skip_label: G) -> (Option<HealRequest>, Vec<String>)
    where
        F: Fn(&HealRequest) -> bool,
        G: Fn(&HealRequest) -> Option<String>,
    {
        let mut deferred = Vec::new();
        let mut selected = None;
        let mut skipped = Vec::new();

        while let Some(item) = self.heap.pop() {
            if can_run(&item.request) {
                selected = Some(item);
                break;
            }
            if let Some(label) = skip_label(&item.request) {
                skipped.push(label);
            }
            deferred.push(item);
        }

        for item in deferred {
            self.heap.push(item);
        }

        (
            selected.map(|item| {
                let key = Self::make_dedup_key(&item.request);
                self.dedup_keys.remove(&key);
                item.request
            }),
            skipped,
        )
    }

    /// Create a deduplication key from a heal request
    fn make_dedup_key(request: &HealRequest) -> String {
        match &request.heal_type {
            HealType::Object {
                bucket,
                object,
                version_id,
            } => {
                format!("object:{}:{}:{}", bucket, object, version_id.as_deref().unwrap_or(""))
            }
            HealType::Bucket { bucket } => {
                format!("bucket:{bucket}")
            }
            HealType::ErasureSet { set_disk_id, .. } => {
                format!("erasure_set:{set_disk_id}")
            }
            HealType::Metadata { bucket, object } => {
                format!("metadata:{bucket}:{object}")
            }
            HealType::MRF { meta_path } => {
                format!("mrf:{meta_path}")
            }
            HealType::ECDecode {
                bucket,
                object,
                version_id,
            } => {
                format!("ecdecode:{}:{}:{}", bucket, object, version_id.as_deref().unwrap_or(""))
            }
        }
    }

    /// Check if a request with the same key already exists in the queue
    #[allow(dead_code)]
    fn contains_key(&self, request: &HealRequest) -> bool {
        let key = Self::make_dedup_key(request);
        self.dedup_keys.contains(&key)
    }

    /// Check if an erasure set heal request for a specific set_disk_id exists
    fn contains_erasure_set(&self, set_disk_id: &str) -> bool {
        let key = format!("erasure_set:{set_disk_id}");
        self.dedup_keys.contains(&key)
    }
}

fn publish_active_heal_count(active_heals: &HashMap<String, Arc<HealTask>>) {
    crate::set_heal_active_tasks(active_heals.len());
}

fn publish_heal_queue_length(queue: &PriorityHealQueue) {
    crate::set_heal_queue_length(queue.len());
}

/// Heal config
#[derive(Debug, Clone)]
pub struct HealConfig {
    /// Whether to enable auto heal
    pub enable_auto_heal: bool,
    /// Heal interval
    pub heal_interval: Duration,
    /// Maximum concurrent heal tasks
    pub max_concurrent_heals: usize,
    /// Maximum concurrent heal tasks allowed for a single erasure set
    pub max_concurrent_per_set: usize,
    /// Task timeout
    pub task_timeout: Duration,
    /// Queue size
    pub queue_size: usize,
    /// Whether duplicate low-priority requests should merge into an existing queued request.
    pub low_priority_merge_enable: bool,
    /// Whether low-priority requests may be dropped when the queue is full.
    pub low_priority_drop_when_full: bool,
    /// Whether notify-driven scheduler wakeups are enabled.
    pub event_driven_scheduler_enable: bool,
    /// Whether per-set bulkhead scheduling is enabled.
    pub set_bulkhead_enable: bool,
    /// Whether erasure-set page parallelism is enabled.
    pub page_parallel_enable: bool,
}

impl Default for HealConfig {
    fn default() -> Self {
        let queue_size: usize =
            rustfs_utils::get_env_usize(rustfs_config::ENV_HEAL_QUEUE_SIZE, rustfs_config::DEFAULT_HEAL_QUEUE_SIZE);
        let heal_interval = Duration::from_secs(rustfs_utils::get_env_u64(
            rustfs_config::ENV_HEAL_INTERVAL_SECS,
            rustfs_config::DEFAULT_HEAL_INTERVAL_SECS,
        ));
        let enable_auto_heal =
            rustfs_utils::get_env_bool(rustfs_config::ENV_HEAL_AUTO_HEAL_ENABLE, rustfs_config::DEFAULT_HEAL_AUTO_HEAL_ENABLE);
        let task_timeout = Duration::from_secs(rustfs_utils::get_env_u64(
            rustfs_config::ENV_HEAL_TASK_TIMEOUT_SECS,
            rustfs_config::DEFAULT_HEAL_TASK_TIMEOUT_SECS,
        ));
        let max_concurrent_heals = rustfs_utils::get_env_usize(
            rustfs_config::ENV_HEAL_MAX_CONCURRENT_HEALS,
            rustfs_config::DEFAULT_HEAL_MAX_CONCURRENT_HEALS,
        );
        let max_concurrent_per_set = rustfs_utils::get_env_usize(
            rustfs_config::ENV_HEAL_MAX_CONCURRENT_PER_SET,
            rustfs_config::DEFAULT_HEAL_MAX_CONCURRENT_PER_SET,
        );
        let low_priority_merge_enable = rustfs_utils::get_env_bool(
            rustfs_config::ENV_HEAL_LOW_PRIORITY_MERGE_ENABLE,
            rustfs_config::DEFAULT_HEAL_LOW_PRIORITY_MERGE_ENABLE,
        );
        let low_priority_drop_when_full = rustfs_utils::get_env_bool(
            rustfs_config::ENV_HEAL_LOW_PRIORITY_DROP_WHEN_FULL,
            rustfs_config::DEFAULT_HEAL_LOW_PRIORITY_DROP_WHEN_FULL,
        );
        let event_driven_scheduler_enable = rustfs_utils::get_env_bool(
            rustfs_config::ENV_HEAL_EVENT_DRIVEN_SCHEDULER_ENABLE,
            rustfs_config::DEFAULT_HEAL_EVENT_DRIVEN_SCHEDULER_ENABLE,
        );
        let set_bulkhead_enable = rustfs_utils::get_env_bool(
            rustfs_config::ENV_HEAL_SET_BULKHEAD_ENABLE,
            rustfs_config::DEFAULT_HEAL_SET_BULKHEAD_ENABLE,
        );
        let page_parallel_enable = rustfs_utils::get_env_bool(
            rustfs_config::ENV_HEAL_PAGE_PARALLEL_ENABLE,
            rustfs_config::DEFAULT_HEAL_PAGE_PARALLEL_ENABLE,
        );
        Self {
            enable_auto_heal,
            heal_interval,        // 10 seconds
            max_concurrent_heals, // max 4,
            max_concurrent_per_set: std::cmp::min(max_concurrent_heals.max(1), max_concurrent_per_set.max(1)),
            task_timeout, // 5 minutes
            queue_size,
            low_priority_merge_enable,
            low_priority_drop_when_full,
            event_driven_scheduler_enable,
            set_bulkhead_enable,
            page_parallel_enable,
        }
    }
}

/// Heal state
#[derive(Debug, Default)]
pub struct HealState {
    /// Whether running
    pub is_running: bool,
    /// Current heal cycle
    pub current_cycle: u64,
    /// Last heal time
    pub last_heal_time: Option<SystemTime>,
    /// Total healed objects
    pub total_healed_objects: u64,
    /// Total heal failures
    pub total_heal_failures: u64,
    /// Current active heal tasks
    pub active_heal_count: usize,
}

/// Heal manager
pub struct HealManager {
    /// Heal config
    config: Arc<RwLock<HealConfig>>,
    /// Heal state
    state: Arc<RwLock<HealState>>,
    /// Active heal tasks
    active_heals: Arc<Mutex<HashMap<String, Arc<HealTask>>>>,
    /// Heal queue (priority-based)
    heal_queue: Arc<Mutex<PriorityHealQueue>>,
    /// Storage layer interface
    storage: Arc<dyn HealStorageAPI>,
    /// Cancel token
    cancel_token: CancellationToken,
    /// Statistics
    statistics: Arc<RwLock<HealStatistics>>,
    /// Scheduler wake-up notifier for event-driven dispatch
    notify: Arc<Notify>,
}

impl HealManager {
    fn classify_full_admission(request: &HealRequest, config: &HealConfig) -> HealAdmissionResult {
        if request.priority == HealPriority::Low && config.low_priority_drop_when_full {
            HealAdmissionResult::Dropped(HealAdmissionDropReason::QueueFull)
        } else {
            HealAdmissionResult::Full
        }
    }

    /// Create new HealManager
    pub fn new(storage: Arc<dyn HealStorageAPI>, config: Option<HealConfig>) -> Self {
        let config = config.unwrap_or_default();
        Self {
            config: Arc::new(RwLock::new(config)),
            state: Arc::new(RwLock::new(HealState::default())),
            active_heals: Arc::new(Mutex::new(HashMap::new())),
            heal_queue: Arc::new(Mutex::new(PriorityHealQueue::new())),
            storage,
            cancel_token: CancellationToken::new(),
            statistics: Arc::new(RwLock::new(HealStatistics::new())),
            notify: Arc::new(Notify::new()),
        }
    }

    /// Start HealManager
    pub async fn start(&self) -> Result<()> {
        let mut state = self.state.write().await;
        if state.is_running {
            warn!("HealManager is already running");
            return Ok(());
        }
        state.is_running = true;
        drop(state);

        info!("Starting HealManager");

        // start scheduler
        self.start_scheduler().await?;

        // start auto disk scanner to heal unformatted disks
        self.start_auto_disk_scanner().await?;

        info!("HealManager started successfully");
        Ok(())
    }

    /// Stop HealManager
    pub async fn stop(&self) -> Result<()> {
        info!("Stopping HealManager");

        // cancel all tasks
        self.cancel_token.cancel();

        // wait for all tasks to complete
        let mut active_heals = self.active_heals.lock().await;
        for task in active_heals.values() {
            if let Err(e) = task.cancel().await {
                warn!("Failed to cancel task {}: {}", task.id, e);
            }
        }
        active_heals.clear();
        publish_active_heal_count(&active_heals);
        crate::set_heal_queue_length(0);

        // update state
        let mut state = self.state.write().await;
        state.is_running = false;

        info!("HealManager stopped successfully");
        Ok(())
    }

    /// Submit heal request
    pub async fn submit_heal_request(&self, request: HealRequest) -> Result<HealAdmissionResult> {
        let config = self.config.read().await;
        let mut queue = self.heal_queue.lock().await;

        let queue_len = queue.len();
        publish_heal_queue_length(&queue);
        let queue_capacity = config.queue_size;

        if queue.contains_key(&request) {
            let admission = if request.priority == HealPriority::Low && !config.low_priority_merge_enable {
                HealAdmissionResult::Dropped(HealAdmissionDropReason::PolicyDropped)
            } else {
                HealAdmissionResult::Merged
            };

            match admission {
                HealAdmissionResult::Merged => {
                    info!("Heal request already queued (duplicate merged): {}", request.id);
                }
                HealAdmissionResult::Dropped(reason) => {
                    warn!(
                        request_id = %request.id,
                        priority = ?request.priority,
                        reason = reason.as_str(),
                        "Dropping duplicate heal request due to admission policy"
                    );
                }
                HealAdmissionResult::Accepted | HealAdmissionResult::Full => {}
            }

            return Ok(admission);
        }

        if queue_len >= queue_capacity {
            let admission = Self::classify_full_admission(&request, &config);
            match admission {
                HealAdmissionResult::Dropped(reason) => {
                    warn!(
                        request_id = %request.id,
                        priority = ?request.priority,
                        queue_len,
                        queue_capacity,
                        reason = reason.as_str(),
                        "Dropping heal request because the queue is full"
                    );
                }
                HealAdmissionResult::Full => {
                    warn!(
                        request_id = %request.id,
                        priority = ?request.priority,
                        queue_len,
                        queue_capacity,
                        "Rejecting heal request because the queue is full"
                    );
                }
                HealAdmissionResult::Accepted | HealAdmissionResult::Merged => {}
            }
            return Ok(admission);
        }

        // Warn when queue is getting full (>80% capacity)
        let capacity_threshold = (queue_capacity as f64 * 0.8) as usize;
        if queue_len >= capacity_threshold {
            warn!(
                "Heal queue is {}% full ({}/{}). Consider increasing queue size or processing capacity.",
                (queue_len * 100) / queue_capacity,
                queue_len,
                queue_capacity
            );
        }

        let request_id = request.id.clone();
        let priority = request.priority;

        let push_outcome = queue.push(request);
        debug_assert_eq!(push_outcome, QueuePushOutcome::Accepted);
        publish_heal_queue_length(&queue);

        // Log queue statistics periodically (when adding high/urgent priority items)
        if matches!(priority, HealPriority::High | HealPriority::Urgent) {
            let stats = queue.get_priority_stats();
            info!(
                "Heal queue stats after adding {:?} priority request: total={}, urgent={}, high={}, normal={}, low={}",
                priority,
                queue_len + 1,
                stats.get(&HealPriority::Urgent).unwrap_or(&0),
                stats.get(&HealPriority::High).unwrap_or(&0),
                stats.get(&HealPriority::Normal).unwrap_or(&0),
                stats.get(&HealPriority::Low).unwrap_or(&0)
            );
        }

        drop(queue);

        info!("Submitted heal request: {} with priority: {:?}", request_id, priority);
        if config.event_driven_scheduler_enable {
            self.notify.notify_one();
        }

        Ok(HealAdmissionResult::Accepted)
    }

    /// Get task status
    pub async fn get_task_status(&self, task_id: &str) -> Result<HealTaskStatus> {
        let active_heals = self.active_heals.lock().await;
        if let Some(task) = active_heals.get(task_id) {
            Ok(task.get_status().await)
        } else {
            Err(Error::TaskNotFound {
                task_id: task_id.to_string(),
            })
        }
    }

    /// Get task progress
    pub async fn get_active_tasks_count(&self) -> usize {
        let active_heals = self.active_heals.lock().await;
        publish_active_heal_count(&active_heals);
        active_heals.len()
    }

    pub async fn get_task_progress(&self, task_id: &str) -> Result<HealProgress> {
        let active_heals = self.active_heals.lock().await;
        if let Some(task) = active_heals.get(task_id) {
            Ok(task.get_progress().await)
        } else {
            Err(Error::TaskNotFound {
                task_id: task_id.to_string(),
            })
        }
    }

    /// Cancel task
    pub async fn cancel_task(&self, task_id: &str) -> Result<()> {
        let mut active_heals = self.active_heals.lock().await;
        if let Some(task) = active_heals.get(task_id) {
            task.cancel().await?;
            active_heals.remove(task_id);
            publish_active_heal_count(&active_heals);
            info!("Cancelled heal task: {}", task_id);
            Ok(())
        } else {
            Err(Error::TaskNotFound {
                task_id: task_id.to_string(),
            })
        }
    }

    /// Get statistics
    pub async fn get_statistics(&self) -> HealStatistics {
        self.statistics.read().await.clone()
    }

    /// Get active task count
    pub async fn get_active_task_count(&self) -> usize {
        let active_heals = self.active_heals.lock().await;
        publish_active_heal_count(&active_heals);
        active_heals.len()
    }

    /// Get queue length
    pub async fn get_queue_length(&self) -> usize {
        let queue = self.heal_queue.lock().await;
        publish_heal_queue_length(&queue);
        queue.len()
    }

    /// Start scheduler
    async fn start_scheduler(&self) -> Result<()> {
        let config = self.config.clone();
        let heal_queue = self.heal_queue.clone();
        let active_heals = self.active_heals.clone();
        let cancel_token = self.cancel_token.clone();
        let statistics = self.statistics.clone();
        let storage = self.storage.clone();
        let notify = self.notify.clone();

        tokio::spawn(async move {
            let mut interval = interval(config.read().await.heal_interval);

            loop {
                let event_driven_scheduler_enable = config.read().await.event_driven_scheduler_enable;
                tokio::select! {
                    _ = cancel_token.cancelled() => {
                        info!("Heal scheduler received shutdown signal");
                        break;
                    }
                    _ = notify.notified(), if event_driven_scheduler_enable => {
                        Self::process_heal_queue(&heal_queue, &active_heals, &config, &statistics, &storage, &notify).await;
                    }
                    _ = interval.tick() => {
                        Self::process_heal_queue(&heal_queue, &active_heals, &config, &statistics, &storage, &notify).await;
                    }
                }
            }
        });

        Ok(())
    }

    /// Start background task to auto scan local disks and enqueue erasure set heal requests
    async fn start_auto_disk_scanner(&self) -> Result<()> {
        let config = self.config.clone();
        let heal_queue = self.heal_queue.clone();
        let active_heals = self.active_heals.clone();
        let cancel_token = self.cancel_token.clone();
        let storage = self.storage.clone();
        let notify = self.notify.clone();
        let mut duration = {
            let config = config.read().await;
            config.heal_interval
        };
        if duration < Duration::from_secs(10) {
            duration = Duration::from_secs(10);
        }
        info!("start_auto_disk_scanner: Starting auto disk scanner with interval: {:?}", duration);

        tokio::spawn(async move {
            let mut interval = interval(duration);

            loop {
                tokio::select! {
                    _ = cancel_token.cancelled() => {
                        info!("start_auto_disk_scanner: Auto disk scanner received shutdown signal");
                        break;
                    }
                    _ = interval.tick() => {
                        // Build list of endpoints that need healing
                        let mut endpoints = Vec::new();
                        for (_, disk_opt) in GLOBAL_LOCAL_DISK_MAP.read().await.iter() {
                            if let Some(disk) = disk_opt {
                                // detect unformatted disk via get_disk_id()
                                match disk.get_disk_id().await {
                                    Err(DiskError::UnformattedDisk) => {
                                        info!("start_auto_disk_scanner: Detected unformatted disk: {}", disk.endpoint());
                                        endpoints.push(disk.endpoint());
                                    }
                                    Err(e) => {
                                        // Log other errors for debugging
                                        tracing::warn!("start_auto_disk_scanner: Disk {} check failed: {:?}", disk.endpoint(), e);
                                    }
                                    Ok(_) => {
                                        // Disk is formatted, no action needed
                                    }
                                }
                            }
                        }

                        if endpoints.is_empty() {
                            debug!("start_auto_disk_scanner: No endpoints need healing");
                            continue;
                        }

                        // Get bucket list for erasure set healing
                        let buckets = match storage.list_buckets().await {
                            Ok(buckets) => buckets.iter().map(|b| b.name.clone()).collect::<Vec<String>>(),
                            Err(e) => {
                                error!("start_auto_disk_scanner: Failed to get bucket list for auto healing: {}", e);
                                continue;
                            }
                        };

                        // Create erasure set heal requests for each endpoint
                        for ep in endpoints {
                            let Some(set_disk_id) =
                                crate::heal::utils::format_set_disk_id_from_i32(ep.pool_idx, ep.set_idx)
                            else {
                                warn!("start_auto_disk_scanner: Skipping endpoint {} without valid pool/set index", ep);
                                continue;
                            };
                            // skip if already queued or healing
                            // Use consistent lock order: queue first, then active_heals to avoid deadlock
                            let mut skip = false;
                            {
                                let queue = heal_queue.lock().await;
                                if queue.contains_erasure_set(&set_disk_id) {
                                    skip = true;
                                }
                            }
                            if !skip {
                                let active = active_heals.lock().await;
                                if active.values().any(|task| {
                                    matches!(
                                        &task.heal_type,
                                        crate::heal::task::HealType::ErasureSet { set_disk_id: active_id, .. }
                                        if active_id == &set_disk_id
                                    )
                                }) {
                                    skip = true;
                                }
                            }

                            if skip {
                                info!("start_auto_disk_scanner: Skipping auto erasure set heal for endpoint: {} (set_disk_id: {}) because it is already queued or healing", ep, set_disk_id);
                                continue;
                            }

                            // enqueue erasure set heal request for this disk
                            let req = HealRequest::new(
                                HealType::ErasureSet {
                                    buckets: buckets.clone(),
                                    set_disk_id: set_disk_id.clone(),
                                },
                                HealOptions::default(),
                                HealPriority::Normal,
                            );
                            let mut queue = heal_queue.lock().await;
                            if matches!(queue.push(req), QueuePushOutcome::Accepted) {
                                publish_heal_queue_length(&queue);
                                let config = config.read().await;
                                if config.event_driven_scheduler_enable {
                                    notify.notify_one();
                                }
                                info!("start_auto_disk_scanner: Enqueued auto erasure set heal for endpoint: {} (set_disk_id: {})", ep, set_disk_id);
                            }
                        }
                    }
                }
            }
        });
        Ok(())
    }

    /// Process heal queue
    /// Processes multiple tasks per cycle when capacity allows and queue has high-priority items
    async fn process_heal_queue(
        heal_queue: &Arc<Mutex<PriorityHealQueue>>,
        active_heals: &Arc<Mutex<HashMap<String, Arc<HealTask>>>>,
        config: &Arc<RwLock<HealConfig>>,
        statistics: &Arc<RwLock<HealStatistics>>,
        storage: &Arc<dyn HealStorageAPI>,
        notify: &Arc<Notify>,
    ) {
        let config = config.read().await;
        let mut active_heals_guard = active_heals.lock().await;
        publish_active_heal_count(&active_heals_guard);

        // Check if new heal tasks can be started
        let active_count = active_heals_guard.len();
        if active_count >= config.max_concurrent_heals {
            return;
        }

        // Calculate how many tasks we can start this cycle
        let available_slots = config.max_concurrent_heals - active_count;

        let mut queue = heal_queue.lock().await;
        let queue_len = queue.len();
        publish_heal_queue_length(&queue);

        if queue_len == 0 {
            return;
        }

        let mut running_per_set = running_erasure_set_counts(&active_heals_guard);
        let mut tasks_started = 0usize;

        for _ in 0..available_slots {
            let selected_request = if config.set_bulkhead_enable {
                let max_concurrent_per_set = config.max_concurrent_per_set;
                let (selected_request, skipped_sets) = queue.pop_runnable_with_skips(
                    |request| can_schedule_request(request, &running_per_set, max_concurrent_per_set),
                    |request| heal_request_set_key(request).map(|_| heal_request_set_metric_label(request)),
                );
                for skipped_set in skipped_sets {
                    record_scheduler_skip(&skipped_set);
                }
                selected_request
            } else {
                queue.pop_next()
            };

            if let Some(request) = selected_request {
                let task_priority = request.priority;
                let task_type_label = heal_request_type_label(&request).to_string();
                let task_set_label = heal_request_set_metric_label(&request);
                if config.set_bulkhead_enable
                    && let Some(set_key) = heal_request_set_key(&request)
                {
                    *running_per_set.entry(set_key).or_insert(0) += 1;
                }
                let task = Arc::new(HealTask::from_request(request, storage.clone()));
                let task_id = task.id.clone();
                active_heals_guard.insert(task_id.clone(), task.clone());
                publish_active_heal_count(&active_heals_guard);
                update_task_running_metric_for_task(&active_heals_guard, task.as_ref());
                let active_heals_clone = active_heals.clone();
                let statistics_clone = statistics.clone();
                let notify_clone = notify.clone();
                let task_type_label_for_spawn = task_type_label.clone();
                let task_set_label_for_spawn = task_set_label.clone();

                // start heal task
                tokio::spawn(async move {
                    info!(
                        "Starting heal task: {} with priority: {:?}, type: {}, set: {}",
                        task_id, task_priority, task_type_label_for_spawn, task_set_label_for_spawn
                    );
                    let result = task.execute().await;
                    match result {
                        Ok(_) => {
                            info!("Heal task completed successfully: {}", task_id);
                        }
                        Err(e) => {
                            error!("Heal task failed: {} - {}", task_id, e);
                        }
                    }
                    let mut active_heals_guard = active_heals_clone.lock().await;
                    if let Some(completed_task) = active_heals_guard.remove(&task_id) {
                        publish_active_heal_count(&active_heals_guard);
                        update_task_running_metric_for_task(&active_heals_guard, completed_task.as_ref());
                        // update statistics
                        let mut stats = statistics_clone.write().await;
                        match completed_task.get_status().await {
                            HealTaskStatus::Completed => {
                                stats.update_task_completion(true);
                            }
                            _ => {
                                stats.update_task_completion(false);
                            }
                        }
                        stats.update_running_tasks(active_heals_guard.len() as u64);
                    }
                    notify_clone.notify_one();
                });
                tasks_started += 1;
            } else {
                break;
            }
        }

        // Update statistics for all started tasks
        let mut stats = statistics.write().await;
        stats.total_tasks += tasks_started as u64;
        stats.update_running_tasks(active_heals_guard.len() as u64);
        publish_active_heal_count(&active_heals_guard);
        publish_heal_queue_length(&queue);

        // Log queue status if items remain
        if !queue.is_empty() {
            let remaining = queue.len();
            if remaining > 10 {
                info!("Heal queue has {} pending requests, {} tasks active", remaining, active_heals_guard.len());
            }
        }
    }
}

impl std::fmt::Debug for HealManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HealManager")
            .field("config", &"<config>")
            .field("state", &"<state>")
            .field("active_heals_count", &"<active_heals>")
            .field("queue_length", &"<queue>")
            .finish()
    }
}

fn heal_request_set_key(request: &HealRequest) -> Option<String> {
    match &request.heal_type {
        HealType::ErasureSet { set_disk_id, .. } => Some(set_disk_id.clone()),
        _ => None,
    }
}

fn heal_request_type_label(request: &HealRequest) -> &'static str {
    match &request.heal_type {
        HealType::Object { .. } => "object",
        HealType::Bucket { .. } => "bucket",
        HealType::ErasureSet { .. } => "erasure_set",
        HealType::Metadata { .. } => "metadata",
        HealType::MRF { .. } => "mrf",
        HealType::ECDecode { .. } => "ec_decode",
    }
}

fn heal_request_set_metric_label(request: &HealRequest) -> String {
    heal_request_set_key(request).unwrap_or_else(|| match (request.options.pool_index, request.options.set_index) {
        (Some(pool), Some(set)) => format!("pool_{pool}_set_{set}"),
        _ => "global".to_string(),
    })
}

fn record_scheduler_skip(set_label: &str) {
    counter!(
        "rustfs_heal_scheduler_skip_total",
        "reason" => "set_limit".to_string(),
        "set" => set_label.to_string()
    )
    .increment(1);
}

fn update_task_running_metric_for_task(active_heals: &HashMap<String, Arc<HealTask>>, task: &HealTask) {
    let type_label = task.metric_type_label();
    let set_label = task.metric_set_label();
    let count = active_heals
        .values()
        .filter(|active_task| active_task.metric_type_label() == type_label && active_task.metric_set_label() == set_label)
        .count();

    gauge!(
        "rustfs_heal_task_running",
        "type" => type_label.to_string(),
        "set" => set_label.clone()
    )
    .set(count as f64);
}

fn running_erasure_set_counts(active_heals: &HashMap<String, Arc<HealTask>>) -> HashMap<String, usize> {
    let mut running = HashMap::new();
    for task in active_heals.values() {
        if let HealType::ErasureSet { set_disk_id, .. } = &task.heal_type {
            *running.entry(set_disk_id.clone()).or_insert(0) += 1;
        }
    }
    running
}

fn can_schedule_request(request: &HealRequest, running_per_set: &HashMap<String, usize>, max_concurrent_per_set: usize) -> bool {
    match heal_request_set_key(request) {
        Some(set_key) => running_per_set.get(&set_key).copied().unwrap_or(0) < max_concurrent_per_set,
        None => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::heal::storage::HealStorageAPI;
    use crate::heal::task::{HealOptions, HealPriority, HealRequest, HealType};
    use rustfs_common::heal_channel::HealOpts;
    use rustfs_ecstore::{
        disk::{DiskStore, endpoint::Endpoint},
        store_api::BucketInfo,
    };
    use rustfs_madmin::heal_commands::HealResultItem;

    struct MockStorage;

    #[async_trait::async_trait]
    impl HealStorageAPI for MockStorage {
        async fn get_object_meta(&self, _bucket: &str, _object: &str) -> Result<Option<rustfs_ecstore::store_api::ObjectInfo>> {
            Ok(None)
        }

        async fn get_object_data(&self, _bucket: &str, _object: &str) -> Result<Option<Vec<u8>>> {
            Ok(None)
        }

        async fn put_object_data(&self, _bucket: &str, _object: &str, _data: &[u8]) -> Result<()> {
            Ok(())
        }

        async fn delete_object(&self, _bucket: &str, _object: &str) -> Result<()> {
            Ok(())
        }

        async fn verify_object_integrity(&self, _bucket: &str, _object: &str) -> Result<bool> {
            Ok(true)
        }

        async fn ec_decode_rebuild(&self, _bucket: &str, _object: &str) -> Result<Vec<u8>> {
            Ok(Vec::new())
        }

        async fn get_disk_status(&self, _endpoint: &Endpoint) -> Result<crate::heal::storage::DiskStatus> {
            Ok(crate::heal::storage::DiskStatus::Ok)
        }

        async fn format_disk(&self, _endpoint: &Endpoint) -> Result<()> {
            Ok(())
        }

        async fn get_bucket_info(&self, _bucket: &str) -> Result<Option<BucketInfo>> {
            Ok(None)
        }

        async fn heal_bucket_metadata(&self, _bucket: &str) -> Result<()> {
            Ok(())
        }

        async fn list_buckets(&self) -> Result<Vec<BucketInfo>> {
            Ok(Vec::new())
        }

        async fn object_exists(&self, _bucket: &str, _object: &str) -> Result<bool> {
            Ok(false)
        }

        async fn get_object_size(&self, _bucket: &str, _object: &str) -> Result<Option<u64>> {
            Ok(None)
        }

        async fn get_object_checksum(&self, _bucket: &str, _object: &str) -> Result<Option<String>> {
            Ok(None)
        }

        async fn heal_object(
            &self,
            _bucket: &str,
            _object: &str,
            _version_id: Option<&str>,
            _opts: &HealOpts,
        ) -> Result<(HealResultItem, Option<Error>)> {
            Ok((HealResultItem::default(), None))
        }

        async fn heal_bucket(&self, _bucket: &str, _opts: &HealOpts) -> Result<HealResultItem> {
            Ok(HealResultItem::default())
        }

        async fn heal_format(&self, _dry_run: bool) -> Result<(HealResultItem, Option<Error>)> {
            Ok((HealResultItem::default(), None))
        }

        async fn list_objects_for_heal(&self, _bucket: &str, _prefix: &str) -> Result<Vec<String>> {
            Ok(Vec::new())
        }

        async fn list_objects_for_heal_page(
            &self,
            _bucket: &str,
            _prefix: &str,
            _continuation_token: Option<&str>,
        ) -> Result<(Vec<String>, Option<String>, bool)> {
            Ok((Vec::new(), None, false))
        }

        async fn get_disk_for_resume(&self, _set_disk_id: &str) -> Result<DiskStore> {
            Err(Error::other("not implemented in tests"))
        }
    }

    #[test]
    fn test_priority_queue_ordering() {
        let mut queue = PriorityHealQueue::new();

        // Add requests with different priorities
        let low_req = HealRequest::new(
            HealType::Bucket {
                bucket: "bucket1".to_string(),
            },
            HealOptions::default(),
            HealPriority::Low,
        );

        let normal_req = HealRequest::new(
            HealType::Bucket {
                bucket: "bucket2".to_string(),
            },
            HealOptions::default(),
            HealPriority::Normal,
        );

        let high_req = HealRequest::new(
            HealType::Bucket {
                bucket: "bucket3".to_string(),
            },
            HealOptions::default(),
            HealPriority::High,
        );

        let urgent_req = HealRequest::new(
            HealType::Bucket {
                bucket: "bucket4".to_string(),
            },
            HealOptions::default(),
            HealPriority::Urgent,
        );

        // Add in random order: low, high, normal, urgent
        assert_eq!(queue.push(low_req), QueuePushOutcome::Accepted);
        assert_eq!(queue.push(high_req), QueuePushOutcome::Accepted);
        assert_eq!(queue.push(normal_req), QueuePushOutcome::Accepted);
        assert_eq!(queue.push(urgent_req), QueuePushOutcome::Accepted);

        assert_eq!(queue.len(), 4);

        // Should pop in priority order: urgent, high, normal, low
        let popped1 = queue.pop().unwrap();
        assert_eq!(popped1.priority, HealPriority::Urgent);

        let popped2 = queue.pop().unwrap();
        assert_eq!(popped2.priority, HealPriority::High);

        let popped3 = queue.pop().unwrap();
        assert_eq!(popped3.priority, HealPriority::Normal);

        let popped4 = queue.pop().unwrap();
        assert_eq!(popped4.priority, HealPriority::Low);

        assert_eq!(queue.len(), 0);
    }

    #[test]
    fn test_priority_queue_fifo_same_priority() {
        let mut queue = PriorityHealQueue::new();

        // Add multiple requests with same priority
        let req1 = HealRequest::new(
            HealType::Bucket {
                bucket: "bucket1".to_string(),
            },
            HealOptions::default(),
            HealPriority::Normal,
        );

        let req2 = HealRequest::new(
            HealType::Bucket {
                bucket: "bucket2".to_string(),
            },
            HealOptions::default(),
            HealPriority::Normal,
        );

        let req3 = HealRequest::new(
            HealType::Bucket {
                bucket: "bucket3".to_string(),
            },
            HealOptions::default(),
            HealPriority::Normal,
        );

        let id1 = req1.id.clone();
        let id2 = req2.id.clone();
        let id3 = req3.id.clone();

        assert_eq!(queue.push(req1), QueuePushOutcome::Accepted);
        assert_eq!(queue.push(req2), QueuePushOutcome::Accepted);
        assert_eq!(queue.push(req3), QueuePushOutcome::Accepted);

        // Should maintain FIFO order for same priority
        let popped1 = queue.pop().unwrap();
        assert_eq!(popped1.id, id1);

        let popped2 = queue.pop().unwrap();
        assert_eq!(popped2.id, id2);

        let popped3 = queue.pop().unwrap();
        assert_eq!(popped3.id, id3);
    }

    #[test]
    fn test_priority_queue_deduplication() {
        let mut queue = PriorityHealQueue::new();

        let req1 = HealRequest::new(
            HealType::Object {
                bucket: "bucket1".to_string(),
                object: "object1".to_string(),
                version_id: None,
            },
            HealOptions::default(),
            HealPriority::Normal,
        );

        let req2 = HealRequest::new(
            HealType::Object {
                bucket: "bucket1".to_string(),
                object: "object1".to_string(),
                version_id: None,
            },
            HealOptions::default(),
            HealPriority::High,
        );

        // First request should be added
        assert_eq!(queue.push(req1), QueuePushOutcome::Accepted);
        assert_eq!(queue.len(), 1);

        // Second request with same object should be rejected (duplicate)
        assert_eq!(queue.push(req2), QueuePushOutcome::Merged);
        assert_eq!(queue.len(), 1);
    }

    #[test]
    fn test_priority_queue_contains_erasure_set() {
        let mut queue = PriorityHealQueue::new();

        let req = HealRequest::new(
            HealType::ErasureSet {
                buckets: vec!["bucket1".to_string()],
                set_disk_id: "pool_0_set_1".to_string(),
            },
            HealOptions::default(),
            HealPriority::Normal,
        );

        assert_eq!(queue.push(req), QueuePushOutcome::Accepted);
        assert!(queue.contains_erasure_set("pool_0_set_1"));
        assert!(!queue.contains_erasure_set("pool_0_set_2"));
    }

    #[test]
    fn test_priority_queue_dedup_key_generation() {
        // Test different heal types generate different keys
        let obj_req = HealRequest::new(
            HealType::Object {
                bucket: "bucket1".to_string(),
                object: "object1".to_string(),
                version_id: None,
            },
            HealOptions::default(),
            HealPriority::Normal,
        );

        let bucket_req = HealRequest::new(
            HealType::Bucket {
                bucket: "bucket1".to_string(),
            },
            HealOptions::default(),
            HealPriority::Normal,
        );

        let erasure_req = HealRequest::new(
            HealType::ErasureSet {
                buckets: vec!["bucket1".to_string()],
                set_disk_id: "pool_0_set_1".to_string(),
            },
            HealOptions::default(),
            HealPriority::Normal,
        );

        let obj_key = PriorityHealQueue::make_dedup_key(&obj_req);
        let bucket_key = PriorityHealQueue::make_dedup_key(&bucket_req);
        let erasure_key = PriorityHealQueue::make_dedup_key(&erasure_req);

        // All keys should be different
        assert_ne!(obj_key, bucket_key);
        assert_ne!(obj_key, erasure_key);
        assert_ne!(bucket_key, erasure_key);

        assert!(obj_key.starts_with("object:"));
        assert!(bucket_key.starts_with("bucket:"));
        assert!(erasure_key.starts_with("erasure_set:"));
    }

    #[test]
    fn test_priority_queue_mixed_priorities_and_types() {
        let mut queue = PriorityHealQueue::new();

        // Add various requests
        let requests = vec![
            (
                HealType::Object {
                    bucket: "b1".to_string(),
                    object: "o1".to_string(),
                    version_id: None,
                },
                HealPriority::Low,
            ),
            (
                HealType::Bucket {
                    bucket: "b2".to_string(),
                },
                HealPriority::Urgent,
            ),
            (
                HealType::ErasureSet {
                    buckets: vec!["b3".to_string()],
                    set_disk_id: "pool_0_set_1".to_string(),
                },
                HealPriority::Normal,
            ),
            (
                HealType::Object {
                    bucket: "b4".to_string(),
                    object: "o4".to_string(),
                    version_id: None,
                },
                HealPriority::High,
            ),
        ];

        for (heal_type, priority) in requests {
            let req = HealRequest::new(heal_type, HealOptions::default(), priority);
            let outcome = queue.push(req);
            assert_eq!(outcome, QueuePushOutcome::Accepted);
        }

        assert_eq!(queue.len(), 4);

        // Check they come out in priority order
        let priorities: Vec<HealPriority> = (0..4).filter_map(|_| queue.pop().map(|r| r.priority)).collect();

        assert_eq!(
            priorities,
            vec![
                HealPriority::Urgent,
                HealPriority::High,
                HealPriority::Normal,
                HealPriority::Low,
            ]
        );
    }

    #[test]
    fn test_priority_queue_stats() {
        let mut queue = PriorityHealQueue::new();

        // Add requests with different priorities
        for _ in 0..3 {
            assert_eq!(
                queue.push(HealRequest::new(
                    HealType::Bucket {
                        bucket: format!("bucket-low-{}", queue.len()),
                    },
                    HealOptions::default(),
                    HealPriority::Low,
                )),
                QueuePushOutcome::Accepted
            );
        }

        for _ in 0..2 {
            assert_eq!(
                queue.push(HealRequest::new(
                    HealType::Bucket {
                        bucket: format!("bucket-normal-{}", queue.len()),
                    },
                    HealOptions::default(),
                    HealPriority::Normal,
                )),
                QueuePushOutcome::Accepted
            );
        }

        assert_eq!(
            queue.push(HealRequest::new(
                HealType::Bucket {
                    bucket: "bucket-high".to_string(),
                },
                HealOptions::default(),
                HealPriority::High,
            )),
            QueuePushOutcome::Accepted
        );

        let stats = queue.get_priority_stats();

        assert_eq!(*stats.get(&HealPriority::Low).unwrap_or(&0), 3);
        assert_eq!(*stats.get(&HealPriority::Normal).unwrap_or(&0), 2);
        assert_eq!(*stats.get(&HealPriority::High).unwrap_or(&0), 1);
        assert_eq!(*stats.get(&HealPriority::Urgent).unwrap_or(&0), 0);
    }

    #[test]
    fn test_priority_queue_is_empty() {
        let mut queue = PriorityHealQueue::new();

        assert!(queue.is_empty());

        assert_eq!(
            queue.push(HealRequest::new(
                HealType::Bucket {
                    bucket: "test".to_string(),
                },
                HealOptions::default(),
                HealPriority::Normal,
            )),
            QueuePushOutcome::Accepted
        );

        assert!(!queue.is_empty());

        queue.pop();

        assert!(queue.is_empty());
    }

    #[test]
    fn test_priority_queue_pop_runnable_skips_blocked_erasure_set() {
        let mut queue = PriorityHealQueue::new();

        let blocked = HealRequest::new(
            HealType::ErasureSet {
                buckets: vec!["bucket-a".to_string()],
                set_disk_id: "pool_0_set_1".to_string(),
            },
            HealOptions::default(),
            HealPriority::Urgent,
        );
        let runnable = HealRequest::new(
            HealType::ErasureSet {
                buckets: vec!["bucket-b".to_string()],
                set_disk_id: "pool_0_set_2".to_string(),
            },
            HealOptions::default(),
            HealPriority::Normal,
        );

        assert_eq!(queue.push(blocked), QueuePushOutcome::Accepted);
        assert_eq!(queue.push(runnable.clone()), QueuePushOutcome::Accepted);

        let mut running = HashMap::new();
        running.insert("pool_0_set_1".to_string(), 1);

        let popped = queue
            .pop_runnable(|request| can_schedule_request(request, &running, 1))
            .expect("should find runnable request");

        match popped.heal_type {
            HealType::ErasureSet { set_disk_id, .. } => assert_eq!(set_disk_id, "pool_0_set_2"),
            other => panic!("expected erasure set request, got {other:?}"),
        }
    }

    #[test]
    fn test_can_schedule_request_respects_per_set_limit() {
        let request = HealRequest::new(
            HealType::ErasureSet {
                buckets: vec!["bucket".to_string()],
                set_disk_id: "pool_0_set_1".to_string(),
            },
            HealOptions::default(),
            HealPriority::Normal,
        );

        let mut running = HashMap::new();
        running.insert("pool_0_set_1".to_string(), 1);

        assert!(!can_schedule_request(&request, &running, 1));
        assert!(can_schedule_request(&request, &running, 2));
    }

    #[tokio::test]
    async fn test_submit_heal_request_returns_merged_for_duplicate() {
        let storage: Arc<dyn HealStorageAPI> = Arc::new(MockStorage);
        let manager = HealManager::new(storage, None);

        let request = HealRequest::new(
            HealType::Object {
                bucket: "bucket".to_string(),
                object: "object".to_string(),
                version_id: None,
            },
            HealOptions::default(),
            HealPriority::Low,
        );

        assert_eq!(
            manager
                .submit_heal_request(request.clone())
                .await
                .expect("first request should be accepted"),
            HealAdmissionResult::Accepted
        );
        assert_eq!(
            manager
                .submit_heal_request(request)
                .await
                .expect("duplicate request should produce admission result"),
            HealAdmissionResult::Merged
        );
    }

    #[tokio::test]
    async fn test_submit_heal_request_returns_merged_before_full_for_duplicate() {
        let storage: Arc<dyn HealStorageAPI> = Arc::new(MockStorage);
        let manager = HealManager::new(
            storage,
            Some(HealConfig {
                queue_size: 1,
                ..HealConfig::default()
            }),
        );

        let request = HealRequest::new(
            HealType::Object {
                bucket: "bucket".to_string(),
                object: "object".to_string(),
                version_id: None,
            },
            HealOptions::default(),
            HealPriority::Low,
        );

        assert_eq!(
            manager
                .submit_heal_request(request.clone())
                .await
                .expect("first request should be accepted"),
            HealAdmissionResult::Accepted
        );
        assert_eq!(
            manager
                .submit_heal_request(request)
                .await
                .expect("duplicate request should merge even when queue is full"),
            HealAdmissionResult::Merged
        );
    }

    #[tokio::test]
    async fn test_submit_heal_request_returns_dropped_for_low_priority_when_full() {
        let storage: Arc<dyn HealStorageAPI> = Arc::new(MockStorage);
        let manager = HealManager::new(
            storage,
            Some(HealConfig {
                queue_size: 1,
                low_priority_drop_when_full: true,
                ..HealConfig::default()
            }),
        );

        let accepted = HealRequest::new(
            HealType::Bucket {
                bucket: "bucket-a".to_string(),
            },
            HealOptions::default(),
            HealPriority::Normal,
        );
        let dropped = HealRequest::new(
            HealType::Bucket {
                bucket: "bucket-b".to_string(),
            },
            HealOptions::default(),
            HealPriority::Low,
        );

        assert_eq!(
            manager
                .submit_heal_request(accepted)
                .await
                .expect("first request should be accepted"),
            HealAdmissionResult::Accepted
        );
        assert_eq!(
            manager
                .submit_heal_request(dropped)
                .await
                .expect("low priority request should be dropped with explicit admission result"),
            HealAdmissionResult::Dropped(HealAdmissionDropReason::QueueFull)
        );
    }

    #[test]
    fn test_running_erasure_set_counts_groups_only_erasure_tasks() {
        let storage: Arc<dyn HealStorageAPI> = Arc::new(MockStorage);
        let erasure_task = Arc::new(HealTask::from_request(
            HealRequest::new(
                HealType::ErasureSet {
                    buckets: vec!["bucket".to_string()],
                    set_disk_id: "pool_0_set_1".to_string(),
                },
                HealOptions::default(),
                HealPriority::Normal,
            ),
            storage.clone(),
        ));
        let object_task = Arc::new(HealTask::from_request(
            HealRequest::new(
                HealType::Object {
                    bucket: "bucket".to_string(),
                    object: "object".to_string(),
                    version_id: None,
                },
                HealOptions::default(),
                HealPriority::Normal,
            ),
            storage,
        ));

        let mut active = HashMap::new();
        active.insert(erasure_task.id.clone(), erasure_task);
        active.insert(object_task.id.clone(), object_task);

        let counts = running_erasure_set_counts(&active);
        assert_eq!(counts.get("pool_0_set_1"), Some(&1));
        assert_eq!(counts.len(), 1);
    }

    #[test]
    fn test_heal_config_respects_feature_flags() {
        temp_env::with_vars(
            [
                (rustfs_config::ENV_HEAL_EVENT_DRIVEN_SCHEDULER_ENABLE, Some("false")),
                (rustfs_config::ENV_HEAL_SET_BULKHEAD_ENABLE, Some("false")),
                (rustfs_config::ENV_HEAL_PAGE_PARALLEL_ENABLE, Some("false")),
            ],
            || {
                let config = HealConfig::default();
                assert!(!config.event_driven_scheduler_enable);
                assert!(!config.set_bulkhead_enable);
                assert!(!config.page_parallel_enable);
            },
        );
    }
}
