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
use rustfs_ecstore::disk::DiskAPI;
use rustfs_ecstore::disk::error::DiskError;
use rustfs_ecstore::global::GLOBAL_LOCAL_DISK_MAP;
use std::{
    collections::{BinaryHeap, HashMap, HashSet},
    sync::Arc,
    time::{Duration, SystemTime},
};
use tokio::{
    sync::{Mutex, RwLock},
    time::interval,
};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

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

    fn is_empty(&self) -> bool {
        self.heap.is_empty()
    }

    fn push(&mut self, request: HealRequest) -> bool {
        let key = Self::make_dedup_key(&request);

        // Check for duplicates
        if self.dedup_keys.contains(&key) {
            return false; // Duplicate request, don't add
        }

        self.dedup_keys.insert(key);
        self.sequence += 1;
        self.heap.push(PriorityQueueItem {
            priority: request.priority,
            sequence: self.sequence,
            request,
        });
        true
    }

    /// Get statistics about queue contents by priority
    fn get_priority_stats(&self) -> HashMap<HealPriority, usize> {
        let mut stats = HashMap::new();
        for item in &self.heap {
            *stats.entry(item.priority).or_insert(0) += 1;
        }
        stats
    }

    fn pop(&mut self) -> Option<HealRequest> {
        self.heap.pop().map(|item| {
            let key = Self::make_dedup_key(&item.request);
            self.dedup_keys.remove(&key);
            item.request
        })
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

/// Heal config
#[derive(Debug, Clone)]
pub struct HealConfig {
    /// Whether to enable auto heal
    pub enable_auto_heal: bool,
    /// Heal interval
    pub heal_interval: Duration,
    /// Maximum concurrent heal tasks
    pub max_concurrent_heals: usize,
    /// Task timeout
    pub task_timeout: Duration,
    /// Queue size
    pub queue_size: usize,
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
        Self {
            enable_auto_heal,
            heal_interval,        // 10 seconds
            max_concurrent_heals, // max 4,
            task_timeout,         // 5 minutes
            queue_size,
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
}

impl HealManager {
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

        // update state
        let mut state = self.state.write().await;
        state.is_running = false;

        info!("HealManager stopped successfully");
        Ok(())
    }

    /// Submit heal request
    pub async fn submit_heal_request(&self, request: HealRequest) -> Result<String> {
        let config = self.config.read().await;
        let mut queue = self.heal_queue.lock().await;

        let queue_len = queue.len();
        let queue_capacity = config.queue_size;

        if queue_len >= queue_capacity {
            return Err(Error::ConfigurationError {
                message: format!("Heal queue is full ({queue_len}/{queue_capacity})"),
            });
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

        // Try to push the request; if it's a duplicate, still return the request_id
        let is_new = queue.push(request);

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

        if is_new {
            info!("Submitted heal request: {} with priority: {:?}", request_id, priority);
        } else {
            info!("Heal request already queued (duplicate): {}", request_id);
        }

        Ok(request_id)
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
        self.active_heals.lock().await.len()
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
        active_heals.len()
    }

    /// Get queue length
    pub async fn get_queue_length(&self) -> usize {
        let queue = self.heal_queue.lock().await;
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

        tokio::spawn(async move {
            let mut interval = interval(config.read().await.heal_interval);

            loop {
                tokio::select! {
                    _ = cancel_token.cancelled() => {
                        info!("Heal scheduler received shutdown signal");
                        break;
                    }
                    _ = interval.tick() => {
                        Self::process_heal_queue(&heal_queue, &active_heals, &config, &statistics, &storage).await;
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
                            info!("start_auto_disk_scanner: No endpoints need healing");
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
                            queue.push(req);
                            info!("start_auto_disk_scanner: Enqueued auto erasure set heal for endpoint: {} (set_disk_id: {})", ep, set_disk_id);
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
    ) {
        let config = config.read().await;
        let mut active_heals_guard = active_heals.lock().await;

        // Check if new heal tasks can be started
        let active_count = active_heals_guard.len();
        if active_count >= config.max_concurrent_heals {
            return;
        }

        // Calculate how many tasks we can start this cycle
        let available_slots = config.max_concurrent_heals - active_count;

        let mut queue = heal_queue.lock().await;
        let queue_len = queue.len();

        if queue_len == 0 {
            return;
        }

        // Process multiple tasks if:
        // 1. We have available slots
        // 2. Queue is not empty
        // Prioritize urgent/high priority tasks by processing up to 2 tasks per cycle if available
        let tasks_to_process = if queue_len > 0 {
            std::cmp::min(available_slots, std::cmp::min(2, queue_len))
        } else {
            0
        };

        for _ in 0..tasks_to_process {
            if let Some(request) = queue.pop() {
                let task_priority = request.priority;
                let task = Arc::new(HealTask::from_request(request, storage.clone()));
                let task_id = task.id.clone();
                active_heals_guard.insert(task_id.clone(), task.clone());
                let active_heals_clone = active_heals.clone();
                let statistics_clone = statistics.clone();

                // start heal task
                tokio::spawn(async move {
                    info!("Starting heal task: {} with priority: {:?}", task_id, task_priority);
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
                });
            } else {
                break;
            }
        }

        // Update statistics for all started tasks
        let mut stats = statistics.write().await;
        stats.total_tasks += tasks_to_process as u64;

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::heal::task::{HealOptions, HealPriority, HealRequest, HealType};

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
        assert!(queue.push(low_req));
        assert!(queue.push(high_req));
        assert!(queue.push(normal_req));
        assert!(queue.push(urgent_req));

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

        assert!(queue.push(req1));
        assert!(queue.push(req2));
        assert!(queue.push(req3));

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
        assert!(queue.push(req1));
        assert_eq!(queue.len(), 1);

        // Second request with same object should be rejected (duplicate)
        assert!(!queue.push(req2));
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

        assert!(queue.push(req));
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
            queue.push(req);
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
            queue.push(HealRequest::new(
                HealType::Bucket {
                    bucket: format!("bucket-low-{}", queue.len()),
                },
                HealOptions::default(),
                HealPriority::Low,
            ));
        }

        for _ in 0..2 {
            queue.push(HealRequest::new(
                HealType::Bucket {
                    bucket: format!("bucket-normal-{}", queue.len()),
                },
                HealOptions::default(),
                HealPriority::Normal,
            ));
        }

        queue.push(HealRequest::new(
            HealType::Bucket {
                bucket: "bucket-high".to_string(),
            },
            HealOptions::default(),
            HealPriority::High,
        ));

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

        queue.push(HealRequest::new(
            HealType::Bucket {
                bucket: "test".to_string(),
            },
            HealOptions::default(),
            HealPriority::Normal,
        ));

        assert!(!queue.is_empty());

        queue.pop();

        assert!(queue.is_empty());
    }
}
