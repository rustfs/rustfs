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
    collections::{HashMap, VecDeque},
    sync::Arc,
    time::{Duration, SystemTime},
};
use tokio::{
    sync::{Mutex, RwLock},
    time::interval,
};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

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
        Self {
            enable_auto_heal: true,
            heal_interval: Duration::from_secs(10), // 10 seconds
            max_concurrent_heals: 4,
            task_timeout: Duration::from_secs(300), // 5 minutes
            queue_size: 1000,
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
    /// Heal queue
    heal_queue: Arc<Mutex<VecDeque<HealRequest>>>,
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
            heal_queue: Arc::new(Mutex::new(VecDeque::new())),
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

        // start auto disk scanner
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

        if queue.len() >= config.queue_size {
            return Err(Error::ConfigurationError {
                message: "Heal queue is full".to_string(),
            });
        }

        let request_id = request.id.clone();
        queue.push_back(request);
        drop(queue);

        info!("Submitted heal request: {}", request_id);
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

        tokio::spawn(async move {
            let mut interval = interval(config.read().await.heal_interval);

            loop {
                tokio::select! {
                    _ = cancel_token.cancelled() => {
                        info!("Auto disk scanner received shutdown signal");
                        break;
                    }
                    _ = interval.tick() => {
                        // Build list of endpoints that need healing
                        let mut endpoints = Vec::new();
                        for (_, disk_opt) in GLOBAL_LOCAL_DISK_MAP.read().await.iter() {
                            if let Some(disk) = disk_opt {
                                // detect unformatted disk via get_disk_id()
                                if let Err(err) = disk.get_disk_id().await {
                                    if err == DiskError::UnformattedDisk {
                                        endpoints.push(disk.endpoint());
                                        continue;
                                    }
                                }
                            }
                        }

                        if endpoints.is_empty() {
                            continue;
                        }

                        // Get bucket list for erasure set healing
                        let buckets = match storage.list_buckets().await {
                            Ok(buckets) => buckets.iter().map(|b| b.name.clone()).collect::<Vec<String>>(),
                            Err(e) => {
                                error!("Failed to get bucket list for auto healing: {}", e);
                                continue;
                            }
                        };

                        // Create erasure set heal requests for each endpoint
                        for ep in endpoints {
                            let Some(set_disk_id) =
                                crate::heal::utils::format_set_disk_id_from_i32(ep.pool_idx, ep.set_idx)
                            else {
                                warn!("Skipping endpoint {} without valid pool/set index", ep);
                                continue;
                            };
                            // skip if already queued or healing
                            // Use consistent lock order: queue first, then active_heals to avoid deadlock
                            let mut skip = false;
                            {
                                let queue = heal_queue.lock().await;
                                if queue.iter().any(|req| {
                                    matches!(
                                        &req.heal_type,
                                        crate::heal::task::HealType::ErasureSet { set_disk_id: queued_id, .. }
                                        if queued_id == &set_disk_id
                                    )
                                }) {
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
                            queue.push_back(req);
                            info!("Enqueued auto erasure set heal for endpoint: {} (set_disk_id: {})", ep, set_disk_id);
                        }
                    }
                }
            }
        });
        Ok(())
    }

    /// Process heal queue
    async fn process_heal_queue(
        heal_queue: &Arc<Mutex<VecDeque<HealRequest>>>,
        active_heals: &Arc<Mutex<HashMap<String, Arc<HealTask>>>>,
        config: &Arc<RwLock<HealConfig>>,
        statistics: &Arc<RwLock<HealStatistics>>,
        storage: &Arc<dyn HealStorageAPI>,
    ) {
        let config = config.read().await;
        let mut active_heals_guard = active_heals.lock().await;

        // check if new heal tasks can be started
        if active_heals_guard.len() >= config.max_concurrent_heals {
            return;
        }

        let mut queue = heal_queue.lock().await;
        if let Some(request) = queue.pop_front() {
            let task = Arc::new(HealTask::from_request(request, storage.clone()));
            let task_id = task.id.clone();
            active_heals_guard.insert(task_id.clone(), task.clone());
            drop(active_heals_guard);
            let active_heals_clone = active_heals.clone();
            let statistics_clone = statistics.clone();

            // start heal task
            tokio::spawn(async move {
                info!("Starting heal task: {}", task_id);
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

            // update statistics
            let mut stats = statistics.write().await;
            stats.total_tasks += 1;
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
