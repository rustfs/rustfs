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

use crate::error::{Error, Result};
use crate::heal::{
    progress::{HealProgress, HealStatistics},
    storage::HealStorageAPI,
    task::{HealRequest, HealTask, HealTaskStatus},
};
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
            heal_interval: Duration::from_secs(60), // 1 minute
            max_concurrent_heals: 4,
            task_timeout: Duration::from_secs(300), // 5 minutes
            queue_size: 1000,
        }
    }
}

/// Heal 状态
#[derive(Debug, Default)]
pub struct HealState {
    /// 是否正在运行
    pub is_running: bool,
    /// 当前 heal 周期
    pub current_cycle: u64,
    /// 最后 heal 时间
    pub last_heal_time: Option<SystemTime>,
    /// 总 heal 对象数
    pub total_healed_objects: u64,
    /// 总 heal 失败数
    pub total_heal_failures: u64,
    /// 当前活跃 heal 任务数
    pub active_heal_count: usize,
}

/// Heal 管理器
pub struct HealManager {
    /// Heal 配置
    config: Arc<RwLock<HealConfig>>,
    /// Heal 状态
    state: Arc<RwLock<HealState>>,
    /// 活跃的 heal 任务
    active_heals: Arc<Mutex<HashMap<String, Arc<HealTask>>>>,
    /// Heal 队列
    heal_queue: Arc<Mutex<VecDeque<HealRequest>>>,
    /// 存储层接口
    storage: Arc<dyn HealStorageAPI>,
    /// 取消令牌
    cancel_token: CancellationToken,
    /// 统计信息
    statistics: Arc<RwLock<HealStatistics>>,
}

impl HealManager {
    /// 创建新的 HealManager
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

    /// 启动 HealManager
    pub async fn start(&self) -> Result<()> {
        let mut state = self.state.write().await;
        if state.is_running {
            warn!("HealManager is already running");
            return Ok(());
        }
        state.is_running = true;
        drop(state);

        info!("Starting HealManager");

        // 启动调度器
        self.start_scheduler().await?;

        // 启动工作器
        self.start_workers().await?;

        info!("HealManager started successfully");
        Ok(())
    }

    /// 停止 HealManager
    pub async fn stop(&self) -> Result<()> {
        info!("Stopping HealManager");

        // 取消所有任务
        self.cancel_token.cancel();

        // 等待所有任务完成
        let mut active_heals = self.active_heals.lock().await;
        for task in active_heals.values() {
            if let Err(e) = task.cancel().await {
                warn!("Failed to cancel task {}: {}", task.id, e);
            }
        }
        active_heals.clear();

        // 更新状态
        let mut state = self.state.write().await;
        state.is_running = false;

        info!("HealManager stopped successfully");
        Ok(())
    }

    /// 提交 heal 请求
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

    /// 获取任务状态
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

    /// 获取任务进度
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

    /// 取消任务
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

    /// 获取统计信息
    pub async fn get_statistics(&self) -> HealStatistics {
        self.statistics.read().await.clone()
    }

    /// 获取活跃任务数量
    pub async fn get_active_task_count(&self) -> usize {
        let active_heals = self.active_heals.lock().await;
        active_heals.len()
    }

    /// 获取队列长度
    pub async fn get_queue_length(&self) -> usize {
        let queue = self.heal_queue.lock().await;
        queue.len()
    }

    /// 启动调度器
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

    /// 启动工作器
    async fn start_workers(&self) -> Result<()> {
        let config = self.config.clone();
        let active_heals = self.active_heals.clone();
        let storage = self.storage.clone();
        let cancel_token = self.cancel_token.clone();
        let statistics = self.statistics.clone();

        let worker_count = config.read().await.max_concurrent_heals;

        for worker_id in 0..worker_count {
            let active_heals = active_heals.clone();
            let _storage = storage.clone();
            let cancel_token = cancel_token.clone();
            let statistics = statistics.clone();

            tokio::spawn(async move {
                info!("Starting heal worker {}", worker_id);

                loop {
                    tokio::select! {
                        _ = cancel_token.cancelled() => {
                            info!("Heal worker {} received shutdown signal", worker_id);
                            break;
                        }
                        _ = async {
                            // 等待任务
                            tokio::time::sleep(Duration::from_millis(100)).await;
                        } => {
                            // 检查是否有可执行的任务
                            let mut active_heals_guard = active_heals.lock().await;
                            let mut completed_tasks = Vec::new();
                            
                            for (id, task) in active_heals_guard.iter() {
                                let status = task.get_status().await;
                                if matches!(status, HealTaskStatus::Completed | HealTaskStatus::Failed { .. } | HealTaskStatus::Cancelled) {
                                    completed_tasks.push(id.clone());
                                }
                            }

                            // 移除已完成的任务
                            for task_id in completed_tasks {
                                if let Some(task) = active_heals_guard.remove(&task_id) {
                                    // 更新统计信息
                                    let mut stats = statistics.write().await;
                                    match task.get_status().await {
                                        HealTaskStatus::Completed => {
                                            stats.update_task_completion(true);
                                        }
                                        HealTaskStatus::Failed { .. } => {
                                            stats.update_task_completion(false);
                                        }
                                        _ => {}
                                    }
                                }
                            }

                            // 更新活跃任务数量
                            let mut stats = statistics.write().await;
                            stats.update_running_tasks(active_heals_guard.len() as u64);
                        }
                    }
                }

                info!("Heal worker {} stopped", worker_id);
            });
        }

        Ok(())
    }

    /// 处理 heal 队列
    async fn process_heal_queue(
        heal_queue: &Arc<Mutex<VecDeque<HealRequest>>>,
        active_heals: &Arc<Mutex<HashMap<String, Arc<HealTask>>>>,
        config: &Arc<RwLock<HealConfig>>,
        statistics: &Arc<RwLock<HealStatistics>>,
        storage: &Arc<dyn HealStorageAPI>,
    ) {
        let config = config.read().await;
        let mut active_heals = active_heals.lock().await;

        // 检查是否可以启动新的 heal 任务
        if active_heals.len() >= config.max_concurrent_heals {
            return;
        }

        let mut queue = heal_queue.lock().await;
        if let Some(request) = queue.pop_front() {
            let task = Arc::new(HealTask::from_request(request, storage.clone()));
            let task_id = task.id.clone();
            active_heals.insert(task_id.clone(), task.clone());

            // 启动 heal 任务
            tokio::spawn(async move {
                info!("Starting heal task: {}", task_id);
                match task.execute().await {
                    Ok(_) => {
                        info!("Heal task completed successfully: {}", task_id);
                    }
                    Err(e) => {
                        error!("Heal task failed: {} - {}", task_id, e);
                    }
                }
            });

            // 更新统计信息
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