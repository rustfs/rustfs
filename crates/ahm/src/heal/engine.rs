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

use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant, SystemTime},
};

use tokio::{
    sync::{mpsc, RwLock},
    time::sleep,
};
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::error::Result;
use super::{HealConfig, HealPriority, HealResult, HealStatistics, HealTask, Status};

/// Main healing engine that coordinates repair operations
pub struct HealEngine {
    config: HealConfig,
    status: Arc<RwLock<Status>>,
    statistics: Arc<RwLock<HealStatistics>>,
    task_queue: Arc<RwLock<Vec<HealTask>>>,
    active_tasks: Arc<RwLock<HashMap<String, HealTask>>>,
    completed_tasks: Arc<RwLock<Vec<HealResult>>>,
    shutdown_tx: Option<mpsc::Sender<()>>,
}

impl HealEngine {
    /// Create a new healing engine
    pub fn new(config: HealConfig) -> Self {
        Self {
            config,
            status: Arc::new(RwLock::new(Status::Initializing)),
            statistics: Arc::new(RwLock::new(HealStatistics::default())),
            task_queue: Arc::new(RwLock::new(Vec::new())),
            active_tasks: Arc::new(RwLock::new(HashMap::new())),
            completed_tasks: Arc::new(RwLock::new(Vec::new())),
            shutdown_tx: None,
        }
    }

    /// Start the healing engine
    pub async fn start(&mut self) -> Result<()> {
        info!("Starting heal engine");
        
        let (shutdown_tx, mut shutdown_rx) = mpsc::channel(1);
        self.shutdown_tx = Some(shutdown_tx);

        // Update status
        {
            let mut status = self.status.write().await;
            *status = Status::Idle;
        }

        let config = self.config.clone();
        let status = Arc::clone(&self.status);
        let statistics = Arc::clone(&self.statistics);
        let task_queue = Arc::clone(&self.task_queue);
        let active_tasks = Arc::clone(&self.active_tasks);
        let completed_tasks = Arc::clone(&self.completed_tasks);

        // Start the main healing loop
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(config.heal_interval);
            
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        if let Err(e) = Self::process_healing_cycle(
                            &config,
                            &status,
                            &statistics,
                            &task_queue,
                            &active_tasks,
                            &completed_tasks,
                        ).await {
                            error!("Healing cycle failed: {}", e);
                        }
                    }
                    _ = shutdown_rx.recv() => {
                        info!("Shutdown signal received, stopping heal engine");
                        break;
                    }
                }
            }

            // Update status to stopped
            let mut status = status.write().await;
            *status = Status::Stopped;
        });

        info!("Heal engine started successfully");
        Ok(())
    }

    /// Stop the healing engine
    pub async fn stop(&mut self) -> Result<()> {
        info!("Stopping heal engine");
        
        // Update status
        {
            let mut status = self.status.write().await;
            *status = Status::Stopping;
        }

        // Send shutdown signal
        if let Some(shutdown_tx) = &self.shutdown_tx {
            let _ = shutdown_tx.send(()).await;
        }

        // Wait for engine to stop
        let mut attempts = 0;
        while attempts < 10 {
            let status = self.status.read().await;
            if *status == Status::Stopped {
                break;
            }
            drop(status);
            sleep(Duration::from_millis(100)).await;
            attempts += 1;
        }

        info!("Heal engine stopped");
        Ok(())
    }

    /// Add a healing task to the queue
    pub async fn add_task(&self, task: HealTask) -> Result<()> {
        let task_id = task.id.clone();
        let queue = Arc::clone(&self.task_queue);
        
        // Add task to priority queue
        queue.write().await.push(task);
        
        info!("Added healing task to queue: {}", task_id);
        Ok(())
    }

    /// Get current engine status
    pub async fn status(&self) -> Status {
        self.status.read().await.clone()
    }

    /// Get current engine status (alias for status)
    pub async fn get_status(&self) -> Status {
        self.status.read().await.clone()
    }

    /// Get engine configuration
    pub async fn get_config(&self) -> HealConfig {
        self.config.clone()
    }

    /// Get healing statistics
    pub async fn statistics(&self) -> HealStatistics {
        self.statistics.read().await.clone()
    }

    /// Get completed healing results
    pub async fn completed_results(&self) -> Vec<HealResult> {
        self.completed_tasks.read().await.clone()
    }

    /// Process a single healing cycle
    async fn process_healing_cycle(
        config: &HealConfig,
        status: &Arc<RwLock<Status>>,
        statistics: &Arc<RwLock<HealStatistics>>,
        task_queue: &Arc<RwLock<Vec<HealTask>>>,
        active_tasks: &Arc<RwLock<HashMap<String, HealTask>>>,
        completed_tasks: &Arc<RwLock<Vec<HealResult>>>,
    ) -> Result<()> {
        // Update status to healing
        {
            let mut status = status.write().await;
            *status = Status::Healing;
        }

        // Get ready tasks from queue
        let mut queue = task_queue.write().await;
        let mut ready_tasks = Vec::new();
        let mut remaining_tasks = Vec::new();

        for task in queue.drain(..) {
            if task.is_ready() {
                ready_tasks.push(task);
            } else {
                remaining_tasks.push(task);
            }
        }

        // Sort ready tasks by priority
        ready_tasks.sort_by(|a, b| a.priority.cmp(&b.priority));

        // Process ready tasks
        let active_count = active_tasks.read().await.len();
        let max_concurrent = config.max_workers.saturating_sub(active_count);
        
        for task in ready_tasks.into_iter().take(max_concurrent) {
            if let Err(e) = Self::process_task(
                config,
                statistics,
                active_tasks,
                completed_tasks,
                task,
            ).await {
                error!("Failed to process healing task: {}", e);
            }
        }

        // Put remaining tasks back in queue
        queue.extend(remaining_tasks);

        // Update statistics
        {
            let mut stats = statistics.write().await;
            stats.queued_tasks = queue.len() as u64;
            stats.active_workers = active_tasks.read().await.len() as u64;
        }

        // Update status back to idle
        {
            let mut status = status.write().await;
            *status = Status::Idle;
        }

        Ok(())
    }

    /// Process a single healing task
    async fn process_task(
        config: &HealConfig,
        statistics: &Arc<RwLock<HealStatistics>>,
        active_tasks: &Arc<RwLock<HashMap<String, HealTask>>>,
        completed_tasks: &Arc<RwLock<Vec<HealResult>>>,
        task: HealTask,
    ) -> Result<()> {
        let task_id = task.id.clone();
        
        // Add task to active tasks
        {
            let mut active = active_tasks.write().await;
            active.insert(task_id.clone(), task.clone());
        }

        // Update statistics
        {
            let mut stats = statistics.write().await;
            stats.total_repairs += 1;
            stats.active_workers = active_tasks.read().await.len() as u64;
        }

        info!("Processing healing task: {}", task_id);

        // Simulate healing operation
        let start_time = Instant::now();
        let result = Self::perform_healing_operation(&task, config).await;
        let duration = start_time.elapsed();

        // Create heal result
        let heal_result = HealResult {
            success: result.is_ok(),
            original_issue: task.issue.clone(),
            repair_duration: duration,
            retry_attempts: task.retry_count,
            error_message: result.err().map(|e| e.to_string()),
            metadata: None,
            completed_at: SystemTime::now(),
        };

        // Update statistics
        {
            let mut stats = statistics.write().await;
            if heal_result.success {
                stats.successful_repairs += 1;
            } else {
                stats.failed_repairs += 1;
            }
            stats.total_repair_time += duration;
            stats.average_repair_time = if stats.total_repairs > 0 {
                Duration::from_secs_f64(
                    stats.total_repair_time.as_secs_f64() / stats.total_repairs as f64
                )
            } else {
                Duration::ZERO
            };
            stats.last_repair_time = Some(SystemTime::now());
            stats.total_retry_attempts += task.retry_count as u64;
        }

        // Add result to completed tasks
        {
            let mut completed = completed_tasks.write().await;
            completed.push(heal_result.clone());
        }

        // Remove task from active tasks
        {
            let mut active = active_tasks.write().await;
            active.remove(&task_id);
        }

        // Update statistics
        {
            let mut stats = statistics.write().await;
            stats.active_workers = active_tasks.read().await.len() as u64;
        }

        if heal_result.success {
            info!("Healing task completed successfully: {}", task_id);
        } else {
            warn!("Healing task failed: {}", task_id);
        }

        Ok(())
    }

    /// Perform the actual healing operation
    async fn perform_healing_operation(task: &HealTask, _config: &HealConfig) -> Result<()> {
        // Simulate healing operation based on issue type
        match task.issue.issue_type {
            crate::scanner::HealthIssueType::MissingReplica => {
                // Simulate replica repair
                sleep(Duration::from_millis(100)).await;
                info!("Repaired missing replica for {}/{}", task.issue.bucket, task.issue.object);
            }
            crate::scanner::HealthIssueType::ChecksumMismatch => {
                // Simulate checksum repair
                sleep(Duration::from_millis(200)).await;
                info!("Repaired checksum mismatch for {}/{}", task.issue.bucket, task.issue.object);
            }
            crate::scanner::HealthIssueType::DiskReadError => {
                // Simulate disk error recovery
                sleep(Duration::from_millis(300)).await;
                info!("Recovered from disk read error for {}/{}", task.issue.bucket, task.issue.object);
            }
            _ => {
                // Generic repair for other issue types
                sleep(Duration::from_millis(150)).await;
                info!("Performed generic repair for {}/{}", task.issue.bucket, task.issue.object);
            }
        }

        // Simulate occasional failures for testing
        if task.retry_count > 0 && task.retry_count % 3 == 0 {
            return Err(crate::error::Error::Other(anyhow::anyhow!("Simulated healing failure")));
        }

        Ok(())
    }

    /// Start healing operations
    pub async fn start_healing(&self) -> Result<()> {
        let mut status = self.status.write().await;
        *status = Status::Running;
        info!("Healing operations started");
        Ok(())
    }

    /// Stop healing operations
    pub async fn stop_healing(&self) -> Result<()> {
        let mut status = self.status.write().await;
        *status = Status::Stopped;
        info!("Healing operations stopped");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scanner::{HealthIssue, HealthIssueType, Severity};

    #[tokio::test]
    async fn test_heal_engine_creation() {
        let config = HealConfig::default();
        let engine = HealEngine::new(config);
        
        assert_eq!(engine.status().await, Status::Initializing);
    }

    #[tokio::test]
    async fn test_heal_engine_start_stop() {
        let config = HealConfig::default();
        let mut engine = HealEngine::new(config);
        
        // Start engine
        engine.start().await.unwrap();
        sleep(Duration::from_millis(100)).await;
        
        // Check status
        let status = engine.status().await;
        assert!(matches!(status, Status::Idle | Status::Healing));
        
        // Stop engine
        engine.stop().await.unwrap();
        sleep(Duration::from_millis(100)).await;
        
        // Check status
        let status = engine.status().await;
        assert_eq!(status, Status::Stopped);
    }

    #[tokio::test]
    async fn test_add_healing_task() {
        let config = HealConfig::default();
        let engine = HealEngine::new(config);
        
        let issue = HealthIssue {
            issue_type: HealthIssueType::MissingReplica,
            severity: Severity::Critical,
            bucket: "test-bucket".to_string(),
            object: "test-object".to_string(),
            description: "Test issue".to_string(),
            metadata: None,
        };
        
        let task = HealTask::new(issue);
        engine.add_task(task).await.unwrap();
        
        let stats = engine.statistics().await;
        assert_eq!(stats.queued_tasks, 1);
    }
} 