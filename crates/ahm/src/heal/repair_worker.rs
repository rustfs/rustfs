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
    sync::Arc,
    time::{Duration, Instant, SystemTime},
};

use tokio::{
    sync::{mpsc, RwLock},
    time::{sleep, timeout},
};
use tracing::{debug, error, info, warn};

use crate::error::Result;
use super::{HealConfig, HealResult, HealTask, Status};

/// Configuration for repair workers
#[derive(Debug, Clone)]
pub struct RepairWorkerConfig {
    /// Worker ID
    pub worker_id: String,
    /// Maximum time to spend on a single repair operation
    pub operation_timeout: Duration,
    /// Whether to enable detailed logging
    pub enable_detailed_logging: bool,
    /// Maximum number of concurrent operations
    pub max_concurrent_operations: usize,
    /// Retry configuration
    pub retry_config: RetryConfig,
}

/// Retry configuration for repair operations
#[derive(Debug, Clone)]
pub struct RetryConfig {
    /// Maximum number of retry attempts
    pub max_attempts: u32,
    /// Initial backoff delay
    pub initial_backoff: Duration,
    /// Maximum backoff delay
    pub max_backoff: Duration,
    /// Backoff multiplier
    pub backoff_multiplier: f64,
    /// Whether to use exponential backoff
    pub exponential_backoff: bool,
}

impl Default for RepairWorkerConfig {
    fn default() -> Self {
        Self {
            worker_id: "worker-1".to_string(),
            operation_timeout: Duration::from_secs(300), // 5 minutes
            enable_detailed_logging: true,
            max_concurrent_operations: 1,
            retry_config: RetryConfig::default(),
        }
    }
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            initial_backoff: Duration::from_secs(1),
            max_backoff: Duration::from_secs(60),
            backoff_multiplier: 2.0,
            exponential_backoff: true,
        }
    }
}

/// Statistics for a repair worker
#[derive(Debug, Clone, Default)]
pub struct WorkerStatistics {
    /// Total number of tasks processed
    pub total_tasks_processed: u64,
    /// Number of successful repairs
    pub successful_repairs: u64,
    /// Number of failed repairs
    pub failed_repairs: u64,
    /// Total time spent on repairs
    pub total_repair_time: Duration,
    /// Average repair time
    pub average_repair_time: Duration,
    /// Number of retry attempts made
    pub total_retry_attempts: u64,
    /// Current worker status
    pub status: WorkerStatus,
    /// Last task completion time
    pub last_task_time: Option<SystemTime>,
}

/// Worker status
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WorkerStatus {
    /// Worker is idle
    Idle,
    /// Worker is processing a task
    Processing,
    /// Worker is retrying a failed task
    Retrying,
    /// Worker is stopping
    Stopping,
    /// Worker has stopped
    Stopped,
    /// Worker encountered an error
    Error(String),
}

impl Default for WorkerStatus {
    fn default() -> Self {
        WorkerStatus::Idle
    }
}

/// Repair worker that executes healing tasks
pub struct RepairWorker {
    config: RepairWorkerConfig,
    statistics: Arc<RwLock<WorkerStatistics>>,
    status: Arc<RwLock<WorkerStatus>>,
    result_tx: mpsc::Sender<HealResult>,
    shutdown_tx: Option<mpsc::Sender<()>>,
}

impl RepairWorker {
    /// Create a new repair worker
    pub fn new(
        config: RepairWorkerConfig,
        result_tx: mpsc::Sender<HealResult>,
    ) -> Self {
        Self {
            config,
            statistics: Arc::new(RwLock::new(WorkerStatistics::default())),
            status: Arc::new(RwLock::new(WorkerStatus::Idle)),
            result_tx,
            shutdown_tx: None,
        }
    }

    /// Start the repair worker
    pub async fn start(&mut self) -> Result<()> {
        info!("Starting repair worker: {}", self.config.worker_id);

        let (_task_tx, task_rx) = mpsc::channel(100);
        let (shutdown_tx, mut shutdown_rx) = mpsc::channel(1);

        self.shutdown_tx = Some(shutdown_tx);

        // Update status
        {
            let mut status = self.status.write().await;
            *status = WorkerStatus::Idle;
        }

        let config = self.config.clone();
        let statistics = Arc::clone(&self.statistics);
        let status = Arc::clone(&self.status);
        let result_tx = self.result_tx.clone();

        // Start the worker loop
        tokio::spawn(async move {
            let mut task_rx = task_rx;
            
            loop {
                tokio::select! {
                    Some(task) = task_rx.recv() => {
                        if let Err(e) = Self::process_task(
                            &config,
                            &statistics,
                            &status,
                            &result_tx,
                            task,
                        ).await {
                            error!("Failed to process task: {}", e);
                        }
                    }
                    _ = shutdown_rx.recv() => {
                        info!("Shutdown signal received, stopping worker: {}", config.worker_id);
                        break;
                    }
                }
            }

            // Update status to stopped
            let mut status = status.write().await;
            *status = WorkerStatus::Stopped;
        });

        info!("Repair worker started: {}", self.config.worker_id);
        Ok(())
    }

    /// Stop the repair worker
    pub async fn stop(&mut self) -> Result<()> {
        info!("Stopping repair worker: {}", self.config.worker_id);

        // Update status
        {
            let mut status = self.status.write().await;
            *status = WorkerStatus::Stopping;
        }

        // Send shutdown signal
        if let Some(shutdown_tx) = &self.shutdown_tx {
            let _ = shutdown_tx.send(()).await;
        }

        // Wait for worker to stop
        let mut attempts = 0;
        while attempts < 10 {
            let status = self.status.read().await;
            if *status == WorkerStatus::Stopped {
                break;
            }
            drop(status);
            sleep(Duration::from_millis(100)).await;
            attempts += 1;
        }

        info!("Repair worker stopped: {}", self.config.worker_id);
        Ok(())
    }

    /// Submit a task to the worker
    pub async fn submit_task(&self, _task: HealTask) -> Result<()> {
        // TODO: Implement task submission
        Err(crate::error::Error::Other(anyhow::anyhow!("Task submission not implemented")))
    }

    /// Get worker statistics
    pub async fn statistics(&self) -> WorkerStatistics {
        self.statistics.read().await.clone()
    }

    /// Get worker status
    pub async fn status(&self) -> WorkerStatus {
        self.status.read().await.clone()
    }

    /// Process a single task
    async fn process_task(
        config: &RepairWorkerConfig,
        statistics: &Arc<RwLock<WorkerStatistics>>,
        status: &Arc<RwLock<WorkerStatus>>,
        result_tx: &mpsc::Sender<HealResult>,
        task: HealTask,
    ) -> Result<()> {
        let task_id = task.id.clone();
        
        // Update status to processing
        {
            let mut status = status.write().await;
            *status = WorkerStatus::Processing;
        }

        // Update statistics
        {
            let mut stats = statistics.write().await;
            stats.total_tasks_processed += 1;
            stats.status = WorkerStatus::Processing;
        }

        info!("Processing repair task: {} (worker: {})", task_id, config.worker_id);

        let start_time = Instant::now();
        let mut attempt = 0;
        let mut last_error = None;

        // Retry loop
        while attempt < config.retry_config.max_attempts {
            attempt += 1;

            if attempt > 1 {
                // Update status to retrying
                {
                    let mut status = status.write().await;
                    *status = WorkerStatus::Retrying;
                }

                // Calculate backoff delay
                let backoff_delay = if config.retry_config.exponential_backoff {
                    let delay = config.retry_config.initial_backoff * 
                        (config.retry_config.backoff_multiplier.powi((attempt - 1) as i32)) as u32;
                    delay.min(config.retry_config.max_backoff)
                } else {
                    config.retry_config.initial_backoff
                };

                warn!("Retrying task {} (attempt {}/{}), waiting {:?}", 
                    task_id, attempt, config.retry_config.max_attempts, backoff_delay);
                sleep(backoff_delay).await;
            }

            // Attempt the repair operation
            let result = timeout(
                config.operation_timeout,
                Self::perform_repair_operation(&task, config)
            ).await;

            match result {
                Ok(Ok(())) => {
                    // Success
                    let duration = start_time.elapsed();
                    let heal_result = HealResult {
                        success: true,
                        original_issue: task.issue.clone(),
                        repair_duration: duration,
                        retry_attempts: attempt - 1,
                        error_message: None,
                        metadata: None,
                        completed_at: SystemTime::now(),
                    };

                    // Send result
                    if let Err(e) = result_tx.send(heal_result).await {
                        error!("Failed to send heal result: {}", e);
                    }

                    // Update statistics
                    {
                        let mut stats = statistics.write().await;
                        stats.successful_repairs += 1;
                        stats.total_repair_time += duration;
                        stats.average_repair_time = if stats.total_tasks_processed > 0 {
                            Duration::from_secs_f64(
                                stats.total_repair_time.as_secs_f64() / stats.total_tasks_processed as f64
                            )
                        } else {
                            Duration::ZERO
                        };
                        stats.total_retry_attempts += (attempt - 1) as u64;
                        stats.last_task_time = Some(SystemTime::now());
                        stats.status = WorkerStatus::Idle;
                    }

                    info!("Successfully completed repair task: {} (worker: {})", task_id, config.worker_id);
                    return Ok(());
                }
                Ok(Err(e)) => {
                    // Operation failed
                    let error_msg = e.to_string();
                    last_error = Some(e);
                    warn!("Repair operation failed for task {} (attempt {}/{}): {}", 
                        task_id, attempt, config.retry_config.max_attempts, error_msg);
                }
                Err(_) => {
                    // Operation timed out
                    last_error = Some(crate::error::Error::Other(anyhow::anyhow!("Operation timed out")));
                    warn!("Repair operation timed out for task {} (attempt {}/{})", 
                        task_id, attempt, config.retry_config.max_attempts);
                }
            }
        }

        // All attempts failed
        let duration = start_time.elapsed();
        let heal_result = HealResult {
            success: false,
            original_issue: task.issue.clone(),
            repair_duration: duration,
            retry_attempts: attempt - 1,
            error_message: last_error.map(|e| e.to_string()),
            metadata: None,
            completed_at: SystemTime::now(),
        };

        // Send result
        if let Err(e) = result_tx.send(heal_result).await {
            error!("Failed to send heal result: {}", e);
        }

        // Update statistics
        {
            let mut stats = statistics.write().await;
            stats.failed_repairs += 1;
            stats.total_repair_time += duration;
            stats.average_repair_time = if stats.total_tasks_processed > 0 {
                Duration::from_secs_f64(
                    stats.total_repair_time.as_secs_f64() / stats.total_tasks_processed as f64
                )
            } else {
                Duration::ZERO
            };
            stats.total_retry_attempts += (attempt - 1) as u64;
            stats.last_task_time = Some(SystemTime::now());
            stats.status = WorkerStatus::Idle;
        }

        error!("Failed to complete repair task after {} attempts: {} (worker: {})", 
            attempt, task_id, config.worker_id);
        Ok(())
    }

    /// Perform the actual repair operation
    async fn perform_repair_operation(task: &HealTask, config: &RepairWorkerConfig) -> Result<()> {
        if config.enable_detailed_logging {
            debug!("Starting repair operation for task: {} (worker: {})", task.id, config.worker_id);
        }

        // Simulate repair operation based on issue type
        match task.issue.issue_type {
            crate::scanner::HealthIssueType::MissingReplica => {
                // Simulate replica repair
                sleep(Duration::from_millis(100)).await;
                if config.enable_detailed_logging {
                    debug!("Repaired missing replica for {}/{}", task.issue.bucket, task.issue.object);
                }
            }
            crate::scanner::HealthIssueType::ChecksumMismatch => {
                // Simulate checksum repair
                sleep(Duration::from_millis(200)).await;
                if config.enable_detailed_logging {
                    debug!("Repaired checksum mismatch for {}/{}", task.issue.bucket, task.issue.object);
                }
            }
            crate::scanner::HealthIssueType::DiskReadError => {
                // Simulate disk error recovery
                sleep(Duration::from_millis(300)).await;
                if config.enable_detailed_logging {
                    debug!("Recovered from disk read error for {}/{}", task.issue.bucket, task.issue.object);
                }
            }
            _ => {
                // Generic repair for other issue types
                sleep(Duration::from_millis(150)).await;
                if config.enable_detailed_logging {
                    debug!("Performed generic repair for {}/{}", task.issue.bucket, task.issue.object);
                }
            }
        }

        // Simulate occasional failures for testing
        if task.retry_count > 0 && task.retry_count % 3 == 0 {
            return Err(crate::error::Error::Other(anyhow::anyhow!("Simulated repair failure")));
        }

        if config.enable_detailed_logging {
            debug!("Completed repair operation for task: {} (worker: {})", task.id, config.worker_id);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scanner::{HealthIssue, HealthIssueType, Severity};

    #[tokio::test]
    async fn test_repair_worker_creation() {
        let config = RepairWorkerConfig::default();
        let (result_tx, _result_rx) = mpsc::channel(100);
        let worker = RepairWorker::new(config, result_tx);
        
        assert_eq!(worker.status().await, WorkerStatus::Idle);
    }

    #[tokio::test]
    async fn test_repair_worker_start_stop() {
        let config = RepairWorkerConfig::default();
        let (result_tx, _result_rx) = mpsc::channel(100);
        let mut worker = RepairWorker::new(config, result_tx);
        
        // Start worker
        worker.start().await.unwrap();
        sleep(Duration::from_millis(100)).await;
        
        // Check status
        let status = worker.status().await;
        assert_eq!(status, WorkerStatus::Idle);
        
        // Stop worker
        worker.stop().await.unwrap();
        sleep(Duration::from_millis(100)).await;
        
        // Check status
        let status = worker.status().await;
        assert_eq!(status, WorkerStatus::Stopped);
    }

    #[tokio::test]
    async fn test_repair_worker_statistics() {
        let config = RepairWorkerConfig::default();
        let (result_tx, _result_rx) = mpsc::channel(100);
        let worker = RepairWorker::new(config, result_tx);
        
        let stats = worker.statistics().await;
        assert_eq!(stats.total_tasks_processed, 0);
        assert_eq!(stats.successful_repairs, 0);
        assert_eq!(stats.failed_repairs, 0);
        assert_eq!(stats.status, WorkerStatus::Idle);
    }
} 