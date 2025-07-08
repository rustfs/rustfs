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
    collections::BinaryHeap,
    sync::Arc,
    time::{Duration, SystemTime},
};

use tokio::sync::RwLock;
use tracing::{debug, info, warn};

use crate::error::Result;
use super::{HealPriority, HealTask};

/// Priority queue for healing tasks
pub struct PriorityQueue {
    tasks: Arc<RwLock<BinaryHeap<HealTask>>>,
    max_size: usize,
    statistics: Arc<RwLock<QueueStatistics>>,
}

/// Statistics for the priority queue
#[derive(Debug, Clone, Default)]
pub struct QueueStatistics {
    /// Total number of tasks added to the queue
    pub total_tasks_added: u64,
    /// Total number of tasks removed from the queue
    pub total_tasks_removed: u64,
    /// Current number of tasks in the queue
    pub current_queue_size: u64,
    /// Maximum queue size reached
    pub max_queue_size_reached: u64,
    /// Number of tasks rejected due to queue being full
    pub tasks_rejected: u64,
    /// Average time tasks spend in queue
    pub average_queue_time: Duration,
    /// Total time all tasks have spent in queue
    pub total_queue_time: Duration,
}

impl PriorityQueue {
    /// Create a new priority queue
    pub fn new(max_size: usize) -> Self {
        Self {
            tasks: Arc::new(RwLock::new(BinaryHeap::new())),
            max_size,
            statistics: Arc::new(RwLock::new(QueueStatistics::default())),
        }
    }

    /// Add a task to the queue
    pub async fn push(&self, task: HealTask) -> Result<()> {
        let mut tasks = self.tasks.write().await;
        let mut stats = self.statistics.write().await;

        if tasks.len() >= self.max_size {
            stats.tasks_rejected += 1;
            warn!("Priority queue is full, rejecting task: {}", task.id);
            return Err(crate::error::Error::Other(anyhow::anyhow!("Queue is full")));
        }

        let task_id = task.id.clone();
        let priority = task.priority.clone();
        tasks.push(task);
        stats.total_tasks_added += 1;
        stats.current_queue_size = tasks.len() as u64;
        stats.max_queue_size_reached = stats.max_queue_size_reached.max(tasks.len() as u64);

        debug!("Added task to priority queue: {} (priority: {:?})", task_id, priority);
        Ok(())
    }

    /// Remove and return the highest priority task
    pub async fn pop(&self) -> Option<HealTask> {
        let mut tasks = self.tasks.write().await;
        let mut stats = self.statistics.write().await;

        if let Some(task) = tasks.pop() {
            stats.total_tasks_removed += 1;
            stats.current_queue_size = tasks.len() as u64;

            // Update queue time statistics
            let queue_time = SystemTime::now().duration_since(task.created_at).unwrap_or(Duration::ZERO);
            stats.total_queue_time += queue_time;
            stats.average_queue_time = if stats.total_tasks_removed > 0 {
                Duration::from_secs_f64(
                    stats.total_queue_time.as_secs_f64() / stats.total_tasks_removed as f64
                )
            } else {
                Duration::ZERO
            };

            debug!("Removed task from priority queue: {} (priority: {:?})", task.id, task.priority);
            Some(task)
        } else {
            None
        }
    }

    /// Peek at the highest priority task without removing it
    pub async fn peek(&self) -> Option<HealTask> {
        let tasks = self.tasks.read().await;
        tasks.peek().cloned()
    }

    /// Get the current size of the queue
    pub async fn len(&self) -> usize {
        self.tasks.read().await.len()
    }

    /// Check if the queue is empty
    pub async fn is_empty(&self) -> bool {
        self.tasks.read().await.is_empty()
    }

    /// Get queue statistics
    pub async fn statistics(&self) -> QueueStatistics {
        self.statistics.read().await.clone()
    }

    /// Clear all tasks from the queue
    pub async fn clear(&self) {
        let mut tasks = self.tasks.write().await;
        let mut stats = self.statistics.write().await;

        let cleared_count = tasks.len();
        tasks.clear();
        stats.current_queue_size = 0;

        info!("Cleared {} tasks from priority queue", cleared_count);
    }

    /// Get all tasks that are ready to be processed
    pub async fn get_ready_tasks(&self, max_count: usize) -> Vec<HealTask> {
        let mut tasks = self.tasks.write().await;
        let mut ready_tasks = Vec::new();
        let mut remaining_tasks = Vec::new();

        while let Some(task) = tasks.pop() {
            if task.is_ready() && ready_tasks.len() < max_count {
                ready_tasks.push(task);
            } else {
                remaining_tasks.push(task);
            }
        }

        // Put remaining tasks back
        for task in remaining_tasks {
            tasks.push(task);
        }

        ready_tasks
    }

    /// Remove a specific task by ID
    pub async fn remove_task(&self, task_id: &str) -> bool {
        let mut tasks = self.tasks.write().await;
        let mut stats = self.statistics.write().await;

        let mut temp_tasks = Vec::new();
        let mut found = false;

        while let Some(task) = tasks.pop() {
            if task.id == task_id {
                found = true;
                stats.total_tasks_removed += 1;
                debug!("Removed specific task from queue: {}", task_id);
            } else {
                temp_tasks.push(task);
            }
        }

        // Put remaining tasks back
        for task in temp_tasks {
            tasks.push(task);
        }

        stats.current_queue_size = tasks.len() as u64;
        found
    }

    /// Get tasks by priority level
    pub async fn get_tasks_by_priority(&self, priority: HealPriority) -> Vec<HealTask> {
        let mut tasks = self.tasks.write().await;
        let mut matching_tasks = Vec::new();
        let mut other_tasks = Vec::new();

        while let Some(task) = tasks.pop() {
            if task.priority == priority {
                matching_tasks.push(task);
            } else {
                other_tasks.push(task);
            }
        }

        // Put other tasks back
        for task in other_tasks {
            tasks.push(task);
        }

        matching_tasks
    }

    /// Update task priority
    pub async fn update_priority(&self, task_id: &str, new_priority: HealPriority) -> bool {
        let mut tasks = self.tasks.write().await;

        let mut temp_tasks = Vec::new();
        let mut found = false;

        while let Some(mut task) = tasks.pop() {
            if task.id == task_id {
                task.priority = new_priority.clone();
                found = true;
                debug!("Updated task priority: {} -> {:?}", task_id, new_priority);
            }
            temp_tasks.push(task);
        }

        // Put all tasks back
        for task in temp_tasks {
            tasks.push(task);
        }

        found
    }
}

// Implement Ord for HealTask to enable priority queue functionality
impl std::cmp::Ord for HealTask {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Higher priority (lower enum value) comes first
        self.priority.cmp(&other.priority)
            .then_with(|| self.created_at.cmp(&other.created_at))
    }
}

impl std::cmp::PartialOrd for HealTask {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl std::cmp::PartialEq for HealTask {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl std::cmp::Eq for HealTask {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scanner::{HealthIssue, HealthIssueType, Severity};

    #[tokio::test]
    async fn test_priority_queue_creation() {
        let queue = PriorityQueue::new(100);
        assert_eq!(queue.len().await, 0);
        assert!(queue.is_empty().await);
    }

    #[tokio::test]
    async fn test_priority_queue_push_pop() {
        let queue = PriorityQueue::new(10);
        
        let issue1 = HealthIssue {
            issue_type: HealthIssueType::MissingReplica,
            severity: Severity::Low,
            bucket: "bucket1".to_string(),
            object: "object1".to_string(),
            description: "Test issue 1".to_string(),
            metadata: None,
        };
        
        let issue2 = HealthIssue {
            issue_type: HealthIssueType::MissingReplica,
            severity: Severity::Critical,
            bucket: "bucket2".to_string(),
            object: "object2".to_string(),
            description: "Test issue 2".to_string(),
            metadata: None,
        };

        let task1 = HealTask::new(issue1);
        let task2 = HealTask::new(issue2);

        // Add tasks
        queue.push(task1.clone()).await.unwrap();
        queue.push(task2.clone()).await.unwrap();

        assert_eq!(queue.len().await, 2);

        // Critical task should come first
        let first_task = queue.pop().await.unwrap();
        assert_eq!(first_task.priority, HealPriority::Critical);
        assert_eq!(first_task.id, task2.id);

        let second_task = queue.pop().await.unwrap();
        assert_eq!(second_task.priority, HealPriority::Low);
        assert_eq!(second_task.id, task1.id);

        assert!(queue.is_empty().await);
    }

    #[tokio::test]
    async fn test_priority_queue_full() {
        let queue = PriorityQueue::new(1);
        
        let issue1 = HealthIssue {
            issue_type: HealthIssueType::MissingReplica,
            severity: Severity::Low,
            bucket: "bucket1".to_string(),
            object: "object1".to_string(),
            description: "Test issue 1".to_string(),
            metadata: None,
        };
        
        let issue2 = HealthIssue {
            issue_type: HealthIssueType::MissingReplica,
            severity: Severity::Critical,
            bucket: "bucket2".to_string(),
            object: "object2".to_string(),
            description: "Test issue 2".to_string(),
            metadata: None,
        };

        let task1 = HealTask::new(issue1);
        let task2 = HealTask::new(issue2);

        // First task should succeed
        queue.push(task1).await.unwrap();
        assert_eq!(queue.len().await, 1);

        // Second task should fail
        let result = queue.push(task2).await;
        assert!(result.is_err());
        assert_eq!(queue.len().await, 1);

        let stats = queue.statistics().await;
        assert_eq!(stats.tasks_rejected, 1);
    }

    #[tokio::test]
    async fn test_priority_queue_remove_task() {
        let queue = PriorityQueue::new(10);
        
        let issue = HealthIssue {
            issue_type: HealthIssueType::MissingReplica,
            severity: Severity::Medium,
            bucket: "bucket1".to_string(),
            object: "object1".to_string(),
            description: "Test issue".to_string(),
            metadata: None,
        };

        let task = HealTask::new(issue);
        let task_id = task.id.clone();

        queue.push(task).await.unwrap();
        assert_eq!(queue.len().await, 1);

        // Remove the task
        let removed = queue.remove_task(&task_id).await;
        assert!(removed);
        assert_eq!(queue.len().await, 0);

        // Try to remove non-existent task
        let removed = queue.remove_task("non-existent").await;
        assert!(!removed);
    }

    #[tokio::test]
    async fn test_priority_queue_update_priority() {
        let queue = PriorityQueue::new(10);
        
        let issue = HealthIssue {
            issue_type: HealthIssueType::MissingReplica,
            severity: Severity::Low,
            bucket: "bucket1".to_string(),
            object: "object1".to_string(),
            description: "Test issue".to_string(),
            metadata: None,
        };

        let task = HealTask::new(issue);
        let task_id = task.id.clone();

        queue.push(task).await.unwrap();

        // Update priority
        let updated = queue.update_priority(&task_id, HealPriority::Critical).await;
        assert!(updated);

        // Check that the task now has higher priority
        let popped_task = queue.pop().await.unwrap();
        assert_eq!(popped_task.priority, HealPriority::Critical);
        assert_eq!(popped_task.id, task_id);
    }
} 