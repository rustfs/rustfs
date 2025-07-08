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

//! Healing subsystem for the AHM system
//!
//! The heal subsystem provides intelligent repair capabilities:
//! - Priority-based healing queue
//! - Real-time and background healing modes
//! - Comprehensive repair validation
//! - Adaptive healing strategies

pub mod engine;
pub mod priority_queue;
pub mod repair_worker;
pub mod validation;

pub use engine::HealEngine;
pub use priority_queue::PriorityQueue;
pub use repair_worker::RepairWorker;
pub use validation::HealValidator;

use std::time::{Duration, SystemTime};
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use derive_builder::Builder;

use crate::scanner::{HealthIssue, HealthIssueType, Severity};

/// Configuration for the healing system
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealConfig {
    /// Maximum number of concurrent repair workers
    pub max_workers: usize,
    /// Maximum number of tasks in the priority queue
    pub max_queue_size: usize,
    /// Timeout for individual repair operations
    pub repair_timeout: Duration,
    /// Interval between healing cycles
    pub heal_interval: Duration,
    /// Whether to enable automatic healing
    pub auto_heal_enabled: bool,
    /// Maximum number of retry attempts for failed repairs
    pub max_retry_attempts: u32,
    /// Backoff delay between retry attempts
    pub retry_backoff_delay: Duration,
    /// Whether to validate repairs after completion
    pub validate_after_repair: bool,
}

impl Default for HealConfig {
    fn default() -> Self {
        Self {
            max_workers: 4,
            max_queue_size: 1000,
            repair_timeout: Duration::from_secs(300), // 5 minutes
            heal_interval: Duration::from_secs(60),   // 1 minute
            auto_heal_enabled: true,
            max_retry_attempts: 3,
            retry_backoff_delay: Duration::from_secs(30),
            validate_after_repair: true,
        }
    }
}

/// Result of a healing operation
#[derive(Debug, Clone)]
pub struct HealResult {
    /// Whether the healing operation was successful
    pub success: bool,
    /// The original health issue that was addressed
    pub original_issue: HealthIssue,
    /// Time taken to complete the repair
    pub repair_duration: Duration,
    /// Number of retry attempts made
    pub retry_attempts: u32,
    /// Error message if repair failed
    pub error_message: Option<String>,
    /// Additional metadata about the repair
    pub metadata: Option<serde_json::Value>,
    /// Timestamp when the repair was completed
    pub completed_at: SystemTime,
}

/// Statistics for the healing system
#[derive(Debug, Clone, Default)]
pub struct HealStatistics {
    /// Total number of repair tasks processed
    pub total_repairs: u64,
    /// Number of successful repairs
    pub successful_repairs: u64,
    /// Number of failed repairs
    pub failed_repairs: u64,
    /// Number of tasks currently in queue
    pub queued_tasks: u64,
    /// Number of active workers
    pub active_workers: u64,
    /// Total time spent on repairs
    pub total_repair_time: Duration,
    /// Average repair time
    pub average_repair_time: Duration,
    /// Last repair completion time
    pub last_repair_time: Option<SystemTime>,
    /// Number of retry attempts made
    pub total_retry_attempts: u64,
}

/// Priority levels for healing tasks
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum HealPriority {
    /// Critical issues that need immediate attention
    Critical = 0,
    /// High priority issues
    High = 1,
    /// Medium priority issues
    Medium = 2,
    /// Low priority issues
    Low = 3,
}

impl From<Severity> for HealPriority {
    fn from(severity: Severity) -> Self {
        match severity {
            Severity::Critical => HealPriority::Critical,
            Severity::High => HealPriority::High,
            Severity::Medium => HealPriority::Medium,
            Severity::Low => HealPriority::Low,
        }
    }
}

/// A healing task to be processed
#[derive(Debug, Clone)]
pub struct HealTask {
    /// Unique identifier for the task
    pub id: String,
    /// The health issue to be repaired
    pub issue: HealthIssue,
    /// Priority level for this task
    pub priority: HealPriority,
    /// When the task was created
    pub created_at: SystemTime,
    /// When the task should be processed (for delayed tasks)
    pub scheduled_at: Option<SystemTime>,
    /// Number of retry attempts made
    pub retry_count: u32,
    /// Maximum number of retry attempts allowed
    pub max_retries: u32,
    /// Additional context for the repair operation
    pub context: Option<serde_json::Value>,
}

impl HealTask {
    /// Create a new healing task
    pub fn new(issue: HealthIssue) -> Self {
        let priority = HealPriority::from(issue.severity);
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            issue,
            priority,
            created_at: SystemTime::now(),
            scheduled_at: None,
            retry_count: 0,
            max_retries: 3,
            context: None,
        }
    }

    /// Create a delayed healing task
    pub fn delayed(issue: HealthIssue, delay: Duration) -> Self {
        let mut task = Self::new(issue);
        task.scheduled_at = Some(SystemTime::now() + delay);
        task
    }

    /// Check if the task is ready to be processed
    pub fn is_ready(&self) -> bool {
        if let Some(scheduled_at) = self.scheduled_at {
            SystemTime::now() >= scheduled_at
        } else {
            true
        }
    }

    /// Check if the task can be retried
    pub fn can_retry(&self) -> bool {
        self.retry_count < self.max_retries
    }

    /// Increment the retry count
    pub fn increment_retry(&mut self) {
        self.retry_count += 1;
    }
}

/// Heal engine status
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Status {
    /// Heal engine is initializing
    Initializing,
    /// Heal engine is idle
    Idle,
    /// Heal engine is running normally
    Running,
    /// Heal engine is actively healing
    Healing,
    /// Heal engine is paused
    Paused,
    /// Heal engine is stopping
    Stopping,
    /// Heal engine has stopped
    Stopped,
    /// Heal engine encountered an error
    Error(String),
}

/// Healing operation modes
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum HealMode {
    /// Real-time healing during GET/PUT operations
    RealTime,
    /// Background healing during scheduled scans
    Background,
    /// On-demand healing triggered by admin
    OnDemand,
    /// Emergency healing for critical issues
    Emergency,
}

/// Validation result for a repaired object
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationResult {
    /// Type of validation performed
    pub validation_type: ValidationType,
    /// Whether validation passed
    pub passed: bool,
    /// Details about the validation
    pub details: String,
    /// Time taken for validation
    pub duration: Duration,
}

/// Types of validation that can be performed
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ValidationType {
    /// Checksum verification
    Checksum,
    /// Shard count verification
    ShardCount,
    /// Data integrity check
    DataIntegrity,
    /// Metadata consistency check
    MetadataConsistency,
    /// Cross-shard redundancy check
    RedundancyCheck,
}

/// Healing strategies for different scenarios
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum HealStrategy {
    /// Repair using available data shards
    DataShardRepair,
    /// Repair using parity shards
    ParityShardRepair,
    /// Hybrid repair using both data and parity
    HybridRepair,
    /// Metadata-only repair
    MetadataRepair,
    /// Full object reconstruction
    FullReconstruction,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_heal_priority_from_severity() {
        assert_eq!(HealPriority::from(Severity::Critical), HealPriority::Critical);
        assert_eq!(HealPriority::from(Severity::High), HealPriority::High);
        assert_eq!(HealPriority::from(Severity::Medium), HealPriority::Medium);
        assert_eq!(HealPriority::from(Severity::Low), HealPriority::Low);
    }

    #[test]
    fn test_heal_task_creation() {
        let issue = HealthIssue {
            issue_type: HealthIssueType::MissingReplica,
            severity: Severity::Critical,
            bucket: "test-bucket".to_string(),
            object: "test-object".to_string(),
            description: "Test issue".to_string(),
            metadata: None,
        };

        let task = HealTask::new(issue.clone());
        assert_eq!(task.priority, HealPriority::Critical);
        assert_eq!(task.issue.bucket, issue.bucket);
        assert_eq!(task.issue.object, issue.object);
        assert_eq!(task.retry_count, 0);
        assert_eq!(task.max_retries, 3);
        assert!(task.is_ready());
    }

    #[test]
    fn test_delayed_heal_task() {
        let issue = HealthIssue {
            issue_type: HealthIssueType::MissingReplica,
            severity: Severity::Medium,
            bucket: "test-bucket".to_string(),
            object: "test-object".to_string(),
            description: "Test issue".to_string(),
            metadata: None,
        };

        let delay = Duration::from_secs(1);
        let task = HealTask::delayed(issue, delay);
        
        assert!(task.scheduled_at.is_some());
        assert!(!task.is_ready()); // Should not be ready immediately
        
        // Wait for the delay to pass
        std::thread::sleep(delay + Duration::from_millis(100));
        assert!(task.is_ready());
    }

    #[test]
    fn test_heal_task_retry_logic() {
        let issue = HealthIssue {
            issue_type: HealthIssueType::MissingReplica,
            severity: Severity::Low,
            bucket: "test-bucket".to_string(),
            object: "test-object".to_string(),
            description: "Test issue".to_string(),
            metadata: None,
        };

        let mut task = HealTask::new(issue);
        assert!(task.can_retry());
        
        task.increment_retry();
        assert_eq!(task.retry_count, 1);
        assert!(task.can_retry());
        
        task.increment_retry();
        task.increment_retry();
        assert_eq!(task.retry_count, 3);
        assert!(!task.can_retry());
    }
} 