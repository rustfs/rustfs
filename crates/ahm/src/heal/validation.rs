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

use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

use crate::error::Result;
use super::{HealResult, HealTask};

/// Configuration for validation operations
#[derive(Debug, Clone)]
pub struct ValidationConfig {
    /// Whether to enable validation after repair
    pub enable_post_repair_validation: bool,
    /// Timeout for validation operations
    pub validation_timeout: Duration,
    /// Whether to enable detailed validation logging
    pub enable_detailed_logging: bool,
    /// Maximum number of validation retries
    pub max_validation_retries: u32,
    /// Validation retry delay
    pub validation_retry_delay: Duration,
}

impl Default for ValidationConfig {
    fn default() -> Self {
        Self {
            enable_post_repair_validation: true,
            validation_timeout: Duration::from_secs(60), // 1 minute
            max_validation_retries: 3,
            validation_retry_delay: Duration::from_secs(5),
            enable_detailed_logging: true,
        }
    }
}

/// Validation result for a repair operation
#[derive(Debug, Clone)]
pub struct ValidationResult {
    /// Whether validation passed
    pub passed: bool,
    /// Validation type
    pub validation_type: ValidationType,
    /// Detailed validation message
    pub message: String,
    /// Time taken for validation
    pub duration: Duration,
    /// Validation timestamp
    pub timestamp: SystemTime,
    /// Additional validation metadata
    pub metadata: Option<serde_json::Value>,
}

/// Types of validation that can be performed
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ValidationType {
    /// Checksum validation
    Checksum,
    /// File existence validation
    FileExistence,
    /// File size validation
    FileSize,
    /// File permissions validation
    FilePermissions,
    /// Metadata consistency validation
    MetadataConsistency,
    /// Replication status validation
    ReplicationStatus,
    /// Data integrity validation
    DataIntegrity,
    /// Custom validation
    Custom(String),
}

/// Statistics for validation operations
#[derive(Debug, Clone, Default)]
pub struct ValidationStatistics {
    /// Total number of validations performed
    pub total_validations: u64,
    /// Number of successful validations
    pub successful_validations: u64,
    /// Number of failed validations
    pub failed_validations: u64,
    /// Total time spent on validation
    pub total_validation_time: Duration,
    /// Average validation time
    pub average_validation_time: Duration,
    /// Number of validation retries
    pub total_validation_retries: u64,
    /// Last validation time
    pub last_validation_time: Option<SystemTime>,
}

/// Validator for repair operations
pub struct HealValidator {
    config: ValidationConfig,
    statistics: Arc<RwLock<ValidationStatistics>>,
}

impl HealValidator {
    /// Create a new validator
    pub fn new(config: ValidationConfig) -> Self {
        Self {
            config,
            statistics: Arc::new(RwLock::new(ValidationStatistics::default())),
        }
    }

    /// Validate a repair operation
    pub async fn validate_repair(&self, task: &HealTask, result: &HealResult) -> Result<Vec<ValidationResult>> {
        if !self.config.enable_post_repair_validation {
            return Ok(Vec::new());
        }

        let start_time = Instant::now();
        let mut validation_results = Vec::new();

        info!("Starting validation for repair task: {}", task.id);

        // Perform different types of validation based on the issue type
        match task.issue.issue_type {
            crate::scanner::HealthIssueType::MissingReplica => {
                validation_results.extend(self.validate_replica_repair(task, result).await?);
            }
            crate::scanner::HealthIssueType::ChecksumMismatch => {
                validation_results.extend(self.validate_checksum_repair(task, result).await?);
            }
            crate::scanner::HealthIssueType::DiskReadError => {
                validation_results.extend(self.validate_disk_repair(task, result).await?);
            }
            _ => {
                validation_results.extend(self.validate_generic_repair(task, result).await?);
            }
        }

        let duration = start_time.elapsed();

        // Update statistics
        {
            let mut stats = self.statistics.write().await;
            stats.total_validations += validation_results.len() as u64;
            stats.total_validation_time += duration;
            stats.average_validation_time = if stats.total_validations > 0 {
                Duration::from_secs_f64(
                    stats.total_validation_time.as_secs_f64() / stats.total_validations as f64
                )
            } else {
                Duration::ZERO
            };
            stats.last_validation_time = Some(SystemTime::now());

            let successful_count = validation_results.iter().filter(|r| r.passed).count();
            let failed_count = validation_results.len() - successful_count;
            stats.successful_validations += successful_count as u64;
            stats.failed_validations += failed_count as u64;
        }

        if self.config.enable_detailed_logging {
            debug!("Validation completed for task {}: {} passed, {} failed", 
                task.id, 
                validation_results.iter().filter(|r| r.passed).count(),
                validation_results.iter().filter(|r| !r.passed).count()
            );
        }

        Ok(validation_results)
    }

    /// Validate replica repair
    async fn validate_replica_repair(&self, task: &HealTask, _result: &HealResult) -> Result<Vec<ValidationResult>> {
        let mut results = Vec::new();

        // Validate file existence
        let existence_result = self.validate_file_existence(&task.issue.bucket, &task.issue.object).await;
        results.push(existence_result);

        // Validate replication status
        let replication_result = self.validate_replication_status(&task.issue.bucket, &task.issue.object).await;
        results.push(replication_result);

        Ok(results)
    }

    /// Validate checksum repair
    async fn validate_checksum_repair(&self, task: &HealTask, _result: &HealResult) -> Result<Vec<ValidationResult>> {
        let mut results = Vec::new();

        // Validate checksum
        let checksum_result = self.validate_checksum(&task.issue.bucket, &task.issue.object).await;
        results.push(checksum_result);

        // Validate data integrity
        let integrity_result = self.validate_data_integrity(&task.issue.bucket, &task.issue.object).await;
        results.push(integrity_result);

        Ok(results)
    }

    /// Validate disk repair
    async fn validate_disk_repair(&self, task: &HealTask, _result: &HealResult) -> Result<Vec<ValidationResult>> {
        let mut results = Vec::new();

        // Validate file existence
        let existence_result = self.validate_file_existence(&task.issue.bucket, &task.issue.object).await;
        results.push(existence_result);

        // Validate file permissions
        let permissions_result = self.validate_file_permissions(&task.issue.bucket, &task.issue.object).await;
        results.push(permissions_result);

        Ok(results)
    }

    /// Validate generic repair
    async fn validate_generic_repair(&self, task: &HealTask, _result: &HealResult) -> Result<Vec<ValidationResult>> {
        let mut results = Vec::new();

        // Validate file existence
        let existence_result = self.validate_file_existence(&task.issue.bucket, &task.issue.object).await;
        results.push(existence_result);

        // Validate metadata consistency
        let metadata_result = self.validate_metadata_consistency(&task.issue.bucket, &task.issue.object).await;
        results.push(metadata_result);

        Ok(results)
    }

    /// Validate file existence
    async fn validate_file_existence(&self, bucket: &str, object: &str) -> ValidationResult {
        let start_time = Instant::now();
        
        // Simulate file existence check
        tokio::time::sleep(Duration::from_millis(10)).await;
        
        let duration = start_time.elapsed();
        let passed = true; // Simulate successful validation
        
        ValidationResult {
            passed,
            validation_type: ValidationType::FileExistence,
            message: format!("File existence validation for {}/{}", bucket, object),
            duration,
            timestamp: SystemTime::now(),
            metadata: None,
        }
    }

    /// Validate checksum
    async fn validate_checksum(&self, bucket: &str, object: &str) -> ValidationResult {
        let start_time = Instant::now();
        
        // Simulate checksum validation
        tokio::time::sleep(Duration::from_millis(20)).await;
        
        let duration = start_time.elapsed();
        let passed = true; // Simulate successful validation
        
        ValidationResult {
            passed,
            validation_type: ValidationType::Checksum,
            message: format!("Checksum validation for {}/{}", bucket, object),
            duration,
            timestamp: SystemTime::now(),
            metadata: None,
        }
    }

    /// Validate replication status
    async fn validate_replication_status(&self, bucket: &str, object: &str) -> ValidationResult {
        let start_time = Instant::now();
        
        // Simulate replication status validation
        tokio::time::sleep(Duration::from_millis(15)).await;
        
        let duration = start_time.elapsed();
        let passed = true; // Simulate successful validation
        
        ValidationResult {
            passed,
            validation_type: ValidationType::ReplicationStatus,
            message: format!("Replication status validation for {}/{}", bucket, object),
            duration,
            timestamp: SystemTime::now(),
            metadata: None,
        }
    }

    /// Validate file permissions
    async fn validate_file_permissions(&self, bucket: &str, object: &str) -> ValidationResult {
        let start_time = Instant::now();
        
        // Simulate file permissions validation
        tokio::time::sleep(Duration::from_millis(5)).await;
        
        let duration = start_time.elapsed();
        let passed = true; // Simulate successful validation
        
        ValidationResult {
            passed,
            validation_type: ValidationType::FilePermissions,
            message: format!("File permissions validation for {}/{}", bucket, object),
            duration,
            timestamp: SystemTime::now(),
            metadata: None,
        }
    }

    /// Validate metadata consistency
    async fn validate_metadata_consistency(&self, bucket: &str, object: &str) -> ValidationResult {
        let start_time = Instant::now();
        
        // Simulate metadata consistency validation
        tokio::time::sleep(Duration::from_millis(25)).await;
        
        let duration = start_time.elapsed();
        let passed = true; // Simulate successful validation
        
        ValidationResult {
            passed,
            validation_type: ValidationType::MetadataConsistency,
            message: format!("Metadata consistency validation for {}/{}", bucket, object),
            duration,
            timestamp: SystemTime::now(),
            metadata: None,
        }
    }

    /// Validate data integrity
    async fn validate_data_integrity(&self, bucket: &str, object: &str) -> ValidationResult {
        let start_time = Instant::now();
        
        // Simulate data integrity validation
        tokio::time::sleep(Duration::from_millis(30)).await;
        
        let duration = start_time.elapsed();
        let passed = true; // Simulate successful validation
        
        ValidationResult {
            passed,
            validation_type: ValidationType::DataIntegrity,
            message: format!("Data integrity validation for {}/{}", bucket, object),
            duration,
            timestamp: SystemTime::now(),
            metadata: None,
        }
    }

    /// Get validation statistics
    pub async fn statistics(&self) -> ValidationStatistics {
        self.statistics.read().await.clone()
    }

    /// Reset validation statistics
    pub async fn reset_statistics(&self) {
        let mut stats = self.statistics.write().await;
        *stats = ValidationStatistics::default();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scanner::{HealthIssue, HealthIssueType, Severity};

    #[tokio::test]
    async fn test_validator_creation() {
        let config = ValidationConfig::default();
        let validator = HealValidator::new(config);
        
        let stats = validator.statistics().await;
        assert_eq!(stats.total_validations, 0);
    }

    #[tokio::test]
    async fn test_validate_repair() {
        let config = ValidationConfig::default();
        let validator = HealValidator::new(config);
        
        let issue = HealthIssue {
            issue_type: HealthIssueType::MissingReplica,
            severity: Severity::Critical,
            bucket: "test-bucket".to_string(),
            object: "test-object".to_string(),
            description: "Test issue".to_string(),
            metadata: None,
        };
        
        let task = super::HealTask::new(issue);
        let result = super::HealResult {
            success: true,
            original_issue: task.issue.clone(),
            repair_duration: Duration::from_secs(1),
            retry_attempts: 0,
            error_message: None,
            metadata: None,
            completed_at: SystemTime::now(),
        };
        
        let validation_results = validator.validate_repair(&task, &result).await.unwrap();
        assert!(!validation_results.is_empty());
        
        let stats = validator.statistics().await;
        assert_eq!(stats.total_validations, validation_results.len() as u64);
    }

    #[tokio::test]
    async fn test_validation_disabled() {
        let mut config = ValidationConfig::default();
        config.enable_post_repair_validation = false;
        let validator = HealValidator::new(config);
        
        let issue = HealthIssue {
            issue_type: HealthIssueType::MissingReplica,
            severity: Severity::Critical,
            bucket: "test-bucket".to_string(),
            object: "test-object".to_string(),
            description: "Test issue".to_string(),
            metadata: None,
        };
        
        let task = super::HealTask::new(issue);
        let result = super::HealResult {
            success: true,
            original_issue: task.issue.clone(),
            repair_duration: Duration::from_secs(1),
            retry_attempts: 0,
            error_message: None,
            metadata: None,
            completed_at: SystemTime::now(),
        };
        
        let validation_results = validator.validate_repair(&task, &result).await.unwrap();
        assert!(validation_results.is_empty());
    }
} 