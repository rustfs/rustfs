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

//! Monitoring and auditing utilities for KMS operations
//!
//! This module provides comprehensive monitoring, metrics collection,
//! and auditing capabilities for KMS operations.

use crate::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;
use tracing::{error, warn};

/// KMS operation types for monitoring
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum KmsOperation {
    /// Encrypt operation
    Encrypt,
    /// Decrypt operation
    Decrypt,
    /// Generate data key operation
    GenerateDataKey,
    /// List keys operation
    ListKeys,
    /// Get key info operation
    GetKeyInfo,
    /// Health check operation
    HealthCheck,
}

impl std::fmt::Display for KmsOperation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            KmsOperation::Encrypt => write!(f, "encrypt"),
            KmsOperation::Decrypt => write!(f, "decrypt"),
            KmsOperation::GenerateDataKey => write!(f, "generate_data_key"),
            KmsOperation::ListKeys => write!(f, "list_keys"),
            KmsOperation::GetKeyInfo => write!(f, "get_key_info"),
            KmsOperation::HealthCheck => write!(f, "health_check"),
        }
    }
}

/// Operation result status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum OperationStatus {
    /// Operation completed successfully
    Success,
    /// Operation failed
    Failure,
    /// Operation timed out
    Timeout,
}

/// Metrics for a specific operation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperationMetrics {
    /// Total number of operations
    pub total_count: u64,
    /// Number of successful operations
    pub success_count: u64,
    /// Number of failed operations
    pub failure_count: u64,
    /// Number of timed out operations
    pub timeout_count: u64,
    /// Average operation duration in milliseconds
    pub avg_duration_ms: f64,
    /// Minimum operation duration in milliseconds
    pub min_duration_ms: f64,
    /// Maximum operation duration in milliseconds
    pub max_duration_ms: f64,
    /// Total duration of all operations in milliseconds
    pub total_duration_ms: f64,
}

impl Default for OperationMetrics {
    fn default() -> Self {
        Self {
            total_count: 0,
            success_count: 0,
            failure_count: 0,
            timeout_count: 0,
            avg_duration_ms: 0.0,
            min_duration_ms: f64::MAX,
            max_duration_ms: 0.0,
            total_duration_ms: 0.0,
        }
    }
}

impl OperationMetrics {
    /// Update metrics with a new operation result
    pub fn update(&mut self, status: OperationStatus, duration: Duration) {
        let duration_ms = duration.as_secs_f64() * 1000.0;

        self.total_count += 1;
        self.total_duration_ms += duration_ms;

        match status {
            OperationStatus::Success => self.success_count += 1,
            OperationStatus::Failure => self.failure_count += 1,
            OperationStatus::Timeout => self.timeout_count += 1,
        }

        if duration_ms < self.min_duration_ms {
            self.min_duration_ms = duration_ms;
        }
        if duration_ms > self.max_duration_ms {
            self.max_duration_ms = duration_ms;
        }

        self.avg_duration_ms = self.total_duration_ms / self.total_count as f64;
    }

    /// Get success rate as a percentage
    pub fn success_rate(&self) -> f64 {
        if self.total_count == 0 {
            0.0
        } else {
            (self.success_count as f64 / self.total_count as f64) * 100.0
        }
    }

    /// Get failure rate as a percentage
    pub fn failure_rate(&self) -> f64 {
        if self.total_count == 0 {
            0.0
        } else {
            (self.failure_count as f64 / self.total_count as f64) * 100.0
        }
    }
}

/// Audit log entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditLogEntry {
    /// Timestamp of the operation
    pub timestamp: u64,
    /// Operation type
    pub operation: KmsOperation,
    /// Operation status
    pub status: OperationStatus,
    /// Operation duration in milliseconds
    pub duration_ms: f64,
    /// Key ID involved in the operation (if applicable)
    pub key_id: Option<String>,
    /// User or service that initiated the operation
    pub principal: Option<String>,
    /// Additional context information
    pub context: HashMap<String, String>,
    /// Error message (if operation failed)
    pub error_message: Option<String>,
}

impl AuditLogEntry {
    /// Create a new audit log entry
    pub fn new(operation: KmsOperation, status: OperationStatus, duration: Duration) -> Self {
        Self {
            timestamp: SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs(),
            operation,
            status,
            duration_ms: duration.as_secs_f64() * 1000.0,
            key_id: None,
            principal: None,
            context: HashMap::new(),
            error_message: None,
        }
    }

    /// Set the key ID for this audit entry
    pub fn with_key_id(mut self, key_id: String) -> Self {
        self.key_id = Some(key_id);
        self
    }

    /// Set the principal for this audit entry
    pub fn with_principal(mut self, principal: String) -> Self {
        self.principal = Some(principal);
        self
    }

    /// Add context information
    pub fn with_context(mut self, key: String, value: String) -> Self {
        self.context.insert(key, value);
        self
    }

    /// Set error message for failed operations
    pub fn with_error(mut self, error_message: String) -> Self {
        self.error_message = Some(error_message);
        self
    }
}

/// Configuration for monitoring and auditing
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MonitoringConfig {
    /// Whether monitoring is enabled
    pub enabled: bool,
    /// Whether audit logging is enabled
    pub audit_enabled: bool,
    /// Maximum number of audit log entries to keep in memory
    pub max_audit_entries: usize,
    /// Metrics collection interval in seconds
    pub metrics_interval_secs: u64,
    /// Whether to log slow operations
    pub log_slow_operations: bool,
    /// Threshold for slow operations in milliseconds
    pub slow_operation_threshold_ms: f64,
}

impl Default for MonitoringConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            audit_enabled: true,
            max_audit_entries: 10000,
            metrics_interval_secs: 60,
            log_slow_operations: true,
            slow_operation_threshold_ms: 1000.0,
        }
    }
}

/// KMS monitoring and auditing manager
pub struct KmsMonitor {
    config: MonitoringConfig,
    metrics: Arc<RwLock<HashMap<KmsOperation, OperationMetrics>>>,
    audit_log: Arc<RwLock<Vec<AuditLogEntry>>>,
}

impl KmsMonitor {
    /// Create a new KMS monitor
    pub fn new(config: MonitoringConfig) -> Self {
        Self {
            config,
            metrics: Arc::new(RwLock::new(HashMap::new())),
            audit_log: Arc::new(RwLock::new(Vec::new())),
        }
    }

    /// Record an operation for monitoring and auditing
    pub async fn record_operation(
        &self,
        operation: KmsOperation,
        status: OperationStatus,
        duration: Duration,
        key_id: Option<String>,
        principal: Option<String>,
        error_message: Option<String>,
    ) -> Result<()> {
        if !self.config.enabled {
            return Ok(());
        }

        // Update metrics
        {
            let mut metrics = self.metrics.write().await;
            let operation_metrics = metrics.entry(operation).or_default();
            operation_metrics.update(status, duration);
        }

        // Log slow operations
        if self.config.log_slow_operations {
            let duration_ms = duration.as_secs_f64() * 1000.0;
            if duration_ms > self.config.slow_operation_threshold_ms {
                warn!("Slow KMS operation detected: {} took {:.2}ms", operation, duration_ms);
            }
        }

        // Add audit log entry
        if self.config.audit_enabled {
            let mut audit_entry = AuditLogEntry::new(operation, status, duration);

            if let Some(key_id) = key_id {
                audit_entry = audit_entry.with_key_id(key_id);
            }

            if let Some(principal) = principal {
                audit_entry = audit_entry.with_principal(principal);
            }

            if let Some(error_message) = error_message {
                audit_entry = audit_entry.with_error(error_message);
            }

            let mut audit_log = self.audit_log.write().await;
            audit_log.push(audit_entry);

            // Trim audit log if it exceeds max size
            if audit_log.len() > self.config.max_audit_entries {
                let excess = audit_log.len() - self.config.max_audit_entries;
                audit_log.drain(0..excess);
            }
        }

        Ok(())
    }

    /// Get current metrics for all operations
    pub async fn get_metrics(&self) -> HashMap<KmsOperation, OperationMetrics> {
        self.metrics.read().await.clone()
    }

    /// Get metrics for a specific operation
    pub async fn get_operation_metrics(&self, operation: KmsOperation) -> Option<OperationMetrics> {
        self.metrics.read().await.get(&operation).cloned()
    }

    /// Get recent audit log entries
    pub async fn get_audit_log(&self, limit: Option<usize>) -> Vec<AuditLogEntry> {
        let audit_log = self.audit_log.read().await;
        let limit = limit.unwrap_or(audit_log.len());

        if limit >= audit_log.len() {
            audit_log.clone()
        } else {
            audit_log[audit_log.len() - limit..].to_vec()
        }
    }

    /// Clear all metrics
    pub async fn clear_metrics(&self) {
        self.metrics.write().await.clear();
    }

    /// Clear audit log
    pub async fn clear_audit_log(&self) {
        self.audit_log.write().await.clear();
    }

    /// Get monitoring configuration
    pub fn config(&self) -> &MonitoringConfig {
        &self.config
    }

    /// Generate a summary report
    pub async fn generate_report(&self) -> MonitoringReport {
        let metrics = self.get_metrics().await;
        let audit_log_size = self.audit_log.read().await.len();

        let mut total_operations = 0;
        let mut total_successes = 0;
        let mut total_failures = 0;
        let mut avg_duration = 0.0;

        for operation_metrics in metrics.values() {
            total_operations += operation_metrics.total_count;
            total_successes += operation_metrics.success_count;
            total_failures += operation_metrics.failure_count;
            avg_duration += operation_metrics.avg_duration_ms;
        }

        if !metrics.is_empty() {
            avg_duration /= metrics.len() as f64;
        }

        MonitoringReport {
            total_operations,
            total_successes,
            total_failures,
            overall_success_rate: if total_operations > 0 {
                (total_successes as f64 / total_operations as f64) * 100.0
            } else {
                0.0
            },
            avg_operation_duration_ms: avg_duration,
            audit_log_entries: audit_log_size,
            operation_metrics: metrics,
        }
    }
}

/// Monitoring report summary
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MonitoringReport {
    /// Total number of operations across all types
    pub total_operations: u64,
    /// Total number of successful operations
    pub total_successes: u64,
    /// Total number of failed operations
    pub total_failures: u64,
    /// Overall success rate as a percentage
    pub overall_success_rate: f64,
    /// Average operation duration across all types in milliseconds
    pub avg_operation_duration_ms: f64,
    /// Number of audit log entries
    pub audit_log_entries: usize,
    /// Detailed metrics per operation type
    pub operation_metrics: HashMap<KmsOperation, OperationMetrics>,
}

/// Operation timer for measuring execution time
pub struct OperationTimer {
    start_time: Instant,
    operation: KmsOperation,
    monitor: Arc<KmsMonitor>,
    key_id: Option<String>,
    principal: Option<String>,
}

impl OperationTimer {
    /// Start timing an operation
    pub fn start(operation: KmsOperation, monitor: Arc<KmsMonitor>) -> Self {
        Self {
            start_time: Instant::now(),
            operation,
            monitor,
            key_id: None,
            principal: None,
        }
    }

    /// Set the key ID for this operation
    pub fn with_key_id(mut self, key_id: String) -> Self {
        self.key_id = Some(key_id);
        self
    }

    /// Set the principal for this operation
    pub fn with_principal(mut self, principal: String) -> Self {
        self.principal = Some(principal);
        self
    }

    /// Complete the operation with success status
    pub async fn complete_success(self) {
        let duration = self.start_time.elapsed();
        if let Err(e) = self
            .monitor
            .record_operation(self.operation, OperationStatus::Success, duration, self.key_id, self.principal, None)
            .await
        {
            error!("Failed to record operation metrics: {}", e);
        }
    }

    /// Complete the operation with failure status
    pub async fn complete_failure(self, error_message: String) {
        let duration = self.start_time.elapsed();
        if let Err(e) = self
            .monitor
            .record_operation(
                self.operation,
                OperationStatus::Failure,
                duration,
                self.key_id,
                self.principal,
                Some(error_message),
            )
            .await
        {
            error!("Failed to record operation metrics: {}", e);
        }
    }

    /// Complete the operation with timeout status
    pub async fn complete_timeout(self) {
        let duration = self.start_time.elapsed();
        if let Err(e) = self
            .monitor
            .record_operation(
                self.operation,
                OperationStatus::Timeout,
                duration,
                self.key_id,
                self.principal,
                Some("Operation timed out".to_string()),
            )
            .await
        {
            error!("Failed to record operation metrics: {}", e);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::test;

    #[test]
    async fn test_operation_metrics() {
        let mut metrics = OperationMetrics::default();

        metrics.update(OperationStatus::Success, Duration::from_millis(100));
        metrics.update(OperationStatus::Failure, Duration::from_millis(200));
        metrics.update(OperationStatus::Success, Duration::from_millis(150));

        assert_eq!(metrics.total_count, 3);
        assert_eq!(metrics.success_count, 2);
        assert_eq!(metrics.failure_count, 1);

        let success_rate = metrics.success_rate();
        let failure_rate = metrics.failure_rate();

        // Allow small floating point differences
        assert!((success_rate - 66.66666666666667).abs() < 0.01);
        assert!((failure_rate - 33.333333333333336).abs() < 0.01);
    }

    #[test]
    async fn test_kms_monitor() {
        let config = MonitoringConfig::default();
        let monitor = KmsMonitor::new(config);

        monitor
            .record_operation(
                KmsOperation::Encrypt,
                OperationStatus::Success,
                Duration::from_millis(100),
                Some("key-123".to_string()),
                Some("user-456".to_string()),
                None,
            )
            .await
            .expect("Failed to record operation");

        let metrics = monitor.get_operation_metrics(KmsOperation::Encrypt).await;
        assert!(metrics.is_some());

        let metrics = metrics.expect("Metrics should be available");
        assert_eq!(metrics.total_count, 1);
        assert_eq!(metrics.success_count, 1);

        let audit_log = monitor.get_audit_log(None).await;
        assert_eq!(audit_log.len(), 1);
        assert_eq!(audit_log[0].operation, KmsOperation::Encrypt);
        assert_eq!(audit_log[0].key_id, Some("key-123".to_string()));
    }

    #[test]
    async fn test_operation_timer() {
        let config = MonitoringConfig::default();
        let monitor = Arc::new(KmsMonitor::new(config));

        let timer = OperationTimer::start(KmsOperation::Decrypt, monitor.clone()).with_key_id("key-789".to_string());

        tokio::time::sleep(Duration::from_millis(10)).await;
        timer.complete_success().await;

        let metrics = monitor.get_operation_metrics(KmsOperation::Decrypt).await;
        assert!(metrics.is_some());

        let metrics = metrics.expect("Metrics should be available");
        assert_eq!(metrics.total_count, 1);
        assert_eq!(metrics.success_count, 1);
    }

    #[test]
    async fn test_monitoring_report() {
        let config = MonitoringConfig::default();
        let monitor = KmsMonitor::new(config);

        monitor
            .record_operation(
                KmsOperation::Encrypt,
                OperationStatus::Success,
                Duration::from_millis(100),
                None,
                None,
                None,
            )
            .await
            .expect("Failed to record encrypt operation");

        monitor
            .record_operation(
                KmsOperation::Decrypt,
                OperationStatus::Failure,
                Duration::from_millis(200),
                None,
                None,
                Some("Test error".to_string()),
            )
            .await
            .expect("Failed to record decrypt operation");

        let report = monitor.generate_report().await;
        assert_eq!(report.total_operations, 2);
        assert_eq!(report.total_successes, 1);
        assert_eq!(report.total_failures, 1);
        assert_eq!(report.overall_success_rate, 50.0);
        assert_eq!(report.audit_log_entries, 2);
    }
}
