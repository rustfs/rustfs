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
use super::{HealthIssue, HealthIssueType, Severity};

/// Configuration for metrics collection
#[derive(Debug, Clone)]
pub struct MetricsConfig {
    /// Collection interval for metrics
    pub collection_interval: Duration,
    /// Retention period for historical metrics
    pub retention_period: Duration,
    /// Maximum number of data points to keep in memory
    pub max_data_points: usize,
    /// Whether to enable detailed metrics collection
    pub enable_detailed_metrics: bool,
    /// Whether to enable performance profiling
    pub enable_profiling: bool,
    /// Whether to enable resource usage tracking
    pub enable_resource_tracking: bool,
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            collection_interval: Duration::from_secs(60), // 1 minute
            retention_period: Duration::from_secs(3600 * 24), // 24 hours
            max_data_points: 1440, // 24 hours worth of minute-level data
            enable_detailed_metrics: true,
            enable_profiling: false,
            enable_resource_tracking: true,
        }
    }
}

/// Scanner performance metrics
#[derive(Debug, Clone)]
pub struct ScannerMetrics {
    /// Objects scanned per second
    pub objects_per_second: f64,
    /// Bytes scanned per second
    pub bytes_per_second: f64,
    /// Average scan time per object
    pub avg_scan_time_per_object: Duration,
    /// Total objects scanned in current cycle
    pub total_objects_scanned: u64,
    /// Total bytes scanned in current cycle
    pub total_bytes_scanned: u64,
    /// Number of health issues detected
    pub health_issues_detected: u64,
    /// Scan success rate (percentage)
    pub success_rate: f64,
    /// Current scan cycle duration
    pub current_cycle_duration: Duration,
    /// Average scan cycle duration
    pub avg_cycle_duration: Duration,
    /// Last scan completion time
    pub last_scan_completion: Option<SystemTime>,
}

/// Resource usage metrics
#[derive(Debug, Clone)]
pub struct ResourceMetrics {
    /// CPU usage percentage
    pub cpu_usage_percent: f64,
    /// Memory usage in bytes
    pub memory_usage_bytes: u64,
    /// Memory usage percentage
    pub memory_usage_percent: f64,
    /// Disk I/O operations per second
    pub disk_io_ops_per_sec: f64,
    /// Disk I/O bytes per second
    pub disk_io_bytes_per_sec: f64,
    /// Network I/O bytes per second
    pub network_io_bytes_per_sec: f64,
    /// Number of active threads
    pub active_threads: u32,
    /// Number of open file descriptors
    pub open_file_descriptors: u32,
}

/// Health metrics summary
#[derive(Debug, Clone)]
pub struct HealthMetrics {
    /// Total health issues by severity
    pub issues_by_severity: HashMap<Severity, u64>,
    /// Total health issues by type
    pub issues_by_type: HashMap<HealthIssueType, u64>,
    /// Objects with health issues
    pub objects_with_issues: u64,
    /// Percentage of objects with issues
    pub objects_with_issues_percent: f64,
    /// Last health check time
    pub last_health_check: SystemTime,
    /// Health score (0-100, higher is better)
    pub health_score: f64,
}

/// Historical metrics data point
#[derive(Debug, Clone)]
pub struct MetricsDataPoint {
    pub timestamp: SystemTime,
    pub scanner_metrics: ScannerMetrics,
    pub resource_metrics: ResourceMetrics,
    pub health_metrics: HealthMetrics,
}

/// Metrics collector for scanner system
pub struct MetricsCollector {
    config: MetricsConfig,
    current_metrics: Arc<RwLock<CurrentMetrics>>,
    historical_data: Arc<RwLock<Vec<MetricsDataPoint>>>,
    collection_start_time: Instant,
}

/// Current metrics state
#[derive(Debug, Clone)]
pub struct CurrentMetrics {
    pub scanner_metrics: ScannerMetrics,
    pub resource_metrics: ResourceMetrics,
    pub health_metrics: HealthMetrics,
    pub last_update: SystemTime,
}

impl MetricsCollector {
    /// Create a new metrics collector
    pub fn new(config: MetricsConfig) -> Self {
        let collector = Self {
            config,
            current_metrics: Arc::new(RwLock::new(CurrentMetrics {
                scanner_metrics: ScannerMetrics {
                    objects_per_second: 0.0,
                    bytes_per_second: 0.0,
                    avg_scan_time_per_object: Duration::ZERO,
                    total_objects_scanned: 0,
                    total_bytes_scanned: 0,
                    health_issues_detected: 0,
                    success_rate: 100.0,
                    current_cycle_duration: Duration::ZERO,
                    avg_cycle_duration: Duration::ZERO,
                    last_scan_completion: None,
                },
                resource_metrics: ResourceMetrics {
                    cpu_usage_percent: 0.0,
                    memory_usage_bytes: 0,
                    memory_usage_percent: 0.0,
                    disk_io_ops_per_sec: 0.0,
                    disk_io_bytes_per_sec: 0.0,
                    network_io_bytes_per_sec: 0.0,
                    active_threads: 0,
                    open_file_descriptors: 0,
                },
                health_metrics: HealthMetrics {
                    issues_by_severity: HashMap::new(),
                    issues_by_type: HashMap::new(),
                    objects_with_issues: 0,
                    objects_with_issues_percent: 0.0,
                    last_health_check: SystemTime::now(),
                    health_score: 100.0,
                },
                last_update: SystemTime::now(),
            })),
            historical_data: Arc::new(RwLock::new(Vec::new())),
            collection_start_time: Instant::now(),
        };

        info!("Metrics collector created with config: {:?}", collector.config);
        collector
    }

    /// Start metrics collection
    pub async fn start_collection(&self) -> Result<()> {
        info!("Starting metrics collection");
        
        let collector = self.clone_for_background();
        tokio::spawn(async move {
            if let Err(e) = collector.run_collection_loop().await {
                error!("Metrics collection error: {}", e);
            }
        });

        Ok(())
    }

    /// Stop metrics collection
    pub async fn stop_collection(&self) -> Result<()> {
        info!("Stopping metrics collection");
        Ok(())
    }

    /// Update scanner metrics
    pub async fn update_scanner_metrics(&self, metrics: ScannerMetrics) -> Result<()> {
        let mut current = self.current_metrics.write().await;
        current.scanner_metrics = metrics;
        current.last_update = SystemTime::now();
        Ok(())
    }

    /// Update resource metrics
    pub async fn update_resource_metrics(&self, metrics: ResourceMetrics) -> Result<()> {
        let mut current = self.current_metrics.write().await;
        current.resource_metrics = metrics;
        current.last_update = SystemTime::now();
        Ok(())
    }

    /// Update health metrics
    pub async fn update_health_metrics(&self, metrics: HealthMetrics) -> Result<()> {
        let mut current = self.current_metrics.write().await;
        current.health_metrics = metrics;
        current.last_update = SystemTime::now();
        Ok(())
    }

    /// Record a health issue
    pub async fn record_health_issue(&self, issue: &HealthIssue) -> Result<()> {
        let mut current = self.current_metrics.write().await;
        
        // Update severity count
        *current.health_metrics.issues_by_severity.entry(issue.severity).or_insert(0) += 1;
        
        // Update type count
        *current.health_metrics.issues_by_type.entry(issue.issue_type.clone()).or_insert(0) += 1;
        
        // Update scanner metrics
        current.scanner_metrics.health_issues_detected += 1;
        
        current.last_update = SystemTime::now();
        Ok(())
    }

    /// Get current metrics
    pub async fn current_metrics(&self) -> CurrentMetrics {
        self.current_metrics.read().await.clone()
    }

    /// Get historical metrics
    pub async fn historical_metrics(&self, duration: Duration) -> Vec<MetricsDataPoint> {
        let historical = self.historical_data.read().await;
        let cutoff_time = SystemTime::now() - duration;
        
        historical.iter()
            .filter(|point| point.timestamp >= cutoff_time)
            .cloned()
            .collect()
    }

    /// Get metrics summary
    pub async fn metrics_summary(&self) -> MetricsSummary {
        let current = self.current_metrics.read().await;
        let historical = self.historical_data.read().await;
        
        let uptime = self.collection_start_time.elapsed();
        let total_data_points = historical.len();
        
        // Calculate averages from historical data
        let avg_objects_per_sec = if !historical.is_empty() {
            historical.iter()
                .map(|point| point.scanner_metrics.objects_per_second)
                .sum::<f64>() / historical.len() as f64
        } else {
            0.0
        };
        
        let avg_bytes_per_sec = if !historical.is_empty() {
            historical.iter()
                .map(|point| point.scanner_metrics.bytes_per_second)
                .sum::<f64>() / historical.len() as f64
        } else {
            0.0
        };
        
        let avg_cpu_usage = if !historical.is_empty() {
            historical.iter()
                .map(|point| point.resource_metrics.cpu_usage_percent)
                .sum::<f64>() / historical.len() as f64
        } else {
            0.0
        };
        
        let avg_memory_usage = if !historical.is_empty() {
            historical.iter()
                .map(|point| point.resource_metrics.memory_usage_percent)
                .sum::<f64>() / historical.len() as f64
        } else {
            0.0
        };
        
        MetricsSummary {
            uptime,
            total_data_points,
            current_scanner_metrics: current.scanner_metrics.clone(),
            current_resource_metrics: current.resource_metrics.clone(),
            current_health_metrics: current.health_metrics.clone(),
            avg_objects_per_sec,
            avg_bytes_per_sec,
            avg_cpu_usage,
            avg_memory_usage,
            last_update: current.last_update,
        }
    }

    /// Clone the collector for background tasks
    fn clone_for_background(&self) -> Arc<Self> {
        Arc::new(Self {
            config: self.config.clone(),
            current_metrics: self.current_metrics.clone(),
            historical_data: self.historical_data.clone(),
            collection_start_time: self.collection_start_time,
        })
    }

    /// Main collection loop
    async fn run_collection_loop(&self) -> Result<()> {
        info!("Metrics collection loop started");

        loop {
            // Collect current metrics
            self.collect_current_metrics().await?;
            
            // Store historical data point
            self.store_historical_data_point().await?;
            
            // Clean up old data
            self.cleanup_old_data().await?;
            
            // Wait for next collection interval
            tokio::time::sleep(self.config.collection_interval).await;
        }
    }

    /// Collect current system metrics
    async fn collect_current_metrics(&self) -> Result<()> {
        if self.config.enable_resource_tracking {
            let resource_metrics = self.collect_resource_metrics().await?;
            self.update_resource_metrics(resource_metrics).await?;
        }
        
        Ok(())
    }

    /// Collect resource usage metrics
    async fn collect_resource_metrics(&self) -> Result<ResourceMetrics> {
        // TODO: Implement actual resource metrics collection
        // For now, return placeholder metrics
        Ok(ResourceMetrics {
            cpu_usage_percent: 0.0,
            memory_usage_bytes: 0,
            memory_usage_percent: 0.0,
            disk_io_ops_per_sec: 0.0,
            disk_io_bytes_per_sec: 0.0,
            network_io_bytes_per_sec: 0.0,
            active_threads: 0,
            open_file_descriptors: 0,
        })
    }

    /// Store current metrics as historical data point
    async fn store_historical_data_point(&self) -> Result<()> {
        let current = self.current_metrics.read().await;
        let data_point = MetricsDataPoint {
            timestamp: SystemTime::now(),
            scanner_metrics: current.scanner_metrics.clone(),
            resource_metrics: current.resource_metrics.clone(),
            health_metrics: current.health_metrics.clone(),
        };
        
        let mut historical = self.historical_data.write().await;
        historical.push(data_point);
        
        // Limit the number of data points
        if historical.len() > self.config.max_data_points {
            historical.remove(0);
        }
        
        Ok(())
    }

    /// Clean up old historical data
    async fn cleanup_old_data(&self) -> Result<()> {
        let cutoff_time = SystemTime::now() - self.config.retention_period;
        let mut historical = self.historical_data.write().await;
        
        historical.retain(|point| point.timestamp >= cutoff_time);
        
        Ok(())
    }

    /// Reset all metrics
    pub async fn reset_metrics(&self) -> Result<()> {
        let mut current = self.current_metrics.write().await;
        *current = CurrentMetrics {
            scanner_metrics: ScannerMetrics {
                objects_per_second: 0.0,
                bytes_per_second: 0.0,
                avg_scan_time_per_object: Duration::ZERO,
                total_objects_scanned: 0,
                total_bytes_scanned: 0,
                health_issues_detected: 0,
                success_rate: 100.0,
                current_cycle_duration: Duration::ZERO,
                avg_cycle_duration: Duration::ZERO,
                last_scan_completion: None,
            },
            resource_metrics: ResourceMetrics {
                cpu_usage_percent: 0.0,
                memory_usage_bytes: 0,
                memory_usage_percent: 0.0,
                disk_io_ops_per_sec: 0.0,
                disk_io_bytes_per_sec: 0.0,
                network_io_bytes_per_sec: 0.0,
                active_threads: 0,
                open_file_descriptors: 0,
            },
            health_metrics: HealthMetrics {
                issues_by_severity: HashMap::new(),
                issues_by_type: HashMap::new(),
                objects_with_issues: 0,
                objects_with_issues_percent: 0.0,
                last_health_check: SystemTime::now(),
                health_score: 100.0,
            },
            last_update: SystemTime::now(),
        };
        
        let mut historical = self.historical_data.write().await;
        historical.clear();
        
        Ok(())
    }
}

/// Summary of all metrics
#[derive(Debug, Clone)]
pub struct MetricsSummary {
    pub uptime: Duration,
    pub total_data_points: usize,
    pub current_scanner_metrics: ScannerMetrics,
    pub current_resource_metrics: ResourceMetrics,
    pub current_health_metrics: HealthMetrics,
    pub avg_objects_per_sec: f64,
    pub avg_bytes_per_sec: f64,
    pub avg_cpu_usage: f64,
    pub avg_memory_usage: f64,
    pub last_update: SystemTime,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_metrics_collector_creation() {
        let config = MetricsConfig::default();
        let collector = MetricsCollector::new(config);
        let metrics = collector.current_metrics().await;
        assert_eq!(metrics.scanner_metrics.total_objects_scanned, 0);
    }

    #[tokio::test]
    async fn test_metrics_update() {
        let config = MetricsConfig::default();
        let collector = MetricsCollector::new(config);
        
        let scanner_metrics = ScannerMetrics {
            objects_per_second: 100.0,
            bytes_per_second: 1024.0,
            avg_scan_time_per_object: Duration::from_millis(10),
            total_objects_scanned: 1000,
            total_bytes_scanned: 1024000,
            health_issues_detected: 5,
            success_rate: 99.5,
            current_cycle_duration: Duration::from_secs(60),
            avg_cycle_duration: Duration::from_secs(65),
            last_scan_completion: Some(SystemTime::now()),
        };
        
        collector.update_scanner_metrics(scanner_metrics).await.unwrap();
        
        let current = collector.current_metrics().await;
        assert_eq!(current.scanner_metrics.total_objects_scanned, 1000);
        assert_eq!(current.scanner_metrics.health_issues_detected, 5);
    }

    #[tokio::test]
    async fn test_health_issue_recording() {
        let config = MetricsConfig::default();
        let collector = MetricsCollector::new(config);
        
        let issue = HealthIssue {
            issue_type: HealthIssueType::DiskFull,
            severity: Severity::High,
            bucket: "test-bucket".to_string(),
            object: "test-object".to_string(),
            description: "Test issue".to_string(),
            metadata: None,
        };
        
        collector.record_health_issue(&issue).await.unwrap();
        
        let current = collector.current_metrics().await;
        assert_eq!(current.scanner_metrics.health_issues_detected, 1);
        assert_eq!(current.health_metrics.issues_by_severity.get(&Severity::High), Some(&1));
    }
} 