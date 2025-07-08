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

//! Metrics collection and aggregation system
//!
//! The metrics subsystem provides comprehensive data collection and analysis:
//! - Real-time metrics collection from all subsystems
//! - Time-series data storage and aggregation
//! - Export capabilities for external monitoring systems
//! - Performance analytics and trend analysis

pub mod collector;
pub mod aggregator;
pub mod storage;
pub mod reporter;

pub use collector::{Collector, CollectorConfig};
pub use aggregator::{Aggregator, AggregatorConfig};
pub use storage::{Storage, StorageConfig};
pub use reporter::{Reporter, ReporterConfig};

use std::time::{Duration, SystemTime};
use std::collections::HashMap;
use serde::{Deserialize, Serialize};

use crate::scanner::{HealthIssue, Severity};

/// Metrics subsystem status
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Status {
    /// Metrics system is initializing
    Initializing,
    /// Metrics system is running normally
    Running,
    /// Metrics system is degraded (some exporters failing)
    Degraded,
    /// Metrics system is stopping
    Stopping,
    /// Metrics system has stopped
    Stopped,
    /// Metrics system encountered an error
    Error(String),
}

/// Metric data point with timestamp and value
#[derive(Debug, Clone)]
pub struct MetricPoint {
    /// Metric name
    pub name: String,
    /// Metric value
    pub value: MetricValue,
    /// Timestamp when metric was collected
    pub timestamp: SystemTime,
    /// Additional labels/tags
    pub labels: HashMap<String, String>,
}

/// Different types of metric values
#[derive(Debug, Clone)]
pub enum MetricValue {
    /// Counter that only increases
    Counter(u64),
    /// Gauge that can go up or down
    Gauge(f64),
    /// Histogram with buckets
    Histogram {
        count: u64,
        sum: f64,
        buckets: Vec<HistogramBucket>,
    },
    /// Summary with quantiles
    Summary {
        count: u64,
        sum: f64,
        quantiles: Vec<Quantile>,
    },
}

/// Histogram bucket
#[derive(Debug, Clone)]
pub struct HistogramBucket {
    /// Upper bound of the bucket
    pub le: f64,
    /// Count of observations in this bucket
    pub count: u64,
}

/// Summary quantile
#[derive(Debug, Clone)]
pub struct Quantile {
    /// Quantile value (e.g., 0.5 for median)
    pub quantile: f64,
    /// Value at this quantile
    pub value: f64,
}

/// Aggregation functions for metrics
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AggregationFunction {
    Sum,
    Average,
    Min,
    Max,
    Count,
    Rate,
    Percentile(u8),
}

/// Time window for aggregation
#[derive(Debug, Clone)]
pub struct TimeWindow {
    /// Duration of the window
    pub duration: Duration,
    /// How often to create new windows
    pub step: Duration,
}

/// Metric export configuration
#[derive(Debug, Clone)]
pub struct ExportConfig {
    /// Export format
    pub format: ExportFormat,
    /// Export destination
    pub destination: ExportDestination,
    /// Export interval
    pub interval: Duration,
    /// Metric filters
    pub filters: Vec<MetricFilter>,
}

/// Supported export formats
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExportFormat {
    /// Prometheus format
    Prometheus,
    /// JSON format
    Json,
    /// CSV format
    Csv,
    /// Custom format
    Custom(String),
}

/// Export destinations
#[derive(Debug, Clone)]
pub enum ExportDestination {
    /// HTTP endpoint
    Http { url: String, headers: HashMap<String, String> },
    /// File system
    File { path: String },
    /// Standard output
    Stdout,
    /// Custom destination
    Custom(String),
}

/// Metric filtering rules
#[derive(Debug, Clone)]
pub struct MetricFilter {
    /// Metric name pattern (regex)
    pub name_pattern: String,
    /// Label filters
    pub label_filters: HashMap<String, String>,
    /// Include or exclude matching metrics
    pub include: bool,
}

/// System-wide metrics that are automatically collected
pub mod system_metrics {
    /// Object-related metrics
    pub const OBJECTS_TOTAL: &str = "rustfs_objects_total";
    pub const OBJECTS_SIZE_BYTES: &str = "rustfs_objects_size_bytes";
    pub const OBJECTS_SCANNED_TOTAL: &str = "rustfs_objects_scanned_total";
    pub const OBJECTS_HEAL_OPERATIONS_TOTAL: &str = "rustfs_objects_heal_operations_total";
    
    /// Scanner metrics
    pub const SCAN_CYCLES_TOTAL: &str = "rustfs_scan_cycles_total";
    pub const SCAN_DURATION_SECONDS: &str = "rustfs_scan_duration_seconds";
    pub const SCAN_RATE_OBJECTS_PER_SECOND: &str = "rustfs_scan_rate_objects_per_second";
    pub const SCAN_RATE_BYTES_PER_SECOND: &str = "rustfs_scan_rate_bytes_per_second";
    
    /// Health metrics
    pub const HEALTH_ISSUES_TOTAL: &str = "rustfs_health_issues_total";
    pub const HEALTH_ISSUES_BY_SEVERITY: &str = "rustfs_health_issues_by_severity";
    pub const HEAL_SUCCESS_RATE: &str = "rustfs_heal_success_rate";
    
    /// System resource metrics
    pub const DISK_USAGE_BYTES: &str = "rustfs_disk_usage_bytes";
    pub const DISK_IOPS: &str = "rustfs_disk_iops";
    pub const MEMORY_USAGE_BYTES: &str = "rustfs_memory_usage_bytes";
    pub const CPU_USAGE_PERCENT: &str = "rustfs_cpu_usage_percent";
    
    /// Performance metrics
    pub const OPERATION_DURATION_SECONDS: &str = "rustfs_operation_duration_seconds";
    pub const ACTIVE_OPERATIONS: &str = "rustfs_active_operations";
    pub const THROUGHPUT_BYTES_PER_SECOND: &str = "rustfs_throughput_bytes_per_second";
}

/// System metrics collected by AHM
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemMetrics {
    /// Timestamp when metrics were collected
    pub timestamp: SystemTime,
    /// CPU usage percentage
    pub cpu_usage: f64,
    /// Memory usage percentage
    pub memory_usage: f64,
    /// Disk usage percentage
    pub disk_usage: f64,
    /// Network I/O bytes per second
    pub network_io: NetworkMetrics,
    /// Disk I/O bytes per second
    pub disk_io: DiskMetrics,
    /// Active operations count
    pub active_operations: u64,
    /// System load average
    pub system_load: f64,
    /// Health issues count by severity
    pub health_issues: std::collections::HashMap<Severity, u64>,
    /// Scan metrics
    pub scan_metrics: ScanMetrics,
    /// Heal metrics
    pub heal_metrics: HealMetrics,
    /// Policy metrics
    pub policy_metrics: PolicyMetrics,
}

/// Network I/O metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkMetrics {
    /// Bytes received per second
    pub bytes_received_per_sec: u64,
    /// Bytes sent per second
    pub bytes_sent_per_sec: u64,
    /// Packets received per second
    pub packets_received_per_sec: u64,
    /// Packets sent per second
    pub packets_sent_per_sec: u64,
}

/// Disk I/O metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiskMetrics {
    /// Bytes read per second
    pub bytes_read_per_sec: u64,
    /// Bytes written per second
    pub bytes_written_per_sec: u64,
    /// Read operations per second
    pub read_ops_per_sec: u64,
    /// Write operations per second
    pub write_ops_per_sec: u64,
    /// Average read latency in milliseconds
    pub avg_read_latency_ms: f64,
    /// Average write latency in milliseconds
    pub avg_write_latency_ms: f64,
}

/// Scan operation metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScanMetrics {
    /// Total objects scanned
    pub objects_scanned: u64,
    /// Total bytes scanned
    pub bytes_scanned: u64,
    /// Scan duration
    pub scan_duration: Duration,
    /// Scan rate (objects per second)
    pub scan_rate_objects_per_sec: f64,
    /// Scan rate (bytes per second)
    pub scan_rate_bytes_per_sec: f64,
    /// Health issues found
    pub health_issues_found: u64,
    /// Scan cycles completed
    pub scan_cycles_completed: u64,
    /// Last scan time
    pub last_scan_time: Option<SystemTime>,
}

/// Heal operation metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealMetrics {
    /// Total repair operations
    pub total_repairs: u64,
    /// Successful repairs
    pub successful_repairs: u64,
    /// Failed repairs
    pub failed_repairs: u64,
    /// Total repair time
    pub total_repair_time: Duration,
    /// Average repair time
    pub average_repair_time: Duration,
    /// Active repair workers
    pub active_repair_workers: u64,
    /// Queued repair tasks
    pub queued_repair_tasks: u64,
    /// Last repair time
    pub last_repair_time: Option<SystemTime>,
    /// Retry attempts
    pub total_retry_attempts: u64,
}

/// Policy evaluation metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolicyMetrics {
    /// Total policy evaluations
    pub total_evaluations: u64,
    /// Allowed operations
    pub allowed_operations: u64,
    /// Denied operations
    pub denied_operations: u64,
    /// Scan policy evaluations
    pub scan_policy_evaluations: u64,
    /// Heal policy evaluations
    pub heal_policy_evaluations: u64,
    /// Retention policy evaluations
    pub retention_policy_evaluations: u64,
    /// Average evaluation time
    pub average_evaluation_time: Duration,
}

impl Default for SystemMetrics {
    fn default() -> Self {
        Self {
            timestamp: SystemTime::now(),
            cpu_usage: 0.0,
            memory_usage: 0.0,
            disk_usage: 0.0,
            network_io: NetworkMetrics::default(),
            disk_io: DiskMetrics::default(),
            active_operations: 0,
            system_load: 0.0,
            health_issues: std::collections::HashMap::new(),
            scan_metrics: ScanMetrics::default(),
            heal_metrics: HealMetrics::default(),
            policy_metrics: PolicyMetrics::default(),
        }
    }
}

impl Default for NetworkMetrics {
    fn default() -> Self {
        Self {
            bytes_received_per_sec: 0,
            bytes_sent_per_sec: 0,
            packets_received_per_sec: 0,
            packets_sent_per_sec: 0,
        }
    }
}

impl Default for DiskMetrics {
    fn default() -> Self {
        Self {
            bytes_read_per_sec: 0,
            bytes_written_per_sec: 0,
            read_ops_per_sec: 0,
            write_ops_per_sec: 0,
            avg_read_latency_ms: 0.0,
            avg_write_latency_ms: 0.0,
        }
    }
}

impl Default for ScanMetrics {
    fn default() -> Self {
        Self {
            objects_scanned: 0,
            bytes_scanned: 0,
            scan_duration: Duration::ZERO,
            scan_rate_objects_per_sec: 0.0,
            scan_rate_bytes_per_sec: 0.0,
            health_issues_found: 0,
            scan_cycles_completed: 0,
            last_scan_time: None,
        }
    }
}

impl Default for HealMetrics {
    fn default() -> Self {
        Self {
            total_repairs: 0,
            successful_repairs: 0,
            failed_repairs: 0,
            total_repair_time: Duration::ZERO,
            average_repair_time: Duration::ZERO,
            active_repair_workers: 0,
            queued_repair_tasks: 0,
            last_repair_time: None,
            total_retry_attempts: 0,
        }
    }
}

impl Default for PolicyMetrics {
    fn default() -> Self {
        Self {
            total_evaluations: 0,
            allowed_operations: 0,
            denied_operations: 0,
            scan_policy_evaluations: 0,
            heal_policy_evaluations: 0,
            retention_policy_evaluations: 0,
            average_evaluation_time: Duration::ZERO,
        }
    }
}

/// Metrics query parameters
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsQuery {
    /// Start time for the query
    pub start_time: SystemTime,
    /// End time for the query
    pub end_time: SystemTime,
    /// Metrics aggregation interval
    pub interval: Duration,
    /// Metrics to include in the query
    pub metrics: Vec<MetricType>,
    /// Filter by severity
    pub severity_filter: Option<Severity>,
    /// Limit number of results
    pub limit: Option<u64>,
}

/// Types of metrics that can be queried
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MetricType {
    /// System metrics (CPU, memory, disk)
    System,
    /// Network metrics
    Network,
    /// Disk I/O metrics
    DiskIo,
    /// Scan metrics
    Scan,
    /// Heal metrics
    Heal,
    /// Policy metrics
    Policy,
    /// Health issues
    HealthIssues,
}

/// Aggregated metrics data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggregatedMetrics {
    /// Query parameters used
    pub query: MetricsQuery,
    /// Aggregated data points
    pub data_points: Vec<MetricsDataPoint>,
    /// Summary statistics
    pub summary: MetricsSummary,
}

/// Individual metrics data point
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsDataPoint {
    /// Timestamp for this data point
    pub timestamp: SystemTime,
    /// System metrics
    pub system: Option<SystemMetrics>,
    /// Network metrics
    pub network: Option<NetworkMetrics>,
    /// Disk I/O metrics
    pub disk_io: Option<DiskMetrics>,
    /// Scan metrics
    pub scan: Option<ScanMetrics>,
    /// Heal metrics
    pub heal: Option<HealMetrics>,
    /// Policy metrics
    pub policy: Option<PolicyMetrics>,
}

/// Summary statistics for aggregated metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsSummary {
    /// Total data points
    pub total_points: u64,
    /// Time range covered
    pub time_range: Duration,
    /// Average CPU usage
    pub avg_cpu_usage: f64,
    /// Average memory usage
    pub avg_memory_usage: f64,
    /// Average disk usage
    pub avg_disk_usage: f64,
    /// Total objects scanned
    pub total_objects_scanned: u64,
    /// Total repairs performed
    pub total_repairs: u64,
    /// Success rate for repairs
    pub repair_success_rate: f64,
    /// Total health issues
    pub total_health_issues: u64,
}

/// Resource usage information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceUsage {
    /// Disk usage information
    pub disk_usage: DiskUsage,
    /// Memory usage information
    pub memory_usage: MemoryUsage,
    /// Network usage information
    pub network_usage: NetworkUsage,
    /// CPU usage information
    pub cpu_usage: CpuUsage,
}

/// Disk usage information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiskUsage {
    /// Total disk space in bytes
    pub total_bytes: u64,
    /// Used disk space in bytes
    pub used_bytes: u64,
    /// Available disk space in bytes
    pub available_bytes: u64,
    /// Usage percentage
    pub usage_percentage: f64,
}

/// Memory usage information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryUsage {
    /// Total memory in bytes
    pub total_bytes: u64,
    /// Used memory in bytes
    pub used_bytes: u64,
    /// Available memory in bytes
    pub available_bytes: u64,
    /// Usage percentage
    pub usage_percentage: f64,
}

/// Network usage information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkUsage {
    /// Bytes received
    pub bytes_received: u64,
    /// Bytes sent
    pub bytes_sent: u64,
    /// Packets received
    pub packets_received: u64,
    /// Packets sent
    pub packets_sent: u64,
}

/// CPU usage information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CpuUsage {
    /// CPU usage percentage
    pub usage_percentage: f64,
    /// Number of CPU cores
    pub cores: u32,
    /// Load average
    pub load_average: f64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_system_metrics_creation() {
        let metrics = SystemMetrics::default();
        assert_eq!(metrics.cpu_usage, 0.0);
        assert_eq!(metrics.memory_usage, 0.0);
        assert_eq!(metrics.active_operations, 0);
    }

    #[test]
    fn test_scan_metrics_creation() {
        let metrics = ScanMetrics::default();
        assert_eq!(metrics.objects_scanned, 0);
        assert_eq!(metrics.bytes_scanned, 0);
        assert_eq!(metrics.scan_cycles_completed, 0);
    }

    #[test]
    fn test_heal_metrics_creation() {
        let metrics = HealMetrics::default();
        assert_eq!(metrics.total_repairs, 0);
        assert_eq!(metrics.successful_repairs, 0);
        assert_eq!(metrics.failed_repairs, 0);
    }

    #[test]
    fn test_metrics_query_creation() {
        let start_time = SystemTime::now();
        let end_time = start_time + Duration::from_secs(3600);
        let query = MetricsQuery {
            start_time,
            end_time,
            interval: Duration::from_secs(60),
            metrics: vec![MetricType::System, MetricType::Scan],
            severity_filter: Some(Severity::Critical),
            limit: Some(100),
        };

        assert_eq!(query.metrics.len(), 2);
        assert_eq!(query.interval, Duration::from_secs(60));
        assert_eq!(query.limit, Some(100));
    }
} 