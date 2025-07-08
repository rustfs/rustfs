// Copyright 2024 RustFS Team

use std::{
    sync::Arc,
    time::{Duration, Instant, SystemTime},
};

use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

use crate::{
    error::Result,
    scanner::{HealthIssue, Severity},
};

use super::{
    AggregatedMetrics, Aggregator, DiskMetrics, HealMetrics, MetricsQuery, MetricType,
    NetworkMetrics, PolicyMetrics, ScanMetrics, SystemMetrics,
};

/// Configuration for the metrics collector
#[derive(Debug, Clone)]
pub struct CollectorConfig {
    /// Collection interval
    pub collection_interval: Duration,
    /// Whether to enable detailed metrics collection
    pub enable_detailed_metrics: bool,
    /// Maximum number of metrics to keep in memory
    pub max_metrics_in_memory: usize,
    /// Whether to enable automatic aggregation
    pub enable_auto_aggregation: bool,
    /// Aggregation interval
    pub aggregation_interval: Duration,
    /// Whether to enable resource monitoring
    pub enable_resource_monitoring: bool,
    /// Resource monitoring interval
    pub resource_monitoring_interval: Duration,
    /// Whether to enable health issue tracking
    pub enable_health_issue_tracking: bool,
    /// Metrics retention period
    pub metrics_retention_period: Duration,
}

impl Default for CollectorConfig {
    fn default() -> Self {
        Self {
            collection_interval: Duration::from_secs(30), // 30 seconds
            enable_detailed_metrics: true,
            max_metrics_in_memory: 10000,
            enable_auto_aggregation: true,
            aggregation_interval: Duration::from_secs(300), // 5 minutes
            enable_resource_monitoring: true,
            resource_monitoring_interval: Duration::from_secs(10), // 10 seconds
            enable_health_issue_tracking: true,
            metrics_retention_period: Duration::from_secs(86400 * 7), // 7 days
        }
    }
}

/// Metrics collector that gathers system metrics
#[derive(Debug)]
pub struct Collector {
    config: CollectorConfig,
    metrics: Arc<RwLock<Vec<SystemMetrics>>>,
    aggregator: Arc<Aggregator>,
    last_collection_time: Arc<RwLock<SystemTime>>,
    collection_count: Arc<RwLock<u64>>,
    health_issues: Arc<RwLock<std::collections::HashMap<Severity, u64>>>,
}

impl Collector {
    /// Create a new metrics collector
    pub async fn new(config: CollectorConfig) -> Result<Self> {
        let aggregator = Arc::new(Aggregator::new(config.aggregation_interval).await?);
        
        Ok(Self {
            config,
            metrics: Arc::new(RwLock::new(Vec::new())),
            aggregator,
            last_collection_time: Arc::new(RwLock::new(SystemTime::now())),
            collection_count: Arc::new(RwLock::new(0)),
            health_issues: Arc::new(RwLock::new(std::collections::HashMap::new())),
        })
    }

    /// Get the configuration
    pub fn config(&self) -> &CollectorConfig {
        &self.config
    }

    /// Collect current system metrics
    pub async fn collect_metrics(&self) -> Result<SystemMetrics> {
        let start_time = Instant::now();
        
        let mut metrics = SystemMetrics::default();
        metrics.timestamp = SystemTime::now();

        // Collect system resource metrics
        if self.config.enable_resource_monitoring {
            self.collect_system_resources(&mut metrics).await?;
        }

        // Collect scan metrics
        self.collect_scan_metrics(&mut metrics).await?;

        // Collect heal metrics
        self.collect_heal_metrics(&mut metrics).await?;

        // Collect policy metrics
        self.collect_policy_metrics(&mut metrics).await?;

        // Collect health issues
        if self.config.enable_health_issue_tracking {
            self.collect_health_issues(&mut metrics).await?;
        }

        // Store metrics
        {
            let mut metrics_store = self.metrics.write().await;
            metrics_store.push(metrics.clone());
            
            // Trim old metrics if we exceed the limit
            if metrics_store.len() > self.config.max_metrics_in_memory {
                let excess = metrics_store.len() - self.config.max_metrics_in_memory;
                metrics_store.drain(0..excess);
            }
        }

        // Update collection statistics
        {
            let mut last_time = self.last_collection_time.write().await;
            *last_time = metrics.timestamp;
            
            let mut count = self.collection_count.write().await;
            *count += 1;
        }

        let collection_time = start_time.elapsed();
        debug!("Metrics collection completed in {:?}", collection_time);

        Ok(metrics)
    }

    /// Collect system resource metrics
    async fn collect_system_resources(&self, metrics: &mut SystemMetrics) -> Result<()> {
        // Simulate system resource collection
        // In a real implementation, this would use system APIs
        
        metrics.cpu_usage = self.get_cpu_usage().await?;
        metrics.memory_usage = self.get_memory_usage().await?;
        metrics.disk_usage = self.get_disk_usage().await?;
        metrics.system_load = self.get_system_load().await?;
        metrics.active_operations = self.get_active_operations().await?;

        // Collect network metrics
        metrics.network_io = self.get_network_metrics().await?;

        // Collect disk I/O metrics
        metrics.disk_io = self.get_disk_io_metrics().await?;

        Ok(())
    }

    /// Collect scan metrics
    async fn collect_scan_metrics(&self, metrics: &mut SystemMetrics) -> Result<()> {
        // In a real implementation, this would get data from the scanner
        metrics.scan_metrics = ScanMetrics::default();
        
        // Simulate some scan metrics
        metrics.scan_metrics.objects_scanned = 1000;
        metrics.scan_metrics.bytes_scanned = 1024 * 1024 * 100; // 100 MB
        metrics.scan_metrics.scan_duration = Duration::from_secs(60);
        metrics.scan_metrics.scan_rate_objects_per_sec = 16.67; // 1000 / 60
        metrics.scan_metrics.scan_rate_bytes_per_sec = 1_747_200.0; // 100MB / 60s
        metrics.scan_metrics.health_issues_found = 5;
        metrics.scan_metrics.scan_cycles_completed = 1;
        metrics.scan_metrics.last_scan_time = Some(SystemTime::now());

        Ok(())
    }

    /// Collect heal metrics
    async fn collect_heal_metrics(&self, metrics: &mut SystemMetrics) -> Result<()> {
        // In a real implementation, this would get data from the heal system
        metrics.heal_metrics = HealMetrics::default();
        
        // Simulate some heal metrics
        metrics.heal_metrics.total_repairs = 10;
        metrics.heal_metrics.successful_repairs = 8;
        metrics.heal_metrics.failed_repairs = 2;
        metrics.heal_metrics.total_repair_time = Duration::from_secs(300);
        metrics.heal_metrics.average_repair_time = Duration::from_secs(30);
        metrics.heal_metrics.active_repair_workers = 2;
        metrics.heal_metrics.queued_repair_tasks = 5;
        metrics.heal_metrics.last_repair_time = Some(SystemTime::now());
        metrics.heal_metrics.total_retry_attempts = 3;

        Ok(())
    }

    /// Collect policy metrics
    async fn collect_policy_metrics(&self, metrics: &mut SystemMetrics) -> Result<()> {
        // In a real implementation, this would get data from the policy system
        metrics.policy_metrics = PolicyMetrics::default();
        
        // Simulate some policy metrics
        metrics.policy_metrics.total_evaluations = 50;
        metrics.policy_metrics.allowed_operations = 45;
        metrics.policy_metrics.denied_operations = 5;
        metrics.policy_metrics.scan_policy_evaluations = 20;
        metrics.policy_metrics.heal_policy_evaluations = 20;
        metrics.policy_metrics.retention_policy_evaluations = 10;
        metrics.policy_metrics.average_evaluation_time = Duration::from_millis(10);

        Ok(())
    }

    /// Collect health issues
    async fn collect_health_issues(&self, metrics: &mut SystemMetrics) -> Result<()> {
        let health_issues = self.health_issues.read().await;
        metrics.health_issues = health_issues.clone();
        Ok(())
    }

    /// Record a health issue
    pub async fn record_health_issue(&self, issue: &HealthIssue) -> Result<()> {
        let mut issues = self.health_issues.write().await;
        let count = issues.entry(issue.severity).or_insert(0);
        *count += 1;
        
        info!("Recorded health issue: {:?} - {}", issue.severity, issue.description);
        Ok(())
    }

    /// Record an event (alias for record_health_issue)
    pub async fn record_event(&self, issue: &HealthIssue) -> Result<()> {
        self.record_health_issue(issue).await
    }

    /// Clear health issues
    pub async fn clear_health_issues(&self) -> Result<()> {
        let mut health_issues = self.health_issues.write().await;
        health_issues.clear();
        
        info!("Cleared all health issues");
        Ok(())
    }

    /// Query metrics with aggregation
    pub async fn query_metrics(&self, query: MetricsQuery) -> Result<AggregatedMetrics> {
        // In a real implementation, this would query the aggregator
        // For now, we'll return a simple aggregated result
        let aggregator = self.aggregator.as_ref();
        let mut aggregator_guard = aggregator.write().await;
        aggregator_guard.aggregate_metrics(query).await
    }

    /// Get metrics for a specific time range
    pub async fn get_metrics_range(&self, start_time: SystemTime, end_time: SystemTime) -> Result<Vec<SystemMetrics>> {
        let metrics = self.metrics.read().await;
        let filtered_metrics: Vec<SystemMetrics> = metrics
            .iter()
            .filter(|m| m.timestamp >= start_time && m.timestamp <= end_time)
            .cloned()
            .collect();
        
        Ok(filtered_metrics)
    }

    /// Get latest metrics
    pub async fn get_latest_metrics(&self) -> Result<Option<SystemMetrics>> {
        let metrics = self.metrics.read().await;
        Ok(metrics.last().cloned())
    }

    /// Get collection statistics
    pub async fn get_collection_statistics(&self) -> CollectionStatistics {
        let collection_count = *self.collection_count.read().await;
        let last_collection_time = *self.last_collection_time.read().await;
        let metrics_count = self.metrics.read().await.len();
        
        CollectionStatistics {
            total_collections: collection_count,
            last_collection_time,
            metrics_in_memory: metrics_count,
            config: self.config.clone(),
        }
    }

    /// Simulated system resource collection methods
    async fn get_cpu_usage(&self) -> Result<f64> {
        // Simulate CPU usage collection
        Ok(25.5) // 25.5%
    }

    async fn get_memory_usage(&self) -> Result<f64> {
        // Simulate memory usage collection
        Ok(60.2) // 60.2%
    }

    async fn get_disk_usage(&self) -> Result<f64> {
        // Simulate disk usage collection
        Ok(45.8) // 45.8%
    }

    async fn get_system_load(&self) -> Result<f64> {
        // Simulate system load collection
        Ok(0.75) // 0.75
    }

    async fn get_active_operations(&self) -> Result<u64> {
        // Simulate active operations count
        Ok(15)
    }

    async fn get_network_metrics(&self) -> Result<NetworkMetrics> {
        // Simulate network metrics collection
        Ok(NetworkMetrics {
            bytes_received_per_sec: 1024 * 1024, // 1 MB/s
            bytes_sent_per_sec: 512 * 1024,      // 512 KB/s
            packets_received_per_sec: 1000,
            packets_sent_per_sec: 500,
        })
    }

    async fn get_disk_io_metrics(&self) -> Result<DiskMetrics> {
        // Simulate disk I/O metrics collection
        Ok(DiskMetrics {
            bytes_read_per_sec: 2 * 1024 * 1024,  // 2 MB/s
            bytes_written_per_sec: 1 * 1024 * 1024, // 1 MB/s
            read_ops_per_sec: 200,
            write_ops_per_sec: 100,
            avg_read_latency_ms: 5.0,
            avg_write_latency_ms: 8.0,
        })
    }
}

/// Collection statistics
#[derive(Debug, Clone)]
pub struct CollectionStatistics {
    pub total_collections: u64,
    pub last_collection_time: SystemTime,
    pub metrics_in_memory: usize,
    pub config: CollectorConfig,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scanner::{HealthIssue, HealthIssueType};

    #[tokio::test]
    async fn test_collector_creation() {
        let config = CollectorConfig::default();
        let collector = Collector::new(config).await.unwrap();
        
        assert_eq!(collector.config().collection_interval, Duration::from_secs(30));
        assert!(collector.config().enable_detailed_metrics);
    }

    #[tokio::test]
    async fn test_metrics_collection() {
        let config = CollectorConfig::default();
        let collector = Collector::new(config).await.unwrap();
        
        let metrics = collector.collect_metrics().await.unwrap();
        assert_eq!(metrics.cpu_usage, 25.5);
        assert_eq!(metrics.memory_usage, 60.2);
        assert_eq!(metrics.disk_usage, 45.8);
    }

    #[tokio::test]
    async fn test_health_issue_recording() {
        let config = CollectorConfig::default();
        let collector = Collector::new(config).await.unwrap();
        
        let issue = HealthIssue {
            issue_type: HealthIssueType::MissingReplica,
            severity: Severity::Critical,
            bucket: "test-bucket".to_string(),
            object: "test-object".to_string(),
            description: "Test issue".to_string(),
            metadata: None,
        };
        
        collector.record_health_issue(&issue).await.unwrap();
        
        let stats = collector.get_collection_statistics().await;
        assert_eq!(stats.total_collections, 0); // No collection yet
    }

    #[tokio::test]
    async fn test_latest_metrics() {
        let config = CollectorConfig::default();
        let collector = Collector::new(config).await.unwrap();
        
        // Initially no metrics
        let latest = collector.get_latest_metrics().await.unwrap();
        assert!(latest.is_none());
        
        // Collect metrics
        collector.collect_metrics().await.unwrap();
        
        // Now should have metrics
        let latest = collector.get_latest_metrics().await.unwrap();
        assert!(latest.is_some());
    }

    #[tokio::test]
    async fn test_collection_statistics() {
        let config = CollectorConfig::default();
        let collector = Collector::new(config).await.unwrap();
        
        let stats = collector.get_collection_statistics().await;
        assert_eq!(stats.total_collections, 0);
        assert_eq!(stats.metrics_in_memory, 0);
        
        // Collect metrics
        collector.collect_metrics().await.unwrap();
        
        let stats = collector.get_collection_statistics().await;
        assert_eq!(stats.total_collections, 1);
        assert_eq!(stats.metrics_in_memory, 1);
    }
} 