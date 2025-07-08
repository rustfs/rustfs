// Copyright 2024 RustFS Team

use std::{
    collections::HashMap,
    time::{Duration, SystemTime},
};

use tracing::{debug, error, info, warn};

use crate::error::Result;

use super::{
    AggregatedMetrics, DiskMetrics, HealMetrics, MetricsDataPoint, MetricsQuery, MetricsSummary,
    NetworkMetrics, PolicyMetrics, ScanMetrics, SystemMetrics,
};

/// Configuration for the metrics aggregator
#[derive(Debug, Clone)]
pub struct AggregatorConfig {
    /// Default aggregation interval
    pub default_interval: Duration,
    /// Maximum number of data points to keep in memory
    pub max_data_points: usize,
    /// Whether to enable automatic aggregation
    pub enable_auto_aggregation: bool,
    /// Aggregation window size
    pub aggregation_window: Duration,
    /// Whether to enable data compression
    pub enable_compression: bool,
    /// Compression threshold (number of points before compression)
    pub compression_threshold: usize,
    /// Whether to enable outlier detection
    pub enable_outlier_detection: bool,
    /// Outlier detection threshold (standard deviations)
    pub outlier_threshold: f64,
}

impl Default for AggregatorConfig {
    fn default() -> Self {
        Self {
            default_interval: Duration::from_secs(300), // 5 minutes
            max_data_points: 10000,
            enable_auto_aggregation: true,
            aggregation_window: Duration::from_secs(3600), // 1 hour
            enable_compression: true,
            compression_threshold: 1000,
            enable_outlier_detection: true,
            outlier_threshold: 2.0, // 2 standard deviations
        }
    }
}

/// Metrics aggregator that processes and aggregates metrics data
#[derive(Debug, Clone)]
pub struct Aggregator {
    config: AggregatorConfig,
    data_points: Vec<MetricsDataPoint>,
    aggregation_cache: HashMap<String, AggregatedMetrics>,
    last_aggregation_time: SystemTime,
    aggregation_count: u64,
}

impl Aggregator {
    /// Create a new metrics aggregator
    pub async fn new(interval: Duration) -> Result<Self> {
        let config = AggregatorConfig {
            default_interval: interval,
            ..Default::default()
        };

        Ok(Self {
            config,
            data_points: Vec::new(),
            aggregation_cache: HashMap::new(),
            last_aggregation_time: SystemTime::now(),
            aggregation_count: 0,
        })
    }

    /// Get the configuration
    pub fn config(&self) -> &AggregatorConfig {
        &self.config
    }

    /// Add metrics data point
    pub async fn add_data_point(&mut self, data_point: MetricsDataPoint) -> Result<()> {
        self.data_points.push(data_point);

        // Trim old data points if we exceed the limit
        if self.data_points.len() > self.config.max_data_points {
            let excess = self.data_points.len() - self.config.max_data_points;
            self.data_points.drain(0..excess);
        }

        // Auto-aggregate if enabled
        if self.config.enable_auto_aggregation {
            self.auto_aggregate().await?;
        }

        Ok(())
    }

    /// Aggregate metrics based on query
    pub async fn aggregate_metrics(&mut self, query: MetricsQuery) -> Result<AggregatedMetrics> {
        let start_time = SystemTime::now();
        
        // Check cache first
        let cache_key = self.generate_cache_key(&query);
        if let Some(cached) = self.aggregation_cache.get(&cache_key) {
            debug!("Returning cached aggregation result");
            return Ok(cached.clone());
        }

        // Filter data points by time range
        let filtered_points: Vec<&MetricsDataPoint> = self
            .data_points
            .iter()
            .filter(|point| {
                point.timestamp >= query.start_time && point.timestamp <= query.end_time
            })
            .collect();

        if filtered_points.is_empty() {
            warn!("No data points found for the specified time range");
            return Ok(AggregatedMetrics {
                query,
                data_points: Vec::new(),
                summary: MetricsSummary::default(),
            });
        }

        // Aggregate data points
        let aggregated_points = self.aggregate_data_points(&filtered_points, &query).await?;

        // Generate summary
        let summary = self.generate_summary(&aggregated_points, &query).await?;

        let result = AggregatedMetrics {
            query,
            data_points: aggregated_points,
            summary,
        };

        // Cache the result
        self.aggregation_cache.insert(cache_key, result.clone());

        let aggregation_time = start_time.elapsed();
        debug!("Metrics aggregation completed in {:?}", aggregation_time);

        Ok(result)
    }

    /// Auto-aggregate data points
    async fn auto_aggregate(&mut self) -> Result<()> {
        let now = SystemTime::now();
        
        // Check if it's time to aggregate
        if now.duration_since(self.last_aggregation_time).unwrap() < self.config.aggregation_window {
            return Ok(());
        }

        // Perform aggregation
        let window_start = now - self.config.aggregation_window;
        let query = MetricsQuery {
            start_time: window_start,
            end_time: now,
            interval: self.config.default_interval,
            metrics: vec![], // All metrics
            severity_filter: None,
            limit: None,
        };

        let _aggregated = self.aggregate_metrics(query).await?;
        
        self.last_aggregation_time = now;
        self.aggregation_count += 1;

        info!("Auto-aggregation completed, count: {}", self.aggregation_count);

        Ok(())
    }

    /// Aggregate data points based on interval
    async fn aggregate_data_points(
        &self,
        points: &[&MetricsDataPoint],
        query: &MetricsQuery,
    ) -> Result<Vec<MetricsDataPoint>> {
        if points.is_empty() {
            return Ok(Vec::new());
        }

        let mut aggregated_points = Vec::new();
        let mut current_bucket_start = query.start_time;
        let mut current_bucket_points = Vec::new();

        for point in points {
            if point.timestamp >= current_bucket_start + query.interval {
                // Process current bucket
                if !current_bucket_points.is_empty() {
                    let aggregated = self.aggregate_bucket(&current_bucket_points, current_bucket_start).await?;
                    aggregated_points.push(aggregated);
                }

                // Start new bucket
                current_bucket_start = current_bucket_start + query.interval;
                current_bucket_points.clear();
            }

            current_bucket_points.push(*point);
        }

        // Process last bucket
        if !current_bucket_points.is_empty() {
            let aggregated = self.aggregate_bucket(&current_bucket_points, current_bucket_start).await?;
            aggregated_points.push(aggregated);
        }

        Ok(aggregated_points)
    }

    /// Aggregate a bucket of data points
    async fn aggregate_bucket(
        &self,
        points: &[&MetricsDataPoint],
        bucket_start: SystemTime,
    ) -> Result<MetricsDataPoint> {
        let mut aggregated = MetricsDataPoint {
            timestamp: bucket_start,
            system: None,
            network: None,
            disk_io: None,
            scan: None,
            heal: None,
            policy: None,
        };

        // Aggregate system metrics
        let system_metrics: Vec<&SystemMetrics> = points
            .iter()
            .filter_map(|p| p.system.as_ref())
            .collect();
        
        if !system_metrics.is_empty() {
            aggregated.system = Some(self.aggregate_system_metrics(&system_metrics).await?);
        }

        // Aggregate network metrics
        let network_metrics: Vec<&NetworkMetrics> = points
            .iter()
            .filter_map(|p| p.network.as_ref())
            .collect();
        
        if !network_metrics.is_empty() {
            aggregated.network = Some(self.aggregate_network_metrics(&network_metrics).await?);
        }

        // Aggregate disk I/O metrics
        let disk_metrics: Vec<&DiskMetrics> = points
            .iter()
            .filter_map(|p| p.disk_io.as_ref())
            .collect();
        
        if !disk_metrics.is_empty() {
            aggregated.disk_io = Some(self.aggregate_disk_metrics(&disk_metrics).await?);
        }

        // Aggregate scan metrics
        let scan_metrics: Vec<&ScanMetrics> = points
            .iter()
            .filter_map(|p| p.scan.as_ref())
            .collect();
        
        if !scan_metrics.is_empty() {
            aggregated.scan = Some(self.aggregate_scan_metrics(&scan_metrics).await?);
        }

        // Aggregate heal metrics
        let heal_metrics: Vec<&HealMetrics> = points
            .iter()
            .filter_map(|p| p.heal.as_ref())
            .collect();
        
        if !heal_metrics.is_empty() {
            aggregated.heal = Some(self.aggregate_heal_metrics(&heal_metrics).await?);
        }

        // Aggregate policy metrics
        let policy_metrics: Vec<&PolicyMetrics> = points
            .iter()
            .filter_map(|p| p.policy.as_ref())
            .collect();
        
        if !policy_metrics.is_empty() {
            aggregated.policy = Some(self.aggregate_policy_metrics(&policy_metrics).await?);
        }

        Ok(aggregated)
    }

    /// Aggregate system metrics
    async fn aggregate_system_metrics(&self, metrics: &[&SystemMetrics]) -> Result<SystemMetrics> {
        if metrics.is_empty() {
            return Ok(SystemMetrics::default());
        }

        let cpu_usage: f64 = metrics.iter().map(|m| m.cpu_usage).sum::<f64>() / metrics.len() as f64;
        let memory_usage: f64 = metrics.iter().map(|m| m.memory_usage).sum::<f64>() / metrics.len() as f64;
        let disk_usage: f64 = metrics.iter().map(|m| m.disk_usage).sum::<f64>() / metrics.len() as f64;
        let system_load: f64 = metrics.iter().map(|m| m.system_load).sum::<f64>() / metrics.len() as f64;
        let active_operations: u64 = metrics.iter().map(|m| m.active_operations).sum::<u64>() / metrics.len() as u64;

        // Aggregate health issues
        let mut health_issues = HashMap::new();
        for metric in metrics {
            for (severity, count) in &metric.health_issues {
                *health_issues.entry(*severity).or_insert(0) += count;
            }
        }

        Ok(SystemMetrics {
            timestamp: SystemTime::now(),
            cpu_usage,
            memory_usage,
            disk_usage,
            network_io: NetworkMetrics::default(), // Will be aggregated separately
            disk_io: DiskMetrics::default(),       // Will be aggregated separately
            active_operations,
            system_load,
            health_issues,
            scan_metrics: ScanMetrics::default(),   // Will be aggregated separately
            heal_metrics: HealMetrics::default(),   // Will be aggregated separately
            policy_metrics: PolicyMetrics::default(), // Will be aggregated separately
        })
    }

    /// Aggregate network metrics
    async fn aggregate_network_metrics(&self, metrics: &[&NetworkMetrics]) -> Result<NetworkMetrics> {
        if metrics.is_empty() {
            return Ok(NetworkMetrics::default());
        }

        let bytes_received_per_sec: u64 = metrics.iter().map(|m| m.bytes_received_per_sec).sum::<u64>() / metrics.len() as u64;
        let bytes_sent_per_sec: u64 = metrics.iter().map(|m| m.bytes_sent_per_sec).sum::<u64>() / metrics.len() as u64;
        let packets_received_per_sec: u64 = metrics.iter().map(|m| m.packets_received_per_sec).sum::<u64>() / metrics.len() as u64;
        let packets_sent_per_sec: u64 = metrics.iter().map(|m| m.packets_sent_per_sec).sum::<u64>() / metrics.len() as u64;

        Ok(NetworkMetrics {
            bytes_received_per_sec,
            bytes_sent_per_sec,
            packets_received_per_sec,
            packets_sent_per_sec,
        })
    }

    /// Aggregate disk metrics
    async fn aggregate_disk_metrics(&self, metrics: &[&DiskMetrics]) -> Result<DiskMetrics> {
        if metrics.is_empty() {
            return Ok(DiskMetrics::default());
        }

        let bytes_read_per_sec: u64 = metrics.iter().map(|m| m.bytes_read_per_sec).sum::<u64>() / metrics.len() as u64;
        let bytes_written_per_sec: u64 = metrics.iter().map(|m| m.bytes_written_per_sec).sum::<u64>() / metrics.len() as u64;
        let read_ops_per_sec: u64 = metrics.iter().map(|m| m.read_ops_per_sec).sum::<u64>() / metrics.len() as u64;
        let write_ops_per_sec: u64 = metrics.iter().map(|m| m.write_ops_per_sec).sum::<u64>() / metrics.len() as u64;
        let avg_read_latency_ms: f64 = metrics.iter().map(|m| m.avg_read_latency_ms).sum::<f64>() / metrics.len() as f64;
        let avg_write_latency_ms: f64 = metrics.iter().map(|m| m.avg_write_latency_ms).sum::<f64>() / metrics.len() as f64;

        Ok(DiskMetrics {
            bytes_read_per_sec,
            bytes_written_per_sec,
            read_ops_per_sec,
            write_ops_per_sec,
            avg_read_latency_ms,
            avg_write_latency_ms,
        })
    }

    /// Aggregate scan metrics
    async fn aggregate_scan_metrics(&self, metrics: &[&ScanMetrics]) -> Result<ScanMetrics> {
        if metrics.is_empty() {
            return Ok(ScanMetrics::default());
        }

        let objects_scanned: u64 = metrics.iter().map(|m| m.objects_scanned).sum();
        let bytes_scanned: u64 = metrics.iter().map(|m| m.bytes_scanned).sum();
        let scan_duration: Duration = metrics.iter().map(|m| m.scan_duration).sum();
        let health_issues_found: u64 = metrics.iter().map(|m| m.health_issues_found).sum();
        let scan_cycles_completed: u64 = metrics.iter().map(|m| m.scan_cycles_completed).sum();

        // Calculate rates
        let total_duration_secs = scan_duration.as_secs_f64();
        let scan_rate_objects_per_sec = if total_duration_secs > 0.0 {
            objects_scanned as f64 / total_duration_secs
        } else {
            0.0
        };

        let scan_rate_bytes_per_sec = if total_duration_secs > 0.0 {
            bytes_scanned as f64 / total_duration_secs
        } else {
            0.0
        };

        Ok(ScanMetrics {
            objects_scanned,
            bytes_scanned,
            scan_duration,
            scan_rate_objects_per_sec,
            scan_rate_bytes_per_sec,
            health_issues_found,
            scan_cycles_completed,
            last_scan_time: metrics.last().and_then(|m| m.last_scan_time),
        })
    }

    /// Aggregate heal metrics
    async fn aggregate_heal_metrics(&self, metrics: &[&HealMetrics]) -> Result<HealMetrics> {
        if metrics.is_empty() {
            return Ok(HealMetrics::default());
        }

        let total_repairs: u64 = metrics.iter().map(|m| m.total_repairs).sum();
        let successful_repairs: u64 = metrics.iter().map(|m| m.successful_repairs).sum();
        let failed_repairs: u64 = metrics.iter().map(|m| m.failed_repairs).sum();
        let total_repair_time: Duration = metrics.iter().map(|m| m.total_repair_time).sum();
        let total_retry_attempts: u64 = metrics.iter().map(|m| m.total_retry_attempts).sum();

        // Calculate average repair time
        let average_repair_time = if total_repairs > 0 {
            let total_ms = total_repair_time.as_millis() as u64;
            Duration::from_millis(total_ms / total_repairs)
        } else {
            Duration::ZERO
        };

        // Get latest values for current state
        let active_repair_workers = metrics.last().map(|m| m.active_repair_workers).unwrap_or(0);
        let queued_repair_tasks = metrics.last().map(|m| m.queued_repair_tasks).unwrap_or(0);
        let last_repair_time = metrics.last().and_then(|m| m.last_repair_time);

        Ok(HealMetrics {
            total_repairs,
            successful_repairs,
            failed_repairs,
            total_repair_time,
            average_repair_time,
            active_repair_workers,
            queued_repair_tasks,
            last_repair_time,
            total_retry_attempts,
        })
    }

    /// Aggregate policy metrics
    async fn aggregate_policy_metrics(&self, metrics: &[&PolicyMetrics]) -> Result<PolicyMetrics> {
        if metrics.is_empty() {
            return Ok(PolicyMetrics::default());
        }

        let total_evaluations: u64 = metrics.iter().map(|m| m.total_evaluations).sum();
        let allowed_operations: u64 = metrics.iter().map(|m| m.allowed_operations).sum();
        let denied_operations: u64 = metrics.iter().map(|m| m.denied_operations).sum();
        let scan_policy_evaluations: u64 = metrics.iter().map(|m| m.scan_policy_evaluations).sum();
        let heal_policy_evaluations: u64 = metrics.iter().map(|m| m.heal_policy_evaluations).sum();
        let retention_policy_evaluations: u64 = metrics.iter().map(|m| m.retention_policy_evaluations).sum();
        let total_evaluation_time: Duration = metrics.iter().map(|m| m.average_evaluation_time).sum();

        // Calculate average evaluation time
        let average_evaluation_time = if total_evaluations > 0 {
            let total_ms = total_evaluation_time.as_millis() as u64;
            Duration::from_millis(total_ms / total_evaluations)
        } else {
            Duration::ZERO
        };

        Ok(PolicyMetrics {
            total_evaluations,
            allowed_operations,
            denied_operations,
            scan_policy_evaluations,
            heal_policy_evaluations,
            retention_policy_evaluations,
            average_evaluation_time,
        })
    }

    /// Generate summary statistics
    async fn generate_summary(
        &self,
        data_points: &[MetricsDataPoint],
        query: &MetricsQuery,
    ) -> Result<MetricsSummary> {
        let total_points = data_points.len() as u64;
        let time_range = query.end_time.duration_since(query.start_time).unwrap_or(Duration::ZERO);

        // Calculate averages from system metrics
        let system_metrics: Vec<&SystemMetrics> = data_points
            .iter()
            .filter_map(|p| p.system.as_ref())
            .collect();

        let avg_cpu_usage = if !system_metrics.is_empty() {
            system_metrics.iter().map(|m| m.cpu_usage).sum::<f64>() / system_metrics.len() as f64
        } else {
            0.0
        };

        let avg_memory_usage = if !system_metrics.is_empty() {
            system_metrics.iter().map(|m| m.memory_usage).sum::<f64>() / system_metrics.len() as f64
        } else {
            0.0
        };

        let avg_disk_usage = if !system_metrics.is_empty() {
            system_metrics.iter().map(|m| m.disk_usage).sum::<f64>() / system_metrics.len() as f64
        } else {
            0.0
        };

        // Calculate totals from scan and heal metrics
        let scan_metrics: Vec<&ScanMetrics> = data_points
            .iter()
            .filter_map(|p| p.scan.as_ref())
            .collect();

        let total_objects_scanned = scan_metrics.iter().map(|m| m.objects_scanned).sum();
        let total_health_issues = scan_metrics.iter().map(|m| m.health_issues_found).sum();

        let heal_metrics: Vec<&HealMetrics> = data_points
            .iter()
            .filter_map(|p| p.heal.as_ref())
            .collect();

        let total_repairs = heal_metrics.iter().map(|m| m.total_repairs).sum();
        let successful_repairs: u64 = heal_metrics.iter().map(|m| m.successful_repairs).sum();
        let repair_success_rate = if total_repairs > 0 {
            successful_repairs as f64 / total_repairs as f64
        } else {
            0.0
        };

        Ok(MetricsSummary {
            total_points,
            time_range,
            avg_cpu_usage,
            avg_memory_usage,
            avg_disk_usage,
            total_objects_scanned,
            total_repairs,
            repair_success_rate,
            total_health_issues,
        })
    }

    /// Generate cache key for query
    fn generate_cache_key(&self, query: &MetricsQuery) -> String {
        format!(
            "{:?}_{:?}_{:?}_{:?}",
            query.start_time, query.end_time, query.interval, query.metrics
        )
    }

    /// Clear old cache entries
    pub async fn clear_old_cache(&mut self) -> Result<()> {
        let now = SystemTime::now();
        let retention_period = Duration::from_secs(3600); // 1 hour

        self.aggregation_cache.retain(|_key, value| {
            if let Some(latest_point) = value.data_points.last() {
                now.duration_since(latest_point.timestamp).unwrap_or(Duration::ZERO) < retention_period
            } else {
                false
            }
        });

        info!("Cleared old cache entries, remaining: {}", self.aggregation_cache.len());
        Ok(())
    }

    /// Get aggregation statistics
    pub fn get_statistics(&self) -> AggregatorStatistics {
        AggregatorStatistics {
            total_data_points: self.data_points.len(),
            total_aggregations: self.aggregation_count,
            cache_size: self.aggregation_cache.len(),
            last_aggregation_time: self.last_aggregation_time,
            config: self.config.clone(),
        }
    }
}

/// Aggregator statistics
#[derive(Debug, Clone)]
pub struct AggregatorStatistics {
    pub total_data_points: usize,
    pub total_aggregations: u64,
    pub cache_size: usize,
    pub last_aggregation_time: SystemTime,
    pub config: AggregatorConfig,
}

impl Default for MetricsSummary {
    fn default() -> Self {
        Self {
            total_points: 0,
            time_range: Duration::ZERO,
            avg_cpu_usage: 0.0,
            avg_memory_usage: 0.0,
            avg_disk_usage: 0.0,
            total_objects_scanned: 0,
            total_repairs: 0,
            repair_success_rate: 0.0,
            total_health_issues: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scanner::Severity;

    #[tokio::test]
    async fn test_aggregator_creation() {
        let aggregator = Aggregator::new(Duration::from_secs(300)).await.unwrap();
        
        assert_eq!(aggregator.config().default_interval, Duration::from_secs(300));
        assert!(aggregator.config().enable_auto_aggregation);
    }

    #[tokio::test]
    async fn test_data_point_addition() {
        let mut aggregator = Aggregator::new(Duration::from_secs(300)).await.unwrap();
        
        let data_point = MetricsDataPoint {
            timestamp: SystemTime::now(),
            system: Some(SystemMetrics::default()),
            network: None,
            disk_io: None,
            scan: None,
            heal: None,
            policy: None,
        };
        
        aggregator.add_data_point(data_point).await.unwrap();
        
        let stats = aggregator.get_statistics();
        assert_eq!(stats.total_data_points, 1);
    }

    #[tokio::test]
    async fn test_metrics_aggregation() {
        let mut aggregator = Aggregator::new(Duration::from_secs(300)).await.unwrap();
        
        // Add some test data points
        for i in 0..5 {
            let mut system_metrics = SystemMetrics::default();
            system_metrics.cpu_usage = i as f64 * 10.0;
            system_metrics.memory_usage = i as f64 * 20.0;
            
            let data_point = MetricsDataPoint {
                timestamp: SystemTime::now() + Duration::from_secs(i * 60),
                system: Some(system_metrics),
                network: None,
                disk_io: None,
                scan: None,
                heal: None,
                policy: None,
            };
            
            aggregator.add_data_point(data_point).await.unwrap();
        }
        
        let query = MetricsQuery {
            start_time: SystemTime::now(),
            end_time: SystemTime::now() + Duration::from_secs(300),
            interval: Duration::from_secs(60),
            metrics: vec![MetricType::System],
            severity_filter: None,
            limit: None,
        };
        
        let result = aggregator.aggregate_metrics(query).await.unwrap();
        assert_eq!(result.data_points.len(), 5);
        assert_eq!(result.summary.total_points, 5);
    }

    #[tokio::test]
    async fn test_system_metrics_aggregation() {
        let mut aggregator = Aggregator::new(Duration::from_secs(300)).await.unwrap();
        
        let metrics = vec![
            SystemMetrics {
                cpu_usage: 10.0,
                memory_usage: 20.0,
                disk_usage: 30.0,
                ..Default::default()
            },
            SystemMetrics {
                cpu_usage: 20.0,
                memory_usage: 40.0,
                disk_usage: 60.0,
                ..Default::default()
            },
        ];
        
        let aggregated = aggregator.aggregate_system_metrics(&metrics.iter().collect::<Vec<_>>()).await.unwrap();
        
        assert_eq!(aggregated.cpu_usage, 15.0);
        assert_eq!(aggregated.memory_usage, 30.0);
        assert_eq!(aggregated.disk_usage, 45.0);
    }

    #[tokio::test]
    async fn test_cache_clearing() {
        let mut aggregator = Aggregator::new(Duration::from_secs(300)).await.unwrap();
        
        // Add some cached data
        let query = MetricsQuery {
            start_time: SystemTime::now() - Duration::from_secs(3600),
            end_time: SystemTime::now() - Duration::from_secs(3000),
            interval: Duration::from_secs(60),
            metrics: vec![],
            severity_filter: None,
            limit: None,
        };
        
        let _result = aggregator.aggregate_metrics(query).await.unwrap();
        
        let stats_before = aggregator.get_statistics();
        assert_eq!(stats_before.cache_size, 1);
        
        aggregator.clear_old_cache().await.unwrap();
        
        let stats_after = aggregator.get_statistics();
        assert_eq!(stats_after.cache_size, 0);
    }
} 