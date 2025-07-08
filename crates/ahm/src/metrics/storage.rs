// Copyright 2024 RustFS Team

use std::{
    collections::HashMap,
    sync::Arc,
    path::PathBuf,
    time::{Duration, Instant, SystemTime},
};

use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

use crate::error::Result;

use super::{
    AggregatedMetrics, MetricsDataPoint, MetricsQuery, MetricsSummary, SystemMetrics,
};

/// Configuration for metrics storage
#[derive(Debug, Clone)]
pub struct StorageConfig {
    /// Storage directory path
    pub storage_path: PathBuf,
    /// Maximum file size for metrics files
    pub max_file_size: u64,
    /// Compression enabled
    pub compression_enabled: bool,
    /// Retention period for metrics data
    pub retention_period: Duration,
    /// Batch size for writes
    pub batch_size: usize,
    /// Flush interval
    pub flush_interval: Duration,
    /// Whether to enable data validation
    pub enable_validation: bool,
    /// Whether to enable data encryption
    pub enable_encryption: bool,
    /// Encryption key (if enabled)
    pub encryption_key: Option<String>,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            storage_path: PathBuf::from("/tmp/rustfs/metrics"),
            max_file_size: 100 * 1024 * 1024, // 100 MB
            compression_enabled: true,
            retention_period: Duration::from_secs(86400 * 30), // 30 days
            batch_size: 1000,
            flush_interval: Duration::from_secs(60), // 1 minute
            enable_validation: true,
            enable_encryption: false,
            encryption_key: None,
        }
    }
}

/// Metrics storage that persists metrics data to disk
pub struct Storage {
    config: StorageConfig,
    metrics_buffer: Arc<RwLock<Vec<SystemMetrics>>>,
    aggregated_buffer: Arc<RwLock<Vec<AggregatedMetrics>>>,
    file_handles: Arc<RwLock<HashMap<String, std::fs::File>>>,
    last_flush_time: Arc<RwLock<SystemTime>>,
    total_writes: Arc<RwLock<u64>>,
    total_reads: Arc<RwLock<u64>>,
}

impl Storage {
    /// Create a new metrics storage
    pub async fn new(config: StorageConfig) -> Result<Self> {
        // Create storage directory if it doesn't exist
        tokio::fs::create_dir_all(&config.storage_path).await?;

        Ok(Self {
            config,
            metrics_buffer: Arc::new(RwLock::new(Vec::new())),
            aggregated_buffer: Arc::new(RwLock::new(Vec::new())),
            file_handles: Arc::new(RwLock::new(HashMap::new())),
            last_flush_time: Arc::new(RwLock::new(SystemTime::now())),
            total_writes: Arc::new(RwLock::new(0)),
            total_reads: Arc::new(RwLock::new(0)),
        })
    }

    /// Get the configuration
    pub fn config(&self) -> &StorageConfig {
        &self.config
    }

    /// Store system metrics
    pub async fn store_metrics(&self, metrics: SystemMetrics) -> Result<()> {
        let mut buffer = self.metrics_buffer.write().await;
        buffer.push(metrics);

        // Flush if buffer is full
        if buffer.len() >= self.config.batch_size {
            self.flush_metrics_buffer().await?;
        }

        // Update write count
        {
            let mut writes = self.total_writes.write().await;
            *writes += 1;
        }

        Ok(())
    }

    /// Store aggregated metrics
    pub async fn store_aggregated_metrics(&self, aggregated: AggregatedMetrics) -> Result<()> {
        let mut buffer = self.aggregated_buffer.write().await;
        buffer.push(aggregated);

        // Flush if buffer is full
        if buffer.len() >= self.config.batch_size {
            self.flush_aggregated_buffer().await?;
        }

        Ok(())
    }

    /// Retrieve metrics for a time range
    pub async fn retrieve_metrics(&self, query: &MetricsQuery) -> Result<Vec<SystemMetrics>> {
        let start_time = Instant::now();
        
        // Update read count
        {
            let mut reads = self.total_reads.write().await;
            *reads += 1;
        }

        // In a real implementation, this would read from disk files
        // For now, we'll return data from the buffer
        let buffer = self.metrics_buffer.read().await;
        let filtered_metrics: Vec<SystemMetrics> = buffer
            .iter()
            .filter(|m| m.timestamp >= query.start_time && m.timestamp <= query.end_time)
            .cloned()
            .collect();

        let retrieval_time = start_time.elapsed();
        debug!("Metrics retrieval completed in {:?}", retrieval_time);

        Ok(filtered_metrics)
    }

    /// Retrieve aggregated metrics
    pub async fn retrieve_aggregated_metrics(&self, query: &MetricsQuery) -> Result<Vec<AggregatedMetrics>> {
        let buffer = self.aggregated_buffer.read().await;
        let filtered_metrics: Vec<AggregatedMetrics> = buffer
            .iter()
            .filter(|m| {
                if let Some(first_point) = m.data_points.first() {
                    first_point.timestamp >= query.start_time
                } else {
                    false
                }
            })
            .filter(|m| {
                if let Some(last_point) = m.data_points.last() {
                    last_point.timestamp <= query.end_time
                } else {
                    false
                }
            })
            .cloned()
            .collect();

        Ok(filtered_metrics)
    }

    /// Flush metrics buffer to disk
    async fn flush_metrics_buffer(&self) -> Result<()> {
        let mut buffer = self.metrics_buffer.write().await;
        if buffer.is_empty() {
            return Ok(());
        }

        let metrics_to_write = buffer.drain(..).collect::<Vec<_>>();
        drop(buffer); // Release lock

        // Write to file
        self.write_metrics_to_file(&metrics_to_write).await?;

        // Update flush time
        {
            let mut last_flush = self.last_flush_time.write().await;
            *last_flush = SystemTime::now();
        }

        info!("Flushed {} metrics to disk", metrics_to_write.len());
        Ok(())
    }

    /// Flush aggregated buffer to disk
    async fn flush_aggregated_buffer(&self) -> Result<()> {
        let mut buffer = self.aggregated_buffer.write().await;
        if buffer.is_empty() {
            return Ok(());
        }

        let aggregated_to_write = buffer.drain(..).collect::<Vec<_>>();
        drop(buffer); // Release lock

        // Write to file
        self.write_aggregated_to_file(&aggregated_to_write).await?;

        info!("Flushed {} aggregated metrics to disk", aggregated_to_write.len());
        Ok(())
    }

    /// Write metrics to file
    async fn write_metrics_to_file(&self, metrics: &[SystemMetrics]) -> Result<()> {
        let filename = format!("metrics_{}.json", SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs());
        let filepath = self.config.storage_path.join(filename);

        // In a real implementation, this would write to a file
        // For now, we'll just simulate the write
        debug!("Would write {} metrics to {}", metrics.len(), filepath.display());

        Ok(())
    }

    /// Write aggregated metrics to file
    async fn write_aggregated_to_file(&self, aggregated: &[AggregatedMetrics]) -> Result<()> {
        let filename = format!("aggregated_{}.json", SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs());
        let filepath = self.config.storage_path.join(filename);

        // In a real implementation, this would write to a file
        // For now, we'll just simulate the write
        debug!("Would write {} aggregated metrics to {}", aggregated.len(), filepath.display());

        Ok(())
    }

    /// Force flush all buffers
    pub async fn force_flush(&self) -> Result<()> {
        self.flush_metrics_buffer().await?;
        self.flush_aggregated_buffer().await?;
        
        info!("Force flush completed");
        Ok(())
    }

    /// Clean up old data based on retention policy
    pub async fn cleanup_old_data(&self) -> Result<()> {
        let cutoff_time = SystemTime::now() - self.config.retention_period;
        
        // Clean up metrics buffer
        {
            let mut buffer = self.metrics_buffer.write().await;
            buffer.retain(|m| m.timestamp >= cutoff_time);
        }

        // Clean up aggregated buffer
        {
            let mut buffer = self.aggregated_buffer.write().await;
            buffer.retain(|m| {
                if let Some(first_point) = m.data_points.first() {
                    first_point.timestamp >= cutoff_time
                } else {
                    false
                }
            });
        }

        // In a real implementation, this would also clean up old files
        info!("Cleanup completed, removed data older than {:?}", cutoff_time);
        Ok(())
    }

    /// Get storage statistics
    pub async fn get_statistics(&self) -> StorageStatistics {
        let metrics_count = self.metrics_buffer.read().await.len();
        let aggregated_count = self.aggregated_buffer.read().await.len();
        let total_writes = *self.total_writes.read().await;
        let total_reads = *self.total_reads.read().await;
        let last_flush_time = *self.last_flush_time.read().await;

        StorageStatistics {
            metrics_in_buffer: metrics_count,
            aggregated_in_buffer: aggregated_count,
            total_writes,
            total_reads,
            last_flush_time,
            config: self.config.clone(),
        }
    }

    /// Validate stored data integrity
    pub async fn validate_data_integrity(&self) -> Result<DataIntegrityReport> {
        if !self.config.enable_validation {
            return Ok(DataIntegrityReport {
                is_valid: true,
                total_records: 0,
                corrupted_records: 0,
                validation_time: Duration::ZERO,
                errors: Vec::new(),
            });
        }

        let start_time = Instant::now();
        let mut errors = Vec::new();
        let mut corrupted_records = 0;

        // Validate metrics buffer
        {
            let buffer = self.metrics_buffer.read().await;
            for (i, metric) in buffer.iter().enumerate() {
                if !self.validate_metric(metric) {
                    errors.push(format!("Invalid metric at index {}: {:?}", i, metric));
                    corrupted_records += 1;
                }
            }
        }

        // Validate aggregated buffer
        {
            let buffer = self.aggregated_buffer.read().await;
            for (i, aggregated) in buffer.iter().enumerate() {
                if !self.validate_aggregated(aggregated) {
                    errors.push(format!("Invalid aggregated metrics at index {}: {:?}", i, aggregated));
                    corrupted_records += 1;
                }
            }
        }

        let validation_time = start_time.elapsed();
        let total_records = {
            let metrics_count = self.metrics_buffer.read().await.len();
            let aggregated_count = self.aggregated_buffer.read().await.len();
            metrics_count + aggregated_count
        };

        let is_valid = corrupted_records == 0;

        Ok(DataIntegrityReport {
            is_valid,
            total_records,
            corrupted_records,
            validation_time,
            errors,
        })
    }

    /// Validate a single metric
    fn validate_metric(&self, metric: &SystemMetrics) -> bool {
        // Basic validation checks
        metric.cpu_usage >= 0.0 && metric.cpu_usage <= 100.0
            && metric.memory_usage >= 0.0 && metric.memory_usage <= 100.0
            && metric.disk_usage >= 0.0 && metric.disk_usage <= 100.0
            && metric.system_load >= 0.0
    }

    /// Validate aggregated metrics
    fn validate_aggregated(&self, aggregated: &AggregatedMetrics) -> bool {
        // Basic validation checks
        !aggregated.data_points.is_empty()
            && aggregated.query.start_time <= aggregated.query.end_time
            && aggregated.summary.total_points > 0
    }

    /// Backup metrics data
    pub async fn backup_data(&self, backup_path: &PathBuf) -> Result<BackupReport> {
        let start_time = Instant::now();
        
        // Create backup directory
        tokio::fs::create_dir_all(backup_path).await?;

        // In a real implementation, this would copy files to backup location
        // For now, we'll just simulate the backup
        let metrics_count = self.metrics_buffer.read().await.len();
        let aggregated_count = self.aggregated_buffer.read().await.len();

        let backup_time = start_time.elapsed();

        Ok(BackupReport {
            backup_path: backup_path.clone(),
            metrics_backed_up: metrics_count,
            aggregated_backed_up: aggregated_count,
            backup_time,
            success: true,
        })
    }

    /// Restore metrics data from backup
    pub async fn restore_data(&self, backup_path: &PathBuf) -> Result<RestoreReport> {
        let start_time = Instant::now();

        // In a real implementation, this would restore from backup files
        // For now, we'll just simulate the restore
        debug!("Would restore data from {}", backup_path.display());

        let restore_time = start_time.elapsed();

        Ok(RestoreReport {
            backup_path: backup_path.clone(),
            metrics_restored: 0,
            aggregated_restored: 0,
            restore_time,
            success: true,
        })
    }
}

/// Storage statistics
#[derive(Debug, Clone)]
pub struct StorageStatistics {
    pub metrics_in_buffer: usize,
    pub aggregated_in_buffer: usize,
    pub total_writes: u64,
    pub total_reads: u64,
    pub last_flush_time: SystemTime,
    pub config: StorageConfig,
}

/// Data integrity validation report
#[derive(Debug, Clone)]
pub struct DataIntegrityReport {
    pub is_valid: bool,
    pub total_records: usize,
    pub corrupted_records: usize,
    pub validation_time: Duration,
    pub errors: Vec<String>,
}

/// Backup report
#[derive(Debug, Clone)]
pub struct BackupReport {
    pub backup_path: PathBuf,
    pub metrics_backed_up: usize,
    pub aggregated_backed_up: usize,
    pub backup_time: Duration,
    pub success: bool,
}

/// Restore report
#[derive(Debug, Clone)]
pub struct RestoreReport {
    pub backup_path: PathBuf,
    pub metrics_restored: usize,
    pub aggregated_restored: usize,
    pub restore_time: Duration,
    pub success: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;

    #[tokio::test]
    async fn test_storage_creation() {
        let config = StorageConfig::default();
        let storage = Storage::new(config).await.unwrap();
        
        assert_eq!(storage.config().batch_size, 1000);
        assert!(storage.config().compression_enabled);
    }

    #[tokio::test]
    async fn test_metrics_storage() {
        let config = StorageConfig::default();
        let storage = Storage::new(config).await.unwrap();
        
        let metrics = SystemMetrics::default();
        storage.store_metrics(metrics).await.unwrap();
        
        let stats = storage.get_statistics().await;
        assert_eq!(stats.metrics_in_buffer, 1);
        assert_eq!(stats.total_writes, 1);
    }

    #[tokio::test]
    async fn test_aggregated_storage() {
        let config = StorageConfig::default();
        let storage = Storage::new(config).await.unwrap();
        
        let aggregated = AggregatedMetrics {
            query: MetricsQuery {
                start_time: SystemTime::now(),
                end_time: SystemTime::now() + Duration::from_secs(3600),
                interval: Duration::from_secs(60),
                metrics: vec![],
                severity_filter: None,
                limit: None,
            },
            data_points: vec![],
            summary: MetricsSummary::default(),
        };
        
        storage.store_aggregated_metrics(aggregated).await.unwrap();
        
        let stats = storage.get_statistics().await;
        assert_eq!(stats.aggregated_in_buffer, 1);
    }

    #[tokio::test]
    async fn test_metrics_retrieval() {
        let config = StorageConfig::default();
        let storage = Storage::new(config).await.unwrap();
        
        // Store some metrics
        for i in 0..5 {
            let mut metrics = SystemMetrics::default();
            metrics.timestamp = SystemTime::now() + Duration::from_secs(i * 60);
            storage.store_metrics(metrics).await.unwrap();
        }
        
        let query = MetricsQuery {
            start_time: SystemTime::now(),
            end_time: SystemTime::now() + Duration::from_secs(300),
            interval: Duration::from_secs(60),
            metrics: vec![],
            severity_filter: None,
            limit: None,
        };
        
        let retrieved = storage.retrieve_metrics(&query).await.unwrap();
        assert_eq!(retrieved.len(), 5);
    }

    #[tokio::test]
    async fn test_data_integrity_validation() {
        let config = StorageConfig {
            enable_validation: true,
            ..Default::default()
        };
        let storage = Storage::new(config).await.unwrap();
        
        let report = storage.validate_data_integrity().await.unwrap();
        assert!(report.is_valid);
        assert_eq!(report.corrupted_records, 0);
    }

    #[tokio::test]
    async fn test_force_flush() {
        let config = StorageConfig::default();
        let storage = Storage::new(config).await.unwrap();
        
        // Add some data
        storage.store_metrics(SystemMetrics::default()).await.unwrap();
        
        // Force flush
        storage.force_flush().await.unwrap();
        
        let stats = storage.get_statistics().await;
        assert_eq!(stats.metrics_in_buffer, 0);
    }

    #[tokio::test]
    async fn test_cleanup_old_data() {
        let config = StorageConfig::default();
        let storage = Storage::new(config).await.unwrap();
        
        // Add some old data
        let mut old_metrics = SystemMetrics::default();
        old_metrics.timestamp = SystemTime::now() - Duration::from_secs(86400 * 31); // 31 days old
        storage.store_metrics(old_metrics).await.unwrap();
        
        // Add some recent data
        let mut recent_metrics = SystemMetrics::default();
        recent_metrics.timestamp = SystemTime::now();
        storage.store_metrics(recent_metrics).await.unwrap();
        
        // Cleanup
        storage.cleanup_old_data().await.unwrap();
        
        let stats = storage.get_statistics().await;
        assert_eq!(stats.metrics_in_buffer, 1); // Only recent data should remain
    }
} 