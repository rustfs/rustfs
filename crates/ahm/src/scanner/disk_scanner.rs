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
    path::Path,
    sync::Arc,
    time::{Duration, Instant, SystemTime},
};

use tokio::sync::RwLock;
use tracing::{error, info};
use anyhow;

use crate::error::Result;
use super::{HealthIssue, HealthIssueType, Severity};

/// Configuration for disk scanning
#[derive(Debug, Clone)]
pub struct DiskScannerConfig {
    /// Scan interval for disk health checks
    pub scan_interval: Duration,
    /// Minimum free space threshold (percentage)
    pub min_free_space_percent: f64,
    /// Maximum disk usage threshold (percentage)
    pub max_disk_usage_percent: f64,
    /// Minimum inode usage threshold (percentage)
    pub min_inode_usage_percent: f64,
    /// Maximum inode usage threshold (percentage)
    pub max_inode_usage_percent: f64,
    /// Whether to check disk I/O performance
    pub check_io_performance: bool,
    /// Whether to check disk temperature (if available)
    pub check_temperature: bool,
    /// Whether to check disk SMART status (if available)
    pub check_smart_status: bool,
    /// Timeout for individual disk operations
    pub operation_timeout: Duration,
    /// Maximum number of concurrent disk scans
    pub max_concurrent_scans: usize,
}

impl Default for DiskScannerConfig {
    fn default() -> Self {
        Self {
            scan_interval: Duration::from_secs(300), // 5 minutes
            min_free_space_percent: 10.0, // 10% minimum free space
            max_disk_usage_percent: 90.0, // 90% maximum usage
            min_inode_usage_percent: 5.0, // 5% minimum inode usage
            max_inode_usage_percent: 95.0, // 95% maximum inode usage
            check_io_performance: true,
            check_temperature: false, // Disabled by default
            check_smart_status: false, // Disabled by default
            operation_timeout: Duration::from_secs(30),
            max_concurrent_scans: 4,
        }
    }
}

/// Disk information and health status
#[derive(Debug, Clone)]
pub struct DiskInfo {
    pub device_path: String,
    pub mount_point: String,
    pub filesystem_type: String,
    pub total_space: u64,
    pub used_space: u64,
    pub free_space: u64,
    pub available_space: u64,
    pub usage_percent: f64,
    pub inode_total: Option<u64>,
    pub inode_used: Option<u64>,
    pub inode_free: Option<u64>,
    pub inode_usage_percent: Option<f64>,
    pub last_scan_time: SystemTime,
    pub health_status: DiskHealthStatus,
    pub performance_metrics: Option<DiskPerformanceMetrics>,
    pub temperature: Option<f64>,
    pub smart_status: Option<SmartStatus>,
}

/// Disk health status
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DiskHealthStatus {
    Healthy,
    Warning,
    Critical,
    Unknown,
}

/// Disk performance metrics
#[derive(Debug, Clone)]
pub struct DiskPerformanceMetrics {
    pub read_bytes_per_sec: f64,
    pub write_bytes_per_sec: f64,
    pub read_operations_per_sec: f64,
    pub write_operations_per_sec: f64,
    pub average_response_time_ms: f64,
    pub queue_depth: f64,
    pub utilization_percent: f64,
    pub last_updated: SystemTime,
}

/// SMART status information
#[derive(Debug, Clone)]
pub struct SmartStatus {
    pub overall_health: SmartHealthStatus,
    pub temperature: Option<f64>,
    pub power_on_hours: Option<u64>,
    pub reallocated_sectors: Option<u64>,
    pub pending_sectors: Option<u64>,
    pub uncorrectable_sectors: Option<u64>,
    pub attributes: HashMap<String, SmartAttribute>,
}

/// SMART health status
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SmartHealthStatus {
    Passed,
    Failed,
    Unknown,
}

/// SMART attribute
#[derive(Debug, Clone)]
pub struct SmartAttribute {
    pub name: String,
    pub value: u64,
    pub worst: u64,
    pub threshold: u64,
    pub status: SmartAttributeStatus,
}

/// SMART attribute status
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SmartAttributeStatus {
    Good,
    Warning,
    Critical,
    Unknown,
}

/// Result of scanning a single disk
#[derive(Debug, Clone)]
pub struct DiskScanResult {
    pub disk_info: DiskInfo,
    pub health_issues: Vec<HealthIssue>,
    pub scan_duration: Duration,
    pub success: bool,
    pub error_message: Option<String>,
}

/// Disk scanner for monitoring disk health and performance
pub struct DiskScanner {
    config: DiskScannerConfig,
    statistics: Arc<RwLock<DiskScannerStatistics>>,
    last_scan_results: Arc<RwLock<HashMap<String, DiskScanResult>>>,
}

/// Statistics for disk scanning
#[derive(Debug, Clone, Default)]
pub struct DiskScannerStatistics {
    pub disks_scanned: u64,
    pub disks_with_issues: u64,
    pub total_issues_found: u64,
    pub total_scan_time: Duration,
    pub average_scan_time: Duration,
    pub last_scan_time: Option<SystemTime>,
    pub scan_cycles_completed: u64,
    pub scan_cycles_failed: u64,
}

impl DiskScanner {
    /// Create a new disk scanner
    pub fn new(config: DiskScannerConfig) -> Self {
        Self {
            config,
            statistics: Arc::new(RwLock::new(DiskScannerStatistics::default())),
            last_scan_results: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Scan all mounted disks
    pub async fn scan_all_disks(&self) -> Result<Vec<DiskScanResult>> {
        let scan_start = Instant::now();
        let mut results = Vec::new();

        // Get list of mounted filesystems
        let mount_points = self.get_mount_points().await?;
        
        info!("Starting disk scan for {} mount points", mount_points.len());

        // Scan each mount point
        for mount_point in mount_points {
            match self.scan_disk(&mount_point).await {
                Ok(result) => {
                    results.push(result.clone());
                    
                    // Store result for later reference
                    let mut last_results = self.last_scan_results.write().await;
                    last_results.insert(mount_point.clone(), result);
                }
                Err(e) => {
                    error!("Failed to scan disk at {}: {}", mount_point, e);
                    
                    // Create error result
                    let error_result = DiskScanResult {
                        disk_info: DiskInfo {
                            device_path: "unknown".to_string(),
                            mount_point: mount_point.clone(),
                            filesystem_type: "unknown".to_string(),
                            total_space: 0,
                            used_space: 0,
                            free_space: 0,
                            available_space: 0,
                            usage_percent: 0.0,
                            inode_total: None,
                            inode_used: None,
                            inode_free: None,
                            inode_usage_percent: None,
                            last_scan_time: SystemTime::now(),
                            health_status: DiskHealthStatus::Unknown,
                            performance_metrics: None,
                            temperature: None,
                            smart_status: None,
                        },
                        health_issues: vec![HealthIssue {
                            issue_type: HealthIssueType::DiskReadError,
                            severity: Severity::High,
                            bucket: "system".to_string(),
                            object: mount_point.clone(),
                            description: format!("Failed to scan disk: {}", e),
                            metadata: None,
                        }],
                        scan_duration: scan_start.elapsed(),
                        success: false,
                        error_message: Some(e.to_string()),
                    };
                    
                    results.push(error_result);
                }
            }
        }

        // Update statistics
        self.update_statistics(|stats| {
            stats.disks_scanned += results.len() as u64;
            stats.disks_with_issues += results.iter().filter(|r| !r.health_issues.is_empty()).count() as u64;
            stats.total_issues_found += results.iter().map(|r| r.health_issues.len() as u64).sum::<u64>();
            stats.total_scan_time += scan_start.elapsed();
            stats.average_scan_time = Duration::from_millis(
                stats.total_scan_time.as_millis() as u64 / stats.disks_scanned.max(1)
            );
            stats.last_scan_time = Some(SystemTime::now());
            stats.scan_cycles_completed += 1;
        }).await;

        info!(
            "Disk scan completed: {} disks, {} issues found in {:?}",
            results.len(),
            results.iter().map(|r| r.health_issues.len()).sum::<usize>(),
            scan_start.elapsed()
        );

        Ok(results)
    }

    /// Scan a single disk
    pub async fn scan_disk(&self, mount_point: &str) -> Result<DiskScanResult> {
        let scan_start = Instant::now();
        let mut health_issues = Vec::new();

        // Get disk space information
        let disk_info = self.get_disk_info(mount_point).await?;

        // Check disk space usage
        if disk_info.usage_percent > self.config.max_disk_usage_percent {
            health_issues.push(HealthIssue {
                issue_type: HealthIssueType::DiskFull,
                severity: if disk_info.usage_percent > 95.0 { Severity::Critical } else { Severity::High },
                bucket: "system".to_string(),
                object: mount_point.to_string(),
                description: format!("Disk usage is {}%, exceeds threshold of {}%", 
                    disk_info.usage_percent, self.config.max_disk_usage_percent),
                metadata: None,
            });
        }

        if disk_info.usage_percent < self.config.min_free_space_percent {
            health_issues.push(HealthIssue {
                issue_type: HealthIssueType::DiskFull,
                severity: Severity::Medium,
                bucket: "system".to_string(),
                object: mount_point.to_string(),
                description: format!("Free space is only {}%, below threshold of {}%", 
                    100.0 - disk_info.usage_percent, self.config.min_free_space_percent),
                metadata: None,
            });
        }

        // Check inode usage if available
        if let Some(inode_usage) = disk_info.inode_usage_percent {
            if inode_usage > self.config.max_inode_usage_percent {
                health_issues.push(HealthIssue {
                    issue_type: HealthIssueType::DiskFull,
                    severity: if inode_usage > 95.0 { Severity::Critical } else { Severity::High },
                    bucket: "system".to_string(),
                    object: mount_point.to_string(),
                    description: format!("Inode usage is {}%, exceeds threshold of {}%", 
                        inode_usage, self.config.max_inode_usage_percent),
                    metadata: None,
                });
            }
        }

        // Check I/O performance if enabled
        if self.config.check_io_performance {
            if let Some(metrics) = &disk_info.performance_metrics {
                if metrics.utilization_percent > 90.0 {
                    health_issues.push(HealthIssue {
                        issue_type: HealthIssueType::DiskReadError,
                        severity: Severity::Medium,
                        bucket: "system".to_string(),
                        object: mount_point.to_string(),
                        description: format!("High disk utilization: {}%", metrics.utilization_percent),
                        metadata: None,
                    });
                }

                if metrics.average_response_time_ms > 100.0 {
                    health_issues.push(HealthIssue {
                        issue_type: HealthIssueType::DiskReadError,
                        severity: Severity::Medium,
                        bucket: "system".to_string(),
                        object: mount_point.to_string(),
                        description: format!("High disk response time: {}ms", metrics.average_response_time_ms),
                        metadata: None,
                    });
                }
            }
        }

        // Check temperature if enabled
        if self.config.check_temperature {
            if let Some(temp) = disk_info.temperature {
                if temp > 60.0 {
                    health_issues.push(HealthIssue {
                        issue_type: HealthIssueType::DiskReadError,
                        severity: if temp > 70.0 { Severity::Critical } else { Severity::High },
                        bucket: "system".to_string(),
                        object: mount_point.to_string(),
                        description: format!("High disk temperature: {}°C", temp),
                        metadata: None,
                    });
                }
            }
        }

        // Check SMART status if enabled
        if self.config.check_smart_status {
            if let Some(smart) = &disk_info.smart_status {
                if smart.overall_health == SmartHealthStatus::Failed {
                    health_issues.push(HealthIssue {
                        issue_type: HealthIssueType::DiskReadError,
                        severity: Severity::Critical,
                        bucket: "system".to_string(),
                        object: mount_point.to_string(),
                        description: "SMART health check failed".to_string(),
                        metadata: None,
                    });
                }
            }
        }

        let scan_duration = scan_start.elapsed();
        let success = health_issues.is_empty();

        Ok(DiskScanResult {
            disk_info,
            health_issues,
            scan_duration,
            success,
            error_message: None,
        })
    }

    /// Get list of mounted filesystems
    async fn get_mount_points(&self) -> Result<Vec<String>> {
        // TODO: Implement actual mount point detection
        // For now, return common mount points
        Ok(vec![
            "/".to_string(),
            "/data".to_string(),
            "/var".to_string(),
        ])
    }

    /// Get disk information for a mount point
    async fn get_disk_info(&self, mount_point: &str) -> Result<DiskInfo> {
        let path = Path::new(mount_point);
        
        // Get filesystem statistics using std::fs instead of nix for now
        let _metadata = match std::fs::metadata(path) {
            Ok(metadata) => metadata,
            Err(e) => {
                return Err(crate::error::Error::Other(anyhow::anyhow!("Failed to get filesystem stats: {}", e)));
            }
        };

        // For now, use placeholder values since we can't easily get filesystem stats
        let total_space = 1000000000; // 1GB placeholder
        let free_space = 500000000;   // 500MB placeholder
        let available_space = 450000000; // 450MB placeholder
        let used_space = total_space - free_space;
        let usage_percent = (used_space as f64 / total_space as f64) * 100.0;

        // Get inode information (placeholder)
        let inode_total = Some(1000000);
        let inode_free = Some(500000);
        let inode_used = Some(500000);
        let inode_usage_percent = Some(50.0);

        // Get filesystem type
        let filesystem_type = self.get_filesystem_type(mount_point).await.unwrap_or_else(|_| "unknown".to_string());

        // Get device path
        let device_path = self.get_device_path(mount_point).await.unwrap_or_else(|_| "unknown".to_string());

        // Get performance metrics if enabled
        let performance_metrics = if self.config.check_io_performance {
            self.get_performance_metrics(&device_path).await.ok()
        } else {
            None
        };

        // Get temperature if enabled
        let temperature = if self.config.check_temperature {
            self.get_disk_temperature(&device_path).await.ok().flatten()
        } else {
            None
        };

        // Get SMART status if enabled
        let smart_status = if self.config.check_smart_status {
            self.get_smart_status(&device_path).await.ok().flatten()
        } else {
            None
        };

        // Determine health status (placeholder - will be set by scan_disk method)
        let health_status = DiskHealthStatus::Healthy;

        Ok(DiskInfo {
            device_path,
            mount_point: mount_point.to_string(),
            filesystem_type,
            total_space,
            used_space,
            free_space,
            available_space,
            usage_percent,
            inode_total,
            inode_used,
            inode_free,
            inode_usage_percent,
            last_scan_time: SystemTime::now(),
            health_status,
            performance_metrics,
            temperature,
            smart_status,
        })
    }

    /// Get filesystem type for a mount point
    async fn get_filesystem_type(&self, _mount_point: &str) -> Result<String> {
        // TODO: Implement filesystem type detection
        // For now, return a placeholder
        Ok("ext4".to_string())
    }

    /// Get device path for a mount point
    async fn get_device_path(&self, _mount_point: &str) -> Result<String> {
        // TODO: Implement device path detection
        // For now, return a placeholder
        Ok("/dev/sda1".to_string())
    }

    /// Get disk performance metrics
    async fn get_performance_metrics(&self, _device_path: &str) -> Result<DiskPerformanceMetrics> {
        // TODO: Implement performance metrics collection
        // For now, return placeholder metrics
        Ok(DiskPerformanceMetrics {
            read_bytes_per_sec: 1000000.0, // 1MB/s
            write_bytes_per_sec: 500000.0, // 500KB/s
            read_operations_per_sec: 100.0,
            write_operations_per_sec: 50.0,
            average_response_time_ms: 5.0,
            queue_depth: 1.0,
            utilization_percent: 10.0,
            last_updated: SystemTime::now(),
        })
    }

    /// Get disk temperature
    async fn get_disk_temperature(&self, _device_path: &str) -> Result<Option<f64>> {
        // TODO: Implement temperature monitoring
        // For now, return None (temperature not available)
        Ok(None)
    }

    /// Get SMART status
    async fn get_smart_status(&self, _device_path: &str) -> Result<Option<SmartStatus>> {
        // TODO: Implement SMART status checking
        // For now, return None (SMART not available)
        Ok(None)
    }

    /// Update scanner statistics
    async fn update_statistics<F>(&self, update_fn: F)
    where
        F: FnOnce(&mut DiskScannerStatistics),
    {
        let mut stats = self.statistics.write().await;
        update_fn(&mut stats);
    }

    /// Get current statistics
    pub async fn statistics(&self) -> DiskScannerStatistics {
        self.statistics.read().await.clone()
    }

    /// Get last scan results
    pub async fn last_scan_results(&self) -> HashMap<String, DiskScanResult> {
        self.last_scan_results.read().await.clone()
    }

    /// Reset statistics
    pub async fn reset_statistics(&self) {
        let mut stats = self.statistics.write().await;
        *stats = DiskScannerStatistics::default();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_disk_scanner_creation() {
        let config = DiskScannerConfig::default();
        let scanner = DiskScanner::new(config);
        assert_eq!(scanner.statistics().await.disks_scanned, 0);
    }

    #[tokio::test]
    async fn test_disk_info_creation() {
        let disk_info = DiskInfo {
            device_path: "/dev/sda1".to_string(),
            mount_point: "/".to_string(),
            filesystem_type: "ext4".to_string(),
            total_space: 1000000000,
            used_space: 500000000,
            free_space: 500000000,
            available_space: 450000000,
            usage_percent: 50.0,
            inode_total: Some(1000000),
            inode_used: Some(500000),
            inode_free: Some(500000),
            inode_usage_percent: Some(50.0),
            last_scan_time: SystemTime::now(),
            health_status: DiskHealthStatus::Healthy,
            performance_metrics: None,
            temperature: None,
            smart_status: None,
        };

        assert_eq!(disk_info.usage_percent, 50.0);
        assert_eq!(disk_info.health_status, DiskHealthStatus::Healthy);
    }
} 