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
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant, SystemTime},
};

use tokio::{
    sync::{broadcast, RwLock},
    time::sleep,
};
use tracing::{error, info, warn};
use tokio_util::sync::CancellationToken;

use crate::{core, error::Result, metrics, SystemEvent};
use crate::core::Status;
use super::{HealthIssue, HealthIssueType, Severity};

/// Represents a discovered object during scanning
#[derive(Debug, Clone)]
pub struct ScannedObject {
    pub bucket: String,
    pub object: String,
    pub version_id: Option<String>,
    pub path: PathBuf,
    pub size: u64,
    pub modified_time: SystemTime,
    pub metadata: HashMap<String, String>,
    pub health_issues: Vec<HealthIssue>,
}

/// Configuration for the scanner engine
#[derive(Debug, Clone)]
pub struct EngineConfig {
    /// Root directory to scan
    pub root_path: String,
    /// Maximum number of concurrent scan workers
    pub max_workers: usize,
    /// Scan interval between cycles
    pub scan_interval: Duration,
    /// Bandwidth limit for scanning (bytes per second)
    pub bandwidth_limit: Option<u64>,
    /// Whether to enable deep scanning (bitrot detection)
    pub enable_deep_scan: bool,
    /// Probability of healing objects during scan (1 in N)
    pub heal_probability: u32,
    /// Maximum folders to scan before compacting
    pub max_folders_before_compact: u64,
    /// Sleep duration between folder scans
    pub folder_sleep_duration: Duration,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            root_path: "/data".to_string(),
            max_workers: 4,
            scan_interval: Duration::from_secs(300), // 5 minutes
            bandwidth_limit: None,
            enable_deep_scan: false,
            heal_probability: 1024, // 1 in 1024 objects
            max_folders_before_compact: 10000,
            folder_sleep_duration: Duration::from_millis(1),
        }
    }
}

/// Scanner statistics
#[derive(Debug, Clone, Default)]
pub struct ScannerStatistics {
    pub objects_scanned: u64,
    pub bytes_scanned: u64,
    pub issues_found: u64,
    pub scan_duration: Duration,
    pub scan_rate_objects_per_sec: f64,
    pub scan_rate_bytes_per_sec: f64,
    pub folders_scanned: u64,
    pub objects_with_issues: u64,
}

/// Main scanner engine
pub struct Engine {
    config: EngineConfig,
    coordinator: Arc<core::Coordinator>,
    metrics: Arc<metrics::Collector>,
    cancel_token: CancellationToken,
    status: Arc<RwLock<Status>>,
    statistics: Arc<RwLock<ScannerStatistics>>,
    scan_cycle: Arc<RwLock<u64>>,
}

impl Engine {
    /// Create a new scanner engine
    pub async fn new(
        config: EngineConfig,
        coordinator: Arc<core::Coordinator>,
        metrics: Arc<metrics::Collector>,
        cancel_token: CancellationToken,
    ) -> Result<Self> {
        let engine = Self {
            config,
            coordinator,
            metrics,
            cancel_token,
            status: Arc::new(RwLock::new(Status::Initializing)),
            statistics: Arc::new(RwLock::new(ScannerStatistics::default())),
            scan_cycle: Arc::new(RwLock::new(0)),
        };

        info!("Scanner engine created with config: {:?}", engine.config);
        Ok(engine)
    }

    /// Start the scanner engine
    pub async fn start(&self) -> Result<()> {
        info!("Starting scanner engine");
        *self.status.write().await = Status::Running;

        let engine = self.clone_for_background();
        tokio::spawn(async move {
            if let Err(e) = engine.run_scan_loop().await {
                error!("Scanner engine error: {}", e);
            }
        });

        Ok(())
    }

    /// Stop the scanner engine
    pub async fn stop(&self) -> Result<()> {
        info!("Stopping scanner engine");
        *self.status.write().await = Status::Stopping;
        self.cancel_token.cancel();
        *self.status.write().await = Status::Stopped;
        Ok(())
    }

    /// Get current status
    pub async fn status(&self) -> Status {
        self.status.read().await.clone()
    }

    /// Get current statistics
    pub async fn statistics(&self) -> ScannerStatistics {
        self.statistics.read().await.clone()
    }

    /// Clone the engine for background tasks
    fn clone_for_background(&self) -> Arc<Self> {
        Arc::new(Self {
            config: self.config.clone(),
            coordinator: self.coordinator.clone(),
            metrics: self.metrics.clone(),
            cancel_token: self.cancel_token.clone(),
            status: self.status.clone(),
            statistics: self.statistics.clone(),
            scan_cycle: self.scan_cycle.clone(),
        })
    }

    /// Main scan loop
    async fn run_scan_loop(&self) -> Result<()> {
        info!("Scanner engine loop started");

        loop {
            tokio::select! {
                _ = self.cancel_token.cancelled() => {
                    info!("Scanner engine received cancellation signal");
                    break;
                }
                _ = sleep(self.config.scan_interval) => {
                    if let Err(e) = self.run_scan_cycle().await {
                        error!("Scan cycle failed: {}", e);
                    }
                }
            }
        }

        Ok(())
    }

    /// Run a single scan cycle
    async fn run_scan_cycle(&self) -> Result<()> {
        let cycle_start = Instant::now();
        let cycle = {
            let mut cycle_guard = self.scan_cycle.write().await;
            *cycle_guard += 1;
            *cycle_guard
        };

        info!("Starting scan cycle {}", cycle);

        // Reset statistics for new cycle
        {
            let mut stats = self.statistics.write().await;
            *stats = ScannerStatistics::default();
        }

        // Scan the root directory
        let scan_result = self.scan_directory(&self.config.root_path).await?;

        // Update statistics
        {
            let mut stats = self.statistics.write().await;
            stats.scan_duration = cycle_start.elapsed();
            stats.objects_scanned = scan_result.objects.len() as u64;
            stats.bytes_scanned = scan_result.total_size;
            stats.issues_found = scan_result.total_issues;
            stats.folders_scanned = scan_result.folders_scanned;
            stats.objects_with_issues = scan_result.objects_with_issues;

            if stats.scan_duration.as_secs() > 0 {
                stats.scan_rate_objects_per_sec = stats.objects_scanned as f64 / stats.scan_duration.as_secs() as f64;
                stats.scan_rate_bytes_per_sec = stats.bytes_scanned as f64 / stats.scan_duration.as_secs() as f64;
            }
        }

        // Publish scan completion event
        let scan_report = crate::scanner::ScanReport {
            scan_id: cycle.to_string(),
            status: "completed".to_string(),
            summary: format!("Scanned {} objects, found {} issues", scan_result.objects.len(), scan_result.total_issues),
            issues_found: scan_result.total_issues,
        };

        self.coordinator.publish_event(SystemEvent::ScanCompleted(scan_report)).await?;

        info!(
            "Scan cycle {} completed: {} objects, {} bytes, {} issues in {:?}",
            cycle,
            scan_result.objects.len(),
            scan_result.total_size,
            scan_result.total_issues,
            cycle_start.elapsed()
        );

        Ok(())
    }

    /// Scan a directory recursively
    async fn scan_directory(&self, path: &str) -> Result<ScanResult> {
        let mut result = ScanResult::default();
        let path_buf = PathBuf::from(path);

        if !path_buf.exists() {
            warn!("Scan path does not exist: {}", path);
            return Ok(result);
        }

        if !path_buf.is_dir() {
            warn!("Scan path is not a directory: {}", path);
            return Ok(result);
        }

        self.scan_directory_recursive(&path_buf, &mut result).await?;
        Ok(result)
    }

    /// Recursively scan a directory
    async fn scan_directory_recursive(&self, dir_path: &Path, result: &mut ScanResult) -> Result<()> {
        result.folders_scanned += 1;

        // Check for cancellation
        if self.cancel_token.is_cancelled() {
            return Ok(());
        }

        let entries = match std::fs::read_dir(dir_path) {
            Ok(entries) => entries,
            Err(e) => {
                warn!("Failed to read directory {}: {}", dir_path.display(), e);
                return Ok(());
            }
        };

        for entry in entries {
            if self.cancel_token.is_cancelled() {
                break;
            }

            let entry = match entry {
                Ok(entry) => entry,
                Err(e) => {
                    warn!("Failed to read directory entry: {}", e);
                    continue;
                }
            };

            let file_path = entry.path();
            let _path_str = file_path.to_string_lossy();
            let entry_name = file_path.file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("unknown");

            // Skip hidden files and system files
            if entry_name.starts_with('.') || entry_name == ".." || entry_name == "." {
                continue;
            }

            if file_path.is_dir() {
                // Recursively scan subdirectories
                Box::pin(self.scan_directory_recursive(&file_path, result)).await?;
            } else if file_path.is_file() {
                // Scan individual file
                if let Some(scanned_object) = self.scan_object(&file_path).await? {
                    result.objects.push(scanned_object.clone());
                    result.total_size += scanned_object.size;

                    if !scanned_object.health_issues.is_empty() {
                        result.objects_with_issues += 1;
                        result.total_issues += scanned_object.health_issues.len() as u64;

                        // Publish health issues
                        for issue in &scanned_object.health_issues {
                            let health_issue = crate::scanner::HealthIssue {
                                issue_type: issue.issue_type.clone(),
                                severity: issue.severity,
                                bucket: scanned_object.bucket.clone(),
                                object: scanned_object.object.clone(),
                                description: issue.description.clone(),
                                metadata: None, // TODO: Convert HashMap to ObjectMetadata
                            };

                            self.coordinator.publish_event(SystemEvent::HealthIssueDetected(health_issue)).await?;
                        }
                    }

                    // Publish object discovered event
                    let metadata = crate::ObjectMetadata {
                        size: scanned_object.size,
                        mod_time: scanned_object.modified_time.duration_since(SystemTime::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_secs() as i64,
                        content_type: "application/octet-stream".to_string(),
                        etag: "".to_string(), // TODO: Calculate actual ETag
                    };

                    self.coordinator.publish_event(SystemEvent::ObjectDiscovered {
                        bucket: scanned_object.bucket.clone(),
                        object: scanned_object.object.clone(),
                        version_id: scanned_object.version_id.clone(),
                        metadata,
                    }).await?;
                }
            }

            // Sleep between items to avoid overwhelming the system
            sleep(self.config.folder_sleep_duration).await;
        }

        Ok(())
    }

    /// Scan a single object file
    async fn scan_object(&self, file_path: &Path) -> Result<Option<ScannedObject>> {
        let metadata = match std::fs::metadata(file_path) {
            Ok(metadata) => metadata,
            Err(e) => {
                warn!("Failed to read file metadata {}: {}", file_path.display(), e);
                return Ok(None);
            }
        };

        // Extract bucket and object from path
        let (bucket, object) = self.extract_bucket_object_from_path(file_path)?;
        if bucket.is_empty() || object.is_empty() {
            return Ok(None);
        }

        // Check for health issues
        let health_issues = self.check_object_health(file_path, &metadata).await?;

        let scanned_object = ScannedObject {
            bucket,
            object,
            version_id: None, // TODO: Extract version ID from path
            path: file_path.to_path_buf(),
            size: metadata.len(),
            modified_time: metadata.modified().unwrap_or(SystemTime::now()),
            metadata: HashMap::new(), // TODO: Extract metadata
            health_issues,
        };

        Ok(Some(scanned_object))
    }

    /// Extract bucket and object name from file path
    fn extract_bucket_object_from_path(&self, file_path: &Path) -> Result<(String, String)> {
        let _path_str = file_path.to_string_lossy();
        let root_path = Path::new(&self.config.root_path);
        
        if let Ok(relative_path) = file_path.strip_prefix(root_path) {
            let components: Vec<&str> = relative_path.components()
                .filter_map(|c| c.as_os_str().to_str())
                .collect();

            if components.len() >= 2 {
                let bucket = components[0].to_string();
                let object = components[1..].join("/");
                return Ok((bucket, object));
            }
        }

        Ok((String::new(), String::new()))
    }

    /// Check object health and detect issues
    async fn check_object_health(&self, file_path: &Path, metadata: &std::fs::Metadata) -> Result<Vec<HealthIssue>> {
        let mut issues = Vec::new();

        // Extract bucket and object from path for health issues
        let (bucket, object) = self.extract_bucket_object_from_path(file_path)?;

        // Check file size
        if metadata.len() == 0 {
            issues.push(HealthIssue {
                issue_type: HealthIssueType::ObjectTooSmall,
                severity: Severity::Low,
                bucket: bucket.clone(),
                object: object.clone(),
                description: "Object has zero size".to_string(),
                metadata: None,
            });
        }

        // Check file permissions
        if !metadata.permissions().readonly() {
            issues.push(HealthIssue {
                issue_type: HealthIssueType::PolicyViolation,
                severity: Severity::Medium,
                bucket: bucket.clone(),
                object: object.clone(),
                description: "Object is not read-only".to_string(),
                metadata: None,
            });
        }

        // TODO: Add more health checks:
        // - Checksum verification
        // - Replication status
        // - Encryption status
        // - Metadata consistency
        // - Disk health

        Ok(issues)
    }

    /// Start scanning operations
    pub async fn start_scan(&self) -> Result<()> {
        let mut status = self.status.write().await;
        *status = Status::Running;
        info!("Scanning operations started");
        Ok(())
    }

    /// Stop scanning operations
    pub async fn stop_scan(&self) -> Result<()> {
        let mut status = self.status.write().await;
        *status = Status::Stopped;
        info!("Scanning operations stopped");
        Ok(())
    }

    /// Get engine configuration
    pub async fn get_config(&self) -> ScanConfig {
        self.config.clone()
    }
}

/// Result of a scan operation
#[derive(Debug, Clone, Default)]
pub struct ScanResult {
    pub objects: Vec<ScannedObject>,
    pub total_size: u64,
    pub total_issues: u64,
    pub folders_scanned: u64,
    pub objects_with_issues: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::Duration;

    #[tokio::test]
    async fn test_engine_creation() {
        let config = EngineConfig::default();
        let coordinator = Arc::new(core::Coordinator::new(
            core::CoordinatorConfig::default(),
            Arc::new(metrics::Collector::new(metrics::CollectorConfig::default()).await.unwrap()),
            CancellationToken::new(),
        ).await.unwrap());
        let metrics = Arc::new(metrics::Collector::new(metrics::CollectorConfig::default()).await.unwrap());
        let cancel_token = CancellationToken::new();

        let engine = Engine::new(config, coordinator, metrics, cancel_token).await;
        assert!(engine.is_ok());
    }

    #[tokio::test]
    async fn test_path_extraction() {
        let config = EngineConfig {
            root_path: "/data".to_string(),
            ..Default::default()
        };
        let coordinator = Arc::new(core::Coordinator::new(
            core::CoordinatorConfig::default(),
            Arc::new(metrics::Collector::new(metrics::CollectorConfig::default()).await.unwrap()),
            CancellationToken::new(),
        ).await.unwrap());
        let metrics = Arc::new(metrics::Collector::new(metrics::CollectorConfig::default()).await.unwrap());
        let cancel_token = CancellationToken::new();

        let engine = Engine::new(config, coordinator, metrics, cancel_token).await.unwrap();
        
        let test_path = Path::new("/data/bucket1/object1.txt");
        let (bucket, object) = engine.extract_bucket_object_from_path(test_path).unwrap();
        
        assert_eq!(bucket, "bucket1");
        assert_eq!(object, "object1.txt");
    }
} 