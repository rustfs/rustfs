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
    time::{Duration, SystemTime},
};

use tokio::sync::RwLock;
use tracing::info;

use crate::error::Result;
use super::{HealthIssue, HealthIssueType, Severity};

/// Configuration for object scanning
#[derive(Debug, Clone)]
pub struct ObjectScannerConfig {
    /// Whether to perform checksum verification
    pub verify_checksum: bool,
    /// Whether to check replication status
    pub check_replication: bool,
    /// Whether to validate metadata consistency
    pub validate_metadata: bool,
    /// Maximum object size to scan (bytes)
    pub max_object_size: u64,
    /// Minimum object size (bytes)
    pub min_object_size: u64,
    /// Timeout for individual object scans
    pub scan_timeout: Duration,
    /// Whether to enable deep scanning (bitrot detection)
    pub enable_deep_scan: bool,
}

impl Default for ObjectScannerConfig {
    fn default() -> Self {
        Self {
            verify_checksum: true,
            check_replication: true,
            validate_metadata: true,
            max_object_size: 1024 * 1024 * 1024 * 1024, // 1TB
            min_object_size: 0,
            scan_timeout: Duration::from_secs(30),
            enable_deep_scan: false,
        }
    }
}

/// Result of scanning a single object
#[derive(Debug, Clone)]
pub struct ObjectScanResult {
    /// Object identifier
    pub bucket: String,
    pub object: String,
    pub version_id: Option<String>,
    /// Scan success status
    pub success: bool,
    /// Object metadata discovered
    pub metadata: Option<ObjectMetadata>,
    /// Health issues detected
    pub health_issues: Vec<HealthIssue>,
    /// Time taken to scan this object
    pub scan_duration: Duration,
    /// Error message if scan failed
    pub error_message: Option<String>,
}

/// Object metadata
#[derive(Debug, Clone)]
pub struct ObjectMetadata {
    pub size: u64,
    pub modified_time: SystemTime,
    pub content_type: String,
    pub etag: String,
    pub checksum: Option<String>,
    pub replication_status: Option<String>,
    pub encryption_status: Option<String>,
    pub custom_metadata: HashMap<String, String>,
}

/// Object scanner for individual object health checking
pub struct ObjectScanner {
    config: ObjectScannerConfig,
    statistics: Arc<RwLock<ObjectScannerStatistics>>,
}

/// Statistics for object scanning
#[derive(Debug, Clone, Default)]
pub struct ObjectScannerStatistics {
    pub objects_scanned: u64,
    pub objects_with_issues: u64,
    pub total_issues_found: u64,
    pub total_scan_time: Duration,
    pub average_scan_time: Duration,
    pub checksum_verifications: u64,
    pub checksum_failures: u64,
    pub replication_checks: u64,
    pub replication_failures: u64,
}

impl ObjectScanner {
    /// Create a new object scanner
    pub fn new(config: ObjectScannerConfig) -> Self {
        Self {
            config,
            statistics: Arc::new(RwLock::new(ObjectScannerStatistics::default())),
        }
    }

    /// Scan a single object for health issues
    pub async fn scan_object(&self, bucket: &str, object: &str, version_id: Option<&str>, path: &Path) -> Result<ObjectScanResult> {
        let scan_start = std::time::Instant::now();
        let mut health_issues = Vec::new();
        let mut error_message = None;

        // Check if file exists
        if !path.exists() {
            return Ok(ObjectScanResult {
                bucket: bucket.to_string(),
                object: object.to_string(),
                version_id: version_id.map(|v| v.to_string()),
                success: false,
                metadata: None,
                health_issues: vec![HealthIssue {
                    issue_type: HealthIssueType::MissingReplica,
                    severity: Severity::Critical,
                    bucket: bucket.to_string(),
                    object: object.to_string(),
                    description: "Object file does not exist".to_string(),
                    metadata: None,
                }],
                scan_duration: scan_start.elapsed(),
                error_message: Some("Object file not found".to_string()),
            });
        }

        // Get file metadata
        let metadata = match std::fs::metadata(path) {
            Ok(metadata) => metadata,
            Err(e) => {
                error_message = Some(format!("Failed to read file metadata: {}", e));
                health_issues.push(HealthIssue {
                    issue_type: HealthIssueType::DiskReadError,
                    severity: Severity::High,
                    bucket: bucket.to_string(),
                    object: object.to_string(),
                    description: "Failed to read file metadata".to_string(),
                    metadata: None,
                });
                return Ok(ObjectScanResult {
                    bucket: bucket.to_string(),
                    object: object.to_string(),
                    version_id: version_id.map(|v| v.to_string()),
                    success: false,
                    metadata: None,
                    health_issues,
                    scan_duration: scan_start.elapsed(),
                    error_message,
                });
            }
        };

        // Check file size
        let file_size = metadata.len();
        if file_size < self.config.min_object_size {
            health_issues.push(HealthIssue {
                issue_type: HealthIssueType::ObjectTooSmall,
                severity: Severity::Low,
                bucket: bucket.to_string(),
                object: object.to_string(),
                description: format!("Object size {} is below minimum {}", file_size, self.config.min_object_size),
                metadata: None,
            });
        }

        if file_size > self.config.max_object_size {
            health_issues.push(HealthIssue {
                issue_type: HealthIssueType::ObjectTooLarge,
                severity: Severity::Medium,
                bucket: bucket.to_string(),
                object: object.to_string(),
                description: format!("Object size {} exceeds maximum {}", file_size, self.config.max_object_size),
                metadata: None,
            });
        }

        // Verify checksum if enabled
        let checksum = if self.config.verify_checksum {
            match self.verify_checksum(path).await {
                Ok(cs) => {
                    self.update_statistics(|stats| stats.checksum_verifications += 1).await;
                    Some(cs)
                }
                Err(_e) => {
                    self.update_statistics(|stats| stats.checksum_failures += 1).await;
                    health_issues.push(HealthIssue {
                        issue_type: HealthIssueType::ChecksumMismatch,
                        severity: Severity::High,
                        bucket: bucket.to_string(),
                        object: object.to_string(),
                        description: "Checksum verification failed".to_string(),
                        metadata: None,
                    });
                    None
                }
            }
        } else {
            None
        };

        // Check replication status if enabled
        let replication_status = if self.config.check_replication {
            match self.check_replication_status(bucket, object).await {
                Ok(status) => {
                    self.update_statistics(|stats| stats.replication_checks += 1).await;
                    Some(status)
                }
                Err(_e) => {
                    self.update_statistics(|stats| stats.replication_failures += 1).await;
                    health_issues.push(HealthIssue {
                        issue_type: HealthIssueType::MissingReplica,
                        severity: Severity::High,
                        bucket: bucket.to_string(),
                        object: object.to_string(),
                        description: "Replication status check failed".to_string(),
                        metadata: None,
                    });
                    None
                }
            }
        } else {
            None
        };

        // Validate metadata if enabled
        if self.config.validate_metadata {
            if let Some(issue) = self.validate_metadata(bucket, object, &metadata).await? {
                health_issues.push(issue);
            }
        }

        // Create object metadata
        let object_metadata = ObjectMetadata {
            size: file_size,
            modified_time: metadata.modified().unwrap_or(SystemTime::now()),
            content_type: self.detect_content_type(path),
            etag: self.calculate_etag(path).await?,
            checksum,
            replication_status,
            encryption_status: None, // TODO: Implement encryption status check
            custom_metadata: HashMap::new(), // TODO: Extract custom metadata
        };

        let scan_duration = scan_start.elapsed();
        let success = health_issues.is_empty();

        // Update statistics
        self.update_statistics(|stats| {
            stats.objects_scanned += 1;
            if !health_issues.is_empty() {
                stats.objects_with_issues += 1;
                stats.total_issues_found += health_issues.len() as u64;
            }
            stats.total_scan_time += scan_duration;
            stats.average_scan_time = Duration::from_millis(
                stats.total_scan_time.as_millis() as u64 / stats.objects_scanned.max(1)
            );
        }).await;

        Ok(ObjectScanResult {
            bucket: bucket.to_string(),
            object: object.to_string(),
            version_id: version_id.map(|v| v.to_string()),
            success,
            metadata: Some(object_metadata),
            health_issues,
            scan_duration,
            error_message,
        })
    }

    /// Verify object checksum
    async fn verify_checksum(&self, _path: &Path) -> Result<String> {
        // TODO: Implement actual checksum verification
        // For now, return a placeholder checksum
        Ok("placeholder_checksum".to_string())
    }

    /// Check object replication status
    async fn check_replication_status(&self, _bucket: &str, _object: &str) -> Result<String> {
        // TODO: Implement actual replication status checking
        // For now, return a placeholder status
        Ok("replicated".to_string())
    }

    /// Validate object metadata
    async fn validate_metadata(&self, _bucket: &str, _object: &str, _metadata: &std::fs::Metadata) -> Result<Option<HealthIssue>> {
        // TODO: Implement actual metadata validation
        // For now, return None (no issues)
        Ok(None)
    }

    /// Detect content type from file extension
    fn detect_content_type(&self, path: &Path) -> String {
        if let Some(extension) = path.extension() {
            match extension.to_str().unwrap_or("").to_lowercase().as_str() {
                "txt" => "text/plain",
                "json" => "application/json",
                "xml" => "application/xml",
                "html" | "htm" => "text/html",
                "css" => "text/css",
                "js" => "application/javascript",
                "png" => "image/png",
                "jpg" | "jpeg" => "image/jpeg",
                "gif" => "image/gif",
                "pdf" => "application/pdf",
                "zip" => "application/zip",
                "tar" => "application/x-tar",
                "gz" => "application/gzip",
                _ => "application/octet-stream",
            }.to_string()
        } else {
            "application/octet-stream".to_string()
        }
    }

    /// Calculate object ETag
    async fn calculate_etag(&self, _path: &Path) -> Result<String> {
        // TODO: Implement actual ETag calculation
        // For now, return a placeholder ETag
        Ok("placeholder_etag".to_string())
    }

    /// Update scanner statistics
    async fn update_statistics<F>(&self, update_fn: F)
    where
        F: FnOnce(&mut ObjectScannerStatistics),
    {
        let mut stats = self.statistics.write().await;
        update_fn(&mut stats);
    }

    /// Get current statistics
    pub async fn statistics(&self) -> ObjectScannerStatistics {
        self.statistics.read().await.clone()
    }

    /// Reset statistics
    pub async fn reset_statistics(&self) {
        let mut stats = self.statistics.write().await;
        *stats = ObjectScannerStatistics::default();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use std::fs::File;
    use std::io::Write;

    #[tokio::test]
    async fn test_object_scanner_creation() {
        let config = ObjectScannerConfig::default();
        let scanner = ObjectScanner::new(config);
        assert_eq!(scanner.statistics().await.objects_scanned, 0);
    }

    #[tokio::test]
    async fn test_content_type_detection() {
        let config = ObjectScannerConfig::default();
        let scanner = ObjectScanner::new(config);
        
        let path = Path::new("test.txt");
        assert_eq!(scanner.detect_content_type(path), "text/plain");
        
        let path = Path::new("test.json");
        assert_eq!(scanner.detect_content_type(path), "application/json");
        
        let path = Path::new("test.unknown");
        assert_eq!(scanner.detect_content_type(path), "application/octet-stream");
    }

    #[tokio::test]
    async fn test_object_scanning() {
        let temp_dir = TempDir::new().unwrap();
        let test_file = temp_dir.path().join("test.txt");
        
        // Create a test file
        let mut file = File::create(&test_file).unwrap();
        writeln!(file, "test content").unwrap();
        
        let config = ObjectScannerConfig::default();
        let scanner = ObjectScanner::new(config);
        
        let result = scanner.scan_object("test-bucket", "test.txt", None, &test_file).await.unwrap();
        
        assert!(result.success);
        assert_eq!(result.bucket, "test-bucket");
        assert_eq!(result.object, "test.txt");
        assert!(result.metadata.is_some());
        
        let metadata = result.metadata.unwrap();
        assert!(metadata.size > 0);
        assert_eq!(metadata.content_type, "text/plain");
    }
} 