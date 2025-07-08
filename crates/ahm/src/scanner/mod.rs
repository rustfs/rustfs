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

//! Scanner subsystem for the AHM system
//!
//! The scanner is responsible for:
//! - Efficient object and disk scanning
//! - Health monitoring and issue detection
//! - Data usage statistics collection
//! - Bandwidth and resource management

pub mod engine;
pub mod metrics_collector;
pub mod object_scanner;
pub mod disk_scanner;
pub mod bandwidth_limiter;

pub use engine::{Engine, EngineConfig};
pub use metrics_collector::{MetricsCollector, MetricsConfig};
pub use object_scanner::{ObjectScanner, ObjectScannerConfig, ObjectScanResult, ObjectMetadata};
pub use disk_scanner::{DiskScanner, DiskScannerConfig};
pub use bandwidth_limiter::{BandwidthLimiter, BandwidthConfig, BandwidthStatistics};

use std::time::{Duration, Instant};
use serde::{Deserialize, Serialize};
// use serde only where needed

// --- Enums ---

/// Defines the severity level of a health issue.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Severity {
    Low,
    Medium,
    High,
    Critical,
}

/// Type of health issue that can be detected.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum HealthIssueType {
    MissingReplica,
    CorruptedData,
    ChecksumMismatch,
    QuorumNotMet,
    DiskFull,
    DiskReadError,
    DiskWriteError,
    NetworkError,
    MetadataMismatch,
    ObjectTooSmall,
    ObjectTooLarge,
    PolicyViolation,
    EncryptionError,
    ReplicationLag,
    Unknown,
}

/// Basic scan mode enum used by ScanStrategy variants
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ScanMode {
    Scheduled,
    Manual,
}

/// Scanner engine status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Status {
    /// Scanner is initializing
    Initializing,
    /// Scanner is running normally
    Running,
    /// Scanner is paused
    Paused,
    /// Scanner is stopping
    Stopping,
    /// Scanner has stopped
    Stopped,
    /// Scanner encountered an error
    Error,
}

// --- Structs ---

/// A health issue detected during scanning
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HealthIssue {
    /// Type of health issue
    pub issue_type: HealthIssueType,
    /// Severity level of the issue
    pub severity: Severity,
    /// Bucket name where the issue was found
    pub bucket: String,
    /// Object name where the issue was found
    pub object: String,
    /// Human-readable description of the issue
    pub description: String,
    /// Additional metadata about the issue
    pub metadata: Option<serde_json::Value>,
}

/// A summary report of a completed scan.
#[derive(Debug, Clone)]
pub struct ScanReport {
    pub scan_id: String,
    pub status: String,
    pub summary: String,
    pub issues_found: u64,
}

/// Scanner configuration.
#[derive(Debug, Clone, Default)]
pub struct ScannerConfig {
    // Configuration for the scanner engine
}

/// Scanner engine status.
#[derive(Debug, Clone, Default)]
pub struct ScannerStatus {
    pub is_running: bool,
    pub last_scan_time: Option<Instant>,
    pub total_scans: u64,
    pub failed_scans: u64,
    pub error_message: Option<String>,
}

/// Result of a single health check on an object.
#[derive(Debug, Clone)]
pub struct HealthCheckResult {
    pub bucket: String,
    pub object: String,
    pub version_id: Option<String>,
    pub metadata: Option<ObjectMetadata>,
    pub issues_detected: Vec<HealthIssue>,
}

/// Scanner statistics and performance metrics
#[derive(Debug, Clone, Default)]
pub struct ScannerStatistics {
    /// Total objects scanned in current cycle
    pub objects_scanned: u64,
    /// Total bytes scanned in current cycle
    pub bytes_scanned: u64,
    /// Number of health issues detected
    pub issues_detected: u64,
    /// Current scan rate (objects per second)
    pub scan_rate_ops: f64,
    /// Current scan rate (bytes per second)
    pub scan_rate_bps: f64,
    /// Time elapsed in current scan cycle
    pub elapsed_time: Duration,
    /// Estimated time remaining
    pub estimated_remaining: Option<Duration>,
    /// Last scan cycle completion time
    pub last_cycle_completed: Option<Instant>,
    /// Average cycle duration
    pub avg_cycle_duration: Option<Duration>,
}

/// Scan scope configuration
#[derive(Debug, Clone)]
pub struct ScanScopeConfig {
    /// Specific buckets to scan (None = all buckets)
    pub buckets: Option<Vec<String>>,
    /// Object prefix filter
    pub object_prefix: Option<String>,
    /// Include hidden/system objects
    pub include_system_objects: bool,
    /// Maximum number of objects to scan
    pub max_objects: Option<u64>,
    /// Scan only objects modified after this time
    pub modified_after: Option<Instant>,
}

impl Default for ScanScopeConfig {
    fn default() -> Self {
        Self {
            buckets: None,
            object_prefix: None,
            include_system_objects: false,
            max_objects: None,
            modified_after: None,
        }
    }
}

/// Scanner configuration for different scan strategies
#[derive(Debug, Clone)]
pub enum ScanStrategy {
    /// Full scan of all objects
    Full {
        /// Scan mode to use
        mode: ScanMode,
        /// Scan scope
        scope: ScanScopeConfig,
    },
    /// Incremental scan based on last scan timestamp
    Incremental {
        /// Reference timestamp for incremental scan
        since: Instant,
        /// Scan mode to use
        mode: ScanMode,
    },
    /// Smart scan that adaptively chooses objects to scan
    Smart {
        /// Probability of scanning each object (0.0 to 1.0)
        sample_rate: f64,
        /// Prioritize objects that haven't been scanned recently
        favor_unscanned: bool,
    },
    /// Targeted scan of specific objects
    Targeted {
        /// Specific objects to scan
        targets: Vec<ObjectTarget>,
        /// Scan mode to use
        mode: ScanMode,
    },
}

/// Target for a specific object scan
#[derive(Debug, Clone)]
pub struct ObjectTarget {
    pub bucket: String,
    pub object: String,
    pub version_id: Option<String>,
} 