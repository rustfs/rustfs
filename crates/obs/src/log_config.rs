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

//! Log system configuration module
//!
//! Supports loading from TOML files or constructing directly in code.
//! Most fields have reasonable production defaults.

use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Complete log system configuration structure
///
/// Supports loading from TOML files and direct code construction.
/// Most fields have reasonable production defaults.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct LogConfig {
    /// Log file storage directory (relative or absolute path)
    /// Default: "./logs"
    pub log_dir: PathBuf,

    /// Log file name prefix (rolling will automatically add date/time suffix)
    /// Example: "app.log." → generates app.log.2026-02-28, etc.
    /// Default: "app.log."
    pub file_prefix: String,

    /// Minimum number of log files to keep (try to keep this many even if total size exceeds limit)
    /// Default: 10
    pub keep_count: usize,

    /// Maximum total size of log directory (bytes), 0 means no size limit
    /// Example: 2GB = 2 * 1024 * 1024 * 1024
    /// Default: 2GB
    pub max_total_size_bytes: u64,

    /// Maximum size of a single log file (bytes), 0 means no limit
    /// When a file exceeds this size, it becomes a candidate for cleanup
    /// Example: 100MB = 100 * 1024 * 1024
    /// Default: 0 (no single file limit)
    pub max_single_file_size_bytes: u64,

    /// Whether to gzip compress old log files before deletion
    /// When enabled, generates .gz files and removes the original files
    /// Default: true
    pub compress_old_files: bool,

    /// Gzip compression level (1-9)
    /// 1 = fastest, lowest compression; 9 = slowest, highest compression
    /// Default: 6 (balances speed and compression ratio)
    pub gzip_compression_level: u32,

    /// Retention period for compressed files (days)
    /// Compressed files older than this will be permanently deleted
    /// 0 means keep forever
    /// Default: 30 days
    pub compressed_file_retention_days: u64,

    /// File patterns to exclude from cleanup (glob patterns)
    /// Example: vec!["*.lock".to_string(), "current.log".to_string()]
    /// These files will never be compressed or deleted
    /// Default: empty (no exclusions)
    pub exclude_patterns: Vec<String>,

    /// Whether to delete empty log files during cleanup
    /// Default: true
    pub delete_empty_files: bool,

    /// Minimum age (seconds) before a file can be cleaned up
    /// Files modified more recently than this will not be touched
    /// Default: 3600 (1 hour)
    pub min_file_age_seconds: u64,

    /// Log file rotation period
    /// Supported values: "daily" (daily), "hourly" (hourly), "minutely" (every minute, for testing)
    /// Default: "daily"
    pub rotation: String,

    /// OpenTelemetry Collector / Jaeger / Tempo OTLP gRPC endpoint
    /// Example: "http://localhost:4317" or "https://otel-collector:4317"
    /// Set to None or empty string to disable OTEL export, falling back to stdout exporter
    /// Default: None (distributed tracing not enforced)
    pub otel_endpoint: Option<String>,

    /// Trace sampling ratio (0.0 ~ 1.0)
    /// 0.0 = no sampling, 1.0 = full sampling
    /// Production recommendation: 0.01 ~ 0.2
    /// Default: 0.1 (10% sampling)
    pub trace_sample_ratio: f64,

    /// Default log level (will be overridden by RUST_LOG environment variable)
    /// Supported values: trace, debug, info, warn, error
    /// Default: "info"
    pub log_level: String,

    /// Cleanup task execution interval (seconds)
    /// Default: 21600 (6 hours)
    /// Recommended range: 1800 (30 minutes) ~ 86400 (24 hours)
    pub cleanup_interval_seconds: u64,

    /// Enable dry-run mode for cleanup (log what would be deleted without actually deleting)
    /// Useful for testing and validation
    /// Default: false
    pub dry_run: bool,
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            log_dir: PathBuf::from("./logs"),
            file_prefix: "app.log.".to_string(),
            keep_count: 10,
            max_total_size_bytes: 2 * 1024 * 1024 * 1024, // 2 GiB
            max_single_file_size_bytes: 0,                // No single file limit
            compress_old_files: true,
            gzip_compression_level: 6,
            compressed_file_retention_days: 30, // Keep compressed files for 30 days
            exclude_patterns: Vec::new(),
            delete_empty_files: true,
            min_file_age_seconds: 3600, // 1 hour
            rotation: "daily".to_string(),
            otel_endpoint: None,
            trace_sample_ratio: 0.1,
            log_level: "info".to_string(),
            cleanup_interval_seconds: 6 * 3600, // 6 hours
            dry_run: false,
        }
    }
}

impl LogConfig {
    /// Load configuration from TOML file
    /// If file does not exist or parsing fails, returns default configuration
    pub fn load_or_default(path: &str) -> Self {
        match std::fs::read_to_string(path)
            .ok()
            .and_then(|content| toml::from_str(&content).ok())
        {
            Some(config) => config,
            None => {
                tracing::warn!("Unable to load config file {}, using default config", path);
                LogConfig::default()
            }
        }
    }
}
