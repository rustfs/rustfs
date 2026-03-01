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

// Observability Keys

mod metrics;
pub use metrics::*;

pub const ENV_OBS_ENDPOINT: &str = "RUSTFS_OBS_ENDPOINT";
pub const ENV_OBS_TRACE_ENDPOINT: &str = "RUSTFS_OBS_TRACE_ENDPOINT";
pub const ENV_OBS_METRIC_ENDPOINT: &str = "RUSTFS_OBS_METRIC_ENDPOINT";
pub const ENV_OBS_LOG_ENDPOINT: &str = "RUSTFS_OBS_LOG_ENDPOINT";
pub const ENV_OBS_USE_STDOUT: &str = "RUSTFS_OBS_USE_STDOUT";
pub const ENV_OBS_SAMPLE_RATIO: &str = "RUSTFS_OBS_SAMPLE_RATIO";
pub const ENV_OBS_METER_INTERVAL: &str = "RUSTFS_OBS_METER_INTERVAL";
pub const ENV_OBS_SERVICE_NAME: &str = "RUSTFS_OBS_SERVICE_NAME";
pub const ENV_OBS_SERVICE_VERSION: &str = "RUSTFS_OBS_SERVICE_VERSION";
pub const ENV_OBS_ENVIRONMENT: &str = "RUSTFS_OBS_ENVIRONMENT";

/// Per-signal enable/disable flags (default: true)
pub const ENV_OBS_TRACES_EXPORT_ENABLED: &str = "RUSTFS_OBS_TRACES_EXPORT_ENABLED";
pub const ENV_OBS_METRICS_EXPORT_ENABLED: &str = "RUSTFS_OBS_METRICS_EXPORT_ENABLED";
pub const ENV_OBS_LOGS_EXPORT_ENABLED: &str = "RUSTFS_OBS_LOGS_EXPORT_ENABLED";

pub const ENV_OBS_LOGGER_LEVEL: &str = "RUSTFS_OBS_LOGGER_LEVEL";
pub const ENV_OBS_LOG_STDOUT_ENABLED: &str = "RUSTFS_OBS_LOG_STDOUT_ENABLED";
pub const ENV_OBS_LOG_DIRECTORY: &str = "RUSTFS_OBS_LOG_DIRECTORY";
pub const ENV_OBS_LOG_FILENAME: &str = "RUSTFS_OBS_LOG_FILENAME";
pub const ENV_OBS_LOG_ROTATION_SIZE_MB: &str = "RUSTFS_OBS_LOG_ROTATION_SIZE_MB";
pub const ENV_OBS_LOG_ROTATION_TIME: &str = "RUSTFS_OBS_LOG_ROTATION_TIME";
pub const ENV_OBS_LOG_KEEP_FILES: &str = "RUSTFS_OBS_LOG_KEEP_FILES";

/// Log cleanup related configurations
pub const ENV_OBS_LOG_KEEP_COUNT: &str = "RUSTFS_OBS_LOG_KEEP_COUNT";
pub const ENV_OBS_LOG_MAX_TOTAL_SIZE_BYTES: &str = "RUSTFS_OBS_LOG_MAX_TOTAL_SIZE_BYTES";
pub const ENV_OBS_LOG_MAX_SINGLE_FILE_SIZE_BYTES: &str = "RUSTFS_OBS_LOG_MAX_SINGLE_FILE_SIZE_BYTES";
pub const ENV_OBS_LOG_COMPRESS_OLD_FILES: &str = "RUSTFS_OBS_LOG_COMPRESS_OLD_FILES";
pub const ENV_OBS_LOG_GZIP_COMPRESSION_LEVEL: &str = "RUSTFS_OBS_LOG_GZIP_COMPRESSION_LEVEL";
pub const ENV_OBS_LOG_COMPRESSED_FILE_RETENTION_DAYS: &str = "RUSTFS_OBS_LOG_COMPRESSED_FILE_RETENTION_DAYS";
pub const ENV_OBS_LOG_EXCLUDE_PATTERNS: &str = "RUSTFS_OBS_LOG_EXCLUDE_PATTERNS";
pub const ENV_OBS_LOG_DELETE_EMPTY_FILES: &str = "RUSTFS_OBS_LOG_DELETE_EMPTY_FILES";
pub const ENV_OBS_LOG_MIN_FILE_AGE_SECONDS: &str = "RUSTFS_OBS_LOG_MIN_FILE_AGE_SECONDS";
pub const ENV_OBS_LOG_CLEANUP_INTERVAL_SECONDS: &str = "RUSTFS_OBS_LOG_CLEANUP_INTERVAL_SECONDS";
pub const ENV_OBS_LOG_DRY_RUN: &str = "RUSTFS_OBS_LOG_DRY_RUN";
pub const ENV_OBS_LOG_MATCH_MODE: &str = "RUSTFS_OBS_LOG_MATCH_MODE";

/// Default values for log cleanup
pub const DEFAULT_OBS_LOG_KEEP_COUNT: usize = 10;
pub const DEFAULT_OBS_LOG_MAX_TOTAL_SIZE_BYTES: u64 = 2 * 1024 * 1024 * 1024; // 2 GiB
pub const DEFAULT_OBS_LOG_MAX_SINGLE_FILE_SIZE_BYTES: u64 = 0; // No single file limit
pub const DEFAULT_OBS_LOG_COMPRESS_OLD_FILES: bool = true;
pub const DEFAULT_OBS_LOG_GZIP_COMPRESSION_LEVEL: u32 = 6;
pub const DEFAULT_OBS_LOG_COMPRESSED_FILE_RETENTION_DAYS: u64 = 30;
pub const DEFAULT_OBS_LOG_DELETE_EMPTY_FILES: bool = true;
pub const DEFAULT_OBS_LOG_MIN_FILE_AGE_SECONDS: u64 = 3600; // 1 hour
pub const DEFAULT_OBS_LOG_CLEANUP_INTERVAL_SECONDS: u64 = 6 * 3600; // 6 hours
pub const DEFAULT_OBS_LOG_DRY_RUN: bool = false;
pub const DEFAULT_OBS_LOG_MATCH_MODE: &str = "suffix";

/// Default values for observability configuration
// ### Supported Environment Values
// - `production` - Secure file-only logging
// - `development` - Full debugging with stdout
// - `test` - Test environment with stdout support
// - `staging` - Staging environment with stdout support
pub const DEFAULT_OBS_ENVIRONMENT_PRODUCTION: &str = "production";
pub const DEFAULT_OBS_ENVIRONMENT_DEVELOPMENT: &str = "development";
pub const DEFAULT_OBS_ENVIRONMENT_TEST: &str = "test";
pub const DEFAULT_OBS_ENVIRONMENT_STAGING: &str = "staging";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_env_keys() {
        assert_eq!(ENV_OBS_ENDPOINT, "RUSTFS_OBS_ENDPOINT");
        assert_eq!(ENV_OBS_TRACE_ENDPOINT, "RUSTFS_OBS_TRACE_ENDPOINT");
        assert_eq!(ENV_OBS_METRIC_ENDPOINT, "RUSTFS_OBS_METRIC_ENDPOINT");
        assert_eq!(ENV_OBS_LOG_ENDPOINT, "RUSTFS_OBS_LOG_ENDPOINT");
        assert_eq!(ENV_OBS_USE_STDOUT, "RUSTFS_OBS_USE_STDOUT");
        assert_eq!(ENV_OBS_SAMPLE_RATIO, "RUSTFS_OBS_SAMPLE_RATIO");
        assert_eq!(ENV_OBS_METER_INTERVAL, "RUSTFS_OBS_METER_INTERVAL");
        assert_eq!(ENV_OBS_SERVICE_NAME, "RUSTFS_OBS_SERVICE_NAME");
        assert_eq!(ENV_OBS_SERVICE_VERSION, "RUSTFS_OBS_SERVICE_VERSION");
        assert_eq!(ENV_OBS_ENVIRONMENT, "RUSTFS_OBS_ENVIRONMENT");
        assert_eq!(ENV_OBS_LOGGER_LEVEL, "RUSTFS_OBS_LOGGER_LEVEL");
        assert_eq!(ENV_OBS_LOG_STDOUT_ENABLED, "RUSTFS_OBS_LOG_STDOUT_ENABLED");
        assert_eq!(ENV_OBS_LOG_DIRECTORY, "RUSTFS_OBS_LOG_DIRECTORY");
        assert_eq!(ENV_OBS_LOG_FILENAME, "RUSTFS_OBS_LOG_FILENAME");
        assert_eq!(ENV_OBS_LOG_ROTATION_SIZE_MB, "RUSTFS_OBS_LOG_ROTATION_SIZE_MB");
        assert_eq!(ENV_OBS_LOG_ROTATION_TIME, "RUSTFS_OBS_LOG_ROTATION_TIME");
        assert_eq!(ENV_OBS_LOG_KEEP_FILES, "RUSTFS_OBS_LOG_KEEP_FILES");
        assert_eq!(ENV_OBS_TRACES_EXPORT_ENABLED, "RUSTFS_OBS_TRACES_EXPORT_ENABLED");
        assert_eq!(ENV_OBS_METRICS_EXPORT_ENABLED, "RUSTFS_OBS_METRICS_EXPORT_ENABLED");
        assert_eq!(ENV_OBS_LOGS_EXPORT_ENABLED, "RUSTFS_OBS_LOGS_EXPORT_ENABLED");
        // Test log cleanup related env keys
        assert_eq!(ENV_OBS_LOG_KEEP_COUNT, "RUSTFS_OBS_LOG_KEEP_COUNT");
        assert_eq!(ENV_OBS_LOG_MAX_TOTAL_SIZE_BYTES, "RUSTFS_OBS_LOG_MAX_TOTAL_SIZE_BYTES");
        assert_eq!(ENV_OBS_LOG_MAX_SINGLE_FILE_SIZE_BYTES, "RUSTFS_OBS_LOG_MAX_SINGLE_FILE_SIZE_BYTES");
        assert_eq!(ENV_OBS_LOG_COMPRESS_OLD_FILES, "RUSTFS_OBS_LOG_COMPRESS_OLD_FILES");
        assert_eq!(ENV_OBS_LOG_GZIP_COMPRESSION_LEVEL, "RUSTFS_OBS_LOG_GZIP_COMPRESSION_LEVEL");
        assert_eq!(
            ENV_OBS_LOG_COMPRESSED_FILE_RETENTION_DAYS,
            "RUSTFS_OBS_LOG_COMPRESSED_FILE_RETENTION_DAYS"
        );
        assert_eq!(ENV_OBS_LOG_EXCLUDE_PATTERNS, "RUSTFS_OBS_LOG_EXCLUDE_PATTERNS");
        assert_eq!(ENV_OBS_LOG_DELETE_EMPTY_FILES, "RUSTFS_OBS_LOG_DELETE_EMPTY_FILES");
        assert_eq!(ENV_OBS_LOG_MIN_FILE_AGE_SECONDS, "RUSTFS_OBS_LOG_MIN_FILE_AGE_SECONDS");
        assert_eq!(ENV_OBS_LOG_CLEANUP_INTERVAL_SECONDS, "RUSTFS_OBS_LOG_CLEANUP_INTERVAL_SECONDS");
        assert_eq!(ENV_OBS_LOG_DRY_RUN, "RUSTFS_OBS_LOG_DRY_RUN");
        assert_eq!(ENV_OBS_LOG_MATCH_MODE, "RUSTFS_OBS_LOG_MATCH_MODE");
    }

    #[test]
    fn test_default_values() {
        assert_eq!(DEFAULT_OBS_ENVIRONMENT_PRODUCTION, "production");
        assert_eq!(DEFAULT_OBS_ENVIRONMENT_DEVELOPMENT, "development");
        assert_eq!(DEFAULT_OBS_ENVIRONMENT_TEST, "test");
        assert_eq!(DEFAULT_OBS_ENVIRONMENT_STAGING, "staging");
        assert_eq!(DEFAULT_OBS_LOG_MATCH_MODE, "suffix");
    }
}
