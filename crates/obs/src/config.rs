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

//! Observability configuration for RustFS.
//!
//! All configuration is read from environment variables. The canonical list of
//! variable names and their defaults lives in `rustfs-config/src/observability/mod.rs`.
//!
//! Two public structs are provided:
//! - [`OtelConfig`] — the primary flat configuration that drives every backend.
//! - [`AppConfig`] — a thin wrapper used when the config is embedded inside a
//!   larger application configuration struct.

use rustfs_config::observability::{
    DEFAULT_OBS_ENVIRONMENT_PRODUCTION, DEFAULT_OBS_LOG_CLEANUP_INTERVAL_SECONDS, DEFAULT_OBS_LOG_COMPRESS_OLD_FILES,
    DEFAULT_OBS_LOG_COMPRESSED_FILE_RETENTION_DAYS, DEFAULT_OBS_LOG_DELETE_EMPTY_FILES, DEFAULT_OBS_LOG_DRY_RUN,
    DEFAULT_OBS_LOG_GZIP_COMPRESSION_LEVEL, DEFAULT_OBS_LOG_KEEP_COUNT, DEFAULT_OBS_LOG_MATCH_MODE,
    DEFAULT_OBS_LOG_MAX_SINGLE_FILE_SIZE_BYTES, DEFAULT_OBS_LOG_MAX_TOTAL_SIZE_BYTES, DEFAULT_OBS_LOG_MIN_FILE_AGE_SECONDS,
    ENV_OBS_ENDPOINT, ENV_OBS_ENVIRONMENT, ENV_OBS_LOG_CLEANUP_INTERVAL_SECONDS, ENV_OBS_LOG_COMPRESS_OLD_FILES,
    ENV_OBS_LOG_COMPRESSED_FILE_RETENTION_DAYS, ENV_OBS_LOG_DELETE_EMPTY_FILES, ENV_OBS_LOG_DIRECTORY, ENV_OBS_LOG_DRY_RUN,
    ENV_OBS_LOG_ENDPOINT, ENV_OBS_LOG_EXCLUDE_PATTERNS, ENV_OBS_LOG_FILENAME, ENV_OBS_LOG_GZIP_COMPRESSION_LEVEL,
    ENV_OBS_LOG_KEEP_COUNT, ENV_OBS_LOG_KEEP_FILES, ENV_OBS_LOG_MATCH_MODE, ENV_OBS_LOG_MAX_SINGLE_FILE_SIZE_BYTES,
    ENV_OBS_LOG_MAX_TOTAL_SIZE_BYTES, ENV_OBS_LOG_MIN_FILE_AGE_SECONDS, ENV_OBS_LOG_ROTATION_TIME, ENV_OBS_LOG_STDOUT_ENABLED,
    ENV_OBS_LOGGER_LEVEL, ENV_OBS_LOGS_EXPORT_ENABLED, ENV_OBS_METER_INTERVAL, ENV_OBS_METRIC_ENDPOINT,
    ENV_OBS_METRICS_EXPORT_ENABLED, ENV_OBS_SAMPLE_RATIO, ENV_OBS_SERVICE_NAME, ENV_OBS_SERVICE_VERSION, ENV_OBS_TRACE_ENDPOINT,
    ENV_OBS_TRACES_EXPORT_ENABLED, ENV_OBS_USE_STDOUT,
};
use rustfs_config::{
    APP_NAME, DEFAULT_LOG_KEEP_FILES, DEFAULT_LOG_LEVEL, DEFAULT_LOG_ROTATION_TIME, DEFAULT_OBS_LOG_FILENAME,
    DEFAULT_OBS_LOG_STDOUT_ENABLED, DEFAULT_OBS_LOGS_EXPORT_ENABLED, DEFAULT_OBS_METRICS_EXPORT_ENABLED,
    DEFAULT_OBS_TRACES_EXPORT_ENABLED, ENVIRONMENT, METER_INTERVAL, SAMPLE_RATIO, SERVICE_VERSION, USE_STDOUT,
};
use rustfs_utils::{get_env_bool, get_env_f64, get_env_opt_str, get_env_str, get_env_u64, get_env_usize};
use serde::{Deserialize, Serialize};
use std::env;

/// Full observability configuration used by all telemetry backends.
///
/// Fields are grouped into three logical sections:
///
/// ## OpenTelemetry / OTLP export
/// Controls whether and where traces, metrics, and logs are exported over the
/// wire using the OTLP/HTTP protocol.
///
/// ## Local logging
/// Controls the rolling-file appender: directory, filename, rotation policy,
/// and the number of files to retain.
///
/// ## Log cleanup
/// Controls the background cleanup task: size limits, compression, retention
/// of compressed archives, exclusion patterns, and dry-run mode.
///
/// # Design Notes
///
/// - All fields are `Option<T>` to allow partial configuration via environment
///   variables with sensible defaults provided by constants in `rustfs-config`.
/// - `log_keep_count` represents the cleaner's minimum retention; `log_keep_files`
///   controls the rolling-appender's file limit (both typically set to the same value).
///
/// # Example
/// ```no_run
/// use rustfs_obs::OtelConfig;
///
/// // Build from environment variables (typical production usage).
/// let config = OtelConfig::new();
///
/// // Build with an explicit OTLP endpoint.
/// let config = OtelConfig::extract_otel_config_from_env(
///     Some("http://otel-collector:4318".to_string())
/// );
/// ```
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct OtelConfig {
    // ── OTLP export ──────────────────────────────────────────────────────────
    /// Root OTLP/HTTP endpoint (e.g. `http://otel-collector:4318`).
    /// Per-signal endpoints below take precedence when set.
    pub endpoint: String,
    /// Dedicated trace endpoint; overrides `endpoint` + `/v1/traces` fallback.
    pub trace_endpoint: Option<String>,
    /// Dedicated metrics endpoint; overrides `endpoint` + `/v1/metrics` fallback.
    pub metric_endpoint: Option<String>,
    /// Dedicated log endpoint; overrides `endpoint` + `/v1/logs` fallback.
    pub log_endpoint: Option<String>,
    /// Whether to export distributed traces (default: `true`).
    pub traces_export_enabled: Option<bool>,
    /// Whether to export metrics (default: `true`).
    pub metrics_export_enabled: Option<bool>,
    /// Whether to export logs via OTLP (default: `true`).
    pub logs_export_enabled: Option<bool>,
    /// **[OTLP-only]** Mirror all signals to stdout in addition to OTLP export.
    /// Only applies when an OTLP endpoint is configured.
    pub use_stdout: Option<bool>,
    /// Fraction of traces to sample, `0.0`–`1.0` (default: `0.1`).
    pub sample_ratio: Option<f64>,
    /// Metrics export interval in seconds (default: `15`).
    pub meter_interval: Option<u64>,
    /// OTel `service.name` attribute (default: `APP_NAME`).
    pub service_name: Option<String>,
    /// OTel `service.version` attribute (default: `SERVICE_VERSION`).
    pub service_version: Option<String>,
    /// Deployment environment tag, e.g. `production` or `development`.
    pub environment: Option<String>,

    // ── Local logging ─────────────────────────────────────────────────────────
    /// Minimum log level directive (default: `info`).
    /// Respects `RUST_LOG` syntax when set via environment.
    pub logger_level: Option<String>,
    /// When `true`, a stdout JSON layer is always attached regardless of the
    /// active backend (default: `false` in production, `true` otherwise).
    pub log_stdout_enabled: Option<bool>,
    /// Directory where rolling log files are written.
    /// When absent or empty, logging falls back to stdout-only mode.
    pub log_directory: Option<String>,
    /// Base name for log files (without date suffix), e.g. `rustfs`.
    /// Used for both rolling-appender naming and cleanup scanning.
    pub log_filename: Option<String>,
    /// Rotation time granularity: `"hourly"` or `"daily"` (default: `"daily"`).
    pub log_rotation_time: Option<String>,
    /// Number of rolling log files to retain (default: `30`).
    /// The rolling-appender will delete the oldest file when this limit is exceeded.
    pub log_keep_files: Option<usize>,

    // ── Log cleanup ───────────────────────────────────────────────────────────
    /// Minimum number of files the cleaner must always preserve.
    /// Typically set to the same value as `log_keep_files`.
    pub log_keep_count: Option<usize>,
    /// Hard ceiling on the total size (bytes) of all log files (default: 2 GiB).
    pub log_max_total_size_bytes: Option<u64>,
    /// Per-file size ceiling (bytes); `0` means unlimited (default: `0`).
    pub log_max_single_file_size_bytes: Option<u64>,
    /// Compress eligible files with gzip before deletion (default: `true`).
    pub log_compress_old_files: Option<bool>,
    /// Gzip compression level `1`–`9` (default: `6`).
    pub log_gzip_compression_level: Option<u32>,
    /// Delete compressed archives older than this many days; `0` = keep forever
    /// (default: `30`).
    pub log_compressed_file_retention_days: Option<u64>,
    /// Comma-separated glob patterns for files that must never be cleaned up.
    pub log_exclude_patterns: Option<String>,
    /// Delete zero-byte log files during cleanup (default: `true`).
    pub log_delete_empty_files: Option<bool>,
    /// A file younger than this many seconds is never touched (default: `3600`).
    pub log_min_file_age_seconds: Option<u64>,
    /// How often the background cleanup task runs, in seconds (default: `21600`).
    pub log_cleanup_interval_seconds: Option<u64>,
    /// Log what *would* be deleted without actually removing anything
    /// (default: `false`).
    pub log_dry_run: Option<bool>,
    /// File matching mode: "prefix" or "suffix" (default: "suffix").
    pub log_match_mode: Option<String>,
}

impl OtelConfig {
    /// Build an [`OtelConfig`] from environment variables.
    ///
    /// The optional `endpoint` argument sets the root OTLP endpoint. If it is
    /// `None` or an empty string the value is read from the
    /// `RUSTFS_OBS_ENDPOINT` environment variable instead.
    ///
    /// When no endpoint is configured at all, `use_stdout` is forced to `true`
    /// so that logs are still visible during development.
    ///
    /// # Example
    /// ```no_run
    /// use rustfs_obs::OtelConfig;
    ///
    /// // Read everything from env vars.
    /// let config = OtelConfig::extract_otel_config_from_env(None);
    ///
    /// // Override the endpoint programmatically.
    /// let config = OtelConfig::extract_otel_config_from_env(
    ///     Some("http://localhost:4318".to_string())
    /// );
    /// ```
    pub fn extract_otel_config_from_env(endpoint: Option<String>) -> OtelConfig {
        let endpoint = match endpoint {
            Some(ep) if !ep.is_empty() => ep,
            _ => env::var(ENV_OBS_ENDPOINT).unwrap_or_default(),
        };

        // Force stdout when there is no remote endpoint so that operators
        // always have *some* log output in the default configuration.
        let use_stdout = if endpoint.is_empty() {
            true
        } else {
            get_env_bool(ENV_OBS_USE_STDOUT, USE_STDOUT)
        };

        // The canonical log directory is resolved only when explicitly set via
        // environment variable. When absent or empty, logging falls back to
        // stdout-only mode (not file-rolling).
        let log_directory = match std::env::var(ENV_OBS_LOG_DIRECTORY) {
            Ok(val) if !val.is_empty() => Some(val),
            _ => None,
        };

        // `log_keep_files` (legacy) and `log_keep_count` (new) share the same
        // environment variables but have slightly different semantics.
        // `log_keep_files` is the rolling-appender retention count; `log_keep_count`
        // is the cleaner's minimum-keep threshold.  Both default to the same value.
        let log_keep_files = Some(get_env_usize(ENV_OBS_LOG_KEEP_FILES, DEFAULT_LOG_KEEP_FILES));
        let log_keep_count = Some(get_env_usize(ENV_OBS_LOG_KEEP_COUNT, DEFAULT_OBS_LOG_KEEP_COUNT));

        // `log_rotation_time` drives the rolling-appender rotation period.
        let log_rotation_time = Some(get_env_str(ENV_OBS_LOG_ROTATION_TIME, DEFAULT_LOG_ROTATION_TIME));

        OtelConfig {
            // OTLP
            endpoint,
            trace_endpoint: get_env_opt_str(ENV_OBS_TRACE_ENDPOINT),
            metric_endpoint: get_env_opt_str(ENV_OBS_METRIC_ENDPOINT),
            log_endpoint: get_env_opt_str(ENV_OBS_LOG_ENDPOINT),
            traces_export_enabled: Some(get_env_bool(ENV_OBS_TRACES_EXPORT_ENABLED, DEFAULT_OBS_TRACES_EXPORT_ENABLED)),
            metrics_export_enabled: Some(get_env_bool(ENV_OBS_METRICS_EXPORT_ENABLED, DEFAULT_OBS_METRICS_EXPORT_ENABLED)),
            logs_export_enabled: Some(get_env_bool(ENV_OBS_LOGS_EXPORT_ENABLED, DEFAULT_OBS_LOGS_EXPORT_ENABLED)),
            use_stdout: Some(use_stdout),
            sample_ratio: Some(get_env_f64(ENV_OBS_SAMPLE_RATIO, SAMPLE_RATIO)),
            meter_interval: Some(get_env_u64(ENV_OBS_METER_INTERVAL, METER_INTERVAL)),
            service_name: Some(get_env_str(ENV_OBS_SERVICE_NAME, APP_NAME)),
            service_version: Some(get_env_str(ENV_OBS_SERVICE_VERSION, SERVICE_VERSION)),
            environment: Some(get_env_str(ENV_OBS_ENVIRONMENT, ENVIRONMENT)),
            // Local logging
            logger_level: Some(get_env_str(ENV_OBS_LOGGER_LEVEL, DEFAULT_LOG_LEVEL)),
            log_stdout_enabled: Some(get_env_bool(ENV_OBS_LOG_STDOUT_ENABLED, DEFAULT_OBS_LOG_STDOUT_ENABLED)),
            log_directory,
            log_filename: Some(get_env_str(ENV_OBS_LOG_FILENAME, DEFAULT_OBS_LOG_FILENAME)),
            log_rotation_time,
            log_keep_files,
            // Log cleanup
            log_keep_count,
            log_max_total_size_bytes: Some(get_env_u64(ENV_OBS_LOG_MAX_TOTAL_SIZE_BYTES, DEFAULT_OBS_LOG_MAX_TOTAL_SIZE_BYTES)),
            log_max_single_file_size_bytes: Some(get_env_u64(
                ENV_OBS_LOG_MAX_SINGLE_FILE_SIZE_BYTES,
                DEFAULT_OBS_LOG_MAX_SINGLE_FILE_SIZE_BYTES,
            )),
            log_compress_old_files: Some(get_env_bool(ENV_OBS_LOG_COMPRESS_OLD_FILES, DEFAULT_OBS_LOG_COMPRESS_OLD_FILES)),
            log_gzip_compression_level: Some(get_env_u64(
                ENV_OBS_LOG_GZIP_COMPRESSION_LEVEL,
                DEFAULT_OBS_LOG_GZIP_COMPRESSION_LEVEL as u64,
            ) as u32),
            log_compressed_file_retention_days: Some(get_env_u64(
                ENV_OBS_LOG_COMPRESSED_FILE_RETENTION_DAYS,
                DEFAULT_OBS_LOG_COMPRESSED_FILE_RETENTION_DAYS,
            )),
            log_exclude_patterns: get_env_opt_str(ENV_OBS_LOG_EXCLUDE_PATTERNS),
            log_delete_empty_files: Some(get_env_bool(ENV_OBS_LOG_DELETE_EMPTY_FILES, DEFAULT_OBS_LOG_DELETE_EMPTY_FILES)),
            log_min_file_age_seconds: Some(get_env_u64(ENV_OBS_LOG_MIN_FILE_AGE_SECONDS, DEFAULT_OBS_LOG_MIN_FILE_AGE_SECONDS)),
            log_cleanup_interval_seconds: Some(get_env_u64(
                ENV_OBS_LOG_CLEANUP_INTERVAL_SECONDS,
                DEFAULT_OBS_LOG_CLEANUP_INTERVAL_SECONDS,
            )),
            log_dry_run: Some(get_env_bool(ENV_OBS_LOG_DRY_RUN, DEFAULT_OBS_LOG_DRY_RUN)),
            log_match_mode: Some(get_env_str(ENV_OBS_LOG_MATCH_MODE, DEFAULT_OBS_LOG_MATCH_MODE)),
        }
    }

    /// Create a new [`OtelConfig`] populated entirely from environment variables.
    ///
    /// Equivalent to `OtelConfig::extract_otel_config_from_env(None)`.
    ///
    /// # Example
    /// ```no_run
    /// use rustfs_obs::OtelConfig;
    /// let config = OtelConfig::new();
    /// ```
    pub fn new() -> Self {
        Self::extract_otel_config_from_env(None)
    }
}

impl Default for OtelConfig {
    fn default() -> Self {
        Self::new()
    }
}

/// Top-level application configuration that embeds [`OtelConfig`].
///
/// Use this when the observability config lives inside a larger `AppConfig`
/// struct, e.g. when deserialising from a config file that also contains other
/// application settings.
///
/// # Example
/// ```
/// use rustfs_obs::AppConfig;
///
/// let config = AppConfig::new_with_endpoint(None);
/// ```
#[derive(Debug, Deserialize, Clone)]
pub struct AppConfig {
    pub observability: OtelConfig,
}

impl AppConfig {
    /// Create an [`AppConfig`] with all observability settings read from the
    /// environment (no explicit endpoint override).
    pub fn new() -> Self {
        Self {
            observability: OtelConfig::default(),
        }
    }

    /// Create an [`AppConfig`] with an explicit OTLP endpoint.
    ///
    /// # Arguments
    /// * `endpoint` - Root OTLP/HTTP endpoint URL, or `None` to read from env.
    ///
    /// # Example
    /// ```no_run
    /// use rustfs_obs::AppConfig;
    ///
    /// let config = AppConfig::new_with_endpoint(Some("http://localhost:4318".to_string()));
    /// ```
    pub fn new_with_endpoint(endpoint: Option<String>) -> Self {
        Self {
            observability: OtelConfig::extract_otel_config_from_env(endpoint),
        }
    }
}

impl Default for AppConfig {
    fn default() -> Self {
        Self::new()
    }
}

/// Returns `true` when the current runtime environment is `production`.
///
/// Reads the `RUSTFS_OBS_ENVIRONMENT` environment variable and compares it
/// case-insensitively against the string `"production"`.
pub fn is_production_environment() -> bool {
    get_env_str(ENV_OBS_ENVIRONMENT, ENVIRONMENT).eq_ignore_ascii_case(DEFAULT_OBS_ENVIRONMENT_PRODUCTION)
}
