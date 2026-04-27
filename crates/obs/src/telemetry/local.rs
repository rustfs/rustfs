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

//! Local logging backend: stdout-only or file-rolling with optional stdout mirror.
//!
//! # Behaviour
//!
//! | Condition                        | Result                                       |
//! |----------------------------------|----------------------------------------------|
//! | No log directory configured      | JSON logs written to **stdout only**         |
//! | Log directory configured         | JSON logs written to **rolling file**;       |
//! |                                  | stdout mirror enabled when `log_stdout_enabled` |
//! |                                  | is `true` or environment is non-production   |
//!
//! The function [`init_local_logging`] is the single entry point for both
//! cases; callers do **not** need to distinguish between stdout and file modes.
//!
//! The file-backed mode delegates retention and compression to
//! [`crate::cleaner`], which keeps the logging setup code focused on subscriber
//! construction while still allowing periodic housekeeping in the background.

use super::guard::OtelGuard;
use crate::TelemetryError;
use crate::cleaner::LogCleaner;
use crate::cleaner::types::{CompressionAlgorithm, FileMatchMode};
use crate::config::OtelConfig;
use crate::global::{METRIC_LOG_CLEANER_RUN_FAILURES_TOTAL, METRIC_LOG_CLEANER_RUNS_TOTAL, set_observability_metric_enabled};
use crate::telemetry::filter::build_env_filter;
use crate::telemetry::rolling::{RollingAppender, Rotation};
use metrics::counter;
use rustfs_config::observability::{
    DEFAULT_OBS_LOG_CLEANUP_INTERVAL_SECONDS, DEFAULT_OBS_LOG_COMPRESS_OLD_FILES, DEFAULT_OBS_LOG_COMPRESSED_FILE_RETENTION_DAYS,
    DEFAULT_OBS_LOG_COMPRESSION_ALGORITHM, DEFAULT_OBS_LOG_DELETE_EMPTY_FILES, DEFAULT_OBS_LOG_DRY_RUN,
    DEFAULT_OBS_LOG_GZIP_COMPRESSION_LEVEL, DEFAULT_OBS_LOG_MATCH_MODE, DEFAULT_OBS_LOG_MAX_SINGLE_FILE_SIZE_BYTES,
    DEFAULT_OBS_LOG_MAX_TOTAL_SIZE_BYTES, DEFAULT_OBS_LOG_MIN_FILE_AGE_SECONDS, DEFAULT_OBS_LOG_PARALLEL_COMPRESS,
    DEFAULT_OBS_LOG_PARALLEL_WORKERS, DEFAULT_OBS_LOG_ZSTD_COMPRESSION_LEVEL, DEFAULT_OBS_LOG_ZSTD_FALLBACK_TO_GZIP,
    DEFAULT_OBS_LOG_ZSTD_WORKERS,
};
use rustfs_config::{APP_NAME, DEFAULT_LOG_KEEP_FILES, DEFAULT_LOG_ROTATION_TIME, DEFAULT_OBS_LOG_STDOUT_ENABLED};
use std::sync::Arc;
use std::{fs, io::IsTerminal, time::Duration};
use tracing::info;
use tracing_error::ErrorLayer;
use tracing_subscriber::{
    fmt::{format::FmtSpan, time::LocalTime},
    layer::SubscriberExt,
    util::SubscriberInitExt,
};

/// Initialize local logging (stdout-only or file-rolling).
///
/// When `log_directory` is empty or `None` in the config the function sets up
/// a non-blocking JSON subscriber that writes to **stdout** and returns
/// immediately — no file I/O, no cleanup task.
///
/// When a log directory is provided the function additionally:
/// 1. Creates the directory (including on Unix, enforces `0755` permissions).
/// 2. Attaches a rolling-file appender (daily or hourly based on
///    `log_rotation_time`).
/// 3. Optionally mirrors output to stdout based on `log_stdout_enabled`.
/// 4. Spawns a background cleanup task that periodically removes or compresses
///    old log files according to the cleanup configuration in [`OtelConfig`].
///
/// # Arguments
/// * `config` - Observability configuration, fully populated from environment variables.
/// * `logger_level` - Effective log level string (e.g., `"info"`).
/// * `is_production` - Whether the runtime environment is production; controls
///   span verbosity and stdout mirroring defaults.
///
/// # Returns
/// An [`OtelGuard`] that keeps the `tracing_appender` worker alive and holds
/// a handle to the cleanup task (if started). Dropping the guard flushes
/// in-flight logs and stops the cleanup task.
///
/// # Errors
/// Returns [`TelemetryError`] if the log directory cannot be created or its
/// permissions cannot be set (Unix only).
pub(super) fn init_local_logging(
    config: &OtelConfig,
    logger_level: &str,
    is_production: bool,
) -> Result<OtelGuard, TelemetryError> {
    // Determine the effective log directory.  An absent or empty value means
    // stdout-only mode: we skip file setup entirely.
    let log_dir_str = config.log_directory.as_deref().filter(|s| !s.is_empty());

    if let Some(log_directory) = log_dir_str {
        match init_file_logging_internal(config, log_directory, logger_level, is_production) {
            Ok(guard) => Ok(guard),
            Err(error) if should_fallback_to_stdout(&error) => {
                emit_file_logging_fallback_warning(log_directory, &error);
                Ok(init_stdout_only(config, logger_level, is_production))
            }
            Err(error) => Err(error),
        }
    } else {
        Ok(init_stdout_only(config, logger_level, is_production))
    }
}

// ─── Stdout-only ─────────────────────────────────────────────────────────────

/// Set up a non-blocking stdout JSON subscriber with no file I/O.
///
/// Used when no log directory has been configured.  The subscriber formats
/// every log record as a JSON line, including RFC-3339 timestamps, thread
/// identifiers, file/line information, and span context.
///
/// # Arguments
/// * `_config` - Unused at the moment; reserved for future configuration.
/// * `logger_level` - Effective log level string.
/// * `is_production` - Controls span event verbosity.
fn init_stdout_only(_config: &OtelConfig, logger_level: &str, is_production: bool) -> OtelGuard {
    let env_filter = build_env_filter(logger_level, None);
    let (nb, guard) = tracing_appender::non_blocking(std::io::stdout());

    // Keep stdout formatting JSON-shaped even in local-only mode so operators
    // can ship the same log schema to external collectors if needed.
    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_timer(LocalTime::rfc_3339())
        .with_target(true)
        .with_ansi(std::io::stdout().is_terminal())
        .with_thread_names(true)
        .with_thread_ids(true)
        .with_file(true)
        .with_line_number(true)
        .with_writer(nb)
        .json()
        .with_current_span(true)
        .with_span_list(true)
        .with_span_events(if is_production { FmtSpan::CLOSE } else { FmtSpan::FULL });

    tracing_subscriber::registry()
        .with(env_filter)
        .with(ErrorLayer::default())
        .with(fmt_layer)
        .init();

    set_observability_metric_enabled(false);
    counter!("rustfs_start_total").increment(1);
    info!("Init stdout logging (level: {})", logger_level);

    OtelGuard {
        tracer_provider: None,
        meter_provider: None,
        logger_provider: None,
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        profiling_agent: None,
        tracing_guard: Some(guard),
        stdout_guard: None,
        cleanup_handle: None,
    }
}

// ─── File-rolling ─────────────────────────────────────────────────────────────

/// Internal implementation for file-based rolling log setup.
///
/// Called by [`init_local_logging`] when a log directory is present.
/// Handles directory creation, permission enforcement (Unix), file appender
/// setup, optional stdout mirror, and log-cleanup task spawning.
///
/// The function intentionally performs all fallible filesystem preparation
/// before registering the subscriber so startup failures are reported early and
/// do not leave partially initialized tracing state behind.
fn init_file_logging_internal(
    config: &OtelConfig,
    log_directory: &str,
    logger_level: &str,
    is_production: bool,
) -> Result<OtelGuard, TelemetryError> {
    let service_name = config.service_name.as_deref().unwrap_or(APP_NAME);
    let log_filename = config.log_filename.as_deref().unwrap_or(service_name);
    let keep_files = config.log_keep_files.unwrap_or(DEFAULT_LOG_KEEP_FILES);

    // ── 1. Ensure the log directory exists ───────────────────────────────────
    if let Err(e) = fs::create_dir_all(log_directory) {
        return Err(TelemetryError::Io(e.to_string()));
    }

    // ── 2. Enforce directory permissions (Unix only) ─────────────────────────
    #[cfg(unix)]
    ensure_dir_permissions(log_directory)?;

    // ── 3. Choose rotation strategy ──────────────────────────────────────────
    // `log_rotation_time` drives the rolling-appender rotation period.
    let rotation_str = config
        .log_rotation_time
        .as_deref()
        .unwrap_or(DEFAULT_LOG_ROTATION_TIME)
        .to_lowercase();

    // Match mode controls how the rolling filename is recognized later by the
    // cleaner. Suffix mode fits timestamp-prefixed filenames especially well.
    let match_mode = FileMatchMode::from_config_str(config.log_match_mode.as_deref().unwrap_or(DEFAULT_OBS_LOG_MATCH_MODE));

    let rotation = match rotation_str.as_str() {
        "minutely" => Rotation::Minutely,
        "hourly" => Rotation::Hourly,
        "daily" => Rotation::Daily,
        _ => Rotation::Daily,
    };

    let max_single_file_size = config
        .log_max_single_file_size_bytes
        .unwrap_or(DEFAULT_OBS_LOG_MAX_SINGLE_FILE_SIZE_BYTES);

    let file_appender =
        RollingAppender::new(log_directory, log_filename.to_string(), rotation, max_single_file_size, match_mode)?;

    let (non_blocking, guard) = tracing_appender::non_blocking(file_appender);

    // ── 4. Build subscriber layers ────────────────────────────────────────────
    let env_filter = build_env_filter(logger_level, None);
    let span_events = if is_production { FmtSpan::CLOSE } else { FmtSpan::FULL };

    // File output stays machine-readable and free of ANSI sequences so the
    // resulting files are safe to parse or ship to log processors.
    let file_layer = tracing_subscriber::fmt::layer()
        .with_timer(LocalTime::rfc_3339())
        .with_target(true)
        .with_ansi(false)
        .with_thread_names(true)
        .with_thread_ids(true)
        .with_file(true)
        .with_line_number(true)
        .with_writer(non_blocking)
        .json()
        .with_current_span(true)
        .with_span_list(true)
        .with_span_events(span_events.clone());

    // Optional stdout mirror: enabled explicitly via `log_stdout_enabled`, or
    // unconditionally in non-production environments so developers still see
    // immediate terminal output while file rotation remains enabled.
    let (stdout_layer, stdout_guard) = if config.log_stdout_enabled.unwrap_or(DEFAULT_OBS_LOG_STDOUT_ENABLED) || !is_production {
        let (stdout_nb, stdout_guard) = tracing_appender::non_blocking(std::io::stdout());
        let enable_color = std::io::stdout().is_terminal();
        (
            Some(
                tracing_subscriber::fmt::layer()
                    .with_timer(LocalTime::rfc_3339())
                    .with_target(true)
                    .with_ansi(enable_color)
                    .with_thread_names(true)
                    .with_thread_ids(true)
                    .with_file(true)
                    .with_line_number(true)
                    .with_writer(stdout_nb) // .json()
                    // .with_current_span(true)
                    // .with_span_list(true)
                    .with_span_events(span_events),
            ),
            Some(stdout_guard),
        )
    } else {
        (None, None)
    };

    tracing_subscriber::registry()
        .with(env_filter)
        .with(ErrorLayer::default())
        .with(file_layer)
        .with(stdout_layer)
        .init();

    set_observability_metric_enabled(false);

    // ── 5. Start background cleanup task ─────────────────────────────────────
    let cleanup_handle = spawn_cleanup_task(config, log_directory, log_filename, keep_files);

    info!(
        "Init file logging at '{}', rotation: {}, keep {} files",
        log_directory, rotation_str, keep_files
    );

    Ok(OtelGuard {
        tracer_provider: None,
        meter_provider: None,
        logger_provider: None,
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        profiling_agent: None,
        tracing_guard: Some(guard),
        stdout_guard,
        cleanup_handle: Some(cleanup_handle),
    })
}

// ─── Directory permissions (Unix) ─────────────────────────────────────────────

/// Ensure the log directory has at most `0755` permissions (Unix only).
///
/// Tightens permissions to `0755` if the directory is more permissive.
/// This prevents world-writable log directories from being a security hazard.
/// No-ops if permissions are already `0755` or stricter.
///
/// The function never broadens permissions; it is strictly a hardening step.
#[cfg(unix)]
pub fn ensure_dir_permissions(log_directory: &str) -> Result<(), TelemetryError> {
    use std::fs::Permissions;
    use std::os::unix::fs::PermissionsExt;

    let desired: u32 = 0o755;
    match fs::metadata(log_directory) {
        Ok(meta) => {
            let current = meta.permissions().mode() & 0o777;
            // Only tighten to 0755 if existing permissions are looser than target.
            if (current & !desired) != 0 {
                if let Err(e) = fs::set_permissions(log_directory, Permissions::from_mode(desired)) {
                    return Err(TelemetryError::SetPermissions(format!(
                        "dir='{log_directory}', want={desired:#o}, have={current:#o}, err={e}"
                    )));
                }
                // Second verification pass to confirm the change took effect.
                if let Ok(meta2) = fs::metadata(log_directory) {
                    let after = meta2.permissions().mode() & 0o777;
                    if after != desired {
                        return Err(TelemetryError::SetPermissions(format!(
                            "dir='{log_directory}', want={desired:#o}, after={after:#o}"
                        )));
                    }
                }
            }
            Ok(())
        }
        Err(e) => Err(TelemetryError::Io(format!("stat '{log_directory}' failed: {e}"))),
    }
}

pub(super) fn should_fallback_to_stdout(error: &TelemetryError) -> bool {
    match error {
        TelemetryError::SetPermissions(_) => true,
        TelemetryError::Io(message) => {
            let message = message.to_ascii_lowercase();
            message.contains("permission denied") || message.contains("os error 13")
        }
        _ => false,
    }
}

pub(super) fn emit_file_logging_fallback_warning(log_directory: &str, error: &TelemetryError) {
    eprintln!(
        "[WARN] Failed to initialize file observability logging at '{}': {}. Falling back to stdout logging.",
        log_directory, error
    );
}

// ─── Cleanup task ─────────────────────────────────────────────────────────────

/// Spawn a background task that periodically cleans up old log files.
///
/// All cleanup parameters are derived from [`OtelConfig`] fields, with
/// sensible defaults when fields are absent.  The task runs on the current
/// Tokio runtime and should be aborted (via the returned `JoinHandle`) when
/// the application shuts down.
///
/// The asynchronous loop itself remains lightweight: each cleanup pass is
/// delegated to `spawn_blocking` because directory traversal, compression, and
/// deletion are inherently blocking filesystem operations.
///
/// # Arguments
/// * `config` - Observability config containing cleanup parameters.
/// * `log_directory` - Directory path of the rolling log files.
/// * `log_filename` - Base filename (used as the file prefix for matching).
/// * `keep_files` - Legacy keep-files count; used as fallback when the new
///   `log_keep_count` field is absent.
///
/// # Returns
/// A [`tokio::task::JoinHandle`] for the spawned cleanup loop.
pub fn spawn_cleanup_task(
    config: &OtelConfig,
    log_directory: &str,
    log_filename: &str,
    keep_files: usize,
) -> tokio::task::JoinHandle<()> {
    let log_dir = std::path::PathBuf::from(log_directory);
    // Use suffix matching for log files like `2026-03-01-06-21.rustfs.log`
    // where `rustfs.log` is the stable suffix generated by the rolling appender.
    let file_pattern = config.log_filename.as_deref().unwrap_or(log_filename).to_string();
    let active_filename = file_pattern.clone();

    // Determine match mode from config, defaulting to the repository-wide
    // observability setting when the caller leaves it unset.
    let match_mode = FileMatchMode::from_config_str(config.log_match_mode.as_deref().unwrap_or(DEFAULT_OBS_LOG_MATCH_MODE));

    let max_total_size = config
        .log_max_total_size_bytes
        .unwrap_or(DEFAULT_OBS_LOG_MAX_TOTAL_SIZE_BYTES);
    let max_single_file_size = config
        .log_max_single_file_size_bytes
        .unwrap_or(DEFAULT_OBS_LOG_MAX_SINGLE_FILE_SIZE_BYTES);
    let compress = config.log_compress_old_files.unwrap_or(DEFAULT_OBS_LOG_COMPRESS_OLD_FILES);
    let gzip_level = config
        .log_gzip_compression_level
        .unwrap_or(DEFAULT_OBS_LOG_GZIP_COMPRESSION_LEVEL);
    let compression_algorithm = CompressionAlgorithm::from_config_str(
        config
            .log_compression_algorithm
            .as_deref()
            .unwrap_or(DEFAULT_OBS_LOG_COMPRESSION_ALGORITHM),
    );
    let parallel_compress = config.log_parallel_compress.unwrap_or(DEFAULT_OBS_LOG_PARALLEL_COMPRESS);
    let parallel_workers = config.log_parallel_workers.unwrap_or(DEFAULT_OBS_LOG_PARALLEL_WORKERS);
    let zstd_level = config
        .log_zstd_compression_level
        .unwrap_or(DEFAULT_OBS_LOG_ZSTD_COMPRESSION_LEVEL);
    let zstd_fallback_to_gzip = config
        .log_zstd_fallback_to_gzip
        .unwrap_or(DEFAULT_OBS_LOG_ZSTD_FALLBACK_TO_GZIP);
    let zstd_workers = config.log_zstd_workers.unwrap_or(DEFAULT_OBS_LOG_ZSTD_WORKERS);
    let retention_days = config
        .log_compressed_file_retention_days
        .unwrap_or(DEFAULT_OBS_LOG_COMPRESSED_FILE_RETENTION_DAYS);
    let exclude_patterns = config
        .log_exclude_patterns
        .as_deref()
        .map(|s| s.split(',').map(|p| p.trim().to_string()).collect())
        .unwrap_or_default();
    let delete_empty = config.log_delete_empty_files.unwrap_or(DEFAULT_OBS_LOG_DELETE_EMPTY_FILES);
    let min_age = config
        .log_min_file_age_seconds
        .unwrap_or(DEFAULT_OBS_LOG_MIN_FILE_AGE_SECONDS);
    let dry_run = config.log_dry_run.unwrap_or(DEFAULT_OBS_LOG_DRY_RUN);
    let cleanup_interval = config
        .log_cleanup_interval_seconds
        .unwrap_or(DEFAULT_OBS_LOG_CLEANUP_INTERVAL_SECONDS);

    let cleaner = Arc::new(
        LogCleaner::builder(log_dir, file_pattern, active_filename)
            .match_mode(match_mode)
            .keep_files(keep_files)
            .max_total_size_bytes(max_total_size)
            .max_single_file_size_bytes(max_single_file_size)
            .compress_old_files(compress)
            .gzip_compression_level(gzip_level)
            // Compression behavior stays fully config-driven, but the builder
            // clamps unsafe numeric values and preserves sensible defaults.
            .compression_algorithm(compression_algorithm)
            .parallel_compress(parallel_compress)
            .parallel_workers(parallel_workers)
            .zstd_compression_level(zstd_level)
            .zstd_fallback_to_gzip(zstd_fallback_to_gzip)
            .zstd_workers(zstd_workers)
            .compressed_file_retention_days(retention_days)
            .exclude_patterns(exclude_patterns)
            .delete_empty_files(delete_empty)
            .min_file_age_seconds(min_age)
            .dry_run(dry_run)
            .build(),
    );

    info!(
        compression_algorithm = %compression_algorithm,
        parallel_compress,
        parallel_workers,
        zstd_level,
        zstd_fallback_to_gzip,
        zstd_workers,
        "log cleaner compression profile configured"
    );

    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(cleanup_interval));
        loop {
            // Wait for the next scheduled tick before dispatching another pass.
            // The blocking filesystem work runs on a dedicated blocking thread.
            interval.tick().await;
            let cleaner_clone = cleaner.clone();
            let result = tokio::task::spawn_blocking(move || cleaner_clone.cleanup()).await;

            match result {
                Ok(Ok(_)) => {
                    counter!(METRIC_LOG_CLEANER_RUNS_TOTAL).increment(1);
                }
                Ok(Err(e)) => {
                    counter!(METRIC_LOG_CLEANER_RUN_FAILURES_TOTAL).increment(1);
                    tracing::warn!("Log cleanup failed: {}", e);
                }
                Err(e) => {
                    counter!(METRIC_LOG_CLEANER_RUN_FAILURES_TOTAL).increment(1);
                    tracing::warn!("Log cleanup task panicked: {}", e);
                }
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::OtelConfig;
    use tempfile::tempdir;

    #[test]
    /// Invalid file names should be reported as errors instead of panicking.
    fn test_init_file_logging_invalid_filename_does_not_panic() {
        let temp_dir = tempdir().expect("create temp dir");
        let temp_path = temp_dir.path().to_str().expect("temp dir path is utf-8");
        let config = OtelConfig {
            log_filename: Some("invalid\0name.log".to_string()),
            ..OtelConfig::default()
        };

        // We must run within a Tokio runtime because init_file_logging_internal spawns a background task.
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let result = init_file_logging_internal(&config, temp_path, "info", true);
            // With eager file opening, an invalid filename (null byte) causes the OS to reject
            // the open() call, so the function returns Err instead of panicking.
            assert!(result.is_err(), "invalid filename must return Err, not panic");
        });
    }

    #[test]
    fn test_permission_denied_errors_fall_back_to_stdout() {
        assert!(should_fallback_to_stdout(&TelemetryError::Io(
            "Permission denied (os error 13)".to_string()
        )));
        assert!(should_fallback_to_stdout(&TelemetryError::SetPermissions(
            "dir='/logs', want=0o755, have=0o777, err=Permission denied (os error 13)".to_string()
        )));
        assert!(!should_fallback_to_stdout(&TelemetryError::Io(
            "No such file or directory (os error 2)".to_string()
        )));
    }
}
