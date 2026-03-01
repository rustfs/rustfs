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

use crate::TelemetryError;
use crate::cleaner::LogCleaner;
use crate::cleaner::types::FileMatchMode;
use crate::config::OtelConfig;
use crate::global::OBSERVABILITY_METRIC_ENABLED;
use crate::telemetry::filter::build_env_filter;
use metrics::counter;
use rustfs_config::observability::{
    DEFAULT_OBS_LOG_CLEANUP_INTERVAL_SECONDS, DEFAULT_OBS_LOG_COMPRESS_OLD_FILES, DEFAULT_OBS_LOG_COMPRESSED_FILE_RETENTION_DAYS,
    DEFAULT_OBS_LOG_DELETE_EMPTY_FILES, DEFAULT_OBS_LOG_DRY_RUN, DEFAULT_OBS_LOG_GZIP_COMPRESSION_LEVEL,
    DEFAULT_OBS_LOG_MAX_SINGLE_FILE_SIZE_BYTES, DEFAULT_OBS_LOG_MAX_TOTAL_SIZE_BYTES, DEFAULT_OBS_LOG_MIN_FILE_AGE_SECONDS,
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

use super::guard::OtelGuard;

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
        init_file_logging_internal(config, log_directory, logger_level, is_production)
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

    OBSERVABILITY_METRIC_ENABLED.set(false).ok();
    counter!("rustfs.start.total").increment(1);
    info!("Init stdout logging (level: {})", logger_level);

    OtelGuard {
        tracer_provider: None,
        meter_provider: None,
        logger_provider: None,
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
    let rotation = config
        .log_rotation_time
        .as_deref()
        .unwrap_or(DEFAULT_LOG_ROTATION_TIME)
        .to_lowercase();

    // Determine match mode from config, defaulting to Suffix
    let match_mode = match config.log_match_mode.as_deref().map(|s| s.to_lowercase()).as_deref() {
        Some("prefix") => FileMatchMode::Prefix,
        _ => FileMatchMode::Suffix,
    };

    use tracing_appender::rolling::{RollingFileAppender, Rotation};
    let file_appender = {
        let rotation = match rotation.as_str() {
            "minutely" => Rotation::MINUTELY,
            "hourly" => Rotation::HOURLY,
            _ => Rotation::DAILY,
        };

        let mut builder = RollingFileAppender::builder()
            .rotation(rotation)
            .max_log_files(keep_files * 2); // Make sure there are some data files to archive to avoid premature deletion

        match match_mode {
            FileMatchMode::Prefix => builder = builder.filename_prefix(log_filename),
            FileMatchMode::Suffix => builder = builder.filename_suffix(log_filename),
        }

        builder
            .build(log_directory)
            .expect("failed to initialize rolling file appender")
    };

    let (non_blocking, guard) = tracing_appender::non_blocking(file_appender);

    // ── 4. Build subscriber layers ────────────────────────────────────────────
    let env_filter = build_env_filter(logger_level, None);
    let span_events = if is_production { FmtSpan::CLOSE } else { FmtSpan::FULL };

    // File layer writes JSON without ANSI codes.
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
    // unconditionally in non-production environments.
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

    OBSERVABILITY_METRIC_ENABLED.set(false).ok();

    // ── 5. Start background cleanup task ─────────────────────────────────────
    let cleanup_handle = spawn_cleanup_task(config, log_directory, log_filename, keep_files);

    info!(
        "Init file logging at '{}', rotation: {}, keep {} files",
        log_directory, rotation, keep_files
    );

    Ok(OtelGuard {
        tracer_provider: None,
        meter_provider: None,
        logger_provider: None,
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
#[cfg(unix)]
fn ensure_dir_permissions(log_directory: &str) -> Result<(), TelemetryError> {
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

// ─── Cleanup task ─────────────────────────────────────────────────────────────

/// Spawn a background task that periodically cleans up old log files.
///
/// All cleanup parameters are derived from [`OtelConfig`] fields, with
/// sensible defaults when fields are absent.  The task runs on the current
/// Tokio runtime and should be aborted (via the returned `JoinHandle`) when
/// the application shuts down.
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
fn spawn_cleanup_task(
    config: &OtelConfig,
    log_directory: &str,
    log_filename: &str,
    keep_files: usize,
) -> tokio::task::JoinHandle<()> {
    let log_dir = std::path::PathBuf::from(log_directory);
    // Use suffix matching for log files like "2026-03-01-06-21.rustfs.log"
    // where "rustfs.log" is the suffix.
    let file_pattern = config.log_filename.as_deref().unwrap_or(log_filename).to_string();

    // Determine match mode from config, defaulting to Suffix
    let match_mode = match config.log_match_mode.as_deref().map(|s| s.to_lowercase()).as_deref() {
        Some("prefix") => FileMatchMode::Prefix,
        _ => FileMatchMode::Suffix,
    };

    let keep_count = config.log_keep_count.unwrap_or(keep_files);
    let max_total_size = config
        .log_max_total_size_bytes
        .unwrap_or(DEFAULT_OBS_LOG_MAX_TOTAL_SIZE_BYTES * keep_count as u64);
    let max_single_file_size = config
        .log_max_single_file_size_bytes
        .unwrap_or(DEFAULT_OBS_LOG_MAX_SINGLE_FILE_SIZE_BYTES);
    let compress = config.log_compress_old_files.unwrap_or(DEFAULT_OBS_LOG_COMPRESS_OLD_FILES);
    let gzip_level = config
        .log_gzip_compression_level
        .unwrap_or(DEFAULT_OBS_LOG_GZIP_COMPRESSION_LEVEL);
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

    let cleaner = Arc::new(LogCleaner::new(
        log_dir,
        file_pattern,
        match_mode,
        keep_count,
        max_total_size,
        max_single_file_size,
        compress,
        gzip_level,
        retention_days,
        exclude_patterns,
        delete_empty,
        min_age,
        dry_run,
    ));

    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(cleanup_interval));
        loop {
            interval.tick().await;
            let cleaner_clone = cleaner.clone();
            let result = tokio::task::spawn_blocking(move || cleaner_clone.cleanup()).await;

            match result {
                Ok(Ok(_)) => {} // Success
                Ok(Err(e)) => tracing::warn!("Log cleanup failed: {}", e),
                Err(e) => tracing::warn!("Log cleanup task panicked: {}", e),
            }
        }
    })
}
