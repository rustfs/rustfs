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

//! dial9-tokio-telemetry integration for RustFS.
//!
//! This module provides low-overhead Tokio runtime-level telemetry,
//! capturing events like PollStart/End, WorkerPark/Unpark, QueueSample, etc.

use crate::TelemetryError;
// Import and re-export TelemetryGuard for use in other crates (like rustfs)
// Use as Dial9TelemetryGuard internally to avoid naming conflicts
use dial9_tokio_telemetry::telemetry::RotatingWriter;
pub use dial9_tokio_telemetry::telemetry::TelemetryGuard;
use dial9_tokio_telemetry::telemetry::TelemetryGuard as Dial9TelemetryGuard;
// Use rustfs_config which re-exports runtime constants
use rustfs_config::{
    DEFAULT_RUNTIME_DIAL9_ENABLED, DEFAULT_RUNTIME_DIAL9_FILE_PREFIX, DEFAULT_RUNTIME_DIAL9_MAX_FILE_SIZE,
    DEFAULT_RUNTIME_DIAL9_OUTPUT_DIR, DEFAULT_RUNTIME_DIAL9_ROTATION_COUNT, DEFAULT_RUNTIME_DIAL9_SAMPLING_RATE,
    ENV_RUNTIME_DIAL9_ENABLED, ENV_RUNTIME_DIAL9_FILE_PREFIX, ENV_RUNTIME_DIAL9_MAX_FILE_SIZE, ENV_RUNTIME_DIAL9_OUTPUT_DIR,
    ENV_RUNTIME_DIAL9_ROTATION_COUNT, ENV_RUNTIME_DIAL9_S3_BUCKET, ENV_RUNTIME_DIAL9_S3_PREFIX, ENV_RUNTIME_DIAL9_SAMPLING_RATE,
};
use rustfs_utils::get_env_bool;
use rustfs_utils::get_env_opt_str;
use rustfs_utils::get_env_opt_u64;
use rustfs_utils::get_env_opt_usize;
use rustfs_utils::get_env_str;
use std::path::PathBuf;
use std::sync::OnceLock;
use std::sync::RwLock;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use tracing::{info, warn};

const LOG_COMPONENT_OBS: &str = "obs";
const LOG_SUBSYSTEM_DIAL9: &str = "dial9";
const EVENT_DIAL9_STATE: &str = "dial9_state";

#[derive(Debug, Clone, Default)]
pub(crate) struct Dial9RuntimeSnapshot {
    pub active_sessions: u64,
    pub errors_total: u64,
    pub disk_usage_bytes: u64,
}

#[derive(Debug)]
struct Dial9RuntimeState {
    enabled: AtomicBool,
    active_sessions: AtomicU64,
    errors_total: AtomicU64,
    output_dir: RwLock<Option<PathBuf>>,
    file_prefix: RwLock<Option<String>>,
}

impl Dial9RuntimeState {
    fn new() -> Self {
        Self {
            enabled: AtomicBool::new(false),
            active_sessions: AtomicU64::new(0),
            errors_total: AtomicU64::new(0),
            output_dir: RwLock::new(None),
            file_prefix: RwLock::new(None),
        }
    }

    fn record_config(&self, config: &Dial9Config) {
        self.enabled.store(config.enabled, Ordering::Relaxed);
        *self.output_dir.write().expect("dial9 output_dir lock should not be poisoned") = Some(PathBuf::from(&config.output_dir));
        *self
            .file_prefix
            .write()
            .expect("dial9 file_prefix lock should not be poisoned") = Some(config.file_prefix.clone());
        if !config.enabled {
            self.active_sessions.store(0, Ordering::Relaxed);
        }
    }

    fn record_runtime_started(&self, config: &Dial9Config) {
        self.record_config(config);
        self.active_sessions.store(1, Ordering::Relaxed);
    }

    fn record_runtime_error(&self, config: &Dial9Config) {
        self.record_config(config);
        self.active_sessions.store(0, Ordering::Relaxed);
        self.errors_total.fetch_add(1, Ordering::Relaxed);
    }

    fn snapshot(&self) -> Dial9RuntimeSnapshot {
        Dial9RuntimeSnapshot {
            active_sessions: self.active_sessions.load(Ordering::Relaxed),
            errors_total: self.errors_total.load(Ordering::Relaxed),
            disk_usage_bytes: self.current_disk_usage_bytes(),
        }
    }

    fn current_disk_usage_bytes(&self) -> u64 {
        let output_dir = self
            .output_dir
            .read()
            .expect("dial9 output_dir lock should not be poisoned")
            .clone();
        let file_prefix = self
            .file_prefix
            .read()
            .expect("dial9 file_prefix lock should not be poisoned")
            .clone();

        let Some(output_dir) = output_dir else {
            return 0;
        };
        let Some(file_prefix) = file_prefix else {
            return 0;
        };

        std::fs::read_dir(output_dir)
            .ok()
            .into_iter()
            .flatten()
            .filter_map(Result::ok)
            .filter_map(|entry| {
                let filename = entry.file_name();
                let filename = filename.to_str()?;
                filename
                    .starts_with(&file_prefix)
                    .then(|| entry.metadata().ok().map(|meta| meta.len()))
            })
            .flatten()
            .sum()
    }
}

fn dial9_runtime_state() -> &'static Dial9RuntimeState {
    static STATE: OnceLock<Dial9RuntimeState> = OnceLock::new();
    STATE.get_or_init(Dial9RuntimeState::new)
}

fn resolve_sampling_rate() -> (f64, bool) {
    match std::env::var(ENV_RUNTIME_DIAL9_SAMPLING_RATE) {
        Ok(raw) => {
            let trimmed = raw.trim();
            if trimmed.is_empty() {
                return (DEFAULT_RUNTIME_DIAL9_SAMPLING_RATE, false);
            }

            match trimmed.parse::<f64>() {
                Ok(value) if value.is_finite() => (value.clamp(0.0, 1.0), true),
                _ => {
                    warn!(
                        event = EVENT_DIAL9_STATE,
                        component = LOG_COMPONENT_OBS,
                        subsystem = LOG_SUBSYSTEM_DIAL9,
                        result = "invalid_sampling_rate",
                        provided = trimmed,
                        fallback = DEFAULT_RUNTIME_DIAL9_SAMPLING_RATE,
                        "dial9 state changed"
                    );
                    (DEFAULT_RUNTIME_DIAL9_SAMPLING_RATE, true)
                }
            }
        }
        Err(_) => (DEFAULT_RUNTIME_DIAL9_SAMPLING_RATE, false),
    }
}

fn warn_ignored_runtime_knobs(config: &Dial9Config, sampling_rate_explicit: bool) {
    if config.s3_bucket.is_some() || config.s3_prefix.is_some() {
        warn!(
            event = EVENT_DIAL9_STATE,
            component = LOG_COMPONENT_OBS,
            subsystem = LOG_SUBSYSTEM_DIAL9,
            result = "unsupported_s3_upload_knobs_ignored",
            s3_bucket = config.s3_bucket.as_deref().unwrap_or(""),
            s3_prefix = config.s3_prefix.as_deref().unwrap_or(""),
            "dial9 state changed"
        );
    }

    if sampling_rate_explicit {
        warn!(
            event = EVENT_DIAL9_STATE,
            component = LOG_COMPONENT_OBS,
            subsystem = LOG_SUBSYSTEM_DIAL9,
            result = "unsupported_sampling_rate_ignored",
            sampling_rate = config.sampling_rate,
            "dial9 state changed"
        );
    }
}

fn sanitize_dial9_max_file_size(bytes: u64) -> u64 {
    bytes.max(1)
}

fn sanitize_dial9_rotation_count(count: usize) -> usize {
    count.max(1)
}

fn dial9_total_rotation_size(max_file_size: u64, rotation_count: usize) -> u64 {
    max_file_size.saturating_mul(u64::try_from(rotation_count).unwrap_or(u64::MAX))
}

/// Configuration for dial9 Tokio telemetry.
#[derive(Debug, Clone)]
pub struct Dial9Config {
    /// Whether dial9 telemetry is enabled
    pub enabled: bool,

    /// Directory where trace files are written
    pub output_dir: String,

    /// Prefix for trace file names
    pub file_prefix: String,

    /// Maximum size of each trace file in bytes
    pub max_file_size: u64,

    /// Number of rotated files to keep
    pub rotation_count: usize,

    /// Optional S3 bucket for uploading trace files
    pub s3_bucket: Option<String>,

    /// Optional S3 prefix for uploaded files
    pub s3_prefix: Option<String>,

    /// Sampling rate (0.0 to 1.0)
    pub sampling_rate: f64,
}

impl Default for Dial9Config {
    fn default() -> Self {
        Self {
            enabled: DEFAULT_RUNTIME_DIAL9_ENABLED,
            output_dir: DEFAULT_RUNTIME_DIAL9_OUTPUT_DIR.to_string(),
            file_prefix: DEFAULT_RUNTIME_DIAL9_FILE_PREFIX.to_string(),
            max_file_size: DEFAULT_RUNTIME_DIAL9_MAX_FILE_SIZE,
            rotation_count: DEFAULT_RUNTIME_DIAL9_ROTATION_COUNT,
            s3_bucket: None,
            s3_prefix: None,
            sampling_rate: DEFAULT_RUNTIME_DIAL9_SAMPLING_RATE,
        }
    }
}

impl Dial9Config {
    /// Create configuration from environment variables.
    pub fn from_env() -> Self {
        let enabled = get_env_bool(ENV_RUNTIME_DIAL9_ENABLED, DEFAULT_RUNTIME_DIAL9_ENABLED);

        if !enabled {
            let config = Self::default();
            dial9_runtime_state().record_config(&config);
            return config;
        }

        let raw_max_file_size = get_env_opt_u64(ENV_RUNTIME_DIAL9_MAX_FILE_SIZE);
        let raw_rotation_count = get_env_opt_usize(ENV_RUNTIME_DIAL9_ROTATION_COUNT);
        let (sampling_rate, sampling_rate_explicit) = resolve_sampling_rate();
        let max_file_size = sanitize_dial9_max_file_size(raw_max_file_size.unwrap_or(DEFAULT_RUNTIME_DIAL9_MAX_FILE_SIZE));
        let rotation_count = sanitize_dial9_rotation_count(raw_rotation_count.unwrap_or(DEFAULT_RUNTIME_DIAL9_ROTATION_COUNT));
        if raw_max_file_size == Some(0) {
            warn!(
                event = EVENT_DIAL9_STATE,
                component = LOG_COMPONENT_OBS,
                subsystem = LOG_SUBSYSTEM_DIAL9,
                result = "invalid_max_file_size",
                fallback_bytes = 1_u64,
                "dial9 state changed"
            );
        }
        if raw_rotation_count == Some(0) {
            warn!(
                event = EVENT_DIAL9_STATE,
                component = LOG_COMPONENT_OBS,
                subsystem = LOG_SUBSYSTEM_DIAL9,
                result = "invalid_rotation_count",
                fallback_count = 1_usize,
                "dial9 state changed"
            );
        }

        let config = Self {
            enabled,
            output_dir: get_env_str(ENV_RUNTIME_DIAL9_OUTPUT_DIR, DEFAULT_RUNTIME_DIAL9_OUTPUT_DIR),
            file_prefix: get_env_str(ENV_RUNTIME_DIAL9_FILE_PREFIX, DEFAULT_RUNTIME_DIAL9_FILE_PREFIX),
            max_file_size,
            rotation_count,
            s3_bucket: get_env_opt_str(ENV_RUNTIME_DIAL9_S3_BUCKET).filter(|s| !s.is_empty()),
            s3_prefix: get_env_opt_str(ENV_RUNTIME_DIAL9_S3_PREFIX).filter(|s| !s.is_empty()),
            sampling_rate,
        };
        warn_ignored_runtime_knobs(&config, sampling_rate_explicit);
        dial9_runtime_state().record_config(&config);
        config
    }

    /// Get the base path for trace files.
    pub fn base_path(&self) -> PathBuf {
        PathBuf::from(&self.output_dir).join(&self.file_prefix)
    }
}

/// Guard for dial9 telemetry session.
///
/// When dropped, this guard will flush any remaining telemetry data.
/// Keep it alive for the duration of your application.
pub struct Dial9SessionGuard {
    /// The underlying dial9 telemetry guard (if enabled)
    _guard: Option<Dial9TelemetryGuard>,
    /// Configuration
    #[allow(dead_code)]
    config: Dial9Config,
}

impl Dial9SessionGuard {
    /// Create a new dial9 session guard.
    ///
    /// Note: This only validates configuration and creates the output directory.
    /// The actual telemetry session is created when building the Tokio runtime
    /// via `build_traced_runtime()`.
    ///
    /// Returns `Ok(None)` if dial9 is disabled.
    pub async fn new(config: Dial9Config) -> Result<Option<Self>, TelemetryError> {
        if !config.enabled {
            info!(
                event = EVENT_DIAL9_STATE,
                component = LOG_COMPONENT_OBS,
                subsystem = LOG_SUBSYSTEM_DIAL9,
                state = "disabled",
                "dial9 state changed"
            );
            return Ok(None);
        }

        info!(
            event = EVENT_DIAL9_STATE,
            component = LOG_COMPONENT_OBS,
            subsystem = LOG_SUBSYSTEM_DIAL9,
            state = "validating",
            output_dir = %config.output_dir,
            file_prefix = %config.file_prefix,
            sampling_rate = config.sampling_rate,
            "dial9 state changed"
        );

        // Only create directory; writer will be created in build_traced_runtime
        if let Err(e) = tokio::fs::create_dir_all(&config.output_dir).await {
            warn!(
                event = EVENT_DIAL9_STATE,
                component = LOG_COMPONENT_OBS,
                subsystem = LOG_SUBSYSTEM_DIAL9,
                result = "output_dir_create_failed",
                output_dir = %config.output_dir,
                error = %e,
                fallback = "disabled",
                "dial9 state changed"
            );
            return Ok(None);
        }

        info!(
            event = EVENT_DIAL9_STATE,
            component = LOG_COMPONENT_OBS,
            subsystem = LOG_SUBSYSTEM_DIAL9,
            state = "validated",
            output_dir = %config.output_dir,
            file_prefix = %config.file_prefix,
            "dial9 state changed"
        );

        dial9_runtime_state().record_config(&config);
        Ok(Some(Self { _guard: None, config }))
    }

    /// Set the telemetry guard (called after runtime creation)
    #[allow(dead_code)]
    pub(crate) fn set_guard(&mut self, guard: Dial9TelemetryGuard) {
        self._guard = Some(guard);
        dial9_runtime_state().record_runtime_started(&self.config);
    }

    /// Check if this guard has an active session.
    pub fn is_active(&self) -> bool {
        dial9_runtime_state().snapshot().active_sessions > 0
    }

    /// Flush any pending telemetry data.
    pub async fn shutdown(&self) {
        if self.is_active() {
            info!(
                event = EVENT_DIAL9_STATE,
                component = LOG_COMPONENT_OBS,
                subsystem = LOG_SUBSYSTEM_DIAL9,
                state = "shutdown_requested",
                result = "runtime_guard_managed_elsewhere",
                "dial9 state"
            );
        }
    }
}

impl Drop for Dial9SessionGuard {
    fn drop(&mut self) {
        if let Some(_guard) = &self._guard {
            // TelemetryGuard flushes automatically when dropped
            info!(
                event = EVENT_DIAL9_STATE,
                component = LOG_COMPONENT_OBS,
                subsystem = LOG_SUBSYSTEM_DIAL9,
                state = "flushed",
                "dial9 state"
            );
        }
    }
}

/// Initialize dial9 telemetry session from environment configuration.
///
/// This function reads configuration from environment variables and creates
/// a dial9 session guard if enabled. The guard should be kept alive for the
/// duration of the application.
///
/// # Returns
///
/// - `Ok(Some(guard))` - Dial9 is enabled and session initialized
/// - `Ok(None)` - Dial9 is disabled or failed to initialize (non-fatal)
/// - `Err(e)` - Fatal error (should not happen with current implementation)
pub async fn init_session() -> Result<Option<Dial9SessionGuard>, TelemetryError> {
    let config = Dial9Config::from_env();
    Dial9SessionGuard::new(config).await
}

/// Check if dial9 telemetry is enabled via environment configuration.
pub fn is_enabled() -> bool {
    get_env_bool(ENV_RUNTIME_DIAL9_ENABLED, DEFAULT_RUNTIME_DIAL9_ENABLED)
}

/// Build a Tokio runtime with dial9 telemetry enabled.
///
/// This function creates a Tokio runtime with dial9 telemetry integrated
/// if enabled via environment variables. Returns a tuple of (Runtime, TelemetryGuard).
///
/// This is internal API used by the runtime builder.
///
/// # Arguments
///
/// * `builder` - The configured Tokio runtime builder
///
/// # Returns
///
/// * `Ok((runtime, guard))` - Successfully created runtime with telemetry
/// * `Err` - If runtime creation fails or dial9 is enabled but fails to initialize
///
/// # Errors
///
/// Returns an error if:
/// - Dial9 is enabled but the runtime builder fails
/// - Dial9 is enabled but writer creation fails
pub fn build_traced_runtime(
    builder: tokio::runtime::Builder,
) -> Result<(tokio::runtime::Runtime, Dial9TelemetryGuard), TelemetryError> {
    if !is_enabled() {
        return Err(TelemetryError::Io("Dial9 is not enabled".to_string()));
    }

    let config = Dial9Config::from_env();
    dial9_runtime_state().record_config(&config);

    // Ensure the output directory exists before creating the writer
    std::fs::create_dir_all(&config.output_dir).map_err(|e| {
        dial9_runtime_state().record_runtime_error(&config);
        TelemetryError::Io(format!("Failed to create dial9 output directory '{}': {}", config.output_dir, e))
    })?;

    // Create rotating writer (synchronous for runtime building)
    let base_path = config.base_path();
    let writer = RotatingWriter::new(
        base_path,
        config.max_file_size,
        dial9_total_rotation_size(config.max_file_size, config.rotation_count),
    )
    .map_err(|e| {
        dial9_runtime_state().record_runtime_error(&config);
        TelemetryError::Io(format!("Failed to create RotatingWriter: {}", e))
    })?;

    // Build traced runtime
    // Note: sampling_rate and S3 upload settings are reserved for future use
    // once the dial9 library provides support for those configuration options.
    let (runtime, guard) = dial9_tokio_telemetry::telemetry::TracedRuntime::builder()
        .with_task_tracking(true)
        .build(builder, writer)
        .map_err(|e| {
            dial9_runtime_state().record_runtime_error(&config);
            TelemetryError::Io(format!("Failed to build TracedRuntime: {}", e))
        })?;

    dial9_runtime_state().record_runtime_started(&config);
    Ok((runtime, guard))
}

pub(crate) fn runtime_stats_snapshot() -> Dial9RuntimeSnapshot {
    dial9_runtime_state().snapshot()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Mutex, OnceLock};
    use tempfile::tempdir;

    static DIAL9_ENV_TEST_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

    fn with_dial9_env_lock<F>(f: F)
    where
        F: FnOnce(),
    {
        let _guard = DIAL9_ENV_TEST_LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .expect("dial9 env test lock should not be poisoned");
        f();
    }

    fn reset_dial9_runtime_state_for_test() {
        let state = dial9_runtime_state();
        state.enabled.store(false, Ordering::Relaxed);
        state.active_sessions.store(0, Ordering::Relaxed);
        state.errors_total.store(0, Ordering::Relaxed);
        *state
            .output_dir
            .write()
            .expect("dial9 output_dir lock should not be poisoned") = None;
        *state
            .file_prefix
            .write()
            .expect("dial9 file_prefix lock should not be poisoned") = None;
    }

    #[test]
    fn test_dial9_config_default() {
        let config = Dial9Config::default();
        assert!(!config.enabled);
        assert_eq!(config.output_dir, DEFAULT_RUNTIME_DIAL9_OUTPUT_DIR);
        assert_eq!(config.file_prefix, DEFAULT_RUNTIME_DIAL9_FILE_PREFIX);
        assert_eq!(config.max_file_size, DEFAULT_RUNTIME_DIAL9_MAX_FILE_SIZE);
        assert_eq!(config.rotation_count, DEFAULT_RUNTIME_DIAL9_ROTATION_COUNT);
        assert_eq!(config.sampling_rate, DEFAULT_RUNTIME_DIAL9_SAMPLING_RATE);
    }

    #[test]
    fn test_dial9_config_base_path() {
        let config = Dial9Config {
            output_dir: "/tmp/telemetry".to_string(),
            file_prefix: "rustfs".to_string(),
            ..Default::default()
        };
        assert_eq!(config.base_path(), PathBuf::from("/tmp/telemetry/rustfs"));
    }

    #[test]
    fn test_dial9_sanitizers_reject_zero_values() {
        assert_eq!(sanitize_dial9_max_file_size(0), 1);
        assert_eq!(sanitize_dial9_rotation_count(0), 1);
    }

    #[test]
    fn test_dial9_total_rotation_size_saturates() {
        assert_eq!(dial9_total_rotation_size(u64::MAX, 2), u64::MAX);
    }

    #[test]
    fn test_is_enabled_default() {
        // Skip if environment variable is explicitly set
        if std::env::var(ENV_RUNTIME_DIAL9_ENABLED).is_ok() {
            println!("Skipping test: RUSTFS_RUNTIME_DIAL9_ENABLED is set");
            return;
        }
        assert!(!is_enabled());
    }

    #[test]
    fn sampling_rate_rejects_nan_and_falls_back_to_default() {
        reset_dial9_runtime_state_for_test();
        with_dial9_env_lock(|| {
            temp_env::with_var(ENV_RUNTIME_DIAL9_ENABLED, Some("true"), || {
                temp_env::with_var(ENV_RUNTIME_DIAL9_SAMPLING_RATE, Some("NaN"), || {
                    let config = Dial9Config::from_env();
                    assert_eq!(config.sampling_rate, DEFAULT_RUNTIME_DIAL9_SAMPLING_RATE);
                });
            });
        });
    }

    #[test]
    fn session_guard_is_active_when_runtime_state_reports_an_active_session() {
        reset_dial9_runtime_state_for_test();
        let config = Dial9Config {
            enabled: true,
            ..Dial9Config::default()
        };
        dial9_runtime_state().record_runtime_started(&config);

        let guard = Dial9SessionGuard { _guard: None, config };

        assert!(guard.is_active());
    }

    #[test]
    fn runtime_stats_snapshot_reports_disk_usage_and_errors() {
        reset_dial9_runtime_state_for_test();
        let dir = tempdir().expect("create temp dir");
        let trace_path = dir.path().join("dial9-trace.segment");
        std::fs::write(&trace_path, vec![0_u8; 128]).expect("write fake dial9 trace");

        let config = Dial9Config {
            enabled: true,
            output_dir: dir.path().to_string_lossy().into_owned(),
            file_prefix: "dial9-trace".to_string(),
            ..Dial9Config::default()
        };
        dial9_runtime_state().record_runtime_started(&config);
        dial9_runtime_state().record_runtime_error(&config);

        let snapshot = runtime_stats_snapshot();
        assert_eq!(snapshot.active_sessions, 0);
        assert_eq!(snapshot.errors_total, 1);
        assert_eq!(snapshot.disk_usage_bytes, 128);
    }
}
