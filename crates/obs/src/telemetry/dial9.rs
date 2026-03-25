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
use rustfs_utils::get_env_f64;
use rustfs_utils::get_env_opt_str;
use rustfs_utils::get_env_str;
use rustfs_utils::get_env_u64;
use rustfs_utils::get_env_usize;
use std::path::PathBuf;
use tracing::{info, warn};

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
            return Self::default();
        }

        Self {
            enabled,
            output_dir: get_env_str(ENV_RUNTIME_DIAL9_OUTPUT_DIR, DEFAULT_RUNTIME_DIAL9_OUTPUT_DIR),
            file_prefix: get_env_str(ENV_RUNTIME_DIAL9_FILE_PREFIX, DEFAULT_RUNTIME_DIAL9_FILE_PREFIX),
            max_file_size: get_env_u64(ENV_RUNTIME_DIAL9_MAX_FILE_SIZE, DEFAULT_RUNTIME_DIAL9_MAX_FILE_SIZE),
            rotation_count: get_env_usize(ENV_RUNTIME_DIAL9_ROTATION_COUNT, DEFAULT_RUNTIME_DIAL9_ROTATION_COUNT),
            s3_bucket: get_env_opt_str(ENV_RUNTIME_DIAL9_S3_BUCKET).filter(|s| !s.is_empty()),
            s3_prefix: get_env_opt_str(ENV_RUNTIME_DIAL9_S3_PREFIX).filter(|s| !s.is_empty()),
            sampling_rate: get_env_f64(ENV_RUNTIME_DIAL9_SAMPLING_RATE, DEFAULT_RUNTIME_DIAL9_SAMPLING_RATE).clamp(0.0, 1.0),
        }
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
            info!("Dial9 telemetry disabled");
            return Ok(None);
        }

        info!(
            output_dir = %config.output_dir,
            file_prefix = %config.file_prefix,
            sampling_rate = config.sampling_rate,
            "Validating dial9 telemetry configuration"
        );

        // Only create directory; writer will be created in build_traced_runtime
        if let Err(e) = tokio::fs::create_dir_all(&config.output_dir).await {
            warn!("Failed to create dial9 output directory '{}': {}", config.output_dir, e);
            warn!("Continuing without dial9 telemetry");
            return Ok(None);
        }

        info!("Dial9 telemetry configuration validated successfully");

        Ok(Some(Self { _guard: None, config }))
    }

    /// Set the telemetry guard (called after runtime creation)
    #[allow(dead_code)]
    pub(crate) fn set_guard(&mut self, guard: Dial9TelemetryGuard) {
        self._guard = Some(guard);
    }

    /// Check if this guard has an active session.
    pub fn is_active(&self) -> bool {
        self._guard.is_some()
    }

    /// Flush any pending telemetry data.
    pub async fn shutdown(&self) {
        if let Some(_guard) = &self._guard {
            info!("Dial9 telemetry data will be flushed on drop");
            // TelemetryGuard handles flushing automatically when dropped
        }
    }
}

impl Drop for Dial9SessionGuard {
    fn drop(&mut self) {
        if let Some(_guard) = &self._guard {
            // TelemetryGuard flushes automatically when dropped
            info!("Dial9 telemetry guard dropped, data flushed");
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

    // Ensure the output directory exists before creating the writer
    std::fs::create_dir_all(&config.output_dir)
        .map_err(|e| TelemetryError::Io(format!("Failed to create dial9 output directory '{}': {}", config.output_dir, e)))?;

    // Create rotating writer (synchronous for runtime building)
    let base_path = config.base_path();
    let writer = RotatingWriter::new(base_path, config.max_file_size, config.max_file_size * config.rotation_count as u64)
        .map_err(|e| TelemetryError::Io(format!("Failed to create RotatingWriter: {}", e)))?;

    // Build traced runtime
    // Note: sampling_rate and S3 upload settings are reserved for future use
    // once the dial9 library provides support for those configuration options.
    dial9_tokio_telemetry::telemetry::TracedRuntime::builder()
        .with_task_tracking(true)
        .build(builder, writer)
        .map_err(|e| TelemetryError::Io(format!("Failed to build TracedRuntime: {}", e)))
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn test_is_enabled_default() {
        // Skip if environment variable is explicitly set
        if std::env::var(ENV_RUNTIME_DIAL9_ENABLED).is_ok() {
            println!("Skipping test: RUSTFS_RUNTIME_DIAL9_ENABLED is set");
            return;
        }
        assert!(!is_enabled());
    }
}
