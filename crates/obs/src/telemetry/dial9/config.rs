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

//! Configuration for dial9 Tokio runtime telemetry.
//!
//! This module carries no dependency on the `dial9-tokio-telemetry` crate, so
//! it compiles identically whether or not the `dial9` feature is enabled. That
//! lets callers read the configured state (and export metrics about it) from a
//! binary that was built without telemetry support.

use super::state::dial9_runtime_state;
use rustfs_config::{
    DEFAULT_RUNTIME_DIAL9_ENABLED, DEFAULT_RUNTIME_DIAL9_FILE_PREFIX, DEFAULT_RUNTIME_DIAL9_MAX_FILE_SIZE,
    DEFAULT_RUNTIME_DIAL9_OUTPUT_DIR, DEFAULT_RUNTIME_DIAL9_ROTATION_COUNT, ENV_RUNTIME_DIAL9_ENABLED,
    ENV_RUNTIME_DIAL9_FILE_PREFIX, ENV_RUNTIME_DIAL9_MAX_FILE_SIZE, ENV_RUNTIME_DIAL9_OUTPUT_DIR,
    ENV_RUNTIME_DIAL9_ROTATION_COUNT, ENV_RUNTIME_DIAL9_S3_BUCKET, ENV_RUNTIME_DIAL9_S3_PREFIX,
};
use rustfs_utils::{get_env_bool, get_env_opt_str, get_env_opt_u64, get_env_opt_usize, get_env_str};
use std::path::PathBuf;
use tracing::warn;

use super::{EVENT_DIAL9_STATE, LOG_COMPONENT_OBS, LOG_SUBSYSTEM_DIAL9};

/// Whether this binary was compiled with dial9 telemetry support.
///
/// Telemetry requires both the `dial9` cargo feature and `--cfg tokio_unstable`;
/// the crate's build script enforces that pairing, so this single flag is
/// sufficient to describe compile-time support.
pub const fn is_supported() -> bool {
    cfg!(feature = "dial9")
}

/// Whether dial9 telemetry is requested via environment configuration.
///
/// This reports operator intent and is independent of whether the binary can
/// honour it. Use [`is_enabled`] to test whether telemetry will actually run.
pub fn is_configured() -> bool {
    get_env_bool(ENV_RUNTIME_DIAL9_ENABLED, DEFAULT_RUNTIME_DIAL9_ENABLED)
}

/// Whether dial9 telemetry will actually run: requested *and* compiled in.
pub fn is_enabled() -> bool {
    is_supported() && is_configured()
}

fn sanitize_max_file_size(bytes: u64) -> u64 {
    bytes.max(1)
}

fn sanitize_rotation_count(count: usize) -> usize {
    count.max(1)
}

/// Total on-disk byte budget for the trace family, saturating on overflow.
pub(super) fn total_rotation_size(max_file_size: u64, rotation_count: usize) -> u64 {
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
}

impl Default for Dial9Config {
    fn default() -> Self {
        Self {
            enabled: DEFAULT_RUNTIME_DIAL9_ENABLED,
            output_dir: DEFAULT_RUNTIME_DIAL9_OUTPUT_DIR.to_string(),
            file_prefix: DEFAULT_RUNTIME_DIAL9_FILE_PREFIX.to_string(),
            max_file_size: DEFAULT_RUNTIME_DIAL9_MAX_FILE_SIZE,
            rotation_count: DEFAULT_RUNTIME_DIAL9_ROTATION_COUNT,
        }
    }
}

impl Dial9Config {
    /// Create configuration from environment variables, recording the resulting
    /// trace location so metrics can report it even on a disabled binary.
    pub fn from_env() -> Self {
        let config = if is_enabled() {
            Self::from_env_enabled()
        } else {
            Self::default()
        };
        dial9_runtime_state().record_config(&config);
        config
    }

    /// Build the enabled configuration from the environment, warning about any
    /// knob that is set but cannot be honoured.
    fn from_env_enabled() -> Self {
        let raw_max_file_size = get_env_opt_u64(ENV_RUNTIME_DIAL9_MAX_FILE_SIZE);
        let raw_rotation_count = get_env_opt_usize(ENV_RUNTIME_DIAL9_ROTATION_COUNT);

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

        // S3 upload is not built (see `warn_unusable_s3_upload`); read the knobs
        // only to warn, and drop them rather than carrying dead configuration.
        warn_unusable_s3_upload(
            get_env_opt_str(ENV_RUNTIME_DIAL9_S3_BUCKET)
                .filter(|s| !s.is_empty())
                .as_deref(),
            get_env_opt_str(ENV_RUNTIME_DIAL9_S3_PREFIX)
                .filter(|s| !s.is_empty())
                .as_deref(),
        );

        Self {
            enabled: true,
            output_dir: get_env_str(ENV_RUNTIME_DIAL9_OUTPUT_DIR, DEFAULT_RUNTIME_DIAL9_OUTPUT_DIR),
            file_prefix: get_env_str(ENV_RUNTIME_DIAL9_FILE_PREFIX, DEFAULT_RUNTIME_DIAL9_FILE_PREFIX),
            max_file_size: sanitize_max_file_size(raw_max_file_size.unwrap_or(DEFAULT_RUNTIME_DIAL9_MAX_FILE_SIZE)),
            rotation_count: sanitize_rotation_count(raw_rotation_count.unwrap_or(DEFAULT_RUNTIME_DIAL9_ROTATION_COUNT)),
        }
    }

    /// Get the base path for trace files.
    pub fn base_path(&self) -> PathBuf {
        PathBuf::from(&self.output_dir).join(&self.file_prefix)
    }

    /// Total on-disk byte budget across all retained segments.
    pub fn total_disk_budget(&self) -> u64 {
        total_rotation_size(self.max_file_size, self.rotation_count)
    }
}

/// S3 upload of sealed segments is not available in any build.
///
/// dial9's `worker-s3` feature depends on aws-sdk-s3-transfer-manager 0.1.3,
/// which pins hyper-rustls 0.24 / rustls-webpki 0.101.7 — a webpki carrying
/// RUSTSEC-2026-0098, -0099 and -0104. Cargo cannot drop a transitive
/// dependency, so this needs an upstream fix (rustfs/backlog#1157, D9-14).
///
/// Warn loudly rather than silently discarding a configured bucket: an operator
/// who set these expects their traces to be uploaded somewhere.
fn warn_unusable_s3_upload(s3_bucket: Option<&str>, s3_prefix: Option<&str>) {
    if s3_bucket.is_none() && s3_prefix.is_none() {
        return;
    }

    warn!(
        event = EVENT_DIAL9_STATE,
        component = LOG_COMPONENT_OBS,
        subsystem = LOG_SUBSYSTEM_DIAL9,
        result = "s3_upload_unsupported",
        s3_bucket = s3_bucket.unwrap_or(""),
        s3_prefix = s3_prefix.unwrap_or(""),
        reason = "dial9 worker-s3 depends on a vulnerable rustls-webpki (RUSTSEC-2026-0098/0099/0104)",
        remedy = "collect trace segments from the output directory instead",
        "dial9 state changed"
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanitizers_reject_zero_values() {
        assert_eq!(sanitize_max_file_size(0), 1);
        assert_eq!(sanitize_rotation_count(0), 1);
    }

    #[test]
    fn total_rotation_size_saturates() {
        assert_eq!(total_rotation_size(u64::MAX, 2), u64::MAX);
    }

    #[test]
    fn default_config_is_disabled() {
        let config = Dial9Config::default();
        assert!(!config.enabled);
        assert_eq!(config.output_dir, DEFAULT_RUNTIME_DIAL9_OUTPUT_DIR);
        assert_eq!(config.file_prefix, DEFAULT_RUNTIME_DIAL9_FILE_PREFIX);
        assert_eq!(config.max_file_size, DEFAULT_RUNTIME_DIAL9_MAX_FILE_SIZE);
        assert_eq!(config.rotation_count, DEFAULT_RUNTIME_DIAL9_ROTATION_COUNT);
    }

    #[test]
    fn base_path_joins_prefix() {
        let config = Dial9Config {
            output_dir: "/tmp/telemetry".to_string(),
            file_prefix: "rustfs".to_string(),
            ..Default::default()
        };
        assert_eq!(config.base_path(), PathBuf::from("/tmp/telemetry/rustfs"));
    }

    #[test]
    fn total_disk_budget_multiplies_file_size_by_rotation_count() {
        let config = Dial9Config {
            max_file_size: 100,
            rotation_count: 10,
            ..Default::default()
        };
        assert_eq!(config.total_disk_budget(), 1_000);
    }

    #[test]
    fn is_enabled_requires_compile_time_support() {
        // `is_enabled` is the conjunction of the two; when support is absent it
        // must stay false no matter what the environment requests.
        if !is_supported() {
            assert!(!is_enabled());
        }
    }
}
