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

//! Telemetry initialisation module for RustFS.
//!
//! This module is the single entry point for all observability backends.
//! Callers should use [`init_telemetry`] and keep the returned [`OtelGuard`]
//! alive for the lifetime of the application.
//!
//! ## Architecture
//!
//! The module is split into focused sub-modules:
//!
//! | Sub-module   | Responsibility                                          |
//! |--------------|---------------------------------------------------------|
//! | `guard`      | [`OtelGuard`] RAII type for provider lifecycle          |
//! | `filter`     | `EnvFilter` construction helpers                        |
//! | `resource`   | OpenTelemetry `Resource` builder                        |
//! | `local`      | Local logging: stdout-only **or** rolling-file          |
//! | `otel`       | Full OTLP/HTTP pipeline (traces + metrics + logs)       |
//!
//! ## Routing rules (evaluated in order)
//!
//! 1. **OpenTelemetry** — if any OTLP endpoint is configured, the full HTTP
//!    pipeline is initialised via [`otel::init_observability_http`].
//! 2. **File logging** — if `RUSTFS_OBS_LOG_DIRECTORY` (or `log_directory` /
//!    `log_dir` in config) is set to a non-empty value, rolling-file logging is
//!    initialised together with an optional stdout mirror.
//! 3. **Stdout only** — default fallback; no file I/O, no remote export.

mod filter;
mod guard;
mod local;
mod otel;
mod recorder;
mod resource;

use crate::TelemetryError;
use crate::config::OtelConfig;
pub use guard::OtelGuard;
pub use recorder::Recorder;
use rustfs_config::observability::ENV_OBS_LOG_DIRECTORY;
use rustfs_config::{DEFAULT_LOG_LEVEL, ENVIRONMENT, observability::DEFAULT_OBS_ENVIRONMENT_PRODUCTION};
use rustfs_utils::get_env_opt_str;

/// Initialize the telemetry subsystem according to the provided configuration.
///
/// Evaluates three routing rules in priority order and delegates to the
/// appropriate backend:
///
/// 1. If any OTLP endpoint is set, initialises the full
///    OpenTelemetry HTTP pipeline (traces + metrics + logs).
/// 2. If a log directory is explicitly configured via the
///    `RUSTFS_OBS_LOG_DIRECTORY` environment variable, initialises
///    rolling-file logging with an optional stdout mirror.
/// 3. Otherwise, falls back to stdout-only JSON logging.
///
/// # Arguments
/// * `config` - Observability configuration, typically built from environment
///   variables via [`OtelConfig::extract_otel_config_from_env`].
///
/// # Returns
/// An [`OtelGuard`] that must be kept alive for the duration of the
/// application.  Dropping it triggers ordered shutdown of all providers.
///
/// # Errors
/// Returns [`TelemetryError`] when a backend fails to initialise (e.g., cannot
/// create the log directory, or an OTLP exporter cannot connect).
pub(crate) fn init_telemetry(config: &OtelConfig) -> Result<OtelGuard, TelemetryError> {
    let environment = config.environment.as_deref().unwrap_or(ENVIRONMENT);
    let is_production = environment.eq_ignore_ascii_case(DEFAULT_OBS_ENVIRONMENT_PRODUCTION);
    let logger_level = config.logger_level.as_deref().unwrap_or(DEFAULT_LOG_LEVEL);

    // ── Rule 1: OpenTelemetry HTTP pipeline ───────────────────────────────────
    // Activated when at least one OTLP endpoint is non-empty.
    let has_obs = !config.endpoint.is_empty()
        || config.trace_endpoint.as_deref().map(|s| !s.is_empty()).unwrap_or(false)
        || config.metric_endpoint.as_deref().map(|s| !s.is_empty()).unwrap_or(false)
        || config.log_endpoint.as_deref().map(|s| !s.is_empty()).unwrap_or(false);

    if has_obs {
        return otel::init_observability_http(config, logger_level, is_production);
    }

    // ── Rule 2 & 3: Local logging (file or stdout) ────────────────────────────
    // `init_local_logging` internally decides between file and stdout mode
    // based on whether a log directory is configured.
    //
    // We check the environment variable here (rather than relying solely on the
    // config struct) to honour dynamic overrides set after config construction.
    let user_set_log_dir = get_env_opt_str(ENV_OBS_LOG_DIRECTORY);
    let effective_config = if user_set_log_dir.as_deref().filter(|d| !d.is_empty()).is_some() {
        // Environment variable is set: ensure the config reflects it so that
        // `init_local_logging` picks up the value even if the struct was built
        // before the env var was set.
        std::borrow::Cow::Owned(OtelConfig {
            log_directory: user_set_log_dir,
            ..config.clone()
        })
    } else {
        std::borrow::Cow::Borrowed(config)
    };

    local::init_local_logging(&effective_config, logger_level, is_production)
}

#[cfg(test)]
mod tests {
    use rustfs_config::observability::DEFAULT_OBS_ENVIRONMENT_PRODUCTION;
    use rustfs_config::{ENVIRONMENT, USE_STDOUT};

    #[test]
    fn test_production_environment_detection() {
        // Verify that case-insensitive comparison correctly identifies production.
        let production_envs = ["production", "PRODUCTION", "Production"];
        for env_value in production_envs {
            let is_production = env_value.eq_ignore_ascii_case(DEFAULT_OBS_ENVIRONMENT_PRODUCTION);
            assert!(is_production, "Should detect '{env_value}' as production environment");
        }
    }

    #[test]
    fn test_non_production_environment_detection() {
        // Verify that non-production environments are not misidentified.
        let non_production_envs = ["development", "test", "staging", "dev", "local"];
        for env_value in non_production_envs {
            let is_production = env_value.eq_ignore_ascii_case(DEFAULT_OBS_ENVIRONMENT_PRODUCTION);
            assert!(!is_production, "Should not detect '{env_value}' as production environment");
        }
    }

    #[test]
    fn test_stdout_behavior_logic() {
        // Validate the stdout-enable logic for different environment/config combinations.
        struct TestCase {
            is_production: bool,
            config_use_stdout: Option<bool>,
            expected_use_stdout: bool,
            description: &'static str,
        }

        let test_cases = [
            TestCase {
                is_production: true,
                config_use_stdout: None,
                expected_use_stdout: false,
                description: "Production with no config should disable stdout",
            },
            TestCase {
                is_production: false,
                config_use_stdout: None,
                expected_use_stdout: USE_STDOUT,
                description: "Non-production with no config should use default",
            },
            TestCase {
                is_production: true,
                config_use_stdout: Some(true),
                expected_use_stdout: true,
                description: "Production with explicit true should enable stdout",
            },
            TestCase {
                is_production: true,
                config_use_stdout: Some(false),
                expected_use_stdout: false,
                description: "Production with explicit false should disable stdout",
            },
            TestCase {
                is_production: false,
                config_use_stdout: Some(true),
                expected_use_stdout: true,
                description: "Non-production with explicit true should enable stdout",
            },
        ];

        for case in &test_cases {
            let default_use_stdout = if case.is_production { false } else { USE_STDOUT };
            let actual = case.config_use_stdout.unwrap_or(default_use_stdout);
            assert_eq!(actual, case.expected_use_stdout, "Test case failed: {}", case.description);
        }
    }

    #[test]
    fn test_log_level_filter_mapping_logic() {
        // Validate the log level string → tracing level mapping used in filters.
        let test_cases = [
            ("trace", "Trace"),
            ("debug", "Debug"),
            ("info", "Info"),
            ("warn", "Warn"),
            ("warning", "Warn"),
            ("error", "Error"),
            ("off", "None"),
            ("invalid_level", "Info"),
        ];

        for (input, expected) in test_cases {
            let mapped = match input.to_lowercase().as_str() {
                "trace" => "Trace",
                "debug" => "Debug",
                "info" => "Info",
                "warn" | "warning" => "Warn",
                "error" => "Error",
                "off" => "None",
                _ => "Info",
            };
            assert_eq!(mapped, expected, "Log level '{input}' should map to '{expected}'");
        }
    }

    #[test]
    fn test_otel_config_environment_defaults() {
        // Verify that environment field defaults behave correctly.
        use crate::config::OtelConfig;
        let config = OtelConfig {
            endpoint: "".to_string(),
            use_stdout: None,
            environment: Some("production".to_string()),
            ..Default::default()
        };
        let environment = config.environment.as_deref().unwrap_or(ENVIRONMENT);
        assert_eq!(environment, "production");

        let dev_config = OtelConfig {
            endpoint: "".to_string(),
            use_stdout: None,
            environment: Some("development".to_string()),
            ..Default::default()
        };
        let dev_environment = dev_config.environment.as_deref().unwrap_or(ENVIRONMENT);
        assert_eq!(dev_environment, "development");
    }
}
