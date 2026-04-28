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

use crate::{AppConfig, GlobalError, OtelConfig, OtelGuard, telemetry::init_telemetry};
use std::sync::{Arc, Mutex};
use tokio::sync::OnceCell;
use tracing::{info, warn};

/// Global guard for OpenTelemetry tracing
static GLOBAL_GUARD: OnceCell<Arc<Mutex<OtelGuard>>> = OnceCell::const_new();

/// Flag indicating if observability metric is enabled
pub(crate) static OBSERVABILITY_METRIC_ENABLED: OnceCell<bool> = OnceCell::const_new();

/// Namespaced metrics for cleaner and rolling logging.
pub(crate) const METRIC_LOG_CLEANER_DELETED_FILES_TOTAL: &str = "rustfs_log_cleaner_deleted_files_total";
pub(crate) const METRIC_LOG_CLEANER_FREED_BYTES_TOTAL: &str = "rustfs_log_cleaner_freed_bytes_total";
pub(crate) const METRIC_LOG_CLEANER_COMPRESS_DURATION_SECONDS: &str = "rustfs_log_cleaner_compress_duration_seconds";
pub(crate) const METRIC_LOG_CLEANER_STEAL_SUCCESS_RATE: &str = "rustfs_log_cleaner_steal_success_rate";
pub(crate) const METRIC_LOG_CLEANER_RUNS_TOTAL: &str = "rustfs_log_cleaner_runs_total";
pub(crate) const METRIC_LOG_CLEANER_RUN_FAILURES_TOTAL: &str = "rustfs_log_cleaner_run_failures_total";
pub(crate) const METRIC_LOG_CLEANER_ROTATION_TOTAL: &str = "rustfs_log_cleaner_rotation_total";
pub(crate) const METRIC_LOG_CLEANER_ROTATION_FAILURES_TOTAL: &str = "rustfs_log_cleaner_rotation_failures_total";
pub(crate) const METRIC_LOG_CLEANER_ROTATION_DURATION_SECONDS: &str = "rustfs_log_cleaner_rotation_duration_seconds";
pub(crate) const METRIC_LOG_CLEANER_ACTIVE_FILE_SIZE_BYTES: &str = "rustfs_log_cleaner_active_file_size_bytes";

/// Check whether Observability metric is enabled
pub fn observability_metric_enabled() -> bool {
    OBSERVABILITY_METRIC_ENABLED.get().copied().unwrap_or(false)
}

/// Set the global observability metrics flag once.
///
/// When this function is called multiple times, only the first value is kept
/// and later values are ignored to preserve OnceCell semantics.
pub(crate) fn set_observability_metric_enabled(enabled: bool) {
    if OBSERVABILITY_METRIC_ENABLED.set(enabled).is_err()
        && let Some(current) = OBSERVABILITY_METRIC_ENABLED.get()
        && *current != enabled
    {
        warn!(
            current = *current,
            requested = enabled,
            "OBSERVABILITY_METRIC_ENABLED was already initialized; keeping original value"
        );
    }
}

/// Initialize the observability module
///
/// # Parameters
/// - `config`: Configuration information
///
/// # Returns
/// A tuple containing the logger and the telemetry guard
///
/// # Example
/// ```no_run
/// # use rustfs_obs::init_obs;
///
/// # #[tokio::main]
/// # async fn main() {
/// #    match init_obs(None).await {
/// #         Ok(guard) => {}
/// #         Err(e) => { eprintln!("Failed to initialize observability: {}", e); }
/// #     }
/// # }
/// ```
pub async fn init_obs(endpoint: Option<String>) -> Result<OtelGuard, GlobalError> {
    // Load the configuration file
    let config = AppConfig::new_with_endpoint(endpoint);
    init_obs_with_config(&config.observability).await
}

/// Initialize the observability module with an explicit [`OtelConfig`].
///
/// This is the lower-level counterpart to [`init_obs`]: it accepts a fully
/// constructed [`OtelConfig`] rather than building one from an endpoint URL.
/// Useful when the config is already available (e.g., embedded in a larger
/// application config struct) or when unit-testing with custom settings.
///
/// # Parameters
/// - `config`: A pre-built [`OtelConfig`] to drive all telemetry backends.
///
/// # Returns
/// An [`OtelGuard`] that must be kept alive for the lifetime of the
/// application. Dropping it flushes and shuts down all providers.
///
/// # Errors
/// Returns [`GlobalError`] when the underlying telemetry backend fails to
/// initialise (e.g., cannot create the log directory, or an OTLP exporter
/// cannot connect).
///
/// # Example
/// ```no_run
/// use rustfs_obs::{AppConfig, init_obs_with_config};
///
/// # #[tokio::main]
/// # async fn main() {
/// let config = AppConfig::new_with_endpoint(Some("http://localhost:4318".to_string()));
/// let _guard = init_obs_with_config(&config.observability)
///     .await
///     .expect("observability init failed");
/// # }
/// ```
pub async fn init_obs_with_config(config: &OtelConfig) -> Result<OtelGuard, GlobalError> {
    let otel_guard = init_telemetry(config)?;
    // Metrics runtime scheduling is exposed by rustfs_obs::init_metrics_runtime().
    Ok(otel_guard)
}

/// Set the global guard for OtelGuard
///
/// # Arguments
/// * `guard` - The OtelGuard instance to set globally
///
/// # Returns
/// * `Ok(())` if successful
/// * `Err(GuardError)` if setting fails
///
/// # Example
/// ```no_run
/// # use rustfs_obs::{ init_obs, set_global_guard};
///
/// # async fn init() -> Result<(), Box<dyn std::error::Error>> {
/// #    let guard = match init_obs(None).await{
/// #         Ok(g) => g,
/// #         Err(e) => { return Err(Box::new(e)); }
/// #    };
/// #    set_global_guard(guard)?;
/// #    Ok(())
/// # }
/// ```
pub fn set_global_guard(guard: OtelGuard) -> Result<(), GlobalError> {
    info!("Initializing global guard");
    GLOBAL_GUARD.set(Arc::new(Mutex::new(guard))).map_err(GlobalError::SetError)
}

/// Get the global guard for OtelGuard
///
/// # Returns
/// * `Ok(Arc<Mutex<OtelGuard>>)` if guard exists
/// * `Err(GuardError)` if guard not initialized
///
/// # Example
/// ```no_run
/// # use rustfs_obs::get_global_guard;
///
/// # async fn trace_operation() -> Result<(), Box<dyn std::error::Error>> {
/// #    let guard = get_global_guard()?;
/// #    let _lock = guard.lock().unwrap();
/// #    // Perform traced operation
/// #    Ok(())
/// # }
/// ```
pub fn get_global_guard() -> Result<Arc<Mutex<OtelGuard>>, GlobalError> {
    GLOBAL_GUARD.get().cloned().ok_or(GlobalError::NotInitialized)
}

#[cfg(test)]
mod tests {
    use super::*;

    const README: &str = include_str!("../README.md");
    const GRAFANA_DASHBOARD: &str = include_str!("../../../.docker/observability/grafana/dashboards/rustfs.json");

    #[tokio::test]
    async fn test_get_uninitialized_guard() {
        let result = get_global_guard();
        assert!(matches!(result, Err(GlobalError::NotInitialized)));
    }

    #[test]
    fn test_log_cleaner_metric_constants_use_rustfs_prefix() {
        let metrics = [
            METRIC_LOG_CLEANER_DELETED_FILES_TOTAL,
            METRIC_LOG_CLEANER_FREED_BYTES_TOTAL,
            METRIC_LOG_CLEANER_COMPRESS_DURATION_SECONDS,
            METRIC_LOG_CLEANER_STEAL_SUCCESS_RATE,
            METRIC_LOG_CLEANER_RUNS_TOTAL,
            METRIC_LOG_CLEANER_RUN_FAILURES_TOTAL,
            METRIC_LOG_CLEANER_ROTATION_TOTAL,
            METRIC_LOG_CLEANER_ROTATION_FAILURES_TOTAL,
            METRIC_LOG_CLEANER_ROTATION_DURATION_SECONDS,
            METRIC_LOG_CLEANER_ACTIVE_FILE_SIZE_BYTES,
        ];

        for metric in metrics {
            assert!(
                metric.starts_with("rustfs_log_cleaner_"),
                "metric '{metric}' should use rustfs_log_cleaner_* namespace"
            );
        }
    }

    #[test]
    fn test_readme_contains_grafana_dashboard_draft_section() {
        assert!(README.contains("## Cleaner & Rotation Metrics"));
        assert!(README.contains("### Grafana Dashboard JSON Draft (Ready to Import)"));
        assert!(README.contains("\"uid\": \"rustfs-log-cleaner\""));
        assert!(README.contains("rustfs_log_cleaner_runs_total"));
        assert!(README.contains("rustfs_log_cleaner_rotation_failures_total"));
    }

    #[test]
    fn test_readme_contains_promql_templates_section() {
        assert!(README.contains("### PromQL Templates"));
        assert!(README.contains("Cleanup failure ratio"));
        assert!(README.contains("histogram_quantile(0.95"));
        assert!(README.contains("max(rustfs_log_cleaner_active_file_size_bytes)"));
    }

    #[test]
    fn test_grafana_dashboard_contains_log_cleaner_row() {
        assert!(GRAFANA_DASHBOARD.contains("\"title\": \"Log Cleaner\""));
        assert!(GRAFANA_DASHBOARD.contains("rustfs_log_cleaner_runs_total"));
        assert!(GRAFANA_DASHBOARD.contains("rustfs_log_cleaner_rotation_failures_total"));
        assert!(GRAFANA_DASHBOARD.contains("rustfs_log_cleaner_active_file_size_bytes"));
    }

    #[test]
    fn test_readme_mentions_deployed_dashboard_path() {
        assert!(README.contains(".docker/observability/grafana/dashboards/rustfs.json"));
        assert!(README.contains("row title: `Log Cleaner`"));
    }

    #[test]
    fn test_init_obs_with_config_signature_is_correct() {
        // Compile-time check: verifies that `init_obs_with_config` is public and
        // accepts `&OtelConfig`, matching the README documentation.
        // The future is created but intentionally not polled — this avoids
        // initialising the global tracing subscriber in a unit-test context.
        fn _type_check() {
            let config = OtelConfig::default();
            let _future = init_obs_with_config(&config);
        }
    }
}
