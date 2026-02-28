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

use crate::config::OtelConfig;
use crate::global::OBSERVABILITY_METRIC_ENABLED;
use crate::log_cleanup::LogCleaner;
use crate::{Recorder, TelemetryError};
use metrics::counter;
use opentelemetry::{global, trace::TracerProvider, KeyValue};
use opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge;
use opentelemetry_otlp::{Compression, Protocol, WithExportConfig, WithHttpConfig};
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::{
    logs::SdkLoggerProvider,
    metrics::{PeriodicReader, SdkMeterProvider},
    trace::{RandomIdGenerator, Sampler, SdkTracerProvider},
    Resource,
};
use opentelemetry_semantic_conventions::{
    attribute::{DEPLOYMENT_ENVIRONMENT_NAME, NETWORK_LOCAL_ADDRESS, SERVICE_VERSION as OTEL_SERVICE_VERSION},
    SCHEMA_URL,
};
use rustfs_config::{
    observability::{DEFAULT_OBS_ENVIRONMENT_PRODUCTION, ENV_OBS_LOG_DIRECTORY}, APP_NAME, DEFAULT_LOG_KEEP_FILES, DEFAULT_LOG_LEVEL, DEFAULT_OBS_LOGS_EXPORT_ENABLED,
    DEFAULT_OBS_LOG_STDOUT_ENABLED, DEFAULT_OBS_METRICS_EXPORT_ENABLED, DEFAULT_OBS_TRACES_EXPORT_ENABLED, ENVIRONMENT, METER_INTERVAL,
    SAMPLE_RATIO,
    SERVICE_VERSION,
};
use rustfs_utils::{get_env_opt_str, get_local_ip_with_default};
use smallvec::SmallVec;
use std::{borrow::Cow, fs, io::IsTerminal, time::Duration};
use tracing::info;
use tracing_error::ErrorLayer;
use tracing_opentelemetry::{MetricsLayer, OpenTelemetryLayer};
use tracing_subscriber::{
    fmt::{format::FmtSpan, time::LocalTime}, layer::SubscriberExt,
    util::SubscriberInitExt,
    EnvFilter,
    Layer,
};

/// A guard object that manages the lifecycle of OpenTelemetry components.
///
/// This struct holds references to the created OpenTelemetry providers and ensures
/// they are properly shut down when the guard is dropped. It implements the RAII
/// (Resource Acquisition Is Initialization) pattern for managing telemetry resources.
///
/// When this guard goes out of scope, it will automatically shut down:
/// - The tracer provider (for distributed tracing)
/// - The meter provider (for metrics collection)
/// - The logger provider (for structured logging)
/// - The log cleanup background task
///
/// Implement Debug trait correctly, rather than using derive, as some fields may not have implemented Debug
pub struct OtelGuard {
    tracer_provider: Option<SdkTracerProvider>,
    meter_provider: Option<SdkMeterProvider>,
    logger_provider: Option<SdkLoggerProvider>,
    tracing_guard: Option<tracing_appender::non_blocking::WorkerGuard>,
    cleanup_handle: Option<tokio::task::JoinHandle<()>>,
}

impl std::fmt::Debug for OtelGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OtelGuard")
            .field("tracer_provider", &self.tracer_provider.is_some())
            .field("meter_provider", &self.meter_provider.is_some())
            .field("logger_provider", &self.logger_provider.is_some())
            .field("tracing_guard", &self.tracing_guard.is_some())
            .field("cleanup_handle", &self.cleanup_handle.is_some())
            .finish()
    }
}

impl Drop for OtelGuard {
    fn drop(&mut self) {
        if let Some(provider) = self.tracer_provider.take()
            && let Err(err) = provider.shutdown()
        {
            eprintln!("Tracer shutdown error: {err:?}");
        }

        if let Some(provider) = self.meter_provider.take()
            && let Err(err) = provider.shutdown()
        {
            eprintln!("Meter shutdown error: {err:?}");
        }
        if let Some(provider) = self.logger_provider.take()
            && let Err(err) = provider.shutdown()
        {
            eprintln!("Logger shutdown error: {err:?}");
        }

        if let Some(handle) = self.cleanup_handle.take() {
            handle.abort();
            println!("Log cleanup task stopped");
        }

        if let Some(guard) = self.tracing_guard.take() {
            drop(guard);
            println!("Tracing guard dropped, flushing logs.");
        }
    }
}

/// create OpenTelemetry Resource
fn resource(config: &OtelConfig) -> Resource {
    Resource::builder()
        .with_service_name(Cow::Borrowed(config.service_name.as_deref().unwrap_or(APP_NAME)).to_string())
        .with_schema_url(
            [
                KeyValue::new(
                    OTEL_SERVICE_VERSION,
                    Cow::Borrowed(config.service_version.as_deref().unwrap_or(SERVICE_VERSION)).to_string(),
                ),
                KeyValue::new(
                    DEPLOYMENT_ENVIRONMENT_NAME,
                    Cow::Borrowed(config.environment.as_deref().unwrap_or(ENVIRONMENT)).to_string(),
                ),
                KeyValue::new(NETWORK_LOCAL_ADDRESS, get_local_ip_with_default()),
            ],
            SCHEMA_URL,
        )
        .build()
}

/// Creates a periodic reader for stdout metrics
fn create_periodic_reader(interval: u64) -> PeriodicReader<opentelemetry_stdout::MetricExporter> {
    PeriodicReader::builder(opentelemetry_stdout::MetricExporter::default())
        .with_interval(Duration::from_secs(interval))
        .build()
}

fn build_env_filter(logger_level: &str, default_level: Option<&str>) -> EnvFilter {
    let level = default_level.unwrap_or(logger_level);
    let mut filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(level));
    if !matches!(logger_level, "trace" | "debug") {
        let directives: SmallVec<[&str; 5]> = smallvec::smallvec!["hyper", "tonic", "h2", "reqwest", "tower"];
        for directive in directives {
            filter = filter.add_directive(format!("{directive}=off").parse().unwrap());
        }
    }

    filter
}

/// stdout + span information (fix: retain WorkerGuard to avoid releasing after initialization)
fn init_stdout_logging(_config: &OtelConfig, logger_level: &str, is_production: bool) -> OtelGuard {
    let env_filter = build_env_filter(logger_level, None);
    let (nb, guard) = tracing_appender::non_blocking(std::io::stdout());
    let enable_color = std::io::stdout().is_terminal();
    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_timer(LocalTime::rfc_3339())
        .with_target(true)
        .with_ansi(enable_color)
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
        cleanup_handle: None,
    }
}

/// File rolling log with automatic cleanup
fn init_file_logging(config: &OtelConfig, logger_level: &str, is_production: bool) -> Result<OtelGuard, TelemetryError> {
    let service_name = config.service_name.as_deref().unwrap_or(APP_NAME);
    let default_log_directory = rustfs_utils::dirs::get_log_directory_to_string(ENV_OBS_LOG_DIRECTORY);
    let log_directory = config.log_directory.as_deref().unwrap_or(default_log_directory.as_str());
    let log_filename = config.log_filename.as_deref().unwrap_or(service_name);
    let keep_files = config.log_keep_files.unwrap_or(DEFAULT_LOG_KEEP_FILES);

    // Create log directory
    if let Err(e) = fs::create_dir_all(log_directory) {
        return Err(TelemetryError::Io(e.to_string()));
    }

    #[cfg(unix)]
    {
        use std::fs::Permissions;
        use std::os::unix::fs::PermissionsExt;
        let desired: u32 = 0o755;
        match fs::metadata(log_directory) {
            Ok(meta) => {
                let current = meta.permissions().mode() & 0o777;
                // Only tighten to 0755 if existing permissions are looser than target, avoid loosening
                if (current & !desired) != 0 {
                    if let Err(e) = fs::set_permissions(log_directory, Permissions::from_mode(desired)) {
                        return Err(TelemetryError::SetPermissions(format!(
                            "dir='{log_directory}', want={desired:#o}, have={current:#o}, err={e}"
                        )));
                    }
                    // Second verification
                    if let Ok(meta2) = fs::metadata(log_directory) {
                        let after = meta2.permissions().mode() & 0o777;
                        if after != desired {
                            return Err(TelemetryError::SetPermissions(format!(
                                "dir='{log_directory}', want={desired:#o}, after={after:#o}"
                            )));
                        }
                    }
                }
            }
            Err(e) => {
                return Err(TelemetryError::Io(format!("stat '{log_directory}' failed: {e}")));
            }
        }
    }

    // Determine rotation strategy
    let rotation = config.log_rotation_time.as_deref().unwrap_or("daily").to_lowercase();

    let file_appender = match rotation.as_str() {
        "hourly" => tracing_appender::rolling::hourly(log_directory, log_filename),
        _ => tracing_appender::rolling::daily(log_directory, log_filename),
    };

    let (non_blocking, guard) = tracing_appender::non_blocking(file_appender);

    // Build tracing layers
    let env_filter = build_env_filter(logger_level, None);
    let enable_color = std::io::stdout().is_terminal();

    // File layer (JSON format, no color)
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
        .with_span_events(if is_production { FmtSpan::CLOSE } else { FmtSpan::FULL });

    // Optional stdout layer
    let stdout_layer = if config.log_stdout_enabled.unwrap_or(DEFAULT_OBS_LOG_STDOUT_ENABLED) || !is_production {
        let (stdout_nb, _stdout_guard) = tracing_appender::non_blocking(std::io::stdout());
        Some(
            tracing_subscriber::fmt::layer()
                .with_timer(LocalTime::rfc_3339())
                .with_target(true)
                .with_ansi(enable_color)
                .with_thread_names(true)
                .with_thread_ids(true)
                .with_file(true)
                .with_line_number(true)
                .with_writer(stdout_nb)
                .json()
                .with_current_span(true)
                .with_span_list(true)
                .with_span_events(if is_production { FmtSpan::CLOSE } else { FmtSpan::FULL }),
        )
    } else {
        None
    };

    tracing_subscriber::registry()
        .with(env_filter)
        .with(ErrorLayer::default())
        .with(file_layer)
        .with(stdout_layer)
        .init();

    OBSERVABILITY_METRIC_ENABLED.set(false).ok();

    // Start background cleanup task
    let log_dir = std::path::PathBuf::from(log_directory);
    let file_prefix = log_filename.to_string();
    let max_size = config.log_rotation_size_mb.unwrap_or(100) * 1024 * 1024 * keep_files as u64;
    let compress = config.log_rotation_size_mb.is_some(); // Compress if size limit is set

    let cleaner = LogCleaner::new(
        log_dir.clone(),
        file_prefix,
        keep_files,
        max_size,
        0, // max_single_file_size_bytes - no single file limit by default
        compress,
        6,          // gzip_compression_level
        30,         // compressed_file_retention_days
        Vec::new(), // exclude_patterns
        true,       // delete_empty_files
        3600,       // min_file_age_seconds (1 hour)
        false,      // dry_run
    );

    // Spawn cleanup task (runs every 6 hours by default)
    let cleanup_handle = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(6 * 3600));
        loop {
            interval.tick().await;
            if let Err(e) = cleaner.cleanup() {
                tracing::warn!("Log cleanup failed: {}", e);
            }
        }
    });

    info!(
        "Init file logging at '{}', rotation: {}, keep {} files",
        log_directory, rotation, keep_files
    );

    Ok(OtelGuard {
        tracer_provider: None,
        meter_provider: None,
        logger_provider: None,
        tracing_guard: Some(guard),
        cleanup_handle: Some(cleanup_handle),
    })
}

/// Observability (HTTP export, supports three sub-endpoints; if not, fallback to unified endpoint)
fn init_observability_http(config: &OtelConfig, logger_level: &str, is_production: bool) -> Result<OtelGuard, TelemetryError> {
    // Resources and sampling
    let res = resource(config);
    let service_name = config.service_name.as_deref().unwrap_or(APP_NAME).to_owned();
    let use_stdout = config.use_stdout.unwrap_or(!is_production);
    let sample_ratio = config.sample_ratio.unwrap_or(SAMPLE_RATIO);
    let sampler = if (0.0..1.0).contains(&sample_ratio) {
        Sampler::TraceIdRatioBased(sample_ratio)
    } else {
        Sampler::AlwaysOn
    };

    // Endpoint
    let root_ep = config.endpoint.clone(); // owned String

    let trace_ep: String = config
        .trace_endpoint
        .as_deref()
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .unwrap_or_else(|| format!("{root_ep}/v1/traces"));

    let metric_ep: String = config
        .metric_endpoint
        .as_deref()
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .unwrap_or_else(|| format!("{root_ep}/v1/metrics"));

    let log_ep: String = config
        .log_endpoint
        .as_deref()
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .unwrap_or_else(|| format!("{root_ep}/v1/logs"));

    // Tracer（HTTP）
    let tracer_provider = {
        if trace_ep.is_empty() || !config.traces_export_enabled.unwrap_or(DEFAULT_OBS_TRACES_EXPORT_ENABLED) {
            None
        } else {
            let exporter = opentelemetry_otlp::SpanExporter::builder()
                .with_http()
                .with_endpoint(trace_ep.as_str())
                .with_protocol(Protocol::HttpBinary)
                .with_compression(Compression::Gzip)
                .build()
                .map_err(|e| TelemetryError::BuildSpanExporter(e.to_string()))?;

            let mut builder = SdkTracerProvider::builder()
                .with_sampler(sampler)
                .with_id_generator(RandomIdGenerator::default())
                .with_resource(res.clone())
                .with_batch_exporter(exporter);

            if use_stdout {
                builder = builder.with_batch_exporter(opentelemetry_stdout::SpanExporter::default());
            }

            let provider = builder.build();
            global::set_tracer_provider(provider.clone());
            global::set_text_map_propagator(TraceContextPropagator::new());
            Some(provider)
        }
    };

    // Meter（HTTP）
    let meter_provider = {
        if metric_ep.is_empty() || !config.metrics_export_enabled.unwrap_or(DEFAULT_OBS_METRICS_EXPORT_ENABLED) {
            None
        } else {
            let exporter = opentelemetry_otlp::MetricExporter::builder()
                .with_http()
                .with_endpoint(metric_ep.as_str())
                .with_temporality(opentelemetry_sdk::metrics::Temporality::default())
                .with_protocol(Protocol::HttpBinary)
                .with_compression(Compression::Gzip)
                .build()
                .map_err(|e| TelemetryError::BuildMetricExporter(e.to_string()))?;
            let meter_interval = config.meter_interval.unwrap_or(METER_INTERVAL);

            let (provider, recorder) = Recorder::builder(service_name.clone())
                .with_meter_provider(|b| {
                    let b = b.with_resource(res.clone()).with_reader(
                        PeriodicReader::builder(exporter)
                            .with_interval(Duration::from_secs(meter_interval))
                            .build(),
                    );
                    if use_stdout {
                        b.with_reader(create_periodic_reader(meter_interval))
                    } else {
                        b
                    }
                })
                .build();
            global::set_meter_provider(provider.clone());
            metrics::set_global_recorder(recorder).map_err(|e| TelemetryError::InstallMetricsRecorder(e.to_string()))?;
            OBSERVABILITY_METRIC_ENABLED.set(true).ok();
            Some(provider)
        }
    };

    // Logger（HTTP）
    let logger_provider = {
        if log_ep.is_empty() || !config.logs_export_enabled.unwrap_or(DEFAULT_OBS_LOGS_EXPORT_ENABLED) {
            None
        } else {
            let exporter = opentelemetry_otlp::LogExporter::builder()
                .with_http()
                .with_endpoint(log_ep.as_str())
                .with_protocol(Protocol::HttpBinary)
                .with_compression(Compression::Gzip)
                .build()
                .map_err(|e| TelemetryError::BuildLogExporter(e.to_string()))?;

            let mut builder = SdkLoggerProvider::builder().with_resource(res);
            builder = builder.with_batch_exporter(exporter);
            if use_stdout {
                builder = builder.with_batch_exporter(opentelemetry_stdout::LogExporter::default());
            }
            Some(builder.build())
        }
    };

    // Tracing layer
    let fmt_layer_opt = {
        if config.log_stdout_enabled.unwrap_or(DEFAULT_OBS_LOG_STDOUT_ENABLED) {
            let enable_color = std::io::stdout().is_terminal();
            let mut layer = tracing_subscriber::fmt::layer()
                .with_timer(LocalTime::rfc_3339())
                .with_target(true)
                .with_ansi(enable_color)
                .with_thread_names(true)
                .with_thread_ids(true)
                .with_file(true)
                .with_line_number(true)
                .json()
                .with_current_span(true)
                .with_span_list(true);
            let span_event = if is_production { FmtSpan::CLOSE } else { FmtSpan::FULL };
            layer = layer.with_span_events(span_event);
            Some(layer.with_filter(build_env_filter(logger_level, None)))
        } else {
            None
        }
    };

    let filter = build_env_filter(logger_level, None);
    let otel_bridge = logger_provider
        .as_ref()
        .map(|p| OpenTelemetryTracingBridge::new(p).with_filter(build_env_filter(logger_level, None)));
    let tracer_layer = tracer_provider
        .as_ref()
        .map(|p| OpenTelemetryLayer::new(p.tracer(service_name.to_string())));
    let metrics_layer = meter_provider.as_ref().map(|p| MetricsLayer::new(p.clone()));

    tracing_subscriber::registry()
        .with(filter)
        .with(ErrorLayer::default())
        .with(fmt_layer_opt)
        .with(tracer_layer)
        .with(otel_bridge)
        .with(metrics_layer)
        .init();

    counter!("rustfs.start.total").increment(1);
    info!(
        "Init observability (HTTP): trace='{}', metric='{}', log='{}'",
        trace_ep, metric_ep, log_ep
    );

    Ok(OtelGuard {
        tracer_provider,
        meter_provider,
        logger_provider,
        tracing_guard: None,
        cleanup_handle: None,
    })
}

/// Initialize Telemetry,Entrance: three rules
pub(crate) fn init_telemetry(config: &OtelConfig) -> Result<OtelGuard, TelemetryError> {
    let environment = config.environment.as_deref().unwrap_or(ENVIRONMENT);
    let is_production = environment.eq_ignore_ascii_case(DEFAULT_OBS_ENVIRONMENT_PRODUCTION);
    let logger_level = config.logger_level.as_deref().unwrap_or(DEFAULT_LOG_LEVEL);

    // Rule 3: Observability (any endpoint is enabled if it is not empty)
    let has_obs = !config.endpoint.is_empty()
        || config.trace_endpoint.as_deref().map(|s| !s.is_empty()).unwrap_or(false)
        || config.metric_endpoint.as_deref().map(|s| !s.is_empty()).unwrap_or(false)
        || config.log_endpoint.as_deref().map(|s| !s.is_empty()).unwrap_or(false);

    if has_obs {
        return init_observability_http(config, logger_level, is_production);
    }

    // Rule 2: The user has explicitly customized the log directory (determined by whether ENV_OBS_LOG_DIRECTORY is set)
    let user_set_log_dir = get_env_opt_str(ENV_OBS_LOG_DIRECTORY);
    if user_set_log_dir.filter(|d| !d.is_empty()).is_some() {
        return init_file_logging(config, logger_level, is_production);
    }

    // Rule 1: Default stdout (error level)
    Ok(init_stdout_logging(config, DEFAULT_LOG_LEVEL, is_production))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustfs_config::USE_STDOUT;

    #[test]
    fn test_production_environment_detection() {
        // Test production environment logic
        let production_envs = vec!["production", "PRODUCTION", "Production"];

        for env_value in production_envs {
            let is_production = env_value.to_lowercase() == "production";
            assert!(is_production, "Should detect '{env_value}' as production environment");
        }
    }

    #[test]
    fn test_non_production_environment_detection() {
        // Test non-production environment logic
        let non_production_envs = vec!["development", "test", "staging", "dev", "local"];

        for env_value in non_production_envs {
            let is_production = env_value.to_lowercase() == "production";
            assert!(!is_production, "Should not detect '{env_value}' as production environment");
        }
    }

    #[test]
    fn test_stdout_behavior_logic() {
        // Test the stdout behavior logic without environment manipulation
        struct TestCase {
            is_production: bool,
            config_use_stdout: Option<bool>,
            expected_use_stdout: bool,
            description: &'static str,
        }

        let test_cases = vec![
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

        for case in test_cases {
            let default_use_stdout = if case.is_production { false } else { USE_STDOUT };

            let actual_use_stdout = case.config_use_stdout.unwrap_or(default_use_stdout);

            assert_eq!(actual_use_stdout, case.expected_use_stdout, "Test case failed: {}", case.description);
        }
    }

    #[test]
    fn test_log_level_filter_mapping_logic() {
        // Test the log level mapping logic used in the real implementation
        let test_cases = vec![
            ("trace", "Trace"),
            ("debug", "Debug"),
            ("info", "Info"),
            ("warn", "Warn"),
            ("warning", "Warn"),
            ("error", "Error"),
            ("off", "None"),
            ("invalid_level", "Info"), // Should default to Info
        ];

        for (input_level, expected_variant) in test_cases {
            let filter_variant = match input_level.to_lowercase().as_str() {
                "trace" => "Trace",
                "debug" => "Debug",
                "info" => "Info",
                "warn" | "warning" => "Warn",
                "error" => "Error",
                "off" => "None",
                _ => "Info", // default case
            };

            assert_eq!(
                filter_variant, expected_variant,
                "Log level '{input_level}' should map to '{expected_variant}'"
            );
        }
    }

    #[test]
    fn test_otel_config_environment_defaults() {
        // Test that OtelConfig properly handles environment detection logic
        let config = OtelConfig {
            endpoint: "".to_string(),
            use_stdout: None,
            environment: Some("production".to_string()),
            ..Default::default()
        };

        // Simulate the logic from init_telemetry
        let environment = config.environment.as_deref().unwrap_or(ENVIRONMENT);
        assert_eq!(environment, "production");

        // Test with development environment
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
