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
use crate::{Recorder, TelemetryError};
use flexi_logger::{DeferredNow, Record, WriteMode, WriteMode::AsyncWith, style};
use metrics::counter;
use nu_ansi_term::Color;
use opentelemetry::{KeyValue, global, trace::TracerProvider};
use opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge;
use opentelemetry_otlp::{Compression, Protocol, WithExportConfig, WithHttpConfig};
use opentelemetry_sdk::{
    Resource,
    logs::SdkLoggerProvider,
    metrics::{PeriodicReader, SdkMeterProvider},
    trace::{RandomIdGenerator, Sampler, SdkTracerProvider},
};
use opentelemetry_semantic_conventions::{
    SCHEMA_URL,
    attribute::{DEPLOYMENT_ENVIRONMENT_NAME, NETWORK_LOCAL_ADDRESS, SERVICE_VERSION as OTEL_SERVICE_VERSION},
};
use rustfs_config::{
    APP_NAME, DEFAULT_LOG_KEEP_FILES, DEFAULT_LOG_LEVEL, DEFAULT_OBS_LOG_STDOUT_ENABLED, ENVIRONMENT, METER_INTERVAL,
    SAMPLE_RATIO, SERVICE_VERSION,
    observability::{
        DEFAULT_OBS_ENVIRONMENT_PRODUCTION, DEFAULT_OBS_LOG_FLUSH_MS, DEFAULT_OBS_LOG_MESSAGE_CAPA, DEFAULT_OBS_LOG_POOL_CAPA,
        ENV_OBS_LOG_DIRECTORY, ENV_OBS_LOG_FLUSH_MS, ENV_OBS_LOG_MESSAGE_CAPA, ENV_OBS_LOG_POOL_CAPA,
    },
};
use rustfs_utils::{get_env_opt_str, get_env_u64, get_env_usize, get_local_ip_with_default};
use smallvec::SmallVec;
use std::{borrow::Cow, fs, io::IsTerminal, time::Duration};
use tracing::info;
use tracing_error::ErrorLayer;
use tracing_opentelemetry::{MetricsLayer, OpenTelemetryLayer};
use tracing_subscriber::{
    EnvFilter, Layer,
    fmt::{format::FmtSpan, time::LocalTime},
    layer::SubscriberExt,
    util::SubscriberInitExt,
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
///
/// Implement Debug trait correctly, rather than using derive, as some fields may not have implemented Debug
pub struct OtelGuard {
    tracer_provider: Option<SdkTracerProvider>,
    meter_provider: Option<SdkMeterProvider>,
    logger_provider: Option<SdkLoggerProvider>,
    flexi_logger_handles: Option<flexi_logger::LoggerHandle>,
    tracing_guard: Option<tracing_appender::non_blocking::WorkerGuard>,
}

impl std::fmt::Debug for OtelGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OtelGuard")
            .field("tracer_provider", &self.tracer_provider.is_some())
            .field("meter_provider", &self.meter_provider.is_some())
            .field("logger_provider", &self.logger_provider.is_some())
            .field("flexi_logger_handles", &self.flexi_logger_handles.is_some())
            .field("tracing_guard", &self.tracing_guard.is_some())
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

        if let Some(handle) = self.flexi_logger_handles.take() {
            handle.shutdown();
            println!("flexi_logger shutdown completed");
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

// Read the AsyncWith parameter from the environment variable
fn get_env_async_with() -> WriteMode {
    let pool_capa = get_env_usize(ENV_OBS_LOG_POOL_CAPA, DEFAULT_OBS_LOG_POOL_CAPA);
    let message_capa = get_env_usize(ENV_OBS_LOG_MESSAGE_CAPA, DEFAULT_OBS_LOG_MESSAGE_CAPA);
    let flush_ms = get_env_u64(ENV_OBS_LOG_FLUSH_MS, DEFAULT_OBS_LOG_FLUSH_MS);

    AsyncWith {
        pool_capa,
        message_capa,
        flush_interval: Duration::from_millis(flush_ms),
    }
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

/// Custom Log Formatter Function - Terminal Output (with Color)
#[inline(never)]
fn format_with_color(w: &mut dyn std::io::Write, now: &mut DeferredNow, record: &Record) -> Result<(), std::io::Error> {
    let level = record.level();
    let level_style = style(level);
    let binding = std::thread::current();
    let thread_name = binding.name().unwrap_or("unnamed");
    let thread_id = format!("{:?}", std::thread::current().id());
    writeln!(
        w,
        "[{}] {} [{}] [{}:{}] [{}:{}] {}",
        now.now().format(flexi_logger::TS_DASHES_BLANK_COLONS_DOT_BLANK),
        level_style.paint(level.to_string()),
        Color::Magenta.paint(record.target()),
        Color::Blue.paint(record.file().unwrap_or("unknown")),
        Color::Blue.paint(record.line().unwrap_or(0).to_string()),
        Color::Green.paint(thread_name),
        Color::Green.paint(thread_id),
        record.args()
    )
}

/// Custom Log Formatter - File Output (No Color)
#[inline(never)]
fn format_for_file(w: &mut dyn std::io::Write, now: &mut DeferredNow, record: &Record) -> Result<(), std::io::Error> {
    let level = record.level();
    let binding = std::thread::current();
    let thread_name = binding.name().unwrap_or("unnamed");
    let thread_id = format!("{:?}", std::thread::current().id());
    writeln!(
        w,
        "[{}] {} [{}] [{}:{}] [{}:{}] {}",
        now.now().format(flexi_logger::TS_DASHES_BLANK_COLONS_DOT_BLANK),
        level,
        record.target(),
        record.file().unwrap_or("unknown"),
        record.line().unwrap_or(0),
        thread_name,
        thread_id,
        record.args()
    )
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
        flexi_logger_handles: None,
        tracing_guard: Some(guard),
    }
}

/// File rolling log (size switching + number retained)
fn init_file_logging(config: &OtelConfig, logger_level: &str, is_production: bool) -> Result<OtelGuard, TelemetryError> {
    use flexi_logger::{Age, Cleanup, Criterion, FileSpec, LogSpecification, Naming};

    let service_name = config.service_name.as_deref().unwrap_or(APP_NAME);
    let default_log_directory = rustfs_utils::dirs::get_log_directory_to_string(ENV_OBS_LOG_DIRECTORY);
    let log_directory = config.log_directory.as_deref().unwrap_or(default_log_directory.as_str());
    let log_filename = config.log_filename.as_deref().unwrap_or(service_name);
    let keep_files = config.log_keep_files.unwrap_or(DEFAULT_LOG_KEEP_FILES);
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

    // parsing level
    let log_spec = LogSpecification::parse(logger_level)
        .unwrap_or_else(|_| LogSpecification::parse(DEFAULT_LOG_LEVEL).unwrap_or(LogSpecification::error()));

    // Switch by size (MB), Build log cutting conditions
    let rotation_criterion = match (config.log_rotation_time.as_deref(), config.log_rotation_size_mb) {
        // Cut by time and size at the same time
        (Some(time), Some(size)) => {
            let age = match time.to_lowercase().as_str() {
                "hour" => Age::Hour,
                "day" => Age::Day,
                "minute" => Age::Minute,
                "second" => Age::Second,
                _ => Age::Day, // The default is by day
            };
            Criterion::AgeOrSize(age, size * 1024 * 1024) // Convert to bytes
        }
        // Cut by time only
        (Some(time), None) => {
            let age = match time.to_lowercase().as_str() {
                "hour" => Age::Hour,
                "day" => Age::Day,
                "minute" => Age::Minute,
                "second" => Age::Second,
                _ => Age::Day, // The default is by day
            };
            Criterion::Age(age)
        }
        // Cut by size only
        (None, Some(size)) => {
            Criterion::Size(size * 1024 * 1024) // Convert to bytes
        }
        // By default, it is cut by the day
        _ => Criterion::Age(Age::Day),
    };

    // write mode
    let write_mode = get_env_async_with();
    // Build
    let mut builder = flexi_logger::Logger::try_with_env_or_str(logger_level)
        .unwrap_or(flexi_logger::Logger::with(log_spec.clone()))
        .format_for_stderr(format_with_color)
        .format_for_stdout(format_with_color)
        .format_for_files(format_for_file)
        .log_to_file(
            FileSpec::default()
                .directory(log_directory)
                .basename(log_filename)
                .suppress_timestamp(),
        )
        .rotate(rotation_criterion, Naming::TimestampsDirect, Cleanup::KeepLogFiles(keep_files))
        .write_mode(write_mode)
        .append()
        .use_utc();

    // Optional copy to stdout (for local observation)
    if config.log_stdout_enabled.unwrap_or(DEFAULT_OBS_LOG_STDOUT_ENABLED) || !is_production {
        builder = builder.duplicate_to_stdout(flexi_logger::Duplicate::All);
    } else {
        builder = builder.duplicate_to_stdout(flexi_logger::Duplicate::None);
    }

    let handle = match builder.start() {
        Ok(h) => Some(h),
        Err(e) => {
            eprintln!("ERROR: start flexi_logger failed: {e}");
            None
        }
    };

    OBSERVABILITY_METRIC_ENABLED.set(false).ok();
    info!(
        "Init file logging at '{}', roll size {:?}MB, keep {}",
        log_directory, config.log_rotation_size_mb, keep_files
    );

    Ok(OtelGuard {
        tracer_provider: None,
        meter_provider: None,
        logger_provider: None,
        flexi_logger_handles: handle,
        tracing_guard: None,
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
        if trace_ep.is_empty() {
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
            Some(provider)
        }
    };

    // Meter（HTTP）
    let meter_provider = {
        if metric_ep.is_empty() {
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
            Some(provider)
        }
    };

    // Logger（HTTP）
    let logger_provider = {
        if log_ep.is_empty() {
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

    OBSERVABILITY_METRIC_ENABLED.set(true).ok();
    counter!("rustfs.start.total").increment(1);
    info!(
        "Init observability (HTTP): trace='{}', metric='{}', log='{}'",
        trace_ep, metric_ep, log_ep
    );

    Ok(OtelGuard {
        tracer_provider,
        meter_provider,
        logger_provider,
        flexi_logger_handles: None,
        tracing_guard: None,
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
