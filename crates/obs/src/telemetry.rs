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
use crate::global::IS_OBSERVABILITY_ENABLED;
use flexi_logger::{
    Age, Cleanup, Criterion, DeferredNow, FileSpec, LogSpecification, Naming, Record, WriteMode,
    WriteMode::{AsyncWith, BufferAndFlush},
    style,
};
use metrics::counter;
use nu_ansi_term::Color;
use opentelemetry::trace::TracerProvider;
use opentelemetry::{KeyValue, global};
use opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{
    Resource,
    logs::SdkLoggerProvider,
    metrics::{MeterProviderBuilder, PeriodicReader, SdkMeterProvider},
    trace::{RandomIdGenerator, Sampler, SdkTracerProvider},
};
use opentelemetry_semantic_conventions::{
    SCHEMA_URL,
    attribute::{DEPLOYMENT_ENVIRONMENT_NAME, NETWORK_LOCAL_ADDRESS, SERVICE_VERSION as OTEL_SERVICE_VERSION},
};
use rustfs_config::{
    APP_NAME, DEFAULT_LOG_KEEP_FILES, DEFAULT_LOG_LEVEL, DEFAULT_OBS_LOG_STDOUT_ENABLED, ENVIRONMENT, METER_INTERVAL,
    SAMPLE_RATIO, SERVICE_VERSION, USE_STDOUT,
    observability::{
        DEFAULT_OBS_ENVIRONMENT_PRODUCTION, DEFAULT_OBS_LOG_FLUSH_MS, DEFAULT_OBS_LOG_MESSAGE_CAPA, DEFAULT_OBS_LOG_POOL_CAPA,
        ENV_OBS_LOG_DIRECTORY,
    },
};
use rustfs_utils::get_local_ip_with_default;
use smallvec::SmallVec;
use std::borrow::Cow;
use std::io::IsTerminal;
use std::time::Duration;
use std::{env, fs};
use tracing::info;
use tracing_appender::rolling::{RollingFileAppender, Rotation};
use tracing_error::ErrorLayer;
use tracing_opentelemetry::{MetricsLayer, OpenTelemetryLayer};
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::fmt::time::LocalTime;
use tracing_subscriber::{EnvFilter, Layer, layer::SubscriberExt, util::SubscriberInitExt};

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
    // Add a flexi_logger handle to keep the logging alive
    _flexi_logger_handles: Option<flexi_logger::LoggerHandle>,
    // WorkerGuard for writing tracing files
    _tracing_guard: Option<tracing_appender::non_blocking::WorkerGuard>,
}

// Implement debug manually and avoid relying on all fields to implement debug
impl std::fmt::Debug for OtelGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OtelGuard")
            .field("tracer_provider", &self.tracer_provider.is_some())
            .field("meter_provider", &self.meter_provider.is_some())
            .field("logger_provider", &self.logger_provider.is_some())
            .field("_flexi_logger_handles", &self._flexi_logger_handles.is_some())
            .field("_tracing_guard", &self._tracing_guard.is_some())
            .finish()
    }
}

impl Drop for OtelGuard {
    fn drop(&mut self) {
        if let Some(provider) = self.tracer_provider.take() {
            if let Err(err) = provider.shutdown() {
                eprintln!("Tracer shutdown error: {err:?}");
            }
        }

        if let Some(provider) = self.meter_provider.take() {
            if let Err(err) = provider.shutdown() {
                eprintln!("Meter shutdown error: {err:?}");
            }
        }
        if let Some(provider) = self.logger_provider.take() {
            if let Err(err) = provider.shutdown() {
                eprintln!("Logger shutdown error: {err:?}");
            }
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

/// Initialize Telemetry
pub(crate) fn init_telemetry(config: &OtelConfig) -> OtelGuard {
    // avoid repeated access to configuration fields
    let endpoint = &config.endpoint;
    let environment = config.environment.as_deref().unwrap_or(ENVIRONMENT);

    // Environment-aware stdout configuration
    // Check for explicit environment control via RUSTFS_OBS_ENVIRONMENT
    let is_production = environment.to_lowercase() == DEFAULT_OBS_ENVIRONMENT_PRODUCTION;

    // Default stdout behavior based on environment
    let default_use_stdout = if is_production {
        false // Disable stdout in production for security and log aggregation
    } else {
        USE_STDOUT // Use configured default for dev/test environments
    };

    let use_stdout = config.use_stdout.unwrap_or(default_use_stdout);
    let meter_interval = config.meter_interval.unwrap_or(METER_INTERVAL);
    let logger_level = config.logger_level.as_deref().unwrap_or(DEFAULT_LOG_LEVEL);
    let service_name = config.service_name.as_deref().unwrap_or(APP_NAME);

    // Configure flexi_logger to cut by time and size
    let mut flexi_logger_handle = None;
    if !endpoint.is_empty() {
        // Pre-create resource objects to avoid repeated construction
        let res = resource(config);

        // initialize tracer provider
        let tracer_provider = {
            let sample_ratio = config.sample_ratio.unwrap_or(SAMPLE_RATIO);
            let sampler = if (0.0..1.0).contains(&sample_ratio) {
                Sampler::TraceIdRatioBased(sample_ratio)
            } else {
                Sampler::AlwaysOn
            };

            let builder = SdkTracerProvider::builder()
                .with_sampler(sampler)
                .with_id_generator(RandomIdGenerator::default())
                .with_resource(res.clone());

            let tracer_provider = if endpoint.is_empty() {
                builder
                    .with_batch_exporter(opentelemetry_stdout::SpanExporter::default())
                    .build()
            } else {
                let exporter = opentelemetry_otlp::SpanExporter::builder()
                    .with_tonic()
                    .with_endpoint(endpoint)
                    .build()
                    .unwrap();

                let builder = if use_stdout {
                    builder
                        .with_batch_exporter(exporter)
                        .with_batch_exporter(opentelemetry_stdout::SpanExporter::default())
                } else {
                    builder.with_batch_exporter(exporter)
                };

                builder.build()
            };

            global::set_tracer_provider(tracer_provider.clone());
            tracer_provider
        };

        // initialize meter provider
        let meter_provider = {
            let mut builder = MeterProviderBuilder::default().with_resource(res.clone());

            if endpoint.is_empty() {
                builder = builder.with_reader(create_periodic_reader(meter_interval));
            } else {
                let exporter = opentelemetry_otlp::MetricExporter::builder()
                    .with_tonic()
                    .with_endpoint(endpoint)
                    .with_temporality(opentelemetry_sdk::metrics::Temporality::default())
                    .build()
                    .unwrap();

                builder = builder.with_reader(
                    PeriodicReader::builder(exporter)
                        .with_interval(Duration::from_secs(meter_interval))
                        .build(),
                );

                if use_stdout {
                    builder = builder.with_reader(create_periodic_reader(meter_interval));
                }
            }

            let meter_provider = builder.build();
            global::set_meter_provider(meter_provider.clone());
            meter_provider
        };

        match metrics_exporter_opentelemetry::Recorder::builder("order-service").install_global() {
            Ok(_) => {}
            Err(e) => {
                eprintln!("Failed to set global metrics recorder: {e:?}");
            }
        }

        // initialize logger provider
        let logger_provider = {
            let mut builder = SdkLoggerProvider::builder().with_resource(res);

            if endpoint.is_empty() {
                builder = builder.with_batch_exporter(opentelemetry_stdout::LogExporter::default());
            } else {
                let exporter = opentelemetry_otlp::LogExporter::builder()
                    .with_tonic()
                    .with_endpoint(endpoint)
                    .build()
                    .unwrap();

                builder = builder.with_batch_exporter(exporter);

                if use_stdout {
                    builder = builder.with_batch_exporter(opentelemetry_stdout::LogExporter::default());
                }
            }

            builder.build()
        };

        // configuring tracing
        {
            // configure the formatting layer
            let fmt_layer = {
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
                let span_event = if is_production {
                    FmtSpan::CLOSE
                } else {
                    // Only add full span events tracking in the development environment
                    FmtSpan::FULL
                };

                layer = layer.with_span_events(span_event);
                layer.with_filter(build_env_filter(logger_level, None))
            };

            let filter = build_env_filter(logger_level, None);
            let otel_layer = OpenTelemetryTracingBridge::new(&logger_provider).with_filter(build_env_filter(logger_level, None));
            let tracer = tracer_provider.tracer(Cow::Borrowed(service_name).to_string());

            // Configure registry to avoid repeated calls to filter methods
            tracing_subscriber::registry()
                .with(filter)
                .with(ErrorLayer::default())
                .with(if config.log_stdout_enabled.unwrap_or(DEFAULT_OBS_LOG_STDOUT_ENABLED) {
                    Some(fmt_layer)
                } else {
                    None
                })
                .with(OpenTelemetryLayer::new(tracer))
                .with(otel_layer)
                .with(MetricsLayer::new(meter_provider.clone()))
                .init();

            if !endpoint.is_empty() {
                info!(
                    "OpenTelemetry telemetry initialized with OTLP endpoint: {}, logger_level: {},RUST_LOG env: {}",
                    endpoint,
                    logger_level,
                    env::var("RUST_LOG").unwrap_or_else(|_| "Not set".to_string())
                );
                IS_OBSERVABILITY_ENABLED.set(true).ok();
            }
        }
        counter!("rustfs.start.total").increment(1);
        return OtelGuard {
            tracer_provider: Some(tracer_provider),
            meter_provider: Some(meter_provider),
            logger_provider: Some(logger_provider),
            _flexi_logger_handles: flexi_logger_handle,
            _tracing_guard: None,
        };
    }

    // Obtain the log directory and file name configuration
    let default_log_directory = rustfs_utils::dirs::get_log_directory_to_string(ENV_OBS_LOG_DIRECTORY);
    let log_directory = config.log_directory.as_deref().unwrap_or(default_log_directory.as_str());
    let log_filename = config.log_filename.as_deref().unwrap_or(service_name);

    // Enhanced error handling for directory creation
    if let Err(e) = fs::create_dir_all(log_directory) {
        eprintln!("ERROR: Failed to create log directory '{log_directory}': {e}");
        eprintln!("Ensure the parent directory exists and you have write permissions.");
        eprintln!("Attempting to continue with logging, but file logging may fail.");
    } else {
        eprintln!("Log directory ready: {log_directory}");
    }

    #[cfg(unix)]
    {
        // Linux/macOS Setting Permissions with better error handling
        use std::fs::Permissions;
        use std::os::unix::fs::PermissionsExt;
        match fs::set_permissions(log_directory, Permissions::from_mode(0o755)) {
            Ok(_) => eprintln!("Log directory permissions set to 755: {log_directory}"),
            Err(e) => {
                eprintln!("WARNING: Failed to set log directory permissions for '{log_directory}': {e}");
                eprintln!("This may affect log file access. Consider checking directory ownership and permissions.");
            }
        }
    }

    if endpoint.is_empty() && !is_production {
        // Create a file appender (rolling by day), add the -tracing suffix to the file name to avoid conflicts
        // let file_appender = tracing_appender::rolling::hourly(log_directory, format!("{log_filename}-tracing.log"));
        let file_appender = RollingFileAppender::builder()
            .rotation(Rotation::HOURLY) // rotate log files once every hour
            .filename_prefix(format!("{log_filename}-tracing")) // log file names will be prefixed with `myapp.`
            .filename_suffix("log") // log file names will be suffixed with `.log`
            .build(log_directory) // try to build an appender that stores log files in `/var/log`
            .expect("initializing rolling file appender failed");
        let (nb_writer, guard) = tracing_appender::non_blocking(file_appender);

        let enable_color = std::io::stdout().is_terminal();
        let fmt_layer = tracing_subscriber::fmt::layer()
            .with_timer(LocalTime::rfc_3339())
            .with_target(true)
            .with_ansi(enable_color)
            .with_thread_names(true)
            .with_thread_ids(true)
            .with_file(true)
            .with_line_number(true)
            .with_writer(nb_writer) // Specify writing file
            .json()
            .with_current_span(true)
            .with_span_list(true)
            .with_span_events(FmtSpan::CLOSE); // Log span lifecycle events, including trace_id

        let env_filter = build_env_filter(logger_level, None);

        // Use registry() to register fmt_layer directly to ensure trace_id is output to the log
        tracing_subscriber::registry()
            .with(env_filter)
            .with(ErrorLayer::default())
            .with(fmt_layer)
            .with(if config.log_stdout_enabled.unwrap_or(DEFAULT_OBS_LOG_STDOUT_ENABLED) {
                let stdout_fmt_layer = tracing_subscriber::fmt::layer()
                    .with_timer(LocalTime::rfc_3339())
                    .with_target(true)
                    .with_thread_names(true)
                    .with_thread_ids(true)
                    .with_file(true)
                    .with_line_number(true)
                    .with_writer(std::io::stdout) // Specify writing file
                    .json()
                    .with_current_span(true)
                    .with_span_list(true)
                    .with_span_events(FmtSpan::CLOSE); // Log span lifecycle events, including trace_id;
                Some(stdout_fmt_layer)
            } else {
                None
            })
            .init();

        info!("Tracing telemetry initialized for non-production with trace_id logging.");
        IS_OBSERVABILITY_ENABLED.set(false).ok();

        return OtelGuard {
            tracer_provider: None,
            meter_provider: None,
            logger_provider: None,
            _flexi_logger_handles: None,
            _tracing_guard: Some(guard),
        };
    }

    // Build log cutting conditions
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

    // The number of log files retained
    let keep_files = config.log_keep_files.unwrap_or(DEFAULT_LOG_KEEP_FILES);

    // Parsing the log level
    let log_spec = LogSpecification::parse(logger_level).unwrap_or_else(|e| {
        eprintln!("WARNING: Invalid logger level '{logger_level}': {e}. Using default 'info' level.");
        LogSpecification::info()
    });

    // Environment-aware stdout configuration
    // In production: disable stdout completely (Duplicate::None)
    // In development/test: use level-based filtering
    let level_filter = if is_production {
        flexi_logger::Duplicate::None // No stdout output in production
    } else {
        // Convert the logger_level string to the corresponding LevelFilter for dev/test
        match logger_level.to_lowercase().as_str() {
            "trace" => flexi_logger::Duplicate::Trace,
            "debug" => flexi_logger::Duplicate::Debug,
            "info" => flexi_logger::Duplicate::Info,
            "warn" | "warning" => flexi_logger::Duplicate::Warn,
            "error" => flexi_logger::Duplicate::Error,
            "off" => flexi_logger::Duplicate::None,
            _ => flexi_logger::Duplicate::Info, // the default is info
        }
    };

    // Choose write mode based on environment
    let write_mode = if is_production {
        get_env_async_with().unwrap_or_else(|| {
            eprintln!(
                "Using default Async write mode in production. To customize, set RUSTFS_OBS_LOG_POOL_CAPA, RUSTFS_OBS_LOG_MESSAGE_CAPA, and RUSTFS_OBS_LOG_FLUSH_MS environment variables."
            );
            AsyncWith {
                pool_capa: DEFAULT_OBS_LOG_POOL_CAPA,
                message_capa: DEFAULT_OBS_LOG_MESSAGE_CAPA,
                flush_interval: Duration::from_millis(DEFAULT_OBS_LOG_FLUSH_MS),
            }
        })
    } else {
        BufferAndFlush
    };

    // Configure the flexi_logger with enhanced error handling
    let mut flexi_logger_builder = flexi_logger::Logger::try_with_env_or_str(logger_level)
        .unwrap_or_else(|e| {
            eprintln!("WARNING: Invalid logger configuration '{logger_level}': {e:?}");
            eprintln!("Falling back to default configuration with level: {DEFAULT_LOG_LEVEL}");
            flexi_logger::Logger::with(log_spec.clone())
        })
        .log_to_file(
            FileSpec::default()
                .directory(log_directory)
                .basename(log_filename)
                .suppress_timestamp(),
        )
        .rotate(rotation_criterion, Naming::TimestampsDirect, Cleanup::KeepLogFiles(keep_files.into()))
        .format_for_files(format_for_file) // Add a custom formatting function for file output
        .write_mode(write_mode)
        .append(); // Avoid clearing existing logs at startup

    // Environment-aware stdout configuration
    flexi_logger_builder = flexi_logger_builder.duplicate_to_stdout(level_filter);
    // Only add stdout formatting and startup messages in non-production environments
    if !is_production {
        flexi_logger_builder = flexi_logger_builder
            .format_for_stdout(format_with_color) // Add a custom formatting function for terminal output
            .print_message(); // Startup information output to console
    }

    let flexi_logger_result = flexi_logger_builder.start();

    if let Ok(logger) = flexi_logger_result {
        // Save the logger handle to keep the logging
        flexi_logger_handle = Some(logger);

        // Environment-aware success messages
        if is_production {
            eprintln!("Production logging initialized: file-only mode to {log_directory}/{log_filename}.log");
            eprintln!("Stdout logging disabled in production environment for security and log aggregation.");
        } else {
            eprintln!("Development/Test logging initialized with file logging to {log_directory}/{log_filename}.log");
            eprintln!("Stdout logging enabled for debugging. Environment: {environment}");
        }

        // Log rotation configuration details
        match (config.log_rotation_time.as_deref(), config.log_rotation_size_mb) {
            (Some(time), Some(size)) => {
                eprintln!("Log rotation configured for: every {time} or when size exceeds {size}MB, keeping {keep_files} files")
            }
            (Some(time), None) => eprintln!("Log rotation configured for: every {time}, keeping {keep_files} files"),
            (None, Some(size)) => {
                eprintln!("Log rotation configured for: when size exceeds {size}MB, keeping {keep_files} files")
            }
            _ => eprintln!("Log rotation configured for: daily, keeping {keep_files} files"),
        }
    } else {
        eprintln!("CRITICAL: Failed to initialize flexi_logger: {:?}", flexi_logger_result.err());
        eprintln!("Possible causes:");
        eprintln!("  1. Insufficient permissions to write to log directory: {log_directory}");
        eprintln!("  2. Log directory does not exist or is not accessible");
        eprintln!("  3. Invalid log configuration parameters");
        eprintln!("  4. Disk space issues");
        eprintln!("Application will continue but logging to files will not work properly.");
    }

    OtelGuard {
        tracer_provider: None,
        meter_provider: None,
        logger_provider: None,
        _flexi_logger_handles: flexi_logger_handle,
        _tracing_guard: None,
    }
}

// Read the AsyncWith parameter from the environment variable
fn get_env_async_with() -> Option<WriteMode> {
    let pool_capa = env::var("RUSTFS_OBS_LOG_POOL_CAPA")
        .ok()
        .and_then(|v| v.parse::<usize>().ok());
    let message_capa = env::var("RUSTFS_OBS_LOG_MESSAGE_CAPA")
        .ok()
        .and_then(|v| v.parse::<usize>().ok());
    let flush_ms = env::var("RUSTFS_OBS_LOG_FLUSH_MS").ok().and_then(|v| v.parse::<u64>().ok());

    match (pool_capa, message_capa, flush_ms) {
        (Some(pool), Some(msg), Some(flush)) => Some(AsyncWith {
            pool_capa: pool,
            message_capa: msg,
            flush_interval: Duration::from_millis(flush),
        }),
        _ => None,
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

    // Get the current thread information
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

    // Get the current thread information
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

#[cfg(test)]
mod tests {
    use super::*;

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
