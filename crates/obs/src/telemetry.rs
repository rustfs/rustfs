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

use crate::OtelConfig;
use flexi_logger::{Age, Cleanup, Criterion, DeferredNow, FileSpec, LogSpecification, Naming, Record, WriteMode, style};
use nu_ansi_term::Color;
use opentelemetry::trace::TracerProvider;
use opentelemetry::{KeyValue, global};
use opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::logs::SdkLoggerProvider;
use opentelemetry_sdk::{
    Resource,
    metrics::{MeterProviderBuilder, PeriodicReader, SdkMeterProvider},
    trace::{RandomIdGenerator, Sampler, SdkTracerProvider},
};
use opentelemetry_semantic_conventions::{
    SCHEMA_URL,
    attribute::{DEPLOYMENT_ENVIRONMENT_NAME, NETWORK_LOCAL_ADDRESS, SERVICE_VERSION as OTEL_SERVICE_VERSION},
};
use rustfs_config::observability::ENV_OBS_LOG_DIRECTORY;
use rustfs_config::{
    APP_NAME, DEFAULT_LOG_KEEP_FILES, DEFAULT_LOG_LEVEL, ENVIRONMENT, METER_INTERVAL, SAMPLE_RATIO, SERVICE_VERSION, USE_STDOUT,
};
use rustfs_utils::get_local_ip_with_default;
use smallvec::SmallVec;
use std::borrow::Cow;
use std::fs;
use std::io::IsTerminal;
use tracing::info;
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
// Implement Debug trait correctly, rather than using derive, as some fields may not have implemented Debug
pub struct OtelGuard {
    tracer_provider: Option<SdkTracerProvider>,
    meter_provider: Option<SdkMeterProvider>,
    logger_provider: Option<SdkLoggerProvider>,
    // Add a flexi_logger handle to keep the logging alive
    _flexi_logger_handles: Option<flexi_logger::LoggerHandle>,
}

// Implement debug manually and avoid relying on all fields to implement debug
impl std::fmt::Debug for OtelGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OtelGuard")
            .field("tracer_provider", &self.tracer_provider.is_some())
            .field("meter_provider", &self.meter_provider.is_some())
            .field("logger_provider", &self.logger_provider.is_some())
            .field("_flexi_logger_handles", &self._flexi_logger_handles.is_some())
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
        .with_interval(std::time::Duration::from_secs(interval))
        .build()
}

/// Initialize Telemetry
pub(crate) fn init_telemetry(config: &OtelConfig) -> OtelGuard {
    // avoid repeated access to configuration fields
    let endpoint = &config.endpoint;
    let use_stdout = config.use_stdout.unwrap_or(USE_STDOUT);
    let meter_interval = config.meter_interval.unwrap_or(METER_INTERVAL);
    let logger_level = config.logger_level.as_deref().unwrap_or(DEFAULT_LOG_LEVEL);
    let service_name = config.service_name.as_deref().unwrap_or(APP_NAME);
    let environment = config.environment.as_deref().unwrap_or(ENVIRONMENT);

    // Configure flexi_logger to cut by time and size
    let mut flexi_logger_handle = None;
    if !endpoint.is_empty() {
        // Pre-create resource objects to avoid repeated construction
        let res = resource(config);

        // initialize tracer provider
        let tracer_provider = {
            let sample_ratio = config.sample_ratio.unwrap_or(SAMPLE_RATIO);
            let sampler = if sample_ratio > 0.0 && sample_ratio < 1.0 {
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
                        .with_interval(std::time::Duration::from_secs(meter_interval))
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
                    .with_line_number(true);

                // Only add full span events tracking in the development environment
                if environment != ENVIRONMENT {
                    layer = layer.with_span_events(FmtSpan::FULL);
                }

                layer.with_filter(build_env_filter(logger_level, None))
            };

            let filter = build_env_filter(logger_level, None);
            let otel_filter = build_env_filter(logger_level, None);
            let otel_layer = OpenTelemetryTracingBridge::new(&logger_provider).with_filter(otel_filter);
            let tracer = tracer_provider.tracer(Cow::Borrowed(service_name).to_string());

            // Configure registry to avoid repeated calls to filter methods
            tracing_subscriber::registry()
                .with(filter)
                .with(ErrorLayer::default())
                .with(if config.local_logging_enabled.unwrap_or(false) {
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
                    std::env::var("RUST_LOG").unwrap_or_else(|_| "Not set".to_string())
                );
            }
        }

        OtelGuard {
            tracer_provider: Some(tracer_provider),
            meter_provider: Some(meter_provider),
            logger_provider: Some(logger_provider),
            _flexi_logger_handles: flexi_logger_handle,
        }
    } else {
        // Obtain the log directory and file name configuration
        let default_log_directory = rustfs_utils::dirs::get_log_directory_to_string(ENV_OBS_LOG_DIRECTORY);
        let log_directory = config.log_directory.as_deref().unwrap_or(default_log_directory.as_str());
        let log_filename = config.log_filename.as_deref().unwrap_or(service_name);

        if let Err(e) = fs::create_dir_all(log_directory) {
            eprintln!("Failed to create log directory {log_directory}: {e}");
        }
        #[cfg(unix)]
        {
            // Linux/macOS Setting Permissions
            // Set the log directory permissions to 755 (rwxr-xr-x)
            use std::fs::Permissions;
            use std::os::unix::fs::PermissionsExt;
            match fs::set_permissions(log_directory, Permissions::from_mode(0o755)) {
                Ok(_) => eprintln!("Log directory permissions set to 755: {log_directory}"),
                Err(e) => eprintln!("Failed to set log directory permissions {log_directory}: {e}"),
            }
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
        let log_spec = LogSpecification::parse(logger_level).unwrap_or(LogSpecification::info());

        // Convert the logger_level string to the corresponding LevelFilter
        let level_filter = match logger_level.to_lowercase().as_str() {
            "trace" => flexi_logger::Duplicate::Trace,
            "debug" => flexi_logger::Duplicate::Debug,
            "info" => flexi_logger::Duplicate::Info,
            "warn" | "warning" => flexi_logger::Duplicate::Warn,
            "error" => flexi_logger::Duplicate::Error,
            "off" => flexi_logger::Duplicate::None,
            _ => flexi_logger::Duplicate::Info, // the default is info
        };

        // Configure the flexi_logger
        let flexi_logger_result = flexi_logger::Logger::try_with_env_or_str(logger_level)
            .unwrap_or_else(|e| {
                eprintln!("Invalid logger level: {logger_level}, using default: {DEFAULT_LOG_LEVEL}, failed error: {e:?}");
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
            .duplicate_to_stdout(level_filter) // Use dynamic levels
            .format_for_stdout(format_with_color) // Add a custom formatting function for terminal output
            .write_mode(WriteMode::BufferAndFlush)
            .append() // Avoid clearing existing logs at startup
            .print_message() // Startup information output to console
            .start();

        if let Ok(logger) = flexi_logger_result {
            // Save the logger handle to keep the logging
            flexi_logger_handle = Some(logger);

            eprintln!("Flexi logger initialized with file logging to {log_directory}/{log_filename}.log");

            // Log logging of log cutting conditions
            match (config.log_rotation_time.as_deref(), config.log_rotation_size_mb) {
                (Some(time), Some(size)) => eprintln!(
                    "Log rotation configured for: every {time} or when size exceeds {size}MB, keeping {keep_files} files"
                ),
                (Some(time), None) => eprintln!("Log rotation configured for: every {time}, keeping {keep_files} files"),
                (None, Some(size)) => {
                    eprintln!("Log rotation configured for: when size exceeds {size}MB, keeping {keep_files} files")
                }
                _ => eprintln!("Log rotation configured for: daily, keeping {keep_files} files"),
            }
        } else {
            eprintln!("Failed to initialize flexi_logger: {:?}", flexi_logger_result.err());
        }

        OtelGuard {
            tracer_provider: None,
            meter_provider: None,
            logger_provider: None,
            _flexi_logger_handles: flexi_logger_handle,
        }
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
