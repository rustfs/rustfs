use crate::global::{ENVIRONMENT, LOGGER_LEVEL, METER_INTERVAL, SAMPLE_RATIO, SERVICE_NAME, SERVICE_VERSION, USE_STDOUT};
use crate::{get_local_ip_with_default, OtelConfig};
use opentelemetry::trace::TracerProvider;
use opentelemetry::{global, KeyValue};
use opentelemetry_appender_tracing::layer;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::logs::SdkLoggerProvider;
use opentelemetry_sdk::{
    metrics::{MeterProviderBuilder, PeriodicReader, SdkMeterProvider},
    trace::{RandomIdGenerator, Sampler, SdkTracerProvider},
    Resource,
};
use opentelemetry_semantic_conventions::{
    attribute::{DEPLOYMENT_ENVIRONMENT_NAME, NETWORK_LOCAL_ADDRESS, SERVICE_VERSION as OTEL_SERVICE_VERSION},
    SCHEMA_URL,
};
use smallvec::SmallVec;
use std::borrow::Cow;
use std::io::IsTerminal;
use tracing::info;
use tracing_error::ErrorLayer;
use tracing_opentelemetry::{MetricsLayer, OpenTelemetryLayer};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter, Layer};

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
/// # Example
///
/// ```
/// use rustfs_obs::{init_telemetry, OtelConfig};
///
/// let config = OtelConfig::default();
/// let otel_guard = init_telemetry(&config);
///
/// // The guard is kept alive for the duration of the application
/// // When it's dropped, all telemetry components are properly shut down
/// drop(otel_guard);
/// ```
#[derive(Debug)]
pub struct OtelGuard {
    tracer_provider: SdkTracerProvider,
    meter_provider: SdkMeterProvider,
    logger_provider: SdkLoggerProvider,
}

impl Drop for OtelGuard {
    fn drop(&mut self) {
        if let Err(err) = self.tracer_provider.shutdown() {
            eprintln!("Tracer shutdown error: {:?}", err);
        }
        if let Err(err) = self.meter_provider.shutdown() {
            eprintln!("Meter shutdown error: {:?}", err);
        }
        if let Err(err) = self.logger_provider.shutdown() {
            eprintln!("Logger shutdown error: {:?}", err);
        }
    }
}

/// create OpenTelemetry Resource
fn resource(config: &OtelConfig) -> Resource {
    Resource::builder()
        .with_service_name(Cow::Borrowed(config.service_name.as_deref().unwrap_or(SERVICE_NAME)).to_string())
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
pub fn init_telemetry(config: &OtelConfig) -> OtelGuard {
    // avoid repeated access to configuration fields
    let endpoint = &config.endpoint;
    let use_stdout = config.use_stdout.unwrap_or(USE_STDOUT);
    let meter_interval = config.meter_interval.unwrap_or(METER_INTERVAL);
    let logger_level = config.logger_level.as_deref().unwrap_or(LOGGER_LEVEL);
    let service_name = config.service_name.as_deref().unwrap_or(SERVICE_NAME);

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
        // optimize filter configuration
        let otel_layer = {
            let filter_otel = match logger_level {
                "trace" | "debug" => {
                    info!("OpenTelemetry tracing initialized with level: {}", logger_level);
                    EnvFilter::new(logger_level)
                }
                _ => {
                    let mut filter = EnvFilter::new(logger_level);

                    // use smallvec to avoid heap allocation
                    let directives: SmallVec<[&str; 5]> = smallvec::smallvec!["hyper", "tonic", "h2", "reqwest", "tower"];

                    for directive in directives {
                        filter = filter.add_directive(format!("{}=off", directive).parse().unwrap());
                    }
                    filter
                }
            };
            layer::OpenTelemetryTracingBridge::new(&logger_provider).with_filter(filter_otel)
        };

        let tracer = tracer_provider.tracer(Cow::Borrowed(service_name).to_string());

        // Configure registry to avoid repeated calls to filter methods
        let level_filter = switch_level(logger_level);
        let registry = tracing_subscriber::registry()
            .with(level_filter)
            .with(OpenTelemetryLayer::new(tracer))
            .with(MetricsLayer::new(meter_provider.clone()))
            .with(otel_layer)
            .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(logger_level)));

        // configure the formatting layer
        let enable_color = std::io::stdout().is_terminal();
        let fmt_layer = tracing_subscriber::fmt::layer()
            .with_ansi(enable_color)
            .with_thread_names(true)
            .with_file(true)
            .with_line_number(true)
            .with_filter(
                EnvFilter::new(logger_level).add_directive(
                    format!("opentelemetry={}", if endpoint.is_empty() { logger_level } else { "off" })
                        .parse()
                        .unwrap(),
                ),
            );

        registry.with(ErrorLayer::default()).with(fmt_layer).init();

        if !endpoint.is_empty() {
            info!(
                "OpenTelemetry telemetry initialized with OTLP endpoint: {}, logger_level: {}",
                endpoint, logger_level
            );
        }
    }

    OtelGuard {
        tracer_provider,
        meter_provider,
        logger_provider,
    }
}

/// Switch log level
fn switch_level(logger_level: &str) -> tracing_subscriber::filter::LevelFilter {
    use tracing_subscriber::filter::LevelFilter;
    match logger_level {
        "error" => LevelFilter::ERROR,
        "warn" => LevelFilter::WARN,
        "info" => LevelFilter::INFO,
        "debug" => LevelFilter::DEBUG,
        "trace" => LevelFilter::TRACE,
        _ => LevelFilter::OFF,
    }
}
