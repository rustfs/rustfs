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
use prometheus::{Encoder, Registry, TextEncoder};
use std::io::IsTerminal;
use std::sync::Arc;
use tokio::sync::{Mutex, OnceCell};
use tracing::{info, warn};
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

/// Global registry for Prometheus metrics
static GLOBAL_REGISTRY: OnceCell<Arc<Mutex<Registry>>> = OnceCell::const_new();

/// Get the global registry instance
/// This function returns a reference to the global registry instance.
///
/// # Returns
/// A reference to the global registry instance
///
/// # Example
/// ```
/// use rustfs_obs::get_global_registry;
///
/// let registry = get_global_registry();
/// ```
pub fn get_global_registry() -> Arc<Mutex<Registry>> {
    GLOBAL_REGISTRY.get().unwrap().clone()
}

/// Prometheus metric endpoints
/// This function returns a string containing the Prometheus metrics.
/// The metrics are collected from the global registry.
/// The function is used to expose the metrics via an HTTP endpoint.
///
/// # Returns
/// A string containing the Prometheus metrics
///
/// # Example
/// ```
/// use rustfs_obs::metrics;
///
/// async fn main() {
///    let metrics = metrics().await;
///   println!("{}", metrics);
/// }
/// ```
pub async fn metrics() -> String {
    let encoder = TextEncoder::new();
    // Get a reference to the registry for reading metrics
    let registry = get_global_registry().lock().await.to_owned();
    let metric_families = registry.gather();
    if metric_families.is_empty() {
        warn!("No metrics available in Prometheus registry");
    } else {
        info!("Metrics collected: {} families", metric_families.len());
    }
    let mut buffer = Vec::new();
    encoder.encode(&metric_families, &mut buffer).unwrap();
    String::from_utf8(buffer).unwrap_or_else(|_| "Error encoding metrics".to_string())
}

/// create OpenTelemetry Resource
fn resource(config: &OtelConfig) -> Resource {
    let config = config.clone();
    Resource::builder()
        .with_service_name(config.service_name.unwrap_or(SERVICE_NAME.to_string()))
        .with_schema_url(
            [
                KeyValue::new(OTEL_SERVICE_VERSION, config.service_version.unwrap_or(SERVICE_VERSION.to_string())),
                KeyValue::new(DEPLOYMENT_ENVIRONMENT_NAME, config.environment.unwrap_or(ENVIRONMENT.to_string())),
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

/// Initialize Meter Provider
fn init_meter_provider(config: &OtelConfig) -> SdkMeterProvider {
    let mut builder = MeterProviderBuilder::default().with_resource(resource(config));
    // If endpoint is empty, use stdout output
    if config.endpoint.is_empty() {
        builder = builder.with_reader(create_periodic_reader(config.meter_interval.unwrap_or(METER_INTERVAL)));
    } else {
        // If endpoint is not empty, use otlp output
        let exporter = opentelemetry_otlp::MetricExporter::builder()
            .with_tonic()
            .with_endpoint(&config.endpoint)
            .with_temporality(opentelemetry_sdk::metrics::Temporality::default())
            .build()
            .unwrap();
        builder = builder.with_reader(
            PeriodicReader::builder(exporter)
                .with_interval(std::time::Duration::from_secs(config.meter_interval.unwrap_or(METER_INTERVAL)))
                .build(),
        );
        // If use_stdout is true, output to stdout at the same time
        if config.use_stdout.unwrap_or(USE_STDOUT) {
            builder = builder.with_reader(create_periodic_reader(config.meter_interval.unwrap_or(METER_INTERVAL)));
        }
    }
    let registry = Registry::new();
    // Set global registry
    GLOBAL_REGISTRY.set(Arc::new(Mutex::new(registry.clone()))).unwrap();
    // Create Prometheus exporter
    let prometheus_exporter = opentelemetry_prometheus::exporter().with_registry(registry).build().unwrap();
    // Build meter provider
    let meter_provider = builder.with_reader(prometheus_exporter).build();
    global::set_meter_provider(meter_provider.clone());
    meter_provider
}

/// Initialize Tracer Provider
fn init_tracer_provider(config: &OtelConfig) -> SdkTracerProvider {
    let sample_ratio = config.sample_ratio.unwrap_or(SAMPLE_RATIO);
    let sampler = if sample_ratio > 0.0 && sample_ratio < 1.0 {
        Sampler::TraceIdRatioBased(sample_ratio)
    } else {
        Sampler::AlwaysOn
    };
    let builder = SdkTracerProvider::builder()
        .with_sampler(sampler)
        .with_id_generator(RandomIdGenerator::default())
        .with_resource(resource(config));

    let tracer_provider = if config.endpoint.is_empty() {
        builder
            .with_batch_exporter(opentelemetry_stdout::SpanExporter::default())
            .build()
    } else {
        let exporter = opentelemetry_otlp::SpanExporter::builder()
            .with_tonic()
            .with_endpoint(&config.endpoint)
            .build()
            .unwrap();
        if config.use_stdout.unwrap_or(USE_STDOUT) {
            builder
                .with_batch_exporter(exporter)
                .with_batch_exporter(opentelemetry_stdout::SpanExporter::default())
        } else {
            builder.with_batch_exporter(exporter)
        }
        .build()
    };

    global::set_tracer_provider(tracer_provider.clone());
    tracer_provider
}

/// Initialize Telemetry
pub fn init_telemetry(config: &OtelConfig) -> OtelGuard {
    let tracer_provider = init_tracer_provider(config);
    let meter_provider = init_meter_provider(config);

    // Initialize logger provider based on configuration
    let logger_provider = {
        let mut builder = SdkLoggerProvider::builder().with_resource(resource(config));

        if config.endpoint.is_empty() {
            // Use stdout exporter when no endpoint is configured
            builder = builder.with_simple_exporter(opentelemetry_stdout::LogExporter::default());
        } else {
            // Configure OTLP exporter when endpoint is provided
            let exporter = opentelemetry_otlp::LogExporter::builder()
                .with_tonic()
                .with_endpoint(&config.endpoint)
                .build()
                .unwrap();

            builder = builder.with_batch_exporter(exporter);

            // Add stdout exporter if requested
            if config.use_stdout.unwrap_or(USE_STDOUT) {
                builder = builder.with_batch_exporter(opentelemetry_stdout::LogExporter::default());
            }
        }

        builder.build()
    };
    let config = config.clone();
    let logger_level = config.logger_level.unwrap_or(LOGGER_LEVEL.to_string());
    let logger_level = logger_level.as_str();
    // Setup OpenTelemetryTracingBridge layer
    let otel_layer = {
        // Filter to prevent infinite telemetry loops
        // This blocks events from OpenTelemetry and its dependent libraries (tonic, reqwest, etc.)
        // from being sent back to OpenTelemetry itself
        let filter_otel = match logger_level {
            "trace" | "debug" => {
                info!("OpenTelemetry tracing initialized with level: {}", logger_level);
                let mut filter = EnvFilter::new(logger_level);
                for directive in ["hyper", "tonic", "h2", "reqwest", "tower"] {
                    filter = filter.add_directive(format!("{}=off", directive).parse().unwrap());
                }
                filter
            }
            _ => {
                let mut filter = EnvFilter::new(logger_level);
                for directive in ["hyper", "tonic", "h2", "reqwest"] {
                    filter = filter.add_directive(format!("{}=off", directive).parse().unwrap());
                }
                filter
            }
        };

        layer::OpenTelemetryTracingBridge::new(&logger_provider).with_filter(filter_otel)
    };

    let tracer = tracer_provider.tracer(config.service_name.unwrap_or(SERVICE_NAME.to_string()));
    let registry = tracing_subscriber::registry()
        .with(switch_level(logger_level))
        .with(OpenTelemetryLayer::new(tracer))
        .with(MetricsLayer::new(meter_provider.clone()))
        .with(otel_layer);
    // Configure formatting layer
    let enable_color = std::io::stdout().is_terminal();
    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_ansi(enable_color)
        .with_thread_names(true)
        .with_file(true)
        .with_line_number(true);

    // Creating a formatting layer with explicit type to avoid type mismatches
    let fmt_layer = fmt_layer.with_filter(
        EnvFilter::new(logger_level).add_directive(
            format!("opentelemetry={}", if config.endpoint.is_empty() { logger_level } else { "off" })
                .parse()
                .unwrap(),
        ),
    );

    registry.with(ErrorLayer::default()).with(fmt_layer).init();
    if !config.endpoint.is_empty() {
        info!(
            "OpenTelemetry telemetry initialized with OTLP endpoint: {}, logger_level: {}",
            config.endpoint, logger_level
        );
    }

    OtelGuard {
        tracer_provider,
        meter_provider,
        logger_provider,
    }
}

/// Switch log level
fn switch_level(logger_level: &str) -> tracing_subscriber::filter::LevelFilter {
    match logger_level {
        "error" => tracing_subscriber::filter::LevelFilter::ERROR,
        "warn" => tracing_subscriber::filter::LevelFilter::WARN,
        "info" => tracing_subscriber::filter::LevelFilter::INFO,
        "debug" => tracing_subscriber::filter::LevelFilter::DEBUG,
        "trace" => tracing_subscriber::filter::LevelFilter::TRACE,
        _ => tracing_subscriber::filter::LevelFilter::OFF,
    }
}
