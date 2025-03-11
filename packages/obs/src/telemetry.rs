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
    attribute::{DEPLOYMENT_ENVIRONMENT_NAME, NETWORK_LOCAL_ADDRESS, SERVICE_NAME, SERVICE_VERSION},
    SCHEMA_URL,
};
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
        .with_service_name(config.service_name.clone())
        .with_schema_url(
            [
                KeyValue::new(SERVICE_NAME, config.service_name.clone()),
                KeyValue::new(SERVICE_VERSION, config.service_version.clone()),
                KeyValue::new(DEPLOYMENT_ENVIRONMENT_NAME, config.deployment_environment.clone()),
                KeyValue::new(NETWORK_LOCAL_ADDRESS, get_local_ip_with_default()),
            ],
            SCHEMA_URL,
        )
        .build()
}

/// Initialize Meter Provider
fn init_meter_provider(config: &OtelConfig) -> SdkMeterProvider {
    let mut builder = MeterProviderBuilder::default().with_resource(resource(config));
    // If endpoint is empty, use stdout output
    if config.endpoint.is_empty() {
        builder = builder.with_reader(
            PeriodicReader::builder(opentelemetry_stdout::MetricExporter::default())
                .with_interval(std::time::Duration::from_secs(config.meter_interval))
                .build(),
        );
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
                .with_interval(std::time::Duration::from_secs(config.meter_interval))
                .build(),
        );
        // If use_stdout is true, output to stdout at the same time
        if config.use_stdout {
            builder = builder.with_reader(
                PeriodicReader::builder(opentelemetry_stdout::MetricExporter::default())
                    .with_interval(std::time::Duration::from_secs(config.meter_interval))
                    .build(),
            );
        }
    }

    let meter_provider = builder.build();
    global::set_meter_provider(meter_provider.clone());
    meter_provider
}

/// Initialize Tracer Provider
fn init_tracer_provider(config: &OtelConfig) -> SdkTracerProvider {
    let sampler = if config.sample_ratio > 0.0 && config.sample_ratio < 1.0 {
        Sampler::TraceIdRatioBased(config.sample_ratio)
    } else {
        Sampler::AlwaysOn
    };
    let builder = SdkTracerProvider::builder()
        .with_sampler(sampler)
        .with_id_generator(RandomIdGenerator::default())
        .with_resource(resource(config));

    let tracer_provider = if config.endpoint.is_empty() {
        builder
            .with_simple_exporter(opentelemetry_stdout::SpanExporter::default())
            .build()
    } else {
        let exporter = opentelemetry_otlp::SpanExporter::builder()
            .with_tonic()
            .with_endpoint(&config.endpoint)
            .build()
            .unwrap();
        if config.use_stdout {
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
    let tracer = tracer_provider.tracer(config.service_name.clone());

    let logger_provider = if config.endpoint.is_empty() {
        SdkLoggerProvider::builder()
            .with_resource(resource(config))
            .with_simple_exporter(opentelemetry_stdout::LogExporter::default())
            .build()
    } else {
        let exporter = opentelemetry_otlp::LogExporter::builder()
            .with_tonic()
            .with_endpoint(&config.endpoint)
            .build()
            .unwrap();
        SdkLoggerProvider::builder()
            .with_resource(resource(config))
            .with_batch_exporter(exporter)
            .with_batch_exporter(opentelemetry_stdout::LogExporter::default())
            .build()
    };

    let otel_layer = layer::OpenTelemetryTracingBridge::new(&logger_provider);
    // For the OpenTelemetry layer, add a tracing filter to filter events from
    // OpenTelemetry and its dependent crates (opentelemetry-otlp uses crates
    // like reqwest/tonic etc.) from being sent back to OTel itself, thus
    // preventing infinite telemetry generation. The filter levels are set as
    // follows:
    // - Allow `info` level and above by default.
    // - Restrict `opentelemetry`, `hyper`, `tonic`, and `reqwest` completely.
    // Note: This will also drop events from crates like `tonic` etc. even when
    // they are used outside the OTLP Exporter. For more details, see:
    // https://github.com/open-telemetry/opentelemetry-rust/issues/761
    let filter_otel = EnvFilter::new("info")
        .add_directive("hyper=off".parse().unwrap())
        .add_directive("opentelemetry=off".parse().unwrap())
        .add_directive("tonic=off".parse().unwrap())
        .add_directive("h2=off".parse().unwrap())
        .add_directive("reqwest=off".parse().unwrap());
    let otel_layer = otel_layer.with_filter(filter_otel);
    let registry = tracing_subscriber::registry()
        .with(tracing_subscriber::filter::LevelFilter::INFO)
        .with(OpenTelemetryLayer::new(tracer))
        .with(MetricsLayer::new(meter_provider.clone()))
        .with(otel_layer);
    if config.endpoint.is_empty() {
        // Create a new tracing::Fmt layer to print the logs to stdout. It has a
        // default filter of `info` level and above, and `debug` and above for logs
        // from OpenTelemetry crates. The filter levels can be customized as needed.
        let filter_fmt = EnvFilter::new("info").add_directive("opentelemetry=debug".parse().unwrap());
        let fmt_layer = tracing_subscriber::fmt::layer()
            .with_thread_names(true)
            .with_filter(filter_fmt);

        registry
            .with(tracing_subscriber::fmt::layer().with_ansi(true))
            .with(fmt_layer)
            .init();
    } else {
        registry.with(tracing_subscriber::fmt::layer().with_ansi(false)).init();
        println!("Logs and meter,tracer enabled");
    }

    OtelGuard {
        tracer_provider,
        meter_provider,
        logger_provider,
    }
}
