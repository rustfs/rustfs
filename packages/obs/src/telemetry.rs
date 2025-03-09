use opentelemetry::trace::TracerProvider;
use opentelemetry::{global, KeyValue};
use opentelemetry_appender_tracing::layer;
use opentelemetry_otlp::{self, WithExportConfig};
use opentelemetry_sdk::logs::SdkLoggerProvider;
use opentelemetry_sdk::{
    metrics::{MeterProviderBuilder, PeriodicReader, SdkMeterProvider},
    trace::{RandomIdGenerator, Sampler, SdkTracerProvider},
    Resource,
};
use opentelemetry_semantic_conventions::attribute::NETWORK_LOCAL_ADDRESS;
use opentelemetry_semantic_conventions::{
    attribute::{DEPLOYMENT_ENVIRONMENT_NAME, SERVICE_NAME, SERVICE_VERSION},
    SCHEMA_URL,
};
use std::time::Duration;
use tracing::{info, Level};
use tracing_opentelemetry::{MetricsLayer, OpenTelemetryLayer};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter, Layer};

/// Telemetry is a wrapper around the OpenTelemetry SDK Tracer and Meter providers.
/// It initializes the global Tracer and Meter providers, and sets up the tracing subscriber.
/// The Tracer and Meter providers are shut down when the Telemetry instance is dropped.
/// This is a convenience struct to ensure that the global providers are properly initialized and shut down.
///
/// # Example
/// ```
/// use rustfs_obs::Telemetry;
///
/// let _telemetry = Telemetry::init();
/// ```
pub struct Telemetry {
    tracer_provider: SdkTracerProvider,
    meter_provider: SdkMeterProvider,
}

impl Telemetry {
    pub fn init() -> Self {
        // Define service resource information
        let resource = Resource::builder()
            .with_service_name("rustfs-service")
            .with_schema_url(
                [
                    KeyValue::new(SERVICE_NAME, "rustfs-service"),
                    KeyValue::new(SERVICE_VERSION, "0.1.0"),
                    KeyValue::new(DEPLOYMENT_ENVIRONMENT_NAME, "develop"),
                    KeyValue::new(NETWORK_LOCAL_ADDRESS, "127.0.0.1"),
                ],
                SCHEMA_URL,
            )
            .build();

        let tracer_exporter = opentelemetry_otlp::SpanExporter::builder()
            .with_tonic()
            .with_endpoint("http://localhost:4317")
            .with_protocol(opentelemetry_otlp::Protocol::Grpc)
            .with_timeout(Duration::from_secs(3))
            .build()
            .unwrap();

        // Configure Tracer Provider
        let tracer_provider = SdkTracerProvider::builder()
            .with_sampler(Sampler::AlwaysOn)
            .with_id_generator(RandomIdGenerator::default())
            .with_resource(resource.clone())
            .with_batch_exporter(tracer_exporter)
            // .with_simple_exporter(opentelemetry_stdout::SpanExporter::default())
            .build();

        let meter_exporter = opentelemetry_otlp::MetricExporter::builder()
            .with_tonic()
            .with_endpoint("http://localhost:4317")
            .with_protocol(opentelemetry_otlp::Protocol::Grpc)
            .with_timeout(Duration::from_secs(3))
            .with_temporality(opentelemetry_sdk::metrics::Temporality::default())
            .build()
            .unwrap();

        let meter_reader = PeriodicReader::builder(meter_exporter)
            .with_interval(Duration::from_secs(30))
            .build();

        // For debugging in development
        // let meter_stdout_reader = PeriodicReader::builder(opentelemetry_stdout::MetricExporter::default()).build();

        // Configure Meter Provider
        let meter_provider = MeterProviderBuilder::default()
            .with_resource(resource.clone())
            .with_reader(meter_reader)
            // .with_reader(meter_stdout_reader)
            .build();

        // Set global Tracer and Meter providers
        global::set_tracer_provider(tracer_provider.clone());
        global::set_meter_provider(meter_provider.clone());

        let tracer = tracer_provider.tracer("rustfs-service");

        // // let _stdout_exporter = opentelemetry_stdout::LogExporter::default();
        // let otlp_exporter = opentelemetry_otlp::LogExporter::builder()
        //     .with_tonic()
        //     .with_endpoint("http://localhost:4317")
        //     // .with_timeout(Duration::from_secs(3))
        //     // .with_protocol(opentelemetry_otlp::Protocol::Grpc)
        //     .build()
        //     .unwrap();
        // let provider: SdkLoggerProvider = SdkLoggerProvider::builder()
        //     .with_resource(resource.clone())
        //     .with_simple_exporter(otlp_exporter)
        //     .build();
        // let filter_otel = EnvFilter::new("debug")
        //     // .add_directive("hyper=off".parse().unwrap())
        //     // .add_directive("opentelemetry=off".parse().unwrap())
        //     // .add_directive("tonic=off".parse().unwrap())
        //     // .add_directive("h2=off".parse().unwrap())
        //     .add_directive("reqwest=off".parse().unwrap());
        // let otel_layer = layer::OpenTelemetryTracingBridge::new(&provider).with_filter(filter_otel);
        let filter_fmt = EnvFilter::new("info").add_directive("opentelemetry=debug".parse().unwrap());
        let fmt_layer = tracing_subscriber::fmt::layer()
            .with_thread_names(true)
            .with_filter(filter_fmt);

        // Configure `tracing subscriber`
        tracing_subscriber::registry()
            .with(tracing_subscriber::filter::LevelFilter::from_level(Level::DEBUG))
            .with(tracing_subscriber::fmt::layer().with_ansi(true))
            .with(MetricsLayer::new(meter_provider.clone()))
            .with(OpenTelemetryLayer::new(tracer))
            // .with(otel_layer)
            .with(fmt_layer)
            .init();
        info!(name: "my-event-name", target: "my-system", event_id = 20, user_name = "otel", user_email = "otel@opentelemetry.io", message = "This is an example message");
        Self {
            tracer_provider,
            meter_provider,
        }
    }
}

impl Drop for Telemetry {
    fn drop(&mut self) {
        if let Err(err) = self.tracer_provider.shutdown() {
            eprintln!("{err:?}");
        }
        if let Err(err) = self.meter_provider.shutdown() {
            eprintln!("{err:?}");
        }
    }
}
