use opentelemetry::trace::TracerProvider;
use opentelemetry::{global, KeyValue};
use opentelemetry_otlp::{self, WithExportConfig};
use opentelemetry_sdk::{
    metrics::{MeterProviderBuilder, PeriodicReader, SdkMeterProvider},
    trace::{RandomIdGenerator, Sampler, SdkTracerProvider},
    Resource,
};
use opentelemetry_semantic_conventions::{
    attribute::{DEPLOYMENT_ENVIRONMENT_NAME, SERVICE_NAME, SERVICE_VERSION},
    SCHEMA_URL,
};
use tracing::Level;
use tracing_opentelemetry::{MetricsLayer, OpenTelemetryLayer};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

/// Telemetry is a wrapper around the OpenTelemetry SDK Tracer and Meter providers.
/// It initializes the global Tracer and Meter providers, and sets up the tracing subscriber.
/// The Tracer and Meter providers are shut down when the Telemetry instance is dropped.
/// This is a convenience struct to ensure that the global providers are properly initialized and shut down.
///
/// # Example
/// ```
/// use rustfs_logging::Telemetry;
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
            .with_schema_url(
                [
                    KeyValue::new(SERVICE_NAME, env!("CARGO_PKG_NAME")),
                    KeyValue::new(SERVICE_VERSION, env!("CARGO_PKG_VERSION")),
                    KeyValue::new(DEPLOYMENT_ENVIRONMENT_NAME, "develop"),
                ],
                SCHEMA_URL,
            )
            .build();

        let tracer_exporter = opentelemetry_otlp::SpanExporter::builder()
            .with_tonic()
            .with_endpoint("http://localhost:4317")
            .build()
            .unwrap();

        // Configure Tracer Provider
        let tracer_provider = SdkTracerProvider::builder()
            .with_sampler(Sampler::AlwaysOn)
            .with_id_generator(RandomIdGenerator::default())
            .with_resource(resource.clone())
            .with_batch_exporter(tracer_exporter)
            .with_simple_exporter(opentelemetry_stdout::SpanExporter::default())
            .build();

        let meter_exporter = opentelemetry_otlp::MetricExporter::builder()
            .with_tonic()
            .with_endpoint("http://localhost:4317")
            .with_temporality(opentelemetry_sdk::metrics::Temporality::default())
            .build()
            .unwrap();

        let meter_reader = PeriodicReader::builder(meter_exporter)
            .with_interval(std::time::Duration::from_secs(30))
            .build();

        // For debugging in development
        let meter_stdout_reader = PeriodicReader::builder(opentelemetry_stdout::MetricExporter::default()).build();

        // Configure Meter Provider
        let meter_provider = MeterProviderBuilder::default()
            .with_resource(resource)
            .with_reader(meter_reader)
            .with_reader(meter_stdout_reader)
            .build();

        // Set global Tracer and Meter providers
        global::set_tracer_provider(tracer_provider.clone());
        global::set_meter_provider(meter_provider.clone());

        let tracer = tracer_provider.tracer("tracing-otel-subscriber-rustfs-service");

        // Configure `tracing subscriber`
        tracing_subscriber::registry()
            .with(tracing_subscriber::filter::LevelFilter::from_level(Level::DEBUG))
            .with(tracing_subscriber::fmt::layer().with_ansi(true))
            .with(MetricsLayer::new(meter_provider.clone()))
            .with(OpenTelemetryLayer::new(tracer))
            .init();

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
