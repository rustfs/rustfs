use opentelemetry::global;
use opentelemetry::trace::TraceContextExt;
use rustfs_obs::{init_logging, load_config, LogEntry};
use std::time::{Duration, SystemTime};
use tracing::{info, instrument, Span};
use tracing_core::Level;
use tracing_opentelemetry::OpenTelemetrySpanExt;

#[tokio::main]
async fn main() {
    let start_time = SystemTime::now();
    let config = load_config(Some("packages/obs/examples/config".to_string()));
    info!("Configuration file loading is complete {:?}", config.clone());
    let (logger, _guard) = init_logging(config);
    info!("Log module initialization is completed");
    // Simulate the operation
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Record Metrics
    let meter = global::meter("rustfs.rs");
    let request_duration = meter.f64_histogram("s3_request_duration_seconds").build();
    request_duration.record(
        start_time.elapsed().unwrap().as_secs_f64(),
        &[opentelemetry::KeyValue::new("operation", "put_object")],
    );

    // Gets the current span
    let span = Span::current();
    // Use 'OpenTelemetrySpanExt' to get 'SpanContext'
    let span_context = span.context(); // Get context via OpenTelemetrySpanExt
    let span_id = span_context.span().span_context().span_id().to_string(); // Get the SpanId
    let trace_id = span_context.span().span_context().trace_id().to_string(); // Get the TraceId
    let result = logger
        .log(LogEntry::new(
            Level::INFO,
            "Process user requests".to_string(),
            "api_handler".to_string(),
            Some("req-12345".to_string()),
            Some("user-6789".to_string()),
            vec![
                ("endpoint".to_string(), "/api/v1/data".to_string()),
                ("method".to_string(), "GET".to_string()),
                ("span_id".to_string(), span_id),
                ("trace_id".to_string(), trace_id),
            ],
        ))
        .await;
    info!("Logging is completed {:?}", result);
    put_object("bucket".to_string(), "object".to_string(), "user".to_string()).await;
    info!("Logging is completed");
    tokio::time::sleep(Duration::from_secs(2)).await;
    info!("Program ends");
}

#[instrument(fields(bucket, object, user))]
async fn put_object(bucket: String, object: String, user: String) {
    let start_time = SystemTime::now();
    info!("Starting PUT operation");
    // Gets the current span
    let span = Span::current();
    // Use 'OpenTelemetrySpanExt' to get 'SpanContext'
    let span_context = span.context(); // Get context via OpenTelemetrySpanExt
    let span_id = span_context.span().span_context().span_id().to_string(); // Get the SpanId
    let trace_id = span_context.span().span_context().trace_id().to_string(); // Get the TraceId
    info!(
        "Starting PUT operation content: bucket = {}, object = {}, user = {},span_id = {},trace_id = {},start_time = {}",
        bucket,
        object,
        user,
        span_id,
        trace_id,
        start_time.elapsed().unwrap().as_secs_f64()
    );
    // Simulate the operation
    tokio::time::sleep(Duration::from_millis(100)).await;

    info!("PUT operation completed");
}
