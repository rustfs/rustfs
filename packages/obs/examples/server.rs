use opentelemetry::global;
use rustfs_obs::{get_logger, init_obs, load_config, log_info, LogEntry};
use std::time::{Duration, SystemTime};
use tracing::{info, instrument};
use tracing_core::Level;

#[tokio::main]
async fn main() {
    let config = load_config(Some("packages/obs/examples/config".to_string()));
    let (_logger, _guard) = init_obs(config.clone()).await;
    // Simulate the operation
    tokio::time::sleep(Duration::from_millis(100)).await;
    run(
        "service-demo".to_string(),
        "object-demo".to_string(),
        "user-demo".to_string(),
        "service-demo".to_string(),
    )
    .await;
    info!("Program ends");
}

#[instrument(fields(bucket, object, user))]
async fn run(bucket: String, object: String, user: String, service_name: String) {
    let start_time = SystemTime::now();
    info!("Log module initialization is completed service_name: {:?}", service_name);

    // Record Metrics
    let meter = global::meter("rustfs.rs");
    let request_duration = meter.f64_histogram("s3_request_duration_seconds").build();
    request_duration.record(
        start_time.elapsed().unwrap().as_secs_f64(),
        &[opentelemetry::KeyValue::new("operation", "run")],
    );

    let result = get_logger()
        .lock()
        .await
        .log(LogEntry::new(
            Level::INFO,
            "Process user requests".to_string(),
            "api_handler".to_string(),
            Some("demo-audit".to_string()),
            Some("req-12345".to_string()),
            Some(user),
            vec![
                ("endpoint".to_string(), "/api/v1/data".to_string()),
                ("method".to_string(), "GET".to_string()),
                ("bucket".to_string(), bucket),
                ("object-length".to_string(), object.len().to_string()),
            ],
        ))
        .await;
    info!("Logging is completed {:?}", result);
    put_object("bucket".to_string(), "object".to_string(), "user".to_string()).await;
    info!("Logging is completed");
    tokio::time::sleep(Duration::from_secs(2)).await;
    info!("Program run ends");
}

#[instrument(fields(bucket, object, user))]
async fn put_object(bucket: String, object: String, user: String) {
    let start_time = SystemTime::now();
    info!("Starting put_object operation");

    let meter = global::meter("rustfs.rs");
    let request_duration = meter.f64_histogram("s3_request_duration_seconds").build();
    request_duration.record(
        start_time.elapsed().unwrap().as_secs_f64(),
        &[opentelemetry::KeyValue::new("operation", "put_object")],
    );

    info!(
        "Starting PUT operation content: bucket = {}, object = {}, user = {},start_time = {}",
        bucket,
        object,
        user,
        start_time.elapsed().unwrap().as_secs_f64()
    );

    let result = log_info("put_object logger info", "put_object").await;
    info!("put_object is completed {:?}", result);
    // Simulate the operation
    tokio::time::sleep(Duration::from_millis(100)).await;

    info!("PUT operation completed");
}
