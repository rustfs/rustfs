use opentelemetry::global;
use rustfs_obs::{BaseLogEntry, ServerLogEntry, SystemObserver, get_logger, init_obs, log_info};
use std::collections::HashMap;
use std::time::{Duration, SystemTime};
use tracing::{error, info, instrument};
use tracing_core::Level;

#[tokio::main]
async fn main() {
    let obs_conf = Some("crates/obs/examples/config.toml".to_string());
    let (_logger, _guard) = init_obs(obs_conf).await;
    let span = tracing::span!(Level::INFO, "main");
    let _enter = span.enter();
    info!("Program starts");
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
    let meter = global::meter("rustfs");
    let request_duration = meter.f64_histogram("s3_request_duration_seconds").build();
    request_duration.record(
        start_time.elapsed().unwrap().as_secs_f64(),
        &[opentelemetry::KeyValue::new("operation", "run")],
    );

    match SystemObserver::init_process_observer(meter).await {
        Ok(_) => info!("Process observer initialized successfully"),
        Err(e) => error!("Failed to initialize process observer: {:?}", e),
    }

    let base_entry = BaseLogEntry::new()
        .message(Some("run logger api_handler info".to_string()))
        .request_id(Some("request_id".to_string()))
        .timestamp(chrono::DateTime::from(start_time))
        .tags(Some(HashMap::default()));

    let server_entry = ServerLogEntry::new(Level::INFO, "api_handler".to_string())
        .with_base(base_entry)
        .user_id(Some(user.clone()))
        .add_field("operation".to_string(), "login".to_string())
        .add_field("bucket".to_string(), bucket.clone())
        .add_field("object".to_string(), object.clone());

    let result = get_logger().lock().await.log_server_entry(server_entry).await;
    info!("Logging is completed {:?}", result);
    put_object("bucket".to_string(), "object".to_string(), "user".to_string()).await;
    info!("Logging is completed");
    tokio::time::sleep(Duration::from_secs(2)).await;
    info!("Program run ends");
}

#[instrument(fields(bucket, object, user))]
async fn put_object(bucket: String, object: String, user: String) {
    let start_time = SystemTime::now();
    info!("Starting put_object operation time: {:?}", start_time);

    let meter = global::meter("rustfs");
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
