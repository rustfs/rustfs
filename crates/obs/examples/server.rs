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

use opentelemetry::global;
use rustfs_obs::{SystemObserver, init_obs};
use std::time::{Duration, SystemTime};
use tracing::{Level, error, info, instrument};

#[tokio::main]
async fn main() {
    let obs_conf = Some("http://localhost:4317".to_string());
    let _guard = init_obs(obs_conf).await;
    let span = tracing::span!(Level::INFO, "main");
    let _enter = span.enter();
    info!("Program starts");
    // Simulate the operation
    tokio::time::sleep(Duration::from_millis(100)).await;
    run("service-demo".to_string()).await;
    info!("Program ends");
}

#[instrument(fields(bucket, object, user))]
async fn run(service_name: String) {
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

    // Simulate the operation
    tokio::time::sleep(Duration::from_millis(100)).await;

    info!("PUT operation completed");
}
