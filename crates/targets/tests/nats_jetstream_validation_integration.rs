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

//! JetStream stream validation integration tests for the NATS target.
//!
//! These tests are ignored because they require a running NATS server with JetStream enabled. To
//! run locally:
//!
//!     docker run -d --name rustfs-nats-test -p 4222:4222 nats:2 -js
//!     cargo test -p rustfs-targets --test nats_jetstream_validation_integration -- --ignored
//!
//! Override the server URL with RUSTFS_TEST_NATS_URL. Each test provisions and removes its own
//! stream. RustFS never provisions the stream, the test harness does, mirroring the operator role.

use async_nats::jetstream::stream::Config;
use rustfs_targets::check_nats_server_available;
use rustfs_targets::target::TargetType;
use rustfs_targets::target::nats::NATSArgs;
use std::time::Duration;
use uuid::Uuid;

fn server_url() -> String {
    std::env::var("RUSTFS_TEST_NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:4222".to_string())
}

fn jetstream_args(subject: &str, stream_name: &str) -> NATSArgs {
    NATSArgs {
        enable: true,
        address: server_url(),
        subject: subject.to_string(),
        username: String::new(),
        password: String::new(),
        token: String::new(),
        credentials_file: String::new(),
        tls_ca: String::new(),
        tls_client_cert: String::new(),
        tls_client_key: String::new(),
        tls_required: false,
        queue_dir: "/tmp/rustfs-nats-jetstream-test".to_string(),
        queue_limit: 0,
        jetstream_enable: Some(true),
        jetstream_stream_name: Some(stream_name.to_string()),
        jetstream_ack_timeout_secs: Some(30),
        target_type: TargetType::NotifyEvent,
    }
}

/// A pre-provisioned writable stream: it captures the subject, returns acks, accepts writes, and its
/// duplicate window exceeds the retry lifetime. The test harness creates it, never RustFS.
async fn provision_writable_stream(stream_name: &str, subject: &str) {
    let client = async_nats::connect(server_url()).await.expect("connect to NATS");
    let context = async_nats::jetstream::new(client);
    // Clear any leftover from a prior aborted run. Absent is fine, so the result is dropped.
    let _ = context.delete_stream(stream_name).await;
    context
        .create_stream(Config {
            name: stream_name.to_string(),
            subjects: vec![subject.to_string()],
            duplicate_window: Duration::from_secs(300),
            ..Default::default()
        })
        .await
        .expect("provision the stream");
}

async fn remove_stream(stream_name: &str) {
    let client = async_nats::connect(server_url()).await.expect("connect to NATS");
    let context = async_nats::jetstream::new(client);
    // Best-effort teardown. A failure to delete does not fail the test, so the result is dropped.
    let _ = context.delete_stream(stream_name).await;
}

#[tokio::test]
#[ignore]
async fn missing_stream_fails_the_health_check() {
    let stream_name = format!("RUSTFS_TEST_{}", Uuid::new_v4().simple());
    let args = jetstream_args("rustfs.events", &stream_name);
    // No stream is provisioned, so the lookup must fail.
    let err = check_nats_server_available(&args)
        .await
        .expect_err("a missing stream fails the health check");
    assert!(!err.to_string().is_empty(), "the failure carries a generic message");
}

#[tokio::test]
#[ignore]
async fn valid_stream_passes_the_health_check() {
    let stream_name = format!("RUSTFS_TEST_{}", Uuid::new_v4().simple());
    let subject = "rustfs.events";
    provision_writable_stream(&stream_name, subject).await;
    let args = jetstream_args(subject, &stream_name);
    let result = check_nats_server_available(&args).await;
    remove_stream(&stream_name).await;
    result.expect("a pre-provisioned writable stream passes the health check");
}

#[tokio::test]
#[ignore]
async fn stream_not_capturing_the_subject_fails_the_health_check() {
    let stream_name = format!("RUSTFS_TEST_{}", Uuid::new_v4().simple());
    // The stream binds a different subject than the target publishes to.
    provision_writable_stream(&stream_name, "some.other.subject").await;
    let args = jetstream_args("rustfs.events", &stream_name);
    let result = check_nats_server_available(&args).await;
    remove_stream(&stream_name).await;
    result.expect_err("a stream that does not capture the subject fails the health check");
}

#[tokio::test]
#[ignore]
async fn too_small_duplicate_window_fails_the_health_check() {
    let stream_name = format!("RUSTFS_TEST_{}", Uuid::new_v4().simple());
    let subject = "rustfs.events";
    let client = async_nats::connect(server_url()).await.expect("connect to NATS");
    let context = async_nats::jetstream::new(client);
    // Clear any leftover from a prior aborted run. Absent is fine, so the result is dropped.
    let _ = context.delete_stream(&stream_name).await;
    context
        .create_stream(Config {
            name: stream_name.clone(),
            subjects: vec![subject.to_string()],
            // Below the worst-case retry lifetime at the default ack timeout.
            duplicate_window: Duration::from_secs(60),
            ..Default::default()
        })
        .await
        .expect("provision the stream");
    let args = jetstream_args(subject, &stream_name);
    let result = check_nats_server_available(&args).await;
    remove_stream(&stream_name).await;
    result.expect_err("a too-small duplicate window fails the health check");
}
