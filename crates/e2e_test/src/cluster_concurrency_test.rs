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

use crate::common::RustFSTestClusterEnvironment;
use aws_sdk_s3::Client;
use aws_sdk_s3::error::SdkError;
use bytes::Bytes;
use serial_test::serial;
use std::sync::Arc;
use tokio::sync::Barrier;
use tracing::{info, warn};

const BUCKET: &str = "conditional-put-race-bucket";

async fn cleanup_object(client: &Client, key: &str) {
    if let Err(e) = client.delete_object().bucket(BUCKET).key(key).send().await {
        warn!("Failed to delete object '{}' from bucket '{}' during cleanup: {:?}", key, BUCKET, e);
    }
}

async fn conditional_put(
    client: &Client,
    key: &str,
    data: &[u8],
    client_id: usize,
) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
    let result = client
        .put_object()
        .bucket(BUCKET)
        .key(key)
        .body(Bytes::copy_from_slice(data).into())
        .if_none_match("*")
        .send()
        .await;

    match result {
        Ok(resp) => {
            info!("  Client {} SUCCEEDED - ETag: {}", client_id, resp.e_tag().unwrap_or("none"));
            Ok(true)
        }
        Err(SdkError::ServiceError(e)) => {
            let code = e.err().meta().code().unwrap_or("");
            let message = e.err().meta().message().unwrap_or("");
            if code == "PreconditionFailed" {
                warn!("  Client {} got 412 PreconditionFailed", client_id);
                Ok(false)
            } else {
                warn!("  Client {} got ServiceError: code={}, message={}", client_id, code, message);
                Err(e.into_err().into())
            }
        }
        Err(e) => {
            warn!("  Client {} got non-ServiceError: {:?}", client_id, e);
            Err(e.into())
        }
    }
}

async fn run_race_iteration(
    clients: &[Client],
    test_key: &str,
    iteration: usize,
) -> Result<usize, Box<dyn std::error::Error + Send + Sync>> {
    cleanup_object(&clients[0], test_key).await;
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let head_result = clients[0].head_object().bucket(BUCKET).key(test_key).send().await;

    if head_result.is_ok() {
        warn!("Warning: Object still exists after cleanup, skipping iteration {}", iteration);
        return Ok(0);
    }

    info!("\n=== Iteration {} ===", iteration);
    info!("Launching {} concurrent conditional PUTs to different nodes...", clients.len());

    let barrier = Arc::new(Barrier::new(clients.len()));
    let test_key = test_key.to_string();

    let mut handles = vec![];
    for (i, client) in clients.iter().enumerate() {
        let client = client.clone();
        let barrier = barrier.clone();
        let key = test_key.clone();
        let data = format!("data from client {}", i).into_bytes();

        let handle = tokio::spawn(async move {
            barrier.wait().await;
            conditional_put(&client, &key, &data, i).await
        });
        handles.push(handle);
    }

    let mut success_count = 0;
    let mut had_error = false;
    for handle in handles {
        match handle.await {
            Ok(Ok(true)) => success_count += 1,
            Ok(Ok(false)) => {}
            Ok(Err(e)) => {
                had_error = true;
                info!("  Error: {}", e);
            }
            Err(e) => {
                had_error = true;
                info!("  Task error: {}", e);
            }
        }
    }

    info!("Result: {} out of {} succeeded", success_count, clients.len());

    if success_count > 1 {
        info!(">>> RACE CONDITION DETECTED!");
    } else if success_count == 1 {
        info!(">>> Correct behavior: exactly 1 writer succeeded.");
    } else if had_error {
        return Err("all conditional PUTs failed (e.g. cluster/bucket not ready)".into());
    } else {
        info!(">>> Unexpected: no writers succeeded.");
    }

    Ok(success_count)
}

#[tokio::test]
#[serial]
async fn test_conditional_put_race_cluster() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    crate::common::init_logging();
    info!("Starting conditional PUT race test with auto cluster");

    let mut cluster = RustFSTestClusterEnvironment::new(4).await?;
    cluster.start().await?;

    cluster.create_test_bucket(BUCKET).await?;

    let clients = cluster.create_all_clients()?;

    let iterations = 5;
    let mut races_detected = 0;
    let mut correct_count = 0;
    let mut error_count = 0;

    for i in 1..=iterations {
        let test_key = format!("race-test-{}-{}", std::process::id(), i);

        match run_race_iteration(&clients, &test_key, i).await {
            Ok(success_count) => {
                if success_count > 1 {
                    races_detected += 1;
                } else if success_count == 1 {
                    correct_count += 1;
                }
            }
            Err(e) => {
                error_count += 1;
                warn!("Iteration {} failed (not a race; e.g. cluster/network): {}", i, e)
            }
        }

        cleanup_object(&clients[0], &test_key).await;
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    }

    info!("\n====================================");
    info!("SUMMARY");
    info!("====================================");
    info!("Total iterations:   {}", iterations);
    info!("Correct (1 winner): {}", correct_count);
    info!("Race conditions:    {}", races_detected);
    info!("Errors (skipped):   {}", error_count);

    assert_eq!(races_detected, 0, "Race conditions detected: {}/{}", races_detected, iterations);
    assert_eq!(
        error_count, 0,
        "{} iteration(s) failed due to errors (e.g. cluster not ready)",
        error_count
    );

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_conditional_put_basic_cluster() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    crate::common::init_logging();
    info!("Starting basic conditional PUT test with auto cluster");

    let mut cluster = RustFSTestClusterEnvironment::new(4).await?;
    cluster.start().await?;

    cluster.create_test_bucket(BUCKET).await?;

    let client = cluster.create_s3_client(0)?;
    let test_key = "basic-conditional-put";
    cleanup_object(&client, test_key).await;

    let result = client
        .put_object()
        .bucket(BUCKET)
        .key(test_key)
        .body(Bytes::from("first write").into())
        .if_none_match("*")
        .send()
        .await;
    assert!(result.is_ok(), "First PUT with If-None-Match:* should succeed");

    let result = client
        .put_object()
        .bucket(BUCKET)
        .key(test_key)
        .body(Bytes::from("second write").into())
        .if_none_match("*")
        .send()
        .await;
    assert!(result.is_err(), "Second PUT with If-None-Match:* should fail");

    assert!(
        matches!(result, Err(SdkError::ServiceError(_))),
        "Expected ServiceError but got different error type"
    );

    if let Err(SdkError::ServiceError(e)) = result {
        let code = e.err().meta().code().unwrap_or("");
        assert_eq!(code, "PreconditionFailed");
    }

    cleanup_object(&client, test_key).await;
    Ok(())
}
