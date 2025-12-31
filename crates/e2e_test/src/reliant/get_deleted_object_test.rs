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

//! Test for GetObject on deleted objects
//!
//! This test reproduces the issue where getting a deleted object returns
//! a networking error instead of NoSuchKey.

#![cfg(test)]

use aws_config::meta::region::RegionProviderChain;
use aws_sdk_s3::Client;
use aws_sdk_s3::config::{Credentials, Region};
use aws_sdk_s3::error::SdkError;
use bytes::Bytes;
use serial_test::serial;
use std::error::Error;
use tracing::info;

const ENDPOINT: &str = "http://localhost:9000";
const ACCESS_KEY: &str = "rustfsadmin";
const SECRET_KEY: &str = "rustfsadmin";
const BUCKET: &str = "test-get-deleted-bucket";

async fn create_aws_s3_client() -> Result<Client, Box<dyn Error>> {
    let region_provider = RegionProviderChain::default_provider().or_else(Region::new("us-east-1"));
    let shared_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(region_provider)
        .credentials_provider(Credentials::new(ACCESS_KEY, SECRET_KEY, None, None, "static"))
        .endpoint_url(ENDPOINT)
        .load()
        .await;

    let client = Client::from_conf(
        aws_sdk_s3::Config::from(&shared_config)
            .to_builder()
            .force_path_style(true)
            .build(),
    );
    Ok(client)
}

/// Setup test bucket, creating it if it doesn't exist
async fn setup_test_bucket(client: &Client) -> Result<(), Box<dyn Error>> {
    match client.create_bucket().bucket(BUCKET).send().await {
        Ok(_) => {}
        Err(SdkError::ServiceError(e)) => {
            let e = e.into_err();
            let error_code = e.meta().code().unwrap_or("");
            if !error_code.eq("BucketAlreadyExists") && !error_code.eq("BucketAlreadyOwnedByYou") {
                return Err(e.into());
            }
        }
        Err(e) => {
            return Err(e.into());
        }
    }
    Ok(())
}

#[tokio::test]
#[serial]
#[ignore = "requires running RustFS server at localhost:9000"]
async fn test_get_deleted_object_returns_nosuchkey() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_test_writer()
        .try_init();

    info!("🧪 Starting test_get_deleted_object_returns_nosuchkey");

    let client = create_aws_s3_client().await?;
    setup_test_bucket(&client).await?;

    // Upload a test object
    let key = "test-file-to-delete.txt";
    let content = b"This will be deleted soon!";

    info!("Uploading object: {}", key);
    client
        .put_object()
        .bucket(BUCKET)
        .key(key)
        .body(Bytes::from_static(content).into())
        .send()
        .await?;

    // Verify object exists
    info!("Verifying object exists");
    let get_result = client.get_object().bucket(BUCKET).key(key).send().await;

    assert!(get_result.is_ok(), "Object should exist after upload");

    // Delete the object
    info!("Deleting object: {}", key);
    client.delete_object().bucket(BUCKET).key(key).send().await?;

    // Try to get the deleted object - should return NoSuchKey error
    info!("Attempting to get deleted object - expecting NoSuchKey error");
    let get_result = client.get_object().bucket(BUCKET).key(key).send().await;

    // Check that we get an error
    assert!(get_result.is_err(), "Getting deleted object should return an error");

    // Check that the error is NoSuchKey, not a networking error
    let err = get_result.unwrap_err();

    // Print the error for debugging
    info!("Error received: {:?}", err);

    // Check if it's a service error
    match err {
        SdkError::ServiceError(service_err) => {
            let s3_err = service_err.into_err();
            info!("Service error code: {:?}", s3_err.meta().code());

            // The error should be NoSuchKey
            assert!(s3_err.is_no_such_key(), "Error should be NoSuchKey, got: {s3_err:?}");

            info!("✅ Test passed: GetObject on deleted object correctly returns NoSuchKey");
        }
        other_err => {
            panic!("Expected ServiceError with NoSuchKey, but got: {other_err:?}");
        }
    }

    // Cleanup
    let _ = client.delete_object().bucket(BUCKET).key(key).send().await;

    Ok(())
}

/// Test that HeadObject on a deleted object also returns NoSuchKey
#[tokio::test]
#[serial]
#[ignore = "requires running RustFS server at localhost:9000"]
async fn test_head_deleted_object_returns_nosuchkey() -> Result<(), Box<dyn std::error::Error>> {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_test_writer()
        .try_init();

    info!("🧪 Starting test_head_deleted_object_returns_nosuchkey");

    let client = create_aws_s3_client().await?;
    setup_test_bucket(&client).await?;

    let key = "test-head-deleted.txt";
    let content = b"Test content for HeadObject";

    // Upload and verify
    client
        .put_object()
        .bucket(BUCKET)
        .key(key)
        .body(Bytes::from_static(content).into())
        .send()
        .await?;

    // Delete the object
    client.delete_object().bucket(BUCKET).key(key).send().await?;

    // Try to head the deleted object
    let head_result = client.head_object().bucket(BUCKET).key(key).send().await;

    assert!(head_result.is_err(), "HeadObject on deleted object should return an error");

    match head_result.unwrap_err() {
        SdkError::ServiceError(service_err) => {
            let s3_err = service_err.into_err();
            assert!(
                s3_err.meta().code() == Some("NoSuchKey") || s3_err.meta().code() == Some("NotFound"),
                "Error should be NoSuchKey or NotFound, got: {s3_err:?}"
            );
            info!("✅ HeadObject correctly returns NoSuchKey/NotFound");
        }
        other_err => {
            panic!("Expected ServiceError but got: {other_err:?}");
        }
    }

    Ok(())
}

/// Test GetObject with non-existent key (never existed)
#[tokio::test]
#[serial]
#[ignore = "requires running RustFS server at localhost:9000"]
async fn test_get_nonexistent_object_returns_nosuchkey() -> Result<(), Box<dyn std::error::Error>> {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_test_writer()
        .try_init();

    info!("🧪 Starting test_get_nonexistent_object_returns_nosuchkey");

    let client = create_aws_s3_client().await?;
    setup_test_bucket(&client).await?;

    // Try to get an object that never existed
    let key = "this-key-never-existed.txt";

    let get_result = client.get_object().bucket(BUCKET).key(key).send().await;

    assert!(get_result.is_err(), "Getting non-existent object should return an error");

    match get_result.unwrap_err() {
        SdkError::ServiceError(service_err) => {
            let s3_err = service_err.into_err();
            assert!(s3_err.is_no_such_key(), "Error should be NoSuchKey, got: {s3_err:?}");
            info!("✅ GetObject correctly returns NoSuchKey for non-existent object");
        }
        other_err => {
            panic!("Expected ServiceError with NoSuchKey, but got: {other_err:?}");
        }
    }

    Ok(())
}

/// Test multiple consecutive GetObject calls on deleted object
/// This ensures the fix is stable and doesn't have race conditions
#[tokio::test]
#[serial]
#[ignore = "requires running RustFS server at localhost:9000"]
async fn test_multiple_gets_deleted_object() -> Result<(), Box<dyn std::error::Error>> {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_test_writer()
        .try_init();

    info!("🧪 Starting test_multiple_gets_deleted_object");

    let client = create_aws_s3_client().await?;
    setup_test_bucket(&client).await?;

    let key = "test-multiple-gets.txt";
    let content = b"Test content";

    // Upload and delete
    client
        .put_object()
        .bucket(BUCKET)
        .key(key)
        .body(Bytes::from_static(content).into())
        .send()
        .await?;

    client.delete_object().bucket(BUCKET).key(key).send().await?;

    // Try multiple consecutive GetObject calls
    for i in 1..=5 {
        info!("Attempt {} to get deleted object", i);
        let get_result = client.get_object().bucket(BUCKET).key(key).send().await;

        assert!(get_result.is_err(), "Attempt {i}: should return error");

        match get_result.unwrap_err() {
            SdkError::ServiceError(service_err) => {
                let s3_err = service_err.into_err();
                assert!(s3_err.is_no_such_key(), "Attempt {i}: Error should be NoSuchKey, got: {s3_err:?}");
            }
            other_err => {
                panic!("Attempt {i}: Expected ServiceError but got: {other_err:?}");
            }
        }
    }

    info!("✅ All 5 attempts correctly returned NoSuchKey");
    Ok(())
}
