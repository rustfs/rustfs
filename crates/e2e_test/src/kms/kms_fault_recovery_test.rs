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

//! KMS Fault Recovery and Error Handling Tests
//!
//! This test suite validates KMS behavior under failure conditions:
//! - KMS service unavailability
//! - Network interruptions during multipart uploads
//! - Disk space limitations
//! - Corrupted key files
//! - Recovery from transient failures

use super::common::LocalKMSTestEnvironment;
use crate::common::{TEST_BUCKET, init_logging};
use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::types::ServerSideEncryption;
use std::fs;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{info, warn};

/// Test KMS behavior when key directory is temporarily unavailable
#[tokio::test]
async fn test_kms_key_directory_unavailable() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();
    info!("🧪 Testing KMS behavior with unavailable key directory");

    let mut kms_env = LocalKMSTestEnvironment::new().await?;
    let _default_key_id = kms_env.start_rustfs_for_local_kms().await?;
    kms_env.wait_for_kms_ready().await?;

    let s3_client = kms_env.base_env.create_s3_client();
    kms_env.base_env.create_test_bucket(TEST_BUCKET).await?;

    // First, upload a normal encrypted file to verify KMS is working
    info!("📤 Uploading test file with KMS encryption");
    let test_data = b"Test data before key directory issue";
    let object_key = "test-before-key-issue";

    let put_response = s3_client
        .put_object()
        .bucket(TEST_BUCKET)
        .key(object_key)
        .body(aws_sdk_s3::primitives::ByteStream::from(test_data.to_vec()))
        .server_side_encryption(ServerSideEncryption::Aes256)
        .send()
        .await?;

    assert_eq!(put_response.server_side_encryption(), Some(&ServerSideEncryption::Aes256));

    // Temporarily rename the key directory to simulate unavailability
    info!("🔧 Simulating key directory unavailability");
    let backup_dir = format!("{}.backup", kms_env.kms_keys_dir);
    fs::rename(&kms_env.kms_keys_dir, &backup_dir)?;

    // Try to upload another file - this should fail gracefully
    info!("📤 Attempting upload with unavailable key directory");
    let test_data2 = b"Test data during key directory issue";
    let object_key2 = "test-during-key-issue";

    let put_result2 = s3_client
        .put_object()
        .bucket(TEST_BUCKET)
        .key(object_key2)
        .body(aws_sdk_s3::primitives::ByteStream::from(test_data2.to_vec()))
        .server_side_encryption(ServerSideEncryption::Aes256)
        .send()
        .await;

    let unavailable_error = put_result2.expect_err("a missing Local KMS key directory must reject encrypted writes");
    assert_eq!(unavailable_error.raw_response().map(|response| response.status().as_u16()), Some(500));
    assert_eq!(
        unavailable_error.as_service_error().and_then(ProvideErrorMetadata::code),
        Some("InternalError")
    );
    let unavailable_absence = s3_client
        .get_object()
        .bucket(TEST_BUCKET)
        .key(object_key2)
        .send()
        .await
        .expect_err("a write rejected by unavailable KMS must not publish an object");
    assert_eq!(unavailable_absence.raw_response().map(|response| response.status().as_u16()), Some(404));
    assert_eq!(
        unavailable_absence.as_service_error().and_then(ProvideErrorMetadata::code),
        Some("NoSuchKey")
    );
    info!("✅ Upload correctly failed when key directory unavailable");

    // Restore the key directory
    info!("🔧 Restoring key directory");
    fs::rename(&backup_dir, &kms_env.kms_keys_dir)?;

    // Wait a moment for KMS to detect the restored directory
    sleep(Duration::from_secs(2)).await;

    // Try uploading again - this should work
    info!("📤 Uploading after key directory restoration");
    let test_data3 = b"Test data after key directory restoration";
    let object_key3 = "test-after-key-restoration";

    let put_response3 = s3_client
        .put_object()
        .bucket(TEST_BUCKET)
        .key(object_key3)
        .body(aws_sdk_s3::primitives::ByteStream::from(test_data3.to_vec()))
        .server_side_encryption(ServerSideEncryption::Aes256)
        .send()
        .await?;

    assert_eq!(put_response3.server_side_encryption(), Some(&ServerSideEncryption::Aes256));

    let get_response3 = s3_client.get_object().bucket(TEST_BUCKET).key(object_key3).send().await?;
    assert_eq!(get_response3.server_side_encryption(), Some(&ServerSideEncryption::Aes256));
    let downloaded_data3 = get_response3.body.collect().await?.into_bytes();
    assert_eq!(downloaded_data3.as_ref(), test_data3);

    // Verify we can still access the original file
    info!("📥 Verifying access to original encrypted file");
    let get_response = s3_client.get_object().bucket(TEST_BUCKET).key(object_key).send().await?;

    let downloaded_data = get_response.body.collect().await?.into_bytes();
    assert_eq!(downloaded_data.as_ref(), test_data);

    kms_env.base_env.delete_test_bucket(TEST_BUCKET).await?;
    info!("✅ Key directory unavailability test completed successfully");
    Ok(())
}

/// Test handling of corrupted key files
#[tokio::test]
async fn test_kms_corrupted_key_files() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();
    info!("🧪 Testing KMS behavior with corrupted key files");

    let mut kms_env = LocalKMSTestEnvironment::new().await?;
    let default_key_id = kms_env.start_rustfs_for_local_kms().await?;
    kms_env.wait_for_kms_ready().await?;

    let s3_client = kms_env.base_env.create_s3_client();
    kms_env.base_env.create_test_bucket(TEST_BUCKET).await?;

    // Upload a file with valid key
    info!("📤 Uploading file with valid key");
    let test_data = b"Test data before key corruption";
    let object_key = "test-before-corruption";

    s3_client
        .put_object()
        .bucket(TEST_BUCKET)
        .key(object_key)
        .body(aws_sdk_s3::primitives::ByteStream::from(test_data.to_vec()))
        .server_side_encryption(ServerSideEncryption::Aes256)
        .send()
        .await?;

    // Corrupt the default key file
    info!("🔧 Corrupting default key file");
    let key_file_path = format!("{}/{}.key", kms_env.kms_keys_dir, default_key_id);
    let backup_key_path = format!("{key_file_path}.backup");

    // Backup the original key file
    fs::copy(&key_file_path, &backup_key_path)?;

    // Write corrupted data to the key file
    fs::write(&key_file_path, b"corrupted key data")?;

    // Wait for potential key cache to expire
    sleep(Duration::from_secs(1)).await;

    // Try to upload with corrupted key - this should fail
    info!("📤 Attempting upload with corrupted key");
    let test_data2 = b"Test data with corrupted key";
    let object_key2 = "test-with-corrupted-key";

    let put_result2 = s3_client
        .put_object()
        .bucket(TEST_BUCKET)
        .key(object_key2)
        .body(aws_sdk_s3::primitives::ByteStream::from(test_data2.to_vec()))
        .server_side_encryption(ServerSideEncryption::Aes256)
        .send()
        .await;

    let corrupt_error = put_result2.expect_err("corrupt Local KMS key material must reject encrypted writes");
    assert_eq!(corrupt_error.raw_response().map(|response| response.status().as_u16()), Some(500));
    assert_eq!(
        corrupt_error.as_service_error().and_then(ProvideErrorMetadata::code),
        Some("InternalError")
    );
    let corrupt_absence = s3_client
        .get_object()
        .bucket(TEST_BUCKET)
        .key(object_key2)
        .send()
        .await
        .expect_err("a write rejected by corrupt KMS material must not publish an object");
    assert_eq!(corrupt_absence.raw_response().map(|response| response.status().as_u16()), Some(404));
    assert_eq!(corrupt_absence.as_service_error().and_then(ProvideErrorMetadata::code), Some("NoSuchKey"));
    info!("✅ Upload correctly failed with corrupted key");

    // Restore the original key file
    info!("🔧 Restoring original key file");
    fs::copy(&backup_key_path, &key_file_path)?;
    fs::remove_file(&backup_key_path)?;

    // Wait for KMS to detect the restored key
    sleep(Duration::from_secs(2)).await;

    // Try uploading again - this should work
    info!("📤 Uploading after key restoration");
    let test_data3 = b"Test data after key restoration";
    let object_key3 = "test-after-key-restoration";

    let put_response3 = s3_client
        .put_object()
        .bucket(TEST_BUCKET)
        .key(object_key3)
        .body(aws_sdk_s3::primitives::ByteStream::from(test_data3.to_vec()))
        .server_side_encryption(ServerSideEncryption::Aes256)
        .send()
        .await?;

    assert_eq!(put_response3.server_side_encryption(), Some(&ServerSideEncryption::Aes256));

    let get_response3 = s3_client.get_object().bucket(TEST_BUCKET).key(object_key3).send().await?;
    assert_eq!(get_response3.server_side_encryption(), Some(&ServerSideEncryption::Aes256));
    let downloaded_data3 = get_response3.body.collect().await?.into_bytes();
    assert_eq!(downloaded_data3.as_ref(), test_data3);

    kms_env.base_env.delete_test_bucket(TEST_BUCKET).await?;
    info!("✅ Corrupted key files test completed successfully");
    Ok(())
}

/// Test multipart upload interruption and recovery
#[tokio::test]
async fn test_kms_multipart_upload_interruption() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();
    info!("🧪 Testing KMS multipart upload interruption and recovery");

    let mut kms_env = LocalKMSTestEnvironment::new().await?;
    let _default_key_id = kms_env.start_rustfs_for_local_kms().await?;
    kms_env.wait_for_kms_ready().await?;

    let s3_client = kms_env.base_env.create_s3_client();
    kms_env.base_env.create_test_bucket(TEST_BUCKET).await?;

    // Test data for multipart upload
    let part_size = 5 * 1024 * 1024; // 5MB per part
    let total_parts = 3;
    let total_size = part_size * total_parts;
    let test_data: Vec<u8> = (0..total_size).map(|i| (i % 256) as u8).collect();
    let object_key = "multipart-interruption-test";

    info!("📤 Starting multipart upload with encryption");

    // Initiate multipart upload
    let create_multipart_output = s3_client
        .create_multipart_upload()
        .bucket(TEST_BUCKET)
        .key(object_key)
        .server_side_encryption(ServerSideEncryption::Aes256)
        .send()
        .await?;

    let upload_id = create_multipart_output.upload_id().unwrap();
    info!("✅ Multipart upload initiated with ID: {}", upload_id);

    // Upload first part successfully
    info!("📤 Uploading part 1");
    let part1_data = &test_data[0..part_size];
    let upload_part1_output = s3_client
        .upload_part()
        .bucket(TEST_BUCKET)
        .key(object_key)
        .upload_id(upload_id)
        .part_number(1)
        .body(aws_sdk_s3::primitives::ByteStream::from(part1_data.to_vec()))
        .send()
        .await?;

    let part1_etag = upload_part1_output.e_tag().unwrap().to_string();
    info!("✅ Part 1 uploaded successfully");

    // Upload second part successfully
    info!("📤 Uploading part 2");
    let part2_data = &test_data[part_size..part_size * 2];
    let upload_part2_output = s3_client
        .upload_part()
        .bucket(TEST_BUCKET)
        .key(object_key)
        .upload_id(upload_id)
        .part_number(2)
        .body(aws_sdk_s3::primitives::ByteStream::from(part2_data.to_vec()))
        .send()
        .await?;

    let part2_etag = upload_part2_output.e_tag().unwrap().to_string();
    info!("✅ Part 2 uploaded successfully");

    // Simulate interruption - we'll NOT upload part 3 and instead abort the upload
    info!("🔧 Simulating upload interruption");

    // Abort the multipart upload
    s3_client
        .abort_multipart_upload()
        .bucket(TEST_BUCKET)
        .key(object_key)
        .upload_id(upload_id)
        .send()
        .await?;
    info!("✅ Multipart upload aborted successfully");

    // Try to complete the aborted upload - this should fail
    info!("🔍 Attempting to complete aborted upload");
    let completed_parts = vec![
        aws_sdk_s3::types::CompletedPart::builder()
            .part_number(1)
            .e_tag(&part1_etag)
            .build(),
        aws_sdk_s3::types::CompletedPart::builder()
            .part_number(2)
            .e_tag(&part2_etag)
            .build(),
    ];

    let completed_multipart_upload = aws_sdk_s3::types::CompletedMultipartUpload::builder()
        .set_parts(Some(completed_parts))
        .build();

    let complete_error = s3_client
        .complete_multipart_upload()
        .bucket(TEST_BUCKET)
        .key(object_key)
        .upload_id(upload_id)
        .multipart_upload(completed_multipart_upload)
        .send()
        .await
        .expect_err("an aborted multipart upload must not be completable");
    assert_eq!(complete_error.raw_response().map(|response| response.status().as_u16()), Some(404));
    assert_eq!(
        complete_error.as_service_error().and_then(ProvideErrorMetadata::code),
        Some("NoSuchUpload")
    );
    assert_eq!(
        complete_error.as_service_error().and_then(ProvideErrorMetadata::message),
        Some(
            "The specified multipart upload does not exist. The upload ID may be invalid, or the upload may have been aborted or completed."
        )
    );
    info!("✅ Correctly failed to complete aborted upload");

    let missing_object = s3_client
        .get_object()
        .bucket(TEST_BUCKET)
        .key(object_key)
        .send()
        .await
        .expect_err("aborting a multipart upload must not publish an object");
    assert_eq!(missing_object.raw_response().map(|response| response.status().as_u16()), Some(404));
    assert_eq!(missing_object.as_service_error().and_then(ProvideErrorMetadata::code), Some("NoSuchKey"));

    // Start a new multipart upload and complete it successfully
    info!("📤 Starting new multipart upload");
    let create_multipart_output2 = s3_client
        .create_multipart_upload()
        .bucket(TEST_BUCKET)
        .key(object_key)
        .server_side_encryption(ServerSideEncryption::Aes256)
        .send()
        .await?;

    let upload_id2 = create_multipart_output2.upload_id().unwrap();

    // Upload all parts for the new upload
    let mut completed_parts2 = Vec::new();
    for part_number in 1..=total_parts {
        let start = (part_number - 1) * part_size;
        let end = std::cmp::min(start + part_size, total_size);
        let part_data = &test_data[start..end];

        let upload_part_output = s3_client
            .upload_part()
            .bucket(TEST_BUCKET)
            .key(object_key)
            .upload_id(upload_id2)
            .part_number(part_number as i32)
            .body(aws_sdk_s3::primitives::ByteStream::from(part_data.to_vec()))
            .send()
            .await?;

        let etag = upload_part_output.e_tag().unwrap().to_string();
        completed_parts2.push(
            aws_sdk_s3::types::CompletedPart::builder()
                .part_number(part_number as i32)
                .e_tag(&etag)
                .build(),
        );

        info!("✅ Part {} uploaded successfully", part_number);
    }

    // Complete the new multipart upload
    let completed_multipart_upload2 = aws_sdk_s3::types::CompletedMultipartUpload::builder()
        .set_parts(Some(completed_parts2))
        .build();

    let _complete_output2 = s3_client
        .complete_multipart_upload()
        .bucket(TEST_BUCKET)
        .key(object_key)
        .upload_id(upload_id2)
        .multipart_upload(completed_multipart_upload2)
        .send()
        .await?;

    info!("✅ New multipart upload completed successfully");

    // Verify the completed upload
    let get_response = s3_client.get_object().bucket(TEST_BUCKET).key(object_key).send().await?;

    assert_eq!(get_response.server_side_encryption(), Some(&ServerSideEncryption::Aes256));
    let downloaded_data = get_response.body.collect().await?.into_bytes();
    assert_eq!(downloaded_data.len(), total_size);
    assert_eq!(&downloaded_data[..], &test_data[..]);

    info!("✅ Downloaded data matches original test data");

    kms_env.base_env.delete_test_bucket(TEST_BUCKET).await?;
    info!("✅ Multipart upload interruption test completed successfully");
    Ok(())
}

/// Test concurrent KMS encryption requests
#[tokio::test]
async fn test_kms_concurrent_encryption_requests() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let mut kms_env = LocalKMSTestEnvironment::new().await?;
    let _default_key_id = kms_env.start_rustfs_for_local_kms().await?;
    kms_env.wait_for_kms_ready().await?;

    let s3_client = kms_env.base_env.create_s3_client();
    kms_env.base_env.create_test_bucket(TEST_BUCKET).await?;

    // Test multiple rapid encryption requests
    info!("📤 Testing rapid successive encryption requests");
    let mut upload_tasks = Vec::new();

    for i in 0..10 {
        let client = s3_client.clone();
        let test_data = format!("Rapid test data {i}").into_bytes();
        let object_key = format!("rapid-test-{i}");

        let task = tokio::spawn(async move {
            let result = client
                .put_object()
                .bucket(TEST_BUCKET)
                .key(&object_key)
                .body(aws_sdk_s3::primitives::ByteStream::from(test_data))
                .server_side_encryption(ServerSideEncryption::Aes256)
                .send()
                .await;
            (object_key, result)
        });

        upload_tasks.push(task);
    }

    // Wait for all uploads to complete
    let mut failures = Vec::new();

    for task in upload_tasks {
        let (object_key, result) = task.await?;
        match result {
            Ok(_) => {
                info!("✅ Rapid upload {} succeeded", object_key);
            }
            Err(e) => {
                warn!("❌ Rapid upload {} failed: {}", object_key, e);
                failures.push(format!("{object_key}: {e}"));
            }
        }
    }

    assert!(
        failures.is_empty(),
        "all 10 concurrent KMS uploads must succeed; failures: {}",
        failures.join("; ")
    );

    kms_env.base_env.delete_test_bucket(TEST_BUCKET).await?;
    Ok(())
}
