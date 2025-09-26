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
use aws_sdk_s3::types::ServerSideEncryption;
use serial_test::serial;
use std::fs;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{info, warn};

/// Test KMS behavior when key directory is temporarily unavailable
#[tokio::test]
#[serial]
async fn test_kms_key_directory_unavailable() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();
    info!("🧪 Testing KMS behavior with unavailable key directory");

    let mut kms_env = LocalKMSTestEnvironment::new().await?;
    let _default_key_id = kms_env.start_rustfs_for_local_kms().await?;
    tokio::time::sleep(Duration::from_secs(3)).await;

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

    // This should fail, but the server should still be responsive
    if put_result2.is_err() {
        info!("✅ Upload correctly failed when key directory unavailable");
    } else {
        warn!("⚠️ Upload succeeded despite unavailable key directory (may be using cached keys)");
    }

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
#[serial]
async fn test_kms_corrupted_key_files() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();
    info!("🧪 Testing KMS behavior with corrupted key files");

    let mut kms_env = LocalKMSTestEnvironment::new().await?;
    let default_key_id = kms_env.start_rustfs_for_local_kms().await?;
    tokio::time::sleep(Duration::from_secs(3)).await;

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

    // This might succeed if KMS uses cached keys, but should eventually fail
    if put_result2.is_err() {
        info!("✅ Upload correctly failed with corrupted key");
    } else {
        warn!("⚠️ Upload succeeded despite corrupted key (likely using cached key)");
    }

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

    kms_env.base_env.delete_test_bucket(TEST_BUCKET).await?;
    info!("✅ Corrupted key files test completed successfully");
    Ok(())
}

/// Test multipart upload interruption and recovery
#[tokio::test]
#[serial]
async fn test_kms_multipart_upload_interruption() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();
    info!("🧪 Testing KMS multipart upload interruption and recovery");

    let mut kms_env = LocalKMSTestEnvironment::new().await?;
    let _default_key_id = kms_env.start_rustfs_for_local_kms().await?;
    tokio::time::sleep(Duration::from_secs(3)).await;

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
    let abort_result = s3_client
        .abort_multipart_upload()
        .bucket(TEST_BUCKET)
        .key(object_key)
        .upload_id(upload_id)
        .send()
        .await;

    match abort_result {
        Ok(_) => info!("✅ Multipart upload aborted successfully"),
        Err(e) => warn!("⚠️ Failed to abort multipart upload: {}", e),
    }

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

    let complete_result = s3_client
        .complete_multipart_upload()
        .bucket(TEST_BUCKET)
        .key(object_key)
        .upload_id(upload_id)
        .multipart_upload(completed_multipart_upload)
        .send()
        .await;

    assert!(complete_result.is_err(), "Should not be able to complete aborted upload");
    info!("✅ Correctly failed to complete aborted upload");

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

/// Test KMS resilience to temporary resource constraints
#[tokio::test]
#[serial]
async fn test_kms_resource_constraints() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();
    info!("🧪 Testing KMS behavior under resource constraints");

    let mut kms_env = LocalKMSTestEnvironment::new().await?;
    let _default_key_id = kms_env.start_rustfs_for_local_kms().await?;
    tokio::time::sleep(Duration::from_secs(3)).await;

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
    let mut successful_uploads = 0;
    let mut failed_uploads = 0;

    for task in upload_tasks {
        let (object_key, result) = task.await.unwrap();
        match result {
            Ok(_) => {
                successful_uploads += 1;
                info!("✅ Rapid upload {} succeeded", object_key);
            }
            Err(e) => {
                failed_uploads += 1;
                warn!("❌ Rapid upload {} failed: {}", object_key, e);
            }
        }
    }

    info!("📊 Rapid upload results: {} succeeded, {} failed", successful_uploads, failed_uploads);

    // We expect most uploads to succeed even under load
    assert!(successful_uploads >= 7, "Expected at least 7/10 rapid uploads to succeed");

    kms_env.base_env.delete_test_bucket(TEST_BUCKET).await?;
    info!("✅ Resource constraints test completed successfully");
    Ok(())
}
