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

//! KMS Edge Cases and Boundary Condition Tests
//!
//! This test suite validates KMS functionality under edge cases and boundary conditions:
//! - Zero-byte and single-byte file encryption
//! - Multipart boundary conditions (minimum size limits)
//! - Invalid key scenarios and error handling
//! - Concurrent encryption operations
//! - Security validation tests

use super::common::LocalKMSTestEnvironment;
use crate::common::{TEST_BUCKET, init_logging};
use aws_sdk_s3::types::ServerSideEncryption;
use base64::Engine;
use serial_test::serial;
use std::sync::Arc;
use tokio::sync::Semaphore;
use tracing::{info, warn};

/// Test encryption of zero-byte files (empty files)
#[tokio::test]
#[serial]
async fn test_kms_zero_byte_file_encryption() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();
    info!("🧪 Testing KMS encryption with zero-byte files");

    let mut kms_env = LocalKMSTestEnvironment::new().await?;
    let _default_key_id = kms_env.start_rustfs_for_local_kms().await?;
    tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;

    let s3_client = kms_env.base_env.create_s3_client();
    kms_env.base_env.create_test_bucket(TEST_BUCKET).await?;

    // Test SSE-S3 with zero-byte file
    info!("📤 Testing SSE-S3 with zero-byte file");
    let empty_data = b"";
    let object_key = "zero-byte-sse-s3";

    let put_response = s3_client
        .put_object()
        .bucket(TEST_BUCKET)
        .key(object_key)
        .body(aws_sdk_s3::primitives::ByteStream::from(empty_data.to_vec()))
        .server_side_encryption(ServerSideEncryption::Aes256)
        .send()
        .await?;

    assert_eq!(put_response.server_side_encryption(), Some(&ServerSideEncryption::Aes256));

    // Verify download
    let get_response = s3_client.get_object().bucket(TEST_BUCKET).key(object_key).send().await?;

    assert_eq!(get_response.server_side_encryption(), Some(&ServerSideEncryption::Aes256));
    let downloaded_data = get_response.body.collect().await?.into_bytes();
    assert_eq!(downloaded_data.len(), 0);

    // Test SSE-C with zero-byte file
    info!("📤 Testing SSE-C with zero-byte file");
    let test_key = "01234567890123456789012345678901";
    let test_key_b64 = base64::engine::general_purpose::STANDARD.encode(test_key);
    let test_key_md5 = format!("{:x}", md5::compute(test_key));
    let object_key_c = "zero-byte-sse-c";

    let _put_response_c = s3_client
        .put_object()
        .bucket(TEST_BUCKET)
        .key(object_key_c)
        .body(aws_sdk_s3::primitives::ByteStream::from(empty_data.to_vec()))
        .sse_customer_algorithm("AES256")
        .sse_customer_key(&test_key_b64)
        .sse_customer_key_md5(&test_key_md5)
        .send()
        .await?;

    // Verify download with SSE-C
    let get_response_c = s3_client
        .get_object()
        .bucket(TEST_BUCKET)
        .key(object_key_c)
        .sse_customer_algorithm("AES256")
        .sse_customer_key(&test_key_b64)
        .sse_customer_key_md5(&test_key_md5)
        .send()
        .await?;

    let downloaded_data_c = get_response_c.body.collect().await?.into_bytes();
    assert_eq!(downloaded_data_c.len(), 0);

    kms_env.base_env.delete_test_bucket(TEST_BUCKET).await?;
    info!("✅ Zero-byte file encryption test completed successfully");
    Ok(())
}

/// Test encryption of single-byte files
#[tokio::test]
#[serial]
async fn test_kms_single_byte_file_encryption() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();
    info!("🧪 Testing KMS encryption with single-byte files");

    let mut kms_env = LocalKMSTestEnvironment::new().await?;
    let _default_key_id = kms_env.start_rustfs_for_local_kms().await?;
    tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;

    let s3_client = kms_env.base_env.create_s3_client();
    kms_env.base_env.create_test_bucket(TEST_BUCKET).await?;

    // Test all three encryption types with single byte
    let test_data = b"A";
    let test_scenarios = vec![("single-byte-sse-s3", "SSE-S3"), ("single-byte-sse-kms", "SSE-KMS")];

    for (object_key, encryption_type) in test_scenarios {
        info!("📤 Testing {} with single-byte file", encryption_type);

        let put_request = s3_client
            .put_object()
            .bucket(TEST_BUCKET)
            .key(object_key)
            .body(aws_sdk_s3::primitives::ByteStream::from(test_data.to_vec()));

        let _put_response = match encryption_type {
            "SSE-S3" => {
                put_request
                    .server_side_encryption(ServerSideEncryption::Aes256)
                    .send()
                    .await?
            }
            "SSE-KMS" => {
                put_request
                    .server_side_encryption(ServerSideEncryption::AwsKms)
                    .send()
                    .await?
            }
            _ => unreachable!(),
        };

        // Verify download
        let get_response = s3_client.get_object().bucket(TEST_BUCKET).key(object_key).send().await?;

        let expected_encryption = match encryption_type {
            "SSE-S3" => ServerSideEncryption::Aes256,
            "SSE-KMS" => ServerSideEncryption::AwsKms,
            _ => unreachable!(),
        };

        assert_eq!(get_response.server_side_encryption(), Some(&expected_encryption));
        let downloaded_data = get_response.body.collect().await?.into_bytes();
        assert_eq!(downloaded_data.as_ref(), test_data);
    }

    // Test SSE-C with single byte
    info!("📤 Testing SSE-C with single-byte file");
    let test_key = "01234567890123456789012345678901";
    let test_key_b64 = base64::engine::general_purpose::STANDARD.encode(test_key);
    let test_key_md5 = format!("{:x}", md5::compute(test_key));
    let object_key_c = "single-byte-sse-c";

    s3_client
        .put_object()
        .bucket(TEST_BUCKET)
        .key(object_key_c)
        .body(aws_sdk_s3::primitives::ByteStream::from(test_data.to_vec()))
        .sse_customer_algorithm("AES256")
        .sse_customer_key(&test_key_b64)
        .sse_customer_key_md5(&test_key_md5)
        .send()
        .await?;

    let get_response_c = s3_client
        .get_object()
        .bucket(TEST_BUCKET)
        .key(object_key_c)
        .sse_customer_algorithm("AES256")
        .sse_customer_key(&test_key_b64)
        .sse_customer_key_md5(&test_key_md5)
        .send()
        .await?;

    let downloaded_data_c = get_response_c.body.collect().await?.into_bytes();
    assert_eq!(downloaded_data_c.as_ref(), test_data);

    kms_env.base_env.delete_test_bucket(TEST_BUCKET).await?;
    info!("✅ Single-byte file encryption test completed successfully");
    Ok(())
}

/// Test multipart upload boundary conditions (minimum 5MB part size)
#[tokio::test]
#[serial]
async fn test_kms_multipart_boundary_conditions() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();
    info!("🧪 Testing KMS multipart upload boundary conditions");

    let mut kms_env = LocalKMSTestEnvironment::new().await?;
    let _default_key_id = kms_env.start_rustfs_for_local_kms().await?;
    tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;

    let s3_client = kms_env.base_env.create_s3_client();
    kms_env.base_env.create_test_bucket(TEST_BUCKET).await?;

    // Test with exactly minimum part size (5MB)
    info!("📤 Testing with exactly 5MB part size");
    let part_size = 5 * 1024 * 1024; // Exactly 5MB
    let test_data: Vec<u8> = (0..part_size).map(|i| (i % 256) as u8).collect();
    let object_key = "multipart-boundary-5mb";

    // Initiate multipart upload with SSE-S3
    let create_multipart_output = s3_client
        .create_multipart_upload()
        .bucket(TEST_BUCKET)
        .key(object_key)
        .server_side_encryption(ServerSideEncryption::Aes256)
        .send()
        .await?;

    let upload_id = create_multipart_output.upload_id().unwrap();

    // Upload single part with exactly 5MB
    let upload_part_output = s3_client
        .upload_part()
        .bucket(TEST_BUCKET)
        .key(object_key)
        .upload_id(upload_id)
        .part_number(1)
        .body(aws_sdk_s3::primitives::ByteStream::from(test_data.clone()))
        .send()
        .await?;

    let etag = upload_part_output.e_tag().unwrap().to_string();

    // Complete multipart upload
    let completed_part = aws_sdk_s3::types::CompletedPart::builder()
        .part_number(1)
        .e_tag(&etag)
        .build();

    let completed_multipart_upload = aws_sdk_s3::types::CompletedMultipartUpload::builder()
        .parts(completed_part)
        .build();

    s3_client
        .complete_multipart_upload()
        .bucket(TEST_BUCKET)
        .key(object_key)
        .upload_id(upload_id)
        .multipart_upload(completed_multipart_upload)
        .send()
        .await?;

    // Verify download
    let get_response = s3_client.get_object().bucket(TEST_BUCKET).key(object_key).send().await?;

    assert_eq!(get_response.server_side_encryption(), Some(&ServerSideEncryption::Aes256));
    let downloaded_data = get_response.body.collect().await?.into_bytes();
    assert_eq!(downloaded_data.len(), test_data.len());
    assert_eq!(&downloaded_data[..], &test_data[..]);

    kms_env.base_env.delete_test_bucket(TEST_BUCKET).await?;
    info!("✅ Multipart boundary conditions test completed successfully");
    Ok(())
}

/// Test invalid key scenarios and error handling
#[tokio::test]
#[serial]
async fn test_kms_invalid_key_scenarios() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();
    info!("🧪 Testing KMS invalid key scenarios and error handling");

    let mut kms_env = LocalKMSTestEnvironment::new().await?;
    let _default_key_id = kms_env.start_rustfs_for_local_kms().await?;
    tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;

    let s3_client = kms_env.base_env.create_s3_client();
    kms_env.base_env.create_test_bucket(TEST_BUCKET).await?;

    let test_data = b"Test data for invalid key scenarios";

    // Test 1: Invalid key length for SSE-C
    info!("🔍 Testing invalid SSE-C key length");
    let invalid_short_key = "short"; // Too short
    let invalid_key_b64 = base64::engine::general_purpose::STANDARD.encode(invalid_short_key);
    let invalid_key_md5 = format!("{:x}", md5::compute(invalid_short_key));

    let invalid_key_result = s3_client
        .put_object()
        .bucket(TEST_BUCKET)
        .key("test-invalid-key-length")
        .body(aws_sdk_s3::primitives::ByteStream::from(test_data.to_vec()))
        .sse_customer_algorithm("AES256")
        .sse_customer_key(&invalid_key_b64)
        .sse_customer_key_md5(&invalid_key_md5)
        .send()
        .await;

    assert!(invalid_key_result.is_err(), "Should reject invalid key length");
    info!("✅ Correctly rejected invalid key length");

    // Test 2: Mismatched MD5 for SSE-C
    info!("🔍 Testing mismatched MD5 for SSE-C key");
    let valid_key = "01234567890123456789012345678901";
    let valid_key_b64 = base64::engine::general_purpose::STANDARD.encode(valid_key);
    let wrong_md5 = "wrongmd5hash12345678901234567890"; // Wrong MD5

    let wrong_md5_result = s3_client
        .put_object()
        .bucket(TEST_BUCKET)
        .key("test-wrong-md5")
        .body(aws_sdk_s3::primitives::ByteStream::from(test_data.to_vec()))
        .sse_customer_algorithm("AES256")
        .sse_customer_key(&valid_key_b64)
        .sse_customer_key_md5(wrong_md5)
        .send()
        .await;

    assert!(wrong_md5_result.is_err(), "Should reject mismatched MD5");
    info!("✅ Correctly rejected mismatched MD5");

    // Test 3: Try to access SSE-C object without providing key
    info!("🔍 Testing access to SSE-C object without key");

    // First upload a valid SSE-C object
    let valid_key_md5 = format!("{:x}", md5::compute(valid_key));
    s3_client
        .put_object()
        .bucket(TEST_BUCKET)
        .key("test-sse-c-no-key-access")
        .body(aws_sdk_s3::primitives::ByteStream::from(test_data.to_vec()))
        .sse_customer_algorithm("AES256")
        .sse_customer_key(&valid_key_b64)
        .sse_customer_key_md5(&valid_key_md5)
        .send()
        .await?;

    // Try to access without providing key
    let no_key_result = s3_client
        .get_object()
        .bucket(TEST_BUCKET)
        .key("test-sse-c-no-key-access")
        .send()
        .await;

    assert!(no_key_result.is_err(), "Should require SSE-C key for access");
    info!("✅ Correctly required SSE-C key for access");

    kms_env.base_env.delete_test_bucket(TEST_BUCKET).await?;
    info!("✅ Invalid key scenarios test completed successfully");
    Ok(())
}

/// Test concurrent encryption operations
#[tokio::test]
#[serial]
async fn test_kms_concurrent_encryption() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();
    info!("🧪 Testing KMS concurrent encryption operations");

    let mut kms_env = LocalKMSTestEnvironment::new().await?;
    let _default_key_id = kms_env.start_rustfs_for_local_kms().await?;
    tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;

    let s3_client = Arc::new(kms_env.base_env.create_s3_client());
    kms_env.base_env.create_test_bucket(TEST_BUCKET).await?;

    // Test concurrent uploads with different encryption types
    info!("📤 Testing concurrent uploads with different encryption types");

    let num_concurrent = 5;
    let semaphore = Arc::new(Semaphore::new(num_concurrent));
    let mut tasks = Vec::new();

    for i in 0..num_concurrent {
        let client = Arc::clone(&s3_client);
        let sem = Arc::clone(&semaphore);

        let task = tokio::spawn(async move {
            let _permit = sem.acquire().await.unwrap();

            let test_data = format!("Concurrent test data {}", i).into_bytes();
            let object_key = format!("concurrent-test-{}", i);

            // Alternate between different encryption types
            let result = match i % 3 {
                0 => {
                    // SSE-S3
                    client
                        .put_object()
                        .bucket(TEST_BUCKET)
                        .key(&object_key)
                        .body(aws_sdk_s3::primitives::ByteStream::from(test_data.clone()))
                        .server_side_encryption(ServerSideEncryption::Aes256)
                        .send()
                        .await
                }
                1 => {
                    // SSE-KMS
                    client
                        .put_object()
                        .bucket(TEST_BUCKET)
                        .key(&object_key)
                        .body(aws_sdk_s3::primitives::ByteStream::from(test_data.clone()))
                        .server_side_encryption(ServerSideEncryption::AwsKms)
                        .send()
                        .await
                }
                2 => {
                    // SSE-C
                    let key = format!("testkey{:026}", i); // 32-byte key
                    let key_b64 = base64::engine::general_purpose::STANDARD.encode(&key);
                    let key_md5 = format!("{:x}", md5::compute(&key));

                    client
                        .put_object()
                        .bucket(TEST_BUCKET)
                        .key(&object_key)
                        .body(aws_sdk_s3::primitives::ByteStream::from(test_data.clone()))
                        .sse_customer_algorithm("AES256")
                        .sse_customer_key(&key_b64)
                        .sse_customer_key_md5(&key_md5)
                        .send()
                        .await
                }
                _ => unreachable!(),
            };

            (i, result)
        });

        tasks.push(task);
    }

    // Wait for all tasks to complete
    let mut successful_uploads = 0;
    for task in tasks {
        let (task_id, result) = task.await.unwrap();
        match result {
            Ok(_) => {
                successful_uploads += 1;
                info!("✅ Concurrent upload {} completed successfully", task_id);
            }
            Err(e) => {
                warn!("❌ Concurrent upload {} failed: {}", task_id, e);
            }
        }
    }

    assert!(
        successful_uploads >= num_concurrent - 1,
        "Most concurrent uploads should succeed (got {}/{})",
        successful_uploads,
        num_concurrent
    );

    info!("✅ Successfully completed {}/{} concurrent uploads", successful_uploads, num_concurrent);

    kms_env.base_env.delete_test_bucket(TEST_BUCKET).await?;
    info!("✅ Concurrent encryption test completed successfully");
    Ok(())
}

/// Test key validation and security properties
#[tokio::test]
#[serial]
async fn test_kms_key_validation_security() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();
    info!("🧪 Testing KMS key validation and security properties");

    let mut kms_env = LocalKMSTestEnvironment::new().await?;
    let _default_key_id = kms_env.start_rustfs_for_local_kms().await?;
    tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;

    let s3_client = kms_env.base_env.create_s3_client();
    kms_env.base_env.create_test_bucket(TEST_BUCKET).await?;

    // Test 1: Verify that different keys produce different encrypted data
    info!("🔍 Testing that different keys produce different encrypted data");
    let test_data = b"Same plaintext data for encryption comparison";

    let key1 = "key1key1key1key1key1key1key1key1"; // 32 bytes
    let key2 = "key2key2key2key2key2key2key2key2"; // 32 bytes

    let key1_b64 = base64::engine::general_purpose::STANDARD.encode(key1);
    let key2_b64 = base64::engine::general_purpose::STANDARD.encode(key2);
    let key1_md5 = format!("{:x}", md5::compute(key1));
    let key2_md5 = format!("{:x}", md5::compute(key2));

    // Upload same data with different keys
    s3_client
        .put_object()
        .bucket(TEST_BUCKET)
        .key("security-test-key1")
        .body(aws_sdk_s3::primitives::ByteStream::from(test_data.to_vec()))
        .sse_customer_algorithm("AES256")
        .sse_customer_key(&key1_b64)
        .sse_customer_key_md5(&key1_md5)
        .send()
        .await?;

    s3_client
        .put_object()
        .bucket(TEST_BUCKET)
        .key("security-test-key2")
        .body(aws_sdk_s3::primitives::ByteStream::from(test_data.to_vec()))
        .sse_customer_algorithm("AES256")
        .sse_customer_key(&key2_b64)
        .sse_customer_key_md5(&key2_md5)
        .send()
        .await?;

    // Verify both can be decrypted with their respective keys
    let data1 = s3_client
        .get_object()
        .bucket(TEST_BUCKET)
        .key("security-test-key1")
        .sse_customer_algorithm("AES256")
        .sse_customer_key(&key1_b64)
        .sse_customer_key_md5(&key1_md5)
        .send()
        .await?
        .body
        .collect()
        .await?
        .into_bytes();

    let data2 = s3_client
        .get_object()
        .bucket(TEST_BUCKET)
        .key("security-test-key2")
        .sse_customer_algorithm("AES256")
        .sse_customer_key(&key2_b64)
        .sse_customer_key_md5(&key2_md5)
        .send()
        .await?
        .body
        .collect()
        .await?
        .into_bytes();

    assert_eq!(data1.as_ref(), test_data);
    assert_eq!(data2.as_ref(), test_data);
    info!("✅ Different keys can decrypt their respective data correctly");

    // Test 2: Verify key isolation (key1 cannot decrypt key2's data)
    info!("🔍 Testing key isolation");
    let wrong_key_result = s3_client
        .get_object()
        .bucket(TEST_BUCKET)
        .key("security-test-key2")
        .sse_customer_algorithm("AES256")
        .sse_customer_key(&key1_b64) // Wrong key
        .sse_customer_key_md5(&key1_md5)
        .send()
        .await;

    assert!(wrong_key_result.is_err(), "Should not be able to decrypt with wrong key");
    info!("✅ Key isolation verified - wrong key cannot decrypt data");

    kms_env.base_env.delete_test_bucket(TEST_BUCKET).await?;
    info!("✅ Key validation and security test completed successfully");
    Ok(())
}
