// Copyright 2024 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
#![allow(clippy::upper_case_acronyms)]
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Step-by-step test cases for sharded upload encryption
//!
//! This test suite will validate every step of the sharded upload encryption feature:
//! 1. Test the underlying single-shard encryption (validate the encryption underlying logic)
//! 2. Test multi-shard uploads (verify shard stitching logic)
//! 3. Test the saving and reading of encrypted metadata
//! 4. Test the complete sharded upload encryption process

use super::common::LocalKMSTestEnvironment;
use crate::common::{TEST_BUCKET, init_logging};
use serial_test::serial;
use tracing::{debug, info};

/// Step 1: Test the basic single-file encryption function (ensure that SSE-S3 works properly in non-sharded scenarios)
#[tokio::test]
#[serial]
async fn test_step1_basic_single_file_encryption() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();
    info!("🧪 Step 1: Test the basic single-file encryption function");

    let mut kms_env = LocalKMSTestEnvironment::new().await?;
    let _default_key_id = kms_env.start_rustfs_for_local_kms().await?;
    tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;

    let s3_client = kms_env.base_env.create_s3_client();
    kms_env.base_env.create_test_bucket(TEST_BUCKET).await?;

    // Test small file encryption (should be stored inline)
    let test_data = b"Hello, this is a small test file for SSE-S3!";
    let object_key = "test-single-file-encrypted";

    info!("📤 Upload a small file ({} bytes) with SSE-S3 encryption enabled", test_data.len());
    let put_response = s3_client
        .put_object()
        .bucket(TEST_BUCKET)
        .key(object_key)
        .body(aws_sdk_s3::primitives::ByteStream::from(test_data.to_vec()))
        .server_side_encryption(aws_sdk_s3::types::ServerSideEncryption::Aes256)
        .send()
        .await?;

    debug!("PUT responds to ETags: {:?}", put_response.e_tag());
    debug!("PUT responds to SSE: {:?}", put_response.server_side_encryption());

    // Verify that the PUT response contains the correct cipher header
    assert_eq!(
        put_response.server_side_encryption(),
        Some(&aws_sdk_s3::types::ServerSideEncryption::Aes256)
    );

    info!("📥 Download the file and verify the encryption status");
    let get_response = s3_client.get_object().bucket(TEST_BUCKET).key(object_key).send().await?;

    debug!("GET responds to SSE: {:?}", get_response.server_side_encryption());

    // Verify that the GET response contains the correct cipher header
    assert_eq!(
        get_response.server_side_encryption(),
        Some(&aws_sdk_s3::types::ServerSideEncryption::Aes256)
    );

    // Verify data integrity
    let downloaded_data = get_response.body.collect().await?.into_bytes();
    assert_eq!(&downloaded_data[..], test_data);

    kms_env.base_env.delete_test_bucket(TEST_BUCKET).await?;
    info!("✅ Step 1: The basic single file encryption function is normal");
    Ok(())
}

/// Step 2: Test the unencrypted shard upload (make sure the shard upload base is working properly)
#[tokio::test]
#[serial]
async fn test_step2_basic_multipart_upload_without_encryption() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();
    info!("🧪 Step 2: Test unencrypted shard uploads");

    let mut kms_env = LocalKMSTestEnvironment::new().await?;
    let _default_key_id = kms_env.start_rustfs_for_local_kms().await?;
    tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;

    let s3_client = kms_env.base_env.create_s3_client();
    kms_env.base_env.create_test_bucket(TEST_BUCKET).await?;

    let object_key = "test-multipart-no-encryption";
    let part_size = 5 * 1024 * 1024; // 5MB per part (S3 minimum)
    let total_parts = 2;
    let total_size = part_size * total_parts;

    // Generate test data (with obvious patterns for easy verification)
    let test_data: Vec<u8> = (0..total_size).map(|i| (i % 256) as u8).collect();

    info!(
        "🚀 Start sharded upload (unencrypted): {} parts, {}MB each",
        total_parts,
        part_size / (1024 * 1024)
    );

    // Step 1: Create a sharded upload
    let create_multipart_output = s3_client
        .create_multipart_upload()
        .bucket(TEST_BUCKET)
        .key(object_key)
        .send()
        .await?;

    let upload_id = create_multipart_output.upload_id().unwrap();
    info!("📋 Create a shard upload with ID: {}", upload_id);

    // Step 2: Upload individual shards
    let mut completed_parts = Vec::new();
    for part_number in 1..=total_parts {
        let start = (part_number - 1) * part_size;
        let end = std::cmp::min(start + part_size, total_size);
        let part_data = &test_data[start..end];

        info!("📤 Upload the shard {} ({} bytes)", part_number, part_data.len());

        let upload_part_output = s3_client
            .upload_part()
            .bucket(TEST_BUCKET)
            .key(object_key)
            .upload_id(upload_id)
            .part_number(part_number as i32)
            .body(aws_sdk_s3::primitives::ByteStream::from(part_data.to_vec()))
            .send()
            .await?;

        let etag = upload_part_output.e_tag().unwrap().to_string();
        completed_parts.push(
            aws_sdk_s3::types::CompletedPart::builder()
                .part_number(part_number as i32)
                .e_tag(&etag)
                .build(),
        );

        debug!("Fragment {} upload complete,ETag: {}", part_number, etag);
    }

    // Step 3: Complete the shard upload
    let completed_multipart_upload = aws_sdk_s3::types::CompletedMultipartUpload::builder()
        .set_parts(Some(completed_parts))
        .build();

    info!("🔗 Complete the shard upload");
    let complete_output = s3_client
        .complete_multipart_upload()
        .bucket(TEST_BUCKET)
        .key(object_key)
        .upload_id(upload_id)
        .multipart_upload(completed_multipart_upload)
        .send()
        .await?;

    debug!("Complete the shard upload,ETag: {:?}", complete_output.e_tag());

    // Step 4: Download and verify
    info!("📥 Download the file and verify data integrity");
    let get_response = s3_client.get_object().bucket(TEST_BUCKET).key(object_key).send().await?;

    let downloaded_data = get_response.body.collect().await?.into_bytes();
    assert_eq!(downloaded_data.len(), total_size);
    assert_eq!(&downloaded_data[..], &test_data[..]);

    kms_env.base_env.delete_test_bucket(TEST_BUCKET).await?;
    info!("✅ Step 2: Unencrypted shard upload functions normally");
    Ok(())
}

/// Step 3: Test Shard Upload + SSE-S3 Encryption (Focus Test)
#[tokio::test]
#[serial]
async fn test_step3_multipart_upload_with_sse_s3() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();
    info!("🧪 Step 3: Test Shard Upload + SSE-S3 Encryption");

    let mut kms_env = LocalKMSTestEnvironment::new().await?;
    let _default_key_id = kms_env.start_rustfs_for_local_kms().await?;
    tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;

    let s3_client = kms_env.base_env.create_s3_client();
    kms_env.base_env.create_test_bucket(TEST_BUCKET).await?;

    let object_key = "test-multipart-sse-s3";
    let part_size = 5 * 1024 * 1024; // 5MB per part
    let total_parts = 2;
    let total_size = part_size * total_parts;

    // Generate test data
    let test_data: Vec<u8> = (0..total_size).map(|i| ((i / 1000) % 256) as u8).collect();

    info!(
        "🔐 Start sharded upload (SSE-S3 encryption): {} parts, {}MB each",
        total_parts,
        part_size / (1024 * 1024)
    );

    // Step 1: Create a shard upload and enable SSE-S3
    let create_multipart_output = s3_client
        .create_multipart_upload()
        .bucket(TEST_BUCKET)
        .key(object_key)
        .server_side_encryption(aws_sdk_s3::types::ServerSideEncryption::Aes256)
        .send()
        .await?;

    let upload_id = create_multipart_output.upload_id().unwrap();
    info!("📋 Create an encrypted shard upload with ID: {}", upload_id);

    // Verify the CreateMultipartUpload response (if there is an SSE header)
    if let Some(sse) = create_multipart_output.server_side_encryption() {
        debug!("CreateMultipartUpload Contains SSE responses: {:?}", sse);
        assert_eq!(sse, &aws_sdk_s3::types::ServerSideEncryption::Aes256);
    } else {
        debug!("CreateMultipartUpload does not contain SSE response headers (normal in some implementations)");
    }

    // Step 2: Upload individual shards
    let mut completed_parts = Vec::new();
    for part_number in 1..=total_parts {
        let start = (part_number - 1) * part_size;
        let end = std::cmp::min(start + part_size, total_size);
        let part_data = &test_data[start..end];

        info!("🔐 Upload encrypted shards {} ({} bytes)", part_number, part_data.len());

        let upload_part_output = s3_client
            .upload_part()
            .bucket(TEST_BUCKET)
            .key(object_key)
            .upload_id(upload_id)
            .part_number(part_number as i32)
            .body(aws_sdk_s3::primitives::ByteStream::from(part_data.to_vec()))
            .send()
            .await?;

        let etag = upload_part_output.e_tag().unwrap().to_string();
        completed_parts.push(
            aws_sdk_s3::types::CompletedPart::builder()
                .part_number(part_number as i32)
                .e_tag(&etag)
                .build(),
        );

        debug!("Encrypted shard {} upload complete,ETag: {}", part_number, etag);
    }

    // Step 3: Complete the shard upload
    let completed_multipart_upload = aws_sdk_s3::types::CompletedMultipartUpload::builder()
        .set_parts(Some(completed_parts))
        .build();

    info!("🔗 Complete the encrypted shard upload");
    let complete_output = s3_client
        .complete_multipart_upload()
        .bucket(TEST_BUCKET)
        .key(object_key)
        .upload_id(upload_id)
        .multipart_upload(completed_multipart_upload)
        .send()
        .await?;

    debug!("Encrypted multipart upload completed with ETag {:?}", complete_output.e_tag());

    // Step 4: HEAD request to inspect metadata
    info!("📋 Inspecting object metadata");
    let head_response = s3_client.head_object().bucket(TEST_BUCKET).key(object_key).send().await?;

    debug!("HEAD response SSE: {:?}", head_response.server_side_encryption());
    debug!("HEAD response metadata: {:?}", head_response.metadata());

    // Step 5: GET request to download and verify
    info!("📥 Downloading encrypted object for verification");
    let get_response = s3_client.get_object().bucket(TEST_BUCKET).key(object_key).send().await?;

    debug!("GET response SSE: {:?}", get_response.server_side_encryption());

    // 🎯 Critical check: GET response must include SSE-S3 headers
    assert_eq!(
        get_response.server_side_encryption(),
        Some(&aws_sdk_s3::types::ServerSideEncryption::Aes256)
    );

    // Verify data integrity
    let downloaded_data = get_response.body.collect().await?.into_bytes();
    assert_eq!(downloaded_data.len(), total_size);
    assert_eq!(&downloaded_data[..], &test_data[..]);

    kms_env.base_env.delete_test_bucket(TEST_BUCKET).await?;
    info!("✅ Step 3 passed: multipart upload with SSE-S3 encryption");
    Ok(())
}

/// Step 4: test larger multipart uploads (streaming encryption)
#[tokio::test]
#[serial]
async fn test_step4_large_multipart_upload_with_encryption() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();
    info!("🧪 Step 4: test large-file multipart encryption");

    let mut kms_env = LocalKMSTestEnvironment::new().await?;
    let _default_key_id = kms_env.start_rustfs_for_local_kms().await?;
    tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;

    let s3_client = kms_env.base_env.create_s3_client();
    kms_env.base_env.create_test_bucket(TEST_BUCKET).await?;

    let object_key = "test-large-multipart-encrypted";
    let part_size = 6 * 1024 * 1024; // 6 MB per part (greater than the 1 MB encryption chunk)
    let total_parts = 3; // 18 MB total
    let total_size = part_size * total_parts;

    info!(
        "🗂️ Generated large-file test data: {} parts, {} MB each, {} MB total",
        total_parts,
        part_size / (1024 * 1024),
        total_size / (1024 * 1024)
    );

    // Generate large test data (complex pattern for validation)
    let test_data: Vec<u8> = (0..total_size)
        .map(|i| {
            let part_num = i / part_size;
            let offset_in_part = i % part_size;
            ((part_num * 100 + offset_in_part / 1000) % 256) as u8
        })
        .collect();

    info!("🔐 Starting large-file multipart upload (SSE-S3 encryption)");

    // Create multipart upload
    let create_multipart_output = s3_client
        .create_multipart_upload()
        .bucket(TEST_BUCKET)
        .key(object_key)
        .server_side_encryption(aws_sdk_s3::types::ServerSideEncryption::Aes256)
        .send()
        .await?;

    let upload_id = create_multipart_output.upload_id().unwrap();
    info!("📋 Created large encrypted multipart upload, ID: {}", upload_id);

    // Upload each part
    let mut completed_parts = Vec::new();
    for part_number in 1..=total_parts {
        let start = (part_number - 1) * part_size;
        let end = std::cmp::min(start + part_size, total_size);
        let part_data = &test_data[start..end];

        info!(
            "🔐 Uploading encrypted large-file part {} ({:.2} MB)",
            part_number,
            part_data.len() as f64 / (1024.0 * 1024.0)
        );

        let upload_part_output = s3_client
            .upload_part()
            .bucket(TEST_BUCKET)
            .key(object_key)
            .upload_id(upload_id)
            .part_number(part_number as i32)
            .body(aws_sdk_s3::primitives::ByteStream::from(part_data.to_vec()))
            .send()
            .await?;

        let etag = upload_part_output.e_tag().unwrap().to_string();
        completed_parts.push(
            aws_sdk_s3::types::CompletedPart::builder()
                .part_number(part_number as i32)
                .e_tag(&etag)
                .build(),
        );

        debug!("Large encrypted part {} uploaded with ETag {}", part_number, etag);
    }

    // Complete the multipart upload
    let completed_multipart_upload = aws_sdk_s3::types::CompletedMultipartUpload::builder()
        .set_parts(Some(completed_parts))
        .build();

    info!("🔗 Completing large encrypted multipart upload");
    let complete_output = s3_client
        .complete_multipart_upload()
        .bucket(TEST_BUCKET)
        .key(object_key)
        .upload_id(upload_id)
        .multipart_upload(completed_multipart_upload)
        .send()
        .await?;

    debug!("Large encrypted multipart upload completed with ETag {:?}", complete_output.e_tag());

    // Download and verify
    info!("📥 Downloading large object for verification");
    let get_response = s3_client.get_object().bucket(TEST_BUCKET).key(object_key).send().await?;

    // Verify encryption headers
    assert_eq!(
        get_response.server_side_encryption(),
        Some(&aws_sdk_s3::types::ServerSideEncryption::Aes256)
    );

    // Verify data integrity
    let downloaded_data = get_response.body.collect().await?.into_bytes();
    assert_eq!(downloaded_data.len(), total_size);

    // Validate bytes individually (stricter for large files)
    for (i, (&actual, &expected)) in downloaded_data.iter().zip(test_data.iter()).enumerate() {
        if actual != expected {
            panic!("Large file mismatch at byte {i}: actual={actual}, expected={expected}");
        }
    }

    kms_env.base_env.delete_test_bucket(TEST_BUCKET).await?;
    info!("✅ Step 4 passed: large-file multipart encryption succeeded");
    Ok(())
}

/// Step 5: test multipart uploads for every encryption mode
#[tokio::test]
#[serial]
async fn test_step5_all_encryption_types_multipart() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();
    info!("🧪 Step 5: test multipart uploads for every encryption mode");

    let mut kms_env = LocalKMSTestEnvironment::new().await?;
    let _default_key_id = kms_env.start_rustfs_for_local_kms().await?;
    tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;

    let s3_client = kms_env.base_env.create_s3_client();
    kms_env.base_env.create_test_bucket(TEST_BUCKET).await?;

    let part_size = 5 * 1024 * 1024; // 5MB per part
    let total_parts = 2;
    let total_size = part_size * total_parts;

    // Test SSE-KMS
    info!("🔐 Testing SSE-KMS multipart upload");
    test_multipart_encryption_type(
        &s3_client,
        TEST_BUCKET,
        "test-multipart-sse-kms",
        total_size,
        part_size,
        total_parts,
        EncryptionType::SSEKMS,
    )
    .await?;

    // Test SSE-C
    info!("🔐 Testing SSE-C multipart upload");
    test_multipart_encryption_type(
        &s3_client,
        TEST_BUCKET,
        "test-multipart-sse-c",
        total_size,
        part_size,
        total_parts,
        EncryptionType::SSEC,
    )
    .await?;

    kms_env.base_env.delete_test_bucket(TEST_BUCKET).await?;
    info!("✅ Step 5 passed: multipart uploads succeeded for every encryption mode");
    Ok(())
}

#[derive(Debug)]
enum EncryptionType {
    SSEKMS,
    SSEC,
}

/// Helper: test multipart uploads for a specific encryption type
async fn test_multipart_encryption_type(
    s3_client: &aws_sdk_s3::Client,
    bucket: &str,
    object_key: &str,
    total_size: usize,
    part_size: usize,
    total_parts: usize,
    encryption_type: EncryptionType,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Generate test data
    let test_data: Vec<u8> = (0..total_size).map(|i| ((i * 7) % 256) as u8).collect();

    // Prepare SSE-C keys when required
    let (sse_c_key, sse_c_md5) = if matches!(encryption_type, EncryptionType::SSEC) {
        let key = "01234567890123456789012345678901";
        let key_b64 = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, key);
        let key_md5 = format!("{:x}", md5::compute(key));
        (Some(key_b64), Some(key_md5))
    } else {
        (None, None)
    };

    info!("📋 Creating multipart upload - {:?}", encryption_type);

    // Create multipart upload
    let mut create_request = s3_client.create_multipart_upload().bucket(bucket).key(object_key);

    create_request = match encryption_type {
        EncryptionType::SSEKMS => create_request.server_side_encryption(aws_sdk_s3::types::ServerSideEncryption::AwsKms),
        EncryptionType::SSEC => create_request
            .sse_customer_algorithm("AES256")
            .sse_customer_key(sse_c_key.as_ref().unwrap())
            .sse_customer_key_md5(sse_c_md5.as_ref().unwrap()),
    };

    let create_multipart_output = create_request.send().await?;
    let upload_id = create_multipart_output.upload_id().unwrap();

    // Upload parts
    let mut completed_parts = Vec::new();
    for part_number in 1..=total_parts {
        let start = (part_number - 1) * part_size;
        let end = std::cmp::min(start + part_size, total_size);
        let part_data = &test_data[start..end];

        let mut upload_request = s3_client
            .upload_part()
            .bucket(bucket)
            .key(object_key)
            .upload_id(upload_id)
            .part_number(part_number as i32)
            .body(aws_sdk_s3::primitives::ByteStream::from(part_data.to_vec()));

        // SSE-C requires the key on each UploadPart request
        if matches!(encryption_type, EncryptionType::SSEC) {
            upload_request = upload_request
                .sse_customer_algorithm("AES256")
                .sse_customer_key(sse_c_key.as_ref().unwrap())
                .sse_customer_key_md5(sse_c_md5.as_ref().unwrap());
        }

        let upload_part_output = upload_request.send().await?;
        let etag = upload_part_output.e_tag().unwrap().to_string();
        completed_parts.push(
            aws_sdk_s3::types::CompletedPart::builder()
                .part_number(part_number as i32)
                .e_tag(&etag)
                .build(),
        );

        debug!("{:?} part {} uploaded", encryption_type, part_number);
    }

    // Complete the multipart upload
    let completed_multipart_upload = aws_sdk_s3::types::CompletedMultipartUpload::builder()
        .set_parts(Some(completed_parts))
        .build();

    let _complete_output = s3_client
        .complete_multipart_upload()
        .bucket(bucket)
        .key(object_key)
        .upload_id(upload_id)
        .multipart_upload(completed_multipart_upload)
        .send()
        .await?;

    // Download and verify
    let mut get_request = s3_client.get_object().bucket(bucket).key(object_key);

    // SSE-C requires the key on GET requests
    if matches!(encryption_type, EncryptionType::SSEC) {
        get_request = get_request
            .sse_customer_algorithm("AES256")
            .sse_customer_key(sse_c_key.as_ref().unwrap())
            .sse_customer_key_md5(sse_c_md5.as_ref().unwrap());
    }

    let get_response = get_request.send().await?;

    // Verify encryption headers
    match encryption_type {
        EncryptionType::SSEKMS => {
            assert_eq!(
                get_response.server_side_encryption(),
                Some(&aws_sdk_s3::types::ServerSideEncryption::AwsKms)
            );
        }
        EncryptionType::SSEC => {
            assert_eq!(get_response.sse_customer_algorithm(), Some("AES256"));
        }
    }

    // Verify data integrity
    let downloaded_data = get_response.body.collect().await?.into_bytes();
    assert_eq!(downloaded_data.len(), total_size);
    assert_eq!(&downloaded_data[..], &test_data[..]);

    info!("✅ {:?} multipart upload test passed", encryption_type);
    Ok(())
}
