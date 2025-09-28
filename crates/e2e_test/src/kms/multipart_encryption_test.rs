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

use super::common::LocalKMSTestEnvironment;
use crate::common::{TEST_BUCKET, init_logging};
use serial_test::serial;
use tracing::{debug, info};

#[tokio::test]
#[serial]
async fn test_step1_basic_single_file_encryption() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();
    info!("🧪 step1: test basic single file encryption");

    let mut kms_env = LocalKMSTestEnvironment::new().await?;
    let _default_key_id = kms_env.start_rustfs_for_local_kms().await?;
    tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;

    let s3_client = kms_env.base_env.create_s3_client();
    kms_env.base_env.create_test_bucket(TEST_BUCKET).await?;

    // test small file encryption (should inline store)
    let test_data = b"Hello, this is a small test file for SSE-S3!";
    let object_key = "test-single-file-encrypted";

    info!("📤 step1: upload small file ({}) with SSE-S3 encryption", test_data.len());
    let put_response = s3_client
        .put_object()
        .bucket(TEST_BUCKET)
        .key(object_key)
        .body(aws_sdk_s3::primitives::ByteStream::from(test_data.to_vec()))
        .server_side_encryption(aws_sdk_s3::types::ServerSideEncryption::Aes256)
        .send()
        .await?;

    debug!("PUT response ETag: {:?}", put_response.e_tag());
    debug!("PUT response SSE: {:?}", put_response.server_side_encryption());

    // verify PUT response contains correct encryption header
    assert_eq!(
        put_response.server_side_encryption(),
        Some(&aws_sdk_s3::types::ServerSideEncryption::Aes256)
    );

    info!("📥 step1: download file and verify encryption status");
    let get_response = s3_client.get_object().bucket(TEST_BUCKET).key(object_key).send().await?;

    debug!("GET response SSE: {:?}", get_response.server_side_encryption());

    // verify GET response contains correct encryption header
    assert_eq!(
        get_response.server_side_encryption(),
        Some(&aws_sdk_s3::types::ServerSideEncryption::Aes256)
    );

    // verify data integrity
    let downloaded_data = get_response.body.collect().await?.into_bytes();
    assert_eq!(&downloaded_data[..], test_data);

    kms_env.base_env.delete_test_bucket(TEST_BUCKET).await?;
    info!("✅ step1: basic single file encryption works as expected");
    Ok(())
}

/// test basic multipart upload without encryption
#[tokio::test]
#[serial]
async fn test_step2_basic_multipart_upload_without_encryption() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();
    info!("🧪 step2: test basic multipart upload without encryption");

    let mut kms_env = LocalKMSTestEnvironment::new().await?;
    let _default_key_id = kms_env.start_rustfs_for_local_kms().await?;
    tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;

    let s3_client = kms_env.base_env.create_s3_client();
    kms_env.base_env.create_test_bucket(TEST_BUCKET).await?;

    let object_key = "test-multipart-no-encryption";
    let part_size = 5 * 1024 * 1024; // 5MB per part (S3 minimum)
    let total_parts = 2;
    let total_size = part_size * total_parts;

    // generate test data (with clear pattern for easy verification)
    let test_data: Vec<u8> = (0..total_size).map(|i| (i % 256) as u8).collect();

    info!(
        "🚀 step2: start multipart upload (no encryption) with {} parts, each {}MB",
        total_parts,
        part_size / (1024 * 1024)
    );

    // step1: create multipart upload
    let create_multipart_output = s3_client
        .create_multipart_upload()
        .bucket(TEST_BUCKET)
        .key(object_key)
        .send()
        .await?;

    let upload_id = create_multipart_output.upload_id().unwrap();
    info!("📋 step2: create multipart upload, ID: {}", upload_id);

    // step2: upload each part
    let mut completed_parts = Vec::new();
    for part_number in 1..=total_parts {
        let start = (part_number - 1) * part_size;
        let end = std::cmp::min(start + part_size, total_size);
        let part_data = &test_data[start..end];

        info!("📤 step2: upload part {} ({} bytes)", part_number, part_data.len());

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

        debug!("step2: part {} uploaded, ETag: {}", part_number, etag);
    }

    // step3: complete multipart upload
    let completed_multipart_upload = aws_sdk_s3::types::CompletedMultipartUpload::builder()
        .set_parts(Some(completed_parts))
        .build();

    info!("🔗 step2: complete multipart upload");
    let complete_output = s3_client
        .complete_multipart_upload()
        .bucket(TEST_BUCKET)
        .key(object_key)
        .upload_id(upload_id)
        .multipart_upload(completed_multipart_upload)
        .send()
        .await?;

    debug!("step2: multipart upload completed, ETag: {:?}", complete_output.e_tag());

    // step4: verify multipart upload completed successfully
    assert_eq!(
        complete_output.e_tag().unwrap().to_string(),
        format!("\"{}-{}-{}\"", object_key, upload_id, total_parts)
    );

    // verify data integrity
    info!("📥 step2: download file and verify data integrity");
    let get_response = s3_client.get_object().bucket(TEST_BUCKET).key(object_key).send().await?;

    let downloaded_data = get_response.body.collect().await?.into_bytes();
    assert_eq!(downloaded_data.len(), total_size);
    assert_eq!(&downloaded_data[..], &test_data[..]);

    kms_env.base_env.delete_test_bucket(TEST_BUCKET).await?;
    info!("✅ step2: basic multipart upload without encryption works as expected");
    Ok(())
}

/// test multipart upload with SSE-S3 encryption
#[tokio::test]
#[serial]
async fn test_step3_multipart_upload_with_sse_s3() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();
    info!("🧪 step3: test multipart upload with SSE-S3 encryption");

    let mut kms_env = LocalKMSTestEnvironment::new().await?;
    let _default_key_id = kms_env.start_rustfs_for_local_kms().await?;
    tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;

    let s3_client = kms_env.base_env.create_s3_client();
    kms_env.base_env.create_test_bucket(TEST_BUCKET).await?;

    let object_key = "test-multipart-sse-s3";
    let part_size = 5 * 1024 * 1024; // 5MB per part
    let total_parts = 2;
    let total_size = part_size * total_parts;

    // generate test data (with clear pattern for easy verification)
    let test_data: Vec<u8> = (0..total_size).map(|i| ((i / 1000) % 256) as u8).collect();

    info!(
        "🔐 step3: start multipart upload with SSE-S3 encryption: {} parts, each {}MB",
        total_parts,
        part_size / (1024 * 1024)
    );

    // step1: create multipart upload and enable SSE-S3
    let create_multipart_output = s3_client
        .create_multipart_upload()
        .bucket(TEST_BUCKET)
        .key(object_key)
        .server_side_encryption(aws_sdk_s3::types::ServerSideEncryption::Aes256)
        .send()
        .await?;

    let upload_id = create_multipart_output.upload_id().unwrap();
    info!("📋 step3: create multipart upload with SSE-S3 encryption, ID: {}", upload_id);

    // step2: verify CreateMultipartUpload response (SSE-S3 header should be included)
    if let Some(sse) = create_multipart_output.server_side_encryption() {
        debug!("CreateMultipartUpload response contains SSE header: {:?}", sse);
        assert_eq!(sse, &aws_sdk_s3::types::ServerSideEncryption::Aes256);
    } else {
        debug!("CreateMultipartUpload response does not contain SSE header (some implementations may return empty string)");
    }

    // step2: upload each part
    let mut completed_parts = Vec::new();
    for part_number in 1..=total_parts {
        let start = (part_number - 1) * part_size;
        let end = std::cmp::min(start + part_size, total_size);
        let part_data = &test_data[start..end];

        info!("🔐 step3: upload encrypted part {} ({} bytes)", part_number, part_data.len());

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

        debug!("step3: part {} uploaded, ETag: {}", part_number, etag);
    }

    // step3: complete multipart upload
    let completed_multipart_upload = aws_sdk_s3::types::CompletedMultipartUpload::builder()
        .set_parts(Some(completed_parts))
        .build();

    info!("🔗 step3: complete multipart upload with SSE-S3 encryption");
    let complete_output = s3_client
        .complete_multipart_upload()
        .bucket(TEST_BUCKET)
        .key(object_key)
        .upload_id(upload_id)
        .multipart_upload(completed_multipart_upload)
        .send()
        .await?;

    debug!(
        "step3: complete multipart upload with SSE-S3 encryption, ETag: {:?}",
        complete_output.e_tag()
    );

    // step4: HEAD request to check metadata
    info!("📋 step4: check object metadata");
    let head_response = s3_client.head_object().bucket(TEST_BUCKET).key(object_key).send().await?;

    debug!("HEAD response SSE: {:?}", head_response.server_side_encryption());
    debug!("HEAD response metadata: {:?}", head_response.metadata());

    // step5: GET request to download and verify
    info!("📥 step5: download encrypted file and verify");
    let get_response = s3_client.get_object().bucket(TEST_BUCKET).key(object_key).send().await?;

    debug!("GET response SSE: {:?}", get_response.server_side_encryption());

    // step5: verify GET response contains SSE-S3 encryption header
    assert_eq!(
        get_response.server_side_encryption(),
        Some(&aws_sdk_s3::types::ServerSideEncryption::Aes256)
    );

    // step5: verify downloaded data matches original test data
    let downloaded_data = get_response.body.collect().await?.into_bytes();
    assert_eq!(downloaded_data.len(), total_size);
    assert_eq!(&downloaded_data[..], &test_data[..]);

    kms_env.base_env.delete_test_bucket(TEST_BUCKET).await?;
    info!("✅ step3: multipart upload with SSE-S3 encryption function is normal");
    Ok(())
}

/// step4: test larger multipart upload with encryption (streaming encryption)
#[tokio::test]
#[serial]
async fn test_step4_large_multipart_upload_with_encryption() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();
    info!("🧪 step4: test larger multipart upload with encryption (streaming encryption)");

    let mut kms_env = LocalKMSTestEnvironment::new().await?;
    let _default_key_id = kms_env.start_rustfs_for_local_kms().await?;
    tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;

    let s3_client = kms_env.base_env.create_s3_client();
    kms_env.base_env.create_test_bucket(TEST_BUCKET).await?;

    let object_key = "test-large-multipart-encrypted";
    let part_size = 6 * 1024 * 1024; // 6MB per part (大于1MB加密块大小)
    let total_parts = 3; // 总共18MB
    let total_size = part_size * total_parts;

    info!(
        "🗂️ step4: generate large test data: {} parts, each {}MB, total {}MB",
        total_parts,
        part_size / (1024 * 1024),
        total_size / (1024 * 1024)
    );

    // step4: generate large test data (using complex pattern for verification)
    let test_data: Vec<u8> = (0..total_size)
        .map(|i| {
            let part_num = i / part_size;
            let offset_in_part = i % part_size;
            ((part_num * 100 + offset_in_part / 1000) % 256) as u8
        })
        .collect();

    info!("🔐 step4: start large multipart upload with encryption (SSE-S3)");

    // step4: create multipart upload
    let create_multipart_output = s3_client
        .create_multipart_upload()
        .bucket(TEST_BUCKET)
        .key(object_key)
        .server_side_encryption(aws_sdk_s3::types::ServerSideEncryption::Aes256)
        .send()
        .await?;

    let upload_id = create_multipart_output.upload_id().unwrap();
    info!("📋 step4: create multipart upload with encryption (SSE-S3), ID: {}", upload_id);

    // step4: upload parts
    let mut completed_parts = Vec::new();
    for part_number in 1..=total_parts {
        let start = (part_number - 1) * part_size;
        let end = std::cmp::min(start + part_size, total_size);
        let part_data = &test_data[start..end];

        info!(
            "🔐 step4: upload part {} ({:.2}MB)",
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

        debug!("step4: upload part {} completed, ETag: {}", part_number, etag);
    }

    // step4: complete multipart upload
    let completed_multipart_upload = aws_sdk_s3::types::CompletedMultipartUpload::builder()
        .set_parts(Some(completed_parts))
        .build();

    info!("🔗 step4: complete multipart upload with encryption (SSE-S3)");
    let complete_output = s3_client
        .complete_multipart_upload()
        .bucket(TEST_BUCKET)
        .key(object_key)
        .upload_id(upload_id)
        .multipart_upload(completed_multipart_upload)
        .send()
        .await?;

    debug!(
        "step4: complete multipart upload with encryption (SSE-S3), ETag: {:?}",
        complete_output.e_tag()
    );

    // step4: download and verify
    info!("📥 step4: download and verify large multipart upload with encryption (SSE-S3)");
    let get_response = s3_client.get_object().bucket(TEST_BUCKET).key(object_key).send().await?;

    // step4: verify encryption header
    assert_eq!(
        get_response.server_side_encryption(),
        Some(&aws_sdk_s3::types::ServerSideEncryption::Aes256)
    );

    // step4: verify data integrity
    let downloaded_data = get_response.body.collect().await?.into_bytes();
    assert_eq!(downloaded_data.len(), total_size);

    // step4: verify data matches original test data
    for (i, (&actual, &expected)) in downloaded_data.iter().zip(test_data.iter()).enumerate() {
        if actual != expected {
            panic!(
                "step4: large multipart upload with encryption (SSE-S3) data mismatch at byte {}: actual={}, expected={}",
                i, actual, expected
            );
        }
    }

    kms_env.base_env.delete_test_bucket(TEST_BUCKET).await?;
    info!("✅ step4: large multipart upload with encryption (SSE-S3) functionality normal");
    Ok(())
}

/// step5: test all encryption types multipart upload
#[tokio::test]
#[serial]
async fn test_step5_all_encryption_types_multipart() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();
    info!("🧪 step5: test all encryption types multipart upload");

    let mut kms_env = LocalKMSTestEnvironment::new().await?;
    let _default_key_id = kms_env.start_rustfs_for_local_kms().await?;
    tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;

    let s3_client = kms_env.base_env.create_s3_client();
    kms_env.base_env.create_test_bucket(TEST_BUCKET).await?;

    let part_size = 5 * 1024 * 1024; // 5MB per part
    let total_parts = 2;
    let total_size = part_size * total_parts;

    // step5: test SSE-KMS multipart upload
    info!("🔐 step5: test SSE-KMS multipart upload");
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

    // step5: test SSE-C multipart upload
    info!("🔐 step5: test SSE-C multipart upload");
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
    info!("✅ step5: all encryption types multipart upload functionality normal");
    Ok(())
}

#[derive(Debug)]
enum EncryptionType {
    SSEKMS,
    SSEC,
}

/// step5: test specific encryption type multipart upload
async fn test_multipart_encryption_type(
    s3_client: &aws_sdk_s3::Client,
    bucket: &str,
    object_key: &str,
    total_size: usize,
    part_size: usize,
    total_parts: usize,
    encryption_type: EncryptionType,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // step5: generate test data
    let test_data: Vec<u8> = (0..total_size).map(|i| ((i * 7) % 256) as u8).collect();

    // step5: prepare SSE-C key and MD5 (if needed)
    let (sse_c_key, sse_c_md5) = if matches!(encryption_type, EncryptionType::SSEC) {
        let key = "01234567890123456789012345678901";
        let key_b64 = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, key);
        let key_md5 = format!("{:x}", md5::compute(key));
        (Some(key_b64), Some(key_md5))
    } else {
        (None, None)
    };

    // step5: create multipart upload
    info!("🔗 step5: create multipart upload with encryption {:?}", encryption_type);

    // step5: create multipart upload request
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

        // step5: include SSE-C key and MD5 in each UploadPart request (if needed)
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

        // step5: complete multipart upload request
        debug!("🔗 step5: complete multipart upload part {} with etag {}", part_number, etag);
    }

    // step5: complete multipart upload
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

    // step5: download and verify multipart upload
    info!("🔗 step5: download and verify multipart upload with encryption {:?}", encryption_type);

    let mut get_request = s3_client.get_object().bucket(bucket).key(object_key);

    // step5: include SSE-C key and MD5 in each GET request (if needed)
    if matches!(encryption_type, EncryptionType::SSEC) {
        get_request = get_request
            .sse_customer_algorithm("AES256")
            .sse_customer_key(sse_c_key.as_ref().unwrap())
            .sse_customer_key_md5(sse_c_md5.as_ref().unwrap());
    }

    let get_response = get_request.send().await?;

    // step5: verify encryption headers
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

    // step5: verify data integrity
    let downloaded_data = get_response.body.collect().await?.into_bytes();
    assert_eq!(downloaded_data.len(), total_size);
    assert_eq!(&downloaded_data[..], &test_data[..]);

    // step5: verify data integrity
    info!(
        "✅ step5: verify data integrity for multipart upload with encryption {:?}",
        encryption_type
    );
    Ok(())
}
