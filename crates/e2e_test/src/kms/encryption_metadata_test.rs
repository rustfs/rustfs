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

//! Integration tests that focus on surface headers/metadata emitted by the
//! managed encryption pipeline (SSE-S3/SSE-KMS).

use super::common::LocalKMSTestEnvironment;
use crate::common::{TEST_BUCKET, init_logging};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    CompletedMultipartUpload, CompletedPart, ServerSideEncryption, ServerSideEncryptionByDefault,
    ServerSideEncryptionConfiguration, ServerSideEncryptionRule,
};
use serial_test::serial;
use std::collections::{HashMap, VecDeque};
use tracing::info;

fn assert_encryption_metadata(metadata: &HashMap<String, String>, expected_size: usize) {
    for key in [
        "x-rustfs-encryption-key",
        "x-rustfs-encryption-iv",
        "x-rustfs-encryption-context",
        "x-rustfs-encryption-original-size",
    ] {
        assert!(metadata.contains_key(key), "expected managed encryption metadata '{key}' to be present");
        assert!(
            !metadata.get(key).unwrap().is_empty(),
            "managed encryption metadata '{key}' should not be empty"
        );
    }

    let size_value = metadata
        .get("x-rustfs-encryption-original-size")
        .expect("managed encryption metadata should include original size");
    let parsed_size: usize = size_value
        .parse()
        .expect("x-rustfs-encryption-original-size should be numeric");
    assert_eq!(parsed_size, expected_size, "recorded original size should match uploaded payload length");
}

fn assert_storage_encrypted(storage_root: &std::path::Path, bucket: &str, key: &str, plaintext: &[u8]) {
    let mut stack = VecDeque::from([storage_root.to_path_buf()]);
    let mut scanned = 0;
    let mut plaintext_path: Option<std::path::PathBuf> = None;

    while let Some(current) = stack.pop_front() {
        let Ok(metadata) = std::fs::metadata(&current) else { continue };
        if metadata.is_dir() {
            if let Ok(entries) = std::fs::read_dir(&current) {
                for entry in entries.flatten() {
                    stack.push_back(entry.path());
                }
            }
            continue;
        }

        let path_str = current.to_string_lossy();
        if !(path_str.contains(bucket) || path_str.contains(key)) {
            continue;
        }

        scanned += 1;
        let Ok(bytes) = std::fs::read(&current) else { continue };
        if bytes.len() < plaintext.len() {
            continue;
        }
        if bytes.windows(plaintext.len()).any(|window| window == plaintext) {
            plaintext_path = Some(current);
            break;
        }
    }

    assert!(
        scanned > 0,
        "Failed to locate stored data files for bucket '{bucket}' and key '{key}' under {storage_root:?}"
    );
    assert!(plaintext_path.is_none(), "Plaintext detected on disk at {:?}", plaintext_path.unwrap());
}

#[tokio::test]
#[serial]
async fn test_head_reports_managed_metadata_for_sse_s3() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();
    info!("Validating SSE-S3 managed encryption metadata exposure");

    let mut kms_env = LocalKMSTestEnvironment::new().await?;
    let _default_key = kms_env.start_rustfs_for_local_kms().await?;
    tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;

    let s3_client = kms_env.base_env.create_s3_client();
    kms_env.base_env.create_test_bucket(TEST_BUCKET).await?;

    // Bucket level default SSE-S3 configuration.
    let encryption_config = ServerSideEncryptionConfiguration::builder()
        .rules(
            ServerSideEncryptionRule::builder()
                .apply_server_side_encryption_by_default(
                    ServerSideEncryptionByDefault::builder()
                        .sse_algorithm(ServerSideEncryption::Aes256)
                        .build()
                        .unwrap(),
                )
                .build(),
        )
        .build()
        .unwrap();

    s3_client
        .put_bucket_encryption()
        .bucket(TEST_BUCKET)
        .server_side_encryption_configuration(encryption_config)
        .send()
        .await?;

    let payload = b"metadata-sse-s3-payload";
    let key = "metadata-sse-s3-object";

    s3_client
        .put_object()
        .bucket(TEST_BUCKET)
        .key(key)
        .body(payload.to_vec().into())
        .send()
        .await?;

    let head = s3_client.head_object().bucket(TEST_BUCKET).key(key).send().await?;

    assert_eq!(
        head.server_side_encryption(),
        Some(&ServerSideEncryption::Aes256),
        "head_object should advertise SSE-S3"
    );

    let metadata = head
        .metadata()
        .expect("head_object should return managed encryption metadata");
    assert_encryption_metadata(metadata, payload.len());

    assert_storage_encrypted(std::path::Path::new(&kms_env.base_env.temp_dir), TEST_BUCKET, key, payload);

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_head_reports_managed_metadata_for_sse_kms_and_copy() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();
    info!("Validating SSE-KMS managed encryption metadata (including copy)");

    let mut kms_env = LocalKMSTestEnvironment::new().await?;
    let default_key_id = kms_env.start_rustfs_for_local_kms().await?;
    tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;

    let s3_client = kms_env.base_env.create_s3_client();
    kms_env.base_env.create_test_bucket(TEST_BUCKET).await?;

    let encryption_config = ServerSideEncryptionConfiguration::builder()
        .rules(
            ServerSideEncryptionRule::builder()
                .apply_server_side_encryption_by_default(
                    ServerSideEncryptionByDefault::builder()
                        .sse_algorithm(ServerSideEncryption::AwsKms)
                        .kms_master_key_id(&default_key_id)
                        .build()
                        .unwrap(),
                )
                .build(),
        )
        .build()
        .unwrap();

    s3_client
        .put_bucket_encryption()
        .bucket(TEST_BUCKET)
        .server_side_encryption_configuration(encryption_config)
        .send()
        .await?;

    let payload = b"metadata-sse-kms-payload";
    let source_key = "metadata-sse-kms-object";

    s3_client
        .put_object()
        .bucket(TEST_BUCKET)
        .key(source_key)
        .body(payload.to_vec().into())
        .send()
        .await?;

    let head_source = s3_client.head_object().bucket(TEST_BUCKET).key(source_key).send().await?;

    assert_eq!(
        head_source.server_side_encryption(),
        Some(&ServerSideEncryption::AwsKms),
        "source object should report SSE-KMS"
    );
    assert_eq!(
        head_source.ssekms_key_id().unwrap(),
        &default_key_id,
        "source object should maintain the configured KMS key id"
    );
    let source_metadata = head_source
        .metadata()
        .expect("source object should include managed encryption metadata");
    assert_encryption_metadata(source_metadata, payload.len());

    let dest_key = "metadata-sse-kms-object-copy";
    let copy_source = format!("{TEST_BUCKET}/{source_key}");

    s3_client
        .copy_object()
        .bucket(TEST_BUCKET)
        .key(dest_key)
        .copy_source(copy_source)
        .send()
        .await?;

    let head_dest = s3_client.head_object().bucket(TEST_BUCKET).key(dest_key).send().await?;

    assert_eq!(
        head_dest.server_side_encryption(),
        Some(&ServerSideEncryption::AwsKms),
        "copied object should remain encrypted with SSE-KMS"
    );
    assert_eq!(
        head_dest.ssekms_key_id().unwrap(),
        &default_key_id,
        "copied object should keep the default KMS key id"
    );
    let dest_metadata = head_dest
        .metadata()
        .expect("copied object should include managed encryption metadata");
    assert_encryption_metadata(dest_metadata, payload.len());

    let copied_body = s3_client
        .get_object()
        .bucket(TEST_BUCKET)
        .key(dest_key)
        .send()
        .await?
        .body
        .collect()
        .await?
        .into_bytes();
    assert_eq!(&copied_body[..], payload, "copied object payload should match source");

    let storage_root = std::path::Path::new(&kms_env.base_env.temp_dir);
    assert_storage_encrypted(storage_root, TEST_BUCKET, source_key, payload);
    assert_storage_encrypted(storage_root, TEST_BUCKET, dest_key, payload);

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_multipart_upload_writes_encrypted_data() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();
    info!("Validating ciphertext persistence for multipart SSE-KMS uploads");

    let mut kms_env = LocalKMSTestEnvironment::new().await?;
    let default_key_id = kms_env.start_rustfs_for_local_kms().await?;
    tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;

    let s3_client = kms_env.base_env.create_s3_client();
    kms_env.base_env.create_test_bucket(TEST_BUCKET).await?;

    let encryption_config = ServerSideEncryptionConfiguration::builder()
        .rules(
            ServerSideEncryptionRule::builder()
                .apply_server_side_encryption_by_default(
                    ServerSideEncryptionByDefault::builder()
                        .sse_algorithm(ServerSideEncryption::AwsKms)
                        .kms_master_key_id(&default_key_id)
                        .build()
                        .unwrap(),
                )
                .build(),
        )
        .build()
        .unwrap();

    s3_client
        .put_bucket_encryption()
        .bucket(TEST_BUCKET)
        .server_side_encryption_configuration(encryption_config)
        .send()
        .await?;

    let key = "multipart-encryption-object";
    let part_size = 5 * 1024 * 1024; // minimum part size required by S3 semantics
    let part_one = vec![0xA5; part_size];
    let part_two = vec![0x5A; part_size];
    let combined: Vec<u8> = part_one.iter().chain(part_two.iter()).copied().collect();

    let create_output = s3_client
        .create_multipart_upload()
        .bucket(TEST_BUCKET)
        .key(key)
        .send()
        .await?;

    let upload_id = create_output.upload_id().unwrap();

    let part1 = s3_client
        .upload_part()
        .bucket(TEST_BUCKET)
        .key(key)
        .upload_id(upload_id)
        .part_number(1)
        .body(ByteStream::from(part_one.clone()))
        .send()
        .await?;

    let part2 = s3_client
        .upload_part()
        .bucket(TEST_BUCKET)
        .key(key)
        .upload_id(upload_id)
        .part_number(2)
        .body(ByteStream::from(part_two.clone()))
        .send()
        .await?;

    let completed = CompletedMultipartUpload::builder()
        .parts(CompletedPart::builder().part_number(1).e_tag(part1.e_tag().unwrap()).build())
        .parts(CompletedPart::builder().part_number(2).e_tag(part2.e_tag().unwrap()).build())
        .build();

    s3_client
        .complete_multipart_upload()
        .bucket(TEST_BUCKET)
        .key(key)
        .upload_id(upload_id)
        .multipart_upload(completed)
        .send()
        .await?;

    let head = s3_client.head_object().bucket(TEST_BUCKET).key(key).send().await?;
    assert_eq!(
        head.server_side_encryption(),
        Some(&ServerSideEncryption::AwsKms),
        "multipart head_object should expose SSE-KMS"
    );
    assert_eq!(
        head.ssekms_key_id().unwrap(),
        &default_key_id,
        "multipart object should retain bucket default KMS key"
    );

    assert_encryption_metadata(
        head.metadata().expect("multipart head_object should expose managed metadata"),
        combined.len(),
    );

    // Data returned to clients should decrypt back to original payload
    let fetched = s3_client
        .get_object()
        .bucket(TEST_BUCKET)
        .key(key)
        .send()
        .await?
        .body
        .collect()
        .await?
        .into_bytes();
    assert_eq!(&fetched[..], &combined[..]);

    assert_storage_encrypted(std::path::Path::new(&kms_env.base_env.temp_dir), TEST_BUCKET, key, &combined);

    Ok(())
}
