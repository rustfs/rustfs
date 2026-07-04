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

//! Regression test for Issue #4238 (encryption case): restoring a historical version of an
//! SSE-S3 encrypted object onto the current key must produce a readable object.
//!
//! The version-restore path writes a NEW version. A metadata-only (zero-copy) version copy
//! would make the new version point at the source ciphertext while carrying freshly derived
//! encryption metadata, yielding an undecryptable object. The store layer therefore performs a
//! full read/write copy through `put_object` when the handler supplies a reader, keeping the
//! stored bytes consistent with the new version's key metadata.

use super::common::{LocalKMSTestEnvironment, create_key_with_specific_id};
use crate::common::init_logging;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{BucketVersioningStatus, ServerSideEncryption, VersioningConfiguration};
use serial_test::serial;
use tracing::info;

#[tokio::test]
#[serial]
async fn test_self_copy_of_historical_sse_s3_version_is_readable() {
    init_logging();
    info!("Issue #4238 (SSE): restoring an encrypted historical version must stay decryptable");

    let mut kms_env = LocalKMSTestEnvironment::new().await.expect("failed to create local KMS env");
    // Start the server with the local KMS backend. The dev-defaults flag is required for the
    // in-process local KMS master key used by e2e tests.
    let default_key_id = "rustfs-e2e-test-default-key";
    let keys_dir = kms_env.kms_keys_dir.clone();
    create_key_with_specific_id(&keys_dir, default_key_id)
        .await
        .expect("failed to create local KMS key");
    kms_env
        .base_env
        .start_rustfs_server_with_env(
            vec![
                "--kms-enable",
                "--kms-backend",
                "local",
                "--kms-key-dir",
                &keys_dir,
                "--kms-default-key-id",
                default_key_id,
            ],
            &[("RUSTFS_KMS_ALLOW_INSECURE_DEV_DEFAULTS", "true")],
        )
        .await
        .expect("failed to start RustFS with local KMS");
    tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;

    let client = kms_env.base_env.create_s3_client();
    let bucket = "copy-object-version-restore-sse-test";
    let key = "secrets/report.txt";

    client
        .create_bucket()
        .bucket(bucket)
        .send()
        .await
        .expect("failed to create bucket");
    client
        .put_bucket_versioning()
        .bucket(bucket)
        .versioning_configuration(
            VersioningConfiguration::builder()
                .status(BucketVersioningStatus::Enabled)
                .build(),
        )
        .send()
        .await
        .expect("failed to enable versioning");

    // Version 1: SSE-S3 encrypted content we want to restore later.
    let v1_content = b"encrypted original report -- version one";
    let put_v1 = client
        .put_object()
        .bucket(bucket)
        .key(key)
        .content_type("text/plain; charset=utf-8")
        .metadata("origin", "v1")
        .server_side_encryption(ServerSideEncryption::Aes256)
        .body(ByteStream::from_static(v1_content))
        .send()
        .await
        .expect("PUT v1 failed");
    assert_eq!(put_v1.server_side_encryption(), Some(&ServerSideEncryption::Aes256));
    let v1_id = put_v1.version_id().expect("v1 must have a version id").to_string();

    // Version 2: becomes the current version.
    let v2_content = b"encrypted updated report -- version two (current)";
    let put_v2 = client
        .put_object()
        .bucket(bucket)
        .key(key)
        .content_type("application/octet-stream")
        .metadata("origin", "v2")
        .server_side_encryption(ServerSideEncryption::Aes256)
        .body(ByteStream::from_static(v2_content))
        .send()
        .await
        .expect("PUT v2 failed");
    let v2_id = put_v2.version_id().expect("v2 must have a version id").to_string();
    assert_ne!(v1_id, v2_id, "the two puts must produce distinct versions");

    // Restore v1 by copying it onto the current key with the DEFAULT (COPY) directive, keeping
    // the destination encrypted. Under the old zero-copy version-restore path this new version
    // would be undecryptable.
    let copy_out = client
        .copy_object()
        .bucket(bucket)
        .key(key)
        .copy_source(format!("{bucket}/{key}?versionId={v1_id}"))
        .server_side_encryption(ServerSideEncryption::Aes256)
        .send()
        .await
        .expect("CopyObject restoring an encrypted historical version must succeed (issue #4238)");
    let restored_id = copy_out
        .version_id()
        .expect("restore copy must create a new version id")
        .to_string();
    assert_ne!(restored_id, v1_id, "restore must create a NEW version, not mutate v1");
    assert_ne!(restored_id, v2_id, "restore must create a NEW version, not mutate v2");

    // The current object must now decrypt to v1's plaintext (proves stored ciphertext matches
    // the new version's key metadata) and preserve Content-Type and user metadata.
    let get = client
        .get_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .expect("GET after restore failed");
    assert_eq!(get.server_side_encryption(), Some(&ServerSideEncryption::Aes256));
    assert_eq!(
        get.content_type(),
        Some("text/plain; charset=utf-8"),
        "Content-Type must be preserved from v1"
    );
    assert_eq!(
        get.metadata().and_then(|m| m.get("origin")),
        Some(&"v1".to_string()),
        "user metadata must be preserved from v1"
    );
    let body = get.body.collect().await.expect("collect body").into_bytes();
    assert_eq!(body.as_ref(), v1_content, "restored current object must decrypt to v1 content");

    // Original versions must remain intact and independently decryptable.
    let get_v1 = client
        .get_object()
        .bucket(bucket)
        .key(key)
        .version_id(&v1_id)
        .send()
        .await
        .expect("GET v1 failed");
    let v1_body = get_v1.body.collect().await.expect("collect v1 body").into_bytes();
    assert_eq!(v1_body.as_ref(), v1_content, "v1 must remain intact after restore");

    let get_v2 = client
        .get_object()
        .bucket(bucket)
        .key(key)
        .version_id(&v2_id)
        .send()
        .await
        .expect("GET v2 failed");
    let v2_body = get_v2.body.collect().await.expect("collect v2 body").into_bytes();
    assert_eq!(v2_body.as_ref(), v2_content, "v2 must remain intact after restore");

    kms_env.base_env.stop_server();
}
