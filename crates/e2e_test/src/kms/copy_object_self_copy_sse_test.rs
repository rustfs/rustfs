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

//! Regression test: a same-key CopyObject that only rewrites metadata must never re-key a
//! managed-SSE (SSE-S3 / SSE-KMS) object.
//!
//! On an **unversioned** bucket the handler marks a same-name copy `metadata_only`, and the
//! store layer then updates `xl.meta` in place without touching the data blocks. The handler
//! nevertheless strips the source encryption metadata and generates a *fresh* DEK for the
//! destination. Combining the two writes "new DEK + old ciphertext": the object is permanently
//! undecryptable. The fix forces a full data rewrite whenever the copy re-derives managed
//! encryption material, so the stored bytes always match the key metadata beside them.
//!
//! Companion to `copy_object_version_restore_sse_test` (issue #4238), which pins the same
//! invariant for the versioned historical-restore path.

use super::common::{LocalKMSTestEnvironment, create_key_with_specific_id};
use crate::common::init_logging;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    MetadataDirective, ServerSideEncryption, ServerSideEncryptionByDefault, ServerSideEncryptionConfiguration,
    ServerSideEncryptionRule,
};
use tracing::info;

#[tokio::test]
async fn test_metadata_replace_self_copy_of_sse_object_stays_decryptable() {
    init_logging();
    info!("same-key CopyObject with REPLACE metadata must not re-key an SSE-S3 object");

    let mut kms_env = LocalKMSTestEnvironment::new().await.expect("failed to create local KMS env");
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
    kms_env.wait_for_kms_ready().await.expect("KMS ready");

    let client = kms_env.base_env.create_s3_client();
    // Deliberately an UNVERSIONED bucket: that is the branch where the store layer can service
    // the self-copy as a pure metadata update.
    let bucket = "copy-object-self-copy-sse-test";
    let key = "secrets/report.txt";

    client
        .create_bucket()
        .bucket(bucket)
        .send()
        .await
        .expect("failed to create bucket");

    // Content long enough that a truncated/garbled decrypt cannot coincidentally match.
    let content = b"encrypted payload that must survive a metadata-only self copy -- 0123456789";
    let put = client
        .put_object()
        .bucket(bucket)
        .key(key)
        .content_type("text/plain; charset=utf-8")
        .metadata("stage", "before")
        .server_side_encryption(ServerSideEncryption::Aes256)
        .body(ByteStream::from_static(content))
        .send()
        .await
        .expect("PUT failed");
    assert_eq!(put.server_side_encryption(), Some(&ServerSideEncryption::Aes256));

    // Copy the object onto itself, replacing user metadata. This is the `mc cp --attr` /
    // "edit metadata in place" shape that AWS supports on an existing object.
    let copy_out = client
        .copy_object()
        .bucket(bucket)
        .key(key)
        .copy_source(format!("{bucket}/{key}"))
        .metadata_directive(MetadataDirective::Replace)
        .content_type("text/plain; charset=utf-8")
        .metadata("stage", "after")
        .server_side_encryption(ServerSideEncryption::Aes256)
        .send()
        .await
        .expect("same-key CopyObject with REPLACE metadata must succeed");
    assert_eq!(copy_out.server_side_encryption(), Some(&ServerSideEncryption::Aes256));

    // The object must still decrypt to the original plaintext. Before the fix the stored
    // ciphertext was left untouched while the metadata carried a brand-new DEK, so this GET
    // either failed outright or returned garbage.
    let get = client
        .get_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .expect("GET after self-copy failed: the object was re-keyed without rewriting the ciphertext");
    assert_eq!(get.server_side_encryption(), Some(&ServerSideEncryption::Aes256));
    assert_eq!(
        get.metadata().and_then(|m| m.get("stage")),
        Some(&"after".to_string()),
        "REPLACE metadata must take effect"
    );
    let body = get.body.collect().await.expect("collect body").into_bytes();
    assert_eq!(
        body.as_ref(),
        content,
        "object must still decrypt to the original plaintext after a metadata-only self copy"
    );

    kms_env.base_env.stop_server();
}

#[tokio::test]
async fn test_metadata_replace_self_copy_dropping_sse_rewrites_plaintext() {
    init_logging();
    info!("same-key CopyObject that drops SSE must rewrite the data, not orphan the ciphertext");

    let mut kms_env = LocalKMSTestEnvironment::new().await.expect("failed to create local KMS env");
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
    kms_env.wait_for_kms_ready().await.expect("KMS ready");

    let client = kms_env.base_env.create_s3_client();
    // Unversioned, and deliberately WITHOUT a bucket default-encryption rule, so the copy below
    // resolves to "no destination encryption".
    let bucket = "copy-object-self-copy-drop-sse-test";
    let key = "secrets/report.txt";

    client
        .create_bucket()
        .bucket(bucket)
        .send()
        .await
        .expect("failed to create bucket");

    let content = b"encrypted payload whose ciphertext must not survive as bogus plaintext -- 0123456789";
    client
        .put_object()
        .bucket(bucket)
        .key(key)
        .metadata("stage", "before")
        .server_side_encryption(ServerSideEncryption::Aes256)
        .body(ByteStream::from_static(content))
        .send()
        .await
        .expect("PUT failed");

    // Self-copy with REPLACE and no SSE header. Per AWS semantics the destination ends up
    // unencrypted. The dangerous outcome is the silent one: the handler strips the source key
    // metadata while a metadata-only copy leaves the ciphertext in place, so a later GET would
    // hand back raw ciphertext as if it were plaintext — corruption with no error anywhere.
    client
        .copy_object()
        .bucket(bucket)
        .key(key)
        .copy_source(format!("{bucket}/{key}"))
        .metadata_directive(MetadataDirective::Replace)
        .metadata("stage", "after")
        .send()
        .await
        .expect("same-key CopyObject dropping SSE must succeed");

    let get = client
        .get_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .expect("GET after self-copy failed");
    assert_eq!(
        get.server_side_encryption(),
        None,
        "destination must be unencrypted once the copy drops SSE"
    );
    assert_eq!(
        get.metadata().and_then(|m| m.get("stage")),
        Some(&"after".to_string()),
        "REPLACE metadata must take effect"
    );
    let body = get.body.collect().await.expect("collect body").into_bytes();
    assert_eq!(
        body.as_ref(),
        content,
        "object must read back as the original plaintext, not the orphaned ciphertext"
    );

    kms_env.base_env.stop_server();
}

#[tokio::test]
async fn test_metadata_replace_self_copy_under_bucket_default_sse_stays_decryptable() {
    init_logging();
    info!("bucket default encryption must also keep a same-key copy off the metadata-only path");

    let mut kms_env = LocalKMSTestEnvironment::new().await.expect("failed to create local KMS env");
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
    kms_env.wait_for_kms_ready().await.expect("KMS ready");

    let client = kms_env.base_env.create_s3_client();
    let bucket = "copy-object-self-copy-bucket-default-sse-test";
    let key = "secrets/report.txt";

    client
        .create_bucket()
        .bucket(bucket)
        .send()
        .await
        .expect("failed to create bucket");

    // Store the object as PLAINTEXT first: no SSE header and no bucket default rule yet. This is
    // what makes the case sharp — at copy time the source metadata carries no encryption markers,
    // so the source-side half of the guard cannot fire.
    let content = b"plaintext payload that must not be orphaned under a new DEK -- 0123456789";
    let put = client
        .put_object()
        .bucket(bucket)
        .key(key)
        .metadata("stage", "before")
        .body(ByteStream::from_static(content))
        .send()
        .await
        .expect("PUT failed");
    assert_eq!(put.server_side_encryption(), None, "the object must start out unencrypted");

    // Only NOW enable bucket default encryption. The destination's encryption therefore comes
    // from the bucket rule and from nowhere else: the source is unencrypted and the copy request
    // carries no SSE header. A guard that only inspects request headers (MinIO decides
    // `isTargetEncrypted` from `crypto.S3.IsRequested(r.Header)`) would let this through, yet
    // `sse_encryption` still mints a fresh DEK from the resolved bucket default — which is why
    // the guard keys off the *effective* encryption rather than the requested one.
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
    client
        .put_bucket_encryption()
        .bucket(bucket)
        .server_side_encryption_configuration(encryption_config)
        .send()
        .await
        .expect("failed to set bucket default encryption");

    // No SSE header on the copy — the bucket default alone drives the destination encryption.
    client
        .copy_object()
        .bucket(bucket)
        .key(key)
        .copy_source(format!("{bucket}/{key}"))
        .metadata_directive(MetadataDirective::Replace)
        .metadata("stage", "after")
        .send()
        .await
        .expect("same-key CopyObject under bucket default encryption must succeed");

    let get = client
        .get_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .expect("GET after self-copy failed: the object was re-keyed without rewriting the ciphertext");
    assert_eq!(get.server_side_encryption(), Some(&ServerSideEncryption::Aes256));
    assert_eq!(
        get.metadata().and_then(|m| m.get("stage")),
        Some(&"after".to_string()),
        "REPLACE metadata must take effect"
    );
    let body = get.body.collect().await.expect("collect body").into_bytes();
    assert_eq!(
        body.as_ref(),
        content,
        "object must still decrypt to the original plaintext after a metadata-only self copy"
    );

    kms_env.base_env.stop_server();
}
