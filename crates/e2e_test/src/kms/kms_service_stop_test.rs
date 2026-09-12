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

//! SSE-KMS writes against a node whose KMS is stopped, was never configured,
//! or runs without a default key.
//!
//! `docs/operations/kms-backend-security.md` promises `503` for a configured
//! KMS that is not running and `400 InvalidRequest` when KMS was never
//! configured. The bare `aws:kms` form (no key id, no bucket default) used to
//! miss both branches and surface as `500`, so every scenario here is
//! exercised with and without a key id.

use super::common::{
    LocalKMSTestEnvironment, assert_s3_error, create_key_with_specific_id, start_kms, stop_kms, wait_for_kms_ready,
};
use crate::common::{RustFSTestEnvironment, TEST_BUCKET, init_logging};
use aws_sdk_s3::Client;
use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::ServerSideEncryption;
use tracing::info;

const SERVICE_UNAVAILABLE_MESSAGE: &str = "The service is unavailable. Please retry.";
const KMS_NOT_CONFIGURED_MESSAGE: &str = "SSE-KMS requires a configured and running KMS service";
const KMS_NO_DEFAULT_KEY_MESSAGE: &str =
    "SSE-KMS requires a KMS key id: the request named none and the KMS service has no default key";

/// Issue an SSE-KMS PutObject and a CreateMultipartUpload, each with and
/// without a key id, and require every one of them to fail with `status`/`code`/`message`.
async fn assert_sse_kms_writes_refused(client: &Client, key_prefix: &str, status: u16, code: &str, message: &str) {
    for (label, key_id) in [("bare", None), ("keyed", Some("rustfs-e2e-refused-key"))] {
        let object_key = format!("{key_prefix}-{label}");
        let mut put = client
            .put_object()
            .bucket(TEST_BUCKET)
            .key(&object_key)
            .body(ByteStream::from_static(b"must not be published"))
            .server_side_encryption(ServerSideEncryption::AwsKms);
        if let Some(key_id) = key_id {
            put = put.ssekms_key_id(key_id);
        }
        assert_s3_error(put.send().await, status, code, message, &format!("{label} SSE-KMS PutObject"));

        let mut create = client
            .create_multipart_upload()
            .bucket(TEST_BUCKET)
            .key(&object_key)
            .server_side_encryption(ServerSideEncryption::AwsKms);
        if let Some(key_id) = key_id {
            create = create.ssekms_key_id(key_id);
        }
        assert_s3_error(
            create.send().await,
            status,
            code,
            message,
            &format!("{label} SSE-KMS CreateMultipartUpload"),
        );

        let absence = client
            .get_object()
            .bucket(TEST_BUCKET)
            .key(&object_key)
            .send()
            .await
            .expect_err("a refused SSE-KMS write must not publish an object");
        assert_eq!(absence.raw_response().map(|response| response.status().as_u16()), Some(404));
        assert_eq!(absence.as_service_error().and_then(ProvideErrorMetadata::code), Some("NoSuchKey"));
    }
}

/// A configured KMS that an operator stopped is a transient outage: every
/// SSE-KMS write is refused with `503` until `kms/start`, after which the bare
/// form resolves the service default key again and earlier objects read back.
#[tokio::test]
async fn test_sse_kms_writes_are_refused_with_503_while_kms_is_stopped() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();
    let mut kms_env = LocalKMSTestEnvironment::new().await?;
    let default_key_id = kms_env.start_rustfs_for_local_kms().await?;
    kms_env.wait_for_kms_ready().await?;
    let base = &kms_env.base_env;
    let client = base.create_s3_client();
    base.create_test_bucket(TEST_BUCKET).await?;

    let object_key = "written-before-stop";
    let payload = b"encrypted under the service default key".to_vec();
    let put = client
        .put_object()
        .bucket(TEST_BUCKET)
        .key(object_key)
        .body(ByteStream::from(payload.clone()))
        .server_side_encryption(ServerSideEncryption::AwsKms)
        .send()
        .await?;
    assert_eq!(put.server_side_encryption(), Some(&ServerSideEncryption::AwsKms));
    assert_eq!(put.ssekms_key_id(), Some(default_key_id.as_str()));

    info!("stopping KMS through the admin API");
    stop_kms(&base.url, &base.access_key, &base.secret_key).await?;
    assert_sse_kms_writes_refused(&client, "while-stopped", 503, "ServiceUnavailable", SERVICE_UNAVAILABLE_MESSAGE).await;

    info!("starting KMS again");
    start_kms(&base.url, &base.access_key, &base.secret_key).await?;
    wait_for_kms_ready(&base.url, &base.access_key, &base.secret_key).await?;

    let restored = client
        .put_object()
        .bucket(TEST_BUCKET)
        .key("written-after-start")
        .body(ByteStream::from_static(b"service is back"))
        .server_side_encryption(ServerSideEncryption::AwsKms)
        .send()
        .await?;
    assert_eq!(restored.ssekms_key_id(), Some(default_key_id.as_str()));

    let read_back = client.get_object().bucket(TEST_BUCKET).key(object_key).send().await?;
    assert_eq!(read_back.body.collect().await?.into_bytes().as_ref(), payload.as_slice());

    base.delete_test_bucket(TEST_BUCKET).await?;
    Ok(())
}

/// A node that only carries `RUSTFS_SSE_S3_MASTER_KEY` serves SSE-S3 but has
/// no KMS to name: SSE-KMS is a client configuration error (`400`), and it
/// must never be downgraded onto the local master key.
#[tokio::test]
async fn test_sse_kms_writes_are_refused_with_400_when_kms_was_never_configured()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();
    let mut env = RustFSTestEnvironment::new().await?;
    // base64 of 32 zero bytes: a valid master key shape for the SSE-S3 fallback.
    env.start_rustfs_server_with_env(vec![], &[("RUSTFS_SSE_S3_MASTER_KEY", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")])
        .await?;
    let client = env.create_s3_client();
    env.create_test_bucket(TEST_BUCKET).await?;

    let sse_s3 = client
        .put_object()
        .bucket(TEST_BUCKET)
        .key("sse-s3-fallback")
        .body(ByteStream::from_static(b"local master key still serves AES256"))
        .server_side_encryption(ServerSideEncryption::Aes256)
        .send()
        .await?;
    assert_eq!(sse_s3.server_side_encryption(), Some(&ServerSideEncryption::Aes256));

    assert_sse_kms_writes_refused(&client, "no-kms", 400, "InvalidRequest", KMS_NOT_CONFIGURED_MESSAGE).await;

    env.delete_test_bucket(TEST_BUCKET).await?;
    Ok(())
}

/// A running KMS without a default key can serve a keyed request but has
/// nothing to resolve a bare `aws:kms` request to; that is the caller's
/// omission, not a server fault.
#[tokio::test]
async fn test_bare_sse_kms_write_is_refused_with_400_when_kms_has_no_default_key()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();
    let mut kms_env = LocalKMSTestEnvironment::new().await?;
    let named_key_id = "rustfs-e2e-named-key";
    create_key_with_specific_id(&kms_env.kms_keys_dir, named_key_id).await?;
    let key_dir = kms_env.kms_keys_dir.clone();
    kms_env
        .base_env
        .start_rustfs_server_with_env(
            vec!["--kms-enable", "--kms-backend", "local", "--kms-key-dir", &key_dir],
            &[("RUSTFS_KMS_ALLOW_INSECURE_DEV_DEFAULTS", "true")],
        )
        .await?;
    kms_env.wait_for_kms_ready().await?;
    let base = &kms_env.base_env;
    let client = base.create_s3_client();
    base.create_test_bucket(TEST_BUCKET).await?;

    let keyed = client
        .put_object()
        .bucket(TEST_BUCKET)
        .key("keyed-without-default")
        .body(ByteStream::from_static(b"a named key needs no default"))
        .server_side_encryption(ServerSideEncryption::AwsKms)
        .ssekms_key_id(named_key_id)
        .send()
        .await?;
    assert_eq!(keyed.ssekms_key_id(), Some(named_key_id));

    let bare = client
        .put_object()
        .bucket(TEST_BUCKET)
        .key("bare-without-default")
        .body(ByteStream::from_static(b"must not be published"))
        .server_side_encryption(ServerSideEncryption::AwsKms)
        .send()
        .await;
    assert_s3_error(
        bare,
        400,
        "InvalidRequest",
        KMS_NO_DEFAULT_KEY_MESSAGE,
        "bare SSE-KMS PutObject without default key",
    );

    let bare_multipart = client
        .create_multipart_upload()
        .bucket(TEST_BUCKET)
        .key("bare-without-default")
        .server_side_encryption(ServerSideEncryption::AwsKms)
        .send()
        .await;
    assert_s3_error(
        bare_multipart,
        400,
        "InvalidRequest",
        KMS_NO_DEFAULT_KEY_MESSAGE,
        "bare SSE-KMS CreateMultipartUpload without default key",
    );

    base.delete_test_bucket(TEST_BUCKET).await?;
    Ok(())
}
