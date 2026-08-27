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

//! Anonymous access to SSE-KMS objects under per-key authorization.
//!
//! Locks both halves of the anonymous contract decided in backlog#2028 (D4):
//!
//! - **Enforcement on**: anonymous requests hold no `kms` grants, so a public
//!   bucket policy does not let them read SSE-KMS objects or write through an
//!   SSE-KMS default-encryption rule. Both fail with `AccessDenied`.
//! - **Enforcement off** (the default): bucket policy alone governs anonymous
//!   access, matching the pre-enforcement behavior — public SSE-KMS objects are
//!   decrypted and served, and anonymous writes are encrypted under the default
//!   key.
//!
//! The denial today is emergent — an empty-account principal falling through to
//! the IAM default deny — so without this file a refactor of principal
//! construction or policy evaluation could silently flip it. Each test carries a
//! plaintext-object positive control: a denial proves nothing while the bucket
//! policy has not propagated.

use super::common::{LocalKMSTestEnvironment, create_key_with_specific_id};
use crate::common::{init_logging, local_http_client};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    ServerSideEncryption, ServerSideEncryptionByDefault, ServerSideEncryptionConfiguration, ServerSideEncryptionRule,
};
use std::time::Duration;

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

const DEFAULT_KEY: &str = "kms-anon-default-key";
const BUCKET: &str = "kms-anon-enforcement";
const PLAIN_OBJECT: &str = "plain.txt";
const ENCRYPTED_OBJECT: &str = "encrypted.txt";
const PAYLOAD: &[u8] = b"kms anonymous enforcement payload";

/// How long a bucket policy change may take to reach the request path.
const POLICY_PROPAGATION: Duration = Duration::from_secs(20);

/// Start a local-KMS server and build the public-bucket fixture.
///
/// The bucket holds a plaintext object (the positive control), an SSE-KMS
/// object, an SSE-KMS default-encryption rule, and a bucket policy opening
/// `GetObject`/`PutObject` to everyone. The enforcement switch defaults to off,
/// so the enforcing case has to set it explicitly.
async fn start_public_sse_kms_bucket(env: &mut LocalKMSTestEnvironment, enforce: bool) -> TestResult {
    create_key_with_specific_id(&env.kms_keys_dir, DEFAULT_KEY).await?;

    let key_dir = env.kms_keys_dir.clone();
    let args = vec![
        "--kms-enable",
        "--kms-backend",
        "local",
        "--kms-key-dir",
        key_dir.as_str(),
        "--kms-default-key-id",
        DEFAULT_KEY,
    ];
    let mut envs = vec![("RUSTFS_KMS_ALLOW_INSECURE_DEV_DEFAULTS", "true")];
    if enforce {
        envs.push(("RUSTFS_KMS_ENFORCE_SSE_KEY_POLICY", "true"));
    }
    env.base_env.start_rustfs_server_with_env(args, &envs).await?;
    env.base_env.create_test_bucket(BUCKET).await?;

    let owner = env.base_env.create_s3_client();

    owner
        .put_object()
        .bucket(BUCKET)
        .key(PLAIN_OBJECT)
        .body(ByteStream::from_static(PAYLOAD))
        .send()
        .await?;
    owner
        .put_object()
        .bucket(BUCKET)
        .key(ENCRYPTED_OBJECT)
        .body(ByteStream::from_static(PAYLOAD))
        .server_side_encryption(ServerSideEncryption::AwsKms)
        .ssekms_key_id(DEFAULT_KEY)
        .send()
        .await?;

    let encryption_config = ServerSideEncryptionConfiguration::builder()
        .rules(
            ServerSideEncryptionRule::builder()
                .apply_server_side_encryption_by_default(
                    ServerSideEncryptionByDefault::builder()
                        .sse_algorithm(ServerSideEncryption::AwsKms)
                        .kms_master_key_id(DEFAULT_KEY)
                        .build()?,
                )
                .build(),
        )
        .build()?;
    owner
        .put_bucket_encryption()
        .bucket(BUCKET)
        .server_side_encryption_configuration(encryption_config)
        .send()
        .await?;

    let policy = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Sid": "PublicReadWrite",
            "Effect": "Allow",
            "Principal": "*",
            "Action": ["s3:GetObject", "s3:PutObject"],
            "Resource": [format!("arn:aws:s3:::{BUCKET}/*")]
        }]
    })
    .to_string();
    owner.put_bucket_policy().bucket(BUCKET).policy(&policy).send().await?;
    let _ = owner.delete_public_access_block().bucket(BUCKET).send().await;

    Ok(())
}

fn object_url(env: &LocalKMSTestEnvironment, key: &str) -> String {
    format!("{}/{BUCKET}/{key}", env.base_env.url)
}

async fn anonymous_get(env: &LocalKMSTestEnvironment, key: &str) -> Result<reqwest::Response, reqwest::Error> {
    local_http_client().get(object_url(env, key)).send().await
}

async fn anonymous_put(env: &LocalKMSTestEnvironment, key: &str) -> Result<reqwest::Response, reqwest::Error> {
    local_http_client().put(object_url(env, key)).body(PAYLOAD).send().await
}

/// Retry the plaintext read until the public bucket policy is live.
async fn wait_for_public_read(env: &LocalKMSTestEnvironment) -> TestResult {
    let deadline = tokio::time::Instant::now() + POLICY_PROPAGATION;
    loop {
        let status = anonymous_get(env, PLAIN_OBJECT).await?.status();
        if status.as_u16() == 200 {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(format!("positive control never became readable: anonymous GET {PLAIN_OBJECT} -> {status}").into());
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

async fn assert_anonymous_denied(response: reqwest::Response, what: &str) -> TestResult {
    let status = response.status().as_u16();
    let body = response.text().await?;
    assert_eq!(status, 403, "{what} must be denied, got {status}: {body}");
    assert!(body.contains("AccessDenied"), "{what} must carry AccessDenied: {body}");
    Ok(())
}

/// Enforcement on: a public bucket policy does not exempt anonymous requests
/// from per-key authorization, on either the read or the default-encryption
/// write path.
#[tokio::test(flavor = "multi_thread")]
async fn anonymous_sse_kms_denied_under_enforcement() -> TestResult {
    init_logging();

    let mut env = LocalKMSTestEnvironment::new().await?;
    start_public_sse_kms_bucket(&mut env, true).await?;
    wait_for_public_read(&env).await?;

    let read = anonymous_get(&env, ENCRYPTED_OBJECT).await?;
    assert_anonymous_denied(read, "anonymous GET of an SSE-KMS object").await?;

    let write = anonymous_put(&env, "anon-write.txt").await?;
    assert_anonymous_denied(write, "anonymous PUT through an SSE-KMS default-encryption rule").await?;

    Ok(())
}

/// Enforcement off (the default): bucket policy alone governs anonymous access,
/// and the default-encryption rule still encrypts anonymous writes.
#[tokio::test(flavor = "multi_thread")]
async fn anonymous_sse_kms_governed_by_bucket_policy_without_enforcement() -> TestResult {
    init_logging();

    let mut env = LocalKMSTestEnvironment::new().await?;
    start_public_sse_kms_bucket(&mut env, false).await?;
    wait_for_public_read(&env).await?;

    let read = anonymous_get(&env, ENCRYPTED_OBJECT).await?;
    assert_eq!(read.status().as_u16(), 200, "anonymous GET of a public SSE-KMS object must succeed");
    assert_eq!(read.bytes().await?.as_ref(), PAYLOAD, "the object must be served decrypted");

    let write = anonymous_put(&env, "anon-write.txt").await?;
    assert_eq!(write.status().as_u16(), 200, "anonymous PUT to a public bucket must succeed");

    let stored = env
        .base_env
        .create_s3_client()
        .head_object()
        .bucket(BUCKET)
        .key("anon-write.txt")
        .send()
        .await?;
    assert_eq!(
        stored.server_side_encryption(),
        Some(&ServerSideEncryption::AwsKms),
        "the anonymous write must be encrypted by the bucket default rule"
    );

    Ok(())
}
