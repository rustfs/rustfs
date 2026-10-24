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

//! Regression tests for Issue #2036
//! Verifies that anonymous access works correctly with bucket policies
//! when PublicAccessBlock configuration is missing or explicitly set.

use crate::common::{RustFSTestEnvironment, init_logging, local_http_client};
use crate::sts_query_compat_test::{OPA_AUTH_TOKEN, OpaMock};
use aws_sdk_s3::types::{PublicAccessBlockConfiguration, Tag, Tagging};
use tracing::info;

async fn setup_public_bucket(
    env: &RustFSTestEnvironment,
    bucket_name: &str,
) -> Result<aws_sdk_s3::Client, Box<dyn std::error::Error + Send + Sync>> {
    let admin_client = env.create_s3_client();

    admin_client.create_bucket().bucket(bucket_name).send().await?;

    let policy_json = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "AllowAnonymousGetObject",
                "Effect": "Allow",
                "Principal": "*",
                "Action": ["s3:GetObject"],
                "Resource": [format!("arn:aws:s3:::{}/*", bucket_name)]
            }
        ]
    })
    .to_string();

    admin_client
        .put_bucket_policy()
        .bucket(bucket_name)
        .policy(&policy_json)
        .send()
        .await?;

    admin_client
        .put_object()
        .bucket(bucket_name)
        .key("test.txt")
        .body(aws_sdk_s3::primitives::ByteStream::from_static(b"hello anonymous"))
        .send()
        .await?;

    Ok(admin_client)
}

async fn anonymous_get_object(
    env: &RustFSTestEnvironment,
    bucket_name: &str,
    key: &str,
) -> Result<reqwest::Response, reqwest::Error> {
    let url = format!("{}/{}/{}", env.url, bucket_name, key);
    local_http_client().get(&url).send().await
}

#[tokio::test]
async fn test_plain_http_matches_secure_transport_false_bucket_policy() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;
    assert!(env.url.starts_with("http://"), "fixture must exercise plaintext HTTP: {}", env.url);

    let bucket = "secure-transport-policy";
    let key = "plain-http.txt";
    let admin = env.create_s3_client();
    admin.create_bucket().bucket(bucket).send().await?;
    admin
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(aws_sdk_s3::primitives::ByteStream::from_static(b"plain http"))
        .send()
        .await?;
    admin
        .put_bucket_policy()
        .bucket(bucket)
        .policy(
            serde_json::json!({
                "Version": "2012-10-17",
                "Statement": [{
                    "Effect": "Allow",
                    "Principal": "*",
                    "Action": "s3:GetObject",
                    "Resource": format!("arn:aws:s3:::{bucket}/*"),
                    "Condition": {"Bool": {"aws:SecureTransport": "false"}}
                }]
            })
            .to_string(),
        )
        .send()
        .await?;

    let response = anonymous_get_object(&env, bucket, key).await?;
    assert_eq!(response.status().as_u16(), 200);
    assert_eq!(response.bytes().await?.as_ref(), b"plain http");
    Ok(())
}

/// Root credentials have no attached IAM policy, so these requests depend on
/// the owner flag reaching policy evaluation unchanged.
#[tokio::test]
async fn test_root_requests_keep_policy_owner_flag() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let opa = OpaMock::start().await?;
    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server_with_env(
        vec![],
        &[
            ("RUSTFS_POLICY_PLUGIN_URL", opa.url.as_str()),
            ("RUSTFS_POLICY_PLUGIN_AUTH_TOKEN", OPA_AUTH_TOKEN),
        ],
    )
    .await?;

    let bucket = "root-owner-flag";
    let key = "owner.txt";
    let client = env.create_s3_client();
    client.create_bucket().bucket(bucket).send().await?;
    client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(aws_sdk_s3::primitives::ByteStream::from_static(b"owner access"))
        .send()
        .await?;

    // Duplicate stored tags are ambiguous. An owner request must not enter the
    // OPA tag-loading path, which rejects this metadata before evaluation.
    client
        .put_bucket_tagging()
        .bucket(bucket)
        .tagging(
            Tagging::builder()
                .tag_set(Tag::builder().key("department").value("finance").build()?)
                .tag_set(Tag::builder().key("department").value("engineering").build()?)
                .build()?,
        )
        .send()
        .await?;
    let object = client.get_object().bucket(bucket).key(key).send().await?;

    assert_eq!(object.body.collect().await?.into_bytes().as_ref(), b"owner access");
    Ok(())
}

/// Issue #2036: Anonymous GetObject should succeed when bucket policy allows it
/// and no PublicAccessBlock configuration exists (ConfigNotFound).
#[tokio::test]
async fn test_anonymous_access_allowed_when_public_access_block_missing() -> Result<(), Box<dyn std::error::Error + Send + Sync>>
{
    init_logging();
    info!("Starting test: anonymous access with missing PublicAccessBlock config...");

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket_name = "anon-test-no-pab";
    let admin_client = setup_public_bucket(&env, bucket_name).await?;

    let _ = admin_client.delete_public_access_block().bucket(bucket_name).send().await;

    let resp = anonymous_get_object(&env, bucket_name, "test.txt").await?;
    assert_eq!(
        resp.status().as_u16(),
        200,
        "Anonymous GetObject should succeed when no PublicAccessBlock config exists"
    );

    info!("Test passed: anonymous access allowed with missing PublicAccessBlock config");
    Ok(())
}

/// Anonymous GetObject should be denied when RestrictPublicBuckets is true.
#[tokio::test]
async fn test_anonymous_access_denied_when_restrict_public_buckets_enabled()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();
    info!("Starting test: anonymous access denied with RestrictPublicBuckets=true...");

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket_name = "anon-test-restrict";
    let admin_client = setup_public_bucket(&env, bucket_name).await?;

    admin_client
        .put_public_access_block()
        .bucket(bucket_name)
        .public_access_block_configuration(
            PublicAccessBlockConfiguration::builder()
                .restrict_public_buckets(true)
                .build(),
        )
        .send()
        .await?;

    let resp = anonymous_get_object(&env, bucket_name, "test.txt").await?;
    assert_eq!(
        resp.status().as_u16(),
        403,
        "Anonymous GetObject should be denied when RestrictPublicBuckets is true"
    );

    info!("Test passed: anonymous access denied with RestrictPublicBuckets=true");
    Ok(())
}

/// Anonymous GetObject should succeed when PublicAccessBlock exists but
/// RestrictPublicBuckets is explicitly false.
#[tokio::test]
async fn test_anonymous_access_allowed_when_restrict_public_buckets_disabled()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();
    info!("Starting test: anonymous access allowed with RestrictPublicBuckets=false...");

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket_name = "anon-test-allow";
    let admin_client = setup_public_bucket(&env, bucket_name).await?;

    admin_client
        .put_public_access_block()
        .bucket(bucket_name)
        .public_access_block_configuration(
            PublicAccessBlockConfiguration::builder()
                .restrict_public_buckets(false)
                .build(),
        )
        .send()
        .await?;

    let resp = anonymous_get_object(&env, bucket_name, "test.txt").await?;
    assert_eq!(
        resp.status().as_u16(),
        200,
        "Anonymous GetObject should succeed when RestrictPublicBuckets is false"
    );

    info!("Test passed: anonymous access allowed with RestrictPublicBuckets=false");
    Ok(())
}

/// A policy granting anonymous `s3:ListBucket` also permits ListObjectVersions.
/// That grant must still be subject to RestrictPublicBuckets: the versions listing
/// reaches authorization through a fallback branch, and that branch has to apply the
/// same public-access gate as a direct grant.
#[tokio::test]
async fn ghsa_x298_anonymous_list_object_versions_denied_when_restrict_public_buckets_enabled()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();
    info!("Starting test: anonymous ListObjectVersions denied with RestrictPublicBuckets=true...");

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket_name = "anon-test-restrict-versions";
    let admin_client = env.create_s3_client();
    admin_client.create_bucket().bucket(bucket_name).send().await?;

    let policy_json = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "AllowAnonymousListBucket",
                "Effect": "Allow",
                "Principal": "*",
                "Action": ["s3:ListBucket"],
                "Resource": [format!("arn:aws:s3:::{}", bucket_name)]
            }
        ]
    })
    .to_string();

    admin_client
        .put_bucket_policy()
        .bucket(bucket_name)
        .policy(&policy_json)
        .send()
        .await?;

    admin_client
        .put_object()
        .bucket(bucket_name)
        .key("test.txt")
        .body(aws_sdk_s3::primitives::ByteStream::from_static(b"hello anonymous"))
        .send()
        .await?;

    // Without the public-access block the fallback grant is expected to work.
    let versions_url = format!("{}/{}?versions=", env.url, bucket_name);
    let resp = local_http_client().get(&versions_url).send().await?;
    assert_eq!(
        resp.status().as_u16(),
        200,
        "Anonymous ListObjectVersions should succeed via the s3:ListBucket grant"
    );

    admin_client
        .put_public_access_block()
        .bucket(bucket_name)
        .public_access_block_configuration(
            PublicAccessBlockConfiguration::builder()
                .restrict_public_buckets(true)
                .build(),
        )
        .send()
        .await?;

    let resp = local_http_client().get(&versions_url).send().await?;
    assert_eq!(
        resp.status().as_u16(),
        403,
        "Anonymous ListObjectVersions must be denied when RestrictPublicBuckets is true"
    );

    info!("Test passed: anonymous ListObjectVersions denied with RestrictPublicBuckets=true");
    Ok(())
}
