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
use aws_sdk_s3::types::PublicAccessBlockConfiguration;
use serial_test::serial;
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

/// Issue #2036: Anonymous GetObject should succeed when bucket policy allows it
/// and no PublicAccessBlock configuration exists (ConfigNotFound).
#[tokio::test]
#[serial]
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
#[serial]
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
#[serial]
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
