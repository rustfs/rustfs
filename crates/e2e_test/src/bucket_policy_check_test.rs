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

//! Regression test for Issue #1423
//! Verifies that Bucket Policies are honored for Authenticated Users.

use crate::common::{RustFSTestEnvironment, init_logging};
use aws_sdk_s3::config::{Credentials, Region};
use aws_sdk_s3::{Client, Config};
use serial_test::serial;
use tracing::info;

async fn create_user(
    env: &RustFSTestEnvironment,
    username: &str,
    password: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let create_user_body = serde_json::json!({
        "secretKey": password,
        "status": "enabled"
    })
    .to_string();

    let create_user_url = format!("{}/rustfs/admin/v3/add-user?accessKey={}", env.url, username);
    crate::common::awscurl_put(&create_user_url, &create_user_body, &env.access_key, &env.secret_key).await?;
    Ok(())
}

fn create_user_client(env: &RustFSTestEnvironment, access_key: &str, secret_key: &str) -> Client {
    let credentials = Credentials::new(access_key, secret_key, None, None, "test-user");
    let config = Config::builder()
        .credentials_provider(credentials)
        .region(Region::new("us-east-1"))
        .endpoint_url(&env.url)
        .force_path_style(true)
        .behavior_version_latest()
        .build();

    Client::from_conf(config)
}

#[tokio::test]
#[serial]
async fn test_bucket_policy_authenticated_user() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();
    info!("Starting test_bucket_policy_authenticated_user...");

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let admin_client = env.create_s3_client();
    let bucket_name = "bucket-policy-auth-test";
    let object_key = "test-object.txt";
    let user_access = "testuser";
    let user_secret = "testpassword";

    // 1. Create Bucket (Admin)
    admin_client.create_bucket().bucket(bucket_name).send().await?;

    // 2. Create User (Admin API)
    create_user(&env, user_access, user_secret).await?;

    // 3. Create User Client
    let user_client = create_user_client(&env, user_access, user_secret);

    // 4. Verify Access Denied initially (No Policy)
    let result = user_client.list_objects_v2().bucket(bucket_name).send().await;
    if result.is_ok() {
        return Err("Should be Access Denied initially".into());
    }

    // 5. Apply Bucket Policy Allowed User
    let policy_json = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "AllowTestUser",
                "Effect": "Allow",
                "Principal": {
                    "AWS": [user_access]
                },
                "Action": [
                    "s3:ListBucket",
                    "s3:GetObject",
                    "s3:PutObject",
                    "s3:DeleteObject"
                ],
                "Resource": [
                    format!("arn:aws:s3:::{}", bucket_name),
                    format!("arn:aws:s3:::{}/*", bucket_name)
                ]
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

    // 6. Verify Access Allowed (With Bucket Policy)
    info!("Verifying PutObject...");
    user_client
        .put_object()
        .bucket(bucket_name)
        .key(object_key)
        .body(aws_sdk_s3::primitives::ByteStream::from_static(b"hello world"))
        .send()
        .await
        .map_err(|e| format!("PutObject failed: {}", e))?;

    info!("Verifying ListObjects...");
    let list_res = user_client
        .list_objects_v2()
        .bucket(bucket_name)
        .send()
        .await
        .map_err(|e| format!("ListObjects failed: {}", e))?;
    assert_eq!(list_res.contents().len(), 1);

    info!("Verifying GetObject...");
    user_client
        .get_object()
        .bucket(bucket_name)
        .key(object_key)
        .send()
        .await
        .map_err(|e| format!("GetObject failed: {}", e))?;

    info!("Verifying DeleteObject...");
    user_client
        .delete_object()
        .bucket(bucket_name)
        .key(object_key)
        .send()
        .await
        .map_err(|e| format!("DeleteObject failed: {}", e))?;

    info!("Test Passed!");
    Ok(())
}
