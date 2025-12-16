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

//! Tests for AWS IAM policy variables with single-value, multi-value, and nested scenarios

use crate::common::{awscurl_put, init_logging};
use crate::policy::test_env::PolicyTestEnvironment;
use aws_sdk_s3::primitives::ByteStream;
use serial_test::serial;
use tracing::info;

/// Helper function to create a regular user with given credentials
async fn create_user(
    env: &PolicyTestEnvironment,
    username: &str,
    password: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let create_user_body = serde_json::json!({
        "secretKey": password,
        "status": "enabled"
    })
    .to_string();

    let create_user_url = format!("{}/rustfs/admin/v3/add-user?accessKey={}", env.url, username);
    awscurl_put(&create_user_url, &create_user_body, &env.access_key, &env.secret_key).await?;
    Ok(())
}

/// Helper function to create an STS user with given credentials
async fn create_sts_user(
    env: &PolicyTestEnvironment,
    username: &str,
    password: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // For STS, we create a regular user first, then use it to assume roles
    create_user(env, username, password).await?;
    Ok(())
}

/// Helper function to create and attach a policy
async fn create_and_attach_policy(
    env: &PolicyTestEnvironment,
    policy_name: &str,
    username: &str,
    policy_document: serde_json::Value,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let policy_string = policy_document.to_string();

    // Create policy
    let add_policy_url = format!("{}/rustfs/admin/v3/add-canned-policy?name={}", env.url, policy_name);
    awscurl_put(&add_policy_url, &policy_string, &env.access_key, &env.secret_key).await?;

    // Attach policy to user
    let attach_policy_url = format!(
        "{}/rustfs/admin/v3/set-user-or-group-policy?policyName={}&userOrGroup={}&isGroup=false",
        env.url, policy_name, username
    );
    awscurl_put(&attach_policy_url, "", &env.access_key, &env.secret_key).await?;
    Ok(())
}

/// Helper function to clean up test resources
async fn cleanup_user_and_policy(env: &PolicyTestEnvironment, username: &str, policy_name: &str) {
    // Create admin client for cleanup
    let admin_client = env.create_s3_client(&env.access_key, &env.secret_key);

    // Delete buckets that might have been created by this user
    let bucket_patterns = [
        format!("{username}-test-bucket"),
        format!("{username}-bucket1"),
        format!("{username}-bucket2"),
        format!("{username}-bucket3"),
        format!("prefix-{username}-suffix"),
        format!("{username}-test"),
        format!("{username}-sts-bucket"),
        format!("{username}-service-bucket"),
        "private-test-bucket".to_string(), // For deny test
    ];

    // Try to delete objects and buckets
    for bucket_name in &bucket_patterns {
        let _ = admin_client
            .delete_object()
            .bucket(bucket_name)
            .key("test-object.txt")
            .send()
            .await;
        let _ = admin_client
            .delete_object()
            .bucket(bucket_name)
            .key("test-sts-object.txt")
            .send()
            .await;
        let _ = admin_client
            .delete_object()
            .bucket(bucket_name)
            .key("test-service-object.txt")
            .send()
            .await;
        let _ = admin_client.delete_bucket().bucket(bucket_name).send().await;
    }

    // Remove user
    let remove_user_url = format!("{}/rustfs/admin/v3/remove-user?accessKey={}", env.url, username);
    let _ = awscurl_put(&remove_user_url, "", &env.access_key, &env.secret_key).await;

    // Remove policy
    let remove_policy_url = format!("{}/rustfs/admin/v3/remove-canned-policy?name={}", env.url, policy_name);
    let _ = awscurl_put(&remove_policy_url, "", &env.access_key, &env.secret_key).await;
}

/// Test AWS policy variables with single-value scenarios
#[tokio::test(flavor = "multi_thread")]
#[serial]
#[ignore = "Starts a rustfs server; enable when running full E2E"]
pub async fn test_aws_policy_variables_single_value() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    test_aws_policy_variables_single_value_impl().await
}

/// Implementation function for single-value policy variables test
pub async fn test_aws_policy_variables_single_value_impl() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();
    info!("Starting AWS policy variables single-value test");

    let env = PolicyTestEnvironment::with_address("127.0.0.1:9000").await?;

    test_aws_policy_variables_single_value_impl_with_env(&env).await
}

/// Implementation function for single-value policy variables test with shared environment
pub async fn test_aws_policy_variables_single_value_impl_with_env(
    env: &PolicyTestEnvironment,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Create test user
    let test_user = "testuser1";
    let test_password = "testpassword123";
    let policy_name = "test-single-value-policy";

    // Create cleanup function
    let cleanup = || async {
        cleanup_user_and_policy(env, test_user, policy_name).await;
    };

    let create_user_body = serde_json::json!({
        "secretKey": test_password,
        "status": "enabled"
    })
    .to_string();

    let create_user_url = format!("{}/rustfs/admin/v3/add-user?accessKey={}", env.url, test_user);
    awscurl_put(&create_user_url, &create_user_body, &env.access_key, &env.secret_key).await?;

    // Create policy with single-value AWS variables
    let policy_document = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": ["s3:ListAllMyBuckets"],
                "Resource": ["arn:aws:s3:::*"]
            },
            {
                "Effect": "Allow",
                "Action": ["s3:CreateBucket"],
                "Resource": [format!("arn:aws:s3:::{}-*", "${aws:username}")]
            },
            {
                "Effect": "Allow",
                "Action": ["s3:ListBucket"],
                "Resource": [format!("arn:aws:s3:::{}-*", "${aws:username}")]
            },
            {
                "Effect": "Allow",
                "Action": ["s3:PutObject", "s3:GetObject"],
                "Resource": [format!("arn:aws:s3:::{}-*/*", "${aws:username}")]
            }
        ]
    })
    .to_string();

    let add_policy_url = format!("{}/rustfs/admin/v3/add-canned-policy?name={}", env.url, policy_name);
    awscurl_put(&add_policy_url, &policy_document, &env.access_key, &env.secret_key).await?;

    // Attach policy to user
    let attach_policy_url = format!(
        "{}/rustfs/admin/v3/set-user-or-group-policy?policyName={}&userOrGroup={}&isGroup=false",
        env.url, policy_name, test_user
    );
    awscurl_put(&attach_policy_url, "", &env.access_key, &env.secret_key).await?;

    // Create S3 client for test user
    let test_client = env.create_s3_client(test_user, test_password);

    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // Test 1: User should be able to list buckets (allowed by policy)
    info!("Test 1: User listing buckets");
    let list_result = test_client.list_buckets().send().await;
    if let Err(e) = list_result {
        cleanup().await;
        return Err(format!("User should be able to list buckets: {e}").into());
    }

    // Test 2: User should be able to create bucket matching username pattern
    info!("Test 2: User creating bucket matching pattern");
    let bucket_name = format!("{test_user}-test-bucket");
    let create_result = test_client.create_bucket().bucket(&bucket_name).send().await;
    if let Err(e) = create_result {
        cleanup().await;
        return Err(format!("User should be able to create bucket matching username pattern: {e}").into());
    }

    // Test 3: User should be able to list objects in their own bucket
    info!("Test 3: User listing objects in their bucket");
    let list_objects_result = test_client.list_objects_v2().bucket(&bucket_name).send().await;
    if let Err(e) = list_objects_result {
        cleanup().await;
        return Err(format!("User should be able to list objects in their own bucket: {e}").into());
    }

    // Test 4: User should be able to put object in their own bucket
    info!("Test 4: User putting object in their bucket");
    let put_result = test_client
        .put_object()
        .bucket(&bucket_name)
        .key("test-object.txt")
        .body(ByteStream::from_static(b"Hello, Policy Variables!"))
        .send()
        .await;
    if let Err(e) = put_result {
        cleanup().await;
        return Err(format!("User should be able to put object in their own bucket: {e}").into());
    }

    // Test 5: User should be able to get object from their own bucket
    info!("Test 5: User getting object from their bucket");
    let get_result = test_client
        .get_object()
        .bucket(&bucket_name)
        .key("test-object.txt")
        .send()
        .await;
    if let Err(e) = get_result {
        cleanup().await;
        return Err(format!("User should be able to get object from their own bucket: {e}").into());
    }

    // Test 6: User should NOT be able to create bucket NOT matching username pattern
    info!("Test 6: User attempting to create bucket NOT matching pattern");
    let other_bucket_name = "other-user-bucket";
    let create_other_result = test_client.create_bucket().bucket(other_bucket_name).send().await;
    if create_other_result.is_ok() {
        cleanup().await;
        return Err("User should NOT be able to create bucket NOT matching username pattern".into());
    }

    // Cleanup
    info!("Cleaning up test resources");
    cleanup().await;

    info!("AWS policy variables single-value test completed successfully");
    Ok(())
}

/// Test AWS policy variables with multi-value scenarios
#[tokio::test(flavor = "multi_thread")]
#[serial]
#[ignore = "Starts a rustfs server; enable when running full E2E"]
pub async fn test_aws_policy_variables_multi_value() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    test_aws_policy_variables_multi_value_impl().await
}

/// Implementation function for multi-value policy variables test
pub async fn test_aws_policy_variables_multi_value_impl() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();
    info!("Starting AWS policy variables multi-value test");

    let env = PolicyTestEnvironment::with_address("127.0.0.1:9000").await?;

    test_aws_policy_variables_multi_value_impl_with_env(&env).await
}

/// Implementation function for multi-value policy variables test with shared environment
pub async fn test_aws_policy_variables_multi_value_impl_with_env(
    env: &PolicyTestEnvironment,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Create test user
    let test_user = "testuser2";
    let test_password = "testpassword123";
    let policy_name = "test-multi-value-policy";

    // Create cleanup function
    let cleanup = || async {
        cleanup_user_and_policy(env, test_user, policy_name).await;
    };

    // Create user
    create_user(env, test_user, test_password).await?;

    // Create policy with multi-value AWS variables
    let policy_document = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": ["s3:ListAllMyBuckets"],
                "Resource": ["arn:aws:s3:::*"]
            },
            {
                "Effect": "Allow",
                "Action": ["s3:CreateBucket"],
                "Resource": [
                    format!("arn:aws:s3:::{}-bucket1", "${aws:username}"),
                    format!("arn:aws:s3:::{}-bucket2", "${aws:username}"),
                    format!("arn:aws:s3:::{}-bucket3", "${aws:username}")
                ]
            },
            {
                "Effect": "Allow",
                "Action": ["s3:ListBucket"],
                "Resource": [
                    format!("arn:aws:s3:::{}-bucket1", "${aws:username}"),
                    format!("arn:aws:s3:::{}-bucket2", "${aws:username}"),
                    format!("arn:aws:s3:::{}-bucket3", "${aws:username}")
                ]
            }
        ]
    });

    create_and_attach_policy(env, policy_name, test_user, policy_document).await?;

    // Create S3 client for test user
    let test_client = env.create_s3_client(test_user, test_password);

    // Test 1: User should be able to create buckets matching any of the multi-value patterns
    info!("Test 1: User creating first bucket matching multi-value pattern");
    let bucket1_name = format!("{test_user}-bucket1");
    let create_result1 = test_client.create_bucket().bucket(&bucket1_name).send().await;
    if let Err(e) = create_result1 {
        cleanup().await;
        return Err(format!("User should be able to create first bucket matching multi-value pattern: {e}").into());
    }

    info!("Test 2: User creating second bucket matching multi-value pattern");
    let bucket2_name = format!("{test_user}-bucket2");
    let create_result2 = test_client.create_bucket().bucket(&bucket2_name).send().await;
    if let Err(e) = create_result2 {
        cleanup().await;
        return Err(format!("User should be able to create second bucket matching multi-value pattern: {e}").into());
    }

    info!("Test 3: User creating third bucket matching multi-value pattern");
    let bucket3_name = format!("{test_user}-bucket3");
    let create_result3 = test_client.create_bucket().bucket(&bucket3_name).send().await;
    if let Err(e) = create_result3 {
        cleanup().await;
        return Err(format!("User should be able to create third bucket matching multi-value pattern: {e}").into());
    }

    // Test 4: User should NOT be able to create bucket NOT matching any multi-value pattern
    info!("Test 4: User attempting to create bucket NOT matching any pattern");
    let other_bucket_name = format!("{test_user}-other-bucket");
    let create_other_result = test_client.create_bucket().bucket(&other_bucket_name).send().await;
    if create_other_result.is_ok() {
        cleanup().await;
        return Err("User should NOT be able to create bucket NOT matching any multi-value pattern".into());
    }

    // Test 5: User should be able to list objects in their allowed buckets
    info!("Test 5: User listing objects in allowed buckets");
    let list_objects_result1 = test_client.list_objects_v2().bucket(&bucket1_name).send().await;
    if let Err(e) = list_objects_result1 {
        cleanup().await;
        return Err(format!("User should be able to list objects in first allowed bucket: {e}").into());
    }

    let list_objects_result2 = test_client.list_objects_v2().bucket(&bucket2_name).send().await;
    if let Err(e) = list_objects_result2 {
        cleanup().await;
        return Err(format!("User should be able to list objects in second allowed bucket: {e}").into());
    }

    // Cleanup
    info!("Cleaning up test resources");
    cleanup().await;

    info!("AWS policy variables multi-value test completed successfully");
    Ok(())
}

/// Test AWS policy variables with variable concatenation
#[tokio::test(flavor = "multi_thread")]
#[serial]
#[ignore = "Starts a rustfs server; enable when running full E2E"]
pub async fn test_aws_policy_variables_concatenation() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    test_aws_policy_variables_concatenation_impl().await
}

/// Implementation function for concatenation policy variables test
pub async fn test_aws_policy_variables_concatenation_impl() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();
    info!("Starting AWS policy variables concatenation test");

    let env = PolicyTestEnvironment::with_address("127.0.0.1:9000").await?;

    test_aws_policy_variables_concatenation_impl_with_env(&env).await
}

/// Implementation function for concatenation policy variables test with shared environment
pub async fn test_aws_policy_variables_concatenation_impl_with_env(
    env: &PolicyTestEnvironment,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Create test user
    let test_user = "testuser3";
    let test_password = "testpassword123";
    let policy_name = "test-concatenation-policy";

    // Create cleanup function
    let cleanup = || async {
        cleanup_user_and_policy(env, test_user, policy_name).await;
    };

    // Create user
    create_user(env, test_user, test_password).await?;

    // Create policy with variable concatenation
    let policy_document = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": ["s3:ListAllMyBuckets"],
                "Resource": ["arn:aws:s3:::*"]
            },
            {
                "Effect": "Allow",
                "Action": ["s3:CreateBucket"],
                "Resource": [format!("arn:aws:s3:::prefix-{}-suffix", "${aws:username}")]
            },
            {
                "Effect": "Allow",
                "Action": ["s3:ListBucket"],
                "Resource": [format!("arn:aws:s3:::prefix-{}-suffix", "${aws:username}")]
            }
        ]
    });

    create_and_attach_policy(env, policy_name, test_user, policy_document).await?;

    // Create S3 client for test user
    let test_client = env.create_s3_client(test_user, test_password);

    // Add a small delay to allow policy to propagate
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // Test: User should be able to create bucket matching concatenated pattern
    info!("Test: User creating bucket matching concatenated pattern");
    let bucket_name = format!("prefix-{test_user}-suffix");
    let create_result = test_client.create_bucket().bucket(&bucket_name).send().await;
    if let Err(e) = create_result {
        cleanup().await;
        return Err(format!("User should be able to create bucket matching concatenated pattern: {e}").into());
    }

    // Test: User should be able to list objects in the concatenated pattern bucket
    info!("Test: User listing objects in concatenated pattern bucket");
    let list_objects_result = test_client.list_objects_v2().bucket(&bucket_name).send().await;
    if let Err(e) = list_objects_result {
        cleanup().await;
        return Err(format!("User should be able to list objects in concatenated pattern bucket: {e}").into());
    }

    // Cleanup
    info!("Cleaning up test resources");
    cleanup().await;

    info!("AWS policy variables concatenation test completed successfully");
    Ok(())
}

/// Test AWS policy variables with nested scenarios
#[tokio::test(flavor = "multi_thread")]
#[serial]
#[ignore = "Starts a rustfs server; enable when running full E2E"]
pub async fn test_aws_policy_variables_nested() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    test_aws_policy_variables_nested_impl().await
}

/// Implementation function for nested policy variables test
pub async fn test_aws_policy_variables_nested_impl() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();
    info!("Starting AWS policy variables nested test");

    let env = PolicyTestEnvironment::with_address("127.0.0.1:9000").await?;

    test_aws_policy_variables_nested_impl_with_env(&env).await
}

/// Test AWS policy variables with STS temporary credentials
#[tokio::test(flavor = "multi_thread")]
#[serial]
#[ignore = "Starts a rustfs server; enable when running full E2E"]
pub async fn test_aws_policy_variables_sts() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    test_aws_policy_variables_sts_impl().await
}

/// Implementation function for STS policy variables test
pub async fn test_aws_policy_variables_sts_impl() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();
    info!("Starting AWS policy variables STS test");

    let env = PolicyTestEnvironment::with_address("127.0.0.1:9000").await?;

    test_aws_policy_variables_sts_impl_with_env(&env).await
}

/// Implementation function for nested policy variables test with shared environment
pub async fn test_aws_policy_variables_nested_impl_with_env(
    env: &PolicyTestEnvironment,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Create test user
    let test_user = "testuser4";
    let test_password = "testpassword123";
    let policy_name = "test-nested-policy";

    // Create cleanup function
    let cleanup = || async {
        cleanup_user_and_policy(env, test_user, policy_name).await;
    };

    // Create user
    create_user(env, test_user, test_password).await?;

    // Create policy with nested variables - this tests complex variable resolution
    let policy_document = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": ["s3:ListAllMyBuckets"],
                "Resource": ["arn:aws:s3:::*"]
            },
            {
                "Effect": "Allow",
                "Action": ["s3:CreateBucket"],
                "Resource": ["arn:aws:s3:::${${aws:username}-test}"]
            },
            {
                "Effect": "Allow",
                "Action": ["s3:ListBucket"],
                "Resource": ["arn:aws:s3:::${${aws:username}-test}"]
            }
        ]
    });

    create_and_attach_policy(env, policy_name, test_user, policy_document).await?;

    // Create S3 client for test user
    let test_client = env.create_s3_client(test_user, test_password);

    // Add a small delay to allow policy to propagate
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // Test nested variable resolution
    info!("Test: Nested variable resolution");

    // Create bucket with expected resolved name
    let expected_bucket = format!("{test_user}-test");

    // Attempt to create bucket with resolved name
    let create_result = test_client.create_bucket().bucket(&expected_bucket).send().await;

    // Verify bucket creation succeeds (nested variable resolved correctly)
    if let Err(e) = create_result {
        cleanup().await;
        return Err(format!("User should be able to create bucket with nested variable: {e}").into());
    }

    // Verify bucket creation fails with unresolved variable
    let unresolved_bucket = format!("${{}}-test {test_user}");
    let create_unresolved = test_client.create_bucket().bucket(&unresolved_bucket).send().await;

    if create_unresolved.is_ok() {
        cleanup().await;
        return Err("User should NOT be able to create bucket with unresolved variable".into());
    }

    // Cleanup
    info!("Cleaning up test resources");
    cleanup().await;

    info!("AWS policy variables nested test completed successfully");
    Ok(())
}

/// Implementation function for STS policy variables test with shared environment
pub async fn test_aws_policy_variables_sts_impl_with_env(
    env: &PolicyTestEnvironment,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Create test user for STS
    let test_user = "testuser-sts";
    let test_password = "testpassword123";
    let policy_name = "test-sts-policy";

    // Create cleanup function
    let cleanup = || async {
        cleanup_user_and_policy(env, test_user, policy_name).await;
    };

    // Create STS user
    create_sts_user(env, test_user, test_password).await?;

    // Create policy with STS-compatible variables
    let policy_document = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": ["s3:ListAllMyBuckets"],
                "Resource": ["arn:aws:s3:::*"]
            },
            {
                "Effect": "Allow",
                "Action": ["s3:CreateBucket"],
                "Resource": [format!("arn:aws:s3:::{}-sts-bucket", "${aws:username}")]
            },
            {
                "Effect": "Allow",
                "Action": ["s3:ListBucket", "s3:PutObject", "s3:GetObject"],
                "Resource": [format!("arn:aws:s3:::{}-sts-bucket/*", "${aws:username}")]
            }
        ]
    });

    create_and_attach_policy(env, policy_name, test_user, policy_document).await?;

    // Create S3 client for test user
    let test_client = env.create_s3_client(test_user, test_password);

    // Add a small delay to allow policy to propagate
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // Test: User should be able to create bucket matching STS pattern
    info!("Test: User creating bucket matching STS pattern");
    let bucket_name = format!("{test_user}-sts-bucket");
    let create_result = test_client.create_bucket().bucket(&bucket_name).send().await;
    if let Err(e) = create_result {
        cleanup().await;
        return Err(format!("User should be able to create STS bucket: {e}").into());
    }

    // Test: User should be able to put object in STS bucket
    info!("Test: User putting object in STS bucket");
    let put_result = test_client
        .put_object()
        .bucket(&bucket_name)
        .key("test-sts-object.txt")
        .body(ByteStream::from_static(b"STS Test Object"))
        .send()
        .await;
    if let Err(e) = put_result {
        cleanup().await;
        return Err(format!("User should be able to put object in STS bucket: {e}").into());
    }

    // Test: User should be able to get object from STS bucket
    info!("Test: User getting object from STS bucket");
    let get_result = test_client
        .get_object()
        .bucket(&bucket_name)
        .key("test-sts-object.txt")
        .send()
        .await;
    if let Err(e) = get_result {
        cleanup().await;
        return Err(format!("User should be able to get object from STS bucket: {e}").into());
    }

    // Test: User should be able to list objects in STS bucket
    info!("Test: User listing objects in STS bucket");
    let list_result = test_client.list_objects_v2().bucket(&bucket_name).send().await;
    if let Err(e) = list_result {
        cleanup().await;
        return Err(format!("User should be able to list objects in STS bucket: {e}").into());
    }

    // Cleanup
    info!("Cleaning up test resources");
    cleanup().await;

    info!("AWS policy variables STS test completed successfully");
    Ok(())
}

/// Test AWS policy variables with deny scenarios
#[tokio::test(flavor = "multi_thread")]
#[serial]
#[ignore = "Starts a rustfs server; enable when running full E2E"]
pub async fn test_aws_policy_variables_deny() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    test_aws_policy_variables_deny_impl().await
}

/// Implementation function for deny policy variables test
pub async fn test_aws_policy_variables_deny_impl() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();
    info!("Starting AWS policy variables deny test");

    let env = PolicyTestEnvironment::with_address("127.0.0.1:9000").await?;

    test_aws_policy_variables_deny_impl_with_env(&env).await
}

/// Implementation function for deny policy variables test with shared environment
pub async fn test_aws_policy_variables_deny_impl_with_env(
    env: &PolicyTestEnvironment,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Create test user
    let test_user = "testuser5";
    let test_password = "testpassword123";
    let policy_name = "test-deny-policy";

    // Create cleanup function
    let cleanup = || async {
        cleanup_user_and_policy(env, test_user, policy_name).await;
    };

    // Create user
    create_user(env, test_user, test_password).await?;

    // Create policy with both allow and deny statements
    let policy_document = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [
            // Allow general access
            {
                "Effect": "Allow",
                "Action": ["s3:ListAllMyBuckets"],
                "Resource": ["arn:aws:s3:::*"]
            },
            // Allow creating buckets matching username pattern
            {
                "Effect": "Allow",
                "Action": ["s3:CreateBucket"],
                "Resource": [format!("arn:aws:s3:::{}-*", "${aws:username}")]
            },
            // Deny creating buckets with "private" in the name
            {
                "Effect": "Deny",
                "Action": ["s3:CreateBucket"],
                "Resource": ["arn:aws:s3:::*private*"]
            }
        ]
    });

    create_and_attach_policy(env, policy_name, test_user, policy_document).await?;

    // Create S3 client for test user
    let test_client = env.create_s3_client(test_user, test_password);

    // Add a small delay to allow policy to propagate
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // Test 1: User should be able to create bucket matching username pattern
    info!("Test 1: User creating bucket matching username pattern");
    let bucket_name = format!("{test_user}-test-bucket");
    let create_result = test_client.create_bucket().bucket(&bucket_name).send().await;
    if let Err(e) = create_result {
        cleanup().await;
        return Err(format!("User should be able to create bucket matching username pattern: {e}").into());
    }

    // Test 2: User should NOT be able to create bucket with "private" in the name (deny rule)
    info!("Test 2: User attempting to create bucket with 'private' in name (should be denied)");
    let private_bucket_name = "private-test-bucket";
    let create_private_result = test_client.create_bucket().bucket(private_bucket_name).send().await;
    if create_private_result.is_ok() {
        cleanup().await;
        return Err("User should NOT be able to create bucket with 'private' in name due to deny rule".into());
    }

    // Cleanup
    info!("Cleaning up test resources");
    cleanup().await;

    info!("AWS policy variables deny test completed successfully");
    Ok(())
}
