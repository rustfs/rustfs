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

use crate::common::{RustFSTestEnvironment, admin_ok, build_test_s3_config, build_test_sts_client, init_logging};
use aws_sdk_s3::Client;
use aws_sdk_s3::error::ProvideErrorMetadata;
use tokio::time::{Duration, Instant};

fn user_client(env: &RustFSTestEnvironment, access_key: &str, secret_key: &str, session_token: Option<&str>) -> Client {
    Client::from_conf(build_test_s3_config(
        &env.url,
        access_key,
        secret_key,
        session_token,
        "list-buckets-iam-filter",
    ))
}

fn bucket_names(buckets: &[aws_sdk_s3::types::Bucket]) -> Vec<String> {
    let mut names = buckets
        .iter()
        .filter_map(|bucket| bucket.name().map(str::to_owned))
        .collect::<Vec<_>>();
    names.sort();
    names
}

async fn create_user(
    env: &RustFSTestEnvironment,
    access_key: &str,
    secret_key: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let body = serde_json::json!({ "secretKey": secret_key, "status": "enabled" }).to_string();
    admin_ok(
        env,
        http::Method::PUT,
        &format!("/rustfs/admin/v3/add-user?accessKey={access_key}"),
        Some(body),
    )
    .await?;
    Ok(())
}

async fn create_service_account(
    env: &RustFSTestEnvironment,
    target_user: &str,
    policy: Option<&serde_json::Value>,
) -> Result<(String, String), Box<dyn std::error::Error + Send + Sync>> {
    let request = match policy {
        Some(policy) => serde_json::json!({ "targetUser": target_user, "policy": policy }),
        None => serde_json::json!({ "targetUser": target_user }),
    };
    let response = admin_ok(env, http::Method::PUT, "/rustfs/admin/v3/add-service-accounts", Some(request.to_string())).await?;
    let response: serde_json::Value = serde_json::from_str(&response)?;
    let access_key = response["credentials"]["accessKey"]
        .as_str()
        .ok_or("service account response should contain credentials.accessKey")?
        .to_owned();
    let secret_key = response["credentials"]["secretKey"]
        .as_str()
        .ok_or("service account response should contain credentials.secretKey")?
        .to_owned();
    Ok((access_key, secret_key))
}

#[tokio::test]
async fn list_buckets_filters_with_iam_bucket_resources() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();
    let mut env = RustFSTestEnvironment::new().await?;
    env.capture_log_path = Some(format!("{}/server.log", env.temp_dir));
    env.start_rustfs_server_with_env(vec![], &[("RUST_LOG", "rustfs=debug,rustfs_notify=debug")])
        .await?;

    let admin_client = env.create_s3_client();
    for bucket in [
        "benchmark-artifacts",
        "benchmark-denied",
        "benchmark-location-only",
        "benchmark-test1",
        "testuser1-artifacts",
    ] {
        admin_client.create_bucket().bucket(bucket).send().await?;
    }
    assert_eq!(
        bucket_names(admin_client.list_buckets().send().await?.buckets()),
        vec![
            "benchmark-artifacts",
            "benchmark-denied",
            "benchmark-location-only",
            "benchmark-test1",
            "testuser1-artifacts"
        ]
    );

    let access_key = "benchmark";
    let secret_key = "benchmark-secret-1234567890";
    create_user(&env, access_key, secret_key).await?;

    let policy_name = "benchmark-bucket-prefix";
    let policy = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": ["s3:*"],
                "Resource": ["arn:aws:s3:::benchmark-*", "arn:aws:s3:::benchmark-*/*"],
                "Condition": {
                    "StringEquals": {
                        "s3:prefix": [""],
                        "s3:delimiter": ["/"]
                    }
                }
            },
            {
                "Effect": "Deny",
                "Action": ["s3:ListBucket", "s3:GetBucketLocation"],
                "Resource": ["arn:aws:s3:::benchmark-denied"]
            },
            {
                "Effect": "Deny",
                "Action": ["s3:ListBucket"],
                "Resource": ["arn:aws:s3:::benchmark-location-only"]
            },
            {
                "Effect": "Allow",
                "Action": ["sts:AssumeRole"],
                "Resource": ["arn:aws:s3:::*"]
            }
        ]
    })
    .to_string();
    admin_ok(
        &env,
        http::Method::PUT,
        &format!("/rustfs/admin/v3/add-canned-policy?name={policy_name}"),
        Some(policy),
    )
    .await?;
    admin_ok(
        &env,
        http::Method::PUT,
        &format!("/rustfs/admin/v3/set-user-or-group-policy?policyName={policy_name}&userOrGroup={access_key}&isGroup=false"),
        Some(String::new()),
    )
    .await?;

    let bucket_policy_allow = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": { "AWS": [access_key] },
            "Action": ["s3:ListBucket"],
            "Resource": ["arn:aws:s3:::testuser1-artifacts"]
        }]
    })
    .to_string();
    admin_client
        .put_bucket_policy()
        .bucket("testuser1-artifacts")
        .policy(bucket_policy_allow)
        .send()
        .await?;

    let bucket_policy_deny = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Deny",
            "Principal": { "AWS": [access_key] },
            "Action": ["s3:ListBucket", "s3:GetBucketLocation"],
            "Resource": ["arn:aws:s3:::benchmark-artifacts"]
        }]
    })
    .to_string();
    admin_client
        .put_bucket_policy()
        .bucket("benchmark-artifacts")
        .policy(bucket_policy_deny)
        .send()
        .await?;

    let benchmark_client = user_client(&env, access_key, secret_key, None);
    benchmark_client
        .list_objects_v2()
        .bucket("testuser1-artifacts")
        .send()
        .await?;

    assert_eq!(
        bucket_names(benchmark_client.list_buckets().send().await?.buckets()),
        vec!["benchmark-artifacts", "benchmark-location-only", "benchmark-test1"]
    );
    let log_path = env.capture_log_path.as_deref().expect("server log path should be configured");
    let deadline = Instant::now() + Duration::from_secs(5);
    let audit_log = loop {
        let audit_log = tokio::fs::read_to_string(log_path).await?;
        if [
            "iam_implicit_deny",
            "s3_authorization_denied",
            "ListAllMyBucketsAction",
            "benchmark",
            "DEBUG",
        ]
        .iter()
        .all(|field| audit_log.contains(field))
            || Instant::now() >= deadline
        {
            break audit_log;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    assert_eq!(audit_log.matches("iam_implicit_deny").count(), 1, "{audit_log}");
    for field in ["s3_authorization_denied", "ListAllMyBucketsAction", "benchmark", "DEBUG"] {
        assert!(audit_log.contains(field), "missing {field} in {audit_log}");
    }

    let denied_access_key = "no-bucket-access";
    let denied_secret_key = "no-bucket-access-secret-1234567890";
    create_user(&env, denied_access_key, denied_secret_key).await?;
    let denied = user_client(&env, denied_access_key, denied_secret_key, None)
        .list_buckets()
        .send()
        .await
        .expect_err("a user without IAM bucket permissions must be denied");
    assert_eq!(denied.as_service_error().and_then(ProvideErrorMetadata::code), Some("AccessDenied"));

    let put_only_policy_name = "put-only-no-bucket-discovery";
    let put_only_policy = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Action": ["s3:PutObject"],
            "Resource": ["arn:aws:s3:::benchmark-*/*"]
        }]
    })
    .to_string();
    admin_ok(
        &env,
        http::Method::PUT,
        &format!("/rustfs/admin/v3/add-canned-policy?name={put_only_policy_name}"),
        Some(put_only_policy),
    )
    .await?;
    admin_ok(
        &env,
        http::Method::PUT,
        &format!(
            "/rustfs/admin/v3/set-user-or-group-policy?policyName={put_only_policy_name}&userOrGroup={denied_access_key}&isGroup=false"
        ),
        Some(String::new()),
    )
    .await?;
    let denied = user_client(&env, denied_access_key, denied_secret_key, None)
        .list_buckets()
        .send()
        .await
        .expect_err("an unrelated IAM action must not reveal bucket names");
    assert_eq!(denied.as_service_error().and_then(ProvideErrorMetadata::code), Some("AccessDenied"));

    let list_all_policy_name = "list-all-buckets";
    let list_all_policy = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Action": ["s3:ListAllMyBuckets"],
            "Resource": ["arn:aws:s3:::*"]
        }]
    })
    .to_string();
    admin_ok(
        &env,
        http::Method::PUT,
        &format!("/rustfs/admin/v3/add-canned-policy?name={list_all_policy_name}"),
        Some(list_all_policy),
    )
    .await?;
    admin_ok(
        &env,
        http::Method::PUT,
        &format!(
            "/rustfs/admin/v3/set-user-or-group-policy?policyName={list_all_policy_name}&userOrGroup={denied_access_key}&isGroup=false"
        ),
        Some(String::new()),
    )
    .await?;
    assert_eq!(
        bucket_names(
            user_client(&env, denied_access_key, denied_secret_key, None)
                .list_buckets()
                .send()
                .await?
                .buckets()
        ),
        vec![
            "benchmark-artifacts",
            "benchmark-denied",
            "benchmark-location-only",
            "benchmark-test1",
            "testuser1-artifacts"
        ]
    );

    let group_user = "benchmark-group-user";
    let group_secret = "benchmark-group-secret-1234567890";
    let group_name = "benchmark-group";
    create_user(&env, group_user, group_secret).await?;
    admin_ok(
        &env,
        http::Method::PUT,
        "/rustfs/admin/v3/update-group-members",
        Some(
            serde_json::json!({
                "group": group_name,
                "members": [group_user],
                "isRemove": false,
                "groupStatus": "enabled"
            })
            .to_string(),
        ),
    )
    .await?;
    admin_ok(
        &env,
        http::Method::PUT,
        &format!("/rustfs/admin/v3/set-user-or-group-policy?policyName={policy_name}&userOrGroup={group_name}&isGroup=true"),
        Some(String::new()),
    )
    .await?;

    assert_eq!(
        bucket_names(
            user_client(&env, group_user, group_secret, None)
                .list_buckets()
                .send()
                .await?
                .buckets()
        ),
        vec!["benchmark-artifacts", "benchmark-location-only", "benchmark-test1"]
    );

    let (service_access_key, service_secret_key) = create_service_account(&env, group_user, None).await?;
    assert_eq!(
        bucket_names(
            user_client(&env, &service_access_key, &service_secret_key, None)
                .list_buckets()
                .send()
                .await?
                .buckets()
        ),
        vec!["benchmark-artifacts", "benchmark-location-only", "benchmark-test1"]
    );

    let service_account_policy = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Action": ["s3:ListBucket", "s3:GetBucketLocation"],
            "Resource": ["arn:aws:s3:::benchmark-test1"],
            "Condition": {
                "StringEquals": {
                    "s3:prefix": [""],
                    "s3:delimiter": ["/"]
                }
            }
        }]
    });
    let (restricted_service_access_key, restricted_service_secret_key) =
        create_service_account(&env, group_user, Some(&service_account_policy)).await?;
    assert_eq!(
        bucket_names(
            user_client(&env, &restricted_service_access_key, &restricted_service_secret_key, None,)
                .list_buckets()
                .send()
                .await?
                .buckets()
        ),
        vec!["benchmark-test1"]
    );

    let sts_client = build_test_sts_client(&env.url, group_user, group_secret, None, "list-buckets-iam-filter-sts");
    let inherited = sts_client
        .assume_role()
        .role_arn("arn:aws:iam::123456789012:role/list-buckets")
        .role_session_name("list-buckets-iam-filter-inherited")
        .send()
        .await?;
    let inherited = inherited
        .credentials()
        .ok_or("AssumeRole response should contain inherited temporary credentials")?;
    assert_eq!(
        bucket_names(
            user_client(
                &env,
                inherited.access_key_id(),
                inherited.secret_access_key(),
                Some(inherited.session_token()),
            )
            .list_buckets()
            .send()
            .await?
            .buckets()
        ),
        vec!["benchmark-artifacts", "benchmark-location-only", "benchmark-test1"]
    );

    let session_policy = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Action": ["s3:ListBucket", "s3:GetBucketLocation"],
            "Resource": ["arn:aws:s3:::benchmark-test1"],
            "Condition": {
                "StringEquals": {
                    "s3:prefix": [""],
                    "s3:delimiter": ["/"]
                }
            }
        }]
    })
    .to_string();
    let assumed = sts_client
        .assume_role()
        .role_arn("arn:aws:iam::123456789012:role/list-buckets")
        .role_session_name("list-buckets-iam-filter")
        .policy(session_policy)
        .send()
        .await?;
    let temporary = assumed
        .credentials()
        .ok_or("AssumeRole response should contain temporary credentials")?;
    assert_eq!(
        bucket_names(
            user_client(
                &env,
                temporary.access_key_id(),
                temporary.secret_access_key(),
                Some(temporary.session_token()),
            )
            .list_buckets()
            .send()
            .await?
            .buckets()
        ),
        vec!["benchmark-test1"]
    );
    Ok(())
}
