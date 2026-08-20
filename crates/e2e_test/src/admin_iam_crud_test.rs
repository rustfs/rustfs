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

//! Systematic HTTP e2e for the admin IAM management surface (backlog#1154
//! peri-2, first batch): the full user / canned-policy / service-account CRUD
//! lifecycle over real signed HTTP against a real binary, plus a non-admin
//! denial probe for every management endpoint (reusing the sec-4 assertion
//! pattern — the authorization gate itself is pinned by `admin_auth_test`).
//!
//! Before this suite the ~40 admin handler modules only had helper-level unit
//! tests; no test proved that `mc admin user add` style flows work end to end
//! (create -> attach policy -> the credential actually gains S3 access ->
//! service account inherits it -> deletion revokes it).
//!
//! Later batches tracked on backlog#1154: config get/set, info, pools status,
//! group lifecycle, import/export IAM.

use crate::common::{
    RustFSTestEnvironment, admin_ok, admin_request, admin_request_with_session_token, build_test_sts_client, init_logging,
};
use aws_sdk_s3::config::{Credentials, Region};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::{Client, Config};
use reqwest::StatusCode;
use std::error::Error;
use tokio::time::{Duration, sleep};

type TestResult = Result<(), Box<dyn Error + Send + Sync>>;

fn build_s3_client(url: &str, access_key: &str, secret_key: &str) -> Client {
    let config = Config::builder()
        .credentials_provider(Credentials::new(access_key, secret_key, None, None, "e2e-admin-crud"))
        .region(Region::new("us-east-1"))
        .endpoint_url(url)
        .force_path_style(true)
        .behavior_version_latest()
        .build();
    Client::from_conf(config)
}

/// Retries an S3 PUT until IAM propagation makes it succeed (bounded).
async fn wait_for_s3_put(client: &Client, bucket: &str, key: &str, within: Duration) -> TestResult {
    let deadline = tokio::time::Instant::now() + within;
    loop {
        match client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(ByteStream::from_static(b"peri-2 probe"))
            .send()
            .await
        {
            Ok(_) => return Ok(()),
            Err(err) => {
                if tokio::time::Instant::now() >= deadline {
                    return Err(format!("PUT {bucket}/{key} never succeeded: {err:?}").into());
                }
            }
        }
        sleep(Duration::from_millis(500)).await;
    }
}

/// A canned policy granting full S3 access to one bucket.
fn bucket_rw_policy(bucket: &str) -> String {
    serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Action": ["s3:*"],
            "Resource": [
                format!("arn:aws:s3:::{bucket}"),
                format!("arn:aws:s3:::{bucket}/*")
            ]
        }]
    })
    .to_string()
}

async fn create_user_with_service_account_update_policy(
    env: &RustFSTestEnvironment,
    user: &str,
    secret: &str,
    policy: &str,
) -> TestResult {
    admin_ok(
        env,
        http::Method::PUT,
        &format!("/rustfs/admin/v3/add-user?accessKey={user}"),
        Some(serde_json::json!({ "secretKey": secret, "status": "enabled" }).to_string()),
    )
    .await?;
    admin_ok(
        env,
        http::Method::PUT,
        &format!("/rustfs/admin/v3/add-canned-policy?name={policy}"),
        Some(
            serde_json::json!({
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Action": ["admin:UpdateServiceAccount"]
                    },
                    {
                        "Effect": "Allow",
                        "Action": ["sts:AssumeRole"],
                        "Resource": ["arn:aws:s3:::*"]
                    }
                ]
            })
            .to_string(),
        ),
    )
    .await?;
    admin_ok(
        env,
        http::Method::POST,
        "/rustfs/admin/v3/idp/builtin/policy/attach",
        Some(serde_json::json!({ "policies": [policy], "user": user }).to_string()),
    )
    .await?;
    Ok(())
}

async fn create_service_account_for(
    env: &RustFSTestEnvironment,
    parent: &str,
) -> Result<(String, String), Box<dyn Error + Send + Sync>> {
    let response = admin_ok(
        env,
        http::Method::PUT,
        "/rustfs/admin/v3/add-service-accounts",
        Some(serde_json::json!({ "targetUser": parent }).to_string()),
    )
    .await?;
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

async fn assert_admin_status(
    env: &RustFSTestEnvironment,
    credentials: (&str, &str, Option<&str>),
    path: &str,
    body: String,
    expected: StatusCode,
    context: &str,
) -> TestResult {
    let (access_key, secret_key, session_token) = credentials;
    let (status, response) =
        admin_request_with_session_token(&env.url, http::Method::POST, path, Some(body), access_key, secret_key, session_token)
            .await?;
    assert_eq!(status, expected, "{context}: got {status}: {response}");
    if expected == StatusCode::FORBIDDEN {
        assert!(response.contains("AccessDenied"), "{context}: expected AccessDenied body, got {response}");
    }
    Ok(())
}

#[tokio::test]
async fn test_update_service_account_enforces_owner_and_parent_scope() -> TestResult {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let parent = "updateparent";
    let parent_secret = "updateparentsecret";
    let outsider = "updateoutsider";
    let outsider_secret = "updateoutsidersecret";
    let ordinary = "updateordinary";
    let ordinary_secret = "updateordinarysecret";
    create_user_with_service_account_update_policy(&env, parent, parent_secret, "update-parent-policy").await?;
    create_user_with_service_account_update_policy(&env, outsider, outsider_secret, "update-outsider-policy").await?;
    admin_ok(
        &env,
        http::Method::POST,
        "/rustfs/admin/v3/idp/builtin/policy/attach",
        Some(serde_json::json!({ "policies": ["consoleAdmin"], "user": outsider }).to_string()),
    )
    .await?;
    admin_ok(
        &env,
        http::Method::PUT,
        &format!("/rustfs/admin/v3/add-user?accessKey={ordinary}"),
        Some(serde_json::json!({ "secretKey": ordinary_secret, "status": "enabled" }).to_string()),
    )
    .await?;

    let (target_access_key, _) = create_service_account_for(&env, parent).await?;
    let target_path = format!("/rustfs/admin/v3/update-service-account?accessKey={target_access_key}");

    assert_admin_status(
        &env,
        (&env.access_key, &env.secret_key, None),
        &target_path,
        serde_json::json!({}).to_string(),
        StatusCode::NO_CONTENT,
        "root no-op update across parents must succeed",
    )
    .await?;

    let custom_policy = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Action": ["s3:GetObject"],
            "Resource": ["arn:aws:s3:::update-scope/*"]
        }]
    });
    assert_admin_status(
        &env,
        (&env.access_key, &env.secret_key, None),
        &target_path,
        serde_json::json!({ "newPolicy": custom_policy }).to_string(),
        StatusCode::NO_CONTENT,
        "root implied-to-custom update across parents must succeed",
    )
    .await?;

    assert_admin_status(
        &env,
        (parent, parent_secret, None),
        &target_path,
        serde_json::json!({ "newDescription": "updated by parent" }).to_string(),
        StatusCode::NO_CONTENT,
        "parent with UpdateServiceAccount may update its own service account",
    )
    .await?;

    let takeover = serde_json::json!({
        "newSecretKey": "cross-parent-takeover-secret",
        "newDescription": "cross-parent takeover"
    })
    .to_string();
    assert_admin_status(
        &env,
        (ordinary, ordinary_secret, None),
        &target_path,
        takeover.clone(),
        StatusCode::FORBIDDEN,
        "ordinary user must not update another parent's service account",
    )
    .await?;
    assert_admin_status(
        &env,
        (outsider, outsider_secret, None),
        &target_path,
        takeover.clone(),
        StatusCode::FORBIDDEN,
        "non-owner consoleAdmin must not update across parents",
    )
    .await?;

    let (derived_access_key, derived_secret_key) = create_service_account_for(&env, outsider).await?;
    assert_admin_status(
        &env,
        (&derived_access_key, &derived_secret_key, None),
        &target_path,
        takeover.clone(),
        StatusCode::FORBIDDEN,
        "service-account credential must not update across parents",
    )
    .await?;

    let assumed = build_test_sts_client(&env.url, outsider, outsider_secret, None, "e2e-admin-update-service-account")
        .assume_role()
        .role_arn("arn:aws:iam::123456789012:role/update-service-account")
        .role_session_name("update-service-account-scope")
        .send()
        .await?;
    let temporary = assumed
        .credentials()
        .ok_or("AssumeRole response should contain credentials")?;
    assert_admin_status(
        &env,
        (temporary.access_key_id(), temporary.secret_access_key(), Some(temporary.session_token())),
        &target_path,
        takeover,
        StatusCode::FORBIDDEN,
        "temporary credential must not update across parents",
    )
    .await?;

    let info = admin_ok(
        &env,
        http::Method::GET,
        &format!("/rustfs/admin/v3/info-service-account?accessKey={target_access_key}"),
        None,
    )
    .await?;
    let info: serde_json::Value = serde_json::from_str(&info)?;
    assert_eq!(
        info["impliedPolicy"].as_bool(),
        Some(false),
        "root update must replace the implied policy with a custom policy"
    );
    assert!(
        info["policy"].as_str().is_some_and(|policy| policy.contains("s3:GetObject")),
        "custom policy must round-trip through the handler: {info}"
    );
    assert_eq!(
        info["description"].as_str(),
        Some("updated by parent"),
        "denied takeover attempts must not mutate target"
    );

    let (missing_status, missing_body) = admin_request(
        &env.url,
        http::Method::POST,
        "/rustfs/admin/v3/update-service-account?accessKey=missing-service-account",
        Some(serde_json::json!({}).to_string()),
        &env.access_key,
        &env.secret_key,
    )
    .await?;
    assert_eq!(missing_status, StatusCode::NOT_FOUND, "missing target must fail closed: {missing_body}");
    assert!(
        missing_body.contains("NoSuchResource"),
        "missing target must preserve the lookup error: {missing_body}"
    );

    env.stop_server();
    Ok(())
}

/// Full user -> policy -> service-account lifecycle, proving each management
/// call takes effect on the data plane, not just that the endpoint answers 200.
#[tokio::test]
async fn test_admin_user_policy_service_account_crud_lifecycle() -> TestResult {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "peri2-crud";
    let user = "peri2user";
    let user_secret = "peri2usersecret";
    let policy = "peri2-rw";

    let root_client = env.create_s3_client();
    root_client.create_bucket().bucket(bucket).send().await?;

    // --- canned policy CRUD ---------------------------------------------------
    admin_ok(
        &env,
        http::Method::PUT,
        &format!("/rustfs/admin/v3/add-canned-policy?name={policy}"),
        Some(bucket_rw_policy(bucket)),
    )
    .await?;

    let info = admin_ok(
        &env,
        http::Method::GET,
        &format!("/rustfs/admin/v3/info-canned-policy?name={policy}"),
        None,
    )
    .await?;
    let info_json: serde_json::Value = serde_json::from_str(&info)?;
    let statement_json = info_json.get("Policy").cloned().unwrap_or(info_json.clone());
    assert!(
        statement_json.to_string().contains("s3:*"),
        "info-canned-policy must round-trip the policy document: {info}"
    );

    let listed = admin_ok(&env, http::Method::GET, "/rustfs/admin/v3/list-canned-policies", None).await?;
    assert!(listed.contains(policy), "list-canned-policies must contain {policy}: {listed}");

    // --- user CRUD --------------------------------------------------------------
    admin_ok(
        &env,
        http::Method::PUT,
        &format!("/rustfs/admin/v3/add-user?accessKey={user}"),
        Some(serde_json::json!({ "secretKey": user_secret, "status": "enabled" }).to_string()),
    )
    .await?;

    let users = admin_ok(&env, http::Method::GET, "/rustfs/admin/v3/list-users", None).await?;
    assert!(users.contains(user), "list-users must contain {user}: {users}");

    let user_info = admin_ok(&env, http::Method::GET, &format!("/rustfs/admin/v3/user-info?accessKey={user}"), None).await?;
    let user_info: rustfs_madmin::UserInfo = serde_json::from_str(&user_info)
        .map_err(|e| format!("user-info response must decode as rustfs_madmin::UserInfo: {e}"))?;
    assert_eq!(user_info.status, rustfs_madmin::AccountStatus::Enabled);

    // The fresh user is authenticatable but unauthorized for the bucket.
    let user_client = build_s3_client(&env.url, user, user_secret);
    let denied = user_client
        .put_object()
        .bucket(bucket)
        .key("before-attach")
        .body(ByteStream::from_static(b"x"))
        .send()
        .await;
    assert!(denied.is_err(), "user without a policy must not be able to write to {bucket}");

    // --- attach policy: the credential actually gains S3 access -----------------
    admin_ok(
        &env,
        http::Method::POST,
        "/rustfs/admin/v3/idp/builtin/policy/attach",
        Some(serde_json::json!({ "policies": [policy], "user": user }).to_string()),
    )
    .await?;

    wait_for_s3_put(&user_client, bucket, "after-attach.txt", Duration::from_secs(10)).await?;

    let user_info = admin_ok(&env, http::Method::GET, &format!("/rustfs/admin/v3/user-info?accessKey={user}"), None).await?;
    let user_info: rustfs_madmin::UserInfo = serde_json::from_str(&user_info)?;
    assert!(
        user_info.policy_name.as_deref().is_some_and(|p| p.contains(policy)),
        "user-info must reflect the attached policy, got {:?}",
        user_info.policy_name
    );

    // --- service account: create for the user, credential works, info/list pin it
    let sa_resp = admin_ok(
        &env,
        http::Method::PUT,
        "/rustfs/admin/v3/add-service-accounts",
        Some(serde_json::json!({ "targetUser": user }).to_string()),
    )
    .await?;
    let sa_json: serde_json::Value = serde_json::from_str(&sa_resp)?;
    let sa_ak = sa_json["credentials"]["accessKey"]
        .as_str()
        .ok_or(format!("add-service-accounts response missing credentials.accessKey: {sa_resp}"))?
        .to_string();
    let sa_sk = sa_json["credentials"]["secretKey"]
        .as_str()
        .ok_or(format!("add-service-accounts response missing credentials.secretKey: {sa_resp}"))?
        .to_string();

    let sa_client = build_s3_client(&env.url, &sa_ak, &sa_sk);
    wait_for_s3_put(&sa_client, bucket, "via-service-account.txt", Duration::from_secs(10)).await?;

    let sa_info = admin_ok(
        &env,
        http::Method::GET,
        &format!("/rustfs/admin/v3/info-service-account?accessKey={sa_ak}"),
        None,
    )
    .await?;
    let sa_info: serde_json::Value = serde_json::from_str(&sa_info)?;
    assert_eq!(
        sa_info["parentUser"].as_str(),
        Some(user),
        "info-service-account must name the parent user: {sa_info}"
    );

    let sa_list = admin_ok(
        &env,
        http::Method::GET,
        &format!("/rustfs/admin/v3/list-service-accounts?user={user}"),
        None,
    )
    .await?;
    let sa_list: rustfs_madmin::ListServiceAccountsResp = serde_json::from_str(&sa_list)
        .map_err(|e| format!("list-service-accounts must decode as rustfs_madmin::ListServiceAccountsResp: {e}"))?;
    assert!(
        sa_list.accounts.iter().any(|a| a.access_key == sa_ak),
        "list-service-accounts must contain {sa_ak}"
    );

    // --- deletion revokes access -------------------------------------------------
    admin_ok(
        &env,
        http::Method::DELETE,
        &format!("/rustfs/admin/v3/delete-service-account?accessKey={sa_ak}"),
        None,
    )
    .await?;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let revoked = sa_client
            .put_object()
            .bucket(bucket)
            .key("after-sa-delete")
            .body(ByteStream::from_static(b"x"))
            .send()
            .await;
        if revoked.is_err() {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            return Err("deleted service account credential still works".into());
        }
        sleep(Duration::from_millis(500)).await;
    }

    // Disable then remove the user; the credential must stop working.
    admin_ok(
        &env,
        http::Method::PUT,
        &format!("/rustfs/admin/v3/set-user-status?accessKey={user}&status=disabled"),
        None,
    )
    .await?;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let disabled = user_client
            .put_object()
            .bucket(bucket)
            .key("after-disable")
            .body(ByteStream::from_static(b"x"))
            .send()
            .await;
        if disabled.is_err() {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            return Err("disabled user credential still works".into());
        }
        sleep(Duration::from_millis(500)).await;
    }

    admin_ok(
        &env,
        http::Method::DELETE,
        &format!("/rustfs/admin/v3/remove-user?accessKey={user}"),
        None,
    )
    .await?;
    let users = admin_ok(&env, http::Method::GET, "/rustfs/admin/v3/list-users", None).await?;
    assert!(!users.contains(user), "removed user must disappear from list-users: {users}");

    admin_ok(
        &env,
        http::Method::DELETE,
        &format!("/rustfs/admin/v3/remove-canned-policy?name={policy}"),
        None,
    )
    .await?;
    let (status, _) = admin_request(
        &env.url,
        http::Method::GET,
        &format!("/rustfs/admin/v3/info-canned-policy?name={policy}"),
        None,
        &env.access_key,
        &env.secret_key,
    )
    .await?;
    assert!(!status.is_success(), "info-canned-policy must fail after remove-canned-policy");

    env.stop_server();
    Ok(())
}

/// Every management endpoint in the first batch must deny an authenticated but
/// non-admin credential with 403 AccessDenied (sec-4 assertion pattern; the
/// gate implementation itself is owned by sec-4 / admin_auth_test).
#[tokio::test]
async fn test_admin_iam_endpoints_deny_non_admin_credential() -> TestResult {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let limited = "peri2limited";
    let limited_secret = "peri2limitedsecret";
    let other = "peri2other";
    for (user, secret) in [(limited, limited_secret), (other, "peri2othersecret")] {
        admin_ok(
            &env,
            http::Method::PUT,
            &format!("/rustfs/admin/v3/add-user?accessKey={user}"),
            Some(serde_json::json!({ "secretKey": secret, "status": "enabled" }).to_string()),
        )
        .await?;
    }

    // Admin-only management endpoints. Targeted probes aim at a DIFFERENT user
    // (`other`): operations a principal performs on itself run in `deny_only`
    // mode (see should_check_deny_only in rustfs/src/admin/handlers/user.rs)
    // and would not be denied. Also deliberately excludes the self-service
    // account endpoints (add/list service accounts), which any authenticated
    // user may call for themselves.
    let probes: Vec<(http::Method, String, Option<String>)> = vec![
        (
            http::Method::PUT,
            "/rustfs/admin/v3/add-user?accessKey=peri2evil".to_string(),
            Some(serde_json::json!({ "secretKey": "evilsecret123", "status": "enabled" }).to_string()),
        ),
        (http::Method::GET, "/rustfs/admin/v3/list-users".to_string(), None),
        (http::Method::GET, format!("/rustfs/admin/v3/user-info?accessKey={other}"), None),
        (
            http::Method::PUT,
            format!("/rustfs/admin/v3/set-user-status?accessKey={other}&status=disabled"),
            None,
        ),
        (http::Method::DELETE, format!("/rustfs/admin/v3/remove-user?accessKey={other}"), None),
        (
            http::Method::PUT,
            "/rustfs/admin/v3/add-canned-policy?name=peri2evilpolicy".to_string(),
            Some(bucket_rw_policy("peri2-any")),
        ),
        (http::Method::GET, "/rustfs/admin/v3/list-canned-policies".to_string(), None),
        (http::Method::GET, "/rustfs/admin/v3/info-canned-policy?name=readwrite".to_string(), None),
        (
            http::Method::DELETE,
            "/rustfs/admin/v3/remove-canned-policy?name=readwrite".to_string(),
            None,
        ),
        (
            http::Method::POST,
            "/rustfs/admin/v3/idp/builtin/policy/attach".to_string(),
            Some(serde_json::json!({ "policies": ["readwrite"], "user": other }).to_string()),
        ),
    ];

    for (method, path, body) in probes {
        let (status, text) = admin_request(&env.url, method.clone(), &path, body, limited, limited_secret).await?;
        assert_eq!(
            status,
            StatusCode::FORBIDDEN,
            "non-admin credential must get 403 on {method} {path}, got {status}: {text}"
        );
        assert!(text.contains("AccessDenied"), "denial on {method} {path} must carry AccessDenied: {text}");
    }

    env.stop_server();
    Ok(())
}
