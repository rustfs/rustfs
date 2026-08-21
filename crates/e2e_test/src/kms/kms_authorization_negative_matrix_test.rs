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

//! Negative authorization matrix for per-key KMS access control.
//!
//! Every case here is an end-to-end denial that the pre-`kms` resource server
//! allowed, so a regression that reopens one of them fails this file rather than
//! only a unit test. The matrix varies one dimension at a time:
//!
//! - **wrong identity**: a caller holding S3 rights but no `kms` grant at all
//! - **wrong key**: a caller scoped to key A naming key B
//! - **wrong action**: a caller holding `kms:GenerateDataKey` but not `kms:Decrypt`
//!   (and, on the admin plane, `kms:DisableKey` but not `kms:RotateKey`)
//! - **wrong context**: an explicit `Deny` beating a wildcard `Allow`, and SSE-S3
//!   staying exempt from `kms` authorization
//!
//! Each matrix opens with a positive control. Without it a denial proves nothing:
//! an identity whose policy has not propagated yet is denied everything.

use super::common::{LocalKMSTestEnvironment, create_key_with_specific_id};
use crate::common::{admin_ok, admin_request, init_logging};
use aws_sdk_s3::Client;
use aws_sdk_s3::config::{Config, Credentials, Region};
use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::ServerSideEncryption;
use std::time::Duration;
use tracing::info;

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;
type S3OperationResult<T> = Result<T, Box<aws_sdk_s3::Error>>;

const ALLOWED_KEY: &str = "kms-matrix-allowed-key";
const OTHER_KEY: &str = "kms-matrix-other-key";
const BUCKET: &str = "kms-authz-matrix";
const SECRET: &str = "kms-matrix-secret";
const PAYLOAD: &[u8] = b"kms authorization matrix payload";

/// How long an identity change may take to reach the request path.
const IAM_PROPAGATION: Duration = Duration::from_secs(20);

fn s3_client(url: &str, access_key: &str, secret_key: &str) -> Client {
    let config = Config::builder()
        .credentials_provider(Credentials::new(access_key, secret_key, None, None, "kms-authz-matrix"))
        .region(Region::new("us-east-1"))
        .endpoint_url(url)
        .force_path_style(true)
        .behavior_version_latest()
        .build();
    Client::from_conf(config)
}

/// Start a server whose SSE-KMS data path authorizes against the named key.
///
/// The enforcement switch defaults to off for compatibility, so it has to be set
/// explicitly; without it every negative case below would silently pass as an allow.
async fn start_enforcing_server(env: &mut LocalKMSTestEnvironment, extra_env: &[(&str, &str)]) -> TestResult {
    create_key_with_specific_id(&env.kms_keys_dir, ALLOWED_KEY).await?;
    create_key_with_specific_id(&env.kms_keys_dir, OTHER_KEY).await?;

    let key_dir = env.kms_keys_dir.clone();
    let args = vec![
        "--kms-enable",
        "--kms-backend",
        "local",
        "--kms-key-dir",
        key_dir.as_str(),
        "--kms-default-key-id",
        ALLOWED_KEY,
    ];

    let mut envs = vec![("RUSTFS_KMS_ALLOW_INSECURE_DEV_DEFAULTS", "true")];
    envs.extend_from_slice(extra_env);

    env.base_env.start_rustfs_server_with_env(args, &envs).await?;
    Ok(())
}

/// Create `user` with `policy_document` attached under a canned policy of the same name.
async fn provision_user(env: &LocalKMSTestEnvironment, user: &str, policy_document: &str) -> TestResult {
    admin_ok(
        &env.base_env,
        http::Method::PUT,
        &format!("/rustfs/admin/v3/add-canned-policy?name={user}"),
        Some(policy_document.to_string()),
    )
    .await?;
    provision_user_with_policy(env, user, user).await
}

/// Create `user` and attach an existing policy (built-in or canned) by name.
async fn provision_user_with_policy(env: &LocalKMSTestEnvironment, user: &str, policy_name: &str) -> TestResult {
    admin_ok(
        &env.base_env,
        http::Method::PUT,
        &format!("/rustfs/admin/v3/add-user?accessKey={user}"),
        Some(serde_json::json!({ "secretKey": SECRET, "status": "enabled" }).to_string()),
    )
    .await?;
    admin_ok(
        &env.base_env,
        http::Method::PUT,
        &format!("/rustfs/admin/v3/set-user-or-group-policy?policyName={policy_name}&userOrGroup={user}&isGroup=false"),
        None,
    )
    .await?;
    Ok(())
}

/// The S3 half of every data-path policy below: full object access, no KMS grant.
fn s3_full_access_statement() -> serde_json::Value {
    serde_json::json!({
        "Effect": "Allow",
        "Action": ["s3:*"],
        "Resource": ["arn:aws:s3:::*"]
    })
}

fn policy_document(statements: Vec<serde_json::Value>) -> String {
    serde_json::json!({ "Version": "2012-10-17", "Statement": statements }).to_string()
}

async fn put_sse_kms(client: &Client, key: &str, kms_key_id: &str) -> S3OperationResult<()> {
    client
        .put_object()
        .bucket(BUCKET)
        .key(key)
        .body(ByteStream::from_static(PAYLOAD))
        .server_side_encryption(ServerSideEncryption::AwsKms)
        .ssekms_key_id(kms_key_id)
        .send()
        .await
        .map(|_| ())
        .map_err(|error| Box::new(aws_sdk_s3::Error::from(error)))
}

/// Assert the operation failed with `AccessDenied` rather than any other error.
///
/// A bare `is_err` would also accept `KMSKeyDisabled` or an internal error, which
/// would hide both a leak of key state and an outage masquerading as a denial.
fn assert_access_denied<T: std::fmt::Debug, E: std::fmt::Debug + std::borrow::Borrow<aws_sdk_s3::Error>>(
    result: Result<T, E>,
    what: &str,
) {
    let error = result.expect_err(&format!("{what} must be denied"));
    assert_eq!(
        error.borrow().code(),
        Some("AccessDenied"),
        "{what} must fail with AccessDenied: {error:?}"
    );
}

/// Retry an SSE-KMS write until the identity's policy has reached the request path.
async fn wait_for_sse_kms_write(client: &Client, key: &str, kms_key_id: &str) -> TestResult {
    let deadline = tokio::time::Instant::now() + IAM_PROPAGATION;
    loop {
        match put_sse_kms(client, key, kms_key_id).await {
            Ok(()) => return Ok(()),
            Err(error) if tokio::time::Instant::now() >= deadline => {
                return Err(format!("positive control never became authorized: {error:?}").into());
            }
            Err(_) => tokio::time::sleep(Duration::from_millis(500)).await,
        }
    }
}

/// Retry an admin call until it stops returning 403, i.e. the policy is live.
async fn wait_for_admin_success(
    env: &LocalKMSTestEnvironment,
    user: &str,
    method: http::Method,
    path: &str,
    body: Option<String>,
) -> TestResult {
    let deadline = tokio::time::Instant::now() + IAM_PROPAGATION;
    loop {
        let (status, response) = admin_request(&env.base_env.url, method.clone(), path, body.clone(), user, SECRET).await?;
        if status.is_success() {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(format!("positive control never became authorized: {method} {path} -> {status} {response}").into());
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

async fn assert_admin_denied(
    env: &LocalKMSTestEnvironment,
    user: &str,
    method: http::Method,
    path: &str,
    body: Option<String>,
    what: &str,
) -> TestResult {
    let (status, response) = admin_request(&env.base_env.url, method, path, body, user, SECRET).await?;
    assert_eq!(status.as_u16(), 403, "{what} must be denied, got {status}: {response}");
    assert!(response.contains("AccessDenied"), "{what} must carry AccessDenied: {response}");
    Ok(())
}

fn disable_body(key_id: &str) -> String {
    serde_json::json!({ "key_id": key_id }).to_string()
}

/// Data-path matrix: SSE-KMS writes and reads are authorized against the resolved key.
#[tokio::test]
async fn sse_kms_per_key_authorization_negative_matrix() -> TestResult {
    init_logging();

    let mut env = LocalKMSTestEnvironment::new().await?;
    start_enforcing_server(&mut env, &[("RUSTFS_KMS_ENFORCE_SSE_KEY_POLICY", "true")]).await?;
    env.base_env.create_test_bucket(BUCKET).await?;

    // Scoped to ALLOWED_KEY only.
    provision_user(
        &env,
        "kmsmatrixscoped",
        &policy_document(vec![
            s3_full_access_statement(),
            serde_json::json!({
                "Effect": "Allow",
                "Action": ["kms:GenerateDataKey", "kms:Decrypt"],
                "Resource": [format!("arn:aws:kms:::key/{ALLOWED_KEY}")]
            }),
        ]),
    )
    .await?;
    // S3 rights only: the identity shape that existed before per-key authorization.
    provision_user(&env, "kmsmatrixs3only", &policy_document(vec![s3_full_access_statement()])).await?;
    // May wrap a data key but may never unwrap one.
    provision_user(
        &env,
        "kmsmatrixwriter",
        &policy_document(vec![
            s3_full_access_statement(),
            serde_json::json!({
                "Effect": "Allow",
                "Action": ["kms:GenerateDataKey"],
                "Resource": ["arn:aws:kms:::*"]
            }),
        ]),
    )
    .await?;
    // Wildcard allow, explicit deny on one key.
    provision_user(
        &env,
        "kmsmatrixdenied",
        &policy_document(vec![
            s3_full_access_statement(),
            serde_json::json!({
                "Effect": "Allow",
                "Action": ["kms:*"],
                "Resource": ["arn:aws:kms:::*"]
            }),
            serde_json::json!({
                "Effect": "Deny",
                "Action": ["kms:*"],
                "Resource": [format!("arn:aws:kms:::key/{OTHER_KEY}")]
            }),
        ]),
    )
    .await?;

    let scoped = s3_client(&env.base_env.url, "kmsmatrixscoped", SECRET);
    let s3_only = s3_client(&env.base_env.url, "kmsmatrixs3only", SECRET);
    let writer = s3_client(&env.base_env.url, "kmsmatrixwriter", SECRET);
    let denied = s3_client(&env.base_env.url, "kmsmatrixdenied", SECRET);

    // --- positive control -----------------------------------------------------
    wait_for_sse_kms_write(&scoped, "scoped/allowed", ALLOWED_KEY).await?;
    let read = scoped.get_object().bucket(BUCKET).key("scoped/allowed").send().await?;
    assert_eq!(read.body.collect().await?.into_bytes().as_ref(), PAYLOAD);
    info!("positive control: scoped identity may write and read under its own key");

    // --- wrong key ------------------------------------------------------------
    assert_access_denied(
        put_sse_kms(&scoped, "scoped/other", OTHER_KEY).await,
        "SSE-KMS write under a key outside the identity's scope",
    );

    // --- wrong identity -------------------------------------------------------
    assert_access_denied(
        put_sse_kms(&s3_only, "s3only/allowed", ALLOWED_KEY).await,
        "SSE-KMS write by an identity holding no kms grant",
    );
    // The object the scoped identity wrote is readable by its owner only.
    assert_access_denied(
        s3_only
            .get_object()
            .bucket(BUCKET)
            .key("scoped/allowed")
            .send()
            .await
            .map(|_| ())
            .map_err(|err| Box::new(aws_sdk_s3::Error::from(err))),
        "SSE-KMS read by an identity holding no kms grant",
    );

    // --- wrong action ---------------------------------------------------------
    wait_for_sse_kms_write(&writer, "writer/allowed", ALLOWED_KEY).await?;
    assert_access_denied(
        writer
            .get_object()
            .bucket(BUCKET)
            .key("writer/allowed")
            .send()
            .await
            .map(|_| ())
            .map_err(|err| Box::new(aws_sdk_s3::Error::from(err))),
        "SSE-KMS read by an identity holding kms:GenerateDataKey but not kms:Decrypt",
    );

    // --- wrong context: explicit Deny beats a wildcard Allow -------------------
    wait_for_sse_kms_write(&denied, "denied/allowed", ALLOWED_KEY).await?;
    assert_access_denied(
        put_sse_kms(&denied, "denied/other", OTHER_KEY).await,
        "SSE-KMS write under a key covered by an explicit Deny",
    );

    // --- wrong context: SSE-S3 is out of scope --------------------------------
    // SSE-S3 wraps its data key with a server-owned key the caller never names, so
    // it must stay reachable for an identity with no kms grant at all.
    s3_only
        .put_object()
        .bucket(BUCKET)
        .key("s3only/sse-s3")
        .body(ByteStream::from_static(PAYLOAD))
        .server_side_encryption(ServerSideEncryption::Aes256)
        .send()
        .await?;
    let sse_s3_read = s3_only.get_object().bucket(BUCKET).key("s3only/sse-s3").send().await?;
    assert_eq!(sse_s3_read.body.collect().await?.into_bytes().as_ref(), PAYLOAD);

    // ... and so must an unencrypted object.
    s3_only
        .put_object()
        .bucket(BUCKET)
        .key("s3only/plain")
        .body(ByteStream::from_static(PAYLOAD))
        .send()
        .await?;
    s3_only.get_object().bucket(BUCKET).key("s3only/plain").send().await?;

    Ok(())
}

/// Admin-plane matrix: KMS key endpoints are authorized against the key they name.
///
/// Runs without the SSE enforcement switch: admin scoping is unconditional, and
/// leaving the switch off proves the two planes are independent.
#[tokio::test]
async fn kms_admin_per_key_authorization_negative_matrix() -> TestResult {
    init_logging();

    let mut env = LocalKMSTestEnvironment::new().await?;
    start_enforcing_server(&mut env, &[]).await?;

    // Built-in role templates, attached by name.
    provision_user_with_policy(&env, "kmsmatrixkeyadmin", "KMSKeyAdministrator").await?;
    provision_user_with_policy(&env, "kmsmatrixauditor", "KMSAuditor").await?;
    // A narrowed copy of the administrator template, scoped to one key.
    provision_user(
        &env,
        "kmsmatrixscopedadmin",
        &policy_document(vec![serde_json::json!({
            "Effect": "Allow",
            "Action": ["kms:DisableKey", "kms:EnableKey"],
            "Resource": [format!("arn:aws:kms:::key/{ALLOWED_KEY}")]
        })]),
    )
    .await?;

    // --- positive control -----------------------------------------------------
    wait_for_admin_success(
        &env,
        "kmsmatrixkeyadmin",
        http::Method::POST,
        "/rustfs/admin/v3/kms/keys/disable",
        Some(disable_body(OTHER_KEY)),
    )
    .await?;
    admin_request(
        &env.base_env.url,
        http::Method::POST,
        "/rustfs/admin/v3/kms/keys/enable",
        Some(disable_body(OTHER_KEY)),
        "kmsmatrixkeyadmin",
        SECRET,
    )
    .await?;

    // --- wrong action: the administrator template withholds service-wide powers -
    assert_admin_denied(
        &env,
        "kmsmatrixkeyadmin",
        http::Method::GET,
        "/rustfs/admin/v3/kms/config",
        None,
        "KMSKeyAdministrator reading the KMS backend configuration (kms:Configure)",
    )
    .await?;
    assert_admin_denied(
        &env,
        "kmsmatrixkeyadmin",
        http::Method::GET,
        "/rustfs/admin/v3/kms/backup",
        None,
        "KMSKeyAdministrator exporting a backup bundle (kms:Backup)",
    )
    .await?;
    // Separation of duties: managing a key never implies using it.
    assert_admin_denied(
        &env,
        "kmsmatrixkeyadmin",
        http::Method::POST,
        "/rustfs/admin/v3/kms/generate-data-key",
        Some(serde_json::json!({ "key_id": ALLOWED_KEY }).to_string()),
        "KMSKeyAdministrator generating a data key (kms:GenerateDataKey)",
    )
    .await?;

    // --- wrong action: the auditor template is read-only ----------------------
    wait_for_admin_success(&env, "kmsmatrixauditor", http::Method::GET, "/rustfs/admin/v3/kms/keys", None).await?;
    assert_admin_denied(
        &env,
        "kmsmatrixauditor",
        http::Method::POST,
        "/rustfs/admin/v3/kms/keys/disable",
        Some(disable_body(ALLOWED_KEY)),
        "KMSAuditor disabling a key (kms:DisableKey)",
    )
    .await?;

    // --- wrong key ------------------------------------------------------------
    wait_for_admin_success(
        &env,
        "kmsmatrixscopedadmin",
        http::Method::POST,
        "/rustfs/admin/v3/kms/keys/disable",
        Some(disable_body(ALLOWED_KEY)),
    )
    .await?;
    assert_admin_denied(
        &env,
        "kmsmatrixscopedadmin",
        http::Method::POST,
        "/rustfs/admin/v3/kms/keys/disable",
        Some(disable_body(OTHER_KEY)),
        "key-scoped administrator disabling a key outside its scope",
    )
    .await?;
    // --- wrong action, same key ----------------------------------------------
    assert_admin_denied(
        &env,
        "kmsmatrixscopedadmin",
        http::Method::POST,
        "/rustfs/admin/v3/kms/keys/rotate",
        Some(disable_body(ALLOWED_KEY)),
        "key-scoped administrator rotating a key it may only enable and disable",
    )
    .await?;

    // --- wrong identity -------------------------------------------------------
    provision_user(&env, "kmsmatrixnokms", &policy_document(vec![s3_full_access_statement()])).await?;
    assert_admin_denied(
        &env,
        "kmsmatrixnokms",
        http::Method::POST,
        "/rustfs/admin/v3/kms/keys/disable",
        Some(disable_body(ALLOWED_KEY)),
        "identity holding no kms grant disabling a key",
    )
    .await?;

    Ok(())
}
