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
use aws_sdk_sts::Client;
use aws_sdk_sts::error::ProvideErrorMetadata;
use aws_sdk_sts::operation::RequestId;
use bytes::Bytes;
use http::header::{AUTHORIZATION, CONTENT_TYPE};
use http::{Request, Response};
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use serde_json::Value;
use std::collections::BTreeSet;
use std::convert::Infallible;
use std::error::Error;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::{Duration, timeout};

type BoxError = Box<dyn Error + Send + Sync>;
type TestResult = Result<(), BoxError>;
const OPA_AUTH_TOKEN: &str = "sts-opa-token";

fn sts_client(url: &str, access_key: &str, secret_key: &str, session_token: Option<&str>) -> Client {
    build_test_sts_client(url, access_key, secret_key, session_token, "e2e-sts-query-compat")
}

async fn create_root_service_account(env: &RustFSTestEnvironment) -> Result<(String, String), BoxError> {
    let body = admin_ok(
        env,
        http::Method::PUT,
        "/rustfs/admin/v3/add-service-accounts",
        Some(serde_json::json!({ "targetUser": env.access_key.clone() }).to_string()),
    )
    .await?;
    let response: Value = serde_json::from_str(&body)?;
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

async fn create_user_with_policy(
    env: &RustFSTestEnvironment,
    user: &str,
    secret: &str,
    policy_name: &str,
    statements: Value,
) -> TestResult {
    create_user(env, user, secret).await?;
    admin_ok(
        env,
        http::Method::PUT,
        &format!("/rustfs/admin/v3/add-canned-policy?name={policy_name}"),
        Some(
            serde_json::json!({
                "Version": "2012-10-17",
                "Statement": statements,
            })
            .to_string(),
        ),
    )
    .await?;
    admin_ok(
        env,
        http::Method::POST,
        "/rustfs/admin/v3/idp/builtin/policy/attach",
        Some(serde_json::json!({ "policies": [policy_name], "user": user }).to_string()),
    )
    .await?;
    Ok(())
}

async fn create_user(env: &RustFSTestEnvironment, user: &str, secret: &str) -> TestResult {
    admin_ok(
        env,
        http::Method::PUT,
        &format!("/rustfs/admin/v3/add-user?accessKey={user}"),
        Some(serde_json::json!({ "secretKey": secret, "status": "enabled" }).to_string()),
    )
    .await?;
    Ok(())
}

async fn assert_access_denied(client: &Client, context: &str) -> TestResult {
    let error = client
        .assume_role()
        .role_arn("arn:aws:iam::123456789012:role/test")
        .role_session_name("sts-query-compat-e2e")
        .send()
        .await
        .expect_err("AssumeRole must be denied");
    let service_error = error
        .as_service_error()
        .ok_or_else(|| format!("{context} should deserialize as an STS service error: {error:?}"))?;

    assert_eq!(error.raw_response().map(|response| response.status().as_u16()), Some(403));
    assert_eq!(service_error.code(), Some("AccessDenied"));
    assert_eq!(service_error.message(), Some("Access Denied"));
    assert!(
        error.request_id().is_some_and(|request_id| !request_id.is_empty()),
        "{context} should include a request ID"
    );
    Ok(())
}

async fn assert_list_buckets_access_denied(
    env: &RustFSTestEnvironment,
    access_key: &str,
    secret_key: &str,
    context: &str,
) -> TestResult {
    let error = aws_sdk_s3::Client::from_conf(build_test_s3_config(
        &env.url,
        access_key,
        secret_key,
        None,
        "e2e-list-buckets-opa-unavailable",
    ))
    .list_buckets()
    .send()
    .await
    .expect_err("ListBuckets must be denied while OPA is unavailable");
    let service_error = error
        .as_service_error()
        .ok_or_else(|| format!("{context} should deserialize as an S3 service error: {error:?}"))?;

    assert_eq!(error.raw_response().map(|response| response.status().as_u16()), Some(403));
    assert_eq!(service_error.code(), Some("AccessDenied"));
    Ok(())
}

async fn assert_opa_unavailable_denies_sts_and_list_buckets(env: &RustFSTestEnvironment, context: &str) -> TestResult {
    let user = "opaunavailable";
    let secret = "stsOpaUnavailableSecret123";
    create_user_with_policy(
        env,
        user,
        secret,
        "sts-opa-unavailable-local-policy",
        serde_json::json!([{
            "Effect": "Allow",
            "Action": ["s3:ListAllMyBuckets"],
            "Resource": ["arn:aws:s3:::*"],
        }]),
    )
    .await?;

    assert_access_denied(&sts_client(&env.url, user, secret, None), context).await?;
    assert_list_buckets_access_denied(env, user, secret, context).await
}

async fn handle_opa_request(
    request: Request<Incoming>,
    requests: mpsc::UnboundedSender<Value>,
    validation_started: mpsc::UnboundedSender<()>,
    validation_mode: OpaValidationMode,
    expected_authorization: Option<String>,
) -> Result<Response<Full<Bytes>>, Infallible> {
    if let Some(expected_authorization) = expected_authorization
        && request.headers().get(AUTHORIZATION).and_then(|value| value.to_str().ok()) != Some(expected_authorization.as_str())
    {
        return Ok(Response::builder()
            .status(401)
            .body(Full::new(Bytes::new()))
            .expect("static OPA unauthorized response must be valid"));
    }

    let body = match request.into_body().collect().await {
        Ok(body) => body.to_bytes(),
        Err(error) => {
            return Ok(Response::builder()
                .status(400)
                .body(Full::new(Bytes::from(error.to_string())))
                .expect("static OPA error response must be valid"));
        }
    };

    let payload = if body.is_empty() {
        None
    } else {
        match serde_json::from_slice::<Value>(&body) {
            Ok(payload) => Some(payload),
            Err(error) => {
                return Ok(Response::builder()
                    .status(400)
                    .body(Full::new(Bytes::from(error.to_string())))
                    .expect("static OPA error response must be valid"));
            }
        }
    };
    if payload.is_none() {
        let _ = validation_started.send(());
        match validation_mode {
            OpaValidationMode::Blocked => std::future::pending::<()>().await,
            OpaValidationMode::Unavailable => {
                return Ok(Response::builder()
                    .status(503)
                    .body(Full::new(Bytes::new()))
                    .expect("static OPA unavailable response must be valid"));
            }
            OpaValidationMode::Ready => {}
        }
    }
    let allow = match payload.as_ref().and_then(|value| value.pointer("/input/identity/account")) {
        Some(Value::String(account)) if account == "opaallow" => payload
            .as_ref()
            .and_then(|value| value.pointer("/input/context/deny_only"))
            .and_then(Value::as_bool)
            .unwrap_or(false),
        Some(Value::String(account)) if account == "opadeny" => false,
        Some(Value::String(account))
            if account == "opaunavailable" && matches!(validation_mode, OpaValidationMode::Unavailable) =>
        {
            true
        }
        Some(Value::String(account)) if account == "opalistbuckets" => {
            let action = payload
                .as_ref()
                .and_then(|value| value.pointer("/input/action"))
                .and_then(Value::as_str);
            let bucket = payload
                .as_ref()
                .and_then(|value| value.pointer("/input/resource/bucket"))
                .and_then(Value::as_str);
            matches!(
                (action, bucket),
                (Some("s3:ListBucket"), Some("opa-list-visible")) | (Some("s3:GetBucketLocation"), Some("opa-list-location"))
            )
        }
        None => true,
        _ => false,
    };
    if let Some(payload) = payload {
        let _ = requests.send(payload);
    }
    let body =
        serde_json::to_vec(&serde_json::json!({ "result": { "allow": allow } })).expect("static OPA response must serialize");
    Ok(Response::builder()
        .header(CONTENT_TYPE, "application/json")
        .body(Full::new(Bytes::from(body)))
        .expect("static OPA response must be valid"))
}

#[derive(Clone, Copy)]
enum OpaValidationMode {
    Ready,
    Blocked,
    Unavailable,
}

struct OpaMock {
    url: String,
    requests: mpsc::UnboundedReceiver<Value>,
    validation_started: mpsc::UnboundedReceiver<()>,
    task: JoinHandle<()>,
}

impl OpaMock {
    async fn start() -> Result<Self, BoxError> {
        Self::start_with_mode(OpaValidationMode::Ready, Some(OPA_AUTH_TOKEN)).await
    }

    async fn start_blocked() -> Result<Self, BoxError> {
        Self::start_with_mode(OpaValidationMode::Blocked, None).await
    }

    async fn start_with_mode(validation_mode: OpaValidationMode, auth_token: Option<&str>) -> Result<Self, BoxError> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("http://{}/v1/data/rustfs/authz/allow", listener.local_addr()?);
        let (requests_tx, requests) = mpsc::unbounded_channel();
        let (validation_started_tx, validation_started) = mpsc::unbounded_channel();
        let expected_authorization = auth_token.map(|token| format!("Bearer {token}"));
        let task = tokio::spawn(async move {
            let mut connections = JoinSet::new();
            loop {
                tokio::select! {
                    accepted = listener.accept() => {
                        let Ok((stream, _)) = accepted else { break };
                        let requests = requests_tx.clone();
                        let validation_started = validation_started_tx.clone();
                        let validation_mode = validation_mode;
                        let expected_authorization = expected_authorization.clone();
                        connections.spawn(async move {
                            let handler = service_fn(move |request| {
                                handle_opa_request(
                                    request,
                                    requests.clone(),
                                    validation_started.clone(),
                                    validation_mode,
                                    expected_authorization.clone(),
                                )
                            });
                            let _ = http1::Builder::new()
                                .serve_connection(TokioIo::new(stream), handler)
                                .await;
                        });
                    }
                    _ = connections.join_next(), if !connections.is_empty() => {}
                }
            }
        });
        Ok(Self {
            url,
            requests,
            validation_started,
            task,
        })
    }

    async fn next_request(&mut self) -> Result<Value, BoxError> {
        timeout(Duration::from_secs(5), self.requests.recv())
            .await?
            .ok_or_else(|| "OPA request channel closed".into())
    }

    async fn wait_for_validation(&mut self) -> TestResult {
        timeout(Duration::from_secs(5), self.validation_started.recv())
            .await?
            .ok_or_else(|| "OPA validation channel closed".into())
    }
}

impl Drop for OpaMock {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[tokio::test]
async fn test_sts_query_responses_are_aws_sdk_compatible() -> TestResult {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let assumed = sts_client(&env.url, &env.access_key, &env.secret_key, None)
        .assume_role()
        .role_arn("arn:aws:iam::123456789012:role/test")
        .role_session_name("sts-query-compat-e2e")
        .send()
        .await?;
    assert!(
        assumed.request_id().is_some_and(|request_id| !request_id.is_empty()),
        "successful AssumeRole should include a request ID"
    );
    let temporary = assumed
        .credentials()
        .ok_or("successful AssumeRole response should contain credentials")?;

    let invalid_signature = sts_client(&env.url, &env.access_key, "incorrect-secret-key", None)
        .assume_role()
        .role_arn("arn:aws:iam::123456789012:role/test")
        .role_session_name("sts-query-invalid-signature")
        .send()
        .await
        .expect_err("an invalid signature must be rejected");
    assert_eq!(invalid_signature.raw_response().map(|response| response.status().as_u16()), Some(403));
    let invalid_signature_service_error = invalid_signature
        .as_service_error()
        .ok_or_else(|| format!("invalid signature should deserialize as an STS service error: {invalid_signature:?}"))?;
    assert_eq!(invalid_signature_service_error.code(), Some("SignatureDoesNotMatch"));
    assert!(
        invalid_signature_service_error
            .message()
            .is_some_and(|message| message.starts_with("The request signature we calculated does not match")),
        "signature rejection should preserve the canonical error message"
    );
    assert!(
        invalid_signature
            .request_id()
            .is_some_and(|request_id| !request_id.is_empty()),
        "signature rejection should include a request ID"
    );

    assert_access_denied(
        &sts_client(
            &env.url,
            temporary.access_key_id(),
            temporary.secret_access_key(),
            Some(temporary.session_token()),
        ),
        "temporary credential",
    )
    .await?;

    let (service_access_key, service_secret_key) = create_root_service_account(&env).await?;
    assert_access_denied(
        &sts_client(&env.url, &service_access_key, &service_secret_key, None),
        "service-account denial",
    )
    .await?;

    let implicit_user = "stsimplicit";
    let explicit_allow_user = "stsallow";
    let explicit_deny_user = "stsdeny";
    let policyless_user = "stspolicyless";
    let secret = "stsAuthzSecret123";
    create_user(&env, policyless_user, secret).await?;
    assert_access_denied(&sts_client(&env.url, policyless_user, secret, None), "policyless user").await?;
    create_user_with_policy(
        &env,
        implicit_user,
        secret,
        "sts-implicit-policy",
        serde_json::json!([{
            "Effect": "Allow",
            "Action": ["s3:ListAllMyBuckets"],
            "Resource": ["arn:aws:s3:::*"],
        }]),
    )
    .await?;
    create_user_with_policy(
        &env,
        explicit_allow_user,
        secret,
        "sts-allow-policy",
        serde_json::json!([{
            "Effect": "Allow",
            "Action": ["sts:AssumeRole"],
            "Resource": ["arn:aws:s3:::*"],
        }]),
    )
    .await?;
    create_user_with_policy(
        &env,
        explicit_deny_user,
        secret,
        "sts-deny-policy",
        serde_json::json!([
            {
                "Effect": "Allow",
                "Action": ["sts:AssumeRole"],
                "Resource": ["arn:aws:s3:::*"],
            },
            {
                "Effect": "Deny",
                "Action": ["sts:AssumeRole"],
                "Resource": ["arn:aws:s3:::*"],
            }
        ]),
    )
    .await?;

    for user in [implicit_user, explicit_allow_user] {
        let output = sts_client(&env.url, user, secret, None)
            .assume_role()
            .role_arn("arn:aws:iam::123456789012:role/test")
            .role_session_name("sts-authz-e2e")
            .send()
            .await
            .map_err(|error| format!("{user} should be allowed to call AssumeRole: {error:?}"))?;
        let credentials = output
            .credentials()
            .ok_or_else(|| format!("{user} AssumeRole response should contain credentials"))?;
        assert!(!credentials.access_key_id().is_empty());
        assert!(!credentials.secret_access_key().is_empty());
        assert!(!credentials.session_token().is_empty());
    }
    assert_access_denied(&sts_client(&env.url, explicit_deny_user, secret, None), "explicit sts:AssumeRole Deny").await?;

    env.stop_server();
    Ok(())
}

#[tokio::test]
async fn test_sts_assume_role_opa_contract() -> TestResult {
    init_logging();

    let mut opa = OpaMock::start().await?;
    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server_with_env(
        vec![],
        &[
            ("RUSTFS_POLICY_PLUGIN_URL", opa.url.as_str()),
            ("RUSTFS_POLICY_PLUGIN_AUTH_TOKEN", OPA_AUTH_TOKEN),
        ],
    )
    .await?;

    let secret = "stsOpaSecret123";
    create_user_with_policy(
        &env,
        "opaallow",
        secret,
        "sts-opa-local-deny-policy",
        serde_json::json!([{
            "Effect": "Deny",
            "Action": ["sts:AssumeRole"],
            "Resource": ["arn:aws:s3:::*"],
        }]),
    )
    .await?;
    create_user_with_policy(
        &env,
        "opadeny",
        secret,
        "sts-opa-local-allow-policy",
        serde_json::json!([{
            "Effect": "Allow",
            "Action": ["sts:AssumeRole"],
            "Resource": ["arn:aws:s3:::*"],
        }]),
    )
    .await?;

    sts_client(&env.url, "opaallow", secret, None)
        .assume_role()
        .role_arn("arn:aws:iam::123456789012:role/test")
        .role_session_name("sts-opa-contract")
        .send()
        .await
        .map_err(|error| format!("OPA allow should override the local explicit Deny: {error:?}"))?;
    assert_access_denied(
        &sts_client(&env.url, "opadeny", secret, None),
        "OPA denial despite local sts:AssumeRole Allow",
    )
    .await?;

    let mut accounts = BTreeSet::new();
    for _ in 0..2 {
        let request = opa.next_request().await?;
        assert_eq!(request.pointer("/input/action").and_then(Value::as_str), Some("sts:AssumeRole"));
        assert_eq!(request.pointer("/input/context/deny_only").and_then(Value::as_bool), Some(true));
        let account = request
            .pointer("/input/identity/account")
            .and_then(Value::as_str)
            .ok_or("OPA input should include identity.account")?;
        accounts.insert(account.to_owned());
    }
    assert_eq!(accounts, BTreeSet::from(["opaallow".to_owned(), "opadeny".to_owned()]));

    env.stop_server();
    Ok(())
}

#[tokio::test]
async fn test_list_buckets_opa_contract() -> TestResult {
    init_logging();

    let mut opa = OpaMock::start().await?;
    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server_with_env(
        vec![],
        &[
            ("RUSTFS_POLICY_PLUGIN_URL", opa.url.as_str()),
            ("RUSTFS_POLICY_PLUGIN_AUTH_TOKEN", OPA_AUTH_TOKEN),
        ],
    )
    .await?;

    let admin_client = env.create_s3_client();
    for bucket in ["opa-list-hidden", "opa-list-location", "opa-list-visible"] {
        admin_client.create_bucket().bucket(bucket).send().await?;
    }

    let user = "opalistbuckets";
    let secret = "opaListBucketsSecret123";
    create_user(&env, user, secret).await?;

    let output = aws_sdk_s3::Client::from_conf(build_test_s3_config(&env.url, user, secret, None, "e2e-list-buckets-opa"))
        .list_buckets()
        .send()
        .await?;
    let mut names = output
        .buckets()
        .iter()
        .filter_map(|bucket| bucket.name().map(str::to_owned))
        .collect::<Vec<_>>();
    names.sort();
    assert_eq!(names, ["opa-list-location", "opa-list-visible"]);

    let mut evaluations = BTreeSet::new();
    for _ in 0..6 {
        let request = opa.next_request().await?;
        assert_eq!(request.pointer("/input/identity/account").and_then(Value::as_str), Some(user));
        assert_eq!(request.pointer("/input/context/deny_only").and_then(Value::as_bool), Some(false));

        let action = request
            .pointer("/input/action")
            .and_then(Value::as_str)
            .ok_or("OPA ListBuckets input should include action")?;
        let bucket = request
            .pointer("/input/resource/bucket")
            .and_then(Value::as_str)
            .ok_or("OPA ListBuckets input should include resource.bucket")?;
        if bucket.is_empty() {
            assert_eq!(action, "s3:ListAllMyBuckets");
            assert!(request.pointer("/input/context/conditions/prefix").is_none());
            assert!(request.pointer("/input/context/conditions/delimiter").is_none());
        } else {
            let expected_arn = format!("arn:aws:s3:::{bucket}");
            assert_eq!(request.pointer("/input/context/conditions/prefix"), Some(&serde_json::json!([""])));
            assert_eq!(request.pointer("/input/context/conditions/delimiter"), Some(&serde_json::json!(["/"])));
            assert_eq!(
                request.pointer("/input/resource/arn").and_then(Value::as_str),
                Some(expected_arn.as_str())
            );
        }
        evaluations.insert((action.to_owned(), bucket.to_owned()));
    }
    assert_eq!(
        evaluations,
        BTreeSet::from([
            ("s3:GetBucketLocation".to_owned(), "opa-list-hidden".to_owned()),
            ("s3:GetBucketLocation".to_owned(), "opa-list-location".to_owned()),
            ("s3:ListAllMyBuckets".to_owned(), String::new()),
            ("s3:ListBucket".to_owned(), "opa-list-hidden".to_owned()),
            ("s3:ListBucket".to_owned(), "opa-list-location".to_owned()),
            ("s3:ListBucket".to_owned(), "opa-list-visible".to_owned()),
        ])
    );
    assert!(
        matches!(opa.requests.try_recv(), Err(mpsc::error::TryRecvError::Empty)),
        "ListBuckets should not make redundant OPA evaluations"
    );

    env.stop_server();
    Ok(())
}

#[tokio::test]
async fn test_sts_and_list_buckets_fail_closed_while_opa_is_initializing() -> TestResult {
    init_logging();

    let mut opa = OpaMock::start_blocked().await?;
    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server_with_env(vec![], &[("RUSTFS_POLICY_PLUGIN_URL", opa.url.as_str())])
        .await?;
    opa.wait_for_validation().await?;

    assert_opa_unavailable_denies_sts_and_list_buckets(&env, "configured OPA initialization").await?;

    env.stop_server();
    Ok(())
}

#[tokio::test]
async fn test_sts_and_list_buckets_fail_closed_after_opa_validation_failure() -> TestResult {
    init_logging();

    let mut opa = OpaMock::start_with_mode(OpaValidationMode::Unavailable, None).await?;
    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server_with_env(vec![], &[("RUSTFS_POLICY_PLUGIN_URL", opa.url.as_str())])
        .await?;
    opa.wait_for_validation().await?;

    assert_opa_unavailable_denies_sts_and_list_buckets(&env, "configured OPA validation failure").await?;

    env.stop_server();
    Ok(())
}

#[tokio::test]
async fn test_sts_query_rate_limit_error_is_aws_sdk_compatible() -> TestResult {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server_with_env(
        vec![],
        &[
            ("RUSTFS_API_RATE_LIMIT_ENABLE", "true"),
            ("RUSTFS_API_RATE_LIMIT_RPM", "60"),
            ("RUSTFS_API_RATE_LIMIT_BURST", "1"),
        ],
    )
    .await?;

    let client = sts_client(&env.url, &env.access_key, &env.secret_key, None);
    let mut throttled = None;
    let request = || {
        client
            .assume_role()
            .role_arn("arn:aws:iam::123456789012:role/test")
            .role_session_name("sts-query-rate-limit")
            .send()
    };
    let (first, second, third, fourth) = tokio::join!(request(), request(), request(), request());
    for result in [first, second, third, fourth] {
        if let Err(error) = result
            && error.raw_response().map(|response| response.status().as_u16()) == Some(429)
        {
            throttled = Some(error);
            break;
        }
    }

    let error = throttled.ok_or("at least one concurrent STS request should be throttled at burst one")?;
    let service_error = error
        .as_service_error()
        .ok_or_else(|| format!("rate limit response should deserialize as an STS service error: {error:?}"))?;
    assert_eq!(service_error.code(), Some("TooManyRequests"));
    assert!(
        service_error
            .message()
            .is_some_and(|message| message.starts_with("Request rate limit exceeded")),
        "rate limit response should preserve the server message"
    );
    assert!(
        error.request_id().is_some_and(|request_id| !request_id.is_empty()),
        "rate limit response should include a request ID"
    );

    env.stop_server();
    Ok(())
}
