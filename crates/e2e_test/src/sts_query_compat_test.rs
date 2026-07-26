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

use crate::common::{RustFSTestEnvironment, init_logging, local_http_client};
use aws_sdk_sts::config::retry::RetryConfig;
use aws_sdk_sts::config::{Credentials, Region};
use aws_sdk_sts::error::ProvideErrorMetadata;
use aws_sdk_sts::operation::RequestId;
use aws_sdk_sts::{Client, Config};
use aws_smithy_http_client::Builder as SmithyHttpClientBuilder;
use http::header::{CONTENT_TYPE, HOST};
use rustfs_signer::constants::UNSIGNED_PAYLOAD;
use rustfs_signer::sign_v4;
use s3s::Body;
use serial_test::serial;
use std::error::Error;

type BoxError = Box<dyn Error + Send + Sync>;
type TestResult = Result<(), BoxError>;

fn sts_client(url: &str, access_key: &str, secret_key: &str, session_token: Option<&str>) -> Client {
    let mut config = Config::builder()
        .credentials_provider(Credentials::new(
            access_key,
            secret_key,
            session_token.map(str::to_owned),
            None,
            "e2e-sts-query-compat",
        ))
        .region(Region::new("us-east-1"))
        .endpoint_url(url)
        .retry_config(RetryConfig::standard().with_max_attempts(1))
        .behavior_version_latest();
    if url.starts_with("http://") {
        config = config.http_client(SmithyHttpClientBuilder::new().build_http());
    }
    Client::from_conf(config.build())
}

async fn create_root_service_account(env: &RustFSTestEnvironment) -> Result<(String, String), BoxError> {
    let path = "/rustfs/admin/v3/add-service-accounts";
    let url = format!("{}{path}", env.url);
    let uri = url.parse::<http::Uri>()?;
    let authority = uri.authority().ok_or("admin URL missing authority")?.to_string();
    let body = serde_json::json!({ "targetUser": env.access_key.clone() }).to_string();
    let request = http::Request::builder()
        .method(http::Method::PUT)
        .uri(uri)
        .header(HOST, authority)
        .header(CONTENT_TYPE, "application/json")
        .header("x-amz-content-sha256", UNSIGNED_PAYLOAD)
        .body(Body::empty())?;
    let content_length = i64::try_from(body.len()).map_err(|_| "service account request body is too large")?;
    let signed = sign_v4(request, content_length, &env.access_key, &env.secret_key, "", "us-east-1");
    let mut request = local_http_client().put(&url);
    for (name, value) in signed.headers() {
        request = request.header(name, value);
    }
    let response = request.body(body).send().await?;
    let status = response.status();
    let body = response.text().await?;
    if !status.is_success() {
        return Err(format!("create service account failed: {status} {body}").into());
    }

    let response: serde_json::Value = serde_json::from_str(&body)?;
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

async fn assert_chaining_denied(client: &Client, credential_kind: &str) -> TestResult {
    let error = client
        .assume_role()
        .role_arn("arn:aws:iam::123456789012:role/test")
        .role_session_name("sts-query-compat-e2e")
        .send()
        .await
        .expect_err("credential chaining must be denied");
    let service_error = error
        .as_service_error()
        .ok_or_else(|| format!("{credential_kind} denial should deserialize as an STS service error: {error:?}"))?;

    assert_eq!(error.raw_response().map(|response| response.status().as_u16()), Some(403));
    assert_eq!(service_error.code(), Some("AccessDenied"));
    assert_eq!(service_error.message(), Some("Access Denied"));
    assert!(
        error.request_id().is_some_and(|request_id| !request_id.is_empty()),
        "{credential_kind} denial should include a request ID"
    );
    Ok(())
}

#[tokio::test]
#[serial]
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
        invalid_signature
            .request_id()
            .is_some_and(|request_id| !request_id.is_empty()),
        "signature rejection should include a request ID"
    );

    assert_chaining_denied(
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
    assert_chaining_denied(&sts_client(&env.url, &service_access_key, &service_secret_key, None), "service account").await?;

    env.stop_server();
    Ok(())
}

#[tokio::test]
#[serial]
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
