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

use super::*;
use crate::oidc::test_support::*;
use openidconnect::AsyncHttpClient;
use rustfs_config::MAX_OIDC_RESPONSE_SIZE;
use rustfs_utils::egress::{ENV_OUTBOUND_ALLOW_ORIGINS, OutboundPolicy};
use std::sync::Arc;

#[test]
fn format_http_headers_redacts_sensitive_values() {
    let mut headers = http::HeaderMap::new();
    headers.insert(http::header::AUTHORIZATION, "Basic Y2xpZW50OnNlY3JldA==".parse().unwrap());
    headers.insert(http::header::CONTENT_TYPE, "application/json".parse().unwrap());
    headers.insert(http::header::COOKIE, "session=super-secret".parse().unwrap());

    let rendered = format_http_headers(&headers);

    // Sensitive header values never appear; only their length is emitted.
    assert!(!rendered.contains("Y2xpZW50OnNlY3JldA=="), "authorization value leaked: {rendered}");
    assert!(!rendered.contains("super-secret"), "cookie value leaked: {rendered}");
    assert!(
        rendered.contains("authorization=<redacted len="),
        "expected redacted authorization: {rendered}"
    );
    assert!(rendered.contains("cookie=<redacted len="), "expected redacted cookie: {rendered}");
    // Non-sensitive header values are preserved for diagnostics.
    assert!(
        rendered.contains("content-type=application/json"),
        "content-type should be visible: {rendered}"
    );
}
#[test]
fn is_sensitive_header_is_case_insensitive() {
    assert!(is_sensitive_header("Authorization"));
    assert!(is_sensitive_header("PROXY-AUTHORIZATION"));
    assert!(is_sensitive_header("Set-Cookie"));
    assert!(!is_sensitive_header("content-type"));
    assert!(!is_sensitive_header("x-request-id"));
}
#[test]
fn build_oidc_http_client_rejects_forbidden_targets_without_allowlist() {
    // Cloud metadata endpoint is never allowed.
    assert!(
        matches!(
            build_oidc_http_client("http://169.254.169.254/latest/meta-data/", None, &[], None),
            Err(OidcHttpError::ForbiddenOutbound(_))
        ),
        "metadata endpoint must be rejected"
    );
    // Loopback is rejected by default (no allow-origins configured).
    assert!(
        matches!(
            build_oidc_http_client("http://127.0.0.1:8080/.well-known/openid-configuration", None, &[], None),
            Err(OidcHttpError::ForbiddenOutbound(_))
        ),
        "loopback must be rejected by default"
    );
    // A public hostname passes the up-front shape/host check; the resolved IP is still
    // re-classified at connection time by the pinned resolver.
    assert!(
        build_oidc_http_client("https://accounts.example.com/.well-known/openid-configuration", None, &[], None).is_ok(),
        "public https endpoint should build"
    );
}
#[tokio::test]
async fn oidc_resolves_the_provider_directly_with_an_environment_proxy() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind proxy listener");
    let proxy = format!("http://{}", listener.local_addr().expect("proxy address"));
    temp_env::async_with_vars(
        [
            ("HTTPS_PROXY", Some(proxy.as_str())),
            ("https_proxy", Some(proxy.as_str())),
            ("NO_PROXY", None),
            ("no_proxy", None),
        ],
        async {
            let (client, url) = build_oidc_http_client("https://provider.invalid", Some(&OutboundPolicy::default()), &[], None)
                .expect("build the provider client");
            tokio::select! {
                result = client.get(url).send() => assert!(result.expect_err("reserved .invalid domain").is_connect()),
                _ = listener.accept() => panic!("OIDC connected to the environment proxy"),
            }
        },
    )
    .await;
}
#[test]
fn build_oidc_http_client_honors_explicit_allowlist_for_loopback() {
    let policy = OutboundPolicy::from_allowed_origins("http://127.0.0.1:8080").expect("origin should parse");
    assert!(
        build_oidc_http_client("http://127.0.0.1:8080/.well-known/openid-configuration", Some(&policy), &[], None).is_ok(),
        "explicitly allow-listed loopback origin should build"
    );
    // A metadata endpoint stays forbidden even when a loopback origin is allow-listed.
    assert!(
        matches!(
            build_oidc_http_client("http://169.254.169.254/", Some(&policy), &[], None),
            Err(OidcHttpError::ForbiddenOutbound(_))
        ),
        "metadata endpoint stays forbidden despite an unrelated allow-list entry"
    );
}
#[tokio::test]
async fn oidc_nonrecoverable_dns_policy_rejection_omits_allowlist_hint() {
    let uri = "https://metadata.internal/latest/meta-data";
    let http_client = ReqwestHttpClient::with_policy_and_dns_resolver(
        OutboundPolicy::default(),
        Arc::new(RejectingDnsResolver {
            allow_origin_can_recover: false,
            calls: None,
        }),
    );
    let request = http::Request::builder()
        .uri(uri)
        .body(Vec::new())
        .expect("request should build");

    let error = http_client
        .call(request)
        .await
        .expect_err("metadata DNS answer should be rejected");
    let message = error.to_string();

    assert!(matches!(error, OidcHttpError::ForbiddenOutbound(_)));
    assert!(message.contains("metadata.internal"));
    assert!(!message.contains(&format!("add https://metadata.internal to {ENV_OUTBOUND_ALLOW_ORIGINS}")));
}

#[test]
#[serial_test::serial]
fn build_oidc_http_client_rejects_plaintext_http_for_non_loopback_by_default() {
    temp_env::with_var(rustfs_config::oidc::ENV_IDENTITY_OPENID_ALLOW_INSECURE_TRANSPORT, None::<&str>, || {
        let err = build_oidc_http_client("http://idp.example.com/.well-known/openid-configuration", None, &[], None)
            .expect_err("non-loopback plaintext http:// OIDC URL must be rejected");
        assert!(matches!(err, OidcHttpError::ForbiddenOutbound(ref msg) if msg.contains("plaintext http://")));

        // Loopback http:// stays allowed when explicitly allowlisted in OutboundPolicy.
        let policy = OutboundPolicy::from_allowed_origins("http://127.0.0.1:8080").expect("origin should parse");
        assert!(
            build_oidc_http_client("http://127.0.0.1:8080/.well-known/openid-configuration", Some(&policy), &[], None).is_ok()
        );
    });

    temp_env::with_var(rustfs_config::oidc::ENV_IDENTITY_OPENID_ALLOW_INSECURE_TRANSPORT, Some("true"), || {
        assert!(
            build_oidc_http_client("http://idp.example.com/.well-known/openid-configuration", None, &[], None).is_ok(),
            "explicit opt-in must permit plaintext http:// OIDC URLs"
        );
    });
}
#[test]
fn oidc_metadata_endpoint_rejection_does_not_offer_allowlist_bypass() {
    let error = build_oidc_http_client("http://169.254.169.254/latest/meta-data/", Some(&OutboundPolicy::default()), &[], None)
        .expect_err("metadata endpoint must remain forbidden");
    let message = error.to_string();

    assert!(message.contains("metadata endpoint"));
    assert!(!message.contains(&format!("add http://169.254.169.254 to {ENV_OUTBOUND_ALLOW_ORIGINS}")));
}
#[tokio::test]
async fn oidc_response_body_at_the_limit_is_accepted() {
    let Some((base, handle)) = start_unbounded_body_server(MAX_OIDC_RESPONSE_SIZE) else {
        return;
    };

    let body = fetch_oidc_mock_body(&base)
        .await
        .expect("a body at the limit must be accepted");

    assert_eq!(body.len(), MAX_OIDC_RESPONSE_SIZE);
    handle.join().expect("mock body server thread should exit");
}
#[tokio::test]
async fn oidc_response_body_past_the_limit_is_rejected() {
    let Some((base, handle)) = start_unbounded_body_server(MAX_OIDC_RESPONSE_SIZE + 1) else {
        return;
    };

    let err = fetch_oidc_mock_body(&base)
        .await
        .map(|body| body.len())
        .expect_err("an oversized provider response must fail closed instead of being buffered");

    assert!(
        matches!(err, OidcHttpError::ResponseTooLarge(MAX_OIDC_RESPONSE_SIZE)),
        "unexpected error: {err}"
    );
    handle.join().expect("mock body server thread should exit");
}
