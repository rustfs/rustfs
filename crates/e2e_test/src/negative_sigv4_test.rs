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

//! Negative header-SigV4 regression suite (backlog#1151 sec-1).
//!
//! RustFS delegates SigV4 verification to the `s3s` dependency, so nothing in
//! this repository pins OUR end-to-end wiring of it: a future dependency swap
//! or misconfiguration could silently start accepting forged header
//! signatures. These tests send REJECTED SigV4 header-auth requests against a
//! live server and assert the HTTP status plus the S3 error code in the
//! response XML, guarding the rejection contract regardless of who performs
//! the underlying verification.
//!
//! Signatures are hand-built (rather than produced by the AWS SDK, which
//! cannot emit an invalid signature) by reusing the primitive HMAC/scope
//! helpers from `rustfs_signer::request_signature_v4`. This gives the test
//! full control over the timestamp, secret key, signed payload hash, and the
//! final signature bytes.
//!
//! Missing-credential negatives are intentionally NOT duplicated here: those
//! are already covered by `multipart_auth_test` (anonymous / no-credential
//! cases) and `anonymous_access_test`. This module covers only PRESENT but
//! rejected header-SigV4 requests.

use crate::common::{RustFSTestEnvironment, init_logging, local_http_client};
use aws_sdk_s3::primitives::ByteStream;
use rustfs_signer::constants::UNSIGNED_PAYLOAD;
use rustfs_signer::request_signature_v4::{SIGN_V4_ALGORITHM, get_scope, get_signature, get_signing_key};
use serial_test::serial;
use std::fmt::Write as _;
use time::macros::format_description;
use time::{Duration, OffsetDateTime};
use tracing::info;

const REGION: &str = "us-east-1";
const BUCKET: &str = "negative-sigv4-bucket";

/// Lowercase hex encoding (matches SigV4 canonical hex format).
fn hex_lower(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(out, "{b:02x}");
    }
    out
}

fn sha256_hex(data: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    hex_lower(&Sha256::digest(data))
}

fn amz_datetime(t: OffsetDateTime) -> String {
    let fmt = format_description!("[year][month][day]T[hour][minute][second]Z");
    t.format(&fmt).expect("format x-amz-date")
}

/// A minimal hand-rolled SigV4 header signer with full control over every
/// input, so tests can deliberately produce forged / stale / mismatched
/// requests. Always signs exactly `host;x-amz-content-sha256;x-amz-date`.
struct SigV4 {
    access_key: String,
    secret_key: String,
    host: String,
    time: OffsetDateTime,
}

struct SignedHeaders {
    authorization: String,
    amz_date: String,
    content_sha256: String,
}

impl SigV4 {
    fn new(env: &RustFSTestEnvironment) -> Self {
        Self {
            access_key: env.access_key.clone(),
            secret_key: env.secret_key.clone(),
            host: env.address.clone(),
            time: OffsetDateTime::now_utc(),
        }
    }

    /// Build the Authorization header (and the companion `x-amz-date` /
    /// `x-amz-content-sha256` header values) for a request.
    ///
    /// `content_sha256` is the value placed in the `x-amz-content-sha256`
    /// header AND folded into the canonical request — pass the hash of the
    /// body you *claim* to send, which may differ from what you actually send.
    fn sign(&self, method: &str, path: &str, canonical_query: &str, content_sha256: &str) -> SignedHeaders {
        let amz_date = amz_datetime(self.time);
        let signed_headers = "host;x-amz-content-sha256;x-amz-date";

        let canonical_headers = format!(
            "host:{host}\nx-amz-content-sha256:{sha}\nx-amz-date:{date}\n",
            host = self.host,
            sha = content_sha256,
            date = amz_date,
        );
        let canonical_request =
            format!("{method}\n{path}\n{canonical_query}\n{canonical_headers}\n{signed_headers}\n{content_sha256}");

        let scope = get_scope(REGION, self.time, "s3");
        let string_to_sign = format!("{SIGN_V4_ALGORITHM}\n{amz_date}\n{scope}\n{}", sha256_hex(canonical_request.as_bytes()));
        let signing_key = get_signing_key(&self.secret_key, REGION, self.time, "s3");
        let signature = get_signature(signing_key, &string_to_sign);

        let credential = format!("{}/{scope}", self.access_key);
        let authorization =
            format!("{SIGN_V4_ALGORITHM} Credential={credential}, SignedHeaders={signed_headers}, Signature={signature}");

        SignedHeaders {
            authorization,
            amz_date,
            content_sha256: content_sha256.to_string(),
        }
    }
}

/// Send a request carrying explicit SigV4 headers. `reqwest` populates `Host`
/// (matching the signed host) and `Content-Length` automatically.
async fn send_signed(
    env: &RustFSTestEnvironment,
    method: reqwest::Method,
    path: &str,
    headers: &SignedHeaders,
    body: Option<Vec<u8>>,
) -> reqwest::Result<reqwest::Response> {
    let url = format!("{}{}", env.url, path);
    let mut builder = local_http_client()
        .request(method, &url)
        .header("x-amz-date", &headers.amz_date)
        .header("x-amz-content-sha256", &headers.content_sha256)
        .header("authorization", &headers.authorization);
    if let Some(body) = body {
        builder = builder.body(body);
    }
    builder.send().await
}

/// Send a request with a raw (possibly malformed) Authorization header while
/// keeping the other SigV4 headers well-formed.
async fn send_raw_authorization(
    env: &RustFSTestEnvironment,
    method: reqwest::Method,
    path: &str,
    authorization: &str,
) -> reqwest::Result<reqwest::Response> {
    let url = format!("{}{}", env.url, path);
    local_http_client()
        .request(method, &url)
        .header("x-amz-date", amz_datetime(OffsetDateTime::now_utc()))
        .header("x-amz-content-sha256", UNSIGNED_PAYLOAD)
        .header("authorization", authorization)
        .send()
        .await
}

fn assert_error_code(body: &str, code: &str) {
    assert!(
        body.contains(&format!("<Code>{code}</Code>")),
        "expected S3 error code <Code>{code}</Code> in response body, got:\n{body}"
    );
}

async fn setup(env: &mut RustFSTestEnvironment) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    env.start_rustfs_server(vec![]).await?;
    env.create_test_bucket(BUCKET).await?;
    Ok(())
}

/// Positive control: a correctly hand-signed request must succeed. Without
/// this, every negative assertion below could pass for the wrong reason (a
/// broken signer that never produces a valid signature).
#[tokio::test]
#[serial]
async fn valid_header_sigv4_request_succeeds() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();
    let mut env = RustFSTestEnvironment::new().await?;
    setup(&mut env).await?;

    let key = "valid-control.txt";
    let expected = b"valid-sigv4-control-body";
    env.create_s3_client()
        .put_object()
        .bucket(BUCKET)
        .key(key)
        .body(ByteStream::from_static(expected))
        .send()
        .await?;

    let path = format!("/{BUCKET}/{key}");
    let signer = SigV4::new(&env);
    let headers = signer.sign("GET", &path, "", UNSIGNED_PAYLOAD);
    let resp = send_signed(&env, reqwest::Method::GET, &path, &headers, None).await?;

    assert_eq!(resp.status().as_u16(), 200, "correctly signed GET should succeed");
    let bytes = resp.bytes().await?;
    assert_eq!(bytes.as_ref(), expected, "GET body must match stored object");
    info!("valid header SigV4 control passed");
    Ok(())
}

/// (a) Tampering the `Signature=` component must be rejected with
/// SignatureDoesNotMatch / 403.
#[tokio::test]
#[serial]
async fn tampered_signature_returns_signature_does_not_match() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();
    let mut env = RustFSTestEnvironment::new().await?;
    setup(&mut env).await?;

    let path = format!("/{BUCKET}/any-key.txt");
    let signer = SigV4::new(&env);
    let mut headers = signer.sign("GET", &path, "", UNSIGNED_PAYLOAD);

    // Flip bytes inside the Signature= hex without changing its length/shape.
    let marker = "Signature=";
    let idx = headers
        .authorization
        .find(marker)
        .expect("authorization must carry Signature=")
        + marker.len();
    let (head, sig) = headers.authorization.split_at(idx);
    let tampered: String = sig
        .chars()
        .map(|c| match c {
            '0' => 'f',
            'a' => '0',
            other => other,
        })
        .collect();
    assert_ne!(sig, tampered, "tamper must actually change the signature hex");
    headers.authorization = format!("{head}{tampered}");

    let resp = send_signed(&env, reqwest::Method::GET, &path, &headers, None).await?;
    let status = resp.status();
    let body = resp.text().await?;
    assert_eq!(status.as_u16(), 403, "tampered signature must be 403, body:\n{body}");
    assert_error_code(&body, "SignatureDoesNotMatch");
    Ok(())
}

/// (b) A valid AccessKeyId paired with the wrong secret key must be rejected
/// with SignatureDoesNotMatch / 403.
#[tokio::test]
#[serial]
async fn wrong_secret_key_returns_signature_does_not_match() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();
    let mut env = RustFSTestEnvironment::new().await?;
    setup(&mut env).await?;

    let path = format!("/{BUCKET}/any-key.txt");
    let mut signer = SigV4::new(&env);
    // Correct, existing access key; wrong (but validly-shaped) secret.
    signer.secret_key = "totally-wrong-secret-key".to_string();
    let headers = signer.sign("GET", &path, "", UNSIGNED_PAYLOAD);

    let resp = send_signed(&env, reqwest::Method::GET, &path, &headers, None).await?;
    let status = resp.status();
    let body = resp.text().await?;
    assert_eq!(status.as_u16(), 403, "wrong secret must be 403, body:\n{body}");
    assert_error_code(&body, "SignatureDoesNotMatch");
    Ok(())
}

/// (c) A correctly signed request whose actual body differs from the signed
/// `x-amz-content-sha256` must NOT be accepted (must not return 200). The
/// signature itself is valid (it covers the *declared* hash), so the server is
/// forced to detect the payload/hash mismatch while streaming the body.
#[tokio::test]
#[serial]
async fn tampered_payload_is_rejected() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();
    let mut env = RustFSTestEnvironment::new().await?;
    setup(&mut env).await?;

    let path = format!("/{BUCKET}/tampered-payload.txt");
    let claimed_body = b"the-body-i-claim-to-send";
    let actual_body = b"the-body-i-really-send!!";
    assert_eq!(claimed_body.len(), actual_body.len(), "keep content-length stable for the mismatch");

    let signer = SigV4::new(&env);
    // Sign over the hash of the CLAIMED body (single-chunk payload hash), then
    // send a different body of equal length.
    let headers = signer.sign("PUT", &path, "", &sha256_hex(claimed_body));

    let result = send_signed(&env, reqwest::Method::PUT, &path, &headers, Some(actual_body.to_vec())).await;
    match result {
        Ok(resp) => {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            assert_ne!(status.as_u16(), 200, "payload mismatch must not succeed, body:\n{body}");
            assert!(
                status.is_client_error() || status.is_server_error(),
                "payload mismatch must be an error status, got {status}, body:\n{body}"
            );
            info!(%status, "tampered payload rejected with error status");
        }
        // A mid-stream hash-mismatch abort surfacing as a transport error is
        // also a valid rejection (definitely not a 200 success).
        Err(err) => info!(%err, "tampered payload rejected via transport error"),
    }
    Ok(())
}

/// (e) A request whose `x-amz-date` is skewed beyond the server's tolerance
/// (s3s default 900s / 15 min) must be rejected with RequestTimeTooSkewed /
/// 403. The signature is otherwise valid: the credential-scope date and
/// x-amz-date both derive from the same skewed timestamp, so skew — not a
/// signature mismatch — is the failure.
#[tokio::test]
#[serial]
async fn skewed_date_returns_request_time_too_skewed() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();
    let mut env = RustFSTestEnvironment::new().await?;
    setup(&mut env).await?;

    let path = format!("/{BUCKET}/any-key.txt");
    let mut signer = SigV4::new(&env);
    signer.time = OffsetDateTime::now_utc() - Duration::minutes(20); // > 15 min window
    let headers = signer.sign("GET", &path, "", UNSIGNED_PAYLOAD);

    let resp = send_signed(&env, reqwest::Method::GET, &path, &headers, None).await?;
    let status = resp.status();
    let body = resp.text().await?;
    assert_eq!(status.as_u16(), 403, "skewed date must be 403, body:\n{body}");
    assert_error_code(&body, "RequestTimeTooSkewed");
    Ok(())
}

/// (d) Malformed (but PRESENT, not missing) Authorization headers must produce
/// clean 4xx errors — never a 5xx and never a panic/hang. Each variant is a
/// structurally invalid SigV4 header that must be rejected before any
/// credential/service handling.
#[tokio::test]
#[serial]
async fn malformed_authorization_header_returns_clean_4xx() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();
    let mut env = RustFSTestEnvironment::new().await?;
    setup(&mut env).await?;

    let path = format!("/{BUCKET}/any-key.txt");
    let variants = [
        // Algorithm token only, nothing else.
        "AWS4-HMAC-SHA256",
        // Well-formed algorithm but unparseable remainder.
        "AWS4-HMAC-SHA256 total-garbage-not-sigv4",
        // Missing the Signature= component entirely.
        "AWS4-HMAC-SHA256 Credential=rustfsadmin/20240101/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-content-sha256;x-amz-date",
        // Credential scope is not the required access/date/region/service/aws4_request shape.
        "AWS4-HMAC-SHA256 Credential=not-a-valid-scope, SignedHeaders=host, Signature=deadbeef",
        // Empty value.
        "AWS4-HMAC-SHA256 ",
    ];

    for variant in variants {
        let resp = send_raw_authorization(&env, reqwest::Method::GET, &path, variant).await?;
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        assert!(
            status.is_client_error(),
            "malformed Authorization {variant:?} must yield a 4xx (got {status}); body:\n{body}"
        );
        assert!(
            !status.is_server_error(),
            "malformed Authorization {variant:?} must never yield a 5xx (got {status})"
        );
        info!(%status, variant, "malformed Authorization rejected with clean 4xx");
    }
    Ok(())
}
