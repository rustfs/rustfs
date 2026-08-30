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
use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::primitives::ByteStream;
use rustfs_signer::constants::{UNSIGNED_PAYLOAD, UNSIGNED_PAYLOAD_TRAILER};
use rustfs_signer::request_signature_v4::{SIGN_V4_ALGORITHM, get_scope, get_signature, get_signing_key};
use std::fmt::Write as _;
use std::io::Cursor;
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
        self.sign_with_extra_headers(method, path, canonical_query, content_sha256, &[])
    }

    /// Sign additional request headers while preserving SigV4's lowercase,
    /// lexicographically sorted canonical-header representation.
    fn sign_with_extra_headers(
        &self,
        method: &str,
        path: &str,
        canonical_query: &str,
        content_sha256: &str,
        extra_signed_headers: &[(&str, &str)],
    ) -> SignedHeaders {
        let amz_date = amz_datetime(self.time);
        let mut canonical_header_values = vec![
            ("host", self.host.as_str()),
            ("x-amz-content-sha256", content_sha256),
            ("x-amz-date", amz_date.as_str()),
        ];
        canonical_header_values.extend(extra_signed_headers.iter().copied());
        canonical_header_values.sort_unstable_by(|left, right| left.0.cmp(right.0));

        let signed_headers = canonical_header_values
            .iter()
            .map(|(name, _)| *name)
            .collect::<Vec<_>>()
            .join(";");
        let mut canonical_headers = String::new();
        for (name, value) in canonical_header_values {
            let _ = writeln!(canonical_headers, "{name}:{value}");
        }
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

async fn build_single_member_archive(
    member_key: &str,
    member_body: &[u8],
) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
    let mut builder = tokio_tar::Builder::new(Cursor::new(Vec::new()));
    let mut header = tokio_tar::Header::new_gnu();
    header.set_size(member_body.len() as u64);
    header.set_mode(0o644);
    header.set_cksum();
    builder.append_data(&mut header, member_key, Cursor::new(member_body)).await?;
    Ok(builder.into_inner().await?.into_inner())
}

fn sha256_base64(data: &[u8]) -> String {
    use sha2::{Digest, Sha256};

    base64_simd::STANDARD.encode_to_string(Sha256::digest(data))
}

fn encode_unsigned_aws_chunked_with_sha256_trailer(decoded: &[u8]) -> Vec<u8> {
    let checksum = sha256_base64(decoded);
    let mut encoded = format!("{:x}\r\n", decoded.len()).into_bytes();
    encoded.extend_from_slice(decoded);
    encoded.extend_from_slice(b"\r\n0\r\n\r\n");
    encoded.extend_from_slice(format!("x-amz-checksum-sha256:{checksum}").as_bytes());
    encoded
}

/// Positive control: a correctly hand-signed request must succeed. Without
/// this, every negative assertion below could pass for the wrong reason (a
/// broken signer that never produces a valid signature).
#[tokio::test]
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

/// `STREAMING-UNSIGNED-PAYLOAD-TRAILER` disables per-chunk signatures, not the
/// seed/header SigV4 signature. A forged request must be rejected before the
/// Snowball handler can publish any archive member.
#[tokio::test]
async fn snowball_streaming_unsigned_trailer_rejects_forged_signature() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();
    let mut env = RustFSTestEnvironment::new().await?;
    setup(&mut env).await?;

    let archive_key = "forged-streaming-snowball.tar";
    let member_key = "must-not-be-published.txt";
    let archive = build_single_member_archive(member_key, b"forged request payload").await?;
    let decoded_content_length = archive.len().to_string();
    let encoded_body = encode_unsigned_aws_chunked_with_sha256_trailer(&archive);
    let path = format!("/{BUCKET}/{archive_key}");

    let mut signer = SigV4::new(&env);
    signer.secret_key = "wrong-secret-for-forged-streaming-request".to_string();
    let extra_signed_headers = [
        ("content-encoding", "aws-chunked"),
        ("x-amz-decoded-content-length", decoded_content_length.as_str()),
        ("x-amz-meta-snowball-auto-extract", "true"),
        ("x-amz-trailer", "x-amz-checksum-sha256"),
    ];
    let headers = signer.sign_with_extra_headers("PUT", &path, "", UNSIGNED_PAYLOAD_TRAILER, &extra_signed_headers);

    let response = local_http_client()
        .put(format!("{}{}", env.url, path))
        .header("authorization", &headers.authorization)
        .header("content-encoding", "aws-chunked")
        .header("x-amz-content-sha256", &headers.content_sha256)
        .header("x-amz-date", &headers.amz_date)
        .header("x-amz-decoded-content-length", &decoded_content_length)
        .header("x-amz-meta-snowball-auto-extract", "true")
        .header("x-amz-trailer", "x-amz-checksum-sha256")
        .body(encoded_body)
        .send()
        .await?;
    let status = response.status();
    let body = response.text().await?;
    assert_eq!(status.as_u16(), 403, "forged streaming signature must be 403, body:\n{body}");
    assert_error_code(&body, "SignatureDoesNotMatch");

    let absent = env
        .create_s3_client()
        .get_object()
        .bucket(BUCKET)
        .key(member_key)
        .send()
        .await
        .expect_err("a forged streaming request must not publish a Snowball member");
    assert_eq!(absent.raw_response().map(|response| response.status().as_u16()), Some(404));
    assert_eq!(absent.as_service_error().and_then(ProvideErrorMetadata::code), Some("NoSuchKey"));

    env.stop_server();
    Ok(())
}

/// Snowball must consume the complete aws-chunked body before reading the
/// trailing checksum exported by s3s into the PutObject response.
#[tokio::test]
async fn snowball_streaming_unsigned_trailer_returns_sha256_checksum() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();
    let mut env = RustFSTestEnvironment::new().await?;
    setup(&mut env).await?;

    let archive_key = "valid-streaming-snowball.tar";
    let member_key = "streaming-checksum-member.txt";
    let member_body = b"valid streaming Snowball payload";
    let archive = build_single_member_archive(member_key, member_body).await?;
    let expected_checksum = sha256_base64(&archive);
    let decoded_content_length = archive.len().to_string();
    let encoded_body = encode_unsigned_aws_chunked_with_sha256_trailer(&archive);
    let path = format!("/{BUCKET}/{archive_key}");

    let signer = SigV4::new(&env);
    let extra_signed_headers = [
        ("content-encoding", "aws-chunked"),
        ("x-amz-decoded-content-length", decoded_content_length.as_str()),
        ("x-amz-meta-snowball-auto-extract", "true"),
        ("x-amz-sdk-checksum-algorithm", "SHA256"),
        ("x-amz-trailer", "x-amz-checksum-sha256"),
    ];
    let headers = signer.sign_with_extra_headers("PUT", &path, "", UNSIGNED_PAYLOAD_TRAILER, &extra_signed_headers);

    let response = local_http_client()
        .put(format!("{}{}", env.url, path))
        .header("authorization", &headers.authorization)
        .header("content-encoding", "aws-chunked")
        .header("x-amz-content-sha256", &headers.content_sha256)
        .header("x-amz-date", &headers.amz_date)
        .header("x-amz-decoded-content-length", &decoded_content_length)
        .header("x-amz-meta-snowball-auto-extract", "true")
        .header("x-amz-sdk-checksum-algorithm", "SHA256")
        .header("x-amz-trailer", "x-amz-checksum-sha256")
        .body(encoded_body)
        .send()
        .await?;
    let status = response.status();
    let response_checksum = response
        .headers()
        .get("x-amz-checksum-sha256")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let response_body = response.text().await?;
    assert_eq!(status.as_u16(), 200, "valid streaming Snowball PUT failed, body:\n{response_body}");
    assert_eq!(response_checksum.as_deref(), Some(expected_checksum.as_str()));

    let member = env
        .create_s3_client()
        .get_object()
        .bucket(BUCKET)
        .key(member_key)
        .send()
        .await?;
    let stored = member.body.collect().await?.into_bytes();
    assert_eq!(stored.as_ref(), member_body);

    env.stop_server();
    Ok(())
}

/// (b) A valid AccessKeyId paired with the wrong secret key must be rejected
/// with SignatureDoesNotMatch / 403.
#[tokio::test]
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
async fn tampered_payload_is_rejected() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();
    let mut env = RustFSTestEnvironment::new().await?;
    setup(&mut env).await?;

    let key = "tampered-payload.txt";
    let path = format!("/{BUCKET}/{key}");
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
            assert!(
                status.is_client_error(),
                "payload mismatch must be rejected with a client error, got {status}, body:\n{body}"
            );
            info!(%status, "tampered payload rejected with error status");
        }
        // A mid-stream hash-mismatch abort surfacing as a transport error is
        // also a valid rejection (definitely not a 200 success).
        Err(err) => {
            assert!(!err.is_connect(), "connection failure is not proof of payload rejection: {err}");
            assert!(!err.is_timeout(), "request timeout is not proof of payload rejection: {err}");
            info!(%err, "tampered payload rejected via mid-stream transport error");
        }
    }

    let absent = env
        .create_s3_client()
        .get_object()
        .bucket(BUCKET)
        .key(key)
        .send()
        .await
        .expect_err("a tampered payload must not publish an object");
    assert_eq!(absent.raw_response().map(|response| response.status().as_u16()), Some(404));
    assert_eq!(absent.as_service_error().and_then(ProvideErrorMetadata::code), Some("NoSuchKey"));
    Ok(())
}

/// A signed UploadPart body must pass the same payload-hash gate as PutObject.
/// Rejection must happen before the part is published into the multipart upload.
#[tokio::test]
async fn tampered_upload_part_payload_is_rejected() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();
    let mut env = RustFSTestEnvironment::new().await?;
    setup(&mut env).await?;

    let key = "tampered-upload-part.bin";
    let client = env.create_s3_client();
    let upload = client.create_multipart_upload().bucket(BUCKET).key(key).send().await?;
    let upload_id = upload.upload_id().ok_or("create multipart upload omitted upload_id")?;

    let path = format!("/{BUCKET}/{key}");
    let canonical_query = format!("partNumber=1&uploadId={}", urlencoding::encode(upload_id));
    let request_target = format!("{path}?{canonical_query}");
    let claimed_body = b"the-part-i-claim-to-send";
    let actual_body = b"the-part-i-really-send!!";
    assert_eq!(claimed_body.len(), actual_body.len(), "keep content-length stable for the mismatch");

    let signer = SigV4::new(&env);
    let headers = signer.sign("PUT", &path, &canonical_query, &sha256_hex(claimed_body));
    let resp = send_signed(&env, reqwest::Method::PUT, &request_target, &headers, Some(actual_body.to_vec())).await?;
    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();
    assert_eq!(
        status,
        reqwest::StatusCode::BAD_REQUEST,
        "multipart payload mismatch must be rejected as BadDigest, body:\n{body}"
    );
    assert_error_code(&body, "BadDigest");

    let parts = client
        .list_parts()
        .bucket(BUCKET)
        .key(key)
        .upload_id(upload_id)
        .send()
        .await?;
    assert!(parts.parts().is_empty(), "a tampered UploadPart must not publish a part");

    client
        .abort_multipart_upload()
        .bucket(BUCKET)
        .key(key)
        .upload_id(upload_id)
        .send()
        .await?;
    Ok(())
}

/// (e) A request whose `x-amz-date` is skewed beyond the server's tolerance
/// (s3s default 900s / 15 min) must be rejected with RequestTimeTooSkewed /
/// 403. The signature is otherwise valid: the credential-scope date and
/// x-amz-date both derive from the same skewed timestamp, so skew — not a
/// signature mismatch — is the failure.
#[tokio::test]
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
