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

//! Negative presigned-URL (query-string SigV4) regression suite (backlog#1151
//! sec-2).
//!
//! Presigned URLs are the query-string SigV4 surface: the signature and its
//! scope/expiry travel as `X-Amz-*` query parameters rather than in an
//! `Authorization` header. Until now this repository only exercised presigned
//! URLs on the *happy* path (e.g. `head_object_consistency_test`), so nothing
//! pinned OUR end-to-end wiring of expiry enforcement or query-signature
//! verification — a future dependency swap or misconfiguration could silently
//! start honouring expired or forged presigned URLs. These tests send REJECTED
//! presigned requests against a live server and assert the HTTP status plus the
//! S3 error `<Code>` in the response XML, guarding the rejection contract
//! regardless of who performs the underlying verification.
//!
//! This is the query-string sibling of `negative_sigv4_test` (sec-1, header
//! SigV4); the two cover distinct attacker-controlled auth surfaces and share
//! no test cases.
//!
//! Expiry is controlled WITHOUT real waiting: the AWS SDK presigner accepts an
//! explicit `start_time`, so an already-expired URL is produced by signing with
//! a timestamp far enough in the past that `start_time + X-Amz-Expires` is
//! already behind the server clock. Forged variants are produced by presigning
//! a valid URL and then mutating the query (`X-Amz-Signature`) or the signed
//! target (object key) after the fact, and by presigning with the wrong secret.

use crate::common::{RustFSTestEnvironment, init_logging, local_http_client};
use aws_sdk_s3::config::{Credentials, Region};
use aws_sdk_s3::presigning::{PresignedRequest, PresigningConfig};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::{Client, Config};
use aws_smithy_http_client::Builder as SmithyHttpClientBuilder;
use serial_test::serial;
use std::time::{Duration, SystemTime};
use tracing::info;

const REGION: &str = "us-east-1";
const BUCKET: &str = "presigned-negative-bucket";
/// Object that `setup` stores; positive-control GETs read it back.
const CANONICAL_KEY: &str = "canonical-object.txt";
const CANONICAL_BODY: &[u8] = b"presigned-negative-canonical-body";

/// Build an S3 client bound to this environment but with a caller-chosen secret
/// key (mirrors `common::build_test_s3_config`, which is private). Used to
/// presign with the WRONG secret while keeping the real access key id.
fn s3_client_with_secret(env: &RustFSTestEnvironment, secret: &str) -> Client {
    let credentials = Credentials::new(&env.access_key, secret, None, None, "e2e-presigned-negative");
    let mut config = Config::builder()
        .credentials_provider(credentials)
        .region(Region::new(REGION))
        .endpoint_url(&env.url)
        .force_path_style(true)
        .behavior_version_latest();
    if env.url.starts_with("http://") {
        config = config.http_client(SmithyHttpClientBuilder::new().build_http());
    }
    Client::from_conf(config.build())
}

/// A presigning config that is ALREADY expired the moment it is produced:
/// signed as of one hour ago with a 60s validity window, so the server sees a
/// request whose `X-Amz-Date + X-Amz-Expires` is ~59 minutes in the past. No
/// real waiting, no flakiness.
fn expired_config() -> PresigningConfig {
    PresigningConfig::builder()
        .start_time(SystemTime::now() - Duration::from_secs(3600))
        .expires_in(Duration::from_secs(60))
        .build()
        .expect("valid presigning config")
}

/// A generous, valid presigning window for positive controls / pre-tamper URLs.
fn valid_config() -> PresigningConfig {
    PresigningConfig::expires_in(Duration::from_secs(300)).expect("valid presigning config")
}

/// Flip bytes inside the `X-Amz-Signature=` query value without changing its
/// length, producing a structurally valid but incorrect signature.
fn tamper_signature(uri: &str) -> String {
    let marker = "X-Amz-Signature=";
    let idx = uri.find(marker).expect("presigned uri must carry X-Amz-Signature") + marker.len();
    let (head, rest) = uri.split_at(idx);
    let end = rest.find('&').unwrap_or(rest.len());
    let (sig, tail) = rest.split_at(end);
    let tampered: String = sig
        .chars()
        .map(|c| match c {
            '0' => 'f',
            'a' => '0',
            other => other,
        })
        .collect();
    assert_ne!(sig, tampered, "tamper must actually change the signature hex");
    format!("{head}{tampered}{tail}")
}

fn assert_error_code(body: &str, code: &str) {
    assert!(
        body.contains(&format!("<Code>{code}</Code>")),
        "expected S3 error code <Code>{code}</Code> in response body, got:\n{body}"
    );
}

/// Replay a `PresignedRequest` faithfully: same method, same URI, forward every
/// signed header, attach an optional body. `reqwest` derives `Host` from the
/// URI (matching the signed host).
async fn send_presigned(pr: &PresignedRequest, body: Option<Vec<u8>>) -> reqwest::Result<reqwest::Response> {
    send_raw(pr.method(), pr.uri(), pr.headers(), body).await
}

/// Replay against an ARBITRARY (possibly tampered) URI while keeping the signed
/// method/headers of the original presigned request.
async fn send_raw<'a>(
    method: &str,
    uri: &str,
    headers: impl Iterator<Item = (&'a str, &'a str)>,
    body: Option<Vec<u8>>,
) -> reqwest::Result<reqwest::Response> {
    let method = reqwest::Method::from_bytes(method.as_bytes()).expect("valid HTTP method");
    let mut builder = local_http_client().request(method, uri);
    for (k, v) in headers {
        builder = builder.header(k, v);
    }
    if let Some(body) = body {
        builder = builder.body(body);
    }
    builder.send().await
}

async fn setup(env: &mut RustFSTestEnvironment) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    env.start_rustfs_server(vec![]).await?;
    env.create_test_bucket(BUCKET).await?;
    env.create_s3_client()
        .put_object()
        .bucket(BUCKET)
        .key(CANONICAL_KEY)
        .body(ByteStream::from_static(CANONICAL_BODY))
        .send()
        .await?;
    Ok(())
}

/// Positive control (GET): a valid presigned GET must succeed and return the
/// stored bytes. Without this, every negative assertion could pass for the
/// wrong reason (a server that rejects all presigned URLs).
#[tokio::test]
#[serial]
async fn valid_presigned_get_succeeds() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();
    let mut env = RustFSTestEnvironment::new().await?;
    setup(&mut env).await?;

    let pr = env
        .create_s3_client()
        .get_object()
        .bucket(BUCKET)
        .key(CANONICAL_KEY)
        .presigned(valid_config())
        .await?;

    let resp = send_presigned(&pr, None).await?;
    assert_eq!(resp.status().as_u16(), 200, "valid presigned GET should succeed");
    let bytes = resp.bytes().await?;
    assert_eq!(bytes.as_ref(), CANONICAL_BODY, "presigned GET body must match stored object");
    info!("valid presigned GET control passed");
    Ok(())
}

/// Positive control (PUT): a valid presigned PUT must store the object, which we
/// verify with a follow-up authenticated HEAD.
#[tokio::test]
#[serial]
async fn valid_presigned_put_succeeds() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();
    let mut env = RustFSTestEnvironment::new().await?;
    setup(&mut env).await?;

    let key = "presigned-put-ok.txt";
    let body = b"stored-via-presigned-put".to_vec();
    let pr = env
        .create_s3_client()
        .put_object()
        .bucket(BUCKET)
        .key(key)
        .presigned(valid_config())
        .await?;

    let resp = send_presigned(&pr, Some(body.clone())).await?;
    assert!(resp.status().is_success(), "valid presigned PUT should succeed, got {}", resp.status());

    let head = env.create_s3_client().head_object().bucket(BUCKET).key(key).send().await?;
    assert_eq!(head.content_length(), Some(body.len() as i64), "stored object length must match");
    info!("valid presigned PUT control passed");
    Ok(())
}

/// (a) An already-expired presigned GET must be rejected with 403 / AccessDenied
/// ("Request has expired"). s3s checks expiry BEFORE the signature, so the
/// signature here is otherwise valid — only the elapsed window is at fault.
#[tokio::test]
#[serial]
async fn expired_presigned_get_is_rejected() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();
    let mut env = RustFSTestEnvironment::new().await?;
    setup(&mut env).await?;

    let pr = env
        .create_s3_client()
        .get_object()
        .bucket(BUCKET)
        .key(CANONICAL_KEY)
        .presigned(expired_config())
        .await?;

    let resp = send_presigned(&pr, None).await?;
    let status = resp.status();
    let body = resp.text().await?;
    assert_eq!(status.as_u16(), 403, "expired presigned GET must be 403, body:\n{body}");
    assert_error_code(&body, "AccessDenied");
    Ok(())
}

/// (b) Tampering the `X-Amz-Signature` query value must be rejected with 403 /
/// SignatureDoesNotMatch.
#[tokio::test]
#[serial]
async fn tampered_signature_returns_signature_does_not_match() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();
    let mut env = RustFSTestEnvironment::new().await?;
    setup(&mut env).await?;

    let pr = env
        .create_s3_client()
        .get_object()
        .bucket(BUCKET)
        .key(CANONICAL_KEY)
        .presigned(valid_config())
        .await?;

    let tampered_uri = tamper_signature(pr.uri());
    let resp = send_raw(pr.method(), &tampered_uri, pr.headers(), None).await?;
    let status = resp.status();
    let body = resp.text().await?;
    assert_eq!(status.as_u16(), 403, "tampered presigned signature must be 403, body:\n{body}");
    assert_error_code(&body, "SignatureDoesNotMatch");
    Ok(())
}

/// (c) A presigned URL generated with the WRONG secret (but the real access key
/// id) must be rejected with 403 / SignatureDoesNotMatch.
#[tokio::test]
#[serial]
async fn wrong_secret_key_returns_signature_does_not_match() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();
    let mut env = RustFSTestEnvironment::new().await?;
    setup(&mut env).await?;

    let bad_client = s3_client_with_secret(&env, "totally-wrong-secret-key");
    let pr = bad_client
        .get_object()
        .bucket(BUCKET)
        .key(CANONICAL_KEY)
        .presigned(valid_config())
        .await?;

    let resp = send_presigned(&pr, None).await?;
    let status = resp.status();
    let body = resp.text().await?;
    assert_eq!(status.as_u16(), 403, "wrong-secret presigned URL must be 403, body:\n{body}");
    assert_error_code(&body, "SignatureDoesNotMatch");
    Ok(())
}

/// (d) Changing the signed target (the object key in the path) AFTER signing
/// must be rejected with 403 / SignatureDoesNotMatch: the presented request no
/// longer matches the canonical request the signature covers. The signature
/// check runs during auth, before any object lookup, so the swapped key need
/// not even exist.
#[tokio::test]
#[serial]
async fn tampered_target_key_returns_signature_does_not_match() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();
    let mut env = RustFSTestEnvironment::new().await?;
    setup(&mut env).await?;

    let signed_key = "signed-target.txt";
    let served_key = "served-target.txt";
    let pr = env
        .create_s3_client()
        .get_object()
        .bucket(BUCKET)
        .key(signed_key)
        .presigned(valid_config())
        .await?;

    // Swap the object key in the path while leaving the (now stale) signature
    // and its scope untouched.
    let signed_segment = format!("/{signed_key}?");
    let served_segment = format!("/{served_key}?");
    let uri = pr.uri();
    assert!(uri.contains(&signed_segment), "presigned uri must contain the signed key path: {uri}");
    let tampered_uri = uri.replace(&signed_segment, &served_segment);

    let resp = send_raw(pr.method(), &tampered_uri, pr.headers(), None).await?;
    let status = resp.status();
    let body = resp.text().await?;
    assert_eq!(status.as_u16(), 403, "tampered target key must be 403, body:\n{body}");
    assert_error_code(&body, "SignatureDoesNotMatch");
    Ok(())
}

/// (e / acceptance 4 negative half) Tampering the signature of a presigned PUT
/// must be rejected with 403 / SignatureDoesNotMatch — the write must not land.
#[tokio::test]
#[serial]
async fn tampered_presigned_put_returns_signature_does_not_match() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();
    let mut env = RustFSTestEnvironment::new().await?;
    setup(&mut env).await?;

    let key = "presigned-put-tampered.txt";
    let pr = env
        .create_s3_client()
        .put_object()
        .bucket(BUCKET)
        .key(key)
        .presigned(valid_config())
        .await?;

    let tampered_uri = tamper_signature(pr.uri());
    let resp = send_raw(pr.method(), &tampered_uri, pr.headers(), Some(b"should-not-be-stored".to_vec())).await?;
    let status = resp.status();
    let body = resp.text().await?;
    assert_eq!(status.as_u16(), 403, "tampered presigned PUT must be 403, body:\n{body}");
    assert_error_code(&body, "SignatureDoesNotMatch");

    // The rejected write must not have created the object.
    let head = env.create_s3_client().head_object().bucket(BUCKET).key(key).send().await;
    assert!(head.is_err(), "tampered presigned PUT must not store the object");
    Ok(())
}
