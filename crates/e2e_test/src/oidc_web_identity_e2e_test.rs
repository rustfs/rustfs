// Copyright 2026 RustFS Team
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

//! End-to-end `AssumeRoleWithWebIdentity` OIDC test matrix (`rustfs/backlog#2724`).
//!
//! Spins up an in-process mock OIDC Identity Provider (discovery, JWKS, and
//! refresh-token `/token` endpoint) and drives a real `rustfs` binary through
//! the full web-identity STS -> S3/Admin lifecycle:
//!
//! - E2E-01: Discovery & JWKS reachable; provider initializes cleanly.
//! - E2E-02: `AssumeRoleWithWebIdentity` with a real ES256 `id_token` returns
//!   temporary `AccessKeyId`, `SecretAccessKey`, `SessionToken`, `Expiration`,
//!   and `SubjectFromWebIdentityToken`.
//! - E2E-03: `groups` claim maps to matching canned IAM policies.
//! - E2E-04: In-policy bucket and object CRUD succeed.
//! - E2E-05: Out-of-policy `PutObject`, `GetObject`, `DeleteObject`,
//!   `CreateBucket`, and `DeleteBucket` all fail with `AccessDenied`.
//! - E2E-06: Read-only OIDC user can read/list and is denied on writes.
//! - E2E-07: Temporary OIDC credentials are rejected with `403` on Admin APIs.
//! - E2E-08 / P1-2: Inline STS session policy narrows OIDC permissions to the
//!   intersection of the IAM group policy and the session policy.
//! - E2E-09: `DurationSeconds` below 900 or above 43200 is clamped to `[900, 43200]`.
//! - E2E-10: Refreshed `id_token` minted from the mock IdP's `refresh_token`
//!   endpoint exchanges into working S3 credentials.
//! - E2E-11: Token with no `groups` claim and no `ROLE_POLICY` is rejected.
//! - E2E-12: Token whose `groups` claim names only non-existent policies is rejected.
//! - E2E-13: 10 concurrent `AssumeRoleWithWebIdentity` requests all succeed.
//! - P1-2 Multi-audience: `aud` containing `client_id` + configured
//!   `OTHER_AUDIENCES` succeeds, while missing `client_id` or unlisted extra
//!   audience is rejected.

use crate::common::{
    AdminTransport, RustFSTestEnvironment, admin_add_canned_policy_via, admin_request_with_session_token, build_test_s3_config,
    build_test_sts_client, init_logging, local_http_client,
};
use aws_sdk_s3::Client as S3Client;
use aws_sdk_s3::error::{ProvideErrorMetadata as S3ProvideErrorMetadata, SdkError as S3SdkError};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_sts::error::ProvideErrorMetadata as StsProvideErrorMetadata;
use base64_simd::URL_SAFE_NO_PAD;
use http::Method;
use rcgen::{KeyPair, PKCS_ECDSA_P256_SHA256};
use rustls::SignatureScheme;
use rustls::crypto::aws_lc_rs::sign::any_ecdsa_type;
use rustls::pki_types::PrivateKeyDer;
use serde_json::{Value, json};
use std::error::Error;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use time::OffsetDateTime;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::task::JoinSet;

type BoxError = Box<dyn Error + Send + Sync>;
type TestResult<T = ()> = Result<T, BoxError>;

const CLIENT_ID: &str = "rustfs-e2e-oidc-client";
const EXTRA_AUDIENCE: &str = "trusted-extra-audience";
const KEY_ID: &str = "e2e-es256-key-1";

struct Es256Signer {
    kid: String,
    pkcs8_der: Vec<u8>,
    jwk: Value,
}

impl Es256Signer {
    fn generate(kid: &str) -> Result<Self, BoxError> {
        let key_pair = KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256)?;
        let pub_raw = key_pair.public_key_raw();
        if pub_raw.len() != 65 || pub_raw[0] != 0x04 {
            return Err(format!("unexpected uncompressed P-256 public key length {}", pub_raw.len()).into());
        }
        let x = URL_SAFE_NO_PAD.encode_to_string(&pub_raw[1..33]);
        let y = URL_SAFE_NO_PAD.encode_to_string(&pub_raw[33..65]);
        let jwk = json!({
            "kty": "EC",
            "crv": "P-256",
            "alg": "ES256",
            "use": "sig",
            "kid": kid,
            "x": x,
            "y": y,
        });
        Ok(Self {
            kid: kid.to_string(),
            pkcs8_der: key_pair.serialize_der(),
            jwk,
        })
    }

    fn sign_jwt(&self, claims: &Value) -> Result<String, BoxError> {
        let header = json!({
            "alg": "ES256",
            "typ": "JWT",
            "kid": self.kid,
        });
        let header_b64 = URL_SAFE_NO_PAD.encode_to_string(serde_json::to_vec(&header)?);
        let payload_b64 = URL_SAFE_NO_PAD.encode_to_string(serde_json::to_vec(claims)?);
        let signing_input = format!("{header_b64}.{payload_b64}");

        let private_key = PrivateKeyDer::Pkcs8(self.pkcs8_der.clone().into());
        let signing_key = any_ecdsa_type(&private_key)?;
        let signer = signing_key
            .choose_scheme(&[SignatureScheme::ECDSA_NISTP256_SHA256])
            .ok_or("ES256 signer scheme unavailable")?;
        let der_sig = signer.sign(signing_input.as_bytes())?;
        let raw_rs = der_ecdsa_to_raw_p256(&der_sig)?;
        let sig_b64 = URL_SAFE_NO_PAD.encode_to_string(raw_rs);
        Ok(format!("{signing_input}.{sig_b64}"))
    }
}

/// Convert ASN.1 DER `SEQUENCE { INTEGER r, INTEGER s }` into fixed 64-byte JWS `r || s`.
fn der_ecdsa_to_raw_p256(der: &[u8]) -> Result<[u8; 64], BoxError> {
    if der.len() < 8 || der[0] != 0x30 {
        return Err("invalid DER ECDSA signature: missing SEQUENCE tag".into());
    }
    let (seq_len, mut idx) = if der[1] & 0x80 == 0 {
        (der[1] as usize, 2)
    } else {
        return Err("invalid DER ECDSA signature: unexpected long SEQUENCE length".into());
    };
    if idx + seq_len != der.len() {
        return Err("invalid DER ECDSA signature: mismatched SEQUENCE length".into());
    }
    let mut out = [0u8; 64];
    for slot in [0usize, 32usize] {
        if idx + 2 > der.len() || der[idx] != 0x02 {
            return Err("invalid DER ECDSA signature: missing INTEGER tag".into());
        }
        let len = der[idx + 1] as usize;
        idx += 2;
        if idx + len > der.len() {
            return Err("invalid DER ECDSA signature: truncated INTEGER".into());
        }
        let mut int_bytes = &der[idx..idx + len];
        idx += len;
        while int_bytes.len() > 32 && int_bytes[0] == 0 {
            int_bytes = &int_bytes[1..];
        }
        if int_bytes.len() > 32 {
            return Err("invalid DER ECDSA signature: integer exceeds 32 bytes".into());
        }
        let start = slot + (32 - int_bytes.len());
        out[start..slot + 32].copy_from_slice(int_bytes);
    }
    if idx != der.len() {
        return Err("invalid DER ECDSA signature: trailing bytes".into());
    }
    Ok(out)
}

struct MockOidcIdp {
    base: String,
    signer: Arc<Es256Signer>,
    discovery_hits: Arc<AtomicUsize>,
    jwks_hits: Arc<AtomicUsize>,
    token_hits: Arc<AtomicUsize>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for MockOidcIdp {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl MockOidcIdp {
    async fn start() -> Result<Self, BoxError> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let base = format!("http://{}", listener.local_addr()?);
        let signer = Arc::new(Es256Signer::generate(KEY_ID)?);
        let discovery_hits = Arc::new(AtomicUsize::new(0));
        let jwks_hits = Arc::new(AtomicUsize::new(0));
        let token_hits = Arc::new(AtomicUsize::new(0));

        let discovery_doc = json!({
            "issuer": base,
            "authorization_endpoint": format!("{base}/authorize"),
            "token_endpoint": format!("{base}/token"),
            "jwks_uri": format!("{base}/jwks"),
            "response_types_supported": ["code", "id_token"],
            "subject_types_supported": ["public"],
            "id_token_signing_alg_values_supported": ["ES256"],
        })
        .to_string();
        let jwks_doc = json!({ "keys": [signer.jwk] }).to_string();

        let server_base = base.clone();
        let server_signer = Arc::clone(&signer);
        let server_disc = Arc::clone(&discovery_hits);
        let server_jwks = Arc::clone(&jwks_hits);
        let server_tok = Arc::clone(&token_hits);

        let task = tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    break;
                };
                let discovery_doc = discovery_doc.clone();
                let jwks_doc = jwks_doc.clone();
                let server_base = server_base.clone();
                let server_signer = Arc::clone(&server_signer);
                let server_disc = Arc::clone(&server_disc);
                let server_jwks = Arc::clone(&server_jwks);
                let server_tok = Arc::clone(&server_tok);

                tokio::spawn(async move {
                    let mut buf = Vec::with_capacity(2048);
                    let mut tmp = [0u8; 1024];
                    let header_end = loop {
                        let Ok(n) = stream.read(&mut tmp).await else {
                            return;
                        };
                        if n == 0 {
                            return;
                        }
                        buf.extend_from_slice(&tmp[..n]);
                        if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                            break pos + 4;
                        }
                        if buf.len() > 16_384 {
                            return;
                        }
                    };
                    let head = String::from_utf8_lossy(&buf[..header_end]).into_owned();
                    let content_len: usize = head
                        .lines()
                        .find_map(|line| {
                            let (name, val) = line.split_once(':')?;
                            name.eq_ignore_ascii_case("content-length")
                                .then(|| val.trim().parse().ok())
                                .flatten()
                        })
                        .unwrap_or(0);
                    while buf.len() < header_end + content_len {
                        let Ok(n) = stream.read(&mut tmp).await else {
                            return;
                        };
                        if n == 0 {
                            break;
                        }
                        buf.extend_from_slice(&tmp[..n]);
                    }

                    let req_line = head.lines().next().unwrap_or_default();
                    let mut parts = req_line.split_whitespace();
                    let method = parts.next().unwrap_or_default();
                    let path = parts.next().unwrap_or_default();

                    let (status, body) = if path.ends_with("/.well-known/openid-configuration") {
                        server_disc.fetch_add(1, Ordering::SeqCst);
                        (200, discovery_doc)
                    } else if path == "/jwks" {
                        server_jwks.fetch_add(1, Ordering::SeqCst);
                        (200, jwks_doc)
                    } else if method == "POST" && path == "/token" {
                        let req_body = String::from_utf8_lossy(&buf[header_end..]);
                        if req_body.contains("grant_type=refresh_token") && req_body.contains("refresh_token=e2e-refresh-token") {
                            let n = server_tok.fetch_add(1, Ordering::SeqCst);
                            let now = OffsetDateTime::now_utc().unix_timestamp();
                            let claims = json!({
                                "iss": server_base,
                                "sub": "alice-refreshed",
                                "aud": CLIENT_ID,
                                "iat": now,
                                "exp": now + 600,
                                "jti": format!("refreshed-e2e-{n}"),
                                "groups": ["alice-rw"],
                            });
                            match server_signer.sign_jwt(&claims) {
                                Ok(id_token) => (
                                    200,
                                    json!({
                                        "access_token": "e2e-access-token",
                                        "token_type": "Bearer",
                                        "expires_in": 600,
                                        "refresh_token": "e2e-refresh-token-next",
                                        "id_token": id_token,
                                    })
                                    .to_string(),
                                ),
                                Err(err) => (500, json!({"error": err.to_string()}).to_string()),
                            }
                        } else {
                            (400, json!({"error": "invalid_grant"}).to_string())
                        }
                    } else {
                        (404, json!({"error": "not_found"}).to_string())
                    };

                    let reason = match status {
                        200 => "OK",
                        400 => "Bad Request",
                        500 => "Internal Server Error",
                        _ => "Not Found",
                    };
                    let resp = format!(
                        "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    let _ = stream.write_all(resp.as_bytes()).await;
                });
            }
        });

        Ok(Self {
            base,
            signer,
            discovery_hits,
            jwks_hits,
            token_hits,
            task,
        })
    }

    fn issue_id_token(&self, sub: &str, aud: Value, groups: Option<&[&str]>) -> Result<String, BoxError> {
        let now = OffsetDateTime::now_utc().unix_timestamp();
        let mut claims = json!({
            "iss": self.base,
            "sub": sub,
            "aud": aud,
            "iat": now,
            "exp": now + 600,
        });
        if let Some(groups) = groups {
            claims["groups"] = json!(groups);
        }
        self.signer.sign_jwt(&claims)
    }
}

fn s3_client_for_sts(url: &str, creds: &aws_sdk_sts::types::Credentials) -> S3Client {
    S3Client::from_conf(build_test_s3_config(
        url,
        creds.access_key_id(),
        creds.secret_access_key(),
        Some(creds.session_token()),
        "e2e-oidc-web-identity-s3",
    ))
}

fn assert_s3_access_denied<T, E>(result: Result<T, S3SdkError<E>>, context: &str)
where
    T: std::fmt::Debug,
    E: S3ProvideErrorMetadata + std::fmt::Debug,
{
    let err = result.expect_err(&format!("{context}: expected AccessDenied"));
    assert_eq!(
        err.raw_response().map(|r| r.status().as_u16()),
        Some(403),
        "{context}: expected HTTP 403, got {err:?}"
    );
    assert_eq!(
        err.as_service_error().and_then(S3ProvideErrorMetadata::code),
        Some("AccessDenied"),
        "{context}: expected AccessDenied code, got {err:?}"
    );
}

#[tokio::test]
async fn oidc_assume_role_with_web_identity_full_e2e_matrix() -> TestResult {
    init_logging();
    let idp = MockOidcIdp::start().await?;
    let discovery_url = format!("{}/.well-known/openid-configuration", idp.base);

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server_without_cleanup_with_env(&[
        ("RUSTFS_IDENTITY_OPENID_ENABLE", "on"),
        ("RUSTFS_IDENTITY_OPENID_CONFIG_URL", discovery_url.as_str()),
        ("RUSTFS_IDENTITY_OPENID_CLIENT_ID", CLIENT_ID),
        ("RUSTFS_IDENTITY_OPENID_SCOPES", "openid,profile,email,groups"),
        ("RUSTFS_IDENTITY_OPENID_CLAIM_NAME", "groups"),
        ("RUSTFS_IDENTITY_OPENID_OTHER_AUDIENCES", EXTRA_AUDIENCE),
        ("RUSTFS_OUTBOUND_ALLOW_ORIGINS", idp.base.as_str()),
        ("NO_PROXY", "127.0.0.1,localhost"),
        ("no_proxy", "127.0.0.1,localhost"),
    ])
    .await?;
    env.wait_for_server_ready().await?;

    // E2E-01: Discovery and JWKS endpoints were fetched and accepted at startup.
    assert!(
        idp.discovery_hits.load(Ordering::SeqCst) >= 1,
        "E2E-01: OIDC discovery endpoint must have been reached"
    );
    assert!(
        idp.jwks_hits.load(Ordering::SeqCst) >= 1,
        "E2E-01: OIDC JWKS endpoint must have been reached"
    );

    let root_s3 = env.create_s3_client();
    let allowed_bucket = "oidc-alice-bucket";
    let denied_bucket = "oidc-private-bucket";
    root_s3.create_bucket().bucket(allowed_bucket).send().await?;
    root_s3.create_bucket().bucket(denied_bucket).send().await?;
    root_s3
        .put_object()
        .bucket(denied_bucket)
        .key("secret.txt")
        .body(ByteStream::from_static(b"top-secret"))
        .send()
        .await?;
    root_s3
        .put_object()
        .bucket(allowed_bucket)
        .key("narrow/readable.txt")
        .body(ByteStream::from_static(b"narrow-ok"))
        .send()
        .await?;
    root_s3
        .put_object()
        .bucket(allowed_bucket)
        .key("other/blocked.txt")
        .body(ByteStream::from_static(b"other-blocked"))
        .send()
        .await?;

    // Register canned IAM policies referenced by OIDC `groups` claims.
    for (name, doc) in [
        (
            "alice-rw",
            json!({
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Action": ["s3:ListBucket", "s3:GetBucketLocation"],
                        "Resource": [format!("arn:aws:s3:::{allowed_bucket}")]
                    },
                    {
                        "Effect": "Allow",
                        "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
                        "Resource": [format!("arn:aws:s3:::{allowed_bucket}/*")]
                    }
                ]
            }),
        ),
        (
            "bob-ro",
            json!({
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Action": ["s3:ListBucket"],
                        "Resource": [format!("arn:aws:s3:::{allowed_bucket}")]
                    },
                    {
                        "Effect": "Allow",
                        "Action": ["s3:GetObject"],
                        "Resource": [format!("arn:aws:s3:::{allowed_bucket}/*")]
                    }
                ]
            }),
        ),
    ] {
        admin_add_canned_policy_via(AdminTransport::Signed, &env.url, &env.access_key, &env.secret_key, name, &doc.to_string())
            .await?;
    }

    // STS client for AssumeRoleWithWebIdentity (unauthenticated at SigV4 layer; JWT is auth).
    let sts = build_test_sts_client(&env.url, "unused", "unused", None, "e2e-oidc-sts");

    // -----------------------------------------------------------------------
    // E2E-02, E2E-03, E2E-04, E2E-05: Alice RW exchange + in-policy CRUD + out-of-policy denies
    // -----------------------------------------------------------------------
    let alice_jwt = idp.issue_id_token("alice-sub", json!(CLIENT_ID), Some(&["alice-rw"]))?;
    let alice_assumed = sts
        .assume_role_with_web_identity()
        .role_arn("arn:aws:iam::123456789012:role/oidc-alice")
        .role_session_name("alice-e2e-session")
        .web_identity_token(&alice_jwt)
        .duration_seconds(1800)
        .send()
        .await?;
    assert_eq!(alice_assumed.subject_from_web_identity_token(), Some("alice-sub"));
    let alice_creds = alice_assumed.credentials().ok_or("E2E-02: missing STS credentials")?;
    assert!(!alice_creds.access_key_id().is_empty());
    assert!(!alice_creds.secret_access_key().is_empty());
    assert!(!alice_creds.session_token().is_empty());

    let alice_s3 = s3_client_for_sts(&env.url, alice_creds);

    // E2E-04: In-policy CRUD all succeed.
    alice_s3
        .put_object()
        .bucket(allowed_bucket)
        .key("work/item.txt")
        .body(ByteStream::from_static(b"alice-payload"))
        .send()
        .await?;
    let got = alice_s3
        .get_object()
        .bucket(allowed_bucket)
        .key("work/item.txt")
        .send()
        .await?;
    assert_eq!(got.body.collect().await?.into_bytes().as_ref(), b"alice-payload");
    let listed = alice_s3
        .list_objects_v2()
        .bucket(allowed_bucket)
        .prefix("work/")
        .send()
        .await?;
    assert_eq!(listed.contents().len(), 1);
    alice_s3
        .delete_object()
        .bucket(allowed_bucket)
        .key("work/item.txt")
        .send()
        .await?;

    // E2E-05: Out-of-policy put/get/delete/mb/rb all denied with 403 AccessDenied.
    assert_s3_access_denied(
        alice_s3
            .put_object()
            .bucket(denied_bucket)
            .key("forged.txt")
            .body(ByteStream::from_static(b"nope"))
            .send()
            .await,
        "E2E-05 out-of-policy PutObject",
    );
    assert_s3_access_denied(
        alice_s3.get_object().bucket(denied_bucket).key("secret.txt").send().await,
        "E2E-05 out-of-policy GetObject",
    );
    assert_s3_access_denied(
        alice_s3.delete_object().bucket(denied_bucket).key("secret.txt").send().await,
        "E2E-05 out-of-policy DeleteObject",
    );
    assert_s3_access_denied(
        alice_s3.create_bucket().bucket("oidc-unauth-create").send().await,
        "E2E-05 out-of-policy CreateBucket",
    );
    assert_s3_access_denied(
        alice_s3.delete_bucket().bucket(denied_bucket).send().await,
        "E2E-05 out-of-policy DeleteBucket",
    );

    // -----------------------------------------------------------------------
    // E2E-06: Read-only user succeeds on read/list, denied on all writes
    // -----------------------------------------------------------------------
    let bob_jwt = idp.issue_id_token("bob-sub", json!(CLIENT_ID), Some(&["bob-ro"]))?;
    let bob_assumed = sts
        .assume_role_with_web_identity()
        .role_arn("arn:aws:iam::123456789012:role/oidc-bob")
        .role_session_name("bob-e2e-session")
        .web_identity_token(&bob_jwt)
        .send()
        .await?;
    let bob_creds = bob_assumed.credentials().ok_or("E2E-06: missing Bob STS credentials")?;
    let bob_s3 = s3_client_for_sts(&env.url, bob_creds);

    let bob_got = bob_s3
        .get_object()
        .bucket(allowed_bucket)
        .key("narrow/readable.txt")
        .send()
        .await?;
    assert_eq!(bob_got.body.collect().await?.into_bytes().as_ref(), b"narrow-ok");
    assert_s3_access_denied(
        bob_s3
            .put_object()
            .bucket(allowed_bucket)
            .key("narrow/bob-write.txt")
            .body(ByteStream::from_static(b"nope"))
            .send()
            .await,
        "E2E-06 readonly PutObject",
    );
    assert_s3_access_denied(
        bob_s3
            .delete_object()
            .bucket(allowed_bucket)
            .key("narrow/readable.txt")
            .send()
            .await,
        "E2E-06 readonly DeleteObject",
    );
    assert_s3_access_denied(
        bob_s3.create_bucket().bucket("bob-ro-bucket").send().await,
        "E2E-06 readonly CreateBucket",
    );

    // -----------------------------------------------------------------------
    // E2E-07: Temporary OIDC credentials rejected with 403 on Admin API
    // -----------------------------------------------------------------------
    let (admin_status, admin_body) = admin_request_with_session_token(
        &env.url,
        Method::GET,
        "/rustfs/admin/v3/info",
        None,
        alice_creds.access_key_id(),
        alice_creds.secret_access_key(),
        Some(alice_creds.session_token()),
    )
    .await?;
    assert_eq!(
        admin_status.as_u16(),
        403,
        "E2E-07: OIDC temporary credentials must get 403 on admin API, body: {admin_body}"
    );
    assert!(
        admin_body.contains("AccessDenied"),
        "E2E-07: admin API rejection must carry AccessDenied, body: {admin_body}"
    );

    // -----------------------------------------------------------------------
    // E2E-08 & P1-2 (3): Inline session policy narrows OIDC session permissions
    // -----------------------------------------------------------------------
    let inline_session_policy = json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Action": ["s3:GetObject"],
            "Resource": [format!("arn:aws:s3:::{allowed_bucket}/narrow/*")]
        }]
    })
    .to_string();
    let narrowed_assumed = sts
        .assume_role_with_web_identity()
        .role_arn("arn:aws:iam::123456789012:role/oidc-alice-narrow")
        .role_session_name("alice-narrowed")
        .web_identity_token(&alice_jwt)
        .policy(inline_session_policy)
        .send()
        .await?;
    let narrowed_creds = narrowed_assumed.credentials().ok_or("E2E-08: missing narrowed STS creds")?;
    let narrowed_s3 = s3_client_for_sts(&env.url, narrowed_creds);

    // Within intersection (group allows all on allowed_bucket/*, session policy allows GetObject on narrow/*).
    let narrow_obj = narrowed_s3
        .get_object()
        .bucket(allowed_bucket)
        .key("narrow/readable.txt")
        .send()
        .await?;
    assert_eq!(narrow_obj.body.collect().await?.into_bytes().as_ref(), b"narrow-ok");

    // Outside session policy prefix (allowed by group policy, denied by session policy intersection).
    assert_s3_access_denied(
        narrowed_s3
            .get_object()
            .bucket(allowed_bucket)
            .key("other/blocked.txt")
            .send()
            .await,
        "E2E-08 session policy narrows prefix",
    );
    // Outside session policy action (PutObject allowed by group policy, denied by session policy).
    assert_s3_access_denied(
        narrowed_s3
            .put_object()
            .bucket(allowed_bucket)
            .key("narrow/denied-write.txt")
            .body(ByteStream::from_static(b"nope"))
            .send()
            .await,
        "E2E-08 session policy narrows action",
    );

    // -----------------------------------------------------------------------
    // E2E-09: DurationSeconds clamping to [900, 43200]
    // -----------------------------------------------------------------------
    let now_before_min = OffsetDateTime::now_utc().unix_timestamp();
    let min_assumed = sts
        .assume_role_with_web_identity()
        .role_arn("arn:aws:iam::123456789012:role/oidc-min")
        .role_session_name("duration-min")
        .web_identity_token(&alice_jwt)
        .duration_seconds(60)
        .send()
        .await?;
    let min_exp = min_assumed
        .credentials()
        .ok_or("E2E-09 min: missing creds")?
        .expiration()
        .secs();
    let min_ttl = min_exp - now_before_min;
    assert!(
        (840..=960).contains(&min_ttl),
        "E2E-09: DurationSeconds=60 must clamp to ~900s, got ttl={min_ttl}s"
    );

    let now_before_max = OffsetDateTime::now_utc().unix_timestamp();
    let max_assumed = sts
        .assume_role_with_web_identity()
        .role_arn("arn:aws:iam::123456789012:role/oidc-max")
        .role_session_name("duration-max")
        .web_identity_token(&alice_jwt)
        .duration_seconds(999_999)
        .send()
        .await?;
    let max_exp = max_assumed
        .credentials()
        .ok_or("E2E-09 max: missing creds")?
        .expiration()
        .secs();
    let max_ttl = max_exp - now_before_max;
    assert!(
        (43_140..=43_260).contains(&max_ttl),
        "E2E-09: DurationSeconds=999999 must clamp to ~43200s, got ttl={max_ttl}s"
    );

    // -----------------------------------------------------------------------
    // E2E-10: refresh_token -> new id_token -> AssumeRoleWithWebIdentity succeeds
    // -----------------------------------------------------------------------
    let token_resp_text = local_http_client()
        .post(format!("{}/token", idp.base))
        .header("content-type", "application/x-www-form-urlencoded")
        .body(format!("grant_type=refresh_token&refresh_token=e2e-refresh-token&client_id={CLIENT_ID}"))
        .send()
        .await?
        .error_for_status()?
        .text()
        .await?;
    let token_resp: Value = serde_json::from_str(&token_resp_text)?;
    let refreshed_jwt = token_resp["id_token"].as_str().ok_or("E2E-10: missing refreshed id_token")?;
    assert_eq!(idp.token_hits.load(Ordering::SeqCst), 1);
    let refreshed_assumed = sts
        .assume_role_with_web_identity()
        .role_arn("arn:aws:iam::123456789012:role/oidc-refreshed")
        .role_session_name("refreshed-session")
        .web_identity_token(refreshed_jwt)
        .send()
        .await?;
    assert_eq!(refreshed_assumed.subject_from_web_identity_token(), Some("alice-refreshed"));
    let refreshed_s3 = s3_client_for_sts(&env.url, refreshed_assumed.credentials().ok_or("E2E-10: missing creds")?);
    let refreshed_obj = refreshed_s3
        .get_object()
        .bucket(allowed_bucket)
        .key("narrow/readable.txt")
        .send()
        .await?;
    assert_eq!(refreshed_obj.body.collect().await?.into_bytes().as_ref(), b"narrow-ok");

    // -----------------------------------------------------------------------
    // E2E-11: No groups and no ROLE_POLICY -> rejected (no policies available)
    // -----------------------------------------------------------------------
    let no_groups_jwt = idp.issue_id_token("orphan-sub", json!(CLIENT_ID), None)?;
    let no_groups_err = sts
        .assume_role_with_web_identity()
        .role_arn("arn:aws:iam::123456789012:role/oidc-orphan")
        .role_session_name("orphan-session")
        .web_identity_token(&no_groups_jwt)
        .send()
        .await
        .expect_err("E2E-11: token without groups or ROLE_POLICY must be rejected");
    let no_groups_status = no_groups_err.raw_response().map(|r| r.status().as_u16());
    let no_groups_msg = no_groups_err
        .as_service_error()
        .and_then(StsProvideErrorMetadata::message)
        .unwrap_or_default()
        .to_ascii_lowercase();
    assert!(
        matches!(no_groups_status, Some(400 | 403)),
        "E2E-11: expected 400/403, got {no_groups_err:?}"
    );
    assert!(
        no_groups_msg.contains("no policies"),
        "E2E-11: expected no-policies message, got: {no_groups_err:?}"
    );

    // -----------------------------------------------------------------------
    // E2E-12: groups claim pointing only to a nonexistent policy -> rejected
    // -----------------------------------------------------------------------
    let missing_policy_jwt = idp.issue_id_token("ghost-sub", json!(CLIENT_ID), Some(&["nonexistent-policy-xyz"]))?;
    let missing_policy_err = sts
        .assume_role_with_web_identity()
        .role_arn("arn:aws:iam::123456789012:role/oidc-ghost")
        .role_session_name("ghost-session")
        .web_identity_token(&missing_policy_jwt)
        .send()
        .await
        .expect_err("E2E-12: token referencing only nonexistent policies must be rejected");
    assert!(
        matches!(missing_policy_err.raw_response().map(|r| r.status().as_u16()), Some(400 | 403)),
        "E2E-12: expected 400/403 for nonexistent policy group, got {missing_policy_err:?}"
    );

    // -----------------------------------------------------------------------
    // P1-2 Multi-audience E2E: client_id + OTHER_AUDIENCES allowed; missing
    // client_id or untrusted extra audience rejected
    // -----------------------------------------------------------------------
    let multi_aud_ok_jwt = idp.issue_id_token("multi-aud-sub", json!([CLIENT_ID, EXTRA_AUDIENCE]), Some(&["alice-rw"]))?;
    let multi_aud_assumed = sts
        .assume_role_with_web_identity()
        .role_arn("arn:aws:iam::123456789012:role/oidc-multi-aud")
        .role_session_name("multi-aud-ok")
        .web_identity_token(&multi_aud_ok_jwt)
        .send()
        .await?;
    let multi_aud_s3 = s3_client_for_sts(&env.url, multi_aud_assumed.credentials().ok_or("multi-aud creds")?);
    assert_eq!(
        multi_aud_s3
            .get_object()
            .bucket(allowed_bucket)
            .key("narrow/readable.txt")
            .send()
            .await?
            .body
            .collect()
            .await?
            .into_bytes()
            .as_ref(),
        b"narrow-ok"
    );

    for (label, aud) in [
        ("OTHER_AUDIENCES without client_id", json!([EXTRA_AUDIENCE])),
        ("client_id with untrusted extra audience", json!([CLIENT_ID, "untrusted-aud"])),
    ] {
        let bad_jwt = idp.issue_id_token("bad-aud-sub", aud, Some(&["alice-rw"]))?;
        let bad_err = sts
            .assume_role_with_web_identity()
            .role_arn("arn:aws:iam::123456789012:role/oidc-bad-aud")
            .role_session_name("bad-aud")
            .web_identity_token(&bad_jwt)
            .send()
            .await
            .expect_err(&format!("P1-2 {label}: must be rejected"));
        assert_eq!(
            bad_err.raw_response().map(|r| r.status().as_u16()),
            Some(403),
            "P1-2 {label}: expected HTTP 403, got {bad_err:?}"
        );
    }

    // -----------------------------------------------------------------------
    // E2E-13: 10 concurrent AssumeRoleWithWebIdentity exchanges all succeed
    // -----------------------------------------------------------------------
    let mut join_set = JoinSet::new();
    for idx in 0..10usize {
        let endpoint = env.url.clone();
        let jwt = idp.issue_id_token(&format!("concurrent-user-{idx}"), json!(CLIENT_ID), Some(&["alice-rw"]))?;
        join_set.spawn(async move {
            let client = build_test_sts_client(&endpoint, "unused", "unused", None, "e2e-oidc-concurrent");
            let resp = client
                .assume_role_with_web_identity()
                .role_arn("arn:aws:iam::123456789012:role/oidc-concurrent")
                .role_session_name(format!("concurrent-{idx}"))
                .web_identity_token(&jwt)
                .send()
                .await?;
            let creds = resp.credentials().ok_or("concurrent exchange missing credentials")?;
            let s3 = s3_client_for_sts(&endpoint, creds);
            let body = s3
                .get_object()
                .bucket(allowed_bucket)
                .key("narrow/readable.txt")
                .send()
                .await?
                .body
                .collect()
                .await?
                .into_bytes();
            assert_eq!(body.as_ref(), b"narrow-ok");
            Ok::<String, BoxError>(creds.access_key_id().to_string())
        });
    }
    let mut issued_keys = std::collections::BTreeSet::new();
    while let Some(res) = join_set.join_next().await {
        let key = res??;
        assert!(issued_keys.insert(key), "E2E-13: concurrent exchanges should mint unique access keys");
    }
    assert_eq!(issued_keys.len(), 10, "E2E-13: all 10 concurrent exchanges must succeed");

    env.stop_server();
    Ok(())
}
