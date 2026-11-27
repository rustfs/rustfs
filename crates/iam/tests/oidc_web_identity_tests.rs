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

//! OIDC Web Identity (`AssumeRoleWithWebIdentity`) verification, multi-audience,
//! concurrent JWKS refresh, multi-provider routing, refresh-token lifecycle, and
//! clock-skew/claim-boundary integration tests (`rustfs/backlog#2724`).
//!
//! Exercises the real public `rustfs_iam` OIDC discovery, JWKS verification,
//! and `FederatedIdentityService::assume_role_with_web_identity` pipeline
//! against local mock HTTP OIDC providers without modifying `crates/iam/src/oidc/`.

use async_trait::async_trait;
use base64_simd::URL_SAFE_NO_PAD;
use jsonwebtoken::{Algorithm, EncodingKey, Header};
use rustfs_credentials::Credentials;
use rustfs_iam::build_oidc_sys_with_extra_root_ca_provider;
use rustfs_iam::federation::{
    CoreFederatedAuthorizationMapper, FederatedIdentityService, FederatedSessionBinding, FederatedSessionBindingError,
    FederatedSessionTransaction, FederationError,
};
use rustfs_iam::oidc::{OidcExtraRootCaMaterial, OidcExtraRootCaProvider, StandardOidcAdapter};
use serde_json::{Value, json};
use serial_test::serial;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::Notify;
use tokio::time::{Duration, timeout};

struct RecordingBinding;

#[async_trait]
impl FederatedSessionBinding for RecordingBinding {
    async fn bind(&self, transaction: &FederatedSessionTransaction) -> Result<Credentials, FederatedSessionBindingError> {
        Ok(Credentials {
            access_key: format!("STS-{}", transaction.authorization.provider_id()),
            secret_key: "synthetic-sts-secret".to_string(),
            session_token: "synthetic-sts-token".to_string(),
            parent_user: transaction.authorization.subject().to_string(),
            ..Default::default()
        })
    }
}

fn es256_key_and_jwk(kid: &str) -> (EncodingKey, Value, String) {
    let certified = rcgen::generate_simple_self_signed(vec![format!("{kid}.invalid")]).expect("generate test ECDSA cert");
    let pem = certified.signing_key.serialize_pem();
    let encoding_key = EncodingKey::from_ec_pem(pem.as_bytes()).expect("encode test ES256 key");
    let mut jwk = jsonwebtoken::jwk::Jwk::from_encoding_key(&encoding_key, Algorithm::ES256).expect("derive public JWK");
    jwk.common.key_id = Some(kid.to_string());
    jwk.common.public_key_use = Some(jsonwebtoken::jwk::PublicKeyUse::Signature);
    (encoding_key, serde_json::to_value(jwk).expect("serialize JWK"), pem)
}

fn sign_es256(kid: &str, claims: &Value, key: &EncodingKey) -> String {
    let mut header = Header::new(Algorithm::ES256);
    header.kid = Some(kid.to_string());
    jsonwebtoken::encode(&header, claims, key).expect("sign ES256 JWT")
}

struct IdpRealm {
    issuer: String,
    client_id: String,
    discovery_doc: String,
    active_key: Mutex<(String, EncodingKey)>,
    active_jwks: Mutex<String>,
    jwks_fetches: AtomicUsize,
    discovery_fetches: AtomicUsize,
    token_fetches: AtomicUsize,
    pause_next_jwks: Mutex<Option<(Arc<Notify>, Arc<Notify>)>>,
}

/// Process-wide single-origin loopback HTTP router for mock OIDC realms.
///
/// `rustfs_utils::egress` caches `RUSTFS_OUTBOUND_ALLOW_ORIGINS` once per
/// process in a `OnceLock`. Hosting every test's mock IdP realm under a distinct
/// path prefix (`http://127.0.0.1:<port>/idp-<n>`) on one shared origin keeps
/// all tests isolated while staying within that single allowlisted origin under
/// both `cargo test` (single-process) and `cargo nextest` (per-test process).
struct MockIdpHub {
    origin: String,
    realms: Arc<Mutex<HashMap<String, Arc<IdpRealm>>>>,
    next_id: AtomicUsize,
}

impl MockIdpHub {
    fn global() -> &'static Self {
        static HUB: OnceLock<MockIdpHub> = OnceLock::new();
        HUB.get_or_init(|| {
            let realms: Arc<Mutex<HashMap<String, Arc<IdpRealm>>>> = Arc::new(Mutex::new(HashMap::new()));
            let server_realms = Arc::clone(&realms);
            let (tx, rx) = std::sync::mpsc::channel::<String>();
            std::thread::Builder::new()
                .name("oidc-mock-idp-hub".to_string())
                .spawn(move || {
                    let rt = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .expect("build mock OIDC hub runtime");
                    rt.block_on(async move {
                        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind mock OIDC hub");
                        let origin = format!("http://{}", listener.local_addr().expect("mock OIDC hub addr"));
                        tx.send(origin).expect("send mock OIDC hub origin");
                        loop {
                            let Ok((mut stream, _)) = listener.accept().await else {
                                break;
                            };
                            let realms = Arc::clone(&server_realms);
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
                                let content_length: usize = head
                                    .lines()
                                    .find_map(|line| {
                                        let (name, val) = line.split_once(':')?;
                                        name.eq_ignore_ascii_case("content-length")
                                            .then(|| val.trim().parse().ok())
                                            .flatten()
                                    })
                                    .unwrap_or(0);
                                while buf.len() < header_end + content_length {
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
                                let raw_path = parts.next().unwrap_or_default();
                                let path = raw_path.split('?').next().unwrap_or(raw_path);
                                let trimmed = path.trim_start_matches('/');
                                let (realm_id, subpath) = match trimmed.split_once('/') {
                                    Some((id, rest)) => (id, format!("/{rest}")),
                                    None => (trimmed, "/".to_string()),
                                };

                                let realm = realms.lock().expect("realms mutex").get(realm_id).cloned();
                                let (status, body) = match realm {
                                    None => (404, json!({"error": "unknown_realm"}).to_string()),
                                    Some(realm) if subpath == "/.well-known/openid-configuration" => {
                                        realm.discovery_fetches.fetch_add(1, Ordering::SeqCst);
                                        (200, realm.discovery_doc.clone())
                                    }
                                    Some(realm) if subpath == "/jwks" => {
                                        realm.jwks_fetches.fetch_add(1, Ordering::SeqCst);
                                        let barrier = realm.pause_next_jwks.lock().expect("pause mutex").take();
                                        if let Some((entered, release)) = barrier {
                                            let released = release.notified();
                                            tokio::pin!(released);
                                            released.as_mut().enable();
                                            entered.notify_one();
                                            let _ = timeout(Duration::from_secs(5), released).await;
                                        }
                                        (200, realm.active_jwks.lock().expect("jwks mutex").clone())
                                    }
                                    Some(realm) if method == "POST" && subpath == "/token" => {
                                        let req_body = String::from_utf8_lossy(&buf[header_end..]);
                                        if req_body.contains("grant_type=refresh_token")
                                            && req_body.contains("refresh_token=valid-refresh-token")
                                        {
                                            let count = realm.token_fetches.fetch_add(1, Ordering::SeqCst);
                                            let now = time::OffsetDateTime::now_utc().unix_timestamp();
                                            let (kid, key) = realm.active_key.lock().expect("key mutex").clone();
                                            let refreshed = sign_es256(
                                                &kid,
                                                &json!({
                                                    "iss": realm.issuer,
                                                    "sub": "refresh-user",
                                                    "aud": realm.client_id,
                                                    "iat": now,
                                                    "exp": now + 600,
                                                    "jti": format!("refreshed-{count}"),
                                                    "groups": ["readwrite"],
                                                }),
                                                &key,
                                            );
                                            (
                                                200,
                                                json!({
                                                    "access_token": "synthetic-access-token",
                                                    "token_type": "Bearer",
                                                    "expires_in": 600,
                                                    "refresh_token": "rotated-refresh-token",
                                                    "id_token": refreshed,
                                                })
                                                .to_string(),
                                            )
                                        } else {
                                            (400, json!({"error": "invalid_grant"}).to_string())
                                        }
                                    }
                                    Some(_) => (404, json!({"error": "not_found"}).to_string()),
                                };

                                let reason = match status {
                                    200 => "OK",
                                    400 => "Bad Request",
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
                })
                .expect("spawn mock OIDC hub thread");
            let origin = rx.recv().expect("receive mock OIDC hub origin");
            MockIdpHub {
                origin,
                realms,
                next_id: AtomicUsize::new(1),
            }
        })
    }

    fn origin(&self) -> &str {
        &self.origin
    }
}

struct MockIdp {
    realm_id: String,
    base: String,
    realm: Arc<IdpRealm>,
}

impl Drop for MockIdp {
    fn drop(&mut self) {
        if let Ok(mut map) = MockIdpHub::global().realms.lock() {
            map.remove(&self.realm_id);
        }
    }
}

impl MockIdp {
    async fn start(kid: &str, client_id: &str) -> Self {
        let hub = MockIdpHub::global();
        let seq = hub.next_id.fetch_add(1, Ordering::SeqCst);
        let realm_id = format!("idp-{seq}");
        let base = format!("{}/{realm_id}", hub.origin());
        let (key, jwk, _pem) = es256_key_and_jwk(kid);
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

        let realm = Arc::new(IdpRealm {
            issuer: base.clone(),
            client_id: client_id.to_string(),
            discovery_doc,
            active_key: Mutex::new((kid.to_string(), key)),
            active_jwks: Mutex::new(json!({"keys": [jwk]}).to_string()),
            jwks_fetches: AtomicUsize::new(0),
            discovery_fetches: AtomicUsize::new(0),
            token_fetches: AtomicUsize::new(0),
            pause_next_jwks: Mutex::new(None),
        });
        hub.realms
            .lock()
            .expect("realms mutex")
            .insert(realm_id.clone(), Arc::clone(&realm));

        Self { realm_id, base, realm }
    }

    fn sign_claims(&self, claims: &Value) -> String {
        let (kid, key) = self.realm.active_key.lock().expect("key mutex").clone();
        sign_es256(&kid, claims, &key)
    }

    fn current_jwks_json(&self) -> String {
        self.realm.active_jwks.lock().expect("jwks mutex").clone()
    }

    fn rotate_key(&self, new_kid: &str) {
        let (key, jwk, _pem) = es256_key_and_jwk(new_kid);
        *self.realm.active_key.lock().expect("key mutex") = (new_kid.to_string(), key);
        *self.realm.active_jwks.lock().expect("jwks mutex") = json!({"keys": [jwk]}).to_string();
    }

    fn jwks_fetches(&self) -> usize {
        self.realm.jwks_fetches.load(Ordering::SeqCst)
    }

    fn discovery_fetches(&self) -> usize {
        self.realm.discovery_fetches.load(Ordering::SeqCst)
    }

    fn token_fetches(&self) -> usize {
        self.realm.token_fetches.load(Ordering::SeqCst)
    }

    fn pause_next_jwks(&self, entered: Arc<Notify>, release: Arc<Notify>) {
        *self.realm.pause_next_jwks.lock().expect("pause mutex") = Some((entered, release));
    }

    fn standard_claims(&self, sub: &str, groups: &[&str]) -> Value {
        let now = time::OffsetDateTime::now_utc().unix_timestamp();
        json!({
            "iss": self.base,
            "sub": sub,
            "aud": self.realm.client_id,
            "iat": now,
            "exp": now + 300,
            "groups": groups,
        })
    }
}

fn empty_ca_provider() -> OidcExtraRootCaProvider {
    OidcExtraRootCaProvider::new(|| async { Ok(OidcExtraRootCaMaterial::default()) })
}

async fn build_federated_service(env_vars: Vec<(&str, Option<&str>)>) -> FederatedIdentityService {
    let hub_origin = MockIdpHub::global().origin();
    let mut vars: Vec<(&str, Option<&str>)> = vec![
        ("NO_PROXY", Some("127.0.0.1,localhost")),
        ("no_proxy", Some("127.0.0.1,localhost")),
        ("HTTP_PROXY", None),
        ("HTTPS_PROXY", None),
        ("http_proxy", None),
        ("https_proxy", None),
        ("RUSTFS_OUTBOUND_ALLOW_ORIGINS", Some(hub_origin)),
        ("RUSTFS_IDENTITY_OPENID_ENABLE", None),
        ("RUSTFS_IDENTITY_OPENID_CONFIG_URL", None),
        ("RUSTFS_IDENTITY_OPENID_CLIENT_ID", None),
        ("RUSTFS_IDENTITY_OPENID_CLIENT_SECRET", None),
        ("RUSTFS_IDENTITY_OPENID_ISSUER", None),
        ("RUSTFS_IDENTITY_OPENID_OTHER_AUDIENCES", None),
        ("RUSTFS_IDENTITY_OPENID_CLAIM_NAME", None),
        ("RUSTFS_IDENTITY_OPENID_CLAIM_PREFIX", None),
        ("RUSTFS_IDENTITY_OPENID_ROLE_POLICY", None),
        ("RUSTFS_IDENTITY_OPENID_GROUPS_CLAIM", None),
        ("RUSTFS_IDENTITY_OPENID_ROLES_CLAIM", None),
        ("RUSTFS_IDENTITY_OPENID_HIDE_FROM_UI", None),
        ("RUSTFS_IDENTITY_OPENID_ENABLE_ALPHA", None),
        ("RUSTFS_IDENTITY_OPENID_CONFIG_URL_ALPHA", None),
        ("RUSTFS_IDENTITY_OPENID_CLIENT_ID_ALPHA", None),
        ("RUSTFS_IDENTITY_OPENID_CLAIM_NAME_ALPHA", None),
        ("RUSTFS_IDENTITY_OPENID_CLAIM_PREFIX_ALPHA", None),
        ("RUSTFS_IDENTITY_OPENID_ENABLE_BETA", None),
        ("RUSTFS_IDENTITY_OPENID_CONFIG_URL_BETA", None),
        ("RUSTFS_IDENTITY_OPENID_CLIENT_ID_BETA", None),
        ("RUSTFS_IDENTITY_OPENID_CLAIM_NAME_BETA", None),
        ("RUSTFS_IDENTITY_OPENID_CLAIM_PREFIX_BETA", None),
    ];
    vars.extend(env_vars);
    let oidc = temp_env::async_with_vars(vars, async {
        build_oidc_sys_with_extra_root_ca_provider(empty_ca_provider())
            .await
            .expect("build OIDC sys from env")
    })
    .await;
    let adapter = Arc::new(StandardOidcAdapter::new(oidc));
    let mapper = CoreFederatedAuthorizationMapper::new(adapter.authorization_rules());
    adapter.into_service(mapper)
}

fn assert_token_verification_err(
    result: Result<rustfs_iam::federation::FederatedSession, FederationError>,
    context: &str,
) -> String {
    match result {
        Err(FederationError::TokenVerification(msg)) => {
            assert!(!msg.trim().is_empty(), "{context}: error message must not be empty");
            msg
        }
        Err(other) => panic!("{context}: expected FederationError::TokenVerification, got {other:?}"),
        Ok(session) => panic!(
            "{context}: expected rejection, but verification succeeded for subject={}",
            session.authorization.subject()
        ),
    }
}

/// Covers backlog#2724 P0-3: UT-03 (tampered payload), UT-06 (`alg=none`),
/// UT-07 (HS256 algorithm confusion), and UT-08 (empty/garbage/malformed tokens),
/// alongside a valid positive control to prove verification is wired live.
#[tokio::test]
#[serial]
async fn web_identity_rejects_tampered_payload_alg_none_hs256_confusion_and_malformed_tokens() {
    let idp = MockIdp::start("ut-key-1", "rustfs-ut-client").await;
    let client_secret = "synthetic-oidc-client-secret-for-ut07-confusion-check";
    let config_url = format!("{}/.well-known/openid-configuration", idp.base);
    let service = build_federated_service(vec![
        ("RUSTFS_IDENTITY_OPENID_ENABLE", Some("on")),
        ("RUSTFS_IDENTITY_OPENID_CONFIG_URL", Some(&config_url)),
        ("RUSTFS_IDENTITY_OPENID_CLIENT_ID", Some("rustfs-ut-client")),
        ("RUSTFS_IDENTITY_OPENID_CLIENT_SECRET", Some(client_secret)),
        ("RUSTFS_IDENTITY_OPENID_CLAIM_NAME", Some("groups")),
    ])
    .await;
    assert_eq!(idp.discovery_fetches(), 1, "initial discovery should hit mock IdP once");

    // Positive control: valid token succeeds.
    let valid_claims = idp.standard_claims("alice", &["readonly"]);
    let valid_token = idp.sign_claims(&valid_claims);
    let session = service
        .assume_role_with_web_identity(&valid_token, 3600, None, &RecordingBinding)
        .await
        .expect("valid ES256 token must verify");
    assert_eq!(session.authorization.subject(), "alice");
    assert_eq!(session.authorization.policies(), ["readonly"]);

    // UT-03: Tampered payload (escalate groups to consoleAdmin) while keeping original header & signature.
    let segments: Vec<&str> = valid_token.split('.').collect();
    assert_eq!(segments.len(), 3, "JWT must have 3 dot-separated segments");
    let mut escalated_claims = valid_claims.clone();
    escalated_claims["groups"] = json!(["consoleAdmin"]);
    let tampered_payload_b64 = URL_SAFE_NO_PAD.encode_to_string(serde_json::to_vec(&escalated_claims).unwrap());
    let ut03_token = format!("{}.{}.{}", segments[0], tampered_payload_b64, segments[2]);
    let ut03_err = assert_token_verification_err(
        service
            .assume_role_with_web_identity(&ut03_token, 3600, None, &RecordingBinding)
            .await,
        "UT-03 tampered payload",
    );
    assert!(
        ut03_err.to_ascii_lowercase().contains("signature") || ut03_err.to_ascii_lowercase().contains("verification"),
        "UT-03 error should report signature failure, got: {ut03_err}"
    );

    // UT-06: alg=none unsigned JWT (both empty signature and non-empty bogus signature).
    let none_header_b64 = URL_SAFE_NO_PAD.encode_to_string(br#"{"alg":"none","typ":"JWT"}"#);
    let payload_b64 = URL_SAFE_NO_PAD.encode_to_string(serde_json::to_vec(&valid_claims).unwrap());
    for (label, token) in [
        ("UT-06 alg=none empty sig", format!("{none_header_b64}.{payload_b64}.")),
        ("UT-06 alg=none bogus sig", format!("{none_header_b64}.{payload_b64}.ZmFrZXNpZw")),
        (
            "UT-06 alg=None mixed case",
            format!("{}.{payload_b64}.", URL_SAFE_NO_PAD.encode_to_string(br#"{"alg":"None","typ":"JWT"}"#)),
        ),
    ] {
        assert_token_verification_err(
            service
                .assume_role_with_web_identity(&token, 3600, None, &RecordingBinding)
                .await,
            label,
        );
    }

    // UT-07: HS256 algorithm confusion against asymmetric ES256 provider
    // (signed with client_secret AND signed with the public JWKS document bytes).
    for (label, secret_bytes) in [
        ("UT-07 HS256 with client_secret", client_secret.as_bytes().to_vec()),
        ("UT-07 HS256 with public JWKS bytes", idp.current_jwks_json().into_bytes()),
    ] {
        let mut hs_header = Header::new(Algorithm::HS256);
        hs_header.kid = Some("ut-key-1".to_string());
        let hs_key = EncodingKey::from_secret(&secret_bytes).expect("construct HS256 key");
        let hs_token = jsonwebtoken::encode(&hs_header, &valid_claims, &hs_key).expect("sign HS256 confusion token");
        assert_token_verification_err(
            service
                .assume_role_with_web_identity(&hs_token, 3600, None, &RecordingBinding)
                .await,
            label,
        );
    }

    // UT-08: Empty, whitespace, garbage, truncated, invalid base64, and missing-iss tokens.
    let no_iss_claims = {
        let now = time::OffsetDateTime::now_utc().unix_timestamp();
        json!({"sub": "alice", "aud": "rustfs-ut-client", "iat": now, "exp": now + 300, "groups": ["readonly"]})
    };
    let no_iss_token = idp.sign_claims(&no_iss_claims);
    for (label, bad_token) in [
        ("UT-08 empty", ""),
        ("UT-08 whitespace", "   \t\n "),
        ("UT-08 garbage", "not-a-jwt-at-all"),
        ("UT-08 two segments", "eyJhbGciOiJFUzI1NiJ9.eyJpc3MiOiJ4In0"),
        ("UT-08 invalid base64 payload", "eyJhbGciOiJFUzI1NiJ9.!!!not-base64!!!.sig"),
        ("UT-08 non-JSON payload", "eyJhbGciOiJFUzI1NiJ9.bm90LWpzb24.sig"),
        ("UT-08 missing iss claim", no_iss_token.as_str()),
    ] {
        assert_token_verification_err(
            service
                .assume_role_with_web_identity(bad_token, 3600, None, &RecordingBinding)
                .await,
            label,
        );
    }
}

/// Covers backlog#2724 P1-2 (1): `OTHER_AUDIENCES` positive multi-audience token
/// acceptance, rejection of untrusted additional audience, and OIDC Core 3.1.3.7
/// requirement that `client_id` must still be present in `aud`.
#[tokio::test]
#[serial]
async fn web_identity_enforces_other_audiences_semantics() {
    let idp = MockIdp::start("aud-key-1", "rustfs-primary-client").await;
    let config_url = format!("{}/.well-known/openid-configuration", idp.base);
    let service = build_federated_service(vec![
        ("RUSTFS_IDENTITY_OPENID_ENABLE", Some("on")),
        ("RUSTFS_IDENTITY_OPENID_CONFIG_URL", Some(&config_url)),
        ("RUSTFS_IDENTITY_OPENID_CLIENT_ID", Some("rustfs-primary-client")),
        ("RUSTFS_IDENTITY_OPENID_OTHER_AUDIENCES", Some("partner-app,analytics-svc")),
        ("RUSTFS_IDENTITY_OPENID_CLAIM_NAME", Some("groups")),
    ])
    .await;

    // Positive: aud contains both client_id and a configured OTHER_AUDIENCES entry.
    let mut multi_aud_ok = idp.standard_claims("multi-aud-user", &["readwrite"]);
    multi_aud_ok["aud"] = json!(["rustfs-primary-client", "partner-app"]);
    let ok_session = service
        .assume_role_with_web_identity(&idp.sign_claims(&multi_aud_ok), 3600, None, &RecordingBinding)
        .await
        .expect("token with client_id + configured OTHER_AUDIENCES must succeed");
    assert_eq!(ok_session.authorization.subject(), "multi-aud-user");
    assert_eq!(ok_session.authorization.policies(), ["readwrite"]);

    // Negative: aud contains client_id plus an UNLISTED additional audience.
    let mut multi_aud_untrusted = idp.standard_claims("multi-aud-user", &["readwrite"]);
    multi_aud_untrusted["aud"] = json!(["rustfs-primary-client", "untrusted-third-party"]);
    let err = assert_token_verification_err(
        service
            .assume_role_with_web_identity(&idp.sign_claims(&multi_aud_untrusted), 3600, None, &RecordingBinding)
            .await,
        "untrusted extra audience",
    );
    assert!(err.to_ascii_lowercase().contains("audience"), "expected audience rejection, got: {err}");

    // Negative: aud contains ONLY an OTHER_AUDIENCES value without client_id (OIDC Core 3.1.3.7).
    let mut missing_client_id = idp.standard_claims("multi-aud-user", &["readwrite"]);
    missing_client_id["aud"] = json!(["partner-app"]);
    let err = assert_token_verification_err(
        service
            .assume_role_with_web_identity(&idp.sign_claims(&missing_client_id), 3600, None, &RecordingBinding)
            .await,
        "OTHER_AUDIENCES without client_id",
    );
    assert!(
        err.to_ascii_lowercase().contains("audience"),
        "expected audience rejection when client_id is absent, got: {err}"
    );
}

/// Covers backlog#2724 P1-2 (2): concurrent web-identity verification across an
/// active JWKS rotation window remains consistent and single-flight.
#[tokio::test]
#[serial]
async fn web_identity_concurrent_verifications_stay_consistent_during_jwks_rotation() {
    let idp = MockIdp::start("conc-key-1", "rustfs-conc-client").await;
    let config_url = format!("{}/.well-known/openid-configuration", idp.base);
    let service = Arc::new(
        build_federated_service(vec![
            ("RUSTFS_IDENTITY_OPENID_ENABLE", Some("on")),
            ("RUSTFS_IDENTITY_OPENID_CONFIG_URL", Some(&config_url)),
            ("RUSTFS_IDENTITY_OPENID_CLIENT_ID", Some("rustfs-conc-client")),
            ("RUSTFS_IDENTITY_OPENID_CLAIM_NAME", Some("groups")),
        ])
        .await,
    );

    let initial_jwks_fetches = idp.jwks_fetches();
    assert_eq!(initial_jwks_fetches, 1, "initial discovery should fetch JWKS once");

    // Rotate key on the IdP and install a deterministic barrier on the next JWKS fetch.
    idp.rotate_key("conc-key-2");
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    idp.pause_next_jwks(Arc::clone(&entered), Arc::clone(&release));

    let rotated_token = idp.sign_claims(&idp.standard_claims("rotated-concurrent", &["readwrite"]));
    let mut tasks = Vec::new();
    for _ in 0..12 {
        let service = Arc::clone(&service);
        let token = rotated_token.clone();
        tasks.push(tokio::spawn(async move {
            service
                .assume_role_with_web_identity(&token, 3600, None, &RecordingBinding)
                .await
        }));
    }

    timeout(Duration::from_secs(5), entered.notified())
        .await
        .expect("at least one concurrent task should enter JWKS refresh");
    release.notify_one();

    for handle in tasks {
        let session = timeout(Duration::from_secs(5), handle)
            .await
            .expect("concurrent verification timeout")
            .expect("concurrent verification join")
            .expect("all concurrent verifications must succeed after JWKS refresh");
        assert_eq!(session.authorization.subject(), "rotated-concurrent");
        assert_eq!(session.authorization.policies(), ["readwrite"]);
    }
    assert_eq!(
        idp.jwks_fetches(),
        initial_jwks_fetches + 1,
        "concurrent callers must coalesce onto a single JWKS refresh"
    );
}

/// Covers backlog#2724 P2-2 (1): two simultaneously enabled OIDC providers route
/// strictly by `iss`, reject cross-provider signatures, and rotate JWKS independently.
#[tokio::test]
#[serial]
async fn web_identity_routes_multiple_providers_by_issuer_with_independent_jwks_rotation() {
    let idp_a = MockIdp::start("alpha-key-1", "client-alpha").await;
    let idp_b = MockIdp::start("beta-key-1", "client-beta").await;
    let url_a = format!("{}/.well-known/openid-configuration", idp_a.base);
    let url_b = format!("{}/.well-known/openid-configuration", idp_b.base);

    let service = build_federated_service(vec![
        ("RUSTFS_IDENTITY_OPENID_ENABLE_ALPHA", Some("on")),
        ("RUSTFS_IDENTITY_OPENID_CONFIG_URL_ALPHA", Some(&url_a)),
        ("RUSTFS_IDENTITY_OPENID_CLIENT_ID_ALPHA", Some("client-alpha")),
        ("RUSTFS_IDENTITY_OPENID_CLAIM_NAME_ALPHA", Some("groups")),
        ("RUSTFS_IDENTITY_OPENID_CLAIM_PREFIX_ALPHA", Some("alpha-")),
        ("RUSTFS_IDENTITY_OPENID_ENABLE_BETA", Some("on")),
        ("RUSTFS_IDENTITY_OPENID_CONFIG_URL_BETA", Some(&url_b)),
        ("RUSTFS_IDENTITY_OPENID_CLIENT_ID_BETA", Some("client-beta")),
        ("RUSTFS_IDENTITY_OPENID_CLAIM_NAME_BETA", Some("groups")),
        ("RUSTFS_IDENTITY_OPENID_CLAIM_PREFIX_BETA", Some("beta-")),
    ])
    .await;

    let tok_a = idp_a.sign_claims(&idp_a.standard_claims("user-a", &["reader"]));
    let tok_b = idp_b.sign_claims(&idp_b.standard_claims("user-b", &["writer"]));

    let sess_a = service
        .assume_role_with_web_identity(&tok_a, 3600, None, &RecordingBinding)
        .await
        .expect("provider alpha token must verify");
    assert_eq!(sess_a.authorization.provider_id(), "ALPHA");
    assert_eq!(sess_a.authorization.policies(), ["alpha-reader"]);

    let sess_b = service
        .assume_role_with_web_identity(&tok_b, 3600, None, &RecordingBinding)
        .await
        .expect("provider beta token must verify");
    assert_eq!(sess_b.authorization.provider_id(), "BETA");
    assert_eq!(sess_b.authorization.policies(), ["beta-writer"]);

    // Rotate ONLY alpha's signing key BEFORE any failed-signature probes on alpha
    // (which intentionally consume alpha's 60s JWKS refresh anti-DoS cooldown);
    // beta's JWKS fetch counter must remain untouched.
    let beta_fetches_before = idp_b.jwks_fetches();
    idp_a.rotate_key("alpha-key-2");
    let tok_a_rotated = idp_a.sign_claims(&idp_a.standard_claims("user-a-rotated", &["admin"]));
    let sess_a2 = service
        .assume_role_with_web_identity(&tok_a_rotated, 3600, None, &RecordingBinding)
        .await
        .expect("rotated alpha token must refresh alpha JWKS and verify");
    assert_eq!(sess_a2.authorization.provider_id(), "ALPHA");
    assert_eq!(sess_a2.authorization.policies(), ["alpha-admin"]);

    // Beta still verifies purely from cache with zero additional JWKS fetches.
    let sess_b2 = service
        .assume_role_with_web_identity(&tok_b, 3600, None, &RecordingBinding)
        .await
        .expect("beta token must continue verifying unaffected by alpha rotation");
    assert_eq!(sess_b2.authorization.provider_id(), "BETA");
    assert_eq!(
        idp_b.jwks_fetches(),
        beta_fetches_before,
        "rotating alpha must not trigger a JWKS refresh on beta"
    );

    // Cross-provider forgery: issuer claims alpha, signed with beta's key -> rejected.
    let mut forged_claims = idp_a.standard_claims("user-a", &["reader"]);
    forged_claims["iss"] = json!(idp_a.base);
    let forged_tok = idp_b.sign_claims(&forged_claims);
    assert_token_verification_err(
        service
            .assume_role_with_web_identity(&forged_tok, 3600, None, &RecordingBinding)
            .await,
        "cross-provider forged signature",
    );
}

/// Covers backlog#2724 P2-2 (2): IdP refresh_token exchange mints a fresh `id_token`
/// with updated claims/expiry, which RustFS accepts cleanly.
#[tokio::test]
#[serial]
async fn web_identity_accepts_refreshed_id_token_from_idp_token_endpoint() {
    let idp = MockIdp::start("refresh-key-1", "rustfs-refresh-client").await;
    let config_url = format!("{}/.well-known/openid-configuration", idp.base);
    let service = build_federated_service(vec![
        ("RUSTFS_IDENTITY_OPENID_ENABLE", Some("on")),
        ("RUSTFS_IDENTITY_OPENID_CONFIG_URL", Some(&config_url)),
        ("RUSTFS_IDENTITY_OPENID_CLIENT_ID", Some("rustfs-refresh-client")),
        ("RUSTFS_IDENTITY_OPENID_CLAIM_NAME", Some("groups")),
    ])
    .await;

    // Initial ID token verifies with initial policy.
    let initial_token = idp.sign_claims(&idp.standard_claims("refresh-user", &["readonly"]));
    let s1 = service
        .assume_role_with_web_identity(&initial_token, 3600, None, &RecordingBinding)
        .await
        .expect("initial id_token must verify");
    assert_eq!(s1.authorization.policies(), ["readonly"]);

    // Exchange refresh_token at the mock IdP's /token endpoint for a new id_token.
    let http = reqwest::Client::builder().no_proxy().build().expect("local reqwest client");
    let token_resp_text = http
        .post(format!("{}/token", idp.base))
        .header("content-type", "application/x-www-form-urlencoded")
        .body("grant_type=refresh_token&refresh_token=valid-refresh-token&client_id=rustfs-refresh-client")
        .send()
        .await
        .expect("call mock IdP /token")
        .error_for_status()
        .expect("mock IdP /token 200")
        .text()
        .await
        .expect("read /token body");
    let token_resp: Value = serde_json::from_str(&token_resp_text).expect("parse /token JSON");
    let refreshed_id_token = token_resp["id_token"].as_str().expect("refreshed id_token field");
    assert_ne!(refreshed_id_token, initial_token);

    let s2 = service
        .assume_role_with_web_identity(refreshed_id_token, 3600, None, &RecordingBinding)
        .await
        .expect("refreshed id_token must verify");
    assert_eq!(s2.authorization.subject(), "refresh-user");
    assert_eq!(s2.authorization.policies(), ["readwrite"]);
    assert_eq!(idp.token_fetches(), 1);
}

/// Covers backlog#2724 P2-2 (3): missing `iat` rejection, `exp` expiration boundary,
/// and `nbf` handling pinned against `openidconnect`'s verifier contract.
#[tokio::test]
#[serial]
async fn web_identity_pins_missing_iat_expiration_and_nbf_semantics() {
    let idp = MockIdp::start("clock-key-1", "rustfs-clock-client").await;
    let config_url = format!("{}/.well-known/openid-configuration", idp.base);
    let service = build_federated_service(vec![
        ("RUSTFS_IDENTITY_OPENID_ENABLE", Some("on")),
        ("RUSTFS_IDENTITY_OPENID_CONFIG_URL", Some(&config_url)),
        ("RUSTFS_IDENTITY_OPENID_CLIENT_ID", Some("rustfs-clock-client")),
        ("RUSTFS_IDENTITY_OPENID_CLAIM_NAME", Some("groups")),
    ])
    .await;

    let now = time::OffsetDateTime::now_utc().unix_timestamp();

    // Missing `iat` claim is rejected during CoreIdToken parsing (iat is required per OIDC Core 2).
    let missing_iat = json!({
        "iss": idp.base,
        "sub": "clock-user",
        "aud": "rustfs-clock-client",
        "exp": now + 300,
        "groups": ["readonly"],
    });
    let err = assert_token_verification_err(
        service
            .assume_role_with_web_identity(&idp.sign_claims(&missing_iat), 3600, None, &RecordingBinding)
            .await,
        "missing iat claim",
    );
    assert!(
        err.to_ascii_lowercase().contains("parse") || err.to_ascii_lowercase().contains("iat"),
        "missing iat should fail token parsing, got: {err}"
    );

    // Expired token (exp in the past) is rejected.
    let expired = json!({
        "iss": idp.base,
        "sub": "clock-user",
        "aud": "rustfs-clock-client",
        "iat": now - 600,
        "exp": now - 5,
        "groups": ["readonly"],
    });
    let exp_err = assert_token_verification_err(
        service
            .assume_role_with_web_identity(&idp.sign_claims(&expired), 3600, None, &RecordingBinding)
            .await,
        "expired token",
    );
    assert!(
        exp_err.to_ascii_lowercase().contains("expired"),
        "expired token error should mention expiration, got: {exp_err}"
    );

    // Valid token with past `nbf` is accepted.
    let past_nbf = json!({
        "iss": idp.base,
        "sub": "clock-user",
        "aud": "rustfs-clock-client",
        "iat": now,
        "nbf": now - 30,
        "exp": now + 300,
        "groups": ["readonly"],
    });
    let ok = service
        .assume_role_with_web_identity(&idp.sign_claims(&past_nbf), 3600, None, &RecordingBinding)
        .await
        .expect("token with past nbf must be accepted");
    assert_eq!(ok.authorization.subject(), "clock-user");

    // Pin current `openidconnect` crate behavior for `nbf` in the future:
    // OIDC Core 2 defines `exp` and `iat` as standard ID token time claims and does
    // not mandate `nbf` rejection (CoreIdTokenClaims does not deserialize `nbf`),
    // so a future `nbf` with valid `iat`/`exp` is currently accepted unless RustFS
    // adds an explicit `nbf` check. Pinning this ensures any future policy change
    // triggers an explicit test update.
    let future_nbf = json!({
        "iss": idp.base,
        "sub": "clock-user",
        "aud": "rustfs-clock-client",
        "iat": now,
        "nbf": now + 3600,
        "exp": now + 7200,
        "groups": ["readonly"],
    });
    let future_nbf_res = service
        .assume_role_with_web_identity(&idp.sign_claims(&future_nbf), 3600, None, &RecordingBinding)
        .await;
    assert!(
        future_nbf_res.is_ok(),
        "characterization pin: openidconnect CoreIdTokenVerifier ignores nbf when iat/exp are valid; got {future_nbf_res:?}"
    );
}
