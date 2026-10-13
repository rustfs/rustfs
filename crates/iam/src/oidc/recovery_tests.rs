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

use super::*;
use crate::oidc::test_support::{build_mocked_oidc_provider_config, oidc_es256_key_and_jwk, test_sourced_config};
use jsonwebtoken::{Algorithm, EncodingKey, Header};
use rustfs_utils::egress::OutboundPolicy;
use std::sync::atomic::{AtomicU16, AtomicUsize, Ordering};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_util::task::AbortOnDropHandle;

struct RecoveryIdp {
    base: String,
    config: OidcProviderConfig,
    key: EncodingKey,
    discovery_status: Arc<AtomicU16>,
    jwks_status: Arc<AtomicU16>,
    requests: Arc<AtomicUsize>,
    cancelled_requests: Arc<AtomicUsize>,
    _server: AbortOnDropHandle<()>,
}

impl RecoveryIdp {
    async fn start(id: &str, workload: bool, explicit_issuer: bool) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind recovery IdP");
        let base = format!("http://{}", listener.local_addr().expect("recovery IdP address"));
        let mut config = build_mocked_oidc_provider_config(id, &base);
        config.hide_from_ui = workload;
        config.claim_name = "groups".into();
        config.claim_prefix = "team/".into();
        config.issuer = explicit_issuer.then(|| format!("https://issuer.example.com/{id}"));
        let (key, jwk) = oidc_es256_key_and_jwk(id);
        let mut document = serde_json::json!({
            "issuer": config.issuer.as_deref().unwrap_or(&base),
            "authorization_endpoint": format!("{base}/authorize"),
            "token_endpoint": format!("{base}/token"),
            "jwks_uri": format!("{base}/jwks"),
            "response_types_supported": ["code"],
            "subject_types_supported": ["public"],
            "id_token_signing_alg_values_supported": ["ES256"],
        });
        if workload {
            let fields = document.as_object_mut().expect("discovery document");
            fields.remove("authorization_endpoint");
            fields.remove("token_endpoint");
        }
        let document = document.to_string();
        let jwks = serde_json::json!({"keys": [jwk]}).to_string();
        let discovery_status = Arc::new(AtomicU16::new(503));
        let jwks_status = Arc::new(AtomicU16::new(200));
        let requests = Arc::new(AtomicUsize::new(0));
        let cancelled_requests = Arc::new(AtomicUsize::new(0));
        let server_discovery_status = discovery_status.clone();
        let server_jwks_status = jwks_status.clone();
        let server_requests = requests.clone();
        let server_cancelled = cancelled_requests.clone();
        let server = tokio::spawn(async move {
            loop {
                let (mut stream, _) = listener.accept().await.expect("accept discovery request");
                let mut request = Vec::new();
                while !request.ends_with(b"\r\n\r\n") {
                    request.push(stream.read_u8().await.expect("read discovery request"));
                    assert!(request.len() < 8192);
                }
                let headers = String::from_utf8(request).expect("HTTP request headers");
                let path = headers
                    .lines()
                    .next()
                    .expect("request line")
                    .split_whitespace()
                    .nth(1)
                    .expect("path");
                let (status, body) = if path.ends_with("/.well-known/openid-configuration") {
                    (server_discovery_status.load(Ordering::SeqCst), document.as_str())
                } else {
                    assert_eq!(path, "/jwks");
                    (server_jwks_status.load(Ordering::SeqCst), jwks.as_str())
                };
                server_requests.fetch_add(1, Ordering::SeqCst);
                if status == 0 {
                    continue; // Simulate a transport failure before a response is available.
                }
                if status == 1 {
                    let mut byte = [0];
                    assert_eq!(stream.read(&mut byte).await.expect("client closes cancelled discovery"), 0);
                    server_cancelled.fetch_add(1, Ordering::SeqCst);
                    continue;
                }
                let body = if status == 200 { body } else { "" };
                let response = format!(
                    "HTTP/1.1 {status} Test\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                stream.write_all(response.as_bytes()).await.expect("write discovery response");
            }
        });
        Self {
            base,
            config,
            key,
            discovery_status,
            jwks_status,
            requests,
            cancelled_requests,
            _server: AbortOnDropHandle::new(server),
        }
    }

    fn token(&self, key: &EncodingKey, audience: &str) -> String {
        let mut header = Header::new(Algorithm::ES256);
        header.kid = Some(self.config.id.clone());
        let now = time::OffsetDateTime::now_utc().unix_timestamp();
        jsonwebtoken::encode(
            &header,
            &serde_json::json!({
                "iss": self.config.issuer.as_deref().unwrap_or(&self.base),
                "sub": "recovered-user", "aud": audience, "iat": now, "exp": now + 300,
                "groups": ["readonly"],
            }),
            key,
        )
        .expect("sign recovery test token")
    }
}

async fn initialize(idps: &[&RecoveryIdp]) -> Arc<OidcSys> {
    let origins = idps.iter().map(|idp| idp.base.as_str()).collect::<Vec<_>>().join(",");
    let client = ReqwestHttpClient::with_policy(OutboundPolicy::from_allowed_origins(&origins).expect("allow test IdPs"));
    let snapshot = OidcConfigSnapshot::new(idps.iter().map(|idp| test_sourced_config(idp.config.clone())).collect());
    Arc::new(
        OidcSys::new_with_config_snapshot(client, snapshot)
            .await
            .expect("initialize OIDC with unavailable IdPs"),
    )
}

async fn wait_until(mut condition: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while !condition() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("recovery condition should be reached");
}

#[tokio::test]
async fn web_identity_recovers_transport_failure_and_preserves_verification() {
    for explicit in [false, true] {
        let idp = RecoveryIdp::start("workload", true, explicit).await;
        idp.discovery_status.store(0, Ordering::SeqCst);
        let sys = initialize(&[&idp]).await;
        let adapter = StandardOidcAdapter::new(sys.clone());
        let mapper = CoreFederatedAuthorizationMapper::new(adapter.authorization_rules());
        assert!(sys.has_providers());
        assert!(sys.list_visible_providers().is_empty());
        let token = idp.token(&idp.key, &idp.config.client_id);
        assert!(adapter.verify_identity(&token).await.is_err());

        idp.discovery_status.store(200, Ordering::SeqCst);
        tokio::time::timeout(Duration::from_secs(5), sys.recover_failed_providers())
            .await
            .expect("recover workload discovery");
        let identity = adapter
            .verify_identity(&token)
            .await
            .expect("verify recovered workload token");
        let authorization = mapper.map(identity);
        assert_eq!(authorization.provider_id(), "workload");
        assert_eq!(authorization.subject(), "recovered-user");
        assert_eq!(authorization.policies(), ["team/readonly"]);
        assert!(
            sys.authorize_url("workload", "https://console.example.com/callback", None)
                .await
                .is_err()
        );

        let (wrong_key, _) = oidc_es256_key_and_jwk("wrong");
        assert!(
            adapter
                .verify_identity(&idp.token(&wrong_key, &idp.config.client_id))
                .await
                .is_err()
        );
        assert!(adapter.verify_identity(&idp.token(&idp.key, "wrong-audience")).await.is_err());
    }
}

#[tokio::test]
async fn recovery_waits_for_jwks_and_claims_pending_providers_once() {
    let idp = RecoveryIdp::start("console", false, true).await;
    idp.discovery_status.store(200, Ordering::SeqCst);
    idp.jwks_status.store(503, Ordering::SeqCst);
    let sys = initialize(&[&idp]).await;
    assert!(sys.provider_runtime.get_provider_state("console").is_err());
    let requests = idp.requests.load(Ordering::SeqCst);
    idp.jwks_status.store(200, Ordering::SeqCst);
    tokio::time::timeout(
        Duration::from_secs(5),
        futures::future::join_all((0..16).map(|_| sys.recover_failed_providers())),
    )
    .await
    .expect("concurrent recovery calls complete");
    assert!(sys.provider_runtime.get_provider_state("console").is_ok());
    assert_eq!(
        idp.requests.load(Ordering::SeqCst),
        requests + 2,
        "only one discovery/JWKS round should run"
    );
}

#[tokio::test]
async fn recovery_backoff_is_independent_of_login_traffic() {
    let idp = RecoveryIdp::start("console", false, true).await;
    let sys = initialize(&[&idp]).await;
    let requests = idp.requests.load(Ordering::SeqCst);
    let recovery_sys = sys.clone();
    let recovery = AbortOnDropHandle::new(tokio::spawn(async move { recovery_sys.recover_failed_providers().await }));
    wait_until(|| idp.requests.load(Ordering::SeqCst) > requests).await;
    assert_eq!(idp.requests.load(Ordering::SeqCst), requests + 1);
    for _ in 0..32 {
        assert!(
            sys.authorize_url("console", "https://console.example.com/callback", None)
                .await
                .is_err()
        );
    }
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        idp.requests.load(Ordering::SeqCst),
        requests + 1,
        "login attempts must not bypass discovery backoff"
    );
    idp.discovery_status.store(200, Ordering::SeqCst);
    tokio::time::timeout(Duration::from_secs(5), recovery)
        .await
        .expect("recovery deadline")
        .expect("recovery task");
    assert!(
        sys.authorize_url("console", "https://console.example.com/callback", None)
            .await
            .is_ok()
    );
}

#[tokio::test]
async fn unavailable_provider_does_not_block_another_provider() {
    let unavailable = RecoveryIdp::start("unavailable", false, true).await;
    let available = RecoveryIdp::start("available", false, true).await;
    let sys = initialize(&[&unavailable, &available]).await;
    available.discovery_status.store(200, Ordering::SeqCst);
    let recovery_sys = sys.clone();
    let recovery = AbortOnDropHandle::new(tokio::spawn(async move { recovery_sys.recover_failed_providers().await }));
    wait_until(|| sys.provider_runtime.get_provider_state("available").is_ok()).await;
    assert!(sys.provider_runtime.get_provider_state("unavailable").is_err());
    assert!(
        sys.authorize_url("available", "https://console.example.com/callback", None)
            .await
            .is_ok()
    );
    drop(recovery);
}

#[tokio::test]
async fn recovery_preserves_ready_provider_and_inflight_sessions() {
    let ready = RecoveryIdp::start("ready", false, true).await;
    let recovering = RecoveryIdp::start("recovering", false, true).await;
    ready.discovery_status.store(200, Ordering::SeqCst);
    let sys = initialize(&[&ready, &recovering]).await;
    let auth_url = sys
        .authorize_url("ready", "https://console.example.com/callback", Some("/buckets".into()))
        .await
        .expect("begin login with ready provider");
    let auth: HashMap<String, String> = url::Url::parse(&auth_url)
        .expect("authorization URL")
        .query_pairs()
        .into_owned()
        .collect();
    let logout = sys
        .create_logout_token("ready", "verified-id-token")
        .await
        .expect("retain logout continuation");
    let discovered_at = sys
        .provider_runtime
        .get_provider_state("ready")
        .expect("ready state")
        .discovered_at;
    let ready_requests = ready.requests.load(Ordering::SeqCst);

    recovering.discovery_status.store(200, Ordering::SeqCst);
    tokio::time::timeout(Duration::from_secs(5), sys.recover_failed_providers())
        .await
        .expect("recover second provider");
    assert_eq!(ready.requests.load(Ordering::SeqCst), ready_requests);
    assert_eq!(
        sys.provider_runtime
            .get_provider_state("ready")
            .expect("ready state remains")
            .discovered_at,
        discovered_at
    );
    let session = sys
        .state_store
        .take(&auth["state"])
        .await
        .expect("in-flight login remains available");
    assert_eq!(session.provider_id, "ready");
    assert_eq!(session.nonce, auth["nonce"]);
    assert_eq!(session.redirect_after.as_deref(), Some("/buckets"));
    assert_eq!(
        PkceCodeChallenge::from_code_verifier_sha256(&PkceCodeVerifier::new(session.pkce_verifier)).as_str(),
        auth["code_challenge"]
    );
    assert_eq!(
        sys.state_store
            .take_logout(&logout)
            .await
            .expect("logout continuation remains available")
            .provider_id,
        "ready"
    );
}

#[tokio::test]
async fn startup_schedules_only_recoverable_http_errors() {
    for explicit in [false, true] {
        let idp = RecoveryIdp::start("status", false, explicit).await;
        for (status, retryable) in [
            (408, true),
            (429, true),
            (500, true),
            (503, true),
            (400, false),
            (401, false),
            (403, false),
            (404, false),
        ] {
            idp.discovery_status.store(status, Ordering::SeqCst);
            let sys = initialize(&[&idp]).await;
            assert_eq!(
                sys.pending_discovery.lock().await.len(),
                usize::from(retryable),
                "explicit={explicit}, status={status}"
            );
            assert!(sys.provider_runtime.get_provider_state("status").is_err());
        }
    }
}

#[tokio::test]
async fn recovery_bounds_concurrency_and_cancels_inflight_requests() {
    let mut idps = Vec::new();
    for index in 0..6 {
        idps.push(RecoveryIdp::start(&format!("provider-{index}"), false, true).await);
    }
    let refs: Vec<_> = idps.iter().collect();
    let sys = initialize(&refs).await;
    let initial_requests: usize = idps.iter().map(|idp| idp.requests.load(Ordering::SeqCst)).sum();
    for idp in &idps {
        idp.discovery_status.store(1, Ordering::SeqCst);
    }
    let recovery_sys = sys.clone();
    let recovery = AbortOnDropHandle::new(tokio::spawn(async move { recovery_sys.recover_failed_providers().await }));
    let requests = || idps.iter().map(|idp| idp.requests.load(Ordering::SeqCst)).sum::<usize>();
    wait_until(|| requests() >= initial_requests + 4).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(requests(), initial_requests + 4, "at most four discovery rounds may run concurrently");
    drop(recovery);
    wait_until(|| {
        idps.iter()
            .map(|idp| idp.cancelled_requests.load(Ordering::SeqCst))
            .sum::<usize>()
            == 4
    })
    .await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(requests(), initial_requests + 4, "cancelled recovery must not start queued work");
    assert!(
        idps.iter()
            .all(|idp| sys.provider_runtime.get_provider_state(&idp.config.id).is_err())
    );
}

#[tokio::test]
async fn disabled_and_rejected_providers_do_not_schedule_recovery() {
    let mut disabled = RecoveryIdp::start("disabled", false, true).await;
    disabled.config.enabled = false;
    let sys = initialize(&[&disabled]).await;
    sys.recover_failed_providers().await;
    assert!(!sys.has_providers());
    assert_eq!(disabled.requests.load(Ordering::SeqCst), 0);

    let blocked = RecoveryIdp::start("blocked", false, true).await;
    let sys = OidcSys::new_with_config_snapshot(
        ReqwestHttpClient::with_policy(OutboundPolicy::default()),
        OidcConfigSnapshot::new(vec![test_sourced_config(blocked.config.clone())]),
    )
    .await
    .expect("policy rejection remains non-fatal for startup");
    assert!(sys.pending_discovery.lock().await.is_empty());
    sys.recover_failed_providers().await;
    assert_eq!(blocked.requests.load(Ordering::SeqCst), 0);
    assert!(sys.provider_runtime.get_provider_state("blocked").is_err());
}

#[tokio::test]
async fn recovery_stops_on_issuer_validation_failure() {
    let mut idp = RecoveryIdp::start("console", false, true).await;
    idp.config.issuer = Some("https://unexpected.example.com".into());
    let sys = initialize(&[&idp]).await;
    idp.discovery_status.store(200, Ordering::SeqCst);
    tokio::time::timeout(Duration::from_secs(5), sys.recover_failed_providers())
        .await
        .expect("validation failure stops recovery");
    assert_eq!(
        idp.requests.load(Ordering::SeqCst),
        2,
        "issuer rejection must not fetch JWKS or schedule another attempt"
    );
    assert!(sys.provider_runtime.get_provider_state("console").is_err());
    assert!(
        sys.authorize_url("console", "https://console.example.com/callback", None)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn recovery_logs_failure_reasons_without_repeating_warnings() {
    use std::io::{Read, Seek};

    for (explicit, status, expected_kind) in [
        (false, 503, "http_status"),
        (true, 503, "http_status"),
        (true, 0, "request"),
        (true, 1, "timeout"),
    ] {
        let idp = RecoveryIdp::start("logging", false, explicit).await;
        let sys = initialize(&[&idp]).await;
        let requests = idp.requests.load(Ordering::SeqCst);
        idp.discovery_status.store(status, Ordering::SeqCst);
        let mut output = tempfile::tempfile().expect("create captured log file");
        let subscriber = tracing_subscriber::fmt()
            .without_time()
            .with_ansi(false)
            .with_max_level(tracing::Level::WARN)
            .with_writer(output.try_clone().expect("clone captured log file"))
            .finish();
        let _subscriber = tracing::subscriber::set_default(subscriber);
        tracing::callsite::rebuild_interest_cache();
        tokio::time::timeout(Duration::from_secs(25), async {
            tokio::join!(sys.recover_failed_providers(), async {
                // Two failing rounds exercise suppression; a timeout is already bounded by the HTTP client.
                while if status == 1 {
                    idp.cancelled_requests.load(Ordering::SeqCst) == 0
                } else {
                    idp.requests.load(Ordering::SeqCst) < requests + if explicit { 2 } else { 4 }
                } {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                idp.discovery_status.store(200, Ordering::SeqCst);
            });
        })
        .await
        .expect("recover after recording failure diagnostics");
        output.rewind().expect("rewind captured log file");
        let mut logs = String::new();
        output.read_to_string(&mut logs).expect("read captured log file");
        assert_eq!(logs.lines().count(), 1, "explicit={explicit}, status={status}: {logs}");
        assert!(logs.contains("provider_discovery_recovery_pending"), "{logs}");
        assert!(logs.contains(&format!("error_kind=\"{expected_kind}\"")), "{logs}");
        if status == 503 {
            assert!(logs.contains("error_detail=503"), "{logs}");
        }
        assert!(logs.contains("provider_id=logging"), "{logs}");
        assert!(logs.contains("suppressed=0"), "{logs}");
    }
}
