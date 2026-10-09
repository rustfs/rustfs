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
use super::{
    config::{OidcProviderConfig, OidcProviderConfigSource, SourcedOidcProviderConfig},
    provider::ProviderRuntime,
    runtime::OidcSys,
    state::OidcStateStore,
    transport::ReqwestHttpClient,
};
use std::collections::HashMap;

#[cfg(any(test, feature = "test-util"))]
#[doc(hidden)]
pub fn make_test_sys(configs: Vec<OidcProviderConfig>) -> OidcSys {
    let configs = configs
        .into_iter()
        .map(test_sourced_config)
        .map(|provider| (provider.config.id.clone(), provider))
        .collect();
    OidcSys {
        configs,
        provider_runtime: ProviderRuntime::new(
            ReqwestHttpClient::new().expect("failed to initialize OIDC HTTP clients"),
            HashMap::new(),
        ),
        state_store: OidcStateStore::new(),
    }
}

#[cfg(any(test, feature = "test-util"))]
pub(super) fn test_sourced_config(config: OidcProviderConfig) -> SourcedOidcProviderConfig {
    SourcedOidcProviderConfig {
        config,
        source: OidcProviderConfigSource::Persisted,
    }
}

#[cfg(any(test, feature = "test-util"))]
#[doc(hidden)]
pub fn test_config(id: &str) -> OidcProviderConfig {
    OidcProviderConfig {
        id: id.to_string(),
        enabled: true,
        config_url: format!("https://example.com/{id}/.well-known/openid-configuration"),
        issuer: None,
        client_id: "client-id".to_string(),
        client_secret: None,
        scopes: vec!["openid".to_string()],
        other_audiences: vec![],
        redirect_uri: None,
        redirect_uri_dynamic: true,
        claim_name: "groups".to_string(),
        claim_prefix: String::new(),
        role_policy: String::new(),
        display_name: id.to_string(),
        groups_claim: "groups".to_string(),
        roles_claim: String::new(),
        email_claim: "email".to_string(),
        username_claim: "preferred_username".to_string(),
        hide_from_ui: false,
    }
}

#[cfg(test)]
use super::{config::OidcProviderValidationResult, provider::discover_provider, transport::OidcHttpError};
#[cfg(test)]
use jsonwebtoken::{Algorithm, EncodingKey};
#[cfg(test)]
use openidconnect::AsyncHttpClient;
#[cfg(test)]
use rustfs_utils::egress::{OutboundDnsPolicyRejection, OutboundPolicy};
#[cfg(test)]
use std::sync::Arc;
#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};
#[cfg(test)]
use url::Url;

#[cfg(test)]
#[derive(Clone)]
pub(super) struct RejectingDnsResolver {
    pub(super) allow_origin_can_recover: bool,
    pub(super) calls: Option<Arc<AtomicUsize>>,
}

#[cfg(test)]
impl reqwest::dns::Resolve for RejectingDnsResolver {
    fn resolve(&self, name: reqwest::dns::Name) -> reqwest::dns::Resolving {
        if let Some(calls) = &self.calls {
            calls.fetch_add(1, Ordering::Relaxed);
        }
        let host = name.as_str().to_string();
        let rejection = OutboundDnsPolicyRejection::new(host, self.allow_origin_can_recover);
        Box::pin(async move { Err(std::io::Error::new(std::io::ErrorKind::PermissionDenied, rejection).into()) })
    }
}

#[cfg(test)]
pub(super) fn build_mocked_oidc_provider_config(id: &str, config_url: &str) -> OidcProviderConfig {
    OidcProviderConfig {
        id: id.to_string(),
        enabled: true,
        config_url: config_url.to_string(),
        issuer: None,
        client_id: "rustfs-oidc-test".to_string(),
        client_secret: None,
        scopes: vec!["openid".to_string()],
        other_audiences: vec![],
        redirect_uri: None,
        redirect_uri_dynamic: false,
        claim_name: "sub".to_string(),
        claim_prefix: "oidc".to_string(),
        role_policy: String::new(),
        display_name: "mock-oidc".to_string(),
        groups_claim: "groups".to_string(),
        roles_claim: String::new(),
        email_claim: "email".to_string(),
        username_claim: "username".to_string(),
        hide_from_ui: false,
    }
}
#[cfg(test)]
pub(super) fn read_mock_oidc_request(stream: &mut impl std::io::Read) -> String {
    let mut request_bytes = Vec::new();
    let mut buffer = [0u8; 4096];
    loop {
        match stream.read(&mut buffer) {
            Ok(0) => break,
            Ok(n) => request_bytes.extend_from_slice(&buffer[..n]),
            Err(e) if matches!(e.kind(), std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut) => break,
            Err(_) => break,
        }
        if request_bytes.windows(4).any(|w| w == b"\r\n\r\n") {
            break;
        }
        if request_bytes.len() >= 8192 {
            break;
        }
    }
    String::from_utf8_lossy(&request_bytes).into_owned()
}
#[cfg(test)]
pub(super) fn read_mock_oidc_request_path(stream: &mut impl std::io::Read) -> String {
    read_mock_oidc_request(stream)
        .lines()
        .next()
        .unwrap_or("")
        .split_whitespace()
        .nth(1)
        .unwrap_or("")
        .to_string()
}
#[cfg(test)]
pub(super) fn mock_oidc_response(path: &str, discovery_body: &str, expected_jwks_path: &str, jwks_body: &str) -> String {
    let (status, body) = if path.ends_with("/.well-known/openid-configuration") {
        (200, discovery_body)
    } else if path == expected_jwks_path {
        (200, jwks_body)
    } else {
        (404, r#"{"error":"not found"}"#)
    };

    format!(
        "HTTP/1.1 {status} {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        if status == 200 { "OK" } else { "Not Found" },
        body.len()
    )
}
#[cfg(test)]
pub(super) fn start_mock_oidc_discovery_server_with_jwks<F, J>(
    build_discovery_issuer: F,
    max_requests: usize,
    signing_alg: &'static str,
    workload: bool,
    jwks_response: J,
) -> Option<(String, std::thread::JoinHandle<()>)>
where
    F: Fn(&str) -> (String, String, String) + Send + 'static,
    J: Fn(usize) -> String + Send + 'static,
{
    use std::io::Write;
    use std::net::{Shutdown, TcpListener};
    use std::sync::mpsc;
    use std::time::{Duration, Instant};

    // After the last completed response, exit if no new connection arrives within this window.
    // Keep the mock server alive long enough for slower CI/macOS test environments to finish
    // discovery + JWKS requests without racing the shutdown timer.
    const IDLE_SHUTDOWN: Duration = Duration::from_secs(1);
    const ABSOLUTE_CAP: Duration = Duration::from_secs(5);

    let listener = match TcpListener::bind("127.0.0.1:0") {
        Ok(listener) => listener,
        Err(err) if err.kind() == std::io::ErrorKind::PermissionDenied => return None,
        Err(err) => panic!("test listener should bind: {err}"),
    };
    let base = format!("http://{}", listener.local_addr().expect("listener local address should be available"));
    let (discovery_issuer, discovery_jwks_uri, expected_jwks_path) = build_discovery_issuer(&base);
    let mut discovery_document = serde_json::json!({
        "issuer": discovery_issuer,
        "authorization_endpoint": format!("{base}/authorize"),
        "token_endpoint": format!("{base}/token"),
        "jwks_uri": discovery_jwks_uri,
        "response_types_supported": ["code"],
        "response_modes_supported": ["query"],
        "subject_types_supported": ["public"],
        "id_token_signing_alg_values_supported": [signing_alg],
    });
    if workload {
        let fields = discovery_document.as_object_mut().expect("mock metadata is an object");
        fields.remove("authorization_endpoint");
        fields.remove("token_endpoint");
        fields.insert("response_types_supported".into(), serde_json::json!(["id_token"]));
    }
    let discovery_body = discovery_document.to_string();
    let (ready_tx, ready_rx) = mpsc::channel();

    let handle = std::thread::spawn(move || {
        listener
            .set_nonblocking(true)
            .expect("failed to set discovery mock listener non-blocking");
        let _ = ready_tx.send(());

        let mut seen = 0usize;
        let mut jwks_fetches = 0usize;
        let start = Instant::now();
        let mut last_completed = Instant::now();

        loop {
            if seen > 0 && last_completed.elapsed() >= IDLE_SHUTDOWN {
                break;
            }
            if start.elapsed() >= ABSOLUTE_CAP {
                break;
            }

            let mut stream = match listener.accept() {
                Ok((stream, _)) => stream,
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    std::thread::sleep(Duration::from_millis(5));
                    continue;
                }
                Err(_) => break,
            };
            stream
                .set_nonblocking(false)
                .expect("failed to set discovery mock stream blocking");

            seen += 1;
            stream
                .set_nonblocking(false)
                .expect("failed to set discovery mock stream blocking");
            stream
                .set_read_timeout(Some(Duration::from_secs(1)))
                .expect("failed to set discovery mock read timeout");

            let request = read_mock_oidc_request(&mut stream);
            let path = request
                .lines()
                .next()
                .and_then(|line| line.split_whitespace().nth(1))
                .unwrap_or("");
            let jwks_body = jwks_response(jwks_fetches);
            if path == expected_jwks_path {
                jwks_fetches += 1;
            }
            let mut response = mock_oidc_response(path, &discovery_body, &expected_jwks_path, &jwks_body);
            if path.contains("/.well-known/openid-configuration") {
                assert!(
                    request
                        .lines()
                        .filter_map(|line| line.split_once(':'))
                        .any(|(name, value)| { name.eq_ignore_ascii_case("accept") && value.trim() == "application/json" }),
                    "discovery Accept must remain unchanged"
                );
            }
            if path == expected_jwks_path {
                let expected_type = if workload {
                    "application/jwk-set+json"
                } else {
                    "application/json"
                };
                let accepts_type = request.lines().filter_map(|line| line.split_once(':')).any(|(name, value)| {
                    name.eq_ignore_ascii_case("accept") && value.split(',').any(|item| item.trim() == expected_type)
                });
                if !accepts_type {
                    response = "HTTP/1.1 406 Not Acceptable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".into();
                } else if workload {
                    response = response.replace("Content-Type: application/json", "Content-Type: application/jwk-set+json");
                }
            }
            let _ = stream.write_all(response.as_bytes());
            let _ = stream.flush();
            let _ = stream.shutdown(Shutdown::Both);
            last_completed = Instant::now();

            if seen >= max_requests {
                break;
            }
        }
    });
    ready_rx
        .recv_timeout(Duration::from_millis(100))
        .expect("mock OIDC discovery server should become ready");

    Some((base, handle))
}
#[cfg(test)]
pub(super) fn start_mock_oidc_discovery_server<F>(
    build_discovery_issuer: F,
    max_requests: usize,
) -> Option<(String, std::thread::JoinHandle<()>)>
where
    F: Fn(&str) -> (String, String, String) + Send + 'static,
{
    start_mock_oidc_discovery_server_with_jwks(build_discovery_issuer, max_requests, "RS256", false, |_| {
        r#"{"keys":[]}"#.to_string()
    })
}
#[cfg(test)]
pub(super) fn oidc_es256_key_and_jwk(kid: &str) -> (EncodingKey, serde_json::Value) {
    let certified = rcgen::generate_simple_self_signed(vec![format!("{kid}.invalid")]).expect("OIDC signing key should generate");
    let encoding_key =
        EncodingKey::from_ec_pem(certified.signing_key.serialize_pem().as_bytes()).expect("OIDC signing key should encode");
    let mut jwk = jsonwebtoken::jwk::Jwk::from_encoding_key(&encoding_key, Algorithm::ES256)
        .expect("OIDC public JWK should derive from signing key");
    jwk.common.key_id = Some(kid.to_string());
    jwk.common.public_key_use = Some(jsonwebtoken::jwk::PublicKeyUse::Signature);
    (encoding_key, serde_json::to_value(jwk).expect("OIDC JWK should serialize"))
}
#[cfg(test)]
pub(super) fn start_mock_oidc_tls_discovery_server<F>(
    build_discovery_issuer: F,
    max_requests: usize,
) -> Option<(String, String, std::thread::JoinHandle<()>)>
where
    F: Fn(&str) -> (String, String, String) + Send + 'static,
{
    use std::io::Write;
    use std::net::{Shutdown, TcpListener};
    use std::sync::mpsc;
    use std::time::{Duration, Instant};

    const IDLE_SHUTDOWN: Duration = Duration::from_secs(1);
    const ABSOLUTE_CAP: Duration = Duration::from_secs(5);

    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let certified =
        rcgen::generate_simple_self_signed(vec!["127.0.0.1".to_string()]).expect("generate OIDC TLS test certificate");
    let cert_pem = certified.cert.pem();
    let server_config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(
            vec![certified.cert.der().clone()],
            rustls_pki_types::PrivateKeyDer::try_from(certified.signing_key.serialize_der())
                .expect("convert OIDC TLS test private key"),
        )
        .expect("build OIDC TLS mock server config");

    let listener = match TcpListener::bind("127.0.0.1:0") {
        Ok(listener) => listener,
        Err(err) if err.kind() == std::io::ErrorKind::PermissionDenied => return None,
        Err(err) => panic!("test TLS listener should bind: {err}"),
    };
    let base = format!("https://{}", listener.local_addr().expect("listener local address should be available"));
    let (discovery_issuer, discovery_jwks_uri, expected_jwks_path) = build_discovery_issuer(&base);
    let discovery_body = serde_json::json!({
        "issuer": discovery_issuer,
        "authorization_endpoint": format!("{base}/authorize"),
        "token_endpoint": format!("{base}/token"),
        "jwks_uri": discovery_jwks_uri,
        "response_types_supported": ["code"],
        "response_modes_supported": ["query"],
        "subject_types_supported": ["public"],
        "id_token_signing_alg_values_supported": ["RS256"],
    })
    .to_string();
    let jwks_body = r#"{"keys":[]}"#;
    let (ready_tx, ready_rx) = mpsc::channel();

    let handle = std::thread::spawn(move || {
        let server_config = Arc::new(server_config);
        listener
            .set_nonblocking(true)
            .expect("failed to set TLS discovery mock listener non-blocking");
        let _ = ready_tx.send(());

        let mut seen = 0usize;
        let start = Instant::now();
        let mut last_completed = Instant::now();

        loop {
            if seen > 0 && last_completed.elapsed() >= IDLE_SHUTDOWN {
                break;
            }
            if start.elapsed() >= ABSOLUTE_CAP {
                break;
            }

            let tcp_stream = match listener.accept() {
                Ok((stream, _)) => stream,
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    std::thread::sleep(Duration::from_millis(5));
                    continue;
                }
                Err(_) => break,
            };
            tcp_stream
                .set_nonblocking(false)
                .expect("failed to set TLS discovery mock stream blocking");
            tcp_stream
                .set_read_timeout(Some(Duration::from_secs(1)))
                .expect("failed to set TLS discovery mock read timeout");

            seen += 1;
            let connection = match rustls::ServerConnection::new(server_config.clone()) {
                Ok(connection) => connection,
                Err(_) => break,
            };
            let mut stream = rustls::StreamOwned::new(connection, tcp_stream);
            let path = read_mock_oidc_request_path(&mut stream);
            let response = mock_oidc_response(&path, &discovery_body, &expected_jwks_path, jwks_body);
            let _ = stream.write_all(response.as_bytes());
            let _ = stream.flush();
            let _ = stream.sock.shutdown(Shutdown::Both);
            last_completed = Instant::now();

            if seen >= max_requests {
                break;
            }
        }
    });
    ready_rx
        .recv_timeout(Duration::from_millis(100))
        .expect("mock TLS OIDC discovery server should become ready");

    Some((base, cert_pem, handle))
}
#[cfg(test)]
pub(super) fn discovery_error_contains_all_variants(err: &str, base: &str) -> bool {
    err.contains(base) && err.contains(&format!("{base}/")) && err.contains("discovery failed for all issuer variants")
}
#[cfg(test)]
pub(super) async fn validate_mocked_oidc_provider_config(
    config: &OidcProviderConfig,
) -> Result<OidcProviderValidationResult, String> {
    // The mock discovery/JWKS/token endpoints share the loopback origin of `config_url`.
    // Explicitly allow that origin so the egress policy does not reject the loopback mock.
    let origin = Url::parse(&config.config_url)
        .map_err(|_| "invalid mock config_url".to_string())?
        .origin()
        .ascii_serialization();
    let policy = OutboundPolicy::from_allowed_origins(&origin).map_err(|err| err.to_string())?;
    let http_client = ReqwestHttpClient::with_policy(policy);
    let state = discover_provider(config, &http_client).await?;

    Ok(OidcProviderValidationResult {
        issuer: state.metadata.issuer().to_string(),
        authorization_endpoint: state.metadata.authorization_endpoint(),
        token_endpoint: state.metadata.token_endpoint().map(ToString::to_string),
    })
}
#[cfg(test)]
pub(super) fn start_unbounded_body_server(body_len: usize) -> Option<(String, std::thread::JoinHandle<()>)> {
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::mpsc;
    use std::time::Duration;

    let listener = match TcpListener::bind("127.0.0.1:0") {
        Ok(listener) => listener,
        Err(err) if err.kind() == std::io::ErrorKind::PermissionDenied => return None,
        Err(err) => panic!("test listener should bind: {err}"),
    };
    let base = format!("http://{}", listener.local_addr().expect("listener local address should be available"));
    let (ready_tx, ready_rx) = mpsc::channel();

    let handle = std::thread::spawn(move || {
        let _ = ready_tx.send(());
        let Ok((mut stream, _)) = listener.accept() else {
            return;
        };
        let _ = stream.set_read_timeout(Some(Duration::from_secs(1)));
        let mut buffer = [0u8; 4096];
        let _ = stream.read(&mut buffer);
        let _ = stream.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n");

        let chunk = vec![b'a'; 64 * 1024];
        let mut written = 0usize;
        while written < body_len {
            let take = chunk.len().min(body_len - written);
            if stream.write_all(&chunk[..take]).is_err() {
                break;
            }
            written += take;
        }
        let _ = stream.flush();
    });
    ready_rx
        .recv_timeout(Duration::from_millis(100))
        .expect("mock body server should become ready");

    Some((base, handle))
}
#[cfg(test)]
pub(super) async fn fetch_oidc_mock_body(base: &str) -> Result<Vec<u8>, OidcHttpError> {
    let policy = OutboundPolicy::from_allowed_origins(base).expect("origin should parse");
    let client = ReqwestHttpClient::with_policy(policy);
    let request = http::Request::builder()
        .method(http::Method::GET)
        .uri(base)
        .body(Vec::new())
        .expect("request should build");
    client.call(request).await.map(http::Response::into_body)
}
