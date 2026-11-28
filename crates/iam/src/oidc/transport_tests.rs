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
use crate::oidc::{provider::discover_provider, test_support::*};
use openidconnect::AsyncHttpClient;
use rustfs_config::MAX_OIDC_RESPONSE_SIZE;
use rustfs_utils::egress::{ENV_OUTBOUND_ALLOW_ORIGINS, OutboundDnsLookup, OutboundPolicy, OutboundProxyMatcher};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

async fn read_http_head(stream: &mut TcpStream) -> String {
    let mut raw = Vec::new();
    let mut buf = [0_u8; 512];
    while !raw.windows(4).any(|w| w == b"\r\n\r\n") {
        let n = stream.read(&mut buf).await.expect("read HTTP request headers");
        if n == 0 {
            break;
        }
        raw.extend_from_slice(&buf[..n]);
    }
    String::from_utf8_lossy(&raw).into_owned()
}

async fn accept_and_respond_json(listener: &TcpListener, body: &'static str) -> String {
    let (mut stream, _) = listener.accept().await.expect("accept mock HTTP connection");
    let head = read_http_head(&mut stream).await;
    let response = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(response.as_bytes()).await.expect("write mock HTTP response");
    head
}

#[test]
fn format_http_headers_redacts_sensitive_values() {
    let mut headers = http::HeaderMap::new();
    headers.insert(
        http::header::AUTHORIZATION,
        "Basic Y2xpZW50OnNlY3JldA==".parse().expect("parse authorization header"),
    );
    headers.insert(
        http::header::PROXY_AUTHORIZATION,
        "Basic cHJveHk6c2VjcmV0".parse().expect("parse proxy-authorization header"),
    );
    headers.insert(http::header::CONTENT_TYPE, "application/json".parse().expect("parse content-type header"));
    headers.insert(http::header::COOKIE, "session=super-secret".parse().expect("parse cookie header"));

    let rendered = format_http_headers(&headers);

    // Sensitive header values never appear; only their length is emitted.
    assert!(!rendered.contains("Y2xpZW50OnNlY3JldA=="), "authorization value leaked: {rendered}");
    assert!(!rendered.contains("cHJveHk6c2VjcmV0"), "proxy-authorization value leaked: {rendered}");
    assert!(!rendered.contains("super-secret"), "cookie value leaked: {rendered}");
    assert!(
        rendered.contains("authorization=<redacted len="),
        "expected redacted authorization: {rendered}"
    );
    assert!(
        rendered.contains("proxy-authorization=<redacted len="),
        "expected redacted proxy-authorization: {rendered}"
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
    let no_proxy = OidcTransportTestOverrides::with_proxy_matcher(OutboundProxyMatcher::none());
    // Cloud metadata endpoint is never allowed.
    assert!(
        matches!(
            build_oidc_http_client("http://169.254.169.254/latest/meta-data/", None, &[], Some(&no_proxy)),
            Err(OidcHttpError::ForbiddenOutbound(_))
        ),
        "metadata endpoint must be rejected"
    );
    // Loopback is rejected by default (no allow-origins configured).
    assert!(
        matches!(
            build_oidc_http_client("http://127.0.0.1:8080/.well-known/openid-configuration", None, &[], Some(&no_proxy)),
            Err(OidcHttpError::ForbiddenOutbound(_))
        ),
        "loopback must be rejected by default"
    );
    // A public hostname passes the up-front shape/host check; the resolved IP is still
    // re-classified at connection time by the pinned resolver.
    assert!(
        build_oidc_http_client(
            "https://accounts.example.com/.well-known/openid-configuration",
            None,
            &[],
            Some(&no_proxy)
        )
        .is_ok(),
        "public https endpoint should build"
    );
}

#[test]
fn build_oidc_http_client_honors_explicit_allowlist_for_loopback() {
    let no_proxy = OidcTransportTestOverrides::with_proxy_matcher(OutboundProxyMatcher::none());
    let policy = OutboundPolicy::from_allowed_origins("http://127.0.0.1:8080").expect("origin should parse");
    assert!(
        build_oidc_http_client(
            "http://127.0.0.1:8080/.well-known/openid-configuration",
            Some(&policy),
            &[],
            Some(&no_proxy)
        )
        .is_ok(),
        "explicitly allow-listed loopback origin should build"
    );
    // A metadata endpoint stays forbidden even when a loopback origin is allow-listed.
    assert!(
        matches!(
            build_oidc_http_client("http://169.254.169.254/", Some(&policy), &[], Some(&no_proxy)),
            Err(OidcHttpError::ForbiddenOutbound(_))
        ),
        "metadata endpoint stays forbidden despite an unrelated allow-list entry"
    );
}

#[tokio::test]
async fn oidc_nonrecoverable_dns_policy_rejection_omits_allowlist_hint() {
    let uri = "http://metadata.internal/latest/meta-data";
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
    assert!(!message.contains(&format!("add http://metadata.internal to {ENV_OUTBOUND_ALLOW_ORIGINS}")));
}

#[test]
fn oidc_metadata_endpoint_rejection_does_not_offer_allowlist_bypass() {
    let no_proxy = OidcTransportTestOverrides::with_proxy_matcher(OutboundProxyMatcher::none());
    let error = build_oidc_http_client(
        "http://169.254.169.254/latest/meta-data/",
        Some(&OutboundPolicy::default()),
        &[],
        Some(&no_proxy),
    )
    .expect_err("metadata endpoint must remain forbidden");
    let message = error.to_string();

    assert!(message.contains("metadata endpoint"));
    assert!(!message.contains(&format!("add http://169.254.169.254 to {ENV_OUTBOUND_ALLOW_ORIGINS}")));
}

#[tokio::test]
async fn oidc_https_discovery_tunnels_through_private_proxy_hostname_without_allowlist() {
    use std::io::Write;
    use std::net::{Shutdown, TcpListener as StdTcpListener};
    use std::sync::mpsc;
    use std::time::{Duration, Instant};

    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let certified = rcgen::generate_simple_self_signed(vec!["sso-sit.intra.kommunale.it".to_string()])
        .expect("generate OIDC TLS certificate for proxied IdP");
    let cert_pem = certified.cert.pem();
    let server_config = Arc::new(
        rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(
                vec![certified.cert.der().clone()],
                rustls_pki_types::PrivateKeyDer::try_from(certified.signing_key.serialize_der())
                    .expect("convert proxied IdP TLS private key"),
            )
            .expect("build proxied IdP TLS server config"),
    );

    let listener = match StdTcpListener::bind("127.0.0.1:0") {
        Ok(listener) => listener,
        Err(err) if err.kind() == std::io::ErrorKind::PermissionDenied => return,
        Err(err) => panic!("mock CONNECT proxy listener should bind: {err}"),
    };
    let proxy_addr = listener.local_addr().expect("mock CONNECT proxy local address");

    let issuer = "https://sso-sit.intra.kommunale.it/realms/rustfs";
    let jwks_uri = "https://sso-sit.intra.kommunale.it/realms/rustfs/protocol/openid-connect/certs";
    let discovery_body = serde_json::json!({
        "issuer": issuer,
        "authorization_endpoint": "https://sso-sit.intra.kommunale.it/realms/rustfs/protocol/openid-connect/auth",
        "token_endpoint": "https://sso-sit.intra.kommunale.it/realms/rustfs/protocol/openid-connect/token",
        "jwks_uri": jwks_uri,
        "response_types_supported": ["code"],
        "response_modes_supported": ["query"],
        "subject_types_supported": ["public"],
        "id_token_signing_alg_values_supported": ["RS256"],
    })
    .to_string();

    let (ready_tx, ready_rx) = mpsc::channel();
    let proxy_handle = std::thread::spawn(move || {
        listener
            .set_nonblocking(true)
            .expect("set mock CONNECT proxy listener non-blocking");
        let _ = ready_tx.send(());

        let mut connect_targets = Vec::new();
        let mut inner_paths = Vec::new();
        let deadline = Instant::now() + Duration::from_secs(5);

        while connect_targets.len() < 2 && Instant::now() < deadline {
            let mut tcp_stream = match listener.accept() {
                Ok((stream, _)) => stream,
                Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                    std::thread::sleep(Duration::from_millis(5));
                    continue;
                }
                Err(_) => break,
            };
            tcp_stream
                .set_nonblocking(false)
                .expect("set mock CONNECT proxy stream blocking");
            tcp_stream
                .set_read_timeout(Some(Duration::from_secs(2)))
                .expect("set mock CONNECT proxy read timeout");

            let connect_req = read_mock_oidc_request(&mut tcp_stream);
            let mut connect_parts = connect_req.lines().next().unwrap_or_default().split_whitespace();
            let method = connect_parts.next().unwrap_or_default();
            let authority = connect_parts.next().unwrap_or_default().to_string();
            assert_eq!(method, "CONNECT", "expected HTTP CONNECT tunnel request: {connect_req}");
            connect_targets.push(authority);

            tcp_stream
                .write_all(b"HTTP/1.1 200 Connection Established\r\n\r\n")
                .expect("write CONNECT 200 response");
            tcp_stream.flush().expect("flush CONNECT 200 response");

            let tls_conn = rustls::ServerConnection::new(Arc::clone(&server_config)).expect("create TLS server connection");
            let mut tls_stream = rustls::StreamOwned::new(tls_conn, tcp_stream);
            let path = read_mock_oidc_request_path(&mut tls_stream);
            let response =
                mock_oidc_response(&path, &discovery_body, "/realms/rustfs/protocol/openid-connect/certs", r#"{"keys":[]}"#);
            inner_paths.push(path);
            tls_stream
                .write_all(response.as_bytes())
                .expect("write tunneled OIDC response");
            tls_stream.conn.send_close_notify();
            let _ = tls_stream.flush();
            let _ = tls_stream.sock.shutdown(Shutdown::Both);
        }

        (connect_targets, inner_paths)
    });
    ready_rx
        .recv_timeout(Duration::from_millis(200))
        .expect("mock CONNECT proxy should become ready");

    let proxy_dns_lookups = Arc::new(AtomicUsize::new(0));
    let target_dns_lookups = Arc::new(AtomicUsize::new(0));
    let proxy_dns_counter = Arc::clone(&proxy_dns_lookups);
    let target_dns_counter = Arc::clone(&target_dns_lookups);
    let dns_lookup: OutboundDnsLookup = Arc::new(move |host| match host {
        "bb-proxy.intra.kommunale.it" => {
            proxy_dns_counter.fetch_add(1, Ordering::SeqCst);
            // Include a forbidden metadata IP alongside the proxy IP to verify proxy DNS
            // filtering drops metadata while keeping allowed operator proxy addresses.
            Some(vec![
                SocketAddr::new(proxy_addr.ip(), 0),
                SocketAddr::new(IpAddr::V4(Ipv4Addr::new(169, 254, 169, 254)), 0),
            ])
        }
        "sso-sit.intra.kommunale.it" => {
            target_dns_counter.fetch_add(1, Ordering::SeqCst);
            Some(vec![SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 20, 30, 40)), 0)])
        }
        _ => None,
    });

    let proxy_url = format!("http://bb-proxy.intra.kommunale.it:{}", proxy_addr.port());
    let proxy_matcher = OutboundProxyMatcher::from_env_values(Some(&proxy_url), Some(&proxy_url), None, None);
    let extra_root_certs =
        parse_oidc_extra_root_certs("test proxied OIDC CA", cert_pem.as_bytes()).expect("parse proxied OIDC CA");
    // OutboundPolicy::default() has an empty RUSTFS_OUTBOUND_ALLOW_ORIGINS allowlist.
    let http_client = ReqwestHttpClient::with_policy_and_extra_root_certs(OutboundPolicy::default(), extra_root_certs)
        .with_proxy_matcher(proxy_matcher)
        .with_dns_lookup(dns_lookup);

    let config = build_mocked_oidc_provider_config(
        "kommunale",
        "https://sso-sit.intra.kommunale.it/realms/rustfs/.well-known/openid-configuration",
    );
    let state = discover_provider(&config, &http_client)
        .await
        .expect("proxied HTTPS OIDC discovery must succeed without RUSTFS_OUTBOUND_ALLOW_ORIGINS");

    assert_eq!(state.metadata.issuer().as_str(), issuer);
    let (connect_targets, inner_paths) = proxy_handle.join().expect("mock CONNECT proxy thread should exit cleanly");
    assert_eq!(
        connect_targets,
        vec![
            "sso-sit.intra.kommunale.it:443".to_string(),
            "sso-sit.intra.kommunale.it:443".to_string()
        ]
    );
    assert_eq!(
        inner_paths,
        vec![
            "/realms/rustfs/.well-known/openid-configuration".to_string(),
            "/realms/rustfs/protocol/openid-connect/certs".to_string()
        ]
    );
    assert!(
        proxy_dns_lookups.load(Ordering::SeqCst) >= 2,
        "proxy hostname must be re-resolved for discovery and JWKS connections"
    );
    assert_eq!(
        target_dns_lookups.load(Ordering::SeqCst),
        0,
        "proxied OIDC discovery must delegate target hostname DNS resolution to the proxy"
    );
}

#[tokio::test]
async fn oidc_http_request_forwards_through_authenticated_private_proxy_without_leaking_credentials() {
    const PROXY_SECRET: &str = "proxy-secret-9611";
    // base64("oidc-proxy:proxy-secret-9611")
    const EXPECTED_PROXY_TOKEN: &str = "b2lkYy1wcm94eTpwcm94eS1zZWNyZXQtOTYxMQ==";

    let listener = match TcpListener::bind("127.0.0.1:0").await {
        Ok(listener) => listener,
        Err(err) if err.kind() == std::io::ErrorKind::PermissionDenied => return,
        Err(err) => panic!("mock HTTP forward proxy should bind: {err}"),
    };
    let proxy_addr = listener.local_addr().expect("mock HTTP proxy local addr");
    let server = tokio::spawn(async move { accept_and_respond_json(&listener, r#"{"keys":[]}"#).await });

    let dns_lookup: OutboundDnsLookup = Arc::new(move |host| match host {
        "bb-proxy.intra.kommunale.it" => Some(vec![SocketAddr::new(proxy_addr.ip(), 0)]),
        _ => None,
    });
    let proxy_url = format!("http://oidc-proxy:{PROXY_SECRET}@bb-proxy.intra.kommunale.it:{}", proxy_addr.port());
    let proxy_matcher = OutboundProxyMatcher::all(&proxy_url);

    // Private/CGNAT proxy literals also pass build_oidc_http_client while target URL checks still run first.
    for allowed_proxy in ["http://10.20.30.40:8080", "http://100.64.1.2:8080"] {
        let overrides = OidcTransportTestOverrides::with_proxy_matcher(OutboundProxyMatcher::all(allowed_proxy));
        assert!(
            build_oidc_http_client(
                "https://sso-sit.intra.kommunale.it/.well-known/openid-configuration",
                Some(&OutboundPolicy::default()),
                &[],
                Some(&overrides),
            )
            .is_ok(),
            "private/CGNAT proxy literal {allowed_proxy} must be accepted"
        );
    }
    let auth_overrides = OidcTransportTestOverrides::with_proxy_matcher(proxy_matcher.clone());
    let forbidden_target_err = build_oidc_http_client(
        "http://10.0.0.5:8080/.well-known/openid-configuration",
        Some(&OutboundPolicy::default()),
        &[],
        Some(&auth_overrides),
    )
    .expect_err("private IP literal OIDC target must still require target allowlist even when a proxy is set")
    .to_string();
    assert!(forbidden_target_err.contains(&format!("add http://10.0.0.5:8080 to {ENV_OUTBOUND_ALLOW_ORIGINS}")));
    assert!(!forbidden_target_err.contains(PROXY_SECRET));

    let http_client = ReqwestHttpClient::with_policy(OutboundPolicy::default())
        .with_proxy_matcher(proxy_matcher)
        .with_dns_lookup(dns_lookup);
    let request = http::Request::builder()
        .method(http::Method::GET)
        .uri("http://sso-sit.intra.kommunale.it/realms/rustfs/certs")
        .body(Vec::new())
        .expect("build proxied HTTP request");
    let response = http_client
        .call(request)
        .await
        .expect("HTTP forward-proxied OIDC request should succeed");
    assert_eq!(response.status(), http::StatusCode::OK);
    assert_eq!(response.body().as_slice(), br#"{"keys":[]}"#);

    let captured_head = server.await.expect("forward-proxy task should finish");
    assert!(
        captured_head.starts_with("GET http://sso-sit.intra.kommunale.it/realms/rustfs/certs HTTP/1.1\r\n"),
        "unexpected forward-proxy request line: {captured_head}"
    );
    assert!(
        captured_head
            .to_ascii_lowercase()
            .contains(&format!("proxy-authorization: basic {}", EXPECTED_PROXY_TOKEN.to_ascii_lowercase())),
        "forward-proxy request missing Proxy-Authorization header: {captured_head}"
    );
}

#[tokio::test]
async fn oidc_rejected_proxy_names_proxy_and_omits_target_allowlist_hint() {
    const PROXY_SECRET: &str = "secret-proxy-9611";
    let target_uri = "https://sso-sit.intra.kommunale.it/realms/rustfs/.well-known/openid-configuration";

    for (bad_proxy, expected_fragment) in [
        (
            "http://169.254.169.254:8080",
            "outbound proxy host '169.254.169.254' is not allowed: metadata endpoint",
        ),
        (
            "http://0xa9fea9fe:8080",
            "outbound proxy host '169.254.169.254' is not allowed: metadata endpoint",
        ),
        (
            "http://169.254.1.1:8080",
            "outbound proxy host '169.254.1.1' is not allowed: link-local address",
        ),
        (
            "socks5://bb-proxy.intra.kommunale.it:1080",
            "outbound proxy URL scheme 'socks5' is not allowed",
        ),
        (
            &format!("http://proxy-user:{PROXY_SECRET}@169.254.169.254:8080"),
            "outbound proxy host '169.254.169.254' is not allowed: metadata endpoint",
        ),
    ] {
        let overrides = OidcTransportTestOverrides::with_proxy_matcher(OutboundProxyMatcher::all(bad_proxy));
        let error = build_oidc_http_client(target_uri, Some(&OutboundPolicy::default()), &[], Some(&overrides))
            .expect_err("forbidden or unsupported proxy must be rejected");
        let message = error.to_string();

        assert!(matches!(error, OidcHttpError::ForbiddenOutbound(_)), "unexpected variant: {error:?}");
        assert!(message.contains(expected_fragment), "missing '{expected_fragment}' in: {message}");
        assert!(
            message.contains("HTTP_PROXY/HTTPS_PROXY/ALL_PROXY/NO_PROXY"),
            "missing proxy remediation hint in: {message}"
        );
        assert!(
            !message.contains("sso-sit.intra.kommunale.it"),
            "proxy error must not blame OIDC target origin: {message}"
        );
        assert!(
            !message.contains(ENV_OUTBOUND_ALLOW_ORIGINS),
            "proxy error must not suggest {ENV_OUTBOUND_ALLOW_ORIGINS}: {message}"
        );
        assert!(!message.contains(PROXY_SECRET), "proxy error leaked credentials: {message}");
    }

    let proxy_dns_calls = Arc::new(AtomicUsize::new(0));
    let calls_for_lookup = Arc::clone(&proxy_dns_calls);
    let metadata_dns: OutboundDnsLookup = Arc::new(move |host| match host {
        "bb-proxy.intra.kommunale.it" => {
            calls_for_lookup.fetch_add(1, Ordering::SeqCst);
            Some(vec![SocketAddr::new(IpAddr::V4(Ipv4Addr::new(169, 254, 169, 254)), 0)])
        }
        _ => None,
    });
    let http_client = ReqwestHttpClient::with_policy(OutboundPolicy::default())
        .with_proxy_matcher(OutboundProxyMatcher::all("http://bb-proxy.intra.kommunale.it:8080"))
        .with_dns_lookup(metadata_dns);

    let request = http::Request::builder()
        .uri(target_uri)
        .body(Vec::new())
        .expect("build OIDC request");
    let call_err = http_client
        .call(request)
        .await
        .expect_err("proxy hostname resolving to metadata must be rejected");
    assert!(matches!(call_err, OidcHttpError::ForbiddenOutbound(_)));
    let call_msg = call_err.to_string();
    assert!(call_msg.contains("outbound proxy 'bb-proxy.intra.kommunale.it'"), "{call_msg}");
    assert!(!call_msg.contains("sso-sit.intra.kommunale.it"), "{call_msg}");
    assert!(!call_msg.contains(ENV_OUTBOUND_ALLOW_ORIGINS), "{call_msg}");
    assert_eq!(proxy_dns_calls.load(Ordering::SeqCst), 1);

    let config = build_mocked_oidc_provider_config("kommunale", target_uri);
    let discovery_err = discover_provider(&config, &http_client)
        .await
        .err()
        .expect("discovery must fail immediately when proxy DNS resolves to metadata");
    assert!(
        discovery_err.starts_with("OIDC provider discovery blocked by outbound policy:"),
        "{discovery_err}"
    );
    assert!(discovery_err.contains("outbound proxy 'bb-proxy.intra.kommunale.it'"), "{discovery_err}");
    assert!(!discovery_err.contains("sso-sit.intra.kommunale.it"), "{discovery_err}");
    assert!(!discovery_err.contains(ENV_OUTBOUND_ALLOW_ORIGINS), "{discovery_err}");
    assert_eq!(
        proxy_dns_calls.load(Ordering::SeqCst),
        2,
        "discovery must not retry a proxy policy rejection across transport retries or issuer variants"
    );
}

#[tokio::test]
async fn oidc_no_proxy_bypasses_proxy_and_enforces_direct_target_dns_policy() {
    let proxy_listener = match TcpListener::bind("127.0.0.1:0").await {
        Ok(listener) => listener,
        Err(err) if err.kind() == std::io::ErrorKind::PermissionDenied => return,
        Err(err) => panic!("mock proxy listener should bind: {err}"),
    };
    let proxy_addr = proxy_listener.local_addr().expect("mock proxy local addr");
    let proxy_hits = Arc::new(AtomicUsize::new(0));
    let proxy_hits_task = Arc::clone(&proxy_hits);
    let proxy_task = tokio::spawn(async move {
        if proxy_listener.accept().await.is_ok() {
            proxy_hits_task.fetch_add(1, Ordering::SeqCst);
        }
    });

    let direct_listener = TcpListener::bind("127.0.0.1:0").await.expect("bind direct OIDC listener");
    let direct_addr = direct_listener.local_addr().expect("direct OIDC local addr");
    let direct_hits = Arc::new(AtomicUsize::new(0));
    let direct_hits_task = Arc::clone(&direct_hits);
    let direct_task = tokio::spawn(async move {
        let _ = accept_and_respond_json(&direct_listener, r#"{"keys":[]}"#).await;
        direct_hits_task.fetch_add(1, Ordering::SeqCst);
    });

    let proxy_url = format!("http://bb-proxy.intra.kommunale.it:{}", proxy_addr.port());
    let matcher = OutboundProxyMatcher::from_env_values(Some(&proxy_url), Some(&proxy_url), None, Some(".intra.kommunale.it"));
    let dns_lookup: OutboundDnsLookup = Arc::new(move |host| match host {
        "sso-sit.intra.kommunale.it" => Some(vec![SocketAddr::new(direct_addr.ip(), 0)]),
        "bb-proxy.intra.kommunale.it" => Some(vec![SocketAddr::new(proxy_addr.ip(), 0)]),
        _ => None,
    });

    let target_origin = format!("http://sso-sit.intra.kommunale.it:{}", direct_addr.port());
    let target_uri = format!("{target_origin}/.well-known/openid-configuration");

    // Without RUSTFS_OUTBOUND_ALLOW_ORIGINS, NO_PROXY bypasses the proxy and rejects the private/loopback target IP.
    let unallowlisted_client = ReqwestHttpClient::with_policy(OutboundPolicy::default())
        .with_proxy_matcher(matcher.clone())
        .with_dns_lookup(Arc::clone(&dns_lookup));
    let blocked_req = http::Request::builder()
        .uri(&target_uri)
        .body(Vec::new())
        .expect("build unallowlisted NO_PROXY request");
    let blocked_err = unallowlisted_client
        .call(blocked_req)
        .await
        .expect_err("NO_PROXY direct target with private/loopback DNS must be blocked when unallowlisted");
    assert!(matches!(blocked_err, OidcHttpError::ForbiddenOutbound(_)));
    let blocked_msg = blocked_err.to_string();
    assert!(
        blocked_msg.contains(&format!("add {target_origin} to {ENV_OUTBOUND_ALLOW_ORIGINS}")),
        "direct target rejection should offer {ENV_OUTBOUND_ALLOW_ORIGINS} hint: {blocked_msg}"
    );
    assert_eq!(proxy_hits.load(Ordering::SeqCst), 0, "NO_PROXY target must not hit proxy");
    assert_eq!(direct_hits.load(Ordering::SeqCst), 0, "unallowlisted target must not connect");

    // Once allowlisted in RUSTFS_OUTBOUND_ALLOW_ORIGINS, the direct NO_PROXY connection succeeds.
    let allowlisted_policy = OutboundPolicy::from_allowed_origins(&target_origin).expect("parse allowlisted OIDC origin");
    let allowlisted_client = ReqwestHttpClient::with_policy(allowlisted_policy)
        .with_proxy_matcher(matcher)
        .with_dns_lookup(dns_lookup);
    let allowed_req = http::Request::builder()
        .uri(&target_uri)
        .body(Vec::new())
        .expect("build allowlisted NO_PROXY request");
    let response = allowlisted_client
        .call(allowed_req)
        .await
        .expect("allowlisted NO_PROXY target should connect directly");
    assert_eq!(response.status(), http::StatusCode::OK);
    assert_eq!(response.body().as_slice(), br#"{"keys":[]}"#);

    direct_task.await.expect("direct target server should finish");
    proxy_task.abort();
    assert_eq!(direct_hits.load(Ordering::SeqCst), 1);
    assert_eq!(proxy_hits.load(Ordering::SeqCst), 0, "NO_PROXY target must bypass proxy");
}

#[tokio::test]
async fn oidc_loopback_targets_bypass_configured_proxy() {
    let proxy_listener = match TcpListener::bind("127.0.0.1:0").await {
        Ok(listener) => listener,
        Err(err) if err.kind() == std::io::ErrorKind::PermissionDenied => return,
        Err(err) => panic!("mock proxy listener should bind: {err}"),
    };
    let proxy_addr = proxy_listener.local_addr().expect("mock proxy local addr");
    let proxy_hits = Arc::new(AtomicUsize::new(0));
    let proxy_hits_task = Arc::clone(&proxy_hits);
    let proxy_task = tokio::spawn(async move {
        if proxy_listener.accept().await.is_ok() {
            proxy_hits_task.fetch_add(1, Ordering::SeqCst);
        }
    });
    let proxy_matcher = OutboundProxyMatcher::all(&format!("http://127.0.0.1:{}", proxy_addr.port()));

    let v4_listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind IPv4 loopback OIDC listener");
    let v4_port = v4_listener.local_addr().expect("IPv4 loopback local addr").port();
    let v4_server = tokio::spawn(async move {
        for _ in 0..2 {
            let _ = accept_and_respond_json(&v4_listener, "{}").await;
        }
    });

    let v4_allowlist = format!("http://127.0.0.1:{v4_port},http://localhost:{v4_port}");
    let v4_policy = OutboundPolicy::from_allowed_origins(&v4_allowlist).expect("allow IPv4 loopback and localhost origins");
    let localhost_dns: OutboundDnsLookup = Arc::new(|host| match host {
        "localhost" => Some(vec![SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)]),
        _ => None,
    });
    let v4_client = ReqwestHttpClient::with_policy(v4_policy)
        .with_proxy_matcher(proxy_matcher.clone())
        .with_dns_lookup(localhost_dns);

    for target_uri in [
        format!("http://127.0.0.1:{v4_port}/.well-known/openid-configuration"),
        format!("http://localhost:{v4_port}/.well-known/openid-configuration"),
    ] {
        let req = http::Request::builder()
            .uri(&target_uri)
            .body(Vec::new())
            .expect("build loopback OIDC request");
        let resp = v4_client
            .call(req)
            .await
            .unwrap_or_else(|err| panic!("loopback OIDC target {target_uri} should bypass proxy: {err}"));
        assert_eq!(resp.status(), http::StatusCode::OK);
    }
    v4_server.await.expect("IPv4 loopback server should finish");

    // Verify [::1] bypasses proxy validation even against a forbidden metadata proxy URL,
    // and also connects directly over IPv6 loopback when available on the host.
    let v6_static_policy = OutboundPolicy::from_allowed_origins("http://[::1]:9000").expect("allow [::1] origin");
    let metadata_proxy_overrides =
        OidcTransportTestOverrides::with_proxy_matcher(OutboundProxyMatcher::all("http://169.254.169.254:8080"));
    assert!(
        build_oidc_http_client(
            "http://[::1]:9000/.well-known/openid-configuration",
            Some(&v6_static_policy),
            &[],
            Some(&metadata_proxy_overrides),
        )
        .is_ok(),
        "http://[::1] target must bypass proxy configuration"
    );

    if let Ok(v6_listener) = TcpListener::bind("[::1]:0").await {
        let v6_port = v6_listener.local_addr().expect("IPv6 loopback local addr").port();
        let v6_server = tokio::spawn(async move {
            let _ = accept_and_respond_json(&v6_listener, "{}").await;
        });
        let v6_policy = OutboundPolicy::from_allowed_origins(&format!("http://[::1]:{v6_port}")).expect("allow [::1] test port");
        let v6_client = ReqwestHttpClient::with_policy(v6_policy).with_proxy_matcher(proxy_matcher);
        let req = http::Request::builder()
            .uri(format!("http://[::1]:{v6_port}/.well-known/openid-configuration"))
            .body(Vec::new())
            .expect("build IPv6 loopback OIDC request");
        let resp = v6_client
            .call(req)
            .await
            .expect("http://[::1] OIDC request should connect directly and bypass proxy");
        assert_eq!(resp.status(), http::StatusCode::OK);
        v6_server.await.expect("IPv6 loopback server should finish");
    }

    proxy_task.abort();
    assert_eq!(proxy_hits.load(Ordering::SeqCst), 0, "loopback targets must never contact proxy");
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
