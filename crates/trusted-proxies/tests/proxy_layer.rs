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

use axum::{
    body::{self, Body},
    http::{Request, Response},
};
use rustfs_trusted_proxies::{ClientInfo, TrustedProxyLayer};
use std::convert::Infallible;
use std::net::SocketAddr;
use tower::{Layer, ServiceExt, service_fn};

#[tokio::test]
async fn simple_proxy_uses_only_the_rightmost_observed_peer() {
    for (chain, expected) in [
        ("203.0.113.7, 198.51.100.9", "198.51.100.9"),
        ("203.0.113.7, 10.20.0.9", "10.20.0.9"),
        ("203.0.113.7, ::ffff:198.51.100.9", "198.51.100.9"),
        ("198.51.100.9, 10.20.0.8", "10.20.0.8"),
    ] {
        let service = service_fn(|request: Request<Body>| async move {
            let info = request.extensions().get::<ClientInfo>().expect("client identity");
            assert_eq!(info.forwarded_proto.as_deref(), Some("http"));
            Ok::<_, Infallible>(Response::new(Body::from(info.real_ip.to_string())))
        });
        let service = TrustedProxyLayer::enabled().layer(service);
        let mut request = Request::builder().body(Body::empty()).expect("request");
        request
            .extensions_mut()
            .insert("[::ffff:10.0.0.5]:9000".parse::<SocketAddr>().expect("peer"));
        request
            .headers_mut()
            .append("x-forwarded-for", "203.0.113.1".parse().expect("forged line"));
        request
            .headers_mut()
            .append("x-forwarded-for", chain.parse().expect("proxy line"));
        request
            .headers_mut()
            .append("x-forwarded-proto", "https".parse().expect("forged proto"));
        request
            .headers_mut()
            .append("x-forwarded-proto", "http".parse().expect("proxy proto"));
        request
            .headers_mut()
            .insert("forwarded", "for=127.0.0.1;proto=https".parse().expect("forged family"));
        let response = service.oneshot(request).await.expect("proxy response");
        assert_eq!(response.status(), 200);
        let body = body::to_bytes(response.into_body(), 1024).await.expect("response body");
        assert_eq!(body.as_ref(), expected.as_bytes());
    }
}

#[tokio::test]
async fn invalid_proxy_chains_never_reach_the_inner_service() {
    use rustfs_trusted_proxies::{LegacyTrustedProxyLayer, TrustedProxy, TrustedProxyConfig, ValidationMode};
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    let peer: SocketAddr = "10.0.0.5:9000".parse().expect("peer");
    for mode in [
        None,
        Some(ValidationMode::HopByHop),
        Some(ValidationMode::Lenient),
        Some(ValidationMode::Strict),
    ] {
        for invalid in ["203.0.113.7, not-an-ip", "203.0.113.7, 0.0.0.0", "203.0.113.7,", ""] {
            let calls = Arc::new(AtomicUsize::new(0));
            let called = Arc::clone(&calls);
            let service = service_fn(move |_request: Request<Body>| {
                called.fetch_add(1, Ordering::SeqCst);
                async { Ok::<_, Infallible>(Response::new(Body::empty())) }
            });
            let layer = match mode {
                None => TrustedProxyLayer::enabled(),
                Some(mode) => TrustedProxyLayer::legacy(LegacyTrustedProxyLayer::enabled(
                    TrustedProxyConfig::new(vec![TrustedProxy::Single(peer.ip())], mode, false, 3, false, vec![]),
                    None,
                )),
            };
            let mut request = Request::builder()
                .header("x-forwarded-for", invalid)
                .header("forwarded", "for=127.0.0.1;proto=https")
                .header("x-real-ip", "127.0.0.1")
                .body(Body::empty())
                .expect("request");
            request.extensions_mut().insert(peer);
            let response = layer.layer(service).oneshot(request).await.expect("proxy rejection");
            assert_eq!(response.status(), 400, "mode={mode:?}, header={invalid:?}");
            assert_eq!(calls.load(Ordering::SeqCst), 0);
        }
    }
}

#[tokio::test]
async fn legacy_chain_rejection_never_falls_back_to_proxy_identity() {
    use rustfs_trusted_proxies::{LegacyTrustedProxyLayer, TrustedProxy, TrustedProxyConfig, ValidationMode};
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    let peer: SocketAddr = "10.0.0.5:9000".parse().expect("peer");
    for mode in [ValidationMode::HopByHop, ValidationMode::Lenient, ValidationMode::Strict] {
        for chain in [None, Some("203.0.113.1, 10.0.0.1, 10.0.0.2, 198.51.100.9")] {
            let calls = Arc::new(AtomicUsize::new(0));
            let called = Arc::clone(&calls);
            let service = service_fn(move |_request: Request<Body>| {
                called.fetch_add(1, Ordering::SeqCst);
                async { Ok::<_, Infallible>(Response::new(Body::empty())) }
            });
            let layer = LegacyTrustedProxyLayer::enabled(
                TrustedProxyConfig::new(vec![TrustedProxy::Single(peer.ip())], mode, false, 3, false, vec![]),
                None,
            );
            let mut request = Request::builder()
                .header("forwarded", "for=127.0.0.1;proto=https")
                .body(Body::empty())
                .expect("request");
            request.extensions_mut().insert(peer);
            if let Some(chain) = chain {
                request
                    .headers_mut()
                    .insert("x-forwarded-for", chain.parse().expect("long chain"));
            }
            let response = layer.layer(service).oneshot(request).await.expect("proxy rejection");
            assert_eq!(response.status(), 400);
            assert_eq!(calls.load(Ordering::SeqCst), 0);
        }
    }
}

#[tokio::test]
async fn legacy_proxy_selects_one_family_and_preserves_repeated_lines() {
    use rustfs_trusted_proxies::{LegacyTrustedProxyLayer, TrustedProxy, TrustedProxyConfig, ValidationMode};
    let peer: SocketAddr = "10.0.0.5:9000".parse().expect("peer");
    for rfc7239 in [false, true] {
        for mode in [ValidationMode::HopByHop, ValidationMode::Lenient] {
            let service = service_fn(|request: Request<Body>| async move {
                let info = request.extensions().get::<ClientInfo>().expect("client identity");
                assert_eq!(info.real_ip.to_string(), "198.51.100.9");
                assert_eq!(info.forwarded_proto.as_deref(), Some("http"));
                Ok::<_, Infallible>(Response::new(Body::empty()))
            });
            let layer = LegacyTrustedProxyLayer::enabled(
                TrustedProxyConfig::new(vec![TrustedProxy::Single(peer.ip())], mode, rfc7239, 5, false, vec![]),
                None,
            );
            let mut request = Request::builder()
                .header("x-forwarded-for", "127.0.0.1")
                .header("forwarded", "for=127.0.0.1;proto=https")
                .body(Body::empty())
                .expect("request");
            request.extensions_mut().insert(peer);
            if rfc7239 {
                request
                    .headers_mut()
                    .append("forwarded", "for=198.51.100.9;proto=http".parse().expect("proxy line"));
            } else {
                request
                    .headers_mut()
                    .append("x-forwarded-for", "198.51.100.9".parse().expect("proxy line"));
                request
                    .headers_mut()
                    .insert("x-forwarded-proto", "http".parse().expect("proxy proto"));
            }
            let response = layer.layer(service).oneshot(request).await.expect("valid proxy request");
            assert_eq!(response.status(), 200);
        }
    }
}

#[tokio::test]
async fn test_layer_inserts_client_info_for_internal_proxy() {
    let peer_addr = SocketAddr::from(([10, 0, 0, 5], 9000));
    let service = service_fn(|request: Request<Body>| async move {
        let client_info = request.extensions().get::<ClientInfo>().cloned().unwrap();
        Ok::<_, Infallible>(Response::new(Body::from(client_info.real_ip.to_string())))
    });
    let service = TrustedProxyLayer::enabled().layer(service);

    let mut request = Request::builder()
        .uri("/")
        .header("x-forwarded-for", "203.0.113.10")
        .body(Body::empty())
        .unwrap();
    request.extensions_mut().insert(peer_addr);

    let response = service.oneshot(request).await.unwrap();

    let body = body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    assert_eq!(std::str::from_utf8(&body).unwrap(), "203.0.113.10");
}

#[tokio::test]
async fn test_layer_ignores_forwarded_header_for_public_peer() {
    let peer_addr = SocketAddr::from(([8, 8, 8, 8], 9000));
    let service = service_fn(|request: Request<Body>| async move {
        let client_info = request.extensions().get::<ClientInfo>().cloned().unwrap();
        Ok::<_, Infallible>(Response::new(Body::from(client_info.real_ip.to_string())))
    });
    let service = TrustedProxyLayer::enabled().layer(service);

    let mut request = Request::builder()
        .uri("/")
        .header("x-forwarded-for", "203.0.113.10")
        .body(Body::empty())
        .unwrap();
    request.extensions_mut().insert(peer_addr);

    let response = service.oneshot(request).await.unwrap();

    let body = body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    assert_eq!(std::str::from_utf8(&body).unwrap(), "8.8.8.8");
}
