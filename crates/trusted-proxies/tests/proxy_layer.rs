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
