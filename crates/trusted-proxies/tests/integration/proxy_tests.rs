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

use axum::body::Body;
use axum::{Router, routing::get};
use rustfs_trusted_proxies::{TrustedProxy, TrustedProxyConfig, TrustedProxyLayer, ValidationMode};
use tower::ServiceExt;

#[tokio::test]
async fn test_proxy_validation_flow() {
    let proxies = vec![TrustedProxy::Single("127.0.0.1".parse().unwrap())];
    let config = TrustedProxyConfig::new(proxies, ValidationMode::HopByHop, true, 10, true, vec![]);
    let proxy_layer = TrustedProxyLayer::enabled(config, None);

    let app = Router::new().route("/test", get(|| async { "OK" })).layer(proxy_layer);

    let request = axum::http::Request::builder()
        .uri("/test")
        .header("X-Forwarded-For", "203.0.113.195")
        .body(Body::empty())
        .unwrap();

    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), 200);
}
