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

use std::sync::Arc;
use axum::body::Body;
use axum::{routing::get, Router};
use serde_json::{json};
use tower::ServiceExt;
use rustfs_trusted_proxies::config::{AppConfig, TrustedProxy, TrustedProxyConfig, ValidationMode};
use rustfs_trusted_proxies::state::AppState;

fn create_test_app_state() -> AppState {
    let proxies = vec![TrustedProxy::Single("127.0.0.1".parse().unwrap())];
    let proxy_config = TrustedProxyConfig::new(proxies, ValidationMode::HopByHop, true, 10, true, vec![]);
    let config = AppConfig::new(
        proxy_config,
        rustfs_trusted_proxies::config::CacheConfig::default(),
        rustfs_trusted_proxies::config::MonitoringConfig::default(),
        rustfs_trusted_proxies::config::CloudConfig::default(),
        "127.0.0.1:3000".parse().unwrap(),
    );
    AppState {
        config: Arc::new(config),
        metrics: None,
    }
}

#[tokio::test]
async fn test_health_check_endpoint() {
    let state = create_test_app_state();
    let app = Router::new()
        .route("/health", get(|| async { axum::response::Json(json!({"status": "healthy"})) }))
        .with_state(state);

    let request = axum::http::Request::builder().uri("/health").body(Body::empty()).unwrap();
    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), 200);
}
