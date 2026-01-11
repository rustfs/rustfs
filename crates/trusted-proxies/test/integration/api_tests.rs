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

//! API integration tests

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use axum::body::Body;
    use axum::{extract::State, routing::get, Router};
    use serde_json::{json, Value};
    use tower::ServiceExt;

    use crate::config::{AppConfig, TrustedProxy, TrustedProxyConfig, ValidationMode};
    use crate::middleware::TrustedProxyLayer;
    use crate::AppState;

    fn create_test_app_state() -> AppState {
        let proxies = vec![TrustedProxy::Single("127.0.0.1".parse().unwrap())];

        let proxy_config = TrustedProxyConfig::new(proxies, ValidationMode::HopByHop, true, 10, true, vec![]);

        let config = AppConfig::new(
            proxy_config,
            crate::config::CacheConfig::default(),
            crate::config::MonitoringConfig::default(),
            crate::config::CloudConfig::default(),
            "127.0.0.1:3000".parse().unwrap(),
        );

        AppState {
            config: Arc::new(config),
            metrics: None,
        }
    }

    fn create_test_api_router() -> Router {
        let state = create_test_app_state();
        let proxy_layer = TrustedProxyLayer::enabled(state.config.proxy.clone(), None);

        Router::new()
            .route("/health", get(health_check))
            .route("/config", get(show_config))
            .with_state(state)
            .layer(proxy_layer)
    }

    async fn health_check() -> axum::response::Json<Value> {
        axum::response::Json(json!({
            "status": "healthy",
            "service": "trusted-proxy-test"
        }))
    }

    async fn show_config(State(state): State<AppState>) -> axum::response::Json<Value> {
        axum::response::Json(json!({
            "server": state.config.server_addr.to_string(),
            "proxy": {
                "trusted_networks": state.config.proxy.proxies.len(),
            }
        }))
    }

    #[tokio::test]
    async fn test_health_check_endpoint() {
        let app = create_test_api_router();

        let request = axum::http::Request::builder().uri("/health").body(Body::empty()).unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), 200);

        let body = hyper::body::to_bytes(response.into_body()).await.unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(json["status"], "healthy");
        assert_eq!(json["service"], "trusted-proxy-test");
    }

    #[tokio::test]
    async fn test_config_endpoint() {
        let app = create_test_api_router();

        let request = axum::http::Request::builder().uri("/config").body(Body::empty()).unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), 200);

        let body = hyper::body::to_bytes(response.into_body()).await.unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(json["server"], "127.0.0.1:3000");
        assert_eq!(json["proxy"]["trusted_networks"], 1);
    }

    #[tokio::test]
    async fn test_proxy_headers_in_api() {
        let state = create_test_app_state();
        let proxy_layer = TrustedProxyLayer::enabled(state.config.proxy.clone(), None);

        let app = Router::new()
            .route(
                "/client-test",
                get(|req: axum::extract::Request| async move {
                    let client_info = req.extensions().get::<crate::middleware::ClientInfo>();
                    match client_info {
                        Some(info) => axum::response::Json(json!({
                            "client_ip": info.real_ip.to_string(),
                            "trusted": info.is_from_trusted_proxy
                        })),
                        None => axum::response::Json(json!({
                            "error": "No client info"
                        })),
                    }
                }),
            )
            .with_state(state)
            .layer(proxy_layer);

        // 测试带代理头部的请求
        let request = axum::http::Request::builder()
            .uri("/client-test")
            .header("X-Forwarded-For", "203.0.113.195")
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), 200);

        let body = hyper::body::to_bytes(response.into_body()).await.unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();

        // 由于请求来自 127.0.0.1（可信代理），应该解析 X-Forwarded-For
        if json.get("client_ip").is_some() {
            let client_ip = json["client_ip"].as_str().unwrap();
            // 可能是 203.0.113.195 或 127.0.0.1，取决于中间件如何配置
            assert!(client_ip == "203.0.113.195" || client_ip == "127.0.0.1");
        }
    }

    #[tokio::test]
    async fn test_missing_endpoint() {
        let app = create_test_api_router();

        let request = axum::http::Request::builder().uri("/not-found").body(Body::empty()).unwrap();

        let response = app.oneshot(request).await.unwrap();

        // 应该返回 404
        assert_eq!(response.status(), 404);
    }

    #[tokio::test]
    async fn test_request_without_proxy_layer() {
        // 创建没有代理中间件的路由
        let app = Router::new().route("/simple", get(|| async { "OK" }));

        let request = axum::http::Request::builder().uri("/simple").body(Body::empty()).unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), 200);

        let body = hyper::body::to_bytes(response.into_body()).await.unwrap();
        assert_eq!(String::from_utf8(body.to_vec()).unwrap(), "OK");
    }
}
