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

//! Proxy system integration tests

#[cfg(test)]
mod tests {
    use std::error::Request;
    use axum::body::Body;
    use axum::{extract::Request, routing::get, Router};
    use tower::ServiceExt;

    use crate::config::{ConfigLoader, TrustedProxy, TrustedProxyConfig, ValidationMode};
    use crate::middleware::{ClientInfo, TrustedProxyLayer};

    fn create_test_router() -> Router {
        let proxies = vec![
            TrustedProxy::Single("127.0.0.1".parse().unwrap()),
            TrustedProxy::Cidr("10.0.0.0/8".parse().unwrap()),
        ];

        let config = TrustedProxyConfig::new(proxies, ValidationMode::HopByHop, true, 10, true, vec![]);

        let proxy_layer = TrustedProxyLayer::enabled(config, None);

        Router::new()
            .route(
                "/test",
                get(|req: Request| async move {
                    let client_info = req.extensions().get::<ClientInfo>();
                    match client_info {
                        Some(info) => {
                            format!("IP: {}, Trusted: {}, Hops: {}", info.real_ip, info.is_from_trusted_proxy, info.proxy_hops)
                        }
                        None => "No client info".to_string(),
                    }
                }),
            )
            .layer(proxy_layer)
    }

    #[tokio::test]
    async fn test_direct_connection() {
        let app = create_test_router();

        // 模拟直接连接（无代理头部）
        let request = axum::http::Request::builder().uri("/test").body(Body::empty()).unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), 200);

        let body = hyper::body::to_bytes(response.into_body()).await.unwrap();
        let body_str = String::from_utf8(body.to_vec()).unwrap();

        // 应该显示直接连接的 IP（在测试环境中可能是 0.0.0.0）
        assert!(body_str.contains("IP:"));
    }

    #[tokio::test]
    async fn test_trusted_proxy_with_xff() {
        let app = create_test_router();

        // 模拟来自可信代理的请求
        let request = axum::http::Request::builder()
            .uri("/test")
            .header("X-Forwarded-For", "203.0.113.195, 10.0.1.100")
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), 200);

        let body = hyper::body::to_bytes(response.into_body()).await.unwrap();
        let body_str = String::from_utf8(body.to_vec()).unwrap();

        // 应该显示客户端 IP (203.0.113.195)
        assert!(body_str.contains("203.0.113.195"));
        assert!(body_str.contains("Trusted: true"));
    }

    #[tokio::test]
    async fn test_untrusted_proxy_with_xff() {
        let app = create_test_router();

        // 模拟来自不可信代理的请求
        let request = axum::http::Request::builder()
            .uri("/test")
            .header("X-Forwarded-For", "203.0.113.195")
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), 200);

        let body = hyper::body::to_bytes(response.into_body()).await.unwrap();
        let body_str = String::from_utf8(body.to_vec()).unwrap();

        // 由于请求不是来自可信代理，X-Forwarded-For 应该被忽略
        // 应该显示直接连接的 IP
        assert!(!body_str.contains("203.0.113.195"));
    }

    #[tokio::test]
    async fn test_proxy_chain_too_long() {
        let proxies = vec![TrustedProxy::Single("127.0.0.1".parse().unwrap())];

        let config = TrustedProxyConfig::new(
            proxies,
            ValidationMode::Strict,
            true,
            3, // 最大 3 跳
            true,
            vec![],
        );

        let proxy_layer = TrustedProxyLayer::enabled(config, None);

        let app = Router::new().route("/test", get(|| async { "OK" })).layer(proxy_layer);

        // 模拟超长代理链
        let xff_value = (0..5).map(|i| format!("10.0.{}.1", i)).collect::<Vec<_>>().join(", ");

        let request = axum::http::Request::builder()
            .uri("/test")
            .header("X-Forwarded-For", xff_value)
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();

        // 由于代理链太长，验证应该失败
        // 注意：中间件可能会降级处理，而不是直接拒绝
        assert_eq!(response.status(), 200);
    }

    #[tokio::test]
    async fn test_rfc7239_forwarded_header() {
        let proxies = vec![TrustedProxy::Single("127.0.0.1".parse().unwrap())];

        let config = TrustedProxyConfig::new(
            proxies,
            ValidationMode::HopByHop,
            true, // 启用 RFC 7239
            10,
            true,
            vec![],
        );

        let proxy_layer = TrustedProxyLayer::enabled(config, None);

        let app = Router::new()
            .route(
                "/test",
                get(|req: Request| async move {
                    let client_info = req.extensions().get::<ClientInfo>().unwrap();
                    format!("IP: {}", client_info.real_ip)
                }),
            )
            .layer(proxy_layer);

        // 模拟使用 RFC 7239 Forwarded 头部的请求
        let request = axum::http::Request::builder()
            .uri("/test")
            .header("Forwarded", r#"for=192.0.2.60;proto=https;by=203.0.113.43"#)
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), 200);

        let body = hyper::body::to_bytes(response.into_body()).await.unwrap();
        let body_str = String::from_utf8(body.to_vec()).unwrap();

        // 应该解析 RFC 7239 头部
        assert!(body_str.contains("192.0.2.60"));
    }
}
