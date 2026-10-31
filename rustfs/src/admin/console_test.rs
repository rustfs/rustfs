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

#[cfg(test)]
mod tests {
    use crate::config::Opt;
    use serial_test::serial;

    /// Sends a CORS preflight request through a router wrapped with the given
    /// layer and returns the `access-control-allow-origin` header, if any.
    async fn preflight_allow_origin(cors: tower_http::cors::CorsLayer, origin: &str) -> Option<String> {
        use axum::{Router, body::Body, routing::get};
        use tower::ServiceExt;

        let app = Router::new().route("/", get(|| async { "ok" })).layer(cors);
        let response = app
            .oneshot(
                http::Request::builder()
                    .method(http::Method::OPTIONS)
                    .uri("/")
                    .header(http::header::ORIGIN, origin)
                    .header(http::header::ACCESS_CONTROL_REQUEST_METHOD, "GET")
                    .body(Body::empty())
                    .expect("preflight request must build"),
            )
            .await
            .expect("preflight request must not fail");

        response
            .headers()
            .get(http::header::ACCESS_CONTROL_ALLOW_ORIGIN)
            .map(|value| value.to_str().expect("allow-origin header must be valid UTF-8").to_string())
    }

    /// Runs a request through a router that returns a brotli-encoded response without
    /// `Content-Length` and returns the response's `content-length` header value, if any.
    async fn brotli_response_content_length(has_content_length: bool) -> Option<String> {
        use crate::admin::console::buffer_brotli_responses;
        use axum::middleware;
        use axum::{Router, body::Body, routing::get};
        use tower::ServiceExt;

        let app = Router::new()
            .route(
                "/",
                get(move || async move {
                    let mut builder = http::Response::builder().header(http::header::CONTENT_ENCODING, "br");
                    if has_content_length {
                        builder = builder.header(http::header::CONTENT_LENGTH, "4");
                    }
                    builder.body(Body::from("data")).expect("response must build")
                }),
            )
            .layer(middleware::from_fn(buffer_brotli_responses));

        let response = app
            .oneshot(
                http::Request::builder()
                    .uri("/")
                    .body(Body::empty())
                    .expect("request must build"),
            )
            .await
            .expect("request must not fail");

        response
            .headers()
            .get(http::header::CONTENT_LENGTH)
            .map(|value| value.to_str().expect("content-length header must be valid UTF-8").to_string())
    }

    #[tokio::test]
    #[serial]
    async fn test_buffer_brotli_responses() {
        // Brotli response without Content-Length must have Content-Length added.
        assert!(
            brotli_response_content_length(false).await.is_some(),
            "brotli response without Content-Length must have Content-Length added"
        );

        // Brotli response that already carries Content-Length must pass through unchanged.
        assert_eq!(
            brotli_response_content_length(true).await.as_deref(),
            Some("4"),
            "brotli response that already has Content-Length must not be re-buffered"
        );
    }

    #[tokio::test]
    #[serial]
    async fn test_console_cors_configuration() {
        use crate::admin::console::parse_cors_origins;

        // Wildcard configuration must allow any origin.
        let wildcard = parse_cors_origins(Some(&"*".to_string()));
        assert_eq!(
            preflight_allow_origin(wildcard, "http://anywhere.example").await.as_deref(),
            Some("*"),
            "wildcard configuration must answer preflight with a permissive allow-origin"
        );

        // An explicit list must echo listed origins and refuse unlisted ones.
        let listed = parse_cors_origins(Some(&"http://localhost:3000,https://admin.example.com".to_string()));
        assert_eq!(
            preflight_allow_origin(listed, "http://localhost:3000").await.as_deref(),
            Some("http://localhost:3000"),
            "a listed origin must be echoed back on preflight"
        );
        let listed = parse_cors_origins(Some(&"http://localhost:3000,https://admin.example.com".to_string()));
        assert_eq!(
            preflight_allow_origin(listed, "https://other.example").await,
            None,
            "an unlisted origin must not receive an allow-origin header"
        );

        // Empty and unset configurations fall back to same-origin only:
        // no cross-origin caller may be allowed.
        let empty = parse_cors_origins(Some(&"".to_string()));
        assert_eq!(
            preflight_allow_origin(empty, "http://localhost:3000").await,
            None,
            "empty configuration must not allow any cross-origin caller"
        );
        let unset = parse_cors_origins(None);
        assert_eq!(
            preflight_allow_origin(unset, "http://localhost:3000").await,
            None,
            "unset configuration must not allow any cross-origin caller"
        );
    }

    #[tokio::test]
    #[serial]
    async fn test_console_tls_configuration() {
        // Test TLS configuration options (now uses shared tls_path)
        let args = vec!["rustfs", "/tmp/test", "--tls-path", "/path/to/tls"];
        let opt = Opt::parse_from(args);

        assert_eq!(opt.tls_path, Some("/path/to/tls".to_string()));
    }

    #[tokio::test]
    #[serial]
    async fn test_console_health_check_endpoint() {
        // Test that console health check can be called
        // This test would need a running server to be comprehensive
        // For now, we test configuration and startup behavior
        let args = vec!["rustfs", "/tmp/test", "--console-address", ":0"];
        let opt = Opt::parse_from(args);

        // Verify the configuration supports health checks
        assert!(opt.console_enable, "Console should be enabled for health checks");
    }

    #[tokio::test]
    #[serial]
    async fn test_console_separate_logging_target() {
        // Test that console uses separate logging targets
        use tracing::info;

        // This test verifies that logging targets are properly set up
        info!(target: "rustfs::console::startup", "Test console startup log");
        info!(target: "rustfs::console::access", "Test console access log");
        info!(target: "rustfs::console::error", "Test console error log");
        info!(target: "rustfs::console::shutdown", "Test console shutdown log");

        // In a real implementation, we would verify these logs are captured separately
    }

    #[tokio::test]
    #[serial]
    async fn test_console_configuration_validation() {
        // Test configuration validation
        let args = vec![
            "rustfs",
            "/tmp/test",
            "--console-enable",
            "true",
            "--console-address",
            ":9001",
        ];
        let opt = Opt::parse_from(args);

        // Verify all console-related configuration is parsed correctly
        assert!(opt.console_enable);
        assert_eq!(opt.console_address, ":9001");
    }
}
