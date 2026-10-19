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

/// Predefined CPU profiling path for RustFS server.
/// This path is used to access CPU profiling data.
pub(crate) const PROFILE_CPU_PATH: &str = "/profile/cpu";

/// This path is used to access memory profiling data.
pub(crate) const PROFILE_MEMORY_PATH: &str = "/profile/memory";

/// Favicon path to handle browser requests for the favicon.
/// This path serves the favicon.ico file.
pub(crate) const FAVICON_PATH: &str = "/favicon.ico";
pub(crate) const APPLE_TOUCH_ICON_PATH: &str = "/apple-touch-icon.png";
pub(crate) const APPLE_TOUCH_ICON_PRECOMPOSED_PATH: &str = "/apple-touch-icon-precomposed.png";

/// Predefined health check path for RustFS server.
/// This path is used to check the health status of the server.
pub(crate) const HEALTH_PREFIX: &str = "/health";

/// Predefined readiness check path for RustFS server.
/// This path is used to check dependency readiness and may return 503.
pub(crate) const HEALTH_READY_PATH: &str = "/health/ready";

/// Health liveness probe compatibility alias path.
pub(crate) const HEALTH_COMPAT_LIVE_PATH: &str = "/health/live";

/// MinIO-compatible health liveness probe alias path.
pub(crate) const MINIO_HEALTH_LIVE_PATH: &str = "/minio/health/live";

/// MinIO-compatible health readiness probe alias path.
pub(crate) const MINIO_HEALTH_READY_PATH: &str = "/minio/health/ready";

/// MinIO-compatible cluster health probe alias path.
pub(crate) const MINIO_HEALTH_CLUSTER_PATH: &str = "/minio/health/cluster";

/// MinIO-compatible cluster read health probe alias path.
pub(crate) const MINIO_HEALTH_CLUSTER_READ_PATH: &str = "/minio/health/cluster/read";

/// Predefined administrative prefix for RustFS server routes.
/// This prefix is used for endpoints that handle administrative tasks
/// such as configuration, monitoring, and management.
pub(crate) const ADMIN_PREFIX: &str = "/rustfs/admin";

/// MinIO-compatible administrative prefix accepted by RustFS.
/// This alias allows stock MinIO admin tooling to reach RustFS handlers.
pub(crate) const MINIO_ADMIN_PREFIX: &str = "/minio/admin";

/// Iceberg REST Catalog prefix for RustFS S3 Tables control-plane routes.
pub(crate) const TABLE_CATALOG_PREFIX: &str = "/iceberg/v1";

/// MinIO AIStor-compatible Iceberg REST Catalog prefix alias.
pub(crate) const TABLE_CATALOG_COMPAT_PREFIX: &str = "/_iceberg/v1";

/// Returns true for the admin prefix itself or slash-delimited children.
pub(crate) fn is_admin_path(path: &str) -> bool {
    has_path_prefix(path, ADMIN_PREFIX) || has_path_prefix(path, MINIO_ADMIN_PREFIX) || is_table_catalog_path(path)
}

pub(crate) fn is_table_catalog_path(path: &str) -> bool {
    has_path_prefix(path, TABLE_CATALOG_PREFIX) || has_path_prefix(path, TABLE_CATALOG_COMPAT_PREFIX)
}

pub(crate) fn has_path_prefix(path: &str, prefix: &str) -> bool {
    path == prefix || path.strip_prefix(prefix).is_some_and(|suffix| suffix.starts_with('/'))
}

/// Environment variable name for overriding the default
/// administrative prefix path.
pub(crate) const RUSTFS_ADMIN_PREFIX: &str = "/rustfs/admin/v3";

/// MinIO-compatible admin API prefix accepted by RustFS.
pub(crate) const MINIO_ADMIN_V3_PREFIX: &str = "/minio/admin/v3";

/// Console asset base path embedded at build time and used as the startup default.
/// It must match NEXT_PUBLIC_BASE_PATH when building the bundled frontend.
pub(crate) const CONSOLE_PREFIX: &str = match option_env!("RUSTFS_CONSOLE_BASE_PATH") {
    Some(path) if !path.is_empty() => path,
    _ => rustfs_config::DEFAULT_CONSOLE_PREFIX,
};

static CONFIGURED_CONSOLE_PREFIX: std::sync::OnceLock<String> = std::sync::OnceLock::new();
static CONFIGURED_API_PREFIX: std::sync::OnceLock<Option<String>> = std::sync::OnceLock::new();

/// The prefix is fixed before listeners start; request handling never reads the environment.
pub(crate) fn console_prefix() -> &'static str {
    CONFIGURED_CONSOLE_PREFIX.get().map(String::as_str).unwrap_or(CONSOLE_PREFIX)
}

/// `None` when `RUSTFS_API_PREFIX` is unset. Fixed before listeners start.
pub(crate) fn api_prefix() -> Option<&'static str> {
    CONFIGURED_API_PREFIX.get().and_then(|prefix| prefix.as_deref())
}

/// How the API prefix applies to a request path.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum ApiPathMatch<'a> {
    /// The path below the API prefix (empty for the bare prefix). Console paths are matched
    /// first so a console nested under the API prefix keeps its full path, and internode RPC
    /// paths are never stripped so they cannot reach the RPC handlers through the prefixed
    /// external lane.
    Strip(&'a str),
    /// Not API-prefixed; leave the request untouched.
    PassThrough,
    /// The decoded path is under the API prefix but the raw path is not. s3s routes and signs
    /// the decoded path, so a percent-encoded prefix would reuse the signature of the
    /// prefixed path for another object.
    Reject,
}

pub(crate) fn strip_api_prefix(path: &str) -> ApiPathMatch<'_> {
    let Some(api) = api_prefix() else {
        return ApiPathMatch::PassThrough;
    };
    classify_api_path(path, api, console_prefix())
}

fn classify_api_path<'a>(path: &'a str, api_prefix: &str, console_prefix: &str) -> ApiPathMatch<'a> {
    if encoded_prefix_variant(path, api_prefix, console_prefix) {
        return ApiPathMatch::Reject;
    }
    match strip_prefix_except(path, api_prefix, console_prefix) {
        Some(remainder) => ApiPathMatch::Strip(remainder),
        None => ApiPathMatch::PassThrough,
    }
}

fn strip_prefix_except<'a>(path: &'a str, api_prefix: &str, console_prefix: &str) -> Option<&'a str> {
    if has_path_prefix(path, console_prefix) || !has_path_prefix(path, api_prefix) {
        return None;
    }
    let remainder = path.strip_prefix(api_prefix)?;
    if is_internode_path(remainder) {
        return None;
    }
    Some(remainder)
}

fn is_internode_path(path: &str) -> bool {
    [RPC_PREFIX, TONIC_PREFIX, "/rustfs/peer"]
        .iter()
        .any(|internode| has_path_prefix(path, internode))
}

fn encoded_prefix_variant(path: &str, api_prefix: &str, console_prefix: &str) -> bool {
    if !path.contains('%') {
        return false;
    }
    let decoded = percent_encoding::percent_decode_str(path).decode_utf8_lossy();
    if !has_path_prefix(&decoded, api_prefix) {
        return false;
    }
    if !has_path_prefix(path, api_prefix) {
        return true;
    }
    if has_path_prefix(&decoded, console_prefix) && !has_path_prefix(path, console_prefix) {
        return true;
    }
    // The plain form of an internode path passes through unstripped, so its encoded form must
    // not be stripped (signed path and route would disagree).
    let raw_remainder = path.strip_prefix(api_prefix).unwrap_or_default();
    let decoded_remainder = decoded.strip_prefix(api_prefix).unwrap_or_default();
    is_internode_path(decoded_remainder) && !is_internode_path(raw_remainder)
}

pub(crate) fn init_console_prefix() -> std::io::Result<()> {
    let api_prefix = match std::env::var(rustfs_config::ENV_RUSTFS_API_PREFIX) {
        Ok(value) if value.is_empty() => None,
        Ok(value) => Some(validate_path_prefix(&value, rustfs_config::ENV_RUSTFS_API_PREFIX)?),
        Err(std::env::VarError::NotPresent) => None,
        Err(err) => return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, err)),
    };
    let raw = match std::env::var(rustfs_config::ENV_RUSTFS_CONSOLE_PREFIX) {
        Ok(value) => value,
        Err(std::env::VarError::NotPresent) => format!("{}{CONSOLE_PREFIX}", api_prefix.as_deref().unwrap_or("")),
        Err(err) => return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, err)),
    };
    let prefix = validate_path_prefix(&raw, rustfs_config::ENV_RUSTFS_CONSOLE_PREFIX)?;
    if api_prefix.as_deref().is_some_and(|api| has_path_prefix(api, &prefix)) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "RUSTFS_API_PREFIX must not equal or be nested under RUSTFS_CONSOLE_PREFIX",
        ));
    }
    if CONFIGURED_API_PREFIX.get_or_init(|| api_prefix.clone()) != &api_prefix {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "RUSTFS_API_PREFIX cannot change after server initialization",
        ));
    }
    if CONFIGURED_CONSOLE_PREFIX.get_or_init(|| prefix.clone()) != &prefix {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "RUSTFS_CONSOLE_PREFIX cannot change after server initialization",
        ));
    }
    Ok(())
}

fn validate_path_prefix(raw: &str, env_name: &str) -> std::io::Result<String> {
    let prefix = raw.strip_suffix('/').unwrap_or(raw);
    // Keep the value safe in HTTP headers, Axum routes, and embedded HTML/JS.
    if !prefix.starts_with('/')
        || prefix.len() > 256
        || prefix[1..].split('/').any(|segment| {
            segment.is_empty()
                || matches!(segment, "." | "..")
                || !segment
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~'))
        })
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("{env_name} must be a non-root absolute path of at most 256 bytes with nonempty URL-safe segments"),
        ));
    }
    let reserved = [
        ADMIN_PREFIX,
        MINIO_ADMIN_PREFIX,
        TABLE_CATALOG_PREFIX,
        TABLE_CATALOG_COMPAT_PREFIX,
        RPC_PREFIX,
        TONIC_PREFIX,
        "/rustfs/peer",
        HEALTH_PREFIX,
        "/minio/health",
        "/profile",
        "/index.html",
        FAVICON_PATH,
        APPLE_TOUCH_ICON_PATH,
        APPLE_TOUCH_ICON_PRECOMPOSED_PATH,
    ];
    if reserved
        .iter()
        .any(|path| has_path_prefix(prefix, path) || has_path_prefix(path, prefix))
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("{env_name} overlaps a reserved server route"),
        ));
    }
    Ok(prefix.to_string())
}

/// Predefined RPC prefix for RustFS server routes.
/// This prefix is used for endpoints that handle remote procedure calls (RPC).
pub(crate) const RPC_PREFIX: &str = "/rustfs/rpc";

/// Predefined gRPC service prefix for RustFS server.
/// This prefix is used for gRPC service endpoints.
/// For example, the full gRPC method path would be "/node_service.NodeService/MethodName".
pub(crate) const TONIC_PREFIX: &str = "/node_service.NodeService";

/// version information path for RustFS server. This path is used to access version information about the RustFS server.
pub(crate) const VERSION: &str = "/version";

/// license information path for RustFS server. This path is used to access license information about the RustFS server.
pub(crate) const LICENSE: &str = "/license";

/// LOGO art for RustFS server.
pub const LOGO: &str = r#"

░█▀▄░█░█░█▀▀░▀█▀░█▀▀░█▀▀
░█▀▄░█░█░▀▀█░░█░░█▀▀░▀▀█
░▀░▀░▀▀▀░▀▀▀░░▀░░▀░░░▀▀▀

"#;

#[cfg(test)]
mod console_prefix_tests {
    use super::*;

    #[test]
    fn console_prefix_validation() {
        for (raw, expected) in [
            (CONSOLE_PREFIX, CONSOLE_PREFIX),
            ("/console", "/console"),
            ("/management/console/", "/management/console"),
            ("/health-dashboard", "/health-dashboard"),
        ] {
            assert_eq!(
                validate_path_prefix(raw, rustfs_config::ENV_RUSTFS_CONSOLE_PREFIX).expect("valid console prefix"),
                expected
            );
        }
        for raw in [
            "",
            "/",
            "console",
            "//console",
            "/console//",
            "/a//b",
            "/a/../b",
            "/a/./b",
            "/%2e%2e",
            "/console?x=1",
            "/console#x",
            "/console\\x",
            "/a\n",
            "/{param}",
            "/<script>",
            "/控制台",
            "/rustfs",
            "/rustfs/admin",
            "/rustfs/admin/v3/ui",
            "/minio",
            "/minio/admin",
            "/health",
            "/health/ui",
            "/iceberg",
            "/_iceberg/v1",
            "/rustfs/rpc",
            "/rustfs/peer",
            "/node_service.NodeService",
            "/profile",
            "/index.html",
            "/favicon.ico",
            "/index.html",
        ] {
            assert_eq!(
                validate_path_prefix(raw, rustfs_config::ENV_RUSTFS_CONSOLE_PREFIX)
                    .expect_err(raw)
                    .kind(),
                std::io::ErrorKind::InvalidInput
            );
        }
        assert!(validate_path_prefix(&format!("/{}", "a".repeat(255)), rustfs_config::ENV_RUSTFS_CONSOLE_PREFIX).is_ok());
        assert!(validate_path_prefix(&format!("/{}", "a".repeat(256)), rustfs_config::ENV_RUSTFS_CONSOLE_PREFIX).is_err());
    }

    #[test]
    fn configured_console_prefix_subprocesses() {
        // Startup configuration is process-wide; isolate each value from other unit tests.
        for prefix in [None, Some("/console"), Some("/management/console/")] {
            let mut command = std::process::Command::new(std::env::current_exe().expect("test executable"));
            command
                .args(["console_prefix_process_case", "--test-threads=1"])
                .env("RUSTFS_TEST_CONSOLE_PREFIX_PROCESS", "1")
                .env("RUSTFS_CONSOLE_BASE_PATH", "/runtime-ignored/console")
                .env("RUSTFS_BROWSER_REDIRECT_URL", "https://console.example.com")
                .env("RUSTFS_HEALTH_ENDPOINT_ENABLE", "true")
                .env("RUSTFS_CONSOLE_RATE_LIMIT_ENABLE", "false");
            if let Some(prefix) = prefix {
                command.env(rustfs_config::ENV_RUSTFS_CONSOLE_PREFIX, prefix);
            } else {
                command.env_remove(rustfs_config::ENV_RUSTFS_CONSOLE_PREFIX);
            }
            let output = command.output().expect("run isolated console tests");
            assert!(
                output.status.success(),
                "prefix {prefix:?}: {}{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
        }
    }

    #[tokio::test]
    async fn console_prefix_process_case_routes() {
        if std::env::var_os("RUSTFS_TEST_CONSOLE_PREFIX_PROCESS").is_none() {
            return;
        }
        use axum::body::Body;
        use http::{Request, StatusCode};
        use tower::ServiceExt;
        init_console_prefix().expect("initialize configured console prefix");
        let expected = std::env::var(rustfs_config::ENV_RUSTFS_CONSOLE_PREFIX).unwrap_or_else(|_| CONSOLE_PREFIX.to_string());
        let prefix = expected.trim_end_matches('/');
        assert_eq!(console_prefix(), prefix);
        assert!(crate::admin::console::is_console_path(&format!("{prefix}/index.html")));
        assert!(!crate::admin::console::is_console_path(&format!("{prefix}-other/index.html")));
        for path in ["/rustfs/admin/v3/info", "/minio/admin/v3/info", "/health", "/bucket/object"] {
            assert!(!crate::admin::console::is_console_path(path), "reserved or S3 path {path}");
        }
        assert_eq!(
            crate::server::compress::PathCategory::classify(&format!("{prefix}/asset.js")),
            crate::server::compress::PathCategory::Console
        );
        if prefix != CONSOLE_PREFIX {
            assert!(!crate::admin::console::is_console_path(CONSOLE_PREFIX));
        }
        crate::admin::console::init_console_cfg(std::net::Ipv4Addr::LOCALHOST.into(), 9001);
        let router = crate::admin::console::make_console_server();
        for (suffix, expected_status, expected_ready) in [
            ("/health", StatusCode::OK, None),
            ("/health/live", StatusCode::OK, None),
            ("/health/ready", StatusCode::SERVICE_UNAVAILABLE, Some(false)),
        ] {
            let response = router
                .clone()
                .oneshot(
                    Request::builder()
                        .uri(format!("{prefix}{suffix}"))
                        .body(Body::empty())
                        .expect("health request"),
                )
                .await
                .expect("health response");
            assert_eq!(response.status(), expected_status, "{suffix}");
            let body = axum::body::to_bytes(response.into_body(), 65536).await.expect("health body");
            let payload: serde_json::Value = serde_json::from_slice(&body).expect("health JSON");
            assert_eq!(payload.get("ready").and_then(serde_json::Value::as_bool), expected_ready, "{suffix}");
        }
        for suffix in ["/version", "/license"] {
            let response = router
                .clone()
                .oneshot(
                    Request::builder()
                        .uri(format!("{prefix}{suffix}"))
                        .body(Body::empty())
                        .expect("console request"),
                )
                .await
                .expect("console response");
            assert_eq!(response.status(), StatusCode::OK, "{suffix}");
            assert_eq!(response.headers()[http::header::CONTENT_TYPE], "application/json");
        }
    }
}

#[cfg(test)]
mod api_prefix_tests {
    use super::*;

    #[test]
    fn api_prefix_validation() {
        let env = rustfs_config::ENV_RUSTFS_API_PREFIX;
        for (raw, expected) in [
            ("/tools/rustfs", "/tools/rustfs"),
            ("/tools/rustfs/api/", "/tools/rustfs/api"),
            ("/p", "/p"),
        ] {
            assert_eq!(validate_path_prefix(raw, env).expect("valid API prefix"), expected);
        }
        for raw in [
            "",
            "/",
            "tools",
            "//tools",
            "/tools//rustfs",
            "/tools/..",
            "/tools/./x",
            "/tools x",
            "/tools?x=1",
            "/%2e%2e",
            "/控制台",
            "/rustfs",
            "/rustfs/admin",
            "/health",
            "/minio",
            "/profile",
            "/rustfs/rpc",
        ] {
            let error = validate_path_prefix(raw, env).expect_err(raw);
            assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput, "{raw}");
            assert!(error.to_string().contains("RUSTFS_API_PREFIX"), "{raw}: {error}");
        }
        assert!(validate_path_prefix(&format!("/{}", "a".repeat(255)), env).is_ok());
        assert!(validate_path_prefix(&format!("/{}", "a".repeat(256)), env).is_err());
    }

    #[test]
    fn strip_prefix_except_matches_console_paths_first() {
        let strip = |path| strip_prefix_except(path, "/tools/rustfs", "/tools/rustfs/rustfs/console");
        assert_eq!(strip("/tools/rustfs"), Some(""));
        assert_eq!(strip("/tools/rustfs/"), Some("/"));
        assert_eq!(strip("/tools/rustfs/bucket/key"), Some("/bucket/key"));
        assert_eq!(strip("/tools/rustfs/bucket/a%2Fb"), Some("/bucket/a%2Fb"));
        assert_eq!(strip("/tools/rustfs/rustfs/admin/v3/info"), Some("/rustfs/admin/v3/info"));
        assert_eq!(strip("/tools/rustfs/rustfs/console"), None);
        assert_eq!(strip("/tools/rustfs/rustfs/console/index.html"), None);
        assert_eq!(strip("/tools/rustfs/rustfs/console-other/x"), Some("/rustfs/console-other/x"));
        assert_eq!(strip("/tools/rustfsx/bucket"), None);
        assert_eq!(strip("/bucket/key"), None);
        assert_eq!(strip("/health"), None);
    }

    #[test]
    fn strip_prefix_except_never_strips_internode_paths() {
        let strip = |path| strip_prefix_except(path, "/tools/rustfs", "/tools/rustfs/rustfs/console");
        for path in [
            "/tools/rustfs/rustfs/rpc",
            "/tools/rustfs/rustfs/rpc/read_file",
            "/tools/rustfs/node_service.NodeService",
            "/tools/rustfs/node_service.NodeService/Ping",
            "/tools/rustfs/rustfs/peer",
            "/tools/rustfs/rustfs/peer/health",
        ] {
            assert_eq!(strip(path), None, "{path}");
        }
        assert_eq!(strip("/tools/rustfs/rustfs/rpcx/k"), Some("/rustfs/rpcx/k"));
        assert_eq!(strip("/tools/rustfs/rustfs/peers/k"), Some("/rustfs/peers/k"));
        assert_eq!(strip("/tools/rustfs/node_service.NodeServicex"), Some("/node_service.NodeServicex"));
    }

    #[test]
    fn classify_api_path_rejects_percent_encoded_prefix_variants() {
        let classify = |path| classify_api_path(path, "/tools/rustfs", "/tools/rustfs/rustfs/console");
        for path in [
            "/tools/rustf%73/b/k",
            "/tools%2Frustfs/b/k",
            "/%74ools/rustfs/b/k",
            "/tools/rustfs%2Fb/k",
            "/%74ools/rustfs",
            "/tools/rustfs/rustfs/%63onsole/x",
            "/%74ools/rustfs/rustfs/console/x",
            "/tools/rustfs/%72ustfs/rpc/x",
            "/tools/rustfs/rustfs%2Frpc/x",
            "/tools/rustfs/node_s%65rvice.NodeService/Ping",
            "/tools/rustfs/rustfs/%70eer/health",
        ] {
            assert_eq!(classify(path), ApiPathMatch::Reject, "{path}");
        }
        // Raw-prefixed requests with an encoded remainder stay strippable; the signed path is
        // prefix plus remainder, which matches what s3s routes after decoding.
        assert_eq!(classify("/tools/rustfs/b/a%2Fb"), ApiPathMatch::Strip("/b/a%2Fb"));
        assert_eq!(classify("/tools/rustfs/b/%74ools"), ApiPathMatch::Strip("/b/%74ools"));
        // Not under the prefix once decoded: untouched.
        for path in [
            "/b/k",
            "/b/a%2Fb",
            "/tools/other%2Fx",
            "/tools%2Frustfsx/b",
            "/tools/rustfs/rustfs/console/x",
        ] {
            assert_eq!(classify(path), ApiPathMatch::PassThrough, "{path}");
        }
        assert_eq!(classify("/tools/rustfs/rustfs/rpc/x"), ApiPathMatch::PassThrough);
    }

    #[test]
    fn strip_api_prefix_is_none_when_unset() {
        // The prefix is process-wide; unit tests in this binary never configure it.
        assert_eq!(api_prefix(), None);
        assert_eq!(strip_api_prefix("/tools/rustfs/bucket"), ApiPathMatch::PassThrough);
        assert_eq!(strip_api_prefix("/%74ools/rustfs/bucket"), ApiPathMatch::PassThrough);
    }

    #[test]
    fn configured_api_prefix_subprocesses() {
        // (api prefix, console prefix, expected console prefix or None when startup must fail)
        for (api, console, expected) in [
            ("/tools/rustfs", None, Some("/tools/rustfs/rustfs/console")),
            ("/tools/rustfs/", Some("/tools/rustfs/console/"), Some("/tools/rustfs/console")),
            ("/tools/rustfs/api", Some("/tools/rustfs/console"), Some("/tools/rustfs/console")),
            ("", None, Some(CONSOLE_PREFIX)),
            ("/console", Some("/console"), None),
            ("/console/api", Some("/console"), None),
            ("/rustfs/admin", None, None),
            ("api", None, None),
        ] {
            let mut command = std::process::Command::new(std::env::current_exe().expect("test executable"));
            command
                .args(["api_prefix_process_case", "--test-threads=1"])
                .env("RUSTFS_TEST_API_PREFIX_PROCESS", expected.unwrap_or("fail"))
                .env(rustfs_config::ENV_RUSTFS_API_PREFIX, api);
            if let Some(console) = console {
                command.env(rustfs_config::ENV_RUSTFS_CONSOLE_PREFIX, console);
            } else {
                command.env_remove(rustfs_config::ENV_RUSTFS_CONSOLE_PREFIX);
            }
            let output = command.output().expect("run isolated API prefix test");
            assert!(
                output.status.success(),
                "api {api:?} console {console:?}: {}{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
        }
    }

    #[tokio::test]
    async fn api_prefix_process_case_routes() {
        use http::Request;
        use tower::{Layer, Service};
        let Ok(expected_console) = std::env::var("RUSTFS_TEST_API_PREFIX_PROCESS") else {
            return;
        };
        if expected_console == "fail" {
            let error = init_console_prefix().expect_err("invalid prefix combination must fail startup");
            assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
            return;
        }
        init_console_prefix().expect("initialize prefixes");
        assert_eq!(console_prefix(), expected_console);
        let api = std::env::var(rustfs_config::ENV_RUSTFS_API_PREFIX).expect("API prefix");
        let api = api.trim_end_matches('/');
        assert_eq!(api_prefix(), (!api.is_empty()).then_some(api));
        if api.is_empty() {
            assert_eq!(strip_api_prefix("/bucket/key"), ApiPathMatch::PassThrough);
            return;
        }
        assert_eq!(strip_api_prefix(&format!("{api}/bucket/key")), ApiPathMatch::Strip("/bucket/key"));
        assert_eq!(strip_api_prefix(api), ApiPathMatch::Strip(""));
        assert_eq!(strip_api_prefix("/bucket/key"), ApiPathMatch::PassThrough);
        assert_eq!(strip_api_prefix(&format!("{}/index.html", console_prefix())), ApiPathMatch::PassThrough);
        assert_eq!(strip_api_prefix(&format!("{api}/rustfs/rpc/x")), ApiPathMatch::PassThrough);
        let encoded = format!("/%{:02X}{}", api.as_bytes()[1], &api[2..]);
        assert_eq!(strip_api_prefix(&format!("{encoded}/bucket/key")), ApiPathMatch::Reject);

        // The recorded path itself is asserted in the layer tests; here only presence matters.
        let inner = tower::service_fn(|req: Request<()>| async move {
            let body = format!("{}|{}", req.uri(), !req.extensions().is_empty());
            Ok::<_, std::convert::Infallible>(http::Response::new(body))
        });
        let mut service = crate::server::layer::ApiPrefixLayer.layer(inner);
        let request = Request::builder()
            .uri(format!("{api}/bucket/key?x=1"))
            .body(())
            .expect("request");
        assert_eq!(service.call(request).await.expect("response").into_body(), "/bucket/key?x=1|true");
        let console_uri = format!("{}/index.html", console_prefix());
        let request = Request::builder().uri(&console_uri).body(()).expect("request");
        assert_eq!(service.call(request).await.expect("response").into_body(), format!("{console_uri}|false"));
        let request = Request::builder()
            .uri(format!("{encoded}/bucket/key"))
            .body(())
            .expect("request");
        let response = service.call(request).await.expect("response");
        assert_eq!(response.status(), http::StatusCode::BAD_REQUEST);
        assert!(response.into_body().is_empty());
    }
}
