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

/// The prefix is fixed before listeners start; request handling never reads the environment.
pub(crate) fn console_prefix() -> &'static str {
    CONFIGURED_CONSOLE_PREFIX.get().map(String::as_str).unwrap_or(CONSOLE_PREFIX)
}

pub(crate) fn init_console_prefix() -> std::io::Result<()> {
    let raw = match std::env::var(rustfs_config::ENV_RUSTFS_CONSOLE_PREFIX) {
        Ok(value) => value,
        Err(std::env::VarError::NotPresent) => CONSOLE_PREFIX.to_string(),
        Err(err) => return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, err)),
    };
    let prefix = validate_console_prefix(&raw)?;
    if CONFIGURED_CONSOLE_PREFIX.get_or_init(|| prefix.clone()) != &prefix {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "RUSTFS_CONSOLE_PREFIX cannot change after server initialization",
        ));
    }
    Ok(())
}

fn validate_console_prefix(raw: &str) -> std::io::Result<String> {
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
            "RUSTFS_CONSOLE_PREFIX must be a non-root absolute path of at most 256 bytes with nonempty URL-safe segments",
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
            "RUSTFS_CONSOLE_PREFIX overlaps a reserved server route",
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
            assert_eq!(validate_console_prefix(raw).expect("valid console prefix"), expected);
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
            assert_eq!(validate_console_prefix(raw).expect_err(raw).kind(), std::io::ErrorKind::InvalidInput);
        }
        assert!(validate_console_prefix(&format!("/{}", "a".repeat(255))).is_ok());
        assert!(validate_console_prefix(&format!("/{}", "a".repeat(256))).is_err());
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
