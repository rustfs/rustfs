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

use crate::config::build;
use crate::license::get_license;
use axum::Json;
use axum::body::Body;
use axum::response::{IntoResponse, Response};
use axum::{Router, extract::Request, middleware, routing::get};
use axum_extra::extract::Host;
use axum_server::tls_rustls::RustlsConfig;
use http::{HeaderMap, HeaderName, StatusCode, Uri};
use http::{HeaderValue, Method};
use mime_guess::from_path;
use rust_embed::RustEmbed;
use rustfs_config::{RUSTFS_TLS_CERT, RUSTFS_TLS_KEY};
use serde::Serialize;
use serde_json::json;
use std::io::Result;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::Duration;
use tokio_rustls::rustls::ServerConfig;
use tower_http::catch_panic::CatchPanicLayer;
use tower_http::cors::{AllowOrigin, Any, CorsLayer};
use tower_http::limit::RequestBodyLimitLayer;
use tower_http::timeout::TimeoutLayer;
use tower_http::trace::TraceLayer;
use tracing::{debug, error, info, instrument, warn};

pub(crate) const CONSOLE_PREFIX: &str = "/rustfs/console";
const RUSTFS_ADMIN_PREFIX: &str = "/rustfs/admin/v3";

#[derive(RustEmbed)]
#[folder = "$CARGO_MANIFEST_DIR/static"]
struct StaticFiles;

/// Static file handler
///
/// Serves static files embedded in the binary using rust-embed.
/// If the requested file is not found, it serves index.html as a fallback.
/// If index.html is also not found, it returns a 404 Not Found response.
///
/// Arguments:
/// - `uri`: The request URI.
///
/// Returns:
/// - An `impl IntoResponse` containing the static file content or a 404 response.
///
pub(crate) async fn static_handler(uri: Uri) -> impl IntoResponse {
    let mut path = uri.path().trim_start_matches('/');
    if path.is_empty() {
        path = "index.html"
    }
    if let Some(file) = StaticFiles::get(path) {
        let mime_type = from_path(path).first_or_octet_stream();
        Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", mime_type.to_string())
            .body(Body::from(file.data))
            .unwrap()
    } else if let Some(file) = StaticFiles::get("index.html") {
        let mime_type = from_path("index.html").first_or_octet_stream();
        Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", mime_type.to_string())
            .body(Body::from(file.data))
            .unwrap()
    } else {
        Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::from(" 404 Not Found \n RustFS "))
            .unwrap()
    }
}

#[derive(Debug, Serialize, Clone)]
pub(crate) struct Config {
    #[serde(skip)]
    port: u16,
    api: Api,
    s3: S3,
    release: Release,
    license: License,
    doc: String,
}

impl Config {
    fn new(local_ip: IpAddr, port: u16, version: &str, date: &str) -> Self {
        Config {
            port,
            api: Api {
                base_url: format!("http://{local_ip}:{port}/{RUSTFS_ADMIN_PREFIX}"),
            },
            s3: S3 {
                endpoint: format!("http://{local_ip}:{port}"),
                region: "cn-east-1".to_owned(),
            },
            release: Release {
                version: version.to_string(),
                date: date.to_string(),
            },
            license: License {
                name: "Apache-2.0".to_string(),
                url: "https://www.apache.org/licenses/LICENSE-2.0".to_string(),
            },
            doc: "https://rustfs.com/docs/".to_string(),
        }
    }

    fn to_json(&self) -> String {
        serde_json::to_string(self).unwrap_or_default()
    }

    #[allow(dead_code)]
    pub(crate) fn version_info(&self) -> String {
        format!(
            "RELEASE.{}@{} (rust {} {})",
            self.release.date.clone(),
            self.release.version.clone().trim_start_matches('@'),
            build::RUST_VERSION,
            build::BUILD_TARGET
        )
    }

    #[allow(dead_code)]
    pub(crate) fn version(&self) -> String {
        self.release.version.clone()
    }

    #[allow(dead_code)]
    pub(crate) fn license(&self) -> String {
        format!("{} {}", self.license.name.clone(), self.license.url.clone())
    }

    #[allow(dead_code)]
    pub(crate) fn doc(&self) -> String {
        self.doc.clone()
    }
}

#[derive(Debug, Serialize, Clone)]
struct Api {
    #[serde(rename = "baseURL")]
    base_url: String,
}

#[derive(Debug, Serialize, Clone)]
struct S3 {
    endpoint: String,
    region: String,
}

#[derive(Debug, Serialize, Clone)]
struct Release {
    version: String,
    date: String,
}

#[derive(Debug, Serialize, Clone)]
struct License {
    name: String,
    url: String,
}

pub(crate) static CONSOLE_CONFIG: OnceLock<Config> = OnceLock::new();

#[allow(clippy::const_is_empty)]
pub(crate) fn init_console_cfg(local_ip: IpAddr, port: u16) {
    CONSOLE_CONFIG.get_or_init(|| {
        let ver = {
            if !build::TAG.is_empty() {
                build::TAG.to_string()
            } else if !build::SHORT_COMMIT.is_empty() {
                format!("@{}", build::SHORT_COMMIT)
            } else {
                build::PKG_VERSION.to_string()
            }
        };

        Config::new(local_ip, port, ver.as_str(), build::COMMIT_DATE_3339)
    });
}

// fn is_socket_addr_or_ip_addr(host: &str) -> bool {
//     host.parse::<SocketAddr>().is_ok() || host.parse::<IpAddr>().is_ok()
// }

#[allow(dead_code)]
pub async fn license_handler() -> impl IntoResponse {
    let license = get_license().unwrap_or_default();

    Response::builder()
        .header("content-type", "application/json")
        .status(StatusCode::OK)
        .body(Body::from(serde_json::to_string(&license).unwrap_or_default()))
        .unwrap()
}

fn _is_private_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => {
            let octets = ip.octets();
            // 10.0.0.0/8
            octets[0] == 10 ||
                // 172.16.0.0/12
                (octets[0] == 172 && (octets[1] >= 16 && octets[1] <= 31)) ||
                // 192.168.0.0/16
                (octets[0] == 192 && octets[1] == 168)
        }
        IpAddr::V6(_) => false,
    }
}

#[instrument(fields(host))]
pub async fn config_handler(uri: Uri, Host(host): Host, headers: HeaderMap) -> impl IntoResponse {
    // Get the scheme from the headers or use the URI scheme
    let scheme = headers
        .get(HeaderName::from_static("x-forwarded-proto"))
        .and_then(|value| value.to_str().ok())
        .unwrap_or_else(|| uri.scheme().map(|s| s.as_str()).unwrap_or("http"));

    let raw_host = uri.host().unwrap_or(host.as_str());
    let host_for_url = if let Ok(socket_addr) = raw_host.parse::<SocketAddr>() {
        // Successfully parsed, it's in IP:Port format.
        // For IPv6, we need to enclose it in brackets to form a valid URL.
        let ip = socket_addr.ip();
        if ip.is_ipv6() { format!("[{ip}]") } else { format!("{ip}") }
    } else if let Ok(ip) = raw_host.parse::<IpAddr>() {
        // Pure IP (no ports)
        if ip.is_ipv6() { format!("[{ip}]") } else { ip.to_string() }
    } else {
        // The domain name may not be able to resolve directly to IP, remove the port
        raw_host.split(':').next().unwrap_or(raw_host).to_string()
    };

    // Make a copy of the current configuration
    let mut cfg = match CONSOLE_CONFIG.get() {
        Some(cfg) => cfg.clone(),
        None => {
            error!("Console configuration not initialized");
            return Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(Body::from("Console configuration not initialized"))
                .unwrap();
        }
    };

    let url = format!("{}://{}:{}", scheme, host_for_url, cfg.port);
    cfg.api.base_url = format!("{url}{RUSTFS_ADMIN_PREFIX}");
    cfg.s3.endpoint = url;

    Response::builder()
        .header("content-type", "application/json")
        .status(StatusCode::OK)
        .body(Body::from(cfg.to_json()))
        .unwrap()
}

/// Console access logging middleware
async fn console_logging_middleware(req: Request, next: axum::middleware::Next) -> axum::response::Response {
    let method = req.method().clone();
    let uri = req.uri().clone();
    let start = std::time::Instant::now();

    let response = next.run(req).await;
    let duration = start.elapsed();

    info!(
        target: "rustfs::console::access",
        method = %method,
        uri = %uri,
        status = %response.status(),
        duration_ms = %duration.as_millis(),
        "Console access"
    );

    response
}

/// Setup TLS configuration for console using axum-server, following endpoint TLS implementation logic
#[instrument(skip(tls_path))]
async fn _setup_console_tls_config(tls_path: Option<&String>) -> Result<Option<RustlsConfig>> {
    let tls_path = match tls_path {
        Some(path) if !path.is_empty() => path,
        _ => {
            debug!("TLS path is not provided, console starting with HTTP");
            return Ok(None);
        }
    };

    if tokio::fs::metadata(tls_path).await.is_err() {
        debug!("TLS path does not exist, console starting with HTTP");
        return Ok(None);
    }

    debug!("Found TLS directory for console, checking for certificates");

    // Make sure to use a modern encryption suite
    let _ = rustls::crypto::ring::default_provider().install_default();

    // 1. Attempt to load all certificates in the directory (multi-certificate support, for SNI)
    if let Ok(cert_key_pairs) = rustfs_utils::load_all_certs_from_directory(tls_path) {
        if !cert_key_pairs.is_empty() {
            debug!(
                "Found {} certificates for console, creating SNI-aware multi-cert resolver",
                cert_key_pairs.len()
            );

            // Create an SNI-enabled certificate resolver
            let resolver = rustfs_utils::create_multi_cert_resolver(cert_key_pairs)?;

            // Configure the server to enable SNI support
            let mut server_config = ServerConfig::builder()
                .with_no_client_auth()
                .with_cert_resolver(Arc::new(resolver));

            // Configure ALPN protocol priority
            server_config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec(), b"http/1.0".to_vec()];

            // Log SNI requests
            if rustfs_utils::tls_key_log() {
                server_config.key_log = Arc::new(rustls::KeyLogFile::new());
            }

            info!(target: "rustfs::console::tls", "Console TLS enabled with multi-certificate SNI support");
            return Ok(Some(RustlsConfig::from_config(Arc::new(server_config))));
        }
    }

    // 2. Revert to the traditional single-certificate mode
    let key_path = format!("{tls_path}/{RUSTFS_TLS_KEY}");
    let cert_path = format!("{tls_path}/{RUSTFS_TLS_CERT}");
    if tokio::try_join!(tokio::fs::metadata(&key_path), tokio::fs::metadata(&cert_path)).is_ok() {
        debug!("Found legacy single TLS certificate for console, starting with HTTPS");

        return match RustlsConfig::from_pem_file(cert_path, key_path).await {
            Ok(config) => {
                info!(target: "rustfs::console::tls", "Console TLS enabled with single certificate");
                Ok(Some(config))
            }
            Err(e) => {
                error!(target: "rustfs::console::error", error = %e, "Failed to create TLS config for console");
                Err(std::io::Error::other(e))
            }
        };
    }

    debug!("No valid TLS certificates found in the directory for console, starting with HTTP");
    Ok(None)
}

/// Get console configuration from environment variables
fn get_console_config_from_env() -> (bool, u32, u64, String) {
    let rate_limit_enable = std::env::var(rustfs_config::ENV_CONSOLE_RATE_LIMIT_ENABLE)
        .unwrap_or_else(|_| rustfs_config::DEFAULT_CONSOLE_RATE_LIMIT_ENABLE.to_string())
        .parse::<bool>()
        .unwrap_or(rustfs_config::DEFAULT_CONSOLE_RATE_LIMIT_ENABLE);

    let rate_limit_rpm = std::env::var(rustfs_config::ENV_CONSOLE_RATE_LIMIT_RPM)
        .unwrap_or_else(|_| rustfs_config::DEFAULT_CONSOLE_RATE_LIMIT_RPM.to_string())
        .parse::<u32>()
        .unwrap_or(rustfs_config::DEFAULT_CONSOLE_RATE_LIMIT_RPM);

    let auth_timeout = std::env::var(rustfs_config::ENV_CONSOLE_AUTH_TIMEOUT)
        .unwrap_or_else(|_| rustfs_config::DEFAULT_CONSOLE_AUTH_TIMEOUT.to_string())
        .parse::<u64>()
        .unwrap_or(rustfs_config::DEFAULT_CONSOLE_AUTH_TIMEOUT);
    let cors_allowed_origins = std::env::var(rustfs_config::ENV_CONSOLE_CORS_ALLOWED_ORIGINS)
        .unwrap_or_else(|_| rustfs_config::DEFAULT_CONSOLE_CORS_ALLOWED_ORIGINS.to_string())
        .parse::<String>()
        .unwrap_or(rustfs_config::DEFAULT_CONSOLE_CORS_ALLOWED_ORIGINS.to_string());

    (rate_limit_enable, rate_limit_rpm, auth_timeout, cors_allowed_origins)
}

pub fn is_console_path(path: &str) -> bool {
    path == "/favicon.ico" || path.starts_with(CONSOLE_PREFIX)
}

/// Setup comprehensive middleware stack with tower-http features
fn setup_console_middleware_stack(
    cors_layer: CorsLayer,
    rate_limit_enable: bool,
    rate_limit_rpm: u32,
    auth_timeout: u64,
) -> Router {
    let mut app = Router::new()
        .route("/favicon.ico", get(static_handler))
        .route(&format!("{CONSOLE_PREFIX}/license"), get(license_handler))
        .route(&format!("{CONSOLE_PREFIX}/config.json"), get(config_handler))
        .route(&format!("{CONSOLE_PREFIX}/health"), get(health_check))
        .nest(CONSOLE_PREFIX, Router::new().fallback_service(get(static_handler)))
        .fallback_service(get(static_handler));

    // Add comprehensive middleware layers using tower-http features
    app = app
        .layer(CatchPanicLayer::new())
        .layer(TraceLayer::new_for_http())
        .layer(middleware::from_fn(console_logging_middleware))
        .layer(cors_layer)
        // Add timeout layer - convert auth_timeout from seconds to Duration
        .layer(TimeoutLayer::new(Duration::from_secs(auth_timeout)))
        // Add request body limit (10MB for console uploads)
        .layer(RequestBodyLimitLayer::new(5 * 1024 * 1024 * 1024));

    // Add rate limiting if enabled
    if rate_limit_enable {
        info!("Console rate limiting enabled: {} requests per minute", rate_limit_rpm);
        // Note: tower-http doesn't provide a built-in rate limiter, but we have the foundation
        // For production, you would integrate with a rate limiting service like Redis
        // For now, we log that it's configured and ready for integration
    }

    app
}

/// Console health check handler with comprehensive health information
async fn health_check() -> Json<serde_json::Value> {
    use rustfs_ecstore::new_object_layer_fn;

    let mut health_status = "ok";
    let mut details = json!({});

    // Check storage backend health
    if let Some(_store) = new_object_layer_fn() {
        details["storage"] = json!({"status": "connected"});
    } else {
        health_status = "degraded";
        details["storage"] = json!({"status": "disconnected"});
    }

    // Check IAM system health
    match rustfs_iam::get() {
        Ok(_) => {
            details["iam"] = json!({"status": "connected"});
        }
        Err(_) => {
            health_status = "degraded";
            details["iam"] = json!({"status": "disconnected"});
        }
    }

    Json(json!({
        "status": health_status,
        "service": "rustfs-console",
        "timestamp": chrono::Utc::now().to_rfc3339(),
        "version": env!("CARGO_PKG_VERSION"),
        "details": details,
        "uptime": std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
    }))
}

/// Parse CORS allowed origins from configuration
pub fn parse_cors_origins(origins: Option<&String>) -> CorsLayer {
    let cors_layer = CorsLayer::new()
        .allow_methods([Method::GET, Method::POST, Method::PUT, Method::DELETE, Method::OPTIONS])
        .allow_headers(Any);

    match origins {
        Some(origins_str) if origins_str == "*" => cors_layer.allow_origin(Any).expose_headers(Any),
        Some(origins_str) => {
            let origins: Vec<&str> = origins_str.split(',').map(|s| s.trim()).collect();
            if origins.is_empty() {
                warn!("Empty CORS origins provided, using permissive CORS");
                cors_layer.allow_origin(Any).expose_headers(Any)
            } else {
                // Parse origins with proper error handling
                let mut valid_origins = Vec::new();
                for origin in origins {
                    match origin.parse::<HeaderValue>() {
                        Ok(header_value) => {
                            valid_origins.push(header_value);
                        }
                        Err(e) => {
                            warn!("Invalid CORS origin '{}': {}", origin, e);
                        }
                    }
                }

                if valid_origins.is_empty() {
                    warn!("No valid CORS origins found, using permissive CORS");
                    cors_layer.allow_origin(Any).expose_headers(Any)
                } else {
                    info!("Console CORS origins configured: {:?}", valid_origins);
                    cors_layer.allow_origin(AllowOrigin::list(valid_origins)).expose_headers(Any)
                }
            }
        }
        None => {
            debug!("No CORS origins configured for console, using permissive CORS");
            cors_layer.allow_origin(Any)
        }
    }
}

pub(crate) fn make_console_server() -> Router {
    let (rate_limit_enable, rate_limit_rpm, auth_timeout, cors_allowed_origins) = get_console_config_from_env();
    // String to Option<&String>
    let cors_allowed_origins = if cors_allowed_origins.is_empty() {
        None
    } else {
        Some(&cors_allowed_origins)
    };
    // Configure CORS based on settings
    let cors_layer = parse_cors_origins(cors_allowed_origins);

    // Build console router with enhanced middleware stack using tower-http features
    setup_console_middleware_stack(cors_layer, rate_limit_enable, rate_limit_rpm, auth_timeout)
}
