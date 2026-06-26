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

use crate::admin::handlers::health::{HealthProbe, build_health_response_parts, collect_probe_readiness};
use crate::admin::runtime_sources::{default_admin_usecase, resolve_oidc_handle};
use crate::admin::storage_api::access::RequestContext;
use crate::license::has_valid_license;
use crate::server::has_path_prefix;
use crate::server::{
    CONSOLE_PREFIX, FAVICON_PATH, HEALTH_PREFIX, HEALTH_READY_PATH, HeaderMapCarrier, LICENSE, RUSTFS_ADMIN_PREFIX,
    RequestContextLayer, VERSION,
};
use crate::version::build;
use axum::{
    Json, Router,
    body::Body,
    extract::Request,
    middleware,
    response::{IntoResponse, Response},
    routing::get,
};
use http::{HeaderMap, HeaderName, HeaderValue, Method, StatusCode, Uri};
use mime_guess::from_path;
use opentelemetry::global;
use rust_embed::RustEmbed;
use serde::Serialize;
use std::{
    net::{IpAddr, SocketAddr},
    sync::OnceLock,
    time::Duration,
};
use tower_http::catch_panic::CatchPanicLayer;
use tower_http::compression::CompressionLayer;
use tower_http::cors::{AllowOrigin, Any, CorsLayer};
use tower_http::limit::RequestBodyLimitLayer;
use tower_http::request_id::{MakeRequestUuid, PropagateRequestIdLayer, SetRequestIdLayer};
use tower_http::timeout::TimeoutLayer;
use tower_http::trace::TraceLayer;
use tracing::{Span, debug, error, info, instrument, warn};
use tracing_opentelemetry::OpenTelemetrySpanExt;

#[derive(RustEmbed)]
#[folder = "$CARGO_MANIFEST_DIR/static"]
struct StaticFiles;

/// Static file handler
///
/// Serves static files embedded in the binary using rust-embed.
/// If the requested file is not found, it serves index.html as a fallback.
/// If index.html is also not found, it returns a 404 Not Found response.
///
/// # Arguments:
/// - `uri`: The request URI.
///
/// # Returns:
/// - An `impl IntoResponse` containing the static file content or a 404 response.
///
async fn static_handler(uri: Uri) -> impl IntoResponse {
    let mut path = uri.path().trim_start_matches('/');
    if path.is_empty() {
        path = "index.html"
    }

    // Try the exact path first
    if let Some(file) = StaticFiles::get(path) {
        let mime_type = from_path(path).first_or_octet_stream();
        return Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", mime_type.to_string())
            .body(Body::from(file.data))
            .unwrap();
    }

    // For directory paths (trailing slash), try <path>index.html
    if path.ends_with('/') {
        let index_path = format!("{path}index.html");
        if let Some(file) = StaticFiles::get(&index_path) {
            let mime_type = from_path(&index_path).first_or_octet_stream();
            return Response::builder()
                .status(StatusCode::OK)
                .header("Content-Type", mime_type.to_string())
                .body(Body::from(file.data))
                .unwrap();
        }
    }

    // SPA fallback: serve root index.html for client-side routing
    if let Some(file) = StaticFiles::get("index.html") {
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
    #[serde(skip_serializing_if = "Vec::is_empty")]
    oidc: Vec<OidcProviderInfo>,
}

#[derive(Debug, Serialize, Clone)]
struct OidcProviderInfo {
    provider_id: String,
    display_name: String,
}

impl Config {
    fn new(local_ip: IpAddr, port: u16, version: &str, date: &str) -> Self {
        let http_prefix = rustfs_config::RUSTFS_HTTP_PREFIX;

        // Collect OIDC provider info if available
        let oidc = resolve_oidc_handle()
            .map(|sys| {
                sys.list_visible_providers()
                    .into_iter()
                    .map(|p| OidcProviderInfo {
                        provider_id: p.provider_id,
                        display_name: p.display_name,
                    })
                    .collect()
            })
            .unwrap_or_default();

        Config {
            port,
            api: Api {
                base_url: build_console_api_base_url(&format!("{http_prefix}{local_ip}:{port}")),
                discovery: console_api_discovery(),
            },
            s3: S3 {
                endpoint: format!("{http_prefix}{local_ip}:{port}"),
                region: rustfs_config::RUSTFS_REGION.to_string(),
            },
            release: Release {
                version: version.to_string(),
                date: date.to_string(),
            },
            license: License {
                name: rustfs_config::RUSTFS_LICENSE.to_string(),
                url: rustfs_config::RUSTFS_LICENSE_URL.to_string(),
            },
            doc: rustfs_config::RUSTFS_DOCS_URL.to_string(),
            oidc,
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

fn build_console_api_base_url(base_url: &str) -> String {
    format!("{base_url}{RUSTFS_ADMIN_PREFIX}")
}

#[derive(Debug, Serialize, Clone)]
struct Api {
    #[serde(rename = "baseURL")]
    base_url: String,
    discovery: ApiDiscovery,
}

#[derive(Debug, Serialize, Clone)]
struct ApiDiscovery {
    #[serde(rename = "runtimeCapabilities")]
    runtime_capabilities: String,
    #[serde(rename = "clusterSnapshot")]
    cluster_snapshot: String,
    #[serde(rename = "extensionsCatalog")]
    extensions_catalog: String,
}

fn console_api_discovery() -> ApiDiscovery {
    let usecase = default_admin_usecase();
    ApiDiscovery {
        runtime_capabilities: usecase.runtime_capabilities_route().to_string(),
        cluster_snapshot: usecase.cluster_snapshot_route().to_string(),
        extensions_catalog: usecase.extensions_catalog_route().to_string(),
    }
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

/// Global console configuration
static CONSOLE_CONFIG: OnceLock<Config> = OnceLock::new();

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

#[derive(Serialize)]
struct LicensePublicStatus {
    licensed: bool,
}

/// Returns coarse public license status without exposing license metadata.
#[instrument]
async fn license_handler() -> impl IntoResponse {
    Json(LicensePublicStatus {
        licensed: has_valid_license(),
    })
}

/// Check if the given IP address is a private IP
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

/// Version handler
/// Returns the current version information of the console.
///
/// # Returns:
/// - 200 OK with JSON body containing version details if configuration is initialized.
/// - 500 Internal Server Error if configuration is not initialized.
#[instrument]
async fn version_handler() -> impl IntoResponse {
    match CONSOLE_CONFIG.get() {
        Some(cfg) => Response::builder()
            .header("content-type", "application/json")
            .status(StatusCode::OK)
            .body(Body::from(
                serde_json::json!({
                    "version": cfg.release.version,
                    "version_info": cfg.version_info(),
                    "date": cfg.release.date,
                })
                .to_string(),
            ))
            .unwrap(),
        None => Response::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .body(Body::from("Console configuration not initialized"))
            .unwrap(),
    }
}

/// Configuration handler
/// Returns the current console configuration in JSON format.
/// The configuration is dynamically adjusted based on the request's host and scheme.
///
/// # Arguments:
/// - `uri`: The request URI.
/// - `headers`: The request headers.
///
/// # Returns:
/// - 200 OK with JSON body containing the console configuration if initialized.
/// - 500 Internal Server Error if configuration is not initialized.
#[instrument(fields(uri))]
#[allow(dead_code)]
async fn config_handler(uri: Uri, headers: HeaderMap) -> impl IntoResponse {
    // Get the scheme from the headers or use the URI scheme
    let scheme = headers
        .get(HeaderName::from_static("x-forwarded-proto"))
        .and_then(|value| value.to_str().ok())
        .unwrap_or_else(|| uri.scheme().map(|s| s.as_str()).unwrap_or("http"));

    // Prefer URI host, fallback to `Host` header
    let header_host = headers
        .get(http::header::HOST)
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default();

    let raw_host = uri.host().unwrap_or(header_host);

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
    cfg.api.base_url = build_console_api_base_url(&url);
    cfg.s3.endpoint = url;

    Response::builder()
        .header("content-type", "application/json")
        .status(StatusCode::OK)
        .body(Body::from(cfg.to_json()))
        .unwrap()
}

/// Console access logging middleware
/// Logs each console access with method, URI, status code, and duration.
///
/// # Arguments:
/// - `req`: The incoming request.
/// - `next`: The next middleware or handler in the chain.
///
/// # Returns:
/// - The response from the next middleware or handler.
async fn console_logging_middleware(req: Request, next: middleware::Next) -> Response {
    let method = req.method().clone();
    let uri = req.uri().clone();
    let start = std::time::Instant::now();
    let response = next.run(req).await;
    let duration = start.elapsed();
    let request_id = response
        .headers()
        .get("x-request-id")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("unknown")
        .to_string();

    info!(
        target: "rustfs::console::access",
        request_id = %request_id,
        method = %method,
        uri = %uri,
        status = %response.status(),
        duration_ms = %duration.as_millis(),
        "Console access"
    );

    response
}

/// Get console configuration from environment variables
/// Returns a tuple containing console configuration values from environment variables.
///
/// # Returns:
/// - rate_limit_enable: bool indicating if rate limiting is enabled.
/// - rate_limit_rpm: u32 indicating the rate limit in requests per minute.
/// - auth_timeout: u64 indicating the authentication timeout in seconds.
/// - cors_allowed_origins: String containing allowed CORS origins.
///
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
        .unwrap_or_else(|_| rustfs_config::DEFAULT_CONSOLE_CORS_ALLOWED_ORIGINS.to_string());

    (rate_limit_enable, rate_limit_rpm, auth_timeout, cors_allowed_origins)
}

/// Check if the given path is for console access
///
/// # Arguments:
/// - `path`: The request path.
///
/// # Returns:
/// - `true` if the path is for console access, `false` otherwise.
pub fn is_console_path(path: &str) -> bool {
    path == FAVICON_PATH || has_path_prefix(path, CONSOLE_PREFIX)
}

/// Setup comprehensive middleware stack with tower-http features
///
/// # Arguments:
/// - `cors_layer`: The CORS layer to apply.
/// - `rate_limit_enable`: bool indicating if rate limiting is enabled.
/// - `rate_limit_rpm`: u32 indicating the rate limit in requests per minute.
/// - `auth_timeout`: u64 indicating the authentication timeout in seconds.
///
/// # Returns:
/// - A `Router` with the configured middleware stack.
fn setup_console_middleware_stack(
    cors_layer: CorsLayer,
    rate_limit_enable: bool,
    rate_limit_rpm: u32,
    auth_timeout: u64,
) -> Router {
    let mut app = Router::new()
        .route(FAVICON_PATH, get(static_handler))
        .route(&format!("{CONSOLE_PREFIX}{LICENSE}"), get(license_handler))
        .route(&format!("{CONSOLE_PREFIX}{VERSION}"), get(version_handler))
        .nest(CONSOLE_PREFIX, Router::new().fallback_service(get(static_handler)))
        .fallback_service(get(static_handler));

    if rustfs_utils::get_env_bool(rustfs_config::ENV_HEALTH_ENDPOINT_ENABLE, rustfs_config::DEFAULT_HEALTH_ENDPOINT_ENABLE) {
        app = app
            .route(&format!("{CONSOLE_PREFIX}{HEALTH_PREFIX}"), get(health_check).head(health_check))
            .route(
                &format!("{CONSOLE_PREFIX}{}", crate::server::HEALTH_COMPAT_LIVE_PATH),
                get(health_check).head(health_check),
            )
            .route(&format!("{CONSOLE_PREFIX}{HEALTH_READY_PATH}"), get(health_check).head(health_check));
    } else {
        // Keep disabled health probes from falling through to the SPA fallback.
        app = app
            .route(
                &format!("{CONSOLE_PREFIX}{HEALTH_PREFIX}"),
                get(health_route_disabled).head(health_route_disabled),
            )
            .route(
                &format!("{CONSOLE_PREFIX}{}", crate::server::HEALTH_COMPAT_LIVE_PATH),
                get(health_route_disabled).head(health_route_disabled),
            )
            .route(
                &format!("{CONSOLE_PREFIX}{HEALTH_READY_PATH}"),
                get(health_route_disabled).head(health_route_disabled),
            );
    }

    // Add comprehensive middleware layers using tower-http features
    app = app
        .layer(CatchPanicLayer::new())
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(|request: &Request| {
                    let request_context = request.extensions().get::<RequestContext>();
                    let request_id = request_context.map(|ctx| ctx.request_id.as_str()).unwrap_or("unknown");
                    let trace_id = request_context.and_then(|ctx| ctx.trace_id.as_deref()).unwrap_or("unknown");
                    let span_id = request_context.and_then(|ctx| ctx.span_id.as_deref()).unwrap_or("unknown");

                    let parent_context = global::get_text_map_propagator(|propagator| {
                        propagator.extract(&HeaderMapCarrier::new(request.headers()))
                    });

                    let span = tracing::info_span!(
                        "console-request",
                        request_id = %request_id,
                        trace_id = %trace_id,
                        span_id = %span_id,
                        method = %request.method(),
                        uri = %request.uri(),
                        status_code = tracing::field::Empty,
                    );

                    if span.is_disabled() {
                        return span;
                    }

                    if let Err(err) = span.set_parent(parent_context) {
                        debug!(error = ?err, "Failed to propagate tracing context for console request");
                    }

                    span
                })
                .on_response(|response: &Response, _latency: Duration, span: &Span| {
                    span.record("status_code", tracing::field::display(response.status()));
                }),
        )
        .layer(PropagateRequestIdLayer::x_request_id())
        .layer(RequestContextLayer)
        .layer(SetRequestIdLayer::x_request_id(MakeRequestUuid))
        // Compress responses
        .layer(CompressionLayer::new())
        .layer(middleware::from_fn(console_logging_middleware))
        .layer(cors_layer)
        // Add timeout layer - convert auth_timeout from seconds to Duration
        .layer(TimeoutLayer::with_status_code(
            StatusCode::REQUEST_TIMEOUT,
            Duration::from_secs(auth_timeout),
        ))
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
///
/// # Arguments:
/// - `method`: The HTTP method of the request.
///
/// # Returns:
/// - A `Response` containing the health check result.
#[instrument]
async fn health_check(method: Method, uri: Uri) -> Response {
    let probe = if uri.path().strip_prefix(CONSOLE_PREFIX) == Some(HEALTH_READY_PATH) {
        HealthProbe::Readiness
    } else {
        HealthProbe::Liveness
    };
    let readiness_report = collect_probe_readiness(probe).await;
    let uptime = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let response_parts =
        build_health_response_parts(method.clone(), probe, readiness_report.as_ref(), "rustfs-console", Some(uptime), None);

    let builder = Response::builder()
        .status(response_parts.status_code)
        .header("content-type", "application/json");

    match method {
        // GET: Returns complete JSON
        Method::GET => {
            let body_json = response_parts.payload.unwrap_or_else(|| {
                serde_json::json!({
                    "status": "error",
                    "service": "rustfs-console",
                })
            });

            // Return a minimal JSON when serialization fails to avoid panic
            let body_str = serde_json::to_string(&body_json).unwrap_or_else(|e| {
                error!(
                    target: "rustfs::console::health",
                    "failed to serialize health check body: {}",
                    e
                );
                // Simplified back-up JSON
                "{\"status\":\"error\",\"service\":\"rustfs-console\"}".to_string()
            });
            builder.body(Body::from(body_str)).unwrap_or_else(|e| {
                error!(
                    target: "rustfs::console::health",
                    "failed to build GET health response: {}",
                    e
                );
                Response::builder()
                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                    .body(Body::from("failed to build response"))
                    .unwrap_or_else(|_| Response::new(Body::from("")))
            })
        }

        // HEAD: Only status + headers are returned, body is empty
        Method::HEAD => builder.body(Body::empty()).unwrap_or_else(|e| {
            error!(
                target: "rustfs::console::health",
                "failed to build HEAD health response: {}",
                e
            );
            Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(Body::from("failed to build response"))
                .unwrap_or_else(|e| {
                    error!(
                        target: "rustfs::console::health",
                        "failed to build HEAD health empty response, reason: {}",
                        e
                    );
                    Response::new(Body::from(""))
                })
        }),

        // Other methods: 405
        _ => Response::builder()
            .status(StatusCode::METHOD_NOT_ALLOWED)
            .header("allow", "GET, HEAD")
            .body(Body::from("Method Not Allowed"))
            .unwrap_or_else(|e| {
                error!(
                    target: "rustfs::console::health",
                    "failed to build 405 response: {}",
                    e
                );
                Response::new(Body::from("Method Not Allowed"))
            }),
    }
}

async fn health_route_disabled() -> StatusCode {
    StatusCode::NOT_FOUND
}

/// Parse CORS allowed origins from configuration.
///
/// When no origins are configured (None or an empty string), the layer is
/// left without `Access-Control-Allow-Origin` so browsers treat responses
/// as same-origin only. Operators that need cross-origin access set
/// `RUSTFS_CONSOLE_CORS_ALLOWED_ORIGINS` to a comma-separated allow-list,
/// or to `*` to allow any origin.
///
/// # Arguments
/// - `origins`: An optional reference to a string containing allowed origins.
///
/// # Returns
/// - A `CorsLayer` configured with the specified origins.
pub fn parse_cors_origins(origins: Option<&String>) -> CorsLayer {
    let cors_layer = CorsLayer::new()
        .allow_methods([Method::GET, Method::POST, Method::PUT, Method::DELETE, Method::OPTIONS])
        .allow_headers(Any);

    match origins {
        Some(origins_str) if origins_str.trim() == "*" => cors_layer.allow_origin(Any).expose_headers(Any),
        Some(origins_str) if !origins_str.trim().is_empty() => {
            let origins: Vec<&str> = origins_str.split(',').map(|s| s.trim()).filter(|s| !s.is_empty()).collect();
            let mut valid_origins = Vec::new();
            for origin in origins {
                match origin.parse::<HeaderValue>() {
                    Ok(header_value) => valid_origins.push(header_value),
                    Err(e) => warn!("Invalid CORS origin '{}': {}", origin, e),
                }
            }

            if valid_origins.is_empty() {
                warn!("No valid CORS origins parsed from configuration; defaulting to same-origin only");
                cors_layer
            } else {
                info!("Console CORS origins configured: {:?}", valid_origins);
                cors_layer.allow_origin(AllowOrigin::list(valid_origins)).expose_headers(Any)
            }
        }
        _ => {
            debug!("No CORS origins configured for console; same-origin only");
            cors_layer
        }
    }
}

/// Create and configure the console server router
///
/// # Returns:
/// - A `Router` configured for the console server with middleware.
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

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::routing::get;
    use http::{Request, StatusCode};
    use http_body_util::BodyExt;
    use serial_test::serial;
    use std::io;
    use std::net::{IpAddr, Ipv4Addr};
    use std::sync::{Arc, Mutex};
    use temp_env::async_with_vars;
    use tower::ServiceExt;
    use tracing_subscriber::{Registry, layer::SubscriberExt};

    #[derive(Clone, Default)]
    struct SharedWriter {
        inner: Arc<Mutex<Vec<u8>>>,
    }

    struct SharedWriterGuard {
        inner: Arc<Mutex<Vec<u8>>>,
    }

    impl<'writer> tracing_subscriber::fmt::MakeWriter<'writer> for SharedWriter {
        type Writer = SharedWriterGuard;

        fn make_writer(&'writer self) -> Self::Writer {
            SharedWriterGuard {
                inner: Arc::clone(&self.inner),
            }
        }
    }

    impl io::Write for SharedWriterGuard {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            let mut inner = self.inner.lock().expect("shared writer lock should not be poisoned");
            inner.extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn console_api_base_url_keeps_rustfs_admin_prefix() {
        let cfg = Config::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 9001, "test", "2026-03-16T00:00:00Z");

        assert!(
            cfg.api.base_url.ends_with("/rustfs/admin/v3"),
            "console baseURL must keep using the RustFS admin prefix"
        );
        assert!(
            !cfg.api.base_url.ends_with("/minio/admin/v3"),
            "console baseURL must not switch to the MinIO admin prefix by default"
        );
    }

    #[test]
    fn console_api_base_url_builder_preserves_existing_console_contract() {
        assert_eq!(
            build_console_api_base_url("http://127.0.0.1:9001"),
            "http://127.0.0.1:9001/rustfs/admin/v3"
        );
    }

    #[test]
    fn console_config_exposes_admin_discovery_paths() {
        let cfg = Config::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 9001, "test", "2026-03-16T00:00:00Z");

        assert_eq!(cfg.api.discovery.runtime_capabilities, "/rustfs/admin/v4/runtime/capabilities");
        assert_eq!(cfg.api.discovery.cluster_snapshot, "/rustfs/admin/v4/cluster/snapshot");
        assert_eq!(cfg.api.discovery.extensions_catalog, "/rustfs/admin/v4/extensions/catalog");
    }

    #[tokio::test]
    async fn console_config_handler_serializes_admin_discovery_paths() {
        init_console_cfg(IpAddr::V4(Ipv4Addr::LOCALHOST), 9001);

        let response = config_handler(Uri::from_static("http://127.0.0.1:9001/rustfs/console/api/v1/config"), HeaderMap::new())
            .await
            .into_response();

        assert_eq!(response.status(), StatusCode::OK);
        let body = response.into_body();
        let bytes = body.collect().await.expect("collect console config body").to_bytes();
        let value: serde_json::Value = serde_json::from_slice(&bytes).expect("console config JSON should deserialize");

        assert_eq!(value["api"]["discovery"]["runtimeCapabilities"], "/rustfs/admin/v4/runtime/capabilities");
        assert_eq!(value["api"]["discovery"]["clusterSnapshot"], "/rustfs/admin/v4/cluster/snapshot");
        assert_eq!(value["api"]["discovery"]["extensionsCatalog"], "/rustfs/admin/v4/extensions/catalog");
    }

    #[test]
    fn external_admin_paths_are_not_console_paths() {
        assert!(is_console_path("/rustfs/console/"));
        assert!(!is_console_path("/minio/admin/v3/info"));
        assert!(!is_console_path("/rustfs/admin/v3/info"));
    }

    // setup_console_middleware_stack reads ENV_HEALTH_ENDPOINT_ENABLE; serialise
    // with other tests that override that env var to avoid cross-task leakage.
    #[tokio::test]
    #[serial]
    async fn console_middleware_stack_propagates_request_id_header() {
        let app = setup_console_middleware_stack(parse_cors_origins(None), false, 0, 30);
        let request = Request::builder()
            .uri(format!("{CONSOLE_PREFIX}{HEALTH_PREFIX}"))
            .body(Body::empty())
            .expect("failed to build request");

        let response = app.oneshot(request).await.expect("request should succeed");
        assert_eq!(response.status(), StatusCode::OK);
        assert!(
            response.headers().contains_key("x-request-id"),
            "console response should include propagated x-request-id header"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn console_trace_layer_records_request_id_on_current_span() {
        let writer = SharedWriter::default();
        let subscriber = Registry::default().with(
            tracing_subscriber::fmt::layer()
                .without_time()
                .with_target(false)
                .with_level(false)
                .with_ansi(false)
                .json()
                .flatten_event(true)
                .with_current_span(true)
                .with_span_list(true)
                .with_writer(writer.clone()),
        );
        let _guard = tracing::subscriber::set_default(subscriber);

        let app = Router::new()
            .route(
                "/trace-test",
                get(|| async {
                    tracing::info!("console handler log");
                    StatusCode::OK
                }),
            )
            .layer(
                TraceLayer::new_for_http()
                    .make_span_with(|request: &http::Request<_>| {
                        let request_context = request.extensions().get::<RequestContext>();
                        let request_id = request_context.map(|ctx| ctx.request_id.as_str()).unwrap_or("unknown");
                        let trace_id = request_context.and_then(|ctx| ctx.trace_id.as_deref()).unwrap_or("unknown");
                        let span_id = request_context.and_then(|ctx| ctx.span_id.as_deref()).unwrap_or("unknown");

                        let parent_context = global::get_text_map_propagator(|propagator| {
                            propagator.extract(&HeaderMapCarrier::new(request.headers()))
                        });

                        let span = tracing::info_span!(
                            "console-request",
                            request_id = %request_id,
                            trace_id = %trace_id,
                            span_id = %span_id,
                            method = %request.method(),
                            uri = %request.uri(),
                            status_code = tracing::field::Empty,
                        );

                        if span.is_disabled() {
                            return span;
                        }

                        if let Err(err) = span.set_parent(parent_context) {
                            debug!(error = ?err, "Failed to propagate tracing context for console request");
                        }

                        span
                    })
                    .on_response(|response: &Response, _latency: Duration, span: &Span| {
                        span.record("status_code", tracing::field::display(response.status()));
                    }),
            )
            .layer(RequestContextLayer)
            .layer(SetRequestIdLayer::x_request_id(MakeRequestUuid));

        app.oneshot(
            Request::builder()
                .uri("/trace-test")
                .body(Body::empty())
                .expect("failed to build trace test request"),
        )
        .await
        .expect("trace test request should complete");

        let output = String::from_utf8(
            writer
                .inner
                .lock()
                .expect("shared writer lock should not be poisoned")
                .clone(),
        )
        .expect("console trace log output should be valid UTF-8");

        let log = output
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).expect("console log line should be valid JSON"))
            .find(|value| value.get("message").and_then(serde_json::Value::as_str) == Some("console handler log"))
            .expect("expected console handler log entry");

        assert_eq!(log["span"]["method"], serde_json::Value::String("GET".to_string()));
        assert_eq!(log["span"]["name"], serde_json::Value::String("console-request".to_string()));
        assert!(
            log.get("span")
                .and_then(|span| span.get("request_id"))
                .and_then(serde_json::Value::as_str)
                .is_some(),
            "{output}"
        );
        assert_ne!(
            log.get("span")
                .and_then(|span| span.get("request_id"))
                .and_then(serde_json::Value::as_str),
            Some("unknown"),
            "{output}"
        );
    }

    /// Regression: when no console CORS origins are configured (the new
    /// default), the layer must NOT emit `Access-Control-Allow-Origin`, so
    /// browsers treat responses as same-origin only.
    #[tokio::test]
    #[serial]
    async fn default_console_cors_is_same_origin_only() {
        let app = setup_console_middleware_stack(parse_cors_origins(None), false, 0, 30);

        let request = Request::builder()
            .method("OPTIONS")
            .uri(format!("{CONSOLE_PREFIX}/license"))
            .header("origin", "https://example.com")
            .header("access-control-request-method", "GET")
            .body(Body::empty())
            .expect("build preflight");

        let response = app.oneshot(request).await.expect("preflight should complete");

        assert!(
            response.headers().get("access-control-allow-origin").is_none(),
            "default console CORS must not emit Access-Control-Allow-Origin"
        );
        assert!(
            response.headers().get("access-control-allow-credentials").is_none(),
            "default console CORS must not emit Access-Control-Allow-Credentials"
        );
    }

    /// Operators that opt in to wildcard origins (via `*`) keep the previous
    /// permissive behavior.
    #[tokio::test]
    #[serial]
    async fn explicit_wildcard_console_cors_allows_any_origin() {
        let star = "*".to_string();
        let app = setup_console_middleware_stack(parse_cors_origins(Some(&star)), false, 0, 30);

        let request = Request::builder()
            .method("OPTIONS")
            .uri(format!("{CONSOLE_PREFIX}/license"))
            .header("origin", "https://example.com")
            .header("access-control-request-method", "GET")
            .body(Body::empty())
            .expect("build preflight");

        let response = app.oneshot(request).await.expect("preflight should complete");

        assert_eq!(
            response
                .headers()
                .get("access-control-allow-origin")
                .and_then(|v| v.to_str().ok()),
            Some("*"),
            "explicit `*` origin must produce wildcard Allow-Origin"
        );
    }

    /// Whitespace-padded wildcard ("` * `") must still be treated as wildcard
    /// rather than falling into the comma-separated parser. Common when the
    /// origin string is templated through env vars.
    #[tokio::test]
    #[serial]
    async fn whitespace_padded_wildcard_console_cors_allows_any_origin() {
        let star = " * ".to_string();
        let app = setup_console_middleware_stack(parse_cors_origins(Some(&star)), false, 0, 30);

        let request = Request::builder()
            .method("OPTIONS")
            .uri(format!("{CONSOLE_PREFIX}/license"))
            .header("origin", "https://example.com")
            .header("access-control-request-method", "GET")
            .body(Body::empty())
            .expect("build preflight");

        let response = app.oneshot(request).await.expect("preflight should complete");

        assert_eq!(
            response
                .headers()
                .get("access-control-allow-origin")
                .and_then(|v| v.to_str().ok()),
            Some("*"),
            "whitespace-padded `*` origin must produce wildcard Allow-Origin"
        );
    }

    // Mutates the global ENV_HEALTH_ENDPOINT_ENABLE env var; serialise to
    // avoid leaking the override into other async tests in the same module.
    #[tokio::test]
    #[serial]
    async fn console_middleware_stack_hides_health_routes_when_disabled() {
        async_with_vars([(rustfs_config::ENV_HEALTH_ENDPOINT_ENABLE, Some("false"))], async {
            let app = setup_console_middleware_stack(parse_cors_origins(None), false, 0, 30);

            let health_response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .uri(format!("{CONSOLE_PREFIX}{HEALTH_PREFIX}"))
                        .body(Body::empty())
                        .expect("failed to build health request"),
                )
                .await
                .expect("health request should complete");
            assert_eq!(health_response.status(), StatusCode::NOT_FOUND);

            let readiness_response = app
                .oneshot(
                    Request::builder()
                        .uri(format!("{CONSOLE_PREFIX}{HEALTH_READY_PATH}"))
                        .body(Body::empty())
                        .expect("failed to build readiness request"),
                )
                .await
                .expect("readiness request should complete");
            assert_eq!(readiness_response.status(), StatusCode::NOT_FOUND);
        })
        .await;
    }

    #[tokio::test]
    async fn console_license_route_returns_public_status_only() {
        let app = setup_console_middleware_stack(parse_cors_origins(None), false, 0, 30);
        let request = Request::builder()
            .uri(format!("{CONSOLE_PREFIX}{LICENSE}"))
            .body(Body::empty())
            .expect("failed to build license request");

        let response = app.oneshot(request).await.expect("license request should complete");
        assert_eq!(response.status(), StatusCode::OK);

        let body = response
            .into_body()
            .collect()
            .await
            .expect("license body should collect")
            .to_bytes();
        let value: serde_json::Value = serde_json::from_slice(&body).expect("license response should be valid JSON");

        assert_eq!(value, serde_json::json!({ "licensed": false }));
        assert!(value.get("name").is_none());
        assert!(value.get("expired").is_none());
    }
}
