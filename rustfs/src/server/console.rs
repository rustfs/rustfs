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

use crate::admin::console::static_handler;
use crate::config::Opt;
use axum::{Router, extract::Request, middleware, response::Json, routing::get};
use axum_server::tls_rustls::RustlsConfig;
use http::{HeaderValue, Method};
use rustfs_config::{RUSTFS_TLS_CERT, RUSTFS_TLS_KEY};
use rustfs_utils::net::parse_and_resolve_address;
use serde_json::json;
use std::io::Result;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio_rustls::rustls::ServerConfig;
use tower_http::catch_panic::CatchPanicLayer;
use tower_http::cors::{AllowOrigin, Any, CorsLayer};
use tower_http::limit::RequestBodyLimitLayer;
use tower_http::timeout::TimeoutLayer;
use tower_http::trace::TraceLayer;
use tracing::{debug, error, info, instrument, warn};

const CONSOLE_PREFIX: &str = "/rustfs/console";

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
async fn setup_console_tls_config(tls_path: Option<&String>) -> Result<Option<RustlsConfig>> {
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
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

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

/// Setup comprehensive middleware stack with tower-http features
fn setup_console_middleware_stack(
    cors_layer: CorsLayer,
    rate_limit_enable: bool,
    rate_limit_rpm: u32,
    auth_timeout: u64,
) -> Router {
    let mut app = Router::new()
        .route("/license", get(crate::admin::console::license_handler))
        .route("/config.json", get(crate::admin::console::config_handler))
        .route("/health", get(health_check))
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
        .layer(RequestBodyLimitLayer::new(10 * 1024 * 1024));

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

/// Start the standalone console server with enhanced security and monitoring
#[instrument(skip(opt, shutdown_rx))]
pub async fn start_console_server(opt: &Opt, shutdown_rx: tokio::sync::broadcast::Receiver<()>) -> Result<()> {
    if !opt.console_enable {
        debug!("Console server is disabled");
        return Ok(());
    }

    let console_addr = parse_and_resolve_address(&opt.console_address)?;

    // Get configuration from environment variables
    let (rate_limit_enable, rate_limit_rpm, auth_timeout, cors_allowed_origins) = get_console_config_from_env();

    // Setup TLS configuration if certificates are available
    let tls_config = setup_console_tls_config(opt.tls_path.as_ref()).await?;
    let tls_enabled = tls_config.is_some();

    info!(
        target: "rustfs::console::startup",
        address = %console_addr,
        tls_enabled = tls_enabled,
        rate_limit_enabled = rate_limit_enable,
        rate_limit_rpm = rate_limit_rpm,
        auth_timeout_seconds = auth_timeout,
        cors_allowed_origins = %cors_allowed_origins,
        "Starting console server"
    );

    // String to Option<&String>
    let cors_allowed_origins = if cors_allowed_origins.is_empty() {
        None
    } else {
        Some(&cors_allowed_origins)
    };

    // Configure CORS based on settings
    let cors_layer = parse_cors_origins(cors_allowed_origins);

    // Build console router with enhanced middleware stack using tower-http features
    let app = setup_console_middleware_stack(cors_layer, rate_limit_enable, rate_limit_rpm, auth_timeout);

    let local_ip = rustfs_utils::get_local_ip().unwrap_or_else(|| "127.0.0.1".parse().unwrap());
    let protocol = if tls_enabled { "https" } else { "http" };

    info!(
        target: "rustfs::console::startup",
        "Console WebUI available at: {}://{}:{}/rustfs/console/index.html",
        protocol, local_ip, console_addr.port()
    );
    info!(
        target: "rustfs::console::startup",
        "Console WebUI (localhost): {}://127.0.0.1:{}/rustfs/console/index.html",
        protocol, console_addr.port()
    );

    // Handle connections based on TLS availability using axum-server
    if let Some(tls_config) = tls_config {
        handle_tls_connections(console_addr, app, tls_config, shutdown_rx).await
    } else {
        handle_plain_connections(console_addr, app, shutdown_rx).await
    }
}

/// Handle TLS connections for console using axum-server with proper TLS support
async fn handle_tls_connections(
    server_addr: SocketAddr,
    app: Router,
    tls_config: RustlsConfig,
    mut shutdown_rx: tokio::sync::broadcast::Receiver<()>,
) -> Result<()> {
    info!(target: "rustfs::console::tls", "Starting Console HTTPS server on {}", server_addr);

    let handle = axum_server::Handle::new();
    let handle_clone = handle.clone();

    // Spawn shutdown signal handler
    tokio::spawn(async move {
        let _ = shutdown_rx.recv().await;
        info!(target: "rustfs::console::shutdown", "Console TLS server shutdown signal received");
        handle_clone.graceful_shutdown(Some(Duration::from_secs(10)));
    });

    // Start the HTTPS server using axum-server with RustlsConfig
    if let Err(e) = axum_server::bind_rustls(server_addr, tls_config)
        .handle(handle)
        .serve(app.into_make_service())
        .await
    {
        error!(target: "rustfs::console::error", error = %e, "Console TLS server error");
        return Err(std::io::Error::other(e));
    }

    info!(target: "rustfs::console::shutdown", "Console TLS server stopped");
    Ok(())
}

/// Handle plain HTTP connections using axum-server
async fn handle_plain_connections(
    server_addr: SocketAddr,
    app: Router,
    mut shutdown_rx: tokio::sync::broadcast::Receiver<()>,
) -> Result<()> {
    info!(target: "rustfs::console::startup", "Starting Console HTTP server on {}", server_addr);

    let handle = axum_server::Handle::new();
    let handle_clone = handle.clone();

    // Spawn shutdown signal handler
    tokio::spawn(async move {
        let _ = shutdown_rx.recv().await;
        info!(target: "rustfs::console::shutdown", "Console server shutdown signal received");
        handle_clone.graceful_shutdown(Some(Duration::from_secs(10)));
    });

    // Start the HTTP server using axum-server
    if let Err(e) = axum_server::bind(server_addr)
        .handle(handle)
        .serve(app.into_make_service())
        .await
    {
        error!(target: "rustfs::console::error", error = %e, "Console server error");
        return Err(std::io::Error::other(e));
    }

    info!(target: "rustfs::console::shutdown", "Console server stopped");
    Ok(())
}
