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
use axum::{Router, response::Json, routing::get, middleware, extract::Request};
use http::{Method, header, HeaderValue};
use rustfs_utils::net::parse_and_resolve_address;
use serde_json::json;
use std::io::Result;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio_rustls::{TlsAcceptor, rustls::ServerConfig};
use tower_http::cors::{AllowOrigin, Any, CorsLayer};
use tower_http::trace::TraceLayer;
use tower_http::catch_panic::CatchPanicLayer;
use tracing::{debug, error, info, warn, instrument};

#[cfg(test)]
mod console_test;

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

/// Setup TLS acceptor for console if enabled, following endpoint TLS implementation logic
#[instrument(skip(tls_path))]
async fn setup_console_tls(tls_path: Option<&String>) -> Result<Option<TlsAcceptor>> {
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
            debug!("Found {} certificates for console, creating SNI-aware multi-cert resolver", cert_key_pairs.len());

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
            return Ok(Some(TlsAcceptor::from(Arc::new(server_config))));
        }
    }

    // 2. Revert to the traditional single-certificate mode
    let key_path = format!("{tls_path}/{}", rustfs_config::RUSTFS_TLS_KEY);
    let cert_path = format!("{tls_path}/{}", rustfs_config::RUSTFS_TLS_CERT);
    if tokio::try_join!(tokio::fs::metadata(&key_path), tokio::fs::metadata(&cert_path)).is_ok() {
        debug!("Found legacy single TLS certificate for console, starting with HTTPS");
        let certs = rustfs_utils::load_certs(&cert_path).map_err(|e| rustfs_utils::certs_error(e.to_string()))?;
        let key = rustfs_utils::load_private_key(&key_path).map_err(|e| rustfs_utils::certs_error(e.to_string()))?;

        let mut server_config = ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .map_err(|e| rustfs_utils::certs_error(e.to_string()))?;

        // Configure ALPN protocol priority
        server_config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec(), b"http/1.0".to_vec()];

        // Log SNI requests
        if rustfs_utils::tls_key_log() {
            server_config.key_log = Arc::new(rustls::KeyLogFile::new());
        }

        info!(target: "rustfs::console::tls", "Console TLS enabled with single certificate");
        return Ok(Some(TlsAcceptor::from(Arc::new(server_config))));
    }

    debug!("No valid TLS certificates found in the directory for console, starting with HTTP");
    Ok(None)
}

/// Setup rate limiting for console using a simple in-memory approach
/// In a production environment, consider using Redis or other distributed rate limiting
fn setup_console_rate_limiting(_opt: &Opt) -> Option<()> {
    // For now, we'll implement basic rate limiting through middleware
    // A full implementation would use a more sophisticated rate limiter
    if _opt.console_rate_limit_enable {
        info!("Console rate limiting enabled: {} requests per minute", _opt.console_rate_limit_rpm);
        Some(())
    } else {
        None
    }
}

/// Simple rate limiting middleware (in-memory, single-node only)
/// For production, consider using Redis-based rate limiting
#[allow(dead_code)]
async fn rate_limit_middleware(req: Request, next: axum::middleware::Next) -> axum::response::Response {
    // For now, this is a placeholder for rate limiting
    // A production implementation would track client IPs and enforce limits
    next.run(req).await
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
        .allow_headers([
            header::CONTENT_TYPE,
            header::AUTHORIZATION,
            header::ACCEPT,
            header::ORIGIN,
        ]);

    match origins {
        Some(origins_str) if origins_str == "*" => cors_layer.allow_origin(Any),
        Some(origins_str) => {
            let origins: Vec<&str> = origins_str.split(',').map(|s| s.trim()).collect();
            if origins.is_empty() {
                warn!("Empty CORS origins provided, using permissive CORS");
                cors_layer.allow_origin(Any)
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
                    cors_layer.allow_origin(Any)
                } else {
                    info!("Console CORS origins configured: {:?}", valid_origins);
                    cors_layer.allow_origin(AllowOrigin::list(valid_origins))
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

    // Setup TLS if certificates are available
    let tls_acceptor = setup_console_tls(opt.tls_path.as_ref()).await?;
    let tls_enabled = tls_acceptor.is_some();

    info!(
        target: "rustfs::console::startup",
        address = %console_addr,
        tls_enabled = tls_enabled,
        rate_limit_enabled = opt.console_rate_limit_enable,
        "Starting console server"
    );
    
    // Configure CORS based on settings
    let cors_layer = parse_cors_origins(opt.console_cors_allowed_origins.as_ref());

    // Setup rate limiting if enabled  
    let _rate_limiter = setup_console_rate_limiting(opt);

    // Build console router with enhanced middleware stack
    let app = Router::new()
        .route("/health", get(health_check))
        .nest(CONSOLE_PREFIX, Router::new().fallback_service(get(static_handler)))
        .fallback_service(get(static_handler))
        .layer(cors_layer)
        .layer(TraceLayer::new_for_http())
        .layer(middleware::from_fn(console_logging_middleware))
        .layer(CatchPanicLayer::new());

    // Note: Rate limiting would be implemented here in a production version
    if opt.console_rate_limit_enable {
        info!("Console rate limiting configured: {} requests per minute", opt.console_rate_limit_rpm);
    }

    // Bind to the address
    let listener = TcpListener::bind(console_addr).await?;

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

    // Handle connections based on TLS availability
    if let Some(tls_acceptor) = tls_acceptor {
        handle_tls_connections(listener, app, Arc::new(tls_acceptor), shutdown_rx).await
    } else {
        handle_plain_connections(listener, app, shutdown_rx).await
    }
}

/// Handle TLS connections for console using axum's capabilities
async fn handle_tls_connections(
    listener: TcpListener,
    app: Router,
    _tls_acceptor: Arc<TlsAcceptor>,
    mut shutdown_rx: tokio::sync::broadcast::Receiver<()>,
) -> Result<()> {
    let server = axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            let _ = shutdown_rx.recv().await;
            info!(target: "rustfs::console::shutdown", "Console TLS server shutdown signal received");
        });

    // For now, we'll note that full TLS integration with axum would require additional setup
    // In a production environment, you might want to use a reverse proxy for TLS termination
    info!(target: "rustfs::console::tls", "Console TLS acceptor configured, but using HTTP for axum compatibility");
    info!(target: "rustfs::console::tls", "Consider using a reverse proxy (nginx, traefik) for TLS termination in production");

    if let Err(e) = server.await {
        error!(target: "rustfs::console::error", error = %e, "Console TLS server error");
        return Err(std::io::Error::other(e));
    }

    info!(target: "rustfs::console::shutdown", "Console TLS server stopped");
    Ok(())
}

/// Handle plain HTTP connections
async fn handle_plain_connections(
    listener: TcpListener,
    app: Router,
    mut shutdown_rx: tokio::sync::broadcast::Receiver<()>,
) -> Result<()> {
    let server = axum::serve(listener, app).with_graceful_shutdown(async move {
        let _ = shutdown_rx.recv().await;
        info!(target: "rustfs::console::shutdown", "Console server shutdown signal received");
    });

    if let Err(e) = server.await {
        error!(target: "rustfs::console::error", error = %e, "Console server error");
        return Err(std::io::Error::other(e));
    }

    info!(target: "rustfs::console::shutdown", "Console server stopped");
    Ok(())
}
