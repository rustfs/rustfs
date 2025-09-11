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
use axum::{Router, response::Json, routing::get};
use http::{Method, header};
use rustfs_utils::net::parse_and_resolve_address;
use serde_json::json;
use std::io::Result;
use tokio::net::TcpListener;
use tower_http::cors::{AllowOrigin, Any, CorsLayer};
use tower_http::trace::TraceLayer;
use tracing::{debug, error, info, warn};

#[cfg(test)]
mod console_test;

const CONSOLE_PREFIX: &str = "/rustfs/console";

/// Console health check handler
async fn health_check() -> Json<serde_json::Value> {
    Json(json!({
        "status": "ok",
        "service": "rustfs-console",
        "timestamp": chrono::Utc::now().to_rfc3339()
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
            header::X_REQUESTED_WITH,
        ]);

    match origins {
        Some(origins_str) if origins_str == "*" => cors_layer.allow_origin(Any),
        Some(origins_str) => {
            let origins: Vec<&str> = origins_str.split(',').map(|s| s.trim()).collect();
            if origins.is_empty() {
                warn!("Empty CORS origins provided, using permissive CORS");
                cors_layer.allow_origin(Any)
            } else {
                let parsed_origins: Result<Vec<_>, _> = origins.into_iter().map(|origin| origin.parse()).collect();

                match parsed_origins {
                    Ok(valid_origins) => {
                        info!("Console CORS origins configured: {:?}", valid_origins);
                        cors_layer.allow_origin(AllowOrigin::list(valid_origins))
                    }
                    Err(e) => {
                        warn!("Invalid CORS origins provided ({}), using permissive CORS", e);
                        cors_layer.allow_origin(Any)
                    }
                }
            }
        }
        None => {
            debug!("No CORS origins configured for console, using permissive CORS");
            cors_layer.allow_origin(Any)
        }
    }
}

/// Start the standalone console server
pub async fn start_console_server(opt: &Opt, mut shutdown_rx: tokio::sync::broadcast::Receiver<()>) -> Result<()> {
    if !opt.console_enable {
        debug!("Console server is disabled");
        return Ok(());
    }

    let console_addr = parse_and_resolve_address(&opt.console_address)?;

    info!("Starting console server on: {}", console_addr);

    // Configure CORS based on settings
    let cors_layer = parse_cors_origins(opt.console_cors_allowed_origins.as_ref());

    // Build console router with health check
    let app = Router::new()
        .route("/health", get(health_check))
        .nest(CONSOLE_PREFIX, Router::new().fallback_service(get(static_handler)))
        .fallback_service(get(static_handler))
        .layer(cors_layer)
        .layer(TraceLayer::new_for_http());

    // Bind to the address
    let listener = TcpListener::bind(console_addr).await?;

    let local_ip = rustfs_utils::get_local_ip().unwrap_or_else(|| "127.0.0.1".parse().unwrap());
    info!(
        "   WebUI: http://{}:{}/rustfs/console/index.html http://127.0.0.1:{}/rustfs/console/index.html",
        local_ip,
        console_addr.port(),
        console_addr.port()
    );

    // Start the server with graceful shutdown
    let server = axum::serve(listener, app).with_graceful_shutdown(async move {
        let _ = shutdown_rx.recv().await;
        info!("Console server shutdown signal received");
    });

    if let Err(e) = server.await {
        error!("Console server error: {}", e);
        return Err(std::io::Error::other(e));
    }

    info!("Console server stopped");
    Ok(())
}
