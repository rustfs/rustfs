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
use axum::{Router, routing::get};
use http::{Method, header};
use rustfs_utils::net::parse_and_resolve_address;
use std::io::Result;
use tokio::net::TcpListener;
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;
use tracing::{debug, error, info};

const CONSOLE_PREFIX: &str = "/rustfs/console";

/// Start the standalone console server
pub async fn start_console_server(opt: &Opt, mut shutdown_rx: tokio::sync::broadcast::Receiver<()>) -> Result<()> {
    if !opt.console_enable {
        debug!("Console server is disabled");
        return Ok(());
    }

    let console_addr = parse_and_resolve_address(&opt.console_address)?;

    info!("Starting console server on: {}", console_addr);

    // Build console router
    let app = Router::new()
        .nest(CONSOLE_PREFIX, Router::new().fallback_service(get(static_handler)))
        .fallback_service(get(static_handler))
        .layer(
            CorsLayer::new()
                .allow_origin(tower_http::cors::Any)
                .allow_methods([Method::GET, Method::POST, Method::PUT, Method::DELETE])
                .allow_headers([header::CONTENT_TYPE, header::AUTHORIZATION]),
        )
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
