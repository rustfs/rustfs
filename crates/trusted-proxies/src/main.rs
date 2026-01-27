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

//! Main application entry point for the RustFS Trusted Proxies service.

use axum::{routing::get, Router};
use std::sync::Arc;
use tokio::net::TcpListener;
use tracing::{info, Level};
use tracing_subscriber::EnvFilter;

mod api;
mod cloud;
mod config;
mod error;
mod middleware;
mod proxy;
mod state;
mod utils;

use api::handlers;
use config::{AppConfig, ConfigLoader, MonitoringConfig};
use error::AppError;
use middleware::TrustedProxyLayer;
use proxy::metrics::default_proxy_metrics;
use state::AppState;

#[tokio::main]
async fn main() -> Result<(), AppError> {
    // Load environment variables from .env file if present.
    dotenvy::dotenv().ok();

    // Load application configuration from environment variables.
    let config = ConfigLoader::from_env_or_default();

    // Initialize the logging system.
    init_logging(&config.monitoring)?;

    // Print a summary of the loaded configuration.
    ConfigLoader::print_summary(&config);

    // Initialize metrics collector if enabled.
    let metrics = if config.monitoring.metrics_enabled {
        let m = default_proxy_metrics(true);
        m.print_summary();
        Some(m)
    } else {
        None
    };

    // Create shared application state.
    let state = AppState {
        config: Arc::new(config.clone()),
        metrics: metrics.clone(),
    };

    // Initialize the trusted proxy middleware layer.
    let proxy_layer = TrustedProxyLayer::enabled(config.proxy.clone(), metrics);

    // Build the Axum application router.
    let app = Router::new()
        .route("/health", get(handlers::health_check))
        .route("/config", get(handlers::show_config))
        .route("/client-info", get(handlers::client_info))
        .route("/proxy-test", get(handlers::proxy_test))
        .route("/metrics", get(handlers::metrics))
        .with_state(state)
        .layer(proxy_layer)
        .layer(tower_http::trace::TraceLayer::new_for_http())
        .layer(tower_http::cors::CorsLayer::permissive())
        .layer(tower_http::compression::CompressionLayer::new());

    // Bind the TCP listener and start the server.
    let addr = config.server_addr;
    let listener = TcpListener::bind(addr).await?;

    info!("Server listening on http://{}", addr);
    info!("Available endpoints:");
    info!("  GET /health      - Service health check");
    info!("  GET /config      - Current configuration summary");
    info!("  GET /client-info - Extracted client information");
    info!("  GET /proxy-test  - Debugging endpoint for proxy headers");
    info!("  GET /metrics     - Prometheus metrics (if enabled)");

    axum::serve(listener, app).await?;

    Ok(())
}

/// Initializes the tracing subscriber for logging.
fn init_logging(monitoring_config: &MonitoringConfig) -> Result<(), AppError> {
    let filter = EnvFilter::builder()
        .with_default_directive(monitoring_config.log_level.parse().unwrap_or(Level::INFO.into()))
        .from_env_lossy();

    let subscriber = tracing_subscriber::fmt().with_env_filter(filter);

    if monitoring_config.structured_logging {
        subscriber.json().init();
    } else {
        subscriber.init();
    }

    info!("Logging initialized with level: {}", monitoring_config.log_level);

    Ok(())
}
