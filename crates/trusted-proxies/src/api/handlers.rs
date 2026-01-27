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

//! API request handlers for the trusted proxy service.

use crate::AppState;
use crate::error::AppError;
use crate::middleware::ClientInfo;
use axum::{
    extract::{Request, State},
    http::StatusCode,
    response::{IntoResponse, Json},
};
use serde_json::{Value, json};

/// Health check endpoint to verify service availability.
#[allow(dead_code)]
pub async fn health_check() -> impl IntoResponse {
    Json(json!({
        "status": "healthy",
        "timestamp": jiff::Timestamp::now().to_string(),
        "service": "trusted-proxy",
        "version": env!("CARGO_PKG_VERSION"),
    }))
}

/// Returns the current application configuration.
#[allow(dead_code)]
pub async fn show_config(State(state): State<AppState>) -> Result<Json<Value>, AppError> {
    let config = &state.config;

    let response = json!({
        "server": {
            "addr": config.server_addr.to_string(),
        },
        "proxy": {
            "trusted_networks_count": config.proxy.proxies.len(),
            "validation_mode": config.proxy.validation_mode.as_str(),
            "max_hops": config.proxy.max_hops,
            "enable_rfc7239": config.proxy.enable_rfc7239,
        },
        "cache": {
            "capacity": config.cache.capacity,
            "ttl_seconds": config.cache.ttl_seconds,
        },
        "monitoring": {
            "metrics_enabled": config.monitoring.metrics_enabled,
            "log_level": config.monitoring.log_level,
        },
        "cloud": {
            "metadata_enabled": config.cloud.metadata_enabled,
            "cloudflare_enabled": config.cloud.cloudflare_ips_enabled,
        },
    });

    Ok(Json(response))
}

/// Returns information about the client as identified by the trusted proxy middleware.
#[allow(dead_code)]
pub async fn client_info(State(_state): State<AppState>, req: Request) -> impl IntoResponse {
    // Retrieve the verified client information from the request extensions.
    let client_info = req.extensions().get::<ClientInfo>();

    match client_info {
        Some(info) => {
            let response = json!({
                "client": {
                    "real_ip": info.real_ip.to_string(),
                    "is_from_trusted_proxy": info.is_from_trusted_proxy,
                    "proxy_hops": info.proxy_hops,
                    "validation_mode": info.validation_mode.as_str(),
                },
                "headers": {
                    "forwarded_host": info.forwarded_host,
                    "forwarded_proto": info.forwarded_proto,
                },
                "warnings": info.warnings,
                "timestamp": jiff::Timestamp::now().to_string(),
            });

            Json(response).into_response()
        }
        None => {
            let response = json!({
                "error": "Client information not available",
                "message": "The trusted proxy middleware may not be enabled or configured correctly.",
            });

            (StatusCode::INTERNAL_SERVER_ERROR, Json(response)).into_response()
        }
    }
}

/// Debugging endpoint that returns all proxy-related headers received in the request.
#[allow(dead_code)]
pub async fn proxy_test(req: Request) -> Json<Value> {
    // Collect all headers related to proxying.
    let headers: Vec<(String, String)> = req
        .headers()
        .iter()
        .filter(|(name, _)| {
            let name_str = name.as_str().to_lowercase();
            name_str.contains("forwarded") || name_str.contains("x-forwarded") || name_str.contains("x-real")
        })
        .map(|(name, value)| (name.to_string(), value.to_str().unwrap_or("[INVALID]").to_string()))
        .collect();

    // Get the direct peer address.
    let peer_addr = req
        .extensions()
        .get::<std::net::SocketAddr>()
        .map(|addr| addr.to_string())
        .unwrap_or_else(|| "unknown".to_string());

    Json(json!({
        "peer_addr": peer_addr,
        "method": req.method().to_string(),
        "uri": req.uri().to_string(),
        "proxy_headers": headers,
        "timestamp": jiff::Timestamp::now().to_string(),
    }))
}

/// Endpoint for retrieving Prometheus metrics.
#[allow(dead_code)]
pub async fn metrics(State(state): State<AppState>) -> impl IntoResponse {
    if !state.config.monitoring.metrics_enabled {
        return (StatusCode::NOT_FOUND, "Metrics are not enabled").into_response();
    }

    // In a production environment, this would return the actual Prometheus-formatted metrics.
    let metrics_summary = json!({
        "status": "metrics_enabled",
        "note": "Prometheus metrics are being collected. Use a compatible exporter to view them.",
    });

    Json(metrics_summary).into_response()
}
