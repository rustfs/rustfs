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

//! API request handlers

use axum::{
    extract::{Request, State},
    http::StatusCode,
    response::{IntoResponse, Json},
};
use serde_json::{json, Value};

use crate::error::AppError;
use crate::middleware::ClientInfo;
use crate::AppState;

/// 健康检查端点
pub async fn health_check() -> impl IntoResponse {
    Json(json!({
        "status": "healthy",
        "timestamp": chrono::Utc::now().to_rfc3339(),
        "service": "trusted-proxy",
        "version": env!("CARGO_PKG_VERSION"),
    }))
}

/// 显示配置信息
pub async fn show_config(State(state): State<AppState>) -> Result<Json<Value>, AppError> {
    let config = &state.config;

    let response = json!({
        "server": {
            "addr": config.server_addr.to_string(),
        },
        "proxy": {
            "trusted_networks_count": config.proxy.proxies.len(),
            "validation_mode": format!("{:?}", config.proxy.validation_mode),
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

/// 显示客户端信息
pub async fn client_info(State(state): State<AppState>, req: Request) -> impl IntoResponse {
    // 从请求扩展中获取客户端信息
    let client_info = req.extensions().get::<ClientInfo>();

    match client_info {
        Some(info) => {
            let response = json!({
                "client": {
                    "real_ip": info.real_ip.to_string(),
                    "is_from_trusted_proxy": info.is_from_trusted_proxy,
                    "proxy_hops": info.proxy_hops,
                    "validation_mode": format!("{:?}", info.validation_mode),
                },
                "headers": {
                    "forwarded_host": info.forwarded_host,
                    "forwarded_proto": info.forwarded_proto,
                },
                "warnings": info.warnings,
                "timestamp": chrono::Utc::now().to_rfc3339(),
            });

            Json(response).into_response()
        }
        None => {
            let response = json!({
                "error": "Client information not available",
                "message": "The trusted proxy middleware may not be enabled or configured correctly",
            });

            (StatusCode::INTERNAL_SERVER_ERROR, Json(response)).into_response()
        }
    }
}

/// 代理测试端点（用于测试代理头部）
pub async fn proxy_test(req: Request) -> Json<Value> {
    // 收集所有代理相关的头部
    let headers: Vec<(String, String)> = req
        .headers()
        .iter()
        .filter(|(name, _)| {
            let name_str = name.as_str().to_lowercase();
            name_str.contains("forwarded") || name_str.contains("x-forwarded") || name_str.contains("x-real")
        })
        .map(|(name, value)| (name.to_string(), value.to_str().unwrap_or("[INVALID]").to_string()))
        .collect();

    // 获取对端地址
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
        "timestamp": chrono::Utc::now().to_rfc3339(),
    }))
}

/// 指标端点（Prometheus 格式）
pub async fn metrics(State(state): State<AppState>) -> impl IntoResponse {
    if !state.config.monitoring.metrics_enabled {
        return (StatusCode::NOT_FOUND, "Metrics are not enabled".to_string()).into_response();
    }

    // 在实际应用中，这里应该返回 Prometheus 格式的指标
    // 这里返回简单的 JSON 作为示例
    let metrics = json!({
        "message": "Metrics endpoint",
        "note": "In a real implementation, this would return Prometheus format metrics",
        "status": "metrics_enabled",
    });

    Json(metrics).into_response()
}
