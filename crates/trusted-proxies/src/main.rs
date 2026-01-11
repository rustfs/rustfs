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

//! Main application entry point for the trusted proxy system

use std::net::SocketAddr;
use std::sync::Arc;

use axum::{
    extract::State,
    response::{IntoResponse, Json},
    routing::get,
    Router,
};
use tokio::net::TcpListener;
use tracing::{error, info};

mod api;
mod cloud;
mod config;
mod error;
mod middleware;
mod proxy;
mod state;
mod utils;

use api::handlers;
use config::{AppConfig, ConfigLoader};
use error::AppError;
use middleware::TrustedProxyLayer;
use proxy::metrics::{default_proxy_metrics, ProxyMetrics};
use state::AppState;

#[tokio::main]
async fn main() -> Result<(), AppError> {
    // 加载环境变量
    dotenvy::dotenv().ok();

    // 从环境变量加载配置
    let config = ConfigLoader::from_env_or_default();

    // 初始化日志
    init_logging(&config.monitoring)?;

    // 打印配置摘要
    ConfigLoader::print_summary(&config);

    // 初始化指标收集器
    let metrics = if config.monitoring.metrics_enabled {
        let metrics = default_proxy_metrics(true);
        metrics.print_summary();
        Some(metrics)
    } else {
        None
    };

    // 创建应用状态
    let state = AppState {
        config: Arc::new(config),
        metrics: metrics.clone(),
    };

    // 创建可信代理中间件层
    let proxy_layer = TrustedProxyLayer::enabled(state.clone().config.proxy.clone(), metrics);

    // 创建路由
    let app = Router::new()
        // 健康检查端点
        .route("/health", get(handlers::health_check))
        // 配置查看端点
        .route("/config", get(handlers::show_config))
        // 客户端信息端点
        .route("/client-info", get(handlers::client_info))
        // 代理测试端点
        .route("/proxy-test", get(handlers::proxy_test))
        // 指标端点（如果启用）
        .route("/metrics", get(handlers::metrics))
        // 添加应用状态
        .with_state(state.clone())
        // 添加可信代理中间件
        .layer(proxy_layer)
        // 添加追踪中间件（如果启用）
        .layer(tower_http::trace::TraceLayer::new_for_http())
        // 添加 CORS 中间件
        .layer(tower_http::cors::CorsLayer::permissive())
        // 添加压缩中间件
        .layer(tower_http::compression::CompressionLayer::new());

    // 启动服务器
    let addr = state.config.server_addr;
    let listener = TcpListener::bind(addr).await.map_err(|e| AppError::Io(e))?;

    info!("Server listening on http://{}", addr);
    info!("Available endpoints:");
    info!("  GET /health      - Health check");
    info!("  GET /config      - Show configuration");
    info!("  GET /client-info - Show client information");
    info!("  GET /proxy-test  - Test proxy headers");
    info!("  GET /metrics     - Prometheus metrics (if enabled)");

    axum::serve(listener, app).await.map_err(|e| AppError::Io(e))?;

    Ok(())
}

/// 初始化日志系统
fn init_logging(monitoring_config: &config::MonitoringConfig) -> Result<(), AppError> {
    // 创建日志过滤器
    let filter = tracing_subscriber::EnvFilter::builder()
        .with_default_directive(monitoring_config.log_level.parse().unwrap_or(tracing::Level::INFO.into()))
        .from_env_lossy();

    // 根据配置选择日志格式
    if monitoring_config.structured_logging {
        // 结构化日志（JSON 格式）
        tracing_subscriber::fmt().json().with_env_filter(filter).init();
    } else {
        // 普通文本日志
        tracing_subscriber::fmt().with_env_filter(filter).init();
    }

    info!("Logging initialized with level: {}", monitoring_config.log_level);

    Ok(())
}
