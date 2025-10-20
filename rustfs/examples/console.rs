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

use axum::{
    Router,
    body::{Body, to_bytes},
    extract::Request,
    http::{StatusCode, Uri, header},
    response::{IntoResponse, Response},
    routing::{any, get, get_service, post},
};
use axum_server::tls_rustls::RustlsConfig;
use reqwest::Client as ReqwestClient;
use std::io::IsTerminal;
use std::net::SocketAddr;
// use std::sync::Arc;
use tower_http::services::ServeDir;
use tracing::{debug, error, instrument};
use tracing_subscriber::fmt::time::LocalTime;
// 假设使用 tracing for 日志

// 定义常量
const CONSOLE_PREFIX: &str = "/rustfs/console"; // console 路径前缀，不转发
const PROXY_TARGET: &str = "http://127.0.0.1:9000"; // 转发目标地址（内网）
const STATIC_DIR: &str = "rustfs/static"; // 静态文件目录，请替换为实际路径

// 初始化 tracing 日志
fn init_tracing() {
    use tracing_subscriber::{EnvFilter, fmt, prelude::*};

    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(
            tracing_subscriber::fmt::layer()
                .with_timer(LocalTime::rfc_3339())
                .with_target(true)
                .with_ansi(std::io::stdout().is_terminal())
                .with_thread_names(true)
                .with_thread_ids(true)
                .with_file(true)
                .with_line_number(true)
                .with_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("debug"))),
        )
        .with(EnvFilter::from_default_env())
        .init();
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 初始化日志
    init_tracing();

    // Make sure to use a modern encryption suite
    let _ = rustls::crypto::ring::default_provider().install_default();
    // 配置 TLS 证书：从 PEM 文件加载证书和私钥
    // 注意：请替换为实际的证书和密钥文件路径
    let _tls_config = RustlsConfig::from_pem_file("deploy/certs/rustfs_cert.pem", "deploy/certs/rustfs_key.pem").await?;

    // 监听地址：9001 端口，支持 IPv6 和 IPv4
    let addr: SocketAddr = "[::]:9001".parse()?;
    // let addr: SocketAddr = "0.0.0.0:9001".parse()?;
    println!("Listening on {}", addr);
    // 构建 Axum Router，包括路由和 fallback
    let app = build_router();

    // 使用 axum-server 启动 HTTPS 服务器
    match axum_server::bind(addr)
        .handle(axum_server::Handle::new())
        .serve(app.into_make_service())
        .await
    {
        Ok(_) => {
            tracing::info!("Server exited normally");
            println!("Server exited normally");
        }
        Err(e) => {
            error!("Server error: {}", e);
            println!("Server error: {}", e);
        }
    }
    tracing::info!("Server stopped");
    println!("Server stopped");
    Ok(())
}

// 构建 Router 函数：配置所有路由、处理程序和代理 fallback
fn build_router() -> Router {
    // 创建静态文件服务：使用 tower_http::ServeDir
    // 假设静态文件在 STATIC_DIR 目录下
    let static_service = ServeDir::new(STATIC_DIR).append_index_html_on_directories(true); // 如果目录，添加 index.html

    // 创建 Router
    let app = Router::new()
        // 首页路由：使用 ServeDir for GET，非 GET 返回 405
        .route("/", get_service(static_service.clone()))
        .route("/", post(proxy_handler))
        .route("/", any(method_not_allowed))
        // 现有 admin 路由：license，仅 GET，其他方法 405
        .route("/license", get(license_handler))
        .route("/license", any(method_not_allowed))
        // config.json 路由：仅 GET，其他 405
        .route("/config.json", get(config_handler))
        .route("/config.json", any(method_not_allowed))
        // health 检查路由：仅 GET，其他 405
        .route("/health", get(health_check))
        .route("/health", any(method_not_allowed))
        // 嵌套 console 前缀路由：fallback 使用 ServeDir for GET，其他方法 405
        .nest_service(CONSOLE_PREFIX, get_service(static_service.clone()))
        // .nest(CONSOLE_PREFIX, Router::new().fallback_service(get(static_service)))
        // .route(&format!("{}/*path", CONSOLE_PREFIX), any(method_not_allowed))
        // 全局 fallback：所有未匹配路径和方法，转发到 9000 端口
        .fallback(any(proxy_handler));

    app
}

#[instrument]
async fn license_handler() -> &'static str {
    "license"
}

#[instrument]
async fn config_handler() -> &'static str {
    let json_config = r#"
{
  "api": {
    "base_url": "http://127.0.0.1:9001/rustfs/admin/v3"
  },
  "s3": {
    "endpoint": "http://127.0.0.1:9001",
    "region": "cn-east-1"
  },
  "release": {
    "version": "@v1.0.0",
    "date": "2024-01-01"
  },
  "license": {
    "name": "Apache-2.0",
    "url": "https://www.apache.org/licenses/LICENSE-2.0"
  },
  "doc": "https://rustfs.com/docs/"
}
    "#;
    json_config
}

// 方法不允许的处理程序：返回 405 状态码，并日志
#[instrument]
async fn method_not_allowed(req: Request) -> Response {
    error!("Method not allowed: {} {}", req.method(), req.uri());
    (StatusCode::METHOD_NOT_ALLOWED, "Method not allowed").into_response()
}

// 代理处理程序：使用 reqwest 将请求转发到 9000 端口
// 支持所有 HTTP 方法，包括 GET、POST、PUT、DELETE 等
// 增强错误处理和日志
#[instrument(skip(req))]
async fn proxy_handler(req: Request) -> Response {
    // 创建 reqwest Client：每次 new，或使用 Arc<Client>全局
    // 为简单，每次创建；如需优化，用 static Arc::new(ReqwestClient::new())
    let client = match ReqwestClient::builder().build() {
        Ok(c) => c,
        Err(e) => {
            error!("Failed to create reqwest client: {}", e);
            return (StatusCode::INTERNAL_SERVER_ERROR, "Failed to create proxy client").into_response();
        }
    };

    // 构建目标 URL：PROXY_TARGET + 原始路径 + 查询参数
    let path_and_query = req.uri().path_and_query().map(|p| p.as_str()).unwrap_or("");
    let target_url = format!("{}{}", PROXY_TARGET, path_and_query);

    debug!("Proxying request to: {}", target_url);
    println!("Proxying request to: {}", target_url);

    // 构建 reqwest 请求
    let mut reqwest_req = client.request(req.method().clone(), target_url);

    // 复制 headers，但设置 Host 为目标
    let target_uri: Uri = PROXY_TARGET.parse().unwrap(); // 假设有效
    let host = target_uri
        .authority()
        .map(|a| a.as_str())
        .unwrap_or("127.0.0.1:9000")
        .to_string();

    for (key, value) in req.headers() {
        if key != header::HOST {
            reqwest_req = reqwest_req.header(key.as_str(), value.as_bytes());
            println!("Proxying header: {}: {:?}", key.as_str(), value);
        }
    }
    reqwest_req = reqwest_req.header(header::HOST.as_str(), host);

    // 动态获取 body 长度的策略：
    // 1) 如果存在 Content-Length 头，优先使用该值（无需一次性读取）
    // 2) 否则异步读取完整 body（to_bytes）并使用其 len()
    // 3) 若需流式处理可遍历 req.into_body() 的 chunk 并累加长度（示例注释）
    let content_length = req
        .headers()
        .get(header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<usize>().ok());

    let body_len = content_length.unwrap_or_else(|| usize::MAX);
    debug!("Request body length determined: {}", body_len);
    println!("Request body length determined: {}", body_len);
    // 处理 body：从 axum Request 取出 body
    let body_bytes = match to_bytes(req.into_body(), body_len).await {
        Ok(b) => b,
        Err(e) => {
            error!("Failed to read request body: {}", e);
            println!("Failed to read request body: {}", e);
            return (StatusCode::BAD_REQUEST, "Failed to read body").into_response();
        }
    };

    reqwest_req = reqwest_req.body(body_bytes.to_vec());
    println!(
        "Proxying request body of length: {}, content: {}",
        body_bytes.len(),
        String::from_utf8_lossy(&body_bytes)
    );
    // 发送请求
    let response = match reqwest_req.send().await {
        Ok(resp) => resp,
        Err(e) => {
            error!("Proxy request failed: {}", e);
            println!("Proxy request failed: {}", e);
            return (StatusCode::BAD_GATEWAY, format!("Proxy error: {}", e)).into_response();
        }
    };
    println!("Received response with status: {}", response.status());
    // 构建 axum Response
    let mut axum_resp = Response::builder().status(response.status());

    // 复制 response headers
    for (key, value) in response.headers() {
        axum_resp = axum_resp.header(key.clone(), value.clone());
        println!("Proxying response header: {}: {:?}", key.as_str(), value);
    }
    println!("Finished proxying headers. body follows...");
    // 获取 response body
    let resp_body = match response.bytes().await {
        Ok(b) => Body::from(b),
        Err(e) => {
            error!("Failed to read proxy response body: {}", e);
            println!("Failed to read proxy response body: {}", e);
            return (StatusCode::BAD_GATEWAY, "Failed to read response body").into_response();
        }
    };
    println!("Finished proxy response body. building response.");
    match axum_resp.body(resp_body) {
        Ok(r) => r,
        Err(e) => {
            error!("Failed to build response: {}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, "Failed to build response").into_response()
        }
    }
}

// 健康检查处理程序：简单返回"OK"
#[instrument]
async fn health_check() -> &'static str {
    "OK"
}

// 注意：
// 1. 添加 tower-http 依赖到 Cargo.toml: tower-http = { version = "0.5", features = ["fs"] }
// 2. 添加 reqwest 依赖：reqwest = { version = "0.12", features = ["default-tls"] }
// 3. 添加 tracing 依赖：tracing = "0.1", tracing-subscriber = "0.3"
// 4. 静态目录：替换 STATIC_DIR 为实际路径，如 "static/"
// 5. body 处理：使用 to_bytes 读取整个 body，因为 reqwest body 需要 owned data。如果 body 很大，可考虑 streaming，但 reqwest 支持 reqwest::Body::from(hyper::Body)，但为兼容，用 bytes。
//    为 streaming，可用 hyper to reqwest adapter，但简单用 bytes。
// 6. 错误日志：使用 tracing 的 error! 宏记录错误，debug for info。
// 7. instrument：使用 tracing::instrument for span 日志。
// 8. 如果需要全局 client：use once_cell::sync::Lazy; static CLIENT: Lazy<ReqwestClient> = Lazy::new(|| ReqwestClient::new());
//    然后用 &*CLIENT
