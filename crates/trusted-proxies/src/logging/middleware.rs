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

//! Logging middleware for Axum

use std::task::{Context, Poll};
use std::time::Instant;
use tower::Service;
use uuid::Uuid;

use crate::logging::Logger;

/// 请求日志中间件层
#[derive(Clone)]
pub struct RequestLoggingLayer {
    logger: Logger,
}

impl RequestLoggingLayer {
    /// 创建新的日志中间件层
    pub fn new(logger: Logger) -> Self {
        Self { logger }
    }
}

impl<S> tower::Layer<S> for RequestLoggingLayer {
    type Service = RequestLoggingMiddleware<S>;

    fn layer(&self, inner: S) -> Self::Service {
        RequestLoggingMiddleware {
            inner,
            logger: self.logger.clone(),
        }
    }
}

/// 请求日志中间件服务
#[derive(Clone)]
pub struct RequestLoggingMiddleware<S> {
    inner: S,
    logger: Logger,
}

impl<S> Service<axum::extract::Request> for RequestLoggingMiddleware<S>
where
    S: Service<axum::extract::Request, Response = axum::response::Response> + Clone + Send + 'static,
    S::Future: Send,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = std::pin::Pin<Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: axum::extract::Request) -> Self::Future {
        let logger = self.logger.clone();
        let mut inner = self.inner.clone();

        Box::pin(async move {
            // 生成请求 ID
            let request_id = Uuid::new_v4().to_string();

            // 记录请求开始时间和日志
            let start_time = Instant::now();
            logger.log_request(&req, &request_id);

            // 将请求 ID 添加到请求扩展中
            let mut req = req;
            req.extensions_mut().insert(RequestId(request_id.clone()));

            // 处理请求
            let result = inner.call(req).await;

            // 计算处理时间
            let duration = start_time.elapsed();

            // 记录响应
            match &result {
                Ok(response) => {
                    logger.log_response(response, &request_id, duration);
                }
                Err(error) => {
                    logger.log_error(error, Some(&request_id));
                }
            }

            result
        })
    }
}

/// 请求 ID 包装器
#[derive(Debug, Clone)]
pub struct RequestId(String);

impl RequestId {
    /// 获取请求 ID
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for RequestId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// 代理特定的日志中间件
#[derive(Clone)]
pub struct ProxyLoggingMiddleware<S> {
    inner: S,
    logger: Logger,
}

impl<S> ProxyLoggingMiddleware<S> {
    /// 创建新的代理日志中间件
    pub fn new(inner: S, logger: Logger) -> Self {
        Self { inner, logger }
    }
}

impl<S> Service<axum::extract::Request> for ProxyLoggingMiddleware<S>
where
    S: Service<axum::extract::Request, Response = axum::response::Response> + Clone + Send + 'static,
    S::Future: Send,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut req: axum::extract::Request) -> Self::Future {
        // 记录代理相关信息
        let peer_addr = req.extensions().get::<std::net::SocketAddr>().copied();
        let client_info = req.extensions().get::<crate::middleware::ClientInfo>();

        if let (Some(addr), Some(info)) = (peer_addr, client_info) {
            self.logger
                .log_info(&format!("Proxy request from {}: {}", addr, info.to_log_string()), None);

            // 如果有警告，记录它们
            if !info.warnings.is_empty() {
                for warning in &info.warnings {
                    self.logger.log_warning(warning, Some("proxy_validation"));
                }
            }
        }

        self.inner.call(req)
    }
}

/// 代理日志中间件层
#[derive(Clone)]
pub struct ProxyLoggingLayer {
    logger: Logger,
}

impl ProxyLoggingLayer {
    /// 创建新的代理日志中间件层
    pub fn new(logger: Logger) -> Self {
        Self { logger }
    }
}

impl<S> tower::Layer<S> for ProxyLoggingLayer {
    type Service = ProxyLoggingMiddleware<S>;

    fn layer(&self, inner: S) -> Self::Service {
        ProxyLoggingMiddleware::new(inner, self.logger.clone())
    }
}
