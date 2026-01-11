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

//! Tower service implementation for trusted proxy middleware

use std::sync::Arc;
use std::task::{ready, Context, Poll};

use axum::extract::Request;
use axum::response::Response;
use tower::Service;
use tracing::{debug, instrument, Span};

use crate::error::ProxyError;
use crate::middleware::layer::TrustedProxyLayer;
use crate::proxy::{ClientInfo, ProxyValidator};

/// 可信代理中间件服务
#[derive(Clone)]
pub struct TrustedProxyMiddleware<S> {
    /// 内部服务
    inner: S,
    /// 代理验证器
    validator: Arc<ProxyValidator>,
    /// 是否启用中间件
    enabled: bool,
}

impl<S> TrustedProxyMiddleware<S> {
    /// 创建新的中间件服务
    pub fn new(inner: S, validator: Arc<ProxyValidator>, enabled: bool) -> Self {
        Self {
            inner,
            validator,
            enabled,
        }
    }

    /// 从层创建中间件服务
    pub fn from_layer(inner: S, layer: &TrustedProxyLayer) -> Self {
        Self::new(inner, layer.validator.clone(), layer.enabled)
    }
}

impl<S> Service<Request> for TrustedProxyMiddleware<S>
where
    S: Service<Request, Response = Response> + Clone + Send + 'static,
    S::Future: Send,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    #[instrument(
        name = "trusted_proxy_middleware",
        skip_all,
        fields(
            http.method = %req.method(),
            http.uri = %req.uri(),
            http.version = ?req.version(),
            enabled = self.enabled,
        )
    )]
    fn call(&mut self, mut req: Request) -> Self::Future {
        let span = Span::current();

        // 如果中间件未启用，直接传递请求
        if !self.enabled {
            debug!("Trusted proxy middleware is disabled");
            return self.inner.call(req);
        }

        // 记录请求开始时间
        let start_time = std::time::Instant::now();

        // 提取对端地址
        let peer_addr = req.extensions().get::<std::net::SocketAddr>().copied();

        // 为 span 添加字段
        if let Some(addr) = peer_addr {
            span.record("peer.addr", addr.to_string());
        }

        // 验证请求并提取客户端信息
        match self.validator.validate_request(peer_addr, req.headers()) {
            Ok(client_info) => {
                // 记录客户端信息到 span
                span.record("client.ip", client_info.real_ip.to_string());
                span.record("client.trusted", client_info.is_from_trusted_proxy);
                span.record("client.hops", client_info.proxy_hops as i64);

                // 将客户端信息存入请求扩展
                req.extensions_mut().insert(client_info);

                // 记录验证成功
                let duration = start_time.elapsed();
                debug!("Proxy validation successful in {:?}", duration);
            }
            Err(err) => {
                // 记录验证失败
                span.record("error", true);
                span.record("error.message", err.to_string());

                // 如果是可恢复的错误，创建默认的客户端信息
                if err.is_recoverable() {
                    let client_info = ClientInfo::direct(
                        peer_addr.unwrap_or_else(|| std::net::SocketAddr::new(std::net::IpAddr::from([0, 0, 0, 0]), 0)),
                    );
                    req.extensions_mut().insert(client_info);
                } else {
                    // 对于不可恢复的错误，记录警告
                    debug!("Unrecoverable proxy validation error: {}", err);
                }
            }
        }

        // 调用内部服务
        self.inner.call(req)
    }
}
