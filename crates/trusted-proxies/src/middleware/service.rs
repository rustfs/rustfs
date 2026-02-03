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

//! Tower service implementation for the trusted proxy middleware.

use crate::{ClientInfo, ProxyValidator, TrustedProxyLayer};
use http::Request;
use std::sync::Arc;
use std::task::{Context, Poll};
use tower::Service;
use tracing::{Span, debug, instrument};

/// Tower Service for the trusted proxy middleware.
#[derive(Clone)]
pub struct TrustedProxyMiddleware<S> {
    /// The inner service being wrapped.
    pub(crate) inner: S,
    /// The validator used to verify proxy chains.
    pub(crate) validator: Arc<ProxyValidator>,
    /// Whether the middleware is enabled.
    pub(crate) enabled: bool,
}

impl<S> TrustedProxyMiddleware<S> {
    /// Creates a new `TrustedProxyMiddleware`.
    pub fn new(inner: S, validator: Arc<ProxyValidator>, enabled: bool) -> Self {
        Self {
            inner,
            validator,
            enabled,
        }
    }

    /// Creates a new `TrustedProxyMiddleware` from a `TrustedProxyLayer`.
    pub fn from_layer(inner: S, layer: &TrustedProxyLayer) -> Self {
        Self::new(inner, layer.validator.clone(), layer.enabled)
    }
}

impl<S, ReqBody> Service<Request<ReqBody>> for TrustedProxyMiddleware<S>
where
    S: Service<Request<ReqBody>> + Clone + Send + 'static,
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
            peer.addr = tracing::field::Empty,
            client.ip = tracing::field::Empty,
            client.trusted = tracing::field::Empty,
            client.hops = tracing::field::Empty,
            error = tracing::field::Empty,
            error.message = tracing::field::Empty,
        )
    )]
    fn call(&mut self, mut req: Request<ReqBody>) -> Self::Future {
        let span = Span::current();

        // If the middleware is disabled, pass the request through immediately.
        if !self.enabled {
            debug!("Trusted proxy middleware is disabled");
            return self.inner.call(req);
        }

        let start_time = std::time::Instant::now();

        // Extract the direct peer address from the request extensions.
        let peer_addr = req.extensions().get::<std::net::SocketAddr>().copied();

        if let Some(addr) = peer_addr {
            span.record("peer.addr", addr.to_string());
        }

        // Validate the request and extract client information.
        match self.validator.validate_request(peer_addr, req.headers()) {
            Ok(client_info) => {
                span.record("client.ip", client_info.real_ip.to_string());
                span.record("client.trusted", client_info.is_from_trusted_proxy);
                span.record("client.hops", client_info.proxy_hops as i64);

                // Insert the verified client info into the request extensions.
                req.extensions_mut().insert(client_info);

                let duration = start_time.elapsed();
                debug!("Proxy validation successful in {:?}", duration);
            }
            Err(err) => {
                span.record("error", true);
                span.record("error.message", err.to_string());

                // If the error is recoverable, fallback to a direct connection info.
                if err.is_recoverable() {
                    let client_info = ClientInfo::direct(
                        peer_addr.unwrap_or_else(|| std::net::SocketAddr::new(std::net::IpAddr::from([0, 0, 0, 0]), 0)),
                    );
                    req.extensions_mut().insert(client_info);
                } else {
                    debug!("Unrecoverable proxy validation error: {}", err);
                }
            }
        }

        // Call the inner service.
        self.inner.call(req)
    }
}
