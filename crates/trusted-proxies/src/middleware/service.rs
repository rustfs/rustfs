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
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tower::Service;
use tracing::debug;

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
    S::Future: Send + 'static,
    ReqBody: Send + 'static,
    S::Response: 'static,
    S::Error: 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut req: Request<ReqBody>) -> Self::Future {
        let mut inner = self.inner.clone();
        let validator = self.validator.clone();
        let enabled = self.enabled;

        // If the middleware is disabled, pass the request through immediately.
        if !enabled {
            debug!("Trusted proxy middleware is disabled");
            return Box::pin(async move { inner.call(req).await });
        }

        Box::pin(async move {
            let start_time = std::time::Instant::now();

            // Extract the direct peer address from the request extensions.
            let peer_addr = req.extensions().get::<std::net::SocketAddr>().copied();

            // Validate the request and extract client information.
            match validator.validate_request(peer_addr, req.headers()).await {
                Ok(client_info) => {
                    // Insert the verified client info into the request extensions.
                    req.extensions_mut().insert(client_info);

                    let duration = start_time.elapsed();
                    debug!("Proxy validation successful in {:?}", duration);
                }
                Err(err) => {
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
            inner.call(req).await
        })
    }
}
