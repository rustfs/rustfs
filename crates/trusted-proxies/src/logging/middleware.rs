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

//! Logging middleware for the Axum web framework.

use crate::logging::Logger;
use std::task::{Context, Poll};
use std::time::Instant;
use tower::Service;
use uuid::Uuid;

/// Tower Layer for request logging middleware.
#[derive(Clone)]
pub struct RequestLoggingLayer {
    logger: Logger,
}

impl RequestLoggingLayer {
    /// Creates a new `RequestLoggingLayer`.
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

/// Tower Service for request logging middleware.
#[derive(Clone)]
pub struct RequestLoggingMiddleware<S> {
    inner: S,
    logger: Logger,
}

impl<S> Service<axum::extract::Request> for RequestLoggingMiddleware<S>
where
    S: Service<axum::extract::Request, Response = axum::response::Response> + Clone + Send + 'static,
    S::Future: Send,
    S::Error: std::error::Error + Send + Sync + 'static,
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
            // Generate a unique request ID for correlation.
            let request_id = Uuid::new_v4().to_string();

            let start_time = Instant::now();
            logger.log_request(&req, &request_id);

            // Inject the request ID into the request extensions.
            let mut req = req;
            req.extensions_mut().insert(RequestId(request_id.clone()));

            // Process the request.
            let result = inner.call(req).await;

            let duration = start_time.elapsed();

            // Log the response or error.
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

/// Wrapper for a unique request ID.
#[derive(Debug, Clone)]
pub struct RequestId(String);

impl RequestId {
    /// Returns the request ID as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for RequestId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Middleware specifically for logging proxy-related information.
#[derive(Clone)]
pub struct ProxyLoggingMiddleware<S> {
    inner: S,
    logger: Logger,
}

impl<S> ProxyLoggingMiddleware<S> {
    /// Creates a new `ProxyLoggingMiddleware`.
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

    fn call(&mut self, req: axum::extract::Request) -> Self::Future {
        // Log proxy-specific details if available.
        let peer_addr = req.extensions().get::<std::net::SocketAddr>().copied();
        let client_info = req.extensions().get::<crate::middleware::ClientInfo>();

        if let (Some(addr), Some(info)) = (peer_addr, client_info) {
            self.logger
                .log_info(&format!("Proxy request from {}: {}", addr, info.to_log_string()), None);

            // Log any warnings generated during proxy validation.
            if !info.warnings.is_empty() {
                for warning in &info.warnings {
                    self.logger.log_warning(warning, Some("proxy_validation"));
                }
            }
        }

        self.inner.call(req)
    }
}

/// Tower Layer for proxy logging middleware.
#[derive(Clone)]
pub struct ProxyLoggingLayer {
    logger: Logger,
}

impl ProxyLoggingLayer {
    /// Creates a new `ProxyLoggingLayer`.
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
