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

use bytes::Bytes;
use http::{Request as HttpRequest, Response, StatusCode};
use http_body::Body;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use rustfs_common::GlobalReadiness;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tower::{Layer, Service};
use tracing::debug;

/// ReadinessGateLayer ensures that the system components (IAM, Storage)
/// are fully initialized before allowing any request to proceed.
#[derive(Clone)]
pub struct ReadinessGateLayer {
    readiness: Arc<GlobalReadiness>,
}

impl ReadinessGateLayer {
    /// Create a new ReadinessGateLayer
    /// # Arguments
    /// * `readiness` - An Arc to the GlobalReadiness instance
    ///
    /// # Returns
    /// A new instance of ReadinessGateLayer
    pub fn new(readiness: Arc<GlobalReadiness>) -> Self {
        Self { readiness }
    }
}

impl<S> Layer<S> for ReadinessGateLayer {
    type Service = ReadinessGateService<S>;

    /// Wrap the inner service with ReadinessGateService
    /// # Arguments
    /// * `inner` - The inner service to wrap
    /// # Returns
    /// An instance of ReadinessGateService
    fn layer(&self, inner: S) -> Self::Service {
        ReadinessGateService {
            inner,
            readiness: self.readiness.clone(),
        }
    }
}

#[derive(Clone)]
pub struct ReadinessGateService<S> {
    inner: S,
    readiness: Arc<GlobalReadiness>,
}

type BoxError = Box<dyn std::error::Error + Send + Sync>;
type BoxBody = http_body_util::combinators::UnsyncBoxBody<Bytes, BoxError>;
impl<S, B> Service<HttpRequest<Incoming>> for ReadinessGateService<S>
where
    S: Service<HttpRequest<Incoming>, Response = Response<B>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Send + 'static,
    B: Body<Data = Bytes> + Send + 'static,
    B::Error: Into<BoxError> + Send + 'static,
{
    type Response = Response<BoxBody>;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: HttpRequest<Incoming>) -> Self::Future {
        let mut inner = self.inner.clone();
        let readiness = self.readiness.clone();
        Box::pin(async move {
            let path = req.uri().path();
            debug!("ReadinessGateService: Received request for path: {}", path);
            // 1) Exact match: fixed probe/resource path
            let is_exact_probe = matches!(
                path,
                crate::server::PROFILE_MEMORY_PATH
                    | crate::server::PROFILE_CPU_PATH
                    | crate::server::HEALTH_PREFIX
                    | crate::server::FAVICON_PATH
            );

            // 2) Prefix matching: the entire set of route prefixes (including their subpaths)
            let is_prefix_probe = path.starts_with(crate::server::RUSTFS_ADMIN_PREFIX)
                || path.starts_with(crate::server::CONSOLE_PREFIX)
                || path.starts_with(crate::server::RPC_PREFIX)
                || path.starts_with(crate::server::ADMIN_PREFIX)
                || path.starts_with(crate::server::TONIC_PREFIX);

            let is_probe = is_exact_probe || is_prefix_probe;
            if !is_probe && !readiness.is_ready() {
                let body: BoxBody = Full::new(Bytes::from_static(b"Service not ready"))
                    .map_err(|e| -> BoxError { Box::new(e) })
                    .boxed_unsync();

                let resp = Response::builder()
                    .status(StatusCode::SERVICE_UNAVAILABLE)
                    .header(http::header::RETRY_AFTER, "5")
                    .header(http::header::CONTENT_TYPE, "text/plain; charset=utf-8")
                    .header(http::header::CACHE_CONTROL, "no-store")
                    .body(body)
                    .expect("failed to build not ready response");
                return Ok(resp);
            }
            let resp = inner.call(req).await?;
            // System is ready, forward to the actual S3/RPC handlers
            // Transparently converts any response body into a BoxBody, and then Trace/Cors/Compression continues to work
            let (parts, body) = resp.into_parts();
            let body: BoxBody = body.map_err(Into::into).boxed_unsync();
            Ok(Response::from_parts(parts, body))
        })
    }
}
