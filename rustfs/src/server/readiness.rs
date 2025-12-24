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

use crate::server::hybrid::HybridBody;
use http::{Response, StatusCode};
use hyper::body::Incoming;
use rustfs_common::GlobalReadiness;
use s3s::HttpRequest;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tower::{Layer, Service};

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

impl<S, RestBody, GrpcBody> Service<HttpRequest<Incoming>> for ReadinessGateService<S>
where
    S: Service<HttpRequest<Incoming>, Response = Response<HybridBody<RestBody, GrpcBody>>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Into<Box<dyn std::error::Error + Send + Sync>> + Send + 'static,
    RestBody: Default + Send + 'static,
    GrpcBody: Send + 'static,
{
    type Response = Response<HybridBody<RestBody, GrpcBody>>;
    type Error = Box<dyn std::error::Error + Send + Sync>;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx).map_err(Into::into)
    }

    fn call(&mut self, req: HttpRequest<Incoming>) -> Self::Future {
        let readiness = self.readiness.clone();

        let path = req.uri().path();
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
            || path.starts_with(crate::server::ADMIN_PREFIX);

        let is_probe = is_exact_probe || is_prefix_probe;
        // System is ready, forward to the actual S3/RPC handlers
        if !is_probe && !readiness.is_ready() {
            // let mut resps = Response::new(AxumBody::from(Body::from("system not ready")));
            // // 503 + Retry-After, prompting the client to try again later
            // *resps.status_mut() = StatusCode::SERVICE_UNAVAILABLE;
            // resps
            //     .headers_mut()
            //     .insert(http::header::RETRY_AFTER, http::HeaderValue::from_static("5"));
            // // Clarify return types to avoid content sniffing
            // resps
            //     .headers_mut()
            //     .insert(http::header::CONTENT_TYPE, http::HeaderValue::from_static("text/plain; charset=utf-8"));
            // // Have the upstream/agent not cache the "not ready" response
            // resps
            //     .headers_mut()
            //     .insert(http::header::CACHE_CONTROL, http::HeaderValue::from_static("no-store"));
            // return Ok(resps);
            let resp = Response::builder()
                .status(StatusCode::SERVICE_UNAVAILABLE)
                .header(http::header::RETRY_AFTER, "5")
                .header(http::header::CONTENT_TYPE, "text/plain; charset=utf-8")
                .header(http::header::CACHE_CONTROL, "no-store")
                .body(HybridBody::Rest {
                    rest_body: RestBody::default(),
                })
                .expect("failed to build not ready response");
            return Box::pin(async move { Ok(resp) });
        }
        let mut inner = self.inner.clone();
        Box::pin(async move { inner.call(req).await.map_err(Into::into) })
    }
}
