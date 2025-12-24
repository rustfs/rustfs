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

use axum::body::Body;
use futures::future::BoxFuture;
use http::{Request, Response, StatusCode};
use rustfs_common::GlobalReadiness;
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

impl<S, ReqBody, ResBody> Service<Request<ReqBody>> for ReadinessGateService<S>
where
    S: Service<Request<ReqBody>, Response = Response<ResBody>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    ResBody: Default + Send + 'static,
    ReqBody: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request<ReqBody>) -> Self::Future {
        let readiness = self.readiness.clone();
        // System is ready, forward to the actual S3/RPC handlers
        let mut inner = self.inner.clone();
        Box::pin(async move {
            let path = req.uri().path();
            let is_probe = matches!(
                path,
                "/profile" | "/health" | "/rustfs/admin" | "/favicon.ico" | "/rustfs/console" | "/rustfs/rpc"
            );
            if !is_probe && !readiness.is_ready() {
                let resp = Response::builder()
                    .status(StatusCode::SERVICE_UNAVAILABLE)
                    .header("Retry-After", "5")
                    .body(Body::from("system not ready"))
                    .expect("build response");
                return Ok(resp);
            }
            inner.call(req).await
        })
    }
}
