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
use http::{Request as HttpRequest, Response, StatusCode};
use hyper::body::Incoming;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use tower::{Layer, Service};
use tracing::debug;

/// Redirect layer that redirects browser requests to the console
#[derive(Clone)]
pub struct RedirectLayer;

impl<S> Layer<S> for RedirectLayer {
    type Service = RedirectService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        RedirectService { inner }
    }
}

/// Service implementation for redirect functionality
#[derive(Clone)]
pub struct RedirectService<S> {
    inner: S,
}

impl<S, RestBody, GrpcBody> Service<HttpRequest<Incoming>> for RedirectService<S>
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
        // Check if this is a GET request without Authorization header and User-Agent contains Mozilla
        // and the path is either "/" or "/index.html"
        let path = req.uri().path().trim_end_matches('/');
        let should_redirect = req.method() == http::Method::GET
            && !req.headers().contains_key(http::header::AUTHORIZATION)
            && req
                .headers()
                .get(http::header::USER_AGENT)
                .and_then(|v| v.to_str().ok())
                .map(|ua| ua.contains("Mozilla"))
                .unwrap_or(false)
            && (path.is_empty() || path == "/rustfs" || path == "/index.html");

        if should_redirect {
            debug!("Redirecting browser request from {} to console", path);

            // Create redirect response
            let redirect_response = Response::builder()
                .status(StatusCode::FOUND)
                .header(http::header::LOCATION, "/rustfs/console/")
                .body(HybridBody::Rest {
                    rest_body: RestBody::default(),
                })
                .expect("failed to build redirect response");

            return Box::pin(async move { Ok(redirect_response) });
        }

        // Otherwise, forward to the next service
        let mut inner = self.inner.clone();
        Box::pin(async move { inner.call(req).await.map_err(Into::into) })
    }
}
