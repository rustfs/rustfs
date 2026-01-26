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

use crate::admin::console::is_console_path;
use crate::server::cors;
use crate::server::hybrid::HybridBody;
use crate::server::{ADMIN_PREFIX, RPC_PREFIX};
use crate::storage::apply_cors_headers;
use http::{HeaderMap, HeaderValue, Method, Request as HttpRequest, Response, StatusCode};
use hyper::body::Incoming;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tower::{Layer, Service};
use tracing::{debug, info};

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

/// Conditional CORS layer that only applies to S3 API requests
/// (not Admin, not Console, not RPC)
#[derive(Clone)]
pub struct ConditionalCorsLayer {
    cors_origins: Option<String>,
}

impl ConditionalCorsLayer {
    pub fn new() -> Self {
        let cors_origins = std::env::var("RUSTFS_CORS_ALLOWED_ORIGINS").ok().filter(|s| !s.is_empty());
        Self { cors_origins }
    }

    /// Exact paths that should be excluded from being treated as S3 paths.
    const EXCLUDED_EXACT_PATHS: &'static [&'static str] = &["/health", "/profile/cpu", "/profile/memory"];

    fn is_s3_path(path: &str) -> bool {
        // Exclude Admin, Console, RPC, and configured special paths
        !path.starts_with(ADMIN_PREFIX)
            && !path.starts_with(RPC_PREFIX)
            && !is_console_path(path)
            && !Self::EXCLUDED_EXACT_PATHS.contains(&path)
    }

    fn apply_cors_headers(&self, request_headers: &HeaderMap, response_headers: &mut HeaderMap) {
        let origin = request_headers
            .get(cors::standard::ORIGIN)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());

        let allowed_origin = match (origin, &self.cors_origins) {
            (Some(orig), Some(config)) if config == "*" => Some(orig),
            (Some(orig), Some(config)) => {
                let origins: Vec<&str> = config.split(',').map(|s| s.trim()).collect();
                if origins.contains(&orig.as_str()) { Some(orig) } else { None }
            }
            (Some(orig), None) => Some(orig), // Default: allow all if not configured
            _ => None,
        };

        // Track whether we're using a specific origin (not wildcard)
        let using_specific_origin = if let Some(origin) = &allowed_origin {
            if let Ok(header_value) = HeaderValue::from_str(origin) {
                response_headers.insert(cors::response::ACCESS_CONTROL_ALLOW_ORIGIN, header_value);
                true // Using specific origin, credentials allowed
            } else {
                false
            }
        } else {
            false
        };

        // Allow all methods by default (S3-compatible set)
        response_headers.insert(
            cors::response::ACCESS_CONTROL_ALLOW_METHODS,
            HeaderValue::from_static("GET, POST, PUT, DELETE, OPTIONS, HEAD"),
        );

        // Allow all headers by default
        response_headers.insert(cors::response::ACCESS_CONTROL_ALLOW_HEADERS, HeaderValue::from_static("*"));

        // Expose common headers
        response_headers.insert(
            cors::response::ACCESS_CONTROL_EXPOSE_HEADERS,
            HeaderValue::from_static("x-request-id, content-type, content-length, etag"),
        );

        // Only set credentials when using a specific origin (not wildcard)
        // CORS spec: credentials cannot be used with wildcard origins
        if using_specific_origin {
            response_headers.insert(cors::response::ACCESS_CONTROL_ALLOW_CREDENTIALS, HeaderValue::from_static("true"));
        }
    }
}

impl Default for ConditionalCorsLayer {
    fn default() -> Self {
        Self::new()
    }
}

impl<S> Layer<S> for ConditionalCorsLayer {
    type Service = ConditionalCorsService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        ConditionalCorsService {
            inner,
            cors_origins: Arc::new(self.cors_origins.clone()),
        }
    }
}

/// Service implementation for conditional CORS
#[derive(Clone)]
pub struct ConditionalCorsService<S> {
    inner: S,
    cors_origins: Arc<Option<String>>,
}

impl<S, ResBody> Service<HttpRequest<Incoming>> for ConditionalCorsService<S>
where
    S: Service<HttpRequest<Incoming>, Response = Response<ResBody>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Into<Box<dyn std::error::Error + Send + Sync>> + Send + 'static,
    ResBody: Default + Send + 'static,
{
    type Response = Response<ResBody>;
    type Error = Box<dyn std::error::Error + Send + Sync>;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx).map_err(Into::into)
    }

    fn call(&mut self, req: HttpRequest<Incoming>) -> Self::Future {
        let path = req.uri().path().to_string();
        let method = req.method().clone();
        let request_headers = req.headers().clone();
        let cors_origins = self.cors_origins.clone();
        // Handle OPTIONS preflight requests - return response directly without calling handler
        if method == Method::OPTIONS && request_headers.contains_key(cors::standard::ORIGIN) {
            info!("OPTIONS preflight request for path: {}", path);

            let path_trimmed = path.trim_start_matches('/');
            let bucket = path_trimmed.split('/').next().unwrap_or("").to_string(); // virtual host style?
            let method_clone = method.clone();
            let request_headers_clone = request_headers.clone();

            return Box::pin(async move {
                let mut response = Response::builder().status(StatusCode::OK).body(ResBody::default()).unwrap();

                if ConditionalCorsLayer::is_s3_path(&path)
                    && !bucket.is_empty()
                    && cors_origins.is_some()
                    && let Some(cors_headers) = apply_cors_headers(&bucket, &method_clone, &request_headers_clone).await
                {
                    for (key, value) in cors_headers.iter() {
                        response.headers_mut().insert(key, value.clone());
                    }
                    return Ok(response);
                }

                let cors_layer = ConditionalCorsLayer {
                    cors_origins: (*cors_origins).clone(),
                };
                cors_layer.apply_cors_headers(&request_headers_clone, response.headers_mut());

                Ok(response)
            });
        }

        let mut inner = self.inner.clone();
        Box::pin(async move {
            let mut response = inner.call(req).await.map_err(Into::into)?;

            // Apply CORS headers only to S3 API requests (non-OPTIONS)
            if request_headers.contains_key(cors::standard::ORIGIN)
                && !response.headers().contains_key(cors::response::ACCESS_CONTROL_ALLOW_ORIGIN)
            {
                let cors_layer = ConditionalCorsLayer {
                    cors_origins: (*cors_origins).clone(),
                };
                cors_layer.apply_cors_headers(&request_headers, response.headers_mut());
            }

            Ok(response)
        })
    }
}
