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
use crate::admin::console::make_console_server;
use crate::server::{ADMIN_PREFIX, HEALTH_PREFIX, PROFILE_CPU_PATH, PROFILE_MEMORY_PATH, RPC_PREFIX};
use http::HeaderValue;
use hyper::HeaderMap;
use hyper::Method;
use hyper::StatusCode;
use hyper::Uri;
use hyper::http::Extensions;
use matchit::Params;
use matchit::Router;
use rustfs_ecstore::rpc::verify_rpc_signature;
use s3s::Body;
use s3s::S3Request;
use s3s::S3Response;
use s3s::S3Result;
use s3s::header;
use s3s::route::S3Route;
use s3s::s3_error;
use tower::Service;
use tracing::error;
use tracing::info;

pub struct S3Router<T> {
    router: Router<T>,
    console_enabled: bool,
    console_router: Option<axum::routing::RouterIntoService<Body>>,
}

impl<T: Operation> S3Router<T> {
    pub fn new(console_enabled: bool) -> Self {
        let router = Router::new();

        let console_router = if console_enabled {
            Some(make_console_server().into_service::<Body>())
        } else {
            None
        };

        Self {
            router,
            console_enabled,
            console_router,
        }
    }

    pub fn insert(&mut self, method: Method, path: &str, operation: T) -> std::io::Result<()> {
        let path = Self::make_route_str(method, path);

        // warn!("set uri {}", &path);

        self.router.insert(path, operation).map_err(std::io::Error::other)?;

        Ok(())
    }

    fn make_route_str(method: Method, path: &str) -> String {
        format!("{}|{}", method.as_str(), path)
    }
}

impl<T: Operation> Default for S3Router<T> {
    fn default() -> Self {
        Self::new(false)
    }
}

#[async_trait::async_trait]
impl<T> S3Route for S3Router<T>
where
    T: Operation,
{
    fn is_match(&self, method: &Method, uri: &Uri, headers: &HeaderMap, _: &mut Extensions) -> bool {
        let path = uri.path();

        // Handle OPTIONS requests for CORS preflight (all paths)
        // This ensures OPTIONS requests are handled by our router before default S3 implementation
        if method == Method::OPTIONS && headers.contains_key("origin") {
            return true;
        }

        // Profiling endpoints
        if method == Method::GET && (path == PROFILE_CPU_PATH || path == PROFILE_MEMORY_PATH) {
            return true;
        }

        // Health check
        if (method == Method::HEAD || method == Method::GET) && path == HEALTH_PREFIX {
            return true;
        }

        // AssumeRole
        if method == Method::POST
            && path == "/"
            && headers
                .get(header::CONTENT_TYPE)
                .and_then(|v| v.to_str().ok())
                .map(|ct| ct.split(';').next().unwrap_or("").trim().to_lowercase())
                .map(|ct| ct == "application/x-www-form-urlencoded")
                .unwrap_or(false)
        {
            return true;
        }

        path.starts_with(ADMIN_PREFIX) || path.starts_with(RPC_PREFIX) || is_console_path(path)
    }

    // check_access before call
    async fn check_access(&self, req: &mut S3Request<Body>) -> S3Result<()> {
        // Allow unauthenticated access to health check
        let path = req.uri.path();

        // Allow OPTIONS requests for CORS preflight (no authentication required)
        // CORS preflight requests are sent by browsers and don't include authentication
        if req.method == Method::OPTIONS && req.headers.contains_key("origin") {
            return Ok(());
        }

        // Profiling endpoints
        if req.method == Method::GET && (path == PROFILE_CPU_PATH || path == PROFILE_MEMORY_PATH) {
            return Ok(());
        }

        // Health check
        if (req.method == Method::HEAD || req.method == Method::GET) && path == HEALTH_PREFIX {
            return Ok(());
        }

        // Allow unauthenticated access to console static files if console is enabled
        if self.console_enabled && is_console_path(path) {
            return Ok(());
        }

        // Check RPC signature verification
        if req.uri.path().starts_with(RPC_PREFIX) {
            // Skip signature verification for HEAD requests (health checks)
            if req.method != Method::HEAD {
                verify_rpc_signature(&req.uri.to_string(), &req.method, &req.headers).map_err(|e| {
                    error!("RPC signature verification failed: {}", e);
                    s3_error!(AccessDenied, "{}", e)
                })?;
            }
            return Ok(());
        }

        // For non-RPC admin requests, check credentials
        match req.credentials {
            Some(_) => Ok(()),
            None => Err(s3_error!(AccessDenied, "Signature is required")),
        }
    }

    async fn call(&self, req: S3Request<Body>) -> S3Result<S3Response<Body>> {
        // Console requests should be handled by console router first (including OPTIONS)
        // Console has its own CORS layer configured
        if self.console_enabled && is_console_path(req.uri.path()) {
            if let Some(console_router) = &self.console_router {
                let mut console_router = console_router.clone();
                let req = convert_request(req);
                let result = console_router.call(req).await;
                return match result {
                    Ok(resp) => Ok(convert_response(resp)),
                    Err(e) => Err(s3_error!(InternalError, "{}", e)),
                };
            }
            return Err(s3_error!(InternalError, "console is not enabled"));
        }

        // Handle OPTIONS preflight requests
        if req.method == Method::OPTIONS && req.headers.contains_key("origin") {
            let path = req.uri.path();
            info!("path: {}", path);
            // Handle Admin API CORS preflight
            if path.starts_with(ADMIN_PREFIX) || path.starts_with(RPC_PREFIX) || path == "/" {
                let mut response = S3Response::new(Body::empty());
                apply_admin_cors_preflight(&req.headers, &mut response);
                return Ok(response);
            }

            // Handle S3 bucket CORS preflight
            let path = path.trim_start_matches('/');
            let bucket = path.split('/').next().unwrap_or("").to_string();

            if !bucket.is_empty() {
                // Handle CORS preflight using existing function
                if let Some(cors_headers) = crate::storage::ecfs::apply_cors_headers(&bucket, &req.method, &req.headers).await {
                    let mut response = S3Response::new(Body::empty());
                    for (key, value) in cors_headers.iter() {
                        response.headers.insert(key, value.clone());
                    }
                    return Ok(response);
                }
            }

            // If no CORS rule matched, return 200 OK without CORS headers
            // This is the correct behavior per S3 CORS spec
            return Ok(S3Response::new(Body::empty()));
        }

        let uri = format!("{}|{}", &req.method, req.uri.path());
        // Clone headers before moving req
        let request_headers = req.headers.clone();

        if let Ok(mat) = self.router.at(&uri) {
            let op: &T = mat.value;
            let mut resp = op.call(req, mat.params).await?;
            resp.status = Some(resp.output.0);
            let mut response = resp.map_output(|x| x.1);

            apply_admin_cors_headers(&request_headers, &mut response.headers);

            return Ok(response);
        }

        Err(s3_error!(NotImplemented))
    }
}

#[async_trait::async_trait]
pub trait Operation: Send + Sync + 'static {
    // fn method() -> Method;
    // fn uri() -> &'static str;
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>>;
}

pub struct AdminOperation(pub &'static dyn Operation);

#[async_trait::async_trait]
impl Operation for AdminOperation {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        self.0.call(req, params).await
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct Extra {
    pub credentials: Option<s3s::auth::Credentials>,
    pub region: Option<String>,
    pub service: Option<String>,
}

fn convert_request(req: S3Request<Body>) -> http::Request<Body> {
    let (mut parts, _) = http::Request::new(Body::empty()).into_parts();
    parts.method = req.method;
    parts.uri = req.uri;
    parts.headers = req.headers;
    parts.extensions = req.extensions;
    parts.extensions.insert(Extra {
        credentials: req.credentials,
        region: req.region,
        service: req.service,
    });
    http::Request::from_parts(parts, req.input)
}

fn convert_response(resp: http::Response<axum::body::Body>) -> S3Response<Body> {
    let (parts, body) = resp.into_parts();
    let mut s3_resp = S3Response::new(Body::http_body_unsync(body));
    s3_resp.status = Some(parts.status);
    s3_resp.headers = parts.headers;
    s3_resp.extensions = parts.extensions;
    s3_resp
}

/// Get admin CORS allowed origins from environment variable
fn get_admin_cors_origins() -> Option<String> {
    std::env::var("RUSTFS_ADMIN_CORS_ALLOWED_ORIGINS")
        .ok()
        .filter(|s| !s.is_empty())
        .or_else(|| {
            // Fallback to RUSTFS_CORS_ALLOWED_ORIGINS if admin-specific is not set
            std::env::var("RUSTFS_CORS_ALLOWED_ORIGINS").ok().filter(|s| !s.is_empty())
        })
}

/// Apply CORS headers to Admin API preflight (OPTIONS) response
pub fn apply_admin_cors_preflight(headers: &HeaderMap, response: &mut S3Response<Body>) {
    let origin = headers.get("origin").and_then(|v| v.to_str().ok()).map(|s| s.to_string());

    let cors_origins = get_admin_cors_origins();
    let allowed_origin = match (origin, cors_origins) {
        (Some(orig), Some(config)) if config == "*" => Some(orig),
        (Some(orig), Some(config)) => {
            let origins: Vec<&str> = config.split(',').map(|s| s.trim()).collect();
            if origins.contains(&orig.as_str()) { Some(orig) } else { None }
        }
        (Some(orig), None) => Some(orig), // Default: allow all if not configured
        _ => None,
    };

    if let Some(origin) = allowed_origin {
        if let Ok(header_value) = HeaderValue::from_str(&origin) {
            response.headers.insert("access-control-allow-origin", header_value);
        }
    }

    // Get requested headers from preflight request
    let requested_headers = headers
        .get("access-control-request-headers")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("*");

    response.headers.insert(
        "access-control-allow-methods",
        HeaderValue::from_static("GET, POST, PUT, DELETE, OPTIONS, HEAD"),
    );
    response.headers.insert(
        "access-control-allow-headers",
        HeaderValue::from_str(requested_headers).unwrap_or_else(|_| HeaderValue::from_static("*")),
    );
    response.headers.insert(
        "access-control-max-age",
        HeaderValue::from_static("86400"), // 24 hours
    );
}

/// Apply CORS headers to Admin API response
fn apply_admin_cors_headers(request_headers: &HeaderMap, response_headers: &mut HeaderMap) {
    let origin = request_headers
        .get("origin")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    let cors_origins = get_admin_cors_origins();
    let allowed_origin = match (origin, cors_origins) {
        (Some(orig), Some(config)) if config == "*" => Some(orig),
        (Some(orig), Some(config)) => {
            let origins: Vec<&str> = config.split(',').map(|s| s.trim()).collect();
            if origins.contains(&orig.as_str()) { Some(orig) } else { None }
        }
        (Some(orig), None) => Some(orig), // Default: allow all if not configured
        _ => None,
    };

    if let Some(origin) = allowed_origin {
        if let Ok(header_value) = HeaderValue::from_str(&origin) {
            response_headers.insert("access-control-allow-origin", header_value);
        }
    }

    // Expose common headers that might be needed by clients
    response_headers.insert(
        "access-control-expose-headers",
        HeaderValue::from_static("x-request-id, content-type, content-length"),
    );
}
