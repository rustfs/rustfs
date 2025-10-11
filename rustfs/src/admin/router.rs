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

use axum::routing::get;
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

use crate::admin::ADMIN_PREFIX;
use crate::admin::console;
use crate::admin::rpc::RPC_PREFIX;

const CONSOLE_PREFIX: &str = "/rustfs/console";

pub struct S3Router<T> {
    router: Router<T>,
    console_enabled: bool,
    console_router: Option<axum::routing::RouterIntoService<Body>>,
}

impl<T: Operation> S3Router<T> {
    pub fn new(console_enabled: bool) -> Self {
        let router = Router::new();

        let console_router = if console_enabled {
            Some(
                axum::Router::new()
                    .nest(CONSOLE_PREFIX, axum::Router::new().fallback_service(get(console::static_handler)))
                    .fallback_service(get(console::static_handler))
                    .into_service::<Body>(),
            )
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
        if method == Method::GET && uri.path() == "/health" {
            return true;
        }

        // AssumeRole
        if method == Method::POST && uri.path() == "/" {
            if let Some(val) = headers.get(header::CONTENT_TYPE) {
                if val.as_bytes() == b"application/x-www-form-urlencoded" {
                    return true;
                }
            }
        }

        uri.path().starts_with(ADMIN_PREFIX) || uri.path().starts_with(RPC_PREFIX) || uri.path().starts_with(CONSOLE_PREFIX)
    }

    // check_access before call
    async fn check_access(&self, req: &mut S3Request<Body>) -> S3Result<()> {
        // Allow unauthenticated access to health check
        if req.method == Method::GET && req.uri.path() == "/health" {
            return Ok(());
        }
        // Allow unauthenticated access to console static files if console is enabled
        if self.console_enabled && req.uri.path().starts_with(CONSOLE_PREFIX) {
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
        if self.console_enabled && req.uri.path().starts_with(CONSOLE_PREFIX) {
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

        let uri = format!("{}|{}", &req.method, req.uri.path());
        if let Ok(mat) = self.router.at(&uri) {
            let op: &T = mat.value;
            let mut resp = op.call(req, mat.params).await?;
            resp.status = Some(resp.output.0);
            return Ok(resp.map_output(|x| x.1));
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
