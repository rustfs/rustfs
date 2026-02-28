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
use crate::server::{ADMIN_PREFIX, CONSOLE_PREFIX, RPC_PREFIX, RUSTFS_ADMIN_PREFIX};
use crate::storage::apply_cors_headers;
use bytes::Bytes;
use http::{HeaderMap, HeaderValue, Method, Request as HttpRequest, Response, StatusCode};
use http_body::Body;
use http_body_util::BodyExt;
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

#[derive(Clone)]
pub struct ObjectAttributesEtagFixLayer;

impl<S> Layer<S> for ObjectAttributesEtagFixLayer {
    type Service = ObjectAttributesEtagFixService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        ObjectAttributesEtagFixService { inner }
    }
}

#[derive(Clone)]
pub struct ObjectAttributesEtagFixService<S> {
    inner: S,
}

impl<S, RestBody, GrpcBody> Service<HttpRequest<Incoming>> for ObjectAttributesEtagFixService<S>
where
    S: Service<HttpRequest<Incoming>, Response = Response<HybridBody<RestBody, GrpcBody>>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Send + 'static,
    RestBody: Body<Data = Bytes> + From<Bytes> + Send + 'static,
    RestBody::Error: Into<S::Error> + Send + 'static,
    GrpcBody: Send + 'static,
{
    type Response = Response<HybridBody<RestBody, GrpcBody>>;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: HttpRequest<Incoming>) -> Self::Future {
        let is_target = is_object_attributes_request(&req);
        let mut inner = self.inner.clone();

        Box::pin(async move {
            let response = inner.call(req).await?;
            let (parts, body) = response.into_parts();

            let response = match body {
                HybridBody::Rest { rest_body } => {
                    if !is_target {
                        Response::from_parts(parts, HybridBody::Rest { rest_body })
                    } else {
                        let rest_body = fix_object_attributes_etag_in_xml(rest_body).await.map_err(Into::into)?;

                        let mut parts = parts;
                        parts.headers.remove(http::header::CONTENT_LENGTH);

                        Response::from_parts(parts, HybridBody::Rest { rest_body })
                    }
                }
                HybridBody::Grpc { grpc_body } => Response::from_parts(parts, HybridBody::Grpc { grpc_body }),
            };

            Ok(response)
        })
    }
}

async fn fix_object_attributes_etag_in_xml<RestBody>(body: RestBody) -> Result<RestBody, RestBody::Error>
where
    RestBody: Body<Data = Bytes> + From<Bytes>,
{
    let bytes = BodyExt::collect(body).await?.to_bytes();
    let xml = String::from_utf8(bytes.to_vec()).unwrap_or_else(|_| String::from_utf8_lossy(&bytes).into_owned());
    let fixed = strip_quotes_from_first_etag(xml);
    Ok(RestBody::from(Bytes::from(fixed)))
}

fn strip_quotes_from_first_etag(xml: String) -> String {
    let Some(start) = xml.find("<ETag>") else {
        return xml;
    };
    let value_start = start + "<ETag>".len();
    let value_rest = &xml[value_start..];
    let Some(end_offset) = value_rest.find("</ETag>") else {
        return xml;
    };
    let value_end = value_start + end_offset;
    let raw = &xml[value_start..value_end];

    let Some(trimmed) = raw.strip_prefix('"').and_then(|v| v.strip_suffix('"')) else {
        return xml;
    };
    if trimmed == raw {
        return xml;
    }

    let mut fixed = String::with_capacity(xml.len() - 2);
    fixed.push_str(&xml[..value_start]);
    fixed.push_str(trimmed);
    fixed.push_str(&xml[value_end..]);
    fixed
}

fn is_object_attributes_request(req: &HttpRequest<Incoming>) -> bool {
    if req.method() != Method::GET {
        return false;
    }

    let path = req.uri().path();
    if path.starts_with(ADMIN_PREFIX)
        || path.starts_with(RUSTFS_ADMIN_PREFIX)
        || path.starts_with(CONSOLE_PREFIX)
        || path.starts_with(RPC_PREFIX)
    {
        return false;
    }

    let has_object_attributes_query = req.uri().query().is_some_and(|query| {
        query.split('&').any(|part| {
            let (name, _value) = part.split_once('=').unwrap_or((part, ""));
            matches!(
                name.to_ascii_lowercase().as_str(),
                "attributes" | "object-attributes" | "x-amz-object-attributes"
            )
        })
    });
    let has_object_attributes_header = req
        .headers()
        .get(http::header::HeaderName::from_static("x-amz-object-attributes"))
        .is_some();

    has_object_attributes_query || has_object_attributes_header
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
    const EXCLUDED_EXACT_PATHS: &'static [&'static str] = &["/health", "/health/ready", "/profile/cpu", "/profile/memory"];

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

        if method == Method::OPTIONS && ConditionalCorsLayer::is_s3_path(&path) {
            return Box::pin(async move { Ok(Response::builder().status(StatusCode::OK).body(ResBody::default()).unwrap()) });
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

#[cfg(test)]
mod tests {
    use super::*;
    use http_body_util::BodyExt;
    use http_body_util::Full;

    #[test]
    fn test_strip_quotes_from_first_etag_removes_quotes() {
        let input = String::from("<GetObjectAttributesOutput><ETag>\"abc\"</ETag></GetObjectAttributesOutput>");
        let output = strip_quotes_from_first_etag(input);

        assert_eq!(output, "<GetObjectAttributesOutput><ETag>abc</ETag></GetObjectAttributesOutput>");
    }

    #[test]
    fn test_strip_quotes_from_first_etag_keeps_non_quoted_value() {
        let input = String::from("<GetObjectAttributesOutput><ETag>abc</ETag></GetObjectAttributesOutput>");
        let output = strip_quotes_from_first_etag(input.clone());

        assert_eq!(output, input);
    }

    #[test]
    fn test_strip_quotes_from_first_etag_only_first_occurrence() {
        let input = String::from("<GetObjectAttributesOutput><ETag>\"first\"</ETag><ETag>\"second\"</ETag></GetObjectAttributesOutput>");
        let output = strip_quotes_from_first_etag(input);

        assert_eq!(output, "<GetObjectAttributesOutput><ETag>first</ETag><ETag>\"second\"</ETag></GetObjectAttributesOutput>");
    }

    #[tokio::test]
    async fn test_fix_object_attributes_etag_in_xml() {
        let body = Full::from(Bytes::from(
            "<GetObjectAttributesOutput><ETag>\"abc\"</ETag><Checksum>CRC32C</Checksum></GetObjectAttributesOutput>",
        ));
        let fixed = fix_object_attributes_etag_in_xml(body).await.unwrap();
        let bytes = BodyExt::collect(fixed).await.unwrap().to_bytes();

        assert_eq!(
            bytes,
            Bytes::from_static(
                b"<GetObjectAttributesOutput><ETag>abc</ETag><Checksum>CRC32C</Checksum></GetObjectAttributesOutput>",
            ),
        );
    }
}
