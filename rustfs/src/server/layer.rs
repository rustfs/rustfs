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
use crate::error::ApiError;
use crate::server::cors;
use crate::server::hybrid::HybridBody;
use crate::server::{ADMIN_PREFIX, CONSOLE_PREFIX, MINIO_ADMIN_PREFIX, MINIO_ADMIN_V3_PREFIX, RPC_PREFIX, RUSTFS_ADMIN_PREFIX};
use crate::storage::apply_cors_headers;
use crate::storage::request_context::{RequestContext, extract_request_id_from_headers};
use bytes::Bytes;
use http::{HeaderMap, HeaderValue, Method, Request as HttpRequest, Response, StatusCode};
use http_body::Body;
use http_body_util::BodyExt;
use hyper::body::Incoming;
use opentelemetry::global;
use opentelemetry::trace::TraceContextExt;
use rustfs_utils::get_env_opt_str;
use rustfs_utils::http::headers::AMZ_REQUEST_ID;
use s3s::S3ErrorCode;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Instant;
use tower::{Layer, Service};
use tracing::debug;

/// A carrier that adapts [`HeaderMap`] for OpenTelemetry trace context propagation.
struct HeaderMapCarrier<'a>(&'a HeaderMap);

impl<'a> opentelemetry::propagation::Extractor for HeaderMapCarrier<'a> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).and_then(|v| v.to_str().ok())
    }

    fn keys(&self) -> Vec<&str> {
        self.0.keys().map(|k| k.as_str()).collect()
    }

    fn get_all(&self, key: &str) -> Option<Vec<&str>> {
        let headers = self
            .0
            .get_all(key)
            .iter()
            .filter_map(|value| value.to_str().ok())
            .collect::<Vec<_>>();

        if headers.is_empty() { None } else { Some(headers) }
    }
}

/// Tower middleware layer that creates a canonical [`RequestContext`] from HTTP headers
/// and injects it into `request.extensions()`.
///
/// This layer must be placed after `SetRequestIdLayer` in the middleware stack,
/// as it reads the `x-request-id` header that `SetRequestIdLayer` generates.
///
/// Additionally, it sets the `x-amz-request-id` request header for S3 compatibility
/// if not already present.
#[derive(Clone, Default)]
pub struct RequestContextLayer;

impl<S> Layer<S> for RequestContextLayer {
    type Service = RequestContextService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        RequestContextService { inner }
    }
}

/// Service that injects [`RequestContext`] into every request.
#[derive(Clone)]
pub struct RequestContextService<S> {
    inner: S,
}

impl<S, B> Service<HttpRequest<B>> for RequestContextService<S>
where
    S: Service<HttpRequest<B>>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut req: HttpRequest<B>) -> Self::Future {
        let request_id = extract_request_id_from_headers(req.headers());

        // Extract OpenTelemetry trace/span context from incoming headers
        let parent_cx = global::get_text_map_propagator(|propagator| propagator.extract(&HeaderMapCarrier(req.headers())));
        let span_ref = parent_cx.span();
        let span_context = span_ref.span_context();
        let trace_id = if span_context.is_valid() {
            Some(span_context.trace_id().to_string())
        } else {
            None
        };
        let span_id = if span_context.is_valid() {
            Some(span_context.span_id().to_string())
        } else {
            None
        };

        // Preserve the upstream x-amz-request-id if present (S3 client forwarding),
        // otherwise fall back to the canonical request_id.
        let x_amz_request_id = req
            .headers()
            .get(AMZ_REQUEST_ID)
            .and_then(|v| v.to_str().ok())
            .map(String::from)
            .unwrap_or_else(|| request_id.clone());

        let ctx = RequestContext {
            request_id: request_id.clone(),
            x_amz_request_id,
            trace_id,
            span_id,
            start_time: Instant::now(),
        };

        req.extensions_mut().insert(ctx);

        // Set x-amz-request-id for S3 compatibility downstream
        if !req.headers().contains_key(AMZ_REQUEST_ID)
            && let Ok(val) = HeaderValue::from_str(&request_id)
        {
            req.headers_mut()
                .insert(http::header::HeaderName::from_static(AMZ_REQUEST_ID), val);
        }

        self.inner.call(req)
    }
}

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
pub struct AdminChunkedContentLengthCompatLayer;

impl<S> Layer<S> for AdminChunkedContentLengthCompatLayer {
    type Service = AdminChunkedContentLengthCompatService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        AdminChunkedContentLengthCompatService { inner }
    }
}

#[derive(Clone)]
pub struct AdminChunkedContentLengthCompatService<S> {
    inner: S,
}

impl<S, ReqBody, ResBody> Service<HttpRequest<ReqBody>> for AdminChunkedContentLengthCompatService<S>
where
    S: Service<HttpRequest<ReqBody>, Response = Response<ResBody>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Into<Box<dyn std::error::Error + Send + Sync>> + Send + 'static,
    ReqBody: Send + 'static,
    ResBody: Send + 'static,
{
    type Response = Response<ResBody>;
    type Error = Box<dyn std::error::Error + Send + Sync>;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx).map_err(Into::into)
    }

    fn call(&mut self, mut req: HttpRequest<ReqBody>) -> Self::Future {
        if should_force_zero_content_length_for_admin_empty_body(&req) {
            req.headers_mut()
                .insert(http::header::CONTENT_LENGTH, HeaderValue::from_static("0"));
        }

        let mut inner = self.inner.clone();
        Box::pin(async move { inner.call(req).await.map_err(Into::into) })
    }
}

fn should_force_zero_content_length_for_admin_empty_body<B>(req: &HttpRequest<B>) -> bool {
    is_empty_body_admin_path(req.method(), req.uri().path()) && !req.headers().contains_key(http::header::CONTENT_LENGTH)
}

fn is_empty_body_admin_path(method: &Method, path: &str) -> bool {
    match *method {
        Method::PUT => matches!(
            path,
            "/minio/admin/v3/set-user-status"
                | "/minio/admin/v3/set-group-status"
                | "/rustfs/admin/v3/set-user-status"
                | "/rustfs/admin/v3/set-group-status"
        ),
        Method::POST => matches!(
            path,
            "/minio/admin/v3/rebalance/start"
                | "/minio/admin/v3/rebalance/stop"
                | "/minio/admin/v3/pools/decommission"
                | "/minio/admin/v3/pools/cancel"
                | "/rustfs/admin/v3/rebalance/start"
                | "/rustfs/admin/v3/rebalance/stop"
                | "/rustfs/admin/v3/pools/decommission"
                | "/rustfs/admin/v3/pools/cancel"
        ),
        _ => false,
    }
}

#[derive(Clone)]
pub struct S3ErrorMessageCompatLayer;

impl<S> Layer<S> for S3ErrorMessageCompatLayer {
    type Service = S3ErrorMessageCompatService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        S3ErrorMessageCompatService { inner }
    }
}

#[derive(Clone)]
pub struct S3ErrorMessageCompatService<S> {
    inner: S,
}

impl<S, RestBody, GrpcBody> Service<HttpRequest<Incoming>> for S3ErrorMessageCompatService<S>
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
        let mut inner = self.inner.clone();

        Box::pin(async move {
            let response = inner.call(req).await?;
            let (parts, body) = response.into_parts();
            let should_fix = parts.status == StatusCode::FORBIDDEN && is_xml_response(&parts.headers);

            let response = match body {
                HybridBody::Rest { rest_body } => {
                    if !should_fix {
                        Response::from_parts(parts, HybridBody::Rest { rest_body })
                    } else {
                        let (rest_body, changed) = fix_s3_error_message_in_xml(rest_body).await.map_err(Into::into)?;
                        let mut parts = parts;
                        if changed {
                            parts.headers.remove(http::header::CONTENT_LENGTH);
                        }
                        Response::from_parts(parts, HybridBody::Rest { rest_body })
                    }
                }
                HybridBody::Grpc { grpc_body } => Response::from_parts(parts, HybridBody::Grpc { grpc_body }),
            };

            Ok(response)
        })
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
            let should_fix = is_target && parts.status.is_success() && is_xml_response(&parts.headers);

            let response = match body {
                HybridBody::Rest { rest_body } => {
                    if !should_fix {
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

/// Tower middleware that strips the body (and body-describing headers) from
/// responses whose HTTP status code MUST NOT carry a body per RFC 9110 §6.4.1
/// and §15 (1xx, 204, 205, 304).
///
/// The inner s3s layer serializes every `S3Error` — including 304 `NotModified`
/// preconditions — as an XML body. Returning that body for a 304 is a protocol
/// violation: hyper's HTTP/1.1 encoder forces the body to zero length but
/// preserves the response, while the HTTP/2 path fills in `content-length`
/// from the body's size hint and writes DATA frames after a HEADERS frame that
/// should have carried END_STREAM. h2 clients (curl, browsers) and proxies see
/// the malformed response as a connection-level failure — in the wild this
/// surfaces as `GOAWAY error=0` on h2 and as an upstream-disconnect 5xx from
/// reverse proxies like ngrok (`ERR_NGROK_3004`).
#[derive(Clone)]
pub struct BodylessStatusFixLayer;

impl<S> Layer<S> for BodylessStatusFixLayer {
    type Service = BodylessStatusFixService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        BodylessStatusFixService { inner }
    }
}

#[derive(Clone)]
pub struct BodylessStatusFixService<S> {
    inner: S,
}

impl<S, ReqBody, RestBody, GrpcBody> Service<HttpRequest<ReqBody>> for BodylessStatusFixService<S>
where
    S: Service<HttpRequest<ReqBody>, Response = Response<HybridBody<RestBody, GrpcBody>>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Send + 'static,
    ReqBody: Send + 'static,
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

    fn call(&mut self, req: HttpRequest<ReqBody>) -> Self::Future {
        let mut inner = self.inner.clone();

        Box::pin(async move {
            let response = inner.call(req).await?;
            let (mut parts, body) = response.into_parts();

            if !is_bodyless_status(parts.status) {
                return Ok(Response::from_parts(parts, body));
            }

            let response = match body {
                HybridBody::Rest { .. } => {
                    parts.headers.remove(http::header::CONTENT_LENGTH);
                    parts.headers.remove(http::header::CONTENT_TYPE);
                    parts.headers.remove(http::header::TRANSFER_ENCODING);
                    Response::from_parts(
                        parts,
                        HybridBody::Rest {
                            rest_body: RestBody::from(Bytes::new()),
                        },
                    )
                }
                HybridBody::Grpc { grpc_body } => Response::from_parts(parts, HybridBody::Grpc { grpc_body }),
            };

            Ok(response)
        })
    }
}

/// Tower middleware that strips the actual response body for `HEAD` requests
/// while preserving metadata headers such as `Content-Length`.
///
/// The inner s3s layer may serialize S3 errors as XML bodies. That is valid for
/// regular requests, but for `HEAD` the HTTP layer must suppress the response
/// body entirely. If we forward the serialized error body over HTTP/2, clients
/// observe DATA frames on a `HEAD` response and fail the exchange with a
/// protocol error.
#[derive(Clone)]
pub struct HeadRequestBodyFixLayer;

impl<S> Layer<S> for HeadRequestBodyFixLayer {
    type Service = HeadRequestBodyFixService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        HeadRequestBodyFixService { inner }
    }
}

#[derive(Clone)]
pub struct HeadRequestBodyFixService<S> {
    inner: S,
}

impl<S, ReqBody, RestBody, GrpcBody> Service<HttpRequest<ReqBody>> for HeadRequestBodyFixService<S>
where
    S: Service<HttpRequest<ReqBody>, Response = Response<HybridBody<RestBody, GrpcBody>>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    ReqBody: Send + 'static,
    RestBody: Body<Data = Bytes> + From<Bytes> + Send + 'static,
    GrpcBody: Send + 'static,
{
    type Response = Response<HybridBody<RestBody, GrpcBody>>;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: HttpRequest<ReqBody>) -> Self::Future {
        let is_head = req.method() == Method::HEAD;
        let mut inner = self.inner.clone();

        Box::pin(async move {
            let response = inner.call(req).await?;
            if !is_head {
                return Ok(response);
            }

            let (mut parts, body) = response.into_parts();
            parts.headers.remove(http::header::TRANSFER_ENCODING);

            let response = match body {
                HybridBody::Rest { .. } => Response::from_parts(
                    parts,
                    HybridBody::Rest {
                        rest_body: RestBody::from(Bytes::new()),
                    },
                ),
                HybridBody::Grpc { grpc_body } => Response::from_parts(parts, HybridBody::Grpc { grpc_body }),
            };

            Ok(response)
        })
    }
}

fn is_bodyless_status(status: StatusCode) -> bool {
    status.is_informational()
        || status == StatusCode::NO_CONTENT
        || status == StatusCode::RESET_CONTENT
        || status == StatusCode::NOT_MODIFIED
}

fn is_xml_response(headers: &HeaderMap) -> bool {
    let is_xml = headers
        .get(http::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .map(|content_type| content_type.to_ascii_lowercase().contains("xml"))
        .unwrap_or(false);
    if !is_xml {
        return false;
    }

    match headers
        .get(http::header::CONTENT_ENCODING)
        .and_then(|value| value.to_str().ok())
    {
        Some(encoding) => encoding.trim().is_empty() || encoding.eq_ignore_ascii_case("identity"),
        None => true,
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

async fn fix_s3_error_message_in_xml<RestBody>(body: RestBody) -> Result<(RestBody, bool), RestBody::Error>
where
    RestBody: Body<Data = Bytes> + From<Bytes>,
{
    let bytes = BodyExt::collect(body).await?.to_bytes();
    let xml = String::from_utf8(bytes.to_vec()).unwrap_or_else(|_| String::from_utf8_lossy(&bytes).into_owned());
    let (fixed, changed) = insert_missing_signature_error_message(xml);
    Ok((RestBody::from(Bytes::from(fixed)), changed))
}

fn insert_missing_signature_error_message(mut xml: String) -> (String, bool) {
    if !xml.contains("<Code>SignatureDoesNotMatch</Code>") || xml.contains("<Message>") {
        return (xml, false);
    }

    let Some(code_end) = xml.find("</Code>") else {
        return (xml, false);
    };

    let message = ApiError::error_code_to_message(&S3ErrorCode::SignatureDoesNotMatch);
    xml.insert_str(code_end + "</Code>".len(), &format!("<Message>{message}</Message>"));
    (xml, true)
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
        || path.starts_with(MINIO_ADMIN_PREFIX)
        || path.starts_with(RUSTFS_ADMIN_PREFIX)
        || path.starts_with(MINIO_ADMIN_V3_PREFIX)
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
        let cors_origins = get_env_opt_str(rustfs_config::ENV_CORS_ALLOWED_ORIGINS).filter(|s| !s.is_empty());
        Self { cors_origins }
    }

    /// Exact paths that should be excluded from being treated as S3 paths.
    const EXCLUDED_EXACT_PATHS: &'static [&'static str] = &["/health", "/health/ready", "/profile/cpu", "/profile/memory"];

    fn is_s3_path(path: &str) -> bool {
        // Exclude Admin, Console, RPC, and configured special paths
        !path.starts_with(ADMIN_PREFIX)
            && !path.starts_with(MINIO_ADMIN_PREFIX)
            && !path.starts_with(RPC_PREFIX)
            && !is_console_path(path)
            && !Self::EXCLUDED_EXACT_PATHS.contains(&path)
    }

    fn apply_cors_headers(&self, request_headers: &HeaderMap, response_headers: &mut HeaderMap) {
        let Some(origin) = request_headers.get(cors::standard::ORIGIN).and_then(|v| v.to_str().ok()) else {
            return;
        };
        let Some(config) = self
            .cors_origins
            .as_deref()
            .map(str::trim)
            .filter(|config| !config.is_empty())
        else {
            return;
        };

        let (allow_origin, allow_credentials) = if config == "*" {
            (HeaderValue::from_static("*"), false)
        } else if config.split(',').map(str::trim).any(|allowed| allowed == origin) {
            let Ok(origin) = HeaderValue::from_str(origin) else {
                return;
            };
            (origin, true)
        } else {
            return;
        };

        response_headers.insert(cors::response::ACCESS_CONTROL_ALLOW_ORIGIN, allow_origin);

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

        // Credentials are only safe for origins matched from an explicit allow-list.
        if allow_credentials {
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

async fn resolve_s3_options_cors_headers(bucket: &str, request_headers: &HeaderMap) -> Option<HeaderMap> {
    apply_cors_headers(bucket, &http::Method::OPTIONS, request_headers).await
}

fn clear_cors_response_headers(headers: &mut HeaderMap) {
    headers.remove(cors::response::ACCESS_CONTROL_ALLOW_ORIGIN);
    headers.remove(cors::response::ACCESS_CONTROL_ALLOW_METHODS);
    headers.remove(cors::response::ACCESS_CONTROL_ALLOW_HEADERS);
    headers.remove(cors::response::ACCESS_CONTROL_EXPOSE_HEADERS);
    headers.remove(cors::response::ACCESS_CONTROL_ALLOW_CREDENTIALS);
    headers.remove(cors::response::ACCESS_CONTROL_MAX_AGE);
}

fn apply_bucket_cors_result(response_headers: &mut HeaderMap, bucket_cors_headers: &HeaderMap) {
    // Bucket-level CORS is authoritative for S3 object/bucket paths.
    // Clear any previously-populated CORS response headers (e.g. generic/system defaults),
    // then apply the evaluated bucket result (which may be intentionally empty).
    clear_cors_response_headers(response_headers);
    for (key, value) in bucket_cors_headers.iter() {
        response_headers.insert(key, value.clone());
    }
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
        let is_s3 = ConditionalCorsLayer::is_s3_path(&path);
        let is_root = path == "/";

        if method == Method::OPTIONS {
            let has_acrm = request_headers.contains_key(cors::request::ACCESS_CONTROL_REQUEST_METHOD);

            if is_root {
                return Box::pin(async move {
                    if !has_acrm || !request_headers.contains_key(cors::standard::ORIGIN) {
                        return Ok(Response::builder()
                            .status(StatusCode::BAD_REQUEST)
                            .body(ResBody::default())
                            .unwrap());
                    }

                    let mut response = Response::builder().status(StatusCode::OK).body(ResBody::default()).unwrap();
                    let cors_layer = ConditionalCorsLayer {
                        cors_origins: (*cors_origins).clone(),
                    };
                    cors_layer.apply_cors_headers(&request_headers, response.headers_mut());
                    Ok(response)
                });
            }

            if is_s3 {
                let path_trimmed = path.trim_start_matches('/');
                let bucket = path_trimmed.split('/').next().unwrap_or("").to_string();

                return Box::pin(async move {
                    if !has_acrm || !request_headers.contains_key(cors::standard::ORIGIN) {
                        return Ok(Response::builder()
                            .status(StatusCode::BAD_REQUEST)
                            .body(ResBody::default())
                            .unwrap());
                    }

                    let cors_layer = ConditionalCorsLayer {
                        cors_origins: (*cors_origins).clone(),
                    };

                    if let Some(cors_headers) = resolve_s3_options_cors_headers(&bucket, &request_headers).await {
                        let cors_allowed = cors_headers.contains_key(cors::response::ACCESS_CONTROL_ALLOW_ORIGIN);
                        let status = if cors_allowed { StatusCode::OK } else { StatusCode::FORBIDDEN };

                        let mut response = Response::builder().status(status).body(ResBody::default()).unwrap();
                        if cors_allowed {
                            for (key, value) in cors_headers.iter() {
                                response.headers_mut().insert(key, value.clone());
                            }
                        }
                        return Ok(response);
                    }

                    // No bucket-level CORS config: fall back to global/default CORS behavior.
                    let mut response = Response::builder().status(StatusCode::OK).body(ResBody::default()).unwrap();
                    cors_layer.apply_cors_headers(&request_headers, response.headers_mut());
                    Ok(response)
                });
            }

            let request_headers_clone = request_headers.clone();
            return Box::pin(async move {
                let mut response = Response::builder().status(StatusCode::OK).body(ResBody::default()).unwrap();
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

            if request_headers.contains_key(cors::standard::ORIGIN)
                && !response.headers().contains_key(cors::response::ACCESS_CONTROL_ALLOW_ORIGIN)
            {
                let cors_layer = ConditionalCorsLayer {
                    cors_origins: (*cors_origins).clone(),
                };

                if is_s3 {
                    let bucket = path.trim_start_matches('/').split('/').next().unwrap_or("");
                    if path == "/" {
                        cors_layer.apply_cors_headers(&request_headers, response.headers_mut());
                    } else if !bucket.is_empty() {
                        match apply_cors_headers(bucket, &method, &request_headers).await {
                            Some(bucket_cors_headers) => {
                                // Bucket-level CORS is authoritative when configured, even if it
                                // intentionally resolves to an empty header set (no rule match).
                                apply_bucket_cors_result(response.headers_mut(), &bucket_cors_headers);
                            }
                            None => {
                                // No bucket-level CORS config: fall back to global/default policy.
                                cors_layer.apply_cors_headers(&request_headers, response.headers_mut());
                            }
                        }
                    } else {
                        cors_layer.apply_cors_headers(&request_headers, response.headers_mut());
                    }
                } else {
                    cors_layer.apply_cors_headers(&request_headers, response.headers_mut());
                }
            }

            Ok(response)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::future::{Ready, ready};
    use http::Request;
    use http_body_util::BodyExt;
    use http_body_util::Full;
    use std::convert::Infallible;
    use std::sync::Mutex;
    use temp_env::with_var;

    #[derive(Clone, Debug)]
    struct CaptureService;

    impl<B> Service<Request<B>> for CaptureService {
        type Response = Request<B>;
        type Error = Infallible;
        type Future = Ready<Result<Self::Response, Self::Error>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, req: Request<B>) -> Self::Future {
            ready(Ok(req))
        }
    }

    #[derive(Clone, Default)]
    struct HeaderCaptureService {
        headers: Arc<Mutex<Option<HeaderMap>>>,
    }

    impl HeaderCaptureService {
        fn headers(&self) -> Arc<Mutex<Option<HeaderMap>>> {
            Arc::clone(&self.headers)
        }
    }

    impl<B: Send + 'static> Service<Request<B>> for HeaderCaptureService {
        type Response = Response<Full<Bytes>>;
        type Error = Infallible;
        type Future = Ready<Result<Self::Response, Self::Error>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, req: Request<B>) -> Self::Future {
            *self.headers.lock().expect("capture headers") = Some(req.headers().clone());
            ready(Ok(Response::new(Full::from(Bytes::new()))))
        }
    }

    #[test]
    fn admin_chunked_put_without_content_length_is_normalized() {
        let request = Request::builder()
            .method(Method::PUT)
            .uri("/minio/admin/v3/set-user-status?accessKey=test&status=enabled")
            .body(())
            .expect("request");

        assert!(should_force_zero_content_length_for_admin_empty_body(&request));
    }

    #[test]
    fn admin_empty_body_post_without_content_length_is_normalized() {
        let paths = [
            "/minio/admin/v3/rebalance/start",
            "/minio/admin/v3/rebalance/stop",
            "/minio/admin/v3/pools/decommission?pool=http%3A%2F%2Fminio-%7B1...4%7D%3A9000%2Fdata%7B1...2%7D",
            "/minio/admin/v3/pools/cancel?pool=http%3A%2F%2Fminio-%7B1...4%7D%3A9000%2Fdata%7B1...2%7D",
            "/rustfs/admin/v3/rebalance/start",
            "/rustfs/admin/v3/rebalance/stop",
            "/rustfs/admin/v3/pools/decommission?pool=http%3A%2F%2Fminio-%7B1...4%7D%3A9000%2Fdata%7B1...2%7D",
            "/rustfs/admin/v3/pools/cancel?pool=http%3A%2F%2Fminio-%7B1...4%7D%3A9000%2Fdata%7B1...2%7D",
        ];

        for path in paths {
            let request = Request::builder().method(Method::POST).uri(path).body(()).expect("request");

            assert!(
                should_force_zero_content_length_for_admin_empty_body(&request),
                "{path} should force Content-Length: 0"
            );
        }
    }

    #[tokio::test]
    async fn admin_empty_body_post_layer_inserts_zero_content_length() {
        let capture = HeaderCaptureService::default();
        let headers = capture.headers();
        let mut service = AdminChunkedContentLengthCompatLayer.layer(capture);
        let request = Request::builder()
            .method(Method::POST)
            .uri("/rustfs/admin/v3/rebalance/start")
            .body(())
            .expect("request");

        let _ = service.call(request).await.expect("service call");

        let headers = headers.lock().expect("captured headers").take().expect("captured headers");
        assert_eq!(headers.get(http::header::CONTENT_LENGTH).unwrap(), "0");
    }

    #[test]
    fn admin_request_with_explicit_content_length_is_left_unchanged() {
        let request = Request::builder()
            .method(Method::PUT)
            .uri("/minio/admin/v3/set-group-status?group=test&status=enabled")
            .header(http::header::CONTENT_LENGTH, "0")
            .body(())
            .expect("request");

        assert!(!should_force_zero_content_length_for_admin_empty_body(&request));
    }

    #[test]
    fn non_admin_chunked_put_is_not_normalized() {
        let request = Request::builder()
            .method(Method::PUT)
            .uri("/bucket/object")
            .body(())
            .expect("request");

        assert!(!should_force_zero_content_length_for_admin_empty_body(&request));
    }

    #[test]
    fn non_empty_body_admin_post_path_is_not_normalized() {
        let request = Request::builder()
            .method(Method::POST)
            .uri("/minio/admin/v3/update-service-account")
            .body(())
            .expect("request");

        assert!(!should_force_zero_content_length_for_admin_empty_body(&request));
    }

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
        let input =
            String::from("<GetObjectAttributesOutput><ETag>\"first\"</ETag><ETag>\"second\"</ETag></GetObjectAttributesOutput>");
        let output = strip_quotes_from_first_etag(input);

        assert_eq!(
            output,
            "<GetObjectAttributesOutput><ETag>first</ETag><ETag>\"second\"</ETag></GetObjectAttributesOutput>"
        );
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

    #[tokio::test]
    async fn test_fix_s3_error_message_in_xml_reports_changed_body() {
        let body = Full::from(Bytes::from_static(b"<Error><Code>SignatureDoesNotMatch</Code></Error>"));

        let (fixed, changed) = fix_s3_error_message_in_xml(body).await.unwrap();
        let bytes = BodyExt::collect(fixed).await.unwrap().to_bytes();

        assert!(changed);
        assert!(bytes.starts_with(b"<Error><Code>SignatureDoesNotMatch</Code><Message>"));
        assert!(bytes.ends_with(b"</Message></Error>"));
    }

    #[tokio::test]
    async fn test_fix_s3_error_message_in_xml_reports_unchanged_body() {
        let input = Bytes::from_static(b"<Error><Code>AccessDenied</Code></Error>");
        let body = Full::from(input.clone());

        let (fixed, changed) = fix_s3_error_message_in_xml(body).await.unwrap();
        let bytes = BodyExt::collect(fixed).await.unwrap().to_bytes();

        assert!(!changed);
        assert_eq!(bytes, input);
    }

    #[test]
    fn test_insert_missing_signature_error_message() {
        let (fixed, changed) =
            insert_missing_signature_error_message("<Error><Code>SignatureDoesNotMatch</Code></Error>".to_string());

        assert!(changed);
        assert!(fixed.contains("<Code>SignatureDoesNotMatch</Code><Message>The request signature we calculated does not match the signature you provided."));
    }

    #[test]
    fn test_insert_missing_signature_error_message_preserves_existing_message() {
        let input = "<Error><Code>SignatureDoesNotMatch</Code><Message>custom</Message></Error>".to_string();
        let (fixed, changed) = insert_missing_signature_error_message(input.clone());

        assert!(!changed);
        assert_eq!(fixed, input);
    }

    #[test]
    fn test_is_s3_path_excludes_admin_and_special_paths() {
        assert!(ConditionalCorsLayer::is_s3_path("/my-bucket/key"));
        assert!(ConditionalCorsLayer::is_s3_path("/"));
        assert!(!ConditionalCorsLayer::is_s3_path("/rustfs/admin/v3/info"));
        assert!(!ConditionalCorsLayer::is_s3_path("/minio/admin/v3/info"));
        assert!(!ConditionalCorsLayer::is_s3_path("/health"));
        assert!(!ConditionalCorsLayer::is_s3_path("/health/ready"));
    }

    #[test]
    fn test_generic_cors_layer_omits_headers_without_configured_origins() {
        let cors = ConditionalCorsLayer { cors_origins: None };
        let mut req_headers = HeaderMap::new();
        req_headers.insert("origin", "https://example.com".parse().unwrap());

        let mut resp_headers = HeaderMap::new();
        cors.apply_cors_headers(&req_headers, &mut resp_headers);

        assert!(resp_headers.get(cors::response::ACCESS_CONTROL_ALLOW_ORIGIN).is_none());
        assert!(resp_headers.get(cors::response::ACCESS_CONTROL_ALLOW_CREDENTIALS).is_none());
        assert!(resp_headers.get(cors::response::ACCESS_CONTROL_ALLOW_METHODS).is_none());
        assert!(resp_headers.get(cors::response::ACCESS_CONTROL_ALLOW_HEADERS).is_none());
        assert!(resp_headers.get(cors::response::ACCESS_CONTROL_EXPOSE_HEADERS).is_none());
    }

    #[test]
    fn test_generic_cors_layer_respects_configured_origins() {
        let cors = ConditionalCorsLayer {
            cors_origins: Some("https://allowed.com".to_string()),
        };

        let mut req_headers = HeaderMap::new();
        req_headers.insert("origin", "https://denied.com".parse().unwrap());
        let mut resp_headers = HeaderMap::new();
        cors.apply_cors_headers(&req_headers, &mut resp_headers);
        assert!(resp_headers.get(cors::response::ACCESS_CONTROL_ALLOW_ORIGIN).is_none());

        let mut req_headers = HeaderMap::new();
        req_headers.insert("origin", "https://allowed.com".parse().unwrap());
        let mut resp_headers = HeaderMap::new();
        cors.apply_cors_headers(&req_headers, &mut resp_headers);
        assert_eq!(
            resp_headers.get(cors::response::ACCESS_CONTROL_ALLOW_ORIGIN).unwrap(),
            "https://allowed.com"
        );
        assert_eq!(resp_headers.get(cors::response::ACCESS_CONTROL_ALLOW_CREDENTIALS).unwrap(), "true");
    }

    #[test]
    fn test_generic_cors_layer_wildcard_does_not_allow_credentials() {
        let cors = ConditionalCorsLayer {
            cors_origins: Some("*".to_string()),
        };

        let mut req_headers = HeaderMap::new();
        req_headers.insert("origin", "https://example.com".parse().unwrap());
        let mut resp_headers = HeaderMap::new();
        cors.apply_cors_headers(&req_headers, &mut resp_headers);

        assert_eq!(resp_headers.get(cors::response::ACCESS_CONTROL_ALLOW_ORIGIN).unwrap(), "*");
        assert!(resp_headers.get(cors::response::ACCESS_CONTROL_ALLOW_CREDENTIALS).is_none());
    }

    #[test]
    fn test_conditional_cors_layer_reads_env() {
        with_var(rustfs_config::ENV_CORS_ALLOWED_ORIGINS, Some("https://allowed.com"), || {
            let cors = ConditionalCorsLayer::new();
            assert_eq!(cors.cors_origins.as_deref(), Some("https://allowed.com"));
        });
    }

    #[test]
    fn request_context_layer_populates_context_and_s3_request_id_from_x_request_id() {
        let mut service = RequestContextLayer.layer(CaptureService);
        let request = Request::builder()
            .uri("/bucket/object")
            .header("x-request-id", "req-123")
            .body(())
            .expect("request");

        let request = service.call(request).into_inner().expect("service call should succeed");
        let context = request
            .extensions()
            .get::<RequestContext>()
            .expect("request context should be present");

        assert_eq!(context.request_id, "req-123");
        assert_eq!(context.x_amz_request_id, "req-123");
        assert!(context.trace_id.is_none());
        assert!(context.span_id.is_none());
        assert_eq!(request.headers().get(AMZ_REQUEST_ID).unwrap(), "req-123");
    }

    #[test]
    fn request_context_layer_preserves_upstream_s3_request_id() {
        let mut service = RequestContextLayer.layer(CaptureService);
        let request = Request::builder()
            .uri("/bucket/object")
            .header("x-request-id", "req-123")
            .header(AMZ_REQUEST_ID, "amz-456")
            .body(())
            .expect("request");

        let request = service.call(request).into_inner().expect("service call should succeed");
        let context = request
            .extensions()
            .get::<RequestContext>()
            .expect("request context should be present");

        assert_eq!(context.request_id, "req-123");
        assert_eq!(context.x_amz_request_id, "amz-456");
        assert_eq!(request.headers().get(AMZ_REQUEST_ID).unwrap(), "amz-456");
    }

    #[tokio::test]
    async fn test_resolve_s3_options_cors_headers_no_headers_without_match() {
        let mut req_headers = HeaderMap::new();
        req_headers.insert("origin", "https://example.com".parse().unwrap());
        req_headers.insert("access-control-request-method", "GET".parse().unwrap());

        let headers = resolve_s3_options_cors_headers("bbb", &req_headers).await;
        assert!(headers.is_none());
    }

    #[test]
    fn test_apply_bucket_cors_result_clears_existing_cors_headers_with_empty_result() {
        let mut response_headers = HeaderMap::new();
        response_headers.insert(
            cors::response::ACCESS_CONTROL_ALLOW_ORIGIN,
            HeaderValue::from_static("https://foo.example"),
        );
        response_headers.insert(
            cors::response::ACCESS_CONTROL_ALLOW_METHODS,
            HeaderValue::from_static("GET, POST, PUT, DELETE, OPTIONS, HEAD"),
        );
        response_headers.insert(cors::response::ACCESS_CONTROL_ALLOW_HEADERS, HeaderValue::from_static("*"));
        response_headers.insert(cors::response::ACCESS_CONTROL_EXPOSE_HEADERS, HeaderValue::from_static("etag"));
        response_headers.insert(cors::response::ACCESS_CONTROL_ALLOW_CREDENTIALS, HeaderValue::from_static("true"));
        response_headers.insert(cors::response::ACCESS_CONTROL_MAX_AGE, HeaderValue::from_static("3600"));

        let bucket_cors_headers = HeaderMap::new();
        apply_bucket_cors_result(&mut response_headers, &bucket_cors_headers);

        assert!(response_headers.get(cors::response::ACCESS_CONTROL_ALLOW_ORIGIN).is_none());
        assert!(response_headers.get(cors::response::ACCESS_CONTROL_ALLOW_METHODS).is_none());
        assert!(response_headers.get(cors::response::ACCESS_CONTROL_ALLOW_HEADERS).is_none());
        assert!(response_headers.get(cors::response::ACCESS_CONTROL_EXPOSE_HEADERS).is_none());
        assert!(
            response_headers
                .get(cors::response::ACCESS_CONTROL_ALLOW_CREDENTIALS)
                .is_none()
        );
        assert!(response_headers.get(cors::response::ACCESS_CONTROL_MAX_AGE).is_none());
    }

    mod bodyless_status_fix {
        use super::*;
        use crate::server::hybrid::HybridBody;
        use http_body_util::Empty;

        // The production service takes `Request<Incoming>`, but `Incoming` can't be
        // constructed in unit tests. `BodylessStatusFixService` doesn't inspect the
        // request body, so parameterising over an arbitrary `B` is safe here.
        #[derive(Clone)]
        struct FixedResponse {
            status: StatusCode,
            body: Bytes,
            content_type: Option<&'static str>,
        }

        impl<B: Send + 'static> Service<Request<B>> for FixedResponse {
            type Response = Response<HybridBody<Full<Bytes>, Empty<Bytes>>>;
            type Error = Infallible;
            type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

            fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
                Poll::Ready(Ok(()))
            }

            fn call(&mut self, _req: Request<B>) -> Self::Future {
                let this = self.clone();
                Box::pin(async move {
                    let body = this.body.clone();
                    let len = body.len();
                    let mut builder = Response::builder().status(this.status);
                    builder = builder.header(http::header::CONTENT_LENGTH, len.to_string());
                    if let Some(ct) = this.content_type {
                        builder = builder.header(http::header::CONTENT_TYPE, ct);
                    }
                    builder = builder.header(http::header::ETAG, "\"abc123\"");
                    Ok(builder
                        .body(HybridBody::Rest {
                            rest_body: Full::from(body),
                        })
                        .expect("build response"))
                })
            }
        }

        fn empty_request() -> Request<()> {
            Request::builder().uri("/").body(()).expect("request")
        }

        async fn collect_body<B: Body<Data = Bytes>>(body: B) -> Bytes
        where
            B::Error: std::fmt::Debug,
        {
            BodyExt::collect(body).await.expect("collect body").to_bytes()
        }

        #[tokio::test]
        async fn strips_body_and_content_headers_for_304() {
            let mut svc = BodylessStatusFixLayer.layer(FixedResponse {
                status: StatusCode::NOT_MODIFIED,
                body: Bytes::from_static(b"<Error><Code>NotModified</Code></Error>"),
                content_type: Some("application/xml"),
            });

            let res = svc.call(empty_request()).await.expect("service call");
            let (parts, body) = res.into_parts();

            assert_eq!(parts.status, StatusCode::NOT_MODIFIED);
            assert!(parts.headers.get(http::header::CONTENT_LENGTH).is_none());
            assert!(parts.headers.get(http::header::CONTENT_TYPE).is_none());
            assert_eq!(parts.headers.get(http::header::ETAG).unwrap(), "\"abc123\"");

            let bytes = collect_body(body).await;
            assert!(bytes.is_empty(), "304 response body must be empty");
        }

        #[tokio::test]
        async fn strips_body_for_204() {
            let mut svc = BodylessStatusFixLayer.layer(FixedResponse {
                status: StatusCode::NO_CONTENT,
                body: Bytes::from_static(b"unexpected"),
                content_type: None,
            });

            let res = svc.call(empty_request()).await.expect("service call");
            let (parts, body) = res.into_parts();

            assert_eq!(parts.status, StatusCode::NO_CONTENT);
            assert!(parts.headers.get(http::header::CONTENT_LENGTH).is_none());

            let bytes = collect_body(body).await;
            assert!(bytes.is_empty());
        }

        #[tokio::test]
        async fn preserves_body_for_200() {
            let payload = Bytes::from_static(b"hello");
            let mut svc = BodylessStatusFixLayer.layer(FixedResponse {
                status: StatusCode::OK,
                body: payload.clone(),
                content_type: Some("text/plain"),
            });

            let res = svc.call(empty_request()).await.expect("service call");
            let (parts, body) = res.into_parts();

            assert_eq!(parts.status, StatusCode::OK);
            assert_eq!(parts.headers.get(http::header::CONTENT_TYPE).unwrap(), "text/plain");
            assert_eq!(
                parts.headers.get(http::header::CONTENT_LENGTH).unwrap(),
                payload.len().to_string().as_str()
            );

            let bytes = collect_body(body).await;
            assert_eq!(bytes, payload);
        }

        #[test]
        fn is_bodyless_status_matches_rfc9110_statuses() {
            assert!(is_bodyless_status(StatusCode::CONTINUE));
            assert!(is_bodyless_status(StatusCode::SWITCHING_PROTOCOLS));
            assert!(is_bodyless_status(StatusCode::NO_CONTENT));
            assert!(is_bodyless_status(StatusCode::RESET_CONTENT));
            assert!(is_bodyless_status(StatusCode::NOT_MODIFIED));

            assert!(!is_bodyless_status(StatusCode::OK));
            assert!(!is_bodyless_status(StatusCode::PARTIAL_CONTENT));
            assert!(!is_bodyless_status(StatusCode::NOT_FOUND));
            assert!(!is_bodyless_status(StatusCode::PRECONDITION_FAILED));
            assert!(!is_bodyless_status(StatusCode::INTERNAL_SERVER_ERROR));
        }
    }

    mod head_request_body_fix {
        use super::*;
        use crate::server::hybrid::HybridBody;
        use http_body_util::Empty;

        #[derive(Clone)]
        struct FixedResponse {
            status: StatusCode,
            body: Bytes,
            content_type: Option<&'static str>,
        }

        impl<B: Send + 'static> Service<Request<B>> for FixedResponse {
            type Response = Response<HybridBody<Full<Bytes>, Empty<Bytes>>>;
            type Error = Infallible;
            type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

            fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
                Poll::Ready(Ok(()))
            }

            fn call(&mut self, _req: Request<B>) -> Self::Future {
                let this = self.clone();
                Box::pin(async move {
                    let body = this.body.clone();
                    let len = body.len();
                    let mut builder = Response::builder().status(this.status);
                    builder = builder.header(http::header::CONTENT_LENGTH, len.to_string());
                    builder = builder.header(http::header::TRANSFER_ENCODING, "chunked");
                    if let Some(ct) = this.content_type {
                        builder = builder.header(http::header::CONTENT_TYPE, ct);
                    }
                    Ok(builder
                        .body(HybridBody::Rest {
                            rest_body: Full::from(body),
                        })
                        .expect("build response"))
                })
            }
        }

        fn request_with_method(method: Method) -> Request<()> {
            Request::builder()
                .method(method)
                .uri("/bucket/object")
                .body(())
                .expect("request")
        }

        async fn collect_body<B: Body<Data = Bytes>>(body: B) -> Bytes
        where
            B::Error: std::fmt::Debug,
        {
            BodyExt::collect(body).await.expect("collect body").to_bytes()
        }

        #[tokio::test]
        async fn strips_body_for_head_errors_but_preserves_metadata_headers() {
            let payload = Bytes::from_static(b"<?xml version=\"1.0\"?><Error><Code>NoSuchKey</Code></Error>");
            let mut svc = HeadRequestBodyFixLayer.layer(FixedResponse {
                status: StatusCode::NOT_FOUND,
                body: payload.clone(),
                content_type: Some("application/xml"),
            });

            let res = svc.call(request_with_method(Method::HEAD)).await.expect("service call");
            let (parts, body) = res.into_parts();

            assert_eq!(parts.status, StatusCode::NOT_FOUND);
            assert_eq!(
                parts.headers.get(http::header::CONTENT_LENGTH).unwrap(),
                payload.len().to_string().as_str()
            );
            assert_eq!(parts.headers.get(http::header::CONTENT_TYPE).unwrap(), "application/xml");
            assert!(parts.headers.get(http::header::TRANSFER_ENCODING).is_none());

            let bytes = collect_body(body).await;
            assert!(bytes.is_empty(), "HEAD response body must be empty");
        }

        #[tokio::test]
        async fn preserves_body_for_get_errors() {
            let payload = Bytes::from_static(b"<?xml version=\"1.0\"?><Error><Code>NoSuchKey</Code></Error>");
            let mut svc = HeadRequestBodyFixLayer.layer(FixedResponse {
                status: StatusCode::NOT_FOUND,
                body: payload.clone(),
                content_type: Some("application/xml"),
            });

            let res = svc.call(request_with_method(Method::GET)).await.expect("service call");
            let (parts, body) = res.into_parts();

            assert_eq!(parts.status, StatusCode::NOT_FOUND);
            assert_eq!(
                parts.headers.get(http::header::CONTENT_LENGTH).unwrap(),
                payload.len().to_string().as_str()
            );
            assert_eq!(parts.headers.get(http::header::TRANSFER_ENCODING).unwrap(), "chunked");

            let bytes = collect_body(body).await;
            assert_eq!(bytes, payload);
        }
    }

    #[test]
    fn test_apply_bucket_cors_result_replaces_existing_cors_headers() {
        let mut response_headers = HeaderMap::new();
        response_headers.insert(
            cors::response::ACCESS_CONTROL_ALLOW_ORIGIN,
            HeaderValue::from_static("https://foo.example"),
        );
        response_headers.insert(
            cors::response::ACCESS_CONTROL_ALLOW_METHODS,
            HeaderValue::from_static("GET, POST, PUT, DELETE, OPTIONS, HEAD"),
        );

        let mut bucket_cors_headers = HeaderMap::new();
        bucket_cors_headers.insert(
            cors::response::ACCESS_CONTROL_ALLOW_ORIGIN,
            HeaderValue::from_static("https://allowed.example"),
        );
        bucket_cors_headers.insert(cors::response::ACCESS_CONTROL_ALLOW_METHODS, HeaderValue::from_static("GET"));

        apply_bucket_cors_result(&mut response_headers, &bucket_cors_headers);

        assert_eq!(
            response_headers.get(cors::response::ACCESS_CONTROL_ALLOW_ORIGIN).unwrap(),
            "https://allowed.example"
        );
        assert_eq!(response_headers.get(cors::response::ACCESS_CONTROL_ALLOW_METHODS).unwrap(), "GET");
    }
}
