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

use super::runtime_sources;
use crate::admin::console::is_console_path;
use crate::app::object_traffic_health::ObjectTrafficHealth;
use crate::error::ApiError;
use crate::server::RemoteAddr;
use crate::server::cors;
use crate::server::hybrid::{HybridBody, is_grpc_request};
use crate::server::{
    ADMIN_PREFIX, CONSOLE_PREFIX, HEALTH_COMPAT_LIVE_PATH, HEALTH_PREFIX, HEALTH_READY_PATH, HealthProbe, MINIO_ADMIN_PREFIX,
    MINIO_ADMIN_V3_PREFIX, MINIO_HEALTH_CLUSTER_PATH, MINIO_HEALTH_CLUSTER_READ_PATH, MINIO_HEALTH_LIVE_PATH,
    MINIO_HEALTH_READY_PATH, PROFILE_CPU_PATH, PROFILE_MEMORY_PATH, RPC_PREFIX, RUSTFS_ADMIN_PREFIX, active_http_requests,
    build_health_response_parts, collect_probe_readiness, has_path_prefix, is_admin_path, is_table_catalog_path,
    kms_probe_staleness_limit, kms_ready_from_probe,
};
use crate::storage_api::server::layer::apply_cors_headers;
use crate::storage_api::server::layer::request_context::{RequestContext, extract_request_id_from_headers, spawn_traced};
use bytes::{Bytes, BytesMut};
use futures::future::Either;
use http::{HeaderMap, HeaderValue, Method, Request as HttpRequest, Response, StatusCode, Uri};
use http_body::Body;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use pin_project_lite::pin_project;
use quick_xml::events::Event;
use rustfs_obs::HTTP_SERVER_LOG_TARGET;
#[cfg(feature = "swift")]
use rustfs_protocols::swift::SwiftRouter;
use rustfs_trusted_proxies::ClientInfo;
use rustfs_utils::get_env_opt_str;
use rustfs_utils::http::headers::{AMZ_REQUEST_ID, REQUEST_ID_HEADER};
use s3s::S3ErrorCode;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::future::Future;
use std::net::{IpAddr, SocketAddr};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;
use tower::{Layer, Service};
use tracing::{Level, debug, error, info, warn};
use url::form_urlencoded;

const HTTP_REQUEST_COMPLETED_EVENT: &str = "http_request_completed";
const HTTP_REQUEST_FAILED_EVENT: &str = "http_request_failed";
const HTTP_REQUEST_INFLIGHT_SLOW_EVENT: &str = "http_request_inflight_slow";
const LOG_COMPONENT_SERVER: &str = "server";
const LOG_SUBSYSTEM_HTTP: &str = "http";
const REDACTED_QUERY_VALUE: &str = "redacted";
const OBJECT_ZIP_DOWNLOADS_PATH: &str = "/v3/object-zip-downloads/";
const HTTP_REQUEST_INFLIGHT_WARN_THRESHOLD: Duration = Duration::from_secs(5);
const STS_RESPONSE_METADATA_TAG: &str = "ResponseMetadata";
const STS_REQUEST_ID_TAG: &str = "RequestId";
const STS_SUCCESS_RESPONSE_TAGS: [&str; 2] = ["AssumeRoleResponse", "AssumeRoleWithWebIdentityResponse"];
#[cfg(feature = "swift")]
const SWIFT_API_PATH_PREFIX: &str = "/v1/";

pub(crate) fn redact_sensitive_uri_query(uri: &http::Uri) -> String {
    let path = uri.path();
    if !is_object_zip_download_path(path) {
        return uri.to_string();
    }

    let Some(query) = uri.query() else {
        return uri.to_string();
    };

    let mut redacted_token = false;
    let mut serializer = form_urlencoded::Serializer::new(String::new());
    for (key, value) in form_urlencoded::parse(query.as_bytes()) {
        if key == "token" {
            redacted_token = true;
            serializer.append_pair(&key, REDACTED_QUERY_VALUE);
        } else {
            serializer.append_pair(&key, &value);
        }
    }

    if !redacted_token {
        return uri.to_string();
    }

    let redacted_query = serializer.finish();
    let path_and_query = if redacted_query.is_empty() {
        path.to_string()
    } else {
        format!("{path}?{redacted_query}")
    };
    let mut parts = uri.clone().into_parts();
    match path_and_query.parse() {
        Ok(path_and_query) => {
            parts.path_and_query = Some(path_and_query);
            http::Uri::from_parts(parts)
                .map(|uri| uri.to_string())
                .unwrap_or_else(|_| uri.to_string())
        }
        Err(_) => uri.to_string(),
    }
}

fn is_object_zip_download_path(path: &str) -> bool {
    (path.starts_with(ADMIN_PREFIX) || path.starts_with(MINIO_ADMIN_PREFIX))
        && path.contains(OBJECT_ZIP_DOWNLOADS_PATH)
        && path.ends_with(".zip")
}

/// Tower middleware layer that creates a canonical [`RequestContext`] from HTTP headers
/// and injects it into `request.extensions()`.
///
/// This layer must be placed after `SetRequestIdLayer` in the middleware stack,
/// as it reads the `x-request-id` header that `SetRequestIdLayer` generates.
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
        let request_context = RequestContext::from_headers(req.headers());
        req.extensions_mut().insert(request_context);

        self.inner.call(req)
    }
}

fn uses_server_owned_s3_request_id<B>(req: &HttpRequest<B>, console_redirect_enabled: bool) -> bool {
    if is_grpc_request(req)
        || req.uri().path().starts_with(RPC_PREFIX)
        || is_sts_query_request(req.method(), req.uri(), req.headers())
        || (console_redirect_enabled && is_console_redirect_request(req))
    {
        return false;
    }

    #[cfg(feature = "swift")]
    if req.uri().path().starts_with(SWIFT_API_PATH_PREFIX) && SwiftRouter::new(true, None).matches(req.uri()) {
        return false;
    }

    let path = req.uri().path();
    let method = req.method();
    let is_admin_health_request =
        (method == Method::GET || method == Method::HEAD) && matches!(path, HEALTH_PREFIX | HEALTH_READY_PATH);
    let is_profile_request = method == Method::GET && matches!(path, PROFILE_CPU_PATH | PROFILE_MEMORY_PATH);
    let is_public_health_alias_request = (method == Method::GET || method == Method::HEAD)
        && matches!(
            path,
            HEALTH_COMPAT_LIVE_PATH
                | MINIO_HEALTH_LIVE_PATH
                | MINIO_HEALTH_READY_PATH
                | MINIO_HEALTH_CLUSTER_PATH
                | MINIO_HEALTH_CLUSTER_READ_PATH
        )
        && is_public_health_endpoint_request(method, path);

    !(is_admin_path(path)
        || is_console_path(path)
        || is_admin_health_request
        || is_profile_request
        || is_public_health_alias_request)
}

/// Creates a server-owned context and response ID for S3 requests while
/// preserving the existing request-ID propagation contract for non-S3 routes.
#[derive(Clone, Default)]
pub struct ExternalRequestContextLayer {
    console_redirect_enabled: bool,
}

impl ExternalRequestContextLayer {
    pub(crate) fn new(console_redirect_enabled: bool) -> Self {
        Self {
            console_redirect_enabled,
        }
    }
}

impl<S> Layer<S> for ExternalRequestContextLayer {
    type Service = ExternalRequestContextService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        ExternalRequestContextService {
            inner,
            console_redirect_enabled: self.console_redirect_enabled,
        }
    }
}

#[derive(Clone)]
pub struct ExternalRequestContextService<S> {
    inner: S,
    console_redirect_enabled: bool,
}

impl<S, B, ResBody> Service<HttpRequest<B>> for ExternalRequestContextService<S>
where
    S: Service<HttpRequest<B>, Response = Response<ResBody>>,
{
    type Response = Response<ResBody>;
    type Error = S::Error;
    type Future = ExternalRequestContextFuture<S::Future>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut req: HttpRequest<B>) -> Self::Future {
        let is_s3 = uses_server_owned_s3_request_id(&req, self.console_redirect_enabled);
        let has_request_id = req.headers().contains_key(REQUEST_ID_HEADER);
        let request_context = if is_s3 {
            let request_context = RequestContext::from_external_headers(req.headers());
            if !has_request_id && let Ok(request_id) = HeaderValue::from_str(&request_context.request_id) {
                req.headers_mut().insert(REQUEST_ID_HEADER, request_id);
            }
            request_context
        } else {
            if !has_request_id {
                let request_id = uuid::Uuid::new_v4().to_string();
                if let Ok(request_id) = HeaderValue::from_str(&request_id) {
                    req.headers_mut().insert(REQUEST_ID_HEADER, request_id);
                }
            }
            RequestContext::from_propagated_headers(req.headers())
        };
        let request_id = if is_s3 {
            HeaderValue::from_str(&request_context.request_id).ok()
        } else {
            req.headers().get(REQUEST_ID_HEADER).cloned()
        };
        req.extensions_mut().insert(request_context);

        ExternalRequestContextFuture {
            inner: self.inner.call(req),
            request_id,
            is_s3,
        }
    }
}

pin_project! {
    pub struct ExternalRequestContextFuture<F> {
        #[pin]
        inner: F,
        request_id: Option<HeaderValue>,
        is_s3: bool,
    }
}

impl<F, ResBody, E> Future for ExternalRequestContextFuture<F>
where
    F: Future<Output = Result<Response<ResBody>, E>>,
{
    type Output = Result<Response<ResBody>, E>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let mut response = match this.inner.poll(cx) {
            Poll::Ready(Ok(response)) => response,
            Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
            Poll::Pending => return Poll::Pending,
        };

        if let Some(request_id) = this.request_id.take() {
            if *this.is_s3 {
                response.headers_mut().insert(REQUEST_ID_HEADER, request_id.clone());
                response.headers_mut().insert(AMZ_REQUEST_ID, request_id);
            } else if !response.headers().contains_key(REQUEST_ID_HEADER) {
                response.headers_mut().insert(REQUEST_ID_HEADER, request_id);
            }
        }

        Poll::Ready(Ok(response))
    }
}

#[derive(Clone, Default)]
pub struct RequestLoggingLayer;

impl<S> Layer<S> for RequestLoggingLayer {
    type Service = RequestLoggingService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        RequestLoggingService { inner }
    }
}

#[derive(Clone)]
pub struct RequestLoggingService<S> {
    inner: S,
}

#[derive(Clone, Debug)]
struct RequestLogContext {
    request_id: String,
    trace_id: Option<String>,
    span_id: Option<String>,
    client_ip: Option<IpAddr>,
    peer_addr: Option<SocketAddr>,
    method: Method,
    uri: Uri,
    request_started_at: Option<RequestContext>,
    fallback_start: Instant,
}

impl RequestLogContext {
    fn from_request<B>(req: &HttpRequest<B>) -> Self {
        let request_context = req.extensions().get::<RequestContext>().cloned();
        let request_id = request_context
            .as_ref()
            .map(|ctx| ctx.request_id.clone())
            .unwrap_or_else(|| extract_request_id_from_headers(req.headers()));
        Self {
            request_id,
            trace_id: request_context.as_ref().and_then(|ctx| ctx.trace_id.clone()),
            span_id: request_context.as_ref().and_then(|ctx| ctx.span_id.clone()),
            client_ip: req.extensions().get::<ClientInfo>().map(|info| info.real_ip),
            peer_addr: req.extensions().get::<RemoteAddr>().map(|addr| addr.0),
            method: req.method().clone(),
            uri: req.uri().clone(),
            request_started_at: request_context,
            fallback_start: Instant::now(),
        }
    }

    fn duration_ms(&self) -> u64 {
        self.request_started_at
            .as_ref()
            .map(RequestContext::duration_ms)
            .unwrap_or_else(|| self.fallback_start.elapsed().as_millis().try_into().unwrap_or(u64::MAX))
    }

    fn result_label(status: StatusCode) -> &'static str {
        if status.is_server_error() {
            "server_error"
        } else if status.is_client_error() {
            "client_error"
        } else if status.is_redirection() {
            "redirect"
        } else {
            "success"
        }
    }

    fn peer_addr(&self) -> String {
        self.client_ip
            .map(|addr| addr.to_string())
            .or_else(|| self.peer_addr.map(|addr| addr.to_string()))
            .unwrap_or_else(|| "unknown".to_string())
    }

    fn redacted_uri(&self) -> String {
        redact_sensitive_uri_query(&self.uri)
    }

    fn log_slow_inflight(&self) {
        if !tracing::enabled!(target: HTTP_SERVER_LOG_TARGET, Level::WARN) {
            return;
        }
        warn!(
            target: HTTP_SERVER_LOG_TARGET,
            event = HTTP_REQUEST_INFLIGHT_SLOW_EVENT,
            component = LOG_COMPONENT_SERVER,
            subsystem = LOG_SUBSYSTEM_HTTP,
            request_id = %self.request_id,
            trace_id = %self.trace_id.as_deref().unwrap_or("unknown"),
            span_id = %self.span_id.as_deref().unwrap_or("unknown"),
            peer_addr = %self.peer_addr(),
            method = %self.method.as_str(),
            uri = %self.redacted_uri(),
            duration_ms = self.duration_ms(),
            active_requests = active_http_requests(),
            threshold_ms = HTTP_REQUEST_INFLIGHT_WARN_THRESHOLD.as_millis() as u64,
            state = "response_pending",
            "HTTP request remains in flight"
        );
    }

    fn log_response<ResBody>(&self, response: &Response<ResBody>) {
        let duration_ms = self.duration_ms();
        let status = response.status();
        let status_code = status.as_u16();
        let result = Self::result_label(status);
        let trace_id = self.trace_id.as_deref().unwrap_or("unknown");
        let span_id = self.span_id.as_deref().unwrap_or("unknown");

        if status.is_server_error() {
            if !tracing::enabled!(target: HTTP_SERVER_LOG_TARGET, Level::ERROR) {
                return;
            }
            error!(
                target: HTTP_SERVER_LOG_TARGET,
                event = HTTP_REQUEST_COMPLETED_EVENT,
                component = LOG_COMPONENT_SERVER,
                subsystem = LOG_SUBSYSTEM_HTTP,
                request_id = %self.request_id,
                trace_id = %trace_id,
                span_id = %span_id,
                peer_addr = %self.peer_addr(),
                method = %self.method.as_str(),
                uri = %self.redacted_uri(),
                status_code,
                duration_ms,
                result,
                "HTTP request completed"
            );
        } else {
            if !tracing::enabled!(target: HTTP_SERVER_LOG_TARGET, Level::INFO) {
                return;
            }
            info!(
                target: HTTP_SERVER_LOG_TARGET,
                event = HTTP_REQUEST_COMPLETED_EVENT,
                component = LOG_COMPONENT_SERVER,
                subsystem = LOG_SUBSYSTEM_HTTP,
                request_id = %self.request_id,
                trace_id = %trace_id,
                span_id = %span_id,
                peer_addr = %self.peer_addr(),
                method = %self.method.as_str(),
                uri = %self.redacted_uri(),
                status_code,
                duration_ms,
                result,
                "HTTP request completed"
            );
        }
    }

    fn log_failure<E>(&self, error: &E)
    where
        E: std::fmt::Display,
    {
        error!(
            target: HTTP_SERVER_LOG_TARGET,
            event = HTTP_REQUEST_FAILED_EVENT,
            component = LOG_COMPONENT_SERVER,
            subsystem = LOG_SUBSYSTEM_HTTP,
            request_id = %self.request_id,
            trace_id = %self.trace_id.as_deref().unwrap_or("unknown"),
            span_id = %self.span_id.as_deref().unwrap_or("unknown"),
            peer_addr = %self.peer_addr(),
            method = %self.method.as_str(),
            uri = %self.redacted_uri(),
            duration_ms = self.duration_ms(),
            result = "service_error",
            error = %error,
            "HTTP request failed before a response was produced"
        );
    }
}

impl<S, B, ResBody> Service<HttpRequest<B>> for RequestLoggingService<S>
where
    S: Service<HttpRequest<B>, Response = Response<ResBody>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    S::Error: std::fmt::Display + Send + 'static,
    B: Send + 'static,
{
    type Response = Response<ResBody>;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: HttpRequest<B>) -> Self::Future {
        let context = RequestLogContext::from_request(&req);
        let mut inner = self.inner.clone();
        let watchdog = tracing::enabled!(target: HTTP_SERVER_LOG_TARGET, Level::WARN).then(CancellationToken::new);
        if let Some(watchdog) = watchdog.as_ref() {
            spawn_traced({
                let watchdog = watchdog.clone();
                let watchdog_context = context.clone();
                async move {
                    tokio::select! {
                        _ = watchdog.cancelled() => {}
                        _ = tokio::time::sleep(HTTP_REQUEST_INFLIGHT_WARN_THRESHOLD) => {
                            watchdog_context.log_slow_inflight();
                        }
                    }
                }
            });
        }

        Box::pin(async move {
            let result = inner.call(req).await;
            if let Some(watchdog) = watchdog {
                watchdog.cancel();
            }
            match &result {
                Ok(response) => context.log_response(response),
                Err(error) => context.log_failure(error),
            }
            result
        })
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

fn is_console_redirect_request<B>(req: &HttpRequest<B>) -> bool {
    let path = req.uri().path().trim_end_matches('/');
    req.method() == http::Method::GET
        && !req.headers().contains_key(http::header::AUTHORIZATION)
        && req
            .headers()
            .get(http::header::USER_AGENT)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|user_agent| user_agent.contains("Mozilla"))
        && (path.is_empty() || path == "/rustfs" || path == "/index.html")
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
        let path = req.uri().path().trim_end_matches('/');
        if is_console_redirect_request(&req) {
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

/// Adds `Content-Length: 0` for routes whose requests are known to carry no
/// body, but where some S3-compatible clients omit the header entirely.
///
/// The normalization runs before authentication so downstream request
/// validation sees an explicit empty body length without requiring every
/// handler to special-case absent `Content-Length`.
#[derive(Clone)]
pub struct EmptyBodyContentLengthCompatLayer;

impl<S> Layer<S> for EmptyBodyContentLengthCompatLayer {
    type Service = EmptyBodyContentLengthCompatService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        EmptyBodyContentLengthCompatService { inner }
    }
}

#[derive(Clone)]
pub struct EmptyBodyContentLengthCompatService<S> {
    inner: S,
}

impl<S, ReqBody, ResBody> Service<HttpRequest<ReqBody>> for EmptyBodyContentLengthCompatService<S>
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
        if should_force_zero_content_length_for_empty_body_route(&req) {
            req.headers_mut()
                .insert(http::header::CONTENT_LENGTH, HeaderValue::from_static("0"));
            req.headers_mut().remove(http::header::TRANSFER_ENCODING);
        }

        let mut inner = self.inner.clone();
        Box::pin(async move { inner.call(req).await.map_err(Into::into) })
    }
}

fn should_force_zero_content_length_for_empty_body_route<B>(req: &HttpRequest<B>) -> bool {
    if req.headers().contains_key(http::header::CONTENT_LENGTH) {
        return false;
    }

    if is_empty_body_admin_path(req.method(), req.uri()) {
        return true;
    }

    if req.headers().contains_key(http::header::TRANSFER_ENCODING) {
        return false;
    }

    if is_empty_body_console_path(req.method(), req.uri()) {
        return true;
    }

    is_empty_body_s3_path(req.method(), req.uri())
}

fn is_empty_body_admin_path(method: &Method, uri: &http::Uri) -> bool {
    let path = uri.path();
    match *method {
        Method::GET => is_admin_path(path),
        Method::PUT => matches!(
            path,
            "/minio/admin/v3/set-user-status"
                | "/minio/admin/v3/set-group-status"
                | "/minio/admin/v3/restore-config-history-kv"
                | "/rustfs/admin/v3/set-user-status"
                | "/rustfs/admin/v3/set-group-status"
                | "/rustfs/admin/v3/restore-config-history-kv"
        ),
        Method::POST => {
            matches!(
                path,
                "/minio/admin/v3/rebalance/start"
                    | "/minio/admin/v3/rebalance/stop"
                    | "/minio/admin/v3/background-heal/status"
                    | "/minio/admin/v3/pools/decommission"
                    | "/minio/admin/v3/pools/cancel"
                    | "/rustfs/admin/v3/rebalance/start"
                    | "/rustfs/admin/v3/rebalance/stop"
                    | "/rustfs/admin/v3/background-heal/status"
                    | "/rustfs/admin/v3/pools/decommission"
                    | "/rustfs/admin/v3/pools/cancel"
            ) || is_heal_status_query(path, uri.query())
        }
        _ => false,
    }
}

fn is_heal_status_query(path: &str, query: Option<&str>) -> bool {
    let Some(query) = query else {
        return false;
    };

    if !path.find("/v3/heal/").is_some_and(|index| path[..index].ends_with("/admin")) {
        return false;
    }

    query.split('&').any(|param| {
        param
            .split_once('=')
            .map(|(key, value)| key == "clientToken" && !value.is_empty())
            .unwrap_or(false)
    })
}

fn is_empty_body_s3_path(method: &Method, uri: &http::Uri) -> bool {
    matches!(*method, Method::GET | Method::HEAD | Method::DELETE) && ConditionalCorsLayer::is_s3_path(uri.path())
}

fn is_empty_body_console_path(method: &Method, uri: &http::Uri) -> bool {
    matches!(*method, Method::GET | Method::HEAD) && is_console_path(uri.path())
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

impl<S, ReqBody, RestBody, GrpcBody> Service<HttpRequest<ReqBody>> for S3ErrorMessageCompatService<S>
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
        let is_sts_query =
            req.method() == Method::POST && req.uri().path() == "/" && req.extensions().get::<StsQueryRequest>().is_some();
        let mut inner = self.inner.clone();

        Box::pin(async move {
            let response = inner.call(req).await?;
            if is_sts_query || response.status() != StatusCode::FORBIDDEN || !is_xml_response(response.headers()) {
                return Ok(response);
            }

            let (parts, body) = response.into_parts();

            let response = match body {
                HybridBody::Rest { rest_body } => {
                    let (rest_body, changed) = fix_s3_error_message_in_xml(rest_body).await.map_err(Into::into)?;
                    let mut parts = parts;
                    if changed {
                        parts.headers.remove(http::header::CONTENT_LENGTH);
                    }
                    Response::from_parts(parts, HybridBody::Rest { rest_body })
                }
                HybridBody::Grpc { grpc_body } => Response::from_parts(parts, HybridBody::Grpc { grpc_body }),
            };

            Ok(response)
        })
    }
}

#[derive(Clone)]
pub struct StsQueryApiCompatLayer;

#[derive(Clone, Copy, Debug)]
pub(crate) struct StsQueryRequest;

pub(crate) fn is_sts_query_request(method: &Method, uri: &Uri, headers: &HeaderMap) -> bool {
    method == Method::POST
        && uri.path() == "/"
        && headers
            .get(http::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.split(';').next())
            .is_some_and(|value| value.trim().eq_ignore_ascii_case("application/x-www-form-urlencoded"))
}

impl<S> Layer<S> for StsQueryApiCompatLayer {
    type Service = StsQueryApiCompatService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        StsQueryApiCompatService { inner }
    }
}

#[derive(Clone)]
pub struct StsQueryApiCompatService<S> {
    inner: S,
}

type StsBoxError = Box<dyn std::error::Error + Send + Sync>;
type StsBoxBody = tower_http::body::UnsyncBoxBody<Bytes, StsBoxError>;

impl<S, RequestBody> Service<HttpRequest<RequestBody>> for StsQueryApiCompatService<S>
where
    S: Service<HttpRequest<RequestBody>, Response = Response<StsBoxBody>> + Send + 'static,
    S::Future: Send + 'static,
    S::Error: From<StsBoxError> + Send + 'static,
{
    type Response = Response<StsBoxBody>;
    type Error = S::Error;
    type Future = Either<S::Future, Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut req: HttpRequest<RequestBody>) -> Self::Future {
        if !is_sts_query_request(req.method(), req.uri(), req.headers()) {
            return Either::Left(self.inner.call(req));
        }

        req.extensions_mut().insert(StsQueryRequest);
        let (request_id, request_id_header) = sts_response_request_id(&req);
        let future = self.inner.call(req);

        Either::Right(Box::pin(async move {
            let response = future.await?;
            let (mut parts, body) = response.into_parts();
            parts.headers.insert(AMZ_REQUEST_ID, request_id_header);

            let (body, changed) = convert_sts_query_response(body, parts.status, &request_id).await?;
            if changed {
                parts.headers.remove(http::header::CONTENT_LENGTH);
                parts
                    .headers
                    .insert(http::header::CONTENT_TYPE, HeaderValue::from_static("application/xml"));
            }
            let body = tower_http::body::UnsyncBoxBody::new(Full::from(body).map_err(|error| -> StsBoxError { match error {} }));
            let response = Response::from_parts(parts, body);

            Ok(response)
        }))
    }
}

#[derive(Clone)]
pub struct IcebergRestErrorCompatLayer;

impl<S> Layer<S> for IcebergRestErrorCompatLayer {
    type Service = IcebergRestErrorCompatService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        IcebergRestErrorCompatService { inner }
    }
}

#[derive(Clone)]
pub struct IcebergRestErrorCompatService<S> {
    inner: S,
}

impl<S, ReqBody, RestBody, GrpcBody> Service<HttpRequest<ReqBody>> for IcebergRestErrorCompatService<S>
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
        let catalog_path =
            (req.method() != Method::HEAD && is_table_catalog_path(req.uri().path())).then(|| req.uri().path().to_string());
        let mut inner = self.inner.clone();

        Box::pin(async move {
            let response = inner.call(req).await?;
            if catalog_path.is_none() || response.status().is_success() || !is_xml_response(response.headers()) {
                return Ok(response);
            }

            let (parts, body) = response.into_parts();

            let response = match body {
                HybridBody::Rest { rest_body } => {
                    let (rest_body, converted_status) = convert_iceberg_error_in_xml(
                        rest_body,
                        parts.status,
                        catalog_path.as_deref().expect("catalog path was checked"),
                    )
                    .await
                    .map_err(Into::into)?;
                    let mut parts = parts;
                    if let Some(status) = converted_status {
                        parts.status = status;
                        parts.headers.remove(http::header::CONTENT_LENGTH);
                        parts
                            .headers
                            .insert(http::header::CONTENT_TYPE, HeaderValue::from_static("application/json"));
                    }
                    Response::from_parts(parts, HybridBody::Rest { rest_body })
                }
                HybridBody::Grpc { grpc_body } => Response::from_parts(parts, HybridBody::Grpc { grpc_body }),
            };

            Ok(response)
        })
    }
}

#[derive(Debug, Deserialize)]
struct SerializedS3Error {
    #[serde(rename = "Code")]
    code: String,
    #[serde(default, rename = "Message")]
    message: Option<String>,
}

#[derive(Debug, Serialize)]
struct IcebergRestErrorEnvelope {
    error: IcebergRestError,
}

#[derive(Debug, Serialize)]
struct IcebergRestError {
    message: String,
    #[serde(rename = "type")]
    error_type: String,
    code: u16,
}

async fn convert_iceberg_error_in_xml<RestBody>(
    body: RestBody,
    status: StatusCode,
    path: &str,
) -> Result<(RestBody, Option<StatusCode>), RestBody::Error>
where
    RestBody: Body<Data = Bytes> + From<Bytes>,
{
    let bytes = BodyExt::collect(body).await?.to_bytes();
    let Some(parsed) = std::str::from_utf8(&bytes)
        .ok()
        .and_then(|xml| quick_xml::de::from_str::<SerializedS3Error>(xml).ok())
    else {
        return Ok((RestBody::from(bytes), None));
    };
    let status = iceberg_rest_status(status, &parsed.code);
    let envelope = IcebergRestErrorEnvelope {
        error: IcebergRestError {
            message: parsed.message.unwrap_or_else(|| parsed.code.clone()),
            error_type: iceberg_rest_error_type(&parsed.code, status, path),
            code: status.as_u16(),
        },
    };
    let Ok(json) = serde_json::to_vec(&envelope) else {
        return Ok((RestBody::from(bytes), None));
    };
    Ok((RestBody::from(Bytes::from(json)), Some(status)))
}

fn iceberg_rest_status(status: StatusCode, error_code: &str) -> StatusCode {
    if status == StatusCode::PRECONDITION_FAILED || error_code == "PreconditionFailed" {
        StatusCode::CONFLICT
    } else {
        status
    }
}

fn iceberg_rest_error_type(error_code: &str, status: StatusCode, path: &str) -> String {
    if error_code.ends_with("Exception") {
        return error_code.to_string();
    }
    match error_code {
        "AccessDenied" | "SignatureDoesNotMatch" => "ForbiddenException".to_string(),
        "InvalidAccessKeyId" => "NotAuthorizedException".to_string(),
        "InvalidArgument" | "InvalidRequest" => "BadRequestException".to_string(),
        "PreconditionFailed" => "CommitFailedException".to_string(),
        _ => match status {
            StatusCode::UNAUTHORIZED => "NotAuthorizedException".to_string(),
            StatusCode::FORBIDDEN => "ForbiddenException".to_string(),
            StatusCode::NOT_FOUND => iceberg_not_found_error_type(path).to_string(),
            StatusCode::CONFLICT => "CommitFailedException".to_string(),
            status if status.is_server_error() => "RESTException".to_string(),
            _ => "BadRequestException".to_string(),
        },
    }
}

fn iceberg_not_found_error_type(path: &str) -> &'static str {
    if path.contains("/views/") {
        "NoSuchViewException"
    } else if path.contains("/tables/") {
        "NoSuchTableException"
    } else {
        "NoSuchNamespaceException"
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

impl<S, ReqBody, RestBody, GrpcBody> Service<HttpRequest<ReqBody>> for ObjectAttributesEtagFixService<S>
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
        let is_target = is_object_attributes_request(&req);
        let mut inner = self.inner.clone();

        Box::pin(async move {
            let response = inner.call(req).await?;
            if !is_target || !response.status().is_success() || !is_xml_response(response.headers()) {
                return Ok(response);
            }

            let (parts, body) = response.into_parts();

            let response = match body {
                HybridBody::Rest { rest_body } => {
                    let rest_body = fix_object_attributes_etag_in_xml(rest_body).await.map_err(Into::into)?;

                    let mut parts = parts;
                    parts.headers.remove(http::header::CONTENT_LENGTH);

                    Response::from_parts(parts, HybridBody::Rest { rest_body })
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
            if !is_bodyless_status(response.status()) {
                return Ok(response);
            }

            let (mut parts, body) = response.into_parts();
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

#[derive(Clone)]
pub struct PublicHealthEndpointLayer {
    server_ctx: Arc<crate::runtime_sources::ServerContextSlot>,
}

impl PublicHealthEndpointLayer {
    pub fn new(server_ctx: Arc<crate::runtime_sources::ServerContextSlot>) -> Self {
        Self { server_ctx }
    }
}

impl<S> Layer<S> for PublicHealthEndpointLayer {
    type Service = PublicHealthEndpointService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        PublicHealthEndpointService {
            inner,
            server_ctx: Arc::clone(&self.server_ctx),
        }
    }
}

#[derive(Clone)]
pub struct PublicHealthEndpointService<S> {
    inner: S,
    server_ctx: Arc<crate::runtime_sources::ServerContextSlot>,
}

fn health_endpoint_enabled() -> bool {
    rustfs_utils::get_env_bool(rustfs_config::ENV_HEALTH_ENDPOINT_ENABLE, rustfs_config::DEFAULT_HEALTH_ENDPOINT_ENABLE)
}

fn health_compat_busy_check_enabled() -> bool {
    rustfs_utils::get_env_bool(
        rustfs_config::ENV_HEALTH_COMPAT_BUSY_CHECK_ENABLE,
        rustfs_config::DEFAULT_HEALTH_COMPAT_BUSY_CHECK_ENABLE,
    )
}

fn health_compat_busy_max_active_requests() -> u64 {
    rustfs_utils::get_env_usize(
        rustfs_config::ENV_HEALTH_COMPAT_BUSY_MAX_ACTIVE_REQUESTS,
        rustfs_config::DEFAULT_HEALTH_COMPAT_BUSY_MAX_ACTIVE_REQUESTS,
    ) as u64
}

fn health_compat_kms_ready_check_enabled() -> bool {
    rustfs_utils::get_env_bool(
        rustfs_config::ENV_HEALTH_COMPAT_KMS_READY_CHECK_ENABLE,
        rustfs_config::DEFAULT_HEALTH_COMPAT_KMS_READY_CHECK_ENABLE,
    )
}

fn resolve_public_health_probe(method: &Method, path: &str) -> Option<HealthProbe> {
    if (method != Method::GET && method != Method::HEAD) || !health_endpoint_enabled() {
        return None;
    }

    match path {
        HEALTH_PREFIX | HEALTH_COMPAT_LIVE_PATH | MINIO_HEALTH_LIVE_PATH => Some(HealthProbe::Liveness),
        HEALTH_READY_PATH | MINIO_HEALTH_READY_PATH => Some(HealthProbe::Readiness),
        MINIO_HEALTH_CLUSTER_PATH => Some(HealthProbe::ClusterWrite),
        MINIO_HEALTH_CLUSTER_READ_PATH => Some(HealthProbe::ClusterRead),
        _ => None,
    }
}

fn alias_busy_threshold_exceeded(active_requests: u64) -> bool {
    if !health_compat_busy_check_enabled() {
        return false;
    }

    let max_active_requests = health_compat_busy_max_active_requests();
    max_active_requests > 0 && active_requests >= max_active_requests
}

fn is_public_health_endpoint_request(method: &Method, path: &str) -> bool {
    resolve_public_health_probe(method, path).is_some()
}

async fn health_kms_ready() -> bool {
    let Some(service_manager) = runtime_sources::current_kms_runtime_service_manager() else {
        return true;
    };

    let service_running = matches!(service_manager.get_status().await, rustfs_kms::KmsServiceStatus::Running);
    let probe_status = service_manager.probe_status();
    kms_ready_from_probe(service_running, probe_status.as_deref(), kms_probe_staleness_limit())
}

async fn build_public_health_http_response<RestBody, GrpcBody>(
    method: Method,
    path: String,
    object_traffic_health: Option<Arc<ObjectTrafficHealth>>,
) -> Response<HybridBody<RestBody, GrpcBody>>
where
    RestBody: From<Bytes>,
{
    let probe = resolve_public_health_probe(&method, path.as_str())
        .expect("public health endpoint request should always resolve health probe");

    if probe == HealthProbe::Readiness && alias_busy_threshold_exceeded(active_http_requests()) {
        let retry_after = HeaderValue::from_static("5");
        let body_bytes = Bytes::from_static(b"{\"status\":\"busy\",\"ready\":false}");
        let mut builder = Response::builder()
            .status(StatusCode::TOO_MANY_REQUESTS)
            .header(http::header::CONTENT_TYPE, "application/json")
            .header(http::header::RETRY_AFTER, retry_after);
        if let Ok(val) = HeaderValue::from_str(&body_bytes.len().to_string()) {
            builder = builder.header(http::header::CONTENT_LENGTH, val);
        }
        return builder
            .body(HybridBody::Rest {
                rest_body: RestBody::from(body_bytes),
            })
            .expect("failed to build health busy response");
    }

    let readiness_report = collect_probe_readiness(probe, object_traffic_health.as_deref()).await;
    let kms_ready = if probe == HealthProbe::Readiness && health_compat_kms_ready_check_enabled() {
        Some(health_kms_ready().await)
    } else {
        None
    };

    let response_parts =
        build_health_response_parts(method, probe, readiness_report.as_ref(), "rustfs-endpoint", None, kms_ready);
    let body = response_parts
        .payload
        .map(|payload| Bytes::from(serde_json::to_vec(&payload).unwrap_or_else(|_| b"{}".to_vec())))
        .unwrap_or_default();

    Response::builder()
        .status(response_parts.status_code)
        .header(http::header::CONTENT_TYPE, "application/json")
        .body(HybridBody::Rest {
            rest_body: RestBody::from(body),
        })
        .expect("failed to build health response")
}

impl<S, ReqBody, RestBody, GrpcBody> Service<HttpRequest<ReqBody>> for PublicHealthEndpointService<S>
where
    S: Service<HttpRequest<ReqBody>, Response = Response<HybridBody<RestBody, GrpcBody>>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    ReqBody: Send + 'static,
    RestBody: From<Bytes> + Send + 'static,
    GrpcBody: Send + 'static,
{
    type Response = Response<HybridBody<RestBody, GrpcBody>>;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: HttpRequest<ReqBody>) -> Self::Future {
        let method = req.method();
        let path = req.uri().path();

        if is_public_health_endpoint_request(method, path) {
            let method = method.clone();
            let path = path.to_owned();
            let object_traffic_health = self
                .server_ctx
                .installed_app_context()
                .map(|context| context.object_traffic_health());
            return Box::pin(async move { Ok(build_public_health_http_response(method, path, object_traffic_health).await) });
        }

        let mut inner = self.inner.clone();
        Box::pin(async move { inner.call(req).await })
    }
}

/// Structured-log event emitted when a virtual-hosted-style request cannot be
/// routed because no server domain is configured.
const VIRTUAL_HOST_STYLE_UNROUTABLE_EVENT: &str = "virtual_host_style_unroutable";

/// Detects S3 requests that can only be virtual-hosted-style yet cannot be routed
/// because no server domain is configured (`RUSTFS_SERVER_DOMAINS` unset).
///
/// Without a configured domain, s3s parses every request as path-style, so a
/// virtual-hosted-style `PUT /` (the AWS SDK / Terraform default for `CreateBucket`)
/// arrives with an empty bucket and fails with an opaque `501 NotImplemented`.
///
/// Only the service root (`/`) with a method that has no path-style equivalent
/// (`PUT`/`DELETE`) and a DNS-style host (not an IP/socket address, and dotted like
/// `bucket.example.com`) is matched. The host is read from the `Host` header
/// (HTTP/1.1) or the request URI authority (HTTP/2 `:authority`). Such requests
/// already fail today, so returning a clearer error never changes the outcome of a
/// routable request. Returns the request host (without port) when matched.
fn unroutable_virtual_host_target(method: &Method, uri: &Uri, headers: &HeaderMap) -> Option<String> {
    if *method != Method::PUT && *method != Method::DELETE {
        return None;
    }
    if uri.path() != "/" {
        return None;
    }

    // `Host` is set for HTTP/1.1; HTTP/2 carries the host in the URI authority.
    let raw_authority = match headers.get(http::header::HOST).and_then(|value| value.to_str().ok()) {
        Some(value) => value,
        None => uri.authority().map(|authority| authority.as_str())?,
    };
    if raw_authority.is_empty() {
        return None;
    }
    // IP / socket-address hosts always use path-style addressing in s3s.
    if rustfs_utils::is_socket_addr(raw_authority) {
        return None;
    }
    // Parse as an Authority to split host/port robustly (e.g. bracketed IPv6) rather
    // than slicing on `:`.
    let authority = raw_authority.parse::<http::uri::Authority>().ok()?;
    let host = authority.host();
    // Require a dotted DNS name (the shape of a virtual-hosted bucket subdomain).
    if !host.contains('.') || host.starts_with('.') || host.ends_with('.') {
        return None;
    }

    Some(host.to_owned())
}

fn xml_escape(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '&' => escaped.push_str("&amp;"),
            '<' => escaped.push_str("&lt;"),
            '>' => escaped.push_str("&gt;"),
            '"' => escaped.push_str("&quot;"),
            '\'' => escaped.push_str("&apos;"),
            _ => escaped.push(ch),
        }
    }
    escaped
}

fn build_virtual_host_hint_response<RestBody, GrpcBody>(
    version: http::Version,
    host: &str,
    resource: &str,
) -> Response<HybridBody<RestBody, GrpcBody>>
where
    RestBody: From<Bytes>,
{
    let message = format!(
        "Virtual-hosted-style request for host '{host}' could not be routed because no server domain is \
         configured. Set RUSTFS_SERVER_DOMAINS to the S3 endpoint domain your buckets are addressed under \
         (for example RUSTFS_SERVER_DOMAINS=s3.example.com, so that bucket.s3.example.com routes to bucket \
         'bucket'), or configure your S3 client to use path-style addressing (AWS SDK: force_path_style=true; \
         Terraform aws provider: s3_use_path_style = true)."
    );
    let body = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
         <Error><Code>NotImplemented</Code><Message>{message}</Message><Resource>{resource}</Resource></Error>",
        message = xml_escape(&message),
        resource = xml_escape(resource),
    );

    let mut builder = Response::builder()
        .status(StatusCode::NOT_IMPLEMENTED)
        .header(http::header::CONTENT_TYPE, "application/xml");
    // This short-circuit path does not drain the request body. For HTTP/1.x, signal
    // connection close so an undrained body cannot disrupt keep-alive reuse. `Connection`
    // is a forbidden header in HTTP/2+, so it is only set for HTTP/1.x.
    if !matches!(version, http::Version::HTTP_2 | http::Version::HTTP_3) {
        builder = builder.header(http::header::CONNECTION, "close");
    }
    builder
        .body(HybridBody::Rest {
            rest_body: RestBody::from(Bytes::from(body)),
        })
        .expect("failed to build virtual-host hint response")
}

/// Returns an actionable error for virtual-hosted-style S3 requests that cannot be
/// routed because `RUSTFS_SERVER_DOMAINS` is not configured. See
/// [`unroutable_virtual_host_target`]. The layer is only installed when no server
/// domain is configured, so it is a pure pass-through for every routable request.
#[derive(Clone)]
pub struct VirtualHostStyleHintLayer;

impl<S> Layer<S> for VirtualHostStyleHintLayer {
    type Service = VirtualHostStyleHintService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        VirtualHostStyleHintService { inner }
    }
}

#[derive(Clone)]
pub struct VirtualHostStyleHintService<S> {
    inner: S,
}

impl<S, ReqBody, RestBody, GrpcBody> Service<HttpRequest<ReqBody>> for VirtualHostStyleHintService<S>
where
    S: Service<HttpRequest<ReqBody>, Response = Response<HybridBody<RestBody, GrpcBody>>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    ReqBody: Send + 'static,
    RestBody: From<Bytes> + Send + 'static,
    GrpcBody: Send + 'static,
{
    type Response = Response<HybridBody<RestBody, GrpcBody>>;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: HttpRequest<ReqBody>) -> Self::Future {
        if let Some(host) = unroutable_virtual_host_target(req.method(), req.uri(), req.headers()) {
            let resource = req.uri().path().to_owned();
            let version = req.version();
            debug!(
                event = VIRTUAL_HOST_STYLE_UNROUTABLE_EVENT,
                component = LOG_COMPONENT_SERVER,
                subsystem = LOG_SUBSYSTEM_HTTP,
                host = %host,
                method = %req.method(),
                "Rejected virtual-hosted-style request: no server domain configured (set RUSTFS_SERVER_DOMAINS or use path-style)"
            );
            return Box::pin(async move { Ok(build_virtual_host_hint_response(version, &host, &resource)) });
        }

        let mut inner = self.inner.clone();
        Box::pin(async move { inner.call(req).await })
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

fn sts_response_request_id<B>(req: &HttpRequest<B>) -> (String, HeaderValue) {
    let request_id = req
        .extensions()
        .get::<RequestContext>()
        .map(|context| context.request_id.as_str())
        .filter(|request_id| !request_id.trim().is_empty())
        .unwrap_or("sts-request-id")
        .to_owned();

    match HeaderValue::from_str(&request_id) {
        Ok(header) => (request_id, header),
        Err(_) => ("sts-request-id".to_owned(), HeaderValue::from_static("sts-request-id")),
    }
}

async fn convert_sts_query_response<RestBody>(
    body: RestBody,
    status: StatusCode,
    request_id: &str,
) -> Result<(Bytes, bool), RestBody::Error>
where
    RestBody: Body<Data = Bytes>,
{
    let bytes = BodyExt::collect(body).await?.to_bytes();
    let Ok(xml) = std::str::from_utf8(&bytes) else {
        return Ok((bytes, false));
    };

    let converted = if status.is_success() {
        add_sts_response_metadata(xml, request_id)
    } else {
        Some(Bytes::from(wrap_sts_error_response(xml, status, request_id)))
    };

    match converted {
        Some(xml) => Ok((xml, true)),
        None => Ok((bytes, false)),
    }
}

fn add_sts_response_metadata(xml: &str, request_id: &str) -> Option<Bytes> {
    let mut reader = quick_xml::Reader::from_str(xml);
    let mut depth = 0_usize;
    let mut root_prefix = None;

    loop {
        let event_start = usize::try_from(reader.buffer_position()).ok()?;
        match reader.read_event() {
            Ok(Event::Start(element)) => {
                if depth == 0 {
                    if !is_sts_success_response_tag(element.local_name().as_ref()) {
                        return None;
                    }
                    root_prefix = element
                        .name()
                        .prefix()
                        .map(|prefix| std::str::from_utf8(prefix.as_ref()).map(str::to_owned))
                        .transpose()
                        .ok()?;
                } else if depth == 1 && element.local_name().as_ref() == STS_RESPONSE_METADATA_TAG.as_bytes() {
                    return None;
                }
                depth += 1;
            }
            Ok(Event::Empty(element)) => {
                if depth == 0 {
                    return None;
                }
                if depth == 1 && element.local_name().as_ref() == STS_RESPONSE_METADATA_TAG.as_bytes() {
                    return None;
                }
            }
            Ok(Event::End(element)) => {
                if depth == 0 {
                    return None;
                }
                if depth == 1 {
                    if !is_sts_success_response_tag(element.local_name().as_ref()) {
                        return None;
                    }
                    let prefix = root_prefix.as_deref().map(|prefix| format!("{prefix}:")).unwrap_or_default();
                    let metadata = format!(
                        "<{prefix}{STS_RESPONSE_METADATA_TAG}>\
                         <{prefix}{STS_REQUEST_ID_TAG}>{request_id}</{prefix}{STS_REQUEST_ID_TAG}>\
                         </{prefix}{STS_RESPONSE_METADATA_TAG}>",
                        request_id = xml_escape(request_id),
                    );
                    let capacity = xml.len().checked_add(metadata.len())?;
                    let mut converted = BytesMut::with_capacity(capacity);
                    converted.extend_from_slice(&xml.as_bytes()[..event_start]);
                    converted.extend_from_slice(metadata.as_bytes());
                    converted.extend_from_slice(&xml.as_bytes()[event_start..]);
                    return Some(converted.freeze());
                }
                depth -= 1;
            }
            Ok(Event::Eof) | Err(_) => return None,
            _ => {}
        }
    }
}

fn is_sts_success_response_tag(tag: &[u8]) -> bool {
    STS_SUCCESS_RESPONSE_TAGS.iter().any(|candidate| candidate.as_bytes() == tag)
}

fn wrap_sts_error_response(xml: &str, status: StatusCode, request_id: &str) -> String {
    let parsed = quick_xml::de::from_str::<SerializedS3Error>(xml).ok();
    let (code, message) = parsed
        .as_ref()
        .map(|error| {
            let message = match error.message.as_deref() {
                Some(message) => Cow::Borrowed(message),
                None if error.code == S3ErrorCode::SignatureDoesNotMatch.as_str() => {
                    Cow::Owned(ApiError::error_code_to_message(&S3ErrorCode::SignatureDoesNotMatch))
                }
                None => Cow::Borrowed(error.code.as_str()),
            };
            (error.code.as_str(), message)
        })
        .unwrap_or_else(|| {
            let (code, message) = sts_error_for_status(status);
            (code, Cow::Borrowed(message))
        });
    let error_type = if status.is_server_error() { "Receiver" } else { "Sender" };

    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
         <ErrorResponse xmlns=\"https://sts.amazonaws.com/doc/2011-06-15/\">\
        <Error><Type>{error_type}</Type><Code>{code}</Code><Message>{message}</Message></Error>\
         <RequestId>{request_id}</RequestId></ErrorResponse>",
        code = xml_escape(code),
        message = xml_escape(&message),
        request_id = xml_escape(request_id),
    )
}

fn sts_error_for_status(status: StatusCode) -> (&'static str, &'static str) {
    match status {
        StatusCode::BAD_REQUEST => ("InvalidRequest", "Invalid Request"),
        StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => ("AccessDenied", "Access Denied"),
        StatusCode::TOO_MANY_REQUESTS => ("TooManyRequests", "Request rate limit exceeded"),
        StatusCode::SERVICE_UNAVAILABLE => ("ServiceUnavailable", "Service Unavailable"),
        _ => ("InternalError", "Internal Server Error"),
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

fn is_object_attributes_request<B>(req: &HttpRequest<B>) -> bool {
    if req.method() != Method::GET {
        return false;
    }

    let path = req.uri().path();
    if has_path_prefix(path, ADMIN_PREFIX)
        || has_path_prefix(path, MINIO_ADMIN_PREFIX)
        || has_path_prefix(path, RUSTFS_ADMIN_PREFIX)
        || has_path_prefix(path, MINIO_ADMIN_V3_PREFIX)
        || is_table_catalog_path(path)
        || has_path_prefix(path, CONSOLE_PREFIX)
        || has_path_prefix(path, RPC_PREFIX)
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
    const EXCLUDED_EXACT_PATHS: &'static [&'static str] = &[
        "/health",
        "/health/live",
        "/health/ready",
        "/minio/health/live",
        "/minio/health/ready",
        "/minio/health/cluster",
        "/minio/health/cluster/read",
        "/profile/cpu",
        "/profile/memory",
    ];

    fn is_s3_path(path: &str) -> bool {
        // Exclude Admin, Console, RPC, and configured special paths
        !has_path_prefix(path, ADMIN_PREFIX)
            && !has_path_prefix(path, MINIO_ADMIN_PREFIX)
            && !is_table_catalog_path(path)
            && !has_path_prefix(path, RPC_PREFIX)
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
            HeaderValue::from_static("x-request-id, x-amz-request-id, content-type, content-length, etag"),
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

impl<S, ReqBody, ResBody> Service<HttpRequest<ReqBody>> for ConditionalCorsService<S>
where
    S: Service<HttpRequest<ReqBody>, Response = Response<ResBody>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Into<Box<dyn std::error::Error + Send + Sync>> + Send + 'static,
    ReqBody: Send + 'static,
    ResBody: Default + Send + 'static,
{
    type Response = Response<ResBody>;
    type Error = Box<dyn std::error::Error + Send + Sync>;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx).map_err(Into::into)
    }

    fn call(&mut self, req: HttpRequest<ReqBody>) -> Self::Future {
        let is_options = req.method() == Method::OPTIONS;
        let has_origin = req.headers().contains_key(cors::standard::ORIGIN);
        if !is_options && !has_origin {
            let mut inner = self.inner.clone();
            return Box::pin(async move { inner.call(req).await.map_err(Into::into) });
        }

        let path = req.uri().path().to_string();
        let method = req.method().clone();
        let request_headers = req.headers().clone();
        let cors_origins = self.cors_origins.clone();
        let is_s3 = ConditionalCorsLayer::is_s3_path(&path);
        let is_root = path == "/";

        if is_options {
            let has_acrm = request_headers.contains_key(cors::request::ACCESS_CONTROL_REQUEST_METHOD);

            if is_root {
                return Box::pin(async move {
                    if !has_acrm || !request_headers.contains_key(cors::standard::ORIGIN) {
                        return Ok(Response::builder()
                            .status(StatusCode::BAD_REQUEST)
                            .body(ResBody::default())
                            .unwrap());
                    }

                    let mut response = Response::builder()
                        .status(StatusCode::OK)
                        .body(ResBody::default())
                        .expect("valid response body");
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

                        let mut response = Response::builder()
                            .status(status)
                            .body(ResBody::default())
                            .expect("valid response body");
                        if cors_allowed {
                            for (key, value) in cors_headers.iter() {
                                response.headers_mut().insert(key, value.clone());
                            }
                        }
                        return Ok(response);
                    }

                    // No bucket-level CORS config: fall back to global/default CORS behavior.
                    let mut response = Response::builder()
                        .status(StatusCode::OK)
                        .body(ResBody::default())
                        .expect("valid response body");
                    cors_layer.apply_cors_headers(&request_headers, response.headers_mut());
                    Ok(response)
                });
            }

            let request_headers_clone = request_headers.clone();
            return Box::pin(async move {
                let mut response = Response::builder()
                    .status(StatusCode::OK)
                    .body(ResBody::default())
                    .expect("valid response body");
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

/// Rewrites the request-target of a root-level double-slash `GET //` to `GET /`
/// so it routes to `ListBuckets`, matching MinIO browser compatibility.
///
/// The AWS S3 browser client (SigV4) concatenates the endpoint with a leading
/// slash and emits `GET //` for `ListBuckets`. The `s3s` path parser strips the
/// single leading slash and then treats the remaining `/` as an empty bucket
/// name, rejecting the request with `InvalidBucketName` before it is routed.
/// MinIO's `api-router.go` registers an explicit `//` route to `ListBuckets`;
/// this layer reproduces that behavior by collapsing the path to `/` up front.
///
/// The rewrite is intentionally limited to a path that is exactly `//` (optionally
/// followed by a query string). It never touches bucket/object requests, so object
/// keys with embedded or leading slashes are unaffected (those are normalized by
/// `s3s` via `normalize_forward_slash_path`).
#[derive(Clone)]
pub struct DoubleSlashListBucketsCompatLayer;

impl<S> Layer<S> for DoubleSlashListBucketsCompatLayer {
    type Service = DoubleSlashListBucketsCompatService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        DoubleSlashListBucketsCompatService { inner }
    }
}

#[derive(Clone)]
pub struct DoubleSlashListBucketsCompatService<S> {
    inner: S,
}

impl<S, ReqBody, ResBody> Service<HttpRequest<ReqBody>> for DoubleSlashListBucketsCompatService<S>
where
    S: Service<HttpRequest<ReqBody>, Response = Response<ResBody>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    ReqBody: Send + 'static,
    ResBody: Send + 'static,
{
    type Response = Response<ResBody>;
    type Error = S::Error;
    type Future = S::Future;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut req: HttpRequest<ReqBody>) -> Self::Future {
        if req.method() == Method::GET
            && let Some(rewritten) = rewrite_double_slash_root(req.uri())
        {
            *req.uri_mut() = rewritten;
        }
        self.inner.call(req)
    }
}

/// Returns the `/`-rooted URI when `uri` has a request-target path of exactly `//`.
///
/// Returns `None` for every other path so the rewrite is confined to the
/// root-level double-slash case.
fn rewrite_double_slash_root(uri: &Uri) -> Option<Uri> {
    if uri.path() != "//" {
        return None;
    }

    let mut parts = uri.clone().into_parts();
    let path_and_query = match uri.query() {
        Some(query) => format!("/?{query}"),
        None => "/".to_string(),
    };
    parts.path_and_query = Some(path_and_query.parse().ok()?);
    Uri::from_parts(parts).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::compress::{HttpCompressionConfig, PathAwareHttpCompressionPredicate, PathCategoryInjectionLayer};
    use crate::server::{FAVICON_PATH, LICENSE, RemoteAddr, VERSION};
    use futures::future::{Ready, ready};
    use http::Request;
    use http_body_util::BodyExt;
    use http_body_util::Empty;
    use http_body_util::Full;
    use opentelemetry::global;
    use opentelemetry_sdk::propagation::TraceContextPropagator;
    use serial_test::serial;
    use std::convert::Infallible;
    use std::io::{self, Write};
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use temp_env::{async_with_vars, with_var};
    use tracing_subscriber::{Registry, fmt::MakeWriter, layer::SubscriberExt};

    fn public_health_layer() -> PublicHealthEndpointLayer {
        PublicHealthEndpointLayer::new(crate::runtime_sources::ServerContextSlot::new())
    }

    async fn public_health_layer_with_tracker(object_traffic_health: Arc<ObjectTrafficHealth>) -> PublicHealthEndpointLayer {
        let app_context = crate::app::gating_test_env::app_context_with_object_traffic_health(object_traffic_health).await;
        let server_ctx = crate::runtime_sources::ServerContextSlot::new();
        assert!(server_ctx.install(app_context));
        PublicHealthEndpointLayer::new(server_ctx)
    }

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
        request_context: Arc<Mutex<Option<RequestContext>>>,
        response_request_id: Option<HeaderValue>,
    }

    impl HeaderCaptureService {
        fn with_response_request_id(request_id: &'static str) -> Self {
            Self {
                response_request_id: Some(HeaderValue::from_static(request_id)),
                ..Self::default()
            }
        }

        fn headers(&self) -> Arc<Mutex<Option<HeaderMap>>> {
            Arc::clone(&self.headers)
        }

        fn request_context(&self) -> Arc<Mutex<Option<RequestContext>>> {
            Arc::clone(&self.request_context)
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
            *self.request_context.lock().expect("capture request context") = req.extensions().get::<RequestContext>().cloned();
            let mut response = Response::new(Full::from(Bytes::new()));
            if let Some(request_id) = self.response_request_id.clone() {
                response.headers_mut().insert(REQUEST_ID_HEADER, request_id);
            }
            ready(Ok(response))
        }
    }

    async fn assert_non_s3_request_id_contract(mut request: Request<()>, route: &str) {
        let capture = HeaderCaptureService::default();
        let captured_context = capture.request_context();
        let mut service = ExternalRequestContextLayer::default().layer(capture);
        request
            .headers_mut()
            .insert(REQUEST_ID_HEADER, HeaderValue::from_static("client-request-id"));
        request
            .headers_mut()
            .insert(AMZ_REQUEST_ID, HeaderValue::from_static("client-amz-request-id"));

        let response = service.call(request).await.expect("non-S3 response");

        assert_eq!(
            response
                .headers()
                .get(REQUEST_ID_HEADER)
                .and_then(|value| value.to_str().ok()),
            Some("client-request-id"),
            "non-S3 x-request-id contract changed for {route}"
        );
        assert!(
            !response.headers().contains_key(AMZ_REQUEST_ID),
            "non-S3 response unexpectedly gained x-amz-request-id for {route}"
        );
        let context = captured_context
            .lock()
            .expect("captured request context")
            .clone()
            .expect("non-S3 request context");
        assert_eq!(context.request_id, "client-request-id", "non-S3 context changed for {route}");
        assert_eq!(context.x_amz_request_id, "client-request-id");
        assert!(context.trace_id.is_none());
        assert!(context.span_id.is_none());
    }

    #[tokio::test]
    async fn external_request_context_rejects_client_request_id_as_canonical() {
        global::set_text_map_propagator(TraceContextPropagator::new());
        let capture = HeaderCaptureService::default();
        let captured_headers = capture.headers();
        let captured_context = capture.request_context();
        let mut service = ExternalRequestContextLayer::default().layer(capture);
        let mut request = Request::builder().uri("/bucket/object").body(()).expect("build S3 request");
        request
            .headers_mut()
            .insert(REQUEST_ID_HEADER, HeaderValue::from_static("client-supplied-request-id"));
        request
            .headers_mut()
            .insert(AMZ_REQUEST_ID, HeaderValue::from_static("client-supplied-amz-request-id"));
        request.headers_mut().insert(
            "traceparent",
            HeaderValue::from_static("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"),
        );

        let response = service.call(request).await.expect("response");
        let request_id = response
            .headers()
            .get(REQUEST_ID_HEADER)
            .and_then(|value| value.to_str().ok())
            .expect("canonical request ID");

        assert!(uuid::Uuid::parse_str(request_id).is_ok());
        assert_ne!(request_id, "client-supplied-request-id");
        assert_eq!(
            response.headers().get(AMZ_REQUEST_ID).and_then(|value| value.to_str().ok()),
            Some(request_id)
        );
        let headers = captured_headers.lock().expect("captured headers");
        let headers = headers.as_ref().expect("inner request headers");
        assert_eq!(headers.get(REQUEST_ID_HEADER).expect("client x-request-id"), "client-supplied-request-id");
        assert_eq!(
            headers.get(AMZ_REQUEST_ID).expect("client x-amz-request-id"),
            "client-supplied-amz-request-id"
        );
        assert_eq!(
            headers.get("traceparent").expect("client traceparent"),
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
        );
        let context = captured_context
            .lock()
            .expect("captured request context")
            .clone()
            .expect("S3 request context");
        assert_eq!(context.request_id, request_id);
        assert_eq!(context.trace_id.as_deref(), Some("4bf92f3577b34da6a3ce929d0e0e4736"));
        assert_eq!(context.span_id.as_deref(), Some("00f067aa0ba902b7"));
    }

    #[tokio::test]
    async fn external_request_context_replaces_empty_response_id() {
        let capture = HeaderCaptureService::default();
        let captured_headers = capture.headers();
        let mut service = ExternalRequestContextLayer::default().layer(capture);
        let mut request = Request::builder().uri("/bucket/object").body(()).expect("build S3 request");
        request.headers_mut().insert(REQUEST_ID_HEADER, HeaderValue::from_static(""));
        request.headers_mut().insert(AMZ_REQUEST_ID, HeaderValue::from_static("   "));

        let response = service.call(request).await.expect("response");
        let request_id = response
            .headers()
            .get(REQUEST_ID_HEADER)
            .and_then(|value| value.to_str().ok())
            .expect("canonical request ID");

        assert!(uuid::Uuid::parse_str(request_id).is_ok());
        assert_eq!(
            response.headers().get(AMZ_REQUEST_ID).and_then(|value| value.to_str().ok()),
            Some(request_id)
        );
        let headers = captured_headers.lock().expect("captured headers");
        let headers = headers.as_ref().expect("inner request headers");
        assert_eq!(headers.get(REQUEST_ID_HEADER).expect("empty client x-request-id"), "");
        assert_eq!(headers.get(AMZ_REQUEST_ID).expect("blank client x-amz-request-id"), "   ");
    }

    #[tokio::test]
    async fn external_request_context_inserts_generated_id_when_header_is_absent() {
        let capture = HeaderCaptureService::default();
        let captured_headers = capture.headers();
        let mut service = ExternalRequestContextLayer::default().layer(capture);
        let request = Request::builder().uri("/bucket/object").body(()).expect("build S3 request");

        let response = service.call(request).await.expect("response");
        let response_request_id = response
            .headers()
            .get(REQUEST_ID_HEADER)
            .and_then(|value| value.to_str().ok())
            .expect("canonical request ID");
        let headers = captured_headers.lock().expect("captured headers");
        let headers = headers.as_ref().expect("inner request headers");

        assert_eq!(
            headers.get(REQUEST_ID_HEADER).and_then(|value| value.to_str().ok()),
            Some(response_request_id)
        );
    }

    #[tokio::test]
    async fn non_s3_request_id_contract_preserves_client_correlation() {
        for path in [
            "/rustfs/admin/v3/info",
            "/minio/admin/v3/info",
            "/rustfs/console/",
            HEALTH_PREFIX,
            "/iceberg/v1/config",
            "/rustfs/rpc/v1/read-file",
            "/rustfs/rpcx",
        ] {
            let request = Request::builder().uri(path).body(()).expect("build non-S3 request");
            assert_non_s3_request_id_contract(request, path).await;
        }
    }

    #[tokio::test]
    async fn non_s3_request_context_preserves_propagated_trace_context() {
        global::set_text_map_propagator(TraceContextPropagator::new());
        let capture = HeaderCaptureService::default();
        let captured_context = capture.request_context();
        let mut service = ExternalRequestContextLayer::default().layer(capture);
        let request = Request::builder()
            .uri("/rustfs/admin/v3/info")
            .header(REQUEST_ID_HEADER, "client-request-id")
            .header("traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
            .body(())
            .expect("build admin request");

        let response = service.call(request).await.expect("admin response");
        let context = captured_context
            .lock()
            .expect("captured request context")
            .clone()
            .expect("admin request context");

        assert_eq!(
            response
                .headers()
                .get(REQUEST_ID_HEADER)
                .and_then(|value| value.to_str().ok()),
            Some("client-request-id")
        );
        assert_eq!(context.request_id, "client-request-id");
        assert_eq!(context.x_amz_request_id, "client-request-id");
        assert_eq!(context.trace_id.as_deref(), Some("4bf92f3577b34da6a3ce929d0e0e4736"));
        assert_eq!(context.span_id.as_deref(), Some("00f067aa0ba902b7"));
    }

    #[test]
    fn console_redirect_request_id_contract_follows_redirect_enablement() {
        for path in ["/", "/rustfs", "/index.html"] {
            let request = Request::builder()
                .method(Method::GET)
                .uri(path)
                .header(http::header::USER_AGENT, "Mozilla/5.0")
                .body(())
                .expect("build console redirect request");

            assert!(!uses_server_owned_s3_request_id(&request, true));
            assert!(uses_server_owned_s3_request_id(&request, false));
        }
    }

    #[test]
    fn method_scoped_control_routes_preserve_request_id_contract() {
        for path in [HEALTH_READY_PATH, PROFILE_CPU_PATH, PROFILE_MEMORY_PATH] {
            let control_request = Request::builder()
                .method(Method::GET)
                .uri(path)
                .body(())
                .expect("build control-plane request");
            assert!(!uses_server_owned_s3_request_id(&control_request, false));

            let s3_request = Request::builder()
                .method(Method::POST)
                .uri(path)
                .body(())
                .expect("build S3 request");
            assert!(uses_server_owned_s3_request_id(&s3_request, false));
        }
    }

    #[test]
    #[serial]
    fn public_health_alias_request_id_contract_follows_enablement() {
        let request = Request::builder()
            .method(Method::GET)
            .uri(MINIO_HEALTH_LIVE_PATH)
            .body(())
            .expect("build health request");

        with_var(rustfs_config::ENV_HEALTH_ENDPOINT_ENABLE, Some("true"), || {
            assert!(!uses_server_owned_s3_request_id(&request, false));
        });
        with_var(rustfs_config::ENV_HEALTH_ENDPOINT_ENABLE, Some("false"), || {
            assert!(uses_server_owned_s3_request_id(&request, false));
        });
    }

    #[tokio::test]
    async fn grpc_request_id_contract_preserves_client_correlation() {
        for path in [
            "/node_service.NodeService/GetMetrics",
            "/node_service.HealControlService/HealControl",
            "/node_service.TierMutationControlService/PrepareTierMutation",
        ] {
            let request = Request::builder()
                .version(http::Version::HTTP_2)
                .uri(path)
                .header(http::header::CONTENT_TYPE, "application/grpc")
                .body(())
                .expect("build gRPC request");
            assert_non_s3_request_id_contract(request, path).await;
        }
    }

    #[tokio::test]
    async fn sts_query_request_id_contract_preserves_client_correlation() {
        let request = Request::builder()
            .method(Method::POST)
            .uri("/")
            .header(http::header::CONTENT_TYPE, "application/x-www-form-urlencoded")
            .body(())
            .expect("build STS Query request");
        assert_non_s3_request_id_contract(request, "STS Query").await;
    }

    #[cfg(feature = "swift")]
    #[tokio::test]
    async fn swift_request_id_contract_preserves_client_correlation() {
        let path = "/v1/AUTH_project/container/object";
        let request = Request::builder().uri(path).body(()).expect("build Swift request");
        assert_non_s3_request_id_contract(request, path).await;
    }

    #[cfg(feature = "swift")]
    #[test]
    fn swift_request_id_classification_preserves_v1_prefix_boundary() {
        let swift_request = Request::builder()
            .uri("/v1/AUTH_project/container/object")
            .body(())
            .expect("build Swift request");
        assert!(!uses_server_owned_s3_request_id(&swift_request, false));

        let double_slash_request = Request::builder()
            .uri("//v1/AUTH_project/container/object")
            .body(())
            .expect("build double-slash request");
        assert!(uses_server_owned_s3_request_id(&double_slash_request, false));
    }

    #[cfg(feature = "swift")]
    #[tokio::test]
    async fn non_swift_v1_path_keeps_s3_request_id_contract() {
        let capture = HeaderCaptureService::default();
        let mut service = ExternalRequestContextLayer::default().layer(capture);
        let mut request = Request::builder()
            .uri("/v1/not-a-swift-account/object")
            .body(())
            .expect("build S3 request");
        request
            .headers_mut()
            .insert(REQUEST_ID_HEADER, HeaderValue::from_static("client-request-id"));

        let response = service.call(request).await.expect("S3 response");
        let response_request_id = response
            .headers()
            .get(REQUEST_ID_HEADER)
            .and_then(|value| value.to_str().ok())
            .expect("S3 response request ID");

        assert!(uuid::Uuid::parse_str(response_request_id).is_ok());
        assert_eq!(
            response.headers().get(AMZ_REQUEST_ID).and_then(|value| value.to_str().ok()),
            Some(response_request_id)
        );
    }

    #[tokio::test]
    async fn non_s3_response_preserves_handler_request_id() {
        let capture = HeaderCaptureService::with_response_request_id("handler-request-id");
        let mut service = ExternalRequestContextLayer::default().layer(capture);
        let mut request = Request::builder()
            .uri("/rustfs/admin/v3/info")
            .body(())
            .expect("build admin request");
        request
            .headers_mut()
            .insert(REQUEST_ID_HEADER, HeaderValue::from_static("client-request-id"));

        let response = service.call(request).await.expect("admin response");

        assert_eq!(
            response
                .headers()
                .get(REQUEST_ID_HEADER)
                .and_then(|value| value.to_str().ok()),
            Some("handler-request-id")
        );
        assert!(!response.headers().contains_key(AMZ_REQUEST_ID));
    }

    #[tokio::test]
    async fn non_s3_request_without_id_generates_only_x_request_id() {
        let capture = HeaderCaptureService::default();
        let captured_context = capture.request_context();
        let mut service = ExternalRequestContextLayer::default().layer(capture);
        let request = Request::builder()
            .uri("/rustfs/admin/v3/info")
            .body(())
            .expect("build admin request");

        let response = service.call(request).await.expect("admin response");
        let response_request_id = response
            .headers()
            .get(REQUEST_ID_HEADER)
            .and_then(|value| value.to_str().ok())
            .expect("generated admin request ID");

        assert!(uuid::Uuid::parse_str(response_request_id).is_ok());
        assert!(!response.headers().contains_key(AMZ_REQUEST_ID));
        let context = captured_context
            .lock()
            .expect("captured request context")
            .clone()
            .expect("admin request context");
        assert_eq!(context.request_id, response_request_id);
    }

    #[derive(Clone, Default)]
    struct CountingHybridService {
        calls: Arc<AtomicUsize>,
    }

    impl CountingHybridService {
        fn calls(&self) -> Arc<AtomicUsize> {
            Arc::clone(&self.calls)
        }
    }

    impl<B: Send + 'static> Service<Request<B>> for CountingHybridService {
        type Response = Response<HybridBody<Full<Bytes>, Full<Bytes>>>;
        type Error = Infallible;
        type Future = Ready<Result<Self::Response, Self::Error>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _req: Request<B>) -> Self::Future {
            self.calls.fetch_add(1, Ordering::SeqCst);
            ready(Ok(Response::builder()
                .status(StatusCode::IM_A_TEAPOT)
                .body(HybridBody::Rest {
                    rest_body: Full::from(Bytes::from_static(b"inner")),
                })
                .expect("response")))
        }
    }

    #[tokio::test]
    #[serial]
    async fn public_health_endpoint_layer_handles_health_before_inner_service() {
        async_with_vars([(rustfs_config::ENV_HEALTH_ENDPOINT_ENABLE, Some("true"))], async {
            let inner = CountingHybridService::default();
            let calls = inner.calls();
            let mut service = public_health_layer().layer(inner);

            let response = service
                .call(
                    Request::builder()
                        .method(Method::GET)
                        .uri(HEALTH_PREFIX)
                        .header(http::header::HOST, "localhost:9000")
                        .body(Full::<Bytes>::from(Bytes::new()))
                        .expect("request"),
                )
                .await
                .expect("health response");

            assert_eq!(response.status(), StatusCode::OK);
            assert_eq!(calls.load(Ordering::SeqCst), 0);
            assert_eq!(
                response
                    .headers()
                    .get(http::header::CONTENT_TYPE)
                    .and_then(|value| value.to_str().ok()),
                Some("application/json")
            );

            let body = BodyExt::collect(response.into_body()).await.expect("body").to_bytes();
            let payload: serde_json::Value =
                serde_json::from_slice(&body).expect("public liveness health response should be valid JSON");
            assert_eq!(payload["status"], "ok");
            assert_eq!(payload["ready"], true);
            assert!(payload.get("details").is_none());
            assert!(payload.get("degradedReasons").is_none());
        })
        .await;
    }

    #[test]
    fn unroutable_virtual_host_target_matches_service_root_writes() {
        let mut headers = HeaderMap::new();
        headers.insert(http::header::HOST, HeaderValue::from_static("my-bucket.s3.example.com"));
        assert_eq!(
            unroutable_virtual_host_target(&Method::PUT, &Uri::from_static("/"), &headers),
            Some("my-bucket.s3.example.com".to_owned())
        );
        assert_eq!(
            unroutable_virtual_host_target(&Method::DELETE, &Uri::from_static("/"), &headers),
            Some("my-bucket.s3.example.com".to_owned())
        );
    }

    #[test]
    fn unroutable_virtual_host_target_strips_port() {
        let mut headers = HeaderMap::new();
        headers.insert(http::header::HOST, HeaderValue::from_static("bucket.localhost:9000"));
        assert_eq!(
            unroutable_virtual_host_target(&Method::PUT, &Uri::from_static("/"), &headers),
            Some("bucket.localhost".to_owned())
        );
    }

    #[test]
    fn unroutable_virtual_host_target_uses_uri_authority_when_host_header_absent() {
        // HTTP/2 clients carry the host in the URI authority (`:authority`) rather than a Host header.
        let uri = Uri::from_static("https://my-bucket.s3.example.com/");
        assert_eq!(
            unroutable_virtual_host_target(&Method::PUT, &uri, &HeaderMap::new()),
            Some("my-bucket.s3.example.com".to_owned())
        );
    }

    #[test]
    fn unroutable_virtual_host_target_ignores_non_matching_requests() {
        let mut vhost = HeaderMap::new();
        vhost.insert(http::header::HOST, HeaderValue::from_static("bucket.s3.example.com"));
        // Path-style request carries the bucket in the path.
        assert_eq!(unroutable_virtual_host_target(&Method::PUT, &Uri::from_static("/bucket"), &vhost), None);
        // GET / is ListBuckets, a valid path-style service call.
        assert_eq!(unroutable_virtual_host_target(&Method::GET, &Uri::from_static("/"), &vhost), None);

        // IP / socket-address hosts always use path-style addressing.
        let mut ip = HeaderMap::new();
        ip.insert(http::header::HOST, HeaderValue::from_static("127.0.0.1:9000"));
        assert_eq!(unroutable_virtual_host_target(&Method::PUT, &Uri::from_static("/"), &ip), None);

        // Bracketed IPv6 authority (with port) is a socket address, not a bucket subdomain.
        let mut ipv6 = HeaderMap::new();
        ipv6.insert(http::header::HOST, HeaderValue::from_static("[::1]:9000"));
        assert_eq!(unroutable_virtual_host_target(&Method::PUT, &Uri::from_static("/"), &ipv6), None);

        // Bare single-label hosts are not virtual-hosted bucket subdomains.
        let mut bare = HeaderMap::new();
        bare.insert(http::header::HOST, HeaderValue::from_static("localhost:9000"));
        assert_eq!(unroutable_virtual_host_target(&Method::PUT, &Uri::from_static("/"), &bare), None);

        // Trailing-dot FQDN is not matched (avoids suggesting a domain with a trailing dot).
        let mut fqdn = HeaderMap::new();
        fqdn.insert(http::header::HOST, HeaderValue::from_static("bucket.example.com."));
        assert_eq!(unroutable_virtual_host_target(&Method::PUT, &Uri::from_static("/"), &fqdn), None);

        // Missing Host header and no URI authority.
        assert_eq!(
            unroutable_virtual_host_target(&Method::PUT, &Uri::from_static("/"), &HeaderMap::new()),
            None
        );
    }

    #[test]
    fn build_virtual_host_hint_response_is_actionable() {
        let response: Response<HybridBody<Full<Bytes>, Full<Bytes>>> =
            build_virtual_host_hint_response(http::Version::HTTP_11, "my-bucket.s3.example.com", "/");
        assert_eq!(response.status(), StatusCode::NOT_IMPLEMENTED);
        assert_eq!(
            response
                .headers()
                .get(http::header::CONTENT_TYPE)
                .and_then(|value| value.to_str().ok()),
            Some("application/xml")
        );
        // HTTP/1.x error responses close the connection (the request body is not drained).
        assert_eq!(
            response
                .headers()
                .get(http::header::CONNECTION)
                .and_then(|value| value.to_str().ok()),
            Some("close")
        );

        // `Connection` is forbidden in HTTP/2, so it must not be set there.
        let h2_response: Response<HybridBody<Full<Bytes>, Full<Bytes>>> =
            build_virtual_host_hint_response(http::Version::HTTP_2, "my-bucket.s3.example.com", "/");
        assert!(h2_response.headers().get(http::header::CONNECTION).is_none());
    }

    #[tokio::test]
    async fn virtual_host_style_hint_layer_short_circuits_unroutable_put() {
        let inner = CountingHybridService::default();
        let calls = inner.calls();
        let mut service = VirtualHostStyleHintLayer.layer(inner);

        let response = service
            .call(
                Request::builder()
                    .method(Method::PUT)
                    .uri("/")
                    .header(http::header::HOST, "my-bucket.s3.example.com")
                    .body(Full::<Bytes>::from(Bytes::new()))
                    .expect("request"),
            )
            .await
            .expect("hint response");

        assert_eq!(response.status(), StatusCode::NOT_IMPLEMENTED);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert_eq!(
            response
                .headers()
                .get(http::header::CONNECTION)
                .and_then(|value| value.to_str().ok()),
            Some("close")
        );

        let body = BodyExt::collect(response.into_body()).await.expect("body").to_bytes();
        let body = String::from_utf8(body.to_vec()).expect("utf8 body");
        assert!(body.contains("RUSTFS_SERVER_DOMAINS=s3.example.com"));
        assert!(body.contains("s3_use_path_style"));
    }

    #[tokio::test]
    async fn virtual_host_style_hint_layer_passes_through_path_style() {
        let inner = CountingHybridService::default();
        let calls = inner.calls();
        let mut service = VirtualHostStyleHintLayer.layer(inner);

        let response = service
            .call(
                Request::builder()
                    .method(Method::PUT)
                    .uri("/my-bucket")
                    .header(http::header::HOST, "s3.example.com")
                    .body(Full::<Bytes>::from(Bytes::new()))
                    .expect("request"),
            )
            .await
            .expect("inner response");

        // Path-style request reaches the inner service unchanged.
        assert_eq!(response.status(), StatusCode::IM_A_TEAPOT);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    #[serial]
    async fn public_health_endpoint_layer_handles_ready_head_before_inner_service() {
        async_with_vars([(rustfs_config::ENV_HEALTH_ENDPOINT_ENABLE, Some("true"))], async {
            let inner = CountingHybridService::default();
            let calls = inner.calls();
            let mut service = public_health_layer().layer(inner);

            let response = service
                .call(
                    Request::builder()
                        .method(Method::HEAD)
                        .uri(HEALTH_READY_PATH)
                        .body(Full::<Bytes>::from(Bytes::new()))
                        .expect("request"),
                )
                .await
                .expect("health response");

            assert!(response.status() == StatusCode::OK || response.status() == StatusCode::SERVICE_UNAVAILABLE);
            assert_eq!(calls.load(Ordering::SeqCst), 0);

            let body = BodyExt::collect(response.into_body()).await.expect("body").to_bytes();
            assert!(body.is_empty());
        })
        .await;
    }

    #[tokio::test]
    #[serial]
    async fn public_health_endpoint_layer_handles_health_live_path() {
        async_with_vars([(rustfs_config::ENV_HEALTH_ENDPOINT_ENABLE, Some("true"))], async {
            let inner = CountingHybridService::default();
            let calls = inner.calls();
            let mut service = public_health_layer().layer(inner);

            let response = service
                .call(
                    Request::builder()
                        .method(Method::GET)
                        .uri(HEALTH_COMPAT_LIVE_PATH)
                        .body(Full::<Bytes>::from(Bytes::new()))
                        .expect("request"),
                )
                .await
                .expect("health response");

            assert_eq!(response.status(), StatusCode::OK);
            assert_eq!(calls.load(Ordering::SeqCst), 0);
        })
        .await;
    }

    #[tokio::test]
    #[serial]
    async fn public_health_endpoint_layer_handles_minio_health_live_path() {
        async_with_vars([(rustfs_config::ENV_HEALTH_ENDPOINT_ENABLE, Some("true"))], async {
            let inner = CountingHybridService::default();
            let calls = inner.calls();
            let mut service = public_health_layer().layer(inner);

            let response = service
                .call(
                    Request::builder()
                        .method(Method::GET)
                        .uri(MINIO_HEALTH_LIVE_PATH)
                        .body(Full::<Bytes>::from(Bytes::new()))
                        .expect("request"),
                )
                .await
                .expect("health response");

            assert_eq!(response.status(), StatusCode::OK);
            assert_eq!(calls.load(Ordering::SeqCst), 0);
        })
        .await;
    }

    #[tokio::test]
    #[serial]
    async fn public_health_endpoint_layer_handles_minio_health_ready_head_before_inner_service() {
        async_with_vars([(rustfs_config::ENV_HEALTH_ENDPOINT_ENABLE, Some("true"))], async {
            let inner = CountingHybridService::default();
            let calls = inner.calls();
            let mut service = public_health_layer().layer(inner);

            let response = service
                .call(
                    Request::builder()
                        .method(Method::HEAD)
                        .uri(MINIO_HEALTH_READY_PATH)
                        .body(Full::<Bytes>::from(Bytes::new()))
                        .expect("request"),
                )
                .await
                .expect("health response");

            assert!(response.status() == StatusCode::OK || response.status() == StatusCode::SERVICE_UNAVAILABLE);
            assert_eq!(calls.load(Ordering::SeqCst), 0);

            let body = BodyExt::collect(response.into_body()).await.expect("body").to_bytes();
            assert!(body.is_empty());
        })
        .await;
    }

    #[tokio::test]
    #[serial]
    async fn public_readiness_aliases_use_the_installed_object_progress() {
        async_with_vars(
            [
                (rustfs_config::ENV_HEALTH_ENDPOINT_ENABLE, Some("true")),
                (rustfs_config::ENV_HEALTH_MINIMAL_RESPONSE_ENABLE, Some("false")),
            ],
            async {
                let object_traffic_health = Arc::new(ObjectTrafficHealth::enabled_for_test(Duration::ZERO));
                let stalled = object_traffic_health
                    .track_read_storage()
                    .expect("read tracking must be enabled");
                let inner = CountingHybridService::default();
                let calls = inner.calls();
                let mut service = public_health_layer_with_tracker(Arc::clone(&object_traffic_health))
                    .await
                    .layer(inner);

                let response = service
                    .call(
                        Request::builder()
                            .method(Method::GET)
                            .uri(HEALTH_READY_PATH)
                            .body(Full::<Bytes>::from(Bytes::new()))
                            .expect("canonical readiness request"),
                    )
                    .await
                    .expect("canonical readiness response");
                assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
                let body = BodyExt::collect(response.into_body())
                    .await
                    .expect("readiness body")
                    .to_bytes();
                let payload: serde_json::Value = serde_json::from_slice(&body).expect("readiness JSON");
                assert_eq!(payload["ready"], false);
                assert_eq!(payload["degradedReasons"], serde_json::json!(["object_read_stalled"]));

                let response = service
                    .call(
                        Request::builder()
                            .method(Method::HEAD)
                            .uri(MINIO_HEALTH_READY_PATH)
                            .body(Full::<Bytes>::from(Bytes::new()))
                            .expect("MinIO readiness request"),
                    )
                    .await
                    .expect("MinIO readiness response");
                assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
                assert!(
                    BodyExt::collect(response.into_body())
                        .await
                        .expect("HEAD body")
                        .to_bytes()
                        .is_empty()
                );

                let response = service
                    .call(
                        Request::builder()
                            .method(Method::GET)
                            .uri(HEALTH_COMPAT_LIVE_PATH)
                            .body(Full::<Bytes>::from(Bytes::new()))
                            .expect("liveness request"),
                    )
                    .await
                    .expect("liveness response");
                assert_eq!(response.status(), StatusCode::OK);
                assert_eq!(calls.load(Ordering::SeqCst), 0);

                drop(stalled);
                let response = service
                    .call(
                        Request::builder()
                            .method(Method::HEAD)
                            .uri(HEALTH_READY_PATH)
                            .body(Full::<Bytes>::from(Bytes::new()))
                            .expect("recovered readiness request"),
                    )
                    .await
                    .expect("recovered readiness response");
                assert_eq!(response.status(), StatusCode::OK);
                assert!(
                    BodyExt::collect(response.into_body())
                        .await
                        .expect("HEAD body")
                        .to_bytes()
                        .is_empty()
                );
            },
        )
        .await;
    }

    #[tokio::test]
    #[serial]
    async fn public_health_endpoint_layer_handles_minio_health_cluster_before_inner_service() {
        async_with_vars([(rustfs_config::ENV_HEALTH_ENDPOINT_ENABLE, Some("true"))], async {
            let inner = CountingHybridService::default();
            let calls = inner.calls();
            let mut service = public_health_layer().layer(inner);

            let response = service
                .call(
                    Request::builder()
                        .method(Method::GET)
                        .uri(MINIO_HEALTH_CLUSTER_PATH)
                        .body(Full::<Bytes>::from(Bytes::new()))
                        .expect("request"),
                )
                .await
                .expect("health response");

            assert!(response.status() == StatusCode::OK || response.status() == StatusCode::SERVICE_UNAVAILABLE);
            assert_eq!(calls.load(Ordering::SeqCst), 0);
        })
        .await;
    }

    #[tokio::test]
    #[serial]
    async fn public_health_endpoint_layer_handles_minio_health_cluster_read_before_inner_service() {
        async_with_vars([(rustfs_config::ENV_HEALTH_ENDPOINT_ENABLE, Some("true"))], async {
            let inner = CountingHybridService::default();
            let calls = inner.calls();
            let mut service = public_health_layer().layer(inner);

            let response = service
                .call(
                    Request::builder()
                        .method(Method::GET)
                        .uri(MINIO_HEALTH_CLUSTER_READ_PATH)
                        .body(Full::<Bytes>::from(Bytes::new()))
                        .expect("request"),
                )
                .await
                .expect("health response");

            assert!(response.status() == StatusCode::OK || response.status() == StatusCode::SERVICE_UNAVAILABLE);
            assert_eq!(calls.load(Ordering::SeqCst), 0);
        })
        .await;
    }

    #[tokio::test]
    #[serial]
    async fn public_health_endpoint_layer_forwards_unknown_health_path_when_endpoint_disabled() {
        async_with_vars([(rustfs_config::ENV_HEALTH_ENDPOINT_ENABLE, Some("false"))], async {
            let inner = CountingHybridService::default();
            let calls = inner.calls();
            let mut service = public_health_layer().layer(inner);

            let response = service
                .call(
                    Request::builder()
                        .method(Method::GET)
                        .uri("/health/live")
                        .body(Full::<Bytes>::from(Bytes::new()))
                        .expect("request"),
                )
                .await
                .expect("inner response");

            assert_eq!(response.status(), StatusCode::IM_A_TEAPOT);
            assert_eq!(calls.load(Ordering::SeqCst), 1);
        })
        .await;
    }

    #[tokio::test]
    #[serial]
    async fn public_health_endpoint_layer_forwards_minio_health_alias_when_endpoint_disabled() {
        async_with_vars([(rustfs_config::ENV_HEALTH_ENDPOINT_ENABLE, Some("false"))], async {
            let inner = CountingHybridService::default();
            let calls = inner.calls();
            let mut service = public_health_layer().layer(inner);

            let response = service
                .call(
                    Request::builder()
                        .method(Method::GET)
                        .uri(MINIO_HEALTH_LIVE_PATH)
                        .body(Full::<Bytes>::from(Bytes::new()))
                        .expect("request"),
                )
                .await
                .expect("inner response");

            assert_eq!(response.status(), StatusCode::IM_A_TEAPOT);
            assert_eq!(calls.load(Ordering::SeqCst), 1);
        })
        .await;
    }

    #[test]
    fn alias_busy_threshold_exceeded_requires_switch_and_positive_threshold() {
        with_var(rustfs_config::ENV_HEALTH_COMPAT_BUSY_CHECK_ENABLE, Some("true"), || {
            with_var(rustfs_config::ENV_HEALTH_COMPAT_BUSY_MAX_ACTIVE_REQUESTS, Some("2"), || {
                assert!(!alias_busy_threshold_exceeded(1));
                assert!(alias_busy_threshold_exceeded(2));
                assert!(alias_busy_threshold_exceeded(3));
            });
        });

        with_var(rustfs_config::ENV_HEALTH_COMPAT_BUSY_CHECK_ENABLE, Some("false"), || {
            with_var(rustfs_config::ENV_HEALTH_COMPAT_BUSY_MAX_ACTIVE_REQUESTS, Some("1"), || {
                assert!(!alias_busy_threshold_exceeded(100));
            });
        });
    }

    #[tokio::test]
    #[serial]
    async fn public_health_endpoint_layer_forwards_health_when_endpoint_disabled() {
        async_with_vars([(rustfs_config::ENV_HEALTH_ENDPOINT_ENABLE, Some("false"))], async {
            let inner = CountingHybridService::default();
            let calls = inner.calls();
            let mut service = public_health_layer().layer(inner);

            let response = service
                .call(
                    Request::builder()
                        .method(Method::GET)
                        .uri(HEALTH_PREFIX)
                        .body(Full::<Bytes>::from(Bytes::new()))
                        .expect("request"),
                )
                .await
                .expect("inner response");

            assert_eq!(response.status(), StatusCode::IM_A_TEAPOT);
            assert_eq!(calls.load(Ordering::SeqCst), 1);
        })
        .await;
    }

    #[tokio::test]
    async fn public_health_endpoint_layer_forwards_non_health_requests() {
        let inner = CountingHybridService::default();
        let calls = inner.calls();
        let mut service = public_health_layer().layer(inner);

        let response = service
            .call(
                Request::builder()
                    .method(Method::GET)
                    .uri("/bucket/object")
                    .body(Full::<Bytes>::from(Bytes::new()))
                    .expect("request"),
            )
            .await
            .expect("inner response");

        assert_eq!(response.status(), StatusCode::IM_A_TEAPOT);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn admin_chunked_put_without_content_length_is_normalized() {
        let request = Request::builder()
            .method(Method::PUT)
            .uri("/minio/admin/v3/set-user-status?accessKey=test&status=enabled")
            .body(())
            .expect("request");

        assert!(should_force_zero_content_length_for_empty_body_route(&request));
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
                should_force_zero_content_length_for_empty_body_route(&request),
                "{path} should force Content-Length: 0"
            );
        }
    }

    #[test]
    fn admin_empty_body_get_without_content_length_is_normalized() {
        let paths = [
            format!("{MINIO_ADMIN_PREFIX}/v3/is-admin"),
            format!("{MINIO_ADMIN_PREFIX}/v3/accountinfo"),
            format!("{MINIO_ADMIN_PREFIX}/v3/info"),
            format!("{ADMIN_PREFIX}/v3/is-admin"),
            format!("{ADMIN_PREFIX}/v3/accountinfo"),
            format!("{ADMIN_PREFIX}/v3/info"),
        ];

        for path in paths {
            let request = Request::builder()
                .method(Method::GET)
                .uri(path.as_str())
                .body(())
                .expect("request");

            assert!(
                should_force_zero_content_length_for_empty_body_route(&request),
                "{path} should force Content-Length: 0"
            );
        }
    }

    #[test]
    fn non_s3_non_admin_get_without_content_length_is_not_normalized() {
        let paths = ["/rustfs/rpc/read_file_stream", "/health", "/profile/cpu"];

        for path in paths {
            let request = Request::builder().method(Method::GET).uri(path).body(()).expect("request");

            assert!(
                !should_force_zero_content_length_for_empty_body_route(&request),
                "{path} should not force Content-Length: 0"
            );
        }
    }

    #[tokio::test]
    async fn empty_body_layer_inserts_zero_content_length_for_admin_get() {
        let capture = HeaderCaptureService::default();
        let headers = capture.headers();
        let mut service = EmptyBodyContentLengthCompatLayer.layer(capture);
        let request = Request::builder()
            .method(Method::GET)
            .uri("/rustfs/admin/v3/accountinfo")
            .body(())
            .expect("request");

        let _ = service.call(request).await.expect("service call");

        let headers = headers.lock().expect("captured headers").take().expect("captured headers");
        assert_eq!(headers.get(http::header::CONTENT_LENGTH).unwrap(), "0");
    }

    #[test]
    fn s3_empty_body_get_without_content_length_is_normalized() {
        let paths = [
            "/?x-id=ListBuckets",
            "/",
            "/bucket?list-type=2",
            "/bucket/object.txt",
            "/rustfs/administrator/object",
            "/minio/adminx/object",
        ];

        for path in paths {
            let request = Request::builder().method(Method::GET).uri(path).body(()).expect("request");

            assert!(
                should_force_zero_content_length_for_empty_body_route(&request),
                "{path} should force Content-Length: 0"
            );
        }
    }

    #[test]
    fn s3_empty_body_head_without_content_length_is_normalized() {
        let request = Request::builder()
            .method(Method::HEAD)
            .uri("/bucket/object.txt")
            .body(())
            .expect("request");

        assert!(should_force_zero_content_length_for_empty_body_route(&request));
    }

    #[test]
    fn console_empty_body_get_without_content_length_is_normalized() {
        let paths = [
            FAVICON_PATH.to_string(),
            format!("{CONSOLE_PREFIX}{LICENSE}"),
            format!("{CONSOLE_PREFIX}{VERSION}"),
            format!("{CONSOLE_PREFIX}{HEALTH_PREFIX}"),
            format!("{CONSOLE_PREFIX}{HEALTH_COMPAT_LIVE_PATH}"),
            format!("{CONSOLE_PREFIX}{HEALTH_READY_PATH}"),
            format!("{CONSOLE_PREFIX}/index.html"),
            format!("{CONSOLE_PREFIX}/assets/app.js"),
        ];

        for path in paths {
            let request = Request::builder()
                .method(Method::GET)
                .uri(path.as_str())
                .body(())
                .expect("request");

            assert!(
                should_force_zero_content_length_for_empty_body_route(&request),
                "{path} should force Content-Length: 0"
            );
        }
    }

    #[test]
    fn console_empty_body_head_without_content_length_is_normalized() {
        let paths = [
            format!("{CONSOLE_PREFIX}{HEALTH_PREFIX}"),
            format!("{CONSOLE_PREFIX}{HEALTH_COMPAT_LIVE_PATH}"),
            format!("{CONSOLE_PREFIX}{HEALTH_READY_PATH}"),
            format!("{CONSOLE_PREFIX}/index.html"),
        ];

        for path in paths {
            let request = Request::builder()
                .method(Method::HEAD)
                .uri(path.as_str())
                .body(())
                .expect("request");

            assert!(
                should_force_zero_content_length_for_empty_body_route(&request),
                "{path} should force Content-Length: 0"
            );
        }
    }

    #[tokio::test]
    async fn empty_body_layer_inserts_zero_content_length_for_s3_and_console_get() {
        for path in ["/?x-id=ListBuckets".to_string(), format!("{CONSOLE_PREFIX}/index.html")] {
            let capture = HeaderCaptureService::default();
            let headers = capture.headers();
            let mut service = EmptyBodyContentLengthCompatLayer.layer(capture);
            let request = Request::builder()
                .method(Method::GET)
                .uri(path.as_str())
                .body(())
                .expect("request");

            let _ = service.call(request).await.expect("service call");

            let headers = headers.lock().expect("captured headers").take().expect("captured headers");
            assert_eq!(headers.get(http::header::CONTENT_LENGTH).unwrap(), "0");
        }
    }

    #[tokio::test]
    async fn empty_body_layer_inserts_zero_content_length_for_admin_post() {
        let capture = HeaderCaptureService::default();
        let headers = capture.headers();
        let mut service = EmptyBodyContentLengthCompatLayer.layer(capture);
        let request = Request::builder()
            .method(Method::POST)
            .uri("/rustfs/admin/v3/rebalance/start")
            .body(())
            .expect("request");

        let _ = service.call(request).await.expect("service call");

        let headers = headers.lock().expect("captured headers").take().expect("captured headers");
        assert_eq!(headers.get(http::header::CONTENT_LENGTH).unwrap(), "0");
    }

    #[tokio::test]
    async fn empty_body_layer_inserts_zero_content_length_for_admin_put() {
        let capture = HeaderCaptureService::default();
        let headers = capture.headers();
        let mut service = EmptyBodyContentLengthCompatLayer.layer(capture);
        let request = Request::builder()
            .method(Method::PUT)
            .uri("/rustfs/admin/v3/set-group-status?group=test&status=enabled")
            .body(())
            .expect("request");

        let _ = service.call(request).await.expect("service call");

        let headers = headers.lock().expect("captured headers").take().expect("captured headers");
        assert_eq!(headers.get(http::header::CONTENT_LENGTH).unwrap(), "0");
    }

    #[tokio::test]
    async fn empty_body_layer_normalizes_admin_chunked_request_without_content_length() {
        let capture = HeaderCaptureService::default();
        let headers = capture.headers();
        let mut service = EmptyBodyContentLengthCompatLayer.layer(capture);
        let request = Request::builder()
            .method(Method::POST)
            .uri(format!("{MINIO_ADMIN_V3_PREFIX}/rebalance/start"))
            .header(http::header::TRANSFER_ENCODING, "chunked")
            .body(())
            .expect("request");

        let _ = service.call(request).await.expect("service call");

        let headers = headers.lock().expect("captured headers").take().expect("captured headers");
        assert_eq!(headers.get(http::header::CONTENT_LENGTH).unwrap(), "0");
        assert!(headers.get(http::header::TRANSFER_ENCODING).is_none());
    }

    #[test]
    fn admin_heal_status_query_without_content_length_is_normalized() {
        let paths = [
            format!("{ADMIN_PREFIX}/v3/heal/?clientToken=root-heal"),
            format!("{ADMIN_PREFIX}/v3/heal/bucket?clientToken=bucket-heal"),
        ];

        for path in paths {
            let request = Request::builder()
                .method(Method::POST)
                .uri(path.clone())
                .header(http::header::TRANSFER_ENCODING, "chunked")
                .body(())
                .expect("request");

            assert!(
                should_force_zero_content_length_for_empty_body_route(&request),
                "{path} should force Content-Length: 0"
            );
        }
    }

    #[test]
    fn admin_background_heal_status_without_content_length_is_normalized() {
        let paths = [
            format!("{MINIO_ADMIN_V3_PREFIX}/background-heal/status"),
            format!("{ADMIN_PREFIX}/v3/background-heal/status"),
        ];

        for path in paths {
            let request = Request::builder()
                .method(Method::POST)
                .uri(path.clone())
                .header(http::header::TRANSFER_ENCODING, "chunked")
                .body(())
                .expect("request");

            assert!(
                should_force_zero_content_length_for_empty_body_route(&request),
                "{path} should force Content-Length: 0"
            );
        }
    }

    #[test]
    fn admin_heal_start_without_status_token_is_not_normalized() {
        let request = Request::builder()
            .method(Method::POST)
            .uri(format!("{ADMIN_PREFIX}/v3/heal/"))
            .header(http::header::TRANSFER_ENCODING, "chunked")
            .body(())
            .expect("request");

        assert!(!should_force_zero_content_length_for_empty_body_route(&request));
    }

    #[test]
    fn s3_delete_object_version_without_content_length_is_normalized() {
        let request = Request::builder()
            .method(Method::DELETE)
            .uri("/bucket/object.txt?versionId=3HL4kqtJlcpXrof3Gj0OmxJnVBH40Nrjfkd")
            .body(())
            .expect("request");

        assert!(should_force_zero_content_length_for_empty_body_route(&request));
    }

    #[tokio::test]
    async fn empty_body_layer_inserts_zero_content_length_for_s3_delete_object_version() {
        let capture = HeaderCaptureService::default();
        let headers = capture.headers();
        let mut service = EmptyBodyContentLengthCompatLayer.layer(capture);
        let request = Request::builder()
            .method(Method::DELETE)
            .uri("/bucket/object.txt?versionId=3HL4kqtJlcpXrof3Gj0OmxJnVBH40Nrjfkd")
            .body(())
            .expect("request");

        let _ = service.call(request).await.expect("service call");

        let headers = headers.lock().expect("captured headers").take().expect("captured headers");
        assert_eq!(headers.get(http::header::CONTENT_LENGTH).unwrap(), "0");
    }

    #[tokio::test]
    async fn empty_body_layer_preserves_explicit_content_length_header() {
        let capture = HeaderCaptureService::default();
        let headers = capture.headers();
        let mut service = EmptyBodyContentLengthCompatLayer.layer(capture);
        let request = Request::builder()
            .method(Method::PUT)
            .uri("/minio/admin/v3/set-group-status?group=test&status=enabled")
            .header(http::header::CONTENT_LENGTH, "7")
            .body(())
            .expect("request");

        let _ = service.call(request).await.expect("service call");

        let headers = headers.lock().expect("captured headers").take().expect("captured headers");
        assert_eq!(headers.get(http::header::CONTENT_LENGTH).unwrap(), "7");
    }

    #[test]
    fn admin_request_with_explicit_content_length_is_left_unchanged() {
        let request = Request::builder()
            .method(Method::PUT)
            .uri("/minio/admin/v3/set-group-status?group=test&status=enabled")
            .header(http::header::CONTENT_LENGTH, "0")
            .body(())
            .expect("request");

        assert!(!should_force_zero_content_length_for_empty_body_route(&request));
    }

    #[test]
    fn admin_restore_config_history_without_content_length_is_normalized() {
        let request = Request::builder()
            .method(Method::PUT)
            .uri("/minio/admin/v3/restore-config-history-kv?restoreId=test")
            .body(())
            .expect("request");

        assert!(should_force_zero_content_length_for_empty_body_route(&request));
    }

    #[test]
    fn s3_put_object_is_not_normalized() {
        let request = Request::builder()
            .method(Method::PUT)
            .uri("/bucket/object")
            .body(())
            .expect("request");

        assert!(!should_force_zero_content_length_for_empty_body_route(&request));
    }

    #[test]
    fn s3_delete_bucket_without_content_length_is_normalized() {
        let request = Request::builder()
            .method(Method::DELETE)
            .uri("/bucket")
            .body(())
            .expect("request");

        assert!(should_force_zero_content_length_for_empty_body_route(&request));
    }

    #[test]
    fn s3_delete_object_without_version_id_is_normalized() {
        let request = Request::builder()
            .method(Method::DELETE)
            .uri("/bucket/object")
            .body(())
            .expect("request");

        assert!(should_force_zero_content_length_for_empty_body_route(&request));
    }

    #[test]
    fn s3_delete_with_transfer_encoding_is_not_normalized() {
        let request = Request::builder()
            .method(Method::DELETE)
            .uri("/bucket/object?versionId=3HL4kqtJlcpXrof3Gj0OmxJnVBH40Nrjfkd")
            .header(http::header::TRANSFER_ENCODING, "chunked")
            .body(())
            .expect("request");

        assert!(!should_force_zero_content_length_for_empty_body_route(&request));
    }

    #[test]
    fn s3_get_with_transfer_encoding_is_not_normalized() {
        let request = Request::builder()
            .method(Method::GET)
            .uri("/?x-id=ListBuckets")
            .header(http::header::TRANSFER_ENCODING, "chunked")
            .body(())
            .expect("request");

        assert!(!should_force_zero_content_length_for_empty_body_route(&request));
    }

    #[test]
    fn console_get_with_transfer_encoding_is_not_normalized() {
        let request = Request::builder()
            .method(Method::GET)
            .uri(format!("{CONSOLE_PREFIX}/index.html"))
            .header(http::header::TRANSFER_ENCODING, "chunked")
            .body(())
            .expect("request");

        assert!(!should_force_zero_content_length_for_empty_body_route(&request));
    }

    #[test]
    fn console_post_without_content_length_is_not_normalized() {
        let request = Request::builder()
            .method(Method::POST)
            .uri(format!("{CONSOLE_PREFIX}/upload"))
            .body(())
            .expect("request");

        assert!(!should_force_zero_content_length_for_empty_body_route(&request));
    }

    #[test]
    fn non_s3_delete_paths_are_not_normalized() {
        let paths = [
            "/minio/admin/v3/pools/cancel?versionId=unused",
            "/rustfs/admin/v3/pools/cancel?versionId=unused",
            "/rustfs/rpc/read_file_stream?versionId=unused",
            "/rustfs/console/index.html?versionId=unused",
            "/health?versionId=unused",
            "/health/ready?versionId=unused",
            "/profile/cpu?versionId=unused",
            "/profile/memory?versionId=unused",
        ];

        for path in paths {
            let request = Request::builder().method(Method::DELETE).uri(path).body(()).expect("request");

            assert!(
                !should_force_zero_content_length_for_empty_body_route(&request),
                "{path} should not force Content-Length: 0"
            );
        }
    }

    #[test]
    fn non_empty_body_admin_post_path_is_not_normalized() {
        let request = Request::builder()
            .method(Method::POST)
            .uri("/minio/admin/v3/update-service-account")
            .body(())
            .expect("request");

        assert!(!should_force_zero_content_length_for_empty_body_route(&request));
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

    #[derive(Clone)]
    struct FixedHybridResponse {
        status: StatusCode,
        body: Bytes,
        content_type: &'static str,
    }

    impl<B: Send + 'static> Service<Request<B>> for FixedHybridResponse {
        type Response = Response<HybridBody<Full<Bytes>, Empty<Bytes>>>;
        type Error = Infallible;
        type Future = Ready<Result<Self::Response, Self::Error>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _req: Request<B>) -> Self::Future {
            let body = self.body.clone();
            ready(Ok(Response::builder()
                .status(self.status)
                .header(http::header::CONTENT_TYPE, self.content_type)
                .header(http::header::CONTENT_LENGTH, body.len().to_string())
                .body(HybridBody::Rest {
                    rest_body: Full::from(body),
                })
                .expect("fixed hybrid response")))
        }
    }

    async fn collect_hybrid_response(
        response: Response<HybridBody<Full<Bytes>, Empty<Bytes>>>,
    ) -> (StatusCode, HeaderMap, String) {
        let status = response.status();
        let headers = response.headers().clone();
        let body = BodyExt::collect(response.into_body())
            .await
            .expect("collect hybrid body")
            .to_bytes();
        (
            status,
            headers,
            String::from_utf8(body.to_vec()).expect("hybrid response body should be UTF-8"),
        )
    }

    #[tokio::test]
    async fn s3_error_message_compat_fixes_regular_forbidden_xml() {
        let body = Bytes::from_static(b"<Error><Code>SignatureDoesNotMatch</Code></Error>");
        let mut service = S3ErrorMessageCompatLayer.layer(FixedHybridResponse {
            status: StatusCode::FORBIDDEN,
            body,
            content_type: "application/xml",
        });
        let request = Request::builder()
            .method(Method::GET)
            .uri("/bucket/object")
            .body(())
            .expect("request");

        let response = service.call(request).await.expect("service response");
        let (status, headers, body) = collect_hybrid_response(response).await;

        assert_eq!(status, StatusCode::FORBIDDEN);
        assert!(headers.get(http::header::CONTENT_LENGTH).is_none());
        assert!(body.contains("<Message>"));
    }

    #[tokio::test]
    async fn s3_error_message_compat_leaves_sts_query_response_unchanged() {
        let input = Bytes::from_static(b"<Error><Code>SignatureDoesNotMatch</Code></Error>");
        let mut service = S3ErrorMessageCompatLayer.layer(FixedHybridResponse {
            status: StatusCode::FORBIDDEN,
            body: input.clone(),
            content_type: "application/xml",
        });
        let mut request = Request::builder().method(Method::POST).uri("/").body(()).expect("request");
        request.extensions_mut().insert(StsQueryRequest);

        let response = service.call(request).await.expect("service response");
        let (_status, headers, body) = collect_hybrid_response(response).await;

        let expected_len = input.len().to_string();
        assert_eq!(
            headers
                .get(http::header::CONTENT_LENGTH)
                .and_then(|value| value.to_str().ok()),
            Some(expected_len.as_str())
        );
        assert_eq!(body.as_bytes(), input.as_ref());
    }

    #[tokio::test]
    async fn iceberg_rest_error_compat_converts_catalog_xml_errors() {
        let mut service = IcebergRestErrorCompatLayer.layer(FixedHybridResponse {
            status: StatusCode::NOT_FOUND,
            body: Bytes::from_static(b"<Error><Code>NoSuchTableException</Code><Message>missing</Message></Error>"),
            content_type: "application/xml",
        });
        let request = Request::builder()
            .method(Method::GET)
            .uri("/iceberg/v1/warehouse/namespaces/ns/tables/events")
            .body(())
            .expect("request");

        let response = service.call(request).await.expect("service response");
        let (status, headers, body) = collect_hybrid_response(response).await;

        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(headers.get(http::header::CONTENT_TYPE).unwrap(), "application/json");
        assert!(headers.get(http::header::CONTENT_LENGTH).is_none());
        assert!(body.contains("\"type\":\"NoSuchTableException\""));
    }

    #[tokio::test]
    async fn iceberg_rest_error_compat_leaves_non_catalog_errors_unchanged() {
        let input = Bytes::from_static(b"<Error><Code>NoSuchKey</Code><Message>missing</Message></Error>");
        let mut service = IcebergRestErrorCompatLayer.layer(FixedHybridResponse {
            status: StatusCode::NOT_FOUND,
            body: input.clone(),
            content_type: "application/xml",
        });
        let request = Request::builder()
            .method(Method::GET)
            .uri("/bucket/object")
            .body(())
            .expect("request");

        let response = service.call(request).await.expect("service response");
        let (status, headers, body) = collect_hybrid_response(response).await;

        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(headers.get(http::header::CONTENT_TYPE).unwrap(), "application/xml");
        assert_eq!(body.as_bytes(), input.as_ref());
    }

    #[tokio::test]
    async fn object_attributes_etag_fix_rewrites_target_response() {
        let mut service = ObjectAttributesEtagFixLayer.layer(FixedHybridResponse {
            status: StatusCode::OK,
            body: Bytes::from_static(b"<GetObjectAttributesOutput><ETag>\"abc\"</ETag></GetObjectAttributesOutput>"),
            content_type: "application/xml",
        });
        let request = Request::builder()
            .method(Method::GET)
            .uri("/bucket/object?attributes")
            .body(())
            .expect("request");

        let response = service.call(request).await.expect("service response");
        let (_status, headers, body) = collect_hybrid_response(response).await;

        assert!(headers.get(http::header::CONTENT_LENGTH).is_none());
        assert!(body.contains("<ETag>abc</ETag>"));
    }

    #[tokio::test]
    async fn object_attributes_etag_fix_leaves_regular_get_unchanged() {
        let input = Bytes::from_static(b"<GetObjectAttributesOutput><ETag>\"abc\"</ETag></GetObjectAttributesOutput>");
        let mut service = ObjectAttributesEtagFixLayer.layer(FixedHybridResponse {
            status: StatusCode::OK,
            body: input.clone(),
            content_type: "application/xml",
        });
        let request = Request::builder()
            .method(Method::GET)
            .uri("/bucket/object")
            .body(())
            .expect("request");

        let response = service.call(request).await.expect("service response");
        let (_status, headers, body) = collect_hybrid_response(response).await;

        let expected_len = input.len().to_string();
        assert_eq!(
            headers
                .get(http::header::CONTENT_LENGTH)
                .and_then(|value| value.to_str().ok()),
            Some(expected_len.as_str())
        );
        assert_eq!(body.as_bytes(), input.as_ref());
    }

    #[derive(Clone)]
    struct FixedStsResponse {
        status: StatusCode,
        body: Bytes,
        upstream_request_id: Option<&'static str>,
    }

    impl<RequestBody: Send + 'static> Service<Request<RequestBody>> for FixedStsResponse {
        type Response = Response<StsBoxBody>;
        type Error = StsBoxError;
        type Future = Ready<Result<Self::Response, Self::Error>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _req: Request<RequestBody>) -> Self::Future {
            let body = tower_http::body::UnsyncBoxBody::new(
                Full::from(self.body.clone()).map_err(|error| -> StsBoxError { match error {} }),
            );
            let mut response = Response::builder()
                .status(self.status)
                .header(http::header::CONTENT_TYPE, "application/xml")
                .header(http::header::CONTENT_LENGTH, self.body.len());
            if let Some(request_id) = self.upstream_request_id {
                response = response.header(AMZ_REQUEST_ID, request_id);
            }
            ready(Ok(response.body(body).expect("fixed STS response")))
        }
    }

    async fn call_sts_compat_layer(
        status: StatusCode,
        body: &'static [u8],
        upstream_request_id: Option<&'static str>,
    ) -> (HeaderMap, String) {
        let mut service = StsQueryApiCompatLayer.layer(FixedStsResponse {
            status,
            body: Bytes::from_static(body),
            upstream_request_id,
        });
        let mut request = Request::builder()
            .method(Method::POST)
            .uri("/")
            .header(http::header::CONTENT_TYPE, "application/x-www-form-urlencoded")
            .body(())
            .expect("STS request");
        request.extensions_mut().insert(RequestContext {
            request_id: "server-request-id".to_string(),
            x_amz_request_id: "untrusted-upstream-id".to_string(),
            trace_id: None,
            span_id: None,
            start_time: Instant::now(),
        });

        let response = service.call(request).await.expect("STS compatibility response");
        let headers = response.headers().clone();
        let body = BodyExt::collect(response.into_body())
            .await
            .expect("collect STS response")
            .to_bytes();
        (headers, String::from_utf8(body.to_vec()).expect("STS response should be UTF-8 XML"))
    }

    #[tokio::test]
    async fn sts_compat_layer_uses_canonical_request_id_for_success_header_and_xml() {
        let (headers, body) = call_sts_compat_layer(
            StatusCode::OK,
            b"<AssumeRoleResponse><AssumeRoleResult/></AssumeRoleResponse>",
            Some("upstream-request-id"),
        )
        .await;

        assert_eq!(
            headers.get(AMZ_REQUEST_ID).and_then(|value| value.to_str().ok()),
            Some("server-request-id")
        );
        assert!(!headers.contains_key(http::header::CONTENT_LENGTH));
        assert!(body.contains("<ResponseMetadata><RequestId>server-request-id</RequestId></ResponseMetadata>"));
        assert!(!body.contains("untrusted-upstream-id"));
    }

    #[tokio::test]
    async fn sts_compat_layer_uses_same_request_id_for_error_header_and_xml() {
        let (headers, body) = call_sts_compat_layer(
            StatusCode::FORBIDDEN,
            b"<Error><Code>AccessDenied</Code><Message>Access Denied</Message></Error>",
            None,
        )
        .await;

        assert_eq!(
            headers.get(AMZ_REQUEST_ID).and_then(|value| value.to_str().ok()),
            Some("server-request-id")
        );
        assert!(body.contains("<ErrorResponse xmlns=\"https://sts.amazonaws.com/doc/2011-06-15/\">"));
        assert!(body.contains("<RequestId>server-request-id</RequestId>"));
    }

    #[test]
    fn sts_response_request_id_rejects_empty_context_id() {
        let mut request = Request::new(());
        request.extensions_mut().insert(RequestContext {
            request_id: String::new(),
            x_amz_request_id: String::new(),
            trace_id: None,
            span_id: None,
            start_time: Instant::now(),
        });

        let (request_id, header) = sts_response_request_id(&request);
        assert_eq!(request_id, "sts-request-id");
        assert_eq!(header, HeaderValue::from_static("sts-request-id"));
    }

    #[test]
    fn sts_query_route_match_requires_post_root_and_form_content_type() {
        let uri = Uri::from_static("/");
        let mut headers = HeaderMap::new();

        assert!(!is_sts_query_request(&Method::POST, &uri, &headers));

        headers.insert(http::header::CONTENT_TYPE, HeaderValue::from_static("application/json"));
        assert!(!is_sts_query_request(&Method::POST, &uri, &headers));

        headers.insert(
            http::header::CONTENT_TYPE,
            HeaderValue::from_static("application/x-www-form-urlencoded; charset=utf-8"),
        );
        assert!(is_sts_query_request(&Method::POST, &uri, &headers));
        assert!(!is_sts_query_request(&Method::GET, &uri, &headers));
        assert!(!is_sts_query_request(&Method::POST, &Uri::from_static("/bucket"), &headers));
    }

    #[test]
    fn sts_success_response_gets_response_metadata() {
        let xml = "<AssumeRoleResponse xmlns=\"https://sts.amazonaws.com/doc/2011-06-15/\">\
             <AssumeRoleResult><Credentials/></AssumeRoleResult></AssumeRoleResponse>";
        let converted = add_sts_response_metadata(xml, "request-123").expect("AssumeRole response should be converted");
        let converted = std::str::from_utf8(&converted).expect("converted response should be UTF-8 XML");

        assert!(converted.contains("<Credentials/>"));
        assert!(converted.contains("<ResponseMetadata><RequestId>request-123</RequestId></ResponseMetadata>"));
        assert!(converted.ends_with("</AssumeRoleResponse>"));
    }

    #[test]
    fn sts_web_identity_success_response_gets_response_metadata() {
        let xml = "<AssumeRoleWithWebIdentityResponse xmlns=\"https://sts.amazonaws.com/doc/2011-06-15/\">\
             <AssumeRoleWithWebIdentityResult><Credentials/></AssumeRoleWithWebIdentityResult>\
             </AssumeRoleWithWebIdentityResponse>";
        let converted =
            add_sts_response_metadata(xml, "request-123").expect("AssumeRoleWithWebIdentity response should be converted");
        let converted = std::str::from_utf8(&converted).expect("converted response should be UTF-8 XML");

        assert!(converted.contains("<Credentials/>"));
        assert!(converted.contains("<ResponseMetadata><RequestId>request-123</RequestId></ResponseMetadata>"));
        assert!(converted.ends_with("</AssumeRoleWithWebIdentityResponse>"));
    }

    #[test]
    fn sts_namespaced_success_response_gets_namespaced_metadata() {
        let xml = "<sts:AssumeRoleResponse xmlns:sts=\"https://sts.amazonaws.com/doc/2011-06-15/\">\
                   <sts:AssumeRoleResult/></sts:AssumeRoleResponse>";
        let converted = add_sts_response_metadata(xml, "request-123").expect("namespaced response should be converted");
        let converted = std::str::from_utf8(&converted).expect("converted response should be UTF-8 XML");

        assert!(converted.contains("<sts:ResponseMetadata><sts:RequestId>request-123</sts:RequestId></sts:ResponseMetadata>"));
        assert!(converted.ends_with("</sts:AssumeRoleResponse>"));
    }

    #[test]
    fn sts_success_response_with_existing_metadata_is_unchanged() {
        let xml = "<AssumeRoleResponse xmlns=\"https://sts.amazonaws.com/doc/2011-06-15/\">\
                   <AssumeRoleResult/>\
                   <ResponseMetadata source=\"upstream\"><RequestId>existing</RequestId></ResponseMetadata>\
                   </AssumeRoleResponse>";

        assert!(add_sts_response_metadata(xml, "request-123").is_none());
    }

    #[test]
    fn sts_error_response_uses_query_api_envelope() {
        let converted = wrap_sts_error_response(
            "<Error><Code>AccessDenied</Code><Message>Access &amp; Denied</Message></Error>",
            StatusCode::FORBIDDEN,
            "request-456",
        );

        assert!(converted.contains("<ErrorResponse xmlns=\"https://sts.amazonaws.com/doc/2011-06-15/\">"));
        assert!(converted.contains("<Type>Sender</Type>"));
        assert!(converted.contains("<Code>AccessDenied</Code>"));
        assert!(converted.contains("<Message>Access &amp; Denied</Message>"));
        assert!(converted.contains("<RequestId>request-456</RequestId>"));
    }

    #[test]
    fn sts_signature_error_response_fills_missing_message() {
        let converted =
            wrap_sts_error_response("<Error><Code>SignatureDoesNotMatch</Code></Error>", StatusCode::FORBIDDEN, "request-456");
        let expected = ApiError::error_code_to_message(&S3ErrorCode::SignatureDoesNotMatch);

        assert!(converted.contains(&format!("<Message>{expected}</Message>")));
    }

    #[tokio::test]
    async fn sts_compat_layer_wraps_non_xml_short_circuit_errors() {
        let (headers, body) =
            call_sts_compat_layer(StatusCode::SERVICE_UNAVAILABLE, b"Service not ready: waiting for iam", None).await;

        assert_eq!(
            headers.get(AMZ_REQUEST_ID).and_then(|value| value.to_str().ok()),
            Some("server-request-id")
        );
        assert!(body.contains("<Type>Receiver</Type>"));
        assert!(body.contains("<Code>ServiceUnavailable</Code>"));
        assert!(body.contains("<Message>Service Unavailable</Message>"));
        assert!(body.contains("<RequestId>server-request-id</RequestId>"));
        assert!(!body.contains("waiting for iam"));
    }

    #[tokio::test]
    async fn sts_compat_layer_replaces_upstream_and_duplicate_error_request_ids() {
        let (headers, body) = call_sts_compat_layer(
            StatusCode::FORBIDDEN,
            b"<Error><Code>AccessDenied</Code><Message>Access Denied</Message>\
              <RequestId>body-request-id-1</RequestId><RequestId>body-request-id-2</RequestId></Error>",
            Some("header-request-id"),
        )
        .await;

        assert_eq!(
            headers.get(AMZ_REQUEST_ID).and_then(|value| value.to_str().ok()),
            Some("server-request-id")
        );
        assert_eq!(body.matches("<RequestId>").count(), 1);
        assert!(body.contains("<RequestId>server-request-id</RequestId>"));
        for stale_request_id in ["header-request-id", "body-request-id-1", "body-request-id-2"] {
            assert!(!body.contains(stale_request_id));
        }
    }

    #[tokio::test]
    async fn sts_compat_layer_falls_back_for_empty_and_malformed_error_xml() {
        for (body, status, expected_code, expected_message) in [
            (
                b"".as_slice(),
                StatusCode::SERVICE_UNAVAILABLE,
                "ServiceUnavailable",
                "Service Unavailable",
            ),
            (
                b"<Error><Code>AccessDenied".as_slice(),
                StatusCode::BAD_GATEWAY,
                "InternalError",
                "Internal Server Error",
            ),
        ] {
            let (headers, body) = call_sts_compat_layer(status, body, None).await;

            assert_eq!(
                headers.get(AMZ_REQUEST_ID).and_then(|value| value.to_str().ok()),
                Some("server-request-id")
            );
            assert!(body.contains("<Type>Receiver</Type>"));
            assert!(body.contains(&format!("<Code>{expected_code}</Code>")));
            assert!(body.contains(&format!("<Message>{expected_message}</Message>")));
            assert_eq!(body.matches("<RequestId>").count(), 1);
            assert!(body.contains("<RequestId>server-request-id</RequestId>"));
        }
    }

    #[tokio::test]
    async fn sts_compat_layer_prevents_pre_conversion_compression() {
        let xml = format!(
            "<AssumeRoleResponse><AssumeRoleResult>{}</AssumeRoleResult></AssumeRoleResponse>",
            "x".repeat(2048)
        );
        let mut service = tower::ServiceBuilder::new()
            .layer(StsQueryApiCompatLayer)
            .layer(tower_http::catch_panic::CatchPanicLayer::new())
            .layer(
                tower_http::compression::CompressionLayer::new().compress_when(PathAwareHttpCompressionPredicate::new(
                    HttpCompressionConfig {
                        enabled: true,
                        extensions: Vec::new(),
                        mime_patterns: vec!["application/xml".to_owned()],
                        min_size: 0,
                    },
                )),
            )
            .layer(PathCategoryInjectionLayer)
            .service(FixedStsResponse {
                status: StatusCode::OK,
                body: Bytes::from(xml),
                upstream_request_id: None,
            });
        let mut request = Request::builder()
            .method(Method::POST)
            .uri("/")
            .header(http::header::CONTENT_TYPE, "application/x-www-form-urlencoded")
            .header(http::header::ACCEPT_ENCODING, "gzip")
            .body(())
            .expect("STS request");
        request.extensions_mut().insert(RequestContext {
            request_id: "server-request-id".to_string(),
            x_amz_request_id: String::new(),
            trace_id: None,
            span_id: None,
            start_time: Instant::now(),
        });

        let response = service.call(request).await.expect("STS compatibility response");
        assert!(
            !response.headers().contains_key(http::header::CONTENT_ENCODING),
            "STS XML must stay uncompressed until the compatibility envelope is complete"
        );
        let body = BodyExt::collect(response.into_body())
            .await
            .expect("collect STS response")
            .to_bytes();
        let body = String::from_utf8(body.to_vec()).expect("STS response should be UTF-8 XML");
        assert!(body.contains("<ResponseMetadata><RequestId>server-request-id</RequestId></ResponseMetadata>"));
    }

    #[tokio::test]
    async fn iceberg_rest_error_conversion_returns_standard_json_envelope() {
        let body = Full::from(Bytes::from_static(
            b"<Error><Code>NoSuchTableException</Code><Message>table not found</Message></Error>",
        ));

        let (body, status) =
            convert_iceberg_error_in_xml(body, StatusCode::NOT_FOUND, "/iceberg/v1/warehouse/namespaces/ns/tables/events")
                .await
                .expect("convert Iceberg error");
        let value: serde_json::Value =
            serde_json::from_slice(&BodyExt::collect(body).await.expect("collect converted body").to_bytes())
                .expect("JSON error envelope");

        assert_eq!(status, Some(StatusCode::NOT_FOUND));
        assert_eq!(value["error"]["code"], 404);
        assert_eq!(value["error"]["type"], "NoSuchTableException");
        assert_eq!(value["error"]["message"], "table not found");
    }

    #[tokio::test]
    async fn iceberg_rest_error_conversion_maps_precondition_to_commit_conflict() {
        let body = Full::from(Bytes::from_static(
            b"<Error><Code>PreconditionFailed</Code><Message>version token changed</Message></Error>",
        ));

        let (body, status) = convert_iceberg_error_in_xml(
            body,
            StatusCode::PRECONDITION_FAILED,
            "/_iceberg/v1/warehouse/namespaces/ns/tables/events",
        )
        .await
        .expect("convert Iceberg conflict");
        let value: serde_json::Value =
            serde_json::from_slice(&BodyExt::collect(body).await.expect("collect converted body").to_bytes())
                .expect("JSON error envelope");

        assert_eq!(status, Some(StatusCode::CONFLICT));
        assert_eq!(value["error"]["code"], 409);
        assert_eq!(value["error"]["type"], "CommitFailedException");
    }

    #[tokio::test]
    async fn iceberg_rest_error_conversion_preserves_forbidden_status() {
        let body = Full::from(Bytes::from_static(
            b"<Error><Code>SignatureDoesNotMatch</Code><Message>signature mismatch</Message></Error>",
        ));

        let (body, status) = convert_iceberg_error_in_xml(body, StatusCode::FORBIDDEN, "/iceberg/v1/config")
            .await
            .expect("convert Iceberg auth error");
        let value: serde_json::Value =
            serde_json::from_slice(&BodyExt::collect(body).await.expect("collect converted body").to_bytes())
                .expect("JSON error envelope");

        assert_eq!(status, Some(StatusCode::FORBIDDEN));
        assert_eq!(value["error"]["code"], 403);
        assert_eq!(value["error"]["type"], "ForbiddenException");
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
        assert!(!ConditionalCorsLayer::is_s3_path(&format!(
            "{}/config",
            crate::server::TABLE_CATALOG_PREFIX
        )));
        assert!(!ConditionalCorsLayer::is_s3_path("/_iceberg/v1/config"));
        assert!(ConditionalCorsLayer::is_s3_path("/minio/adminx/object"));
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
        let exposed = resp_headers
            .get(cors::response::ACCESS_CONTROL_EXPOSE_HEADERS)
            .and_then(|value| value.to_str().ok())
            .expect("exposed response headers");
        assert!(exposed.split(',').any(|header| header.trim() == "x-request-id"));
        assert!(exposed.split(',').any(|header| header.trim() == "x-amz-request-id"));
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

    #[derive(Clone)]
    struct CorsOkService;

    impl<B> Service<Request<B>> for CorsOkService {
        type Response = Response<Empty<Bytes>>;
        type Error = Infallible;
        type Future = Ready<Result<Self::Response, Self::Error>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _req: Request<B>) -> Self::Future {
            ready(Ok(Response::builder()
                .status(StatusCode::OK)
                .body(Empty::new())
                .expect("response")))
        }
    }

    #[tokio::test]
    async fn conditional_cors_passthrough_without_origin() {
        let layer = ConditionalCorsLayer {
            cors_origins: Some("*".to_string()),
        };
        let mut service = layer.layer(CorsOkService);
        let request = Request::builder()
            .method(Method::GET)
            .uri("/bucket/object")
            .body(())
            .expect("request");

        let response = service.call(request).await.expect("response");

        assert_eq!(response.status(), StatusCode::OK);
        assert!(response.headers().get(cors::response::ACCESS_CONTROL_ALLOW_ORIGIN).is_none());
    }

    #[tokio::test]
    async fn conditional_cors_applies_origin_headers() {
        let layer = ConditionalCorsLayer {
            cors_origins: Some("*".to_string()),
        };
        let mut service = layer.layer(CorsOkService);
        let request = Request::builder()
            .method(Method::GET)
            .uri("/bucket/object")
            .header(cors::standard::ORIGIN, "https://example.com")
            .body(())
            .expect("request");

        let response = service.call(request).await.expect("response");

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers().get(cors::response::ACCESS_CONTROL_ALLOW_ORIGIN).unwrap(), "*");
    }

    #[test]
    fn request_context_layer_populates_context_without_mutating_signed_headers() {
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
        assert!(request.headers().get(AMZ_REQUEST_ID).is_none());
    }

    #[test]
    fn request_context_layer_does_not_mutate_upstream_s3_request_id() {
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

    #[test]
    fn request_context_layer_extracts_trace_context_from_traceparent_header() {
        global::set_text_map_propagator(TraceContextPropagator::new());

        let mut service = RequestContextLayer.layer(CaptureService);
        let request = Request::builder()
            .uri("/bucket/object")
            .header("x-request-id", "req-trace-123")
            .header("traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
            .body(())
            .expect("request");

        let request = service.call(request).into_inner().expect("service call should succeed");
        let context = request
            .extensions()
            .get::<RequestContext>()
            .expect("request context should be present");

        assert_eq!(context.request_id, "req-trace-123");
        assert_eq!(context.trace_id.as_deref(), Some("4bf92f3577b34da6a3ce929d0e0e4736"));
        assert_eq!(context.span_id.as_deref(), Some("00f067aa0ba902b7"));
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

    #[derive(Clone)]
    struct StatusService {
        status: StatusCode,
    }

    impl StatusService {
        fn new(status: StatusCode) -> Self {
            Self { status }
        }
    }

    impl<B> Service<Request<B>> for StatusService {
        type Response = Response<Full<Bytes>>;
        type Error = Infallible;
        type Future = Ready<Result<Self::Response, Self::Error>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _req: Request<B>) -> Self::Future {
            ready(Ok(Response::builder()
                .status(self.status)
                .body(Full::from(Bytes::new()))
                .expect("response")))
        }
    }

    #[derive(Clone, Default)]
    struct SharedWriter {
        buffer: Arc<Mutex<Vec<u8>>>,
    }

    struct SharedWriterGuard {
        buffer: Arc<Mutex<Vec<u8>>>,
    }

    impl Write for SharedWriterGuard {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.buffer.lock().expect("log buffer").extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl<'writer> MakeWriter<'writer> for SharedWriter {
        type Writer = SharedWriterGuard;

        fn make_writer(&'writer self) -> Self::Writer {
            SharedWriterGuard {
                buffer: self.buffer.clone(),
            }
        }
    }

    #[test]
    fn request_log_context_classifies_statuses() {
        assert_eq!(RequestLogContext::result_label(StatusCode::OK), "success");
        assert_eq!(RequestLogContext::result_label(StatusCode::TEMPORARY_REDIRECT), "redirect");
        assert_eq!(RequestLogContext::result_label(StatusCode::BAD_REQUEST), "client_error");
        assert_eq!(RequestLogContext::result_label(StatusCode::INTERNAL_SERVER_ERROR), "server_error");
    }

    #[test]
    fn request_log_context_prefers_request_context_and_remote_addr_extensions() {
        let mut request = Request::builder()
            .method(Method::PUT)
            .uri("/bucket/object.txt")
            .body(())
            .expect("request");
        request.extensions_mut().insert(RequestContext {
            request_id: "req-ctx".to_string(),
            x_amz_request_id: "amz-ctx".to_string(),
            trace_id: Some("trace-123".to_string()),
            span_id: Some("span-456".to_string()),
            start_time: Instant::now(),
        });
        request
            .extensions_mut()
            .insert(RemoteAddr("127.0.0.1:9000".parse().expect("socket addr")));

        let context = RequestLogContext::from_request(&request);

        assert_eq!(context.request_id, "req-ctx");
        assert_eq!(context.trace_id.as_deref(), Some("trace-123"));
        assert_eq!(context.span_id.as_deref(), Some("span-456"));
        assert_eq!(context.peer_addr(), "127.0.0.1:9000");
        assert_eq!(context.method.as_str(), "PUT");
        assert_eq!(context.redacted_uri(), "/bucket/object.txt");
    }

    #[test]
    fn request_log_context_redacts_object_zip_download_tokens() {
        let request = Request::builder()
            .method(Method::GET)
            .uri("/rustfs/admin/v3/object-zip-downloads/download-id.zip?token=secret-token&part=1")
            .body(())
            .expect("request");

        let context = RequestLogContext::from_request(&request);

        let uri = context.redacted_uri();
        assert_eq!(uri, "/rustfs/admin/v3/object-zip-downloads/download-id.zip?token=redacted&part=1");
        assert!(!uri.contains("secret-token"));
    }

    #[test]
    fn request_log_context_redacts_object_zip_download_tokens_for_minio_admin_prefix() {
        let request = Request::builder()
            .method(Method::GET)
            .uri("/minio/admin/v3/object-zip-downloads/download-id.zip?token=secret-token&part=1")
            .body(())
            .expect("request");

        let context = RequestLogContext::from_request(&request);

        let uri = context.redacted_uri();
        assert_eq!(uri, "/minio/admin/v3/object-zip-downloads/download-id.zip?token=redacted&part=1");
        assert!(!uri.contains("secret-token"));
    }

    #[test]
    fn redact_sensitive_uri_query_preserves_non_zip_download_uris() {
        let uri: http::Uri = "/rustfs/admin/v3/users?token=not-a-download-token".parse().expect("uri");

        assert_eq!(redact_sensitive_uri_query(&uri), "/rustfs/admin/v3/users?token=not-a-download-token");
    }

    #[tokio::test]
    async fn request_logging_layer_emits_single_completion_event_with_standard_fields() {
        let writer = SharedWriter::default();
        let captured = writer.buffer.clone();
        let subscriber = Registry::default().with(
            tracing_subscriber::fmt::layer()
                .without_time()
                .with_target(false)
                .with_level(false)
                .with_ansi(false)
                .with_writer(writer),
        );

        let _guard = tracing::subscriber::set_default(subscriber);

        let mut service = tower::ServiceBuilder::new()
            .layer(RequestContextLayer)
            .layer(RequestLoggingLayer)
            .service(StatusService::new(StatusCode::OK));

        let mut request: Request<Full<Bytes>> = Request::builder()
            .method(Method::GET)
            .uri("/bucket/object.txt")
            .header("x-request-id", "req-123")
            .body(Full::from(Bytes::new()))
            .expect("request");
        request
            .extensions_mut()
            .insert(RemoteAddr("127.0.0.1:9000".parse().expect("socket addr")));

        let response = service.call(request).await.expect("response");
        assert_eq!(response.status(), StatusCode::OK);

        let output = String::from_utf8(captured.lock().expect("captured logs").clone()).expect("utf8 logs");
        assert_eq!(output.matches("HTTP request completed").count(), 1, "{output}");
        assert!(output.contains("event"), "{output}");
        assert!(output.contains("http_request_completed"), "{output}");
        assert!(output.contains("component"), "{output}");
        assert!(output.contains("server"), "{output}");
        assert!(output.contains("subsystem"), "{output}");
        assert!(output.contains("http"), "{output}");
        assert!(output.contains("request_id"), "{output}");
        assert!(output.contains("req-123"), "{output}");
        assert!(output.contains("peer_addr"), "{output}");
        assert!(output.contains("127.0.0.1:9000"), "{output}");
        assert!(output.contains("method"), "{output}");
        assert!(output.contains("GET"), "{output}");
        assert!(output.contains("uri"), "{output}");
        assert!(output.contains("/bucket/object.txt"), "{output}");
        assert!(output.contains("status_code"), "{output}");
        assert!(output.contains("200"), "{output}");
        assert!(output.contains("result"), "{output}");
        assert!(output.contains("success"), "{output}");
        assert!(output.contains("duration_ms"), "{output}");
    }

    #[tokio::test]
    async fn request_logging_layer_uses_request_context_trace_fields() {
        let writer = SharedWriter::default();
        let captured = writer.buffer.clone();
        let subscriber = Registry::default().with(
            tracing_subscriber::fmt::layer()
                .without_time()
                .with_target(false)
                .with_level(false)
                .with_ansi(false)
                .with_writer(writer),
        );

        let _guard = tracing::subscriber::set_default(subscriber);

        let mut service = RequestLoggingLayer.layer(StatusService::new(StatusCode::INTERNAL_SERVER_ERROR));

        let mut request = Request::builder()
            .method(Method::GET)
            .uri("/bucket/object.txt")
            .body(())
            .expect("request");
        request.extensions_mut().insert(RequestContext {
            request_id: "req-ctx".to_string(),
            x_amz_request_id: "amz-ctx".to_string(),
            trace_id: Some("trace-ctx".to_string()),
            span_id: Some("span-ctx".to_string()),
            start_time: Instant::now(),
        });
        request
            .extensions_mut()
            .insert(RemoteAddr("127.0.0.1:9000".parse().expect("socket addr")));

        let response = service.call(request).await.expect("response");
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);

        let output = String::from_utf8(captured.lock().expect("captured logs").clone()).expect("utf8 logs");
        assert!(output.contains("http_request_completed"), "{output}");
        assert!(output.contains("req-ctx"), "{output}");
        assert!(output.contains("trace-ctx"), "{output}");
        assert!(output.contains("span-ctx"), "{output}");
        assert!(output.contains("500"), "{output}");
        assert!(output.contains("server_error"), "{output}");
    }

    #[test]
    fn rewrite_double_slash_root_only_matches_exact_double_slash() {
        // Exactly `//` is rewritten to `/` (query preserved).
        assert_eq!(
            rewrite_double_slash_root(&Uri::from_static("//")).map(|u| u.to_string()),
            Some("/".to_string())
        );
        assert_eq!(
            rewrite_double_slash_root(&Uri::from_static("//?x-id=ListBuckets")).map(|u| u.to_string()),
            Some("/?x-id=ListBuckets".to_string())
        );

        // Everything else is left untouched.
        assert_eq!(rewrite_double_slash_root(&Uri::from_static("/")), None);
        assert_eq!(rewrite_double_slash_root(&Uri::from_static("//bucket")), None);
        assert_eq!(rewrite_double_slash_root(&Uri::from_static("/bucket")), None);
        assert_eq!(rewrite_double_slash_root(&Uri::from_static("/bucket//key")), None);
        assert_eq!(rewrite_double_slash_root(&Uri::from_static("///")), None);
    }

    /// Inner service that records the request-target path it received and
    /// returns an empty response, so the layer's URI rewrite can be observed.
    #[derive(Clone, Default)]
    struct UriCaptureService {
        path: Arc<Mutex<Option<String>>>,
    }

    impl<B> Service<Request<B>> for UriCaptureService {
        type Response = Response<Full<Bytes>>;
        type Error = Infallible;
        type Future = Ready<Result<Self::Response, Self::Error>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, req: Request<B>) -> Self::Future {
            *self.path.lock().expect("path lock") = Some(req.uri().path().to_string());
            ready(Ok(Response::new(Full::from(Bytes::new()))))
        }
    }

    #[tokio::test]
    async fn double_slash_compat_layer_rewrites_get_double_slash() {
        let inner = UriCaptureService::default();
        let seen = inner.path.clone();
        let mut service = DoubleSlashListBucketsCompatLayer.layer(inner);

        let observe = |seen: &Arc<Mutex<Option<String>>>| seen.lock().expect("path lock").clone();

        // `GET //` becomes `GET /`.
        let req = Request::builder()
            .method(Method::GET)
            .uri("//")
            .body(Full::<Bytes>::from(Bytes::new()))
            .expect("request");
        service.call(req).await.expect("response");
        assert_eq!(observe(&seen).as_deref(), Some("/"));

        // Non-GET `//` is not rewritten.
        let req = Request::builder()
            .method(Method::PUT)
            .uri("//")
            .body(Full::<Bytes>::from(Bytes::new()))
            .expect("request");
        service.call(req).await.expect("response");
        assert_eq!(observe(&seen).as_deref(), Some("//"));

        // `GET //bucket` is not rewritten.
        let req = Request::builder()
            .method(Method::GET)
            .uri("//bucket")
            .body(Full::<Bytes>::from(Bytes::new()))
            .expect("request");
        service.call(req).await.expect("response");
        assert_eq!(observe(&seen).as_deref(), Some("//bucket"));
    }
}
