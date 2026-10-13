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
use crate::server::RemoteAddr;
use crate::server::cors;
use crate::server::hybrid::{HybridBody, is_grpc_request};
use crate::server::{
    ADMIN_PREFIX, HEALTH_COMPAT_LIVE_PATH, HEALTH_PREFIX, HEALTH_READY_PATH, HealthProbe, MINIO_ADMIN_PREFIX,
    MINIO_HEALTH_CLUSTER_PATH, MINIO_HEALTH_CLUSTER_READ_PATH, MINIO_HEALTH_LIVE_PATH, MINIO_HEALTH_READY_PATH, PROFILE_CPU_PATH,
    PROFILE_MEMORY_PATH, RPC_PREFIX, active_http_requests, build_health_response_parts, collect_probe_readiness, console_prefix,
    has_path_prefix, is_admin_path, is_table_catalog_path, kms_probe_staleness_limit, kms_ready_from_probe,
};
use crate::shared_types::ReadinessDegradedReason;
use crate::storage_api::server::layer::apply_cors_headers;
use crate::storage_api::server::layer::request_context::{RequestContext, extract_request_id_from_headers, spawn_traced};
use bytes::Bytes;
use http::{HeaderMap, HeaderValue, Method, Request as HttpRequest, Response, StatusCode, Uri};
#[cfg(test)]
use http_body_util::{BodyExt, Full};
#[cfg(test)]
use hyper::body::Incoming;
use pin_project_lite::pin_project;
use rustfs_common::GlobalReadiness;
use rustfs_common::trace_bus::{
    TelemetryTraceEvent, TelemetryTraceOperation, TelemetryTraceStatus, telemetry_trace_emit, telemetry_trace_subscriber_count,
};
use rustfs_io_metrics::s3_http_metrics::S3HttpRequestGuard;
use rustfs_obs::HTTP_SERVER_LOG_TARGET;
#[cfg(feature = "swift")]
use rustfs_protocols::swift::SwiftRouter;
use rustfs_trusted_proxies::ClientInfo;
use rustfs_utils::get_env_opt_str;
use rustfs_utils::http::headers::{AMZ_REQUEST_ID, REQUEST_ID_HEADER};
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

pub(super) const LOG_COMPONENT_SERVER: &str = "server";

pub(super) const LOG_SUBSYSTEM_HTTP: &str = "http";

const REDACTED_QUERY_VALUE: &str = "redacted";

const OBJECT_ZIP_DOWNLOADS_PATH: &str = "/v3/object-zip-downloads/";

const HTTP_REQUEST_INFLIGHT_WARN_THRESHOLD: Duration = Duration::from_secs(5);

static HTTP_SERVER_ERROR_LOGS: [rustfs_utils::LogThrottle; 100] = [const { rustfs_utils::LogThrottle::new(5_000) }; 100];

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

        // This outer boundary includes readiness, rate-limit and auth
        // rejections. Metric attribution never depends on an enabled span.
        let mut metrics = is_s3.then(|| s3_http_request_guard(req.method().as_str()));
        let inner = match metrics.as_mut() {
            Some(metrics) => metrics.in_scope(|| self.inner.call(req)),
            None => self.inner.call(req),
        };
        ExternalRequestContextFuture {
            inner,
            request_id,
            is_s3,
            metrics,
        }
    }
}

/// Start accounting for an external S3 request. While a typed telemetry trace
/// is being recorded, the finished request is also published to the trace bus
/// as a pre-classified event; otherwise no clock is read.
pub fn s3_http_request_guard(method: &str) -> S3HttpRequestGuard {
    let guard = S3HttpRequestGuard::new(method);
    if telemetry_trace_subscriber_count() == 0 {
        return guard;
    }
    guard.with_completion_observer(emit_s3_request_telemetry)
}

fn emit_s3_request_telemetry(operation: rustfs_s3_ops::S3Operation, duration: Duration, succeeded: bool) {
    let Some(operation) = telemetry_operation(operation) else {
        return;
    };
    let status = if succeeded {
        TelemetryTraceStatus::Ok
    } else {
        TelemetryTraceStatus::Error
    };
    telemetry_trace_emit(|| TelemetryTraceEvent::new(operation, duration, status));
}

fn telemetry_operation(operation: rustfs_s3_ops::S3Operation) -> Option<TelemetryTraceOperation> {
    use rustfs_s3_ops::S3Operation;
    match operation {
        S3Operation::GetObject => Some(TelemetryTraceOperation::GetObject),
        S3Operation::PutObject => Some(TelemetryTraceOperation::PutObject),
        S3Operation::HeadObject => Some(TelemetryTraceOperation::HeadObject),
        S3Operation::ListObjects | S3Operation::ListObjectsV2 => Some(TelemetryTraceOperation::ListObjects),
        _ => None,
    }
}

pin_project! {
    pub struct ExternalRequestContextFuture<F> {
        #[pin]
        inner: F,
        request_id: Option<HeaderValue>,
        is_s3: bool,
        metrics: Option<S3HttpRequestGuard>,
    }
}

impl<F, ResBody, E> Future for ExternalRequestContextFuture<F>
where
    F: Future<Output = Result<Response<ResBody>, E>>,
{
    type Output = Result<Response<ResBody>, E>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let result = match this.metrics.as_mut() {
            Some(metrics) => metrics.in_scope(|| this.inner.poll(cx)),
            None => this.inner.poll(cx),
        };
        let mut response = match result {
            Poll::Ready(Ok(response)) => response,
            Poll::Ready(Err(error)) => {
                if let Some(metrics) = this.metrics.as_mut() {
                    metrics.service_error();
                }
                return Poll::Ready(Err(error));
            }
            Poll::Pending => return Poll::Pending,
        };

        if let Some(metrics) = this.metrics.as_mut() {
            metrics.response(response.status().as_u16());
        }
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
    has_s3_accounting: bool,
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
            has_s3_accounting: S3HttpRequestGuard::is_active(),
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
            let suppressed_errors = if self.has_s3_accounting {
                let Some(suppressed) = HTTP_SERVER_ERROR_LOGS[usize::from(status_code - 500)].claim() else {
                    return;
                };
                suppressed
            } else {
                0
            };
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
                uri = self.uri.path(),
                status_code,
                suppressed_errors,
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

impl<S, ReqBody, RestBody, GrpcBody> Service<HttpRequest<ReqBody>> for RedirectService<S>
where
    S: Service<HttpRequest<ReqBody>, Response = Response<HybridBody<RestBody, GrpcBody>>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Into<Box<dyn std::error::Error + Send + Sync>> + Send + 'static,
    ReqBody: Send + 'static,
    RestBody: Default + Send + 'static,
    GrpcBody: Send + 'static,
{
    type Response = Response<HybridBody<RestBody, GrpcBody>>;
    type Error = Box<dyn std::error::Error + Send + Sync>;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx).map_err(Into::into)
    }

    fn call(&mut self, req: HttpRequest<ReqBody>) -> Self::Future {
        let path = req.uri().path().trim_end_matches('/');
        if is_console_redirect_request(&req) {
            debug!("Redirecting browser request from {} to console", path);

            // Create redirect response
            let redirect_response = Response::builder()
                .status(StatusCode::FOUND)
                .header(http::header::LOCATION, format!("{}/", console_prefix()))
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

pub(crate) fn is_sts_query_request(method: &Method, uri: &Uri, headers: &HeaderMap) -> bool {
    method == Method::POST
        && uri.path() == "/"
        && headers
            .get(http::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.split(';').next())
            .is_some_and(|value| value.trim().eq_ignore_ascii_case("application/x-www-form-urlencoded"))
}

#[derive(Clone)]
pub struct PublicHealthEndpointLayer {
    server_ctx: Arc<crate::runtime_sources::ServerContextSlot>,
    readiness: Arc<GlobalReadiness>,
}

impl PublicHealthEndpointLayer {
    pub fn new(server_ctx: Arc<crate::runtime_sources::ServerContextSlot>, readiness: Arc<GlobalReadiness>) -> Self {
        Self { server_ctx, readiness }
    }
}

impl<S> Layer<S> for PublicHealthEndpointLayer {
    type Service = PublicHealthEndpointService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        PublicHealthEndpointService {
            inner,
            server_ctx: Arc::clone(&self.server_ctx),
            readiness: Arc::clone(&self.readiness),
        }
    }
}

#[derive(Clone)]
pub struct PublicHealthEndpointService<S> {
    inner: S,
    server_ctx: Arc<crate::runtime_sources::ServerContextSlot>,
    readiness: Arc<GlobalReadiness>,
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
    readiness: &GlobalReadiness,
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

    let mut readiness_report = collect_probe_readiness(probe, object_traffic_health.as_deref()).await;
    if probe == HealthProbe::Readiness
        && !readiness.is_ready()
        && let Some(report) = readiness_report.as_mut()
    {
        report
            .degraded_reasons
            .push(ReadinessDegradedReason::StartupFinalizationPending);
    }
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
            let readiness = Arc::clone(&self.readiness);
            return Box::pin(async move {
                Ok(build_public_health_http_response(method, path, object_traffic_health, readiness.as_ref()).await)
            });
        }

        let mut inner = self.inner.clone();
        Box::pin(async move { inner.call(req).await })
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

    pub(super) fn is_s3_path(path: &str) -> bool {
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

#[cfg(test)]
pub(in crate::server) mod tests {
    #[tokio::test]
    async fn console_prefix_process_case_browser_redirect() {
        if std::env::var_os("RUSTFS_TEST_CONSOLE_PREFIX_PROCESS").is_none() {
            return;
        }
        crate::server::init_console_prefix().expect("initialize console prefix");
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("redirect listener");
        let addr = listener.local_addr().expect("redirect listener address");
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("redirect client");
            let inner = tower::service_fn(|_request: Request<Incoming>| async {
                Ok::<_, Infallible>(Response::new(HybridBody::<Empty<Bytes>, Empty<Bytes>>::Rest { rest_body: Empty::new() }))
            });
            let service = RedirectLayer.layer(inner);
            hyper::server::conn::http1::Builder::new()
                .serve_connection(
                    hyper_util::rt::TokioIo::new(stream),
                    hyper_util::service::TowerToHyperService::new(service),
                )
                .await
                .expect("redirect connection");
        });
        let client = reqwest::Client::builder()
            .no_proxy()
            .http1_only()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(Duration::from_secs(5))
            .build()
            .expect("redirect client");
        let response = client
            .get(format!("http://{addr}/"))
            .header(http::header::USER_AGENT, "Mozilla/5.0")
            .header(http::header::CONNECTION, "close")
            .send()
            .await
            .expect("browser response");
        assert_eq!(response.status(), StatusCode::FOUND);
        assert_eq!(response.headers()[http::header::LOCATION], format!("{}/", console_prefix()));
        response.bytes().await.expect("redirect body");
        tokio::time::timeout(Duration::from_secs(5), server)
            .await
            .expect("bounded redirect server shutdown")
            .expect("redirect task");
    }

    #[test]
    fn console_prefix_process_case_classification() {
        if std::env::var_os("RUSTFS_TEST_CONSOLE_PREFIX_PROCESS").is_none() {
            return;
        }
        crate::server::init_console_prefix().expect("initialize console prefix");
        let prefix = crate::server::console_prefix();
        let console_uri = format!("{prefix}/index.html").parse().expect("console URI");
        assert!(is_empty_body_console_path(&Method::GET, &console_uri));
        let request = HttpRequest::builder()
            .uri(format!("{prefix}/index.html?attributes"))
            .body(())
            .expect("console attributes request");
        assert!(!is_object_attributes_request(&request));
        let s3_request = HttpRequest::builder()
            .uri("/bucket/object?attributes")
            .body(())
            .expect("S3 attributes request");
        assert!(is_object_attributes_request(&s3_request));
    }

    use super::*;
    use crate::server::CONSOLE_PREFIX;
    use crate::server::legacy_compat::{is_empty_body_console_path, is_object_attributes_request};
    use futures::future::{Ready, ready};
    use http::Request;
    use http_body_util::Empty;
    use opentelemetry::global;
    use opentelemetry_sdk::propagation::TraceContextPropagator;
    use serial_test::serial;
    use std::convert::Infallible;
    use std::io::{self, Write};
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use temp_env::{async_with_vars, with_var};
    use tracing_subscriber::{Registry, fmt::MakeWriter, layer::SubscriberExt};

    #[test]
    fn telemetry_adapter_accepts_only_the_frozen_s3_operations() {
        use rustfs_s3_ops::S3Operation;
        assert_eq!(telemetry_operation(S3Operation::GetObject), Some(TelemetryTraceOperation::GetObject));
        assert_eq!(telemetry_operation(S3Operation::PutObject), Some(TelemetryTraceOperation::PutObject));
        assert_eq!(telemetry_operation(S3Operation::HeadObject), Some(TelemetryTraceOperation::HeadObject));
        assert_eq!(telemetry_operation(S3Operation::ListObjects), Some(TelemetryTraceOperation::ListObjects));
        assert_eq!(
            telemetry_operation(S3Operation::ListObjectsV2),
            Some(TelemetryTraceOperation::ListObjects)
        );
        assert_eq!(telemetry_operation(S3Operation::DeleteObject), None);
    }

    fn public_health_layer() -> PublicHealthEndpointLayer {
        let readiness = Arc::new(GlobalReadiness::new());
        readiness.mark_stage(rustfs_common::SystemStage::FullReady);
        PublicHealthEndpointLayer::new(crate::runtime_sources::ServerContextSlot::new(), readiness)
    }

    #[tokio::test]
    async fn external_s3_http_outcomes_cover_response_error_cancel_and_exclusions_at_warn() {
        use rustfs_io_metrics::{record_s3_op, s3_http_metrics::s3_http_metrics_snapshot};
        use rustfs_s3_ops::S3Operation;
        let _logs = tracing::subscriber::set_default(
            tracing_subscriber::fmt()
                .with_max_level(tracing::Level::WARN)
                .with_writer(std::io::sink)
                .finish(),
        );
        let totals = || {
            s3_http_metrics_snapshot()
                .into_iter()
                .filter(|series| series.operation == S3Operation::RestoreObject.as_str())
                .fold(std::collections::BTreeMap::<String, u64>::new(), |mut result, series| {
                    *result.entry(series.outcome.to_string()).or_default() += series.total;
                    result
                })
        };
        let before = totals();
        let inner = tower::service_fn(|req: Request<()>| async move {
            record_s3_op(S3Operation::RestoreObject);
            match req.uri().path() {
                "/bucket/cancel" => std::future::pending::<Result<Response<()>, io::Error>>().await,
                "/bucket/service-error" => Err(io::Error::other("test service failure")),
                path => Ok(Response::builder()
                    .status(match path {
                        "/bucket/denied" => StatusCode::FORBIDDEN,
                        "/bucket/unavailable" => StatusCode::SERVICE_UNAVAILABLE,
                        _ => StatusCode::OK,
                    })
                    .body(())
                    .expect("response")),
            }
        });
        let mut service = ExternalRequestContextLayer::default().layer(inner);
        for path in ["/bucket/ok", "/bucket/denied", "/bucket/unavailable"] {
            let response = service
                .call(Request::builder().method(Method::PATCH).uri(path).body(()).expect("request"))
                .await
                .expect("response");
            assert!(response.headers().contains_key(AMZ_REQUEST_ID));
        }
        let error = service
            .call(
                Request::builder()
                    .method(Method::PATCH)
                    .uri("/bucket/service-error")
                    .body(())
                    .expect("request"),
            )
            .await;
        assert!(error.is_err());
        let mut cancelled = Box::pin(
            service.call(
                Request::builder()
                    .method(Method::PATCH)
                    .uri("/bucket/cancel")
                    .body(())
                    .expect("request"),
            ),
        );
        assert!(futures::poll!(cancelled.as_mut()).is_pending());
        drop(cancelled);
        for path in [
            "/rustfs/admin/v3/realtime",
            "/minio/admin/v3/storageinfo",
            CONSOLE_PREFIX,
            "/rustfs/rpc/test",
            "/health/ready",
            "/_iceberg/v1/config",
        ] {
            service
                .call(Request::builder().uri(path).body(()).expect("excluded request"))
                .await
                .expect("excluded response");
        }
        let after = totals();
        for outcome in ["2xx", "4xx", "5xx", "service_error", "cancelled"] {
            assert_eq!(
                after.get(outcome).copied().unwrap_or_default() - before.get(outcome).copied().unwrap_or_default(),
                1,
                "{outcome}"
            );
        }
    }

    #[tokio::test]
    async fn external_s3_http_outcomes_include_real_readiness_rejections() {
        use rustfs_io_metrics::s3_http_metrics::s3_http_metrics_snapshot;
        let rejected = || {
            s3_http_metrics_snapshot()
                .into_iter()
                .filter(|series| series.method == "TRACE" && series.operation == "unknown" && series.outcome == "5xx")
                .map(|series| series.total)
                .sum::<u64>()
        };
        let before = rejected();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("isolated HTTP listener");
        let addr = listener.local_addr().expect("listener address");
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("HTTP client");
            let service = tower::ServiceBuilder::new()
                .layer(ExternalRequestContextLayer::default())
                .layer(crate::server::ReadinessGateLayer::new(Arc::new(GlobalReadiness::new())))
                .service(StatusService::new(StatusCode::OK));
            hyper::server::conn::http1::Builder::new()
                .serve_connection(
                    hyper_util::rt::TokioIo::new(stream),
                    hyper_util::service::TowerToHyperService::new(service),
                )
                .await
                .expect("HTTP connection");
        });
        let client = reqwest::Client::builder()
            .no_proxy()
            .http1_only()
            .timeout(Duration::from_secs(5))
            .build()
            .expect("local HTTP client");
        let response = client
            .request(Method::TRACE, format!("http://{addr}/bucket/object"))
            .header(http::header::CONNECTION, "close")
            .send()
            .await
            .expect("readiness response");
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert!(response.headers().contains_key(AMZ_REQUEST_ID));
        let _body = response.bytes().await.expect("readiness body");
        tokio::time::timeout(Duration::from_secs(5), server)
            .await
            .expect("bounded HTTP server shutdown")
            .expect("server task");
        assert_eq!(rejected() - before, 1, "rejection is counted before the inner trace layer");
    }

    async fn public_health_layer_with_tracker(object_traffic_health: Arc<ObjectTrafficHealth>) -> PublicHealthEndpointLayer {
        let readiness = Arc::new(GlobalReadiness::new());
        readiness.mark_stage(rustfs_common::SystemStage::FullReady);
        public_health_layer_with_tracker_and_readiness(object_traffic_health, readiness).await
    }

    async fn public_health_layer_with_tracker_and_readiness(
        object_traffic_health: Arc<ObjectTrafficHealth>,
        readiness: Arc<GlobalReadiness>,
    ) -> PublicHealthEndpointLayer {
        let app_context = crate::app::gating_test_env::app_context_with_object_traffic_health(object_traffic_health).await;
        let server_ctx = crate::runtime_sources::ServerContextSlot::new();
        assert!(server_ctx.install(app_context));
        PublicHealthEndpointLayer::new(server_ctx, readiness)
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
    pub(in crate::server) struct HeaderCaptureService {
        headers: Arc<Mutex<Option<HeaderMap>>>,
        request_context: Arc<Mutex<Option<RequestContext>>>,
        response_request_id: Option<HeaderValue>,
    }

    impl HeaderCaptureService {
        pub(in crate::server) fn with_response_request_id(request_id: &'static str) -> Self {
            Self {
                response_request_id: Some(HeaderValue::from_static(request_id)),
                ..Self::default()
            }
        }

        pub(in crate::server) fn headers(&self) -> Arc<Mutex<Option<HeaderMap>>> {
            Arc::clone(&self.headers)
        }

        pub(in crate::server) fn request_context(&self) -> Arc<Mutex<Option<RequestContext>>> {
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
            CONSOLE_PREFIX,
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
    pub(in crate::server) struct CountingHybridService {
        calls: Arc<AtomicUsize>,
    }

    impl CountingHybridService {
        pub(in crate::server) fn calls(&self) -> Arc<AtomicUsize> {
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
        async_with_vars(
            [
                (rustfs_config::ENV_HEALTH_ENDPOINT_ENABLE, Some("true")),
                (rustfs_config::ENV_HEALTH_MINIMAL_RESPONSE_ENABLE, Some("false")),
            ],
            async {
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
                assert!(payload.get("ready").is_none());
                assert!(payload.get("details").is_none());
                assert!(payload.get("degradedReasons").is_none());
            },
        )
        .await;
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
    async fn public_readiness_waits_for_s3_admission_publication() {
        async_with_vars(
            [
                (rustfs_config::ENV_HEALTH_ENDPOINT_ENABLE, Some("true")),
                (rustfs_config::ENV_HEALTH_MINIMAL_RESPONSE_ENABLE, Some("false")),
            ],
            async {
                let object_traffic_health = Arc::new(ObjectTrafficHealth::enabled_for_test(Duration::ZERO));
                let readiness = Arc::new(GlobalReadiness::new());
                let inner = CountingHybridService::default();
                let calls = inner.calls();
                let mut service = public_health_layer_with_tracker_and_readiness(object_traffic_health, Arc::clone(&readiness))
                    .await
                    .layer(inner);

                let response = service
                    .call(
                        Request::builder()
                            .method(Method::GET)
                            .uri(HEALTH_READY_PATH)
                            .body(Full::<Bytes>::from(Bytes::new()))
                            .expect("readiness request before admission publication"),
                    )
                    .await
                    .expect("readiness response before admission publication");
                assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
                let body = BodyExt::collect(response.into_body())
                    .await
                    .expect("readiness body before admission publication")
                    .to_bytes();
                let payload: serde_json::Value = serde_json::from_slice(&body).expect("readiness JSON");
                assert_eq!(payload["ready"], false);
                assert_eq!(payload["details"]["storage"]["ready"], true);
                assert_eq!(payload["details"]["iam"]["ready"], true);
                assert_eq!(payload["details"]["lock"]["ready"], true);
                assert_eq!(payload["degradedReasons"], serde_json::json!(["startup_finalization_pending"]));

                let response = service
                    .call(
                        Request::builder()
                            .method(Method::GET)
                            .uri(HEALTH_COMPAT_LIVE_PATH)
                            .body(Full::<Bytes>::from(Bytes::new()))
                            .expect("liveness request before admission publication"),
                    )
                    .await
                    .expect("liveness response before admission publication");
                assert_eq!(response.status(), StatusCode::OK);
                let body = BodyExt::collect(response.into_body())
                    .await
                    .expect("liveness body before admission publication")
                    .to_bytes();
                let payload: serde_json::Value = serde_json::from_slice(&body).expect("liveness JSON");
                assert_eq!(payload["status"], "ok");
                assert!(payload.get("ready").is_none());

                readiness.mark_stage(rustfs_common::SystemStage::FullReady);
                let response = service
                    .call(
                        Request::builder()
                            .method(Method::HEAD)
                            .uri(MINIO_HEALTH_READY_PATH)
                            .body(Full::<Bytes>::from(Bytes::new()))
                            .expect("readiness request after admission publication"),
                    )
                    .await
                    .expect("readiness response after admission publication");
                assert_eq!(response.status(), StatusCode::OK);
                assert_eq!(calls.load(Ordering::SeqCst), 0);
            },
        )
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
                let body = BodyExt::collect(response.into_body())
                    .await
                    .expect("liveness body")
                    .to_bytes();
                let payload: serde_json::Value = serde_json::from_slice(&body).expect("liveness JSON");
                assert_eq!(payload["status"], "ok");
                assert!(payload.get("ready").is_none());
                assert!(payload.get("degradedReasons").is_none());
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
    async fn request_logging_bounds_s3_failure_bursts_without_losing_counts_or_leaking_queries() {
        use rustfs_io_metrics::s3_http_metrics::s3_http_metrics_snapshot;
        let count = || {
            s3_http_metrics_snapshot()
                .iter()
                .filter(|series| series.method == "CONNECT" && series.operation == "unknown" && series.outcome == "5xx")
                .map(|series| series.total)
                .sum::<u64>()
        };
        let before = count();
        let writer = SharedWriter::default();
        let captured = writer.buffer.clone();
        let _subscriber = tracing::subscriber::set_default(
            tracing_subscriber::fmt()
                .with_max_level(tracing::Level::WARN)
                .without_time()
                .with_ansi(false)
                .with_writer(writer)
                .finish(),
        );
        // A distinct status isolates this test's process-wide log window.
        let mut service = tower::ServiceBuilder::new()
            .layer(ExternalRequestContextLayer::default())
            .layer(RequestLoggingLayer)
            .service(StatusService::new(StatusCode::from_u16(599).expect("server error")));
        for _ in 0..10 {
            service
                .call(
                    Request::builder()
                        .method(Method::CONNECT)
                        .uri("/bucket/object?X-Amz-Signature=private-signature&X-Amz-Security-Token=private-session")
                        .body(())
                        .expect("request"),
                )
                .await
                .expect("response");
        }
        assert_eq!(count() - before, 10);
        let output = String::from_utf8(captured.lock().expect("logs").clone()).expect("UTF-8 logs");
        assert_eq!(output.matches("http_request_completed").count(), 1, "{output}");
        assert!(output.contains("/bucket/object"));
        assert!(!output.contains("private-"));
        assert!(!output.contains("X-Amz-"));
        for _ in 0..2 {
            service
                .call(
                    Request::builder()
                        .uri("/rustfs/admin/v3/info")
                        .body(())
                        .expect("admin request"),
                )
                .await
                .expect("admin response");
        }
        let output = String::from_utf8(captured.lock().expect("logs").clone()).expect("UTF-8 logs");
        assert_eq!(
            output.matches("http_request_completed").count(),
            3,
            "admin logging is not throttled: {output}"
        );
        assert_eq!(count() - before, 10);
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
}
