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

//! Canonical request context carried through the entire request lifecycle.
//!
//! # Architecture
//!
//! ```text
//! External S3 HTTP ingress
//!   → generates a server-owned request ID without mutating signed headers
//!   → ExternalRequestContextLayer creates RequestContext
//!     → stores in request.extensions()
//! Auth (FS::check)
//!   → copies RequestContext into ReqInfo.request_context
//! Storage (FS methods)
//!   → reads ReqInfo for bucket/object/version
//!   → reads RequestContext for request_id/trace_id/span_id
//! Timeout Wrapper
//!   → receives canonical request_id from caller
//!   → passes to deadlock_detector.register_request()
//! OperationHelper
//!   → reads RequestContext.request_id for audit log
//!   → spawn_background_with_context() for audit/notify
//! tokio::spawn (request-internal)
//!   → spawn_traced() = tokio::spawn + .instrument(Span::current())
//! ```
//!
//! # Frozen Rules (T00 Guardrails)
//!
//! ## request-id contract
//! - External S3 response headers: `x-request-id` and `x-amz-request-id`
//! - Non-S3 response header: propagated `x-request-id`
//! - Compatibility wire header: `x-amz-request-id`
//! - Canonical internal field: `RequestContext.request_id`
//! - Client-provided request ID headers are never canonical on external S3 requests
//! - Internal modules MUST NOT generate a second request id under the field name `request_id`
//!   except for orphan/non-ingress fallback paths where no canonical request-id exists.
//! - Internal identifiers for sub-operations should use `operation_id` or `subtask_id`
//!
//! ## tokio::spawn usage
//! - **Request-internal tasks** (cache invalidation, metrics, read/write subtasks):
//!   Use `spawn_traced()` which wraps `tokio::spawn` with `.instrument(Span::current())`
//! - **Post-request side effects** (audit flush, notify, replication enqueue):
//!   Use `spawn_background_with_context()` which creates a correlated child span
//!   with explicit `request_id`
//! - **Infrastructure tasks** (server loop, TLS reload, deadlock detection):
//!   Plain `tokio::spawn` is acceptable; these are not request-scoped
//! - NEVER use bare `tokio::spawn` in request-handling code paths

use http::HeaderMap;
use metrics::counter;
use opentelemetry::global;
use opentelemetry::trace::TraceContextExt;
use rustfs_utils::http::headers::{AMZ_REQUEST_ID, REQUEST_ID_HEADER};
use std::time::Instant;
use tracing::Span;
use tracing_opentelemetry::OpenTelemetrySpanExt;

/// Canonical request context carried through the entire request lifecycle.
///
/// Created exactly once at HTTP ingress. Cloned by value; never mutated after creation.
#[derive(Clone, Debug)]
pub struct RequestContext {
    /// Canonical request ID: server-owned for external S3 requests and
    /// propagated for non-S3 and trusted internal requests.
    pub request_id: String,
    /// Compatibility-only alias that preserves an incoming
    /// `x-amz-request-id`, or mirrors [`Self::request_id`] when absent.
    ///
    /// Internal correlation must use [`Self::request_id`]. This field remains
    /// for compatibility with existing in-crate consumers.
    pub x_amz_request_id: String,
    /// OpenTelemetry trace ID (if present from upstream propagation).
    pub trace_id: Option<String>,
    /// OpenTelemetry span ID (if present from upstream propagation).
    pub span_id: Option<String>,
    /// Request ingress timestamp.
    pub start_time: Instant,
}

impl RequestContext {
    /// Create a context for a trusted internal request that may propagate its
    /// canonical ID through headers.
    pub(crate) fn from_headers(headers: &HeaderMap) -> Self {
        Self::new(
            extract_request_id_from_headers(headers),
            headers,
            extract_trace_context_ids_from_headers(headers),
        )
    }

    /// Create a context for a non-S3 request while mirroring the propagated
    /// canonical request ID into the compatibility alias.
    pub(crate) fn from_propagated_headers(headers: &HeaderMap) -> Self {
        let request_id = extract_request_id_from_headers(headers);
        let (trace_id, span_id) = extract_trace_context_ids_from_headers(headers)
            .map(|(trace_id, span_id)| (Some(trace_id), Some(span_id)))
            .unwrap_or((None, None));
        Self {
            x_amz_request_id: request_id.clone(),
            request_id,
            trace_id,
            span_id,
            start_time: Instant::now(),
        }
    }

    /// Create an external request context with a server-owned ID while keeping
    /// client headers unchanged for signature verification.
    pub(crate) fn from_external_headers(headers: &HeaderMap) -> Self {
        Self::new(uuid::Uuid::new_v4().to_string(), headers, extract_trace_context_ids_from_headers(headers))
    }

    fn new(request_id: String, headers: &HeaderMap, trace_context: Option<(String, String)>) -> Self {
        let x_amz_request_id = headers
            .get(AMZ_REQUEST_ID)
            .and_then(|value| value.to_str().ok())
            .map(String::from)
            .unwrap_or_else(|| request_id.clone());
        let (trace_id, span_id) = trace_context
            .map(|(trace_id, span_id)| (Some(trace_id), Some(span_id)))
            .unwrap_or((None, None));

        Self {
            request_id,
            x_amz_request_id,
            trace_id,
            span_id,
            start_time: Instant::now(),
        }
    }

    /// Create a fallback `RequestContext` for paths that bypass HTTP ingress.
    /// Generates a canonical internal `request_id` in `trace-{trace_id}` or `req-{uuid}` format.
    pub fn fallback() -> Self {
        let trace_ctx = current_trace_context_ids();
        let id = build_fallback_request_id(trace_ctx.as_ref());
        counter!("rustfs_log_chain_fallback_request_id_total", "source" => "request_context_fallback").increment(1);
        Self {
            request_id: id.clone(),
            x_amz_request_id: id,
            trace_id: trace_ctx.as_ref().map(|(trace_id, _)| trace_id.clone()),
            span_id: trace_ctx.as_ref().map(|(_, span_id)| span_id.clone()),
            start_time: Instant::now(),
        }
    }

    /// Return the elapsed request lifetime in whole milliseconds.
    pub fn duration_ms(&self) -> u64 {
        self.start_time.elapsed().as_millis().try_into().unwrap_or(u64::MAX)
    }
}

fn current_trace_context_ids() -> Option<(String, String)> {
    let current_context = Span::current().context();
    let current_span = current_context.span();
    let span_context = current_span.span_context();
    if !span_context.is_valid() {
        return None;
    }

    Some((span_context.trace_id().to_string(), span_context.span_id().to_string()))
}

struct HeaderMapExtractor<'a>(&'a HeaderMap);

impl opentelemetry::propagation::Extractor for HeaderMapExtractor<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).and_then(|v| v.to_str().ok())
    }

    fn keys(&self) -> Vec<&str> {
        self.0.keys().map(|k| k.as_str()).collect()
    }
}

fn build_fallback_request_id(trace_ctx: Option<&(String, String)>) -> String {
    trace_ctx
        .map(|(trace_id, _)| format!("trace-{trace_id}"))
        .unwrap_or_else(|| format!("req-{}", &uuid::Uuid::new_v4().to_string()[..8]))
}

fn generate_fallback_request_id() -> String {
    let trace_ctx = current_trace_context_ids();
    build_fallback_request_id(trace_ctx.as_ref())
}

/// Extract remote trace/span IDs from HTTP headers using the configured
/// OpenTelemetry text map propagator (for example W3C `traceparent`).
pub fn extract_trace_context_ids_from_headers(headers: &HeaderMap) -> Option<(String, String)> {
    let parent_context = global::get_text_map_propagator(|propagator| propagator.extract(&HeaderMapExtractor(headers)));
    let span_ref = parent_context.span();
    let span_context = span_ref.span_context();
    if !span_context.is_valid() {
        return None;
    }

    Some((span_context.trace_id().to_string(), span_context.span_id().to_string()))
}

/// Extract the canonical internal `request_id` from HTTP request headers.
///
/// Priority:
/// 1. `x-request-id` (primary, set by `SetRequestIdLayer`)
/// 2. `x-amz-request-id` (fallback, from S3 client forwarding)
/// 3. generated fallback id (`trace-{trace_id}` or `req-{uuid}`)
pub fn extract_request_id_from_headers(headers: &HeaderMap) -> String {
    let request_id = headers
        .get(REQUEST_ID_HEADER)
        .and_then(|v| v.to_str().ok())
        .filter(|value| !value.trim().is_empty())
        .map(String::from)
        .or_else(|| {
            headers
                .get(AMZ_REQUEST_ID)
                .and_then(|v| v.to_str().ok())
                .filter(|value| !value.trim().is_empty())
                .map(String::from)
        });

    if request_id.is_none() {
        let source = if headers.contains_key(REQUEST_ID_HEADER) || headers.contains_key(AMZ_REQUEST_ID) {
            "headers_empty_or_invalid"
        } else {
            "headers_missing"
        };
        counter!("rustfs_log_chain_fallback_request_id_total", "source" => source).increment(1);
    }

    request_id.unwrap_or_else(generate_fallback_request_id)
}

/// Spawn a request-internal task that inherits the current tracing span.
///
/// Use this for tasks that are part of the request processing pipeline
/// (e.g., cache invalidation, metrics recording, read/write subtasks).
///
/// # Rules
/// - Do NOT use this for post-request side effects (audit, notify).
///   Use `crate::storage::helper::spawn_background_with_context` instead.
/// - Do NOT use bare `tokio::spawn` in request-handling code paths.
pub fn spawn_traced<F>(fut: F)
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    tokio::spawn(tracing::Instrument::instrument(fut, tracing::Span::current()));
}

/// Spawn a request-internal task and return its join handle to the caller.
pub fn spawn_traced_join<F>(fut: F) -> tokio::task::JoinHandle<F::Output>
where
    F: std::future::Future + Send + 'static,
    F::Output: Send + 'static,
{
    tokio::spawn(tracing::Instrument::instrument(fut, tracing::Span::current()))
}

#[cfg(test)]
#[allow(unused_imports)]
mod tests {
    use super::{RequestContext, extract_request_id_from_headers, extract_trace_context_ids_from_headers};
    use http::{HeaderMap, HeaderValue};
    use opentelemetry::global;
    use opentelemetry::trace::{SpanContext, TraceContextExt, TraceFlags, TraceId, TraceState, TracerProvider as _};
    use opentelemetry_sdk::propagation::TraceContextPropagator;
    use opentelemetry_sdk::trace::SdkTracerProvider;
    use tracing_opentelemetry::OpenTelemetrySpanExt;
    use tracing_subscriber::{Registry, layer::SubscriberExt};

    fn with_trace_parent<F>(trace_id_hex: &str, f: F)
    where
        F: FnOnce(),
    {
        let provider = SdkTracerProvider::builder().build();
        let tracer = provider.tracer("request-context-tests");
        let subscriber = Registry::default().with(tracing_opentelemetry::layer().with_tracer(tracer));

        tracing::subscriber::with_default(subscriber, || {
            let span = tracing::info_span!("request-context-test-span");

            let trace_id = TraceId::from_hex(trace_id_hex).expect("trace id should be valid hex");
            let span_id = opentelemetry::trace::SpanId::from_hex("0102030405060708").expect("span id should be valid hex");
            let parent = SpanContext::new(trace_id, span_id, TraceFlags::SAMPLED, true, TraceState::default());
            span.set_parent(opentelemetry::Context::new().with_remote_span_context(parent))
                .expect("failed to set parent context");
            let _guard = span.enter();

            f();
        });
        let _ = provider.shutdown();
    }

    #[test]
    fn test_request_context_clone_send_sync() {
        fn assert_clone_send_sync<T: Clone + Send + Sync>() {}
        assert_clone_send_sync::<RequestContext>();
    }

    #[test]
    fn test_request_context_fallback_generates_id() {
        let ctx = RequestContext::fallback();
        assert!(ctx.request_id.starts_with("req-"));
        assert_eq!(ctx.request_id, ctx.x_amz_request_id);
        assert!(ctx.trace_id.is_none());
        assert!(ctx.span_id.is_none());
    }

    #[test]
    fn test_request_context_from_headers_prioritizes_canonical_request_id() {
        let mut headers = HeaderMap::new();
        headers.insert("x-request-id", HeaderValue::from_static("canonical-request-id"));
        headers.insert("x-amz-request-id", HeaderValue::from_static("client-request-id"));

        let ctx = RequestContext::from_headers(&headers);

        assert_eq!(ctx.request_id, "canonical-request-id");
        assert_eq!(ctx.x_amz_request_id, "client-request-id");
    }

    #[test]
    fn test_request_context_from_headers_preserves_empty_amz_alias() {
        let mut headers = HeaderMap::new();
        headers.insert("x-request-id", HeaderValue::from_static("canonical-request-id"));
        headers.insert("x-amz-request-id", HeaderValue::from_static(""));

        let ctx = RequestContext::from_headers(&headers);

        assert_eq!(ctx.request_id, "canonical-request-id");
        assert_eq!(ctx.x_amz_request_id, "");
    }

    #[test]
    fn test_propagated_request_context_mirrors_canonical_id_and_preserves_trace_context() {
        global::set_text_map_propagator(TraceContextPropagator::new());
        let mut headers = HeaderMap::new();
        headers.insert("x-request-id", HeaderValue::from_static("canonical-request-id"));
        headers.insert("x-amz-request-id", HeaderValue::from_static("untrusted-amz-request-id"));
        headers.insert(
            "traceparent",
            HeaderValue::from_static("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"),
        );

        let ctx = RequestContext::from_propagated_headers(&headers);

        assert_eq!(ctx.request_id, "canonical-request-id");
        assert_eq!(ctx.x_amz_request_id, "canonical-request-id");
        assert_eq!(ctx.trace_id.as_deref(), Some("4bf92f3577b34da6a3ce929d0e0e4736"));
        assert_eq!(ctx.span_id.as_deref(), Some("00f067aa0ba902b7"));
    }

    #[test]
    fn test_external_request_context_owns_id_and_preserves_trace_context() {
        global::set_text_map_propagator(TraceContextPropagator::new());
        let mut headers = HeaderMap::new();
        headers.insert("x-request-id", HeaderValue::from_static("client-request-id"));
        headers.insert("x-amz-request-id", HeaderValue::from_static("client-amz-request-id"));
        headers.insert(
            "traceparent",
            HeaderValue::from_static("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"),
        );

        let ctx = RequestContext::from_external_headers(&headers);

        assert_ne!(ctx.request_id, "client-request-id");
        assert!(uuid::Uuid::parse_str(&ctx.request_id).is_ok());
        assert_eq!(ctx.x_amz_request_id, "client-amz-request-id");
        assert_eq!(ctx.trace_id.as_deref(), Some("4bf92f3577b34da6a3ce929d0e0e4736"));
        assert_eq!(ctx.span_id.as_deref(), Some("00f067aa0ba902b7"));
    }

    #[test]
    fn test_request_context_fallback_uses_trace_prefix_when_span_context_valid() {
        let trace_id = "70f5f77e2f0a4f24be343b59f8b66f8f";
        with_trace_parent(trace_id, || {
            let ctx = RequestContext::fallback();
            assert_eq!(ctx.request_id, format!("trace-{trace_id}"));
            assert_eq!(ctx.trace_id.as_deref(), Some(trace_id));
            assert!(ctx.span_id.is_some());
        });
    }

    #[test]
    fn test_request_context_duration_ms_is_non_negative() {
        let ctx = RequestContext::fallback();
        assert!(ctx.duration_ms() <= 10);
    }

    #[test]
    fn test_extract_request_id_from_x_request_id() {
        let mut headers = HeaderMap::new();
        headers.insert("x-request-id", "test-uuid-123".parse().unwrap());
        let id = extract_request_id_from_headers(&headers);
        assert_eq!(id, "test-uuid-123");
    }

    #[test]
    fn test_extract_request_id_fallback_to_amz() {
        let mut headers = HeaderMap::new();
        headers.insert("x-amz-request-id", "amz-uuid-456".parse().unwrap());
        let id = extract_request_id_from_headers(&headers);
        assert_eq!(id, "amz-uuid-456");
    }

    #[test]
    fn test_extract_request_id_priority() {
        let mut headers = HeaderMap::new();
        headers.insert("x-request-id", "x-req-789".parse().unwrap());
        headers.insert("x-amz-request-id", "amz-req-000".parse().unwrap());
        let id = extract_request_id_from_headers(&headers);
        assert_eq!(id, "x-req-789");
    }

    #[test]
    fn test_extract_request_id_ignores_empty_header_values() {
        let mut headers = HeaderMap::new();
        headers.insert("x-request-id", http::HeaderValue::from_static(""));
        headers.insert("x-amz-request-id", http::HeaderValue::from_static("amz-req-000"));
        assert_eq!(extract_request_id_from_headers(&headers), "amz-req-000");

        headers.insert(
            "x-amz-request-id",
            http::HeaderValue::from_bytes(b" \t").expect("optional whitespace is a valid header value"),
        );
        let id = extract_request_id_from_headers(&headers);
        assert!(id.starts_with("req-"), "empty request ID headers must generate a fallback ID");
    }

    #[test]
    fn test_extract_trace_context_ids_from_traceparent_header() {
        global::set_text_map_propagator(TraceContextPropagator::new());

        let mut headers = HeaderMap::new();
        headers.insert(
            "traceparent",
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
                .parse()
                .expect("traceparent header"),
        );

        let trace_ctx = extract_trace_context_ids_from_headers(&headers).expect("trace context should be extracted");
        assert_eq!(trace_ctx.0, "4bf92f3577b34da6a3ce929d0e0e4736");
        assert_eq!(trace_ctx.1, "00f067aa0ba902b7");
    }

    #[test]
    fn test_extract_request_id_no_headers() {
        let headers = HeaderMap::new();
        let id = extract_request_id_from_headers(&headers);
        assert!(
            id.starts_with("req-") || id.starts_with("trace-"),
            "fallback request id should use req-/trace- prefix, got: {}",
            id
        );
    }

    #[test]
    fn test_extract_request_id_no_headers_uses_trace_prefix_when_span_context_valid() {
        let trace_id = "8d8b7d58055d45f793b8ca7fcb91bc17";
        with_trace_parent(trace_id, || {
            let headers = HeaderMap::new();
            let id = extract_request_id_from_headers(&headers);
            assert_eq!(id, format!("trace-{trace_id}"));
        });
    }
}
