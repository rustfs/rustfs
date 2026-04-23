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
//! HTTP Ingress (SetRequestIdLayer)
//!   → generates x-request-id UUID
//!   → RequestContextLayer creates RequestContext
//!     → stores in request.extensions()
//!     → sets x-amz-request-id header
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
//! ## request-id
//! - Canonical source: HTTP ingress `x-request-id` header (set by `SetRequestIdLayer`)
//! - `x-amz-request_id` is an alias for S3 compatibility, always equal to `request_id`
//! - Internal modules MUST NOT generate a second request-id under the name `request_id`
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
use opentelemetry::trace::TraceContextExt;
use rustfs_utils::http::headers::AMZ_REQUEST_ID;
use std::time::Instant;
use tracing::Span;
use tracing_opentelemetry::OpenTelemetrySpanExt;

/// Canonical request context carried through the entire request lifecycle.
///
/// Created exactly once at HTTP ingress. Cloned by value; never mutated after creation.
#[derive(Clone, Debug)]
pub struct RequestContext {
    /// Canonical request ID (from `x-request-id` header, set by `SetRequestIdLayer`).
    pub request_id: String,
    /// S3-compatible request ID alias (preserves upstream `x-amz-request-id` if present,
    /// otherwise equals `request_id`).
    pub x_amz_request_id: String,
    /// OpenTelemetry trace ID (if present from upstream propagation).
    pub trace_id: Option<String>,
    /// OpenTelemetry span ID (if present from upstream propagation).
    pub span_id: Option<String>,
    /// Request ingress timestamp.
    pub start_time: Instant,
}

impl RequestContext {
    /// Create a fallback `RequestContext` for paths that bypass HTTP ingress.
    /// Generates a `trace-{trace_id}` or `req-{uuid}` format request-id.
    pub fn fallback() -> Self {
        let id = generate_fallback_request_id();
        let (trace_id, span_id) = current_trace_context_ids();
        counter!("rustfs.log.chain.fallback_request_id.total", "source" => "request_context_fallback").increment(1);
        Self {
            request_id: id.clone(),
            x_amz_request_id: id,
            trace_id,
            span_id,
            start_time: Instant::now(),
        }
    }
}

fn current_trace_context_ids() -> (Option<String>, Option<String>) {
    let current_context = Span::current().context();
    let current_span = current_context.span();
    let span_context = current_span.span_context();
    if !span_context.is_valid() {
        return (None, None);
    }

    (Some(span_context.trace_id().to_string()), Some(span_context.span_id().to_string()))
}

fn generate_fallback_request_id() -> String {
    let current_context = Span::current().context();
    let current_span = current_context.span();
    let span_context = current_span.span_context();
    if span_context.is_valid() {
        return format!("trace-{}", span_context.trace_id());
    }
    format!("req-{}", &uuid::Uuid::new_v4().to_string()[..8])
}

/// Extract the canonical request ID from HTTP headers.
///
/// Priority:
/// 1. `x-request-id` (primary, set by `SetRequestIdLayer`)
/// 2. `x-amz-request-id` (fallback, from S3 client forwarding)
/// 3. generated fallback id (`trace-{trace_id}` or `req-{uuid}`)
pub fn extract_request_id_from_headers(headers: &HeaderMap) -> String {
    let request_id = headers
        .get("x-request-id")
        .and_then(|v| v.to_str().ok())
        .map(String::from)
        .or_else(|| headers.get(AMZ_REQUEST_ID).and_then(|v| v.to_str().ok()).map(String::from))
        .unwrap_or_else(generate_fallback_request_id);

    if !headers.contains_key("x-request-id") && !headers.contains_key(AMZ_REQUEST_ID) {
        counter!("rustfs.log.chain.fallback_request_id.total", "source" => "headers_missing").increment(1);
    }

    request_id
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

#[cfg(test)]
mod tests {
    use super::*;

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
    fn test_extract_request_id_no_headers() {
        let headers = HeaderMap::new();
        let id = extract_request_id_from_headers(&headers);
        assert!(
            id.starts_with("req-") || id.starts_with("trace-"),
            "fallback request id should use req-/trace- prefix, got: {}",
            id
        );
    }
}
