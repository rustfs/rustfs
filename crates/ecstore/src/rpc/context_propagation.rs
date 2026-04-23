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

use http::{HeaderMap, HeaderValue};
use opentelemetry::{global, propagation::Injector, trace::TraceContextExt};
use tracing::Span;
use tracing_opentelemetry::OpenTelemetrySpanExt;

pub(crate) const REQUEST_ID_HEADER: &str = "x-request-id";

struct HttpHeaderInjector<'a> {
    headers: &'a mut HeaderMap,
}

impl Injector for HttpHeaderInjector<'_> {
    fn set(&mut self, key: &str, value: String) {
        let Ok(name) = http::header::HeaderName::from_bytes(key.as_bytes()) else {
            return;
        };
        let Ok(val) = HeaderValue::from_str(&value) else {
            return;
        };
        self.headers.insert(name, val);
    }
}

struct MetadataInjector<'a> {
    metadata: &'a mut tonic::metadata::MetadataMap,
}

impl Injector for MetadataInjector<'_> {
    fn set(&mut self, key: &str, value: String) {
        let Ok(meta_key) = tonic::metadata::MetadataKey::from_bytes(key.as_bytes()) else {
            return;
        };
        let Ok(meta_value) = tonic::metadata::MetadataValue::try_from(value.as_str()) else {
            return;
        };
        self.metadata.insert(meta_key, meta_value);
    }
}

fn current_trace_id() -> Option<String> {
    let current_context = Span::current().context();
    let current_span = current_context.span();
    let span_context = current_span.span_context();
    if !span_context.is_valid() {
        return None;
    }
    Some(span_context.trace_id().to_string())
}

fn fallback_request_id() -> String {
    format!("req-{}", &uuid::Uuid::new_v4().to_string()[..8])
}

fn propagated_request_id() -> String {
    current_trace_id()
        .map(|trace_id| format!("trace-{trace_id}"))
        .unwrap_or_else(fallback_request_id)
}

pub(crate) fn inject_trace_context_into_http_headers(headers: &mut HeaderMap) {
    let current_context = Span::current().context();
    global::get_text_map_propagator(|propagator| {
        let mut injector = HttpHeaderInjector { headers };
        propagator.inject_context(&current_context, &mut injector);
    });
}

pub(crate) fn inject_request_id_into_http_headers(headers: &mut HeaderMap) {
    if headers.contains_key(REQUEST_ID_HEADER) {
        return;
    }
    let request_id = propagated_request_id();
    if let Ok(value) = HeaderValue::from_str(&request_id) {
        headers.insert(REQUEST_ID_HEADER, value);
    }
}

pub(crate) fn inject_trace_context_into_metadata(metadata: &mut tonic::metadata::MetadataMap) {
    let current_context = Span::current().context();
    global::get_text_map_propagator(|propagator| {
        let mut injector = MetadataInjector { metadata };
        propagator.inject_context(&current_context, &mut injector);
    });
}

pub(crate) fn inject_request_id_into_metadata(metadata: &mut tonic::metadata::MetadataMap) {
    let request_id_key = tonic::metadata::MetadataKey::from_static(REQUEST_ID_HEADER);
    if metadata.contains_key(&request_id_key) {
        return;
    }
    let request_id = propagated_request_id();
    let Ok(value) = tonic::metadata::MetadataValue::try_from(request_id.as_str()) else {
        return;
    };
    metadata.insert(request_id_key, value);
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry::trace::{SpanContext, TraceContextExt, TraceFlags, TraceId, TraceState, TracerProvider as _};
    use opentelemetry_sdk::trace::SdkTracerProvider;
    use tracing_opentelemetry::OpenTelemetrySpanExt;
    use tracing_subscriber::{Registry, layer::SubscriberExt};

    fn with_trace_parent<F>(trace_id_hex: &str, f: F)
    where
        F: FnOnce(),
    {
        let provider = SdkTracerProvider::builder().build();
        let tracer = provider.tracer("context-propagation-tests");
        let subscriber = Registry::default().with(tracing_opentelemetry::layer().with_tracer(tracer));

        tracing::subscriber::with_default(subscriber, || {
            let span = tracing::info_span!("context-propagation-test-span");

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
    fn test_inject_request_id_into_http_headers_preserves_existing_value() {
        let mut headers = HeaderMap::new();
        headers.insert(REQUEST_ID_HEADER, HeaderValue::from_static("req-upstream-123"));

        with_trace_parent("0123456789abcdef0123456789abcdef", || {
            inject_request_id_into_http_headers(&mut headers);
        });

        assert_eq!(headers.get(REQUEST_ID_HEADER).and_then(|v| v.to_str().ok()), Some("req-upstream-123"));
    }

    #[test]
    fn test_inject_request_id_into_http_headers_uses_trace_id_when_missing() {
        let trace_id = "abcdefabcdefabcdefabcdefabcdefab";
        let mut headers = HeaderMap::new();

        with_trace_parent(trace_id, || {
            inject_request_id_into_http_headers(&mut headers);
        });

        assert_eq!(
            headers.get(REQUEST_ID_HEADER).and_then(|v| v.to_str().ok()),
            Some(format!("trace-{trace_id}").as_str())
        );
    }

    #[test]
    fn test_inject_request_id_into_metadata_preserves_existing_value() {
        let mut metadata = tonic::metadata::MetadataMap::new();
        metadata.insert(
            tonic::metadata::MetadataKey::from_static(REQUEST_ID_HEADER),
            tonic::metadata::MetadataValue::from_static("req-upstream-456"),
        );

        with_trace_parent("fedcba9876543210fedcba9876543210", || {
            inject_request_id_into_metadata(&mut metadata);
        });

        assert_eq!(metadata.get(REQUEST_ID_HEADER).and_then(|v| v.to_str().ok()), Some("req-upstream-456"));
    }

    #[test]
    fn test_inject_request_id_into_metadata_uses_trace_id_when_missing() {
        let trace_id = "1234567890abcdef1234567890abcdef";
        let mut metadata = tonic::metadata::MetadataMap::new();

        with_trace_parent(trace_id, || {
            inject_request_id_into_metadata(&mut metadata);
        });

        assert_eq!(
            metadata.get(REQUEST_ID_HEADER).and_then(|v| v.to_str().ok()),
            Some(format!("trace-{trace_id}").as_str())
        );
    }

    #[test]
    fn test_inject_request_id_into_http_headers_uses_req_fallback_when_trace_missing() {
        let mut headers = HeaderMap::new();
        inject_request_id_into_http_headers(&mut headers);

        let request_id = headers
            .get(REQUEST_ID_HEADER)
            .and_then(|v| v.to_str().ok())
            .expect("request id should be injected");
        assert!(request_id.starts_with("req-"), "expected req- fallback, got: {request_id}");
    }

    #[test]
    fn test_inject_request_id_into_metadata_uses_req_fallback_when_trace_missing() {
        let mut metadata = tonic::metadata::MetadataMap::new();
        inject_request_id_into_metadata(&mut metadata);

        let request_id = metadata
            .get(REQUEST_ID_HEADER)
            .and_then(|v| v.to_str().ok())
            .expect("request id should be injected");
        assert!(request_id.starts_with("req-"), "expected req- fallback, got: {request_id}");
    }
}
