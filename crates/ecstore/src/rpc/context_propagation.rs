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
    if let Some(trace_id) = current_trace_id()
        && let Ok(value) = HeaderValue::from_str(&trace_id)
    {
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
    let Some(trace_id) = current_trace_id() else {
        return;
    };
    let Ok(value) = tonic::metadata::MetadataValue::try_from(trace_id.as_str()) else {
        return;
    };
    metadata.insert(request_id_key, value);
}
