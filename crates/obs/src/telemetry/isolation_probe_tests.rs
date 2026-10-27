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

//! Test-only diagnostic isolation prototype. Nothing here is a GET hook.

use super::local::build_json_log_layer;
use opentelemetry::{
    Context, InstrumentationScope, KeyValue,
    baggage::BaggageExt,
    logs::AnyValue,
    trace::{SpanContext, SpanId, TraceContextExt, TraceFlags, TraceId, TraceState},
};
use opentelemetry_appender_tracing::layer::{OpenTelemetryTracingBridge, TracingSpanAttributes};
use opentelemetry_sdk::{
    Resource,
    error::OTelSdkResult,
    logs::{LogBatch, LogExporter, SdkLogRecord, SdkLoggerProvider},
};
use serde_json::Value;
use std::{
    collections::BTreeSet,
    io::{self, Write},
    sync::{Arc, Mutex},
};
use tracing::{Dispatch, dispatcher};
use tracing_subscriber::{fmt::format::FmtSpan, layer::SubscriberExt};

const TARGET: &str = "rustfs_isolation_probe_test";
const FAKE_URI: &str = "/synthetic/private-object";
const FAKE_AUTH: &str = "synthetic-private-header";

#[derive(Clone, Debug, Default)]
struct MemoryWriter(Arc<Mutex<Vec<u8>>>);

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for MemoryWriter {
    type Writer = Self;

    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

impl Write for MemoryWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0.lock().expect("capture JSON output").extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[derive(Clone, Debug, Default)]
struct MemoryExporter {
    records: Arc<Mutex<Vec<(SdkLogRecord, InstrumentationScope)>>>,
    resource: Arc<Mutex<Option<Resource>>>,
}

impl LogExporter for MemoryExporter {
    async fn export(&self, batch: LogBatch<'_>) -> OTelSdkResult {
        self.records
            .lock()
            .expect("capture exporter records")
            .extend(batch.iter().map(|(record, scope)| (record.clone(), scope.clone())));
        Ok(())
    }

    fn set_resource(&mut self, resource: &Resource) {
        *self.resource.lock().expect("capture exporter resource") = Some(resource.clone());
    }
}

struct Sinks {
    dispatch: Dispatch,
    json: MemoryWriter,
    exporter: MemoryExporter,
    _provider: SdkLoggerProvider,
}

impl Sinks {
    fn new(span_list: bool, enrich_spans: bool) -> Self {
        let json = MemoryWriter::default();
        let exporter = MemoryExporter::default();
        let provider = SdkLoggerProvider::builder()
            .with_resource(Resource::builder_empty().build())
            .with_simple_exporter(exporter.clone())
            .build();
        let bridge = OpenTelemetryTracingBridge::builder(&provider);
        let bridge = if enrich_spans {
            bridge.with_tracing_span_attributes(TracingSpanAttributes::all()).build()
        } else {
            bridge.build()
        };
        let subscriber = tracing_subscriber::registry()
            .with(build_json_log_layer(json.clone(), false, FmtSpan::NONE, span_list))
            .with(bridge);
        Self {
            dispatch: Dispatch::new(subscriber),
            json,
            exporter,
            _provider: provider,
        }
    }

    fn json_records(&self) -> Vec<Value> {
        let bytes = self.json.0.lock().expect("capture JSON output").clone();
        let text = String::from_utf8(bytes).expect("JSON output is UTF-8");
        text.lines()
            .map(|line| serde_json::from_str(line).expect("each captured record is JSON"))
            .collect()
    }

    fn exported_records(&self) -> Vec<(SdkLogRecord, InstrumentationScope)> {
        self.exporter.records.lock().expect("capture exporter records").clone()
    }
}

fn fake_ambient_context() -> Context {
    Context::new()
        .with_remote_span_context(SpanContext::new(
            TraceId::from(17),
            SpanId::from(19),
            TraceFlags::SAMPLED,
            true,
            TraceState::default(),
        ))
        .with_baggage([KeyValue::new("synthetic-secret", FAKE_AUTH)])
}

fn emit_root_event(phase: u64) {
    tracing::info!(target: TARGET, parent: None, expected = 1024_u64, emitted = 64_u64, phase, "diagnostic fixture");
}

// The initial implementation deliberately models `parent: None` without
// isolation so the contract below has a reviewable red baseline.
fn emit_candidate(_dispatch: &Dispatch, phase: u64) {
    assert!(
        Context::current().baggage().get("synthetic-secret").is_none(),
        "ambient baggage must be hidden"
    );
    assert!(!Context::current().has_active_span(), "ambient OTEL span must be hidden");
    emit_root_event(phase);
}

#[test]
fn parent_none_alone_keeps_json_parent_and_otel_thread_context() {
    let sinks = Sinks::new(true, true);
    let _context_guard = fake_ambient_context().attach();

    dispatcher::with_default(&sinks.dispatch, || {
        let span = tracing::info_span!("http-request", uri = FAKE_URI, authorization = FAKE_AUTH, request_id = "synthetic-id");
        let _span_guard = span.enter();
        emit_root_event(0);
    });

    let json = sinks.json_records();
    let logs = sinks.exported_records();
    assert_eq!(json.len(), 1, "the control event must be captured");
    assert_eq!(logs.len(), 1, "the control event must reach the exporter");
    assert_eq!(json[0]["span"]["uri"].as_str(), Some(FAKE_URI));
    assert_eq!(json[0]["span"]["authorization"].as_str(), Some(FAKE_AUTH));
    let context = logs[0].0.trace_context().expect("ambient OTEL trace context is present");
    assert_eq!(context.trace_id, TraceId::from(17));
    assert_eq!(context.span_id, SpanId::from(19));
}

#[test]
fn diagnostic_fixture_is_detached_for_both_sinks_and_restores_context() {
    for span_list in [false, true] {
        for enrich_spans in [false, true] {
            let outer = Sinks::new(span_list, enrich_spans);
            let isolated = Sinks::new(span_list, enrich_spans);
            let _context_guard = fake_ambient_context().attach();

            dispatcher::with_default(&outer.dispatch, || {
                let span =
                    tracing::info_span!("http-request", uri = FAKE_URI, authorization = FAKE_AUTH, request_id = "synthetic-id");
                let _span_guard = span.enter();
                for phase in [0, 1] {
                    emit_candidate(&isolated.dispatch, phase);
                    assert_eq!(tracing::Span::current().id(), span.id(), "restore the outer tracing span");
                    assert_eq!(Context::current().span().span_context().trace_id(), TraceId::from(17));
                    assert!(Context::current().baggage().get("synthetic-secret").is_some());
                }
            });

            assert!(outer.json_records().is_empty(), "diagnostic events must not reach the outer JSON sink");
            assert!(outer.exported_records().is_empty(), "diagnostic events must not reach the outer exporter");
            let json = isolated.json_records();
            let logs = isolated.exported_records();
            assert_eq!(json.len(), 2, "both diagnostic events must reach the isolated JSON sink");
            assert_eq!(logs.len(), 2, "both diagnostic events must reach the isolated exporter");

            let json_keys: BTreeSet<&str> = [
                "timestamp",
                "level",
                "target",
                "filename",
                "line_number",
                "threadName",
                "threadId",
                "message",
                "expected",
                "emitted",
                "phase",
            ]
            .into_iter()
            .collect();
            let otel_keys: BTreeSet<&str> = [
                "code.namespace",
                "code.filepath",
                "code.filename",
                "code.lineno",
                "expected",
                "emitted",
                "phase",
            ]
            .into_iter()
            .collect();
            let resource = isolated.exporter.resource.lock().expect("capture exporter resource");
            assert!(
                resource
                    .as_ref()
                    .expect("resource was passed to exporter")
                    .iter()
                    .next()
                    .is_none()
            );
            for (phase, (entry, (log, scope))) in [0_u64, 1].into_iter().zip(json.iter().zip(&logs)) {
                let object = entry.as_object().expect("diagnostic JSON must be an object");
                assert!(object.keys().all(|key| json_keys.contains(key.as_str())), "unexpected JSON field");
                assert_eq!(entry["target"].as_str(), Some(TARGET));
                assert_eq!(entry["phase"].as_u64(), Some(phase));
                assert_eq!(entry["expected"].as_u64(), Some(1024));
                assert_eq!(entry["emitted"].as_u64(), Some(64));
                assert!(!entry.to_string().contains(FAKE_URI));
                assert!(!entry.to_string().contains(FAKE_AUTH));

                assert_eq!(log.target().map(|target| target.as_ref()), Some(TARGET));
                assert!(log.trace_context().is_none(), "ambient OTEL context must not reach the exporter");
                assert_eq!(scope.name(), "");
                assert!(scope.version().is_none());
                assert!(scope.schema_url().is_none());
                assert!(scope.attributes().next().is_none());
                assert!(
                    log.attributes_iter().all(|(key, _)| otel_keys.contains(key.as_str())),
                    "unexpected OTEL field"
                );
                let phase = i64::try_from(phase).expect("fixture phase fits into i64");
                assert!(
                    log.attributes_iter()
                        .any(|(key, value)| key.as_str() == "phase" && value == &AnyValue::Int(phase))
                );
                assert!(
                    log.attributes_iter()
                        .any(|(key, value)| key.as_str() == "expected" && value == &AnyValue::Int(1024))
                );
                assert!(
                    log.attributes_iter()
                        .any(|(key, value)| key.as_str() == "emitted" && value == &AnyValue::Int(64))
                );
                assert!(!format!("{log:?}").contains(FAKE_URI));
                assert!(!format!("{log:?}").contains(FAKE_AUTH));
            }
        }
    }
}
