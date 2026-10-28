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

// This prototype is synchronous and must use a fresh, span-free dispatcher.
fn emit_candidate(dispatch: &Dispatch, phase: u64) {
    let _context_guard = Context::new().attach();
    dispatcher::with_default(dispatch, || {
        assert!(
            Context::current().baggage().get("synthetic-secret").is_none(),
            "ambient baggage must be hidden"
        );
        assert!(!Context::current().has_active_span(), "ambient OTEL span must be hidden");
        emit_root_event(phase);
    });
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

mod async_isolation {
    use super::*;
    use std::{
        collections::HashSet,
        future::pending,
        panic::{AssertUnwindSafe, catch_unwind},
        sync::atomic::{AtomicBool, AtomicUsize, Ordering},
        time::Duration,
    };
    use tracing::{Instrument, instrument::WithSubscriber};
    use tracing_subscriber::EnvFilter;

    const PARTICIPANTS: usize = 4;
    const WAIT: Duration = Duration::from_secs(30);

    fn parent_span(dispatch: &Dispatch) -> tracing::Span {
        dispatcher::with_default(dispatch, || {
            tracing::info_span!("http-request", uri = FAKE_URI, authorization = FAKE_AUTH, request_id = "synthetic-id")
        })
    }

    fn observe_once(dispatch: &Dispatch, parent: &Option<tracing::span::Id>, phase: u64) {
        let _context = fake_ambient_context().attach();
        assert_eq!(&tracing::Span::current().id(), parent);
        emit_candidate(dispatch, phase);
        assert_eq!(&tracing::Span::current().id(), parent);
        assert_eq!(Context::current().span().span_context().trace_id(), TraceId::from(17));
        assert!(Context::current().baggage().get("synthetic-secret").is_some());
    }

    fn assert_isolated_events(sinks: &Sinks, expected_phases: &[u64]) {
        let json = sinks.json_records();
        let logs = sinks.exported_records();
        assert_eq!(json.len(), expected_phases.len(), "each event must reach the isolated JSON sink");
        assert_eq!(logs.len(), expected_phases.len(), "each event must reach the isolated exporter");
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
        let mut json_phases = Vec::new();
        for entry in &json {
            let object = entry.as_object().expect("diagnostic JSON must be an object");
            assert!(object.keys().all(|key| json_keys.contains(key.as_str())), "unexpected JSON field");
            assert_eq!(entry["target"].as_str(), Some(TARGET));
            assert_eq!(entry["message"].as_str(), Some("diagnostic fixture"));
            assert_eq!(entry["expected"].as_u64(), Some(1024));
            assert_eq!(entry["emitted"].as_u64(), Some(64));
            json_phases.push(entry["phase"].as_u64().expect("phase is an unsigned number"));
            assert!(!entry.to_string().contains(FAKE_URI));
            assert!(!entry.to_string().contains(FAKE_AUTH));
        }
        let mut otel_phases = Vec::new();
        for (log, scope) in &logs {
            assert_eq!(log.target().map(|target| target.as_ref()), Some(TARGET));
            assert!(log.trace_context().is_none());
            assert!(log.attributes_iter().all(|(key, _)| otel_keys.contains(key.as_str())));
            assert!(matches!(log.body(), Some(AnyValue::String(body)) if body.as_str() == "diagnostic fixture"));
            assert!(
                log.attributes_iter()
                    .any(|(key, value)| key.as_str() == "expected" && value == &AnyValue::Int(1024))
            );
            assert!(
                log.attributes_iter()
                    .any(|(key, value)| key.as_str() == "emitted" && value == &AnyValue::Int(64))
            );
            let phase = log
                .attributes_iter()
                .find_map(|(key, value)| match (key.as_str(), value) {
                    ("phase", AnyValue::Int(phase)) => u64::try_from(*phase).ok(),
                    _ => None,
                })
                .expect("phase is a nonnegative OTEL integer");
            otel_phases.push(phase);
            assert_eq!(scope.name(), "");
            assert!(scope.version().is_none());
            assert!(scope.schema_url().is_none());
            assert!(scope.attributes().next().is_none());
            assert!(!format!("{log:?}").contains(FAKE_URI));
            assert!(!format!("{log:?}").contains(FAKE_AUTH));
        }
        json_phases.sort_unstable();
        otel_phases.sort_unstable();
        let mut expected_phases = expected_phases.to_vec();
        expected_phases.sort_unstable();
        assert_eq!(json_phases, expected_phases);
        assert_eq!(otel_phases, expected_phases);
        let resource = sinks.exporter.resource.lock().expect("capture exporter resource");
        assert!(resource.as_ref().expect("exporter resource exists").iter().next().is_none());
    }

    #[test]
    fn separate_threads_keep_their_contexts_across_async_suspension() {
        let outer = Sinks::new(true, true);
        let isolated = Sinks::new(true, true);
        let barrier = Arc::new(tokio::sync::Barrier::new(PARTICIPANTS));
        let thread_ids: HashSet<_> = std::thread::scope(|scope| {
            let mut handles = Vec::new();
            for _ in 0..PARTICIPANTS {
                let outer_dispatch = outer.dispatch.clone();
                let isolated_dispatch = isolated.dispatch.clone();
                let barrier = Arc::clone(&barrier);
                handles.push(scope.spawn(move || {
                    let thread_id = std::thread::current().id();
                    let span = parent_span(&outer_dispatch);
                    let span_id = span.id();
                    let runtime = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .expect("build a test-only runtime");
                    runtime.block_on(
                        async move {
                            for phase in [0_u64, 1] {
                                tokio::time::timeout(WAIT, barrier.wait())
                                    .await
                                    .expect("all threads reach the barrier");
                                observe_once(&isolated_dispatch, &span_id, phase);
                                tokio::task::yield_now().await;
                            }
                        }
                        .instrument(span)
                        .with_subscriber(outer_dispatch),
                    );
                    thread_id
                }));
            }
            handles
                .into_iter()
                .map(|handle| handle.join().expect("thread completes"))
                .collect()
        });
        assert_eq!(thread_ids.len(), PARTICIPANTS, "the fixture must use distinct OS threads");
        assert!(outer.json_records().is_empty());
        assert!(outer.exported_records().is_empty());
        assert_isolated_events(&isolated, &[0, 1, 0, 1, 0, 1, 0, 1]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn shared_runtime_reenters_outer_context_after_each_await() {
        let outer = Sinks::new(true, true);
        let isolated = Sinks::new(true, true);
        let barrier = Arc::new(tokio::sync::Barrier::new(PARTICIPANTS));
        let mut tasks = Vec::new();
        for _ in 0..PARTICIPANTS {
            let outer_dispatch = outer.dispatch.clone();
            let isolated_dispatch = isolated.dispatch.clone();
            let barrier = Arc::clone(&barrier);
            let span = parent_span(&outer_dispatch);
            let span_id = span.id();
            tasks.push(tokio::spawn(
                async move {
                    for phase in [0_u64, 1] {
                        tokio::time::timeout(WAIT, barrier.wait())
                            .await
                            .expect("all tasks reach the barrier");
                        observe_once(&isolated_dispatch, &span_id, phase);
                        tokio::task::yield_now().await;
                    }
                }
                .instrument(span)
                .with_subscriber(outer_dispatch),
            ));
        }
        for task in tasks {
            tokio::time::timeout(WAIT, task)
                .await
                .expect("task finishes")
                .expect("task succeeds");
        }
        assert!(outer.json_records().is_empty());
        assert!(outer.exported_records().is_empty());
        assert_isolated_events(&isolated, &[0, 1, 0, 1, 0, 1, 0, 1]);
    }

    #[test]
    fn either_isolation_step_alone_leaves_ambient_context() {
        let outer = Sinks::new(true, true);
        let separate = Sinks::new(true, true);
        dispatcher::with_default(&outer.dispatch, || {
            let span = parent_span(&outer.dispatch);
            let _span = span.enter();
            let _context = fake_ambient_context().attach();
            dispatcher::with_default(&separate.dispatch, || emit_root_event(0));
            let _empty_context = Context::new().attach();
            emit_root_event(1);
        });

        let separate_json = separate.json_records();
        let separate_logs = separate.exported_records();
        assert_eq!(separate_json.len(), 1);
        assert_eq!(separate_logs.len(), 1);
        assert!(separate_json[0].get("span").is_none());
        assert_eq!(
            separate_logs[0]
                .0
                .trace_context()
                .expect("ambient trace still exists")
                .trace_id,
            TraceId::from(17)
        );

        let outer_json = outer.json_records();
        let outer_logs = outer.exported_records();
        assert_eq!(outer_json.len(), 1);
        assert_eq!(outer_logs.len(), 1);
        assert_eq!(outer_json[0]["span"]["uri"].as_str(), Some(FAKE_URI));
        assert_eq!(outer_json[0]["span"]["authorization"].as_str(), Some(FAKE_AUTH));
        assert!(outer_logs[0].0.trace_context().is_none());
    }

    fn filtered_sinks(filter: &str, sensitive_resource: bool) -> Sinks {
        let json = MemoryWriter::default();
        let exporter = MemoryExporter::default();
        let resource = if sensitive_resource {
            Resource::builder_empty()
                .with_attributes([KeyValue::new("synthetic.resource", FAKE_AUTH)])
                .build()
        } else {
            Resource::builder_empty().build()
        };
        let provider = SdkLoggerProvider::builder()
            .with_resource(resource)
            .with_simple_exporter(exporter.clone())
            .build();
        let bridge = OpenTelemetryTracingBridge::builder(&provider)
            .with_tracing_span_attributes(TracingSpanAttributes::all())
            .build();
        let subscriber = tracing_subscriber::registry()
            .with(EnvFilter::new(filter))
            .with(build_json_log_layer(json.clone(), false, FmtSpan::NONE, true))
            .with(bridge);
        Sinks {
            dispatch: Dispatch::new(subscriber),
            json,
            exporter,
            _provider: provider,
        }
    }

    #[test]
    fn filters_and_resources_do_not_cross_dispatchers() {
        let outer = filtered_sinks("info,rustfs_isolation_probe_test=off", true);
        let isolated = filtered_sinks("off,rustfs_isolation_probe_test=info", false);
        let disabled = filtered_sinks("off", false);
        dispatcher::with_default(&outer.dispatch, || {
            let span = parent_span(&outer.dispatch);
            let _span = span.enter();
            let parent_id = span.id();
            observe_once(&isolated.dispatch, &parent_id, 0);
            observe_once(&disabled.dispatch, &parent_id, 1);
            tracing::info!(target: "isolation_outer_control", value = 1_u64, "outer control");
        });
        let outer_json = outer.json_records();
        let outer_logs = outer.exported_records();
        assert_eq!(outer_json.len(), 1);
        assert_eq!(outer_logs.len(), 1);
        assert_eq!(outer_json[0]["target"].as_str(), Some("isolation_outer_control"));
        assert_eq!(outer_logs[0].0.target().map(|target| target.as_ref()), Some("isolation_outer_control"));
        assert!(
            outer
                .exporter
                .resource
                .lock()
                .expect("capture outer resource")
                .as_ref()
                .expect("outer resource exists")
                .get(&opentelemetry::Key::new("synthetic.resource"))
                .is_some()
        );
        assert_isolated_events(&isolated, &[0]);
        assert!(disabled.json_records().is_empty());
        assert!(disabled.exported_records().is_empty());
    }

    struct CancellationProbe {
        outer: Dispatch,
        isolated: Dispatch,
        drops: Arc<AtomicUsize>,
        restored: Arc<AtomicBool>,
    }

    impl Drop for CancellationProbe {
        fn drop(&mut self) {
            dispatcher::with_default(&self.outer, || {
                let span = parent_span(&self.outer);
                let _span = span.enter();
                let _context = fake_ambient_context().attach();
                emit_candidate(&self.isolated, 1);
                let restored = tracing::Span::current().id() == span.id()
                    && Context::current().span().span_context().trace_id() == TraceId::from(17)
                    && Context::current().baggage().get("synthetic-secret").is_some();
                self.restored.store(restored, Ordering::SeqCst);
            });
            self.drops.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancellation_drop_emits_in_isolation_and_restores_context() {
        let outer = Sinks::new(true, true);
        let isolated = Sinks::new(true, true);
        let drops = Arc::new(AtomicUsize::new(0));
        let restored = Arc::new(AtomicBool::new(false));
        let probe = CancellationProbe {
            outer: outer.dispatch.clone(),
            isolated: isolated.dispatch.clone(),
            drops: Arc::clone(&drops),
            restored: Arc::clone(&restored),
        };
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            let _probe = probe;
            started_tx.send(()).expect("signal that the cancellation fixture is active");
            pending::<()>().await;
        });
        tokio::time::timeout(WAIT, started_rx)
            .await
            .expect("task starts")
            .expect("start signal");
        task.abort();
        let error = tokio::time::timeout(WAIT, task)
            .await
            .expect("cancelled task finishes")
            .expect_err("the task must be cancelled");
        assert!(error.is_cancelled());
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        assert!(restored.load(Ordering::SeqCst));
        assert!(outer.json_records().is_empty());
        assert!(outer.exported_records().is_empty());
        assert_isolated_events(&isolated, &[1]);
    }

    #[derive(Clone)]
    struct PanicWriter;

    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for PanicWriter {
        type Writer = Self;

        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    impl Write for PanicWriter {
        fn write(&mut self, _: &[u8]) -> io::Result<usize> {
            panic!("synthetic writer panic");
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn panic_during_sink_write_restores_the_outer_context() {
        let outer = Sinks::new(true, true);
        let panic_dispatch =
            Dispatch::new(tracing_subscriber::registry().with(build_json_log_layer(PanicWriter, false, FmtSpan::NONE, true)));
        dispatcher::with_default(&outer.dispatch, || {
            let span = parent_span(&outer.dispatch);
            let _span = span.enter();
            let _context = fake_ambient_context().attach();
            let panic = catch_unwind(AssertUnwindSafe(|| emit_candidate(&panic_dispatch, 0)));
            assert!(panic.is_err(), "the fixture must panic while writing the isolated event");
            assert_eq!(tracing::Span::current().id(), span.id());
            assert_eq!(Context::current().span().span_context().trace_id(), TraceId::from(17));
            assert!(Context::current().baggage().get("synthetic-secret").is_some());
            tracing::info!(target: "isolation_outer_control", value = 1_u64, "outer control");
        });
        let json = outer.json_records();
        let logs = outer.exported_records();
        assert_eq!(json.len(), 1);
        assert_eq!(logs.len(), 1);
        assert_eq!(json[0]["target"].as_str(), Some("isolation_outer_control"));
        assert_eq!(logs[0].0.target().map(|target| target.as_ref()), Some("isolation_outer_control"));
    }

    #[derive(Clone)]
    struct ObservedPanicWriter(Arc<AtomicBool>);

    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for ObservedPanicWriter {
        type Writer = Self;

        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    impl Write for ObservedPanicWriter {
        fn write(&mut self, _: &[u8]) -> io::Result<usize> {
            self.0.store(true, Ordering::SeqCst);
            panic!("synthetic writer panic");
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    fn classify_panic_isolation() -> &'static str {
        let outer = Sinks::new(true, true);
        let reached = Arc::new(AtomicBool::new(false));
        let panic_dispatch = Dispatch::new(tracing_subscriber::registry().with(build_json_log_layer(
            ObservedPanicWriter(Arc::clone(&reached)),
            false,
            FmtSpan::NONE,
            true,
        )));
        let early_result = dispatcher::with_default(&outer.dispatch, || {
            let span = parent_span(&outer.dispatch);
            let _span = span.enter();
            let _context = fake_ambient_context().attach();
            let outer_span = span.id();
            if outer_span.is_none() {
                return Some("outer_span_missing");
            }
            let caught = catch_unwind(AssertUnwindSafe(|| emit_candidate(&panic_dispatch, 0))).is_err();
            if !reached.load(Ordering::SeqCst) {
                return Some("writer_not_reached");
            }
            if !caught {
                return Some("writer_panic_not_caught");
            }
            let dispatcher_restored = dispatcher::get_default(|current| {
                current
                    .downcast_ref::<MemoryWriter>()
                    .is_some_and(|writer| Arc::ptr_eq(&writer.0, &outer.json.0))
            });
            if !dispatcher_restored {
                return Some("dispatcher_not_restored");
            }
            if tracing::Span::current().id() != outer_span {
                return Some("tracing_not_restored");
            }
            let current = Context::current();
            if current.span().span_context().trace_id() != TraceId::from(17) {
                return Some("trace_not_restored");
            }
            if current.span().span_context().span_id() != SpanId::from(19) {
                return Some("span_id_not_restored");
            }
            if current.baggage().get("synthetic-secret").map(|value| value.as_str()) != Some(FAKE_AUTH) {
                return Some("baggage_not_restored");
            }
            tracing::info!(target: "isolation_outer_control", value = 1_u64, "outer control");
            None
        });
        if let Some(class) = early_result {
            return class;
        }
        let bytes = match outer.json.0.lock() {
            Ok(bytes) => bytes.clone(),
            Err(_) => return "outer_json_unknown",
        };
        let text = match std::str::from_utf8(&bytes) {
            Ok(text) => text,
            Err(_) => return "outer_json_unknown",
        };
        let json: Vec<Value> = match text.lines().map(serde_json::from_str).collect() {
            Ok(json) => json,
            Err(_) => return "outer_json_unknown",
        };
        let logs = outer.exported_records();
        if json.len() == 2
            && json[0]["target"].as_str() == Some(TARGET)
            && json[1]["target"].as_str() == Some("isolation_outer_control")
        {
            return "stale_isolated_json";
        }
        if json.len() != 1 || json[0]["target"].as_str() != Some("isolation_outer_control") {
            return "outer_json_unexpected";
        }
        if logs.len() != 1 || logs[0].0.target().map(|target| target.as_ref()) != Some("isolation_outer_control") {
            return "outer_otel_unexpected";
        }
        "clean"
    }

    type PanicHook = Box<dyn Fn(&std::panic::PanicHookInfo<'_>) + Send + Sync + 'static>;

    struct RestorePanicHook(Option<PanicHook>);

    impl Drop for RestorePanicHook {
        fn drop(&mut self) {
            if let Some(previous) = self.0.take() {
                std::panic::set_hook(previous);
            }
        }
    }

    #[test]
    fn classify_panic_writer_isolation_without_raw_output() {
        // This process-global hook is used only by the workflow's single exact test.
        let hook = RestorePanicHook(Some(std::panic::take_hook()));
        std::panic::set_hook(Box::new(|_| {
            let _ = writeln!(std::io::stderr(), "\nRUSTFS_PANIC_HOOK=redacted");
        }));
        let result = catch_unwind(AssertUnwindSafe(|| {
            let class = catch_unwind(AssertUnwindSafe(classify_panic_isolation)).unwrap_or("diagnostic_internal_panic");
            println!("\nRUSTFS_PANIC_DIAGNOSTIC={class}");
            assert!(class == "clean", "isolation diagnostic failed");
        }));
        drop(hook);
        if let Err(payload) = result {
            std::panic::resume_unwind(payload);
        }
    }
}
