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

//! External S3 HTTP outcomes, including requests rejected before S3 dispatch.
//! Admin snapshots and metric exporters share these counters. The older
//! operation counter counts handler entries and is not an HTTP denominator.
//!
//! Entry-to-dispatch wall time starts at the external request guard and ends
//! at its first typed S3 dispatch, or at response/error/cancellation when no
//! operation was dispatched. It includes authentication, parsing and scheduler
//! waits within that boundary; it is not pure queue time or body-stream time.
//! Timing is enabled only when the general metrics gate is on at entry.

use rustfs_s3_ops::S3Operation;
use std::cell::Cell;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{LazyLock, OnceLock};
use std::time::{Duration, Instant};

/// Receives a finished external request's first dispatched operation, its
/// latency, and whether it produced a 2xx response. The server injects this
/// so the leaf metrics crate never depends on a trace-bus implementation.
pub type S3HttpCompletionObserver = fn(S3Operation, Duration, bool);
pub type S3HttpCompletionObserverEnabled = fn() -> bool;

const METRIC: &str = "rustfs_s3_http_requests_total";
const LOCK_FAILURE_METRIC: &str = "rustfs_s3_lock_failures_total";
const ENTRY_TO_DISPATCH_METRIC: &str = "rustfs_s3_http_entry_to_dispatch_duration_seconds";
const METHODS: [&str; 10] = [
    "GET", "PUT", "POST", "DELETE", "HEAD", "OPTIONS", "PATCH", "CONNECT", "TRACE", "OTHER",
];
const OUTCOMES: [&str; 8] = ["1xx", "2xx", "3xx", "4xx", "5xx", "unknown", "service_error", "cancelled"];
const UNKNOWN_OPERATION: usize = S3Operation::ALL.len();
static COUNTERS: LazyLock<HttpOutcomeCounters> = LazyLock::new(HttpOutcomeCounters::new);
static COMPLETION_OBSERVER: OnceLock<(S3HttpCompletionObserverEnabled, S3HttpCompletionObserver)> = OnceLock::new();

/// A bounded S3 operation captured from the current external-request context.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct S3HttpOperationContext(S3Operation);

impl S3HttpOperationContext {
    /// Return the stable operation index used for bounded metric-handle lookup.
    pub fn operation_index(self) -> usize {
        self.0.metric_index()
    }

    /// Return the stable S3 operation label associated with this context.
    pub fn operation_name(self) -> &'static str {
        self.0.as_str()
    }
}

/// Return the current S3 operation, when the task is inside a dispatched S3 request.
pub fn current_s3_http_operation_context() -> Option<S3HttpOperationContext> {
    #[cfg(feature = "diagnostic-read-version-caller")]
    if let Ok(index) = ISOLATED_OPERATION.try_with(Cell::get) {
        return S3Operation::ALL.get(index).copied().map(S3HttpOperationContext);
    }
    CURRENT_OPERATION
        .try_with(|current| {
            let index = current.get().operation;
            S3Operation::ALL.get(index).copied().map(S3HttpOperationContext)
        })
        .ok()
        .flatten()
}

/// Return the current S3 operation label, or `None` for internal/non-S3 work.
pub fn current_s3_http_operation_name() -> Option<&'static str> {
    current_s3_http_operation_context().map(S3HttpOperationContext::operation_name)
}

/// Reapply a captured S3 operation while polling work on a child Tokio task.
pub async fn with_s3_http_operation_context<F>(context: Option<S3HttpOperationContext>, future: F) -> F::Output
where
    F: Future,
{
    match context {
        Some(context) => {
            #[cfg(feature = "diagnostic-read-version-caller")]
            return with_isolated_s3_http_operation_context(Some(context), future).await;
            #[cfg(not(feature = "diagnostic-read-version-caller"))]
            CURRENT_OPERATION
                .scope(Cell::new(RequestScopeState::operation_only(context.operation_index())), future)
                .await
        }
        None => future.await,
    }
}

/// Restore an item's operation, including explicit absence, without changing
/// the ambient HTTP request guard or borrowing the request that flushed a batch.
#[cfg(feature = "diagnostic-read-version-caller")]
pub async fn with_isolated_s3_http_operation_context<F>(context: Option<S3HttpOperationContext>, future: F) -> F::Output
where
    F: Future,
{
    let index = context.map_or(UNKNOWN_OPERATION, S3HttpOperationContext::operation_index);
    ISOLATED_OPERATION
        .scope(Cell::new(index), async {
            match context {
                Some(context) => {
                    CURRENT_OPERATION
                        .scope(Cell::new(RequestScopeState::operation_only(context.operation_index())), future)
                        .await
                }
                None => future.await,
            }
        })
        .await
}

#[cfg(feature = "diagnostic-read-version-caller")]
tokio::task_local! {
    static ISOLATED_OPERATION: Cell<usize>;
}

#[derive(Clone, Copy)]
struct RequestScopeState {
    operation: usize,
    entry_started: Option<Instant>,
}

impl RequestScopeState {
    // A child operation carries identity, never ownership of its parent's timer.
    fn operation_only(operation: usize) -> Self {
        Self {
            operation,
            entry_started: None,
        }
    }
}

tokio::task_local! {
    static CURRENT_OPERATION: Cell<RequestScopeState>;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct S3HttpMetricSnapshot {
    pub method: &'static str,
    pub operation: &'static str,
    pub outcome: &'static str,
    pub total: u64,
}

struct OutcomeCounter {
    total: AtomicU64,
    exported: OnceLock<metrics::Counter>,
}

struct HttpOutcomeCounters(Box<[OutcomeCounter]>);

impl HttpOutcomeCounters {
    fn new() -> Self {
        Self(
            std::iter::repeat_with(|| OutcomeCounter {
                total: AtomicU64::new(0),
                exported: OnceLock::new(),
            })
            .take(METHODS.len() * (UNKNOWN_OPERATION + 1) * OUTCOMES.len())
            .collect(),
        )
    }

    fn record(&self, method: usize, operation: usize, outcome: usize) {
        let counter = &self.0[(method * (UNKNOWN_OPERATION + 1) + operation) * OUTCOMES.len() + outcome];
        counter.total.fetch_add(1, Ordering::Relaxed);
        counter
            .exported
            .get_or_init(|| {
                counter!(METRIC, "method" => METHODS[method], "op" => operation_label(operation), "outcome" => OUTCOMES[outcome])
            })
            .increment(1);
    }

    fn snapshot(&self) -> Vec<S3HttpMetricSnapshot> {
        // Individual series are monotonic; a concurrent snapshot is not a
        // transaction across series. Rates must compare consecutive samples.
        self.0
            .iter()
            .enumerate()
            .filter_map(|(index, counter)| {
                let total = counter.total.load(Ordering::Relaxed);
                (total != 0).then(|| S3HttpMetricSnapshot {
                    method: METHODS[index / OUTCOMES.len() / (UNKNOWN_OPERATION + 1)],
                    operation: operation_label(index / OUTCOMES.len() % (UNKNOWN_OPERATION + 1)),
                    outcome: OUTCOMES[index % OUTCOMES.len()],
                    total,
                })
            })
            .collect()
    }
}

fn operation_label(index: usize) -> &'static str {
    S3Operation::ALL.get(index).map_or("unknown", |op| op.as_str())
}

/// Set the request's first dispatched operation without counting another handler entry.
pub fn observe_s3_http_operation(op: S3Operation) {
    let _ = CURRENT_OPERATION.try_with(|current| {
        // Internal operations must not overwrite the external request's first
        // dispatched operation. No task-local scope means non-HTTP work.
        let mut state = current.get();
        if state.operation == UNKNOWN_OPERATION {
            state.operation = op.metric_index();
            let started = state.entry_started.take();
            current.set(state);
            if let Some(started) = started {
                record_entry_to_dispatch(op.metric_index(), "dispatched", started);
            }
        }
    });
}

/// Attribute a lock-related S3 failure to the request operation without
/// exporting bucket, object, owner, or request identifiers as metric labels.
pub fn record_s3_http_lock_failure(resource_class: &'static str, mode: &'static str, reason: &'static str) {
    let _ = CURRENT_OPERATION.try_with(|current| {
        counter!(
            LOCK_FAILURE_METRIC,
            "op" => operation_label(current.get().operation),
            "resource_class" => resource_class,
            "mode" => mode,
            "reason" => reason
        )
        .increment(1);
    });
}

/// An external request is counted exactly once: at response headers, at a
/// service error, or when its future is dropped before producing a response.
/// Body-stream failures after headers use the existing streaming metrics.
pub struct S3HttpRequestGuard {
    method: usize,
    operation: usize,
    completion: Option<(S3HttpCompletionObserver, Instant)>,
    finished: bool,
    entry_started: Option<Instant>,
}

impl S3HttpRequestGuard {
    /// First typed external operation seen so far, including after a pending
    /// service poll. This does not finish or transfer HTTP outcome accounting.
    pub fn operation(&self) -> Option<S3Operation> {
        S3Operation::ALL.get(self.operation).copied()
    }

    pub fn is_active() -> bool {
        CURRENT_OPERATION.try_with(|_| ()).is_ok()
    }

    pub fn new(method: &str) -> Self {
        Self {
            method: METHODS.iter().position(|known| *known == method).unwrap_or(METHODS.len() - 1),
            operation: UNKNOWN_OPERATION,
            completion: COMPLETION_OBSERVER
                .get()
                .and_then(|(enabled, observer)| enabled().then(|| (*observer, Instant::now()))),
            finished: false,
            entry_started: crate::metrics_enabled().then(Instant::now),
        }
    }

    /// Report the request to `observer` when it finishes with a dispatched S3
    /// operation. Latency is measured from this call.
    pub fn with_completion_observer(mut self, observer: S3HttpCompletionObserver) -> Self {
        self.completion = Some((observer, Instant::now()));
        self
    }

    /// Attribute existing operation instrumentation without changing S3
    /// handlers or propagating metric labels through storage/RPC contracts.
    pub fn in_scope<T>(&mut self, f: impl FnOnce() -> T) -> T {
        // Copy back even while unwinding: a dispatch already consumed this
        // entry timer and must not be emitted again by the outer drop guard.
        struct CopyBack<'a>(&'a mut S3HttpRequestGuard);
        impl Drop for CopyBack<'_> {
            fn drop(&mut self) {
                let state = CURRENT_OPERATION.with(Cell::get);
                self.0.operation = state.operation;
                self.0.entry_started = state.entry_started;
            }
        }
        let state = RequestScopeState {
            operation: self.operation,
            entry_started: self.entry_started,
        };
        CURRENT_OPERATION.sync_scope(Cell::new(state), || {
            let _copy_back = CopyBack(self);
            f()
        })
    }

    pub fn response(&mut self, status: u16) {
        let outcome = match status {
            100..=599 => usize::from(status / 100 - 1),
            _ => 5,
        };
        self.finish(outcome);
    }

    pub fn service_error(&mut self) {
        self.finish(6);
    }

    fn finish(&mut self, outcome: usize) {
        if !self.finished {
            if let Some(started) = self.entry_started.take() {
                record_entry_to_dispatch(UNKNOWN_OPERATION, OUTCOMES[outcome], started);
            }
            COUNTERS.record(self.method, self.operation, outcome);
            if let Some(((observer, started_at), operation)) = self.completion.take().zip(S3Operation::ALL.get(self.operation)) {
                observer(*operation, started_at.elapsed(), outcome == 1);
            }
            self.finished = true;
        }
    }
}

fn record_entry_to_dispatch(operation: usize, outcome: &'static str, started: Instant) {
    // The entry gate owns this sample. A later gate change must neither lose an
    // enabled request nor start timing an already-running disabled request.
    histogram!(ENTRY_TO_DISPATCH_METRIC, "op" => operation_label(operation), "outcome" => outcome)
        .record(started.elapsed().as_secs_f64());
}

pub fn install_s3_http_completion_observer(enabled: S3HttpCompletionObserverEnabled, observer: S3HttpCompletionObserver) {
    let _ = COMPLETION_OBSERVER.set((enabled, observer));
}

impl Drop for S3HttpRequestGuard {
    fn drop(&mut self) {
        self.finish(7);
    }
}

pub fn s3_http_metrics_snapshot() -> Vec<S3HttpMetricSnapshot> {
    COUNTERS.snapshot()
}

#[cfg(test)]
mod tests {
    use super::*;
    use metrics::with_local_recorder;
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};

    use crate::tests::METRICS_FLAG_LOCK;
    use std::collections::BTreeMap;

    struct RestoreMetricsGate(bool);

    impl Drop for RestoreMetricsGate {
        fn drop(&mut self) {
            crate::set_metrics_enabled(self.0);
        }
    }

    fn entry_samples(recorder: &DebuggingRecorder) -> BTreeMap<(String, String), Vec<f64>> {
        recorder
            .snapshotter()
            .snapshot()
            .into_vec()
            .into_iter()
            .filter(|(key, _, _, _)| key.key().name() == ENTRY_TO_DISPATCH_METRIC)
            .map(|(key, _, _, value)| {
                let labels: BTreeMap<_, _> = key.key().labels().map(|label| (label.key(), label.value())).collect();
                assert_eq!(labels.keys().copied().collect::<Vec<_>>(), ["op", "outcome"]);
                let DebugValue::Histogram(samples) = value else {
                    panic!("entry-to-dispatch metric must be a histogram");
                };
                (
                    (labels["op"].to_owned(), labels["outcome"].to_owned()),
                    samples.into_iter().map(|sample| sample.0).collect(),
                )
            })
            .collect()
    }

    #[test]
    fn entry_to_dispatch_stops_at_first_typed_dispatch_across_polls() {
        let _lock = METRICS_FLAG_LOCK.lock().expect("metrics gate lock");
        let _restore = RestoreMetricsGate(crate::metrics_enabled());
        let recorder = DebuggingRecorder::new();
        with_local_recorder(&recorder, || {
            crate::set_metrics_enabled(true);
            let mut request = S3HttpRequestGuard::new("PUT");
            let started = Instant::now() - Duration::from_secs(2);
            request.entry_started = Some(started);
            request.in_scope(|| assert_eq!(current_s3_http_operation_context(), None));
            assert_eq!(request.entry_started, Some(started));
            assert!(entry_samples(&recorder).is_empty());
            request.in_scope(|| {
                observe_s3_http_operation(S3Operation::PutObject);
                observe_s3_http_operation(S3Operation::GetObject);
            });
            assert!(request.entry_started.is_none());
            let dispatched = entry_samples(&recorder);
            assert_eq!(dispatched.len(), 1);
            let samples = &dispatched[&("s3:PutObject".to_owned(), "dispatched".to_owned())];
            assert_eq!(samples.len(), 1);
            assert!(samples[0] >= 2.0, "wall time must include pre-dispatch polls");
            request.in_scope(|| observe_s3_http_operation(S3Operation::DeleteObject));
            request.response(200);
            request.service_error();
            drop(request);
            // DebuggingRecorder drains histogram samples on snapshot. The
            // dispatch snapshot above held exactly one sample; completion must
            // not produce another sample after that drain. Empty registered
            // series remain in the snapshot and are not additional samples.
            assert!(
                entry_samples(&recorder).values().all(Vec::is_empty),
                "completion cannot extend or duplicate dispatch timing"
            );
        });
    }

    #[test]
    fn entry_to_dispatch_records_undispatched_response_error_and_drop_once() {
        let _lock = METRICS_FLAG_LOCK.lock().expect("metrics gate lock");
        let _restore = RestoreMetricsGate(crate::metrics_enabled());
        let recorder = DebuggingRecorder::new();
        with_local_recorder(&recorder, || {
            crate::set_metrics_enabled(true);
            let mut rejected = S3HttpRequestGuard::new("GET");
            rejected.in_scope(|| assert!(S3HttpRequestGuard::is_active()));
            rejected.response(403);
            rejected.service_error();
            drop(rejected);
            let mut failed = S3HttpRequestGuard::new("DELETE");
            failed.service_error();
            failed.response(500);
            drop(failed);
            drop(S3HttpRequestGuard::new("POST"));
        });
        let samples = entry_samples(&recorder);
        assert_eq!(samples.len(), 3);
        for outcome in ["4xx", "service_error", "cancelled"] {
            let row = &samples[&("unknown".to_owned(), outcome.to_owned())];
            assert_eq!(row.len(), 1, "{outcome} must be terminal exactly once");
            assert!(row[0].is_finite() && row[0] >= 0.0);
        }
    }

    #[test]
    fn entry_to_dispatch_captures_gate_at_entry() {
        let _lock = METRICS_FLAG_LOCK.lock().expect("metrics gate lock");
        let _restore = RestoreMetricsGate(crate::metrics_enabled());
        let recorder = DebuggingRecorder::new();
        with_local_recorder(&recorder, || {
            crate::set_metrics_enabled(false);
            let mut disabled = S3HttpRequestGuard::new("GET");
            assert!(disabled.entry_started.is_none(), "disabled entry must not read the clock");
            crate::set_metrics_enabled(true);
            disabled.in_scope(|| observe_s3_http_operation(S3Operation::GetObject));
            disabled.response(200);
            assert!(entry_samples(&recorder).is_empty(), "enabling mid-request cannot create a partial sample");
            let mut dispatched = S3HttpRequestGuard::new("PUT");
            let mut undispatched = S3HttpRequestGuard::new("DELETE");
            crate::set_metrics_enabled(false);
            dispatched.in_scope(|| observe_s3_http_operation(S3Operation::PutObject));
            dispatched.response(204);
            undispatched.response(503);
        });
        let samples = entry_samples(&recorder);
        assert_eq!(samples.len(), 2, "disabling mid-request must preserve both enabled entry samples");
        assert_eq!(samples[&("s3:PutObject".to_owned(), "dispatched".to_owned())].len(), 1);
        assert_eq!(samples[&("unknown".to_owned(), "5xx".to_owned())].len(), 1);
    }

    #[test]
    fn entry_to_dispatch_nested_disabled_request_does_not_consume_outer_timer() {
        let _lock = METRICS_FLAG_LOCK.lock().expect("metrics gate lock");
        let _restore = RestoreMetricsGate(crate::metrics_enabled());
        let recorder = DebuggingRecorder::new();
        with_local_recorder(&recorder, || {
            crate::set_metrics_enabled(true);
            let mut outer = S3HttpRequestGuard::new("PUT");
            let outer_started = outer.entry_started;
            outer.in_scope(|| {
                crate::set_metrics_enabled(false);
                let mut inner = S3HttpRequestGuard::new("GET");
                inner.in_scope(|| observe_s3_http_operation(S3Operation::GetObject));
                inner.response(200);
                let current = CURRENT_OPERATION.with(Cell::get);
                assert_eq!(current.operation, UNKNOWN_OPERATION);
                assert_eq!(current.entry_started, outer_started);
                observe_s3_http_operation(S3Operation::PutObject);
            });
            outer.response(204);
        });
        let samples = entry_samples(&recorder);
        assert_eq!(samples.len(), 1);
        assert_eq!(samples[&("s3:PutObject".to_owned(), "dispatched".to_owned())].len(), 1);
    }

    #[test]
    fn entry_to_dispatch_nested_unwind_consumes_each_timer_once() {
        let _lock = METRICS_FLAG_LOCK.lock().expect("metrics gate lock");
        let _restore = RestoreMetricsGate(crate::metrics_enabled());
        let recorder = DebuggingRecorder::new();
        with_local_recorder(&recorder, || {
            crate::set_metrics_enabled(true);
            let mut outer = S3HttpRequestGuard::new("PUT");
            outer.in_scope(|| {
                let failed = std::panic::catch_unwind(|| {
                    let mut inner = S3HttpRequestGuard::new("GET");
                    inner.in_scope(|| {
                        observe_s3_http_operation(S3Operation::GetObject);
                        panic!("dispatch then unwind");
                    });
                });
                assert!(failed.is_err());
                assert_eq!(current_s3_http_operation_context(), None, "outer operation must be restored");
                assert!(CURRENT_OPERATION.with(Cell::get).entry_started.is_some());
                observe_s3_http_operation(S3Operation::PutObject);
            });
            outer.response(200);
            assert!(!S3HttpRequestGuard::is_active());
        });
        let samples = entry_samples(&recorder);
        assert_eq!(samples.len(), 2, "unwinding cannot emit a second unknown/cancelled sample");
        for op in ["s3:GetObject", "s3:PutObject"] {
            assert_eq!(samples[&(op.to_owned(), "dispatched".to_owned())].len(), 1);
        }
    }

    #[test]
    fn entry_to_dispatch_child_context_carries_no_parent_timer() {
        let _lock = METRICS_FLAG_LOCK.lock().expect("metrics gate lock");
        let _restore = RestoreMetricsGate(crate::metrics_enabled());
        let recorder = DebuggingRecorder::new();
        with_local_recorder(&recorder, || {
            crate::set_metrics_enabled(true);
            let mut origin = S3HttpRequestGuard::new("GET");
            let context = origin.in_scope(|| {
                observe_s3_http_operation(S3Operation::GetObject);
                current_s3_http_operation_context().expect("typed origin operation")
            });
            origin.response(200);
            let mut outer = S3HttpRequestGuard::new("PUT");
            outer.in_scope(|| {
                let outer_started = CURRENT_OPERATION.with(Cell::get).entry_started;
                let mut child = std::pin::pin!(with_s3_http_operation_context(Some(context), async {
                    assert_eq!(current_s3_http_operation_name(), Some("s3:GetObject"));
                    assert!(CURRENT_OPERATION.with(Cell::get).entry_started.is_none());
                    observe_s3_http_operation(S3Operation::DeleteObject);
                }));
                assert!(
                    child
                        .as_mut()
                        .poll(&mut std::task::Context::from_waker(std::task::Waker::noop()))
                        .is_ready()
                );
                assert_eq!(CURRENT_OPERATION.with(Cell::get).entry_started, outer_started);
                observe_s3_http_operation(S3Operation::PutObject);
            });
            outer.response(200);
        });
        let samples = entry_samples(&recorder);
        assert_eq!(samples.len(), 2);
        assert_eq!(samples[&("s3:GetObject".to_owned(), "dispatched".to_owned())].len(), 1);
        assert_eq!(samples[&("s3:PutObject".to_owned(), "dispatched".to_owned())].len(), 1);
    }

    #[cfg(feature = "diagnostic-read-version-caller")]
    #[test]
    fn entry_to_dispatch_absent_operation_scope_preserves_ambient_timer_across_polls() {
        let _lock = METRICS_FLAG_LOCK.lock().expect("metrics gate lock");
        let _restore = RestoreMetricsGate(crate::metrics_enabled());
        let recorder = DebuggingRecorder::new();
        with_local_recorder(&recorder, || {
            crate::set_metrics_enabled(true);
            let mut request = S3HttpRequestGuard::new("PUT");
            let started = request.entry_started;
            let mut polls = 0;
            let mut isolated = Box::pin(with_isolated_s3_http_operation_context(
                None,
                std::future::poll_fn(|_| {
                    polls += 1;
                    assert_eq!(current_s3_http_operation_context(), None);
                    assert!(S3HttpRequestGuard::is_active());
                    let ambient = CURRENT_OPERATION.with(Cell::get);
                    assert_eq!(ambient.operation, UNKNOWN_OPERATION);
                    assert_eq!(ambient.entry_started, started);
                    if polls == 1 {
                        std::task::Poll::Pending
                    } else {
                        std::task::Poll::Ready(17)
                    }
                }),
            ));
            let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
            request.in_scope(|| assert!(isolated.as_mut().poll(&mut cx).is_pending()));
            assert_eq!(request.entry_started, started);
            assert!(entry_samples(&recorder).is_empty());
            request.in_scope(|| assert_eq!(isolated.as_mut().poll(&mut cx), std::task::Poll::Ready(17)));
            drop(isolated);
            assert_eq!(polls, 2);
            assert_eq!(request.entry_started, started);
            assert!(
                ISOLATED_OPERATION.try_with(Cell::get).is_err(),
                "absence scope must restore its task local"
            );
            assert!(
                entry_samples(&recorder).is_empty(),
                "the absence scope itself cannot consume the ambient timer"
            );
            request.in_scope(|| {
                observe_s3_http_operation(S3Operation::PutObject);
                observe_s3_http_operation(S3Operation::GetObject);
            });
            request.response(200);
        });
        let samples = entry_samples(&recorder);
        assert_eq!(samples.len(), 1);
        assert_eq!(samples[&("s3:PutObject".to_owned(), "dispatched".to_owned())].len(), 1);
    }

    #[test]
    fn completion_observer_sees_each_dispatched_request_once() {
        static SEEN: std::sync::Mutex<Vec<(S3Operation, bool)>> = std::sync::Mutex::new(Vec::new());
        fn observe(operation: S3Operation, _duration: Duration, succeeded: bool) {
            SEEN.lock().expect("observer log").push((operation, succeeded));
        }

        let mut ok = S3HttpRequestGuard::new("GET").with_completion_observer(observe);
        ok.in_scope(|| observe_s3_http_operation(S3Operation::GetObject));
        ok.response(200);
        drop(ok);

        let mut failed = S3HttpRequestGuard::new("PUT").with_completion_observer(observe);
        failed.in_scope(|| observe_s3_http_operation(S3Operation::PutObject));
        failed.response(503);

        // Rejected before S3 dispatch: counted, but there is no operation to report.
        let mut undispatched = S3HttpRequestGuard::new("GET").with_completion_observer(observe);
        undispatched.service_error();

        assert_eq!(
            *SEEN.lock().expect("observer log"),
            [(S3Operation::GetObject, true), (S3Operation::PutObject, false)]
        );
    }

    #[tokio::test]
    async fn captured_operation_context_can_be_reapplied_to_child_task() {
        let mut request = S3HttpRequestGuard::new("GET");
        let operation = request
            .in_scope(|| {
                observe_s3_http_operation(S3Operation::GetObject);
                current_s3_http_operation_context()
            })
            .expect("dispatched S3 operation should be captured");

        let child_operation = tokio::spawn(with_s3_http_operation_context(Some(operation), async {
            current_s3_http_operation_name()
        }))
        .await
        .expect("child task should finish");

        assert_eq!(child_operation, Some("s3:GetObject"));
    }

    #[test]
    fn observing_dispatch_does_not_count_a_second_handler_entry() {
        let _lock = METRICS_FLAG_LOCK.lock().expect("metrics gate lock");
        let _restore = RestoreMetricsGate(crate::metrics_enabled());
        crate::set_metrics_enabled(false);
        let recorder = DebuggingRecorder::new();
        let mut request = S3HttpRequestGuard::new("PUT");
        with_local_recorder(&recorder, || {
            request.in_scope(|| {
                observe_s3_http_operation(S3Operation::CreateBucket);
                observe_s3_http_operation(S3Operation::DeleteBucket);
                assert_eq!(current_s3_http_operation_name(), Some("s3:CreateBucket"));
            });
        });
        assert!(recorder.snapshotter().snapshot().into_vec().is_empty());
    }

    #[test]
    fn outcome_counters_distinguish_partial_and_complete_write_failure() {
        let counters = HttpOutcomeCounters::new();
        let recorder = DebuggingRecorder::new();
        with_local_recorder(&recorder, || {
            for _ in 0..99 {
                counters.record(1, S3Operation::PutObject.metric_index(), 1);
            }
            counters.record(1, S3Operation::PutObject.metric_index(), 4);
            for _ in 0..100 {
                counters.record(1, UNKNOWN_OPERATION, 4);
            }
        });
        let snapshot = counters.snapshot();
        assert_eq!(snapshot.iter().map(|series| series.total).sum::<u64>(), 200);
        assert_eq!(
            snapshot
                .iter()
                .find(|s| s.operation == S3Operation::PutObject.as_str() && s.outcome == "5xx")
                .expect("write failure")
                .total,
            1
        );
        assert_eq!(
            snapshot
                .iter()
                .find(|s| s.operation == "unknown")
                .expect("pre-dispatch failures")
                .total,
            100
        );
        let exported = recorder.snapshotter().snapshot().into_vec();
        assert_eq!(exported.len(), 3);
        for (key, _, _, _) in exported {
            let labels: Vec<_> = key.key().labels().map(|label| label.key()).collect();
            assert_eq!(labels, ["method", "op", "outcome"]);
        }
    }

    #[cfg(feature = "diagnostic-read-version-caller")]
    #[tokio::test]
    async fn isolated_absent_operation_does_not_inherit_the_flush_request() {
        let mut request = S3HttpRequestGuard::new("GET");
        let operation = request.in_scope(|| {
            observe_s3_http_operation(S3Operation::GetObject);
            current_s3_http_operation_context().expect("GetObject operation should be captured")
        });
        with_s3_http_operation_context(Some(operation), async {
            assert!(S3HttpRequestGuard::is_active());
            with_isolated_s3_http_operation_context(None, async {
                assert_eq!(current_s3_http_operation_context(), None);
                assert!(
                    S3HttpRequestGuard::is_active(),
                    "isolating an item must leave ambient HTTP accounting intact"
                );
                tokio::task::yield_now().await;
                assert_eq!(current_s3_http_operation_context(), None);
            })
            .await;
            assert_eq!(current_s3_http_operation_context(), Some(operation));
        })
        .await;
        assert_eq!(current_s3_http_operation_context(), None);
        with_isolated_s3_http_operation_context(None, async {
            assert!(
                !S3HttpRequestGuard::is_active(),
                "absent operation must not fabricate an HTTP request guard"
            );
        })
        .await;
    }

    #[test]
    fn request_guard_preserves_operation_across_polls_and_finishes_once() {
        let totals = || {
            s3_http_metrics_snapshot()
                .into_iter()
                .filter(|series| series.method == "CONNECT")
                .map(|series| ((series.operation, series.outcome), series.total))
                .collect::<std::collections::BTreeMap<_, _>>()
        };
        let before = totals();
        let mut request = S3HttpRequestGuard::new("CONNECT");
        request.in_scope(|| observe_s3_http_operation(S3Operation::PutObject));
        request.in_scope(|| {
            assert!(S3HttpRequestGuard::is_active());
            observe_s3_http_operation(S3Operation::GetObject);
        });
        assert!(!S3HttpRequestGuard::is_active());
        request.response(204);
        request.response(503);
        request.service_error();
        drop(request);
        let after = totals();
        let key = (S3Operation::PutObject.as_str(), "2xx");
        assert_eq!(after[&key] - before.get(&key).copied().unwrap_or_default(), 1);
        assert_eq!(after.values().sum::<u64>() - before.values().sum::<u64>(), 1);
    }

    #[test]
    fn request_operation_is_scoped_and_first_dispatch_wins() {
        let mut request = S3HttpRequestGuard::new("PUT");
        request.in_scope(|| {
            observe_s3_http_operation(S3Operation::PutObject);
            observe_s3_http_operation(S3Operation::GetObject);
        });
        assert_eq!(request.operation, S3Operation::PutObject.metric_index());
        observe_s3_http_operation(S3Operation::GetObject);
        let other = S3HttpRequestGuard::new("attacker-controlled-method");
        assert_eq!(other.method, METHODS.len() - 1);
        assert_eq!(other.operation, UNKNOWN_OPERATION);
    }

    #[test]
    fn lock_failure_metric_uses_bounded_request_labels() {
        let recorder = DebuggingRecorder::new();
        with_local_recorder(&recorder, || {
            let mut request = S3HttpRequestGuard::new("PUT");
            request.in_scope(|| {
                observe_s3_http_operation(S3Operation::PutObject);
                record_s3_http_lock_failure("system_metadata", "write", "timeout");
            });
            record_s3_http_lock_failure("object_namespace", "read", "quorum");
        });

        let exported = recorder
            .snapshotter()
            .snapshot()
            .into_vec()
            .into_iter()
            .filter(|(key, _, _, _)| key.key().name() == LOCK_FAILURE_METRIC)
            .collect::<Vec<_>>();
        assert_eq!(exported.len(), 1, "background lock errors must not be attributed to an S3 request");
        let (key, _, _, value) = &exported[0];
        assert!(matches!(value, DebugValue::Counter(1)));
        let labels = key
            .key()
            .labels()
            .map(|label| (label.key(), label.value()))
            .collect::<std::collections::BTreeMap<_, _>>();
        assert_eq!(
            labels,
            std::collections::BTreeMap::from([
                ("mode", "write"),
                ("op", S3Operation::PutObject.as_str()),
                ("reason", "timeout"),
                ("resource_class", "system_metadata"),
            ])
        );
    }
}
