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

use rustfs_s3_ops::S3Operation;
use std::cell::Cell;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{LazyLock, OnceLock};
use std::time::{Duration, Instant};

/// Receives a finished external request's first dispatched operation, its
/// latency, and whether it produced a 2xx response. The server injects this
/// so the leaf metrics crate never depends on a trace-bus implementation.
pub type S3HttpCompletionObserver = fn(S3Operation, Duration, bool);
pub type S3HttpCompletionObserverEnabled = fn() -> bool;

const METRIC: &str = "rustfs_s3_http_requests_total";
const METHODS: [&str; 10] = [
    "GET", "PUT", "POST", "DELETE", "HEAD", "OPTIONS", "PATCH", "CONNECT", "TRACE", "OTHER",
];
const OUTCOMES: [&str; 8] = ["1xx", "2xx", "3xx", "4xx", "5xx", "unknown", "service_error", "cancelled"];
const UNKNOWN_OPERATION: usize = S3Operation::ALL.len();
static COUNTERS: LazyLock<HttpOutcomeCounters> = LazyLock::new(HttpOutcomeCounters::new);
static COMPLETION_OBSERVER: OnceLock<(S3HttpCompletionObserverEnabled, S3HttpCompletionObserver)> = OnceLock::new();

tokio::task_local! {
    static CURRENT_OPERATION: Cell<usize>;
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

pub(crate) fn observe_s3_http_operation(op: S3Operation) {
    let _ = CURRENT_OPERATION.try_with(|current| {
        // Internal operations must not overwrite the external request's first
        // dispatched operation. No task-local scope means non-HTTP work.
        if current.get() == UNKNOWN_OPERATION {
            current.set(op.metric_index());
        }
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
}

impl S3HttpRequestGuard {
    pub fn is_active() -> bool {
        CURRENT_OPERATION.try_with(|_| ()).is_ok()
    }

    pub fn new(method: &str) -> Self {
        Self {
            method: METHODS.iter().position(|known| *known == method).unwrap_or(METHODS.len() - 1),
            operation: UNKNOWN_OPERATION,
            completion: COMPLETION_OBSERVER
                .get()
                .and_then(|(enabled, observer)| enabled().then_some((*observer, Instant::now()))),
            finished: false,
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
        CURRENT_OPERATION.sync_scope(Cell::new(self.operation), || {
            let result = f();
            self.operation = CURRENT_OPERATION.with(Cell::get);
            result
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
            COUNTERS.record(self.method, self.operation, outcome);
            if let Some(((observer, started_at), operation)) = self.completion.take().zip(S3Operation::ALL.get(self.operation)) {
                observer(*operation, started_at.elapsed(), outcome == 1);
            }
            self.finished = true;
        }
    }
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
    use metrics_util::debugging::DebuggingRecorder;

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
}
