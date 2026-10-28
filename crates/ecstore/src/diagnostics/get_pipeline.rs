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

//! Bounded, request-coherent diagnostics for a separate build. Overlapping
//! stage totals are work sums, not an additive request critical path.

use super::batch_read_diagnostics::{cpu_ns, parse_rate, thread_id, valid_request_id};
use serde::Serialize;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex, OnceLock};
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use tracing::Dispatch;

const TARGET: &str = rustfs_config::LOCAL_BATCH_DIAGNOSTIC_LOG_TARGET;
const EVENT_GET_PIPELINE_DIAGNOSTIC: &str = "get_pipeline_diagnostic";
const LOG_COMPONENT_ECSTORE: &str = "ecstore";
const LOG_SUBSYSTEM_GET_PIPELINE: &str = "get_pipeline_diagnostic";
const ENV_SAMPLE_EVERY: &str = "RUSTFS_DIAGNOSTIC_GET_STAGE_SAMPLE_EVERY";
const MAX_REQUESTS: u64 = 4096;
// Each fixed-schema event reserves 8 KiB, including its bounded ASCII ID.
// Reserve the final summary before accepting any batch events. Exhaustion
// censors further batch events and remains visible in that summary.
const MAX_EVENT_BYTES: u64 = 8192;
const MAX_EVENTS: u64 = (1024 * 1024 * 1024) / MAX_EVENT_BYTES;
static RATE: OnceLock<u64> = OnceLock::new();
static BUDGET: LazyLock<Arc<Budget>> = LazyLock::new(|| Arc::new(Budget::default()));

tokio::task_local! {
    static REQUEST: Option<Arc<RequestSample>>;
}

#[derive(Default)]
struct Budget {
    sequence: AtomicU64,
    requests: AtomicU64,
    events: AtomicU64,
}

fn reserve(counter: &AtomicU64, limit: u64) -> bool {
    counter
        .try_update(Ordering::Relaxed, Ordering::Relaxed, |n| (n < limit).then(|| n + 1))
        .is_ok()
}

pub(crate) fn mode_enabled() -> bool {
    *RATE.get_or_init(|| parse_rate(std::env::var(ENV_SAMPLE_EVERY).ok().as_deref())) != 0
}

/// Capture once at the external canonical-ID boundary, never from a client header.
pub fn capture_request(id: &str) -> Option<Arc<RequestSample>> {
    let rate = *RATE.get_or_init(|| parse_rate(std::env::var(ENV_SAMPLE_EVERY).ok().as_deref()));
    if rate == 0 || !tracing::enabled!(target: TARGET, tracing::Level::TRACE) {
        return None;
    }
    capture(id, rate, true, &BUDGET)
}

fn capture(id: &str, rate: u64, enabled: bool, budget: &Arc<Budget>) -> Option<Arc<RequestSample>> {
    if rate == 0 || !enabled || !valid_request_id(id) {
        return None;
    }
    let request_sequence = budget.sequence.fetch_add(1, Ordering::Relaxed);
    if !request_sequence.is_multiple_of(rate) || !reserve(&budget.requests, MAX_REQUESTS) || !reserve(&budget.events, MAX_EVENTS)
    {
        return None;
    }
    Some(Arc::new(RequestSample {
        id: Arc::from(id),
        request_sequence,
        sample_every: rate,
        start: Instant::now(),
        start_unix_ns: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .ok()
            .and_then(|d| d.as_nanos().try_into().ok()),
        dispatch: tracing::dispatcher::get_default(Clone::clone),
        budget: Arc::clone(budget),
        truncated: AtomicBool::new(false),
        stats: Mutex::new(Stats::default()),
    }))
}

/// Scope only the synchronous operation/poll. Task locals are explicitly restored.
pub fn in_context<T>(sample: Option<&Arc<RequestSample>>, operation: impl FnOnce() -> T) -> T {
    REQUEST.sync_scope(sample.cloned(), operation)
}

pub(crate) fn current() -> Option<Arc<RequestSample>> {
    REQUEST.try_with(Clone::clone).ok().flatten()
}

pub(crate) fn with_captured_context<F: std::future::Future>(future: F) -> impl std::future::Future<Output = F::Output> {
    REQUEST.scope(current(), future)
}

#[derive(Clone, Copy)]
pub(crate) enum Stage {
    Verify,
    Copy,
    Reconstruct,
    Emit,
    ResponsePoll,
}

impl Stage {
    fn index(self) -> usize {
        self as usize
    }
}

#[derive(Clone, Copy, Default, Serialize)]
struct Total {
    calls: u64,
    bytes: u64,
    wall_ns: u64,
    cpu_ns: u64,
    cpu_unavailable: u64,
    failures: u64,
    cancelled: u64,
    first_tid: Option<i32>,
    last_tid: Option<i32>,
    thread_changes: u64,
}

#[derive(Clone, Copy, Default)]
struct Stats {
    totals: [Total; 5],
    terminal: Option<&'static str>,
    response_ready_ns: Option<u64>,
    first_data_poll_end_ns: Option<u64>,
    body_end_ns: Option<u64>,
    overflowed: bool,
    poisoned: bool,
}

/// Owned by the request, producer and selected blocking work. The last owner
/// emits after cancellation too; no mutex is held across await or event output.
pub struct RequestSample {
    id: Arc<str>,
    request_sequence: u64,
    sample_every: u64,
    start: Instant,
    start_unix_ns: Option<u64>,
    dispatch: Dispatch,
    budget: Arc<Budget>,
    truncated: AtomicBool,
    stats: Mutex<Stats>,
}

impl RequestSample {
    pub(crate) fn id(&self) -> Arc<str> {
        Arc::clone(&self.id)
    }

    pub(crate) fn allow_batch_event(&self) -> bool {
        if reserve(&self.budget.events, MAX_EVENTS) {
            return true;
        }
        self.truncated.store(true, Ordering::Relaxed);
        false
    }

    fn update(&self, operation: impl FnOnce(&mut Stats)) {
        let mut stats = self.stats.lock().unwrap_or_else(|poison| {
            let mut stats = poison.into_inner();
            stats.poisoned = true;
            stats
        });
        operation(&mut stats);
    }

    fn record(&self, stage: Stage, bytes: usize, wall: u128, cpu: Option<u64>, outcome: Option<bool>, tid: Option<i32>) {
        self.update(|stats| {
            if matches!(stage, Stage::ResponsePoll) && bytes != 0 && stats.first_data_poll_end_ns.is_none() {
                stats.first_data_poll_end_ns = self.start.elapsed().as_nanos().try_into().ok();
            }
            let total = &mut stats.totals[stage.index()];
            if let Some(tid) = tid {
                if total.last_tid.is_some_and(|previous| previous != tid) {
                    if let Some(changes) = total.thread_changes.checked_add(1) {
                        total.thread_changes = changes;
                    } else {
                        total.thread_changes = u64::MAX;
                        stats.overflowed = true;
                    }
                }
                total.first_tid.get_or_insert(tid);
                total.last_tid = Some(tid);
            }
            let mut add = |destination: &mut u64, value: u128| {
                if let Some(sum) = u64::try_from(value).ok().and_then(|v| destination.checked_add(v)) {
                    *destination = sum;
                } else {
                    *destination = u64::MAX;
                    stats.overflowed = true;
                }
            };
            add(&mut total.calls, 1);
            add(&mut total.bytes, u128::try_from(bytes).unwrap_or(u128::MAX));
            add(&mut total.wall_ns, wall);
            if let Some(cpu) = cpu {
                add(&mut total.cpu_ns, u128::from(cpu));
            } else {
                add(&mut total.cpu_unavailable, 1);
            }
            if outcome == Some(false) {
                add(&mut total.failures, 1);
            }
            if outcome.is_none() {
                add(&mut total.cancelled, 1);
            }
        });
    }

    fn finish_body(&self, outcome: &'static str) {
        self.update(|stats| {
            if stats.terminal.is_none() {
                stats.terminal = Some(outcome);
                stats.body_end_ns = self.start.elapsed().as_nanos().try_into().ok();
            }
        });
    }
}

impl Drop for RequestSample {
    fn drop(&mut self) {
        let stats = *self.stats.get_mut().unwrap_or_else(|p| {
            let s = p.into_inner();
            s.poisoned = true;
            s
        });
        let Ok(totals) = serde_json::to_string(&stats.totals) else {
            return;
        };
        tracing::dispatcher::with_default(&self.dispatch, || {
            tracing::trace!(target: TARGET, parent: None,
                event = EVENT_GET_PIPELINE_DIAGNOSTIC, component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_GET_PIPELINE, result = stats.terminal.unwrap_or("cancelled"),
                request_id = self.id.as_ref(), request_sequence = self.request_sequence, sample_every = self.sample_every,
                pid = std::process::id(), emitter_tid = thread_id(),
                start_unix_ns = self.start_unix_ns,
                lifetime_wall_ns = u64::try_from(self.start.elapsed().as_nanos()).ok(),
                response_ready_ns = stats.response_ready_ns, first_data_poll_end_ns = stats.first_data_poll_end_ns,
                body_end_ns = stats.body_end_ns,
                stages = "verify,copy,reconstruct,emit,response_poll", stage_totals_json = totals,
                truncated = self.truncated.load(Ordering::Relaxed), overflowed = stats.overflowed,
                poisoned = stats.poisoned,
                "Sampled GET pipeline totals");
        });
    }
}

fn cpu_delta(before: Option<(i32, u64)>, after: Option<(i32, u64)>) -> Option<u64> {
    let ((before_tid, before_cpu), (after_tid, after_cpu)) = before.zip(after)?;
    (before_tid == after_tid).then(|| after_cpu.checked_sub(before_cpu)).flatten()
}

fn cpu_snapshot() -> Option<(i32, u64)> {
    thread_id().zip(cpu_ns())
}

// Private synchronous guard: callers cannot carry thread CPU state across await.
struct SyncMeasurement {
    sample: Arc<RequestSample>,
    stage: Stage,
    bytes: usize,
    start: Instant,
    cpu: Option<(i32, u64)>,
    outcome: Option<bool>,
}

impl Drop for SyncMeasurement {
    fn drop(&mut self) {
        let cpu = cpu_delta(self.cpu, cpu_snapshot());
        self.sample
            .record(self.stage, self.bytes, self.start.elapsed().as_nanos(), cpu, self.outcome, thread_id());
    }
}

pub(crate) fn measure_sync<T>(stage: Stage, bytes: usize, operation: impl FnOnce() -> T, success: impl FnOnce(&T) -> bool) -> T {
    let Some(sample) = current() else {
        return operation();
    };
    let mut guard = SyncMeasurement {
        sample,
        stage,
        bytes,
        start: Instant::now(),
        cpu: cpu_snapshot(),
        outcome: None,
    };
    let result = operation();
    guard.outcome = Some(success(&result));
    result
}

/// Async scopes intentionally carry no CPU snapshot. Drop records cancellation.
pub(crate) struct ElapsedMeasurement {
    sample: Arc<RequestSample>,
    stage: Stage,
    start: Instant,
    outcome: Option<bool>,
    bytes: usize,
}

impl ElapsedMeasurement {
    pub(crate) fn emit() -> Option<Self> {
        current().map(|sample| Self {
            sample,
            stage: Stage::Emit,
            start: Instant::now(),
            outcome: None,
            bytes: 0,
        })
    }
    pub(crate) fn finish(mut self, success: bool, bytes: usize) {
        self.outcome = Some(success);
        self.bytes = bytes;
    }
}

impl Drop for ElapsedMeasurement {
    fn drop(&mut self) {
        self.sample
            .record(self.stage, self.bytes, self.start.elapsed().as_nanos(), None, self.outcome, None);
    }
}

/// Complete once at server body EOF/error; this is not a client/network ACK.
pub fn finish_response(sample: Option<&Arc<RequestSample>>, success: bool) {
    if let Some(sample) = sample {
        sample.finish_body(if success { "body_complete" } else { "body_error" });
    }
}

/// Mark the server service's response-ready boundary, not a network send.
pub fn response_ready(sample: Option<&Arc<RequestSample>>) {
    if let Some(sample) = sample {
        sample.update(|stats| {
            if stats.response_ready_ns.is_none() {
                stats.response_ready_ns = sample.start.elapsed().as_nanos().try_into().ok();
            }
        });
    }
}

/// The request/body owner calls this on drop, before any surviving producer drains.
pub fn cancel_response(sample: Option<&Arc<RequestSample>>) {
    if let Some(sample) = sample {
        sample.finish_body("cancelled");
    }
}

pub fn fail_service(sample: Option<&Arc<RequestSample>>) {
    if let Some(sample) = sample {
        sample.finish_body("service_error");
    }
}

/// Observe exactly one synchronous body poll, preserving its return value.
pub fn response_poll<T>(sample: Option<&Arc<RequestSample>>, operation: impl FnOnce() -> (T, usize, bool)) -> T {
    in_context(sample, || {
        let Some(sample) = current() else {
            return operation().0;
        };
        let mut guard = SyncMeasurement {
            sample,
            stage: Stage::ResponsePoll,
            bytes: 0,
            start: Instant::now(),
            cpu: cpu_snapshot(),
            outcome: None,
        };
        let (result, bytes, success) = operation();
        guard.bytes = bytes;
        guard.outcome = Some(success);
        result
    })
}

#[cfg(test)]
pub(crate) fn test_request() -> Arc<RequestSample> {
    capture("canonical-stage-test", 1, true, &Arc::new(Budget::default())).expect("select test request")
}

#[cfg(test)]
pub(crate) fn test_total(sample: &RequestSample, stage: Stage) -> (u64, u64, u64, u64, u64, u64) {
    let total = sample.stats.lock().expect("snapshot diagnostic total").totals[stage.index()];
    (
        total.calls,
        total.bytes,
        total.cpu_ns,
        total.cpu_unavailable,
        total.failures,
        total.cancelled,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Value, json};
    use std::collections::BTreeMap;
    use tracing::field::{Field, Visit};
    use tracing_subscriber::{Layer, layer::Context, prelude::*};

    #[derive(Default)]
    struct Values(BTreeMap<String, Value>);
    impl Visit for Values {
        fn record_u64(&mut self, f: &Field, v: u64) {
            self.0.insert(f.name().into(), json!(v));
        }
        fn record_bool(&mut self, f: &Field, v: bool) {
            self.0.insert(f.name().into(), json!(v));
        }
        fn record_str(&mut self, f: &Field, v: &str) {
            self.0.insert(f.name().into(), json!(v));
        }
        fn record_debug(&mut self, f: &Field, v: &dyn std::fmt::Debug) {
            self.0.insert(f.name().into(), json!(format!("{v:?}")));
        }
    }
    type Records = Arc<Mutex<Vec<BTreeMap<String, Value>>>>;
    struct Recorder(Records);
    impl<S: tracing::Subscriber> Layer<S> for Recorder {
        fn on_event(&self, event: &tracing::Event<'_>, _: Context<'_, S>) {
            if event.metadata().target() != TARGET {
                return;
            }
            assert!(event.is_root(), "diagnostic must never inherit URI-bearing spans");
            let mut fields = Values::default();
            event.record(&mut fields);
            self.0.lock().expect("record bounded event").push(fields.0);
        }
    }
    fn sample() -> Arc<RequestSample> {
        capture("canonical-123", 1, true, &Arc::new(Budget::default())).expect("select canonical request")
    }
    fn stats(sample: &RequestSample) -> Stats {
        *sample.stats.lock().expect("snapshot stage totals")
    }

    #[test]
    fn selection_is_disabled_filtered_validated_and_bounded() {
        let budget = Arc::new(Budget::default());
        assert!(capture("canonical", 0, true, &budget).is_none());
        assert!(capture("canonical", 64, false, &budget).is_none());
        for id in ["", "invalid/id", "invalid\nid", &"a".repeat(129)] {
            assert!(capture(id, 1, true, &budget).is_none());
        }
        assert_eq!(budget.sequence.load(Ordering::Relaxed), 0);
        let selected: Vec<_> = (0..130)
            .filter(|_| capture("canonical", 64, true, &budget).is_some())
            .collect();
        assert_eq!(selected, [0, 64, 128]);
        budget.requests.store(MAX_REQUESTS, Ordering::Relaxed);
        assert!(capture("canonical", 1, true, &budget).is_none());
        budget.requests.store(0, Ordering::Relaxed);
        budget.events.store(MAX_EVENTS, Ordering::Relaxed);
        assert!(capture("canonical", 1, true, &budget).is_none());
    }

    #[test]
    fn sync_errors_and_unwinds_preserve_behavior_without_payloads() {
        let sample = sample();
        let result: Result<(), _> = in_context(Some(&sample), || {
            measure_sync(Stage::Verify, 123, || Err("private-error-payload"), Result::is_ok)
        });
        assert_eq!(result, Err("private-error-payload"));
        let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            in_context(Some(&sample), || measure_sync(Stage::Copy, 9, || panic!("original panic"), |_: &()| true));
        }));
        assert!(panic.is_err(), "diagnostics must not swallow panics");
        let totals = stats(&sample).totals;
        assert_eq!(totals[Stage::Verify.index()].failures, 1);
        assert_eq!(totals[Stage::Verify.index()].bytes, 123);
        assert_eq!(totals[Stage::Copy.index()].cancelled, 1);
        assert!(current().is_none(), "task-local must restore after unwind");
    }

    #[test]
    fn cpu_requires_same_thread_available_monotonic_snapshots() {
        assert_eq!(cpu_delta(Some((1, 100)), Some((1, 145))), Some(45));
        assert_eq!(cpu_delta(Some((1, 100)), Some((2, 145))), None);
        assert_eq!(cpu_delta(Some((1, 100)), Some((1, 99))), None);
        assert_eq!(cpu_delta(None, Some((1, 100))), None);
        assert_eq!(cpu_delta(Some((1, 100)), None), None);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn captured_context_survives_spawn_and_pending_without_leaking() {
        let sample = sample();
        let cloned = Arc::clone(&sample);
        let work = in_context(Some(&sample), || {
            with_captured_context(async move {
                assert!(Arc::ptr_eq(&current().expect("producer sample"), &cloned));
                tokio::task::yield_now().await;
                assert!(Arc::ptr_eq(&current().expect("resumed producer sample"), &cloned));
                measure_sync(Stage::Copy, 17, || 42, |_| true)
            })
        });
        assert_eq!(tokio::spawn(work).await.expect("join producer"), 42);
        assert!(current().is_none());
        assert_eq!(stats(&sample).totals[Stage::Copy.index()].bytes, 17);
    }

    #[test]
    fn elapsed_scope_can_move_threads_and_has_no_cpu_delta() {
        let sample = sample();
        let guard = in_context(Some(&sample), ElapsedMeasurement::emit).expect("sample emit");
        std::thread::spawn(move || guard.finish(true, 81))
            .join()
            .expect("finish on different thread");
        let cancelled = in_context(Some(&sample), ElapsedMeasurement::emit).expect("sample cancelled emit");
        drop(cancelled);
        let total = stats(&sample).totals[Stage::Emit.index()];
        assert_eq!(
            (total.calls, total.bytes, total.cpu_ns, total.cpu_unavailable, total.cancelled),
            (2, 81, 0, 2, 1)
        );
    }

    #[test]
    fn summary_waits_for_producer_and_emits_once_with_reserved_budget() {
        let records = Records::default();
        let subscriber = tracing_subscriber::registry().with(Recorder(Arc::clone(&records)));
        let producer = tracing::subscriber::with_default(subscriber, || {
            let span = tracing::info_span!("untrusted-request", uri = "/private?signature=secret");
            let _entered = span.enter();
            let sample = sample();
            finish_response(Some(&sample), false);
            finish_response(Some(&sample), true);
            sample.budget.events.store(MAX_EVENTS, Ordering::Relaxed);
            assert!(!sample.allow_batch_event());
            sample
        });
        assert!(records.lock().expect("before producer drop").is_empty());
        drop(producer);
        let records = records.lock().expect("last owner emitted");
        assert_eq!(records.len(), 1);
        assert_eq!(records[0]["result"], "body_error");
        assert_eq!(records[0]["truncated"], true);
        assert_eq!(records[0]["request_id"], "canonical-123");
        assert!(
            !serde_json::to_string(&*records)
                .expect("serialize fixed fields")
                .contains("signature")
        );
    }

    #[test]
    fn terminal_states_and_poll_values_are_preserved() {
        let sample = sample();
        response_ready(Some(&sample));
        let result = response_poll(Some(&sample), || (std::task::Poll::<Result<(), &str>>::Pending, 0, true));
        assert_eq!(result, std::task::Poll::Pending);
        let result = response_poll(Some(&sample), || (Err::<(), _>("original"), 512, false));
        assert_eq!(result, Err("original"));
        response_ready(Some(&sample));
        fail_service(Some(&sample));
        finish_response(Some(&sample), true);
        let stats = stats(&sample);
        assert_eq!(stats.terminal, Some("service_error"));
        assert!(stats.response_ready_ns.is_some());
        assert!(stats.body_end_ns.is_some());
        assert_eq!(stats.totals[Stage::ResponsePoll.index()].calls, 2);
        assert_eq!(stats.totals[Stage::ResponsePoll.index()].bytes, 512);
        assert_eq!(stats.totals[Stage::ResponsePoll.index()].failures, 1);
        assert!(current().is_none());
    }

    #[test]
    fn cancelled_owner_boundary_precedes_last_producer_drain() {
        let sample = sample();
        cancel_response(Some(&sample));
        let boundary = stats(&sample).body_end_ns.expect("cancelled body boundary");
        let producer = Arc::clone(&sample);
        in_context(Some(&producer), || measure_sync(Stage::Copy, 7, || (), |_| true));
        finish_response(Some(&sample), true);
        assert_eq!(stats(&sample).terminal, Some("cancelled"));
        assert_eq!(
            stats(&sample).body_end_ns,
            Some(boundary),
            "late work must not overwrite cancellation time"
        );
        assert_eq!(stats(&sample).totals[Stage::Copy.index()].bytes, 7);
    }

    #[test]
    fn overflow_is_censored_and_owned_schema_fits_reserved_bytes() {
        let sample = sample();
        sample.record(Stage::Copy, 0, u128::MAX, Some(u64::MAX), Some(true), Some(1));
        sample.record(Stage::Copy, 1, 1, Some(1), Some(true), Some(2));
        assert!(stats(&sample).overflowed);
        let total = stats(&sample).totals[Stage::Copy.index()];
        assert_eq!((total.first_tid, total.last_tid, total.thread_changes), (Some(1), Some(2), 1));
        let maximum = Total {
            calls: u64::MAX,
            bytes: u64::MAX,
            wall_ns: u64::MAX,
            cpu_ns: u64::MAX,
            cpu_unavailable: u64::MAX,
            failures: u64::MAX,
            cancelled: u64::MAX,
            first_tid: Some(i32::MAX),
            last_tid: Some(i32::MAX),
            thread_changes: u64::MAX,
        };
        let payload = serde_json::to_string(&[maximum; 5]).expect("serialize maximum fixed schema");
        let escaped = serde_json::to_string(&payload).expect("serialize JSON field");
        assert!(escaped.len() + 128 + 2048 < usize::try_from(MAX_EVENT_BYTES).expect("bounded event size"));
    }

    #[tokio::test]
    async fn production_bitrot_copy_follows_successful_verification_only() {
        use crate::erasure::coding::BitrotReader;
        use rustfs_utils::HashAlgorithm;
        let payload = b"verified payload";
        let algo = HashAlgorithm::HighwayHash256S;
        let valid = [algo.hash_encode(payload).as_ref(), payload].concat();
        for scenario in ["valid", "corrupt", "short"] {
            let mut bytes = valid.clone();
            if scenario == "corrupt" {
                bytes[0] ^= 1;
            }
            if scenario == "short" {
                bytes.pop();
            }
            let sample = sample();
            let mut reader =
                BitrotReader::new(std::io::Cursor::new(bytes::Bytes::from(bytes)), payload.len(), algo.clone(), false);
            let mut out = vec![0x23];
            let result =
                in_context(Some(&sample), || with_captured_context(reader.read_appending(&mut out, payload.len()))).await;
            let totals = stats(&sample).totals;
            if scenario == "valid" {
                assert_eq!(result.expect("valid shard"), payload.len());
                assert_eq!(&out[1..], payload);
                assert_eq!(totals[Stage::Verify.index()].calls, 1);
                assert_eq!(totals[Stage::Copy.index()].bytes, u64::try_from(payload.len()).expect("small payload"));
            } else {
                assert!(result.is_err(), "corrupt and short shards must fail");
                assert_eq!(out, [0x23], "unverified bytes must not reach output");
                assert_eq!(totals[Stage::Copy.index()].calls, 0);
                if scenario == "corrupt" {
                    assert_eq!(totals[Stage::Verify.index()].failures, 1);
                }
            }
        }
    }
}

#[cfg(test)]
mod cohort_test {
    use super::*;

    #[test]
    fn production_cohort_uses_one_decision_for_every_batch() {
        const CHILD: &str = "RUSTFS_TEST_PIPELINE_COHORT_CHILD";
        if std::env::var_os(CHILD).is_none() {
            let result = std::process::Command::new(std::env::current_exe().expect("current test executable"))
                .args([
                    "--exact",
                    "diagnostics::get_pipeline::cohort_test::production_cohort_uses_one_decision_for_every_batch",
                    "--nocapture",
                ])
                .env(CHILD, "1")
                .env(ENV_SAMPLE_EVERY, "64")
                .env("RUSTFS_DIAGNOSTIC_LOCAL_BATCH_SAMPLE_EVERY", "65536")
                .status()
                .expect("run isolated cohort contract");
            assert!(result.success(), "request and batch sampling contract failed");
            return;
        }
        let subscriber = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::TRACE)
            .with_writer(std::io::sink)
            .finish();
        tracing::subscriber::with_default(subscriber, || {
            let selected = capture_request("server-cohort-1").expect("first request is selected");
            assert_eq!((selected.request_sequence, selected.sample_every), (0, 64));
            let not_selected = capture_request("server-cohort-2");
            assert!(not_selected.is_none());
            let batches = in_context(Some(&selected), || {
                let context =
                    super::super::batch_read_diagnostics::request_context("server-cohort-1").expect("selected canonical context");
                assert!(Arc::ptr_eq(&context, &selected.id), "selected ID must reuse the request allocation");
                assert!(super::super::batch_read_diagnostics::request_context("different-request").is_none());
                let first = super::super::batch_read_diagnostics::Sample::capture().expect("first selected batch");
                let second =
                    super::super::batch_read_diagnostics::Sample::capture().expect("same request must select its second batch");
                (first, second)
            });
            assert!(in_context(not_selected.as_ref(), super::super::batch_read_diagnostics::Sample::capture).is_none());
            assert!(
                super::super::batch_read_diagnostics::request_context("server-cohort-2").is_none(),
                "unselected request must not retain a diagnostic ID"
            );
            assert!(current().is_none());
            assert!(
                Arc::strong_count(&selected) >= 3,
                "blocking batch work must retain the request until drained"
            );
            drop(batches);
            assert_eq!(Arc::strong_count(&selected), 1);
        });
    }
}
