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

//! Sampled local batch diagnostics for a separate, default-off build.
//!
//! Event fields contain server-owned correlation IDs, sizes and timing, never
//! object/disk paths or I/O error strings. They are not performance acceptance.
//! Linux supplies thread CPU time; other targets report it absent.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use tracing::{Dispatch, Level};

const LOG_COMPONENT_ECSTORE: &str = "ecstore";
const LOG_SUBSYSTEM_LOCAL_BATCH: &str = "local_batch_diagnostic";
const EVENT_LOCAL_BATCH_DIAGNOSTIC: &str = "local_batch_diagnostic";
const TARGET: &str = rustfs_config::LOCAL_BATCH_DIAGNOSTIC_LOG_TARGET;
const ENV_SAMPLE_EVERY: &str = "RUSTFS_DIAGNOSTIC_LOCAL_BATCH_SAMPLE_EVERY";
const MAX_SAMPLE_EVERY: u64 = 65_536;
static SAMPLE_EVERY: OnceLock<u64> = OnceLock::new();
static NEXT_BATCH: AtomicU64 = AtomicU64::new(0);

tokio::task_local! {
    static REQUEST_ID: Option<Arc<str>>;
}

fn parse_rate(value: Option<&str>) -> u64 {
    value
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|rate| (1..=MAX_SAMPLE_EVERY).contains(rate))
        .unwrap_or(0)
}

fn enabled_rate() -> u64 {
    *SAMPLE_EVERY.get_or_init(|| parse_rate(std::env::var(ENV_SAMPLE_EVERY).ok().as_deref()))
}

fn recording_enabled() -> bool {
    enabled_rate() != 0 && tracing::enabled!(target: TARGET, Level::TRACE)
}

fn valid_request_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 128
        && id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
}

/// Retain a bounded server-owned canonical ID only while diagnostics are enabled.
///
/// The external S3 boundary must supply its canonical ID, not a client header.
pub fn request_context(id: &str) -> Option<Arc<str>> {
    (recording_enabled() && valid_request_id(id)).then(|| Arc::from(id))
}

/// Poll a request under its diagnostic ID without retaining request fields.
pub fn in_request_context<T>(id: Option<&Arc<str>>, operation: impl FnOnce() -> T) -> T {
    REQUEST_ID.sync_scope(id.cloned(), operation)
}

/// Return the current ID for explicit diagnostic propagation checks.
pub fn current_request_id() -> Option<Arc<str>> {
    REQUEST_ID.try_with(Clone::clone).ok().flatten()
}

/// Capture before spawning the legacy producer; task locals are not inherited.
pub(crate) fn with_captured_request_context<F: std::future::Future>(future: F) -> impl std::future::Future<Output = F::Output> {
    REQUEST_ID.scope(current_request_id(), future)
}

#[derive(Clone, Copy)]
pub(crate) enum Stage {
    Execution,
    Metadata,
    Open,
    Allocation,
    Read,
}

impl Stage {
    fn as_str(self) -> &'static str {
        match self {
            Self::Execution => "execution",
            Self::Metadata => "metadata",
            Self::Open => "open",
            Self::Allocation => "allocation",
            Self::Read => "read",
        }
    }
}

fn unix_ns() -> Option<u64> {
    SystemTime::now().duration_since(UNIX_EPOCH).ok()?.as_nanos().try_into().ok()
}

#[cfg(target_os = "linux")]
fn cpu_ns() -> Option<u64> {
    use rustix::time::{ClockId, DynamicClockId, clock_gettime_dynamic};
    let time = clock_gettime_dynamic(DynamicClockId::Known(ClockId::ThreadCPUTime)).ok()?;
    let seconds = u64::try_from(time.tv_sec).ok()?;
    let nanos = u64::try_from(time.tv_nsec).ok().filter(|value| *value < 1_000_000_000)?;
    seconds.checked_mul(1_000_000_000)?.checked_add(nanos)
}

#[cfg(not(target_os = "linux"))]
fn cpu_ns() -> Option<u64> {
    None
}

fn thread_id() -> Option<i32> {
    #[cfg(target_os = "linux")]
    {
        thread_local! { static TID: i32 = rustix::thread::gettid().as_raw_nonzero().get(); }
        Some(TID.with(|id| *id))
    }
    #[cfg(not(target_os = "linux"))]
    {
        None
    }
}

pub(crate) struct Started {
    wall: Instant,
    unix_ns: Option<u64>,
    cpu_ns: Option<u64>,
}
struct Timing {
    unix_ns: Option<u64>,
    wall_ns: Option<u64>,
    cpu_ns: Option<u64>,
    cpu_status: &'static str,
}

pub(crate) struct Sample {
    batch_id: u64,
    request_id: Option<Arc<str>>,
    submitted: Instant,
    submitted_unix_ns: Option<u64>,
    dispatch: Dispatch,
}

impl Sample {
    pub(crate) fn capture() -> Option<Self> {
        Self::capture_with(enabled_rate(), tracing::enabled!(target: TARGET, Level::TRACE), &NEXT_BATCH)
    }

    fn capture_with(rate: u64, enabled: bool, sequence: &AtomicU64) -> Option<Self> {
        if rate == 0 || !enabled {
            return None;
        }
        let batch_id = sequence.fetch_add(1, Ordering::Relaxed);
        if !batch_id.is_multiple_of(rate) {
            return None;
        }
        Some(Self {
            batch_id,
            request_id: current_request_id(),
            submitted: Instant::now(),
            submitted_unix_ns: unix_ns(),
            dispatch: tracing::dispatcher::get_default(Clone::clone),
        })
    }

    pub(crate) fn dispatch(&self) -> &Dispatch {
        &self.dispatch
    }

    pub(crate) fn start(&self) -> Started {
        Started {
            wall: Instant::now(),
            unix_ns: unix_ns(),
            cpu_ns: cpu_ns(),
        }
    }

    pub(crate) fn worker_started(&self) {
        self.emit(
            "queue_wait",
            None,
            true,
            Timing {
                unix_ns: self.submitted_unix_ns,
                wall_ns: self.submitted.elapsed().as_nanos().try_into().ok(),
                cpu_ns: None,
                cpu_status: "not_applicable",
            },
        );
    }

    pub(crate) fn finish(&self, stage: Stage, shard: Option<usize>, start: Started, success: bool) {
        let cpu = start
            .cpu_ns
            .zip(cpu_ns())
            .and_then(|(before, after)| after.checked_sub(before));
        self.emit(
            stage.as_str(),
            shard,
            success,
            Timing {
                unix_ns: start.unix_ns,
                wall_ns: start.wall.elapsed().as_nanos().try_into().ok(),
                cpu_ns: cpu,
                cpu_status: if cpu.is_some() { "available" } else { "unavailable" },
            },
        );
    }

    fn emit(&self, stage: &'static str, shard: Option<usize>, success: bool, timing: Timing) {
        tracing::trace!(target: TARGET, parent: None,
            event = EVENT_LOCAL_BATCH_DIAGNOSTIC, component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_LOCAL_BATCH, result = if success { "success" } else { "error" },
            stage, batch_id = self.batch_id, request_id = self.request_id.as_deref(),
            request_context = if self.request_id.is_some() { "present" } else { "missing" },
            pid = std::process::id(), tid = thread_id(), shard_index = shard,
            submitted_unix_ns = self.submitted_unix_ns, start_unix_ns = timing.unix_ns,
            wall_ns = timing.wall_ns, cpu_ns = timing.cpu_ns, cpu_clock_status = timing.cpu_status,
            "Sampled local batch stage");
    }

    pub(crate) fn shard_result(&self, shard: usize, requested: usize, read: usize, success: bool) {
        tracing::trace!(target: TARGET, parent: None,
            event = EVENT_LOCAL_BATCH_DIAGNOSTIC, component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_LOCAL_BATCH, result = if success { "success" } else { "error" },
            stage = "shard_result", batch_id = self.batch_id, request_id = self.request_id.as_deref(),
            request_context = if self.request_id.is_some() { "present" } else { "missing" },
            pid = std::process::id(), tid = thread_id(), submitted_unix_ns = self.submitted_unix_ns,
            shard_index = shard, requested_bytes = requested, read_bytes = read,
            "Sampled local batch result");
    }
}

pub(crate) fn measure<T, E>(
    sample: Option<&Sample>,
    stage: Stage,
    shard: usize,
    operation: impl FnOnce() -> Result<T, E>,
) -> Result<T, E> {
    let Some(sample) = sample else { return operation() };
    let start = sample.start();
    let result = operation();
    sample.finish(stage, Some(shard), start, result.is_ok());
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Value, json};
    use std::collections::BTreeMap;
    use std::sync::{Barrier, Mutex, mpsc};
    use tracing::field::{Field, Visit};
    use tracing_subscriber::{Layer, layer::Context, prelude::*};

    #[derive(Default)]
    struct Values(BTreeMap<String, Value>);
    impl Visit for Values {
        fn record_u64(&mut self, field: &Field, value: u64) {
            self.0.insert(field.name().into(), json!(value));
        }
        fn record_i64(&mut self, field: &Field, value: i64) {
            self.0.insert(field.name().into(), json!(value));
        }
        fn record_bool(&mut self, field: &Field, value: bool) {
            self.0.insert(field.name().into(), json!(value));
        }
        fn record_str(&mut self, field: &Field, value: &str) {
            self.0.insert(field.name().into(), json!(value));
        }
        fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
            self.0.insert(field.name().into(), json!(format!("{value:?}")));
        }
    }

    struct Records {
        events: Mutex<Vec<BTreeMap<String, Value>>>,
        block_queue: std::sync::atomic::AtomicBool,
        queue_started: Mutex<Option<mpsc::Sender<()>>>,
        release: Barrier,
    }

    struct Recorder(Arc<Records>);
    impl<S: tracing::Subscriber> Layer<S> for Recorder {
        fn on_event(&self, event: &tracing::Event<'_>, _: Context<'_, S>) {
            if event.metadata().target() != TARGET {
                return;
            }
            assert!(event.is_root(), "diagnostic events must not inherit URI-bearing spans");
            let mut fields = Values::default();
            event.record(&mut fields);
            let queue = fields.0.get("stage") == Some(&json!("queue_wait"));
            self.0.events.lock().expect("record diagnostic event").push(fields.0);
            if queue && self.0.block_queue.swap(false, Ordering::SeqCst) {
                if let Some(sender) = self.0.queue_started.lock().expect("queue sender").take() {
                    sender.send(()).expect("signal worker start");
                }
                self.0.release.wait();
            }
        }
    }

    fn records() -> Arc<Records> {
        Arc::new(Records {
            events: Mutex::new(Vec::new()),
            block_queue: false.into(),
            queue_started: Mutex::new(None),
            release: Barrier::new(2),
        })
    }

    #[test]
    fn rate_is_default_off_and_bounded() {
        for value in [None, Some("0"), Some("invalid"), Some("-1"), Some("65537")] {
            assert_eq!(parse_rate(value), 0);
        }
        assert_eq!(parse_rate(Some("1")), 1);
        assert_eq!(parse_rate(Some("64")), 64);
        assert_eq!(parse_rate(Some("65536")), MAX_SAMPLE_EVERY);
    }

    #[test]
    fn disabled_or_filtered_capture_does_not_advance_sampling() {
        let sequence = AtomicU64::new(0);
        assert!(Sample::capture_with(0, true, &sequence).is_none());
        assert!(Sample::capture_with(64, false, &sequence).is_none());
        assert_eq!(sequence.load(Ordering::Relaxed), 0);
        let result = measure::<(), _>(None, Stage::Read, 0, || Err("original error"));
        assert_eq!(result, Err("original error"));
    }

    #[test]
    fn sampling_keeps_the_registered_interval() {
        let sequence = AtomicU64::new(0);
        let selected: Vec<_> = (0..12)
            .filter_map(|_| Sample::capture_with(4, true, &sequence).map(|sample| sample.batch_id))
            .collect();
        assert_eq!(selected, [0, 4, 8]);
    }

    #[test]
    fn request_context_is_bounded_and_restored_on_unwind() {
        assert!(valid_request_id("server-owned-uuid_123"));
        assert!(valid_request_id(&"a".repeat(128)));
        for value in ["", "secret/path", "line\nbreak", &"a".repeat(129)] {
            assert!(!valid_request_id(value));
        }
        let outer: Arc<str> = Arc::from("server-outer");
        in_request_context(Some(&outer), || {
            in_request_context(None, || assert!(current_request_id().is_none()));
            assert_eq!(current_request_id().as_deref(), Some("server-outer"));
            let nested: Arc<str> = Arc::from("server-inner");
            let result = std::panic::catch_unwind(|| in_request_context(Some(&nested), || panic!("test unwind")));
            assert!(result.is_err());
            assert_eq!(current_request_id().as_deref(), Some("server-outer"));
        });
        assert!(current_request_id().is_none());
    }

    #[tokio::test]
    async fn captured_request_context_crosses_spawn_without_leaking_to_caller() {
        let original: Arc<str> = Arc::from("server-detached-producer");
        let task = in_request_context(Some(&original), || {
            tokio::spawn(with_captured_request_context(async {
                tokio::task::yield_now().await;
                current_request_id()
            }))
        });
        assert!(current_request_id().is_none());
        assert_eq!(task.await.expect("detached diagnostic context").as_deref(), Some(original.as_ref()));
        assert!(current_request_id().is_none());
    }

    #[test]
    fn failed_partial_operation_preserves_error_and_initialized_byte_count() {
        let _interest = crate::test_tracing::pin_callsite_interest_for_test();
        let state = records();
        let subscriber = tracing_subscriber::registry().with(Recorder(Arc::clone(&state)));
        tracing::subscriber::with_default(subscriber, || {
            let parent = tracing::info_span!("request", uri = "/must-not-copy-parent-path");
            let _entered = parent.enter();
            let sample = Sample::capture_with(1, true, &AtomicU64::new(0)).expect("sampled diagnostic");
            let mut read = 0;
            let result = measure::<(), _>(Some(&sample), Stage::Read, 0, || {
                read = 2;
                Err("partial read failure")
            });
            assert_eq!(result, Err("partial read failure"));
            sample.shard_result(0, 4, read, false);
        });
        let events = state.events.lock().expect("read events");
        assert!(
            events
                .iter()
                .any(|e| e.get("stage") == Some(&json!("read")) && e.get("result") == Some(&json!("error")))
        );
        assert!(
            events
                .iter()
                .any(|e| e.get("read_bytes") == Some(&json!(2)) && e.get("requested_bytes") == Some(&json!(4)))
        );
    }

    #[test]
    fn unavailable_cpu_is_absent_and_not_a_zero_measurement() {
        let _interest = crate::test_tracing::pin_callsite_interest_for_test();
        let state = records();
        let subscriber = tracing_subscriber::registry().with(Recorder(Arc::clone(&state)));
        tracing::subscriber::with_default(subscriber, || {
            let sample = Sample::capture_with(1, true, &AtomicU64::new(0)).expect("diagnostic sample");
            sample.emit(
                "metadata",
                Some(0),
                true,
                Timing {
                    unix_ns: None,
                    wall_ns: Some(1),
                    cpu_ns: None,
                    cpu_status: "unavailable",
                },
            );
            sample.emit(
                "metadata",
                Some(0),
                true,
                Timing {
                    unix_ns: None,
                    wall_ns: Some(1),
                    cpu_ns: Some(0),
                    cpu_status: "available",
                },
            );
        });
        let events = state.events.lock().expect("CPU availability events");
        assert!(!events[0].contains_key("cpu_ns"));
        assert_eq!(events[0]["cpu_clock_status"], "unavailable");
        assert_eq!(events[1]["cpu_ns"], 0);
        assert_eq!(events[1]["cpu_clock_status"], "available");
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn thread_cpu_clock_is_available_without_zero_substitution() {
        let before = cpu_ns().expect("Linux thread CPU clock available");
        for value in 0..10_000 {
            std::hint::black_box(value);
        }
        let after = cpu_ns().expect("Linux thread CPU clock remains available");
        assert!(after >= before);
    }

    #[cfg(unix)]
    #[test]
    fn production_batch_diagnostic_survives_caller_cancellation() {
        const CHILD: &str = "RUSTFS_TEST_BATCH_DIAGNOSTIC_CHILD";
        if std::env::var_os(CHILD).is_none() {
            let status = std::process::Command::new(std::env::current_exe().expect("current test executable"))
                .args([
                    "--exact",
                    "diagnostics::batch_read_diagnostics::tests::production_batch_diagnostic_survives_caller_cancellation",
                    "--nocapture",
                ])
                .env(CHILD, "1")
                .env(ENV_SAMPLE_EVERY, "1")
                .status()
                .expect("run isolated diagnostic test");
            assert!(status.success(), "isolated production diagnostic failed");
            return;
        }
        let _interest = crate::test_tracing::pin_callsite_interest_for_test();
        let state = records();
        let recorder = crate::test_metrics::CapturingRecorder::default();
        rustfs_io_metrics::set_get_stage_metrics_enabled(true);
        let subscriber = tracing_subscriber::registry().with(Recorder(Arc::clone(&state)));
        metrics::with_local_recorder(&recorder, || {
            tracing::subscriber::with_default(subscriber, || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("test runtime");
                runtime.block_on(async {
                    let directory = tempfile::tempdir().expect("diagnostic test directory");
                    let path = directory.path().join("must-not-appear-in-trace");
                    std::fs::write(&path, b"abcdef").expect("write payload");
                    let results = REQUEST_ID
                        .scope(
                            Some(Arc::from("server-original")),
                            crate::disk::local::batch_shard_pread(vec![
                                (path.clone(), 1, 3),
                                (directory.path().join("missing-secret-path"), 0, 3),
                                (path.clone(), 0, 0),
                                (path.clone(), 7, 1),
                            ]),
                        )
                        .await;
                    assert_eq!(results[0].as_ref().expect("positioned read").as_ref(), b"bcd");
                    assert!(results[1].is_err());
                    assert!(results[2].as_ref().expect("zero-length read").is_empty());
                    assert!(results[3].is_err());
                    let counter = "rustfs_io_get_object_local_batch_shards_total";
                    assert_eq!(recorder.counter_value(counter, &[("outcome", "success")]), 2);
                    assert_eq!(recorder.counter_value(counter, &[("outcome", "error")]), 2);
                    assert_eq!(recorder.counter_value("rustfs_io_get_object_local_batch_read_bytes_total", &[]), 3);
                    {
                        let events = state.events.lock().expect("normal events");
                        let indices: Vec<_> = events
                            .iter()
                            .filter(|event| event.get("stage") == Some(&json!("shard_result")))
                            .map(|event| event["shard_index"].as_u64().expect("shard index"))
                            .collect();
                        assert_eq!(indices, [0, 1, 2, 3], "errors and empty reads retain their input index");
                        for (index, result) in [(0, "success"), (1, "error"), (2, "success"), (3, "error")] {
                            assert!(
                                events.iter().any(|event| {
                                    event.get("stage") == Some(&json!("shard_result"))
                                        && event.get("shard_index") == Some(&json!(index))
                                        && event.get("result") == Some(&json!(result))
                                }),
                                "missing diagnostic result for shard {index}"
                            );
                        }
                        assert!(
                            !events
                                .iter()
                                .any(|e| e.get("stage") == Some(&json!("read")) && e.get("shard_index") == Some(&json!(2)))
                        );
                        assert!(!format!("{events:?}").contains("must-not-appear"));
                        assert!(!format!("{events:?}").contains("missing-secret"));
                    }
                    let (sender, receiver) = mpsc::channel();
                    *state.queue_started.lock().expect("install queue signal") = Some(sender);
                    state.block_queue.store(true, Ordering::SeqCst);
                    let mut future = Box::pin(REQUEST_ID.scope(
                        Some(Arc::from("server-cancelled")),
                        crate::disk::local::batch_shard_pread(vec![(path, 0, 6)]),
                    ));
                    assert!(futures::poll!(future.as_mut()).is_pending());
                    receiver
                        .recv_timeout(std::time::Duration::from_secs(5))
                        .expect("blocking worker started");
                    drop(future);
                    state.release.wait();
                    let deadline = Instant::now() + std::time::Duration::from_secs(5);
                    loop {
                        if state.events.lock().expect("completion events").iter().any(|e| {
                            e.get("stage") == Some(&json!("execution")) && e.get("request_id") == Some(&json!("server-cancelled"))
                        }) {
                            break;
                        }
                        assert!(Instant::now() < deadline, "detached worker must complete diagnostic");
                        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                    }
                });
            })
        });
    }
}
