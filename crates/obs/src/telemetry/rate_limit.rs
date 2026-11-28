// Copyright 2026 RustFS Team
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

//! Sink-side rate-limiting tracing layer for file-backed log outputs.
//!
//! Protects on-disk log sinks against unthrottled hot-path log storms even if a
//! call site lacks a dedicated source-side throttle. Wraps only the file layer
//! via [`RateLimitedLayer`] so metrics, traces, and sibling layers remain
//! unaffected.
//!
//! Token accounting executes strictly in [`Layer::on_event`] (never for spans,
//! and only after upstream `EnvFilter` / target filters have admitted the
//! event), using lock-free single-word atomic fixed-window counters:
//! - a global file-sink ceiling across all call sites;
//! - a fixed-size per-callsite fairness table (zero heap growth) so one noisy
//!   site cannot starve infrequent errors from other call sites.

use crate::config::OtelConfig;
use crate::global::METRIC_LOG_CLEANER_SINK_DROPPED_EVENTS_TOTAL;
use metrics::counter;
use rustfs_config::observability::DEFAULT_OBS_LOG_SINK_MAX_EVENTS_PER_SEC;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;
use tracing::{Dispatch, Event, Metadata, Subscriber, span, subscriber::Interest};
use tracing_subscriber::{Layer, layer::Context, registry::LookupSpan};

/// Number of fixed direct-mapped callsite fairness slots (power of two).
const CALLSITE_SLOTS: usize = 256;
/// Number of low bits reserved for the per-window event counter (`2^24 - 1`).
const COUNT_BITS: u32 = 24;
const COUNT_MASK: u64 = (1u64 << COUNT_BITS) - 1;
const WINDOW_MASK: u64 = (1u64 << (64 - COUNT_BITS)) - 1;

/// Clock abstraction for deterministic, sleep-free unit tests.
trait WindowClock: Send + Sync {
    fn current_window(&self) -> u64;
}

struct MonotonicSecondClock {
    started: Instant,
}

impl MonotonicSecondClock {
    fn new() -> Self {
        Self { started: Instant::now() }
    }
}

impl WindowClock for MonotonicSecondClock {
    fn current_window(&self) -> u64 {
        self.started.elapsed().as_secs() & WINDOW_MASK
    }
}

/// Lock-free fixed-window counter packed into one `AtomicU64`:
/// upper 40 bits = window index, lower 24 bits = admitted count in that window.
#[derive(Default)]
struct PackedWindowBucket {
    state: AtomicU64,
}

impl PackedWindowBucket {
    fn try_admit(&self, window: u64, limit: u64) -> bool {
        if limit == 0 {
            return true;
        }
        let capped_limit = limit.min(COUNT_MASK);
        let window = window & WINDOW_MASK;
        let mut packed = self.state.load(Ordering::Acquire);
        loop {
            let stored_window = packed >> COUNT_BITS;
            let stored_count = packed & COUNT_MASK;
            let next = if stored_window != window {
                (window << COUNT_BITS) | 1
            } else if stored_count < capped_limit {
                (window << COUNT_BITS) | (stored_count + 1)
            } else {
                return false;
            };
            match self
                .state
                .compare_exchange_weak(packed, next, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => return true,
                Err(observed) => packed = observed,
            }
        }
    }
}

struct RateLimiterState {
    global_limit_per_sec: u64,
    per_callsite_limit_per_sec: u64,
    global_bucket: PackedWindowBucket,
    callsite_buckets: Box<[PackedWindowBucket; CALLSITE_SLOTS]>,
    dropped_events: AtomicU64,
    clock: Box<dyn WindowClock>,
}

/// Lock-free global and per-callsite event rate limiter backing [`RateLimitedLayer`].
#[derive(Clone)]
pub(crate) struct FileSinkRateLimitFilter {
    state: Arc<RateLimiterState>,
}

impl FileSinkRateLimitFilter {
    /// Construct from [`OtelConfig`] defaults and overrides.
    ///
    /// When `log_sink_max_events_per_sec` is `Some(0)`, sink-side rate limiting
    /// is disabled (pass-through). When `log_sink_per_callsite_max_events_per_sec`
    /// is unset, each callsite bucket is capped at `(global / 4).max(1)` so a
    /// single noisy site cannot exhaust the global sink budget.
    pub(crate) fn from_config(config: &OtelConfig) -> Self {
        let global_limit = config
            .log_sink_max_events_per_sec
            .unwrap_or(DEFAULT_OBS_LOG_SINK_MAX_EVENTS_PER_SEC);
        let per_callsite_limit = if global_limit == 0 {
            0
        } else {
            config
                .log_sink_per_callsite_max_events_per_sec
                .unwrap_or_else(|| (global_limit / 4).max(1))
        };
        Self::new(global_limit, per_callsite_limit)
    }

    /// Construct with explicit per-second global and per-callsite limits.
    pub(crate) fn new(global_limit_per_sec: u64, per_callsite_limit_per_sec: u64) -> Self {
        Self::with_clock(global_limit_per_sec, per_callsite_limit_per_sec, Box::new(MonotonicSecondClock::new()))
    }

    fn with_clock(global_limit_per_sec: u64, per_callsite_limit_per_sec: u64, clock: Box<dyn WindowClock>) -> Self {
        let callsite_buckets = Box::new(std::array::from_fn(|_| PackedWindowBucket::default()));
        Self {
            state: Arc::new(RateLimiterState {
                global_limit_per_sec,
                per_callsite_limit_per_sec,
                global_bucket: PackedWindowBucket::default(),
                callsite_buckets,
                dropped_events: AtomicU64::new(0),
                clock,
            }),
        }
    }

    /// Total events shed by this sink filter.
    #[cfg(test)]
    pub(crate) fn dropped_events(&self) -> u64 {
        self.state.dropped_events.load(Ordering::Acquire)
    }

    fn callsite_slot(meta: &Metadata<'_>) -> usize {
        // Deterministic FNV-1a hash of (target, file, line) into
        // `[0, CALLSITE_SLOTS)` so slot mapping is stable across runs/ASLR.
        let mut h: u64 = 0xcbf2_9ce4_8422_2325;
        for byte in meta.target().as_bytes() {
            h ^= u64::from(*byte);
            h = h.wrapping_mul(0x0100_0000_01b3);
        }
        h ^= 0xff;
        h = h.wrapping_mul(0x0100_0000_01b3);
        if let Some(file) = meta.file() {
            for byte in file.as_bytes() {
                h ^= u64::from(*byte);
                h = h.wrapping_mul(0x0100_0000_01b3);
            }
        }
        h ^= u64::from(meta.line().unwrap_or(0));
        h = h.wrapping_mul(0x0100_0000_01b3);
        h ^= h >> 32;
        (h as usize) & (CALLSITE_SLOTS - 1)
    }

    fn allow_event(&self, meta: &Metadata<'_>) -> bool {
        if self.state.global_limit_per_sec == 0 && self.state.per_callsite_limit_per_sec == 0 {
            return true;
        }
        let window = self.state.clock.current_window();

        // Check per-callsite fairness first so a single stormy site is dropped
        // before it can consume the shared global budget reserved for other sites.
        if self.state.per_callsite_limit_per_sec > 0 {
            let slot = Self::callsite_slot(meta);
            if !self.state.callsite_buckets[slot].try_admit(window, self.state.per_callsite_limit_per_sec) {
                self.record_drop();
                return false;
            }
        }

        if self.state.global_limit_per_sec > 0 && !self.state.global_bucket.try_admit(window, self.state.global_limit_per_sec) {
            self.record_drop();
            return false;
        }

        true
    }

    fn record_drop(&self) {
        self.state.dropped_events.fetch_add(1, Ordering::Relaxed);
        counter!(METRIC_LOG_CLEANER_SINK_DROPPED_EVENTS_TOTAL).increment(1);
    }
}

/// Wrapping [`Layer`] that enforces a [`FileSinkRateLimitFilter`] strictly on
/// `on_event` for its inner file-sink layer while forwarding all span lifecycle
/// hooks untouched.
pub(crate) struct RateLimitedLayer<L> {
    inner: L,
    limiter: FileSinkRateLimitFilter,
}

impl<L> RateLimitedLayer<L> {
    pub(crate) fn new(inner: L, limiter: FileSinkRateLimitFilter) -> Self {
        Self { inner, limiter }
    }
}

impl<S, L> Layer<S> for RateLimitedLayer<L>
where
    S: Subscriber + for<'lookup> LookupSpan<'lookup>,
    L: Layer<S>,
{
    fn on_register_dispatch(&self, collector: &Dispatch) {
        self.inner.on_register_dispatch(collector);
    }

    fn on_layer(&mut self, subscriber: &mut S) {
        self.inner.on_layer(subscriber);
    }

    fn register_callsite(&self, metadata: &'static Metadata<'static>) -> Interest {
        self.inner.register_callsite(metadata)
    }

    fn enabled(&self, metadata: &Metadata<'_>, ctx: Context<'_, S>) -> bool {
        self.inner.enabled(metadata, ctx)
    }

    fn on_new_span(&self, attrs: &span::Attributes<'_>, id: &span::Id, ctx: Context<'_, S>) {
        self.inner.on_new_span(attrs, id, ctx);
    }

    fn on_record(&self, span: &span::Id, values: &span::Record<'_>, ctx: Context<'_, S>) {
        self.inner.on_record(span, values, ctx);
    }

    fn on_follows_from(&self, span: &span::Id, follows: &span::Id, ctx: Context<'_, S>) {
        self.inner.on_follows_from(span, follows, ctx);
    }

    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
        if self.limiter.allow_event(event.metadata()) {
            self.inner.on_event(event, ctx);
        }
    }

    fn on_enter(&self, id: &span::Id, ctx: Context<'_, S>) {
        self.inner.on_enter(id, ctx);
    }

    fn on_exit(&self, id: &span::Id, ctx: Context<'_, S>) {
        self.inner.on_exit(id, ctx);
    }

    fn on_close(&self, id: span::Id, ctx: Context<'_, S>) {
        self.inner.on_close(id, ctx);
    }

    fn on_id_change(&self, old: &span::Id, new: &span::Id, ctx: Context<'_, S>) {
        self.inner.on_id_change(old, new, ctx);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::telemetry::local::build_json_log_layer;
    use std::io;
    use std::sync::Mutex;
    use tracing_subscriber::{EnvFilter, Registry, fmt::format::FmtSpan, layer::SubscriberExt};

    struct ManualClock {
        window: Arc<AtomicU64>,
    }

    impl WindowClock for ManualClock {
        fn current_window(&self) -> u64 {
            self.window.load(Ordering::Acquire)
        }
    }

    #[derive(Clone, Default)]
    struct CaptureWriter {
        buf: Arc<Mutex<Vec<u8>>>,
    }

    struct CaptureGuard(Arc<Mutex<Vec<u8>>>);

    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for CaptureWriter {
        type Writer = CaptureGuard;
        fn make_writer(&'a self) -> Self::Writer {
            CaptureGuard(Arc::clone(&self.buf))
        }
    }

    impl io::Write for CaptureGuard {
        fn write(&mut self, data: &[u8]) -> io::Result<usize> {
            self.0.lock().expect("capture lock").extend_from_slice(data);
            Ok(data.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl CaptureWriter {
        fn lines(&self) -> Vec<String> {
            let bytes = self.buf.lock().expect("capture lock").clone();
            String::from_utf8(bytes)
                .expect("valid UTF-8")
                .lines()
                .map(str::to_owned)
                .collect()
        }
    }

    fn test_filter(global: u64, per_callsite: u64) -> (FileSinkRateLimitFilter, Arc<AtomicU64>) {
        let clock = Arc::new(AtomicU64::new(0));
        let filter = FileSinkRateLimitFilter::with_clock(
            global,
            per_callsite,
            Box::new(ManualClock {
                window: Arc::clone(&clock),
            }),
        );
        (filter, clock)
    }

    #[test]
    fn storm_is_capped_per_window_and_recovers_on_next_window() {
        let (filter, clock) = test_filter(5, 5);
        let file_writer = CaptureWriter::default();
        let stdout_writer = CaptureWriter::default();

        let file_layer =
            RateLimitedLayer::new(build_json_log_layer(file_writer.clone(), false, FmtSpan::NONE, false), filter.clone());
        let stdout_layer = build_json_log_layer(stdout_writer.clone(), false, FmtSpan::NONE, false);
        let subscriber = Registry::default().with(file_layer).with(stdout_layer);

        tracing::subscriber::with_default(subscriber, || {
            for idx in 0..20 {
                tracing::error!(idx, "storm event");
            }
            clock.store(1, Ordering::Release);
            for idx in 0..3 {
                tracing::error!(idx, "next window event");
            }
        });

        assert_eq!(file_writer.lines().len(), 8, "5 in window 0 + 3 in window 1");
        assert_eq!(filter.dropped_events(), 15, "15 excess storm events dropped");
        assert_eq!(
            stdout_writer.lines().len(),
            23,
            "sibling stdout layer must not be throttled by file-sink filter"
        );
    }

    #[test]
    fn noisy_callsite_cannot_starve_distinct_critical_callsite() {
        let (filter, _clock) = test_filter(10, 3);
        let file_writer = CaptureWriter::default();
        let layer = RateLimitedLayer::new(build_json_log_layer(file_writer.clone(), false, FmtSpan::NONE, false), filter.clone());
        let subscriber = Registry::default().with(layer);

        tracing::subscriber::with_default(subscriber, || {
            let span = tracing::info_span!("req", request_id = "req-42");
            let _guard = span.enter();
            for i in 0..50 {
                tracing::warn!(i, "noisy site");
            }
            tracing::error!("distinct critical site");
        });

        let lines = file_writer.lines();
        assert_eq!(lines.len(), 4, "3 from noisy site + 1 from critical site");
        assert!(
            lines
                .iter()
                .any(|line| line.contains("distinct critical site") && line.contains("req-42")),
            "distinct callsite must still be logged with intact span request_id: {lines:?}"
        );
        assert_eq!(filter.dropped_events(), 47);
    }

    #[test]
    fn upstream_env_filter_rejections_consume_zero_sink_budget() {
        let (filter, _clock) = test_filter(2, 2);
        let file_writer = CaptureWriter::default();
        let file_layer =
            RateLimitedLayer::new(build_json_log_layer(file_writer.clone(), false, FmtSpan::NONE, false), filter.clone());
        let subscriber = Registry::default().with(EnvFilter::new("error")).with(file_layer);

        tracing::subscriber::with_default(subscriber, || {
            for _ in 0..10 {
                tracing::info!("filtered out upstream");
            }
            tracing::error!("first real error");
            tracing::error!("second real error");
            tracing::error!("third real error over budget");
        });

        assert_eq!(file_writer.lines().len(), 2);
        assert_eq!(filter.dropped_events(), 1);
    }

    #[test]
    fn zero_limits_disable_sink_rate_limiting() {
        let (filter, _clock) = test_filter(0, 0);
        let file_writer = CaptureWriter::default();
        let layer = RateLimitedLayer::new(build_json_log_layer(file_writer.clone(), false, FmtSpan::NONE, false), filter.clone());
        let subscriber = Registry::default().with(layer);

        tracing::subscriber::with_default(subscriber, || {
            for idx in 0..25 {
                tracing::error!(idx, "unbounded event");
            }
        });

        assert_eq!(file_writer.lines().len(), 25);
        assert_eq!(filter.dropped_events(), 0);
    }
}
