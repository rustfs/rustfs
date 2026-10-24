// Copyright 2026 RustFS Team
// Licensed under the Apache License, Version 2.0.

//! Bounded foreground completion windows for background resource feedback.
//! Recording uses fixed atomic storage, without an allocation, mutex or task
//! per request. A single reader rotates banks and waits for the retired bank's
//! writers before reading it. Delayed writers delay publication, not the
//! recorded window boundary. Percentiles are intervals, never exact values.

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

const SUB_BUCKETS: usize = 64;
const FINITE_BUCKETS: usize = 27 * SUB_BUCKETS;
const BUCKETS: usize = FINITE_BUCKETS + 1;
const MAX_MICROS: u64 = u32::MAX as u64;

/// Bounded foreground categories; no bucket, object or request identifiers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ForegroundClass {
    /// Object body reads.
    Read,
    /// Object writes, including multipart and copy operations.
    Write,
    /// Metadata and listing operations.
    Metadata,
    /// Other operations, including rejection before typed dispatch.
    Other,
}

impl ForegroundClass {
    /// Stable order shared by window summaries.
    pub const ALL: [Self; 4] = [Self::Read, Self::Write, Self::Metadata, Self::Other];

    /// Fixed metric label.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Read => "read",
            Self::Write => "write",
            Self::Metadata => "metadata",
            Self::Other => "other",
        }
    }

    const fn index(self) -> usize {
        match self {
            Self::Read => 0,
            Self::Write => 1,
            Self::Metadata => 2,
            Self::Other => 3,
        }
    }
}

/// Outcome at the observed response-body boundary, not a client ACK.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CompletionOutcome {
    /// Successful response body completed.
    Success,
    /// Error response, service error, or body-stream failure.
    Failed,
    /// The request/body was dropped before completion.
    Cancelled,
}

/// Inclusive bounds containing the observed percentile.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LatencyBounds {
    /// Inclusive lower bound in nanoseconds.
    pub lower_nanos: u64,
    /// Inclusive upper bound; absent for the overflow bucket.
    pub upper_nanos: Option<u64>,
}

/// One class in a complete observation window.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ClassCompletions {
    /// All finishes, including errors and cancellation.
    pub finished: u64,
    /// Error responses, service errors and body-stream failures.
    pub failed: u64,
    /// Censored observations; these must not be treated as successful latency.
    pub cancelled: u64,
    /// P99 of non-cancelled finishes, when at least 100 samples exist.
    /// This count permits computing the rank; it is not statistical confidence.
    pub p99: Option<LatencyBounds>,
}

/// Clock readings bracketing an atomic event-window cut. A scheduler pause
/// between these readings is visible, rather than disguised as precise timing.
#[derive(Clone, Copy, Debug)]
pub struct WindowBoundary {
    /// Read immediately before the event cut.
    pub earliest: Instant,
    /// Read immediately after the event cut.
    pub latest: Instant,
}

/// Complete windows retain their actual cut brackets, even if publication waits.
#[derive(Clone, Copy, Debug)]
pub struct ForegroundWindow {
    /// Start of this event window.
    pub start: WindowBoundary,
    /// Cut of the retired bank, possibly older than publication time.
    pub end: WindowBoundary,
    /// Request starts in this window, including pre-dispatch rejection.
    pub started: Option<u64>,
    /// Requests still alive at the cut; absent after accounting overflow.
    pub in_flight: Option<u64>,
    /// Per-class completions in ForegroundClass::ALL order.
    pub classes: [Option<ClassCompletions>; 4],
    /// False after a counter overflow or an inconsistent event count.
    pub valid: bool,
}

struct ClassBank {
    latencies: [AtomicU64; BUCKETS],
    failed: AtomicU64,
    cancelled: AtomicU64,
}

impl ClassBank {
    fn new() -> Self {
        Self {
            latencies: std::array::from_fn(|_| AtomicU64::new(0)),
            failed: AtomicU64::new(0),
            cancelled: AtomicU64::new(0),
        }
    }
}

struct Bank {
    writers: AtomicUsize,
    started: AtomicU64,
    overflowed: AtomicBool,
    classes: [ClassBank; 4],
}

impl Bank {
    fn new() -> Self {
        Self {
            writers: AtomicUsize::new(0),
            started: AtomicU64::new(0),
            overflowed: AtomicBool::new(false),
            classes: std::array::from_fn(|_| ClassBank::new()),
        }
    }

    fn increment(&self, counter: &AtomicU64) {
        if counter.fetch_add(1, Ordering::Relaxed) == u64::MAX {
            self.overflowed.store(true, Ordering::Relaxed);
        }
    }
}

struct Writer<'a>(&'a Bank);

impl Drop for Writer<'_> {
    fn drop(&mut self) {
        self.0.writers.fetch_sub(1, Ordering::SeqCst);
    }
}

/// One process-wide recorder. Its fixed storage is allocated with the owner.
/// A request holds only a reference, timestamps and bounded enum state.
pub struct ForegroundRecorder {
    created_at: Instant,
    epoch: AtomicU64,
    reader_taken: AtomicBool,
    banks: [Bank; 2],
}

impl Default for ForegroundRecorder {
    fn default() -> Self {
        Self::new()
    }
}

impl ForegroundRecorder {
    /// Construct empty accounting for one lifetime of the owning process.
    pub fn new() -> Self {
        Self {
            created_at: Instant::now(),
            epoch: AtomicU64::new(0),
            reader_taken: AtomicBool::new(false),
            banks: std::array::from_fn(|_| Bank::new()),
        }
    }

    fn writer(&self) -> Writer<'_> {
        loop {
            let epoch = self.epoch.load(Ordering::SeqCst);
            let bank = &self.banks[usize::from(epoch & 1 != 0)];
            bank.writers.fetch_add(1, Ordering::SeqCst);
            let writer = Writer(bank);
            // Sequential ordering joins registration, the reader's cut, and
            // its zero-writer check. A late old-epoch registrant cannot write
            // into a bank that the reader has begun draining or reusing.
            if self.epoch.load(Ordering::SeqCst) == epoch {
                return writer;
            }
        }
    }

    /// Begin at the external service-call boundary. Drop records cancellation
    /// unless finish has already recorded a response-body outcome.
    pub fn start(&self) -> ForegroundObservation<'_> {
        let started_at = Instant::now();
        let writer = self.writer();
        writer.0.increment(&writer.0.started);
        ForegroundObservation {
            recorder: self,
            started_at: Some(started_at),
            class: ForegroundClass::Other,
        }
    }

    fn finish(&self, class: ForegroundClass, elapsed: Duration, outcome: CompletionOutcome) {
        let writer = self.writer();
        let bank = writer.0;
        let class = &bank.classes[class.index()];
        if outcome == CompletionOutcome::Cancelled {
            bank.increment(&class.cancelled);
            return;
        }
        let bucket = latency_bucket(elapsed);
        bank.increment(&class.latencies[bucket]);
        if outcome == CompletionOutcome::Failed {
            bank.increment(&class.failed);
        }
    }

    /// Claim the sole reader. It cannot be replaced after being dropped:
    /// resetting its carried in-flight count would falsify later windows.
    pub fn take_reader(&self) -> Option<ForegroundReader<'_>> {
        self.reader_taken
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .ok()?;
        Some(ForegroundReader {
            recorder: self,
            start: WindowBoundary {
                earliest: self.created_at,
                latest: self.created_at,
            },
            retired: None,
            in_flight: Some(0),
        })
    }
}

/// Non-cloneable request ownership, moved from the service future to its body.
pub struct ForegroundObservation<'a> {
    recorder: &'a ForegroundRecorder,
    started_at: Option<Instant>,
    class: ForegroundClass,
}

impl ForegroundObservation<'_> {
    /// Set the first dispatched operation's bounded category.
    pub fn set_class(&mut self, class: ForegroundClass) {
        self.class = class;
    }

    /// Finish once. Later calls or Drop do not produce another observation.
    pub fn finish(&mut self, outcome: CompletionOutcome) {
        if let Some(started_at) = self.started_at.take() {
            self.recorder.finish(self.class, started_at.elapsed(), outcome);
        }
    }
}

impl Drop for ForegroundObservation<'_> {
    fn drop(&mut self) {
        self.finish(CompletionOutcome::Cancelled);
    }
}

/// Single-reader state; never acquired by a request path.
pub struct ForegroundReader<'a> {
    recorder: &'a ForegroundRecorder,
    start: WindowBoundary,
    retired: Option<(usize, WindowBoundary)>,
    in_flight: Option<u64>,
}

impl ForegroundReader<'_> {
    /// Cut a new window, or finish publishing an earlier cut. A live retired
    /// writer returns None without blocking, discarding data or moving its cut.
    pub fn sample(&mut self) -> Option<ForegroundWindow> {
        if self.retired.is_none() {
            let epoch = self.recorder.epoch.load(Ordering::SeqCst);
            let next = epoch.checked_add(1)?;
            let earliest = Instant::now();
            self.recorder.epoch.store(next, Ordering::SeqCst);
            let cut = WindowBoundary {
                earliest,
                latest: Instant::now(),
            };
            self.retired = Some((usize::from(epoch & 1 != 0), cut));
        }
        let (index, end) = self.retired?;
        let bank = &self.recorder.banks[index];
        if bank.writers.load(Ordering::SeqCst) != 0 {
            return None;
        }

        let started = bank.started.swap(0, Ordering::Relaxed);
        let overflowed = bank.overflowed.swap(false, Ordering::Relaxed);
        let classes = std::array::from_fn(|index| {
            let class = &bank.classes[index];
            let failed = class.failed.swap(0, Ordering::Relaxed);
            let cancelled = class.cancelled.swap(0, Ordering::Relaxed);
            let buckets = std::array::from_fn::<_, BUCKETS, _>(|i| class.latencies[i].swap(0, Ordering::Relaxed));
            let non_cancelled = buckets.iter().try_fold(0_u64, |sum, count| sum.checked_add(*count));
            let finished = non_cancelled.and_then(|total| total.checked_add(cancelled));
            let finished = finished.filter(|_| !overflowed && non_cancelled.is_some_and(|total| failed <= total))?;
            Some(ClassCompletions {
                finished,
                failed,
                cancelled,
                p99: non_cancelled
                    .filter(|total| *total >= 100)
                    .and_then(|total| percentile_99(&buckets, total)),
            })
        });
        let started = (!overflowed).then_some(started);
        let finished = classes
            .iter()
            .try_fold(0_u64, |sum, class| sum.checked_add(class.as_ref()?.finished));
        self.in_flight = self
            .in_flight
            .zip(finished)
            .and_then(|(active, finished)| active.checked_add(started?)?.checked_sub(finished));
        let window = ForegroundWindow {
            start: self.start,
            end,
            started,
            in_flight: self.in_flight,
            classes,
            valid: self.in_flight.is_some(),
        };
        self.start = end;
        self.retired = None;
        Some(window)
    }
}

fn latency_bucket(elapsed: Duration) -> usize {
    let micros = elapsed.as_nanos().div_ceil(1_000);
    let Ok(micros) = u64::try_from(micros) else {
        return FINITE_BUCKETS;
    };
    if micros > MAX_MICROS {
        return FINITE_BUCKETS;
    }
    if micros < SUB_BUCKETS as u64 {
        return usize::try_from(micros).unwrap_or(FINITE_BUCKETS);
    }
    let shift = 63 - micros.leading_zeros() - 6;
    usize::try_from((u64::from(shift) + 1) * SUB_BUCKETS as u64 + (micros >> shift) - SUB_BUCKETS as u64)
        .unwrap_or(FINITE_BUCKETS)
}

fn bucket_bounds(index: usize) -> LatencyBounds {
    if index == FINITE_BUCKETS {
        return LatencyBounds {
            lower_nanos: MAX_MICROS * 1_000 + 1,
            upper_nanos: None,
        };
    }
    let (lower, upper) = if index < SUB_BUCKETS {
        (index as u64, index as u64)
    } else {
        let shift = index / SUB_BUCKETS - 1;
        let lower = (SUB_BUCKETS + index % SUB_BUCKETS) as u64 * (1_u64 << shift);
        (lower, lower + (1_u64 << shift) - 1)
    };
    LatencyBounds {
        lower_nanos: lower.saturating_sub(1) * 1_000 + u64::from(lower != 0),
        upper_nanos: Some(upper * 1_000),
    }
}

fn percentile_99(buckets: &[u64; BUCKETS], total: u64) -> Option<LatencyBounds> {
    let rank = (u128::from(total) * 99).div_ceil(100);
    let mut cumulative = 0_u128;
    for (index, count) in buckets.iter().enumerate() {
        cumulative += u128::from(*count);
        if cumulative >= rank {
            return Some(bucket_bounds(index));
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn percentile_intervals_contain_each_bucket_edge_and_neighbour() {
        for index in 0..BUCKETS {
            let bounds = bucket_bounds(index);
            assert_eq!(latency_bucket(Duration::from_nanos(bounds.lower_nanos)), index);
            if let Some(upper) = bounds.upper_nanos {
                assert!(upper >= bounds.lower_nanos);
                assert_eq!(latency_bucket(Duration::from_nanos(upper)), index);
                assert_eq!(latency_bucket(Duration::from_nanos(upper + 1)), index + 1);
            }
        }
        assert_eq!(latency_bucket(Duration::MAX), FINITE_BUCKETS);
    }

    #[test]
    fn body_ownership_carries_in_flight_counts_across_windows() {
        let recorder = ForegroundRecorder::new();
        let mut reader = recorder.take_reader().expect("first reader");
        let mut request = recorder.start();
        request.set_class(ForegroundClass::Read);
        let first = reader.sample().expect("cut with a live request");
        assert!(first.valid);
        assert_eq!(first.started, Some(1));
        assert_eq!(first.in_flight, Some(1));
        assert_eq!(first.classes[0].expect("valid read count").finished, 0);

        request.finish(CompletionOutcome::Success);
        request.finish(CompletionOutcome::Failed);
        drop(request);
        let second = reader.sample().expect("body finished");
        assert!(second.valid);
        assert_eq!(second.started, Some(0));
        assert_eq!(second.in_flight, Some(0));
        let read = second.classes[0].expect("valid read summary");
        assert_eq!((read.finished, read.failed, read.cancelled, read.p99), (1, 0, 0, None));
        assert_eq!(first.end.earliest, second.start.earliest);
        assert_eq!(first.end.latest, second.start.latest);
    }

    #[test]
    fn cancellations_are_counted_without_fabricating_success_latencies() {
        let recorder = ForegroundRecorder::new();
        let mut reader = recorder.take_reader().expect("first reader");
        for _ in 0..150 {
            let mut request = recorder.start();
            request.set_class(ForegroundClass::Write);
        }
        let window = reader.sample().expect("cancelled requests");
        let write = window.classes[1].expect("known write count");
        assert!(window.valid);
        assert_eq!(window.in_flight, Some(0));
        assert_eq!((write.finished, write.cancelled, write.failed, write.p99), (150, 150, 0, None));
    }

    #[test]
    fn delayed_retired_writer_preserves_the_original_cut_and_next_bank() {
        let recorder = ForegroundRecorder::new();
        let mut reader = recorder.take_reader().expect("first reader");
        let writer = recorder.writer();
        assert!(reader.sample().is_none(), "never read a live writer's bank");
        let original_cut = reader.retired.expect("pending cut").1;
        let request = recorder.start(); // a start after the cut belongs to the new bank
        writer.0.increment(&writer.0.started);
        drop(writer);
        let previous = reader.sample().expect("retired writer drained");
        assert_eq!(previous.end.earliest, original_cut.earliest);
        assert_eq!(previous.end.latest, original_cut.latest);
        assert_eq!(previous.started, Some(1));
        assert_eq!(previous.in_flight, Some(1));
        drop(request);
        let current = reader.sample().expect("next bank");
        assert_eq!(current.started, Some(1));
        assert_eq!(current.in_flight, Some(1));
        assert_eq!(current.classes[3].expect("unclassified cancellation").cancelled, 1);
    }

    #[test]
    fn completed_percentile_uses_rank_and_never_turns_maximum_into_p99() {
        let recorder = ForegroundRecorder::new();
        let mut reader = recorder.take_reader().expect("first reader");
        for i in 0..100 {
            let writer = recorder.writer();
            writer.0.increment(&writer.0.started);
            drop(writer);
            recorder.finish(
                ForegroundClass::Read,
                if i == 99 {
                    Duration::from_secs(60)
                } else {
                    Duration::from_millis(2)
                },
                if i == 99 {
                    CompletionOutcome::Failed
                } else {
                    CompletionOutcome::Success
                },
            );
        }
        let window = reader.sample().expect("complete window");
        let read = window.classes[0].expect("read summary");
        assert_eq!((read.finished, read.failed, read.cancelled), (100, 1, 0));
        let p99 = read.p99.expect("100 non-cancelled samples");
        assert!(p99.lower_nanos <= 2_000_000);
        assert!(p99.upper_nanos.is_some_and(|upper| (2_000_000..3_000_000).contains(&upper)));
    }

    #[test]
    fn overflow_stays_unknown_and_reader_cannot_reset_event_accounting() {
        let recorder = ForegroundRecorder::new();
        {
            let mut reader = recorder.take_reader().expect("first reader");
            recorder.banks[0].started.store(u64::MAX, Ordering::Relaxed);
            drop(recorder.start());
            let invalid = reader.sample().expect("overflow window");
            assert!(!invalid.valid);
            assert_eq!(invalid.started, None);
            assert_eq!(invalid.in_flight, None);
            assert_eq!(invalid.classes, [None; 4]);
            let later = reader.sample().expect("later empty window");
            assert!(!later.valid, "a lost carried count cannot recover by inventing zero active work");
        }
        assert!(recorder.take_reader().is_none(), "reader replacement would lose live request ownership");
    }

    #[test]
    fn concurrent_rotation_preserves_every_start_finish_and_cancellation() {
        let recorder = Arc::new(ForegroundRecorder::new());
        let mut reader = recorder.take_reader().expect("first reader");
        let workers: Vec<_> = (0..4)
            .map(|_| {
                let recorder = Arc::clone(&recorder);
                std::thread::spawn(move || {
                    for index in 0..10_000 {
                        let mut request = recorder.start();
                        request.set_class(ForegroundClass::Read);
                        if index % 10 != 0 {
                            request.finish(CompletionOutcome::Success);
                        }
                    }
                })
            })
            .collect();
        let (mut started, mut finished, mut cancelled) = (0, 0, 0);
        let mut add_window = |window: ForegroundWindow| {
            assert!(window.valid, "concurrent event cuts must stay balanced");
            started += window.started.expect("valid start count");
            let reads = window.classes[0].expect("valid read count");
            finished += reads.finished;
            cancelled += reads.cancelled;
        };
        while workers.iter().any(|worker| !worker.is_finished()) {
            if let Some(window) = reader.sample() {
                add_window(window);
            }
            std::thread::yield_now();
        }
        for worker in workers {
            worker.join().expect("writer must finish");
        }
        add_window(reader.sample().expect("drain after writers join"));
        // A previously retired window may have been published by the last
        // sample; cut once more to include the current, still open bank.
        let final_window = reader.sample().expect("final open bank");
        assert_eq!(final_window.in_flight, Some(0));
        add_window(final_window);
        assert_eq!((started, finished, cancelled), (40_000, 40_000, 4_000));
    }
}
