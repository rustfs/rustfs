// Copyright 2026 RustFS Team
// Licensed under the Apache License, Version 2.0.

//! In-process foreground windows for background protection. The HTTP service
//! owns each observation through response-body completion, failure or drop.
//! This is server-side production of the body, not proof of client receipt.

use rustfs_concurrency::foreground::{ForegroundObservation, ForegroundRecorder, ForegroundWindow};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{LazyLock, RwLock};
use std::time::Duration;
use tokio_util::sync::CancellationToken;

static RECORDER: LazyLock<ForegroundRecorder> = LazyLock::new(ForegroundRecorder::new);
static ENABLED: AtomicBool = AtomicBool::new(false);
static LATEST: RwLock<Option<ForegroundWindow>> = RwLock::new(None);

pub(crate) fn start_request() -> Option<ForegroundObservation<'static>> {
    ENABLED.load(Ordering::Acquire).then(|| RECORDER.start())
}

/// Latest complete event window. Consumers must check validity, cut-bracket
/// uncertainty and age; an old or low-sample window is not evidence of idle.
pub fn latest_window() -> Option<ForegroundWindow> {
    *LATEST.read().unwrap_or_else(std::sync::PoisonError::into_inner)
}

struct RecorderLifetime;

impl Drop for RecorderLifetime {
    fn drop(&mut self) {
        ENABLED.store(false, Ordering::Release);
        let last = LATEST.write().unwrap_or_else(std::sync::PoisonError::into_inner).take();
        if let Some(window) = last {
            // Clear earlier exports even if the export gate has since changed.
            // The original cut and numeric values are not rewritten as zero.
            invalidate_window(window);
        }
    }
}

pub(crate) fn start_sampler(cancel: CancellationToken) -> Option<tokio::task::JoinHandle<()>> {
    if cancel.is_cancelled() {
        return None;
    }
    let mut reader = RECORDER.take_reader()?;
    let lifetime = RecorderLifetime;
    ENABLED.store(true, Ordering::Release);
    Some(tokio::spawn(async move {
        // Also clear availability if the worker is dropped before its first poll.
        let _lifetime = lifetime;
        let mut tick = tokio::time::interval(Duration::from_secs(1));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                biased;
                _ = cancel.cancelled() => break,
                _ = tick.tick() => {
                    if let Some(window) = reader.sample() {
                        *LATEST.write().unwrap_or_else(std::sync::PoisonError::into_inner) = Some(window);
                        if rustfs_io_metrics::metrics_enabled() {
                            record_window(window);
                        }
                    }
                }
            }
        }
    }))
}

fn invalidate_window(mut window: ForegroundWindow) {
    window.valid = false;
    window.started = None;
    window.in_flight = None;
    window.classes = [None; 4];
    record_window(window);
}

fn record_window(window: ForegroundWindow) {
    // Exported f64 gauges are diagnostic. Control consumers use the exact
    // integer fields in latest_window, never these potentially rounded values.
    metrics::gauge!("rustfs_background_foreground_window_valid").set(f64::from(window.valid));
    metrics::gauge!("rustfs_background_foreground_window_cut_uncertainty_seconds")
        .set(window.end.latest.duration_since(window.end.earliest).as_secs_f64());
    metrics::gauge!("rustfs_background_foreground_window_duration_seconds")
        .set(window.end.latest.duration_since(window.start.earliest).as_secs_f64());
    if let Some(active) = window.in_flight {
        metrics::gauge!("rustfs_background_foreground_window_in_flight").set(active as f64);
    }
    if let Some(started) = window.started {
        metrics::gauge!("rustfs_background_foreground_window_starts").set(started as f64);
    }
    for (class, counts) in rustfs_concurrency::foreground::ForegroundClass::ALL
        .into_iter()
        .zip(window.classes)
    {
        let label = class.as_str();
        metrics::gauge!("rustfs_background_foreground_window_class_available", "class" => label).set(f64::from(counts.is_some()));
        let p99 = counts.and_then(|counts| counts.p99);
        metrics::gauge!("rustfs_background_foreground_window_p99_available", "class" => label).set(f64::from(p99.is_some()));
        metrics::gauge!("rustfs_background_foreground_window_p99_upper_available", "class" => label)
            .set(f64::from(p99.and_then(|bounds| bounds.upper_nanos).is_some()));
        let Some(counts) = counts else {
            continue;
        };
        metrics::gauge!("rustfs_background_foreground_window_completions", "class" => label).set(counts.finished as f64);
        metrics::gauge!("rustfs_background_foreground_window_failures", "class" => label).set(counts.failed as f64);
        metrics::gauge!("rustfs_background_foreground_window_cancellations", "class" => label).set(counts.cancelled as f64);
        if let Some(bounds) = counts.p99 {
            metrics::gauge!("rustfs_background_foreground_window_p99_lower_nanoseconds", "class" => label)
                .set(bounds.lower_nanos as f64);
            if let Some(upper) = bounds.upper_nanos {
                metrics::gauge!("rustfs_background_foreground_window_p99_upper_nanoseconds", "class" => label).set(upper as f64);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};
    use rustfs_concurrency::foreground::{ClassCompletions, LatencyBounds, WindowBoundary};
    use std::time::Instant;

    #[test]
    fn unavailable_windows_clear_availability_without_fabricating_values() {
        for stage in 0..4 {
            let recorder = DebuggingRecorder::new();
            let now = Instant::now();
            let mut window = ForegroundWindow {
                start: WindowBoundary {
                    earliest: now,
                    latest: now,
                },
                end: WindowBoundary {
                    earliest: now,
                    latest: now,
                },
                started: Some(100),
                in_flight: Some(0),
                classes: [Some(ClassCompletions {
                    finished: 100,
                    failed: 0,
                    cancelled: 0,
                    p99: Some(LatencyBounds {
                        lower_nanos: 1_001,
                        upper_nanos: Some(2_000),
                    }),
                }); 4],
                valid: true,
            };
            metrics::with_local_recorder(&recorder, || {
                record_window(window);
                if stage == 1 {
                    window.classes[0].as_mut().expect("known read class").p99 = None;
                    record_window(window);
                }
                if stage == 2 {
                    window.classes[0] = None;
                    window.in_flight = None;
                    window.started = None;
                    window.valid = false;
                    record_window(window);
                }
                if stage == 3 {
                    invalidate_window(window);
                }
            });
            let values = recorder.snapshotter().snapshot().into_vec();
            let gauge = |suffix: &str| {
                let name = format!("rustfs_background_foreground_window_{suffix}");
                values.iter().find_map(|(key, _, _, value)| {
                    (key.key().name() == name
                        && key
                            .key()
                            .labels()
                            .any(|label| label.key() == "class" && label.value() == "read"))
                    .then(|| match value {
                        DebugValue::Gauge(value) => value.into_inner(),
                        _ => panic!("expected gauge {name}"),
                    })
                })
            };
            assert_eq!(gauge("class_available"), Some(if stage < 2 { 1.0 } else { 0.0 }));
            assert_eq!(gauge("p99_available"), Some(if stage == 0 { 1.0 } else { 0.0 }));
            assert_eq!(gauge("p99_upper_available"), Some(if stage == 0 { 1.0 } else { 0.0 }));
            assert_eq!(gauge("p99_lower_nanoseconds"), Some(1_001.0));
            assert_eq!(gauge("p99_upper_nanoseconds"), Some(2_000.0));
            assert!(values.iter().any(|(key, _, _, value)| {
                key.key().name() == "rustfs_background_foreground_window_valid"
                    && matches!(value, DebugValue::Gauge(value) if value.into_inner() == if stage < 2 { 1.0 } else { 0.0 })
            }));
        }
    }

    #[tokio::test(start_paused = true)]
    async fn sampler_cancellation_disables_recording_without_resetting_accounting() {
        let recorder = DebuggingRecorder::new();
        let _metrics = metrics::set_default_local_recorder(&recorder);
        let cancelled = CancellationToken::new();
        cancelled.cancel();
        assert!(start_sampler(cancelled).is_none());
        assert!(start_request().is_none());
        let cancel = CancellationToken::new();
        let worker = start_sampler(cancel.clone()).expect("first live sampler");
        let request = start_request().expect("recording enabled before first sampler poll");
        drop(request);
        tokio::time::sleep(Duration::from_millis(1)).await;
        let window = latest_window().expect("sampler publishes actual events before shutdown");
        assert!(window.valid);
        assert_eq!((window.started, window.in_flight), (Some(1), Some(0)));
        assert_eq!(window.classes[3].expect("undispatched request").cancelled, 1);
        // Model a prior export without changing the process-global metrics gate.
        record_window(window);
        cancel.cancel();
        tokio::time::timeout(Duration::from_secs(1), worker)
            .await
            .expect("bounded shutdown")
            .expect("sampler task");
        assert!(start_request().is_none());
        assert!(latest_window().is_none());
        let values = recorder.snapshotter().snapshot().into_vec();
        assert!(
            values.iter().any(|(key, _, _, value)| {
                key.key().name() == "rustfs_background_foreground_window_valid"
                    && matches!(value, DebugValue::Gauge(value) if value.into_inner() == 0.0)
            }),
            "shutdown must invalidate earlier exported windows"
        );
        assert!(
            start_sampler(CancellationToken::new()).is_none(),
            "never reset carried in-flight accounting"
        );
    }
}
