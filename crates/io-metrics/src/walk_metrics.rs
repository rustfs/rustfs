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

//! Remote directory-walk activity. Scratch bytes describe Rust buffer capacity,
//! not resident memory or allocator-retained pages. No bucket, key, or peer labels.
//! Read/written totals cover metadata payload, exclude transport framing, and are
//! batched until the logical walk ends (including error/cancellation). Gauges
//! change immediately; completed-byte rates can lag a long-running walk.

use metrics::{Counter, Gauge, Histogram, counter, gauge, histogram};
use std::time::Instant;

/// One logical remote walk, including stream-open retries and timeout handling.
/// Captured handles balance gauges even if metric emission is later disabled.
#[derive(Default)]
#[must_use = "keep this guard alive for the logical remote walk"]
pub struct RemoteWalkGuard {
    inner: Option<Observation>,
}

struct Observation {
    active: Gauge,
    scratch: Gauge,
    read_bytes: Counter,
    written_bytes: Counter,
    read: u64,
    written: u64,
    outcome: &'static str,
    started: Instant,
    duration: Histogram,
}

impl RemoteWalkGuard {
    pub fn start() -> Self {
        if !crate::metrics_enabled() {
            return Self::default();
        }
        counter!("rustfs_remote_walk_started_total").increment(1);
        let active = gauge!("rustfs_remote_walk_active");
        active.increment(1.0);
        Self {
            inner: Some(Observation {
                active,
                scratch: gauge!("rustfs_remote_walk_scratch_bytes_current"),
                read_bytes: counter!("rustfs_remote_walk_read_bytes_total"),
                written_bytes: counter!("rustfs_remote_walk_written_bytes_total"),
                read: 0,
                written: 0,
                outcome: "cancelled",
                started: Instant::now(),
                duration: histogram!("rustfs_remote_walk_duration_seconds"),
            }),
        }
    }

    /// Track a copy buffer until the returned guard is dropped. Transport and
    /// downstream buffers are outside this gauge. The logical walk can outlive
    /// its scratch while timeout/error health handling is still pending.
    pub fn scratch_allocated(&self, capacity: usize) -> WalkScratchGuard {
        if let Some(inner) = &self.inner {
            inner.scratch.increment(capacity as f64);
            counter!("rustfs_remote_walk_scratch_allocations_total").increment(1);
            counter!("rustfs_remote_walk_scratch_allocated_bytes_total").increment(u64::try_from(capacity).unwrap_or(u64::MAX));
            histogram!("rustfs_remote_walk_scratch_capacity_bytes").record(capacity as f64);
            WalkScratchGuard {
                gauge: Some(inner.scratch.clone()),
                bytes: capacity,
            }
        } else {
            WalkScratchGuard { gauge: None, bytes: 0 }
        }
    }

    pub fn read(&mut self, bytes: usize) {
        if let Some(inner) = &mut self.inner {
            inner.read = inner.read.saturating_add(u64::try_from(bytes).unwrap_or(u64::MAX));
        }
    }

    /// Count only bytes accepted by the downstream writer, including partial
    /// writes before failure or cancellation. Emission is batched on drop.
    pub fn written(&mut self, bytes: usize) {
        if let Some(inner) = &mut self.inner {
            inner.written = inner.written.saturating_add(u64::try_from(bytes).unwrap_or(u64::MAX));
        }
    }

    pub fn finish(&mut self, success: bool) {
        if let Some(inner) = &mut self.inner {
            inner.outcome = if success { "ok" } else { "error" };
        }
    }
}

impl Drop for RemoteWalkGuard {
    fn drop(&mut self) {
        if let Some(inner) = &self.inner {
            inner.read_bytes.increment(inner.read);
            inner.written_bytes.increment(inner.written);
            inner.active.decrement(1.0);
            counter!("rustfs_remote_walk_completed_total", "outcome" => inner.outcome).increment(1);
            inner.duration.record(inner.started.elapsed().as_secs_f64());
        }
    }
}

/// Live scratch capacity, independent of the logical walk's completion.
#[must_use = "keep this guard alive for the lifetime of the walk copy buffer"]
pub struct WalkScratchGuard {
    gauge: Option<Gauge>,
    bytes: usize,
}

impl Drop for WalkScratchGuard {
    fn drop(&mut self) {
        if let Some(gauge) = &self.gauge {
            gauge.decrement(self.bytes as f64);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::{METRICS_FLAG_LOCK, counter_total, gauge_value, histogram_samples};
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};

    struct RestoreMetricsGate(bool);

    impl Drop for RestoreMetricsGate {
        fn drop(&mut self) {
            crate::set_metrics_enabled(self.0);
        }
    }

    #[test]
    fn overlapping_walks_balance_on_success_error_and_cancellation() {
        let _lock = METRICS_FLAG_LOCK.lock().expect("metrics gate lock");
        let _restore = RestoreMetricsGate(crate::metrics_enabled());
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        metrics::with_local_recorder(&recorder, || {
            crate::set_metrics_enabled(true);
            let mut success = RemoteWalkGuard::start();
            let mut error = RemoteWalkGuard::start();
            let cancelled = RemoteWalkGuard::start();
            let _success_buffer = success.scratch_allocated(16_384);
            let _error_buffer = error.scratch_allocated(16_384);
            success.read(10);
            success.written(10);
            success.finish(true);
            error.read(20);
            error.written(7);
            error.finish(false);
            // Changing the gate must not strand increments from existing walks.
            crate::set_metrics_enabled(false);
            drop((success, error, cancelled));
        });
        let rows = snapshotter.snapshot().into_vec();
        assert_eq!(gauge_value(&rows, "rustfs_remote_walk_active"), Some(0.0));
        assert_eq!(gauge_value(&rows, "rustfs_remote_walk_scratch_bytes_current"), Some(0.0));
        assert_eq!(counter_total(&rows, "rustfs_remote_walk_started_total"), Some(3));
        assert_eq!(counter_total(&rows, "rustfs_remote_walk_read_bytes_total"), Some(30));
        assert_eq!(counter_total(&rows, "rustfs_remote_walk_written_bytes_total"), Some(17));
        assert_eq!(counter_total(&rows, "rustfs_remote_walk_scratch_allocations_total"), Some(2));
        assert_eq!(counter_total(&rows, "rustfs_remote_walk_scratch_allocated_bytes_total"), Some(32_768));
        assert_eq!(histogram_samples(&rows, "rustfs_remote_walk_scratch_capacity_bytes"), vec![16_384.0; 2]);
        assert_eq!(histogram_samples(&rows, "rustfs_remote_walk_duration_seconds").len(), 3);
        for outcome in ["ok", "error", "cancelled"] {
            assert!(
                rows.iter().any(|(key, _, _, value)| {
                    key.key().name() == "rustfs_remote_walk_completed_total"
                        && key
                            .key()
                            .labels()
                            .any(|label| label.key() == "outcome" && label.value() == outcome)
                        && matches!(value, DebugValue::Counter(1))
                }),
                "missing exact completion for {outcome}"
            );
        }
    }

    #[test]
    fn live_walks_account_for_opening_and_allocated_scratch_separately() {
        let _lock = METRICS_FLAG_LOCK.lock().expect("metrics gate lock");
        let _restore = RestoreMetricsGate(crate::metrics_enabled());
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        metrics::with_local_recorder(&recorder, || {
            crate::set_metrics_enabled(true);
            let copying = RemoteWalkGuard::start();
            let _opening = RemoteWalkGuard::start();
            let _buffer = copying.scratch_allocated(16_384);
            let rows = snapshotter.snapshot().into_vec();
            assert_eq!(gauge_value(&rows, "rustfs_remote_walk_active"), Some(2.0));
            assert_eq!(gauge_value(&rows, "rustfs_remote_walk_scratch_bytes_current"), Some(16_384.0));
            // DebuggingRecorder snapshots consume values; final balance is
            // asserted in the separate test without an intermediate snapshot.
            crate::set_metrics_enabled(false);
        });
    }

    #[test]
    fn scratch_is_released_before_post_copy_walk_handling_finishes() {
        let _lock = METRICS_FLAG_LOCK.lock().expect("metrics gate lock");
        let _restore = RestoreMetricsGate(crate::metrics_enabled());
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        metrics::with_local_recorder(&recorder, || {
            crate::set_metrics_enabled(true);
            let walk = RemoteWalkGuard::start();
            let buffer = walk.scratch_allocated(16_384);
            drop(buffer);
            let rows = snapshotter.snapshot().into_vec();
            assert_eq!(gauge_value(&rows, "rustfs_remote_walk_active"), Some(1.0));
            assert_eq!(gauge_value(&rows, "rustfs_remote_walk_scratch_bytes_current"), Some(0.0));
            crate::set_metrics_enabled(false);
        });
    }

    #[test]
    fn disabled_walk_metrics_do_not_register_handles() {
        let _lock = METRICS_FLAG_LOCK.lock().expect("metrics gate lock");
        let _restore = RestoreMetricsGate(crate::metrics_enabled());
        crate::set_metrics_enabled(false);
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        metrics::with_local_recorder(&recorder, || {
            let mut walk = RemoteWalkGuard::start();
            let _buffer = walk.scratch_allocated(16_384);
            walk.read(9);
            walk.written(4);
            walk.finish(false);
        });
        assert!(snapshotter.snapshot().into_vec().is_empty());
    }
}
