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

//! Metrics collector for I/O operation tracking and latency analysis.
//!
//! Provides latency percentile calculation (P50, P95, P99) and automatic
//! reporting to the `metrics` crate for OTEL export.

use super::performance::PerformanceMetrics;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;
use tokio::sync::RwLock;

/// How many recorded operations elapse between P95/P99 recomputations.
///
/// The sliding-window mean (`avg`) is refreshed on every operation — it is O(1)
/// and the autotuner reads it — but the percentiles need an O(n log n) sort of
/// the window, so they are recomputed at most once per this many operations.
/// Both consumers (the autotuner tick and OTEL export) sample on their own
/// periodic cadence, never per-IO, so a bounded lag here is invisible to them
/// while the per-op sort cost is amortised away.
const PERCENTILE_RECOMPUTE_INTERVAL: u32 = 128;

/// Sliding window of I/O latency samples plus a running sum, so the window mean
/// is maintained in O(1) as samples enter and leave.
struct LatencyWindow {
    samples: VecDeque<Duration>,
    /// Sum of `samples` in microseconds, kept in step with every push/pop.
    sum_micros: u128,
}

/// Metrics collector for tracking I/O operations and computing latency percentiles.
///
/// Maintains a sliding window of I/O latency samples and updates P95/P99 metrics.
/// Automatically reports to the `metrics` crate for OTEL export.
pub struct MetricsCollector {
    /// The underlying metrics (shared reference)
    metrics: Arc<PerformanceMetrics>,
    /// Sliding window of I/O latency samples (+ running sum) for mean/percentile calculation
    io_latency: RwLock<LatencyWindow>,
    /// Maximum number of latency samples to keep
    max_latency_samples: usize,
    /// Operations recorded since the last P95/P99 recompute (throttle counter).
    ops_since_percentile: AtomicU32,
}

impl MetricsCollector {
    /// Create a new metrics collector.
    ///
    /// # Arguments
    ///
    /// * `metrics` - The underlying metrics structure to update
    /// * `max_latency_samples` - Maximum number of latency samples to keep for percentile calculation
    pub fn new(metrics: Arc<PerformanceMetrics>, max_latency_samples: usize) -> Self {
        Self {
            metrics,
            io_latency: RwLock::new(LatencyWindow {
                samples: VecDeque::new(),
                sum_micros: 0,
            }),
            max_latency_samples,
            ops_since_percentile: AtomicU32::new(0),
        }
    }

    /// Create a new metrics collector with default settings (1000 max samples).
    pub fn with_default_max_samples(metrics: Arc<PerformanceMetrics>) -> Self {
        Self::new(metrics, 1000)
    }

    /// Record an I/O operation with its duration.
    ///
    /// This method:
    /// 1. Updates byte counters in PerformanceMetrics
    /// 2. Updates operation counters in PerformanceMetrics
    /// 3. Records latency for P95/P99 calculation
    /// 4. Reports to the `metrics` crate for OTEL export
    ///
    /// # Arguments
    ///
    /// * `bytes` - Number of bytes transferred
    /// * `duration` - Duration of the I/O operation
    /// * `is_read` - true for read operations, false for writes
    pub async fn record_io_operation(&self, bytes: u64, duration: Duration, is_read: bool) {
        // Update byte counters in PerformanceMetrics
        if is_read {
            self.metrics.record_bytes_read(bytes);
        } else {
            self.metrics.record_bytes_written(bytes);
        }

        // Update operation counters in PerformanceMetrics
        if is_read {
            self.metrics.record_disk_read();
        } else {
            self.metrics.record_disk_write();
        }

        // Report to metrics crate for OTEL export
        crate::record_data_transfer(bytes, duration.as_millis() as f64);

        // Update the sliding window and the O(1) running mean under a short write
        // lock, and decide — still under the lock, so the count is exact — whether
        // this op crosses the percentile-recompute throttle.
        let (mean_us, recompute_percentiles) = {
            let mut window = self.io_latency.write().await;
            window.samples.push_back(duration);
            window.sum_micros += duration.as_micros();

            // Keep only the most recent samples (O(1) removal from front).
            if window.samples.len() > self.max_latency_samples
                && let Some(old) = window.samples.pop_front()
            {
                window.sum_micros -= old.as_micros();
            }

            let len = window.samples.len() as u128;
            let mean_us = window.sum_micros.checked_div(len).unwrap_or(0) as u64;

            let n = self.ops_since_percentile.fetch_add(1, Ordering::Relaxed) + 1;
            let recompute = n >= PERCENTILE_RECOMPUTE_INTERVAL;
            if recompute {
                self.ops_since_percentile.store(0, Ordering::Relaxed);
            }
            (mean_us, recompute)
        };

        // The mean is cheap and the autotuner reads it, so refresh it every op.
        self.metrics.avg_io_latency_us.store(mean_us, Ordering::Relaxed);
        crate::record_io_latency(mean_us as f64 / 1000.0); // Convert to ms

        // The P95/P99 sort is the expensive part and only feeds OTEL export, so it
        // is throttled to once per PERCENTILE_RECOMPUTE_INTERVAL operations.
        if recompute_percentiles {
            self.update_latency_percentiles().await;
        }
    }

    /// Recompute the P95/P99 latency percentiles from the current window.
    ///
    /// This sorts a snapshot of the window (O(n log n)), so it is driven on a
    /// throttle from the record path rather than per operation. The mean is not
    /// computed here — it is maintained in O(1) on every `record_io_operation`.
    async fn update_latency_percentiles(&self) {
        // Snapshot the window micros under a read lock, then sort outside the lock.
        let mut sorted: Vec<u128> = {
            let window = self.io_latency.read().await;
            if window.samples.is_empty() {
                return;
            }
            window.samples.iter().map(|d| d.as_micros()).collect()
        };
        sorted.sort_unstable();

        let len = sorted.len();

        // Calculate P95
        let p95_idx = ((len as f64) * 0.95) as usize;
        if let Some(&p95) = sorted.get(p95_idx.min(len - 1)) {
            self.metrics.p95_io_latency_us.store(p95 as u64, Ordering::Relaxed);
            crate::record_io_latency_p95(p95 as f64 / 1000.0);
        }

        // Calculate P99
        let p99_idx = ((len as f64) * 0.99) as usize;
        if let Some(&p99) = sorted.get(p99_idx.min(len - 1)) {
            self.metrics.p99_io_latency_us.store(p99 as u64, Ordering::Relaxed);
            crate::record_io_latency_p99(p99 as f64 / 1000.0);
        }
    }

    /// Get the number of recorded latency samples.
    pub async fn sample_count(&self) -> usize {
        self.io_latency.read().await.samples.len()
    }

    /// Get the maximum number of samples this collector will retain.
    pub fn max_samples(&self) -> usize {
        self.max_latency_samples
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_collector_creation() {
        let metrics = Arc::new(PerformanceMetrics::new());
        let collector = MetricsCollector::with_default_max_samples(metrics);
        assert_eq!(collector.max_samples(), 1000);
    }

    #[tokio::test]
    async fn test_record_io_basic() {
        let metrics = Arc::new(PerformanceMetrics::new());
        let collector = MetricsCollector::new(metrics.clone(), 10);

        collector.record_io_operation(1024, Duration::from_millis(10), true).await;

        assert_eq!(metrics.total_bytes_read.load(Ordering::Relaxed), 1024);
        assert_eq!(metrics.disk_read_count.load(Ordering::Relaxed), 1);
        assert_eq!(collector.sample_count().await, 1);
    }

    #[tokio::test]
    async fn test_latency_percentiles() {
        let metrics = Arc::new(PerformanceMetrics::new());
        let collector = MetricsCollector::new(metrics.clone(), 10);

        // Record some latencies
        collector.record_io_operation(0, Duration::from_micros(100), true).await;
        collector.record_io_operation(0, Duration::from_micros(200), true).await;
        collector.record_io_operation(0, Duration::from_micros(300), true).await;
        collector.record_io_operation(0, Duration::from_micros(400), true).await;
        collector.record_io_operation(0, Duration::from_micros(500), true).await;

        // The mean is refreshed on every op.
        let avg = metrics.avg_io_latency_us.load(Ordering::Relaxed);
        assert_eq!(avg, 300); // (100+200+300+400+500) / 5

        // P95/P99 are throttled off the record path; force a recompute to exercise
        // the percentile math directly.
        collector.update_latency_percentiles().await;
        let p95 = metrics.p95_io_latency_us.load(Ordering::Relaxed);
        let p99 = metrics.p99_io_latency_us.load(Ordering::Relaxed);

        // P95 should be close to 500 (5th element)
        // P99 should be 500 (same as max)
        assert!(p95 >= 400); // Allow some tolerance
        assert_eq!(p99, 500);
    }

    #[tokio::test]
    async fn test_percentiles_throttled_but_mean_updates_per_op() {
        let metrics = Arc::new(PerformanceMetrics::new());
        let collector = MetricsCollector::new(metrics.clone(), 1000);

        // Fewer ops than the recompute interval: the mean tracks every op, but the
        // percentiles must not have been recomputed yet (they stay at their init 0).
        for _ in 0..(PERCENTILE_RECOMPUTE_INTERVAL - 1) {
            collector.record_io_operation(0, Duration::from_micros(200), true).await;
        }
        assert_eq!(metrics.avg_io_latency_us.load(Ordering::Relaxed), 200);
        assert_eq!(
            metrics.p99_io_latency_us.load(Ordering::Relaxed),
            0,
            "percentiles must not be recomputed before the throttle interval"
        );

        // The op that crosses the interval triggers exactly one recompute.
        collector.record_io_operation(0, Duration::from_micros(200), true).await;
        assert_eq!(metrics.p99_io_latency_us.load(Ordering::Relaxed), 200);
    }

    #[tokio::test]
    async fn test_sample_limit() {
        let metrics = Arc::new(PerformanceMetrics::new());
        let collector = MetricsCollector::new(metrics.clone(), 5); // Max 5 samples

        // Record more than the limit
        for _ in 0..10 {
            collector.record_io_operation(0, Duration::from_millis(1), true).await;
        }

        // Should only keep 5 samples
        assert_eq!(collector.sample_count().await, 5);
    }

    #[tokio::test]
    async fn test_read_write_distinction() {
        let metrics = Arc::new(PerformanceMetrics::new());
        let collector = MetricsCollector::new(metrics.clone(), 10);

        collector.record_io_operation(1024, Duration::from_millis(10), true).await;
        collector.record_io_operation(2048, Duration::from_millis(5), false).await;

        assert_eq!(metrics.total_bytes_read.load(Ordering::Relaxed), 1024);
        assert_eq!(metrics.total_bytes_written.load(Ordering::Relaxed), 2048);
        assert_eq!(metrics.disk_read_count.load(Ordering::Relaxed), 1);
        assert_eq!(metrics.disk_write_count.load(Ordering::Relaxed), 1);
    }
}
