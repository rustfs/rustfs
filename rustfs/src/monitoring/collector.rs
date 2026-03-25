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

use super::metrics::PerformanceMetrics;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

/// Metrics collector for tracking I/O operations and computing latency percentiles.
///
/// Maintains a sliding window of I/O latency samples and updates P95/P99 metrics
/// for monitoring dashboards.
pub struct MetricsCollector {
    /// The underlying metrics (shared reference)
    metrics: Arc<PerformanceMetrics>,
    /// I/O latency samples for percentile calculation
    io_latency_samples: RwLock<Vec<Duration>>,
    /// Maximum number of latency samples to keep
    max_latency_samples: usize,
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
            io_latency_samples: RwLock::new(Vec::new()),
            max_latency_samples,
        }
    }

    /// Create a new metrics collector with default settings (1000 max samples).
    pub fn with_default_max_samples(metrics: Arc<PerformanceMetrics>) -> Self {
        Self::new(metrics, 1000)
    }

    /// Record an I/O operation with its duration.
    ///
    /// Updates byte counters, operation counters, and records latency for
    /// P95/P99 calculation.
    ///
    /// # Arguments
    ///
    /// * `bytes` - Number of bytes transferred
    /// * `duration` - Duration of the I/O operation
    /// * `is_read` - true for read operations, false for writes
    pub async fn record_io_operation(&self, bytes: u64, duration: Duration, is_read: bool) {
        // Update byte counters
        if is_read {
            self.metrics.record_bytes_read(bytes);
        } else {
            self.metrics.record_bytes_written(bytes);
        }

        // Update operation counters
        if is_read {
            self.metrics.record_disk_read();
        } else {
            self.metrics.record_disk_write();
        }

        // Record latency sample
        let mut samples = self.io_latency_samples.write().await;
        samples.push(duration);

        // Keep only the most recent samples
        if samples.len() > self.max_latency_samples {
            samples.remove(0);
        }

        // Update latency percentiles
        drop(samples); // Release write lock before calling update
        self.update_latency_percentiles().await;
    }

    /// Update the latency percentile metrics (P50, P95, P99).
    async fn update_latency_percentiles(&self) {
        let samples = self.io_latency_samples.read().await;
        if samples.is_empty() {
            return;
        }

        // Sort samples to calculate percentiles
        let mut sorted: Vec<_> = samples.iter().map(|d| d.as_micros()).collect();
        drop(samples); // Release read lock before sort
        sorted.sort();

        let len = sorted.len();

        // Average
        let sum: u128 = sorted.iter().sum();
        let avg = (sum / len as u128) as u64;
        self.metrics.avg_io_latency_us.store(avg, Ordering::Relaxed);

        // P95
        let p95_idx = ((len as f64) * 0.95) as usize;
        if let Some(&p95) = sorted.get(p95_idx.min(len - 1)) {
            self.metrics.p95_io_latency_us.store(p95 as u64, Ordering::Relaxed);
        }

        // P99
        let p99_idx = ((len as f64) * 0.99) as usize;
        if let Some(&p99) = sorted.get(p99_idx.min(len - 1)) {
            self.metrics.p99_io_latency_us.store(p99 as u64, Ordering::Relaxed);
        }
    }

    /// Get the number of recorded latency samples.
    pub async fn sample_count(&self) -> usize {
        self.io_latency_samples.read().await.len()
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

        // Check average
        let avg = metrics.avg_io_latency_us.load(Ordering::Relaxed);
        assert_eq!(avg, 300); // (100+200+300+400+500) / 5

        // Check percentiles
        let p95 = metrics.p95_io_latency_us.load(Ordering::Relaxed);
        let p99 = metrics.p99_io_latency_us.load(Ordering::Relaxed);

        // P95 should be close to 500 (5th element)
        // P99 should be 500 (same as max)
        assert!(p95 >= 400); // Allow some tolerance
        assert_eq!(p99, 500);
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
