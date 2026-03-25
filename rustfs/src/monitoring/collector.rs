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

//! Metrics collector for RustFS performance monitoring.

use super::metrics::PerformanceMetrics;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

/// Metrics collector for tracking I/O operations and performance.
///
/// This collector maintains samples of I/O latencies and computes
/// percentile values (P95, P99) for monitoring dashboards.
pub struct MetricsCollector {
    /// The underlying metrics
    metrics: Arc<PerformanceMetrics>,
    /// I/O latency samples for percentile calculation
    io_latency_samples: Arc<RwLock<Vec<Duration>>>,
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
            io_latency_samples: Arc::new(RwLock::new(Vec::new())),
            max_latency_samples,
        }
    }

    /// Create a new metrics collector with default settings.
    ///
    /// Uses a maximum of 1000 latency samples.
    pub fn with_default_max_samples(metrics: Arc<PerformanceMetrics>) -> Self {
        Self::new(metrics, 1000)
    }

    /// Record an I/O operation.
    ///
    /// This updates the metrics counters and records the latency for
    /// percentile calculation.
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

    /// Get a reference to the underlying metrics.
    pub fn metrics(&self) -> &Arc<PerformanceMetrics> {
        &self.metrics
    }
}

/// I/O operation type for metrics collection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IoOpType {
    /// Read operation
    Read,
    /// Write operation
    Write,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_collector_creation() {
        let metrics = Arc::new(PerformanceMetrics::new());
        let collector = MetricsCollector::with_default_max_samples(metrics);
        // Test synchronous creation
        assert_eq!(collector.max_latency_samples, 1000);
    }

    #[test]
    fn test_latency_samples_limit() {
        let metrics = Arc::new(PerformanceMetrics::new());
        let collector = MetricsCollector::new(metrics, 10);
        assert_eq!(collector.max_latency_samples, 10);
    }
}
