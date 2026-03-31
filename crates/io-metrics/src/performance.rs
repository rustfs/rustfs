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

//! Performance metrics structure with atomic counters.
//!
//! Provides a shared metrics instance that can be used across all RustFS
//! components for consistent performance monitoring.
//!
//! # Example
//!
//! ```rust
//! use rustfs_io_metrics::PerformanceMetrics;
//!
//! let metrics = PerformanceMetrics::new();
//! metrics.record_cache_hit();
//! metrics.record_bytes_read(1024);
//! ```

use std::sync::atomic::{AtomicU64, Ordering};

/// Performance metrics with atomic counters.
///
/// Thread-safe metrics structure that can be shared across threads.
/// All fields use atomic operations for lock-free access.
#[derive(Debug)]
pub struct PerformanceMetrics {
    // ===== Cache Metrics =====
    /// Total cache hits (all levels)
    pub cache_hits: AtomicU64,
    /// Total cache misses
    pub cache_misses: AtomicU64,
    /// L1 cache hits (hot objects < 1MB)
    pub l1_cache_hits: AtomicU64,
    /// L2 cache hits (standard objects < 10MB)
    pub l2_cache_hits: AtomicU64,

    // ===== I/O Metrics =====
    /// Total bytes read from disk
    pub total_bytes_read: AtomicU64,
    /// Total bytes written to disk
    pub total_bytes_written: AtomicU64,
    /// Disk read operation count
    pub disk_read_count: AtomicU64,
    /// Disk write operation count
    pub disk_write_count: AtomicU64,
    /// Average I/O latency in microseconds
    pub avg_io_latency_us: AtomicU64,
    /// P95 I/O latency in microseconds
    pub p95_io_latency_us: AtomicU64,
    /// P99 I/O latency in microseconds
    pub p99_io_latency_us: AtomicU64,

    // ===== Concurrency Metrics =====
    /// Current concurrent requests
    pub current_concurrent_requests: AtomicU64,
    /// Peak concurrent requests
    pub peak_concurrent_requests: AtomicU64,

    // ===== Error Metrics =====
    /// Total errors
    pub total_errors: AtomicU64,
    /// Timeout errors
    pub timeout_errors: AtomicU64,
    /// Disk errors
    pub disk_errors: AtomicU64,
}

impl PerformanceMetrics {
    /// Create a new PerformanceMetrics instance with all values initialized to zero.
    pub fn new() -> Self {
        Self {
            cache_hits: AtomicU64::new(0),
            cache_misses: AtomicU64::new(0),
            l1_cache_hits: AtomicU64::new(0),
            l2_cache_hits: AtomicU64::new(0),
            total_bytes_read: AtomicU64::new(0),
            total_bytes_written: AtomicU64::new(0),
            disk_read_count: AtomicU64::new(0),
            disk_write_count: AtomicU64::new(0),
            avg_io_latency_us: AtomicU64::new(0),
            p95_io_latency_us: AtomicU64::new(0),
            p99_io_latency_us: AtomicU64::new(0),
            current_concurrent_requests: AtomicU64::new(0),
            peak_concurrent_requests: AtomicU64::new(0),
            total_errors: AtomicU64::new(0),
            timeout_errors: AtomicU64::new(0),
            disk_errors: AtomicU64::new(0),
        }
    }

    /// Calculate the cache hit rate (0.0 to 1.0).
    ///
    /// Returns 0.0 if there have been no cache accesses.
    pub fn cache_hit_rate(&self) -> f64 {
        let hits = self.cache_hits.load(Ordering::Relaxed);
        let misses = self.cache_misses.load(Ordering::Relaxed);
        let total = hits + misses;

        if total == 0 { 0.0 } else { hits as f64 / total as f64 }
    }

    /// Get the L1 cache hit rate (0.0 to 1.0).
    ///
    /// Returns the ratio of L1 hits to total cache hits.
    pub fn l1_hit_rate(&self) -> f64 {
        let l1_hits = self.l1_cache_hits.load(Ordering::Relaxed);
        let total_hits = self.cache_hits.load(Ordering::Relaxed);

        if total_hits == 0 {
            0.0
        } else {
            l1_hits as f64 / total_hits as f64
        }
    }

    // ===== Cache Recording Methods =====

    /// Record a cache hit.
    #[inline]
    pub fn record_cache_hit(&self) {
        self.cache_hits.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a cache miss.
    #[inline]
    pub fn record_cache_miss(&self) {
        self.cache_misses.fetch_add(1, Ordering::Relaxed);
    }

    /// Record an L1 cache hit (includes recording total cache hit).
    #[inline]
    pub fn record_l1_hit(&self) {
        self.l1_cache_hits.fetch_add(1, Ordering::Relaxed);
        self.record_cache_hit();
    }

    /// Record an L2 cache hit (includes recording total cache hit).
    #[inline]
    pub fn record_l2_hit(&self) {
        self.l2_cache_hits.fetch_add(1, Ordering::Relaxed);
        self.record_cache_hit();
    }

    // ===== I/O Recording Methods =====

    /// Record bytes read.
    #[inline]
    pub fn record_bytes_read(&self, bytes: u64) {
        self.total_bytes_read.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Record bytes written.
    #[inline]
    pub fn record_bytes_written(&self, bytes: u64) {
        self.total_bytes_written.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Record a disk read operation.
    #[inline]
    pub fn record_disk_read(&self) {
        self.disk_read_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a disk write operation.
    #[inline]
    pub fn record_disk_write(&self) {
        self.disk_write_count.fetch_add(1, Ordering::Relaxed);
    }

    // ===== Concurrency Recording Methods =====

    /// Update concurrent request count and track peak.
    #[inline]
    pub fn update_concurrent_requests(&self, count: u64) {
        self.current_concurrent_requests.store(count, Ordering::Relaxed);

        // Update peak using lock-free CAS loop
        let mut peak = self.peak_concurrent_requests.load(Ordering::Relaxed);
        loop {
            if count <= peak {
                break;
            }
            match self
                .peak_concurrent_requests
                .compare_exchange_weak(peak, count, Ordering::Relaxed, Ordering::Relaxed)
            {
                Ok(_) => break,
                Err(new_peak) => peak = new_peak,
            }
        }
    }

    // ===== Error Recording Methods =====

    /// Record a generic error.
    #[inline]
    pub fn record_error(&self) {
        self.total_errors.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a timeout error (includes recording total error).
    #[inline]
    pub fn record_timeout(&self) {
        self.timeout_errors.fetch_add(1, Ordering::Relaxed);
        self.record_error();
    }

    /// Record a disk error (includes recording total error).
    #[inline]
    pub fn record_disk_error(&self) {
        self.disk_errors.fetch_add(1, Ordering::Relaxed);
        self.record_error();
    }
}

impl Default for PerformanceMetrics {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_creation() {
        let metrics = PerformanceMetrics::new();
        assert_eq!(metrics.cache_hits.load(Ordering::Relaxed), 0);
        assert_eq!(metrics.cache_misses.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn test_cache_hit_rate() {
        let metrics = PerformanceMetrics::new();

        // No accesses yet
        assert_eq!(metrics.cache_hit_rate(), 0.0);

        // Record some hits and misses
        for _ in 0..5 {
            metrics.record_cache_hit();
        }
        for _ in 0..3 {
            metrics.record_cache_miss();
        }

        assert_eq!(metrics.cache_hit_rate(), 5.0 / 8.0);
    }

    #[test]
    fn test_l1_hit_rate() {
        let metrics = PerformanceMetrics::new();

        metrics.record_l1_hit(); // Records both L1 and total
        metrics.record_l2_hit(); // Records L2 and total
        metrics.record_cache_hit(); // Direct total hit

        assert_eq!(metrics.cache_hits.load(Ordering::Relaxed), 3);
        assert_eq!(metrics.l1_cache_hits.load(Ordering::Relaxed), 1);
        assert_eq!(metrics.l1_hit_rate(), 1.0 / 3.0);
    }

    #[test]
    fn test_io_recording() {
        let metrics = PerformanceMetrics::new();

        metrics.record_bytes_read(1024 * 1024); // 1MB
        metrics.record_disk_read();

        assert_eq!(metrics.total_bytes_read.load(Ordering::Relaxed), 1024 * 1024);
        assert_eq!(metrics.disk_read_count.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_concurrent_tracking() {
        let metrics = PerformanceMetrics::new();

        metrics.update_concurrent_requests(5);
        assert_eq!(metrics.current_concurrent_requests.load(Ordering::Relaxed), 5);
        assert_eq!(metrics.peak_concurrent_requests.load(Ordering::Relaxed), 5);

        metrics.update_concurrent_requests(3);
        assert_eq!(metrics.current_concurrent_requests.load(Ordering::Relaxed), 3);
        assert_eq!(metrics.peak_concurrent_requests.load(Ordering::Relaxed), 5); // Peak stays at 5
    }

    #[test]
    fn test_error_recording() {
        let metrics = PerformanceMetrics::new();

        metrics.record_timeout();
        assert_eq!(metrics.total_errors.load(Ordering::Relaxed), 1);
        assert_eq!(metrics.timeout_errors.load(Ordering::Relaxed), 1);

        metrics.record_disk_error();
        assert_eq!(metrics.total_errors.load(Ordering::Relaxed), 2);
        assert_eq!(metrics.disk_errors.load(Ordering::Relaxed), 1);
    }
}
