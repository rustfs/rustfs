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

//! Capacity Metrics for monitoring

use metrics::{counter, gauge, histogram};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tracing::info;

/// Capacity metrics for monitoring
#[derive(Debug, Default)]
pub struct CapacityMetrics {
    /// Cache hit count
    pub cache_hits: AtomicU64,
    /// Cache miss count
    pub cache_misses: AtomicU64,
    /// Scheduled update count
    pub scheduled_updates: AtomicU64,
    /// Write triggered update count
    pub write_triggered_updates: AtomicU64,
    /// Update failure count
    pub update_failures: AtomicU64,
    /// Total update duration in microseconds
    pub total_update_duration_us: AtomicU64,
    /// Update count for average calculation
    pub update_count: AtomicU64,
    /// Symlink count encountered during capacity calculation
    pub symlink_count: AtomicU64,
    /// Total size of symlink targets
    pub symlink_size: AtomicU64,
    /// Dynamic timeout usage count
    pub dynamic_timeout_count: AtomicU64,
    /// Timeout fallback to sampling count
    pub timeout_fallback_count: AtomicU64,
    /// Stall detection count
    pub stall_detected_count: AtomicU64,
}

impl CapacityMetrics {
    /// Create new metrics
    pub fn new() -> Self {
        Self::default()
    }

    /// Record cache hit
    pub fn record_cache_hit(&self) {
        self.cache_hits.fetch_add(1, Ordering::Relaxed);
        counter!("rustfs.capacity.cache.hits").increment(1);
    }

    /// Record cache miss
    pub fn record_cache_miss(&self) {
        self.cache_misses.fetch_add(1, Ordering::Relaxed);
        counter!("rustfs.capacity.cache.misses").increment(1);
    }

    /// Record scheduled update
    #[allow(dead_code)]
    pub fn record_scheduled_update(&self) {
        self.scheduled_updates.fetch_add(1, Ordering::Relaxed);
        counter!("rustfs.capacity.update.scheduled").increment(1);
    }

    /// Record write triggered update
    #[allow(dead_code)]
    pub fn record_write_triggered_update(&self) {
        self.write_triggered_updates.fetch_add(1, Ordering::Relaxed);
        counter!("rustfs.capacity.update.write_triggered").increment(1);
    }

    /// Record update failure
    #[allow(dead_code)]
    pub fn record_update_failure(&self) {
        self.update_failures.fetch_add(1, Ordering::Relaxed);
        counter!("rustfs.capacity.update.failures").increment(1);
    }

    /// Record write operation
    #[allow(dead_code)]
    pub fn record_write_operation(&self) {
        counter!("rustfs.capacity.write.operations").increment(1);
    }

    /// Record symlink encountered
    pub fn record_symlink(&self, size: u64) {
        self.symlink_count.fetch_add(1, Ordering::Relaxed);
        self.symlink_size.fetch_add(size, Ordering::Relaxed);
        counter!("rustfs.capacity.symlinks.encountered").increment(1);
        gauge!("rustfs.capacity.symlinks.total_size").set(size as f64);
    }

    /// Record dynamic timeout usage
    pub fn record_dynamic_timeout(&self) {
        self.dynamic_timeout_count.fetch_add(1, Ordering::Relaxed);
        counter!("rustfs.capacity.timeout.dynamic").increment(1);
    }

    /// Record timeout fallback to sampling
    pub fn record_timeout_fallback(&self) {
        self.timeout_fallback_count.fetch_add(1, Ordering::Relaxed);
        counter!("rustfs.capacity.timeout.fallback").increment(1);
    }

    /// Record stall detection
    pub fn record_stall_detected(&self) {
        self.stall_detected_count.fetch_add(1, Ordering::Relaxed);
        counter!("rustfs.capacity.timeout.stall").increment(1);
    }

    /// Get symlink statistics
    #[allow(dead_code)]
    pub fn get_symlink_stats(&self) -> (u64, u64) {
        (self.symlink_count.load(Ordering::Relaxed), self.symlink_size.load(Ordering::Relaxed))
    }

    /// Get timeout statistics
    #[allow(dead_code)]
    pub fn get_timeout_stats(&self) -> (u64, u64, u64) {
        (
            self.dynamic_timeout_count.load(Ordering::Relaxed),
            self.timeout_fallback_count.load(Ordering::Relaxed),
            self.stall_detected_count.load(Ordering::Relaxed),
        )
    }

    /// Record update duration
    #[allow(dead_code)]
    pub fn record_update_duration(&self, duration: Duration) {
        let duration_us = duration.as_micros() as u64;
        self.total_update_duration_us.fetch_add(duration_us, Ordering::Relaxed);
        self.update_count.fetch_add(1, Ordering::Relaxed);

        histogram!("rustfs.capacity.update.duration_us").record(duration_us as f64);
    }

    /// Get cache hit rate
    pub fn get_cache_hit_rate(&self) -> f64 {
        let hits = self.cache_hits.load(Ordering::Relaxed);
        let misses = self.cache_misses.load(Ordering::Relaxed);
        let total = hits + misses;
        if total == 0 { 0.0 } else { hits as f64 / total as f64 }
    }

    /// Get average update duration
    pub fn get_avg_update_duration(&self) -> Duration {
        let total_us = self.total_update_duration_us.load(Ordering::Relaxed);
        let count = self.update_count.load(Ordering::Relaxed);
        if count == 0 {
            Duration::from_secs(0)
        } else {
            Duration::from_micros(total_us / count)
        }
    }

    /// Get metrics summary
    pub fn get_summary(&self) -> MetricsSummary {
        MetricsSummary {
            cache_hits: self.cache_hits.load(Ordering::Relaxed),
            cache_misses: self.cache_misses.load(Ordering::Relaxed),
            cache_hit_rate: self.get_cache_hit_rate(),
            scheduled_updates: self.scheduled_updates.load(Ordering::Relaxed),
            write_triggered_updates: self.write_triggered_updates.load(Ordering::Relaxed),
            update_failures: self.update_failures.load(Ordering::Relaxed),
            avg_update_duration: self.get_avg_update_duration(),
            symlink_count: self.symlink_count.load(Ordering::Relaxed),
            symlink_size: self.symlink_size.load(Ordering::Relaxed),
            dynamic_timeout_count: self.dynamic_timeout_count.load(Ordering::Relaxed),
            timeout_fallback_count: self.timeout_fallback_count.load(Ordering::Relaxed),
            stall_detected_count: self.stall_detected_count.load(Ordering::Relaxed),
        }
    }

    /// Log metrics summary
    pub fn log_summary(&self) {
        let summary = self.get_summary();

        // Update gauges for current values
        gauge!("rustfs.capacity.cache.hit_rate").set(summary.cache_hit_rate);
        gauge!("rustfs.capacity.cache.hits_total").set(summary.cache_hits as f64);
        gauge!("rustfs.capacity.cache.misses_total").set(summary.cache_misses as f64);
        gauge!("rustfs.capacity.update.scheduled_total").set(summary.scheduled_updates as f64);
        gauge!("rustfs.capacity.update.write_triggered_total").set(summary.write_triggered_updates as f64);
        gauge!("rustfs.capacity.update.failures_total").set(summary.update_failures as f64);
        gauge!("rustfs.capacity.symlinks.count").set(summary.symlink_count as f64);
        gauge!("rustfs.capacity.symlinks.size").set(summary.symlink_size as f64);
        gauge!("rustfs.capacity.timeout.dynamic_total").set(summary.dynamic_timeout_count as f64);
        gauge!("rustfs.capacity.timeout.fallback_total").set(summary.timeout_fallback_count as f64);
        gauge!("rustfs.capacity.timeout.stall_total").set(summary.stall_detected_count as f64);

        info!(
            "Capacity Metrics: cache_hit_rate={:.2}%, cache_hits={}, cache_misses={}, scheduled_updates={}, write_triggered_updates={}, update_failures={}, avg_update_duration={:?}, symlinks={}, symlink_size={}, dynamic_timeouts={}, timeout_fallbacks={}, stalls={}",
            summary.cache_hit_rate * 100.0,
            summary.cache_hits,
            summary.cache_misses,
            summary.scheduled_updates,
            summary.write_triggered_updates,
            summary.update_failures,
            summary.avg_update_duration,
            summary.symlink_count,
            summary.symlink_size,
            summary.dynamic_timeout_count,
            summary.timeout_fallback_count,
            summary.stall_detected_count
        );
    }
}

/// Metrics summary
#[derive(Debug, Clone)]
pub struct MetricsSummary {
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub cache_hit_rate: f64,
    pub scheduled_updates: u64,
    pub write_triggered_updates: u64,
    pub update_failures: u64,
    pub avg_update_duration: Duration,
    pub symlink_count: u64,
    pub symlink_size: u64,
    pub dynamic_timeout_count: u64,
    pub timeout_fallback_count: u64,
    pub stall_detected_count: u64,
}

/// Global metrics instance
static CAPACITY_METRICS: std::sync::OnceLock<Arc<CapacityMetrics>> = std::sync::OnceLock::new();

/// Get global metrics
pub fn get_capacity_metrics() -> Arc<CapacityMetrics> {
    CAPACITY_METRICS.get_or_init(|| Arc::new(CapacityMetrics::new())).clone()
}

/// Start metrics logging task
pub async fn start_metrics_logging(interval: Duration) {
    let metrics = get_capacity_metrics();

    tokio::spawn(async move {
        let mut timer = tokio::time::interval(interval);

        loop {
            timer.tick().await;
            metrics.log_summary();
        }
    });
}

/// Record a write operation globally
#[allow(dead_code)]
pub fn record_global_write_operation() {
    let metrics = get_capacity_metrics();
    metrics.record_write_operation();
}

/// Record cache hit globally
#[allow(dead_code)]
pub fn record_global_cache_hit() {
    let metrics = get_capacity_metrics();
    metrics.record_cache_hit();
}

/// Record cache miss globally
#[allow(dead_code)]
pub fn record_global_cache_miss() {
    let metrics = get_capacity_metrics();
    metrics.record_cache_miss();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_creation() {
        let metrics = CapacityMetrics::new();
        assert_eq!(metrics.cache_hits.load(Ordering::Relaxed), 0);
        assert_eq!(metrics.cache_misses.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn test_record_cache_hit() {
        let metrics = CapacityMetrics::new();
        metrics.record_cache_hit();
        metrics.record_cache_hit();
        assert_eq!(metrics.cache_hits.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn test_cache_hit_rate() {
        let metrics = CapacityMetrics::new();
        metrics.record_cache_hit();
        metrics.record_cache_hit();
        metrics.record_cache_miss();

        let rate = metrics.get_cache_hit_rate();
        assert!((rate - 0.6666666666666666).abs() < 0.0001);
    }

    #[test]
    fn test_avg_update_duration() {
        let metrics = CapacityMetrics::new();
        metrics.record_update_duration(Duration::from_millis(100));
        metrics.record_update_duration(Duration::from_millis(200));

        let avg = metrics.get_avg_update_duration();
        assert_eq!(avg, Duration::from_millis(150));
    }

    #[test]
    fn test_get_summary() {
        let metrics = CapacityMetrics::new();
        metrics.record_cache_hit();
        metrics.record_scheduled_update();
        metrics.record_update_duration(Duration::from_millis(100));

        let summary = metrics.get_summary();
        assert_eq!(summary.cache_hits, 1);
        assert_eq!(summary.scheduled_updates, 1);
        assert_eq!(summary.avg_update_duration, Duration::from_millis(100));
        assert_eq!(summary.symlink_count, 0);
        assert_eq!(summary.dynamic_timeout_count, 0);
    }

    #[test]
    fn test_record_write_operation() {
        let metrics = CapacityMetrics::new();
        metrics.record_write_operation();
        metrics.record_write_operation();
        // This test just ensures the method doesn't panic
        assert_eq!(metrics.write_triggered_updates.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn test_record_symlink() {
        let metrics = CapacityMetrics::new();
        metrics.record_symlink(1024);
        metrics.record_symlink(2048);

        let (count, size) = metrics.get_symlink_stats();
        assert_eq!(count, 2);
        assert_eq!(size, 3072);
    }

    #[test]
    fn test_record_dynamic_timeout() {
        let metrics = CapacityMetrics::new();
        metrics.record_dynamic_timeout();
        metrics.record_dynamic_timeout();

        let (dynamic, fallback, stalls) = metrics.get_timeout_stats();
        assert_eq!(dynamic, 2);
        assert_eq!(fallback, 0);
        assert_eq!(stalls, 0);
    }

    #[test]
    fn test_record_timeout_fallback() {
        let metrics = CapacityMetrics::new();
        metrics.record_timeout_fallback();
        metrics.record_stall_detected();

        let (dynamic, fallback, stalls) = metrics.get_timeout_stats();
        assert_eq!(dynamic, 0);
        assert_eq!(fallback, 1);
        assert_eq!(stalls, 1);
    }
}
