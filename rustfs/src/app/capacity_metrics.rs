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

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tracing::{info, warn};

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
}

impl CapacityMetrics {
    /// Create new metrics
    pub fn new() -> Self {
        Self::default()
    }

    /// Record cache hit
    pub fn record_cache_hit(&self) {
        self.cache_hits.fetch_add(1, Ordering::Relaxed);
    }

    /// Record cache miss
    pub fn record_cache_miss(&self) {
        self.cache_misses.fetch_add(1, Ordering::Relaxed);
    }

    /// Record scheduled update
    pub fn record_scheduled_update(&self) {
        self.scheduled_updates.fetch_add(1, Ordering::Relaxed);
    }

    /// Record write triggered update
    pub fn record_write_triggered_update(&self) {
        self.write_triggered_updates.fetch_add(1, Ordering::Relaxed);
    }

    /// Record update failure
    pub fn record_update_failure(&self) {
        self.update_failures.fetch_add(1, Ordering::Relaxed);
    }

    /// Record update duration
    pub fn record_update_duration(&self, duration: Duration) {
        let duration_us = duration.as_micros() as u64;
        self.total_update_duration_us.fetch_add(duration_us, Ordering::Relaxed);
        self.update_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Get cache hit rate
    pub fn get_cache_hit_rate(&self) -> f64 {
        let hits = self.cache_hits.load(Ordering::Relaxed);
        let misses = self.cache_misses.load(Ordering::Relaxed);
        let total = hits + misses;
        if total == 0 {
            0.0
        } else {
            hits as f64 / total as f64
        }
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
        }
    }

    /// Log metrics summary
    pub fn log_summary(&self) {
        let summary = self.get_summary();
        info!(
            "Capacity Metrics: cache_hit_rate={:.2}%, cache_hits={}, cache_misses={}, scheduled_updates={}, write_triggered_updates={}, update_failures={}, avg_update_duration={:?}",
            summary.cache_hit_rate * 100.0,
            summary.cache_hits,
            summary.cache_misses,
            summary.scheduled_updates,
            summary.write_triggered_updates,
            summary.update_failures,
            summary.avg_update_duration
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
}

/// Global metrics instance
static CAPACITY_METRICS: std::sync::OnceLock<Arc<CapacityMetrics>> = std::sync::OnceLock::new();

/// Get global metrics
pub fn get_capacity_metrics() -> Arc<CapacityMetrics> {
    CAPACITY_METRICS
        .get_or_init(|| Arc::new(CapacityMetrics::new()))
        .clone()
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
    }
}
