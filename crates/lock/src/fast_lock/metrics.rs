// Copyright 2024 RustFS Team

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Atomic metrics for lock operations
#[derive(Debug)]
pub struct ShardMetrics {
    pub fast_path_success: AtomicU64,
    pub slow_path_success: AtomicU64,
    pub timeouts: AtomicU64,
    pub releases: AtomicU64,
    pub cleanups: AtomicU64,
    pub contention_events: AtomicU64,
    pub total_wait_time_ns: AtomicU64,
    pub max_wait_time_ns: AtomicU64,
}

impl Default for ShardMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl ShardMetrics {
    pub fn new() -> Self {
        Self {
            fast_path_success: AtomicU64::new(0),
            slow_path_success: AtomicU64::new(0),
            timeouts: AtomicU64::new(0),
            releases: AtomicU64::new(0),
            cleanups: AtomicU64::new(0),
            contention_events: AtomicU64::new(0),
            total_wait_time_ns: AtomicU64::new(0),
            max_wait_time_ns: AtomicU64::new(0),
        }
    }

    pub fn record_fast_path_success(&self) {
        self.fast_path_success.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_slow_path_success(&self) {
        self.slow_path_success.fetch_add(1, Ordering::Relaxed);
        self.contention_events.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_timeout(&self) {
        self.timeouts.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_release(&self) {
        self.releases.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_cleanup(&self, count: usize) {
        self.cleanups.fetch_add(count as u64, Ordering::Relaxed);
    }

    pub fn record_wait_time(&self, wait_time: Duration) {
        let wait_ns = wait_time.as_nanos() as u64;
        self.total_wait_time_ns.fetch_add(wait_ns, Ordering::Relaxed);

        // Update max wait time
        let mut current_max = self.max_wait_time_ns.load(Ordering::Relaxed);
        while wait_ns > current_max {
            match self
                .max_wait_time_ns
                .compare_exchange_weak(current_max, wait_ns, Ordering::Relaxed, Ordering::Relaxed)
            {
                Ok(_) => break,
                Err(x) => current_max = x,
            }
        }
    }

    /// Get total successful acquisitions
    pub fn total_acquisitions(&self) -> u64 {
        self.fast_path_success.load(Ordering::Relaxed) + self.slow_path_success.load(Ordering::Relaxed)
    }

    /// Get fast path hit rate (0.0 to 1.0)
    pub fn fast_path_rate(&self) -> f64 {
        let total = self.total_acquisitions();
        if total == 0 {
            0.0
        } else {
            self.fast_path_success.load(Ordering::Relaxed) as f64 / total as f64
        }
    }

    /// Get average wait time in nanoseconds
    pub fn avg_wait_time_ns(&self) -> f64 {
        let total_wait = self.total_wait_time_ns.load(Ordering::Relaxed);
        let slow_path = self.slow_path_success.load(Ordering::Relaxed);

        if slow_path == 0 {
            0.0
        } else {
            total_wait as f64 / slow_path as f64
        }
    }

    /// Get snapshot of current metrics
    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            fast_path_success: self.fast_path_success.load(Ordering::Relaxed),
            slow_path_success: self.slow_path_success.load(Ordering::Relaxed),
            timeouts: self.timeouts.load(Ordering::Relaxed),
            releases: self.releases.load(Ordering::Relaxed),
            cleanups: self.cleanups.load(Ordering::Relaxed),
            contention_events: self.contention_events.load(Ordering::Relaxed),
            total_wait_time_ns: self.total_wait_time_ns.load(Ordering::Relaxed),
            max_wait_time_ns: self.max_wait_time_ns.load(Ordering::Relaxed),
        }
    }
}

/// Snapshot of metrics at a point in time
#[derive(Debug, Clone)]
pub struct MetricsSnapshot {
    pub fast_path_success: u64,
    pub slow_path_success: u64,
    pub timeouts: u64,
    pub releases: u64,
    pub cleanups: u64,
    pub contention_events: u64,
    pub total_wait_time_ns: u64,
    pub max_wait_time_ns: u64,
}

impl MetricsSnapshot {
    pub fn total_acquisitions(&self) -> u64 {
        self.fast_path_success + self.slow_path_success
    }

    pub fn fast_path_rate(&self) -> f64 {
        let total = self.total_acquisitions();
        if total == 0 {
            0.0
        } else {
            self.fast_path_success as f64 / total as f64
        }
    }

    pub fn avg_wait_time(&self) -> Duration {
        if self.slow_path_success == 0 {
            Duration::ZERO
        } else {
            Duration::from_nanos(self.total_wait_time_ns / self.slow_path_success)
        }
    }

    pub fn max_wait_time(&self) -> Duration {
        Duration::from_nanos(self.max_wait_time_ns)
    }

    pub fn timeout_rate(&self) -> f64 {
        let total_attempts = self.total_acquisitions() + self.timeouts;
        if total_attempts == 0 {
            0.0
        } else {
            self.timeouts as f64 / total_attempts as f64
        }
    }
}

/// Global metrics aggregator
#[derive(Debug)]
pub struct GlobalMetrics {
    shard_count: usize,
    start_time: Instant,
    cleanup_runs: AtomicU64,
    total_objects_cleaned: AtomicU64,
}

impl GlobalMetrics {
    pub fn new(shard_count: usize) -> Self {
        Self {
            shard_count,
            start_time: Instant::now(),
            cleanup_runs: AtomicU64::new(0),
            total_objects_cleaned: AtomicU64::new(0),
        }
    }

    pub fn record_cleanup_run(&self, objects_cleaned: usize) {
        self.cleanup_runs.fetch_add(1, Ordering::Relaxed);
        self.total_objects_cleaned
            .fetch_add(objects_cleaned as u64, Ordering::Relaxed);
    }

    pub fn uptime(&self) -> Duration {
        self.start_time.elapsed()
    }

    /// Aggregate metrics from all shards
    pub fn aggregate_shard_metrics(&self, shard_metrics: &[MetricsSnapshot]) -> AggregatedMetrics {
        let mut total = MetricsSnapshot {
            fast_path_success: 0,
            slow_path_success: 0,
            timeouts: 0,
            releases: 0,
            cleanups: 0,
            contention_events: 0,
            total_wait_time_ns: 0,
            max_wait_time_ns: 0,
        };

        for snapshot in shard_metrics {
            total.fast_path_success += snapshot.fast_path_success;
            total.slow_path_success += snapshot.slow_path_success;
            total.timeouts += snapshot.timeouts;
            total.releases += snapshot.releases;
            total.cleanups += snapshot.cleanups;
            total.contention_events += snapshot.contention_events;
            total.total_wait_time_ns += snapshot.total_wait_time_ns;
            total.max_wait_time_ns = total.max_wait_time_ns.max(snapshot.max_wait_time_ns);
        }

        AggregatedMetrics {
            shard_metrics: total,
            shard_count: self.shard_count,
            uptime: self.uptime(),
            cleanup_runs: self.cleanup_runs.load(Ordering::Relaxed),
            total_objects_cleaned: self.total_objects_cleaned.load(Ordering::Relaxed),
        }
    }
}

/// Aggregated metrics from all shards
#[derive(Debug, Clone)]
pub struct AggregatedMetrics {
    pub shard_metrics: MetricsSnapshot,
    pub shard_count: usize,
    pub uptime: Duration,
    pub cleanup_runs: u64,
    pub total_objects_cleaned: u64,
}

impl AggregatedMetrics {
    /// Get operations per second
    pub fn ops_per_second(&self) -> f64 {
        let total_ops = self.shard_metrics.total_acquisitions() + self.shard_metrics.releases;
        let uptime_secs = self.uptime.as_secs_f64();

        if uptime_secs > 0.0 {
            total_ops as f64 / uptime_secs
        } else {
            0.0
        }
    }

    /// Get average locks per shard
    pub fn avg_locks_per_shard(&self) -> f64 {
        if self.shard_count > 0 {
            self.shard_metrics.total_acquisitions() as f64 / self.shard_count as f64
        } else {
            0.0
        }
    }

    /// Check if performance is healthy
    pub fn is_healthy(&self) -> bool {
        let fast_path_rate = self.shard_metrics.fast_path_rate();
        let timeout_rate = self.shard_metrics.timeout_rate();
        let avg_wait = self.shard_metrics.avg_wait_time();

        // Healthy if:
        // - Fast path rate > 80%
        // - Timeout rate < 5%
        // - Average wait time < 10ms
        fast_path_rate > 0.8 && timeout_rate < 0.05 && avg_wait < Duration::from_millis(10)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_shard_metrics() {
        let metrics = ShardMetrics::new();

        metrics.record_fast_path_success();
        metrics.record_fast_path_success();
        metrics.record_slow_path_success();
        metrics.record_timeout();

        assert_eq!(metrics.total_acquisitions(), 3);
        assert_eq!(metrics.fast_path_rate(), 2.0 / 3.0);

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.fast_path_success, 2);
        assert_eq!(snapshot.slow_path_success, 1);
        assert_eq!(snapshot.timeouts, 1);
    }

    #[test]
    fn test_global_metrics() {
        let global = GlobalMetrics::new(4);
        let shard_metrics = [ShardMetrics::new(), ShardMetrics::new()];

        shard_metrics[0].record_fast_path_success();
        shard_metrics[1].record_slow_path_success();

        let snapshots: Vec<MetricsSnapshot> = shard_metrics.iter().map(|m| m.snapshot()).collect();
        let aggregated = global.aggregate_shard_metrics(&snapshots);
        assert_eq!(aggregated.shard_metrics.total_acquisitions(), 2);
        assert_eq!(aggregated.shard_count, 4);
    }
}
