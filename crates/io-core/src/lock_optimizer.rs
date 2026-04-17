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

//! Lock optimization utilities.
//!
//! This module provides lock optimization strategies and statistics
//! to improve concurrent performance.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

/// Lock optimization configuration.
#[derive(Debug, Clone)]
pub struct LockOptimizeConfig {
    /// Whether optimization is enabled.
    pub enabled: bool,
    /// Lock acquire timeout.
    pub acquire_timeout: Duration,
    /// Maximum hold time warning threshold.
    pub max_hold_time_warning: Duration,
    /// Enable adaptive spinning.
    pub adaptive_spin: bool,
    /// Maximum spin iterations.
    pub max_spin_iterations: usize,
}

impl Default for LockOptimizeConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            acquire_timeout: Duration::from_secs(5),
            max_hold_time_warning: Duration::from_millis(100),
            adaptive_spin: true,
            max_spin_iterations: 1000,
        }
    }
}

/// Lock statistics.
#[derive(Debug, Default)]
pub struct LockStats {
    /// Number of locks acquired.
    pub locks_acquired: AtomicU64,
    /// Number of locks released early (before timeout).
    pub locks_released_early: AtomicU64,
    /// Total hold time in nanoseconds.
    pub total_hold_time_ns: AtomicU64,
    /// Maximum hold time in nanoseconds.
    pub max_hold_time_ns: AtomicU64,
    /// Number of contention events.
    pub contentions: AtomicU64,
    /// Number of spin successes.
    pub spin_successes: AtomicU64,
    /// Number of spin failures.
    pub spin_failures: AtomicU64,
}

impl LockStats {
    /// Create new lock statistics.
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a lock acquisition.
    pub fn record_acquire(&self) {
        self.locks_acquired.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a lock release.
    pub fn record_release(&self, hold_time: Duration) {
        let ns = hold_time.as_nanos() as u64;
        self.total_hold_time_ns.fetch_add(ns, Ordering::Relaxed);

        // Update max hold time
        let mut current = self.max_hold_time_ns.load(Ordering::Relaxed);
        while ns > current {
            match self
                .max_hold_time_ns
                .compare_exchange_weak(current, ns, Ordering::Relaxed, Ordering::Relaxed)
            {
                Ok(_) => break,
                Err(actual) => current = actual,
            }
        }
    }

    /// Record an early release.
    pub fn record_early_release(&self) {
        self.locks_released_early.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a contention event.
    pub fn record_contention(&self) {
        self.contentions.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a spin success.
    pub fn record_spin_success(&self) {
        self.spin_successes.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a spin failure.
    pub fn record_spin_failure(&self) {
        self.spin_failures.fetch_add(1, Ordering::Relaxed);
    }

    /// Get total locks acquired.
    pub fn total_acquired(&self) -> u64 {
        self.locks_acquired.load(Ordering::Relaxed)
    }

    /// Get average hold time.
    pub fn avg_hold_time(&self) -> Duration {
        let total = self.total_hold_time_ns.load(Ordering::Relaxed);
        let count = self.locks_acquired.load(Ordering::Relaxed);
        total.checked_div(count).map(Duration::from_nanos).unwrap_or(Duration::ZERO)
    }

    /// Get maximum hold time.
    pub fn max_hold_time(&self) -> Duration {
        Duration::from_nanos(self.max_hold_time_ns.load(Ordering::Relaxed))
    }

    /// Get contention rate.
    pub fn contention_rate(&self) -> f64 {
        let acquired = self.locks_acquired.load(Ordering::Relaxed);
        let contentions = self.contentions.load(Ordering::Relaxed);
        if acquired == 0 {
            0.0
        } else {
            contentions as f64 / acquired as f64
        }
    }

    /// Get spin success rate.
    pub fn spin_success_rate(&self) -> f64 {
        let successes = self.spin_successes.load(Ordering::Relaxed);
        let failures = self.spin_failures.load(Ordering::Relaxed);
        let total = successes + failures;
        if total == 0 { 0.0 } else { successes as f64 / total as f64 }
    }

    /// Reset all statistics.
    pub fn reset(&self) {
        self.locks_acquired.store(0, Ordering::Relaxed);
        self.locks_released_early.store(0, Ordering::Relaxed);
        self.total_hold_time_ns.store(0, Ordering::Relaxed);
        self.max_hold_time_ns.store(0, Ordering::Relaxed);
        self.contentions.store(0, Ordering::Relaxed);
        self.spin_successes.store(0, Ordering::Relaxed);
        self.spin_failures.store(0, Ordering::Relaxed);
    }
}

/// Lock optimizer.
pub struct LockOptimizer {
    /// Configuration.
    config: LockOptimizeConfig,
    /// Statistics.
    stats: LockStats,
    /// Current spin iterations (adaptive).
    current_spin: AtomicUsize,
}

impl LockOptimizer {
    /// Create a new lock optimizer.
    pub fn new(config: LockOptimizeConfig) -> Self {
        Self {
            config,
            stats: LockStats::new(),
            current_spin: AtomicUsize::new(100),
        }
    }

    /// Create with default configuration.
    pub fn with_defaults() -> Self {
        Self::new(LockOptimizeConfig::default())
    }

    /// Get the configuration.
    pub fn config(&self) -> &LockOptimizeConfig {
        &self.config
    }

    /// Get the statistics.
    pub fn stats(&self) -> &LockStats {
        &self.stats
    }

    /// Record lock acquisition.
    pub fn on_acquire(&self) {
        if !self.config.enabled {
            return;
        }
        self.stats.record_acquire();
    }

    /// Record lock release.
    pub fn on_release(&self, hold_time: Duration) {
        if !self.config.enabled {
            return;
        }
        self.stats.record_release(hold_time);

        // Check for early release
        if hold_time < self.config.acquire_timeout / 2 {
            self.stats.record_early_release();
        }
    }

    /// Record contention.
    pub fn on_contention(&self) {
        if !self.config.enabled {
            return;
        }
        self.stats.record_contention();
    }

    /// Perform adaptive spin.
    ///
    /// Returns true if the lock was acquired during spinning.
    pub fn try_spin<F>(&self, mut try_acquire: F) -> bool
    where
        F: FnMut() -> bool,
    {
        if !self.config.enabled || !self.config.adaptive_spin {
            return false;
        }

        let spin_count = self.current_spin.load(Ordering::Relaxed).min(self.config.max_spin_iterations);

        for _ in 0..spin_count {
            if try_acquire() {
                self.stats.record_spin_success();
                self.adapt_spin(true);
                return true;
            }
            // Hint to the CPU that we're spinning
            std::hint::spin_loop();
        }

        self.stats.record_spin_failure();
        self.adapt_spin(false);
        false
    }

    /// Adapt spin count based on success/failure.
    fn adapt_spin(&self, success: bool) {
        let current = self.current_spin.load(Ordering::Relaxed);
        let new_count = if success {
            // Increase spin count on success (up to max)
            (current * 2).min(self.config.max_spin_iterations)
        } else {
            // Decrease spin count on failure (down to min)
            (current / 2).max(10)
        };
        self.current_spin.store(new_count, Ordering::Relaxed);
    }

    /// Get current spin count.
    pub fn current_spin_count(&self) -> usize {
        self.current_spin.load(Ordering::Relaxed)
    }

    /// Check if hold time is excessive.
    pub fn is_hold_time_excessive(&self, hold_time: Duration) -> bool {
        hold_time > self.config.max_hold_time_warning
    }

    /// Reset statistics.
    pub fn reset_stats(&self) {
        self.stats.reset();
    }
}

/// RAII guard for tracking lock hold time.
pub struct LockGuard<'a> {
    optimizer: &'a LockOptimizer,
    start: Instant,
}

impl<'a> LockGuard<'a> {
    /// Create a new lock guard.
    pub fn new(optimizer: &'a LockOptimizer) -> Self {
        optimizer.on_acquire();
        Self {
            optimizer,
            start: Instant::now(),
        }
    }
}

impl Drop for LockGuard<'_> {
    fn drop(&mut self) {
        self.optimizer.on_release(self.start.elapsed());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lock_stats() {
        let stats = LockStats::new();

        stats.record_acquire();
        stats.record_acquire();
        stats.record_release(Duration::from_millis(10));
        stats.record_release(Duration::from_millis(20));

        assert_eq!(stats.total_acquired(), 2);
        assert!(stats.avg_hold_time() >= Duration::from_millis(15));
        assert_eq!(stats.max_hold_time(), Duration::from_millis(20));
    }

    #[test]
    fn test_contention_rate() {
        let stats = LockStats::new();

        stats.record_acquire();
        stats.record_acquire();
        stats.record_acquire();
        stats.record_contention();

        assert!((stats.contention_rate() - 0.3333333333333333).abs() < 0.01);
    }

    #[test]
    fn test_spin_stats() {
        let stats = LockStats::new();

        stats.record_spin_success();
        stats.record_spin_success();
        stats.record_spin_failure();

        assert!((stats.spin_success_rate() - 0.6666666666666666).abs() < 0.01);
    }

    #[test]
    fn test_lock_optimizer() {
        let optimizer = LockOptimizer::with_defaults();

        {
            let _guard = LockGuard::new(&optimizer);
            std::thread::sleep(Duration::from_millis(10));
        }

        assert_eq!(optimizer.stats().total_acquired(), 1);
        assert!(optimizer.stats().avg_hold_time() >= Duration::from_millis(10));
    }

    #[test]
    fn test_adaptive_spin() {
        let optimizer = LockOptimizer::with_defaults();

        // Simulate successful spin
        let acquired = optimizer.try_spin(|| true);
        assert!(acquired);
        assert!(optimizer.current_spin_count() > 100); // Should increase

        // Simulate failed spin
        let acquired = optimizer.try_spin(|| false);
        assert!(!acquired);
        assert!(optimizer.current_spin_count() < 200); // Should decrease
    }

    #[test]
    fn test_disabled_optimizer() {
        let config = LockOptimizeConfig {
            enabled: false,
            ..Default::default()
        };
        let optimizer = LockOptimizer::new(config);

        optimizer.on_acquire();
        optimizer.on_release(Duration::from_millis(10));

        // Should not track when disabled
        assert_eq!(optimizer.stats().total_acquired(), 0);
    }
}
