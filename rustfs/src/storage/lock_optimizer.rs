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

//! Lock Optimization for GetObject Operations.
//!
//! This module provides optimized lock management for read operations,
//! reducing lock contention by releasing locks early (after metadata read)
//! rather than holding them for the entire data transfer duration.
//!
//! # Migration Note
//!
//! For new code, consider using `rustfs_io_core::LockOptimizer` which provides
//! the same core functionality with better separation of concerns. This module
//! remains for backward compatibility and storage-specific configuration.
//!
//! ```ignore
//! // Recommended: Use io-core directly
//! use rustfs_io_core::LockOptimizer;
//! let optimizer = LockOptimizer::with_defaults();
//! ```

// Allow dead_code for public API that may be used by external modules or future features
#![allow(dead_code)]
//! # Key Features
//!
//! - Early lock release after metadata read
//! - Lock hold time monitoring
//! - Configurable optimization (can be disabled for debugging)
//! - Prometheus metrics for lock contention analysis
//!
//! # Architecture
//!
//! ```text
//! Traditional:  [Acquire Lock] --> [Read Metadata] --> [Transfer Data] --> [Release Lock]
//!                           |<------------------ Lock Held ------------------>|
//!
//! Optimized:    [Acquire Lock] --> [Read Metadata] --> [Release Lock] --> [Transfer Data]
//!                           |<- Lock Held ->|
//! ```

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tracing::debug;

use metrics::histogram;

/// Lock optimization configuration.
#[derive(Debug, Clone)]
pub struct LockOptimizeConfig {
    /// Whether to enable lock optimization.
    /// When enabled, read locks are released after metadata read.
    /// When disabled, locks are held for the entire operation (traditional behavior).
    pub enabled: bool,
    /// Lock acquisition timeout.
    pub acquire_timeout: Duration,
}

impl Default for LockOptimizeConfig {
    fn default() -> Self {
        Self {
            enabled: rustfs_config::DEFAULT_OBJECT_LOCK_OPTIMIZATION_ENABLE,
            acquire_timeout: Duration::from_secs(rustfs_config::DEFAULT_OBJECT_LOCK_ACQUIRE_TIMEOUT),
        }
    }
}

impl LockOptimizeConfig {
    /// Load configuration from environment variables.
    pub fn from_env() -> Self {
        let enabled = rustfs_utils::get_env_bool(
            rustfs_config::ENV_OBJECT_LOCK_OPTIMIZATION_ENABLE,
            rustfs_config::DEFAULT_OBJECT_LOCK_OPTIMIZATION_ENABLE,
        );
        let acquire_timeout = Duration::from_secs(rustfs_utils::get_env_u64(
            rustfs_config::ENV_OBJECT_LOCK_ACQUIRE_TIMEOUT,
            rustfs_config::DEFAULT_OBJECT_LOCK_ACQUIRE_TIMEOUT,
        ));

        Self {
            enabled,
            acquire_timeout,
        }
    }
}

/// Statistics for lock optimization monitoring.
#[derive(Debug, Default)]
pub struct LockStats {
    /// Total locks acquired.
    pub locks_acquired: AtomicU64,
    /// Total locks released early.
    pub locks_released_early: AtomicU64,
    /// Total lock hold time in microseconds.
    pub total_hold_time_us: AtomicU64,
    /// Maximum lock hold time in microseconds.
    pub max_hold_time_us: AtomicU64,
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

    /// Record an early lock release.
    pub fn record_early_release(&self, hold_time: Duration) {
        self.locks_released_early.fetch_add(1, Ordering::Relaxed);
        self.record_hold_time(hold_time);
    }

    /// Record lock hold time.
    fn record_hold_time(&self, hold_time: Duration) {
        let hold_time_us = hold_time.as_micros() as u64;
        self.total_hold_time_us.fetch_add(hold_time_us, Ordering::Relaxed);

        // Update max hold time
        let mut current_max = self.max_hold_time_us.load(Ordering::Relaxed);
        while hold_time_us > current_max {
            match self
                .max_hold_time_us
                .compare_exchange_weak(current_max, hold_time_us, Ordering::Relaxed, Ordering::Relaxed)
            {
                Ok(_) => break,
                Err(actual) => current_max = actual,
            }
        }
    }

    /// Get average hold time.
    pub fn avg_hold_time(&self) -> Duration {
        let total = self.total_hold_time_us.load(Ordering::Relaxed);
        let count = self.locks_released_early.load(Ordering::Relaxed);
        total.checked_div(count).map(Duration::from_micros).unwrap_or(Duration::ZERO)
    }

    /// Get maximum hold time.
    pub fn max_hold_time(&self) -> Duration {
        Duration::from_micros(self.max_hold_time_us.load(Ordering::Relaxed))
    }
}

/// Global lock statistics.
static LOCK_STATS: std::sync::OnceLock<Arc<LockStats>> = std::sync::OnceLock::new();

/// Get global lock statistics.
pub fn get_lock_stats() -> Arc<LockStats> {
    LOCK_STATS.get_or_init(|| Arc::new(LockStats::new())).clone()
}

/// An optimized lock guard that supports early release.
///
/// This wraps the actual lock guard and provides:
/// - Early release capability (before drop)
/// - Hold time tracking
/// - Metrics reporting
pub struct OptimizedLockGuard<G> {
    /// The underlying lock guard.
    guard: Option<G>,
    /// When the lock was acquired.
    acquire_time: Instant,
    /// Whether the lock has been released.
    released: bool,
    /// Lock resource name (for logging).
    resource: String,
    /// Statistics reference.
    stats: Arc<LockStats>,
}

impl<G> OptimizedLockGuard<G> {
    /// Create a new optimized lock guard.
    pub fn new(guard: G, resource: impl Into<String>) -> Self {
        let stats = get_lock_stats();
        stats.record_acquire();

        Self {
            guard: Some(guard),
            acquire_time: Instant::now(),
            released: false,
            resource: resource.into(),
            stats,
        }
    }

    /// Get the lock hold time so far.
    pub fn hold_time(&self) -> Duration {
        self.acquire_time.elapsed()
    }

    /// Check if the lock has been released.
    pub fn is_released(&self) -> bool {
        self.released
    }

    /// Release the lock early (before drop).
    ///
    /// This is the key optimization: releasing the lock after
    /// metadata read rather than waiting for the entire operation.
    pub fn early_release(&mut self) {
        if self.released {
            return;
        }

        let hold_time = self.hold_time();
        self.guard.take();
        self.released = true;

        self.stats.record_early_release(hold_time);

        histogram!("rustfs.lock.hold.duration.seconds").record(hold_time.as_secs_f64());

        debug!(
            resource = %self.resource,
            hold_time_ms = hold_time.as_millis(),
            "Lock released early (optimization active)"
        );
    }

    /// Get a reference to the underlying guard.
    pub fn as_ref(&self) -> Option<&G> {
        if self.released { None } else { self.guard.as_ref() }
    }
}

impl<G> Drop for OptimizedLockGuard<G> {
    fn drop(&mut self) {
        if !self.released {
            let hold_time = self.hold_time();
            self.guard.take();
            self.released = true;

            self.stats.record_early_release(hold_time);

            histogram!("rustfs.lock.hold.duration.seconds").record(hold_time.as_secs_f64());

            debug!(
                resource = %self.resource,
                hold_time_ms = hold_time.as_millis(),
                "Lock released on drop (normal release)"
            );
        }
    }
}

/// A scope guard that releases a lock when it goes out of scope.
///
/// This is a simpler version of OptimizedLockGuard for cases
/// where we just need RAII semantics without tracking.
pub struct LockScopeGuard<G> {
    guard: Option<G>,
}

impl<G> LockScopeGuard<G> {
    /// Create a new scope guard.
    pub fn new(guard: G) -> Self {
        Self { guard: Some(guard) }
    }

    /// Release the lock early.
    pub fn release(&mut self) {
        self.guard.take();
    }
}

impl<G> Drop for LockScopeGuard<G> {
    fn drop(&mut self) {
        self.guard.take();
    }
}

/// Helper for managing lock optimization in GetObject operations.
///
/// This provides a clean interface for the common pattern:
/// 1. Acquire lock
/// 2. Read metadata
/// 3. Release lock (if optimization enabled)
/// 4. Transfer data (without lock)
pub struct LockOptimizer {
    /// Configuration.
    config: LockOptimizeConfig,
}

impl LockOptimizer {
    /// Create a new lock optimizer with default configuration.
    pub fn new() -> Self {
        Self {
            config: LockOptimizeConfig::from_env(),
        }
    }

    /// Create a new lock optimizer with custom configuration.
    pub fn with_config(config: LockOptimizeConfig) -> Self {
        Self { config }
    }

    /// Check if lock optimization is enabled.
    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    /// Get the lock acquisition timeout.
    pub fn acquire_timeout(&self) -> Duration {
        self.config.acquire_timeout
    }

    /// Wrap a lock guard for optimization.
    pub fn wrap_guard<G>(&self, guard: G, resource: impl Into<String>) -> OptimizedLockGuard<G> {
        OptimizedLockGuard::new(guard, resource)
    }

    /// Execute a metadata read operation with lock optimization.
    ///
    /// This is the main entry point for optimized lock usage:
    /// - If optimization is enabled: lock is released after metadata_fn completes
    /// - If optimization is disabled: lock is held until the returned guard is dropped
    ///
    /// # Arguments
    ///
    /// * `guard` - The lock guard to optimize
    /// * `resource` - Resource name for logging
    /// * `metadata_fn` - Function to read metadata while holding lock
    ///
    /// # Returns
    ///
    /// A tuple of (metadata result, optional guard to hold for later release)
    pub async fn with_optimized_lock<G, F, Fut, T>(
        &self,
        guard: G,
        resource: impl Into<String>,
        metadata_fn: F,
    ) -> (T, Option<OptimizedLockGuard<G>>)
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = T>,
    {
        let resource = resource.into();
        let mut optimized = OptimizedLockGuard::new(guard, &resource);

        // Execute metadata read while holding lock
        let result = metadata_fn().await;

        if self.config.enabled {
            // Release lock early
            optimized.early_release();
            (result, None)
        } else {
            // Keep lock for caller to release
            (result, Some(optimized))
        }
    }
}

impl Default for LockOptimizer {
    fn default() -> Self {
        Self::new()
    }
}

/// Check if lock optimization is enabled globally.
pub fn is_lock_optimization_enabled() -> bool {
    rustfs_utils::get_env_bool(
        rustfs_config::ENV_OBJECT_LOCK_OPTIMIZATION_ENABLE,
        rustfs_config::DEFAULT_OBJECT_LOCK_OPTIMIZATION_ENABLE,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    #[test]
    fn test_lock_optimize_config_default() {
        let config = LockOptimizeConfig::default();
        assert!(config.enabled);
        assert_eq!(config.acquire_timeout, Duration::from_secs(5));
    }

    #[test]
    fn test_lock_stats() {
        let stats = LockStats::new();

        stats.record_acquire();
        stats.record_early_release(Duration::from_millis(100));
        stats.record_early_release(Duration::from_millis(200));

        assert_eq!(stats.locks_acquired.load(Ordering::Relaxed), 1);
        assert_eq!(stats.locks_released_early.load(Ordering::Relaxed), 2);
        assert_eq!(stats.max_hold_time(), Duration::from_millis(200));
    }

    #[test]
    fn test_optimized_lock_guard() {
        let guard = Mutex::new(42);
        let locked = guard.lock().unwrap();
        let mut optimized = OptimizedLockGuard::new(locked, "test-resource");

        assert!(!optimized.is_released());
        assert!(optimized.hold_time() < Duration::from_secs(1));

        optimized.early_release();
        assert!(optimized.is_released());
    }

    #[test]
    fn test_lock_optimizer() {
        let optimizer = LockOptimizer::new();
        assert!(optimizer.is_enabled());
    }

    #[tokio::test]
    async fn test_with_optimized_lock_enabled() {
        let optimizer = LockOptimizer::new();
        let guard = Mutex::new(42);
        let locked = guard.lock().unwrap();

        let (result, returned_guard) = optimizer.with_optimized_lock(locked, "test-resource", || async { 100 }).await;

        assert_eq!(result, 100);
        // With optimization enabled, guard should be None (released early)
        assert!(returned_guard.is_none());
    }

    #[tokio::test]
    async fn test_with_optimized_lock_disabled() {
        let config = LockOptimizeConfig {
            enabled: false,
            acquire_timeout: Duration::from_secs(5),
        };
        let optimizer = LockOptimizer::with_config(config);
        let guard = Mutex::new(42);
        let locked = guard.lock().unwrap();

        let (result, returned_guard) = optimizer.with_optimized_lock(locked, "test-resource", || async { 100 }).await;

        assert_eq!(result, 100);
        // With optimization disabled, guard should be Some (held for later)
        assert!(returned_guard.is_some());
    }
}
