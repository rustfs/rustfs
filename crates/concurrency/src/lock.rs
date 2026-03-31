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

//! Lock optimization management

use rustfs_io_core::{LockOptimizer as CoreLockOptimizer, LockStats};
use rustfs_io_metrics::lock_metrics;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Lock configuration
#[derive(Debug, Clone)]
pub struct LockConfig {
    /// Enable lock optimization
    pub enabled: bool,
    /// Lock acquisition timeout
    pub acquire_timeout: Duration,
}

impl Default for LockConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            acquire_timeout: Duration::from_secs(5),
        }
    }
}

/// Lock manager
pub struct LockManager {
    config: LockConfig,
    optimizer: Arc<CoreLockOptimizer>,
}

impl LockManager {
    /// Create a new lock manager
    pub fn new(enabled: bool, acquire_timeout: Duration) -> Self {
        let config = LockConfig {
            enabled,
            acquire_timeout,
        };

        let core_config = rustfs_io_core::LockOptimizeConfig {
            enabled,
            acquire_timeout,
            max_hold_time_warning: Duration::from_millis(100),
            adaptive_spin: true,
            max_spin_iterations: 1000,
        };

        Self {
            config,
            optimizer: Arc::new(CoreLockOptimizer::new(core_config)),
        }
    }

    /// Get the configuration
    pub fn config(&self) -> &LockConfig {
        &self.config
    }

    /// Get the core optimizer
    pub fn optimizer(&self) -> Arc<CoreLockOptimizer> {
        self.optimizer.clone()
    }

    /// Get lock statistics
    pub fn stats(&self) -> &LockStats {
        self.optimizer.stats()
    }

    /// Optimize a lock guard
    pub fn optimize<G>(&self, guard: G, resource: impl Into<String>) -> OptimizedLockGuard<G> {
        OptimizedLockGuard::new(guard, resource, self.optimizer.clone())
    }

    /// Check if optimization is enabled
    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }
}

/// Optimized lock guard with early release support
pub struct OptimizedLockGuard<G> {
    guard: Option<G>,
    acquire_time: Instant,
    released: bool,
    resource: String,
    optimizer: Arc<CoreLockOptimizer>,
}

impl<G> OptimizedLockGuard<G> {
    fn new(guard: G, resource: impl Into<String>, optimizer: Arc<CoreLockOptimizer>) -> Self {
        optimizer.on_acquire();
        lock_metrics::record_lock_optimization_enabled(optimizer.config().enabled);

        Self {
            guard: Some(guard),
            acquire_time: Instant::now(),
            released: false,
            resource: resource.into(),
            optimizer,
        }
    }

    /// Get the lock hold time
    pub fn hold_time(&self) -> Duration {
        self.acquire_time.elapsed()
    }

    /// Check if the lock has been released
    pub fn is_released(&self) -> bool {
        self.released
    }

    /// Release the lock early
    pub fn early_release(&mut self) {
        if self.released {
            return;
        }

        let hold_time = self.hold_time();
        self.guard.take();
        self.released = true;

        self.optimizer.on_release(hold_time);
        lock_metrics::record_lock_hold_time(hold_time);

        tracing::debug!(
            resource = %self.resource,
            hold_time_ms = hold_time.as_millis(),
            "Lock released early (optimization active)"
        );
    }

    /// Get a reference to the underlying guard
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

            self.optimizer.on_release(hold_time);
            lock_metrics::record_lock_hold_time(hold_time);

            tracing::debug!(
                resource = %self.resource,
                hold_time_ms = hold_time.as_millis(),
                "Lock released on drop (normal release)"
            );
        }
    }
}

/// Lock scope guard for RAII semantics
pub struct LockScopeGuard<G> {
    guard: Option<G>,
}

impl<G> LockScopeGuard<G> {
    /// Create a new scope guard
    pub fn new(guard: G) -> Self {
        Self { guard: Some(guard) }
    }

    /// Get a reference to the guard
    pub fn as_ref(&self) -> Option<&G> {
        self.guard.as_ref()
    }
}

impl<G> Drop for LockScopeGuard<G> {
    fn drop(&mut self) {
        self.guard.take();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    #[test]
    fn test_lock_manager_creation() {
        let manager = LockManager::new(true, Duration::from_secs(5));
        assert!(manager.is_enabled());
    }

    #[test]
    fn test_optimized_lock_guard() {
        let manager = LockManager::new(true, Duration::from_secs(5));
        let mutex = Mutex::new(42);
        let guard = mutex.lock().unwrap();

        let optimized = manager.optimize(guard, "test_resource");
        assert!(!optimized.is_released());

        let mut optimized = optimized;
        optimized.early_release();
        assert!(optimized.is_released());
    }
}
