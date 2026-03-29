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

//! Main concurrency manager

use crate::config::ConcurrencyConfig;
use std::sync::Arc;

/// Snapshot of disk permit queue usage for GetObject orchestration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GetObjectQueueSnapshot {
    /// Total permits configured for disk reads.
    pub total_permits: usize,
    /// Permits currently in use.
    pub permits_in_use: usize,
}

impl GetObjectQueueSnapshot {
    /// Create a queue snapshot from total and available permits.
    pub fn from_available_permits(total_permits: usize, available_permits: usize) -> Self {
        Self {
            total_permits,
            permits_in_use: total_permits.saturating_sub(available_permits),
        }
    }

    /// Return currently available permits.
    pub fn permits_available(&self) -> usize {
        self.total_permits.saturating_sub(self.permits_in_use)
    }

    /// Return queue utilization percentage in the 0-100 range.
    pub fn utilization_percent(&self) -> f64 {
        if self.total_permits == 0 {
            0.0
        } else {
            (self.permits_in_use as f64 / self.total_permits as f64) * 100.0
        }
    }

    /// Return whether the queue is considered congested.
    pub fn is_congested(&self, threshold_percent: f64) -> bool {
        self.utilization_percent() > threshold_percent
    }
}

/// Minimal cache writeback decision inputs for GetObject orchestration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GetObjectCacheEligibility {
    /// Whether response caching is globally enabled.
    pub cache_enabled: bool,
    /// Whether the selected I/O strategy allows cache writeback.
    pub cache_writeback_enabled: bool,
    /// Whether the request is for a specific multipart part.
    pub is_part_request: bool,
    /// Whether the request is a range read.
    pub is_range_request: bool,
    /// Whether server-side or customer-provided encryption was applied.
    pub encryption_applied: bool,
    /// Response payload size in bytes.
    pub response_size: i64,
    /// Maximum cacheable object size in bytes.
    pub max_cacheable_size: usize,
}

impl GetObjectCacheEligibility {
    /// Return whether this GetObject response should be cached.
    pub fn should_cache(&self) -> bool {
        self.cache_enabled
            && self.cache_writeback_enabled
            && !self.is_part_request
            && !self.is_range_request
            && !self.encryption_applied
            && self.response_size > 0
            && (self.response_size as usize) <= self.max_cacheable_size
    }
}

/// Main concurrency manager that provides access to all concurrency features
pub struct ConcurrencyManager {
    config: ConcurrencyConfig,

    #[cfg(feature = "timeout")]
    timeout: Arc<crate::timeout::TimeoutManager>,

    #[cfg(feature = "lock")]
    lock: Arc<crate::lock::LockManager>,

    #[cfg(feature = "deadlock")]
    deadlock: Arc<crate::deadlock::DeadlockManager>,

    #[cfg(feature = "backpressure")]
    backpressure: Arc<crate::backpressure::BackpressureManager>,

    #[cfg(feature = "scheduler")]
    scheduler: Arc<crate::scheduler::SchedulerManager>,
}

impl ConcurrencyManager {
    /// Create a new concurrency manager with the given configuration
    pub fn new(config: ConcurrencyConfig) -> Self {
        // Validate configuration
        if let Err(e) = config.validate() {
            panic!("Invalid concurrency configuration: {}", e);
        }

        Self {
            #[cfg(feature = "timeout")]
            timeout: Arc::new(crate::timeout::TimeoutManager::new(
                config.default_timeout,
                config.max_timeout,
                config.enable_dynamic_timeout,
            )),

            #[cfg(feature = "lock")]
            lock: Arc::new(crate::lock::LockManager::new(
                config.enable_lock_optimization,
                config.lock_acquire_timeout,
            )),

            #[cfg(feature = "deadlock")]
            deadlock: Arc::new(crate::deadlock::DeadlockManager::new(
                config.enable_deadlock_detection,
                config.deadlock_check_interval,
                config.hang_threshold,
            )),

            #[cfg(feature = "backpressure")]
            backpressure: Arc::new(crate::backpressure::BackpressureManager::new(
                config.backpressure_buffer_size,
                config.high_watermark,
                config.low_watermark,
            )),

            #[cfg(feature = "scheduler")]
            scheduler: Arc::new(crate::scheduler::SchedulerManager::new(
                config.io_buffer_size,
                config.max_buffer_size,
                config.high_priority_threshold,
                config.low_priority_threshold,
            )),

            config,
        }
    }

    /// Create with default configuration
    pub fn with_defaults() -> Self {
        Self::new(ConcurrencyConfig::default())
    }

    /// Create from environment variables
    pub fn from_env() -> Self {
        Self::new(ConcurrencyConfig::from_env())
    }

    /// Get the configuration
    pub fn config(&self) -> &ConcurrencyConfig {
        &self.config
    }

    // ============================================
    // Feature enable checks
    // ============================================

    /// Check if timeout feature is enabled
    pub fn is_timeout_enabled(&self) -> bool {
        #[cfg(feature = "timeout")]
        {
            self.config.features.timeout
        }
        #[cfg(not(feature = "timeout"))]
        {
            false
        }
    }

    /// Check if lock feature is enabled
    pub fn is_lock_enabled(&self) -> bool {
        #[cfg(feature = "lock")]
        {
            self.config.features.lock
        }
        #[cfg(not(feature = "lock"))]
        {
            false
        }
    }

    /// Check if deadlock feature is enabled
    pub fn is_deadlock_enabled(&self) -> bool {
        #[cfg(feature = "deadlock")]
        {
            self.config.features.deadlock
        }
        #[cfg(not(feature = "deadlock"))]
        {
            false
        }
    }

    /// Check if backpressure feature is enabled
    pub fn is_backpressure_enabled(&self) -> bool {
        #[cfg(feature = "backpressure")]
        {
            self.config.features.backpressure
        }
        #[cfg(not(feature = "backpressure"))]
        {
            false
        }
    }

    /// Check if scheduler feature is enabled
    pub fn is_scheduler_enabled(&self) -> bool {
        #[cfg(feature = "scheduler")]
        {
            self.config.features.scheduler
        }
        #[cfg(not(feature = "scheduler"))]
        {
            false
        }
    }

    // ============================================
    // Feature accessors
    // ============================================

    /// Get timeout manager
    #[cfg(feature = "timeout")]
    pub fn timeout(&self) -> Arc<crate::timeout::TimeoutManager> {
        self.timeout.clone()
    }

    /// Get lock manager
    #[cfg(feature = "lock")]
    pub fn lock(&self) -> Arc<crate::lock::LockManager> {
        self.lock.clone()
    }

    /// Get deadlock manager
    #[cfg(feature = "deadlock")]
    pub fn deadlock(&self) -> Arc<crate::deadlock::DeadlockManager> {
        self.deadlock.clone()
    }

    /// Get backpressure manager
    #[cfg(feature = "backpressure")]
    pub fn backpressure(&self) -> Arc<crate::backpressure::BackpressureManager> {
        self.backpressure.clone()
    }

    /// Get scheduler manager
    #[cfg(feature = "scheduler")]
    pub fn scheduler(&self) -> Arc<crate::scheduler::SchedulerManager> {
        self.scheduler.clone()
    }

    // ============================================
    // Lifecycle management
    // ============================================

    /// Start all enabled services (e.g., deadlock detection background task)
    pub async fn start(&self) {
        #[cfg(feature = "deadlock")]
        {
            if self.config.enable_deadlock_detection {
                self.deadlock.start().await;
            }
        }

        tracing::info!(
            "Concurrency manager started (timeout={}, lock={}, deadlock={}, backpressure={}, scheduler={})",
            self.is_timeout_enabled(),
            self.is_lock_enabled(),
            self.is_deadlock_enabled(),
            self.is_backpressure_enabled(),
            self.is_scheduler_enabled()
        );
    }

    /// Stop all services
    pub async fn stop(&self) {
        #[cfg(feature = "deadlock")]
        {
            self.deadlock.stop().await;
        }

        tracing::info!("Concurrency manager stopped");
    }
}

impl Default for ConcurrencyManager {
    fn default() -> Self {
        Self::with_defaults()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_queue_snapshot() {
        let snapshot = GetObjectQueueSnapshot::from_available_permits(64, 16);
        assert_eq!(snapshot.permits_in_use, 48);
        assert_eq!(snapshot.permits_available(), 16);
        assert!(snapshot.is_congested(70.0));
    }

    #[test]
    fn test_cache_eligibility() {
        let plan = GetObjectCacheEligibility {
            cache_enabled: true,
            cache_writeback_enabled: true,
            is_part_request: false,
            is_range_request: false,
            encryption_applied: false,
            response_size: 1024,
            max_cacheable_size: 2048,
        };
        assert!(plan.should_cache());
    }

    #[test]
    fn test_manager_creation() {
        let manager = ConcurrencyManager::with_defaults();
        assert!(manager.config().validate().is_ok());
    }

    #[tokio::test]
    async fn test_manager_lifecycle() {
        let manager = ConcurrencyManager::with_defaults();
        manager.start().await;
        manager.stop().await;
    }

    #[test]
    fn test_feature_checks() {
        let manager = ConcurrencyManager::with_defaults();

        // These should return the feature flag status
        let _ = manager.is_timeout_enabled();
        let _ = manager.is_lock_enabled();
        let _ = manager.is_deadlock_enabled();
        let _ = manager.is_backpressure_enabled();
        let _ = manager.is_scheduler_enabled();
    }
}
