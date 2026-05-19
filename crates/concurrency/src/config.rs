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

//! Configuration for concurrency management

use crate::{
    backpressure::PipeBackpressurePolicy, deadlock::DeadlockMonitorPolicy, scheduler::SchedulerPolicy,
    timeout::TimeoutManagerPolicy,
};
use std::time::Duration;

/// Feature flags for concurrency modules
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConcurrencyFeatures {
    /// Enable timeout control
    pub timeout: bool,
    /// Enable lock optimization
    pub lock: bool,
    /// Enable deadlock detection
    pub deadlock: bool,
    /// Enable backpressure management
    pub backpressure: bool,
    /// Enable I/O scheduling
    pub scheduler: bool,
}

impl Default for ConcurrencyFeatures {
    fn default() -> Self {
        Self {
            timeout: cfg!(feature = "timeout"),
            lock: cfg!(feature = "lock"),
            deadlock: cfg!(feature = "deadlock"),
            backpressure: cfg!(feature = "backpressure"),
            scheduler: cfg!(feature = "scheduler"),
        }
    }
}

impl ConcurrencyFeatures {
    /// Create with all features enabled
    pub fn all() -> Self {
        Self {
            timeout: true,
            lock: true,
            deadlock: true,
            backpressure: true,
            scheduler: true,
        }
    }

    /// Create with no features enabled
    pub fn none() -> Self {
        Self {
            timeout: false,
            lock: false,
            deadlock: false,
            backpressure: false,
            scheduler: false,
        }
    }

    /// Check if any feature is enabled
    pub fn any_enabled(&self) -> bool {
        self.timeout || self.lock || self.deadlock || self.backpressure || self.scheduler
    }
}

/// Facade policy for lock manager behavior.
#[derive(Debug, Clone)]
pub struct LockManagerPolicy {
    /// Enable lock optimization.
    pub enabled: bool,
    /// Lock acquisition timeout.
    pub acquire_timeout: Duration,
}

impl Default for LockManagerPolicy {
    fn default() -> Self {
        Self {
            enabled: true,
            acquire_timeout: Duration::from_secs(5),
        }
    }
}

/// Main configuration for concurrency management
#[derive(Debug, Clone)]
pub struct ConcurrencyConfig {
    /// Feature flags
    pub features: ConcurrencyFeatures,
    /// Timeout facade policy.
    pub timeout_policy: TimeoutManagerPolicy,
    /// Lock facade policy.
    pub lock_policy: LockManagerPolicy,
    /// Deadlock facade policy.
    pub deadlock_policy: DeadlockMonitorPolicy,
    /// Backpressure facade policy.
    pub backpressure_policy: PipeBackpressurePolicy,
    /// Scheduler facade policy.
    pub scheduler_policy: SchedulerPolicy,
}

impl Default for ConcurrencyConfig {
    fn default() -> Self {
        Self {
            features: ConcurrencyFeatures::default(),
            timeout_policy: TimeoutManagerPolicy::default(),
            lock_policy: LockManagerPolicy::default(),
            deadlock_policy: DeadlockMonitorPolicy::default(),
            backpressure_policy: PipeBackpressurePolicy::default(),
            scheduler_policy: SchedulerPolicy::default(),
        }
    }
}

impl ConcurrencyConfig {
    /// Create configuration from environment variables
    pub fn from_env() -> Self {
        let mut config = Self::default();

        // Read from environment if available
        if let Ok(val) = std::env::var("RUSTFS_TIMEOUT_DEFAULT")
            && let Ok(secs) = val.parse::<u64>()
        {
            config.timeout_policy.default_timeout = Duration::from_secs(secs);
        }

        if let Ok(val) = std::env::var("RUSTFS_TIMEOUT_MAX")
            && let Ok(secs) = val.parse::<u64>()
        {
            config.timeout_policy.max_timeout = Duration::from_secs(secs);
        }

        if let Ok(val) = std::env::var("RUSTFS_BACKPRESSURE_BUFFER_SIZE")
            && let Ok(size) = val.parse::<usize>()
        {
            config.backpressure_policy.buffer_size = size;
        }

        if let Ok(val) = std::env::var("RUSTFS_IO_BUFFER_SIZE")
            && let Ok(size) = val.parse::<usize>()
        {
            config.scheduler_policy.base_buffer_size = size;
        }

        config
    }

    /// Validate configuration
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.timeout_policy.default_timeout > self.timeout_policy.max_timeout {
            return Err(ConfigError::InvalidTimeout("default_timeout cannot exceed max_timeout".to_string()));
        }

        if self.backpressure_policy.high_watermark <= self.backpressure_policy.low_watermark
            || self.backpressure_policy.high_watermark > 100
        {
            return Err(ConfigError::InvalidBackpressure(
                "high_watermark must be > low_watermark and <= 100".to_string(),
            ));
        }

        if self.scheduler_policy.base_buffer_size > self.scheduler_policy.max_buffer_size {
            return Err(ConfigError::InvalidScheduler("io_buffer_size cannot exceed max_buffer_size".to_string()));
        }

        Ok(())
    }

    /// Build the timeout facade policy from the aggregate concurrency config.
    pub fn timeout_policy(&self) -> TimeoutManagerPolicy {
        self.timeout_policy.clone()
    }

    /// Build the lock facade policy from the aggregate concurrency config.
    pub fn lock_policy(&self) -> LockManagerPolicy {
        self.lock_policy.clone()
    }

    /// Build the deadlock facade policy from the aggregate concurrency config.
    pub fn deadlock_policy(&self) -> DeadlockMonitorPolicy {
        self.deadlock_policy.clone()
    }

    /// Build the backpressure facade policy from the aggregate concurrency config.
    pub fn backpressure_policy(&self) -> PipeBackpressurePolicy {
        self.backpressure_policy.clone()
    }

    /// Build the scheduler facade policy from the aggregate concurrency config.
    pub fn scheduler_policy(&self) -> SchedulerPolicy {
        self.scheduler_policy.clone()
    }
}

/// Configuration error
#[allow(clippy::enum_variant_names)]
#[derive(Debug, Clone, thiserror::Error)]
pub enum ConfigError {
    /// Invalid timeout configuration
    #[error("Invalid timeout config: {0}")]
    InvalidTimeout(String),

    /// Invalid backpressure configuration
    #[error("Invalid backpressure config: {0}")]
    InvalidBackpressure(String),

    /// Invalid scheduler configuration
    #[error("Invalid scheduler config: {0}")]
    InvalidScheduler(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = ConcurrencyConfig::default();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_invalid_timeout() {
        let config = ConcurrencyConfig {
            timeout_policy: TimeoutManagerPolicy {
                default_timeout: Duration::from_secs(100),
                max_timeout: Duration::from_secs(50),
                enable_dynamic: true,
            },
            ..Default::default()
        };
        assert!(
            config.validate().is_err(),
            "validate() should return an error when default_timeout > max_timeout"
        );
    }

    #[test]
    fn test_features() {
        let features = ConcurrencyFeatures::all();
        assert!(features.any_enabled());

        let features = ConcurrencyFeatures::none();
        assert!(!features.any_enabled());
    }

    #[test]
    fn test_timeout_policy_mapping() {
        let config = ConcurrencyConfig::default();
        let policy = config.timeout_policy();
        assert_eq!(policy.default_timeout, config.timeout_policy.default_timeout);
        assert_eq!(policy.max_timeout, config.timeout_policy.max_timeout);
        assert_eq!(policy.enable_dynamic, config.timeout_policy.enable_dynamic);
    }

    #[test]
    fn test_lock_policy_mapping() {
        let config = ConcurrencyConfig::default();
        let policy = config.lock_policy();
        assert_eq!(policy.enabled, config.lock_policy.enabled);
        assert_eq!(policy.acquire_timeout, config.lock_policy.acquire_timeout);
    }

    #[test]
    fn test_deadlock_policy_mapping() {
        let config = ConcurrencyConfig::default();
        let policy = config.deadlock_policy();
        assert_eq!(policy.enabled, config.deadlock_policy.enabled);
        assert_eq!(policy.check_interval, config.deadlock_policy.check_interval);
        assert_eq!(policy.hang_threshold, config.deadlock_policy.hang_threshold);
    }

    #[test]
    fn test_backpressure_policy_mapping() {
        let config = ConcurrencyConfig::default();
        let policy = config.backpressure_policy();
        assert_eq!(policy.buffer_size, config.backpressure_policy.buffer_size);
        assert_eq!(policy.high_watermark, config.backpressure_policy.high_watermark);
        assert_eq!(policy.low_watermark, config.backpressure_policy.low_watermark);
    }

    #[test]
    fn test_scheduler_policy_mapping() {
        let config = ConcurrencyConfig::default();
        let policy = config.scheduler_policy();
        assert_eq!(policy.base_buffer_size, config.scheduler_policy.base_buffer_size);
        assert_eq!(policy.max_buffer_size, config.scheduler_policy.max_buffer_size);
        assert_eq!(policy.high_priority_threshold, config.scheduler_policy.high_priority_threshold);
        assert_eq!(policy.low_priority_threshold, config.scheduler_policy.low_priority_threshold);
    }
}
