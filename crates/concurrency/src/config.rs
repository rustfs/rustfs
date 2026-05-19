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

/// Main configuration for concurrency management
#[derive(Debug, Clone)]
pub struct ConcurrencyConfig {
    /// Feature flags
    pub features: ConcurrencyFeatures,

    // Timeout configuration
    /// Default timeout duration
    pub default_timeout: Duration,
    /// Maximum timeout duration
    pub max_timeout: Duration,
    /// Enable dynamic timeout
    pub enable_dynamic_timeout: bool,

    // Lock configuration
    /// Enable lock optimization
    pub enable_lock_optimization: bool,
    /// Lock acquisition timeout
    pub lock_acquire_timeout: Duration,

    // Deadlock configuration
    /// Enable deadlock detection
    pub enable_deadlock_detection: bool,
    /// Deadlock check interval
    pub deadlock_check_interval: Duration,
    /// Hang threshold
    pub hang_threshold: Duration,

    // Backpressure configuration
    /// Buffer size for backpressure
    pub backpressure_buffer_size: usize,
    /// High watermark percentage
    pub high_watermark: u32,
    /// Low watermark percentage
    pub low_watermark: u32,

    // Scheduler configuration
    /// Base buffer size for I/O
    pub io_buffer_size: usize,
    /// Maximum buffer size
    pub max_buffer_size: usize,
    /// High priority size threshold
    pub high_priority_threshold: usize,
    /// Low priority size threshold
    pub low_priority_threshold: usize,
}

impl Default for ConcurrencyConfig {
    fn default() -> Self {
        Self {
            features: ConcurrencyFeatures::default(),

            // Timeout defaults
            default_timeout: Duration::from_secs(30),
            max_timeout: Duration::from_secs(300),
            enable_dynamic_timeout: true,

            // Lock defaults
            enable_lock_optimization: true,
            lock_acquire_timeout: Duration::from_secs(5),

            // Deadlock defaults
            enable_deadlock_detection: false,
            deadlock_check_interval: Duration::from_secs(10),
            hang_threshold: Duration::from_secs(60),

            // Backpressure defaults
            backpressure_buffer_size: 4 * 1024 * 1024, // 4MB
            high_watermark: 80,
            low_watermark: 50,

            // Scheduler defaults
            io_buffer_size: 64 * 1024,                // 64KB
            max_buffer_size: 4 * 1024 * 1024,         // 4MB
            high_priority_threshold: 1024 * 1024,     // 1MB
            low_priority_threshold: 10 * 1024 * 1024, // 10MB
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
            config.default_timeout = Duration::from_secs(secs);
        }

        if let Ok(val) = std::env::var("RUSTFS_TIMEOUT_MAX")
            && let Ok(secs) = val.parse::<u64>()
        {
            config.max_timeout = Duration::from_secs(secs);
        }

        if let Ok(val) = std::env::var("RUSTFS_BACKPRESSURE_BUFFER_SIZE")
            && let Ok(size) = val.parse::<usize>()
        {
            config.backpressure_buffer_size = size;
        }

        if let Ok(val) = std::env::var("RUSTFS_IO_BUFFER_SIZE")
            && let Ok(size) = val.parse::<usize>()
        {
            config.io_buffer_size = size;
        }

        config
    }

    /// Validate configuration
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.default_timeout > self.max_timeout {
            return Err(ConfigError::InvalidTimeout("default_timeout cannot exceed max_timeout".to_string()));
        }

        if self.high_watermark <= self.low_watermark || self.high_watermark > 100 {
            return Err(ConfigError::InvalidBackpressure(
                "high_watermark must be > low_watermark and <= 100".to_string(),
            ));
        }

        if self.io_buffer_size > self.max_buffer_size {
            return Err(ConfigError::InvalidScheduler("io_buffer_size cannot exceed max_buffer_size".to_string()));
        }

        Ok(())
    }

    /// Build the timeout facade policy from the aggregate concurrency config.
    pub fn timeout_policy(&self) -> TimeoutManagerPolicy {
        TimeoutManagerPolicy {
            default_timeout: self.default_timeout,
            max_timeout: self.max_timeout,
            enable_dynamic: self.enable_dynamic_timeout,
        }
    }

    /// Build the deadlock facade policy from the aggregate concurrency config.
    pub fn deadlock_policy(&self) -> DeadlockMonitorPolicy {
        DeadlockMonitorPolicy {
            enabled: self.enable_deadlock_detection,
            check_interval: self.deadlock_check_interval,
            hang_threshold: self.hang_threshold,
        }
    }

    /// Build the backpressure facade policy from the aggregate concurrency config.
    pub fn backpressure_policy(&self) -> PipeBackpressurePolicy {
        PipeBackpressurePolicy {
            buffer_size: self.backpressure_buffer_size,
            high_watermark: self.high_watermark,
            low_watermark: self.low_watermark,
        }
    }

    /// Build the scheduler facade policy from the aggregate concurrency config.
    pub fn scheduler_policy(&self) -> SchedulerPolicy {
        SchedulerPolicy {
            base_buffer_size: self.io_buffer_size,
            max_buffer_size: self.max_buffer_size,
            high_priority_threshold: self.high_priority_threshold,
            low_priority_threshold: self.low_priority_threshold,
        }
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
            default_timeout: Duration::from_secs(100),
            max_timeout: Duration::from_secs(50),
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
        assert_eq!(policy.default_timeout, config.default_timeout);
        assert_eq!(policy.max_timeout, config.max_timeout);
        assert_eq!(policy.enable_dynamic, config.enable_dynamic_timeout);
    }

    #[test]
    fn test_deadlock_policy_mapping() {
        let config = ConcurrencyConfig::default();
        let policy = config.deadlock_policy();
        assert_eq!(policy.enabled, config.enable_deadlock_detection);
        assert_eq!(policy.check_interval, config.deadlock_check_interval);
        assert_eq!(policy.hang_threshold, config.hang_threshold);
    }

    #[test]
    fn test_backpressure_policy_mapping() {
        let config = ConcurrencyConfig::default();
        let policy = config.backpressure_policy();
        assert_eq!(policy.buffer_size, config.backpressure_buffer_size);
        assert_eq!(policy.high_watermark, config.high_watermark);
        assert_eq!(policy.low_watermark, config.low_watermark);
    }

    #[test]
    fn test_scheduler_policy_mapping() {
        let config = ConcurrencyConfig::default();
        let policy = config.scheduler_policy();
        assert_eq!(policy.base_buffer_size, config.io_buffer_size);
        assert_eq!(policy.max_buffer_size, config.max_buffer_size);
        assert_eq!(policy.high_priority_threshold, config.high_priority_threshold);
        assert_eq!(policy.low_priority_threshold, config.low_priority_threshold);
    }
}
