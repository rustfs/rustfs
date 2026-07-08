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
#[derive(Debug, Clone, Copy)]
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
#[derive(Debug, Clone, Default)]
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

impl ConcurrencyConfig {
    /// Create configuration from environment variables
    pub fn from_env() -> Self {
        let mut config = Self::default();

        // Read from environment if available
        if let Some(secs) = parse_env::<u64>("RUSTFS_TIMEOUT_DEFAULT") {
            config.timeout_policy.default_timeout = Duration::from_secs(secs);
        }

        if let Some(secs) = parse_env::<u64>("RUSTFS_TIMEOUT_MAX") {
            config.timeout_policy.max_timeout = Duration::from_secs(secs);
        }

        if let Some(size) = parse_env::<usize>("RUSTFS_BACKPRESSURE_BUFFER_SIZE") {
            config.backpressure_policy.buffer_size = size;
        }

        if let Some(size) = parse_env::<usize>("RUSTFS_IO_BUFFER_SIZE") {
            config.scheduler_policy.base_buffer_size = size;
        }

        config
    }

    /// Validate configuration
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.timeout_policy.default_timeout.is_zero() || self.timeout_policy.max_timeout.is_zero() {
            return Err(ConfigError::InvalidTimeout("timeouts must be > 0".to_string()));
        }
        if self.timeout_policy.default_timeout > self.timeout_policy.max_timeout {
            return Err(ConfigError::InvalidTimeout("default_timeout cannot exceed max_timeout".to_string()));
        }
        if self.timeout_policy.min_timeout > self.timeout_policy.max_timeout {
            return Err(ConfigError::InvalidTimeout("min_timeout cannot exceed max_timeout".to_string()));
        }

        // A zero-capacity pipe (duplex(0)) never accepts writes, hanging producers forever.
        if self.backpressure_policy.buffer_size == 0 {
            return Err(ConfigError::InvalidBackpressure("buffer_size must be > 0".to_string()));
        }
        if self.backpressure_policy.high_watermark <= self.backpressure_policy.low_watermark
            || self.backpressure_policy.high_watermark > 100
        {
            return Err(ConfigError::InvalidBackpressure(
                "high_watermark must be > low_watermark and <= 100".to_string(),
            ));
        }

        if self.scheduler_policy.base_buffer_size == 0 {
            return Err(ConfigError::InvalidScheduler("base_buffer_size must be > 0".to_string()));
        }
        if self.scheduler_policy.base_buffer_size > self.scheduler_policy.max_buffer_size {
            return Err(ConfigError::InvalidScheduler(
                "base_buffer_size cannot exceed max_buffer_size".to_string(),
            ));
        }
        // Ensure the derived io-core config also holds its invariants.
        self.scheduler_policy
            .to_core_config()
            .validate()
            .map_err(|e| ConfigError::InvalidScheduler(e.to_string()))?;

        Ok(())
    }
}

/// Parse an environment variable, logging a warning instead of silently
/// falling back to the default when the value is set but unparsable.
fn parse_env<T: std::str::FromStr>(name: &str) -> Option<T> {
    let val = std::env::var(name).ok()?;
    match val.parse() {
        Ok(parsed) => Some(parsed),
        Err(_) => {
            tracing::warn!(env_var = name, value = %val, "ignoring unparsable environment variable");
            None
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
            timeout_policy: TimeoutManagerPolicy {
                default_timeout: Duration::from_secs(100),
                max_timeout: Duration::from_secs(50),
                enable_dynamic: true,
                ..Default::default()
            },
            ..Default::default()
        };
        assert!(
            config.validate().is_err(),
            "validate() should return an error when default_timeout > max_timeout"
        );
    }

    #[test]
    fn test_invalid_min_timeout() {
        let config = ConcurrencyConfig {
            timeout_policy: TimeoutManagerPolicy {
                min_timeout: Duration::from_secs(100),
                max_timeout: Duration::from_secs(50),
                ..Default::default()
            },
            ..Default::default()
        };
        assert!(
            config.validate().is_err(),
            "validate() should return an error when min_timeout > max_timeout"
        );
    }

    #[test]
    fn test_zero_backpressure_buffer_size_rejected() {
        let config = ConcurrencyConfig {
            backpressure_policy: PipeBackpressurePolicy {
                buffer_size: 0,
                ..Default::default()
            },
            ..Default::default()
        };
        assert!(
            matches!(config.validate(), Err(ConfigError::InvalidBackpressure(_))),
            "validate() must reject buffer_size=0 (duplex(0) hangs all writes)"
        );
    }

    #[test]
    fn test_zero_timeouts_rejected() {
        let config = ConcurrencyConfig {
            timeout_policy: TimeoutManagerPolicy {
                default_timeout: Duration::ZERO,
                ..Default::default()
            },
            ..Default::default()
        };
        assert!(matches!(config.validate(), Err(ConfigError::InvalidTimeout(_))));

        let config = ConcurrencyConfig {
            timeout_policy: TimeoutManagerPolicy {
                max_timeout: Duration::ZERO,
                ..Default::default()
            },
            ..Default::default()
        };
        assert!(matches!(config.validate(), Err(ConfigError::InvalidTimeout(_))));
    }

    #[test]
    fn test_zero_scheduler_base_buffer_size_rejected() {
        let config = ConcurrencyConfig {
            scheduler_policy: SchedulerPolicy {
                base_buffer_size: 0,
                ..Default::default()
            },
            ..Default::default()
        };
        assert!(matches!(config.validate(), Err(ConfigError::InvalidScheduler(_))));
    }

    #[test]
    fn test_small_max_buffer_size_passes_core_validation() {
        // max_buffer_size below the io-core default min_buffer_size (4KB) must
        // still yield a valid core config (min lowered by to_core_config).
        let config = ConcurrencyConfig {
            scheduler_policy: SchedulerPolicy {
                base_buffer_size: 1024,
                max_buffer_size: 2048,
                ..Default::default()
            },
            ..Default::default()
        };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_features() {
        let features = ConcurrencyFeatures::all();
        assert!(features.any_enabled());

        let features = ConcurrencyFeatures::none();
        assert!(!features.any_enabled());
    }
}
