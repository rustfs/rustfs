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

//! I/O scheduler configuration types.
//!
//! This module provides configuration types for the I/O scheduler,
//! including priority thresholds, queue capacities, and load thresholds.

use std::time::Duration;

/// I/O scheduler configuration.
#[derive(Debug, Clone, PartialEq)]
pub struct IoSchedulerConfig {
    /// Maximum concurrent disk reads.
    pub max_concurrent_reads: usize,
    /// High priority size threshold in bytes.
    pub high_priority_size_threshold: usize,
    /// Low priority size threshold in bytes.
    pub low_priority_size_threshold: usize,
    /// High priority queue capacity.
    pub queue_high_capacity: usize,
    /// Normal priority queue capacity.
    pub queue_normal_capacity: usize,
    /// Low priority queue capacity.
    pub queue_low_capacity: usize,
    /// Starvation prevention check interval in milliseconds.
    pub starvation_prevention_interval_ms: u64,
    /// Starvation threshold in seconds.
    pub starvation_threshold_secs: u64,
    /// Load sampling window size.
    pub load_sample_window: usize,
    /// High load wait time threshold in milliseconds.
    pub load_high_threshold_ms: u64,
    /// Low load wait time threshold in milliseconds.
    pub load_low_threshold_ms: u64,
    /// Whether priority scheduling is enabled.
    pub enable_priority: bool,

    // Enhanced scheduling configuration fields
    /// Storage media detection enabled.
    pub storage_detection_enabled: bool,
    /// Sequential detection enabled.
    pub sequential_detection_enabled: bool,
    /// Bandwidth monitoring enabled.
    pub bandwidth_monitoring_enabled: bool,
    /// Adaptive buffer sizing enabled.
    pub adaptive_buffer_enabled: bool,
    /// Base buffer size for I/O operations.
    pub base_buffer_size: usize,
    /// Maximum buffer size.
    pub max_buffer_size: usize,
    /// Minimum buffer size.
    pub min_buffer_size: usize,
}

impl Default for IoSchedulerConfig {
    fn default() -> Self {
        Self {
            max_concurrent_reads: 32,
            high_priority_size_threshold: 64 * 1024,      // 64KB
            low_priority_size_threshold: 4 * 1024 * 1024, // 4MB
            queue_high_capacity: 100,
            queue_normal_capacity: 500,
            queue_low_capacity: 200,
            starvation_prevention_interval_ms: 100,
            starvation_threshold_secs: 5,
            load_sample_window: 10,
            load_high_threshold_ms: 50,
            load_low_threshold_ms: 5,
            enable_priority: true,
            storage_detection_enabled: true,
            sequential_detection_enabled: true,
            bandwidth_monitoring_enabled: true,
            adaptive_buffer_enabled: true,
            base_buffer_size: 128 * 1024, // 128KB
            max_buffer_size: 1024 * 1024, // 1MB
            min_buffer_size: 4 * 1024,    // 4KB
        }
    }
}

impl IoSchedulerConfig {
    /// Create a new configuration with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Validate the configuration.
    ///
    /// # Errors
    ///
    /// Returns an error if any configuration value is invalid.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.max_concurrent_reads == 0 {
            return Err(ConfigError::InvalidValue("max_concurrent_reads must be > 0".to_string()));
        }
        if self.high_priority_size_threshold >= self.low_priority_size_threshold {
            return Err(ConfigError::InvalidValue(
                "high_priority_size_threshold must be < low_priority_size_threshold".to_string(),
            ));
        }
        if self.min_buffer_size > self.max_buffer_size {
            return Err(ConfigError::InvalidValue("min_buffer_size must be <= max_buffer_size".to_string()));
        }
        if self.base_buffer_size < self.min_buffer_size || self.base_buffer_size > self.max_buffer_size {
            return Err(ConfigError::InvalidValue(
                "base_buffer_size must be between min_buffer_size and max_buffer_size".to_string(),
            ));
        }
        Ok(())
    }

    /// Get the starvation prevention interval as a Duration.
    pub fn starvation_prevention_interval(&self) -> Duration {
        Duration::from_millis(self.starvation_prevention_interval_ms)
    }

    /// Get the starvation threshold as a Duration.
    pub fn starvation_threshold(&self) -> Duration {
        Duration::from_secs(self.starvation_threshold_secs)
    }

    /// Get the high load threshold as a Duration.
    pub fn load_high_threshold(&self) -> Duration {
        Duration::from_millis(self.load_high_threshold_ms)
    }

    /// Get the low load threshold as a Duration.
    pub fn load_low_threshold(&self) -> Duration {
        Duration::from_millis(self.load_low_threshold_ms)
    }

    /// Builder pattern: set max concurrent reads.
    pub fn with_max_concurrent_reads(mut self, value: usize) -> Self {
        self.max_concurrent_reads = value;
        self
    }

    /// Builder pattern: set priority thresholds.
    pub fn with_priority_thresholds(mut self, high: usize, low: usize) -> Self {
        self.high_priority_size_threshold = high;
        self.low_priority_size_threshold = low;
        self
    }

    /// Builder pattern: set buffer sizes.
    pub fn with_buffer_sizes(mut self, base: usize, min: usize, max: usize) -> Self {
        self.base_buffer_size = base;
        self.min_buffer_size = min;
        self.max_buffer_size = max;
        self
    }

    /// Builder pattern: enable/disable priority scheduling.
    pub fn with_priority_enabled(mut self, enabled: bool) -> Self {
        self.enable_priority = enabled;
        self
    }
}

/// Configuration error type.
#[derive(Debug, Clone, thiserror::Error)]
pub enum ConfigError {
    /// Invalid configuration value.
    #[error("Invalid configuration: {0}")]
    InvalidValue(String),
}

/// I/O priority queue configuration.
#[derive(Debug, Clone, PartialEq)]
pub struct IoPriorityQueueConfig {
    /// High priority queue capacity.
    pub high_capacity: usize,
    /// Normal priority queue capacity.
    pub normal_capacity: usize,
    /// Low priority queue capacity.
    pub low_capacity: usize,
    /// Starvation prevention interval.
    pub starvation_interval: Duration,
    /// Starvation threshold.
    pub starvation_threshold: Duration,
}

impl Default for IoPriorityQueueConfig {
    fn default() -> Self {
        Self {
            high_capacity: 100,
            normal_capacity: 500,
            low_capacity: 200,
            starvation_interval: Duration::from_millis(100),
            starvation_threshold: Duration::from_secs(5),
        }
    }
}

impl IoPriorityQueueConfig {
    /// Create from IoSchedulerConfig.
    pub fn from_scheduler_config(config: &IoSchedulerConfig) -> Self {
        Self {
            high_capacity: config.queue_high_capacity,
            normal_capacity: config.queue_normal_capacity,
            low_capacity: config.queue_low_capacity,
            starvation_interval: config.starvation_prevention_interval(),
            starvation_threshold: config.starvation_threshold(),
        }
    }

    /// Get total capacity across all queues.
    pub fn total_capacity(&self) -> usize {
        self.high_capacity + self.normal_capacity + self.low_capacity
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = IoSchedulerConfig::default();
        assert!(config.validate().is_ok());
        assert!(config.enable_priority);
        assert!(config.adaptive_buffer_enabled);
    }

    #[test]
    fn test_config_validation() {
        let config = IoSchedulerConfig::new().with_max_concurrent_reads(0);
        assert!(config.validate().is_err());

        let config = IoSchedulerConfig::new().with_priority_thresholds(1024 * 1024, 1024);
        assert!(config.validate().is_err());

        let config = IoSchedulerConfig::new().with_buffer_sizes(1024, 4096, 512);
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_builder_pattern() {
        let config = IoSchedulerConfig::new()
            .with_max_concurrent_reads(64)
            .with_priority_thresholds(32 * 1024, 8 * 1024 * 1024)
            .with_buffer_sizes(256 * 1024, 8 * 1024, 2 * 1024 * 1024)
            .with_priority_enabled(false);

        assert_eq!(config.max_concurrent_reads, 64);
        assert_eq!(config.high_priority_size_threshold, 32 * 1024);
        assert!(!config.enable_priority);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_priority_queue_config() {
        let config = IoSchedulerConfig::default();
        let pq_config = IoPriorityQueueConfig::from_scheduler_config(&config);

        assert_eq!(pq_config.high_capacity, config.queue_high_capacity);
        assert_eq!(pq_config.normal_capacity, config.queue_normal_capacity);
        assert_eq!(pq_config.low_capacity, config.queue_low_capacity);
        assert!(pq_config.total_capacity() > 0);
    }

    #[test]
    fn test_duration_helpers() {
        let config = IoSchedulerConfig::default();

        assert_eq!(config.starvation_prevention_interval(), Duration::from_millis(100));
        assert_eq!(config.starvation_threshold(), Duration::from_secs(5));
        assert_eq!(config.load_high_threshold(), Duration::from_millis(50));
        assert_eq!(config.load_low_threshold(), Duration::from_millis(5));
    }
}
