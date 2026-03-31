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

//! Backpressure management for I/O operations.
//!
//! This module provides backpressure mechanisms to prevent system overload
//! and maintain stability under high load conditions.

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

/// Backpressure configuration.
#[derive(Debug, Clone)]
pub struct BackpressureConfig {
    /// Maximum concurrent operations.
    pub max_concurrent: usize,
    /// High water mark (percentage of max_concurrent).
    pub high_water_mark: f64,
    /// Low water mark (percentage of max_concurrent).
    pub low_water_mark: f64,
    /// Cooldown period after applying backpressure.
    pub cooldown: Duration,
    /// Whether backpressure is enabled.
    pub enabled: bool,
}

impl Default for BackpressureConfig {
    fn default() -> Self {
        Self {
            max_concurrent: 32,
            high_water_mark: 0.8,
            low_water_mark: 0.5,
            cooldown: Duration::from_millis(100),
            enabled: true,
        }
    }
}

impl BackpressureConfig {
    /// Create new configuration.
    pub fn new() -> Self {
        Self::default()
    }

    /// Get the high water mark threshold.
    pub fn high_threshold(&self) -> usize {
        (self.max_concurrent as f64 * self.high_water_mark) as usize
    }

    /// Get the low water mark threshold.
    pub fn low_threshold(&self) -> usize {
        (self.max_concurrent as f64 * self.low_water_mark) as usize
    }

    /// Validate the configuration.
    pub fn validate(&self) -> Result<(), BackpressureError> {
        if self.max_concurrent == 0 {
            return Err(BackpressureError::InvalidConfig("max_concurrent must be > 0".to_string()));
        }
        if self.high_water_mark <= self.low_water_mark || self.high_water_mark > 1.0 {
            return Err(BackpressureError::InvalidConfig(
                "high_water_mark must be > low_water_mark and <= 1.0".to_string(),
            ));
        }
        if self.low_water_mark < 0.0 {
            return Err(BackpressureError::InvalidConfig("low_water_mark must be >= 0.0".to_string()));
        }
        Ok(())
    }
}

/// Backpressure error.
#[derive(Debug, Clone, thiserror::Error)]
pub enum BackpressureError {
    /// Invalid configuration.
    #[error("Invalid backpressure config: {0}")]
    InvalidConfig(String),
}

/// Backpressure state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum BackpressureState {
    /// Normal operation.
    #[default]
    Normal,
    /// Warning: approaching high water mark.
    Warning,
    /// Critical: backpressure applied.
    Critical,
}

impl BackpressureState {
    /// Get state as string.
    pub fn as_str(&self) -> &'static str {
        match self {
            BackpressureState::Normal => "normal",
            BackpressureState::Warning => "warning",
            BackpressureState::Critical => "critical",
        }
    }
}

/// Backpressure monitor.
pub struct BackpressureMonitor {
    /// Configuration.
    config: BackpressureConfig,
    /// Current concurrent operations.
    current: AtomicUsize,
    /// Total operations processed.
    total_processed: AtomicU64,
    /// Total operations rejected.
    total_rejected: AtomicU64,
    /// Current state.
    state: std::sync::Mutex<BackpressureState>,
    /// Last state change time.
    last_state_change: std::sync::Mutex<Option<Instant>>,
    /// Whether backpressure is currently active.
    active: AtomicBool,
}

impl BackpressureMonitor {
    /// Create a new backpressure monitor.
    pub fn new(config: BackpressureConfig) -> Self {
        Self {
            config,
            current: AtomicUsize::new(0),
            total_processed: AtomicU64::new(0),
            total_rejected: AtomicU64::new(0),
            state: std::sync::Mutex::new(BackpressureState::Normal),
            last_state_change: std::sync::Mutex::new(None),
            active: AtomicBool::new(false),
        }
    }

    /// Create with default configuration.
    pub fn with_defaults() -> Self {
        Self::new(BackpressureConfig::default())
    }

    /// Get the configuration.
    pub fn config(&self) -> &BackpressureConfig {
        &self.config
    }

    /// Get current concurrent operations.
    pub fn current(&self) -> usize {
        self.current.load(Ordering::Relaxed)
    }

    /// Get current state.
    pub fn state(&self) -> BackpressureState {
        if let Ok(state) = self.state.lock() {
            *state
        } else {
            BackpressureState::Normal
        }
    }

    /// Check if backpressure is active.
    pub fn is_active(&self) -> bool {
        self.active.load(Ordering::Relaxed)
    }

    /// Try to acquire a slot for a new operation.
    ///
    /// Returns true if the operation should proceed, false if it should be rejected.
    pub fn try_acquire(&self) -> bool {
        if !self.config.enabled {
            self.current.fetch_add(1, Ordering::Relaxed);
            self.total_processed.fetch_add(1, Ordering::Relaxed);
            return true;
        }

        let high_threshold = self.config.high_threshold();

        // Use a CAS loop to ensure we never exceed `max_concurrent` under contention.
        loop {
            let current = self.current.load(Ordering::Relaxed);

            if current >= self.config.max_concurrent {
                // At capacity: reject
                self.total_rejected.fetch_add(1, Ordering::Relaxed);
                return false;
            }

            let new = current + 1;
            match self
                .current
                .compare_exchange_weak(current, new, Ordering::Relaxed, Ordering::Relaxed)
            {
                Ok(_) => {
                    // Successfully acquired a slot.
                    self.total_processed.fetch_add(1, Ordering::Relaxed);

                    // Update state if needed, based on the pre-increment value `current`.
                    if current >= high_threshold {
                        self.set_state(BackpressureState::Critical);
                        self.active.store(true, Ordering::Relaxed);
                    } else if current >= self.config.low_threshold() {
                        self.set_state(BackpressureState::Warning);
                    }

                    return true;
                }
                Err(_) => {
                    // Another thread raced with us; retry with the updated value.
                    continue;
                }
            }
        }
    }

    /// Release a slot after operation completes.
    pub fn release(&self) {
        let prev = self.current.fetch_sub(1, Ordering::Relaxed);
        let low_threshold = self.config.low_threshold();

        // Update state if needed
        if prev <= low_threshold + 1 {
            self.set_state(BackpressureState::Normal);
            self.active.store(false, Ordering::Relaxed);
        }
    }

    /// Set the state.
    fn set_state(&self, new_state: BackpressureState) {
        if let Ok(mut state) = self.state.lock()
            && *state != new_state
        {
            *state = new_state;
            if let Ok(mut last) = self.last_state_change.lock() {
                *last = Some(Instant::now());
            }
        }
    }

    /// Get total processed operations.
    pub fn total_processed(&self) -> u64 {
        self.total_processed.load(Ordering::Relaxed)
    }

    /// Get total rejected operations.
    pub fn total_rejected(&self) -> u64 {
        self.total_rejected.load(Ordering::Relaxed)
    }

    /// Get rejection rate.
    pub fn rejection_rate(&self) -> f64 {
        let processed = self.total_processed.load(Ordering::Relaxed);
        let rejected = self.total_rejected.load(Ordering::Relaxed);
        let total = processed + rejected;
        if total == 0 { 0.0 } else { rejected as f64 / total as f64 }
    }

    /// Check if we should apply backpressure based on cooldown.
    pub fn should_apply_backpressure(&self) -> bool {
        if !self.config.enabled {
            return false;
        }

        let current = self.current.load(Ordering::Relaxed);
        if current < self.config.high_threshold() {
            return false;
        }

        // Check cooldown
        if let Ok(last) = self.last_state_change.lock()
            && let Some(last_time) = *last
            && last_time.elapsed() < self.config.cooldown
        {
            return false;
        }

        true
    }

    /// Reset statistics.
    pub fn reset_stats(&self) {
        self.total_processed.store(0, Ordering::Relaxed);
        self.total_rejected.store(0, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_backpressure_config() {
        let config = BackpressureConfig::default();
        assert!(config.validate().is_ok());
        assert_eq!(config.high_threshold(), 25); // 32 * 0.8
        assert_eq!(config.low_threshold(), 16); // 32 * 0.5
    }

    #[test]
    fn test_backpressure_config_validation() {
        let config = BackpressureConfig {
            max_concurrent: 0,
            ..Default::default()
        };
        assert!(config.validate().is_err());

        let config = BackpressureConfig {
            high_water_mark: 0.3,
            low_water_mark: 0.5,
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_backpressure_monitor() {
        let config = BackpressureConfig {
            max_concurrent: 4,
            high_water_mark: 0.75, // high threshold = 3
            low_water_mark: 0.5,   // low threshold = 2
            ..Default::default()
        };
        let monitor = BackpressureMonitor::new(config);

        // Acquire slots - current = 1 after acquire
        assert!(monitor.try_acquire());
        // State is Normal (1 < low_threshold=2)

        // current = 2 after acquire
        assert!(monitor.try_acquire());
        // State should be Warning (2 >= low_threshold=2)

        // current = 3 after acquire
        assert!(monitor.try_acquire());
        // State should be Critical (3 >= high_threshold=3)

        // current = 4 after acquire
        assert!(monitor.try_acquire());
        // At capacity now

        assert!(!monitor.try_acquire()); // Should be rejected

        // Release slots - current = 3 after release
        monitor.release();
        // State is still Critical (3 >= high_threshold=3)

        // current = 2 after release
        monitor.release();
        // State should be Warning (2 >= low_threshold=2 but < high_threshold=3)

        // current = 1 after release
        monitor.release();
        // State should be Normal (1 < low_threshold=2)
        assert_eq!(monitor.state(), BackpressureState::Normal);
    }

    #[test]
    fn test_rejection_rate() {
        let config = BackpressureConfig {
            max_concurrent: 2,
            ..Default::default()
        };
        let monitor = BackpressureMonitor::new(config);

        assert!(monitor.try_acquire());
        assert!(monitor.try_acquire());
        assert!(!monitor.try_acquire()); // Rejected

        assert!((monitor.rejection_rate() - 0.3333333333333333).abs() < 0.01);
    }

    #[test]
    fn test_disabled_backpressure() {
        let config = BackpressureConfig {
            max_concurrent: 1,
            enabled: false,
            ..Default::default()
        };
        let monitor = BackpressureMonitor::new(config);

        // Should always succeed when disabled
        assert!(monitor.try_acquire());
        assert!(monitor.try_acquire());
        assert!(monitor.try_acquire());
    }
}
