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

//! Cold file tiering configuration.
//!
//! This module provides configuration for automatic cold file tiering,
//! which moves infrequently accessed files to cheaper storage backends.

use serde::{Deserialize, Serialize};
use std::sync::RwLock;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU32, Ordering};

/// Default number of days since last access to consider a file "cold"
pub const DEFAULT_COLD_THRESHOLD_DAYS: i64 = 30;

/// Default access time update sampling rate (1 = every access)
pub const DEFAULT_ACCESS_TIME_SAMPLE_RATE: u32 = 1;

/// Default maximum concurrent transitions
pub const DEFAULT_MAX_CONCURRENT_TRANSITIONS: usize = 10;

/// Default minimum file size for cold tiering (1MB)
pub const DEFAULT_MIN_FILE_SIZE: i64 = 1024 * 1024;

/// Configuration for automatic cold file tiering
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ColdTierConfig {
    /// Enable automatic cold file tiering
    pub enabled: bool,

    /// Number of days since last access to consider a file "cold"
    pub cold_threshold_days: i64,

    /// Target tier name for cold files (must be configured in tier config)
    pub target_tier: String,

    /// Minimum file size in bytes to consider for cold tiering
    /// (small files may not benefit from tiering)
    pub min_file_size: Option<i64>,

    /// Maximum concurrent transitions
    pub max_concurrent_transitions: Option<usize>,

    /// Access time update sampling rate (1 = every access, 10 = every 10th access)
    /// Higher values reduce write amplification but decrease accuracy
    pub access_time_sample_rate: Option<u32>,
}

impl ColdTierConfig {
    /// Create a new ColdTierConfig with default values
    pub fn new() -> Self {
        Self {
            enabled: false,
            cold_threshold_days: DEFAULT_COLD_THRESHOLD_DAYS,
            target_tier: String::new(),
            min_file_size: Some(DEFAULT_MIN_FILE_SIZE),
            max_concurrent_transitions: Some(DEFAULT_MAX_CONCURRENT_TRANSITIONS),
            access_time_sample_rate: Some(DEFAULT_ACCESS_TIME_SAMPLE_RATE),
        }
    }

    /// Check if cold tiering is enabled and properly configured
    pub fn is_enabled(&self) -> bool {
        self.enabled && !self.target_tier.is_empty()
    }

    /// Get the cold threshold in days
    pub fn get_cold_threshold_days(&self) -> i64 {
        if self.cold_threshold_days > 0 {
            self.cold_threshold_days
        } else {
            DEFAULT_COLD_THRESHOLD_DAYS
        }
    }

    /// Get the minimum file size for cold tiering
    pub fn get_min_file_size(&self) -> i64 {
        self.min_file_size.unwrap_or(DEFAULT_MIN_FILE_SIZE)
    }

    /// Get the access time sampling rate
    pub fn get_sample_rate(&self) -> u32 {
        self.access_time_sample_rate.unwrap_or(DEFAULT_ACCESS_TIME_SAMPLE_RATE).max(1)
    }

    /// Get max concurrent transitions
    pub fn get_max_concurrent_transitions(&self) -> usize {
        self.max_concurrent_transitions.unwrap_or(DEFAULT_MAX_CONCURRENT_TRANSITIONS)
    }
}

/// Global cold tier configuration state
pub struct GlobalColdTierConfig {
    enabled: AtomicBool,
    cold_threshold_days: AtomicI64,
    target_tier: RwLock<String>,
    min_file_size: AtomicI64,
    access_time_sample_rate: AtomicU32,
    access_counter: AtomicU32,
}

impl Default for GlobalColdTierConfig {
    fn default() -> Self {
        Self::new()
    }
}

impl GlobalColdTierConfig {
    /// Create a new GlobalColdTierConfig with default values
    pub fn new() -> Self {
        Self {
            enabled: AtomicBool::new(false),
            cold_threshold_days: AtomicI64::new(DEFAULT_COLD_THRESHOLD_DAYS),
            target_tier: RwLock::new(String::new()),
            min_file_size: AtomicI64::new(DEFAULT_MIN_FILE_SIZE),
            access_time_sample_rate: AtomicU32::new(DEFAULT_ACCESS_TIME_SAMPLE_RATE),
            access_counter: AtomicU32::new(0),
        }
    }

    /// Update configuration from ColdTierConfig
    pub fn update(&self, config: &ColdTierConfig) {
        self.enabled.store(config.enabled, Ordering::SeqCst);
        self.cold_threshold_days
            .store(config.get_cold_threshold_days(), Ordering::SeqCst);
        self.min_file_size.store(config.get_min_file_size(), Ordering::SeqCst);
        self.access_time_sample_rate.store(config.get_sample_rate(), Ordering::SeqCst);

        if let Ok(mut tier) = self.target_tier.write() {
            *tier = config.target_tier.clone();
        }
    }

    /// Check if cold tiering is enabled
    pub fn is_enabled(&self) -> bool {
        if !self.enabled.load(Ordering::SeqCst) {
            return false;
        }
        if let Ok(tier) = self.target_tier.read() {
            !tier.is_empty()
        } else {
            false
        }
    }

    /// Get the cold threshold in days
    pub fn get_cold_threshold_days(&self) -> i64 {
        self.cold_threshold_days.load(Ordering::SeqCst)
    }

    /// Get the target tier name
    pub fn get_target_tier(&self) -> String {
        self.target_tier.read().map(|t| t.clone()).unwrap_or_default()
    }

    /// Get minimum file size for cold tiering
    pub fn get_min_file_size(&self) -> i64 {
        self.min_file_size.load(Ordering::SeqCst)
    }

    /// Check if access time should be updated based on sampling rate
    /// Returns true if this access should trigger an access time update
    pub fn should_update_access_time(&self) -> bool {
        let rate = self.access_time_sample_rate.load(Ordering::SeqCst);
        if rate <= 1 {
            return true;
        }

        let count = self.access_counter.fetch_add(1, Ordering::Relaxed);
        count % rate == 0
    }
}

lazy_static::lazy_static! {
    /// Global cold tier configuration
    pub static ref GLOBAL_COLD_TIER_CONFIG: GlobalColdTierConfig = GlobalColdTierConfig::new();
}

/// Initialize cold tier configuration from environment variables
pub fn init_cold_tier_config_from_env() {
    let enabled = std::env::var("RUSTFS_COLD_TIER_ENABLED")
        .map(|v| v.to_lowercase() == "true" || v == "1")
        .unwrap_or(false);

    let threshold_days = std::env::var("RUSTFS_COLD_TIER_THRESHOLD_DAYS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_COLD_THRESHOLD_DAYS);

    let target_tier = std::env::var("RUSTFS_COLD_TIER_TARGET").unwrap_or_default();

    let min_file_size = std::env::var("RUSTFS_COLD_TIER_MIN_FILE_SIZE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_MIN_FILE_SIZE);

    let sample_rate = std::env::var("RUSTFS_COLD_TIER_SAMPLE_RATE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_ACCESS_TIME_SAMPLE_RATE);

    let config = ColdTierConfig {
        enabled,
        cold_threshold_days: threshold_days,
        target_tier,
        min_file_size: Some(min_file_size),
        max_concurrent_transitions: None,
        access_time_sample_rate: Some(sample_rate),
    };

    GLOBAL_COLD_TIER_CONFIG.update(&config);

    if config.is_enabled() {
        tracing::info!(
            "Cold tier configuration initialized: enabled={}, threshold_days={}, target_tier={}, min_file_size={}, sample_rate={}",
            config.enabled,
            config.cold_threshold_days,
            config.target_tier,
            config.get_min_file_size(),
            config.get_sample_rate()
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cold_tier_config_defaults() {
        let config = ColdTierConfig::new();
        assert!(!config.is_enabled());
        assert_eq!(config.get_cold_threshold_days(), DEFAULT_COLD_THRESHOLD_DAYS);
        assert_eq!(config.get_min_file_size(), DEFAULT_MIN_FILE_SIZE);
        assert_eq!(config.get_sample_rate(), DEFAULT_ACCESS_TIME_SAMPLE_RATE);
    }

    #[test]
    fn test_cold_tier_config_enabled() {
        let config = ColdTierConfig {
            enabled: true,
            cold_threshold_days: 60,
            target_tier: "COLD_STORAGE".to_string(),
            min_file_size: Some(1024),
            max_concurrent_transitions: Some(5),
            access_time_sample_rate: Some(10),
        };

        assert!(config.is_enabled());
        assert_eq!(config.get_cold_threshold_days(), 60);
        assert_eq!(config.get_min_file_size(), 1024);
        assert_eq!(config.get_sample_rate(), 10);
        assert_eq!(config.get_max_concurrent_transitions(), 5);
    }

    #[test]
    fn test_cold_tier_config_not_enabled_without_target() {
        let config = ColdTierConfig {
            enabled: true,
            cold_threshold_days: 30,
            target_tier: String::new(), // Empty target
            ..Default::default()
        };

        assert!(!config.is_enabled());
    }

    #[test]
    fn test_global_cold_tier_config_update() {
        let global = GlobalColdTierConfig::new();

        let config = ColdTierConfig {
            enabled: true,
            cold_threshold_days: 45,
            target_tier: "TEST_TIER".to_string(),
            min_file_size: Some(2048),
            max_concurrent_transitions: None,
            access_time_sample_rate: Some(5),
        };

        global.update(&config);

        assert!(global.is_enabled());
        assert_eq!(global.get_cold_threshold_days(), 45);
        assert_eq!(global.get_target_tier(), "TEST_TIER");
        assert_eq!(global.get_min_file_size(), 2048);
    }

    #[test]
    fn test_should_update_access_time_sampling() {
        let global = GlobalColdTierConfig::new();

        // With rate 1, should always update
        global.access_time_sample_rate.store(1, Ordering::SeqCst);
        assert!(global.should_update_access_time());
        assert!(global.should_update_access_time());

        // Reset counter
        global.access_counter.store(0, Ordering::SeqCst);

        // With rate 3, should update every 3rd access (0, 3, 6, ...)
        global.access_time_sample_rate.store(3, Ordering::SeqCst);
        assert!(global.should_update_access_time()); // count 0
        assert!(!global.should_update_access_time()); // count 1
        assert!(!global.should_update_access_time()); // count 2
        assert!(global.should_update_access_time()); // count 3
    }
}
