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

//! Cache configuration and adaptive TTL for object caching.
//!
//! This module provides cache configuration types and adaptive TTL
//! algorithms for optimizing cache behavior based on access patterns.

use std::time::Duration;

/// Cache configuration.
#[derive(Debug, Clone)]
pub struct CacheConfig {
    /// Maximum cache capacity (number of entries).
    pub max_capacity: u64,
    /// Default TTL in seconds.
    pub default_ttl_seconds: u64,
    /// Maximum memory usage in bytes.
    pub max_memory_bytes: u64,
    /// Number of concurrent shards.
    pub concurrency_shards: usize,
    /// Whether adaptive TTL is enabled.
    pub adaptive_ttl_enabled: bool,
    /// Minimum TTL in seconds.
    pub min_ttl_seconds: u64,
    /// Maximum TTL in seconds.
    pub max_ttl_seconds: u64,
    /// TTL extension factor for hot items.
    pub ttl_extension_factor: f64,
    /// TTL reduction factor for cold items.
    pub ttl_reduction_factor: f64,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            max_capacity: 10_000,
            default_ttl_seconds: 300,            // 5 minutes
            max_memory_bytes: 100 * 1024 * 1024, // 100 MB
            concurrency_shards: num_cpus::get(),
            adaptive_ttl_enabled: true,
            min_ttl_seconds: 60,   // 1 minute
            max_ttl_seconds: 3600, // 1 hour
            ttl_extension_factor: 1.5,
            ttl_reduction_factor: 0.7,
        }
    }
}

impl CacheConfig {
    /// Create a new cache configuration with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Validate the configuration.
    pub fn validate(&self) -> Result<(), CacheConfigError> {
        if self.max_capacity == 0 {
            return Err(CacheConfigError::InvalidValue("max_capacity must be > 0".to_string()));
        }
        if self.min_ttl_seconds >= self.max_ttl_seconds {
            return Err(CacheConfigError::InvalidValue("min_ttl_seconds must be < max_ttl_seconds".to_string()));
        }
        if self.default_ttl_seconds < self.min_ttl_seconds || self.default_ttl_seconds > self.max_ttl_seconds {
            return Err(CacheConfigError::InvalidValue(
                "default_ttl_seconds must be between min_ttl_seconds and max_ttl_seconds".to_string(),
            ));
        }
        if self.ttl_extension_factor <= 1.0 {
            return Err(CacheConfigError::InvalidValue("ttl_extension_factor must be > 1.0".to_string()));
        }
        if self.ttl_reduction_factor >= 1.0 || self.ttl_reduction_factor <= 0.0 {
            return Err(CacheConfigError::InvalidValue(
                "ttl_reduction_factor must be between 0.0 and 1.0".to_string(),
            ));
        }
        Ok(())
    }

    /// Get the default TTL as a Duration.
    pub fn default_ttl(&self) -> Duration {
        Duration::from_secs(self.default_ttl_seconds)
    }

    /// Get the minimum TTL as a Duration.
    pub fn min_ttl(&self) -> Duration {
        Duration::from_secs(self.min_ttl_seconds)
    }

    /// Get the maximum TTL as a Duration.
    pub fn max_ttl(&self) -> Duration {
        Duration::from_secs(self.max_ttl_seconds)
    }

    /// Builder pattern: set max capacity.
    pub fn with_max_capacity(mut self, value: u64) -> Self {
        self.max_capacity = value;
        self
    }

    /// Builder pattern: set TTL range.
    pub fn with_ttl_range(mut self, min: u64, default: u64, max: u64) -> Self {
        self.min_ttl_seconds = min;
        self.default_ttl_seconds = default;
        self.max_ttl_seconds = max;
        self
    }

    /// Builder pattern: enable/disable adaptive TTL.
    pub fn with_adaptive_ttl(mut self, enabled: bool) -> Self {
        self.adaptive_ttl_enabled = enabled;
        self
    }
}

/// Cache configuration error.
#[derive(Debug, Clone, thiserror::Error)]
pub enum CacheConfigError {
    /// Invalid configuration value.
    #[error("Invalid cache configuration: {0}")]
    InvalidValue(String),
}

/// Adaptive TTL calculator.
#[derive(Debug, Clone)]
pub struct AdaptiveTTL {
    /// Cache configuration.
    config: CacheConfig,
    /// Access count threshold for hot items.
    hot_threshold: u64,
    /// Access count threshold for cold items.
    cold_threshold: u64,
    /// Time window for access counting.
    access_window: Duration,
}

impl Default for AdaptiveTTL {
    fn default() -> Self {
        Self {
            config: CacheConfig::default(),
            hot_threshold: 10,
            cold_threshold: 2,
            access_window: Duration::from_secs(60),
        }
    }
}

impl AdaptiveTTL {
    /// Create a new adaptive TTL calculator.
    pub fn new(config: CacheConfig) -> Self {
        Self {
            config,
            hot_threshold: 10,
            cold_threshold: 2,
            access_window: Duration::from_secs(60),
        }
    }

    /// Create with custom thresholds.
    pub fn with_thresholds(mut self, hot: u64, cold: u64) -> Self {
        self.hot_threshold = hot;
        self.cold_threshold = cold;
        self
    }

    /// Create with custom access window.
    pub fn with_access_window(mut self, window: Duration) -> Self {
        self.access_window = window;
        self
    }

    /// Get the configuration.
    pub fn config(&self) -> &CacheConfig {
        &self.config
    }

    /// Calculate adjusted TTL based on access pattern.
    ///
    /// # Arguments
    ///
    /// * `base_ttl` - The base TTL value
    /// * `access_count` - Number of accesses in the window
    /// * `cache_hit_rate` - Overall cache hit rate (0.0 to 1.0)
    ///
    /// # Returns
    ///
    /// The adjusted TTL value.
    pub fn calculate_ttl(&self, base_ttl: Duration, access_count: u64, cache_hit_rate: f64) -> Duration {
        if !self.config.adaptive_ttl_enabled {
            return base_ttl;
        }

        let mut adjusted_ttl = base_ttl;

        // Adjust based on access count
        if access_count >= self.hot_threshold {
            // Hot item: extend TTL
            adjusted_ttl = Duration::from_secs_f64(adjusted_ttl.as_secs_f64() * self.config.ttl_extension_factor);
        } else if access_count <= self.cold_threshold {
            // Cold item: reduce TTL
            adjusted_ttl = Duration::from_secs_f64(adjusted_ttl.as_secs_f64() * self.config.ttl_reduction_factor);
        }

        // Adjust based on cache hit rate
        if cache_hit_rate > 0.8 {
            // High hit rate: extend TTL
            adjusted_ttl = Duration::from_secs_f64(adjusted_ttl.as_secs_f64() * 1.2);
        } else if cache_hit_rate < 0.3 {
            // Low hit rate: reduce TTL
            adjusted_ttl = Duration::from_secs_f64(adjusted_ttl.as_secs_f64() * 0.8);
        }

        // Clamp to configured range
        adjusted_ttl.clamp(self.config.min_ttl(), self.config.max_ttl())
    }

    /// Determine if an item should be evicted early.
    ///
    /// # Arguments
    ///
    /// * `access_count` - Number of accesses since insertion
    /// * `age` - Time since insertion
    /// * `current_ttl` - Current TTL value
    ///
    /// # Returns
    ///
    /// True if the item should be evicted early.
    pub fn should_evict_early(&self, access_count: u64, age: Duration, current_ttl: Duration) -> bool {
        // Evict early if:
        // 1. Item is cold (low access count)
        // 2. Age is significant (> 50% of TTL)
        // 3. No recent accesses
        if access_count <= self.cold_threshold && age > current_ttl / 2 {
            return true;
        }
        false
    }

    /// Calculate priority score for an item.
    ///
    /// Higher score = higher priority to keep in cache.
    pub fn calculate_priority(&self, access_count: u64, age: Duration, size: usize) -> f64 {
        // Priority = access_frequency * recency_factor / size_factor
        let access_frequency = access_count as f64 / self.access_window.as_secs_f64().max(1.0);

        // Recency factor: newer items have higher priority
        let recency_factor = 1.0 / (1.0 + age.as_secs_f64() / 60.0);

        // Size factor: smaller items have higher priority (more items can fit)
        let size_factor = (size as f64 / 1024.0).max(1.0);

        access_frequency * recency_factor / size_factor
    }
}

/// Cache statistics.
#[derive(Debug, Clone, Default)]
pub struct CacheStats {
    /// Number of cache hits.
    pub hits: u64,
    /// Number of cache misses.
    pub misses: u64,
    /// Number of entries in the cache.
    pub entries: u64,
    /// Total memory used in bytes.
    pub memory_bytes: u64,
    /// Number of evictions.
    pub evictions: u64,
    /// Number of TTL expirations.
    pub ttl_expirations: u64,
}

impl CacheStats {
    /// Create new cache statistics.
    pub fn new() -> Self {
        Self::default()
    }

    /// Get the hit rate (0.0 to 1.0).
    pub fn hit_rate(&self) -> f64 {
        let total = self.hits + self.misses;
        if total == 0 { 0.0 } else { self.hits as f64 / total as f64 }
    }

    /// Get the miss rate (0.0 to 1.0).
    pub fn miss_rate(&self) -> f64 {
        1.0 - self.hit_rate()
    }

    /// Get the total number of lookups.
    pub fn total_lookups(&self) -> u64 {
        self.hits + self.misses
    }

    /// Record a cache hit.
    pub fn record_hit(&mut self) {
        self.hits += 1;
    }

    /// Record a cache miss.
    pub fn record_miss(&mut self) {
        self.misses += 1;
    }

    /// Record an eviction.
    pub fn record_eviction(&mut self) {
        self.evictions += 1;
    }

    /// Record a TTL expiration.
    pub fn record_ttl_expiration(&mut self) {
        self.ttl_expirations += 1;
    }

    /// Reset all statistics.
    pub fn reset(&mut self) {
        *self = Self::default();
    }
}

/// Cache health status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheHealthStatus {
    /// Cache is healthy (high hit rate).
    Healthy,
    /// Cache is degraded (medium hit rate).
    Degraded,
    /// Cache is unhealthy (low hit rate).
    Unhealthy,
    /// Cache status is unknown.
    Unknown,
}

impl CacheHealthStatus {
    /// Determine health status from hit rate.
    pub fn from_hit_rate(hit_rate: f64) -> Self {
        if hit_rate >= 0.8 {
            CacheHealthStatus::Healthy
        } else if hit_rate >= 0.5 {
            CacheHealthStatus::Degraded
        } else if hit_rate >= 0.0 {
            CacheHealthStatus::Unhealthy
        } else {
            CacheHealthStatus::Unknown
        }
    }

    /// Get the status as a string.
    pub fn as_str(&self) -> &'static str {
        match self {
            CacheHealthStatus::Healthy => "healthy",
            CacheHealthStatus::Degraded => "degraded",
            CacheHealthStatus::Unhealthy => "unhealthy",
            CacheHealthStatus::Unknown => "unknown",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cache_config_default() {
        let config = CacheConfig::default();
        assert!(config.validate().is_ok());
        assert!(config.adaptive_ttl_enabled);
    }

    #[test]
    fn test_cache_config_validation() {
        let config = CacheConfig::new().with_max_capacity(0);
        assert!(config.validate().is_err());

        let config = CacheConfig::new().with_ttl_range(100, 50, 10);
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_adaptive_ttl() {
        let ttl = AdaptiveTTL::default();

        // Hot item
        let base = Duration::from_secs(300);
        let adjusted = ttl.calculate_ttl(base, 15, 0.5);
        assert!(adjusted > base);

        // Cold item
        let adjusted = ttl.calculate_ttl(base, 1, 0.5);
        assert!(adjusted < base);
    }

    #[test]
    fn test_cache_stats() {
        let mut stats = CacheStats::new();

        stats.record_hit();
        stats.record_hit();
        stats.record_miss();

        assert_eq!(stats.hits, 2);
        assert_eq!(stats.misses, 1);
        assert!((stats.hit_rate() - 0.6666666666666666).abs() < 0.01);
    }

    #[test]
    fn test_cache_health_status() {
        assert_eq!(CacheHealthStatus::from_hit_rate(0.9), CacheHealthStatus::Healthy);
        assert_eq!(CacheHealthStatus::from_hit_rate(0.6), CacheHealthStatus::Degraded);
        assert_eq!(CacheHealthStatus::from_hit_rate(0.2), CacheHealthStatus::Unhealthy);
    }

    #[test]
    fn test_should_evict_early() {
        let ttl = AdaptiveTTL::default();

        // Cold item with significant age
        assert!(ttl.should_evict_early(1, Duration::from_secs(200), Duration::from_secs(300)));

        // Hot item
        assert!(!ttl.should_evict_early(20, Duration::from_secs(200), Duration::from_secs(300)));
    }

    #[test]
    fn test_calculate_priority() {
        let ttl = AdaptiveTTL::default();

        // High access count = high priority
        let high_priority = ttl.calculate_priority(100, Duration::from_secs(10), 1024);
        let low_priority = ttl.calculate_priority(1, Duration::from_secs(100), 1024);
        assert!(high_priority > low_priority);

        // Smaller size = higher priority
        let small_priority = ttl.calculate_priority(10, Duration::from_secs(10), 1024);
        let large_priority = ttl.calculate_priority(10, Duration::from_secs(10), 10240);
        assert!(small_priority > large_priority);
    }
}
