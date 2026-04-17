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

//! High-performance cache implementation for proxy validation results using Moka.

use moka::sync::Cache;
use std::net::IpAddr;
use std::time::Duration;

use crate::ProxyMetrics;

/// Cache for storing IP validation results.
#[derive(Debug, Clone)]
pub struct IpValidationCache {
    /// The underlying Moka cache.
    cache: Cache<IpAddr, bool>,
    /// Configured capacity.
    capacity: usize,
    /// Whether the cache is enabled.
    enabled: bool,
    /// Optional metrics collector for cache activity.
    metrics: Option<ProxyMetrics>,
}

impl IpValidationCache {
    /// Creates a new `IpValidationCache` using Moka.
    pub fn new(capacity: usize, ttl: Duration, enabled: bool, metrics: Option<ProxyMetrics>) -> Self {
        let cache = Cache::builder().max_capacity(capacity as u64).time_to_live(ttl).build();
        let this = Self {
            cache,
            capacity,
            enabled,
            metrics,
        };
        this.update_cache_size_metric();
        this
    }

    /// Checks if an IP is trusted, using the cache if available.
    pub fn is_trusted(&self, ip: &IpAddr, validator: impl FnOnce(&IpAddr) -> bool) -> bool {
        if !self.enabled {
            return validator(ip);
        }

        // Attempt to get the result from cache.
        if let Some(is_trusted) = self.cache.get(ip) {
            self.record_cache_hit();
            return is_trusted;
        }

        // Cache miss: perform validation. Only positive trust decisions are cached
        // to avoid polluting the cache with one-off untrusted client IPs.
        self.record_cache_miss();
        let is_trusted = validator(ip);
        if is_trusted {
            self.cache.insert(*ip, is_trusted);
            self.update_cache_size_metric();
        }

        is_trusted
    }

    /// Clears all entries from the cache.
    pub fn clear(&self) {
        self.cache.invalidate_all();
        self.cache.run_pending_tasks();
        self.update_cache_size_metric();
    }

    /// Runs pending cache maintenance tasks and refreshes size metrics.
    pub fn run_maintenance(&self) {
        if !self.enabled {
            return;
        }

        self.cache.run_pending_tasks();
        self.update_cache_size_metric();
    }

    /// Returns statistics about the current state of the cache.
    pub fn stats(&self) -> CacheStats {
        if self.enabled {
            self.cache.run_pending_tasks();
        }

        let entry_count = self.cache.entry_count();

        CacheStats {
            size: entry_count as usize,
            capacity: self.capacity,
        }
    }

    /// Returns whether the cache is enabled.
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    fn record_cache_hit(&self) {
        if let Some(metrics) = &self.metrics {
            metrics.record_cache_hit();
        }
    }

    fn record_cache_miss(&self) {
        if let Some(metrics) = &self.metrics {
            metrics.record_cache_miss();
        }
    }

    fn update_cache_size_metric(&self) {
        if let Some(metrics) = &self.metrics {
            metrics.set_cache_size(self.cache.entry_count() as usize);
        }
    }
}

/// Statistics about the IP validation cache.
#[derive(Debug, Clone)]
pub struct CacheStats {
    /// Current number of entries in the cache.
    pub size: usize,
    /// Maximum capacity of the cache.
    pub capacity: usize,
}
