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

use moka::future::Cache;
use std::net::IpAddr;
use std::time::Duration;

/// Cache for storing IP validation results.
#[derive(Debug, Clone)]
pub struct IpValidationCache {
    /// The underlying Moka cache.
    cache: Cache<IpAddr, bool>,
    /// Whether the cache is enabled.
    enabled: bool,
}

impl IpValidationCache {
    /// Creates a new `IpValidationCache` using Moka.
    pub fn new(capacity: usize, ttl: Duration, enabled: bool) -> Self {
        let cache = Cache::builder().max_capacity(capacity as u64).time_to_live(ttl).build();

        Self { cache, enabled }
    }

    /// Checks if an IP is trusted, using the cache if available.
    pub async fn is_trusted(&self, ip: &IpAddr, validator: impl FnOnce(&IpAddr) -> bool) -> bool {
        if !self.enabled {
            return validator(ip);
        }

        // Attempt to get the result from cache.
        if let Some(is_trusted) = self.cache.get(ip).await {
            metrics::counter!("rustfs_trusted_proxy_cache_hits").increment(1);
            return is_trusted;
        }

        // Cache miss: perform validation and update cache.
        metrics::counter!("rustfs_trusted_proxy_cache_misses").increment(1);
        let is_trusted = validator(ip);
        self.cache.insert(*ip, is_trusted).await;

        is_trusted
    }

    /// Clears all entries from the cache.
    pub async fn clear(&self) {
        self.cache.invalidate_all();
        metrics::gauge!("rustfs_trusted_proxy_cache_size").set(0.0);
    }

    /// Returns statistics about the current state of the cache.
    pub fn stats(&self) -> CacheStats {
        let entry_count = self.cache.entry_count();

        CacheStats {
            size: entry_count as usize,
            // Moka doesn't expose max_capacity directly in a simple way after build,
            // but we can track it if needed.
            capacity: 0,
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
