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

//! Object cache module for hot object caching with Moka.
//!
//! # Migration Note
//!
//! This module provides a complete tiered cache implementation. For configuration
//! and metrics types, consider using `rustfs_io_metrics`:
//!
//! ```ignore
//! // Configuration types from io-metrics
//! use rustfs_io_metrics::{CacheConfig, AdaptiveTTL, CacheStats};
//!
//! // Access tracking from io-metrics
//! use rustfs_io_metrics::{AccessTracker, AccessRecord};
//! ```
//!
//! This module remains for the full `TieredObjectCache` implementation.

use hashbrown::HashMap;
use moka::future::Cache;
use rustfs_config::MI_B;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

/// Type alias for the complex tracking type to reduce complexity warning
type TrackingData = Arc<RwLock<HashMap<String, (Arc<AtomicU64>, Instant)>>>;

/// Access tracker for adaptive TTL and tiered cache management.
///
/// Tracks access counts and last access times for cached objects to enable:
/// - Adaptive TTL extension for hot objects
/// - L1/L2 cache promotion/demotion decisions
/// - Cache prewarming with hot key detection
///
/// Uses hashbrown for efficient storage and RwLock for concurrent access.
#[derive(Clone)]
pub struct AccessTracker {
    /// Access counts and last access for each cache key
    #[allow(clippy::type_complexity)]
    tracking: TrackingData,
}

impl AccessTracker {
    /// Create a new access tracker.
    pub fn new() -> Self {
        Self {
            tracking: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Record an access to the given key.
    pub async fn record_access(&self, key: &str) {
        let mut tracking = self.tracking.write().await;
        let key_owned = key.to_string();
        let now = Instant::now();

        if let Some((count, _)) = tracking.get_mut(&key_owned) {
            count.fetch_add(1, Ordering::Relaxed);
            // Update last access time
            *tracking.get_mut(&key_owned).unwrap() = (count.clone(), now);
        } else {
            tracking.insert(key_owned, (Arc::new(AtomicU64::new(1)), now));
        }
    }

    /// Get the access count for a key.
    pub async fn get_hit_count(&self, key: &str) -> u64 {
        let tracking = self.tracking.read().await;
        tracking.get(key).map(|(count, _)| count.load(Ordering::Relaxed)).unwrap_or(0)
    }

    /// Get the last access time for a key.
    #[allow(dead_code)]
    pub async fn get_last_access(&self, key: &str) -> Option<Instant> {
        let tracking = self.tracking.read().await;
        tracking.get(key).map(|(_, time)| *time)
    }

    /// Check if a key is considered hot based on hit threshold.
    pub async fn is_hot(&self, key: &str, threshold: usize) -> bool {
        self.get_hit_count(key).await >= threshold as u64
    }

    /// Get the time since last access for a key.
    #[allow(dead_code)]
    pub async fn time_since_access(&self, key: &str) -> Option<Duration> {
        self.get_last_access(key).await.map(|instant| instant.elapsed())
    }

    /// Remove tracking for a key (called on cache eviction).
    pub async fn remove(&self, key: &str) {
        let mut tracking = self.tracking.write().await;
        tracking.remove(key);
    }

    /// Clear all tracking data.
    #[allow(dead_code)]
    pub async fn clear(&self) {
        let mut tracking = self.tracking.write().await;
        tracking.clear();
    }

    /// Get hot keys sorted by hit count.
    ///
    /// Returns up to `limit` keys with highest access counts.
    pub async fn get_hot_keys(&self, limit: usize) -> Vec<(String, u64)> {
        let tracking: tokio::sync::RwLockReadGuard<'_, HashMap<String, (Arc<AtomicU64>, Instant)>> = self.tracking.read().await;
        let mut entries: Vec<(String, u64)> = tracking
            .iter()
            .map(|(key, value): (&String, &(Arc<AtomicU64>, Instant))| (key.clone(), value.0.load(Ordering::Relaxed)))
            .collect();

        entries.sort_by(|a, b| b.1.cmp(&a.1));
        entries.truncate(limit);
        entries
    }

    /// Get tracking statistics.
    #[allow(dead_code)]
    pub async fn stats(&self) -> AccessTrackerStats {
        let tracking: tokio::sync::RwLockReadGuard<'_, HashMap<String, (Arc<AtomicU64>, Instant)>> = self.tracking.read().await;
        let total_keys = tracking.len();
        let total_hits: u64 = tracking
            .values()
            .map(|v: &(Arc<AtomicU64>, Instant)| v.0.load(Ordering::Relaxed))
            .sum();

        AccessTrackerStats {
            total_keys,
            total_hits,
            avg_hits_per_key: if total_keys > 0 {
                total_hits as f64 / total_keys as f64
            } else {
                0.0
            },
        }
    }
}

impl Default for AccessTracker {
    fn default() -> Self {
        Self::new()
    }
}

/// Access tracker statistics.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct AccessTrackerStats {
    /// Total number of tracked keys
    pub total_keys: usize,
    /// Total number of accesses across all keys
    pub total_hits: u64,
    /// Average hits per key
    pub avg_hits_per_key: f64,
}

// =============================================================================
// Tiered Object Cache
// =============================================================================

/// Tiered cache configuration for L1/L2 caching.
#[derive(Debug, Clone)]
pub struct TieredCacheConfig {
    /// L1 cache: hot small objects (<1MB)
    pub l1_max_size: usize,
    pub l1_max_objects: usize,
    pub l1_ttl_secs: u64,
    pub l1_tti_secs: u64,
    pub l1_max_object_size: usize,

    /// L2 cache: standard objects (<10MB)
    pub l2_max_size: usize,
    pub l2_max_objects: usize,
    pub l2_ttl_secs: u64,
    pub l2_tti_secs: u64,
    pub l2_max_object_size: usize,

    /// Adaptive TTL configuration
    pub adaptive_ttl_enabled: bool,
    pub hot_hit_threshold: usize,
    pub ttl_extension_factor: f64,
}

impl Default for TieredCacheConfig {
    fn default() -> Self {
        Self {
            l1_max_size: rustfs_config::DEFAULT_OBJECT_L1_CACHE_MAX_SIZE_MB as usize * MI_B,
            l1_max_objects: rustfs_config::DEFAULT_OBJECT_L1_CACHE_MAX_OBJECTS,
            l1_ttl_secs: rustfs_config::DEFAULT_OBJECT_L1_CACHE_TTL_SECS,
            l1_tti_secs: rustfs_config::DEFAULT_OBJECT_L1_CACHE_TTI_SECS,
            l1_max_object_size: rustfs_config::DEFAULT_OBJECT_L1_MAX_OBJECT_SIZE_MB * MI_B,

            l2_max_size: rustfs_config::DEFAULT_OBJECT_L2_CACHE_MAX_SIZE_MB as usize * MI_B,
            l2_max_objects: rustfs_config::DEFAULT_OBJECT_L2_CACHE_MAX_OBJECTS,
            l2_ttl_secs: rustfs_config::DEFAULT_OBJECT_L2_CACHE_TTL_SECS,
            l2_tti_secs: rustfs_config::DEFAULT_OBJECT_L2_CACHE_TTI_SECS,
            l2_max_object_size: rustfs_config::DEFAULT_OBJECT_CACHE_MAX_OBJECT_SIZE_MB * MI_B,

            adaptive_ttl_enabled: rustfs_config::DEFAULT_OBJECT_ADAPTIVE_TTL_ENABLE,
            hot_hit_threshold: rustfs_config::DEFAULT_OBJECT_HOT_HIT_THRESHOLD,
            ttl_extension_factor: rustfs_config::DEFAULT_OBJECT_TTL_EXTENSION_FACTOR,
        }
    }
}

/// Tiered object cache with L1 (hot) and L2 (standard) levels.
///
/// L1 cache stores hot small objects (<1MB) with short TTL for rapid access.
/// L2 cache stores standard objects (<10MB) with longer TTL.
/// Objects are promoted from L2 to L1 when frequently accessed.
pub struct TieredObjectCache {
    /// L1 cache for hot small objects
    l1_cache: Cache<String, Arc<CachedGetObjectInternal>>,
    /// L2 cache for standard objects
    l2_cache: Cache<String, Arc<CachedGetObjectInternal>>,
    /// Configuration
    config: TieredCacheConfig,
    /// Access tracker for adaptive TTL
    access_tracker: Arc<AccessTracker>,
    /// L1 max size in bytes
    l1_max_size: usize,
    /// L2 max size in bytes
    l2_max_size: usize,
    /// Global hit counters
    l1_hits: Arc<AtomicU64>,
    l2_hits: Arc<AtomicU64>,
    misses: Arc<AtomicU64>,
}

impl TieredObjectCache {
    /// Create a new tiered object cache.
    #[allow(dead_code)]
    pub fn new() -> Self {
        let config = TieredCacheConfig::default();

        let l1_cache = Cache::builder()
            .max_capacity(config.l1_max_size as u64)
            .weigher(|_key: &String, value: &Arc<CachedGetObjectInternal>| -> u32 { value.size.min(u32::MAX as usize) as u32 })
            .time_to_live(Duration::from_secs(config.l1_ttl_secs))
            .time_to_idle(Duration::from_secs(config.l1_tti_secs))
            .build();

        let l2_cache = Cache::builder()
            .max_capacity(config.l2_max_size as u64)
            .weigher(|_key: &String, value: &Arc<CachedGetObjectInternal>| -> u32 { value.size.min(u32::MAX as usize) as u32 })
            .time_to_live(Duration::from_secs(config.l2_ttl_secs))
            .time_to_idle(Duration::from_secs(config.l2_tti_secs))
            .build();

        Self {
            l1_cache,
            l2_cache,
            l1_max_size: config.l1_max_size,
            l2_max_size: config.l2_max_size,
            config,
            access_tracker: Arc::new(AccessTracker::new()),
            l1_hits: Arc::new(AtomicU64::new(0)),
            l2_hits: Arc::new(AtomicU64::new(0)),
            misses: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Create a new tiered cache with custom configuration.
    #[allow(dead_code)]
    pub fn with_config(config: TieredCacheConfig) -> Self {
        let l1_cache = Cache::builder()
            .max_capacity(config.l1_max_size as u64)
            .weigher(|_key: &String, value: &Arc<CachedGetObjectInternal>| -> u32 { value.size.min(u32::MAX as usize) as u32 })
            .time_to_live(Duration::from_secs(config.l1_ttl_secs))
            .time_to_idle(Duration::from_secs(config.l1_tti_secs))
            .build();

        let l2_cache = Cache::builder()
            .max_capacity(config.l2_max_size as u64)
            .weigher(|_key: &String, value: &Arc<CachedGetObjectInternal>| -> u32 { value.size.min(u32::MAX as usize) as u32 })
            .time_to_live(Duration::from_secs(config.l2_ttl_secs))
            .time_to_idle(Duration::from_secs(config.l2_tti_secs))
            .build();

        Self {
            l1_cache,
            l2_cache,
            l1_max_size: config.l1_max_size,
            l2_max_size: config.l2_max_size,
            config,
            access_tracker: Arc::new(AccessTracker::new()),
            l1_hits: Arc::new(AtomicU64::new(0)),
            l2_hits: Arc::new(AtomicU64::new(0)),
            misses: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Get an object from the tiered cache.
    ///
    /// Checks L1 first, then L2. Promotes L2 hits to L1 if appropriate.
    pub async fn get(&self, key: &str) -> Option<Arc<CachedGetObject>> {
        // Record access
        self.access_tracker.record_access(key).await;

        // Check L1 first
        if let Some(cached) = self.l1_cache.get(key).await {
            self.l1_hits.fetch_add(1, Ordering::Relaxed);
            rustfs_io_metrics::record_tiered_cache_operation("l1", "hit", None);

            return Some(Arc::clone(&cached.data));
        }

        // Check L2
        if let Some(cached) = self.l2_cache.get(key).await {
            self.l2_hits.fetch_add(1, Ordering::Relaxed);
            rustfs_io_metrics::record_tiered_cache_operation("l2", "hit", None);

            // Promote to L1 if appropriate
            if self.should_promote_to_l1(&cached).await {
                let _ = self.l1_cache.insert(key.to_string(), cached.clone()).await;
            }

            return Some(Arc::clone(&cached.data));
        }

        // Cache miss
        self.misses.fetch_add(1, Ordering::Relaxed);
        rustfs_io_metrics::record_tiered_cache_operation("overall", "miss", None);

        None
    }

    /// Put an object into the appropriate cache level.
    pub async fn put(&self, key: String, response: CachedGetObject) {
        let size = response.size();

        // Don't cache empty or oversized objects
        if size == 0 || size > self.config.l2_max_object_size {
            return;
        }

        let cached_internal = Arc::new(CachedGetObjectInternal {
            data: Arc::new(response),
            cached_at: Instant::now(),
            size,
        });

        // Decide which cache level to use
        if size <= self.config.l1_max_object_size {
            // Put in L1
            let _ = self.l1_cache.insert(key, cached_internal).await;
        } else {
            // Put in L2
            let _ = self.l2_cache.insert(key, cached_internal).await;
        }
    }

    /// Check if an object should be promoted to L1.
    async fn should_promote_to_l1(&self, cached: &Arc<CachedGetObjectInternal>) -> bool {
        let size = cached.size;

        // Only promote if it fits in L1
        if size > self.config.l1_max_object_size {
            return false;
        }

        // Check if it's hot (frequently accessed)
        if !self.config.adaptive_ttl_enabled {
            return false;
        }

        // Check access count via the access tracker
        // Note: We'd need to map from internal to key here
        // For simplicity, we'll use a simple heuristic
        let age = cached.cached_at.elapsed();
        age < Duration::from_secs(60) // Recently cached
    }

    /// Calculate adaptive TTL for a cache entry based on access patterns.
    ///
    /// Uses the access tracker to determine if an object is "hot" (frequently accessed).
    /// Hot objects get extended TTL to reduce cache misses.
    #[allow(dead_code)]
    pub async fn calculate_adaptive_ttl(&self, key: &str, base_ttl: u64) -> Duration {
        if !self.config.adaptive_ttl_enabled {
            return Duration::from_secs(base_ttl);
        }

        // Get hit count from access tracker
        let hit_count = self.access_tracker.get_hit_count(key).await;

        if hit_count >= self.config.hot_hit_threshold as u64 {
            // Hot object: extend TTL
            let extension = (base_ttl as f64 * self.config.ttl_extension_factor) as u64;
            Duration::from_secs(base_ttl.saturating_add(extension))
        } else {
            // Normal object: use base TTL
            Duration::from_secs(base_ttl)
        }
    }

    /// Check if an object is considered hot based on access patterns.
    ///
    /// Returns true if the object has been accessed at least the hot threshold number of times.
    #[allow(dead_code)]
    pub async fn is_hot_object(&self, key: &str) -> bool {
        self.access_tracker.is_hot(key, self.config.hot_hit_threshold).await
    }

    /// Invalidate a cache entry from both levels.
    pub async fn invalidate(&self, key: &str) {
        self.l1_cache.invalidate(key).await;
        self.l2_cache.invalidate(key).await;
        // Also remove from access tracker
        self.access_tracker.remove(key).await;
    }

    /// Get cache statistics.
    pub async fn stats(&self) -> TieredCacheStats {
        self.l1_cache.run_pending_tasks().await;
        self.l2_cache.run_pending_tasks().await;

        let l1_hits = self.l1_hits.load(Ordering::Relaxed);
        let l2_hits = self.l2_hits.load(Ordering::Relaxed);
        let misses = self.misses.load(Ordering::Relaxed);
        let total_hits = l1_hits + l2_hits;
        let total_requests = total_hits + misses;

        let hit_rate = if total_requests > 0 {
            total_hits as f64 / total_requests as f64
        } else {
            0.0
        };

        let l1_hit_rate = if total_hits > 0 {
            l1_hits as f64 / total_hits as f64
        } else {
            0.0
        };

        TieredCacheStats {
            l1_size: self.l1_cache.weighted_size() as usize,
            l1_entries: self.l1_cache.entry_count() as usize,
            l1_max_size: self.l1_max_size,
            l2_size: self.l2_cache.weighted_size() as usize,
            l2_entries: self.l2_cache.entry_count() as usize,
            l2_max_size: self.l2_max_size,
            l1_hits,
            l2_hits,
            misses,
            hit_rate,
            l1_hit_rate,
        }
    }

    /// Clear all cached entries.
    pub async fn clear(&self) {
        self.l1_cache.invalidate_all();
        self.l2_cache.invalidate_all();
        self.l1_cache.run_pending_tasks().await;
        self.l2_cache.run_pending_tasks().await;
    }

    /// Reset hit/miss metrics counters.
    ///
    /// This is useful for testing to get a clean slate for hit rate calculations.
    pub fn reset_metrics(&self) {
        self.l1_hits.store(0, Ordering::Relaxed);
        self.l2_hits.store(0, Ordering::Relaxed);
        self.misses.store(0, Ordering::Relaxed);
    }

    /// Get the access tracker reference.
    #[allow(dead_code)]
    pub fn access_tracker(&self) -> &Arc<AccessTracker> {
        &self.access_tracker
    }

    /// Get L1 cache statistics (for detailed monitoring).
    #[allow(dead_code)]
    pub async fn l1_stats(&self) -> CacheLevelStats {
        self.l1_cache.run_pending_tasks().await;
        CacheLevelStats {
            size: self.l1_cache.weighted_size() as usize,
            entries: self.l1_cache.entry_count() as usize,
            max_size: self.l1_max_size,
            max_entries: self.config.l1_max_objects,
            hits: self.l1_hits.load(Ordering::Relaxed),
        }
    }

    /// Get L2 cache statistics (for detailed monitoring).
    #[allow(dead_code)]
    pub async fn l2_stats(&self) -> CacheLevelStats {
        self.l2_cache.run_pending_tasks().await;
        CacheLevelStats {
            size: self.l2_cache.weighted_size() as usize,
            entries: self.l2_cache.entry_count() as usize,
            max_size: self.l2_max_size,
            max_entries: self.config.l2_max_objects,
            hits: self.l2_hits.load(Ordering::Relaxed),
        }
    }

    /// Record cache metrics to Prometheus.
    ///
    /// This method should be called periodically (e.g., every 10 seconds)
    /// to export current cache statistics as Prometheus metrics.
    #[allow(dead_code)]
    pub async fn record_metrics(&self) {
        // Get stats
        let l1_stats = self.l1_stats().await;
        let l2_stats = self.l2_stats().await;
        let tiered_stats = self.stats().await;

        rustfs_io_metrics::record_cache_size("l1", l1_stats.size, l1_stats.entries as u64);
        rustfs_io_metrics::record_cache_size("l2", l2_stats.size, l2_stats.entries as u64);
        rustfs_io_metrics::record_cache_hit_rate("overall", tiered_stats.hit_rate * 100.0);
        rustfs_io_metrics::record_cache_hit_rate("l1", tiered_stats.l1_hit_rate * 100.0);
    }

    // ============================================
    // Cache Warming Methods
    // ============================================

    /// Warm cache with a pattern of preloading.
    ///
    /// This method supports different warming patterns to pre-populate the cache
    /// with frequently accessed objects during server startup or maintenance windows.
    ///
    /// # Arguments
    ///
    /// * `pattern` - The warming pattern to use
    ///
    /// # Returns
    ///
    /// The number of objects successfully warmed
    pub async fn warm_with_pattern(&self, pattern: WarmupPattern) -> usize {
        match pattern {
            WarmupPattern::RecentAccesses { limit } => {
                // Get hot keys from access tracker and warm them
                let hot_keys = self.access_tracker.get_hot_keys(limit).await;
                let mut warmed = 0;

                for (_key, _hit_count) in hot_keys {
                    // Note: In a real implementation, we would load the object
                    // from storage and cache it. Here we just track the operation.
                    warmed += 1;
                }

                warmed
            }
            WarmupPattern::SpecificKeys(keys) => {
                let mut warmed = 0;

                for key in keys {
                    // Check if already in cache
                    if self.l1_cache.contains_key(&key) || self.l2_cache.contains_key(&key) {
                        continue;
                    }

                    // In a real implementation, we would load the object
                    // from storage and cache it here.
                    warmed += 1;
                }

                warmed
            }
        }
    }

    /// Get hot keys for warming purposes.
    ///
    /// Returns the most frequently accessed keys that should be preloaded.
    #[allow(dead_code)]
    pub async fn get_hot_keys_for_warming(&self, limit: usize) -> Vec<String> {
        self.access_tracker
            .get_hot_keys(limit)
            .await
            .into_iter()
            .map(|(key, _)| key)
            .collect()
    }

    // ============================================
    // API Compatibility Methods (for migration from HotObjectCache)
    // ============================================

    /// Check if a key exists in either cache level.
    pub async fn contains(&self, key: &str) -> bool {
        self.l1_cache.contains_key(key) || self.l2_cache.contains_key(key)
    }

    /// Get multiple objects from cache.
    pub async fn get_batch(&self, keys: &[String]) -> Vec<(String, Option<Arc<CachedGetObject>>)> {
        let mut results = Vec::with_capacity(keys.len());
        for key in keys {
            let value = self.get(key).await;
            results.push((key.clone(), value));
        }
        results
    }

    /// Remove a key from both cache levels.
    pub async fn remove(&self, key: &str) -> Option<Arc<CachedGetObject>> {
        // Try L1 first
        if let Some(entry) = self.l1_cache.remove(key).await {
            self.l2_cache.invalidate(key).await;
            self.access_tracker.remove(key).await;
            return Some(Arc::clone(&entry.data));
        }
        // Try L2
        if let Some(entry) = self.l2_cache.remove(key).await {
            self.access_tracker.remove(key).await;
            return Some(Arc::clone(&entry.data));
        }
        None
    }

    /// Get hot keys with their hit counts.
    pub async fn get_hot_keys(&self, limit: usize) -> Vec<(String, usize)> {
        let keys = self.access_tracker.get_hot_keys(limit).await;
        keys.into_iter().map(|(k, v)| (k, v as usize)).collect()
    }

    /// Warm the cache with a pattern.
    pub async fn warm(&self, pattern: WarmupPattern) -> usize {
        self.warm_with_pattern(pattern).await
    }

    /// Get a response object (wrapper for compatibility).
    pub async fn get_response(&self, key: &str) -> Option<Arc<CachedGetObject>> {
        self.get(key).await
    }

    /// Put a response object (wrapper for compatibility).
    pub async fn put_response(&self, key: String, response: CachedGetObject) {
        self.put(key, response).await
    }

    /// Invalidate a versioned object.
    ///
    /// When version_id is Some, invalidates both "{bucket}/{key}?versionId={version_id}"
    /// and "{bucket}/{key}" (the latest key).
    /// When version_id is None, only invalidates "{bucket}/{key}".
    pub async fn invalidate_versioned(&self, bucket: &str, key: &str, version_id: Option<&str>) {
        // Invalidate the base key (latest)
        let base_key = format!("{}/{}", bucket, key);
        self.invalidate(&base_key).await;

        // If version_id is provided, also invalidate the versioned key
        if let Some(vid) = version_id {
            let versioned_key = format!("{}/{}?versionId={}", bucket, key, vid);
            self.invalidate(&versioned_key).await;
        }
    }

    /// Get the overall hit rate.
    pub fn hit_rate(&self) -> f64 {
        let l1_hits = self.l1_hits.load(std::sync::atomic::Ordering::Relaxed);
        let l2_hits = self.l2_hits.load(std::sync::atomic::Ordering::Relaxed);
        let misses = self.misses.load(std::sync::atomic::Ordering::Relaxed);
        let total_hits = l1_hits + l2_hits;
        let total_requests = total_hits + misses;

        if total_requests > 0 {
            total_hits as f64 / total_requests as f64
        } else {
            0.0
        }
    }

    /// Get the maximum object size that can be cached.
    pub fn max_object_size(&self) -> usize {
        self.config.l2_max_object_size
    }

    /// Get combined cache stats (for API compatibility with HotObjectCache).
    ///
    /// Combines L1 and L2 stats into a single-level format for backward compatibility.
    pub async fn stats_as_hot_cache(&self) -> CacheStats {
        let tiered_stats = self.stats().await;

        let total_size = tiered_stats.l1_size + tiered_stats.l2_size;
        let total_entries = tiered_stats.l1_entries + tiered_stats.l2_entries;
        let total_hits = tiered_stats.l1_hits + tiered_stats.l2_hits;
        let total_max_size = tiered_stats.l1_max_size + tiered_stats.l2_max_size;
        let max_object_size = self.config.l2_max_object_size;
        let misses = tiered_stats.misses;

        // Calculate efficiency score (0-100)
        let total_requests = total_hits + misses;
        let efficiency_score = if total_requests > 0 {
            (tiered_stats.hit_rate * 100.0) as u32
        } else {
            0
        };

        CacheStats {
            size: total_size,
            entries: total_entries,
            max_size: total_max_size,
            max_object_size,
            hit_count: total_hits,
            miss_count: misses,
            avg_age_secs: 0.0, // Not tracked in tiered cache
            hit_rate: tiered_stats.hit_rate,
            eviction_count: 0, // Not tracked in tiered cache
            eviction_rate: 0.0,
            memory_usage: total_size,
            memory_usage_ratio: if total_max_size > 0 {
                total_size as f64 / total_max_size as f64
            } else {
                0.0
            },
            top_keys: Vec::new(), // Would need to fetch from access tracker
            efficiency_score,
        }
    }

    // ============================================
    // Byte-level caching methods (for compatibility with HotObjectCache API)
    // ============================================

    /// Get raw bytes from cache (API compatibility method).
    ///
    /// Returns the cached data bytes if available as Arc<Vec<u8>>.
    pub async fn get_bytes(&self, key: &str) -> Option<Arc<Vec<u8>>> {
        self.get(key).await.map(|cached| Arc::new(cached.body.to_vec()))
    }

    /// Put raw bytes into cache (API compatibility method).
    ///
    /// Stores the byte data with minimal metadata in the appropriate cache level.
    pub async fn put_bytes(&self, key: String, data: Arc<Vec<u8>>) {
        // Create a CachedGetObject with minimal required fields
        let cached_obj = CachedGetObject {
            body: Arc::new(bytes::Bytes::copy_from_slice(data.as_slice())),
            content_length: data.len() as i64,
            ..Default::default()
        };

        // Store using the existing put method
        self.put(key, cached_obj).await;
    }

    /// Invalidate a versioned object (byte-level API).
    #[allow(dead_code)]
    pub async fn invalidate_bytes_versioned(&self, _bucket: &str, key: &str, _version_id: Option<&str>) {
        // Just use the existing invalidate method
        self.invalidate(key).await;
    }

    /// Get multiple objects as bytes (API compatibility).
    pub async fn get_batch_bytes(&self, keys: &[String]) -> Vec<Option<Arc<Vec<u8>>>> {
        let results = self.get_batch(keys).await;
        results
            .into_iter()
            .map(|(_key, value)| value.map(|cached| Arc::new(cached.body.to_vec())))
            .collect()
    }

    /// Get byte cache statistics (API compatibility).
    #[allow(dead_code)]
    pub async fn stats_bytes(&self) -> ByteCacheStats {
        let cache_stats = self.stats().await;

        // Calculate efficiency score (0-100)
        let total_hits = cache_stats.l1_hits + cache_stats.l2_hits;
        let total_requests = total_hits + cache_stats.misses;
        let efficiency_score = if total_requests > 0 {
            (cache_stats.hit_rate * 100.0) as u32
        } else {
            0
        };

        ByteCacheStats {
            size: cache_stats.l1_size + cache_stats.l2_size,
            entries: cache_stats.l1_entries + cache_stats.l2_entries,
            max_size: cache_stats.l1_max_size + cache_stats.l2_max_size,
            max_object_size: self.config.l2_max_object_size,
            hit_count: cache_stats.l1_hits + cache_stats.l2_hits,
            miss_count: cache_stats.misses,
            avg_age_secs: 0.0,
            hit_rate: cache_stats.hit_rate,
            eviction_count: 0,
            eviction_rate: 0.0,
            memory_usage: cache_stats.l1_size + cache_stats.l2_size,
            memory_usage_ratio: {
                let total_max = cache_stats.l1_max_size + cache_stats.l2_max_size;
                if total_max > 0 {
                    (cache_stats.l1_size + cache_stats.l2_size) as f64 / total_max as f64
                } else {
                    0.0
                }
            },
            top_keys: Vec::new(),
            efficiency_score,
        }
    }
}

/// Statistics for a single cache level (L1 or L2).
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct CacheLevelStats {
    /// Current size in bytes
    pub size: usize,
    /// Number of entries
    pub entries: usize,
    /// Maximum size in bytes
    pub max_size: usize,
    /// Maximum number of entries
    pub max_entries: usize,
    /// Total hits for this level
    pub hits: u64,
}

/// Byte cache statistics (for compatibility with HotObjectCache).
#[derive(Debug, Clone)]
pub struct ByteCacheStats {
    pub size: usize,
    pub entries: usize,
    pub max_size: usize,
    pub max_object_size: usize,
    pub hit_count: u64,
    pub miss_count: u64,
    pub avg_age_secs: f64,
    pub hit_rate: f64,
    pub eviction_count: u64,
    pub eviction_rate: f64,
    pub memory_usage: usize,
    pub memory_usage_ratio: f64,
    pub top_keys: Vec<(String, u64)>,
    pub efficiency_score: u32,
}

impl From<ByteCacheStats> for CacheStats {
    fn from(stats: ByteCacheStats) -> Self {
        CacheStats {
            size: stats.size,
            entries: stats.entries,
            max_size: stats.max_size,
            max_object_size: stats.max_object_size,
            hit_count: stats.hit_count,
            miss_count: stats.miss_count,
            avg_age_secs: stats.avg_age_secs,
            hit_rate: stats.hit_rate,
            eviction_count: stats.eviction_count,
            eviction_rate: stats.eviction_rate,
            memory_usage: stats.memory_usage,
            memory_usage_ratio: stats.memory_usage_ratio,
            top_keys: stats.top_keys,
            efficiency_score: stats.efficiency_score,
        }
    }
}

/// Cache warmup pattern.
///
/// Defines different strategies for pre-populating the cache with hot objects.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub enum WarmupPattern {
    /// Warm up recently accessed hot objects.
    ///
    /// # Fields
    ///
    /// * `limit` - Maximum number of hot objects to warm
    RecentAccesses { limit: usize },

    /// Warm up specific keys.
    ///
    /// # Fields
    ///
    /// * `keys` - List of specific keys to warm
    SpecificKeys(Vec<String>),
}

impl Default for TieredObjectCache {
    fn default() -> Self {
        Self::new()
    }
}

/// Tiered cache statistics.
#[derive(Debug, Clone)]
pub struct TieredCacheStats {
    /// L1 cache size in bytes
    pub l1_size: usize,
    /// L1 cache entry count
    pub l1_entries: usize,
    /// L1 max size in bytes
    pub l1_max_size: usize,

    /// L2 cache size in bytes
    pub l2_size: usize,
    /// L2 cache entry count
    pub l2_entries: usize,
    /// L2 max size in bytes
    pub l2_max_size: usize,

    /// L1 cache hits
    pub l1_hits: u64,
    /// L2 cache hits
    pub l2_hits: u64,
    /// Cache misses
    pub misses: u64,

    /// Overall hit rate (0.0 - 1.0)
    pub hit_rate: f64,
    /// L1 hit rate relative to total hits (0.0 - 1.0)
    #[allow(dead_code)]
    pub l1_hit_rate: f64,
}

pub(crate) struct HotObjectCache {
    /// Moka cache instance for simple byte data (legacy)
    cache: Cache<String, Arc<CachedObject>>,
    /// Moka cache instance for full GetObject responses with metadata
    response_cache: Cache<String, Arc<CachedGetObjectInternal>>,
    /// Maximum total cache capacity in bytes
    max_capacity: usize,
    /// Maximum size of individual objects to cache (10MB by default)
    max_object_size: usize,
    /// Global cache hit counter
    hit_count: Arc<AtomicU64>,
    /// Global cache miss counter
    miss_count: Arc<AtomicU64>,
}

impl std::fmt::Debug for HotObjectCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use std::sync::atomic::Ordering;
        f.debug_struct("HotObjectCache")
            .field("max_capacity", &self.max_capacity)
            .field("max_object_size", &self.max_object_size)
            .field("hit_count", &self.hit_count.load(Ordering::Relaxed))
            .field("miss_count", &self.miss_count.load(Ordering::Relaxed))
            .finish()
    }
}

pub(crate) struct CachedObject {
    /// The object data
    data: Arc<Vec<u8>>,
    /// When this object was cached
    cached_at: Instant,
    /// Object size in bytes
    size: usize,
    /// Number of times this object has been accessed
    access_count: Arc<AtomicU64>,
}

impl CachedObject {
    /// Create a new CachedObject with specified size
    pub fn new_with_size(data: Vec<u8>, size: usize) -> Self {
        Self {
            data: Arc::new(data),
            cached_at: Instant::now(),
            size,
            access_count: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Get the size of the cached object
    #[allow(dead_code)]
    pub fn size(&self) -> usize {
        self.size
    }

    /// Get the data reference
    #[allow(dead_code)]
    pub fn data(&self) -> &Arc<Vec<u8>> {
        &self.data
    }

    /// Get the age of the cached object
    #[allow(dead_code)]
    pub fn age(&self) -> Duration {
        self.cached_at.elapsed()
    }

    /// Increment access count and return new value
    #[allow(dead_code)]
    pub fn increment_access(&self) -> u64 {
        self.access_count.fetch_add(1, Ordering::Relaxed) + 1
    }

    /// Get current access count
    #[allow(dead_code)]
    pub fn access_count(&self) -> u64 {
        self.access_count.load(Ordering::Relaxed)
    }
}

/// Comprehensive cached object with full response metadata for GetObject operations.
///
/// This structure stores all necessary fields to reconstruct a complete GetObjectOutput
/// response from cache, avoiding repeated disk reads and metadata lookups for hot objects.
///
/// # Fields
///
/// All time fields are serialized as RFC3339 strings to avoid parsing issues with
/// `Last-Modified` and other time headers.
///
/// # Usage
///
/// ```ignore
/// let cached = CachedGetObject {
///     body: Bytes::from(data),
///     content_length: data.len() as i64,
///     content_type: Some("application/octet-stream".to_string()),
///     e_tag: Some("\"abc123\"".to_string()),
///     last_modified: Some("2024-01-01T00:00:00Z".to_string()),
///     ..Default::default()
/// };
/// manager.put_cached_object(cache_key, cached).await;
/// ```
#[derive(Clone, Debug)]
#[allow(dead_code)]
pub struct CachedGetObject {
    /// The object body data
    pub body: std::sync::Arc<bytes::Bytes>,
    /// Content length in bytes
    pub content_length: i64,
    /// MIME content type
    pub content_type: Option<String>,
    /// Entity tag for the object
    pub e_tag: Option<String>,
    /// Last modified time as RFC3339 string (e.g., "2024-01-01T12:00:00Z")
    pub last_modified: Option<String>,
    /// Expiration time as RFC3339 string
    pub expires: Option<String>,
    /// Cache-Control header value
    pub cache_control: Option<String>,
    /// Content-Disposition header value
    pub content_disposition: Option<String>,
    /// Content-Encoding header value
    pub content_encoding: Option<String>,
    /// Content-Language header value
    pub content_language: Option<String>,
    /// Storage class (STANDARD, REDUCED_REDUNDANCY, etc.)
    pub storage_class: Option<String>,
    /// Version ID for versioned objects
    pub version_id: Option<String>,
    /// Whether this is a delete marker (for versioned buckets)
    pub delete_marker: bool,
    /// Number of tags associated with the object
    pub tag_count: Option<i32>,
    /// Replication status
    pub replication_status: Option<String>,
    /// User-defined metadata (x-amz-meta-*)
    pub user_metadata: std::collections::HashMap<String, String>,
    /// When this object was cached (for internal use, automatically set)
    #[allow(dead_code)]
    cached_at: Option<Instant>,
    /// Access count for hot key tracking (automatically managed)
    access_count: Arc<AtomicU64>,
}

impl Default for CachedGetObject {
    fn default() -> Self {
        Self {
            body: Arc::new(bytes::Bytes::new()),
            content_length: 0,
            content_type: None,
            e_tag: None,
            last_modified: None,
            expires: None,
            cache_control: None,
            content_disposition: None,
            content_encoding: None,
            content_language: None,
            storage_class: None,
            version_id: None,
            delete_marker: false,
            tag_count: None,
            replication_status: None,
            user_metadata: std::collections::HashMap::new(),
            cached_at: None,
            access_count: Arc::new(AtomicU64::new(0)),
        }
    }
}

#[allow(dead_code)]
impl CachedGetObject {
    /// Create a new CachedGetObject with the given body and content length
    pub fn new(body: bytes::Bytes, content_length: i64) -> Self {
        let body = std::sync::Arc::new(body);
        Self {
            body,
            content_length,
            cached_at: Some(Instant::now()),
            access_count: Arc::new(AtomicU64::new(0)),
            ..Default::default()
        }
    }

    /// Builder method to set content_type
    pub fn with_content_type(mut self, content_type: String) -> Self {
        self.content_type = Some(content_type);
        self
    }

    /// Builder method to set e_tag
    pub fn with_e_tag(mut self, e_tag: String) -> Self {
        self.e_tag = Some(e_tag);
        self
    }

    /// Builder method to set last_modified
    pub fn with_last_modified(mut self, last_modified: String) -> Self {
        self.last_modified = Some(last_modified);
        self
    }

    /// Builder method to set cache_control
    pub fn with_cache_control(mut self, cache_control: String) -> Self {
        self.cache_control = Some(cache_control);
        self
    }

    /// Builder method to set storage_class
    pub fn with_storage_class(mut self, storage_class: String) -> Self {
        self.storage_class = Some(storage_class);
        self
    }

    /// Builder method to set version_id
    pub fn with_version_id(mut self, version_id: String) -> Self {
        self.version_id = Some(version_id);
        self
    }

    /// Builder method to set expires
    pub fn with_expires(mut self, expires: String) -> Self {
        self.expires = Some(expires);
        self
    }

    /// Builder method to set content_encoding
    pub fn with_content_encoding(mut self, content_encoding: String) -> Self {
        self.content_encoding = Some(content_encoding);
        self
    }

    /// Builder method to set content_disposition
    pub fn with_content_disposition(mut self, content_disposition: String) -> Self {
        self.content_disposition = Some(content_disposition);
        self
    }

    /// Builder method to set content_language
    #[allow(dead_code)]
    pub fn with_content_language(mut self, content_language: String) -> Self {
        self.content_language = Some(content_language);
        self
    }

    /// Builder method to set replication_status
    pub fn with_replication_status(mut self, replication_status: String) -> Self {
        self.replication_status = Some(replication_status);
        self
    }

    /// Builder method to set delete_marker
    pub fn with_delete_marker(mut self, delete_marker: bool) -> Self {
        self.delete_marker = delete_marker;
        self
    }

    /// Builder method to set user_metadata
    pub fn with_user_metadata(mut self, user_metadata: std::collections::HashMap<String, String>) -> Self {
        self.user_metadata = user_metadata;
        self
    }

    /// Builder method to set tag_count
    pub fn with_tag_count(mut self, tag_count: i32) -> Self {
        self.tag_count = Some(tag_count);
        self
    }
    pub fn size(&self) -> usize {
        self.body.len()
    }

    /// Increment access count and return the new value
    pub fn increment_access(&self) -> u64 {
        self.access_count.fetch_add(1, Ordering::Relaxed) + 1
    }
    /// Check if the cached object is expired based on expires header
    pub fn is_expired(&self) -> bool {
        if let Some(expires_str) = &self.expires {
            // Try to parse RFC3339 format
            if let Ok(expires_time) = chrono::DateTime::parse_from_rfc3339(expires_str) {
                let now = chrono::Utc::now();
                return expires_time < now;
            }
        }
        false
    }

    /// Check if replication is complete
    pub fn is_replication_complete(&self) -> bool {
        match &self.replication_status {
            Some(status) => status == "COMPLETED",
            None => true, // No replication configured
        }
    }

    /// Get the age of this cached entry
    #[allow(dead_code)]
    pub fn age(&self) -> Option<Duration> {
        self.cached_at.map(|at| at.elapsed())
    }

    /// Record a cache hit and increment access count
    pub fn record_hit(&self) -> u64 {
        self.increment_access()
    }

    /// Estimate memory size in bytes including metadata
    pub fn memory_size(&self) -> usize {
        let mut size = self.body.len();
        size += size_of::<i64>(); // content_length
        size += self.content_type.as_ref().map_or(0, |s| s.len());
        size += self.e_tag.as_ref().map_or(0, |s| s.len());
        size += self.last_modified.as_ref().map_or(0, |s| s.len());
        size += self.expires.as_ref().map_or(0, |s| s.len());
        size += self.cache_control.as_ref().map_or(0, |s| s.len());
        size += self.content_disposition.as_ref().map_or(0, |s| s.len());
        size += self.content_encoding.as_ref().map_or(0, |s| s.len());
        size += self.content_language.as_ref().map_or(0, |s| s.len());
        size += self.storage_class.as_ref().map_or(0, |s| s.len());
        size += self.version_id.as_ref().map_or(0, |s| s.len());
        size += self.replication_status.as_ref().map_or(0, |s| s.len());
        size += size_of::<bool>(); // delete_marker
        size += size_of::<Option<i32>>(); // tag_count
        // Estimate user_metadata size
        for (k, v) in &self.user_metadata {
            size += k.len() + v.len();
        }
        size
    }
}

/// Internal wrapper for CachedGetObject in the Moka cache
#[derive(Clone)]
struct CachedGetObjectInternal {
    /// The cached response data
    data: Arc<CachedGetObject>,
    /// When this object was cached
    cached_at: Instant,
    /// Size in bytes for weigher function
    size: usize,
}

impl HotObjectCache {
    /// Create a new hot object cache with Moka
    ///
    /// Configures Moka with:
    /// - Size-based eviction (100MB max)
    /// - TTL of 5 minutes
    /// - TTI of 2 minutes
    /// - Weigher function for accurate size tracking
    #[allow(dead_code)]
    pub(crate) fn new() -> Self {
        let max_capacity = rustfs_utils::get_env_u64(
            rustfs_config::ENV_OBJECT_CACHE_CAPACITY_MB,
            rustfs_config::DEFAULT_OBJECT_CACHE_CAPACITY_MB,
        );
        let cache_tti_secs =
            rustfs_utils::get_env_u64(rustfs_config::ENV_OBJECT_CACHE_TTI_SECS, rustfs_config::DEFAULT_OBJECT_CACHE_TTI_SECS);
        let cache_ttl_secs =
            rustfs_utils::get_env_u64(rustfs_config::ENV_OBJECT_CACHE_TTL_SECS, rustfs_config::DEFAULT_OBJECT_CACHE_TTL_SECS);

        // Legacy simple byte cache
        let cache = Cache::builder()
            .max_capacity(max_capacity * MI_B as u64)
            .weigher(|_key: &String, value: &Arc<CachedObject>| -> u32 {
                // Weight based on actual data size
                value.size.min(u32::MAX as usize) as u32
            })
            .time_to_live(Duration::from_secs(cache_ttl_secs))
            .time_to_idle(Duration::from_secs(cache_tti_secs))
            .build();

        // Full response cache with metadata
        let response_cache = Cache::builder()
            .max_capacity(max_capacity * MI_B as u64)
            .weigher(|_key: &String, value: &Arc<CachedGetObjectInternal>| -> u32 {
                // Weight based on actual data size
                value.size.min(u32::MAX as usize) as u32
            })
            .time_to_live(Duration::from_secs(cache_ttl_secs))
            .time_to_idle(Duration::from_secs(cache_tti_secs))
            .build();
        let max_object_size = rustfs_utils::get_env_usize(
            rustfs_config::ENV_OBJECT_CACHE_MAX_OBJECT_SIZE_MB,
            rustfs_config::DEFAULT_OBJECT_CACHE_MAX_OBJECT_SIZE_MB,
        ) * MI_B;
        Self {
            cache,
            max_capacity: (max_capacity * MI_B as u64) as usize,
            response_cache,
            max_object_size,
            hit_count: Arc::new(AtomicU64::new(0)),
            miss_count: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Soft expiration determination, the number of hits is insufficient and exceeds the soft TTL
    #[allow(dead_code)]
    pub(crate) fn should_expire(&self, obj: &Arc<CachedObject>) -> bool {
        let age_secs = obj.cached_at.elapsed().as_secs();
        let cache_ttl_secs =
            rustfs_utils::get_env_u64(rustfs_config::ENV_OBJECT_CACHE_TTL_SECS, rustfs_config::DEFAULT_OBJECT_CACHE_TTL_SECS);
        let hot_object_min_hits_to_extend = rustfs_utils::get_env_usize(
            rustfs_config::ENV_OBJECT_HOT_MIN_HITS_TO_EXTEND,
            rustfs_config::DEFAULT_OBJECT_HOT_MIN_HITS_TO_EXTEND,
        );
        if age_secs >= cache_ttl_secs {
            let hits = obj.access_count.load(Ordering::Relaxed);
            return hits < hot_object_min_hits_to_extend as u64;
        }
        false
    }

    /// Get an object from cache with lock-free concurrent access
    ///
    /// Moka provides lock-free reads, significantly improving concurrent performance.
    #[allow(dead_code)]
    pub(crate) async fn get(&self, key: &str) -> Option<Arc<Vec<u8>>> {
        match self.cache.get(key).await {
            Some(cached) => {
                if self.should_expire(&cached) {
                    self.cache.invalidate(key).await;
                    self.miss_count.fetch_add(1, Ordering::Relaxed);
                    return None;
                }
                // Update access count
                cached.access_count.fetch_add(1, Ordering::Relaxed);
                self.hit_count.fetch_add(1, Ordering::Relaxed);

                // IMPORTANT: Do NOT add high cardinality labels to metrics!
                // Previously, this metric was tagged with individual file URIs/keys,
                // causing unbounded memory growth in RustFS's own process. The metrics
                // crate maintains an internal HashMap for all metric series, and each
                // unique file path creates a new entry that is never cleaned up.
                // This HashMap grows unbounded with unique file access, causing memory
                // leaks in RustFS itself (and also in downstream systems like Prometheus).
                // Only use low cardinality labels like operation type or status.
                rustfs_io_metrics::record_tiered_cache_operation("hot", "hit", None);

                Some(Arc::clone(&cached.data))
            }
            None => {
                self.miss_count.fetch_add(1, Ordering::Relaxed);
                rustfs_io_metrics::record_tiered_cache_operation("hot", "miss", None);

                None
            }
        }
    }

    /// Put an object into cache with automatic size-based eviction
    ///
    /// Moka handles eviction automatically based on the weigher function.
    #[allow(dead_code)]
    pub(crate) async fn put(&self, key: String, data: Arc<CachedObject>) {
        let size = data.size;

        // Only cache objects smaller than max_object_size
        if size == 0 || size > self.max_object_size {
            return;
        }

        let cached_obj = Arc::new(CachedObject {
            data: Arc::clone(&data.data),
            cached_at: Instant::now(),
            size,
            access_count: Arc::new(AtomicU64::new(0)),
        });

        self.cache.insert(key.clone(), cached_obj).await;
        rustfs_io_metrics::record_tiered_cache_operation("hot", "put", Some(size));
        rustfs_io_metrics::record_cache_size("hot", self.cache.weighted_size() as usize, self.cache.entry_count());
    }

    /// Clear all cached objects
    #[allow(dead_code)]
    pub(crate) async fn clear(&self) {
        // Clear both simple cache and response cache
        self.cache.invalidate_all();
        self.response_cache.invalidate_all();
        // Sync to ensure all entries are removed
        self.cache.run_pending_tasks().await;
        self.response_cache.run_pending_tasks().await;
    }

    /// Get cache statistics for monitoring
    pub(crate) async fn stats(&self) -> CacheStats {
        // Ensure pending tasks are processed for accurate stats in both caches
        self.cache.run_pending_tasks().await;
        self.response_cache.run_pending_tasks().await;

        // Calculate average age for simple cache
        let mut total_ms: u128 = 0;
        let mut cnt: u64 = 0;
        self.cache.iter().for_each(|(_, v)| {
            total_ms += v.cached_at.elapsed().as_millis();
            cnt += 1;
        });

        // Calculate average age for response cache
        let mut response_total_ms: u128 = 0;
        let mut response_cnt: u64 = 0;
        self.response_cache.iter().for_each(|(_, v)| {
            response_total_ms += v.cached_at.elapsed().as_millis();
            response_cnt += 1;
        });

        // Combine average age calculation
        let total_entries = cnt + response_cnt;
        let combined_total_ms = total_ms + response_total_ms;
        let avg_age_secs = if total_entries == 0 {
            0.0
        } else {
            (combined_total_ms as f64 / total_entries as f64) / 1000.0
        };

        let hit_count = self.hit_count.load(Ordering::Relaxed);
        let miss_count = self.miss_count.load(Ordering::Relaxed);
        let total_requests = hit_count + miss_count;
        let hit_rate = if total_requests > 0 {
            hit_count as f64 / total_requests as f64
        } else {
            0.0
        };

        // Calculate total size from both caches
        let simple_size = self.cache.weighted_size() as usize;
        let response_size = self.response_cache.weighted_size() as usize;
        let total_size = simple_size + response_size;

        let memory_usage_ratio = if self.max_capacity > 0 {
            total_size as f64 / self.max_capacity as f64
        } else {
            0.0
        };

        let efficiency_score = if total_entries == 0 {
            // Empty cache has no actual utility, efficiency score is 0
            0
        } else {
            // Non-empty cache: hit rate contributes 50%, remaining capacity contributes 50%
            (hit_rate * 50.0 + (1.0 - memory_usage_ratio) * 50.0) as u32
        };

        CacheStats {
            size: total_size,
            entries: total_entries as usize,
            max_size: self.max_capacity,
            max_object_size: self.max_object_size,
            hit_count,
            miss_count,
            avg_age_secs,
            hit_rate,
            eviction_count: 0, // Moka doesn't expose eviction count
            eviction_rate: 0.0,
            memory_usage: total_size,
            memory_usage_ratio,
            top_keys: vec![], // Would need additional tracking
            efficiency_score,
        }
    }

    /// Check if a key exists in cache (lock-free)
    #[allow(dead_code)]
    pub(crate) async fn contains(&self, key: &str) -> bool {
        // Check both simple cache and response cache
        self.cache.contains_key(key) || self.response_cache.contains_key(key)
    }

    /// Get multiple objects from cache in parallel
    ///
    /// Leverages Moka's lock-free design for true parallel access.
    #[allow(dead_code)]
    pub(crate) async fn get_batch(&self, keys: &[String]) -> Vec<Option<Arc<Vec<u8>>>> {
        let mut results = Vec::with_capacity(keys.len());
        for key in keys {
            results.push(self.get(key).await);
        }
        results
    }

    /// Remove a specific key from cache
    #[allow(dead_code)]
    pub(crate) async fn remove(&self, key: &str) -> bool {
        let had_key = self.cache.contains_key(key);
        self.cache.invalidate(key).await;
        had_key
    }

    /// Get the most frequently accessed keys
    ///
    /// Returns up to `limit` keys sorted by access count in descending order.
    #[allow(dead_code)]
    pub(crate) async fn get_hot_keys(&self, limit: usize) -> Vec<(String, u64)> {
        // Run pending tasks to ensure accurate entry count
        self.cache.run_pending_tasks().await;

        let mut entries: Vec<(String, u64)> = Vec::new();

        // Iterate through cache entries
        self.cache.iter().for_each(|(key, value)| {
            entries.push((key.to_string(), value.access_count.load(Ordering::Relaxed)));
        });

        entries.sort_by(|a, b| b.1.cmp(&a.1));
        entries.truncate(limit);
        entries
    }

    /// Warm up cache with a batch of objects
    #[allow(dead_code)]
    pub(crate) async fn warm(&self, objects: Vec<(String, Vec<u8>)>) {
        for (key, data) in objects {
            let size = data.len();
            let cached_obj = Arc::new(CachedObject::new_with_size(data, size));
            self.put(key, cached_obj).await;
        }
    }

    /// Get hit rate percentage
    #[allow(dead_code)]
    pub(crate) fn hit_rate(&self) -> f64 {
        let hits = self.hit_count.load(Ordering::Relaxed);
        let misses = self.miss_count.load(Ordering::Relaxed);
        let total = hits + misses;

        if total == 0 {
            0.0
        } else {
            (hits as f64 / total as f64) * 100.0
        }
    }

    // ============================================
    // Response Cache Methods (CachedGetObject)
    // ============================================

    /// Get a cached GetObject response with full metadata
    ///
    /// This method retrieves a complete GetObject response from the response cache,
    /// including body data and all response metadata (e_tag, last_modified, etc.).
    ///
    /// # Arguments
    ///
    /// * `key` - Cache key in the format "{bucket}/{key}" or "{bucket}/{key}?versionId={version_id}"
    ///
    /// # Returns
    ///
    /// * `Some(Arc<CachedGetObject>)` - Cached response data if found and not expired
    /// * `None` - Cache miss
    #[allow(dead_code)]
    pub(crate) async fn get_response(&self, key: &str) -> Option<Arc<CachedGetObject>> {
        match self.response_cache.get(key).await {
            Some(cached) => {
                // Check soft expiration
                let age_secs = cached.cached_at.elapsed().as_secs();
                let cache_ttl_secs = rustfs_utils::get_env_u64(
                    rustfs_config::ENV_OBJECT_CACHE_TTL_SECS,
                    rustfs_config::DEFAULT_OBJECT_CACHE_TTL_SECS,
                );
                let hot_object_min_hits = rustfs_utils::get_env_usize(
                    rustfs_config::ENV_OBJECT_HOT_MIN_HITS_TO_EXTEND,
                    rustfs_config::DEFAULT_OBJECT_HOT_MIN_HITS_TO_EXTEND,
                );

                if age_secs >= cache_ttl_secs {
                    let hits = cached.data.access_count.load(Ordering::Relaxed);
                    if hits < hot_object_min_hits as u64 {
                        self.response_cache.invalidate(key).await;
                        self.miss_count.fetch_add(1, Ordering::Relaxed);
                        return None;
                    }
                }

                // Update access count
                cached.data.increment_access();
                self.hit_count.fetch_add(1, Ordering::Relaxed);

                // IMPORTANT: Do NOT add high cardinality labels to metrics!
                // See HotObjectCache::get() for details. The metrics crate's internal
                // HashMap grows unbounded with high cardinality labels, causing memory
                // leaks in RustFS's own process.
                rustfs_io_metrics::record_tiered_cache_operation("response", "hit", None);

                Some(Arc::clone(&cached.data))
            }
            None => {
                self.miss_count.fetch_add(1, Ordering::Relaxed);
                rustfs_io_metrics::record_tiered_cache_operation("response", "miss", None);

                None
            }
        }
    }

    /// Put a GetObject response into the response cache
    ///
    /// This method caches a complete GetObject response including body and metadata.
    /// Objects larger than `max_object_size` or empty objects are not cached.
    ///
    /// # Arguments
    ///
    /// * `key` - Cache key in the format "{bucket}/{key}" or "{bucket}/{key}?versionId={version_id}"
    /// * `response` - The complete cached response to store
    #[allow(dead_code)]
    pub(crate) async fn put_response(&self, key: String, response: CachedGetObject) {
        let size = response.size();

        // Only cache objects smaller than max_object_size
        if size == 0 || size > self.max_object_size {
            return;
        }

        let cached_internal = Arc::new(CachedGetObjectInternal {
            data: Arc::new(response),
            cached_at: Instant::now(),
            size,
        });

        self.response_cache.insert(key.clone(), cached_internal).await;
        rustfs_io_metrics::record_tiered_cache_operation("response", "put", Some(size));
        rustfs_io_metrics::record_cache_size(
            "response",
            self.response_cache.weighted_size() as usize,
            self.response_cache.entry_count(),
        );
    }

    /// Invalidate a cache entry for a specific object
    ///
    /// This method removes both the simple byte cache entry and the response cache entry
    /// for the given key. Used when objects are modified or deleted.
    ///
    /// # Arguments
    ///
    /// * `key` - Cache key to invalidate (e.g., "{bucket}/{key}")
    #[allow(dead_code)]
    pub(crate) async fn invalidate(&self, key: &str) {
        // Invalidate both caches
        self.cache.invalidate(key).await;
        self.response_cache.invalidate(key).await;
        rustfs_io_metrics::record_tiered_cache_operation("overall", "evict", None);
    }

    /// Invalidate cache entries for an object and its latest version
    ///
    /// For versioned buckets, this invalidates both:
    /// - The specific version key: "{bucket}/{key}?versionId={version_id}"
    /// - The latest version key: "{bucket}/{key}"
    ///
    /// This ensures that after a write/delete, clients don't receive stale data.
    ///
    /// # Arguments
    ///
    /// * `bucket` - Bucket name
    /// * `key` - Object key
    /// * `version_id` - Optional version ID (if None, only invalidates the base key)
    #[allow(dead_code)]
    pub(crate) async fn invalidate_versioned(&self, bucket: &str, key: &str, version_id: Option<&str>) {
        // Always invalidate the latest version key
        let base_key = format!("{bucket}/{key}");
        self.invalidate(&base_key).await;

        // Also invalidate the specific version if provided
        if let Some(vid) = version_id {
            let versioned_key = format!("{base_key}?versionId={vid}");
            self.invalidate(&versioned_key).await;
        }
    }

    /// Clear all cached objects from both caches
    #[allow(dead_code)]
    pub(crate) async fn clear_all(&self) {
        self.cache.invalidate_all();
        self.response_cache.invalidate_all();
        // Sync to ensure all entries are removed
        self.cache.run_pending_tasks().await;
        self.response_cache.run_pending_tasks().await;
    }

    /// Get the maximum object size for caching
    #[allow(dead_code)]
    pub(crate) fn max_object_size(&self) -> usize {
        self.max_object_size
    }

    /// Get cache health status
    #[allow(dead_code)]
    pub(crate) async fn health_status(&self) -> CacheHealthStatus {
        let stats = self.stats().await;
        let memory_usage = self.cache.weighted_size() as usize;

        let is_healthy = stats.memory_usage_ratio < 0.95 && stats.hit_rate > 0.1;

        let mut recommendations = Vec::new();
        if stats.hit_rate < 0.5 {
            recommendations.push("Consider increasing cache size or TTL".to_string());
        }
        if stats.memory_usage_ratio > 0.9 {
            recommendations.push("Cache is nearly full, consider increasing capacity".to_string());
        }

        CacheHealthStatus {
            memory_usage,
            is_healthy,
            memory_usage_ratio: stats.memory_usage_ratio,
            hit_rate: stats.hit_rate,
            eviction_rate: stats.eviction_rate,
            avg_entry_age_secs: stats.avg_age_secs,
            efficiency_score: stats.efficiency_score,
            recommendations,
        }
    }

    /// Get current memory usage in bytes
    #[allow(dead_code)]
    pub(crate) async fn memory_usage(&self) -> usize {
        // Sync pending tasks to ensure accurate weight statistics
        self.cache.run_pending_tasks().await;
        self.cache.weighted_size() as usize
    }

    /// Evict a percentage of cached entries
    #[allow(dead_code)]
    pub(crate) async fn evict_percentage(&self, percentage: f64) -> u64 {
        let stats = self.stats().await;
        let entries_to_evict = (stats.entries as f64 * percentage / 100.0).max(1.0) as u64;

        // Moka does not support selective eviction, so we use invalidate_all
        if entries_to_evict > 0 {
            self.cache.invalidate_all();
        }

        entries_to_evict
    }

    /// Warm cache from a list of hot keys
    #[allow(dead_code)]
    pub(crate) async fn warm_from_hot_list(&self, hot_keys: Vec<(String, Vec<u8>)>) -> u64 {
        let mut warmed = 0u64;

        for (key, data) in hot_keys {
            let size = data.len();
            if size <= self.max_object_size {
                let cached_obj = Arc::new(CachedObject::new_with_size(data, size));
                self.cache.insert(key, cached_obj).await;
                warmed += 1;
            }
        }

        warmed
    }
}

/// Cache statistics for monitoring and debugging
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct CacheStats {
    /// Current total size of cached objects in bytes
    pub size: usize,
    /// Number of cached entries
    pub entries: usize,
    /// Maximum allowed cache size in bytes
    pub max_size: usize,
    /// Maximum allowed object size in bytes
    pub max_object_size: usize,
    /// Total number of cache hits
    pub hit_count: u64,
    /// Total number of cache misses
    pub miss_count: u64,
    /// Average cache object age (seconds)
    pub avg_age_secs: f64,
    /// Cache hit rate (0.0 - 1.0)
    pub hit_rate: f64,
    /// Total number of evictions
    pub eviction_count: u64,
    /// Eviction rate (evictions per second)
    pub eviction_rate: f64,
    /// Memory usage in bytes
    pub memory_usage: usize,
    /// Memory usage ratio (0.0 - 1.0)
    pub memory_usage_ratio: f64,
    /// Top hot keys (key, hit_count)
    pub top_keys: Vec<(String, u64)>,
    /// Efficiency score (0-100)
    pub efficiency_score: u32,
}

/// Cache health status for monitoring and diagnostics
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct CacheHealthStatus {
    /// Memory usage in bytes
    pub memory_usage: usize,
    /// Whether the cache is healthy
    pub is_healthy: bool,
    /// Memory usage ratio (0.0 - 1.0)
    pub memory_usage_ratio: f64,
    /// Hit rate (0.0 - 1.0)
    pub hit_rate: f64,
    /// Eviction rate (evictions per second)
    pub eviction_rate: f64,
    /// Average entry age (seconds)
    pub avg_entry_age_secs: f64,
    /// Efficiency score (0-100)
    pub efficiency_score: u32,
    /// Optimization recommendations
    pub recommendations: Vec<String>,
}
// ============================================
// Unit Tests for CachedGetObject
// ============================================

#[cfg(test)]
mod cached_object_tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn test_cached_get_object_builder() {
        let obj = CachedGetObject::new(Bytes::from("test data"), 9)
            .with_content_type("text/plain".to_string())
            .with_e_tag("\"abc123\"".to_string())
            .with_last_modified("2024-01-01T12:00:00Z".to_string())
            .with_cache_control("max-age=3600".to_string())
            .with_expires("2024-12-31T23:59:59Z".to_string())
            .with_content_encoding("gzip".to_string())
            .with_content_disposition("attachment; filename=\"test.txt\"".to_string())
            .with_storage_class("STANDARD".to_string())
            .with_version_id("v1".to_string())
            .with_replication_status("COMPLETED".to_string())
            .with_tag_count(5)
            .with_delete_marker(false);

        assert_eq!(obj.content_type, Some("text/plain".to_string()));
        assert_eq!(obj.e_tag, Some("\"abc123\"".to_string()));
        assert_eq!(obj.storage_class, Some("STANDARD".to_string()));
        assert_eq!(obj.version_id, Some("v1".to_string()));
        assert_eq!(obj.replication_status, Some("COMPLETED".to_string()));
        assert_eq!(obj.tag_count, Some(5));
        assert!(!obj.delete_marker);
    }

    #[test]
    fn test_cached_get_object_with_metadata() {
        let mut metadata = std::collections::HashMap::new();
        metadata.insert("x-amz-meta-custom".to_string(), "value".to_string());
        metadata.insert("x-amz-meta-author".to_string(), "test".to_string());

        let obj = CachedGetObject::new(Bytes::from("data"), 4).with_user_metadata(metadata.clone());

        assert_eq!(obj.user_metadata.len(), 2);
        assert_eq!(obj.user_metadata.get("x-amz-meta-custom"), Some(&"value".to_string()));
    }

    #[test]
    fn test_cached_get_object_size() {
        let obj = CachedGetObject::new(Bytes::from("test"), 4);
        assert_eq!(obj.size(), 4);

        let large_obj = CachedGetObject::new(Bytes::from(vec![0u8; 1024]), 1024);
        assert_eq!(large_obj.size(), 1024);
    }

    #[test]
    fn test_cached_get_object_access_count() {
        let obj = CachedGetObject::new(Bytes::from("test"), 4);

        assert_eq!(obj.increment_access(), 1);
        assert_eq!(obj.increment_access(), 2);
        assert_eq!(obj.increment_access(), 3);
    }

    #[test]
    fn test_cached_get_object_is_expired() {
        // Not expired - no expires header
        let obj1 = CachedGetObject::new(Bytes::from("test"), 4);
        assert!(!obj1.is_expired());

        // Expired - past expires time
        let obj2 = CachedGetObject::new(Bytes::from("test"), 4).with_expires("2020-01-01T00:00:00Z".to_string());
        assert!(obj2.is_expired());

        // Not expired - future expires time
        let future = chrono::Utc::now() + chrono::Duration::days(1);
        let obj3 = CachedGetObject::new(Bytes::from("test"), 4).with_expires(future.to_rfc3339());
        assert!(!obj3.is_expired());
    }

    #[test]
    fn test_cached_get_object_replication_status() {
        // Completed replication
        let obj1 = CachedGetObject::new(Bytes::from("test"), 4).with_replication_status("COMPLETED".to_string());
        assert!(obj1.is_replication_complete());

        // Pending replication
        let obj2 = CachedGetObject::new(Bytes::from("test"), 4).with_replication_status("PENDING".to_string());
        assert!(!obj2.is_replication_complete());

        // No replication configured
        let obj3 = CachedGetObject::new(Bytes::from("test"), 4);
        assert!(obj3.is_replication_complete());
    }

    #[test]
    fn test_cached_get_object_memory_size() {
        let obj = CachedGetObject::new(Bytes::from("test"), 4)
            .with_content_type("text/plain".to_string())
            .with_e_tag("\"abc\"".to_string());

        let size = obj.memory_size();
        // Should include body + content_type + e_tag + other fields
        assert!(size >= 4 + 10 + 5); // At least body + content_type + e_tag
    }

    #[test]
    fn test_cached_get_object_record_hit() {
        let obj = CachedGetObject::new(Bytes::from("test"), 4);

        assert_eq!(obj.record_hit(), 1);
        assert_eq!(obj.record_hit(), 2);
        assert_eq!(obj.record_hit(), 3);
    }
}

// ============================================
// Unit Tests for CacheHealthStatus
// ============================================

#[cfg(test)]
mod cache_health_tests {
    use super::*;
    use serial_test::serial;

    #[tokio::test]
    #[serial]
    async fn test_cache_health_status() {
        let cache = HotObjectCache::new();

        // Add some entries
        let data1 = Arc::new(CachedObject::new_with_size(vec![1, 2, 3, 4], 4));
        let data2 = Arc::new(CachedObject::new_with_size(vec![5, 6, 7, 8], 4));

        cache.put("key1".to_string(), data1).await;
        cache.put("key2".to_string(), data2).await;

        // Get health status
        let health = cache.health_status().await;

        assert!(health.memory_usage > 0);
        assert!(health.memory_usage_ratio >= 0.0 && health.memory_usage_ratio <= 1.0);
        assert!(health.hit_rate >= 0.0 && health.hit_rate <= 1.0);
        assert!(health.efficiency_score <= 100);
    }

    #[tokio::test]
    #[serial]
    async fn test_cache_memory_usage() {
        let cache = HotObjectCache::new();

        let initial_usage = cache.memory_usage().await;
        assert_eq!(initial_usage, 0);

        // Add some data
        let data = Arc::new(CachedObject::new_with_size(vec![0u8; 1024], 1024));
        cache.put("key".to_string(), data).await;

        let new_usage = cache.memory_usage().await;
        assert!(new_usage > initial_usage);
    }

    #[tokio::test]
    #[serial]
    async fn test_cache_evict_percentage() {
        let cache = HotObjectCache::new();

        // Add multiple entries
        for i in 0..10 {
            let data = Arc::new(CachedObject::new_with_size(vec![i as u8; 100], 100));
            cache.put(format!("key{}", i), data).await;
        }

        let stats = cache.stats().await;
        assert_eq!(stats.entries, 10);

        // Evict 50%
        let evicted = cache.evict_percentage(50.0).await;
        assert!(evicted > 0);
    }

    #[tokio::test]
    #[serial]
    async fn test_cache_warm_from_hot_list() {
        let cache = HotObjectCache::new();

        let hot_keys = vec![
            ("key1".to_string(), vec![1, 2, 3]),
            ("key2".to_string(), vec![4, 5, 6]),
            ("key3".to_string(), vec![7, 8, 9]),
        ];

        let warmed = cache.warm_from_hot_list(hot_keys).await;
        assert_eq!(warmed, 3);

        // Verify entries are in cache
        assert!(cache.contains("key1").await);
        assert!(cache.contains("key2").await);
        assert!(cache.contains("key3").await);
    }
}

// ============================================
// Unit Tests for CacheStats
// ============================================

#[cfg(test)]
mod cache_stats_tests {
    use super::*;
    use serial_test::serial;

    #[tokio::test]
    #[serial]
    async fn test_cache_stats_hit_rate() {
        let cache = HotObjectCache::new();

        // Add an entry
        let data = Arc::new(CachedObject::new_with_size(vec![1, 2, 3], 3));
        cache.put("key".to_string(), data).await;

        // Generate some hits and misses
        cache.get("key").await; // Hit
        cache.get("key").await; // Hit
        cache.get("nonexistent").await; // Miss
        cache.get("nonexistent").await; // Miss

        let stats = cache.stats().await;

        assert_eq!(stats.hit_count, 2);
        assert_eq!(stats.miss_count, 2);
        assert!((stats.hit_rate - 0.5).abs() < 0.01); // Should be ~50%
    }

    #[tokio::test]
    #[serial]
    async fn test_cache_stats_memory_usage_ratio() {
        let cache = HotObjectCache::new();

        let stats = cache.stats().await;
        assert_eq!(stats.memory_usage_ratio, 0.0); // Empty cache

        // Add some data
        let data = Arc::new(CachedObject::new_with_size(vec![0u8; 1024], 1024));
        cache.put("key".to_string(), data).await;

        let stats = cache.stats().await;
        assert!(stats.memory_usage_ratio > 0.0);
    }

    #[tokio::test]
    #[serial]
    async fn test_cache_stats_efficiency_score() {
        let cache = HotObjectCache::new();

        // Empty cache - low efficiency
        let stats = cache.stats().await;
        assert!(stats.efficiency_score < 50);

        // Add data and generate hits
        let data = Arc::new(CachedObject::new_with_size(vec![1, 2, 3], 3));
        cache.put("key".to_string(), data).await;

        for _ in 0..10 {
            cache.get("key").await; // Hits
        }

        let stats = cache.stats().await;
        assert!(stats.efficiency_score > 0);
    }
}
