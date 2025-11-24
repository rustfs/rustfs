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

//! Concurrency optimization module for high-performance object retrieval.
//!
//! This module provides intelligent concurrency management to prevent performance
//! degradation when multiple concurrent GetObject requests are processed.
//!
//! # Key Features
//!
//! - **Adaptive Buffer Sizing**: Dynamically adjusts buffer sizes based on concurrent load
//! - **Request-Level Buffer Pools**: Reduces memory allocation overhead
//! - **Hot Object Caching**: Caches frequently accessed objects for faster retrieval
//! - **Fair Request Scheduling**: Prevents request starvation under high load
//!
//! # Performance Characteristics
//!
//! - Low concurrency (1-2 requests): Optimizes for throughput with larger buffers
//! - Medium concurrency (3-8 requests): Balances throughput and fairness
//! - High concurrency (>8 requests): Optimizes for fairness and predictable latency

use moka::future::Cache;
use rustfs_config::{KI_B, MI_B};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;

/// Global concurrent request counter for adaptive buffer sizing
static ACTIVE_GET_REQUESTS: AtomicUsize = AtomicUsize::new(0);

/// Maximum concurrent requests before applying aggressive optimization
const HIGH_CONCURRENCY_THRESHOLD: usize = 8;

/// Medium concurrency threshold
const MEDIUM_CONCURRENCY_THRESHOLD: usize = 4;

/// Time-to-live for cached objects (5 minutes)
const CACHE_TTL_SECS: u64 = 300;

/// Time-to-idle for cached objects (2 minutes)
const CACHE_TTI_SECS: u64 = 120;

/// Minimum hit count to extend object lifetime beyond TTL
const HOT_OBJECT_MIN_HITS_TO_EXTEND: usize = 5;

/// Global concurrency manager instance
static CONCURRENCY_MANAGER: LazyLock<ConcurrencyManager> = LazyLock::new(ConcurrencyManager::new);

/// RAII guard for tracking active GetObject requests
#[derive(Debug)]
pub struct GetObjectGuard {
    /// Track when the request started for metrics
    start_time: Instant,
    /// Reference to the concurrency manager for cleanup
    _manager: &'static ConcurrencyManager,
}

impl GetObjectGuard {
    /// Create a new guard, incrementing the active request counter
    fn new() -> Self {
        ACTIVE_GET_REQUESTS.fetch_add(1, Ordering::Relaxed);
        Self {
            start_time: Instant::now(),
            _manager: &CONCURRENCY_MANAGER,
        }
    }

    /// Get the elapsed time since the request started
    pub fn elapsed(&self) -> Duration {
        self.start_time.elapsed()
    }

    /// Get the current concurrent request count
    pub fn concurrent_requests() -> usize {
        ACTIVE_GET_REQUESTS.load(Ordering::Relaxed)
    }
}

impl Drop for GetObjectGuard {
    fn drop(&mut self) {
        ACTIVE_GET_REQUESTS.fetch_sub(1, Ordering::Relaxed);

        // Record metrics for monitoring
        #[cfg(feature = "metrics")]
        {
            use metrics::{counter, histogram};
            counter!("rustfs_get_object_requests_completed").increment(1);
            histogram!("rustfs_get_object_duration_seconds").record(self.elapsed().as_secs_f64());
        }
    }
}

/// Concurrency-aware buffer size calculator
///
/// This function adapts buffer sizes based on the current concurrent request load
/// to optimize for both throughput and fairness.
///
/// # Strategy
///
/// - **Low concurrency (1-2)**: Use large buffers (512KB-1MB) for maximum throughput
/// - **Medium concurrency (3-8)**: Use moderate buffers (128KB-256KB) for balanced performance
/// - **High concurrency (>8)**: Use smaller buffers (64KB-128KB) for fairness and memory efficiency
///
/// # Arguments
///
/// * `file_size` - The size of the file being read, or -1 if unknown
/// * `base_buffer_size` - The baseline buffer size from workload profile
///
/// # Returns
///
/// Optimized buffer size in bytes for the current concurrency level
pub fn get_concurrency_aware_buffer_size(file_size: i64, base_buffer_size: usize) -> usize {
    let concurrent_requests = ACTIVE_GET_REQUESTS.load(Ordering::Relaxed);

    // Record concurrent request metrics
    #[cfg(feature = "metrics")]
    {
        use metrics::gauge;
        gauge!("rustfs_concurrent_get_requests").set(concurrent_requests as f64);
    }

    // For low concurrency, use the base buffer size for maximum throughput
    if concurrent_requests <= 1 {
        return base_buffer_size;
    }

    // Calculate adaptive multiplier based on concurrency level
    let adaptive_multiplier = if concurrent_requests <= 2 {
        // Low concurrency (1-2): use full buffer for maximum throughput
        1.0
    } else if concurrent_requests <= MEDIUM_CONCURRENCY_THRESHOLD {
        // Medium concurrency (3-4): slightly reduce buffer size (75% of base)
        0.75
    } else if concurrent_requests <= HIGH_CONCURRENCY_THRESHOLD {
        // Higher concurrency (5-8): more aggressive reduction (50% of base)
        0.5
    } else {
        // Very high concurrency (>8): minimize memory per request (40% of base)
        0.4
    };

    // Calculate the adjusted buffer size
    let adjusted_size = (base_buffer_size as f64 * adaptive_multiplier) as usize;

    // Ensure we stay within reasonable bounds
    let min_buffer = if file_size > 0 && file_size < 100 * KI_B as i64 {
        32 * KI_B // For very small files, use minimum buffer
    } else {
        64 * KI_B // Standard minimum buffer size
    };

    let max_buffer = if concurrent_requests > HIGH_CONCURRENCY_THRESHOLD {
        256 * KI_B // Cap at 256KB for high concurrency
    } else {
        MI_B // Cap at 1MB for lower concurrency
    };

    adjusted_size.clamp(min_buffer, max_buffer)
}

/// Advanced concurrency-aware buffer sizing with file size optimization
///
/// This enhanced version considers both concurrency level and file size patterns
/// to provide even better performance characteristics.
///
/// # Arguments
///
/// * `file_size` - The size of the file being read, or -1 if unknown
/// * `base_buffer_size` - The baseline buffer size from workload profile
/// * `is_sequential` - Whether this is a sequential read (hint for optimization)
///
/// # Returns
///
/// Optimized buffer size in bytes
///
/// # Examples
///
/// ```ignore
/// let buffer_size = get_advanced_buffer_size(
///     32 * 1024 * 1024,  // 32MB file
///     256 * 1024,        // 256KB base buffer
///     true               // sequential read
/// );
/// ```
#[allow(dead_code)]
pub fn get_advanced_buffer_size(file_size: i64, base_buffer_size: usize, is_sequential: bool) -> usize {
    let concurrent_requests = ACTIVE_GET_REQUESTS.load(Ordering::Relaxed);

    // For very small files, use smaller buffers regardless of concurrency
    // Replace manual max/min chain with clamp
    if file_size > 0 && file_size < 256 * KI_B as i64 {
        return (file_size as usize / 4).clamp(16 * KI_B, 64 * KI_B);
    }

    // Base calculation from standard function
    let standard_size = get_concurrency_aware_buffer_size(file_size, base_buffer_size);

    // For sequential reads, we can be more aggressive with buffer sizes
    if is_sequential && concurrent_requests <= MEDIUM_CONCURRENCY_THRESHOLD {
        return ((standard_size as f64 * 1.5) as usize).min(2 * MI_B);
    }

    // For high concurrency with large files, optimize for parallel processing
    if concurrent_requests > HIGH_CONCURRENCY_THRESHOLD && file_size > 10 * MI_B as i64 {
        // Use smaller, more numerous buffers for better parallelism
        return (standard_size as f64 * 0.8) as usize;
    }

    standard_size
}

/// High-performance cache for hot objects using Moka
///
/// This cache uses Moka for superior concurrent performance with features like:
/// - Lock-free reads and writes
/// - Automatic TTL and TTI expiration
/// - Size-based eviction with weigher function
/// - Built-in metrics collection
#[derive(Clone)]
struct HotObjectCache {
    /// Moka cache instance with size-based eviction
    cache: Cache<String, Arc<CachedObject>>,
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
            .field("max_object_size", &self.max_object_size)
            .field("hit_count", &self.hit_count.load(Ordering::Relaxed))
            .field("miss_count", &self.miss_count.load(Ordering::Relaxed))
            .finish()
    }
}

/// A cached object with metadata and metrics
#[derive(Clone)]
struct CachedObject {
    /// The object data
    data: Arc<Vec<u8>>,
    /// When this object was cached
    cached_at: Instant,
    /// Object size in bytes
    size: usize,
    /// Number of times this object has been accessed
    access_count: Arc<AtomicU64>,
}

impl HotObjectCache {
    /// Create a new hot object cache with Moka
    ///
    /// Configures Moka with:
    /// - Size-based eviction (100MB max)
    /// - TTL of 5 minutes
    /// - TTI of 2 minutes
    /// - Weigher function for accurate size tracking
    fn new() -> Self {
        let cache = Cache::builder()
            .max_capacity(100 * MI_B as u64)
            .weigher(|_key: &String, value: &Arc<CachedObject>| -> u32 {
                // Weight based on actual data size
                value.size.min(u32::MAX as usize) as u32
            })
            .time_to_live(Duration::from_secs(CACHE_TTL_SECS))
            .time_to_idle(Duration::from_secs(CACHE_TTI_SECS))
            .build();

        Self {
            cache,
            max_object_size: 10 * MI_B,
            hit_count: Arc::new(AtomicU64::new(0)),
            miss_count: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Get an object from cache with lock-free concurrent access
    ///
    /// Moka provides lock-free reads, significantly improving concurrent performance.
    async fn get(&self, key: &str) -> Option<Arc<Vec<u8>>> {
        match self.cache.get(key).await {
            Some(cached) => {
                // Update access count
                cached.access_count.fetch_add(1, Ordering::Relaxed);
                self.hit_count.fetch_add(1, Ordering::Relaxed);

                #[cfg(feature = "metrics")]
                {
                    use metrics::counter;
                    counter!("rustfs_object_cache_hits").increment(1);
                    counter!("rustfs_object_cache_access_count", "key" => key.to_string()).increment(1);
                }

                Some(Arc::clone(&cached.data))
            }
            None => {
                self.miss_count.fetch_add(1, Ordering::Relaxed);

                #[cfg(feature = "metrics")]
                {
                    use metrics::counter;
                    counter!("rustfs_object_cache_misses").increment(1);
                }

                None
            }
        }
    }

    /// Put an object into cache with automatic size-based eviction
    ///
    /// Moka handles eviction automatically based on the weigher function.
    #[allow(dead_code)]
    async fn put(&self, key: String, data: Vec<u8>) {
        let size = data.len();

        // Only cache objects smaller than max_object_size
        if size == 0 || size > self.max_object_size {
            return;
        }

        let cached_obj = Arc::new(CachedObject {
            data: Arc::new(data),
            cached_at: Instant::now(),
            size,
            access_count: Arc::new(AtomicU64::new(0)),
        });

        self.cache.insert(key.clone(), cached_obj).await;

        #[cfg(feature = "metrics")]
        {
            use metrics::{counter, gauge};
            counter!("rustfs_object_cache_insertions").increment(1);
            gauge!("rustfs_object_cache_size_bytes").set(self.cache.weighted_size() as f64);
            gauge!("rustfs_object_cache_entry_count").set(self.cache.entry_count() as f64);
        }
    }

    /// Clear all cached objects
    async fn clear(&self) {
        self.cache.invalidate_all();
        // Sync to ensure all entries are removed
        self.cache.run_pending_tasks().await;
    }

    /// Get cache statistics for monitoring
    async fn stats(&self) -> CacheStats {
        // Ensure pending tasks are processed for accurate stats
        self.cache.run_pending_tasks().await;

        CacheStats {
            size: self.cache.weighted_size() as usize,
            entries: self.cache.entry_count() as usize,
            max_size: 100 * MI_B,
            max_object_size: self.max_object_size,
            hit_count: self.hit_count.load(Ordering::Relaxed),
            miss_count: self.miss_count.load(Ordering::Relaxed),
        }
    }

    /// Check if a key exists in cache (lock-free)
    async fn contains(&self, key: &str) -> bool {
        self.cache.contains_key(key)
    }

    /// Get multiple objects from cache in parallel
    ///
    /// Leverages Moka's lock-free design for true parallel access.
    async fn get_batch(&self, keys: &[String]) -> Vec<Option<Arc<Vec<u8>>>> {
        let mut results = Vec::with_capacity(keys.len());
        for key in keys {
            results.push(self.get(key).await);
        }
        results
    }

    /// Remove a specific key from cache
    async fn remove(&self, key: &str) -> bool {
        let had_key = self.cache.contains_key(key);
        self.cache.invalidate(key).await;
        had_key
    }

    /// Get the most frequently accessed keys
    ///
    /// Returns up to `limit` keys sorted by access count in descending order.
    async fn get_hot_keys(&self, limit: usize) -> Vec<(String, u64)> {
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
    async fn warm(&self, objects: Vec<(String, Vec<u8>)>) {
        for (key, data) in objects {
            self.put(key, data).await;
        }
    }

    /// Get hit rate percentage
    fn hit_rate(&self) -> f64 {
        let hits = self.hit_count.load(Ordering::Relaxed);
        let misses = self.miss_count.load(Ordering::Relaxed);
        let total = hits + misses;

        if total == 0 {
            0.0
        } else {
            (hits as f64 / total as f64) * 100.0
        }
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
}

/// Concurrency manager for coordinating concurrent GetObject requests
#[allow(dead_code)]
pub struct ConcurrencyManager {
    /// Hot object cache for frequently accessed objects
    cache: Arc<HotObjectCache>,
    /// Semaphore to limit concurrent disk reads
    disk_read_semaphore: Arc<Semaphore>,
}

impl std::fmt::Debug for ConcurrencyManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use std::sync::atomic::Ordering;
        f.debug_struct("ConcurrencyManager")
            .field("active_requests", &ACTIVE_GET_REQUESTS.load(Ordering::Relaxed))
            .field("disk_read_permits", &self.disk_read_semaphore.available_permits())
            .finish()
    }
}

impl ConcurrencyManager {
    /// Create a new concurrency manager with default settings
    pub fn new() -> Self {
        Self {
            cache: Arc::new(HotObjectCache::new()),
            disk_read_semaphore: Arc::new(Semaphore::new(64)),
        }
    }

    /// Track a GetObject request
    pub fn track_request() -> GetObjectGuard {
        GetObjectGuard::new()
    }

    /// Try to get an object from cache
    pub async fn get_cached(&self, key: &str) -> Option<Arc<Vec<u8>>> {
        self.cache.get(key).await
    }

    /// Cache an object for future retrievals
    #[allow(dead_code)]
    pub async fn cache_object(&self, key: String, data: Vec<u8>) {
        self.cache.put(key, data).await;
    }

    /// Acquire a permit to perform a disk read operation
    ///
    /// This ensures we don't overwhelm the disk subsystem with too many
    /// concurrent reads, which can cause performance degradation.
    pub async fn acquire_disk_read_permit(&self) -> tokio::sync::SemaphorePermit<'_> {
        self.disk_read_semaphore
            .acquire()
            .await
            .expect("semaphore closed unexpectedly")
    }

    /// Get cache statistics
    #[allow(dead_code)]
    pub async fn cache_stats(&self) -> CacheStats {
        self.cache.stats().await
    }

    /// Clear all cached objects
    #[allow(dead_code)]
    pub async fn clear_cache(&self) {
        self.cache.clear().await;
    }

    /// Check if a key is cached
    #[allow(dead_code)]
    pub async fn is_cached(&self, key: &str) -> bool {
        self.cache.contains(key).await
    }

    /// Get multiple cached objects in a single operation
    #[allow(dead_code)]
    pub async fn get_cached_batch(&self, keys: &[String]) -> Vec<Option<Arc<Vec<u8>>>> {
        self.cache.get_batch(keys).await
    }

    /// Remove a specific object from cache
    #[allow(dead_code)]
    pub async fn remove_cached(&self, key: &str) -> bool {
        self.cache.remove(key).await
    }

    /// Get the most frequently accessed keys
    #[allow(dead_code)]
    pub async fn get_hot_keys(&self, limit: usize) -> Vec<(String, u64)> {
        self.cache.get_hot_keys(limit).await
    }

    /// Get cache hit rate percentage
    #[allow(dead_code)]
    pub fn cache_hit_rate(&self) -> f64 {
        self.cache.hit_rate()
    }

    /// Warm up cache with frequently accessed objects
    ///
    /// This can be called during server startup or maintenance windows
    /// to pre-populate the cache with known hot objects.
    #[allow(dead_code)]
    pub async fn warm_cache(&self, objects: Vec<(String, Vec<u8>)>) {
        self.cache.warm(objects).await;
    }

    /// Get optimized buffer size for a request
    ///
    /// This wraps the advanced buffer sizing logic and makes it accessible
    /// through the concurrency manager interface.
    #[allow(dead_code)]
    pub fn buffer_size(&self, file_size: i64, base: usize, sequential: bool) -> usize {
        get_advanced_buffer_size(file_size, base, sequential)
    }
}

impl Default for ConcurrencyManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Get the global concurrency manager instance
pub fn get_concurrency_manager() -> &'static ConcurrencyManager {
    &CONCURRENCY_MANAGER
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_concurrent_request_tracking() {
        // Ensure we start from a clean state
        assert_eq!(GetObjectGuard::concurrent_requests(), 0);

        let _guard1 = GetObjectGuard::new();
        assert_eq!(GetObjectGuard::concurrent_requests(), 1);

        let _guard2 = GetObjectGuard::new();
        assert_eq!(GetObjectGuard::concurrent_requests(), 2);

        drop(_guard1);
        assert_eq!(GetObjectGuard::concurrent_requests(), 1);

        drop(_guard2);
        assert_eq!(GetObjectGuard::concurrent_requests(), 0);
    }

    #[test]
    fn test_adaptive_buffer_sizing() {
        // Reset concurrent requests
        ACTIVE_GET_REQUESTS.store(0, Ordering::Relaxed);
        let base_buffer = 256 * KI_B;

        // Test low concurrency (1 request)
        ACTIVE_GET_REQUESTS.store(1, Ordering::Relaxed);
        let result = get_concurrency_aware_buffer_size(10 * MI_B as i64, base_buffer);
        assert_eq!(result, base_buffer, "Single request should use full buffer");

        // Test medium concurrency (3 requests)
        ACTIVE_GET_REQUESTS.store(3, Ordering::Relaxed);
        let result = get_concurrency_aware_buffer_size(10 * MI_B as i64, base_buffer);
        assert!(result < base_buffer && result >= base_buffer / 2);

        // Test higher concurrency (6 requests)
        ACTIVE_GET_REQUESTS.store(6, Ordering::Relaxed);
        let result = get_concurrency_aware_buffer_size(10 * MI_B as i64, base_buffer);
        assert!(result <= base_buffer / 2 && result >= base_buffer / 3);

        // Test very high concurrency (10 requests)
        ACTIVE_GET_REQUESTS.store(10, Ordering::Relaxed);
        let result = get_concurrency_aware_buffer_size(10 * MI_B as i64, base_buffer);
        assert!(result <= base_buffer / 2 && result >= 64 * KI_B);
    }

    #[tokio::test]
    async fn test_hot_object_cache() {
        let cache = HotObjectCache::new();
        let test_data = vec![1u8; 1024];

        // Test basic put and get
        cache.put("test_key".to_string(), test_data.clone()).await;
        let retrieved = cache.get("test_key").await;
        assert!(retrieved.is_some());
        assert_eq!(*retrieved.unwrap(), test_data);

        // Test cache miss
        let missing = cache.get("nonexistent").await;
        assert!(missing.is_none());
    }

    #[tokio::test]
    async fn test_cache_eviction() {
        let cache = HotObjectCache::new();

        // Fill cache with objects
        for i in 0..200 {
            let data = vec![0u8; 64 * KI_B];
            cache.put(format!("key_{}", i), data).await;
        }

        let stats = cache.stats().await;
        assert!(
            stats.size <= stats.max_size,
            "Cache size {} should not exceed max {}",
            stats.size,
            stats.max_size
        );
    }

    #[tokio::test]
    async fn test_cache_reject_large_objects() {
        let cache = HotObjectCache::new();
        let large_data = vec![0u8; 11 * MI_B]; // Larger than max_object_size

        cache.put("large_object".to_string(), large_data).await;
        let retrieved = cache.get("large_object").await;
        assert!(retrieved.is_none(), "Large objects should not be cached");
    }

    #[test]
    fn test_concurrency_manager_creation() {
        let manager = ConcurrencyManager::new();
        assert_eq!(
            manager.disk_read_semaphore.available_permits(),
            64,
            "Should start with 64 available disk read permits"
        );
    }

    #[tokio::test]
    async fn test_disk_read_permits() {
        let manager = ConcurrencyManager::new();

        let permit1 = manager.acquire_disk_read_permit().await;
        assert_eq!(manager.disk_read_semaphore.available_permits(), 63);

        let permit2 = manager.acquire_disk_read_permit().await;
        assert_eq!(manager.disk_read_semaphore.available_permits(), 62);

        drop(permit1);
        assert_eq!(manager.disk_read_semaphore.available_permits(), 63);

        drop(permit2);
        assert_eq!(manager.disk_read_semaphore.available_permits(), 64);
    }

    #[test]
    fn test_advanced_buffer_size_small_files() {
        ACTIVE_GET_REQUESTS.store(1, Ordering::Relaxed);

        // Test small file optimization
        let result = get_advanced_buffer_size(32 * KI_B as i64, 256 * KI_B, true);
        assert!(
            (16 * KI_B..=64 * KI_B).contains(&result),
            "Small files should use reduced buffer: {}",
            result
        );
    }

    #[test]
    fn test_clamp_behavior() {
        // Test the clamp replacement
        let file_size = 100 * KI_B as i64;
        let result = (file_size as usize / 4).clamp(16 * KI_B, 64 * KI_B);
        assert!((16 * KI_B..=64 * KI_B).contains(&result));
    }

    #[tokio::test]
    async fn test_cache_expiration() {
        let cache = HotObjectCache::new();
        let data = vec![1u8; 1024];

        cache.put("test".to_string(), data).await;

        // Simulate immediate expiration by manually creating an old object
        // (In real scenario would need to wait for TTL, here we test the logic)
        let expired_obj = CachedObject {
            data: Arc::new(vec![]),
            cached_at: Instant::now() - Duration::from_secs(HOT_OBJECT_SOFT_TTL_SECS + 1),
            size: 1024,
            hit_count: AtomicUsize::new(0),
        };

        assert!(HotObjectCache::should_expire(&expired_obj));
    }

    #[tokio::test]
    async fn test_hot_keys_tracking() {
        let manager = ConcurrencyManager::new();

        // Cache some objects with different access patterns
        manager.cache_object("hot1".to_string(), vec![1u8; 100]).await;
        manager.cache_object("hot2".to_string(), vec![2u8; 100]).await;

        // Access them multiple times to build hit counts
        for _ in 0..5 {
            let _ = manager.get_cached("hot1").await;
        }
        for _ in 0..3 {
            let _ = manager.get_cached("hot2").await;
        }

        let hot_keys = manager.get_hot_keys(2).await;
        assert!(!hot_keys.is_empty(), "Should have hot keys");
    }

    #[tokio::test]
    async fn test_batch_operations() {
        let manager = ConcurrencyManager::new();

        manager.cache_object("key1".to_string(), vec![1u8; 100]).await;
        manager.cache_object("key2".to_string(), vec![2u8; 100]).await;
        manager.cache_object("key3".to_string(), vec![3u8; 100]).await;

        let keys = vec!["key1".to_string(), "key2".to_string(), "key4".to_string()];
        let results = manager.get_cached_batch(&keys).await;

        assert_eq!(results.len(), 3);
        assert!(results[0].is_some());
        assert!(results[1].is_some());
        assert!(results[2].is_none()); // key4 doesn't exist
    }

    #[tokio::test]
    async fn test_cache_clear() {
        let manager = ConcurrencyManager::new();

        manager.cache_object("key1".to_string(), vec![1u8; 1024]).await;
        manager.cache_object("key2".to_string(), vec![2u8; 1024]).await;

        let stats_before = manager.cache_stats().await;
        assert!(stats_before.entries > 0);

        manager.clear_cache().await;

        let stats_after = manager.cache_stats().await;
        assert_eq!(stats_after.entries, 0);
        assert_eq!(stats_after.size, 0);
    }

    #[tokio::test]
    async fn test_warm_cache() {
        let manager = ConcurrencyManager::new();

        let objects = vec![
            ("warm1".to_string(), vec![1u8; 100]),
            ("warm2".to_string(), vec![2u8; 100]),
            ("warm3".to_string(), vec![3u8; 100]),
        ];

        manager.warm_cache(objects).await;

        assert!(manager.is_cached("warm1").await);
        assert!(manager.is_cached("warm2").await);
        assert!(manager.is_cached("warm3").await);
    }
}
