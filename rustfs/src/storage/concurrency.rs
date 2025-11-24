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

use rustfs_config::{KI_B, MI_B};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant};
use tokio::sync::{RwLock, Semaphore};

/// Global concurrent request counter for adaptive buffer sizing
static ACTIVE_GET_REQUESTS: AtomicUsize = AtomicUsize::new(0);

/// Maximum concurrent requests before applying aggressive optimization
const HIGH_CONCURRENCY_THRESHOLD: usize = 8;

/// Medium concurrency threshold
const MEDIUM_CONCURRENCY_THRESHOLD: usize = 4;

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
pub fn get_advanced_buffer_size(file_size: i64, base_buffer_size: usize, is_sequential: bool) -> usize {
    let concurrent_requests = ACTIVE_GET_REQUESTS.load(Ordering::Relaxed);

    // For very small files, use smaller buffers regardless of concurrency
    if file_size > 0 && file_size < 256 * KI_B as i64 {
        return (file_size as usize / 4).max(16 * KI_B).min(64 * KI_B);
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

/// Simple LRU cache for hot objects
///
/// This cache stores frequently accessed small objects (<= 10MB) to reduce
/// disk I/O and improve response times under high concurrency.
#[derive(Debug)]
struct HotObjectCache {
    /// Maximum size of objects to cache (10MB by default)
    max_object_size: usize,
    /// Maximum total cache size (100MB by default)
    max_cache_size: usize,
    /// Current cache size in bytes
    current_size: AtomicUsize,
    /// Cached objects with their data and metadata
    cache: RwLock<lru::LruCache<String, Arc<CachedObject>>>,
}

/// A cached object with metadata
#[derive(Debug)]
struct CachedObject {
    /// The object data
    data: Arc<Vec<u8>>,
    /// When this object was cached
    cached_at: Instant,
    /// Object size in bytes
    size: usize,
    /// Number of times this object has been served from cache
    hit_count: AtomicUsize,
}

impl HotObjectCache {
    /// Create a new hot object cache
    fn new() -> Self {
        Self {
            max_object_size: 10 * MI_B,
            max_cache_size: 100 * MI_B,
            current_size: AtomicUsize::new(0),
            cache: RwLock::new(lru::LruCache::new(std::num::NonZeroUsize::new(1000).unwrap())),
        }
    }

    /// Try to get an object from cache with optimized read-first pattern
    ///
    /// Uses `peek()` for initial lookup to avoid write lock contention when possible.
    /// This significantly improves concurrent read performance.
    async fn get(&self, key: &str) -> Option<Arc<Vec<u8>>> {
        // First, try with read lock using peek (doesn't promote in LRU)
        {
            let cache = self.cache.read().await;
            if let Some(cached) = cache.peek(key) {
                // Clone the data reference while holding read lock
                let data = Arc::clone(&cached.data);
                drop(cache);

                // Now acquire write lock to promote in LRU and update stats
                let mut cache_write = self.cache.write().await;
                if let Some(cached) = cache_write.get(key) {
                    cached.hit_count.fetch_add(1, Ordering::Relaxed);

                    #[cfg(feature = "metrics")]
                    {
                        use metrics::counter;
                        counter!("rustfs_object_cache_hits").increment(1);
                    }

                    return Some(data);
                }
            }
        }

        #[cfg(feature = "metrics")]
        {
            use metrics::counter;
            counter!("rustfs_object_cache_misses").increment(1);
        }

        None
    }

    /// Put an object into cache if it's small enough
    async fn put(&self, key: String, data: Vec<u8>) {
        let size = data.len();

        // Only cache objects smaller than max_object_size
        if size > self.max_object_size {
            return;
        }

        let cached_obj = Arc::new(CachedObject {
            data: Arc::new(data),
            cached_at: Instant::now(),
            size,
            hit_count: AtomicUsize::new(0),
        });

        let mut cache = self.cache.write().await;

        // Evict items if cache is too large
        // Note: We load the current_size inside the write lock to avoid race conditions
        let mut current = self.current_size.load(Ordering::Relaxed);
        while current + size > self.max_cache_size {
            if let Some((_, evicted)) = cache.pop_lru() {
                current -= evicted.size;
                self.current_size.store(current, Ordering::Relaxed);
            } else {
                break;
            }
        }

        cache.put(key, cached_obj);
        current += size;
        self.current_size.store(current, Ordering::Relaxed);

        #[cfg(feature = "metrics")]
        {
            use metrics::{counter, gauge};
            counter!("rustfs_object_cache_insertions").increment(1);
            gauge!("rustfs_object_cache_size_bytes").set(self.current_size.load(Ordering::Relaxed) as f64);
        }
    }

    /// Clear the cache
    async fn clear(&self) {
        let mut cache = self.cache.write().await;
        cache.clear();
        self.current_size.store(0, Ordering::Relaxed);
    }

    /// Get cache statistics
    async fn stats(&self) -> CacheStats {
        let cache = self.cache.read().await;
        CacheStats {
            size: self.current_size.load(Ordering::Relaxed),
            entries: cache.len(),
            max_size: self.max_cache_size,
            max_object_size: self.max_object_size,
        }
    }

    /// Check if a key exists in cache without promoting it in LRU
    async fn contains(&self, key: &str) -> bool {
        let cache = self.cache.read().await;
        cache.peek(key).is_some()
    }

    /// Get multiple objects from cache in a single operation
    ///
    /// This is more efficient than calling get() multiple times as it acquires
    /// the lock only once for all operations.
    async fn get_batch(&self, keys: &[String]) -> Vec<Option<Arc<Vec<u8>>>> {
        let mut cache = self.cache.write().await;
        let mut results = Vec::with_capacity(keys.len());

        for key in keys {
            if let Some(cached) = cache.get(key) {
                cached.hit_count.fetch_add(1, Ordering::Relaxed);
                results.push(Some(Arc::clone(&cached.data)));

                #[cfg(feature = "metrics")]
                {
                    use metrics::counter;
                    counter!("rustfs_object_cache_hits").increment(1);
                }
            } else {
                results.push(None);

                #[cfg(feature = "metrics")]
                {
                    use metrics::counter;
                    counter!("rustfs_object_cache_misses").increment(1);
                }
            }
        }

        results
    }

    /// Remove a specific key from cache
    ///
    /// Returns true if the key was found and removed, false otherwise.
    async fn remove(&self, key: &str) -> bool {
        let mut cache = self.cache.write().await;
        if let Some(cached) = cache.pop(key) {
            let current = self.current_size.load(Ordering::Relaxed);
            self.current_size
                .store(current.saturating_sub(cached.size), Ordering::Relaxed);
            true
        } else {
            false
        }
    }

    /// Get the most frequently accessed keys
    ///
    /// Returns up to `limit` keys sorted by hit count in descending order.
    async fn get_hot_keys(&self, limit: usize) -> Vec<(String, usize)> {
        let cache = self.cache.read().await;
        let mut keys_with_hits: Vec<(String, usize)> = cache
            .iter()
            .map(|(k, v)| (k.clone(), v.hit_count.load(Ordering::Relaxed)))
            .collect();

        keys_with_hits.sort_by(|a, b| b.1.cmp(&a.1));
        keys_with_hits.truncate(limit);
        keys_with_hits
    }
}

/// Cache statistics
#[derive(Debug, Clone)]
pub struct CacheStats {
    /// Current cache size in bytes
    pub size: usize,
    /// Number of cached entries
    pub entries: usize,
    /// Maximum cache size
    pub max_size: usize,
    /// Maximum object size that can be cached
    pub max_object_size: usize,
}

/// Concurrency manager for coordinating concurrent GetObject requests
#[derive(Debug)]
pub struct ConcurrencyManager {
    /// Hot object cache for frequently accessed small files
    cache: Arc<HotObjectCache>,
    /// Semaphore to limit maximum concurrent disk reads
    /// This prevents disk I/O saturation under extreme load
    disk_read_semaphore: Arc<Semaphore>,
}

impl ConcurrencyManager {
    /// Create a new concurrency manager
    pub fn new() -> Self {
        Self {
            cache: Arc::new(HotObjectCache::new()),
            // Allow up to 64 concurrent disk reads by default
            // This can be tuned based on disk performance characteristics
            disk_read_semaphore: Arc::new(Semaphore::new(64)),
        }
    }

    /// Start tracking a new GetObject request
    ///
    /// Returns a guard that automatically decrements the counter when dropped
    pub fn track_request() -> GetObjectGuard {
        GetObjectGuard::new()
    }

    /// Try to get an object from cache
    pub async fn get_cached(&self, key: &str) -> Option<Arc<Vec<u8>>> {
        self.cache.get(key).await
    }

    /// Cache an object if it's eligible
    pub async fn cache_object(&self, key: String, data: Vec<u8>) {
        self.cache.put(key, data).await
    }

    /// Acquire a disk read permit
    ///
    /// This ensures we don't overwhelm the disk with too many concurrent reads
    ///
    /// # Panics
    ///
    /// This function will panic if the semaphore has been closed, which should never
    /// happen in normal operation since the semaphore is owned by the ConcurrencyManager
    /// which lives for the entire application lifetime.
    pub async fn acquire_disk_read_permit(&self) -> tokio::sync::SemaphorePermit<'_> {
        self.disk_read_semaphore
            .acquire()
            .await
            .expect("Failed to acquire disk read permit: semaphore is closed. This indicates a serious internal error.")
    }

    /// Get cache statistics
    pub async fn cache_stats(&self) -> CacheStats {
        self.cache.stats().await
    }

    /// Clear the cache
    pub async fn clear_cache(&self) {
        self.cache.clear().await
    }

    /// Check if a key exists in cache
    ///
    /// This is a lightweight check that doesn't promote the key in LRU order.
    pub async fn is_cached(&self, key: &str) -> bool {
        self.cache.contains(key).await
    }

    /// Get multiple objects from cache in batch
    ///
    /// More efficient than individual get_cached() calls when fetching multiple objects.
    pub async fn get_cached_batch(&self, keys: &[String]) -> Vec<Option<Arc<Vec<u8>>>> {
        self.cache.get_batch(keys).await
    }

    /// Remove a specific object from cache
    ///
    /// Returns true if the object was cached and removed.
    pub async fn remove_cached(&self, key: &str) -> bool {
        self.cache.remove(key).await
    }

    /// Get the most frequently accessed keys
    ///
    /// Useful for identifying hot objects and optimizing cache strategies.
    pub async fn get_hot_keys(&self, limit: usize) -> Vec<(String, usize)> {
        self.cache.get_hot_keys(limit).await
    }

    /// Warm up cache with frequently accessed objects
    ///
    /// This can be called during server startup or maintenance windows
    /// to pre-populate the cache with known hot objects.
    pub async fn warm_cache(&self, objects: Vec<(String, Vec<u8>)>) {
        for (key, data) in objects {
            self.cache_object(key, data).await;
        }
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
        // Reset counter
        ACTIVE_GET_REQUESTS.store(0, Ordering::Relaxed);

        let base_size = 256 * KI_B;

        // Low concurrency: use base size
        ACTIVE_GET_REQUESTS.store(1, Ordering::Relaxed);
        assert_eq!(get_concurrency_aware_buffer_size(10 * MI_B as i64, base_size), base_size);

        // Medium concurrency: reduce to 75%
        ACTIVE_GET_REQUESTS.store(3, Ordering::Relaxed);
        let result = get_concurrency_aware_buffer_size(10 * MI_B as i64, base_size);
        assert!(result < base_size);
        assert!(result >= base_size / 2);

        // High concurrency: reduce to 50%
        ACTIVE_GET_REQUESTS.store(6, Ordering::Relaxed);
        let result = get_concurrency_aware_buffer_size(10 * MI_B as i64, base_size);
        assert!(result <= base_size / 2);
        assert!(result >= base_size / 3);

        // Very high concurrency: reduce to 40%
        ACTIVE_GET_REQUESTS.store(10, Ordering::Relaxed);
        let result = get_concurrency_aware_buffer_size(10 * MI_B as i64, base_size);
        assert!(result <= base_size / 2);
        assert!(result >= 64 * KI_B); // Should stay above minimum

        // Reset for other tests
        ACTIVE_GET_REQUESTS.store(0, Ordering::Relaxed);
    }

    #[tokio::test]
    async fn test_hot_object_cache() {
        let cache = HotObjectCache::new();

        // Cache a small object
        let data = vec![1u8; 1024];
        cache.put("test-key".to_string(), data.clone()).await;

        // Retrieve it
        let cached = cache.get("test-key").await;
        assert!(cached.is_some());
        assert_eq!(*cached.unwrap(), data);

        // Try to get non-existent key
        let missing = cache.get("missing-key").await;
        assert!(missing.is_none());

        // Cache too large object (> 10MB)
        let large_data = vec![1u8; 11 * MI_B];
        cache.put("large-key".to_string(), large_data).await;

        // Should not be cached
        let not_cached = cache.get("large-key").await;
        assert!(not_cached.is_none());
    }

    #[tokio::test]
    async fn test_cache_eviction() {
        let cache = HotObjectCache::new();

        // Fill cache with small objects
        for i in 0..20 {
            let data = vec![1u8; 6 * MI_B]; // 6MB each
            cache.put(format!("key-{}", i), data).await;
        }

        // Check that old items were evicted
        let stats = cache.stats().await;
        assert!(stats.size <= cache.max_cache_size);

        // First keys should be evicted
        let first = cache.get("key-0").await;
        assert!(first.is_none());

        // Recent keys should still be there
        let recent = cache.get("key-19").await;
        assert!(recent.is_some());
    }

    #[test]
    fn test_concurrency_manager_creation() {
        let manager = ConcurrencyManager::new();
        assert_eq!(manager.disk_read_semaphore.available_permits(), 64);
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
}
