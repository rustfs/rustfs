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

use moka::future::Cache;
use rustfs_config::MI_B;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Hot object cache for frequently accessed objects.
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
pub struct CachedGetObject {
    /// The object body data
    pub body: bytes::Bytes,
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
    cached_at: Option<Instant>,
    /// Access count for hot key tracking (automatically managed)
    access_count: Arc<AtomicU64>,
}

impl Default for CachedGetObject {
    fn default() -> Self {
        Self {
            body: bytes::Bytes::new(),
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

impl CachedGetObject {
    /// Create a new CachedGetObject with the given body and content length
    pub fn new(body: bytes::Bytes, content_length: i64) -> Self {
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

    /// Get the size in bytes for cache eviction calculations
    pub fn size(&self) -> usize {
        self.body.len()
    }

    /// Increment access count and return the new value
    pub fn increment_access(&self) -> u64 {
        self.access_count.fetch_add(1, Ordering::Relaxed) + 1
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
                #[cfg(all(feature = "metrics", not(test)))]
                {
                    use metrics::counter;
                    counter!("rustfs.object.cache.hits").increment(1);
                }

                Some(Arc::clone(&cached.data))
            }
            None => {
                self.miss_count.fetch_add(1, Ordering::Relaxed);

                #[cfg(all(feature = "metrics", not(test)))]
                {
                    use metrics::counter;
                    counter!("rustfs.object.cache.misses").increment(1);
                }

                None
            }
        }
    }

    /// Put an object into cache with automatic size-based eviction
    ///
    /// Moka handles eviction automatically based on the weigher function.
    pub(crate) async fn put(&self, key: String, data: Vec<u8>) {
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

        #[cfg(all(feature = "metrics", not(test)))]
        {
            use metrics::{counter, gauge};
            counter!("rustfs.object.cache.insertions").increment(1);
            gauge!("rustfs_object_cache_size_bytes").set(self.cache.weighted_size() as f64);
            gauge!("rustfs_object_cache_entry_count").set(self.cache.entry_count() as f64);
        }
    }

    /// Clear all cached objects
    pub(crate) async fn clear(&self) {
        self.cache.invalidate_all();
        // Sync to ensure all entries are removed
        self.cache.run_pending_tasks().await;
    }

    /// Get cache statistics for monitoring
    pub(crate) async fn stats(&self) -> CacheStats {
        // Ensure pending tasks are processed for accurate stats
        self.cache.run_pending_tasks().await;
        let mut total_ms: u128 = 0;
        let mut cnt: u64 = 0;
        self.cache.iter().for_each(|(_, v)| {
            total_ms += v.cached_at.elapsed().as_millis();
            cnt += 1;
        });
        let avg_age_secs = if cnt == 0 {
            0.0
        } else {
            (total_ms as f64 / cnt as f64) / 1000.0
        };
        CacheStats {
            size: self.cache.weighted_size() as usize,
            entries: self.cache.entry_count() as usize,
            max_size: self.max_capacity,
            max_object_size: self.max_object_size,
            hit_count: self.hit_count.load(Ordering::Relaxed),
            miss_count: self.miss_count.load(Ordering::Relaxed),
            avg_age_secs,
        }
    }

    /// Check if a key exists in cache (lock-free)
    pub(crate) async fn contains(&self, key: &str) -> bool {
        self.cache.contains_key(key)
    }

    /// Get multiple objects from cache in parallel
    ///
    /// Leverages Moka's lock-free design for true parallel access.
    pub(crate) async fn get_batch(&self, keys: &[String]) -> Vec<Option<Arc<Vec<u8>>>> {
        let mut results = Vec::with_capacity(keys.len());
        for key in keys {
            results.push(self.get(key).await);
        }
        results
    }

    /// Remove a specific key from cache
    pub(crate) async fn remove(&self, key: &str) -> bool {
        let had_key = self.cache.contains_key(key);
        self.cache.invalidate(key).await;
        had_key
    }

    /// Get the most frequently accessed keys
    ///
    /// Returns up to `limit` keys sorted by access count in descending order.
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
    pub(crate) async fn warm(&self, objects: Vec<(String, Vec<u8>)>) {
        for (key, data) in objects {
            self.put(key, data).await;
        }
    }

    /// Get hit rate percentage
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
                #[cfg(all(feature = "metrics", not(test)))]
                {
                    use metrics::counter;
                    counter!("rustfs_object_response_cache_hits").increment(1);
                }

                Some(Arc::clone(&cached.data))
            }
            None => {
                self.miss_count.fetch_add(1, Ordering::Relaxed);

                #[cfg(all(feature = "metrics", not(test)))]
                {
                    use metrics::counter;
                    counter!("rustfs_object_response_cache_misses").increment(1);
                }

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

        #[cfg(all(feature = "metrics", not(test)))]
        {
            use metrics::{counter, gauge};
            counter!("rustfs_object_response_cache_insertions").increment(1);
            gauge!("rustfs_object_response_cache_size_bytes").set(self.response_cache.weighted_size() as f64);
            gauge!("rustfs_object_response_cache_entry_count").set(self.response_cache.entry_count() as f64);
        }
    }

    /// Invalidate a cache entry for a specific object
    ///
    /// This method removes both the simple byte cache entry and the response cache entry
    /// for the given key. Used when objects are modified or deleted.
    ///
    /// # Arguments
    ///
    /// * `key` - Cache key to invalidate (e.g., "{bucket}/{key}")
    pub(crate) async fn invalidate(&self, key: &str) {
        // Invalidate both caches
        self.cache.invalidate(key).await;
        self.response_cache.invalidate(key).await;

        #[cfg(all(feature = "metrics", not(test)))]
        {
            use metrics::counter;
            counter!("rustfs_object_cache_invalidations_total").increment(1);
        }
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
    pub(crate) async fn clear_all(&self) {
        self.cache.invalidate_all();
        self.response_cache.invalidate_all();
        // Sync to ensure all entries are removed
        self.cache.run_pending_tasks().await;
        self.response_cache.run_pending_tasks().await;
    }

    /// Get the maximum object size for caching
    pub(crate) fn max_object_size(&self) -> usize {
        self.max_object_size
    }
}

/// Cache statistics for monitoring and debugging
#[derive(Debug, Clone)]
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
}
