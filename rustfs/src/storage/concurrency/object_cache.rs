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
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

// ============================================
// Hot Object Cache Types
// ============================================

/// Hot object cache for frequently accessed objects.
///
/// This cache uses Moka, a high-performance, lock-free cache library,
/// to provide sub-5ms response times for frequently accessed objects.
/// The cache automatically evicts entries based on size (100MB max),
/// TTL (5 minutes), and TTI (2 minutes).
pub(crate) struct HotObjectCache {
    /// Moka cache instance for simple byte data (legacy)
    cache: Cache<String, Arc<CachedObject>>,
    /// Moka cache instance for full GetObject responses with metadata
    response_cache: Cache<String, Arc<CachedGetObjectInternal>>,
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

/// Cached object with access tracking.
///
/// This structure wraps the actual data with metadata for cache management.
#[derive(Clone)]
struct CachedObject {
    /// The cached data
    data: Arc<Vec<u8>>,
    /// When this object was cached
    cached_at: Instant,
    /// Access count for hot object detection
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
    pub(crate) fn new() -> Self {
        let max_capacity = rustfs_utils::get_env_usize(
            rustfs_config::ENV_OBJECT_CACHE_CAPACITY_MB,
            rustfs_config::DEFAULT_OBJECT_CACHE_CAPACITY_MB as usize,
        );
        let cache_ttl_secs =
            rustfs_utils::get_env_u64(rustfs_config::ENV_OBJECT_CACHE_TTL_SECS, rustfs_config::DEFAULT_OBJECT_CACHE_TTL_SECS);
        let cache_tti_secs =
            rustfs_utils::get_env_u64(rustfs_config::ENV_OBJECT_CACHE_TTI_SECS, rustfs_config::DEFAULT_OBJECT_CACHE_TTI_SECS);

        // Create Moka cache with size-based eviction
        let cache = Cache::builder()
            .max_capacity(max_capacity as u64 * MI_B as u64)
            .weigher(|_key: &String, value: &Arc<CachedObject>| -> u32 {
                // Weight based on actual data size
                value.data.len().min(u32::MAX as usize) as u32
            })
            .time_to_live(Duration::from_secs(cache_ttl_secs))
            .time_to_idle(Duration::from_secs(cache_tti_secs))
            .build();

        // Create response cache with same configuration
        let response_cache = Cache::builder()
            .max_capacity((max_capacity * MI_B) as u64)
            .weigher(|_key: &String, value: &Arc<CachedGetObjectInternal>| -> u32 { value.size.min(u32::MAX as usize) as u32 })
            .time_to_live(Duration::from_secs(cache_ttl_secs))
            .time_to_idle(Duration::from_secs(cache_tti_secs))
            .build();

        let max_object_size = rustfs_utils::get_env_usize(
            rustfs_config::ENV_OBJECT_CACHE_MAX_OBJECT_SIZE_MB,
            rustfs_config::DEFAULT_OBJECT_CACHE_MAX_OBJECT_SIZE_MB,
        ) * MI_B;

        Self {
            cache,
            response_cache,
            max_object_size,
            hit_count: Arc::new(AtomicU64::new(0)),
            miss_count: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Soft expiration determination
    fn should_expire(&self, obj: &Arc<CachedObject>) -> bool {
        let age_secs = obj.cached_at.elapsed().as_secs();
        let cache_ttl_secs =
            rustfs_utils::get_env_u64(rustfs_config::ENV_OBJECT_CACHE_TTL_SECS, rustfs_config::DEFAULT_OBJECT_CACHE_TTL_SECS);
        let hot_object_min_hits = rustfs_utils::get_env_usize(
            rustfs_config::ENV_OBJECT_HOT_MIN_HITS_TO_EXTEND,
            rustfs_config::DEFAULT_OBJECT_HOT_MIN_HITS_TO_EXTEND,
        );
        if age_secs >= cache_ttl_secs {
            let hits = obj.access_count.load(Ordering::Relaxed);
            return hits < hot_object_min_hits as u64;
        }
        false
    }

    /// Get an object from cache
    pub(crate) async fn get(&self, key: &str) -> Option<Arc<Vec<u8>>> {
        match self.cache.get(key).await {
            Some(cached) => {
                if self.should_expire(&cached) {
                    self.cache.invalidate(key).await;
                    self.miss_count.fetch_add(1, Ordering::Relaxed);
                    return None;
                }
                cached.access_count.fetch_add(1, Ordering::Relaxed);
                self.hit_count.fetch_add(1, Ordering::Relaxed);

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

    /// Put an object into cache
    pub(crate) async fn put(&self, key: String, data: Vec<u8>) {
        if data.len() > self.max_object_size {
            return;
        }

        let cached = Arc::new(CachedObject {
            data: Arc::new(data),
            cached_at: Instant::now(),
            access_count: Arc::new(AtomicU64::new(0)),
        });

        self.cache.insert(key, cached).await;
    }

    /// Get a cached GetObject response
    pub(crate) async fn get_response(&self, key: &str) -> Option<Arc<CachedGetObject>> {
        match self.response_cache.get(key).await {
            Some(cached) => {
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

                cached.data.increment_access();
                self.hit_count.fetch_add(1, Ordering::Relaxed);

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

    /// Put a GetObject response into cache
    pub(crate) async fn put_response(&self, key: String, response: CachedGetObject) {
        let size = response.size();
        if size > self.max_object_size {
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

    /// Invalidate a cache entry
    pub(crate) async fn invalidate(&self, key: &str) {
        self.cache.invalidate(key).await;
        self.response_cache.invalidate(key).await;

        #[cfg(all(feature = "metrics", not(test)))]
        {
            use metrics::counter;
            counter!("rustfs_object_cache_invalidations_total").increment(1);
        }
    }

    /// Invalidate cache entries for versioned object
    pub(crate) async fn invalidate_versioned(&self, bucket: &str, key: &str, version_id: Option<&str>) {
        let base_key = format!("{bucket}/{key}");
        self.invalidate(&base_key).await;

        if let Some(vid) = version_id {
            let versioned_key = format!("{base_key}?versionId={vid}");
            self.invalidate(&versioned_key).await;
        }
    }

    /// Get cache statistics
    pub(crate) async fn stats(&self) -> CacheStats {
        self.cache.run_pending_tasks().await;
        CacheStats {
            entries: self.cache.entry_count(),
            size: self.cache.weighted_size(),
            hits: self.hit_count.load(Ordering::Relaxed),
            misses: self.miss_count.load(Ordering::Relaxed),
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

    /// Get maximum cacheable object size
    pub(crate) fn max_object_size(&self) -> usize {
        self.max_object_size
    }

    /// Clear all cached objects
    pub(crate) async fn clear_all(&self) {
        self.cache.invalidate_all();
        self.response_cache.invalidate_all();
        self.cache.run_pending_tasks().await;
        self.response_cache.run_pending_tasks().await;
    }
}

/// Cache statistics for monitoring
#[derive(Debug, Clone)]
pub struct CacheStats {
    /// Number of entries in cache
    pub entries: u64,
    /// Total size in bytes
    pub size: u64,
    /// Total cache hits
    pub hits: u64,
    /// Total cache misses
    pub misses: u64,
}
