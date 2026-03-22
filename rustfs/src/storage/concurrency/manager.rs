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

//! Concurrency manager for coordinating concurrent GetObject requests.

use super::io_schedule::{IoLoadLevel, IoLoadMetrics, IoPriority, IoQueueStatus, IoStrategy, get_advanced_buffer_size};
use super::object_cache::{CacheStats, CachedGetObject, HotObjectCache};
use super::request_guard::GetObjectGuard;
use rustfs_config::{KI_B, MI_B};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::Duration;
use tokio::sync::Semaphore;
use tracing::debug;

/// Global concurrency manager instance
pub(crate) static CONCURRENCY_MANAGER: LazyLock<ConcurrencyManager> = LazyLock::new(ConcurrencyManager::new);

#[derive(Clone)]
pub struct ConcurrencyManager {
    /// Hot object cache for frequently accessed objects
    cache: Arc<HotObjectCache>,
    /// Semaphore to limit concurrent disk reads
    disk_read_semaphore: Arc<Semaphore>,
    /// Whether object caching is enabled (from RUSTFS_OBJECT_CACHE_ENABLE env var)
    cache_enabled: bool,
    /// I/O load metrics for adaptive strategy calculation
    io_metrics: Arc<Mutex<IoLoadMetrics>>,
}

impl std::fmt::Debug for ConcurrencyManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use std::sync::atomic::Ordering;
        let io_metrics_info = if let Ok(metrics) = self.io_metrics.lock() {
            format!("avg_wait={:?}, observations={}", metrics.average_wait(), metrics.observation_count())
        } else {
            "locked".to_string()
        };
        f.debug_struct("ConcurrencyManager")
            .field("active_requests", &super::io_schedule::ACTIVE_GET_REQUESTS.load(Ordering::Relaxed))
            .field("disk_read_permits", &self.disk_read_semaphore.available_permits())
            .field("io_metrics", &io_metrics_info)
            .finish()
    }
}
#[allow(dead_code)]
impl ConcurrencyManager {
    /// Create a new concurrency manager with default settings
    ///
    /// Reads configuration from environment variables:
    /// - `RUSTFS_OBJECT_CACHE_ENABLE`: Enable/disable object caching (default: false)
    pub fn new() -> Self {
        let cache_enabled =
            rustfs_utils::get_env_bool(rustfs_config::ENV_OBJECT_CACHE_ENABLE, rustfs_config::DEFAULT_OBJECT_CACHE_ENABLE);

        let max_disk_reads = rustfs_utils::get_env_usize(
            rustfs_config::ENV_OBJECT_MAX_CONCURRENT_DISK_READS,
            rustfs_config::DEFAULT_OBJECT_MAX_CONCURRENT_DISK_READS,
        );

        Self {
            cache: Arc::new(HotObjectCache::new()),
            disk_read_semaphore: Arc::new(Semaphore::new(max_disk_reads)),
            cache_enabled,
            io_metrics: Arc::new(Mutex::new(IoLoadMetrics::new(100))), // Keep last 100 observations
        }
    }

    /// Check if object caching is enabled
    ///
    /// Returns true if the `RUSTFS_OBJECT_CACHE_ENABLE` environment variable
    /// is set to "true" (case-insensitive). When disabled, cache lookups and
    /// writebacks are skipped, reducing memory usage at the cost of repeated
    /// disk reads for the same objects.
    ///
    /// # Returns
    ///
    /// `true` if caching is enabled, `false` otherwise
    pub fn is_cache_enabled(&self) -> bool {
        self.cache_enabled
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
    pub async fn cache_object(&self, key: String, data: Vec<u8>) {
        self.cache.put(key, data).await;
    }

    /// Acquire a permit to perform a disk read operation
    ///
    /// This ensures we don't overwhelm the disk subsystem with too many
    /// concurrent reads, which can cause performance degradation.
    pub async fn acquire_disk_read_permit(&self) -> Result<tokio::sync::SemaphorePermit<'_>, tokio::sync::AcquireError> {
        self.disk_read_semaphore.acquire().await
    }

    // ============================================
    // Adaptive I/O Strategy Methods
    // ============================================

    /// Record a disk permit wait observation for load tracking.
    ///
    /// This method updates the rolling metrics used to calculate adaptive I/O
    /// strategies. Should be called after each disk permit acquisition.
    ///
    /// # Arguments
    ///
    /// * `wait_duration` - Time spent waiting for the disk read permit
    pub fn record_permit_wait(&self, wait_duration: Duration) {
        if let Ok(mut metrics) = self.io_metrics.lock() {
            metrics.record(wait_duration);
        }

        // Record histogram metric for Prometheus
        #[cfg(all(feature = "metrics", not(test)))]
        {
            use metrics::histogram;
            histogram!("rustfs.disk.permit.wait.duration.seconds").record(wait_duration.as_secs_f64());
        }
    }

    /// Calculate an adaptive I/O strategy based on disk permit wait time.
    ///
    /// This method analyzes the permit wait duration to determine the current
    /// I/O load level and returns optimized parameters for the read operation.
    ///
    /// # Arguments
    ///
    /// * `permit_wait_duration` - Time spent waiting for disk read permit
    /// * `base_buffer_size` - Base buffer size from workload configuration
    ///
    /// # Returns
    ///
    /// An `IoStrategy` containing optimized I/O parameters.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let permit_wait_start = Instant::now();
    /// let _permit = manager.acquire_disk_read_permit().await;
    /// let permit_wait_duration = permit_wait_start.elapsed();
    ///
    /// let strategy = manager.calculate_io_strategy(permit_wait_duration, 256 * 1024);
    /// let optimal_buffer = strategy.buffer_size;
    /// ```
    pub fn calculate_io_strategy(&self, permit_wait_duration: Duration, base_buffer_size: usize) -> IoStrategy {
        // Record the observation for future smoothing
        self.record_permit_wait(permit_wait_duration);

        // Calculate strategy from the current wait duration
        IoStrategy::from_wait_duration(permit_wait_duration, base_buffer_size)
    }

    /// Get the smoothed I/O load level based on recent observations.
    ///
    /// This uses the rolling window of permit wait times to provide a more
    /// stable estimate of the current load level, reducing oscillation from
    /// transient spikes.
    ///
    /// # Returns
    ///
    /// The smoothed `IoLoadLevel` based on average recent wait times.
    pub fn smoothed_load_level(&self) -> IoLoadLevel {
        if let Ok(metrics) = self.io_metrics.lock() {
            metrics.smoothed_load_level()
        } else {
            IoLoadLevel::Medium // Default to medium if lock fails
        }
    }

    /// Get I/O load statistics for monitoring.
    ///
    /// Returns statistics about recent disk permit wait times for
    /// monitoring dashboards and capacity planning.
    ///
    /// # Returns
    ///
    /// A tuple of (average_wait, p95_wait, max_wait, observation_count)
    pub fn io_load_stats(&self) -> (Duration, Duration, Duration, u64) {
        if let Ok(metrics) = self.io_metrics.lock() {
            (
                metrics.average_wait(),
                metrics.p95_wait(),
                metrics.max_wait(),
                metrics.observation_count(),
            )
        } else {
            (Duration::ZERO, Duration::ZERO, Duration::ZERO, 0)
        }
    }

    /// Get the recommended buffer size based on current I/O load.
    ///
    /// This is a convenience method that combines load level detection with
    /// buffer size calculation. Uses the smoothed load level for stability.
    ///
    /// # Arguments
    ///
    /// * `base_buffer_size` - Base buffer size from workload configuration
    ///
    /// # Returns
    ///
    /// Recommended buffer size in bytes.
    pub fn adaptive_buffer_size(&self, base_buffer_size: usize) -> usize {
        let load_level = self.smoothed_load_level();
        let multiplier = match load_level {
            IoLoadLevel::Low => 1.0,
            IoLoadLevel::Medium => 0.75,
            IoLoadLevel::High => 0.5,
            IoLoadLevel::Critical => 0.4,
        };

        let buffer_size = ((base_buffer_size as f64) * multiplier) as usize;
        buffer_size.clamp(32 * KI_B, MI_B)
    }

    /// Get cache statistics
    pub async fn cache_stats(&self) -> CacheStats {
        self.cache.stats().await
    }

    /// Clear all cached objects
    pub async fn clear_cache(&self) {
        self.cache.clear().await;
    }

    /// Check if a key is cached
    pub async fn is_cached(&self, key: &str) -> bool {
        self.cache.contains(key).await
    }

    /// Get multiple cached objects in a single operation
    pub async fn get_cached_batch(&self, keys: &[String]) -> Vec<Option<Arc<Vec<u8>>>> {
        self.cache.get_batch(keys).await
    }

    /// Remove a specific object from cache
    pub async fn remove_cached(&self, key: &str) -> bool {
        self.cache.remove(key).await
    }

    /// Get the most frequently accessed keys
    pub async fn get_hot_keys(&self, limit: usize) -> Vec<(String, u64)> {
        self.cache.get_hot_keys(limit).await
    }

    /// Get cache hit rate percentage
    pub fn cache_hit_rate(&self) -> f64 {
        self.cache.hit_rate()
    }

    /// Warm up cache with frequently accessed objects
    ///
    /// This can be called during server startup or maintenance windows
    /// to pre-populate the cache with known hot objects.
    pub async fn warm_cache(&self, objects: Vec<(String, Vec<u8>)>) {
        self.cache.warm(objects).await;
    }

    /// Get optimized buffer size for a request
    ///
    /// This wraps the advanced buffer sizing logic and makes it accessible
    /// through the concurrency manager interface.
    pub fn buffer_size(&self, file_size: i64, base: usize, sequential: bool) -> usize {
        get_advanced_buffer_size(file_size, base, sequential)
    }

    // ============================================
    // Response Cache Methods (CachedGetObject)
    // ============================================

    /// Get a cached GetObject response with full metadata
    ///
    /// This method retrieves a complete GetObject response from the response cache,
    /// including body data and all response metadata (e_tag, last_modified, content_type, etc.).
    ///
    /// # Arguments
    ///
    /// * `key` - Cache key in the format "{bucket}/{key}" or "{bucket}/{key}?versionId={version_id}"
    ///
    /// # Returns
    ///
    /// * `Some(Arc<CachedGetObject>)` - Cached response data if found and not expired
    /// * `None` - Cache miss
    ///
    /// # Example
    ///
    /// ```ignore
    /// let cache_key = format!("{}/{}", bucket, key);
    /// if let Some(cached) = manager.get_cached_object(&cache_key).await {
    ///     // Build response from cached data
    ///     let output = GetObjectOutput {
    ///         body: Some(StreamingBlob::from(cached.body.clone())),
    ///         content_length: Some(cached.content_length),
    ///         e_tag: cached.e_tag.clone(),
    ///         last_modified: cached.last_modified.as_ref().map(|s| parse_rfc3339(s)),
    ///         ..Default::default()
    ///     };
    /// }
    /// ```
    pub async fn get_cached_object(&self, key: &str) -> Option<Arc<CachedGetObject>> {
        self.cache.get_response(key).await
    }

    /// Cache a complete GetObject response for future retrievals
    ///
    /// This method caches a complete GetObject response including body and all metadata.
    /// Objects larger than the maximum cache size (10MB by default) or empty objects
    /// are not cached.
    ///
    /// # Arguments
    ///
    /// * `key` - Cache key in the format "{bucket}/{key}" or "{bucket}/{key}?versionId={version_id}"
    /// * `response` - The complete cached response to store
    ///
    /// # Example
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
    pub async fn put_cached_object(&self, key: String, response: CachedGetObject) {
        self.cache.put_response(key, response).await;
    }

    /// Invalidate cache entries for a specific object
    ///
    /// This method removes both simple byte cache and response cache entries
    /// for the given key. Should be called after write operations (put_object,
    /// copy_object, delete_object, etc.) to prevent stale data from being served.
    ///
    /// # Arguments
    ///
    /// * `key` - Cache key to invalidate (e.g., "{bucket}/{key}")
    ///
    /// # Example
    ///
    /// ```ignore
    /// // After put_object succeeds
    /// let cache_key = format!("{}/{}", bucket, key);
    /// manager.invalidate_cache(&cache_key).await;
    /// ```
    pub async fn invalidate_cache(&self, key: &str) {
        self.cache.invalidate(key).await;
    }

    /// Invalidate cache entries for an object and its latest version
    ///
    /// For versioned buckets, this invalidates both:
    /// - The specific version key: "{bucket}/{key}?versionId={version_id}"
    /// - The latest version key: "{bucket}/{key}"
    ///
    /// This ensures that after a write/delete, clients don't receive stale data.
    /// Should be called after any write operation that modifies object data or creates
    /// new versions.
    ///
    /// # Arguments
    ///
    /// * `bucket` - Bucket name
    /// * `key` - Object key
    /// * `version_id` - Optional version ID (if None, only invalidates the base key)
    ///
    /// # Example
    ///
    /// ```ignore
    /// // After delete_object with version
    /// manager.invalidate_cache_versioned(&bucket, &key, Some(&version_id)).await;
    ///
    /// // After put_object (invalidates latest)
    /// manager.invalidate_cache_versioned(&bucket, &key, None).await;
    /// ```
    pub async fn invalidate_cache_versioned(&self, bucket: &str, key: &str, version_id: Option<&str>) {
        self.cache.invalidate_versioned(bucket, key, version_id).await;
    }

    /// Generate a cache key for an object
    ///
    /// Creates a cache key in the appropriate format based on whether a version ID
    /// is specified. For versioned requests, uses "{bucket}/{key}?versionId={version_id}".
    /// For non-versioned requests, uses "{bucket}/{key}".
    ///
    /// # Arguments
    ///
    /// * `bucket` - Bucket name
    /// * `key` - Object key
    /// * `version_id` - Optional version ID
    ///
    /// # Returns
    ///
    /// Cache key string
    pub fn make_cache_key(bucket: &str, key: &str, version_id: Option<&str>) -> String {
        match version_id {
            Some(vid) => format!("{bucket}/{key}?versionId={vid}"),
            None => format!("{bucket}/{key}"),
        }
    }

    /// Get maximum cacheable object size
    ///
    /// Returns the maximum size in bytes for objects that can be cached.
    /// Objects larger than this size are not cached to prevent memory exhaustion.
    pub fn max_object_size(&self) -> usize {
        self.cache.max_object_size()
    }

    // ============================================
    // Priority-Based I/O Scheduling Methods
    // ============================================

    /// Get I/O priority for a request based on its size.
    ///
    /// This enables priority-based scheduling where small requests
    /// are processed before large requests to prevent starvation.
    ///
    /// # Arguments
    ///
    /// * `request_size` - Size of the request in bytes (-1 if unknown)
    ///
    /// # Returns
    ///
    /// Priority level (High, Normal, or Low)
    pub fn get_io_priority(&self, request_size: i64) -> IoPriority {
        if request_size < 0 {
            // Unknown size, use normal priority
            IoPriority::Normal
        } else {
            IoPriority::from_size(request_size)
        }
    }

    /// Check if priority scheduling is enabled.
    pub fn is_priority_scheduling_enabled(&self) -> bool {
        rustfs_utils::get_env_bool(
            rustfs_config::ENV_OBJECT_PRIORITY_SCHEDULING_ENABLE,
            rustfs_config::DEFAULT_OBJECT_PRIORITY_SCHEDULING_ENABLE,
        )
    }

    /// Get current I/O queue status for monitoring.
    ///
    /// Returns information about permit usage and waiting requests.
    pub fn io_queue_status(&self) -> IoQueueStatus {
        let total_permits = rustfs_utils::get_env_usize(
            rustfs_config::ENV_OBJECT_MAX_CONCURRENT_DISK_READS,
            rustfs_config::DEFAULT_OBJECT_MAX_CONCURRENT_DISK_READS,
        );
        let permits_in_use = total_permits.saturating_sub(self.disk_read_semaphore.available_permits());

        IoQueueStatus {
            total_permits,
            permits_in_use,
            high_priority_waiting: 0, // Would need additional tracking
            normal_priority_waiting: 0,
            low_priority_waiting: 0,
        }
    }

    /// Acquire a disk read permit with priority awareness.
    ///
    /// When priority scheduling is enabled, this method logs the priority
    /// for observability. The actual acquisition uses the same semaphore
    /// but priority information is used for monitoring.
    ///
    /// # Arguments
    ///
    /// * `priority` - Priority level for this request
    ///
    /// # Returns
    ///
    /// Semaphore permit on success, error on failure
    pub async fn acquire_priority_permit(
        &self,
        priority: IoPriority,
    ) -> Result<tokio::sync::SemaphorePermit<'_>, tokio::sync::AcquireError> {
        #[cfg(feature = "metrics")]
        {
            use metrics::counter;
            counter!("rustfs.disk.read.queue.total", "priority" => priority.as_str()).increment(1);
        }

        debug!(
            priority = %priority,
            available_permits = self.disk_read_semaphore.available_permits(),
            "Acquiring disk read permit"
        );

        self.disk_read_semaphore.acquire().await
    }

    /// Get the global concurrency manager instance.
    pub fn global() -> &'static Self {
        &CONCURRENCY_MANAGER
    }
}

impl Default for ConcurrencyManager {
    fn default() -> Self {
        Self::new()
    }
}
