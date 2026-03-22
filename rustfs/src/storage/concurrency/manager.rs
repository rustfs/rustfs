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

use super::io_schedule::{IoLoadLevel, IoLoadMetrics, IoPriority, IoQueueStatus, IoStrategy};
use super::object_cache::{CacheStats, CachedGetObject, HotObjectCache};
use super::request_guard::GetObjectGuard;
use rustfs_config::{KI_B, MI_B};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::Duration;
use tokio::sync::Semaphore;
use tracing::debug;

/// Global concurrency manager instance
pub(crate) static CONCURRENCY_MANAGER: LazyLock<ConcurrencyManager> = LazyLock::new(ConcurrencyManager::new);

/// Concurrency manager for coordinating concurrent GetObject requests.
pub struct ConcurrencyManager {
    /// Hot object cache for frequently accessed objects
    cache: Arc<HotObjectCache>,
    /// Semaphore to limit concurrent disk reads
    disk_read_semaphore: Arc<Semaphore>,
    /// Whether object caching is enabled
    cache_enabled: bool,
    /// I/O load metrics for adaptive strategy calculation
    io_metrics: Arc<Mutex<IoLoadMetrics>>,
}

impl std::fmt::Debug for ConcurrencyManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let io_metrics_info = if let Ok(metrics) = self.io_metrics.lock() {
            format!("avg_wait={:?}, observations={}", metrics.average_wait(), metrics.observation_count())
        } else {
            "locked".to_string()
        };
        f.debug_struct("ConcurrencyManager")
            .field("cache_enabled", &self.cache_enabled)
            .field("available_permits", &self.disk_read_semaphore.available_permits())
            .field("io_metrics", &io_metrics_info)
            .finish()
    }
}

impl ConcurrencyManager {
    /// Create a new concurrency manager.
    pub fn new() -> Self {
        let max_concurrent_reads = rustfs_utils::get_env_usize(
            rustfs_config::ENV_OBJECT_MAX_CONCURRENT_DISK_READS,
            rustfs_config::DEFAULT_OBJECT_MAX_CONCURRENT_DISK_READS,
        );
        let cache_enabled =
            rustfs_utils::get_env_bool(rustfs_config::ENV_OBJECT_CACHE_ENABLE, rustfs_config::DEFAULT_OBJECT_CACHE_ENABLE);

        Self {
            cache: Arc::new(HotObjectCache::new()),
            disk_read_semaphore: Arc::new(Semaphore::new(max_concurrent_reads)),
            cache_enabled,
            io_metrics: Arc::new(Mutex::new(IoLoadMetrics::new(100))),
        }
    }

    /// Get the global concurrency manager instance.
    pub fn global() -> &'static Self {
        &CONCURRENCY_MANAGER
    }

    /// Create a new request guard for tracking concurrent requests.
    pub fn track_request() -> GetObjectGuard {
        GetObjectGuard::new()
    }

    /// Make a cache key from bucket, key, and optional version ID.
    pub fn make_cache_key(bucket: &str, key: &str, version_id: Option<&str>) -> String {
        match version_id {
            Some(vid) => format!("{bucket}/{key}?versionId={vid}"),
            None => format!("{bucket}/{key}"),
        }
    }

    // ============================================
    // Cache Methods
    // ============================================

    /// Check if caching is enabled.
    pub fn is_cache_enabled(&self) -> bool {
        self.cache_enabled
    }

    /// Get a cached object.
    pub async fn get_cached_object(&self, key: &str) -> Option<Arc<CachedGetObject>> {
        if !self.cache_enabled {
            return None;
        }
        self.cache.get_response(key).await
    }

    /// Cache an object.
    pub async fn cache_object(&self, key: String, data: Vec<u8>) {
        if !self.cache_enabled {
            return;
        }
        self.cache.put(key, data).await;
    }

    /// Get a cached GetObject response.
    pub async fn get_cached_response(&self, key: &str) -> Option<Arc<CachedGetObject>> {
        if !self.cache_enabled {
            return None;
        }
        self.cache.get_response(key).await
    }

    /// Cache a GetObject response.
    pub async fn cache_response(&self, key: String, response: CachedGetObject) {
        if !self.cache_enabled {
            return;
        }
        self.cache.put_response(key, response).await;
    }

    /// Invalidate a cached object.
    pub async fn invalidate_cached(&self, key: &str) {
        self.cache.invalidate(key).await;
    }

    /// Invalidate a versioned cached object.
    pub async fn invalidate_cached_versioned(&self, bucket: &str, key: &str, version_id: Option<&str>) {
        self.cache.invalidate_versioned(bucket, key, version_id).await;
    }

    /// Invalidate a versioned cached object (alias).
    pub async fn invalidate_cache_versioned(&self, bucket: &str, key: &str, version_id: Option<&str>) {
        self.invalidate_cached_versioned(bucket, key, version_id).await;
    }

    /// Get cache statistics.
    pub async fn cache_stats(&self) -> CacheStats {
        self.cache.stats().await
    }

    /// Get cache hit rate.
    pub fn cache_hit_rate(&self) -> f64 {
        self.cache.hit_rate()
    }

    /// Clear all cached objects.
    pub async fn clear_cache(&self) {
        self.cache.clear_all().await;
    }

    /// Get maximum cacheable object size.
    pub fn max_cache_object_size(&self) -> usize {
        self.cache.max_object_size()
    }

    // ============================================
    // Disk Read Rate Limiting
    // ============================================

    /// Acquire a disk read permit.
    pub async fn acquire_disk_read_permit(&self) -> Result<tokio::sync::SemaphorePermit<'_>, tokio::sync::AcquireError> {
        self.disk_read_semaphore.acquire().await
    }

    /// Get the number of available disk read permits.
    pub fn available_disk_read_permits(&self) -> usize {
        self.disk_read_semaphore.available_permits()
    }

    // ============================================
    // I/O Strategy and Load Metrics
    // ============================================

    /// Record a disk permit wait observation.
    pub fn record_permit_wait(&self, wait_duration: Duration) {
        if let Ok(mut metrics) = self.io_metrics.lock() {
            metrics.record(wait_duration);
        }

        #[cfg(all(feature = "metrics", not(test)))]
        {
            use metrics::histogram;
            histogram!("rustfs.disk.permit.wait.duration.seconds").record(wait_duration.as_secs_f64());
        }
    }

    /// Calculate an adaptive I/O strategy.
    pub fn calculate_io_strategy(&self, permit_wait_duration: Duration, base_buffer_size: usize) -> IoStrategy {
        self.record_permit_wait(permit_wait_duration);
        IoStrategy::from_wait_duration(permit_wait_duration, base_buffer_size)
    }

    /// Get the smoothed I/O load level.
    pub fn smoothed_load_level(&self) -> IoLoadLevel {
        if let Ok(metrics) = self.io_metrics.lock() {
            metrics.smoothed_load_level()
        } else {
            IoLoadLevel::Medium
        }
    }

    /// Get adaptive buffer size based on current load.
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

    // ============================================
    // Priority-Based I/O Scheduling
    // ============================================

    /// Get I/O priority for a request.
    pub fn get_io_priority(&self, request_size: i64) -> IoPriority {
        if request_size < 0 {
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

    /// Get current I/O queue status.
    pub fn io_queue_status(&self) -> IoQueueStatus {
        let total_permits = rustfs_utils::get_env_usize(
            rustfs_config::ENV_OBJECT_MAX_CONCURRENT_DISK_READS,
            rustfs_config::DEFAULT_OBJECT_MAX_CONCURRENT_DISK_READS,
        );
        let permits_in_use = total_permits.saturating_sub(self.disk_read_semaphore.available_permits());

        IoQueueStatus {
            total_permits,
            permits_in_use,
            high_priority_waiting: 0,
            normal_priority_waiting: 0,
            low_priority_waiting: 0,
        }
    }

    /// Acquire a disk read permit with priority awareness.
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
}

impl Default for ConcurrencyManager {
    fn default() -> Self {
        Self::new()
    }
}
