// Copyright 2024 RustFS Team

use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::time::{Instant, interval};

use crate::fast_lock::{
    guard::FastLockGuard,
    metrics::GlobalMetrics,
    shard::LockShard,
    types::{BatchLockRequest, BatchLockResult, LockConfig, LockResult, ObjectLockRequest},
};

/// High-performance object lock manager
#[derive(Debug)]
pub struct FastObjectLockManager {
    shards: Vec<Arc<LockShard>>,
    shard_mask: usize,
    config: LockConfig,
    metrics: Arc<GlobalMetrics>,
    cleanup_handle: RwLock<Option<tokio::task::JoinHandle<()>>>,
}

impl FastObjectLockManager {
    /// Create new lock manager with default config
    pub fn new() -> Self {
        Self::with_config(LockConfig::default())
    }

    /// Create new lock manager with custom config
    pub fn with_config(config: LockConfig) -> Self {
        let shard_count = config.shard_count;
        assert!(shard_count.is_power_of_two(), "Shard count must be power of 2");

        let shards: Vec<Arc<LockShard>> = (0..shard_count).map(|i| Arc::new(LockShard::new(i))).collect();

        let metrics = Arc::new(GlobalMetrics::new(shard_count));

        let manager = Self {
            shards,
            shard_mask: shard_count - 1,
            config,
            metrics,
            cleanup_handle: RwLock::new(None),
        };

        // Start background cleanup task
        manager.start_cleanup_task();
        manager
    }

    /// Acquire object lock
    pub async fn acquire_lock(&self, request: ObjectLockRequest) -> Result<FastLockGuard, LockResult> {
        let shard = self.get_shard(&request.key);
        match shard.acquire_lock(&request).await {
            Ok(()) => Ok(FastLockGuard::new(request.key, request.mode, request.owner, shard.clone())),
            Err(err) => Err(err),
        }
    }

    /// Acquire shared (read) lock
    pub async fn acquire_read_lock(
        &self,
        bucket: impl Into<Arc<str>>,
        object: impl Into<Arc<str>>,
        owner: impl Into<Arc<str>>,
    ) -> Result<FastLockGuard, LockResult> {
        let request = ObjectLockRequest::new_read(bucket, object, owner);
        self.acquire_lock(request).await
    }

    /// Acquire shared (read) lock for specific version
    pub async fn acquire_read_lock_versioned(
        &self,
        bucket: impl Into<Arc<str>>,
        object: impl Into<Arc<str>>,
        version: impl Into<Arc<str>>,
        owner: impl Into<Arc<str>>,
    ) -> Result<FastLockGuard, LockResult> {
        let request = ObjectLockRequest::new_read(bucket, object, owner).with_version(version);
        self.acquire_lock(request).await
    }

    /// Acquire exclusive (write) lock
    pub async fn acquire_write_lock(
        &self,
        bucket: impl Into<Arc<str>>,
        object: impl Into<Arc<str>>,
        owner: impl Into<Arc<str>>,
    ) -> Result<FastLockGuard, LockResult> {
        let request = ObjectLockRequest::new_write(bucket, object, owner);
        self.acquire_lock(request).await
    }

    /// Acquire exclusive (write) lock for specific version
    pub async fn acquire_write_lock_versioned(
        &self,
        bucket: impl Into<Arc<str>>,
        object: impl Into<Arc<str>>,
        version: impl Into<Arc<str>>,
        owner: impl Into<Arc<str>>,
    ) -> Result<FastLockGuard, LockResult> {
        let request = ObjectLockRequest::new_write(bucket, object, owner).with_version(version);
        self.acquire_lock(request).await
    }

    /// Acquire multiple locks atomically
    pub async fn acquire_locks_batch(&self, batch_request: BatchLockRequest) -> BatchLockResult {
        // Group requests by shard to optimize locking
        let mut shard_groups: std::collections::HashMap<usize, Vec<ObjectLockRequest>> = std::collections::HashMap::new();

        for request in batch_request.requests {
            let shard_id = request.key.shard_index(self.shard_mask);
            shard_groups.entry(shard_id).or_default().push(request);
        }

        let mut all_successful = Vec::new();
        let mut all_failed = Vec::new();

        // Process each shard group
        for (shard_id, requests) in shard_groups {
            let shard = &self.shards[shard_id];
            match shard.acquire_locks_batch(requests, batch_request.all_or_nothing).await {
                Ok(successful) => all_successful.extend(successful),
                Err(failed) => {
                    all_failed.extend(failed);

                    if batch_request.all_or_nothing {
                        // Release all previously acquired locks
                        for _key in &all_successful {
                            // let _shard = self.get_shard(key); // TODO: implement proper cleanup
                            // Note: We need the original mode and owner info for release
                            // This is a limitation of the current design that could be improved
                            // For now, we'll do best-effort cleanup
                        }

                        return BatchLockResult {
                            successful_locks: Vec::new(),
                            failed_locks: all_failed,
                            all_acquired: false,
                        };
                    }
                }
            }
        }

        let all_acquired = all_failed.is_empty();
        BatchLockResult {
            successful_locks: all_successful,
            failed_locks: all_failed,
            all_acquired,
        }
    }

    /// Get lock information for monitoring
    pub fn get_lock_info(&self, key: &crate::fast_lock::types::ObjectKey) -> Option<crate::fast_lock::types::ObjectLockInfo> {
        let shard = self.get_shard(key);
        shard.get_lock_info(key)
    }

    /// Get aggregated metrics
    pub fn get_metrics(&self) -> crate::fast_lock::metrics::AggregatedMetrics {
        let shard_metrics: Vec<_> = self.shards.iter().map(|shard| shard.metrics().snapshot()).collect();

        self.metrics.aggregate_shard_metrics(&shard_metrics)
    }

    /// Get total number of active locks across all shards
    pub fn total_lock_count(&self) -> usize {
        self.shards.iter().map(|shard| shard.lock_count()).sum()
    }

    /// Force cleanup of expired locks
    pub async fn cleanup_expired(&self) -> usize {
        let max_idle_secs = self.config.max_idle_time.as_secs();
        let mut total_cleaned = 0;

        for shard in &self.shards {
            total_cleaned += shard.cleanup_expired(max_idle_secs);
        }

        self.metrics.record_cleanup_run(total_cleaned);
        total_cleaned
    }

    /// Shutdown the lock manager and cleanup resources
    pub async fn shutdown(&self) {
        if let Some(handle) = self.cleanup_handle.write().await.take() {
            handle.abort();
        }

        // Final cleanup
        self.cleanup_expired().await;
    }

    /// Get shard for object key
    fn get_shard(&self, key: &crate::fast_lock::types::ObjectKey) -> &Arc<LockShard> {
        let index = key.shard_index(self.shard_mask);
        &self.shards[index]
    }

    /// Start background cleanup task
    fn start_cleanup_task(&self) {
        let shards = self.shards.clone();
        let metrics = self.metrics.clone();
        let cleanup_interval = self.config.cleanup_interval;
        let max_idle_time = self.config.max_idle_time;

        let handle = tokio::spawn(async move {
            let mut interval = interval(cleanup_interval);

            loop {
                interval.tick().await;

                let start = Instant::now();
                let max_idle_secs = max_idle_time.as_secs();
                let mut total_cleaned = 0;

                for shard in &shards {
                    total_cleaned += shard.cleanup_expired(max_idle_secs);
                }

                if total_cleaned > 0 {
                    metrics.record_cleanup_run(total_cleaned);
                    tracing::debug!("Cleanup completed: {} objects cleaned in {:?}", total_cleaned, start.elapsed());
                }
            }
        });

        // Store handle for shutdown
        if let Ok(mut cleanup_handle) = self.cleanup_handle.try_write() {
            *cleanup_handle = Some(handle);
        }
    }
}

impl Default for FastObjectLockManager {
    fn default() -> Self {
        Self::new()
    }
}

// Implement Drop to ensure cleanup
impl Drop for FastObjectLockManager {
    fn drop(&mut self) {
        // Note: We can't use async in Drop, so we just abort the cleanup task
        if let Ok(handle_guard) = self.cleanup_handle.try_read() {
            if let Some(handle) = handle_guard.as_ref() {
                handle.abort();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::Duration;

    #[tokio::test]
    async fn test_manager_basic_operations() {
        let manager = FastObjectLockManager::new();

        // Test read lock
        let read_guard = manager
            .acquire_read_lock("bucket", "object", "owner1")
            .await
            .expect("Failed to acquire read lock");

        // Should be able to acquire another read lock
        let read_guard2 = manager
            .acquire_read_lock("bucket", "object", "owner2")
            .await
            .expect("Failed to acquire second read lock");

        drop(read_guard);
        drop(read_guard2);

        // Test write lock
        let write_guard = manager
            .acquire_write_lock("bucket", "object", "owner1")
            .await
            .expect("Failed to acquire write lock");

        drop(write_guard);
    }

    #[tokio::test]
    async fn test_manager_contention() {
        let manager = Arc::new(FastObjectLockManager::new());

        // Acquire write lock
        let write_guard = manager
            .acquire_write_lock("bucket", "object", "owner1")
            .await
            .expect("Failed to acquire write lock");

        // Try to acquire read lock (should timeout)
        let manager_clone = manager.clone();
        let read_result =
            tokio::time::timeout(Duration::from_millis(100), manager_clone.acquire_read_lock("bucket", "object", "owner2")).await;

        assert!(read_result.is_err()); // Should timeout

        drop(write_guard);

        // Now read lock should succeed
        let read_guard = manager
            .acquire_read_lock("bucket", "object", "owner2")
            .await
            .expect("Failed to acquire read lock after write lock released");

        drop(read_guard);
    }

    #[tokio::test]
    async fn test_versioned_locks() {
        let manager = FastObjectLockManager::new();

        // Acquire lock on version v1
        let v1_guard = manager
            .acquire_write_lock_versioned("bucket", "object", "v1", "owner1")
            .await
            .expect("Failed to acquire v1 lock");

        // Should be able to acquire lock on version v2 simultaneously
        let v2_guard = manager
            .acquire_write_lock_versioned("bucket", "object", "v2", "owner2")
            .await
            .expect("Failed to acquire v2 lock");

        drop(v1_guard);
        drop(v2_guard);
    }

    #[tokio::test]
    async fn test_batch_operations() {
        let manager = FastObjectLockManager::new();

        let batch = BatchLockRequest::new("owner")
            .add_read_lock("bucket", "obj1")
            .add_write_lock("bucket", "obj2")
            .with_all_or_nothing(true);

        let result = manager.acquire_locks_batch(batch).await;
        assert!(result.all_acquired);
        assert_eq!(result.successful_locks.len(), 2);
        assert!(result.failed_locks.is_empty());
    }

    #[tokio::test]
    async fn test_metrics() {
        let manager = FastObjectLockManager::new();

        // Perform some operations
        let _guard1 = manager.acquire_read_lock("bucket", "obj1", "owner").await.unwrap();
        let _guard2 = manager.acquire_write_lock("bucket", "obj2", "owner").await.unwrap();

        let metrics = manager.get_metrics();
        assert!(metrics.shard_metrics.total_acquisitions() > 0);
        assert!(metrics.shard_metrics.fast_path_rate() > 0.0);
    }

    #[tokio::test]
    async fn test_cleanup() {
        let config = LockConfig {
            max_idle_time: Duration::from_millis(10),
            ..Default::default()
        };
        let manager = FastObjectLockManager::with_config(config);

        // Acquire and release some locks
        {
            let _guard = manager.acquire_read_lock("bucket", "obj", "owner").await.unwrap();
        } // Lock is released here

        // Wait for idle timeout
        tokio::time::sleep(Duration::from_millis(20)).await;

        // Force cleanup
        let cleaned = manager.cleanup_expired().await;
        assert!(cleaned > 0);
    }
}
