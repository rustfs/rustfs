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

use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::time::{Instant, interval};

use crate::fast_lock::{
    guard::FastLockGuard,
    manager_trait::LockManager,
    metrics::{AggregatedMetrics, GlobalMetrics},
    shard::LockShard,
    types::{BatchLockRequest, BatchLockResult, LockConfig, LockResult, ObjectKey, ObjectLockInfo, ObjectLockRequest},
};

/// High-performance object lock manager
#[derive(Debug)]
pub struct FastObjectLockManager {
    pub shards: Vec<Arc<LockShard>>,
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
            Ok(()) => {
                let guard = FastLockGuard::new(request.key, request.mode, request.owner, shard.clone());
                // Register guard to prevent premature cleanup
                shard.register_guard(guard.guard_id());
                Ok(guard)
            }
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
        // let bucket = bucket.into();
        // let object = object.into();
        // let owner = owner.into();
        // error!("acquire_write_lock: bucket={:?}, object={:?}, owner={:?}", bucket, object, owner);
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

    /// Acquire high-priority read lock - optimized for database queries
    pub async fn acquire_high_priority_read_lock(
        &self,
        bucket: impl Into<Arc<str>>,
        object: impl Into<Arc<str>>,
        owner: impl Into<Arc<str>>,
    ) -> Result<FastLockGuard, LockResult> {
        let request =
            ObjectLockRequest::new_read(bucket, object, owner).with_priority(crate::fast_lock::types::LockPriority::High);
        self.acquire_lock(request).await
    }

    /// Acquire high-priority write lock - optimized for database queries
    pub async fn acquire_high_priority_write_lock(
        &self,
        bucket: impl Into<Arc<str>>,
        object: impl Into<Arc<str>>,
        owner: impl Into<Arc<str>>,
    ) -> Result<FastLockGuard, LockResult> {
        let request =
            ObjectLockRequest::new_write(bucket, object, owner).with_priority(crate::fast_lock::types::LockPriority::High);
        self.acquire_lock(request).await
    }

    /// Acquire critical priority read lock - for system operations
    pub async fn acquire_critical_read_lock(
        &self,
        bucket: impl Into<Arc<str>>,
        object: impl Into<Arc<str>>,
        owner: impl Into<Arc<str>>,
    ) -> Result<FastLockGuard, LockResult> {
        let request =
            ObjectLockRequest::new_read(bucket, object, owner).with_priority(crate::fast_lock::types::LockPriority::Critical);
        self.acquire_lock(request).await
    }

    /// Acquire critical priority write lock - for system operations
    pub async fn acquire_critical_write_lock(
        &self,
        bucket: impl Into<Arc<str>>,
        object: impl Into<Arc<str>>,
        owner: impl Into<Arc<str>>,
    ) -> Result<FastLockGuard, LockResult> {
        let request =
            ObjectLockRequest::new_write(bucket, object, owner).with_priority(crate::fast_lock::types::LockPriority::Critical);
        self.acquire_lock(request).await
    }

    /// Acquire multiple locks atomically - optimized version
    pub async fn acquire_locks_batch(&self, batch_request: BatchLockRequest) -> BatchLockResult {
        // Pre-sort requests by (shard_id, key) to avoid deadlocks
        let mut sorted_requests = batch_request.requests;
        sorted_requests.sort_unstable_by(|a, b| {
            let shard_a = a.key.shard_index(self.shard_mask);
            let shard_b = b.key.shard_index(self.shard_mask);
            shard_a.cmp(&shard_b).then_with(|| a.key.cmp(&b.key))
        });

        // Try to use stack-allocated vectors for small batches, fallback to heap if needed
        let shard_groups = self.group_requests_by_shard(sorted_requests);

        // Choose strategy based on request type
        if batch_request.all_or_nothing {
            self.acquire_locks_two_phase_commit(&shard_groups).await
        } else {
            self.acquire_locks_best_effort(&shard_groups).await
        }
    }

    /// Group requests by shard with proper fallback handling
    fn group_requests_by_shard(
        &self,
        requests: Vec<ObjectLockRequest>,
    ) -> std::collections::HashMap<usize, Vec<ObjectLockRequest>> {
        let mut shard_groups = std::collections::HashMap::new();

        for request in requests {
            let shard_id = request.key.shard_index(self.shard_mask);
            shard_groups.entry(shard_id).or_insert_with(Vec::new).push(request);
        }

        shard_groups
    }

    /// Best effort acquisition (allows partial success)
    async fn acquire_locks_best_effort(
        &self,
        shard_groups: &std::collections::HashMap<usize, Vec<ObjectLockRequest>>,
    ) -> BatchLockResult {
        let mut all_successful = Vec::new();
        let mut all_failed = Vec::new();
        let mut guards = Vec::new();

        for (&shard_id, requests) in shard_groups {
            let shard = self.shards[shard_id].clone();

            for request in requests {
                let key = request.key.clone();
                let owner = request.owner.clone();
                let mode = request.mode;

                let acquired = if shard.try_fast_path_only(request) {
                    true
                } else {
                    match shard.acquire_lock(request).await {
                        Ok(()) => true,
                        Err(err) => {
                            all_failed.push((key.clone(), err));
                            false
                        }
                    }
                };

                if acquired {
                    let guard = FastLockGuard::new(key.clone(), mode, owner.clone(), shard.clone());
                    shard.register_guard(guard.guard_id());
                    all_successful.push(key);
                    guards.push(guard);
                }
            }
        }

        let all_acquired = all_failed.is_empty();
        BatchLockResult {
            successful_locks: all_successful,
            failed_locks: all_failed,
            all_acquired,
            guards,
        }
    }

    /// Two-phase commit for atomic acquisition
    async fn acquire_locks_two_phase_commit(
        &self,
        shard_groups: &std::collections::HashMap<usize, Vec<ObjectLockRequest>>,
    ) -> BatchLockResult {
        // Phase 1: Try to acquire all locks
        let mut acquired_guards = Vec::new();
        let mut failed_locks = Vec::new();

        'outer: for (&shard_id, requests) in shard_groups {
            let shard = self.shards[shard_id].clone();

            for request in requests {
                match shard.acquire_lock(request).await {
                    Ok(()) => {
                        let guard = FastLockGuard::new(request.key.clone(), request.mode, request.owner.clone(), shard.clone());
                        shard.register_guard(guard.guard_id());
                        acquired_guards.push(guard);
                    }
                    Err(err) => {
                        failed_locks.push((request.key.clone(), err));
                        break 'outer; // Stop on first failure
                    }
                }
            }
        }

        // Phase 2: If any failed, release all acquired locks with error tracking
        if !failed_locks.is_empty() {
            // Drop guards to release any acquired locks.
            drop(acquired_guards);
            return BatchLockResult {
                successful_locks: Vec::new(),
                failed_locks,
                all_acquired: false,
                guards: Vec::new(),
            };
        }

        let successful_locks = acquired_guards.iter().map(|guard| guard.key().clone()).collect();
        BatchLockResult {
            successful_locks,
            failed_locks: Vec::new(),
            all_acquired: true,
            guards: acquired_guards,
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

    /// Get pool statistics from all shards
    pub fn get_pool_stats(&self) -> Vec<(u64, u64, u64, usize)> {
        self.shards.iter().map(|shard| shard.pool_stats()).collect()
    }

    /// Force cleanup of expired locks using adaptive strategy
    pub async fn cleanup_expired(&self) -> usize {
        let mut total_cleaned = 0;

        for shard in &self.shards {
            total_cleaned += shard.adaptive_cleanup();
        }

        self.metrics.record_cleanup_run(total_cleaned);
        total_cleaned
    }

    /// Force cleanup with traditional strategy (for compatibility)
    pub async fn cleanup_expired_traditional(&self) -> usize {
        let max_idle_millis = self.config.max_idle_time.as_millis() as u64;
        let mut total_cleaned = 0;

        for shard in &self.shards {
            total_cleaned += shard.cleanup_expired_millis(max_idle_millis);
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
    pub fn get_shard(&self, key: &crate::fast_lock::types::ObjectKey) -> &Arc<LockShard> {
        let index = key.shard_index(self.shard_mask);
        &self.shards[index]
    }

    /// Start background cleanup task
    fn start_cleanup_task(&self) {
        let shards = self.shards.clone();
        let metrics = self.metrics.clone();
        let cleanup_interval = self.config.cleanup_interval;
        let _max_idle_time = self.config.max_idle_time;

        let handle = tokio::spawn(async move {
            let mut interval = interval(cleanup_interval);

            loop {
                interval.tick().await;

                let start = Instant::now();
                let mut total_cleaned = 0;

                // Use adaptive cleanup for better performance
                for shard in &shards {
                    total_cleaned += shard.adaptive_cleanup();
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
        if let Ok(handle_guard) = self.cleanup_handle.try_read()
            && let Some(handle) = handle_guard.as_ref()
        {
            handle.abort();
        }
    }
}

impl Clone for FastObjectLockManager {
    fn clone(&self) -> Self {
        Self {
            shards: self.shards.clone(),
            shard_mask: self.shard_mask,
            config: self.config.clone(),
            metrics: self.metrics.clone(),
            cleanup_handle: RwLock::new(None), // Don't clone the cleanup task
        }
    }
}

#[async_trait::async_trait]
impl LockManager for FastObjectLockManager {
    async fn acquire_lock(&self, request: ObjectLockRequest) -> Result<FastLockGuard, LockResult> {
        self.acquire_lock(request).await
    }

    async fn acquire_read_lock(
        &self,
        bucket: impl Into<Arc<str>> + Send,
        object: impl Into<Arc<str>> + Send,
        owner: impl Into<Arc<str>> + Send,
    ) -> Result<FastLockGuard, LockResult> {
        self.acquire_read_lock(bucket, object, owner).await
    }

    async fn acquire_read_lock_versioned(
        &self,
        bucket: impl Into<Arc<str>> + Send,
        object: impl Into<Arc<str>> + Send,
        version: impl Into<Arc<str>> + Send,
        owner: impl Into<Arc<str>> + Send,
    ) -> Result<FastLockGuard, LockResult> {
        self.acquire_read_lock_versioned(bucket, object, version, owner).await
    }

    async fn acquire_write_lock(
        &self,
        bucket: impl Into<Arc<str>> + Send,
        object: impl Into<Arc<str>> + Send,
        owner: impl Into<Arc<str>> + Send,
    ) -> Result<FastLockGuard, LockResult> {
        self.acquire_write_lock(bucket, object, owner).await
    }

    async fn acquire_write_lock_versioned(
        &self,
        bucket: impl Into<Arc<str>> + Send,
        object: impl Into<Arc<str>> + Send,
        version: impl Into<Arc<str>> + Send,
        owner: impl Into<Arc<str>> + Send,
    ) -> Result<FastLockGuard, LockResult> {
        self.acquire_write_lock_versioned(bucket, object, version, owner).await
    }

    async fn acquire_locks_batch(&self, batch_request: BatchLockRequest) -> BatchLockResult {
        self.acquire_locks_batch(batch_request).await
    }

    fn get_lock_info(&self, key: &ObjectKey) -> Option<ObjectLockInfo> {
        self.get_lock_info(key)
    }

    fn get_metrics(&self) -> AggregatedMetrics {
        self.get_metrics()
    }

    fn total_lock_count(&self) -> usize {
        self.total_lock_count()
    }

    fn get_pool_stats(&self) -> Vec<(u64, u64, u64, usize)> {
        self.get_pool_stats()
    }

    async fn cleanup_expired(&self) -> usize {
        self.cleanup_expired().await
    }

    async fn cleanup_expired_traditional(&self) -> usize {
        self.cleanup_expired_traditional().await
    }

    async fn shutdown(&self) {
        self.shutdown().await
    }

    fn is_disabled(&self) -> bool {
        false
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
            max_idle_time: Duration::from_secs(1), // Use 1 second for easier testing
            ..Default::default()
        };
        let manager = FastObjectLockManager::with_config(config);

        // Acquire and release some locks
        {
            let _guard = manager.acquire_read_lock("bucket", "obj1", "owner1").await.unwrap();
            let _guard2 = manager.acquire_read_lock("bucket", "obj2", "owner2").await.unwrap();
        } // Locks are released here

        // Check lock count before cleanup
        let count_before = manager.total_lock_count();
        assert!(count_before >= 2, "Should have at least 2 locks before cleanup");

        // Wait for idle timeout
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Force cleanup with traditional method to ensure cleanup for testing
        let cleaned = manager.cleanup_expired_traditional().await;

        let count_after = manager.total_lock_count();

        // The test should pass if cleanup works at all
        assert!(
            cleaned > 0 || count_after < count_before,
            "Cleanup should either clean locks or they should be cleaned by other means"
        );
    }
}
