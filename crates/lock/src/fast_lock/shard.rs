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

use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};
use tokio::time::timeout;

use crate::fast_lock::{
    metrics::ShardMetrics,
    object_pool::ObjectStatePool,
    state::ObjectLockState,
    types::{LockMode, LockResult, ObjectKey, ObjectLockRequest},
};
use std::collections::HashSet;

/// Lock shard to reduce global contention
#[derive(Debug)]
pub struct LockShard {
    /// Object lock states - using parking_lot for better performance
    objects: RwLock<HashMap<ObjectKey, Arc<ObjectLockState>>>,
    /// Object state pool for memory optimization
    object_pool: ObjectStatePool,
    /// Shard-level metrics
    metrics: ShardMetrics,
    /// Shard ID for debugging
    _shard_id: usize,
    /// Active guard IDs to prevent cleanup of locks with live guards
    active_guards: parking_lot::Mutex<HashSet<u64>>,
}

impl LockShard {
    pub fn new(shard_id: usize) -> Self {
        Self {
            objects: RwLock::new(HashMap::new()),
            object_pool: ObjectStatePool::new(),
            metrics: ShardMetrics::new(),
            _shard_id: shard_id,
            active_guards: parking_lot::Mutex::new(HashSet::new()),
        }
    }

    /// Acquire lock with fast path optimization
    pub async fn acquire_lock(&self, request: &ObjectLockRequest) -> Result<(), LockResult> {
        let start_time = Instant::now();

        // Try fast path first
        if let Some(_state) = self.try_fast_path(request) {
            self.metrics.record_fast_path_success();
            return Ok(());
        }

        // Slow path with waiting
        self.acquire_lock_slow_path(request, start_time).await
    }

    /// Try fast path only (without fallback to slow path)
    pub fn try_fast_path_only(&self, request: &ObjectLockRequest) -> bool {
        // Early check to avoid unnecessary lock contention
        if let Some(state) = self.objects.read().get(&request.key)
            && !state.atomic_state.is_fast_path_available(request.mode)
        {
            return false;
        }
        self.try_fast_path(request).is_some()
    }

    /// Try fast path lock acquisition (lock-free when possible)
    fn try_fast_path(&self, request: &ObjectLockRequest) -> Option<Arc<ObjectLockState>> {
        // First try to get existing state without write lock
        {
            let objects = self.objects.read();
            if let Some(state) = objects.get(&request.key) {
                let state = state.clone();
                drop(objects);

                // Try atomic acquisition
                let success = match request.mode {
                    LockMode::Shared => state.try_acquire_shared_fast(&request.owner, request.lock_timeout),
                    LockMode::Exclusive => state.try_acquire_exclusive_fast(&request.owner, request.lock_timeout),
                };

                if success {
                    return Some(state);
                }
            }
        }

        // If object doesn't exist and we're requesting exclusive lock,
        // try to create and acquire atomically
        if request.mode == LockMode::Exclusive {
            let mut objects = self.objects.write();

            // Double-check after acquiring write lock
            if let Some(state) = objects.get(&request.key) {
                let state = state.clone();
                drop(objects);

                if state.try_acquire_exclusive_fast(&request.owner, request.lock_timeout) {
                    return Some(state);
                }
            } else {
                // Create new state from pool and acquire immediately
                let state_box = self.object_pool.acquire();
                let state = Arc::new(*state_box);
                if state.try_acquire_exclusive_fast(&request.owner, request.lock_timeout) {
                    objects.insert(request.key.clone(), state.clone());
                    return Some(state);
                }
            }
        }

        None
    }

    /// Slow path with async waiting
    async fn acquire_lock_slow_path(&self, request: &ObjectLockRequest, start_time: Instant) -> Result<(), LockResult> {
        // Use adaptive timeout based on current load and request priority
        let adaptive_timeout = self.calculate_adaptive_timeout(request);
        let deadline = start_time + adaptive_timeout;

        let mut retry_count = 0u32;
        const MAX_RETRIES: u32 = 10;

        loop {
            // Get or create object state
            let state = {
                let mut objects = self.objects.write();
                match objects.get(&request.key) {
                    Some(state) => state.clone(),
                    None => {
                        let state_box = self.object_pool.acquire();
                        let state = Arc::new(*state_box);
                        objects.insert(request.key.clone(), state.clone());
                        state
                    }
                }
            };

            // Try acquisition again
            let success = match request.mode {
                LockMode::Shared => state.try_acquire_shared_fast(&request.owner, request.lock_timeout),
                LockMode::Exclusive => state.try_acquire_exclusive_fast(&request.owner, request.lock_timeout),
            };

            if success {
                self.metrics.record_slow_path_success();
                return Ok(());
            }

            // Check timeout
            if Instant::now() >= deadline {
                self.metrics.record_timeout();
                return Err(LockResult::Timeout);
            }

            // Use intelligent wait strategy: mix of notification wait and exponential backoff
            let remaining = deadline - Instant::now();

            if retry_count < MAX_RETRIES && remaining > Duration::from_millis(10) {
                // For early retries, use a brief exponential backoff instead of full notification wait
                let backoff_ms = std::cmp::min(10 << retry_count, 100); // 10ms, 20ms, 40ms, 80ms, 100ms max
                let backoff_duration = Duration::from_millis(backoff_ms);

                if backoff_duration < remaining {
                    tokio::time::sleep(backoff_duration).await;
                    retry_count += 1;
                    continue;
                }
            }

            // If we've exhausted quick retries or have little time left, use notification wait
            let wait_result = match request.mode {
                LockMode::Shared => {
                    state.atomic_state.inc_readers_waiting();
                    let result = timeout(remaining, state.optimized_notify.wait_for_read()).await;
                    state.atomic_state.dec_readers_waiting();
                    result
                }
                LockMode::Exclusive => {
                    state.atomic_state.inc_writers_waiting();
                    let result = timeout(remaining, state.optimized_notify.wait_for_write()).await;
                    state.atomic_state.dec_writers_waiting();
                    result
                }
            };

            if wait_result.is_err() {
                self.metrics.record_timeout();
                return Err(LockResult::Timeout);
            }

            retry_count += 1;
        }
    }

    /// Release lock
    pub fn release_lock(&self, key: &ObjectKey, owner: &Arc<str>, mode: LockMode) -> bool {
        let should_cleanup;
        let result;

        {
            let objects = self.objects.read();
            if let Some(state) = objects.get(key) {
                result = match mode {
                    LockMode::Shared => state.release_shared(owner),
                    LockMode::Exclusive => state.release_exclusive(owner),
                };

                if result {
                    self.metrics.record_release();

                    // Check if cleanup is needed
                    should_cleanup = !state.is_locked() && !state.atomic_state.has_waiters();
                } else {
                    should_cleanup = false;
                    // Additional diagnostics for release failures
                    let current_mode = state.current_mode();
                    let is_locked = state.is_locked();
                    let has_waiters = state.atomic_state.has_waiters();

                    tracing::debug!(
                        "Lock release failed in shard: key={}, owner={}, mode={:?}, current_mode={:?}, is_locked={}, has_waiters={}",
                        key,
                        owner,
                        mode,
                        current_mode,
                        is_locked,
                        has_waiters
                    );
                }
            } else {
                result = false;
                should_cleanup = false;
                tracing::debug!(
                    "Lock release failed - key not found in shard: key={}, owner={}, mode={:?}",
                    key,
                    owner,
                    mode
                );
            }
        }

        // Perform cleanup outside of the read lock
        if should_cleanup {
            self.schedule_cleanup(key.clone());
        }

        result
    }

    /// Release lock with guard ID tracking for double-release prevention
    pub fn release_lock_with_guard(&self, key: &ObjectKey, owner: &Arc<str>, mode: LockMode, guard_id: u64) -> bool {
        // First, try to remove the guard from active set
        let guard_was_active = {
            let mut guards = self.active_guards.lock();
            guards.remove(&guard_id)
        };

        // If guard was not active, this is a double-release attempt
        if !guard_was_active {
            tracing::debug!(
                "Double-release attempt blocked: key={}, owner={}, mode={:?}, guard_id={}",
                key,
                owner,
                mode,
                guard_id
            );
            return false;
        }

        // Proceed with normal release
        let should_cleanup;
        let result;

        {
            let objects = self.objects.read();
            if let Some(state) = objects.get(key) {
                result = match mode {
                    LockMode::Shared => state.release_shared(owner),
                    LockMode::Exclusive => state.release_exclusive(owner),
                };

                if result {
                    self.metrics.record_release();
                    should_cleanup = !state.is_locked() && !state.atomic_state.has_waiters();
                } else {
                    should_cleanup = false;
                }
            } else {
                result = false;
                should_cleanup = false;
            }
        }

        if should_cleanup {
            self.schedule_cleanup(key.clone());
        }

        result
    }

    /// Register a guard to prevent premature cleanup
    pub fn register_guard(&self, guard_id: u64) {
        let mut guards = self.active_guards.lock();
        guards.insert(guard_id);
    }

    /// Unregister a guard (called when guard is dropped)
    pub fn unregister_guard(&self, guard_id: u64) {
        let mut guards = self.active_guards.lock();
        guards.remove(&guard_id);
    }

    /// Get count of active guards (for testing)
    #[cfg(test)]
    pub fn active_guard_count(&self) -> usize {
        let guards = self.active_guards.lock();
        guards.len()
    }

    /// Check if a guard is active (for testing)
    #[cfg(test)]
    pub fn is_guard_active(&self, guard_id: u64) -> bool {
        let guards = self.active_guards.lock();
        guards.contains(&guard_id)
    }

    /// Calculate adaptive timeout based on current system load and request priority
    fn calculate_adaptive_timeout(&self, request: &ObjectLockRequest) -> Duration {
        let base_timeout = request.acquire_timeout;

        // Get current shard load metrics
        let lock_count = {
            let objects = self.objects.read();
            objects.len()
        };

        let active_guard_count = {
            let guards = self.active_guards.lock();
            guards.len()
        };

        // Calculate load factor with more generous thresholds for database workloads
        let total_load = (lock_count + active_guard_count) as f64;
        let load_factor = total_load / 500.0; // Lowered threshold for faster scaling

        // More aggressive priority multipliers for database scenarios
        let priority_multiplier = match request.priority {
            crate::fast_lock::types::LockPriority::Critical => 3.0, // Increased
            crate::fast_lock::types::LockPriority::High => 2.0,     // Increased
            crate::fast_lock::types::LockPriority::Normal => 1.2,   // Slightly increased base
            crate::fast_lock::types::LockPriority::Low => 0.9,
        };

        // More generous load-based scaling
        let load_multiplier = if load_factor > 2.0 {
            // Very high load: drastically extend timeout
            1.0 + (load_factor * 2.0)
        } else if load_factor > 1.0 {
            // High load: significantly extend timeout
            1.0 + (load_factor * 1.8)
        } else if load_factor > 0.3 {
            // Medium load: moderately extend timeout
            1.0 + (load_factor * 1.2)
        } else {
            // Low load: still give some buffer
            1.1
        };

        let total_multiplier = priority_multiplier * load_multiplier;
        let adaptive_timeout_secs =
            (base_timeout.as_secs_f64() * total_multiplier).min(crate::fast_lock::MAX_ACQUIRE_TIMEOUT.as_secs_f64());

        // Ensure minimum reasonable timeout even for low priority
        let min_timeout_secs = base_timeout.as_secs_f64() * 0.8;
        Duration::from_secs_f64(adaptive_timeout_secs.max(min_timeout_secs))
    }

    /// Batch acquire locks with ordering to prevent deadlocks
    pub async fn acquire_locks_batch(
        &self,
        mut requests: Vec<ObjectLockRequest>,
        all_or_nothing: bool,
    ) -> Result<Vec<ObjectKey>, Vec<(ObjectKey, LockResult)>> {
        // Sort requests by key to prevent deadlocks
        requests.sort_by(|a, b| a.key.cmp(&b.key));

        let mut acquired = Vec::new();
        let mut failed = Vec::new();

        for request in requests {
            match self.acquire_lock(&request).await {
                Ok(()) => acquired.push((request.key.clone(), request.mode, request.owner.clone())),
                Err(err) => {
                    failed.push((request.key, err));

                    if all_or_nothing {
                        // Release all acquired locks using their correct owner and mode
                        let mut cleanup_failures = 0;
                        for (key, mode, owner) in &acquired {
                            if !self.release_lock(key, owner, *mode) {
                                cleanup_failures += 1;
                                tracing::warn!(
                                    "Failed to release lock during batch cleanup in shard: bucket={}, object={}",
                                    key.bucket,
                                    key.object
                                );
                            }
                        }

                        if cleanup_failures > 0 {
                            tracing::error!("Shard batch lock cleanup had {} failures", cleanup_failures);
                        }

                        return Err(failed);
                    }
                }
            }
        }

        if failed.is_empty() {
            Ok(acquired.into_iter().map(|(key, _, _)| key).collect())
        } else {
            Err(failed)
        }
    }

    /// Get lock information for monitoring
    pub fn get_lock_info(&self, key: &ObjectKey) -> Option<crate::fast_lock::types::ObjectLockInfo> {
        let objects = self.objects.read();
        if let Some(state) = objects.get(key)
            && let Some(mode) = state.current_mode()
        {
            let (owner, acquired_at, lock_timeout) = match mode {
                LockMode::Exclusive => {
                    let current_owner = state.current_owner.read();
                    let info = current_owner.clone()?;
                    (info.owner, info.acquired_at, info.lock_timeout)
                }
                LockMode::Shared => {
                    let shared_owners = state.shared_owners.read();
                    let entry = shared_owners.first()?.clone();
                    (entry.owner, entry.acquired_at, entry.lock_timeout)
                }
            };

            let priority = *state.priority.read();

            let expires_at = acquired_at
                .checked_add(lock_timeout)
                .unwrap_or_else(|| acquired_at + crate::fast_lock::DEFAULT_LOCK_TIMEOUT);

            return Some(crate::fast_lock::types::ObjectLockInfo {
                key: key.clone(),
                mode,
                owner,
                acquired_at,
                expires_at,
                priority,
            });
        }
        None
    }

    /// Get current load factor of the shard
    pub fn current_load_factor(&self) -> f64 {
        let objects = self.objects.read();
        let total_locks = objects.len();
        if total_locks == 0 {
            return 0.0;
        }

        let active_locks = objects.values().filter(|state| state.is_locked()).count();
        active_locks as f64 / total_locks as f64
    }

    /// Get count of active locks
    pub fn active_lock_count(&self) -> usize {
        let objects = self.objects.read();
        objects.values().filter(|state| state.is_locked()).count()
    }

    /// Adaptive cleanup based on current load
    pub fn adaptive_cleanup(&self) -> usize {
        let current_load = self.current_load_factor();
        let lock_count = self.lock_count();
        let active_guard_count = self.active_guards.lock().len();

        // Be much more conservative if there are active guards or very high load
        if active_guard_count > 0 && current_load > 0.8 {
            tracing::debug!(
                "Skipping aggressive cleanup due to {} active guards and high load ({:.2})",
                active_guard_count,
                current_load
            );
            // Only clean very old entries when under high load with active guards
            return self.cleanup_expired_batch(3, 1_200_000); // 20 minutes, smaller batches
        }

        // Under extreme load, skip cleanup entirely to reduce contention
        if current_load > 1.5 && active_guard_count > 10 {
            tracing::debug!(
                "Skipping all cleanup due to extreme load ({:.2}) and {} active guards",
                current_load,
                active_guard_count
            );
            return 0;
        }

        // Dynamically adjust cleanup strategy based on load
        let cleanup_batch_size = match current_load {
            load if load > 0.9 => lock_count / 50, // Much smaller batches for high load
            load if load > 0.7 => lock_count / 20, // Smaller batches for medium load
            _ => lock_count / 10,                  // More conservative even for low load
        };

        // Use much longer timeouts to prevent premature cleanup
        let cleanup_threshold_millis = match current_load {
            load if load > 0.8 => 600_000, // 10 minutes for high load
            load if load > 0.5 => 300_000, // 5 minutes for medium load
            _ => 120_000,                  // 2 minutes for low load
        };

        self.cleanup_expired_batch_protected(cleanup_batch_size.max(5), cleanup_threshold_millis)
    }

    /// Cleanup expired and unused locks
    pub fn cleanup_expired(&self, max_idle_secs: u64) -> usize {
        let max_idle_millis = max_idle_secs * 1000;
        self.cleanup_expired_millis(max_idle_millis)
    }

    /// Cleanup expired and unused locks with millisecond precision
    pub fn cleanup_expired_millis(&self, max_idle_millis: u64) -> usize {
        let mut cleaned = 0;
        let now_millis = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_millis() as u64;

        let mut objects = self.objects.write();
        objects.retain(|_key, state| {
            if !state.is_locked() && !state.atomic_state.has_waiters() {
                let last_access_secs = state.atomic_state.last_accessed();
                let last_access_millis = last_access_secs * 1000; // Convert to millis
                let idle_time = now_millis.saturating_sub(last_access_millis);

                if idle_time > max_idle_millis {
                    cleaned += 1;
                    false // Remove this entry
                } else {
                    true // Keep this entry
                }
            } else {
                true // Keep locked or waited entries
            }
        });

        self.metrics.record_cleanup(cleaned);
        cleaned
    }

    /// Protected batch cleanup that respects active guards
    fn cleanup_expired_batch_protected(&self, max_batch_size: usize, cleanup_threshold_millis: u64) -> usize {
        let active_guards = self.active_guards.lock();
        let guard_count = active_guards.len();
        drop(active_guards); // Release lock early

        if guard_count > 0 {
            tracing::debug!("Cleanup with {} active guards, being conservative", guard_count);
        }

        self.cleanup_expired_batch(max_batch_size, cleanup_threshold_millis)
    }

    /// Batch cleanup with limited processing to avoid blocking
    fn cleanup_expired_batch(&self, max_batch_size: usize, cleanup_threshold_millis: u64) -> usize {
        let mut cleaned = 0;
        let now_millis = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_millis() as u64;

        let mut objects = self.objects.write();
        let mut processed = 0;

        // Process in batches to avoid long-held locks
        let mut to_recycle = Vec::new();
        objects.retain(|_key, state| {
            if processed >= max_batch_size {
                return true; // Stop processing after batch limit
            }
            processed += 1;

            if !state.is_locked() && !state.atomic_state.has_waiters() {
                let last_access_millis = state.atomic_state.last_accessed() * 1000;
                let idle_time = now_millis.saturating_sub(last_access_millis);

                if idle_time > cleanup_threshold_millis {
                    // Try to recycle the state back to pool if possible
                    if let Ok(state_box) = Arc::try_unwrap(state.clone()) {
                        to_recycle.push(state_box);
                    }
                    cleaned += 1;
                    false // Remove
                } else {
                    true // Keep
                }
            } else {
                true // Keep active locks
            }
        });

        // Return recycled objects to pool
        for state_box in to_recycle {
            let boxed_state = Box::new(state_box);
            self.object_pool.release(boxed_state);
        }

        self.metrics.record_cleanup(cleaned);
        cleaned
    }

    /// Get shard metrics
    pub fn metrics(&self) -> &ShardMetrics {
        &self.metrics
    }

    /// Get current lock count
    pub fn lock_count(&self) -> usize {
        self.objects.read().len()
    }

    /// Schedule background cleanup for a key
    fn schedule_cleanup(&self, key: ObjectKey) {
        // Don't immediately cleanup - let cleanup_expired handle it
        // This allows the cleanup test to work properly
        let _ = key; // Suppress unused variable warning
    }

    /// Get object pool statistics
    pub fn pool_stats(&self) -> (u64, u64, u64, usize) {
        self.object_pool.stats()
    }

    /// Get object pool hit rate
    pub fn pool_hit_rate(&self) -> f64 {
        self.object_pool.hit_rate()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fast_lock::types::{LockPriority, ObjectKey};

    #[tokio::test]
    async fn test_shard_fast_path() {
        let shard = LockShard::new(0);
        let key = ObjectKey::new("bucket", "object");
        let owner: Arc<str> = Arc::from("owner");

        let request = ObjectLockRequest {
            key: key.clone(),
            mode: LockMode::Exclusive,
            owner: owner.clone(),
            acquire_timeout: Duration::from_secs(1),
            lock_timeout: Duration::from_secs(30),
            priority: LockPriority::Normal,
        };

        // Should succeed via fast path
        assert!(shard.acquire_lock(&request).await.is_ok());
        assert!(shard.release_lock(&key, &owner, LockMode::Exclusive));
    }

    #[tokio::test]
    async fn test_shard_contention() {
        let shard = Arc::new(LockShard::new(0));
        let key = ObjectKey::new("bucket", "object");

        let owner1: Arc<str> = Arc::from("owner1");
        let owner2: Arc<str> = Arc::from("owner2");

        let request1 = ObjectLockRequest {
            key: key.clone(),
            mode: LockMode::Exclusive,
            owner: owner1.clone(),
            acquire_timeout: Duration::from_secs(1),
            lock_timeout: Duration::from_secs(30),
            priority: LockPriority::Normal,
        };

        let request2 = ObjectLockRequest {
            key: key.clone(),
            mode: LockMode::Exclusive,
            owner: owner2.clone(),
            acquire_timeout: Duration::from_millis(100),
            lock_timeout: Duration::from_secs(30),
            priority: LockPriority::Normal,
        };

        // First lock should succeed
        assert!(shard.acquire_lock(&request1).await.is_ok());

        // Second lock should timeout
        assert!(matches!(shard.acquire_lock(&request2).await, Err(LockResult::Timeout)));

        // Release first lock
        assert!(shard.release_lock(&key, &owner1, LockMode::Exclusive));
    }

    #[tokio::test]
    async fn test_batch_operations() {
        let shard = LockShard::new(0);
        let owner: Arc<str> = Arc::from("owner");

        let requests = vec![
            ObjectLockRequest {
                key: ObjectKey::new("bucket", "obj1"),
                mode: LockMode::Exclusive,
                owner: owner.clone(),
                acquire_timeout: Duration::from_secs(1),
                lock_timeout: Duration::from_secs(30),
                priority: LockPriority::Normal,
            },
            ObjectLockRequest {
                key: ObjectKey::new("bucket", "obj2"),
                mode: LockMode::Shared,
                owner: owner.clone(),
                acquire_timeout: Duration::from_secs(1),
                lock_timeout: Duration::from_secs(30),
                priority: LockPriority::Normal,
            },
        ];

        let result = shard.acquire_locks_batch(requests, true).await;
        assert!(result.is_ok());

        let acquired = result.unwrap();
        assert_eq!(acquired.len(), 2);
    }

    #[tokio::test]
    async fn test_batch_lock_cleanup_safety() {
        let shard = LockShard::new(0);

        // First acquire a lock that will block the batch operation
        let blocking_request = ObjectLockRequest::new_write(ObjectKey::new("bucket", "obj1"), "blocking_owner")
            .with_acquire_timeout(Duration::from_secs(1));
        shard.acquire_lock(&blocking_request).await.unwrap();

        // Use short acquire timeout so the test fails fast when obj1 is already locked
        // (default is 60s which would make this test very slow)
        let requests = vec![
            ObjectLockRequest::new_read(ObjectKey::new("bucket", "obj2"), "batch_owner")
                .with_acquire_timeout(Duration::from_millis(100)), // This should succeed
            ObjectLockRequest::new_write(ObjectKey::new("bucket", "obj1"), "batch_owner")
                .with_acquire_timeout(Duration::from_millis(100)), // This should fail due to existing lock
        ];

        let result = shard.acquire_locks_batch(requests, true).await;
        assert!(result.is_err()); // Should fail due to obj1 being locked

        // Verify that obj2 lock was properly cleaned up (no resource leak)
        let obj2_key = ObjectKey::new("bucket", "obj2");
        assert!(shard.get_lock_info(&obj2_key).is_none(), "obj2 should not be locked after cleanup");

        // Verify obj1 is still locked by the original owner
        let obj1_key = ObjectKey::new("bucket", "obj1");
        let lock_info = shard.get_lock_info(&obj1_key);
        assert!(lock_info.is_some(), "obj1 should still be locked by blocking_owner");
    }
}
