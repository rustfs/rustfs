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

use crate::fast_lock::{
    shard::LockShard,
    types::{LockMode, ObjectKey},
};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// Global counter for guard IDs to prevent double-release
static GUARD_ID_COUNTER: AtomicU64 = AtomicU64::new(1);

/// RAII guard for fast object locks
///
/// Automatically releases the lock when dropped, ensuring no lock leakage
/// even in panic scenarios.
pub struct FastLockGuard {
    key: ObjectKey,
    mode: LockMode,
    owner: Arc<str>,
    shard: Option<Arc<LockShard>>, // None when locks are disabled
    released: bool,
    disabled: bool, // True when locks are disabled globally
    /// Unique ID for this guard instance to prevent double-release
    guard_id: u64,
}

impl FastLockGuard {
    pub(crate) fn new(key: ObjectKey, mode: LockMode, owner: Arc<str>, shard: Arc<LockShard>) -> Self {
        let guard_id = GUARD_ID_COUNTER.fetch_add(1, Ordering::Relaxed);
        Self {
            key,
            mode,
            owner,
            shard: Some(shard),
            released: false,
            disabled: false,
            guard_id,
        }
    }

    /// Create a disabled guard (when locks are globally disabled)
    pub(crate) fn new_disabled(key: ObjectKey, mode: LockMode, owner: Arc<str>) -> Self {
        let guard_id = GUARD_ID_COUNTER.fetch_add(1, Ordering::Relaxed);
        Self {
            key,
            mode,
            owner,
            shard: None,
            released: false,
            disabled: true,
            guard_id,
        }
    }

    /// Get the object key this guard protects
    pub fn key(&self) -> &ObjectKey {
        &self.key
    }

    /// Get the lock mode (Shared or Exclusive)
    pub fn mode(&self) -> LockMode {
        self.mode
    }

    /// Get the lock owner
    pub fn owner(&self) -> &Arc<str> {
        &self.owner
    }

    /// Manually release the lock early
    ///
    /// Returns true if the lock was successfully released, false if it was
    /// already released or the release failed.
    pub fn release(&mut self) -> bool {
        if self.released {
            return false;
        }

        if self.disabled {
            // For disabled locks, always succeed
            self.released = true;
            if let Some(shard) = &self.shard {
                shard.unregister_guard(self.guard_id);
            }
            return true;
        }

        if let Some(shard) = &self.shard {
            let success = shard.release_lock_with_guard(&self.key, &self.owner, self.mode, self.guard_id);
            if success {
                self.released = true;
                // Unregister the guard after successful release
                shard.unregister_guard(self.guard_id);
            }
            success
        } else {
            // Should not happen, but handle gracefully
            self.released = true;
            false
        }
    }

    /// Check if the lock has been released
    pub fn is_released(&self) -> bool {
        self.released
    }

    /// Check if this guard represents a disabled lock
    pub fn is_disabled(&self) -> bool {
        self.disabled
    }

    /// Get the unique guard ID
    pub fn guard_id(&self) -> u64 {
        self.guard_id
    }

    /// Get lock information for monitoring
    pub fn lock_info(&self) -> Option<crate::fast_lock::types::ObjectLockInfo> {
        if self.released || self.disabled {
            None
        } else if let Some(shard) = &self.shard {
            shard.get_lock_info(&self.key)
        } else {
            None
        }
    }
}

impl Drop for FastLockGuard {
    fn drop(&mut self) {
        if let Some(shard) = &self.shard {
            if !self.released && !self.disabled {
                let success = shard.release_lock_with_guard(&self.key, &self.owner, self.mode, self.guard_id);
                if !success {
                    // For high-concurrency scenarios, this is likely due to:
                    // 1. Lock was already released by another thread
                    // 2. Lock was cleaned up by background cleanup
                    // 3. Legitimate double-drop scenario
                    tracing::debug!(
                        "Guard release failed (likely already released): key={}, owner={}, mode={:?}, guard_id={}",
                        self.key,
                        self.owner,
                        self.mode,
                        self.guard_id
                    );
                }
                // Always unregister the guard to prevent leaks, regardless of release success
                shard.unregister_guard(self.guard_id);
            } else {
                // If guard was already released or disabled, just unregister it
                shard.unregister_guard(self.guard_id);
            }
        }
    }
}

impl std::fmt::Debug for FastLockGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FastLockGuard")
            .field("key", &self.key)
            .field("mode", &self.mode)
            .field("owner", &self.owner)
            .field("released", &self.released)
            .field("disabled", &self.disabled)
            .field("guard_id", &self.guard_id)
            .finish()
    }
}

/// Multiple lock guards that can be released atomically
///
/// Useful for batch operations where you want to ensure all locks
/// are held until a critical section is complete.
#[derive(Debug)]
pub struct MultipleLockGuards {
    guards: Vec<FastLockGuard>,
}

impl MultipleLockGuards {
    /// Create new multiple guards container
    pub fn new() -> Self {
        Self { guards: Vec::new() }
    }

    /// Add a guard to the collection
    pub fn add(&mut self, guard: FastLockGuard) {
        self.guards.push(guard);
    }

    /// Get number of guards
    pub fn len(&self) -> usize {
        self.guards.len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.guards.is_empty()
    }

    /// Get iterator over guards
    pub fn iter(&self) -> std::slice::Iter<'_, FastLockGuard> {
        self.guards.iter()
    }

    /// Get mutable iterator over guards
    pub fn iter_mut(&mut self) -> std::slice::IterMut<'_, FastLockGuard> {
        self.guards.iter_mut()
    }

    /// Release all locks manually
    ///
    /// Returns the number of locks successfully released.
    pub fn release_all(&mut self) -> usize {
        let mut released_count = 0;
        for guard in &mut self.guards {
            if guard.release() {
                released_count += 1;
            }
        }
        released_count
    }

    /// Check how many locks are still held
    pub fn active_count(&self) -> usize {
        self.guards.iter().filter(|guard| !guard.is_released()).count()
    }

    /// Get all object keys
    pub fn keys(&self) -> Vec<&ObjectKey> {
        self.guards.iter().map(|guard| guard.key()).collect()
    }

    /// Split guards by lock mode (consumes the original guards)
    pub fn split_by_mode(mut self) -> (Vec<FastLockGuard>, Vec<FastLockGuard>) {
        let mut shared_guards = Vec::new();
        let mut exclusive_guards = Vec::new();

        for guard in self.guards.drain(..) {
            match guard.mode() {
                LockMode::Shared => shared_guards.push(guard),
                LockMode::Exclusive => exclusive_guards.push(guard),
            }
        }

        (shared_guards, exclusive_guards)
    }

    /// Split guards by lock mode without consuming (returns references)
    pub fn split_by_mode_ref(&self) -> (Vec<&FastLockGuard>, Vec<&FastLockGuard>) {
        let mut shared_guards = Vec::new();
        let mut exclusive_guards = Vec::new();

        for guard in &self.guards {
            match guard.mode() {
                LockMode::Shared => shared_guards.push(guard),
                LockMode::Exclusive => exclusive_guards.push(guard),
            }
        }

        (shared_guards, exclusive_guards)
    }

    /// Merge multiple guard collections into this one
    pub fn merge(&mut self, mut other: MultipleLockGuards) {
        self.guards.append(&mut other.guards);
    }

    /// Merge multiple individual guards into this collection
    pub fn merge_guards(&mut self, guards: Vec<FastLockGuard>) {
        self.guards.extend(guards);
    }

    /// Filter guards by predicate (non-consuming)
    pub fn filter<F>(&self, predicate: F) -> Vec<&FastLockGuard>
    where
        F: Fn(&FastLockGuard) -> bool,
    {
        self.guards.iter().filter(|guard| predicate(guard)).collect()
    }

    /// Filter guards by predicate (consuming)
    pub fn filter_owned<F>(self, predicate: F) -> Vec<FastLockGuard>
    where
        F: Fn(&FastLockGuard) -> bool,
    {
        // Use a safe approach that avoids Drop interaction issues
        self.into_iter().filter(|guard| predicate(guard)).collect()
    }

    /// Get guards for specific bucket
    pub fn guards_for_bucket(&self, bucket: &str) -> Vec<&FastLockGuard> {
        self.filter(|guard| guard.key().bucket.as_ref() == bucket)
    }

    /// Get guards for specific owner
    pub fn guards_for_owner(&self, owner: &str) -> Vec<&FastLockGuard> {
        self.filter(|guard| guard.owner().as_ref() == owner)
    }
}

impl Default for MultipleLockGuards {
    fn default() -> Self {
        Self::new()
    }
}

impl From<Vec<FastLockGuard>> for MultipleLockGuards {
    fn from(guards: Vec<FastLockGuard>) -> Self {
        Self { guards }
    }
}

impl From<FastLockGuard> for MultipleLockGuards {
    fn from(guard: FastLockGuard) -> Self {
        Self { guards: vec![guard] }
    }
}

impl IntoIterator for MultipleLockGuards {
    type Item = FastLockGuard;
    type IntoIter = std::vec::IntoIter<FastLockGuard>;

    fn into_iter(mut self) -> Self::IntoIter {
        // Use mem::replace to avoid Drop interaction issues
        // This approach is safer than mem::take as it prevents the Drop from seeing empty state
        let guards = std::mem::take(&mut self.guards);
        std::mem::forget(self); // Prevent Drop from running on emptied state
        guards.into_iter()
    }
}

impl<'a> IntoIterator for &'a MultipleLockGuards {
    type Item = &'a FastLockGuard;
    type IntoIter = std::slice::Iter<'a, FastLockGuard>;

    fn into_iter(self) -> Self::IntoIter {
        self.guards.iter()
    }
}

impl<'a> IntoIterator for &'a mut MultipleLockGuards {
    type Item = &'a mut FastLockGuard;
    type IntoIter = std::slice::IterMut<'a, FastLockGuard>;

    fn into_iter(self) -> Self::IntoIter {
        self.guards.iter_mut()
    }
}

impl Drop for MultipleLockGuards {
    fn drop(&mut self) {
        // Guards will be dropped individually, each releasing their lock
        let active_count = self.active_count();
        if active_count > 0 {
            tracing::debug!("Dropping MultipleLockGuards with {} active locks", active_count);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fast_lock::{manager::FastObjectLockManager, types::ObjectKey};
    use std::collections::HashSet;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[tokio::test]
    async fn test_guard_basic_operations() {
        let manager = FastObjectLockManager::new();

        let mut guard = manager
            .acquire_write_lock("bucket", "object", "owner")
            .await
            .expect("Failed to acquire lock");

        assert!(!guard.is_released());
        assert_eq!(guard.mode(), LockMode::Exclusive);
        assert_eq!(guard.key().bucket.as_ref(), "bucket");
        assert_eq!(guard.key().object.as_ref(), "object");

        // Manual release
        assert!(guard.release());
        assert!(guard.is_released());

        // Second release should fail
        assert!(!guard.release());
    }

    #[tokio::test]
    async fn test_guard_id_uniqueness() {
        let manager = FastObjectLockManager::new();
        let mut guard_ids = HashSet::new();

        // Acquire multiple guards and verify unique IDs
        let mut guards = Vec::new();
        for i in 0..100 {
            let object_name = format!("object_{i}");
            let guard = manager
                .acquire_write_lock("bucket", object_name.as_str(), "owner")
                .await
                .expect("Failed to acquire lock");

            let guard_id = guard.guard_id();
            assert!(guard_ids.insert(guard_id), "Guard ID {guard_id} is not unique");
            guards.push(guard);
        }

        assert_eq!(guard_ids.len(), 100, "Expected 100 unique guard IDs");
    }

    #[tokio::test]
    async fn test_guard_double_release_protection() {
        let manager = FastObjectLockManager::new();

        // Acquire a real lock
        let mut guard = manager
            .acquire_write_lock("bucket", "object", "owner")
            .await
            .expect("Failed to acquire lock");

        let guard_id = guard.guard_id();
        let key = guard.key().clone();
        let owner = guard.owner().clone();
        let mode = guard.mode();
        let shard = manager.get_shard(&key);

        // First manual release should succeed
        assert!(guard.release(), "First release should succeed");

        // Second manual release should fail
        assert!(!guard.release(), "Second release should fail");

        // Direct shard release with guard_id should also fail (guard already unregistered)
        let direct_release = shard.release_lock_with_guard(&key, &owner, mode, guard_id);
        assert!(!direct_release, "Direct release after manual release should fail");
    }

    #[tokio::test]
    async fn test_guard_lifecycle_registration() {
        let manager = FastObjectLockManager::new();
        let key = ObjectKey::new("bucket", "object");
        let shard = manager.get_shard(&key);

        // Initially no active guards
        assert_eq!(shard.active_guard_count(), 0);

        // Acquire lock - should register guard
        let guard = manager
            .acquire_write_lock("bucket", "object", "owner")
            .await
            .expect("Failed to acquire lock");

        let guard_id = guard.guard_id();

        // Verify guard is registered
        assert!(shard.is_guard_active(guard_id), "Guard should be registered");

        // Drop guard - should unregister
        drop(guard);

        // Give a moment for cleanup
        tokio::task::yield_now().await;

        // Try to acquire the same lock again - should succeed
        let guard2 = manager
            .acquire_write_lock("bucket", "object", "owner2")
            .await
            .expect("Should be able to acquire lock again after previous guard dropped");

        assert_ne!(guard_id, guard2.guard_id(), "New guard should have different ID");
        drop(guard2);
    }

    #[tokio::test]
    async fn test_concurrent_guard_stress() {
        let manager = Arc::new(FastObjectLockManager::new());
        let success_count = Arc::new(AtomicUsize::new(0));
        let double_release_blocked = Arc::new(AtomicUsize::new(0));

        // Spawn concurrent tasks
        let mut handles = Vec::new();
        for task_id in 0..20 {
            let manager = manager.clone();
            let success_count = success_count.clone();
            let double_release_blocked = double_release_blocked.clone();

            let handle = tokio::spawn(async move {
                for i in 0..10 {
                    let object_name = format!("obj_{task_id}_{i}");

                    // Acquire lock
                    let mut guard = match manager.acquire_write_lock("bucket", object_name.as_str(), "owner").await {
                        Ok(g) => g,
                        Err(_) => continue,
                    };

                    let guard_id = guard.guard_id();
                    let key = guard.key().clone();
                    let owner = guard.owner().clone();
                    let mode = guard.mode();
                    let shard = manager.get_shard(&key);

                    // Manual release
                    if guard.release() {
                        success_count.fetch_add(1, Ordering::SeqCst);
                    }

                    // Try to release again directly - should be blocked
                    if !shard.release_lock_with_guard(&key, &owner, mode, guard_id) {
                        double_release_blocked.fetch_add(1, Ordering::SeqCst);
                    }
                }
            });
            handles.push(handle);
        }

        futures::future::join_all(handles).await;

        let successes = success_count.load(Ordering::SeqCst);
        let blocked = double_release_blocked.load(Ordering::SeqCst);

        // Should have many successful releases and all double releases blocked
        assert!(successes > 150, "Expected many successful releases, got {successes}");
        assert_eq!(blocked, successes, "All double releases should be blocked");

        // Verify no active guards remain
        for shard in &manager.shards {
            assert_eq!(shard.active_guard_count(), 0, "No guards should remain active");
        }
    }

    #[tokio::test]
    async fn test_guard_with_different_owners() {
        let manager = FastObjectLockManager::new();

        // Test that guards with different owners for shared locks work correctly
        let mut guard1 = manager.acquire_read_lock("bucket", "object", "owner1").await.unwrap();
        let mut guard2 = manager.acquire_read_lock("bucket", "object", "owner2").await.unwrap();

        assert_ne!(guard1.guard_id(), guard2.guard_id());
        assert_eq!(guard1.owner().as_ref(), "owner1");
        assert_eq!(guard2.owner().as_ref(), "owner2");

        assert!(guard1.release());
        assert!(guard2.release());
    }

    #[tokio::test]
    async fn test_guard_cleanup_protection() {
        let manager = Arc::new(FastObjectLockManager::new());

        // Acquire multiple locks for the same object to ensure they're in the same shard
        let mut guards = Vec::new();
        for i in 0..10 {
            let owner_name = format!("owner_{i}");
            let guard = manager
                .acquire_read_lock("bucket", "shared_object", owner_name.as_str())
                .await
                .expect("Failed to acquire lock");
            guards.push(guard);
        }

        let key = ObjectKey::new("bucket", "shared_object");
        let shard = manager.get_shard(&key);

        // Verify guards are registered (all for the same object/shard)
        assert_eq!(shard.active_guard_count(), 10, "All guards should be registered in the same shard");

        // Try cleanup - should be conservative due to active guards
        let initial_lock_count = shard.lock_count();
        let cleaned = shard.adaptive_cleanup();

        // Should clean very little due to active guards
        assert!(cleaned <= 5, "Should be conservative with active guards, cleaned: {cleaned}");

        // Locks should be protected by active guards
        let remaining_locks = shard.lock_count();
        assert_eq!(remaining_locks, initial_lock_count, "Locks should be protected by active guards");

        // Release half the guards
        for _ in 0..5 {
            let mut guard = guards.pop().unwrap();
            assert!(guard.release());
        }

        // Verify remaining guards are still active
        assert_eq!(shard.active_guard_count(), 5, "Half the guards should remain active");
    }

    #[tokio::test]
    async fn test_guard_auto_release() {
        let manager = FastObjectLockManager::new();
        let key = ObjectKey::new("bucket", "object");

        // Acquire lock in a scope
        {
            let _guard = manager
                .acquire_write_lock("bucket", "object", "owner")
                .await
                .expect("Failed to acquire lock");

            // Lock should be held here
            assert!(manager.get_lock_info(&key).is_some());
        } // Guard dropped here, lock should be released

        // Give a moment for cleanup
        tokio::task::yield_now().await;

        // Should be able to acquire the lock again immediately
        let _guard2 = manager
            .acquire_write_lock("bucket", "object", "owner2")
            .await
            .expect("Failed to re-acquire lock after auto-release");
    }

    #[tokio::test]
    async fn test_multiple_guards() {
        let manager = FastObjectLockManager::new();
        let mut multiple = MultipleLockGuards::new();

        // Acquire multiple locks
        let guard1 = manager.acquire_read_lock("bucket", "obj1", "owner").await.unwrap();
        let guard2 = manager.acquire_read_lock("bucket", "obj2", "owner").await.unwrap();
        let guard3 = manager.acquire_write_lock("bucket", "obj3", "owner").await.unwrap();

        multiple.add(guard1);
        multiple.add(guard2);
        multiple.add(guard3);

        assert_eq!(multiple.len(), 3);
        assert_eq!(multiple.active_count(), 3);

        // Test split by mode without consuming
        let (shared_refs, exclusive_refs) = multiple.split_by_mode_ref();
        assert_eq!(shared_refs.len(), 2);
        assert_eq!(exclusive_refs.len(), 1);

        // Original should still have all guards
        assert_eq!(multiple.len(), 3);

        // Split by mode (consuming)
        let (shared, exclusive) = multiple.split_by_mode();
        assert_eq!(shared.len(), 2);
        assert_eq!(exclusive.len(), 1);

        // Test merge functionality
        let mut new_multiple = MultipleLockGuards::new();
        new_multiple.merge_guards(shared);
        new_multiple.merge_guards(exclusive);
        assert_eq!(new_multiple.len(), 3);
    }

    #[tokio::test]
    async fn test_into_iter_safety() {
        let manager = FastObjectLockManager::new();
        let mut multiple = MultipleLockGuards::new();

        // Acquire some locks
        let guard1 = manager.acquire_read_lock("bucket", "obj1", "owner").await.unwrap();
        let guard2 = manager.acquire_read_lock("bucket", "obj2", "owner").await.unwrap();

        multiple.add(guard1);
        multiple.add(guard2);

        assert_eq!(multiple.len(), 2);

        // Test into_iter consumption
        let guards: Vec<_> = multiple.into_iter().collect();
        assert_eq!(guards.len(), 2);

        // multiple is consumed here, so we can't access it anymore
        // This ensures Drop is handled correctly without double-drop issues
    }

    #[tokio::test]
    async fn test_guard_panic_safety() {
        let manager = Arc::new(FastObjectLockManager::new());
        let _key = ObjectKey::new("bucket", "object");

        // Test that locks are released even if task panics
        let manager_clone = manager.clone();
        let handle = tokio::spawn(async move {
            let _guard = manager_clone
                .acquire_write_lock("bucket", "object", "owner")
                .await
                .expect("Failed to acquire lock");

            // Simulate panic
            panic!("Simulated panic");
        });

        // Wait for panic
        let _ = handle.await;

        // Should be able to acquire lock again
        let _guard = manager
            .acquire_write_lock("bucket", "object", "owner2")
            .await
            .expect("Failed to acquire lock after panic");
    }

    #[tokio::test]
    async fn test_guard_registration_cleanup() {
        let manager = crate::fast_lock::FastObjectLockManager::new();

        // Test guard registration cleanup after drop
        {
            let mut guard = manager
                .acquire_write_lock("bucket", "object", "owner")
                .await
                .expect("Failed to acquire lock");

            let shard = manager.get_shard(&guard.key);
            assert_eq!(shard.active_guard_count(), 1);
            assert!(shard.is_guard_active(guard.guard_id()));

            // Manual release should clean up registration
            assert!(guard.release());
            assert_eq!(shard.active_guard_count(), 0);
            assert!(!shard.is_guard_active(guard.guard_id()));
        } // Drop here should not cause issues

        // Test guard registration cleanup after drop without manual release
        {
            let guard = manager
                .acquire_write_lock("bucket", "object2", "owner")
                .await
                .expect("Failed to acquire lock");

            let shard = manager.get_shard(&guard.key);
            assert_eq!(shard.active_guard_count(), 1);
            assert!(shard.is_guard_active(guard.guard_id()));

            // Don't manually release - let drop handle it
        } // Drop should clean up registration automatically

        // Verify cleanup happened
        let key = crate::fast_lock::types::ObjectKey::new("bucket", "object2");
        let shard = manager.get_shard(&key);
        assert_eq!(shard.active_guard_count(), 0);

        // Should be able to acquire new lock after cleanup
        let _new_guard = manager
            .acquire_write_lock("bucket", "object2", "owner2")
            .await
            .expect("Failed to acquire lock after cleanup");
    }

    #[tokio::test]
    async fn test_disabled_guard_cleanup() {
        let manager = crate::fast_lock::FastObjectLockManager::new();

        // Test disabled guard cleanup
        let disabled_guard = FastLockGuard::new_disabled(
            crate::fast_lock::types::ObjectKey::new("bucket", "object"),
            crate::fast_lock::types::LockMode::Exclusive,
            "owner".into(),
        );

        // Disabled guards don't register with shards initially
        let shard = manager.get_shard(&disabled_guard.key);
        assert_eq!(shard.active_guard_count(), 0);

        // But if they had a shard reference and were registered,
        // release should still clean them up properly
        let mut disabled_guard_with_shard = FastLockGuard {
            key: crate::fast_lock::types::ObjectKey::new("bucket", "object"),
            mode: crate::fast_lock::types::LockMode::Exclusive,
            owner: "owner".into(),
            shard: Some(shard.clone()),
            released: false,
            disabled: true,
            guard_id: crate::fast_lock::guard::GUARD_ID_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
        };

        // Manually register this disabled guard to test cleanup
        shard.register_guard(disabled_guard_with_shard.guard_id());
        assert_eq!(shard.active_guard_count(), 1);

        // Release should clean up even for disabled guards
        assert!(disabled_guard_with_shard.release());
        assert_eq!(shard.active_guard_count(), 0);
    }

    #[tokio::test]
    async fn test_high_priority_lock_performance() {
        let manager = crate::fast_lock::FastObjectLockManager::new();

        // Test high-priority lock acquisition under simulated load
        let mut handles = Vec::new();

        // Create background low-priority locks to simulate load
        for i in 0..50 {
            let manager_clone = manager.clone();
            let handle = tokio::spawn(async move {
                let _guard = manager_clone
                    .acquire_read_lock("bucket", format!("object-{i}"), "background")
                    .await;
                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            });
            handles.push(handle);
        }

        // Give background tasks time to start
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // High-priority request should complete faster
        let start = std::time::Instant::now();
        let _high_priority_guard = manager
            .acquire_high_priority_read_lock("bucket", "priority-object", "priority-owner")
            .await
            .expect("High priority lock should succeed");
        let high_priority_duration = start.elapsed();

        // Normal priority request for comparison
        let start = std::time::Instant::now();
        let _normal_guard = manager
            .acquire_read_lock("bucket", "normal-object", "normal-owner")
            .await
            .expect("Normal lock should succeed");
        let normal_duration = start.elapsed();

        // Clean up background tasks
        for handle in handles {
            let _ = handle.await;
        }

        // High priority should generally perform reasonably well
        // This is more of a performance validation than a strict requirement
        println!("High priority: {high_priority_duration:?}, Normal: {normal_duration:?}");

        // Both operations should complete in reasonable time (less than 100ms in test environment)
        // This validates that the priority system isn't causing severe degradation
        assert!(high_priority_duration < std::time::Duration::from_millis(100));
        assert!(normal_duration < std::time::Duration::from_millis(100));
    }

    #[tokio::test]
    async fn test_adaptive_timeout_under_load() {
        let manager = crate::fast_lock::FastObjectLockManager::new();

        // Create high load by acquiring many locks
        let mut _guards = Vec::new();
        for i in 0..100 {
            if let Ok(guard) = manager
                .acquire_write_lock("bucket", format!("load-object-{i}"), "loader")
                .await
            {
                _guards.push(guard);
            }
        }

        // Test that locks with different priorities get different effective timeouts
        let start = std::time::Instant::now();
        let critical_result = manager
            .acquire_critical_write_lock("bucket", "critical-object", "critical-owner")
            .await;
        let critical_duration = start.elapsed();

        let start = std::time::Instant::now();
        let normal_result = manager.acquire_write_lock("bucket", "normal-object", "normal-owner").await;
        let normal_duration = start.elapsed();

        // Both should eventually succeed or fail, but critical should have longer timeout
        println!(
            "Critical result: {:?} ({}ms), Normal result: {:?} ({}ms)",
            critical_result.is_ok(),
            critical_duration.as_millis(),
            normal_result.is_ok(),
            normal_duration.as_millis()
        );

        // At minimum, the system should handle the requests gracefully
        assert!(critical_duration < std::time::Duration::from_secs(65)); // Should not exceed max timeout
        assert!(normal_duration < std::time::Duration::from_secs(65)); // Should not exceed max timeout
    }

    #[tokio::test]
    async fn test_database_workload_simulation() {
        let manager = crate::fast_lock::FastObjectLockManager::new();

        // Simulate a complex database query like TPC-H that touches many objects
        let mut handles = Vec::new();
        let _total_objects = 200;
        let concurrent_queries = 20;

        // Each query touches multiple objects (simulating joins, aggregations, etc.)
        for query_id in 0..concurrent_queries {
            let manager_clone = manager.clone();
            let handle = tokio::spawn(async move {
                let mut query_locks = Vec::new();
                let objects_per_query = 10 + (query_id % 5); // 10-14 objects per query

                // Try to acquire all locks for this "query"
                for obj_id in 0..objects_per_query {
                    let bucket = "databend";
                    let object = format!("table_partition_{query_id}_{obj_id}");
                    let owner = format!("query_{query_id}");

                    match manager_clone.acquire_high_priority_read_lock(bucket, object, owner).await {
                        Ok(guard) => query_locks.push(guard),
                        Err(_) => {
                            // Query failed to acquire all needed locks
                            return false;
                        }
                    }
                }

                // Simulate query execution time
                tokio::time::sleep(tokio::time::Duration::from_millis(50 + query_id * 5)).await;

                // Locks will be released when guards are dropped
                true
            });
            handles.push(handle);
        }

        // Also add some background write operations (simulating inserts/updates)
        for write_id in 0..5 {
            let manager_clone = manager.clone();
            let handle = tokio::spawn(async move {
                let bucket = "databend";
                let object = format!("write_target_{write_id}");
                let owner = format!("writer_{write_id}");

                match manager_clone.acquire_write_lock(bucket, object, owner).await {
                    Ok(_guard) => {
                        // Simulate write operation
                        tokio::time::sleep(tokio::time::Duration::from_millis(30)).await;
                        true
                    }
                    Err(_) => false,
                }
            });
            handles.push(handle);
        }

        // Wait for all operations to complete
        let mut successful_operations = 0;
        for handle in handles {
            if let Ok(success) = handle.await
                && success
            {
                successful_operations += 1;
            }
        }

        // We expect most operations to succeed with the new timeouts and optimizations
        let total_operations = concurrent_queries + 5;
        let success_rate = successful_operations as f64 / total_operations as f64;

        println!(
            "Database workload simulation: {}/{} operations succeeded ({:.1}%)",
            successful_operations,
            total_operations,
            success_rate * 100.0
        );

        // With the new optimizations, we should achieve at least 90% success rate
        assert!(success_rate >= 0.9, "Success rate too low: {:.1}%", success_rate * 100.0);
    }

    #[tokio::test]
    async fn test_extreme_concurrency_with_long_timeouts() {
        let manager = crate::fast_lock::FastObjectLockManager::new();

        // Test that the new longer timeouts can handle extreme concurrency
        let mut handles = Vec::new();
        let total_concurrent_requests = 100;

        for i in 0..total_concurrent_requests {
            let manager_clone = manager.clone();
            let handle = tokio::spawn(async move {
                let bucket = "test";
                let object = format!("extreme_load_object_{}", i % 20); // Force some contention
                let owner = format!("user_{i}");

                // Mix of read and write locks to create realistic contention
                let result = if i % 3 == 0 {
                    manager_clone.acquire_write_lock(bucket, object, owner).await
                } else {
                    manager_clone.acquire_high_priority_read_lock(bucket, object, owner).await
                };

                match result {
                    Ok(_guard) => {
                        // Simulate some work
                        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
                        true
                    }
                    Err(_) => false,
                }
            });
            handles.push(handle);
        }

        let mut successful = 0;
        for handle in handles {
            if let Ok(true) = handle.await {
                successful += 1;
            }
        }

        let success_rate = successful as f64 / total_concurrent_requests as f64;
        println!(
            "Extreme concurrency test: {}/{} succeeded ({:.1}%)",
            successful,
            total_concurrent_requests,
            success_rate * 100.0
        );

        // With longer timeouts and better retry logic, we should handle this better
        assert!(
            success_rate >= 0.8,
            "Success rate too low under extreme load: {:.1}%",
            success_rate * 100.0
        );
    }

    #[tokio::test]
    async fn test_multi_client_datacenter_simulation() {
        let manager = Arc::new(crate::fast_lock::FastObjectLockManager::new());

        // Simulate multiple Databend nodes (clients) accessing shared storage
        let num_clients = 8;
        let queries_per_client = 15;
        let shared_tables = 50; // Shared data files that all clients might access

        let mut all_handles = Vec::new();
        let start_time = std::time::Instant::now();

        // Each "client" represents a separate Databend instance
        for client_id in 0..num_clients {
            let manager_clone = manager.clone();

            // Each client runs multiple concurrent queries
            let client_handles: Vec<_> = (0..queries_per_client)
                .map(|query_id| {
                    let manager_ref = manager_clone.clone();
                    tokio::spawn(async move {
                        let mut acquired_locks = Vec::new();
                        let query_complexity = 3 + (query_id % 8); // 3-10 tables per query

                        // Simulate complex query touching multiple shared tables
                        for table_idx in 0..query_complexity {
                            // Create realistic contention - multiple clients often access same popular tables
                            let table_id = match table_idx {
                                0 => query_id % 5,             // High contention on first few tables
                                1 => query_id % 10,            // Medium contention
                                _ => query_id % shared_tables, // Lower contention
                            };

                            let bucket = "datacenter-shared";
                            let object = format!("table_{table_id}_{client_id}_partition_{table_idx}");
                            let owner = format!("client_{client_id}_query_{query_id}");

                            // Mix of operations - mostly reads with some writes
                            let lock_result = if table_idx == 0 && query_id % 7 == 0 {
                                // Occasional write operations (updates/inserts)
                                manager_ref.acquire_high_priority_write_lock(bucket, object, owner).await
                            } else if query_id % 3 == 0 {
                                // Critical analytical queries
                                manager_ref.acquire_critical_read_lock(bucket, object, owner).await
                            } else {
                                // Normal read queries
                                manager_ref.acquire_high_priority_read_lock(bucket, object, owner).await
                            };

                            match lock_result {
                                Ok(guard) => acquired_locks.push(guard),
                                Err(_) => {
                                    // Query failed - return partial success info
                                    return (client_id, query_id, false, acquired_locks.len(), query_complexity);
                                }
                            }
                        }

                        // Simulate query execution time (varies by complexity)
                        let execution_time = 20 + (query_complexity * 8) + (client_id * 3);
                        tokio::time::sleep(tokio::time::Duration::from_millis(execution_time)).await;

                        (client_id, query_id, true, acquired_locks.len(), query_complexity)
                    })
                })
                .collect();

            all_handles.extend(client_handles);
        }

        // Wait for all queries across all clients to complete
        let mut results = Vec::new();
        for handle in all_handles {
            if let Ok(result) = handle.await {
                results.push(result);
            }
        }

        let total_time = start_time.elapsed();

        // Analyze results
        let total_queries = results.len();
        let successful_queries = results.iter().filter(|(_, _, success, _, _)| *success).count();
        let total_locks_acquired: usize = results.iter().map(|(_, _, _, locks, _)| *locks).sum();
        let total_locks_needed: usize = results.iter().map(|(_, _, _, _, complexity)| *complexity as usize).sum();

        // Per-client statistics
        let mut client_stats = std::collections::HashMap::new();
        for (client_id, _, success, _, _) in &results {
            let entry = client_stats.entry(*client_id).or_insert((0, 0));
            entry.0 += 1; // total queries
            if *success {
                entry.1 += 1;
            } // successful queries
        }

        println!("\n=== Multi-Client Datacenter Simulation Results ===");
        println!("Total execution time: {total_time:?}");
        println!("Total clients: {num_clients}");
        println!("Queries per client: {queries_per_client}");
        println!("Total queries executed: {total_queries}");
        println!(
            "Successful queries: {} ({:.1}%)",
            successful_queries,
            successful_queries as f64 / total_queries as f64 * 100.0
        );
        println!(
            "Locks acquired: {}/{} ({:.1}%)",
            total_locks_acquired,
            total_locks_needed,
            total_locks_acquired as f64 / total_locks_needed as f64 * 100.0
        );

        // Per-client breakdown
        println!("\nPer-client success rates:");
        for client_id in 0..num_clients {
            if let Some((total, success)) = client_stats.get(&client_id) {
                println!(
                    "  Client {}: {}/{} ({:.1}%)",
                    client_id,
                    success,
                    total,
                    *success as f64 / *total as f64 * 100.0
                );
            }
        }

        let overall_success_rate = successful_queries as f64 / total_queries as f64;
        let lock_acquisition_rate = total_locks_acquired as f64 / total_locks_needed as f64;

        // In a real datacenter scenario with multiple Databend instances,
        // we should achieve high success rates with the new optimizations
        assert!(
            overall_success_rate >= 0.85,
            "Multi-client success rate too low: {:.1}% (expected >= 85%)",
            overall_success_rate * 100.0
        );

        assert!(
            lock_acquisition_rate >= 0.90,
            "Lock acquisition rate too low: {:.1}% (expected >= 90%)",
            lock_acquisition_rate * 100.0
        );

        // Performance assertion - should complete in reasonable time
        assert!(
            total_time < std::time::Duration::from_secs(120),
            "Multi-client test took too long: {total_time:?}"
        );
    }

    #[tokio::test]
    async fn test_thundering_herd_scenario() {
        let manager = Arc::new(crate::fast_lock::FastObjectLockManager::new());

        // Simulate the "thundering herd" problem where many clients
        // simultaneously try to access the same hot data
        let num_concurrent_clients = 50;
        let hot_objects = 5; // Very few objects that everyone wants

        let mut handles = Vec::new();
        let start_time = std::time::Instant::now();

        // All clients trying to access the same hot objects simultaneously
        for client_id in 0..num_concurrent_clients {
            let manager_clone = manager.clone();

            let handle = tokio::spawn(async move {
                let mut client_success = 0;
                let mut client_attempts = 0;

                // Each client tries to access all hot objects
                for obj_id in 0..hot_objects {
                    client_attempts += 1;

                    let bucket = "hot-data";
                    let object = format!("popular_table_{obj_id}");
                    let owner = format!("thundering_client_{client_id}");

                    // Simulate different access patterns
                    let result = match obj_id % 3 {
                        0 => {
                            // Most popular object - everyone reads
                            manager_clone.acquire_critical_read_lock(bucket, object, owner).await
                        }
                        1 => {
                            // Second most popular - mix of reads and occasional writes
                            if client_id % 10 == 0 {
                                manager_clone.acquire_critical_write_lock(bucket, object, owner).await
                            } else {
                                manager_clone.acquire_high_priority_read_lock(bucket, object, owner).await
                            }
                        }
                        _ => {
                            // Other objects - normal priority
                            manager_clone.acquire_read_lock(bucket, object, owner).await
                        }
                    };

                    match result {
                        Ok(_guard) => {
                            client_success += 1;
                            // Simulate brief work with the data
                            tokio::time::sleep(tokio::time::Duration::from_millis(5)).await;
                        }
                        Err(_) => {
                            // Failed to get lock
                        }
                    }
                }

                (client_id, client_success, client_attempts)
            });

            handles.push(handle);
        }

        // Wait for all clients to finish
        let mut total_successes = 0;
        let mut total_attempts = 0;

        for handle in handles {
            if let Ok((_, successes, attempts)) = handle.await {
                total_successes += successes;
                total_attempts += attempts;
            }
        }

        let total_time = start_time.elapsed();
        let success_rate = total_successes as f64 / total_attempts as f64;

        println!("\n=== Thundering Herd Scenario Results ===");
        println!("Concurrent clients: {num_concurrent_clients}");
        println!("Hot objects: {hot_objects}");
        println!("Total attempts: {total_attempts}");
        println!("Total successes: {total_successes}");
        println!("Success rate: {:.1}%", success_rate * 100.0);
        println!("Total time: {total_time:?}");
        println!(
            "Average time per operation: {:.1}ms",
            total_time.as_millis() as f64 / total_attempts as f64
        );

        // Thundering herd is the hardest scenario - expect at least 75% success
        assert!(
            success_rate >= 0.75,
            "Thundering herd success rate too low: {:.1}% (expected >= 75%)",
            success_rate * 100.0
        );

        // Should handle this volume in reasonable time
        assert!(
            total_time < std::time::Duration::from_secs(180),
            "Thundering herd test took too long: {total_time:?}"
        );
    }

    #[tokio::test]
    async fn test_mixed_workload_stress() {
        let manager = Arc::new(crate::fast_lock::FastObjectLockManager::new());

        // Mixed workload: OLTP (many small fast transactions) + OLAP (few large analytical queries)
        let mut handles = Vec::new();
        let start_time = std::time::Instant::now();

        // OLTP workload - many small, fast operations
        for oltp_id in 0..30 {
            let manager_clone = manager.clone();
            let handle = tokio::spawn(async move {
                let mut successes = 0;
                let operations_per_transaction = 3; // Small transactions

                for tx_id in 0..10 {
                    // 10 transactions per OLTP client
                    let mut tx_success = true;
                    let mut _tx_locks = Vec::new();

                    for op_id in 0..operations_per_transaction {
                        let bucket = "oltp-data";
                        let object = format!("record_{}_{}", oltp_id * 10 + tx_id, op_id);
                        let owner = format!("oltp_{oltp_id}_{tx_id}");

                        // OLTP is mostly writes
                        let result = manager_clone.acquire_write_lock(bucket, object, owner).await;
                        match result {
                            Ok(guard) => _tx_locks.push(guard),
                            Err(_) => {
                                tx_success = false;
                                break;
                            }
                        }
                    }

                    if tx_success {
                        successes += 1;
                        // Simulate fast OLTP operation
                        tokio::time::sleep(tokio::time::Duration::from_millis(2)).await;
                    }
                }

                (oltp_id, successes, 10) // (client_id, successes, total_attempts)
            });
            handles.push(handle);
        }

        // OLAP workload - fewer, larger analytical queries
        for olap_id in 0..5 {
            let manager_clone = manager.clone();
            let handle = tokio::spawn(async move {
                let mut successes = 0;

                for query_id in 0..3 {
                    // 3 large queries per OLAP client
                    let mut _query_locks = Vec::new();
                    let mut query_success = true;
                    let tables_per_query = 15; // Large analytical queries

                    for table_id in 0..tables_per_query {
                        let bucket = "olap-data";
                        let object = format!(
                            "analytics_table_{}_{}",
                            table_id % 20, // Some overlap between queries
                            query_id
                        );
                        let owner = format!("olap_{olap_id}_{query_id}");

                        // OLAP is mostly reads with high priority
                        let result = manager_clone.acquire_critical_read_lock(bucket, object, owner).await;
                        match result {
                            Ok(guard) => _query_locks.push(guard),
                            Err(_) => {
                                query_success = false;
                                break;
                            }
                        }
                    }

                    if query_success {
                        successes += 1;
                        // Simulate long analytical query
                        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                    }
                }

                (olap_id + 100, successes, 3) // (client_id, successes, total_attempts)
            });
            handles.push(handle);
        }

        // Collect results
        let mut oltp_successes = 0;
        let mut oltp_attempts = 0;
        let mut olap_successes = 0;
        let mut olap_attempts = 0;

        for handle in handles {
            if let Ok((client_id, successes, attempts)) = handle.await {
                if client_id >= 100 {
                    // OLAP client
                    olap_successes += successes;
                    olap_attempts += attempts;
                } else {
                    // OLTP client
                    oltp_successes += successes;
                    oltp_attempts += attempts;
                }
            }
        }

        let total_time = start_time.elapsed();
        let oltp_success_rate = oltp_successes as f64 / oltp_attempts as f64;
        let olap_success_rate = olap_successes as f64 / olap_attempts as f64;

        println!("\n=== Mixed Workload Stress Test Results ===");
        println!("Total time: {total_time:?}");
        println!(
            "OLTP: {}/{} transactions succeeded ({:.1}%)",
            oltp_successes,
            oltp_attempts,
            oltp_success_rate * 100.0
        );
        println!(
            "OLAP: {}/{} queries succeeded ({:.1}%)",
            olap_successes,
            olap_attempts,
            olap_success_rate * 100.0
        );

        // Both workloads should succeed at high rates
        assert!(oltp_success_rate >= 0.90, "OLTP success rate too low: {:.1}%", oltp_success_rate * 100.0);
        assert!(olap_success_rate >= 0.85, "OLAP success rate too low: {:.1}%", olap_success_rate * 100.0);
    }
}
