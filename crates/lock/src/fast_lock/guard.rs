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
}

impl FastLockGuard {
    pub(crate) fn new(key: ObjectKey, mode: LockMode, owner: Arc<str>, shard: Arc<LockShard>) -> Self {
        Self {
            key,
            mode,
            owner,
            shard: Some(shard),
            released: false,
            disabled: false,
        }
    }

    /// Create a disabled guard (when locks are globally disabled)
    pub(crate) fn new_disabled(key: ObjectKey, mode: LockMode, owner: Arc<str>) -> Self {
        Self {
            key,
            mode,
            owner,
            shard: None,
            released: false,
            disabled: true,
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
            return true;
        }

        if let Some(shard) = &self.shard {
            let success = shard.release_lock(&self.key, &self.owner, self.mode);
            if success {
                self.released = true;
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
        if !self.released && !self.disabled {
            if let Some(shard) = &self.shard {
                let success = shard.release_lock(&self.key, &self.owner, self.mode);
                if !success {
                    tracing::warn!(
                        "Failed to release lock during drop: key={}, owner={}, mode={:?}",
                        self.key,
                        self.owner,
                        self.mode
                    );
                }
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
    async fn test_guard_iteration_improvements() {
        let manager = FastObjectLockManager::new();
        let mut multiple = MultipleLockGuards::new();

        // Acquire locks for different buckets and owners
        let guard1 = manager.acquire_read_lock("bucket1", "obj1", "owner1").await.unwrap();
        let guard2 = manager.acquire_read_lock("bucket2", "obj2", "owner1").await.unwrap();
        let guard3 = manager.acquire_write_lock("bucket1", "obj3", "owner2").await.unwrap();

        multiple.add(guard1);
        multiple.add(guard2);
        multiple.add(guard3);

        // Test filtering by bucket
        let bucket1_guards = multiple.guards_for_bucket("bucket1");
        assert_eq!(bucket1_guards.len(), 2);

        // Test filtering by owner
        let owner1_guards = multiple.guards_for_owner("owner1");
        assert_eq!(owner1_guards.len(), 2);

        // Test custom filter
        let write_guards = multiple.filter(|guard| guard.mode() == LockMode::Exclusive);
        assert_eq!(write_guards.len(), 1);

        // Test that original is not consumed
        assert_eq!(multiple.len(), 3);
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
}
