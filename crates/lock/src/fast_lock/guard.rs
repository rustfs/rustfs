// Copyright 2024 RustFS Team

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
    shard: Arc<LockShard>,
    released: bool,
}

impl FastLockGuard {
    pub(crate) fn new(key: ObjectKey, mode: LockMode, owner: Arc<str>, shard: Arc<LockShard>) -> Self {
        Self {
            key,
            mode,
            owner,
            shard,
            released: false,
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

        let success = self.shard.release_lock(&self.key, &self.owner, self.mode);
        if success {
            self.released = true;
        }
        success
    }

    /// Check if the lock has been released
    pub fn is_released(&self) -> bool {
        self.released
    }

    /// Get lock information for monitoring
    pub fn lock_info(&self) -> Option<crate::fast_lock::types::ObjectLockInfo> {
        if self.released {
            None
        } else {
            self.shard.get_lock_info(&self.key)
        }
    }
}

impl Drop for FastLockGuard {
    fn drop(&mut self) {
        if !self.released {
            let success = self.shard.release_lock(&self.key, &self.owner, self.mode);
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

impl std::fmt::Debug for FastLockGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FastLockGuard")
            .field("key", &self.key)
            .field("mode", &self.mode)
            .field("owner", &self.owner)
            .field("released", &self.released)
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

    /// Split guards by lock mode
    pub fn split_by_mode(&mut self) -> (Vec<FastLockGuard>, Vec<FastLockGuard>) {
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
        std::mem::take(&mut self.guards).into_iter()
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

        // Split by mode
        let (shared, exclusive) = multiple.split_by_mode();
        assert_eq!(shared.len(), 2);
        assert_eq!(exclusive.len(), 1);
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
