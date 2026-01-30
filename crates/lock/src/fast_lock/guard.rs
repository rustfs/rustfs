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
