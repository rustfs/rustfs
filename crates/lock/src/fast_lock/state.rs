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
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime};
use tokio::sync::Notify;

use crate::fast_lock::optimized_notify::OptimizedNotify;
use crate::fast_lock::types::{LockMode, LockPriority};

/// Optimized atomic lock state encoding in u64
/// Bits: [63:48] reserved | [47:32] writers_waiting | [31:16] readers_waiting | [15:8] readers_count | [7:1] flags | [0] writer_flag
const WRITER_FLAG_MASK: u64 = 0x1;
const READERS_SHIFT: u8 = 8;
const READERS_MASK: u64 = 0xFF << READERS_SHIFT; // Support up to 255 concurrent readers
const READERS_WAITING_SHIFT: u8 = 16;
const READERS_WAITING_MASK: u64 = 0xFFFF << READERS_WAITING_SHIFT;
const WRITERS_WAITING_SHIFT: u8 = 32;
const WRITERS_WAITING_MASK: u64 = 0xFFFF << WRITERS_WAITING_SHIFT;

// Fast path check masks
const NO_WRITER_AND_NO_WAITING_WRITERS: u64 = WRITER_FLAG_MASK | WRITERS_WAITING_MASK;
const COMPLETELY_UNLOCKED: u64 = 0;

/// Fast atomic lock state for single version
#[derive(Debug)]
pub struct AtomicLockState {
    state: AtomicU64,
    last_accessed: AtomicU64,
}

impl Default for AtomicLockState {
    fn default() -> Self {
        Self::new()
    }
}

impl AtomicLockState {
    pub fn new() -> Self {
        Self {
            state: AtomicU64::new(0),
            last_accessed: AtomicU64::new(
                SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap_or(Duration::ZERO)
                    .as_secs(),
            ),
        }
    }

    /// Check if fast path is available for given lock mode
    #[inline(always)]
    pub fn is_fast_path_available(&self, mode: LockMode) -> bool {
        let state = self.state.load(Ordering::Relaxed); // Use Relaxed for better performance
        match mode {
            LockMode::Shared => {
                // No writer and no waiting writers
                (state & NO_WRITER_AND_NO_WAITING_WRITERS) == 0
            }
            LockMode::Exclusive => {
                // Completely unlocked
                state == COMPLETELY_UNLOCKED
            }
        }
    }

    /// Try to acquire shared lock (fast path)
    pub fn try_acquire_shared(&self) -> bool {
        self.update_access_time();

        loop {
            let current = self.state.load(Ordering::Acquire);

            // Fast path check - cannot acquire if there's a writer or writers waiting
            if (current & NO_WRITER_AND_NO_WAITING_WRITERS) != 0 {
                return false;
            }

            let readers = self.readers_count(current);
            if readers == 0xFF {
                // Updated limit to 255
                return false; // Too many readers
            }

            let new_state = current + (1 << READERS_SHIFT);

            if self
                .state
                .compare_exchange_weak(current, new_state, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                return true;
            }
        }
    }

    /// Try to acquire exclusive lock (fast path)
    pub fn try_acquire_exclusive(&self) -> bool {
        self.update_access_time();

        // Must be completely unlocked to acquire exclusive
        let expected = 0;
        let new_state = WRITER_FLAG_MASK;

        self.state
            .compare_exchange(expected, new_state, Ordering::AcqRel, Ordering::Relaxed)
            .is_ok()
    }

    /// Release shared lock
    pub fn release_shared(&self) -> bool {
        loop {
            let current = self.state.load(Ordering::Acquire);
            let readers = self.readers_count(current);

            if readers == 0 {
                return false; // No shared lock to release
            }

            let new_state = current - (1 << READERS_SHIFT);

            if self
                .state
                .compare_exchange_weak(current, new_state, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                self.update_access_time();
                return true;
            }
        }
    }

    /// Release exclusive lock
    pub fn release_exclusive(&self) -> bool {
        loop {
            let current = self.state.load(Ordering::Acquire);

            if (current & WRITER_FLAG_MASK) == 0 {
                return false; // No exclusive lock to release
            }

            let new_state = current & !WRITER_FLAG_MASK;

            if self
                .state
                .compare_exchange_weak(current, new_state, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                self.update_access_time();
                return true;
            }
        }
    }

    /// Increment waiting readers count
    pub fn inc_readers_waiting(&self) {
        loop {
            let current = self.state.load(Ordering::Acquire);
            let waiting = self.readers_waiting(current);

            if waiting == 0xFFFF {
                break; // Max waiting readers
            }

            let new_state = current + (1 << READERS_WAITING_SHIFT);

            if self
                .state
                .compare_exchange_weak(current, new_state, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                break;
            }
        }
    }

    /// Decrement waiting readers count
    pub fn dec_readers_waiting(&self) {
        loop {
            let current = self.state.load(Ordering::Acquire);
            let waiting = self.readers_waiting(current);

            if waiting == 0 {
                break; // No waiting readers
            }

            let new_state = current - (1 << READERS_WAITING_SHIFT);

            if self
                .state
                .compare_exchange_weak(current, new_state, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                break;
            }
        }
    }

    /// Increment waiting writers count
    pub fn inc_writers_waiting(&self) {
        loop {
            let current = self.state.load(Ordering::Acquire);
            let waiting = self.writers_waiting(current);

            if waiting == 0xFFFF {
                break; // Max waiting writers
            }

            let new_state = current + (1 << WRITERS_WAITING_SHIFT);

            if self
                .state
                .compare_exchange_weak(current, new_state, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                break;
            }
        }
    }

    /// Decrement waiting writers count
    pub fn dec_writers_waiting(&self) {
        loop {
            let current = self.state.load(Ordering::Acquire);
            let waiting = self.writers_waiting(current);

            if waiting == 0 {
                break; // No waiting writers
            }

            let new_state = current - (1 << WRITERS_WAITING_SHIFT);

            if self
                .state
                .compare_exchange_weak(current, new_state, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                break;
            }
        }
    }

    /// Check if lock is completely free
    pub fn is_free(&self) -> bool {
        let state = self.state.load(Ordering::Acquire);
        state == 0
    }

    /// Check if anyone is waiting
    pub fn has_waiters(&self) -> bool {
        let state = self.state.load(Ordering::Acquire);
        self.readers_waiting(state) > 0 || self.writers_waiting(state) > 0
    }

    /// Get last access time
    pub fn last_accessed(&self) -> u64 {
        self.last_accessed.load(Ordering::Relaxed)
    }

    pub fn update_access_time(&self) {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_secs();
        self.last_accessed.store(now, Ordering::Relaxed);
    }

    fn readers_count(&self, state: u64) -> u8 {
        ((state & READERS_MASK) >> READERS_SHIFT) as u8
    }

    fn readers_waiting(&self, state: u64) -> u16 {
        ((state & READERS_WAITING_MASK) >> READERS_WAITING_SHIFT) as u16
    }

    fn writers_waiting(&self, state: u64) -> u16 {
        ((state & WRITERS_WAITING_MASK) >> WRITERS_WAITING_SHIFT) as u16
    }
}

/// Object lock state with version support - optimized memory layout
#[derive(Debug)]
#[repr(align(64))] // Align to cache line boundary
pub struct ObjectLockState {
    // First cache line: Most frequently accessed data
    /// Atomic state for fast operations
    pub atomic_state: AtomicLockState,

    // Second cache line: Notification mechanisms
    /// Notification for readers (traditional)
    pub read_notify: Notify,
    /// Notification for writers (traditional)
    pub write_notify: Notify,
    /// Optimized notification system (optional)
    pub optimized_notify: OptimizedNotify,

    // Third cache line: Less frequently accessed data
    /// Current owner of exclusive lock (if any)
    pub current_owner: parking_lot::RwLock<Option<Arc<str>>>,
    /// Shared owners - optimized for small number of readers
    pub shared_owners: parking_lot::RwLock<smallvec::SmallVec<[Arc<str>; 4]>>,
    /// Lock priority for conflict resolution
    pub priority: parking_lot::RwLock<LockPriority>,
}

impl Default for ObjectLockState {
    fn default() -> Self {
        Self::new()
    }
}

impl ObjectLockState {
    pub fn new() -> Self {
        Self {
            atomic_state: AtomicLockState::new(),
            read_notify: Notify::new(),
            write_notify: Notify::new(),
            optimized_notify: OptimizedNotify::new(),
            current_owner: parking_lot::RwLock::new(None),
            shared_owners: parking_lot::RwLock::new(smallvec::SmallVec::new()),
            priority: parking_lot::RwLock::new(LockPriority::Normal),
        }
    }

    /// Try fast path shared lock acquisition
    pub fn try_acquire_shared_fast(&self, owner: &Arc<str>) -> bool {
        if self.atomic_state.try_acquire_shared() {
            self.atomic_state.update_access_time();
            let mut shared = self.shared_owners.write();
            if !shared.contains(owner) {
                shared.push(owner.clone());
            }
            true
        } else {
            false
        }
    }

    /// Try fast path exclusive lock acquisition
    pub fn try_acquire_exclusive_fast(&self, owner: &Arc<str>) -> bool {
        if self.atomic_state.try_acquire_exclusive() {
            self.atomic_state.update_access_time();
            let mut current = self.current_owner.write();
            *current = Some(owner.clone());
            true
        } else {
            false
        }
    }

    /// Release shared lock
    pub fn release_shared(&self, owner: &Arc<str>) -> bool {
        let mut shared = self.shared_owners.write();
        if let Some(pos) = shared.iter().position(|x| x.as_ref() == owner.as_ref()) {
            shared.remove(pos);
            if self.atomic_state.release_shared() {
                // Notify waiting writers if no more readers
                if shared.is_empty() {
                    drop(shared);
                    self.optimized_notify.notify_writer();
                }
                true
            } else {
                // Inconsistency detected - atomic state shows no shared lock but owner was found
                tracing::warn!(
                    "Atomic state inconsistency during shared lock release: owner={}, remaining_owners={}",
                    owner,
                    shared.len()
                );
                // Re-add owner to maintain consistency
                shared.push(owner.clone());
                false
            }
        } else {
            // Owner not found in shared owners list
            tracing::debug!(
                "Shared lock release failed - owner not found: owner={}, current_owners={:?}",
                owner,
                shared.iter().map(|s| s.as_ref()).collect::<Vec<_>>()
            );
            false
        }
    }

    /// Release exclusive lock
    pub fn release_exclusive(&self, owner: &Arc<str>) -> bool {
        let mut current = self.current_owner.write();
        if current.as_ref() == Some(owner) {
            if self.atomic_state.release_exclusive() {
                *current = None;
                drop(current);
                // Notify waiters using optimized system - prefer writers over readers
                if self
                    .atomic_state
                    .writers_waiting(self.atomic_state.state.load(Ordering::Acquire))
                    > 0
                {
                    self.optimized_notify.notify_writer();
                } else {
                    self.optimized_notify.notify_readers();
                }
                true
            } else {
                // Atomic state inconsistency - current owner matches but atomic release failed
                tracing::warn!(
                    "Atomic state inconsistency during exclusive lock release: owner={}, atomic_state={:b}",
                    owner,
                    self.atomic_state.state.load(Ordering::Acquire)
                );
                false
            }
        } else {
            // Owner mismatch
            tracing::debug!(
                "Exclusive lock release failed - owner mismatch: expected_owner={}, actual_owner={:?}",
                owner,
                current.as_ref().map(|s| s.as_ref())
            );
            false
        }
    }

    /// Check if object is locked
    pub fn is_locked(&self) -> bool {
        !self.atomic_state.is_free()
    }

    /// Get current lock mode
    pub fn current_mode(&self) -> Option<LockMode> {
        let state = self.atomic_state.state.load(Ordering::Acquire);
        if (state & WRITER_FLAG_MASK) != 0 {
            Some(LockMode::Exclusive)
        } else if self.atomic_state.readers_count(state) > 0 {
            Some(LockMode::Shared)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_atomic_lock_state() {
        let state = AtomicLockState::new();

        // Test shared lock
        assert!(state.try_acquire_shared());
        assert!(state.try_acquire_shared());
        assert!(!state.try_acquire_exclusive());

        assert!(state.release_shared());
        assert!(state.release_shared());
        assert!(!state.release_shared());

        // Test exclusive lock
        assert!(state.try_acquire_exclusive());
        assert!(!state.try_acquire_shared());
        assert!(!state.try_acquire_exclusive());

        assert!(state.release_exclusive());
        assert!(!state.release_exclusive());
    }

    #[test]
    fn test_object_lock_state() {
        let state = ObjectLockState::new();
        let owner1 = Arc::from("owner1");
        let owner2 = Arc::from("owner2");

        // Test shared locks
        assert!(state.try_acquire_shared_fast(&owner1));
        assert!(state.try_acquire_shared_fast(&owner2));
        assert!(!state.try_acquire_exclusive_fast(&owner1));

        assert!(state.release_shared(&owner1));
        assert!(state.release_shared(&owner2));

        // Test exclusive lock
        assert!(state.try_acquire_exclusive_fast(&owner1));
        assert!(!state.try_acquire_shared_fast(&owner2));
        assert!(state.release_exclusive(&owner1));
    }
}
