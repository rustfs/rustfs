// Copyright 2024 RustFS Team

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::SystemTime;
use tokio::sync::Notify;

use crate::fast_lock::types::{LockMode, LockPriority};

/// Atomic lock state encoding in u64
/// Bits:  [63:48] writers_waiting | [47:32] readers_waiting | [31:16] readers_count | [15:1] flags | [0] writer_flag
const WRITER_FLAG_MASK: u64 = 0x1;
const READERS_SHIFT: u8 = 16;
const READERS_MASK: u64 = 0xFFFF << READERS_SHIFT;
const READERS_WAITING_SHIFT: u8 = 32;
const READERS_WAITING_MASK: u64 = 0xFFFF << READERS_WAITING_SHIFT;
const WRITERS_WAITING_SHIFT: u8 = 48;
const WRITERS_WAITING_MASK: u64 = 0xFFFF << WRITERS_WAITING_SHIFT;

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
            last_accessed: AtomicU64::new(SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs()),
        }
    }

    /// Try to acquire shared lock (fast path)
    pub fn try_acquire_shared(&self) -> bool {
        self.update_access_time();

        loop {
            let current = self.state.load(Ordering::Acquire);

            // Cannot acquire if there's a writer or writers waiting
            if (current & WRITER_FLAG_MASK) != 0 || self.writers_waiting(current) > 0 {
                return false;
            }

            let readers = self.readers_count(current);
            if readers == 0xFFFF {
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

    fn update_access_time(&self) {
        let now = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs();
        self.last_accessed.store(now, Ordering::Relaxed);
    }

    fn readers_count(&self, state: u64) -> u16 {
        ((state & READERS_MASK) >> READERS_SHIFT) as u16
    }

    fn readers_waiting(&self, state: u64) -> u16 {
        ((state & READERS_WAITING_MASK) >> READERS_WAITING_SHIFT) as u16
    }

    fn writers_waiting(&self, state: u64) -> u16 {
        ((state & WRITERS_WAITING_MASK) >> WRITERS_WAITING_SHIFT) as u16
    }
}

/// Object lock state with version support
#[derive(Debug)]
pub struct ObjectLockState {
    /// Current owner of exclusive lock (if any)
    pub current_owner: parking_lot::RwLock<Option<Arc<str>>>,
    /// Shared owners set
    pub shared_owners: parking_lot::RwLock<std::collections::HashSet<Arc<str>>>,
    /// Atomic state for fast operations
    pub atomic_state: AtomicLockState,
    /// Notification for readers
    pub read_notify: Notify,
    /// Notification for writers  
    pub write_notify: Notify,
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
            current_owner: parking_lot::RwLock::new(None),
            shared_owners: parking_lot::RwLock::new(std::collections::HashSet::new()),
            atomic_state: AtomicLockState::new(),
            read_notify: Notify::new(),
            write_notify: Notify::new(),
            priority: parking_lot::RwLock::new(LockPriority::Normal),
        }
    }

    /// Try fast path shared lock acquisition
    pub fn try_acquire_shared_fast(&self, owner: &Arc<str>) -> bool {
        if self.atomic_state.try_acquire_shared() {
            let mut shared = self.shared_owners.write();
            shared.insert(owner.clone());
            true
        } else {
            false
        }
    }

    /// Try fast path exclusive lock acquisition
    pub fn try_acquire_exclusive_fast(&self, owner: &Arc<str>) -> bool {
        if self.atomic_state.try_acquire_exclusive() {
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
        if shared.remove(owner) {
            if self.atomic_state.release_shared() {
                // Notify waiting writers if no more readers
                if shared.is_empty() {
                    drop(shared);
                    self.write_notify.notify_one();
                }
                true
            } else {
                // Inconsistency - re-add owner
                shared.insert(owner.clone());
                false
            }
        } else {
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
                // Notify waiters - prefer writers over readers
                if self
                    .atomic_state
                    .writers_waiting(self.atomic_state.state.load(Ordering::Acquire))
                    > 0
                {
                    self.write_notify.notify_one();
                } else {
                    self.read_notify.notify_waiters();
                }
                true
            } else {
                false
            }
        } else {
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
