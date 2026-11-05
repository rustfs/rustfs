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

use crate::fast_lock::state::ObjectLockState;
use crossbeam_queue::SegQueue;
use std::sync::atomic::{AtomicU64, Ordering};

/// Simple object pool for ObjectLockState to reduce allocation overhead
#[derive(Debug)]
pub struct ObjectStatePool {
    pool: SegQueue<Box<ObjectLockState>>,
    stats: PoolStats,
}

#[derive(Debug)]
struct PoolStats {
    hits: AtomicU64,
    misses: AtomicU64,
    releases: AtomicU64,
}

impl ObjectStatePool {
    pub fn new() -> Self {
        Self {
            pool: SegQueue::new(),
            stats: PoolStats {
                hits: AtomicU64::new(0),
                misses: AtomicU64::new(0),
                releases: AtomicU64::new(0),
            },
        }
    }

    /// Get an ObjectLockState from the pool or create a new one
    pub fn acquire(&self) -> Box<ObjectLockState> {
        if let Some(mut obj) = self.pool.pop() {
            self.stats.hits.fetch_add(1, Ordering::Relaxed);
            obj.reset_for_reuse();
            obj
        } else {
            self.stats.misses.fetch_add(1, Ordering::Relaxed);
            Box::new(ObjectLockState::new())
        }
    }

    /// Return an ObjectLockState to the pool
    pub fn release(&self, obj: Box<ObjectLockState>) {
        // Only keep the pool at reasonable size to avoid memory bloat
        if self.pool.len() < 1000 {
            self.stats.releases.fetch_add(1, Ordering::Relaxed);
            self.pool.push(obj);
        }
        // Otherwise let it drop naturally
    }

    /// Get pool statistics
    pub fn stats(&self) -> (u64, u64, u64, usize) {
        let hits = self.stats.hits.load(Ordering::Relaxed);
        let misses = self.stats.misses.load(Ordering::Relaxed);
        let releases = self.stats.releases.load(Ordering::Relaxed);
        let pool_size = self.pool.len();
        (hits, misses, releases, pool_size)
    }

    /// Get hit rate (0.0 to 1.0)
    pub fn hit_rate(&self) -> f64 {
        let hits = self.stats.hits.load(Ordering::Relaxed);
        let misses = self.stats.misses.load(Ordering::Relaxed);
        let total = hits + misses;

        if total == 0 { 0.0 } else { hits as f64 / total as f64 }
    }
}

impl Default for ObjectStatePool {
    fn default() -> Self {
        Self::new()
    }
}

impl ObjectLockState {
    /// Reset state for reuse from pool
    pub fn reset_for_reuse(&mut self) {
        // Reset atomic state
        self.atomic_state = crate::fast_lock::state::AtomicLockState::new();

        // Clear owners
        *self.current_owner.write() = None;
        self.shared_owners.write().clear();

        // Reset priority
        *self.priority.write() = crate::fast_lock::types::LockPriority::Normal;

        // Note: We don't reset notifications as they should be handled by drop/recreation
        // The optimized_notify will be reset automatically on next use
        self.optimized_notify = crate::fast_lock::optimized_notify::OptimizedNotify::new();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fast_lock::state::{ExclusiveOwnerInfo, SharedOwnerEntry};
    use std::sync::Arc;
    use std::time::{Duration, SystemTime};

    #[test]
    fn test_object_pool() {
        let pool = ObjectStatePool::new();

        // First acquisition should be a miss
        let obj1 = pool.acquire();
        let (hits, misses, _, _) = pool.stats();
        assert_eq!(hits, 0);
        assert_eq!(misses, 1);

        // Return to pool
        pool.release(obj1);
        let (_, _, releases, pool_size) = pool.stats();
        assert_eq!(releases, 1);
        assert_eq!(pool_size, 1);

        // Second acquisition should be a hit
        let _obj2 = pool.acquire();
        let (hits, misses, _, _) = pool.stats();
        assert_eq!(hits, 1);
        assert_eq!(misses, 1);

        assert_eq!(pool.hit_rate(), 0.5);
    }

    #[test]
    fn test_state_reset() {
        let mut state = ObjectLockState::new();

        // Modify state
        *state.current_owner.write() = Some(ExclusiveOwnerInfo {
            owner: Arc::from("test_owner"),
            acquired_at: SystemTime::now(),
            lock_timeout: Duration::from_secs(30),
        });
        state.shared_owners.write().push(SharedOwnerEntry {
            owner: Arc::from("shared_owner"),
            count: 1,
            acquired_at: SystemTime::now(),
            lock_timeout: Duration::from_secs(30),
        });

        // Reset
        state.reset_for_reuse();

        // Verify reset
        assert!(state.current_owner.read().is_none());
        assert!(state.shared_owners.read().is_empty());
    }
}
