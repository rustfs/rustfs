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

use async_trait::async_trait;
use dashmap::DashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

use crate::{
    config::LockConfig,
    error::{LockError, Result},
    types::{LockId, LockInfo, LockRequest, LockResponse, LockStats},
};

/// Local lock manager trait, defines core operations for local locks
#[async_trait::async_trait]
pub trait LocalLockManager: Send + Sync {
    /// Acquire write lock
    ///
    /// # Parameters
    /// - resource: Unique resource identifier (e.g., path)
    /// - owner: Lock holder identifier
    /// - timeout: Timeout for acquiring the lock
    ///
    /// # Returns
    /// - Ok(true): Successfully acquired
    /// - Ok(false): Timeout without acquiring lock
    /// - Err: Error occurred
    async fn lock(&self, resource: &str, owner: &str, timeout: Duration) -> std::io::Result<bool>;

    /// Acquire read lock
    async fn rlock(&self, resource: &str, owner: &str, timeout: Duration) -> std::io::Result<bool>;

    /// Release write lock
    async fn unlock(&self, resource: &str, owner: &str) -> std::io::Result<()>;

    /// Release read lock
    async fn runlock(&self, resource: &str, owner: &str) -> std::io::Result<()>;

    /// Check if resource is locked (read or write)
    async fn is_locked(&self, resource: &str) -> bool;
}

/// Basic implementation struct for local lock manager
///
/// Internally maintains a mapping table from resources to lock objects, using DashMap for high concurrency performance
#[derive(Debug)]
pub struct LocalLockMap {
    /// Resource lock mapping table, key is unique resource identifier, value is lock object
    /// Uses DashMap to implement sharded locks for improved concurrency performance
    locks: Arc<DashMap<String, Arc<RwLock<LocalLockEntry>>>>,
}

/// Lock object for a single resource
#[derive(Debug)]
pub struct LocalLockEntry {
    /// Current write lock holder
    pub writer: Option<String>,
    /// Set of current read lock holders
    pub readers: Vec<String>,
    /// Lock expiration time (set when either write or read lock is held, None means no timeout)
    pub expires_at: Option<Instant>,
}

impl LocalLockMap {
    /// Create a new local lock manager
    pub fn new() -> Self {
        let map = Self {
            locks: Arc::new(DashMap::new()),
        };
        map.spawn_expiry_task();
        map
    }

    /// Start background task to periodically clean up expired locks
    fn spawn_expiry_task(&self) {
        let locks = self.locks.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            loop {
                interval.tick().await;
                let now = Instant::now();
                let mut to_remove = Vec::new();

                // DashMap's iter() method provides concurrency-safe iteration
                for item in locks.iter() {
                    let mut entry_guard = item.value().write().await;
                    if let Some(exp) = entry_guard.expires_at {
                        if exp <= now {
                            // Clear lock content
                            entry_guard.writer = None;
                            entry_guard.readers.clear();
                            entry_guard.expires_at = None;

                            // If entry is completely empty, mark for deletion
                            if entry_guard.writer.is_none() && entry_guard.readers.is_empty() {
                                to_remove.push(item.key().clone());
                            }
                        }
                    }
                }

                // Remove empty entries
                for key in to_remove {
                    locks.remove(&key);
                }
            }
        });
    }

    /// Batch acquire write locks
    ///
    /// Attempt to acquire write locks on all resources, if any resource fails to lock, rollback all previously locked resources
    pub async fn lock_batch(
        &self,
        resources: &[String],
        owner: &str,
        timeout: std::time::Duration,
        ttl: Option<Duration>,
    ) -> crate::error::Result<bool> {
        let mut locked = Vec::new();
        let expires_at = ttl.map(|t| Instant::now() + t);
        for resource in resources {
            match self.lock_with_ttl(resource, owner, timeout, expires_at).await {
                Ok(true) => {
                    locked.push(resource.clone());
                }
                Ok(false) => {
                    // Rollback previously locked resources
                    for locked_resource in locked {
                        let _ = self.unlock(&locked_resource, owner).await;
                    }
                    return Ok(false);
                }
                Err(e) => {
                    // Rollback previously locked resources
                    for locked_resource in locked {
                        let _ = self.unlock(&locked_resource, owner).await;
                    }
                    return Err(crate::error::LockError::internal(format!("Lock failed: {e}")));
                }
            }
        }
        Ok(true)
    }

    /// Batch release write locks
    pub async fn unlock_batch(&self, resources: &[String], owner: &str) -> crate::error::Result<()> {
        for resource in resources {
            let _ = self.unlock(resource, owner).await;
        }
        Ok(())
    }

    /// Batch acquire read locks
    pub async fn rlock_batch(
        &self,
        resources: &[String],
        owner: &str,
        timeout: std::time::Duration,
        ttl: Option<Duration>,
    ) -> crate::error::Result<bool> {
        let mut locked = Vec::new();
        let expires_at = ttl.map(|t| Instant::now() + t);
        for resource in resources {
            match self.rlock_with_ttl(resource, owner, timeout, expires_at).await {
                Ok(true) => {
                    locked.push(resource.clone());
                }
                Ok(false) => {
                    // Rollback previously locked resources
                    for locked_resource in locked {
                        let _ = self.runlock(&locked_resource, owner).await;
                    }
                    return Ok(false);
                }
                Err(e) => {
                    // Rollback previously locked resources
                    for locked_resource in locked {
                        let _ = self.runlock(&locked_resource, owner).await;
                    }
                    return Err(crate::error::LockError::internal(format!("Read lock failed: {e}")));
                }
            }
        }
        Ok(true)
    }

    /// Batch release read locks
    pub async fn runlock_batch(&self, resources: &[String], owner: &str) -> crate::error::Result<()> {
        for resource in resources {
            let _ = self.runlock(resource, owner).await;
        }
        Ok(())
    }
}

/// Local lock manager
pub struct LocalLockManagerImpl {
    config: Arc<LockConfig>,
}

impl LocalLockManagerImpl {
    /// Create a new local lock manager
    pub fn new(config: Arc<LockConfig>) -> Result<Self> {
        Ok(Self { config })
    }
}

#[async_trait]
impl super::LockManager for LocalLockManagerImpl {
    async fn acquire_exclusive(&self, _request: LockRequest) -> Result<LockResponse> {
        Err(LockError::internal("Local lock manager not implemented yet"))
    }

    async fn acquire_shared(&self, _request: LockRequest) -> Result<LockResponse> {
        Err(LockError::internal("Local lock manager not implemented yet"))
    }

    async fn release(&self, _lock_id: &LockId) -> Result<bool> {
        Err(LockError::internal("Local lock manager not implemented yet"))
    }

    async fn refresh(&self, _lock_id: &LockId) -> Result<bool> {
        Err(LockError::internal("Local lock manager not implemented yet"))
    }

    async fn force_release(&self, _lock_id: &LockId) -> Result<bool> {
        Err(LockError::internal("Local lock manager not implemented yet"))
    }

    async fn check_status(&self, _lock_id: &LockId) -> Result<Option<LockInfo>> {
        Err(LockError::internal("Local lock manager not implemented yet"))
    }

    async fn get_stats(&self) -> Result<LockStats> {
        Ok(LockStats::default())
    }

    async fn shutdown(&self) -> Result<()> {
        Ok(())
    }
}

#[async_trait::async_trait]
impl LocalLockManager for LocalLockMap {
    /// Acquire write lock. If resource is not locked, owner gets write lock; otherwise wait until timeout.
    async fn lock(&self, resource: &str, owner: &str, timeout: Duration) -> std::io::Result<bool> {
        Self::lock_with_ttl(self, resource, owner, timeout, None).await
    }

    /// Acquire read lock. If resource has no write lock, owner gets read lock; otherwise wait until timeout.
    async fn rlock(&self, resource: &str, owner: &str, timeout: Duration) -> std::io::Result<bool> {
        Self::rlock_with_ttl(self, resource, owner, timeout, None).await
    }

    /// Release write lock. Only the owner holding the write lock can release it.
    async fn unlock(&self, resource: &str, owner: &str) -> std::io::Result<()> {
        if let Some(entry) = self.locks.get(resource) {
            let mut entry_guard = entry.value().write().await;
            if entry_guard.writer.as_deref() == Some(owner) {
                entry_guard.writer = None;
                entry_guard.expires_at = None;
            }
            entry_guard.readers.retain(|r| r != owner);
            if entry_guard.readers.is_empty() && entry_guard.writer.is_none() {
                entry_guard.expires_at = None;
            }
        }
        Ok(())
    }

    /// Release read lock. Only the owner holding the read lock can release it.
    async fn runlock(&self, resource: &str, owner: &str) -> std::io::Result<()> {
        self.unlock(resource, owner).await
    }

    /// Check if resource is locked (having write lock or read lock is considered locked).
    async fn is_locked(&self, resource: &str) -> bool {
        if let Some(entry) = self.locks.get(resource) {
            let entry_guard = entry.value().read().await;
            entry_guard.writer.is_some() || !entry_guard.readers.is_empty()
        } else {
            false
        }
    }
}

impl LocalLockMap {
    /// Write lock with timeout support
    pub async fn lock_with_ttl(
        &self,
        resource: &str,
        owner: &str,
        timeout: Duration,
        expires_at: Option<Instant>,
    ) -> std::io::Result<bool> {
        let start = Instant::now();
        loop {
            {
                // DashMap's entry API automatically handles sharded locks
                let entry = self.locks.entry(resource.to_string()).or_insert_with(|| {
                    Arc::new(RwLock::new(LocalLockEntry {
                        writer: None,
                        readers: Vec::new(),
                        expires_at: None,
                    }))
                });

                let mut entry_guard = entry.value().write().await;
                // Write lock only needs to check if there's a write lock, no need to check read locks
                // If read locks exist, write lock should wait
                if entry_guard.writer.is_none() {
                    entry_guard.writer = Some(owner.to_string());
                    entry_guard.expires_at = expires_at;
                    return Ok(true);
                }
            }
            if start.elapsed() >= timeout {
                return Ok(false);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// Read lock with timeout support
    pub async fn rlock_with_ttl(
        &self,
        resource: &str,
        owner: &str,
        timeout: Duration,
        expires_at: Option<Instant>,
    ) -> std::io::Result<bool> {
        let start = Instant::now();
        loop {
            {
                // DashMap's entry API automatically handles sharded locks
                let entry = self.locks.entry(resource.to_string()).or_insert_with(|| {
                    Arc::new(RwLock::new(LocalLockEntry {
                        writer: None,
                        readers: Vec::new(),
                        expires_at: None,
                    }))
                });

                let mut entry_guard = entry.value().write().await;
                if entry_guard.writer.is_none() {
                    if !entry_guard.readers.contains(&owner.to_string()) {
                        entry_guard.readers.push(owner.to_string());
                    }
                    entry_guard.expires_at = expires_at;
                    return Ok(true);
                }
            }
            if start.elapsed() >= timeout {
                return Ok(false);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

impl Default for LocalLockMap {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::task;

    /// Test basic write lock acquisition and release
    #[tokio::test]
    async fn test_write_lock_basic() {
        let lock_map = LocalLockMap::new();
        let ok = lock_map.lock("foo", "owner1", Duration::from_millis(100)).await.unwrap();
        assert!(ok, "Write lock should be successfully acquired");
        assert!(lock_map.is_locked("foo").await, "Lock state should be locked");
        lock_map.unlock("foo", "owner1").await.unwrap();
        assert!(!lock_map.is_locked("foo").await, "Should be unlocked after release");
    }

    /// Test basic read lock acquisition and release
    #[tokio::test]
    async fn test_read_lock_basic() {
        let lock_map = LocalLockMap::new();
        let ok = lock_map.rlock("bar", "reader1", Duration::from_millis(100)).await.unwrap();
        assert!(ok, "Read lock should be successfully acquired");
        assert!(lock_map.is_locked("bar").await, "Lock state should be locked");
        lock_map.runlock("bar", "reader1").await.unwrap();
        assert!(!lock_map.is_locked("bar").await, "Should be unlocked after release");
    }

    /// Test write lock mutual exclusion
    #[tokio::test]
    async fn test_write_lock_mutex() {
        let lock_map = Arc::new(LocalLockMap::new());
        // owner1 acquires write lock first
        let ok = lock_map.lock("res", "owner1", Duration::from_millis(100)).await.unwrap();
        assert!(ok);
        // owner2 tries to acquire write lock on same resource, should timeout and fail
        let lock_map2 = lock_map.clone();
        let fut = task::spawn(async move { lock_map2.lock("res", "owner2", Duration::from_millis(50)).await.unwrap() });
        let ok2 = fut.await.unwrap();
        assert!(!ok2, "Write locks should be mutually exclusive, owner2 acquisition should fail");
        lock_map.unlock("res", "owner1").await.unwrap();
    }

    /// Test read lock sharing
    #[tokio::test]
    async fn test_read_lock_shared() {
        let lock_map = Arc::new(LocalLockMap::new());
        let ok1 = lock_map.rlock("res2", "reader1", Duration::from_millis(100)).await.unwrap();
        assert!(ok1);
        let lock_map2 = lock_map.clone();
        let fut = task::spawn(async move { lock_map2.rlock("res2", "reader2", Duration::from_millis(100)).await.unwrap() });
        let ok2 = fut.await.unwrap();
        assert!(ok2, "Multiple read locks should be shareable");
        lock_map.runlock("res2", "reader1").await.unwrap();
        lock_map.runlock("res2", "reader2").await.unwrap();
    }

    /// Test mutual exclusion between write lock and read lock
    #[tokio::test]
    async fn test_write_read_mutex() {
        let lock_map = Arc::new(LocalLockMap::new());
        // Acquire write lock first
        let ok = lock_map.lock("res3", "owner1", Duration::from_millis(100)).await.unwrap();
        assert!(ok);
        // Read lock should fail to acquire
        let lock_map2 = lock_map.clone();
        let fut = task::spawn(async move { lock_map2.rlock("res3", "reader1", Duration::from_millis(50)).await.unwrap() });
        let ok2 = fut.await.unwrap();
        assert!(!ok2, "Read lock should fail to acquire when write lock exists");
        lock_map.unlock("res3", "owner1").await.unwrap();
    }

    /// Test timeout failure when acquiring lock
    #[tokio::test]
    async fn test_lock_timeout() {
        let lock_map = Arc::new(LocalLockMap::new());
        let ok = lock_map.lock("res4", "owner1", Duration::from_millis(100)).await.unwrap();
        assert!(ok);
        // owner2 tries to acquire write lock on same resource with very short timeout, should fail
        let lock_map2 = lock_map.clone();
        let fut = task::spawn(async move { lock_map2.lock("res4", "owner2", Duration::from_millis(1)).await.unwrap() });
        let ok2 = fut.await.unwrap();
        assert!(!ok2, "Should fail due to timeout");
        lock_map.unlock("res4", "owner1").await.unwrap();
    }

    /// Test that owner can only release locks they hold
    #[tokio::test]
    async fn test_owner_unlock() {
        let lock_map = LocalLockMap::new();
        let ok = lock_map.lock("res5", "owner1", Duration::from_millis(100)).await.unwrap();
        assert!(ok);
        // owner2 tries to release owner1's lock, should not affect lock state
        lock_map.unlock("res5", "owner2").await.unwrap();
        assert!(lock_map.is_locked("res5").await, "Non-owner cannot release others' locks");
        lock_map.unlock("res5", "owner1").await.unwrap();
        assert!(!lock_map.is_locked("res5").await);
    }

    /// Correctness in concurrent scenarios
    #[tokio::test]
    async fn test_concurrent_readers() {
        let lock_map = Arc::new(LocalLockMap::new());
        let mut handles = vec![];
        for i in 0..10 {
            let lock_map = lock_map.clone();
            handles.push(task::spawn(async move {
                let owner = format!("reader{i}");
                let ok = lock_map.rlock("res6", &owner, Duration::from_millis(100)).await.unwrap();
                assert!(ok);
                lock_map.runlock("res6", &owner).await.unwrap();
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
        assert!(!lock_map.is_locked("res6").await, "Should be unlocked after all read locks are released");
    }

    #[tokio::test]
    async fn test_lock_expiry() {
        let map = LocalLockMap::new();
        let key = "res1".to_string();
        let owner = "owner1";
        // Acquire lock with TTL 100ms
        let ok = map
            .lock_batch(std::slice::from_ref(&key), owner, Duration::from_millis(10), Some(Duration::from_millis(100)))
            .await
            .unwrap();
        assert!(ok);
        assert!(map.is_locked(&key).await);

        // Wait up to 2 seconds until lock is cleaned up
        let mut waited = 0;
        while map.is_locked(&key).await && waited < 2000 {
            tokio::time::sleep(Duration::from_millis(50)).await;
            waited += 50;
        }
        assert!(!map.is_locked(&key).await, "Lock should be automatically released after TTL");
    }

    #[tokio::test]
    async fn test_rlock_expiry() {
        let map = LocalLockMap::new();
        let key = "res2".to_string();
        let owner = "owner2";
        // Acquire read lock with TTL 80ms
        let ok = map
            .rlock_batch(std::slice::from_ref(&key), owner, Duration::from_millis(10), Some(Duration::from_millis(80)))
            .await
            .unwrap();
        assert!(ok);
        assert!(map.is_locked(&key).await);

        // Wait up to 2 seconds until lock is cleaned up
        let mut waited = 0;
        while map.is_locked(&key).await && waited < 2000 {
            tokio::time::sleep(Duration::from_millis(50)).await;
            waited += 50;
        }
        assert!(!map.is_locked(&key).await, "Read lock should be automatically released after TTL");
    }

    /// Test high concurrency performance of DashMap version
    #[tokio::test]
    async fn test_concurrent_performance() {
        let map = Arc::new(LocalLockMap::new());
        let mut handles = vec![];

        // Create multiple concurrent tasks, each operating on different resources
        for i in 0..50 {
            let map = map.clone();
            let resource = format!("resource_{i}");
            let owner = format!("owner_{i}");

            handles.push(tokio::spawn(async move {
                // Acquire write lock
                let ok = map.lock(&resource, &owner, Duration::from_millis(100)).await.unwrap();
                assert!(ok);

                // Hold lock briefly
                tokio::time::sleep(Duration::from_millis(10)).await;

                // Release lock
                map.unlock(&resource, &owner).await.unwrap();

                // Verify lock is released
                assert!(!map.is_locked(&resource).await);
            }));
        }

        // Wait for all tasks to complete
        for handle in handles {
            handle.await.unwrap();
        }

        // Verify all resources are released
        for i in 0..50 {
            let resource = format!("resource_{i}");
            assert!(!map.is_locked(&resource).await);
        }
    }
}
