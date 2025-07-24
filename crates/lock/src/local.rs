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

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

use crate::LockRequest;

/// local lock entry
#[derive(Debug)]
pub struct LocalLockEntry {
    /// current writer
    pub writer: Option<String>,
    /// current readers with their lock counts
    pub readers: HashMap<String, usize>,
    /// lock expiration time
    pub expires_at: Option<Instant>,
}

/// local lock map
#[derive(Debug)]
pub struct LocalLockMap {
    /// LockId to lock object map
    pub locks: Arc<RwLock<HashMap<crate::types::LockId, Arc<RwLock<LocalLockEntry>>>>>,
    /// Shutdown flag for background tasks
    shutdown: Arc<AtomicBool>,
}

impl Default for LocalLockMap {
    fn default() -> Self {
        Self::new()
    }
}

impl LocalLockMap {
    /// create new local lock map
    pub fn new() -> Self {
        let map = Self {
            locks: Arc::new(RwLock::new(HashMap::new())),
            shutdown: Arc::new(AtomicBool::new(false)),
        };
        map.spawn_expiry_task();
        map
    }

    /// spawn expiry task to clean up expired locks
    fn spawn_expiry_task(&self) {
        let locks = self.locks.clone();
        let shutdown = self.shutdown.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            loop {
                interval.tick().await;

                if shutdown.load(Ordering::Relaxed) {
                    tracing::debug!("Expiry task shutting down");
                    break;
                }

                let now = Instant::now();
                let mut to_remove = Vec::new();

                {
                    let locks_guard = locks.read().await;
                    for (key, entry) in locks_guard.iter() {
                        if let Ok(mut entry_guard) = entry.try_write() {
                            if let Some(exp) = entry_guard.expires_at {
                                if exp <= now {
                                    entry_guard.writer = None;
                                    entry_guard.readers.clear();
                                    entry_guard.expires_at = None;

                                    if entry_guard.writer.is_none() && entry_guard.readers.is_empty() {
                                        to_remove.push(key.clone());
                                    }
                                }
                            }
                        }
                    }
                }

                if !to_remove.is_empty() {
                    let mut locks_guard = locks.write().await;
                    for key in to_remove {
                        locks_guard.remove(&key);
                    }
                }
            }
        });
    }

    /// write lock with TTL, support timeout, use LockRequest
    pub async fn lock_with_ttl_id(&self, request: &LockRequest) -> std::io::Result<bool> {
        let start = Instant::now();
        let expires_at = Some(Instant::now() + request.ttl);

        loop {
            // get or create lock entry
            let entry = {
                let mut locks_guard = self.locks.write().await;
                locks_guard
                    .entry(request.lock_id.clone())
                    .or_insert_with(|| {
                        Arc::new(RwLock::new(LocalLockEntry {
                            writer: None,
                            readers: HashMap::new(),
                            expires_at: None,
                        }))
                    })
                    .clone()
            };

            // try to get write lock to modify state
            if let Ok(mut entry_guard) = entry.try_write() {
                // check expired state
                let now = Instant::now();
                if let Some(exp) = entry_guard.expires_at {
                    if exp <= now {
                        entry_guard.writer = None;
                        entry_guard.readers.clear();
                        entry_guard.expires_at = None;
                    }
                }

                // check if can get write lock
                if entry_guard.writer.is_none() && entry_guard.readers.is_empty() {
                    entry_guard.writer = Some(request.owner.clone());
                    entry_guard.expires_at = expires_at;
                    tracing::debug!("Write lock acquired for resource '{}' by owner '{}'", request.resource, request.owner);
                    return Ok(true);
                }
            }

            if start.elapsed() >= request.acquire_timeout {
                return Ok(false);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// read lock with TTL, support timeout, use LockRequest
    pub async fn rlock_with_ttl_id(&self, request: &LockRequest) -> std::io::Result<bool> {
        let start = Instant::now();
        let expires_at = Some(Instant::now() + request.ttl);

        loop {
            // get or create lock entry
            let entry = {
                let mut locks_guard = self.locks.write().await;
                locks_guard
                    .entry(request.lock_id.clone())
                    .or_insert_with(|| {
                        Arc::new(RwLock::new(LocalLockEntry {
                            writer: None,
                            readers: HashMap::new(),
                            expires_at: None,
                        }))
                    })
                    .clone()
            };

            // try to get write lock to modify state
            if let Ok(mut entry_guard) = entry.try_write() {
                // check expired state
                let now = Instant::now();
                if let Some(exp) = entry_guard.expires_at {
                    if exp <= now {
                        entry_guard.writer = None;
                        entry_guard.readers.clear();
                        entry_guard.expires_at = None;
                    }
                }

                // check if can get read lock
                if entry_guard.writer.is_none() {
                    // increase read lock count
                    *entry_guard.readers.entry(request.owner.clone()).or_insert(0) += 1;
                    entry_guard.expires_at = expires_at;
                    tracing::debug!("Read lock acquired for resource '{}' by owner '{}'", request.resource, request.owner);
                    return Ok(true);
                }
            }

            if start.elapsed() >= request.acquire_timeout {
                return Ok(false);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// unlock by LockId and owner - need to specify owner to correctly unlock
    pub async fn unlock_by_id_and_owner(&self, lock_id: &crate::types::LockId, owner: &str) -> std::io::Result<()> {
        println!("Unlocking lock_id: {lock_id:?}, owner: {owner}");
        let mut need_remove = false;

        {
            let locks_guard = self.locks.read().await;
            if let Some(entry) = locks_guard.get(lock_id) {
                println!("Found lock entry, attempting to acquire write lock...");
                match entry.try_write() {
                    Ok(mut entry_guard) => {
                        println!("Successfully acquired write lock for unlock");
                        // try to release write lock
                        if entry_guard.writer.as_ref() == Some(&owner.to_string()) {
                            println!("Releasing write lock for owner: {owner}");
                            entry_guard.writer = None;
                        }
                        // try to release read lock
                        else if let Some(count) = entry_guard.readers.get_mut(owner) {
                            println!("Releasing read lock for owner: {owner} (count: {count})");
                            *count -= 1;
                            if *count == 0 {
                                entry_guard.readers.remove(owner);
                                println!("Removed owner {owner} from readers");
                            }
                        } else {
                            println!("Owner {owner} not found in writers or readers");
                        }
                        // check if need to remove
                        if entry_guard.readers.is_empty() && entry_guard.writer.is_none() {
                            println!("Lock entry is empty, marking for removal");
                            entry_guard.expires_at = None;
                            need_remove = true;
                        } else {
                            println!(
                                "Lock entry still has content: writer={:?}, readers={:?}",
                                entry_guard.writer, entry_guard.readers
                            );
                        }
                    }
                    Err(_) => {
                        println!("Failed to acquire write lock for unlock - this is the problem!");
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::WouldBlock,
                            "Failed to acquire write lock for unlock",
                        ));
                    }
                }
            } else {
                println!("Lock entry not found for lock_id: {lock_id:?}");
            }
        }

        // only here, entry's Ref is really dropped, can safely remove
        if need_remove {
            println!("Removing lock entry from map...");
            let mut locks_guard = self.locks.write().await;
            let removed = locks_guard.remove(lock_id);
            println!("Lock entry removed: {:?}", removed.is_some());
        }
        println!("Unlock operation completed");
        Ok(())
    }

    /// unlock by LockId - smart release (compatible with old interface, but may be inaccurate)
    pub async fn unlock_by_id(&self, lock_id: &crate::types::LockId) -> std::io::Result<()> {
        let mut need_remove = false;

        {
            let locks_guard = self.locks.read().await;
            if let Some(entry) = locks_guard.get(lock_id) {
                if let Ok(mut entry_guard) = entry.try_write() {
                    // release write lock first
                    if entry_guard.writer.is_some() {
                        entry_guard.writer = None;
                    }
                    // if no write lock, release first read lock
                    else if let Some((owner, _)) = entry_guard.readers.iter().next() {
                        let owner = owner.clone();
                        if let Some(count) = entry_guard.readers.get_mut(&owner) {
                            *count -= 1;
                            if *count == 0 {
                                entry_guard.readers.remove(&owner);
                            }
                        }
                    }

                    // if completely idle, clean entry
                    if entry_guard.readers.is_empty() && entry_guard.writer.is_none() {
                        entry_guard.expires_at = None;
                        need_remove = true;
                    }
                }
            }
        }

        if need_remove {
            let mut locks_guard = self.locks.write().await;
            locks_guard.remove(lock_id);
        }
        Ok(())
    }

    /// runlock by LockId and owner - need to specify owner to correctly unlock read lock
    pub async fn runlock_by_id_and_owner(&self, lock_id: &crate::types::LockId, owner: &str) -> std::io::Result<()> {
        let mut need_remove = false;

        {
            let locks_guard = self.locks.read().await;
            if let Some(entry) = locks_guard.get(lock_id) {
                if let Ok(mut entry_guard) = entry.try_write() {
                    // release read lock
                    if let Some(count) = entry_guard.readers.get_mut(owner) {
                        *count -= 1;
                        if *count == 0 {
                            entry_guard.readers.remove(owner);
                        }
                    }

                    // if completely idle, clean entry
                    if entry_guard.readers.is_empty() && entry_guard.writer.is_none() {
                        entry_guard.expires_at = None;
                        need_remove = true;
                    }
                }
            }
        }

        if need_remove {
            let mut locks_guard = self.locks.write().await;
            locks_guard.remove(lock_id);
        }
        Ok(())
    }

    /// runlock by LockId - smart release read lock (compatible with old interface)
    pub async fn runlock_by_id(&self, lock_id: &crate::types::LockId) -> std::io::Result<()> {
        let mut need_remove = false;

        {
            let locks_guard = self.locks.read().await;
            if let Some(entry) = locks_guard.get(lock_id) {
                if let Ok(mut entry_guard) = entry.try_write() {
                    // release first read lock
                    if let Some((owner, _)) = entry_guard.readers.iter().next() {
                        let owner = owner.clone();
                        if let Some(count) = entry_guard.readers.get_mut(&owner) {
                            *count -= 1;
                            if *count == 0 {
                                entry_guard.readers.remove(&owner);
                            }
                        }
                    }

                    // if completely idle, clean entry
                    if entry_guard.readers.is_empty() && entry_guard.writer.is_none() {
                        entry_guard.expires_at = None;
                        need_remove = true;
                    }
                }
            }
        }

        if need_remove {
            let mut locks_guard = self.locks.write().await;
            locks_guard.remove(lock_id);
        }
        Ok(())
    }

    /// check if resource is locked
    pub async fn is_locked(&self, resource: &str) -> bool {
        let lock_id = crate::types::LockId::new_deterministic(resource);
        let locks_guard = self.locks.read().await;
        if let Some(entry) = locks_guard.get(&lock_id) {
            let entry_guard = entry.read().await;
            entry_guard.writer.is_some() || !entry_guard.readers.is_empty()
        } else {
            false
        }
    }

    /// get lock info for a resource
    pub async fn get_lock(&self, resource: &str) -> Option<crate::types::LockInfo> {
        let lock_id = crate::types::LockId::new_deterministic(resource);
        let locks_guard = self.locks.read().await;
        if let Some(entry) = locks_guard.get(&lock_id) {
            let entry_guard = entry.read().await;

            if let Some(owner) = &entry_guard.writer {
                Some(crate::types::LockInfo {
                    id: lock_id,
                    resource: resource.to_string(),
                    lock_type: crate::types::LockType::Exclusive,
                    status: crate::types::LockStatus::Acquired,
                    owner: owner.clone(),
                    acquired_at: std::time::SystemTime::now(),
                    expires_at: std::time::SystemTime::now() + std::time::Duration::from_secs(30),
                    last_refreshed: std::time::SystemTime::now(),
                    metadata: crate::types::LockMetadata::default(),
                    priority: crate::types::LockPriority::Normal,
                    wait_start_time: None,
                })
            } else if !entry_guard.readers.is_empty() {
                let owner = entry_guard.readers.keys().next().unwrap().clone();
                Some(crate::types::LockInfo {
                    id: lock_id,
                    resource: resource.to_string(),
                    lock_type: crate::types::LockType::Shared,
                    status: crate::types::LockStatus::Acquired,
                    owner,
                    acquired_at: std::time::SystemTime::now(),
                    expires_at: std::time::SystemTime::now() + std::time::Duration::from_secs(30),
                    last_refreshed: std::time::SystemTime::now(),
                    metadata: crate::types::LockMetadata::default(),
                    priority: crate::types::LockPriority::Normal,
                    wait_start_time: None,
                })
            } else {
                None
            }
        } else {
            None
        }
    }

    /// get statistics
    pub async fn get_stats(&self) -> crate::types::LockStats {
        let mut stats = crate::types::LockStats::default();
        let locks_guard = self.locks.read().await;

        for (_, entry) in locks_guard.iter() {
            let entry_guard = entry.read().await;
            if entry_guard.writer.is_some() {
                stats.exclusive_locks += 1;
            }
            stats.shared_locks += entry_guard.readers.len();
        }

        stats.total_locks = stats.exclusive_locks + stats.shared_locks;
        stats
    }

    /// shutdown background tasks
    pub async fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::task;
    use tokio::time::{sleep, timeout};

    /// Test basic write lock operations
    #[tokio::test]
    async fn test_write_lock_basic() {
        let lock_map = LocalLockMap::new();

        // create a simple lock request
        let request = LockRequest {
            lock_id: crate::types::LockId::new_deterministic("test_resource"),
            resource: "test_resource".to_string(),
            lock_type: crate::types::LockType::Exclusive,
            owner: "test_owner".to_string(),
            acquire_timeout: Duration::from_millis(100),
            ttl: Duration::from_millis(100),
            metadata: crate::types::LockMetadata::default(),
            priority: crate::types::LockPriority::Normal,
            deadlock_detection: false,
        };

        // try to acquire lock
        println!("Attempting to acquire lock...");
        let result = lock_map.lock_with_ttl_id(&request).await;
        println!("Lock acquisition result: {result:?}");

        match result {
            Ok(success) => {
                if success {
                    println!("Lock acquired successfully");
                    // check lock state
                    let is_locked = lock_map.is_locked("test_resource").await;
                    println!("Is locked: {is_locked}");

                    // try to unlock
                    println!("Attempting to unlock...");
                    let unlock_result = lock_map.unlock_by_id_and_owner(&request.lock_id, "test_owner").await;
                    println!("Unlock result: {unlock_result:?}");

                    // check lock state again
                    let is_locked_after = lock_map.is_locked("test_resource").await;
                    println!("Is locked after unlock: {is_locked_after}");

                    assert!(!is_locked_after, "Should be unlocked after release");
                } else {
                    println!("Lock acquisition failed (timeout)");
                }
            }
            Err(e) => {
                println!("Lock acquisition error: {e:?}");
                panic!("Lock acquisition failed with error: {e:?}");
            }
        }
    }

    /// Test basic read lock operations  
    #[tokio::test]
    async fn test_read_lock_basic() {
        let lock_map = LocalLockMap::new();

        // Test successful acquisition
        let request = LockRequest {
            lock_id: crate::types::LockId::new_deterministic("bar"),
            resource: "bar".to_string(),
            lock_type: crate::types::LockType::Shared,
            owner: "reader1".to_string(),
            acquire_timeout: Duration::from_millis(100),
            ttl: Duration::from_millis(100),
            metadata: crate::types::LockMetadata::default(),
            priority: crate::types::LockPriority::Normal,
            deadlock_detection: false,
        };

        let ok = lock_map.rlock_with_ttl_id(&request).await.unwrap();
        assert!(ok, "Read lock should be successfully acquired");
        assert!(lock_map.is_locked("bar").await, "Lock state should be locked");

        // Test lock info
        let lock_info = lock_map.get_lock("bar").await;
        assert!(lock_info.is_some(), "Lock info should exist");
        let info = lock_info.unwrap();
        assert_eq!(info.owner, "reader1");
        assert_eq!(info.lock_type, crate::types::LockType::Shared);

        // Test unlock with owner
        lock_map.runlock_by_id_and_owner(&request.lock_id, "reader1").await.unwrap();
        assert!(!lock_map.is_locked("bar").await, "Should be unlocked after release");
    }

    /// Test write lock mutual exclusion
    #[tokio::test]
    async fn test_write_lock_mutex() {
        let lock_map = Arc::new(LocalLockMap::new());

        // Owner1 acquires write lock
        let request1 = LockRequest {
            lock_id: crate::types::LockId::new_deterministic("res_mutex_test"),
            resource: "res_mutex_test".to_string(),
            lock_type: crate::types::LockType::Exclusive,
            owner: "owner1".to_string(),
            acquire_timeout: Duration::from_millis(100),
            ttl: Duration::from_millis(100),
            metadata: crate::types::LockMetadata::default(),
            priority: crate::types::LockPriority::Normal,
            deadlock_detection: false,
        };

        let ok = lock_map.lock_with_ttl_id(&request1).await.unwrap();
        assert!(ok, "First write lock should succeed");

        // Owner2 tries to acquire write lock on same resource - should fail due to timeout
        let lock_map2 = lock_map.clone();
        let request2 = LockRequest {
            lock_id: crate::types::LockId::new_deterministic("res_mutex_test"),
            resource: "res_mutex_test".to_string(),
            lock_type: crate::types::LockType::Exclusive,
            owner: "owner2".to_string(),
            acquire_timeout: Duration::from_millis(50),
            ttl: Duration::from_millis(50),
            metadata: crate::types::LockMetadata::default(),
            priority: crate::types::LockPriority::Normal,
            deadlock_detection: false,
        };

        let request2_clone = request2.clone();
        let result = timeout(Duration::from_millis(100), async move {
            lock_map2.lock_with_ttl_id(&request2_clone).await.unwrap()
        })
        .await;

        assert!(result.is_ok(), "Lock attempt should complete");
        assert!(!result.unwrap(), "Second write lock should fail due to conflict");

        // Release first lock
        lock_map.unlock_by_id_and_owner(&request1.lock_id, "owner1").await.unwrap();

        // Now owner2 should be able to acquire the lock
        let ok = lock_map.lock_with_ttl_id(&request2).await.unwrap();
        assert!(ok, "Write lock should succeed after first is released");
        lock_map.unlock_by_id_and_owner(&request2.lock_id, "owner2").await.unwrap();
    }

    /// Test read lock sharing
    #[tokio::test]
    async fn test_read_lock_sharing() {
        let lock_map = LocalLockMap::new();

        // Multiple readers should be able to acquire read locks
        let request1 = LockRequest {
            lock_id: crate::types::LockId::new_deterministic("res_sharing_test"),
            resource: "res_sharing_test".to_string(),
            lock_type: crate::types::LockType::Shared,
            owner: "reader1".to_string(),
            acquire_timeout: Duration::from_millis(100),
            ttl: Duration::from_millis(100),
            metadata: crate::types::LockMetadata::default(),
            priority: crate::types::LockPriority::Normal,
            deadlock_detection: false,
        };

        let request2 = LockRequest {
            lock_id: crate::types::LockId::new_deterministic("res_sharing_test"),
            resource: "res_sharing_test".to_string(),
            lock_type: crate::types::LockType::Shared,
            owner: "reader2".to_string(),
            acquire_timeout: Duration::from_millis(100),
            ttl: Duration::from_millis(100),
            metadata: crate::types::LockMetadata::default(),
            priority: crate::types::LockPriority::Normal,
            deadlock_detection: false,
        };

        let request3 = LockRequest {
            lock_id: crate::types::LockId::new_deterministic("res_sharing_test"),
            resource: "res_sharing_test".to_string(),
            lock_type: crate::types::LockType::Shared,
            owner: "reader3".to_string(),
            acquire_timeout: Duration::from_millis(100),
            ttl: Duration::from_millis(100),
            metadata: crate::types::LockMetadata::default(),
            priority: crate::types::LockPriority::Normal,
            deadlock_detection: false,
        };

        let ok1 = lock_map.rlock_with_ttl_id(&request1).await.unwrap();
        let ok2 = lock_map.rlock_with_ttl_id(&request2).await.unwrap();
        let ok3 = lock_map.rlock_with_ttl_id(&request3).await.unwrap();
        assert!(ok1 && ok2 && ok3, "All read locks should succeed");
        assert!(lock_map.is_locked("res_sharing_test").await, "Resource should be locked");

        // Release readers one by one
        lock_map.runlock_by_id_and_owner(&request1.lock_id, "reader1").await.unwrap();
        assert!(
            lock_map.is_locked("res_sharing_test").await,
            "Should still be locked with remaining readers"
        );

        lock_map.runlock_by_id_and_owner(&request2.lock_id, "reader2").await.unwrap();
        assert!(lock_map.is_locked("res_sharing_test").await, "Should still be locked with one reader");

        lock_map.runlock_by_id_and_owner(&request3.lock_id, "reader3").await.unwrap();
        assert!(
            !lock_map.is_locked("res_sharing_test").await,
            "Should be unlocked when all readers release"
        );
    }

    /// Test read-write lock exclusion
    #[tokio::test]
    async fn test_read_write_exclusion() {
        let lock_map = LocalLockMap::new();

        // Reader acquires read lock
        let read_request = LockRequest {
            lock_id: crate::types::LockId::new_deterministic("res_rw_test"),
            resource: "res_rw_test".to_string(),
            lock_type: crate::types::LockType::Shared,
            owner: "reader1".to_string(),
            acquire_timeout: Duration::from_millis(100),
            ttl: Duration::from_millis(100),
            metadata: crate::types::LockMetadata::default(),
            priority: crate::types::LockPriority::Normal,
            deadlock_detection: false,
        };

        let ok = lock_map.rlock_with_ttl_id(&read_request).await.unwrap();
        assert!(ok, "Read lock should succeed");

        // Writer tries to acquire write lock - should fail
        let write_request = LockRequest {
            lock_id: crate::types::LockId::new_deterministic("res_rw_test"),
            resource: "res_rw_test".to_string(),
            lock_type: crate::types::LockType::Exclusive,
            owner: "writer1".to_string(),
            acquire_timeout: Duration::from_millis(50),
            ttl: Duration::from_millis(50),
            metadata: crate::types::LockMetadata::default(),
            priority: crate::types::LockPriority::Normal,
            deadlock_detection: false,
        };

        let result = timeout(Duration::from_millis(100), async {
            lock_map.lock_with_ttl_id(&write_request).await.unwrap()
        })
        .await;

        assert!(result.is_ok(), "Write lock attempt should complete");
        assert!(!result.unwrap(), "Write lock should fail when read lock is held");

        // Release read lock
        lock_map
            .runlock_by_id_and_owner(&read_request.lock_id, "reader1")
            .await
            .unwrap();

        // Now writer should be able to acquire the lock with longer TTL
        let write_request_long_ttl = LockRequest {
            lock_id: crate::types::LockId::new_deterministic("res_rw_test"),
            resource: "res_rw_test".to_string(),
            lock_type: crate::types::LockType::Exclusive,
            owner: "writer1".to_string(),
            acquire_timeout: Duration::from_millis(100),
            ttl: Duration::from_millis(200), // Longer TTL to prevent expiration during test
            metadata: crate::types::LockMetadata::default(),
            priority: crate::types::LockPriority::Normal,
            deadlock_detection: false,
        };
        let ok = lock_map.lock_with_ttl_id(&write_request_long_ttl).await.unwrap();
        assert!(ok, "Write lock should succeed after read lock is released");

        // Reader tries to acquire read lock while write lock is held - should fail
        let read_request2 = LockRequest {
            lock_id: crate::types::LockId::new_deterministic("res_rw_test"),
            resource: "res_rw_test".to_string(),
            lock_type: crate::types::LockType::Shared,
            owner: "reader2".to_string(),
            acquire_timeout: Duration::from_millis(50),
            ttl: Duration::from_millis(50),
            metadata: crate::types::LockMetadata::default(),
            priority: crate::types::LockPriority::Normal,
            deadlock_detection: false,
        };

        let result = timeout(Duration::from_millis(100), async {
            lock_map.rlock_with_ttl_id(&read_request2).await.unwrap()
        })
        .await;

        assert!(result.is_ok(), "Read lock attempt should complete");
        assert!(!result.unwrap(), "Read lock should fail when write lock is held");

        // Release write lock
        lock_map
            .unlock_by_id_and_owner(&write_request_long_ttl.lock_id, "writer1")
            .await
            .unwrap();
    }

    /// Test statistics
    #[tokio::test]
    async fn test_statistics() {
        let lock_map = LocalLockMap::new();

        // Initially no locks
        let stats = lock_map.get_stats().await;
        assert_eq!(stats.total_locks, 0, "Should have no locks initially");
        assert_eq!(stats.exclusive_locks, 0, "Should have no exclusive locks initially");
        assert_eq!(stats.shared_locks, 0, "Should have no shared locks initially");

        // Add some locks
        let write_request = LockRequest {
            lock_id: crate::types::LockId::new_deterministic("res1_stats_test"),
            resource: "res1_stats_test".to_string(),
            lock_type: crate::types::LockType::Exclusive,
            owner: "owner1".to_string(),
            acquire_timeout: Duration::from_millis(100),
            ttl: Duration::from_millis(100),
            metadata: crate::types::LockMetadata::default(),
            priority: crate::types::LockPriority::Normal,
            deadlock_detection: false,
        };

        let read_request1 = LockRequest {
            lock_id: crate::types::LockId::new_deterministic("res2_stats_test"),
            resource: "res2_stats_test".to_string(),
            lock_type: crate::types::LockType::Shared,
            owner: "reader1".to_string(),
            acquire_timeout: Duration::from_millis(100),
            ttl: Duration::from_millis(100),
            metadata: crate::types::LockMetadata::default(),
            priority: crate::types::LockPriority::Normal,
            deadlock_detection: false,
        };

        let read_request2 = LockRequest {
            lock_id: crate::types::LockId::new_deterministic("res2_stats_test"),
            resource: "res2_stats_test".to_string(),
            lock_type: crate::types::LockType::Shared,
            owner: "reader2".to_string(),
            acquire_timeout: Duration::from_millis(100),
            ttl: Duration::from_millis(100),
            metadata: crate::types::LockMetadata::default(),
            priority: crate::types::LockPriority::Normal,
            deadlock_detection: false,
        };

        lock_map.lock_with_ttl_id(&write_request).await.unwrap();
        lock_map.rlock_with_ttl_id(&read_request1).await.unwrap();
        lock_map.rlock_with_ttl_id(&read_request2).await.unwrap();

        let stats = lock_map.get_stats().await;
        assert_eq!(stats.exclusive_locks, 1, "Should have 1 exclusive lock");
        assert_eq!(stats.shared_locks, 2, "Should have 2 shared locks");
        assert_eq!(stats.total_locks, 3, "Should have 3 total locks");

        // Clean up
        lock_map
            .unlock_by_id_and_owner(&write_request.lock_id, "owner1")
            .await
            .unwrap();
        lock_map
            .runlock_by_id_and_owner(&read_request1.lock_id, "reader1")
            .await
            .unwrap();
        lock_map
            .runlock_by_id_and_owner(&read_request2.lock_id, "reader2")
            .await
            .unwrap();
    }

    /// Test concurrent access
    #[tokio::test]
    async fn test_concurrent_access() {
        let lock_map = Arc::new(LocalLockMap::new());
        let num_tasks = 10;
        let num_iterations = 100;

        let mut handles = Vec::new();

        for i in 0..num_tasks {
            let lock_map = lock_map.clone();
            let owner = format!("owner{i}");
            let handle = task::spawn(async move {
                for j in 0..num_iterations {
                    let resource = format!("resource{}", j % 5);
                    let request = LockRequest {
                        lock_id: crate::types::LockId::new_deterministic(&resource),
                        resource: resource.clone(),
                        lock_type: if j % 2 == 0 {
                            crate::types::LockType::Exclusive
                        } else {
                            crate::types::LockType::Shared
                        },
                        owner: owner.clone(),
                        acquire_timeout: Duration::from_millis(10),
                        ttl: Duration::from_millis(10),
                        metadata: crate::types::LockMetadata::default(),
                        priority: crate::types::LockPriority::Normal,
                        deadlock_detection: false,
                    };

                    if request.lock_type == crate::types::LockType::Exclusive {
                        if lock_map.lock_with_ttl_id(&request).await.unwrap() {
                            sleep(Duration::from_micros(100)).await;
                            lock_map.unlock_by_id_and_owner(&request.lock_id, &owner).await.unwrap();
                        }
                    } else if lock_map.rlock_with_ttl_id(&request).await.unwrap() {
                        sleep(Duration::from_micros(100)).await;
                        lock_map.runlock_by_id_and_owner(&request.lock_id, &owner).await.unwrap();
                    }
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.await.unwrap();
        }

        // Verify no locks remain
        let stats = lock_map.get_stats().await;
        assert_eq!(stats.total_locks, 0, "No locks should remain after concurrent access");
    }

    #[tokio::test]
    async fn test_write_lock_timeout_and_reacquire() {
        let lock_map = LocalLockMap::new();

        // 1. acquire lock
        let request = LockRequest {
            lock_id: crate::types::LockId::new_deterministic("timeout_resource"),
            resource: "timeout_resource".to_string(),
            lock_type: crate::types::LockType::Exclusive,
            owner: "owner1".to_string(),
            acquire_timeout: Duration::from_millis(100),
            ttl: Duration::from_millis(200),
            metadata: crate::types::LockMetadata::default(),
            priority: crate::types::LockPriority::Normal,
            deadlock_detection: false,
        };

        let ok = lock_map.lock_with_ttl_id(&request).await.unwrap();
        assert!(ok, "First lock should succeed");

        // 2. try to acquire lock again, should fail
        let request2 = LockRequest {
            lock_id: crate::types::LockId::new_deterministic("timeout_resource"),
            resource: "timeout_resource".to_string(),
            lock_type: crate::types::LockType::Exclusive,
            owner: "owner2".to_string(),
            acquire_timeout: Duration::from_millis(100),
            ttl: Duration::from_millis(200),
            metadata: crate::types::LockMetadata::default(),
            priority: crate::types::LockPriority::Normal,
            deadlock_detection: false,
        };
        let ok2 = lock_map.lock_with_ttl_id(&request2).await.unwrap();
        assert!(!ok2, "Second lock should fail before timeout");

        // 3. wait for TTL to expire
        tokio::time::sleep(Duration::from_millis(300)).await;

        // 4. try to acquire lock again, should succeed
        let ok3 = lock_map.lock_with_ttl_id(&request2).await.unwrap();
        assert!(ok3, "Lock should succeed after timeout");
    }
}
