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

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, Notify, RwLock};

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
    /// number of writers waiting (for simple fairness against reader storms)
    pub writer_pending: usize,
    /// notifiers for readers/writers
    pub notify_readers: Arc<Notify>,
    pub notify_writers: Arc<Notify>,
}

/// local lock map
#[derive(Debug)]
pub struct LocalLockMap {
    /// LockId to lock object map
    pub locks: Arc<RwLock<HashMap<crate::types::LockId, Arc<RwLock<LocalLockEntry>>>>>,
    /// Shutdown flag for background tasks
    shutdown: Arc<AtomicBool>,
    /// expiration schedule map: when -> lock_ids
    expirations: Arc<Mutex<BTreeMap<Instant, Vec<crate::types::LockId>>>>,
    /// notify expiry task when new earlier deadline arrives
    exp_notify: Arc<Notify>,
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
            expirations: Arc::new(Mutex::new(BTreeMap::new())),
            exp_notify: Arc::new(Notify::new()),
        };
        map.spawn_expiry_task();
        map
    }

    /// spawn expiry task to clean up expired locks
    fn spawn_expiry_task(&self) {
        let locks = self.locks.clone();
        let shutdown = self.shutdown.clone();
        let expirations = self.expirations.clone();
        let exp_notify = self.exp_notify.clone();
        tokio::spawn(async move {
            loop {
                if shutdown.load(Ordering::Relaxed) {
                    tracing::debug!("Expiry task shutting down");
                    break;
                }

                // Find next deadline and drain due ids
                let (due_ids, wait_duration) = {
                    let mut due = Vec::new();
                    let mut guard = expirations.lock().await;
                    let now = Instant::now();
                    let next_deadline = guard.first_key_value().map(|(k, _)| *k);
                    // drain all <= now
                    let mut keys_to_remove = Vec::new();
                    for (k, v) in guard.range(..=now).map(|(k, v)| (*k, v.clone())) {
                        due.extend(v);
                        keys_to_remove.push(k);
                    }
                    for k in keys_to_remove {
                        guard.remove(&k);
                    }
                    let wait = if due.is_empty() {
                        next_deadline.and_then(|dl| {
                            if dl > now {
                                Some(dl - now)
                            } else {
                                Some(Duration::from_millis(0))
                            }
                        })
                    } else {
                        Some(Duration::from_millis(0))
                    };
                    (due, wait)
                };

                if !due_ids.is_empty() {
                    // process due ids without holding the map lock during awaits
                    let now = Instant::now();
                    // collect entries to process
                    let entries: Vec<(crate::types::LockId, Arc<RwLock<LocalLockEntry>>)> = {
                        let locks_guard = locks.read().await;
                        due_ids
                            .into_iter()
                            .filter_map(|id| locks_guard.get(&id).cloned().map(|e| (id, e)))
                            .collect()
                    };

                    let mut to_remove = Vec::new();
                    for (lock_id, entry) in entries {
                        let mut entry_guard = entry.write().await;
                        if let Some(exp) = entry_guard.expires_at {
                            if exp <= now {
                                entry_guard.writer = None;
                                entry_guard.readers.clear();
                                entry_guard.expires_at = None;
                                entry_guard.notify_writers.notify_waiters();
                                entry_guard.notify_readers.notify_waiters();
                                if entry_guard.writer.is_none() && entry_guard.readers.is_empty() {
                                    to_remove.push(lock_id);
                                }
                            }
                        }
                    }
                    if !to_remove.is_empty() {
                        let mut locks_w = locks.write().await;
                        for id in to_remove {
                            let _ = locks_w.remove(&id);
                        }
                    }
                    continue; // immediately look for next
                }

                // nothing due; wait for next deadline or notification
                if let Some(dur) = wait_duration {
                    tokio::select! {
                        _ = tokio::time::sleep(dur) => {},
                        _ = exp_notify.notified() => {},
                    }
                } else {
                    // no deadlines, wait for new schedule or shutdown tick
                    exp_notify.notified().await;
                }
            }
        });
    }

    /// schedule an expiry time for the given lock id (inline, avoid per-acquisition spawn)
    async fn schedule_expiry(&self, id: crate::types::LockId, exp: Instant) {
        let mut guard = self.expirations.lock().await;
        let is_earliest = match guard.first_key_value() {
            Some((k, _)) => exp < *k,
            None => true,
        };
        guard.entry(exp).or_insert_with(Vec::new).push(id);
        drop(guard);
        if is_earliest {
            self.exp_notify.notify_waiters();
        }
    }

    /// write lock with TTL, support timeout, use LockRequest
    pub async fn lock_with_ttl_id(&self, request: &LockRequest) -> std::io::Result<bool> {
        let start = Instant::now();

        loop {
            // get or create lock entry (double-checked to reduce write-lock contention)
            let entry = if let Some(e) = {
                let locks_guard = self.locks.read().await;
                locks_guard.get(&request.lock_id).cloned()
            } {
                e
            } else {
                let mut locks_guard = self.locks.write().await;
                locks_guard
                    .entry(request.lock_id.clone())
                    .or_insert_with(|| {
                        Arc::new(RwLock::new(LocalLockEntry {
                            writer: None,
                            readers: HashMap::new(),
                            expires_at: None,
                            writer_pending: 0,
                            notify_readers: Arc::new(Notify::new()),
                            notify_writers: Arc::new(Notify::new()),
                        }))
                    })
                    .clone()
            };

            // attempt acquisition or wait using Notify
            let notify_to_wait = {
                let mut entry_guard = entry.write().await;
                // check expired state
                let now = Instant::now();
                if let Some(exp) = entry_guard.expires_at {
                    if exp <= now {
                        entry_guard.writer = None;
                        entry_guard.readers.clear();
                        entry_guard.expires_at = None;
                    }
                }

                // try acquire
                if entry_guard.writer.is_none() && entry_guard.readers.is_empty() {
                    entry_guard.writer = Some(request.owner.clone());
                    let expires_at = Instant::now() + request.ttl;
                    entry_guard.expires_at = Some(expires_at);
                    tracing::debug!("Write lock acquired for resource '{}' by owner '{}'", request.resource, request.owner);
                    {
                        drop(entry_guard);
                        self.schedule_expiry(request.lock_id.clone(), expires_at).await;
                    }
                    return Ok(true);
                }
                // couldn't acquire now, mark as pending writer and choose notifier
                entry_guard.writer_pending = entry_guard.writer_pending.saturating_add(1);
                entry_guard.notify_writers.clone()
            };

            // wait with remaining timeout
            let elapsed = start.elapsed();
            if elapsed >= request.acquire_timeout {
                // best-effort decrement pending counter
                if let Ok(mut eg) = entry.try_write() {
                    eg.writer_pending = eg.writer_pending.saturating_sub(1);
                } else {
                    let mut eg = entry.write().await;
                    eg.writer_pending = eg.writer_pending.saturating_sub(1);
                }
                return Ok(false);
            }
            let remaining = request.acquire_timeout - elapsed;
            if tokio::time::timeout(remaining, notify_to_wait.notified()).await.is_err() {
                // timeout; decrement pending before returning
                if let Ok(mut eg) = entry.try_write() {
                    eg.writer_pending = eg.writer_pending.saturating_sub(1);
                } else {
                    let mut eg = entry.write().await;
                    eg.writer_pending = eg.writer_pending.saturating_sub(1);
                }
                return Ok(false);
            }
            // woke up; decrement pending before retrying
            if let Ok(mut eg) = entry.try_write() {
                eg.writer_pending = eg.writer_pending.saturating_sub(1);
            } else {
                let mut eg = entry.write().await;
                eg.writer_pending = eg.writer_pending.saturating_sub(1);
            }
        }
    }

    /// read lock with TTL, support timeout, use LockRequest
    pub async fn rlock_with_ttl_id(&self, request: &LockRequest) -> std::io::Result<bool> {
        let start = Instant::now();

        loop {
            // get or create lock entry (double-checked to reduce write-lock contention)
            let entry = if let Some(e) = {
                let locks_guard = self.locks.read().await;
                locks_guard.get(&request.lock_id).cloned()
            } {
                e
            } else {
                let mut locks_guard = self.locks.write().await;
                locks_guard
                    .entry(request.lock_id.clone())
                    .or_insert_with(|| {
                        Arc::new(RwLock::new(LocalLockEntry {
                            writer: None,
                            readers: HashMap::new(),
                            expires_at: None,
                            writer_pending: 0,
                            notify_readers: Arc::new(Notify::new()),
                            notify_writers: Arc::new(Notify::new()),
                        }))
                    })
                    .clone()
            };

            // attempt acquisition or wait using Notify
            let notify_to_wait = {
                let mut entry_guard = entry.write().await;
                // check expired state
                let now = Instant::now();
                if let Some(exp) = entry_guard.expires_at {
                    if exp <= now {
                        entry_guard.writer = None;
                        entry_guard.readers.clear();
                        entry_guard.expires_at = None;
                    }
                }

                if entry_guard.writer.is_none() && entry_guard.writer_pending == 0 {
                    *entry_guard.readers.entry(request.owner.clone()).or_insert(0) += 1;
                    let expires_at = Instant::now() + request.ttl;
                    entry_guard.expires_at = Some(expires_at);
                    tracing::debug!("Read lock acquired for resource '{}' by owner '{}'", request.resource, request.owner);
                    {
                        drop(entry_guard);
                        self.schedule_expiry(request.lock_id.clone(), expires_at).await;
                    }
                    return Ok(true);
                }

                // choose notifier: prefer waiting on writers if writers pending, else readers
                if entry_guard.writer_pending > 0 {
                    entry_guard.notify_writers.clone()
                } else {
                    entry_guard.notify_readers.clone()
                }
            };

            // wait with remaining timeout
            let elapsed = start.elapsed();
            if elapsed >= request.acquire_timeout {
                return Ok(false);
            }
            let remaining = request.acquire_timeout - elapsed;
            if tokio::time::timeout(remaining, notify_to_wait.notified()).await.is_err() {
                return Ok(false);
            }
        }
    }

    /// unlock by LockId and owner - need to specify owner to correctly unlock
    pub async fn unlock_by_id_and_owner(&self, lock_id: &crate::types::LockId, owner: &str) -> std::io::Result<()> {
        // first, get the entry without holding the write lock on the map
        let entry = {
            let locks_guard = self.locks.read().await;
            match locks_guard.get(lock_id) {
                Some(e) => e.clone(),
                None => return Err(std::io::Error::new(std::io::ErrorKind::NotFound, "Lock entry not found")),
            }
        };

        let mut need_remove = false;
        let (notify_writers, notify_readers, writer_pending, writer_none) = {
            let mut entry_guard = entry.write().await;

            // try to release write lock
            if entry_guard.writer.as_ref() == Some(&owner.to_string()) {
                entry_guard.writer = None;
            }
            // try to release read lock
            else if let Some(count) = entry_guard.readers.get_mut(owner) {
                *count -= 1;
                if *count == 0 {
                    entry_guard.readers.remove(owner);
                }
            } else {
                // owner not found, treat as no-op
            }

            // check if need to remove
            if entry_guard.readers.is_empty() && entry_guard.writer.is_none() {
                entry_guard.expires_at = None;
                need_remove = true;
            }

            // capture notifications and state
            (
                entry_guard.notify_writers.clone(),
                entry_guard.notify_readers.clone(),
                entry_guard.writer_pending,
                entry_guard.writer.is_none(),
            )
        };

        if writer_pending > 0 && writer_none {
            // Wake a single writer to preserve fairness and avoid thundering herd
            notify_writers.notify_one();
        } else if writer_none {
            // No writers waiting, allow readers to proceed
            notify_readers.notify_waiters();
        }

        if need_remove {
            let mut locks_guard = self.locks.write().await;
            let _ = locks_guard.remove(lock_id);
        }
        Ok(())
    }

    /// unlock by LockId - smart release (compatible with old interface, but may be inaccurate)
    pub async fn unlock_by_id(&self, lock_id: &crate::types::LockId) -> std::io::Result<()> {
        let entry = {
            let locks_guard = self.locks.read().await;
            match locks_guard.get(lock_id) {
                Some(e) => e.clone(),
                None => return Ok(()), // nothing to do
            }
        };

        let mut need_remove = false;
        let (notify_writers, notify_readers, writer_pending, writer_none) = {
            let mut entry_guard = entry.write().await;

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

            if entry_guard.readers.is_empty() && entry_guard.writer.is_none() {
                entry_guard.expires_at = None;
                need_remove = true;
            }

            (
                entry_guard.notify_writers.clone(),
                entry_guard.notify_readers.clone(),
                entry_guard.writer_pending,
                entry_guard.writer.is_none(),
            )
        };

        if writer_pending > 0 && writer_none {
            notify_writers.notify_one();
        } else if writer_none {
            notify_readers.notify_waiters();
        }

        if need_remove {
            let mut locks_guard = self.locks.write().await;
            let _ = locks_guard.remove(lock_id);
        }
        Ok(())
    }

    /// runlock by LockId and owner - need to specify owner to correctly unlock read lock
    pub async fn runlock_by_id_and_owner(&self, lock_id: &crate::types::LockId, owner: &str) -> std::io::Result<()> {
        let entry = {
            let locks_guard = self.locks.read().await;
            match locks_guard.get(lock_id) {
                Some(e) => e.clone(),
                None => return Ok(()),
            }
        };

        let mut need_remove = false;
        let (notify_writers, notify_readers, writer_pending, writer_none) = {
            let mut entry_guard = entry.write().await;

            // release read lock
            if let Some(count) = entry_guard.readers.get_mut(owner) {
                *count -= 1;
                if *count == 0 {
                    entry_guard.readers.remove(owner);
                }
            }

            if entry_guard.readers.is_empty() && entry_guard.writer.is_none() {
                entry_guard.expires_at = None;
                need_remove = true;
            }

            (
                entry_guard.notify_writers.clone(),
                entry_guard.notify_readers.clone(),
                entry_guard.writer_pending,
                entry_guard.writer.is_none(),
            )
        };

        if writer_pending > 0 && writer_none {
            notify_writers.notify_waiters();
        } else if writer_none {
            notify_readers.notify_waiters();
        }

        if need_remove {
            let mut locks_guard = self.locks.write().await;
            let _ = locks_guard.remove(lock_id);
        }
        Ok(())
    }

    /// runlock by LockId - smart release read lock (compatible with old interface)
    pub async fn runlock_by_id(&self, lock_id: &crate::types::LockId) -> std::io::Result<()> {
        let entry = {
            let locks_guard = self.locks.read().await;
            match locks_guard.get(lock_id) {
                Some(e) => e.clone(),
                None => return Ok(()),
            }
        };

        let mut need_remove = false;
        let (notify_writers, notify_readers, writer_pending, writer_none) = {
            let mut entry_guard = entry.write().await;

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

            if entry_guard.readers.is_empty() && entry_guard.writer.is_none() {
                entry_guard.expires_at = None;
                need_remove = true;
            }

            (
                entry_guard.notify_writers.clone(),
                entry_guard.notify_readers.clone(),
                entry_guard.writer_pending,
                entry_guard.writer.is_none(),
            )
        };

        if writer_pending > 0 && writer_none {
            notify_writers.notify_waiters();
        } else if writer_none {
            notify_readers.notify_waiters();
        }

        if need_remove {
            let mut locks_guard = self.locks.write().await;
            let _ = locks_guard.remove(lock_id);
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
