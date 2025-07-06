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

use dashmap::DashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

/// 本地锁条目
#[derive(Debug)]
pub struct LocalLockEntry {
    /// 当前写锁持有者
    pub writer: Option<String>,
    /// 当前读锁持有者集合
    pub readers: Vec<String>,
    /// 锁过期时间
    pub expires_at: Option<Instant>,
}

/// 本地锁映射管理器
///
/// 内部维护从资源到锁对象的映射表，使用DashMap实现高并发性能
#[derive(Debug)]
pub struct LocalLockMap {
    /// 资源锁映射表，key是唯一资源标识符，value是锁对象
    /// 使用DashMap实现分片锁以提高并发性能
    pub locks: Arc<DashMap<String, Arc<RwLock<LocalLockEntry>>>>,
    /// LockId 到 (resource, owner) 的映射
    pub lockid_map: Arc<DashMap<crate::types::LockId, (String, String)>>,
}

impl LocalLockMap {
    /// 创建新的本地锁管理器
    pub fn new() -> Self {
        let map = Self {
            locks: Arc::new(DashMap::new()),
            lockid_map: Arc::new(DashMap::new()),
        };
        map.spawn_expiry_task();
        map
    }

    /// 启动后台任务定期清理过期的锁
    fn spawn_expiry_task(&self) {
        let locks = self.locks.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            loop {
                interval.tick().await;
                let now = Instant::now();
                let mut to_remove = Vec::new();

                // DashMap的iter()方法提供并发安全的迭代
                for item in locks.iter() {
                    let mut entry_guard = item.value().write().await;
                    if let Some(exp) = entry_guard.expires_at {
                        if exp <= now {
                            // 清除锁内容
                            entry_guard.writer = None;
                            entry_guard.readers.clear();
                            entry_guard.expires_at = None;

                            // 如果条目完全为空，标记为删除
                            if entry_guard.writer.is_none() && entry_guard.readers.is_empty() {
                                to_remove.push(item.key().clone());
                            }
                        }
                    }
                }

                // 删除空条目
                for key in to_remove {
                    locks.remove(&key);
                }
            }
        });
    }

    /// 批量获取写锁
    ///
    /// 尝试在所有资源上获取写锁，如果任何资源锁定失败，回滚所有之前锁定的资源
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
            match self.lock_with_ttl_id(resource, owner, timeout, expires_at).await {
                Ok(true) => {
                    locked.push(resource.clone());
                }
                Ok(false) => {
                    // 回滚之前锁定的资源
                    for locked_resource in locked {
                        let _ = self.unlock(&locked_resource, owner).await;
                    }
                    return Ok(false);
                }
                Err(e) => {
                    // 回滚之前锁定的资源
                    for locked_resource in locked {
                        let _ = self.unlock(&locked_resource, owner).await;
                    }
                    return Err(crate::error::LockError::internal(format!("Lock failed: {e}")));
                }
            }
        }
        Ok(true)
    }

    /// 批量释放写锁
    pub async fn unlock_batch(&self, resources: &[String], owner: &str) -> crate::error::Result<()> {
        for resource in resources {
            let _ = self.unlock(resource, owner).await;
        }
        Ok(())
    }

    /// 批量获取读锁
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
            match self.rlock_with_ttl_id(resource, owner, timeout, expires_at).await {
                Ok(true) => {
                    locked.push(resource.clone());
                }
                Ok(false) => {
                    // 回滚之前锁定的资源
                    for locked_resource in locked {
                        let _ = self.runlock(&locked_resource, owner).await;
                    }
                    return Ok(false);
                }
                Err(e) => {
                    // 回滚之前锁定的资源
                    for locked_resource in locked {
                        let _ = self.runlock(&locked_resource, owner).await;
                    }
                    return Err(crate::error::LockError::internal(format!("RLock failed: {e}")));
                }
            }
        }
        Ok(true)
    }

    /// 批量释放读锁
    pub async fn runlock_batch(&self, resources: &[String], owner: &str) -> crate::error::Result<()> {
        for resource in resources {
            let _ = self.runlock(resource, owner).await;
        }
        Ok(())
    }

    /// 带TTL的写锁，支持超时，返回 LockId
    pub async fn lock_with_ttl_id(
        &self,
        resource: &str,
        owner: &str,
        timeout: Duration,
        expires_at: Option<Instant>,
    ) -> std::io::Result<bool> {
        let start = Instant::now();
        let mut last_check = start;

        loop {
            {
                let entry = self.locks.entry(resource.to_string()).or_insert_with(|| {
                    Arc::new(RwLock::new(LocalLockEntry {
                        writer: None,
                        readers: Vec::new(),
                        expires_at: None,
                    }))
                });
                let mut entry_guard = entry.value().write().await;
                if let Some(exp) = entry_guard.expires_at {
                    if exp <= Instant::now() {
                        entry_guard.writer = None;
                        entry_guard.readers.clear();
                        entry_guard.expires_at = None;
                    }
                }
                // 写锁需要检查没有写锁且没有读锁，或者当前owner已经持有写锁（重入性）
                tracing::debug!("Lock attempt for resource '{}' by owner '{}': writer={:?}, readers={:?}", 
                    resource, owner, entry_guard.writer, entry_guard.readers);
                
                let can_acquire = if let Some(current_writer) = &entry_guard.writer {
                    // 如果已经有写锁，只有同一个owner可以重入
                    current_writer == owner
                } else {
                    // 如果没有写锁，需要确保也没有读锁
                    entry_guard.readers.is_empty()
                };
                
                if can_acquire {
                    entry_guard.writer = Some(owner.to_string());
                    entry_guard.expires_at = expires_at;
                    let lock_id = crate::types::LockId::new_deterministic(resource);
                    self.lockid_map
                        .insert(lock_id.clone(), (resource.to_string(), owner.to_string()));
                    tracing::debug!("Lock acquired for resource '{}' by owner '{}'", resource, owner);
                    return Ok(true);
                } else {
                    tracing::debug!("Lock denied for resource '{}' by owner '{}': writer={:?}, readers={:?}", 
                        resource, owner, entry_guard.writer, entry_guard.readers);
                }
            }
            if start.elapsed() >= timeout {
                return Ok(false);
            }
            if last_check.elapsed() >= Duration::from_millis(50) {
                tokio::time::sleep(Duration::from_millis(10)).await;
                last_check = Instant::now();
            } else {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        }
    }

    /// 带TTL的读锁，支持超时，返回 LockId
    pub async fn rlock_with_ttl_id(
        &self,
        resource: &str,
        owner: &str,
        timeout: Duration,
        expires_at: Option<Instant>,
    ) -> std::io::Result<bool> {
        let start = Instant::now();
        let mut last_check = start;
        loop {
            {
                let entry = self.locks.entry(resource.to_string()).or_insert_with(|| {
                    Arc::new(RwLock::new(LocalLockEntry {
                        writer: None,
                        readers: Vec::new(),
                        expires_at: None,
                    }))
                });
                let mut entry_guard = entry.value().write().await;
                if let Some(exp) = entry_guard.expires_at {
                    if exp <= Instant::now() {
                        entry_guard.writer = None;
                        entry_guard.readers.clear();
                        entry_guard.expires_at = None;
                    }
                }
                if entry_guard.writer.is_none() {
                    if !entry_guard.readers.contains(&owner.to_string()) {
                        entry_guard.readers.push(owner.to_string());
                    }
                    entry_guard.expires_at = expires_at;
                    let lock_id = crate::types::LockId::new_deterministic(resource);
                    self.lockid_map
                        .insert(lock_id.clone(), (resource.to_string(), owner.to_string()));
                    return Ok(true);
                }
            }
            if start.elapsed() >= timeout {
                return Ok(false);
            }
            if last_check.elapsed() >= Duration::from_millis(50) {
                tokio::time::sleep(Duration::from_millis(10)).await;
                last_check = Instant::now();
            } else {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        }
    }

    /// 通过 LockId 解锁
    pub async fn unlock_by_id(&self, lock_id: &crate::types::LockId) -> std::io::Result<()> {
        if let Some((resource, owner)) = self.lockid_map.get(lock_id).map(|v| v.clone()) {
            self.unlock(&resource, &owner).await?;
            self.lockid_map.remove(lock_id);
            Ok(())
        } else {
            Err(std::io::Error::new(std::io::ErrorKind::NotFound, "LockId not found"))
        }
    }
    /// 通过 LockId 解锁读锁
    pub async fn runlock_by_id(&self, lock_id: &crate::types::LockId) -> std::io::Result<()> {
        if let Some((resource, owner)) = self.lockid_map.get(lock_id).map(|v| v.clone()) {
            self.runlock(&resource, &owner).await?;
            self.lockid_map.remove(lock_id);
            Ok(())
        } else {
            Err(std::io::Error::new(std::io::ErrorKind::NotFound, "LockId not found"))
        }
    }

    /// 批量写锁，返回 Vec<LockId>
    pub async fn lock_batch_id(
        &self,
        resources: &[String],
        owner: &str,
        timeout: std::time::Duration,
        ttl: Option<Duration>,
    ) -> crate::error::Result<Vec<crate::types::LockId>> {
        let mut locked = Vec::new();
        let expires_at = ttl.map(|t| Instant::now() + t);
        for resource in resources {
            match self.lock_with_ttl_id(resource, owner, timeout, expires_at).await {
                Ok(true) => {
                    locked.push(crate::types::LockId::new_deterministic(resource));
                }
                Ok(false) => {
                    // 回滚
                    for lock_id in locked {
                        let _ = self.unlock_by_id(&lock_id).await;
                    }
                    return Ok(Vec::new());
                }
                Err(e) => {
                    for lock_id in locked {
                        let _ = self.unlock_by_id(&lock_id).await;
                    }
                    return Err(crate::error::LockError::internal(format!("Lock failed: {e}")));
                }
            }
        }
        Ok(locked)
    }

    /// 批量释放写锁
    pub async fn unlock_batch_id(&self, lock_ids: &[crate::types::LockId]) -> crate::error::Result<()> {
        for lock_id in lock_ids {
            let _ = self.unlock_by_id(lock_id).await;
        }
        Ok(())
    }

    /// 批量读锁，返回 Vec<LockId>
    pub async fn rlock_batch_id(
        &self,
        resources: &[String],
        owner: &str,
        timeout: std::time::Duration,
        ttl: Option<Duration>,
    ) -> crate::error::Result<Vec<crate::types::LockId>> {
        let mut locked = Vec::new();
        let expires_at = ttl.map(|t| Instant::now() + t);
        for resource in resources {
            match self.rlock_with_ttl_id(resource, owner, timeout, expires_at).await {
                Ok(true) => {
                    locked.push(crate::types::LockId::new_deterministic(resource));
                }
                Ok(false) => {
                    for lock_id in locked {
                        let _ = self.runlock_by_id(&lock_id).await;
                    }
                    return Ok(Vec::new());
                }
                Err(e) => {
                    for lock_id in locked {
                        let _ = self.runlock_by_id(&lock_id).await;
                    }
                    return Err(crate::error::LockError::internal(format!("RLock failed: {e}")));
                }
            }
        }
        Ok(locked)
    }

    /// 批量释放读锁
    pub async fn runlock_batch_id(&self, lock_ids: &[crate::types::LockId]) -> crate::error::Result<()> {
        for lock_id in lock_ids {
            let _ = self.runlock_by_id(lock_id).await;
        }
        Ok(())
    }

    /// 带超时的写锁
    pub async fn lock(&self, resource: &str, owner: &str, timeout: Duration) -> std::io::Result<bool> {
        self.lock_with_ttl_id(resource, owner, timeout, None).await
    }

    /// 带超时的读锁
    pub async fn rlock(&self, resource: &str, owner: &str, timeout: Duration) -> std::io::Result<bool> {
        self.rlock_with_ttl_id(resource, owner, timeout, None).await
    }

    /// 释放写锁
    pub async fn unlock(&self, resource: &str, owner: &str) -> std::io::Result<()> {
        if let Some(entry) = self.locks.get(resource) {
            let mut entry_guard = entry.value().write().await;
            if entry_guard.writer.as_ref() == Some(&owner.to_string()) {
                entry_guard.writer = None;
                if entry_guard.readers.is_empty() {
                    entry_guard.expires_at = None;
                }
            }
        }
        Ok(())
    }

    /// 释放读锁
    pub async fn runlock(&self, resource: &str, owner: &str) -> std::io::Result<()> {
        if let Some(entry) = self.locks.get(resource) {
            let mut entry_guard = entry.value().write().await;
            entry_guard.readers.retain(|r| r != &owner.to_string());
            if entry_guard.readers.is_empty() && entry_guard.writer.is_none() {
                entry_guard.expires_at = None;
            }
        }
        Ok(())
    }

    /// 检查资源是否被锁定
    pub async fn is_locked(&self, resource: &str) -> bool {
        if let Some(entry) = self.locks.get(resource) {
            let entry_guard = entry.value().read().await;
            entry_guard.writer.is_some() || !entry_guard.readers.is_empty()
        } else {
            false
        }
    }

    /// 获取锁信息
    pub async fn get_lock(&self, resource: &str) -> Option<crate::types::LockInfo> {
        if let Some(entry) = self.locks.get(resource) {
            let entry_guard = entry.value().read().await;
            if let Some(writer) = &entry_guard.writer {
                Some(crate::types::LockInfo {
                    id: crate::types::LockId::new("test-lock"),
                    resource: resource.to_string(),
                    lock_type: crate::types::LockType::Exclusive,
                    status: crate::types::LockStatus::Acquired,
                    owner: writer.clone(),
                    acquired_at: std::time::SystemTime::now(),
                    expires_at: entry_guard
                        .expires_at
                        .map(|t| {
                            std::time::SystemTime::UNIX_EPOCH
                                + std::time::Duration::from_secs(t.duration_since(Instant::now()).as_secs())
                        })
                        .unwrap_or_else(|| std::time::SystemTime::now() + std::time::Duration::from_secs(30)),
                    last_refreshed: std::time::SystemTime::now(),
                    metadata: crate::types::LockMetadata::default(),
                    priority: crate::types::LockPriority::Normal,
                    wait_start_time: None,
                })
            } else if !entry_guard.readers.is_empty() {
                Some(crate::types::LockInfo {
                    id: crate::types::LockId::new("test-lock"),
                    resource: resource.to_string(),
                    lock_type: crate::types::LockType::Shared,
                    status: crate::types::LockStatus::Acquired,
                    owner: entry_guard.readers[0].clone(),
                    acquired_at: std::time::SystemTime::now(),
                    expires_at: entry_guard
                        .expires_at
                        .map(|t| {
                            std::time::SystemTime::UNIX_EPOCH
                                + std::time::Duration::from_secs(t.duration_since(Instant::now()).as_secs())
                        })
                        .unwrap_or_else(|| std::time::SystemTime::now() + std::time::Duration::from_secs(30)),
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

    /// Acquire exclusive lock
    pub async fn acquire_exclusive_lock(
        &self,
        resource: &str,
        _lock_id: &crate::types::LockId,
        owner: &str,
        timeout: Duration,
    ) -> crate::error::Result<()> {
        let success = self
            .lock_with_ttl_id(resource, owner, timeout, None)
            .await
            .map_err(|e| crate::error::LockError::internal(format!("Lock acquisition failed: {e}")))?;

        if success {
            Ok(())
        } else {
            Err(crate::error::LockError::internal("Lock acquisition timeout"))
        }
    }

    /// Acquire shared lock
    pub async fn acquire_shared_lock(
        &self,
        resource: &str,
        _lock_id: &crate::types::LockId,
        owner: &str,
        timeout: Duration,
    ) -> crate::error::Result<()> {
        let success = self
            .rlock_with_ttl_id(resource, owner, timeout, None)
            .await
            .map_err(|e| crate::error::LockError::internal(format!("Shared lock acquisition failed: {e}")))?;

        if success {
            Ok(())
        } else {
            Err(crate::error::LockError::internal("Shared lock acquisition timeout"))
        }
    }

    /// Release lock
    pub async fn release_lock(&self, resource: &str, owner: &str) -> crate::error::Result<()> {
        self.unlock(resource, owner)
            .await
            .map_err(|e| crate::error::LockError::internal(format!("Lock release failed: {e}")))
    }

    /// Refresh lock
    pub async fn refresh_lock(&self, resource: &str, _owner: &str) -> crate::error::Result<()> {
        // For local locks, refresh is not needed as they don't expire automatically
        // Just check if the lock still exists
        if self.is_locked(resource).await {
            Ok(())
        } else {
            Err(crate::error::LockError::internal("Lock not found or expired"))
        }
    }

    /// Force release lock
    pub async fn force_release_lock(&self, resource: &str) -> crate::error::Result<()> {
        if let Some(entry) = self.locks.get(resource) {
            let mut entry_guard = entry.value().write().await;
            entry_guard.writer = None;
            entry_guard.readers.clear();
            entry_guard.expires_at = None;
            Ok(())
        } else {
            Ok(())
        }
    }

    /// Clean up expired locks
    pub async fn cleanup_expired_locks(&self) -> usize {
        let now = Instant::now();
        let mut cleaned = 0;
        let mut to_remove = Vec::new();

        for item in self.locks.iter() {
            let mut entry_guard = item.value().write().await;
            if let Some(exp) = entry_guard.expires_at {
                if exp <= now {
                    entry_guard.writer = None;
                    entry_guard.readers.clear();
                    entry_guard.expires_at = None;
                    cleaned += 1;

                    if entry_guard.writer.is_none() && entry_guard.readers.is_empty() {
                        to_remove.push(item.key().clone());
                    }
                }
            }
        }

        for key in to_remove {
            self.locks.remove(&key);
        }

        cleaned
    }

    /// List all locks
    pub async fn list_locks(&self) -> Vec<crate::types::LockInfo> {
        let mut locks = Vec::new();
        for item in self.locks.iter() {
            if let Some(lock_info) = self.get_lock(item.key()).await {
                locks.push(lock_info);
            }
        }
        locks
    }

    /// Get locks for a specific resource
    pub async fn get_locks_for_resource(&self, resource: &str) -> Vec<crate::types::LockInfo> {
        if let Some(lock_info) = self.get_lock(resource).await {
            vec![lock_info]
        } else {
            Vec::new()
        }
    }

    /// Get statistics
    pub async fn get_stats(&self) -> crate::types::LockStats {
        let mut stats = crate::types::LockStats::default();
        let mut total_locks = 0;
        let mut exclusive_locks = 0;
        let mut shared_locks = 0;

        for item in self.locks.iter() {
            let entry_guard = item.value().read().await;
            if entry_guard.writer.is_some() {
                exclusive_locks += 1;
                total_locks += 1;
            }
            if !entry_guard.readers.is_empty() {
                shared_locks += entry_guard.readers.len();
                total_locks += entry_guard.readers.len();
            }
        }

        stats.total_locks = total_locks;
        stats.exclusive_locks = exclusive_locks;
        stats.shared_locks = shared_locks;
        stats.last_updated = std::time::SystemTime::now();
        stats
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

    /// 测试基本写锁获取和释放
    #[tokio::test]
    async fn test_write_lock_basic() {
        let lock_map = LocalLockMap::new();
        let ok = lock_map.lock("foo", "owner1", Duration::from_millis(100)).await.unwrap();
        assert!(ok, "Write lock should be successfully acquired");
        assert!(lock_map.is_locked("foo").await, "Lock state should be locked");
        lock_map.unlock("foo", "owner1").await.unwrap();
        assert!(!lock_map.is_locked("foo").await, "Should be unlocked after release");
    }

    /// 测试基本读锁获取和释放
    #[tokio::test]
    async fn test_read_lock_basic() {
        let lock_map = LocalLockMap::new();
        let ok = lock_map.rlock("bar", "reader1", Duration::from_millis(100)).await.unwrap();
        assert!(ok, "Read lock should be successfully acquired");
        assert!(lock_map.is_locked("bar").await, "Lock state should be locked");
        lock_map.runlock("bar", "reader1").await.unwrap();
        assert!(!lock_map.is_locked("bar").await, "Should be unlocked after release");
    }

    /// 测试写锁互斥
    #[tokio::test]
    async fn test_write_lock_mutex() {
        let lock_map = Arc::new(LocalLockMap::new());
        // owner1首先获取写锁
        let ok = lock_map.lock("res", "owner1", Duration::from_millis(100)).await.unwrap();
        assert!(ok);
        // owner2尝试在同一资源上获取写锁，应该超时并失败
        let lock_map2 = lock_map.clone();
        let fut = task::spawn(async move { lock_map2.lock("res", "owner2", Duration::from_millis(50)).await.unwrap() });
        let ok2 = fut.await.unwrap();
        assert!(!ok2, "Write locks should be mutually exclusive, owner2 acquisition should fail");
        lock_map.unlock("res", "owner1").await.unwrap();
    }

    /// 测试读锁共享
    #[tokio::test]
    async fn test_read_lock_sharing() {
        let lock_map = Arc::new(LocalLockMap::new());
        // reader1获取读锁
        let ok1 = lock_map.rlock("res", "reader1", Duration::from_millis(100)).await.unwrap();
        assert!(ok1);
        // reader2也应该能够获取读锁
        let ok2 = lock_map.rlock("res", "reader2", Duration::from_millis(100)).await.unwrap();
        assert!(ok2, "Multiple readers should be able to acquire read locks");

        lock_map.runlock("res", "reader1").await.unwrap();
        lock_map.runlock("res", "reader2").await.unwrap();
    }

    /// 测试批量锁操作
    #[tokio::test]
    async fn test_batch_lock_operations() {
        let lock_map = LocalLockMap::new();
        let resources = vec!["res1".to_string(), "res2".to_string(), "res3".to_string()];

        // 批量获取写锁
        let ok = lock_map
            .lock_batch(&resources, "owner1", Duration::from_millis(100), None)
            .await
            .unwrap();
        assert!(ok, "Batch lock should succeed");

        // 检查所有资源都被锁定
        for resource in &resources {
            assert!(lock_map.is_locked(resource).await, "Resource {resource} should be locked");
        }

        // 批量释放写锁
        lock_map.unlock_batch(&resources, "owner1").await.unwrap();

        // 检查所有资源都被释放
        for resource in &resources {
            assert!(!lock_map.is_locked(resource).await, "Resource {resource} should be unlocked");
        }
    }
}
