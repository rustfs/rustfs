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
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime};
use tokio::sync::RwLock;
use tokio::time::Instant;

use crate::{
    FastLockGuard, GlobalLockManager, LockClient, LockId, LockInfo, LockManager, LockMetadata, LockPriority, LockRequest,
    LockResponse, LockStats, LockStatus, LockType, Result,
};

/// Default shard count for guard storage (must be power of 2)
const DEFAULT_GUARD_SHARD_COUNT: usize = 64;

type GuardShard = Arc<RwLock<HashMap<LockId, LocalGuardEntry>>>;
type GuardStorage = Arc<Vec<GuardShard>>;

/// Local lock client using FastLock with sharded guard storage for better concurrency
#[derive(Debug)]
pub struct LocalClient {
    /// Sharded guard storage to reduce lock contention
    guard_storage: GuardStorage,
    /// Mask for fast shard index calculation (shard_count - 1)
    shard_mask: usize,
    /// Optional lock manager (if None, uses global singleton)
    manager: Option<Arc<GlobalLockManager>>,
    reaper_started: AtomicBool,
    reaper_interval: Duration,
}

#[derive(Debug)]
struct LocalGuardEntry {
    guard: FastLockGuard,
    acquired_at: SystemTime,
    last_refreshed: SystemTime,
    expires_at: SystemTime,
    deadline: Instant,
    ttl: Duration,
}

impl LocalGuardEntry {
    fn new(guard: FastLockGuard, ttl: Duration) -> Self {
        let acquired_at = SystemTime::now();
        let monotonic_now = Instant::now();
        Self {
            guard,
            acquired_at,
            last_refreshed: acquired_at,
            expires_at: acquired_at.checked_add(ttl).unwrap_or(acquired_at),
            deadline: monotonic_now.checked_add(ttl).unwrap_or(monotonic_now),
            ttl,
        }
    }

    fn is_expired(&self) -> bool {
        self.deadline <= Instant::now()
    }

    fn refresh(&mut self) {
        let now = SystemTime::now();
        let monotonic_now = Instant::now();
        self.expires_at = now.checked_add(self.ttl).unwrap_or(now);
        self.last_refreshed = now;
        self.deadline = monotonic_now.checked_add(self.ttl).unwrap_or(monotonic_now);
    }
}

impl LocalClient {
    /// Create new local client with default shard count
    pub fn new() -> Self {
        Self::with_shard_count(DEFAULT_GUARD_SHARD_COUNT)
    }

    /// Create new local client with custom shard count
    /// Shard count must be a power of 2 for efficient masking
    pub fn with_shard_count(shard_count: usize) -> Self {
        assert!(shard_count.is_power_of_two(), "Shard count must be power of 2");

        let guard_storage: Vec<GuardShard> = (0..shard_count).map(|_| Arc::new(RwLock::new(HashMap::new()))).collect();

        Self::with_storage(Arc::new(guard_storage), None, crate::fast_lock::CLEANUP_INTERVAL)
    }

    fn with_storage(guard_storage: GuardStorage, manager: Option<Arc<GlobalLockManager>>, reaper_interval: Duration) -> Self {
        let shard_count = guard_storage.len();
        debug_assert!(shard_count.is_power_of_two());
        Self {
            guard_storage,
            shard_mask: shard_count - 1,
            manager,
            reaper_started: AtomicBool::new(false),
            reaper_interval,
        }
    }

    /// Create new local client with a specific lock manager
    /// This allows simulating multi-node environments where each node has its own lock backend
    pub fn with_manager(manager: Arc<GlobalLockManager>) -> Self {
        let guard_storage = (0..DEFAULT_GUARD_SHARD_COUNT)
            .map(|_| Arc::new(RwLock::new(HashMap::new())))
            .collect();
        Self::with_storage(Arc::new(guard_storage), Some(manager), crate::fast_lock::CLEANUP_INTERVAL)
    }

    #[cfg(test)]
    pub(crate) fn with_manager_and_reaper_interval(manager: Arc<GlobalLockManager>, reaper_interval: Duration) -> Self {
        let guard_storage = (0..DEFAULT_GUARD_SHARD_COUNT)
            .map(|_| Arc::new(RwLock::new(HashMap::new())))
            .collect();
        Self::with_storage(Arc::new(guard_storage), Some(manager), reaper_interval)
    }

    /// Get the lock manager (injected manager if available, otherwise global singleton)
    pub fn get_lock_manager(&self) -> Arc<GlobalLockManager> {
        self.manager.clone().unwrap_or_else(crate::get_global_lock_manager)
    }

    /// Get the shard index for a given lock ID
    fn get_shard_index(&self, lock_id: &LockId) -> usize {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        lock_id.hash(&mut hasher);
        (hasher.finish() as usize) & self.shard_mask
    }

    /// Get the shard for a given lock ID
    fn get_shard(&self, lock_id: &LockId) -> &Arc<RwLock<HashMap<LockId, LocalGuardEntry>>> {
        let index = self.get_shard_index(lock_id);
        &self.guard_storage[index]
    }

    async fn reclaim_expired_guards_for_resource(&self, resource: &crate::ObjectKey) -> usize {
        let expired_entries = Self::extract_expired_guards(&self.guard_storage, Some(resource)).await;
        Self::release_reclaimed_guards(expired_entries, Some(resource))
    }

    async fn extract_expired_guards(storage: &GuardStorage, resource: Option<&crate::ObjectKey>) -> Vec<LocalGuardEntry> {
        let mut expired_entries = Vec::new();
        for shard in storage.iter() {
            let mut guards = shard.write().await;
            expired_entries.extend(
                guards
                    .extract_if(|lock_id, entry| {
                        resource.is_none_or(|resource| &lock_id.resource == resource) && entry.is_expired()
                    })
                    .map(|(_, entry)| entry),
            );
        }
        expired_entries
    }

    fn release_reclaimed_guards(
        entries: impl IntoIterator<Item = LocalGuardEntry>,
        resource: Option<&crate::ObjectKey>,
    ) -> usize {
        let mut reclaimed = 0;
        for mut entry in entries {
            let _ = entry.guard.release();
            rustfs_io_metrics::record_lock_reclaimed();
            reclaimed += 1;
        }
        if reclaimed > 0 {
            if let Some(resource) = resource {
                tracing::debug!(event = "lock_guard_reclaimed", resource = %resource, count = reclaimed, "expired lock guards reclaimed");
            } else {
                tracing::debug!(event = "lock_guard_reaper_sweep", count = reclaimed, "expired lock guards reclaimed");
            }
        }
        reclaimed
    }

    fn ensure_reaper(&self) {
        if self.reaper_started.swap(true, Ordering::AcqRel) {
            return;
        }
        let storage = Arc::downgrade(&self.guard_storage);
        let interval = self.reaper_interval;
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            loop {
                ticker.tick().await;
                let Some(storage) = storage.upgrade() else {
                    break;
                };
                let expired_entries = Self::extract_expired_guards(&storage, None).await;
                Self::release_reclaimed_guards(expired_entries, None);
            }
        });
    }
}

impl Default for LocalClient {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl LockClient for LocalClient {
    async fn acquire_lock(&self, request: &LockRequest) -> Result<LockResponse> {
        self.ensure_reaper();
        let lock_manager = self.get_lock_manager();
        let reclaimed_before_acquire = self.reclaim_expired_guards_for_resource(&request.resource).await;
        let acquire_deadline = Instant::now()
            .checked_add(request.acquire_timeout)
            .unwrap_or_else(Instant::now);

        let build_lock_request = |acquire_timeout| match request.lock_type {
            LockType::Exclusive => crate::ObjectLockRequest::new_write(request.resource.clone(), request.owner.clone())
                .with_acquire_timeout(acquire_timeout),
            LockType::Shared => crate::ObjectLockRequest::new_read(request.resource.clone(), request.owner.clone())
                .with_acquire_timeout(acquire_timeout),
        };

        let mut retried_after_reclaim = reclaimed_before_acquire > 0;
        loop {
            let remaining = acquire_deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Ok(LockResponse::failure("Lock acquisition timeout", request.acquire_timeout));
            }
            match lock_manager.acquire_lock(build_lock_request(remaining)).await {
                Ok(guard) => {
                    let lock_id = request.lock_id.clone();
                    let entry = LocalGuardEntry::new(guard, request.ttl);
                    let acquired_at = entry.acquired_at;
                    let expires_at = entry.expires_at;

                    {
                        let shard = self.get_shard(&lock_id);
                        let mut guards = shard.write().await;
                        guards.insert(lock_id.clone(), entry);
                    }

                    let lock_info = LockInfo {
                        id: lock_id,
                        resource: request.resource.clone(),
                        lock_type: request.lock_type,
                        status: crate::types::LockStatus::Acquired,
                        owner: request.owner.clone(),
                        acquired_at,
                        expires_at,
                        last_refreshed: acquired_at,
                        metadata: request.metadata.clone(),
                        priority: request.priority,
                        wait_start_time: None,
                    };
                    return Ok(LockResponse::success(lock_info, Duration::ZERO));
                }
                Err(crate::fast_lock::LockResult::Timeout) => {
                    if !retried_after_reclaim && self.reclaim_expired_guards_for_resource(&request.resource).await > 0 {
                        retried_after_reclaim = true;
                        continue;
                    }
                    return Ok(LockResponse::failure("Lock acquisition timeout", request.acquire_timeout));
                }
                Err(crate::fast_lock::LockResult::Conflict {
                    current_owner,
                    current_mode,
                }) => {
                    if !retried_after_reclaim && self.reclaim_expired_guards_for_resource(&request.resource).await > 0 {
                        retried_after_reclaim = true;
                        continue;
                    }
                    return Ok(LockResponse::failure(
                        format!("Lock conflict: resource held by {current_owner} in {current_mode:?} mode"),
                        Duration::ZERO,
                    ));
                }
                Err(crate::fast_lock::LockResult::Acquired) => {
                    unreachable!("Acquired should not be an error")
                }
            }
        }
    }

    async fn release(&self, lock_id: &LockId) -> Result<bool> {
        let shard = self.get_shard(lock_id);
        let mut guards = shard.write().await;
        if let Some(guard) = guards.remove(lock_id) {
            // Guard automatically releases the lock when dropped
            drop(guard.guard);
            Ok(true)
        } else {
            // Lock not found or already released
            Ok(false)
        }
    }

    async fn refresh(&self, lock_id: &LockId) -> Result<bool> {
        let shard = self.get_shard(lock_id);
        let expired_entry = {
            let mut guards = shard.write().await;
            let Some(entry) = guards.get_mut(lock_id) else {
                return Ok(false);
            };
            if entry.is_expired() {
                guards.remove(lock_id)
            } else {
                entry.refresh();
                None
            }
        };

        if let Some(entry) = expired_entry {
            Self::release_reclaimed_guards([entry], Some(&lock_id.resource));
            Ok(false)
        } else {
            Ok(true)
        }
    }

    async fn force_release(&self, lock_id: &LockId) -> Result<bool> {
        self.release(lock_id).await
    }

    async fn check_status(&self, lock_id: &LockId) -> Result<Option<LockInfo>> {
        let shard = self.get_shard(lock_id);
        let guards = shard.read().await;
        if let Some(entry) = guards.get(lock_id) {
            // We have an active guard for this lock
            let lock_type = match entry.guard.mode() {
                crate::LockMode::Shared => LockType::Shared,
                crate::LockMode::Exclusive => LockType::Exclusive,
            };
            let status = if entry.is_expired() {
                LockStatus::Expired
            } else {
                LockStatus::Acquired
            };
            Ok(Some(LockInfo {
                id: lock_id.clone(),
                resource: lock_id.resource.clone(),
                lock_type,
                status,
                owner: entry.guard.owner().to_string(),
                acquired_at: entry.acquired_at,
                expires_at: entry.expires_at,
                last_refreshed: entry.last_refreshed,
                metadata: LockMetadata::default(),
                priority: LockPriority::Normal,
                wait_start_time: None,
            }))
        } else {
            Ok(None)
        }
    }

    async fn list_lock_leases(&self) -> Vec<crate::LockLeaseInfo> {
        let mut leases = Vec::new();
        for shard in self.guard_storage.iter() {
            let guards = shard.read().await;
            leases.reserve(guards.len());
            leases.extend(guards.iter().map(|(lock_id, entry)| crate::LockLeaseInfo {
                resource: lock_id.resource.clone(),
                lock_type: match entry.guard.mode() {
                    crate::LockMode::Shared => LockType::Shared,
                    crate::LockMode::Exclusive => LockType::Exclusive,
                },
                owner: entry.guard.owner().to_string(),
                acquired_at: entry.acquired_at,
                remaining_ttl: entry.deadline.saturating_duration_since(Instant::now()),
                guard_id: (!entry.guard.is_disabled()).then(|| entry.guard.guard_id()),
            }));
        }
        leases
    }

    async fn get_stats(&self) -> Result<LockStats> {
        Ok(LockStats::default())
    }

    async fn close(&self) -> Result<()> {
        Ok(())
    }

    async fn is_online(&self) -> bool {
        true
    }

    async fn is_local(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{GlobalLockManager, LockClient, LockRequest, LockType};

    fn request(resource: crate::ObjectKey, owner: &str, ttl: Duration) -> LockRequest {
        LockRequest::new(resource, LockType::Exclusive, owner)
            .with_ttl(ttl)
            .with_acquire_timeout(Duration::from_millis(80))
    }

    async fn wait_until_reaped(client: &LocalClient, lock_id: &LockId) {
        for _ in 0..80 {
            if client.check_status(lock_id).await.unwrap().is_none() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        panic!("lock guard was not reaped before test deadline");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn expired_guard_is_reaped_without_resource_reacquire() {
        let manager = Arc::new(GlobalLockManager::new());
        let client = LocalClient::with_manager_and_reaper_interval(manager.clone(), Duration::from_millis(5));
        let request = request(crate::ObjectKey::new("bucket", "unique-chunk"), "owner-a", Duration::from_millis(10));
        let lock_id = request.lock_id.clone();

        assert!(client.acquire_lock(&request).await.unwrap().success);
        assert!(client.check_status(&lock_id).await.unwrap().is_some());
        tokio::time::sleep(Duration::from_millis(15)).await;
        wait_until_reaped(&client, &lock_id).await;
        assert!(
            client.list_lock_leases().await.is_empty(),
            "reaped guards must disappear from lease diagnostics"
        );

        let direct = manager
            .acquire_lock(crate::ObjectLockRequest::new_write(request.resource.clone(), "owner-b"))
            .await;
        assert!(direct.is_ok());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn sibling_client_cannot_reclaim_but_owner_reaper_releases_shared_lock() {
        let manager = Arc::new(GlobalLockManager::new());
        let owner = LocalClient::with_manager_and_reaper_interval(manager.clone(), Duration::from_millis(5));
        let contender = LocalClient::with_manager_and_reaper_interval(manager, Duration::from_millis(5));
        let request_a = request(crate::ObjectKey::new("bucket", "shared-resource"), "owner-a", Duration::from_millis(10));
        assert!(owner.acquire_lock(&request_a).await.unwrap().success);

        let request_b = request(request_a.resource.clone(), "owner-b", Duration::from_millis(20))
            .with_acquire_timeout(Duration::from_millis(5));
        assert!(!contender.acquire_lock(&request_b).await.unwrap().success);

        tokio::time::sleep(Duration::from_millis(25)).await;
        assert!(owner.check_status(&request_a.lock_id).await.unwrap().is_none());
        assert!(contender.acquire_lock(&request_b).await.unwrap().success);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn refresh_wins_before_deadline_and_reaper_wins_after_deadline() {
        let manager = Arc::new(GlobalLockManager::new());
        let client = LocalClient::with_manager_and_reaper_interval(manager, Duration::from_millis(5));
        let request = request(crate::ObjectKey::new("bucket", "refresh-race"), "owner-a", Duration::from_millis(25));
        let lock_id = request.lock_id.clone();
        assert!(client.acquire_lock(&request).await.unwrap().success);

        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(client.refresh(&lock_id).await.unwrap());
        tokio::time::sleep(Duration::from_millis(15)).await;
        assert!(client.check_status(&lock_id).await.unwrap().is_some());
        wait_until_reaped(&client, &lock_id).await;
    }

    #[tokio::test(start_paused = true)]
    async fn lease_snapshot_tracks_refresh_without_resetting_acquisition_time() {
        let manager = Arc::new(GlobalLockManager::new());
        let client = LocalClient::with_manager_and_reaper_interval(manager, Duration::from_secs(60));
        client.reaper_started.store(true, Ordering::Release);
        let lock_request = request(crate::ObjectKey::new("bucket", "lease-snapshot"), "owner-a", Duration::from_secs(30));
        let lock_id = lock_request.lock_id.clone();

        assert!(
            client
                .acquire_lock(&lock_request)
                .await
                .expect("lease-backed lock should acquire")
                .success
        );
        let initial = client.list_lock_leases().await.pop().expect("acquired lock should be listed");
        let initial_status = client
            .check_status(&lock_id)
            .await
            .expect("initial lock status should be readable")
            .expect("newly acquired lock should remain held");
        assert_eq!(initial_status.last_refreshed, initial_status.acquired_at);

        tokio::time::advance(Duration::from_secs(20)).await;
        let aging = client
            .list_lock_leases()
            .await
            .pop()
            .expect("held lock should remain listed before refresh");
        assert_eq!(aging.remaining_ttl, Duration::from_secs(10));
        let aging_status = client
            .check_status(&lock_id)
            .await
            .expect("aging lock status should be readable")
            .expect("aging lock should remain held");
        assert_eq!(aging_status.last_refreshed, initial_status.last_refreshed);

        assert!(client.refresh(&lock_id).await.expect("refresh should return a result"));

        let refreshed = client
            .list_lock_leases()
            .await
            .pop()
            .expect("refreshed lock should be listed");
        let status = client
            .check_status(&lock_id)
            .await
            .expect("lock status should be readable")
            .expect("refreshed lock should remain held");

        assert_eq!(refreshed.acquired_at, initial.acquired_at);
        assert_eq!(refreshed.guard_id, initial.guard_id);
        assert_eq!(status.acquired_at, initial.acquired_at);
        assert!(status.last_refreshed > initial_status.last_refreshed);
        assert_eq!(refreshed.remaining_ttl, Duration::from_secs(30));

        tokio::time::advance(Duration::from_secs(30)).await;
        let expired = client
            .list_lock_leases()
            .await
            .pop()
            .expect("unreaped lease should remain listed");
        assert_eq!(expired.remaining_ttl, Duration::ZERO);
    }

    #[tokio::test(start_paused = true)]
    async fn refresh_after_expiry_releases_guard_without_reviving_it() {
        let manager = Arc::new(GlobalLockManager::new());
        let client = LocalClient::with_manager_and_reaper_interval(manager, Duration::from_secs(60));
        client.reaper_started.store(true, Ordering::Release);
        let lock_request = request(
            crate::ObjectKey::new("bucket", "refresh-after-expiry"),
            "owner-a",
            Duration::from_secs(10),
        );
        let lock_id = lock_request.lock_id.clone();

        assert!(
            client
                .acquire_lock(&lock_request)
                .await
                .expect("initial owner should acquire the lock")
                .success
        );
        tokio::time::advance(Duration::from_secs(11)).await;

        assert!(
            !client
                .refresh(&lock_id)
                .await
                .expect("expired refresh should return a result"),
            "an expired guard must not be refreshed"
        );
        assert!(
            client
                .check_status(&lock_id)
                .await
                .expect("expired guard status should be readable")
                .is_none(),
            "expired guard should be removed after refresh"
        );

        let contender = request(
            crate::ObjectKey::new("bucket", "refresh-after-expiry"),
            "owner-b",
            Duration::from_secs(10),
        );
        assert!(
            client
                .acquire_lock(&contender)
                .await
                .expect("contender should receive an acquisition result")
                .success,
            "released guard must be acquirable by a new owner"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn zero_ttl_is_reaped_and_oversized_ttl_does_not_panic() {
        let manager = Arc::new(GlobalLockManager::new());
        let client = LocalClient::with_manager_and_reaper_interval(manager, Duration::from_millis(5));

        let zero = request(crate::ObjectKey::new("bucket", "zero-ttl"), "owner-zero", Duration::ZERO);
        let zero_id = zero.lock_id.clone();
        assert!(client.acquire_lock(&zero).await.unwrap().success);
        wait_until_reaped(&client, &zero_id).await;

        let huge = request(crate::ObjectKey::new("bucket", "huge-ttl"), "owner-huge", Duration::MAX);
        let huge_id = huge.lock_id.clone();
        assert!(client.acquire_lock(&huge).await.unwrap().success);
        wait_until_reaped(&client, &huge_id).await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn acquire_retry_preserves_total_deadline() {
        let manager = Arc::new(GlobalLockManager::new());
        let client = LocalClient::with_manager_and_reaper_interval(manager, Duration::from_secs(60));
        let first = request(crate::ObjectKey::new("bucket", "deadline-budget"), "owner-a", Duration::from_millis(10));
        assert!(client.acquire_lock(&first).await.unwrap().success);

        let second =
            request(first.resource.clone(), "owner-b", Duration::from_millis(30)).with_acquire_timeout(Duration::from_millis(60));
        let started = Instant::now();
        let response = client.acquire_lock(&second).await.unwrap();
        assert!(!response.success, "the first attempt consumed the caller's acquire budget");
        assert!(
            started.elapsed() < Duration::from_millis(100),
            "reclaim retry must not double the acquire budget"
        );
        let recovered = client.acquire_lock(&second).await.unwrap();
        assert!(recovered.success, "the reclaimed guard must be available to the next request");
    }
}
