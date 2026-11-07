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

use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::collections::HashMap;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime};
use tokio::sync::Notify;
use tokio::time::{Instant, timeout, timeout_at};

use crate::{
    FastLockGuard, GlobalLockManager, LockClient, LockId, LockInfo, LockManager, LockMetadata, LockPriority, LockRequest,
    LockResponse, LockStats, LockStatus, LockType, Result,
};

/// Default shard count for guard storage (must be power of 2)
const DEFAULT_GUARD_SHARD_COUNT: usize = 64;
const EVENT_NAMESPACE_LOCK_ACQUIRE: &str = "namespace_lock_acquire";
const EVENT_NAMESPACE_LOCK_RELEASE: &str = "namespace_lock_release";
const LOG_COMPONENT_LOCK: &str = "lock";
const LOG_SUBSYSTEM_NAMESPACE: &str = "namespace";

type GuardMap = HashMap<LockId, LocalGuardEntry>;
type GuardShard = Arc<GuardRegistry>;
type GuardStorage = Arc<Vec<GuardShard>>;

/// Registry access never reserves the table for a task that has not resumed.
/// Only short synchronous map operations hold the native lock; contention
/// waits on a notification without blocking an executor thread.
#[derive(Debug, Default)]
struct GuardRegistry {
    entries: RwLock<GuardMap>,
    changed: Notify,
}

struct GuardRegistryWake<'a>(&'a Notify);

impl Drop for GuardRegistryWake<'_> {
    fn drop(&mut self) {
        self.0.notify_waiters();
    }
}

struct GuardRegistryAccess<'a, G> {
    // Field drop order unlocks the table before waking its waiters, including
    // when a map operation unwinds. Physical guards are removed before Drop.
    guard: G,
    _wake: GuardRegistryWake<'a>,
}

impl<G: Deref> Deref for GuardRegistryAccess<'_, G> {
    type Target = G::Target;

    fn deref(&self) -> &Self::Target {
        &self.guard
    }
}

impl<G: DerefMut> DerefMut for GuardRegistryAccess<'_, G> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.guard
    }
}

impl GuardRegistry {
    fn access<G>(&self, guard: G) -> GuardRegistryAccess<'_, G> {
        GuardRegistryAccess {
            guard,
            _wake: GuardRegistryWake(&self.changed),
        }
    }

    async fn read(&self) -> GuardRegistryAccess<'_, RwLockReadGuard<'_, GuardMap>> {
        tokio::task::consume_budget().await;
        if let Some(guard) = self.entries.try_read() {
            return self.access(guard);
        }
        loop {
            tokio::task::consume_budget().await;
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Some(guard) = self.entries.try_read() {
                return self.access(guard);
            }
            notified.await;
        }
    }

    async fn write(&self) -> GuardRegistryAccess<'_, RwLockWriteGuard<'_, GuardMap>> {
        tokio::task::consume_budget().await;
        if let Some(guard) = self.entries.try_write() {
            return self.access(guard);
        }
        loop {
            tokio::task::consume_budget().await;
            // Register before rechecking so an unlock between the first
            // failed try and this wait cannot leave the task asleep.
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Some(guard) = self.entries.try_write() {
                return self.access(guard);
            }
            notified.await;
        }
    }
}

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

        let guard_storage: Vec<GuardShard> = (0..shard_count).map(|_| Arc::new(GuardRegistry::default())).collect();

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
            .map(|_| Arc::new(GuardRegistry::default()))
            .collect();
        Self::with_storage(Arc::new(guard_storage), Some(manager), crate::fast_lock::CLEANUP_INTERVAL)
    }

    #[cfg(test)]
    pub(crate) fn with_manager_and_reaper_interval(manager: Arc<GlobalLockManager>, reaper_interval: Duration) -> Self {
        let guard_storage = (0..DEFAULT_GUARD_SHARD_COUNT)
            .map(|_| Arc::new(GuardRegistry::default()))
            .collect();
        Self::with_storage(Arc::new(guard_storage), Some(manager), reaper_interval)
    }

    /// Get the lock manager (injected manager if available, otherwise global singleton)
    pub fn get_lock_manager(&self) -> Arc<GlobalLockManager> {
        self.manager.clone().unwrap_or_else(crate::get_global_lock_manager)
    }

    /// Guard entries for one resource share a shard so expiration checks do not
    /// need to take every shard's write lock on the acquisition hot path.
    fn get_resource_shard_index(&self, resource: &crate::ObjectKey) -> usize {
        resource.shard_index(self.shard_mask)
    }

    /// Get the shard for a given lock ID
    fn get_shard(&self, lock_id: &LockId) -> &GuardShard {
        let index = self.get_resource_shard_index(&lock_id.resource);
        &self.guard_storage[index]
    }

    async fn reclaim_expired_guards_for_resource(&self, resource: &crate::ObjectKey) -> usize {
        let shard = &self.guard_storage[self.get_resource_shard_index(resource)];
        let has_expired = {
            let guards = shard.read().await;
            guards
                .iter()
                .any(|(lock_id, entry)| &lock_id.resource == resource && entry.is_expired())
        };
        if !has_expired {
            return 0;
        }

        let expired_entries = {
            let mut guards = shard.write().await;
            guards
                .extract_if(|lock_id, entry| &lock_id.resource == resource && entry.is_expired())
                .map(|(_, entry)| entry)
                .collect::<Vec<_>>()
        };
        Self::release_reclaimed_guards(expired_entries, Some(resource))
    }

    async fn extract_expired_guards(storage: &GuardStorage) -> Vec<LocalGuardEntry> {
        let mut expired_entries = Vec::new();
        for shard in storage.iter() {
            let has_expired = {
                let guards = shard.read().await;
                guards.values().any(LocalGuardEntry::is_expired)
            };
            if !has_expired {
                continue;
            }
            let mut guards = shard.write().await;
            expired_entries.extend(guards.extract_if(|_, entry| entry.is_expired()).map(|(_, entry)| entry));
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
                let expired_entries = Self::extract_expired_guards(&storage).await;
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
        let started = Instant::now();
        self.ensure_reaper();
        if request.lock_id.resource != request.resource {
            return Ok(LockResponse::failure(
                "Lock id resource does not match the requested resource",
                Duration::ZERO,
            ));
        }
        let acquire_deadline = Instant::now()
            .checked_add(request.acquire_timeout)
            .unwrap_or_else(Instant::now);
        let lock_manager = self.get_lock_manager();
        let initial_remaining = acquire_deadline.saturating_duration_since(Instant::now());
        let reclaimed_before_acquire = if initial_remaining.is_zero() {
            0
        } else {
            timeout(initial_remaining, self.reclaim_expired_guards_for_resource(&request.resource))
                .await
                .unwrap_or(0)
        };
        let reclaim_elapsed = started.elapsed();

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
            let manager_started = Instant::now();
            match lock_manager.acquire_lock(build_lock_request(remaining)).await {
                Ok(guard) => {
                    let manager_elapsed = manager_started.elapsed();
                    let registration_started = Instant::now();
                    let physical_guard_id = guard.guard_id();
                    let lock_id = request.lock_id.clone();
                    let entry = LocalGuardEntry::new(guard, request.ttl);
                    let acquired_at = entry.acquired_at;
                    let expires_at = entry.expires_at;

                    let (replaced_entry, registration_wait, registration_mutation, registry_entries, registry_capacity) = {
                        let shard = self.get_shard(&lock_id);
                        let Ok(mut guards) = timeout_at(acquire_deadline, shard.write()).await else {
                            return Ok(LockResponse::failure("Lock acquisition timeout", request.acquire_timeout));
                        };
                        // A ready table cannot publish a grant after the peer
                        // budget expired while its acquisition task was idle.
                        if Instant::now() >= acquire_deadline {
                            drop(guards);
                            return Ok(LockResponse::failure("Lock acquisition timeout", request.acquire_timeout));
                        }
                        let registration_wait = registration_started.elapsed();
                        let mutation_started = Instant::now();
                        let replaced_entry = guards.insert(lock_id.clone(), entry);
                        (
                            replaced_entry,
                            registration_wait,
                            mutation_started.elapsed(),
                            guards.len(),
                            guards.capacity(),
                        )
                    };
                    // Repeated shared requests can replace an entry. Releasing
                    // its physical guard must not reenter locks under the table.
                    drop(replaced_entry);

                    let registration_elapsed = registration_started.elapsed();
                    if started.elapsed() >= Duration::from_millis(100) {
                        tracing::trace!(
                            target: "rustfs_lock::namespace_owner",
                            event = EVENT_NAMESPACE_LOCK_ACQUIRE,
                            component = LOG_COMPONENT_LOCK,
                            subsystem = LOG_SUBSYSTEM_NAMESPACE,
                            state = "local_completed",
                            resource = %request.resource,
                            lock_id = %lock_id,
                            mode = ?request.lock_type,
                            physical_guard_id,
                            reclaim_ms = reclaim_elapsed.as_millis(),
                            manager_ms = manager_elapsed.as_millis(),
                            registration_ms = registration_elapsed.as_millis(),
                            registration_wait_ms = registration_wait.as_millis(),
                            registration_mutation_ms = registration_mutation.as_millis(),
                            registry_entries,
                            registry_capacity,
                            acquire_ms = started.elapsed().as_millis(),
                            "Namespace local lock acquisition completed slowly"
                        );
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
                    if !retried_after_reclaim {
                        let remaining = acquire_deadline.saturating_duration_since(Instant::now());
                        if !remaining.is_zero() {
                            let _ = timeout(remaining, self.reclaim_expired_guards_for_resource(&request.resource)).await;
                        }
                        retried_after_reclaim = true;
                        continue;
                    }
                    return Ok(LockResponse::failure("Lock acquisition timeout", request.acquire_timeout));
                }
                Err(crate::fast_lock::LockResult::Conflict {
                    current_owner,
                    current_mode,
                }) => {
                    if !retried_after_reclaim {
                        let remaining = acquire_deadline.saturating_duration_since(Instant::now());
                        if !remaining.is_zero() {
                            let _ = timeout(remaining, self.reclaim_expired_guards_for_resource(&request.resource)).await;
                        }
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
        let guard = {
            let mut guards = shard.write().await;
            guards.remove(lock_id)
        };
        if let Some(guard) = guard {
            // Guard automatically releases the lock when dropped
            let physical_guard_id = (!guard.guard.is_disabled()).then(|| guard.guard.guard_id());
            drop(guard.guard);
            if let Some(physical_guard_id) = physical_guard_id {
                tracing::trace!(
                    target: "rustfs_lock::namespace_owner",
                    event = EVENT_NAMESPACE_LOCK_RELEASE,
                    component = LOG_COMPONENT_LOCK,
                    subsystem = LOG_SUBSYSTEM_NAMESPACE,
                    state = "mapped",
                    peer_kind = "local",
                    lock_id = %lock_id,
                    process_id = std::process::id(),
                    physical_guard_id,
                    "Namespace peer lock mapped to physical guard"
                );
            }
            Ok(true)
        } else {
            // Lock not found or already released
            Ok(false)
        }
    }

    async fn cleanup_grant(&self, lock_id: &LockId) -> Result<super::LockCleanupOutcome> {
        self.release(lock_id).await.map(|released| {
            if released {
                super::LockCleanupOutcome::Released
            } else {
                super::LockCleanupOutcome::AlreadyAbsent
            }
        })
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
    use futures::FutureExt;
    use std::task::Poll;

    fn request(resource: crate::ObjectKey, owner: &str, ttl: Duration) -> LockRequest {
        LockRequest::new(resource, LockType::Exclusive, owner)
            .with_ttl(ttl)
            .with_acquire_timeout(Duration::from_millis(80))
    }

    fn hold_guard_shard(shard: GuardShard) -> (std::sync::mpsc::Sender<()>, std::thread::JoinHandle<()>) {
        let (entered_tx, entered_rx) = std::sync::mpsc::sync_channel(0);
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let thread = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("build the independent guard-table owner runtime");
            let guard = runtime.block_on(shard.write());
            entered_tx.send(()).expect("signal that the guard table is occupied");
            // A dropped controller also releases the table on test failure.
            let _ = release_rx.recv_timeout(Duration::from_secs(3));
            drop(guard);
        });
        entered_rx
            .recv_timeout(Duration::from_secs(3))
            .expect("guard-table owner must enter before the tested operation");
        (release_tx, thread)
    }

    #[tokio::test(start_paused = true)]
    async fn unpolled_registry_writer_does_not_block_compatible_acquisition() {
        let manager = Arc::new(GlobalLockManager::new());
        let client = LocalClient::with_manager(manager.clone());
        client.reaper_started.store(true, Ordering::Release);
        let first = LockRequest::new(crate::ObjectKey::new("bucket", ""), LockType::Shared, "first");
        assert!(client.acquire_lock(&first).await.expect("first shared lock").success);
        let (release_table, table_owner) = hold_guard_shard(client.get_shard(&first.lock_id).clone());
        let mut pending_release = Box::pin(client.release(&first.lock_id));
        assert!(matches!(futures::poll!(&mut pending_release), Poll::Pending));
        release_table.send(()).expect("release the occupied table");
        table_owner.join().expect("guard-table owner must finish");

        // The earlier writer is deliberately not polled: an unoccupied
        // bookkeeping table must remain usable by a currently running peer.
        let second = LockRequest::new(first.resource.clone(), LockType::Shared, "second");
        let response = client
            .acquire_lock(&second)
            .now_or_never()
            .expect("a sleeping registry writer must not reserve the available table")
            .expect("compatible shared acquisition should return a response");
        assert!(response.success);
        assert_eq!(
            manager
                .as_fast_lock_manager()
                .expect("enabled physical lock backend")
                .get_shard(&first.resource)
                .active_guard_count(),
            2
        );
        assert!(
            pending_release
                .await
                .expect("the earlier writer still releases its exact grant")
        );
        assert!(client.release(&second.lock_id).await.expect("release the second grant"));
        assert_eq!(
            manager
                .as_fast_lock_manager()
                .expect("enabled physical lock backend")
                .get_shard(&first.resource)
                .active_guard_count(),
            0
        );
    }

    #[tokio::test(start_paused = true)]
    async fn acquisition_deadline_rejects_registration_after_physical_grant() {
        for unlock_before_poll in [false, true] {
            let manager = Arc::new(GlobalLockManager::new());
            let client = LocalClient::with_manager(manager.clone());
            client.reaper_started.store(true, Ordering::Release);
            let resource = crate::ObjectKey::new("bucket", "registration-deadline");
            let blocker = manager
                .acquire_lock(crate::ObjectLockRequest::new_write(resource.clone(), "blocker"))
                .await
                .expect("hold the physical lock until guard-table preflight finishes");
            let request =
                request(resource.clone(), "owner", Duration::from_secs(30)).with_acquire_timeout(Duration::from_millis(20));
            let mut acquire = Box::pin(tokio::task::unconstrained(client.acquire_lock(&request)));
            assert!(matches!(futures::poll!(&mut acquire), Poll::Pending));
            let (release_table, table_owner) = hold_guard_shard(client.get_shard(&request.lock_id).clone());
            drop(blocker);
            assert!(matches!(futures::poll!(&mut acquire), Poll::Pending));
            assert_eq!(
                manager
                    .as_fast_lock_manager()
                    .expect("enabled physical lock backend")
                    .get_shard(&resource)
                    .active_guard_count(),
                1,
                "the tested future owns a physical grant"
            );
            tokio::time::advance(Duration::from_millis(21)).await;
            if unlock_before_poll {
                release_table.send(()).expect("make registration ready after its deadline");
                table_owner.join().expect("guard-table owner must finish");
                assert!(!acquire.await.expect("expired registration response").success);
            } else {
                let response = timeout(Duration::from_millis(100), &mut acquire)
                    .await
                    .expect("registration must honor its deadline while the table remains occupied")
                    .expect("expired registration response");
                assert!(!response.success);
                release_table.send(()).expect("release the occupied table");
                table_owner.join().expect("guard-table owner must finish");
            }
            assert!(
                client
                    .check_status(&request.lock_id)
                    .await
                    .expect("check the expired identity")
                    .is_none()
            );
            assert_eq!(
                manager
                    .as_fast_lock_manager()
                    .expect("enabled physical lock backend")
                    .get_shard(&resource)
                    .active_guard_count(),
                0,
                "expired registration must release its grant"
            );
            assert!(
                manager
                    .acquire_lock(crate::ObjectLockRequest::new_write(resource, "next-owner"))
                    .await
                    .is_ok(),
                "the next owner must not inherit an abandoned physical grant"
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn cancelling_registration_releases_the_unpublished_physical_grant() {
        let manager = Arc::new(GlobalLockManager::new());
        let client = LocalClient::with_manager(manager.clone());
        client.reaper_started.store(true, Ordering::Release);
        let resource = crate::ObjectKey::new("bucket", "cancel-registration");
        let blocker = manager
            .acquire_lock(crate::ObjectLockRequest::new_write(resource.clone(), "blocker"))
            .await
            .expect("hold the physical lock through registry preflight");
        let request = request(resource.clone(), "cancelled", Duration::from_secs(30));
        let mut acquire = Box::pin(tokio::task::unconstrained(client.acquire_lock(&request)));
        assert!(matches!(futures::poll!(&mut acquire), Poll::Pending));
        let (release_table, table_owner) = hold_guard_shard(client.get_shard(&request.lock_id).clone());
        drop(blocker);
        assert!(matches!(futures::poll!(&mut acquire), Poll::Pending));
        let backend = manager.as_fast_lock_manager().expect("enabled physical lock backend");
        assert_eq!(backend.get_shard(&resource).active_guard_count(), 1);
        drop(acquire);
        assert_eq!(backend.get_shard(&resource).active_guard_count(), 0);
        assert!(
            manager
                .acquire_lock(crate::ObjectLockRequest::new_write(resource, "next-owner"))
                .await
                .is_ok(),
            "cancellation must release the physical lock without waiting for the table"
        );
        release_table.send(()).expect("release the occupied table");
        table_owner.join().expect("guard-table owner must finish");
        assert!(
            client
                .check_status(&request.lock_id)
                .await
                .expect("check cancelled identity")
                .is_none()
        );
        assert!(client.list_lock_leases().await.is_empty());
    }

    #[tokio::test]
    async fn cancelling_a_registry_waiter_keeps_other_waiters_live() {
        let shard = Arc::new(GuardRegistry::default());
        let (release_table, table_owner) = hold_guard_shard(shard.clone());
        let mut cancelled = Box::pin(shard.write());
        let mut survivor = Box::pin(shard.read());
        assert!(matches!(futures::poll!(&mut cancelled), Poll::Pending));
        assert!(matches!(futures::poll!(&mut survivor), Poll::Pending));
        drop(cancelled);
        release_table.send(()).expect("wake registry waiters");
        table_owner.join().expect("guard-table owner must finish");
        let access = timeout(Duration::from_millis(100), survivor)
            .await
            .expect("a cancelled waiter must not consume another waiter's unlock notification");
        assert!(access.is_empty());
    }

    #[tokio::test]
    async fn uncontended_registry_access_retains_cooperative_scheduling() {
        let shard = GuardRegistry::default();
        let mut yielded = false;
        for _ in 0..1024 {
            match shard.read().now_or_never() {
                Some(access) => drop(access),
                None => {
                    yielded = true;
                    break;
                }
            }
        }
        assert!(yielded, "a loop of ready registry operations must still yield to other executor tasks");
    }

    #[tokio::test]
    async fn repeated_shared_identity_replaces_only_its_previous_physical_guard() {
        let manager = Arc::new(GlobalLockManager::new());
        let client = LocalClient::with_manager(manager.clone());
        let request = LockRequest::new(crate::ObjectKey::new("bucket", ""), LockType::Shared, "owner");
        assert!(client.acquire_lock(&request).await.expect("first shared grant").success);
        assert!(client.acquire_lock(&request).await.expect("repeated shared identity").success);
        let backend = manager.as_fast_lock_manager().expect("enabled physical lock backend");
        assert_eq!(backend.get_shard(&request.resource).active_guard_count(), 1);
        assert_eq!(client.list_lock_leases().await.len(), 1);
        assert!(client.release(&request.lock_id).await.expect("release the replacement grant"));
        assert_eq!(backend.get_shard(&request.resource).active_guard_count(), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn hot_shared_registry_keeps_refresh_reaper_and_snapshots_live() {
        for width in [64, 128] {
            let manager = Arc::new(GlobalLockManager::new());
            let client = Arc::new(LocalClient::with_manager_and_reaper_interval(manager.clone(), Duration::from_millis(1)));
            let resource = crate::ObjectKey::new("bucket", "");
            let expired = LockRequest::new(resource.clone(), LockType::Shared, "expired").with_ttl(Duration::ZERO);
            assert!(client.acquire_lock(&expired).await.expect("expired seed grant").success);
            let acquired = Arc::new(tokio::sync::Barrier::new(width + 1));
            let observed = Arc::new(Notify::new());
            let mut tasks = tokio::task::JoinSet::new();
            for index in 0..width {
                let client = client.clone();
                let resource = resource.clone();
                let acquired = acquired.clone();
                let observed = observed.clone();
                tasks.spawn(async move {
                    for iteration in 0..8 {
                        let request = LockRequest::new(resource.clone(), LockType::Shared, format!("load-{index}"));
                        assert!(client.acquire_lock(&request).await.expect("concurrent shared grant").success);
                        if iteration == 0 {
                            let notification = observed.notified();
                            tokio::pin!(notification);
                            notification.as_mut().enable();
                            acquired.wait().await;
                            notification.await;
                        } else {
                            tokio::task::yield_now().await;
                        }
                        assert!(client.refresh(&request.lock_id).await.expect("refresh concurrent grant"));
                        assert!(
                            client
                                .check_status(&request.lock_id)
                                .await
                                .expect("read concurrent grant")
                                .is_some()
                        );
                        assert!(client.release(&request.lock_id).await.expect("release concurrent grant"));
                    }
                });
            }
            timeout(Duration::from_secs(5), acquired.wait())
                .await
                .expect("all shared grants must reach the snapshot gate");
            let snapshot = client.list_lock_leases().await;
            assert_eq!(snapshot.iter().filter(|lease| lease.owner.starts_with("load-")).count(), width);
            observed.notify_waiters();
            let mut completed = 0;
            timeout(Duration::from_secs(5), async {
                while let Some(result) = tasks.join_next().await {
                    result.expect("all shared grant workers must finish");
                    completed += 1;
                    let _ = client.list_lock_leases().await;
                }
            })
            .await
            .expect("refresh, cleanup, and concurrent snapshots must make progress");
            assert_eq!(completed, width);
            wait_until_reaped(&client, &expired.lock_id).await;
            assert!(client.list_lock_leases().await.is_empty());
            assert!(
                manager
                    .as_fast_lock_manager()
                    .expect("enabled physical lock backend")
                    .list_locks()
                    .is_empty()
            );
        }
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

    #[tokio::test]
    async fn acquire_does_not_wait_on_unrelated_guard_shards() {
        let manager = Arc::new(GlobalLockManager::new());
        let client = LocalClient::with_manager_and_reaper_interval(manager, Duration::from_secs(60));
        client.reaper_started.store(true, Ordering::Release);
        let resource = crate::ObjectKey::new("bucket", "resource-shard-hot-path");
        let target_shard = client.get_resource_shard_index(&resource);

        let mut unrelated_writes = Vec::new();
        for (index, shard) in client.guard_storage.iter().enumerate() {
            if index != target_shard {
                unrelated_writes.push(shard.write().await);
            }
        }

        let lock_request = request(resource, "owner", Duration::from_secs(30));
        let response = client
            .acquire_lock(&lock_request)
            .now_or_never()
            .expect("an acquire must not sweep unrelated guard shards")
            .expect("local acquire should return a response");
        assert!(response.success);

        drop(unrelated_writes);
        assert!(client.release(&lock_request.lock_id).await.unwrap());
    }

    #[tokio::test]
    async fn reclaim_expired_guard_only_writes_the_resource_shard() {
        let manager = Arc::new(GlobalLockManager::new());
        let client = LocalClient::with_manager_and_reaper_interval(manager, Duration::from_secs(60));
        client.reaper_started.store(true, Ordering::Release);
        let resource = crate::ObjectKey::new("bucket", "resource-shard-expired");
        let target_shard = client.get_resource_shard_index(&resource);
        let expired = request(resource.clone(), "expired-owner", Duration::ZERO);
        assert!(client.acquire_lock(&expired).await.unwrap().success);

        let mut unrelated_writes = Vec::new();
        for (index, shard) in client.guard_storage.iter().enumerate() {
            if index != target_shard {
                unrelated_writes.push(shard.write().await);
            }
        }

        let replacement = request(resource, "replacement-owner", Duration::from_secs(30));
        let response = client
            .acquire_lock(&replacement)
            .now_or_never()
            .expect("expired guard reclaim must only write the resource shard")
            .expect("local acquire should return a response");
        assert!(response.success);

        drop(unrelated_writes);
        assert!(client.release(&replacement.lock_id).await.unwrap());
    }

    #[tokio::test]
    async fn acquire_deadline_includes_resource_shard_reclaim_wait() {
        let manager = Arc::new(GlobalLockManager::new());
        let client = LocalClient::with_manager_and_reaper_interval(manager, Duration::from_secs(60));
        client.reaper_started.store(true, Ordering::Release);
        let resource = crate::ObjectKey::new("bucket", "resource-shard-deadline");
        let target_shard = client.get_resource_shard_index(&resource);
        let (release_table, table_owner) = hold_guard_shard(client.guard_storage[target_shard].clone());
        let lock_request = request(resource, "owner", Duration::from_secs(30)).with_acquire_timeout(Duration::from_millis(10));

        let response = tokio::time::timeout(Duration::from_millis(100), client.acquire_lock(&lock_request))
            .await
            .expect("acquire should return after its reclaim budget expires")
            .expect("local acquire should return a response");
        assert!(!response.success);
        release_table.send(()).expect("release the occupied resource table");
        table_owner.join().expect("guard-table owner must finish");
    }

    #[tokio::test]
    async fn acquire_rejects_a_lock_id_for_another_resource() {
        let client = LocalClient::with_manager(Arc::new(GlobalLockManager::new()));
        let mut lock_request = request(crate::ObjectKey::new("bucket", "requested"), "owner", Duration::from_secs(30));
        lock_request.lock_id.resource = crate::ObjectKey::new("bucket", "other");

        let response = client.acquire_lock(&lock_request).await.unwrap();
        assert!(!response.success);
        assert!(response.error.unwrap().contains("does not match"));
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
