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

use crate::{
    ObjectKey,
    client::LockClient,
    error::{LockError, Result},
    types::{LockId, LockInfo, LockRequest, LockResponse, LockStatus, LockType},
};
use futures::future::join_all;
use rustfs_io_metrics::{
    record_read_lock_held_acquire, record_read_lock_held_release, record_write_lock_held_acquire, record_write_lock_held_release,
};
use std::sync::{Arc, LazyLock};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tracing::warn;
use uuid::Uuid;

/// Generate a new aggregate lock ID for multiple client locks
fn generate_aggregate_lock_id(resource: &ObjectKey) -> LockId {
    LockId {
        resource: resource.clone(),
        uuid: Uuid::new_v4().to_string(),
    }
}

#[derive(Debug, Clone)]
struct UnlockJob {
    /// Entries to release: each (LockId, client) pair will be released independently.
    entries: Vec<(LockId, Arc<dyn LockClient>)>,
}

#[derive(Debug)]
struct UnlockRuntime {
    tx: mpsc::Sender<UnlockJob>,
}

// Global unlock runtime with background worker
static UNLOCK_RUNTIME: LazyLock<UnlockRuntime> = LazyLock::new(|| {
    // Larger buffer to reduce contention during bursts
    let (tx, mut rx) = mpsc::channel::<UnlockJob>(8192);

    // Spawn background worker when first used; assumes a Tokio runtime is available
    tokio::spawn(async move {
        while let Some(job) = rx.recv().await {
            // Best-effort release across all (LockId, client) entries.
            let results = join_all(
                job.entries
                    .into_iter()
                    .map(|(lock_id, client)| async move { client.release(&lock_id).await.unwrap_or(false) }),
            )
            .await;
            let any_ok = results.into_iter().any(|released| released);

            if !any_ok {
                tracing::warn!("DistributedLockGuard background release failed for one or more entries");
            } else {
                tracing::debug!("DistributedLockGuard background released one or more entries");
            }
        }
    });

    UnlockRuntime { tx }
});

/// A RAII guard for distributed locks that releases the lock asynchronously when dropped.
#[derive(Debug)]
pub struct DistributedLockGuard {
    /// The public-facing lock id. For multi-client scenarios this is typically
    /// an aggregate id; for single-client it is the only id.
    lock_id: LockId,
    /// All underlying (LockId, client) entries that should be released when the
    /// guard is dropped.
    entries: Vec<(LockId, Arc<dyn LockClient>)>,
    lock_type: LockType,
    /// If true, Drop will not try to release (used if user manually released).
    disarmed: bool,
}

impl DistributedLockGuard {
    /// Create a new guard.
    ///
    /// - `lock_id` is the id returned to the caller (`lock_id()`).
    /// - `entries` is the full list of underlying (LockId, client) pairs
    ///   that should be released when this guard is dropped.
    pub(crate) fn new(lock_id: LockId, entries: Vec<(LockId, Arc<dyn LockClient>)>, lock_type: LockType) -> Self {
        record_lock_held_acquire(lock_type);
        Self {
            lock_id,
            entries,
            lock_type,
            disarmed: false,
        }
    }

    /// Get the lock id associated with this guard
    pub fn lock_id(&self) -> &LockId {
        &self.lock_id
    }

    /// Manually disarm the guard so dropping it won't release the lock.
    /// Call this if you explicitly released the lock elsewhere.
    pub fn disarm(&mut self) {
        if !self.disarmed {
            record_lock_held_release(self.lock_type);
        }
        self.disarmed = true;
    }

    /// Check if the guard has been disarmed (lock already released)
    pub fn is_disarmed(&self) -> bool {
        self.disarmed
    }

    /// Manually release the lock early.
    /// This sends a release job to the background worker and then disarms the guard
    /// to prevent double-release on drop.
    /// Returns true if the lock was released (or was already released), false otherwise.
    pub fn release(&mut self) -> bool {
        if self.disarmed {
            // Lock was already released, return true to indicate lock is in released state
            return true;
        }

        let job = UnlockJob {
            entries: self.entries.clone(),
        };

        // Try a non-blocking send to avoid panics
        let success = if let Err(err) = UNLOCK_RUNTIME.tx.try_send(job) {
            // Channel full or closed; best-effort fallback: spawn a detached task
            let entries = self.entries.clone();
            tracing::warn!(
                "DistributedLockGuard channel send failed ({}), spawning fallback unlock task for {} entries",
                err,
                entries.len()
            );

            // If runtime is not available, this will panic; but in RustFS we are inside Tokio contexts.
            let handle = tokio::spawn(async move {
                let futures_iter = entries
                    .into_iter()
                    .map(|(lock_id, client)| async move { client.release(&lock_id).await.unwrap_or(false) });
                let _ = join_all(futures_iter).await;
            });
            // Explicitly drop the JoinHandle to acknowledge detaching the task.
            drop(handle);
            true // Consider it successful even if we had to use fallback
        } else {
            true
        };

        // Disarm to prevent double-release on drop
        self.disarmed = true;
        record_lock_held_release(self.lock_type);
        success
    }
}

impl Drop for DistributedLockGuard {
    fn drop(&mut self) {
        // Call release() to handle the actual release logic
        // If already disarmed, release() will return early
        // Setting disarmed in release() is harmless here since we're dropping anyway
        let _ = self.release();
    }
}

/// Distributed lock handler for distributed use cases
/// Uses quorum-based acquisition and aggregate lock ID mapping
#[derive(Debug)]
pub struct DistributedLock {
    /// Lock clients for this namespace
    clients: Vec<Arc<dyn LockClient>>,
    /// Namespace identifier
    namespace: String,
    /// Quorum size for exclusive/write operations
    quorum: usize,
}

type LockAcquireTaskResult = (usize, Result<LockResponse>);

impl DistributedLock {
    /// Create new distributed lock
    pub fn new(namespace: String, clients: Vec<Arc<dyn LockClient>>, quorum: usize) -> Self {
        let q = if clients.len() <= 1 {
            1
        } else {
            quorum.clamp(1, clients.len())
        };

        Self {
            clients,
            namespace,
            quorum: q,
        }
    }

    /// Get namespace identifier
    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    fn read_quorum(&self) -> usize {
        let client_count = self.clients.len();
        if client_count <= 1 {
            1
        } else {
            client_count - (client_count / 2)
        }
    }

    fn required_quorum(&self, lock_type: LockType) -> usize {
        match lock_type {
            LockType::Shared => self.read_quorum(),
            LockType::Exclusive => self.quorum,
        }
    }

    /// Get resource key for this namespace
    pub fn get_resource_key(&self, resource: &ObjectKey) -> String {
        format!("{}:{}", self.namespace, resource)
    }

    /// Get clients (for health check and stats)
    pub(crate) fn clients(&self) -> &[Arc<dyn LockClient>] {
        &self.clients
    }

    /// Acquire a lock and return a RAII guard
    pub(crate) async fn acquire_guard(&self, request: &LockRequest) -> Result<Option<DistributedLockGuard>> {
        if self.clients.is_empty() {
            return Err(LockError::internal("No lock clients available"));
        }

        let required_quorum = self.required_quorum(request.lock_type);
        let (resp, individual_locks) = self.acquire_lock_quorum(request).await?;
        if resp.success {
            // Use aggregate lock_id from LockResponse's LockInfo
            // The aggregate id is what we expose to callers; individual_locks carries
            // the real (LockId, client) pairs that must be released.
            let aggregate_lock_id = resp
                .lock_info
                .as_ref()
                .map(|info| info.id.clone())
                .unwrap_or_else(|| LockId::new_unique(&request.resource));

            Ok(Some(DistributedLockGuard::new(aggregate_lock_id, individual_locks, request.lock_type)))
        } else {
            // Check if it's a timeout or quorum failure
            if let Some(error_msg) = &resp.error {
                if request.suppress_contention_logs {
                    tracing::debug!(
                        resource = %request.resource,
                        owner = %request.owner,
                        "acquire_lock_quorum contention: {}",
                        error_msg
                    );
                } else {
                    warn!(
                        resource = %request.resource,
                        owner = %request.owner,
                        "acquire_lock_quorum error: {}",
                        error_msg
                    );
                }
                if error_msg.contains("quorum") {
                    // This is a quorum failure - return appropriate error
                    let achieved = individual_locks.len();
                    Err(LockError::QuorumNotReached {
                        required: required_quorum,
                        achieved,
                    })
                } else if error_msg.contains("timeout") || resp.wait_time >= request.acquire_timeout {
                    // This is a timeout - return None so caller can convert to timeout error
                    Ok(None)
                } else {
                    // Other failure - return None for backward compatibility
                    Ok(None)
                }
            } else {
                Ok(None)
            }
        }
    }

    /// Convenience: acquire exclusive lock as a guard
    pub async fn lock_guard(
        &self,
        resource: ObjectKey,
        owner: &str,
        timeout: Duration,
        ttl: Duration,
    ) -> Result<Option<DistributedLockGuard>> {
        let req = LockRequest::new(resource, LockType::Exclusive, owner)
            .with_acquire_timeout(timeout)
            .with_ttl(ttl);
        self.acquire_guard(&req).await
    }

    /// Convenience: acquire exclusive lock with expected contention logs suppressed
    pub async fn lock_guard_quiet(
        &self,
        resource: ObjectKey,
        owner: &str,
        timeout: Duration,
        ttl: Duration,
    ) -> Result<Option<DistributedLockGuard>> {
        let req = LockRequest::new(resource, LockType::Exclusive, owner)
            .with_acquire_timeout(timeout)
            .with_ttl(ttl)
            .with_suppress_contention_logs(true);
        self.acquire_guard(&req).await
    }

    /// Convenience: acquire shared lock as a guard
    pub async fn rlock_guard(
        &self,
        resource: ObjectKey,
        owner: &str,
        timeout: Duration,
        ttl: Duration,
    ) -> Result<Option<DistributedLockGuard>> {
        let req = LockRequest::new(resource, LockType::Shared, owner)
            .with_acquire_timeout(timeout)
            .with_ttl(ttl);
        self.acquire_guard(&req).await
    }

    fn spawn_lock_requests(&self, request: &LockRequest) -> JoinSet<LockAcquireTaskResult> {
        let mut pending = JoinSet::new();
        for (idx, client) in self.clients.iter().cloned().enumerate() {
            let request = request.clone();
            pending.spawn(async move { (idx, client.acquire_lock(&request).await) });
        }
        pending
    }

    async fn release_entries(entries: &[(LockId, Arc<dyn LockClient>)], context: &'static str) {
        let release_results = join_all(
            entries
                .iter()
                .map(|(lock_id, client)| async move { (lock_id, client.release(lock_id).await) }),
        )
        .await;

        for (lock_id, result) in release_results {
            match result {
                Ok(true) | Ok(false) => {}
                Err(err) => {
                    tracing::warn!("{context}: failed to release lock {} on client: {}", lock_id, err);
                }
            }
        }
    }

    fn spawn_pending_cleanup(
        mut pending: JoinSet<LockAcquireTaskResult>,
        clients: Vec<Arc<dyn LockClient>>,
        fallback_lock_id: LockId,
        context: &'static str,
    ) {
        let handle = tokio::spawn(async move {
            while let Some(join_result) = pending.join_next().await {
                match join_result {
                    Ok((idx, Ok(resp))) if resp.success => {
                        let lock_id = resp
                            .lock_info
                            .as_ref()
                            .map(|info| info.id.clone())
                            .unwrap_or_else(|| fallback_lock_id.clone());
                        let Some(client) = clients.get(idx) else {
                            tracing::warn!("{context}: missing client for pending lock cleanup at index {}", idx);
                            continue;
                        };

                        if let Err(err) = client.release(&lock_id).await {
                            tracing::warn!("{context}: failed to cleanup late lock {} on client {}: {}", lock_id, idx, err);
                        }
                    }
                    Ok((idx, Ok(resp))) => {
                        tracing::debug!(
                            "{context}: pending lock request on client {} completed without success: {:?}",
                            idx,
                            resp.error
                        );
                    }
                    Ok((idx, Err(err))) => {
                        tracing::warn!("{context}: pending lock request on client {} failed: {}", idx, err);
                    }
                    Err(err) => {
                        tracing::warn!("{context}: pending lock cleanup task join failed: {}", err);
                    }
                }
            }
        });
        drop(handle);
    }

    fn log_failed_lock_response(&self, request: &LockRequest, idx: usize, error: String) {
        if request.suppress_contention_logs {
            tracing::debug!(
                resource = %request.resource,
                owner = %request.owner,
                "Failed to acquire lock on client from response: {}, error: {}",
                idx,
                error
            );
        } else {
            tracing::warn!(
                resource = %request.resource,
                owner = %request.owner,
                "Failed to acquire lock on client from response: {}, error: {}",
                idx,
                error
            );
        }
    }

    /// Quorum-based lock acquisition: success if at least the required quorum succeeds.
    /// Collects all individual lock_ids from successful clients and creates an aggregate lock_id.
    /// Returns the LockResponse with aggregate lock_id and individual lock mappings.
    async fn acquire_lock_quorum(&self, request: &LockRequest) -> Result<(LockResponse, Vec<(LockId, Arc<dyn LockClient>)>)> {
        let required_quorum = self.required_quorum(request.lock_type);
        let mut pending = self.spawn_lock_requests(request);
        let mut individual_locks: Vec<(LockId, Arc<dyn LockClient>)> = Vec::new();
        let fallback_lock_id = request.lock_id.clone();

        while let Some(join_result) = pending.join_next().await {
            match join_result {
                Ok((idx, Ok(resp))) => {
                    if resp.success {
                        let lock_id = resp
                            .lock_info
                            .as_ref()
                            .map(|info| info.id.clone())
                            .unwrap_or_else(|| fallback_lock_id.clone());

                        if let Some(client) = self.clients.get(idx) {
                            individual_locks.push((lock_id, client.clone()));
                        } else {
                            tracing::warn!("Missing lock client at index {} while recording success", idx);
                        }
                    } else {
                        let error = resp.error.unwrap_or_else(|| "unknown error".to_string());
                        self.log_failed_lock_response(request, idx, error);
                    }
                }
                Ok((idx, Err(err))) => {
                    tracing::warn!("Failed to acquire lock on client {}: {}", idx, err);
                }
                Err(err) => {
                    tracing::warn!("Lock acquisition task join failed: {}", err);
                }
            }

            if individual_locks.len() >= required_quorum {
                if !pending.is_empty() {
                    Self::spawn_pending_cleanup(
                        pending,
                        self.clients.clone(),
                        fallback_lock_id.clone(),
                        "distributed_lock_success_cleanup",
                    );
                }

                let aggregate_lock_id = generate_aggregate_lock_id(&request.resource);
                tracing::debug!(
                    "Generated aggregate lock_id {} for {} individual locks on resource {}",
                    aggregate_lock_id,
                    individual_locks.len(),
                    request.resource
                );

                let resp = LockResponse::success(
                    LockInfo {
                        id: aggregate_lock_id,
                        resource: request.resource.clone(),
                        lock_type: request.lock_type,
                        status: LockStatus::Acquired,
                        owner: request.owner.clone(),
                        acquired_at: std::time::SystemTime::now(),
                        expires_at: std::time::SystemTime::now() + request.ttl,
                        last_refreshed: std::time::SystemTime::now(),
                        metadata: request.metadata.clone(),
                        priority: request.priority,
                        wait_start_time: None,
                    },
                    Duration::ZERO,
                );
                return Ok((resp, individual_locks));
            }

            if individual_locks.len() + pending.len() < required_quorum {
                let rollback_count = individual_locks.len();
                Self::release_entries(&individual_locks, "distributed_lock_quorum_rollback").await;
                if !pending.is_empty() {
                    Self::spawn_pending_cleanup(
                        pending,
                        self.clients.clone(),
                        fallback_lock_id.clone(),
                        "distributed_lock_failure_cleanup",
                    );
                }

                let resp = LockResponse::failure(
                    format!("Failed to acquire quorum: {rollback_count}/{required_quorum} required"),
                    Duration::ZERO,
                );
                return Ok((resp, individual_locks));
            }
        }

        let rollback_count = individual_locks.len();
        Self::release_entries(&individual_locks, "distributed_lock_quorum_rollback").await;
        let resp = LockResponse::failure(
            format!("Failed to acquire quorum: {rollback_count}/{required_quorum} required"),
            Duration::ZERO,
        );
        Ok((resp, individual_locks))
    }
}

#[inline(always)]
fn record_lock_held_acquire(lock_type: LockType) {
    match lock_type {
        LockType::Shared => record_read_lock_held_acquire(),
        LockType::Exclusive => record_write_lock_held_acquire(),
    }
}

#[inline(always)]
fn record_lock_held_release(lock_type: LockType) {
    match lock_type {
        LockType::Shared => record_read_lock_held_release(),
        LockType::Exclusive => record_write_lock_held_release(),
    }
}
