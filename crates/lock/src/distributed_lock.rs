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
use std::sync::{Arc, LazyLock};
use std::time::Duration;
use tokio::sync::mpsc;
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
            let mut any_ok = false;
            for (lock_id, client) in job.entries.into_iter() {
                if client.release(&lock_id).await.unwrap_or(false) {
                    any_ok = true;
                }
            }

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
    /// If true, Drop will not try to release (used if user manually released).
    disarmed: bool,
}

impl DistributedLockGuard {
    /// Create a new guard.
    ///
    /// - `lock_id` is the id returned to the caller (`lock_id()`).
    /// - `entries` is the full list of underlying (LockId, client) pairs
    ///   that should be released when this guard is dropped.
    pub(crate) fn new(lock_id: LockId, entries: Vec<(LockId, Arc<dyn LockClient>)>) -> Self {
        Self {
            lock_id,
            entries,
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
                let _ = futures::future::join_all(futures_iter).await;
            });
            // Explicitly drop the JoinHandle to acknowledge detaching the task.
            drop(handle);
            true // Consider it successful even if we had to use fallback
        } else {
            true
        };

        // Disarm to prevent double-release on drop
        self.disarmed = true;
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
    /// Quorum size for operations (majority for distributed)
    quorum: usize,
}

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

            Ok(Some(DistributedLockGuard::new(aggregate_lock_id, individual_locks)))
        } else {
            // Check if it's a timeout or quorum failure
            if let Some(error_msg) = &resp.error {
                warn!("acquire_lock_quorum error: {}", error_msg);
                if error_msg.contains("quorum") {
                    // This is a quorum failure - return appropriate error
                    // Extract achieved count from error message or use individual_locks.len()
                    let achieved = individual_locks.len();
                    Err(LockError::QuorumNotReached {
                        required: self.quorum,
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

    /// Quorum-based lock acquisition: success if at least `self.quorum` clients succeed.
    /// Collects all individual lock_ids from successful clients and creates an aggregate lock_id.
    /// Returns the LockResponse with aggregate lock_id and individual lock mappings.
    async fn acquire_lock_quorum(&self, request: &LockRequest) -> Result<(LockResponse, Vec<(LockId, Arc<dyn LockClient>)>)> {
        let futs: Vec<_> = self
            .clients
            .iter()
            .enumerate()
            .map(|(idx, client)| async move { (idx, client.acquire_lock(request).await) })
            .collect();

        let results = futures::future::join_all(futs).await;
        // Store all individual lock_ids and their corresponding clients
        let mut individual_locks: Vec<(LockId, Arc<dyn LockClient>)> = Vec::new();

        for (idx, result) in results {
            match result {
                Ok(resp) => {
                    if resp.success {
                        // Collect individual lock_id and client for each successful acquisition
                        if let Some(lock_info) = &resp.lock_info
                            && idx < self.clients.len()
                        {
                            // Save the individual lock_id returned by each client
                            individual_locks.push((lock_info.id.clone(), self.clients[idx].clone()));
                        }
                    } else {
                        tracing::warn!(
                            "Failed to acquire lock on client from response: {}, error: {}",
                            idx,
                            resp.error.unwrap_or_else(|| "unknown error".to_string())
                        );
                    }
                }
                Err(e) => {
                    tracing::warn!("Failed to acquire lock on client {}: {}", idx, e);
                }
            }
        }

        if individual_locks.len() >= self.quorum {
            // Generate a new aggregate lock_id for multiple client locks
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
            Ok((resp, individual_locks))
        } else {
            // Rollback: release all locks that were successfully acquired
            let rollback_count = individual_locks.len();
            for (individual_lock_id, client) in individual_locks {
                if let Err(e) = client.release(&individual_lock_id).await {
                    tracing::warn!("Failed to rollback lock {} on client: {}", individual_lock_id, e);
                }
            }

            let resp = LockResponse::failure(
                format!("Failed to acquire quorum: {}/{} required", rollback_count, self.quorum),
                Duration::ZERO,
            );
            Ok((resp, Vec::new()))
        }
    }
}
