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
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinSet;
use tracing::{debug, warn};
use uuid::Uuid;

const UNLOCK_RETRY_ATTEMPTS: usize = 3;
const UNLOCK_RETRY_BACKOFF: Duration = Duration::from_millis(100);
const LOCK_ACQUIRE_RETRY_INITIAL_BACKOFF: Duration = Duration::from_millis(250);
const LOCK_ACQUIRE_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(1);
const REMOTE_LOCK_RPC_FAILED_PREFIX: &str = "remote lock rpc failed:";
const REMOTE_LOCK_RPC_TIMED_OUT_PREFIX: &str = "remote lock rpc timed out:";
const UNRECOVERABLE_QUORUM_FAILURE_PREFIX: &str = "unrecoverable quorum failure";

#[derive(Debug)]
enum LockAcquireFailureKind {
    NonRetryable,
    RetryableContention,
    UnrecoverableQuorum,
}

/// Generate a new aggregate lock ID for multiple client locks
fn generate_aggregate_lock_id(resource: &ObjectKey) -> LockId {
    LockId {
        resource: resource.clone(),
        uuid: Uuid::new_v4().to_string(),
    }
}

fn is_remote_lock_rpc_failure(error: &str) -> bool {
    has_case_insensitive_prefix(error, REMOTE_LOCK_RPC_FAILED_PREFIX)
        || has_case_insensitive_prefix(error, REMOTE_LOCK_RPC_TIMED_OUT_PREFIX)
}

fn is_remote_lock_rpc_timeout(error: &str) -> bool {
    has_case_insensitive_prefix(error, REMOTE_LOCK_RPC_TIMED_OUT_PREFIX)
}

fn has_case_insensitive_prefix(error: &str, expected_prefix: &str) -> bool {
    error
        .get(0..expected_prefix.len())
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case(expected_prefix))
}

fn is_unrecoverable_quorum_error(error: &str) -> bool {
    error
        .get(0..UNRECOVERABLE_QUORUM_FAILURE_PREFIX.len())
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case(UNRECOVERABLE_QUORUM_FAILURE_PREFIX))
}

fn classify_lock_failure(error: &str) -> LockAcquireFailureKind {
    if is_unrecoverable_quorum_error(error) {
        return LockAcquireFailureKind::UnrecoverableQuorum;
    }

    if error.to_ascii_lowercase().contains("timeout") || is_remote_lock_rpc_failure(error) {
        return LockAcquireFailureKind::RetryableContention;
    }

    LockAcquireFailureKind::NonRetryable
}

fn should_warn_lock_failure(error: &str) -> bool {
    if is_remote_lock_rpc_failure(error) {
        return !is_remote_lock_rpc_timeout(error);
    }

    matches!(
        classify_lock_failure(error),
        LockAcquireFailureKind::NonRetryable | LockAcquireFailureKind::UnrecoverableQuorum
    )
}

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
    /// This spawns a background release task and then disarms the guard
    /// to prevent double-release on drop.
    /// Returns true if release was scheduled or the guard was already disarmed.
    pub fn release(&mut self) -> bool {
        if self.disarmed {
            // Lock was already released, return true to indicate lock is in released state
            return true;
        }

        let entries = self.entries.clone();
        DistributedLock::spawn_release_cleanup(entries, "distributed_lock_guard_release");

        // Disarm to prevent double-release on drop
        self.disarmed = true;
        record_lock_held_release(self.lock_type);
        true
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

struct LockAcquireQuorumResult {
    response: LockResponse,
    individual_locks: Vec<(LockId, Arc<dyn LockClient>)>,
    failure_kind: Option<LockAcquireFailureKind>,
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
        let LockAcquireQuorumResult {
            response: resp,
            individual_locks,
            failure_kind,
            ..
        } = self.acquire_lock_quorum_with_retry(request).await?;
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
                } else if matches!(failure_kind, Some(LockAcquireFailureKind::NonRetryable)) {
                    warn!(
                        resource = %request.resource,
                        owner = %request.owner,
                        "acquire_lock_quorum error: {}",
                        error_msg
                    );
                } else if matches!(failure_kind, Some(LockAcquireFailureKind::RetryableContention)) {
                    debug!(
                        resource = %request.resource,
                        owner = %request.owner,
                        "acquire_lock_quorum contention: {}",
                        error_msg
                    );
                } else {
                    debug!(
                        resource = %request.resource,
                        owner = %request.owner,
                        "acquire_lock_quorum final failure: {}",
                        error_msg
                    );
                }

                if matches!(failure_kind, Some(LockAcquireFailureKind::UnrecoverableQuorum)) {
                    let achieved = individual_locks.len();
                    Err(LockError::QuorumNotReached {
                        required: required_quorum,
                        achieved,
                    })
                } else {
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

    fn lock_acquire_retry_backoff(attempt: usize) -> Duration {
        LOCK_ACQUIRE_RETRY_INITIAL_BACKOFF * attempt as u32
    }

    fn lock_acquire_attempt_timeout(&self, remaining: Duration) -> Duration {
        if self.clients.len() <= 1 {
            remaining
        } else {
            remaining.min(LOCK_ACQUIRE_ATTEMPT_TIMEOUT)
        }
    }

    async fn release_entries(entries: Vec<(LockId, Arc<dyn LockClient>)>, context: &'static str) {
        let mut pending = entries;

        for attempt in 1..=UNLOCK_RETRY_ATTEMPTS {
            let release_results = join_all(pending.into_iter().map(|(lock_id, client)| async move {
                match client.release(&lock_id).await {
                    Ok(true) => None,
                    Ok(false) => {
                        warn!(%lock_id, attempt, context, "distributed unlock did not find lock on client");
                        Some((lock_id, client))
                    }
                    Err(err) => {
                        warn!(%lock_id, attempt, context, "distributed unlock failed on client: {}", err);
                        Some((lock_id, client))
                    }
                }
            }))
            .await;

            pending = release_results.into_iter().flatten().collect();
            if pending.is_empty() {
                debug!(attempt, context, "distributed unlock completed");
                return;
            }

            if attempt < UNLOCK_RETRY_ATTEMPTS {
                tokio::time::sleep(UNLOCK_RETRY_BACKOFF * attempt as u32).await;
            }
        }

        warn!(
            remaining = pending.len(),
            attempts = UNLOCK_RETRY_ATTEMPTS,
            context,
            "distributed unlock left unreleased entries after retry"
        );
    }

    fn spawn_release_cleanup(entries: Vec<(LockId, Arc<dyn LockClient>)>, context: &'static str) {
        if entries.is_empty() {
            return;
        }

        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            let join_handle = handle.spawn(async move {
                Self::release_entries(entries, context).await;
            });
            drop(join_handle);
            return;
        }

        let join_handle = std::thread::spawn(move || match tokio::runtime::Builder::new_current_thread().enable_all().build() {
            Ok(runtime) => runtime.block_on(async move {
                Self::release_entries(entries, context).await;
            }),
            Err(err) => warn!(context, "failed to create fallback unlock runtime: {}", err),
        });
        drop(join_handle);
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

                        Self::release_entries(vec![(lock_id, client.clone())], context).await;
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
        if request.suppress_contention_logs || !should_warn_lock_failure(&error) {
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

    async fn acquire_lock_quorum_with_retry(&self, request: &LockRequest) -> Result<LockAcquireQuorumResult> {
        let start = std::time::Instant::now();
        let mut attempt = 1;
        let mut last_result = None;

        loop {
            let elapsed = start.elapsed();
            if elapsed >= request.acquire_timeout {
                break;
            }

            let remaining = request.acquire_timeout - elapsed;
            let mut attempt_request = request.clone();
            attempt_request.acquire_timeout = self.lock_acquire_attempt_timeout(remaining);
            attempt_request.lock_id = LockId::new_unique(&request.resource);

            let result = self.acquire_lock_quorum_once(&attempt_request).await?;
            if result.response.success || !matches!(result.failure_kind, Some(LockAcquireFailureKind::RetryableContention)) {
                return Ok(result);
            }

            last_result = Some(result);
            let backoff = Self::lock_acquire_retry_backoff(attempt);
            if start.elapsed().saturating_add(backoff) >= request.acquire_timeout {
                break;
            }

            tokio::time::sleep(backoff).await;
            attempt += 1;
        }

        Ok(last_result.unwrap_or_else(|| LockAcquireQuorumResult {
            response: LockResponse::failure("Lock acquisition timeout", request.acquire_timeout),
            individual_locks: Vec::new(),
            failure_kind: Some(LockAcquireFailureKind::RetryableContention),
        }))
    }

    /// Quorum-based lock acquisition: success if at least the required quorum succeeds.
    /// Collects all individual lock_ids from successful clients and creates an aggregate lock_id.
    /// Returns the LockResponse with aggregate lock_id and individual lock mappings.
    async fn acquire_lock_quorum_once(&self, request: &LockRequest) -> Result<LockAcquireQuorumResult> {
        let required_quorum = self.required_quorum(request.lock_type);
        let mut pending = self.spawn_lock_requests(request);
        let mut individual_locks: Vec<(LockId, Arc<dyn LockClient>)> = Vec::new();
        let fallback_lock_id = request.lock_id.clone();
        let mut last_failure = None;
        let mut hard_failures = 0usize;

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
                        if is_remote_lock_rpc_failure(&error) && !is_remote_lock_rpc_timeout(&error) {
                            hard_failures += 1;
                        }
                        self.log_failed_lock_response(request, idx, error.clone());
                        last_failure = Some(error);
                    }
                }
                Ok((idx, Err(err))) => {
                    hard_failures += 1;
                    tracing::warn!("Failed to acquire lock on client {}: {}", idx, err);
                    last_failure = Some(err.to_string());
                }
                Err(err) => {
                    hard_failures += 1;
                    tracing::warn!("Lock acquisition task join failed: {}", err);
                    last_failure = Some(err.to_string());
                }
            }

            if self.clients.len().saturating_sub(hard_failures) < required_quorum {
                let rollback_count = individual_locks.len();
                Self::spawn_release_cleanup(individual_locks.clone(), "distributed_lock_quorum_rollback");
                if !pending.is_empty() {
                    Self::spawn_pending_cleanup(
                        pending,
                        self.clients.clone(),
                        fallback_lock_id.clone(),
                        "distributed_lock_failure_cleanup",
                    );
                }

                let mut error = format!(
                    "Unrecoverable quorum failure: {rollback_count}/{required_quorum} required; {hard_failures} clients failed"
                );
                if let Some(last_failure) = last_failure {
                    error.push_str("; last failure: ");
                    error.push_str(&last_failure);
                }
                let resp = LockResponse::failure(error, Duration::ZERO);
                return Ok(LockAcquireQuorumResult {
                    response: resp,
                    individual_locks,
                    failure_kind: Some(LockAcquireFailureKind::UnrecoverableQuorum),
                });
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
                return Ok(LockAcquireQuorumResult {
                    response: resp,
                    individual_locks,
                    failure_kind: None,
                });
            }

            if individual_locks.len() + pending.len() < required_quorum {
                let rollback_count = individual_locks.len();
                Self::spawn_release_cleanup(individual_locks.clone(), "distributed_lock_quorum_rollback");
                if !pending.is_empty() {
                    Self::spawn_pending_cleanup(
                        pending,
                        self.clients.clone(),
                        fallback_lock_id.clone(),
                        "distributed_lock_failure_cleanup",
                    );
                }

                let mut error = format!("Failed to acquire quorum: {rollback_count}/{required_quorum} required");
                let failure_kind = if hard_failures > 0 {
                    error = format!(
                        "Unrecoverable quorum failure: {rollback_count}/{required_quorum} required; {hard_failures} clients failed; {error}"
                    );
                    LockAcquireFailureKind::UnrecoverableQuorum
                } else {
                    LockAcquireFailureKind::RetryableContention
                };
                if let Some(last_failure) = last_failure {
                    error.push_str("; last failure: ");
                    error.push_str(&last_failure);
                }
                let resp = LockResponse::failure(error, Duration::ZERO);
                return Ok(LockAcquireQuorumResult {
                    response: resp,
                    individual_locks,
                    failure_kind: Some(failure_kind),
                });
            }
        }

        let rollback_count = individual_locks.len();
        Self::spawn_release_cleanup(individual_locks.clone(), "distributed_lock_quorum_rollback");
        let mut error = format!("Failed to acquire quorum: {rollback_count}/{required_quorum} required");
        if let Some(last_failure) = &last_failure {
            error.push_str("; last failure: ");
            error.push_str(last_failure);
        }
        let resp = LockResponse::failure(error, Duration::ZERO);
        Ok(LockAcquireQuorumResult {
            response: resp,
            individual_locks,
            failure_kind: Some(if hard_failures > 0 {
                LockAcquireFailureKind::UnrecoverableQuorum
            } else {
                let fallback_kind = last_failure.as_deref().map(classify_lock_failure);
                fallback_kind.unwrap_or(LockAcquireFailureKind::RetryableContention)
            }),
        })
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

#[cfg(test)]
mod tests {
    use super::{DistributedLock, LOCK_ACQUIRE_ATTEMPT_TIMEOUT, is_remote_lock_rpc_failure, should_warn_lock_failure};
    use crate::{LockError, LockId, LockInfo, LockRequest, LockResponse, LockStats, LockType, ObjectKey, client::LockClient};
    use std::{
        collections::{HashMap, VecDeque},
        sync::{Arc, Mutex},
        time::Duration,
    };

    #[derive(Debug)]
    struct ResponseClient {
        response: LockResponse,
        delay: Duration,
    }

    impl ResponseClient {
        fn new(response: LockResponse) -> Self {
            Self {
                response,
                delay: Duration::ZERO,
            }
        }

        fn with_delay(mut self, delay: Duration) -> Self {
            self.delay = delay;
            self
        }

        fn into_client(self) -> Arc<dyn LockClient> {
            Arc::new(self)
        }
    }

    #[async_trait::async_trait]
    impl LockClient for ResponseClient {
        async fn acquire_lock(&self, _request: &LockRequest) -> crate::Result<LockResponse> {
            if !self.delay.is_zero() {
                tokio::time::sleep(self.delay).await;
            }
            Ok(self.response.clone())
        }

        async fn release(&self, _lock_id: &LockId) -> crate::Result<bool> {
            Ok(false)
        }

        async fn refresh(&self, _lock_id: &LockId) -> crate::Result<bool> {
            Ok(false)
        }

        async fn force_release(&self, _lock_id: &LockId) -> crate::Result<bool> {
            Ok(false)
        }

        async fn check_status(&self, _lock_id: &LockId) -> crate::Result<Option<LockInfo>> {
            Ok(None)
        }

        async fn get_stats(&self) -> crate::Result<LockStats> {
            Ok(LockStats::default())
        }

        async fn close(&self) -> crate::Result<()> {
            Ok(())
        }

        async fn is_online(&self) -> bool {
            true
        }

        async fn is_local(&self) -> bool {
            false
        }
    }

    #[derive(Debug)]
    struct TimeoutRecordingClient {
        seen_timeouts: Arc<Mutex<Vec<Duration>>>,
    }

    impl TimeoutRecordingClient {
        fn new(seen_timeouts: Arc<Mutex<Vec<Duration>>>) -> Self {
            Self { seen_timeouts }
        }

        fn into_client(self) -> Arc<dyn LockClient> {
            Arc::new(self)
        }
    }

    #[async_trait::async_trait]
    impl LockClient for TimeoutRecordingClient {
        async fn acquire_lock(&self, request: &LockRequest) -> crate::Result<LockResponse> {
            self.seen_timeouts.lock().unwrap().push(request.acquire_timeout);
            Ok(LockResponse::failure("Lock acquisition timeout", Duration::ZERO))
        }

        async fn release(&self, _lock_id: &LockId) -> crate::Result<bool> {
            Ok(false)
        }

        async fn refresh(&self, _lock_id: &LockId) -> crate::Result<bool> {
            Ok(false)
        }

        async fn force_release(&self, _lock_id: &LockId) -> crate::Result<bool> {
            Ok(false)
        }

        async fn check_status(&self, _lock_id: &LockId) -> crate::Result<Option<LockInfo>> {
            Ok(None)
        }

        async fn get_stats(&self) -> crate::Result<LockStats> {
            Ok(LockStats::default())
        }

        async fn close(&self) -> crate::Result<()> {
            Ok(())
        }

        async fn is_online(&self) -> bool {
            true
        }

        async fn is_local(&self) -> bool {
            false
        }
    }

    #[derive(Debug, Clone, Copy)]
    enum AcquirePlan {
        Success { delay: Duration },
        Failure { error: &'static str, delay: Duration },
    }

    #[derive(Debug)]
    struct SequencedClient {
        plans: Mutex<VecDeque<AcquirePlan>>,
        active: tokio::sync::Mutex<HashMap<LockId, LockInfo>>,
        seen_ids: Arc<Mutex<Vec<LockId>>>,
    }

    impl SequencedClient {
        fn new(plans: Vec<AcquirePlan>, seen_ids: Arc<Mutex<Vec<LockId>>>) -> Self {
            Self {
                plans: Mutex::new(plans.into()),
                active: tokio::sync::Mutex::new(HashMap::new()),
                seen_ids,
            }
        }
    }

    #[async_trait::async_trait]
    impl LockClient for SequencedClient {
        async fn acquire_lock(&self, request: &LockRequest) -> crate::Result<LockResponse> {
            self.seen_ids.lock().unwrap().push(request.lock_id.clone());

            let plan = self
                .plans
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or(AcquirePlan::Success { delay: Duration::ZERO });

            match plan {
                AcquirePlan::Success { delay } => {
                    if !delay.is_zero() {
                        tokio::time::sleep(delay).await;
                    }

                    let lock_info = LockInfo {
                        id: request.lock_id.clone(),
                        resource: request.resource.clone(),
                        lock_type: request.lock_type,
                        status: crate::types::LockStatus::Acquired,
                        owner: request.owner.clone(),
                        acquired_at: std::time::SystemTime::now(),
                        expires_at: std::time::SystemTime::now() + request.ttl,
                        last_refreshed: std::time::SystemTime::now(),
                        metadata: request.metadata.clone(),
                        priority: request.priority,
                        wait_start_time: None,
                    };

                    self.active.lock().await.insert(request.lock_id.clone(), lock_info.clone());

                    Ok(LockResponse::success(lock_info, Duration::ZERO))
                }
                AcquirePlan::Failure { error, delay } => {
                    if !delay.is_zero() {
                        tokio::time::sleep(delay).await;
                    }
                    Ok(LockResponse::failure(error, Duration::ZERO))
                }
            }
        }

        async fn release(&self, lock_id: &LockId) -> crate::Result<bool> {
            Ok(self.active.lock().await.remove(lock_id).is_some())
        }

        async fn refresh(&self, _lock_id: &LockId) -> crate::Result<bool> {
            Ok(false)
        }

        async fn force_release(&self, lock_id: &LockId) -> crate::Result<bool> {
            self.release(lock_id).await
        }

        async fn check_status(&self, lock_id: &LockId) -> crate::Result<Option<LockInfo>> {
            Ok(self.active.lock().await.get(lock_id).cloned())
        }

        async fn get_stats(&self) -> crate::Result<LockStats> {
            Ok(LockStats::default())
        }

        async fn close(&self) -> crate::Result<()> {
            Ok(())
        }

        async fn is_online(&self) -> bool {
            true
        }

        async fn is_local(&self) -> bool {
            false
        }
    }

    #[test]
    fn test_is_remote_lock_rpc_failure() {
        assert!(is_remote_lock_rpc_failure("Remote lock RPC failed: backend unreachable"));
        assert!(is_remote_lock_rpc_failure("remote lock rpc failed: temporary network issue"));
        assert!(is_remote_lock_rpc_failure("Remote lock RPC timed out: RPC timed out after 50ms"));
        assert!(!is_remote_lock_rpc_failure("Lock is already held"));
        assert!(!is_remote_lock_rpc_failure("acquired lock failed for other reason"));
    }

    #[test]
    fn test_should_warn_lock_failure() {
        assert!(should_warn_lock_failure("Remote lock RPC failed: backend unreachable"));
        assert!(should_warn_lock_failure("permission denied"));
        assert!(should_warn_lock_failure("Unrecoverable quorum failure: 1/3 required"));
        assert!(!should_warn_lock_failure("Remote lock RPC timed out: RPC timed out after 50ms"));
        assert!(!should_warn_lock_failure("Lock acquisition timeout"));
    }

    #[tokio::test]
    async fn acquire_guard_returns_quorum_error_when_rpc_failures_make_quorum_impossible() {
        let clients: Vec<Arc<dyn LockClient>> = vec![
            ResponseClient::new(LockResponse::failure("Remote lock RPC failed: node unavailable", Duration::ZERO)).into_client(),
            ResponseClient::new(LockResponse::failure("lock already held", Duration::ZERO))
                .with_delay(Duration::from_millis(50))
                .into_client(),
            ResponseClient::new(LockResponse::failure("Remote lock RPC failed: connection refused", Duration::ZERO))
                .into_client(),
            ResponseClient::new(LockResponse::failure("lock already held", Duration::ZERO))
                .with_delay(Duration::from_millis(50))
                .into_client(),
        ];
        let lock = DistributedLock::new("test".to_string(), clients, 3);
        let request = LockRequest::new(ObjectKey::new("bucket", "object"), LockType::Exclusive, "owner")
            .with_acquire_timeout(Duration::from_secs(1));

        let result = lock.acquire_guard(&request).await;

        assert!(
            matches!(
                result,
                Err(LockError::QuorumNotReached {
                    required: 3,
                    achieved: 0
                })
            ),
            "unexpected result: {result:?}"
        );
    }

    #[tokio::test]
    async fn acquire_guard_retries_rpc_timeouts_until_caller_timeout() {
        let clients: Vec<Arc<dyn LockClient>> = vec![
            ResponseClient::new(LockResponse::failure(
                "Remote lock RPC timed out: RPC timed out after 50ms",
                Duration::ZERO,
            ))
            .into_client(),
            ResponseClient::new(LockResponse::failure("lock already held", Duration::ZERO))
                .with_delay(Duration::from_millis(50))
                .into_client(),
            ResponseClient::new(LockResponse::failure(
                "Remote lock RPC timed out: RPC timed out after 50ms",
                Duration::ZERO,
            ))
            .into_client(),
            ResponseClient::new(LockResponse::failure("lock already held", Duration::ZERO))
                .with_delay(Duration::from_millis(50))
                .into_client(),
        ];
        let lock = DistributedLock::new("test".to_string(), clients, 3);
        let request = LockRequest::new(ObjectKey::new("bucket", "object"), LockType::Exclusive, "owner")
            .with_acquire_timeout(Duration::from_millis(900));

        let started = tokio::time::Instant::now();
        let result = lock.acquire_guard(&request).await;
        let elapsed = started.elapsed();

        assert!(matches!(result, Ok(None)), "unexpected result: {result:?}");
        assert!(
            elapsed >= Duration::from_millis(250),
            "remote RPC timeouts should be retried within the caller timeout, got {elapsed:?}"
        );
    }

    #[tokio::test]
    async fn acquire_guard_returns_timeout_when_zero_locks_make_quorum_impossible_for_attempt() {
        let clients: Vec<Arc<dyn LockClient>> = vec![
            ResponseClient::new(LockResponse::failure("lock already held", Duration::ZERO)).into_client(),
            ResponseClient::new(LockResponse::failure("lock already held", Duration::ZERO)).into_client(),
            ResponseClient::new(LockResponse::failure("lock already held", Duration::ZERO))
                .with_delay(Duration::from_secs(1))
                .into_client(),
            ResponseClient::new(LockResponse::failure("lock already held", Duration::ZERO))
                .with_delay(Duration::from_secs(1))
                .into_client(),
        ];
        let lock = DistributedLock::new("test".to_string(), clients, 3);
        let request = LockRequest::new(ObjectKey::new("bucket", "object"), LockType::Exclusive, "owner")
            .with_acquire_timeout(Duration::from_millis(120));

        let started = tokio::time::Instant::now();
        let result = lock.acquire_guard(&request).await;

        assert!(matches!(result, Ok(None)), "unexpected result: {result:?}");
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "acquire should fail this attempt before waiting for delayed impossible-quorum tasks"
        );
    }

    #[tokio::test]
    async fn acquire_guard_retries_transient_timeout_before_quorum() {
        let clients: Vec<Arc<dyn LockClient>> = vec![
            ResponseClient::new(LockResponse::failure(
                "Timeout { resource: \"bucket/object-flaky-acquire@latest\", timeout: 1s }",
                Duration::ZERO,
            ))
            .into_client(),
            ResponseClient::new(LockResponse::failure(
                "Timeout { resource: \"bucket/object-flaky-acquire@latest\", timeout: 1s }",
                Duration::ZERO,
            ))
            .into_client(),
            ResponseClient::new(LockResponse::failure(
                "Timeout { resource: \"bucket/object-flaky-acquire@latest\", timeout: 1s }",
                Duration::ZERO,
            ))
            .into_client(),
            ResponseClient::new(LockResponse::failure(
                "Timeout { resource: \"bucket/object-flaky-acquire@latest\", timeout: 1s }",
                Duration::ZERO,
            ))
            .into_client(),
        ];
        let lock = DistributedLock::new("test".to_string(), clients, 3);
        let request = LockRequest::new(ObjectKey::new("bucket", "object"), LockType::Exclusive, "owner")
            .with_acquire_timeout(Duration::from_millis(900));

        let started = tokio::time::Instant::now();
        let result = lock.acquire_guard(&request).await;
        let elapsed = started.elapsed();

        assert!(matches!(result, Ok(None)), "unexpected result: {result:?}");
        assert!(
            elapsed >= Duration::from_millis(250),
            "expected at least one retry attempt for transient timeout, got {elapsed:?}"
        );
    }

    #[tokio::test]
    async fn acquire_guard_uses_bounded_distributed_attempt_timeout() {
        let seen_timeouts = Arc::new(Mutex::new(Vec::new()));
        let clients: Vec<Arc<dyn LockClient>> = (0..4)
            .map(|_| TimeoutRecordingClient::new(seen_timeouts.clone()).into_client())
            .collect();
        let lock = DistributedLock::new("test".to_string(), clients, 3);
        let request = LockRequest::new(ObjectKey::new("bucket", "object"), LockType::Exclusive, "owner")
            .with_acquire_timeout(Duration::from_millis(1500));

        let result = lock.acquire_guard(&request).await;

        assert!(matches!(result, Ok(None)), "unexpected result: {result:?}");
        let seen_timeouts = seen_timeouts.lock().unwrap();
        assert!(!seen_timeouts.is_empty(), "expected lock clients to observe acquire timeouts");
        assert!(
            seen_timeouts.iter().all(|timeout| *timeout <= LOCK_ACQUIRE_ATTEMPT_TIMEOUT),
            "distributed attempts should be bounded by {LOCK_ACQUIRE_ATTEMPT_TIMEOUT:?}, saw {seen_timeouts:?}"
        );
    }

    #[tokio::test]
    async fn acquire_guard_retries_partial_quorum_after_rollback() {
        let seen_ids = Arc::new(Mutex::new(Vec::new()));
        let clients: Vec<Arc<dyn LockClient>> = vec![
            Arc::new(SequencedClient::new(
                vec![
                    AcquirePlan::Success { delay: Duration::ZERO },
                    AcquirePlan::Success { delay: Duration::ZERO },
                ],
                seen_ids.clone(),
            )),
            Arc::new(SequencedClient::new(
                vec![
                    AcquirePlan::Failure {
                        error: "Lock acquisition timeout",
                        delay: Duration::ZERO,
                    },
                    AcquirePlan::Success { delay: Duration::ZERO },
                ],
                seen_ids.clone(),
            )),
            Arc::new(SequencedClient::new(
                vec![
                    AcquirePlan::Failure {
                        error: "Lock acquisition timeout",
                        delay: Duration::ZERO,
                    },
                    AcquirePlan::Success { delay: Duration::ZERO },
                ],
                seen_ids.clone(),
            )),
            Arc::new(SequencedClient::new(
                vec![
                    AcquirePlan::Failure {
                        error: "Lock acquisition timeout",
                        delay: Duration::ZERO,
                    },
                    AcquirePlan::Success { delay: Duration::ZERO },
                ],
                seen_ids.clone(),
            )),
        ];
        let lock = DistributedLock::new("test".to_string(), clients, 3);
        let request = LockRequest::new(ObjectKey::new("bucket", "object"), LockType::Exclusive, "owner")
            .with_acquire_timeout(Duration::from_secs(2));

        let guard = lock
            .acquire_guard(&request)
            .await
            .expect("partial quorum retry should not fail")
            .expect("second attempt should reach quorum");

        let unique_id_count = seen_ids
            .lock()
            .unwrap()
            .iter()
            .fold(Vec::<String>::new(), |mut uuids, lock_id| {
                if !uuids.iter().any(|uuid| uuid == &lock_id.uuid) {
                    uuids.push(lock_id.uuid.clone());
                }
                uuids
            })
            .len();

        assert!(unique_id_count >= 2, "partial quorum should retry with a fresh lock id");
        drop(guard);
    }

    #[tokio::test]
    async fn acquire_guard_returns_timeout_when_quorum_remains_contended() {
        let clients: Vec<Arc<dyn LockClient>> = vec![
            ResponseClient::new(LockResponse::failure("lock already held", Duration::ZERO)).into_client(),
            ResponseClient::new(LockResponse::failure("lock already held", Duration::ZERO)).into_client(),
            ResponseClient::new(LockResponse::failure("lock already held", Duration::ZERO)).into_client(),
            ResponseClient::new(LockResponse::failure("lock already held", Duration::ZERO)).into_client(),
        ];
        let lock = DistributedLock::new("test".to_string(), clients, 3);
        let request = LockRequest::new(ObjectKey::new("bucket", "object"), LockType::Exclusive, "owner")
            .with_acquire_timeout(Duration::from_millis(120));

        let result = lock.acquire_guard(&request).await;

        assert!(matches!(result, Ok(None)), "unexpected result: {result:?}");
    }

    #[tokio::test]
    async fn acquire_guard_uses_fresh_lock_ids_across_retry_attempts() {
        let seen_ids = Arc::new(Mutex::new(Vec::new()));
        let delayed_success_a = Arc::new(SequencedClient::new(
            vec![
                AcquirePlan::Success {
                    delay: Duration::from_millis(400),
                },
                AcquirePlan::Success { delay: Duration::ZERO },
            ],
            seen_ids.clone(),
        ));
        let delayed_success_b = Arc::new(SequencedClient::new(
            vec![
                AcquirePlan::Success {
                    delay: Duration::from_millis(400),
                },
                AcquirePlan::Success { delay: Duration::ZERO },
            ],
            seen_ids.clone(),
        ));

        let clients: Vec<Arc<dyn LockClient>> = vec![
            Arc::new(SequencedClient::new(
                vec![
                    AcquirePlan::Failure {
                        error: "Lock acquisition timeout",
                        delay: Duration::ZERO,
                    },
                    AcquirePlan::Success { delay: Duration::ZERO },
                ],
                seen_ids.clone(),
            )),
            Arc::new(SequencedClient::new(
                vec![
                    AcquirePlan::Failure {
                        error: "Lock acquisition timeout",
                        delay: Duration::ZERO,
                    },
                    AcquirePlan::Success { delay: Duration::ZERO },
                ],
                seen_ids.clone(),
            )),
            delayed_success_a.clone(),
            delayed_success_b.clone(),
        ];

        let lock = DistributedLock::new("test".to_string(), clients, 3);
        let request = LockRequest::new(ObjectKey::new("bucket", "object"), LockType::Exclusive, "owner")
            .with_acquire_timeout(Duration::from_secs(2));

        let guard = lock
            .acquire_guard(&request)
            .await
            .expect("retry path should not fail")
            .expect("second attempt should reach quorum");

        tokio::time::sleep(Duration::from_millis(700)).await;

        let mut unique_ids = Vec::new();
        for lock_id in seen_ids.lock().unwrap().iter() {
            if unique_ids.iter().all(|id: &LockId| id.uuid != lock_id.uuid) {
                unique_ids.push(lock_id.clone());
            }
        }

        assert!(
            unique_ids.len() >= 2,
            "expected retry attempts to use distinct lock ids, saw {unique_ids:?}"
        );

        let retry_lock_ids = unique_ids.iter().skip(1).cloned().collect::<Vec<_>>();
        assert!(
            !retry_lock_ids.is_empty(),
            "expected at least one retry-attempt lock id, saw {unique_ids:?}"
        );

        let delayed_a_active = delayed_success_a.active.lock().await;
        let delayed_b_active = delayed_success_b.active.lock().await;
        let remaining_delayed_lock_ids = delayed_a_active
            .keys()
            .chain(delayed_b_active.keys())
            .cloned()
            .collect::<Vec<_>>();

        assert!(
            remaining_delayed_lock_ids.len() == 1,
            "exactly one delayed client lock should remain held by the retry guard after late cleanup"
        );
        assert!(
            retry_lock_ids
                .iter()
                .any(|retry_lock_id| retry_lock_id.uuid == remaining_delayed_lock_ids[0].uuid),
            "late cleanup must not leave only a first-attempt delayed lock active"
        );
        drop(delayed_b_active);
        drop(delayed_a_active);

        drop(guard);
    }
}
