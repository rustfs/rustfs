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
    record_lock_refresh_quorum_lost, record_read_lock_held_acquire, record_read_lock_held_release,
    record_write_lock_held_acquire, record_write_lock_held_release,
};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::sync::Notify;
use tokio::task::{JoinHandle, JoinSet};
use tracing::{debug, warn};
use uuid::Uuid;

const UNLOCK_RETRY_ATTEMPTS: usize = 3;
const UNLOCK_RETRY_BACKOFF: Duration = Duration::from_millis(100);
const LOCK_ACQUIRE_RETRY_INITIAL_BACKOFF: Duration = Duration::from_millis(250);
const LOCK_ACQUIRE_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(1);
const REMOTE_LOCK_RPC_FAILED_PREFIX: &str = "remote lock rpc failed:";
const REMOTE_LOCK_RPC_TIMED_OUT_PREFIX: &str = "remote lock rpc timed out:";
const UNRECOVERABLE_QUORUM_FAILURE_PREFIX: &str = "unrecoverable quorum failure";

#[derive(Clone, Copy, Debug)]
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

/// Derive a safe heartbeat interval for a freshly acquired guard.
///
/// Deliberately avoids `Duration::clamp` (which asserts `min <= max` and would panic for
/// sub-second ttls where `ttl - 1s` underflows to zero). Returns `None` for degenerate cases so
/// no heartbeat is spawned:
/// - `entries_len <= 1`: single/degenerate path (matches a local, non-distributed lock),
/// - `interval.is_zero()` or `interval >= ttl`: too small a ttl / too large an interval to renew.
fn derive_refresh_interval(entries_len: usize, ttl: Duration, injected: Option<Duration>) -> Option<Duration> {
    if entries_len <= 1 {
        return None;
    }
    let interval = injected.unwrap_or(ttl / 3);
    if interval.is_zero() || interval >= ttl {
        return None;
    }
    Some(interval)
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

fn classify_lock_error_failure(error: &LockError) -> LockAcquireFailureKind {
    if error.is_retryable() {
        LockAcquireFailureKind::RetryableContention
    } else {
        classify_lock_failure(&error.to_string())
    }
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

/// Observability channel for lock-loss: set once when a guard's heartbeat detects it has
/// lost refresh quorum. Phase 1 only signals (warn + metric + this flag); Phase 2 (backlog#899)
/// will `select!` on `notified()` inside long write operations to abort and avoid split-brain.
#[derive(Debug, Default)]
pub struct LockLostSignal {
    lost: AtomicBool,
    notify: Notify,
}

impl LockLostSignal {
    fn mark_lost(&self) {
        self.lost.store(true, Ordering::SeqCst);
        self.notify.notify_waiters();
    }

    /// Whether refresh quorum has been lost for the associated guard.
    pub fn is_lost(&self) -> bool {
        self.lost.load(Ordering::SeqCst)
    }

    /// Resolves once the lock is declared lost (immediately if already lost).
    pub async fn notified(&self) {
        if self.is_lost() {
            return;
        }
        self.notify.notified().await;
    }
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
    /// Background heartbeat task that periodically refreshes the per-client leases.
    /// `None` for degenerate/single-client paths that do not need renewal.
    refresh_task: Option<JoinHandle<()>>,
    /// Lock-loss signal, shared with the heartbeat task.
    lock_lost: Arc<LockLostSignal>,
}

impl DistributedLockGuard {
    /// Create a new guard.
    ///
    /// - `lock_id` is the id returned to the caller (`lock_id()`).
    /// - `entries` is the full list of underlying (LockId, client) pairs
    ///   that should be released when this guard is dropped.
    /// - `refresh_interval`: `Some` spawns a heartbeat that refreshes every entry on that
    ///   cadence; `None` (degenerate/single-client, or interval derived away) spawns nothing.
    /// - `refresh_quorum`: minimum refreshes that must keep succeeding; if `not_found` exceeds
    ///   `entries.len() - refresh_quorum` the guard is declared lost.
    /// - `owner`/`resource`: diagnostics only.
    pub(crate) fn new(
        lock_id: LockId,
        entries: Vec<(LockId, Arc<dyn LockClient>)>,
        lock_type: LockType,
        refresh_interval: Option<Duration>,
        refresh_quorum: usize,
        owner: String,
        resource: ObjectKey,
    ) -> Self {
        record_lock_held_acquire(lock_type);
        let lock_lost = Arc::new(LockLostSignal::default());
        let refresh_task = refresh_interval.and_then(|interval| {
            // Only spawn when a tokio runtime is available; guard construction off-runtime
            // (e.g. some tests) simply skips the heartbeat.
            tokio::runtime::Handle::try_current().ok().map(|handle| {
                let entries = entries.clone();
                let lock_lost = lock_lost.clone();
                handle.spawn(Self::run_heartbeat(entries, interval, refresh_quorum, lock_lost, owner, resource))
            })
        });
        Self {
            lock_id,
            entries,
            lock_type,
            disarmed: false,
            refresh_task,
            lock_lost,
        }
    }

    /// Heartbeat loop: every `interval`, refresh all entries and classify the outcomes.
    /// `Ok(true)` = refreshed, `Ok(false)` = not_found, `Err` = RPC jitter (ignored, absorbed by
    /// the ttl > interval margin and retried next tick). Declares the lock lost when
    /// `not_found > entries.len() - refresh_quorum`. Phase 1 does not release on loss: the guarded
    /// operation is still running, and tearing the lock down here would only widen the window.
    async fn run_heartbeat(
        entries: Vec<(LockId, Arc<dyn LockClient>)>,
        interval: Duration,
        refresh_quorum: usize,
        lock_lost: Arc<LockLostSignal>,
        owner: String,
        resource: ObjectKey,
    ) {
        let tolerable_not_found = entries.len().saturating_sub(refresh_quorum);
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        // The first tick fires immediately; skip it so the first refresh lands one interval
        // after acquisition rather than right away.
        ticker.tick().await;
        loop {
            ticker.tick().await;

            let results = join_all(entries.iter().map(|(lock_id, client)| {
                let lock_id = lock_id.clone();
                let client = client.clone();
                async move { client.refresh(&lock_id).await }
            }))
            .await;

            let mut refreshed = 0usize;
            let mut not_found = 0usize;
            for result in &results {
                match result {
                    Ok(true) => refreshed += 1,
                    Ok(false) => not_found += 1,
                    // RPC jitter: count as neither; the ttl > interval margin covers a transient
                    // dip and the next tick retries.
                    Err(_) => {}
                }
            }

            if not_found > tolerable_not_found {
                warn!(
                    resource = %resource,
                    owner = %owner,
                    refreshed,
                    not_found,
                    entries = entries.len(),
                    refresh_quorum,
                    "lock refresh lost quorum"
                );
                record_lock_refresh_quorum_lost();
                lock_lost.mark_lost();
            }
        }
    }

    /// Get the lock id associated with this guard
    pub fn lock_id(&self) -> &LockId {
        &self.lock_id
    }

    /// Whether the guard's heartbeat has observed a refresh-quorum loss.
    pub fn is_lock_lost(&self) -> bool {
        self.lock_lost.is_lost()
    }

    /// Shared lock-loss signal (used by callers to `select!` on `notified()`; Phase 2).
    pub fn lock_lost(&self) -> Arc<LockLostSignal> {
        self.lock_lost.clone()
    }

    /// Abort the heartbeat task, if running. Idempotent.
    ///
    /// Aborting may truncate an in-flight refresh, but refresh is idempotent and only extends
    /// an existing lease (never creates one): a late refresh reaching a backend that already
    /// dropped the entry returns `Ok(false)` with no side effect.
    fn stop_heartbeat(&mut self) {
        if let Some(task) = self.refresh_task.take() {
            task.abort();
        }
    }

    /// Manually disarm the guard so dropping it won't release the lock.
    /// Call this if you explicitly released the lock elsewhere.
    pub fn disarm(&mut self) {
        // Stop renewing a lock the caller says it released elsewhere.
        self.stop_heartbeat();
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
        // Stop the heartbeat before releasing so no refresh races the unlock. Abort is safe even
        // if disarmed (take() makes it idempotent / a no-op when never spawned).
        self.stop_heartbeat();

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
    quorum_impossible: bool,
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

            // Heartbeat operates on the per-client individual locks (their per-client ids), never
            // the aggregate id, so refreshes round-trip to the exact backend entries.
            let refresh_interval = derive_refresh_interval(individual_locks.len(), request.ttl, request.refresh_interval);
            Ok(Some(DistributedLockGuard::new(
                aggregate_lock_id,
                individual_locks,
                request.lock_type,
                refresh_interval,
                required_quorum,
                request.owner.clone(),
                request.resource.clone(),
            )))
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
        LOCK_ACQUIRE_RETRY_INITIAL_BACKOFF.saturating_mul(attempt.try_into().unwrap_or(u32::MAX))
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

        if let Some(mut result) = last_result {
            if result.quorum_impossible {
                result.failure_kind = Some(LockAcquireFailureKind::UnrecoverableQuorum);
            }
            Ok(result)
        } else {
            Ok(LockAcquireQuorumResult {
                response: LockResponse::failure("Lock acquisition timeout", request.acquire_timeout),
                individual_locks: Vec::new(),
                failure_kind: Some(LockAcquireFailureKind::RetryableContention),
                quorum_impossible: false,
            })
        }
    }

    fn lock_acquire_timeout_result(timeout: Duration) -> LockAcquireQuorumResult {
        LockAcquireQuorumResult {
            response: LockResponse::failure("Lock acquisition timeout", timeout),
            individual_locks: Vec::new(),
            failure_kind: Some(LockAcquireFailureKind::RetryableContention),
            quorum_impossible: false,
        }
    }

    fn lock_acquire_attempt_timeout_result(
        timeout: Duration,
        individual_locks: Vec<(LockId, Arc<dyn LockClient>)>,
        last_failure: Option<String>,
        last_failure_kind: Option<LockAcquireFailureKind>,
    ) -> LockAcquireQuorumResult {
        let failure_kind =
            if let Some(kind @ (LockAcquireFailureKind::NonRetryable | LockAcquireFailureKind::UnrecoverableQuorum)) =
                last_failure_kind
            {
                kind
            } else {
                return Self::lock_acquire_timeout_result(timeout);
            };

        let mut error = "Lock acquisition timeout".to_string();
        if let Some(last_failure) = last_failure {
            error.push_str("; last failure: ");
            error.push_str(&last_failure);
        }

        LockAcquireQuorumResult {
            response: LockResponse::failure(error, timeout),
            individual_locks,
            failure_kind: Some(failure_kind),
            quorum_impossible: matches!(failure_kind, LockAcquireFailureKind::UnrecoverableQuorum),
        }
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
        let mut last_failure_kind = None;
        let mut last_hard_failure_kind = None;
        let mut hard_failures = 0usize;
        let start = tokio::time::Instant::now();

        while !pending.is_empty() {
            let remaining = request.acquire_timeout.saturating_sub(start.elapsed());
            if remaining.is_zero() {
                Self::spawn_release_cleanup(individual_locks.clone(), "distributed_lock_attempt_timeout");
                Self::spawn_pending_cleanup(
                    pending,
                    self.clients.clone(),
                    fallback_lock_id.clone(),
                    "distributed_lock_attempt_timeout_cleanup",
                );
                return Ok(Self::lock_acquire_attempt_timeout_result(
                    request.acquire_timeout,
                    individual_locks,
                    last_failure,
                    last_failure_kind,
                ));
            }

            let join_result = match tokio::time::timeout(remaining, pending.join_next()).await {
                Ok(Some(join_result)) => join_result,
                Ok(None) => break,
                Err(_) => {
                    Self::spawn_release_cleanup(individual_locks.clone(), "distributed_lock_attempt_timeout");
                    Self::spawn_pending_cleanup(
                        pending,
                        self.clients.clone(),
                        fallback_lock_id.clone(),
                        "distributed_lock_attempt_timeout_cleanup",
                    );
                    return Ok(Self::lock_acquire_attempt_timeout_result(
                        request.acquire_timeout,
                        individual_locks,
                        last_failure,
                        last_failure_kind,
                    ));
                }
            };

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
                        let failure_kind = classify_lock_failure(&error);
                        if is_remote_lock_rpc_failure(&error) && !is_remote_lock_rpc_timeout(&error) {
                            hard_failures += 1;
                            last_hard_failure_kind = Some(failure_kind);
                        }
                        self.log_failed_lock_response(request, idx, error.clone());
                        last_failure = Some(error);
                        last_failure_kind = Some(failure_kind);
                    }
                }
                Ok((idx, Err(err))) => {
                    hard_failures += 1;
                    let failure_kind = classify_lock_error_failure(&err);
                    tracing::warn!("Failed to acquire lock on client {}: {}", idx, err);
                    last_failure = Some(err.to_string());
                    last_failure_kind = Some(failure_kind);
                    last_hard_failure_kind = Some(failure_kind);
                }
                Err(err) => {
                    hard_failures += 1;
                    tracing::warn!("Lock acquisition task join failed: {}", err);
                    last_failure = Some(err.to_string());
                    last_failure_kind = Some(LockAcquireFailureKind::NonRetryable);
                    last_hard_failure_kind = Some(LockAcquireFailureKind::NonRetryable);
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
                let failure_kind = last_hard_failure_kind.unwrap_or(LockAcquireFailureKind::UnrecoverableQuorum);
                return Ok(LockAcquireQuorumResult {
                    response: resp,
                    individual_locks,
                    failure_kind: Some(failure_kind),
                    quorum_impossible: true,
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
                    quorum_impossible: false,
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
                let (failure_kind, quorum_impossible) = if hard_failures > 0 {
                    error = format!(
                        "Unrecoverable quorum failure: {rollback_count}/{required_quorum} required; {hard_failures} clients failed; {error}"
                    );
                    (last_hard_failure_kind.unwrap_or(LockAcquireFailureKind::UnrecoverableQuorum), true)
                } else {
                    (LockAcquireFailureKind::RetryableContention, false)
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
                    quorum_impossible,
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
                last_hard_failure_kind.unwrap_or(LockAcquireFailureKind::UnrecoverableQuorum)
            } else {
                last_failure_kind.unwrap_or(LockAcquireFailureKind::RetryableContention)
            }),
            quorum_impossible: hard_failures > 0,
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
    use super::{
        DistributedLock, LOCK_ACQUIRE_ATTEMPT_TIMEOUT, LockAcquireFailureKind, is_remote_lock_rpc_failure,
        should_warn_lock_failure,
    };
    use crate::{LockError, LockId, LockInfo, LockRequest, LockResponse, LockStats, LockType, ObjectKey, client::LockClient};
    use std::assert_matches;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::{
        collections::{HashMap, VecDeque},
        sync::{Arc, Mutex},
        time::Duration,
    };

    #[derive(Clone, Copy, Debug)]
    enum RefreshOutcome {
        Alive,    // Ok(true)  refresh succeeded
        NotFound, // Ok(false) remote already lost the lock (reclaimed / never held)
        RpcError, // Err        RPC jitter
    }

    /// Counting test client: acquires successfully and echoes back request.lock_id as
    /// the per-client id, so the guard heartbeat calls refresh with that id; refresh
    /// increments a counter and returns per the configured outcome.
    #[derive(Debug)]
    struct RefreshCountingClient {
        refresh_calls: Arc<AtomicUsize>,
        outcome: RefreshOutcome,
        active: tokio::sync::Mutex<std::collections::HashSet<LockId>>,
    }

    impl RefreshCountingClient {
        fn new(outcome: RefreshOutcome, counter: Arc<AtomicUsize>) -> Arc<Self> {
            Arc::new(Self {
                refresh_calls: counter,
                outcome,
                active: tokio::sync::Mutex::new(std::collections::HashSet::new()),
            })
        }
    }

    #[async_trait::async_trait]
    impl LockClient for RefreshCountingClient {
        async fn acquire_lock(&self, request: &LockRequest) -> crate::Result<LockResponse> {
            self.active.lock().await.insert(request.lock_id.clone());
            let info = LockInfo {
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
            Ok(LockResponse::success(info, Duration::ZERO))
        }
        async fn release(&self, lock_id: &LockId) -> crate::Result<bool> {
            Ok(self.active.lock().await.remove(lock_id))
        }
        async fn refresh(&self, _lock_id: &LockId) -> crate::Result<bool> {
            self.refresh_calls.fetch_add(1, Ordering::SeqCst);
            match self.outcome {
                RefreshOutcome::Alive => Ok(true),
                RefreshOutcome::NotFound => Ok(false),
                RefreshOutcome::RpcError => Err(LockError::internal("refresh rpc jitter")),
            }
        }
        async fn force_release(&self, lock_id: &LockId) -> crate::Result<bool> {
            self.release(lock_id).await
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

    fn counting_clients(outcomes: &[RefreshOutcome]) -> (Vec<Arc<dyn LockClient>>, Vec<Arc<AtomicUsize>>) {
        let mut clients: Vec<Arc<dyn LockClient>> = Vec::new();
        let mut counters = Vec::new();
        for outcome in outcomes {
            let counter = Arc::new(AtomicUsize::new(0));
            clients.push(RefreshCountingClient::new(*outcome, counter.clone()) as Arc<dyn LockClient>);
            counters.push(counter);
        }
        (clients, counters)
    }

    // A1 -- heartbeat broadcasts refresh every interval; a live lock stays refreshed (#899 keepalive).
    #[tokio::test]
    async fn heartbeat_keeps_distributed_lock_refreshed() {
        let (clients, counters) = counting_clients(&[
            RefreshOutcome::Alive,
            RefreshOutcome::Alive,
            RefreshOutcome::Alive,
            RefreshOutcome::Alive,
        ]);
        let lock = DistributedLock::new("test".to_string(), clients, 3);
        let request = LockRequest::new(ObjectKey::new("bucket", "object"), LockType::Exclusive, "owner")
            .with_acquire_timeout(Duration::from_millis(200))
            .with_ttl(Duration::from_millis(120))
            .with_refresh_interval(Duration::from_millis(20)); // interval < ttl -> spawn

        let guard = lock
            .acquire_guard(&request)
            .await
            .expect("acquire should not error")
            .expect("quorum should be reached");

        tokio::time::sleep(Duration::from_millis(90)).await; // spans >2 intervals

        // The guard tracks exactly the quorum of per-client locks (excess successes are cleaned
        // up on acquire), so the heartbeat broadcasts to at least `quorum` clients.
        let refreshed_clients = counters.iter().filter(|c| c.load(Ordering::SeqCst) >= 1).count();
        assert!(
            refreshed_clients >= 3,
            "heartbeat should refresh at least the quorum of tracked clients, got {refreshed_clients}"
        );
        assert!(!guard.is_lock_lost(), "all-alive refresh must not signal lock loss");
        drop(guard);
    }

    // A2 -- single client (degenerate path) spawns no heartbeat (matches localLockInstance).
    #[tokio::test]
    async fn single_client_guard_spawns_no_heartbeat() {
        let (clients, counters) = counting_clients(&[RefreshOutcome::Alive]);
        let lock = DistributedLock::new("test".to_string(), clients, 1);
        let request = LockRequest::new(ObjectKey::new("bucket", "object"), LockType::Exclusive, "owner")
            .with_ttl(Duration::from_millis(120))
            .with_refresh_interval(Duration::from_millis(20));

        let guard = lock.acquire_guard(&request).await.unwrap().unwrap();
        tokio::time::sleep(Duration::from_millis(90)).await;

        assert_eq!(counters[0].load(Ordering::SeqCst), 0, "single-client guard must not run a heartbeat");
        drop(guard);
    }

    // A3 -- dropping the guard aborts the heartbeat before release; no refresh lands after drop.
    #[tokio::test]
    async fn dropping_guard_stops_heartbeat() {
        let (clients, counters) = counting_clients(&[
            RefreshOutcome::Alive,
            RefreshOutcome::Alive,
            RefreshOutcome::Alive,
            RefreshOutcome::Alive,
        ]);
        let lock = DistributedLock::new("test".to_string(), clients, 3);
        let request = LockRequest::new(ObjectKey::new("bucket", "object"), LockType::Exclusive, "owner")
            .with_ttl(Duration::from_millis(120))
            .with_refresh_interval(Duration::from_millis(20));

        let guard = lock.acquire_guard(&request).await.unwrap().unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        drop(guard); // should abort the heartbeat first
        let after_drop: Vec<usize> = counters.iter().map(|c| c.load(Ordering::SeqCst)).collect();

        tokio::time::sleep(Duration::from_millis(80)).await; // wait >2 more intervals
        for (idx, counter) in counters.iter().enumerate() {
            assert_eq!(
                counter.load(Ordering::SeqCst),
                after_drop[idx],
                "client {idx} must not be refreshed after guard drop"
            );
        }
    }

    // A4 -- losing refresh quorum (not_found > n - refresh_quorum) signals lock loss.
    // 4 clients / write-quorum 3 -> n-quorum = 1; 2 NotFound (>1) -> lost.
    #[tokio::test]
    async fn heartbeat_quorum_loss_signals_lock_lost() {
        let (clients, _counters) = counting_clients(&[
            RefreshOutcome::NotFound,
            RefreshOutcome::NotFound,
            RefreshOutcome::Alive,
            RefreshOutcome::Alive,
        ]);
        let lock = DistributedLock::new("test".to_string(), clients, 3);
        let request = LockRequest::new(ObjectKey::new("bucket", "object"), LockType::Exclusive, "owner")
            .with_ttl(Duration::from_millis(120))
            .with_refresh_interval(Duration::from_millis(20));

        let guard = lock.acquire_guard(&request).await.unwrap().unwrap();
        tokio::time::sleep(Duration::from_millis(60)).await; // at least one tick

        assert!(
            guard.is_lock_lost(),
            "losing refresh quorum (2 not_found of 4, quorum 3) must signal lock loss"
        );
        let signal = guard.lock_lost();
        tokio::time::timeout(Duration::from_millis(20), signal.notified())
            .await
            .expect("lock_lost().notified() must resolve once quorum lost");
        drop(guard);
    }

    // Phase 2 passthrough: NamespaceLockGuard::Standard must forward the distributed
    // guard's lock-loss signal so the write path can fence its commit on it.
    #[tokio::test]
    async fn namespace_guard_forwards_lock_lost() {
        let (clients, _counters) = counting_clients(&[
            RefreshOutcome::NotFound,
            RefreshOutcome::NotFound,
            RefreshOutcome::Alive,
            RefreshOutcome::Alive,
        ]);
        let lock = DistributedLock::new("test".to_string(), clients, 3);
        let request = LockRequest::new(ObjectKey::new("bucket", "object"), LockType::Exclusive, "owner")
            .with_ttl(Duration::from_millis(120))
            .with_refresh_interval(Duration::from_millis(20));

        let guard = lock.acquire_guard(&request).await.unwrap().unwrap();
        tokio::time::sleep(Duration::from_millis(60)).await; // at least one tick -> lost

        let ns_guard = crate::namespace::NamespaceLockGuard::Standard(guard);
        assert!(
            ns_guard.is_lock_lost(),
            "NamespaceLockGuard::Standard must forward the distributed guard's lock-loss signal"
        );
    }

    // A5 -- refresh RPC jitter (Err) is not counted as not_found; the lock is not declared lost.
    #[tokio::test]
    async fn heartbeat_rpc_error_not_counted_as_lock_lost() {
        let (clients, _counters) = counting_clients(&[
            RefreshOutcome::RpcError,
            RefreshOutcome::RpcError,
            RefreshOutcome::Alive,
            RefreshOutcome::Alive,
        ]);
        let lock = DistributedLock::new("test".to_string(), clients, 3);
        let request = LockRequest::new(ObjectKey::new("bucket", "object"), LockType::Exclusive, "owner")
            .with_ttl(Duration::from_millis(120))
            .with_refresh_interval(Duration::from_millis(20));

        let guard = lock.acquire_guard(&request).await.unwrap().unwrap();
        tokio::time::sleep(Duration::from_millis(60)).await;

        assert!(
            !guard.is_lock_lost(),
            "RPC jitter (Err) must NOT be counted as not_found; lock must not be declared lost"
        );
        drop(guard);
    }

    // A6 -- sub-second ttl boundary: no panic, and skip heartbeat when interval>=ttl
    //        (fixes clamp panic; aligns with the 50ms-ttl distributed path in namespace tests).
    #[tokio::test]
    async fn subsecond_ttl_guard_does_not_panic_and_skips_heartbeat() {
        // tiny ttl; the point is that acquire must not panic.
        let (clients, _counters) = counting_clients(&[RefreshOutcome::Alive, RefreshOutcome::Alive, RefreshOutcome::Alive]);
        let lock = DistributedLock::new("test".to_string(), clients, 2);
        let request = LockRequest::new(ObjectKey::new("bucket", "object"), LockType::Exclusive, "owner")
            .with_acquire_timeout(Duration::from_millis(100))
            .with_ttl(Duration::from_millis(50)); // a clamp(1s, ttl-1s) would panic here
        let guard = lock
            .acquire_guard(&request)
            .await
            .expect("subsecond ttl acquire must not panic")
            .expect("quorum should be reached");
        drop(guard);

        // inject interval >= ttl -> degenerate to no spawn
        let (clients2, counters2) = counting_clients(&[RefreshOutcome::Alive, RefreshOutcome::Alive, RefreshOutcome::Alive]);
        let lock2 = DistributedLock::new("test".to_string(), clients2, 2);
        let request2 = LockRequest::new(ObjectKey::new("bucket", "object2"), LockType::Exclusive, "owner")
            .with_ttl(Duration::from_millis(50))
            .with_refresh_interval(Duration::from_millis(50)); // interval >= ttl -> None
        let guard2 = lock2.acquire_guard(&request2).await.unwrap().unwrap();
        tokio::time::sleep(Duration::from_millis(60)).await;
        for c in &counters2 {
            assert_eq!(c.load(Ordering::SeqCst), 0, "interval>=ttl must not spawn heartbeat");
        }
        drop(guard2);
    }

    // A7 -- disarm() must also stop the heartbeat (public API gap).
    #[tokio::test]
    async fn disarm_stops_heartbeat() {
        let (clients, counters) = counting_clients(&[
            RefreshOutcome::Alive,
            RefreshOutcome::Alive,
            RefreshOutcome::Alive,
            RefreshOutcome::Alive,
        ]);
        let lock = DistributedLock::new("test".to_string(), clients, 3);
        let request = LockRequest::new(ObjectKey::new("bucket", "object"), LockType::Exclusive, "owner")
            .with_ttl(Duration::from_millis(120))
            .with_refresh_interval(Duration::from_millis(20));

        let mut guard = lock.acquire_guard(&request).await.unwrap().unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        guard.disarm(); // should abort the heartbeat
        let after_disarm: Vec<usize> = counters.iter().map(|c| c.load(Ordering::SeqCst)).collect();

        tokio::time::sleep(Duration::from_millis(80)).await;
        for (idx, counter) in counters.iter().enumerate() {
            assert_eq!(
                counter.load(Ordering::SeqCst),
                after_disarm[idx],
                "disarm() must stop the heartbeat for client {idx}"
            );
        }
        drop(guard);
    }

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
        ClientError { message: &'static str, delay: Duration },
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
                AcquirePlan::ClientError { message, delay } => {
                    if !delay.is_zero() {
                        tokio::time::sleep(delay).await;
                    }
                    Err(LockError::internal(message))
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

        assert_matches!(result, Ok(None));
        assert!(
            elapsed >= Duration::from_millis(250),
            "remote RPC timeouts should be retried within the caller timeout, got {elapsed:?}"
        );
    }

    #[tokio::test]
    async fn acquire_guard_retries_remote_rpc_failures_that_temporarily_preclude_quorum() {
        let clients: Vec<Arc<dyn LockClient>> = vec![
            Arc::new(SequencedClient::new(
                vec![
                    AcquirePlan::Failure {
                        error: "Remote lock RPC failed: node unavailable",
                        delay: Duration::ZERO,
                    },
                    AcquirePlan::Success { delay: Duration::ZERO },
                ],
                Arc::new(Mutex::new(Vec::new())),
            )),
            Arc::new(SequencedClient::new(
                vec![
                    AcquirePlan::Failure {
                        error: "Remote lock RPC failed: connection reset",
                        delay: Duration::ZERO,
                    },
                    AcquirePlan::Success { delay: Duration::ZERO },
                ],
                Arc::new(Mutex::new(Vec::new())),
            )),
            Arc::new(SequencedClient::new(
                vec![
                    AcquirePlan::Success { delay: Duration::ZERO },
                    AcquirePlan::Success { delay: Duration::ZERO },
                ],
                Arc::new(Mutex::new(Vec::new())),
            )),
            Arc::new(SequencedClient::new(
                vec![
                    AcquirePlan::Success { delay: Duration::ZERO },
                    AcquirePlan::Success { delay: Duration::ZERO },
                ],
                Arc::new(Mutex::new(Vec::new())),
            )),
        ];
        let lock = DistributedLock::new("test".to_string(), clients, 3);
        let request = LockRequest::new(ObjectKey::new("bucket", "object"), LockType::Exclusive, "owner")
            .with_acquire_timeout(Duration::from_secs(2));

        let guard = lock
            .acquire_guard(&request)
            .await
            .expect("transient remote RPC quorum gap should retry")
            .expect("second attempt should acquire quorum");

        assert!(guard.entries.len() >= 3, "retry should acquire quorum entries");
        drop(guard);
    }

    #[tokio::test]
    async fn acquire_guard_retries_client_errors_that_temporarily_preclude_quorum() {
        let clients: Vec<Arc<dyn LockClient>> = vec![
            Arc::new(SequencedClient::new(
                vec![
                    AcquirePlan::ClientError {
                        message: "can not get client, err: temporary transport unavailable",
                        delay: Duration::ZERO,
                    },
                    AcquirePlan::Success { delay: Duration::ZERO },
                ],
                Arc::new(Mutex::new(Vec::new())),
            )),
            Arc::new(SequencedClient::new(
                vec![
                    AcquirePlan::ClientError {
                        message: "can not get client, err: connection pool busy",
                        delay: Duration::ZERO,
                    },
                    AcquirePlan::Success { delay: Duration::ZERO },
                ],
                Arc::new(Mutex::new(Vec::new())),
            )),
            Arc::new(SequencedClient::new(
                vec![
                    AcquirePlan::Success { delay: Duration::ZERO },
                    AcquirePlan::Success { delay: Duration::ZERO },
                ],
                Arc::new(Mutex::new(Vec::new())),
            )),
            Arc::new(SequencedClient::new(
                vec![
                    AcquirePlan::Success { delay: Duration::ZERO },
                    AcquirePlan::Success { delay: Duration::ZERO },
                ],
                Arc::new(Mutex::new(Vec::new())),
            )),
        ];
        let lock = DistributedLock::new("test".to_string(), clients, 3);
        let request = LockRequest::new(ObjectKey::new("bucket", "object"), LockType::Exclusive, "owner")
            .with_acquire_timeout(Duration::from_secs(2));

        let guard = lock
            .acquire_guard(&request)
            .await
            .expect("retryable client error quorum gap should retry")
            .expect("second attempt should acquire quorum");

        assert!(guard.entries.len() >= 3, "retry should acquire quorum entries");
        drop(guard);
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

        assert_matches!(result, Ok(None));
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "acquire should fail this attempt before waiting for delayed impossible-quorum tasks"
        );
    }

    #[tokio::test]
    async fn acquire_guard_retries_attempt_timeout_when_hard_failure_does_not_preclude_quorum() {
        let clients: Vec<Arc<dyn LockClient>> = vec![
            Arc::new(SequencedClient::new(
                vec![
                    AcquirePlan::Failure {
                        error: "Remote lock RPC failed: node unavailable",
                        delay: Duration::ZERO,
                    },
                    AcquirePlan::Success { delay: Duration::ZERO },
                ],
                Arc::new(Mutex::new(Vec::new())),
            )),
            Arc::new(SequencedClient::new(
                vec![
                    AcquirePlan::Success {
                        delay: Duration::from_millis(1200),
                    },
                    AcquirePlan::Success { delay: Duration::ZERO },
                ],
                Arc::new(Mutex::new(Vec::new())),
            )),
            Arc::new(SequencedClient::new(
                vec![
                    AcquirePlan::Success {
                        delay: Duration::from_millis(1200),
                    },
                    AcquirePlan::Success { delay: Duration::ZERO },
                ],
                Arc::new(Mutex::new(Vec::new())),
            )),
            Arc::new(SequencedClient::new(
                vec![
                    AcquirePlan::Success {
                        delay: Duration::from_millis(1200),
                    },
                    AcquirePlan::Success { delay: Duration::ZERO },
                ],
                Arc::new(Mutex::new(Vec::new())),
            )),
        ];
        let lock = DistributedLock::new("test".to_string(), clients, 3);
        let request = LockRequest::new(ObjectKey::new("bucket", "object"), LockType::Exclusive, "owner")
            .with_acquire_timeout(Duration::from_secs(2));

        let guard = lock
            .acquire_guard(&request)
            .await
            .expect("attempt timeout with possible quorum should retry")
            .expect("second attempt should acquire quorum");

        assert_eq!(guard.entries.len(), 3);
        drop(guard);
    }

    #[tokio::test]
    async fn acquire_quorum_timeout_preserves_non_retryable_failure() {
        let clients: Vec<Arc<dyn LockClient>> = vec![
            ResponseClient::new(LockResponse::failure("permission denied", Duration::ZERO)).into_client(),
            ResponseClient::new(LockResponse::failure("lock already held", Duration::ZERO))
                .with_delay(Duration::from_secs(1))
                .into_client(),
            ResponseClient::new(LockResponse::failure("lock already held", Duration::ZERO))
                .with_delay(Duration::from_secs(1))
                .into_client(),
        ];
        let lock = DistributedLock::new("test".to_string(), clients, 2);
        let request = LockRequest::new(ObjectKey::new("bucket", "object"), LockType::Exclusive, "owner")
            .with_acquire_timeout(Duration::from_millis(120));

        let result = lock.acquire_lock_quorum_with_retry(&request).await.unwrap();

        assert_matches!(result.failure_kind, Some(LockAcquireFailureKind::NonRetryable));
        assert!(
            result
                .response
                .error
                .as_deref()
                .is_some_and(|error| error.contains("permission denied")),
            "unexpected response: {:?}",
            result.response
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

        assert_matches!(result, Ok(None));
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

        assert_matches!(result, Ok(None));
        let seen_timeouts = seen_timeouts.lock().unwrap();
        assert!(!seen_timeouts.is_empty(), "expected lock clients to observe acquire timeouts");
        assert!(
            seen_timeouts.iter().all(|timeout| *timeout <= LOCK_ACQUIRE_ATTEMPT_TIMEOUT),
            "distributed attempts should be bounded by {LOCK_ACQUIRE_ATTEMPT_TIMEOUT:?}, saw {seen_timeouts:?}"
        );
    }

    #[tokio::test]
    async fn acquire_guard_enforces_attempt_timeout_when_clients_ignore_budget() {
        let clients: Vec<Arc<dyn LockClient>> = (0..4)
            .map(|_| {
                ResponseClient::new(LockResponse::failure("lock already held", Duration::ZERO))
                    .with_delay(Duration::from_secs(5))
                    .into_client()
            })
            .collect();
        let lock = DistributedLock::new("test".to_string(), clients, 3);
        let request = LockRequest::new(ObjectKey::new("bucket", "object"), LockType::Exclusive, "owner")
            .with_acquire_timeout(Duration::from_millis(200));

        let result = tokio::time::timeout(Duration::from_millis(800), lock.acquire_guard(&request)).await;

        assert_matches!(result, Ok(Ok(None)));
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
