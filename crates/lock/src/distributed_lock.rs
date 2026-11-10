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
    client::{BatchAcquireOutcome, LockCleanupOutcome, LockClient},
    error::{LockError, Result},
    types::{LockId, LockInfo, LockRequest, LockResponse, LockStatus, LockType},
};
use futures::{
    future::join_all,
    stream::{FuturesUnordered, StreamExt},
};
use rand::RngExt as _;
use rustfs_io_metrics::{
    record_lock_refresh_quorum_lost, record_read_lock_held_acquire, record_read_lock_held_release,
    record_write_lock_held_acquire, record_write_lock_held_release,
};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::Notify;
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::Instant;
use tracing::{Instrument as _, debug, warn};
use uuid::Uuid;

const EVENT_DISTRIBUTED_LOCK_CLEANUP: &str = "distributed_lock_cleanup";
const EVENT_NAMESPACE_LOCK_ACQUIRE: &str = "namespace_lock_acquire";
const LOG_COMPONENT_LOCK: &str = "lock";
const LOG_SUBSYSTEM_NAMESPACE: &str = "namespace";

const UNLOCK_RETRY_ATTEMPTS: usize = 3;
const UNLOCK_RETRY_BACKOFF: Duration = Duration::from_millis(100);
/// Slow retry schedule for unlocks that survive the fast retry loop. Lock RPC
/// timeouts under load are transient (issue #7363); giving up after three
/// quick attempts left orphaned entries for the server lease to expire.
const DEFERRED_UNLOCK_BACKOFF: [Duration; 5] = [
    Duration::from_secs(1),
    Duration::from_secs(2),
    Duration::from_secs(4),
    Duration::from_secs(8),
    Duration::from_secs(16),
];
const LOCK_ACQUIRE_RETRY_INITIAL_BACKOFF: Duration = Duration::from_millis(250);
// Bound a peer's contention wait without discarding slow responses to that wait.
const LOCK_PEER_ACQUIRE_TIMEOUT: Duration = Duration::from_secs(1);
const LOCK_ACQUIRE_SPARE_HEDGES: usize = 1;
const REMOTE_LOCK_RPC_FAILED_PREFIX: &str = "remote lock rpc failed:";
const REMOTE_LOCK_RPC_TIMED_OUT_PREFIX: &str = "remote lock rpc timed out:";
const UNRECOVERABLE_QUORUM_FAILURE_PREFIX: &str = "unrecoverable quorum failure";

#[derive(Clone, Copy, Debug)]
enum LockAcquireFailureKind {
    NonRetryable,
    RetryableContention,
    UnrecoverableQuorum,
}

#[derive(PartialEq, Eq)]
struct BatchLockPeerDiagnostics {
    peer_index: usize,
    granted_keys: usize,
    retryable_no_grant_keys: usize,
    terminal_no_grant_keys: usize,
    ambiguous_keys: usize,
    failed_response_keys: usize,
    missing_response_keys: usize,
    call_failed_keys: usize,
    extra_responses: usize,
    call_error_kind: Option<&'static str>,
    elapsed_ms: u128,
}

impl std::fmt::Debug for BatchLockPeerDiagnostics {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BatchLockPeerDiagnostics")
            .field("peer_index", &self.peer_index)
            .field("granted_keys", &self.granted_keys)
            .field("retryable_no_grant_keys", &self.retryable_no_grant_keys)
            .field("terminal_no_grant_keys", &self.terminal_no_grant_keys)
            .field("ambiguous_keys", &self.ambiguous_keys)
            .field("failed_response_keys", &self.failed_response_keys)
            .field("missing_response_keys", &self.missing_response_keys)
            .field("call_failed_keys", &self.call_failed_keys)
            .field("extra_responses", &self.extra_responses)
            .field("call_error_kind", &self.call_error_kind)
            .field("elapsed_ms", &self.elapsed_ms)
            .finish()
    }
}

impl BatchLockPeerDiagnostics {
    fn from_outcomes(peer_index: usize, key_count: usize, outcomes: &[BatchAcquireOutcome], elapsed: Duration) -> Self {
        let mut diagnostics = Self {
            peer_index,
            granted_keys: 0,
            retryable_no_grant_keys: 0,
            terminal_no_grant_keys: 0,
            ambiguous_keys: 0,
            failed_response_keys: 0,
            missing_response_keys: key_count.saturating_sub(outcomes.len()),
            call_failed_keys: 0,
            extra_responses: outcomes.len().saturating_sub(key_count),
            call_error_kind: None,
            elapsed_ms: elapsed.as_millis(),
        };
        for outcome in outcomes.iter().take(key_count) {
            match outcome {
                BatchAcquireOutcome::Granted { .. } => diagnostics.granted_keys += 1,
                BatchAcquireOutcome::RetryableNoGrant { .. } => diagnostics.retryable_no_grant_keys += 1,
                BatchAcquireOutcome::TerminalNoGrant { .. } => diagnostics.terminal_no_grant_keys += 1,
                BatchAcquireOutcome::Ambiguous { .. } => diagnostics.ambiguous_keys += 1,
            }
        }
        diagnostics.failed_response_keys =
            diagnostics.retryable_no_grant_keys + diagnostics.terminal_no_grant_keys + diagnostics.ambiguous_keys;
        diagnostics
    }

    #[cfg(test)]
    fn from_response(peer_index: usize, key_count: usize, response: &Result<Vec<LockResponse>>, elapsed: Duration) -> Self {
        let mut diagnostics = Self {
            peer_index,
            granted_keys: 0,
            retryable_no_grant_keys: 0,
            terminal_no_grant_keys: 0,
            ambiguous_keys: 0,
            failed_response_keys: 0,
            missing_response_keys: 0,
            call_failed_keys: 0,
            extra_responses: 0,
            call_error_kind: None,
            elapsed_ms: elapsed.as_millis(),
        };
        match response {
            Ok(responses) => {
                for response in responses.iter().take(key_count) {
                    if response.success {
                        diagnostics.granted_keys += 1;
                    } else {
                        // Legacy response errors have already lost their source;
                        // a failed response is not proof of contention or an RPC timeout.
                        diagnostics.failed_response_keys += 1;
                    }
                }
                diagnostics.missing_response_keys = key_count.saturating_sub(responses.len());
                diagnostics.extra_responses = responses.len().saturating_sub(key_count);
            }
            Err(error) => {
                diagnostics.call_failed_keys = key_count;
                diagnostics.call_error_kind = Some(match error {
                    LockError::Timeout { .. } => "timeout",
                    LockError::ResourceNotFound { .. } => "resource_not_found",
                    LockError::PermissionDenied { .. } => "permission_denied",
                    LockError::Network { .. } => "network",
                    LockError::Internal { .. } => "internal",
                    LockError::AlreadyLocked { .. } => "already_locked",
                    LockError::InvalidHandle { .. } => "invalid_handle",
                    LockError::Configuration { .. } => "configuration",
                    LockError::Serialization { .. } => "serialization",
                    LockError::Deserialization { .. } => "deserialization",
                    LockError::InsufficientNodes { .. } => "insufficient_nodes",
                    LockError::QuorumNotReached { .. } => "quorum_not_reached",
                    LockError::QueueFull { .. } => "queue_full",
                    LockError::NotOwner { .. } => "not_owner",
                });
            }
        }
        diagnostics
    }
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

/// Only the local backend's explicit no-grant responses permit reusing a round's ID.
/// RPC timeouts and unknown failures can still own a late grant and its cleanup.
fn is_definitive_lock_contention(error: &str) -> bool {
    error == "Lock acquisition timeout" || error.starts_with("Lock conflict: resource held by ")
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
/// - `entries_len == 0`: no acquired backend lease to renew,
/// - `interval.is_zero()` or `interval >= ttl`: too small a ttl / too large an interval to renew.
fn derive_refresh_interval(entries_len: usize, ttl: Duration, injected: Option<Duration>) -> Option<Duration> {
    if entries_len == 0 {
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
    valid_until: Mutex<Option<Instant>>,
    notify: Notify,
}

impl LockLostSignal {
    fn set_valid_until(&self, valid_until: Instant) {
        if self.lost.load(Ordering::SeqCst) {
            return;
        }
        match self.valid_until.lock() {
            Ok(mut deadline) => {
                if deadline.is_some_and(|current| Instant::now() >= current) {
                    drop(deadline);
                    self.mark_lost();
                    return;
                }
                *deadline = Some(valid_until);
                self.notify.notify_waiters();
            }
            Err(_) => self.mark_lost(),
        }
    }

    fn mark_lost(&self) {
        self.lost.store(true, Ordering::SeqCst);
        self.notify.notify_waiters();
    }

    /// Whether refresh quorum has been lost for the associated guard.
    pub fn is_lost(&self) -> bool {
        if self.lost.load(Ordering::SeqCst) {
            return true;
        }
        let expired = match self.valid_until.lock() {
            Ok(deadline) => deadline.is_some_and(|valid_until| Instant::now() >= valid_until),
            Err(_) => true,
        };
        if expired {
            self.mark_lost();
        }
        expired
    }

    /// Resolves once the lock is declared lost (immediately if already lost).
    pub async fn notified(&self) {
        self.notified_after_registration(|| {}).await;
    }

    async fn notified_after_registration(&self, after_registration: impl FnOnce()) {
        let mut after_registration = Some(after_registration);
        loop {
            let notified = self.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Some(after_registration) = after_registration.take() {
                after_registration();
            }
            if self.is_lost() {
                return;
            }

            let valid_until = match self.valid_until.lock() {
                Ok(deadline) => *deadline,
                Err(_) => return,
            };
            match valid_until {
                Some(valid_until) => {
                    tokio::select! {
                        _ = tokio::time::sleep_until(valid_until) => {}
                        _ = &mut notified => {}
                    }
                }
                None => notified.await,
            }
        }
    }
}

#[derive(Clone, Debug)]
struct HeldLockEntry {
    lock_id: LockId,
    client: Arc<dyn LockClient>,
    valid_until: Instant,
}

impl HeldLockEntry {
    fn release_entry(&self) -> (LockId, Arc<dyn LockClient>) {
        (self.lock_id.clone(), self.client.clone())
    }
}

/// Own grants and pending replies until normal completion transfers their cleanup.
/// Cancelling the caller must not abort a reply that can still carry a grant.
struct LockAcquireRound {
    pending: JoinSet<LockAcquireTaskResult>,
    grants: Vec<HeldLockEntry>,
    clients: Arc<[Arc<dyn LockClient>]>,
    fallback_lock_id: LockId,
}

impl Drop for LockAcquireRound {
    fn drop(&mut self) {
        if !self.grants.is_empty() {
            DistributedLock::spawn_release_cleanup(
                self.grants.iter().map(HeldLockEntry::release_entry).collect(),
                "distributed_lock_cancelled_acquisition",
            );
        }
        if !self.pending.is_empty() {
            DistributedLock::spawn_pending_cleanup(
                std::mem::take(&mut self.pending),
                self.clients.clone(),
                self.fallback_lock_id.clone(),
                "distributed_lock_cancelled_acquisition_cleanup",
            );
        }
    }
}

/// Keep completed grants and every unconsumed batch reply owned across cancellation.
struct LockBatchAcquireRound {
    pending: JoinSet<LockBatchAcquireTaskResult>,
    grants: Vec<Vec<HeldLockEntry>>,
    clients: Arc<[Arc<dyn LockClient>]>,
    span: tracing::Span,
}

enum BatchRetryCleanupResult {
    Key { key: usize, confirmed: bool },
    Peer { key: usize, peer: usize, confirmed: bool },
}

impl Drop for LockBatchAcquireRound {
    fn drop(&mut self) {
        let _span = self.span.enter();
        let entries = self.grants.iter().flatten().map(HeldLockEntry::release_entry).collect();
        DistributedLock::spawn_release_cleanup(entries, "distributed_lock_cancelled_batch_acquisition");
        if !self.pending.is_empty() {
            DistributedLock::spawn_pending_batch_cleanup(
                std::mem::take(&mut self.pending),
                self.clients.clone(),
                "distributed_lock_cancelled_batch_acquisition_cleanup",
            );
        }
    }
}

#[derive(Clone, Debug)]
struct LockHeartbeatConfig {
    ttl: Duration,
    interval: Option<Duration>,
    refresh_quorum: usize,
    owner: String,
    resource: ObjectKey,
}

#[derive(Clone, Copy, Debug, Default)]
struct LockRefreshStats {
    refreshed: usize,
    not_found: usize,
    refresh_errors: usize,
}

/// A RAII guard for distributed locks that releases the lock asynchronously when dropped.
#[derive(Debug)]
pub struct DistributedLockGuard {
    /// The public-facing lock id. For multi-client scenarios this is typically
    /// an aggregate id; for single-client it is the only id.
    lock_id: LockId,
    /// All underlying leases that should be refreshed and released with the guard.
    entries: Vec<HeldLockEntry>,
    lock_type: LockType,
    /// If true, Drop will not try to release (used if user manually released).
    disarmed: bool,
    /// Background lease task for renewable leases.
    refresh_task: Option<JoinHandle<()>>,
    /// Lock-loss signal, shared with the heartbeat task.
    lock_lost: Arc<LockLostSignal>,
}

impl DistributedLockGuard {
    /// Create a new guard.
    ///
    /// - `lock_id` is the id returned to the caller (`lock_id()`).
    /// - `entries` is the full list of underlying leases that should be refreshed and released
    ///   with this guard.
    /// - `heartbeat`: lease TTL, optional renewal cadence, quorum, and diagnostics.
    fn new(lock_id: LockId, entries: Vec<HeldLockEntry>, lock_type: LockType, heartbeat: LockHeartbeatConfig) -> Self {
        record_lock_held_acquire(lock_type);
        let lock_lost = Arc::new(LockLostSignal::default());
        match Self::quorum_valid_until(&entries, heartbeat.refresh_quorum) {
            Some(valid_until) => lock_lost.set_valid_until(valid_until),
            None => lock_lost.mark_lost(),
        }
        // Non-renewable guards are fenced directly by LockLostSignal's deadline checks.
        let refresh_task = heartbeat.interval.and_then(|_| {
            tokio::runtime::Handle::try_current().ok().map(|handle| {
                let entries = entries.clone();
                let lock_lost = lock_lost.clone();
                handle.spawn(Self::run_heartbeat(entries, heartbeat, lock_lost))
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

    fn quorum_valid_until(entries: &[HeldLockEntry], refresh_quorum: usize) -> Option<Instant> {
        if refresh_quorum == 0 || entries.len() < refresh_quorum {
            return None;
        }

        let mut deadlines: Vec<_> = entries.iter().map(|entry| entry.valid_until).collect();
        deadlines.sort_unstable();
        deadlines.get(deadlines.len() - refresh_quorum).copied()
    }

    fn signal_refresh_quorum_lost(
        lock_lost: &LockLostSignal,
        config: &LockHeartbeatConfig,
        entries: usize,
        stats: LockRefreshStats,
    ) {
        warn!(
            resource = %config.resource,
            owner = config.owner,
            refreshed = stats.refreshed,
            not_found = stats.not_found,
            refresh_errors = stats.refresh_errors,
            entries,
            refresh_quorum = config.refresh_quorum,
            "lock refresh lost quorum"
        );
        record_lock_refresh_quorum_lost();
        lock_lost.mark_lost();
    }

    /// Refresh every tracked lease while a quorum is still known to be valid.
    ///
    /// A successful refresh extends only that entry's conservative local deadline. RPC errors
    /// retain the previous deadline, so transient jitter is tolerated but an unconfirmed lease
    /// can never remain valid beyond its backend TTL. Waiting for a slow refresh is also fenced
    /// by the current quorum deadline.
    async fn run_heartbeat(mut entries: Vec<HeldLockEntry>, config: LockHeartbeatConfig, lock_lost: Arc<LockLostSignal>) {
        let Some(interval) = config.interval else {
            return;
        };

        let now = Instant::now();
        // Acquisition can consume most of the original lease. Anchor the first tick
        // to that lease instead of waiting another full interval after admission.
        let first_refresh = Self::quorum_valid_until(&entries, config.refresh_quorum)
            .and_then(|deadline| deadline.checked_sub(config.ttl.saturating_sub(interval)))
            .unwrap_or(now)
            .min(now.checked_add(interval).unwrap_or(now));
        let mut ticker = tokio::time::interval_at(first_refresh, interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        loop {
            let Some(valid_until) = Self::quorum_valid_until(&entries, config.refresh_quorum) else {
                Self::signal_refresh_quorum_lost(&lock_lost, &config, entries.len(), LockRefreshStats::default());
                return;
            };
            lock_lost.set_valid_until(valid_until);

            tokio::select! {
                biased;
                _ = tokio::time::sleep_until(valid_until) => {
                    Self::signal_refresh_quorum_lost(&lock_lost, &config, entries.len(), LockRefreshStats::default());
                    return;
                }
                _ = ticker.tick() => {}
            }

            let mut stats = LockRefreshStats::default();
            let mut pending = FuturesUnordered::new();
            for (idx, entry) in entries.iter().enumerate() {
                let refresh_started = Instant::now();
                let lock_id = entry.lock_id.clone();
                let client = entry.client.clone();
                pending.push(async move { (idx, refresh_started, client.refresh(&lock_id).await) });
            }

            while !pending.is_empty() {
                let Some(valid_until) = Self::quorum_valid_until(&entries, config.refresh_quorum) else {
                    Self::signal_refresh_quorum_lost(&lock_lost, &config, entries.len(), stats);
                    return;
                };
                lock_lost.set_valid_until(valid_until);

                let next = tokio::select! {
                    biased;
                    _ = tokio::time::sleep_until(valid_until) => {
                        Self::signal_refresh_quorum_lost(&lock_lost, &config, entries.len(), stats);
                        return;
                    }
                    next = pending.next() => next
                };
                let Some((idx, refresh_started, result)) = next else {
                    break;
                };

                match result {
                    Ok(true) => {
                        stats.refreshed += 1;
                        entries[idx].valid_until = refresh_started.checked_add(config.ttl).unwrap_or(refresh_started);
                    }
                    Ok(false) => {
                        stats.not_found += 1;
                        entries[idx].valid_until = Instant::now();
                    }
                    Err(_) => stats.refresh_errors += 1,
                }
            }
        }
    }

    /// Get the lock id associated with this guard
    pub fn lock_id(&self) -> &LockId {
        &self.lock_id
    }

    /// Actual granted peer identities, which may differ from the logical guard ID.
    pub fn granted_lock_ids(&self) -> impl Iterator<Item = &LockId> {
        self.entries.iter().map(|entry| &entry.lock_id)
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

        let entries = self.entries.iter().map(HeldLockEntry::release_entry).collect();
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
    clients: Arc<[Arc<dyn LockClient>]>,
    /// Namespace identifier
    namespace: Arc<str>,
    /// Quorum size for exclusive/write operations
    quorum: usize,
}

type LockAcquireTaskResult = (usize, Result<LockResponse>);
struct LockBatchAcquireTaskResult {
    peer_index: usize,
    key_indices: Vec<usize>,
    requests: Arc<[LockRequest]>,
    outcomes: Vec<BatchAcquireOutcome>,
    started: Instant,
    elapsed: Duration,
}

fn normalized_batch_outcome(request: &LockRequest, outcome: Option<BatchAcquireOutcome>) -> BatchAcquireOutcome {
    match outcome {
        Some(BatchAcquireOutcome::Granted {
            lock_info: Some(ref info),
            ..
        }) if info.id != request.lock_id || info.resource != request.resource || info.lock_type != request.lock_type => {
            BatchAcquireOutcome::Ambiguous {
                error: LockError::internal("Batch peer returned mismatched grant metadata"),
                cleanup_owned_by_client: false,
            }
        }
        Some(outcome) => outcome,
        None => BatchAcquireOutcome::Ambiguous {
            error: LockError::internal("Batch peer omitted an acquisition outcome"),
            cleanup_owned_by_client: false,
        },
    }
}

struct LockAcquireQuorumResult {
    response: LockResponse,
    individual_locks: Vec<HeldLockEntry>,
    failure_kind: Option<LockAcquireFailureKind>,
    quorum_impossible: bool,
    attempt_timed_out: bool,
}

impl DistributedLock {
    /// Create new distributed lock
    pub fn new(namespace: String, clients: Vec<Arc<dyn LockClient>>, quorum: usize) -> Self {
        Self::new_shared(namespace.into(), clients.into(), quorum)
    }

    /// Create a distributed lock that shares existing namespace and client allocations.
    pub(crate) fn new_shared(namespace: Arc<str>, clients: Arc<[Arc<dyn LockClient>]>, quorum: usize) -> Self {
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
    #[hotpath::measure]
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
            let heartbeat = LockHeartbeatConfig {
                ttl: request.ttl,
                interval: derive_refresh_interval(individual_locks.len(), request.ttl, request.refresh_interval),
                refresh_quorum: required_quorum,
                owner: request.owner.clone(),
                resource: request.resource.clone(),
            };
            Ok(Some(DistributedLockGuard::new(
                aggregate_lock_id,
                individual_locks,
                request.lock_type,
                heartbeat,
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
    #[hotpath::measure]
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
    #[hotpath::measure]
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
    #[hotpath::measure]
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

    /// Acquire independent exclusive locks with one sub-batch per peer and round.
    /// A successful key starts its heartbeat immediately. Only completed, explicitly
    /// retryable attempts whose grants have been cleaned may enter another round.
    /// The acquisition, rollback wait and backoff share one absolute deadline.
    ///
    /// Cancellation or expiry transfers pending replies and exact-ID cleanup to
    /// independent owners. Exhausted cleanup retries still rely on backend leases;
    /// uncertain replies never establish that another attempt is safe on that peer.
    pub async fn acquire_guard_batch(
        &self,
        resources: &[ObjectKey],
        owner: &str,
        timeout: Duration,
        ttl: Duration,
    ) -> Vec<Result<DistributedLockGuard>> {
        let entered = Instant::now();
        let deadline = entered.checked_add(timeout).unwrap_or(entered);
        if resources.is_empty() {
            return Vec::new();
        }
        if self.clients.is_empty() {
            return resources
                .iter()
                .map(|_| Err(LockError::internal("No lock clients available")))
                .collect();
        }

        let required_quorum = self.required_quorum(LockType::Exclusive);
        let mut results: Vec<Option<Result<DistributedLockGuard>>> = (0..resources.len()).map(|_| None).collect();
        let mut eligible = vec![vec![true; self.clients.len()]; resources.len()];
        let mut achieved = vec![0; resources.len()];
        let mut cleanup_pending = vec![false; resources.len()];
        let mut peer_cleanup_pending = vec![vec![false; self.clients.len()]; resources.len()];
        let cleanup_capacity = resources.len().saturating_mul(self.clients.len().saturating_add(1));
        let (cleanup_completed, mut cleanup_results) = tokio::sync::mpsc::channel(cleanup_capacity);
        let mut next_cleanup = None;
        let mut attempt = 0usize;
        while results.iter().any(Option::is_none) && Instant::now() < deadline {
            while let Some(completed) = next_cleanup.take().or_else(|| cleanup_results.try_recv().ok()) {
                match completed {
                    BatchRetryCleanupResult::Key { key, confirmed } => {
                        cleanup_pending[key] = false;
                        if !confirmed && results[key].is_none() {
                            results[key] = Some(Err(LockError::QuorumNotReached {
                                required: required_quorum,
                                achieved: achieved[key],
                            }));
                        }
                    }
                    BatchRetryCleanupResult::Peer { key, peer, confirmed } => {
                        peer_cleanup_pending[key][peer] = false;
                        eligible[key][peer] = confirmed;
                    }
                }
            }
            for (key, result) in results.iter_mut().enumerate() {
                if result.is_none()
                    && !cleanup_pending[key]
                    && !peer_cleanup_pending[key].iter().any(|&pending| pending)
                    && eligible[key].iter().filter(|&&value| value).count() < required_quorum
                {
                    *result = Some(Err(LockError::QuorumNotReached {
                        required: required_quorum,
                        achieved: achieved[key],
                    }));
                }
            }
            if results.iter().all(Option::is_some) {
                break;
            }
            if !results.iter().enumerate().any(|(key, result)| {
                result.is_none()
                    && !cleanup_pending[key]
                    && eligible[key].iter().filter(|&&value| value).count() >= required_quorum
            }) {
                // Per-key cleanup tasks own their grants independently. Waiting
                // here is necessary only when no other key can start a round.
                match tokio::time::timeout_at(deadline, cleanup_results.recv()).await {
                    Ok(Some(completed)) => {
                        next_cleanup = Some(completed);
                        continue;
                    }
                    Ok(None) | Err(_) => break,
                }
            }
            attempt = attempt.saturating_add(1);
            let round_started = Instant::now();
            let wait = self.peer_acquire_timeout(deadline.saturating_duration_since(round_started));
            let round_deadline = round_started.checked_add(wait).unwrap_or(round_started).min(deadline);
            let requests: Vec<_> = resources
                .iter()
                .enumerate()
                .map(|(key, resource)| {
                    (results[key].is_none() && !cleanup_pending[key]).then(|| {
                        LockRequest::new(resource.clone(), LockType::Exclusive, owner)
                            .with_acquire_timeout(wait)
                            .with_ttl(ttl)
                    })
                })
                .collect();
            let mut round = LockBatchAcquireRound {
                pending: JoinSet::new(),
                grants: vec![Vec::new(); resources.len()],
                clients: self.clients.clone(),
                span: tracing::Span::current(),
            };
            for (peer_index, client) in self.clients.iter().enumerate() {
                let key_indices: Vec<_> = (0..resources.len())
                    .filter(|&key| results[key].is_none() && !cleanup_pending[key] && eligible[key][peer_index])
                    .collect();
                if key_indices.is_empty() {
                    continue;
                }
                let requests: Arc<[LockRequest]> = key_indices.iter().filter_map(|&key| requests[key].clone()).collect();
                let client = client.clone();
                round.pending.spawn(
                    async move {
                        // The local deadline is fixed before scheduling or is_local().
                        // Remote clients need the total deadline for queue/RPC headroom.
                        let local = client.is_local().await;
                        let peer_deadline = if local { round_deadline } else { deadline };
                        let outcomes = if Instant::now() >= peer_deadline {
                            requests
                                .iter()
                                .map(|request| BatchAcquireOutcome::RetryableNoGrant {
                                    error: LockError::timeout(request.resource.to_string(), request.acquire_timeout),
                                })
                                .collect()
                        } else {
                            client.acquire_locks_batch_until(&requests, peer_deadline).await
                        };
                        LockBatchAcquireTaskResult {
                            peer_index,
                            key_indices,
                            requests,
                            outcomes,
                            started: round_started,
                            elapsed: round_started.elapsed(),
                        }
                    }
                    .in_current_span(),
                );
            }

            let mut pending_by_key: Vec<Vec<bool>> = eligible
                .iter()
                .enumerate()
                .map(|(key, peers)| {
                    peers
                        .iter()
                        .map(|&enabled| enabled && results[key].is_none() && !cleanup_pending[key])
                        .collect()
                })
                .collect();
            let mut excluded_pending = false;
            let mut peer_outcomes = Vec::with_capacity(self.clients.len());
            let mut rollback_grants = 0usize;
            // A peer batch can contain both successful and unresolved keys. Keep
            // consuming its entire output until every key is settled or D_total.
            while !round.pending.is_empty() && results.iter().any(Option::is_none) {
                let joined = tokio::select! {
                    biased;
                    _ = tokio::time::sleep_until(deadline) => break,
                    joined = round.pending.join_next() => joined,
                };
                let Some(joined) = joined else { break };
                let reply = match joined {
                    Ok(reply) => reply,
                    Err(error) => {
                        warn!(
                            event = EVENT_NAMESPACE_LOCK_ACQUIRE,
                            component = LOG_COMPONENT_LOCK,
                            subsystem = LOG_SUBSYSTEM_NAMESPACE,
                            state = "batch_task_failed",
                            failure = if error.is_cancelled() { "cancelled" } else { "panicked" },
                            "Namespace batch lock peer task failed"
                        );
                        // A panicked implementation cannot prove no-grant. Do not
                        // retry this round's requests with an unknown outcome.
                        for (key, peers) in eligible.iter_mut().enumerate() {
                            if results[key].is_none() {
                                peers.fill(false);
                            }
                        }
                        continue;
                    }
                };
                peer_outcomes.push(BatchLockPeerDiagnostics::from_outcomes(
                    reply.peer_index,
                    reply.requests.len(),
                    &reply.outcomes,
                    reply.elapsed,
                ));
                let client = self.clients[reply.peer_index].clone();
                let valid_until = reply.started.checked_add(ttl).unwrap_or(reply.started);
                let mut outcomes = reply.outcomes.into_iter();
                let mut redundant = Vec::new();
                let mut uncertain = Vec::new();
                for (&key, request) in reply.key_indices.iter().zip(reply.requests.iter()) {
                    pending_by_key[key][reply.peer_index] = false;
                    let outcome = normalized_batch_outcome(request, outcomes.next());
                    match outcome {
                        BatchAcquireOutcome::Granted { .. } => {
                            let entry = HeldLockEntry {
                                lock_id: request.lock_id.clone(),
                                client: client.clone(),
                                valid_until,
                            };
                            if results[key].is_some() {
                                redundant.push(entry.release_entry());
                            } else {
                                round.grants[key].push(entry);
                            }
                        }
                        BatchAcquireOutcome::RetryableNoGrant { .. } => {}
                        BatchAcquireOutcome::TerminalNoGrant { .. } => eligible[key][reply.peer_index] = false,
                        BatchAcquireOutcome::Ambiguous {
                            cleanup_owned_by_client, ..
                        } => {
                            eligible[key][reply.peer_index] = false;
                            if !cleanup_owned_by_client {
                                uncertain.push((request.lock_id.clone(), client.clone()));
                            }
                        }
                    }
                }
                // Transfer cleanup synchronously before the next cancellation point.
                rollback_grants += redundant.len();
                Self::spawn_release_cleanup(redundant, "distributed_lock_batch_redundant_grants");
                Self::spawn_uncertain_batch_cleanup(uncertain, "distributed_lock_batch_ambiguous");

                let now = Instant::now();
                for (key, resource) in resources.iter().enumerate() {
                    if results[key].is_some() || cleanup_pending[key] {
                        continue;
                    }
                    achieved[key] = round.grants[key].iter().filter(|entry| now < entry.valid_until).count();
                    if now < deadline && achieved[key] >= required_quorum {
                        let (entries, expired): (Vec<_>, Vec<_>) = std::mem::take(&mut round.grants[key])
                            .into_iter()
                            .partition(|entry| now < entry.valid_until);
                        rollback_grants += expired.len();
                        Self::spawn_release_cleanup(
                            expired.iter().map(HeldLockEntry::release_entry).collect(),
                            "distributed_lock_batch_expired_grants",
                        );
                        let heartbeat = LockHeartbeatConfig {
                            ttl,
                            interval: derive_refresh_interval(entries.len(), ttl, None),
                            refresh_quorum: required_quorum,
                            owner: owner.to_owned(),
                            resource: resource.clone(),
                        };
                        results[key] = Some(Ok(DistributedLockGuard::new(
                            LockId::new_unique(resource),
                            entries,
                            LockType::Exclusive,
                            heartbeat,
                        )));
                    }
                }
                // Completed eligible peers can form a new quorum after rollback,
                // even when a silent peer could complete the current quorum.
                // Keep waiting only for keys without enough completed peers.
                let can_retry_without_pending = results.iter().enumerate().all(|(key, result)| {
                    if result.is_some() || cleanup_pending[key] {
                        return true;
                    }
                    let pending_votes = pending_by_key[key].iter().filter(|&&value| value).count();
                    pending_votes == 0
                        || eligible[key]
                            .iter()
                            .zip(&pending_by_key[key])
                            .filter(|(eligible, pending)| **eligible && !**pending)
                            .count()
                            >= required_quorum
                });
                if can_retry_without_pending && !round.pending.is_empty() {
                    for (key, peers) in eligible.iter_mut().enumerate() {
                        if results[key].is_none() {
                            for (peer, enabled) in peers.iter_mut().enumerate() {
                                if pending_by_key[key][peer] {
                                    *enabled = false;
                                    peer_cleanup_pending[key][peer] = true;
                                }
                            }
                        }
                    }
                    excluded_pending = true;
                    break;
                }
            }

            for (key, grants) in round.grants.iter_mut().enumerate() {
                if results[key].is_none() && !cleanup_pending[key] {
                    achieved[key] = grants.iter().filter(|entry| Instant::now() < entry.valid_until).count();
                }
                if grants.is_empty() {
                    continue;
                }
                rollback_grants += grants.len();
                cleanup_pending[key] = true;
                let entries = std::mem::take(grants);
                let completed = cleanup_completed.clone();
                // Spawn before the next await so cancellation cannot abandon
                // these exact-ID grants. Other keys consume their own results.
                Self::spawn_cleanup(
                    async move {
                        let confirmed = Self::release_batch_for_retry(entries).await;
                        let _ = completed.send(BatchRetryCleanupResult::Key { key, confirmed }).await;
                    },
                    "distributed_lock_batch_quorum_rollback",
                );
            }
            // Dropping the round detaches all late replies before rollback awaits.
            // A selected peer is retried only when the old wave fully completed.
            let round_completed = round.pending.is_empty() || excluded_pending;
            if excluded_pending {
                Self::spawn_retry_peer_cleanup(
                    std::mem::take(&mut round.pending),
                    self.clients.clone(),
                    cleanup_completed.clone(),
                );
            }
            drop(round);
            for (key, result) in results.iter_mut().enumerate() {
                if result.is_none()
                    && ((!cleanup_pending[key]
                        && (!round_completed
                            || (!peer_cleanup_pending[key].iter().any(|&pending| pending)
                                && eligible[key].iter().filter(|&&value| value).count() < required_quorum)))
                        || Instant::now() >= deadline)
                {
                    *result = Some(Err(LockError::QuorumNotReached {
                        required: required_quorum,
                        achieved: achieved[key],
                    }));
                }
            }
            let mut keys_by_achieved_votes = vec![0; self.clients.len() + 1];
            for &votes in &achieved {
                keys_by_achieved_votes[votes] += 1;
            }
            let failed_keys = results.iter().filter(|result| matches!(result, Some(Err(_)))).count();
            let successful_keys = results.iter().filter(|result| matches!(result, Some(Ok(_)))).count();
            peer_outcomes.sort_unstable_by_key(|outcome| outcome.peer_index);
            if failed_keys > 0 {
                warn!(
                    event = EVENT_NAMESPACE_LOCK_ACQUIRE,
                    component = LOG_COMPONENT_LOCK,
                    subsystem = LOG_SUBSYSTEM_NAMESPACE,
                    state = "batch_quorum_failed",
                    round = attempt,
                    key_count = resources.len(),
                    peer_count = self.clients.len(),
                    required_quorum,
                    successful_keys,
                    failed_keys,
                    ?keys_by_achieved_votes,
                    ?peer_outcomes,
                    rollback_grants,
                    acquire_ms = round_started.elapsed().as_millis(),
                    "Namespace batch lock acquisition did not reach quorum for all keys"
                );
            } else {
                debug!(
                    event = EVENT_NAMESPACE_LOCK_ACQUIRE,
                    component = LOG_COMPONENT_LOCK,
                    subsystem = LOG_SUBSYSTEM_NAMESPACE,
                    state = "batch_round_completed",
                    round = attempt,
                    key_count = resources.len(),
                    peer_count = self.clients.len(),
                    required_quorum,
                    successful_keys,
                    retrying_keys = resources.len() - successful_keys,
                    ?keys_by_achieved_votes,
                    ?peer_outcomes,
                    rollback_grants,
                    acquire_ms = round_started.elapsed().as_millis(),
                    "Namespace batch lock acquisition round completed"
                );
            }
            if results.iter().any(Option::is_none) && Instant::now() < deadline {
                let remaining = deadline.saturating_duration_since(Instant::now());
                let backoff = Self::lock_acquire_retry_backoff(attempt, &mut rand::rng()).min(remaining / 2);
                if backoff.is_zero() {
                    break;
                }
                tokio::time::sleep_until(Instant::now().checked_add(backoff).unwrap_or(deadline).min(deadline)).await;
            }
        }
        results
            .into_iter()
            .map(|result| match result {
                Some(Ok(guard)) if !guard.is_lock_lost() => Ok(guard),
                Some(Ok(_)) | None => Err(LockError::QuorumNotReached {
                    required: required_quorum,
                    achieved: 0,
                }),
                Some(Err(error)) => Err(error),
            })
            .collect()
    }

    fn spawn_lock_requests_until(
        &self,
        pending: &mut JoinSet<LockAcquireTaskResult>,
        request: &LockRequest,
        next_client_index: &mut usize,
        target_pending: usize,
    ) {
        while pending.len() < target_pending {
            if *next_client_index >= self.clients.len() {
                break;
            }
            let idx = *next_client_index;
            *next_client_index = (*next_client_index).saturating_add(1);
            self.spawn_lock_request(pending, request, idx);
        }
    }

    fn spawn_lock_request(&self, pending: &mut JoinSet<LockAcquireTaskResult>, request: &LockRequest, idx: usize) {
        let Some(client) = self.clients.get(idx).cloned() else {
            return;
        };
        let request = request.clone();
        let scheduled = Instant::now();
        pending.spawn(async move {
            let entered = Instant::now();
            let response = client.acquire_lock(&request).await;
            if scheduled.elapsed() >= Duration::from_millis(100) {
                tracing::trace!(
                    target: "rustfs_lock::namespace_owner",
                    event = EVENT_NAMESPACE_LOCK_ACQUIRE,
                    component = LOG_COMPONENT_LOCK,
                    subsystem = LOG_SUBSYSTEM_NAMESPACE,
                    state = "peer_completed",
                    result = response.as_ref().is_ok_and(|response| response.success),
                    resource = %request.resource,
                    lock_id = %request.lock_id,
                    peer_index = idx,
                    queue_ms = entered.duration_since(scheduled).as_millis(),
                    acquire_ms = entered.elapsed().as_millis(),
                    "Namespace peer lock acquisition completed slowly"
                );
            }
            (idx, response)
        });
    }

    fn lock_acquire_retry_backoff(attempt: usize, rng: &mut impl rand::Rng) -> Duration {
        let base = LOCK_ACQUIRE_RETRY_INITIAL_BACKOFF.saturating_mul(attempt.try_into().unwrap_or(u32::MAX));
        let jitter = base / 4;
        let lower = base.saturating_sub(jitter);
        let upper = base.saturating_add(jitter);
        let lower_nanos = u64::try_from(lower.as_nanos()).unwrap_or(u64::MAX);
        let upper_nanos = u64::try_from(upper.as_nanos()).unwrap_or(u64::MAX);

        Duration::from_nanos(rng.random_range(lower_nanos..=upper_nanos))
    }

    fn peer_acquire_timeout(&self, remaining: Duration) -> Duration {
        if self.clients.len() <= 1 {
            remaining
        } else {
            remaining.min(LOCK_PEER_ACQUIRE_TIMEOUT)
        }
    }

    async fn release_entries(entries: Vec<(LockId, Arc<dyn LockClient>)>, context: &'static str) {
        let mut pending = entries;

        for attempt in 1..=UNLOCK_RETRY_ATTEMPTS {
            pending = Self::release_pending_once(pending, attempt, context).await;
            if pending.is_empty() {
                debug!(
                    event = EVENT_DISTRIBUTED_LOCK_CLEANUP,
                    component = LOG_COMPONENT_LOCK,
                    subsystem = LOG_SUBSYSTEM_NAMESPACE,
                    state = "completed",
                    attempt,
                    context,
                    "Distributed unlock cleanup completed"
                );
                return;
            }

            if attempt < UNLOCK_RETRY_ATTEMPTS {
                tokio::time::sleep(UNLOCK_RETRY_BACKOFF * attempt as u32).await;
            }
        }

        warn!(
            event = EVENT_DISTRIBUTED_LOCK_CLEANUP,
            component = LOG_COMPONENT_LOCK,
            subsystem = LOG_SUBSYSTEM_NAMESPACE,
            state = "deferred",
            remaining = pending.len(),
            attempts = UNLOCK_RETRY_ATTEMPTS,
            context,
            "Distributed unlock cleanup remains unconfirmed; continuing with deferred retries"
        );
        Self::release_entries_deferred(pending, context).await;
    }

    /// Retain rollback ownership independently of the foreground deadline. Keys
    /// with an unconfirmed grant are not retried; their remaining cleanup continues.
    async fn release_batch_for_retry(mut entries: Vec<HeldLockEntry>) -> bool {
        for attempt in 1..=UNLOCK_RETRY_ATTEMPTS {
            entries = join_all(entries.into_iter().map(|entry| async move {
                match entry.client.cleanup_grant(&entry.lock_id).await {
                    Ok(LockCleanupOutcome::Released | LockCleanupOutcome::AlreadyAbsent) => None,
                    Ok(LockCleanupOutcome::Unconfirmed) | Err(_) => Some(entry),
                }
            }))
            .await
            .into_iter()
            .flatten()
            .collect();
            if entries.is_empty() {
                return true;
            }
            if attempt < UNLOCK_RETRY_ATTEMPTS {
                let multiplier = u32::try_from(attempt).unwrap_or(u32::MAX);
                tokio::time::sleep(UNLOCK_RETRY_BACKOFF.saturating_mul(multiplier)).await;
            }
        }
        Self::spawn_cleanup(
            Self::release_entries_deferred(
                entries.into_iter().map(|entry| entry.release_entry()).collect(),
                "distributed_lock_batch_quorum_rollback",
            ),
            "distributed_lock_batch_quorum_rollback",
        );
        false
    }

    fn spawn_uncertain_batch_cleanup(entries: Vec<(LockId, Arc<dyn LockClient>)>, context: &'static str) {
        if !entries.is_empty() {
            Self::spawn_cleanup(Self::release_uncertain_batch(entries, context), context);
        }
    }

    /// A peer skipped by the foreground round becomes usable again only after
    /// its old request completes and any exact-ID grant is confirmed cleaned.
    fn spawn_retry_peer_cleanup(
        mut pending: JoinSet<LockBatchAcquireTaskResult>,
        clients: Arc<[Arc<dyn LockClient>]>,
        completed: tokio::sync::mpsc::Sender<BatchRetryCleanupResult>,
    ) {
        Self::spawn_cleanup(
            async move {
                let mut cleanups = JoinSet::new();
                while let Some(joined) = pending.join_next().await {
                    let Ok(reply) = joined else { continue };
                    let peer = reply.peer_index;
                    let Some(client) = clients.get(peer) else { continue };
                    let mut outcomes = reply.outcomes.into_iter();
                    for (&key, request) in reply.key_indices.iter().zip(reply.requests.iter()) {
                        let outcome = normalized_batch_outcome(request, outcomes.next());
                        let client = client.clone();
                        let entry = HeldLockEntry {
                            lock_id: request.lock_id.clone(),
                            client: client.clone(),
                            valid_until: reply.started,
                        };
                        let completed = completed.clone();
                        // Each task owns its grant before the drain next awaits.
                        cleanups.spawn(
                            async move {
                                let confirmed = match outcome {
                                    BatchAcquireOutcome::Granted { .. } => Self::release_batch_for_retry(vec![entry]).await,
                                    BatchAcquireOutcome::RetryableNoGrant { .. } => true,
                                    BatchAcquireOutcome::TerminalNoGrant { .. } => false,
                                    BatchAcquireOutcome::Ambiguous {
                                        cleanup_owned_by_client, ..
                                    } => {
                                        if !cleanup_owned_by_client {
                                            Self::spawn_uncertain_batch_cleanup(
                                                vec![(entry.lock_id, client)],
                                                "distributed_lock_batch_retry_late_ambiguous",
                                            );
                                        }
                                        false
                                    }
                                };
                                let _ = completed.send(BatchRetryCleanupResult::Peer { key, peer, confirmed }).await;
                            }
                            .in_current_span(),
                        );
                    }
                }
                while cleanups.join_next().await.is_some() {}
            },
            "distributed_lock_batch_retry_late_cleanup",
        );
    }

    /// Ambiguous acquisitions can still grant after an absence reply. Only an
    /// exact-ID release acknowledgement ends this conservative cleanup schedule.
    async fn release_uncertain_batch(mut pending: Vec<(LockId, Arc<dyn LockClient>)>, context: &'static str) {
        let delays = [Duration::ZERO, UNLOCK_RETRY_BACKOFF, UNLOCK_RETRY_BACKOFF * 2]
            .into_iter()
            .chain(DEFERRED_UNLOCK_BACKOFF);
        for delay in delays {
            if pending.is_empty() {
                return;
            }
            tokio::time::sleep(delay).await;
            pending = join_all(pending.into_iter().map(|(lock_id, client)| async move {
                match client.release(&lock_id).await {
                    Ok(true) => None,
                    Ok(false) | Err(_) => Some((lock_id, client)),
                }
            }))
            .await
            .into_iter()
            .flatten()
            .collect();
        }
        if !pending.is_empty() {
            warn!(
                event = EVENT_DISTRIBUTED_LOCK_CLEANUP,
                component = LOG_COMPONENT_LOCK,
                subsystem = LOG_SUBSYSTEM_NAMESPACE,
                state = "uncertain",
                context,
                remaining = pending.len(),
                "Ambiguous batch grant cleanup remains unconfirmed after bounded retries"
            );
        }
    }

    async fn release_pending_once(
        pending: Vec<(LockId, Arc<dyn LockClient>)>,
        attempt: usize,
        context: &'static str,
    ) -> Vec<(LockId, Arc<dyn LockClient>)> {
        let release_results = join_all(pending.into_iter().map(|(lock_id, client)| async move {
            match client.cleanup_grant(&lock_id).await {
                Ok(LockCleanupOutcome::Released | LockCleanupOutcome::AlreadyAbsent) => None,
                Ok(LockCleanupOutcome::Unconfirmed) => {
                    warn!(
                        event = EVENT_DISTRIBUTED_LOCK_CLEANUP,
                        component = LOG_COMPONENT_LOCK,
                        subsystem = LOG_SUBSYSTEM_NAMESPACE,
                        state = "unconfirmed",
                        %lock_id,
                        attempt,
                        context,
                        "Distributed unlock cleanup was not confirmed by client"
                    );
                    Some((lock_id, client))
                }
                Err(err) => {
                    warn!(
                        event = EVENT_DISTRIBUTED_LOCK_CLEANUP,
                        component = LOG_COMPONENT_LOCK,
                        subsystem = LOG_SUBSYSTEM_NAMESPACE,
                        state = "failed",
                        %lock_id,
                        attempt,
                        context,
                        error = %err,
                        "Distributed unlock cleanup failed on client"
                    );
                    Some((lock_id, client))
                }
            }
        }))
        .await;

        release_results.into_iter().flatten().collect()
    }

    /// Bounded slow retries for entries the fast loop could not release. Every
    /// caller runs on a background task, so waiting here blocks nobody; after
    /// the schedule is exhausted cleanup remains unconfirmed. A backend lease
    /// bounds the lifetime of any grant that still exists.
    async fn release_entries_deferred(mut pending: Vec<(LockId, Arc<dyn LockClient>)>, context: &'static str) {
        let mut attempt = UNLOCK_RETRY_ATTEMPTS;
        for delay in DEFERRED_UNLOCK_BACKOFF {
            tokio::time::sleep(delay).await;
            attempt += 1;
            pending = Self::release_pending_once(pending, attempt, context).await;
            if pending.is_empty() {
                debug!(
                    event = EVENT_DISTRIBUTED_LOCK_CLEANUP,
                    component = LOG_COMPONENT_LOCK,
                    subsystem = LOG_SUBSYSTEM_NAMESPACE,
                    state = "completed",
                    attempt,
                    context,
                    "Deferred distributed unlock cleanup converged"
                );
                return;
            }
        }

        warn!(
            event = EVENT_DISTRIBUTED_LOCK_CLEANUP,
            component = LOG_COMPONENT_LOCK,
            subsystem = LOG_SUBSYSTEM_NAMESPACE,
            state = "unconfirmed",
            remaining = pending.len(),
            attempts = attempt,
            context,
            "Distributed unlock cleanup remains unconfirmed after deferred retries"
        );
    }

    fn spawn_release_cleanup(entries: Vec<(LockId, Arc<dyn LockClient>)>, context: &'static str) {
        if entries.is_empty() {
            return;
        }

        Self::spawn_cleanup(Self::release_entries(entries, context), context);
    }

    fn spawn_cleanup(cleanup: impl std::future::Future<Output = ()> + Send + 'static, context: &'static str) {
        let cleanup = cleanup.in_current_span();
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            let join_handle = handle.spawn(cleanup);
            drop(join_handle);
            return;
        }

        let join_handle = std::thread::spawn(move || match tokio::runtime::Builder::new_current_thread().enable_all().build() {
            Ok(runtime) => runtime.block_on(cleanup),
            Err(err) => warn!(
                event = EVENT_DISTRIBUTED_LOCK_CLEANUP,
                component = LOG_COMPONENT_LOCK,
                subsystem = LOG_SUBSYSTEM_NAMESPACE,
                state = "runtime_failed",
                context,
                error = %err,
                "Failed to create fallback unlock runtime"
            ),
        });
        drop(join_handle);
    }

    fn spawn_pending_batch_cleanup(
        mut pending: JoinSet<LockBatchAcquireTaskResult>,
        clients: Arc<[Arc<dyn LockClient>]>,
        context: &'static str,
    ) {
        Self::spawn_cleanup(
            async move {
                let mut releases = JoinSet::new();
                while let Some(joined) = pending.join_next().await {
                    match joined {
                        Ok(reply) => {
                            let peer_outcome = BatchLockPeerDiagnostics::from_outcomes(
                                reply.peer_index,
                                reply.requests.len(),
                                &reply.outcomes,
                                reply.elapsed,
                            );
                            debug!(
                                event = EVENT_DISTRIBUTED_LOCK_CLEANUP,
                                component = LOG_COMPONENT_LOCK,
                                subsystem = LOG_SUBSYSTEM_NAMESPACE,
                                state = "batch_reply_completed",
                                context,
                                ?peer_outcome,
                                "Unconsumed batch lock reply retained for cleanup"
                            );
                            let Some(client) = clients.get(reply.peer_index) else { continue };
                            let mut outcomes = reply.outcomes.into_iter();
                            let mut confirmed = Vec::new();
                            let mut uncertain = Vec::new();
                            for request in reply.requests.iter() {
                                match normalized_batch_outcome(request, outcomes.next()) {
                                    BatchAcquireOutcome::Granted { .. } => {
                                        confirmed.push((request.lock_id.clone(), client.clone()))
                                    }
                                    BatchAcquireOutcome::Ambiguous {
                                        cleanup_owned_by_client: false,
                                        ..
                                    } => {
                                        uncertain.push((request.lock_id.clone(), client.clone()));
                                    }
                                    BatchAcquireOutcome::RetryableNoGrant { .. }
                                    | BatchAcquireOutcome::TerminalNoGrant { .. }
                                    | BatchAcquireOutcome::Ambiguous {
                                        cleanup_owned_by_client: true,
                                        ..
                                    } => {}
                                }
                            }
                            // Keep cleanup concurrent across peers without blocking the
                            // drain on one release or one deferred uncertainty schedule.
                            releases.spawn(
                                async move {
                                    tokio::join!(
                                        Self::release_entries(confirmed, context),
                                        Self::release_uncertain_batch(uncertain, context)
                                    );
                                }
                                .in_current_span(),
                            );
                        }
                        Err(error) => warn!(
                            event = EVENT_DISTRIBUTED_LOCK_CLEANUP,
                            component = LOG_COMPONENT_LOCK,
                            subsystem = LOG_SUBSYSTEM_NAMESPACE,
                            state = "task_failed",
                            context,
                            failure = if error.is_cancelled() { "cancelled" } else { "panicked" },
                            "Pending batch lock acquisition task failed"
                        ),
                    }
                }
                while let Some(result) = releases.join_next().await {
                    if let Err(error) = result {
                        warn!(
                            event = EVENT_DISTRIBUTED_LOCK_CLEANUP,
                            component = LOG_COMPONENT_LOCK,
                            subsystem = LOG_SUBSYSTEM_NAMESPACE,
                            state = "task_failed",
                            context,
                            failure = if error.is_cancelled() { "cancelled" } else { "panicked" },
                            "Pending batch grant release cleanup task failed"
                        );
                    }
                }
            },
            context,
        );
    }

    fn spawn_pending_cleanup(
        mut pending: JoinSet<LockAcquireTaskResult>,
        clients: Arc<[Arc<dyn LockClient>]>,
        fallback_lock_id: LockId,
        context: &'static str,
    ) {
        Self::spawn_cleanup(
            async move {
                let mut releases = JoinSet::new();
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

                            // Deferred retries may wait tens of seconds; never hold up the
                            // next late completion behind them.
                            releases.spawn(Self::release_entries(vec![(lock_id, client.clone())], context));
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
                // Keep the cleanup runtime alive until releases finish. They run
                // concurrently, so a deferred retry never blocks another late grant.
                while let Some(result) = releases.join_next().await {
                    if let Err(error) = result {
                        warn!(
                            event = EVENT_DISTRIBUTED_LOCK_CLEANUP,
                            component = LOG_COMPONENT_LOCK,
                            subsystem = LOG_SUBSYSTEM_NAMESPACE,
                            state = "task_failed",
                            context,
                            failure = if error.is_cancelled() { "cancelled" } else { "panicked" },
                            "Pending grant release cleanup task failed"
                        );
                    }
                }
            },
            context,
        );
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
        let start = tokio::time::Instant::now();
        let mut attempt = 1;
        let mut last_result: Option<LockAcquireQuorumResult> = None;

        loop {
            let elapsed = start.elapsed();
            if elapsed >= request.acquire_timeout {
                break;
            }

            let remaining = request.acquire_timeout - elapsed;
            let mut attempt_request = request.clone();
            attempt_request.acquire_timeout = self.peer_acquire_timeout(remaining);
            attempt_request.lock_id = LockId::new_unique(&request.resource);

            // A peer may grant promptly but deliver its response after its local
            // contention budget. Keep this round's owners until a response proves
            // quorum impossible or the caller's remaining budget expires.
            let result = self.acquire_lock_quorum_once(&attempt_request, remaining).await?;
            if !result.response.success {
                tracing::trace!(
                    target: "rustfs_lock::namespace_owner",
                    event = EVENT_NAMESPACE_LOCK_ACQUIRE,
                    component = LOG_COMPONENT_LOCK,
                    subsystem = LOG_SUBSYSTEM_NAMESPACE,
                    state = "failed",
                    namespace = %self.namespace,
                    resource = %request.resource,
                    lock_id = %attempt_request.lock_id,
                    mode = ?request.lock_type,
                    attempt,
                    required = self.required_quorum(request.lock_type),
                    achieved = result.individual_locks.len(),
                    failure = ?result.failure_kind,
                    timeout_ms = remaining.as_millis(),
                    peer_timeout_ms = attempt_request.acquire_timeout.as_millis(),
                    "Namespace quorum acquisition round failed"
                );
            }
            if result.response.success || !matches!(result.failure_kind, Some(LockAcquireFailureKind::RetryableContention)) {
                return Ok(result);
            }

            // A final retry can time out without establishing whether the
            // previously observed quorum failure has recovered.
            let remaining = request.acquire_timeout.saturating_sub(start.elapsed());
            let preserve_quorum_failure = result.attempt_timed_out
                && remaining.is_zero()
                && last_result.as_ref().is_some_and(|result| result.quorum_impossible);
            if !preserve_quorum_failure {
                last_result = Some(result);
            }
            if remaining.is_zero() {
                break;
            }
            let backoff = Self::lock_acquire_retry_backoff(attempt, &mut rand::rng());
            // Keep time for another acquire attempt instead of abandoning the
            // caller's remaining budget when the jittered backoff grows large.
            tokio::time::sleep(backoff.min(remaining / 2)).await;
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
                attempt_timed_out: true,
            })
        }
    }

    fn lock_acquire_attempt_timeout_result(
        timeout: Duration,
        individual_locks: Vec<HeldLockEntry>,
        last_failure: Option<String>,
        last_failure_kind: Option<LockAcquireFailureKind>,
    ) -> LockAcquireQuorumResult {
        let failure_kind = last_failure_kind.unwrap_or(LockAcquireFailureKind::RetryableContention);

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
            attempt_timed_out: true,
        }
    }

    /// Quorum-based lock acquisition: success if at least the required quorum succeeds.
    /// Collects all individual lock_ids from successful clients and creates an aggregate lock_id.
    /// Returns the LockResponse with aggregate lock_id and individual lock mappings.
    async fn acquire_lock_quorum_once(&self, request: &LockRequest, quorum_timeout: Duration) -> Result<LockAcquireQuorumResult> {
        let voter_count =
            u32::try_from(self.clients.len().max(1)).map_err(|_| LockError::internal("lock voter count exceeds u32"))?;
        let required_quorum = self.required_quorum(request.lock_type);
        let attempt_started = Instant::now();
        let quorum_deadline = attempt_started.checked_add(quorum_timeout).unwrap_or(attempt_started);
        let grant_valid_until = attempt_started.checked_add(request.ttl).unwrap_or(attempt_started);
        let mut round = LockAcquireRound {
            pending: JoinSet::new(),
            grants: Vec::new(),
            clients: self.clients.clone(),
            fallback_lock_id: request.lock_id.clone(),
        };
        let mut next_client_index = 0usize;
        self.spawn_lock_requests_until(
            &mut round.pending,
            request,
            &mut next_client_index,
            required_quorum
                .saturating_add(LOCK_ACQUIRE_SPARE_HEDGES)
                .min(self.clients.len()),
        );
        let mut last_failure = None;
        let mut last_failure_kind = None;
        let mut last_hard_failure_kind = None;
        let mut hard_failures = 0usize;
        let mut retry_at = vec![None; self.clients.len()];
        let mut contention_retries = vec![0usize; self.clients.len()];
        let start = attempt_started;
        // A silent prefix must not hide healthy voters later in the list.
        // Spread additional hedges across the existing attempt budget; fast
        // quorums still use only the initial quorum-plus-spare requests.
        let hedge_interval = request.acquire_timeout / voter_count;
        let hedge_timer = tokio::time::sleep(hedge_interval);
        tokio::pin!(hedge_timer);

        while !round.pending.is_empty() {
            let remaining = quorum_timeout.saturating_sub(start.elapsed());
            if remaining.is_zero() {
                Self::spawn_release_cleanup(
                    round.grants.iter().map(HeldLockEntry::release_entry).collect(),
                    "distributed_lock_attempt_timeout",
                );
                Self::spawn_pending_cleanup(
                    std::mem::take(&mut round.pending),
                    self.clients.clone(),
                    round.fallback_lock_id.clone(),
                    "distributed_lock_attempt_timeout_cleanup",
                );
                return Ok(Self::lock_acquire_attempt_timeout_result(
                    quorum_timeout,
                    std::mem::take(&mut round.grants),
                    last_failure,
                    last_failure_kind,
                ));
            }

            let next_retry = retry_at.iter().flatten().copied().min();
            let next_result = tokio::select! {
                biased;
                _ = tokio::time::sleep_until(quorum_deadline) => None,
                result = round.pending.join_next(), if !round.pending.is_empty() => result,
                _ = tokio::time::sleep_until(next_retry.unwrap_or(quorum_deadline)), if next_retry.is_some() => {
                    let now = Instant::now();
                    if now < quorum_deadline {
                        for (idx, retry) in retry_at.iter_mut().enumerate() {
                            if retry.is_some_and(|deadline| deadline <= now) {
                                *retry = None;
                                let mut retry_request = request.clone();
                                retry_request.acquire_timeout = request.acquire_timeout.min(quorum_deadline - now);
                                self.spawn_lock_request(&mut round.pending, &retry_request, idx);
                            }
                        }
                    }
                    continue;
                }
                _ = &mut hedge_timer, if next_client_index < self.clients.len() => {
                    if start.elapsed() < quorum_timeout {
                        let target_in_flight = round.pending.len().saturating_add(1);
                        self.spawn_lock_requests_until(
                            &mut round.pending, request, &mut next_client_index, target_in_flight,
                        );
                    }
                    hedge_timer.set(tokio::time::sleep(hedge_interval));
                    continue;
                }
            };
            let join_result = match next_result {
                Some(join_result) => join_result,
                None => {
                    Self::spawn_release_cleanup(
                        round.grants.iter().map(HeldLockEntry::release_entry).collect(),
                        "distributed_lock_attempt_timeout",
                    );
                    Self::spawn_pending_cleanup(
                        std::mem::take(&mut round.pending),
                        self.clients.clone(),
                        round.fallback_lock_id.clone(),
                        "distributed_lock_attempt_timeout_cleanup",
                    );
                    return Ok(Self::lock_acquire_attempt_timeout_result(
                        quorum_timeout,
                        std::mem::take(&mut round.grants),
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
                            .unwrap_or_else(|| round.fallback_lock_id.clone());

                        if let Some(client) = self.clients.get(idx) {
                            round.grants.push(HeldLockEntry {
                                lock_id,
                                client: client.clone(),
                                valid_until: grant_valid_until,
                            });
                        } else {
                            tracing::warn!("Missing lock client at index {} while recording success", idx);
                        }
                    } else {
                        let error = resp.error.unwrap_or_else(|| "unknown error".to_string());
                        // Only an authoritative no-grant response is safe to retry under
                        // the same ID. A successful or still-pending voter is never retried.
                        if is_definitive_lock_contention(&error) {
                            contention_retries[idx] = contention_retries[idx].saturating_add(1);
                            let backoff = LOCK_ACQUIRE_RETRY_INITIAL_BACKOFF
                                .saturating_mul(contention_retries[idx].try_into().unwrap_or(u32::MAX));
                            retry_at[idx] = Instant::now()
                                .checked_add(backoff)
                                .filter(|deadline| *deadline < quorum_deadline);
                        }
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
                let rollback_count = round.grants.len();
                Self::spawn_release_cleanup(
                    round.grants.iter().map(HeldLockEntry::release_entry).collect(),
                    "distributed_lock_quorum_rollback",
                );
                if !round.pending.is_empty() {
                    Self::spawn_pending_cleanup(
                        std::mem::take(&mut round.pending),
                        self.clients.clone(),
                        round.fallback_lock_id.clone(),
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
                    individual_locks: std::mem::take(&mut round.grants),
                    failure_kind: Some(failure_kind),
                    quorum_impossible: true,
                    attempt_timed_out: false,
                });
            }

            if round.grants.len() >= required_quorum {
                // Timeout polls its inner join first. A ready last vote must
                // not override the acquisition deadline after a delayed poll.
                if Instant::now() >= quorum_deadline {
                    Self::spawn_release_cleanup(
                        round.grants.iter().map(HeldLockEntry::release_entry).collect(),
                        "distributed_lock_attempt_timeout",
                    );
                    Self::spawn_pending_cleanup(
                        std::mem::take(&mut round.pending),
                        self.clients.clone(),
                        round.fallback_lock_id.clone(),
                        "distributed_lock_attempt_timeout_cleanup",
                    );
                    return Ok(Self::lock_acquire_attempt_timeout_result(
                        quorum_timeout,
                        std::mem::take(&mut round.grants),
                        last_failure,
                        last_failure_kind,
                    ));
                }
                // Slow delivery must not turn grants whose conservative lease
                // deadline already passed into a successful quorum.
                if Instant::now() >= grant_valid_until {
                    Self::spawn_release_cleanup(
                        round.grants.iter().map(HeldLockEntry::release_entry).collect(),
                        "distributed_lock_expired_quorum",
                    );
                    if !round.pending.is_empty() {
                        Self::spawn_pending_cleanup(
                            std::mem::take(&mut round.pending),
                            self.clients.clone(),
                            round.fallback_lock_id.clone(),
                            "distributed_lock_expired_quorum_cleanup",
                        );
                    }
                    return Ok(LockAcquireQuorumResult {
                        response: LockResponse::failure(
                            "Lock acquisition timeout: grant lease expired before quorum",
                            Duration::ZERO,
                        ),
                        individual_locks: std::mem::take(&mut round.grants),
                        failure_kind: Some(LockAcquireFailureKind::RetryableContention),
                        quorum_impossible: false,
                        attempt_timed_out: false,
                    });
                }
                if !round.pending.is_empty() {
                    Self::spawn_pending_cleanup(
                        std::mem::take(&mut round.pending),
                        self.clients.clone(),
                        round.fallback_lock_id.clone(),
                        "distributed_lock_success_cleanup",
                    );
                }

                let aggregate_lock_id = generate_aggregate_lock_id(&request.resource);
                tracing::debug!(
                    "Generated aggregate lock_id {} for {} individual locks on resource {}",
                    aggregate_lock_id,
                    round.grants.len(),
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
                    individual_locks: std::mem::take(&mut round.grants),
                    failure_kind: None,
                    quorum_impossible: false,
                    attempt_timed_out: false,
                });
            }

            let needed = required_quorum.saturating_sub(round.grants.len());
            self.spawn_lock_requests_until(
                &mut round.pending,
                request,
                &mut next_client_index,
                needed.saturating_add(LOCK_ACQUIRE_SPARE_HEDGES),
            );
            let unstarted_clients = self.clients.len().saturating_sub(next_client_index);
            // Scheduled retries cannot rescue an otherwise impossible round. Roll back
            // its partial cohort so competing exclusive callers can make progress.
            if round.grants.len() + round.pending.len() + unstarted_clients < required_quorum {
                let rollback_count = round.grants.len();
                Self::spawn_release_cleanup(
                    round.grants.iter().map(HeldLockEntry::release_entry).collect(),
                    "distributed_lock_quorum_rollback",
                );
                if !round.pending.is_empty() {
                    Self::spawn_pending_cleanup(
                        std::mem::take(&mut round.pending),
                        self.clients.clone(),
                        round.fallback_lock_id.clone(),
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
                    individual_locks: std::mem::take(&mut round.grants),
                    failure_kind: Some(failure_kind),
                    quorum_impossible,
                    attempt_timed_out: false,
                });
            }
        }

        let rollback_count = round.grants.len();
        Self::spawn_release_cleanup(
            round.grants.iter().map(HeldLockEntry::release_entry).collect(),
            "distributed_lock_quorum_rollback",
        );
        let mut error = format!("Failed to acquire quorum: {rollback_count}/{required_quorum} required");
        if let Some(last_failure) = &last_failure {
            error.push_str("; last failure: ");
            error.push_str(last_failure);
        }
        let resp = LockResponse::failure(error, Duration::ZERO);
        Ok(LockAcquireQuorumResult {
            response: resp,
            individual_locks: std::mem::take(&mut round.grants),
            failure_kind: Some(if hard_failures > 0 {
                last_hard_failure_kind.unwrap_or(LockAcquireFailureKind::UnrecoverableQuorum)
            } else {
                last_failure_kind.unwrap_or(LockAcquireFailureKind::RetryableContention)
            }),
            quorum_impossible: hard_failures > 0,
            attempt_timed_out: false,
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
mod batch_acquire_tests;

#[cfg(test)]
mod batch_retry_tests;

#[cfg(test)]
mod tests {
    use super::{
        BatchLockPeerDiagnostics, DEFERRED_UNLOCK_BACKOFF, DistributedLock, LOCK_ACQUIRE_RETRY_INITIAL_BACKOFF,
        LOCK_ACQUIRE_SPARE_HEDGES, LOCK_PEER_ACQUIRE_TIMEOUT, LockAcquireFailureKind, LockLostSignal, UNLOCK_RETRY_ATTEMPTS,
        UNLOCK_RETRY_BACKOFF, is_definitive_lock_contention, is_remote_lock_rpc_failure, should_warn_lock_failure,
    };
    use crate::{
        GlobalLockManager, LocalClient, LockCleanupOutcome, LockError, LockId, LockInfo, LockRequest, LockResponse, LockStats,
        LockType, ObjectKey, client::LockClient,
    };
    use rand::{SeedableRng as _, TryRng, rngs::StdRng};
    use std::assert_matches;
    use std::convert::Infallible;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::{
        collections::{HashMap, VecDeque},
        sync::{Arc, Mutex},
        time::Duration,
    };
    use tokio::time::Instant;

    #[derive(Clone, Copy, Debug)]
    enum RefreshOutcome {
        Alive,               // Ok(true)  refresh succeeded
        SlowAlive(Duration), // Ok(true)  after a delayed response
        NotFound,            // Ok(false) remote already lost the lock (reclaimed / never held)
        RpcError,            // Err        RPC jitter
    }

    struct ConstantRng(u64);

    impl TryRng for ConstantRng {
        type Error = Infallible;

        fn try_next_u32(&mut self) -> Result<u32, Self::Error> {
            Ok(self.0 as u32)
        }

        fn try_next_u64(&mut self) -> Result<u64, Self::Error> {
            Ok(self.0)
        }

        fn try_fill_bytes(&mut self, dst: &mut [u8]) -> Result<(), Self::Error> {
            for chunk in dst.chunks_mut(std::mem::size_of::<u64>()) {
                chunk.copy_from_slice(&self.0.to_ne_bytes()[..chunk.len()]);
            }
            Ok(())
        }
    }

    #[test]
    fn lock_acquire_retry_backoff_uses_bounded_jitter() {
        let mut rng = StdRng::seed_from_u64(42);
        let base = LOCK_ACQUIRE_RETRY_INITIAL_BACKOFF * 3;
        let jitter = base / 4;
        let lower = base - jitter;
        let upper = base + jitter;
        let samples = (0..32)
            .map(|_| DistributedLock::lock_acquire_retry_backoff(3, &mut rng))
            .collect::<Vec<_>>();

        assert!(samples.iter().all(|delay| *delay >= lower && *delay <= upper));
        assert!(samples.windows(2).any(|pair| pair[0] != pair[1]));

        let mut lower_rng = ConstantRng(0);
        let mut upper_rng = ConstantRng(u64::MAX);
        assert_eq!(DistributedLock::lock_acquire_retry_backoff(3, &mut lower_rng), lower);
        assert_eq!(DistributedLock::lock_acquire_retry_backoff(3, &mut upper_rng), upper);
    }

    /// Counting test client: acquires successfully and echoes back request.lock_id as
    /// the per-client id, so the guard heartbeat calls refresh with that id; refresh
    /// increments a counter and returns per the configured outcome.
    #[derive(Debug)]
    struct RefreshCountingClient {
        acquire_calls: AtomicUsize,
        acquire_delay: Duration,
        refresh_calls: Arc<AtomicUsize>,
        outcome: RefreshOutcome,
        active: tokio::sync::Mutex<std::collections::HashSet<LockId>>,
    }

    impl RefreshCountingClient {
        fn new(outcome: RefreshOutcome, counter: Arc<AtomicUsize>) -> Arc<Self> {
            Self::with_acquire_delay(outcome, counter, Duration::ZERO)
        }

        fn with_acquire_delay(outcome: RefreshOutcome, counter: Arc<AtomicUsize>, acquire_delay: Duration) -> Arc<Self> {
            Arc::new(Self {
                acquire_calls: AtomicUsize::new(0),
                acquire_delay,
                refresh_calls: counter,
                outcome,
                active: tokio::sync::Mutex::new(std::collections::HashSet::new()),
            })
        }
    }

    #[async_trait::async_trait]
    impl LockClient for RefreshCountingClient {
        async fn acquire_lock(&self, request: &LockRequest) -> crate::Result<LockResponse> {
            self.acquire_calls.fetch_add(1, Ordering::SeqCst);
            if !self.acquire_delay.is_zero() {
                tokio::time::sleep(self.acquire_delay).await;
            }
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
                RefreshOutcome::SlowAlive(delay) => {
                    tokio::time::sleep(delay).await;
                    Ok(true)
                }
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

    #[tokio::test(start_paused = true)]
    async fn delayed_quorum_refreshes_before_its_original_lease_expires() {
        let counter = Arc::new(AtomicUsize::new(0));
        let clients = (0..4)
            .map(|_| {
                RefreshCountingClient::with_acquire_delay(RefreshOutcome::Alive, counter.clone(), Duration::from_millis(250))
                    as Arc<dyn LockClient>
            })
            .collect();
        let lock = DistributedLock::new("delayed-quorum-refresh".to_string(), clients, 3);
        let request = LockRequest::new(ObjectKey::new("bucket", "object"), LockType::Exclusive, "owner")
            .with_ttl(Duration::from_millis(300))
            .with_acquire_timeout(Duration::from_secs(1));
        let started = Instant::now();
        let guard = lock
            .acquire_guard(&request)
            .await
            .expect("acquisition result")
            .expect("live quorum");
        assert_eq!(started.elapsed(), Duration::from_millis(250));
        tokio::task::yield_now().await;
        assert!(counter.load(Ordering::SeqCst) >= 3, "renew before the remaining 50ms lease expires");
        for _ in 0..4 {
            tokio::time::advance(Duration::from_millis(100)).await;
            tokio::task::yield_now().await;
            assert!(!guard.is_lock_lost(), "renewed quorum must survive the original acquisition lease");
        }
        assert!(started.elapsed() > request.ttl);
        drop(guard);
    }

    #[tokio::test(start_paused = true)]
    async fn delayed_quorum_failed_refresh_still_expires_at_original_deadline() {
        for outcome in [
            RefreshOutcome::RpcError,
            RefreshOutcome::SlowAlive(Duration::from_millis(100)),
        ] {
            let counter = Arc::new(AtomicUsize::new(0));
            let clients = (0..4)
                .map(|_| {
                    RefreshCountingClient::with_acquire_delay(outcome, counter.clone(), Duration::from_millis(250))
                        as Arc<dyn LockClient>
                })
                .collect();
            let lock = DistributedLock::new("delayed-quorum-failed-refresh".to_string(), clients, 3);
            let request = LockRequest::new(ObjectKey::new("bucket", "object"), LockType::Exclusive, "owner")
                .with_ttl(Duration::from_millis(300))
                .with_acquire_timeout(Duration::from_secs(1));
            let started = Instant::now();
            let guard = lock
                .acquire_guard(&request)
                .await
                .expect("acquisition result")
                .expect("live quorum");
            tokio::task::yield_now().await;
            assert!(counter.load(Ordering::SeqCst) >= 3, "remaining lease must allow a renewal attempt");
            guard.lock_lost().notified().await;
            assert_eq!(started.elapsed(), request.ttl, "unconfirmed renewal must not extend the lease");
            assert!(guard.is_lock_lost());
            drop(guard);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn fresh_quorum_keeps_normal_first_refresh_interval() {
        let (clients, counters) = counting_clients(&[RefreshOutcome::Alive; 4]);
        let lock = DistributedLock::new("fresh-quorum-refresh".to_string(), clients, 3);
        let request = LockRequest::new(ObjectKey::new("bucket", "object"), LockType::Exclusive, "owner")
            .with_ttl(Duration::from_millis(300));
        let guard = lock
            .acquire_guard(&request)
            .await
            .expect("acquisition result")
            .expect("live quorum");
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_millis(99)).await;
        tokio::task::yield_now().await;
        assert!(counters.iter().all(|counter| counter.load(Ordering::SeqCst) == 0));
        tokio::time::advance(Duration::from_millis(1)).await;
        tokio::task::yield_now().await;
        assert!(counters.iter().filter(|counter| counter.load(Ordering::SeqCst) > 0).count() >= 3);
        drop(guard);
    }

    #[tokio::test(start_paused = true)]
    async fn fast_read_quorum_keeps_initial_fanout_bound() {
        let peers: Vec<_> = (0..8)
            .map(|_| RefreshCountingClient::new(RefreshOutcome::Alive, Arc::new(AtomicUsize::new(0))))
            .collect();
        let clients = peers.iter().cloned().map(|peer| peer as Arc<dyn LockClient>).collect();
        let lock = DistributedLock::new("fast-fanout".to_string(), clients, 5);
        let request = LockRequest::new(ObjectKey::new("bucket", "object"), LockType::Shared, "owner")
            .with_acquire_timeout(Duration::from_secs(1));
        let guard = lock
            .acquire_guard(&request)
            .await
            .expect("fast quorum")
            .expect("admitted guard");
        tokio::task::yield_now().await;
        let calls: usize = peers.iter().map(|peer| peer.acquire_calls.load(Ordering::SeqCst)).sum();
        assert!(calls >= 4, "four votes must be acquired");
        assert!(calls <= 4 + LOCK_ACQUIRE_SPARE_HEDGES, "fast success must not fan out to the whole fleet");
        drop(guard);
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

    // A2 -- a quorum-one distributed lease still requires renewal.
    #[tokio::test]
    async fn single_client_guard_keeps_backend_lease_refreshed() {
        let (clients, counters) = counting_clients(&[RefreshOutcome::Alive]);
        let lock = DistributedLock::new("test".to_string(), clients, 1);
        let request = LockRequest::new(ObjectKey::new("bucket", "object"), LockType::Exclusive, "owner")
            .with_ttl(Duration::from_millis(120))
            .with_refresh_interval(Duration::from_millis(20));

        let guard = lock
            .acquire_guard(&request)
            .await
            .expect("single-client lock acquisition should not error")
            .expect("single-client lock quorum should be reached");
        assert!(
            guard.refresh_task.is_some(),
            "single-client distributed guard must spawn a background task"
        );
        tokio::time::timeout(Duration::from_secs(2), async {
            while counters[0].load(Ordering::SeqCst) < 2 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("single-client distributed guard should renew before the test deadline");

        assert!(
            counters[0].load(Ordering::SeqCst) >= 2,
            "single-client distributed guard must renew beyond the original backend TTL"
        );
        assert!(!guard.is_lock_lost(), "successful quorum-one refresh must preserve the lease");
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

        let guard = lock
            .acquire_guard(&request)
            .await
            .expect("distributed lock acquisition should not error")
            .expect("distributed lock quorum should be reached");
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

        let guard = lock
            .acquire_guard(&request)
            .await
            .expect("distributed lock acquisition should not error")
            .expect("distributed lock quorum should be reached");
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

    #[tokio::test]
    async fn lock_lost_notification_cannot_be_missed_after_waiter_registration() {
        let signal = LockLostSignal::default();

        tokio::time::timeout(Duration::from_secs(2), signal.notified_after_registration(|| signal.mark_lost()))
            .await
            .expect("loss between waiter registration and state recheck must still resolve");
    }

    #[tokio::test]
    async fn expired_lease_is_fenced_without_waiting_for_monitor_scheduling() {
        let signal = LockLostSignal::default();
        signal.set_valid_until(tokio::time::Instant::now());

        assert!(signal.is_lost(), "an elapsed backend deadline must fence the owner immediately");
        tokio::time::timeout(Duration::from_secs(2), signal.notified())
            .await
            .expect("an elapsed backend deadline must resolve lock-loss waiters immediately");
    }

    #[tokio::test]
    async fn deadline_notification_permanently_fences_the_signal() {
        let signal = LockLostSignal::default();
        signal.set_valid_until(tokio::time::Instant::now() + Duration::from_millis(10));

        tokio::time::timeout(Duration::from_secs(2), signal.notified())
            .await
            .expect("the backend deadline must resolve lock-loss waiters");

        assert!(
            signal.lost.load(Ordering::SeqCst),
            "deadline notification must persist the lock-loss fence"
        );
    }

    #[test]
    fn expired_lease_cannot_be_revived_by_a_later_deadline() {
        let signal = LockLostSignal::default();
        signal.set_valid_until(tokio::time::Instant::now());
        assert!(signal.is_lost(), "an elapsed lease must become permanently fenced");

        signal.set_valid_until(tokio::time::Instant::now() + Duration::from_secs(60));

        assert!(signal.is_lost(), "a later heartbeat must not revive an expired lease");
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

        let guard = lock
            .acquire_guard(&request)
            .await
            .expect("namespace lock acquisition should not error")
            .expect("namespace lock quorum should be reached");
        tokio::time::sleep(Duration::from_millis(60)).await; // at least one tick -> lost

        let ns_guard = crate::namespace::NamespaceLockGuard::Standard(guard);
        assert!(
            ns_guard.is_lock_lost(),
            "NamespaceLockGuard::Standard must forward the distributed guard's lock-loss signal"
        );
    }

    // A5 -- refresh RPC jitter is tolerated within the lease, but an unconfirmed lease cannot
    // remain valid after its backend TTL.
    #[tokio::test]
    async fn heartbeat_rpc_error_expires_at_backend_ttl() {
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

        let guard = lock
            .acquire_guard(&request)
            .await
            .expect("distributed lock acquisition should not error")
            .expect("distributed lock quorum should be reached");
        tokio::time::sleep(Duration::from_millis(60)).await;

        assert!(
            !guard.is_lock_lost(),
            "transient RPC jitter before the backend TTL must not immediately signal lock loss"
        );

        let signal = guard.lock_lost();
        tokio::time::timeout(Duration::from_secs(2), signal.notified())
            .await
            .expect("unconfirmed leases must signal lock loss when their backend TTL expires");
        assert!(guard.is_lock_lost(), "the guard must fail closed once refresh quorum expires");
        drop(guard);
    }

    // A6 -- a refresh call that remains in flight cannot keep an expired quorum alive.
    #[tokio::test]
    async fn heartbeat_slow_refresh_is_fenced_by_backend_ttl() {
        let (clients, _counters) =
            counting_clients(&[RefreshOutcome::SlowAlive(Duration::from_millis(200)), RefreshOutcome::Alive]);
        let lock = DistributedLock::new("test".to_string(), clients, 2);
        let request = LockRequest::new(ObjectKey::new("bucket", "object"), LockType::Exclusive, "owner")
            .with_ttl(Duration::from_millis(120))
            .with_refresh_interval(Duration::from_millis(20));

        let guard = lock
            .acquire_guard(&request)
            .await
            .expect("distributed lock acquisition should not error")
            .expect("distributed lock quorum should be reached");
        let signal = guard.lock_lost();
        tokio::time::timeout(Duration::from_secs(2), signal.notified())
            .await
            .expect("an in-flight refresh must not extend an unconfirmed lease past its TTL");
        assert!(
            guard.is_lock_lost(),
            "slow refresh must fail closed at the last confirmed quorum deadline"
        );
        drop(guard);
    }

    // A7 -- sub-second ttl boundary: no panic, and skip heartbeat when interval>=ttl
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
        let guard2 = lock2
            .acquire_guard(&request2)
            .await
            .expect("subsecond lock acquisition should not error")
            .expect("subsecond lock quorum should be reached");
        assert!(
            guard2.refresh_task.is_none(),
            "non-renewable subsecond guard must not spawn a background task"
        );
        tokio::time::timeout(Duration::from_secs(2), guard2.lock_lost().notified())
            .await
            .expect("non-renewable subsecond guard must signal loss at its backend TTL");
        for c in &counters2 {
            assert_eq!(c.load(Ordering::SeqCst), 0, "interval>=ttl must not refresh the backend lease");
        }
        assert!(guard2.is_lock_lost(), "interval>=ttl guard must fail closed after backend TTL");
        drop(guard2);
    }

    // A8 -- disarm() must also stop the heartbeat (public API gap).
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

        let mut guard = lock
            .acquire_guard(&request)
            .await
            .expect("distributed lock acquisition should not error")
            .expect("distributed lock quorum should be reached");
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

    /// Fails `release` a fixed number of times before succeeding, mimicking a
    /// peer whose lock RPCs time out under load and then recover.
    #[derive(Debug)]
    struct FlakyReleaseClient {
        failures_left: AtomicUsize,
        release_calls: Arc<AtomicUsize>,
        release_success: bool,
    }

    #[async_trait::async_trait]
    impl LockClient for FlakyReleaseClient {
        async fn acquire_lock(&self, _request: &LockRequest) -> crate::Result<LockResponse> {
            Ok(LockResponse::failure("unused", Duration::ZERO))
        }

        async fn release(&self, _lock_id: &LockId) -> crate::Result<bool> {
            self.release_calls.fetch_add(1, Ordering::SeqCst);
            if self.failures_left.load(Ordering::SeqCst) > 0 {
                self.failures_left.fetch_sub(1, Ordering::SeqCst);
                return Err(LockError::internal("remote lock rpc timed out: release"));
            }
            Ok(self.release_success)
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

    #[tokio::test(start_paused = true)]
    async fn release_entries_keeps_retrying_transient_failures_after_the_fast_loop() {
        let release_calls = Arc::new(AtomicUsize::new(0));
        let client: Arc<dyn LockClient> = Arc::new(FlakyReleaseClient {
            failures_left: AtomicUsize::new(UNLOCK_RETRY_ATTEMPTS + 2),
            release_calls: release_calls.clone(),
            release_success: true,
        });
        let lock_id = LockId::new_unique(&ObjectKey::new("bucket", "object"));

        DistributedLock::release_entries(vec![(lock_id, client)], "test_deferred_unlock").await;

        assert_eq!(
            release_calls.load(Ordering::SeqCst),
            UNLOCK_RETRY_ATTEMPTS + 3,
            "two deferred attempts fail, the third releases the entry"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn release_entries_gives_up_after_the_deferred_schedule() {
        let release_calls = Arc::new(AtomicUsize::new(0));
        let client: Arc<dyn LockClient> = Arc::new(FlakyReleaseClient {
            failures_left: AtomicUsize::new(usize::MAX),
            release_calls: release_calls.clone(),
            release_success: true,
        });
        let lock_id = LockId::new_unique(&ObjectKey::new("bucket", "object"));

        DistributedLock::release_entries(vec![(lock_id, client)], "test_deferred_unlock_abandoned").await;

        assert_eq!(
            release_calls.load(Ordering::SeqCst),
            UNLOCK_RETRY_ATTEMPTS + DEFERRED_UNLOCK_BACKOFF.len(),
            "the retry budget stays bounded when cleanup cannot be confirmed"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn release_entries_keeps_retrying_unconfirmed_legacy_results() {
        let release_calls = Arc::new(AtomicUsize::new(0));
        let client: Arc<dyn LockClient> = Arc::new(FlakyReleaseClient {
            failures_left: AtomicUsize::new(0),
            release_calls: release_calls.clone(),
            release_success: false,
        });
        let lock_id = LockId::new_unique(&ObjectKey::new("bucket", "legacy-unlock"));

        DistributedLock::release_entries(vec![(lock_id, client)], "test_legacy_unlock").await;

        assert_eq!(
            release_calls.load(Ordering::SeqCst),
            UNLOCK_RETRY_ATTEMPTS + DEFERRED_UNLOCK_BACKOFF.len()
        );
    }

    /// Uses a real local backend, with only the transport acknowledgement lost.
    #[derive(Debug)]
    struct AckLossClient {
        backend: Arc<LocalClient>,
        lose_ack: AtomicBool,
        release_calls: AtomicUsize,
        released: tokio::sync::Notify,
        resume: tokio::sync::Notify,
    }

    impl AckLossClient {
        fn new(lose_ack: bool) -> Self {
            Self {
                backend: Arc::new(LocalClient::with_manager(Arc::new(GlobalLockManager::new()))),
                lose_ack: AtomicBool::new(lose_ack),
                release_calls: AtomicUsize::new(0),
                released: tokio::sync::Notify::new(),
                resume: tokio::sync::Notify::new(),
            }
        }
    }

    #[async_trait::async_trait]
    impl LockClient for AckLossClient {
        async fn acquire_lock(&self, request: &LockRequest) -> crate::Result<LockResponse> {
            self.backend.acquire_lock(request).await
        }

        async fn release(&self, lock_id: &LockId) -> crate::Result<bool> {
            self.cleanup_grant(lock_id)
                .await
                .map(|outcome| matches!(outcome, LockCleanupOutcome::Released))
        }

        async fn cleanup_grant(&self, lock_id: &LockId) -> crate::Result<LockCleanupOutcome> {
            self.release_calls.fetch_add(1, Ordering::SeqCst);
            let outcome = self.backend.cleanup_grant(lock_id).await?;
            if outcome == LockCleanupOutcome::Released {
                self.released.notify_one();
                if self.lose_ack.swap(false, Ordering::SeqCst) {
                    self.resume.notified().await;
                    return Err(LockError::internal("remote lock rpc timed out: release"));
                }
            }
            Ok(outcome)
        }

        async fn refresh(&self, lock_id: &LockId) -> crate::Result<bool> {
            self.backend.refresh(lock_id).await
        }

        async fn force_release(&self, lock_id: &LockId) -> crate::Result<bool> {
            self.backend.force_release(lock_id).await
        }

        async fn check_status(&self, lock_id: &LockId) -> crate::Result<Option<LockInfo>> {
            self.backend.check_status(lock_id).await
        }

        async fn get_stats(&self) -> crate::Result<LockStats> {
            self.backend.get_stats().await
        }

        async fn close(&self) -> crate::Result<()> {
            self.backend.close().await
        }

        async fn is_online(&self) -> bool {
            true
        }

        async fn is_local(&self) -> bool {
            false
        }
    }

    #[tokio::test(start_paused = true)]
    async fn release_entries_converges_after_lost_ack_without_releasing_successor() {
        let client = Arc::new(AckLossClient::new(true));
        let request = LockRequest::new(ObjectKey::new("bucket", "lost-release-ack"), LockType::Exclusive, "first-owner");
        assert!(client.acquire_lock(&request).await.expect("acquire predecessor").success);
        let cleanup_client = client.clone();
        let lock_id = request.lock_id.clone();
        let started = tokio::time::Instant::now();
        let cleanup = tokio::spawn(async move {
            DistributedLock::release_entries(vec![(lock_id, cleanup_client)], "test_lost_release_ack").await;
        });
        tokio::time::timeout(Duration::from_secs(1), client.released.notified())
            .await
            .expect("predecessor was released before its ACK was lost");

        let successor = LockRequest::new(request.resource.clone(), LockType::Exclusive, "successor");
        assert!(client.acquire_lock(&successor).await.expect("acquire successor").success);
        client.resume.notify_one();
        tokio::time::timeout(Duration::from_secs(60), cleanup)
            .await
            .expect("cleanup stays bounded")
            .expect("cleanup task completed");

        assert_eq!(
            client.release_calls.load(Ordering::SeqCst),
            2,
            "stop at the first authoritative absence response"
        );
        assert_eq!(started.elapsed(), UNLOCK_RETRY_BACKOFF, "do not enter the deferred retry schedule");
        assert!(
            client
                .backend
                .check_status(&request.lock_id)
                .await
                .expect("check predecessor")
                .is_none()
        );
        assert!(
            client
                .backend
                .check_status(&successor.lock_id)
                .await
                .expect("check successor")
                .is_some()
        );
        // FastLock acquisition deadlines use the real clock. Resume Tokio time
        // so virtual-time retries cannot expire the successor's lease while
        // waiting for the contender's real-time deadline.
        tokio::time::resume();
        let contender =
            LockRequest::new(request.resource, LockType::Exclusive, "contender").with_acquire_timeout(Duration::from_millis(50));
        assert!(
            !client
                .acquire_lock(&contender)
                .await
                .expect("check successor exclusivity")
                .success
        );
        assert!(client.backend.release(&successor.lock_id).await.expect("release successor"));
    }

    #[tokio::test(start_paused = true)]
    async fn release_before_acquire_keeps_the_pending_grant_cleanup_owner() {
        let client = Arc::new(AckLossClient::new(false));
        let blocker = LockRequest::new(ObjectKey::new("bucket", "release-before-grant"), LockType::Exclusive, "blocker");
        assert!(client.acquire_lock(&blocker).await.expect("acquire blocker").success);
        let late = LockRequest::new(blocker.resource.clone(), LockType::Exclusive, "late-owner");
        let late_id = late.lock_id.clone();
        let started = Arc::new(tokio::sync::Notify::new());
        let task_started = started.clone();
        let acquire_client = client.clone();
        let mut pending = tokio::task::JoinSet::new();
        pending.spawn(async move {
            task_started.notify_one();
            (0, acquire_client.acquire_lock(&late).await)
        });
        tokio::time::timeout(Duration::from_secs(1), started.notified())
            .await
            .expect("pending acquire task started");
        assert!(!client.release(&late_id).await.expect("release arrived before grant"));
        let clients: Arc<[Arc<dyn LockClient>]> = vec![client.clone() as Arc<dyn LockClient>].into();
        DistributedLock::spawn_pending_cleanup(pending, clients, late_id.clone(), "test_release_before_grant");
        assert!(
            client
                .backend
                .release(&blocker.lock_id)
                .await
                .expect("unblock pending acquire")
        );
        tokio::time::timeout(Duration::from_secs(1), client.released.notified())
            .await
            .expect("late grant retained its independent cleanup owner");

        assert!(
            client
                .backend
                .check_status(&late_id)
                .await
                .expect("check late grant")
                .is_none()
        );
        assert_eq!(
            client.release_calls.load(Ordering::SeqCst),
            2,
            "clean up again after the late grant actually succeeds"
        );
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
        completed_acquires: AtomicUsize,
        requested_acquires: AtomicUsize,
        release_observer: Option<std::sync::mpsc::Sender<()>>,
    }

    impl SequencedClient {
        fn new(plans: Vec<AcquirePlan>, seen_ids: Arc<Mutex<Vec<LockId>>>) -> Self {
            Self {
                plans: Mutex::new(plans.into()),
                active: tokio::sync::Mutex::new(HashMap::new()),
                seen_ids,
                completed_acquires: AtomicUsize::new(0),
                requested_acquires: AtomicUsize::new(0),
                release_observer: None,
            }
        }
    }

    #[async_trait::async_trait]
    impl LockClient for SequencedClient {
        async fn acquire_lock(&self, request: &LockRequest) -> crate::Result<LockResponse> {
            self.requested_acquires.fetch_add(1, Ordering::SeqCst);
            self.seen_ids.lock().unwrap().push(request.lock_id.clone());

            let plan = self
                .plans
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or(AcquirePlan::Success { delay: Duration::ZERO });

            let response = match plan {
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
            };
            self.completed_acquires.fetch_add(1, Ordering::SeqCst);
            response
        }

        async fn release(&self, lock_id: &LockId) -> crate::Result<bool> {
            let released = self.active.lock().await.remove(lock_id).is_some();
            if released && let Some(observer) = &self.release_observer {
                let _ = observer.send(());
            }
            Ok(released)
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
    fn only_backend_no_grant_responses_allow_same_round_retry() {
        assert!(is_definitive_lock_contention("Lock acquisition timeout"));
        assert!(is_definitive_lock_contention("Lock conflict: resource held by owner in Exclusive mode"));
        for error in [
            "Remote lock RPC timed out: Lock acquisition timeout",
            "Remote lock RPC failed: Lock conflict: resource held by owner in Exclusive mode",
            "Timeout { resource: bucket/object, timeout: 1s }",
            "permission denied",
            "unknown error",
        ] {
            assert!(
                !is_definitive_lock_contention(error),
                "ambiguous failure must not reuse a lock ID: {error}"
            );
        }
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
    async fn shared_lock_acquire_starts_quorum_plus_one_clients() {
        let seen_ids = (0..4).map(|_| Arc::new(Mutex::new(Vec::new()))).collect::<Vec<_>>();
        let clients: Vec<Arc<dyn LockClient>> = vec![
            Arc::new(SequencedClient::new(
                vec![AcquirePlan::Success {
                    delay: Duration::from_millis(20),
                }],
                seen_ids[0].clone(),
            )),
            Arc::new(SequencedClient::new(
                vec![AcquirePlan::Success {
                    delay: Duration::from_millis(20),
                }],
                seen_ids[1].clone(),
            )),
            Arc::new(SequencedClient::new(
                vec![AcquirePlan::Success {
                    delay: Duration::from_millis(200),
                }],
                seen_ids[2].clone(),
            )),
            Arc::new(SequencedClient::new(
                vec![AcquirePlan::Success { delay: Duration::ZERO }],
                seen_ids[3].clone(),
            )),
        ];
        let lock = DistributedLock::new("test".to_string(), clients, 3);
        let request = LockRequest::new(ObjectKey::new("bucket", "object"), LockType::Shared, "owner")
            .with_acquire_timeout(Duration::from_secs(1));

        let guard = lock
            .acquire_guard(&request)
            .await
            .expect("shared lock acquisition should not error")
            .expect("read quorum should be reached");

        assert_eq!(guard.entries.len(), 2, "read quorum should track exactly two acquired entries");
        assert_eq!(
            seen_ids[3].lock().unwrap().len(),
            0usize,
            "fourth client should stay unstarted while one hedge is pending"
        );
        drop(guard);
    }

    #[tokio::test]
    async fn shared_lock_acquire_replenishes_after_early_failure() {
        let seen_ids = (0..4).map(|_| Arc::new(Mutex::new(Vec::new()))).collect::<Vec<_>>();
        let clients: Vec<Arc<dyn LockClient>> = vec![
            Arc::new(SequencedClient::new(
                vec![AcquirePlan::Failure {
                    error: "lock already held",
                    delay: Duration::ZERO,
                }],
                seen_ids[0].clone(),
            )),
            Arc::new(SequencedClient::new(
                vec![AcquirePlan::Success {
                    delay: Duration::from_millis(80),
                }],
                seen_ids[1].clone(),
            )),
            Arc::new(SequencedClient::new(
                vec![AcquirePlan::Success {
                    delay: Duration::from_millis(120),
                }],
                seen_ids[2].clone(),
            )),
            Arc::new(SequencedClient::new(
                vec![AcquirePlan::Success { delay: Duration::ZERO }],
                seen_ids[3].clone(),
            )),
        ];
        let lock = DistributedLock::new("test".to_string(), clients, 3);
        let request = LockRequest::new(ObjectKey::new("bucket", "object"), LockType::Shared, "owner")
            .with_acquire_timeout(Duration::from_secs(1));

        let guard = lock
            .acquire_guard(&request)
            .await
            .expect("shared lock acquisition should not error")
            .expect("read quorum should be reached after replenishing the failed slot");

        assert_eq!(guard.entries.len(), 2, "read quorum should be reached");
        assert_eq!(
            seen_ids[3].lock().unwrap().len(),
            1usize,
            "fourth client should be started after an early failure"
        );
        drop(guard);
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

    fn clients_with_initial_quorum_loss(retries: &[AcquirePlan]) -> Vec<Arc<dyn LockClient>> {
        (0..4)
            .map(|index| {
                let first = if index < 2 {
                    AcquirePlan::Success { delay: Duration::ZERO }
                } else {
                    AcquirePlan::Failure {
                        error: "Remote lock RPC failed: node unavailable",
                        delay: Duration::ZERO,
                    }
                };
                let mut plans = vec![first];
                plans.extend_from_slice(retries);
                Arc::new(SequencedClient::new(plans, Arc::new(Mutex::new(Vec::new())))) as Arc<dyn LockClient>
            })
            .collect()
    }

    #[tokio::test(start_paused = true)]
    async fn acquire_guard_preserves_quorum_failure_when_last_retry_times_out() {
        let clients = clients_with_initial_quorum_loss(&[AcquirePlan::Success {
            delay: Duration::from_secs(1),
        }]);
        let lock = DistributedLock::new("test".to_string(), clients, 3);
        let request = LockRequest::new(ObjectKey::new("bucket", "object"), LockType::Exclusive, "owner")
            .with_acquire_timeout(Duration::from_millis(180));

        let result = lock.acquire_guard(&request).await;
        assert!(
            matches!(result, Err(LockError::QuorumNotReached { required: 3, .. })),
            "a late inconclusive retry must not hide an observed quorum failure: {result:?}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn acquire_guard_preserves_quorum_failure_when_retry_backoff_fits_but_attempt_times_out() {
        let clients = clients_with_initial_quorum_loss(&[AcquirePlan::Success {
            delay: Duration::from_secs(1),
        }]);
        let lock = DistributedLock::new("test".to_string(), clients, 3);
        let request = LockRequest::new(ObjectKey::new("bucket", "object"), LockType::Exclusive, "owner")
            .with_acquire_timeout(Duration::from_millis(500));

        let result = lock.acquire_guard(&request).await;
        assert!(
            matches!(result, Err(LockError::QuorumNotReached { required: 3, .. })),
            "an inconclusive final attempt must not hide an observed quorum failure: {result:?}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn acquire_guard_recovers_quorum_with_shortened_retry() {
        let clients = clients_with_initial_quorum_loss(&[AcquirePlan::Success { delay: Duration::ZERO }]);
        let lock = DistributedLock::new("test".to_string(), clients, 3);
        let request = LockRequest::new(ObjectKey::new("bucket", "object"), LockType::Exclusive, "owner")
            .with_acquire_timeout(Duration::from_millis(180));

        let guard = lock
            .acquire_guard(&request)
            .await
            .expect("a retry should recover from quorum loss")
            .expect("a recovered quorum should acquire the lock");
        assert_eq!(guard.entries.len(), 3);
    }

    #[tokio::test(start_paused = true)]
    async fn acquire_guard_replaces_quorum_failure_with_later_contention() {
        let clients = clients_with_initial_quorum_loss(
            &[AcquirePlan::Failure {
                error: "Lock acquisition timeout",
                delay: Duration::ZERO,
            }; 64],
        );
        let lock = DistributedLock::new("test".to_string(), clients, 3);
        let request = LockRequest::new(ObjectKey::new("bucket", "object"), LockType::Exclusive, "owner")
            .with_acquire_timeout(Duration::from_millis(180));

        let result = lock.acquire_guard(&request).await;
        assert!(matches!(result, Ok(None)), "later contention should replace the earlier quorum failure");
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

    #[tokio::test(start_paused = true)]
    async fn acquire_quorum_timeout_preserves_partial_grants() {
        let clients = (0..4)
            .map(|index| {
                Arc::new(SequencedClient::new(
                    vec![AcquirePlan::Success {
                        delay: if index == 0 { Duration::ZERO } else { Duration::from_secs(2) },
                    }],
                    Arc::new(Mutex::new(Vec::new())),
                ))
            })
            .collect::<Vec<_>>();
        let lock = DistributedLock::new(
            "partial-grants".to_string(),
            clients.iter().map(|client| client.clone() as Arc<dyn LockClient>).collect(),
            3,
        );
        let request = LockRequest::new(ObjectKey::new("bucket", "partial-grants"), LockType::Shared, "owner")
            .with_acquire_timeout(Duration::from_secs(1));
        let result = lock
            .acquire_lock_quorum_once(&request, request.acquire_timeout)
            .await
            .expect("partial quorum should return a timeout response");
        assert!(!result.response.success);
        assert!(result.attempt_timed_out);
        assert_matches!(result.failure_kind, Some(LockAcquireFailureKind::RetryableContention));
        assert_eq!(result.individual_locks.len(), 1, "retain received grants for timeout diagnostics");
        assert_eq!(result.individual_locks[0].lock_id, request.lock_id);
        wait_for_acquisition_cleanup(&clients).await;
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
    async fn acquire_guard_bounds_peer_contention_wait() {
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
            seen_timeouts.iter().all(|timeout| *timeout <= LOCK_PEER_ACQUIRE_TIMEOUT),
            "peer contention waits should be bounded by {LOCK_PEER_ACQUIRE_TIMEOUT:?}, saw {seen_timeouts:?}"
        );
    }

    async fn wait_for_acquisition_cleanup(clients: &[Arc<SequencedClient>]) {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let mut cleaned = true;
                for client in clients {
                    let requested = client.requested_acquires.load(Ordering::SeqCst);
                    cleaned &= requested >= 1
                        && client.completed_acquires.load(Ordering::SeqCst) == requested
                        && client.active.lock().await.is_empty();
                }
                if cleaned {
                    break;
                }
                tokio::time::advance(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("every completed and late grant must be cleaned up");
    }

    fn contention_retry_clients(error: &'static str, failures: usize, retry_delay: Duration) -> Vec<Arc<SequencedClient>> {
        (0..4)
            .map(|idx| {
                let plans = match idx {
                    2 => {
                        let mut plans = vec![
                            AcquirePlan::Failure {
                                error,
                                delay: Duration::ZERO
                            };
                            failures
                        ];
                        plans.push(AcquirePlan::Success { delay: retry_delay });
                        plans
                    }
                    3 => vec![AcquirePlan::Success {
                        delay: Duration::from_secs(3),
                    }],
                    _ => vec![AcquirePlan::Success { delay: Duration::ZERO }],
                };
                Arc::new(SequencedClient::new(plans, Arc::new(Mutex::new(Vec::new()))))
            })
            .collect()
    }

    #[tokio::test(start_paused = true)]
    async fn contended_voter_retries_without_discarding_grants_or_pending_peer() {
        for error in [
            "Lock acquisition timeout",
            "Lock conflict: resource held by owner in Exclusive mode",
        ] {
            let clients = contention_retry_clients(error, 1, Duration::ZERO);
            let lock = DistributedLock::new(
                "contention-with-silent-peer".to_string(),
                clients.iter().cloned().map(|client| client as Arc<dyn LockClient>).collect(),
                3,
            );
            let request = LockRequest::new(ObjectKey::new("bucket", "object"), LockType::Exclusive, "owner")
                .with_acquire_timeout(Duration::from_secs(2))
                .with_refresh_interval(Duration::ZERO);
            let started = Instant::now();
            let guard = lock
                .acquire_guard(&request)
                .await
                .expect("acquisition result")
                .expect("healthy voter retries");
            assert_eq!(started.elapsed(), LOCK_ACQUIRE_RETRY_INITIAL_BACKOFF);
            assert_eq!(guard.entries.len(), 3, "each voter can contribute at most one grant");
            let round_id = &guard.entries[0].lock_id;
            for (idx, client) in clients.iter().enumerate() {
                assert_eq!(client.requested_acquires.load(Ordering::SeqCst), if idx == 2 { 2 } else { 1 });
                assert!(client.seen_ids.lock().expect("observed IDs").iter().all(|id| id == round_id));
            }
            drop(guard);
            wait_for_acquisition_cleanup(&clients).await;
        }
    }

    #[tokio::test(start_paused = true)]
    async fn ambiguous_voter_failure_never_retries_with_the_same_id() {
        for error in ["Remote lock RPC timed out: RPC timed out after 1s", "unknown error"] {
            let clients = contention_retry_clients(error, 1, Duration::ZERO);
            let lock = DistributedLock::new(
                "ambiguous-voter-failure".to_string(),
                clients.iter().cloned().map(|client| client as Arc<dyn LockClient>).collect(),
                3,
            );
            let request = LockRequest::new(ObjectKey::new("bucket", "object"), LockType::Exclusive, "owner")
                .with_acquire_timeout(Duration::from_secs(2));
            let result = lock
                .acquire_lock_quorum_once(&request, request.acquire_timeout)
                .await
                .expect("round result");
            assert!(!result.response.success);
            assert!(result.attempt_timed_out);
            assert_eq!(clients[2].requested_acquires.load(Ordering::SeqCst), 1);
            wait_for_acquisition_cleanup(&clients).await;
        }
    }

    #[tokio::test(start_paused = true)]
    async fn permanent_contention_retries_are_bounded_by_backoff_and_deadline() {
        let clients = contention_retry_clients("Lock acquisition timeout", 16, Duration::ZERO);
        let lock = DistributedLock::new(
            "bounded-voter-retries".to_string(),
            clients.iter().cloned().map(|client| client as Arc<dyn LockClient>).collect(),
            3,
        );
        let request = LockRequest::new(ObjectKey::new("bucket", "object"), LockType::Exclusive, "owner")
            .with_acquire_timeout(Duration::from_secs(2));
        let started = Instant::now();
        let result = lock
            .acquire_lock_quorum_once(&request, request.acquire_timeout)
            .await
            .expect("round result");
        assert!(!result.response.success);
        assert!(result.attempt_timed_out);
        assert_eq!(started.elapsed(), request.acquire_timeout);
        assert_eq!(result.individual_locks.len(), 2, "retries must not duplicate existing votes");
        assert_eq!(clients[2].requested_acquires.load(Ordering::SeqCst), 4, "250ms, 500ms, 750ms backoffs");
        wait_for_acquisition_cleanup(&clients).await;
    }

    #[tokio::test(start_paused = true)]
    async fn impossible_round_releases_partial_cohort_without_waiting_for_retries() {
        let clients = (0..4)
            .map(|idx| {
                Arc::new(SequencedClient::new(
                    vec![if idx < 2 {
                        AcquirePlan::Success { delay: Duration::ZERO }
                    } else {
                        AcquirePlan::Failure {
                            error: "Lock acquisition timeout",
                            delay: Duration::ZERO,
                        }
                    }],
                    Arc::new(Mutex::new(Vec::new())),
                ))
            })
            .collect::<Vec<_>>();
        let lock = DistributedLock::new(
            "split-exclusive-cohort".to_string(),
            clients.iter().cloned().map(|client| client as Arc<dyn LockClient>).collect(),
            3,
        );
        let request = LockRequest::new(ObjectKey::new("bucket", "object"), LockType::Exclusive, "owner")
            .with_acquire_timeout(Duration::from_secs(2));
        let started = Instant::now();
        let result = lock
            .acquire_lock_quorum_once(&request, request.acquire_timeout)
            .await
            .expect("round result");
        assert!(!result.response.success);
        assert!(
            !result.attempt_timed_out,
            "a split minority must yield instead of retaining grants until the deadline"
        );
        assert_eq!(started.elapsed(), Duration::ZERO);
        assert!(
            clients
                .iter()
                .all(|client| client.requested_acquires.load(Ordering::SeqCst) == 1)
        );
        wait_for_acquisition_cleanup(&clients).await;
    }

    #[tokio::test(start_paused = true)]
    async fn timed_out_contention_retry_releases_its_late_grant() {
        let clients = contention_retry_clients("Lock acquisition timeout", 1, Duration::from_secs(2));
        let lock = DistributedLock::new(
            "late-retry-cleanup".to_string(),
            clients.iter().cloned().map(|client| client as Arc<dyn LockClient>).collect(),
            3,
        );
        let request = LockRequest::new(ObjectKey::new("bucket", "object"), LockType::Exclusive, "owner")
            .with_acquire_timeout(Duration::from_secs(1));
        let result = lock
            .acquire_lock_quorum_once(&request, request.acquire_timeout)
            .await
            .expect("round result");
        assert!(!result.response.success);
        assert!(result.attempt_timed_out);
        assert_eq!(clients[2].requested_acquires.load(Ordering::SeqCst), 2);
        wait_for_acquisition_cleanup(&clients).await;
    }

    #[tokio::test(start_paused = true)]
    async fn acquire_guard_keeps_slow_responses_in_the_same_quorum_round() {
        for lock_type in [LockType::Shared, LockType::Exclusive] {
            for delay in [Duration::from_millis(1200), Duration::from_millis(2500)] {
                let seen_ids = Arc::new(Mutex::new(Vec::new()));
                let clients = (0..4)
                    .map(|index| {
                        Arc::new(SequencedClient::new(
                            vec![
                                AcquirePlan::Success {
                                    delay: if index == 0 { Duration::ZERO } else { delay }
                                };
                                8
                            ],
                            seen_ids.clone(),
                        ))
                    })
                    .collect::<Vec<_>>();
                let lock = DistributedLock::new(
                    "slow-responses".to_string(),
                    clients.iter().map(|client| client.clone() as Arc<dyn LockClient>).collect(),
                    3,
                );
                let request = LockRequest::new(ObjectKey::new("bucket", "slow-responses"), lock_type, "owner")
                    .with_acquire_timeout(Duration::from_secs(5));
                let started = tokio::time::Instant::now();
                let guard = lock
                    .acquire_guard(&request)
                    .await
                    .expect("healthy delayed peers must not produce a quorum error")
                    .expect("slow responses must count toward the original quorum round");
                assert_eq!(guard.entries.len(), lock.required_quorum(lock_type));
                assert!(
                    started.elapsed() < Duration::from_secs(3),
                    "the first healthy round must finish within its latency"
                );
                let ids = seen_ids.lock().expect("recorded acquisition identities").clone();
                assert!(ids.len() >= 3, "quorum and spare voters must be contacted");
                assert!(
                    ids.iter().all(|id| id == &ids[0]),
                    "slow replies must not cause a fresh acquisition identity"
                );
                drop(guard);
                wait_for_acquisition_cleanup(&clients).await;
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn acquire_guard_bounds_slow_quorum_wait_and_cleans_late_grants() {
        let clients = (0..4)
            .map(|index| {
                Arc::new(SequencedClient::new(
                    vec![AcquirePlan::Success {
                        delay: if index == 0 { Duration::ZERO } else { Duration::from_secs(6) },
                    }],
                    Arc::new(Mutex::new(Vec::new())),
                ))
            })
            .collect::<Vec<_>>();
        let lock = DistributedLock::new(
            "late-responses".to_string(),
            clients.iter().map(|client| client.clone() as Arc<dyn LockClient>).collect(),
            3,
        );
        let request = LockRequest::new(ObjectKey::new("bucket", "late-responses"), LockType::Exclusive, "owner")
            .with_acquire_timeout(Duration::from_secs(5));
        let started = tokio::time::Instant::now();
        assert!(
            lock.acquire_guard(&request)
                .await
                .expect("deadline is a retryable timeout")
                .is_none()
        );
        assert_eq!(
            started.elapsed(),
            Duration::from_secs(5),
            "pending peers must not extend the caller's budget"
        );
        wait_for_acquisition_cleanup(&clients).await;
    }

    #[tokio::test(start_paused = true)]
    async fn acquire_quorum_rejects_ready_final_vote_after_deadline() {
        use futures::poll;
        use std::task::Poll;

        let clients = (0..3)
            .map(|index| {
                Arc::new(SequencedClient::new(
                    vec![AcquirePlan::Success {
                        delay: if index == 0 { Duration::ZERO } else { Duration::from_secs(5) },
                    }],
                    Arc::new(Mutex::new(Vec::new())),
                ))
            })
            .collect::<Vec<_>>();
        let lock = DistributedLock::new(
            "ready-after-deadline".to_string(),
            clients.iter().map(|client| client.clone() as Arc<dyn LockClient>).collect(),
            3,
        );
        let request = LockRequest::new(ObjectKey::new("bucket", "ready-after-deadline"), LockType::Shared, "owner")
            .with_acquire_timeout(Duration::from_secs(5));
        let mut attempt = Box::pin(tokio::task::unconstrained(
            lock.acquire_lock_quorum_once(&request, request.acquire_timeout),
        ));
        assert!(matches!(poll!(&mut attempt), Poll::Pending));
        tokio::time::timeout(Duration::from_secs(1), async {
            while clients
                .iter()
                .any(|client| client.requested_acquires.load(Ordering::SeqCst) == 0)
                || clients[0].completed_acquires.load(Ordering::SeqCst) == 0
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("initial voters must start before delaying the coordinator");
        // Consume the first vote, then leave the coordinator inside its
        // timeout-wrapped join while the remaining responses become ready.
        assert!(matches!(poll!(&mut attempt), Poll::Pending));
        tokio::time::advance(Duration::from_secs(6)).await;
        tokio::time::timeout(Duration::from_secs(1), async {
            while clients
                .iter()
                .any(|client| client.completed_acquires.load(Ordering::SeqCst) == 0)
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("late votes must be queued before the coordinator resumes");
        let result = attempt.await.expect("an expired round should return a response");
        assert!(
            !result.response.success,
            "ready join results must not override the caller's expired budget"
        );
        assert!(result.attempt_timed_out);
        wait_for_acquisition_cleanup(&clients).await;
    }

    #[tokio::test(start_paused = true)]
    async fn acquire_guard_rejects_quorum_after_grant_leases_expire() {
        let clients = (0..4)
            .map(|_| {
                Arc::new(SequencedClient::new(
                    vec![
                        AcquirePlan::Success {
                            delay: Duration::from_millis(1200)
                        };
                        8
                    ],
                    Arc::new(Mutex::new(Vec::new())),
                ))
            })
            .collect::<Vec<_>>();
        let lock = DistributedLock::new(
            "expired-responses".to_string(),
            clients.iter().map(|client| client.clone() as Arc<dyn LockClient>).collect(),
            3,
        );
        let request = LockRequest::new(ObjectKey::new("bucket", "expired-responses"), LockType::Exclusive, "owner")
            .with_acquire_timeout(Duration::from_secs(3))
            .with_ttl(Duration::from_secs(1));
        assert!(
            lock.acquire_guard(&request)
                .await
                .expect("expired grants may retry within the budget")
                .is_none()
        );
        wait_for_acquisition_cleanup(&clients).await;
    }

    #[tokio::test(start_paused = true)]
    async fn cancelling_acquisition_cleans_confirmed_and_pending_grants() {
        let clients = (0..4)
            .map(|index| {
                Arc::new(SequencedClient::new(
                    vec![AcquirePlan::Success {
                        delay: if index == 0 {
                            Duration::ZERO
                        } else {
                            Duration::from_millis(1200)
                        },
                    }],
                    Arc::new(Mutex::new(Vec::new())),
                ))
            })
            .collect::<Vec<_>>();
        let lock = DistributedLock::new(
            "cancelled-acquisition".to_string(),
            clients.iter().map(|client| client.clone() as Arc<dyn LockClient>).collect(),
            3,
        );
        let request = LockRequest::new(ObjectKey::new("bucket", "cancelled-acquisition"), LockType::Shared, "owner")
            .with_acquire_timeout(Duration::from_secs(5));
        let task = tokio::spawn(async move { lock.acquire_guard(&request).await });
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if !clients[0].active.lock().await.is_empty()
                    && clients
                        .iter()
                        .all(|client| client.requested_acquires.load(Ordering::SeqCst) >= 1)
                {
                    break;
                }
                tokio::time::advance(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("one grant and pending voters must exist before cancellation");
        task.abort();
        assert!(task.await.expect_err("the caller must be cancelled").is_cancelled());
        wait_for_acquisition_cleanup(&clients).await;
    }

    #[test]
    fn pending_cleanup_runs_outside_the_callers_runtime() {
        let (released, observed) = std::sync::mpsc::channel();
        let mut client =
            SequencedClient::new(vec![AcquirePlan::Success { delay: Duration::ZERO }], Arc::new(Mutex::new(Vec::new())));
        client.release_observer = Some(released);
        let client = Arc::new(client);
        let request = LockRequest::new(ObjectKey::new("bucket", "outside-runtime"), LockType::Exclusive, "owner");
        let fallback_id = request.lock_id.clone();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test runtime");
        let pending = runtime.block_on(async {
            let task_client = client.clone();
            let mut pending = tokio::task::JoinSet::new();
            pending.spawn(async move { (0, task_client.acquire_lock(&request).await) });
            while client.completed_acquires.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
            pending
        });
        drop(runtime);
        DistributedLock::spawn_pending_cleanup(
            pending,
            vec![client as Arc<dyn LockClient>].into(),
            fallback_id,
            "test_pending_cleanup_without_runtime",
        );
        observed
            .recv_timeout(Duration::from_secs(5))
            .expect("an unobserved grant must release even outside the caller runtime");
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

    #[tokio::test(start_paused = true)]
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

        let result = tokio::time::timeout(Duration::from_secs(1), lock.acquire_guard(&request)).await;

        assert_matches!(result, Ok(Ok(None)));
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

    #[test]
    fn batch_peer_diagnostics_preserves_response_failure_and_missing_counts() {
        let success = LockResponse {
            success: true,
            lock_info: None,
            error: None,
            wait_time: Duration::ZERO,
            position_in_queue: None,
        };
        let responses = Ok(vec![
            success.clone(),
            LockResponse::failure("Remote lock RPC timed out", Duration::ZERO),
            success,
        ]);
        assert_eq!(
            BatchLockPeerDiagnostics::from_response(2, 4, &responses, Duration::from_millis(17)),
            BatchLockPeerDiagnostics {
                peer_index: 2,
                granted_keys: 2,
                retryable_no_grant_keys: 0,
                terminal_no_grant_keys: 0,
                ambiguous_keys: 0,
                failed_response_keys: 1,
                missing_response_keys: 1,
                call_failed_keys: 0,
                extra_responses: 0,
                call_error_kind: None,
                elapsed_ms: 17,
            },
            "failed legacy responses and missing entries must remain distinct without guessing their cause"
        );
        let extra = BatchLockPeerDiagnostics::from_response(0, 1, &responses, Duration::ZERO);
        assert_eq!(extra.granted_keys, 1, "out-of-range responses cannot add valid grant votes");
        assert_eq!(extra.failed_response_keys, 0);
        assert_eq!(extra.missing_response_keys, 0);
        assert_eq!(extra.extra_responses, 2, "extra peer entries must remain visible");
    }

    #[test]
    fn batch_peer_diagnostics_counts_all_keys_for_a_typed_call_failure() {
        for (error, expected_kind) in [
            (LockError::timeout("peer", Duration::from_secs(3)), "timeout"),
            (LockError::internal("Remote lock RPC timed out"), "internal"),
            (LockError::permission_denied("request rejected"), "permission_denied"),
        ] {
            assert_eq!(
                BatchLockPeerDiagnostics::from_response(1, 1000, &Err(error), Duration::from_secs(3)),
                BatchLockPeerDiagnostics {
                    peer_index: 1,
                    granted_keys: 0,
                    retryable_no_grant_keys: 0,
                    terminal_no_grant_keys: 0,
                    ambiguous_keys: 0,
                    failed_response_keys: 0,
                    missing_response_keys: 0,
                    call_failed_keys: 1000,
                    extra_responses: 0,
                    call_error_kind: Some(expected_kind),
                    elapsed_ms: 3000,
                },
                "one failed peer call affects every requested key, and only its typed variant identifies the cause"
            );
        }
    }

    /// Per-object scripted client: grants only the named objects and records
    /// every released lock's resource.
    #[derive(Debug)]
    struct ScriptedBatchClient {
        grants: Vec<String>,
        releases: Arc<Mutex<Vec<String>>>,
    }

    impl ScriptedBatchClient {
        fn build(grants: &[&str]) -> (Arc<dyn LockClient>, Arc<Mutex<Vec<String>>>) {
            let releases = Arc::new(Mutex::new(Vec::new()));
            let client = Arc::new(Self {
                grants: grants.iter().map(|object| (*object).to_string()).collect(),
                releases: Arc::clone(&releases),
            }) as Arc<dyn LockClient>;
            (client, releases)
        }
    }

    #[async_trait::async_trait]
    impl LockClient for ScriptedBatchClient {
        async fn acquire_lock(&self, request: &LockRequest) -> crate::Result<LockResponse> {
            if self.grants.iter().any(|object| object == request.resource.object.as_ref()) {
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
            } else {
                Ok(LockResponse::failure("denied by test script", Duration::ZERO))
            }
        }

        async fn release(&self, lock_id: &LockId) -> crate::Result<bool> {
            self.releases
                .lock()
                .expect("release log")
                .push(lock_id.resource.object.to_string());
            Ok(true)
        }

        async fn refresh(&self, _lock_id: &LockId) -> crate::Result<bool> {
            Ok(true)
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

    /// A batch key granted by fewer than quorum clients fails alone, and its
    /// partial grants are released synchronously before the call returns
    /// (PR9155 review coverage for the distributed rollback path).
    #[tokio::test]
    async fn acquire_guard_batch_releases_partial_grants_below_quorum() {
        let (c0, c0_releases) = ScriptedBatchClient::build(&["kept", "lost"]);
        let (c1, _c1_releases) = ScriptedBatchClient::build(&["kept"]);
        let (c2, _c2_releases) = ScriptedBatchClient::build(&[]);
        let lock = DistributedLock::new("batch-test".to_string(), vec![c0, c1, c2], 2);

        let resources = vec![ObjectKey::new("bucket", "kept"), ObjectKey::new("bucket", "lost")];
        let results = lock
            .acquire_guard_batch(&resources, "owner", Duration::from_millis(200), Duration::from_secs(30))
            .await;

        assert!(results[0].is_ok(), "kept reached quorum: {:?}", results[0].as_ref().err());
        let error = results[1].as_ref().expect_err("lost must fail below quorum");
        assert!(
            matches!(
                error,
                LockError::QuorumNotReached {
                    required: 2,
                    achieved: 1
                }
            ),
            "lost must report its partial vote: {error:?}"
        );
        // Exactly the orphaned grant is released before the call returns,
        // while "kept" stays held by its live guard.
        assert_eq!(
            c0_releases.lock().expect("release log").clone(),
            vec!["lost".to_string()],
            "only the below-quorum key's partial grant is released synchronously"
        );
        drop(results);
    }
}
