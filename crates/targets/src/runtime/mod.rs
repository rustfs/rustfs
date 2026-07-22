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

pub mod adapter;
pub mod ops_diagnostics;
pub mod ops_profiler;
pub mod s3_hooks;
pub mod sidecar;
pub mod sidecar_protocol;
pub mod tls;

use crate::Target;
use crate::arn::TargetID;
use crate::plugin::PluginEvent;
use crate::store::{Key, Store, ensure_store_entry_raw_readable};
use crate::target::QueuedPayload;
use crate::target::TargetDeliverySnapshot;
use crate::target::{TargetHealth, TargetHealthReason, TargetHealthState};
use crate::{StoreError, TargetError};
use futures_util::stream::{FuturesUnordered, StreamExt};
use std::sync::Arc;
use std::{collections::HashMap, fmt::Debug};
use std::{future::Future, pin::Pin, time::Duration};
use tokio::sync::{Semaphore, mpsc};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

fn join_failure_reason(error: &tokio::task::JoinError) -> &'static str {
    if error.is_cancelled() {
        "join_cancelled"
    } else {
        "join_panicked"
    }
}

/// Maximum number of replay attempts before a stored entry is exhausted. Each attempt runs one full
/// send (one ack wait at the configured timeout for a JetStream entry), then a backoff sleep before
/// the next. The JetStream duplicate-window validation derives its worst-case retry lifetime from the
/// attempt count and REPLAY_BASE_RETRY_DELAY through inter_attempt_backoff_sum, the same source the
/// sleep schedule builds on. Pinning tests hold each layer of the coupling: the shared per-attempt
/// term, the sum over the schedule, retry_lifetime against that sum, and the realized sleep in
/// replay_backoff_sleep under a paused clock.
pub(crate) const REPLAY_MAX_RETRIES: usize = 5;

/// Base unit of the exponential replay backoff. The sleep before the retry at shift n is this
/// multiplied by 2^n.
pub(crate) const REPLAY_BASE_RETRY_DELAY: Duration = Duration::from_secs(2);

/// Backoff sleep before the retry attempt at `shift`: REPLAY_BASE_RETRY_DELAY doubled `shift` times.
/// The single per-attempt term the replay sleep and the duplicate-window sum both derive from.
pub(crate) fn replay_backoff_term(shift: u32) -> Duration {
    REPLAY_BASE_RETRY_DELAY.saturating_mul(1u32 << shift)
}

/// Sum of the backoff sleeps that run between `attempts` replay sends. The retry at shift k is
/// preceded by replay_backoff_term(k) for k in 1..attempts, so attempts minus one terms contribute
/// and no sleep follows the final attempt. retry_lifetime derives its worst-case span from the same
/// sum, so the sleep schedule and the stream duplicate-window requirement move together.
pub(crate) fn inter_attempt_backoff_sum(attempts: usize) -> Duration {
    let mut total = Duration::ZERO;
    for shift in 1..attempts as u32 {
        total = total.saturating_add(replay_backoff_term(shift));
    }
    total
}

/// Shared target trait object used by the runtime manager.
pub type SharedTarget<E> = Arc<dyn Target<E> + Send + Sync>;
type ReplayHook<E> = Arc<dyn Fn(ReplayEvent<E>) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;

const HEALTH_COLLECTION_TIMEOUT: Duration = Duration::from_secs(5);
const HEALTH_PROBE_CONCURRENCY: usize = 10;

pub(crate) enum PrepareTargetResult<E>
where
    E: PluginEvent,
{
    Ready(SharedTarget<E>),
    Degraded {
        error: TargetError,
        target: SharedTarget<E>,
    },
    Failed {
        error: TargetError,
        target: Box<dyn Target<E> + Send + Sync>,
    },
    Cancelled(Box<dyn Target<E> + Send + Sync>),
}

/// Tracks a running replay worker: its cancel channel and, when the worker was
/// spawned in-process, the [`JoinHandle`] used to await its exit on shutdown.
struct ReplayWorkerHandle {
    cancel_tx: mpsc::Sender<()>,
    join: Option<JoinHandle<()>>,
}

#[derive(Default)]
pub struct ReplayWorkerManager {
    cancellers: HashMap<String, ReplayWorkerHandle>,
}

impl Debug for ReplayWorkerManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReplayWorkerManager")
            .field("worker_count", &self.cancellers.len())
            .finish()
    }
}

impl ReplayWorkerManager {
    pub fn new() -> Self {
        Self {
            cancellers: HashMap::new(),
        }
    }

    /// Registers a cancel channel without a join handle.
    ///
    /// Used where the worker's lifetime is managed elsewhere (or in tests). Such
    /// workers are signalled on `stop_all` but not awaited. Prefer
    /// [`Self::insert_with_handle`] for in-process workers so shutdown can join
    /// them and avoid orphaned tasks.
    pub fn insert(&mut self, target_id: String, cancel_tx: mpsc::Sender<()>) {
        self.cancellers
            .insert(target_id, ReplayWorkerHandle { cancel_tx, join: None });
    }

    /// Registers a cancel channel together with the worker's join handle so
    /// `stop_all` can await the worker's exit.
    pub fn insert_with_handle(&mut self, target_id: String, cancel_tx: mpsc::Sender<()>, join: JoinHandle<()>) {
        self.cancellers.insert(
            target_id,
            ReplayWorkerHandle {
                cancel_tx,
                join: Some(join),
            },
        );
    }

    pub fn len(&self) -> usize {
        self.cancellers.len()
    }

    pub fn is_empty(&self) -> bool {
        self.cancellers.is_empty()
    }

    pub fn snapshot(&self, target_count: usize) -> RuntimeStatusSnapshot {
        RuntimeStatusSnapshot {
            replay_worker_count: self.len(),
            target_count,
        }
    }

    /// Stops every replay worker: it first signals cancellation to all of them,
    /// then strictly awaits each worker's exit. A worker already awaiting a
    /// delivery acknowledgement is allowed to finish; aborting it at an
    /// arbitrary deadline could leave an acknowledged queue entry undeleted and
    /// make the replacement worker deliver it again. Signalling before joining
    /// lets all workers wind down concurrently. Legacy joinless registrations
    /// can only be signalled.
    pub async fn stop_all(&mut self, log_prefix: &str) {
        let handles: Vec<(String, ReplayWorkerHandle)> = self.cancellers.drain().collect();
        let mut joins = std::collections::VecDeque::new();

        // Phase 1: signal cancellation to all workers.
        for (target_id, handle) in handles {
            tracing::info!(target_id = %target_id, "{log_prefix}");
            let _ = handle.cancel_tx.try_send(());
            if let Some(join) = handle.join {
                joins.push_back((target_id, join));
            } else {
                tracing::warn!(
                    target_id = %target_id,
                    "Replay worker has no join handle; cancellation was signalled but exit cannot be verified"
                );
            }
        }

        // Phase 2: strict join. Delivery operations own their own protocol
        // deadlines; lifecycle must not invent a shorter deadline that turns an
        // acknowledgement race into duplicate delivery.
        while let Some((target_id, join)) = joins.pop_front() {
            if let Err(err) = join.await {
                tracing::warn!(target_id = %target_id, reason = join_failure_reason(&err), "Replay worker terminated abnormally");
            }
        }
    }
}

pub struct RuntimeActivation<E>
where
    E: PluginEvent,
{
    pub replay_workers: ReplayWorkerManager,
    pub targets: Vec<SharedTarget<E>>,
}

/// Targets whose persistent queue stores are open and are ready to start
/// replay. This distinct stage prevents activation from skipping store open.
pub struct OpenedActivation<E>
where
    E: PluginEvent,
{
    pub(crate) targets: Vec<SharedTarget<E>>,
}

struct TargetActivationFailure {
    detail: String,
}

/// Targets that have completed initialization but have not started replay
/// workers yet. Keeping preparation dormant lets lifecycle orchestration stop
/// the previous workers before the replacement workers are spawned.
pub struct PreparedActivation<E>
where
    E: PluginEvent,
{
    failures: Vec<TargetActivationFailure>,
    rejected_targets: Vec<SharedTarget<E>>,
    pub(crate) targets: Vec<SharedTarget<E>>,
}

impl<E> PreparedActivation<E>
where
    E: PluginEvent,
{
    pub fn failure_summary(&self) -> Option<String> {
        if self.failures.is_empty() {
            return None;
        }

        Some(
            self.failures
                .iter()
                .map(|failure| failure.detail.clone())
                .collect::<Vec<_>>()
                .join("; "),
        )
    }

    pub fn extend_creation_failures(&mut self, failures: impl IntoIterator<Item = String>) {
        self.failures
            .extend(failures.into_iter().map(|detail| TargetActivationFailure { detail }));
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RuntimeStatusSnapshot {
    pub replay_worker_count: usize,
    pub target_count: usize,
}

/// A read-only runtime snapshot for a target instance.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RuntimeTargetSnapshot {
    pub failed_messages: u64,
    pub failed_store_length: u64,
    pub queue_length: u64,
    pub target_id: String,
    pub target_type: String,
    pub total_messages: u64,
}

pub type RuntimeTargetHealthState = TargetHealthState;
pub type RuntimeTargetHealthReason = TargetHealthReason;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeTargetHealthSnapshot {
    pub account_id: String,
    pub enabled: bool,
    pub error_message: Option<String>,
    pub state: RuntimeTargetHealthState,
    pub reason: RuntimeTargetHealthReason,
    pub target_id: String,
    pub target_type: String,
}

pub enum ReplayEvent<E>
where
    E: PluginEvent,
{
    Delivered {
        key: Key,
        target: SharedTarget<E>,
    },
    RetryableError {
        error: TargetError,
        key: Key,
        retry_count: usize,
        target: SharedTarget<E>,
    },
    Dropped {
        key: Key,
        reason: String,
        target: SharedTarget<E>,
    },
    PermanentFailure {
        error: TargetError,
        key: Key,
        target: SharedTarget<E>,
    },
    RetryExhausted {
        detail: String,
        key: Key,
        target: SharedTarget<E>,
    },
    UnreadableEntry {
        error: StoreError,
        key: Key,
        target: SharedTarget<E>,
    },
}

/// Shared runtime container for managing instantiated targets.
///
/// This intentionally focuses on low-risk shared lifecycle primitives first:
/// add/remove/close/list/snapshot. Replay workers and reload orchestration can
/// be layered on top in later phases.
pub struct TargetRuntimeManager<E>
where
    E: PluginEvent,
{
    targets: HashMap<String, SharedTarget<E>>,
}

impl<E> Default for TargetRuntimeManager<E>
where
    E: PluginEvent,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<E> Debug for TargetRuntimeManager<E>
where
    E: PluginEvent,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TargetRuntimeManager")
            .field("target_count", &self.targets.len())
            .finish()
    }
}

impl<E> TargetRuntimeManager<E>
where
    E: PluginEvent,
{
    pub fn new() -> Self {
        Self { targets: HashMap::new() }
    }

    pub fn add_arc(&mut self, target: SharedTarget<E>) -> Option<SharedTarget<E>> {
        let key = target.id().to_string();
        self.targets.insert(key, target)
    }

    pub fn add_boxed(&mut self, target: Box<dyn Target<E> + Send + Sync>) -> Option<SharedTarget<E>> {
        self.add_arc(Arc::from(target))
    }

    pub fn get(&self, key: &str) -> Option<SharedTarget<E>> {
        self.targets.get(key).cloned()
    }

    pub fn get_by_target_id(&self, target_id: &TargetID) -> Option<SharedTarget<E>> {
        self.get(&target_id.to_string())
    }

    pub fn remove(&mut self, key: &str) -> Option<SharedTarget<E>> {
        self.targets.remove(key)
    }

    pub fn remove_by_target_id(&mut self, target_id: &TargetID) -> Option<SharedTarget<E>> {
        self.remove(&target_id.to_string())
    }

    pub fn clear(&mut self) {
        self.targets.clear();
    }

    pub async fn remove_and_close(&mut self, key: &str) -> Option<SharedTarget<E>> {
        let target = self.targets.remove(key)?;
        if let Err(err) = target.close().await {
            tracing::error!(target_id = %key, error = %err, "Failed to close target during removal");
        }
        Some(target)
    }

    pub async fn remove_by_target_id_and_close(&mut self, target_id: &TargetID) -> Option<SharedTarget<E>> {
        self.remove_and_close(&target_id.to_string()).await
    }

    /// Closes and removes every target, returning the id and error of each target whose close failed.
    /// Surfacing them lets a caller fail an explicit shutdown while still tearing down the rest of the
    /// runtime.
    pub async fn clear_and_close(&mut self) -> Vec<(String, TargetError)> {
        let targets = std::mem::take(&mut self.targets);
        let mut closes = FuturesUnordered::new();
        for (target_id, target) in targets {
            closes.push(async move { (target_id, target.close().await) });
        }

        let mut errors = Vec::new();
        while let Some((target_id, result)) = closes.next().await {
            if let Err(err) = result {
                tracing::error!(target_id = %target_id, error = %err, "Failed to close target during shutdown");
                errors.push((target_id, err));
            }
        }
        errors
    }

    pub fn target_ids(&self) -> Vec<TargetID> {
        self.targets.values().map(|target| target.id()).collect()
    }

    pub fn keys(&self) -> Vec<String> {
        self.targets.keys().cloned().collect()
    }

    pub fn values(&self) -> Vec<SharedTarget<E>> {
        self.targets.values().cloned().collect()
    }

    pub fn len(&self) -> usize {
        self.targets.len()
    }

    pub fn is_empty(&self) -> bool {
        self.targets.is_empty()
    }

    pub fn snapshots(&self) -> Vec<RuntimeTargetSnapshot> {
        let mut snapshots = Vec::with_capacity(self.targets.len());
        for target in self.targets.values() {
            let delivery = target.delivery_snapshot();
            let target_id = target.id();
            snapshots.push(snapshot_from_delivery(target_id, delivery));
        }
        snapshots.sort_by(|a, b| a.target_id.cmp(&b.target_id));
        snapshots
    }

    pub fn status_snapshot(&self, replay_workers: &ReplayWorkerManager) -> RuntimeStatusSnapshot {
        replay_workers.snapshot(self.len())
    }

    pub async fn health_snapshots(&self) -> Vec<RuntimeTargetHealthSnapshot> {
        health_snapshots_for_targets(self.values()).await
    }
}

pub async fn health_snapshots_for_targets<E>(targets: Vec<SharedTarget<E>>) -> Vec<RuntimeTargetHealthSnapshot>
where
    E: PluginEvent,
{
    let deadline = tokio::time::Instant::now() + HEALTH_COLLECTION_TIMEOUT;
    let permits = Arc::new(Semaphore::new(HEALTH_PROBE_CONCURRENCY));
    let mut tasks = Vec::with_capacity(targets.len());
    for target in targets {
        let permits = Arc::clone(&permits);
        let enabled = target.is_enabled();
        let target_id = target.id();
        let task = tokio::spawn(async move {
            if enabled {
                match tokio::time::timeout_at(deadline, async {
                    let Ok(_permit) = permits.acquire_owned().await else {
                        return TargetHealth::error(TargetHealthReason::HealthCheckFailed);
                    };
                    target.health().await
                })
                .await
                {
                    Ok(health) => health,
                    Err(_) => TargetHealth::error(TargetHealthReason::TimedOut),
                }
            } else {
                TargetHealth::disabled()
            }
        });
        tasks.push((enabled, target_id, task));
    }

    let mut snapshots = Vec::with_capacity(tasks.len());
    for (enabled, target_id, task) in tasks {
        let health = task
            .await
            .unwrap_or_else(|_| TargetHealth::error(TargetHealthReason::HealthCheckFailed));
        snapshots.push(RuntimeTargetHealthSnapshot {
            account_id: target_id.id.clone(),
            enabled,
            error_message: (health.state == TargetHealthState::Error).then(|| health.reason.as_str().to_string()),
            state: health.state,
            reason: health.reason,
            target_id: target_id.to_string(),
            target_type: target_id.name,
        });
    }

    snapshots.sort_by(|a, b| a.target_id.cmp(&b.target_id));
    snapshots
}

fn snapshot_from_delivery(target_id: TargetID, delivery: TargetDeliverySnapshot) -> RuntimeTargetSnapshot {
    RuntimeTargetSnapshot {
        failed_messages: delivery.failed_messages,
        failed_store_length: delivery.failed_store_length,
        queue_length: delivery.queue_length,
        target_id: target_id.to_string(),
        target_type: target_id.name,
        total_messages: delivery.total_messages,
    }
}

pub async fn init_target_and_optionally_start_replay<E, F, G>(
    target: Box<dyn Target<E> + Send + Sync>,
    on_replay_start: F,
    start_replay: G,
) -> Option<(SharedTarget<E>, Option<(mpsc::Sender<()>, JoinHandle<()>)>)>
where
    E: PluginEvent,
    F: FnOnce(&str, bool),
    G: FnOnce(
        Box<dyn Store<QueuedPayload, Error = StoreError, Key = Key> + Send>,
        SharedTarget<E>,
    ) -> (mpsc::Sender<()>, JoinHandle<()>),
{
    let shared = match prepare_target(target, None).await {
        PrepareTargetResult::Ready(target) => target,
        PrepareTargetResult::Degraded { target, .. } => target,
        PrepareTargetResult::Failed { target, .. } => {
            let _ = target.close().await;
            return None;
        }
        PrepareTargetResult::Cancelled(_) => unreachable!("preparation without a cancellation token cannot be cancelled"),
    };
    let target_id = shared.id().to_string();
    if !shared.is_enabled() {
        on_replay_start(&target_id, false);
        return Some((shared, None));
    }

    let cancel = shared
        .store()
        .map(|store| start_replay(store.boxed_clone(), Arc::clone(&shared)));
    on_replay_start(&target_id, cancel.is_some());
    Some((shared, cancel))
}

pub(crate) async fn prepare_target<E>(
    target: Box<dyn Target<E> + Send + Sync>,
    cancellation: Option<&CancellationToken>,
) -> PrepareTargetResult<E>
where
    E: PluginEvent,
{
    let target_id = target.id().to_string();
    let has_store = target.store().is_some();

    let init_result = match cancellation {
        Some(cancellation) => {
            tokio::select! {
                biased;
                _ = cancellation.cancelled() => return PrepareTargetResult::Cancelled(target),
                result = target.init() => result,
            }
        }
        None => target.init().await,
    };

    if let Err(err) = init_result {
        tracing::error!(target_id = %target_id, reason = "initialization_failed", "Failed to initialize target");
        if !has_store {
            return PrepareTargetResult::Failed { error: err, target };
        }
        tracing::warn!(
            target_id = %target_id,
            "Proceeding with store-backed target despite init failure"
        );
        return PrepareTargetResult::Degraded {
            error: err,
            target: Arc::from(target),
        };
    }

    PrepareTargetResult::Ready(Arc::from(target))
}

type ActivatedTarget<E> = (SharedTarget<E>, Option<(mpsc::Sender<()>, JoinHandle<()>)>);

pub async fn activate_targets_with_replay<E, F, Fut>(
    targets: Vec<Box<dyn Target<E> + Send + Sync>>,
    mut activate_one: F,
) -> RuntimeActivation<E>
where
    E: PluginEvent,
    F: FnMut(Box<dyn Target<E> + Send + Sync>) -> Fut,
    Fut: Future<Output = Option<ActivatedTarget<E>>>,
{
    let mut replay_workers = ReplayWorkerManager::new();
    let mut shared_targets = Vec::new();

    for target in targets {
        if let Some((shared_target, replay)) = activate_one(target).await {
            let target_id = shared_target.id().to_string();
            if let Some((cancel_tx, join)) = replay {
                replay_workers.insert_with_handle(target_id, cancel_tx, join);
            }
            shared_targets.push(shared_target);
        }
    }

    RuntimeActivation {
        replay_workers,
        targets: shared_targets,
    }
}

pub fn start_replay_worker<E>(
    mut store: Box<dyn Store<QueuedPayload, Error = StoreError, Key = Key> + Send>,
    target: SharedTarget<E>,
    hook: ReplayHook<E>,
    semaphore: Option<Arc<Semaphore>>,
    batch_timeout: Duration,
    idle_sleep: Duration,
) -> (mpsc::Sender<()>, JoinHandle<()>)
where
    E: PluginEvent,
{
    let (cancel_tx, cancel_rx) = mpsc::channel(1);

    let join = tokio::spawn(async move {
        stream_replay_worker(&mut *store, target, cancel_rx, hook, semaphore, batch_timeout, idle_sleep).await;
    });

    (cancel_tx, join)
}

/// Number of readable entries accumulated before a replay batch is flushed under a single semaphore
/// permit.
const REPLAY_BATCH_SIZE: usize = 16;

/// Sleeps for `dur` unless a cancel signal arrives first. Returns `true` if
/// cancellation was observed. Used so idle waits, inter-scan pauses, and retry
/// backoff all react promptly to shutdown instead of blocking for the full
/// duration.
async fn sleep_or_cancelled(dur: Duration, cancel_rx: &mut mpsc::Receiver<()>) -> bool {
    tokio::select! {
        biased;
        _ = cancel_rx.recv() => true,
        _ = tokio::time::sleep(dur) => false,
    }
}

/// Seeds an interval tracker one interval in the past so the first eligible tick runs the action
/// immediately. The monotonic clock starts at host boot, so a process started within one interval
/// of boot cannot represent an instant that far back. The seed falls back to now in that case and
/// the first run waits one full interval.
fn seed_interval_start(now: tokio::time::Instant, interval: Duration) -> tokio::time::Instant {
    now.checked_sub(interval).unwrap_or(now)
}

async fn stream_replay_worker<E>(
    store: &mut (dyn Store<QueuedPayload, Error = StoreError, Key = Key> + Send),
    target: SharedTarget<E>,
    mut cancel_rx: mpsc::Receiver<()>,
    hook: ReplayHook<E>,
    semaphore: Option<Arc<Semaphore>>,
    batch_timeout: Duration,
    idle_sleep: Duration,
) where
    E: PluginEvent,
{
    // Lower bound between two failed-store expired-entry removal scans. Far below the retention TTL,
    // so retention is unaffected, while keeping the read_dir scan off every idle tick.
    const FAILED_STORE_PRUNE_INTERVAL: Duration = Duration::from_secs(60);

    let mut batch_keys = Vec::with_capacity(REPLAY_BATCH_SIZE);
    let mut last_flush = tokio::time::Instant::now();
    // Seeded one interval back so the first eligible tick runs the removal. Near host boot the
    // seed is now and the first removal waits one interval.
    let mut last_prune = seed_interval_start(tokio::time::Instant::now(), FAILED_STORE_PRUNE_INTERVAL);

    loop {
        if cancel_rx.try_recv().is_ok() {
            return;
        }

        // Remove expired failed-store entries on the replay tick rather than a separate timer, at most
        // once per interval so the idle path does not scan the directory on every tick. Only a target
        // that records terminal failures carries a failed store, so a target without one skips the scan.
        if let Some(failed_store) = target.failed_store()
            && last_prune.elapsed() >= FAILED_STORE_PRUNE_INTERVAL
        {
            // Run the stat-and-sort scan off the async worker thread. The owned handle shares the same
            // directory and cached-count state through its Arc handles, so the scan reconciles the
            // count the live handles read.
            let maintenance_failed = failed_store.boxed_clone_failed();
            let outcome = tokio::task::spawn_blocking(move || maintenance_failed.prune_failed_store()).await;
            match outcome {
                Ok(Err(err)) => {
                    tracing::warn!(target_id = %target.id(), error = %err, "Failed to prune the failed-events store");
                }
                Ok(Ok(_)) => {}
                Err(join_err) => {
                    tracing::warn!(
                        target_id = %target.id(),
                        reason = join_failure_reason(&join_err),
                        "The failed-events maintenance task failed to join"
                    );
                }
            }
            last_prune = tokio::time::Instant::now();
        }

        let keys = store.list();
        if keys.is_empty() {
            if !batch_keys.is_empty() && last_flush.elapsed() >= batch_timeout {
                if process_replay_batch(&*store, &mut batch_keys, target.clone(), &hook, semaphore.clone(), &mut cancel_rx).await
                {
                    return;
                }
                last_flush = tokio::time::Instant::now();
            }
            if sleep_or_cancelled(idle_sleep, &mut cancel_rx).await {
                return;
            }
            continue;
        }

        for key in keys {
            if cancel_rx.try_recv().is_ok() {
                if !batch_keys.is_empty() {
                    process_replay_batch(&*store, &mut batch_keys, target.clone(), &hook, semaphore.clone(), &mut cancel_rx)
                        .await;
                }
                return;
            }

            match ensure_store_entry_raw_readable(&*store, &key) {
                Ok(true) => {}
                Ok(false) => continue,
                Err(err) => {
                    hook(ReplayEvent::UnreadableEntry {
                        error: err,
                        key,
                        target: target.clone(),
                    })
                    .await;
                    continue;
                }
            }

            // Skip keys already pending in the current batch: an un-flushed
            // partial batch carries across scans, and `store.list()` keeps
            // returning not-yet-delivered keys, so without this guard the same
            // key would be enqueued repeatedly.
            if batch_keys
                .iter()
                .any(|pending: &Key| pending.to_key_string() == key.to_key_string())
            {
                continue;
            }

            batch_keys.push(key);
            // Flush once a full batch has accumulated or the batch has aged past
            // batch_timeout — real size/time-based batching, not once-per-entry.
            if batch_keys.len() >= REPLAY_BATCH_SIZE || last_flush.elapsed() >= batch_timeout {
                if process_replay_batch(&*store, &mut batch_keys, target.clone(), &hook, semaphore.clone(), &mut cancel_rx).await
                {
                    return;
                }
                last_flush = tokio::time::Instant::now();
            }
        }

        // Flush a partial batch that has aged past batch_timeout, so a lone or slow-arriving entry is
        // delivered rather than stranded waiting for a full batch to accumulate.
        if !batch_keys.is_empty() && last_flush.elapsed() >= batch_timeout {
            if process_replay_batch(&*store, &mut batch_keys, target.clone(), &hook, semaphore.clone(), &mut cancel_rx).await {
                return;
            }
            last_flush = tokio::time::Instant::now();
        }

        if sleep_or_cancelled(Duration::from_millis(100), &mut cancel_rx).await {
            return;
        }
    }
}

/// Delivers a batch of queued entries, each send bounded by a freshly acquired semaphore permit held
/// across the send and released before any backoff sleep or failed-store move.
///
/// Returns `true` if a cancel signal was observed while processing (e.g. during
/// retry backoff), so the caller can stop promptly instead of continuing to
/// drain a store that a replacement worker may already own.
async fn process_replay_batch<E>(
    store: &(dyn Store<QueuedPayload, Error = StoreError, Key = Key> + Send),
    batch_keys: &mut Vec<Key>,
    target: SharedTarget<E>,
    hook: &ReplayHook<E>,
    semaphore: Option<Arc<Semaphore>>,
    cancel_rx: &mut mpsc::Receiver<()>,
) -> bool
where
    E: PluginEvent,
{
    const MAX_RETRIES: usize = REPLAY_MAX_RETRIES;

    if batch_keys.is_empty() {
        return false;
    }

    let mut cancelled = false;
    'keys: for key in batch_keys.iter() {
        let mut retry_count = 0usize;
        let mut success = false;
        let mut last_retryable_detail = String::new();

        while retry_count < MAX_RETRIES && !success {
            // The permit bounds how many replay sends run concurrently across targets sharing the
            // semaphore. It is held across the send await so the cap is real, then released before
            // any backoff sleep or failed-store move, because the sleep blocks for the backoff and
            // holding the shared permit across it would throttle the whole replay path. An absent
            // semaphore means no bound (the audit path).
            let permit = match &semaphore {
                None => None,
                Some(sem) => match sem.clone().acquire_owned().await {
                    Ok(permit) => Some(permit),
                    Err(err) => {
                        tracing::error!(error = %err, "Failed to acquire replay semaphore permit");
                        // Drop the batch so its keys do not strand under the dedup guard.
                        batch_keys.clear();
                        return cancelled;
                    }
                },
            };

            let result = target.send_from_store(key.clone()).await;
            drop(permit);

            match result {
                Ok(_) => {
                    hook(ReplayEvent::Delivered {
                        key: key.clone(),
                        target: target.clone(),
                    })
                    .await;
                    success = true;
                }
                Err(err) => match err {
                    TargetError::NotConnected
                    | TargetError::Timeout(_)
                    | TargetError::JetStreamPublish { retryable: true, .. } => {
                        retry_count += 1;
                        last_retryable_detail = err.to_string();
                        hook(ReplayEvent::RetryableError {
                            error: err,
                            key: key.clone(),
                            retry_count,
                            target: target.clone(),
                        })
                        .await;
                        // The backoff runs only when another attempt follows, so the final failure
                        // proceeds straight to the exhaustion handling. Cancellation is observed
                        // during the sleep so shutdown/reload is not blocked for the full
                        // (potentially many-second) delay.
                        if retry_count < MAX_RETRIES && replay_backoff_sleep(key, retry_count, cancel_rx).await {
                            cancelled = true;
                            break 'keys;
                        }
                    }
                    TargetError::JetStreamPublish { retryable: false, .. } => {
                        // The hook fires only on a completed move. On a failed move the entry stays
                        // live, the next scan retries the move, and a repaired failed-store
                        // directory heals without a restart, with the hook then firing exactly once.
                        if move_failed_entry(store, &target, key, &err, retry_count as u32).await {
                            hook(ReplayEvent::PermanentFailure {
                                error: err,
                                key: key.clone(),
                                target: target.clone(),
                            })
                            .await;
                        }
                        break;
                    }
                    TargetError::Dropped(reason) => {
                        hook(ReplayEvent::Dropped {
                            key: key.clone(),
                            reason,
                            target: target.clone(),
                        })
                        .await;
                        break;
                    }
                    other => {
                        hook(ReplayEvent::PermanentFailure {
                            error: other,
                            key: key.clone(),
                            target: target.clone(),
                        })
                        .await;
                        break;
                    }
                },
            }
        }

        if retry_count >= MAX_RETRIES && !success {
            // A retryable error that never succeeded within the bound leaves the entry on the live
            // queue for the next scan. The exhaustion is reported through the hook for metrics.
            hook(ReplayEvent::RetryExhausted {
                detail: last_retryable_detail,
                key: key.clone(),
                target: target.clone(),
            })
            .await;
        }
    }

    batch_keys.clear();
    cancelled
}

/// Sleeps the exponential backoff for a retry attempt, with key-derived jitter. Returns `true` if a
/// cancel signal arrives first, so the caller can stop promptly instead of blocking for the full delay.
async fn replay_backoff_sleep(key: &Key, retry_count: usize, cancel_rx: &mut mpsc::Receiver<()>) -> bool {
    let jitter = Duration::from_millis(key.to_string().len() as u64 % 500);
    sleep_or_cancelled(replay_backoff_term(retry_count as u32) + jitter, cancel_rx).await
}

/// Reacts to a terminal classification by delegating to the target's terminal-failure handling and
/// reports whether the entry was handled. The target moves the entry to its failed-events store and
/// reports true, or declines and reports false so the caller keeps the entry live and skips the
/// final-failure hook. A target without a terminal-failure store always declines.
async fn move_failed_entry<E>(
    store: &(dyn Store<QueuedPayload, Error = StoreError, Key = Key> + Send),
    target: &SharedTarget<E>,
    key: &Key,
    error: &TargetError,
    retry_count: u32,
) -> bool
where
    E: PluginEvent,
{
    target.handle_terminal_failure(store, key, error, retry_count).await
}

#[cfg(test)]
mod tests {
    use super::{HEALTH_PROBE_CONCURRENCY, TargetRuntimeManager, health_snapshots_for_targets};
    use crate::PluginEvent;
    use crate::StoreError;
    use crate::arn::TargetID;
    use crate::store::{Key, Store};
    use crate::target::{EntityTarget, QueuedPayload, QueuedPayloadMeta};
    use crate::{SharedTarget, Target, TargetError};
    use async_trait::async_trait;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::time::Duration;
    use tokio::sync::{Notify, Semaphore};

    #[tokio::test(start_paused = true)]
    async fn seed_interval_start_backdates_by_one_interval() {
        // With one interval of clock history available, the seed is exactly one interval in the
        // past, so the first eligible tick runs the action immediately.
        let origin = tokio::time::Instant::now();
        tokio::time::advance(std::time::Duration::from_secs(60)).await;
        let now = tokio::time::Instant::now();
        assert_eq!(super::seed_interval_start(now, std::time::Duration::from_secs(60)), origin);
    }

    #[test]
    fn seed_interval_start_falls_back_to_now_when_the_clock_is_too_young() {
        // Duration::MAX reaches past the monotonic clock origin on any host, so the checked
        // subtraction yields no instant and the seed falls back to now instead of panicking.
        let now = tokio::time::Instant::now();
        assert_eq!(super::seed_interval_start(now, std::time::Duration::MAX), now);
    }

    #[test]
    fn inter_attempt_backoff_sum_matches_the_realized_sleep_schedule() {
        // Sums the shared per-attempt term over the schedule shifts (1..REPLAY_MAX_RETRIES, no
        // trailing sleep after the last attempt) and asserts it equals inter_attempt_backoff_sum.
        // The sleep's own use of the term is pinned by the paused-clock test below.
        let mut realized = std::time::Duration::ZERO;
        for shift in 1..super::REPLAY_MAX_RETRIES as u32 {
            realized += super::replay_backoff_term(shift);
        }
        assert_eq!(super::inter_attempt_backoff_sum(super::REPLAY_MAX_RETRIES), realized);
        // 2s * (2 + 4 + 8 + 16) at the default base.
        assert_eq!(realized, std::time::Duration::from_secs(60));
    }

    #[tokio::test(start_paused = true)]
    async fn replay_backoff_sleep_sleeps_the_term_for_the_retry_plus_key_jitter() {
        // Drives the sleep itself, so a shifted argument at its replay_backoff_term call (for
        // example retry_count minus 1) fails here even though the term and sum tests still pass.
        // The jitter is deterministic, the key string length modulo 500 milliseconds, so the
        // virtual elapsed time is exact under the paused clock.
        let key = Key {
            name: "0198c0de-0000-7000-8000-000000000000".to_string(),
            extension: ".event".to_string(),
            item_count: 1,
            compress: false,
        };
        let jitter = std::time::Duration::from_millis(key.to_string().len() as u64 % 500);
        let retry_count = 3usize;

        // The held sender keeps the cancel channel open so the sleep runs to its deadline.
        let (_cancel_tx, mut cancel_rx) = tokio::sync::mpsc::channel::<()>(1);
        let start = tokio::time::Instant::now();
        let cancelled = super::replay_backoff_sleep(&key, retry_count, &mut cancel_rx).await;

        assert!(!cancelled, "no cancel signal was sent");
        assert_eq!(
            start.elapsed(),
            super::replay_backoff_term(retry_count as u32) + jitter,
            "the sleep is the term for this retry count plus the key-derived jitter"
        );
    }

    #[derive(Clone)]
    struct TestTarget {
        id: TargetID,
        block_on_close: Arc<AtomicBool>,
        close_gate: Arc<Semaphore>,
        close_calls: Arc<AtomicUsize>,
        enabled: bool,
        health_delay: Duration,
        close_started: Arc<Notify>,
    }

    impl TestTarget {
        fn new(id: &str, name: &str) -> Self {
            Self {
                id: TargetID::new(id.to_string(), name.to_string()),
                block_on_close: Arc::new(AtomicBool::new(false)),
                close_gate: Arc::new(Semaphore::new(0)),
                close_calls: Arc::new(AtomicUsize::new(0)),
                enabled: true,
                health_delay: Duration::ZERO,
                close_started: Arc::new(Notify::new()),
            }
        }

        fn with_health_delay(id: &str, delay: Duration) -> Self {
            Self {
                health_delay: delay,
                ..Self::new(id, "webhook")
            }
        }

        fn disabled(id: &str) -> Self {
            Self {
                enabled: false,
                ..Self::new(id, "webhook")
            }
        }
    }

    #[async_trait]
    impl<E> Target<E> for TestTarget
    where
        E: PluginEvent,
    {
        fn id(&self) -> TargetID {
            self.id.clone()
        }

        async fn is_active(&self) -> Result<bool, TargetError> {
            tokio::time::sleep(self.health_delay).await;
            Ok(true)
        }

        async fn save(&self, _event: Arc<EntityTarget<E>>) -> Result<(), TargetError> {
            Ok(())
        }

        async fn send_raw_from_store(&self, _key: Key, _body: Vec<u8>, _meta: QueuedPayloadMeta) -> Result<(), TargetError> {
            Ok(())
        }

        async fn close(&self) -> Result<(), TargetError> {
            self.close_calls.fetch_add(1, Ordering::SeqCst);
            self.close_started.notify_one();
            if self.block_on_close.load(Ordering::SeqCst) {
                let _permit = self.close_gate.acquire().await.expect("close gate should remain open");
            }
            Ok(())
        }

        fn store(&self) -> Option<&(dyn Store<QueuedPayload, Error = StoreError, Key = Key> + Send + Sync)> {
            None
        }

        fn clone_dyn(&self) -> Box<dyn Target<E> + Send + Sync> {
            Box::new(self.clone())
        }

        fn is_enabled(&self) -> bool {
            self.enabled
        }
    }

    #[tokio::test]
    async fn runtime_manager_removes_and_closes_target() {
        let mut manager = TargetRuntimeManager::<String>::new();
        let target = TestTarget::new("primary", "webhook");
        let close_calls = Arc::clone(&target.close_calls);

        manager.add_boxed(Box::new(target));
        assert_eq!(manager.len(), 1);

        let removed = manager.remove_and_close("primary:webhook").await;
        assert!(removed.is_some());
        assert_eq!(manager.len(), 0);
        assert_eq!(close_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn runtime_manager_starts_all_target_closes_before_waiting_for_completion() {
        let mut manager = TargetRuntimeManager::<String>::new();
        let first = TestTarget::new("first", "webhook");
        let second = TestTarget::new("second", "webhook");
        let first_observer = first.clone();
        let second_observer = second.clone();
        manager.add_boxed(Box::new(first));
        manager.add_boxed(Box::new(second));

        let first_close_key = manager
            .keys()
            .into_iter()
            .next()
            .expect("two targets should have a first close key");
        let (blocked, unblocked) = if first_close_key == first_observer.id.to_string() {
            (first_observer, second_observer)
        } else {
            (second_observer, first_observer)
        };
        blocked.block_on_close.store(true, Ordering::SeqCst);

        let close_task = tokio::spawn(async move { manager.clear_and_close().await });

        tokio::time::timeout(std::time::Duration::from_secs(1), blocked.close_started.notified())
            .await
            .expect("the first target close should start");
        tokio::time::timeout(std::time::Duration::from_secs(1), unblocked.close_started.notified())
            .await
            .expect("a blocked first close must not prevent the second close from starting");
        assert!(!close_task.is_finished(), "clear_and_close must still await the blocked target");

        blocked.close_gate.add_permits(1);
        let errors = close_task.await.expect("clear_and_close task should join");
        assert!(errors.is_empty());
        assert_eq!(blocked.close_calls.load(Ordering::SeqCst), 1);
        assert_eq!(unblocked.close_calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn runtime_manager_snapshots_targets() {
        let mut manager = TargetRuntimeManager::<String>::new();
        manager.add_boxed(Box::new(TestTarget::new("primary", "webhook")));

        let snapshots = manager.snapshots();
        assert_eq!(snapshots.len(), 1);
        assert_eq!(snapshots[0].target_id, "primary:webhook");
        assert_eq!(snapshots[0].target_type, "webhook");
    }

    #[tokio::test(start_paused = true)]
    async fn health_snapshot_allows_a_four_second_probe() {
        let mut manager = TargetRuntimeManager::<String>::new();
        manager.add_boxed(Box::new(TestTarget::with_health_delay("slow", Duration::from_secs(4))));

        let snapshots = manager.health_snapshots().await;

        assert_eq!(snapshots.len(), 1);
        assert_eq!(snapshots[0].state, crate::TargetHealthState::Online);
        assert_eq!(snapshots[0].reason, crate::TargetHealthReason::Reachable);
    }

    #[tokio::test(start_paused = true)]
    async fn health_snapshot_times_out_after_five_seconds() {
        let mut manager = TargetRuntimeManager::<String>::new();
        manager.add_boxed(Box::new(TestTarget::with_health_delay("stalled", Duration::from_secs(6))));

        let snapshots = manager.health_snapshots().await;

        assert_eq!(snapshots.len(), 1);
        assert_eq!(snapshots[0].state, crate::TargetHealthState::Error);
        assert_eq!(snapshots[0].reason, crate::TargetHealthReason::TimedOut);
    }

    #[tokio::test(start_paused = true)]
    async fn health_collection_deadline_does_not_scale_with_target_count() {
        let mut manager = TargetRuntimeManager::<String>::new();
        for index in 0..24 {
            manager.add_boxed(Box::new(TestTarget::with_health_delay(
                &format!("stalled-{index}"),
                Duration::from_secs(30),
            )));
        }
        let started = tokio::time::Instant::now();

        let snapshots = manager.health_snapshots().await;

        assert_eq!(started.elapsed(), Duration::from_secs(5));
        assert_eq!(snapshots.len(), 24);
        assert!(
            snapshots
                .iter()
                .all(|snapshot| snapshot.reason == crate::TargetHealthReason::TimedOut)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn disabled_target_does_not_wait_for_probe_capacity() {
        let mut targets: Vec<SharedTarget<String>> = (0..HEALTH_PROBE_CONCURRENCY)
            .map(|index| {
                Arc::new(TestTarget::with_health_delay(&format!("stalled-{index}"), Duration::from_secs(30)))
                    as SharedTarget<String>
            })
            .collect();
        targets.push(Arc::new(TestTarget::disabled("disabled")));

        let snapshots = health_snapshots_for_targets(targets).await;
        let disabled = snapshots
            .iter()
            .find(|snapshot| snapshot.target_id == "disabled:webhook")
            .expect("disabled target snapshot");

        assert_eq!(disabled.state, crate::TargetHealthState::Disabled);
        assert_eq!(disabled.reason, crate::TargetHealthReason::Disabled);
    }

    #[tokio::test]
    async fn sleep_or_cancelled_returns_immediately_on_cancel() {
        let (cancel_tx, mut cancel_rx) = tokio::sync::mpsc::channel::<()>(1);
        cancel_tx.send(()).await.unwrap();

        // A pending cancel signal must short-circuit a long sleep.
        let start = std::time::Instant::now();
        let cancelled = super::sleep_or_cancelled(std::time::Duration::from_secs(30), &mut cancel_rx).await;
        assert!(cancelled);
        assert!(
            start.elapsed() < std::time::Duration::from_secs(5),
            "cancel should not wait for the full sleep"
        );
    }

    #[tokio::test]
    async fn sleep_or_cancelled_returns_false_when_not_cancelled() {
        let (_cancel_tx, mut cancel_rx) = tokio::sync::mpsc::channel::<()>(1);
        let cancelled = super::sleep_or_cancelled(std::time::Duration::from_millis(10), &mut cancel_rx).await;
        assert!(!cancelled);
    }

    #[tokio::test]
    async fn stop_all_joins_and_awaits_worker_exit() {
        use super::ReplayWorkerManager;
        use std::sync::atomic::AtomicBool;

        let mut manager = ReplayWorkerManager::new();
        let exited = Arc::new(AtomicBool::new(false));

        let (cancel_tx, mut cancel_rx) = tokio::sync::mpsc::channel::<()>(1);
        let exited_task = Arc::clone(&exited);
        let join = tokio::spawn(async move {
            // Run until cancelled, then record clean exit.
            loop {
                if super::sleep_or_cancelled(std::time::Duration::from_millis(50), &mut cancel_rx).await {
                    break;
                }
            }
            exited_task.store(true, Ordering::SeqCst);
        });

        manager.insert_with_handle("primary:webhook".to_string(), cancel_tx, join);
        assert_eq!(manager.len(), 1);

        // stop_all must signal AND await the worker: once it returns, the worker
        // has actually exited (no orphaned task).
        manager.stop_all("stopping test worker").await;

        assert!(manager.is_empty());
        assert!(exited.load(Ordering::SeqCst), "stop_all must await the worker to completion");
    }

    #[tokio::test(start_paused = true)]
    async fn stop_all_does_not_abort_delivery_awaiting_acknowledgement() {
        use super::ReplayWorkerManager;
        use std::sync::atomic::{AtomicBool, Ordering};
        use tokio::sync::Notify;

        let mut manager = ReplayWorkerManager::new();
        let acknowledgement = Arc::new(Notify::new());
        let worker_started = Arc::new(Notify::new());
        let exited = Arc::new(AtomicBool::new(false));
        let (cancel_tx, mut cancel_rx) = tokio::sync::mpsc::channel::<()>(1);
        let worker_acknowledgement = acknowledgement.clone();
        let worker_started_signal = worker_started.clone();
        let worker_exited = exited.clone();
        let join = tokio::spawn(async move {
            worker_started_signal.notify_one();
            let _ = cancel_rx.recv().await;
            // Model a protocol operation that has accepted the request but has
            // not returned its acknowledgement yet. Lifecycle must not abort
            // this future or the same durable entry can be sent twice.
            worker_acknowledgement.notified().await;
            worker_exited.store(true, Ordering::SeqCst);
        });
        manager.insert_with_handle("primary:webhook".to_string(), cancel_tx, join);
        worker_started.notified().await;

        let mut stop = Box::pin(manager.stop_all("stopping ack-pending test worker"));
        tokio::select! {
            biased;
            _ = &mut stop => panic!("stop_all returned before the pending acknowledgement"),
            _ = std::future::ready(()) => {}
        }
        tokio::time::advance(std::time::Duration::from_secs(60)).await;
        tokio::select! {
            biased;
            _ = &mut stop => panic!("stop_all aborted an acknowledgement-pending delivery"),
            _ = std::future::ready(()) => {}
        }

        acknowledgement.notify_one();
        stop.await;
        assert!(exited.load(Ordering::SeqCst));
        assert!(manager.is_empty());
    }

    mod classifier {
        use super::super::{ReplayEvent, stream_replay_worker};
        use crate::arn::TargetID;
        use crate::plugin::PluginEvent;
        use crate::store::{FailedEventStore, Key, QueueStore, Store};
        use crate::target::{EntityTarget, QueuedPayload, QueuedPayloadMeta};
        use crate::{StoreError, Target, TargetError};
        use async_trait::async_trait;
        use rustfs_s3_types::EventName;
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::time::Duration;
        use tokio::sync::{Semaphore, mpsc};

        /// Records each ReplayEvent class the worker emitted, so a test can assert no event class was
        /// silently skipped.
        #[derive(Default)]
        struct ReplayTally {
            delivered: AtomicUsize,
            retryable: AtomicUsize,
            dropped: AtomicUsize,
            permanent_failure: AtomicUsize,
            retry_exhausted: AtomicUsize,
            unreadable: AtomicUsize,
        }

        /// A target whose send_from_store returns a programmed error each call, so the replay
        /// classifier can be exercised per error class against a real on-disk store.
        #[derive(Clone)]
        struct ProgrammedTarget {
            id: TargetID,
            error: Option<Arc<TargetError>>,
            send_calls: Arc<AtomicUsize>,
            in_flight: Arc<AtomicUsize>,
            max_in_flight: Arc<AtomicUsize>,
            send_gate: Arc<Semaphore>,
            // A clone of the worker's store so the mock can delete on the Dropped or success path, as
            // the real send_from_store does, rather than leaving the entry to be re-listed forever.
            store: QueueStore<QueuedPayload>,
            // The failed-events handle the terminal move writes to. Defaults to the store itself and is
            // overridden with a rejecting handle to model an unwritable failed store.
            failed_handle: Arc<dyn FailedEventStore>,
        }

        impl ProgrammedTarget {
            fn new(error: Option<TargetError>, store: QueueStore<QueuedPayload>) -> Self {
                let failed_handle: Arc<dyn FailedEventStore> = Arc::new(store.clone());
                Self::new_with_failed_store(error, store, failed_handle)
            }

            fn new_with_failed_store(
                error: Option<TargetError>,
                store: QueueStore<QueuedPayload>,
                failed_handle: Arc<dyn FailedEventStore>,
            ) -> Self {
                let send_gate = Arc::new(Semaphore::new(Semaphore::MAX_PERMITS));
                Self {
                    id: TargetID::new("target-a".to_string(), "nats".to_string()),
                    error: error.map(Arc::new),
                    send_calls: Arc::new(AtomicUsize::new(0)),
                    in_flight: Arc::new(AtomicUsize::new(0)),
                    max_in_flight: Arc::new(AtomicUsize::new(0)),
                    send_gate,
                    store,
                    failed_handle,
                }
            }
        }

        /// Models the NATS target's terminal move for the classifier tests: reads the entry, encodes it
        /// as a terminal failed entry, writes it to the failed-events store, then clears the live entry.
        /// A failed write leaves the live entry in place for the next scan. The delete outcome mirrors
        /// the production move, so a delete failure returns false rather than reporting the entry handled.
        fn move_terminal_entry_for_test(
            store: &(dyn Store<QueuedPayload, Error = StoreError, Key = Key> + Send),
            failed_store: &dyn FailedEventStore,
            key: &Key,
            error: &TargetError,
            retry_count: u32,
        ) -> bool {
            let raw = match store.get_raw(key) {
                Ok(raw) => raw,
                Err(StoreError::NotFound) => return true,
                Err(_) => return false,
            };
            let Ok(queued) = QueuedPayload::decode(&raw) else {
                return false;
            };
            let resolved = crate::target::nats::resolve_dedup_id(&queued.meta.dedup_id, key);
            let Ok(encoded) = crate::target::encode_failed_entry(
                queued,
                crate::target::FailedErrorClass::Terminal,
                error,
                retry_count,
                &resolved,
            ) else {
                return false;
            };
            if failed_store.put_failed_raw(&key.name, &encoded).is_err() {
                return false;
            }
            // A missing entry is not a failure, any other delete error declines the move, matching the
            // production delete which treats NotFound as done and propagates every other error.
            matches!(store.del(key), Ok(()) | Err(StoreError::NotFound))
        }

        #[async_trait]
        impl<E> Target<E> for ProgrammedTarget
        where
            E: PluginEvent,
        {
            fn id(&self) -> TargetID {
                self.id.clone()
            }
            async fn is_active(&self) -> Result<bool, TargetError> {
                Ok(true)
            }
            async fn save(&self, _event: Arc<EntityTarget<E>>) -> Result<(), TargetError> {
                Ok(())
            }
            async fn send_raw_from_store(&self, _key: Key, _body: Vec<u8>, _meta: QueuedPayloadMeta) -> Result<(), TargetError> {
                Ok(())
            }
            async fn send_from_store(&self, key: Key) -> Result<(), TargetError> {
                self.send_calls.fetch_add(1, Ordering::SeqCst);
                let live = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                self.max_in_flight.fetch_max(live, Ordering::SeqCst);
                // The gate lets a test hold a send in flight to observe permit behaviour during the
                // await. It is open by default.
                let permit = self.send_gate.clone().acquire_owned().await;
                drop(permit);
                self.in_flight.fetch_sub(1, Ordering::SeqCst);
                match self.error.as_deref() {
                    // A Dropped error means send_from_store already removed the invalid entry before
                    // returning, so the mock deletes it too. A retryable or terminal publish error
                    // leaves the entry in place, matching the real path where the entry is cleared only
                    // on Ok or by the failed-store move.
                    Some(TargetError::Dropped(reason)) => {
                        let _ = self.store.del(&key);
                        Err(TargetError::Dropped(reason.clone()))
                    }
                    Some(error) => Err(clone_error(error)),
                    None => {
                        // Success clears the entry, as the real default implementation does.
                        let _ = self.store.del(&key);
                        Ok(())
                    }
                }
            }
            async fn close(&self) -> Result<(), TargetError> {
                Ok(())
            }
            fn store(&self) -> Option<&(dyn Store<QueuedPayload, Error = StoreError, Key = Key> + Send + Sync)> {
                None
            }
            async fn handle_terminal_failure(
                &self,
                store: &(dyn Store<QueuedPayload, Error = StoreError, Key = Key> + Send),
                key: &Key,
                error: &TargetError,
                retry_count: u32,
            ) -> bool {
                move_terminal_entry_for_test(store, self.failed_handle.as_ref(), key, error, retry_count)
            }
            fn clone_dyn(&self) -> Box<dyn Target<E> + Send + Sync> {
                Box::new(self.clone())
            }
            fn is_enabled(&self) -> bool {
                true
            }
        }

        /// A store that delegates every operation to a real on-disk QueueStore but fails
        /// put_failed_raw or del while the matching flag is set, modeling an unwritable failed-store
        /// directory or a live queue whose delete fails, either of which an operator later repairs.
        #[derive(Clone)]
        struct FailingFailedStore {
            inner: QueueStore<QueuedPayload>,
            fail_put_failed_raw: Arc<std::sync::atomic::AtomicBool>,
            fail_del: Arc<std::sync::atomic::AtomicBool>,
        }

        impl Store<QueuedPayload> for FailingFailedStore {
            type Error = StoreError;
            type Key = Key;

            fn open(&self) -> Result<(), Self::Error> {
                self.inner.open()
            }
            fn put(&self, item: Arc<QueuedPayload>) -> Result<Self::Key, Self::Error> {
                self.inner.put(item)
            }
            fn put_multiple(&self, items: Vec<QueuedPayload>) -> Result<Self::Key, Self::Error> {
                self.inner.put_multiple(items)
            }
            fn put_raw(&self, data: &[u8]) -> Result<Self::Key, Self::Error> {
                self.inner.put_raw(data)
            }
            fn get(&self, key: &Self::Key) -> Result<QueuedPayload, Self::Error> {
                self.inner.get(key)
            }
            fn get_multiple(&self, key: &Self::Key) -> Result<Vec<QueuedPayload>, Self::Error> {
                self.inner.get_multiple(key)
            }
            fn get_raw(&self, key: &Self::Key) -> Result<Vec<u8>, Self::Error> {
                self.inner.get_raw(key)
            }
            fn del(&self, key: &Self::Key) -> Result<(), Self::Error> {
                if self.fail_del.load(Ordering::SeqCst) {
                    return Err(StoreError::Internal("injected del failure".to_string()));
                }
                self.inner.del(key)
            }
            fn delete(&self) -> Result<(), Self::Error> {
                self.inner.delete()
            }
            fn list(&self) -> Vec<Self::Key> {
                self.inner.list()
            }
            fn len(&self) -> usize {
                self.inner.len()
            }
            fn is_empty(&self) -> bool {
                self.inner.is_empty()
            }
            fn boxed_clone(&self) -> Box<dyn Store<QueuedPayload, Error = Self::Error, Key = Self::Key> + Send + Sync> {
                Box::new(self.clone())
            }
        }

        impl FailedEventStore for FailingFailedStore {
            fn put_failed_raw(&self, entry_name: &str, data: &[u8]) -> Result<String, StoreError> {
                if self.fail_put_failed_raw.load(Ordering::SeqCst) {
                    return Err(StoreError::Internal("injected put_failed_raw failure".to_string()));
                }
                self.inner.put_failed_raw(entry_name, data)
            }
            fn prune_failed_store(&self) -> Result<usize, StoreError> {
                self.inner.prune_failed_store()
            }
            fn failed_len(&self) -> usize {
                self.inner.failed_len()
            }
            fn boxed_clone_failed(&self) -> Box<dyn FailedEventStore> {
                Box::new(self.clone())
            }
        }

        fn clone_error(error: &TargetError) -> TargetError {
            match error {
                TargetError::NotConnected => TargetError::NotConnected,
                TargetError::Timeout(value) => TargetError::Timeout(value.clone()),
                TargetError::Dropped(value) => TargetError::Dropped(value.clone()),
                TargetError::JetStreamPublish { retryable, detail } => TargetError::JetStreamPublish {
                    retryable: *retryable,
                    detail: detail.clone(),
                },
                TargetError::Network(value) => TargetError::Network(value.clone()),
                other => TargetError::Unknown(other.to_string()),
            }
        }

        fn temp_dir(name: &str) -> std::path::PathBuf {
            std::env::temp_dir().join(format!("rustfs-runtime-{name}-{}", uuid::Uuid::new_v4()))
        }

        fn seed_entry(store: &QueueStore<QueuedPayload>, dedup_id: &str) -> Key {
            let mut meta = QueuedPayloadMeta::new(
                EventName::ObjectCreatedPut,
                "bucket-a".to_string(),
                "obj.txt".to_string(),
                "application/json",
                7,
            );
            meta.dedup_id = dedup_id.to_string();
            let payload = QueuedPayload::new(meta, br#"{"x":1}"#.to_vec());
            store.put_raw(&payload.encode().unwrap()).unwrap()
        }

        /// Runs the replay worker against the seeded store for up to 600 virtual seconds, then cancels.
        ///
        /// The worker runs on a store clone (the QueueStore shares its state through an Arc), so the
        /// caller's handle observes drain while the worker holds its own mutable borrow. Tests run with
        /// a paused clock, so the production backoff sleeps cost no real time and the loop drains fast.
        async fn run_worker(store: &mut QueueStore<QueuedPayload>, target: Arc<ProgrammedTarget>, tally: Arc<ReplayTally>) {
            run_worker_until(store, target, tally, |_| false).await;
        }

        /// Runs the replay worker like run_worker, polling the tally once per virtual second and
        /// cancelling as soon as the stop condition holds, so a test over an entry that stays live
        /// can observe exactly one retry cycle instead of every cycle the full window admits.
        async fn run_worker_until(
            store: &mut QueueStore<QueuedPayload>,
            target: Arc<ProgrammedTarget>,
            tally: Arc<ReplayTally>,
            stop: impl Fn(&ReplayTally) -> bool,
        ) {
            let shared: Arc<dyn Target<String> + Send + Sync> = target.clone();
            let (cancel_tx, cancel_rx) = mpsc::channel(1);
            let hook = {
                let tally = tally.clone();
                Arc::new(move |event: ReplayEvent<String>| {
                    let tally = tally.clone();
                    Box::pin(async move {
                        match event {
                            ReplayEvent::Delivered { .. } => tally.delivered.fetch_add(1, Ordering::SeqCst),
                            ReplayEvent::RetryableError { .. } => tally.retryable.fetch_add(1, Ordering::SeqCst),
                            ReplayEvent::Dropped { .. } => tally.dropped.fetch_add(1, Ordering::SeqCst),
                            ReplayEvent::PermanentFailure { .. } => tally.permanent_failure.fetch_add(1, Ordering::SeqCst),
                            ReplayEvent::RetryExhausted { .. } => tally.retry_exhausted.fetch_add(1, Ordering::SeqCst),
                            ReplayEvent::UnreadableEntry { .. } => tally.unreadable.fetch_add(1, Ordering::SeqCst),
                        };
                    }) as std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>
                }) as super::super::ReplayHook<String>
            };

            let semaphore = Some(Arc::new(Semaphore::new(1)));
            let mut worker_store = store.clone();
            let worker = tokio::spawn(async move {
                stream_replay_worker(
                    &mut worker_store,
                    shared,
                    cancel_rx,
                    hook,
                    semaphore,
                    Duration::from_millis(10),
                    Duration::from_millis(10),
                )
                .await;
            });

            // The paused clock makes the production backoff sleeps free, so the virtual window lets
            // the worker fully process the seeded entry (a full retry exhaustion is about two
            // virtual minutes) before the worker is cancelled. The once-per-virtual-second poll
            // cancels within one second of the stop condition, well inside a retry cycle.
            for _ in 0..600 {
                if stop(&tally) {
                    break;
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            let _ = cancel_tx.send(()).await;
            let _ = worker.await;
        }

        // A retryable JetStream publish error retries within the bound and, on exhaustion, leaves the
        // entry on the live queue rather than moving it to the failed store. Covers TimedOut,
        // BrokenPipe, MaxAckPending, and the bounded StreamNotFound path, all of which the classifier
        // carries as a retryable publish error.
        #[tokio::test(start_paused = true)]
        async fn retryable_publish_error_retries_and_keeps_the_entry_live() {
            for detail in ["timed out", "broken pipe", "max ack pending", "stream not found"] {
                let dir = temp_dir(&format!("retryable-{}", detail.replace(' ', "-")));
                let mut store = QueueStore::<QueuedPayload>::new_with_compression(&dir, 64, ".test", false);
                store.open().unwrap();
                seed_entry(&store, "minted-id");

                let target = Arc::new(ProgrammedTarget::new(
                    Some(TargetError::JetStreamPublish {
                        retryable: true,
                        detail: detail.to_string(),
                    }),
                    store.clone(),
                ));
                let tally = Arc::new(ReplayTally::default());
                // The entry stays live and is rescanned after exhaustion, so the run stops at the
                // first exhaustion to observe one full retry cycle.
                run_worker_until(&mut store, target.clone(), tally.clone(), |t| {
                    t.retry_exhausted.load(Ordering::SeqCst) >= 1
                })
                .await;

                assert!(tally.retryable.load(Ordering::SeqCst) >= 1, "{detail} retries at least once");
                assert_eq!(tally.retry_exhausted.load(Ordering::SeqCst), 1, "{detail} exhausts the bound once");
                assert_eq!(store.len(), 1, "{detail} keeps the entry on the live queue");
                assert_eq!(store.failed_len(), 0, "{detail} writes no failed entry");
                assert!(!dir.join("failed").exists(), "{detail} creates no failed directory");

                let _ = store.delete();
            }
        }

        // A terminal JetStream publish error writes a terminal failed entry on the first attempt with
        // no retry, and the live entry is cleared. Covers MaxPayloadExceeded, WrongLastMessageId,
        // WrongLastSequence, and the terminal Other code, all carried as a non-retryable publish error.
        #[tokio::test(start_paused = true)]
        async fn terminal_publish_error_writes_terminal_entry_without_retry() {
            for detail in [
                "max payload exceeded",
                "wrong last message id",
                "wrong last sequence",
                "stream sealed",
            ] {
                let dir = temp_dir(&format!("terminal-{}", detail.replace(' ', "-")));
                let mut store = QueueStore::<QueuedPayload>::new_with_compression(&dir, 64, ".test", false);
                store.open().unwrap();
                seed_entry(&store, "minted-id");

                let target = Arc::new(ProgrammedTarget::new(
                    Some(TargetError::JetStreamPublish {
                        retryable: false,
                        detail: detail.to_string(),
                    }),
                    store.clone(),
                ));
                let tally = Arc::new(ReplayTally::default());
                run_worker(&mut store, target.clone(), tally.clone()).await;

                assert_eq!(tally.permanent_failure.load(Ordering::SeqCst), 1, "{detail} fails permanently once");
                assert_eq!(tally.retryable.load(Ordering::SeqCst), 0, "{detail} does not retry");
                assert_eq!(tally.retry_exhausted.load(Ordering::SeqCst), 0, "{detail} does not reach exhaustion");
                assert_eq!(store.len(), 0, "{detail} clears the live entry");
                assert_eq!(store.failed_len(), 1, "{detail} writes one terminal failed entry");

                // The failed entry carries the terminal class and the full meta.
                let failed_dir = dir.join("failed");
                let failed_file = std::fs::read_dir(&failed_dir).unwrap().next().unwrap().unwrap().path();
                let decoded = QueuedPayload::decode(&std::fs::read(&failed_file).unwrap()).unwrap();
                let failure = decoded.meta.failure.unwrap();
                assert_eq!(failure.error_class, crate::target::FailedErrorClass::Terminal);
                assert_eq!(failure.nats_msg_id, "minted-id");
                assert_eq!(decoded.meta.bucket_name, "bucket-a");

                let _ = store.delete();
            }
        }

        // A Dropped entry is reported through the drop hook rather than silently discarded, and it
        // writes no failed entry.
        #[tokio::test(start_paused = true)]
        async fn no_replay_path_silently_drops_an_event() {
            // A Dropped error is recorded through the drop hook and writes no failed entry, because a
            // dropped payload is an invalid entry the store itself removed, not a deliverable event.
            let dir = temp_dir("nodrop-dropped");
            let mut store = QueueStore::<QueuedPayload>::new_with_compression(&dir, 64, ".test", false);
            store.open().unwrap();
            seed_entry(&store, "id");
            let target = Arc::new(ProgrammedTarget::new(
                Some(TargetError::Dropped("invalid entry".to_string())),
                store.clone(),
            ));
            let tally = Arc::new(ReplayTally::default());
            run_worker(&mut store, target.clone(), tally.clone()).await;
            assert_eq!(tally.dropped.load(Ordering::SeqCst), 1, "a dropped entry is reported, not silent");
            assert_eq!(store.failed_len(), 0, "a dropped invalid entry writes no failed entry");

            let _ = store.delete();
        }

        // The audit path and the notify path monomorphize the same generic replay worker, so the
        // classifier and the failed-store write run identically for an audit entry type. Driving the
        // worker over a non-String event type exercises the audit monomorphization.
        #[tokio::test(start_paused = true)]
        async fn audit_event_type_runs_the_identical_classifier_and_failed_store_logic() {
            let dir = temp_dir("audit-parity");
            let mut store = QueueStore::<QueuedPayload>::new_with_compression(&dir, 64, ".test", false);
            store.open().unwrap();
            seed_entry(&store, "audit-id");

            let shared: Arc<dyn Target<u64> + Send + Sync> = Arc::new(ProgrammedTarget::new(
                Some(TargetError::JetStreamPublish {
                    retryable: false,
                    detail: "max payload exceeded".to_string(),
                }),
                store.clone(),
            ));
            let (cancel_tx, cancel_rx) = mpsc::channel(1);
            let hook = Arc::new(move |_event: ReplayEvent<u64>| {
                Box::pin(async move {}) as std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>
            }) as super::super::ReplayHook<u64>;
            let semaphore = Some(Arc::new(Semaphore::new(1)));
            let worker = stream_replay_worker(
                &mut store,
                shared,
                cancel_rx,
                hook,
                semaphore,
                Duration::from_millis(10),
                Duration::from_millis(10),
            );
            let driver = async move {
                tokio::time::sleep(Duration::from_secs(5)).await;
                let _ = cancel_tx.send(()).await;
            };
            tokio::join!(worker, driver);

            assert_eq!(store.len(), 0, "the audit live entry is cleared after the move");
            assert_eq!(store.failed_len(), 1, "the audit path writes a failed entry through the shared worker");

            let _ = store.delete();
        }

        // The replay admission permit is held across the send await, so under a single-permit
        // semaphore only one worker enters its send at a time. Two workers share a single-permit
        // semaphore and a closed gate that holds every send in flight. The first worker admits and
        // enters its gated send. The second worker blocks on the permit and never enters its send, so
        // at most one send is in flight. The permit is still released before the backoff sleep, so a
        // send that fails does not hold the shared permit across the backoff.
        #[tokio::test]
        async fn admission_permit_is_held_across_the_send_bounding_concurrent_sends() {
            // A shared gate held closed keeps every send blocked inside its await, and a shared
            // in-flight counter observes how many sends are concurrently in their await.
            let shared_gate = Arc::new(Semaphore::new(Semaphore::MAX_PERMITS));
            shared_gate.forget_permits(Semaphore::MAX_PERMITS);
            let in_flight = Arc::new(AtomicUsize::new(0));
            let max_in_flight = Arc::new(AtomicUsize::new(0));
            let semaphore = Some(Arc::new(Semaphore::new(1)));

            let mut stores = Vec::new();
            let mut workers = Vec::new();
            let mut cancels = Vec::new();
            for index in 0..2 {
                let dir = temp_dir(&format!("permit-{index}"));
                let store = QueueStore::<QueuedPayload>::new_with_compression(&dir, 64, ".test", false);
                store.open().unwrap();
                seed_entry(&store, &format!("id-{index}"));

                let mut target = ProgrammedTarget::new(
                    Some(TargetError::JetStreamPublish {
                        retryable: true,
                        detail: "timed out".to_string(),
                    }),
                    store.clone(),
                );
                target.send_gate = shared_gate.clone();
                target.in_flight = in_flight.clone();
                target.max_in_flight = max_in_flight.clone();
                let shared: Arc<dyn Target<String> + Send + Sync> = Arc::new(target);

                let (cancel_tx, cancel_rx) = mpsc::channel(1);
                let hook = Arc::new(move |_event: ReplayEvent<String>| {
                    Box::pin(async move {}) as std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>
                }) as super::super::ReplayHook<String>;
                let semaphore = semaphore.clone();
                let mut worker_store = store.clone();
                let worker = tokio::spawn(async move {
                    stream_replay_worker(
                        &mut worker_store,
                        shared,
                        cancel_rx,
                        hook,
                        semaphore,
                        Duration::from_millis(10),
                        Duration::from_millis(10),
                    )
                    .await;
                });
                stores.push(store);
                workers.push(worker);
                cancels.push(cancel_tx);
            }

            // Wait until the first worker has admitted and entered its gated send.
            for _ in 0..200 {
                if in_flight.load(Ordering::SeqCst) >= 1 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            // Give the second worker ample time to try to admit. It must block on the held permit and
            // never enter its send.
            tokio::time::sleep(Duration::from_millis(100)).await;

            assert_eq!(
                max_in_flight.load(Ordering::SeqCst),
                1,
                "the single permit is held across the send, so only one send is in flight at a time"
            );

            // The observation is complete. Abort the workers rather than draining them, so the test
            // does not wait out the production backoff after the gated sends resume.
            drop(cancels);
            for worker in workers {
                worker.abort();
                let _ = worker.await;
            }
            for store in stores {
                let _ = store.delete();
            }
        }

        // With the flag off the NATS target never produces a JetStream publish error, so the worker
        // never writes the failed store. A non-JetStream connectivity error retries and exhausts
        // through the existing path without creating the failed directory.
        #[tokio::test(start_paused = true)]
        async fn flag_off_path_creates_no_failed_directory() {
            let dir = temp_dir("flag-off");
            let mut store = QueueStore::<QueuedPayload>::new_with_compression(&dir, 64, ".test", false);
            store.open().unwrap();
            seed_entry(&store, "");

            // A flag-off target surfaces connectivity errors, not JetStreamPublish, so the failed-store
            // move is never triggered.
            let target = Arc::new(ProgrammedTarget::new(Some(TargetError::NotConnected), store.clone()));
            let tally = Arc::new(ReplayTally::default());
            run_worker(&mut store, target.clone(), tally.clone()).await;

            assert!(
                tally.retry_exhausted.load(Ordering::SeqCst) >= 1,
                "the connectivity error exhausts its retries"
            );
            assert!(!dir.join("failed").exists(), "no failed directory is created on the flag-off path");
            assert_eq!(store.failed_len(), 0);

            let _ = store.delete();
        }

        // A connect-level failure on a JetStream target surfaces as NotConnected, mapped at the
        // publish path. The worker retries it with backoff and leaves the entry live at exhaustion,
        // so a broker outage writes no failed-store entry and the event delivers when the connection
        // recovers, matching the Core path.
        #[tokio::test(start_paused = true)]
        async fn jetstream_connect_failure_retries_and_keeps_the_entry_live() {
            let dir = temp_dir("connect-failure");
            let mut store = QueueStore::<QueuedPayload>::new_with_compression(&dir, 64, ".test", false);
            store.open().unwrap();
            seed_entry(&store, "minted-id");

            let target = Arc::new(ProgrammedTarget::new(Some(TargetError::NotConnected), store.clone()));
            let tally = Arc::new(ReplayTally::default());
            // The live entry is rescanned after exhaustion, so the run stops at the first
            // exhaustion to observe one full retry cycle.
            run_worker_until(&mut store, target.clone(), tally.clone(), |t| {
                t.retry_exhausted.load(Ordering::SeqCst) >= 1
            })
            .await;

            assert!(
                tally.retryable.load(Ordering::SeqCst) >= 1,
                "the connect failure retries within the bound"
            );
            assert_eq!(tally.retry_exhausted.load(Ordering::SeqCst), 1, "the exhaustion hook fires exactly once");
            assert_eq!(store.len(), 1, "the entry stays live through the outage");
            assert_eq!(store.failed_len(), 0, "an outage writes no failed entry");
            assert!(!dir.join("failed").exists(), "no failed-store write occurs for a connect failure");

            let _ = store.delete();
        }

        /// Spawns the replay worker over a FailingFailedStore and returns the cancel channel and
        /// join handle, so a test can advance virtual time, repair the failed store mid-run, and
        /// observe the hook tally across the repair.
        fn spawn_failing_store_worker(
            store: &FailingFailedStore,
            target: Arc<ProgrammedTarget>,
            tally: Arc<ReplayTally>,
        ) -> (mpsc::Sender<()>, tokio::task::JoinHandle<()>) {
            let shared: Arc<dyn Target<String> + Send + Sync> = target;
            let (cancel_tx, cancel_rx) = mpsc::channel(1);
            let hook = Arc::new(move |event: ReplayEvent<String>| {
                let tally = tally.clone();
                Box::pin(async move {
                    match event {
                        ReplayEvent::Delivered { .. } => tally.delivered.fetch_add(1, Ordering::SeqCst),
                        ReplayEvent::RetryableError { .. } => tally.retryable.fetch_add(1, Ordering::SeqCst),
                        ReplayEvent::Dropped { .. } => tally.dropped.fetch_add(1, Ordering::SeqCst),
                        ReplayEvent::PermanentFailure { .. } => tally.permanent_failure.fetch_add(1, Ordering::SeqCst),
                        ReplayEvent::RetryExhausted { .. } => tally.retry_exhausted.fetch_add(1, Ordering::SeqCst),
                        ReplayEvent::UnreadableEntry { .. } => tally.unreadable.fetch_add(1, Ordering::SeqCst),
                    };
                }) as std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>
            }) as super::super::ReplayHook<String>;
            let semaphore = Some(Arc::new(Semaphore::new(1)));
            let mut worker_store = store.clone();
            let worker = tokio::spawn(async move {
                stream_replay_worker(
                    &mut worker_store,
                    shared,
                    cancel_rx,
                    hook,
                    semaphore,
                    Duration::from_millis(10),
                    Duration::from_millis(10),
                )
                .await;
            });
            (cancel_tx, worker)
        }

        // A terminal failure whose failed-store move fails must not count as a final failure. No
        // PermanentFailure hook fires, no failed entry lands, and the live entry stays for the next
        // scan. Once the failed store is repaired, the move completes and the hook fires exactly
        // once, so one lost event counts once rather than zero or once per scan.
        #[tokio::test(start_paused = true)]
        async fn a_failed_terminal_move_emits_no_hook_and_heals_once_repaired() {
            let dir = temp_dir("move-fail-terminal");
            let inner = QueueStore::<QueuedPayload>::new_with_compression(&dir, 64, ".test", false);
            inner.open().unwrap();
            seed_entry(&inner, "minted-id");

            let fail_flag = Arc::new(std::sync::atomic::AtomicBool::new(true));
            let store = FailingFailedStore {
                inner: inner.clone(),
                fail_put_failed_raw: fail_flag.clone(),
                fail_del: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            };
            // The target's terminal move writes through the rejecting failed handle, so the injected
            // failure is what the move hits.
            let target = Arc::new(ProgrammedTarget::new_with_failed_store(
                Some(TargetError::JetStreamPublish {
                    retryable: false,
                    detail: "max payload exceeded".to_string(),
                }),
                inner.clone(),
                Arc::new(store.clone()),
            ));
            let tally = Arc::new(ReplayTally::default());
            let (cancel_tx, worker) = spawn_failing_store_worker(&store, target, tally.clone());

            // Many scans run while the failed store rejects the move.
            tokio::time::sleep(Duration::from_secs(300)).await;
            assert_eq!(
                tally.permanent_failure.load(Ordering::SeqCst),
                0,
                "no final-failure hook fires while the move keeps failing"
            );
            assert_eq!(inner.len(), 1, "the live entry stays for the next scan");
            assert_eq!(inner.failed_len(), 0, "no failed entry landed");

            // The operator repairs the failed store. The next scan completes the move.
            fail_flag.store(false, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_secs(300)).await;
            assert_eq!(
                tally.permanent_failure.load(Ordering::SeqCst),
                1,
                "the hook fires exactly once for the completed move"
            );
            assert_eq!(inner.len(), 0, "the live entry is cleared by the completed move");
            assert_eq!(inner.failed_len(), 1, "the failed entry landed once");

            let _ = cancel_tx.send(()).await;
            let _ = worker.await;
            let _ = inner.delete();
        }

        // A terminal move whose failed-record write succeeds but whose live delete fails is not
        // handled: no final-failure hook fires and the live entry stays for the next scan, while the
        // durable failed copy is already in place and stays single through the idempotent overwrite.
        #[tokio::test(start_paused = true)]
        async fn a_move_with_a_failing_delete_reports_unhandled_and_keeps_the_entry() {
            let dir = temp_dir("move-del-fail");
            let inner = QueueStore::<QueuedPayload>::new_with_compression(&dir, 64, ".test", false);
            inner.open().unwrap();
            seed_entry(&inner, "minted-id");

            // The worker's live store rejects deletes while the failed handle (the plain inner store)
            // accepts the failed-record write, so only the delete half of the move fails.
            let store = FailingFailedStore {
                inner: inner.clone(),
                fail_put_failed_raw: Arc::new(std::sync::atomic::AtomicBool::new(false)),
                fail_del: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            };
            let target = Arc::new(ProgrammedTarget::new(
                Some(TargetError::JetStreamPublish {
                    retryable: false,
                    detail: "max payload exceeded".to_string(),
                }),
                inner.clone(),
            ));
            let tally = Arc::new(ReplayTally::default());
            let (cancel_tx, worker) = spawn_failing_store_worker(&store, target, tally.clone());

            // Many scans run while the live delete keeps failing.
            tokio::time::sleep(Duration::from_secs(300)).await;
            assert_eq!(
                tally.permanent_failure.load(Ordering::SeqCst),
                0,
                "no final-failure hook fires while the delete fails"
            );
            assert_eq!(inner.len(), 1, "the live entry stays for the next scan");
            assert_eq!(inner.failed_len(), 1, "the durable failed copy landed exactly once");

            let _ = cancel_tx.send(()).await;
            let _ = worker.await;
            let _ = inner.delete();
        }

        // The fifth failure is the last attempt, so no backoff follows it. The exhaustion is reported
        // after the four inter-attempt backoffs, 2s * (2 + 4 + 8 + 16) = 60s plus sub-second jitter,
        // well inside the 100s bound asserted here. A trailing backoff after the final failure would
        // add 64s more and push the report past the bound, failing the assertion.
        #[tokio::test(start_paused = true)]
        async fn exhaustion_reports_without_a_trailing_backoff() {
            let dir = temp_dir("no-trailing-backoff");
            let store = QueueStore::<QueuedPayload>::new_with_compression(&dir, 64, ".test", false);
            store.open().unwrap();
            seed_entry(&store, "minted-id");

            // The delegating wrapper with both failure flags off behaves as the plain on-disk store.
            let wrapper = FailingFailedStore {
                inner: store.clone(),
                fail_put_failed_raw: Arc::new(std::sync::atomic::AtomicBool::new(false)),
                fail_del: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            };
            let target = Arc::new(ProgrammedTarget::new(
                Some(TargetError::JetStreamPublish {
                    retryable: true,
                    detail: "timed out".to_string(),
                }),
                store.clone(),
            ));
            let tally = Arc::new(ReplayTally::default());

            let start = tokio::time::Instant::now();
            let (cancel_tx, worker) = spawn_failing_store_worker(&wrapper, target, tally.clone());

            // Poll the virtual clock until the first exhaustion is reported.
            while tally.retry_exhausted.load(Ordering::SeqCst) == 0 {
                assert!(start.elapsed() < Duration::from_secs(600), "the exhaustion never reported");
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            let elapsed = start.elapsed();
            assert!(
                elapsed < Duration::from_secs(100),
                "the exhaustion reports after the inter-attempt backoffs only, got {elapsed:?}"
            );
            assert_eq!(store.len(), 1, "the entry stays on the live queue");
            assert_eq!(store.failed_len(), 0, "a retryable exhaustion writes no failed entry");

            let _ = cancel_tx.send(()).await;
            let _ = worker.await;
            let _ = store.delete();
        }

        // A raw Network error models a non-JetStream core target, whose connect failure keeps the core
        // handling: a permanent failure left in the live queue, never diverted into the JetStream
        // retryable path or the failed store.
        #[tokio::test(start_paused = true)]
        async fn core_network_error_is_not_diverted_to_the_failed_store() {
            let dir = temp_dir("core-network");
            let mut store = QueueStore::<QueuedPayload>::new_with_compression(&dir, 64, ".test", false);
            store.open().unwrap();
            seed_entry(&store, "");

            let target = Arc::new(ProgrammedTarget::new(
                Some(TargetError::Network("connection refused".to_string())),
                store.clone(),
            ));
            let tally = Arc::new(ReplayTally::default());
            run_worker(&mut store, target.clone(), tally.clone()).await;

            assert!(
                tally.permanent_failure.load(Ordering::SeqCst) >= 1,
                "a core Network error is reported permanent, not silently dropped"
            );
            assert_eq!(tally.retryable.load(Ordering::SeqCst), 0, "a core Network error is not retried");
            assert_eq!(
                tally.retry_exhausted.load(Ordering::SeqCst),
                0,
                "a core Network error does not reach exhaustion"
            );
            assert_eq!(store.failed_len(), 0, "a core Network error writes no failed entry");
            assert_eq!(store.len(), 1, "a core Network error is left queued, its existing behaviour");
            assert!(!dir.join("failed").exists(), "no failed directory is created for a core Network error");

            let _ = store.delete();
        }

        // The single failed-store invariant: only a non-retryable error ever produces a failed-store
        // entry. One retryable entry driven past the budget stays on the live queue while one terminal
        // entry moves, so after both are processed the failed store holds exactly the terminal entry.
        #[tokio::test(start_paused = true)]
        async fn only_a_non_retryable_error_produces_a_failed_store_entry() {
            // A target that reads each entry and returns a terminal error for the entry whose dedup id
            // marks it terminal, and a retryable error for every other entry.
            #[derive(Clone)]
            struct ClassifyingTarget {
                id: TargetID,
                store: QueueStore<QueuedPayload>,
            }
            #[async_trait]
            impl Target<String> for ClassifyingTarget {
                fn id(&self) -> TargetID {
                    self.id.clone()
                }
                async fn is_active(&self) -> Result<bool, TargetError> {
                    Ok(true)
                }
                async fn save(&self, _event: Arc<EntityTarget<String>>) -> Result<(), TargetError> {
                    Ok(())
                }
                async fn send_raw_from_store(
                    &self,
                    _key: Key,
                    _body: Vec<u8>,
                    _meta: QueuedPayloadMeta,
                ) -> Result<(), TargetError> {
                    Ok(())
                }
                async fn send_from_store(&self, key: Key) -> Result<(), TargetError> {
                    let raw = self
                        .store
                        .get_raw(&key)
                        .map_err(|err| TargetError::Unknown(err.to_string()))?;
                    let decoded = QueuedPayload::decode(&raw).map_err(|err| TargetError::Unknown(err.to_string()))?;
                    if decoded.meta.dedup_id == "terminal-id" {
                        Err(TargetError::JetStreamPublish {
                            retryable: false,
                            detail: "max payload exceeded".to_string(),
                        })
                    } else {
                        Err(TargetError::JetStreamPublish {
                            retryable: true,
                            detail: "timed out".to_string(),
                        })
                    }
                }
                async fn close(&self) -> Result<(), TargetError> {
                    Ok(())
                }
                fn store(&self) -> Option<&(dyn Store<QueuedPayload, Error = StoreError, Key = Key> + Send + Sync)> {
                    None
                }
                async fn handle_terminal_failure(
                    &self,
                    store: &(dyn Store<QueuedPayload, Error = StoreError, Key = Key> + Send),
                    key: &Key,
                    error: &TargetError,
                    retry_count: u32,
                ) -> bool {
                    move_terminal_entry_for_test(store, &self.store, key, error, retry_count)
                }
                fn clone_dyn(&self) -> Box<dyn Target<String> + Send + Sync> {
                    Box::new(self.clone())
                }
                fn is_enabled(&self) -> bool {
                    true
                }
            }

            let dir = temp_dir("terminals-only-invariant");
            let store = QueueStore::<QueuedPayload>::new_with_compression(&dir, 64, ".test", false);
            store.open().unwrap();
            seed_entry(&store, "retryable-id");
            seed_entry(&store, "terminal-id");

            let shared: Arc<dyn Target<String> + Send + Sync> = Arc::new(ClassifyingTarget {
                id: TargetID::new("target-a".to_string(), "nats".to_string()),
                store: store.clone(),
            });
            let tally = Arc::new(ReplayTally::default());
            let (cancel_tx, cancel_rx) = mpsc::channel(1);
            let hook = {
                let tally = tally.clone();
                Arc::new(move |event: ReplayEvent<String>| {
                    let tally = tally.clone();
                    Box::pin(async move {
                        match event {
                            ReplayEvent::Delivered { .. } => tally.delivered.fetch_add(1, Ordering::SeqCst),
                            ReplayEvent::RetryableError { .. } => tally.retryable.fetch_add(1, Ordering::SeqCst),
                            ReplayEvent::Dropped { .. } => tally.dropped.fetch_add(1, Ordering::SeqCst),
                            ReplayEvent::PermanentFailure { .. } => tally.permanent_failure.fetch_add(1, Ordering::SeqCst),
                            ReplayEvent::RetryExhausted { .. } => tally.retry_exhausted.fetch_add(1, Ordering::SeqCst),
                            ReplayEvent::UnreadableEntry { .. } => tally.unreadable.fetch_add(1, Ordering::SeqCst),
                        };
                    }) as std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>
                }) as super::super::ReplayHook<String>
            };
            let semaphore = Some(Arc::new(Semaphore::new(1)));
            let mut worker_store = store.clone();
            let worker = tokio::spawn(async move {
                stream_replay_worker(
                    &mut worker_store,
                    shared,
                    cancel_rx,
                    hook,
                    semaphore,
                    Duration::from_millis(10),
                    Duration::from_millis(10),
                )
                .await;
            });

            // Drive until the terminal entry has moved and the retryable entry has exhausted at least
            // once. Both share one batch, so one pass processes each.
            for _ in 0..600 {
                if store.failed_len() >= 1 && tally.retry_exhausted.load(Ordering::SeqCst) >= 1 {
                    break;
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            let _ = cancel_tx.send(()).await;
            let _ = worker.await;

            assert_eq!(store.failed_len(), 1, "the failed store holds exactly one entry");
            assert_eq!(store.len(), 1, "the retryable entry stays on the live queue");

            // The one failed entry is the terminal one.
            let failed_dir = dir.join("failed");
            let failed_file = std::fs::read_dir(&failed_dir).unwrap().next().unwrap().unwrap().path();
            let decoded = QueuedPayload::decode(&std::fs::read(&failed_file).unwrap()).unwrap();
            let failure = decoded.meta.failure.unwrap();
            assert_eq!(failure.error_class, crate::target::FailedErrorClass::Terminal);
            assert_eq!(failure.nats_msg_id, "terminal-id");

            let _ = store.delete();
        }
    }
}
