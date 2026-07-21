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

use crate::error::NotificationError;
use crate::{event::Event, integration::NotificationMetrics, rule_engine::NotifyRuleEngine};
use rustfs_config::notify::{DEFAULT_NOTIFY_SEND_CONCURRENCY, ENV_NOTIFY_SEND_CONCURRENCY};
use rustfs_targets::Target;
use rustfs_targets::arn::TargetID;
use rustfs_targets::target::EntityTarget;
use rustfs_targets::{SharedTarget, TargetRuntimeManager};
use std::collections::HashMap;
use std::sync::{Arc, LazyLock, Mutex as StdMutex, Weak};
use tokio::sync::{RwLock, Semaphore, watch};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, instrument, warn};

const LOG_COMPONENT_NOTIFY: &str = "notify";
const LOG_SUBSYSTEM_DISPATCH: &str = "dispatch";
const EVENT_NOTIFY_DISPATCH_SKIPPED: &str = "notify_dispatch_skipped";
const EVENT_NOTIFY_DISPATCH_FAILED: &str = "notify_dispatch_failed";
const EVENT_NOTIFY_DISPATCH_STARTED: &str = "notify_dispatch_started";
const EVENT_NOTIFY_DISPATCH_COMPLETED: &str = "notify_dispatch_completed";
const EVENT_NOTIFY_RUNTIME_LIFECYCLE: &str = "notify_runtime_lifecycle";

async fn wait_for_dispatch_tasks(handles: Vec<JoinHandle<()>>) {
    for handle in handles {
        if let Err(e) = handle.await {
            let reason = if e.is_cancelled() { "join_cancelled" } else { "join_panicked" };
            error!(
                event = EVENT_NOTIFY_DISPATCH_FAILED,
                component = LOG_COMPONENT_NOTIFY,
                subsystem = LOG_SUBSYSTEM_DISPATCH,
                reason,
                "Notify dispatch task failed"
            );
        }
    }
}

pub type SharedNotifyTargetList = Arc<RwLock<TargetList>>;

static TARGET_LIST_DISPATCH_GATES: LazyLock<StdMutex<HashMap<usize, Weak<RwLock<()>>>>> =
    LazyLock::new(|| StdMutex::new(HashMap::new()));

pub(crate) fn shared_dispatch_gate(target_list: &SharedNotifyTargetList, preferred: Option<Arc<RwLock<()>>>) -> Arc<RwLock<()>> {
    let key = Arc::as_ptr(target_list) as usize;
    let mut gates = TARGET_LIST_DISPATCH_GATES.lock().unwrap_or_else(|err| err.into_inner());
    gates.retain(|_, gate| gate.strong_count() != 0);
    if let Some(gate) = gates.get(&key).and_then(Weak::upgrade) {
        return gate;
    }
    let gate = preferred.unwrap_or_else(|| Arc::new(RwLock::new(())));
    gates.insert(key, Arc::downgrade(&gate));
    gate
}

pub(crate) struct DirectDispatchTracker {
    cancellation: CancellationToken,
    inflight: watch::Sender<usize>,
}

impl DirectDispatchTracker {
    fn new() -> Self {
        let (inflight, _) = watch::channel(0);
        Self {
            cancellation: CancellationToken::new(),
            inflight,
        }
    }

    fn acquire(self: &Arc<Self>) -> DirectDispatchLease {
        self.inflight.send_modify(|count| *count += 1);
        DirectDispatchLease {
            tracker: Arc::clone(self),
        }
    }

    pub(crate) async fn wait_idle(&self) {
        let mut inflight = self.inflight.subscribe();
        while *inflight.borrow_and_update() != 0 {
            if inflight.changed().await.is_err() {
                return;
            }
        }
    }

    fn cancel_pending(&self) {
        self.cancellation.cancel();
    }
}

struct DirectDispatchLease {
    tracker: Arc<DirectDispatchTracker>,
}

impl DirectDispatchLease {
    fn cancellation(&self) -> CancellationToken {
        self.tracker.cancellation.clone()
    }
}

impl Drop for DirectDispatchLease {
    fn drop(&mut self) {
        self.tracker.inflight.send_modify(|count| {
            if let Some(next) = count.checked_sub(1) {
                *count = next;
            } else {
                error!(
                    event = EVENT_NOTIFY_DISPATCH_FAILED,
                    component = LOG_COMPONENT_NOTIFY,
                    subsystem = LOG_SUBSYSTEM_DISPATCH,
                    reason = "direct_lease_underflow",
                    "Notify direct dispatch lease accounting underflowed"
                );
            }
        });
    }
}

/// Resolves the effective send concurrency (semaphore permit count).
///
/// A value of `0` would build a zero-permit semaphore, so `acquire` never
/// completes and every dispatch deadlocks. A misconfigured
/// `RUSTFS_NOTIFY_SEND_CONCURRENCY=0` therefore coerces back to the default
/// instead of silently wedging notifications (backlog#984).
fn resolve_send_concurrency() -> usize {
    let configured = rustfs_utils::get_env_usize(ENV_NOTIFY_SEND_CONCURRENCY, DEFAULT_NOTIFY_SEND_CONCURRENCY);
    coerce_send_concurrency(configured)
}

/// Coerces a configured send concurrency into a valid semaphore permit count.
/// `0` maps back to the default; any positive value is passed through. Kept as a
/// pure function so the coercion is unit-testable without mutating process env.
fn coerce_send_concurrency(configured: usize) -> usize {
    if configured == 0 {
        warn!(
            event = EVENT_NOTIFY_RUNTIME_LIFECYCLE,
            component = LOG_COMPONENT_NOTIFY,
            subsystem = LOG_SUBSYSTEM_DISPATCH,
            configured,
            default = DEFAULT_NOTIFY_SEND_CONCURRENCY,
            "invalid RUSTFS_NOTIFY_SEND_CONCURRENCY=0; falling back to default to avoid a zero-permit deadlock"
        );
        DEFAULT_NOTIFY_SEND_CONCURRENCY
    } else {
        configured
    }
}

/// Manages event notification to targets based on rules
pub struct EventNotifier {
    dispatch_gate: Arc<RwLock<()>>,
    enqueue_limiter: Arc<Semaphore>,
    metrics: Arc<NotificationMetrics>,
    rule_engine: NotifyRuleEngine,
    target_list: SharedNotifyTargetList,
    send_limiter: Arc<Semaphore>,
}

impl Default for EventNotifier {
    fn default() -> Self {
        Self::new(Arc::new(NotificationMetrics::new()), NotifyRuleEngine::new())
    }
}

impl EventNotifier {
    /// Creates a new EventNotifier
    ///
    /// # Returns
    /// Returns a new instance of EventNotifier.
    pub fn new(metrics: Arc<NotificationMetrics>, rule_engine: NotifyRuleEngine) -> Self {
        let max_inflight = resolve_send_concurrency();
        let target_list = Arc::new(RwLock::new(TargetList::new()));
        let dispatch_gate = shared_dispatch_gate(&target_list, None);
        EventNotifier {
            dispatch_gate,
            enqueue_limiter: Arc::new(Semaphore::new(max_inflight)),
            metrics,
            rule_engine,
            target_list,
            send_limiter: Arc::new(Semaphore::new(max_inflight)),
        }
    }

    /// Returns a reference to the target list
    /// This method provides access to the target list for external use.
    ///
    /// # Returns
    /// Returns an `Arc<RwLock<TargetList>>` representing the target list.
    pub fn target_list(&self) -> SharedNotifyTargetList {
        Arc::clone(&self.target_list)
    }

    pub(crate) fn dispatch_gate(&self) -> Arc<RwLock<()>> {
        self.dispatch_gate.clone()
    }

    /// Returns a list of ARNs for the registered targets
    ///
    /// # Arguments
    /// * `region` - The region to use for generating the ARNs
    ///
    /// # Returns
    /// Returns a vector of strings representing the ARNs of the registered targets
    pub async fn get_arn_list(&self, region: &str) -> Vec<String> {
        let target_list_guard = self.target_list.read().await;
        target_list_guard
            .keys()
            .iter()
            .map(|target_id| target_id.to_arn(region).to_string())
            .collect()
    }

    pub async fn remove_all_bucket_targets(&self) {
        let mut target_list_guard = self.target_list.write().await;
        target_list_guard.clear_targets_only().await;
        info!(
            event = EVENT_NOTIFY_RUNTIME_LIFECYCLE,
            component = LOG_COMPONENT_NOTIFY,
            subsystem = LOG_SUBSYSTEM_DISPATCH,
            state = "targets_cleared",
            "notify runtime lifecycle"
        );
    }

    /// Sends an event to the appropriate targets based on the bucket rules
    ///
    /// # Arguments
    /// * `event` - The event to send
    #[instrument(skip_all)]
    pub async fn send(&self, event: Arc<Event>) {
        let bucket_name = &event.s3.bucket.name;
        let object_key = &event.s3.object.key;
        let event_name = event.event_name;

        let target_ids = self.rule_engine.match_targets(bucket_name, event_name, object_key).await;
        if target_ids.is_empty() {
            debug!(
                event = EVENT_NOTIFY_DISPATCH_SKIPPED,
                component = LOG_COMPONENT_NOTIFY,
                subsystem = LOG_SUBSYSTEM_DISPATCH,
                bucket = %bucket_name,
                object = %object_key,
                reason = "no_matching_targets",
                "Skipped notify dispatch"
            );
            self.metrics.increment_skipped();
            return;
        }
        let target_ids_len = target_ids.len();
        let mut deferred_handles = Vec::new();
        let mut direct_handles = Vec::new();

        // A lifecycle writer holds this gate only while handing queue-store
        // ownership from one runtime generation to the next. Taking the read
        // guard before cloning targets means the writer both blocks new sends
        // and drains every save already using the old generation.
        let dispatch_guard = Arc::new(self.dispatch_gate.clone().read_owned().await);

        // Use scope to limit the borrow scope of target_list
        let target_list_guard = self.target_list.read().await;
        debug!(
            event = EVENT_NOTIFY_DISPATCH_STARTED,
            component = LOG_COMPONENT_NOTIFY,
            subsystem = LOG_SUBSYSTEM_DISPATCH,
            bucket = %bucket_name,
            object = %object_key,
            target_count = target_ids_len,
            "notify dispatch"
        );
        for target_id in target_ids {
            // `get` now returns Option<Arc<dyn Target + Send + Sync>>
            if let Some(target_arc) = target_list_guard.get(&target_id) {
                // Clone an Arc<Box<dyn Target>> (which is where target_list is stored) to move into an asynchronous task
                // target_arc is already Arc, clone it for the async task
                let target_for_task = target_arc.clone();
                if !target_for_task.is_enabled() {
                    debug!(
                        event = EVENT_NOTIFY_DISPATCH_SKIPPED,
                        component = LOG_COMPONENT_NOTIFY,
                        subsystem = LOG_SUBSYSTEM_DISPATCH,
                        target_id = %target_for_task.id(),
                        reason = "target_disabled",
                        "Skipped notify dispatch target"
                    );
                    continue;
                }
                let is_deferred = target_for_task.store().is_some();
                let direct_dispatch_lease = (!is_deferred).then(|| target_list_guard.direct_dispatch_lease());
                let direct_cancellation = direct_dispatch_lease.as_ref().map(DirectDispatchLease::cancellation);
                let deferred_dispatch_guard = is_deferred.then(|| Arc::clone(&dispatch_guard));
                let limiter = if is_deferred {
                    self.enqueue_limiter.clone()
                } else {
                    self.send_limiter.clone()
                };
                let metrics = self.metrics.clone();
                let event_clone = event.clone();
                let target_name_for_task = target_for_task.name(); // Get the name before generating the task
                debug!(
                    event = EVENT_NOTIFY_DISPATCH_STARTED,
                    component = LOG_COMPONENT_NOTIFY,
                    subsystem = LOG_SUBSYSTEM_DISPATCH,
                    target_id = %target_for_task.id(),
                    deferred = is_deferred,
                    "notify dispatch"
                );
                // Use cloned data in closures to avoid borrowing conflicts
                // Create an EntityTarget from the event
                let entity_target: Arc<EntityTarget<Event>> = Arc::new(EntityTarget {
                    object_name: object_key.to_string(),
                    bucket_name: bucket_name.to_string(),
                    event_name,
                    data: event_clone.as_ref().clone(),
                });
                let handle = tokio::spawn(async move {
                    let _direct_dispatch_lease = direct_dispatch_lease;
                    let _deferred_dispatch_guard = deferred_dispatch_guard;
                    metrics.increment_processing();
                    let permit = if let Some(cancellation) = direct_cancellation {
                        tokio::select! {
                            biased;
                            _ = cancellation.cancelled() => {
                                metrics.decrement_processing();
                                metrics.increment_skipped();
                                debug!(
                                    event = EVENT_NOTIFY_DISPATCH_SKIPPED,
                                    component = LOG_COMPONENT_NOTIFY,
                                    subsystem = LOG_SUBSYSTEM_DISPATCH,
                                    target_id = %target_name_for_task,
                                    reason = "runtime_generation_replaced",
                                    "Skipped pending direct notify dispatch"
                                );
                                return;
                            }
                            permit = limiter.acquire_owned() => permit,
                        }
                    } else {
                        limiter.acquire_owned().await
                    };
                    let _permit = match permit {
                        Ok(permit) => permit,
                        Err(e) => {
                            error!(
                                event = EVENT_NOTIFY_DISPATCH_FAILED,
                                component = LOG_COMPONENT_NOTIFY,
                                subsystem = LOG_SUBSYSTEM_DISPATCH,
                                target_id = %target_name_for_task,
                                error = %e,
                                reason = "permit_acquire_failed",
                                "Failed to acquire notify dispatch permit"
                            );
                            metrics.increment_failed();
                            return;
                        }
                    };
                    if let Err(e) = target_for_task.save(entity_target.clone()).await {
                        metrics.increment_failed();
                        error!(
                            event = EVENT_NOTIFY_DISPATCH_FAILED,
                            component = LOG_COMPONENT_NOTIFY,
                            subsystem = LOG_SUBSYSTEM_DISPATCH,
                            target_id = %target_name_for_task,
                            error = %e,
                            reason = "send_failed",
                            "Failed to dispatch notify event"
                        );
                    } else {
                        // Counting lifecycle (backlog#979): `increment_processing` above marks
                        // the event as in-flight exactly once. For a non-deferred target, `save`
                        // performs synchronous delivery, so the event is fully processed here and
                        // we transition it from processing to processed. For a deferred
                        // (store-backed) target, `save` only enqueues the event to the store: it
                        // stays in-flight until the replay worker actually delivers it and emits
                        // `ReplayEvent::Delivered`, which is the single place that calls
                        // `increment_processed` for that event. Decrementing `processing_events`
                        // here as well would double-count the completion and underflow the
                        // counter back to `usize::MAX`, corrupting metrics and any in-flight
                        // backpressure decisions.
                        if !is_deferred {
                            metrics.increment_processed();
                        }
                        debug!(
                            event = EVENT_NOTIFY_DISPATCH_COMPLETED,
                            component = LOG_COMPONENT_NOTIFY,
                            subsystem = LOG_SUBSYSTEM_DISPATCH,
                            target_id = %target_name_for_task,
                            deferred = is_deferred,
                            "notify dispatch"
                        );
                    }
                });
                if is_deferred {
                    deferred_handles.push(handle);
                } else {
                    direct_handles.push(handle);
                }
            } else {
                warn!(
                    event = EVENT_NOTIFY_DISPATCH_FAILED,
                    component = LOG_COMPONENT_NOTIFY,
                    subsystem = LOG_SUBSYSTEM_DISPATCH,
                    target_id = %target_id,
                    reason = "target_missing_from_runtime",
                    "Matched notify target is missing from runtime"
                );
                self.metrics.increment_skipped();
            }
        }
        // target_list is automatically released here
        drop(target_list_guard);

        // Every store-backed save owns a share of the generation guard, so
        // caller cancellation cannot race lifecycle handoff with an enqueue.
        // Direct targets own no queue store and may finish against the detached
        // target while lifecycle progresses.
        drop(dispatch_guard);
        wait_for_dispatch_tasks(deferred_handles).await;
        wait_for_dispatch_tasks(direct_handles).await;
        debug!(
            event = EVENT_NOTIFY_DISPATCH_COMPLETED,
            component = LOG_COMPONENT_NOTIFY,
            subsystem = LOG_SUBSYSTEM_DISPATCH,
            bucket = %bucket_name,
            target_count = target_ids_len,
            "notify dispatch"
        );
    }

    /// Initializes the targets for buckets from shared target handles.
    #[instrument(skip(self, targets_to_init))]
    pub async fn init_bucket_targets_shared(&self, targets_to_init: Vec<SharedTarget<Event>>) -> Result<(), NotificationError> {
        let mut target_list_guard = self.target_list.write().await;
        // Close the currently registered targets before replacing them. A bare
        // `clear()` drops the old targets without invoking `close()`, leaking their
        // connections/streams; `clear_targets_only` closes each one first (backlog#984).
        target_list_guard.clear_targets_only().await;

        for target in targets_to_init {
            debug!(
                event = EVENT_NOTIFY_RUNTIME_LIFECYCLE,
                component = LOG_COMPONENT_NOTIFY,
                subsystem = LOG_SUBSYSTEM_DISPATCH,
                state = "target_init",
                target_id = %target.id(),
                "Initializing notify runtime target"
            );
            target_list_guard.add(target)?;
        }

        tracing::info!(
            event = EVENT_NOTIFY_RUNTIME_LIFECYCLE,
            component = LOG_COMPONENT_NOTIFY,
            subsystem = LOG_SUBSYSTEM_DISPATCH,
            state = "targets_initialized",
            target_count = target_list_guard.len(),
            "notify runtime lifecycle"
        );
        Ok(())
    }
}

/// A thread-safe list of targets
pub struct TargetList {
    direct_dispatches: Arc<DirectDispatchTracker>,
    /// Map of TargetID to Target
    runtime: TargetRuntimeManager<Event>,
}

impl Default for TargetList {
    fn default() -> Self {
        Self::new()
    }
}

impl TargetList {
    /// Creates a new TargetList
    pub fn new() -> Self {
        TargetList {
            direct_dispatches: Arc::new(DirectDispatchTracker::new()),
            runtime: TargetRuntimeManager::new(),
        }
    }

    /// Adds a target to the list
    ///
    /// # Arguments
    /// * `target` - The target to add
    ///
    /// # Returns
    /// Returns `Ok(())` if the target was added successfully, or a `NotificationError` if an error occurred.
    pub fn add(&mut self, target: Arc<dyn Target<Event> + Send + Sync>) -> Result<(), NotificationError> {
        let id = target.id();
        if self.runtime.get_by_target_id(&id).is_some() {
            // Potentially update or log a warning/error if replacing an existing target.
            warn!(
                event = EVENT_NOTIFY_RUNTIME_LIFECYCLE,
                component = LOG_COMPONENT_NOTIFY,
                subsystem = LOG_SUBSYSTEM_DISPATCH,
                state = "target_overwrite",
                target_id = %id,
                "Overwriting existing notify runtime target"
            );
        }
        self.runtime.add_arc(target);
        Ok(())
    }

    /// Clears all targets from the list
    pub fn clear(&mut self) {
        self.runtime.clear();
    }

    /// Removes a target by ID. Note: This does not stop its associated event stream.
    /// Stream cancellation should be handled by EventNotifier.
    ///
    /// # Arguments
    /// * `id` - The ID of the target to remove
    ///
    /// # Returns
    /// Returns the removed target if it existed, otherwise `None`.
    pub async fn remove_target_only(&mut self, id: &TargetID) -> Option<SharedTarget<Event>> {
        self.runtime.remove_by_target_id_and_close(id).await
    }

    /// Clears all targets from the list. Note: This does not stop their associated event streams.
    /// Stream cancellation should be handled by EventNotifier.
    pub async fn clear_targets_only(&mut self) {
        self.runtime.clear_and_close().await;
    }

    /// Returns a target by ID
    ///
    /// # Arguments
    /// * `id` - The ID of the target to retrieve
    ///
    /// # Returns
    /// Returns the target if it exists, otherwise `None`.
    pub fn get(&self, id: &TargetID) -> Option<SharedTarget<Event>> {
        self.runtime.get_by_target_id(id)
    }

    /// Returns all target IDs
    pub fn keys(&self) -> Vec<TargetID> {
        self.runtime.target_ids()
    }

    /// Returns all targets in the list
    pub fn values(&self) -> Vec<SharedTarget<Event>> {
        self.runtime.values()
    }

    pub fn runtime_snapshots(&self) -> Vec<rustfs_targets::RuntimeTargetSnapshot> {
        self.runtime.snapshots()
    }

    pub async fn runtime_health_snapshots(&self) -> Vec<rustfs_targets::RuntimeTargetHealthSnapshot> {
        self.runtime.health_snapshots().await
    }

    pub fn runtime_status_snapshot(
        &self,
        replay_workers: &rustfs_targets::ReplayWorkerManager,
    ) -> rustfs_targets::RuntimeStatusSnapshot {
        self.runtime.status_snapshot(replay_workers)
    }

    fn direct_dispatch_lease(&self) -> DirectDispatchLease {
        self.direct_dispatches.acquire()
    }

    pub(crate) fn replace_runtime(
        &mut self,
        replacement: TargetRuntimeManager<Event>,
    ) -> (TargetRuntimeManager<Event>, Arc<DirectDispatchTracker>) {
        let runtime = std::mem::replace(&mut self.runtime, replacement);
        let direct_dispatches = std::mem::replace(&mut self.direct_dispatches, Arc::new(DirectDispatchTracker::new()));
        direct_dispatches.cancel_pending();
        (runtime, direct_dispatches)
    }

    pub fn runtime_mut(&mut self) -> &mut TargetRuntimeManager<Event> {
        &mut self.runtime
    }

    /// Returns the number of targets
    pub fn len(&self) -> usize {
        self.runtime.len()
    }

    /// is_empty can be derived from len()
    pub fn is_empty(&self) -> bool {
        self.runtime.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{rule_engine::NotifyRuleEngine, rules::RulesMap};
    use async_trait::async_trait;
    use rustfs_s3_types::EventName;
    use rustfs_targets::StoreError;
    use rustfs_targets::{
        ReplayWorkerManager, TargetError,
        store::{Key, QueueStore, Store},
        target::{EntityTarget, QueuedPayload, QueuedPayloadMeta},
    };
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use tokio::sync::Notify;

    #[tokio::test]
    async fn encoded_event_key_matches_raw_prefix_suffix_filter() {
        let target_id = TargetID::new("primary".to_string(), "webhook".to_string());
        let mut rules_map = RulesMap::new();
        rules_map.add_rule_config(&[EventName::ObjectCreatedPut], "uploads/*.csv".to_string(), target_id.clone());

        let rule_engine = NotifyRuleEngine::new();
        rule_engine.set_bucket_rules("test-bucket", rules_map).await;

        let targets = rule_engine
            .match_targets("test-bucket", EventName::ObjectCreatedPut, "uploads%2Freport.csv")
            .await;

        assert!(targets.contains(&target_id));
    }

    #[tokio::test]
    async fn encoded_event_key_matches_raw_and_decoded_rule_targets() {
        let raw_target = TargetID::new("raw".to_string(), "webhook".to_string());
        let decoded_target = TargetID::new("decoded".to_string(), "webhook".to_string());
        let mut rules_map = RulesMap::new();
        rules_map.add_rule_config(&[EventName::ObjectCreatedPut], "uploads%2F*.csv".to_string(), raw_target.clone());
        rules_map.add_rule_config(&[EventName::ObjectCreatedPut], "uploads/*.csv".to_string(), decoded_target.clone());

        let rule_engine = NotifyRuleEngine::new();
        rule_engine.set_bucket_rules("test-bucket", rules_map).await;

        let targets = rule_engine
            .match_targets("test-bucket", EventName::ObjectCreatedPut, "uploads%2Freport.csv")
            .await;

        assert_eq!(targets.len(), 2);
        assert!(targets.contains(&raw_target));
        assert!(targets.contains(&decoded_target));
    }

    #[tokio::test]
    async fn encoded_event_key_does_not_bypass_suffix_filter() {
        let target_id = TargetID::new("primary".to_string(), "webhook".to_string());
        let mut rules_map = RulesMap::new();
        rules_map.add_rule_config(&[EventName::ObjectCreatedPut], "uploads/*.csv".to_string(), target_id);

        let rule_engine = NotifyRuleEngine::new();
        rule_engine.set_bucket_rules("test-bucket", rules_map).await;

        let root_targets = rule_engine
            .match_targets("test-bucket", EventName::ObjectCreatedPut, "report.csv")
            .await;
        let suffix_targets = rule_engine
            .match_targets("test-bucket", EventName::ObjectCreatedPut, "uploads%2Freport.txt")
            .await;

        assert!(root_targets.is_empty());
        assert!(suffix_targets.is_empty());
    }

    #[derive(Clone)]
    struct TestTarget {
        block_first_save: Option<(Arc<Notify>, Arc<Notify>)>,
        close_calls: Arc<AtomicUsize>,
        close_entered: Option<Arc<Notify>>,
        id: TargetID,
        enabled: bool,
        save_calls: Arc<AtomicUsize>,
        selected_calls: Arc<AtomicUsize>,
        store: Option<QueueStore<QueuedPayload>>,
    }

    impl TestTarget {
        fn new(id: &str, name: &str, enabled: bool) -> Self {
            Self {
                block_first_save: None,
                close_calls: Arc::new(AtomicUsize::new(0)),
                close_entered: None,
                id: TargetID::new(id.to_string(), name.to_string()),
                enabled,
                save_calls: Arc::new(AtomicUsize::new(0)),
                selected_calls: Arc::new(AtomicUsize::new(0)),
                store: None,
            }
        }

        fn with_blocked_first_save(mut self, entered: Arc<Notify>, release: Arc<Notify>) -> Self {
            self.block_first_save = Some((entered, release));
            self
        }

        fn with_store(mut self, store: QueueStore<QueuedPayload>) -> Self {
            self.store = Some(store);
            self
        }

        fn with_close_observer(mut self, close_entered: Arc<Notify>) -> Self {
            self.close_entered = Some(close_entered);
            self
        }
    }

    #[async_trait]
    impl<E> Target<E> for TestTarget
    where
        E: rustfs_targets::PluginEvent,
    {
        fn id(&self) -> TargetID {
            self.id.clone()
        }

        async fn is_active(&self) -> Result<bool, TargetError> {
            Ok(self.enabled)
        }

        async fn save(&self, _event: Arc<EntityTarget<E>>) -> Result<(), TargetError> {
            let call = self.save_calls.fetch_add(1, Ordering::SeqCst);
            if call == 0
                && let Some((entered, release)) = &self.block_first_save
            {
                entered.notify_one();
                release.notified().await;
            }
            Ok(())
        }

        async fn send_raw_from_store(&self, _key: Key, _body: Vec<u8>, _meta: QueuedPayloadMeta) -> Result<(), TargetError> {
            Ok(())
        }

        async fn close(&self) -> Result<(), TargetError> {
            self.close_calls.fetch_add(1, Ordering::SeqCst);
            if let Some(close_entered) = &self.close_entered {
                close_entered.notify_one();
            }
            Ok(())
        }

        fn store(&self) -> Option<&(dyn Store<QueuedPayload, Error = StoreError, Key = Key> + Send + Sync)> {
            self.store
                .as_ref()
                .map(|store| store as &(dyn Store<QueuedPayload, Error = StoreError, Key = Key> + Send + Sync))
        }

        fn clone_dyn(&self) -> Box<dyn Target<E> + Send + Sync> {
            let cloned = self.clone();
            Box::new(cloned)
        }

        async fn init(&self) -> Result<(), TargetError> {
            Ok(())
        }

        fn is_enabled(&self) -> bool {
            self.selected_calls.fetch_add(1, Ordering::SeqCst);
            self.enabled
        }
    }

    #[tokio::test]
    async fn lifecycle_pause_drains_entered_deferred_dispatch_and_blocks_new_dispatch() {
        let metrics = Arc::new(NotificationMetrics::new());
        let rule_engine = NotifyRuleEngine::new();
        let notifier = Arc::new(EventNotifier::new(metrics.clone(), rule_engine.clone()));
        let save_entered = Arc::new(Notify::new());
        let save_release = Arc::new(Notify::new());
        let queue_dir = tempfile::tempdir().expect("queue tempdir should be created");
        let target = TestTarget::new("gated-target", "webhook", true)
            .with_blocked_first_save(save_entered.clone(), save_release.clone())
            .with_store(QueueStore::new(queue_dir.path(), 16, ".event"));

        let mut rules_map = RulesMap::new();
        rules_map.add_rule_config(&[EventName::ObjectCreatedPut], "*".to_string(), target.id.clone());
        rule_engine.set_bucket_rules("bucket", rules_map).await;
        notifier
            .target_list()
            .write()
            .await
            .add(Arc::new(target.clone()))
            .expect("target install should succeed");

        let facade = crate::runtime_facade::NotifyRuntimeFacade::new_with_dispatch_gate(
            notifier.target_list(),
            Arc::new(RwLock::new(ReplayWorkerManager::new())),
            notifier.dispatch_gate(),
            Arc::new(Semaphore::new(1)),
            metrics,
        );
        let first_dispatch = tokio::spawn({
            let notifier = notifier.clone();
            async move {
                notifier
                    .send(Arc::new(Event::new_test_event("bucket", "first", EventName::ObjectCreatedPut)))
                    .await;
            }
        });
        save_entered.notified().await;
        assert_eq!(target.save_calls.load(Ordering::SeqCst), 1);

        let mut pause = Box::pin(facade.pause_dispatch());
        tokio::select! {
            biased;
            _ = &mut pause => panic!("lifecycle pause crossed an in-flight dispatch"),
            _ = std::future::ready(()) => {}
        }

        save_release.notify_one();
        first_dispatch.await.expect("first dispatch task should finish");
        let pause_guard = pause.await;

        let replacement = TestTarget::new("gated-target", "webhook", true);
        {
            let target_list = notifier.target_list();
            let mut target_list = target_list.write().await;
            target_list.clear();
            target_list
                .add(Arc::new(replacement.clone()))
                .expect("replacement target install should succeed");
        }

        let mut second_dispatch =
            Box::pin(notifier.send(Arc::new(Event::new_test_event("bucket", "second", EventName::ObjectCreatedPut))));
        tokio::select! {
            biased;
            _ = &mut second_dispatch => panic!("dispatch crossed the lifecycle pause"),
            _ = std::future::ready(()) => {}
        }
        assert_eq!(
            replacement.selected_calls.load(Ordering::SeqCst),
            0,
            "a paused dispatch must not select a target from the replacement generation early"
        );
        assert_eq!(target.save_calls.load(Ordering::SeqCst), 1);
        assert_eq!(replacement.save_calls.load(Ordering::SeqCst), 0);

        drop(pause_guard);
        second_dispatch.await;
        assert_eq!(target.selected_calls.load(Ordering::SeqCst), 1);
        assert_eq!(target.save_calls.load(Ordering::SeqCst), 1);
        assert_eq!(replacement.selected_calls.load(Ordering::SeqCst), 1);
        assert_eq!(replacement.save_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn lifecycle_pause_does_not_wait_for_direct_network_dispatch() {
        let metrics = Arc::new(NotificationMetrics::new());
        let rule_engine = NotifyRuleEngine::new();
        let notifier = Arc::new(EventNotifier::new(metrics.clone(), rule_engine.clone()));
        let save_entered = Arc::new(Notify::new());
        let save_release = Arc::new(Notify::new());
        let target =
            TestTarget::new("direct-target", "webhook", true).with_blocked_first_save(save_entered.clone(), save_release.clone());

        let mut rules_map = RulesMap::new();
        rules_map.add_rule_config(&[EventName::ObjectCreatedPut], "*".to_string(), target.id.clone());
        rule_engine.set_bucket_rules("bucket", rules_map).await;
        notifier
            .target_list()
            .write()
            .await
            .add(Arc::new(target))
            .expect("target install should succeed");

        let facade = crate::runtime_facade::NotifyRuntimeFacade::new_with_dispatch_gate(
            notifier.target_list(),
            Arc::new(RwLock::new(ReplayWorkerManager::new())),
            notifier.dispatch_gate(),
            Arc::new(Semaphore::new(1)),
            metrics,
        );
        let dispatch = tokio::spawn({
            let notifier = notifier.clone();
            async move {
                notifier
                    .send(Arc::new(Event::new_test_event("bucket", "object", EventName::ObjectCreatedPut)))
                    .await;
            }
        });
        save_entered.notified().await;

        let mut pause = Box::pin(facade.pause_dispatch());
        let pause_guard = tokio::select! {
            biased;
            guard = &mut pause => guard,
            _ = std::future::ready(()) => panic!("a direct network send blocked lifecycle handoff"),
        };
        drop(pause_guard);
        save_release.notify_one();
        dispatch.await.expect("direct dispatch should finish after release");
    }

    #[tokio::test]
    async fn replacement_cancels_permit_waiting_direct_dispatch_before_closing_target() {
        let metrics = Arc::new(NotificationMetrics::new());
        let rule_engine = NotifyRuleEngine::new();
        let notifier = Arc::new(EventNotifier {
            send_limiter: Arc::new(Semaphore::new(1)),
            ..EventNotifier::new(metrics.clone(), rule_engine.clone())
        });
        let first_entered = Arc::new(Notify::new());
        let first_release = Arc::new(Notify::new());
        let close_entered = Arc::new(Notify::new());
        let target = TestTarget::new("direct-target", "webhook", true)
            .with_blocked_first_save(first_entered.clone(), first_release.clone())
            .with_close_observer(close_entered);

        let mut rules_map = RulesMap::new();
        rules_map.add_rule_config(&[EventName::ObjectCreatedPut], "*".to_string(), target.id.clone());
        rule_engine.set_bucket_rules("bucket", rules_map).await;
        notifier
            .target_list()
            .write()
            .await
            .add(Arc::new(target.clone()))
            .expect("target should install");

        let facade = crate::runtime_facade::NotifyRuntimeFacade::new_with_dispatch_gate(
            notifier.target_list(),
            Arc::new(RwLock::new(ReplayWorkerManager::new())),
            notifier.dispatch_gate(),
            Arc::new(Semaphore::new(1)),
            metrics.clone(),
        );
        let first = tokio::spawn({
            let notifier = notifier.clone();
            async move {
                notifier
                    .send(Arc::new(Event::new_test_event("bucket", "first", EventName::ObjectCreatedPut)))
                    .await;
            }
        });
        first_entered.notified().await;

        // This task selects the old generation and acquires its lease before
        // waiting for the saturated direct-send permit.
        let second = tokio::spawn({
            let notifier = notifier.clone();
            async move {
                notifier
                    .send(Arc::new(Event::new_test_event("bucket", "second", EventName::ObjectCreatedPut)))
                    .await;
            }
        });
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while target.selected_calls.load(Ordering::SeqCst) != 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("both direct sends should select the old generation");

        let activation = facade.activate_targets_with_replay(Vec::new()).await;
        let mut replace = Box::pin(facade.replace_targets(activation));
        tokio::select! {
            biased;
            result = &mut replace => panic!("replacement closed a generation with selected direct sends: {result:?}"),
            _ = std::future::ready(()) => {}
        }
        assert_eq!(target.close_calls.load(Ordering::SeqCst), 0);

        first_release.notify_one();
        first.await.expect("first direct dispatch should finish");
        second.await.expect("permit-waiting direct dispatch should be cancelled");
        replace.await.expect("replacement should close after direct leases drain");
        assert_eq!(target.save_calls.load(Ordering::SeqCst), 1);
        assert_eq!(target.close_calls.load(Ordering::SeqCst), 1);
        assert_eq!(metrics.processing_count(), 0);
        assert_eq!(metrics.processed_count(), 1);
        assert_eq!(metrics.skipped_count(), 1);
    }

    #[tokio::test]
    async fn caller_abort_does_not_release_deferred_generation_lease() {
        let metrics = Arc::new(NotificationMetrics::new());
        let rule_engine = NotifyRuleEngine::new();
        let notifier = Arc::new(EventNotifier::new(metrics.clone(), rule_engine.clone()));
        let save_entered = Arc::new(Notify::new());
        let save_release = Arc::new(Notify::new());
        let queue_dir = tempfile::tempdir().expect("queue tempdir should be created");
        let target = TestTarget::new("deferred", "webhook", true)
            .with_blocked_first_save(save_entered.clone(), save_release.clone())
            .with_store(QueueStore::new(queue_dir.path(), 16, ".event"));

        let mut rules_map = RulesMap::new();
        rules_map.add_rule_config(&[EventName::ObjectCreatedPut], "*".to_string(), target.id.clone());
        rule_engine.set_bucket_rules("bucket", rules_map).await;
        notifier
            .target_list()
            .write()
            .await
            .add(Arc::new(target))
            .expect("target should install");
        let facade = crate::runtime_facade::NotifyRuntimeFacade::new_with_dispatch_gate(
            notifier.target_list(),
            Arc::new(RwLock::new(ReplayWorkerManager::new())),
            notifier.dispatch_gate(),
            Arc::new(Semaphore::new(1)),
            metrics,
        );

        let dispatch = tokio::spawn({
            let notifier = notifier.clone();
            async move {
                notifier
                    .send(Arc::new(Event::new_test_event("bucket", "object", EventName::ObjectCreatedPut)))
                    .await;
            }
        });
        save_entered.notified().await;
        dispatch.abort();
        let _ = dispatch.await;

        let mut pause = Box::pin(facade.pause_dispatch());
        tokio::select! {
            biased;
            _ = &mut pause => panic!("caller abort released the deferred generation lease"),
            _ = std::future::ready(()) => {}
        }
        save_release.notify_one();
        let pause_guard = pause.await;
        drop(pause_guard);
    }

    #[tokio::test]
    async fn deferred_enqueue_concurrency_is_bounded() {
        const LIMIT: usize = 2;
        const TARGETS: usize = 3;

        let metrics = Arc::new(NotificationMetrics::new());
        let rule_engine = NotifyRuleEngine::new();
        let notifier = Arc::new(EventNotifier {
            enqueue_limiter: Arc::new(Semaphore::new(LIMIT)),
            ..EventNotifier::new(metrics, rule_engine.clone())
        });
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let queue_dir = tempfile::tempdir().expect("queue tempdir should be created");
        let mut targets = Vec::new();
        let mut rules_map = RulesMap::new();
        for index in 0..TARGETS {
            let target = TestTarget::new(&format!("deferred-{index}"), "webhook", true)
                .with_blocked_first_save(entered.clone(), release.clone())
                .with_store(QueueStore::new(queue_dir.path().join(index.to_string()), 16, ".event"));
            rules_map.add_rule_config(&[EventName::ObjectCreatedPut], "*".to_string(), target.id.clone());
            notifier
                .target_list()
                .write()
                .await
                .add(Arc::new(target.clone()))
                .expect("target should install");
            targets.push(target);
        }
        rule_engine.set_bucket_rules("bucket", rules_map).await;

        let dispatch = tokio::spawn({
            let notifier = notifier.clone();
            async move {
                notifier
                    .send(Arc::new(Event::new_test_event("bucket", "object", EventName::ObjectCreatedPut)))
                    .await;
            }
        });
        let total_calls = || {
            targets
                .iter()
                .map(|target| target.save_calls.load(Ordering::SeqCst))
                .sum::<usize>()
        };
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while total_calls() != LIMIT {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the configured number of enqueues should enter");
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        assert_eq!(total_calls(), LIMIT, "enqueue concurrency exceeded its semaphore capacity");

        release.notify_waiters();
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while total_calls() != TARGETS {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the waiting enqueue should enter after a permit is released");
        release.notify_waiters();
        dispatch.await.expect("all bounded enqueues should finish");
    }

    #[tokio::test]
    async fn deferred_enqueue_does_not_wait_for_a_blocked_direct_send_permit() {
        let metrics = Arc::new(NotificationMetrics::new());
        let rule_engine = NotifyRuleEngine::new();
        let notifier = Arc::new(EventNotifier {
            send_limiter: Arc::new(Semaphore::new(1)),
            ..EventNotifier::new(metrics.clone(), rule_engine.clone())
        });
        let direct_entered = Arc::new(Notify::new());
        let direct_release = Arc::new(Notify::new());
        let direct =
            TestTarget::new("direct", "webhook", true).with_blocked_first_save(direct_entered.clone(), direct_release.clone());
        let deferred_entered = Arc::new(Notify::new());
        let deferred_release = Arc::new(Notify::new());
        let queue_dir = tempfile::tempdir().expect("queue tempdir should be created");
        let deferred = TestTarget::new("deferred", "webhook", true)
            .with_blocked_first_save(deferred_entered.clone(), deferred_release.clone())
            .with_store(QueueStore::new(queue_dir.path(), 16, ".event"));

        let mut direct_rules = RulesMap::new();
        direct_rules.add_rule_config(&[EventName::ObjectCreatedPut], "*".to_string(), direct.id.clone());
        rule_engine.set_bucket_rules("direct-bucket", direct_rules).await;
        let mut deferred_rules = RulesMap::new();
        deferred_rules.add_rule_config(&[EventName::ObjectCreatedPut], "*".to_string(), deferred.id.clone());
        rule_engine.set_bucket_rules("deferred-bucket", deferred_rules).await;
        {
            let target_list = notifier.target_list();
            let mut target_list = target_list.write().await;
            target_list.add(Arc::new(direct)).expect("direct target should install");
            target_list.add(Arc::new(deferred)).expect("deferred target should install");
        }

        let facade = crate::runtime_facade::NotifyRuntimeFacade::new_with_dispatch_gate(
            notifier.target_list(),
            Arc::new(RwLock::new(ReplayWorkerManager::new())),
            notifier.dispatch_gate(),
            Arc::new(Semaphore::new(1)),
            metrics,
        );
        let direct_dispatch = tokio::spawn({
            let notifier = notifier.clone();
            async move {
                notifier
                    .send(Arc::new(Event::new_test_event("direct-bucket", "object", EventName::ObjectCreatedPut)))
                    .await;
            }
        });
        direct_entered.notified().await;
        let deferred_dispatch = tokio::spawn({
            let notifier = notifier.clone();
            async move {
                notifier
                    .send(Arc::new(Event::new_test_event("deferred-bucket", "object", EventName::ObjectCreatedPut)))
                    .await;
            }
        });
        tokio::time::timeout(std::time::Duration::from_secs(1), deferred_entered.notified())
            .await
            .expect("queue persistence must not wait behind a direct network send permit");

        let mut pause = Box::pin(facade.pause_dispatch());
        tokio::select! {
            biased;
            _ = &mut pause => panic!("lifecycle pause crossed the blocked deferred enqueue"),
            _ = std::future::ready(()) => {}
        }
        deferred_release.notify_one();
        deferred_dispatch
            .await
            .expect("deferred dispatch should finish after release");
        let pause_guard = tokio::select! {
            biased;
            guard = &mut pause => guard,
            _ = std::future::ready(()) => panic!("direct network delivery kept the lifecycle gate locked"),
        };
        drop(pause_guard);

        direct_release.notify_one();
        direct_dispatch.await.expect("direct dispatch should finish after release");
    }

    #[tokio::test]
    async fn test_send_event_skips_disabled_target() {
        let rule_engine = NotifyRuleEngine::new();
        let notifier = EventNotifier::new(Arc::new(NotificationMetrics::new()), rule_engine.clone());

        let enabled_target = TestTarget::new("enabled-target", "webhook", true);
        let disabled_target = TestTarget::new("disabled-target", "webhook", false);

        let mut rules_map = RulesMap::new();
        rules_map.add_rule_config(&[EventName::ObjectCreatedPut], "*".to_string(), enabled_target.id.clone());
        rules_map.add_rule_config(&[EventName::ObjectCreatedPut], "*".to_string(), disabled_target.id.clone());

        rule_engine.set_bucket_rules("bucket", rules_map).await;
        notifier
            .target_list()
            .write()
            .await
            .add(Arc::new(enabled_target.clone()))
            .unwrap();
        notifier
            .target_list()
            .write()
            .await
            .add(Arc::new(disabled_target.clone()))
            .unwrap();

        let event = Arc::new(Event::new_test_event("bucket", "object", EventName::ObjectCreatedPut));
        notifier.send(event).await;

        assert_eq!(enabled_target.save_calls.load(Ordering::SeqCst), 1);
        assert_eq!(disabled_target.save_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn send_event_respects_prefix_suffix_filters() {
        let rule_engine = NotifyRuleEngine::new();
        let notifier = EventNotifier::new(Arc::new(NotificationMetrics::new()), rule_engine.clone());
        let target = TestTarget::new("filtered-target", "webhook", true);
        let mut rules_map = RulesMap::new();
        rules_map.add_rule_config(&[EventName::ObjectCreatedPut], "uploads/*.csv".to_string(), target.id.clone());

        rule_engine.set_bucket_rules("bucket", rules_map).await;
        notifier.target_list().write().await.add(Arc::new(target.clone())).unwrap();

        notifier
            .send(Arc::new(Event::new_test_event("bucket", "report.csv", EventName::ObjectCreatedPut)))
            .await;
        notifier
            .send(Arc::new(Event::new_test_event(
                "bucket",
                "uploads/report.txt",
                EventName::ObjectCreatedPut,
            )))
            .await;

        assert_eq!(target.save_calls.load(Ordering::SeqCst), 0);

        notifier
            .send(Arc::new(Event::new_test_event(
                "bucket",
                "uploads/report.csv",
                EventName::ObjectCreatedPut,
            )))
            .await;

        assert_eq!(target.save_calls.load(Ordering::SeqCst), 1);
    }

    /// A store-backed (deferred) target. `save` only enqueues to the store, so
    /// the actual delivery happens later in the replay worker.
    #[derive(Clone)]
    struct DeferredTestTarget {
        id: TargetID,
        save_calls: Arc<AtomicUsize>,
        store: QueueStore<QueuedPayload>,
    }

    impl DeferredTestTarget {
        fn new(id: &str, name: &str) -> Self {
            Self {
                id: TargetID::new(id.to_string(), name.to_string()),
                save_calls: Arc::new(AtomicUsize::new(0)),
                // The store is never actually written to here: `save` below only bumps a
                // counter. It just has to exist so the notifier treats this target as
                // store-backed (deferred delivery), exercising the deferred counting path.
                store: QueueStore::new(std::env::temp_dir().join("rustfs-notify-979-noop-store"), 0, ""),
            }
        }
    }

    #[async_trait]
    impl<E> Target<E> for DeferredTestTarget
    where
        E: rustfs_targets::PluginEvent,
    {
        fn id(&self) -> TargetID {
            self.id.clone()
        }

        async fn is_active(&self) -> Result<bool, TargetError> {
            Ok(true)
        }

        async fn save(&self, _event: Arc<EntityTarget<E>>) -> Result<(), TargetError> {
            self.save_calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        async fn send_raw_from_store(&self, _key: Key, _body: Vec<u8>, _meta: QueuedPayloadMeta) -> Result<(), TargetError> {
            Ok(())
        }

        async fn close(&self) -> Result<(), TargetError> {
            Ok(())
        }

        fn store(&self) -> Option<&(dyn Store<QueuedPayload, Error = StoreError, Key = Key> + Send + Sync)> {
            Some(&self.store)
        }

        fn clone_dyn(&self) -> Box<dyn Target<E> + Send + Sync> {
            Box::new(self.clone())
        }

        async fn init(&self) -> Result<(), TargetError> {
            Ok(())
        }

        fn is_enabled(&self) -> bool {
            true
        }
    }

    /// Regression test for backlog#979 (a): dispatching to a store-backed
    /// (deferred) target must count the event as in-flight exactly once and must
    /// not decrement `processing_events` at enqueue time. The replay worker is the
    /// single owner of the completion decrement (`increment_processed`); an extra
    /// decrement here previously double-counted and underflowed the counter back
    /// to `usize::MAX`.
    #[tokio::test]
    async fn deferred_target_dispatch_does_not_underflow_processing_counter() {
        let metrics = Arc::new(NotificationMetrics::new());
        let rule_engine = NotifyRuleEngine::new();
        let notifier = EventNotifier::new(metrics.clone(), rule_engine.clone());

        let target = DeferredTestTarget::new("deferred-target", "webhook");
        let mut rules_map = RulesMap::new();
        rules_map.add_rule_config(&[EventName::ObjectCreatedPut], "*".to_string(), target.id.clone());
        rule_engine.set_bucket_rules("bucket", rules_map).await;
        notifier.target_list().write().await.add(Arc::new(target.clone())).unwrap();

        // Dispatch enqueues to the store; the event stays in-flight (processing == 1).
        notifier
            .send(Arc::new(Event::new_test_event("bucket", "object", EventName::ObjectCreatedPut)))
            .await;

        assert_eq!(target.save_calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            metrics.processing_count(),
            1,
            "a queued deferred event must remain counted as in-flight after enqueue"
        );
        assert_eq!(metrics.processed_count(), 0);

        // Simulate the replay worker delivering the queued event (runtime_facade
        // maps `ReplayEvent::Delivered` to `increment_processed`).
        metrics.increment_processed();

        assert_eq!(
            metrics.processing_count(),
            0,
            "processing counter must return to zero, not underflow to usize::MAX"
        );
        assert_ne!(metrics.processing_count(), usize::MAX);
        assert_eq!(metrics.processed_count(), 1);
    }

    /// Regression test for backlog#984: a `RUSTFS_NOTIFY_SEND_CONCURRENCY=0`
    /// misconfiguration must not build a zero-permit semaphore (which would
    /// deadlock every dispatch); it must fall back to the default. Positive values
    /// pass through unchanged.
    #[test]
    fn zero_send_concurrency_falls_back_to_default() {
        assert_eq!(
            coerce_send_concurrency(0),
            DEFAULT_NOTIFY_SEND_CONCURRENCY,
            "zero concurrency must fall back to the default, never a zero-permit semaphore"
        );
        assert!(coerce_send_concurrency(0) > 0, "resolved concurrency must be strictly positive");
        assert_eq!(coerce_send_concurrency(1), 1);
        assert_eq!(coerce_send_concurrency(128), 128);
    }

    /// Regression test for backlog#984: replacing the runtime target set via
    /// `init_bucket_targets_shared` must `close()` the previously registered
    /// targets instead of dropping them silently (connection/stream leak).
    #[tokio::test]
    async fn init_bucket_targets_shared_closes_replaced_targets() {
        let rule_engine = NotifyRuleEngine::new();
        let notifier = EventNotifier::new(Arc::new(NotificationMetrics::new()), rule_engine);

        let old_target = ClosableTestTarget::new("old", "webhook");
        notifier
            .init_bucket_targets_shared(vec![Arc::new(old_target.clone()) as SharedTarget<Event>])
            .await
            .expect("initial install should succeed");
        assert_eq!(old_target.close_calls.load(Ordering::SeqCst), 0, "target must not close on first install");

        let new_target = ClosableTestTarget::new("new", "webhook");
        notifier
            .init_bucket_targets_shared(vec![Arc::new(new_target.clone()) as SharedTarget<Event>])
            .await
            .expect("replacement install should succeed");

        assert_eq!(
            old_target.close_calls.load(Ordering::SeqCst),
            1,
            "the replaced target must be closed exactly once"
        );
        assert_eq!(
            new_target.close_calls.load(Ordering::SeqCst),
            0,
            "the freshly installed target must stay open"
        );
    }

    /// A target that records `close()` invocations, for lifecycle assertions.
    #[derive(Clone)]
    struct ClosableTestTarget {
        id: TargetID,
        close_calls: Arc<AtomicUsize>,
    }

    impl ClosableTestTarget {
        fn new(id: &str, name: &str) -> Self {
            Self {
                id: TargetID::new(id.to_string(), name.to_string()),
                close_calls: Arc::new(AtomicUsize::new(0)),
            }
        }
    }

    #[async_trait]
    impl<E> Target<E> for ClosableTestTarget
    where
        E: rustfs_targets::PluginEvent,
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

        async fn close(&self) -> Result<(), TargetError> {
            self.close_calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        fn store(&self) -> Option<&(dyn Store<QueuedPayload, Error = StoreError, Key = Key> + Send + Sync)> {
            None
        }

        fn clone_dyn(&self) -> Box<dyn Target<E> + Send + Sync> {
            Box::new(self.clone())
        }

        async fn init(&self) -> Result<(), TargetError> {
            Ok(())
        }

        fn is_enabled(&self) -> bool {
            true
        }
    }
}
