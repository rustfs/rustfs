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
    Event, NotificationError,
    error::transition_join_error,
    integration::NotificationMetrics,
    notifier::{DirectDispatchTracker, SharedNotifyTargetList, shared_dispatch_gate},
};
use rustfs_targets::{
    BuiltinPluginRuntimeAdapter, OpenedActivation, PluginRuntimeAdapter, PreparedActivation, ReplayEvent, ReplayWorkerManager,
    RuntimeActivation, Target, TargetRuntimeManager,
};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::Duration;
use tokio::sync::{OwnedRwLockWriteGuard, RwLock, Semaphore};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

const LOG_COMPONENT_NOTIFY: &str = "notify";
const LOG_SUBSYSTEM_RUNTIME: &str = "runtime";
const EVENT_NOTIFY_RUNTIME_LIFECYCLE: &str = "notify_runtime_lifecycle";
const EVENT_NOTIFY_RUNTIME_SHUTDOWN_FAILED: &str = "notify_runtime_shutdown_failed";
const EVENT_NOTIFY_REPLAY_RETRY_EXHAUSTED: &str = "notify_replay_retry_exhausted";
const TARGET_CLOSE_TIMEOUT: Duration = Duration::from_secs(10);

pub(crate) struct DetachedNotifyRuntime {
    direct_dispatches: Arc<DirectDispatchTracker>,
    runtime: TargetRuntimeManager<Event>,
    replay_workers: ReplayWorkerManager,
}

// Multi-lock publication order: replay_workers -> target_list ->
// publication_gate. The lifecycle may already hold dispatch_gate while
// entering this facade; no path that needs these three locks may acquire
// publication_gate before either runtime lock.
#[derive(Clone)]
pub struct NotifyRuntimeFacade {
    dispatch_gate: Arc<RwLock<()>>,
    legacy_terminated: Arc<AtomicBool>,
    target_list: SharedNotifyTargetList,
    replay_workers: Arc<RwLock<ReplayWorkerManager>>,
    runtime_adapter: Arc<BuiltinPluginRuntimeAdapter<Event>>,
}

impl NotifyRuntimeFacade {
    pub fn new(
        target_list: SharedNotifyTargetList,
        replay_workers: Arc<RwLock<ReplayWorkerManager>>,
        concurrency_limiter: Arc<Semaphore>,
        metrics: Arc<NotificationMetrics>,
    ) -> Self {
        let dispatch_gate = shared_dispatch_gate(&target_list, None);
        Self::new_with_dispatch_gate(target_list, replay_workers, dispatch_gate, concurrency_limiter, metrics)
    }

    pub(crate) fn new_with_dispatch_gate(
        target_list: SharedNotifyTargetList,
        replay_workers: Arc<RwLock<ReplayWorkerManager>>,
        dispatch_gate: Arc<RwLock<()>>,
        concurrency_limiter: Arc<Semaphore>,
        metrics: Arc<NotificationMetrics>,
    ) -> Self {
        let dispatch_gate = shared_dispatch_gate(&target_list, Some(dispatch_gate));
        let replay_metrics = metrics;
        let runtime_adapter = BuiltinPluginRuntimeAdapter::new(
            Arc::new(move |event: ReplayEvent<Event>| {
                let metrics = replay_metrics.clone();
                Box::pin(async move {
                    match event {
                        ReplayEvent::Delivered { .. } => metrics.increment_processed(),
                        ReplayEvent::RetryableError { .. } => {}
                        ReplayEvent::RetryExhausted { detail, key, target } => {
                            warn!(
                                event = EVENT_NOTIFY_REPLAY_RETRY_EXHAUSTED,
                                component = LOG_COMPONENT_NOTIFY,
                                subsystem = LOG_SUBSYSTEM_RUNTIME,
                                target_id = %target.id(),
                                replay_key = %key,
                                error = %detail,
                                "notify replay retry budget exhausted, entry stays queued and retries"
                            );
                        }
                        ReplayEvent::Dropped { target, .. } | ReplayEvent::PermanentFailure { target, .. } => {
                            target.record_final_failure();
                            metrics.increment_failed();
                        }
                        ReplayEvent::UnreadableEntry { .. } => {}
                    }
                })
            }),
            Arc::new(|target_id, has_replay| {
                if has_replay {
                    info!(
                        event = EVENT_NOTIFY_RUNTIME_LIFECYCLE,
                        component = LOG_COMPONENT_NOTIFY,
                        subsystem = LOG_SUBSYSTEM_RUNTIME,
                        target_id = %target_id,
                        state = "replay_started",
                        "notify runtime lifecycle"
                    );
                } else {
                    debug!(
                        event = EVENT_NOTIFY_RUNTIME_LIFECYCLE,
                        component = LOG_COMPONENT_NOTIFY,
                        subsystem = LOG_SUBSYSTEM_RUNTIME,
                        target_id = %target_id,
                        state = "replay_skipped",
                        reason = "no_store_configured",
                        "notify runtime lifecycle"
                    );
                }
            }),
            Some(concurrency_limiter),
            Duration::from_secs(5),
            Duration::from_millis(500),
            "Stop event stream processing for target",
        );

        Self {
            dispatch_gate,
            legacy_terminated: Arc::new(AtomicBool::new(false)),
            target_list,
            replay_workers,
            runtime_adapter: Arc::new(runtime_adapter),
        }
    }

    pub(crate) async fn pause_dispatch(&self) -> OwnedRwLockWriteGuard<()> {
        self.dispatch_gate.clone().write_owned().await
    }

    pub async fn activate_targets_with_replay(
        &self,
        targets: Vec<Box<dyn Target<Event> + Send + Sync>>,
    ) -> RuntimeActivation<Event> {
        // The compatibility pair must not start replacement replay before
        // replace_targets has stopped and joined the current generation.
        self.runtime_adapter.prepare_dormant_compat_activation(targets).await
    }

    pub(crate) async fn prepare_targets(
        &self,
        targets: Vec<Box<dyn Target<Event> + Send + Sync>>,
        cancellation: &CancellationToken,
    ) -> PreparedActivation<Event> {
        self.runtime_adapter.prepare_targets_cancellable(targets, cancellation).await
    }

    pub(crate) async fn open_prepared_stores(
        &self,
        prepared: PreparedActivation<Event>,
    ) -> Result<(OpenedActivation<Event>, PreparedActivation<Event>), NotificationError> {
        let runtime_adapter = self.runtime_adapter.clone();
        tokio::task::spawn_blocking(move || runtime_adapter.open_prepared_stores(prepared))
            .await
            .map_err(transition_join_error)
    }

    pub(crate) async fn commit_prepared<Committed>(
        &self,
        opened: OpenedActivation<Event>,
        on_committed: Committed,
    ) -> (DetachedNotifyRuntime, PreparedActivation<Event>)
    where
        Committed: FnOnce(bool),
    {
        // The lifecycle coordinator validates the generation immediately before
        // entering the non-cancellable handoff. Once old replay workers have
        // been joined, this generation must publish before a later accepted
        // intent can run; abandoning it here would leave the old runtime
        // visible without replay workers.
        let mut replay_workers = self.replay_workers.write().await;
        let mut target_list = self.target_list.write().await;

        let (activation, rejected) = self.runtime_adapter.try_activate_prepared(opened);
        let fully_activated = rejected.failure_summary().is_none();
        let (runtime, replay_workers, direct_dispatches) =
            Self::swap_activation(&mut target_list, &mut replay_workers, activation);
        on_committed(fully_activated);
        (
            DetachedNotifyRuntime {
                direct_dispatches,
                runtime,
                replay_workers,
            },
            rejected,
        )
    }

    pub(crate) async fn commit_disabled<Committed>(&self, on_committed: Committed) -> DetachedNotifyRuntime
    where
        Committed: FnOnce(),
    {
        // Lock order: replay_workers -> target_list. The lifecycle coordinator
        // crosses its publication barrier before stopping the old workers, so
        // this non-cancellable commit must publish even if a newer intent was
        // accepted while the workers joined.
        let mut replay_workers = self.replay_workers.write().await;
        let mut target_list = self.target_list.write().await;

        let (runtime, direct_dispatches) = target_list.replace_runtime(TargetRuntimeManager::new());
        let detached = DetachedNotifyRuntime {
            direct_dispatches,
            runtime,
            replay_workers: std::mem::take(&mut *replay_workers),
        };
        on_committed();
        detached
    }

    pub async fn replace_targets(&self, mut activation: RuntimeActivation<Event>) -> Result<(), NotificationError> {
        // A caller may supply an activation created outside the compatibility
        // prepare method. Stop any already-running replacement workers before
        // entering the ordered handoff; the supported activate→replace pair is
        // dormant here and therefore never overlaps the old generation.
        self.runtime_adapter.stop_replay_workers(&mut activation.replay_workers).await;
        let dispatch_guard = self.pause_dispatch().await;
        if self.legacy_terminated.load(Ordering::Acquire) {
            drop(dispatch_guard);
            self.runtime_adapter
                .close_compat_activation(activation)
                .await
                .map_err(NotificationError::Target)?;
            return Err(NotificationError::Initialization("Notification runtime has terminated".to_string()));
        }
        self.stop_active_replay_workers().await;
        let (activation, open_rejected, activation_rejected) = self.runtime_adapter.start_dormant_compat_activation(activation);
        let activation_failures = [open_rejected.failure_summary(), activation_rejected.failure_summary()]
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        let (old_runtime, old_replay_workers, old_direct_dispatches) = {
            // Lock order: replay_workers -> target_list (matches notify AGENTS.md).
            let mut replay_workers = self.replay_workers.write().await;
            let mut target_list = self.target_list.write().await;
            Self::swap_activation(&mut target_list, &mut replay_workers, activation)
        };
        drop(dispatch_guard);
        let close_old = self.close_detached_targets(DetachedNotifyRuntime {
            direct_dispatches: old_direct_dispatches,
            runtime: old_runtime,
            replay_workers: old_replay_workers,
        });
        let close_open_rejected = self.close_prepared(open_rejected);
        let close_activation_rejected = self.close_prepared(activation_rejected);
        let (close_old, close_open_rejected, close_activation_rejected) =
            tokio::join!(close_old, close_open_rejected, close_activation_rejected);
        close_old?;
        close_open_rejected?;
        close_activation_rejected?;
        if !activation_failures.is_empty() {
            return Err(NotificationError::Initialization(format!(
                "one or more notification targets failed to activate: {}",
                activation_failures.join("; ")
            )));
        }
        Ok(())
    }

    pub async fn stop_replay_workers(&self) {
        let _dispatch_guard = self.pause_dispatch().await;
        self.stop_active_replay_workers().await;
    }

    pub(crate) async fn stop_active_replay_workers(&self) {
        let mut detached = {
            let mut replay_workers = self.replay_workers.write().await;
            std::mem::take(&mut *replay_workers)
        };
        self.runtime_adapter.stop_replay_workers(&mut detached).await;
    }

    pub async fn shutdown(&self) {
        let _ = self.shutdown_checked().await;
    }

    pub async fn shutdown_checked(&self) -> Result<(), NotificationError> {
        let _dispatch_guard = self.pause_dispatch().await;
        self.legacy_terminated.store(true, Ordering::Release);
        info!(
            event = EVENT_NOTIFY_RUNTIME_LIFECYCLE,
            component = LOG_COMPONENT_NOTIFY,
            subsystem = LOG_SUBSYSTEM_RUNTIME,
            state = "stopping",
            "notify runtime lifecycle"
        );

        let (detached_runtime, detached_replay_workers, detached_direct_dispatches) = {
            // Lock order: replay_workers -> target_list (matches notify AGENTS.md).
            let mut replay_workers = self.replay_workers.write().await;
            let mut target_list = self.target_list.write().await;
            let (runtime, direct_dispatches) = target_list.replace_runtime(TargetRuntimeManager::new());
            (runtime, std::mem::take(&mut *replay_workers), direct_dispatches)
        };
        let active_targets = detached_replay_workers.len();
        info!(
            event = EVENT_NOTIFY_RUNTIME_LIFECYCLE,
            component = LOG_COMPONENT_NOTIFY,
            subsystem = LOG_SUBSYSTEM_RUNTIME,
            state = "replay_stopping",
            active_targets,
            "notify runtime lifecycle"
        );

        let shutdown_result = self
            .close_detached(DetachedNotifyRuntime {
                direct_dispatches: detached_direct_dispatches,
                runtime: detached_runtime,
                replay_workers: detached_replay_workers,
            })
            .await;
        if let Err(err) = &shutdown_result {
            tracing::error!(
                event = EVENT_NOTIFY_RUNTIME_SHUTDOWN_FAILED,
                component = LOG_COMPONENT_NOTIFY,
                subsystem = LOG_SUBSYSTEM_RUNTIME,
                error = %err,
                "Failed to shutdown notify runtime cleanly"
            );
        }

        info!(
            event = EVENT_NOTIFY_RUNTIME_LIFECYCLE,
            component = LOG_COMPONENT_NOTIFY,
            subsystem = LOG_SUBSYSTEM_RUNTIME,
            state = "stopped",
            "notify runtime lifecycle"
        );
        shutdown_result
    }

    fn swap_activation(
        target_list: &mut crate::notifier::TargetList,
        replay_workers: &mut ReplayWorkerManager,
        activation: RuntimeActivation<Event>,
    ) -> (TargetRuntimeManager<Event>, ReplayWorkerManager, Arc<DirectDispatchTracker>) {
        let mut replacement = TargetRuntimeManager::new();
        for target in activation.targets {
            replacement.add_arc(target);
        }
        let (runtime, direct_dispatches) = target_list.replace_runtime(replacement);
        (runtime, std::mem::replace(replay_workers, activation.replay_workers), direct_dispatches)
    }

    pub(crate) async fn stop_detached_replay(&self, detached: &mut DetachedNotifyRuntime) {
        // Replay join is intentionally not wrapped in the target-close timeout:
        // returning before a worker is confirmed stopped could let a
        // replacement drain the same persistent queue concurrently.
        self.runtime_adapter.stop_replay_workers(&mut detached.replay_workers).await;
    }

    pub(crate) async fn close_detached_targets(&self, mut detached: DetachedNotifyRuntime) -> Result<(), NotificationError> {
        // Direct sends are allowed to finish after the replacement runtime is
        // published, but the old targets must remain open until every task that
        // selected that generation has released its lease.
        detached.direct_dispatches.wait_idle().await;
        match tokio::time::timeout(TARGET_CLOSE_TIMEOUT, detached.runtime.clear_and_close()).await {
            Ok(close_errors) if close_errors.is_empty() => Ok(()),
            Ok(close_errors) => {
                let targets = close_errors
                    .into_iter()
                    .map(|(target_id, _)| target_id)
                    .collect::<Vec<_>>()
                    .join("; ");
                Err(NotificationError::Target(rustfs_targets::TargetError::Storage(format!(
                    "Failed to close {targets}"
                ))))
            }
            Err(_) => Err(NotificationError::Target(rustfs_targets::TargetError::Timeout(
                "Timed out closing replaced notification targets".to_string(),
            ))),
        }
    }

    async fn close_detached(&self, mut detached: DetachedNotifyRuntime) -> Result<(), NotificationError> {
        self.stop_detached_replay(&mut detached).await;
        self.close_detached_targets(detached).await
    }

    pub(crate) async fn close_prepared(&self, prepared: PreparedActivation<Event>) -> Result<(), NotificationError> {
        match tokio::time::timeout(TARGET_CLOSE_TIMEOUT, self.runtime_adapter.close_prepared(prepared)).await {
            Ok(result) => result.map_err(NotificationError::Target),
            Err(_) => Err(NotificationError::Target(rustfs_targets::TargetError::Timeout(
                "Timed out closing superseded notification targets".to_string(),
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::NotifyRuntimeFacade;
    use crate::{
        Event, integration::NotificationMetrics, notifier::EventNotifier, rule_engine::NotifyRuleEngine,
        runtime_view::NotifyRuntimeView,
    };
    use async_trait::async_trait;
    use rustfs_targets::arn::TargetID;
    use rustfs_targets::store::{Key, QueueStore, Store};
    use rustfs_targets::target::{EntityTarget, QueuedPayload, QueuedPayloadMeta};
    use rustfs_targets::{ReplayWorkerManager, SharedTarget, StoreError, Target, TargetError};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::{Notify, RwLock, Semaphore};

    #[derive(Clone)]
    struct TestTarget {
        close_entered: Option<Arc<Notify>>,
        close_error: bool,
        close_release: Option<Arc<Notify>>,
        close_calls: Arc<AtomicUsize>,
        id: TargetID,
        store: Option<QueueStore<QueuedPayload>>,
    }

    impl TestTarget {
        fn new(id: &str, name: &str) -> Self {
            Self {
                close_entered: None,
                close_error: false,
                close_release: None,
                close_calls: Arc::new(AtomicUsize::new(0)),
                id: TargetID::new(id.to_string(), name.to_string()),
                store: None,
            }
        }

        fn with_blocking_close(mut self, entered: Arc<Notify>, release: Arc<Notify>) -> Self {
            self.close_entered = Some(entered);
            self.close_release = Some(release);
            self
        }

        fn with_close_error(mut self) -> Self {
            self.close_error = true;
            self
        }

        fn with_store(mut self, store: QueueStore<QueuedPayload>) -> Self {
            self.store = Some(store);
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
            if let Some(entered) = &self.close_entered {
                entered.notify_one();
            }
            if let Some(release) = &self.close_release {
                release.notified().await;
            }
            if self.close_error {
                return Err(TargetError::Storage("forced close failure".to_string()));
            }
            Ok(())
        }

        fn store(&self) -> Option<&(dyn Store<QueuedPayload, Error = StoreError, Key = Key> + Send + Sync)> {
            self.store
                .as_ref()
                .map(|store| store as &(dyn Store<QueuedPayload, Error = StoreError, Key = Key> + Send + Sync))
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

    fn build_facade() -> (NotifyRuntimeFacade, Arc<EventNotifier>, Arc<RwLock<ReplayWorkerManager>>) {
        let metrics = Arc::new(NotificationMetrics::new());
        let notifier = Arc::new(EventNotifier::new(metrics.clone(), NotifyRuleEngine::new()));
        let target_list = notifier.target_list();
        let replay_workers = Arc::new(RwLock::new(ReplayWorkerManager::new()));
        let facade = NotifyRuntimeFacade::new_with_dispatch_gate(
            target_list,
            replay_workers.clone(),
            notifier.dispatch_gate(),
            Arc::new(Semaphore::new(4)),
            metrics,
        );
        (facade, notifier, replay_workers)
    }

    #[tokio::test]
    async fn runtime_facade_stops_empty_replay_workers() {
        let (facade, _, _) = build_facade();
        facade.stop_replay_workers().await;
    }

    #[tokio::test]
    async fn runtime_facade_activates_empty_target_list() {
        let (facade, _, _) = build_facade();
        let activation = facade.activate_targets_with_replay(Vec::new()).await;

        assert!(activation.targets.is_empty());
        assert_eq!(activation.replay_workers.len(), 0);
    }

    #[tokio::test]
    async fn compatibility_activation_stays_dormant_until_ordered_replace() {
        let (facade, _, replay_workers) = build_facade();
        let queue_root = tempfile::tempdir().expect("queue root");
        let store = QueueStore::new_with_compression(queue_root.path(), 16, ".event", false);
        let target = TestTarget::new("primary", "webhook").with_store(store);

        let activation = facade.activate_targets_with_replay(vec![Box::new(target)]).await;
        assert_eq!(activation.targets.len(), 1);
        assert_eq!(activation.replay_workers.len(), 0, "compatibility prepare must not start replay early");
        assert_eq!(replay_workers.read().await.len(), 0);

        facade
            .replace_targets(activation)
            .await
            .expect("ordered compatibility replace should succeed");
        assert_eq!(replay_workers.read().await.len(), 1);
        facade.shutdown_checked().await.expect("test runtime should shut down");
    }

    #[tokio::test]
    async fn runtime_facade_replace_targets_commits_runtime_state() {
        let (facade, notifier, replay_workers) = build_facade();
        let target = TestTarget::new("primary", "webhook");
        let activation = rustfs_targets::RuntimeActivation {
            replay_workers: ReplayWorkerManager::new(),
            targets: vec![Arc::new(target) as SharedTarget<Event>],
        };

        facade
            .replace_targets(activation)
            .await
            .expect("replace_targets should succeed");

        let runtime_view = NotifyRuntimeView::new(notifier.target_list(), replay_workers.clone());
        let active_targets = runtime_view.get_active_targets().await;
        assert_eq!(active_targets, vec![TargetID::new("primary".to_string(), "webhook".to_string())]);
        assert_eq!(replay_workers.read().await.len(), 0);
    }

    #[tokio::test]
    async fn runtime_queries_do_not_wait_for_target_close() {
        let (facade, notifier, replay_workers) = build_facade();
        let close_entered = Arc::new(Notify::new());
        let close_release = Arc::new(Notify::new());
        let target = TestTarget::new("primary", "webhook").with_blocking_close(close_entered.clone(), close_release.clone());
        facade
            .replace_targets(rustfs_targets::RuntimeActivation {
                replay_workers: ReplayWorkerManager::new(),
                targets: vec![Arc::new(target) as SharedTarget<Event>],
            })
            .await
            .expect("target install should succeed");

        let shutdown = tokio::spawn({
            let facade = facade.clone();
            async move { facade.shutdown_checked().await }
        });
        close_entered.notified().await;

        let target_list = notifier.target_list();
        assert!(target_list.try_read().is_ok(), "target list lock must not be held during close");
        assert!(replay_workers.try_read().is_ok(), "replay manager lock must not be held during close");
        close_release.notify_one();
        shutdown
            .await
            .expect("shutdown task should not panic")
            .expect("shutdown should succeed");

        let snapshot = NotifyRuntimeView::new(target_list, replay_workers)
            .runtime_status_snapshot()
            .await;
        assert_eq!(snapshot.target_count, 0);
        assert_eq!(snapshot.replay_worker_count, 0);
    }

    #[tokio::test]
    async fn runtime_locks_are_released_while_replay_worker_joins() {
        let (facade, notifier, replay_workers) = build_facade();
        let cancel_received = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let (cancel_tx, mut cancel_rx) = tokio::sync::mpsc::channel(1);
        let join = tokio::spawn({
            let cancel_received = cancel_received.clone();
            let release = release.clone();
            async move {
                let _ = cancel_rx.recv().await;
                cancel_received.notify_one();
                release.notified().await;
            }
        });
        replay_workers
            .write()
            .await
            .insert_with_handle("blocked-worker".to_string(), cancel_tx, join);

        let stop = tokio::spawn({
            let facade = facade.clone();
            async move { facade.stop_replay_workers().await }
        });
        cancel_received.notified().await;

        assert!(replay_workers.try_read().is_ok(), "replay manager lock must not be held during join");
        let target_list = notifier.target_list();
        assert!(target_list.try_read().is_ok(), "target list lock must not be held during replay join");

        release.notify_one();
        stop.await.expect("stop task should finish");
    }

    #[tokio::test]
    async fn shutdown_returns_close_error_after_detaching_runtime() {
        let (facade, notifier, replay_workers) = build_facade();
        let target = TestTarget::new("primary", "webhook").with_close_error();
        facade
            .replace_targets(rustfs_targets::RuntimeActivation {
                replay_workers: ReplayWorkerManager::new(),
                targets: vec![Arc::new(target) as SharedTarget<Event>],
            })
            .await
            .expect("target install should succeed");

        let err = facade.shutdown_checked().await.expect_err("close failure should propagate");
        assert!(matches!(err, crate::NotificationError::Target(TargetError::Storage(_))));

        let snapshot = NotifyRuntimeView::new(notifier.target_list(), replay_workers)
            .runtime_status_snapshot()
            .await;
        assert_eq!(snapshot.target_count, 0);
        assert_eq!(snapshot.replay_worker_count, 0);
    }

    #[tokio::test(start_paused = true)]
    async fn shutdown_bounds_a_target_that_never_closes() {
        let (facade, notifier, replay_workers) = build_facade();
        let close_entered = Arc::new(Notify::new());
        let never_release = Arc::new(Notify::new());
        let target = TestTarget::new("primary", "webhook").with_blocking_close(close_entered.clone(), never_release);
        facade
            .replace_targets(rustfs_targets::RuntimeActivation {
                replay_workers: ReplayWorkerManager::new(),
                targets: vec![Arc::new(target) as SharedTarget<Event>],
            })
            .await
            .expect("target install should succeed");

        let shutdown = tokio::spawn({
            let facade = facade.clone();
            async move { facade.shutdown_checked().await }
        });
        close_entered.notified().await;
        tokio::time::advance(super::TARGET_CLOSE_TIMEOUT).await;

        let err = shutdown
            .await
            .expect("shutdown task should not panic")
            .expect_err("blocked close must time out");
        assert!(matches!(err, crate::NotificationError::Target(TargetError::Timeout(_))));
        let snapshot = NotifyRuntimeView::new(notifier.target_list(), replay_workers)
            .runtime_status_snapshot()
            .await;
        assert_eq!(snapshot.target_count, 0);
        assert_eq!(snapshot.replay_worker_count, 0);
    }
}
