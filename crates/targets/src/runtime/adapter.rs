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

use super::{
    OpenedActivation, PrepareTargetResult, PreparedActivation, ReplayEvent, ReplayWorkerManager, RuntimeActivation,
    RuntimeStatusSnapshot, RuntimeTargetHealthSnapshot, TargetActivationFailure, TargetRuntimeManager, prepare_target,
    start_replay_worker,
};
use crate::plugin::PluginEvent;
use crate::{SharedTarget, Target, TargetError};
use async_trait::async_trait;
use rayon::prelude::*;
use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::Pin;
use std::sync::{Arc, LazyLock};
use std::time::Duration;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

type ReplayHook<E> = Arc<dyn Fn(ReplayEvent<E>) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;
type ReplayStartObserver = Arc<dyn Fn(&str, bool) + Send + Sync>;

const MAX_PARALLEL_STORE_OPENS: usize = 4;

static STORE_OPEN_POOL: LazyLock<Result<rayon::ThreadPool, rayon::ThreadPoolBuildError>> = LazyLock::new(|| {
    rayon::ThreadPoolBuilder::new()
        .num_threads(MAX_PARALLEL_STORE_OPENS)
        .thread_name(|index| format!("rustfs-target-store-open-{index}"))
        .build()
});

enum StoreOpenOutcome<E>
where
    E: PluginEvent,
{
    Accepted(SharedTarget<E>),
    Rejected { panicked: bool, target: SharedTarget<E> },
}

fn open_target_store<E>(target: SharedTarget<E>) -> StoreOpenOutcome<E>
where
    E: PluginEvent,
{
    match catch_unwind(AssertUnwindSafe(|| target.store().map(|store| store.open()))) {
        Ok(None | Some(Ok(()))) => StoreOpenOutcome::Accepted(target),
        Ok(Some(Err(_))) => StoreOpenOutcome::Rejected { panicked: false, target },
        Err(_) => StoreOpenOutcome::Rejected { panicked: true, target },
    }
}

/// Shared runtime contract for target plugins.
#[async_trait]
pub trait PluginRuntimeAdapter<E>: Send + Sync
where
    E: PluginEvent,
{
    async fn activate_with_replay(&self, targets: Vec<Box<dyn Target<E> + Send + Sync>>) -> RuntimeActivation<E>;

    async fn replace_runtime_targets(
        &self,
        runtime: &mut TargetRuntimeManager<E>,
        replay_workers: &mut ReplayWorkerManager,
        activation: RuntimeActivation<E>,
    ) -> Result<(), TargetError>;

    async fn stop_replay_workers(&self, replay_workers: &mut ReplayWorkerManager);

    fn snapshot_runtime_status(
        &self,
        runtime: &TargetRuntimeManager<E>,
        replay_workers: &ReplayWorkerManager,
    ) -> RuntimeStatusSnapshot;

    async fn snapshot_runtime_health(&self, runtime: &TargetRuntimeManager<E>) -> Vec<RuntimeTargetHealthSnapshot>;

    async fn shutdown(
        &self,
        runtime: &mut TargetRuntimeManager<E>,
        replay_workers: &mut ReplayWorkerManager,
    ) -> Result<(), TargetError>;
}

/// Built-in in-process runtime adapter that preserves the current replay and
/// activation behavior while presenting a stable runtime contract to callers.
#[derive(Clone)]
pub struct BuiltinPluginRuntimeAdapter<E>
where
    E: PluginEvent,
{
    replay_hook: ReplayHook<E>,
    replay_start_observer: ReplayStartObserver,
    replay_semaphore: Option<Arc<Semaphore>>,
    batch_timeout: Duration,
    idle_sleep: Duration,
    stop_log_prefix: Arc<str>,
}

impl<E> BuiltinPluginRuntimeAdapter<E>
where
    E: PluginEvent,
{
    pub fn new(
        replay_hook: ReplayHook<E>,
        replay_start_observer: ReplayStartObserver,
        replay_semaphore: Option<Arc<Semaphore>>,
        batch_timeout: Duration,
        idle_sleep: Duration,
        stop_log_prefix: impl Into<Arc<str>>,
    ) -> Self {
        Self {
            replay_hook,
            replay_start_observer,
            replay_semaphore,
            batch_timeout,
            idle_sleep,
            stop_log_prefix: stop_log_prefix.into(),
        }
    }

    pub async fn prepare_targets(&self, targets: Vec<Box<dyn Target<E> + Send + Sync>>) -> PreparedActivation<E> {
        self.prepare_targets_inner(targets, None).await
    }

    pub async fn prepare_targets_cancellable(
        &self,
        targets: Vec<Box<dyn Target<E> + Send + Sync>>,
        cancellation: &CancellationToken,
    ) -> PreparedActivation<E> {
        self.prepare_targets_inner(targets, Some(cancellation)).await
    }

    async fn prepare_targets_inner(
        &self,
        targets: Vec<Box<dyn Target<E> + Send + Sync>>,
        cancellation: Option<&CancellationToken>,
    ) -> PreparedActivation<E> {
        let mut prepared = Vec::with_capacity(targets.len());
        let mut failures = Vec::new();
        let mut rejected_targets = Vec::new();
        let mut targets = targets.into_iter();
        while let Some(target) = targets.next() {
            match prepare_target(target, cancellation).await {
                PrepareTargetResult::Ready(target) => prepared.push(target),
                PrepareTargetResult::Degraded { error, target } => {
                    drop(error);
                    tracing::warn!(
                        target_id = %target.id(),
                        reason = "initialization_failed",
                        "Target initialization failed during lifecycle preparation"
                    );
                    failures.push(TargetActivationFailure {
                        detail: format!("{}: initialization failed", target.id()),
                    });
                    prepared.push(target);
                }
                PrepareTargetResult::Failed { error, target } => {
                    drop(error);
                    let target_id = target.id().to_string();
                    tracing::warn!(
                        target_id,
                        reason = "initialization_failed",
                        "Target initialization failed during lifecycle preparation"
                    );
                    failures.push(TargetActivationFailure {
                        detail: format!("{target_id}: initialization failed"),
                    });
                    rejected_targets.push(Arc::from(target));
                }
                PrepareTargetResult::Cancelled(target) => {
                    prepared.push(Arc::from(target));
                    prepared.extend(targets.map(Arc::from));
                    break;
                }
            }
        }
        PreparedActivation {
            failures,
            rejected_targets,
            targets: prepared,
        }
    }

    /// Opens queue stores only after the previous runtime generation has been
    /// quiesced. Targets whose stores cannot be opened retain the established
    /// fault-isolation behavior and are returned for lock-free shutdown.
    pub fn open_prepared_stores(&self, prepared: PreparedActivation<E>) -> (OpenedActivation<E>, PreparedActivation<E>) {
        let mut accepted = Vec::with_capacity(prepared.targets.len());
        let mut failures = prepared.failures;
        let mut rejected = prepared.rejected_targets;
        let outcomes = if prepared.targets.len() < 2 {
            prepared.targets.into_iter().map(open_target_store).collect()
        } else {
            match STORE_OPEN_POOL.as_ref() {
                // Vec's indexed parallel iterator preserves configuration
                // order in collect, keeping failure summaries deterministic.
                Ok(pool) => pool.install(|| prepared.targets.into_par_iter().map(open_target_store).collect::<Vec<_>>()),
                Err(err) => {
                    tracing::warn!(error = %err, "Failed to create target store open pool; opening stores serially");
                    prepared.targets.into_iter().map(open_target_store).collect()
                }
            }
        };
        for outcome in outcomes {
            match outcome {
                StoreOpenOutcome::Accepted(target) => accepted.push(target),
                StoreOpenOutcome::Rejected { panicked, target } => {
                    if panicked {
                        tracing::error!(
                            target_id = %target.id(),
                            reason = "store_open_panicked",
                            "Target queue store panicked while opening during runtime handoff"
                        );
                    } else {
                        tracing::error!(
                            target_id = %target.id(),
                            reason = "store_open_failed",
                            "Failed to open target queue store during runtime handoff"
                        );
                    }
                    failures.push(TargetActivationFailure {
                        detail: format!("{}: queue store open failed", target.id()),
                    });
                    rejected.push(target);
                }
            }
        }
        (
            OpenedActivation { targets: accepted },
            PreparedActivation {
                failures,
                rejected_targets: rejected,
                targets: Vec::new(),
            },
        )
    }

    pub fn try_activate_prepared(&self, opened: OpenedActivation<E>) -> (RuntimeActivation<E>, PreparedActivation<E>) {
        let mut replay_workers = ReplayWorkerManager::new();
        let mut accepted = Vec::with_capacity(opened.targets.len());
        let mut failures = Vec::new();
        let mut rejected_targets = Vec::new();
        for target in opened.targets {
            let target_id = target.id().to_string();
            let replay = catch_unwind(AssertUnwindSafe(|| {
                target.store().filter(|_| target.is_enabled()).map(|store| {
                    start_replay_worker(
                        store.boxed_clone(),
                        Arc::clone(&target),
                        Arc::clone(&self.replay_hook),
                        self.replay_semaphore.clone(),
                        self.batch_timeout,
                        self.idle_sleep,
                    )
                })
            }));
            let replay = match replay {
                Ok(replay) => replay,
                Err(_) => {
                    tracing::error!(
                        target_id,
                        reason = "replay_activation_panicked",
                        "Target replay activation panicked during runtime handoff"
                    );
                    failures.push(TargetActivationFailure {
                        detail: format!("{target_id}: replay activation failed"),
                    });
                    rejected_targets.push(target);
                    continue;
                }
            };
            (self.replay_start_observer)(&target_id, replay.is_some());
            if let Some((cancel_tx, join)) = replay {
                replay_workers.insert_with_handle(target_id, cancel_tx, join);
            }
            accepted.push(target);
        }

        (
            RuntimeActivation {
                replay_workers,
                targets: accepted,
            },
            PreparedActivation {
                failures,
                rejected_targets,
                targets: Vec::new(),
            },
        )
    }

    #[doc(hidden)]
    pub async fn prepare_dormant_compat_activation(
        &self,
        targets: Vec<Box<dyn Target<E> + Send + Sync>>,
    ) -> RuntimeActivation<E> {
        let PreparedActivation {
            failures,
            rejected_targets,
            targets,
        } = self.prepare_targets(targets).await;
        let rejected = PreparedActivation {
            failures,
            rejected_targets,
            targets: Vec::new(),
        };
        if let Err(err) = self.close_prepared(rejected).await {
            tracing::warn!(error = %err, "Failed to close targets rejected while preparing compatibility activation");
        }
        RuntimeActivation {
            replay_workers: ReplayWorkerManager::new(),
            targets,
        }
    }

    #[doc(hidden)]
    pub fn start_dormant_compat_activation(
        &self,
        activation: RuntimeActivation<E>,
    ) -> (RuntimeActivation<E>, PreparedActivation<E>, PreparedActivation<E>) {
        let prepared = PreparedActivation {
            failures: Vec::new(),
            rejected_targets: Vec::new(),
            targets: activation.targets,
        };
        let (opened, open_rejected) = self.open_prepared_stores(prepared);
        let (activation, activation_rejected) = self.try_activate_prepared(opened);
        (activation, open_rejected, activation_rejected)
    }

    #[doc(hidden)]
    pub async fn close_compat_activation(&self, mut activation: RuntimeActivation<E>) -> Result<(), TargetError> {
        let mut runtime = TargetRuntimeManager::new();
        for target in activation.targets {
            runtime.add_arc(target);
        }
        self.shutdown(&mut runtime, &mut activation.replay_workers).await
    }

    pub async fn close_prepared(&self, prepared: PreparedActivation<E>) -> Result<(), TargetError> {
        let mut runtime = TargetRuntimeManager::new();
        for target in prepared.targets.into_iter().chain(prepared.rejected_targets) {
            runtime.add_arc(target);
        }
        let mut replay_workers = ReplayWorkerManager::new();
        self.shutdown(&mut runtime, &mut replay_workers).await
    }
}

#[async_trait]
impl<E> PluginRuntimeAdapter<E> for BuiltinPluginRuntimeAdapter<E>
where
    E: PluginEvent,
{
    async fn activate_with_replay(&self, targets: Vec<Box<dyn Target<E> + Send + Sync>>) -> RuntimeActivation<E> {
        let prepared = self.prepare_targets(targets).await;
        let (opened, rejected) = self.open_prepared_stores(prepared);
        if let Err(err) = self.close_prepared(rejected).await {
            tracing::warn!(error = %err, "Failed to close targets whose queue stores could not be opened");
        }
        let (activation, rejected) = self.try_activate_prepared(opened);
        if let Err(err) = self.close_prepared(rejected).await {
            tracing::warn!(error = %err, "Failed to close targets rejected during replay activation");
        }
        activation
    }

    async fn replace_runtime_targets(
        &self,
        runtime: &mut TargetRuntimeManager<E>,
        replay_workers: &mut ReplayWorkerManager,
        activation: RuntimeActivation<E>,
    ) -> Result<(), TargetError> {
        // Stop (and join) the old replay workers before installing the new set so
        // no two workers ever drain the same store concurrently, then close the
        // old targets. A close failure during reload is logged but does not abort
        // the reload — the new configuration must still take effect.
        self.stop_replay_workers(replay_workers).await;
        let close_errors = runtime.clear_and_close().await;
        if !close_errors.is_empty() {
            tracing::warn!(failed_targets = close_errors.len(), "Some targets failed to close during runtime reload");
        }

        for target in activation.targets {
            runtime.add_arc(target);
        }

        *replay_workers = activation.replay_workers;
        Ok(())
    }

    async fn stop_replay_workers(&self, replay_workers: &mut ReplayWorkerManager) {
        replay_workers.stop_all(&self.stop_log_prefix).await;
    }

    fn snapshot_runtime_status(
        &self,
        runtime: &TargetRuntimeManager<E>,
        replay_workers: &ReplayWorkerManager,
    ) -> RuntimeStatusSnapshot {
        runtime.status_snapshot(replay_workers)
    }

    async fn snapshot_runtime_health(&self, runtime: &TargetRuntimeManager<E>) -> Vec<RuntimeTargetHealthSnapshot> {
        runtime.health_snapshots().await
    }

    async fn shutdown(
        &self,
        runtime: &mut TargetRuntimeManager<E>,
        replay_workers: &mut ReplayWorkerManager,
    ) -> Result<(), TargetError> {
        // On explicit shutdown, propagate any close/flush failures instead of
        // swallowing them: the runtime is still fully torn down, but the caller
        // learns that a target could not be flushed/closed cleanly.
        self.stop_replay_workers(replay_workers).await;
        let close_errors = runtime.clear_and_close().await;
        if !close_errors.is_empty() {
            let detail = close_errors
                .into_iter()
                .map(|(target_id, _)| target_id)
                .collect::<Vec<_>>()
                .join("; ");
            return Err(TargetError::Storage(format!("Failed to close {detail}")));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{BuiltinPluginRuntimeAdapter, MAX_PARALLEL_STORE_OPENS, PluginRuntimeAdapter};
    use crate::PluginEvent;
    use crate::arn::TargetID;
    use crate::store::{Key, QueueStore, Store};
    use crate::target::{EntityTarget, QueuedPayload, QueuedPayloadMeta};
    use crate::{StoreError, Target, TargetError};
    use async_trait::async_trait;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Condvar, Mutex};
    use std::time::Duration;
    use tempfile::tempdir;
    use tokio::sync::Notify;
    use tokio_util::sync::CancellationToken;

    type TestStore = dyn Store<QueuedPayload, Error = StoreError, Key = Key> + Send + Sync;

    type BeforeOpen = Arc<dyn Fn() + Send + Sync>;

    #[derive(Clone)]
    struct TestOpenStore {
        before_clone: BeforeOpen,
        before_open: BeforeOpen,
        store: QueueStore<QueuedPayload>,
    }

    impl Store<QueuedPayload> for TestOpenStore {
        type Error = StoreError;
        type Key = Key;

        fn open(&self) -> Result<(), Self::Error> {
            (self.before_open)();
            self.store.open()
        }

        fn put(&self, item: Arc<QueuedPayload>) -> Result<Self::Key, Self::Error> {
            self.store.put(item)
        }

        fn put_multiple(&self, items: Vec<QueuedPayload>) -> Result<Self::Key, Self::Error> {
            self.store.put_multiple(items)
        }

        fn put_raw(&self, data: &[u8]) -> Result<Self::Key, Self::Error> {
            self.store.put_raw(data)
        }

        fn get(&self, key: &Self::Key) -> Result<QueuedPayload, Self::Error> {
            self.store.get(key)
        }

        fn get_multiple(&self, key: &Self::Key) -> Result<Vec<QueuedPayload>, Self::Error> {
            self.store.get_multiple(key)
        }

        fn get_raw(&self, key: &Self::Key) -> Result<Vec<u8>, Self::Error> {
            self.store.get_raw(key)
        }

        fn del(&self, key: &Self::Key) -> Result<(), Self::Error> {
            self.store.del(key)
        }

        fn delete(&self) -> Result<(), Self::Error> {
            self.store.delete()
        }

        fn list(&self) -> Vec<Self::Key> {
            self.store.list()
        }

        fn len(&self) -> usize {
            self.store.len()
        }

        fn is_empty(&self) -> bool {
            self.store.is_empty()
        }

        fn boxed_clone(&self) -> Box<dyn Store<QueuedPayload, Error = Self::Error, Key = Self::Key> + Send + Sync> {
            (self.before_clone)();
            Box::new(self.clone())
        }
    }

    #[derive(Default)]
    struct StoreOpenGate {
        changed: Condvar,
        state: Mutex<StoreOpenGateState>,
    }

    #[derive(Default)]
    struct StoreOpenGateState {
        active: usize,
        max_active: usize,
        released: bool,
    }

    impl StoreOpenGate {
        fn enter(&self) {
            let mut state = self.state.lock().unwrap_or_else(|err| err.into_inner());
            state.active += 1;
            state.max_active = state.max_active.max(state.active);
            self.changed.notify_all();
            while !state.released {
                state = self.changed.wait(state).unwrap_or_else(|err| err.into_inner());
            }
            state.active -= 1;
        }

        fn wait_for_active(&self, expected: usize, timeout: Duration) -> bool {
            let state = self.state.lock().unwrap_or_else(|err| err.into_inner());
            let (state, _) = self
                .changed
                .wait_timeout_while(state, timeout, |state| state.max_active < expected)
                .unwrap_or_else(|err| err.into_inner());
            state.max_active >= expected
        }

        fn release(&self) {
            let mut state = self.state.lock().unwrap_or_else(|err| err.into_inner());
            state.released = true;
            self.changed.notify_all();
        }

        fn max_active(&self) -> usize {
            self.state.lock().unwrap_or_else(|err| err.into_inner()).max_active
        }
    }

    #[derive(Clone)]
    struct TestTarget {
        close_calls: Arc<AtomicUsize>,
        id: TargetID,
        init_calls: Arc<AtomicUsize>,
        init_entered: Option<Arc<Notify>>,
        init_fails: bool,
        store: Option<Arc<TestStore>>,
    }

    impl TestTarget {
        fn new(id: &str, name: &str) -> Self {
            Self {
                close_calls: Arc::new(AtomicUsize::new(0)),
                id: TargetID::new(id.to_string(), name.to_string()),
                init_calls: Arc::new(AtomicUsize::new(0)),
                init_entered: None,
                init_fails: false,
                store: None,
            }
        }

        fn with_failed_init(mut self) -> Self {
            self.init_fails = true;
            self
        }

        fn with_pending_init(mut self, init_entered: Arc<Notify>) -> Self {
            self.init_entered = Some(init_entered);
            self
        }

        fn with_store(mut self) -> Self {
            let dir = tempdir().expect("tempdir should be created for queue store tests");
            let store = QueueStore::<QueuedPayload>::new(dir.path(), 16, ".queue");
            store.open().expect("queue store should open");
            self.store = Some(Arc::new(store));
            self
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
            self.store.as_deref()
        }

        fn clone_dyn(&self) -> Box<dyn Target<E> + Send + Sync> {
            Box::new(self.clone())
        }

        async fn init(&self) -> Result<(), TargetError> {
            self.init_calls.fetch_add(1, Ordering::SeqCst);
            if let Some(init_entered) = &self.init_entered {
                init_entered.notify_one();
                return std::future::pending().await;
            }
            if self.init_fails {
                return Err(TargetError::Configuration("forced init failure".to_string()));
            }
            Ok(())
        }

        fn is_enabled(&self) -> bool {
            true
        }
    }

    fn builtin_adapter() -> BuiltinPluginRuntimeAdapter<String> {
        BuiltinPluginRuntimeAdapter::new(
            Arc::new(|_event| Box::pin(async {})),
            Arc::new(|_target_id, _has_replay| {}),
            None,
            Duration::from_millis(10),
            Duration::from_millis(10),
            "stopping test replay worker",
        )
    }

    #[tokio::test]
    async fn builtin_adapter_handles_empty_target_activation() {
        let adapter = builtin_adapter();
        let activation = adapter.activate_with_replay(Vec::new()).await;

        assert!(activation.targets.is_empty());
        assert!(activation.replay_workers.is_empty());
    }

    #[tokio::test]
    async fn builtin_adapter_skips_non_store_target_when_init_fails() {
        let adapter = builtin_adapter();
        let target = TestTarget::new("primary", "webhook").with_failed_init();

        let activation = adapter.activate_with_replay(vec![Box::new(target)]).await;

        assert!(activation.targets.is_empty());
        assert!(activation.replay_workers.is_empty());
    }

    #[tokio::test]
    async fn builtin_adapter_keeps_store_backed_target_when_init_fails() {
        let adapter = builtin_adapter();
        let target = TestTarget::new("primary", "webhook").with_failed_init().with_store();

        let activation = adapter.activate_with_replay(vec![Box::new(target)]).await;

        assert_eq!(activation.targets.len(), 1);
        assert_eq!(activation.replay_workers.len(), 1);
    }

    #[tokio::test]
    async fn prepared_store_target_reports_init_failure_without_dropping_queue_runtime() {
        let adapter = builtin_adapter();
        let target = TestTarget::new("primary", "webhook").with_failed_init().with_store();

        let prepared = adapter.prepare_targets(vec![Box::new(target)]).await;
        assert_eq!(prepared.targets.len(), 1);
        assert!(
            prepared
                .failure_summary()
                .is_some_and(|summary| summary.contains("initialization failed") && !summary.contains("forced init failure"))
        );

        let (opened, rejected) = adapter.open_prepared_stores(prepared);
        assert!(rejected.failure_summary().is_some());
        let (mut activation, activation_rejected) = adapter.try_activate_prepared(opened);
        assert!(activation_rejected.failure_summary().is_none());
        assert_eq!(activation.targets.len(), 1);
        assert_eq!(activation.replay_workers.len(), 1);
        activation.replay_workers.stop_all("stop degraded target replay worker").await;
    }

    #[tokio::test]
    async fn cancellable_preparation_returns_current_and_remaining_targets_for_shutdown() {
        let adapter = builtin_adapter();
        let init_entered = Arc::new(Notify::new());
        let first = TestTarget::new("first", "webhook").with_pending_init(init_entered.clone());
        let first_close_calls = first.close_calls.clone();
        let second = TestTarget::new("second", "webhook");
        let second_close_calls = second.close_calls.clone();
        let second_init_calls = second.init_calls.clone();
        let cancellation = CancellationToken::new();
        let prepare_adapter = adapter.clone();
        let prepare_cancellation = cancellation.clone();
        let prepare = tokio::spawn(async move {
            prepare_adapter
                .prepare_targets_cancellable(vec![Box::new(first), Box::new(second)], &prepare_cancellation)
                .await
        });

        init_entered.notified().await;
        cancellation.cancel();
        let prepared = tokio::time::timeout(Duration::from_secs(1), prepare)
            .await
            .expect("cancellation should interrupt target initialization")
            .expect("preparation task should finish");
        assert_eq!(prepared.targets.len(), 2);

        adapter
            .close_prepared(prepared)
            .await
            .expect("cancelled targets should close");
        assert_eq!(first_close_calls.load(Ordering::SeqCst), 1);
        assert_eq!(second_close_calls.load(Ordering::SeqCst), 1);
        assert_eq!(second_init_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn prepared_activation_opens_store_before_starting_replay() {
        let adapter = builtin_adapter();
        let dir = tempdir().expect("tempdir should be created");
        let queue_path = dir.path().join("queue");
        let mut target = TestTarget::new("primary", "webhook");
        target.store = Some(Arc::new(QueueStore::<QueuedPayload>::new(&queue_path, 16, ".queue")));

        let prepared = adapter.prepare_targets(vec![Box::new(target)]).await;
        assert!(!queue_path.exists(), "dormant preparation must not open the queue store");

        let (opened, rejected) = adapter.open_prepared_stores(prepared);
        assert!(rejected.targets.is_empty());
        assert!(queue_path.is_dir(), "handoff must open the queue store before activation");
        let (mut activation, activation_rejected) = adapter.try_activate_prepared(opened);
        assert!(activation_rejected.failure_summary().is_none());
        assert_eq!(activation.replay_workers.len(), 1);
        activation
            .replay_workers
            .stop_all("stop prepared activation test worker")
            .await;
    }

    #[tokio::test]
    async fn activation_closes_a_target_when_its_queue_store_cannot_open() {
        let adapter = builtin_adapter();
        let dir = tempdir().expect("tempdir should be created");
        let invalid_base = dir.path().join("not-a-directory");
        std::fs::write(&invalid_base, b"file").expect("invalid queue base should be created");
        let mut target = TestTarget::new("primary", "webhook");
        let close_calls = target.close_calls.clone();
        target.store = Some(Arc::new(QueueStore::<QueuedPayload>::new(&invalid_base, 16, ".queue")));

        let activation = adapter.activate_with_replay(vec![Box::new(target)]).await;

        assert!(activation.targets.is_empty());
        assert!(activation.replay_workers.is_empty());
        assert_eq!(close_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn prepared_stores_open_with_bounded_parallelism_and_stable_order() {
        const TARGETS: usize = MAX_PARALLEL_STORE_OPENS * 2;

        let adapter = builtin_adapter();
        let dir = tempdir().expect("tempdir should be created");
        let gate = Arc::new(StoreOpenGate::default());
        let mut targets: Vec<Box<dyn Target<String> + Send + Sync>> = Vec::with_capacity(TARGETS);
        let mut expected_ids = Vec::with_capacity(TARGETS);
        for index in 0..TARGETS {
            let mut target = TestTarget::new(&format!("target-{index}"), "webhook");
            expected_ids.push(target.id.to_string());
            let open_gate = gate.clone();
            target.store = Some(Arc::new(TestOpenStore {
                before_clone: Arc::new(|| {}),
                before_open: Arc::new(move || open_gate.enter()),
                store: QueueStore::new(dir.path().join(index.to_string()), 16, ".queue"),
            }));
            targets.push(Box::new(target));
        }

        let prepared = adapter.prepare_targets(targets).await;
        let open_adapter = adapter.clone();
        let opening = tokio::task::spawn_blocking(move || open_adapter.open_prepared_stores(prepared));
        let wait_gate = gate.clone();
        let reached_bound =
            tokio::task::spawn_blocking(move || wait_gate.wait_for_active(MAX_PARALLEL_STORE_OPENS, Duration::from_secs(30)))
                .await
                .expect("store-open observer should not panic");
        gate.release();
        let (opened, rejected) = opening.await.expect("bounded store opens should not panic");
        let opened_ids = opened
            .targets
            .iter()
            .map(|target| target.id().to_string())
            .collect::<Vec<_>>();

        assert!(reached_bound, "store opens did not use the configured parallelism");
        assert_eq!(gate.max_active(), MAX_PARALLEL_STORE_OPENS);
        assert_eq!(opened_ids, expected_ids, "parallel store opens must preserve configuration order");
        assert!(rejected.targets.is_empty());
        assert!(rejected.failure_summary().is_none());
    }

    #[tokio::test]
    async fn panicking_store_open_rejects_and_closes_only_that_target() {
        let adapter = builtin_adapter();
        let dir = tempdir().expect("tempdir should be created");
        let mut target = TestTarget::new("panicking", "webhook");
        let close_calls = target.close_calls.clone();
        target.store = Some(Arc::new(TestOpenStore {
            before_clone: Arc::new(|| {}),
            before_open: Arc::new(|| panic!("forced store open panic: do-not-expose-payload")),
            store: QueueStore::new(dir.path(), 16, ".queue"),
        }));

        let prepared = adapter.prepare_targets(vec![Box::new(target)]).await;
        let (opened, rejected) = adapter.open_prepared_stores(prepared);
        let summary = rejected
            .failure_summary()
            .expect("panicking store should report a generic activation failure");
        let (activation, activation_rejected) = adapter.try_activate_prepared(opened);

        assert!(activation.targets.is_empty(), "a target without an open store must not become visible");
        assert!(
            activation.replay_workers.is_empty(),
            "a rejected target must not publish without a replay worker"
        );
        assert!(activation_rejected.failure_summary().is_none());
        assert!(summary.contains("queue store open failed"));
        assert!(!summary.contains("do-not-expose-payload"));
        adapter
            .close_prepared(rejected)
            .await
            .expect("a target rejected after a store panic should close");
        assert_eq!(close_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn panicking_store_clone_cannot_publish_target_without_replay_worker() {
        let adapter = builtin_adapter();
        let dir = tempdir().expect("tempdir should be created");
        let mut target = TestTarget::new("panicking-clone", "webhook");
        let close_calls = target.close_calls.clone();
        target.store = Some(Arc::new(TestOpenStore {
            before_clone: Arc::new(|| panic!("forced store clone panic: do-not-expose-payload")),
            before_open: Arc::new(|| {}),
            store: QueueStore::new(dir.path(), 16, ".queue"),
        }));

        let prepared = adapter.prepare_targets(vec![Box::new(target)]).await;
        let (opened, open_rejected) = adapter.open_prepared_stores(prepared);
        assert!(open_rejected.failure_summary().is_none());
        let (activation, rejected) = adapter.try_activate_prepared(opened);
        let summary = rejected
            .failure_summary()
            .expect("panicking store clone should report a generic activation failure");

        assert!(activation.targets.is_empty(), "a target without a replay worker must not become visible");
        assert!(activation.replay_workers.is_empty());
        assert!(summary.contains("replay activation failed"));
        assert!(!summary.contains("do-not-expose-payload"));
        adapter
            .close_prepared(rejected)
            .await
            .expect("a target rejected during replay activation should close");
        assert_eq!(close_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn builtin_adapter_shutdown_clears_runtime_and_replay_workers() {
        let adapter = builtin_adapter();
        let target = TestTarget::new("primary", "webhook");
        let close_calls = Arc::clone(&target.close_calls);
        let mut runtime = crate::runtime::TargetRuntimeManager::new();
        let mut replay_workers = crate::runtime::ReplayWorkerManager::new();

        let activation = adapter.activate_with_replay(vec![Box::new(target)]).await;
        adapter
            .replace_runtime_targets(&mut runtime, &mut replay_workers, activation)
            .await
            .expect("replace_runtime_targets should succeed");

        assert_eq!(runtime.len(), 1);
        assert_eq!(replay_workers.len(), 0);

        adapter
            .shutdown(&mut runtime, &mut replay_workers)
            .await
            .expect("shutdown should succeed");

        assert!(runtime.is_empty());
        assert!(replay_workers.is_empty());
        assert_eq!(close_calls.load(Ordering::SeqCst), 1);
    }
}
