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
    Event, NotificationError, config_manager::notify_configuration_hint, error::transition_join_error, registry::TargetRegistry,
    runtime_facade::NotifyRuntimeFacade,
};
use async_trait::async_trait;
use rustfs_config::server_config::Config;
use rustfs_targets::Target;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use tokio::sync::{Mutex, RwLock};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

type CurrentGeneration = Arc<dyn Fn() -> bool + Send + Sync>;
type CommitGeneration = Box<dyn FnOnce(bool) + Send>;
type CleanupFuture = Pin<Box<dyn Future<Output = Result<(), NotificationError>> + Send>>;

const LOG_COMPONENT_NOTIFY: &str = "notify";
const LOG_SUBSYSTEM_LIFECYCLE: &str = "lifecycle";
const EVENT_NOTIFY_RUNTIME_LIFECYCLE: &str = "notify_runtime_lifecycle";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NotificationRuntimeState {
    LiveOnly,
    TargetsEnabled { generation: u64 },
    Terminated,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DesiredMode {
    LiveOnly,
    TargetsEnabled,
    Terminated,
}

#[derive(Clone)]
struct DesiredState {
    cancellation: CancellationToken,
    config: Config,
    generation: u64,
    mode: DesiredMode,
}

struct RuntimeApply {
    cleanup: Option<CleanupFuture>,
}

impl RuntimeApply {
    fn complete() -> Self {
        Self { cleanup: None }
    }

    fn with_cleanup(cleanup: CleanupFuture) -> Self {
        Self { cleanup: Some(cleanup) }
    }

    async fn finish(self) -> Result<(), NotificationError> {
        match self.cleanup {
            Some(cleanup) => cleanup.await,
            None => Ok(()),
        }
    }
}

enum TransitionResult {
    Ready(Result<(), NotificationError>),
    Running(JoinHandle<Result<(), NotificationError>>),
}

pub struct NotificationLifecycleTransition {
    result: TransitionResult,
}

impl NotificationLifecycleTransition {
    fn ready(result: Result<(), NotificationError>) -> Self {
        Self {
            result: TransitionResult::Ready(result),
        }
    }

    fn running(handle: JoinHandle<Result<(), NotificationError>>) -> Self {
        Self {
            result: TransitionResult::Running(handle),
        }
    }

    pub async fn wait(self) -> Result<(), NotificationError> {
        match self.result {
            TransitionResult::Ready(result) => result,
            TransitionResult::Running(handle) => handle.await.map_err(transition_join_error)?,
        }
    }
}

#[async_trait]
trait LifecycleRuntime: Send + Sync {
    async fn cache_config(&self, config: Config);

    async fn enable_targets(
        &self,
        config: &Config,
        cancellation: &CancellationToken,
        publication_gate: Arc<StdMutex<()>>,
        is_current: CurrentGeneration,
        on_committed: CommitGeneration,
    ) -> Result<RuntimeApply, NotificationError>;

    async fn disable_targets(
        &self,
        publication_gate: Arc<StdMutex<()>>,
        is_current: CurrentGeneration,
        on_committed: CommitGeneration,
    ) -> Result<RuntimeApply, NotificationError>;
}

struct DefaultLifecycleRuntime {
    config: Arc<RwLock<Config>>,
    registry: Arc<TargetRegistry>,
    runtime_facade: NotifyRuntimeFacade,
    #[cfg(test)]
    handoff_observer: Option<Arc<dyn Fn() + Send + Sync>>,
}

#[async_trait]
impl LifecycleRuntime for DefaultLifecycleRuntime {
    async fn cache_config(&self, config: Config) {
        *self.config.write().await = config;
    }

    async fn enable_targets(
        &self,
        config: &Config,
        cancellation: &CancellationToken,
        publication_gate: Arc<StdMutex<()>>,
        is_current: CurrentGeneration,
        on_committed: CommitGeneration,
    ) -> Result<RuntimeApply, NotificationError> {
        info!(
            event = EVENT_NOTIFY_RUNTIME_LIFECYCLE,
            component = LOG_COMPONENT_NOTIFY,
            subsystem = LOG_SUBSYSTEM_LIFECYCLE,
            state = "preparing",
            "notify runtime lifecycle"
        );

        let (targets, creation_failures): (Vec<Box<dyn Target<Event> + Send + Sync>>, Vec<String>) =
            self.registry.create_dormant_targets_from_config(config).await?;
        if targets.is_empty() {
            debug!(
                event = EVENT_NOTIFY_RUNTIME_LIFECYCLE,
                component = LOG_COMPONENT_NOTIFY,
                subsystem = LOG_SUBSYSTEM_LIFECYCLE,
                state = "idle",
                reason = "no_targets_configured",
                hint = %notify_configuration_hint(),
                "notify runtime lifecycle"
            );
        }

        let mut prepared = self.runtime_facade.prepare_targets(targets, cancellation).await;
        prepared.extend_creation_failures(creation_failures);
        if !is_current() {
            let runtime_facade = self.runtime_facade.clone();
            return Ok(RuntimeApply::with_cleanup(Box::pin(async move {
                runtime_facade.close_prepared(prepared).await
            })));
        }

        // Drain sends that cloned the old targets, then keep later sends queued
        // until queue-store ownership and the target list move atomically. A
        // successful current-generation check under publication_gate is the
        // non-cancellable commit barrier: later intents serialize after this
        // handoff instead of abandoning a workerless old runtime.
        let dispatch_guard = self.runtime_facade.pause_dispatch().await;
        {
            let _publication_guard = publication_gate.lock().unwrap_or_else(|err| err.into_inner());
            if !is_current() {
                drop(_publication_guard);
                drop(dispatch_guard);
                let runtime_facade = self.runtime_facade.clone();
                return Ok(RuntimeApply::with_cleanup(Box::pin(async move {
                    runtime_facade.close_prepared(prepared).await
                })));
            }
        }

        #[cfg(test)]
        if let Some(observer) = &self.handoff_observer {
            observer();
        }

        self.runtime_facade.stop_active_replay_workers().await;
        let (opened, rejected) = self.runtime_facade.open_prepared_stores(prepared).await?;
        let open_complete = rejected.failure_summary().is_none();
        // A newer accepted generation remains queued behind apply_lock and
        // replaces this one next.
        let (detached, activation_rejected) = self
            .runtime_facade
            .commit_prepared(opened, move |activation_complete| on_committed(open_complete && activation_complete))
            .await;
        let activation_failures = [rejected.failure_summary(), activation_rejected.failure_summary()]
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        drop(dispatch_guard);
        let runtime_facade = self.runtime_facade.clone();

        info!(
            event = EVENT_NOTIFY_RUNTIME_LIFECYCLE,
            component = LOG_COMPONENT_NOTIFY,
            subsystem = LOG_SUBSYSTEM_LIFECYCLE,
            state = "enabled",
            "notify runtime lifecycle"
        );
        Ok(RuntimeApply::with_cleanup(Box::pin(async move {
            let (close_old, close_rejected, close_activation_rejected) = tokio::join!(
                runtime_facade.close_detached_targets(detached),
                runtime_facade.close_prepared(rejected),
                runtime_facade.close_prepared(activation_rejected),
            );
            close_old?;
            close_rejected?;
            close_activation_rejected?;
            if !activation_failures.is_empty() {
                return Err(NotificationError::Initialization(format!(
                    "one or more notification targets failed to activate: {}",
                    activation_failures.join("; ")
                )));
            }
            Ok(())
        })))
    }

    async fn disable_targets(
        &self,
        publication_gate: Arc<StdMutex<()>>,
        is_current: CurrentGeneration,
        on_committed: CommitGeneration,
    ) -> Result<RuntimeApply, NotificationError> {
        let dispatch_guard = self.runtime_facade.pause_dispatch().await;
        {
            let _publication_guard = publication_gate.lock().unwrap_or_else(|err| err.into_inner());
            if !is_current() {
                drop(_publication_guard);
                drop(dispatch_guard);
                return Ok(RuntimeApply::complete());
            }
        }

        // The generation check above is the non-cancellable publication
        // barrier. Later intents serialize behind apply_lock while the old
        // replay workers stop, so LiveOnly is not visible until every old
        // worker has joined.
        self.runtime_facade.stop_active_replay_workers().await;
        let detached = self.runtime_facade.commit_disabled(move || on_committed(true)).await;
        drop(dispatch_guard);
        let runtime_facade = self.runtime_facade.clone();
        Ok(RuntimeApply::with_cleanup(Box::pin(async move {
            runtime_facade.close_detached_targets(detached).await
        })))
    }
}

struct LifecycleCoordinatorInner {
    applied_generation: AtomicU64,
    apply_lock: Mutex<()>,
    desired: StdMutex<DesiredState>,
    publication_gate: Arc<StdMutex<()>>,
    runtime: Arc<dyn LifecycleRuntime>,
    state: StdMutex<NotificationRuntimeState>,
}

#[derive(Clone)]
pub(crate) struct NotifyLifecycleCoordinator {
    inner: Arc<LifecycleCoordinatorInner>,
}

impl NotifyLifecycleCoordinator {
    pub(crate) fn new(config: Arc<RwLock<Config>>, registry: Arc<TargetRegistry>, runtime_facade: NotifyRuntimeFacade) -> Self {
        let runtime = Arc::new(DefaultLifecycleRuntime {
            config,
            registry,
            runtime_facade,
            #[cfg(test)]
            handoff_observer: None,
        });
        Self::with_runtime(Config::new(), runtime)
    }

    fn with_runtime(initial_config: Config, runtime: Arc<dyn LifecycleRuntime>) -> Self {
        Self {
            inner: Arc::new(LifecycleCoordinatorInner {
                applied_generation: AtomicU64::new(0),
                apply_lock: Mutex::new(()),
                desired: StdMutex::new(DesiredState {
                    cancellation: CancellationToken::new(),
                    config: initial_config,
                    generation: 0,
                    mode: DesiredMode::LiveOnly,
                }),
                publication_gate: Arc::new(StdMutex::new(())),
                runtime,
                state: StdMutex::new(NotificationRuntimeState::LiveOnly),
            }),
        }
    }

    pub(crate) fn set_mode(&self, enabled: bool, config: Option<Config>) -> NotificationLifecycleTransition {
        self.accept(false, move |desired| {
            desired.mode = if enabled {
                DesiredMode::TargetsEnabled
            } else {
                DesiredMode::LiveOnly
            };
            if let Some(config) = config {
                desired.config = config;
            }
            Ok(true)
        })
    }

    pub(crate) fn update_config(&self, config: Config) -> NotificationLifecycleTransition {
        self.accept(false, move |desired| {
            desired.config = config;
            Ok(true)
        })
    }

    pub(crate) fn terminate(&self) -> NotificationLifecycleTransition {
        self.accept(true, |desired| {
            if desired.mode == DesiredMode::Terminated {
                return Ok(false);
            }
            desired.mode = DesiredMode::Terminated;
            Ok(true)
        })
    }

    pub(crate) fn state(&self) -> NotificationRuntimeState {
        *self.inner.state.lock().unwrap_or_else(|err| err.into_inner())
    }

    pub(crate) fn is_converged(&self) -> bool {
        let desired_generation = self.inner.desired.lock().unwrap_or_else(|err| err.into_inner()).generation;
        self.inner.applied_generation.load(Ordering::Acquire) >= desired_generation
    }

    fn accept<F>(&self, allow_terminated: bool, update: F) -> NotificationLifecycleTransition
    where
        F: FnOnce(&mut DesiredState) -> Result<bool, NotificationError>,
    {
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            return NotificationLifecycleTransition::ready(Err(NotificationError::Initialization(
                "Notification lifecycle coordinator is unavailable".to_string(),
            )));
        };
        let _publication_guard = self.inner.publication_gate.lock().unwrap_or_else(|err| err.into_inner());
        let mut desired = self.inner.desired.lock().unwrap_or_else(|err| err.into_inner());
        if desired.mode == DesiredMode::Terminated && !allow_terminated {
            return NotificationLifecycleTransition::ready(Err(NotificationError::Initialization(
                "Notification runtime has terminated".to_string(),
            )));
        }

        let previous = desired.clone();
        let changed = match update(&mut desired) {
            Ok(changed) => changed,
            Err(err) => return NotificationLifecycleTransition::ready(Err(err)),
        };
        if changed {
            let Some(next_generation) = desired.generation.checked_add(1) else {
                *desired = previous;
                return NotificationLifecycleTransition::ready(Err(NotificationError::Initialization(
                    "Notification lifecycle generation is exhausted".to_string(),
                )));
            };
            desired.generation = next_generation;
            previous.cancellation.cancel();
            desired.cancellation = CancellationToken::new();
        }
        let accepted = desired.clone();
        drop(desired);
        drop(_publication_guard);
        let inner = Arc::clone(&self.inner);
        NotificationLifecycleTransition::running(runtime.spawn(async move { inner.converge(accepted).await }))
    }
}

impl LifecycleCoordinatorInner {
    async fn converge(self: &Arc<Self>, desired: DesiredState) -> Result<(), NotificationError> {
        let _apply_guard = self.apply_lock.lock().await;
        if self.applied_generation.load(Ordering::Acquire) >= desired.generation {
            return Ok(());
        }
        if !self.current_matches(desired.generation, desired.mode) {
            return Ok(());
        }

        self.runtime.cache_config(desired.config.clone()).await;
        let current_inner = Arc::clone(self);
        let generation = desired.generation;
        let mode = desired.mode;
        let is_current: CurrentGeneration = Arc::new(move || current_inner.current_matches(generation, mode));
        let publication_gate = self.publication_gate.clone();

        let state = match desired.mode {
            DesiredMode::LiveOnly => NotificationRuntimeState::LiveOnly,
            DesiredMode::TargetsEnabled => NotificationRuntimeState::TargetsEnabled {
                generation: desired.generation,
            },
            DesiredMode::Terminated => NotificationRuntimeState::Terminated,
        };
        let committed_inner = Arc::clone(self);
        let on_committed: CommitGeneration =
            Box::new(move |complete| committed_inner.mark_published(generation, state, complete));

        let applied = match desired.mode {
            DesiredMode::LiveOnly | DesiredMode::Terminated => {
                self.runtime
                    .disable_targets(publication_gate, is_current, on_committed)
                    .await?
            }
            DesiredMode::TargetsEnabled => {
                self.runtime
                    .enable_targets(&desired.config, &desired.cancellation, publication_gate, is_current, on_committed)
                    .await?
            }
        };
        drop(_apply_guard);
        applied.finish().await
    }

    fn current_matches(&self, generation: u64, mode: DesiredMode) -> bool {
        let desired = self.desired.lock().unwrap_or_else(|err| err.into_inner());
        desired.generation == generation && desired.mode == mode
    }

    fn mark_published(&self, generation: u64, state: NotificationRuntimeState, complete: bool) {
        *self.state.lock().unwrap_or_else(|err| err.into_inner()) = state;
        if complete {
            self.applied_generation.store(generation, Ordering::Release);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        CommitGeneration, CurrentGeneration, DefaultLifecycleRuntime, LifecycleRuntime, NotificationRuntimeState,
        NotifyLifecycleCoordinator, RuntimeApply,
    };
    use crate::{
        Event, NotificationError, integration::NotificationMetrics, notifier::EventNotifier, registry::TargetRegistry,
        rule_engine::NotifyRuleEngine, runtime_facade::NotifyRuntimeFacade, runtime_view::NotifyRuntimeView,
    };
    use async_trait::async_trait;
    use rustfs_config::ENABLE_KEY;
    use rustfs_config::server_config::{Config, KVS};
    use rustfs_targets::arn::TargetID;
    use rustfs_targets::store::{Key, QueueStore, Store};
    use rustfs_targets::target::{EntityTarget, QueuedPayload, QueuedPayloadMeta};
    use rustfs_targets::{
        EventName, ReplayWorkerManager, StoreError, Target, TargetError, TargetPluginDescriptor, TargetPluginRegistry,
    };
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex as StdMutex};
    use tokio::sync::{Notify, RwLock, Semaphore, mpsc};
    use tokio_util::sync::CancellationToken;

    const BLOCKING_INIT_TARGET_TYPE: &str = "lifecycle_blocking_init";
    const RETRY_INIT_TARGET_TYPE: &str = "lifecycle_retry_init";
    const REPLAY_TARGET_TYPE: &str = "lifecycle_replay";

    struct BlockingInitState {
        close_calls: AtomicUsize,
        init_entered: Notify,
    }

    #[derive(Clone)]
    struct BlockingInitTarget {
        id: TargetID,
        state: Arc<BlockingInitState>,
    }

    #[async_trait]
    impl Target<Event> for BlockingInitTarget {
        fn id(&self) -> TargetID {
            self.id.clone()
        }

        async fn is_active(&self) -> Result<bool, TargetError> {
            Ok(true)
        }

        async fn save(&self, _event: Arc<EntityTarget<Event>>) -> Result<(), TargetError> {
            Ok(())
        }

        async fn send_raw_from_store(&self, _key: Key, _body: Vec<u8>, _meta: QueuedPayloadMeta) -> Result<(), TargetError> {
            Ok(())
        }

        async fn close(&self) -> Result<(), TargetError> {
            self.state.close_calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        fn store(&self) -> Option<&(dyn Store<QueuedPayload, Error = StoreError, Key = Key> + Send + Sync)> {
            None
        }

        fn clone_dyn(&self) -> Box<dyn Target<Event> + Send + Sync> {
            Box::new(self.clone())
        }

        async fn init(&self) -> Result<(), TargetError> {
            self.state.init_entered.notify_one();
            std::future::pending::<()>().await;
            Ok(())
        }

        fn is_enabled(&self) -> bool {
            true
        }
    }

    #[derive(Clone)]
    struct RetryInitTarget {
        fail_once: Arc<AtomicBool>,
        id: TargetID,
        should_fail: bool,
    }

    #[async_trait]
    impl Target<Event> for RetryInitTarget {
        fn id(&self) -> TargetID {
            self.id.clone()
        }

        async fn is_active(&self) -> Result<bool, TargetError> {
            Ok(true)
        }

        async fn save(&self, _event: Arc<EntityTarget<Event>>) -> Result<(), TargetError> {
            Ok(())
        }

        async fn send_raw_from_store(&self, _key: Key, _body: Vec<u8>, _meta: QueuedPayloadMeta) -> Result<(), TargetError> {
            Ok(())
        }

        async fn close(&self) -> Result<(), TargetError> {
            Ok(())
        }

        fn store(&self) -> Option<&(dyn Store<QueuedPayload, Error = StoreError, Key = Key> + Send + Sync)> {
            None
        }

        fn clone_dyn(&self) -> Box<dyn Target<Event> + Send + Sync> {
            Box::new(self.clone())
        }

        async fn init(&self) -> Result<(), TargetError> {
            if self.should_fail && self.fail_once.swap(false, Ordering::AcqRel) {
                return Err(TargetError::Initialization("forced transient init failure".to_string()));
            }
            Ok(())
        }

        fn is_enabled(&self) -> bool {
            true
        }
    }

    struct ReplayState {
        active_workers: AtomicUsize,
        completed_deliveries: AtomicUsize,
        created_targets: AtomicUsize,
        delivery_attempts: AtomicUsize,
        delivery_release: Notify,
        delivered_bodies: StdMutex<Vec<Vec<u8>>>,
        blocked_init_generation: usize,
        blocked_init_release: Notify,
        init_tx: mpsc::UnboundedSender<usize>,
        list_tx: mpsc::UnboundedSender<usize>,
        peak_workers: AtomicUsize,
        send_tx: mpsc::UnboundedSender<usize>,
    }

    struct ReplayWorkerLease {
        state: Arc<ReplayState>,
    }

    impl ReplayWorkerLease {
        fn acquire(state: Arc<ReplayState>) -> Self {
            let active = state.active_workers.fetch_add(1, Ordering::SeqCst) + 1;
            let mut peak = state.peak_workers.load(Ordering::SeqCst);
            while active > peak {
                match state
                    .peak_workers
                    .compare_exchange_weak(peak, active, Ordering::SeqCst, Ordering::SeqCst)
                {
                    Ok(_) => break,
                    Err(observed) => peak = observed,
                }
            }
            Self { state }
        }
    }

    impl Drop for ReplayWorkerLease {
        fn drop(&mut self) {
            self.state.active_workers.fetch_sub(1, Ordering::SeqCst);
        }
    }

    #[derive(Clone)]
    struct ObservedQueueStore {
        generation: usize,
        inner: QueueStore<QueuedPayload>,
        state: Arc<ReplayState>,
        _worker_lease: Option<Arc<ReplayWorkerLease>>,
    }

    impl Store<QueuedPayload> for ObservedQueueStore {
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
            self.inner.del(key)
        }

        fn delete(&self) -> Result<(), Self::Error> {
            self.inner.delete()
        }

        fn list(&self) -> Vec<Self::Key> {
            let _ = self.state.list_tx.send(self.generation);
            self.inner.list()
        }

        fn len(&self) -> usize {
            self.inner.len()
        }

        fn is_empty(&self) -> bool {
            self.inner.is_empty()
        }

        fn boxed_clone(&self) -> Box<dyn Store<QueuedPayload, Error = Self::Error, Key = Self::Key> + Send + Sync> {
            Box::new(Self {
                generation: self.generation,
                inner: self.inner.clone(),
                state: self.state.clone(),
                _worker_lease: Some(Arc::new(ReplayWorkerLease::acquire(self.state.clone()))),
            })
        }
    }

    #[derive(Clone)]
    struct ReplayTarget {
        id: TargetID,
        generation: usize,
        state: Arc<ReplayState>,
        store: ObservedQueueStore,
    }

    #[async_trait]
    impl Target<Event> for ReplayTarget {
        fn id(&self) -> TargetID {
            self.id.clone()
        }

        async fn is_active(&self) -> Result<bool, TargetError> {
            Ok(true)
        }

        async fn save(&self, _event: Arc<EntityTarget<Event>>) -> Result<(), TargetError> {
            Ok(())
        }

        async fn send_raw_from_store(&self, _key: Key, body: Vec<u8>, _meta: QueuedPayloadMeta) -> Result<(), TargetError> {
            let attempt = self.state.delivery_attempts.fetch_add(1, Ordering::SeqCst) + 1;
            let _ = self.state.send_tx.send(attempt);
            self.state.delivery_release.notified().await;
            self.state
                .delivered_bodies
                .lock()
                .unwrap_or_else(|err| err.into_inner())
                .push(body);
            self.state.completed_deliveries.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        async fn close(&self) -> Result<(), TargetError> {
            Ok(())
        }

        fn store(&self) -> Option<&(dyn Store<QueuedPayload, Error = StoreError, Key = Key> + Send + Sync)> {
            Some(&self.store)
        }

        fn clone_dyn(&self) -> Box<dyn Target<Event> + Send + Sync> {
            Box::new(self.clone())
        }

        async fn init(&self) -> Result<(), TargetError> {
            let _ = self.state.init_tx.send(self.generation);
            if self.generation == self.state.blocked_init_generation {
                self.state.blocked_init_release.notified().await;
            }
            Ok(())
        }

        fn is_enabled(&self) -> bool {
            true
        }
    }

    struct TestRuntime {
        block_cleanup: AtomicBool,
        block_enable: AtomicBool,
        cached_config: StdMutex<Config>,
        cleanup_entered: Arc<Notify>,
        cleanup_release: Arc<Notify>,
        disable_calls: AtomicUsize,
        enable_calls: AtomicUsize,
        enable_entered: Notify,
        fail_enable: AtomicBool,
        enable_release: Notify,
        enabled_configs: StdMutex<Vec<Config>>,
    }

    impl TestRuntime {
        fn new(initial_config: Config) -> Self {
            Self {
                block_cleanup: AtomicBool::new(false),
                block_enable: AtomicBool::new(false),
                cached_config: StdMutex::new(initial_config),
                cleanup_entered: Arc::new(Notify::new()),
                cleanup_release: Arc::new(Notify::new()),
                disable_calls: AtomicUsize::new(0),
                enable_calls: AtomicUsize::new(0),
                enable_entered: Notify::new(),
                fail_enable: AtomicBool::new(false),
                enable_release: Notify::new(),
                enabled_configs: StdMutex::new(Vec::new()),
            }
        }

        fn block_next_enable(&self) {
            self.block_enable.store(true, Ordering::Release);
        }

        fn block_next_cleanup(&self) {
            self.block_cleanup.store(true, Ordering::Release);
        }

        fn fail_next_enable(&self) {
            self.fail_enable.store(true, Ordering::Release);
        }

        fn last_enabled_config(&self) -> Option<Config> {
            self.enabled_configs
                .lock()
                .unwrap_or_else(|err| err.into_inner())
                .last()
                .cloned()
        }
    }

    #[async_trait]
    impl LifecycleRuntime for TestRuntime {
        async fn cache_config(&self, config: Config) {
            *self.cached_config.lock().unwrap_or_else(|err| err.into_inner()) = config;
        }

        async fn enable_targets(
            &self,
            config: &Config,
            _cancellation: &CancellationToken,
            publication_gate: Arc<StdMutex<()>>,
            is_current: CurrentGeneration,
            on_committed: CommitGeneration,
        ) -> Result<RuntimeApply, NotificationError> {
            self.enable_calls.fetch_add(1, Ordering::SeqCst);
            if self.block_enable.swap(false, Ordering::AcqRel) {
                self.enable_entered.notify_one();
                self.enable_release.notified().await;
            }
            if self.fail_enable.swap(false, Ordering::AcqRel) {
                return Err(NotificationError::Initialization("forced enable failure".to_string()));
            }
            let _publication_guard = publication_gate.lock().unwrap_or_else(|err| err.into_inner());
            if !is_current() {
                return Ok(RuntimeApply::complete());
            }
            self.enabled_configs
                .lock()
                .unwrap_or_else(|err| err.into_inner())
                .push(config.clone());
            on_committed(true);
            if self.block_cleanup.swap(false, Ordering::AcqRel) {
                let cleanup_entered = self.cleanup_entered.clone();
                let cleanup_release = self.cleanup_release.clone();
                return Ok(RuntimeApply::with_cleanup(Box::pin(async move {
                    cleanup_entered.notify_one();
                    cleanup_release.notified().await;
                    Ok(())
                })));
            }
            Ok(RuntimeApply::complete())
        }

        async fn disable_targets(
            &self,
            publication_gate: Arc<StdMutex<()>>,
            is_current: CurrentGeneration,
            on_committed: CommitGeneration,
        ) -> Result<RuntimeApply, NotificationError> {
            let _publication_guard = publication_gate.lock().unwrap_or_else(|err| err.into_inner());
            if !is_current() {
                return Ok(RuntimeApply::complete());
            }
            self.disable_calls.fetch_add(1, Ordering::SeqCst);
            on_committed(true);
            Ok(RuntimeApply::complete())
        }
    }

    fn config_with_target(name: &str) -> Config {
        let mut config = Config::new();
        config
            .0
            .entry("notify_webhook".to_string())
            .or_default()
            .insert(name.to_string(), KVS::default());
        config
    }

    fn config_with_enabled_test_target(target_type: &str, name: &str) -> Config {
        let mut target = KVS::new();
        target.insert(ENABLE_KEY.to_string(), "on".to_string());
        let mut config = Config::new();
        config
            .0
            .entry(format!("notify_{target_type}"))
            .or_default()
            .insert(name.to_string(), target);
        config
    }

    fn real_runtime(
        initial_config: Config,
        registry: Arc<TargetRegistry>,
        handoff_observer: Option<Arc<dyn Fn() + Send + Sync>>,
    ) -> (NotifyLifecycleCoordinator, NotifyRuntimeView, Arc<RwLock<ReplayWorkerManager>>) {
        let metrics = Arc::new(NotificationMetrics::new());
        let rule_engine = NotifyRuleEngine::new();
        let notifier = Arc::new(EventNotifier::new(metrics.clone(), rule_engine));
        let target_list = notifier.target_list();
        let replay_workers = Arc::new(RwLock::new(ReplayWorkerManager::new()));
        let runtime_view = NotifyRuntimeView::new(target_list.clone(), replay_workers.clone());
        let runtime_facade = NotifyRuntimeFacade::new_with_dispatch_gate(
            target_list,
            replay_workers.clone(),
            notifier.dispatch_gate(),
            Arc::new(Semaphore::new(4)),
            metrics,
        );
        let config = Arc::new(RwLock::new(initial_config.clone()));
        let runtime = Arc::new(DefaultLifecycleRuntime {
            config,
            registry,
            runtime_facade,
            handoff_observer,
        });
        (
            NotifyLifecycleCoordinator::with_runtime(initial_config, runtime),
            runtime_view,
            replay_workers,
        )
    }

    fn blocking_init_runtime() -> (
        Arc<BlockingInitState>,
        NotifyLifecycleCoordinator,
        NotifyRuntimeView,
        Arc<AtomicUsize>,
        Config,
    ) {
        let state = Arc::new(BlockingInitState {
            close_calls: AtomicUsize::new(0),
            init_entered: Notify::new(),
        });
        let mut plugins = TargetPluginRegistry::<Event>::new();
        let factory_state = state.clone();
        plugins.register(TargetPluginDescriptor::new(
            BLOCKING_INIT_TARGET_TYPE,
            &[ENABLE_KEY],
            |_config| Ok(()),
            move |id, _config| {
                Ok(Box::new(BlockingInitTarget {
                    id: TargetID::new(id, BLOCKING_INIT_TARGET_TYPE.to_string()),
                    state: factory_state.clone(),
                }))
            },
        ));
        let config = config_with_enabled_test_target(BLOCKING_INIT_TARGET_TYPE, "primary");
        let handoff_count = Arc::new(AtomicUsize::new(0));
        let observer_count = handoff_count.clone();
        let (coordinator, runtime_view, _) = real_runtime(
            config.clone(),
            Arc::new(TargetRegistry::with_plugins(plugins)),
            Some(Arc::new(move || {
                observer_count.fetch_add(1, Ordering::SeqCst);
            })),
        );
        (state, coordinator, runtime_view, handoff_count, config)
    }

    async fn recv_until_generation(receiver: &mut mpsc::UnboundedReceiver<usize>, expected: usize, context: &str) {
        loop {
            let generation = receiver.recv().await.expect(context);
            if generation == expected {
                return;
            }
        }
    }

    fn coordinator(runtime: Arc<TestRuntime>, initial_config: Config) -> NotifyLifecycleCoordinator {
        NotifyLifecycleCoordinator::with_runtime(initial_config, runtime)
    }

    #[tokio::test]
    async fn slow_enable_then_disable_converges_to_live_only() {
        let initial = config_with_target("initial");
        let runtime = Arc::new(TestRuntime::new(initial.clone()));
        runtime.block_next_enable();
        let coordinator = coordinator(runtime.clone(), initial);

        let enable = coordinator.set_mode(true, None);
        runtime.enable_entered.notified().await;
        let disable = coordinator.set_mode(false, None);
        runtime.enable_release.notify_one();

        enable.wait().await.expect("enable transition should finish");
        disable.wait().await.expect("disable transition should finish");

        assert_eq!(coordinator.state(), NotificationRuntimeState::LiveOnly);
        assert_eq!(runtime.enable_calls.load(Ordering::SeqCst), 1);
        assert_eq!(runtime.disable_calls.load(Ordering::SeqCst), 1);
        assert_eq!(runtime.last_enabled_config(), None, "superseded enable must not publish targets");
    }

    #[tokio::test]
    async fn real_runtime_disable_supersedes_target_init_and_leaves_no_runtime_state() {
        let (state, coordinator, runtime_view, handoff_count, config) = blocking_init_runtime();

        let enable = coordinator.set_mode(true, Some(config));
        state.init_entered.notified().await;
        let disable = coordinator.set_mode(false, None);

        enable.wait().await.expect("superseded real enable should finish");
        disable.wait().await.expect("real disable should converge");

        let status = runtime_view.runtime_status_snapshot().await;
        assert_eq!(status.target_count, 0);
        assert_eq!(status.replay_worker_count, 0);
        assert_eq!(state.close_calls.load(Ordering::SeqCst), 1);
        assert_eq!(handoff_count.load(Ordering::SeqCst), 0);
        assert_eq!(coordinator.state(), NotificationRuntimeState::LiveOnly);
    }

    #[tokio::test]
    async fn real_runtime_disable_publishes_live_only_after_replay_join() {
        let (coordinator, _, replay_workers) = real_runtime(Config::new(), Arc::new(TargetRegistry::new()), None);
        coordinator
            .set_mode(true, Some(Config::new()))
            .wait()
            .await
            .expect("empty target runtime should enable");

        let (cancel_tx, mut cancel_rx) = mpsc::channel(1);
        let stop_entered = Arc::new(Notify::new());
        let stop_release = Arc::new(Notify::new());
        let worker_entered = stop_entered.clone();
        let worker_release = stop_release.clone();
        let join = tokio::spawn(async move {
            cancel_rx.recv().await.expect("worker should receive cancellation");
            worker_entered.notify_one();
            worker_release.notified().await;
        });
        replay_workers
            .write()
            .await
            .insert_with_handle("blocking-replay".to_string(), cancel_tx, join);

        let disable = coordinator.set_mode(false, None);
        stop_entered.notified().await;
        assert_eq!(
            coordinator.state(),
            NotificationRuntimeState::TargetsEnabled { generation: 1 },
            "LiveOnly must not publish while an old replay worker is still running"
        );

        stop_release.notify_one();
        disable.wait().await.expect("disable should finish after replay joins");
        assert_eq!(coordinator.state(), NotificationRuntimeState::LiveOnly);
    }

    #[tokio::test]
    async fn real_runtime_terminate_cancels_target_init_and_is_final() {
        let (state, coordinator, runtime_view, handoff_count, config) = blocking_init_runtime();

        let enable = coordinator.set_mode(true, Some(config));
        state.init_entered.notified().await;
        let terminate = coordinator.terminate();

        enable.wait().await.expect("superseded real enable should finish");
        terminate.wait().await.expect("real terminate should converge");

        let status = runtime_view.runtime_status_snapshot().await;
        assert_eq!(status.target_count, 0);
        assert_eq!(status.replay_worker_count, 0);
        assert_eq!(state.close_calls.load(Ordering::SeqCst), 1);
        assert_eq!(handoff_count.load(Ordering::SeqCst), 0);
        assert_eq!(coordinator.state(), NotificationRuntimeState::Terminated);
        let err = coordinator
            .set_mode(false, None)
            .wait()
            .await
            .expect_err("terminated runtime cannot be restarted");
        assert!(matches!(
            err,
            NotificationError::Initialization(detail) if detail == "Notification runtime has terminated"
        ));
    }

    #[tokio::test]
    async fn partial_activation_reports_error_and_same_config_can_retry() {
        let fail_once = Arc::new(AtomicBool::new(true));
        let mut plugins = TargetPluginRegistry::<Event>::new();
        let factory_fail_once = fail_once.clone();
        plugins.register(TargetPluginDescriptor::new(
            RETRY_INIT_TARGET_TYPE,
            &[ENABLE_KEY],
            |_config| Ok(()),
            move |id, _config| {
                Ok(Box::new(RetryInitTarget {
                    should_fail: id == "bad",
                    id: TargetID::new(id, RETRY_INIT_TARGET_TYPE.to_string()),
                    fail_once: factory_fail_once.clone(),
                }))
            },
        ));
        let mut config = config_with_enabled_test_target(RETRY_INIT_TARGET_TYPE, "good");
        let mut bad = KVS::new();
        bad.insert(ENABLE_KEY.to_string(), "on".to_string());
        config
            .0
            .get_mut(&format!("notify_{RETRY_INIT_TARGET_TYPE}"))
            .expect("test target subsystem should exist")
            .insert("bad".to_string(), bad);
        let (coordinator, runtime_view, _) = real_runtime(config.clone(), Arc::new(TargetRegistry::with_plugins(plugins)), None);

        coordinator
            .set_mode(true, Some(config.clone()))
            .wait()
            .await
            .expect_err("the originating transition must report the failed target");
        let partial = runtime_view.runtime_status_snapshot().await;
        assert_eq!(partial.target_count, 1, "the healthy target should remain fault-isolated and active");
        assert!(!coordinator.is_converged(), "a partial generation must remain retryable");
        assert_eq!(coordinator.state(), NotificationRuntimeState::TargetsEnabled { generation: 1 });

        coordinator
            .update_config(config)
            .wait()
            .await
            .expect("the same persisted config should recover after the transient failure");
        let recovered = runtime_view.runtime_status_snapshot().await;
        assert_eq!(recovered.target_count, 2);
        assert!(coordinator.is_converged());
        assert_eq!(coordinator.state(), NotificationRuntimeState::TargetsEnabled { generation: 2 });

        coordinator
            .set_mode(false, None)
            .wait()
            .await
            .expect("test runtime should shut down");
    }

    #[tokio::test(start_paused = true)]
    async fn real_runtime_reload_never_overlaps_replay_workers_or_redelivers_queued_payload() {
        let queue_root = tempfile::tempdir().expect("queue root should be created");
        let queue_dir = queue_root.path().join("shared-queue");
        let body = br#"{"x":1}"#.to_vec();
        let seed_store = QueueStore::<QueuedPayload>::new_with_compression(&queue_dir, 16, ".event", false);
        seed_store.open().expect("seed queue should open");
        let meta = QueuedPayloadMeta::new(
            EventName::ObjectCreatedPut,
            "bucket-a".to_string(),
            "obj.txt".to_string(),
            "application/json",
            body.len(),
        );
        let payload = QueuedPayload::new(meta, body.clone());
        seed_store
            .put_raw(&payload.encode().expect("queued payload should encode"))
            .expect("queued payload should be seeded");
        assert_eq!(seed_store.len(), 1);
        drop(seed_store);

        let (init_tx, mut init_rx) = mpsc::unbounded_channel();
        let (list_tx, mut list_rx) = mpsc::unbounded_channel();
        let (send_tx, mut send_rx) = mpsc::unbounded_channel();
        let state = Arc::new(ReplayState {
            active_workers: AtomicUsize::new(0),
            blocked_init_generation: 3,
            blocked_init_release: Notify::new(),
            completed_deliveries: AtomicUsize::new(0),
            created_targets: AtomicUsize::new(0),
            delivery_attempts: AtomicUsize::new(0),
            delivery_release: Notify::new(),
            delivered_bodies: StdMutex::new(Vec::new()),
            init_tx,
            list_tx,
            peak_workers: AtomicUsize::new(0),
            send_tx,
        });
        let mut plugins = TargetPluginRegistry::<Event>::new();
        let factory_state = state.clone();
        let factory_queue_dir = queue_dir.clone();
        plugins.register(TargetPluginDescriptor::new(
            REPLAY_TARGET_TYPE,
            &[ENABLE_KEY],
            |_config| Ok(()),
            move |id, _config| {
                let generation = factory_state.created_targets.fetch_add(1, Ordering::SeqCst) + 1;
                Ok(Box::new(ReplayTarget {
                    id: TargetID::new(id, REPLAY_TARGET_TYPE.to_string()),
                    generation,
                    state: factory_state.clone(),
                    store: ObservedQueueStore {
                        generation,
                        inner: QueueStore::new_with_compression(&factory_queue_dir, 16, ".event", false),
                        state: factory_state.clone(),
                        _worker_lease: None,
                    },
                }))
            },
        ));

        let (handoff_tx, mut handoff_rx) = mpsc::unbounded_channel();
        let handoff_count = Arc::new(AtomicUsize::new(0));
        let observer_count = handoff_count.clone();
        let handoff_observer: Arc<dyn Fn() + Send + Sync> = Arc::new(move || {
            let generation = observer_count.fetch_add(1, Ordering::SeqCst) + 1;
            let _ = handoff_tx.send(generation);
        });
        let config = config_with_enabled_test_target(REPLAY_TARGET_TYPE, "primary");
        let (coordinator, runtime_view, _) =
            real_runtime(config.clone(), Arc::new(TargetRegistry::with_plugins(plugins)), Some(handoff_observer));

        coordinator
            .set_mode(true, Some(config.clone()))
            .wait()
            .await
            .expect("initial real runtime enable should succeed");
        assert_eq!(init_rx.recv().await.expect("first target init should be observed"), 1);
        assert_eq!(handoff_rx.recv().await.expect("first handoff should be observed"), 1);
        recv_until_generation(&mut list_rx, 1, "first replay worker should scan the queue").await;

        let status = runtime_view.runtime_status_snapshot().await;
        assert_eq!(status.target_count, 1);
        assert_eq!(status.replay_worker_count, 1);
        assert_eq!(state.active_workers.load(Ordering::SeqCst), 1);
        assert_eq!(state.peak_workers.load(Ordering::SeqCst), 1);

        tokio::time::advance(std::time::Duration::from_secs(6)).await;
        assert_eq!(send_rx.recv().await.expect("seeded payload delivery should start"), 1);

        let second_generation = coordinator.update_config(config.clone());
        assert_eq!(init_rx.recv().await.expect("replacement target init should be observed"), 2);
        assert_eq!(handoff_rx.recv().await.expect("replacement handoff should be observed"), 2);

        let status_during_handoff = runtime_view.runtime_status_snapshot().await;
        assert_eq!(status_during_handoff.target_count, 1);
        assert_eq!(status_during_handoff.replay_worker_count, 0);
        assert_eq!(state.active_workers.load(Ordering::SeqCst), 1);
        assert_eq!(state.peak_workers.load(Ordering::SeqCst), 1);

        let third_generation = coordinator.update_config(config);
        state.delivery_release.notify_one();
        second_generation
            .wait()
            .await
            .expect("second generation should commit after crossing the handoff barrier");
        assert_eq!(init_rx.recv().await.expect("third target init should be observed"), 3);
        assert_eq!(coordinator.state(), NotificationRuntimeState::TargetsEnabled { generation: 2 });
        recv_until_generation(&mut list_rx, 2, "second-generation replay worker should scan the queue").await;

        let status_after_second_generation = runtime_view.runtime_status_snapshot().await;
        assert_eq!(status_after_second_generation.target_count, 1);
        assert_eq!(status_after_second_generation.replay_worker_count, 1);
        assert_eq!(state.active_workers.load(Ordering::SeqCst), 1);
        assert_eq!(state.peak_workers.load(Ordering::SeqCst), 1);

        state.blocked_init_release.notify_one();
        assert_eq!(handoff_rx.recv().await.expect("third handoff should be observed"), 3);
        third_generation
            .wait()
            .await
            .expect("third generation should converge after the committed second generation");
        recv_until_generation(&mut list_rx, 3, "third-generation replay worker should scan the queue").await;

        assert_eq!(state.delivery_attempts.load(Ordering::SeqCst), 1);
        assert_eq!(state.completed_deliveries.load(Ordering::SeqCst), 1);
        assert_eq!(state.peak_workers.load(Ordering::SeqCst), 1);
        assert_eq!(state.active_workers.load(Ordering::SeqCst), 1);
        assert_eq!(*state.delivered_bodies.lock().unwrap_or_else(|err| err.into_inner()), vec![body]);
        let status_after_reload = runtime_view.runtime_status_snapshot().await;
        assert_eq!(status_after_reload.target_count, 1);
        assert_eq!(status_after_reload.replay_worker_count, 1);
        assert_eq!(coordinator.state(), NotificationRuntimeState::TargetsEnabled { generation: 3 });

        tokio::time::advance(std::time::Duration::from_secs(6)).await;
        recv_until_generation(&mut list_rx, 3, "third-generation replay worker should rescan after the batch timeout").await;
        assert!(matches!(send_rx.try_recv(), Err(mpsc::error::TryRecvError::Empty)));

        coordinator
            .set_mode(false, None)
            .wait()
            .await
            .expect("real runtime disable should stop the replacement worker");
        let stopped = runtime_view.runtime_status_snapshot().await;
        assert_eq!(stopped.target_count, 0);
        assert_eq!(stopped.replay_worker_count, 0);
        assert_eq!(state.active_workers.load(Ordering::SeqCst), 0);

        let verify_store = QueueStore::<QueuedPayload>::new_with_compression(&queue_dir, 16, ".event", false);
        verify_store.open().expect("queue should reopen after runtime shutdown");
        assert_eq!(verify_store.len(), 0);
    }

    #[tokio::test]
    async fn later_config_generation_wins_after_slow_reload() {
        let first = config_with_target("first");
        let second = config_with_target("second");
        let runtime = Arc::new(TestRuntime::new(first.clone()));
        runtime.block_next_enable();
        let coordinator = coordinator(runtime.clone(), first.clone());

        let enable = coordinator.set_mode(true, Some(first));
        runtime.enable_entered.notified().await;
        let reload = coordinator.update_config(second.clone());
        runtime.enable_release.notify_one();

        enable.wait().await.expect("first generation should finish");
        reload.wait().await.expect("latest generation should finish");

        assert_eq!(runtime.last_enabled_config(), Some(second));
        assert_eq!(coordinator.state(), NotificationRuntimeState::TargetsEnabled { generation: 2 });
    }

    #[tokio::test]
    async fn accepted_transition_survives_caller_cancellation() {
        let initial = config_with_target("initial");
        let runtime = Arc::new(TestRuntime::new(initial.clone()));
        runtime.block_next_enable();
        let coordinator = coordinator(runtime.clone(), initial);

        let receipt = coordinator.set_mode(true, None);
        runtime.enable_entered.notified().await;
        drop(receipt);
        runtime.enable_release.notify_one();

        let apply_guard = coordinator.inner.apply_lock.lock().await;
        drop(apply_guard);
        assert!(matches!(coordinator.state(), NotificationRuntimeState::TargetsEnabled { generation: 1 }));
    }

    #[tokio::test]
    async fn disabled_config_update_is_cached_until_reenabled() {
        let initial = config_with_target("initial");
        let latest = config_with_target("latest");
        let runtime = Arc::new(TestRuntime::new(initial.clone()));
        let coordinator = coordinator(runtime.clone(), initial);

        coordinator
            .update_config(latest.clone())
            .wait()
            .await
            .expect("disabled update should be cached");
        assert_eq!(runtime.enable_calls.load(Ordering::SeqCst), 0);
        assert_eq!(coordinator.state(), NotificationRuntimeState::LiveOnly);

        coordinator
            .set_mode(true, None)
            .wait()
            .await
            .expect("re-enable should use cached config");
        assert_eq!(runtime.last_enabled_config(), Some(latest));
    }

    #[tokio::test]
    async fn concurrent_enable_publications_activate_once() {
        let initial = config_with_target("initial");
        let runtime = Arc::new(TestRuntime::new(initial.clone()));
        let coordinator = coordinator(runtime.clone(), initial);

        let apply_guard = coordinator.inner.apply_lock.lock().await;
        let first = coordinator.set_mode(true, None);
        let second = coordinator.set_mode(true, None);
        drop(apply_guard);

        first.wait().await.expect("first enable should converge");
        second.wait().await.expect("second enable should converge");
        assert_eq!(runtime.enable_calls.load(Ordering::SeqCst), 1);
        assert!(matches!(coordinator.state(), NotificationRuntimeState::TargetsEnabled { generation: 2 }));
    }

    #[tokio::test]
    async fn committed_generation_cleanup_does_not_block_a_newer_generation() {
        let initial = config_with_target("initial");
        let latest = config_with_target("latest");
        let runtime = Arc::new(TestRuntime::new(initial.clone()));
        runtime.block_next_cleanup();
        let coordinator = coordinator(runtime.clone(), initial);

        let first = coordinator.set_mode(true, None);
        runtime.cleanup_entered.notified().await;

        coordinator
            .update_config(latest.clone())
            .wait()
            .await
            .expect("newer generation should not wait for old cleanup");
        assert_eq!(runtime.enable_calls.load(Ordering::SeqCst), 2);
        assert_eq!(runtime.last_enabled_config(), Some(latest));

        runtime.cleanup_release.notify_one();
        first.wait().await.expect("old generation cleanup should finish");
    }

    #[tokio::test]
    async fn superseded_receipt_does_not_retry_a_failed_latest_generation() {
        let initial = config_with_target("initial");
        let latest = config_with_target("latest");
        let runtime = Arc::new(TestRuntime::new(initial.clone()));
        let coordinator = coordinator(runtime.clone(), initial);

        let apply_guard = coordinator.inner.apply_lock.lock().await;
        let superseded = coordinator.set_mode(true, None);
        tokio::task::yield_now().await;
        let latest_transition = coordinator.update_config(latest.clone());
        tokio::task::yield_now().await;
        runtime.fail_next_enable();
        drop(apply_guard);

        superseded
            .wait()
            .await
            .expect("a superseded generation should not apply the latest intent");
        latest_transition
            .wait()
            .await
            .expect_err("the latest generation must own its apply error");
        assert_eq!(runtime.enable_calls.load(Ordering::SeqCst), 1);
        assert!(!coordinator.is_converged());

        coordinator
            .update_config(latest.clone())
            .wait()
            .await
            .expect("an explicit publication should retry the failed configuration");
        assert_eq!(runtime.enable_calls.load(Ordering::SeqCst), 2);
        assert_eq!(runtime.last_enabled_config(), Some(latest));
    }

    #[tokio::test]
    async fn failed_generation_can_retry_the_same_config() {
        let initial = config_with_target("initial");
        let latest = config_with_target("latest");
        let runtime = Arc::new(TestRuntime::new(initial.clone()));
        let coordinator = coordinator(runtime.clone(), initial);

        coordinator
            .set_mode(true, None)
            .wait()
            .await
            .expect("initial enable should succeed");
        runtime.fail_next_enable();
        coordinator
            .update_config(latest.clone())
            .wait()
            .await
            .expect_err("forced reload should fail");
        assert!(!coordinator.is_converged());

        coordinator
            .update_config(latest.clone())
            .wait()
            .await
            .expect("same config should be retryable");
        assert!(coordinator.is_converged());
        assert_eq!(runtime.last_enabled_config(), Some(latest));
    }

    #[tokio::test]
    async fn suspend_is_restartable_but_terminate_is_final() {
        let initial = config_with_target("initial");
        let runtime = Arc::new(TestRuntime::new(initial.clone()));
        let coordinator = coordinator(runtime, initial);

        coordinator.set_mode(true, None).wait().await.expect("enable should succeed");
        coordinator
            .set_mode(false, None)
            .wait()
            .await
            .expect("suspend should succeed");
        coordinator
            .set_mode(true, None)
            .wait()
            .await
            .expect("suspended runtime should restart");
        coordinator.terminate().wait().await.expect("terminate should succeed");
        coordinator.terminate().wait().await.expect("terminate should be idempotent");

        let err = coordinator
            .set_mode(true, None)
            .wait()
            .await
            .expect_err("terminated runtime must reject restart");
        assert!(matches!(
            err,
            NotificationError::Initialization(detail) if detail == "Notification runtime has terminated"
        ));
        assert_eq!(coordinator.state(), NotificationRuntimeState::Terminated);
    }

    #[tokio::test]
    async fn terminate_supersedes_blocked_enable_before_commit() {
        let initial = config_with_target("initial");
        let runtime = Arc::new(TestRuntime::new(initial.clone()));
        runtime.block_next_enable();
        let coordinator = coordinator(runtime.clone(), initial);

        let enable = coordinator.set_mode(true, None);
        runtime.enable_entered.notified().await;
        let terminate = coordinator.terminate();
        runtime.enable_release.notify_one();

        enable.wait().await.expect("superseded enable task should finish");
        terminate.wait().await.expect("terminate should finish");
        assert_eq!(runtime.last_enabled_config(), None);
        assert_eq!(coordinator.state(), NotificationRuntimeState::Terminated);
    }

    #[test]
    fn publication_without_tokio_runtime_does_not_mutate_desired_state() {
        let initial = config_with_target("initial");
        let latest = config_with_target("latest");
        let runtime = Arc::new(TestRuntime::new(initial.clone()));
        let coordinator = coordinator(runtime, initial.clone());

        let _transition = coordinator.update_config(latest);
        let desired = coordinator.inner.desired.lock().unwrap_or_else(|err| err.into_inner());
        assert_eq!(desired.generation, 0);
        assert_eq!(desired.config, initial);
    }
}
