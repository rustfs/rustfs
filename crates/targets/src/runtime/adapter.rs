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
    ReplayEvent, ReplayWorkerManager, RuntimeActivation, RuntimeStatusSnapshot, RuntimeTargetHealthSnapshot,
    TargetRuntimeManager, activate_targets_with_replay, init_target_and_optionally_start_replay, start_replay_worker,
};
use crate::{Target, TargetError};
use async_trait::async_trait;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;

type ReplayHook<E> = Arc<dyn Fn(ReplayEvent<E>) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;
type ReplayStartObserver = Arc<dyn Fn(&str, bool) + Send + Sync>;

/// Shared runtime contract for target plugins.
#[async_trait]
pub trait PluginRuntimeAdapter<E>: Send + Sync
where
    E: Send + Sync + 'static + Clone + Serialize + DeserializeOwned,
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
    E: Send + Sync + 'static + Clone + Serialize + DeserializeOwned,
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
    E: Send + Sync + 'static + Clone + Serialize + DeserializeOwned,
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
}

#[async_trait]
impl<E> PluginRuntimeAdapter<E> for BuiltinPluginRuntimeAdapter<E>
where
    E: Send + Sync + 'static + Clone + Serialize + DeserializeOwned,
{
    async fn activate_with_replay(&self, targets: Vec<Box<dyn Target<E> + Send + Sync>>) -> RuntimeActivation<E> {
        let replay_hook = Arc::clone(&self.replay_hook);
        let replay_start_observer = Arc::clone(&self.replay_start_observer);
        let replay_semaphore = self.replay_semaphore.clone();
        let batch_timeout = self.batch_timeout;
        let idle_sleep = self.idle_sleep;

        activate_targets_with_replay(targets, move |target| {
            let replay_hook = Arc::clone(&replay_hook);
            let replay_start_observer = Arc::clone(&replay_start_observer);
            let replay_semaphore = replay_semaphore.clone();

            async move {
                init_target_and_optionally_start_replay(
                    target,
                    move |target_id, has_replay| replay_start_observer(target_id, has_replay),
                    move |store, target| {
                        start_replay_worker(
                            store,
                            target,
                            Arc::clone(&replay_hook),
                            replay_semaphore.clone(),
                            batch_timeout,
                            idle_sleep,
                        )
                    },
                )
                .await
            }
        })
        .await
    }

    async fn replace_runtime_targets(
        &self,
        runtime: &mut TargetRuntimeManager<E>,
        replay_workers: &mut ReplayWorkerManager,
        activation: RuntimeActivation<E>,
    ) -> Result<(), TargetError> {
        self.stop_replay_workers(replay_workers).await;
        runtime.clear_and_close().await;

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
        self.stop_replay_workers(replay_workers).await;
        runtime.clear_and_close().await;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{BuiltinPluginRuntimeAdapter, PluginRuntimeAdapter};
    use crate::arn::TargetID;
    use crate::store::{Key, QueueStore, Store};
    use crate::target::{EntityTarget, QueuedPayload, QueuedPayloadMeta};
    use crate::{StoreError, Target, TargetError};
    use async_trait::async_trait;
    use serde::{Serialize, de::DeserializeOwned};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;
    use tempfile::tempdir;

    #[derive(Clone)]
    struct TestTarget {
        close_calls: Arc<AtomicUsize>,
        id: TargetID,
        init_fails: bool,
        store: Option<QueueStore<QueuedPayload>>,
    }

    impl TestTarget {
        fn new(id: &str, name: &str) -> Self {
            Self {
                close_calls: Arc::new(AtomicUsize::new(0)),
                id: TargetID::new(id.to_string(), name.to_string()),
                init_fails: false,
                store: None,
            }
        }

        fn with_failed_init(mut self) -> Self {
            self.init_fails = true;
            self
        }

        fn with_store(mut self) -> Self {
            let dir = tempdir().expect("tempdir should be created for queue store tests");
            let store = QueueStore::<QueuedPayload>::new(dir.path(), 16, ".queue");
            store.open().expect("queue store should open");
            self.store = Some(store);
            self
        }
    }

    #[async_trait]
    impl<E> Target<E> for TestTarget
    where
        E: Send + Sync + 'static + Clone + Serialize + DeserializeOwned,
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
            self.store
                .as_ref()
                .map(|store| store as &(dyn Store<QueuedPayload, Error = StoreError, Key = Key> + Send + Sync))
        }

        fn clone_dyn(&self) -> Box<dyn Target<E> + Send + Sync> {
            Box::new(self.clone())
        }

        async fn init(&self) -> Result<(), TargetError> {
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
