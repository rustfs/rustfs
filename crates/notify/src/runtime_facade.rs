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

use crate::{Event, NotificationError, integration::NotificationMetrics, notifier::EventNotifier};
use rustfs_targets::{
    ReplayEvent, ReplayWorkerManager, RuntimeActivation, Target, activate_targets_with_replay,
    init_target_and_optionally_start_replay, start_replay_worker,
};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{RwLock, Semaphore};
use tracing::info;

#[derive(Clone)]
pub struct NotifyRuntimeFacade {
    notifier: Arc<EventNotifier>,
    replay_workers: Arc<RwLock<ReplayWorkerManager>>,
    concurrency_limiter: Arc<Semaphore>,
    metrics: Arc<NotificationMetrics>,
}

impl NotifyRuntimeFacade {
    pub fn new(
        notifier: Arc<EventNotifier>,
        replay_workers: Arc<RwLock<ReplayWorkerManager>>,
        concurrency_limiter: Arc<Semaphore>,
        metrics: Arc<NotificationMetrics>,
    ) -> Self {
        Self {
            notifier,
            replay_workers,
            concurrency_limiter,
            metrics,
        }
    }

    pub async fn activate_targets_with_replay(
        &self,
        targets: Vec<Box<dyn Target<Event> + Send + Sync>>,
    ) -> RuntimeActivation<Event> {
        activate_targets_with_replay(targets, |target| {
            let metrics = self.metrics.clone();
            let limiter = self.concurrency_limiter.clone();
            async move {
                let target_id = target.id();
                info!("Initializing target: {}", target_id);
                init_target_and_optionally_start_replay(
                    target,
                    |target_id, has_replay| {
                        if has_replay {
                            info!("Event stream processing for target {} is started successfully", target_id);
                        } else {
                            info!("Target {} has no replay worker to start", target_id);
                        }
                    },
                    move |store, target| {
                        start_replay_worker(
                            store,
                            target,
                            Arc::new(move |event| {
                                let metrics = metrics.clone();
                                Box::pin(async move {
                                    match event {
                                        ReplayEvent::Delivered { .. } => metrics.increment_processed(),
                                        ReplayEvent::RetryableError { .. } => {}
                                        ReplayEvent::Dropped { target, .. }
                                        | ReplayEvent::PermanentFailure { target, .. }
                                        | ReplayEvent::RetryExhausted { target, .. } => {
                                            target.record_final_failure();
                                            metrics.increment_failed();
                                        }
                                        ReplayEvent::UnreadableEntry { .. } => {}
                                    }
                                })
                            }),
                            Some(limiter.clone()),
                            Duration::from_secs(5),
                            Duration::from_millis(500),
                        )
                    },
                )
                .await
            }
        })
        .await
    }

    pub async fn replace_targets(&self, activation: RuntimeActivation<Event>) -> Result<(), NotificationError> {
        self.stop_replay_workers().await;
        self.notifier.remove_all_bucket_targets().await;
        self.notifier.init_bucket_targets_shared(activation.targets).await?;
        *self.replay_workers.write().await = activation.replay_workers;
        Ok(())
    }

    pub async fn stop_replay_workers(&self) {
        let mut replay_workers = self.replay_workers.write().await;
        replay_workers.stop_all("Stop event stream processing for target").await;
    }

    pub async fn shutdown(&self) {
        info!("Turn off the notification system");

        let active_targets = self.replay_workers.read().await.len();
        info!("Stops {} active event stream processing tasks", active_targets);

        self.stop_replay_workers().await;
        tokio::time::sleep(Duration::from_millis(500)).await;

        info!("Notify the system to be shut down completed");
    }
}

#[cfg(test)]
mod tests {
    use super::NotifyRuntimeFacade;
    use crate::{Event, integration::NotificationMetrics, notifier::EventNotifier, runtime_view::NotifyRuntimeView};
    use async_trait::async_trait;
    use rustfs_targets::arn::TargetID;
    use rustfs_targets::store::{Key, Store};
    use rustfs_targets::target::{EntityTarget, QueuedPayload, QueuedPayloadMeta};
    use rustfs_targets::{ReplayWorkerManager, SharedTarget, StoreError, Target, TargetError};
    use serde::{Serialize, de::DeserializeOwned};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::{RwLock, Semaphore};

    #[derive(Clone)]
    struct TestTarget {
        close_calls: Arc<AtomicUsize>,
        id: TargetID,
    }

    impl TestTarget {
        fn new(id: &str, name: &str) -> Self {
            Self {
                close_calls: Arc::new(AtomicUsize::new(0)),
                id: TargetID::new(id.to_string(), name.to_string()),
            }
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

    fn build_facade() -> (NotifyRuntimeFacade, Arc<EventNotifier>, Arc<RwLock<ReplayWorkerManager>>) {
        let metrics = Arc::new(NotificationMetrics::new());
        let notifier = Arc::new(EventNotifier::new(metrics.clone()));
        let replay_workers = Arc::new(RwLock::new(ReplayWorkerManager::new()));
        let facade = NotifyRuntimeFacade::new(notifier.clone(), replay_workers.clone(), Arc::new(Semaphore::new(4)), metrics);
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
}
