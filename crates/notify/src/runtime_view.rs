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

use crate::{Event, NotificationTargetMetricSnapshot, notifier::SharedNotifyTargetList};
use rustfs_targets::{ReplayWorkerManager, RuntimeTargetHealthSnapshot, SharedTarget, arn::TargetID};
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Clone)]
pub struct NotifyRuntimeView {
    target_list: SharedNotifyTargetList,
    stream_cancellers: Arc<RwLock<ReplayWorkerManager>>,
}

impl NotifyRuntimeView {
    pub fn new(target_list: SharedNotifyTargetList, stream_cancellers: Arc<RwLock<ReplayWorkerManager>>) -> Self {
        Self {
            target_list,
            stream_cancellers,
        }
    }

    pub async fn get_active_targets(&self) -> Vec<TargetID> {
        self.target_list.read().await.keys()
    }

    pub fn get_all_targets(&self) -> SharedNotifyTargetList {
        self.target_list.clone()
    }

    pub async fn get_target_values(&self) -> Vec<SharedTarget<Event>> {
        self.target_list.read().await.values()
    }

    pub async fn snapshot_target_metrics(&self) -> Vec<NotificationTargetMetricSnapshot> {
        self.target_list
            .read()
            .await
            .runtime_snapshots()
            .into_iter()
            .map(|snapshot| NotificationTargetMetricSnapshot {
                failed_messages: snapshot.failed_messages,
                queue_length: snapshot.queue_length,
                target_id: snapshot.target_id,
                target_type: snapshot.target_type,
                total_messages: snapshot.total_messages,
            })
            .collect()
    }

    pub async fn snapshot_target_health(&self) -> Vec<RuntimeTargetHealthSnapshot> {
        self.target_list.read().await.runtime_health_snapshots().await
    }

    pub async fn runtime_status_snapshot(&self) -> rustfs_targets::RuntimeStatusSnapshot {
        let replay_workers = self.stream_cancellers.read().await;
        let target_list = self.target_list.read().await;
        target_list.runtime_status_snapshot(&replay_workers)
    }
}

#[cfg(test)]
mod tests {
    use super::NotifyRuntimeView;
    use crate::{Event, notifier::TargetList};
    use async_trait::async_trait;
    use rustfs_targets::arn::TargetID;
    use rustfs_targets::store::{Key, Store};
    use rustfs_targets::target::{EntityTarget, QueuedPayload, QueuedPayloadMeta, TargetDeliverySnapshot};
    use rustfs_targets::{ReplayWorkerManager, StoreError, Target, TargetError};
    use serde::{Serialize, de::DeserializeOwned};
    use std::sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    };
    use tokio::sync::RwLock;

    #[derive(Clone)]
    struct TestTarget {
        active: bool,
        enabled: bool,
        failed_messages: Arc<AtomicU64>,
        id: TargetID,
        total_messages: Arc<AtomicU64>,
    }

    impl TestTarget {
        fn new(id: &str, name: &str) -> Self {
            Self {
                active: true,
                enabled: true,
                failed_messages: Arc::new(AtomicU64::new(0)),
                id: TargetID::new(id.to_string(), name.to_string()),
                total_messages: Arc::new(AtomicU64::new(0)),
            }
        }

        fn with_active(mut self, active: bool) -> Self {
            self.active = active;
            self
        }

        fn with_enabled(mut self, enabled: bool) -> Self {
            self.enabled = enabled;
            self
        }

        fn record_successes(&self, count: u64) {
            self.total_messages.store(count, Ordering::Relaxed);
        }

        fn record_failures(&self, count: u64) {
            self.failed_messages.store(count, Ordering::Relaxed);
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
            Ok(self.active)
        }

        async fn save(&self, _event: Arc<EntityTarget<E>>) -> Result<(), TargetError> {
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

        fn clone_dyn(&self) -> Box<dyn Target<E> + Send + Sync> {
            Box::new(self.clone())
        }

        async fn init(&self) -> Result<(), TargetError> {
            Ok(())
        }

        fn is_enabled(&self) -> bool {
            self.enabled
        }

        fn delivery_snapshot(&self) -> TargetDeliverySnapshot {
            TargetDeliverySnapshot {
                failed_messages: self.failed_messages.load(Ordering::Relaxed),
                queue_length: 0,
                total_messages: self.total_messages.load(Ordering::Relaxed),
            }
        }
    }

    #[tokio::test]
    async fn runtime_view_reports_empty_runtime_queries() {
        let runtime_view = NotifyRuntimeView::new(
            Arc::new(RwLock::new(TargetList::new())),
            Arc::new(RwLock::new(ReplayWorkerManager::new())),
        );

        assert!(runtime_view.get_active_targets().await.is_empty());
        assert!(runtime_view.get_target_values().await.is_empty());
        assert!(runtime_view.get_all_targets().read().await.is_empty());
    }

    #[tokio::test]
    async fn runtime_view_reports_empty_runtime_snapshots() {
        let runtime_view = NotifyRuntimeView::new(
            Arc::new(RwLock::new(TargetList::new())),
            Arc::new(RwLock::new(ReplayWorkerManager::new())),
        );

        assert!(runtime_view.snapshot_target_metrics().await.is_empty());
        assert!(runtime_view.snapshot_target_health().await.is_empty());

        let status = runtime_view.runtime_status_snapshot().await;
        assert_eq!(status.target_count, 0);
        assert_eq!(status.replay_worker_count, 0);
    }

    #[tokio::test]
    async fn runtime_view_reports_non_empty_runtime_queries_and_snapshots() {
        let target_list = Arc::new(RwLock::new(TargetList::new()));
        let replay_workers = Arc::new(RwLock::new(ReplayWorkerManager::new()));

        let online = Arc::new(TestTarget::new("primary", "webhook"));
        online.record_successes(3);
        online.record_failures(1);

        let disabled = Arc::new(TestTarget::new("backup", "mqtt").with_enabled(false).with_active(false));
        disabled.record_successes(2);

        {
            let mut targets = target_list.write().await;
            targets.add(online.clone() as Arc<dyn Target<Event> + Send + Sync>).unwrap();
            targets.add(disabled.clone() as Arc<dyn Target<Event> + Send + Sync>).unwrap();
        }

        let runtime_view = NotifyRuntimeView::new(target_list.clone(), replay_workers.clone());

        let mut active_targets = runtime_view.get_active_targets().await;
        active_targets.sort();
        assert_eq!(
            active_targets,
            vec![
                TargetID::new("backup".to_string(), "mqtt".to_string()),
                TargetID::new("primary".to_string(), "webhook".to_string())
            ]
        );

        let target_values = runtime_view.get_target_values().await;
        assert_eq!(target_values.len(), 2);
        assert_eq!(runtime_view.get_all_targets().read().await.len(), 2);

        let metric_snapshots = runtime_view.snapshot_target_metrics().await;
        assert_eq!(metric_snapshots.len(), 2);
        assert_eq!(metric_snapshots[0].target_id, "backup:mqtt");
        assert_eq!(metric_snapshots[0].failed_messages, 0);
        assert_eq!(metric_snapshots[0].total_messages, 2);
        assert_eq!(metric_snapshots[1].target_id, "primary:webhook");
        assert_eq!(metric_snapshots[1].failed_messages, 1);
        assert_eq!(metric_snapshots[1].total_messages, 3);

        let health_snapshots = runtime_view.snapshot_target_health().await;
        assert_eq!(health_snapshots.len(), 2);
        assert_eq!(health_snapshots[0].target_id, "backup:mqtt");
        assert!(!health_snapshots[0].enabled);
        assert_eq!(health_snapshots[0].state, rustfs_targets::RuntimeTargetHealthState::Disabled);
        assert_eq!(health_snapshots[1].target_id, "primary:webhook");
        assert!(health_snapshots[1].enabled);
        assert_eq!(health_snapshots[1].state, rustfs_targets::RuntimeTargetHealthState::Online);

        let status = runtime_view.runtime_status_snapshot().await;
        assert_eq!(status.target_count, 2);
        assert_eq!(status.replay_worker_count, 0);
    }
}
