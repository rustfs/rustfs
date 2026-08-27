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
                failed_store_length: snapshot.failed_store_length,
                queue_length: snapshot.queue_length,
                target_id: snapshot.target_id,
                target_type: snapshot.target_type,
                total_messages: snapshot.total_messages,
            })
            .collect()
    }

    pub async fn snapshot_target_health(&self) -> Vec<RuntimeTargetHealthSnapshot> {
        let targets = self.target_list.read().await.values();
        rustfs_targets::health_snapshots_for_targets(targets).await
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
    use rustfs_targets::arn::TargetID;
    use rustfs_targets::target::TargetDeliverySnapshot;
    use rustfs_targets::testkit::MockTarget;
    use rustfs_targets::{ReplayWorkerManager, Target};
    use std::sync::Arc;
    use tokio::sync::{Notify, RwLock};

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

        let online = Arc::new(MockTarget::new("primary", "webhook").with_delivery_snapshot(TargetDeliverySnapshot {
            failed_messages: 1,
            failed_store_length: 7,
            queue_length: 0,
            total_messages: 3,
        }));

        let disabled = Arc::new(
            MockTarget::new("backup", "mqtt")
                .disabled()
                .with_active(false)
                .with_delivery_snapshot(TargetDeliverySnapshot {
                    total_messages: 2,
                    ..TargetDeliverySnapshot::default()
                }),
        );

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
        assert_eq!(metric_snapshots[0].failed_store_length, 0);
        assert_eq!(metric_snapshots[0].total_messages, 2);
        assert_eq!(metric_snapshots[1].target_id, "primary:webhook");
        assert_eq!(metric_snapshots[1].failed_messages, 1);
        assert_eq!(metric_snapshots[1].failed_store_length, 7);
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

    #[tokio::test]
    async fn health_probe_does_not_hold_the_target_list_read_lock() {
        let target_list = Arc::new(RwLock::new(TargetList::new()));
        let release = Arc::new(Notify::new());
        let target = MockTarget::new("blocked", "webhook").with_health_gate(release.clone());
        let started = target.health_started();
        target_list
            .write()
            .await
            .add(Arc::new(target) as Arc<dyn Target<Event> + Send + Sync>)
            .expect("test target should be added");
        let runtime_view = NotifyRuntimeView::new(target_list.clone(), Arc::new(RwLock::new(ReplayWorkerManager::new())));

        let snapshot_task = tokio::spawn(async move { runtime_view.snapshot_target_health().await });
        started.notified().await;

        let write_guard = tokio::time::timeout(std::time::Duration::from_secs(1), target_list.write())
            .await
            .expect("network health probe must not retain the target-list read lock");
        drop(write_guard);
        release.notify_one();

        assert_eq!(snapshot_task.await.expect("snapshot task should finish").len(), 1);
    }
}
