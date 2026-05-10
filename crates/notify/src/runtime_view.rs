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

use crate::{Event, NotificationTargetMetricSnapshot, notifier::TargetList};
use rustfs_targets::{ReplayWorkerManager, RuntimeTargetHealthSnapshot, Target, arn::TargetID};
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Clone)]
pub struct NotifyRuntimeView {
    target_list: Arc<RwLock<TargetList>>,
    stream_cancellers: Arc<RwLock<ReplayWorkerManager>>,
}

impl NotifyRuntimeView {
    pub fn new(target_list: Arc<RwLock<TargetList>>, stream_cancellers: Arc<RwLock<ReplayWorkerManager>>) -> Self {
        Self {
            target_list,
            stream_cancellers,
        }
    }

    pub async fn get_active_targets(&self) -> Vec<TargetID> {
        self.target_list.read().await.keys()
    }

    pub fn get_all_targets(&self) -> Arc<RwLock<TargetList>> {
        self.target_list.clone()
    }

    pub async fn get_target_values(&self) -> Vec<Arc<dyn Target<Event> + Send + Sync>> {
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
    use crate::notifier::TargetList;
    use rustfs_targets::ReplayWorkerManager;
    use std::sync::Arc;
    use tokio::sync::RwLock;

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
}
