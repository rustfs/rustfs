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
    Event,
    bucket_config_manager::NotifyBucketConfigManager,
    config_manager::NotifyConfigManager,
    integration::NotificationMetrics,
    notification_system_subscriber::NotificationSystemSubscriberView,
    notifier::{EventNotifier, SharedNotifyTargetList},
    pipeline::{LiveEventHistory, NotifyPipeline},
    registry::TargetRegistry,
    rule_engine::NotifyRuleEngine,
    runtime_facade::NotifyRuntimeFacade,
    runtime_view::NotifyRuntimeView,
    status_view::NotifyStatusView,
};
use rustfs_ecstore::config::Config;
use rustfs_targets::ReplayWorkerManager;
use std::sync::Arc;
use tokio::sync::{RwLock, Semaphore, broadcast};

#[derive(Clone)]
pub struct NotifyServices {
    pub bucket_config_manager: NotifyBucketConfigManager,
    pub config_manager: NotifyConfigManager,
    pub pipeline: NotifyPipeline,
    pub runtime_facade: NotifyRuntimeFacade,
    pub runtime_view: NotifyRuntimeView,
    pub status_view: NotifyStatusView,
}

impl NotifyServices {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        notifier: Arc<EventNotifier>,
        rule_engine: NotifyRuleEngine,
        target_list: SharedNotifyTargetList,
        registry: Arc<TargetRegistry>,
        config: Arc<RwLock<Config>>,
        stream_cancellers: Arc<RwLock<ReplayWorkerManager>>,
        concurrency_limiter: Arc<Semaphore>,
        metrics: Arc<NotificationMetrics>,
        subscriber_view: Arc<NotificationSystemSubscriberView>,
        live_event_sender: broadcast::Sender<Arc<Event>>,
        live_event_history: Arc<RwLock<LiveEventHistory>>,
    ) -> Self {
        let runtime_view = NotifyRuntimeView::new(target_list.clone(), stream_cancellers.clone());
        let runtime_facade = NotifyRuntimeFacade::new(target_list, stream_cancellers, concurrency_limiter, metrics.clone());
        let config_manager = NotifyConfigManager::new(config, registry, rule_engine.clone(), runtime_facade.clone());
        let bucket_config_manager = NotifyBucketConfigManager::new(notifier.clone(), rule_engine, subscriber_view);
        let pipeline = NotifyPipeline::new(notifier, live_event_sender, live_event_history);
        let status_view = NotifyStatusView::new(metrics);

        Self {
            bucket_config_manager,
            config_manager,
            pipeline,
            runtime_facade,
            runtime_view,
            status_view,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::NotifyServices;
    use crate::{
        integration::NotificationMetrics, notification_system_subscriber::NotificationSystemSubscriberView,
        notifier::EventNotifier, pipeline::LiveEventHistory, registry::TargetRegistry, rule_engine::NotifyRuleEngine,
    };
    use rustfs_ecstore::config::Config;
    use rustfs_targets::ReplayWorkerManager;
    use std::sync::Arc;
    use tokio::sync::{RwLock, Semaphore, broadcast};

    #[tokio::test]
    async fn services_build_empty_runtime_views() {
        let metrics = Arc::new(NotificationMetrics::new());
        let rule_engine = NotifyRuleEngine::new();
        let notifier = Arc::new(EventNotifier::new(metrics.clone(), rule_engine.clone()));
        let target_list = notifier.target_list();
        let registry = Arc::new(TargetRegistry::new());
        let config = Arc::new(RwLock::new(Config::default()));
        let stream_cancellers = Arc::new(RwLock::new(ReplayWorkerManager::new()));
        let concurrency_limiter = Arc::new(Semaphore::new(4));
        let subscriber_view = Arc::new(NotificationSystemSubscriberView::new());
        let (live_event_sender, _) = broadcast::channel(16);
        let live_event_history = Arc::new(RwLock::new(LiveEventHistory::default()));

        let services = NotifyServices::new(
            notifier,
            rule_engine,
            target_list,
            registry,
            config,
            stream_cancellers,
            concurrency_limiter,
            metrics,
            subscriber_view,
            live_event_sender,
            live_event_history,
        );

        assert!(services.runtime_view.get_active_targets().await.is_empty());
        assert_eq!(services.status_view.snapshot_metrics().events_sent_total, 0);
    }
}
