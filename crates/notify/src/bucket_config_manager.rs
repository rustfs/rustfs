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
    BucketNotificationConfig, NotificationError, config_manager::notify_configuration_hint,
    notification_system_subscriber::NotificationSystemSubscriberView, notifier::EventNotifier, rule_engine::NotifyRuleEngine,
    rules::ParseConfigError,
};
use rustfs_s3_types::EventName;
use std::sync::Arc;
use tracing::{debug, info, warn};

const LOG_COMPONENT_NOTIFY: &str = "notify";
const LOG_SUBSYSTEM_BUCKET_CONFIG: &str = "bucket_config";
const EVENT_NOTIFY_BUCKET_CONFIG_VALIDATION: &str = "notify_bucket_config_validation";
const EVENT_NOTIFY_BUCKET_CONFIG_LOADED: &str = "notify_bucket_config_loaded";

#[derive(Clone)]
pub struct NotifyBucketConfigManager {
    notifier: Arc<EventNotifier>,
    rule_engine: NotifyRuleEngine,
    subscriber_view: Arc<NotificationSystemSubscriberView>,
}

impl NotifyBucketConfigManager {
    pub fn new(
        notifier: Arc<EventNotifier>,
        rule_engine: NotifyRuleEngine,
        subscriber_view: Arc<NotificationSystemSubscriberView>,
    ) -> Self {
        Self {
            notifier,
            rule_engine,
            subscriber_view,
        }
    }

    pub async fn has_subscriber(&self, bucket: &str, event: &EventName) -> bool {
        if !self.subscriber_view.has_subscriber(bucket, event) {
            return false;
        }
        self.rule_engine.has_subscriber(bucket, event).await
    }

    pub async fn load_bucket_notification_config(
        &self,
        bucket: &str,
        cfg: &BucketNotificationConfig,
    ) -> Result<(), NotificationError> {
        let arn_list = self.notifier.get_arn_list(&cfg.region).await;
        if arn_list.is_empty() {
            return Err(NotificationError::Configuration(notify_configuration_hint()));
        }
        debug!(
            event = EVENT_NOTIFY_BUCKET_CONFIG_VALIDATION,
            component = LOG_COMPONENT_NOTIFY,
            subsystem = LOG_SUBSYSTEM_BUCKET_CONFIG,
            bucket = %bucket,
            region = %cfg.region,
            available_arn_count = arn_list.len(),
            "notify bucket config validation"
        );

        if let Err(e) = cfg.validate(&cfg.region, &arn_list) {
            debug!(
                event = EVENT_NOTIFY_BUCKET_CONFIG_VALIDATION,
                component = LOG_COMPONENT_NOTIFY,
                subsystem = LOG_SUBSYSTEM_BUCKET_CONFIG,
                bucket = %bucket,
                region = %cfg.region,
                error = %e,
                result = "validation_failed",
                "notify bucket config validation"
            );
            if !matches!(e, ParseConfigError::ArnNotFound(_)) {
                return Err(NotificationError::BucketNotification(e.to_string()));
            }
            warn!(
                event = EVENT_NOTIFY_BUCKET_CONFIG_VALIDATION,
                component = LOG_COMPONENT_NOTIFY,
                subsystem = LOG_SUBSYSTEM_BUCKET_CONFIG,
                bucket = %bucket,
                region = %cfg.region,
                error = %e,
                result = "missing_target_arn",
                "notify bucket config validation"
            );
        }

        self.subscriber_view.apply_bucket_config(bucket, cfg);
        self.rule_engine.set_bucket_rules(bucket, cfg.get_rules_map().clone()).await;
        info!(
            event = EVENT_NOTIFY_BUCKET_CONFIG_LOADED,
            component = LOG_COMPONENT_NOTIFY,
            subsystem = LOG_SUBSYSTEM_BUCKET_CONFIG,
            bucket = %bucket,
            region = %cfg.region,
            rule_count = cfg.get_rules_map().inner().len(),
            "notify bucket config state"
        );
        Ok(())
    }

    pub async fn remove_bucket_notification_config(&self, bucket: &str) {
        self.subscriber_view.clear_bucket(bucket);
        self.rule_engine.clear_bucket_rules(bucket).await;
    }
}

#[cfg(test)]
mod tests {
    use super::NotifyBucketConfigManager;
    use crate::{
        BucketNotificationConfig, integration::NotificationMetrics,
        notification_system_subscriber::NotificationSystemSubscriberView, notifier::EventNotifier, rule_engine::NotifyRuleEngine,
    };
    use rustfs_s3_types::EventName;
    use rustfs_targets::arn::TargetID;
    use std::sync::Arc;

    fn build_manager() -> NotifyBucketConfigManager {
        let metrics = Arc::new(NotificationMetrics::new());
        let rule_engine = NotifyRuleEngine::new();
        let notifier = Arc::new(EventNotifier::new(metrics, rule_engine.clone()));
        let subscriber_view = Arc::new(NotificationSystemSubscriberView::new());
        NotifyBucketConfigManager::new(notifier, rule_engine, subscriber_view)
    }

    #[tokio::test]
    async fn bucket_config_manager_reports_no_subscriber_for_empty_state() {
        let manager = build_manager();
        assert!(!manager.has_subscriber("bucket", &EventName::ObjectCreatedPut).await);
    }

    #[tokio::test]
    async fn bucket_config_manager_clears_bucket_snapshot() {
        let manager = build_manager();
        let target_id = TargetID::new("primary".to_string(), "webhook".to_string());
        let mut cfg = BucketNotificationConfig::new("us-east-1");
        cfg.add_rule(&[EventName::ObjectCreatedPut], "*".to_string(), target_id);

        manager.subscriber_view.apply_bucket_config("bucket", &cfg);
        assert!(manager.subscriber_view.has_subscriber("bucket", &EventName::ObjectCreatedPut));

        manager.remove_bucket_notification_config("bucket").await;
        assert!(!manager.subscriber_view.has_subscriber("bucket", &EventName::ObjectCreatedPut));
    }
}
