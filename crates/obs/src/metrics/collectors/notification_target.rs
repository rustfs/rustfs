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

#![allow(dead_code)]

use crate::metrics::report::PrometheusMetric;
use crate::metrics::schema::notification_target::{
    NOTIFICATION_TARGET_FAILED_MESSAGES_MD, NOTIFICATION_TARGET_QUEUE_LENGTH_MD, NOTIFICATION_TARGET_TOTAL_MESSAGES_MD,
    TARGET_ID, TARGET_TYPE,
};
use std::borrow::Cow;

#[derive(Debug, Clone, Default)]
pub struct NotificationTargetStats {
    pub failed_messages: u64,
    pub queue_length: u64,
    pub target_id: String,
    pub target_type: String,
    pub total_messages: u64,
}

pub fn collect_notification_target_metrics(stats: &[NotificationTargetStats]) -> Vec<PrometheusMetric> {
    if stats.is_empty() {
        return Vec::new();
    }

    let mut metrics = Vec::with_capacity(stats.len() * 3);
    for stat in stats {
        let target_id: Cow<'static, str> = Cow::Owned(stat.target_id.clone());
        let target_type: Cow<'static, str> = Cow::Owned(stat.target_type.clone());

        metrics.push(
            PrometheusMetric::from_descriptor(&NOTIFICATION_TARGET_FAILED_MESSAGES_MD, stat.failed_messages as f64)
                .with_label(TARGET_ID, target_id.clone())
                .with_label(TARGET_TYPE, target_type.clone()),
        );
        metrics.push(
            PrometheusMetric::from_descriptor(&NOTIFICATION_TARGET_QUEUE_LENGTH_MD, stat.queue_length as f64)
                .with_label(TARGET_ID, target_id.clone())
                .with_label(TARGET_TYPE, target_type.clone()),
        );
        metrics.push(
            PrometheusMetric::from_descriptor(&NOTIFICATION_TARGET_TOTAL_MESSAGES_MD, stat.total_messages as f64)
                .with_label(TARGET_ID, target_id)
                .with_label(TARGET_TYPE, target_type),
        );
    }

    metrics
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_collect_notification_target_metrics() {
        let stats = vec![NotificationTargetStats {
            failed_messages: 2,
            queue_length: 4,
            target_id: "primary:webhook".to_string(),
            target_type: "webhook".to_string(),
            total_messages: 42,
        }];

        let metrics = collect_notification_target_metrics(&stats);

        assert_eq!(metrics.len(), 3);
        assert!(metrics.iter().any(|metric| {
            metric.value == 42.0
                && metric
                    .labels
                    .iter()
                    .any(|(key, value)| *key == TARGET_ID && value == "primary:webhook")
                && metric
                    .labels
                    .iter()
                    .any(|(key, value)| *key == TARGET_TYPE && value == "webhook")
        }));
    }
}
