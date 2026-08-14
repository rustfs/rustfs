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

use crate::metrics::report::PrometheusMetric;
use crate::metrics::schema::notification_target::{
    NOTIFICATION_TARGET_FAILED_MESSAGES_BY_SERVER_MD, NOTIFICATION_TARGET_FAILED_MESSAGES_MD,
    NOTIFICATION_TARGET_FAILED_STORE_LENGTH_BY_SERVER_MD, NOTIFICATION_TARGET_FAILED_STORE_LENGTH_MD,
    NOTIFICATION_TARGET_QUEUE_LENGTH_BY_SERVER_MD, NOTIFICATION_TARGET_QUEUE_LENGTH_MD,
    NOTIFICATION_TARGET_TOTAL_MESSAGES_BY_SERVER_MD, NOTIFICATION_TARGET_TOTAL_MESSAGES_MD, SERVER, TARGET_ID, TARGET_TYPE,
};
use std::borrow::Cow;

#[derive(Debug, Clone, Default)]
pub struct NotificationTargetStats {
    pub failed_messages: u64,
    pub failed_store_length: u64,
    pub queue_length: u64,
    pub target_id: String,
    pub target_type: String,
    pub total_messages: u64,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct NotificationTargetRuntimeStats {
    pub(crate) server: String,
    pub(crate) target: NotificationTargetStats,
}

pub fn collect_notification_target_metrics(stats: &[NotificationTargetStats]) -> Vec<PrometheusMetric> {
    if stats.is_empty() {
        return Vec::new();
    }

    let mut metrics = Vec::with_capacity(stats.len() * 8);
    for stat in stats {
        let target_id: Cow<'static, str> = Cow::Owned(stat.target_id.clone());
        let target_type: Cow<'static, str> = Cow::Owned(stat.target_type.clone());

        metrics.push(
            PrometheusMetric::from_descriptor(&NOTIFICATION_TARGET_FAILED_MESSAGES_MD, stat.failed_messages as f64)
                .with_label(TARGET_ID, target_id.clone())
                .with_label(TARGET_TYPE, target_type.clone()),
        );
        metrics.push(
            PrometheusMetric::from_descriptor(&NOTIFICATION_TARGET_FAILED_STORE_LENGTH_MD, stat.failed_store_length as f64)
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

pub(crate) fn collect_notification_target_runtime_metrics(stats: &[NotificationTargetRuntimeStats]) -> Vec<PrometheusMetric> {
    if stats.is_empty() {
        return Vec::new();
    }

    let legacy_stats = stats.iter().map(|stat| stat.target.clone()).collect::<Vec<_>>();
    let mut metrics = collect_notification_target_metrics(&legacy_stats);
    metrics.reserve(stats.len() * 4);
    for stat in stats {
        let server: Cow<'static, str> = Cow::Owned(stat.server.clone());
        let target_id: Cow<'static, str> = Cow::Owned(stat.target.target_id.clone());
        let target_type: Cow<'static, str> = Cow::Owned(stat.target.target_type.clone());

        metrics.push(
            PrometheusMetric::from_descriptor(
                &NOTIFICATION_TARGET_FAILED_MESSAGES_BY_SERVER_MD,
                stat.target.failed_messages as f64,
            )
            .with_label(SERVER, server.clone())
            .with_label(TARGET_ID, target_id.clone())
            .with_label(TARGET_TYPE, target_type.clone()),
        );
        metrics.push(
            PrometheusMetric::from_descriptor(
                &NOTIFICATION_TARGET_FAILED_STORE_LENGTH_BY_SERVER_MD,
                stat.target.failed_store_length as f64,
            )
            .with_label(SERVER, server.clone())
            .with_label(TARGET_ID, target_id.clone())
            .with_label(TARGET_TYPE, target_type.clone()),
        );
        metrics.push(
            PrometheusMetric::from_descriptor(&NOTIFICATION_TARGET_QUEUE_LENGTH_BY_SERVER_MD, stat.target.queue_length as f64)
                .with_label(SERVER, server.clone())
                .with_label(TARGET_ID, target_id.clone())
                .with_label(TARGET_TYPE, target_type.clone()),
        );
        metrics.push(
            PrometheusMetric::from_descriptor(
                &NOTIFICATION_TARGET_TOTAL_MESSAGES_BY_SERVER_MD,
                stat.target.total_messages as f64,
            )
            .with_label(SERVER, server)
            .with_label(TARGET_ID, target_id)
            .with_label(TARGET_TYPE, target_type),
        );
    }

    metrics
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::schema::MetricType;

    #[test]
    fn test_collect_notification_target_metrics() {
        let stats = [NotificationTargetStats {
            failed_messages: 2,
            failed_store_length: 3,
            queue_length: 4,
            target_id: "primary:webhook".to_string(),
            target_type: "webhook".to_string(),
            total_messages: 42,
        }];

        let metrics = collect_notification_target_runtime_metrics(&[NotificationTargetRuntimeStats {
            server: "node1:9000".to_string(),
            target: stats[0].clone(),
        }]);

        assert_eq!(metrics.len(), 8);
        assert!(metrics.iter().any(|metric| {
            metric.value == 3.0
                && metric.name == NOTIFICATION_TARGET_FAILED_STORE_LENGTH_MD.get_full_metric_name()
                && metric
                    .labels
                    .iter()
                    .any(|(key, value)| *key == TARGET_ID && value == "primary:webhook")
        }));
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
        assert!(metrics.iter().any(|metric| {
            metric.value == 4.0
                && metric.name == NOTIFICATION_TARGET_QUEUE_LENGTH_BY_SERVER_MD.get_full_metric_name()
                && metric
                    .labels
                    .iter()
                    .any(|(key, value)| *key == SERVER && value == "node1:9000")
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

    #[test]
    fn notification_target_totals_are_exported_as_gauges() {
        assert_eq!(NOTIFICATION_TARGET_FAILED_MESSAGES_MD.metric_type, MetricType::Gauge);
        assert_eq!(NOTIFICATION_TARGET_FAILED_STORE_LENGTH_MD.metric_type, MetricType::Gauge);
        assert_eq!(NOTIFICATION_TARGET_QUEUE_LENGTH_MD.metric_type, MetricType::Gauge);
        assert_eq!(NOTIFICATION_TARGET_TOTAL_MESSAGES_MD.metric_type, MetricType::Gauge);
    }

    #[test]
    fn notification_target_stats_struct_literal_keeps_legacy_fields() {
        let stats = vec![NotificationTargetStats {
            failed_messages: 2,
            failed_store_length: 3,
            queue_length: 4,
            target_id: "primary:webhook".to_string(),
            target_type: "webhook".to_string(),
            total_messages: 42,
        }];

        assert_eq!(collect_notification_target_metrics(&stats).len(), 4);
    }
}
