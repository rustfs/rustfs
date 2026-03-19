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

//! Logger webhook metrics collector.
//!
//! Collects webhook metrics including failed messages, queue length,
//! and total messages per webhook target.

use crate::format::PrometheusMetric;
use crate::metrics_type::logger_webhook::{
    ENDPOINT_LABEL, NAME_LABEL, WEBHOOK_FAILED_MESSAGES_MD, WEBHOOK_QUEUE_LENGTH_MD, WEBHOOK_TOTAL_MESSAGES_MD,
};
use std::borrow::Cow;

/// Webhook target statistics.
#[derive(Debug, Clone, Default)]
pub struct WebhookTargetStats {
    /// Webhook name
    pub name: String,
    /// Webhook endpoint URL
    pub endpoint: String,
    /// Number of failed messages
    pub failed_messages: u64,
    /// Number of messages in queue
    pub queue_length: u64,
    /// Total number of messages sent
    pub total_messages: u64,
}

/// Collects webhook metrics from the given stats.
///
/// Uses the metric descriptors from `metrics_type::logger_webhook` module.
/// Returns a vector of Prometheus metrics for webhook statistics.
pub fn collect_webhook_metrics(stats: &[WebhookTargetStats]) -> Vec<PrometheusMetric> {
    let mut metrics = Vec::with_capacity(stats.len() * 3);

    for stat in stats {
        let name_label: Cow<'static, str> = Cow::Owned(stat.name.clone());
        let endpoint_label: Cow<'static, str> = Cow::Owned(stat.endpoint.clone());

        metrics.push(
            PrometheusMetric::from_descriptor(&WEBHOOK_FAILED_MESSAGES_MD, stat.failed_messages as f64)
                .with_label(NAME_LABEL, name_label.clone())
                .with_label(ENDPOINT_LABEL, endpoint_label.clone()),
        );
        metrics.push(
            PrometheusMetric::from_descriptor(&WEBHOOK_QUEUE_LENGTH_MD, stat.queue_length as f64)
                .with_label(NAME_LABEL, name_label.clone())
                .with_label(ENDPOINT_LABEL, endpoint_label.clone()),
        );
        metrics.push(
            PrometheusMetric::from_descriptor(&WEBHOOK_TOTAL_MESSAGES_MD, stat.total_messages as f64)
                .with_label(NAME_LABEL, name_label.clone())
                .with_label(ENDPOINT_LABEL, endpoint_label.clone()),
        );
    }

    metrics
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::format::report_metrics;

    #[test]
    fn test_collect_webhook_metrics() {
        let stats = vec![WebhookTargetStats {
            name: "alert-webhook".to_string(),
            endpoint: "https://hooks.example.com/alert".to_string(),
            failed_messages: 3,
            queue_length: 15,
            total_messages: 5000,
        }];

        let metrics = collect_webhook_metrics(&stats);
        report_metrics(&metrics);

        assert_eq!(metrics.len(), 3);

        // Verify we have metrics with the expected values
        let failed = metrics.iter().find(|m| m.value == 3.0);
        assert!(failed.is_some());

        // Verify labels
        let total = metrics.iter().find(|m| m.value == 5000.0);
        assert!(total.is_some());
        let total_metric = total.unwrap();
        assert!(
            total_metric
                .labels
                .iter()
                .any(|(k, v)| *k == NAME_LABEL && v == "alert-webhook")
        );
        assert!(
            total_metric
                .labels
                .iter()
                .any(|(k, v)| *k == ENDPOINT_LABEL && v == "https://hooks.example.com/alert")
        );
    }

    #[test]
    fn test_collect_webhook_metrics_empty() {
        let stats: Vec<WebhookTargetStats> = vec![];
        let metrics = collect_webhook_metrics(&stats);
        assert!(metrics.is_empty());
    }
}
