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

//! Bucket replication metrics collector.

use crate::metrics::report::PrometheusMetric;
use crate::metrics::schema::bucket_replication::{
    BUCKET_L, BUCKET_REPL_BANDWIDTH_CURRENT_MD, BUCKET_REPL_BANDWIDTH_LIMIT_MD, BUCKET_REPL_LAST_HR_FAILED_BYTES_MD,
    BUCKET_REPL_LAST_HR_FAILED_COUNT_MD, BUCKET_REPL_LAST_MIN_FAILED_BYTES_MD, BUCKET_REPL_LAST_MIN_FAILED_COUNT_MD,
    BUCKET_REPL_LATENCY_MS_MD, BUCKET_REPL_PROXIED_DELETE_TAGGING_REQUESTS_FAILURES_MD,
    BUCKET_REPL_PROXIED_DELETE_TAGGING_REQUESTS_TOTAL_MD, BUCKET_REPL_PROXIED_GET_REQUESTS_FAILURES_MD,
    BUCKET_REPL_PROXIED_GET_REQUESTS_TOTAL_MD, BUCKET_REPL_PROXIED_GET_TAGGING_REQUESTS_FAILURES_MD,
    BUCKET_REPL_PROXIED_GET_TAGGING_REQUESTS_TOTAL_MD, BUCKET_REPL_PROXIED_HEAD_REQUESTS_FAILURES_MD,
    BUCKET_REPL_PROXIED_HEAD_REQUESTS_TOTAL_MD, BUCKET_REPL_PROXIED_PUT_TAGGING_REQUESTS_FAILURES_MD,
    BUCKET_REPL_PROXIED_PUT_TAGGING_REQUESTS_TOTAL_MD, BUCKET_REPL_SENT_BYTES_MD, BUCKET_REPL_SENT_COUNT_MD,
    BUCKET_REPL_TOTAL_FAILED_BYTES_MD, BUCKET_REPL_TOTAL_FAILED_COUNT_MD, OPERATION_L, RANGE_L, TARGET_ARN_L,
};
use std::borrow::Cow;

#[derive(Debug, Clone, Default)]
pub struct BucketReplicationTargetStats {
    pub target_arn: String,
    pub bandwidth_limit_bytes_per_sec: u64,
    pub current_bandwidth_bytes_per_sec: f64,
    pub latency_ms: f64,
}

#[derive(Debug, Clone, Default)]
pub struct BucketReplicationBandwidthStats {
    pub bucket: String,
    pub target_arn: String,
    pub limit_bytes_per_sec: u64,
    pub current_bandwidth_bytes_per_sec: f64,
}

#[derive(Debug, Clone, Default)]
pub struct BucketReplicationStats {
    pub bucket: String,
    pub total_failed_bytes: u64,
    pub total_failed_count: u64,
    pub last_min_failed_bytes: u64,
    pub last_min_failed_count: u64,
    pub last_hour_failed_bytes: u64,
    pub last_hour_failed_count: u64,
    pub sent_bytes: u64,
    pub sent_count: u64,
    pub proxied_get_requests_total: u64,
    pub proxied_get_requests_failures: u64,
    pub proxied_head_requests_total: u64,
    pub proxied_head_requests_failures: u64,
    pub proxied_put_tagging_requests_total: u64,
    pub proxied_put_tagging_requests_failures: u64,
    pub proxied_get_tagging_requests_total: u64,
    pub proxied_get_tagging_requests_failures: u64,
    pub proxied_delete_tagging_requests_total: u64,
    pub proxied_delete_tagging_requests_failures: u64,
    pub targets: Vec<BucketReplicationTargetStats>,
}

pub fn collect_bucket_replication_bandwidth_metrics(stats: &[BucketReplicationBandwidthStats]) -> Vec<PrometheusMetric> {
    if stats.is_empty() {
        return Vec::new();
    }

    let mut metrics = Vec::with_capacity(stats.len() * 2);
    for stat in stats {
        let bucket_label: Cow<'static, str> = Cow::Owned(stat.bucket.clone());
        let target_arn_label: Cow<'static, str> = Cow::Owned(stat.target_arn.clone());

        metrics.push(
            PrometheusMetric::from_descriptor(&BUCKET_REPL_BANDWIDTH_LIMIT_MD, stat.limit_bytes_per_sec as f64)
                .with_label(BUCKET_L, bucket_label.clone())
                .with_label(TARGET_ARN_L, target_arn_label.clone()),
        );

        metrics.push(
            PrometheusMetric::from_descriptor(&BUCKET_REPL_BANDWIDTH_CURRENT_MD, stat.current_bandwidth_bytes_per_sec)
                .with_label(BUCKET_L, bucket_label)
                .with_label(TARGET_ARN_L, target_arn_label),
        );
    }

    metrics
}

pub fn collect_bucket_replication_metrics(stats: &[BucketReplicationStats]) -> Vec<PrometheusMetric> {
    if stats.is_empty() {
        return Vec::new();
    }

    let mut metrics = Vec::new();
    for stat in stats {
        let bucket_label: Cow<'static, str> = Cow::Owned(stat.bucket.clone());

        metrics.push(
            PrometheusMetric::from_descriptor(&BUCKET_REPL_TOTAL_FAILED_BYTES_MD, stat.total_failed_bytes as f64)
                .with_label(BUCKET_L, bucket_label.clone()),
        );
        metrics.push(
            PrometheusMetric::from_descriptor(&BUCKET_REPL_TOTAL_FAILED_COUNT_MD, stat.total_failed_count as f64)
                .with_label(BUCKET_L, bucket_label.clone()),
        );
        metrics.push(
            PrometheusMetric::from_descriptor(&BUCKET_REPL_LAST_MIN_FAILED_BYTES_MD, stat.last_min_failed_bytes as f64)
                .with_label(BUCKET_L, bucket_label.clone()),
        );
        metrics.push(
            PrometheusMetric::from_descriptor(&BUCKET_REPL_LAST_MIN_FAILED_COUNT_MD, stat.last_min_failed_count as f64)
                .with_label(BUCKET_L, bucket_label.clone()),
        );
        metrics.push(
            PrometheusMetric::from_descriptor(&BUCKET_REPL_LAST_HR_FAILED_BYTES_MD, stat.last_hour_failed_bytes as f64)
                .with_label(BUCKET_L, bucket_label.clone()),
        );
        metrics.push(
            PrometheusMetric::from_descriptor(&BUCKET_REPL_LAST_HR_FAILED_COUNT_MD, stat.last_hour_failed_count as f64)
                .with_label(BUCKET_L, bucket_label.clone()),
        );
        metrics.push(
            PrometheusMetric::from_descriptor(&BUCKET_REPL_SENT_BYTES_MD, stat.sent_bytes as f64)
                .with_label(BUCKET_L, bucket_label.clone()),
        );
        metrics.push(
            PrometheusMetric::from_descriptor(&BUCKET_REPL_SENT_COUNT_MD, stat.sent_count as f64)
                .with_label(BUCKET_L, bucket_label.clone()),
        );
        metrics.push(
            PrometheusMetric::from_descriptor(&BUCKET_REPL_PROXIED_GET_REQUESTS_TOTAL_MD, stat.proxied_get_requests_total as f64)
                .with_label(BUCKET_L, bucket_label.clone()),
        );
        metrics.push(
            PrometheusMetric::from_descriptor(
                &BUCKET_REPL_PROXIED_GET_REQUESTS_FAILURES_MD,
                stat.proxied_get_requests_failures as f64,
            )
            .with_label(BUCKET_L, bucket_label.clone()),
        );
        metrics.push(
            PrometheusMetric::from_descriptor(
                &BUCKET_REPL_PROXIED_HEAD_REQUESTS_TOTAL_MD,
                stat.proxied_head_requests_total as f64,
            )
            .with_label(BUCKET_L, bucket_label.clone()),
        );
        metrics.push(
            PrometheusMetric::from_descriptor(
                &BUCKET_REPL_PROXIED_HEAD_REQUESTS_FAILURES_MD,
                stat.proxied_head_requests_failures as f64,
            )
            .with_label(BUCKET_L, bucket_label.clone()),
        );
        metrics.push(
            PrometheusMetric::from_descriptor(
                &BUCKET_REPL_PROXIED_PUT_TAGGING_REQUESTS_TOTAL_MD,
                stat.proxied_put_tagging_requests_total as f64,
            )
            .with_label(BUCKET_L, bucket_label.clone()),
        );
        metrics.push(
            PrometheusMetric::from_descriptor(
                &BUCKET_REPL_PROXIED_PUT_TAGGING_REQUESTS_FAILURES_MD,
                stat.proxied_put_tagging_requests_failures as f64,
            )
            .with_label(BUCKET_L, bucket_label.clone()),
        );
        metrics.push(
            PrometheusMetric::from_descriptor(
                &BUCKET_REPL_PROXIED_GET_TAGGING_REQUESTS_TOTAL_MD,
                stat.proxied_get_tagging_requests_total as f64,
            )
            .with_label(BUCKET_L, bucket_label.clone()),
        );
        metrics.push(
            PrometheusMetric::from_descriptor(
                &BUCKET_REPL_PROXIED_GET_TAGGING_REQUESTS_FAILURES_MD,
                stat.proxied_get_tagging_requests_failures as f64,
            )
            .with_label(BUCKET_L, bucket_label.clone()),
        );
        metrics.push(
            PrometheusMetric::from_descriptor(
                &BUCKET_REPL_PROXIED_DELETE_TAGGING_REQUESTS_TOTAL_MD,
                stat.proxied_delete_tagging_requests_total as f64,
            )
            .with_label(BUCKET_L, bucket_label.clone()),
        );
        metrics.push(
            PrometheusMetric::from_descriptor(
                &BUCKET_REPL_PROXIED_DELETE_TAGGING_REQUESTS_FAILURES_MD,
                stat.proxied_delete_tagging_requests_failures as f64,
            )
            .with_label(BUCKET_L, bucket_label.clone()),
        );

        for target in &stat.targets {
            let target_label: Cow<'static, str> = Cow::Owned(target.target_arn.clone());
            metrics.push(
                PrometheusMetric::from_descriptor(&BUCKET_REPL_LATENCY_MS_MD, target.latency_ms)
                    .with_label(BUCKET_L, bucket_label.clone())
                    .with_label(OPERATION_L, Cow::Borrowed("object_replication"))
                    .with_label(RANGE_L, Cow::Borrowed("all"))
                    .with_label(TARGET_ARN_L, target_label),
            );
        }
    }

    metrics
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_collect_bucket_replication_metrics() {
        let stats = vec![BucketReplicationStats {
            bucket: "b1".to_string(),
            total_failed_bytes: 64,
            total_failed_count: 2,
            last_min_failed_bytes: 32,
            last_min_failed_count: 1,
            last_hour_failed_bytes: 64,
            last_hour_failed_count: 2,
            sent_bytes: 1024,
            sent_count: 8,
            proxied_get_requests_total: 5,
            proxied_get_requests_failures: 1,
            proxied_head_requests_total: 4,
            proxied_head_requests_failures: 0,
            proxied_put_tagging_requests_total: 3,
            proxied_put_tagging_requests_failures: 1,
            proxied_get_tagging_requests_total: 2,
            proxied_get_tagging_requests_failures: 0,
            proxied_delete_tagging_requests_total: 1,
            proxied_delete_tagging_requests_failures: 0,
            targets: vec![BucketReplicationTargetStats {
                target_arn: "arn:rustfs:replication:us-east-1:1:target".to_string(),
                bandwidth_limit_bytes_per_sec: 2048,
                current_bandwidth_bytes_per_sec: 1024.0,
                latency_ms: 15.0,
            }],
        }];

        let metrics = collect_bucket_replication_metrics(&stats);
        assert_eq!(metrics.len(), 19);

        let sent_name = BUCKET_REPL_SENT_COUNT_MD.get_full_metric_name();
        assert!(metrics.iter().any(|metric| {
            metric.name == sent_name
                && metric.value == 8.0
                && metric.labels.iter().any(|(key, value)| *key == BUCKET_L && value == "b1")
        }));

        let latency_name = BUCKET_REPL_LATENCY_MS_MD.get_full_metric_name();
        assert!(metrics.iter().any(|metric| {
            metric.name == latency_name
                && metric.value == 15.0
                && metric
                    .labels
                    .iter()
                    .any(|(key, value)| *key == TARGET_ARN_L && value == "arn:rustfs:replication:us-east-1:1:target")
        }));
    }

    #[test]
    fn test_collect_bucket_replication_metrics_empty() {
        let stats: Vec<BucketReplicationStats> = Vec::new();
        let metrics = collect_bucket_replication_metrics(&stats);
        assert!(metrics.is_empty());
    }

    #[test]
    fn test_collect_bucket_replication_bandwidth_metrics() {
        let stats = vec![BucketReplicationBandwidthStats {
            bucket: "b1".to_string(),
            target_arn: "arn:rustfs:replication:us-east-1:1:test-2".to_string(),
            limit_bytes_per_sec: 1_048_576,
            current_bandwidth_bytes_per_sec: 204_800.0,
        }];

        let metrics = collect_bucket_replication_bandwidth_metrics(&stats);
        assert_eq!(metrics.len(), 2);

        let limit_metric_name = BUCKET_REPL_BANDWIDTH_LIMIT_MD.get_full_metric_name();
        let limit_metric = metrics.iter().find(|metric| {
            metric.name == limit_metric_name
                && metric.value == 1_048_576.0
                && metric.labels.iter().any(|(key, value)| *key == BUCKET_L && value == "b1")
        });
        assert!(limit_metric.is_some());
        assert!(
            limit_metric
                .and_then(|metric| {
                    metric
                        .labels
                        .iter()
                        .find(|(key, _)| *key == TARGET_ARN_L)
                        .map(|(_, value)| value.as_ref() == "arn:rustfs:replication:us-east-1:1:test-2")
                })
                .unwrap_or(false)
        );

        let current_metric_name = BUCKET_REPL_BANDWIDTH_CURRENT_MD.get_full_metric_name();
        let current_metric = metrics.iter().find(|metric| {
            metric.name == current_metric_name
                && metric.value == 204_800.0
                && metric.labels.iter().any(|(key, value)| *key == BUCKET_L && value == "b1")
        });
        assert!(current_metric.is_some());
    }

    #[test]
    fn test_collect_bucket_replication_bandwidth_metrics_empty() {
        let stats: Vec<BucketReplicationBandwidthStats> = Vec::new();
        let metrics = collect_bucket_replication_bandwidth_metrics(&stats);
        assert!(metrics.is_empty());
    }
}
