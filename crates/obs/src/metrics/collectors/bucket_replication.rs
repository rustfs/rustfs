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
    BUCKET_L, BUCKET_REPL_BANDWIDTH_CURRENT_MD, BUCKET_REPL_BANDWIDTH_LIMIT_MD, BUCKET_REPL_CURRENT_BACKLOG_BYTES_MD,
    BUCKET_REPL_CURRENT_BACKLOG_COUNT_MD, BUCKET_REPL_CURRENT_TARGET_BACKLOG_BYTES_MD,
    BUCKET_REPL_CURRENT_TARGET_BACKLOG_COUNT_MD, BUCKET_REPL_DURABLE_MRF_AVAILABLE_MD, BUCKET_REPL_DURABLE_MRF_BACKLOG_BYTES_MD,
    BUCKET_REPL_DURABLE_MRF_BACKLOG_COUNT_MD, BUCKET_REPL_DURABLE_MRF_TARGET_BACKLOG_BYTES_MD,
    BUCKET_REPL_DURABLE_MRF_TARGET_BACKLOG_COUNT_MD, BUCKET_REPL_LAST_HR_FAILED_BYTES_MD, BUCKET_REPL_LAST_HR_FAILED_COUNT_MD,
    BUCKET_REPL_LAST_MIN_FAILED_BYTES_MD, BUCKET_REPL_LAST_MIN_FAILED_COUNT_MD, BUCKET_REPL_LATENCY_MS_MD,
    BUCKET_REPL_MRF_DROPPED_COUNT_MD, BUCKET_REPL_MRF_FLUSH_FAILURES_MD, BUCKET_REPL_MRF_LAST_FLUSH_DURATION_MILLIS_MD,
    BUCKET_REPL_MRF_MISSED_COUNT_MD, BUCKET_REPL_MRF_PENDING_BYTES_MD, BUCKET_REPL_MRF_PENDING_COUNT_MD,
    BUCKET_REPL_PROXIED_DELETE_TAGGING_REQUESTS_FAILURES_MD, BUCKET_REPL_PROXIED_DELETE_TAGGING_REQUESTS_TOTAL_MD,
    BUCKET_REPL_PROXIED_GET_REQUESTS_FAILURES_MD, BUCKET_REPL_PROXIED_GET_REQUESTS_TOTAL_MD,
    BUCKET_REPL_PROXIED_GET_TAGGING_REQUESTS_FAILURES_MD, BUCKET_REPL_PROXIED_GET_TAGGING_REQUESTS_TOTAL_MD,
    BUCKET_REPL_PROXIED_HEAD_REQUESTS_FAILURES_MD, BUCKET_REPL_PROXIED_HEAD_REQUESTS_TOTAL_MD,
    BUCKET_REPL_PROXIED_PUT_REQUESTS_FAILURES_MD, BUCKET_REPL_PROXIED_PUT_REQUESTS_TOTAL_MD,
    BUCKET_REPL_PROXIED_PUT_TAGGING_REQUESTS_FAILURES_MD, BUCKET_REPL_PROXIED_PUT_TAGGING_REQUESTS_TOTAL_MD,
    BUCKET_REPL_PROXY_REQUESTS_TOTAL_MD, BUCKET_REPL_RESYNC_CANCELED_TOTAL_MD, BUCKET_REPL_RESYNC_COMPLETED_TOTAL_MD,
    BUCKET_REPL_RESYNC_DURATION_MS_TOTAL_MD, BUCKET_REPL_RESYNC_FAILED_TOTAL_MD, BUCKET_REPL_RESYNC_STARTED_TOTAL_MD,
    BUCKET_REPL_SENT_BYTES_MD, BUCKET_REPL_SENT_COUNT_MD, BUCKET_REPL_TARGET_LAST_HOUR_FAILED_BYTES_MD,
    BUCKET_REPL_TARGET_LAST_HOUR_FAILED_COUNT_MD, BUCKET_REPL_TARGET_LAST_MIN_FAILED_BYTES_MD,
    BUCKET_REPL_TARGET_LAST_MIN_FAILED_COUNT_MD, BUCKET_REPL_TARGET_SENT_BYTES_MD, BUCKET_REPL_TARGET_SENT_COUNT_MD,
    BUCKET_REPL_TARGET_TOTAL_FAILED_BYTES_MD, BUCKET_REPL_TARGET_TOTAL_FAILED_COUNT_MD, BUCKET_REPL_TOTAL_FAILED_BYTES_MD,
    BUCKET_REPL_TOTAL_FAILED_COUNT_MD, OPERATION_L, RANGE_L, RESULT_L, TARGET_ARN_L,
};
use std::borrow::Cow;

const BASE_BUCKET_REPLICATION_METRICS_PER_BUCKET: usize = 37;
const BUCKET_REPLICATION_RUNTIME_FLOW_METRICS_PER_TARGET: usize = 8;
const BASE_BUCKET_REPLICATION_BACKLOG_METRICS_PER_BUCKET: usize = 11;
const BUCKET_REPLICATION_BACKLOG_METRICS_PER_TARGET: usize = 4;

#[derive(Debug, Clone, Default)]
pub struct BucketReplicationTargetStats {
    pub target_arn: String,
    pub bandwidth_limit_bytes_per_sec: u64,
    pub current_bandwidth_bytes_per_sec: f64,
    pub latency_ms: f64,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct BucketReplicationTargetFlowStats {
    pub(crate) target_arn: String,
    pub sent_bytes: u64,
    pub sent_count: u64,
    pub total_failed_bytes: u64,
    pub total_failed_count: u64,
    pub last_min_failed_bytes: u64,
    pub last_min_failed_count: u64,
    pub last_hour_failed_bytes: u64,
    pub last_hour_failed_count: u64,
}

#[derive(Debug, Clone, Default)]
pub struct BucketReplicationBandwidthStats {
    pub bucket: String,
    pub target_arn: String,
    pub limit_bytes_per_sec: u64,
    pub current_bandwidth_bytes_per_sec: f64,
}

#[derive(Debug, Clone, Default)]
pub struct BucketReplicationMetricsSnapshot {
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
    pub proxied_put_requests_total: u64,
    pub proxied_put_requests_failures: u64,
    pub proxied_put_tagging_requests_total: u64,
    pub proxied_put_tagging_requests_failures: u64,
    pub proxied_get_tagging_requests_total: u64,
    pub proxied_get_tagging_requests_failures: u64,
    pub proxied_delete_tagging_requests_total: u64,
    pub proxied_delete_tagging_requests_failures: u64,
    pub resync_started_count: u64,
    pub resync_completed_count: u64,
    pub resync_failed_count: u64,
    pub resync_canceled_count: u64,
    pub resync_duration_ms: u64,
    pub targets: Vec<BucketReplicationTargetStats>,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct BucketReplicationRuntimeStats {
    pub(crate) stats: BucketReplicationMetricsSnapshot,
    pub(crate) target_flows: Vec<BucketReplicationTargetFlowStats>,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct BucketReplicationBacklogStats {
    pub(crate) bucket: String,
    pub(crate) current_backlog_count: u64,
    pub(crate) current_backlog_bytes: u64,
    pub(crate) durable_mrf_available: bool,
    pub(crate) durable_mrf_backlog_count: u64,
    pub(crate) durable_mrf_backlog_bytes: u64,
    pub(crate) mrf_pending_count: u64,
    pub(crate) mrf_pending_bytes: u64,
    pub(crate) mrf_dropped_count: u64,
    pub(crate) mrf_missed_count: u64,
    pub(crate) mrf_flush_failures: u64,
    pub(crate) mrf_last_flush_duration_millis: u64,
    pub(crate) target_backlogs: Vec<BucketReplicationTargetBacklogStats>,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct BucketReplicationTargetBacklogStats {
    pub(crate) target_arn: String,
    pub(crate) current_backlog_count: u64,
    pub(crate) current_backlog_bytes: u64,
    pub(crate) durable_mrf_backlog_count: u64,
    pub(crate) durable_mrf_backlog_bytes: u64,
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

fn push_proxy_request_result_metrics(
    metrics: &mut Vec<PrometheusMetric>,
    bucket_label: Cow<'static, str>,
    operation: &'static str,
    total: u64,
    failures: u64,
) {
    let failure_count = failures.min(total);
    let success_count = total.saturating_sub(failure_count);
    for (result, value) in [("success", success_count), ("failure", failure_count)] {
        metrics.push(
            PrometheusMetric::from_descriptor(&BUCKET_REPL_PROXY_REQUESTS_TOTAL_MD, value as f64)
                .with_label(BUCKET_L, bucket_label.clone())
                .with_label(OPERATION_L, operation)
                .with_label(RESULT_L, result),
        );
    }
}

pub fn collect_bucket_replication_metrics(stats: &[BucketReplicationMetricsSnapshot]) -> Vec<PrometheusMetric> {
    if stats.is_empty() {
        return Vec::new();
    }

    let metric_count = stats
        .iter()
        .map(|stat| BASE_BUCKET_REPLICATION_METRICS_PER_BUCKET + stat.targets.len())
        .sum();
    let mut metrics = Vec::with_capacity(metric_count);
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
            PrometheusMetric::from_descriptor(&BUCKET_REPL_PROXIED_PUT_REQUESTS_TOTAL_MD, stat.proxied_put_requests_total as f64)
                .with_label(BUCKET_L, bucket_label.clone()),
        );
        metrics.push(
            PrometheusMetric::from_descriptor(
                &BUCKET_REPL_PROXIED_PUT_REQUESTS_FAILURES_MD,
                stat.proxied_put_requests_failures as f64,
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
        push_proxy_request_result_metrics(
            &mut metrics,
            bucket_label.clone(),
            "get",
            stat.proxied_get_requests_total,
            stat.proxied_get_requests_failures,
        );
        push_proxy_request_result_metrics(
            &mut metrics,
            bucket_label.clone(),
            "head",
            stat.proxied_head_requests_total,
            stat.proxied_head_requests_failures,
        );
        push_proxy_request_result_metrics(
            &mut metrics,
            bucket_label.clone(),
            "put",
            stat.proxied_put_requests_total,
            stat.proxied_put_requests_failures,
        );
        push_proxy_request_result_metrics(
            &mut metrics,
            bucket_label.clone(),
            "put_tagging",
            stat.proxied_put_tagging_requests_total,
            stat.proxied_put_tagging_requests_failures,
        );
        push_proxy_request_result_metrics(
            &mut metrics,
            bucket_label.clone(),
            "get_tagging",
            stat.proxied_get_tagging_requests_total,
            stat.proxied_get_tagging_requests_failures,
        );
        push_proxy_request_result_metrics(
            &mut metrics,
            bucket_label.clone(),
            "delete_tagging",
            stat.proxied_delete_tagging_requests_total,
            stat.proxied_delete_tagging_requests_failures,
        );
        metrics.push(
            PrometheusMetric::from_descriptor(&BUCKET_REPL_RESYNC_STARTED_TOTAL_MD, stat.resync_started_count as f64)
                .with_label(BUCKET_L, bucket_label.clone()),
        );
        metrics.push(
            PrometheusMetric::from_descriptor(&BUCKET_REPL_RESYNC_COMPLETED_TOTAL_MD, stat.resync_completed_count as f64)
                .with_label(BUCKET_L, bucket_label.clone()),
        );
        metrics.push(
            PrometheusMetric::from_descriptor(&BUCKET_REPL_RESYNC_FAILED_TOTAL_MD, stat.resync_failed_count as f64)
                .with_label(BUCKET_L, bucket_label.clone()),
        );
        metrics.push(
            PrometheusMetric::from_descriptor(&BUCKET_REPL_RESYNC_CANCELED_TOTAL_MD, stat.resync_canceled_count as f64)
                .with_label(BUCKET_L, bucket_label.clone()),
        );
        metrics.push(
            PrometheusMetric::from_descriptor(&BUCKET_REPL_RESYNC_DURATION_MS_TOTAL_MD, stat.resync_duration_ms as f64)
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

pub(crate) fn collect_bucket_replication_runtime_metrics(stats: &[BucketReplicationRuntimeStats]) -> Vec<PrometheusMetric> {
    if stats.is_empty() {
        return Vec::new();
    }

    let legacy_stats = stats.iter().map(|stat| stat.stats.clone()).collect::<Vec<_>>();
    let mut metrics = collect_bucket_replication_metrics(&legacy_stats);
    let flow_count = stats
        .iter()
        .map(|stat| stat.target_flows.len() * BUCKET_REPLICATION_RUNTIME_FLOW_METRICS_PER_TARGET)
        .sum();
    metrics.reserve(flow_count);

    for stat in stats {
        let bucket_label: Cow<'static, str> = Cow::Owned(stat.stats.bucket.clone());
        for target in &stat.target_flows {
            let target_label: Cow<'static, str> = Cow::Owned(target.target_arn.clone());
            metrics.push(
                PrometheusMetric::from_descriptor(&BUCKET_REPL_TARGET_SENT_BYTES_MD, target.sent_bytes as f64)
                    .with_label(BUCKET_L, bucket_label.clone())
                    .with_label(TARGET_ARN_L, target_label.clone()),
            );
            metrics.push(
                PrometheusMetric::from_descriptor(&BUCKET_REPL_TARGET_SENT_COUNT_MD, target.sent_count as f64)
                    .with_label(BUCKET_L, bucket_label.clone())
                    .with_label(TARGET_ARN_L, target_label.clone()),
            );
            metrics.push(
                PrometheusMetric::from_descriptor(&BUCKET_REPL_TARGET_TOTAL_FAILED_BYTES_MD, target.total_failed_bytes as f64)
                    .with_label(BUCKET_L, bucket_label.clone())
                    .with_label(TARGET_ARN_L, target_label.clone()),
            );
            metrics.push(
                PrometheusMetric::from_descriptor(&BUCKET_REPL_TARGET_TOTAL_FAILED_COUNT_MD, target.total_failed_count as f64)
                    .with_label(BUCKET_L, bucket_label.clone())
                    .with_label(TARGET_ARN_L, target_label.clone()),
            );
            metrics.push(
                PrometheusMetric::from_descriptor(
                    &BUCKET_REPL_TARGET_LAST_MIN_FAILED_BYTES_MD,
                    target.last_min_failed_bytes as f64,
                )
                .with_label(BUCKET_L, bucket_label.clone())
                .with_label(TARGET_ARN_L, target_label.clone()),
            );
            metrics.push(
                PrometheusMetric::from_descriptor(
                    &BUCKET_REPL_TARGET_LAST_MIN_FAILED_COUNT_MD,
                    target.last_min_failed_count as f64,
                )
                .with_label(BUCKET_L, bucket_label.clone())
                .with_label(TARGET_ARN_L, target_label.clone()),
            );
            metrics.push(
                PrometheusMetric::from_descriptor(
                    &BUCKET_REPL_TARGET_LAST_HOUR_FAILED_BYTES_MD,
                    target.last_hour_failed_bytes as f64,
                )
                .with_label(BUCKET_L, bucket_label.clone())
                .with_label(TARGET_ARN_L, target_label.clone()),
            );
            metrics.push(
                PrometheusMetric::from_descriptor(
                    &BUCKET_REPL_TARGET_LAST_HOUR_FAILED_COUNT_MD,
                    target.last_hour_failed_count as f64,
                )
                .with_label(BUCKET_L, bucket_label.clone())
                .with_label(TARGET_ARN_L, target_label.clone()),
            );
        }
    }

    metrics
}

pub(crate) fn collect_bucket_replication_backlog_metrics(stats: &[BucketReplicationBacklogStats]) -> Vec<PrometheusMetric> {
    if stats.is_empty() {
        return Vec::new();
    }

    let metric_count = stats
        .iter()
        .map(|stat| {
            BASE_BUCKET_REPLICATION_BACKLOG_METRICS_PER_BUCKET
                + stat.target_backlogs.len() * BUCKET_REPLICATION_BACKLOG_METRICS_PER_TARGET
        })
        .sum();
    let mut metrics = Vec::with_capacity(metric_count);
    for stat in stats {
        let bucket_label: Cow<'static, str> = Cow::Owned(stat.bucket.clone());

        metrics.push(
            PrometheusMetric::from_descriptor(&BUCKET_REPL_CURRENT_BACKLOG_COUNT_MD, stat.current_backlog_count as f64)
                .with_label(BUCKET_L, bucket_label.clone()),
        );
        metrics.push(
            PrometheusMetric::from_descriptor(&BUCKET_REPL_CURRENT_BACKLOG_BYTES_MD, stat.current_backlog_bytes as f64)
                .with_label(BUCKET_L, bucket_label.clone()),
        );
        metrics.push(
            PrometheusMetric::from_descriptor(
                &BUCKET_REPL_DURABLE_MRF_AVAILABLE_MD,
                if stat.durable_mrf_available { 1.0 } else { 0.0 },
            )
            .with_label(BUCKET_L, bucket_label.clone()),
        );
        metrics.push(
            PrometheusMetric::from_descriptor(&BUCKET_REPL_DURABLE_MRF_BACKLOG_COUNT_MD, stat.durable_mrf_backlog_count as f64)
                .with_label(BUCKET_L, bucket_label.clone()),
        );
        metrics.push(
            PrometheusMetric::from_descriptor(&BUCKET_REPL_DURABLE_MRF_BACKLOG_BYTES_MD, stat.durable_mrf_backlog_bytes as f64)
                .with_label(BUCKET_L, bucket_label),
        );
        let bucket_label: Cow<'static, str> = Cow::Owned(stat.bucket.clone());
        metrics.push(
            PrometheusMetric::from_descriptor(&BUCKET_REPL_MRF_PENDING_COUNT_MD, stat.mrf_pending_count as f64)
                .with_label(BUCKET_L, bucket_label.clone()),
        );
        metrics.push(
            PrometheusMetric::from_descriptor(&BUCKET_REPL_MRF_PENDING_BYTES_MD, stat.mrf_pending_bytes as f64)
                .with_label(BUCKET_L, bucket_label.clone()),
        );
        metrics.push(
            PrometheusMetric::from_descriptor(&BUCKET_REPL_MRF_DROPPED_COUNT_MD, stat.mrf_dropped_count as f64)
                .with_label(BUCKET_L, bucket_label.clone()),
        );
        metrics.push(
            PrometheusMetric::from_descriptor(&BUCKET_REPL_MRF_MISSED_COUNT_MD, stat.mrf_missed_count as f64)
                .with_label(BUCKET_L, bucket_label.clone()),
        );
        metrics.push(
            PrometheusMetric::from_descriptor(&BUCKET_REPL_MRF_FLUSH_FAILURES_MD, stat.mrf_flush_failures as f64)
                .with_label(BUCKET_L, bucket_label.clone()),
        );
        metrics.push(
            PrometheusMetric::from_descriptor(
                &BUCKET_REPL_MRF_LAST_FLUSH_DURATION_MILLIS_MD,
                stat.mrf_last_flush_duration_millis as f64,
            )
            .with_label(BUCKET_L, bucket_label),
        );
        for target in &stat.target_backlogs {
            let bucket_label: Cow<'static, str> = Cow::Owned(stat.bucket.clone());
            let target_label: Cow<'static, str> = Cow::Owned(target.target_arn.clone());
            metrics.push(
                PrometheusMetric::from_descriptor(
                    &BUCKET_REPL_CURRENT_TARGET_BACKLOG_COUNT_MD,
                    target.current_backlog_count as f64,
                )
                .with_label(BUCKET_L, bucket_label.clone())
                .with_label(TARGET_ARN_L, target_label.clone()),
            );
            metrics.push(
                PrometheusMetric::from_descriptor(
                    &BUCKET_REPL_CURRENT_TARGET_BACKLOG_BYTES_MD,
                    target.current_backlog_bytes as f64,
                )
                .with_label(BUCKET_L, bucket_label.clone())
                .with_label(TARGET_ARN_L, target_label.clone()),
            );
            metrics.push(
                PrometheusMetric::from_descriptor(
                    &BUCKET_REPL_DURABLE_MRF_TARGET_BACKLOG_COUNT_MD,
                    target.durable_mrf_backlog_count as f64,
                )
                .with_label(BUCKET_L, bucket_label.clone())
                .with_label(TARGET_ARN_L, target_label.clone()),
            );
            metrics.push(
                PrometheusMetric::from_descriptor(
                    &BUCKET_REPL_DURABLE_MRF_TARGET_BACKLOG_BYTES_MD,
                    target.durable_mrf_backlog_bytes as f64,
                )
                .with_label(BUCKET_L, bucket_label)
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
        let stats = vec![BucketReplicationRuntimeStats {
            stats: BucketReplicationMetricsSnapshot {
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
                proxied_put_requests_total: 6,
                proxied_put_requests_failures: 2,
                proxied_put_tagging_requests_total: 3,
                proxied_put_tagging_requests_failures: 1,
                proxied_get_tagging_requests_total: 2,
                proxied_get_tagging_requests_failures: 0,
                proxied_delete_tagging_requests_total: 1,
                proxied_delete_tagging_requests_failures: 1,
                resync_started_count: 2,
                resync_completed_count: 1,
                resync_failed_count: 1,
                resync_canceled_count: 0,
                resync_duration_ms: 1500,
                targets: vec![BucketReplicationTargetStats {
                    target_arn: "arn:rustfs:replication:us-east-1:1:target".to_string(),
                    bandwidth_limit_bytes_per_sec: 2048,
                    current_bandwidth_bytes_per_sec: 1024.0,
                    latency_ms: 15.0,
                }],
            },
            target_flows: vec![BucketReplicationTargetFlowStats {
                target_arn: "arn:rustfs:replication:us-east-1:1:target".to_string(),
                sent_bytes: 512,
                sent_count: 4,
                total_failed_bytes: 96,
                total_failed_count: 3,
                last_min_failed_bytes: 32,
                last_min_failed_count: 1,
                last_hour_failed_bytes: 64,
                last_hour_failed_count: 2,
            }],
        }];

        let metrics = collect_bucket_replication_runtime_metrics(&stats);
        assert_eq!(metrics.len(), 46);

        let sent_name = BUCKET_REPL_SENT_COUNT_MD.get_full_metric_name();
        assert!(metrics.iter().any(|metric| {
            metric.name == sent_name
                && metric.value == 8.0
                && metric.labels.iter().any(|(key, value)| *key == BUCKET_L && value == "b1")
        }));

        let put_total_name = BUCKET_REPL_PROXIED_PUT_REQUESTS_TOTAL_MD.get_full_metric_name();
        assert!(metrics.iter().any(|metric| {
            metric.name == put_total_name
                && metric.value == 6.0
                && metric.labels.iter().any(|(key, value)| *key == BUCKET_L && value == "b1")
        }));

        let put_failures_name = BUCKET_REPL_PROXIED_PUT_REQUESTS_FAILURES_MD.get_full_metric_name();
        assert!(metrics.iter().any(|metric| {
            metric.name == put_failures_name
                && metric.value == 2.0
                && metric.labels.iter().any(|(key, value)| *key == BUCKET_L && value == "b1")
        }));

        let proxy_requests_name = BUCKET_REPL_PROXY_REQUESTS_TOTAL_MD.get_full_metric_name();
        assert!(metrics.iter().any(|metric| {
            metric.name == proxy_requests_name
                && metric.value == 4.0
                && metric.labels.iter().any(|(key, value)| *key == BUCKET_L && value == "b1")
                && metric.labels.iter().any(|(key, value)| *key == OPERATION_L && value == "put")
                && metric
                    .labels
                    .iter()
                    .any(|(key, value)| *key == RESULT_L && value == "success")
        }));
        assert!(metrics.iter().any(|metric| {
            metric.name == proxy_requests_name
                && metric.value == 2.0
                && metric.labels.iter().any(|(key, value)| *key == BUCKET_L && value == "b1")
                && metric.labels.iter().any(|(key, value)| *key == OPERATION_L && value == "put")
                && metric
                    .labels
                    .iter()
                    .any(|(key, value)| *key == RESULT_L && value == "failure")
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

        let target_sent_name = BUCKET_REPL_TARGET_SENT_COUNT_MD.get_full_metric_name();
        assert!(metrics.iter().any(|metric| {
            metric.name == target_sent_name
                && metric.value == 4.0
                && metric
                    .labels
                    .iter()
                    .any(|(key, value)| *key == TARGET_ARN_L && value == "arn:rustfs:replication:us-east-1:1:target")
        }));

        let target_last_min_failed_name = BUCKET_REPL_TARGET_LAST_MIN_FAILED_BYTES_MD.get_full_metric_name();
        assert!(metrics.iter().any(|metric| {
            metric.name == target_last_min_failed_name
                && metric.value == 32.0
                && metric.labels.iter().any(|(key, value)| *key == BUCKET_L && value == "b1")
                && metric
                    .labels
                    .iter()
                    .any(|(key, value)| *key == TARGET_ARN_L && value == "arn:rustfs:replication:us-east-1:1:target")
        }));

        let delete_tagging_total_name = BUCKET_REPL_PROXIED_DELETE_TAGGING_REQUESTS_TOTAL_MD.get_full_metric_name();
        assert!(metrics.iter().any(|metric| {
            metric.name == delete_tagging_total_name
                && metric.value == 1.0
                && metric.labels.iter().any(|(key, value)| *key == BUCKET_L && value == "b1")
        }));

        let delete_tagging_failures_name = BUCKET_REPL_PROXIED_DELETE_TAGGING_REQUESTS_FAILURES_MD.get_full_metric_name();
        assert!(metrics.iter().any(|metric| {
            metric.name == delete_tagging_failures_name
                && metric.value == 1.0
                && metric.labels.iter().any(|(key, value)| *key == BUCKET_L && value == "b1")
        }));

        let resync_started_name = BUCKET_REPL_RESYNC_STARTED_TOTAL_MD.get_full_metric_name();
        assert!(metrics.iter().any(|metric| {
            metric.name == resync_started_name
                && metric.value == 2.0
                && metric.labels.iter().any(|(key, value)| *key == BUCKET_L && value == "b1")
        }));

        let resync_duration_name = BUCKET_REPL_RESYNC_DURATION_MS_TOTAL_MD.get_full_metric_name();
        assert!(metrics.iter().any(|metric| {
            metric.name == resync_duration_name
                && metric.value == 1500.0
                && metric.labels.iter().any(|(key, value)| *key == BUCKET_L && value == "b1")
        }));
    }

    #[test]
    fn test_collect_bucket_replication_backlog_metrics() {
        let stats = vec![BucketReplicationBacklogStats {
            bucket: "b1".to_string(),
            current_backlog_count: 3,
            current_backlog_bytes: 4096,
            durable_mrf_available: true,
            durable_mrf_backlog_count: 2,
            durable_mrf_backlog_bytes: 2048,
            mrf_pending_count: 1,
            mrf_pending_bytes: 512,
            mrf_dropped_count: 3,
            mrf_missed_count: 4,
            mrf_flush_failures: 5,
            mrf_last_flush_duration_millis: 6,
            target_backlogs: vec![BucketReplicationTargetBacklogStats {
                target_arn: "arn:rustfs:replication:target-a".to_string(),
                current_backlog_count: 3,
                current_backlog_bytes: 4096,
                durable_mrf_backlog_count: 2,
                durable_mrf_backlog_bytes: 2048,
            }],
        }];

        let metrics = collect_bucket_replication_backlog_metrics(&stats);
        assert_eq!(metrics.len(), 15);

        let backlog_count_name = BUCKET_REPL_CURRENT_BACKLOG_COUNT_MD.get_full_metric_name();
        assert!(metrics.iter().any(|metric| {
            metric.name == backlog_count_name
                && metric.value == 3.0
                && metric.labels.iter().any(|(key, value)| *key == BUCKET_L && value == "b1")
        }));

        let backlog_bytes_name = BUCKET_REPL_CURRENT_BACKLOG_BYTES_MD.get_full_metric_name();
        assert!(metrics.iter().any(|metric| {
            metric.name == backlog_bytes_name
                && metric.value == 4096.0
                && metric.labels.iter().any(|(key, value)| *key == BUCKET_L && value == "b1")
        }));

        let durable_available_name = BUCKET_REPL_DURABLE_MRF_AVAILABLE_MD.get_full_metric_name();
        assert!(metrics.iter().any(|metric| {
            metric.name == durable_available_name
                && metric.value == 1.0
                && metric.labels.iter().any(|(key, value)| *key == BUCKET_L && value == "b1")
        }));

        let durable_count_name = BUCKET_REPL_DURABLE_MRF_BACKLOG_COUNT_MD.get_full_metric_name();
        assert!(metrics.iter().any(|metric| {
            metric.name == durable_count_name
                && metric.value == 2.0
                && metric.labels.iter().any(|(key, value)| *key == BUCKET_L && value == "b1")
        }));

        let durable_bytes_name = BUCKET_REPL_DURABLE_MRF_BACKLOG_BYTES_MD.get_full_metric_name();
        assert!(metrics.iter().any(|metric| {
            metric.name == durable_bytes_name
                && metric.value == 2048.0
                && metric.labels.iter().any(|(key, value)| *key == BUCKET_L && value == "b1")
        }));

        let target_count_name = BUCKET_REPL_CURRENT_TARGET_BACKLOG_COUNT_MD.get_full_metric_name();
        assert!(metrics.iter().any(|metric| {
            metric.name == target_count_name
                && metric.value == 3.0
                && metric.labels.iter().any(|(key, value)| *key == BUCKET_L && value == "b1")
                && metric
                    .labels
                    .iter()
                    .any(|(key, value)| *key == TARGET_ARN_L && value == "arn:rustfs:replication:target-a")
        }));

        let target_bytes_name = BUCKET_REPL_CURRENT_TARGET_BACKLOG_BYTES_MD.get_full_metric_name();
        assert!(metrics.iter().any(|metric| {
            metric.name == target_bytes_name
                && metric.value == 4096.0
                && metric.labels.iter().any(|(key, value)| *key == BUCKET_L && value == "b1")
                && metric
                    .labels
                    .iter()
                    .any(|(key, value)| *key == TARGET_ARN_L && value == "arn:rustfs:replication:target-a")
        }));

        let durable_target_count_name = BUCKET_REPL_DURABLE_MRF_TARGET_BACKLOG_COUNT_MD.get_full_metric_name();
        assert!(metrics.iter().any(|metric| {
            metric.name == durable_target_count_name
                && metric.value == 2.0
                && metric.labels.iter().any(|(key, value)| *key == BUCKET_L && value == "b1")
                && metric
                    .labels
                    .iter()
                    .any(|(key, value)| *key == TARGET_ARN_L && value == "arn:rustfs:replication:target-a")
        }));

        let durable_target_bytes_name = BUCKET_REPL_DURABLE_MRF_TARGET_BACKLOG_BYTES_MD.get_full_metric_name();
        assert!(metrics.iter().any(|metric| {
            metric.name == durable_target_bytes_name
                && metric.value == 2048.0
                && metric.labels.iter().any(|(key, value)| *key == BUCKET_L && value == "b1")
                && metric
                    .labels
                    .iter()
                    .any(|(key, value)| *key == TARGET_ARN_L && value == "arn:rustfs:replication:target-a")
        }));

        let pending_count_name = BUCKET_REPL_MRF_PENDING_COUNT_MD.get_full_metric_name();
        assert!(metrics.iter().any(|metric| {
            metric.name == pending_count_name
                && metric.value == 1.0
                && metric.labels.iter().any(|(key, value)| *key == BUCKET_L && value == "b1")
        }));

        let pending_bytes_name = BUCKET_REPL_MRF_PENDING_BYTES_MD.get_full_metric_name();
        assert!(metrics.iter().any(|metric| {
            metric.name == pending_bytes_name
                && metric.value == 512.0
                && metric.labels.iter().any(|(key, value)| *key == BUCKET_L && value == "b1")
        }));

        let dropped_count_name = BUCKET_REPL_MRF_DROPPED_COUNT_MD.get_full_metric_name();
        assert!(metrics.iter().any(|metric| {
            metric.name == dropped_count_name
                && metric.value == 3.0
                && metric.labels.iter().any(|(key, value)| *key == BUCKET_L && value == "b1")
        }));

        let missed_count_name = BUCKET_REPL_MRF_MISSED_COUNT_MD.get_full_metric_name();
        assert!(metrics.iter().any(|metric| {
            metric.name == missed_count_name
                && metric.value == 4.0
                && metric.labels.iter().any(|(key, value)| *key == BUCKET_L && value == "b1")
        }));

        let flush_failures_name = BUCKET_REPL_MRF_FLUSH_FAILURES_MD.get_full_metric_name();
        assert!(metrics.iter().any(|metric| {
            metric.name == flush_failures_name
                && metric.value == 5.0
                && metric.labels.iter().any(|(key, value)| *key == BUCKET_L && value == "b1")
        }));

        let flush_duration_name = BUCKET_REPL_MRF_LAST_FLUSH_DURATION_MILLIS_MD.get_full_metric_name();
        assert!(metrics.iter().any(|metric| {
            metric.name == flush_duration_name
                && metric.value == 6.0
                && metric.labels.iter().any(|(key, value)| *key == BUCKET_L && value == "b1")
        }));
    }

    #[test]
    fn test_collect_bucket_replication_metrics_empty() {
        let stats: Vec<BucketReplicationMetricsSnapshot> = Vec::new();
        let metrics = collect_bucket_replication_metrics(&stats);
        assert!(metrics.is_empty());
    }

    #[test]
    fn backlog_metrics_are_bucket_scoped_without_target_labels() {
        let stats = vec![BucketReplicationBacklogStats {
            bucket: "scope-bucket".to_string(),
            current_backlog_count: 5,
            current_backlog_bytes: 8192,
            durable_mrf_available: true,
            durable_mrf_backlog_count: 2,
            durable_mrf_backlog_bytes: 4096,
            mrf_pending_count: 1,
            mrf_pending_bytes: 2,
            mrf_dropped_count: 3,
            mrf_missed_count: 4,
            mrf_flush_failures: 5,
            mrf_last_flush_duration_millis: 6,
            target_backlogs: Vec::new(),
        }];

        let metrics = collect_bucket_replication_backlog_metrics(&stats);
        let backlog_names = [
            BUCKET_REPL_CURRENT_BACKLOG_COUNT_MD.get_full_metric_name(),
            BUCKET_REPL_CURRENT_BACKLOG_BYTES_MD.get_full_metric_name(),
            BUCKET_REPL_DURABLE_MRF_AVAILABLE_MD.get_full_metric_name(),
            BUCKET_REPL_DURABLE_MRF_BACKLOG_COUNT_MD.get_full_metric_name(),
            BUCKET_REPL_DURABLE_MRF_BACKLOG_BYTES_MD.get_full_metric_name(),
            BUCKET_REPL_MRF_PENDING_COUNT_MD.get_full_metric_name(),
            BUCKET_REPL_MRF_PENDING_BYTES_MD.get_full_metric_name(),
            BUCKET_REPL_MRF_DROPPED_COUNT_MD.get_full_metric_name(),
            BUCKET_REPL_MRF_MISSED_COUNT_MD.get_full_metric_name(),
            BUCKET_REPL_MRF_FLUSH_FAILURES_MD.get_full_metric_name(),
            BUCKET_REPL_MRF_LAST_FLUSH_DURATION_MILLIS_MD.get_full_metric_name(),
        ];

        for name in backlog_names {
            let metric = metrics
                .iter()
                .find(|metric| metric.name == name)
                .expect("backlog metric should be emitted");
            assert!(
                metric
                    .labels
                    .iter()
                    .any(|(key, value)| *key == BUCKET_L && value == "scope-bucket")
            );
            assert!(!metric.labels.iter().any(|(key, _)| *key == TARGET_ARN_L));
        }
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
