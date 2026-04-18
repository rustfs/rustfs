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

//! Cluster IAM metrics collector.
//!
//! Collects IAM (Identity and Access Management) metrics including
//! plugin authentication service stats and sync statistics.

use crate::metrics::report::PrometheusMetric;
use crate::metrics::schema::cluster_iam::*;

/// IAM statistics.
#[derive(Debug, Clone, Default)]
pub struct IamStats {
    /// Time in seconds since last failed authn service request
    pub plugin_authn_service_last_fail_seconds: u64,
    /// Time in seconds since last successful authn service request
    pub plugin_authn_service_last_succ_seconds: u64,
    /// Average RTT of successful requests in the last minute (ms)
    pub plugin_authn_service_succ_avg_rtt_ms_minute: u64,
    /// Maximum RTT of successful requests in the last minute (ms)
    pub plugin_authn_service_succ_max_rtt_ms_minute: u64,
    /// Total requests count in the last full minute
    pub plugin_authn_service_total_requests_minute: u64,
    /// Time in milliseconds since last successful IAM data sync
    pub since_last_sync_millis: u64,
    /// Number of failed IAM data syncs since server start
    pub sync_failures: u64,
    /// Number of successful IAM data syncs since server start
    pub sync_successes: u64,
}

/// Collects IAM metrics from the given stats.
///
/// Returns a vector of Prometheus metrics for IAM statistics.
pub fn collect_iam_metrics(stats: &IamStats) -> Vec<PrometheusMetric> {
    vec![
        PrometheusMetric::from_descriptor(
            &PLUGIN_AUTHN_SERVICE_LAST_FAIL_SECONDS_MD,
            stats.plugin_authn_service_last_fail_seconds as f64,
        ),
        PrometheusMetric::from_descriptor(
            &PLUGIN_AUTHN_SERVICE_LAST_SUCC_SECONDS_MD,
            stats.plugin_authn_service_last_succ_seconds as f64,
        ),
        PrometheusMetric::from_descriptor(
            &PLUGIN_AUTHN_SERVICE_SUCC_AVG_RTT_MS_MINUTE_MD,
            stats.plugin_authn_service_succ_avg_rtt_ms_minute as f64,
        ),
        PrometheusMetric::from_descriptor(
            &PLUGIN_AUTHN_SERVICE_SUCC_MAX_RTT_MS_MINUTE_MD,
            stats.plugin_authn_service_succ_max_rtt_ms_minute as f64,
        ),
        PrometheusMetric::from_descriptor(
            &PLUGIN_AUTHN_SERVICE_TOTAL_REQUESTS_MINUTE_MD,
            stats.plugin_authn_service_total_requests_minute as f64,
        ),
        PrometheusMetric::from_descriptor(&SINCE_LAST_SYNC_MILLIS_MD, stats.since_last_sync_millis as f64),
        PrometheusMetric::from_descriptor(&SYNC_FAILURES_MD, stats.sync_failures as f64),
        PrometheusMetric::from_descriptor(&SYNC_SUCCESSES_MD, stats.sync_successes as f64),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::report::report_metrics;

    #[test]
    fn test_collect_iam_metrics() {
        let stats = IamStats {
            plugin_authn_service_last_fail_seconds: 3600,
            plugin_authn_service_last_succ_seconds: 10,
            plugin_authn_service_succ_avg_rtt_ms_minute: 50,
            plugin_authn_service_succ_max_rtt_ms_minute: 200,
            plugin_authn_service_total_requests_minute: 1000,
            since_last_sync_millis: 5000,
            sync_failures: 5,
            sync_successes: 1000,
        };

        let metrics = collect_iam_metrics(&stats);
        report_metrics(&metrics);

        assert_eq!(metrics.len(), 8);

        let sync_successes_name = SYNC_SUCCESSES_MD.get_full_metric_name();
        let sync_successes = metrics.iter().find(|m| m.name == sync_successes_name);
        assert!(sync_successes.is_some());
        assert_eq!(sync_successes.map(|m| m.value), Some(1000.0));
    }

    #[test]
    fn test_collect_iam_metrics_default() {
        let stats = IamStats::default();
        let metrics = collect_iam_metrics(&stats);

        assert_eq!(metrics.len(), 8);
        for metric in &metrics {
            assert_eq!(metric.value, 0.0);
            assert!(metric.labels.is_empty());
        }
    }
}
