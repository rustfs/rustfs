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

//! API request metrics collector.
//!
//! Collects API request metrics including request counts, errors,
//! latency, and traffic statistics.

use crate::metrics::report::PrometheusMetric;
use crate::metrics::schema::request::*;

/// API request statistics for a specific API endpoint.
#[derive(Debug, Clone, Default)]
pub struct ApiRequestStats {
    /// API name (e.g., "GetObject", "PutObject")
    pub name: String,
    /// Request type (e.g., "s3", "admin")
    pub req_type: String,
    /// Number of requests currently in flight
    pub in_flight: u64,
    /// Total number of requests
    pub total: u64,
    /// Total number of errors (4xx + 5xx)
    pub errors_total: u64,
    /// Total number of 5xx errors
    pub errors_5xx: u64,
    /// Total number of 4xx errors
    pub errors_4xx: u64,
    /// Total number of canceled requests
    pub canceled: u64,
    /// TTFB distribution by bucket (le label)
    pub ttfb_distribution: Vec<(String, f64)>,
    /// Bytes sent
    pub sent_bytes: u64,
    /// Bytes received
    pub recv_bytes: u64,
}

/// Collects API request metrics from the given stats.
///
/// Returns a vector of Prometheus metrics for API request statistics.
pub fn collect_request_metrics(stats: &[ApiRequestStats]) -> Vec<PrometheusMetric> {
    let mut metrics = Vec::new();

    for stat in stats {
        // In-flight requests
        metrics.push(
            PrometheusMetric::from_descriptor(&API_REQUESTS_IN_FLIGHT_TOTAL_MD, stat.in_flight as f64)
                .with_label_owned(NAME_LABEL, stat.name.clone())
                .with_label_owned(TYPE_LABEL, stat.req_type.clone()),
        );

        // Total requests
        metrics.push(
            PrometheusMetric::from_descriptor(&API_REQUESTS_TOTAL_MD, stat.total as f64)
                .with_label_owned(NAME_LABEL, stat.name.clone())
                .with_label_owned(TYPE_LABEL, stat.req_type.clone()),
        );

        // Total errors
        metrics.push(
            PrometheusMetric::from_descriptor(&API_REQUESTS_ERRORS_TOTAL_MD, stat.errors_total as f64)
                .with_label_owned(NAME_LABEL, stat.name.clone())
                .with_label_owned(TYPE_LABEL, stat.req_type.clone()),
        );

        // 5xx errors
        metrics.push(
            PrometheusMetric::from_descriptor(&API_REQUESTS_5XX_ERRORS_TOTAL_MD, stat.errors_5xx as f64)
                .with_label_owned(NAME_LABEL, stat.name.clone())
                .with_label_owned(TYPE_LABEL, stat.req_type.clone()),
        );

        // 4xx errors
        metrics.push(
            PrometheusMetric::from_descriptor(&API_REQUESTS_4XX_ERRORS_TOTAL_MD, stat.errors_4xx as f64)
                .with_label_owned(NAME_LABEL, stat.name.clone())
                .with_label_owned(TYPE_LABEL, stat.req_type.clone()),
        );

        // Canceled requests
        metrics.push(
            PrometheusMetric::from_descriptor(&API_REQUESTS_CANCELED_TOTAL_MD, stat.canceled as f64)
                .with_label_owned(NAME_LABEL, stat.name.clone())
                .with_label_owned(TYPE_LABEL, stat.req_type.clone()),
        );

        // TTFB distribution (histogram buckets)
        for (le, value) in &stat.ttfb_distribution {
            metrics.push(
                PrometheusMetric::from_descriptor(&API_REQUESTS_TTFB_SECONDS_DISTRIBUTION_MD, *value)
                    .with_label_owned(NAME_LABEL, stat.name.clone())
                    .with_label_owned(TYPE_LABEL, stat.req_type.clone())
                    .with_label_owned(LE_LABEL, le.clone()),
            );
        }

        // Traffic metrics
        metrics.push(
            PrometheusMetric::from_descriptor(&API_TRAFFIC_SENT_BYTES_MD, stat.sent_bytes as f64)
                .with_label_owned(TYPE_LABEL, stat.req_type.clone()),
        );
        metrics.push(
            PrometheusMetric::from_descriptor(&API_TRAFFIC_RECV_BYTES_MD, stat.recv_bytes as f64)
                .with_label_owned(TYPE_LABEL, stat.req_type.clone()),
        );
    }

    metrics
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::report::report_metrics;

    #[test]
    fn test_collect_request_metrics() {
        let stats = vec![ApiRequestStats {
            name: "GetObject".to_string(),
            req_type: "s3".to_string(),
            in_flight: 10,
            total: 10000,
            errors_total: 50,
            errors_5xx: 10,
            errors_4xx: 40,
            canceled: 5,
            ttfb_distribution: vec![
                ("0.1".to_string(), 5000.0),
                ("0.5".to_string(), 8000.0),
                ("1.0".to_string(), 9500.0),
                ("+Inf".to_string(), 10000.0),
            ],
            sent_bytes: 1024 * 1024 * 500, // 500 MB
            recv_bytes: 1024 * 1024 * 100, // 100 MB
        }];

        let metrics = collect_request_metrics(&stats);
        report_metrics(&metrics);

        // 6 base metrics + 4 TTFB buckets + 2 traffic metrics = 12
        assert_eq!(metrics.len(), 12);

        let total_name = API_REQUESTS_TOTAL_MD.get_full_metric_name();
        let total = metrics.iter().find(|m| m.name == total_name);
        assert!(total.is_some());
        assert_eq!(total.map(|m| m.value), Some(10000.0));

        let in_flight_name = API_REQUESTS_IN_FLIGHT_TOTAL_MD.get_full_metric_name();
        let in_flight = metrics.iter().find(|m| m.name == in_flight_name);
        assert!(in_flight.is_some());
        assert_eq!(in_flight.map(|m| m.value), Some(10.0));
    }

    #[test]
    fn test_collect_request_metrics_empty() {
        let stats: Vec<ApiRequestStats> = vec![];
        let metrics = collect_request_metrics(&stats);
        assert!(metrics.is_empty());
    }
}
