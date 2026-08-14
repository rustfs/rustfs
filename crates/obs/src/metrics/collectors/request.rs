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

//! API request metrics collector.
//!
//! Collects API request metrics including request counts, errors,
//! latency, and traffic statistics.

use crate::metrics::report::PrometheusMetric;
use crate::metrics::schema::request::*;
use std::collections::HashMap;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(crate) struct ApiRequestMetricSupport {
    pub(crate) lifecycle: bool,
    pub(crate) traffic: bool,
    pub(crate) ttfb: bool,
}

impl ApiRequestMetricSupport {
    pub(crate) const ALL: Self = Self {
        lifecycle: true,
        traffic: true,
        ttfb: true,
    };

    pub(crate) const TOTALS_ONLY: Self = Self {
        lifecycle: false,
        traffic: false,
        ttfb: false,
    };
}

/// API request statistics for a specific API endpoint.
#[derive(Debug, Clone)]
pub struct ApiRequestStats {
    /// Server identifier
    pub server: String,
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
    pub(crate) supported_metrics: ApiRequestMetricSupport,
}

impl Default for ApiRequestStats {
    fn default() -> Self {
        Self {
            server: String::new(),
            name: String::new(),
            req_type: String::new(),
            in_flight: 0,
            total: 0,
            errors_total: 0,
            errors_5xx: 0,
            errors_4xx: 0,
            canceled: 0,
            ttfb_distribution: Vec::new(),
            sent_bytes: 0,
            recv_bytes: 0,
            supported_metrics: ApiRequestMetricSupport::ALL,
        }
    }
}

/// Collects API request metrics from the given stats.
///
/// Returns a vector of Prometheus metrics for API request statistics.
pub fn collect_request_metrics(stats: &[ApiRequestStats]) -> Vec<PrometheusMetric> {
    let mut metrics = Vec::new();
    let mut traffic_by_type: HashMap<&str, (u64, u64)> = HashMap::with_capacity(stats.len());
    let mut traffic_by_server_type: HashMap<(&str, &str), (u64, u64)> = HashMap::with_capacity(stats.len());

    for stat in stats {
        if stat.supported_metrics.traffic {
            let entry = traffic_by_type.entry(stat.req_type.as_str()).or_default();
            entry.0 = entry.0.saturating_add(stat.sent_bytes);
            entry.1 = entry.1.saturating_add(stat.recv_bytes);
            if !stat.server.is_empty() {
                let entry = traffic_by_server_type
                    .entry((stat.server.as_str(), stat.req_type.as_str()))
                    .or_default();
                entry.0 = entry.0.saturating_add(stat.sent_bytes);
                entry.1 = entry.1.saturating_add(stat.recv_bytes);
            }
        }

        metrics.push(
            PrometheusMetric::from_descriptor(&API_REQUESTS_TOTAL_MD, stat.total as f64)
                .with_label_owned(NAME_LABEL, stat.name.clone())
                .with_label_owned(TYPE_LABEL, stat.req_type.clone()),
        );

        if stat.supported_metrics.lifecycle {
            metrics.push(
                PrometheusMetric::from_descriptor(&API_REQUESTS_IN_FLIGHT_TOTAL_MD, stat.in_flight as f64)
                    .with_label_owned(NAME_LABEL, stat.name.clone())
                    .with_label_owned(TYPE_LABEL, stat.req_type.clone()),
            );
            metrics.push(
                PrometheusMetric::from_descriptor(&API_REQUESTS_ERRORS_TOTAL_MD, stat.errors_total as f64)
                    .with_label_owned(NAME_LABEL, stat.name.clone())
                    .with_label_owned(TYPE_LABEL, stat.req_type.clone()),
            );
            metrics.push(
                PrometheusMetric::from_descriptor(&API_REQUESTS_5XX_ERRORS_TOTAL_MD, stat.errors_5xx as f64)
                    .with_label_owned(NAME_LABEL, stat.name.clone())
                    .with_label_owned(TYPE_LABEL, stat.req_type.clone()),
            );
            metrics.push(
                PrometheusMetric::from_descriptor(&API_REQUESTS_4XX_ERRORS_TOTAL_MD, stat.errors_4xx as f64)
                    .with_label_owned(NAME_LABEL, stat.name.clone())
                    .with_label_owned(TYPE_LABEL, stat.req_type.clone()),
            );
            metrics.push(
                PrometheusMetric::from_descriptor(&API_REQUESTS_CANCELED_TOTAL_MD, stat.canceled as f64)
                    .with_label_owned(NAME_LABEL, stat.name.clone())
                    .with_label_owned(TYPE_LABEL, stat.req_type.clone()),
            );
        }

        if stat.supported_metrics.ttfb {
            for (le, value) in &stat.ttfb_distribution {
                metrics.push(
                    PrometheusMetric::from_descriptor(&API_REQUESTS_TTFB_SECONDS_DISTRIBUTION_MD, *value)
                        .with_label_owned(NAME_LABEL, stat.name.clone())
                        .with_label_owned(TYPE_LABEL, stat.req_type.clone())
                        .with_label_owned(LE_LABEL, le.clone()),
                );
            }
        }

        if !stat.server.is_empty() {
            metrics.push(
                PrometheusMetric::from_descriptor(&API_REQUESTS_TOTAL_BY_SERVER_MD, stat.total as f64)
                    .with_label_owned(SERVER_LABEL, stat.server.clone())
                    .with_label_owned(NAME_LABEL, stat.name.clone())
                    .with_label_owned(TYPE_LABEL, stat.req_type.clone()),
            );

            if stat.supported_metrics.lifecycle {
                metrics.push(
                    PrometheusMetric::from_descriptor(&API_REQUESTS_IN_FLIGHT_TOTAL_BY_SERVER_MD, stat.in_flight as f64)
                        .with_label_owned(SERVER_LABEL, stat.server.clone())
                        .with_label_owned(NAME_LABEL, stat.name.clone())
                        .with_label_owned(TYPE_LABEL, stat.req_type.clone()),
                );
                metrics.push(
                    PrometheusMetric::from_descriptor(&API_REQUESTS_ERRORS_TOTAL_BY_SERVER_MD, stat.errors_total as f64)
                        .with_label_owned(SERVER_LABEL, stat.server.clone())
                        .with_label_owned(NAME_LABEL, stat.name.clone())
                        .with_label_owned(TYPE_LABEL, stat.req_type.clone()),
                );
                metrics.push(
                    PrometheusMetric::from_descriptor(&API_REQUESTS_5XX_ERRORS_TOTAL_BY_SERVER_MD, stat.errors_5xx as f64)
                        .with_label_owned(SERVER_LABEL, stat.server.clone())
                        .with_label_owned(NAME_LABEL, stat.name.clone())
                        .with_label_owned(TYPE_LABEL, stat.req_type.clone()),
                );
                metrics.push(
                    PrometheusMetric::from_descriptor(&API_REQUESTS_4XX_ERRORS_TOTAL_BY_SERVER_MD, stat.errors_4xx as f64)
                        .with_label_owned(SERVER_LABEL, stat.server.clone())
                        .with_label_owned(NAME_LABEL, stat.name.clone())
                        .with_label_owned(TYPE_LABEL, stat.req_type.clone()),
                );
                metrics.push(
                    PrometheusMetric::from_descriptor(&API_REQUESTS_CANCELED_TOTAL_BY_SERVER_MD, stat.canceled as f64)
                        .with_label_owned(SERVER_LABEL, stat.server.clone())
                        .with_label_owned(NAME_LABEL, stat.name.clone())
                        .with_label_owned(TYPE_LABEL, stat.req_type.clone()),
                );
            }

            if stat.supported_metrics.ttfb {
                for (le, value) in &stat.ttfb_distribution {
                    metrics.push(
                        PrometheusMetric::from_descriptor(&API_REQUESTS_TTFB_SECONDS_DISTRIBUTION_BY_SERVER_MD, *value)
                            .with_label_owned(SERVER_LABEL, stat.server.clone())
                            .with_label_owned(NAME_LABEL, stat.name.clone())
                            .with_label_owned(TYPE_LABEL, stat.req_type.clone())
                            .with_label_owned(LE_LABEL, le.clone()),
                    );
                }
            }
        }
    }

    for (req_type, (sent_bytes, recv_bytes)) in traffic_by_type {
        metrics.push(
            PrometheusMetric::from_descriptor(&API_TRAFFIC_SENT_BYTES_MD, sent_bytes as f64)
                .with_label_owned(TYPE_LABEL, req_type.to_string()),
        );
        metrics.push(
            PrometheusMetric::from_descriptor(&API_TRAFFIC_RECV_BYTES_MD, recv_bytes as f64)
                .with_label_owned(TYPE_LABEL, req_type.to_string()),
        );
    }

    for ((server, req_type), (sent_bytes, recv_bytes)) in traffic_by_server_type {
        metrics.push(
            PrometheusMetric::from_descriptor(&API_TRAFFIC_SENT_BYTES_BY_SERVER_MD, sent_bytes as f64)
                .with_label_owned(SERVER_LABEL, server.to_string())
                .with_label_owned(TYPE_LABEL, req_type.to_string()),
        );
        metrics.push(
            PrometheusMetric::from_descriptor(&API_TRAFFIC_RECV_BYTES_BY_SERVER_MD, recv_bytes as f64)
                .with_label_owned(SERVER_LABEL, server.to_string())
                .with_label_owned(TYPE_LABEL, req_type.to_string()),
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
            server: "node1:9000".to_string(),
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
            supported_metrics: ApiRequestMetricSupport::ALL,
        }];

        let metrics = collect_request_metrics(&stats);
        report_metrics(&metrics);

        assert_eq!(metrics.len(), 24);

        let total_name = API_REQUESTS_TOTAL_MD.get_full_metric_name();
        let total = metrics.iter().find(|m| m.name == total_name);
        assert!(total.is_some());
        assert_eq!(total.map(|m| m.value), Some(10000.0));

        let in_flight_name = API_REQUESTS_IN_FLIGHT_TOTAL_MD.get_full_metric_name();
        let in_flight = metrics.iter().find(|m| m.name == in_flight_name);
        assert!(in_flight.is_some());
        assert_eq!(in_flight.map(|m| m.value), Some(10.0));

        let by_server_total_name = API_REQUESTS_TOTAL_BY_SERVER_MD.get_full_metric_name();
        let by_server_total = metrics.iter().find(|m| {
            m.name == by_server_total_name
                && m.labels
                    .iter()
                    .any(|(key, value)| *key == SERVER_LABEL && value == "node1:9000")
                && m.labels.iter().any(|(key, value)| *key == NAME_LABEL && value == "GetObject")
                && m.labels.iter().any(|(key, value)| *key == TYPE_LABEL && value == "s3")
        });
        assert_eq!(by_server_total.map(|m| m.value), Some(10000.0));

        let by_server_sent_name = API_TRAFFIC_SENT_BYTES_BY_SERVER_MD.get_full_metric_name();
        let by_server_sent = metrics.iter().find(|m| {
            m.name == by_server_sent_name
                && m.labels
                    .iter()
                    .any(|(key, value)| *key == SERVER_LABEL && value == "node1:9000")
                && m.labels.iter().any(|(key, value)| *key == TYPE_LABEL && value == "s3")
        });
        assert_eq!(by_server_sent.map(|m| m.value), Some((1024 * 1024 * 500) as f64));
    }

    #[test]
    fn test_collect_request_metrics_empty() {
        let stats: Vec<ApiRequestStats> = vec![];
        let metrics = collect_request_metrics(&stats);
        assert!(metrics.is_empty());
    }

    #[test]
    fn test_collect_request_metrics_totals_only_skips_unsupported_dimensions() {
        let stats = vec![ApiRequestStats {
            server: "node1:9000".to_string(),
            name: "GetObject".to_string(),
            req_type: "s3".to_string(),
            in_flight: 10,
            total: 100,
            errors_total: 5,
            errors_5xx: 2,
            errors_4xx: 3,
            canceled: 1,
            ttfb_distribution: vec![("+Inf".to_string(), 100.0)],
            sent_bytes: 2048,
            recv_bytes: 1024,
            supported_metrics: ApiRequestMetricSupport::TOTALS_ONLY,
        }];

        let metrics = collect_request_metrics(&stats);

        assert!(
            metrics
                .iter()
                .any(|metric| metric.name == API_REQUESTS_TOTAL_MD.get_full_metric_name())
        );
        assert!(
            metrics
                .iter()
                .any(|metric| metric.name == API_REQUESTS_TOTAL_BY_SERVER_MD.get_full_metric_name())
        );
        assert!(
            !metrics
                .iter()
                .any(|metric| metric.name == API_REQUESTS_IN_FLIGHT_TOTAL_MD.get_full_metric_name())
        );
        assert!(
            !metrics
                .iter()
                .any(|metric| metric.name == API_REQUESTS_ERRORS_TOTAL_MD.get_full_metric_name())
        );
        assert!(
            !metrics
                .iter()
                .any(|metric| metric.name == API_TRAFFIC_SENT_BYTES_MD.get_full_metric_name())
        );
        assert!(
            !metrics
                .iter()
                .any(|metric| metric.name == API_REQUESTS_TTFB_SECONDS_DISTRIBUTION_MD.get_full_metric_name())
        );
    }

    #[test]
    fn test_collect_request_metrics_aggregates_traffic_per_type() {
        let stats = vec![
            ApiRequestStats {
                server: String::new(),
                name: "GetObject".to_string(),
                req_type: "s3".to_string(),
                in_flight: 1,
                total: 10,
                errors_total: 0,
                errors_5xx: 0,
                errors_4xx: 0,
                canceled: 0,
                ttfb_distribution: vec![],
                sent_bytes: 100,
                recv_bytes: 10,
                supported_metrics: ApiRequestMetricSupport::ALL,
            },
            ApiRequestStats {
                server: String::new(),
                name: "HeadObject".to_string(),
                req_type: "s3".to_string(),
                in_flight: 2,
                total: 20,
                errors_total: 1,
                errors_5xx: 1,
                errors_4xx: 0,
                canceled: 0,
                ttfb_distribution: vec![],
                sent_bytes: 200,
                recv_bytes: 20,
                supported_metrics: ApiRequestMetricSupport::ALL,
            },
        ];

        let metrics = collect_request_metrics(&stats);

        let sent_name = API_TRAFFIC_SENT_BYTES_MD.get_full_metric_name();
        let sent_metrics: Vec<_> = metrics.iter().filter(|metric| metric.name == sent_name).collect();
        assert_eq!(sent_metrics.len(), 1);
        assert_eq!(sent_metrics[0].value, 300.0);
        assert!(
            sent_metrics[0]
                .labels
                .iter()
                .any(|(key, value)| *key == TYPE_LABEL && value == "s3")
        );

        let recv_name = API_TRAFFIC_RECV_BYTES_MD.get_full_metric_name();
        let recv_metrics: Vec<_> = metrics.iter().filter(|metric| metric.name == recv_name).collect();
        assert_eq!(recv_metrics.len(), 1);
        assert_eq!(recv_metrics[0].value, 30.0);
        assert!(
            recv_metrics[0]
                .labels
                .iter()
                .any(|(key, value)| *key == TYPE_LABEL && value == "s3")
        );
    }
}
