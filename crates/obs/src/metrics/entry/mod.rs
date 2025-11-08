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

use crate::{MetricDescriptor, MetricName, MetricNamespace, MetricSubsystem, MetricType};

pub(crate) mod descriptor;
pub(crate) mod metric_name;
pub(crate) mod metric_type;
pub(crate) mod namespace;
mod path_utils;
pub(crate) mod subsystem;

/// Create a new counter metric descriptor
pub fn new_counter_md(
    name: impl Into<MetricName>,
    help: impl Into<String>,
    labels: &[&str],
    subsystem: impl Into<MetricSubsystem>,
) -> MetricDescriptor {
    MetricDescriptor::new(
        name.into(),
        MetricType::Counter,
        help.into(),
        labels.iter().map(|&s| s.to_string()).collect(),
        MetricNamespace::RustFS,
        subsystem,
    )
}

/// create a new dashboard metric descriptor
pub fn new_gauge_md(
    name: impl Into<MetricName>,
    help: impl Into<String>,
    labels: &[&str],
    subsystem: impl Into<MetricSubsystem>,
) -> MetricDescriptor {
    MetricDescriptor::new(
        name.into(),
        MetricType::Gauge,
        help.into(),
        labels.iter().map(|&s| s.to_string()).collect(),
        MetricNamespace::RustFS,
        subsystem,
    )
}

/// create a new histogram indicator descriptor
#[allow(dead_code)]
pub fn new_histogram_md(
    name: impl Into<MetricName>,
    help: impl Into<String>,
    labels: &[&str],
    subsystem: impl Into<MetricSubsystem>,
) -> MetricDescriptor {
    MetricDescriptor::new(
        name.into(),
        MetricType::Histogram,
        help.into(),
        labels.iter().map(|&s| s.to_string()).collect(),
        MetricNamespace::RustFS,
        subsystem,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::subsystems;

    #[test]
    fn test_new_histogram_md() {
        // create a histogram indicator descriptor
        let histogram_md = new_histogram_md(
            MetricName::TtfbDistribution,
            "test the response time distribution",
            &["api", "method", "le"],
            subsystems::API_REQUESTS,
        );

        // verify that the metric type is correct
        assert_eq!(histogram_md.metric_type, MetricType::Histogram);

        // verify that the metric name is correct
        assert_eq!(histogram_md.name.as_str(), "seconds_distribution");

        // verify that the help information is correct
        assert_eq!(histogram_md.help, "test the response time distribution");

        // Verify that the label is correct
        assert_eq!(histogram_md.variable_labels.len(), 3);
        assert!(histogram_md.variable_labels.contains(&"api".to_string()));
        assert!(histogram_md.variable_labels.contains(&"method".to_string()));
        assert!(histogram_md.variable_labels.contains(&"le".to_string()));

        // Verify that the namespace is correct
        assert_eq!(histogram_md.namespace, MetricNamespace::RustFS);

        // Verify that the subsystem is correct
        assert_eq!(histogram_md.subsystem, MetricSubsystem::ApiRequests);

        // Verify that the full metric name generated is formatted correctly
        assert_eq!(histogram_md.get_full_metric_name(), "histogram.rustfs_api_requests_seconds_distribution");

        // Tests use custom subsystems
        let custom_histogram_md = new_histogram_md(
            "custom_latency_distribution",
            "custom latency distribution",
            &["endpoint", "le"],
            MetricSubsystem::new("/custom/path-metrics"),
        );

        // Verify the custom name and subsystem
        assert_eq!(
            custom_histogram_md.get_full_metric_name(),
            "histogram.rustfs_custom_path_metrics_custom_latency_distribution"
        );
    }
}
