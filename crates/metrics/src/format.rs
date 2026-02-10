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

//! Prometheus text exposition format renderer.
//!
//! This module renders metrics in the standard Prometheus text format.
//! Optimized for minimal allocations and fast rendering.

use crate::MetricType;
use metrics::{counter, describe_counter, describe_gauge, describe_histogram, gauge};
use std::borrow::Cow;

/// Report metrics using the `metrics` crate.
///
/// This function iterates over the provided metrics and reports them using
/// the `metrics` crate's API. This allows integration with various metrics
/// exporters (e.g., Prometheus) that are configured globally.
pub fn report_metrics(metrics: &[PrometheusMetric]) {
    for metric in metrics {
        // Register metric description (help text)
        // Note: In a real-world scenario, descriptions should ideally be registered once at startup.
        // However, the `metrics` crate handles duplicate registrations gracefully.
        match metric.metric_type {
            MetricType::Counter => describe_counter!(metric.name, metric.help),
            MetricType::Gauge => describe_gauge!(metric.name, metric.help),
            MetricType::Histogram => describe_histogram!(metric.name, metric.help),
        }

        // Convert labels to the format expected by `metrics` crate
        let labels: Vec<(String, String)> = metric.labels.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();

        // Report the metric value
        match metric.metric_type {
            MetricType::Counter => {
                // Use counter! macro to get a handle, then set absolute value.
                // Note: `metrics` crate counters are typically monotonic and support `increment`.
                // Setting an absolute value directly requires `absolute` method if supported by the backend/handle,
                // or we assume the value provided is the absolute count we want to report.
                //
                // Since `metrics` 0.21+, `Counter` has an `absolute` method which sets the counter to a specific value.
                // This is useful for mirroring an external counter.
                let counter = counter!(metric.name, &labels);
                counter.absolute(metric.value as u64);
            }
            MetricType::Gauge => {
                let gauge = gauge!(metric.name, &labels);
                gauge.set(metric.value);
            }
            MetricType::Histogram => {
                let histogram = metrics::histogram!(metric.name, &labels);
                histogram.record(metric.value);
            }
        }
    }
}

/// A single Prometheus metric with labels and value.
///
/// This struct is optimized for performance by using `&'static str` for
/// the name and help text, which are typically compile-time constants.
/// Labels use `Cow<'static, str>` to avoid allocations when possible.
#[derive(Debug, Clone)]
pub struct PrometheusMetric {
    /// The metric name (e.g., "http_requests_total").
    pub name: &'static str,
    /// The type of this metric (counter, gauge, or histogram).
    pub metric_type: MetricType,
    /// Human-readable description shown in Prometheus UI.
    pub help: &'static str,
    /// Key-value label pairs for this metric instance.
    /// Uses Cow to avoid allocations for static label keys.
    pub labels: Vec<(&'static str, Cow<'static, str>)>,
    /// The numeric value of this metric.
    pub value: f64,
}

impl PrometheusMetric {
    /// Creates a new metric with the given name, type, help text, and value.
    ///
    /// Uses static strings to avoid heap allocations for metric metadata.
    #[inline]
    pub const fn new(name: &'static str, metric_type: MetricType, help: &'static str, value: f64) -> Self {
        Self {
            name,
            metric_type,
            help,
            labels: Vec::new(),
            value,
        }
    }

    /// Adds a single label with a static value to this metric.
    #[inline]
    #[allow(dead_code)]
    pub fn with_label(mut self, key: &'static str, value: impl Into<Cow<'static, str>>) -> Self {
        self.labels.push((key, value.into()));
        self
    }

    /// Adds a label with an owned string value.
    ///
    /// Use this when the label value is dynamically generated.
    #[inline]
    #[allow(dead_code)]
    pub fn with_label_owned(mut self, key: &'static str, value: String) -> Self {
        self.labels.push((key, Cow::Owned(value)));
        self
    }

    /// Sets all labels for this metric, replacing any existing labels.
    #[inline]
    #[allow(dead_code)]
    pub fn with_labels(mut self, labels: Vec<(&'static str, Cow<'static, str>)>) -> Self {
        self.labels = labels;
        self
    }
}
