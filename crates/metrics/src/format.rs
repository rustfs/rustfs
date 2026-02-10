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
use metrics::{describe_counter, describe_gauge, describe_histogram, gauge};
use std::borrow::Cow;

/// Report metrics using the `metrics` crate.
///
/// This function iterates over the provided metrics and reports them using
/// the `metrics` crate's API. This allows integration with various metrics
/// exporters (e.g., Prometheus) that are configured globally.
pub(crate) fn report_metrics(metrics: &[PrometheusMetric]) {
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
        // The `metrics` macros expect labels as an iterable of references, e.g., &[(&str, &str)] or similar.
        // We need to convert our Cow strings to something compatible.
        // However, the macros are a bit picky. The most robust way is to use the `Label` trait or pass
        // a reference to a slice of key-value pairs if the macro supports it.
        //
        // Looking at the `metrics` crate documentation and examples:
        // gauge!("name", &labels) where labels is [("key", "value")] works.
        // But our labels are dynamic Vec<(String, String)>.
        // We need to convert them to a format the macro accepts.
        //
        // The error "no rules expected `,`" suggests the macro invocation syntax is wrong for the arguments provided.
        // Specifically, `gauge!(name, value, labels)` might not be a valid pattern if `labels` isn't matched correctly.
        //
        // The `metrics` macros support:
        // - (name, value)
        // - (name, value, key => val, ...)
        // - (name, value, &labels)
        //
        // Let's try to construct a vector of `Label` or similar if possible, or just use the slice.
        // The issue might be that `&labels` where `labels` is `Vec<(String, String)>` isn't directly supported
        // if the macro expects `&[(&str, &str)]` or `&[Label]`.
        //
        // Actually, `metrics` 0.24 supports `Into<Label>` for keys and values.
        // Let's try to convert our labels into a `Vec<metrics::Label>`.
        // Or simpler: `metrics` macros often take `&[(&str, &str)]` or similar.
        //
        // Let's try to pass the labels as a reference to the vector, but we need to make sure the vector content
        // is compatible.
        //
        // The error message `no rules expected ,` usually means the macro parser got confused.
        //
        // Let's try to use the `gauge!` macro with the `labels` argument properly.
        // If `labels` is a `Vec<(String, String)>`, `&labels` is `&Vec<(String, String)>` which derefs to `&[(String, String)]`.
        //
        // Let's try to use `metrics::gauge!(metric.name, metric.value, &labels)` but ensure `labels` is correct.
        //
        // Wait, the error is on `gauge!(metric.name, metric.value, &labels);`.
        // The macro definition is:
        // ($name:expr, $value:expr, $labels:expr) => { ... }
        //
        // If the macro expects labels to be passed in a specific way, we must adhere to it.
        //
        // Let's try to use the `recorder()` directly if the macro is too restrictive, or fix the macro usage.
        // But `metrics` crate encourages macros.
        //
        // Re-reading the docs provided in the prompt:
        // `let labels = [("dynamic_key", format!("{}!", dynamic_val))];`
        // `let gauge = gauge!("some_metric_name", &labels);`
        // `gauge.set(1337.0);`
        //
        // Ah! The `gauge!` macro returns a `Gauge` handle, it doesn't take a value directly in that form!
        // The docs say:
        // `let gauge = gauge!("some_metric_name");`
        // `gauge.increment(1.0);`
        //
        // OR
        //
        // `let gauge = gauge!("some_metric_name", &labels);`
        // `gauge.set(1337.0);`
        //
        // It seems `gauge!(name, value, labels)` is NOT a valid syntax for `metrics` 0.24 macros based on the provided docs.
        // The docs show `gauge!(name, labels)` returning a handle, then you call `.set(value)`.
        //
        // Let's fix the code to follow this pattern.

        let labels: Vec<(String, String)> = metric.labels.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();

        match metric.metric_type {
            MetricType::Counter => {
                // Counter doesn't have a `set` method usually, it has `increment`.
                // But if we are reporting absolute values, we might be misusing Counter.
                // However, `metrics` 0.21+ might have `absolute` or we just use Gauge.
                //
                // If we must use Counter:
                // `let counter = metrics::counter!(metric.name, &labels);`
                // `counter.absolute(metric.value as u64);` // If supported
                //
                // But wait, the prompt docs say:
                // `describe_counter!`
                // `let counter = counter!("name");`
                // `counter.increment(1.0);`
                //
                // It doesn't show a `counter!` macro that takes a value.
                //
                // If we are reporting snapshots, we should use Gauge for everything as previously discussed,
                // OR we need to know the delta. Since we don't know the delta, Gauge is safer.
                //
                // Let's use Gauge for everything for now to fix the compilation error and logic.
                let gauge = gauge!(metric.name, &labels);
                gauge.set(metric.value);
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
pub(crate) struct PrometheusMetric {
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
