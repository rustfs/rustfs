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

use crate::metrics::schema::{MetricDescriptor, MetricType};
use metrics::{counter, describe_counter, describe_gauge, describe_histogram, gauge};
use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

static NAME_CACHE: OnceLock<Mutex<HashMap<String, &'static str>>> = OnceLock::new();
static HELP_CACHE: OnceLock<Mutex<HashMap<String, &'static str>>> = OnceLock::new();

fn intern_string(cache: &OnceLock<Mutex<HashMap<String, &'static str>>>, value: &str) -> &'static str {
    let cache = cache.get_or_init(Default::default);
    let mut cache = cache.lock().unwrap_or_else(|poisoned| poisoned.into_inner());

    if let Some(existing) = cache.get(value) {
        existing
    } else {
        let value = Box::leak(value.to_string().into_boxed_str());
        cache.insert(value.to_string(), value);
        value
    }
}

fn into_static_str(cache: &OnceLock<Mutex<HashMap<String, &'static str>>>, value: &str) -> &'static str {
    intern_string(cache, value)
}

pub fn report_metrics(metrics: &[PrometheusMetric]) {
    for metric in metrics {
        let name = into_static_str(&NAME_CACHE, &metric.name);
        let help = into_static_str(&HELP_CACHE, &metric.help);

        match metric.metric_type {
            MetricType::Counter => describe_counter!(name, help),
            MetricType::Gauge => describe_gauge!(name, help),
            MetricType::Histogram => describe_histogram!(name, help),
        }

        let labels: Vec<(String, String)> = metric.labels.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();

        match metric.metric_type {
            MetricType::Counter => {
                let counter = counter!(name, &labels);
                counter.absolute(metric.value as u64);
            }
            MetricType::Gauge => {
                let gauge = gauge!(name, &labels);
                gauge.set(metric.value);
            }
            MetricType::Histogram => {
                let histogram = metrics::histogram!(name, &labels);
                histogram.record(metric.value);
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct PrometheusMetric {
    pub name: Cow<'static, str>,
    pub metric_type: MetricType,
    pub help: Cow<'static, str>,
    pub labels: Vec<(&'static str, Cow<'static, str>)>,
    pub value: f64,
}

impl PrometheusMetric {
    #[inline]
    pub const fn new(name: &'static str, metric_type: MetricType, help: &'static str, value: f64) -> Self {
        Self {
            name: Cow::Borrowed(name),
            metric_type,
            help: Cow::Borrowed(help),
            labels: Vec::new(),
            value,
        }
    }

    #[inline]
    pub fn new_owned(name: String, metric_type: MetricType, help: String, value: f64) -> Self {
        Self {
            name: Cow::Owned(name),
            metric_type,
            help: Cow::Owned(help),
            labels: Vec::new(),
            value,
        }
    }

    #[inline]
    pub fn from_descriptor(descriptor: &MetricDescriptor, value: f64) -> Self {
        let help = intern_string(&HELP_CACHE, &descriptor.help);
        Self {
            name: Cow::Owned(descriptor.get_full_metric_name()),
            metric_type: descriptor.metric_type,
            help: Cow::Borrowed(help),
            labels: Vec::new(),
            value,
        }
    }

    #[inline]
    #[allow(dead_code)]
    pub fn with_label(mut self, key: &'static str, value: impl Into<Cow<'static, str>>) -> Self {
        self.labels.push((key, value.into()));
        self
    }

    #[inline]
    #[allow(dead_code)]
    pub fn with_label_owned(mut self, key: &'static str, value: String) -> Self {
        self.labels.push((key, Cow::Owned(value)));
        self
    }

    #[inline]
    #[allow(dead_code)]
    pub fn with_labels(mut self, labels: Vec<(&'static str, Cow<'static, str>)>) -> Self {
        self.labels = labels;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::schema::{MetricName, MetricNamespace, MetricSubsystem};

    #[test]
    fn from_descriptor_uses_prometheus_metric_names_for_all_types() {
        let cases = [
            (MetricType::Counter, "rustfs_api_requests_total"),
            (MetricType::Gauge, "rustfs_system_memory_used_bytes"),
            (MetricType::Histogram, "rustfs_custom_path_latency_seconds"),
        ];

        for (metric_type, expected_name) in cases {
            let subsystem = match metric_type {
                MetricType::Counter => MetricSubsystem::ApiRequests,
                MetricType::Gauge => MetricSubsystem::SystemMemory,
                MetricType::Histogram => MetricSubsystem::new("/custom/path"),
            };
            let name = match metric_type {
                MetricType::Counter => MetricName::ApiRequestsTotal,
                MetricType::Gauge => MetricName::Custom("used_bytes".to_string()),
                MetricType::Histogram => MetricName::Custom("latency_seconds".to_string()),
            };

            let metric = PrometheusMetric::from_descriptor(
                &MetricDescriptor::new(name, metric_type, "test help".to_string(), vec![], MetricNamespace::RustFS, subsystem),
                1.0,
            );

            assert_eq!(metric.name, expected_name);
            assert_eq!(metric.metric_type, metric_type);
        }
    }
}
