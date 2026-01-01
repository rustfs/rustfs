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

//! Prometheus metrics module for RustFS observability.
//!
//! This module provides types and utilities for collecting and rendering
//! metrics in Prometheus text exposition format.

pub mod auth;
pub mod collectors;
mod format;

pub use crate::metrics::MetricType;
pub use format::render_metrics;

/// A single Prometheus metric with labels and value.
#[derive(Debug, Clone)]
pub struct PrometheusMetric {
    /// The metric name (e.g., "http_requests_total").
    pub name: String,
    /// The type of this metric (counter, gauge, or histogram).
    pub metric_type: MetricType,
    /// Human-readable description shown in Prometheus UI.
    pub help: String,
    /// Key-value label pairs for this metric instance.
    pub labels: Vec<(String, String)>,
    /// The numeric value of this metric.
    pub value: f64,
}

impl PrometheusMetric {
    /// Creates a new metric with the given name, type, help text, and value.
    pub fn new(name: impl Into<String>, metric_type: MetricType, help: impl Into<String>, value: f64) -> Self {
        Self {
            name: name.into(),
            metric_type,
            help: help.into(),
            labels: Vec::new(),
            value,
        }
    }

    /// Adds a single label to this metric.
    pub fn with_label(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.labels.push((key.into(), value.into()));
        self
    }

    /// Sets all labels for this metric, replacing any existing labels.
    pub fn with_labels(mut self, labels: Vec<(String, String)>) -> Self {
        self.labels = labels;
        self
    }
}
