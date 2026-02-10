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

//! Prometheus metric collectors for RustFS.
//!
//! This module provides collectors that convert RustFS data into Prometheus
//! metrics format. Each collector is responsible for a specific domain:
//!
//! - [`cluster`]: Cluster-wide capacity and object statistics
//! - [`bucket`]: Per-bucket usage and quota metrics
//! - [`node`]: Per-node disk capacity and health metrics
//! - [`resource`]: System resource metrics (CPU, memory, uptime)
//!
//! # Design Philosophy
//!
//! Collectors accept simple data structs rather than internal RustFS types.
//! This design allows HTTP handlers to populate the structs from their
//! available data sources without creating circular dependencies.
//!
//! # Example
//!
//! ```
//! use rustfs_obs::collectors::{
//!     collect_cluster_metrics, ClusterStats,
//!     collect_bucket_metrics, BucketStats,
//!     collect_node_metrics, DiskStats,
//!     collect_resource_metrics, ResourceStats,
//! };
//! use rustfs_obs::render_metrics;
//!
//! // Collect cluster metrics
//! let cluster_stats = ClusterStats {
//!     raw_capacity_bytes: 1_000_000_000,
//!     used_bytes: 500_000_000,
//!     ..Default::default()
//! };
//! let mut metrics = collect_cluster_metrics(&cluster_stats);
//!
//! // Add bucket metrics
//! let bucket_stats = vec![BucketStats {
//!     name: "my-bucket".to_string(),
//!     size_bytes: 100_000,
//!     objects_count: 50,
//!     ..Default::default()
//! }];
//! metrics.extend(collect_bucket_metrics(&bucket_stats));
//!
//! // Render to Prometheus format
//! let output = render_metrics(&metrics);
//! ```

mod bucket;
mod cluster;
mod format;
mod node;
mod resource;

pub use bucket::{collect_bucket_metrics, BucketStats};
pub use cluster::{collect_cluster_metrics, ClusterStats};
pub use node::{collect_node_metrics, DiskStats};
pub use resource::{collect_resource_metrics, ResourceStats};

pub use crate::metrics::MetricType;
pub use format::render_metrics;

use std::borrow::Cow;

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
    pub fn with_label(mut self, key: &'static str, value: impl Into<Cow<'static, str>>) -> Self {
        self.labels.push((key, value.into()));
        self
    }

    /// Adds a label with an owned string value.
    ///
    /// Use this when the label value is dynamically generated.
    #[inline]
    pub fn with_label_owned(mut self, key: &'static str, value: String) -> Self {
        self.labels.push((key, Cow::Owned(value)));
        self
    }

    /// Sets all labels for this metric, replacing any existing labels.
    #[inline]
    pub fn with_labels(mut self, labels: Vec<(&'static str, Cow<'static, str>)>) -> Self {
        self.labels = labels;
        self
    }
}
