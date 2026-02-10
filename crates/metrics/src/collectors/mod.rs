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
//! use rustfs_metrics::collectors::{
//!     collect_cluster_metrics, ClusterStats,
//!     collect_bucket_metrics, BucketStats,
//!     collect_node_metrics, DiskStats,
//!     collect_resource_metrics, ResourceStats,
//! };
//! use rustfs_metrics::report_metrics;
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
//! // Report to metrics system
//! report_metrics(&metrics);
//! ```

mod bucket;
mod cluster;
pub(crate) mod global;
mod node;
mod resource;

pub use bucket::{BucketStats, collect_bucket_metrics};
pub use cluster::{ClusterStats, collect_cluster_metrics};
pub use global::init_metrics_collectors;
pub use node::{DiskStats, collect_node_metrics};
pub use resource::{ResourceStats, collect_resource_metrics};
