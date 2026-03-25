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
//! - [`bucket_replication`]: Per-target replication bandwidth metrics
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

mod audit;
mod bucket;
mod bucket_replication;
mod cluster;
mod cluster_config;
mod cluster_erasure_set;
mod cluster_health;
mod cluster_iam;
mod cluster_usage;
mod dial9;
pub(crate) mod global;
mod ilm;
mod logger_webhook;
mod node;
mod notification;
mod replication;
mod request;
mod resource;
mod scanner;
mod stats_collector;
mod system_cpu;
mod system_drive;
#[cfg(feature = "gpu")]
mod system_gpu;
mod system_memory;
mod system_network;
mod system_process;

pub use audit::{AuditTargetStats, collect_audit_metrics};
pub use bucket::{BucketStats, collect_bucket_metrics};
pub use bucket_replication::{BucketReplicationBandwidthStats, collect_bucket_replication_bandwidth_metrics};
pub use cluster::{ClusterStats, collect_cluster_metrics};
pub use cluster_config::{ClusterConfigStats, collect_cluster_config_metrics};
pub use cluster_erasure_set::{ErasureSetStats, collect_erasure_set_metrics};
pub use cluster_health::{ClusterHealthStats, collect_cluster_health_metrics};
pub use cluster_iam::{IamStats, collect_iam_metrics};
pub use cluster_usage::{BucketUsageStats, ClusterUsageStats, collect_bucket_usage_metrics, collect_cluster_usage_metrics};
pub use dial9::{Dial9Stats, collect_dial9_metrics, is_dial9_enabled};
pub use global::init_metrics_collectors;
pub use ilm::{IlmStats, collect_ilm_metrics};
pub use logger_webhook::{WebhookTargetStats, collect_webhook_metrics};
pub use node::{DiskStats, collect_node_metrics};
pub use notification::{NotificationStats, collect_notification_metrics};
pub use replication::{ReplicationStats, collect_replication_metrics};
pub use request::{ApiRequestStats, collect_request_metrics};
pub use resource::{ResourceStats, collect_resource_metrics};
pub use scanner::{ScannerStats, collect_scanner_metrics};
pub use system_cpu::{CpuStats, ProcessCpuStats, collect_cpu_metrics, collect_process_cpu_metrics};
pub use system_drive::{
    DriveCountStats, DriveDetailedStats, ProcessDiskStats, collect_drive_count_metrics, collect_drive_detailed_metrics,
    collect_process_disk_metrics,
};
#[cfg(feature = "gpu")]
pub use system_gpu::{GpuCollector, GpuError, GpuStats, collect_gpu_metrics};
pub use system_memory::{MemoryStats, ProcessMemoryStats, collect_memory_metrics, collect_process_memory_metrics};
pub use system_network::{NetworkStats, ProcessNetworkStats, collect_network_metrics, collect_process_network_metrics};
pub use system_process::{
    ProcessAttributeError, ProcessAttributes, ProcessStats, ProcessStatusType, collect_process_attributes,
    collect_process_metrics,
};
