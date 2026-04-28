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

pub mod audit;
pub mod bucket;
pub mod bucket_replication;
pub mod cluster;
pub mod cluster_config;
pub mod cluster_erasure_set;
pub mod cluster_health;
pub mod cluster_iam;
pub mod cluster_usage;
pub mod dial9;
pub mod ilm;
pub mod node;
pub mod notification;
pub mod notification_target;
pub mod replication;
pub mod request;
pub mod resource;
pub mod scanner;
pub mod system_cpu;
pub mod system_drive;
#[cfg(feature = "gpu")]
pub mod system_gpu;
pub mod system_memory;
pub mod system_network;
pub mod system_network_host;
pub mod system_process;

pub use audit::{AuditTargetStats, collect_audit_metrics};
pub use bucket::{BucketStats, collect_bucket_metrics};
pub use bucket_replication::{
    BucketReplicationBandwidthStats, BucketReplicationStats, BucketReplicationTargetStats,
    collect_bucket_replication_bandwidth_metrics, collect_bucket_replication_metrics,
};
pub use cluster::{ClusterStats, collect_cluster_metrics};
pub use cluster_config::{ClusterConfigStats, collect_cluster_config_metrics};
pub use cluster_erasure_set::{ErasureSetStats, collect_erasure_set_metrics};
pub use cluster_health::{ClusterHealthStats, collect_cluster_health_metrics};
pub use cluster_iam::{IamStats, collect_iam_metrics};
pub use cluster_usage::{BucketUsageStats, ClusterUsageStats, collect_bucket_usage_metrics, collect_cluster_usage_metrics};
pub use dial9::{Dial9Stats, collect_dial9_metrics, is_dial9_enabled};
pub use ilm::{IlmStats, collect_ilm_metrics};
pub use node::{DiskStats, collect_node_metrics};
pub use notification::{NotificationStats, collect_notification_metrics};
pub use notification_target::{NotificationTargetStats, collect_notification_target_metrics};
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
pub use system_network::{NetworkStats, collect_network_metrics};
pub use system_network_host::{HostNetworkStats, collect_host_network_metrics};
pub use system_process::{
    ProcessAttributeError, ProcessAttributes, ProcessStats, ProcessStatusType, collect_process_attributes,
    collect_process_metrics,
};
