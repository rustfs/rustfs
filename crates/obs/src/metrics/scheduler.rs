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

//! Global metrics collector initialization.
//!
//! This module provides the entry point for initializing all metrics collectors.
//! The actual statistics collection functions are in `stats_collector.rs`.
//!
//! System monitoring collectors (migrated from `rustfs-obs::system`):
//! - Process CPU metrics
//! - Process memory metrics
//! - Process disk I/O metrics
//! - Host network I/O metrics

use crate::metrics::collectors::{
    AuditTargetRuntimeStats,
    AuditTargetStats,
    BucketReplicationBacklogStats,
    BucketReplicationBandwidthStats,
    BucketReplicationRuntimeStats,
    DriveRuntimeDetailedStats,
    NotificationStats,
    NotificationTargetRuntimeStats,
    NotificationTargetStats,
    // System monitoring collectors (migrated from rustfs-obs::system)
    ProcessAttributeError,
    ProcessCpuStats,
    ProcessDiskStats,
    ProcessMemoryStats,
    ReplicationRuntimeStats,
    ScannerRuntimeStats,
    collect_audit_runtime_metrics,
    collect_bucket_metrics,
    collect_bucket_replication_backlog_metrics,
    collect_bucket_replication_bandwidth_metrics,
    collect_bucket_replication_runtime_metrics,
    collect_bucket_usage_metrics,
    collect_cluster_config_metrics,
    collect_cluster_health_metrics,
    collect_cluster_metrics,
    collect_cluster_usage_metrics,
    collect_compression_cluster_metrics,
    collect_cpu_metrics,
    collect_current_dial9_metrics,
    collect_drive_count_metrics,
    collect_drive_runtime_detailed_metrics,
    collect_erasure_set_metrics,
    collect_host_network_metrics,
    collect_iam_metrics,
    collect_ilm_runtime_metrics,
    collect_memory_metrics,
    collect_network_metrics,
    collect_node_metrics,
    collect_notification_runtime_metrics,
    collect_notification_target_runtime_metrics,
    collect_process_attributes,
    collect_process_cpu_metrics,
    collect_process_disk_metrics,
    collect_process_memory_metrics,
    collect_process_metrics,
    collect_replication_runtime_metrics,
    collect_request_metrics,
    collect_resource_metrics,
    collect_scanner_runtime_metrics,
};
use crate::metrics::config::{
    DEFAULT_AUDIT_METRICS_INTERVAL, DEFAULT_BUCKET_METRICS_INTERVAL, DEFAULT_BUCKET_REPLICATION_BANDWIDTH_METRICS_INTERVAL,
    DEFAULT_CLUSTER_METRICS_INTERVAL, DEFAULT_NODE_METRICS_INTERVAL, DEFAULT_NOTIFICATION_METRICS_INTERVAL,
    DEFAULT_RESOURCE_METRICS_INTERVAL, ENV_AUDIT_METRICS_INTERVAL, ENV_BUCKET_METRICS_INTERVAL,
    ENV_BUCKET_REPLICATION_BANDWIDTH_METRICS_INTERVAL, ENV_CLUSTER_METRICS_INTERVAL, ENV_DEFAULT_METRICS_INTERVAL,
    ENV_NODE_METRICS_INTERVAL, ENV_NOTIFICATION_METRICS_INTERVAL, ENV_RESOURCE_METRICS_INTERVAL,
};
use crate::metrics::obs_is_disk_compression_enabled;
use crate::metrics::report::{PrometheusMetric, report_metrics};
use crate::metrics::runtime_sources::bucket_monitor_available;
use crate::metrics::schema::audit::{
    AUDIT_FAILED_MESSAGES_BY_SERVER_MD, AUDIT_FAILED_MESSAGES_MD, AUDIT_FAILED_STORE_LENGTH_BY_SERVER_MD,
    AUDIT_FAILED_STORE_LENGTH_MD, AUDIT_TARGET_QUEUE_LENGTH_BY_SERVER_MD, AUDIT_TARGET_QUEUE_LENGTH_MD,
    AUDIT_TOTAL_MESSAGES_BY_SERVER_MD, AUDIT_TOTAL_MESSAGES_MD, SERVER as AUDIT_SERVER_LABEL,
};
use crate::metrics::schema::bucket_replication::{
    BUCKET_L, BUCKET_REPL_BANDWIDTH_CURRENT_MD, BUCKET_REPL_BANDWIDTH_LIMIT_MD, BUCKET_REPL_CURRENT_BACKLOG_BYTES_MD,
    BUCKET_REPL_CURRENT_BACKLOG_COUNT_MD, BUCKET_REPL_CURRENT_TARGET_BACKLOG_BYTES_MD,
    BUCKET_REPL_CURRENT_TARGET_BACKLOG_COUNT_MD, BUCKET_REPL_DURABLE_MRF_AVAILABLE_MD, BUCKET_REPL_DURABLE_MRF_BACKLOG_BYTES_MD,
    BUCKET_REPL_DURABLE_MRF_BACKLOG_COUNT_MD, BUCKET_REPL_DURABLE_MRF_TARGET_BACKLOG_BYTES_MD,
    BUCKET_REPL_DURABLE_MRF_TARGET_BACKLOG_COUNT_MD, BUCKET_REPL_LATENCY_MS_MD, BUCKET_REPL_MRF_DROPPED_COUNT_MD,
    BUCKET_REPL_MRF_FLUSH_FAILURES_MD, BUCKET_REPL_MRF_LAST_FLUSH_DURATION_MILLIS_MD, BUCKET_REPL_MRF_MISSED_COUNT_MD,
    BUCKET_REPL_MRF_PENDING_BYTES_MD, BUCKET_REPL_MRF_PENDING_COUNT_MD, BUCKET_REPL_PROXY_REQUESTS_TOTAL_MD,
    BUCKET_REPL_TARGET_LAST_HOUR_FAILED_BYTES_MD, BUCKET_REPL_TARGET_LAST_HOUR_FAILED_COUNT_MD,
    BUCKET_REPL_TARGET_LAST_MIN_FAILED_BYTES_MD, BUCKET_REPL_TARGET_LAST_MIN_FAILED_COUNT_MD, BUCKET_REPL_TARGET_SENT_BYTES_MD,
    BUCKET_REPL_TARGET_SENT_COUNT_MD, BUCKET_REPL_TARGET_TOTAL_FAILED_BYTES_MD, BUCKET_REPL_TARGET_TOTAL_FAILED_COUNT_MD,
    OPERATION_L, RANGE_L, RESULT_L, TARGET_ARN_L,
};
use crate::metrics::schema::cluster::{CLUSTER_BUCKETS_TOTAL_MD, CLUSTER_OBJECTS_TOTAL_MD};
use crate::metrics::schema::cluster_usage::{
    BUCKET_LABEL as USAGE_BUCKET_LABEL, RANGE_LABEL as USAGE_RANGE_LABEL, USAGE_BUCKET_DELETE_MARKERS_COUNT_MD,
    USAGE_BUCKET_OBJECT_SIZE_DISTRIBUTION_MD, USAGE_BUCKET_OBJECT_VERSION_COUNT_DISTRIBUTION_MD, USAGE_BUCKET_OBJECTS_TOTAL_MD,
    USAGE_BUCKET_QUOTA_TOTAL_BYTES_MD, USAGE_BUCKET_TOTAL_BYTES_MD, USAGE_BUCKET_VERSIONS_COUNT_MD, USAGE_BUCKETS_COUNT_MD,
    USAGE_DELETE_MARKERS_COUNT_MD, USAGE_OBJECTS_COUNT_MD, USAGE_OBJECTS_DISTRIBUTION_MD, USAGE_SINCE_LAST_UPDATE_SECONDS_MD,
    USAGE_TOTAL_BYTES_MD, USAGE_VERSIONS_COUNT_MD, USAGE_VERSIONS_DISTRIBUTION_MD,
};
use crate::metrics::schema::node_bucket::{BUCKET_OBJECTS_TOTAL_MD, BUCKET_QUOTA_BYTES_MD, BUCKET_USAGE_BYTES_MD};
use crate::metrics::schema::notification_target::{
    NOTIFICATION_TARGET_FAILED_MESSAGES_BY_SERVER_MD, NOTIFICATION_TARGET_FAILED_MESSAGES_MD,
    NOTIFICATION_TARGET_FAILED_STORE_LENGTH_BY_SERVER_MD, NOTIFICATION_TARGET_FAILED_STORE_LENGTH_MD,
    NOTIFICATION_TARGET_QUEUE_LENGTH_BY_SERVER_MD, NOTIFICATION_TARGET_QUEUE_LENGTH_MD,
    NOTIFICATION_TARGET_TOTAL_MESSAGES_BY_SERVER_MD, NOTIFICATION_TARGET_TOTAL_MESSAGES_MD, SERVER as NOTIFICATION_SERVER_LABEL,
    TARGET_ID as NOTIFICATION_TARGET_ID_LABEL, TARGET_TYPE as NOTIFICATION_TARGET_TYPE_LABEL,
};
use crate::metrics::schema::scanner::{
    BUCKET_LABEL as SCANNER_BUCKET_LABEL, CYCLE_SCOPE_LABEL as SCANNER_CYCLE_SCOPE_LABEL, DRIVE_LABEL as SCANNER_DRIVE_LABEL,
    RESULT_LABEL as SCANNER_RESULT_LABEL, SCANNER_ACTIVE_BUCKET_DRIVE_SCAN_AGE_SECONDS_MD, SCANNER_ACTIVE_BUCKET_DRIVE_SCANS_MD,
    SCANNER_BUCKET_DRIVE_RESULT_TOTAL_MD, SCANNER_CYCLE_BUCKET_DRIVE_RESULT_MD, SOURCE_LABEL as SCANNER_SOURCE_LABEL,
};
use crate::metrics::schema::system_drive::{
    API_LABEL as DRIVE_API_LABEL, DISK_ID_LABEL, DRIVE_API_CALLS_MD, DRIVE_API_LATENCY_BY_API_MD, DRIVE_DELETES_TOTAL_MD,
    DRIVE_HEALING_MD, DRIVE_INDEX_LABEL, DRIVE_INFO_MD, DRIVE_LABEL, DRIVE_OFFLINE_DURATION_SECONDS_MD, DRIVE_RUNTIME_STATE_MD,
    DRIVE_SCANNING_MD, DRIVE_WRITES_TOTAL_MD, POOL_INDEX_LABEL, SET_INDEX_LABEL, STATE_LABEL as DRIVE_STATE_LABEL,
};
use crate::metrics::schema::system_process::{PROCESS_EXECUTABLE_NAME_LABEL, PROCESS_PID_LABEL};
use crate::metrics::stats_collector::{
    ProcessMetricBundle, collect_api_request_stats, collect_bucket_replication_bandwidth_stats,
    collect_bucket_replication_stats_bundle, collect_bucket_stats, collect_cluster_and_health_stats,
    collect_cluster_config_stats, collect_cluster_usage_metric_stats, collect_compression_cluster_stats,
    collect_disk_and_system_drive_runtime_stats, collect_erasure_set_stats, collect_host_network_stats, collect_iam_stats,
    collect_ilm_runtime_metric_stats, collect_internode_network_stats, collect_process_metric_bundle_with,
    collect_replication_stats, collect_scanner_runtime_metric_stats, collect_system_cpu_and_memory_stats_with,
};
use crate::node_identity::{SERVER_LABEL, current_local_node_identity};
use crate::telemetry::retire_metric_series;
use futures_util::FutureExt;
use rustfs_audit::audit_target_metrics;
use rustfs_io_metrics::ProcessSampler;
use rustfs_notify::{notification_metrics_snapshot, notification_target_metrics};
use rustfs_utils::get_env_opt_u64;
use serde::Serialize;
use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::panic::AssertUnwindSafe;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;
use sysinfo::{Networks, System};
use tokio::time::{Instant, Interval, MissedTickBehavior};
use tokio_util::sync::CancellationToken;
use tracing::warn;

const LOG_COMPONENT_OBS: &str = "obs";
const LOG_SUBSYSTEM_METRICS_RUNTIME: &str = "metrics_runtime";
const EVENT_METRICS_RUNTIME_STATE: &str = "metrics_runtime_state";

/// Default interval for system monitoring metrics (15 seconds)
const DEFAULT_SYSTEM_METRICS_INTERVAL: Duration = Duration::from_secs(15);
/// Environment variable for system monitoring interval
const ENV_SYSTEM_METRICS_INTERVAL: &str = "RUSTFS_METRICS_SYSTEM_INTERVAL_SEC";
/// Legacy environment variable for system monitoring interval
const LEGACY_SYSTEM_METRICS_INTERVAL: &str = "RUSTFS_OBS_METRICS_SYSTEM_INTERVAL_MS";
const LEGACY_CLUSTER_INTERVAL: &str = "RUSTFS_METRICS_CLUSTER_INTERVAL";
const LEGACY_BUCKET_INTERVAL: &str = "RUSTFS_METRICS_BUCKET_INTERVAL";
const LEGACY_NODE_INTERVAL: &str = "RUSTFS_METRICS_NODE_INTERVAL";
const LEGACY_REPLICATION_BANDWIDTH_INTERVAL: &str = "RUSTFS_METRICS_BUCKET_REPLICATION_BANDWIDTH_INTERVAL";
const LEGACY_RESOURCE_INTERVAL: &str = "RUSTFS_METRICS_RESOURCE_INTERVAL";
const LEGACY_AUDIT_INTERVAL: &str = "RUSTFS_METRICS_AUDIT_INTERVAL";
const LEGACY_NOTIFICATION_INTERVAL: &str = "RUSTFS_METRICS_NOTIFICATION_INTERVAL";
const LEGACY_DEFAULT_INTERVAL: &str = "RUSTFS_METRICS_DEFAULT_INTERVAL";
const AUDIT_TARGET_ID_LABEL: &str = "target_id";

/// Default cycles to emit zero for removed replication bandwidth series before letting them expire.
const DEFAULT_REPL_BW_ZERO_TOMBSTONE_CYCLES: u8 = 3;
/// Env var that overrides the zero-emission tombstone cycles for removed replication bandwidth series.
const ENV_REPL_BW_ZERO_TOMBSTONE_CYCLES: &str = "RUSTFS_METRICS_REPL_BW_ZERO_TOMBSTONE_CYCLES";
const METRICS_RUNTIME_SERVICE_NAME: &str = "metrics_runtime";
const METRICS_RUNTIME_BASE_COLLECTOR_TASKS: u8 = 11;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
enum MetricsCollectorTaskId {
    ClusterStats = 0,
    SupplementaryClusterStats = 1,
    BucketStats = 2,
    NodeDiskStats = 3,
    BucketReplicationBandwidth = 4,
    AuditTargetStats = 5,
    NotificationStats = 6,
    BackgroundWorkflowStats = 7,
    ProcessMetrics = 8,
    InternodeNetworkStats = 9,
    RequestStats = 10,
    CompressionClusterStats = 11,
}

const BASE_COLLECTOR_TASK_IDS: [MetricsCollectorTaskId; METRICS_RUNTIME_BASE_COLLECTOR_TASKS as usize] = [
    MetricsCollectorTaskId::ClusterStats,
    MetricsCollectorTaskId::SupplementaryClusterStats,
    MetricsCollectorTaskId::BucketStats,
    MetricsCollectorTaskId::NodeDiskStats,
    MetricsCollectorTaskId::BucketReplicationBandwidth,
    MetricsCollectorTaskId::AuditTargetStats,
    MetricsCollectorTaskId::NotificationStats,
    MetricsCollectorTaskId::BackgroundWorkflowStats,
    MetricsCollectorTaskId::ProcessMetrics,
    MetricsCollectorTaskId::InternodeNetworkStats,
    MetricsCollectorTaskId::RequestStats,
];

const ALL_COLLECTOR_TASK_IDS: [MetricsCollectorTaskId; METRICS_RUNTIME_BASE_COLLECTOR_TASKS as usize + 1] = [
    MetricsCollectorTaskId::ClusterStats,
    MetricsCollectorTaskId::SupplementaryClusterStats,
    MetricsCollectorTaskId::BucketStats,
    MetricsCollectorTaskId::NodeDiskStats,
    MetricsCollectorTaskId::BucketReplicationBandwidth,
    MetricsCollectorTaskId::AuditTargetStats,
    MetricsCollectorTaskId::NotificationStats,
    MetricsCollectorTaskId::BackgroundWorkflowStats,
    MetricsCollectorTaskId::ProcessMetrics,
    MetricsCollectorTaskId::InternodeNetworkStats,
    MetricsCollectorTaskId::RequestStats,
    MetricsCollectorTaskId::CompressionClusterStats,
];

#[derive(Debug)]
struct MetricsRuntimeCollectorHealth {
    last_success_unix_secs: [AtomicU64; ALL_COLLECTOR_TASK_IDS.len()],
    collector_panics_total: [AtomicU64; ALL_COLLECTOR_TASK_IDS.len()],
    failure_state: [AtomicBool; ALL_COLLECTOR_TASK_IDS.len()],
}

impl MetricsRuntimeCollectorHealth {
    fn new() -> Self {
        Self {
            last_success_unix_secs: std::array::from_fn(|_| AtomicU64::new(0)),
            collector_panics_total: std::array::from_fn(|_| AtomicU64::new(0)),
            failure_state: std::array::from_fn(|_| AtomicBool::new(false)),
        }
    }

    fn record_success(&self, collector_id: MetricsCollectorTaskId) {
        let idx = collector_id as usize;
        self.last_success_unix_secs[idx].store(unix_timestamp_secs_now(), Ordering::Relaxed);
        self.failure_state[idx].store(false, Ordering::Relaxed);
    }

    fn record_panic(&self, collector_id: MetricsCollectorTaskId) {
        let idx = collector_id as usize;
        self.collector_panics_total[idx].fetch_add(1, Ordering::Relaxed);
        self.failure_state[idx].store(true, Ordering::Relaxed);
    }

    fn snapshot(&self, active_collectors: &[MetricsCollectorTaskId]) -> MetricsRuntimeCollectorHealthSnapshot {
        let now = unix_timestamp_secs_now();
        let mut healthy_collectors = 0_u8;
        let mut never_succeeded_collectors = 0_u8;
        let mut collector_panics_total = 0_u64;
        let mut oldest_success_age_secs = 0_u64;

        for collector_id in active_collectors {
            let idx = *collector_id as usize;
            let last_success = self.last_success_unix_secs[idx].load(Ordering::Relaxed);
            let has_failed = self.failure_state[idx].load(Ordering::Relaxed);
            collector_panics_total =
                collector_panics_total.saturating_add(self.collector_panics_total[idx].load(Ordering::Relaxed));

            if last_success == 0 {
                never_succeeded_collectors = never_succeeded_collectors.saturating_add(1);
            } else {
                oldest_success_age_secs = oldest_success_age_secs.max(now.saturating_sub(last_success));
            }

            if last_success > 0 && !has_failed {
                healthy_collectors = healthy_collectors.saturating_add(1);
            }
        }

        MetricsRuntimeCollectorHealthSnapshot {
            healthy_collectors,
            unhealthy_collectors: u8::try_from(active_collectors.len())
                .unwrap_or(u8::MAX)
                .saturating_sub(healthy_collectors),
            never_succeeded_collectors,
            collector_panics_total,
            oldest_success_age_secs,
        }
    }
}

static METRICS_RUNTIME_COLLECTOR_HEALTH: OnceLock<MetricsRuntimeCollectorHealth> = OnceLock::new();

type ReplBwKey = (String, String); // (bucket, target_arn)
type BucketKey = String;
type BucketRangeKey = (String, String); // (bucket, range)
type AuditLegacyTargetKey = String;
type AuditTargetKey = (String, String); // (server, target_id)
type NotificationLegacyTargetKey = (String, String); // (target_id, target_type)
type NotificationTargetKey = (String, String, String); // (server, target_id, target_type)
type DriveTopologyKey = (String, String, String, String, String); // (server, drive, pool, set, drive_index)
type DriveBasicKey = (String, String); // (server, drive)
type DriveTopologyApiKey = (String, String, String, String, String, String); // (server, drive, pool, set, drive_index, api)
type DriveInfoKey = (String, String, String, String, String, String); // (server, drive, pool, set, drive_index, disk_id)
type ScannerCycleBucketDriveResultKey = (String, String, String, String, String); // (server, cycle_scope, bucket, drive, result)
type ScannerBucketDriveResultKey = (String, String, String, String); // (server, bucket, drive, result)
type ScannerActiveBucketDriveKey = (String, String, String, String); // (server, source, bucket, drive)

fn drive_info_live_keys(stats: &[DriveRuntimeDetailedStats]) -> HashSet<DriveInfoKey> {
    stats.iter().filter_map(drive_info_key).collect()
}

fn drive_basic_live_keys(stats: &[DriveRuntimeDetailedStats]) -> HashSet<DriveBasicKey> {
    stats
        .iter()
        .map(|stat| (stat.stats.server.clone(), stat.stats.drive.clone()))
        .collect()
}

fn retire_drive_basic_metric_series(key: &DriveBasicKey) -> usize {
    let labels = [
        (SERVER_LABEL, Cow::Owned(key.0.clone())),
        (DRIVE_LABEL, Cow::Owned(key.1.clone())),
    ];
    retire_metric_series(&DRIVE_WRITES_TOTAL_MD.get_full_metric_name(), &labels)
        + retire_metric_series(&DRIVE_DELETES_TOTAL_MD.get_full_metric_name(), &labels)
}

fn drive_topology_live_keys(stats: &[DriveRuntimeDetailedStats]) -> HashSet<DriveTopologyKey> {
    stats.iter().filter_map(drive_topology_key).collect()
}

fn drive_topology_api_live_keys(stats: &[DriveRuntimeDetailedStats]) -> HashSet<DriveTopologyApiKey> {
    stats
        .iter()
        .filter_map(|stat| {
            let topology = drive_topology_key(stat)?;
            Some(
                stat.api_calls
                    .iter()
                    .map(|(api, _)| api)
                    .chain(stat.api_latency_by_api_micros.iter().map(|(api, _)| api))
                    .map(move |api| {
                        (
                            topology.0.clone(),
                            topology.1.clone(),
                            topology.2.clone(),
                            topology.3.clone(),
                            topology.4.clone(),
                            api.clone(),
                        )
                    }),
            )
        })
        .flatten()
        .collect()
}

fn drive_topology_key(stat: &DriveRuntimeDetailedStats) -> Option<DriveTopologyKey> {
    Some((
        stat.stats.server.clone(),
        stat.stats.drive.clone(),
        stat.pool_index.as_ref()?.clone(),
        stat.set_index.as_ref()?.clone(),
        stat.drive_index.as_ref()?.clone(),
    ))
}

fn drive_info_key(stat: &DriveRuntimeDetailedStats) -> Option<DriveInfoKey> {
    let disk_id = stat.disk_id.as_ref().filter(|disk_id| !disk_id.is_empty())?;
    Some((
        stat.stats.server.clone(),
        stat.stats.drive.clone(),
        stat.pool_index.as_ref()?.clone(),
        stat.set_index.as_ref()?.clone(),
        stat.drive_index.as_ref()?.clone(),
        disk_id.clone(),
    ))
}

fn retire_drive_info_metric_series(key: &DriveInfoKey) -> usize {
    let labels = [
        (SERVER_LABEL, Cow::Owned(key.0.clone())),
        (DRIVE_LABEL, Cow::Owned(key.1.clone())),
        (POOL_INDEX_LABEL, Cow::Owned(key.2.clone())),
        (SET_INDEX_LABEL, Cow::Owned(key.3.clone())),
        (DRIVE_INDEX_LABEL, Cow::Owned(key.4.clone())),
        (DISK_ID_LABEL, Cow::Owned(key.5.clone())),
    ];
    retire_metric_series(&DRIVE_INFO_MD.get_full_metric_name(), &labels)
}

fn retire_drive_topology_metric_series(key: &DriveTopologyKey) -> usize {
    let labels = [
        (SERVER_LABEL, Cow::Owned(key.0.clone())),
        (DRIVE_LABEL, Cow::Owned(key.1.clone())),
        (POOL_INDEX_LABEL, Cow::Owned(key.2.clone())),
        (SET_INDEX_LABEL, Cow::Owned(key.3.clone())),
        (DRIVE_INDEX_LABEL, Cow::Owned(key.4.clone())),
    ];
    let mut retired = 0;
    for descriptor in [&DRIVE_HEALING_MD, &DRIVE_SCANNING_MD, &DRIVE_OFFLINE_DURATION_SECONDS_MD] {
        retired += retire_metric_series(&descriptor.get_full_metric_name(), &labels);
    }
    for state in ["online", "offline", "returning", "suspect", "unknown"] {
        let state_labels = [
            (SERVER_LABEL, Cow::Owned(key.0.clone())),
            (DRIVE_LABEL, Cow::Owned(key.1.clone())),
            (POOL_INDEX_LABEL, Cow::Owned(key.2.clone())),
            (SET_INDEX_LABEL, Cow::Owned(key.3.clone())),
            (DRIVE_INDEX_LABEL, Cow::Owned(key.4.clone())),
            (DRIVE_STATE_LABEL, Cow::Borrowed(state)),
        ];
        retired += retire_metric_series(&DRIVE_RUNTIME_STATE_MD.get_full_metric_name(), &state_labels);
    }
    retired
}

fn retire_drive_topology_api_metric_series(key: &DriveTopologyApiKey) -> usize {
    let labels = [
        (SERVER_LABEL, Cow::Owned(key.0.clone())),
        (DRIVE_LABEL, Cow::Owned(key.1.clone())),
        (POOL_INDEX_LABEL, Cow::Owned(key.2.clone())),
        (SET_INDEX_LABEL, Cow::Owned(key.3.clone())),
        (DRIVE_INDEX_LABEL, Cow::Owned(key.4.clone())),
        (DRIVE_API_LABEL, Cow::Owned(key.5.clone())),
    ];
    retire_metric_series(&DRIVE_API_CALLS_MD.get_full_metric_name(), &labels)
        + retire_metric_series(&DRIVE_API_LATENCY_BY_API_MD.get_full_metric_name(), &labels)
}

fn scanner_cycle_bucket_drive_result_live_keys(stats: &ScannerRuntimeStats) -> HashSet<ScannerCycleBucketDriveResultKey> {
    stats
        .current_cycle_bucket_drive_results
        .iter()
        .map(|result| {
            (
                stats.server.clone(),
                "current".to_string(),
                result.bucket.clone(),
                result.drive.clone(),
                result.result.clone(),
            )
        })
        .chain(stats.last_cycle_bucket_drive_results.iter().map(|result| {
            (
                stats.server.clone(),
                "last".to_string(),
                result.bucket.clone(),
                result.drive.clone(),
                result.result.clone(),
            )
        }))
        .collect()
}

fn scanner_bucket_drive_result_live_keys(stats: &ScannerRuntimeStats) -> HashSet<ScannerBucketDriveResultKey> {
    stats
        .bucket_drive_results
        .iter()
        .map(|result| (stats.server.clone(), result.bucket.clone(), result.drive.clone(), result.result.clone()))
        .collect()
}

fn retire_scanner_cycle_bucket_drive_result_metric_series(key: &ScannerCycleBucketDriveResultKey) -> usize {
    let labels = [
        (SERVER_LABEL, Cow::Owned(key.0.clone())),
        (SCANNER_CYCLE_SCOPE_LABEL, Cow::Owned(key.1.clone())),
        (SCANNER_BUCKET_LABEL, Cow::Owned(key.2.clone())),
        (SCANNER_DRIVE_LABEL, Cow::Owned(key.3.clone())),
        (SCANNER_RESULT_LABEL, Cow::Owned(key.4.clone())),
    ];
    retire_metric_series(&SCANNER_CYCLE_BUCKET_DRIVE_RESULT_MD.get_full_metric_name(), &labels)
}

fn retire_scanner_bucket_drive_result_metric_series(key: &ScannerBucketDriveResultKey) -> usize {
    let labels = [
        (SERVER_LABEL, Cow::Owned(key.0.clone())),
        (SCANNER_BUCKET_LABEL, Cow::Owned(key.1.clone())),
        (SCANNER_DRIVE_LABEL, Cow::Owned(key.2.clone())),
        (SCANNER_RESULT_LABEL, Cow::Owned(key.3.clone())),
    ];
    retire_metric_series(&SCANNER_BUCKET_DRIVE_RESULT_TOTAL_MD.get_full_metric_name(), &labels)
}

fn scanner_active_bucket_drive_live_keys(stats: &ScannerRuntimeStats) -> HashSet<ScannerActiveBucketDriveKey> {
    stats
        .active_bucket_drive_scans
        .iter()
        .map(|active| (stats.server.clone(), active.source.clone(), active.bucket.clone(), active.drive.clone()))
        .collect()
}

fn retire_scanner_active_bucket_drive_metric_series(key: &ScannerActiveBucketDriveKey) -> usize {
    let labels = [
        (SERVER_LABEL, Cow::Owned(key.0.clone())),
        (SCANNER_SOURCE_LABEL, Cow::Owned(key.1.clone())),
        (SCANNER_BUCKET_LABEL, Cow::Owned(key.2.clone())),
        (SCANNER_DRIVE_LABEL, Cow::Owned(key.3.clone())),
    ];
    retire_metric_series(&SCANNER_ACTIVE_BUCKET_DRIVE_SCANS_MD.get_full_metric_name(), &labels)
        + retire_metric_series(&SCANNER_ACTIVE_BUCKET_DRIVE_SCAN_AGE_SECONDS_MD.get_full_metric_name(), &labels)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Default)]
pub struct MetricsRuntimeCollectorHealthSnapshot {
    pub healthy_collectors: u8,
    pub unhealthy_collectors: u8,
    pub never_succeeded_collectors: u8,
    pub collector_panics_total: u64,
    pub oldest_success_age_secs: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MetricsRuntimeServiceState {
    Disabled,
    Running,
    Stopping,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MetricsRuntimeCancellationSource {
    RuntimeToken,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MetricsRuntimeShutdownHandle {
    RuntimeTokenOnly,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct MetricsRuntimeIntervalsSnapshot {
    pub cluster_interval_secs: u64,
    pub bucket_interval_secs: u64,
    pub bucket_replication_bandwidth_interval_secs: u64,
    pub node_interval_secs: u64,
    pub resource_interval_secs: u64,
    pub audit_interval_secs: u64,
    pub notification_interval_secs: u64,
    pub system_interval_secs: u64,
    pub process_interval_secs: u64,
    pub replication_bandwidth_zero_tombstone_cycles: u8,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct MetricsRuntimeStatusSnapshot {
    pub service: &'static str,
    pub state: MetricsRuntimeServiceState,
    pub metrics_enabled: bool,
    pub collector_tasks: u8,
    pub collector_health: MetricsRuntimeCollectorHealthSnapshot,
    pub intervals: MetricsRuntimeIntervalsSnapshot,
    pub cancellation_source: MetricsRuntimeCancellationSource,
    pub shutdown_handle: MetricsRuntimeShutdownHandle,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MetricsRuntimeDesiredState {
    Disabled,
    Enabled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct MetricsRuntimeDesiredSnapshot {
    pub state: MetricsRuntimeDesiredState,
    pub collector_tasks: u8,
    pub intervals: MetricsRuntimeIntervalsSnapshot,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct MetricsRuntimeControllerSnapshot {
    pub desired: MetricsRuntimeDesiredSnapshot,
    pub status: MetricsRuntimeStatusSnapshot,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MetricsRuntimeWorkerMutation {
    None,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct MetricsRuntimeReconcilePlan {
    pub service: &'static str,
    pub desired: MetricsRuntimeDesiredSnapshot,
    pub current_state: MetricsRuntimeServiceState,
    pub worker_mutation: MetricsRuntimeWorkerMutation,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct MetricsRuntimeController;

impl MetricsRuntimeController {
    pub fn snapshot(&self, token: &CancellationToken) -> MetricsRuntimeControllerSnapshot {
        metrics_runtime_controller_snapshot(token)
    }

    pub fn reconcile(&self, token: &CancellationToken) -> MetricsRuntimeReconcilePlan {
        let snapshot = self.snapshot(token);
        self.reconcile_snapshot(snapshot)
    }

    pub fn reconcile_snapshot(&self, snapshot: MetricsRuntimeControllerSnapshot) -> MetricsRuntimeReconcilePlan {
        MetricsRuntimeReconcilePlan {
            service: METRICS_RUNTIME_SERVICE_NAME,
            desired: snapshot.desired,
            current_state: snapshot.status.state,
            worker_mutation: MetricsRuntimeWorkerMutation::None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct MetricsRuntimeConfig {
    cluster_interval: Duration,
    bucket_interval: Duration,
    bucket_replication_bandwidth_interval: Duration,
    node_interval: Duration,
    resource_interval: Duration,
    audit_interval: Duration,
    notification_interval: Duration,
    system_interval: Duration,
    process_interval: Duration,
    replication_bandwidth_zero_tombstone_cycles: u8,
}

fn parse_repl_bw_zero_tombstone_cycles() -> u8 {
    get_env_opt_u64(ENV_REPL_BW_ZERO_TOMBSTONE_CYCLES)
        .filter(|&v| v > 0)
        .map(|v| u8::try_from(v).unwrap_or(u8::MAX))
        .unwrap_or(DEFAULT_REPL_BW_ZERO_TOMBSTONE_CYCLES)
}

/// Parse metrics interval from environment variables with fallback to default.
///
/// Priority: primary_env > legacy_env > default_env > legacy_default > default_value
fn parse_metrics_interval(primary_env: &str, legacy_env: &str, default_interval: Duration) -> Duration {
    get_env_opt_u64(primary_env)
        .or_else(|| get_env_opt_u64(legacy_env))
        .or_else(|| get_env_opt_u64(ENV_DEFAULT_METRICS_INTERVAL))
        .or_else(|| get_env_opt_u64(LEGACY_DEFAULT_INTERVAL))
        .filter(|&v| v > 0)
        .map(Duration::from_secs)
        .unwrap_or(default_interval)
}

fn parse_system_metrics_interval() -> Duration {
    get_env_opt_u64(ENV_SYSTEM_METRICS_INTERVAL)
        .or_else(|| get_env_opt_u64(LEGACY_SYSTEM_METRICS_INTERVAL).map(|ms| if ms == 0 { 0 } else { ms.div_ceil(1000) }))
        .or_else(|| get_env_opt_u64(ENV_DEFAULT_METRICS_INTERVAL))
        .filter(|&v| v > 0)
        .map(Duration::from_secs)
        .unwrap_or(DEFAULT_SYSTEM_METRICS_INTERVAL)
}

fn configured_metrics_runtime_config() -> MetricsRuntimeConfig {
    let cluster_interval =
        parse_metrics_interval(ENV_CLUSTER_METRICS_INTERVAL, LEGACY_CLUSTER_INTERVAL, DEFAULT_CLUSTER_METRICS_INTERVAL);
    let bucket_interval =
        parse_metrics_interval(ENV_BUCKET_METRICS_INTERVAL, LEGACY_BUCKET_INTERVAL, DEFAULT_BUCKET_METRICS_INTERVAL);
    let bucket_replication_bandwidth_interval = parse_metrics_interval(
        ENV_BUCKET_REPLICATION_BANDWIDTH_METRICS_INTERVAL,
        LEGACY_REPLICATION_BANDWIDTH_INTERVAL,
        DEFAULT_BUCKET_REPLICATION_BANDWIDTH_METRICS_INTERVAL,
    );
    let node_interval = parse_metrics_interval(ENV_NODE_METRICS_INTERVAL, LEGACY_NODE_INTERVAL, DEFAULT_NODE_METRICS_INTERVAL);
    let resource_interval =
        parse_metrics_interval(ENV_RESOURCE_METRICS_INTERVAL, LEGACY_RESOURCE_INTERVAL, DEFAULT_RESOURCE_METRICS_INTERVAL);
    let audit_interval =
        parse_metrics_interval(ENV_AUDIT_METRICS_INTERVAL, LEGACY_AUDIT_INTERVAL, DEFAULT_AUDIT_METRICS_INTERVAL);
    let notification_interval = parse_metrics_interval(
        ENV_NOTIFICATION_METRICS_INTERVAL,
        LEGACY_NOTIFICATION_INTERVAL,
        DEFAULT_NOTIFICATION_METRICS_INTERVAL,
    );
    let system_interval = parse_system_metrics_interval();
    let process_interval = resource_interval.min(system_interval);

    MetricsRuntimeConfig {
        cluster_interval,
        bucket_interval,
        bucket_replication_bandwidth_interval,
        node_interval,
        resource_interval,
        audit_interval,
        notification_interval,
        system_interval,
        process_interval,
        replication_bandwidth_zero_tombstone_cycles: parse_repl_bw_zero_tombstone_cycles(),
    }
}

fn metrics_runtime_intervals_snapshot(config: MetricsRuntimeConfig) -> MetricsRuntimeIntervalsSnapshot {
    MetricsRuntimeIntervalsSnapshot {
        cluster_interval_secs: config.cluster_interval.as_secs(),
        bucket_interval_secs: config.bucket_interval.as_secs(),
        bucket_replication_bandwidth_interval_secs: config.bucket_replication_bandwidth_interval.as_secs(),
        node_interval_secs: config.node_interval.as_secs(),
        resource_interval_secs: config.resource_interval.as_secs(),
        audit_interval_secs: config.audit_interval.as_secs(),
        notification_interval_secs: config.notification_interval.as_secs(),
        system_interval_secs: config.system_interval.as_secs(),
        process_interval_secs: config.process_interval.as_secs(),
        replication_bandwidth_zero_tombstone_cycles: config.replication_bandwidth_zero_tombstone_cycles,
    }
}

fn metrics_runtime_collector_health() -> &'static MetricsRuntimeCollectorHealth {
    METRICS_RUNTIME_COLLECTOR_HEALTH.get_or_init(MetricsRuntimeCollectorHealth::new)
}

fn active_metrics_collector_task_ids(compression_enabled: bool) -> &'static [MetricsCollectorTaskId] {
    if compression_enabled {
        &ALL_COLLECTOR_TASK_IDS
    } else {
        &BASE_COLLECTOR_TASK_IDS
    }
}

fn metrics_runtime_collector_tasks(compression_enabled: bool) -> u8 {
    u8::try_from(active_metrics_collector_task_ids(compression_enabled).len()).unwrap_or(u8::MAX)
}

fn build_metrics_runtime_status_snapshot(
    metrics_enabled: bool,
    cancellation_requested: bool,
    config: MetricsRuntimeConfig,
    compression_enabled: bool,
) -> MetricsRuntimeStatusSnapshot {
    let state = if !metrics_enabled {
        MetricsRuntimeServiceState::Disabled
    } else if cancellation_requested {
        MetricsRuntimeServiceState::Stopping
    } else {
        MetricsRuntimeServiceState::Running
    };

    let collector_health = if metrics_enabled {
        metrics_runtime_collector_health().snapshot(active_metrics_collector_task_ids(compression_enabled))
    } else {
        MetricsRuntimeCollectorHealthSnapshot::default()
    };

    MetricsRuntimeStatusSnapshot {
        service: METRICS_RUNTIME_SERVICE_NAME,
        state,
        metrics_enabled,
        collector_tasks: metrics_runtime_collector_tasks(compression_enabled),
        collector_health,
        intervals: metrics_runtime_intervals_snapshot(config),
        cancellation_source: MetricsRuntimeCancellationSource::RuntimeToken,
        shutdown_handle: MetricsRuntimeShutdownHandle::RuntimeTokenOnly,
    }
}

fn build_metrics_runtime_desired_snapshot(
    metrics_enabled: bool,
    config: MetricsRuntimeConfig,
    compression_enabled: bool,
) -> MetricsRuntimeDesiredSnapshot {
    let state = if metrics_enabled {
        MetricsRuntimeDesiredState::Enabled
    } else {
        MetricsRuntimeDesiredState::Disabled
    };

    MetricsRuntimeDesiredSnapshot {
        state,
        collector_tasks: metrics_runtime_collector_tasks(compression_enabled),
        intervals: metrics_runtime_intervals_snapshot(config),
    }
}

fn build_metrics_runtime_controller_snapshot(
    metrics_enabled: bool,
    cancellation_requested: bool,
    config: MetricsRuntimeConfig,
    compression_enabled: bool,
) -> MetricsRuntimeControllerSnapshot {
    MetricsRuntimeControllerSnapshot {
        desired: build_metrics_runtime_desired_snapshot(metrics_enabled, config, compression_enabled),
        status: build_metrics_runtime_status_snapshot(metrics_enabled, cancellation_requested, config, compression_enabled),
    }
}

pub fn metrics_runtime_status_snapshot(token: &CancellationToken) -> MetricsRuntimeStatusSnapshot {
    build_metrics_runtime_status_snapshot(
        crate::observability_metric_enabled(),
        token.is_cancelled(),
        configured_metrics_runtime_config(),
        obs_is_disk_compression_enabled(),
    )
}

pub fn metrics_runtime_controller_snapshot(token: &CancellationToken) -> MetricsRuntimeControllerSnapshot {
    build_metrics_runtime_controller_snapshot(
        crate::observability_metric_enabled(),
        token.is_cancelled(),
        configured_metrics_runtime_config(),
        obs_is_disk_compression_enabled(),
    )
}

fn unix_timestamp_secs_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn stagger_duration(period: Duration, numerator: u32, denominator: u32) -> Duration {
    let staggered_nanos = period
        .as_nanos()
        .saturating_mul(u128::from(numerator))
        .checked_div(u128::from(denominator))
        .unwrap_or(0)
        .min(u128::from(u64::MAX));
    Duration::from_nanos(u64::try_from(staggered_nanos).unwrap_or(u64::MAX))
}

fn metrics_interval(period: Duration, initial_delay: Duration) -> Interval {
    let mut interval = tokio::time::interval_at(Instant::now() + initial_delay, period);
    interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
    interval
}

async fn run_metrics_collector_tick<F>(
    health: &MetricsRuntimeCollectorHealth,
    collector_id: MetricsCollectorTaskId,
    collector: &'static str,
    future: F,
) where
    F: std::future::Future<Output = ()>,
{
    match AssertUnwindSafe(future).catch_unwind().await {
        Ok(()) => health.record_success(collector_id),
        Err(payload) => {
            health.record_panic(collector_id);
            let panic_message = payload
                .downcast_ref::<&'static str>()
                .copied()
                .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
                .unwrap_or("unknown panic payload");
            warn!(
                event = EVENT_METRICS_RUNTIME_STATE,
                component = LOG_COMPONENT_OBS,
                subsystem = LOG_SUBSYSTEM_METRICS_RUNTIME,
                collector,
                result = "panic_caught",
                panic_message,
                "metrics runtime state changed"
            );
        }
    }
}

fn repl_bw_live_keys(stats: &[BucketReplicationBandwidthStats]) -> HashSet<ReplBwKey> {
    stats.iter().map(|s| (s.bucket.clone(), s.target_arn.clone())).collect()
}

fn repl_backlog_live_keys(stats: &[BucketReplicationBacklogStats]) -> HashSet<BucketKey> {
    stats.iter().map(|s| s.bucket.clone()).collect()
}

fn repl_backlog_target_live_keys(stats: &[BucketReplicationBacklogStats]) -> HashSet<ReplBwKey> {
    stats
        .iter()
        .flat_map(|stat| {
            stat.target_backlogs
                .iter()
                .map(|target| (stat.bucket.clone(), target.target_arn.clone()))
        })
        .collect()
}

fn repl_flow_live_keys(stats: &[BucketReplicationRuntimeStats]) -> HashSet<ReplBwKey> {
    stats
        .iter()
        .flat_map(|stat| {
            stat.target_flows
                .iter()
                .map(|target| (stat.stats.bucket.clone(), target.target_arn.clone()))
        })
        .collect()
}

fn repl_proxy_bucket_live_keys(stats: &[BucketReplicationRuntimeStats]) -> HashSet<BucketKey> {
    stats.iter().map(|stat| stat.stats.bucket.clone()).collect()
}

fn update_series_zero_tombstones<T: Clone + Eq + std::hash::Hash>(
    has_seen_valid_snapshot: &mut bool,
    prev_live_keys: &mut HashSet<T>,
    zero_tombstones: &mut HashMap<T, u8>,
    current_live_keys: HashSet<T>,
    tombstone_cycles: u8,
) {
    if *has_seen_valid_snapshot {
        for removed in prev_live_keys.difference(&current_live_keys) {
            zero_tombstones.insert(removed.clone(), tombstone_cycles);
        }
    }

    for key in &current_live_keys {
        zero_tombstones.remove(key);
    }

    *prev_live_keys = current_live_keys;
    *has_seen_valid_snapshot = true;
}

fn expire_series_zero_tombstones<T: Clone + Eq + std::hash::Hash>(zero_tombstones: &mut HashMap<T, u8>) -> Vec<T> {
    let mut expired = Vec::new();
    if !zero_tombstones.is_empty() {
        zero_tombstones.retain(|key, remaining| {
            if *remaining <= 1 {
                expired.push(key.clone());
                false
            } else {
                *remaining -= 1;
                true
            }
        });
    }
    expired
}

fn bucket_live_keys(stats: &[crate::metrics::collectors::BucketStats]) -> HashSet<BucketKey> {
    stats.iter().map(|stat| stat.name.clone()).collect()
}

fn bucket_observation_live_keys(stats: &[crate::metrics::collectors::BucketStats]) -> HashSet<BucketKey> {
    stats
        .iter()
        .filter(|stat| stat.size_bytes.is_some() || stat.objects_count.is_some())
        .map(|stat| stat.name.clone())
        .collect()
}

fn bucket_observation_retire_keys(
    previous_observations: &HashSet<BucketKey>,
    current_buckets: &HashSet<BucketKey>,
    current_observations: &HashSet<BucketKey>,
) -> Vec<BucketKey> {
    previous_observations
        .difference(current_observations)
        .filter(|bucket| current_buckets.contains(*bucket))
        .cloned()
        .collect()
}

fn collect_bucket_zero_tombstone_metrics(zero_tombstones: &HashMap<BucketKey, u8>) -> Vec<PrometheusMetric> {
    if zero_tombstones.is_empty() {
        return Vec::new();
    }

    let mut zero_metrics = Vec::with_capacity(zero_tombstones.len() * 3);
    for bucket in zero_tombstones.keys() {
        let bucket_label: Cow<'static, str> = Cow::Owned(bucket.clone());
        zero_metrics
            .push(PrometheusMetric::from_descriptor(&BUCKET_USAGE_BYTES_MD, 0.0).with_label("bucket", bucket_label.clone()));
        zero_metrics
            .push(PrometheusMetric::from_descriptor(&BUCKET_OBJECTS_TOTAL_MD, 0.0).with_label("bucket", bucket_label.clone()));
        zero_metrics.push(PrometheusMetric::from_descriptor(&BUCKET_QUOTA_BYTES_MD, 0.0).with_label("bucket", bucket_label));
    }

    zero_metrics
}

#[derive(Default)]
struct BucketSeriesState {
    has_seen_snapshot: bool,
    live_keys: HashSet<BucketKey>,
    observation_keys: HashSet<BucketKey>,
    zero_tombstones: HashMap<BucketKey, u8>,
}

struct BucketSeriesUpdate {
    metrics: Vec<PrometheusMetric>,
    retire_observations: Vec<BucketKey>,
    retire_buckets: Vec<BucketKey>,
}

impl BucketSeriesState {
    fn observe(
        &mut self,
        stats: Option<&[crate::metrics::collectors::BucketStats]>,
        tombstone_cycles: u8,
    ) -> Option<BucketSeriesUpdate> {
        let stats = stats?;
        let current_bucket_keys = bucket_live_keys(stats);
        let current_observation_keys = bucket_observation_live_keys(stats);
        let retire_observations =
            bucket_observation_retire_keys(&self.observation_keys, &current_bucket_keys, &current_observation_keys);
        self.observation_keys = current_observation_keys;
        update_series_zero_tombstones(
            &mut self.has_seen_snapshot,
            &mut self.live_keys,
            &mut self.zero_tombstones,
            current_bucket_keys,
            tombstone_cycles,
        );
        let mut metrics = collect_bucket_metrics(stats);
        metrics.extend(collect_bucket_zero_tombstone_metrics(&self.zero_tombstones));
        let retire_buckets = expire_series_zero_tombstones(&mut self.zero_tombstones);
        Some(BucketSeriesUpdate {
            metrics,
            retire_observations,
            retire_buckets,
        })
    }
}

fn retire_bucket_metric_series(bucket: &str) -> usize {
    let bucket_label: Cow<'static, str> = Cow::Owned(bucket.to_string());
    let labels = [("bucket", bucket_label.clone())];
    retire_metric_series(&BUCKET_USAGE_BYTES_MD.get_full_metric_name(), &labels)
        + retire_metric_series(&BUCKET_OBJECTS_TOTAL_MD.get_full_metric_name(), &labels)
        + retire_metric_series(&BUCKET_QUOTA_BYTES_MD.get_full_metric_name(), &labels)
}

fn retire_bucket_observation_metric_series(bucket: &str) -> usize {
    let labels = [("bucket", Cow::Owned(bucket.to_string()))];
    retire_metric_series(&BUCKET_USAGE_BYTES_MD.get_full_metric_name(), &labels)
        + retire_metric_series(&BUCKET_OBJECTS_TOTAL_MD.get_full_metric_name(), &labels)
}

fn retire_cluster_usage_metric_series() -> usize {
    let labels: [(&'static str, Cow<'static, str>); 0] = [];
    [
        USAGE_SINCE_LAST_UPDATE_SECONDS_MD.get_full_metric_name(),
        USAGE_TOTAL_BYTES_MD.get_full_metric_name(),
        USAGE_OBJECTS_COUNT_MD.get_full_metric_name(),
        USAGE_VERSIONS_COUNT_MD.get_full_metric_name(),
        USAGE_DELETE_MARKERS_COUNT_MD.get_full_metric_name(),
        USAGE_BUCKETS_COUNT_MD.get_full_metric_name(),
    ]
    .iter()
    .map(|name| retire_metric_series(name, &labels))
    .sum()
}

fn retire_cluster_usage_distribution_series(metric_name: String, range: &str) -> usize {
    let labels = [(USAGE_RANGE_LABEL, Cow::Owned(range.to_string()))];
    retire_metric_series(&metric_name, &labels)
}

fn bucket_usage_live_keys(stats: &[crate::metrics::collectors::BucketUsageStats]) -> HashSet<BucketKey> {
    stats.iter().map(|stat| stat.bucket.clone()).collect()
}

fn bucket_usage_object_size_live_keys(stats: &[crate::metrics::collectors::BucketUsageStats]) -> HashSet<BucketRangeKey> {
    stats
        .iter()
        .flat_map(|stat| {
            stat.object_size_distribution
                .iter()
                .map(move |(range, _)| (stat.bucket.clone(), range.clone()))
        })
        .collect()
}

fn bucket_usage_version_live_keys(stats: &[crate::metrics::collectors::BucketUsageStats]) -> HashSet<BucketRangeKey> {
    stats
        .iter()
        .flat_map(|stat| {
            stat.version_count_distribution
                .iter()
                .map(move |(range, _)| (stat.bucket.clone(), range.clone()))
        })
        .collect()
}

fn collect_bucket_usage_zero_tombstone_metrics(
    zero_bucket_tombstones: &HashMap<BucketKey, u8>,
    zero_object_size_tombstones: &HashMap<BucketRangeKey, u8>,
    zero_version_tombstones: &HashMap<BucketRangeKey, u8>,
) -> Vec<PrometheusMetric> {
    let mut zero_metrics =
        Vec::with_capacity(zero_bucket_tombstones.len() * 5 + zero_object_size_tombstones.len() + zero_version_tombstones.len());

    for bucket in zero_bucket_tombstones.keys() {
        let bucket_label: Cow<'static, str> = Cow::Owned(bucket.clone());
        zero_metrics.push(
            PrometheusMetric::from_descriptor(&USAGE_BUCKET_TOTAL_BYTES_MD, 0.0)
                .with_label(USAGE_BUCKET_LABEL, bucket_label.clone()),
        );
        zero_metrics.push(
            PrometheusMetric::from_descriptor(&USAGE_BUCKET_OBJECTS_TOTAL_MD, 0.0)
                .with_label(USAGE_BUCKET_LABEL, bucket_label.clone()),
        );
        zero_metrics.push(
            PrometheusMetric::from_descriptor(&USAGE_BUCKET_VERSIONS_COUNT_MD, 0.0)
                .with_label(USAGE_BUCKET_LABEL, bucket_label.clone()),
        );
        zero_metrics.push(
            PrometheusMetric::from_descriptor(&USAGE_BUCKET_DELETE_MARKERS_COUNT_MD, 0.0)
                .with_label(USAGE_BUCKET_LABEL, bucket_label.clone()),
        );
        zero_metrics.push(
            PrometheusMetric::from_descriptor(&USAGE_BUCKET_QUOTA_TOTAL_BYTES_MD, 0.0)
                .with_label(USAGE_BUCKET_LABEL, bucket_label),
        );
    }

    for (bucket, range) in zero_object_size_tombstones.keys() {
        zero_metrics.push(
            PrometheusMetric::from_descriptor(&USAGE_BUCKET_OBJECT_SIZE_DISTRIBUTION_MD, 0.0)
                .with_label(USAGE_RANGE_LABEL, Cow::Owned(range.clone()))
                .with_label(USAGE_BUCKET_LABEL, Cow::Owned(bucket.clone())),
        );
    }

    for (bucket, range) in zero_version_tombstones.keys() {
        zero_metrics.push(
            PrometheusMetric::from_descriptor(&USAGE_BUCKET_OBJECT_VERSION_COUNT_DISTRIBUTION_MD, 0.0)
                .with_label(USAGE_RANGE_LABEL, Cow::Owned(range.clone()))
                .with_label(USAGE_BUCKET_LABEL, Cow::Owned(bucket.clone())),
        );
    }

    zero_metrics
}

fn retire_bucket_usage_metric_series(bucket: &str) -> usize {
    let bucket_label: Cow<'static, str> = Cow::Owned(bucket.to_string());
    let labels = [(USAGE_BUCKET_LABEL, bucket_label.clone())];
    retire_metric_series(&USAGE_BUCKET_TOTAL_BYTES_MD.get_full_metric_name(), &labels)
        + retire_metric_series(&USAGE_BUCKET_OBJECTS_TOTAL_MD.get_full_metric_name(), &labels)
        + retire_metric_series(&USAGE_BUCKET_VERSIONS_COUNT_MD.get_full_metric_name(), &labels)
        + retire_metric_series(&USAGE_BUCKET_DELETE_MARKERS_COUNT_MD.get_full_metric_name(), &labels)
        + retire_metric_series(&USAGE_BUCKET_QUOTA_TOTAL_BYTES_MD.get_full_metric_name(), &labels)
}

fn retire_bucket_usage_distribution_series(metric_name: String, bucket: &str, range: &str) -> usize {
    let labels = [
        (USAGE_RANGE_LABEL, Cow::Owned(range.to_string())),
        (USAGE_BUCKET_LABEL, Cow::Owned(bucket.to_string())),
    ];
    retire_metric_series(&metric_name, &labels)
}

fn audit_target_live_keys(stats: &[AuditTargetRuntimeStats]) -> HashSet<AuditTargetKey> {
    stats
        .iter()
        .map(|stat| (stat.server.clone(), stat.target.target_id.clone()))
        .collect()
}

fn audit_legacy_target_live_keys(stats: &[AuditTargetRuntimeStats]) -> HashSet<AuditLegacyTargetKey> {
    stats.iter().map(|stat| stat.target.target_id.clone()).collect()
}

fn collect_audit_legacy_zero_tombstone_metrics(zero_tombstones: &HashMap<AuditLegacyTargetKey, u8>) -> Vec<PrometheusMetric> {
    if zero_tombstones.is_empty() {
        return Vec::new();
    }

    let mut zero_metrics = Vec::with_capacity(zero_tombstones.len() * 4);
    for target_id in zero_tombstones.keys() {
        let target_id_label: Cow<'static, str> = Cow::Owned(target_id.clone());
        zero_metrics.push(
            PrometheusMetric::from_descriptor(&AUDIT_FAILED_MESSAGES_MD, 0.0)
                .with_label(AUDIT_TARGET_ID_LABEL, target_id_label.clone()),
        );
        zero_metrics.push(
            PrometheusMetric::from_descriptor(&AUDIT_FAILED_STORE_LENGTH_MD, 0.0)
                .with_label(AUDIT_TARGET_ID_LABEL, target_id_label.clone()),
        );
        zero_metrics.push(
            PrometheusMetric::from_descriptor(&AUDIT_TARGET_QUEUE_LENGTH_MD, 0.0)
                .with_label(AUDIT_TARGET_ID_LABEL, target_id_label.clone()),
        );
        zero_metrics.push(
            PrometheusMetric::from_descriptor(&AUDIT_TOTAL_MESSAGES_MD, 0.0).with_label(AUDIT_TARGET_ID_LABEL, target_id_label),
        );
    }

    zero_metrics
}

fn collect_audit_zero_tombstone_metrics(zero_tombstones: &HashMap<AuditTargetKey, u8>) -> Vec<PrometheusMetric> {
    if zero_tombstones.is_empty() {
        return Vec::new();
    }

    let mut zero_metrics = Vec::with_capacity(zero_tombstones.len() * 4);
    for (server, target_id) in zero_tombstones.keys() {
        let server_label: Cow<'static, str> = Cow::Owned(server.clone());
        let target_id_label: Cow<'static, str> = Cow::Owned(target_id.clone());
        zero_metrics.push(
            PrometheusMetric::from_descriptor(&AUDIT_FAILED_MESSAGES_BY_SERVER_MD, 0.0)
                .with_label(AUDIT_SERVER_LABEL, server_label.clone())
                .with_label(AUDIT_TARGET_ID_LABEL, target_id_label.clone()),
        );
        zero_metrics.push(
            PrometheusMetric::from_descriptor(&AUDIT_FAILED_STORE_LENGTH_BY_SERVER_MD, 0.0)
                .with_label(AUDIT_SERVER_LABEL, server_label.clone())
                .with_label(AUDIT_TARGET_ID_LABEL, target_id_label.clone()),
        );
        zero_metrics.push(
            PrometheusMetric::from_descriptor(&AUDIT_TARGET_QUEUE_LENGTH_BY_SERVER_MD, 0.0)
                .with_label(AUDIT_SERVER_LABEL, server_label.clone())
                .with_label(AUDIT_TARGET_ID_LABEL, target_id_label.clone()),
        );
        zero_metrics.push(
            PrometheusMetric::from_descriptor(&AUDIT_TOTAL_MESSAGES_BY_SERVER_MD, 0.0)
                .with_label(AUDIT_SERVER_LABEL, server_label)
                .with_label(AUDIT_TARGET_ID_LABEL, target_id_label),
        );
    }

    zero_metrics
}

fn retire_audit_legacy_target_metric_series(target_id: &str) -> usize {
    let labels = [(AUDIT_TARGET_ID_LABEL, Cow::Owned(target_id.to_string()))];
    retire_metric_series(&AUDIT_FAILED_MESSAGES_MD.get_full_metric_name(), &labels)
        + retire_metric_series(&AUDIT_FAILED_STORE_LENGTH_MD.get_full_metric_name(), &labels)
        + retire_metric_series(&AUDIT_TARGET_QUEUE_LENGTH_MD.get_full_metric_name(), &labels)
        + retire_metric_series(&AUDIT_TOTAL_MESSAGES_MD.get_full_metric_name(), &labels)
}

fn retire_audit_target_metric_series(server: &str, target_id: &str) -> usize {
    let server_labels = [
        (AUDIT_SERVER_LABEL, Cow::Owned(server.to_string())),
        (AUDIT_TARGET_ID_LABEL, Cow::Owned(target_id.to_string())),
    ];
    retire_metric_series(&AUDIT_FAILED_MESSAGES_BY_SERVER_MD.get_full_metric_name(), &server_labels)
        + retire_metric_series(&AUDIT_FAILED_STORE_LENGTH_BY_SERVER_MD.get_full_metric_name(), &server_labels)
        + retire_metric_series(&AUDIT_TARGET_QUEUE_LENGTH_BY_SERVER_MD.get_full_metric_name(), &server_labels)
        + retire_metric_series(&AUDIT_TOTAL_MESSAGES_BY_SERVER_MD.get_full_metric_name(), &server_labels)
}

fn notification_target_live_keys(stats: &[NotificationTargetRuntimeStats]) -> HashSet<NotificationTargetKey> {
    stats
        .iter()
        .map(|stat| (stat.server.clone(), stat.target.target_id.clone(), stat.target.target_type.clone()))
        .collect()
}

fn notification_legacy_target_live_keys(stats: &[NotificationTargetRuntimeStats]) -> HashSet<NotificationLegacyTargetKey> {
    stats
        .iter()
        .map(|stat| (stat.target.target_id.clone(), stat.target.target_type.clone()))
        .collect()
}

fn collect_notification_legacy_target_zero_tombstone_metrics(
    zero_tombstones: &HashMap<NotificationLegacyTargetKey, u8>,
) -> Vec<PrometheusMetric> {
    if zero_tombstones.is_empty() {
        return Vec::new();
    }

    let mut zero_metrics = Vec::with_capacity(zero_tombstones.len() * 4);
    for (target_id, target_type) in zero_tombstones.keys() {
        let target_id_label: Cow<'static, str> = Cow::Owned(target_id.clone());
        let target_type_label: Cow<'static, str> = Cow::Owned(target_type.clone());
        zero_metrics.push(
            PrometheusMetric::from_descriptor(&NOTIFICATION_TARGET_FAILED_MESSAGES_MD, 0.0)
                .with_label(NOTIFICATION_TARGET_ID_LABEL, target_id_label.clone())
                .with_label(NOTIFICATION_TARGET_TYPE_LABEL, target_type_label.clone()),
        );
        zero_metrics.push(
            PrometheusMetric::from_descriptor(&NOTIFICATION_TARGET_FAILED_STORE_LENGTH_MD, 0.0)
                .with_label(NOTIFICATION_TARGET_ID_LABEL, target_id_label.clone())
                .with_label(NOTIFICATION_TARGET_TYPE_LABEL, target_type_label.clone()),
        );
        zero_metrics.push(
            PrometheusMetric::from_descriptor(&NOTIFICATION_TARGET_QUEUE_LENGTH_MD, 0.0)
                .with_label(NOTIFICATION_TARGET_ID_LABEL, target_id_label.clone())
                .with_label(NOTIFICATION_TARGET_TYPE_LABEL, target_type_label.clone()),
        );
        zero_metrics.push(
            PrometheusMetric::from_descriptor(&NOTIFICATION_TARGET_TOTAL_MESSAGES_MD, 0.0)
                .with_label(NOTIFICATION_TARGET_ID_LABEL, target_id_label)
                .with_label(NOTIFICATION_TARGET_TYPE_LABEL, target_type_label),
        );
    }

    zero_metrics
}

fn collect_notification_target_zero_tombstone_metrics(
    zero_tombstones: &HashMap<NotificationTargetKey, u8>,
) -> Vec<PrometheusMetric> {
    if zero_tombstones.is_empty() {
        return Vec::new();
    }

    let mut zero_metrics = Vec::with_capacity(zero_tombstones.len() * 4);
    for (server, target_id, target_type) in zero_tombstones.keys() {
        let server_label: Cow<'static, str> = Cow::Owned(server.clone());
        let target_id_label: Cow<'static, str> = Cow::Owned(target_id.clone());
        let target_type_label: Cow<'static, str> = Cow::Owned(target_type.clone());
        zero_metrics.push(
            PrometheusMetric::from_descriptor(&NOTIFICATION_TARGET_FAILED_MESSAGES_BY_SERVER_MD, 0.0)
                .with_label(NOTIFICATION_SERVER_LABEL, server_label.clone())
                .with_label(NOTIFICATION_TARGET_ID_LABEL, target_id_label.clone())
                .with_label(NOTIFICATION_TARGET_TYPE_LABEL, target_type_label.clone()),
        );
        zero_metrics.push(
            PrometheusMetric::from_descriptor(&NOTIFICATION_TARGET_FAILED_STORE_LENGTH_BY_SERVER_MD, 0.0)
                .with_label(NOTIFICATION_SERVER_LABEL, server_label.clone())
                .with_label(NOTIFICATION_TARGET_ID_LABEL, target_id_label.clone())
                .with_label(NOTIFICATION_TARGET_TYPE_LABEL, target_type_label.clone()),
        );
        zero_metrics.push(
            PrometheusMetric::from_descriptor(&NOTIFICATION_TARGET_QUEUE_LENGTH_BY_SERVER_MD, 0.0)
                .with_label(NOTIFICATION_SERVER_LABEL, server_label.clone())
                .with_label(NOTIFICATION_TARGET_ID_LABEL, target_id_label.clone())
                .with_label(NOTIFICATION_TARGET_TYPE_LABEL, target_type_label.clone()),
        );
        zero_metrics.push(
            PrometheusMetric::from_descriptor(&NOTIFICATION_TARGET_TOTAL_MESSAGES_BY_SERVER_MD, 0.0)
                .with_label(NOTIFICATION_SERVER_LABEL, server_label)
                .with_label(NOTIFICATION_TARGET_ID_LABEL, target_id_label)
                .with_label(NOTIFICATION_TARGET_TYPE_LABEL, target_type_label),
        );
    }

    zero_metrics
}

fn retire_notification_legacy_target_metric_series(target_id: &str, target_type: &str) -> usize {
    let labels = [
        (NOTIFICATION_TARGET_ID_LABEL, Cow::Owned(target_id.to_string())),
        (NOTIFICATION_TARGET_TYPE_LABEL, Cow::Owned(target_type.to_string())),
    ];
    retire_metric_series(&NOTIFICATION_TARGET_FAILED_MESSAGES_MD.get_full_metric_name(), &labels)
        + retire_metric_series(&NOTIFICATION_TARGET_FAILED_STORE_LENGTH_MD.get_full_metric_name(), &labels)
        + retire_metric_series(&NOTIFICATION_TARGET_QUEUE_LENGTH_MD.get_full_metric_name(), &labels)
        + retire_metric_series(&NOTIFICATION_TARGET_TOTAL_MESSAGES_MD.get_full_metric_name(), &labels)
}

fn retire_notification_target_metric_series(server: &str, target_id: &str, target_type: &str) -> usize {
    let server_labels = [
        (NOTIFICATION_SERVER_LABEL, Cow::Owned(server.to_string())),
        (NOTIFICATION_TARGET_ID_LABEL, Cow::Owned(target_id.to_string())),
        (NOTIFICATION_TARGET_TYPE_LABEL, Cow::Owned(target_type.to_string())),
    ];
    retire_metric_series(&NOTIFICATION_TARGET_FAILED_MESSAGES_BY_SERVER_MD.get_full_metric_name(), &server_labels)
        + retire_metric_series(
            &NOTIFICATION_TARGET_FAILED_STORE_LENGTH_BY_SERVER_MD.get_full_metric_name(),
            &server_labels,
        )
        + retire_metric_series(&NOTIFICATION_TARGET_QUEUE_LENGTH_BY_SERVER_MD.get_full_metric_name(), &server_labels)
        + retire_metric_series(&NOTIFICATION_TARGET_TOTAL_MESSAGES_BY_SERVER_MD.get_full_metric_name(), &server_labels)
}

fn update_repl_bw_zero_tombstones(
    monitor_available: bool,
    has_seen_valid_snapshot: &mut bool,
    prev_live_keys: &mut HashSet<ReplBwKey>,
    zero_tombstones: &mut HashMap<ReplBwKey, u8>,
    current_live_keys: HashSet<ReplBwKey>,
    tombstone_cycles: u8,
) {
    if !monitor_available {
        return;
    }

    if *has_seen_valid_snapshot {
        for removed in prev_live_keys.difference(&current_live_keys) {
            zero_tombstones.insert(removed.clone(), tombstone_cycles);
        }
    }

    // Key becomes live again: stop zeroing immediately.
    for key in &current_live_keys {
        zero_tombstones.remove(key);
    }

    *prev_live_keys = current_live_keys;
    *has_seen_valid_snapshot = true;
}

fn update_repl_backlog_zero_tombstones(
    monitor_available: bool,
    has_seen_valid_snapshot: &mut bool,
    prev_live_keys: &mut HashSet<BucketKey>,
    zero_tombstones: &mut HashMap<BucketKey, u8>,
    current_live_keys: HashSet<BucketKey>,
    tombstone_cycles: u8,
) {
    if !monitor_available {
        for key in &current_live_keys {
            zero_tombstones.remove(key);
        }
        return;
    }

    update_series_zero_tombstones(
        has_seen_valid_snapshot,
        prev_live_keys,
        zero_tombstones,
        current_live_keys,
        tombstone_cycles,
    );
}

fn collect_repl_bw_zero_tombstone_metrics(zero_tombstones: &HashMap<ReplBwKey, u8>) -> Vec<PrometheusMetric> {
    if zero_tombstones.is_empty() {
        return Vec::new();
    }

    let mut zero_metrics = Vec::with_capacity(zero_tombstones.len() * 2);
    for (bucket, target_arn) in zero_tombstones.keys() {
        let bucket_label: Cow<'static, str> = Cow::Owned(bucket.clone());
        let target_arn_label: Cow<'static, str> = Cow::Owned(target_arn.clone());

        zero_metrics.push(
            PrometheusMetric::from_descriptor(&BUCKET_REPL_BANDWIDTH_LIMIT_MD, 0.0)
                .with_label(BUCKET_L, bucket_label.clone())
                .with_label(TARGET_ARN_L, target_arn_label.clone()),
        );

        zero_metrics.push(
            PrometheusMetric::from_descriptor(&BUCKET_REPL_BANDWIDTH_CURRENT_MD, 0.0)
                .with_label(BUCKET_L, bucket_label)
                .with_label(TARGET_ARN_L, target_arn_label),
        );
    }

    zero_metrics
}

fn collect_repl_backlog_zero_tombstone_metrics(zero_tombstones: &HashMap<BucketKey, u8>) -> Vec<PrometheusMetric> {
    if zero_tombstones.is_empty() {
        return Vec::new();
    }

    let stats = zero_tombstones
        .keys()
        .map(|bucket| BucketReplicationBacklogStats {
            bucket: bucket.clone(),
            ..Default::default()
        })
        .collect::<Vec<_>>();
    collect_bucket_replication_backlog_metrics(&stats)
}

fn collect_repl_backlog_target_zero_tombstone_metrics(zero_tombstones: &HashMap<ReplBwKey, u8>) -> Vec<PrometheusMetric> {
    if zero_tombstones.is_empty() {
        return Vec::new();
    }

    let mut zero_metrics = Vec::with_capacity(zero_tombstones.len() * 4);
    for (bucket, target_arn) in zero_tombstones.keys() {
        let bucket_label: Cow<'static, str> = Cow::Owned(bucket.clone());
        let target_arn_label: Cow<'static, str> = Cow::Owned(target_arn.clone());
        zero_metrics.push(
            PrometheusMetric::from_descriptor(&BUCKET_REPL_CURRENT_TARGET_BACKLOG_COUNT_MD, 0.0)
                .with_label(BUCKET_L, bucket_label.clone())
                .with_label(TARGET_ARN_L, target_arn_label.clone()),
        );
        zero_metrics.push(
            PrometheusMetric::from_descriptor(&BUCKET_REPL_CURRENT_TARGET_BACKLOG_BYTES_MD, 0.0)
                .with_label(BUCKET_L, bucket_label.clone())
                .with_label(TARGET_ARN_L, target_arn_label.clone()),
        );
        zero_metrics.push(
            PrometheusMetric::from_descriptor(&BUCKET_REPL_DURABLE_MRF_TARGET_BACKLOG_COUNT_MD, 0.0)
                .with_label(BUCKET_L, bucket_label.clone())
                .with_label(TARGET_ARN_L, target_arn_label.clone()),
        );
        zero_metrics.push(
            PrometheusMetric::from_descriptor(&BUCKET_REPL_DURABLE_MRF_TARGET_BACKLOG_BYTES_MD, 0.0)
                .with_label(BUCKET_L, bucket_label)
                .with_label(TARGET_ARN_L, target_arn_label),
        );
    }

    zero_metrics
}

fn collect_repl_flow_zero_tombstone_metrics(zero_tombstones: &HashMap<ReplBwKey, u8>) -> Vec<PrometheusMetric> {
    if zero_tombstones.is_empty() {
        return Vec::new();
    }

    let mut zero_metrics = Vec::with_capacity(zero_tombstones.len());
    for (bucket, target_arn) in zero_tombstones.keys() {
        let bucket_label: Cow<'static, str> = Cow::Owned(bucket.clone());
        let target_arn_label: Cow<'static, str> = Cow::Owned(target_arn.clone());
        zero_metrics.push(
            PrometheusMetric::from_descriptor(&BUCKET_REPL_LATENCY_MS_MD, 0.0)
                .with_label(BUCKET_L, bucket_label)
                .with_label(OPERATION_L, Cow::Borrowed("object_replication"))
                .with_label(RANGE_L, Cow::Borrowed("all"))
                .with_label(TARGET_ARN_L, target_arn_label),
        );
    }

    zero_metrics
}

fn retire_repl_bw_metric_series(bucket: &str, target_arn: &str) -> usize {
    let labels = [
        (BUCKET_L, Cow::Owned(bucket.to_string())),
        (TARGET_ARN_L, Cow::Owned(target_arn.to_string())),
    ];
    retire_metric_series(&BUCKET_REPL_BANDWIDTH_LIMIT_MD.get_full_metric_name(), &labels)
        + retire_metric_series(&BUCKET_REPL_BANDWIDTH_CURRENT_MD.get_full_metric_name(), &labels)
}

fn retire_repl_flow_metric_series(bucket: &str, target_arn: &str) -> usize {
    let labels = [
        (BUCKET_L, Cow::Owned(bucket.to_string())),
        (TARGET_ARN_L, Cow::Owned(target_arn.to_string())),
    ];
    let latency_labels = [
        (BUCKET_L, Cow::Owned(bucket.to_string())),
        (OPERATION_L, Cow::Borrowed("object_replication")),
        (RANGE_L, Cow::Borrowed("all")),
        (TARGET_ARN_L, Cow::Owned(target_arn.to_string())),
    ];
    [
        BUCKET_REPL_TARGET_SENT_BYTES_MD.get_full_metric_name(),
        BUCKET_REPL_TARGET_SENT_COUNT_MD.get_full_metric_name(),
        BUCKET_REPL_TARGET_TOTAL_FAILED_BYTES_MD.get_full_metric_name(),
        BUCKET_REPL_TARGET_TOTAL_FAILED_COUNT_MD.get_full_metric_name(),
        BUCKET_REPL_TARGET_LAST_MIN_FAILED_BYTES_MD.get_full_metric_name(),
        BUCKET_REPL_TARGET_LAST_MIN_FAILED_COUNT_MD.get_full_metric_name(),
        BUCKET_REPL_TARGET_LAST_HOUR_FAILED_BYTES_MD.get_full_metric_name(),
        BUCKET_REPL_TARGET_LAST_HOUR_FAILED_COUNT_MD.get_full_metric_name(),
    ]
    .iter()
    .map(|name| retire_metric_series(name, &labels))
    .sum::<usize>()
        + retire_metric_series(&BUCKET_REPL_LATENCY_MS_MD.get_full_metric_name(), &latency_labels)
}

fn retire_repl_backlog_metric_series(bucket: &str) -> usize {
    let labels = [(BUCKET_L, Cow::Owned(bucket.to_string()))];
    [
        BUCKET_REPL_CURRENT_BACKLOG_COUNT_MD.get_full_metric_name(),
        BUCKET_REPL_CURRENT_BACKLOG_BYTES_MD.get_full_metric_name(),
        BUCKET_REPL_DURABLE_MRF_AVAILABLE_MD.get_full_metric_name(),
        BUCKET_REPL_DURABLE_MRF_BACKLOG_COUNT_MD.get_full_metric_name(),
        BUCKET_REPL_DURABLE_MRF_BACKLOG_BYTES_MD.get_full_metric_name(),
        BUCKET_REPL_MRF_PENDING_COUNT_MD.get_full_metric_name(),
        BUCKET_REPL_MRF_PENDING_BYTES_MD.get_full_metric_name(),
        BUCKET_REPL_MRF_DROPPED_COUNT_MD.get_full_metric_name(),
        BUCKET_REPL_MRF_MISSED_COUNT_MD.get_full_metric_name(),
        BUCKET_REPL_MRF_FLUSH_FAILURES_MD.get_full_metric_name(),
        BUCKET_REPL_MRF_LAST_FLUSH_DURATION_MILLIS_MD.get_full_metric_name(),
    ]
    .iter()
    .map(|name| retire_metric_series(name, &labels))
    .sum()
}

fn retire_bucket_replication_proxy_request_metric_series(bucket: &str) -> usize {
    const PROXY_OPERATIONS: [&str; 6] = ["get", "head", "put", "put_tagging", "get_tagging", "delete_tagging"];
    let mut retired = 0;
    for operation in PROXY_OPERATIONS {
        for result in ["success", "failure"] {
            let labels = [
                (BUCKET_L, Cow::Owned(bucket.to_string())),
                (OPERATION_L, Cow::Borrowed(operation)),
                (RESULT_L, Cow::Borrowed(result)),
            ];
            retired += retire_metric_series(&BUCKET_REPL_PROXY_REQUESTS_TOTAL_MD.get_full_metric_name(), &labels);
        }
    }
    retired
}

fn retire_repl_backlog_target_metric_series(bucket: &str, target_arn: &str) -> usize {
    let labels = [
        (BUCKET_L, Cow::Owned(bucket.to_string())),
        (TARGET_ARN_L, Cow::Owned(target_arn.to_string())),
    ];
    retire_metric_series(&BUCKET_REPL_CURRENT_TARGET_BACKLOG_COUNT_MD.get_full_metric_name(), &labels)
        + retire_metric_series(&BUCKET_REPL_CURRENT_TARGET_BACKLOG_BYTES_MD.get_full_metric_name(), &labels)
        + retire_metric_series(&BUCKET_REPL_DURABLE_MRF_TARGET_BACKLOG_COUNT_MD.get_full_metric_name(), &labels)
        + retire_metric_series(&BUCKET_REPL_DURABLE_MRF_TARGET_BACKLOG_BYTES_MD.get_full_metric_name(), &labels)
}

fn expire_repl_bw_zero_tombstones(monitor_available: bool, zero_tombstones: &mut HashMap<ReplBwKey, u8>) -> Vec<ReplBwKey> {
    if !monitor_available {
        return Vec::new();
    }

    expire_series_zero_tombstones(zero_tombstones)
}

fn expire_repl_backlog_zero_tombstones(monitor_available: bool, zero_tombstones: &mut HashMap<BucketKey, u8>) -> Vec<BucketKey> {
    if !monitor_available {
        return Vec::new();
    }

    expire_series_zero_tombstones(zero_tombstones)
}

fn expire_repl_backlog_target_zero_tombstones(
    target_metrics_available: bool,
    zero_tombstones: &mut HashMap<ReplBwKey, u8>,
) -> Vec<ReplBwKey> {
    if !target_metrics_available {
        return Vec::new();
    }

    expire_series_zero_tombstones(zero_tombstones)
}

/// Initialize all metrics collectors.
///
/// This function spawns background tasks that periodically collect metrics
/// from various sources and report them to the metrics system.
///
/// # Arguments
/// * `token` - A `CancellationToken` that can be used to gracefully shut down
///   all metrics collection tasks.
///
/// # Environment Variables
/// The collection intervals can be configured via environment variables:
/// - `RUSTFS_METRICS_CLUSTER_INTERVAL_SEC`: Cluster metrics interval in seconds (default: 60)
/// - `RUSTFS_METRICS_BUCKET_INTERVAL_SEC`: Bucket metrics interval in seconds (default: 300)
/// - `RUSTFS_METRICS_NODE_INTERVAL_SEC`: Node/disk metrics interval in seconds (default: 60)
/// - `RUSTFS_METRICS_BUCKET_REPLICATION_BANDWIDTH_INTERVAL_SEC`: Bucket replication bandwidth interval in seconds (default: 30)
/// - `RUSTFS_METRICS_RESOURCE_INTERVAL_SEC`: Resource metrics interval in seconds (default: 15)
/// - `RUSTFS_METRICS_DEFAULT_INTERVAL_SEC`: Optional global default interval in seconds.
///
/// Legacy interval names without `_SEC` are still accepted for backward compatibility:
/// - `RUSTFS_METRICS_CLUSTER_INTERVAL`
/// - `RUSTFS_METRICS_BUCKET_INTERVAL`
/// - `RUSTFS_METRICS_NODE_INTERVAL`
/// - `RUSTFS_METRICS_BUCKET_REPLICATION_BANDWIDTH_INTERVAL`
/// - `RUSTFS_METRICS_RESOURCE_INTERVAL`
pub fn init_metrics_runtime(token: CancellationToken) {
    let config = configured_metrics_runtime_config();
    let health = metrics_runtime_collector_health();
    let cluster_interval = config.cluster_interval;
    let bucket_interval = config.bucket_interval;
    let bucket_replication_bandwidth_interval = config.bucket_replication_bandwidth_interval;
    let node_interval = config.node_interval;
    let resource_interval = config.resource_interval;
    let audit_interval = config.audit_interval;
    let notification_interval = config.notification_interval;
    let compression_enabled = obs_is_disk_compression_enabled();

    // Spawn task for cluster metrics
    let token_clone = token.clone();
    tokio::spawn(async move {
        let mut interval = metrics_interval(cluster_interval, Duration::ZERO);
        let mut objects_count_was_authoritative = false;
        let mut buckets_count_was_authoritative = false;
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    run_metrics_collector_tick(health, MetricsCollectorTaskId::ClusterStats, "cluster_stats", async {
                        let (stats, cluster_health) = collect_cluster_and_health_stats().await;
                        if objects_count_was_authoritative && stats.objects_count.is_none() {
                            let labels: [(&'static str, Cow<'static, str>); 0] = [];
                            let _ = retire_metric_series(&CLUSTER_OBJECTS_TOTAL_MD.get_full_metric_name(), &labels);
                        }
                        if buckets_count_was_authoritative && stats.buckets_count.is_none() {
                            let labels: [(&'static str, Cow<'static, str>); 0] = [];
                            let _ = retire_metric_series(&CLUSTER_BUCKETS_TOTAL_MD.get_full_metric_name(), &labels);
                        }
                        objects_count_was_authoritative = stats.objects_count.is_some();
                        buckets_count_was_authoritative = stats.buckets_count.is_some();
                        let mut metrics = collect_cluster_metrics(&stats);
                        metrics.extend(collect_cluster_health_metrics(&cluster_health));
                        report_metrics(&metrics);
                    }).await;
                }
                _ = token_clone.cancelled() => {
                    warn!(event = EVENT_METRICS_RUNTIME_STATE, component = LOG_COMPONENT_OBS, subsystem = LOG_SUBSYSTEM_METRICS_RUNTIME, collector = "cluster_stats", state = "cancelled", "metrics runtime state changed");
                    return;
                }
            }
        }
    });

    // Spawn task for supplementary cluster metrics that are defined in schema/collector
    // but filled by later task-specific runtime sources.
    let token_clone = token.clone();
    tokio::spawn(async move {
        let mut interval = metrics_interval(cluster_interval, stagger_duration(cluster_interval, 1, 3));
        let tombstone_cycles = config.replication_bandwidth_zero_tombstone_cycles;
        let mut has_seen_bucket_usage_snapshot = false;
        let mut prev_bucket_usage_keys: HashSet<BucketKey> = HashSet::new();
        let mut bucket_usage_zero_tombstones: HashMap<BucketKey, u8> = HashMap::new();
        let mut prev_bucket_usage_object_size_keys: HashSet<BucketRangeKey> = HashSet::new();
        let mut bucket_usage_object_size_zero_tombstones: HashMap<BucketRangeKey, u8> = HashMap::new();
        let mut prev_bucket_usage_version_keys: HashSet<BucketRangeKey> = HashSet::new();
        let mut bucket_usage_version_zero_tombstones: HashMap<BucketRangeKey, u8> = HashMap::new();
        let mut prev_cluster_usage_object_size_keys: HashSet<String> = HashSet::new();
        let mut prev_cluster_usage_version_keys: HashSet<String> = HashSet::new();
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    run_metrics_collector_tick(
                        health,
                        MetricsCollectorTaskId::SupplementaryClusterStats,
                        "supplementary_cluster_stats",
                        async {
                            let mut metrics = Vec::new();

                            if let Some(stats) = collect_cluster_config_stats().await {
                                metrics.extend(collect_cluster_config_metrics(&stats));
                            }

                            let erasure_sets = collect_erasure_set_stats().await;
                            if !erasure_sets.is_empty() {
                                metrics.extend(collect_erasure_set_metrics(&erasure_sets));
                            }

                            if let Some(stats) = collect_iam_stats().await {
                                metrics.extend(collect_iam_metrics(&stats));
                            }

                            if let Some((cluster_usage, bucket_usage)) = collect_cluster_usage_metric_stats().await {
                                let current_cluster_usage_object_size_keys = cluster_usage
                                    .object_size_distribution
                                    .iter()
                                    .map(|(range, _)| range.clone())
                                    .collect::<HashSet<_>>();
                                for range in prev_cluster_usage_object_size_keys.difference(&current_cluster_usage_object_size_keys) {
                                    let _ = retire_cluster_usage_distribution_series(
                                        USAGE_OBJECTS_DISTRIBUTION_MD.get_full_metric_name(),
                                        range,
                                    );
                                }
                                prev_cluster_usage_object_size_keys = current_cluster_usage_object_size_keys;
                                let current_cluster_usage_version_keys = cluster_usage
                                    .versions_distribution
                                    .iter()
                                    .map(|(range, _)| range.clone())
                                    .collect::<HashSet<_>>();
                                for range in prev_cluster_usage_version_keys.difference(&current_cluster_usage_version_keys) {
                                    let _ = retire_cluster_usage_distribution_series(
                                        USAGE_VERSIONS_DISTRIBUTION_MD.get_full_metric_name(),
                                        range,
                                    );
                                }
                                prev_cluster_usage_version_keys = current_cluster_usage_version_keys;
                                metrics.extend(collect_cluster_usage_metrics(&cluster_usage));
                                update_series_zero_tombstones(
                                    &mut has_seen_bucket_usage_snapshot,
                                    &mut prev_bucket_usage_keys,
                                    &mut bucket_usage_zero_tombstones,
                                    bucket_usage_live_keys(&bucket_usage),
                                    tombstone_cycles,
                                );
                                update_series_zero_tombstones(
                                    &mut has_seen_bucket_usage_snapshot,
                                    &mut prev_bucket_usage_object_size_keys,
                                    &mut bucket_usage_object_size_zero_tombstones,
                                    bucket_usage_object_size_live_keys(&bucket_usage),
                                    tombstone_cycles,
                                );
                                update_series_zero_tombstones(
                                    &mut has_seen_bucket_usage_snapshot,
                                    &mut prev_bucket_usage_version_keys,
                                    &mut bucket_usage_version_zero_tombstones,
                                    bucket_usage_version_live_keys(&bucket_usage),
                                    tombstone_cycles,
                                );
                                metrics.extend(collect_bucket_usage_metrics(&bucket_usage));
                                metrics.extend(collect_bucket_usage_zero_tombstone_metrics(
                                    &bucket_usage_zero_tombstones,
                                    &bucket_usage_object_size_zero_tombstones,
                                    &bucket_usage_version_zero_tombstones,
                                ));
                                for bucket in expire_series_zero_tombstones(&mut bucket_usage_zero_tombstones) {
                                    let _ = retire_bucket_usage_metric_series(&bucket);
                                }
                                for (bucket, range) in expire_series_zero_tombstones(&mut bucket_usage_object_size_zero_tombstones) {
                                    let _ = retire_bucket_usage_distribution_series(
                                        USAGE_BUCKET_OBJECT_SIZE_DISTRIBUTION_MD.get_full_metric_name(),
                                        &bucket,
                                        &range,
                                    );
                                }
                                for (bucket, range) in expire_series_zero_tombstones(&mut bucket_usage_version_zero_tombstones) {
                                    let _ = retire_bucket_usage_distribution_series(
                                        USAGE_BUCKET_OBJECT_VERSION_COUNT_DISTRIBUTION_MD.get_full_metric_name(),
                                        &bucket,
                                        &range,
                                    );
                                }
                            } else if has_seen_bucket_usage_snapshot {
                                let _ = retire_cluster_usage_metric_series();
                                for range in prev_cluster_usage_object_size_keys.drain() {
                                    let _ = retire_cluster_usage_distribution_series(
                                        USAGE_OBJECTS_DISTRIBUTION_MD.get_full_metric_name(),
                                        &range,
                                    );
                                }
                                for range in prev_cluster_usage_version_keys.drain() {
                                    let _ = retire_cluster_usage_distribution_series(
                                        USAGE_VERSIONS_DISTRIBUTION_MD.get_full_metric_name(),
                                        &range,
                                    );
                                }
                                for bucket in prev_bucket_usage_keys.drain() {
                                    let _ = retire_bucket_usage_metric_series(&bucket);
                                }
                                for (bucket, range) in prev_bucket_usage_object_size_keys.drain() {
                                    let _ = retire_bucket_usage_distribution_series(
                                        USAGE_BUCKET_OBJECT_SIZE_DISTRIBUTION_MD.get_full_metric_name(),
                                        &bucket,
                                        &range,
                                    );
                                }
                                for (bucket, range) in prev_bucket_usage_version_keys.drain() {
                                    let _ = retire_bucket_usage_distribution_series(
                                        USAGE_BUCKET_OBJECT_VERSION_COUNT_DISTRIBUTION_MD.get_full_metric_name(),
                                        &bucket,
                                        &range,
                                    );
                                }
                                bucket_usage_zero_tombstones.clear();
                                bucket_usage_object_size_zero_tombstones.clear();
                                bucket_usage_version_zero_tombstones.clear();
                                has_seen_bucket_usage_snapshot = false;
                            }

                            if !metrics.is_empty() {
                                report_metrics(&metrics);
                            }
                        },
                    ).await;
                }
                _ = token_clone.cancelled() => {
                    warn!(event = EVENT_METRICS_RUNTIME_STATE, component = LOG_COMPONENT_OBS, subsystem = LOG_SUBSYSTEM_METRICS_RUNTIME, collector = "supplementary_cluster_stats", state = "cancelled", "metrics runtime state changed");
                    return;
                }
            }
        }
    });

    // Spawn task for bucket metrics
    let token_clone = token.clone();
    tokio::spawn(async move {
        let mut interval = metrics_interval(bucket_interval, Duration::ZERO);
        let tombstone_cycles = config.replication_bandwidth_zero_tombstone_cycles;
        let mut series_state = BucketSeriesState::default();
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    run_metrics_collector_tick(health, MetricsCollectorTaskId::BucketStats, "bucket_stats", async {
                        let stats = collect_bucket_stats().await;
                        let Some(update) = series_state.observe(stats.as_deref(), tombstone_cycles) else {
                            return;
                        };
                        for bucket in update.retire_observations {
                            let _ = retire_bucket_observation_metric_series(&bucket);
                        }
                        report_metrics(&update.metrics);
                        for bucket in update.retire_buckets {
                            let _ = retire_bucket_metric_series(&bucket);
                        }
                    }).await;
                }
                _ = token_clone.cancelled() => {
                    warn!(event = EVENT_METRICS_RUNTIME_STATE, component = LOG_COMPONENT_OBS, subsystem = LOG_SUBSYSTEM_METRICS_RUNTIME, collector = "bucket_stats", state = "cancelled", "metrics runtime state changed");
                    return;
                }
            }
        }
    });

    // Spawn task for node/disk metrics
    let token_clone = token.clone();
    tokio::spawn(async move {
        let mut interval = metrics_interval(node_interval, Duration::ZERO);
        let mut prev_drive_basic_keys: HashSet<DriveBasicKey> = HashSet::new();
        let mut prev_drive_info_keys: HashSet<DriveInfoKey> = HashSet::new();
        let mut prev_drive_topology_keys: HashSet<DriveTopologyKey> = HashSet::new();
        let mut prev_drive_topology_api_keys: HashSet<DriveTopologyApiKey> = HashSet::new();
        let mut has_seen_drive_info_snapshot = false;
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    run_metrics_collector_tick(health, MetricsCollectorTaskId::NodeDiskStats, "node_disk_stats", async {
                        let (disk_stats, drive_stats, drive_counts) = collect_disk_and_system_drive_runtime_stats().await;
                        let current_drive_info_keys = drive_info_live_keys(&drive_stats);
                        let current_drive_basic_keys = drive_basic_live_keys(&drive_stats);
                        let current_drive_topology_keys = drive_topology_live_keys(&drive_stats);
                        let current_drive_topology_api_keys = drive_topology_api_live_keys(&drive_stats);
                        let retire_drive_info_keys = if has_seen_drive_info_snapshot {
                            prev_drive_info_keys.difference(&current_drive_info_keys).cloned().collect::<Vec<_>>()
                        } else {
                            Vec::new()
                        };
                        let retire_drive_basic_keys = if has_seen_drive_info_snapshot {
                            prev_drive_basic_keys.difference(&current_drive_basic_keys).cloned().collect::<Vec<_>>()
                        } else {
                            Vec::new()
                        };
                        let retire_drive_topology_keys = if has_seen_drive_info_snapshot {
                            prev_drive_topology_keys.difference(&current_drive_topology_keys).cloned().collect::<Vec<_>>()
                        } else {
                            Vec::new()
                        };
                        let retire_drive_topology_api_keys = if has_seen_drive_info_snapshot {
                            prev_drive_topology_api_keys
                                .difference(&current_drive_topology_api_keys)
                                .cloned()
                                .collect::<Vec<_>>()
                        } else {
                            Vec::new()
                        };
                        prev_drive_info_keys = current_drive_info_keys;
                        prev_drive_basic_keys = current_drive_basic_keys;
                        prev_drive_topology_keys = current_drive_topology_keys;
                        prev_drive_topology_api_keys = current_drive_topology_api_keys;
                        has_seen_drive_info_snapshot = true;
                        let mut metrics = collect_node_metrics(&disk_stats);
                        metrics.extend(collect_drive_runtime_detailed_metrics(&drive_stats));
                        metrics.extend(collect_drive_count_metrics(&drive_counts));
                        report_metrics(&metrics);
                        for key in retire_drive_info_keys {
                            let _ = retire_drive_info_metric_series(&key);
                        }
                        for key in retire_drive_basic_keys {
                            let _ = retire_drive_basic_metric_series(&key);
                        }
                        for key in retire_drive_topology_keys {
                            let _ = retire_drive_topology_metric_series(&key);
                        }
                        for key in retire_drive_topology_api_keys {
                            let _ = retire_drive_topology_api_metric_series(&key);
                        }
                    }).await;
                }
                _ = token_clone.cancelled() => {
                    warn!(event = EVENT_METRICS_RUNTIME_STATE, component = LOG_COMPONENT_OBS, subsystem = LOG_SUBSYSTEM_METRICS_RUNTIME, collector = "node_disk_stats", state = "cancelled", "metrics runtime state changed");
                    return;
                }
            }
        }
    });

    // Spawn task for bucket replication bandwidth metrics
    let token_clone = token.clone();
    tokio::spawn(async move {
        let mut interval = metrics_interval(bucket_replication_bandwidth_interval, Duration::ZERO);
        let repl_bw_zero_tombstone_cycles = config.replication_bandwidth_zero_tombstone_cycles;
        let mut prev_live_keys: HashSet<ReplBwKey> = HashSet::new();
        let mut zero_tombstones: HashMap<ReplBwKey, u8> = HashMap::new();
        let mut has_seen_valid_snapshot = false;
        let mut prev_backlog_live_keys: HashSet<BucketKey> = HashSet::new();
        let mut backlog_zero_tombstones: HashMap<BucketKey, u8> = HashMap::new();
        let mut has_seen_valid_backlog_snapshot = false;
        let mut prev_backlog_target_live_keys: HashSet<ReplBwKey> = HashSet::new();
        let mut backlog_target_zero_tombstones: HashMap<ReplBwKey, u8> = HashMap::new();
        let mut has_seen_valid_backlog_target_snapshot = false;
        let mut prev_flow_live_keys: HashSet<ReplBwKey> = HashSet::new();
        let mut flow_zero_tombstones: HashMap<ReplBwKey, u8> = HashMap::new();
        let mut has_seen_valid_flow_snapshot = false;
        let mut prev_proxy_bucket_live_keys: HashSet<BucketKey> = HashSet::new();
        let mut has_seen_proxy_bucket_snapshot = false;
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    run_metrics_collector_tick(
                        health,
                        MetricsCollectorTaskId::BucketReplicationBandwidth,
                        "bucket_replication_bandwidth",
                        async {
                            let monitor_available = bucket_monitor_available();
                            let stats = collect_bucket_replication_bandwidth_stats();

                            let current_live_keys = repl_bw_live_keys(&stats);

                            if !monitor_available {
                                warn!(event = EVENT_METRICS_RUNTIME_STATE, component = LOG_COMPONENT_OBS, subsystem = LOG_SUBSYSTEM_METRICS_RUNTIME, collector = "bucket_replication_bandwidth", result = "bucket_monitor_unavailable", "metrics runtime state changed");
                            }
                            update_repl_bw_zero_tombstones(
                                monitor_available,
                                &mut has_seen_valid_snapshot,
                                &mut prev_live_keys,
                                &mut zero_tombstones,
                                current_live_keys,
                                repl_bw_zero_tombstone_cycles,
                            );
                            let mut metrics = collect_bucket_replication_bandwidth_metrics(&stats);

                            // Phase-1 action: force zero for removed keys during tombstone cycles.
                            metrics.extend(collect_repl_bw_zero_tombstone_metrics(&zero_tombstones));

                            let (bucket_replication, bucket_replication_backlog) = collect_bucket_replication_stats_bundle().await;
                            let current_proxy_bucket_live_keys = repl_proxy_bucket_live_keys(&bucket_replication);
                            let retire_proxy_buckets = if has_seen_proxy_bucket_snapshot {
                                prev_proxy_bucket_live_keys
                                    .difference(&current_proxy_bucket_live_keys)
                                    .cloned()
                                    .collect::<Vec<_>>()
                            } else {
                                Vec::new()
                            };
                            prev_proxy_bucket_live_keys = current_proxy_bucket_live_keys;
                            has_seen_proxy_bucket_snapshot = true;
                            let durable_mrf_available = bucket_replication_backlog.iter().any(|stat| stat.durable_mrf_available);
                            let backlog_target_metrics_available =
                                durable_mrf_available || monitor_available || !bucket_replication_backlog.is_empty();
                            update_repl_backlog_zero_tombstones(
                                monitor_available,
                                &mut has_seen_valid_backlog_snapshot,
                                &mut prev_backlog_live_keys,
                                &mut backlog_zero_tombstones,
                                repl_backlog_live_keys(&bucket_replication_backlog),
                                repl_bw_zero_tombstone_cycles,
                            );
                            if backlog_target_metrics_available {
                                update_series_zero_tombstones(
                                    &mut has_seen_valid_backlog_target_snapshot,
                                    &mut prev_backlog_target_live_keys,
                                    &mut backlog_target_zero_tombstones,
                                    repl_backlog_target_live_keys(&bucket_replication_backlog),
                                    repl_bw_zero_tombstone_cycles,
                                );
                            }
                            metrics.extend(collect_bucket_replication_runtime_metrics(&bucket_replication));
                            update_series_zero_tombstones(
                                &mut has_seen_valid_flow_snapshot,
                                &mut prev_flow_live_keys,
                                &mut flow_zero_tombstones,
                                repl_flow_live_keys(&bucket_replication),
                                repl_bw_zero_tombstone_cycles,
                            );
                            metrics.extend(collect_bucket_replication_backlog_metrics(&bucket_replication_backlog));
                            metrics.extend(collect_repl_backlog_zero_tombstone_metrics(&backlog_zero_tombstones));
                            metrics.extend(collect_repl_backlog_target_zero_tombstone_metrics(&backlog_target_zero_tombstones));
                            metrics.extend(collect_repl_flow_zero_tombstone_metrics(&flow_zero_tombstones));
                            let replication = collect_replication_stats().await;
                            metrics.extend(collect_replication_runtime_metrics(&ReplicationRuntimeStats {
                                server: current_local_node_identity(),
                                stats: replication,
                            }));
                            report_metrics(&metrics);

                            // Phase-2: after N cycles, stop reporting -> series becomes absent after expiration.
                            for (bucket, target_arn) in expire_repl_bw_zero_tombstones(monitor_available, &mut zero_tombstones) {
                                let _ = retire_repl_bw_metric_series(&bucket, &target_arn);
                            }
                            for bucket in expire_repl_backlog_zero_tombstones(monitor_available, &mut backlog_zero_tombstones) {
                                let _ = retire_repl_backlog_metric_series(&bucket);
                            }
                            for (bucket, target_arn) in expire_repl_backlog_target_zero_tombstones(
                                backlog_target_metrics_available,
                                &mut backlog_target_zero_tombstones,
                            ) {
                                let _ = retire_repl_backlog_target_metric_series(&bucket, &target_arn);
                            }
                            for (bucket, target_arn) in expire_series_zero_tombstones(&mut flow_zero_tombstones) {
                                let _ = retire_repl_flow_metric_series(&bucket, &target_arn);
                            }
                            for bucket in retire_proxy_buckets {
                                let _ = retire_bucket_replication_proxy_request_metric_series(&bucket);
                            }
                        },
                    ).await;
                }
                _ = token_clone.cancelled() => {
                    warn!(event = EVENT_METRICS_RUNTIME_STATE, component = LOG_COMPONENT_OBS, subsystem = LOG_SUBSYSTEM_METRICS_RUNTIME, collector = "bucket_replication_bandwidth", state = "cancelled", "metrics runtime state changed");
                    return;
                }
            }
        }
    });

    // Spawn task for audit target delivery metrics
    let token_clone = token.clone();
    tokio::spawn(async move {
        let mut interval = metrics_interval(audit_interval, Duration::ZERO);
        let tombstone_cycles = config.replication_bandwidth_zero_tombstone_cycles;
        let mut has_seen_audit_snapshot = false;
        let mut prev_audit_target_keys: HashSet<AuditTargetKey> = HashSet::new();
        let mut audit_zero_tombstones: HashMap<AuditTargetKey, u8> = HashMap::new();
        let mut has_seen_audit_legacy_snapshot = false;
        let mut prev_audit_legacy_target_keys: HashSet<AuditLegacyTargetKey> = HashSet::new();
        let mut audit_legacy_zero_tombstones: HashMap<AuditLegacyTargetKey, u8> = HashMap::new();
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    run_metrics_collector_tick(health, MetricsCollectorTaskId::AuditTargetStats, "audit_target_stats", async {
                        let server = current_local_node_identity();
                        let stats = audit_target_metrics().await
                            .into_iter()
                            .map(|snapshot| AuditTargetRuntimeStats {
                                server: server.clone(),
                                target: AuditTargetStats {
                                    failed_messages: snapshot.failed_messages,
                                    failed_store_length: snapshot.failed_store_length,
                                    queue_length: snapshot.queue_length,
                                    target_id: snapshot.target_id,
                                    total_messages: snapshot.total_messages,
                                },
                            })
                            .collect::<Vec<_>>();
                        update_series_zero_tombstones(
                            &mut has_seen_audit_snapshot,
                            &mut prev_audit_target_keys,
                            &mut audit_zero_tombstones,
                            audit_target_live_keys(&stats),
                            tombstone_cycles,
                        );
                        update_series_zero_tombstones(
                            &mut has_seen_audit_legacy_snapshot,
                            &mut prev_audit_legacy_target_keys,
                            &mut audit_legacy_zero_tombstones,
                            audit_legacy_target_live_keys(&stats),
                            tombstone_cycles,
                        );
                        let mut metrics = collect_audit_runtime_metrics(&stats);
                        metrics.extend(collect_audit_legacy_zero_tombstone_metrics(&audit_legacy_zero_tombstones));
                        metrics.extend(collect_audit_zero_tombstone_metrics(&audit_zero_tombstones));
                        report_metrics(&metrics);
                        for target_id in expire_series_zero_tombstones(&mut audit_legacy_zero_tombstones) {
                            let _ = retire_audit_legacy_target_metric_series(&target_id);
                        }
                        for (server, target_id) in expire_series_zero_tombstones(&mut audit_zero_tombstones) {
                            let _ = retire_audit_target_metric_series(&server, &target_id);
                        }
                    }).await;
                }
                _ = token_clone.cancelled() => {
                    warn!(event = EVENT_METRICS_RUNTIME_STATE, component = LOG_COMPONENT_OBS, subsystem = LOG_SUBSYSTEM_METRICS_RUNTIME, collector = "audit_target_stats", state = "cancelled", "metrics runtime state changed");
                    return;
                }
            }
        }
    });

    // Spawn task for notification delivery metrics
    let token_clone = token.clone();
    tokio::spawn(async move {
        let mut interval = metrics_interval(notification_interval, Duration::ZERO);
        let tombstone_cycles = config.replication_bandwidth_zero_tombstone_cycles;
        let mut has_seen_notification_target_snapshot = false;
        let mut prev_notification_target_keys: HashSet<NotificationTargetKey> = HashSet::new();
        let mut notification_target_zero_tombstones: HashMap<NotificationTargetKey, u8> = HashMap::new();
        let mut has_seen_notification_legacy_target_snapshot = false;
        let mut prev_notification_legacy_target_keys: HashSet<NotificationLegacyTargetKey> = HashSet::new();
        let mut notification_legacy_target_zero_tombstones: HashMap<NotificationLegacyTargetKey, u8> = HashMap::new();
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    run_metrics_collector_tick(health, MetricsCollectorTaskId::NotificationStats, "notification_stats", async {
                        let snapshot = notification_metrics_snapshot();
                        let server = current_local_node_identity();
                        let mut metrics = collect_notification_runtime_metrics(&NotificationStats {
                            current_send_in_progress: snapshot.current_send_in_progress,
                            events_errors_total: snapshot.events_errors_total,
                            events_sent_total: snapshot.events_sent_total,
                            events_skipped_total: snapshot.events_skipped_total,
                        }, &server);

                        let target_stats = notification_target_metrics().await
                            .into_iter()
                            .map(|snapshot| NotificationTargetRuntimeStats {
                                server: server.clone(),
                                target: NotificationTargetStats {
                                    failed_messages: snapshot.failed_messages,
                                    failed_store_length: snapshot.failed_store_length,
                                    queue_length: snapshot.queue_length,
                                    target_id: snapshot.target_id,
                                    target_type: snapshot.target_type,
                                    total_messages: snapshot.total_messages,
                                },
                            })
                            .collect::<Vec<_>>();
                        update_series_zero_tombstones(
                            &mut has_seen_notification_target_snapshot,
                            &mut prev_notification_target_keys,
                            &mut notification_target_zero_tombstones,
                            notification_target_live_keys(&target_stats),
                            tombstone_cycles,
                        );
                        update_series_zero_tombstones(
                            &mut has_seen_notification_legacy_target_snapshot,
                            &mut prev_notification_legacy_target_keys,
                            &mut notification_legacy_target_zero_tombstones,
                            notification_legacy_target_live_keys(&target_stats),
                            tombstone_cycles,
                        );
                        metrics.extend(collect_notification_target_runtime_metrics(&target_stats));
                        metrics.extend(collect_notification_legacy_target_zero_tombstone_metrics(
                            &notification_legacy_target_zero_tombstones,
                        ));
                        metrics.extend(collect_notification_target_zero_tombstone_metrics(
                            &notification_target_zero_tombstones,
                        ));
                        report_metrics(&metrics);
                        for (target_id, target_type) in expire_series_zero_tombstones(&mut notification_legacy_target_zero_tombstones) {
                            let _ = retire_notification_legacy_target_metric_series(&target_id, &target_type);
                        }
                        for (server, target_id, target_type) in expire_series_zero_tombstones(&mut notification_target_zero_tombstones) {
                            let _ = retire_notification_target_metric_series(&server, &target_id, &target_type);
                        }
                    }).await;
                }
                _ = token_clone.cancelled() => {
                    warn!(event = EVENT_METRICS_RUNTIME_STATE, component = LOG_COMPONENT_OBS, subsystem = LOG_SUBSYSTEM_METRICS_RUNTIME, collector = "notification_stats", state = "cancelled", "metrics runtime state changed");
                    return;
                }
            }
        }
    });

    // Spawn task for background workflow metrics such as ILM and scanner.
    let token_clone = token.clone();
    tokio::spawn(async move {
        let mut interval = metrics_interval(cluster_interval, stagger_duration(cluster_interval, 2, 3));
        let mut has_seen_scanner_snapshot = false;
        let mut prev_scanner_cycle_bucket_drive_result_keys: HashSet<ScannerCycleBucketDriveResultKey> = HashSet::new();
        let mut prev_scanner_bucket_drive_result_keys: HashSet<ScannerBucketDriveResultKey> = HashSet::new();
        let mut prev_scanner_active_bucket_drive_keys: HashSet<ScannerActiveBucketDriveKey> = HashSet::new();
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    run_metrics_collector_tick(
                        health,
                        MetricsCollectorTaskId::BackgroundWorkflowStats,
                        "background_workflow_stats",
                        async {
                            let mut metrics = Vec::new();

                            if let Some(stats) = collect_ilm_runtime_metric_stats().await {
                                metrics.extend(collect_ilm_runtime_metrics(&stats));
                            }

                            let mut retire_scanner_cycle_bucket_drive_result_keys = Vec::new();
                            let mut retire_scanner_bucket_drive_result_keys = Vec::new();
                            let mut retire_scanner_active_bucket_drive_keys = Vec::new();
                            if let Some(stats) = collect_scanner_runtime_metric_stats().await {
                                let current_cycle_keys = scanner_cycle_bucket_drive_result_live_keys(&stats);
                                let current_keys = scanner_bucket_drive_result_live_keys(&stats);
                                let current_active_keys = scanner_active_bucket_drive_live_keys(&stats);
                                if has_seen_scanner_snapshot {
                                    retire_scanner_cycle_bucket_drive_result_keys = prev_scanner_cycle_bucket_drive_result_keys
                                        .difference(&current_cycle_keys)
                                        .cloned()
                                        .collect();
                                    retire_scanner_bucket_drive_result_keys = prev_scanner_bucket_drive_result_keys
                                        .difference(&current_keys)
                                        .cloned()
                                        .collect();
                                    retire_scanner_active_bucket_drive_keys = prev_scanner_active_bucket_drive_keys
                                        .difference(&current_active_keys)
                                        .cloned()
                                        .collect();
                                }
                                prev_scanner_cycle_bucket_drive_result_keys = current_cycle_keys;
                                prev_scanner_bucket_drive_result_keys = current_keys;
                                prev_scanner_active_bucket_drive_keys = current_active_keys;
                                has_seen_scanner_snapshot = true;
                                metrics.extend(collect_scanner_runtime_metrics(&stats));
                            }

                            if !metrics.is_empty() {
                                report_metrics(&metrics);
                            }
                            for key in retire_scanner_cycle_bucket_drive_result_keys {
                                let _ = retire_scanner_cycle_bucket_drive_result_metric_series(&key);
                            }
                            for key in retire_scanner_bucket_drive_result_keys {
                                let _ = retire_scanner_bucket_drive_result_metric_series(&key);
                            }
                            for key in retire_scanner_active_bucket_drive_keys {
                                let _ = retire_scanner_active_bucket_drive_metric_series(&key);
                            }
                        },
                    ).await;
                }
                _ = token_clone.cancelled() => {
                    warn!(event = EVENT_METRICS_RUNTIME_STATE, component = LOG_COMPONENT_OBS, subsystem = LOG_SUBSYSTEM_METRICS_RUNTIME, collector = "background_workflow_stats", state = "cancelled", "metrics runtime state changed");
                    return;
                }
            }
        }
    });

    // Spawn task for API request metrics.
    let token_clone = token.clone();
    tokio::spawn(async move {
        let mut interval = metrics_interval(resource_interval, stagger_duration(resource_interval, 1, 2));
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    run_metrics_collector_tick(health, MetricsCollectorTaskId::RequestStats, "request_stats", async {
                        let metrics = collect_request_metrics(&collect_api_request_stats());
                        if !metrics.is_empty() {
                            report_metrics(&metrics);
                        }
                    }).await;
                }
                _ = token_clone.cancelled() => {
                    warn!(event = EVENT_METRICS_RUNTIME_STATE, component = LOG_COMPONENT_OBS, subsystem = LOG_SUBSYSTEM_METRICS_RUNTIME, collector = "request_stats", state = "cancelled", "metrics runtime state changed");
                    return;
                }
            }
        }
    });

    // Spawn task for system monitoring metrics (migrated from rustfs-obs::system)
    let system_interval = config.system_interval;

    let token_clone = token.clone();
    tokio::spawn(async move {
        let process_attribute_labels = current_process_attribute_labels();
        let mut host_system = System::new_all();
        let mut host_networks = Networks::new();
        let mut process_sampler = ProcessSampler::new();
        let process_interval = config.process_interval;
        let mut interval = metrics_interval(process_interval, Duration::ZERO);
        let now = Instant::now();
        let mut next_resource_run = now;
        let mut next_system_run = now;

        host_system.refresh_cpu_all();
        tokio::time::sleep(sysinfo::MINIMUM_CPU_UPDATE_INTERVAL).await;
        host_system.refresh_cpu_all();

        #[cfg(feature = "gpu")]
        let current_pid = match sysinfo::get_current_pid() {
            Ok(pid) => Some(pid),
            Err(e) => {
                warn!(event = EVENT_METRICS_RUNTIME_STATE, component = LOG_COMPONENT_OBS, subsystem = LOG_SUBSYSTEM_METRICS_RUNTIME, collector = "system_monitoring", result = "current_pid_unavailable", error = %e, "metrics runtime state changed");
                None
            }
        };

        #[cfg(feature = "gpu")]
        let gpu_collector = current_pid.and_then(|pid| {
            use crate::metrics::collectors::GpuCollector;

            match GpuCollector::new(pid) {
                Ok(collector) => Some(collector),
                Err(e) => {
                    warn!(event = EVENT_METRICS_RUNTIME_STATE, component = LOG_COMPONENT_OBS, subsystem = LOG_SUBSYSTEM_METRICS_RUNTIME, collector = "gpu_metrics", result = "collector_init_failed", error = %e, "metrics runtime state changed");
                    None
                }
            }
        });

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    run_metrics_collector_tick(health, MetricsCollectorTaskId::ProcessMetrics, "process_metrics", async {
                        let now = Instant::now();
                    let bundle = collect_process_metric_bundle_with(&mut process_sampler);

                        if now >= next_resource_run {
                            let mut metrics = collect_resource_metrics(&bundle.resource);
                            metrics.extend(collect_process_metrics(&bundle.process));
                            report_metrics(&metrics);
                            advance_deadline(&mut next_resource_run, resource_interval, now);
                        }

                        if now >= next_system_run {
                            let labels = current_process_metric_labels(&process_attribute_labels);
                            #[cfg(feature = "gpu")]
                            let mut metrics =
                                collect_system_monitoring_metrics(&bundle, &labels, &mut host_system, &mut host_networks);
                            #[cfg(not(feature = "gpu"))]
                            let mut metrics =
                                collect_system_monitoring_metrics(&bundle, &labels, &mut host_system, &mut host_networks);

                            metrics.extend(collect_current_dial9_metrics());

                            #[cfg(feature = "gpu")]
                            if let Some(collector) = gpu_collector.as_ref() {
                                use crate::metrics::collectors::collect_gpu_metrics;

                                match collector.collect() {
                                    Ok(gpu_stats) => {
                                        metrics.extend(collect_gpu_metrics(&gpu_stats, &labels));
                                    }
                                    Err(e) => {
                                        warn!(event = EVENT_METRICS_RUNTIME_STATE, component = LOG_COMPONENT_OBS, subsystem = LOG_SUBSYSTEM_METRICS_RUNTIME, collector = "gpu_metrics", result = "collect_failed", error = %e, "metrics runtime state changed");
                                    }
                                }
                            }

                            report_metrics(&metrics);
                            advance_deadline(&mut next_system_run, system_interval, now);
                        }
                    }).await;
                }
                _ = token_clone.cancelled() => {
                    warn!(event = EVENT_METRICS_RUNTIME_STATE, component = LOG_COMPONENT_OBS, subsystem = LOG_SUBSYSTEM_METRICS_RUNTIME, collector = "process_metrics", state = "cancelled", "metrics runtime state changed");
                    return;
                }
            }
        }
    });

    // Spawn task for compression metrics.
    if compression_enabled {
        let token_clone = token.clone();
        tokio::spawn(async move {
            let mut interval = metrics_interval(cluster_interval, stagger_duration(cluster_interval, 3, 4));
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        run_metrics_collector_tick(
                            health,
                            MetricsCollectorTaskId::CompressionClusterStats,
                            "compression_cluster_stats",
                            async {
                                if let Some(stats) = collect_compression_cluster_stats().await {
                                    let metrics = collect_compression_cluster_metrics(&stats);
                                    if !metrics.is_empty() {
                                        report_metrics(&metrics);
                                    }
                                }
                            }
                        ).await;
                    }
                    _ = token_clone.cancelled() => {
                        warn!(event = EVENT_METRICS_RUNTIME_STATE, component = LOG_COMPONENT_OBS, subsystem = LOG_SUBSYSTEM_METRICS_RUNTIME, collector = "compression_cluster_stats", state = "cancelled", "metrics runtime state changed");
                        return;
                    }
                }
            }
        });
    }

    // Spawn task for internode/system network metrics.
    let token_clone = token;
    tokio::spawn(async move {
        let mut interval = metrics_interval(system_interval, Duration::ZERO);
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    run_metrics_collector_tick(
                        health,
                        MetricsCollectorTaskId::InternodeNetworkStats,
                        "internode_network_stats",
                        async {
                            if let Some(stats) = collect_internode_network_stats() {
                                let metrics = collect_network_metrics(&stats);
                                if !metrics.is_empty() {
                                    report_metrics(&metrics);
                                }
                            }
                        },
                    ).await;
                }
                _ = token_clone.cancelled() => {
                    warn!(event = EVENT_METRICS_RUNTIME_STATE, component = LOG_COMPONENT_OBS, subsystem = LOG_SUBSYSTEM_METRICS_RUNTIME, collector = "internode_network_stats", state = "cancelled", "metrics runtime state changed");
                    return;
                }
            }
        }
    });
}

fn advance_deadline(deadline: &mut Instant, interval: Duration, now: Instant) {
    if *deadline > now {
        return;
    }

    let interval_nanos = interval.as_nanos();
    if interval_nanos == 0 {
        return;
    }

    let elapsed = now.duration_since(*deadline);
    let missed_intervals = (elapsed.as_nanos() / interval_nanos) + 1;
    let mut remaining = missed_intervals;

    while remaining > 0 {
        let chunk_u128 = remaining.min(u128::from(u32::MAX));
        let chunk_u32 = chunk_u128 as u32;

        if let Some(advance_by) = interval.checked_mul(chunk_u32) {
            *deadline += advance_by;
            remaining -= chunk_u128;
            continue;
        }

        *deadline += interval;
        remaining -= 1;
    }
}

fn current_process_attribute_labels() -> Vec<(&'static str, Cow<'static, str>)> {
    match collect_process_attributes() {
        Ok(attrs) => vec![
            (PROCESS_PID_LABEL, Cow::Owned(attrs.pid.to_string())),
            (PROCESS_EXECUTABLE_NAME_LABEL, Cow::Owned(attrs.executable_name)),
        ],
        Err(err) => fallback_process_attribute_labels(err),
    }
}

fn fallback_process_attribute_labels(err: ProcessAttributeError) -> Vec<(&'static str, Cow<'static, str>)> {
    warn!(event = EVENT_METRICS_RUNTIME_STATE, component = LOG_COMPONENT_OBS, subsystem = LOG_SUBSYSTEM_METRICS_RUNTIME, collector = "process_metric_labels", result = "collect_failed", error = %err, "metrics runtime state changed");
    vec![
        (PROCESS_PID_LABEL, Cow::Owned(std::process::id().to_string())),
        (PROCESS_EXECUTABLE_NAME_LABEL, Cow::Borrowed("unknown")),
    ]
}

fn current_process_metric_labels(
    process_attribute_labels: &[(&'static str, Cow<'static, str>)],
) -> Vec<(&'static str, Cow<'static, str>)> {
    let mut labels = Vec::with_capacity(process_attribute_labels.len() + 1);
    labels.push((SERVER_LABEL, Cow::Owned(current_local_node_identity())));
    labels.extend(process_attribute_labels.iter().map(|(key, value)| (*key, value.clone())));
    labels
}

fn collect_system_monitoring_metrics(
    bundle: &ProcessMetricBundle,
    labels: &[(&'static str, Cow<'static, str>)],
    host_system: &mut System,
    host_networks: &mut Networks,
) -> Vec<PrometheusMetric> {
    let cpu_stats = ProcessCpuStats {
        usage: bundle.resource.cpu_percent,
        utilization: bundle.resource.cpu_percent,
    };
    let memory_stats = ProcessMemoryStats {
        resident: bundle.process.resident_memory_bytes,
        virtual_mem: bundle.process.virtual_memory_bytes,
    };
    let disk_stats = ProcessDiskStats {
        read_bytes: bundle.disk_read_bytes,
        written_bytes: bundle.disk_write_bytes,
    };
    let network_stats = collect_host_network_stats(host_networks);
    let (system_cpu_stats, system_memory_stats) = collect_system_cpu_and_memory_stats_with(host_system);

    let mut metrics = Vec::new();
    metrics.extend(collect_cpu_metrics(&system_cpu_stats));
    metrics.extend(collect_memory_metrics(&system_memory_stats));
    metrics.extend(collect_process_cpu_metrics(&cpu_stats, Some(labels)));
    metrics.extend(collect_process_memory_metrics(&memory_stats, Some(labels)));
    metrics.extend(collect_process_disk_metrics(&disk_stats, Some(labels)));
    // Interface counters are host-wide, so keep these metrics free of process labels.
    metrics.extend(collect_host_network_metrics(&network_stats, None));
    metrics
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::collectors::scanner::ScannerActiveBucketDriveStats;
    use std::collections::{HashMap, HashSet};
    use std::time::Duration;
    use tokio::time::Instant;

    fn fixed_metrics_runtime_config() -> MetricsRuntimeConfig {
        MetricsRuntimeConfig {
            cluster_interval: Duration::from_secs(60),
            bucket_interval: Duration::from_secs(300),
            bucket_replication_bandwidth_interval: Duration::from_secs(30),
            node_interval: Duration::from_secs(60),
            resource_interval: Duration::from_secs(15),
            audit_interval: Duration::from_secs(45),
            notification_interval: Duration::from_secs(20),
            system_interval: Duration::from_secs(10),
            process_interval: Duration::from_secs(10),
            replication_bandwidth_zero_tombstone_cycles: 3,
        }
    }

    fn reset_metrics_runtime_collector_health_for_test() {
        let health = metrics_runtime_collector_health();
        for entry in &health.last_success_unix_secs {
            entry.store(0, Ordering::Relaxed);
        }
        for entry in &health.collector_panics_total {
            entry.store(0, Ordering::Relaxed);
        }
        for entry in &health.failure_state {
            entry.store(false, Ordering::Relaxed);
        }
    }

    fn repl_bw_key(bucket: &str, target_arn: &str) -> ReplBwKey {
        (bucket.to_string(), target_arn.to_string())
    }

    fn repl_bw_keys(keys: &[(&str, &str)]) -> HashSet<ReplBwKey> {
        keys.iter()
            .map(|(bucket, target_arn)| repl_bw_key(bucket, target_arn))
            .collect()
    }

    fn bucket_key(bucket: &str) -> BucketKey {
        bucket.to_string()
    }

    fn bucket_keys(keys: &[&str]) -> HashSet<BucketKey> {
        keys.iter().map(|bucket| bucket_key(bucket)).collect()
    }

    fn audit_target_key(server: &str, target_id: &str) -> AuditTargetKey {
        (server.to_string(), target_id.to_string())
    }

    fn notification_target_key(server: &str, target_id: &str, target_type: &str) -> NotificationTargetKey {
        (server.to_string(), target_id.to_string(), target_type.to_string())
    }

    fn drive_info_stat(disk_id: &str) -> DriveRuntimeDetailedStats {
        DriveRuntimeDetailedStats {
            pool_index: Some("0".to_string()),
            set_index: Some("1".to_string()),
            drive_index: Some("2".to_string()),
            disk_id: Some(disk_id.to_string()),
            runtime_state: Some("online".to_string()),
            api_calls: vec![("read_all".to_string(), 1)],
            api_latency_by_api_micros: vec![("write_all".to_string(), 2)],
            stats: crate::metrics::DriveDetailedStats {
                server: "server-a".to_string(),
                drive: "/data1".to_string(),
                ..Default::default()
            },
            ..Default::default()
        }
    }

    fn scanner_stats_with_last_result(bucket: &str) -> ScannerRuntimeStats {
        ScannerRuntimeStats {
            server: "server-a".to_string(),
            last_cycle_bucket_drive_results: vec![crate::metrics::scanner::ScannerBucketDriveResultStats {
                bucket: bucket.to_string(),
                drive: "/data1".to_string(),
                result: "success".to_string(),
                count: 1,
            }],
            ..Default::default()
        }
    }

    fn scanner_stats_with_lifetime_result(bucket: &str) -> ScannerRuntimeStats {
        ScannerRuntimeStats {
            server: "server-a".to_string(),
            bucket_drive_results: vec![crate::metrics::scanner::ScannerBucketDriveResultStats {
                bucket: bucket.to_string(),
                drive: "/data1".to_string(),
                result: "success".to_string(),
                count: 1,
            }],
            ..Default::default()
        }
    }

    fn scanner_stats_with_current_result(bucket: &str) -> ScannerRuntimeStats {
        ScannerRuntimeStats {
            server: "server-a".to_string(),
            current_cycle_bucket_drive_results: vec![crate::metrics::scanner::ScannerBucketDriveResultStats {
                bucket: bucket.to_string(),
                drive: "/data1".to_string(),
                result: "success".to_string(),
                count: 1,
            }],
            ..Default::default()
        }
    }

    #[test]
    fn drive_info_live_keys_detect_disk_identity_replacement() {
        let previous = drive_info_live_keys(&[drive_info_stat("disk-old")]);
        let current = drive_info_live_keys(&[drive_info_stat("disk-new")]);
        let retired = previous.difference(&current).cloned().collect::<HashSet<_>>();

        assert!(current.contains(&(
            "server-a".to_string(),
            "/data1".to_string(),
            "0".to_string(),
            "1".to_string(),
            "2".to_string(),
            "disk-new".to_string(),
        )));
        assert!(retired.contains(&(
            "server-a".to_string(),
            "/data1".to_string(),
            "0".to_string(),
            "1".to_string(),
            "2".to_string(),
            "disk-old".to_string(),
        )));
    }

    #[test]
    fn drive_topology_keys_detect_removed_drives() {
        let previous = drive_topology_live_keys(&[drive_info_stat("disk-old")]);
        let current = drive_topology_live_keys(&[]);
        let retired = previous.difference(&current).cloned().collect::<HashSet<_>>();

        assert!(retired.contains(&(
            "server-a".to_string(),
            "/data1".to_string(),
            "0".to_string(),
            "1".to_string(),
            "2".to_string(),
        )));
    }

    #[test]
    fn drive_topology_api_keys_detect_removed_drives() {
        let previous = drive_topology_api_live_keys(&[drive_info_stat("disk-old")]);
        let current = drive_topology_api_live_keys(&[]);
        let retired = previous.difference(&current).cloned().collect::<HashSet<_>>();

        assert!(retired.contains(&(
            "server-a".to_string(),
            "/data1".to_string(),
            "0".to_string(),
            "1".to_string(),
            "2".to_string(),
            "read_all".to_string(),
        )));
        assert!(retired.contains(&(
            "server-a".to_string(),
            "/data1".to_string(),
            "0".to_string(),
            "1".to_string(),
            "2".to_string(),
            "write_all".to_string(),
        )));
    }

    #[test]
    fn scanner_last_bucket_drive_result_keys_detect_superseded_cycle_results() {
        let previous = scanner_cycle_bucket_drive_result_live_keys(&scanner_stats_with_last_result("photos"));
        let current = scanner_cycle_bucket_drive_result_live_keys(&scanner_stats_with_last_result("logs"));
        let retired = previous.difference(&current).cloned().collect::<HashSet<_>>();

        assert!(retired.contains(&(
            "server-a".to_string(),
            "last".to_string(),
            "photos".to_string(),
            "/data1".to_string(),
            "success".to_string(),
        )));
        assert!(current.contains(&(
            "server-a".to_string(),
            "last".to_string(),
            "logs".to_string(),
            "/data1".to_string(),
            "success".to_string(),
        )));
    }

    #[test]
    fn scanner_bucket_drive_result_keys_detect_completed_current_cycle_results() {
        let previous = scanner_cycle_bucket_drive_result_live_keys(&scanner_stats_with_current_result("photos"));
        let current = scanner_cycle_bucket_drive_result_live_keys(&ScannerRuntimeStats {
            server: "server-a".to_string(),
            ..Default::default()
        });
        let retired = previous.difference(&current).cloned().collect::<HashSet<_>>();

        assert!(retired.contains(&(
            "server-a".to_string(),
            "current".to_string(),
            "photos".to_string(),
            "/data1".to_string(),
            "success".to_string(),
        )));
    }

    #[test]
    fn scanner_bucket_drive_result_keys_detect_evicted_lifetime_results() {
        let previous = scanner_bucket_drive_result_live_keys(&scanner_stats_with_lifetime_result("photos"));
        let current = scanner_bucket_drive_result_live_keys(&scanner_stats_with_lifetime_result("logs"));
        let retired = previous.difference(&current).cloned().collect::<HashSet<_>>();

        assert!(retired.contains(&("server-a".to_string(), "photos".to_string(), "/data1".to_string(), "success".to_string(),)));
        assert!(current.contains(&("server-a".to_string(), "logs".to_string(), "/data1".to_string(), "success".to_string(),)));
    }

    #[test]
    fn scanner_active_bucket_drive_keys_detect_completed_scans() {
        let previous = scanner_active_bucket_drive_live_keys(&ScannerRuntimeStats {
            server: "server-a".to_string(),
            active_bucket_drive_scans: vec![ScannerActiveBucketDriveStats {
                source: "usage".to_string(),
                bucket: "photos".to_string(),
                drive: "/data1".to_string(),
                count: 1,
                age_seconds: 3,
            }],
            ..Default::default()
        });
        let current = scanner_active_bucket_drive_live_keys(&ScannerRuntimeStats {
            server: "server-a".to_string(),
            ..Default::default()
        });
        assert!(
            previous
                .difference(&current)
                .any(|key| key == &("server-a".to_string(), "usage".to_string(), "photos".to_string(), "/data1".to_string()))
        );
    }

    #[test]
    fn replication_proxy_bucket_keys_detect_removed_buckets() {
        let previous = repl_proxy_bucket_live_keys(&[BucketReplicationRuntimeStats {
            stats: crate::metrics::BucketReplicationMetricsSnapshot {
                bucket: "photos".to_string(),
                ..Default::default()
            },
            ..Default::default()
        }]);
        let current = repl_proxy_bucket_live_keys(&[BucketReplicationRuntimeStats {
            stats: crate::metrics::BucketReplicationMetricsSnapshot {
                bucket: "logs".to_string(),
                ..Default::default()
            },
            ..Default::default()
        }]);
        let retired = previous.difference(&current).cloned().collect::<HashSet<_>>();

        assert_eq!(retired, bucket_keys(&["photos"]));
        assert_eq!(current, bucket_keys(&["logs"]));
    }

    #[test]
    fn metrics_runtime_status_reports_disabled_state() {
        let snapshot = build_metrics_runtime_status_snapshot(false, false, fixed_metrics_runtime_config(), false);

        assert_eq!(snapshot.service, METRICS_RUNTIME_SERVICE_NAME);
        assert_eq!(snapshot.state, MetricsRuntimeServiceState::Disabled);
        assert!(!snapshot.metrics_enabled);
        assert_eq!(snapshot.collector_tasks, METRICS_RUNTIME_BASE_COLLECTOR_TASKS);
        assert_eq!(snapshot.collector_health, MetricsRuntimeCollectorHealthSnapshot::default());
        assert_eq!(snapshot.intervals.cluster_interval_secs, 60);
        assert_eq!(snapshot.intervals.bucket_interval_secs, 300);
        assert_eq!(snapshot.intervals.process_interval_secs, 10);
        assert_eq!(snapshot.intervals.replication_bandwidth_zero_tombstone_cycles, 3);
        assert_eq!(snapshot.cancellation_source, MetricsRuntimeCancellationSource::RuntimeToken);
        assert_eq!(snapshot.shutdown_handle, MetricsRuntimeShutdownHandle::RuntimeTokenOnly);
    }

    #[test]
    fn metrics_runtime_status_reports_running_and_stopping_states() {
        reset_metrics_runtime_collector_health_for_test();
        let running = build_metrics_runtime_status_snapshot(true, false, fixed_metrics_runtime_config(), false);
        let stopping = build_metrics_runtime_status_snapshot(true, true, fixed_metrics_runtime_config(), false);

        assert_eq!(running.state, MetricsRuntimeServiceState::Running);
        assert_eq!(stopping.state, MetricsRuntimeServiceState::Stopping);
        assert!(running.metrics_enabled);
        assert!(stopping.metrics_enabled);
        assert_eq!(running.collector_health.unhealthy_collectors, METRICS_RUNTIME_BASE_COLLECTOR_TASKS);
    }

    #[test]
    fn metrics_runtime_controller_reconcile_is_idempotent() {
        let controller = MetricsRuntimeController;
        let snapshot = build_metrics_runtime_controller_snapshot(true, false, fixed_metrics_runtime_config(), false);

        let first = controller.reconcile_snapshot(snapshot);
        let second = controller.reconcile_snapshot(snapshot);

        assert_eq!(first, second);
        assert_eq!(first.service, METRICS_RUNTIME_SERVICE_NAME);
        assert_eq!(first.desired.state, MetricsRuntimeDesiredState::Enabled);
        assert_eq!(first.current_state, MetricsRuntimeServiceState::Running);
        assert_eq!(first.worker_mutation, MetricsRuntimeWorkerMutation::None);
    }

    #[test]
    fn metrics_runtime_controller_reports_disabled_without_worker_mutation() {
        let controller = MetricsRuntimeController;
        let snapshot = build_metrics_runtime_controller_snapshot(false, false, fixed_metrics_runtime_config(), false);
        let plan = controller.reconcile_snapshot(snapshot);

        assert_eq!(snapshot.desired.state, MetricsRuntimeDesiredState::Disabled);
        assert_eq!(snapshot.status.state, MetricsRuntimeServiceState::Disabled);
        assert_eq!(plan.current_state, MetricsRuntimeServiceState::Disabled);
        assert_eq!(plan.worker_mutation, MetricsRuntimeWorkerMutation::None);
    }

    #[test]
    fn advance_deadline_keeps_future_deadline_unchanged() {
        let base = Instant::now();
        let mut deadline = base + Duration::from_secs(10);
        advance_deadline(&mut deadline, Duration::from_secs(5), base);
        assert_eq!(deadline, base + Duration::from_secs(10));
    }

    #[test]
    fn advance_deadline_moves_to_first_tick_after_now() {
        let base = Instant::now();
        let mut deadline = base;
        advance_deadline(&mut deadline, Duration::from_secs(5), base + Duration::from_secs(12));
        assert_eq!(deadline, base + Duration::from_secs(15));
    }

    #[tokio::test]
    async fn metrics_interval_uses_delay_missed_tick_behavior() {
        let interval = metrics_interval(Duration::from_secs(5), Duration::from_secs(1));
        assert_eq!(interval.missed_tick_behavior(), MissedTickBehavior::Delay);
    }

    #[test]
    fn metrics_runtime_collector_tasks_expand_when_compression_enabled() {
        assert_eq!(
            metrics_runtime_collector_tasks(false),
            u8::try_from(BASE_COLLECTOR_TASK_IDS.len()).expect("base collector count fits in u8")
        );
        assert_eq!(
            metrics_runtime_collector_tasks(true),
            u8::try_from(ALL_COLLECTOR_TASK_IDS.len()).expect("all collector count fits in u8")
        );
    }

    #[tokio::test]
    async fn supervised_tick_records_success_and_panic_health() {
        let health = MetricsRuntimeCollectorHealth::new();

        run_metrics_collector_tick(&health, MetricsCollectorTaskId::ClusterStats, "cluster_stats", async {}).await;
        let success = health.snapshot(&[MetricsCollectorTaskId::ClusterStats]);
        assert_eq!(success.healthy_collectors, 1);
        assert_eq!(success.unhealthy_collectors, 0);
        assert_eq!(success.collector_panics_total, 0);

        run_metrics_collector_tick(&health, MetricsCollectorTaskId::ClusterStats, "cluster_stats", async {
            panic!("collector panic");
        })
        .await;
        let failed = health.snapshot(&[MetricsCollectorTaskId::ClusterStats]);
        assert_eq!(failed.healthy_collectors, 0);
        assert_eq!(failed.unhealthy_collectors, 1);
        assert_eq!(failed.collector_panics_total, 1);
    }

    #[test]
    fn repl_bw_tombstones_zero_removed_keys_then_expire() {
        let mut has_seen_valid_snapshot = false;
        let mut prev_live_keys = HashSet::new();
        let mut zero_tombstones = HashMap::new();
        let key = repl_bw_key("photos", "arn:rustfs:replication:target-a");

        update_repl_bw_zero_tombstones(
            true,
            &mut has_seen_valid_snapshot,
            &mut prev_live_keys,
            &mut zero_tombstones,
            repl_bw_keys(&[("photos", "arn:rustfs:replication:target-a")]),
            2,
        );
        assert!(has_seen_valid_snapshot);
        assert_eq!(prev_live_keys, repl_bw_keys(&[("photos", "arn:rustfs:replication:target-a")]));
        assert!(zero_tombstones.is_empty());

        update_repl_bw_zero_tombstones(
            true,
            &mut has_seen_valid_snapshot,
            &mut prev_live_keys,
            &mut zero_tombstones,
            HashSet::new(),
            2,
        );
        assert_eq!(zero_tombstones.get(&key), Some(&2));

        let metrics = collect_repl_bw_zero_tombstone_metrics(&zero_tombstones);
        assert_eq!(metrics.len(), 2);
        assert!(metrics.iter().all(|metric| metric.value == 0.0));

        let names = metrics.iter().map(|metric| metric.name.to_string()).collect::<HashSet<_>>();
        assert!(names.contains(&BUCKET_REPL_BANDWIDTH_LIMIT_MD.get_full_metric_name()));
        assert!(names.contains(&BUCKET_REPL_BANDWIDTH_CURRENT_MD.get_full_metric_name()));

        for metric in metrics {
            let labels = metric
                .labels
                .into_iter()
                .map(|(key, value)| (key, value.to_string()))
                .collect::<HashMap<_, _>>();
            assert_eq!(labels.get(BUCKET_L).map(String::as_str), Some("photos"));
            assert_eq!(labels.get(TARGET_ARN_L).map(String::as_str), Some("arn:rustfs:replication:target-a"));
        }

        let expired = expire_repl_bw_zero_tombstones(true, &mut zero_tombstones);
        assert!(expired.is_empty());
        assert_eq!(zero_tombstones.get(&key), Some(&1));

        let expired = expire_repl_bw_zero_tombstones(true, &mut zero_tombstones);
        assert_eq!(expired, vec![key]);
        assert!(zero_tombstones.is_empty());
    }

    #[test]
    fn repl_flow_tombstones_zero_latency_and_retire_target_counters() {
        let zero_tombstones = HashMap::from([(repl_bw_key("photos", "arn:rustfs:replication:target-a"), 2)]);

        let metrics = collect_repl_flow_zero_tombstone_metrics(&zero_tombstones);

        assert_eq!(metrics.len(), 1);
        assert!(metrics.iter().all(|metric| metric.value == 0.0));
        let names = metrics.iter().map(|metric| metric.name.to_string()).collect::<HashSet<_>>();
        assert!(names.contains(&BUCKET_REPL_LATENCY_MS_MD.get_full_metric_name()));
        for metric in metrics {
            let labels = metric
                .labels
                .into_iter()
                .map(|(key, value)| (key, value.to_string()))
                .collect::<HashMap<_, _>>();
            assert_eq!(labels.get(BUCKET_L).map(String::as_str), Some("photos"));
            assert_eq!(labels.get(TARGET_ARN_L).map(String::as_str), Some("arn:rustfs:replication:target-a"));
            if metric.name == BUCKET_REPL_LATENCY_MS_MD.get_full_metric_name() {
                assert_eq!(labels.get(OPERATION_L).map(String::as_str), Some("object_replication"));
                assert_eq!(labels.get(RANGE_L).map(String::as_str), Some("all"));
            }
        }
    }

    #[test]
    fn audit_target_tombstones_include_server_detail_metrics() {
        let zero_tombstones = HashMap::from([(audit_target_key("node1:9000", "audit-webhook"), 2)]);
        let metrics = collect_audit_zero_tombstone_metrics(&zero_tombstones);

        assert_eq!(metrics.len(), 4);
        assert!(metrics.iter().any(|metric| {
            metric.name == AUDIT_TARGET_QUEUE_LENGTH_BY_SERVER_MD.get_full_metric_name()
                && metric.value == 0.0
                && metric
                    .labels
                    .iter()
                    .any(|(name, value)| *name == AUDIT_SERVER_LABEL && value == "node1:9000")
                && metric
                    .labels
                    .iter()
                    .any(|(name, value)| *name == AUDIT_TARGET_ID_LABEL && value == "audit-webhook")
        }));
    }

    #[test]
    fn audit_legacy_tombstones_follow_target_only_liveness() {
        let mut has_seen_snapshot = true;
        let mut prev_live_keys = HashSet::from(["audit-webhook".to_string()]);
        let mut zero_tombstones = HashMap::new();
        let stats = vec![AuditTargetRuntimeStats {
            server: "node2:9000".to_string(),
            target: AuditTargetStats {
                target_id: "audit-webhook".to_string(),
                ..Default::default()
            },
        }];

        update_series_zero_tombstones(
            &mut has_seen_snapshot,
            &mut prev_live_keys,
            &mut zero_tombstones,
            audit_legacy_target_live_keys(&stats),
            2,
        );

        assert!(zero_tombstones.is_empty());
        let metrics = collect_audit_legacy_zero_tombstone_metrics(&HashMap::from([("removed-webhook".to_string(), 2)]));
        assert_eq!(metrics.len(), 4);
        assert!(metrics.iter().any(|metric| {
            metric.name == AUDIT_FAILED_STORE_LENGTH_MD.get_full_metric_name()
                && metric.value == 0.0
                && metric
                    .labels
                    .iter()
                    .any(|(name, value)| *name == AUDIT_TARGET_ID_LABEL && value == "removed-webhook")
        }));
    }

    #[test]
    fn notification_target_tombstones_include_server_detail_metrics() {
        let zero_tombstones = HashMap::from([(notification_target_key("node1:9000", "primary:webhook", "webhook"), 2)]);
        let metrics = collect_notification_target_zero_tombstone_metrics(&zero_tombstones);

        assert_eq!(metrics.len(), 4);
        assert!(metrics.iter().any(|metric| {
            metric.name == NOTIFICATION_TARGET_QUEUE_LENGTH_BY_SERVER_MD.get_full_metric_name()
                && metric.value == 0.0
                && metric
                    .labels
                    .iter()
                    .any(|(name, value)| *name == NOTIFICATION_SERVER_LABEL && value == "node1:9000")
                && metric
                    .labels
                    .iter()
                    .any(|(name, value)| *name == NOTIFICATION_TARGET_ID_LABEL && value == "primary:webhook")
                && metric
                    .labels
                    .iter()
                    .any(|(name, value)| *name == NOTIFICATION_TARGET_TYPE_LABEL && value == "webhook")
        }));
    }

    #[test]
    fn notification_legacy_tombstones_follow_target_only_liveness() {
        let mut has_seen_snapshot = true;
        let mut prev_live_keys = HashSet::from([("primary:webhook".to_string(), "webhook".to_string())]);
        let mut zero_tombstones = HashMap::new();
        let stats = vec![NotificationTargetRuntimeStats {
            server: "node2:9000".to_string(),
            target: NotificationTargetStats {
                target_id: "primary:webhook".to_string(),
                target_type: "webhook".to_string(),
                ..Default::default()
            },
        }];

        update_series_zero_tombstones(
            &mut has_seen_snapshot,
            &mut prev_live_keys,
            &mut zero_tombstones,
            notification_legacy_target_live_keys(&stats),
            2,
        );

        assert!(zero_tombstones.is_empty());
        let metrics = collect_notification_legacy_target_zero_tombstone_metrics(&HashMap::from([(
            ("removed:webhook".to_string(), "webhook".to_string()),
            2,
        )]));
        assert_eq!(metrics.len(), 4);
        assert!(metrics.iter().any(|metric| {
            metric.name == NOTIFICATION_TARGET_FAILED_STORE_LENGTH_MD.get_full_metric_name()
                && metric.value == 0.0
                && metric
                    .labels
                    .iter()
                    .any(|(name, value)| *name == NOTIFICATION_TARGET_ID_LABEL && value == "removed:webhook")
                && metric
                    .labels
                    .iter()
                    .any(|(name, value)| *name == NOTIFICATION_TARGET_TYPE_LABEL && value == "webhook")
        }));
    }

    #[test]
    fn repl_bw_tombstones_stop_zeroing_when_key_becomes_live_again() {
        let mut has_seen_valid_snapshot = false;
        let mut prev_live_keys = HashSet::new();
        let mut zero_tombstones = HashMap::new();
        let live_keys = repl_bw_keys(&[("photos", "arn:rustfs:replication:target-a")]);

        update_repl_bw_zero_tombstones(
            true,
            &mut has_seen_valid_snapshot,
            &mut prev_live_keys,
            &mut zero_tombstones,
            live_keys.clone(),
            3,
        );
        update_repl_bw_zero_tombstones(
            true,
            &mut has_seen_valid_snapshot,
            &mut prev_live_keys,
            &mut zero_tombstones,
            HashSet::new(),
            3,
        );
        assert_eq!(zero_tombstones.get(&repl_bw_key("photos", "arn:rustfs:replication:target-a")), Some(&3));

        update_repl_bw_zero_tombstones(
            true,
            &mut has_seen_valid_snapshot,
            &mut prev_live_keys,
            &mut zero_tombstones,
            live_keys.clone(),
            3,
        );

        assert!(zero_tombstones.is_empty());
        assert_eq!(prev_live_keys, live_keys);
    }

    #[test]
    fn repl_bw_tombstones_do_not_advance_when_monitor_unavailable() {
        let mut has_seen_valid_snapshot = true;
        let mut prev_live_keys = repl_bw_keys(&[("photos", "arn:rustfs:replication:target-a")]);
        let mut zero_tombstones = HashMap::from([(repl_bw_key("videos", "arn:rustfs:replication:target-b"), 1)]);

        update_repl_bw_zero_tombstones(
            false,
            &mut has_seen_valid_snapshot,
            &mut prev_live_keys,
            &mut zero_tombstones,
            HashSet::new(),
            3,
        );

        assert!(has_seen_valid_snapshot);
        assert_eq!(prev_live_keys, repl_bw_keys(&[("photos", "arn:rustfs:replication:target-a")]));
        assert_eq!(zero_tombstones.get(&repl_bw_key("videos", "arn:rustfs:replication:target-b")), Some(&1));

        let expired = expire_repl_bw_zero_tombstones(false, &mut zero_tombstones);
        assert!(expired.is_empty());
        assert_eq!(zero_tombstones.get(&repl_bw_key("videos", "arn:rustfs:replication:target-b")), Some(&1));
    }

    #[test]
    fn repl_backlog_tombstones_zero_removed_buckets_then_expire() {
        let mut has_seen_valid_snapshot = false;
        let mut prev_live_keys = HashSet::new();
        let mut zero_tombstones = HashMap::new();
        let key = bucket_key("photos");

        update_repl_backlog_zero_tombstones(
            true,
            &mut has_seen_valid_snapshot,
            &mut prev_live_keys,
            &mut zero_tombstones,
            bucket_keys(&["photos"]),
            2,
        );
        assert!(has_seen_valid_snapshot);
        assert_eq!(prev_live_keys, bucket_keys(&["photos"]));
        assert!(zero_tombstones.is_empty());

        update_repl_backlog_zero_tombstones(
            true,
            &mut has_seen_valid_snapshot,
            &mut prev_live_keys,
            &mut zero_tombstones,
            HashSet::new(),
            2,
        );
        assert_eq!(zero_tombstones.get(&key), Some(&2));

        let metrics = collect_repl_backlog_zero_tombstone_metrics(&zero_tombstones);
        assert_eq!(metrics.len(), 11);

        let expected_names = HashSet::from([
            BUCKET_REPL_CURRENT_BACKLOG_COUNT_MD.get_full_metric_name(),
            BUCKET_REPL_CURRENT_BACKLOG_BYTES_MD.get_full_metric_name(),
            BUCKET_REPL_DURABLE_MRF_AVAILABLE_MD.get_full_metric_name(),
            BUCKET_REPL_DURABLE_MRF_BACKLOG_COUNT_MD.get_full_metric_name(),
            BUCKET_REPL_DURABLE_MRF_BACKLOG_BYTES_MD.get_full_metric_name(),
            BUCKET_REPL_MRF_PENDING_COUNT_MD.get_full_metric_name(),
            BUCKET_REPL_MRF_PENDING_BYTES_MD.get_full_metric_name(),
            BUCKET_REPL_MRF_DROPPED_COUNT_MD.get_full_metric_name(),
            BUCKET_REPL_MRF_MISSED_COUNT_MD.get_full_metric_name(),
            BUCKET_REPL_MRF_FLUSH_FAILURES_MD.get_full_metric_name(),
            BUCKET_REPL_MRF_LAST_FLUSH_DURATION_MILLIS_MD.get_full_metric_name(),
        ]);
        let mut actual_names = HashSet::new();
        for metric in metrics {
            actual_names.insert(metric.name.to_string());
            assert_eq!(metric.value, 0.0);

            let labels = metric
                .labels
                .into_iter()
                .map(|(key, value)| (key, value.to_string()))
                .collect::<HashMap<_, _>>();
            assert_eq!(labels.get(BUCKET_L).map(String::as_str), Some("photos"));
        }
        assert_eq!(actual_names, expected_names);

        let expired = expire_repl_backlog_zero_tombstones(true, &mut zero_tombstones);
        assert!(expired.is_empty());
        assert_eq!(zero_tombstones.get(&key), Some(&1));

        let expired = expire_repl_backlog_zero_tombstones(true, &mut zero_tombstones);
        assert_eq!(expired, vec![key]);
        assert!(zero_tombstones.is_empty());
    }

    #[test]
    fn repl_backlog_tombstones_stop_zeroing_when_bucket_becomes_live_again() {
        let mut has_seen_valid_snapshot = false;
        let mut prev_live_keys = HashSet::new();
        let mut zero_tombstones = HashMap::new();
        let live_keys = bucket_keys(&["photos"]);

        update_repl_backlog_zero_tombstones(
            true,
            &mut has_seen_valid_snapshot,
            &mut prev_live_keys,
            &mut zero_tombstones,
            live_keys.clone(),
            3,
        );
        update_repl_backlog_zero_tombstones(
            true,
            &mut has_seen_valid_snapshot,
            &mut prev_live_keys,
            &mut zero_tombstones,
            HashSet::new(),
            3,
        );
        assert_eq!(zero_tombstones.get(&bucket_key("photos")), Some(&3));

        update_repl_backlog_zero_tombstones(
            true,
            &mut has_seen_valid_snapshot,
            &mut prev_live_keys,
            &mut zero_tombstones,
            live_keys.clone(),
            3,
        );

        assert!(zero_tombstones.is_empty());
        assert_eq!(prev_live_keys, live_keys);
    }

    #[test]
    fn repl_backlog_tombstones_do_not_advance_when_monitor_unavailable() {
        let mut has_seen_valid_snapshot = true;
        let mut prev_live_keys = bucket_keys(&["photos"]);
        let mut zero_tombstones = HashMap::from([(bucket_key("videos"), 1)]);

        update_repl_backlog_zero_tombstones(
            false,
            &mut has_seen_valid_snapshot,
            &mut prev_live_keys,
            &mut zero_tombstones,
            HashSet::new(),
            3,
        );

        assert!(has_seen_valid_snapshot);
        assert_eq!(prev_live_keys, bucket_keys(&["photos"]));
        assert_eq!(zero_tombstones.get(&bucket_key("videos")), Some(&1));

        let expired = expire_repl_backlog_zero_tombstones(false, &mut zero_tombstones);
        assert!(expired.is_empty());
        assert_eq!(zero_tombstones.get(&bucket_key("videos")), Some(&1));

        update_repl_backlog_zero_tombstones(
            false,
            &mut has_seen_valid_snapshot,
            &mut prev_live_keys,
            &mut zero_tombstones,
            bucket_keys(&["videos"]),
            3,
        );

        assert!(zero_tombstones.is_empty());
        assert_eq!(prev_live_keys, bucket_keys(&["photos"]));
    }

    #[test]
    fn repl_backlog_target_tombstones_zero_removed_targets_then_expire() {
        let mut has_seen_valid_snapshot = false;
        let mut prev_live_keys = HashSet::new();
        let mut zero_tombstones = HashMap::new();
        let key = repl_bw_key("photos", "arn:rustfs:replication:target-a");

        update_series_zero_tombstones(
            &mut has_seen_valid_snapshot,
            &mut prev_live_keys,
            &mut zero_tombstones,
            repl_bw_keys(&[("photos", "arn:rustfs:replication:target-a")]),
            2,
        );
        assert!(has_seen_valid_snapshot);
        assert_eq!(prev_live_keys, repl_bw_keys(&[("photos", "arn:rustfs:replication:target-a")]));
        assert!(zero_tombstones.is_empty());

        update_series_zero_tombstones(&mut has_seen_valid_snapshot, &mut prev_live_keys, &mut zero_tombstones, HashSet::new(), 2);
        assert_eq!(zero_tombstones.get(&key), Some(&2));

        let metrics = collect_repl_backlog_target_zero_tombstone_metrics(&zero_tombstones);
        assert_eq!(metrics.len(), 4);
        let expected_names = HashSet::from([
            BUCKET_REPL_CURRENT_TARGET_BACKLOG_COUNT_MD.get_full_metric_name(),
            BUCKET_REPL_CURRENT_TARGET_BACKLOG_BYTES_MD.get_full_metric_name(),
            BUCKET_REPL_DURABLE_MRF_TARGET_BACKLOG_COUNT_MD.get_full_metric_name(),
            BUCKET_REPL_DURABLE_MRF_TARGET_BACKLOG_BYTES_MD.get_full_metric_name(),
        ]);
        let mut actual_names = HashSet::new();
        for metric in metrics {
            actual_names.insert(metric.name.to_string());
            assert_eq!(metric.value, 0.0);
            let labels = metric
                .labels
                .into_iter()
                .map(|(key, value)| (key, value.to_string()))
                .collect::<HashMap<_, _>>();
            assert_eq!(labels.get(BUCKET_L).map(String::as_str), Some("photos"));
            assert_eq!(labels.get(TARGET_ARN_L).map(String::as_str), Some("arn:rustfs:replication:target-a"));
        }
        assert_eq!(actual_names, expected_names);

        let expired = expire_repl_backlog_target_zero_tombstones(true, &mut zero_tombstones);
        assert!(expired.is_empty());
        assert_eq!(zero_tombstones.get(&key), Some(&1));

        let expired = expire_repl_backlog_target_zero_tombstones(true, &mut zero_tombstones);
        assert_eq!(expired, vec![key]);
        assert!(zero_tombstones.is_empty());
    }

    #[test]
    fn repl_backlog_target_tombstones_do_not_advance_when_target_metrics_unavailable() {
        let key = repl_bw_key("videos", "arn:rustfs:replication:target-b");
        let mut zero_tombstones = HashMap::from([(key.clone(), 1)]);

        let expired = expire_repl_backlog_target_zero_tombstones(false, &mut zero_tombstones);

        assert!(expired.is_empty());
        assert_eq!(zero_tombstones.get(&key), Some(&1));
    }

    #[test]
    fn bucket_tombstones_zero_removed_buckets_then_expire() {
        let mut has_seen_valid_snapshot = false;
        let mut prev_live_keys = HashSet::new();
        let mut zero_tombstones = HashMap::new();
        let live_stats = vec![crate::metrics::collectors::BucketStats {
            name: "tmp".to_string(),
            size_bytes: Some(1024),
            objects_count: Some(8),
            quota_bytes: 2048,
        }];

        update_series_zero_tombstones(
            &mut has_seen_valid_snapshot,
            &mut prev_live_keys,
            &mut zero_tombstones,
            bucket_live_keys(&live_stats),
            2,
        );
        assert!(zero_tombstones.is_empty());

        update_series_zero_tombstones(&mut has_seen_valid_snapshot, &mut prev_live_keys, &mut zero_tombstones, HashSet::new(), 2);
        assert_eq!(zero_tombstones.get("tmp"), Some(&2));

        let metrics = collect_bucket_zero_tombstone_metrics(&zero_tombstones);
        assert_eq!(metrics.len(), 3);
        assert!(metrics.iter().all(|metric| metric.value == 0.0));
        assert!(
            metrics
                .iter()
                .all(|metric| { metric.labels.iter().any(|(key, value)| *key == "bucket" && value == "tmp") })
        );

        let expired = expire_series_zero_tombstones(&mut zero_tombstones);
        assert!(expired.is_empty());
        assert_eq!(zero_tombstones.get("tmp"), Some(&1));

        let expired = expire_series_zero_tombstones(&mut zero_tombstones);
        assert_eq!(expired, vec!["tmp".to_string()]);
        assert!(zero_tombstones.is_empty());
    }

    #[test]
    fn bucket_observation_retirement_distinguishes_unknown_usage_from_deletion() {
        let previous = HashSet::from(["bucket".to_string()]);
        let unknown_stats = vec![crate::metrics::collectors::BucketStats {
            name: "bucket".to_string(),
            size_bytes: None,
            objects_count: None,
            quota_bytes: 1024,
        }];
        let current_buckets = bucket_live_keys(&unknown_stats);
        let current_observations = bucket_observation_live_keys(&unknown_stats);

        assert_eq!(
            bucket_observation_retire_keys(&previous, &current_buckets, &current_observations),
            vec!["bucket".to_string()],
            "an existing bucket with unknown usage must retire its previous usage observations"
        );
        assert!(
            bucket_observation_retire_keys(&previous, &HashSet::new(), &HashSet::new()).is_empty(),
            "a deleted bucket remains governed by the zero-tombstone lifecycle"
        );
    }

    #[test]
    fn unavailable_bucket_snapshot_preserves_metric_series_state() {
        let mut state = BucketSeriesState::default();
        let initial = [crate::metrics::collectors::BucketStats {
            name: "bucket".to_string(),
            size_bytes: Some(512),
            objects_count: Some(2),
            quota_bytes: 1024,
        }];
        assert!(state.observe(Some(&initial), 2).is_some());

        let live_keys = state.live_keys.clone();
        let observation_keys = state.observation_keys.clone();
        let zero_tombstones = state.zero_tombstones.clone();

        assert!(state.observe(None, 2).is_none());
        assert_eq!(state.live_keys, live_keys);
        assert_eq!(state.observation_keys, observation_keys);
        assert_eq!(state.zero_tombstones, zero_tombstones);
    }

    #[test]
    fn parse_system_metrics_interval_rounds_legacy_millis_up_to_one_second() {
        temp_env::with_vars(
            [
                (ENV_SYSTEM_METRICS_INTERVAL, None::<&str>),
                (LEGACY_SYSTEM_METRICS_INTERVAL, Some("500")),
                (ENV_DEFAULT_METRICS_INTERVAL, None::<&str>),
            ],
            || {
                assert_eq!(parse_system_metrics_interval(), Duration::from_secs(1));
            },
        );
    }

    #[test]
    fn parse_system_metrics_interval_rounds_legacy_millis_up() {
        temp_env::with_vars(
            [
                (ENV_SYSTEM_METRICS_INTERVAL, None::<&str>),
                (LEGACY_SYSTEM_METRICS_INTERVAL, Some("1500")),
                (ENV_DEFAULT_METRICS_INTERVAL, None::<&str>),
            ],
            || {
                assert_eq!(parse_system_metrics_interval(), Duration::from_secs(2));
            },
        );
    }
}
