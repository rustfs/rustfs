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
    AuditTargetStats,
    BucketReplicationBandwidthStats,
    NotificationStats,
    NotificationTargetStats,
    // System monitoring collectors (migrated from rustfs-obs::system)
    ProcessAttributeError,
    ProcessCpuStats,
    ProcessDiskStats,
    ProcessMemoryStats,
    collect_audit_metrics,
    collect_bucket_metrics,
    collect_bucket_replication_bandwidth_metrics,
    collect_bucket_replication_metrics,
    collect_bucket_usage_metrics,
    collect_cluster_config_metrics,
    collect_cluster_health_metrics,
    collect_cluster_metrics,
    collect_cluster_usage_metrics,
    collect_cpu_metrics,
    collect_drive_count_metrics,
    collect_drive_detailed_metrics,
    collect_erasure_set_metrics,
    collect_host_network_metrics,
    collect_iam_metrics,
    collect_ilm_metrics,
    collect_memory_metrics,
    collect_network_metrics,
    collect_node_metrics,
    collect_notification_metrics,
    collect_notification_target_metrics,
    collect_process_attributes,
    collect_process_cpu_metrics,
    collect_process_disk_metrics,
    collect_process_memory_metrics,
    collect_process_metrics,
    collect_replication_metrics,
    collect_resource_metrics,
    collect_scanner_metrics,
};
use crate::metrics::config::{
    DEFAULT_AUDIT_METRICS_INTERVAL, DEFAULT_BUCKET_METRICS_INTERVAL, DEFAULT_BUCKET_REPLICATION_BANDWIDTH_METRICS_INTERVAL,
    DEFAULT_CLUSTER_METRICS_INTERVAL, DEFAULT_NODE_METRICS_INTERVAL, DEFAULT_NOTIFICATION_METRICS_INTERVAL,
    DEFAULT_RESOURCE_METRICS_INTERVAL, ENV_AUDIT_METRICS_INTERVAL, ENV_BUCKET_METRICS_INTERVAL,
    ENV_BUCKET_REPLICATION_BANDWIDTH_METRICS_INTERVAL, ENV_CLUSTER_METRICS_INTERVAL, ENV_DEFAULT_METRICS_INTERVAL,
    ENV_NODE_METRICS_INTERVAL, ENV_NOTIFICATION_METRICS_INTERVAL, ENV_RESOURCE_METRICS_INTERVAL,
};
use crate::metrics::report::{PrometheusMetric, report_metrics};
use crate::metrics::schema::bucket_replication::{
    BUCKET_L, BUCKET_REPL_BANDWIDTH_CURRENT_MD, BUCKET_REPL_BANDWIDTH_LIMIT_MD, TARGET_ARN_L,
};
use crate::metrics::stats_collector::{
    ProcessMetricBundle, collect_bucket_replication_bandwidth_stats, collect_bucket_replication_detail_stats,
    collect_bucket_stats, collect_cluster_and_health_stats, collect_cluster_config_stats, collect_cluster_usage_metric_stats,
    collect_disk_and_system_drive_stats, collect_erasure_set_stats, collect_host_network_stats, collect_iam_stats,
    collect_ilm_metric_stats, collect_internode_network_stats, collect_process_metric_bundle, collect_replication_stats,
    collect_scanner_metric_stats, collect_system_cpu_and_memory_stats_with,
};
use crate::storage_compat::ecstore::global::get_global_bucket_monitor;
use rustfs_audit::audit_target_metrics;
use rustfs_notify::{notification_metrics_snapshot, notification_target_metrics};
use rustfs_utils::get_env_opt_u64;
use serde::Serialize;
use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::time::Duration;
use sysinfo::System;
use tokio::time::Instant;
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

/// Default cycles to emit zero for removed replication bandwidth series before letting them expire.
const DEFAULT_REPL_BW_ZERO_TOMBSTONE_CYCLES: u8 = 3;
/// Env var that overrides the zero-emission tombstone cycles for removed replication bandwidth series.
const ENV_REPL_BW_ZERO_TOMBSTONE_CYCLES: &str = "RUSTFS_METRICS_REPL_BW_ZERO_TOMBSTONE_CYCLES";
const METRICS_RUNTIME_SERVICE_NAME: &str = "metrics_runtime";
const METRICS_RUNTIME_COLLECTOR_TASKS: u8 = 10;

type ReplBwKey = (String, String); // (bucket, target_arn)

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
        .or_else(|| get_env_opt_u64(LEGACY_SYSTEM_METRICS_INTERVAL).map(|ms| ms / 1000))
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

fn build_metrics_runtime_status_snapshot(
    metrics_enabled: bool,
    cancellation_requested: bool,
    config: MetricsRuntimeConfig,
) -> MetricsRuntimeStatusSnapshot {
    let state = if !metrics_enabled {
        MetricsRuntimeServiceState::Disabled
    } else if cancellation_requested {
        MetricsRuntimeServiceState::Stopping
    } else {
        MetricsRuntimeServiceState::Running
    };

    MetricsRuntimeStatusSnapshot {
        service: METRICS_RUNTIME_SERVICE_NAME,
        state,
        metrics_enabled,
        collector_tasks: METRICS_RUNTIME_COLLECTOR_TASKS,
        intervals: metrics_runtime_intervals_snapshot(config),
        cancellation_source: MetricsRuntimeCancellationSource::RuntimeToken,
        shutdown_handle: MetricsRuntimeShutdownHandle::RuntimeTokenOnly,
    }
}

fn build_metrics_runtime_desired_snapshot(metrics_enabled: bool, config: MetricsRuntimeConfig) -> MetricsRuntimeDesiredSnapshot {
    let state = if metrics_enabled {
        MetricsRuntimeDesiredState::Enabled
    } else {
        MetricsRuntimeDesiredState::Disabled
    };

    MetricsRuntimeDesiredSnapshot {
        state,
        collector_tasks: METRICS_RUNTIME_COLLECTOR_TASKS,
        intervals: metrics_runtime_intervals_snapshot(config),
    }
}

fn build_metrics_runtime_controller_snapshot(
    metrics_enabled: bool,
    cancellation_requested: bool,
    config: MetricsRuntimeConfig,
) -> MetricsRuntimeControllerSnapshot {
    MetricsRuntimeControllerSnapshot {
        desired: build_metrics_runtime_desired_snapshot(metrics_enabled, config),
        status: build_metrics_runtime_status_snapshot(metrics_enabled, cancellation_requested, config),
    }
}

pub fn metrics_runtime_status_snapshot(token: &CancellationToken) -> MetricsRuntimeStatusSnapshot {
    build_metrics_runtime_status_snapshot(
        crate::observability_metric_enabled(),
        token.is_cancelled(),
        configured_metrics_runtime_config(),
    )
}

pub fn metrics_runtime_controller_snapshot(token: &CancellationToken) -> MetricsRuntimeControllerSnapshot {
    build_metrics_runtime_controller_snapshot(
        crate::observability_metric_enabled(),
        token.is_cancelled(),
        configured_metrics_runtime_config(),
    )
}

fn repl_bw_live_keys(stats: &[BucketReplicationBandwidthStats]) -> HashSet<ReplBwKey> {
    stats.iter().map(|s| (s.bucket.clone(), s.target_arn.clone())).collect()
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

fn expire_repl_bw_zero_tombstones(monitor_available: bool, zero_tombstones: &mut HashMap<ReplBwKey, u8>) {
    if monitor_available && !zero_tombstones.is_empty() {
        zero_tombstones.retain(|_, remaining| {
            if *remaining <= 1 {
                false
            } else {
                *remaining -= 1;
                true
            }
        });
    }
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
    let cluster_interval = config.cluster_interval;
    let bucket_interval = config.bucket_interval;
    let bucket_replication_bandwidth_interval = config.bucket_replication_bandwidth_interval;
    let node_interval = config.node_interval;
    let resource_interval = config.resource_interval;
    let audit_interval = config.audit_interval;
    let notification_interval = config.notification_interval;

    // Spawn task for cluster metrics
    let token_clone = token.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(cluster_interval);
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let (stats, cluster_health) = collect_cluster_and_health_stats().await;
                    let mut metrics = collect_cluster_metrics(&stats);
                    metrics.extend(collect_cluster_health_metrics(&cluster_health));
                    report_metrics(&metrics);
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
        let mut interval = tokio::time::interval(cluster_interval);
        loop {
            tokio::select! {
                _ = interval.tick() => {
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
                        metrics.extend(collect_cluster_usage_metrics(&cluster_usage));
                        metrics.extend(collect_bucket_usage_metrics(&bucket_usage));
                    }

                    if !metrics.is_empty() {
                        report_metrics(&metrics);
                    }
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
        let mut interval = tokio::time::interval(bucket_interval);
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let stats = collect_bucket_stats().await;
                    let metrics = collect_bucket_metrics(&stats);
                    report_metrics(&metrics);
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
        let mut interval = tokio::time::interval(node_interval);
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let (disk_stats, drive_stats, drive_counts) = collect_disk_and_system_drive_stats().await;
                    let mut metrics = collect_node_metrics(&disk_stats);
                    metrics.extend(collect_drive_detailed_metrics(&drive_stats));
                    metrics.extend(collect_drive_count_metrics(&drive_counts));
                    report_metrics(&metrics);
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
        let mut interval = tokio::time::interval(bucket_replication_bandwidth_interval);
        let repl_bw_zero_tombstone_cycles = config.replication_bandwidth_zero_tombstone_cycles;
        let mut prev_live_keys: HashSet<ReplBwKey> = HashSet::new();
        let mut zero_tombstones: HashMap<ReplBwKey, u8> = HashMap::new();
        let mut has_seen_valid_snapshot = false;
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let monitor_available = get_global_bucket_monitor().is_some();
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

                    let bucket_replication = collect_bucket_replication_detail_stats().await;
                    metrics.extend(collect_bucket_replication_metrics(&bucket_replication));
                    let replication = collect_replication_stats().await;
                    metrics.extend(collect_replication_metrics(&replication));
                    report_metrics(&metrics);

                    // Phase-2: after N cycles, stop reporting -> series becomes absent after expiration.
                    expire_repl_bw_zero_tombstones(monitor_available, &mut zero_tombstones);
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
        let mut interval = tokio::time::interval(audit_interval);
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let stats = audit_target_metrics().await
                        .into_iter()
                        .map(|snapshot| AuditTargetStats {
                            failed_messages: snapshot.failed_messages,
                            queue_length: snapshot.queue_length,
                            target_id: snapshot.target_id,
                            total_messages: snapshot.total_messages,
                        })
                        .collect::<Vec<_>>();
                    let metrics = collect_audit_metrics(&stats);
                    report_metrics(&metrics);
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
        let mut interval = tokio::time::interval(notification_interval);
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let snapshot = notification_metrics_snapshot();
                    let mut metrics = collect_notification_metrics(&NotificationStats {
                        current_send_in_progress: snapshot.current_send_in_progress,
                        events_errors_total: snapshot.events_errors_total,
                        events_sent_total: snapshot.events_sent_total,
                        events_skipped_total: snapshot.events_skipped_total,
                    });

                    let target_stats = notification_target_metrics().await
                        .into_iter()
                        .map(|snapshot| NotificationTargetStats {
                            failed_messages: snapshot.failed_messages,
                            queue_length: snapshot.queue_length,
                            target_id: snapshot.target_id,
                            target_type: snapshot.target_type,
                            total_messages: snapshot.total_messages,
                        })
                        .collect::<Vec<_>>();
                    metrics.extend(collect_notification_target_metrics(&target_stats));
                    report_metrics(&metrics);
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
        let mut interval = tokio::time::interval(cluster_interval);
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let mut metrics = Vec::new();

                    if let Some(stats) = collect_ilm_metric_stats().await {
                        metrics.extend(collect_ilm_metrics(&stats));
                    }

                    if let Some(stats) = collect_scanner_metric_stats().await {
                        metrics.extend(collect_scanner_metrics(&stats));
                    }

                    if !metrics.is_empty() {
                        report_metrics(&metrics);
                    }
                }
                _ = token_clone.cancelled() => {
                    warn!(event = EVENT_METRICS_RUNTIME_STATE, component = LOG_COMPONENT_OBS, subsystem = LOG_SUBSYSTEM_METRICS_RUNTIME, collector = "background_workflow_stats", state = "cancelled", "metrics runtime state changed");
                    return;
                }
            }
        }
    });

    // Spawn task for system monitoring metrics (migrated from rustfs-obs::system)
    let system_interval = config.system_interval;

    let token_clone = token.clone();
    tokio::spawn(async move {
        let labels = current_process_metric_labels();
        let mut host_system = System::new_all();
        let process_interval = config.process_interval;
        let mut interval = tokio::time::interval(process_interval);
        let now = Instant::now();
        let mut next_resource_run = now;
        let mut next_system_run = now;

        #[cfg(feature = "gpu")]
        let current_pid = match sysinfo::get_current_pid() {
            Ok(pid) => Some(pid),
            Err(e) => {
                warn!(event = EVENT_METRICS_RUNTIME_STATE, component = LOG_COMPONENT_OBS, subsystem = LOG_SUBSYSTEM_METRICS_RUNTIME, collector = "system_monitoring", result = "current_pid_unavailable", error = %e, "metrics runtime state changed");
                None
            }
        };

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let now = Instant::now();
                    let bundle = collect_process_metric_bundle();

                    if now >= next_resource_run {
                        let mut metrics = collect_resource_metrics(&bundle.resource);
                        metrics.extend(collect_process_metrics(&bundle.process));
                        report_metrics(&metrics);
                        advance_deadline(&mut next_resource_run, resource_interval, now);
                    }

                    if now >= next_system_run {
                        #[cfg(feature = "gpu")]
                        let mut metrics = collect_system_monitoring_metrics(&bundle, &labels, &mut host_system);
                        #[cfg(not(feature = "gpu"))]
                        let metrics = collect_system_monitoring_metrics(&bundle, &labels, &mut host_system);

                        #[cfg(feature = "gpu")]
                        if let Some(pid) = current_pid {
                            use crate::metrics::collectors::{GpuCollector, collect_gpu_metrics};

                            match GpuCollector::new(pid) {
                                Ok(collector) => match collector.collect() {
                                    Ok(gpu_stats) => {
                                        metrics.extend(collect_gpu_metrics(&gpu_stats, &labels));
                                    }
                                    Err(e) => {
                                        warn!(event = EVENT_METRICS_RUNTIME_STATE, component = LOG_COMPONENT_OBS, subsystem = LOG_SUBSYSTEM_METRICS_RUNTIME, collector = "gpu_metrics", result = "collect_failed", error = %e, "metrics runtime state changed");
                                    }
                                },
                                Err(e) => {
                                    warn!(event = EVENT_METRICS_RUNTIME_STATE, component = LOG_COMPONENT_OBS, subsystem = LOG_SUBSYSTEM_METRICS_RUNTIME, collector = "gpu_metrics", result = "collector_init_failed", error = %e, "metrics runtime state changed");
                                }
                            }
                        }

                        report_metrics(&metrics);
                        advance_deadline(&mut next_system_run, system_interval, now);
                    }
                }
                _ = token_clone.cancelled() => {
                    warn!(event = EVENT_METRICS_RUNTIME_STATE, component = LOG_COMPONENT_OBS, subsystem = LOG_SUBSYSTEM_METRICS_RUNTIME, collector = "process_metrics", state = "cancelled", "metrics runtime state changed");
                    return;
                }
            }
        }
    });

    // Spawn task for internode/system network metrics.
    let token_clone = token;
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(system_interval);
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    if let Some(stats) = collect_internode_network_stats() {
                        let metrics = collect_network_metrics(&stats);
                        if !metrics.is_empty() {
                            report_metrics(&metrics);
                        }
                    }
                }
                _ = token_clone.cancelled() => {
                    warn!(event = EVENT_METRICS_RUNTIME_STATE, component = LOG_COMPONENT_OBS, subsystem = LOG_SUBSYSTEM_METRICS_RUNTIME, collector = "internode_network_stats", state = "cancelled", "metrics runtime state changed");
                    return;
                }
            }
        }
    });
}

/// Backward-compatible alias kept during migration.
pub fn init_metrics_collectors(token: CancellationToken) {
    init_metrics_runtime(token);
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

fn current_process_metric_labels() -> Vec<(&'static str, Cow<'static, str>)> {
    match collect_process_attributes() {
        Ok(attrs) => vec![
            ("process_pid", Cow::Owned(attrs.pid.to_string())),
            ("process_executable_name", Cow::Owned(attrs.executable_name)),
        ],
        Err(err) => fallback_process_metric_labels(err),
    }
}

fn fallback_process_metric_labels(err: ProcessAttributeError) -> Vec<(&'static str, Cow<'static, str>)> {
    warn!(event = EVENT_METRICS_RUNTIME_STATE, component = LOG_COMPONENT_OBS, subsystem = LOG_SUBSYSTEM_METRICS_RUNTIME, collector = "process_metric_labels", result = "collect_failed", error = %err, "metrics runtime state changed");
    vec![
        ("process_pid", Cow::Owned(std::process::id().to_string())),
        ("process_executable_name", Cow::Borrowed("unknown")),
    ]
}

fn collect_system_monitoring_metrics(
    bundle: &ProcessMetricBundle,
    labels: &[(&'static str, Cow<'static, str>)],
    host_system: &mut System,
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
    let network_stats = collect_host_network_stats();
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

    fn repl_bw_key(bucket: &str, target_arn: &str) -> ReplBwKey {
        (bucket.to_string(), target_arn.to_string())
    }

    fn repl_bw_keys(keys: &[(&str, &str)]) -> HashSet<ReplBwKey> {
        keys.iter()
            .map(|(bucket, target_arn)| repl_bw_key(bucket, target_arn))
            .collect()
    }

    #[test]
    fn metrics_runtime_status_reports_disabled_state() {
        let snapshot = build_metrics_runtime_status_snapshot(false, false, fixed_metrics_runtime_config());

        assert_eq!(snapshot.service, METRICS_RUNTIME_SERVICE_NAME);
        assert_eq!(snapshot.state, MetricsRuntimeServiceState::Disabled);
        assert!(!snapshot.metrics_enabled);
        assert_eq!(snapshot.collector_tasks, METRICS_RUNTIME_COLLECTOR_TASKS);
        assert_eq!(snapshot.intervals.cluster_interval_secs, 60);
        assert_eq!(snapshot.intervals.bucket_interval_secs, 300);
        assert_eq!(snapshot.intervals.process_interval_secs, 10);
        assert_eq!(snapshot.intervals.replication_bandwidth_zero_tombstone_cycles, 3);
        assert_eq!(snapshot.cancellation_source, MetricsRuntimeCancellationSource::RuntimeToken);
        assert_eq!(snapshot.shutdown_handle, MetricsRuntimeShutdownHandle::RuntimeTokenOnly);
    }

    #[test]
    fn metrics_runtime_status_reports_running_and_stopping_states() {
        let running = build_metrics_runtime_status_snapshot(true, false, fixed_metrics_runtime_config());
        let stopping = build_metrics_runtime_status_snapshot(true, true, fixed_metrics_runtime_config());

        assert_eq!(running.state, MetricsRuntimeServiceState::Running);
        assert_eq!(stopping.state, MetricsRuntimeServiceState::Stopping);
        assert!(running.metrics_enabled);
        assert!(stopping.metrics_enabled);
    }

    #[test]
    fn metrics_runtime_controller_reconcile_is_idempotent() {
        let controller = MetricsRuntimeController;
        let snapshot = build_metrics_runtime_controller_snapshot(true, false, fixed_metrics_runtime_config());

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
        let snapshot = build_metrics_runtime_controller_snapshot(false, false, fixed_metrics_runtime_config());
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

        expire_repl_bw_zero_tombstones(true, &mut zero_tombstones);
        assert_eq!(zero_tombstones.get(&key), Some(&1));

        expire_repl_bw_zero_tombstones(true, &mut zero_tombstones);
        assert!(zero_tombstones.is_empty());
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

        expire_repl_bw_zero_tombstones(false, &mut zero_tombstones);
        assert_eq!(zero_tombstones.get(&repl_bw_key("videos", "arn:rustfs:replication:target-b")), Some(&1));
    }
}
