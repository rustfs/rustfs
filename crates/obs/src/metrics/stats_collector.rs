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

//! Statistics collection functions for metrics.
//!
//! This module contains functions that collect statistics from various
//! RustFS internal sources (storage layer, bucket monitor, system info)
//! and convert them to the Stats structs used by collectors.

use crate::metrics::collectors::scanner::{ScannerActiveBucketDriveStats, ScannerBucketDriveResultStats, ScannerSourceWorkStats};
use crate::metrics::collectors::{
    ApiRequestMetricSupport, ApiRequestStats, BucketReplicationBacklogStats, BucketReplicationBandwidthStats,
    BucketReplicationMetricsSnapshot, BucketReplicationRuntimeStats, BucketReplicationTargetBacklogStats,
    BucketReplicationTargetFlowStats, BucketReplicationTargetStats, BucketStats, BucketUsageStats, ClusterConfigStats,
    ClusterHealthStats, ClusterStats, ClusterUsageStats, CompressionClusterStats, CpuStats, DiskStats, DriveCountStats,
    DriveDetailedStats, DriveRuntimeDetailedStats, ErasureSetStats, HostNetworkStats, IamStats, IlmActionTaskStats,
    IlmBackpressureStats, IlmQueueTaskStats, IlmRuntimeStats, IlmStats, IlmTaskEventStats, MemoryStats, NetworkStats,
    ProcessStats, ProcessStatusType, ReplicationMetricsSnapshot, ResourceStats, ScannerRuntimeStats, ScannerStats,
};
use crate::metrics::runtime_sources::{ObsIlmRuntimeSnapshot, bucket_monitor_handle, iam_metrics_snapshot, ilm_runtime_snapshot};
use crate::metrics::{
    BucketOperations, BucketOptions, ObsBucketReplicationStatsSnapshot, ObsEcstoreResult, ObsStore, StorageAdminApi,
    obs_bucket_replication_stats_snapshot, obs_get_quota_config, obs_get_total_usable_capacity,
    obs_get_total_usable_capacity_free, obs_load_compression_total_from_memory, obs_load_data_usage_from_backend,
    obs_replication_site_stats_snapshot, obs_resolve_object_store_handle,
};
use crate::node_identity::current_local_node_identity;
use jiff::Timestamp;
use rustfs_common::heal_channel::HealScanMode;
use rustfs_common::metrics::{
    ScannerActiveBucketDriveSnapshot, ScannerBucketDriveResultSnapshot, ScannerMetricsReport, ScannerSourceWorkSnapshot,
    global_metrics,
};
use rustfs_io_metrics::internode_metrics::global_internode_metrics;
use rustfs_io_metrics::{
    ProcessResourceSnapshot, ProcessSampler, ProcessStatusSnapshot, ProcessSystemSnapshot, s3_op_metrics_snapshot,
    snapshot_process_resource_and_system, snapshot_process_resource_and_system_with,
};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::SystemTime,
};
use sysinfo::{Networks, System};
use tracing::{instrument, warn};

const LOG_COMPONENT_OBS: &str = "obs";
const LOG_SUBSYSTEM_METRICS_COLLECTOR: &str = "metrics_collector";
const EVENT_METRICS_COLLECTOR_STATE: &str = "metrics_collector_state";

type ObsStorageInfo = <ObsStore as StorageAdminApi>::StorageInfo;
type ObsBackendInfo = <ObsStore as StorageAdminApi>::BackendInfo;

#[derive(Default)]
struct ObsDataUsageInfo {
    last_update: Option<SystemTime>,
    usage_snapshot_complete: bool,
    usage_snapshot_converged: bool,
    buckets_count: u64,
    objects_total_count: u64,
    versions_total_count: u64,
    delete_markers_total_count: u64,
    objects_total_size: u64,
    buckets_usage: HashMap<String, ObsBucketUsageInfo>,
}

#[derive(Default)]
struct ObsBucketUsageInfo {
    size: u64,
    objects_count: u64,
    object_size_histogram: HashMap<String, u64>,
    object_versions_histogram: HashMap<String, u64>,
    versions_count: u64,
    delete_markers_count: u64,
}

#[derive(Debug, Clone, PartialEq)]
struct ObsBucketReplicationBandwidthStats {
    bucket: String,
    target_arn: String,
    limit_bytes_per_sec: i64,
    current_bandwidth_bytes_per_sec: f64,
}

fn usize_to_u64_saturating(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

async fn load_obs_data_usage_from_backend(store: Arc<ObsStore>) -> ObsEcstoreResult<ObsDataUsageInfo> {
    let data_usage = obs_load_data_usage_from_backend(store).await?;
    let usage_snapshot_complete = data_usage.is_complete_bucket_usage_snapshot();

    Ok(ObsDataUsageInfo {
        last_update: data_usage.last_update,
        usage_snapshot_complete,
        usage_snapshot_converged: data_usage.usage_snapshot_converged == Some(true),
        buckets_count: data_usage.buckets_count,
        objects_total_count: data_usage.objects_total_count,
        versions_total_count: data_usage.versions_total_count,
        delete_markers_total_count: data_usage.delete_markers_total_count,
        objects_total_size: data_usage.objects_total_size,
        buckets_usage: data_usage
            .buckets_usage
            .into_iter()
            .map(|(bucket, usage)| {
                (
                    bucket,
                    ObsBucketUsageInfo {
                        size: usage.size,
                        objects_count: usage.objects_count,
                        object_size_histogram: usage.object_size_histogram,
                        object_versions_histogram: usage.object_versions_histogram,
                        versions_count: usage.versions_count,
                        delete_markers_count: usage.delete_markers_count,
                    },
                )
            })
            .collect(),
    })
}

fn bucket_usage_metric_values(data_usage: Option<&ObsDataUsageInfo>, bucket: &str) -> (Option<u64>, Option<u64>) {
    data_usage
        .filter(|usage| usage.usage_snapshot_complete)
        .and_then(|usage| usage.buckets_usage.get(bucket))
        .map(|usage| (Some(usage.size), Some(usage.objects_count)))
        .unwrap_or((None, None))
}

fn data_usage_snapshot_covers_bucket_namespace(data_usage: &ObsDataUsageInfo, buckets: &HashSet<String>) -> bool {
    data_usage.usage_snapshot_complete
        && u64::try_from(buckets.len()).ok() == Some(data_usage.buckets_count)
        && buckets.len() == data_usage.buckets_usage.len()
        && buckets.iter().all(|bucket| data_usage.buckets_usage.contains_key(bucket))
}

fn resolve_obs_object_store_handle() -> Option<Arc<ObsStore>> {
    obs_resolve_object_store_handle()
}

fn scanner_lifecycle_checked_versions(metrics: &ScannerMetricsReport) -> u64 {
    metrics
        .source_work
        .iter()
        .find(|source| source.source == "lifecycle")
        .map(|source| source.checked)
        .unwrap_or_default()
}

fn obs_total_usable_capacity_bytes(storage_info: &ObsStorageInfo) -> u64 {
    usize_to_u64_saturating(obs_get_total_usable_capacity(&storage_info.disks, storage_info))
}

fn obs_total_usable_capacity_free_bytes(storage_info: &ObsStorageInfo) -> u64 {
    usize_to_u64_saturating(obs_get_total_usable_capacity_free(&storage_info.disks, storage_info))
}

fn usable_capacity_used_bytes(usable_capacity: u64, free_bytes: u64) -> u64 {
    usable_capacity.saturating_sub(free_bytes)
}

async fn obs_bucket_quota_limit_bytes(bucket: &str) -> u64 {
    obs_get_quota_config(bucket)
        .await
        .ok()
        .and_then(|(quota, _)| quota.get_quota_limit())
        .unwrap_or(0)
}

fn obs_bucket_replication_bandwidth_stats() -> Option<Vec<ObsBucketReplicationBandwidthStats>> {
    let monitor = bucket_monitor_handle()?;
    Some(
        monitor
            .get_report(|_| true)
            .bucket_stats
            .into_iter()
            .map(|(opts, details)| ObsBucketReplicationBandwidthStats {
                bucket: opts.name,
                target_arn: opts.replication_arn,
                limit_bytes_per_sec: details.limit_bytes_per_sec,
                current_bandwidth_bytes_per_sec: details.current_bandwidth_bytes_per_sec,
            })
            .collect(),
    )
}

async fn obs_ilm_runtime_snapshot() -> ObsIlmRuntimeSnapshot {
    ilm_runtime_snapshot().await
}

async fn obs_bucket_replication_stats_bundle() -> (Vec<BucketReplicationRuntimeStats>, Vec<BucketReplicationBacklogStats>) {
    let snapshots = obs_bucket_replication_stats_snapshot().await;
    let mut detail_stats = Vec::with_capacity(snapshots.len());
    let mut backlog_stats = Vec::with_capacity(snapshots.len());

    for stats in snapshots {
        backlog_stats.push(BucketReplicationBacklogStats {
            bucket: stats.bucket.clone(),
            current_backlog_count: stats.current_backlog_count,
            current_backlog_bytes: stats.current_backlog_bytes,
            durable_mrf_available: stats.durable_mrf_available,
            durable_mrf_backlog_count: stats.durable_mrf_backlog_count,
            durable_mrf_backlog_bytes: stats.durable_mrf_backlog_bytes,
            mrf_pending_count: stats.mrf_pending_count,
            mrf_pending_bytes: stats.mrf_pending_bytes,
            mrf_dropped_count: stats.mrf_dropped_count,
            mrf_missed_count: stats.mrf_missed_count,
            mrf_flush_failures: stats.mrf_flush_failures,
            mrf_last_flush_duration_millis: stats.mrf_last_flush_duration_millis,
            target_backlogs: stats
                .target_backlogs
                .iter()
                .map(|target| BucketReplicationTargetBacklogStats {
                    target_arn: target.target_arn.clone(),
                    current_backlog_count: target.current_backlog_count,
                    current_backlog_bytes: target.current_backlog_bytes,
                    durable_mrf_backlog_count: target.durable_mrf_backlog_count,
                    durable_mrf_backlog_bytes: target.durable_mrf_backlog_bytes,
                })
                .collect(),
        });
        detail_stats.push(bucket_replication_detail_from_snapshot(stats));
    }

    (detail_stats, backlog_stats)
}

fn bucket_replication_detail_from_snapshot(stats: ObsBucketReplicationStatsSnapshot) -> BucketReplicationRuntimeStats {
    let bucket = stats.bucket;
    let (targets, target_flows): (Vec<_>, Vec<_>) = stats
        .targets
        .into_iter()
        .map(|target| {
            (
                BucketReplicationTargetStats {
                    target_arn: target.target_arn.clone(),
                    bandwidth_limit_bytes_per_sec: target.bandwidth_limit_bytes_per_sec,
                    current_bandwidth_bytes_per_sec: target.current_bandwidth_bytes_per_sec,
                    latency_ms: target.latency_ms,
                },
                BucketReplicationTargetFlowStats {
                    target_arn: target.target_arn,
                    sent_bytes: target.sent_bytes,
                    sent_count: target.sent_count,
                    total_failed_bytes: target.total_failed_bytes,
                    total_failed_count: target.total_failed_count,
                    last_min_failed_bytes: target.last_min_failed_bytes,
                    last_min_failed_count: target.last_min_failed_count,
                    last_hour_failed_bytes: target.last_hour_failed_bytes,
                    last_hour_failed_count: target.last_hour_failed_count,
                },
            )
        })
        .unzip();

    BucketReplicationRuntimeStats {
        target_flows,
        stats: BucketReplicationMetricsSnapshot {
            bucket,
            total_failed_bytes: stats.total_failed_bytes,
            total_failed_count: stats.total_failed_count,
            last_min_failed_bytes: stats.last_min_failed_bytes,
            last_min_failed_count: stats.last_min_failed_count,
            last_hour_failed_bytes: stats.last_hour_failed_bytes,
            last_hour_failed_count: stats.last_hour_failed_count,
            sent_bytes: stats.sent_bytes,
            sent_count: stats.sent_count,
            proxied_get_requests_total: stats.proxied_get_requests_total,
            proxied_get_requests_failures: stats.proxied_get_requests_failures,
            proxied_head_requests_total: stats.proxied_head_requests_total,
            proxied_head_requests_failures: stats.proxied_head_requests_failures,
            proxied_put_requests_total: stats.proxied_put_requests_total,
            proxied_put_requests_failures: stats.proxied_put_requests_failures,
            proxied_put_tagging_requests_total: stats.proxied_put_tagging_requests_total,
            proxied_put_tagging_requests_failures: stats.proxied_put_tagging_requests_failures,
            proxied_get_tagging_requests_total: stats.proxied_get_tagging_requests_total,
            proxied_get_tagging_requests_failures: stats.proxied_get_tagging_requests_failures,
            proxied_delete_tagging_requests_total: stats.proxied_delete_tagging_requests_total,
            proxied_delete_tagging_requests_failures: stats.proxied_delete_tagging_requests_failures,
            resync_started_count: stats.resync_started_count,
            resync_completed_count: stats.resync_completed_count,
            resync_failed_count: stats.resync_failed_count,
            resync_canceled_count: stats.resync_canceled_count,
            resync_duration_ms: stats.resync_duration_ms,
            targets,
        },
    }
}

async fn obs_site_replication_stats() -> ReplicationMetricsSnapshot {
    let current_data_transfer_rate = obs_bucket_replication_bandwidth_stats()
        .into_iter()
        .flatten()
        .map(|stat| stat.current_bandwidth_bytes_per_sec)
        .sum::<f64>();
    let stats = obs_replication_site_stats_snapshot(current_data_transfer_rate).await;

    ReplicationMetricsSnapshot {
        average_active_workers: stats.average_active_workers,
        average_queued_bytes: stats.average_queued_bytes,
        average_queued_count: stats.average_queued_count,
        average_data_transfer_rate: stats.average_data_transfer_rate,
        active_workers: stats.active_workers,
        current_data_transfer_rate: stats.current_data_transfer_rate,
        last_minute_queued_bytes: stats.last_minute_queued_bytes,
        last_minute_queued_count: stats.last_minute_queued_count,
        max_active_workers: stats.max_active_workers,
        max_queued_bytes: stats.max_queued_bytes,
        max_queued_count: stats.max_queued_count,
        max_data_transfer_rate: stats.max_data_transfer_rate,
        recent_backlog_count: stats.recent_backlog_count,
    }
}

fn current_scanner_cycle_age_seconds(current_cycle_active: bool, current_started: Timestamp, now: Timestamp) -> u64 {
    if !current_cycle_active {
        0
    } else {
        timestamp_elapsed_seconds_since(now, current_started)
    }
}

fn timestamp_elapsed_seconds_since(now: Timestamp, earlier: Timestamp) -> u64 {
    let duration = now.duration_since(earlier);
    if duration.is_negative() {
        return 0;
    }

    u64::try_from(duration.as_secs()).unwrap_or(u64::MAX)
}

fn scanner_scan_mode_code(scan_mode: &str) -> u64 {
    match scan_mode {
        mode if mode == HealScanMode::Normal.as_str() => HealScanMode::Normal as u8 as u64,
        mode if mode == HealScanMode::Deep.as_str() => HealScanMode::Deep as u8 as u64,
        _ => HealScanMode::Unknown as u8 as u64,
    }
}

fn scanner_work_rate_per_second(count: u64, seconds: f64) -> f64 {
    if seconds > 0.0 && seconds.is_finite() {
        count as f64 / seconds
    } else {
        0.0
    }
}

const DRIVE_STATE_OK: &str = "ok";
const DRIVE_STATE_ONLINE: &str = "online";
const DRIVE_STATE_UNFORMATTED: &str = "unformatted";
const DRIVE_RUNTIME_STATE_RETURNING: &str = "returning";
const CAPACITY_OBSERVATION_LIVE: &str = "live";
const CAPACITY_OBSERVATION_STALE: &str = "stale";
const CAPACITY_OBSERVATION_MISSING: &str = "missing";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ErasureSetQuorumShape {
    data_shards: u32,
    read_quorum: u32,
    write_quorum: u32,
    read_tolerance: u32,
    write_tolerance: u32,
}

fn disk_is_online_for_metrics(state: &str, runtime_state: Option<&str>) -> bool {
    let state_is_acceptable = state.eq_ignore_ascii_case(DRIVE_STATE_OK)
        || state.eq_ignore_ascii_case(DRIVE_STATE_ONLINE)
        || state.eq_ignore_ascii_case(DRIVE_STATE_UNFORMATTED);

    if let Some(runtime_state) = runtime_state {
        let runtime_state_is_acceptable = runtime_state.eq_ignore_ascii_case(DRIVE_STATE_ONLINE)
            || runtime_state.eq_ignore_ascii_case(DRIVE_RUNTIME_STATE_RETURNING);
        return runtime_state_is_acceptable && state_is_acceptable;
    }

    state_is_acceptable
}

fn disk_capacity_observation_state(source: Option<&str>, age_seconds: Option<u64>) -> (&'static str, u64) {
    let age_seconds = age_seconds.unwrap_or(0);
    match source {
        Some("live_probe") => (CAPACITY_OBSERVATION_LIVE, age_seconds),
        Some("snapshot") => (CAPACITY_OBSERVATION_STALE, age_seconds),
        _ => (CAPACITY_OBSERVATION_MISSING, age_seconds),
    }
}

fn disk_topology_label(index: i32) -> Option<String> {
    if index >= 0 { Some(index.to_string()) } else { None }
}

fn non_empty_disk_id(uuid: &str) -> Option<String> {
    let uuid = uuid.trim();
    if uuid.is_empty() { None } else { Some(uuid.to_string()) }
}

fn drive_inode_stats(used_inodes: u64, free_inodes: u64) -> (Option<u64>, Option<u64>, Option<u64>) {
    let total_inodes = used_inodes.saturating_add(free_inodes);
    if total_inodes == 0 {
        (None, None, None)
    } else {
        (Some(used_inodes), Some(free_inodes), Some(total_inodes))
    }
}

fn drive_api_latency_micros(actions: impl Iterator<Item = (u64, u64)>) -> Option<u64> {
    let mut count = 0u64;
    let mut acc_time_ns = 0u64;
    let mut saw_action = false;
    for (action_count, action_acc_time_ns) in actions {
        saw_action = true;
        if action_count > 0 {
            count = count.saturating_add(action_count);
            acc_time_ns = acc_time_ns.saturating_add(action_acc_time_ns);
        }
    }

    saw_action.then(|| acc_time_ns.checked_div(count).unwrap_or_default() / 1_000)
}

fn drive_api_latency_by_api_micros<'a>(actions: impl Iterator<Item = (&'a String, u64, u64)>) -> Vec<(String, u64)> {
    let mut values = actions
        .map(|(api, count, acc_time)| (api.clone(), acc_time.checked_div(count).unwrap_or_default() / 1_000))
        .collect::<Vec<_>>();
    values.sort_by(|left, right| left.0.cmp(&right.0));
    values
}

fn drive_api_calls<'a>(api_calls: impl Iterator<Item = (&'a String, &'a u64)>) -> Vec<(String, u64)> {
    let mut values = api_calls.map(|(api, calls)| (api.clone(), *calls)).collect::<Vec<_>>();
    values.sort_by(|left, right| left.0.cmp(&right.0));
    values
}

fn drive_server_label(endpoint: &str, local_server: &str) -> String {
    endpoint
        .strip_prefix("http://")
        .or_else(|| endpoint.strip_prefix("https://"))
        .and_then(|rest| rest.split('/').next())
        .filter(|authority| !authority.is_empty())
        .unwrap_or(local_server)
        .to_string()
}

fn derive_erasure_set_quorum_shape(set_drive_count: usize, parity: usize) -> ErasureSetQuorumShape {
    let data_shards = set_drive_count.saturating_sub(parity);
    let read_quorum = data_shards.max(1);
    let mut write_quorum = read_quorum;
    if data_shards == parity {
        write_quorum += 1;
    }

    ErasureSetQuorumShape {
        data_shards: data_shards as u32,
        read_quorum: read_quorum as u32,
        write_quorum: write_quorum as u32,
        read_tolerance: parity as u32,
        write_tolerance: set_drive_count.saturating_sub(write_quorum) as u32,
    }
}

fn apply_erasure_set_health(entry: &mut ErasureSetStats) {
    let online = entry.online_drives_count;
    entry.read_health = u8::from(online >= entry.read_quorum);
    entry.write_health = u8::from(online >= entry.write_quorum);
    entry.health = u8::from(entry.write_health == 1);
}

#[derive(Debug, Clone, Default)]
pub struct ProcessMetricBundle {
    pub resource: ResourceStats,
    pub process: ProcessStats,
    pub disk_read_bytes: u64,
    pub disk_write_bytes: u64,
}

/// Collect cluster and cluster-health statistics from a single storage snapshot.
pub async fn collect_cluster_and_health_stats() -> (ClusterStats, ClusterHealthStats) {
    let Some(store) = resolve_obs_object_store_handle() else {
        return (ClusterStats::default(), ClusterHealthStats::default());
    };

    let storage_info = StorageAdminApi::storage_info(store.as_ref()).await;
    let raw_capacity: u64 = storage_info.disks.iter().map(|d| d.total_space).sum();
    let usable_capacity = obs_total_usable_capacity_bytes(&storage_info);
    let free = obs_total_usable_capacity_free_bytes(&storage_info);
    let used = usable_capacity_used_bytes(usable_capacity, free);
    let stale_capacity_drives = storage_info
        .disks
        .iter()
        .filter(|disk| {
            disk_capacity_observation_state(disk.capacity_observation_source.as_deref(), disk.capacity_observation_age_seconds).0
                == CAPACITY_OBSERVATION_STALE
        })
        .count() as u64;
    let missing_capacity_drives = storage_info
        .disks
        .iter()
        .filter(|disk| {
            disk_capacity_observation_state(disk.capacity_observation_source.as_deref(), disk.capacity_observation_age_seconds).0
                == CAPACITY_OBSERVATION_MISSING
        })
        .count() as u64;

    let data_usage = match load_obs_data_usage_from_backend(store).await {
        Ok(data_usage) if data_usage.usage_snapshot_complete => Some(data_usage),
        Ok(_) => None,
        Err(error) => {
            warn!(event = EVENT_METRICS_COLLECTOR_STATE, component = LOG_COMPONENT_OBS, subsystem = LOG_SUBSYSTEM_METRICS_COLLECTOR, collector = "cluster_stats", result = "data_usage_load_failed", error = %error, "metrics collector state changed");
            None
        }
    };
    let buckets_count = data_usage.as_ref().map(|usage| usage.buckets_count);
    let objects_count = data_usage.as_ref().map(|usage| usage.objects_total_count);

    let mut online = 0u64;
    let mut offline = 0u64;
    for disk in &storage_info.disks {
        if disk_is_online_for_metrics(disk.state.as_str(), disk.runtime_state.as_deref()) {
            online += 1;
        } else {
            offline += 1;
        }
    }

    (
        ClusterStats {
            raw_capacity_bytes: raw_capacity,
            usable_capacity_bytes: usable_capacity,
            used_bytes: used,
            free_bytes: free,
            stale_capacity_drives,
            missing_capacity_drives,
            objects_count,
            buckets_count,
        },
        ClusterHealthStats {
            drives_offline_count: offline,
            drives_online_count: online,
            drives_count: storage_info.disks.len() as u64,
        },
    )
}

/// Collect cluster statistics from the storage layer.
#[instrument]
pub async fn collect_cluster_stats() -> ClusterStats {
    let (cluster_stats, _) = collect_cluster_and_health_stats().await;
    cluster_stats
}

/// Collect cluster health statistics from the storage layer.
pub async fn collect_cluster_health_stats() -> ClusterHealthStats {
    let (_, cluster_health_stats) = collect_cluster_and_health_stats().await;
    cluster_health_stats
}

/// Collect bucket statistics from the storage layer.
///
/// `None` means the bucket namespace could not be observed. Callers must keep
/// their prior metric-series state instead of treating that failure as an
/// authoritative empty namespace.
pub async fn collect_bucket_stats() -> Option<Vec<BucketStats>> {
    let store = resolve_obs_object_store_handle()?;

    // Load data usage info from backend to get bucket sizes and object counts
    let data_usage = match load_obs_data_usage_from_backend(store.clone()).await {
        Ok(info) => Some(info),
        Err(e) => {
            warn!(event = EVENT_METRICS_COLLECTOR_STATE, component = LOG_COMPONENT_OBS, subsystem = LOG_SUBSYSTEM_METRICS_COLLECTOR, collector = "bucket_stats", result = "data_usage_load_failed", error = %e, "metrics collector state changed");
            None
        }
    };

    // List all buckets
    let buckets = match store
        .list_bucket(&BucketOptions {
            cached: true,
            ..Default::default()
        })
        .await
    {
        Ok(buckets) => buckets,
        Err(e) => {
            warn!(event = EVENT_METRICS_COLLECTOR_STATE, component = LOG_COMPONENT_OBS, subsystem = LOG_SUBSYSTEM_METRICS_COLLECTOR, collector = "bucket_stats", result = "bucket_list_failed", error = %e, "metrics collector state changed");
            return None;
        }
    };

    let mut stats = Vec::with_capacity(buckets.len());

    for bucket in buckets {
        if bucket.name.starts_with('.') {
            continue;
        }

        // Get size and objects_count from data usage info
        let (size_bytes, objects_count) = bucket_usage_metric_values(data_usage.as_ref(), &bucket.name);

        // Get quota from bucket metadata
        let quota_bytes = obs_bucket_quota_limit_bytes(&bucket.name).await;

        stats.push(BucketStats {
            name: bucket.name,
            size_bytes,
            objects_count,
            quota_bytes,
        });
    }

    Some(stats)
}

/// Collect bucket replication bandwidth stats from the global monitor.
pub fn collect_bucket_replication_bandwidth_stats() -> Vec<BucketReplicationBandwidthStats> {
    let Some(bandwidth_stats) = obs_bucket_replication_bandwidth_stats() else {
        return Vec::new();
    };

    bandwidth_stats
        .into_iter()
        .map(|stat| {
            let target_arn = stat.target_arn;
            let limit_bytes_per_sec = u64::try_from(stat.limit_bytes_per_sec).unwrap_or_else(|_| {
                warn!(event = EVENT_METRICS_COLLECTOR_STATE, component = LOG_COMPONENT_OBS, subsystem = LOG_SUBSYSTEM_METRICS_COLLECTOR, collector = "bucket_replication_bandwidth", result = "invalid_limit_value", target_arn = ?target_arn, limit_value = stat.limit_bytes_per_sec, "metrics collector state changed");
                0
            });

            BucketReplicationBandwidthStats {
                bucket: stat.bucket,
                target_arn,
                limit_bytes_per_sec,
                current_bandwidth_bytes_per_sec: stat.current_bandwidth_bytes_per_sec,
            }
        })
        .collect()
}

/// Collect bucket and target level replication stats from the global replication runtime.
pub async fn collect_bucket_replication_detail_stats() -> Vec<BucketReplicationMetricsSnapshot> {
    obs_bucket_replication_stats_snapshot()
        .await
        .into_iter()
        .map(|snapshot| bucket_replication_detail_from_snapshot(snapshot).stats)
        .collect()
}

pub(crate) async fn collect_bucket_replication_stats_bundle()
-> (Vec<BucketReplicationRuntimeStats>, Vec<BucketReplicationBacklogStats>) {
    obs_bucket_replication_stats_bundle().await
}

/// Collect site-level replication stats from the global replication runtime.
pub async fn collect_replication_stats() -> ReplicationMetricsSnapshot {
    obs_site_replication_stats().await
}

/// Collect S3 API request totals from the in-process operation recorder.
pub(crate) fn collect_api_request_stats() -> Vec<ApiRequestStats> {
    let server = current_local_node_identity();
    s3_op_metrics_snapshot()
        .into_iter()
        .map(|snapshot| ApiRequestStats {
            server: server.clone(),
            name: snapshot.op.to_string(),
            req_type: "s3".to_string(),
            total: snapshot.total,
            supported_metrics: ApiRequestMetricSupport::TOTALS_ONLY,
            ..Default::default()
        })
        .collect()
}

/// Collect disk statistics from the storage layer.
pub async fn collect_disk_stats() -> Vec<DiskStats> {
    let (disk_stats, _, _) = collect_disk_and_system_drive_stats().await;
    disk_stats
}

fn build_system_cpu_stats(system: &System, server: &str) -> CpuStats {
    let cpu_usage = system.global_cpu_usage() as f64;
    let cpu_count = system.cpus().len().max(1) as f64;
    let load_avg = System::load_average().one;

    CpuStats {
        server: server.to_string(),
        avg_idle: (100.0 - cpu_usage).max(0.0),
        load_avg,
        load_avg_perc: (load_avg / cpu_count) * 100.0,
        usage_perc: cpu_usage,
    }
}

fn build_system_memory_stats(system: &System, server: &str) -> MemoryStats {
    let total = system.total_memory();
    let used = system.used_memory();

    MemoryStats {
        server: server.to_string(),
        total,
        used,
        used_perc: if total > 0 {
            (used as f64 / total as f64) * 100.0
        } else {
            0.0
        },
        free: system.free_memory(),
        buffers: 0,
        cache: 0,
        shared: 0,
        available: system.available_memory(),
    }
}

/// Collect system CPU and memory statistics from a shared sysinfo snapshot.
pub fn collect_system_cpu_and_memory_stats() -> (CpuStats, MemoryStats) {
    let mut system = System::new_all();
    collect_system_cpu_and_memory_stats_with(&mut system)
}

/// Collect system CPU and memory statistics by refreshing a reusable sysinfo instance.
pub fn collect_system_cpu_and_memory_stats_with(system: &mut System) -> (CpuStats, MemoryStats) {
    system.refresh_cpu_all();
    system.refresh_memory();
    let server = current_local_node_identity();
    (build_system_cpu_stats(system, &server), build_system_memory_stats(system, &server))
}

/// Collect system CPU statistics from the current host.
pub fn collect_system_cpu_stats() -> CpuStats {
    let (cpu_stats, _) = collect_system_cpu_and_memory_stats();
    cpu_stats
}

/// Collect system memory statistics from the current host.
pub fn collect_system_memory_stats() -> MemoryStats {
    let (_, memory_stats) = collect_system_cpu_and_memory_stats();
    memory_stats
}

/// Collect node disk stats and drive stats from a single storage snapshot.
pub async fn collect_disk_and_system_drive_stats() -> (Vec<DiskStats>, Vec<DriveDetailedStats>, DriveCountStats) {
    let (disk_stats, drive_stats, drive_count_stats) = collect_disk_and_system_drive_runtime_stats().await;
    (disk_stats, drive_stats.into_iter().map(|stat| stat.stats).collect(), drive_count_stats)
}

pub(crate) async fn collect_disk_and_system_drive_runtime_stats()
-> (Vec<DiskStats>, Vec<DriveRuntimeDetailedStats>, DriveCountStats) {
    let Some(store) = resolve_obs_object_store_handle() else {
        return (Vec::new(), Vec::new(), DriveCountStats::default());
    };

    let storage_info = StorageAdminApi::storage_info(store.as_ref()).await;
    let local_server = current_local_node_identity();
    let disk_stats = storage_info
        .disks
        .iter()
        .map(|disk| DiskStats {
            server: drive_server_label(&disk.endpoint, &local_server),
            drive: disk.drive_path.clone(),
            total_bytes: disk.total_space,
            used_bytes: disk.used_space,
            free_bytes: disk.available_space,
        })
        .collect();

    let mut online_count = 0u64;
    let mut offline_count = 0u64;
    let drive_stats = storage_info
        .disks
        .iter()
        .map(|disk| {
            let is_online = disk_is_online_for_metrics(disk.state.as_str(), disk.runtime_state.as_deref());
            let (capacity_observation_state, capacity_observation_age_seconds) = disk_capacity_observation_state(
                disk.capacity_observation_source.as_deref(),
                disk.capacity_observation_age_seconds,
            );
            if is_online {
                online_count += 1;
            } else {
                offline_count += 1;
            }
            let (used_inodes, free_inodes, total_inodes) = drive_inode_stats(disk.used_inodes, disk.free_inodes);

            DriveRuntimeDetailedStats {
                pool_index: disk_topology_label(disk.pool_index),
                set_index: disk_topology_label(disk.set_index),
                drive_index: disk_topology_label(disk.disk_index),
                disk_id: non_empty_disk_id(&disk.uuid),
                runtime_state: Some(disk.runtime_state.as_deref().unwrap_or("unknown").to_ascii_lowercase()),
                healing: disk.healing,
                scanning: disk.scanning,
                offline_duration_seconds: disk.offline_duration_seconds,
                api_calls: disk
                    .metrics
                    .as_ref()
                    .map(|metrics| drive_api_calls(metrics.api_calls.iter()))
                    .unwrap_or_default(),
                api_latency_by_api_micros: disk
                    .metrics
                    .as_ref()
                    .map(|metrics| {
                        drive_api_latency_by_api_micros(
                            metrics
                                .last_minute
                                .iter()
                                .map(|(api, action)| (api, action.count, action.acc_time)),
                        )
                    })
                    .unwrap_or_default(),
                stats: DriveDetailedStats {
                    server: drive_server_label(&disk.endpoint, &local_server),
                    drive: disk.drive_path.clone(),
                    total_bytes: disk.total_space,
                    used_bytes: disk.used_space,
                    free_bytes: disk.available_space,
                    capacity_observation_state,
                    capacity_observation_age_seconds,
                    used_inodes,
                    free_inodes,
                    total_inodes,
                    timeout_errors_total: disk.metrics.as_ref().map(|metrics| metrics.total_errors_timeout),
                    io_errors_total: None,
                    availability_errors_total: disk.metrics.as_ref().map(|metrics| metrics.total_errors_availability),
                    waiting_io: disk.metrics.as_ref().map(|metrics| u64::from(metrics.total_waiting)),
                    api_latency_micros: disk.metrics.as_ref().and_then(|metrics| {
                        drive_api_latency_micros(metrics.last_minute.values().map(|action| (action.count, action.acc_time)))
                    }),
                    health: if is_online { 1 } else { 0 },
                    writes_total: disk.metrics.as_ref().map(|metrics| metrics.total_writes),
                    deletes_total: disk.metrics.as_ref().map(|metrics| metrics.total_deletes),
                    reads_per_sec: None,
                    reads_kb_per_sec: None,
                    reads_await: None,
                    writes_per_sec: None,
                    writes_kb_per_sec: None,
                    writes_await: None,
                    perc_util: None,
                },
            }
        })
        .collect();

    let drive_count_stats = DriveCountStats {
        offline_count,
        online_count,
        total_count: online_count + offline_count,
    };
    (disk_stats, drive_stats, drive_count_stats)
}

/// Collect system drive statistics using the storage layer snapshot.
pub async fn collect_system_drive_stats() -> (Vec<DriveDetailedStats>, DriveCountStats) {
    let (_, drive_stats, drive_count_stats) = collect_disk_and_system_drive_stats().await;
    (drive_stats, drive_count_stats)
}

/// Collect resource and process statistics for the current process in one sysinfo refresh.
#[inline]
pub fn collect_process_metric_bundle() -> ProcessMetricBundle {
    let (resource_snapshot, process_snapshot) = snapshot_process_resource_and_system();
    process_metric_bundle_from_snapshots(resource_snapshot, process_snapshot)
}

#[inline]
pub fn collect_process_metric_bundle_with(sampler: &mut ProcessSampler) -> ProcessMetricBundle {
    let (resource_snapshot, process_snapshot) = snapshot_process_resource_and_system_with(sampler);
    process_metric_bundle_from_snapshots(resource_snapshot, process_snapshot)
}

fn process_metric_bundle_from_snapshots(
    resource_snapshot: ProcessResourceSnapshot,
    process_snapshot: ProcessSystemSnapshot,
) -> ProcessMetricBundle {
    let server = current_local_node_identity();
    let status = match process_snapshot.status {
        ProcessStatusSnapshot::Running => ProcessStatusType::Running,
        ProcessStatusSnapshot::Sleeping => ProcessStatusType::Sleeping,
        ProcessStatusSnapshot::Zombie => ProcessStatusType::Zombie,
        ProcessStatusSnapshot::Other => ProcessStatusType::Other,
    };

    let resource_stats = ResourceStats {
        server: server.clone(),
        cpu_percent: resource_snapshot.cpu_percent,
        memory_bytes: resource_snapshot.memory_bytes,
        uptime_seconds: resource_snapshot.uptime_seconds,
    };
    let process_stats = ProcessStats {
        server,
        locks_read_total: process_snapshot.locks_read_total,
        locks_write_total: process_snapshot.locks_write_total,
        cpu_total_seconds: process_snapshot.cpu_total_seconds,
        file_descriptor_limit_total: process_snapshot.file_descriptor_limit_total,
        file_descriptor_open_total: process_snapshot.file_descriptor_open_total,
        go_routine_total: process_snapshot.go_routine_total,
        io_rchar_bytes: process_snapshot.io_rchar_bytes,
        io_read_bytes: process_snapshot.io_read_bytes,
        io_wchar_bytes: process_snapshot.io_wchar_bytes,
        io_write_bytes: process_snapshot.io_write_bytes,
        resident_memory_bytes: process_snapshot.resident_memory_bytes,
        start_time_seconds: process_snapshot.start_time_seconds,
        status,
        status_value: process_snapshot.status_value,
        syscall_read_total: process_snapshot.syscall_read_total,
        syscall_write_total: process_snapshot.syscall_write_total,
        uptime_seconds: process_snapshot.uptime_seconds,
        virtual_memory_bytes: process_snapshot.virtual_memory_bytes,
        virtual_memory_max_bytes: process_snapshot.virtual_memory_max_bytes,
    };

    ProcessMetricBundle {
        resource: resource_stats,
        process: process_stats,
        disk_read_bytes: process_snapshot.disk_read_bytes,
        disk_write_bytes: process_snapshot.disk_write_bytes,
    }
}

/// Collect resource and process statistics for the current process in one sysinfo refresh.
#[inline]
pub fn collect_process_resource_and_system_stats() -> (ResourceStats, ProcessStats) {
    let bundle = collect_process_metric_bundle();
    (bundle.resource, bundle.process)
}

/// Collect resource statistics for the current process.
#[inline]
pub fn collect_process_stats() -> ResourceStats {
    collect_process_metric_bundle().resource
}

/// Collect process statistics for the current process.
#[inline]
pub fn collect_process_system_stats() -> ProcessStats {
    collect_process_metric_bundle().process
}

/// Collect host network statistics from a refreshed network interface snapshot.
///
/// These counters come from system interfaces and are host-wide, not process-scoped.
pub fn collect_host_network_stats_with(networks: &Networks) -> HostNetworkStats {
    let mut total_received = 0u64;
    let mut total_transmitted = 0u64;
    let mut per_interface = Vec::with_capacity(networks.len());

    for (interface_name, data) in networks {
        let received = data.received();
        let transmitted = data.transmitted();
        total_received += received;
        total_transmitted += transmitted;
        per_interface.push((interface_name.to_string(), received, transmitted));
    }

    HostNetworkStats {
        server: current_local_node_identity(),
        total_received,
        total_transmitted,
        per_interface,
    }
}

/// Collect host network statistics using a persistent `sysinfo::Networks` snapshot.
///
/// `sysinfo` reports network I/O as deltas since the previous refresh, so
/// callers must reuse the same `Networks` instance across collection ticks.
pub fn collect_host_network_stats(networks: &mut Networks) -> HostNetworkStats {
    networks.refresh(true);
    collect_host_network_stats_with(networks)
}

/// Collect internode network metrics from the global internode metrics snapshot.
///
/// The returned values come directly from `global_internode_metrics().snapshot()`
/// and currently include only the counters and dial timing data tracked by the
/// internode metrics runtime.
pub fn collect_internode_network_stats() -> Option<NetworkStats> {
    let snapshot = global_internode_metrics().snapshot();

    Some(NetworkStats {
        server: current_local_node_identity(),
        internode_errors_total: snapshot.errors_total,
        internode_dial_errors_total: snapshot.dial_errors_total,
        internode_dial_avg_time_nanos: snapshot.dial_avg_time_nanos,
        internode_sent_bytes_total: snapshot.sent_bytes_total,
        internode_recv_bytes_total: snapshot.recv_bytes_total,
    })
}

/// Collect cluster config metrics from backend parity configuration.
fn cluster_config_stats_from_backend_parities(
    rr_sc_parity: Option<usize>,
    standard_sc_parity: Option<usize>,
) -> Option<ClusterConfigStats> {
    Some(ClusterConfigStats {
        rrs_parity: u32::try_from(rr_sc_parity?).ok()?,
        standard_parity: u32::try_from(standard_sc_parity?).ok()?,
    })
}

pub async fn collect_cluster_config_stats() -> Option<ClusterConfigStats> {
    let store = resolve_obs_object_store_handle()?;
    let backend = StorageAdminApi::backend_info(store.as_ref()).await;

    cluster_config_stats_from_backend_parities(backend.rr_sc_parity, backend.standard_sc_parity)
}

fn standard_erasure_layout_from_backend(backend: &ObsBackendInfo, pool_idx: usize) -> Option<(usize, usize)> {
    let drives_per_set = backend.drives_per_set.get(pool_idx).copied()?;
    if drives_per_set == 0
        || (!backend.standard_sc_data.is_empty() && backend.standard_sc_data.len() != backend.drives_per_set.len())
        || (!backend.standard_sc_parities.is_empty() && backend.standard_sc_parities.len() != backend.drives_per_set.len())
    {
        return None;
    }

    let has_data = !backend.standard_sc_data.is_empty();
    let has_parities = !backend.standard_sc_parities.is_empty();
    let (data, parity) = match (has_data, has_parities) {
        (true, true) => (
            backend.standard_sc_data.get(pool_idx).copied()?,
            backend.standard_sc_parities.get(pool_idx).copied()?,
        ),
        (true, false) => {
            let data = backend.standard_sc_data.get(pool_idx).copied()?;
            (data, drives_per_set.checked_sub(data)?)
        }
        (false, true) => {
            let parity = backend.standard_sc_parities.get(pool_idx).copied()?;
            (drives_per_set.checked_sub(parity)?, parity)
        }
        (false, false) => {
            let parity = backend.standard_sc_parity?;
            (drives_per_set.checked_sub(parity)?, parity)
        }
    };

    if data == 0 || parity > data || data.checked_add(parity) != Some(drives_per_set) {
        return None;
    }

    Some((data, parity))
}

fn erasure_set_stats_from_backend(storage_info: &ObsStorageInfo, backend: &ObsBackendInfo) -> Vec<ErasureSetStats> {
    let mut grouped: HashMap<(usize, usize), ErasureSetStats> = HashMap::new();

    for disk in &storage_info.disks {
        let (Ok(pool_idx), Ok(set_idx)) = (usize::try_from(disk.pool_index), usize::try_from(disk.set_index)) else {
            continue;
        };
        let Some((_, parity)) = standard_erasure_layout_from_backend(backend, pool_idx) else {
            continue;
        };
        let set_drive_count = backend.drives_per_set[pool_idx];
        let (Ok(pool_id), Ok(set_id), Ok(size), Ok(parity_metric)) = (
            u32::try_from(pool_idx),
            u32::try_from(set_idx),
            u32::try_from(set_drive_count),
            u32::try_from(parity),
        ) else {
            continue;
        };
        let quorum_shape = derive_erasure_set_quorum_shape(set_drive_count, parity);

        let entry = grouped.entry((pool_idx, set_idx)).or_insert_with(|| ErasureSetStats {
            pool_id,
            set_id,
            size,
            parity: parity_metric,
            data_shards: quorum_shape.data_shards,
            read_quorum: quorum_shape.read_quorum,
            write_quorum: quorum_shape.write_quorum,
            online_drives_count: 0,
            healing_drives_count: 0,
            health: 0,
            read_tolerance: quorum_shape.read_tolerance,
            write_tolerance: quorum_shape.write_tolerance,
            read_health: 0,
            write_health: 0,
        });

        if disk_is_online_for_metrics(disk.state.as_str(), disk.runtime_state.as_deref()) {
            entry.online_drives_count += 1;
        }
        if disk.healing {
            entry.healing_drives_count += 1;
        }
    }

    for entry in grouped.values_mut() {
        apply_erasure_set_health(entry);
    }

    let mut stats = grouped.into_values().collect::<Vec<_>>();
    stats.sort_by_key(|stat| (stat.pool_id, stat.set_id));
    stats
}

/// Collect cluster erasure set metrics from storage and backend topology info.
pub async fn collect_erasure_set_stats() -> Vec<ErasureSetStats> {
    let Some(store) = resolve_obs_object_store_handle() else {
        return Vec::new();
    };

    let storage_info = StorageAdminApi::storage_info(store.as_ref()).await;
    let backend = StorageAdminApi::backend_info(store.as_ref()).await;
    erasure_set_stats_from_backend(&storage_info, &backend)
}

pub async fn collect_iam_stats() -> Option<IamStats> {
    let snapshot = iam_metrics_snapshot()?;

    Some(IamStats {
        last_sync_duration_millis: snapshot.last_sync_duration_millis,
        plugin_authn_service_failed_requests_minute: snapshot.plugin_authn_service_failed_requests_minute,
        plugin_authn_service_last_fail_seconds: snapshot.plugin_authn_service_last_fail_seconds,
        plugin_authn_service_last_succ_seconds: snapshot.plugin_authn_service_last_succ_seconds,
        plugin_authn_service_succ_avg_rtt_ms_minute: snapshot.plugin_authn_service_succ_avg_rtt_ms_minute,
        plugin_authn_service_succ_max_rtt_ms_minute: snapshot.plugin_authn_service_succ_max_rtt_ms_minute,
        plugin_authn_service_total_requests_minute: snapshot.plugin_authn_service_total_requests_minute,
        since_last_sync_millis: snapshot.since_last_sync_millis,
        sync_failures: snapshot.sync_failures,
        sync_successes: snapshot.sync_successes,
    })
}

/// Collect cluster and per-bucket usage metrics from backend usage snapshots.
///
/// This reads persisted usage data via `load_data_usage_from_backend()` and
/// builds cluster totals plus per-bucket distributions from the returned
/// histograms. It does not trigger an inline object-data rescan.
pub async fn collect_cluster_usage_metric_stats() -> Option<(ClusterUsageStats, Vec<BucketUsageStats>)> {
    let store = resolve_obs_object_store_handle()?;
    let data_usage = load_obs_data_usage_from_backend(store.clone()).await.ok()?;
    let bucket_namespace = store
        .list_bucket(&BucketOptions {
            cached: true,
            no_metadata: true,
            ..Default::default()
        })
        .await
        .ok()?
        .into_iter()
        .filter(|bucket| !bucket.name.starts_with('.'))
        .map(|bucket| bucket.name)
        .collect::<HashSet<_>>();
    collect_cluster_usage_metric_stats_from_data_usage(data_usage, &bucket_namespace).await
}

async fn collect_cluster_usage_metric_stats_from_data_usage(
    data_usage: ObsDataUsageInfo,
    bucket_namespace: &HashSet<String>,
) -> Option<(ClusterUsageStats, Vec<BucketUsageStats>)> {
    if !data_usage_snapshot_covers_bucket_namespace(&data_usage, bucket_namespace) {
        return None;
    }

    let mut buckets = Vec::with_capacity(data_usage.buckets_usage.len());

    for (bucket_name, usage) in &data_usage.buckets_usage {
        if bucket_name.starts_with('.') {
            continue;
        }

        let quota_bytes = obs_bucket_quota_limit_bytes(bucket_name).await;

        buckets.push(BucketUsageStats {
            bucket: bucket_name.clone(),
            total_bytes: usage.size,
            objects_count: usage.objects_count,
            versions_count: usage.versions_count,
            delete_markers_count: usage.delete_markers_count,
            quota_bytes,
            object_size_distribution: usage
                .object_size_histogram
                .iter()
                .map(|(range, count)| (range.clone(), *count))
                .collect(),
            version_count_distribution: usage
                .object_versions_histogram
                .iter()
                .map(|(range, count)| (range.clone(), *count))
                .collect(),
        });
    }

    buckets.sort_by(|a, b| a.bucket.cmp(&b.bucket));

    Some((
        ClusterUsageStats {
            since_last_update_seconds: crate::metrics::collectors::cluster_usage::usage_since_last_update_seconds(
                data_usage.last_update,
                SystemTime::now(),
            ),
            total_bytes: data_usage.objects_total_size,
            objects_count: data_usage.objects_total_count,
            versions_count: data_usage.versions_total_count,
            delete_markers_count: data_usage.delete_markers_total_count,
            buckets_count: data_usage.buckets_count,
            snapshot_converged: data_usage.usage_snapshot_converged,
            object_size_distribution: data_usage
                .buckets_usage
                .values()
                .flat_map(|usage| usage.object_size_histogram.iter())
                .fold(HashMap::<String, u64>::new(), |mut acc, (range, count)| {
                    *acc.entry(range.clone()).or_default() += *count;
                    acc
                })
                .into_iter()
                .collect(),
            versions_distribution: data_usage
                .buckets_usage
                .values()
                .flat_map(|usage| usage.object_versions_histogram.iter())
                .fold(HashMap::<String, u64>::new(), |mut acc, (range, count)| {
                    *acc.entry(range.clone()).or_default() += *count;
                    acc
                })
                .into_iter()
                .collect(),
        },
        buckets,
    ))
}

fn ilm_action_task_stats(ilm: &ObsIlmRuntimeSnapshot) -> Vec<IlmActionTaskStats> {
    vec![
        IlmActionTaskStats {
            action: "expiry".to_string(),
            state: "pending".to_string(),
            value: ilm.expiry_pending_tasks,
        },
        IlmActionTaskStats {
            action: "transition".to_string(),
            state: "active".to_string(),
            value: ilm.transition_active_tasks,
        },
        IlmActionTaskStats {
            action: "transition".to_string(),
            state: "pending".to_string(),
            value: ilm.transition_pending_tasks,
        },
        IlmActionTaskStats {
            action: "transition".to_string(),
            state: "missed_immediate".to_string(),
            value: ilm.transition_missed_immediate_tasks,
        },
        IlmActionTaskStats {
            action: "transition".to_string(),
            state: "queue_full".to_string(),
            value: ilm.transition_queue_full_tasks,
        },
        IlmActionTaskStats {
            action: "transition".to_string(),
            state: "queue_send_timeout".to_string(),
            value: ilm.transition_queue_send_timeout_tasks,
        },
        IlmActionTaskStats {
            action: "transition".to_string(),
            state: "compensation_scheduled".to_string(),
            value: ilm.transition_compensation_scheduled_tasks,
        },
        IlmActionTaskStats {
            action: "transition".to_string(),
            state: "compensation_running".to_string(),
            value: ilm.transition_compensation_running_tasks,
        },
    ]
}

fn ilm_queue_task_stats(metrics: &ScannerMetricsReport) -> Vec<IlmQueueTaskStats> {
    let expiry = &metrics.lifecycle_expiry;
    let transition = &metrics.lifecycle_transition;
    vec![
        IlmQueueTaskStats {
            action: "expiry".to_string(),
            state: "pending".to_string(),
            value: expiry.current_queued,
        },
        IlmQueueTaskStats {
            action: "expiry".to_string(),
            state: "active".to_string(),
            value: expiry.current_active,
        },
        IlmQueueTaskStats {
            action: "transition".to_string(),
            state: "pending".to_string(),
            value: transition.current_queued,
        },
        IlmQueueTaskStats {
            action: "transition".to_string(),
            state: "active".to_string(),
            value: transition.current_active,
        },
        IlmQueueTaskStats {
            action: "transition".to_string(),
            state: "compensation_running".to_string(),
            value: transition.compensation_running,
        },
    ]
}

fn ilm_task_event_stats(metrics: &ScannerMetricsReport) -> Vec<IlmTaskEventStats> {
    let expiry = &metrics.lifecycle_expiry;
    let transition = &metrics.lifecycle_transition;
    vec![
        IlmTaskEventStats {
            action: "expiry".to_string(),
            result: "queued".to_string(),
            value: expiry.scanner_queued,
        },
        IlmTaskEventStats {
            action: "expiry".to_string(),
            result: "missed".to_string(),
            value: expiry.scanner_missed,
        },
        IlmTaskEventStats {
            action: "expiry".to_string(),
            result: "blocked".to_string(),
            value: expiry.scanner_blocked,
        },
        IlmTaskEventStats {
            action: "expiry".to_string(),
            result: "not_enqueued".to_string(),
            value: expiry.scanner_not_enqueued,
        },
        IlmTaskEventStats {
            action: "expiry".to_string(),
            result: "failed".to_string(),
            value: expiry.delete_failed,
        },
        IlmTaskEventStats {
            action: "transition".to_string(),
            result: "queued".to_string(),
            value: transition.scanner_queued,
        },
        IlmTaskEventStats {
            action: "transition".to_string(),
            result: "missed".to_string(),
            value: transition.scanner_missed,
        },
        IlmTaskEventStats {
            action: "transition".to_string(),
            result: "completed".to_string(),
            value: transition.completed,
        },
        IlmTaskEventStats {
            action: "transition".to_string(),
            result: "failed".to_string(),
            value: transition.failed,
        },
    ]
}

fn ilm_backpressure_stats(metrics: &ScannerMetricsReport) -> Vec<IlmBackpressureStats> {
    vec![
        IlmBackpressureStats {
            action: "expiry".to_string(),
            reason: "queue_missed".to_string(),
            value: metrics.lifecycle_expiry.queue_missed,
        },
        IlmBackpressureStats {
            action: "transition".to_string(),
            reason: "queue_full".to_string(),
            value: metrics.lifecycle_transition.queue_full,
        },
        IlmBackpressureStats {
            action: "transition".to_string(),
            reason: "send_timeout".to_string(),
            value: metrics.lifecycle_transition.queue_send_timeout,
        },
    ]
}

/// Collect ILM metrics from the current lifecycle runtime state.
pub async fn collect_ilm_metric_stats() -> Option<IlmStats> {
    collect_ilm_runtime_metric_stats().await.map(|stats| stats.stats)
}

pub(crate) async fn collect_ilm_runtime_metric_stats() -> Option<IlmRuntimeStats> {
    let ilm = obs_ilm_runtime_snapshot().await;
    let metrics = global_metrics().report().await;
    let versions_scanned = scanner_lifecycle_checked_versions(&metrics);

    Some(IlmRuntimeStats {
        server: current_local_node_identity(),
        action_tasks: ilm_action_task_stats(&ilm),
        queue_tasks: ilm_queue_task_stats(&metrics),
        task_events: ilm_task_event_stats(&metrics),
        backpressure: ilm_backpressure_stats(&metrics),
        versions_scanned,
        stats: IlmStats {
            expiry_pending_tasks: ilm.expiry_pending_tasks,
            transition_active_tasks: ilm.transition_active_tasks,
            transition_pending_tasks: ilm.transition_pending_tasks,
            transition_missed_immediate_tasks: ilm.transition_missed_immediate_tasks,
            transition_queue_full_tasks: ilm.transition_queue_full_tasks,
            transition_queue_send_timeout_tasks: ilm.transition_queue_send_timeout_tasks,
            transition_compensation_scheduled_tasks: ilm.transition_compensation_scheduled_tasks,
            transition_compensation_running_tasks: ilm.transition_compensation_running_tasks,
            versions_scanned,
        },
    })
}

/// Collect scanner metrics from a runtime source.
///
/// Task 5 maps scanner runtime snapshots from `global_metrics()` into the
/// rustfs-obs scanner collector shape.
fn scanner_bucket_scans_started(life_time_ops: &HashMap<String, u64>, bucket_scans_finished: u64) -> u64 {
    life_time_ops
        .get("scan_bucket_drive_start")
        .copied()
        .unwrap_or(bucket_scans_finished)
}

fn scanner_source_work_stats(source_work: &[ScannerSourceWorkSnapshot]) -> Vec<ScannerSourceWorkStats> {
    let mut stats = source_work
        .iter()
        .filter(|work| !work.source.is_empty())
        .map(|work| ScannerSourceWorkStats {
            source: work.source.clone(),
            checked: work.checked,
            queued: work.queued,
            executed: work.executed,
            failed: work.failed,
            skipped: work.skipped,
            missed: work.missed,
        })
        .collect::<Vec<_>>();
    stats.sort_by(|left, right| left.source.cmp(&right.source));
    stats
}

fn scanner_current_cycle_source_work_stats(metrics: &ScannerMetricsReport) -> Vec<ScannerSourceWorkStats> {
    let current = scanner_source_work_stats(&metrics.current_cycle_source_work);
    if !current.is_empty() {
        return current;
    }

    let mut sources = scanner_source_work_stats(&metrics.last_cycle_source_work)
        .into_iter()
        .map(|work| work.source)
        .chain(
            scanner_source_work_stats(&metrics.source_work)
                .into_iter()
                .map(|work| work.source),
        )
        .collect::<Vec<_>>();
    sources.sort();
    sources.dedup();
    sources
        .into_iter()
        .map(|source| ScannerSourceWorkStats {
            source,
            ..Default::default()
        })
        .collect()
}

fn scanner_bucket_drive_result_stats(results: &[ScannerBucketDriveResultSnapshot]) -> Vec<ScannerBucketDriveResultStats> {
    let mut stats = results
        .iter()
        .filter(|result| !result.bucket.is_empty() && !result.drive.is_empty() && !result.result.is_empty() && result.count > 0)
        .map(|result| ScannerBucketDriveResultStats {
            bucket: result.bucket.clone(),
            drive: result.drive.clone(),
            result: result.result.clone(),
            count: result.count,
        })
        .collect::<Vec<_>>();
    stats.sort_by(|left, right| {
        left.bucket
            .cmp(&right.bucket)
            .then_with(|| left.drive.cmp(&right.drive))
            .then_with(|| left.result.cmp(&right.result))
    });
    stats
}

fn scanner_active_bucket_drive_stats(results: &[ScannerActiveBucketDriveSnapshot]) -> Vec<ScannerActiveBucketDriveStats> {
    let mut stats = results
        .iter()
        .filter(|result| !result.source.is_empty() && !result.bucket.is_empty() && !result.drive.is_empty() && result.count > 0)
        .map(|result| ScannerActiveBucketDriveStats {
            source: result.source.clone(),
            bucket: result.bucket.clone(),
            drive: result.drive.clone(),
            count: result.count,
            age_seconds: result.age_seconds,
        })
        .collect::<Vec<_>>();
    stats.sort_by(|left, right| {
        left.source
            .cmp(&right.source)
            .then_with(|| left.bucket.cmp(&right.bucket))
            .then_with(|| left.drive.cmp(&right.drive))
    });
    stats
}

pub async fn collect_scanner_metric_stats() -> Option<ScannerStats> {
    collect_scanner_runtime_metric_stats().await.map(|stats| stats.stats)
}

pub(crate) async fn collect_scanner_runtime_metric_stats() -> Option<ScannerRuntimeStats> {
    let (metrics, runtime_details) = global_metrics().report_with_runtime_details().await;
    let now = Timestamp::now();
    let bucket_scans_finished = metrics.life_time_ops.get("scan_bucket_drive").copied().unwrap_or_default();
    let bucket_scans_started = scanner_bucket_scans_started(&metrics.life_time_ops, bucket_scans_finished);
    let bucket_scans_failed = metrics
        .life_time_ops
        .get("scan_bucket_drive_failure")
        .copied()
        .unwrap_or_default();
    let completed_cycles = metrics.life_time_ops.get("scan_cycle").copied().unwrap_or_default();
    let directories_scanned = metrics.life_time_ops.get("scan_folder").copied().unwrap_or_default();
    let objects_scanned = metrics.life_time_ops.get("scan_object").copied().unwrap_or_default();
    // Real scan coverage: every version the scanner walked, independent of ILM
    // rules. The ILM-checked subset (`scanner_lifecycle_checked_versions`) still
    // feeds the ILM collector's versions_scanned, but the scanner collector must
    // report the total scanned versions — otherwise clusters without lifecycle
    // rules report zero here while objects_scanned keeps climbing.
    let versions_scanned = metrics.versions_scanned;
    let reference_time = metrics.cycles_completed_at.last().copied().unwrap_or(metrics.current_started);
    let last_activity_seconds = timestamp_elapsed_seconds_since(now, reference_time);
    let active_paths = metrics.active_scan_paths as u64;
    let current_cycle_age_seconds = current_scanner_cycle_age_seconds(metrics.current_cycle_active, metrics.current_started, now);
    let current_scan_mode = scanner_scan_mode_code(&metrics.current_scan_mode);
    let current_cycle_age = current_cycle_age_seconds as f64;
    let last_cycle_duration = metrics.last_cycle_duration_seconds;

    Some(ScannerRuntimeStats {
        server: current_local_node_identity(),
        source_work: scanner_source_work_stats(&metrics.source_work),
        current_cycle_source_work: scanner_current_cycle_source_work_stats(&metrics),
        last_cycle_source_work: scanner_source_work_stats(&metrics.last_cycle_source_work),
        bucket_drive_results: scanner_bucket_drive_result_stats(&runtime_details.bucket_drive_results),
        current_cycle_bucket_drive_results: scanner_bucket_drive_result_stats(
            &runtime_details.current_cycle_bucket_drive_results,
        ),
        last_cycle_bucket_drive_results: scanner_bucket_drive_result_stats(&runtime_details.last_cycle_bucket_drive_results),
        active_bucket_drive_scans: scanner_active_bucket_drive_stats(&runtime_details.active_bucket_drive_scans),
        stats: ScannerStats {
            bucket_scans_finished,
            bucket_scans_started,
            bucket_scans_failed,
            directories_scanned,
            objects_scanned,
            versions_scanned,
            last_activity_seconds,
            active_paths,
            oldest_active_path_age_seconds: metrics.oldest_active_path_age_seconds,
            current_set_scan_concurrency_limit: metrics.current_set_scan_concurrency_limit,
            current_set_scans_queued: metrics.current_set_scans_queued,
            current_set_scans_active: metrics.current_set_scans_active,
            current_disk_scan_concurrency_limit: metrics.current_disk_scan_concurrency_limit,
            current_disk_bucket_scans_queued: metrics.current_disk_bucket_scans_queued,
            current_disk_bucket_scans_active: metrics.current_disk_bucket_scans_active,
            throttle_idle_mode_enabled: metrics.throttle_idle_mode_enabled,
            throttle_sleep_factor: metrics.throttle_sleep_factor,
            throttle_max_sleep_seconds: metrics.throttle_max_sleep_seconds,
            yield_every_n_objects: metrics.yield_every_n_objects,
            cycle_interval_seconds: metrics.cycle_interval_seconds,
            cycle_max_duration_seconds: metrics.cycle_max_duration_seconds,
            cycle_max_objects: metrics.cycle_max_objects,
            cycle_max_directories: metrics.cycle_max_directories,
            bitrot_cycle_enabled: metrics.bitrot_cycle_enabled,
            bitrot_cycle_seconds: metrics.bitrot_cycle_seconds,
            current_cycle: metrics.current_cycle,
            completed_cycles,
            current_cycle_age_seconds,
            current_cycle_objects_scanned: metrics.current_cycle_objects_scanned,
            current_cycle_directories_scanned: metrics.current_cycle_directories_scanned,
            current_cycle_bucket_drive_scans: metrics.current_cycle_bucket_drive_scans,
            current_cycle_bucket_drive_failures: metrics.current_cycle_bucket_drive_failures,
            current_cycle_objects_per_second: scanner_work_rate_per_second(
                metrics.current_cycle_objects_scanned,
                current_cycle_age,
            ),
            current_cycle_directories_per_second: scanner_work_rate_per_second(
                metrics.current_cycle_directories_scanned,
                current_cycle_age,
            ),
            current_cycle_bucket_drive_scans_per_second: scanner_work_rate_per_second(
                metrics.current_cycle_bucket_drive_scans,
                current_cycle_age,
            ),
            current_cycle_yield_events: metrics.current_cycle_yield_events,
            current_cycle_yield_duration_seconds: metrics.current_cycle_yield_duration_seconds,
            current_cycle_throttle_sleep_events: metrics.current_cycle_throttle_sleep_events,
            current_cycle_throttle_sleep_duration_seconds: metrics.current_cycle_throttle_sleep_duration_seconds,
            current_cycle_ilm_actions: metrics.current_cycle_ilm_actions,
            current_cycle_heal_objects: metrics.current_cycle_heal_objects,
            current_cycle_replication_checks: metrics.current_cycle_replication_checks,
            current_cycle_usage_saves: metrics.current_cycle_usage_saves,
            current_scan_mode,
            last_cycle_result: metrics.last_cycle_result_code,
            last_cycle_partial_reason: metrics.last_cycle_partial_reason_code,
            last_cycle_duration_seconds: metrics.last_cycle_duration_seconds,
            last_cycle_objects_scanned: metrics.last_cycle_objects_scanned,
            last_cycle_directories_scanned: metrics.last_cycle_directories_scanned,
            last_cycle_bucket_drive_scans: metrics.last_cycle_bucket_drive_scans,
            last_cycle_bucket_drive_failures: metrics.last_cycle_bucket_drive_failures,
            last_cycle_objects_per_second: scanner_work_rate_per_second(metrics.last_cycle_objects_scanned, last_cycle_duration),
            last_cycle_directories_per_second: scanner_work_rate_per_second(
                metrics.last_cycle_directories_scanned,
                last_cycle_duration,
            ),
            last_cycle_bucket_drive_scans_per_second: scanner_work_rate_per_second(
                metrics.last_cycle_bucket_drive_scans,
                last_cycle_duration,
            ),
            last_cycle_yield_events: metrics.last_cycle_yield_events,
            last_cycle_yield_duration_seconds: metrics.last_cycle_yield_duration_seconds,
            last_cycle_throttle_sleep_events: metrics.last_cycle_throttle_sleep_events,
            last_cycle_throttle_sleep_duration_seconds: metrics.last_cycle_throttle_sleep_duration_seconds,
            last_cycle_ilm_actions: metrics.last_cycle_ilm_actions,
            last_cycle_heal_objects: metrics.last_cycle_heal_objects,
            last_cycle_replication_checks: metrics.last_cycle_replication_checks,
            last_cycle_usage_saves: metrics.last_cycle_usage_saves,
            failed_cycles: metrics.failed_cycles,
            superseded_cycles: metrics.superseded_cycles,
            partial_cycles: metrics.partial_cycles,
            partial_cycles_unknown: metrics.partial_cycles_unknown,
            partial_cycles_runtime: metrics.partial_cycles_runtime,
            partial_cycles_objects: metrics.partial_cycles_objects,
            partial_cycles_directories: metrics.partial_cycles_directories,
        },
    })
}

/// Collect cluster-level compression statistics.
pub async fn collect_compression_cluster_stats() -> Option<CompressionClusterStats> {
    let compression_data_usage = obs_load_compression_total_from_memory().await?;

    let original_bytes_total = compression_data_usage.original_bytes_total;
    let compressed_bytes_total = compression_data_usage.compressed_bytes_total;
    let bytes_saved_total = original_bytes_total.saturating_sub(compressed_bytes_total);
    let compression_ratio = if original_bytes_total > 0 {
        compressed_bytes_total as f64 / original_bytes_total as f64
    } else {
        0.0
    };
    let compression_operations_total = compression_data_usage.compression_operations_total;

    Some(CompressionClusterStats {
        original_bytes_total,
        compressed_bytes_total,
        bytes_saved_total,
        compression_ratio,
        compression_operations_total,
    })
}
#[cfg(test)]
mod tests {
    use super::*;
    use rustfs_common::metrics::ScannerSourceWorkSnapshot;
    use std::io::{Read, Write};
    use std::net::{Shutdown, TcpListener, TcpStream};
    use std::thread;
    use std::time::Duration;

    fn storage_info_with_one_online_disk() -> ObsStorageInfo {
        let mut info = ObsStorageInfo::default();
        info.disks.push(Default::default());
        let disk = info.disks.last_mut().expect("inserted disk should exist");
        disk.pool_index = 0;
        disk.set_index = 0;
        disk.disk_index = 0;
        disk.state = DRIVE_STATE_OK.to_string();
        disk.runtime_state = Some(DRIVE_STATE_ONLINE.to_string());
        info
    }

    #[test]
    fn bucket_usage_metrics_distinguish_unknown_from_confirmed_zero() {
        assert_eq!(bucket_usage_metric_values(None, "bucket"), (None, None));

        let mut data_usage = ObsDataUsageInfo {
            usage_snapshot_complete: true,
            ..Default::default()
        };
        data_usage
            .buckets_usage
            .insert("bucket".to_string(), ObsBucketUsageInfo::default());

        assert_eq!(bucket_usage_metric_values(Some(&data_usage), "bucket"), (Some(0), Some(0)));
        assert_eq!(bucket_usage_metric_values(Some(&data_usage), "missing"), (None, None));
    }

    #[tokio::test]
    async fn cluster_usage_metrics_skip_incomplete_snapshot() {
        assert!(
            collect_cluster_usage_metric_stats_from_data_usage(ObsDataUsageInfo::default(), &HashSet::new())
                .await
                .is_none()
        );
    }

    #[tokio::test]
    async fn cluster_usage_metrics_publish_complete_empty_snapshot() {
        let (cluster, buckets) = collect_cluster_usage_metric_stats_from_data_usage(
            ObsDataUsageInfo {
                usage_snapshot_complete: true,
                usage_snapshot_converged: true,
                ..Default::default()
            },
            &HashSet::new(),
        )
        .await
        .expect("complete empty usage should remain publishable");

        assert_eq!(cluster.buckets_count, 0);
        assert_eq!(cluster.objects_count, 0);
        assert_eq!(cluster.total_bytes, 0);
        assert!(cluster.snapshot_converged);
        assert!(buckets.is_empty());
    }

    #[tokio::test]
    async fn cluster_usage_metrics_publish_unknown_convergence_as_unconverged() {
        let (cluster, _) = collect_cluster_usage_metric_stats_from_data_usage(
            ObsDataUsageInfo {
                usage_snapshot_complete: true,
                usage_snapshot_converged: false,
                ..Default::default()
            },
            &HashSet::new(),
        )
        .await
        .expect("complete usage with unknown convergence should remain publishable");

        assert!(!cluster.snapshot_converged);
    }

    #[tokio::test]
    async fn cluster_usage_metrics_skip_snapshot_for_a_different_bucket_namespace() {
        let data_usage = ObsDataUsageInfo {
            usage_snapshot_complete: true,
            buckets_count: 1,
            buckets_usage: HashMap::from([("stale-bucket".to_string(), ObsBucketUsageInfo::default())]),
            ..Default::default()
        };

        assert!(
            collect_cluster_usage_metric_stats_from_data_usage(data_usage, &HashSet::from(["live-bucket".to_string()]))
                .await
                .is_none()
        );
    }

    #[test]
    fn cluster_config_stats_accept_homogeneous_backend_parities() {
        let stats = cluster_config_stats_from_backend_parities(Some(1), Some(2))
            .expect("homogeneous scalar parities should produce cluster config metrics");

        assert_eq!(stats.rrs_parity, 1);
        assert_eq!(stats.standard_parity, 2);
    }

    #[test]
    fn cluster_config_stats_skip_heterogeneous_backend_parities() {
        assert!(cluster_config_stats_from_backend_parities(Some(1), None).is_none());
        assert!(cluster_config_stats_from_backend_parities(None, Some(2)).is_none());
    }

    #[test]
    fn cluster_config_stats_reject_parity_larger_than_u32() {
        let Ok(overflow) = usize::try_from(u64::from(u32::MAX) + 1) else {
            return;
        };

        assert!(cluster_config_stats_from_backend_parities(Some(overflow), Some(2)).is_none());
        assert!(cluster_config_stats_from_backend_parities(Some(1), Some(overflow)).is_none());
    }

    #[tokio::test]
    async fn node_local_resource_stats_use_stable_local_node_identity() {
        let _guard = crate::node_identity::local_node_identity_test_guard().await;
        let previous = rustfs_common::get_global_local_node_name().await;
        rustfs_common::set_global_local_node_name("node1:9000").await;

        let mut system = System::new_all();
        let (cpu, memory) = collect_system_cpu_and_memory_stats_with(&mut system);
        let host_network = collect_host_network_stats_with(&Networks::new());
        let process_bundle =
            process_metric_bundle_from_snapshots(ProcessResourceSnapshot::default(), ProcessSystemSnapshot::default());

        assert_eq!(cpu.server, "node1:9000");
        assert_eq!(memory.server, "node1:9000");
        assert_eq!(host_network.server, "node1:9000");
        assert_eq!(process_bundle.resource.server, "node1:9000");
        assert_eq!(process_bundle.process.server, "node1:9000");

        rustfs_common::set_global_local_node_name(&previous).await;
    }

    #[test]
    fn erasure_set_stats_skip_unknown_backend_layout() {
        let storage_info = storage_info_with_one_online_disk();
        let backend = ObsBackendInfo {
            drives_per_set: vec![8],
            ..Default::default()
        };

        assert!(erasure_set_stats_from_backend(&storage_info, &backend).is_empty());
    }

    #[test]
    fn erasure_set_stats_skip_inconsistent_exact_layout_without_scalar_fallback() {
        let storage_info = storage_info_with_one_online_disk();
        let backend = ObsBackendInfo {
            standard_sc_data: vec![6],
            standard_sc_parities: vec![4],
            standard_sc_parity: Some(2),
            drives_per_set: vec![8],
            ..Default::default()
        };

        assert!(erasure_set_stats_from_backend(&storage_info, &backend).is_empty());
    }

    #[test]
    fn erasure_set_stats_accept_truthful_legacy_scalar_layout() {
        let storage_info = storage_info_with_one_online_disk();
        let backend = ObsBackendInfo {
            standard_sc_parity: Some(2),
            drives_per_set: vec![8],
            ..Default::default()
        };

        let stats = erasure_set_stats_from_backend(&storage_info, &backend);
        assert_eq!(stats.len(), 1);
        assert_eq!(stats[0].size, 8);
        assert_eq!(stats[0].parity, 2);
        assert_eq!(stats[0].data_shards, 6);
    }

    #[test]
    fn erasure_set_stats_exact_data_ignores_stale_scalar() {
        let storage_info = storage_info_with_one_online_disk();
        let backend = ObsBackendInfo {
            standard_sc_data: vec![6],
            standard_sc_parity: Some(4),
            drives_per_set: vec![8],
            ..Default::default()
        };

        let stats = erasure_set_stats_from_backend(&storage_info, &backend);
        assert_eq!(stats.len(), 1);
        assert_eq!(stats[0].parity, 2);
        assert_eq!(stats[0].data_shards, 6);
    }

    fn generate_loopback_traffic() -> std::io::Result<()> {
        let listener = TcpListener::bind(("127.0.0.1", 0))?;
        let addr = listener.local_addr()?;
        let payload = vec![0x5Au8; 64 * 1024];
        let expected_len = payload.len();

        let server = thread::spawn(move || -> std::io::Result<()> {
            let (mut stream, _) = listener.accept()?;
            let mut received = 0usize;
            let mut buf = [0u8; 8192];

            while received < expected_len {
                let read = stream.read(&mut buf)?;
                if read == 0 {
                    break;
                }
                received += read;
            }

            Ok(())
        });

        let mut client = TcpStream::connect(addr)?;
        client.write_all(&payload)?;
        client.flush()?;
        client.shutdown(Shutdown::Write)?;

        server
            .join()
            .expect("loopback traffic server thread should complete successfully")?;
        Ok(())
    }

    #[test]
    fn disk_is_online_for_metrics_accepts_online_state_case_insensitive() {
        assert!(disk_is_online_for_metrics("OnLiNe", Some("online")));
    }

    #[test]
    fn disk_is_online_for_metrics_rejects_offline_runtime_state() {
        assert!(!disk_is_online_for_metrics(DRIVE_STATE_OK, Some("offline")));
    }

    #[test]
    fn disk_topology_label_rejects_unknown_negative_index() {
        assert_eq!(disk_topology_label(-1), None);
        assert_eq!(disk_topology_label(3), Some("3".to_string()));
    }

    #[test]
    fn non_empty_disk_id_rejects_blank_uuid() {
        assert_eq!(non_empty_disk_id("  "), None);
        assert_eq!(non_empty_disk_id("disk-1"), Some("disk-1".to_string()));
    }

    #[test]
    fn drive_server_label_uses_node_identity_for_urls_and_local_paths() {
        assert_eq!(drive_server_label("http://node1:9000/data", "local:9000"), "node1:9000");
        assert_eq!(drive_server_label("https://node2:9443/export/d1", "local:9000"), "node2:9443");
        assert_eq!(drive_server_label("/mnt/data1", "local:9000"), "local:9000");
    }

    #[test]
    fn drive_inode_stats_skip_unknown_zero_inode_totals() {
        assert_eq!(drive_inode_stats(0, 0), (None, None, None));
        assert_eq!(drive_inode_stats(2, 3), (Some(2), Some(3), Some(5)));
    }

    #[test]
    fn drive_api_metrics_are_sorted_and_average_latency() {
        let last_minute = HashMap::from([
            ("write".to_string(), (2, 6_000)),
            ("read".to_string(), (1, 3_000)),
            ("zero".to_string(), (0, 9_000)),
        ]);
        let api_calls = HashMap::from([("write".to_string(), 9), ("read".to_string(), 4)]);

        assert_eq!(drive_api_latency_micros(last_minute.values().copied()), Some(3));
        assert_eq!(
            drive_api_latency_by_api_micros(last_minute.iter().map(|(api, (count, acc_time))| (api, *count, *acc_time))),
            vec![("read".to_string(), 3), ("write".to_string(), 3), ("zero".to_string(), 0)]
        );
        assert_eq!(drive_api_calls(api_calls.iter()), vec![("read".to_string(), 4), ("write".to_string(), 9)]);
    }

    #[test]
    fn drive_api_latency_skips_zero_denominators() {
        let last_minute = HashMap::from([("zero".to_string(), (0, 9_000))]);

        assert_eq!(drive_api_latency_micros(last_minute.values().copied()), Some(0));
        assert_eq!(
            drive_api_latency_by_api_micros(last_minute.iter().map(|(api, (count, acc_time))| (api, *count, *acc_time))),
            vec![("zero".to_string(), 0)]
        );
        assert_eq!(drive_api_latency_micros([].into_iter()), None);
    }

    #[test]
    fn derive_erasure_set_quorum_shape_handles_standard_layout() {
        let shape = derive_erasure_set_quorum_shape(16, 4);

        assert_eq!(
            shape,
            ErasureSetQuorumShape {
                data_shards: 12,
                read_quorum: 12,
                write_quorum: 12,
                read_tolerance: 4,
                write_tolerance: 4,
            }
        );
    }

    #[test]
    fn derive_erasure_set_quorum_shape_handles_equal_data_and_parity() {
        let shape = derive_erasure_set_quorum_shape(4, 2);

        assert_eq!(
            shape,
            ErasureSetQuorumShape {
                data_shards: 2,
                read_quorum: 2,
                write_quorum: 3,
                read_tolerance: 2,
                write_tolerance: 1,
            }
        );
    }

    #[test]
    fn apply_erasure_set_health_marks_read_and_write_health_from_online_count() {
        let mut stats = ErasureSetStats {
            read_quorum: 3,
            write_quorum: 4,
            online_drives_count: 3,
            ..Default::default()
        };

        apply_erasure_set_health(&mut stats);
        assert_eq!(stats.read_health, 1);
        assert_eq!(stats.write_health, 0);
        assert_eq!(stats.health, 0);

        stats.online_drives_count = 4;
        apply_erasure_set_health(&mut stats);
        assert_eq!(stats.read_health, 1);
        assert_eq!(stats.write_health, 1);
        assert_eq!(stats.health, 1);
    }

    #[test]
    fn current_scanner_cycle_age_seconds_returns_zero_when_idle() {
        let now = Timestamp::constant(1_700_000_000, 0);

        assert_eq!(
            current_scanner_cycle_age_seconds(false, now - jiff::SignedDuration::from_secs(30), now),
            0
        );
    }

    #[test]
    fn current_scanner_cycle_age_seconds_clamps_future_start() {
        let now = Timestamp::constant(1_700_000_000, 0);

        assert_eq!(current_scanner_cycle_age_seconds(true, now + jiff::SignedDuration::from_secs(30), now), 0);
    }

    #[test]
    fn current_scanner_cycle_age_seconds_reports_active_first_cycle_elapsed_time() {
        let now = Timestamp::constant(1_700_000_000, 0);

        assert_eq!(
            current_scanner_cycle_age_seconds(true, now - jiff::SignedDuration::from_secs(45), now),
            45
        );
    }

    #[test]
    fn scanner_scan_mode_code_maps_known_modes() {
        assert_eq!(scanner_scan_mode_code(HealScanMode::Normal.as_str()), HealScanMode::Normal as u8 as u64);
        assert_eq!(scanner_scan_mode_code(HealScanMode::Deep.as_str()), HealScanMode::Deep as u8 as u64);
    }

    #[test]
    fn scanner_scan_mode_code_maps_unknown_mode() {
        assert_eq!(scanner_scan_mode_code(""), HealScanMode::Unknown as u8 as u64);
    }

    #[test]
    fn scanner_bucket_scans_started_uses_explicit_started_count() {
        let mut life_time_ops = HashMap::new();
        life_time_ops.insert("scan_bucket_drive_start".to_string(), 7);

        assert_eq!(scanner_bucket_scans_started(&life_time_ops, 5), 7);
    }

    #[test]
    fn scanner_bucket_scans_started_falls_back_to_finished_count() {
        let life_time_ops = HashMap::new();

        assert_eq!(scanner_bucket_scans_started(&life_time_ops, 5), 5);
    }

    #[test]
    fn ilm_action_task_stats_maps_runtime_states() {
        let stats = ilm_action_task_stats(&ObsIlmRuntimeSnapshot {
            expiry_pending_tasks: 1,
            transition_active_tasks: 2,
            transition_pending_tasks: 3,
            transition_missed_immediate_tasks: 4,
            transition_queue_full_tasks: 5,
            transition_queue_send_timeout_tasks: 6,
            transition_compensation_scheduled_tasks: 7,
            transition_compensation_running_tasks: 8,
        });

        assert_eq!(stats.len(), 8);
        assert_eq!(stats[0].action, "expiry");
        assert_eq!(stats[0].state, "pending");
        assert_eq!(stats[0].value, 1);
        assert!(
            stats
                .iter()
                .any(|task| { task.action == "transition" && task.state == "queue_send_timeout" && task.value == 6 })
        );
        assert!(
            stats
                .iter()
                .any(|task| { task.action == "transition" && task.state == "compensation_running" && task.value == 8 })
        );
    }

    #[test]
    fn scanner_lifecycle_checked_versions_uses_lifecycle_checked_source_work() {
        let report = ScannerMetricsReport {
            source_work: vec![
                ScannerSourceWorkSnapshot {
                    source: "usage".to_string(),
                    checked: 11,
                    ..Default::default()
                },
                ScannerSourceWorkSnapshot {
                    source: "lifecycle".to_string(),
                    checked: 37,
                    ..Default::default()
                },
            ],
            life_time_ilm: HashMap::from([("TransitionAction".to_string(), 5), ("DeleteAction".to_string(), 7)]),
            ..Default::default()
        };

        assert_eq!(scanner_lifecycle_checked_versions(&report), 37);
    }

    #[test]
    fn ilm_detail_stats_keep_expiry_and_transition_results_separate() {
        let report = ScannerMetricsReport {
            lifecycle_expiry: rustfs_common::metrics::ScannerLifecycleExpirySnapshot {
                current_queued: 2,
                current_active: 1,
                scanner_queued: 10,
                scanner_missed: 3,
                delete_failed: 4,
                ..Default::default()
            },
            lifecycle_transition: rustfs_common::metrics::ScannerLifecycleTransitionSnapshot {
                current_queued: 5,
                current_active: 6,
                queue_full: 7,
                queue_send_timeout: 8,
                scanner_queued: 11,
                completed: 12,
                failed: 13,
                ..Default::default()
            },
            ..Default::default()
        };

        let queues = ilm_queue_task_stats(&report);
        assert!(
            queues
                .iter()
                .any(|task| task.action == "expiry" && task.state == "pending" && task.value == 2)
        );
        assert!(
            queues
                .iter()
                .any(|task| task.action == "transition" && task.state == "active" && task.value == 6)
        );

        let events = ilm_task_event_stats(&report);
        assert!(
            events
                .iter()
                .any(|event| event.action == "expiry" && event.result == "failed" && event.value == 4)
        );
        assert!(
            events
                .iter()
                .any(|event| event.action == "transition" && event.result == "completed" && event.value == 12)
        );
        assert!(
            events
                .iter()
                .any(|event| event.action == "transition" && event.result == "failed" && event.value == 13)
        );

        let backpressure = ilm_backpressure_stats(&report);
        assert!(
            backpressure
                .iter()
                .any(|event| event.action == "transition" && event.reason == "queue_full" && event.value == 7)
        );
        assert!(
            backpressure
                .iter()
                .any(|event| event.action == "transition" && event.reason == "send_timeout" && event.value == 8)
        );
    }

    #[test]
    fn scanner_source_work_stats_sorts_and_skips_empty_source() {
        let stats = scanner_source_work_stats(&[
            ScannerSourceWorkSnapshot {
                source: "usage".to_string(),
                checked: 11,
                queued: 2,
                executed: 3,
                failed: 4,
                skipped: 5,
                missed: 6,
            },
            ScannerSourceWorkSnapshot {
                checked: 99,
                queued: 99,
                executed: 99,
                failed: 99,
                skipped: 99,
                missed: 99,
                ..Default::default()
            },
            ScannerSourceWorkSnapshot {
                source: "lifecycle".to_string(),
                checked: 21,
                queued: 7,
                executed: 8,
                failed: 9,
                skipped: 10,
                missed: 12,
            },
        ]);

        assert_eq!(stats.len(), 2);
        assert_eq!(stats[0].source, "lifecycle");
        assert_eq!(stats[0].checked, 21);
        assert_eq!(stats[0].missed, 12);
        assert_eq!(stats[1].source, "usage");
        assert_eq!(stats[1].failed, 4);
    }

    #[test]
    fn scanner_current_cycle_source_work_stats_zeroes_idle_sources() {
        let report = ScannerMetricsReport {
            source_work: vec![ScannerSourceWorkSnapshot {
                source: "usage".to_string(),
                checked: 11,
                queued: 2,
                ..Default::default()
            }],
            last_cycle_source_work: vec![ScannerSourceWorkSnapshot {
                source: "lifecycle".to_string(),
                checked: 21,
                queued: 7,
                ..Default::default()
            }],
            ..Default::default()
        };

        let stats = scanner_current_cycle_source_work_stats(&report);

        assert_eq!(stats.len(), 2);
        assert_eq!(stats[0].source, "lifecycle");
        assert_eq!(stats[0].checked, 0);
        assert_eq!(stats[1].source, "usage");
        assert_eq!(stats[1].queued, 0);
    }

    #[test]
    fn scanner_current_cycle_source_work_stats_keeps_active_values() {
        let report = ScannerMetricsReport {
            source_work: vec![ScannerSourceWorkSnapshot {
                source: "usage".to_string(),
                checked: 11,
                ..Default::default()
            }],
            current_cycle_source_work: vec![ScannerSourceWorkSnapshot {
                source: "usage".to_string(),
                checked: 3,
                ..Default::default()
            }],
            ..Default::default()
        };

        let stats = scanner_current_cycle_source_work_stats(&report);

        assert_eq!(stats.len(), 1);
        assert_eq!(stats[0].source, "usage");
        assert_eq!(stats[0].checked, 3);
    }

    #[test]
    fn scanner_lifecycle_checked_versions_defaults_to_zero_when_lifecycle_missing() {
        let report = ScannerMetricsReport {
            source_work: vec![ScannerSourceWorkSnapshot {
                source: "usage".to_string(),
                checked: 11,
                ..Default::default()
            }],
            ..Default::default()
        };

        assert_eq!(scanner_lifecycle_checked_versions(&report), 0);
    }

    #[test]
    fn host_network_stats_require_a_persistent_networks_snapshot() -> std::io::Result<()> {
        if !sysinfo::IS_SUPPORTED_SYSTEM {
            return Ok(());
        }

        let mut persistent_networks = Networks::new();
        persistent_networks.refresh(true);
        let initial_stats = collect_host_network_stats_with(&persistent_networks);
        assert_eq!(
            initial_stats.total_received + initial_stats.total_transmitted,
            0,
            "the first refresh only seeds sysinfo's baseline snapshot"
        );

        match generate_loopback_traffic() {
            Ok(()) => {}
            Err(err) if err.kind() == std::io::ErrorKind::PermissionDenied => {
                return Ok(());
            }
            Err(err) => return Err(err),
        }
        thread::sleep(Duration::from_millis(100));

        let refreshed_stats = collect_host_network_stats(&mut persistent_networks);
        assert!(
            refreshed_stats.total_received > 0 || refreshed_stats.total_transmitted > 0,
            "a persistent Networks instance should report non-zero loopback deltas after traffic"
        );

        let recreated_stats = collect_host_network_stats_with(&Networks::new_with_refreshed_list());
        assert_eq!(
            recreated_stats.total_received + recreated_stats.total_transmitted,
            0,
            "recreating Networks loses the prior refresh baseline and yields zero deltas"
        );
        Ok(())
    }

    #[test]
    fn scanner_work_rate_per_second_reports_rate() {
        assert_eq!(scanner_work_rate_per_second(90, 45.0), 2.0);
    }

    #[test]
    fn scanner_work_rate_per_second_returns_zero_for_invalid_seconds() {
        assert_eq!(scanner_work_rate_per_second(90, 0.0), 0.0);
        assert_eq!(scanner_work_rate_per_second(90, f64::INFINITY), 0.0);
        assert_eq!(scanner_work_rate_per_second(90, f64::NAN), 0.0);
    }

    #[test]
    fn usable_capacity_used_bytes_matches_usable_total_minus_free() {
        assert_eq!(usable_capacity_used_bytes(240, 90), 150);
    }

    #[test]
    fn usable_capacity_used_bytes_saturates_when_free_exceeds_total() {
        assert_eq!(usable_capacity_used_bytes(90, 240), 0);
    }
}
