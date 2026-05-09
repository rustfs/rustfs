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

#![allow(dead_code)]

//! Statistics collection functions for metrics.
//!
//! This module contains functions that collect statistics from various
//! RustFS internal sources (storage layer, bucket monitor, system info)
//! and convert them to the Stats structs used by collectors.

use crate::metrics::collectors::{
    BucketReplicationBandwidthStats, BucketReplicationStats, BucketReplicationTargetStats, BucketStats, BucketUsageStats,
    ClusterConfigStats, ClusterHealthStats, ClusterStats, ClusterUsageStats, CpuStats, DiskStats, DriveCountStats,
    DriveDetailedStats, ErasureSetStats, HostNetworkStats, IamStats, IlmStats, MemoryStats, NetworkStats, ProcessStats,
    ProcessStatusType, ReplicationStats, ResourceStats, ScannerStats,
};
use chrono::Utc;
use rustfs_common::{internode_metrics::global_internode_metrics, metrics::global_metrics};
use rustfs_ecstore::bucket::lifecycle::bucket_lifecycle_ops::{GLOBAL_ExpiryState, GLOBAL_TransitionState};
use rustfs_ecstore::bucket::metadata_sys::get_quota_config;
use rustfs_ecstore::bucket::replication::GLOBAL_REPLICATION_STATS;
use rustfs_ecstore::data_usage::load_data_usage_from_backend;
use rustfs_ecstore::global::get_global_bucket_monitor;
use rustfs_ecstore::pools::{get_total_usable_capacity, get_total_usable_capacity_free};
use rustfs_ecstore::store_api::{BucketOperations, BucketOptions};
use rustfs_ecstore::{StorageAPI, new_object_layer_fn};
use rustfs_iam::{get_global_iam_sys, oidc::oidc_plugin_authn_metrics_snapshot};
use rustfs_io_metrics::{ProcessStatusSnapshot, snapshot_process_resource_and_system};
use std::collections::HashMap;
use std::time::Duration;
use sysinfo::{Networks, System};
use tracing::{instrument, warn};

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
    let Some(store) = new_object_layer_fn() else {
        return (ClusterStats::default(), ClusterHealthStats::default());
    };

    let storage_info = store.storage_info().await;
    let raw_capacity: u64 = storage_info.disks.iter().map(|d| d.total_space).sum();
    let used: u64 = storage_info.disks.iter().map(|d| d.used_space).sum();
    let usable_capacity = get_total_usable_capacity(&storage_info.disks, &storage_info) as u64;
    let free = get_total_usable_capacity_free(&storage_info.disks, &storage_info) as u64;
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

    // Get bucket and object counts from data usage info.
    let (buckets_count, objects_count) = match load_data_usage_from_backend(store.clone()).await {
        Ok(data_usage) => (data_usage.buckets_count, data_usage.objects_total_count),
        Err(e) => {
            warn!("Failed to load data usage from backend: {}", e);
            // Fall back to bucket list for buckets_count, objects_count stays 0.
            let buckets = store
                .list_bucket(&BucketOptions {
                    cached: true,
                    ..Default::default()
                })
                .await
                .unwrap_or_else(|err| {
                    warn!("Failed to list buckets for cluster metrics: {}", err);
                    Vec::new()
                });
            (buckets.len() as u64, 0)
        }
    };

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
pub async fn collect_bucket_stats() -> Vec<BucketStats> {
    let Some(store) = new_object_layer_fn() else {
        return Vec::new();
    };

    // Load data usage info from backend to get bucket sizes and object counts
    let data_usage = match load_data_usage_from_backend(store.clone()).await {
        Ok(info) => Some(info),
        Err(e) => {
            warn!("Failed to load data usage for bucket metrics: {}", e);
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
            warn!("Failed to list buckets for bucket metrics: {}", e);
            return Vec::new();
        }
    };

    let mut stats = Vec::with_capacity(buckets.len());

    for bucket in buckets {
        if bucket.name.starts_with('.') {
            continue;
        }

        // Get size and objects_count from data usage info
        let (size_bytes, objects_count) = data_usage
            .as_ref()
            .and_then(|du| du.buckets_usage.get(&bucket.name))
            .map(|bui| (bui.size, bui.objects_count))
            .unwrap_or((0, 0));

        // Get quota from bucket metadata
        let quota_bytes = match get_quota_config(&bucket.name).await {
            Ok((quota, _)) => quota.get_quota_limit().unwrap_or(0),
            Err(_) => 0, // No quota configured or error
        };

        stats.push(BucketStats {
            name: bucket.name,
            size_bytes,
            objects_count,
            quota_bytes,
        });
    }

    stats
}

/// Collect bucket replication bandwidth stats from the global monitor.
pub fn collect_bucket_replication_bandwidth_stats() -> Vec<BucketReplicationBandwidthStats> {
    let Some(monitor) = get_global_bucket_monitor() else {
        return Vec::new();
    };

    monitor
        .get_report(|_| true)
        .bucket_stats
        .into_iter()
        .map(|(opts, details)| {
            let target_arn = opts.replication_arn;
            let limit_bytes_per_sec = u64::try_from(details.limit_bytes_per_sec).unwrap_or_else(|_| {
                warn!(
                    "Invalid bandwidth limit value for target {:?}: {}",
                    target_arn, details.limit_bytes_per_sec
                );
                0
            });

            BucketReplicationBandwidthStats {
                bucket: opts.name,
                target_arn,
                limit_bytes_per_sec,
                current_bandwidth_bytes_per_sec: details.current_bandwidth_bytes_per_sec,
            }
        })
        .collect()
}

/// Collect bucket and target level replication stats from the global replication runtime.
pub async fn collect_bucket_replication_detail_stats() -> Vec<BucketReplicationStats> {
    let Some(stats) = GLOBAL_REPLICATION_STATS.get() else {
        return Vec::new();
    };

    let all_bucket_stats = stats.get_all().await;
    let mut buckets = Vec::with_capacity(all_bucket_stats.len());

    for (bucket, bucket_stats) in all_bucket_stats {
        let proxy = stats.get_proxy_stats(&bucket).await;
        let mut total_failed_bytes = 0u64;
        let mut total_failed_count = 0u64;
        let mut last_min_failed_bytes = 0u64;
        let mut last_min_failed_count = 0u64;
        let mut last_hour_failed_bytes = 0u64;
        let mut last_hour_failed_count = 0u64;
        let mut sent_bytes = 0u64;
        let mut sent_count = 0u64;
        let mut targets = Vec::with_capacity(bucket_stats.stats.len());

        for (target_arn, target_stats) in bucket_stats.stats {
            total_failed_bytes += target_stats.fail_stats.size.max(0) as u64;
            total_failed_count += target_stats.fail_stats.count.max(0) as u64;

            let last_min = target_stats.fail_stats.recent_since(Duration::from_secs(60));
            last_min_failed_bytes += last_min.size.max(0) as u64;
            last_min_failed_count += last_min.count.max(0) as u64;

            let last_hour = target_stats.fail_stats.recent_since(Duration::from_secs(60 * 60));
            last_hour_failed_bytes += last_hour.size.max(0) as u64;
            last_hour_failed_count += last_hour.count.max(0) as u64;

            sent_bytes += target_stats.replicated_size.max(0) as u64;
            sent_count += target_stats.replicated_count.max(0) as u64;

            targets.push(BucketReplicationTargetStats {
                target_arn,
                bandwidth_limit_bytes_per_sec: target_stats.bandwidth_limit_bytes_per_sec.max(0) as u64,
                current_bandwidth_bytes_per_sec: target_stats.current_bandwidth_bytes_per_sec,
                latency_ms: target_stats.latency.curr,
            });
        }

        buckets.push(BucketReplicationStats {
            bucket,
            total_failed_bytes,
            total_failed_count,
            last_min_failed_bytes,
            last_min_failed_count,
            last_hour_failed_bytes,
            last_hour_failed_count,
            sent_bytes,
            sent_count,
            proxied_get_requests_total: proxy.get_total.max(0) as u64,
            proxied_get_requests_failures: proxy.get_failed.max(0) as u64,
            proxied_head_requests_total: proxy.head_total.max(0) as u64,
            proxied_head_requests_failures: proxy.head_failed.max(0) as u64,
            proxied_put_tagging_requests_total: proxy.put_tag_total.max(0) as u64,
            proxied_put_tagging_requests_failures: proxy.put_tag_failed.max(0) as u64,
            proxied_get_tagging_requests_total: proxy.get_tag_total.max(0) as u64,
            proxied_get_tagging_requests_failures: proxy.get_tag_failed.max(0) as u64,
            proxied_delete_tagging_requests_total: proxy.delete_tag_total.max(0) as u64,
            proxied_delete_tagging_requests_failures: proxy.delete_tag_failed.max(0) as u64,
            targets,
        });
    }

    buckets
}

/// Collect site-level replication stats from the global replication runtime.
pub async fn collect_replication_stats() -> ReplicationStats {
    let Some(stats) = GLOBAL_REPLICATION_STATS.get() else {
        return ReplicationStats::default();
    };

    let site_metrics = stats.get_sr_metrics_for_node().await;
    let current_active_workers = u64::try_from(site_metrics.active_workers.curr).unwrap_or(0);

    let bandwidth_stats = collect_bucket_replication_bandwidth_stats();
    let current_data_transfer_rate = bandwidth_stats
        .iter()
        .map(|stat| stat.current_bandwidth_bytes_per_sec)
        .sum::<f64>();

    let all_bucket_stats = stats.get_all().await;
    let average_data_transfer_rate = all_bucket_stats
        .values()
        .flat_map(|bucket| bucket.stats.values())
        .map(|stat| stat.xfer_rate_lrg.avg + stat.xfer_rate_sml.avg)
        .sum::<f64>();
    let max_data_transfer_rate = all_bucket_stats
        .values()
        .flat_map(|bucket| bucket.stats.values())
        .map(|stat| stat.xfer_rate_lrg.peak + stat.xfer_rate_sml.peak)
        .sum::<f64>();
    let recent_backlog_count = stats
        .mrf_stats
        .values()
        .copied()
        .filter(|value| *value > 0)
        .sum::<i64>()
        .try_into()
        .unwrap_or(0);

    ReplicationStats {
        average_active_workers: site_metrics.active_workers.avg,
        average_queued_bytes: site_metrics.queued.avg.bytes,
        average_queued_count: site_metrics.queued.avg.count,
        average_data_transfer_rate,
        active_workers: current_active_workers,
        current_data_transfer_rate,
        last_minute_queued_bytes: site_metrics.queued.last_minute.bytes.max(0) as u64,
        last_minute_queued_count: site_metrics.queued.last_minute.count.max(0) as u64,
        max_active_workers: u64::try_from(site_metrics.active_workers.max).unwrap_or(0),
        max_queued_bytes: site_metrics.queued.max.bytes.max(0) as u64,
        max_queued_count: site_metrics.queued.max.count.max(0) as u64,
        max_data_transfer_rate,
        recent_backlog_count,
    }
}

/// Collect disk statistics from the storage layer.
pub async fn collect_disk_stats() -> Vec<DiskStats> {
    let (disk_stats, _, _) = collect_disk_and_system_drive_stats().await;
    disk_stats
}

fn build_system_cpu_stats(system: &System) -> CpuStats {
    let cpu_usage = system.global_cpu_usage() as f64;
    let cpu_count = system.cpus().len().max(1) as f64;
    let load_avg = System::load_average().one;

    CpuStats {
        avg_idle: (100.0 - cpu_usage).max(0.0),
        avg_iowait: 0.0,
        load_avg,
        load_avg_perc: (load_avg / cpu_count) * 100.0,
        nice: 0.0,
        steal: 0.0,
        system: cpu_usage,
        user: 0.0,
    }
}

fn build_system_memory_stats(system: &System) -> MemoryStats {
    let total = system.total_memory();
    let used = system.used_memory();

    MemoryStats {
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
    (build_system_cpu_stats(system), build_system_memory_stats(system))
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
    let Some(store) = new_object_layer_fn() else {
        return (Vec::new(), Vec::new(), DriveCountStats::default());
    };

    let storage_info = store.storage_info().await;
    let disk_stats = storage_info
        .disks
        .iter()
        .map(|disk| DiskStats {
            server: disk.endpoint.clone(),
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

            DriveDetailedStats {
                server: disk.endpoint.clone(),
                drive: disk.drive_path.clone(),
                total_bytes: disk.total_space,
                used_bytes: disk.used_space,
                free_bytes: disk.available_space,
                capacity_observation_state,
                capacity_observation_age_seconds,
                used_inodes: 0,
                free_inodes: 0,
                total_inodes: 0,
                timeout_errors_total: 0,
                io_errors_total: 0,
                availability_errors_total: 0,
                waiting_io: 0,
                api_latency_micros: 0,
                health: if is_online { 1 } else { 0 },
                reads_per_sec: 0.0,
                reads_kb_per_sec: 0.0,
                reads_await: 0.0,
                writes_per_sec: 0.0,
                writes_kb_per_sec: 0.0,
                writes_await: 0.0,
                perc_util: if disk.total_space > 0 {
                    (disk.used_space as f64 / disk.total_space as f64) * 100.0
                } else {
                    0.0
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
    let status = match process_snapshot.status {
        ProcessStatusSnapshot::Running => ProcessStatusType::Running,
        ProcessStatusSnapshot::Sleeping => ProcessStatusType::Sleeping,
        ProcessStatusSnapshot::Zombie => ProcessStatusType::Zombie,
        ProcessStatusSnapshot::Other => ProcessStatusType::Other,
    };

    let resource_stats = ResourceStats {
        cpu_percent: resource_snapshot.cpu_percent,
        memory_bytes: resource_snapshot.memory_bytes,
        uptime_seconds: resource_snapshot.uptime_seconds,
    };
    let process_stats = ProcessStats {
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

/// Collect host network statistics from the current network interface snapshot.
///
/// These counters come from system interfaces and are host-wide, not process-scoped.
pub fn collect_host_network_stats() -> HostNetworkStats {
    let networks = Networks::new_with_refreshed_list();
    let mut total_received = 0u64;
    let mut total_transmitted = 0u64;
    let mut per_interface = Vec::with_capacity(networks.len());

    for (interface_name, data) in &networks {
        let received = data.received();
        let transmitted = data.transmitted();
        total_received += received;
        total_transmitted += transmitted;
        per_interface.push((interface_name.to_string(), received, transmitted));
    }

    HostNetworkStats {
        total_received,
        total_transmitted,
        per_interface,
    }
}

/// Collect internode network metrics from the global internode metrics snapshot.
///
/// The returned values come directly from `global_internode_metrics().snapshot()`
/// and currently include only the counters and dial timing data tracked by the
/// internode metrics runtime.
pub fn collect_internode_network_stats() -> Option<NetworkStats> {
    let snapshot = global_internode_metrics().snapshot();

    Some(NetworkStats {
        internode_errors_total: snapshot.errors_total,
        internode_dial_errors_total: snapshot.dial_errors_total,
        internode_dial_avg_time_nanos: snapshot.dial_avg_time_nanos,
        internode_sent_bytes_total: snapshot.sent_bytes_total,
        internode_recv_bytes_total: snapshot.recv_bytes_total,
    })
}

/// Collect cluster config metrics from backend parity configuration.
pub async fn collect_cluster_config_stats() -> Option<ClusterConfigStats> {
    let store = new_object_layer_fn()?;
    let backend = store.backend_info().await;

    Some(ClusterConfigStats {
        rrs_parity: backend.rr_sc_parity.unwrap_or_default() as u32,
        standard_parity: backend.standard_sc_parity.unwrap_or_default() as u32,
    })
}

/// Collect cluster erasure set metrics from storage and backend topology info.
pub async fn collect_erasure_set_stats() -> Vec<ErasureSetStats> {
    let Some(store) = new_object_layer_fn() else {
        return Vec::new();
    };

    let storage_info = store.storage_info().await;
    let backend = store.backend_info().await;
    let mut grouped: HashMap<(usize, usize), ErasureSetStats> = HashMap::new();

    for disk in &storage_info.disks {
        let pool_idx = disk.pool_index.max(0) as usize;
        let set_idx = disk.set_index.max(0) as usize;
        let set_drive_count = backend.drives_per_set.get(pool_idx).copied().unwrap_or_default();
        let parity = backend
            .standard_sc_parities
            .get(pool_idx)
            .copied()
            .or(backend.standard_sc_parity)
            .unwrap_or(set_drive_count / 2);
        let quorum_shape = derive_erasure_set_quorum_shape(set_drive_count, parity);

        let entry = grouped.entry((pool_idx, set_idx)).or_insert_with(|| ErasureSetStats {
            pool_id: pool_idx as u32,
            set_id: set_idx as u32,
            size: set_drive_count as u32,
            parity: parity as u32,
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

pub async fn collect_iam_stats() -> Option<IamStats> {
    let iam_sys = get_global_iam_sys()?;
    let sync = iam_sys.sync_metrics_snapshot();
    let oidc = oidc_plugin_authn_metrics_snapshot();

    Some(IamStats {
        last_sync_duration_millis: sync.last_sync_duration_millis,
        plugin_authn_service_failed_requests_minute: oidc.failed_requests_minute,
        plugin_authn_service_last_fail_seconds: oidc.last_fail_seconds,
        plugin_authn_service_last_succ_seconds: oidc.last_succ_seconds,
        plugin_authn_service_succ_avg_rtt_ms_minute: oidc.succ_avg_rtt_ms_minute,
        plugin_authn_service_succ_max_rtt_ms_minute: oidc.succ_max_rtt_ms_minute,
        plugin_authn_service_total_requests_minute: oidc.total_requests_minute,
        since_last_sync_millis: sync.since_last_sync_millis,
        sync_failures: sync.sync_failures,
        sync_successes: sync.sync_successes,
    })
}

/// Collect cluster and per-bucket usage metrics from backend usage snapshots.
///
/// This reads persisted usage data via `load_data_usage_from_backend()` and
/// builds cluster totals plus per-bucket distributions from the returned
/// histograms. It does not trigger an inline object-data rescan.
pub async fn collect_cluster_usage_metric_stats() -> Option<(ClusterUsageStats, Vec<BucketUsageStats>)> {
    let store = new_object_layer_fn()?;
    let data_usage = load_data_usage_from_backend(store.clone()).await.ok()?;
    let mut buckets = Vec::with_capacity(data_usage.buckets_usage.len());

    for (bucket_name, usage) in &data_usage.buckets_usage {
        if bucket_name.starts_with('.') {
            continue;
        }

        let quota_bytes = match get_quota_config(bucket_name).await {
            Ok((quota, _)) => quota.get_quota_limit().unwrap_or(0),
            Err(_) => 0,
        };

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
            total_bytes: data_usage.objects_total_size,
            objects_count: data_usage.objects_total_count,
            versions_count: data_usage.versions_total_count,
            delete_markers_count: data_usage.delete_markers_total_count,
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

/// Collect ILM metrics from the current lifecycle runtime state.
pub async fn collect_ilm_metric_stats() -> Option<IlmStats> {
    let expiry_pending_tasks = GLOBAL_ExpiryState.read().await.pending_tasks().await as u64;
    let transition_active_tasks = GLOBAL_TransitionState.active_tasks().max(0) as u64;
    let transition_pending_tasks = GLOBAL_TransitionState.pending_tasks() as u64;
    let transition_missed_immediate_tasks = GLOBAL_TransitionState.missed_immediate_tasks().max(0) as u64;
    let metrics = global_metrics().report().await;
    let versions_scanned = metrics.life_time_ilm.values().copied().sum();

    Some(IlmStats {
        expiry_pending_tasks,
        transition_active_tasks,
        transition_pending_tasks,
        transition_missed_immediate_tasks,
        versions_scanned,
    })
}

/// Collect scanner metrics from a runtime source.
///
/// Task 5 maps scanner runtime snapshots from `global_metrics()` into the
/// rustfs-obs scanner collector shape.
pub async fn collect_scanner_metric_stats() -> Option<ScannerStats> {
    let metrics = global_metrics().report().await;
    let bucket_scans_finished = metrics.life_time_ops.get("scan_bucket_drive").copied().unwrap_or_default();
    let directories_scanned = metrics.life_time_ops.get("scan_folder").copied().unwrap_or_default();
    let objects_scanned = metrics.life_time_ops.get("scan_object").copied().unwrap_or_default();
    let versions_scanned = metrics.life_time_ilm.values().copied().sum();
    let reference_time = metrics.cycles_completed_at.last().copied().unwrap_or(metrics.current_started);
    let last_activity_seconds = Utc::now().signed_duration_since(reference_time).num_seconds().max(0) as u64;

    Some(ScannerStats {
        bucket_scans_finished,
        // `global_metrics()` currently tracks completed bucket-drive scans, not a
        // separate started counter. Mirror the finished count until Task 5/Task 10
        // expands the scanner runtime source shape.
        bucket_scans_started: bucket_scans_finished,
        directories_scanned,
        objects_scanned,
        versions_scanned,
        last_activity_seconds,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disk_is_online_for_metrics_accepts_online_state_case_insensitive() {
        assert!(disk_is_online_for_metrics("OnLiNe", Some("online")));
    }

    #[test]
    fn disk_is_online_for_metrics_rejects_offline_runtime_state() {
        assert!(!disk_is_online_for_metrics(DRIVE_STATE_OK, Some("offline")));
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
}
