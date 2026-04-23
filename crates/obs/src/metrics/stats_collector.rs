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
    BucketReplicationBandwidthStats, BucketStats, ClusterHealthStats, ClusterStats, CpuStats, DiskStats, DriveCountStats,
    DriveDetailedStats, HostNetworkStats, MemoryStats, ProcessStats, ProcessStatusType, ResourceStats,
};
use rustfs_common::heal_channel::DriveState;
use rustfs_ecstore::bucket::metadata_sys::get_quota_config;
use rustfs_ecstore::data_usage::load_data_usage_from_backend;
use rustfs_ecstore::global::get_global_bucket_monitor;
use rustfs_ecstore::pools::{get_total_usable_capacity, get_total_usable_capacity_free};
use rustfs_ecstore::store_api::{BucketOperations, BucketOptions};
use rustfs_ecstore::{StorageAPI, new_object_layer_fn};
use rustfs_io_metrics::{ProcessStatusSnapshot, snapshot_process_resource_and_system};
use sysinfo::{Networks, System};
use tracing::{instrument, warn};

#[derive(Debug, Clone, Default)]
pub struct ProcessMetricBundle {
    pub resource: ResourceStats,
    pub process: ProcessStats,
    pub disk_read_bytes: u64,
    pub disk_write_bytes: u64,
}

/// Collect cluster statistics from the storage layer.
#[instrument]
pub async fn collect_cluster_stats() -> ClusterStats {
    let Some(store) = new_object_layer_fn() else {
        return ClusterStats::default();
    };

    let storage_info = store.storage_info().await;

    let raw_capacity: u64 = storage_info.disks.iter().map(|d| d.total_space).sum();
    let used: u64 = storage_info.disks.iter().map(|d| d.used_space).sum();
    let usable_capacity = get_total_usable_capacity(&storage_info.disks, &storage_info) as u64;
    let free = get_total_usable_capacity_free(&storage_info.disks, &storage_info) as u64;

    // Get bucket and object counts from data usage info
    let (buckets_count, objects_count) = match load_data_usage_from_backend(store.clone()).await {
        Ok(data_usage) => (data_usage.buckets_count, data_usage.objects_total_count),
        Err(e) => {
            warn!("Failed to load data usage from backend: {}", e);
            // Fall back to bucket list for buckets_count, objects_count stays 0
            let buckets = store
                .list_bucket(&BucketOptions {
                    cached: true,
                    ..Default::default()
                })
                .await
                .unwrap_or_else(|e| {
                    warn!("Failed to list buckets for cluster metrics: {}", e);
                    Vec::new()
                });
            (buckets.len() as u64, 0)
        }
    };

    ClusterStats {
        raw_capacity_bytes: raw_capacity,
        usable_capacity_bytes: usable_capacity,
        used_bytes: used,
        free_bytes: free,
        objects_count,
        buckets_count,
    }
}

/// Collect cluster health statistics from the storage layer.
pub async fn collect_cluster_health_stats() -> ClusterHealthStats {
    let Some(store) = new_object_layer_fn() else {
        return ClusterHealthStats::default();
    };

    let storage_info = store.storage_info().await;
    let mut online = 0u64;
    let mut offline = 0u64;

    for disk in &storage_info.disks {
        let state = disk.state.as_str();
        if state == DriveState::Ok.to_str() || state == DriveState::Unformatted.to_str() {
            online += 1;
        } else {
            offline += 1;
        }
    }

    ClusterHealthStats {
        drives_offline_count: offline,
        drives_online_count: online,
        drives_count: storage_info.disks.len() as u64,
    }
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

/// Collect disk statistics from the storage layer.
pub async fn collect_disk_stats() -> Vec<DiskStats> {
    let Some(store) = new_object_layer_fn() else {
        return Vec::new();
    };

    let storage_info = store.storage_info().await;

    storage_info
        .disks
        .iter()
        .map(|disk| DiskStats {
            server: disk.endpoint.clone(),
            drive: disk.drive_path.clone(),
            total_bytes: disk.total_space,
            used_bytes: disk.used_space,
            free_bytes: disk.available_space,
        })
        .collect()
}

/// Collect system CPU statistics from the current host.
pub fn collect_system_cpu_stats() -> CpuStats {
    let mut system = System::new_all();
    system.refresh_cpu_all();

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

/// Collect system memory statistics from the current host.
pub fn collect_system_memory_stats() -> MemoryStats {
    let mut system = System::new_all();
    system.refresh_memory();

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

/// Collect system drive statistics using the storage layer snapshot.
pub async fn collect_system_drive_stats() -> (Vec<DriveDetailedStats>, DriveCountStats) {
    let Some(store) = new_object_layer_fn() else {
        return (Vec::new(), DriveCountStats::default());
    };

    let storage_info = store.storage_info().await;
    let total_count = storage_info.disks.len() as u64;

    let drive_stats = storage_info
        .disks
        .iter()
        .map(|disk| DriveDetailedStats {
            server: disk.endpoint.clone(),
            drive: disk.drive_path.clone(),
            total_bytes: disk.total_space,
            used_bytes: disk.used_space,
            free_bytes: disk.available_space,
            used_inodes: 0,
            free_inodes: 0,
            total_inodes: 0,
            timeout_errors_total: 0,
            io_errors_total: 0,
            availability_errors_total: 0,
            waiting_io: 0,
            api_latency_micros: 0,
            health: 1,
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
        })
        .collect();

    (
        drive_stats,
        DriveCountStats {
            offline_count: 0,
            online_count: total_count,
            total_count,
        },
    )
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
