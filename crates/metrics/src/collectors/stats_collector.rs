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

use crate::collectors::{
    BucketReplicationBandwidthStats, BucketStats, ClusterStats, DiskStats, ProcessStats, ProcessStatusType, ResourceStats,
};
use rustfs_ecstore::bucket::metadata_sys::get_quota_config;
use rustfs_ecstore::data_usage::load_data_usage_from_backend;
use rustfs_ecstore::global::get_global_bucket_monitor;
use rustfs_ecstore::pools::{get_total_usable_capacity, get_total_usable_capacity_free};
use rustfs_ecstore::store_api::{BucketOperations, BucketOptions};
use rustfs_ecstore::{StorageAPI, new_object_layer_fn};
use rustfs_io_metrics::{snapshot_process_lock_counts, snapshot_process_platform_stats};
use std::sync::OnceLock;
use std::time::Instant;
use sysinfo::{Pid, ProcessRefreshKind, ProcessesToUpdate, System};
use tracing::{instrument, warn};

/// Process start time for calculating uptime.
static PROCESS_START: OnceLock<Instant> = OnceLock::new();

/// Get the process start time, initializing it on first call.
#[inline]
fn get_process_start() -> &'static Instant {
    PROCESS_START.get_or_init(Instant::now)
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

/// Collect resource and process statistics for the current process in one sysinfo refresh.
#[inline]
pub fn collect_process_resource_and_system_stats() -> (ResourceStats, ProcessStats) {
    let uptime_seconds = get_process_start().elapsed().as_secs();
    let platform_stats = snapshot_process_platform_stats();
    let lock_snapshot = snapshot_process_lock_counts();

    // Collect both resource and process metrics in one refresh to avoid duplicate sysinfo work.
    let mut sys = System::new();
    let pid = Pid::from_u32(std::process::id());
    sys.refresh_processes_specifics(ProcessesToUpdate::Some(&[pid]), true, ProcessRefreshKind::everything());

    if let Some(process) = sys.process(pid) {
        let disk_usage = process.disk_usage();
        let status = ProcessStatusType::from(process.status());
        let resource_stats = ResourceStats {
            cpu_percent: process.cpu_usage() as f64,
            memory_bytes: process.memory(),
            uptime_seconds,
        };
        let process_stats = ProcessStats {
            locks_read_total: lock_snapshot.read_locks_held,
            locks_write_total: lock_snapshot.write_locks_held,
            cpu_total_seconds: process.accumulated_cpu_time() as f64 / 1000.0,
            file_descriptor_limit_total: process.open_files_limit().map_or(0, |value| value as u64),
            file_descriptor_open_total: process.open_files().map_or(0, |value| value as u64),
            go_routine_total: process.tasks().map_or(0, |tasks| tasks.len() as u64),
            io_rchar_bytes: platform_stats.io_rchar_bytes.unwrap_or(disk_usage.total_read_bytes),
            io_read_bytes: platform_stats.io_read_bytes.unwrap_or(disk_usage.total_read_bytes),
            io_wchar_bytes: platform_stats.io_wchar_bytes.unwrap_or(disk_usage.total_written_bytes),
            io_write_bytes: platform_stats.io_write_bytes.unwrap_or(disk_usage.total_written_bytes),
            resident_memory_bytes: process.memory(),
            start_time_seconds: process.start_time(),
            status,
            status_value: status as i64,
            syscall_read_total: platform_stats.syscall_read_total.unwrap_or(0),
            syscall_write_total: platform_stats.syscall_write_total.unwrap_or(0),
            uptime_seconds,
            virtual_memory_bytes: process.virtual_memory(),
            virtual_memory_max_bytes: platform_stats.virtual_memory_max_bytes.unwrap_or(0),
            ..Default::default()
        };
        (resource_stats, process_stats)
    } else {
        (
            ResourceStats {
                cpu_percent: 0.0,
                memory_bytes: 0,
                uptime_seconds,
            },
            ProcessStats {
                uptime_seconds,
                ..Default::default()
            },
        )
    }
}

/// Collect resource statistics for the current process.
#[inline]
pub fn collect_process_stats() -> ResourceStats {
    collect_process_resource_and_system_stats().0
}

/// Collect process statistics for the current process.
#[inline]
pub fn collect_process_system_stats() -> ProcessStats {
    collect_process_resource_and_system_stats().1
}
