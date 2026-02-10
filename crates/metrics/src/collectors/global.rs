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

use crate::collectors::{
    BucketStats, ClusterStats, DiskStats, ResourceStats, collect_bucket_metrics, collect_cluster_metrics, collect_node_metrics,
    collect_resource_metrics,
};
use crate::constants::{
    DEFAULT_BUCKET_METRICS_INTERVAL, DEFAULT_CLUSTER_METRICS_INTERVAL, DEFAULT_NODE_METRICS_INTERVAL,
    DEFAULT_RESOURCE_METRICS_INTERVAL, ENV_BUCKET_METRICS_INTERVAL, ENV_CLUSTER_METRICS_INTERVAL, ENV_DEFAULT_METRICS_INTERVAL,
    ENV_NODE_METRICS_INTERVAL, ENV_RESOURCE_METRICS_INTERVAL,
};
use crate::format::report_metrics;
use rustfs_ecstore::bucket::metadata_sys::get_quota_config;
use rustfs_ecstore::data_usage::load_data_usage_from_backend;
use rustfs_ecstore::pools::{get_total_usable_capacity, get_total_usable_capacity_free};
use rustfs_ecstore::store_api::BucketOptions;
use rustfs_ecstore::{StorageAPI, new_object_layer_fn};
use rustfs_utils::get_env_opt_u64;
use std::sync::OnceLock;
use std::time::{Duration, Instant};
use sysinfo::{Pid, ProcessRefreshKind, ProcessesToUpdate, System};
use tokio_util::sync::CancellationToken;
use tracing::warn;

/// Process start time for calculating uptime.
static PROCESS_START: OnceLock<Instant> = OnceLock::new();

/// Get the process start time, initializing it on first call.
#[inline]
fn get_process_start() -> &'static Instant {
    PROCESS_START.get_or_init(Instant::now)
}

/// Collect cluster statistics from the storage layer.
async fn collect_cluster_stats() -> ClusterStats {
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
async fn collect_bucket_stats() -> Vec<BucketStats> {
    let Some(store) = new_object_layer_fn() else {
        return Vec::new();
    };

    // Load data usage info from backend to get bucket sizes and object counts
    let data_usage = match load_data_usage_from_backend(store.clone()).await {
        Ok(info) => Some(info),
        Err(e) => {
            warn!("Failed to load data usage from backend for bucket metrics: {}", e);
            None
        }
    };

    let buckets = match store
        .list_bucket(&BucketOptions {
            cached: true,
            ..Default::default()
        })
        .await
    {
        Ok(b) => b,
        Err(e) => {
            warn!("Failed to list buckets for metrics: {}", e);
            return Vec::new();
        }
    };

    // Build bucket stats with real data from DataUsageInfo
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

/// Collect disk statistics from the storage layer.
async fn collect_disk_stats() -> Vec<DiskStats> {
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

/// Collect resource statistics for the current process.
///
/// Collects:
/// - Uptime: Calculated from process start time
/// - Memory: Process resident set size from sysinfo
/// - CPU: Process CPU usage percentage from sysinfo
#[inline]
fn collect_process_stats() -> ResourceStats {
    let uptime_seconds = get_process_start().elapsed().as_secs();

    // Use sysinfo for process metrics
    let mut sys = System::new();
    let pid = Pid::from_u32(std::process::id());
    sys.refresh_processes_specifics(
        ProcessesToUpdate::Some(&[pid]),
        true,
        ProcessRefreshKind::nothing().with_cpu().with_memory(),
    );

    if let Some(process) = sys.process(pid) {
        ResourceStats {
            cpu_percent: process.cpu_usage() as f64,
            memory_bytes: process.memory(),
            uptime_seconds,
        }
    } else {
        // Fallback if process not found
        ResourceStats {
            cpu_percent: 0.0,
            memory_bytes: 0,
            uptime_seconds,
        }
    }
}

/// Initialize the metrics collection system with periodic background tasks for cluster, bucket, node, and resource metrics.
///
/// This function spawns background tasks that periodically collect metrics
/// and report them using the `metrics` crate.
///
/// # Arguments
///
/// * `token` - A cancellation token to gracefully stop the metrics collection tasks.
pub fn init_metrics_collectors(token: CancellationToken) {
    // Initialize process start time
    get_process_start();

    // Helper closure to determine interval for a specific metric type
    let get_interval = |env_key: &str, type_default: Duration| -> Duration {
        // 1. Try specific env var
        // 2. Fallback to global default env var (if set differently from hardcoded default)
        // 3. Fallback to type specific default

        // Helper to check if value is valid (non-zero)
        let is_valid = |v: u64| v > 0;

        if let Some(val) = get_env_opt_u64(env_key).filter(|&v| is_valid(v)) {
            Duration::from_secs(val)
        } else if let Some(val) = get_env_opt_u64(ENV_DEFAULT_METRICS_INTERVAL).filter(|&v| is_valid(v)) {
            Duration::from_secs(val)
        } else {
            type_default
        }
    };

    let cluster_interval = get_interval(ENV_CLUSTER_METRICS_INTERVAL, DEFAULT_CLUSTER_METRICS_INTERVAL);
    let bucket_interval = get_interval(ENV_BUCKET_METRICS_INTERVAL, DEFAULT_BUCKET_METRICS_INTERVAL);
    let node_interval = get_interval(ENV_NODE_METRICS_INTERVAL, DEFAULT_NODE_METRICS_INTERVAL);
    let resource_interval = get_interval(ENV_RESOURCE_METRICS_INTERVAL, DEFAULT_RESOURCE_METRICS_INTERVAL);

    // Spawn task for cluster metrics
    let token_clone = token.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(cluster_interval);
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let stats = collect_cluster_stats().await;
                    let metrics = collect_cluster_metrics(&stats);
                    report_metrics(&metrics);
                }
                _ = token_clone.cancelled() => {
                    warn!("Metrics collection for cluster stats cancelled.");
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
                    warn!("Metrics collection for bucket stats cancelled.");
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
                    let stats = collect_disk_stats().await;
                    let metrics = collect_node_metrics(&stats);
                    report_metrics(&metrics);
                }
                _ = token_clone.cancelled() => {
                    warn!("Metrics collection for node/disk stats cancelled.");
                    return;
                }
            }
        }
    });

    // Spawn task for resource metrics
    let token_clone = token.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(resource_interval);
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    // Resource stats collection is synchronous but fast
                    let stats = collect_process_stats();
                    let metrics = collect_resource_metrics(&stats);
                    report_metrics(&metrics);
                }
                _ = token_clone.cancelled() => {
                    warn!("Metrics collection for resource stats cancelled.");
                    return;
                }
            }
        }
    });
}
