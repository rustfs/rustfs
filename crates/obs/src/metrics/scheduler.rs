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
//! - Process network I/O metrics

use crate::metrics::collectors::{
    AuditTargetStats,
    NotificationStats,
    NotificationTargetStats,
    // System monitoring collectors (migrated from rustfs-obs::system)
    ProcessCpuStats,
    ProcessDiskStats,
    ProcessMemoryStats,
    ProcessNetworkStats,
    collect_audit_metrics,
    collect_bucket_metrics,
    collect_bucket_replication_bandwidth_metrics,
    collect_cluster_metrics,
    collect_node_metrics,
    collect_notification_metrics,
    collect_notification_target_metrics,
    collect_process_cpu_metrics,
    collect_process_disk_metrics,
    collect_process_memory_metrics,
    collect_process_metrics,
    collect_process_network_metrics,
    collect_resource_metrics,
};
use crate::metrics::config::{
    DEFAULT_AUDIT_METRICS_INTERVAL, DEFAULT_BUCKET_METRICS_INTERVAL, DEFAULT_BUCKET_REPLICATION_BANDWIDTH_METRICS_INTERVAL,
    DEFAULT_CLUSTER_METRICS_INTERVAL, DEFAULT_NODE_METRICS_INTERVAL, DEFAULT_NOTIFICATION_METRICS_INTERVAL,
    DEFAULT_RESOURCE_METRICS_INTERVAL, ENV_AUDIT_METRICS_INTERVAL, ENV_BUCKET_METRICS_INTERVAL,
    ENV_BUCKET_REPLICATION_BANDWIDTH_METRICS_INTERVAL, ENV_CLUSTER_METRICS_INTERVAL, ENV_DEFAULT_METRICS_INTERVAL,
    ENV_NODE_METRICS_INTERVAL, ENV_NOTIFICATION_METRICS_INTERVAL, ENV_RESOURCE_METRICS_INTERVAL,
};
use crate::metrics::report::report_metrics;
use crate::metrics::stats_collector::{
    collect_bucket_replication_bandwidth_stats, collect_bucket_stats, collect_cluster_stats, collect_disk_stats,
    collect_process_resource_and_system_stats,
};
use rustfs_audit::audit_target_metrics;
use rustfs_notify::{notification_metrics_snapshot, notification_target_metrics};
use rustfs_utils::get_env_opt_u64;
use std::borrow::Cow;
use std::time::Duration;
use sysinfo::{Pid, System};
use tokio_util::sync::CancellationToken;
use tracing::warn;

/// Default interval for system monitoring metrics (15 seconds)
const DEFAULT_SYSTEM_METRICS_INTERVAL: Duration = Duration::from_secs(15);
/// Environment variable for system monitoring interval
const ENV_SYSTEM_METRICS_INTERVAL: &str = "RUSTFS_METRICS_SYSTEM_INTERVAL_SEC";
/// Legacy environment variable for system monitoring interval
const LEGACY_SYSTEM_METRICS_INTERVAL: &str = "RUSTFS_OBS_METRICS_SYSTEM_INTERVAL_MS";

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
    const LEGACY_CLUSTER_INTERVAL: &str = "RUSTFS_METRICS_CLUSTER_INTERVAL";
    const LEGACY_BUCKET_INTERVAL: &str = "RUSTFS_METRICS_BUCKET_INTERVAL";
    const LEGACY_NODE_INTERVAL: &str = "RUSTFS_METRICS_NODE_INTERVAL";
    const LEGACY_REPLICATION_BANDWIDTH_INTERVAL: &str = "RUSTFS_METRICS_BUCKET_REPLICATION_BANDWIDTH_INTERVAL";
    const LEGACY_RESOURCE_INTERVAL: &str = "RUSTFS_METRICS_RESOURCE_INTERVAL";
    const LEGACY_AUDIT_INTERVAL: &str = "RUSTFS_METRICS_AUDIT_INTERVAL";
    const LEGACY_NOTIFICATION_INTERVAL: &str = "RUSTFS_METRICS_NOTIFICATION_INTERVAL";
    const LEGACY_DEFAULT_INTERVAL: &str = "RUSTFS_METRICS_DEFAULT_INTERVAL";

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

    // Read intervals from environment or use defaults
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

    // Spawn task for bucket replication bandwidth metrics
    let token_clone = token.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(bucket_replication_bandwidth_interval);
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let stats = collect_bucket_replication_bandwidth_stats();
                    let metrics = collect_bucket_replication_bandwidth_metrics(&stats);
                    report_metrics(&metrics);
                }
                _ = token_clone.cancelled() => {
                    warn!("Metrics collection for bucket replication bandwidth stats cancelled.");
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
                    let (resource_stats, process_stats) = collect_process_resource_and_system_stats();
                    let mut metrics = collect_resource_metrics(&resource_stats);
                    metrics.extend(collect_process_metrics(&process_stats));
                    report_metrics(&metrics);
                }
                _ = token_clone.cancelled() => {
                    warn!("Metrics collection for resource stats cancelled.");
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
                    warn!("Metrics collection for audit target stats cancelled.");
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
                    warn!("Metrics collection for notification stats cancelled.");
                    return;
                }
            }
        }
    });

    // Spawn task for system monitoring metrics (migrated from rustfs-obs::system)
    let system_interval = get_env_opt_u64(ENV_SYSTEM_METRICS_INTERVAL)
        .or_else(|| get_env_opt_u64(LEGACY_SYSTEM_METRICS_INTERVAL).map(|ms| ms / 1000)) // Convert ms to seconds
        .or_else(|| get_env_opt_u64(ENV_DEFAULT_METRICS_INTERVAL))
        .filter(|&v| v > 0)
        .map(Duration::from_secs)
        .unwrap_or(DEFAULT_SYSTEM_METRICS_INTERVAL);

    let token_clone = token;
    tokio::spawn(async move {
        // Get current process PID
        let pid = match sysinfo::get_current_pid() {
            Ok(p) => p,
            Err(e) => {
                warn!("Failed to get current PID for system monitoring: {}", e);
                return;
            }
        };

        let mut interval = tokio::time::interval(system_interval);
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    // Collect system monitoring metrics
                    let metrics = collect_system_monitoring_metrics(pid);
                    report_metrics(&metrics);
                }
                _ = token_clone.cancelled() => {
                    warn!("System monitoring metrics collection cancelled.");
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

/// Collect all system monitoring metrics for a process.
///
/// This function collects CPU, memory, disk I/O, and network I/O metrics
/// for the specified process PID.
///
/// # Arguments
/// * `pid` - The process ID to monitor
///
/// # Returns
/// A vector of Prometheus metrics for the process.
fn collect_system_monitoring_metrics(pid: Pid) -> Vec<crate::metrics::report::PrometheusMetric> {
    let mut metrics = Vec::new();
    let mut system = System::new();

    // Refresh process information
    system.refresh_processes(sysinfo::ProcessesToUpdate::Some(&[pid]), true);

    if let Some(process) = system.process(pid) {
        // Create labels with process attributes
        let labels: Vec<(&'static str, Cow<'static, str>)> = vec![
            ("process_pid", Cow::Owned(pid.as_u32().to_string())),
            ("process_executable_name", Cow::Owned(process.name().to_string_lossy().to_string())),
        ];

        // Collect CPU metrics
        let cpu_stats = ProcessCpuStats {
            usage: process.cpu_usage() as f64,
            utilization: process.cpu_usage() as f64, // Same as usage for single process
        };
        metrics.extend(collect_process_cpu_metrics(&cpu_stats, Some(&labels)));

        // Collect memory metrics
        let memory_stats = ProcessMemoryStats {
            resident: process.memory(),
            virtual_mem: process.virtual_memory(),
        };
        metrics.extend(collect_process_memory_metrics(&memory_stats, Some(&labels)));

        // Collect disk I/O metrics
        let disk_usage = process.disk_usage();
        let disk_stats = ProcessDiskStats {
            read_bytes: disk_usage.read_bytes,
            written_bytes: disk_usage.written_bytes,
        };
        metrics.extend(collect_process_disk_metrics(&disk_stats, Some(&labels)));

        // Collect network I/O metrics
        // Note: sysinfo 0.38.x provides network info via Networks new type
        // We use Networks::new_with_refreshed_list() to get network interfaces
        let networks = sysinfo::Networks::new_with_refreshed_list();
        let mut total_received = 0u64;
        let mut total_transmitted = 0u64;
        let mut per_interface = Vec::new();

        for (interface_name, data) in networks.iter() {
            let received = data.received();
            let transmitted = data.transmitted();
            total_received += received;
            total_transmitted += transmitted;
            per_interface.push((interface_name.to_string(), received, transmitted));
        }

        let network_stats = ProcessNetworkStats {
            total_received,
            total_transmitted,
            per_interface,
        };
        metrics.extend(collect_process_network_metrics(&network_stats, Some(&labels)));

        // Collect GPU metrics (if gpu feature is enabled)
        #[cfg(feature = "gpu")]
        {
            use crate::metrics::collectors::{GpuCollector, collect_gpu_metrics};

            match GpuCollector::new(pid) {
                Ok(collector) => match collector.collect() {
                    Ok(gpu_stats) => {
                        metrics.extend(collect_gpu_metrics(&gpu_stats, &labels));
                    }
                    Err(e) => {
                        warn!("GPU metrics collection failed: {}", e);
                    }
                },
                Err(e) => {
                    warn!("GPU collector initialization failed: {}", e);
                }
            }
        }
    }

    metrics
}
