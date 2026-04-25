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
    collect_request_metrics,
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
use crate::metrics::stats_collector::{
    ProcessMetricBundle, collect_bucket_replication_bandwidth_stats, collect_bucket_replication_detail_stats,
    collect_bucket_stats, collect_cluster_and_health_stats, collect_cluster_config_stats, collect_cluster_usage_metric_stats,
    collect_disk_and_system_drive_stats, collect_erasure_set_stats, collect_host_network_stats, collect_iam_stats,
    collect_ilm_metric_stats, collect_internode_network_stats, collect_process_metric_bundle, collect_replication_stats,
    collect_request_stats, collect_scanner_metric_stats, collect_system_cpu_and_memory_stats_with,
};
use rustfs_audit::audit_target_metrics;
use rustfs_notify::{notification_metrics_snapshot, notification_target_metrics};
use rustfs_utils::get_env_opt_u64;
use std::borrow::Cow;
use std::time::Duration;
use sysinfo::System;
use tokio::time::Instant;
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
                    let (stats, cluster_health) = collect_cluster_and_health_stats().await;
                    let mut metrics = collect_cluster_metrics(&stats);
                    metrics.extend(collect_cluster_health_metrics(&cluster_health));
                    report_metrics(&metrics);
                }
                _ = token_clone.cancelled() => {
                    warn!("Metrics collection for cluster stats cancelled.");
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
                    warn!("Metrics collection for supplementary cluster stats cancelled.");
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
                    let (disk_stats, drive_stats, drive_counts) = collect_disk_and_system_drive_stats().await;
                    let mut metrics = collect_node_metrics(&disk_stats);
                    metrics.extend(collect_drive_detailed_metrics(&drive_stats));
                    metrics.extend(collect_drive_count_metrics(&drive_counts));
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
                    let mut metrics = collect_bucket_replication_bandwidth_metrics(&stats);
                    let bucket_replication = collect_bucket_replication_detail_stats().await;
                    metrics.extend(collect_bucket_replication_metrics(&bucket_replication));
                    let replication = collect_replication_stats().await;
                    metrics.extend(collect_replication_metrics(&replication));
                    report_metrics(&metrics);
                }
                _ = token_clone.cancelled() => {
                    warn!("Metrics collection for bucket replication bandwidth stats cancelled.");
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
                    warn!("Metrics collection for background workflow stats cancelled.");
                    return;
                }
            }
        }
    });

    // Spawn task for request metrics defined by rustfs-obs request collectors.
    let token_clone = token.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(resource_interval);
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let stats = collect_request_stats().await;
                    if !stats.is_empty() {
                        let metrics = collect_request_metrics(&stats);
                        if !metrics.is_empty() {
                            report_metrics(&metrics);
                        }
                    }
                }
                _ = token_clone.cancelled() => {
                    warn!("Metrics collection for request stats cancelled.");
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

    let token_clone = token.clone();
    tokio::spawn(async move {
        let labels = current_process_metric_labels();
        let mut host_system = System::new_all();
        let process_interval = resource_interval.min(system_interval);
        let mut interval = tokio::time::interval(process_interval);
        let now = Instant::now();
        let mut next_resource_run = now;
        let mut next_system_run = now;

        #[cfg(feature = "gpu")]
        let current_pid = match sysinfo::get_current_pid() {
            Ok(pid) => Some(pid),
            Err(e) => {
                warn!("Failed to get current PID for system monitoring: {}", e);
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
                                        warn!("GPU metrics collection failed: {}", e);
                                    }
                                },
                                Err(e) => {
                                    warn!("GPU collector initialization failed: {}", e);
                                }
                            }
                        }

                        report_metrics(&metrics);
                        advance_deadline(&mut next_system_run, system_interval, now);
                    }
                }
                _ = token_clone.cancelled() => {
                    warn!("Process metrics collection cancelled.");
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
                    warn!("Metrics collection for internode network stats cancelled.");
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
    warn!("Failed to collect process attributes for metrics labels: {}", err);
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
    use super::advance_deadline;
    use std::time::Duration;
    use tokio::time::Instant;

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
}
