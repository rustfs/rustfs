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

use crate::collectors::stats_collector::{
    collect_bucket_replication_bandwidth_stats, collect_bucket_stats, collect_cluster_stats, collect_disk_stats,
    collect_process_stats,
};
use crate::collectors::{
    collect_bucket_metrics, collect_bucket_replication_bandwidth_metrics, collect_cluster_metrics, collect_node_metrics,
    collect_resource_metrics,
};
use crate::constants::{
    DEFAULT_BUCKET_METRICS_INTERVAL, DEFAULT_BUCKET_REPLICATION_BANDWIDTH_METRICS_INTERVAL, DEFAULT_CLUSTER_METRICS_INTERVAL,
    DEFAULT_NODE_METRICS_INTERVAL, DEFAULT_RESOURCE_METRICS_INTERVAL, ENV_BUCKET_METRICS_INTERVAL,
    ENV_BUCKET_REPLICATION_BANDWIDTH_METRICS_INTERVAL, ENV_CLUSTER_METRICS_INTERVAL, ENV_DEFAULT_METRICS_INTERVAL,
    ENV_NODE_METRICS_INTERVAL, ENV_RESOURCE_METRICS_INTERVAL,
};
use crate::format::report_metrics;
use rustfs_utils::get_env_opt_u64;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::warn;

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
pub fn init_metrics_collectors(token: CancellationToken) {
    const LEGACY_CLUSTER_INTERVAL: &str = "RUSTFS_METRICS_CLUSTER_INTERVAL";
    const LEGACY_BUCKET_INTERVAL: &str = "RUSTFS_METRICS_BUCKET_INTERVAL";
    const LEGACY_NODE_INTERVAL: &str = "RUSTFS_METRICS_NODE_INTERVAL";
    const LEGACY_REPLICATION_BANDWIDTH_INTERVAL: &str = "RUSTFS_METRICS_BUCKET_REPLICATION_BANDWIDTH_INTERVAL";
    const LEGACY_RESOURCE_INTERVAL: &str = "RUSTFS_METRICS_RESOURCE_INTERVAL";
    const LEGACY_DEFAULT_INTERVAL: &str = "RUSTFS_METRICS_DEFAULT_INTERVAL";

    fn parse_interval(msc: &str, legacy_msc: &str) -> Option<u64> {
        get_env_opt_u64(msc)
            .or_else(|| get_env_opt_u64(legacy_msc))
            .filter(|&v| v > 0)
    }

    // Read intervals from environment or use defaults, ensuring zero is ignored.
    let cluster_interval = parse_interval(ENV_CLUSTER_METRICS_INTERVAL, LEGACY_CLUSTER_INTERVAL)
        .or_else(|| get_env_opt_u64(ENV_DEFAULT_METRICS_INTERVAL))
        .or_else(|| get_env_opt_u64(LEGACY_DEFAULT_INTERVAL))
        .filter(|&v| v > 0)
        .map(Duration::from_secs)
        .unwrap_or(DEFAULT_CLUSTER_METRICS_INTERVAL);

    let bucket_interval = parse_interval(ENV_BUCKET_METRICS_INTERVAL, LEGACY_BUCKET_INTERVAL)
        .or_else(|| get_env_opt_u64(ENV_DEFAULT_METRICS_INTERVAL))
        .or_else(|| get_env_opt_u64(LEGACY_DEFAULT_INTERVAL))
        .filter(|&v| v > 0)
        .map(Duration::from_secs)
        .unwrap_or(DEFAULT_BUCKET_METRICS_INTERVAL);

    let bucket_replication_bandwidth_interval =
        parse_interval(ENV_BUCKET_REPLICATION_BANDWIDTH_METRICS_INTERVAL, LEGACY_REPLICATION_BANDWIDTH_INTERVAL)
            .or_else(|| get_env_opt_u64(ENV_DEFAULT_METRICS_INTERVAL))
            .or_else(|| get_env_opt_u64(LEGACY_DEFAULT_INTERVAL))
            .filter(|&v| v > 0)
            .map(Duration::from_secs)
            .unwrap_or(DEFAULT_BUCKET_REPLICATION_BANDWIDTH_METRICS_INTERVAL);

    let node_interval = parse_interval(ENV_NODE_METRICS_INTERVAL, LEGACY_NODE_INTERVAL)
        .or_else(|| get_env_opt_u64(ENV_DEFAULT_METRICS_INTERVAL))
        .or_else(|| get_env_opt_u64(LEGACY_DEFAULT_INTERVAL))
        .filter(|&v| v > 0)
        .map(Duration::from_secs)
        .unwrap_or(DEFAULT_NODE_METRICS_INTERVAL);

    let resource_interval = parse_interval(ENV_RESOURCE_METRICS_INTERVAL, LEGACY_RESOURCE_INTERVAL)
        .or_else(|| get_env_opt_u64(ENV_DEFAULT_METRICS_INTERVAL))
        .or_else(|| get_env_opt_u64(LEGACY_DEFAULT_INTERVAL))
        .filter(|&v| v > 0)
        .map(Duration::from_secs)
        .unwrap_or(DEFAULT_RESOURCE_METRICS_INTERVAL);

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
