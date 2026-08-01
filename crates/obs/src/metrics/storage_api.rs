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

use std::collections::HashMap;
use std::time::Duration;

pub(crate) use rustfs_ecstore::api::bucket::bandwidth::monitor::Monitor as ObsBucketBandwidthMonitor;
pub(crate) use rustfs_ecstore::api::bucket::metadata_sys::get_quota_config as obs_get_quota_config;
use rustfs_ecstore::api::bucket::replication::{
    DurableMrfBucketBacklog, DurableMrfTargetBacklog, MrfBucketBacklogObservability, RuntimeReplicationTargetBacklog,
    durable_mrf_backlog_summary_snapshot, durable_mrf_target_backlog_snapshot, get_global_replication_stats,
    mrf_backlog_observability_snapshot,
};
pub(crate) use rustfs_ecstore::api::capacity::{
    get_total_usable_capacity as obs_get_total_usable_capacity,
    get_total_usable_capacity_free as obs_get_total_usable_capacity_free,
};
pub(crate) use rustfs_ecstore::api::compression::is_disk_compression_enabled as obs_is_disk_compression_enabled;
pub(crate) use rustfs_ecstore::api::data_usage::load_compression_total_from_memory as obs_load_compression_total_from_memory;
pub(crate) use rustfs_ecstore::api::data_usage::load_data_usage_from_backend as obs_load_data_usage_from_backend;
pub(crate) use rustfs_ecstore::api::error::Result as ObsEcstoreResult;
pub(crate) use rustfs_ecstore::api::runtime::{
    bucket_monitor as obs_get_global_bucket_monitor, expiry_state_handle as obs_expiry_state_handle,
    object_store_handle as obs_resolve_object_store_handle, transition_state_handle as obs_transition_state_handle,
};
pub(crate) use rustfs_ecstore::api::storage::ECStore as ObsStore;
use rustfs_storage_api as storage_contracts;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ObsBucketReplicationTargetStatsSnapshot {
    pub(crate) target_arn: String,
    pub(crate) bandwidth_limit_bytes_per_sec: u64,
    pub(crate) current_bandwidth_bytes_per_sec: f64,
    pub(crate) latency_ms: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ObsBucketReplicationTargetBacklogSnapshot {
    pub(crate) target_arn: String,
    pub(crate) current_backlog_count: u64,
    pub(crate) current_backlog_bytes: u64,
    pub(crate) durable_mrf_backlog_count: u64,
    pub(crate) durable_mrf_backlog_bytes: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ObsBucketReplicationStatsSnapshot {
    pub(crate) bucket: String,
    pub(crate) total_failed_bytes: u64,
    pub(crate) total_failed_count: u64,
    pub(crate) last_min_failed_bytes: u64,
    pub(crate) last_min_failed_count: u64,
    pub(crate) last_hour_failed_bytes: u64,
    pub(crate) last_hour_failed_count: u64,
    pub(crate) sent_bytes: u64,
    pub(crate) sent_count: u64,
    pub(crate) proxied_get_requests_total: u64,
    pub(crate) proxied_get_requests_failures: u64,
    pub(crate) proxied_head_requests_total: u64,
    pub(crate) proxied_head_requests_failures: u64,
    pub(crate) proxied_put_requests_total: u64,
    pub(crate) proxied_put_requests_failures: u64,
    pub(crate) proxied_put_tagging_requests_total: u64,
    pub(crate) proxied_put_tagging_requests_failures: u64,
    pub(crate) proxied_get_tagging_requests_total: u64,
    pub(crate) proxied_get_tagging_requests_failures: u64,
    pub(crate) proxied_delete_tagging_requests_total: u64,
    pub(crate) proxied_delete_tagging_requests_failures: u64,
    pub(crate) resync_started_count: u64,
    pub(crate) resync_completed_count: u64,
    pub(crate) resync_failed_count: u64,
    pub(crate) resync_canceled_count: u64,
    pub(crate) resync_duration_ms: u64,
    pub(crate) current_backlog_count: u64,
    pub(crate) current_backlog_bytes: u64,
    pub(crate) durable_mrf_available: bool,
    pub(crate) durable_mrf_backlog_count: u64,
    pub(crate) durable_mrf_backlog_bytes: u64,
    pub(crate) mrf_pending_count: u64,
    pub(crate) mrf_pending_bytes: u64,
    pub(crate) mrf_dropped_count: u64,
    pub(crate) mrf_missed_count: u64,
    pub(crate) mrf_flush_failures: u64,
    pub(crate) mrf_last_flush_duration_millis: u64,
    pub(crate) targets: Vec<ObsBucketReplicationTargetStatsSnapshot>,
    pub(crate) target_backlogs: Vec<ObsBucketReplicationTargetBacklogSnapshot>,
}

#[derive(Debug, Clone, Default, PartialEq)]
struct ObsBucketReplicationRuntimeSnapshot {
    total_failed_bytes: u64,
    total_failed_count: u64,
    last_min_failed_bytes: u64,
    last_min_failed_count: u64,
    last_hour_failed_bytes: u64,
    last_hour_failed_count: u64,
    sent_bytes: u64,
    sent_count: u64,
    resync_started_count: u64,
    resync_completed_count: u64,
    resync_failed_count: u64,
    resync_canceled_count: u64,
    resync_duration_ms: u64,
    current_backlog_count: u64,
    current_backlog_bytes: u64,
    targets: Vec<ObsBucketReplicationTargetStatsSnapshot>,
}

#[derive(Debug, Clone, Default, PartialEq)]
struct ObsBucketReplicationProxySnapshot {
    proxied_get_requests_total: u64,
    proxied_get_requests_failures: u64,
    proxied_head_requests_total: u64,
    proxied_head_requests_failures: u64,
    proxied_put_requests_total: u64,
    proxied_put_requests_failures: u64,
    proxied_put_tagging_requests_total: u64,
    proxied_put_tagging_requests_failures: u64,
    proxied_get_tagging_requests_total: u64,
    proxied_get_tagging_requests_failures: u64,
    proxied_delete_tagging_requests_total: u64,
    proxied_delete_tagging_requests_failures: u64,
}

#[derive(Debug, Clone, Default, PartialEq)]
struct ObsBucketReplicationBacklogSnapshot {
    durable_mrf_available: bool,
    durable_bucket: DurableMrfBucketBacklog,
    runtime_targets: Vec<RuntimeReplicationTargetBacklog>,
    durable_targets: Vec<DurableMrfTargetBacklog>,
    mrf_observability: MrfBucketBacklogObservability,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) struct ObsReplicationSiteStatsSnapshot {
    pub(crate) average_active_workers: f64,
    pub(crate) average_queued_bytes: i64,
    pub(crate) average_queued_count: i64,
    pub(crate) average_data_transfer_rate: f64,
    pub(crate) active_workers: u64,
    pub(crate) current_data_transfer_rate: f64,
    pub(crate) last_minute_queued_bytes: u64,
    pub(crate) last_minute_queued_count: u64,
    pub(crate) max_active_workers: u64,
    pub(crate) max_queued_bytes: u64,
    pub(crate) max_queued_count: u64,
    pub(crate) max_data_transfer_rate: f64,
    pub(crate) recent_backlog_count: u64,
}

fn i64_to_u64_floor_zero(value: i64) -> u64 {
    u64::try_from(value.max(0)).unwrap_or(0)
}

fn i32_to_u64_floor_zero(value: i32) -> u64 {
    u64::try_from(value.max(0)).unwrap_or(0)
}

fn replication_backlog_count(failed_counts: impl Iterator<Item = i64>, queued_count: i64) -> u64 {
    let failed_backlog = failed_counts.map(i64_to_u64_floor_zero).sum::<u64>();

    failed_backlog.saturating_add(i64_to_u64_floor_zero(queued_count))
}

fn bucket_replication_stats_snapshot_from_parts(
    bucket: String,
    runtime: ObsBucketReplicationRuntimeSnapshot,
    proxy: ObsBucketReplicationProxySnapshot,
    backlog: ObsBucketReplicationBacklogSnapshot,
) -> ObsBucketReplicationStatsSnapshot {
    let mut target_backlogs = HashMap::with_capacity(backlog.runtime_targets.len().saturating_add(backlog.durable_targets.len()));
    for target in backlog.runtime_targets {
        let entry =
            target_backlogs
                .entry(target.target_arn.clone())
                .or_insert_with(|| ObsBucketReplicationTargetBacklogSnapshot {
                    target_arn: target.target_arn,
                    current_backlog_count: 0,
                    current_backlog_bytes: 0,
                    durable_mrf_backlog_count: 0,
                    durable_mrf_backlog_bytes: 0,
                });
        entry.current_backlog_count = target.count;
        entry.current_backlog_bytes = target.bytes;
    }
    for target in backlog.durable_targets {
        let entry =
            target_backlogs
                .entry(target.target_arn.clone())
                .or_insert_with(|| ObsBucketReplicationTargetBacklogSnapshot {
                    target_arn: target.target_arn,
                    current_backlog_count: 0,
                    current_backlog_bytes: 0,
                    durable_mrf_backlog_count: 0,
                    durable_mrf_backlog_bytes: 0,
                });
        entry.durable_mrf_backlog_count = target.count;
        entry.durable_mrf_backlog_bytes = target.bytes;
    }
    let mut target_backlogs = target_backlogs.into_values().collect::<Vec<_>>();
    target_backlogs.sort_by(|left, right| left.target_arn.cmp(&right.target_arn));

    ObsBucketReplicationStatsSnapshot {
        bucket,
        total_failed_bytes: runtime.total_failed_bytes,
        total_failed_count: runtime.total_failed_count,
        last_min_failed_bytes: runtime.last_min_failed_bytes,
        last_min_failed_count: runtime.last_min_failed_count,
        last_hour_failed_bytes: runtime.last_hour_failed_bytes,
        last_hour_failed_count: runtime.last_hour_failed_count,
        sent_bytes: runtime.sent_bytes,
        sent_count: runtime.sent_count,
        proxied_get_requests_total: proxy.proxied_get_requests_total,
        proxied_get_requests_failures: proxy.proxied_get_requests_failures,
        proxied_head_requests_total: proxy.proxied_head_requests_total,
        proxied_head_requests_failures: proxy.proxied_head_requests_failures,
        proxied_put_requests_total: proxy.proxied_put_requests_total,
        proxied_put_requests_failures: proxy.proxied_put_requests_failures,
        proxied_put_tagging_requests_total: proxy.proxied_put_tagging_requests_total,
        proxied_put_tagging_requests_failures: proxy.proxied_put_tagging_requests_failures,
        proxied_get_tagging_requests_total: proxy.proxied_get_tagging_requests_total,
        proxied_get_tagging_requests_failures: proxy.proxied_get_tagging_requests_failures,
        proxied_delete_tagging_requests_total: proxy.proxied_delete_tagging_requests_total,
        proxied_delete_tagging_requests_failures: proxy.proxied_delete_tagging_requests_failures,
        resync_started_count: runtime.resync_started_count,
        resync_completed_count: runtime.resync_completed_count,
        resync_failed_count: runtime.resync_failed_count,
        resync_canceled_count: runtime.resync_canceled_count,
        resync_duration_ms: runtime.resync_duration_ms,
        current_backlog_count: runtime.current_backlog_count,
        current_backlog_bytes: runtime.current_backlog_bytes,
        durable_mrf_available: backlog.durable_mrf_available,
        durable_mrf_backlog_count: backlog.durable_bucket.count,
        durable_mrf_backlog_bytes: backlog.durable_bucket.bytes,
        mrf_pending_count: backlog.mrf_observability.pending_count,
        mrf_pending_bytes: backlog.mrf_observability.pending_bytes,
        mrf_dropped_count: backlog.mrf_observability.dropped_count,
        mrf_missed_count: backlog.mrf_observability.missed_count,
        mrf_flush_failures: backlog.mrf_observability.flush_failure_count,
        mrf_last_flush_duration_millis: backlog.mrf_observability.last_flush_duration_millis,
        targets: runtime.targets,
        target_backlogs,
    }
}

pub(crate) async fn obs_bucket_replication_stats_snapshot() -> Vec<ObsBucketReplicationStatsSnapshot> {
    let stats = get_global_replication_stats();
    let all_bucket_stats = if let Some(stats) = &stats {
        stats.get_all().await
    } else {
        HashMap::new()
    };
    let replication_storage_available = obs_resolve_object_store_handle().is_some();
    let durable_mrf_summary = if replication_storage_available {
        durable_mrf_backlog_summary_snapshot()
    } else {
        Default::default()
    };
    let durable_mrf_available = durable_mrf_summary.available;
    let durable_buckets = durable_mrf_summary
        .buckets
        .into_iter()
        .map(|bucket| (bucket.bucket.clone(), bucket))
        .collect::<HashMap<String, DurableMrfBucketBacklog>>();
    let mut durable_targets_by_bucket: HashMap<String, Vec<DurableMrfTargetBacklog>> = HashMap::new();
    let durable_mrf_targets = if replication_storage_available && durable_mrf_available {
        durable_mrf_target_backlog_snapshot()
    } else {
        Vec::new()
    };
    for target in durable_mrf_targets {
        durable_targets_by_bucket
            .entry(target.bucket.clone())
            .or_default()
            .push(target);
    }
    let mut runtime_targets_by_bucket: HashMap<String, Vec<RuntimeReplicationTargetBacklog>> = HashMap::new();
    if let Some(stats) = &stats {
        for target in stats.runtime_target_backlog_snapshot() {
            runtime_targets_by_bucket
                .entry(target.bucket.clone())
                .or_default()
                .push(target);
        }
    }
    let mrf_observability = if replication_storage_available {
        mrf_backlog_observability_snapshot()
    } else {
        Default::default()
    };
    let mrf_observability_buckets = mrf_observability
        .buckets
        .into_iter()
        .map(|bucket| (bucket.bucket.clone(), bucket))
        .collect::<HashMap<String, MrfBucketBacklogObservability>>();
    let mut bucket_names = Vec::with_capacity(
        all_bucket_stats
            .len()
            .saturating_add(durable_buckets.len())
            .saturating_add(mrf_observability_buckets.len())
            .saturating_add(runtime_targets_by_bucket.len()),
    );
    bucket_names.extend(all_bucket_stats.keys().cloned());
    bucket_names.extend(
        durable_buckets
            .keys()
            .filter(|bucket| !all_bucket_stats.contains_key(*bucket))
            .cloned(),
    );
    bucket_names.extend(
        mrf_observability_buckets
            .keys()
            .filter(|bucket| !all_bucket_stats.contains_key(*bucket) && !durable_buckets.contains_key(*bucket))
            .cloned(),
    );
    bucket_names.extend(
        runtime_targets_by_bucket
            .keys()
            .filter(|bucket| {
                !all_bucket_stats.contains_key(*bucket)
                    && !durable_buckets.contains_key(*bucket)
                    && !mrf_observability_buckets.contains_key(*bucket)
            })
            .cloned(),
    );
    let mut buckets = Vec::with_capacity(bucket_names.len());

    for bucket in bucket_names {
        let bucket_stats = all_bucket_stats.get(&bucket);
        let proxy = if let Some(stats) = &stats {
            stats.get_proxy_stats(&bucket).await
        } else {
            Default::default()
        };
        let proxy = ObsBucketReplicationProxySnapshot {
            proxied_get_requests_total: i64_to_u64_floor_zero(proxy.get_total),
            proxied_get_requests_failures: i64_to_u64_floor_zero(proxy.get_failed),
            proxied_head_requests_total: i64_to_u64_floor_zero(proxy.head_total),
            proxied_head_requests_failures: i64_to_u64_floor_zero(proxy.head_failed),
            proxied_put_requests_total: i64_to_u64_floor_zero(proxy.put_total),
            proxied_put_requests_failures: i64_to_u64_floor_zero(proxy.put_failed),
            proxied_put_tagging_requests_total: i64_to_u64_floor_zero(proxy.put_tag_total),
            proxied_put_tagging_requests_failures: i64_to_u64_floor_zero(proxy.put_tag_failed),
            proxied_get_tagging_requests_total: i64_to_u64_floor_zero(proxy.get_tag_total),
            proxied_get_tagging_requests_failures: i64_to_u64_floor_zero(proxy.get_tag_failed),
            proxied_delete_tagging_requests_total: i64_to_u64_floor_zero(proxy.delete_tag_total),
            proxied_delete_tagging_requests_failures: i64_to_u64_floor_zero(proxy.delete_tag_failed),
        };
        let mut runtime = ObsBucketReplicationRuntimeSnapshot {
            targets: Vec::with_capacity(bucket_stats.map(|stats| stats.stats.len()).unwrap_or(0)),
            ..Default::default()
        };

        if let Some(bucket_stats) = bucket_stats {
            for (target_arn, target_stats) in &bucket_stats.stats {
                runtime.total_failed_bytes = runtime
                    .total_failed_bytes
                    .saturating_add(i64_to_u64_floor_zero(target_stats.fail_stats.size));
                runtime.total_failed_count = runtime
                    .total_failed_count
                    .saturating_add(i64_to_u64_floor_zero(target_stats.fail_stats.count));

                let last_min = target_stats.fail_stats.recent_since(Duration::from_secs(60));
                runtime.last_min_failed_bytes = runtime
                    .last_min_failed_bytes
                    .saturating_add(i64_to_u64_floor_zero(last_min.size));
                runtime.last_min_failed_count = runtime
                    .last_min_failed_count
                    .saturating_add(i64_to_u64_floor_zero(last_min.count));

                let last_hour = target_stats.fail_stats.recent_since(Duration::from_secs(60 * 60));
                runtime.last_hour_failed_bytes = runtime
                    .last_hour_failed_bytes
                    .saturating_add(i64_to_u64_floor_zero(last_hour.size));
                runtime.last_hour_failed_count = runtime
                    .last_hour_failed_count
                    .saturating_add(i64_to_u64_floor_zero(last_hour.count));

                runtime.sent_bytes = runtime
                    .sent_bytes
                    .saturating_add(i64_to_u64_floor_zero(target_stats.replicated_size));
                runtime.sent_count = runtime
                    .sent_count
                    .saturating_add(i64_to_u64_floor_zero(target_stats.replicated_count));

                runtime.targets.push(ObsBucketReplicationTargetStatsSnapshot {
                    target_arn: target_arn.clone(),
                    bandwidth_limit_bytes_per_sec: i64_to_u64_floor_zero(target_stats.bandwidth_limit_bytes_per_sec),
                    current_bandwidth_bytes_per_sec: target_stats.current_bandwidth_bytes_per_sec,
                    latency_ms: target_stats.latency.curr,
                });
            }
            runtime.resync_started_count = i64_to_u64_floor_zero(bucket_stats.resync_started_count);
            runtime.resync_completed_count = i64_to_u64_floor_zero(bucket_stats.resync_completed_count);
            runtime.resync_failed_count = i64_to_u64_floor_zero(bucket_stats.resync_failed_count);
            runtime.resync_canceled_count = i64_to_u64_floor_zero(bucket_stats.resync_canceled_count);
            runtime.resync_duration_ms = i64_to_u64_floor_zero(bucket_stats.resync_duration_ms);
            runtime.current_backlog_count = i64_to_u64_floor_zero(bucket_stats.q_stat.curr.count);
            runtime.current_backlog_bytes = i64_to_u64_floor_zero(bucket_stats.q_stat.curr.bytes);
        }
        let durable_bucket = durable_buckets.get(&bucket).cloned().unwrap_or_default();
        let runtime_targets = runtime_targets_by_bucket.remove(&bucket).unwrap_or_default();
        let durable_targets = durable_targets_by_bucket.remove(&bucket).unwrap_or_default();
        let mrf_observability = mrf_observability_buckets.get(&bucket).cloned().unwrap_or_default();
        buckets.push(bucket_replication_stats_snapshot_from_parts(
            bucket,
            runtime,
            proxy,
            ObsBucketReplicationBacklogSnapshot {
                durable_mrf_available,
                durable_bucket,
                runtime_targets,
                durable_targets,
                mrf_observability,
            },
        ));
    }

    buckets
}

pub(crate) async fn obs_replication_site_stats_snapshot(current_data_transfer_rate: f64) -> ObsReplicationSiteStatsSnapshot {
    let Some(stats) = get_global_replication_stats() else {
        return ObsReplicationSiteStatsSnapshot::default();
    };

    let site_metrics = stats.get_sr_metrics_for_node().await;
    let all_bucket_stats = stats.get_all().await;
    // These fields keep the existing metric semantics: cluster-wide sums across bucket targets.
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
    let recent_backlog_count = replication_backlog_count(
        all_bucket_stats
            .values()
            .flat_map(|bucket| bucket.stats.values())
            .map(|stat| stat.failed.count),
        site_metrics.queued.curr.count,
    );

    ObsReplicationSiteStatsSnapshot {
        average_active_workers: site_metrics.active_workers.avg,
        average_queued_bytes: site_metrics.queued.avg.bytes,
        average_queued_count: site_metrics.queued.avg.count,
        average_data_transfer_rate,
        active_workers: i32_to_u64_floor_zero(site_metrics.active_workers.curr),
        current_data_transfer_rate,
        last_minute_queued_bytes: i64_to_u64_floor_zero(site_metrics.queued.last_minute.bytes),
        last_minute_queued_count: i64_to_u64_floor_zero(site_metrics.queued.last_minute.count),
        max_active_workers: i32_to_u64_floor_zero(site_metrics.active_workers.max),
        max_queued_bytes: i64_to_u64_floor_zero(site_metrics.queued.max.bytes),
        max_queued_count: i64_to_u64_floor_zero(site_metrics.queued.max.count),
        max_data_transfer_rate,
        recent_backlog_count,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn obs_replication_numeric_conversions_floor_negative_values() {
        assert_eq!(i64_to_u64_floor_zero(-1), 0);
        assert_eq!(i64_to_u64_floor_zero(42), 42);
        assert_eq!(i32_to_u64_floor_zero(-1), 0);
        assert_eq!(i32_to_u64_floor_zero(42), 42);
    }

    #[test]
    fn replication_backlog_count_uses_failed_targets_and_current_queue() {
        assert_eq!(replication_backlog_count([3, 5].into_iter(), 7), 15);
    }

    #[test]
    fn replication_backlog_count_floors_negative_failed_and_queue_values() {
        assert_eq!(replication_backlog_count([-3, 4].into_iter(), -2), 4);
    }

    #[test]
    fn replication_backlog_count_keeps_legacy_failed_backlog_semantics() {
        assert_eq!(replication_backlog_count([9].into_iter(), 0), 9);
    }

    #[test]
    fn bucket_replication_snapshot_maps_runtime_and_durable_backlog() {
        let snapshot = bucket_replication_stats_snapshot_from_parts(
            "runtime-bucket".to_string(),
            ObsBucketReplicationRuntimeSnapshot {
                current_backlog_count: 3,
                current_backlog_bytes: 4096,
                resync_failed_count: 2,
                ..Default::default()
            },
            ObsBucketReplicationProxySnapshot {
                proxied_get_requests_total: 7,
                proxied_get_requests_failures: 1,
                ..Default::default()
            },
            ObsBucketReplicationBacklogSnapshot {
                durable_mrf_available: true,
                durable_bucket: DurableMrfBucketBacklog {
                    bucket: "runtime-bucket".to_string(),
                    count: 5,
                    bytes: 8192,
                },
                runtime_targets: vec![RuntimeReplicationTargetBacklog {
                    bucket: "runtime-bucket".to_string(),
                    target_arn: "arn:rustfs:replication:target-a".to_string(),
                    count: 3,
                    bytes: 4096,
                }],
                durable_targets: vec![DurableMrfTargetBacklog {
                    bucket: "runtime-bucket".to_string(),
                    target_arn: "arn:rustfs:replication:target-a".to_string(),
                    count: 2,
                    bytes: 4096,
                }],
                mrf_observability: MrfBucketBacklogObservability {
                    bucket: "runtime-bucket".to_string(),
                    pending_count: 1,
                    pending_bytes: 512,
                    dropped_count: 2,
                    missed_count: 3,
                    flush_failure_count: 4,
                    last_flush_duration_millis: 5,
                },
            },
        );

        assert_eq!(snapshot.bucket, "runtime-bucket");
        assert_eq!(snapshot.current_backlog_count, 3);
        assert_eq!(snapshot.current_backlog_bytes, 4096);
        assert!(snapshot.durable_mrf_available);
        assert_eq!(snapshot.durable_mrf_backlog_count, 5);
        assert_eq!(snapshot.durable_mrf_backlog_bytes, 8192);
        assert_eq!(snapshot.target_backlogs.len(), 1);
        assert_eq!(snapshot.target_backlogs[0].target_arn, "arn:rustfs:replication:target-a");
        assert_eq!(snapshot.target_backlogs[0].current_backlog_count, 3);
        assert_eq!(snapshot.target_backlogs[0].current_backlog_bytes, 4096);
        assert_eq!(snapshot.target_backlogs[0].durable_mrf_backlog_count, 2);
        assert_eq!(snapshot.target_backlogs[0].durable_mrf_backlog_bytes, 4096);
        assert_eq!(snapshot.mrf_pending_count, 1);
        assert_eq!(snapshot.mrf_pending_bytes, 512);
        assert_eq!(snapshot.mrf_dropped_count, 2);
        assert_eq!(snapshot.mrf_missed_count, 3);
        assert_eq!(snapshot.mrf_flush_failures, 4);
        assert_eq!(snapshot.mrf_last_flush_duration_millis, 5);
        assert_eq!(snapshot.resync_failed_count, 2);
        assert_eq!(snapshot.proxied_get_requests_total, 7);
        assert_eq!(snapshot.proxied_get_requests_failures, 1);
    }

    #[test]
    fn bucket_replication_snapshot_reports_durable_only_bucket() {
        let snapshot = bucket_replication_stats_snapshot_from_parts(
            "durable-only".to_string(),
            ObsBucketReplicationRuntimeSnapshot::default(),
            ObsBucketReplicationProxySnapshot::default(),
            ObsBucketReplicationBacklogSnapshot {
                durable_mrf_available: true,
                durable_bucket: DurableMrfBucketBacklog {
                    bucket: "durable-only".to_string(),
                    count: 11,
                    bytes: 2048,
                },
                ..Default::default()
            },
        );

        assert_eq!(snapshot.bucket, "durable-only");
        assert_eq!(snapshot.current_backlog_count, 0);
        assert!(snapshot.durable_mrf_available);
        assert_eq!(snapshot.durable_mrf_backlog_count, 11);
        assert_eq!(snapshot.durable_mrf_backlog_bytes, 2048);
    }

    #[test]
    fn bucket_replication_snapshot_reports_mrf_observability_only_bucket() {
        let snapshot = bucket_replication_stats_snapshot_from_parts(
            "mrf-observability-only".to_string(),
            ObsBucketReplicationRuntimeSnapshot::default(),
            ObsBucketReplicationProxySnapshot::default(),
            ObsBucketReplicationBacklogSnapshot {
                durable_mrf_available: true,
                mrf_observability: MrfBucketBacklogObservability {
                    bucket: "mrf-observability-only".to_string(),
                    pending_count: 13,
                    pending_bytes: 4096,
                    dropped_count: 1,
                    missed_count: 2,
                    flush_failure_count: 3,
                    last_flush_duration_millis: 4,
                },
                ..Default::default()
            },
        );

        assert_eq!(snapshot.bucket, "mrf-observability-only");
        assert_eq!(snapshot.current_backlog_count, 0);
        assert_eq!(snapshot.current_backlog_bytes, 0);
        assert!(snapshot.durable_mrf_available);
        assert_eq!(snapshot.durable_mrf_backlog_count, 0);
        assert_eq!(snapshot.durable_mrf_backlog_bytes, 0);
        assert_eq!(snapshot.mrf_pending_count, 13);
        assert_eq!(snapshot.mrf_pending_bytes, 4096);
        assert_eq!(snapshot.mrf_dropped_count, 1);
        assert_eq!(snapshot.mrf_missed_count, 2);
        assert_eq!(snapshot.mrf_flush_failures, 3);
        assert_eq!(snapshot.mrf_last_flush_duration_millis, 4);
    }

    #[test]
    fn bucket_replication_snapshot_preserves_durable_mrf_unavailable_state() {
        let snapshot = bucket_replication_stats_snapshot_from_parts(
            "runtime-only".to_string(),
            ObsBucketReplicationRuntimeSnapshot {
                current_backlog_count: 1,
                current_backlog_bytes: 512,
                ..Default::default()
            },
            ObsBucketReplicationProxySnapshot::default(),
            ObsBucketReplicationBacklogSnapshot::default(),
        );

        assert_eq!(snapshot.current_backlog_count, 1);
        assert_eq!(snapshot.current_backlog_bytes, 512);
        assert!(!snapshot.durable_mrf_available);
        assert_eq!(snapshot.durable_mrf_backlog_count, 0);
        assert_eq!(snapshot.durable_mrf_backlog_bytes, 0);
    }
}

pub(crate) mod metrics {
    pub(crate) use super::storage_contracts::{BucketOperations, BucketOptions, StorageAdminApi};

    pub(crate) use super::{
        ObsBucketBandwidthMonitor, ObsBucketReplicationStatsSnapshot, ObsEcstoreResult, ObsStore,
        obs_bucket_replication_stats_snapshot, obs_expiry_state_handle, obs_get_global_bucket_monitor, obs_get_quota_config,
        obs_get_total_usable_capacity, obs_get_total_usable_capacity_free, obs_is_disk_compression_enabled,
        obs_load_compression_total_from_memory, obs_load_data_usage_from_backend, obs_replication_site_stats_snapshot,
        obs_resolve_object_store_handle, obs_transition_state_handle,
    };
}
