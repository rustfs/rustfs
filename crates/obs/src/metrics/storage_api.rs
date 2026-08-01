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

use std::collections::{HashMap, HashSet};
use std::time::Duration;

pub(crate) use rustfs_ecstore::api::bucket::bandwidth::monitor::Monitor as ObsBucketBandwidthMonitor;
pub(crate) use rustfs_ecstore::api::bucket::metadata_sys::get_quota_config as obs_get_quota_config;
use rustfs_ecstore::api::bucket::replication::{DurableMrfBacklog, get_global_replication_stats, read_durable_mrf_backlog};
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
    pub(crate) targets: Vec<ObsBucketReplicationTargetStatsSnapshot>,
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

#[derive(Debug, Clone, Default, PartialEq)]
struct DurableMrfBucketSnapshot {
    count: u64,
    bytes: u64,
}

fn durable_mrf_bucket_snapshots(backlog: &DurableMrfBacklog) -> HashMap<String, DurableMrfBucketSnapshot> {
    if !backlog.available {
        return HashMap::new();
    }

    let mut buckets = HashMap::new();
    for entry in &backlog.entries {
        let bucket = buckets
            .entry(entry.bucket.clone())
            .or_insert_with(DurableMrfBucketSnapshot::default);
        bucket.count = bucket.count.saturating_add(1);
        bucket.bytes = bucket.bytes.saturating_add(i64_to_u64_floor_zero(entry.size));
    }
    buckets
}

pub(crate) async fn obs_bucket_replication_stats_snapshot() -> Vec<ObsBucketReplicationStatsSnapshot> {
    let stats = get_global_replication_stats();
    let all_bucket_stats = if let Some(stats) = &stats {
        stats.get_all().await
    } else {
        HashMap::new()
    };
    let durable_mrf = if let Some(store) = obs_resolve_object_store_handle() {
        read_durable_mrf_backlog(store).await
    } else {
        DurableMrfBacklog::default()
    };
    let durable_buckets = durable_mrf_bucket_snapshots(&durable_mrf);
    let mut bucket_names = all_bucket_stats.keys().cloned().collect::<HashSet<_>>();
    bucket_names.extend(durable_buckets.keys().cloned());
    let mut buckets = Vec::with_capacity(bucket_names.len());

    for bucket in bucket_names {
        let bucket_stats = all_bucket_stats.get(&bucket);
        let proxy = if let Some(stats) = &stats {
            stats.get_proxy_stats(&bucket).await
        } else {
            Default::default()
        };
        let mut total_failed_bytes = 0u64;
        let mut total_failed_count = 0u64;
        let mut last_min_failed_bytes = 0u64;
        let mut last_min_failed_count = 0u64;
        let mut last_hour_failed_bytes = 0u64;
        let mut last_hour_failed_count = 0u64;
        let mut sent_bytes = 0u64;
        let mut sent_count = 0u64;
        let mut targets = Vec::with_capacity(bucket_stats.map(|stats| stats.stats.len()).unwrap_or(0));

        if let Some(bucket_stats) = bucket_stats {
            for (target_arn, target_stats) in &bucket_stats.stats {
                total_failed_bytes = total_failed_bytes.saturating_add(i64_to_u64_floor_zero(target_stats.fail_stats.size));
                total_failed_count = total_failed_count.saturating_add(i64_to_u64_floor_zero(target_stats.fail_stats.count));

                let last_min = target_stats.fail_stats.recent_since(Duration::from_secs(60));
                last_min_failed_bytes = last_min_failed_bytes.saturating_add(i64_to_u64_floor_zero(last_min.size));
                last_min_failed_count = last_min_failed_count.saturating_add(i64_to_u64_floor_zero(last_min.count));

                let last_hour = target_stats.fail_stats.recent_since(Duration::from_secs(60 * 60));
                last_hour_failed_bytes = last_hour_failed_bytes.saturating_add(i64_to_u64_floor_zero(last_hour.size));
                last_hour_failed_count = last_hour_failed_count.saturating_add(i64_to_u64_floor_zero(last_hour.count));

                sent_bytes = sent_bytes.saturating_add(i64_to_u64_floor_zero(target_stats.replicated_size));
                sent_count = sent_count.saturating_add(i64_to_u64_floor_zero(target_stats.replicated_count));

                targets.push(ObsBucketReplicationTargetStatsSnapshot {
                    target_arn: target_arn.clone(),
                    bandwidth_limit_bytes_per_sec: i64_to_u64_floor_zero(target_stats.bandwidth_limit_bytes_per_sec),
                    current_bandwidth_bytes_per_sec: target_stats.current_bandwidth_bytes_per_sec,
                    latency_ms: target_stats.latency.curr,
                });
            }
        }
        let durable_bucket = durable_buckets.get(&bucket).cloned().unwrap_or_default();

        buckets.push(ObsBucketReplicationStatsSnapshot {
            bucket,
            total_failed_bytes,
            total_failed_count,
            last_min_failed_bytes,
            last_min_failed_count,
            last_hour_failed_bytes,
            last_hour_failed_count,
            sent_bytes,
            sent_count,
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
            resync_started_count: bucket_stats
                .map(|bucket_stats| i64_to_u64_floor_zero(bucket_stats.resync_started_count))
                .unwrap_or(0),
            resync_completed_count: bucket_stats
                .map(|bucket_stats| i64_to_u64_floor_zero(bucket_stats.resync_completed_count))
                .unwrap_or(0),
            resync_failed_count: bucket_stats
                .map(|bucket_stats| i64_to_u64_floor_zero(bucket_stats.resync_failed_count))
                .unwrap_or(0),
            resync_canceled_count: bucket_stats
                .map(|bucket_stats| i64_to_u64_floor_zero(bucket_stats.resync_canceled_count))
                .unwrap_or(0),
            resync_duration_ms: bucket_stats
                .map(|bucket_stats| i64_to_u64_floor_zero(bucket_stats.resync_duration_ms))
                .unwrap_or(0),
            current_backlog_count: bucket_stats
                .map(|bucket_stats| i64_to_u64_floor_zero(bucket_stats.q_stat.curr.count))
                .unwrap_or(0),
            current_backlog_bytes: bucket_stats
                .map(|bucket_stats| i64_to_u64_floor_zero(bucket_stats.q_stat.curr.bytes))
                .unwrap_or(0),
            durable_mrf_available: durable_mrf.available,
            durable_mrf_backlog_count: durable_bucket.count,
            durable_mrf_backlog_bytes: durable_bucket.bytes,
            targets,
        });
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
    use rustfs_ecstore::api::bucket::replication::{MrfOpKind, MrfReplicateEntry};

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
    fn durable_mrf_bucket_snapshots_aggregate_valid_entries_by_bucket() {
        let snapshots = durable_mrf_bucket_snapshots(&DurableMrfBacklog {
            available: true,
            entries: vec![
                MrfReplicateEntry {
                    bucket: "b1".to_string(),
                    object: "o1".to_string(),
                    size: 1024,
                    version_id: None,
                    retry_count: 0,
                    op: MrfOpKind::Object,
                    delete_marker_version_id: None,
                    delete_marker: false,
                    delete_marker_mtime: None,
                },
                MrfReplicateEntry {
                    bucket: "b1".to_string(),
                    object: "o2".to_string(),
                    size: 512,
                    version_id: None,
                    retry_count: 0,
                    op: MrfOpKind::Object,
                    delete_marker_version_id: None,
                    delete_marker: false,
                    delete_marker_mtime: None,
                },
                MrfReplicateEntry {
                    bucket: "b2".to_string(),
                    object: "delete".to_string(),
                    size: 0,
                    version_id: None,
                    retry_count: 0,
                    op: MrfOpKind::Delete,
                    delete_marker_version_id: None,
                    delete_marker: false,
                    delete_marker_mtime: None,
                },
            ],
        });

        assert_eq!(snapshots["b1"].count, 2);
        assert_eq!(snapshots["b1"].bytes, 1536);
        assert_eq!(snapshots["b2"].count, 1);
        assert_eq!(snapshots["b2"].bytes, 0);
    }

    #[test]
    fn durable_mrf_bucket_snapshots_do_not_report_unavailable_as_zero() {
        let snapshots = durable_mrf_bucket_snapshots(&DurableMrfBacklog {
            available: false,
            entries: vec![MrfReplicateEntry {
                bucket: "b1".to_string(),
                object: "o1".to_string(),
                size: 1024,
                version_id: None,
                retry_count: 0,
                op: MrfOpKind::Object,
                delete_marker_version_id: None,
                delete_marker: false,
                delete_marker_mtime: None,
            }],
        });

        assert!(snapshots.is_empty());
    }
}

pub(crate) mod metrics {
    pub(crate) use super::storage_contracts::{BucketOperations, BucketOptions, StorageAdminApi};

    pub(crate) use super::{
        ObsBucketBandwidthMonitor, ObsEcstoreResult, ObsStore, obs_bucket_replication_stats_snapshot, obs_expiry_state_handle,
        obs_get_global_bucket_monitor, obs_get_quota_config, obs_get_total_usable_capacity, obs_get_total_usable_capacity_free,
        obs_is_disk_compression_enabled, obs_load_compression_total_from_memory, obs_load_data_usage_from_backend,
        obs_replication_site_stats_snapshot, obs_resolve_object_store_handle, obs_transition_state_handle,
    };
}
