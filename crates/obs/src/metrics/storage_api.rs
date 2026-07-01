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

use std::time::Duration;

pub(crate) use rustfs_ecstore::api::bucket::bandwidth::monitor::Monitor as ObsBucketBandwidthMonitor;
pub(crate) use rustfs_ecstore::api::bucket::metadata_sys::get_quota_config as obs_get_quota_config;
use rustfs_ecstore::api::bucket::replication::get_global_replication_stats;
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

pub(crate) async fn obs_bucket_replication_stats_snapshot() -> Vec<ObsBucketReplicationStatsSnapshot> {
    let Some(stats) = get_global_replication_stats() else {
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
                target_arn,
                bandwidth_limit_bytes_per_sec: i64_to_u64_floor_zero(target_stats.bandwidth_limit_bytes_per_sec),
                current_bandwidth_bytes_per_sec: target_stats.current_bandwidth_bytes_per_sec,
                latency_ms: target_stats.latency.curr,
            });
        }

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
    let recent_backlog_count = stats.mrf_stats.values().copied().filter(|value| *value > 0).sum::<i64>();

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
        recent_backlog_count: i64_to_u64_floor_zero(recent_backlog_count),
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
