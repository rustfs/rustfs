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

use crate::metrics::collectors::{
    BucketReplicationStats as MetricBucketReplicationStats, BucketReplicationTargetStats,
    ReplicationStats as MetricReplicationStats,
};
use rustfs_ecstore::api::{
    bucket as ecstore_bucket, capacity as ecstore_capacity, data_usage as ecstore_data_usage, error as ecstore_error,
    global as ecstore_global, storage as ecstore_storage,
};
use rustfs_storage_api::StorageAdminApi;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

pub(crate) type ObsStore = ecstore_storage::ECStore;
type ObsStorageInfo = <ObsStore as StorageAdminApi>::StorageInfo;

pub(crate) struct ObsDataUsageInfo {
    pub(crate) buckets_count: u64,
    pub(crate) objects_total_count: u64,
    pub(crate) versions_total_count: u64,
    pub(crate) delete_markers_total_count: u64,
    pub(crate) objects_total_size: u64,
    pub(crate) buckets_usage: HashMap<String, ObsBucketUsageInfo>,
}

pub(crate) struct ObsBucketUsageInfo {
    pub(crate) size: u64,
    pub(crate) objects_count: u64,
    pub(crate) object_size_histogram: HashMap<String, u64>,
    pub(crate) object_versions_histogram: HashMap<String, u64>,
    pub(crate) versions_count: u64,
    pub(crate) delete_markers_count: u64,
}

pub(crate) async fn load_obs_data_usage_from_backend(store: Arc<ObsStore>) -> ecstore_error::Result<ObsDataUsageInfo> {
    let data_usage = ecstore_data_usage::load_data_usage_from_backend(store).await?;

    Ok(ObsDataUsageInfo {
        buckets_count: data_usage.buckets_count,
        objects_total_count: data_usage.objects_total_count,
        versions_total_count: data_usage.versions_total_count,
        delete_markers_total_count: data_usage.delete_markers_total_count,
        objects_total_size: data_usage.objects_total_size,
        buckets_usage: data_usage
            .buckets_usage
            .into_iter()
            .map(|(bucket, usage)| {
                (
                    bucket,
                    ObsBucketUsageInfo {
                        size: usage.size,
                        objects_count: usage.objects_count,
                        object_size_histogram: usage.object_size_histogram,
                        object_versions_histogram: usage.object_versions_histogram,
                        versions_count: usage.versions_count,
                        delete_markers_count: usage.delete_markers_count,
                    },
                )
            })
            .collect(),
    })
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ObsBucketReplicationBandwidthStats {
    pub(crate) bucket: String,
    pub(crate) target_arn: String,
    pub(crate) limit_bytes_per_sec: i64,
    pub(crate) current_bandwidth_bytes_per_sec: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ObsIlmRuntimeSnapshot {
    pub(crate) expiry_pending_tasks: u64,
    pub(crate) transition_active_tasks: u64,
    pub(crate) transition_pending_tasks: u64,
    pub(crate) transition_missed_immediate_tasks: u64,
    pub(crate) transition_queue_full_tasks: u64,
    pub(crate) transition_queue_send_timeout_tasks: u64,
    pub(crate) transition_compensation_scheduled_tasks: u64,
    pub(crate) transition_compensation_running_tasks: u64,
}

pub(crate) fn resolve_obs_object_store_handle() -> Option<Arc<ObsStore>> {
    ecstore_global::resolve_object_store_handle()
}

pub(crate) fn obs_total_usable_capacity_bytes(storage_info: &ObsStorageInfo) -> u64 {
    usize_to_u64_saturating(ecstore_capacity::get_total_usable_capacity(&storage_info.disks, storage_info))
}

pub(crate) fn obs_total_usable_capacity_free_bytes(storage_info: &ObsStorageInfo) -> u64 {
    usize_to_u64_saturating(ecstore_capacity::get_total_usable_capacity_free(&storage_info.disks, storage_info))
}

pub(crate) async fn obs_bucket_quota_limit_bytes(bucket: &str) -> u64 {
    ecstore_bucket::metadata_sys::get_quota_config(bucket)
        .await
        .ok()
        .and_then(|(quota, _)| quota.get_quota_limit())
        .unwrap_or(0)
}

pub(crate) fn obs_bucket_monitor_available() -> bool {
    ecstore_global::get_global_bucket_monitor().is_some()
}

pub(crate) fn obs_bucket_replication_bandwidth_stats() -> Option<Vec<ObsBucketReplicationBandwidthStats>> {
    let monitor = ecstore_global::get_global_bucket_monitor()?;
    Some(
        monitor
            .get_report(|_| true)
            .bucket_stats
            .into_iter()
            .map(|(opts, details)| ObsBucketReplicationBandwidthStats {
                bucket: opts.name,
                target_arn: opts.replication_arn,
                limit_bytes_per_sec: details.limit_bytes_per_sec,
                current_bandwidth_bytes_per_sec: details.current_bandwidth_bytes_per_sec,
            })
            .collect(),
    )
}

pub(crate) async fn obs_ilm_runtime_snapshot() -> ObsIlmRuntimeSnapshot {
    let expiry_pending_tasks = {
        let expiry = ecstore_bucket::lifecycle::bucket_lifecycle_ops::GLOBAL_ExpiryState
            .read()
            .await;
        usize_to_u64_saturating(expiry.pending_tasks())
    };
    let transition = &ecstore_bucket::lifecycle::bucket_lifecycle_ops::GLOBAL_TransitionState;

    ObsIlmRuntimeSnapshot {
        expiry_pending_tasks,
        transition_active_tasks: i64_to_u64_floor_zero(transition.active_tasks()),
        transition_pending_tasks: usize_to_u64_saturating(transition.pending_tasks()),
        transition_missed_immediate_tasks: i64_to_u64_floor_zero(transition.missed_immediate_tasks()),
        transition_queue_full_tasks: i64_to_u64_floor_zero(transition.queue_full_tasks()),
        transition_queue_send_timeout_tasks: i64_to_u64_floor_zero(transition.queue_send_timeout_tasks()),
        transition_compensation_scheduled_tasks: i64_to_u64_floor_zero(transition.compensation_scheduled_tasks()),
        transition_compensation_running_tasks: i64_to_u64_floor_zero(transition.compensation_running_tasks()),
    }
}

pub(crate) async fn obs_bucket_replication_detail_stats() -> Vec<MetricBucketReplicationStats> {
    let Some(stats) = ecstore_bucket::replication::GLOBAL_REPLICATION_STATS.get() else {
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
            total_failed_bytes += i64_to_u64_floor_zero(target_stats.fail_stats.size);
            total_failed_count += i64_to_u64_floor_zero(target_stats.fail_stats.count);

            let last_min = target_stats.fail_stats.recent_since(Duration::from_secs(60));
            last_min_failed_bytes += i64_to_u64_floor_zero(last_min.size);
            last_min_failed_count += i64_to_u64_floor_zero(last_min.count);

            let last_hour = target_stats.fail_stats.recent_since(Duration::from_secs(60 * 60));
            last_hour_failed_bytes += i64_to_u64_floor_zero(last_hour.size);
            last_hour_failed_count += i64_to_u64_floor_zero(last_hour.count);

            sent_bytes += i64_to_u64_floor_zero(target_stats.replicated_size);
            sent_count += i64_to_u64_floor_zero(target_stats.replicated_count);

            targets.push(BucketReplicationTargetStats {
                target_arn,
                bandwidth_limit_bytes_per_sec: i64_to_u64_floor_zero(target_stats.bandwidth_limit_bytes_per_sec),
                current_bandwidth_bytes_per_sec: target_stats.current_bandwidth_bytes_per_sec,
                latency_ms: target_stats.latency.curr,
            });
        }

        buckets.push(MetricBucketReplicationStats {
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

pub(crate) async fn obs_site_replication_stats() -> MetricReplicationStats {
    let Some(stats) = ecstore_bucket::replication::GLOBAL_REPLICATION_STATS.get() else {
        return MetricReplicationStats::default();
    };

    let site_metrics = stats.get_sr_metrics_for_node().await;
    let current_data_transfer_rate = obs_bucket_replication_bandwidth_stats()
        .into_iter()
        .flatten()
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
    let recent_backlog_count = stats.mrf_stats.values().copied().filter(|value| *value > 0).sum::<i64>();

    MetricReplicationStats {
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

fn usize_to_u64_saturating(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

fn i64_to_u64_floor_zero(value: i64) -> u64 {
    u64::try_from(value.max(0)).unwrap_or(0)
}

fn i32_to_u64_floor_zero(value: i32) -> u64 {
    u64::try_from(value.max(0)).unwrap_or(0)
}
