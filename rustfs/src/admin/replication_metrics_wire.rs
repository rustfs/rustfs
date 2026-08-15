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

//! Serialize-only wire projections of the internal replication statistics
//! onto the minio-go `replication.Metrics` / `replication.MetricsV2` json
//! shapes consumed by `mc replicate status` (`?replication-metrics[=2]` and
//! the admin `replicationmetrics` endpoint).
//!
//! Red line: the internal `BucketStats` family in
//! `crates/replication/src/stats.rs` is ALSO the intra-cluster peer-RPC wire
//! format — `node_service.rs` encodes it with `rmp_serde::to_vec_named`, so
//! its Rust field names travel between nodes as msgpack map keys. Renaming
//! those serde names would break mixed-version clusters mid rolling upgrade.
//! All madmin/minio-go interop therefore happens in these DTOs; never add
//! `#[serde(rename)]` to the internal structs instead.
//!
//! Field names below are the exact json tags of minio-go
//! `pkg/replication/replication.go` (v7.0.91). Keys minio-go does not know
//! are RustFS extensions; Go decoders ignore unknown keys. `max`/`peak` are
//! both emitted for the queue peak because the MinIO server writes `max`
//! while minio-go reads `peak` (an upstream drift); emitting both keeps every
//! decoder working.

use serde::Serialize;
use std::collections::HashMap;
use std::time::Duration;

use crate::admin::storage_api::replication::{
    BucketReplicationStat as InternalReplicationStat, BucketReplicationStats as InternalReplicationStats, BucketStats,
    InQueueMetric as InternalInQueueMetric, XferStats as InternalXferStats,
};

/// minio-go `replication.RStat`.
#[derive(Debug, Default, Clone, Copy, Serialize)]
pub(crate) struct RStatWire {
    #[serde(rename = "count")]
    pub count: f64,
    #[serde(rename = "bytes")]
    pub bytes: i64,
}

/// minio-go `replication.TimedErrStats`.
#[derive(Debug, Default, Clone, Copy, Serialize)]
pub(crate) struct TimedErrStatsWire {
    #[serde(rename = "lastMinute")]
    pub last_minute: RStatWire,
    #[serde(rename = "lastHour")]
    pub last_hour: RStatWire,
    #[serde(rename = "totals")]
    pub totals: RStatWire,
}

impl TimedErrStatsWire {
    fn add(self, other: TimedErrStatsWire) -> TimedErrStatsWire {
        fn add(a: RStatWire, b: RStatWire) -> RStatWire {
            RStatWire {
                count: a.count + b.count,
                bytes: a.bytes.saturating_add(b.bytes),
            }
        }
        TimedErrStatsWire {
            last_minute: add(self.last_minute, other.last_minute),
            last_hour: add(self.last_hour, other.last_hour),
            totals: add(self.totals, other.totals),
        }
    }
}

/// minio-go `replication.QStat`.
#[derive(Debug, Default, Clone, Copy, Serialize)]
pub(crate) struct QStatWire {
    #[serde(rename = "count")]
    pub count: f64,
    #[serde(rename = "bytes")]
    pub bytes: f64,
}

/// minio-go `replication.InQueueMetric`, with the queue peak emitted under
/// both `peak` (minio-go tag) and `max` (MinIO server tag).
#[derive(Debug, Default, Clone, Copy, Serialize)]
pub(crate) struct InQueueMetricWire {
    #[serde(rename = "curr")]
    pub curr: QStatWire,
    #[serde(rename = "avg")]
    pub avg: QStatWire,
    #[serde(rename = "max")]
    pub max: QStatWire,
    #[serde(rename = "peak")]
    pub peak: QStatWire,
}

impl From<&InternalInQueueMetric> for InQueueMetricWire {
    fn from(metric: &InternalInQueueMetric) -> Self {
        fn qstat(bytes: i64, count: i64) -> QStatWire {
            QStatWire {
                count: count as f64,
                bytes: bytes as f64,
            }
        }
        let peak = qstat(metric.max.bytes, metric.max.count);
        InQueueMetricWire {
            curr: qstat(metric.curr.bytes, metric.curr.count),
            avg: qstat(metric.avg.bytes, metric.avg.count),
            max: peak,
            peak,
        }
    }
}

/// minio-go `replication.XferStats`.
#[derive(Debug, Default, Clone, Copy, Serialize)]
pub(crate) struct XferStatsWire {
    #[serde(rename = "avgRate")]
    pub avg_rate: f64,
    #[serde(rename = "peakRate")]
    pub peak_rate: f64,
    #[serde(rename = "currRate")]
    pub curr_rate: f64,
}

#[derive(Default)]
struct XferStatsAverage {
    sum: XferStatsWire,
    active: u32,
}

impl XferStatsAverage {
    fn add_active(&mut self, stats: XferStatsWire) {
        if stats.peak_rate <= 0.0 {
            return;
        }
        self.add_raw(stats);
        self.active += 1;
    }

    fn add_raw(&mut self, stats: XferStatsWire) {
        self.sum.avg_rate += stats.avg_rate;
        self.sum.curr_rate += stats.curr_rate;
        self.sum.peak_rate = self.sum.peak_rate.max(stats.peak_rate);
    }

    fn finish(self) -> XferStatsWire {
        let active = self.active;
        self.finish_with_divisor(active)
    }

    fn finish_with_divisor(self, divisor: u32) -> XferStatsWire {
        if divisor == 0 {
            return self.sum;
        }
        XferStatsWire {
            avg_rate: self.sum.avg_rate / f64::from(divisor),
            peak_rate: self.sum.peak_rate,
            curr_rate: self.sum.curr_rate / f64::from(divisor),
        }
    }
}

impl From<&InternalXferStats> for XferStatsWire {
    fn from(stats: &InternalXferStats) -> Self {
        XferStatsWire {
            avg_rate: stats.avg,
            peak_rate: stats.peak,
            curr_rate: stats.curr,
        }
    }
}

/// minio-go `replication.WorkerStat`. RustFS does not track per-bucket worker
/// occupancy yet, so this always reports zeros.
#[derive(Debug, Default, Clone, Copy, Serialize)]
pub(crate) struct WorkerStatWire {
    #[serde(rename = "curr")]
    pub curr: i32,
    #[serde(rename = "avg")]
    pub avg: f32,
    #[serde(rename = "max")]
    pub max: i32,
}

/// minio-go `replication.ReplMRFStats`. RustFS does not track the 5-minute /
/// dropped MRF windows, so this always reports zeros; the durable backlog is
/// enumerable via `/v3/replication/mrf` instead.
#[derive(Debug, Default, Clone, Copy, Serialize)]
pub(crate) struct ReplMrfStatsWire {
    #[serde(rename = "failedCount_last5min")]
    pub last_failed_count: u64,
    #[serde(rename = "droppedCount_since_uptime")]
    pub total_dropped_count: u64,
    #[serde(rename = "droppedBytes_since_uptime")]
    pub total_dropped_bytes: u64,
}

/// minio-go `replication.CounterSummary`.
#[derive(Debug, Default, Clone, Copy, Serialize)]
pub(crate) struct CounterSummaryWire {
    #[serde(rename = "last1hr")]
    pub last1hr: u64,
    #[serde(rename = "last1m")]
    pub last1m: u64,
    #[serde(rename = "total")]
    pub total: u64,
}

/// minio-go `replication.TargetMetrics` (one remote target / ARN).
#[derive(Debug, Default, Serialize)]
pub(crate) struct TargetMetricsWire {
    #[serde(rename = "replicationCount")]
    pub replicated_count: i64,
    #[serde(rename = "completedReplicationSize")]
    pub replicated_size: i64,
    /// Bandwidth limit for this target. The tag says "bits" but both MinIO
    /// and minio-go treat the value as bytes/sec; keep bytes/sec.
    #[serde(rename = "limitInBits")]
    pub bandwidth_limit_bytes_per_sec: i64,
    #[serde(rename = "currentBandwidth")]
    pub current_bandwidth_bytes_per_sec: f64,
    #[serde(rename = "failed")]
    pub failed: TimedErrStatsWire,
    #[serde(rename = "failedReplicationSize")]
    pub failed_size: i64,
    #[serde(rename = "failedReplicationCount")]
    pub failed_count: i64,
}

fn target_timed_err_stats(stat: &InternalReplicationStat) -> TimedErrStatsWire {
    // Cluster aggregation merges FailStats without the process-local samples,
    // so the serializable window snapshots (refreshed at each node's
    // collection point, summed by merge) are authoritative here; the live
    // samples only ever agree with or lag them, so take the larger.
    let sampled_minute = stat.fail_stats.recent_since(Duration::from_secs(60));
    let sampled_hour = stat.fail_stats.recent_since(Duration::from_secs(3600));
    let window = |sampled_count: i64, sampled_size: i64, snapshot_count: i64, snapshot_size: i64| RStatWire {
        count: sampled_count.max(snapshot_count) as f64,
        bytes: sampled_size.max(snapshot_size),
    };
    TimedErrStatsWire {
        last_minute: window(
            sampled_minute.count,
            sampled_minute.size,
            stat.fail_stats.last_minute.count,
            stat.fail_stats.last_minute.size,
        ),
        last_hour: window(
            sampled_hour.count,
            sampled_hour.size,
            stat.fail_stats.last_hour.count,
            stat.fail_stats.last_hour.size,
        ),
        totals: RStatWire {
            count: stat.failed.count as f64,
            bytes: stat.failed.size,
        },
    }
}

impl From<&InternalReplicationStat> for TargetMetricsWire {
    fn from(stat: &InternalReplicationStat) -> Self {
        TargetMetricsWire {
            replicated_count: stat.replicated_count,
            replicated_size: stat.replicated_size,
            bandwidth_limit_bytes_per_sec: stat.bandwidth_limit_bytes_per_sec,
            current_bandwidth_bytes_per_sec: stat.current_bandwidth_bytes_per_sec,
            failed: target_timed_err_stats(stat),
            failed_size: stat.failed.size,
            failed_count: stat.failed.count,
        }
    }
}

/// minio-go `replication.Metrics` — the `currStats` member of `MetricsV2` and
/// the whole v1 response body. The trailing snake_case fields are RustFS
/// source-health extension keys (ignored by Go decoders) carried over from
/// the previous response shape.
#[derive(Debug, Default, Serialize)]
pub(crate) struct MetricsWire {
    #[serde(rename = "Stats")]
    pub stats: HashMap<String, TargetMetricsWire>,
    #[serde(rename = "completedReplicationSize")]
    pub replicated_size: i64,
    #[serde(rename = "replicaSize")]
    pub replica_size: i64,
    #[serde(rename = "replicaCount")]
    pub replica_count: i64,
    #[serde(rename = "replicationCount")]
    pub replicated_count: i64,
    #[serde(rename = "failed")]
    pub failed: TimedErrStatsWire,
    #[serde(rename = "queued")]
    pub queued: InQueueMetricWire,
    // RustFS extension keys (source health of the aggregation).
    pub provider_available: bool,
    pub cluster_complete: bool,
    pub observed_node_count: u32,
    pub expected_node_count: u32,
}

impl From<&InternalReplicationStats> for MetricsWire {
    fn from(stats: &InternalReplicationStats) -> Self {
        let mut failed = TimedErrStatsWire::default();
        let mut targets = HashMap::with_capacity(stats.stats.len());
        for (arn, stat) in &stats.stats {
            let target = TargetMetricsWire::from(stat);
            failed = failed.add(target.failed);
            targets.insert(arn.clone(), target);
        }
        MetricsWire {
            stats: targets,
            replicated_size: stats.replicated_size,
            replica_size: stats.replica_size,
            replica_count: stats.replica_count,
            replicated_count: stats.replicated_count,
            failed,
            queued: InQueueMetricWire::from(&stats.q_stat),
            provider_available: stats.provider_available,
            cluster_complete: stats.cluster_complete,
            observed_node_count: stats.observed_node_count,
            expected_node_count: stats.expected_node_count,
        }
    }
}

/// minio-go `replication.ReplQNodeStats`.
#[derive(Debug, Default, Serialize)]
pub(crate) struct ReplQNodeStatsWire {
    #[serde(rename = "nodeName")]
    pub node_name: String,
    #[serde(rename = "uptime")]
    pub uptime: i64,
    #[serde(rename = "activeWorkers")]
    pub workers: WorkerStatWire,
    #[serde(rename = "transferSummary")]
    pub xfer_stats: XferSummaryWire,
    #[serde(rename = "tgtTransferStats")]
    pub tgt_xfer_stats: TargetXferSummaryWire,
    #[serde(rename = "queueStats")]
    pub q_stats: InQueueMetricWire,
    #[serde(rename = "mrfStats")]
    pub mrf_stats: ReplMrfStatsWire,
    #[serde(rename = "retries")]
    pub retries: CounterSummaryWire,
    #[serde(rename = "errors")]
    pub errors: CounterSummaryWire,
}

/// minio-go `replication.ReplQueueStats`.
#[derive(Debug, Default, Serialize)]
pub(crate) struct ReplQueueStatsWire {
    #[serde(rename = "nodes")]
    pub nodes: Vec<ReplQNodeStatsWire>,
}

/// minio-go `replication.MetricsV2` — the `?replication-metrics=2` body.
#[derive(Debug, Default, Serialize)]
pub(crate) struct MetricsV2Wire {
    #[serde(rename = "uptime")]
    pub uptime: i64,
    #[serde(rename = "currStats")]
    pub current_stats: MetricsWire,
    #[serde(rename = "queueStats")]
    pub queue_stats: ReplQueueStatsWire,
    #[serde(rename = "downtimeInfo")]
    pub downtime_info: HashMap<String, serde_json::Value>,
}

/// `transferSummary` map keyed by minio-go `MetricName` (Large/Small/Total).
type XferSummaryWire = HashMap<&'static str, XferStatsWire>;
/// `tgtTransferStats` map keyed by target ARN.
type TargetXferSummaryWire = HashMap<String, XferSummaryWire>;

fn transfer_summaries(stats: &InternalReplicationStats) -> (XferSummaryWire, TargetXferSummaryWire) {
    let mut per_target: TargetXferSummaryWire = HashMap::new();
    let mut large_summary = XferStatsAverage::default();
    let mut small_summary = XferStatsAverage::default();
    let mut total_summary = XferStatsAverage::default();
    let mut active_targets = 0;
    for (arn, stat) in &stats.stats {
        let large = XferStatsWire::from(&stat.xfer_rate_lrg);
        let small = XferStatsWire::from(&stat.xfer_rate_sml);
        let mut target_total = XferStatsAverage::default();
        target_total.add_active(large);
        target_total.add_active(small);
        let total = target_total.finish();
        per_target.insert(arn.clone(), HashMap::from([("Large", large), ("Small", small), ("Total", total)]));
        if large.peak_rate > 0.0 || small.peak_rate > 0.0 {
            active_targets += 1;
            large_summary.add_raw(large);
            small_summary.add_raw(small);
            total_summary.add_raw(large);
            total_summary.add_raw(small);
        }
    }
    let summary = HashMap::from([
        ("Large", large_summary.finish_with_divisor(active_targets)),
        ("Small", small_summary.finish_with_divisor(active_targets)),
        ("Total", total_summary.finish_with_divisor(active_targets)),
    ]);
    (summary, per_target)
}

impl MetricsV2Wire {
    /// Project the aggregated internal stats onto the `MetricsV2` shape.
    ///
    /// The aggregation path leaves `queue_stats.nodes` empty today, so a
    /// single node entry is synthesized from the bucket queue snapshot —
    /// `mc replicate status` derives its queue/worker panels from
    /// `queueStats.nodes` and treats an empty list as "no data".
    pub(crate) fn from_stats(bucket_stats: &BucketStats, node_name: &str) -> Self {
        let (xfer_stats, tgt_xfer_stats) = transfer_summaries(&bucket_stats.replication_stats);
        let mut nodes: Vec<ReplQNodeStatsWire> = bucket_stats
            .queue_stats
            .nodes
            .iter()
            .map(|node| ReplQNodeStatsWire {
                node_name: node_name.to_string(),
                uptime: bucket_stats.uptime,
                q_stats: InQueueMetricWire::from(&node.q_stats),
                ..Default::default()
            })
            .collect();
        if nodes.is_empty() {
            nodes.push(ReplQNodeStatsWire {
                node_name: node_name.to_string(),
                uptime: bucket_stats.uptime,
                q_stats: InQueueMetricWire::from(&bucket_stats.replication_stats.q_stat),
                xfer_stats: xfer_stats.clone(),
                tgt_xfer_stats: tgt_xfer_stats.clone(),
                ..Default::default()
            });
        } else {
            // Attach the transfer summaries to the first node; the internal
            // snapshot does not attribute transfer rates per node.
            if let Some(first) = nodes.first_mut() {
                first.xfer_stats = xfer_stats.clone();
                first.tgt_xfer_stats = tgt_xfer_stats.clone();
            }
        }

        MetricsV2Wire {
            uptime: bucket_stats.uptime,
            current_stats: MetricsWire::from(&bucket_stats.replication_stats),
            queue_stats: ReplQueueStatsWire { nodes },
            downtime_info: HashMap::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_bucket_stats() -> BucketStats {
        let mut stats = BucketStats {
            uptime: 42,
            ..Default::default()
        };
        stats.replication_stats.replica_count = 2;
        stats.replication_stats.replica_size = 128;
        stats.replication_stats.replicated_count = 9;
        stats.replication_stats.replicated_size = 4096;
        let target = stats
            .replication_stats
            .stats
            .entry("arn:minio:replication::t:b".to_string())
            .or_default();
        target.replicated_count = 9;
        target.replicated_size = 4096;
        target.failed.count = 3;
        target.failed.size = 900;
        target.bandwidth_limit_bytes_per_sec = 1024;
        target.current_bandwidth_bytes_per_sec = 512.5;
        stats
            .replication_stats
            .q_stat
            .curr
            .now_count
            .store(4, std::sync::atomic::Ordering::Relaxed);
        stats
            .replication_stats
            .q_stat
            .curr
            .now_bytes
            .store(1200, std::sync::atomic::Ordering::Relaxed);
        stats.replication_stats.q_stat = stats.replication_stats.q_stat.snapshot();
        stats
    }

    #[test]
    fn metrics_wire_matches_minio_go_tags() {
        let stats = sample_bucket_stats();
        let json = serde_json::to_value(MetricsWire::from(&stats.replication_stats)).expect("v1 wire should serialize");

        assert_eq!(json["replicaCount"], 2);
        assert_eq!(json["replicaSize"], 128);
        assert_eq!(json["replicationCount"], 9);
        assert_eq!(json["completedReplicationSize"], 4096);
        assert_eq!(json["queued"]["curr"]["count"], 4.0);
        assert_eq!(json["queued"]["curr"]["bytes"], 1200.0);
        let target = &json["Stats"]["arn:minio:replication::t:b"];
        assert_eq!(target["replicationCount"], 9);
        assert_eq!(target["completedReplicationSize"], 4096);
        assert_eq!(target["limitInBits"], 1024);
        assert_eq!(target["currentBandwidth"], 512.5);
        // failed is the madmin TimedErrStats envelope, not the internal
        // {count,size} pair.
        assert_eq!(target["failed"]["totals"]["count"], 3.0);
        assert_eq!(target["failed"]["totals"]["bytes"], 900);
        assert!(target["failed"].get("count").is_none());
        // Aggregate failed mirrors the per-target totals.
        assert_eq!(json["failed"]["totals"]["count"], 3.0);
    }

    #[test]
    fn metrics_v2_wire_synthesizes_queue_node() {
        let stats = sample_bucket_stats();
        let json = serde_json::to_value(MetricsV2Wire::from_stats(&stats, "node-1:9000")).expect("v2 wire should serialize");

        assert_eq!(json["uptime"], 42);
        assert_eq!(json["currStats"]["replicaCount"], 2);
        let node = &json["queueStats"]["nodes"][0];
        assert_eq!(node["nodeName"], "node-1:9000");
        assert_eq!(node["uptime"], 42);
        assert_eq!(node["queueStats"]["curr"]["count"], 4.0);
        // The queue peak is emitted under both the minio-go tag (`peak`) and
        // the MinIO server tag (`max`).
        assert_eq!(node["queueStats"]["peak"], node["queueStats"]["max"]);
        assert!(node["activeWorkers"].get("curr").is_some());
        assert!(node["transferSummary"].get("Total").is_some());
        assert_eq!(json["downtimeInfo"], serde_json::json!({}));
    }

    /// minio-go's transferSummary labels mean >= 128 MiB for Large; the
    /// producer must bin on the same boundary (MIN_LARGE_OBJ_SIZE, shared
    /// with the worker-pool split), or a 2 MiB replication shows under Large
    /// while Small stays zero.
    #[test]
    fn transfer_summary_bins_on_the_128_mib_boundary() {
        const MIB: i64 = 1024 * 1024;
        let mut stats = BucketStats::default();
        let stat = stats
            .replication_stats
            .stats
            .entry("arn:minio:replication::t:b".to_string())
            .or_default();
        stat.update_xfer_rate(2 * MIB, std::time::Duration::from_secs(1));
        stat.update_xfer_rate(127 * MIB, std::time::Duration::from_secs(1));
        stat.update_xfer_rate(128 * MIB, std::time::Duration::from_secs(1));

        let json = serde_json::to_value(MetricsV2Wire::from_stats(&stats, "node-1")).expect("v2 wire should serialize");
        let summary = &json["queueStats"]["nodes"][0]["tgtTransferStats"]["arn:minio:replication::t:b"];
        let small_peak = summary["Small"]["peakRate"].as_f64().expect("Small peakRate");
        let large_peak = summary["Large"]["peakRate"].as_f64().expect("Large peakRate");
        assert!(
            (small_peak - (127 * MIB) as f64).abs() < 1.0,
            "2 MiB and 127 MiB transfers must bin as Small (peak {small_peak})"
        );
        assert!(
            (large_peak - (128 * MIB) as f64).abs() < 1.0,
            "exactly 128 MiB must bin as Large (peak {large_peak})"
        );
    }

    #[test]
    fn transfer_summaries_average_active_bins_and_targets() {
        let mut stats = BucketStats::default();
        let first = stats.replication_stats.stats.entry("target-a".to_string()).or_default();
        first.xfer_rate_sml.avg = 50.0;
        first.xfer_rate_sml.curr = 40.0;
        first.xfer_rate_sml.peak = 60.0;
        first.xfer_rate_lrg.avg = 100.0;
        first.xfer_rate_lrg.curr = 80.0;
        first.xfer_rate_lrg.peak = 120.0;

        let second = stats.replication_stats.stats.entry("target-b".to_string()).or_default();
        second.xfer_rate_sml.avg = 30.0;
        second.xfer_rate_sml.curr = 20.0;
        second.xfer_rate_sml.peak = 40.0;

        let json = serde_json::to_value(MetricsV2Wire::from_stats(&stats, "node-1")).expect("v2 wire should serialize");
        let node = &json["queueStats"]["nodes"][0];
        let target_a = &node["tgtTransferStats"]["target-a"]["Total"];
        assert_eq!(target_a["avgRate"], 75.0);
        assert_eq!(target_a["currRate"], 60.0);
        assert_eq!(target_a["peakRate"], 120.0);

        let summary = &node["transferSummary"];
        assert_eq!(summary["Small"]["avgRate"], 40.0);
        assert_eq!(summary["Small"]["currRate"], 30.0);
        assert_eq!(summary["Large"]["avgRate"], 50.0);
        assert_eq!(summary["Total"]["avgRate"], 90.0);
        assert_eq!(summary["Total"]["currRate"], 70.0);
        assert_eq!(summary["Total"]["peakRate"], 120.0);
    }

    /// Review regression: both metrics endpoints aggregate first, and the
    /// FailStats merge drops the process-local samples — the rolling windows
    /// must survive a peer-RPC round trip plus aggregation and still reach
    /// the wire body.
    #[test]
    fn failure_windows_survive_aggregation_before_serialization() {
        // Node A: live failure; the windows are stamped at the collection
        // point (get_latest_replication_stats calls refresh_windows before
        // the stats cross the wire), never on the failure hot path.
        let mut node_a = crate::admin::storage_api::replication::BucketReplicationStat::default();
        node_a.fail_stats.add_size(512, None::<&std::io::Error>);
        node_a.fail_stats.refresh_windows();
        node_a.failed = node_a.fail_stats.to_metric();

        // Node A's stats cross the peer RPC wire: the samples are dropped,
        // the window snapshots travel.
        let encoded = rmp_serde::to_vec_named(&node_a).expect("stat should encode");
        let remote: crate::admin::storage_api::replication::BucketReplicationStat =
            rmp_serde::from_slice(&encoded).expect("stat should decode");

        // Aggregation merges the remote stat with an empty local one.
        let merged_fail = remote.fail_stats.merge(&Default::default());
        let aggregated = crate::admin::storage_api::replication::BucketReplicationStat {
            failed: merged_fail.to_metric(),
            fail_stats: merged_fail,
            ..Default::default()
        };

        let mut stats = BucketStats::default();
        stats
            .replication_stats
            .stats
            .insert("arn:minio:replication::t:b".to_string(), aggregated);

        let json = serde_json::to_value(MetricsWire::from(&stats.replication_stats)).expect("wire should serialize");
        let failed = &json["Stats"]["arn:minio:replication::t:b"]["failed"];
        assert_eq!(failed["totals"]["count"], 1.0);
        assert_eq!(
            failed["lastMinute"]["count"], 1.0,
            "the rolling minute window must survive RPC + aggregation"
        );
        assert_eq!(failed["lastMinute"]["bytes"], 512);
        assert_eq!(failed["lastHour"]["count"], 1.0);
    }

    /// Pin the intra-cluster peer-RPC wire format of the internal stats: it
    /// is msgpack with the Rust field names as map keys
    /// (`rmp_serde::to_vec_named` in node_service.rs). If someone "fixes"
    /// the interop bug by renaming the internal serde fields instead of using
    /// these DTOs, this test fails and points them here.
    #[test]
    fn internal_bucket_stats_rpc_wire_stays_snake_case() {
        let stats = sample_bucket_stats();
        let encoded = rmp_serde::to_vec_named(&stats).expect("internal stats should encode");
        let value: serde_json::Value = rmp_serde::from_slice(&encoded).expect("named msgpack should decode generically");

        assert!(
            value.get("replication_stats").is_some(),
            "peer RPC key replication_stats must not be renamed"
        );
        assert!(value["replication_stats"].get("q_stat").is_some());
        assert!(value.get("queue_stats").is_some());
        assert!(value.get("proxy_stats").is_some());

        let decoded: BucketStats = rmp_serde::from_slice(&encoded).expect("round-trip through the peer RPC wire");
        assert_eq!(decoded.replication_stats.replica_count, 2);
    }
}
