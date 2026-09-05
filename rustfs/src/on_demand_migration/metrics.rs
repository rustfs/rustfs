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

//! Projection of application runtime state onto observability-owned DTOs.

use crate::on_demand_migration::backfill::{
    BackfillCheckpoint as SourceBackfillCheckpoint, global_backfill_runner as source_global_backfill_runner,
};
use crate::on_demand_migration::{
    BreakerState as SourceOdmBreakerState, OdmBucketSnapshot as SourceOdmBucketSnapshot,
    OnDemandMigrationSys as SourceOnDemandMigrationSys,
};
use rustfs_obs::metrics::{
    OdmBackfillBucketStats, OnDemandMigrationBreakerState, OnDemandMigrationBucketStats,
    register_on_demand_migration_metrics_source,
};

pub(super) fn register() {
    register_on_demand_migration_metrics_source(snapshot, backfill_snapshot);
}

fn on_demand_migration_stats_from_snapshot(snapshot: SourceOdmBucketSnapshot) -> OnDemandMigrationBucketStats {
    let stats = snapshot.stats;
    OnDemandMigrationBucketStats {
        bucket: snapshot.bucket,
        requests_total: stats.requests_total,
        pulled_bytes_total: stats.pulled_bytes_total,
        pulled_objects_total: stats.pulled_objects_total,
        pull_failures_total: stats.pull_failures_total,
        inflight_pulls: stats.inflight_pulls,
        queue_depth: stats.queue_depth,
        source_latency_buckets: stats
            .source_latency
            .buckets
            .into_iter()
            .map(|bucket| (bucket.le_ms, bucket.count))
            .collect(),
        source_latency_count: stats.source_latency.count,
        source_latency_sum_ms: stats.source_latency.sum_ms,
        breaker_state: match stats.breaker_state {
            SourceOdmBreakerState::Closed => OnDemandMigrationBreakerState::Closed,
            SourceOdmBreakerState::HalfOpen => OnDemandMigrationBreakerState::HalfOpen,
            SourceOdmBreakerState::Open => OnDemandMigrationBreakerState::Open,
        },
    }
}

/// Every bucket with live on-demand migration state on this node, sorted by
/// name. Empty while the module switch is off.
fn snapshot() -> Vec<OnDemandMigrationBucketStats> {
    SourceOnDemandMigrationSys::get()
        .snapshot()
        .into_iter()
        .map(on_demand_migration_stats_from_snapshot)
        .collect()
}

fn on_demand_migration_backfill_stats_from_checkpoint(
    bucket: String,
    checkpoint: SourceBackfillCheckpoint,
) -> OdmBackfillBucketStats {
    OdmBackfillBucketStats {
        bucket,
        state: checkpoint.state.as_str().to_string(),
        listed: checkpoint.listed,
        enqueued: checkpoint.enqueued,
        pulled: checkpoint.pulled,
        skipped_existing: checkpoint.skipped_existing,
        failed: checkpoint.failed,
        bytes: checkpoint.bytes,
    }
}

/// Backfill jobs running on this node, sorted by bucket. Empty until the
/// runner is installed, and empty again once a job finishes: the series are
/// per-node job progress, not a cluster-wide history.
fn backfill_snapshot() -> Vec<OdmBackfillBucketStats> {
    source_global_backfill_runner()
        .map(|runner| {
            runner
                .local_job_snapshots()
                .into_iter()
                .map(|(bucket, checkpoint)| on_demand_migration_backfill_stats_from_checkpoint(bucket, checkpoint))
                .collect()
        })
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn on_demand_migration_snapshot_projects_counters_and_breaker_state() {
        // Pin the runtime wire snapshot and its observability projection together.
        let snapshot: SourceOdmBucketSnapshot = serde_json::from_value(serde_json::json!({
            "bucket": "photos",
            "provider": "minio",
            "endpoint_host": "source.example.com",
            "applied_at": "2026-09-02T10:00:00Z",
            "client_error": null,
            "negative_cache_entries": 0,
            "inflight_keys": 1,
            "max_concurrent_pulls": 8,
            "stats": {
                "requests_total": {"get": {"source_hit": 2}},
                "pulled_bytes_total": 4096,
                "pulled_objects_total": {"inline": 1},
                "pull_failures_total": {"source_timeout": 1},
                "inflight_pulls": 1,
                "queue_depth": 2,
                "source_latency": {
                    "buckets": [{"le_ms": 5, "count": 1}, {"le_ms": 10, "count": 2}],
                    "count": 3,
                    "sum_ms": 90753
                },
                "last_source_error": {"class": "server_error", "at": "2026-09-02T10:00:00Z"},
                "breaker_state": "open"
            }
        }))
        .expect("runtime snapshot decodes");

        let stats = on_demand_migration_stats_from_snapshot(snapshot);

        assert_eq!(stats.bucket, "photos");
        assert_eq!(stats.requests_total["get"]["source_hit"], 2);
        assert_eq!(stats.pulled_bytes_total, 4096);
        assert_eq!(stats.pulled_objects_total["inline"], 1);
        assert_eq!(stats.pull_failures_total["source_timeout"], 1);
        assert_eq!(stats.inflight_pulls, 1);
        assert_eq!(stats.queue_depth, 2);
        assert_eq!(stats.source_latency_buckets, vec![(5, 1), (10, 2)]);
        assert_eq!(stats.source_latency_count, 3);
        assert_eq!(stats.source_latency_sum_ms, 90_753);
        assert_eq!(stats.breaker_state, OnDemandMigrationBreakerState::Open);
    }
}
