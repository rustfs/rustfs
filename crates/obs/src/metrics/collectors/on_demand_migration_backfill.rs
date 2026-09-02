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

//! On-demand migration backfill metrics collector (ODM-12).
//!
//! Turns one checkpoint summary per bucket into the `backfill_*` series
//! declared in `schema::on_demand_migration_backfill`. The stats are plain
//! values so the caller decides where they come from (the runner's
//! checkpoints); nothing here reads storage.

use crate::metrics::report::PrometheusMetric;
use crate::metrics::schema::on_demand_migration_backfill::*;

/// Counters of one bucket's latest backfill job.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OdmBackfillBucketStats {
    pub bucket: String,
    /// Checkpoint state label (`running`, `completed`, ...).
    pub state: String,
    pub listed: u64,
    pub enqueued: u64,
    pub pulled: u64,
    pub skipped_existing: u64,
    pub failed: u64,
    pub bytes: u64,
}

/// Backfill stats of every bucket with a checkpoint, labelled by node.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OdmBackfillRuntimeStats {
    pub server: String,
    pub buckets: Vec<OdmBackfillBucketStats>,
}

/// Seven series per bucket: the state gauge and six counters.
pub fn collect_on_demand_migration_backfill_metrics(stats: &OdmBackfillRuntimeStats) -> Vec<PrometheusMetric> {
    let mut metrics = Vec::with_capacity(stats.buckets.len() * 7);
    for bucket in &stats.buckets {
        let labelled = |descriptor: &'static std::sync::LazyLock<crate::MetricDescriptor>, value: u64| {
            PrometheusMetric::from_descriptor(descriptor, value as f64)
                .with_label_owned(SERVER_LABEL, stats.server.clone())
                .with_label_owned(BUCKET_LABEL, bucket.bucket.clone())
        };
        metrics.push(labelled(&ODM_BACKFILL_JOBS_MD, 1).with_label_owned(STATE_LABEL, bucket.state.clone()));
        metrics.push(labelled(&ODM_BACKFILL_LISTED_MD, bucket.listed));
        metrics.push(labelled(&ODM_BACKFILL_ENQUEUED_MD, bucket.enqueued));
        metrics.push(labelled(&ODM_BACKFILL_PULLED_MD, bucket.pulled));
        metrics.push(labelled(&ODM_BACKFILL_SKIPPED_EXISTING_MD, bucket.skipped_existing));
        metrics.push(labelled(&ODM_BACKFILL_FAILED_MD, bucket.failed));
        metrics.push(labelled(&ODM_BACKFILL_BYTES_MD, bucket.bytes));
    }
    metrics
}

#[cfg(test)]
mod tests {
    use super::*;

    fn labelled<'a>(metrics: &'a [PrometheusMetric], name: &str, bucket: &str) -> Option<&'a PrometheusMetric> {
        metrics.iter().find(|metric| {
            metric.name == name
                && metric
                    .labels
                    .iter()
                    .any(|(label, value)| *label == BUCKET_LABEL && value.as_ref() == bucket)
        })
    }

    #[test]
    fn collects_seven_series_per_bucket_with_the_odm_subsystem_prefix() {
        let stats = OdmBackfillRuntimeStats {
            server: "node1:9000".to_string(),
            buckets: vec![
                OdmBackfillBucketStats {
                    bucket: "photos".to_string(),
                    state: "running".to_string(),
                    listed: 2000,
                    enqueued: 1500,
                    pulled: 1400,
                    skipped_existing: 500,
                    failed: 3,
                    bytes: 73_400_320,
                },
                OdmBackfillBucketStats {
                    bucket: "docs".to_string(),
                    state: "completed".to_string(),
                    ..Default::default()
                },
            ],
        };
        let metrics = collect_on_demand_migration_backfill_metrics(&stats);
        assert_eq!(metrics.len(), 14);
        assert!(
            metrics
                .iter()
                .all(|metric| metric.name.starts_with("rustfs_on_demand_migration_backfill_"))
        );
        assert!(metrics.iter().all(|metric| {
            metric
                .labels
                .iter()
                .any(|(label, value)| *label == SERVER_LABEL && value.as_ref() == "node1:9000")
        }));

        let jobs = labelled(&metrics, &ODM_BACKFILL_JOBS_MD.get_full_metric_name(), "photos").expect("jobs gauge");
        assert_eq!(jobs.value, 1.0);
        assert!(
            jobs.labels
                .iter()
                .any(|(label, value)| *label == STATE_LABEL && value.as_ref() == "running")
        );
        let listed = labelled(&metrics, &ODM_BACKFILL_LISTED_MD.get_full_metric_name(), "photos").expect("listed");
        assert_eq!(listed.value, 2000.0);
        let bytes = labelled(&metrics, &ODM_BACKFILL_BYTES_MD.get_full_metric_name(), "photos").expect("bytes");
        assert_eq!(bytes.value, 73_400_320.0);
        let docs_failed = labelled(&metrics, &ODM_BACKFILL_FAILED_MD.get_full_metric_name(), "docs").expect("docs failed");
        assert_eq!(docs_failed.value, 0.0);
        assert_eq!(
            ODM_BACKFILL_LISTED_MD.get_full_metric_name(),
            "rustfs_on_demand_migration_backfill_listed_total"
        );
    }

    #[test]
    fn no_buckets_means_no_series() {
        let metrics = collect_on_demand_migration_backfill_metrics(&OdmBackfillRuntimeStats::default());
        assert!(metrics.is_empty());
    }
}
