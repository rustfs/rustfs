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

//! Scanner metrics collector.
//!
//! Collects background scanner metrics including bucket-drive scans,
//! directory scans, and object scans.

use crate::metrics::report::PrometheusMetric;
use crate::metrics::schema::scanner::*;

/// Scanner statistics.
#[derive(Debug, Clone, Default)]
pub struct ScannerStats {
    /// Number of bucket-drive scans finished
    pub bucket_scans_finished: u64,
    /// Number of bucket-drive scans started
    pub bucket_scans_started: u64,
    /// Number of bucket-drive scans that failed
    pub bucket_scans_failed: u64,
    /// Number of directories scanned
    pub directories_scanned: u64,
    /// Number of objects scanned
    pub objects_scanned: u64,
    /// Number of object versions scanned
    pub versions_scanned: u64,
    /// Seconds since last scanner activity
    pub last_activity_seconds: u64,
    /// Number of scanner paths currently being processed
    pub active_paths: u64,
    /// Age in seconds of the oldest active scanner path update
    pub oldest_active_path_age_seconds: u64,
    /// Current aggregate set scan concurrency limit
    pub current_set_scan_concurrency_limit: u64,
    /// Current number of queued set scans
    pub current_set_scans_queued: u64,
    /// Current number of active set scans
    pub current_set_scans_active: u64,
    /// Current aggregate disk-bucket scan concurrency limit
    pub current_disk_scan_concurrency_limit: u64,
    /// Current number of queued disk-bucket scans
    pub current_disk_bucket_scans_queued: u64,
    /// Current number of active disk-bucket scans
    pub current_disk_bucket_scans_active: u64,
    /// Whether scanner idle-mode self-throttling is enabled
    pub throttle_idle_mode_enabled: bool,
    /// Effective scanner sleep factor
    pub throttle_sleep_factor: f64,
    /// Effective scanner maximum self-throttle sleep duration in seconds
    pub throttle_max_sleep_seconds: f64,
    /// Object interval for cooperative scanner runtime yields
    pub yield_every_n_objects: u64,
    /// Effective scanner cycle interval in seconds
    pub cycle_interval_seconds: f64,
    /// Effective maximum scanner cycle runtime in seconds
    pub cycle_max_duration_seconds: f64,
    /// Effective maximum objects processed by one scanner cycle
    pub cycle_max_objects: u64,
    /// Effective maximum directories entered by one scanner cycle
    pub cycle_max_directories: u64,
    /// Whether periodic scanner bitrot deep scans are enabled
    pub bitrot_cycle_enabled: bool,
    /// Effective scanner bitrot deep-scan interval in seconds
    pub bitrot_cycle_seconds: f64,
    /// Current scanner cycle number, or zero when idle
    pub current_cycle: u64,
    /// Number of scanner cycles completed since server start
    pub completed_cycles: u64,
    /// Seconds elapsed since the current scanner cycle started
    pub current_cycle_age_seconds: u64,
    /// Number of objects scanned by the currently running scanner cycle
    pub current_cycle_objects_scanned: u64,
    /// Number of directories scanned by the currently running scanner cycle
    pub current_cycle_directories_scanned: u64,
    /// Number of bucket-drive scans finished by the currently running scanner cycle
    pub current_cycle_bucket_drive_scans: u64,
    /// Number of bucket-drive scans that failed in the currently running scanner cycle
    pub current_cycle_bucket_drive_failures: u64,
    /// Object scan rate for the currently running scanner cycle
    pub current_cycle_objects_per_second: f64,
    /// Directory scan rate for the currently running scanner cycle
    pub current_cycle_directories_per_second: f64,
    /// Bucket-drive scan rate for the currently running scanner cycle
    pub current_cycle_bucket_drive_scans_per_second: f64,
    /// Number of scanner cooperative yield events in the current scanner cycle
    pub current_cycle_yield_events: u64,
    /// Total scanner cooperative yield duration in seconds for the current scanner cycle
    pub current_cycle_yield_duration_seconds: f64,
    /// Number of scanner self-throttle sleep events in the current scanner cycle
    pub current_cycle_throttle_sleep_events: u64,
    /// Total scanner self-throttle sleep duration in seconds for the current scanner cycle
    pub current_cycle_throttle_sleep_duration_seconds: f64,
    /// Number of lifecycle actions applied by the current scanner cycle
    pub current_cycle_ilm_actions: u64,
    /// Number of object heal candidates enqueued by the current scanner cycle
    pub current_cycle_heal_objects: u64,
    /// Number of replication heal checks run by the current scanner cycle
    pub current_cycle_replication_checks: u64,
    /// Number of data-usage save operations run by the current scanner cycle
    pub current_cycle_usage_saves: u64,
    /// Current scanner mode: 0 unknown or idle, 1 normal, 2 deep bitrot scan
    pub current_scan_mode: u64,
    /// Last scanner cycle result: 0 unknown, 1 success, 2 error, 3 partial, 4 superseded, 5 deferred
    pub last_cycle_result: u64,
    /// Last scanner partial cycle reason: 0 unknown, 1 runtime, 2 objects, 3 directories
    pub last_cycle_partial_reason: u64,
    /// Duration in seconds of the last finished scanner cycle
    pub last_cycle_duration_seconds: f64,
    /// Number of objects scanned by the last finished scanner cycle
    pub last_cycle_objects_scanned: u64,
    /// Number of directories scanned by the last finished scanner cycle
    pub last_cycle_directories_scanned: u64,
    /// Number of bucket-drive scans finished by the last scanner cycle
    pub last_cycle_bucket_drive_scans: u64,
    /// Number of bucket-drive scans that failed in the last finished scanner cycle
    pub last_cycle_bucket_drive_failures: u64,
    /// Object scan rate for the last finished scanner cycle
    pub last_cycle_objects_per_second: f64,
    /// Directory scan rate for the last finished scanner cycle
    pub last_cycle_directories_per_second: f64,
    /// Bucket-drive scan rate for the last finished scanner cycle
    pub last_cycle_bucket_drive_scans_per_second: f64,
    /// Number of scanner cooperative yield events in the last finished scanner cycle
    pub last_cycle_yield_events: u64,
    /// Total scanner cooperative yield duration in seconds for the last finished scanner cycle
    pub last_cycle_yield_duration_seconds: f64,
    /// Number of scanner self-throttle sleep events in the last finished scanner cycle
    pub last_cycle_throttle_sleep_events: u64,
    /// Total scanner self-throttle sleep duration in seconds for the last finished scanner cycle
    pub last_cycle_throttle_sleep_duration_seconds: f64,
    /// Number of lifecycle actions applied by the last finished scanner cycle
    pub last_cycle_ilm_actions: u64,
    /// Number of object heal candidates enqueued by the last finished scanner cycle
    pub last_cycle_heal_objects: u64,
    /// Number of replication heal checks run by the last finished scanner cycle
    pub last_cycle_replication_checks: u64,
    /// Number of data-usage save operations run by the last finished scanner cycle
    pub last_cycle_usage_saves: u64,
    /// Number of scanner cycles that failed since server start
    pub failed_cycles: u64,
    /// Number of scanner cycles superseded by concurrent namespace activity
    pub superseded_cycles: u64,
    /// Number of scanner cycles stopped by runtime budget since server start
    pub partial_cycles: u64,
    /// Number of scanner cycles stopped by an unknown budget reason
    pub partial_cycles_unknown: u64,
    /// Number of scanner cycles stopped by runtime budget
    pub partial_cycles_runtime: u64,
    /// Number of scanner cycles stopped by object budget
    pub partial_cycles_objects: u64,
    /// Number of scanner cycles stopped by directory budget
    pub partial_cycles_directories: u64,
}

/// Scanner source-work metrics for a source.
#[derive(Debug, Clone, Default)]
pub struct ScannerSourceWorkStats {
    pub source: String,
    pub checked: u64,
    pub queued: u64,
    pub executed: u64,
    pub failed: u64,
    pub skipped: u64,
    pub missed: u64,
}

/// Scanner bucket-drive result metrics for a structured bucket/drive pair.
#[derive(Debug, Clone, Default)]
pub struct ScannerBucketDriveResultStats {
    pub bucket: String,
    pub drive: String,
    pub result: String,
    pub count: u64,
}

#[derive(Debug, Clone, Default)]
pub struct ScannerActiveBucketDriveStats {
    pub source: String,
    pub bucket: String,
    pub drive: String,
    pub count: u64,
    pub age_seconds: u64,
}

/// Scanner statistics with runtime-local node identity and bounded source/result details.
#[derive(Debug, Clone, Default)]
pub(crate) struct ScannerRuntimeStats {
    pub(crate) server: String,
    pub(crate) stats: ScannerStats,
    pub(crate) source_work: Vec<ScannerSourceWorkStats>,
    pub(crate) current_cycle_source_work: Vec<ScannerSourceWorkStats>,
    pub(crate) last_cycle_source_work: Vec<ScannerSourceWorkStats>,
    pub(crate) bucket_drive_results: Vec<ScannerBucketDriveResultStats>,
    pub(crate) current_cycle_bucket_drive_results: Vec<ScannerBucketDriveResultStats>,
    pub(crate) last_cycle_bucket_drive_results: Vec<ScannerBucketDriveResultStats>,
    pub(crate) active_bucket_drive_scans: Vec<ScannerActiveBucketDriveStats>,
}

/// Collects scanner metrics from the given stats.
///
/// Uses the metric descriptors from `metrics_type::scanner` module.
/// Returns a vector of Prometheus metrics for scanner statistics.
pub fn collect_scanner_metrics(stats: &ScannerStats) -> Vec<PrometheusMetric> {
    collect_scanner_metrics_with_runtime(stats, None)
}

pub(crate) fn collect_scanner_runtime_metrics(stats: &ScannerRuntimeStats) -> Vec<PrometheusMetric> {
    collect_scanner_metrics_with_runtime(&stats.stats, Some(stats))
}

fn collect_scanner_metrics_with_runtime(stats: &ScannerStats, runtime: Option<&ScannerRuntimeStats>) -> Vec<PrometheusMetric> {
    fn push_source_work_metric(
        metrics: &mut Vec<PrometheusMetric>,
        descriptor: &'static crate::metrics::schema::MetricDescriptor,
        server: &str,
        source: &str,
        state: &str,
        value: u64,
        cycle_scope: Option<&str>,
    ) {
        let mut metric =
            PrometheusMetric::from_descriptor(descriptor, value as f64).with_label_owned(SERVER_LABEL, server.to_string());
        if let Some(cycle_scope) = cycle_scope {
            metric = metric.with_label_owned(CYCLE_SCOPE_LABEL, cycle_scope.to_string());
        }
        metric = metric
            .with_label_owned(SOURCE_LABEL, source.to_string())
            .with_label_owned(STATE_LABEL, state.to_string());
        metrics.push(metric);
    }

    fn push_source_work_metrics(
        metrics: &mut Vec<PrometheusMetric>,
        descriptor: &'static crate::metrics::schema::MetricDescriptor,
        server: &str,
        source_work: &[ScannerSourceWorkStats],
        cycle_scope: Option<&str>,
    ) {
        for work in source_work {
            push_source_work_metric(metrics, descriptor, server, &work.source, "checked", work.checked, cycle_scope);
            push_source_work_metric(metrics, descriptor, server, &work.source, "queued", work.queued, cycle_scope);
            push_source_work_metric(metrics, descriptor, server, &work.source, "executed", work.executed, cycle_scope);
            push_source_work_metric(metrics, descriptor, server, &work.source, "failed", work.failed, cycle_scope);
            push_source_work_metric(metrics, descriptor, server, &work.source, "skipped", work.skipped, cycle_scope);
            push_source_work_metric(metrics, descriptor, server, &work.source, "missed", work.missed, cycle_scope);
        }
    }

    fn push_bucket_drive_result_metric(
        metrics: &mut Vec<PrometheusMetric>,
        descriptor: &'static crate::metrics::schema::MetricDescriptor,
        server: &str,
        result: &ScannerBucketDriveResultStats,
        cycle_scope: Option<&str>,
    ) {
        let mut metric =
            PrometheusMetric::from_descriptor(descriptor, result.count as f64).with_label_owned(SERVER_LABEL, server.to_string());
        if let Some(cycle_scope) = cycle_scope {
            metric = metric.with_label_owned(CYCLE_SCOPE_LABEL, cycle_scope.to_string());
        }
        metrics.push(
            metric
                .with_label_owned(BUCKET_LABEL, result.bucket.clone())
                .with_label_owned(DRIVE_LABEL, result.drive.clone())
                .with_label_owned(RESULT_LABEL, result.result.clone()),
        );
    }

    fn push_bucket_drive_result_metrics(
        metrics: &mut Vec<PrometheusMetric>,
        descriptor: &'static crate::metrics::schema::MetricDescriptor,
        server: &str,
        results: &[ScannerBucketDriveResultStats],
        cycle_scope: Option<&str>,
    ) {
        for result in results {
            push_bucket_drive_result_metric(metrics, descriptor, server, result, cycle_scope);
        }
    }

    let mut metrics = vec![
        PrometheusMetric::from_descriptor(&SCANNER_BUCKET_SCANS_FINISHED_MD, stats.bucket_scans_finished as f64),
        PrometheusMetric::from_descriptor(&SCANNER_BUCKET_SCANS_STARTED_MD, stats.bucket_scans_started as f64),
        PrometheusMetric::from_descriptor(&SCANNER_BUCKET_SCANS_FAILED_MD, stats.bucket_scans_failed as f64),
        PrometheusMetric::from_descriptor(&SCANNER_DIRECTORIES_SCANNED_MD, stats.directories_scanned as f64),
        PrometheusMetric::from_descriptor(&SCANNER_OBJECTS_SCANNED_MD, stats.objects_scanned as f64),
        PrometheusMetric::from_descriptor(&SCANNER_VERSIONS_SCANNED_MD, stats.versions_scanned as f64),
        PrometheusMetric::from_descriptor(&SCANNER_LAST_ACTIVITY_SECONDS_MD, stats.last_activity_seconds as f64),
        PrometheusMetric::from_descriptor(&SCANNER_ACTIVE_PATHS_MD, stats.active_paths as f64),
        PrometheusMetric::from_descriptor(
            &SCANNER_OLDEST_ACTIVE_PATH_AGE_SECONDS_MD,
            stats.oldest_active_path_age_seconds as f64,
        ),
        PrometheusMetric::from_descriptor(
            &SCANNER_CURRENT_SET_SCAN_CONCURRENCY_LIMIT_MD,
            stats.current_set_scan_concurrency_limit as f64,
        ),
        PrometheusMetric::from_descriptor(&SCANNER_CURRENT_SET_SCANS_QUEUED_MD, stats.current_set_scans_queued as f64),
        PrometheusMetric::from_descriptor(&SCANNER_CURRENT_SET_SCANS_ACTIVE_MD, stats.current_set_scans_active as f64),
        PrometheusMetric::from_descriptor(
            &SCANNER_CURRENT_DISK_SCAN_CONCURRENCY_LIMIT_MD,
            stats.current_disk_scan_concurrency_limit as f64,
        ),
        PrometheusMetric::from_descriptor(
            &SCANNER_CURRENT_DISK_BUCKET_SCANS_QUEUED_MD,
            stats.current_disk_bucket_scans_queued as f64,
        ),
        PrometheusMetric::from_descriptor(
            &SCANNER_CURRENT_DISK_BUCKET_SCANS_ACTIVE_MD,
            stats.current_disk_bucket_scans_active as f64,
        ),
        PrometheusMetric::from_descriptor(
            &SCANNER_THROTTLE_IDLE_MODE_ENABLED_MD,
            bool_metric_value(stats.throttle_idle_mode_enabled),
        ),
        PrometheusMetric::from_descriptor(&SCANNER_THROTTLE_SLEEP_FACTOR_MD, stats.throttle_sleep_factor),
        PrometheusMetric::from_descriptor(&SCANNER_THROTTLE_MAX_SLEEP_SECONDS_MD, stats.throttle_max_sleep_seconds),
        PrometheusMetric::from_descriptor(&SCANNER_YIELD_EVERY_N_OBJECTS_MD, stats.yield_every_n_objects as f64),
        PrometheusMetric::from_descriptor(&SCANNER_CYCLE_INTERVAL_SECONDS_MD, stats.cycle_interval_seconds),
        PrometheusMetric::from_descriptor(&SCANNER_CYCLE_MAX_DURATION_SECONDS_MD, stats.cycle_max_duration_seconds),
        PrometheusMetric::from_descriptor(&SCANNER_CYCLE_MAX_OBJECTS_MD, stats.cycle_max_objects as f64),
        PrometheusMetric::from_descriptor(&SCANNER_CYCLE_MAX_DIRECTORIES_MD, stats.cycle_max_directories as f64),
        PrometheusMetric::from_descriptor(&SCANNER_BITROT_CYCLE_ENABLED_MD, bool_metric_value(stats.bitrot_cycle_enabled)),
        PrometheusMetric::from_descriptor(&SCANNER_BITROT_CYCLE_SECONDS_MD, stats.bitrot_cycle_seconds),
        PrometheusMetric::from_descriptor(&SCANNER_CURRENT_CYCLE_MD, stats.current_cycle as f64),
        PrometheusMetric::from_descriptor(&SCANNER_COMPLETED_CYCLES_MD, stats.completed_cycles as f64),
        PrometheusMetric::from_descriptor(&SCANNER_CURRENT_CYCLE_AGE_SECONDS_MD, stats.current_cycle_age_seconds as f64),
        PrometheusMetric::from_descriptor(&SCANNER_CURRENT_CYCLE_OBJECTS_SCANNED_MD, stats.current_cycle_objects_scanned as f64),
        PrometheusMetric::from_descriptor(
            &SCANNER_CURRENT_CYCLE_DIRECTORIES_SCANNED_MD,
            stats.current_cycle_directories_scanned as f64,
        ),
        PrometheusMetric::from_descriptor(
            &SCANNER_CURRENT_CYCLE_BUCKET_DRIVE_SCANS_MD,
            stats.current_cycle_bucket_drive_scans as f64,
        ),
        PrometheusMetric::from_descriptor(
            &SCANNER_CURRENT_CYCLE_BUCKET_DRIVE_FAILURES_MD,
            stats.current_cycle_bucket_drive_failures as f64,
        ),
        PrometheusMetric::from_descriptor(&SCANNER_CURRENT_CYCLE_OBJECTS_PER_SECOND_MD, stats.current_cycle_objects_per_second),
        PrometheusMetric::from_descriptor(
            &SCANNER_CURRENT_CYCLE_DIRECTORIES_PER_SECOND_MD,
            stats.current_cycle_directories_per_second,
        ),
        PrometheusMetric::from_descriptor(
            &SCANNER_CURRENT_CYCLE_BUCKET_DRIVE_SCANS_PER_SECOND_MD,
            stats.current_cycle_bucket_drive_scans_per_second,
        ),
        PrometheusMetric::from_descriptor(&SCANNER_CURRENT_CYCLE_YIELD_EVENTS_MD, stats.current_cycle_yield_events as f64),
        PrometheusMetric::from_descriptor(
            &SCANNER_CURRENT_CYCLE_YIELD_DURATION_SECONDS_MD,
            stats.current_cycle_yield_duration_seconds,
        ),
        PrometheusMetric::from_descriptor(
            &SCANNER_CURRENT_CYCLE_THROTTLE_SLEEP_EVENTS_MD,
            stats.current_cycle_throttle_sleep_events as f64,
        ),
        PrometheusMetric::from_descriptor(
            &SCANNER_CURRENT_CYCLE_THROTTLE_SLEEP_DURATION_SECONDS_MD,
            stats.current_cycle_throttle_sleep_duration_seconds,
        ),
        PrometheusMetric::from_descriptor(&SCANNER_CURRENT_CYCLE_ILM_ACTIONS_MD, stats.current_cycle_ilm_actions as f64),
        PrometheusMetric::from_descriptor(&SCANNER_CURRENT_CYCLE_HEAL_OBJECTS_MD, stats.current_cycle_heal_objects as f64),
        PrometheusMetric::from_descriptor(
            &SCANNER_CURRENT_CYCLE_REPLICATION_CHECKS_MD,
            stats.current_cycle_replication_checks as f64,
        ),
        PrometheusMetric::from_descriptor(&SCANNER_CURRENT_CYCLE_USAGE_SAVES_MD, stats.current_cycle_usage_saves as f64),
        PrometheusMetric::from_descriptor(&SCANNER_CURRENT_SCAN_MODE_MD, stats.current_scan_mode as f64),
        PrometheusMetric::from_descriptor(&SCANNER_LAST_CYCLE_RESULT_MD, stats.last_cycle_result as f64),
        PrometheusMetric::from_descriptor(&SCANNER_LAST_CYCLE_PARTIAL_REASON_MD, stats.last_cycle_partial_reason as f64),
        PrometheusMetric::from_descriptor(&SCANNER_LAST_CYCLE_DURATION_SECONDS_MD, stats.last_cycle_duration_seconds),
        PrometheusMetric::from_descriptor(&SCANNER_LAST_CYCLE_OBJECTS_SCANNED_MD, stats.last_cycle_objects_scanned as f64),
        PrometheusMetric::from_descriptor(
            &SCANNER_LAST_CYCLE_DIRECTORIES_SCANNED_MD,
            stats.last_cycle_directories_scanned as f64,
        ),
        PrometheusMetric::from_descriptor(&SCANNER_LAST_CYCLE_BUCKET_DRIVE_SCANS_MD, stats.last_cycle_bucket_drive_scans as f64),
        PrometheusMetric::from_descriptor(
            &SCANNER_LAST_CYCLE_BUCKET_DRIVE_FAILURES_MD,
            stats.last_cycle_bucket_drive_failures as f64,
        ),
        PrometheusMetric::from_descriptor(&SCANNER_LAST_CYCLE_OBJECTS_PER_SECOND_MD, stats.last_cycle_objects_per_second),
        PrometheusMetric::from_descriptor(&SCANNER_LAST_CYCLE_DIRECTORIES_PER_SECOND_MD, stats.last_cycle_directories_per_second),
        PrometheusMetric::from_descriptor(
            &SCANNER_LAST_CYCLE_BUCKET_DRIVE_SCANS_PER_SECOND_MD,
            stats.last_cycle_bucket_drive_scans_per_second,
        ),
        PrometheusMetric::from_descriptor(&SCANNER_LAST_CYCLE_YIELD_EVENTS_MD, stats.last_cycle_yield_events as f64),
        PrometheusMetric::from_descriptor(&SCANNER_LAST_CYCLE_YIELD_DURATION_SECONDS_MD, stats.last_cycle_yield_duration_seconds),
        PrometheusMetric::from_descriptor(
            &SCANNER_LAST_CYCLE_THROTTLE_SLEEP_EVENTS_MD,
            stats.last_cycle_throttle_sleep_events as f64,
        ),
        PrometheusMetric::from_descriptor(
            &SCANNER_LAST_CYCLE_THROTTLE_SLEEP_DURATION_SECONDS_MD,
            stats.last_cycle_throttle_sleep_duration_seconds,
        ),
        PrometheusMetric::from_descriptor(&SCANNER_LAST_CYCLE_ILM_ACTIONS_MD, stats.last_cycle_ilm_actions as f64),
        PrometheusMetric::from_descriptor(&SCANNER_LAST_CYCLE_HEAL_OBJECTS_MD, stats.last_cycle_heal_objects as f64),
        PrometheusMetric::from_descriptor(&SCANNER_LAST_CYCLE_REPLICATION_CHECKS_MD, stats.last_cycle_replication_checks as f64),
        PrometheusMetric::from_descriptor(&SCANNER_LAST_CYCLE_USAGE_SAVES_MD, stats.last_cycle_usage_saves as f64),
        PrometheusMetric::from_descriptor(&SCANNER_FAILED_CYCLES_MD, stats.failed_cycles as f64),
        PrometheusMetric::from_descriptor(&SCANNER_SUPERSEDED_CYCLES_MD, stats.superseded_cycles as f64),
        PrometheusMetric::from_descriptor(&SCANNER_PARTIAL_CYCLES_MD, stats.partial_cycles as f64),
        PrometheusMetric::from_descriptor(&SCANNER_PARTIAL_CYCLES_BY_REASON_MD, stats.partial_cycles_unknown as f64)
            .with_label("reason", "unknown"),
        PrometheusMetric::from_descriptor(&SCANNER_PARTIAL_CYCLES_BY_REASON_MD, stats.partial_cycles_runtime as f64)
            .with_label("reason", "runtime"),
        PrometheusMetric::from_descriptor(&SCANNER_PARTIAL_CYCLES_BY_REASON_MD, stats.partial_cycles_objects as f64)
            .with_label("reason", "objects"),
        PrometheusMetric::from_descriptor(&SCANNER_PARTIAL_CYCLES_BY_REASON_MD, stats.partial_cycles_directories as f64)
            .with_label("reason", "directories"),
    ];

    if let Some(runtime) = runtime {
        push_source_work_metrics(&mut metrics, &SCANNER_SOURCE_WORK_TOTAL_MD, &runtime.server, &runtime.source_work, None);
        push_source_work_metrics(
            &mut metrics,
            &SCANNER_CYCLE_SOURCE_WORK_MD,
            &runtime.server,
            &runtime.current_cycle_source_work,
            Some("current"),
        );
        push_source_work_metrics(
            &mut metrics,
            &SCANNER_CYCLE_SOURCE_WORK_MD,
            &runtime.server,
            &runtime.last_cycle_source_work,
            Some("last"),
        );
        push_bucket_drive_result_metrics(
            &mut metrics,
            &SCANNER_BUCKET_DRIVE_RESULT_TOTAL_MD,
            &runtime.server,
            &runtime.bucket_drive_results,
            None,
        );
        push_bucket_drive_result_metrics(
            &mut metrics,
            &SCANNER_CYCLE_BUCKET_DRIVE_RESULT_MD,
            &runtime.server,
            &runtime.current_cycle_bucket_drive_results,
            Some("current"),
        );
        push_bucket_drive_result_metrics(
            &mut metrics,
            &SCANNER_CYCLE_BUCKET_DRIVE_RESULT_MD,
            &runtime.server,
            &runtime.last_cycle_bucket_drive_results,
            Some("last"),
        );
        for active in &runtime.active_bucket_drive_scans {
            let labels = |metric: PrometheusMetric| {
                metric
                    .with_label_owned(SERVER_LABEL, runtime.server.clone())
                    .with_label_owned(SOURCE_LABEL, active.source.clone())
                    .with_label_owned(BUCKET_LABEL, active.bucket.clone())
                    .with_label_owned(DRIVE_LABEL, active.drive.clone())
            };
            metrics.push(labels(PrometheusMetric::from_descriptor(
                &SCANNER_ACTIVE_BUCKET_DRIVE_SCANS_MD,
                active.count as f64,
            )));
            metrics.push(labels(PrometheusMetric::from_descriptor(
                &SCANNER_ACTIVE_BUCKET_DRIVE_SCAN_AGE_SECONDS_MD,
                active.age_seconds as f64,
            )));
        }
    }

    metrics
}

fn bool_metric_value(enabled: bool) -> f64 {
    if enabled { 1.0 } else { 0.0 }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::report::report_metrics;
    use metrics_util::debugging::DebuggingRecorder;
    use rustfs_common::metrics::{Metric, Metrics};

    fn prometheus_counter_name(name: &str) -> String {
        if name.ends_with("_total") {
            name.to_string()
        } else {
            format!("{name}_total")
        }
    }

    #[test]
    fn scanner_lifetime_counters_have_one_prometheus_producer() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let scanner_metrics = collect_scanner_metrics(&ScannerStats {
            directories_scanned: 3,
            objects_scanned: 7,
            ..Default::default()
        });

        metrics::with_local_recorder(&recorder, || {
            Metrics::time(Metric::ScanObject)();
            Metrics::time(Metric::ScanFolder)();
            report_metrics(&scanner_metrics);
        });

        let normalized_counter_names: Vec<_> = snapshotter
            .snapshot()
            .into_vec()
            .into_iter()
            .filter_map(|(composite, _, _, value)| {
                matches!(value, metrics_util::debugging::DebugValue::Counter(_))
                    .then(|| prometheus_counter_name(composite.key().name()))
            })
            .collect();

        for name in [
            "rustfs_scanner_objects_scanned_total",
            "rustfs_scanner_directories_scanned_total",
        ] {
            assert_eq!(
                normalized_counter_names
                    .iter()
                    .filter(|candidate| candidate.as_str() == name)
                    .count(),
                1,
                "scanner lifetime counter must have exactly one producer after Prometheus name normalization"
            );
        }
    }

    #[test]
    fn test_collect_scanner_metrics() {
        let stats = ScannerRuntimeStats {
            server: "node1:9000".to_string(),
            source_work: vec![ScannerSourceWorkStats {
                source: "lifecycle".to_string(),
                checked: 11,
                queued: 2,
                executed: 3,
                failed: 4,
                skipped: 5,
                missed: 6,
            }],
            current_cycle_source_work: vec![ScannerSourceWorkStats {
                source: "usage".to_string(),
                checked: 21,
                queued: 7,
                executed: 8,
                failed: 9,
                skipped: 10,
                missed: 11,
            }],
            last_cycle_source_work: vec![ScannerSourceWorkStats {
                source: "heal".to_string(),
                checked: 31,
                queued: 12,
                executed: 13,
                failed: 14,
                skipped: 15,
                missed: 16,
            }],
            bucket_drive_results: vec![ScannerBucketDriveResultStats {
                bucket: "photos".to_string(),
                drive: "/data1".to_string(),
                result: "success".to_string(),
                count: 3,
            }],
            current_cycle_bucket_drive_results: vec![ScannerBucketDriveResultStats {
                bucket: "photos".to_string(),
                drive: "/data1".to_string(),
                result: "partial".to_string(),
                count: 1,
            }],
            last_cycle_bucket_drive_results: vec![ScannerBucketDriveResultStats {
                bucket: "videos".to_string(),
                drive: "/data2".to_string(),
                result: "error".to_string(),
                count: 2,
            }],
            active_bucket_drive_scans: vec![ScannerActiveBucketDriveStats {
                source: "usage".to_string(),
                bucket: "photos".to_string(),
                drive: "/data1".to_string(),
                count: 2,
                age_seconds: 7,
            }],
            stats: ScannerStats {
                bucket_scans_finished: 100,
                bucket_scans_started: 100,
                bucket_scans_failed: 2,
                directories_scanned: 50000,
                objects_scanned: 1000000,
                versions_scanned: 1500000,
                last_activity_seconds: 30,
                active_paths: 4,
                oldest_active_path_age_seconds: 17,
                current_set_scan_concurrency_limit: 3,
                current_set_scans_queued: 5,
                current_set_scans_active: 2,
                current_disk_scan_concurrency_limit: 6,
                current_disk_bucket_scans_queued: 18,
                current_disk_bucket_scans_active: 4,
                throttle_idle_mode_enabled: true,
                throttle_sleep_factor: 10.0,
                throttle_max_sleep_seconds: 15.0,
                yield_every_n_objects: 128,
                cycle_interval_seconds: 3600.0,
                cycle_max_duration_seconds: 1800.0,
                cycle_max_objects: 1_000_000,
                cycle_max_directories: 100_000,
                bitrot_cycle_enabled: true,
                bitrot_cycle_seconds: 86400.0,
                current_cycle: 12,
                completed_cycles: 11,
                current_cycle_age_seconds: 90,
                current_cycle_objects_scanned: 250,
                current_cycle_directories_scanned: 20,
                current_cycle_bucket_drive_scans: 2,
                current_cycle_bucket_drive_failures: 1,
                current_cycle_objects_per_second: 12.5,
                current_cycle_directories_per_second: 1.0,
                current_cycle_bucket_drive_scans_per_second: 0.1,
                current_cycle_yield_events: 8,
                current_cycle_yield_duration_seconds: 1.25,
                current_cycle_throttle_sleep_events: 4,
                current_cycle_throttle_sleep_duration_seconds: 2.5,
                current_cycle_ilm_actions: 6,
                current_cycle_heal_objects: 2,
                current_cycle_replication_checks: 5,
                current_cycle_usage_saves: 3,
                current_scan_mode: 2,
                last_cycle_result: 1,
                last_cycle_partial_reason: 3,
                last_cycle_duration_seconds: 42.5,
                last_cycle_objects_scanned: 900,
                last_cycle_directories_scanned: 80,
                last_cycle_bucket_drive_scans: 6,
                last_cycle_bucket_drive_failures: 2,
                last_cycle_objects_per_second: 18.0,
                last_cycle_directories_per_second: 1.6,
                last_cycle_bucket_drive_scans_per_second: 0.12,
                last_cycle_yield_events: 30,
                last_cycle_yield_duration_seconds: 9.5,
                last_cycle_throttle_sleep_events: 12,
                last_cycle_throttle_sleep_duration_seconds: 6.75,
                last_cycle_ilm_actions: 44,
                last_cycle_heal_objects: 7,
                last_cycle_replication_checks: 12,
                last_cycle_usage_saves: 9,
                failed_cycles: 3,
                superseded_cycles: 5,
                partial_cycles: 10,
                partial_cycles_unknown: 1,
                partial_cycles_runtime: 2,
                partial_cycles_objects: 3,
                partial_cycles_directories: 4,
            },
        };

        let metrics = collect_scanner_runtime_metrics(&stats);
        report_metrics(&metrics);

        assert_eq!(metrics.len(), 92);

        let objects = metrics.iter().find(|m| m.value == 1000000.0);
        assert!(objects.is_some());

        let last_activity = metrics.iter().find(|m| m.value == 30.0);
        assert!(last_activity.is_some());

        let active_paths = metrics
            .iter()
            .find(|m| m.name == SCANNER_ACTIVE_PATHS_MD.get_full_metric_name());
        assert_eq!(active_paths.map(|m| m.value), Some(4.0));
        assert_eq!(active_paths.map(|m| m.labels.len()), Some(0));

        let active_bucket_drive = metrics
            .iter()
            .find(|m| m.name == SCANNER_ACTIVE_BUCKET_DRIVE_SCANS_MD.get_full_metric_name())
            .expect("active bucket-drive metric");
        assert_eq!(active_bucket_drive.value, 2.0);
        assert!(
            active_bucket_drive
                .labels
                .iter()
                .any(|(name, value)| *name == SOURCE_LABEL && value == "usage")
        );
        assert!(
            active_bucket_drive
                .labels
                .iter()
                .any(|(name, value)| *name == BUCKET_LABEL && value == "photos")
        );
        assert!(
            active_bucket_drive
                .labels
                .iter()
                .any(|(name, value)| *name == DRIVE_LABEL && value == "/data1")
        );
        let active_age = metrics
            .iter()
            .find(|m| m.name == SCANNER_ACTIVE_BUCKET_DRIVE_SCAN_AGE_SECONDS_MD.get_full_metric_name())
            .expect("active bucket-drive age metric");
        assert_eq!(active_age.value, 7.0);

        let bucket_drive_result = metrics
            .iter()
            .find(|m| m.name == SCANNER_BUCKET_DRIVE_RESULT_TOTAL_MD.get_full_metric_name());
        assert_eq!(bucket_drive_result.map(|m| m.value), Some(3.0));
        assert_eq!(
            bucket_drive_result
                .and_then(|m| m.labels.iter().find(|(name, _)| *name == BUCKET_LABEL))
                .map(|(_, value)| value.as_ref()),
            Some("photos")
        );

        let oldest_active_path_age = metrics
            .iter()
            .find(|m| m.name == SCANNER_OLDEST_ACTIVE_PATH_AGE_SECONDS_MD.get_full_metric_name());
        assert_eq!(oldest_active_path_age.map(|m| m.value), Some(17.0));

        let current_set_scan_concurrency_limit = metrics
            .iter()
            .find(|m| m.name == SCANNER_CURRENT_SET_SCAN_CONCURRENCY_LIMIT_MD.get_full_metric_name());
        assert_eq!(current_set_scan_concurrency_limit.map(|m| m.value), Some(3.0));

        let current_set_scans_queued = metrics
            .iter()
            .find(|m| m.name == SCANNER_CURRENT_SET_SCANS_QUEUED_MD.get_full_metric_name());
        assert_eq!(current_set_scans_queued.map(|m| m.value), Some(5.0));

        let current_set_scans_active = metrics
            .iter()
            .find(|m| m.name == SCANNER_CURRENT_SET_SCANS_ACTIVE_MD.get_full_metric_name());
        assert_eq!(current_set_scans_active.map(|m| m.value), Some(2.0));

        let current_disk_scan_concurrency_limit = metrics
            .iter()
            .find(|m| m.name == SCANNER_CURRENT_DISK_SCAN_CONCURRENCY_LIMIT_MD.get_full_metric_name());
        assert_eq!(current_disk_scan_concurrency_limit.map(|m| m.value), Some(6.0));

        let current_disk_bucket_scans_queued = metrics
            .iter()
            .find(|m| m.name == SCANNER_CURRENT_DISK_BUCKET_SCANS_QUEUED_MD.get_full_metric_name());
        assert_eq!(current_disk_bucket_scans_queued.map(|m| m.value), Some(18.0));

        let current_disk_bucket_scans_active = metrics
            .iter()
            .find(|m| m.name == SCANNER_CURRENT_DISK_BUCKET_SCANS_ACTIVE_MD.get_full_metric_name());
        assert_eq!(current_disk_bucket_scans_active.map(|m| m.value), Some(4.0));

        let bucket_scans_failed = metrics
            .iter()
            .find(|m| m.name == SCANNER_BUCKET_SCANS_FAILED_MD.get_full_metric_name());
        assert_eq!(bucket_scans_failed.map(|m| m.value), Some(2.0));

        let throttle_idle_mode_enabled = metrics
            .iter()
            .find(|m| m.name == SCANNER_THROTTLE_IDLE_MODE_ENABLED_MD.get_full_metric_name());
        assert_eq!(throttle_idle_mode_enabled.map(|m| m.value), Some(1.0));

        let throttle_sleep_factor = metrics
            .iter()
            .find(|m| m.name == SCANNER_THROTTLE_SLEEP_FACTOR_MD.get_full_metric_name());
        assert_eq!(throttle_sleep_factor.map(|m| m.value), Some(10.0));

        let throttle_max_sleep = metrics
            .iter()
            .find(|m| m.name == SCANNER_THROTTLE_MAX_SLEEP_SECONDS_MD.get_full_metric_name());
        assert_eq!(throttle_max_sleep.map(|m| m.value), Some(15.0));

        let yield_every_n_objects = metrics
            .iter()
            .find(|m| m.name == SCANNER_YIELD_EVERY_N_OBJECTS_MD.get_full_metric_name());
        assert_eq!(yield_every_n_objects.map(|m| m.value), Some(128.0));

        let cycle_interval_seconds = metrics
            .iter()
            .find(|m| m.name == SCANNER_CYCLE_INTERVAL_SECONDS_MD.get_full_metric_name());
        assert_eq!(cycle_interval_seconds.map(|m| m.value), Some(3600.0));

        let cycle_max_duration_seconds = metrics
            .iter()
            .find(|m| m.name == SCANNER_CYCLE_MAX_DURATION_SECONDS_MD.get_full_metric_name());
        assert_eq!(cycle_max_duration_seconds.map(|m| m.value), Some(1800.0));

        let cycle_max_objects = metrics
            .iter()
            .find(|m| m.name == SCANNER_CYCLE_MAX_OBJECTS_MD.get_full_metric_name());
        assert_eq!(cycle_max_objects.map(|m| m.value), Some(1_000_000.0));

        let cycle_max_directories = metrics
            .iter()
            .find(|m| m.name == SCANNER_CYCLE_MAX_DIRECTORIES_MD.get_full_metric_name());
        assert_eq!(cycle_max_directories.map(|m| m.value), Some(100_000.0));

        let bitrot_cycle_enabled = metrics
            .iter()
            .find(|m| m.name == SCANNER_BITROT_CYCLE_ENABLED_MD.get_full_metric_name());
        assert_eq!(bitrot_cycle_enabled.map(|m| m.value), Some(1.0));

        let bitrot_cycle_seconds = metrics
            .iter()
            .find(|m| m.name == SCANNER_BITROT_CYCLE_SECONDS_MD.get_full_metric_name());
        assert_eq!(bitrot_cycle_seconds.map(|m| m.value), Some(86400.0));

        let current_cycle = metrics
            .iter()
            .find(|m| m.name == SCANNER_CURRENT_CYCLE_MD.get_full_metric_name());
        assert_eq!(current_cycle.map(|m| m.value), Some(12.0));

        let completed_cycles = metrics
            .iter()
            .find(|m| m.name == SCANNER_COMPLETED_CYCLES_MD.get_full_metric_name());
        assert_eq!(completed_cycles.map(|m| m.value), Some(11.0));

        let current_cycle_age = metrics
            .iter()
            .find(|m| m.name == SCANNER_CURRENT_CYCLE_AGE_SECONDS_MD.get_full_metric_name());
        assert_eq!(current_cycle_age.map(|m| m.value), Some(90.0));

        let current_cycle_objects = metrics
            .iter()
            .find(|m| m.name == SCANNER_CURRENT_CYCLE_OBJECTS_SCANNED_MD.get_full_metric_name());
        assert_eq!(current_cycle_objects.map(|m| m.value), Some(250.0));

        let current_cycle_directories = metrics
            .iter()
            .find(|m| m.name == SCANNER_CURRENT_CYCLE_DIRECTORIES_SCANNED_MD.get_full_metric_name());
        assert_eq!(current_cycle_directories.map(|m| m.value), Some(20.0));

        let current_cycle_bucket_drive_scans = metrics
            .iter()
            .find(|m| m.name == SCANNER_CURRENT_CYCLE_BUCKET_DRIVE_SCANS_MD.get_full_metric_name());
        assert_eq!(current_cycle_bucket_drive_scans.map(|m| m.value), Some(2.0));

        let current_cycle_bucket_drive_failures = metrics
            .iter()
            .find(|m| m.name == SCANNER_CURRENT_CYCLE_BUCKET_DRIVE_FAILURES_MD.get_full_metric_name());
        assert_eq!(current_cycle_bucket_drive_failures.map(|m| m.value), Some(1.0));

        let current_cycle_objects_rate = metrics
            .iter()
            .find(|m| m.name == SCANNER_CURRENT_CYCLE_OBJECTS_PER_SECOND_MD.get_full_metric_name());
        assert_eq!(current_cycle_objects_rate.map(|m| m.value), Some(12.5));

        let current_cycle_directories_rate = metrics
            .iter()
            .find(|m| m.name == SCANNER_CURRENT_CYCLE_DIRECTORIES_PER_SECOND_MD.get_full_metric_name());
        assert_eq!(current_cycle_directories_rate.map(|m| m.value), Some(1.0));

        let current_cycle_bucket_drive_scans_rate = metrics
            .iter()
            .find(|m| m.name == SCANNER_CURRENT_CYCLE_BUCKET_DRIVE_SCANS_PER_SECOND_MD.get_full_metric_name());
        assert_eq!(current_cycle_bucket_drive_scans_rate.map(|m| m.value), Some(0.1));

        let current_cycle_yield_events = metrics
            .iter()
            .find(|m| m.name == SCANNER_CURRENT_CYCLE_YIELD_EVENTS_MD.get_full_metric_name());
        assert_eq!(current_cycle_yield_events.map(|m| m.value), Some(8.0));

        let current_cycle_yield_duration = metrics
            .iter()
            .find(|m| m.name == SCANNER_CURRENT_CYCLE_YIELD_DURATION_SECONDS_MD.get_full_metric_name());
        assert_eq!(current_cycle_yield_duration.map(|m| m.value), Some(1.25));

        let current_cycle_throttle_sleep_events = metrics
            .iter()
            .find(|m| m.name == SCANNER_CURRENT_CYCLE_THROTTLE_SLEEP_EVENTS_MD.get_full_metric_name());
        assert_eq!(current_cycle_throttle_sleep_events.map(|m| m.value), Some(4.0));

        let current_cycle_throttle_sleep_duration = metrics
            .iter()
            .find(|m| m.name == SCANNER_CURRENT_CYCLE_THROTTLE_SLEEP_DURATION_SECONDS_MD.get_full_metric_name());
        assert_eq!(current_cycle_throttle_sleep_duration.map(|m| m.value), Some(2.5));

        let current_cycle_ilm_actions = metrics
            .iter()
            .find(|m| m.name == SCANNER_CURRENT_CYCLE_ILM_ACTIONS_MD.get_full_metric_name());
        assert_eq!(current_cycle_ilm_actions.map(|m| m.value), Some(6.0));

        let current_cycle_heal_objects = metrics
            .iter()
            .find(|m| m.name == SCANNER_CURRENT_CYCLE_HEAL_OBJECTS_MD.get_full_metric_name());
        assert_eq!(current_cycle_heal_objects.map(|m| m.value), Some(2.0));

        let current_cycle_replication_checks = metrics
            .iter()
            .find(|m| m.name == SCANNER_CURRENT_CYCLE_REPLICATION_CHECKS_MD.get_full_metric_name());
        assert_eq!(current_cycle_replication_checks.map(|m| m.value), Some(5.0));

        let current_cycle_usage_saves = metrics
            .iter()
            .find(|m| m.name == SCANNER_CURRENT_CYCLE_USAGE_SAVES_MD.get_full_metric_name());
        assert_eq!(current_cycle_usage_saves.map(|m| m.value), Some(3.0));

        let current_scan_mode = metrics
            .iter()
            .find(|m| m.name == SCANNER_CURRENT_SCAN_MODE_MD.get_full_metric_name());
        assert_eq!(current_scan_mode.map(|m| m.value), Some(2.0));

        let last_cycle_result = metrics
            .iter()
            .find(|m| m.name == SCANNER_LAST_CYCLE_RESULT_MD.get_full_metric_name());
        assert_eq!(last_cycle_result.map(|m| m.value), Some(1.0));

        let last_cycle_partial_reason = metrics
            .iter()
            .find(|m| m.name == SCANNER_LAST_CYCLE_PARTIAL_REASON_MD.get_full_metric_name());
        assert_eq!(last_cycle_partial_reason.map(|m| m.value), Some(3.0));

        let last_cycle_duration = metrics
            .iter()
            .find(|m| m.name == SCANNER_LAST_CYCLE_DURATION_SECONDS_MD.get_full_metric_name());
        assert_eq!(last_cycle_duration.map(|m| m.value), Some(42.5));

        let last_cycle_objects = metrics
            .iter()
            .find(|m| m.name == SCANNER_LAST_CYCLE_OBJECTS_SCANNED_MD.get_full_metric_name());
        assert_eq!(last_cycle_objects.map(|m| m.value), Some(900.0));

        let last_cycle_directories = metrics
            .iter()
            .find(|m| m.name == SCANNER_LAST_CYCLE_DIRECTORIES_SCANNED_MD.get_full_metric_name());
        assert_eq!(last_cycle_directories.map(|m| m.value), Some(80.0));

        let last_cycle_bucket_drive_scans = metrics
            .iter()
            .find(|m| m.name == SCANNER_LAST_CYCLE_BUCKET_DRIVE_SCANS_MD.get_full_metric_name());
        assert_eq!(last_cycle_bucket_drive_scans.map(|m| m.value), Some(6.0));

        let last_cycle_bucket_drive_failures = metrics
            .iter()
            .find(|m| m.name == SCANNER_LAST_CYCLE_BUCKET_DRIVE_FAILURES_MD.get_full_metric_name());
        assert_eq!(last_cycle_bucket_drive_failures.map(|m| m.value), Some(2.0));

        let last_cycle_objects_rate = metrics
            .iter()
            .find(|m| m.name == SCANNER_LAST_CYCLE_OBJECTS_PER_SECOND_MD.get_full_metric_name());
        assert_eq!(last_cycle_objects_rate.map(|m| m.value), Some(18.0));

        let last_cycle_directories_rate = metrics
            .iter()
            .find(|m| m.name == SCANNER_LAST_CYCLE_DIRECTORIES_PER_SECOND_MD.get_full_metric_name());
        assert_eq!(last_cycle_directories_rate.map(|m| m.value), Some(1.6));

        let last_cycle_bucket_drive_scans_rate = metrics
            .iter()
            .find(|m| m.name == SCANNER_LAST_CYCLE_BUCKET_DRIVE_SCANS_PER_SECOND_MD.get_full_metric_name());
        assert_eq!(last_cycle_bucket_drive_scans_rate.map(|m| m.value), Some(0.12));

        let last_cycle_yield_events = metrics
            .iter()
            .find(|m| m.name == SCANNER_LAST_CYCLE_YIELD_EVENTS_MD.get_full_metric_name());
        assert_eq!(last_cycle_yield_events.map(|m| m.value), Some(30.0));

        let last_cycle_yield_duration = metrics
            .iter()
            .find(|m| m.name == SCANNER_LAST_CYCLE_YIELD_DURATION_SECONDS_MD.get_full_metric_name());
        assert_eq!(last_cycle_yield_duration.map(|m| m.value), Some(9.5));

        let last_cycle_throttle_sleep_events = metrics
            .iter()
            .find(|m| m.name == SCANNER_LAST_CYCLE_THROTTLE_SLEEP_EVENTS_MD.get_full_metric_name());
        assert_eq!(last_cycle_throttle_sleep_events.map(|m| m.value), Some(12.0));

        let last_cycle_throttle_sleep_duration = metrics
            .iter()
            .find(|m| m.name == SCANNER_LAST_CYCLE_THROTTLE_SLEEP_DURATION_SECONDS_MD.get_full_metric_name());
        assert_eq!(last_cycle_throttle_sleep_duration.map(|m| m.value), Some(6.75));

        let last_cycle_ilm_actions = metrics
            .iter()
            .find(|m| m.name == SCANNER_LAST_CYCLE_ILM_ACTIONS_MD.get_full_metric_name());
        assert_eq!(last_cycle_ilm_actions.map(|m| m.value), Some(44.0));

        let last_cycle_heal_objects = metrics
            .iter()
            .find(|m| m.name == SCANNER_LAST_CYCLE_HEAL_OBJECTS_MD.get_full_metric_name());
        assert_eq!(last_cycle_heal_objects.map(|m| m.value), Some(7.0));

        let last_cycle_replication_checks = metrics
            .iter()
            .find(|m| m.name == SCANNER_LAST_CYCLE_REPLICATION_CHECKS_MD.get_full_metric_name());
        assert_eq!(last_cycle_replication_checks.map(|m| m.value), Some(12.0));

        let last_cycle_usage_saves = metrics
            .iter()
            .find(|m| m.name == SCANNER_LAST_CYCLE_USAGE_SAVES_MD.get_full_metric_name());
        assert_eq!(last_cycle_usage_saves.map(|m| m.value), Some(9.0));

        let failed_cycles = metrics
            .iter()
            .find(|m| m.name == SCANNER_FAILED_CYCLES_MD.get_full_metric_name());
        assert_eq!(failed_cycles.map(|m| m.value), Some(3.0));

        let superseded_cycles = metrics
            .iter()
            .find(|m| m.name == SCANNER_SUPERSEDED_CYCLES_MD.get_full_metric_name());
        assert_eq!(superseded_cycles.map(|m| m.value), Some(5.0));

        let partial_cycles = metrics
            .iter()
            .find(|m| m.name == SCANNER_PARTIAL_CYCLES_MD.get_full_metric_name());
        assert_eq!(partial_cycles.map(|m| m.value), Some(10.0));

        let partial_cycles_runtime = metrics.iter().find(|m| {
            m.name == SCANNER_PARTIAL_CYCLES_BY_REASON_MD.get_full_metric_name()
                && m.labels
                    .iter()
                    .any(|(name, value)| *name == "reason" && value.as_ref() == "runtime")
        });
        assert_eq!(partial_cycles_runtime.map(|m| m.value), Some(2.0));

        let partial_cycles_objects = metrics.iter().find(|m| {
            m.name == SCANNER_PARTIAL_CYCLES_BY_REASON_MD.get_full_metric_name()
                && m.labels
                    .iter()
                    .any(|(name, value)| *name == "reason" && value.as_ref() == "objects")
        });
        assert_eq!(partial_cycles_objects.map(|m| m.value), Some(3.0));

        let partial_cycles_directories = metrics.iter().find(|m| {
            m.name == SCANNER_PARTIAL_CYCLES_BY_REASON_MD.get_full_metric_name()
                && m.labels
                    .iter()
                    .any(|(name, value)| *name == "reason" && value.as_ref() == "directories")
        });
        assert_eq!(partial_cycles_directories.map(|m| m.value), Some(4.0));

        let lifecycle_failed = metrics.iter().find(|m| {
            m.name == SCANNER_SOURCE_WORK_TOTAL_MD.get_full_metric_name()
                && m.labels
                    .iter()
                    .any(|(name, value)| *name == SERVER_LABEL && value.as_ref() == "node1:9000")
                && m.labels
                    .iter()
                    .any(|(name, value)| *name == SOURCE_LABEL && value.as_ref() == "lifecycle")
                && m.labels
                    .iter()
                    .any(|(name, value)| *name == STATE_LABEL && value.as_ref() == "failed")
        });
        assert_eq!(lifecycle_failed.map(|m| m.value), Some(4.0));

        let current_usage_executed = metrics.iter().find(|m| {
            m.name == SCANNER_CYCLE_SOURCE_WORK_MD.get_full_metric_name()
                && m.labels
                    .iter()
                    .any(|(name, value)| *name == SERVER_LABEL && value.as_ref() == "node1:9000")
                && m.labels
                    .iter()
                    .any(|(name, value)| *name == CYCLE_SCOPE_LABEL && value.as_ref() == "current")
                && m.labels
                    .iter()
                    .any(|(name, value)| *name == SOURCE_LABEL && value.as_ref() == "usage")
                && m.labels
                    .iter()
                    .any(|(name, value)| *name == STATE_LABEL && value.as_ref() == "executed")
        });
        assert_eq!(current_usage_executed.map(|m| m.value), Some(8.0));
    }

    #[test]
    fn test_collect_scanner_metrics_default() {
        let stats = ScannerStats::default();
        let metrics = collect_scanner_metrics(&stats);

        assert_eq!(metrics.len(), 69);
        for metric in &metrics {
            assert_eq!(metric.value, 0.0);
            if metric.name == SCANNER_PARTIAL_CYCLES_BY_REASON_MD.get_full_metric_name() {
                assert_eq!(metric.labels.len(), 1);
                assert_eq!(metric.labels[0].0, "reason");
                assert!(matches!(metric.labels[0].1.as_ref(), "unknown" | "runtime" | "objects" | "directories"));
            } else {
                assert!(metric.labels.is_empty());
            }
        }
    }
}
