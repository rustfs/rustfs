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

#![allow(dead_code)]

//! Scanner metrics collector.
//!
//! Collects background scanner metrics including bucket scans,
//! directory scans, and object scans.

use crate::metrics::report::PrometheusMetric;
use crate::metrics::schema::scanner::{
    SCANNER_ACTIVE_PATHS_MD, SCANNER_BUCKET_SCANS_FINISHED_MD, SCANNER_BUCKET_SCANS_STARTED_MD, SCANNER_COMPLETED_CYCLES_MD,
    SCANNER_CURRENT_CYCLE_AGE_SECONDS_MD, SCANNER_CURRENT_CYCLE_BUCKET_DRIVE_SCANS_MD,
    SCANNER_CURRENT_CYCLE_BUCKET_DRIVE_SCANS_PER_SECOND_MD, SCANNER_CURRENT_CYCLE_DIRECTORIES_PER_SECOND_MD,
    SCANNER_CURRENT_CYCLE_DIRECTORIES_SCANNED_MD, SCANNER_CURRENT_CYCLE_MD, SCANNER_CURRENT_CYCLE_OBJECTS_PER_SECOND_MD,
    SCANNER_CURRENT_CYCLE_OBJECTS_SCANNED_MD, SCANNER_CURRENT_SCAN_MODE_MD, SCANNER_DIRECTORIES_SCANNED_MD,
    SCANNER_FAILED_CYCLES_MD, SCANNER_LAST_ACTIVITY_SECONDS_MD, SCANNER_LAST_CYCLE_BUCKET_DRIVE_SCANS_MD,
    SCANNER_LAST_CYCLE_BUCKET_DRIVE_SCANS_PER_SECOND_MD, SCANNER_LAST_CYCLE_DIRECTORIES_PER_SECOND_MD,
    SCANNER_LAST_CYCLE_DIRECTORIES_SCANNED_MD, SCANNER_LAST_CYCLE_DURATION_SECONDS_MD, SCANNER_LAST_CYCLE_OBJECTS_PER_SECOND_MD,
    SCANNER_LAST_CYCLE_OBJECTS_SCANNED_MD, SCANNER_LAST_CYCLE_RESULT_MD, SCANNER_OBJECTS_SCANNED_MD, SCANNER_VERSIONS_SCANNED_MD,
};

/// Scanner statistics.
#[derive(Debug, Clone, Default)]
pub struct ScannerStats {
    /// Number of bucket scans finished
    pub bucket_scans_finished: u64,
    /// Number of bucket scans started
    pub bucket_scans_started: u64,
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
    /// Object scan rate for the currently running scanner cycle
    pub current_cycle_objects_per_second: f64,
    /// Directory scan rate for the currently running scanner cycle
    pub current_cycle_directories_per_second: f64,
    /// Bucket-drive scan rate for the currently running scanner cycle
    pub current_cycle_bucket_drive_scans_per_second: f64,
    /// Current scanner mode: 0 unknown or idle, 1 normal, 2 deep bitrot scan
    pub current_scan_mode: u64,
    /// Last scanner cycle result: 0 unknown, 1 success, 2 error
    pub last_cycle_result: u64,
    /// Duration in seconds of the last finished scanner cycle
    pub last_cycle_duration_seconds: f64,
    /// Number of objects scanned by the last finished scanner cycle
    pub last_cycle_objects_scanned: u64,
    /// Number of directories scanned by the last finished scanner cycle
    pub last_cycle_directories_scanned: u64,
    /// Number of bucket-drive scans finished by the last scanner cycle
    pub last_cycle_bucket_drive_scans: u64,
    /// Object scan rate for the last finished scanner cycle
    pub last_cycle_objects_per_second: f64,
    /// Directory scan rate for the last finished scanner cycle
    pub last_cycle_directories_per_second: f64,
    /// Bucket-drive scan rate for the last finished scanner cycle
    pub last_cycle_bucket_drive_scans_per_second: f64,
    /// Number of scanner cycles that failed since server start
    pub failed_cycles: u64,
}

/// Collects scanner metrics from the given stats.
///
/// Uses the metric descriptors from `metrics_type::scanner` module.
/// Returns a vector of Prometheus metrics for scanner statistics.
pub fn collect_scanner_metrics(stats: &ScannerStats) -> Vec<PrometheusMetric> {
    vec![
        PrometheusMetric::from_descriptor(&SCANNER_BUCKET_SCANS_FINISHED_MD, stats.bucket_scans_finished as f64),
        PrometheusMetric::from_descriptor(&SCANNER_BUCKET_SCANS_STARTED_MD, stats.bucket_scans_started as f64),
        PrometheusMetric::from_descriptor(&SCANNER_DIRECTORIES_SCANNED_MD, stats.directories_scanned as f64),
        PrometheusMetric::from_descriptor(&SCANNER_OBJECTS_SCANNED_MD, stats.objects_scanned as f64),
        PrometheusMetric::from_descriptor(&SCANNER_VERSIONS_SCANNED_MD, stats.versions_scanned as f64),
        PrometheusMetric::from_descriptor(&SCANNER_LAST_ACTIVITY_SECONDS_MD, stats.last_activity_seconds as f64),
        PrometheusMetric::from_descriptor(&SCANNER_ACTIVE_PATHS_MD, stats.active_paths as f64),
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
        PrometheusMetric::from_descriptor(&SCANNER_CURRENT_CYCLE_OBJECTS_PER_SECOND_MD, stats.current_cycle_objects_per_second),
        PrometheusMetric::from_descriptor(
            &SCANNER_CURRENT_CYCLE_DIRECTORIES_PER_SECOND_MD,
            stats.current_cycle_directories_per_second,
        ),
        PrometheusMetric::from_descriptor(
            &SCANNER_CURRENT_CYCLE_BUCKET_DRIVE_SCANS_PER_SECOND_MD,
            stats.current_cycle_bucket_drive_scans_per_second,
        ),
        PrometheusMetric::from_descriptor(&SCANNER_CURRENT_SCAN_MODE_MD, stats.current_scan_mode as f64),
        PrometheusMetric::from_descriptor(&SCANNER_LAST_CYCLE_RESULT_MD, stats.last_cycle_result as f64),
        PrometheusMetric::from_descriptor(&SCANNER_LAST_CYCLE_DURATION_SECONDS_MD, stats.last_cycle_duration_seconds),
        PrometheusMetric::from_descriptor(&SCANNER_LAST_CYCLE_OBJECTS_SCANNED_MD, stats.last_cycle_objects_scanned as f64),
        PrometheusMetric::from_descriptor(
            &SCANNER_LAST_CYCLE_DIRECTORIES_SCANNED_MD,
            stats.last_cycle_directories_scanned as f64,
        ),
        PrometheusMetric::from_descriptor(&SCANNER_LAST_CYCLE_BUCKET_DRIVE_SCANS_MD, stats.last_cycle_bucket_drive_scans as f64),
        PrometheusMetric::from_descriptor(&SCANNER_LAST_CYCLE_OBJECTS_PER_SECOND_MD, stats.last_cycle_objects_per_second),
        PrometheusMetric::from_descriptor(&SCANNER_LAST_CYCLE_DIRECTORIES_PER_SECOND_MD, stats.last_cycle_directories_per_second),
        PrometheusMetric::from_descriptor(
            &SCANNER_LAST_CYCLE_BUCKET_DRIVE_SCANS_PER_SECOND_MD,
            stats.last_cycle_bucket_drive_scans_per_second,
        ),
        PrometheusMetric::from_descriptor(&SCANNER_FAILED_CYCLES_MD, stats.failed_cycles as f64),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::report::report_metrics;

    #[test]
    fn test_collect_scanner_metrics() {
        let stats = ScannerStats {
            bucket_scans_finished: 100,
            bucket_scans_started: 100,
            directories_scanned: 50000,
            objects_scanned: 1000000,
            versions_scanned: 1500000,
            last_activity_seconds: 30,
            active_paths: 4,
            current_cycle: 12,
            completed_cycles: 11,
            current_cycle_age_seconds: 90,
            current_cycle_objects_scanned: 250,
            current_cycle_directories_scanned: 20,
            current_cycle_bucket_drive_scans: 2,
            current_cycle_objects_per_second: 12.5,
            current_cycle_directories_per_second: 1.0,
            current_cycle_bucket_drive_scans_per_second: 0.1,
            current_scan_mode: 2,
            last_cycle_result: 1,
            last_cycle_duration_seconds: 42.5,
            last_cycle_objects_scanned: 900,
            last_cycle_directories_scanned: 80,
            last_cycle_bucket_drive_scans: 6,
            last_cycle_objects_per_second: 18.0,
            last_cycle_directories_per_second: 1.6,
            last_cycle_bucket_drive_scans_per_second: 0.12,
            failed_cycles: 3,
        };

        let metrics = collect_scanner_metrics(&stats);
        report_metrics(&metrics);

        assert_eq!(metrics.len(), 26);

        let objects = metrics.iter().find(|m| m.value == 1000000.0);
        assert!(objects.is_some());

        let last_activity = metrics.iter().find(|m| m.value == 30.0);
        assert!(last_activity.is_some());

        let active_paths = metrics
            .iter()
            .find(|m| m.name == SCANNER_ACTIVE_PATHS_MD.get_full_metric_name());
        assert_eq!(active_paths.map(|m| m.value), Some(4.0));

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

        let current_scan_mode = metrics
            .iter()
            .find(|m| m.name == SCANNER_CURRENT_SCAN_MODE_MD.get_full_metric_name());
        assert_eq!(current_scan_mode.map(|m| m.value), Some(2.0));

        let last_cycle_result = metrics
            .iter()
            .find(|m| m.name == SCANNER_LAST_CYCLE_RESULT_MD.get_full_metric_name());
        assert_eq!(last_cycle_result.map(|m| m.value), Some(1.0));

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

        let failed_cycles = metrics
            .iter()
            .find(|m| m.name == SCANNER_FAILED_CYCLES_MD.get_full_metric_name());
        assert_eq!(failed_cycles.map(|m| m.value), Some(3.0));
    }

    #[test]
    fn test_collect_scanner_metrics_default() {
        let stats = ScannerStats::default();
        let metrics = collect_scanner_metrics(&stats);

        assert_eq!(metrics.len(), 26);
        for metric in &metrics {
            assert_eq!(metric.value, 0.0);
            assert!(metric.labels.is_empty());
        }
    }
}
