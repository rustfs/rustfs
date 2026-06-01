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
    SCANNER_ACTIVE_PATHS_MD, SCANNER_BITROT_CYCLE_ENABLED_MD, SCANNER_BITROT_CYCLE_SECONDS_MD, SCANNER_BUCKET_SCANS_FAILED_MD,
    SCANNER_BUCKET_SCANS_FINISHED_MD, SCANNER_BUCKET_SCANS_STARTED_MD, SCANNER_COMPLETED_CYCLES_MD,
    SCANNER_CURRENT_CYCLE_AGE_SECONDS_MD, SCANNER_CURRENT_CYCLE_BUCKET_DRIVE_FAILURES_MD,
    SCANNER_CURRENT_CYCLE_BUCKET_DRIVE_SCANS_MD, SCANNER_CURRENT_CYCLE_BUCKET_DRIVE_SCANS_PER_SECOND_MD,
    SCANNER_CURRENT_CYCLE_DIRECTORIES_PER_SECOND_MD, SCANNER_CURRENT_CYCLE_DIRECTORIES_SCANNED_MD,
    SCANNER_CURRENT_CYCLE_HEAL_OBJECTS_MD, SCANNER_CURRENT_CYCLE_ILM_ACTIONS_MD, SCANNER_CURRENT_CYCLE_MD,
    SCANNER_CURRENT_CYCLE_OBJECTS_PER_SECOND_MD, SCANNER_CURRENT_CYCLE_OBJECTS_SCANNED_MD,
    SCANNER_CURRENT_CYCLE_REPLICATION_CHECKS_MD, SCANNER_CURRENT_CYCLE_USAGE_SAVES_MD,
    SCANNER_CURRENT_CYCLE_YIELD_DURATION_SECONDS_MD, SCANNER_CURRENT_CYCLE_YIELD_EVENTS_MD, SCANNER_CURRENT_SCAN_MODE_MD,
    SCANNER_CYCLE_INTERVAL_SECONDS_MD, SCANNER_DIRECTORIES_SCANNED_MD, SCANNER_FAILED_CYCLES_MD,
    SCANNER_LAST_ACTIVITY_SECONDS_MD, SCANNER_LAST_CYCLE_BUCKET_DRIVE_FAILURES_MD, SCANNER_LAST_CYCLE_BUCKET_DRIVE_SCANS_MD,
    SCANNER_LAST_CYCLE_BUCKET_DRIVE_SCANS_PER_SECOND_MD, SCANNER_LAST_CYCLE_DIRECTORIES_PER_SECOND_MD,
    SCANNER_LAST_CYCLE_DIRECTORIES_SCANNED_MD, SCANNER_LAST_CYCLE_DURATION_SECONDS_MD, SCANNER_LAST_CYCLE_HEAL_OBJECTS_MD,
    SCANNER_LAST_CYCLE_ILM_ACTIONS_MD, SCANNER_LAST_CYCLE_OBJECTS_PER_SECOND_MD, SCANNER_LAST_CYCLE_OBJECTS_SCANNED_MD,
    SCANNER_LAST_CYCLE_REPLICATION_CHECKS_MD, SCANNER_LAST_CYCLE_RESULT_MD, SCANNER_LAST_CYCLE_USAGE_SAVES_MD,
    SCANNER_LAST_CYCLE_YIELD_DURATION_SECONDS_MD, SCANNER_LAST_CYCLE_YIELD_EVENTS_MD, SCANNER_OBJECTS_SCANNED_MD,
    SCANNER_THROTTLE_IDLE_MODE_ENABLED_MD, SCANNER_THROTTLE_MAX_SLEEP_SECONDS_MD, SCANNER_THROTTLE_SLEEP_FACTOR_MD,
    SCANNER_VERSIONS_SCANNED_MD, SCANNER_YIELD_EVERY_N_OBJECTS_MD,
};

/// Scanner statistics.
#[derive(Debug, Clone, Default)]
pub struct ScannerStats {
    /// Number of bucket scans finished
    pub bucket_scans_finished: u64,
    /// Number of bucket scans started
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
    /// Whether scanner idle-mode self-throttling is enabled
    pub throttle_idle_mode_enabled: u64,
    /// Effective scanner sleep factor
    pub throttle_sleep_factor: f64,
    /// Effective scanner maximum self-throttle sleep duration in seconds
    pub throttle_max_sleep_seconds: f64,
    /// Object interval for cooperative scanner runtime yields
    pub yield_every_n_objects: u64,
    /// Effective scanner cycle interval in seconds
    pub cycle_interval_seconds: f64,
    /// Whether periodic scanner bitrot deep scans are enabled
    pub bitrot_cycle_enabled: u64,
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
    /// Number of scanner self-throttle yield events in the current scanner cycle
    pub current_cycle_yield_events: u64,
    /// Total scanner self-throttle yield duration in seconds for the current scanner cycle
    pub current_cycle_yield_duration_seconds: f64,
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
    /// Number of bucket-drive scans that failed in the last finished scanner cycle
    pub last_cycle_bucket_drive_failures: u64,
    /// Object scan rate for the last finished scanner cycle
    pub last_cycle_objects_per_second: f64,
    /// Directory scan rate for the last finished scanner cycle
    pub last_cycle_directories_per_second: f64,
    /// Bucket-drive scan rate for the last finished scanner cycle
    pub last_cycle_bucket_drive_scans_per_second: f64,
    /// Number of scanner self-throttle yield events in the last finished scanner cycle
    pub last_cycle_yield_events: u64,
    /// Total scanner self-throttle yield duration in seconds for the last finished scanner cycle
    pub last_cycle_yield_duration_seconds: f64,
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
}

/// Collects scanner metrics from the given stats.
///
/// Uses the metric descriptors from `metrics_type::scanner` module.
/// Returns a vector of Prometheus metrics for scanner statistics.
pub fn collect_scanner_metrics(stats: &ScannerStats) -> Vec<PrometheusMetric> {
    vec![
        PrometheusMetric::from_descriptor(&SCANNER_BUCKET_SCANS_FINISHED_MD, stats.bucket_scans_finished as f64),
        PrometheusMetric::from_descriptor(&SCANNER_BUCKET_SCANS_STARTED_MD, stats.bucket_scans_started as f64),
        PrometheusMetric::from_descriptor(&SCANNER_BUCKET_SCANS_FAILED_MD, stats.bucket_scans_failed as f64),
        PrometheusMetric::from_descriptor(&SCANNER_DIRECTORIES_SCANNED_MD, stats.directories_scanned as f64),
        PrometheusMetric::from_descriptor(&SCANNER_OBJECTS_SCANNED_MD, stats.objects_scanned as f64),
        PrometheusMetric::from_descriptor(&SCANNER_VERSIONS_SCANNED_MD, stats.versions_scanned as f64),
        PrometheusMetric::from_descriptor(&SCANNER_LAST_ACTIVITY_SECONDS_MD, stats.last_activity_seconds as f64),
        PrometheusMetric::from_descriptor(&SCANNER_ACTIVE_PATHS_MD, stats.active_paths as f64),
        PrometheusMetric::from_descriptor(&SCANNER_THROTTLE_IDLE_MODE_ENABLED_MD, stats.throttle_idle_mode_enabled as f64),
        PrometheusMetric::from_descriptor(&SCANNER_THROTTLE_SLEEP_FACTOR_MD, stats.throttle_sleep_factor),
        PrometheusMetric::from_descriptor(&SCANNER_THROTTLE_MAX_SLEEP_SECONDS_MD, stats.throttle_max_sleep_seconds),
        PrometheusMetric::from_descriptor(&SCANNER_YIELD_EVERY_N_OBJECTS_MD, stats.yield_every_n_objects as f64),
        PrometheusMetric::from_descriptor(&SCANNER_CYCLE_INTERVAL_SECONDS_MD, stats.cycle_interval_seconds),
        PrometheusMetric::from_descriptor(&SCANNER_BITROT_CYCLE_ENABLED_MD, stats.bitrot_cycle_enabled as f64),
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
        PrometheusMetric::from_descriptor(&SCANNER_CURRENT_CYCLE_ILM_ACTIONS_MD, stats.current_cycle_ilm_actions as f64),
        PrometheusMetric::from_descriptor(&SCANNER_CURRENT_CYCLE_HEAL_OBJECTS_MD, stats.current_cycle_heal_objects as f64),
        PrometheusMetric::from_descriptor(
            &SCANNER_CURRENT_CYCLE_REPLICATION_CHECKS_MD,
            stats.current_cycle_replication_checks as f64,
        ),
        PrometheusMetric::from_descriptor(&SCANNER_CURRENT_CYCLE_USAGE_SAVES_MD, stats.current_cycle_usage_saves as f64),
        PrometheusMetric::from_descriptor(&SCANNER_CURRENT_SCAN_MODE_MD, stats.current_scan_mode as f64),
        PrometheusMetric::from_descriptor(&SCANNER_LAST_CYCLE_RESULT_MD, stats.last_cycle_result as f64),
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
        PrometheusMetric::from_descriptor(&SCANNER_LAST_CYCLE_ILM_ACTIONS_MD, stats.last_cycle_ilm_actions as f64),
        PrometheusMetric::from_descriptor(&SCANNER_LAST_CYCLE_HEAL_OBJECTS_MD, stats.last_cycle_heal_objects as f64),
        PrometheusMetric::from_descriptor(&SCANNER_LAST_CYCLE_REPLICATION_CHECKS_MD, stats.last_cycle_replication_checks as f64),
        PrometheusMetric::from_descriptor(&SCANNER_LAST_CYCLE_USAGE_SAVES_MD, stats.last_cycle_usage_saves as f64),
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
            bucket_scans_failed: 2,
            directories_scanned: 50000,
            objects_scanned: 1000000,
            versions_scanned: 1500000,
            last_activity_seconds: 30,
            active_paths: 4,
            throttle_idle_mode_enabled: 1,
            throttle_sleep_factor: 10.0,
            throttle_max_sleep_seconds: 15.0,
            yield_every_n_objects: 128,
            cycle_interval_seconds: 3600.0,
            bitrot_cycle_enabled: 1,
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
            current_cycle_ilm_actions: 6,
            current_cycle_heal_objects: 2,
            current_cycle_replication_checks: 5,
            current_cycle_usage_saves: 3,
            current_scan_mode: 2,
            last_cycle_result: 1,
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
            last_cycle_ilm_actions: 44,
            last_cycle_heal_objects: 7,
            last_cycle_replication_checks: 12,
            last_cycle_usage_saves: 9,
            failed_cycles: 3,
        };

        let metrics = collect_scanner_metrics(&stats);
        report_metrics(&metrics);

        assert_eq!(metrics.len(), 48);

        let objects = metrics.iter().find(|m| m.value == 1000000.0);
        assert!(objects.is_some());

        let last_activity = metrics.iter().find(|m| m.value == 30.0);
        assert!(last_activity.is_some());

        let active_paths = metrics
            .iter()
            .find(|m| m.name == SCANNER_ACTIVE_PATHS_MD.get_full_metric_name());
        assert_eq!(active_paths.map(|m| m.value), Some(4.0));

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
    }

    #[test]
    fn test_collect_scanner_metrics_default() {
        let stats = ScannerStats::default();
        let metrics = collect_scanner_metrics(&stats);

        assert_eq!(metrics.len(), 48);
        for metric in &metrics {
            assert_eq!(metric.value, 0.0);
            assert!(metric.labels.is_empty());
        }
    }
}
