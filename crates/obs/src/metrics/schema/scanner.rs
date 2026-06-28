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

use crate::{MetricDescriptor, MetricName, new_counter_md, new_gauge_md, subsystems};
use std::sync::LazyLock;

pub static SCANNER_BUCKET_SCANS_FINISHED_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::ScannerBucketScansFinished,
        "Total number of bucket-drive scans finished since server start",
        &[],
        subsystems::SCANNER,
    )
});

pub static SCANNER_BUCKET_SCANS_STARTED_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::ScannerBucketScansStarted,
        "Total number of bucket-drive scans started since server start",
        &[],
        subsystems::SCANNER,
    )
});

pub static SCANNER_BUCKET_SCANS_FAILED_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::ScannerBucketScansFailed,
        "Total number of bucket-drive scans that failed since server start",
        &[],
        subsystems::SCANNER,
    )
});

pub static SCANNER_DIRECTORIES_SCANNED_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::ScannerDirectoriesScanned,
        "Total number of directories scanned since server start",
        &[],
        subsystems::SCANNER,
    )
});

pub static SCANNER_OBJECTS_SCANNED_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::ScannerObjectsScanned,
        "Total number of unique objects scanned since server start",
        &[],
        subsystems::SCANNER,
    )
});

pub static SCANNER_VERSIONS_SCANNED_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::ScannerVersionsScanned,
        "Total number of object versions scanned since server start",
        &[],
        subsystems::SCANNER,
    )
});

pub static SCANNER_LAST_ACTIVITY_SECONDS_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::ScannerLastActivitySeconds,
        "Time elapsed (in seconds) since last scan activity.",
        &[],
        subsystems::SCANNER,
    )
});

pub static SCANNER_ACTIVE_PATHS_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::ScannerActivePaths,
        "Current number of scanner paths being processed.",
        &[],
        subsystems::SCANNER,
    )
});

pub static SCANNER_OLDEST_ACTIVE_PATH_AGE_SECONDS_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::ScannerOldestActivePathAgeSeconds,
        "Time elapsed (in seconds) since the oldest active scanner path was last updated, or zero when idle.",
        &[],
        subsystems::SCANNER,
    )
});

pub static SCANNER_CURRENT_SET_SCAN_CONCURRENCY_LIMIT_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::ScannerCurrentSetScanConcurrencyLimit,
        "Current aggregate scanner set scan concurrency limit across active scanner work.",
        &[],
        subsystems::SCANNER,
    )
});

pub static SCANNER_CURRENT_SET_SCANS_QUEUED_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::ScannerCurrentSetScansQueued,
        "Current number of queued scanner set scans across active scanner work.",
        &[],
        subsystems::SCANNER,
    )
});

pub static SCANNER_CURRENT_SET_SCANS_ACTIVE_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::ScannerCurrentSetScansActive,
        "Current number of active scanner set scans across active scanner work.",
        &[],
        subsystems::SCANNER,
    )
});

pub static SCANNER_CURRENT_DISK_SCAN_CONCURRENCY_LIMIT_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::ScannerCurrentDiskScanConcurrencyLimit,
        "Current aggregate scanner disk-bucket scan concurrency limit across active scanner work.",
        &[],
        subsystems::SCANNER,
    )
});

pub static SCANNER_CURRENT_DISK_BUCKET_SCANS_QUEUED_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::ScannerCurrentDiskBucketScansQueued,
        "Current number of queued scanner disk-bucket scans across active scanner work.",
        &[],
        subsystems::SCANNER,
    )
});

pub static SCANNER_CURRENT_DISK_BUCKET_SCANS_ACTIVE_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::ScannerCurrentDiskBucketScansActive,
        "Current number of active scanner disk-bucket scans across active scanner work.",
        &[],
        subsystems::SCANNER,
    )
});

pub static SCANNER_THROTTLE_IDLE_MODE_ENABLED_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::ScannerThrottleIdleModeEnabled,
        "Whether scanner idle-mode self-throttling is enabled: 1 enabled, 0 disabled.",
        &[],
        subsystems::SCANNER,
    )
});

pub static SCANNER_THROTTLE_SLEEP_FACTOR_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::ScannerThrottleSleepFactor,
        "Effective scanner sleep factor used to compute proportional self-throttle sleeps.",
        &[],
        subsystems::SCANNER,
    )
});

pub static SCANNER_THROTTLE_MAX_SLEEP_SECONDS_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::ScannerThrottleMaxSleepSeconds,
        "Effective maximum scanner self-throttle sleep duration in seconds.",
        &[],
        subsystems::SCANNER,
    )
});

pub static SCANNER_YIELD_EVERY_N_OBJECTS_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::ScannerYieldEveryNObjects,
        "Current object interval for cooperative scanner runtime yields, or zero when disabled.",
        &[],
        subsystems::SCANNER,
    )
});

pub static SCANNER_CYCLE_INTERVAL_SECONDS_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::ScannerCycleIntervalSeconds,
        "Effective scanner cycle interval in seconds.",
        &[],
        subsystems::SCANNER,
    )
});

pub static SCANNER_CYCLE_MAX_DURATION_SECONDS_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::ScannerCycleMaxDurationSeconds,
        "Effective maximum scanner cycle runtime in seconds; zero means unlimited.",
        &[],
        subsystems::SCANNER,
    )
});

pub static SCANNER_CYCLE_MAX_OBJECTS_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::ScannerCycleMaxObjects,
        "Effective maximum objects processed by one scanner cycle; zero means unlimited.",
        &[],
        subsystems::SCANNER,
    )
});

pub static SCANNER_CYCLE_MAX_DIRECTORIES_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::ScannerCycleMaxDirectories,
        "Effective maximum directories entered by one scanner cycle; zero means unlimited.",
        &[],
        subsystems::SCANNER,
    )
});

pub static SCANNER_BITROT_CYCLE_ENABLED_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::ScannerBitrotCycleEnabled,
        "Whether periodic scanner bitrot deep scans are enabled: 1 enabled, 0 disabled.",
        &[],
        subsystems::SCANNER,
    )
});

pub static SCANNER_BITROT_CYCLE_SECONDS_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::ScannerBitrotCycleSeconds,
        "Effective scanner bitrot deep-scan interval in seconds; zero means disabled or deep-scan every cycle.",
        &[],
        subsystems::SCANNER,
    )
});

pub static SCANNER_CURRENT_CYCLE_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::ScannerCurrentCycle,
        "Current scanner cycle number, or zero when no cycle is running.",
        &[],
        subsystems::SCANNER,
    )
});

pub static SCANNER_COMPLETED_CYCLES_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::ScannerCompletedCycles,
        "Total number of scanner cycles completed since server start.",
        &[],
        subsystems::SCANNER,
    )
});

pub static SCANNER_CURRENT_CYCLE_AGE_SECONDS_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::ScannerCurrentCycleAgeSeconds,
        "Time elapsed (in seconds) since the current scanner cycle started, or zero when no cycle is running.",
        &[],
        subsystems::SCANNER,
    )
});

pub static SCANNER_CURRENT_CYCLE_OBJECTS_SCANNED_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::ScannerCurrentCycleObjectsScanned,
        "Number of objects scanned by the currently running scanner cycle.",
        &[],
        subsystems::SCANNER,
    )
});

pub static SCANNER_CURRENT_CYCLE_DIRECTORIES_SCANNED_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::ScannerCurrentCycleDirectoriesScanned,
        "Number of directories scanned by the currently running scanner cycle.",
        &[],
        subsystems::SCANNER,
    )
});

pub static SCANNER_CURRENT_CYCLE_BUCKET_DRIVE_SCANS_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::ScannerCurrentCycleBucketDriveScans,
        "Number of bucket-drive scans finished by the currently running scanner cycle.",
        &[],
        subsystems::SCANNER,
    )
});

pub static SCANNER_CURRENT_CYCLE_BUCKET_DRIVE_FAILURES_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::ScannerCurrentCycleBucketDriveFailures,
        "Number of bucket-drive scans that failed in the currently running scanner cycle.",
        &[],
        subsystems::SCANNER,
    )
});

pub static SCANNER_CURRENT_CYCLE_OBJECTS_PER_SECOND_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::ScannerCurrentCycleObjectsPerSecond,
        "Object scan rate for the currently running scanner cycle.",
        &[],
        subsystems::SCANNER,
    )
});

pub static SCANNER_CURRENT_CYCLE_DIRECTORIES_PER_SECOND_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::ScannerCurrentCycleDirectoriesPerSecond,
        "Directory scan rate for the currently running scanner cycle.",
        &[],
        subsystems::SCANNER,
    )
});

pub static SCANNER_CURRENT_CYCLE_BUCKET_DRIVE_SCANS_PER_SECOND_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::ScannerCurrentCycleBucketDriveScansPerSecond,
        "Bucket-drive scan rate for the currently running scanner cycle.",
        &[],
        subsystems::SCANNER,
    )
});

pub static SCANNER_CURRENT_CYCLE_YIELD_EVENTS_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::ScannerCurrentCycleYieldEvents,
        "Number of scanner cooperative yield events in the currently running scanner cycle.",
        &[],
        subsystems::SCANNER,
    )
});

pub static SCANNER_CURRENT_CYCLE_YIELD_DURATION_SECONDS_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::ScannerCurrentCycleYieldDurationSeconds,
        "Total scanner cooperative yield duration in seconds for the currently running scanner cycle.",
        &[],
        subsystems::SCANNER,
    )
});

pub static SCANNER_CURRENT_CYCLE_THROTTLE_SLEEP_EVENTS_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::ScannerCurrentCycleThrottleSleepEvents,
        "Number of scanner self-throttle sleep events in the currently running scanner cycle.",
        &[],
        subsystems::SCANNER,
    )
});

pub static SCANNER_CURRENT_CYCLE_THROTTLE_SLEEP_DURATION_SECONDS_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::ScannerCurrentCycleThrottleSleepDurationSeconds,
        "Total scanner self-throttle sleep duration in seconds for the currently running scanner cycle.",
        &[],
        subsystems::SCANNER,
    )
});

pub static SCANNER_CURRENT_CYCLE_ILM_ACTIONS_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::ScannerCurrentCycleIlmActions,
        "Number of lifecycle actions applied by the currently running scanner cycle.",
        &[],
        subsystems::SCANNER,
    )
});

pub static SCANNER_CURRENT_CYCLE_HEAL_OBJECTS_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::ScannerCurrentCycleHealObjects,
        "Number of object heal candidates enqueued by the currently running scanner cycle.",
        &[],
        subsystems::SCANNER,
    )
});

pub static SCANNER_CURRENT_CYCLE_REPLICATION_CHECKS_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::ScannerCurrentCycleReplicationChecks,
        "Number of replication heal checks run by the currently running scanner cycle.",
        &[],
        subsystems::SCANNER,
    )
});

pub static SCANNER_CURRENT_CYCLE_USAGE_SAVES_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::ScannerCurrentCycleUsageSaves,
        "Number of data-usage save operations run by the currently running scanner cycle.",
        &[],
        subsystems::SCANNER,
    )
});

pub static SCANNER_CURRENT_SCAN_MODE_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::ScannerCurrentScanMode,
        "Current scanner mode: 0 unknown or idle, 1 normal, 2 deep bitrot scan.",
        &[],
        subsystems::SCANNER,
    )
});

pub static SCANNER_LAST_CYCLE_RESULT_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::ScannerLastCycleResult,
        "Last scanner cycle result: 0 unknown, 1 success, 2 error, 3 partial.",
        &[],
        subsystems::SCANNER,
    )
});

pub static SCANNER_LAST_CYCLE_PARTIAL_REASON_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::ScannerLastCyclePartialReason,
        "Last scanner partial cycle reason: 0 unknown, 1 runtime budget, 2 object budget, 3 directory budget.",
        &[],
        subsystems::SCANNER,
    )
});

pub static SCANNER_LAST_CYCLE_DURATION_SECONDS_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::ScannerLastCycleDurationSeconds,
        "Duration in seconds of the last finished scanner cycle.",
        &[],
        subsystems::SCANNER,
    )
});

pub static SCANNER_LAST_CYCLE_OBJECTS_SCANNED_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::ScannerLastCycleObjectsScanned,
        "Number of objects scanned by the last finished scanner cycle.",
        &[],
        subsystems::SCANNER,
    )
});

pub static SCANNER_LAST_CYCLE_DIRECTORIES_SCANNED_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::ScannerLastCycleDirectoriesScanned,
        "Number of directories scanned by the last finished scanner cycle.",
        &[],
        subsystems::SCANNER,
    )
});

pub static SCANNER_LAST_CYCLE_BUCKET_DRIVE_SCANS_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::ScannerLastCycleBucketDriveScans,
        "Number of bucket-drive scans finished by the last scanner cycle.",
        &[],
        subsystems::SCANNER,
    )
});

pub static SCANNER_LAST_CYCLE_BUCKET_DRIVE_FAILURES_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::ScannerLastCycleBucketDriveFailures,
        "Number of bucket-drive scans that failed in the last finished scanner cycle.",
        &[],
        subsystems::SCANNER,
    )
});

pub static SCANNER_LAST_CYCLE_OBJECTS_PER_SECOND_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::ScannerLastCycleObjectsPerSecond,
        "Object scan rate for the last finished scanner cycle.",
        &[],
        subsystems::SCANNER,
    )
});

pub static SCANNER_LAST_CYCLE_DIRECTORIES_PER_SECOND_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::ScannerLastCycleDirectoriesPerSecond,
        "Directory scan rate for the last finished scanner cycle.",
        &[],
        subsystems::SCANNER,
    )
});

pub static SCANNER_LAST_CYCLE_BUCKET_DRIVE_SCANS_PER_SECOND_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::ScannerLastCycleBucketDriveScansPerSecond,
        "Bucket-drive scan rate for the last finished scanner cycle.",
        &[],
        subsystems::SCANNER,
    )
});

pub static SCANNER_LAST_CYCLE_YIELD_EVENTS_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::ScannerLastCycleYieldEvents,
        "Number of scanner cooperative yield events in the last finished scanner cycle.",
        &[],
        subsystems::SCANNER,
    )
});

pub static SCANNER_LAST_CYCLE_YIELD_DURATION_SECONDS_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::ScannerLastCycleYieldDurationSeconds,
        "Total scanner cooperative yield duration in seconds for the last finished scanner cycle.",
        &[],
        subsystems::SCANNER,
    )
});

pub static SCANNER_LAST_CYCLE_THROTTLE_SLEEP_EVENTS_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::ScannerLastCycleThrottleSleepEvents,
        "Number of scanner self-throttle sleep events in the last finished scanner cycle.",
        &[],
        subsystems::SCANNER,
    )
});

pub static SCANNER_LAST_CYCLE_THROTTLE_SLEEP_DURATION_SECONDS_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::ScannerLastCycleThrottleSleepDurationSeconds,
        "Total scanner self-throttle sleep duration in seconds for the last finished scanner cycle.",
        &[],
        subsystems::SCANNER,
    )
});

pub static SCANNER_LAST_CYCLE_ILM_ACTIONS_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::ScannerLastCycleIlmActions,
        "Number of lifecycle actions applied by the last finished scanner cycle.",
        &[],
        subsystems::SCANNER,
    )
});

pub static SCANNER_LAST_CYCLE_HEAL_OBJECTS_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::ScannerLastCycleHealObjects,
        "Number of object heal candidates enqueued by the last finished scanner cycle.",
        &[],
        subsystems::SCANNER,
    )
});

pub static SCANNER_LAST_CYCLE_REPLICATION_CHECKS_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::ScannerLastCycleReplicationChecks,
        "Number of replication heal checks run by the last finished scanner cycle.",
        &[],
        subsystems::SCANNER,
    )
});

pub static SCANNER_LAST_CYCLE_USAGE_SAVES_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::ScannerLastCycleUsageSaves,
        "Number of data-usage save operations run by the last finished scanner cycle.",
        &[],
        subsystems::SCANNER,
    )
});

pub static SCANNER_FAILED_CYCLES_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::ScannerFailedCycles,
        "Total number of scanner cycles that failed since server start.",
        &[],
        subsystems::SCANNER,
    )
});

pub static SCANNER_PARTIAL_CYCLES_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::ScannerPartialCycles,
        "Total number of scanner cycles stopped before completion by cycle budget.",
        &[],
        subsystems::SCANNER,
    )
});

pub static SCANNER_PARTIAL_CYCLES_BY_REASON_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::ScannerPartialCyclesByReason,
        "Total number of scanner cycles stopped before completion by cycle budget reason.",
        &["reason"],
        subsystems::SCANNER,
    )
});
