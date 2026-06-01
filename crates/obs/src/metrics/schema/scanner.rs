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
        "Total number of bucket scans finished since server start",
        &[],
        subsystems::SCANNER,
    )
});

pub static SCANNER_BUCKET_SCANS_STARTED_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::ScannerBucketScansStarted,
        "Total number of bucket scans started since server start",
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
        "Last scanner cycle result: 0 unknown, 1 success, 2 error.",
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

pub static SCANNER_FAILED_CYCLES_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::ScannerFailedCycles,
        "Total number of scanner cycles that failed since server start.",
        &[],
        subsystems::SCANNER,
    )
});
