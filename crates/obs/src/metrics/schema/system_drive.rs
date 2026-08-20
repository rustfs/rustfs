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

use crate::{MetricDescriptor, MetricName, new_counter_md, new_gauge_md, subsystems};
use std::sync::LazyLock;

/// drive related labels
pub const DRIVE_LABEL: &str = "drive";
/// server label
pub const SERVER_LABEL: &str = "server";
/// pool index label
pub const POOL_INDEX_LABEL: &str = "pool_index";
/// set index label
pub const SET_INDEX_LABEL: &str = "set_index";
/// drive index label
pub const DRIVE_INDEX_LABEL: &str = "drive_index";
/// API label
pub const API_LABEL: &str = "api";
/// Disk id label
pub const DISK_ID_LABEL: &str = "disk_id";
/// State label
pub const STATE_LABEL: &str = "state";

/// All drive-related labels
pub const ALL_DRIVE_LABELS: [&str; 2] = [SERVER_LABEL, DRIVE_LABEL];
/// Drive labels with erasure-set topology.
pub const DRIVE_TOPOLOGY_LABELS: [&str; 5] = [
    SERVER_LABEL,
    DRIVE_LABEL,
    POOL_INDEX_LABEL,
    SET_INDEX_LABEL,
    DRIVE_INDEX_LABEL,
];
/// Drive info labels.
pub const DRIVE_INFO_LABELS: [&str; 6] = [
    SERVER_LABEL,
    DRIVE_LABEL,
    POOL_INDEX_LABEL,
    SET_INDEX_LABEL,
    DRIVE_INDEX_LABEL,
    DISK_ID_LABEL,
];
/// Drive topology labels with a state dimension.
pub const DRIVE_TOPOLOGY_STATE_LABELS: [&str; 6] = [
    SERVER_LABEL,
    DRIVE_LABEL,
    POOL_INDEX_LABEL,
    SET_INDEX_LABEL,
    DRIVE_INDEX_LABEL,
    STATE_LABEL,
];
/// Drive topology labels with an API dimension.
pub const DRIVE_TOPOLOGY_API_LABELS: [&str; 6] = [
    SERVER_LABEL,
    DRIVE_LABEL,
    POOL_INDEX_LABEL,
    SET_INDEX_LABEL,
    DRIVE_INDEX_LABEL,
    API_LABEL,
];

pub static DRIVE_INFO_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::Custom("info".to_string()),
        "Drive topology and stable disk identity information",
        &DRIVE_INFO_LABELS,
        subsystems::SYSTEM_DRIVE,
    )
});

pub static DRIVE_RUNTIME_STATE_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::Custom("runtime_state".to_string()),
        "Drive runtime state (1 for the active state label, 0 otherwise)",
        &DRIVE_TOPOLOGY_STATE_LABELS,
        subsystems::SYSTEM_DRIVE,
    )
});

pub static DRIVE_HEALING_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::Custom("healing".to_string()),
        "Whether the drive is currently healing",
        &DRIVE_TOPOLOGY_LABELS,
        subsystems::SYSTEM_DRIVE,
    )
});

pub static DRIVE_SCANNING_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::Custom("scanning".to_string()),
        "Whether the drive is currently being scanned",
        &DRIVE_TOPOLOGY_LABELS,
        subsystems::SYSTEM_DRIVE,
    )
});

pub static DRIVE_OFFLINE_DURATION_SECONDS_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::Custom("offline_duration_seconds".to_string()),
        "Duration in seconds the drive has been offline",
        &DRIVE_TOPOLOGY_LABELS,
        subsystems::SYSTEM_DRIVE,
    )
});

pub static DRIVE_API_CALLS_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::Custom("api_calls_total".to_string()),
        "Total drive API calls by operation",
        &DRIVE_TOPOLOGY_API_LABELS,
        subsystems::SYSTEM_DRIVE,
    )
});

pub static DRIVE_API_LATENCY_BY_API_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::Custom("api_latency_by_api_micros".to_string()),
        "Average last minute drive API latency in microseconds by operation",
        &DRIVE_TOPOLOGY_API_LABELS,
        subsystems::SYSTEM_DRIVE,
    )
});

pub static DRIVE_USED_BYTES_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::DriveUsedBytes,
        "Total storage used on a drive in bytes",
        &ALL_DRIVE_LABELS[..],
        subsystems::SYSTEM_DRIVE,
    )
});

pub static DRIVE_FREE_BYTES_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::DriveFreeBytes,
        "Total storage free on a drive in bytes",
        &ALL_DRIVE_LABELS[..],
        subsystems::SYSTEM_DRIVE,
    )
});

pub static DRIVE_TOTAL_BYTES_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::DriveTotalBytes,
        "Total storage available on a drive in bytes",
        &ALL_DRIVE_LABELS[..],
        subsystems::SYSTEM_DRIVE,
    )
});

pub static DRIVE_CAPACITY_OBSERVATION_STATE_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::Custom("capacity_observation_state".to_string()),
        "Drive capacity observation state (1 for the active state label, 0 otherwise). States: live, stale, missing",
        &[&ALL_DRIVE_LABELS[..], &["state"]].concat(),
        subsystems::SYSTEM_DRIVE,
    )
});

pub static DRIVE_CAPACITY_OBSERVATION_AGE_SECONDS_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::Custom("capacity_observation_age_seconds".to_string()),
        "Age in seconds of the drive capacity observation currently exported",
        &ALL_DRIVE_LABELS[..],
        subsystems::SYSTEM_DRIVE,
    )
});

pub static DRIVE_USED_INODES_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::DriveUsedInodes,
        "Total used inodes on a drive",
        &ALL_DRIVE_LABELS[..],
        subsystems::SYSTEM_DRIVE,
    )
});

pub static DRIVE_FREE_INODES_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::DriveFreeInodes,
        "Total free inodes on a drive",
        &ALL_DRIVE_LABELS[..],
        subsystems::SYSTEM_DRIVE,
    )
});

pub static DRIVE_TOTAL_INODES_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::DriveTotalInodes,
        "Total inodes available on a drive",
        &ALL_DRIVE_LABELS[..],
        subsystems::SYSTEM_DRIVE,
    )
});

pub static DRIVE_TIMEOUT_ERRORS_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::DriveTimeoutErrorsTotal,
        "Total timeout errors on a drive",
        &ALL_DRIVE_LABELS[..],
        subsystems::SYSTEM_DRIVE,
    )
});

pub static DRIVE_IO_ERRORS_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::DriveIOErrorsTotal,
        "Total I/O errors on a drive",
        &ALL_DRIVE_LABELS[..],
        subsystems::SYSTEM_DRIVE,
    )
});

pub static DRIVE_AVAILABILITY_ERRORS_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::DriveAvailabilityErrorsTotal,
        "Total availability errors (I/O errors, timeouts) on a drive",
        &ALL_DRIVE_LABELS[..],
        subsystems::SYSTEM_DRIVE,
    )
});

pub static DRIVE_WAITING_IO_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::DriveWaitingIO,
        "Total waiting I/O operations on a drive",
        &ALL_DRIVE_LABELS[..],
        subsystems::SYSTEM_DRIVE,
    )
});

pub static DRIVE_API_LATENCY_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::DriveAPILatencyMicros,
        "Average last minute latency in µs for drive API storage operations",
        &ALL_DRIVE_LABELS[..],
        subsystems::SYSTEM_DRIVE,
    )
});

pub static DRIVE_HEALTH_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::DriveHealth,
        "Drive health (0 = offline, 1 = healthy, 2 = healing)",
        &ALL_DRIVE_LABELS[..],
        subsystems::SYSTEM_DRIVE,
    )
});

pub static DRIVE_WRITES_TOTAL_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::DriveWritesTotal,
        "Total successful write operations on a drive",
        &ALL_DRIVE_LABELS[..],
        subsystems::SYSTEM_DRIVE,
    )
});

pub static DRIVE_DELETES_TOTAL_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::DriveDeletesTotal,
        "Total successful delete operations on a drive",
        &ALL_DRIVE_LABELS[..],
        subsystems::SYSTEM_DRIVE,
    )
});

pub static DRIVE_OFFLINE_COUNT_MD: LazyLock<MetricDescriptor> =
    LazyLock::new(|| new_gauge_md(MetricName::DriveOfflineCount, "Count of offline drives", &[], subsystems::SYSTEM_DRIVE));

pub static DRIVE_ONLINE_COUNT_MD: LazyLock<MetricDescriptor> =
    LazyLock::new(|| new_gauge_md(MetricName::DriveOnlineCount, "Count of online drives", &[], subsystems::SYSTEM_DRIVE));

pub static DRIVE_COUNT_MD: LazyLock<MetricDescriptor> =
    LazyLock::new(|| new_gauge_md(MetricName::DriveCount, "Count of all drives", &[], subsystems::SYSTEM_DRIVE));

pub static DRIVE_READS_PER_SEC_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::DriveReadsPerSec,
        "Reads per second on a drive",
        &ALL_DRIVE_LABELS[..],
        subsystems::SYSTEM_DRIVE,
    )
});

pub static DRIVE_READS_KB_PER_SEC_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::DriveReadsKBPerSec,
        "Kilobytes read per second on a drive",
        &ALL_DRIVE_LABELS[..],
        subsystems::SYSTEM_DRIVE,
    )
});

pub static DRIVE_READS_AWAIT_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::DriveReadsAwait,
        "Average time for read requests served on a drive",
        &ALL_DRIVE_LABELS[..],
        subsystems::SYSTEM_DRIVE,
    )
});

pub static DRIVE_WRITES_PER_SEC_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::DriveWritesPerSec,
        "Writes per second on a drive",
        &ALL_DRIVE_LABELS[..],
        subsystems::SYSTEM_DRIVE,
    )
});

pub static DRIVE_WRITES_KB_PER_SEC_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::DriveWritesKBPerSec,
        "Kilobytes written per second on a drive",
        &ALL_DRIVE_LABELS[..],
        subsystems::SYSTEM_DRIVE,
    )
});

pub static DRIVE_WRITES_AWAIT_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::DriveWritesAwait,
        "Average time for write requests served on a drive",
        &ALL_DRIVE_LABELS[..],
        subsystems::SYSTEM_DRIVE,
    )
});

pub static DRIVE_PERC_UTIL_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::DrivePercUtil,
        "Percentage of time the disk was busy",
        &ALL_DRIVE_LABELS[..],
        subsystems::SYSTEM_DRIVE,
    )
});
