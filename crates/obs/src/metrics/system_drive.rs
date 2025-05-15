use crate::metrics::{new_counter_md, new_gauge_md, subsystems, MetricDescriptor, MetricName};

/// 定义标签常量
pub const DRIVE_LABEL: &str = "drive";
pub const POOL_INDEX_LABEL: &str = "pool_index";
pub const SET_INDEX_LABEL: &str = "set_index";
pub const DRIVE_INDEX_LABEL: &str = "drive_index";
pub const API_LABEL: &str = "api";

/// 所有驱动器相关的标签
lazy_static::lazy_static! {
    static ref ALL_DRIVE_LABELS: [&'static str; 4] = [DRIVE_LABEL, POOL_INDEX_LABEL, SET_INDEX_LABEL, DRIVE_INDEX_LABEL];
}

/// 驱动器相关指标描述符
lazy_static::lazy_static! {
    pub static ref DRIVE_USED_BYTES_MD: MetricDescriptor =
        new_gauge_md(
            MetricName::DriveUsedBytes,
            "Total storage used on a drive in bytes",
            &ALL_DRIVE_LABELS[..],
            subsystems::SYSTEM_DRIVE
        );

    pub static ref DRIVE_FREE_BYTES_MD: MetricDescriptor =
        new_gauge_md(
            MetricName::DriveFreeBytes,
            "Total storage free on a drive in bytes",
            &ALL_DRIVE_LABELS[..],
            subsystems::SYSTEM_DRIVE
        );

    pub static ref DRIVE_TOTAL_BYTES_MD: MetricDescriptor =
        new_gauge_md(
            MetricName::DriveTotalBytes,
            "Total storage available on a drive in bytes",
            &ALL_DRIVE_LABELS[..],
            subsystems::SYSTEM_DRIVE
        );

    pub static ref DRIVE_USED_INODES_MD: MetricDescriptor =
        new_gauge_md(
            MetricName::DriveUsedInodes,
            "Total used inodes on a drive",
            &ALL_DRIVE_LABELS[..],
            subsystems::SYSTEM_DRIVE
        );

    pub static ref DRIVE_FREE_INODES_MD: MetricDescriptor =
        new_gauge_md(
            MetricName::DriveFreeInodes,
            "Total free inodes on a drive",
            &ALL_DRIVE_LABELS[..],
            subsystems::SYSTEM_DRIVE
        );

    pub static ref DRIVE_TOTAL_INODES_MD: MetricDescriptor =
        new_gauge_md(
            MetricName::DriveTotalInodes,
            "Total inodes available on a drive",
            &ALL_DRIVE_LABELS[..],
            subsystems::SYSTEM_DRIVE
        );

    pub static ref DRIVE_TIMEOUT_ERRORS_MD: MetricDescriptor =
        new_counter_md(
            MetricName::DriveTimeoutErrorsTotal,
            "Total timeout errors on a drive",
            &ALL_DRIVE_LABELS[..],
            subsystems::SYSTEM_DRIVE
        );

    pub static ref DRIVE_IO_ERRORS_MD: MetricDescriptor =
        new_counter_md(
            MetricName::DriveIOErrorsTotal,
            "Total I/O errors on a drive",
            &ALL_DRIVE_LABELS[..],
            subsystems::SYSTEM_DRIVE
        );

    pub static ref DRIVE_AVAILABILITY_ERRORS_MD: MetricDescriptor =
        new_counter_md(
            MetricName::DriveAvailabilityErrorsTotal,
            "Total availability errors (I/O errors, timeouts) on a drive",
            &ALL_DRIVE_LABELS[..],
            subsystems::SYSTEM_DRIVE
        );

    pub static ref DRIVE_WAITING_IO_MD: MetricDescriptor =
        new_gauge_md(
            MetricName::DriveWaitingIO,
            "Total waiting I/O operations on a drive",
            &ALL_DRIVE_LABELS[..],
            subsystems::SYSTEM_DRIVE
        );

    pub static ref DRIVE_API_LATENCY_MD: MetricDescriptor =
        new_gauge_md(
            MetricName::DriveAPILatencyMicros,
            "Average last minute latency in µs for drive API storage operations",
            &[&ALL_DRIVE_LABELS[..], &[API_LABEL]].concat(),
            subsystems::SYSTEM_DRIVE
        );

    pub static ref DRIVE_HEALTH_MD: MetricDescriptor =
        new_gauge_md(
            MetricName::DriveHealth,
            "Drive health (0 = offline, 1 = healthy, 2 = healing)",
            &ALL_DRIVE_LABELS[..],
            subsystems::SYSTEM_DRIVE
        );

    pub static ref DRIVE_OFFLINE_COUNT_MD: MetricDescriptor =
        new_gauge_md(
            MetricName::DriveOfflineCount,
            "Count of offline drives",
            &[],
            subsystems::SYSTEM_DRIVE
        );

    pub static ref DRIVE_ONLINE_COUNT_MD: MetricDescriptor =
        new_gauge_md(
            MetricName::DriveOnlineCount,
            "Count of online drives",
            &[],
            subsystems::SYSTEM_DRIVE
        );

    pub static ref DRIVE_COUNT_MD: MetricDescriptor =
        new_gauge_md(
            MetricName::DriveCount,
            "Count of all drives",
            &[],
            subsystems::SYSTEM_DRIVE
        );

    pub static ref DRIVE_READS_PER_SEC_MD: MetricDescriptor =
        new_gauge_md(
            MetricName::DriveReadsPerSec,
            "Reads per second on a drive",
            &ALL_DRIVE_LABELS[..],
            subsystems::SYSTEM_DRIVE
        );

    pub static ref DRIVE_READS_KB_PER_SEC_MD: MetricDescriptor =
        new_gauge_md(
            MetricName::DriveReadsKBPerSec,
            "Kilobytes read per second on a drive",
            &ALL_DRIVE_LABELS[..],
            subsystems::SYSTEM_DRIVE
        );

    pub static ref DRIVE_READS_AWAIT_MD: MetricDescriptor =
        new_gauge_md(
            MetricName::DriveReadsAwait,
            "Average time for read requests served on a drive",
            &ALL_DRIVE_LABELS[..],
            subsystems::SYSTEM_DRIVE
        );

    pub static ref DRIVE_WRITES_PER_SEC_MD: MetricDescriptor =
        new_gauge_md(
            MetricName::DriveWritesPerSec,
            "Writes per second on a drive",
            &ALL_DRIVE_LABELS[..],
            subsystems::SYSTEM_DRIVE
        );

    pub static ref DRIVE_WRITES_KB_PER_SEC_MD: MetricDescriptor =
        new_gauge_md(
            MetricName::DriveWritesKBPerSec,
            "Kilobytes written per second on a drive",
            &ALL_DRIVE_LABELS[..],
            subsystems::SYSTEM_DRIVE
        );

    pub static ref DRIVE_WRITES_AWAIT_MD: MetricDescriptor =
        new_gauge_md(
            MetricName::DriveWritesAwait,
            "Average time for write requests served on a drive",
            &ALL_DRIVE_LABELS[..],
            subsystems::SYSTEM_DRIVE
        );

    pub static ref DRIVE_PERC_UTIL_MD: MetricDescriptor =
        new_gauge_md(
            MetricName::DrivePercUtil,
            "Percentage of time the disk was busy",
            &ALL_DRIVE_LABELS[..],
            subsystems::SYSTEM_DRIVE
        );
}
