/// Copy the relevant metric descriptor
use crate::metrics::{new_gauge_md, subsystems, MetricDescriptor, MetricName};

lazy_static::lazy_static! {
    pub static ref REPLICATION_AVERAGE_ACTIVE_WORKERS_MD: MetricDescriptor =
        new_gauge_md(
            MetricName::ReplicationAverageActiveWorkers,
            "Average number of active replication workers",
            &[],
            subsystems::REPLICATION
        );

    pub static ref REPLICATION_AVERAGE_QUEUED_BYTES_MD: MetricDescriptor =
        new_gauge_md(
            MetricName::ReplicationAverageQueuedBytes,
            "Average number of bytes queued for replication since server start",
            &[],
            subsystems::REPLICATION
        );

    pub static ref REPLICATION_AVERAGE_QUEUED_COUNT_MD: MetricDescriptor =
        new_gauge_md(
            MetricName::ReplicationAverageQueuedCount,
            "Average number of objects queued for replication since server start",
            &[],
            subsystems::REPLICATION
        );

    pub static ref REPLICATION_AVERAGE_DATA_TRANSFER_RATE_MD: MetricDescriptor =
        new_gauge_md(
            MetricName::ReplicationAverageDataTransferRate,
            "Average replication data transfer rate in bytes/sec",
            &[],
            subsystems::REPLICATION
        );

    pub static ref REPLICATION_CURRENT_ACTIVE_WORKERS_MD: MetricDescriptor =
        new_gauge_md(
            MetricName::ReplicationCurrentActiveWorkers,
            "Total number of active replication workers",
            &[],
            subsystems::REPLICATION
        );

    pub static ref REPLICATION_CURRENT_DATA_TRANSFER_RATE_MD: MetricDescriptor =
        new_gauge_md(
            MetricName::ReplicationCurrentDataTransferRate,
            "Current replication data transfer rate in bytes/sec",
            &[],
            subsystems::REPLICATION
        );

    pub static ref REPLICATION_LAST_MINUTE_QUEUED_BYTES_MD: MetricDescriptor =
        new_gauge_md(
            MetricName::ReplicationLastMinuteQueuedBytes,
            "Number of bytes queued for replication in the last full minute",
            &[],
            subsystems::REPLICATION
        );

    pub static ref REPLICATION_LAST_MINUTE_QUEUED_COUNT_MD: MetricDescriptor =
        new_gauge_md(
            MetricName::ReplicationLastMinuteQueuedCount,
            "Number of objects queued for replication in the last full minute",
            &[],
            subsystems::REPLICATION
        );

    pub static ref REPLICATION_MAX_ACTIVE_WORKERS_MD: MetricDescriptor =
        new_gauge_md(
            MetricName::ReplicationMaxActiveWorkers,
            "Maximum number of active replication workers seen since server start",
            &[],
            subsystems::REPLICATION
        );

    pub static ref REPLICATION_MAX_QUEUED_BYTES_MD: MetricDescriptor =
        new_gauge_md(
            MetricName::ReplicationMaxQueuedBytes,
            "Maximum number of bytes queued for replication since server start",
            &[],
            subsystems::REPLICATION
        );

    pub static ref REPLICATION_MAX_QUEUED_COUNT_MD: MetricDescriptor =
        new_gauge_md(
            MetricName::ReplicationMaxQueuedCount,
            "Maximum number of objects queued for replication since server start",
            &[],
            subsystems::REPLICATION
        );

    pub static ref REPLICATION_MAX_DATA_TRANSFER_RATE_MD: MetricDescriptor =
        new_gauge_md(
            MetricName::ReplicationMaxDataTransferRate,
            "Maximum replication data transfer rate in bytes/sec seen since server start",
            &[],
            subsystems::REPLICATION
        );

    pub static ref REPLICATION_RECENT_BACKLOG_COUNT_MD: MetricDescriptor =
        new_gauge_md(
            MetricName::ReplicationRecentBacklogCount,
            "Total number of objects seen in replication backlog in the last 5 minutes",
            &[],
            subsystems::REPLICATION
        );
}
