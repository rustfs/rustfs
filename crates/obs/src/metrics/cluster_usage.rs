/// Descriptors of metrics related to cluster object and bucket usage
use crate::metrics::{MetricDescriptor, MetricName, new_gauge_md, subsystems};

/// Bucket labels
pub const BUCKET_LABEL: &str = "bucket";
/// Range labels
pub const RANGE_LABEL: &str = "range";

lazy_static::lazy_static! {
    pub static ref USAGE_SINCE_LAST_UPDATE_SECONDS_MD: MetricDescriptor =
        new_gauge_md(
            MetricName::UsageSinceLastUpdateSeconds,
            "Time since last update of usage metrics in seconds",
            &[],
            subsystems::CLUSTER_USAGE_OBJECTS
        );

    pub static ref USAGE_TOTAL_BYTES_MD: MetricDescriptor =
        new_gauge_md(
            MetricName::UsageTotalBytes,
            "Total cluster usage in bytes",
            &[],
            subsystems::CLUSTER_USAGE_OBJECTS
        );

    pub static ref USAGE_OBJECTS_COUNT_MD: MetricDescriptor =
        new_gauge_md(
            MetricName::UsageObjectsCount,
            "Total cluster objects count",
            &[],
            subsystems::CLUSTER_USAGE_OBJECTS
        );

    pub static ref USAGE_VERSIONS_COUNT_MD: MetricDescriptor =
        new_gauge_md(
            MetricName::UsageVersionsCount,
            "Total cluster object versions (including delete markers) count",
            &[],
            subsystems::CLUSTER_USAGE_OBJECTS
        );

    pub static ref USAGE_DELETE_MARKERS_COUNT_MD: MetricDescriptor =
        new_gauge_md(
            MetricName::UsageDeleteMarkersCount,
            "Total cluster delete markers count",
            &[],
            subsystems::CLUSTER_USAGE_OBJECTS
        );

    pub static ref USAGE_BUCKETS_COUNT_MD: MetricDescriptor =
        new_gauge_md(
            MetricName::UsageBucketsCount,
            "Total cluster buckets count",
            &[],
            subsystems::CLUSTER_USAGE_OBJECTS
        );

    pub static ref USAGE_OBJECTS_DISTRIBUTION_MD: MetricDescriptor =
        new_gauge_md(
            MetricName::UsageSizeDistribution,
            "Cluster object size distribution",
            &[RANGE_LABEL],
            subsystems::CLUSTER_USAGE_OBJECTS
        );

    pub static ref USAGE_VERSIONS_DISTRIBUTION_MD: MetricDescriptor =
        new_gauge_md(
            MetricName::UsageVersionCountDistribution,
            "Cluster object version count distribution",
            &[RANGE_LABEL],
            subsystems::CLUSTER_USAGE_OBJECTS
        );
}

lazy_static::lazy_static! {
    pub static ref USAGE_BUCKET_TOTAL_BYTES_MD: MetricDescriptor =
        new_gauge_md(
            MetricName::UsageBucketTotalBytes,
            "Total bucket size in bytes",
            &[BUCKET_LABEL],
            subsystems::CLUSTER_USAGE_BUCKETS
        );

    pub static ref USAGE_BUCKET_OBJECTS_TOTAL_MD: MetricDescriptor =
        new_gauge_md(
            MetricName::UsageBucketObjectsCount,
            "Total objects count in bucket",
            &[BUCKET_LABEL],
            subsystems::CLUSTER_USAGE_BUCKETS
        );

    pub static ref USAGE_BUCKET_VERSIONS_COUNT_MD: MetricDescriptor =
        new_gauge_md(
            MetricName::UsageBucketVersionsCount,
            "Total object versions (including delete markers) count in bucket",
            &[BUCKET_LABEL],
            subsystems::CLUSTER_USAGE_BUCKETS
        );

    pub static ref USAGE_BUCKET_DELETE_MARKERS_COUNT_MD: MetricDescriptor =
        new_gauge_md(
            MetricName::UsageBucketDeleteMarkersCount,
            "Total delete markers count in bucket",
            &[BUCKET_LABEL],
            subsystems::CLUSTER_USAGE_BUCKETS
        );

    pub static ref USAGE_BUCKET_QUOTA_TOTAL_BYTES_MD: MetricDescriptor =
        new_gauge_md(
            MetricName::UsageBucketQuotaTotalBytes,
            "Total bucket quota in bytes",
            &[BUCKET_LABEL],
            subsystems::CLUSTER_USAGE_BUCKETS
        );

    pub static ref USAGE_BUCKET_OBJECT_SIZE_DISTRIBUTION_MD: MetricDescriptor =
        new_gauge_md(
            MetricName::UsageBucketObjectSizeDistribution,
            "Bucket object size distribution",
            &[RANGE_LABEL, BUCKET_LABEL],
            subsystems::CLUSTER_USAGE_BUCKETS
        );

    pub static ref USAGE_BUCKET_OBJECT_VERSION_COUNT_DISTRIBUTION_MD: MetricDescriptor =
        new_gauge_md(
            MetricName::UsageBucketObjectVersionCountDistribution,
            "Bucket object version count distribution",
            &[RANGE_LABEL, BUCKET_LABEL],
            subsystems::CLUSTER_USAGE_BUCKETS
        );
}
