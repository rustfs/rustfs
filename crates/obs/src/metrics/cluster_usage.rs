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

/// Descriptors of metrics related to cluster object and bucket usage
use crate::metrics::{MetricDescriptor, MetricName, new_gauge_md, subsystems};
use std::sync::LazyLock;

/// Bucket labels
pub const BUCKET_LABEL: &str = "bucket";
/// Range labels
pub const RANGE_LABEL: &str = "range";

pub static USAGE_SINCE_LAST_UPDATE_SECONDS_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::UsageSinceLastUpdateSeconds,
        "Time since last update of usage metrics in seconds",
        &[],
        subsystems::CLUSTER_USAGE_OBJECTS,
    )
});

pub static USAGE_TOTAL_BYTES_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::UsageTotalBytes,
        "Total cluster usage in bytes",
        &[],
        subsystems::CLUSTER_USAGE_OBJECTS,
    )
});

pub static USAGE_OBJECTS_COUNT_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::UsageObjectsCount,
        "Total cluster objects count",
        &[],
        subsystems::CLUSTER_USAGE_OBJECTS,
    )
});

pub static USAGE_VERSIONS_COUNT_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::UsageVersionsCount,
        "Total cluster object versions (including delete markers) count",
        &[],
        subsystems::CLUSTER_USAGE_OBJECTS,
    )
});

pub static USAGE_DELETE_MARKERS_COUNT_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::UsageDeleteMarkersCount,
        "Total cluster delete markers count",
        &[],
        subsystems::CLUSTER_USAGE_OBJECTS,
    )
});

pub static USAGE_BUCKETS_COUNT_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::UsageBucketsCount,
        "Total cluster buckets count",
        &[],
        subsystems::CLUSTER_USAGE_OBJECTS,
    )
});

pub static USAGE_OBJECTS_DISTRIBUTION_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::UsageSizeDistribution,
        "Cluster object size distribution",
        &[RANGE_LABEL],
        subsystems::CLUSTER_USAGE_OBJECTS,
    )
});

pub static USAGE_VERSIONS_DISTRIBUTION_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::UsageVersionCountDistribution,
        "Cluster object version count distribution",
        &[RANGE_LABEL],
        subsystems::CLUSTER_USAGE_OBJECTS,
    )
});

pub static USAGE_BUCKET_TOTAL_BYTES_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::UsageBucketTotalBytes,
        "Total bucket size in bytes",
        &[BUCKET_LABEL],
        subsystems::CLUSTER_USAGE_BUCKETS,
    )
});

pub static USAGE_BUCKET_OBJECTS_TOTAL_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::UsageBucketObjectsCount,
        "Total objects count in bucket",
        &[BUCKET_LABEL],
        subsystems::CLUSTER_USAGE_BUCKETS,
    )
});

pub static USAGE_BUCKET_VERSIONS_COUNT_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::UsageBucketVersionsCount,
        "Total object versions (including delete markers) count in bucket",
        &[BUCKET_LABEL],
        subsystems::CLUSTER_USAGE_BUCKETS,
    )
});

pub static USAGE_BUCKET_DELETE_MARKERS_COUNT_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::UsageBucketDeleteMarkersCount,
        "Total delete markers count in bucket",
        &[BUCKET_LABEL],
        subsystems::CLUSTER_USAGE_BUCKETS,
    )
});

pub static USAGE_BUCKET_QUOTA_TOTAL_BYTES_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::UsageBucketQuotaTotalBytes,
        "Total bucket quota in bytes",
        &[BUCKET_LABEL],
        subsystems::CLUSTER_USAGE_BUCKETS,
    )
});

pub static USAGE_BUCKET_OBJECT_SIZE_DISTRIBUTION_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::UsageBucketObjectSizeDistribution,
        "Bucket object size distribution",
        &[RANGE_LABEL, BUCKET_LABEL],
        subsystems::CLUSTER_USAGE_BUCKETS,
    )
});

pub static USAGE_BUCKET_OBJECT_VERSION_COUNT_DISTRIBUTION_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::UsageBucketObjectVersionCountDistribution,
        "Bucket object version count distribution",
        &[RANGE_LABEL, BUCKET_LABEL],
        subsystems::CLUSTER_USAGE_BUCKETS,
    )
});
