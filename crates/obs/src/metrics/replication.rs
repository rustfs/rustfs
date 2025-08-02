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

/// Metrics for replication subsystem
use crate::metrics::{MetricDescriptor, MetricName, new_gauge_md, subsystems};
use std::sync::LazyLock;

pub static REPLICATION_AVERAGE_ACTIVE_WORKERS_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::ReplicationAverageActiveWorkers,
        "Average number of active replication workers",
        &[],
        subsystems::REPLICATION,
    )
});

pub static REPLICATION_AVERAGE_QUEUED_BYTES_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::ReplicationAverageQueuedBytes,
        "Average number of bytes queued for replication since server start",
        &[],
        subsystems::REPLICATION,
    )
});

pub static REPLICATION_AVERAGE_QUEUED_COUNT_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::ReplicationAverageQueuedCount,
        "Average number of objects queued for replication since server start",
        &[],
        subsystems::REPLICATION,
    )
});

pub static REPLICATION_AVERAGE_DATA_TRANSFER_RATE_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::ReplicationAverageDataTransferRate,
        "Average replication data transfer rate in bytes/sec",
        &[],
        subsystems::REPLICATION,
    )
});

pub static REPLICATION_CURRENT_ACTIVE_WORKERS_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::ReplicationCurrentActiveWorkers,
        "Total number of active replication workers",
        &[],
        subsystems::REPLICATION,
    )
});

pub static REPLICATION_CURRENT_DATA_TRANSFER_RATE_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::ReplicationCurrentDataTransferRate,
        "Current replication data transfer rate in bytes/sec",
        &[],
        subsystems::REPLICATION,
    )
});

pub static REPLICATION_LAST_MINUTE_QUEUED_BYTES_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::ReplicationLastMinuteQueuedBytes,
        "Number of bytes queued for replication in the last full minute",
        &[],
        subsystems::REPLICATION,
    )
});

pub static REPLICATION_LAST_MINUTE_QUEUED_COUNT_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::ReplicationLastMinuteQueuedCount,
        "Number of objects queued for replication in the last full minute",
        &[],
        subsystems::REPLICATION,
    )
});

pub static REPLICATION_MAX_ACTIVE_WORKERS_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::ReplicationMaxActiveWorkers,
        "Maximum number of active replication workers seen since server start",
        &[],
        subsystems::REPLICATION,
    )
});

pub static REPLICATION_MAX_QUEUED_BYTES_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::ReplicationMaxQueuedBytes,
        "Maximum number of bytes queued for replication since server start",
        &[],
        subsystems::REPLICATION,
    )
});

pub static REPLICATION_MAX_QUEUED_COUNT_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::ReplicationMaxQueuedCount,
        "Maximum number of objects queued for replication since server start",
        &[],
        subsystems::REPLICATION,
    )
});

pub static REPLICATION_MAX_DATA_TRANSFER_RATE_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::ReplicationMaxDataTransferRate,
        "Maximum replication data transfer rate in bytes/sec seen since server start",
        &[],
        subsystems::REPLICATION,
    )
});

pub static REPLICATION_RECENT_BACKLOG_COUNT_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::ReplicationRecentBacklogCount,
        "Total number of objects seen in replication backlog in the last 5 minutes",
        &[],
        subsystems::REPLICATION,
    )
});
