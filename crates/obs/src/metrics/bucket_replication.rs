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

/// Bucket copy metric descriptor
use crate::metrics::{MetricDescriptor, MetricName, new_counter_md, new_gauge_md, subsystems};
use std::sync::LazyLock;

/// Bucket level replication metric descriptor
pub const BUCKET_L: &str = "bucket";
/// Replication operation
pub const OPERATION_L: &str = "operation";
/// Replication target ARN
pub const TARGET_ARN_L: &str = "targetArn";
/// Replication range
pub const RANGE_L: &str = "range";

pub static BUCKET_REPL_LAST_HR_FAILED_BYTES_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::LastHourFailedBytes,
        "Total number of bytes failed at least once to replicate in the last hour on a bucket",
        &[BUCKET_L],
        subsystems::BUCKET_REPLICATION,
    )
});

pub static BUCKET_REPL_LAST_HR_FAILED_COUNT_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::LastHourFailedCount,
        "Total number of objects which failed replication in the last hour on a bucket",
        &[BUCKET_L],
        subsystems::BUCKET_REPLICATION,
    )
});

pub static BUCKET_REPL_LAST_MIN_FAILED_BYTES_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::LastMinFailedBytes,
        "Total number of bytes failed at least once to replicate in the last full minute on a bucket",
        &[BUCKET_L],
        subsystems::BUCKET_REPLICATION,
    )
});

pub static BUCKET_REPL_LAST_MIN_FAILED_COUNT_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::LastMinFailedCount,
        "Total number of objects which failed replication in the last full minute on a bucket",
        &[BUCKET_L],
        subsystems::BUCKET_REPLICATION,
    )
});

pub static BUCKET_REPL_LATENCY_MS_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::LatencyMilliSec,
        "Replication latency on a bucket in milliseconds",
        &[BUCKET_L, OPERATION_L, RANGE_L, TARGET_ARN_L],
        subsystems::BUCKET_REPLICATION,
    )
});

pub static BUCKET_REPL_PROXIED_DELETE_TAGGING_REQUESTS_TOTAL_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::ProxiedDeleteTaggingRequestsTotal,
        "Number of DELETE tagging requests proxied to replication target",
        &[BUCKET_L],
        subsystems::BUCKET_REPLICATION,
    )
});

pub static BUCKET_REPL_PROXIED_GET_REQUESTS_FAILURES_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::ProxiedGetRequestsFailures,
        "Number of failures in GET requests proxied to replication target",
        &[BUCKET_L],
        subsystems::BUCKET_REPLICATION,
    )
});

pub static BUCKET_REPL_PROXIED_GET_REQUESTS_TOTAL_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::ProxiedGetRequestsTotal,
        "Number of GET requests proxied to replication target",
        &[BUCKET_L],
        subsystems::BUCKET_REPLICATION,
    )
});

// TODO - add a metric for the number of PUT requests proxied to replication target
pub static BUCKET_REPL_PROXIED_GET_TAGGING_REQUESTS_FAILURES_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::ProxiedGetTaggingRequestFailures,
        "Number of failures in GET tagging requests proxied to replication target",
        &[BUCKET_L],
        subsystems::BUCKET_REPLICATION,
    )
});

pub static BUCKET_REPL_PROXIED_GET_TAGGING_REQUESTS_TOTAL_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::ProxiedGetTaggingRequestsTotal,
        "Number of GET tagging requests proxied to replication target",
        &[BUCKET_L],
        subsystems::BUCKET_REPLICATION,
    )
});

pub static BUCKET_REPL_PROXIED_HEAD_REQUESTS_FAILURES_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::ProxiedHeadRequestsFailures,
        "Number of failures in HEAD requests proxied to replication target",
        &[BUCKET_L],
        subsystems::BUCKET_REPLICATION,
    )
});

pub static BUCKET_REPL_PROXIED_HEAD_REQUESTS_TOTAL_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::ProxiedHeadRequestsTotal,
        "Number of HEAD requests proxied to replication target",
        &[BUCKET_L],
        subsystems::BUCKET_REPLICATION,
    )
});

// TODO - add a metric for the number of PUT requests proxied to replication target
pub static BUCKET_REPL_PROXIED_PUT_TAGGING_REQUESTS_FAILURES_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::ProxiedPutTaggingRequestFailures,
        "Number of failures in PUT tagging requests proxied to replication target",
        &[BUCKET_L],
        subsystems::BUCKET_REPLICATION,
    )
});

pub static BUCKET_REPL_PROXIED_PUT_TAGGING_REQUESTS_TOTAL_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::ProxiedPutTaggingRequestsTotal,
        "Number of PUT tagging requests proxied to replication target",
        &[BUCKET_L],
        subsystems::BUCKET_REPLICATION,
    )
});

pub static BUCKET_REPL_SENT_BYTES_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::SentBytes,
        "Total number of bytes replicated to the target",
        &[BUCKET_L],
        subsystems::BUCKET_REPLICATION,
    )
});

pub static BUCKET_REPL_SENT_COUNT_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::SentCount,
        "Total number of objects replicated to the target",
        &[BUCKET_L],
        subsystems::BUCKET_REPLICATION,
    )
});

pub static BUCKET_REPL_TOTAL_FAILED_BYTES_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::TotalFailedBytes,
        "Total number of bytes failed at least once to replicate since server start",
        &[BUCKET_L],
        subsystems::BUCKET_REPLICATION,
    )
});

pub static BUCKET_REPL_TOTAL_FAILED_COUNT_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::TotalFailedCount,
        "Total number of objects which failed replication since server start",
        &[BUCKET_L],
        subsystems::BUCKET_REPLICATION,
    )
});

// TODO - add a metric for the number of DELETE requests proxied to replication target
pub static BUCKET_REPL_PROXIED_DELETE_TAGGING_REQUESTS_FAILURES_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::ProxiedDeleteTaggingRequestFailures,
        "Number of failures in DELETE tagging requests proxied to replication target",
        &[BUCKET_L],
        subsystems::BUCKET_REPLICATION,
    )
});
