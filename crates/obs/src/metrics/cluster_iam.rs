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

/// IAM related metric descriptors
use crate::metrics::{MetricDescriptor, MetricName, new_counter_md, subsystems};
use std::sync::LazyLock;

pub static LAST_SYNC_DURATION_MILLIS_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::LastSyncDurationMillis,
        "Last successful IAM data sync duration in milliseconds",
        &[],
        subsystems::CLUSTER_IAM,
    )
});

pub static PLUGIN_AUTHN_SERVICE_FAILED_REQUESTS_MINUTE_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::PluginAuthnServiceFailedRequestsMinute,
        "When plugin authentication is configured, returns failed requests count in the last full minute",
        &[],
        subsystems::CLUSTER_IAM,
    )
});

pub static PLUGIN_AUTHN_SERVICE_LAST_FAIL_SECONDS_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::PluginAuthnServiceLastFailSeconds,
        "When plugin authentication is configured, returns time (in seconds) since the last failed request to the service",
        &[],
        subsystems::CLUSTER_IAM,
    )
});

pub static PLUGIN_AUTHN_SERVICE_LAST_SUCC_SECONDS_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::PluginAuthnServiceLastSuccSeconds,
        "When plugin authentication is configured, returns time (in seconds) since the last successful request to the service",
        &[],
        subsystems::CLUSTER_IAM,
    )
});

pub static PLUGIN_AUTHN_SERVICE_SUCC_AVG_RTT_MS_MINUTE_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::PluginAuthnServiceSuccAvgRttMsMinute,
        "When plugin authentication is configured, returns average round-trip-time of successful requests in the last full minute",
        &[],
        subsystems::CLUSTER_IAM,
    )
});

pub static PLUGIN_AUTHN_SERVICE_SUCC_MAX_RTT_MS_MINUTE_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::PluginAuthnServiceSuccMaxRttMsMinute,
        "When plugin authentication is configured, returns maximum round-trip-time of successful requests in the last full minute",
        &[],
        subsystems::CLUSTER_IAM,
    )
});

pub static PLUGIN_AUTHN_SERVICE_TOTAL_REQUESTS_MINUTE_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::PluginAuthnServiceTotalRequestsMinute,
        "When plugin authentication is configured, returns total requests count in the last full minute",
        &[],
        subsystems::CLUSTER_IAM,
    )
});

pub static SINCE_LAST_SYNC_MILLIS_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::SinceLastSyncMillis,
        "Time (in milliseconds) since last successful IAM data sync.",
        &[],
        subsystems::CLUSTER_IAM,
    )
});

pub static SYNC_FAILURES_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::SyncFailures,
        "Number of failed IAM data syncs since server start.",
        &[],
        subsystems::CLUSTER_IAM,
    )
});

pub static SYNC_SUCCESSES_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::SyncSuccesses,
        "Number of successful IAM data syncs since server start.",
        &[],
        subsystems::CLUSTER_IAM,
    )
});
