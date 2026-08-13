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

use crate::{MetricDescriptor, MetricName, MetricSubsystem, new_counter_md, new_gauge_md, subsystems};
use std::sync::LazyLock;
/// name label
pub const NAME_LABEL: &str = "name";
/// type label
pub const TYPE_LABEL: &str = "type";
/// le label (for histogram buckets)
pub const LE_LABEL: &str = "le";
/// server label
pub const SERVER_LABEL: &str = "server";

const API_NAME_TYPE_LABELS: [&str; 2] = [NAME_LABEL, TYPE_LABEL];
const API_SERVER_NAME_TYPE_LABELS: [&str; 3] = [SERVER_LABEL, NAME_LABEL, TYPE_LABEL];
const API_NAME_TYPE_LE_LABELS: [&str; 3] = [NAME_LABEL, TYPE_LABEL, LE_LABEL];
const API_SERVER_NAME_TYPE_LE_LABELS: [&str; 4] = [SERVER_LABEL, NAME_LABEL, TYPE_LABEL, LE_LABEL];
const API_TYPE_LABELS: [&str; 1] = [TYPE_LABEL];
const API_SERVER_TYPE_LABELS: [&str; 2] = [SERVER_LABEL, TYPE_LABEL];

// Declared for MinIO metric parity but never emitted: no collector passes these
// descriptors to `PrometheusMetric::from_descriptor`, so the wire names
// (`rejected_auth_total`, `rejected_header_total`, `rejected_timestamp_total`,
// `rejected_invalid_total`, `waiting_total`, `incoming_total`) never appear in a
// scrape. Kept so the gap stays greppable rather than silently disappearing with
// their `MetricName` variants; wiring an emitter is what retires these allows.
#[allow(dead_code, reason = "declared metric with no emitter; see note above (backlog#1823)")]
pub static API_REJECTED_AUTH_TOTAL_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::ApiRejectedAuthTotal,
        "Total number of requests rejected for auth failure",
        &["type"],
        subsystems::API_REQUESTS,
    )
});

#[allow(dead_code, reason = "declared metric with no emitter; see note above (backlog#1823)")]
pub static API_REJECTED_HEADER_TOTAL_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::ApiRejectedHeaderTotal,
        "Total number of requests rejected for invalid header",
        &["type"],
        MetricSubsystem::ApiRequests,
    )
});

#[allow(dead_code, reason = "declared metric with no emitter; see note above (backlog#1823)")]
pub static API_REJECTED_TIMESTAMP_TOTAL_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::ApiRejectedTimestampTotal,
        "Total number of requests rejected for invalid timestamp",
        &["type"],
        MetricSubsystem::ApiRequests,
    )
});

#[allow(dead_code, reason = "declared metric with no emitter; see note above (backlog#1823)")]
pub static API_REJECTED_INVALID_TOTAL_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::ApiRejectedInvalidTotal,
        "Total number of invalid requests",
        &["type"],
        MetricSubsystem::ApiRequests,
    )
});

#[allow(dead_code, reason = "declared metric with no emitter; see note above (backlog#1823)")]
pub static API_REQUESTS_WAITING_TOTAL_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::ApiRequestsWaitingTotal,
        "Total number of requests in the waiting queue",
        &["type"],
        MetricSubsystem::ApiRequests,
    )
});

#[allow(dead_code, reason = "declared metric with no emitter; see note above (backlog#1823)")]
pub static API_REQUESTS_INCOMING_TOTAL_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::ApiRequestsIncomingTotal,
        "Total number of incoming requests",
        &["type"],
        MetricSubsystem::ApiRequests,
    )
});

pub static API_REQUESTS_IN_FLIGHT_TOTAL_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::ApiRequestsInFlightTotal,
        "Total number of requests currently in flight",
        &API_NAME_TYPE_LABELS,
        MetricSubsystem::ApiRequests,
    )
});

pub static API_REQUESTS_IN_FLIGHT_TOTAL_BY_SERVER_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::Custom("requests_in_flight_total_by_server".to_string()),
        "Total number of requests currently in flight by server",
        &API_SERVER_NAME_TYPE_LABELS,
        MetricSubsystem::ApiRequests,
    )
});

pub static API_REQUESTS_TOTAL_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::ApiRequestsTotal,
        "Total number of requests",
        &API_NAME_TYPE_LABELS,
        MetricSubsystem::ApiRequests,
    )
});

pub static API_REQUESTS_TOTAL_BY_SERVER_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::Custom("requests_total_by_server".to_string()),
        "Total number of requests by server",
        &API_SERVER_NAME_TYPE_LABELS,
        MetricSubsystem::ApiRequests,
    )
});

pub static API_REQUESTS_ERRORS_TOTAL_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::ApiRequestsErrorsTotal,
        "Total number of requests with (4xx and 5xx) errors",
        &API_NAME_TYPE_LABELS,
        MetricSubsystem::ApiRequests,
    )
});

pub static API_REQUESTS_ERRORS_TOTAL_BY_SERVER_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::Custom("requests_errors_total_by_server".to_string()),
        "Total number of requests with (4xx and 5xx) errors by server",
        &API_SERVER_NAME_TYPE_LABELS,
        MetricSubsystem::ApiRequests,
    )
});

pub static API_REQUESTS_5XX_ERRORS_TOTAL_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::ApiRequests5xxErrorsTotal,
        "Total number of requests with 5xx errors",
        &API_NAME_TYPE_LABELS,
        MetricSubsystem::ApiRequests,
    )
});

pub static API_REQUESTS_5XX_ERRORS_TOTAL_BY_SERVER_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::Custom("requests_5xx_errors_total_by_server".to_string()),
        "Total number of requests with 5xx errors by server",
        &API_SERVER_NAME_TYPE_LABELS,
        MetricSubsystem::ApiRequests,
    )
});

pub static API_REQUESTS_4XX_ERRORS_TOTAL_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::ApiRequests4xxErrorsTotal,
        "Total number of requests with 4xx errors",
        &API_NAME_TYPE_LABELS,
        MetricSubsystem::ApiRequests,
    )
});

pub static API_REQUESTS_4XX_ERRORS_TOTAL_BY_SERVER_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::Custom("requests_4xx_errors_total_by_server".to_string()),
        "Total number of requests with 4xx errors by server",
        &API_SERVER_NAME_TYPE_LABELS,
        MetricSubsystem::ApiRequests,
    )
});

pub static API_REQUESTS_CANCELED_TOTAL_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::ApiRequestsCanceledTotal,
        "Total number of requests canceled by the client",
        &API_NAME_TYPE_LABELS,
        MetricSubsystem::ApiRequests,
    )
});

pub static API_REQUESTS_CANCELED_TOTAL_BY_SERVER_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::Custom("requests_canceled_total_by_server".to_string()),
        "Total number of requests canceled by the client by server",
        &API_SERVER_NAME_TYPE_LABELS,
        MetricSubsystem::ApiRequests,
    )
});

pub static API_REQUESTS_TTFB_SECONDS_DISTRIBUTION_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::ApiRequestsTTFBSecondsDistribution,
        "Distribution of time to first byte across API calls",
        &API_NAME_TYPE_LE_LABELS,
        MetricSubsystem::ApiRequests,
    )
});

pub static API_REQUESTS_TTFB_SECONDS_DISTRIBUTION_BY_SERVER_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::Custom("requests_ttfb_seconds_distribution_by_server".to_string()),
        "Distribution of time to first byte across API calls by server",
        &API_SERVER_NAME_TYPE_LE_LABELS,
        MetricSubsystem::ApiRequests,
    )
});

pub static API_TRAFFIC_SENT_BYTES_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::ApiTrafficSentBytes,
        "Total number of bytes sent",
        &API_TYPE_LABELS,
        MetricSubsystem::ApiRequests,
    )
});

pub static API_TRAFFIC_SENT_BYTES_BY_SERVER_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::Custom("traffic_sent_bytes_by_server".to_string()),
        "Total number of bytes sent by server",
        &API_SERVER_TYPE_LABELS,
        MetricSubsystem::ApiRequests,
    )
});

pub static API_TRAFFIC_RECV_BYTES_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::ApiTrafficRecvBytes,
        "Total number of bytes received",
        &API_TYPE_LABELS,
        MetricSubsystem::ApiRequests,
    )
});

pub static API_TRAFFIC_RECV_BYTES_BY_SERVER_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::Custom("traffic_recv_bytes_by_server".to_string()),
        "Total number of bytes received by server",
        &API_SERVER_TYPE_LABELS,
        MetricSubsystem::ApiRequests,
    )
});
