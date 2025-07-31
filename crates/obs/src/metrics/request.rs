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

use crate::metrics::{MetricDescriptor, MetricName, MetricSubsystem, new_counter_md, new_gauge_md, subsystems};
use std::sync::LazyLock;

pub static API_REJECTED_AUTH_TOTAL_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::ApiRejectedAuthTotal,
        "Total number of requests rejected for auth failure",
        &["type"],
        subsystems::API_REQUESTS,
    )
});

pub static API_REJECTED_HEADER_TOTAL_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::ApiRejectedHeaderTotal,
        "Total number of requests rejected for invalid header",
        &["type"],
        MetricSubsystem::ApiRequests,
    )
});

pub static API_REJECTED_TIMESTAMP_TOTAL_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::ApiRejectedTimestampTotal,
        "Total number of requests rejected for invalid timestamp",
        &["type"],
        MetricSubsystem::ApiRequests,
    )
});

pub static API_REJECTED_INVALID_TOTAL_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::ApiRejectedInvalidTotal,
        "Total number of invalid requests",
        &["type"],
        MetricSubsystem::ApiRequests,
    )
});

pub static API_REQUESTS_WAITING_TOTAL_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::ApiRequestsWaitingTotal,
        "Total number of requests in the waiting queue",
        &["type"],
        MetricSubsystem::ApiRequests,
    )
});

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
        &["name", "type"],
        MetricSubsystem::ApiRequests,
    )
});

pub static API_REQUESTS_TOTAL_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::ApiRequestsTotal,
        "Total number of requests",
        &["name", "type"],
        MetricSubsystem::ApiRequests,
    )
});

pub static API_REQUESTS_ERRORS_TOTAL_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::ApiRequestsErrorsTotal,
        "Total number of requests with (4xx and 5xx) errors",
        &["name", "type"],
        MetricSubsystem::ApiRequests,
    )
});

pub static API_REQUESTS_5XX_ERRORS_TOTAL_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::ApiRequests5xxErrorsTotal,
        "Total number of requests with 5xx errors",
        &["name", "type"],
        MetricSubsystem::ApiRequests,
    )
});

pub static API_REQUESTS_4XX_ERRORS_TOTAL_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::ApiRequests4xxErrorsTotal,
        "Total number of requests with 4xx errors",
        &["name", "type"],
        MetricSubsystem::ApiRequests,
    )
});

pub static API_REQUESTS_CANCELED_TOTAL_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::ApiRequestsCanceledTotal,
        "Total number of requests canceled by the client",
        &["name", "type"],
        MetricSubsystem::ApiRequests,
    )
});

pub static API_REQUESTS_TTFB_SECONDS_DISTRIBUTION_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::ApiRequestsTTFBSecondsDistribution,
        "Distribution of time to first byte across API calls",
        &["name", "type", "le"],
        MetricSubsystem::ApiRequests,
    )
});

pub static API_TRAFFIC_SENT_BYTES_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::ApiTrafficSentBytes,
        "Total number of bytes sent",
        &["type"],
        MetricSubsystem::ApiRequests,
    )
});

pub static API_TRAFFIC_RECV_BYTES_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::ApiTrafficRecvBytes,
        "Total number of bytes received",
        &["type"],
        MetricSubsystem::ApiRequests,
    )
});
