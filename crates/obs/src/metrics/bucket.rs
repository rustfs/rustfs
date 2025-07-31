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

/// bucket level s3 metric descriptor
use crate::metrics::{MetricDescriptor, MetricName, new_counter_md, new_gauge_md, new_histogram_md, subsystems};
use std::sync::LazyLock;

pub static BUCKET_API_TRAFFIC_SENT_BYTES_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::ApiTrafficSentBytes,
        "Total number of bytes received for a bucket",
        &["bucket", "type"],
        subsystems::BUCKET_API,
    )
});

pub static BUCKET_API_TRAFFIC_RECV_BYTES_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::ApiTrafficRecvBytes,
        "Total number of bytes sent for a bucket",
        &["bucket", "type"],
        subsystems::BUCKET_API,
    )
});

pub static BUCKET_API_REQUESTS_IN_FLIGHT_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::ApiRequestsInFlightTotal,
        "Total number of requests currently in flight for a bucket",
        &["bucket", "name", "type"],
        subsystems::BUCKET_API,
    )
});

pub static BUCKET_API_REQUESTS_TOTAL_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::ApiRequestsTotal,
        "Total number of requests for a bucket",
        &["bucket", "name", "type"],
        subsystems::BUCKET_API,
    )
});

pub static BUCKET_API_REQUESTS_CANCELED_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::ApiRequestsCanceledTotal,
        "Total number of requests canceled by the client for a bucket",
        &["bucket", "name", "type"],
        subsystems::BUCKET_API,
    )
});

pub static BUCKET_API_REQUESTS_4XX_ERRORS_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::ApiRequests4xxErrorsTotal,
        "Total number of requests with 4xx errors for a bucket",
        &["bucket", "name", "type"],
        subsystems::BUCKET_API,
    )
});

pub static BUCKET_API_REQUESTS_5XX_ERRORS_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::ApiRequests5xxErrorsTotal,
        "Total number of requests with 5xx errors for a bucket",
        &["bucket", "name", "type"],
        subsystems::BUCKET_API,
    )
});

pub static BUCKET_API_REQUESTS_TTFB_SECONDS_DISTRIBUTION_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_histogram_md(
        MetricName::ApiRequestsTTFBSecondsDistribution,
        "Distribution of time to first byte across API calls for a bucket",
        &["bucket", "name", "le", "type"],
        subsystems::BUCKET_API,
    )
});
