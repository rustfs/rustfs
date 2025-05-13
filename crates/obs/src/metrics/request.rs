use crate::metrics::{new_counter_md, new_gauge_md, MetricDescriptor, MetricName};

/// Predefined API metric descriptors
lazy_static::lazy_static! {
    pub static ref API_REJECTED_AUTH_TOTAL_MD: MetricDescriptor =
        new_counter_md(
            MetricName::ApiRejectedAuthTotal.as_str(),
            "Total number of requests rejected for auth failure",
            &["type"]
        );

    pub static ref API_REJECTED_HEADER_TOTAL_MD: MetricDescriptor =
        new_counter_md(
            MetricName::ApiRejectedHeaderTotal.as_str(),
            "Total number of requests rejected for invalid header",
            &["type"]
        );

    pub static ref API_REJECTED_TIMESTAMP_TOTAL_MD: MetricDescriptor =
        new_counter_md(
            MetricName::ApiRejectedTimestampTotal.as_str(),
            "Total number of requests rejected for invalid timestamp",
            &["type"]
        );

    pub static ref API_REJECTED_INVALID_TOTAL_MD: MetricDescriptor =
        new_counter_md(
            MetricName::ApiRejectedInvalidTotal.as_str(),
            "Total number of invalid requests",
            &["type"]
        );

    pub static ref API_REQUESTS_WAITING_TOTAL_MD: MetricDescriptor =
        new_gauge_md(
            MetricName::ApiRequestsWaitingTotal.as_str(),
            "Total number of requests in the waiting queue",
            &["type"]
        );

    pub static ref API_REQUESTS_INCOMING_TOTAL_MD: MetricDescriptor =
        new_gauge_md(
            MetricName::ApiRequestsIncomingTotal.as_str(),
            "Total number of incoming requests",
            &["type"]
        );

    pub static ref API_REQUESTS_IN_FLIGHT_TOTAL_MD: MetricDescriptor =
        new_gauge_md(
            MetricName::ApiRequestsInFlightTotal.as_str(),
            "Total number of requests currently in flight",
            &["name", "type"]
        );

    pub static ref API_REQUESTS_TOTAL_MD: MetricDescriptor =
        new_counter_md(
            MetricName::ApiRequestsTotal.as_str(),
            "Total number of requests",
            &["name", "type"]
        );

    pub static ref API_REQUESTS_ERRORS_TOTAL_MD: MetricDescriptor =
        new_counter_md(
            MetricName::ApiRequestsErrorsTotal.as_str(),
            "Total number of requests with (4xx and 5xx) errors",
            &["name", "type"]
        );

    pub static ref API_REQUESTS_5XX_ERRORS_TOTAL_MD: MetricDescriptor =
        new_counter_md(
            MetricName::ApiRequests5xxErrorsTotal.as_str(),
            "Total number of requests with 5xx errors",
            &["name", "type"]
        );

    pub static ref API_REQUESTS_4XX_ERRORS_TOTAL_MD: MetricDescriptor =
        new_counter_md(
            MetricName::ApiRequests4xxErrorsTotal.as_str(),
            "Total number of requests with 4xx errors",
            &["name", "type"]
        );

    pub static ref API_REQUESTS_CANCELED_TOTAL_MD: MetricDescriptor =
        new_counter_md(
            MetricName::ApiRequestsCanceledTotal.as_str(),
            "Total number of requests canceled by the client",
            &["name", "type"]
        );

    pub static ref API_REQUESTS_TTFB_SECONDS_DISTRIBUTION_MD: MetricDescriptor =
        new_counter_md(
            MetricName::ApiRequestsTTFBSecondsDistribution.as_str(),
            "Distribution of time to first byte across API calls",
            &["name", "type", "le"]
        );

    pub static ref API_TRAFFIC_SENT_BYTES_MD: MetricDescriptor =
        new_counter_md(
            MetricName::ApiTrafficSentBytes.as_str(),
            "Total number of bytes sent",
            &["type"]
        );

    pub static ref API_TRAFFIC_RECV_BYTES_MD: MetricDescriptor =
        new_counter_md(
            MetricName::ApiTrafficRecvBytes.as_str(),
            "Total number of bytes received",
            &["type"]
        );
}
