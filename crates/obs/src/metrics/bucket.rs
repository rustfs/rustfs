use crate::metrics::{new_counter_md, new_gauge_md, new_histogram_md, subsystems, MetricDescriptor, MetricName};

/// Bucket 级别 S3 指标描述符
lazy_static::lazy_static! {
    pub static ref BUCKET_API_TRAFFIC_SENT_BYTES_MD: MetricDescriptor =
        new_counter_md(
            MetricName::ApiTrafficSentBytes,
            "Total number of bytes received for a bucket",
            &["bucket", "type"],
            subsystems::BUCKET_API
        );

    pub static ref BUCKET_API_TRAFFIC_RECV_BYTES_MD: MetricDescriptor =
        new_counter_md(
            MetricName::ApiTrafficRecvBytes,
            "Total number of bytes sent for a bucket",
            &["bucket", "type"],
            subsystems::BUCKET_API
        );

    pub static ref BUCKET_API_REQUESTS_IN_FLIGHT_MD: MetricDescriptor =
        new_gauge_md(
            MetricName::ApiRequestsInFlightTotal,
            "Total number of requests currently in flight for a bucket",
            &["bucket", "name", "type"],
            subsystems::BUCKET_API
        );

    pub static ref BUCKET_API_REQUESTS_TOTAL_MD: MetricDescriptor =
        new_counter_md(
            MetricName::ApiRequestsTotal,
            "Total number of requests for a bucket",
            &["bucket", "name", "type"],
            subsystems::BUCKET_API
        );

    pub static ref BUCKET_API_REQUESTS_CANCELED_MD: MetricDescriptor =
        new_counter_md(
            MetricName::ApiRequestsCanceledTotal,
            "Total number of requests canceled by the client for a bucket",
            &["bucket", "name", "type"],
            subsystems::BUCKET_API
        );

    pub static ref BUCKET_API_REQUESTS_4XX_ERRORS_MD: MetricDescriptor =
        new_counter_md(
            MetricName::ApiRequests4xxErrorsTotal,
            "Total number of requests with 4xx errors for a bucket",
            &["bucket", "name", "type"],
            subsystems::BUCKET_API
        );

    pub static ref BUCKET_API_REQUESTS_5XX_ERRORS_MD: MetricDescriptor =
        new_counter_md(
            MetricName::ApiRequests5xxErrorsTotal,
            "Total number of requests with 5xx errors for a bucket",
            &["bucket", "name", "type"],
            subsystems::BUCKET_API
        );

    pub static ref BUCKET_API_REQUESTS_TTFB_SECONDS_DISTRIBUTION_MD: MetricDescriptor =
        new_histogram_md(
            MetricName::ApiRequestsTTFBSecondsDistribution,
            "Distribution of time to first byte across API calls for a bucket",
            &["bucket", "name", "le", "type"],
            subsystems::BUCKET_API
        );
}
