use crate::metrics::{new_counter_md, subsystems, MetricDescriptor, MetricName};

/// IAM 相关指标描述符
lazy_static::lazy_static! {
    pub static ref LAST_SYNC_DURATION_MILLIS_MD: MetricDescriptor =
        new_counter_md(
            MetricName::LastSyncDurationMillis,
            "Last successful IAM data sync duration in milliseconds",
            &[],  // 无标签
            subsystems::CLUSTER_IAM
        );

    pub static ref PLUGIN_AUTHN_SERVICE_FAILED_REQUESTS_MINUTE_MD: MetricDescriptor =
        new_counter_md(
            MetricName::PluginAuthnServiceFailedRequestsMinute,
            "When plugin authentication is configured, returns failed requests count in the last full minute",
            &[],  // 无标签
            subsystems::CLUSTER_IAM
        );

    pub static ref PLUGIN_AUTHN_SERVICE_LAST_FAIL_SECONDS_MD: MetricDescriptor =
        new_counter_md(
            MetricName::PluginAuthnServiceLastFailSeconds,
            "When plugin authentication is configured, returns time (in seconds) since the last failed request to the service",
            &[],  // 无标签
            subsystems::CLUSTER_IAM
        );

    pub static ref PLUGIN_AUTHN_SERVICE_LAST_SUCC_SECONDS_MD: MetricDescriptor =
        new_counter_md(
            MetricName::PluginAuthnServiceLastSuccSeconds,
            "When plugin authentication is configured, returns time (in seconds) since the last successful request to the service",
            &[],  // 无标签
            subsystems::CLUSTER_IAM
        );

    pub static ref PLUGIN_AUTHN_SERVICE_SUCC_AVG_RTT_MS_MINUTE_MD: MetricDescriptor =
        new_counter_md(
            MetricName::PluginAuthnServiceSuccAvgRttMsMinute,
            "When plugin authentication is configured, returns average round-trip-time of successful requests in the last full minute",
            &[],  // 无标签
            subsystems::CLUSTER_IAM
        );

    pub static ref PLUGIN_AUTHN_SERVICE_SUCC_MAX_RTT_MS_MINUTE_MD: MetricDescriptor =
        new_counter_md(
            MetricName::PluginAuthnServiceSuccMaxRttMsMinute,
            "When plugin authentication is configured, returns maximum round-trip-time of successful requests in the last full minute",
            &[],  // 无标签
            subsystems::CLUSTER_IAM
        );

    pub static ref PLUGIN_AUTHN_SERVICE_TOTAL_REQUESTS_MINUTE_MD: MetricDescriptor =
        new_counter_md(
            MetricName::PluginAuthnServiceTotalRequestsMinute,
            "When plugin authentication is configured, returns total requests count in the last full minute",
            &[],  // 无标签
            subsystems::CLUSTER_IAM
        );

    pub static ref SINCE_LAST_SYNC_MILLIS_MD: MetricDescriptor =
        new_counter_md(
            MetricName::SinceLastSyncMillis,
            "Time (in milliseconds) since last successful IAM data sync.",
            &[],  // 无标签
            subsystems::CLUSTER_IAM
        );

    pub static ref SYNC_FAILURES_MD: MetricDescriptor =
        new_counter_md(
            MetricName::SyncFailures,
            "Number of failed IAM data syncs since server start.",
            &[],  // 无标签
            subsystems::CLUSTER_IAM
        );

    pub static ref SYNC_SUCCESSES_MD: MetricDescriptor =
        new_counter_md(
            MetricName::SyncSuccesses,
            "Number of successful IAM data syncs since server start.",
            &[],  // 无标签
            subsystems::CLUSTER_IAM
        );
}
