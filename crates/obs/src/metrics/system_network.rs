/// Network-related metric descriptors
use crate::metrics::{new_counter_md, new_gauge_md, subsystems, MetricDescriptor, MetricName};

lazy_static::lazy_static! {
    pub static ref INTERNODE_ERRORS_TOTAL_MD: MetricDescriptor =
        new_counter_md(
            MetricName::InternodeErrorsTotal,
            "Total number of failed internode calls",
            &[],
            subsystems::SYSTEM_NETWORK_INTERNODE
        );

    pub static ref INTERNODE_DIAL_ERRORS_TOTAL_MD: MetricDescriptor =
        new_counter_md(
            MetricName::InternodeDialErrorsTotal,
            "Total number of internode TCP dial timeouts and errors",
            &[],
            subsystems::SYSTEM_NETWORK_INTERNODE
        );

    pub static ref INTERNODE_DIAL_AVG_TIME_NANOS_MD: MetricDescriptor =
        new_gauge_md(
            MetricName::InternodeDialAvgTimeNanos,
            "Average dial time of internode TCP calls in nanoseconds",
            &[],
            subsystems::SYSTEM_NETWORK_INTERNODE
        );

    pub static ref INTERNODE_SENT_BYTES_TOTAL_MD: MetricDescriptor =
        new_counter_md(
            MetricName::InternodeSentBytesTotal,
            "Total number of bytes sent to other peer nodes",
            &[],
            subsystems::SYSTEM_NETWORK_INTERNODE
        );

    pub static ref INTERNODE_RECV_BYTES_TOTAL_MD: MetricDescriptor =
        new_counter_md(
            MetricName::InternodeRecvBytesTotal,
            "Total number of bytes received from other peer nodes",
            &[],
            subsystems::SYSTEM_NETWORK_INTERNODE
        );
}
