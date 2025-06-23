/// Metric descriptors related to cluster configuration
use crate::metrics::{MetricDescriptor, MetricName, new_gauge_md, subsystems};

lazy_static::lazy_static! {
    pub static ref CONFIG_RRS_PARITY_MD: MetricDescriptor =
        new_gauge_md(
            MetricName::ConfigRRSParity,
            "Reduced redundancy storage class parity",
            &[],
            subsystems::CLUSTER_CONFIG
        );

    pub static ref CONFIG_STANDARD_PARITY_MD: MetricDescriptor =
        new_gauge_md(
            MetricName::ConfigStandardParity,
            "Standard storage class parity",
            &[],
            subsystems::CLUSTER_CONFIG
        );
}
