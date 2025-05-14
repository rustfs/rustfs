use crate::metrics::{new_gauge_md, subsystems, MetricDescriptor, MetricName};

/// 集群配置相关指标描述符
lazy_static::lazy_static! {
    pub static ref CONFIG_RRS_PARITY_MD: MetricDescriptor =
        new_gauge_md(
            MetricName::ConfigRRSParity,
            "Reduced redundancy storage class parity",
            &[],  // 无标签
            subsystems::CLUSTER_CONFIG
        );

    pub static ref CONFIG_STANDARD_PARITY_MD: MetricDescriptor =
        new_gauge_md(
            MetricName::ConfigStandardParity,
            "Standard storage class parity",
            &[],  // 无标签
            subsystems::CLUSTER_CONFIG
        );
}
