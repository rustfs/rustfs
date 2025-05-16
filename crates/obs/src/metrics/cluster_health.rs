/// Cluster health-related metric descriptors
use crate::metrics::{new_gauge_md, subsystems, MetricDescriptor, MetricName};

lazy_static::lazy_static! {
    pub static ref HEALTH_DRIVES_OFFLINE_COUNT_MD: MetricDescriptor =
        new_gauge_md(
            MetricName::HealthDrivesOfflineCount,
            "Count of offline drives in the cluster",
            &[],
            subsystems::CLUSTER_HEALTH
        );

    pub static ref HEALTH_DRIVES_ONLINE_COUNT_MD: MetricDescriptor =
        new_gauge_md(
            MetricName::HealthDrivesOnlineCount,
            "Count of online drives in the cluster",
            &[],
            subsystems::CLUSTER_HEALTH
        );

    pub static ref HEALTH_DRIVES_COUNT_MD: MetricDescriptor =
        new_gauge_md(
            MetricName::HealthDrivesCount,
            "Count of all drives in the cluster",
            &[],
            subsystems::CLUSTER_HEALTH
        );
}
