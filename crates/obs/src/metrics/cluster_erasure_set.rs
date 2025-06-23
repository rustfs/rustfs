/// Erasure code set related metric descriptors
use crate::metrics::{MetricDescriptor, MetricName, new_gauge_md, subsystems};

/// The label for the pool ID
pub const POOL_ID_L: &str = "pool_id";
/// The label for the pool ID
pub const SET_ID_L: &str = "set_id";

lazy_static::lazy_static! {
    pub static ref ERASURE_SET_OVERALL_WRITE_QUORUM_MD: MetricDescriptor =
        new_gauge_md(
            MetricName::ErasureSetOverallWriteQuorum,
            "Overall write quorum across pools and sets",
            &[],
            subsystems::CLUSTER_ERASURE_SET
        );

    pub static ref ERASURE_SET_OVERALL_HEALTH_MD: MetricDescriptor =
        new_gauge_md(
            MetricName::ErasureSetOverallHealth,
            "Overall health across pools and sets (1=healthy, 0=unhealthy)",
            &[],
            subsystems::CLUSTER_ERASURE_SET
        );

    pub static ref ERASURE_SET_READ_QUORUM_MD: MetricDescriptor =
        new_gauge_md(
            MetricName::ErasureSetReadQuorum,
            "Read quorum for the erasure set in a pool",
            &[POOL_ID_L, SET_ID_L],
            subsystems::CLUSTER_ERASURE_SET
        );

    pub static ref ERASURE_SET_WRITE_QUORUM_MD: MetricDescriptor =
        new_gauge_md(
            MetricName::ErasureSetWriteQuorum,
            "Write quorum for the erasure set in a pool",
            &[POOL_ID_L, SET_ID_L],
            subsystems::CLUSTER_ERASURE_SET
        );

    pub static ref ERASURE_SET_ONLINE_DRIVES_COUNT_MD: MetricDescriptor =
        new_gauge_md(
            MetricName::ErasureSetOnlineDrivesCount,
            "Count of online drives in the erasure set in a pool",
            &[POOL_ID_L, SET_ID_L],
            subsystems::CLUSTER_ERASURE_SET
        );

    pub static ref ERASURE_SET_HEALING_DRIVES_COUNT_MD: MetricDescriptor =
        new_gauge_md(
            MetricName::ErasureSetHealingDrivesCount,
            "Count of healing drives in the erasure set in a pool",
            &[POOL_ID_L, SET_ID_L],
            subsystems::CLUSTER_ERASURE_SET
        );

    pub static ref ERASURE_SET_HEALTH_MD: MetricDescriptor =
        new_gauge_md(
            MetricName::ErasureSetHealth,
            "Health of the erasure set in a pool (1=healthy, 0=unhealthy)",
            &[POOL_ID_L, SET_ID_L],
            subsystems::CLUSTER_ERASURE_SET
        );

    pub static ref ERASURE_SET_READ_TOLERANCE_MD: MetricDescriptor =
        new_gauge_md(
            MetricName::ErasureSetReadTolerance,
            "No of drive failures that can be tolerated without disrupting read operations",
            &[POOL_ID_L, SET_ID_L],
            subsystems::CLUSTER_ERASURE_SET
        );

    pub static ref ERASURE_SET_WRITE_TOLERANCE_MD: MetricDescriptor =
        new_gauge_md(
            MetricName::ErasureSetWriteTolerance,
            "No of drive failures that can be tolerated without disrupting write operations",
            &[POOL_ID_L, SET_ID_L],
            subsystems::CLUSTER_ERASURE_SET
        );

    pub static ref ERASURE_SET_READ_HEALTH_MD: MetricDescriptor =
        new_gauge_md(
            MetricName::ErasureSetReadHealth,
            "Health of the erasure set in a pool for read operations (1=healthy, 0=unhealthy)",
            &[POOL_ID_L, SET_ID_L],
            subsystems::CLUSTER_ERASURE_SET
        );

    pub static ref ERASURE_SET_WRITE_HEALTH_MD: MetricDescriptor =
        new_gauge_md(
            MetricName::ErasureSetWriteHealth,
            "Health of the erasure set in a pool for write operations (1=healthy, 0=unhealthy)",
            &[POOL_ID_L, SET_ID_L],
            subsystems::CLUSTER_ERASURE_SET
        );
}
