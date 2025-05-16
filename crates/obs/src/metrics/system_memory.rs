/// Memory-related metric descriptors
use crate::metrics::{new_gauge_md, subsystems, MetricDescriptor, MetricName};

lazy_static::lazy_static! {
    pub static ref MEM_TOTAL_MD: MetricDescriptor =
        new_gauge_md(
            MetricName::MemTotal,
            "Total memory on the node",
            &[],
            subsystems::SYSTEM_MEMORY
        );

    pub static ref MEM_USED_MD: MetricDescriptor =
        new_gauge_md(
            MetricName::MemUsed,
            "Used memory on the node",
            &[],
            subsystems::SYSTEM_MEMORY
        );

    pub static ref MEM_USED_PERC_MD: MetricDescriptor =
        new_gauge_md(
            MetricName::MemUsedPerc,
            "Used memory percentage on the node",
            &[],
            subsystems::SYSTEM_MEMORY
        );

    pub static ref MEM_FREE_MD: MetricDescriptor =
        new_gauge_md(
            MetricName::MemFree,
            "Free memory on the node",
            &[],
            subsystems::SYSTEM_MEMORY
        );

    pub static ref MEM_BUFFERS_MD: MetricDescriptor =
        new_gauge_md(
            MetricName::MemBuffers,
            "Buffers memory on the node",
            &[],
            subsystems::SYSTEM_MEMORY
        );

    pub static ref MEM_CACHE_MD: MetricDescriptor =
        new_gauge_md(
            MetricName::MemCache,
            "Cache memory on the node",
            &[],
            subsystems::SYSTEM_MEMORY
        );

    pub static ref MEM_SHARED_MD: MetricDescriptor =
        new_gauge_md(
            MetricName::MemShared,
            "Shared memory on the node",
            &[],
            subsystems::SYSTEM_MEMORY
        );

    pub static ref MEM_AVAILABLE_MD: MetricDescriptor =
        new_gauge_md(
            MetricName::MemAvailable,
            "Available memory on the node",
            &[],
            subsystems::SYSTEM_MEMORY
        );
}
