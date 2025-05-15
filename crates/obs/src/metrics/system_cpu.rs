use crate::metrics::{new_gauge_md, subsystems, MetricDescriptor, MetricName};

/// CPU 系统相关指标描述符
lazy_static::lazy_static! {
    pub static ref SYS_CPU_AVG_IDLE_MD: MetricDescriptor =
        new_gauge_md(
            MetricName::SysCPUAvgIdle,
            "Average CPU idle time",
            &[],  // 无标签
            subsystems::SYSTEM_CPU
        );

    pub static ref SYS_CPU_AVG_IOWAIT_MD: MetricDescriptor =
        new_gauge_md(
            MetricName::SysCPUAvgIOWait,
            "Average CPU IOWait time",
            &[],  // 无标签
            subsystems::SYSTEM_CPU
        );

    pub static ref SYS_CPU_LOAD_MD: MetricDescriptor =
        new_gauge_md(
            MetricName::SysCPULoad,
            "CPU load average 1min",
            &[],  // 无标签
            subsystems::SYSTEM_CPU
        );

    pub static ref SYS_CPU_LOAD_PERC_MD: MetricDescriptor =
        new_gauge_md(
            MetricName::SysCPULoadPerc,
            "CPU load average 1min (percentage)",
            &[],  // 无标签
            subsystems::SYSTEM_CPU
        );

    pub static ref SYS_CPU_NICE_MD: MetricDescriptor =
        new_gauge_md(
            MetricName::SysCPUNice,
            "CPU nice time",
            &[],  // 无标签
            subsystems::SYSTEM_CPU
        );

    pub static ref SYS_CPU_STEAL_MD: MetricDescriptor =
        new_gauge_md(
            MetricName::SysCPUSteal,
            "CPU steal time",
            &[],  // 无标签
            subsystems::SYSTEM_CPU
        );

    pub static ref SYS_CPU_SYSTEM_MD: MetricDescriptor =
        new_gauge_md(
            MetricName::SysCPUSystem,
            "CPU system time",
            &[],  // 无标签
            subsystems::SYSTEM_CPU
        );

    pub static ref SYS_CPU_USER_MD: MetricDescriptor =
        new_gauge_md(
            MetricName::SysCPUUser,
            "CPU user time",
            &[],  // 无标签
            subsystems::SYSTEM_CPU
        );
}
