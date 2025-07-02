// Copyright 2024 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

/// CPU system-related metric descriptors
use crate::metrics::{MetricDescriptor, MetricName, new_gauge_md, subsystems};

lazy_static::lazy_static! {
    pub static ref SYS_CPU_AVG_IDLE_MD: MetricDescriptor =
        new_gauge_md(
            MetricName::SysCPUAvgIdle,
            "Average CPU idle time",
            &[],
            subsystems::SYSTEM_CPU
        );

    pub static ref SYS_CPU_AVG_IOWAIT_MD: MetricDescriptor =
        new_gauge_md(
            MetricName::SysCPUAvgIOWait,
            "Average CPU IOWait time",
            &[],
            subsystems::SYSTEM_CPU
        );

    pub static ref SYS_CPU_LOAD_MD: MetricDescriptor =
        new_gauge_md(
            MetricName::SysCPULoad,
            "CPU load average 1min",
            &[],
            subsystems::SYSTEM_CPU
        );

    pub static ref SYS_CPU_LOAD_PERC_MD: MetricDescriptor =
        new_gauge_md(
            MetricName::SysCPULoadPerc,
            "CPU load average 1min (percentage)",
            &[],
            subsystems::SYSTEM_CPU
        );

    pub static ref SYS_CPU_NICE_MD: MetricDescriptor =
        new_gauge_md(
            MetricName::SysCPUNice,
            "CPU nice time",
            &[],
            subsystems::SYSTEM_CPU
        );

    pub static ref SYS_CPU_STEAL_MD: MetricDescriptor =
        new_gauge_md(
            MetricName::SysCPUSteal,
            "CPU steal time",
            &[],
            subsystems::SYSTEM_CPU
        );

    pub static ref SYS_CPU_SYSTEM_MD: MetricDescriptor =
        new_gauge_md(
            MetricName::SysCPUSystem,
            "CPU system time",
            &[],
            subsystems::SYSTEM_CPU
        );

    pub static ref SYS_CPU_USER_MD: MetricDescriptor =
        new_gauge_md(
            MetricName::SysCPUUser,
            "CPU user time",
            &[],
            subsystems::SYSTEM_CPU
        );
}
