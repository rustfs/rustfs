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

#![allow(dead_code)]

//! System CPU metrics collector.
//!
//! Collects CPU-related metrics including load average, idle time,
//! I/O wait, and CPU usage percentages.
//!
//! This collector reuses the metric descriptors defined in `metrics_type::system_cpu`
//! to avoid duplication of metric names, types, and help text.

use crate::format::PrometheusMetric;
use crate::metrics_type::system_cpu::*;

/// CPU statistics for a node.
#[derive(Debug, Clone, Default)]
pub struct CpuStats {
    /// Average CPU idle time (percentage, 0-100)
    pub avg_idle: f64,
    /// Average CPU I/O wait time (percentage, 0-100)
    pub avg_iowait: f64,
    /// CPU load average over 1 minute
    pub load_avg: f64,
    /// CPU load average as percentage
    pub load_avg_perc: f64,
    /// CPU nice time (percentage, 0-100)
    pub nice: f64,
    /// CPU steal time (percentage, 0-100)
    pub steal: f64,
    /// CPU system time (percentage, 0-100)
    pub system: f64,
    /// CPU user time (percentage, 0-100)
    pub user: f64,
}

/// Collects CPU metrics from the given stats.
///
/// Uses the metric descriptors from `metrics_type::system_cpu` module.
/// Returns a vector of Prometheus metrics for CPU statistics.
pub fn collect_cpu_metrics(stats: &CpuStats) -> Vec<PrometheusMetric> {
    vec![
        PrometheusMetric::from_descriptor(&SYS_CPU_AVG_IDLE_MD, stats.avg_idle),
        PrometheusMetric::from_descriptor(&SYS_CPU_AVG_IOWAIT_MD, stats.avg_iowait),
        PrometheusMetric::from_descriptor(&SYS_CPU_LOAD_MD, stats.load_avg),
        PrometheusMetric::from_descriptor(&SYS_CPU_LOAD_PERC_MD, stats.load_avg_perc),
        PrometheusMetric::from_descriptor(&SYS_CPU_NICE_MD, stats.nice),
        PrometheusMetric::from_descriptor(&SYS_CPU_STEAL_MD, stats.steal),
        PrometheusMetric::from_descriptor(&SYS_CPU_SYSTEM_MD, stats.system),
        PrometheusMetric::from_descriptor(&SYS_CPU_USER_MD, stats.user),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::format::report_metrics;

    #[test]
    fn test_collect_cpu_metrics() {
        let stats = CpuStats {
            avg_idle: 75.5,
            avg_iowait: 2.3,
            load_avg: 1.5,
            load_avg_perc: 37.5,
            nice: 0.5,
            steal: 0.1,
            system: 10.0,
            user: 15.0,
        };

        let metrics = collect_cpu_metrics(&stats);
        report_metrics(&metrics);

        assert_eq!(metrics.len(), 8);

        // Verify that metric names are properly generated from descriptors
        assert!(metrics.iter().all(|m| m.name.starts_with("gauge.rustfs_system_cpu_")));
    }

    #[test]
    fn test_collect_cpu_metrics_default() {
        let stats = CpuStats::default();
        let metrics = collect_cpu_metrics(&stats);

        assert_eq!(metrics.len(), 8);
        for metric in &metrics {
            assert_eq!(metric.value, 0.0);
            assert!(metric.labels.is_empty());
        }
    }
}
