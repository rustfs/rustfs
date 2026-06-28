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
//! Collects CPU metrics including load average, CPU time distribution,
//! and process-level CPU usage.
//!
//! This module provides both system-level and process-level CPU metrics,
//! with process-level metrics migrated from `rustfs-obs::system`.

use crate::metrics::report::PrometheusMetric;
use crate::metrics::schema::system_cpu::*;
use crate::metrics::schema::system_process::{PROCESS_CPU_USAGE_MD, PROCESS_CPU_UTILIZATION_MD};
use std::borrow::Cow;

/// System CPU statistics.
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

/// Process CPU statistics.
///
/// Contains CPU usage metrics for a specific process.
#[derive(Debug, Clone, Default)]
pub struct ProcessCpuStats {
    /// CPU usage percentage (0-100)
    pub usage: f64,
    /// CPU utilization percentage (considering multiple cores, can exceed 100)
    pub utilization: f64,
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

/// Collects process CPU metrics from the given stats.
///
/// Uses the metric descriptors from `metrics_type::system_process` module.
/// Returns a vector of Prometheus metrics for process CPU statistics.
///
/// # Arguments
///
/// * `stats` - Process CPU statistics
/// * `labels` - Optional additional labels (e.g., process attributes)
pub fn collect_process_cpu_metrics(
    stats: &ProcessCpuStats,
    labels: Option<&[(&'static str, Cow<'static, str>)]>,
) -> Vec<PrometheusMetric> {
    let mut usage_metric = PrometheusMetric::from_descriptor(&PROCESS_CPU_USAGE_MD, stats.usage);
    let mut utilization_metric = PrometheusMetric::from_descriptor(&PROCESS_CPU_UTILIZATION_MD, stats.utilization);

    if let Some(l) = labels {
        usage_metric.labels.extend(l.iter().map(|(k, v)| (*k, v.clone())));
        utilization_metric.labels.extend(l.iter().map(|(k, v)| (*k, v.clone())));
    }

    vec![usage_metric, utilization_metric]
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::report::report_metrics;

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
        assert!(metrics.iter().all(|m| m.name.starts_with("rustfs_system_cpu_")));
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

    #[test]
    fn test_collect_process_cpu_metrics() {
        let stats = ProcessCpuStats {
            usage: 45.5,
            utilization: 182.0, // 4 cores at ~45% each
        };

        let metrics = collect_process_cpu_metrics(&stats, None);
        report_metrics(&metrics);

        assert_eq!(metrics.len(), 2);

        // Verify usage metric
        let usage_metric = metrics.iter().find(|m| m.name.contains("cpu_usage"));
        assert!(usage_metric.is_some());
        assert_eq!(usage_metric.map(|m| m.value), Some(45.5));

        // Verify utilization metric
        let util_metric = metrics.iter().find(|m| m.name.contains("cpu_utilization"));
        assert!(util_metric.is_some());
        assert_eq!(util_metric.map(|m| m.value), Some(182.0));
    }

    #[test]
    fn test_collect_process_cpu_metrics_with_labels() {
        let stats = ProcessCpuStats {
            usage: 25.0,
            utilization: 100.0,
        };

        let labels = vec![
            ("process_pid", Cow::Borrowed("12345")),
            ("process_executable_name", Cow::Borrowed("rustfs")),
        ];

        let metrics = collect_process_cpu_metrics(&stats, Some(&labels));
        assert_eq!(metrics.len(), 2);

        // All metrics should have the labels
        for metric in &metrics {
            assert_eq!(metric.labels.len(), 2);
        }
    }
}
