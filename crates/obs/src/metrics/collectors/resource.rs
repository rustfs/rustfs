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

//! System resource metrics collector.
//!
//! Collects system-level metrics for the RustFS process including
//! CPU usage, memory consumption, and process uptime.
//!
//! This collector reuses the metric descriptors defined in `metrics_type::process_resource`
//! to avoid duplication of metric names, types, and help text.

use crate::metrics::report::PrometheusMetric;
use crate::metrics::schema::process_resource::*;

/// Resource statistics for metrics collection.
///
/// This struct provides a decoupled interface for collecting resource metrics
/// without depending on specific internal types. HTTP handlers should populate
/// this struct from their available data sources.
#[derive(Debug, Clone, Default)]
pub struct ResourceStats {
    /// CPU usage as a percentage (can exceed 100% on multi-core systems)
    pub cpu_percent: f64,
    /// Resident memory usage in bytes
    pub memory_bytes: u64,
    /// Process uptime in seconds
    pub uptime_seconds: u64,
}

/// Collects resource metrics from the provided resource statistics.
///
/// Uses the metric descriptors from `metrics_type::process_resource` module.
/// Returns a vector of Prometheus metrics for resource statistics.
pub fn collect_resource_metrics(stats: &ResourceStats) -> Vec<PrometheusMetric> {
    vec![
        PrometheusMetric::from_descriptor(&PROCESS_CPU_PERCENT_MD, stats.cpu_percent),
        PrometheusMetric::from_descriptor(&PROCESS_MEMORY_BYTES_MD, stats.memory_bytes as f64),
        PrometheusMetric::from_descriptor(&PROCESS_UPTIME_SECONDS_MD, stats.uptime_seconds as f64),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::report::report_metrics;

    #[test]
    fn test_collect_resource_metrics() {
        let stats = ResourceStats {
            cpu_percent: 45.5,
            memory_bytes: 1024 * 1024 * 256,
            uptime_seconds: 7200,
        };

        let metrics = collect_resource_metrics(&stats);
        report_metrics(&metrics);

        assert_eq!(metrics.len(), 3);

        // Verify CPU metric
        let cpu_metric_name = PROCESS_CPU_PERCENT_MD.get_full_metric_name();
        let cpu = metrics.iter().find(|m| m.name == cpu_metric_name && m.value == 45.5);
        assert!(cpu.is_some());

        // Verify memory metric
        let memory_metric_name = PROCESS_MEMORY_BYTES_MD.get_full_metric_name();
        let memory = metrics
            .iter()
            .find(|m| m.name == memory_metric_name && m.value == (1024 * 1024 * 256) as f64);
        assert!(memory.is_some());

        // Verify uptime metric
        let uptime_metric_name = PROCESS_UPTIME_SECONDS_MD.get_full_metric_name();
        let uptime = metrics.iter().find(|m| m.name == uptime_metric_name && m.value == 7200.0);
        assert!(uptime.is_some());
    }

    #[test]
    fn test_collect_resource_metrics_zero_values() {
        let stats = ResourceStats::default();

        let metrics = collect_resource_metrics(&stats);
        report_metrics(&metrics);

        assert_eq!(metrics.len(), 3);

        for metric in &metrics {
            assert_eq!(metric.value, 0.0);
            assert!(metric.labels.is_empty());
        }
    }

    #[test]
    fn test_collect_resource_metrics_high_cpu() {
        let stats = ResourceStats {
            cpu_percent: 150.0, // Can exceed 100% on multi-core systems
            memory_bytes: 0,
            uptime_seconds: 0,
        };

        let metrics = collect_resource_metrics(&stats);
        report_metrics(&metrics);

        let cpu_metric_name = PROCESS_CPU_PERCENT_MD.get_full_metric_name();
        let cpu = metrics.iter().find(|m| m.name == cpu_metric_name && m.value == 150.0);
        assert!(cpu.is_some());
    }

    #[test]
    fn test_resource_stats_default() {
        let stats = ResourceStats::default();
        assert_eq!(stats.cpu_percent, 0.0);
        assert_eq!(stats.memory_bytes, 0);
        assert_eq!(stats.uptime_seconds, 0);
    }
}
