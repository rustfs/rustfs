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

use crate::MetricType;
use crate::format::PrometheusMetric;

/// Resource statistics for a RustFS process.
///
/// This struct encapsulates the resource usage data that can be
/// collected from the operating system for the current process.
#[derive(Debug, Clone, Default)]
pub struct ResourceStats {
    /// CPU usage as a percentage (0.0 to 100.0+)
    pub cpu_percent: f64,
    /// Resident memory usage in bytes
    pub memory_bytes: u64,
    /// Process uptime in seconds
    pub uptime_seconds: u64,
}

// Static metric definitions
const METRIC_CPU: &str = "rustfs_process_cpu_percent";
const METRIC_MEMORY: &str = "rustfs_process_memory_bytes";
const METRIC_UPTIME: &str = "rustfs_process_uptime_seconds";

const HELP_CPU: &str = "CPU usage of the RustFS process as a percentage";
const HELP_MEMORY: &str = "Resident memory usage of the RustFS process in bytes";
const HELP_UPTIME: &str = "Uptime of the RustFS process in seconds";

/// Number of metrics produced by this collector.
const METRIC_COUNT: usize = 3;

/// Collects system resource metrics from the provided statistics.
///
/// # Metrics Produced
///
/// - `rustfs_process_cpu_percent`: CPU usage as a percentage
/// - `rustfs_process_memory_bytes`: Resident memory usage in bytes
/// - `rustfs_process_uptime_seconds`: Process uptime in seconds
///
/// # Arguments
///
/// * `stats` - Resource statistics for the current process
///
/// # Example
///
/// ```
/// use rustfs_metrics::collectors::{collect_resource_metrics, ResourceStats};
///
/// let stats = ResourceStats {
///     cpu_percent: 25.5,
///     memory_bytes: 1024 * 1024 * 512, // 512 MB
///     uptime_seconds: 3600,
/// };
/// let metrics = collect_resource_metrics(&stats);
/// assert_eq!(metrics.len(), 3);
/// ```
#[must_use]
#[inline]
pub fn collect_resource_metrics(stats: &ResourceStats) -> Vec<PrometheusMetric> {
    let mut metrics = Vec::with_capacity(METRIC_COUNT);

    metrics.push(PrometheusMetric::new(METRIC_CPU, MetricType::Gauge, HELP_CPU, stats.cpu_percent));

    metrics.push(PrometheusMetric::new(
        METRIC_MEMORY,
        MetricType::Gauge,
        HELP_MEMORY,
        stats.memory_bytes as f64,
    ));

    metrics.push(PrometheusMetric::new(
        METRIC_UPTIME,
        MetricType::Gauge,
        HELP_UPTIME,
        stats.uptime_seconds as f64,
    ));

    metrics
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::format::report_metrics;

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
        let cpu = metrics.iter().find(|m| m.name == METRIC_CPU);
        assert!(cpu.is_some());
        assert_eq!(cpu.map(|m| m.value), Some(45.5));

        // Verify memory metric
        let memory = metrics.iter().find(|m| m.name == METRIC_MEMORY);
        assert!(memory.is_some());
        assert_eq!(memory.map(|m| m.value), Some((1024 * 1024 * 256) as f64));

        // Verify uptime metric
        let uptime = metrics.iter().find(|m| m.name == METRIC_UPTIME);
        assert!(uptime.is_some());
        assert_eq!(uptime.map(|m| m.value), Some(7200.0));
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

        let cpu = metrics.iter().find(|m| m.name == METRIC_CPU);
        assert!(cpu.is_some());
        assert_eq!(cpu.map(|m| m.value), Some(150.0));
    }

    #[test]
    fn test_resource_stats_default() {
        let stats = ResourceStats::default();
        assert_eq!(stats.cpu_percent, 0.0);
        assert_eq!(stats.memory_bytes, 0);
        assert_eq!(stats.uptime_seconds, 0);
    }
}
