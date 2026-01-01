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

use crate::prometheus::{MetricType, PrometheusMetric};

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
/// use rustfs_obs::prometheus::collectors::{collect_resource_metrics, ResourceStats};
///
/// let stats = ResourceStats {
///     cpu_percent: 25.5,
///     memory_bytes: 1024 * 1024 * 512, // 512 MB
///     uptime_seconds: 3600,
/// };
/// let metrics = collect_resource_metrics(&stats);
/// ```
#[must_use]
pub fn collect_resource_metrics(stats: &ResourceStats) -> Vec<PrometheusMetric> {
    vec![
        PrometheusMetric::new(
            "rustfs_process_cpu_percent",
            MetricType::Gauge,
            "CPU usage of the RustFS process as a percentage",
            stats.cpu_percent,
        ),
        PrometheusMetric::new(
            "rustfs_process_memory_bytes",
            MetricType::Gauge,
            "Resident memory usage of the RustFS process in bytes",
            stats.memory_bytes as f64,
        ),
        PrometheusMetric::new(
            "rustfs_process_uptime_seconds",
            MetricType::Gauge,
            "Uptime of the RustFS process in seconds",
            stats.uptime_seconds as f64,
        ),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_collect_resource_metrics() {
        let stats = ResourceStats {
            cpu_percent: 45.5,
            memory_bytes: 1024 * 1024 * 256,
            uptime_seconds: 7200,
        };

        let metrics = collect_resource_metrics(&stats);

        assert_eq!(metrics.len(), 3);

        // Verify CPU metric
        let cpu = metrics.iter().find(|m| m.name == "rustfs_process_cpu_percent");
        assert!(cpu.is_some());
        assert_eq!(cpu.map(|m| m.value), Some(45.5));

        // Verify memory metric
        let memory = metrics.iter().find(|m| m.name == "rustfs_process_memory_bytes");
        assert!(memory.is_some());
        assert_eq!(memory.map(|m| m.value), Some((1024 * 1024 * 256) as f64));

        // Verify uptime metric
        let uptime = metrics.iter().find(|m| m.name == "rustfs_process_uptime_seconds");
        assert!(uptime.is_some());
        assert_eq!(uptime.map(|m| m.value), Some(7200.0));
    }

    #[test]
    fn test_collect_resource_metrics_zero_values() {
        let stats = ResourceStats::default();

        let metrics = collect_resource_metrics(&stats);

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

        let cpu = metrics.iter().find(|m| m.name == "rustfs_process_cpu_percent");
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
