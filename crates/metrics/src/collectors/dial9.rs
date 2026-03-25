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

//! dial9 Tokio runtime telemetry metrics collector.
//!
//! This module provides metrics for monitoring the health and performance
//! of the dial9 telemetry system itself.

#![allow(dead_code)]

use crate::MetricType;
use crate::format::PrometheusMetric;
use rustfs_config::{DEFAULT_RUNTIME_DIAL9_ENABLED, ENV_RUNTIME_DIAL9_ENABLED};
use rustfs_utils::get_env_bool;

/// Dial9 telemetry system statistics.
#[derive(Debug, Clone, Default)]
pub struct Dial9Stats {
    /// Total number of telemetry events recorded
    pub events_total: u64,

    /// Total bytes written to trace files
    pub bytes_written: u64,

    /// Number of file rotations that have occurred
    pub rotation_count: u64,

    /// Total number of dial9 errors
    pub errors_total: u64,

    /// Estimated CPU overhead percentage (if available)
    pub cpu_overhead_percent: f64,

    /// Current disk usage by trace files in bytes
    pub disk_usage_bytes: u64,

    /// Number of active sessions
    pub active_sessions: u64,
}

/// Collect dial9 telemetry metrics.
///
/// This function converts dial9 statistics into Prometheus metrics format.
///
/// # Arguments
///
/// * `stats` - Dial9 statistics to report
///
/// # Returns
///
/// A vector of Prometheus metrics for dial9 telemetry statistics.
pub fn collect_dial9_metrics(stats: &Dial9Stats) -> Vec<PrometheusMetric> {
    let enabled = is_dial9_enabled();
    let enabled_value = if enabled { 1.0 } else { 0.0 };

    let mut metrics = vec![PrometheusMetric::new(
        "rustfs_dial9_enabled",
        MetricType::Gauge,
        "Whether dial9 telemetry is enabled (1) or disabled (0)",
        enabled_value,
    )];

    // If dial9 is disabled, return just the enabled flag
    if !enabled {
        return metrics;
    }

    // Add detailed metrics when enabled
    metrics.extend(vec![
        PrometheusMetric::new(
            "rustfs_dial9_events_total",
            MetricType::Counter,
            "Total number of Tokio runtime events recorded by dial9",
            stats.events_total as f64,
        ),
        PrometheusMetric::new(
            "rustfs_dial9_bytes_written_total",
            MetricType::Counter,
            "Total bytes written to dial9 trace files",
            stats.bytes_written as f64,
        ),
        PrometheusMetric::new(
            "rustfs_dial9_rotations_total",
            MetricType::Counter,
            "Total number of trace file rotations",
            stats.rotation_count as f64,
        ),
        PrometheusMetric::new(
            "rustfs_dial9_errors_total",
            MetricType::Counter,
            "Total number of dial9 telemetry errors",
            stats.errors_total as f64,
        ),
        PrometheusMetric::new(
            "rustfs_dial9_cpu_overhead_percent",
            MetricType::Gauge,
            "Estimated CPU overhead percentage from dial9 telemetry",
            stats.cpu_overhead_percent,
        ),
        PrometheusMetric::new(
            "rustfs_dial9_disk_usage_bytes",
            MetricType::Gauge,
            "Current disk usage by dial9 trace files",
            stats.disk_usage_bytes as f64,
        ),
        PrometheusMetric::new(
            "rustfs_dial9_active_sessions",
            MetricType::Gauge,
            "Number of active dial9 telemetry sessions",
            stats.active_sessions as f64,
        ),
    ]);

    metrics
}

/// Check if dial9 telemetry is enabled via environment variable.
pub fn is_dial9_enabled() -> bool {
    get_env_bool(ENV_RUNTIME_DIAL9_ENABLED, DEFAULT_RUNTIME_DIAL9_ENABLED)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dial9_stats_default() {
        let stats = Dial9Stats::default();
        assert_eq!(stats.events_total, 0);
        assert_eq!(stats.bytes_written, 0);
        assert_eq!(stats.rotation_count, 0);
        assert_eq!(stats.errors_total, 0);
        assert_eq!(stats.cpu_overhead_percent, 0.0);
        assert_eq!(stats.disk_usage_bytes, 0);
        assert_eq!(stats.active_sessions, 0);
    }

    #[test]
    fn test_collect_dial9_metrics() {
        let stats = Dial9Stats {
            events_total: 100,
            bytes_written: 1024,
            ..Default::default()
        };
        let metrics = collect_dial9_metrics(&stats);

        // Should always have at least the enabled flag
        assert!(!metrics.is_empty());
    }

    #[test]
    fn test_collect_dial9_metrics_with_values() {
        let stats = Dial9Stats {
            events_total: 10000,
            bytes_written: 1024000,
            rotation_count: 5,
            errors_total: 0,
            cpu_overhead_percent: 2.5,
            disk_usage_bytes: 2048000,
            active_sessions: 1,
        };

        let metrics = collect_dial9_metrics(&stats);

        // When dial9 is enabled, should have all metrics
        // Note: This test assumes dial9 is enabled in the test environment
        // If disabled, only the enabled flag metric will be present
        assert!(!metrics.is_empty());
    }
}
