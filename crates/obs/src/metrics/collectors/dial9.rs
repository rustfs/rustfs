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
//! Reports the health of the telemetry system itself, not the runtime events it
//! records — those live in the trace segments on disk.
//!
//! Every metric here is backed by a value the process actually observes. A
//! counter that cannot be sourced is not exported at all: a series pinned at
//! zero reads as "nothing happened", which is worse than a missing series.

use crate::MetricType;
use crate::metrics::report::PrometheusMetric;
use crate::telemetry::dial9::{is_configured, is_enabled, is_supported, runtime_stats_snapshot};

/// Health of the dial9 telemetry system.
#[derive(Debug, Clone, Default)]
pub struct Dial9Stats {
    /// Cumulative count of telemetry setup failures.
    pub errors_total: u64,

    /// Bytes on disk held by trace segments, as of the last background refresh.
    pub disk_usage_bytes: u64,

    /// Number of active telemetry sessions (0 or 1).
    pub active_sessions: u64,
}

/// Convert dial9 health statistics into Prometheus metrics.
///
/// `rustfs_dial9_supported` reflects compile-time support, `rustfs_dial9_configured`
/// reflects operator intent, and `rustfs_dial9_active_sessions` reflects reality.
/// They disagree when a build lacks the feature, or when the traced runtime
/// failed to build and the process fell back to a standard runtime — read all
/// three before concluding that telemetry is running.
pub fn collect_dial9_metrics(stats: &Dial9Stats) -> Vec<PrometheusMetric> {
    let mut metrics = vec![
        PrometheusMetric::new(
            "rustfs_dial9_supported",
            MetricType::Gauge,
            "Whether this binary was compiled with dial9 telemetry support (1) or not (0)",
            bool_gauge(is_supported()),
        ),
        PrometheusMetric::new(
            "rustfs_dial9_configured",
            MetricType::Gauge,
            "Whether dial9 telemetry is requested via environment configuration (1) or not (0)",
            bool_gauge(is_configured()),
        ),
    ];

    // Without a running session the remaining series carry no information;
    // exporting them would only publish zeros.
    if !is_enabled() {
        return metrics;
    }

    metrics.extend([
        PrometheusMetric::new(
            "rustfs_dial9_active_sessions",
            MetricType::Gauge,
            "Number of active dial9 telemetry sessions",
            stats.active_sessions as f64,
        ),
        PrometheusMetric::new(
            "rustfs_dial9_disk_usage_bytes",
            MetricType::Gauge,
            "Current disk usage by dial9 trace files",
            stats.disk_usage_bytes as f64,
        ),
        PrometheusMetric::new(
            "rustfs_dial9_errors_total",
            MetricType::Counter,
            "Total number of dial9 telemetry setup errors",
            stats.errors_total as f64,
        ),
    ]);

    metrics
}

fn bool_gauge(value: bool) -> f64 {
    if value { 1.0 } else { 0.0 }
}

/// Collect dial9 metrics from the current runtime snapshot.
///
/// Reads cached atomics only; performs no I/O.
pub fn collect_current_dial9_metrics() -> Vec<PrometheusMetric> {
    let snapshot = runtime_stats_snapshot();
    let stats = Dial9Stats {
        errors_total: snapshot.errors_total,
        disk_usage_bytes: snapshot.disk_usage_bytes,
        active_sessions: snapshot.active_sessions,
    };

    collect_dial9_metrics(&stats)
}

/// Whether dial9 telemetry is actually running: compiled in *and* configured.
pub fn is_dial9_enabled() -> bool {
    is_enabled()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn metric_names(metrics: &[PrometheusMetric]) -> Vec<String> {
        metrics.iter().map(|m| m.name.to_string()).collect()
    }

    #[test]
    fn always_reports_support_and_intent() {
        let names = metric_names(&collect_dial9_metrics(&Dial9Stats::default()));
        assert!(names.iter().any(|n| n == "rustfs_dial9_supported"));
        assert!(names.iter().any(|n| n == "rustfs_dial9_configured"));
    }

    #[test]
    fn omits_session_metrics_when_telemetry_is_not_running() {
        // Under `cargo test` dial9 is neither compiled in nor configured, so the
        // collector must stop after the two compile-time/intent gauges rather
        // than publish zeroed session series.
        if is_enabled() {
            return;
        }
        let metrics = collect_dial9_metrics(&Dial9Stats::default());
        assert_eq!(metrics.len(), 2);
        let names = metric_names(&metrics);
        assert!(!names.iter().any(|n| n == "rustfs_dial9_disk_usage_bytes"));
        assert!(!names.iter().any(|n| n == "rustfs_dial9_active_sessions"));
    }

    #[test]
    fn supported_gauge_tracks_compile_time_feature() {
        let metrics = collect_dial9_metrics(&Dial9Stats::default());
        let supported = metrics
            .iter()
            .find(|m| m.name == "rustfs_dial9_supported")
            .expect("supported gauge is always exported");
        assert_eq!(supported.value, bool_gauge(cfg!(feature = "dial9")));
    }

    #[test]
    fn bool_gauge_maps_to_one_and_zero() {
        assert_eq!(bool_gauge(true), 1.0);
        assert_eq!(bool_gauge(false), 0.0);
    }
}
