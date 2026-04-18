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

//! Scanner metrics collector.
//!
//! Collects background scanner metrics including bucket scans,
//! directory scans, and object scans.

use crate::metrics::report::PrometheusMetric;
use crate::metrics::schema::scanner::{
    SCANNER_BUCKET_SCANS_FINISHED_MD, SCANNER_BUCKET_SCANS_STARTED_MD, SCANNER_DIRECTORIES_SCANNED_MD,
    SCANNER_LAST_ACTIVITY_SECONDS_MD, SCANNER_OBJECTS_SCANNED_MD, SCANNER_VERSIONS_SCANNED_MD,
};

/// Scanner statistics.
#[derive(Debug, Clone, Default)]
pub struct ScannerStats {
    /// Number of bucket scans finished
    pub bucket_scans_finished: u64,
    /// Number of bucket scans started
    pub bucket_scans_started: u64,
    /// Number of directories scanned
    pub directories_scanned: u64,
    /// Number of objects scanned
    pub objects_scanned: u64,
    /// Number of object versions scanned
    pub versions_scanned: u64,
    /// Seconds since last scanner activity
    pub last_activity_seconds: u64,
}

/// Collects scanner metrics from the given stats.
///
/// Uses the metric descriptors from `metrics_type::scanner` module.
/// Returns a vector of Prometheus metrics for scanner statistics.
pub fn collect_scanner_metrics(stats: &ScannerStats) -> Vec<PrometheusMetric> {
    vec![
        PrometheusMetric::from_descriptor(&SCANNER_BUCKET_SCANS_FINISHED_MD, stats.bucket_scans_finished as f64),
        PrometheusMetric::from_descriptor(&SCANNER_BUCKET_SCANS_STARTED_MD, stats.bucket_scans_started as f64),
        PrometheusMetric::from_descriptor(&SCANNER_DIRECTORIES_SCANNED_MD, stats.directories_scanned as f64),
        PrometheusMetric::from_descriptor(&SCANNER_OBJECTS_SCANNED_MD, stats.objects_scanned as f64),
        PrometheusMetric::from_descriptor(&SCANNER_VERSIONS_SCANNED_MD, stats.versions_scanned as f64),
        PrometheusMetric::from_descriptor(&SCANNER_LAST_ACTIVITY_SECONDS_MD, stats.last_activity_seconds as f64),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::report::report_metrics;

    #[test]
    fn test_collect_scanner_metrics() {
        let stats = ScannerStats {
            bucket_scans_finished: 100,
            bucket_scans_started: 100,
            directories_scanned: 50000,
            objects_scanned: 1000000,
            versions_scanned: 1500000,
            last_activity_seconds: 30,
        };

        let metrics = collect_scanner_metrics(&stats);
        report_metrics(&metrics);

        assert_eq!(metrics.len(), 6);

        let objects = metrics.iter().find(|m| m.value == 1000000.0);
        assert!(objects.is_some());

        let last_activity = metrics.iter().find(|m| m.value == 30.0);
        assert!(last_activity.is_some());
    }

    #[test]
    fn test_collect_scanner_metrics_default() {
        let stats = ScannerStats::default();
        let metrics = collect_scanner_metrics(&stats);

        assert_eq!(metrics.len(), 6);
        for metric in &metrics {
            assert_eq!(metric.value, 0.0);
            assert!(metric.labels.is_empty());
        }
    }
}
