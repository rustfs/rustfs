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

use crate::metrics::report::PrometheusMetric;
use crate::metrics::schema::compression::*;

/// Cluster-level compression statistics.
#[derive(Debug, Clone, Default)]
pub struct CompressionClusterStats {
    pub original_bytes_total: u64,
    pub compressed_bytes_total: u64,
    pub bytes_saved_total: u64,
    pub compression_ratio: f64,
    pub compression_operations_total: u64,
}

/// Collect cluster-level compression metrics from cluster stats.
pub fn collect_compression_cluster_metrics(stats: &CompressionClusterStats) -> Vec<PrometheusMetric> {
    vec![
        PrometheusMetric::from_descriptor(&COMPRESSION_OPERATIONS_TOTAL, stats.compression_operations_total as f64),
        PrometheusMetric::from_descriptor(&COMPRESSION_BYTES_ORIGINAL_TOTAL, stats.original_bytes_total as f64),
        PrometheusMetric::from_descriptor(&COMPRESSION_BYTES_COMPRESSED_TOTAL, stats.compressed_bytes_total as f64),
        PrometheusMetric::from_descriptor(&COMPRESSION_BYTES_SAVED_TOTAL, stats.bytes_saved_total as f64),
        PrometheusMetric::from_descriptor(&COMPRESSION_RATIO, stats.compression_ratio),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::report::report_metrics;

    #[test]
    fn test_collect_compression_cluster_metrics() {
        let stats = CompressionClusterStats {
            original_bytes_total: 1000,
            compressed_bytes_total: 700,
            bytes_saved_total: 300,
            compression_ratio: 0.7,
            compression_operations_total: 10,
        };

        let metrics = collect_compression_cluster_metrics(&stats);
        report_metrics(&metrics);

        assert_eq!(metrics.len(), 5);

        // Verify original bytes
        let name = COMPRESSION_BYTES_ORIGINAL_TOTAL.get_full_metric_name();
        let m = metrics.iter().find(|m| m.name == name && m.value == 1000.0);
        assert!(m.is_some(), "missing or wrong value for original_bytes_total");

        // Verify compressed bytes
        let name = COMPRESSION_BYTES_COMPRESSED_TOTAL.get_full_metric_name();
        let m = metrics.iter().find(|m| m.name == name && m.value == 700.0);
        assert!(m.is_some(), "missing or wrong value for compressed_bytes_total");

        // Verify saved bytes
        let name = COMPRESSION_BYTES_SAVED_TOTAL.get_full_metric_name();
        let m = metrics.iter().find(|m| m.name == name && m.value == 300.0);
        assert!(m.is_some(), "missing or wrong value for bytes_saved_total");

        // Verify compression ratio
        let name = COMPRESSION_RATIO.get_full_metric_name();
        let m = metrics.iter().find(|m| m.name == name && m.value == 0.7);
        assert!(m.is_some(), "missing or wrong value for compression_ratio");

        // Verify operations total
        let name = COMPRESSION_OPERATIONS_TOTAL.get_full_metric_name();
        let m = metrics.iter().find(|m| m.name == name && m.value == 10.0);
        assert!(m.is_some(), "missing or wrong value for compression_operations_total");
    }

    #[test]
    fn test_collect_compression_cluster_metrics_empty() {
        let stats = CompressionClusterStats::default();

        let metrics = collect_compression_cluster_metrics(&stats);
        report_metrics(&metrics);

        assert_eq!(metrics.len(), 5);

        // All values should be zero
        for metric in &metrics {
            assert_eq!(metric.value, 0.0, "expected zero value for {}", metric.name);
            assert!(metric.labels.is_empty());
        }
    }

    #[test]
    fn test_compression_cluster_stats_default() {
        let stats = CompressionClusterStats::default();
        assert_eq!(stats.original_bytes_total, 0);
        assert_eq!(stats.compressed_bytes_total, 0);
        assert_eq!(stats.bytes_saved_total, 0);
        assert_eq!(stats.compression_ratio, 0.0);
        assert_eq!(stats.compression_operations_total, 0);
    }
}
