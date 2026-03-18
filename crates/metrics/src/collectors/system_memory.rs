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

//! System memory metrics collector.
//!
//! Collects memory-related metrics including total, used, free,
//! buffers, cache, shared, and available memory.
//!
//! This collector reuses the metric descriptors defined in `metrics_type::system_memory`
//! to avoid duplication of metric names, types, and help text.

use crate::format::PrometheusMetric;
use crate::metrics_type::system_memory::*;

/// Memory statistics for a node.
#[derive(Debug, Clone, Default)]
pub struct MemoryStats {
    /// Total memory in bytes
    pub total: u64,
    /// Used memory in bytes
    pub used: u64,
    /// Used memory percentage (0-100)
    pub used_perc: f64,
    /// Free memory in bytes
    pub free: u64,
    /// Buffer memory in bytes
    pub buffers: u64,
    /// Cache memory in bytes
    pub cache: u64,
    /// Shared memory in bytes
    pub shared: u64,
    /// Available memory in bytes
    pub available: u64,
}

/// Collects memory metrics from the given stats.
///
/// Uses the metric descriptors from `metrics_type::system_memory` module.
/// Returns a vector of Prometheus metrics for memory statistics.
pub fn collect_memory_metrics(stats: &MemoryStats) -> Vec<PrometheusMetric> {
    vec![
        PrometheusMetric::from_descriptor(&MEM_TOTAL_MD, stats.total as f64),
        PrometheusMetric::from_descriptor(&MEM_USED_MD, stats.used as f64),
        PrometheusMetric::from_descriptor(&MEM_USED_PERC_MD, stats.used_perc),
        PrometheusMetric::from_descriptor(&MEM_FREE_MD, stats.free as f64),
        PrometheusMetric::from_descriptor(&MEM_BUFFERS_MD, stats.buffers as f64),
        PrometheusMetric::from_descriptor(&MEM_CACHE_MD, stats.cache as f64),
        PrometheusMetric::from_descriptor(&MEM_SHARED_MD, stats.shared as f64),
        PrometheusMetric::from_descriptor(&MEM_AVAILABLE_MD, stats.available as f64),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::format::report_metrics;

    #[test]
    fn test_collect_memory_metrics() {
        let stats = MemoryStats {
            total: 16 * 1024 * 1024 * 1024, // 16 GB
            used: 8 * 1024 * 1024 * 1024,   // 8 GB
            used_perc: 50.0,
            free: 4 * 1024 * 1024 * 1024,      // 4 GB
            buffers: 1024 * 1024 * 512,        // 512 MB
            cache: 2 * 1024 * 1024 * 1024,     // 2 GB
            shared: 1024 * 1024 * 256,         // 256 MB
            available: 6 * 1024 * 1024 * 1024, // 6 GB
        };

        let metrics = collect_memory_metrics(&stats);
        report_metrics(&metrics);

        assert_eq!(metrics.len(), 8);
        assert!(metrics.iter().all(|m| m.name.starts_with("gauge.rustfs_system_memory_")));
    }

    #[test]
    fn test_collect_memory_metrics_default() {
        let stats = MemoryStats::default();
        let metrics = collect_memory_metrics(&stats);

        assert_eq!(metrics.len(), 8);
        for metric in &metrics {
            assert_eq!(metric.value, 0.0);
            assert!(metric.labels.is_empty());
        }
    }
}
