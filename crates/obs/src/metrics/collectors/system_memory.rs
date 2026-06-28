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
//! This module provides both system-level and process-level memory metrics,
//! with process-level metrics migrated from `rustfs-obs::system`.

use crate::metrics::report::PrometheusMetric;
use crate::metrics::schema::system_memory::*;
use crate::metrics::schema::system_process::{PROCESS_RESIDENT_MEMORY_BYTES_MD, PROCESS_VIRTUAL_MEMORY_BYTES_MD};
use std::borrow::Cow;

/// System memory statistics.
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

/// Process memory statistics.
///
/// Contains memory usage metrics for a specific process.
#[derive(Debug, Clone, Default)]
pub struct ProcessMemoryStats {
    /// Resident memory size in bytes
    pub resident: u64,
    /// Virtual memory size in bytes
    pub virtual_mem: u64,
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

/// Collects process memory metrics from the given stats.
///
/// Uses the metric descriptors from `metrics_type::system_process` module.
/// Returns a vector of Prometheus metrics for process memory statistics.
///
/// # Arguments
///
/// * `stats` - Process memory statistics
/// * `labels` - Optional additional labels (e.g., process attributes)
pub fn collect_process_memory_metrics(
    stats: &ProcessMemoryStats,
    labels: Option<&[(&'static str, Cow<'static, str>)]>,
) -> Vec<PrometheusMetric> {
    let mut resident_metric = PrometheusMetric::from_descriptor(&PROCESS_RESIDENT_MEMORY_BYTES_MD, stats.resident as f64);
    let mut virtual_metric = PrometheusMetric::from_descriptor(&PROCESS_VIRTUAL_MEMORY_BYTES_MD, stats.virtual_mem as f64);

    if let Some(l) = labels {
        resident_metric.labels.extend(l.iter().map(|(k, v)| (*k, v.clone())));
        virtual_metric.labels.extend(l.iter().map(|(k, v)| (*k, v.clone())));
    }

    vec![resident_metric, virtual_metric]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::report::report_metrics;

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
        assert!(metrics.iter().all(|m| m.name.starts_with("rustfs_system_memory_")));
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

    #[test]
    fn test_collect_process_memory_metrics() {
        let stats = ProcessMemoryStats {
            resident: 512 * 1024 * 1024,         // 512 MB
            virtual_mem: 2 * 1024 * 1024 * 1024, // 2 GB
        };

        let metrics = collect_process_memory_metrics(&stats, None);
        report_metrics(&metrics);

        assert_eq!(metrics.len(), 2);
    }

    #[test]
    fn test_collect_process_memory_metrics_with_labels() {
        let stats = ProcessMemoryStats {
            resident: 256 * 1024 * 1024,
            virtual_mem: 1024 * 1024 * 1024,
        };

        let labels = vec![("process_pid", Cow::Borrowed("12345"))];

        let metrics = collect_process_memory_metrics(&stats, Some(&labels));
        assert_eq!(metrics.len(), 2);

        for metric in &metrics {
            assert_eq!(metric.labels.len(), 1);
        }
    }
}
