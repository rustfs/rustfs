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

//! System process metrics collector.
//!
//! Collects process-level metrics including file descriptors, memory,
//! syscalls, and runtime statistics.

use crate::format::PrometheusMetric;
use crate::metrics_type::system_process::*;

/// Process statistics for the RustFS server process.
#[derive(Debug, Clone, Default)]
pub struct ProcessStats {
    /// Total read locks held
    pub locks_read_total: u64,
    /// Total write locks held
    pub locks_write_total: u64,
    /// Total CPU time in seconds
    pub cpu_total_seconds: f64,
    /// Total number of async tasks (goroutines equivalent)
    pub go_routine_total: u64,
    /// Total bytes read via read syscalls (rchar)
    pub io_rchar_bytes: u64,
    /// Total bytes actually read from storage
    pub io_read_bytes: u64,
    /// Total bytes written via write syscalls (wchar)
    pub io_wchar_bytes: u64,
    /// Total bytes actually written to storage
    pub io_write_bytes: u64,
    /// Process start time in seconds since Unix epoch
    pub start_time_seconds: u64,
    /// Process uptime in seconds
    pub uptime_seconds: u64,
    /// File descriptor limit
    pub file_descriptor_limit_total: u64,
    /// Open file descriptors count
    pub file_descriptor_open_total: u64,
    /// Total read syscalls
    pub syscall_read_total: u64,
    /// Total write syscalls
    pub syscall_write_total: u64,
    /// Resident memory size in bytes
    pub resident_memory_bytes: u64,
    /// Virtual memory size in bytes
    pub virtual_memory_bytes: u64,
    /// Maximum virtual memory size in bytes
    pub virtual_memory_max_bytes: u64,
}

/// Collects process metrics from the given stats.
///
/// Returns a vector of Prometheus metrics for process statistics.
pub fn collect_process_metrics(stats: &ProcessStats) -> Vec<PrometheusMetric> {
    vec![
        PrometheusMetric::from_descriptor(&PROCESS_LOCKS_READ_TOTAL_MD, stats.locks_read_total as f64),
        PrometheusMetric::from_descriptor(&PROCESS_LOCKS_WRITE_TOTAL_MD, stats.locks_write_total as f64),
        PrometheusMetric::from_descriptor(&PROCESS_CPU_TOTAL_SECONDS_MD, stats.cpu_total_seconds),
        PrometheusMetric::from_descriptor(&PROCESS_GO_ROUTINE_TOTAL_MD, stats.go_routine_total as f64),
        PrometheusMetric::from_descriptor(&PROCESS_IO_RCHAR_BYTES_MD, stats.io_rchar_bytes as f64),
        PrometheusMetric::from_descriptor(&PROCESS_IO_READ_BYTES_MD, stats.io_read_bytes as f64),
        PrometheusMetric::from_descriptor(&PROCESS_IO_WCHAR_BYTES_MD, stats.io_wchar_bytes as f64),
        PrometheusMetric::from_descriptor(&PROCESS_IO_WRITE_BYTES_MD, stats.io_write_bytes as f64),
        PrometheusMetric::from_descriptor(&PROCESS_START_TIME_SECONDS_MD, stats.start_time_seconds as f64),
        PrometheusMetric::from_descriptor(&PROCESS_UPTIME_SECONDS_MD, stats.uptime_seconds as f64),
        PrometheusMetric::from_descriptor(&PROCESS_FILE_DESCRIPTOR_LIMIT_TOTAL_MD, stats.file_descriptor_limit_total as f64),
        PrometheusMetric::from_descriptor(&PROCESS_FILE_DESCRIPTOR_OPEN_TOTAL_MD, stats.file_descriptor_open_total as f64),
        PrometheusMetric::from_descriptor(&PROCESS_SYSCALL_READ_TOTAL_MD, stats.syscall_read_total as f64),
        PrometheusMetric::from_descriptor(&PROCESS_SYSCALL_WRITE_TOTAL_MD, stats.syscall_write_total as f64),
        PrometheusMetric::from_descriptor(&PROCESS_RESIDENT_MEMORY_BYTES_MD, stats.resident_memory_bytes as f64),
        PrometheusMetric::from_descriptor(&PROCESS_VIRTUAL_MEMORY_BYTES_MD, stats.virtual_memory_bytes as f64),
        PrometheusMetric::from_descriptor(&PROCESS_VIRTUAL_MEMORY_MAX_BYTES_MD, stats.virtual_memory_max_bytes as f64),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::format::report_metrics;

    #[test]
    fn test_collect_process_metrics() {
        let stats = ProcessStats {
            locks_read_total: 100,
            locks_write_total: 50,
            cpu_total_seconds: 1234.56,
            go_routine_total: 200,
            io_rchar_bytes: 1024 * 1024 * 500,
            io_read_bytes: 1024 * 1024 * 400,
            io_wchar_bytes: 1024 * 1024 * 300,
            io_write_bytes: 1024 * 1024 * 250,
            start_time_seconds: 1700000000,
            uptime_seconds: 86400,
            file_descriptor_limit_total: 65536,
            file_descriptor_open_total: 1500,
            syscall_read_total: 100000,
            syscall_write_total: 50000,
            resident_memory_bytes: 1024 * 1024 * 512,
            virtual_memory_bytes: 1024 * 1024 * 1024,
            virtual_memory_max_bytes: 1024 * 1024 * 2048,
        };

        let metrics = collect_process_metrics(&stats);
        report_metrics(&metrics);

        assert_eq!(metrics.len(), 17);

        // Verify uptime
        let uptime_name = PROCESS_UPTIME_SECONDS_MD.get_full_metric_name();
        let uptime = metrics.iter().find(|m| m.name == uptime_name);
        assert!(uptime.is_some());
        assert_eq!(uptime.map(|m| m.value), Some(86400.0));

        // Verify file descriptors
        let fd_open_name = PROCESS_FILE_DESCRIPTOR_OPEN_TOTAL_MD.get_full_metric_name();
        let fd_open = metrics.iter().find(|m| m.name == fd_open_name);
        assert!(fd_open.is_some());
        assert_eq!(fd_open.map(|m| m.value), Some(1500.0));
    }

    #[test]
    fn test_collect_process_metrics_default() {
        let stats = ProcessStats::default();
        let metrics = collect_process_metrics(&stats);

        assert_eq!(metrics.len(), 17);
        for metric in &metrics {
            assert_eq!(metric.value, 0.0);
            assert!(metric.labels.is_empty());
        }
    }
}
