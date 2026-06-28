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
//!
//! This module also provides process attribute collection for use as
//! metric labels, migrated from `rustfs-obs::system`.

use crate::metrics::report::PrometheusMetric;
use crate::metrics::schema::system_process::*;
use std::borrow::Cow;
use sysinfo::{Pid, ProcessStatus, System};

/// Process attributes used as metric labels.
///
/// Contains identifying information about the process being monitored.
#[derive(Debug, Clone)]
pub struct ProcessAttributes {
    /// Process ID
    pub pid: u32,
    /// Executable name (e.g., "rustfs")
    pub executable_name: String,
    /// Full path to the executable
    pub executable_path: String,
    /// Full command line with arguments
    pub command: String,
}

impl ProcessAttributes {
    /// Creates a new instance by reading from the current process.
    ///
    /// # Errors
    ///
    /// Returns an error if the current process PID cannot be determined
    /// or if process information cannot be retrieved.
    pub fn current() -> Result<Self, ProcessAttributeError> {
        let pid = sysinfo::get_current_pid().map_err(|e| ProcessAttributeError::PidError(e.to_string()))?;
        Self::from_pid(pid)
    }

    /// Creates a new instance for a specific PID.
    ///
    /// # Arguments
    ///
    /// * `pid` - The process ID to query
    ///
    /// # Errors
    ///
    /// Returns an error if the process does not exist or information
    /// cannot be retrieved.
    pub fn from_pid(pid: Pid) -> Result<Self, ProcessAttributeError> {
        let mut system = System::new();
        system.refresh_processes(sysinfo::ProcessesToUpdate::Some(&[pid]), true);

        let process = system
            .process(pid)
            .ok_or_else(|| ProcessAttributeError::ProcessNotFound(pid.as_u32()))?;

        Ok(ProcessAttributes {
            pid: pid.as_u32(),
            executable_name: process.name().to_string_lossy().to_string(),
            executable_path: process.exe().map(|p| p.to_string_lossy().to_string()).unwrap_or_default(),
            command: process
                .cmd()
                .iter()
                .map(|s| s.to_string_lossy().into_owned())
                .collect::<Vec<_>>()
                .join(" "),
        })
    }

    /// Converts attributes to Prometheus metric labels.
    pub fn to_labels(&self) -> Vec<(&'static str, Cow<'static, str>)> {
        vec![
            ("process_pid", Cow::Owned(self.pid.to_string())),
            ("process_executable_name", Cow::Owned(self.executable_name.clone())),
            ("process_executable_path", Cow::Owned(self.executable_path.clone())),
            ("process_command", Cow::Owned(self.command.clone())),
        ]
    }
}

/// Errors that can occur when collecting process attributes.
#[derive(Debug, Clone)]
pub enum ProcessAttributeError {
    /// Failed to get current process PID
    PidError(String),
    /// Process not found
    ProcessNotFound(u32),
}

impl std::fmt::Display for ProcessAttributeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::PidError(e) => write!(f, "Failed to get current PID: {}", e),
            Self::ProcessNotFound(pid) => write!(f, "Process not found: {}", pid),
        }
    }
}

impl std::error::Error for ProcessAttributeError {}

/// Process status enumeration.
///
/// Maps `sysinfo::ProcessStatus` to a simpler representation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ProcessStatusType {
    /// Process is currently running
    Running = 0,
    /// Process is sleeping (waiting for I/O or event)
    Sleeping = 1,
    /// Process is a zombie (terminated but not reaped)
    Zombie = 2,
    /// Process is in some other state
    #[default]
    Other = 3,
}

impl From<ProcessStatus> for ProcessStatusType {
    fn from(status: ProcessStatus) -> Self {
        match status {
            ProcessStatus::Run => ProcessStatusType::Running,
            ProcessStatus::Sleep => ProcessStatusType::Sleeping,
            ProcessStatus::Zombie => ProcessStatusType::Zombie,
            _ => ProcessStatusType::Other,
        }
    }
}

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
    /// Process status
    pub status: ProcessStatusType,
    /// Process status value (numeric)
    pub status_value: i64,
}

/// Collects process metrics from the given stats.
///
/// Returns a vector of Prometheus metrics for process statistics.
pub fn collect_process_metrics(stats: &ProcessStats) -> Vec<PrometheusMetric> {
    let mut metrics = vec![
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
    ];

    // Add process status metric
    let mut status_metric = PrometheusMetric::from_descriptor(&PROCESS_STATUS_MD, stats.status_value as f64);
    status_metric
        .labels
        .push(("status", Cow::Owned(format!("{:?}", stats.status))));
    metrics.push(status_metric);

    metrics
}

/// Collects process attributes for the current process.
///
/// This is a convenience function that wraps `ProcessAttributes::current()`.
pub fn collect_process_attributes() -> Result<ProcessAttributes, ProcessAttributeError> {
    ProcessAttributes::current()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::report::report_metrics;

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
            status: ProcessStatusType::Running,
            status_value: 0,
        };

        let metrics = collect_process_metrics(&stats);
        report_metrics(&metrics);

        // 17 original metrics + 1 status metric = 18
        assert_eq!(metrics.len(), 18);

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

        // Verify status metric
        let status_name = PROCESS_STATUS_MD.get_full_metric_name();
        let status_metric = metrics.iter().find(|m| m.name == status_name);
        assert!(status_metric.is_some());
        assert_eq!(status_metric.map(|m| m.value), Some(0.0));
    }

    #[test]
    fn test_collect_process_metrics_default() {
        let stats = ProcessStats::default();
        let metrics = collect_process_metrics(&stats);

        // 17 original metrics + 1 status metric = 18
        assert_eq!(metrics.len(), 18);
    }

    #[test]
    fn test_process_attributes_current() {
        // This test should succeed as we're querying the current process
        let result = collect_process_attributes();
        assert!(result.is_ok());

        let attrs = result.unwrap();
        assert!(attrs.pid > 0);
        assert!(!attrs.executable_name.is_empty());
    }

    #[test]
    fn test_process_status_conversion() {
        assert_eq!(ProcessStatusType::from(ProcessStatus::Run), ProcessStatusType::Running);
        assert_eq!(ProcessStatusType::from(ProcessStatus::Sleep), ProcessStatusType::Sleeping);
        assert_eq!(ProcessStatusType::from(ProcessStatus::Zombie), ProcessStatusType::Zombie);
    }

    #[test]
    fn test_process_attributes_to_labels() {
        let attrs = ProcessAttributes {
            pid: 12345,
            executable_name: "rustfs".to_string(),
            executable_path: "/usr/bin/rustfs".to_string(),
            command: "rustfs server /data".to_string(),
        };

        let labels = attrs.to_labels();
        assert_eq!(labels.len(), 4);
        assert_eq!(labels[0].0, "process_pid");
        assert_eq!(labels[0].1, "12345");
    }
}
