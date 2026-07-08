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

//! System GPU metrics collector.
//!
//! Collects GPU memory usage metrics using NVML library.
//! This module is only available when `gpu` feature is enabled.
//!
//! # Example
//!
//! ```ignore
//! use rustfs_obs::metrics::collectors::{GpuCollector, collect_gpu_metrics};
//! use sysinfo::Pid;
//!
//! let pid = sysinfo::get_current_pid().expect("operation should succeed");
//! let collector = GpuCollector::new(pid)?;
//! let stats = collector.collect()?;
//! let metrics = collect_gpu_metrics(&stats, &labels);
//! ```

use crate::metrics::report::PrometheusMetric;

use crate::metrics::schema::system_gpu::PROCESS_GPU_MEMORY_USAGE_MD;

use nvml_wrapper::Nvml;

use nvml_wrapper::enums::device::UsedGpuMemory;
use nvml_wrapper::struct_wrappers::device::ProcessInfo;

use sysinfo::Pid;

use std::borrow::Cow;

use thiserror::Error;

use tracing::{debug, warn};

const LOG_COMPONENT_OBS: &str = "obs";
const LOG_SUBSYSTEM_GPU_METRICS: &str = "gpu_metrics";
const EVENT_GPU_METRICS_STATE: &str = "gpu_metrics_state";

/// GPU statistics.
///
/// Contains GPU memory usage metrics for the monitored process.

#[derive(Debug, Clone, Default)]
pub struct GpuStats {
    /// GPU memory usage in bytes
    pub memory_usage: u64,
}

/// GPU collector error types.

#[derive(Debug, Error)]
pub enum GpuError {
    /// GPU initialization failed
    #[error("GPU initialization failed: {0}")]
    InitError(String),

    /// GPU device access error
    #[error("GPU device error: {0}")]
    DeviceError(String),

    /// Process not found in GPU process list
    #[error("Process not found in GPU process list")]
    ProcessNotFound,
}

/// GPU metrics collector.
///
/// Collects GPU memory usage metrics for a specific process using NVML.
pub struct GpuCollector {
    /// NVML instance for GPU access
    nvml: Nvml,
    /// Process ID to monitor
    pid: Pid,
    /// Cached device count so we only probe NVML topology once at init time.
    device_count: u32,
}

impl GpuCollector {
    /// Creates a new GPU collector for the specified process.
    ///
    /// # Arguments
    ///
    /// * `pid` - The process ID to monitor
    ///
    /// # Errors
    ///
    /// Returns an error if NVML initialization fails.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use rustfs_obs::metrics::collectors::GpuCollector;
    /// use sysinfo::Pid;
    ///
    /// let pid = sysinfo::get_current_pid().expect("operation should succeed");
    /// let collector = GpuCollector::new(pid)?;
    /// ```
    pub fn new(pid: Pid) -> Result<Self, GpuError> {
        let nvml = Nvml::init().map_err(|e| GpuError::InitError(e.to_string()))?;
        let device_count = nvml.device_count().map_err(|e| GpuError::DeviceError(e.to_string()))?;

        if device_count == 0 {
            return Err(GpuError::DeviceError("No GPU device found".to_string()));
        }

        Ok(GpuCollector { nvml, pid, device_count })
    }

    /// Collects GPU metrics for the monitored process.
    ///
    /// Returns GPU memory usage statistics for the process.
    ///
    /// # Errors
    ///
    /// Returns an error if GPU device access fails.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let stats = collector.collect()?;
    /// println!("GPU memory usage: {} bytes", stats.memory_usage);
    /// ```
    pub fn collect(&self) -> Result<GpuStats, GpuError> {
        let mut total_memory_usage = 0_u64;
        let mut process_found = false;

        for device_index in 0..self.device_count {
            let device = match self.nvml.device_by_index(device_index) {
                Ok(device) => device,
                Err(e) => {
                    warn!(
                        event = EVENT_GPU_METRICS_STATE,
                        component = LOG_COMPONENT_OBS,
                        subsystem = LOG_SUBSYSTEM_GPU_METRICS,
                        result = "device_unavailable",
                        device_index,
                        error = %e,
                        "gpu metrics state changed"
                    );
                    continue;
                }
            };

            let compute_processes = match device.running_compute_processes() {
                Ok(processes) => processes,
                Err(e) => {
                    warn!(
                        event = EVENT_GPU_METRICS_STATE,
                        component = LOG_COMPONENT_OBS,
                        subsystem = LOG_SUBSYSTEM_GPU_METRICS,
                        result = "process_stats_unavailable",
                        device_index,
                        process_kind = "compute",
                        error = %e,
                        fallback_memory_usage = 0,
                        "gpu metrics state changed"
                    );
                    Vec::new()
                }
            };

            let graphics_processes = match device.running_graphics_processes() {
                Ok(processes) => processes,
                Err(e) => {
                    warn!(
                        event = EVENT_GPU_METRICS_STATE,
                        component = LOG_COMPONENT_OBS,
                        subsystem = LOG_SUBSYSTEM_GPU_METRICS,
                        result = "process_stats_unavailable",
                        device_index,
                        process_kind = "graphics",
                        error = %e,
                        fallback_memory_usage = 0,
                        "gpu metrics state changed"
                    );
                    Vec::new()
                }
            };

            if let Some(device_memory_usage) =
                sum_device_process_memory_usage(&compute_processes, &graphics_processes, self.pid.as_u32())
            {
                process_found = true;
                total_memory_usage = total_memory_usage.saturating_add(device_memory_usage);
            }
        }

        if process_found {
            return Ok(GpuStats {
                memory_usage: total_memory_usage,
            });
        }

        debug!(
            event = EVENT_GPU_METRICS_STATE,
            component = LOG_COMPONENT_OBS,
            subsystem = LOG_SUBSYSTEM_GPU_METRICS,
            result = "process_not_found",
            pid = self.pid.as_u32(),
            fallback_memory_usage = 0,
            "gpu metrics state changed"
        );

        Ok(GpuStats { memory_usage: 0 })
    }
}

fn sum_device_process_memory_usage(
    compute_processes: &[ProcessInfo],
    graphics_processes: &[ProcessInfo],
    pid: u32,
) -> Option<u64> {
    let compute_memory = sum_matching_process_memory_usage(compute_processes, pid);
    let graphics_memory = sum_matching_process_memory_usage(graphics_processes, pid);

    match (compute_memory, graphics_memory) {
        (None, None) => None,
        (compute_memory, graphics_memory) => Some(compute_memory.unwrap_or(0).saturating_add(graphics_memory.unwrap_or(0))),
    }
}

fn sum_matching_process_memory_usage(processes: &[ProcessInfo], pid: u32) -> Option<u64> {
    let mut total_memory_usage = 0_u64;
    let mut matched = false;

    for process in processes {
        if process.pid == pid {
            matched = true;
            total_memory_usage = total_memory_usage.saturating_add(process_used_gpu_memory(&process.used_gpu_memory));
        }
    }

    matched.then_some(total_memory_usage)
}

fn process_used_gpu_memory(used_gpu_memory: &UsedGpuMemory) -> u64 {
    match used_gpu_memory {
        UsedGpuMemory::Used(bytes) => *bytes,
        UsedGpuMemory::Unavailable => 0,
    }
}

/// Converts GPU stats to Prometheus metrics.
///
/// # Arguments
///
/// * `stats` - GPU statistics to convert
/// * `labels` - Metric labels (typically from ProcessAttributes)
///
/// # Returns
///
/// A vector of Prometheus metrics.
///
/// # Example
///
/// ```ignore
/// use rustfs_obs::metrics::collectors::{GpuStats, collect_gpu_metrics};
///
/// let stats = GpuStats { memory_usage: 1024 };
/// let labels = vec![("process_pid", Cow::Borrowed("1234"))];
/// let metrics = collect_gpu_metrics(&stats, &labels);
/// ```
pub fn collect_gpu_metrics(stats: &GpuStats, labels: &[(&'static str, Cow<'static, str>)]) -> Vec<PrometheusMetric> {
    let mut metric = PrometheusMetric::from_descriptor(&PROCESS_GPU_MEMORY_USAGE_MD, stats.memory_usage as f64);
    metric.labels.extend(labels.iter().map(|(k, v)| (*k, v.clone())));
    vec![metric]
}

#[cfg(test)]
mod tests {
    use super::*;

    fn process_info(pid: u32, used_gpu_memory: UsedGpuMemory) -> ProcessInfo {
        ProcessInfo {
            pid,
            used_gpu_memory,
            gpu_instance_id: None,
            compute_instance_id: None,
        }
    }

    #[test]
    fn test_gpu_stats_default() {
        let stats = GpuStats::default();
        assert_eq!(stats.memory_usage, 0);
    }

    #[test]
    fn test_gpu_error_display() {
        let err = GpuError::InitError("test error".to_string());
        assert!(err.to_string().contains("test error"));

        let err = GpuError::DeviceError("device error".to_string());
        assert!(err.to_string().contains("device error"));

        let err = GpuError::ProcessNotFound;
        assert!(err.to_string().contains("Process not found"));
    }

    #[test]
    fn sums_matching_process_memory_and_treats_unavailable_as_zero() {
        let processes = vec![
            process_info(7, UsedGpuMemory::Used(128)),
            process_info(7, UsedGpuMemory::Unavailable),
            process_info(9, UsedGpuMemory::Used(2048)),
        ];

        assert_eq!(sum_matching_process_memory_usage(&processes, 7), Some(128));
        assert_eq!(sum_matching_process_memory_usage(&processes, 9), Some(2048));
        assert_eq!(sum_matching_process_memory_usage(&processes, 42), None);
    }

    #[test]
    fn sums_compute_and_graphics_process_memory_for_same_device() {
        let compute_processes = vec![process_info(42, UsedGpuMemory::Used(256))];
        let graphics_processes = vec![
            process_info(42, UsedGpuMemory::Used(512)),
            process_info(77, UsedGpuMemory::Used(1024)),
        ];

        assert_eq!(sum_device_process_memory_usage(&compute_processes, &graphics_processes, 42), Some(768));
        assert_eq!(sum_device_process_memory_usage(&compute_processes, &graphics_processes, 99), None);
    }

    #[test]
    fn sums_process_memory_across_multiple_devices() {
        let device_zero_compute = vec![process_info(7, UsedGpuMemory::Used(256))];
        let device_zero_graphics = vec![];
        let device_one_compute = vec![];
        let device_one_graphics = vec![process_info(7, UsedGpuMemory::Used(1024))];

        let total = [
            sum_device_process_memory_usage(&device_zero_compute, &device_zero_graphics, 7),
            sum_device_process_memory_usage(&device_one_compute, &device_one_graphics, 7),
        ]
        .into_iter()
        .flatten()
        .sum::<u64>();

        assert_eq!(total, 1280);
    }
}
