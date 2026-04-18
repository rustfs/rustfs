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
//! let pid = sysinfo::get_current_pid().unwrap();
//! let collector = GpuCollector::new(pid)?;
//! let stats = collector.collect()?;
//! let metrics = collect_gpu_metrics(&stats, &labels);
//! ```

use crate::metrics::report::PrometheusMetric;

use crate::metrics::schema::system_gpu::PROCESS_GPU_MEMORY_USAGE_MD;

use nvml_wrapper::Nvml;

use nvml_wrapper::enums::device::UsedGpuMemory;

use sysinfo::Pid;

use std::borrow::Cow;

use thiserror::Error;

use tracing::warn;

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
    /// let pid = sysinfo::get_current_pid().unwrap();
    /// let collector = GpuCollector::new(pid)?;
    /// ```
    pub fn new(pid: Pid) -> Result<Self, GpuError> {
        let nvml = Nvml::init().map_err(|e| GpuError::InitError(e.to_string()))?;
        Ok(GpuCollector { nvml, pid })
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
        if let Ok(device) = self.nvml.device_by_index(0) {
            if let Ok(gpu_stats) = device.running_compute_processes() {
                for stat in gpu_stats.iter() {
                    if stat.pid == self.pid.as_u32() {
                        let memory_used = match stat.used_gpu_memory {
                            UsedGpuMemory::Used(bytes) => bytes,
                            UsedGpuMemory::Unavailable => 0,
                        };
                        return Ok(GpuStats {
                            memory_usage: memory_used,
                        });
                    }
                }
            } else {
                warn!("Could not get GPU stats, recording 0 for GPU memory usage");
            }
        } else {
            return Err(GpuError::DeviceError("No GPU device found".to_string()));
        }

        // Process not found in GPU process list, return 0 usage
        Ok(GpuStats { memory_usage: 0 })
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
}
