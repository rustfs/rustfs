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

//! Cgroup-aware resource detection for containerized environments.
//!
//! This module provides cgroup-aware detection of CPU and memory limits
//! for RustFS running in containerized environments (Kubernetes, Docker, etc.).
//!
//! # Platform Support
//!
//! - **Linux**: Full cgroup v1/v2 support
//! - **macOS/Windows**: Falls back to host values (cgroup not available)
//!
//! # Problem
//!
//! When RustFS runs in a container, `sysinfo` reports the host's total CPU cores
//! and memory, not the container's limits. This leads to:
//! - Over-provisioned Tokio worker threads and blocking threads
//! - Incorrect memory usage percentages in metrics
//! - Memory budgets based on host RAM instead of container limits
//!
//! # Solution
//!
//! On Linux, this module reads cgroup v1/v2 limits directly from the filesystem:
//! - CPU: `/sys/fs/cgroup/cpu.max` (v2) or `/sys/fs/cgroup/cpu/cpu.cfs_quota_us` (v1)
//! - Memory: `/sys/fs/cgroup/memory.max` (v2) or `/sys/fs/cgroup/memory/memory.limit_in_bytes` (v1)
//!
//! On other platforms, it returns None, allowing the caller to fall back to host values.

use std::sync::OnceLock;

/// Cached cgroup resource limits (computed once at startup).
static CGROUP_RESOURCES: OnceLock<CgroupResources> = OnceLock::new();

/// Detected cgroup resource limits.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CgroupResources {
    /// CPU quota in cores (e.g., 2 means 2 cores). None if unlimited.
    pub cpu_cores: Option<usize>,
    /// Memory limit in bytes. None if unlimited.
    pub memory_bytes: Option<u64>,
    /// Whether cgroup limits were detected.
    pub detected: bool,
}

// Linux-specific cgroup detection
#[cfg(target_os = "linux")]
mod linux {
    /// Cgroup v2 CPU limit path
    const CGROUP_V2_CPU_MAX_PATH: &str = "/sys/fs/cgroup/cpu.max";
    /// Cgroup v1 CPU quota path
    const CGROUP_V1_CPU_QUOTA_PATH: &str = "/sys/fs/cgroup/cpu/cpu.cfs_quota_us";
    /// Cgroup v1 CPU period path
    const CGROUP_V1_CPU_PERIOD_PATH: &str = "/sys/fs/cgroup/cpu/cpu.cfs_period_us";
    /// Cgroup v2 memory limit path
    const CGROUP_V2_MEMORY_MAX_PATH: &str = "/sys/fs/cgroup/memory.max";
    /// Cgroup v1 memory limit path
    const CGROUP_V1_MEMORY_LIMIT_PATH: &str = "/sys/fs/cgroup/memory/memory.limit_in_bytes";

    /// Read a file content and parse as u64, returning None on any error.
    fn read_u64_from_file(path: &str) -> Option<u64> {
        let content = std::fs::read_to_string(path).ok()?;
        let trimmed = content.trim();
        if trimmed.is_empty() || trimmed == "max" {
            return None;
        }
        trimmed.parse::<u64>().ok()
    }

    /// Detect CPU cores from cgroup v2 (`/sys/fs/cgroup/cpu.max`).
    ///
    /// Format: `"$MAX_PERIOD"` or `"$QUOTA $PERIOD"` where MAX means unlimited.
    fn detect_cgroup_v2_cpus() -> Option<usize> {
        let content = std::fs::read_to_string(CGROUP_V2_CPU_MAX_PATH).ok()?;
        let parts: Vec<&str> = content.split_whitespace().collect();

        match parts.len() {
            1 => {
                // Format: "$MAX" (unlimited) or "$QUOTA" with implicit period=100000
                if parts[0] == "max" {
                    return None; // Unlimited
                }
                // Some systems use single value as quota with default period
                let quota = parts[0].parse::<u64>().ok()?;
                if quota == 0 {
                    return None;
                }
                let period = 100_000u64; // Default cgroup period
                Some(quota.div_ceil(period) as usize)
            }
            2 => {
                // Format: "$QUOTA $PERIOD"
                if parts[0] == "max" {
                    return None; // Unlimited
                }
                let quota = parts[0].parse::<u64>().ok()?;
                let period = parts[1].parse::<u64>().ok()?;
                if quota == 0 || period == 0 {
                    return None;
                }
                Some(quota.div_ceil(period) as usize)
            }
            _ => None,
        }
    }

    /// Detect CPU cores from cgroup v1 (`/sys/fs/cgroup/cpu/cpu.cfs_quota_us`).
    fn detect_cgroup_v1_cpus() -> Option<usize> {
        let quota = read_u64_from_file(CGROUP_V1_CPU_QUOTA_PATH)?;
        if quota == 0 || quota == u64::MAX {
            return None; // Unlimited
        }

        let period = read_u64_from_file(CGROUP_V1_CPU_PERIOD_PATH).unwrap_or(100_000);
        if period == 0 {
            return None;
        }

        Some(quota.div_ceil(period) as usize)
    }

    /// Detect CPU cores from cgroup, trying v2 first, then v1.
    pub(super) fn detect_cgroup_cpus() -> Option<usize> {
        detect_cgroup_v2_cpus().or_else(detect_cgroup_v1_cpus)
    }

    /// Detect memory limit from cgroup v2 (`/sys/fs/cgroup/memory.max`).
    fn detect_cgroup_v2_memory() -> Option<u64> {
        let content = std::fs::read_to_string(CGROUP_V2_MEMORY_MAX_PATH).ok()?;
        let trimmed = content.trim();
        if trimmed == "max" || trimmed.is_empty() {
            return None; // Unlimited
        }
        trimmed.parse::<u64>().ok()
    }

    /// Detect memory limit from cgroup v1 (`/sys/fs/cgroup/memory/memory.limit_in_bytes`).
    fn detect_cgroup_v1_memory() -> Option<u64> {
        let limit = read_u64_from_file(CGROUP_V1_MEMORY_LIMIT_PATH)?;
        // cgroup v1 uses a very large value (2^63 or similar) to indicate "unlimited"
        if limit >= (1 << 62) {
            return None;
        }
        Some(limit)
    }

    /// Detect memory limit from cgroup, trying v2 first, then v1.
    pub(super) fn detect_cgroup_memory() -> Option<u64> {
        detect_cgroup_v2_memory().or_else(detect_cgroup_v1_memory)
    }
}

// Non-Linux platforms: cgroup not available
#[cfg(not(target_os = "linux"))]
mod linux {
    /// Cgroup detection not available on non-Linux platforms.
    pub(super) fn detect_cgroup_cpus() -> Option<usize> {
        None
    }

    /// Cgroup detection not available on non-Linux platforms.
    pub(super) fn detect_cgroup_memory() -> Option<u64> {
        None
    }
}

/// Detect all cgroup resources (called once and cached).
fn detect_cgroup_resources() -> CgroupResources {
    let cpu_cores = linux::detect_cgroup_cpus();
    let memory_bytes = linux::detect_cgroup_memory();
    let detected = cpu_cores.is_some() || memory_bytes.is_some();

    CgroupResources {
        cpu_cores,
        memory_bytes,
        detected,
    }
}

/// Get cached cgroup resource limits.
///
/// This function is safe to call from multiple threads. The detection is performed
/// once and the result is cached for the lifetime of the process.
///
/// # Platform Behavior
///
/// - **Linux**: Detects cgroup v1/v2 limits
/// - **macOS/Windows**: Returns default (no limits detected)
pub fn cgroup_resources() -> &'static CgroupResources {
    CGROUP_RESOURCES.get_or_init(detect_cgroup_resources)
}

/// Get effective CPU cores, considering cgroup limits.
///
/// Returns the minimum of host CPU count and cgroup CPU limit.
/// If cgroup limit is not set, returns the host CPU count.
///
/// # Platform Behavior
///
/// - **Linux**: Returns min(host, cgroup_limit)
/// - **macOS/Windows**: Returns host cores (no cgroup)
pub fn effective_cpu_cores(host_cores: usize) -> usize {
    let resources = cgroup_resources();
    match resources.cpu_cores {
        Some(cgroup_cores) => cgroup_cores.min(host_cores).max(1),
        None => host_cores.max(1),
    }
}

/// Get effective memory, considering cgroup limits.
///
/// Returns the minimum of host memory and cgroup memory limit.
/// If cgroup limit is not set, returns the host memory.
///
/// # Platform Behavior
///
/// - **Linux**: Returns min(host, cgroup_limit)
/// - **macOS/Windows**: Returns host memory (no cgroup)
pub fn effective_memory(host_memory: u64) -> u64 {
    let resources = cgroup_resources();
    match resources.memory_bytes {
        Some(cgroup_memory) => cgroup_memory.min(host_memory),
        None => host_memory,
    }
}

/// Get the memory basis (host or cgroup) for the effective memory.
///
/// # Platform Behavior
///
/// - **Linux**: Returns "cgroup" if cgroup limit is active, "host" otherwise
/// - **macOS/Windows**: Always returns "host"
pub fn memory_basis(host_memory: u64) -> &'static str {
    let resources = cgroup_resources();
    match resources.memory_bytes {
        Some(cgroup_memory) if cgroup_memory < host_memory => "cgroup",
        _ => "host",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cgroup_resources_default() {
        let resources = CgroupResources::default();
        assert_eq!(resources.cpu_cores, None);
        assert_eq!(resources.memory_bytes, None);
        assert!(!resources.detected);
    }

    #[test]
    fn test_effective_cpu_cores_no_cgroup() {
        // Without cgroup limits, should return host cores
        assert_eq!(effective_cpu_cores(8), 8);
        assert_eq!(effective_cpu_cores(1), 1);
    }

    #[test]
    fn test_effective_memory_no_cgroup() {
        // Without cgroup limits, should return host memory
        let host_mem = 8 * 1024 * 1024 * 1024;
        assert_eq!(effective_memory(host_mem), host_mem);
    }

    #[test]
    fn test_effective_cpu_cores_minimum_one() {
        // Should always return at least 1
        assert_eq!(effective_cpu_cores(0), 1);
    }

    #[test]
    fn test_memory_basis_no_cgroup() {
        let host_mem = 8 * 1024 * 1024 * 1024;
        assert_eq!(memory_basis(host_mem), "host");
    }

    #[test]
    fn test_cgroup_resources_cached() {
        // Multiple calls should return the same resources
        let res1 = cgroup_resources();
        let res2 = cgroup_resources();
        assert_eq!(res1, res2);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn test_linux_cgroup_detection() {
        // On Linux, cgroup detection should work
        let resources = cgroup_resources();
        // In CI/container environments, cgroup might be available
        // Just verify the struct is valid
        println!("Cgroup detected: {}", resources.detected);
        println!("CPU cores: {:?}", resources.cpu_cores);
        println!("Memory bytes: {:?}", resources.memory_bytes);
    }

    #[cfg(not(target_os = "linux"))]
    #[test]
    fn test_non_linux_cgroup_detection() {
        // On non-Linux, cgroup detection should return None
        let resources = cgroup_resources();
        assert!(!resources.detected);
        assert_eq!(resources.cpu_cores, None);
        assert_eq!(resources.memory_bytes, None);
    }
}
