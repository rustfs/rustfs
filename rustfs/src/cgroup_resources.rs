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
//! This module provides a single source of truth for detecting and applying
//! container resource limits (CPU and memory) from cgroup v1/v2.
//!
//! # Platform Support
//!
//! - **Linux**: Full cgroup v1/v2 support
//! - **macOS/Windows**: Falls back to host values (cgroup not available)
//!
//! # Design Principles
//!
//! - **Single source of truth**: All cgroup detection logic lives here
//! - **Thread-safe**: Results are cached in `OnceLock` after first detection
//! - **Zero-cost fallback**: Non-Linux platforms return host values immediately
//! - **Environment overrides**: Operators can override detected values via env vars

use std::sync::OnceLock;

// ============================================================================
// Environment variable constants
// ============================================================================

/// Disable cgroup detection entirely (for testing or special cases).
const ENV_DISABLE_CGROUP_DETECTION: &str = "RUSTFS_DISABLE_CGROUP_DETECTION";

/// Override detected CPU cores (takes precedence over cgroup).
const ENV_OVERRIDE_CPU_CORES: &str = "RUSTFS_OVERRIDE_CPU_CORES";

/// Override detected memory limit in bytes (takes precedence over cgroup).
const ENV_OVERRIDE_MEMORY_BYTES: &str = "RUSTFS_OVERRIDE_MEMORY_BYTES";

// ============================================================================
// Core types
// ============================================================================

/// Cached container resource configuration (computed once at startup).
static CONTAINER_RESOURCES: OnceLock<ContainerResources> = OnceLock::new();

/// Detected container resource limits with effective values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ContainerResources {
    /// Effective CPU cores (considering cgroup limits, overrides, and host).
    pub cpu_cores: usize,
    /// Effective memory limit in bytes (considering cgroup limits, overrides, and host).
    pub memory_bytes: u64,
    /// Whether cgroup limits were detected.
    pub cgroup_detected: bool,
    /// Whether values were overridden by environment variables.
    pub overridden: bool,
    /// Pre-computed basis string for metrics ("cgroup" or "host").
    pub basis: &'static str,
}

impl Default for ContainerResources {
    fn default() -> Self {
        Self {
            cpu_cores: 1,
            memory_bytes: 0,
            cgroup_detected: false,
            overridden: false,
            basis: "host",
        }
    }
}

// ============================================================================
// Linux cgroup detection
// ============================================================================

#[cfg(target_os = "linux")]
mod cgroup {
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
    /// Format: `"max"` (unlimited) or `"$QUOTA $PERIOD"` or `"$QUOTA"` (implicit period=100000).
    fn detect_cgroup_v2_cpus() -> Option<usize> {
        let content = std::fs::read_to_string(CGROUP_V2_CPU_MAX_PATH).ok()?;
        let parts: Vec<&str> = content.split_whitespace().collect();

        match parts.len() {
            1 => {
                if parts[0] == "max" {
                    return None;
                }
                let quota = parts[0].parse::<u64>().ok()?;
                if quota == 0 {
                    return None;
                }
                Some(quota.div_ceil(100_000) as usize)
            }
            2 => {
                if parts[0] == "max" {
                    return None;
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
            return None;
        }
        let period = read_u64_from_file(CGROUP_V1_CPU_PERIOD_PATH).unwrap_or(100_000);
        if period == 0 {
            return None;
        }
        Some(quota.div_ceil(period) as usize)
    }

    /// Detect CPU cores from cgroup, trying v2 first, then v1.
    pub(super) fn detect_cpus() -> Option<usize> {
        detect_cgroup_v2_cpus().or_else(detect_cgroup_v1_cpus)
    }

    /// Detect memory limit from cgroup v2 (`/sys/fs/cgroup/memory.max`).
    fn detect_cgroup_v2_memory() -> Option<u64> {
        let content = std::fs::read_to_string(CGROUP_V2_MEMORY_MAX_PATH).ok()?;
        let trimmed = content.trim();
        if trimmed == "max" || trimmed.is_empty() {
            return None;
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
    pub(super) fn detect_memory() -> Option<u64> {
        detect_cgroup_v2_memory().or_else(detect_cgroup_v1_memory)
    }
}

// ============================================================================
// Non-Linux fallback
// ============================================================================

#[cfg(not(target_os = "linux"))]
mod cgroup {
    /// Cgroup detection not available on non-Linux platforms.
    pub(super) fn detect_cpus() -> Option<usize> {
        None
    }

    /// Cgroup detection not available on non-Linux platforms.
    pub(super) fn detect_memory() -> Option<u64> {
        None
    }
}

// ============================================================================
// Resource detection and caching
// ============================================================================

/// Detect container resources (called once and cached).
///
/// Detection priority:
/// 1. Environment variable overrides
/// 2. cgroup v1/v2 limits (Linux only)
/// 3. Host values from sysinfo (single System instance for both CPU and memory)
fn detect_container_resources() -> ContainerResources {
    // Check if cgroup detection is disabled
    let cgroup_disabled = std::env::var(ENV_DISABLE_CGROUP_DETECTION)
        .map(|v| v == "1" || v.to_lowercase() == "true")
        .unwrap_or(false);

    // Detect cgroup limits if not disabled
    let (cgroup_cpus, cgroup_memory) = if cgroup_disabled {
        (None, None)
    } else {
        (cgroup::detect_cpus(), cgroup::detect_memory())
    };

    let cgroup_detected = cgroup_cpus.is_some() || cgroup_memory.is_some();

    // Check for environment variable overrides
    let override_cores = std::env::var(ENV_OVERRIDE_CPU_CORES)
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|&v| v > 0);

    let override_memory = std::env::var(ENV_OVERRIDE_MEMORY_BYTES)
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|&v| v > 0);

    let overridden = override_cores.is_some() || override_memory.is_some();

    // Get host values from a single sysinfo::System instance (avoids double init)
    let (host_cores, host_memory) = {
        let mut sys = sysinfo::System::new_with_specifics(sysinfo::RefreshKind::everything().without_processes());
        sys.refresh_cpu_all();
        sys.refresh_memory();
        (sys.cpus().len().max(1), sys.total_memory())
    };

    // Determine effective values: override > cgroup > host
    let cpu_cores = override_cores.or(cgroup_cpus).unwrap_or(host_cores).max(1);
    let memory_bytes = override_memory.or(cgroup_memory).unwrap_or(host_memory);
    let basis = if cgroup_detected { "cgroup" } else { "host" };

    ContainerResources {
        cpu_cores,
        memory_bytes,
        cgroup_detected,
        overridden,
        basis,
    }
}

/// Get cached container resource limits.
///
/// This function is thread-safe. Detection is performed once and cached.
pub fn container_resources() -> &'static ContainerResources {
    CONTAINER_RESOURCES.get_or_init(detect_container_resources)
}

/// Log container resource configuration at startup.
///
/// Should be called once during startup to help operators verify detection.
pub fn log_container_resources() {
    let res = container_resources();
    let memory_mib = res.memory_bytes / (1024 * 1024);

    if res.overridden {
        tracing::info!(
            cpu_cores = res.cpu_cores,
            memory_bytes = res.memory_bytes,
            memory_mib,
            cgroup_detected = res.cgroup_detected,
            "container resources (overridden by environment variables)"
        );
    } else if res.cgroup_detected {
        tracing::info!(
            cpu_cores = res.cpu_cores,
            memory_bytes = res.memory_bytes,
            memory_mib,
            "container resources (detected from cgroup)"
        );
    } else {
        tracing::debug!(
            cpu_cores = res.cpu_cores,
            memory_bytes = res.memory_bytes,
            "container resources (using host values)"
        );
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_container_resources_default() {
        let resources = ContainerResources::default();
        assert_eq!(resources.cpu_cores, 1);
        assert_eq!(resources.memory_bytes, 0);
        assert!(!resources.cgroup_detected);
        assert!(!resources.overridden);
        assert_eq!(resources.basis, "host");
    }

    #[test]
    fn test_container_resources_cached() {
        let res1 = container_resources();
        let res2 = container_resources();
        assert_eq!(res1, res2);
    }

    #[test]
    fn test_cpu_cores_minimum_one() {
        // Even with 0 host cores, should return at least 1
        let res = container_resources();
        assert!(res.cpu_cores >= 1);
    }

    #[test]
    fn test_memory_bytes_positive() {
        let res = container_resources();
        assert!(res.memory_bytes > 0);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn test_linux_cgroup_detection() {
        let res = container_resources();
        // On Linux, cgroup might or might not be available
        // Just verify the struct is valid
        assert!(res.cpu_cores >= 1);
        assert!(res.memory_bytes > 0);
    }

    #[cfg(not(target_os = "linux"))]
    #[test]
    fn test_non_linux_cgroup_detection() {
        let res = container_resources();
        assert!(!res.cgroup_detected);
        assert!(!res.overridden);
    }
}
