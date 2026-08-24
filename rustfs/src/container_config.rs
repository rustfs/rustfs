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

//! Container-aware configuration for RustFS.
//!
//! This module provides container-aware configuration that automatically
//! detects and applies resource limits from cgroup (Kubernetes, Docker, etc.).
//!
//! # Features
//!
//! - Automatic detection of CPU and memory limits from cgroup
//! - Environment variable overrides for all resource settings
//! - Startup logging of detected resource limits
//! - Runtime metrics for resource utilization

use std::sync::OnceLock;

/// Environment variable to disable cgroup detection (for testing or special cases).
const ENV_DISABLE_CGROUP_DETECTION: &str = "RUSTFS_DISABLE_CGROUP_DETECTION";

/// Environment variable to override CPU cores (takes precedence over cgroup).
const ENV_OVERRIDE_CPU_CORES: &str = "RUSTFS_OVERRIDE_CPU_CORES";

/// Environment variable to override memory limit in bytes (takes precedence over cgroup).
const ENV_OVERRIDE_MEMORY_BYTES: &str = "RUSTFS_OVERRIDE_MEMORY_BYTES";

/// Cached container configuration (computed once at startup).
static CONTAINER_CONFIG: OnceLock<ContainerConfig> = OnceLock::new();

/// Detected container resource configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ContainerConfig {
    /// Effective CPU cores (considering cgroup limits and overrides).
    pub cpu_cores: usize,
    /// Effective memory limit in bytes (considering cgroup limits and overrides).
    pub memory_bytes: u64,
    /// Whether cgroup detection was used.
    pub cgroup_detected: bool,
    /// Whether values were overridden by environment variables.
    pub overridden: bool,
}

impl Default for ContainerConfig {
    fn default() -> Self {
        Self {
            cpu_cores: 1,
            memory_bytes: 0,
            cgroup_detected: false,
            overridden: false,
        }
    }
}

/// Detect container configuration from cgroup and environment variables.
fn detect_container_config() -> ContainerConfig {
    // Check if cgroup detection is disabled
    let cgroup_disabled = std::env::var(ENV_DISABLE_CGROUP_DETECTION)
        .map(|v| v == "1" || v.to_lowercase() == "true")
        .unwrap_or(false);

    // Get cgroup resources if not disabled
    let cgroup_res = if cgroup_disabled {
        None
    } else {
        Some(crate::cgroup_resources::cgroup_resources())
    };

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

    // Determine effective values
    let cpu_cores = match (override_cores, cgroup_res) {
        (Some(override_val), _) => override_val,
        (None, Some(res)) => res.cpu_cores.unwrap_or_else(|| {
            // Fallback to host CPU count
            let mut sys =
                sysinfo::System::new_with_specifics(sysinfo::RefreshKind::everything().without_memory().without_processes());
            sys.refresh_cpu_all();
            sys.cpus().len().max(1)
        }),
        (None, None) => {
            // No cgroup, no override - use host CPU count
            let mut sys =
                sysinfo::System::new_with_specifics(sysinfo::RefreshKind::everything().without_memory().without_processes());
            sys.refresh_cpu_all();
            sys.cpus().len().max(1)
        }
    };

    let memory_bytes = match (override_memory, cgroup_res) {
        (Some(override_val), _) => override_val,
        (None, Some(res)) => res.memory_bytes.unwrap_or_else(|| {
            // Fallback to host memory
            let mut sys = sysinfo::System::new();
            sys.refresh_memory();
            sys.total_memory()
        }),
        (None, None) => {
            // No cgroup, no override - use host memory
            let mut sys = sysinfo::System::new();
            sys.refresh_memory();
            sys.total_memory()
        }
    };

    let cgroup_detected = cgroup_res.map(|r| r.detected).unwrap_or(false);

    ContainerConfig {
        cpu_cores,
        memory_bytes,
        cgroup_detected,
        overridden,
    }
}

/// Get cached container configuration.
///
/// This function is safe to call from multiple threads. The detection is performed
/// once and the result is cached for the lifetime of the process.
pub fn container_config() -> &'static ContainerConfig {
    CONTAINER_CONFIG.get_or_init(detect_container_config)
}

/// Log container configuration at startup.
pub fn log_container_config() {
    let config = container_config();

    if config.overridden {
        tracing::info!(
            cpu_cores = config.cpu_cores,
            memory_bytes = config.memory_bytes,
            memory_mib = config.memory_bytes / (1024 * 1024),
            cgroup_detected = config.cgroup_detected,
            "container resources (overridden by environment variables)"
        );
    } else if config.cgroup_detected {
        tracing::info!(
            cpu_cores = config.cpu_cores,
            memory_bytes = config.memory_bytes,
            memory_mib = config.memory_bytes / (1024 * 1024),
            "container resources (detected from cgroup)"
        );
    } else {
        tracing::info!(
            cpu_cores = config.cpu_cores,
            memory_bytes = config.memory_bytes,
            memory_mib = config.memory_bytes / (1024 * 1024),
            "container resources (using host values)"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_container_config_default() {
        let config = ContainerConfig::default();
        assert_eq!(config.cpu_cores, 1);
        assert_eq!(config.memory_bytes, 0);
        assert!(!config.cgroup_detected);
        assert!(!config.overridden);
    }

    #[test]
    fn test_detect_container_config() {
        // This test will use actual system values
        let config = detect_container_config();
        assert!(config.cpu_cores > 0);
        assert!(config.memory_bytes > 0);
    }

    #[test]
    fn test_container_config_cached() {
        // Multiple calls should return the same config
        let config1 = container_config();
        let config2 = container_config();
        assert_eq!(config1, config2);
    }
}
