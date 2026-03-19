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

//! System information display module.
//!
//! This module provides the `--info` command functionality for querying and displaying
//! various system information including system basics, runtime stats, build info,
//! configuration, and dependencies.

use super::{InfoOpts, InfoType};
use crate::version::build;
use rustfs_credentials::Masked;
use std::fmt;

/// CPU information
struct CpuInfo {
    /// Number of logical CPU cores
    core_count: usize,
    /// CPU brand/vendor name
    brand: String,
    /// CPU frequency in MHz
    frequency_mhz: u64,
    /// CPU usage percentage (0-100)
    usage_percent: f64,
}

impl CpuInfo {
    fn collect(sys: &sysinfo::System) -> Self {
        let core_count = sys.cpus().len();
        let brand = sys
            .cpus()
            .first()
            .map(|c| c.brand().to_string())
            .unwrap_or_else(|| "Unknown".to_string());
        let frequency_mhz = sys.cpus().first().map(|c| c.frequency()).unwrap_or(0);

        // Calculate average CPU usage
        let usage_percent = if core_count > 0 {
            sys.cpus().iter().map(|c| c.cpu_usage() as f64).sum::<f64>() / core_count as f64
        } else {
            0.0
        };

        Self {
            core_count,
            brand,
            frequency_mhz,
            usage_percent,
        }
    }

    fn format(&self) -> String {
        format!(
            "CPU Cores: {}\n\
             CPU Brand: {}\n\
             CPU Frequency: {} MHz\n\
             CPU Usage: {:.1}%",
            self.core_count, self.brand, self.frequency_mhz, self.usage_percent
        )
    }
}

/// Memory information
struct MemoryInfo {
    /// Total system memory in bytes
    total_bytes: u64,
    /// Used memory in bytes
    used_bytes: u64,
    /// Available/free memory in bytes
    available_bytes: u64,
    /// Total swap memory in bytes
    total_swap_bytes: u64,
    /// Used swap memory in bytes
    used_swap_bytes: u64,
    /// Memory usage percentage
    usage_percent: f64,
}

impl MemoryInfo {
    fn collect(sys: &sysinfo::System) -> Self {
        let total_bytes = sys.total_memory();
        let used_bytes = sys.used_memory();
        let available_bytes = sys.available_memory();
        let total_swap_bytes = sys.total_swap();
        let used_swap_bytes = sys.used_swap();

        let usage_percent = if total_bytes > 0 {
            (used_bytes as f64 / total_bytes as f64) * 100.0
        } else {
            0.0
        };

        Self {
            total_bytes,
            used_bytes,
            available_bytes,
            total_swap_bytes,
            used_swap_bytes,
            usage_percent,
        }
    }

    fn format_bytes(bytes: u64) -> String {
        const KB: u64 = 1024;
        const MB: u64 = KB * 1024;
        const GB: u64 = MB * 1024;
        const TB: u64 = GB * 1024;

        if bytes >= TB {
            format!("{:.2} TB", bytes as f64 / TB as f64)
        } else if bytes >= GB {
            format!("{:.2} GB", bytes as f64 / GB as f64)
        } else if bytes >= MB {
            format!("{:.2} MB", bytes as f64 / MB as f64)
        } else if bytes >= KB {
            format!("{:.2} KB", bytes as f64 / KB as f64)
        } else {
            format!("{} B", bytes)
        }
    }

    fn format(&self) -> String {
        format!(
            "Total Memory: {}\n\
             Used Memory: {} ({:.1}%)\n\
             Available Memory: {}\n\
             Total Swap: {}\n\
             Used Swap: {}",
            Self::format_bytes(self.total_bytes),
            Self::format_bytes(self.used_bytes),
            self.usage_percent,
            Self::format_bytes(self.available_bytes),
            Self::format_bytes(self.total_swap_bytes),
            Self::format_bytes(self.used_swap_bytes)
        )
    }
}

/// Disk information
struct DiskInfo {
    /// Disk mount point
    mount_point: String,
    /// Disk name/device
    name: String,
    /// File system type
    file_system: String,
    /// Total space in bytes
    total_bytes: u64,
    /// Used space in bytes
    used_bytes: u64,
    /// Available space in bytes
    available_bytes: u64,
    /// Usage percentage
    usage_percent: f64,
    /// Is this a removable disk
    is_removable: bool,
}

impl DiskInfo {
    fn collect_all() -> Vec<Self> {
        let disks = sysinfo::Disks::new_with_refreshed_list();
        disks
            .iter()
            .map(|disk| {
                let total_bytes = disk.total_space();
                let available_bytes = disk.available_space();
                let used_bytes = total_bytes.saturating_sub(available_bytes);
                let usage_percent = if total_bytes > 0 {
                    (used_bytes as f64 / total_bytes as f64) * 100.0
                } else {
                    0.0
                };

                Self {
                    mount_point: disk.mount_point().to_string_lossy().to_string(),
                    name: disk.name().to_string_lossy().to_string(),
                    file_system: format!("{:?}", disk.file_system()),
                    total_bytes,
                    used_bytes,
                    available_bytes,
                    usage_percent,
                    is_removable: disk.is_removable(),
                }
            })
            .collect()
    }

    fn format(&self) -> String {
        format!(
            "  [{}] {}\n\
               Mount: {}\n\
               Type: {}\n\
               Total: {}\n\
               Used: {} ({:.1}%)\n\
               Available: {}\n\
               Removable: {}",
            if self.is_removable { "R" } else { "F" },
            self.name,
            self.mount_point,
            self.file_system,
            MemoryInfo::format_bytes(self.total_bytes),
            MemoryInfo::format_bytes(self.used_bytes),
            self.usage_percent,
            MemoryInfo::format_bytes(self.available_bytes),
            self.is_removable
        )
    }
}

/// Service disk information (disk where the service data is stored)
struct ServiceDiskInfo {
    /// Disk mount point
    mount_point: String,
    /// Disk name/device
    name: String,
    /// File system type
    file_system: String,
    /// Total space in bytes
    total_bytes: u64,
    /// Used space in bytes
    used_bytes: u64,
    /// Available space in bytes
    available_bytes: u64,
    /// Usage percentage
    usage_percent: f64,
    /// Volume paths served by this disk
    volume_paths: Vec<String>,
}

impl ServiceDiskInfo {
    fn collect(volumes: &[String]) -> Option<Self> {
        // Find the disk that contains the first volume path
        let first_volume = volumes.first()?;
        let volume_path = std::path::Path::new(first_volume);

        // Find the disk that contains this volume
        let disks = sysinfo::Disks::new_with_refreshed_list();
        for disk in disks.iter() {
            let mount_point = disk.mount_point();
            if volume_path.starts_with(mount_point) {
                let total_bytes = disk.total_space();
                let available_bytes = disk.available_space();
                let used_bytes = total_bytes.saturating_sub(available_bytes);
                let usage_percent = if total_bytes > 0 {
                    (used_bytes as f64 / total_bytes as f64) * 100.0
                } else {
                    0.0
                };

                // Find all volumes on this disk
                let volume_paths: Vec<String> = volumes
                    .iter()
                    .filter(|v| std::path::Path::new(v).starts_with(mount_point))
                    .cloned()
                    .collect();

                return Some(Self {
                    mount_point: mount_point.to_string_lossy().to_string(),
                    name: disk.name().to_string_lossy().to_string(),
                    file_system: format!("{:?}", disk.file_system()),
                    total_bytes,
                    used_bytes,
                    available_bytes,
                    usage_percent,
                    volume_paths,
                });
            }
        }

        None
    }

    fn format(&self) -> String {
        let volumes_str = if self.volume_paths.is_empty() {
            "(none)".to_string()
        } else {
            self.volume_paths.join(", ")
        };

        format!(
            "=== Service Disk Information ===\n\
             Disk: {}\n\
             Mount Point: {}\n\
             File System: {}\n\
             Total Space: {}\n\
             Used Space: {} ({:.1}%)\n\
             Available Space: {}\n\
             Served Volumes: {}",
            self.name,
            self.mount_point,
            self.file_system,
            MemoryInfo::format_bytes(self.total_bytes),
            MemoryInfo::format_bytes(self.used_bytes),
            self.usage_percent,
            MemoryInfo::format_bytes(self.available_bytes),
            volumes_str
        )
    }
}

/// System basic information
struct SystemInfo {
    os_type: String,
    os_version: String,
    architecture: String,
    hostname: String,
    kernel_version: String,
    /// CPU information
    cpu: CpuInfo,
    /// Memory information
    memory: MemoryInfo,
    /// All disk information
    disks: Vec<DiskInfo>,
}

impl SystemInfo {
    fn collect() -> Self {
        // Create system info collector
        let mut sys = sysinfo::System::new_all();
        sys.refresh_all();
        sys.refresh_cpu_all();

        Self {
            os_type: sysinfo::System::distribution_id(),
            os_version: sysinfo::System::long_os_version().unwrap_or_else(|| "Unknown".to_string()),
            architecture: std::env::consts::ARCH.to_string(),
            hostname: sysinfo::System::host_name().unwrap_or_else(|| "Unknown".to_string()),
            kernel_version: sysinfo::System::kernel_long_version(),
            cpu: CpuInfo::collect(&sys),
            memory: MemoryInfo::collect(&sys),
            disks: DiskInfo::collect_all(),
        }
    }

    fn format(&self) -> String {
        let disks_str = if self.disks.is_empty() {
            "  (no disks found)".to_string()
        } else {
            self.disks.iter().map(|d| d.format()).collect::<Vec<_>>().join("\n")
        };

        format!(
            "=== System Information ===\n\
             OS: {}\n\
             OS Version: {}\n\
             Architecture: {}\n\
             Hostname: {}\n\
             Kernel Version: {}\n\
             \n\
             === CPU Information ===\n\
             {}\n\
             \n\
             === Memory Information ===\n\
             {}\n\
             \n\
             === Disk Information ===\n\
             {}",
            self.os_type,
            self.os_version,
            self.architecture,
            self.hostname,
            self.kernel_version,
            self.cpu.format(),
            self.memory.format(),
            disks_str
        )
    }
}
impl fmt::Display for SystemInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.format())
    }
}

/// Runtime information
struct RuntimeInfo {
    process_id: u32,
    memory_usage_mb: f64,
    cpu_usage_percent: f64,
    thread_count: usize,
}

impl RuntimeInfo {
    fn collect() -> Self {
        let pid = sysinfo::get_current_pid().expect("Failed to get current PID");
        let mut sys = sysinfo::System::new();
        sys.refresh_processes_specifics(
            sysinfo::ProcessesToUpdate::Some(&[pid]),
            true,
            sysinfo::ProcessRefreshKind::everything(),
        );

        let process = sys.process(pid);
        let (memory_usage_mb, cpu_usage_percent) = if let Some(p) = process {
            let memory = p.memory() as f64 / 1024.0 / 1024.0; // Convert to MB
            let cpu = p.cpu_usage() as f64;
            (memory, cpu)
        } else {
            (0.0, 0.0)
        };

        // // Get available CPU parallelism (roughly, logical cores available to the process)
        let cpu_parallelism = std::thread::available_parallelism().map(|p| p.get()).unwrap_or(1);

        Self {
            process_id: pid.as_u32(),
            memory_usage_mb,
            cpu_usage_percent,
            thread_count: cpu_parallelism,
        }
    }

    fn format(&self) -> String {
        format!(
            "=== Runtime Information ===\n\
             Process ID: {}\n\
             Memory Usage: {:.2} MB\n\
             CPU Usage: {:.2}%\n\
             CPU Parallelism (logical cores): {}",
            self.process_id, self.memory_usage_mb, self.cpu_usage_percent, self.thread_count
        )
    }
}
impl fmt::Display for RuntimeInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.format())
    }
}

/// Build information (using shadow-rs generated constants)
struct BuildInfo;

impl BuildInfo {
    fn format() -> String {
        format!(
            "=== Build Information ===\n\
             Version: {}\n\
             Build Time: {}\n\
             Build Profile: {}\n\
             Build OS: {}\n\
             Rust Version: {}\n\
             Git Branch: {}\n\
             Git Commit: {}\n\
             Git Tag: {}\n\
             Git Status: {}",
            build::PKG_VERSION,
            build::BUILD_TIME,
            build::BUILD_RUST_CHANNEL,
            build::BUILD_OS,
            build::RUST_VERSION,
            build::BRANCH,
            build::COMMIT_HASH,
            build::TAG,
            build::GIT_STATUS_FILE
        )
    }
}

impl fmt::Display for BuildInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", Self::format())
    }
}

/// Configuration information display
fn format_config_info() -> String {
    // Get config snapshot for display (from global if initialized, otherwise from env)
    let snapshot = super::get_config_snapshot_for_display();

    // Get workload profile info
    let workload_info = get_workload_profile_info();

    // Mask the access key for display
    let masked_access_key = &Masked(Some(&snapshot.access_key));

    format!(
        "=== Configuration Information ===\n\
         Server Address: {}\n\
         Console Enable: {}\n\
         Console Address: {}\n\
         Region: {}\n\
         Access Key: {}\n\
         Secret Key: ****\n\
         OBS Endpoint: {}\n\
         TLS Path: {}\n\
         KMS Enabled: {}\n\
         KMS Backend: {}\n\
         Buffer Profile: {}\n\
         {}",
        snapshot.address,
        snapshot.console_enable,
        snapshot.console_address,
        snapshot.region.as_deref().unwrap_or("(not set)"),
        masked_access_key,
        if snapshot.obs_endpoint.is_empty() {
            "(not set)"
        } else {
            &snapshot.obs_endpoint
        },
        snapshot.tls_path.as_deref().unwrap_or("(not set)"),
        snapshot.kms_enable,
        snapshot.kms_backend,
        snapshot.buffer_profile,
        workload_info
    )
}

/// Get workload profile information from global buffer config
fn get_workload_profile_info() -> String {
    use super::workload_profiles::{get_global_buffer_config, is_buffer_profile_enabled};

    if !is_buffer_profile_enabled() {
        return "Workload Profile: (disabled)".to_string();
    }

    let config = get_global_buffer_config();
    let profile = config.workload_profile();
    let name = config.workload_name();
    let buffer_config = profile.config();

    format!(
        "Workload Profile: {}\n\
         Buffer Min Size: {} bytes\n\
         Buffer Max Size: {} bytes\n\
         Default Unknown: {} bytes",
        name, buffer_config.min_size, buffer_config.max_size, buffer_config.default_unknown
    )
}

/// Dependency information
fn format_deps_info() -> String {
    let mut output = String::from("=== Build Features ===\n");

    // Check which features are enabled at compile time
    let features = [
        ("metrics", cfg!(feature = "metrics"), "Metrics collection and reporting"),
        ("ftps", cfg!(feature = "ftps"), "FTPS protocol support"),
        ("swift", cfg!(feature = "swift"), "Swift storage backend"),
        ("webdav", cfg!(feature = "webdav"), "WebDAV protocol support"),
        ("license", cfg!(feature = "license"), "License validation"),
        ("full", cfg!(feature = "full"), "All features enabled"),
    ];

    let enabled_count = features.iter().filter(|(_, enabled, _)| *enabled).count();
    output.push_str(&format!("Enabled Features: {}/{}\n\n", enabled_count, features.len()));

    output.push_str("Feature Status:\n");
    for (name, enabled, description) in features {
        let status = if enabled { "[x]" } else { "[ ]" };
        output.push_str(&format!("  {} {} - {}\n", status, name, description));
    }

    // Show default features info
    output.push_str("\n--- Default Features ---\n");
    output.push_str("  metrics (enabled by default)\n");

    // Show feature dependencies
    output.push_str("\n--- Feature Dependencies ---\n");
    output.push_str("  full = metrics + ftps + swift + webdav\n");
    output.push_str("  ftps -> rustfs-protocols/ftps\n");
    output.push_str("  swift -> rustfs-protocols/swift\n");
    output.push_str("  webdav -> rustfs-protocols/webdav\n");

    output
}

/// Execute the info command
pub fn execute_info(opts: &InfoOpts) {
    execute_info_with_volumes(opts, &[])
}

/// Execute info command with volume paths for service disk information
pub fn execute_info_with_volumes(opts: &InfoOpts, volumes: &[String]) {
    let info_type = if opts.all {
        None // None means display all
    } else {
        opts.info_type
    };

    match info_type {
        None => {
            // Display all information
            println!("{}", SystemInfo::collect());
            println!();
            println!("{}", RuntimeInfo::collect());
            println!();
            println!("{}", BuildInfo);
            println!();
            println!("{}", format_config_info());
            println!();
            // Display service disk info if volumes are configured
            if let Some(service_disk) = ServiceDiskInfo::collect(volumes) {
                println!("{}", service_disk.format());
                println!();
            }
            println!("{}", format_deps_info());
        }
        Some(InfoType::System) => {
            println!("{}", SystemInfo::collect());
        }
        Some(InfoType::Runtime) => {
            println!("{}", RuntimeInfo::collect());
        }
        Some(InfoType::Build) => {
            println!("{}", BuildInfo);
        }
        Some(InfoType::Config) => {
            println!("{}", format_config_info());
        }
        Some(InfoType::Deps) => {
            println!("{}", format_deps_info());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_system_info_collect() {
        let info = SystemInfo::collect();
        assert!(!info.os_type.is_empty());
        assert!(!info.architecture.is_empty());
    }

    #[test]
    fn test_runtime_info_collect() {
        let info = RuntimeInfo::collect();
        assert!(info.process_id > 0);
    }
}
