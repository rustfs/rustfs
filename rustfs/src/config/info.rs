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
use serde::Serialize;
use std::fmt;

/// CPU information
#[derive(Serialize)]
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
}

/// Memory information
#[derive(Serialize)]
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
}

/// Disk information
#[derive(Serialize)]
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
}

/// Service disk information (disk where the service data is stored)
#[derive(Serialize)]
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
            "## Service Disk Information\n\n\
             | Property | Value |\n\
             |----------|-------|\n\
             | Disk | {} |\n\
             | Mount Point | {} |\n\
             | File System | {} |\n\
             | Total Space | {} |\n\
             | Used Space | {} ({:.1}%) |\n\
             | Available Space | {} |\n\
             | Served Volumes | {} |",
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
#[derive(Serialize)]
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
        let mut output = String::new();

        // System Information Table
        output.push_str("## System Information\n\n");
        output.push_str("| Property | Value |\n");
        output.push_str("|----------|-------|\n");
        output.push_str(&format!("| OS | {} |\n", self.os_type));
        output.push_str(&format!("| OS Version | {} |\n", self.os_version));
        output.push_str(&format!("| Architecture | {} |\n", self.architecture));
        output.push_str(&format!("| Hostname | {} |\n", self.hostname));
        output.push_str(&format!("| Kernel Version | {} |\n", self.kernel_version));

        // CPU Information Table
        output.push_str("\n## CPU Information\n\n");
        output.push_str("| Property | Value |\n");
        output.push_str("|----------|-------|\n");
        output.push_str(&format!("| Cores | {} |\n", self.cpu.core_count));
        output.push_str(&format!("| Brand | {} |\n", self.cpu.brand));
        output.push_str(&format!("| Frequency | {} MHz |\n", self.cpu.frequency_mhz));
        output.push_str(&format!("| Usage | {:.1}% |\n", self.cpu.usage_percent));

        // Memory Information Table
        output.push_str("\n## Memory Information\n\n");
        output.push_str("| Property | Value |\n");
        output.push_str("|----------|-------|\n");
        output.push_str(&format!("| Total | {} |\n", MemoryInfo::format_bytes(self.memory.total_bytes)));
        output.push_str(&format!(
            "| Used | {} ({:.1}%) |\n",
            MemoryInfo::format_bytes(self.memory.used_bytes),
            self.memory.usage_percent
        ));
        output.push_str(&format!("| Available | {} |\n", MemoryInfo::format_bytes(self.memory.available_bytes)));
        output.push_str(&format!("| Total Swap | {} |\n", MemoryInfo::format_bytes(self.memory.total_swap_bytes)));
        output.push_str(&format!("| Used Swap | {} |\n", MemoryInfo::format_bytes(self.memory.used_swap_bytes)));

        // Disk Information Table
        output.push_str("\n## Disk Information\n\n");
        if self.disks.is_empty() {
            output.push_str("*No disks found*\n");
        } else {
            output.push_str("| Name | Mount Point | Type | Total | Used | Available | Usage | Removable |\n");
            output.push_str("|------|-------------|------|-------|------|-----------|-------|----------|\n");
            for disk in &self.disks {
                output.push_str(&format!(
                    "| {} | {} | {} | {} | {} | {} | {:.1}% | {} |\n",
                    disk.name,
                    disk.mount_point,
                    disk.file_system,
                    MemoryInfo::format_bytes(disk.total_bytes),
                    MemoryInfo::format_bytes(disk.used_bytes),
                    MemoryInfo::format_bytes(disk.available_bytes),
                    disk.usage_percent,
                    disk.is_removable
                ));
            }
        }

        output
    }
}
impl fmt::Display for SystemInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.format())
    }
}

/// Runtime information
#[derive(Serialize)]
struct RuntimeInfo {
    process_id: u32,
    memory_usage_mb: f64,
    cpu_usage_percent: f64,
    thread_count: usize,
}

impl RuntimeInfo {
    fn collect() -> Self {
        let (process_id, memory_usage_mb, cpu_usage_percent) = if let Ok(pid) = sysinfo::get_current_pid() {
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

            (pid.as_u32(), memory_usage_mb, cpu_usage_percent)
        } else {
            // Failed to retrieve current PID; degrade gracefully by
            // skipping per-process stats and using sentinel values.
            (0, 0.0, 0.0)
        };

        // // Get available CPU parallelism (roughly, logical cores available to the process)
        let cpu_parallelism = std::thread::available_parallelism().map(|p| p.get()).unwrap_or(1);

        Self {
            process_id,
            memory_usage_mb,
            cpu_usage_percent,
            thread_count: cpu_parallelism,
        }
    }

    fn format(&self) -> String {
        let pid_display = if self.process_id == 0 {
            "unknown".to_string()
        } else {
            self.process_id.to_string()
        };

        format!(
            "## Runtime Information\n\n\
             | Property | Value |\n\
             |----------|-------|\n\
             | Process ID | {} |\n\
             | Memory Usage | {:.2} MB |\n\
             | CPU Usage | {:.2}% |\n\
             | CPU Parallelism | {} |",
            pid_display, self.memory_usage_mb, self.cpu_usage_percent, self.thread_count
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
            "## Build Information\n\n\
             | Property | Value |\n\
             |----------|-------|\n\
             | Version | {} |\n\
             | Build Time | {} |\n\
             | Build Profile | {} |\n\
             | Build OS | {} |\n\
             | Rust Version | {} |\n\
             | Git Branch | {} |\n\
             | Git Commit | {} |\n\
             | Git Tag | {} |\n\
             | Git Status | {} |",
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

// ============================================================================
// JSON Output Structures
// ============================================================================

/// Build information for JSON output
#[derive(Serialize)]
struct BuildInfoJson {
    version: &'static str,
    build_time: &'static str,
    build_profile: &'static str,
    build_os: &'static str,
    rust_version: &'static str,
    git_branch: &'static str,
    git_commit: &'static str,
    git_tag: &'static str,
    git_status: &'static str,
}

impl BuildInfo {
    fn collect_json() -> BuildInfoJson {
        BuildInfoJson {
            version: build::PKG_VERSION,
            build_time: build::BUILD_TIME,
            build_profile: build::BUILD_RUST_CHANNEL,
            build_os: build::BUILD_OS,
            rust_version: build::RUST_VERSION,
            git_branch: build::BRANCH,
            git_commit: build::COMMIT_HASH,
            git_tag: build::TAG,
            git_status: build::GIT_STATUS_FILE,
        }
    }
}

/// Configuration information for JSON output
#[derive(Serialize)]
struct ConfigInfoJson {
    address: String,
    console_enable: bool,
    console_address: String,
    region: Option<String>,
    access_key: String,
    obs_endpoint: String,
    tls_path: Option<String>,
    kms_enable: bool,
    kms_backend: String,
    buffer_profile: String,
    workload_profile: Option<WorkloadProfileJson>,
}

/// Workload profile information for JSON output
#[derive(Serialize)]
struct WorkloadProfileJson {
    name: String,
    buffer_min_size: usize,
    buffer_max_size: usize,
    default_unknown: usize,
}

fn collect_config_info_json() -> ConfigInfoJson {
    let snapshot = super::get_config_snapshot_for_display();

    // Get workload profile info
    let workload_profile = {
        use super::workload_profiles::{get_global_buffer_config, is_buffer_profile_enabled};
        if is_buffer_profile_enabled() {
            let config = get_global_buffer_config();
            let profile = config.workload_profile();
            let buffer_config = profile.config();
            Some(WorkloadProfileJson {
                name: config.workload_name(),
                buffer_min_size: buffer_config.min_size,
                buffer_max_size: buffer_config.max_size,
                default_unknown: buffer_config.default_unknown,
            })
        } else {
            None
        }
    };

    ConfigInfoJson {
        address: snapshot.address.clone(),
        console_enable: snapshot.console_enable,
        console_address: snapshot.console_address.clone(),
        region: snapshot.region.clone(),
        access_key: snapshot.access_key.clone(),
        obs_endpoint: snapshot.obs_endpoint.clone(),
        tls_path: snapshot.tls_path.clone(),
        kms_enable: snapshot.kms_enable,
        kms_backend: snapshot.kms_backend.clone(),
        buffer_profile: snapshot.buffer_profile.clone(),
        workload_profile,
    }
}

/// Feature information for JSON output
#[derive(Serialize)]
struct FeatureInfoJson {
    name: &'static str,
    enabled: bool,
    description: &'static str,
}

struct FeatureSpec {
    name: &'static str,
    enabled: bool,
    description: &'static str,
    dependencies: &'static str,
    default_enabled: bool,
}

fn feature_specs() -> [FeatureSpec; 8] {
    [
        FeatureSpec {
            name: "metrics-gpu",
            enabled: cfg!(feature = "metrics-gpu"),
            description: "Metrics GPU support",
            dependencies: "rustfs-obs/gpu",
            default_enabled: false,
        },
        FeatureSpec {
            name: "ftps",
            enabled: cfg!(feature = "ftps"),
            description: "FTPS protocol support",
            dependencies: "rustfs-protocols/ftps",
            default_enabled: false,
        },
        FeatureSpec {
            name: "swift",
            enabled: cfg!(feature = "swift"),
            description: "Swift storage backend",
            dependencies: "rustfs-protocols/swift",
            default_enabled: false,
        },
        FeatureSpec {
            name: "webdav",
            enabled: cfg!(feature = "webdav"),
            description: "WebDAV protocol support",
            dependencies: "rustfs-protocols/webdav",
            default_enabled: false,
        },
        FeatureSpec {
            name: "license",
            enabled: cfg!(feature = "license"),
            description: "License validation",
            dependencies: "(none)",
            default_enabled: false,
        },
        FeatureSpec {
            name: "io-scheduler-debug",
            enabled: cfg!(feature = "io-scheduler-debug"),
            description: "Enable debug information in I/O scheduler",
            dependencies: "(none)",
            default_enabled: false,
        },
        FeatureSpec {
            name: "manual-test-runners",
            enabled: cfg!(feature = "manual-test-runners"),
            description: "Enable manual test binaries",
            dependencies: "(none)",
            default_enabled: false,
        },
        FeatureSpec {
            name: "full",
            enabled: cfg!(feature = "full"),
            description: "All features enabled",
            dependencies: "metrics-gpu + ftps + swift + webdav",
            default_enabled: false,
        },
    ]
}

/// Dependency information for JSON output
#[derive(Serialize)]
struct DepsInfoJson {
    enabled_count: usize,
    total_count: usize,
    features: Vec<FeatureInfoJson>,
}

fn collect_deps_info_json() -> DepsInfoJson {
    let features: Vec<FeatureInfoJson> = feature_specs()
        .into_iter()
        .map(|feature| FeatureInfoJson {
            name: feature.name,
            enabled: feature.enabled,
            description: feature.description,
        })
        .collect();

    let enabled_count = features.iter().filter(|f| f.enabled).count();
    let total_count = features.len();

    DepsInfoJson {
        enabled_count,
        total_count,
        features,
    }
}

/// All information combined for JSON output
#[derive(Serialize)]
struct AllInfoJson {
    system: SystemInfo,
    runtime: RuntimeInfo,
    build: BuildInfoJson,
    config: ConfigInfoJson,
    deps: DepsInfoJson,
    #[serde(skip_serializing_if = "Option::is_none")]
    service_disk: Option<ServiceDiskInfo>,
}

/// Print info as JSON
fn print_info_json<T: Serialize>(info: &T) {
    match serde_json::to_string_pretty(info) {
        Ok(json) => println!("{}", json),
        Err(e) => eprintln!("Failed to serialize info to JSON: {}", e),
    }
}

/// Execute info command with JSON output
fn execute_info_json(info_type: Option<InfoType>, volumes: &[String]) {
    match info_type {
        None => {
            // Output all info as nested JSON object
            let all_info = AllInfoJson {
                system: SystemInfo::collect(),
                runtime: RuntimeInfo::collect(),
                build: BuildInfo::collect_json(),
                config: collect_config_info_json(),
                deps: collect_deps_info_json(),
                service_disk: ServiceDiskInfo::collect(volumes),
            };
            print_info_json(&all_info);
        }
        Some(InfoType::System) => {
            print_info_json(&SystemInfo::collect());
        }
        Some(InfoType::Runtime) => {
            print_info_json(&RuntimeInfo::collect());
        }
        Some(InfoType::Build) => {
            print_info_json(&BuildInfo::collect_json());
        }
        Some(InfoType::Config) => {
            print_info_json(&collect_config_info_json());
        }
        Some(InfoType::Deps) => {
            print_info_json(&collect_deps_info_json());
        }
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

    let obs_endpoint = if snapshot.obs_endpoint.is_empty() {
        "(not set)"
    } else {
        &snapshot.obs_endpoint
    };

    format!(
        "## Configuration Information\n\n\
         | Property | Value |\n\
         |----------|-------|\n\
         | Server Address | {} |\n\
         | Console Enable | {} |\n\
         | Console Address | {} |\n\
         | Region | {} |\n\
         | Access Key | {} |\n\
         | Secret Key | **** |\n\
         | OBS Endpoint | {} |\n\
         | TLS Path | {} |\n\
         | KMS Enabled | {} |\n\
         | KMS Backend | {} |\n\
         | Buffer Profile | {} |\n\
         {}",
        snapshot.address,
        snapshot.console_enable,
        snapshot.console_address,
        snapshot.region.as_deref().unwrap_or("(not set)"),
        masked_access_key,
        obs_endpoint,
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
        return "| Workload Profile | (disabled) |".to_string();
    }

    let config = get_global_buffer_config();
    let profile = config.workload_profile();
    let name = config.workload_name();
    let buffer_config = profile.config();

    format!(
        "| Workload Profile | {} |\n\
         | Buffer Min Size | {} bytes |\n\
         | Buffer Max Size | {} bytes |\n\
         | Default Unknown | {} bytes |",
        name, buffer_config.min_size, buffer_config.max_size, buffer_config.default_unknown
    )
}

/// Dependency information
fn format_deps_info() -> String {
    let features = feature_specs();

    let enabled_count = features.iter().filter(|feature| feature.enabled).count();

    let mut output = format!(
        "## Build Features\n\n\
         | Property | Value |\n\
         |----------|-------|\n\
         | Enabled Features | {}/{} |\n\n",
        enabled_count,
        features.len()
    );

    output.push_str("### Feature Status\n\n");
    output.push_str("| Feature | Status | Description |\n");
    output.push_str("|---------|--------|-------------|\n");
    for feature in &features {
        let status = if feature.enabled { "✓" } else { "✗" };
        output.push_str(&format!("| {} | {} | {} |\n", feature.name, status, feature.description));
    }

    output.push_str("\n### Default Features\n\n");
    output.push_str("| Feature | Note |\n");
    output.push_str("|---------|------|\n");
    for feature in features.iter().filter(|feature| feature.default_enabled) {
        output.push_str(&format!("| {} | enabled by default |\n", feature.name));
    }

    output.push_str("\n### Feature Dependencies\n\n");
    output.push_str("| Feature | Dependencies |\n");
    output.push_str("|---------|-------------|\n");
    for feature in &features {
        output.push_str(&format!("| {} | {} |\n", feature.name, feature.dependencies));
    }

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

    // JSON output path
    if opts.json {
        execute_info_json(info_type, volumes);
        return;
    }

    // Markdown output path (default)
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

    #[test]
    fn test_collect_deps_info_json_matches_cargo_features() {
        let info = collect_deps_info_json();
        let feature_names: Vec<_> = info.features.iter().map(|feature| feature.name).collect();

        assert_eq!(info.total_count, 8);
        assert_eq!(info.features.len(), 8);
        assert!(feature_names.contains(&"metrics-gpu"));
        assert!(feature_names.contains(&"io-scheduler-debug"));
        assert!(feature_names.contains(&"manual-test-runners"));
        assert!(!feature_names.contains(&"metrics"));
        assert!(!feature_names.contains(&"direct-io"));
    }

    #[test]
    fn test_format_deps_info_matches_cargo_feature_output() {
        let output = format_deps_info();

        assert!(output.contains("| metrics-gpu |"));
        assert!(output.contains("| io-scheduler-debug |"));
        assert!(output.contains("| manual-test-runners |"));
        assert!(output.contains("| full | metrics-gpu + ftps + swift + webdav |"));
        assert!(!output.contains("| direct-io |"));
    }
}
