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

/// System basic information
struct SystemInfo {
    os_type: String,
    os_version: String,
    architecture: String,
    hostname: String,
    kernel_version: String,
}

impl SystemInfo {
    fn collect() -> Self {
        Self {
            os_type: sysinfo::System::name().unwrap_or_else(|| "Unknown".to_string()),
            os_version: sysinfo::System::os_version().unwrap_or_else(|| "Unknown".to_string()),
            architecture: std::env::consts::ARCH.to_string(),
            hostname: sysinfo::System::host_name().unwrap_or_else(|| "Unknown".to_string()),
            kernel_version: sysinfo::System::kernel_version().unwrap_or_else(|| "Unknown".to_string()),
        }
    }

    fn format(&self) -> String {
        format!(
            "=== System Information ===\n\
             OS: {}\n\
             OS Version: {}\n\
             Architecture: {}\n\
             Hostname: {}\n\
             Kernel Version: {}",
            self.os_type, self.os_version, self.architecture, self.hostname, self.kernel_version
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

        // Get thread count from std::thread
        let thread_count = std::thread::available_parallelism().map(|p| p.get()).unwrap_or(1);

        Self {
            process_id: pid.as_u32(),
            memory_usage_mb,
            cpu_usage_percent,
            thread_count,
        }
    }

    fn format(&self) -> String {
        format!(
            "=== Runtime Information ===\n\
             Process ID: {}\n\
             Memory Usage: {:.2} MB\n\
             CPU Usage: {:.2}%\n\
             Thread Count: {}",
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
    // Get config snapshot from global storage
    let snapshot = super::get_or_init_config_snapshot();

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
         Buffer Profile: {}",
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
        snapshot.buffer_profile
    )
}

/// Dependency information
fn format_deps_info() -> String {
    // Internal RustFS crates with their descriptions
    let internal_crates = [
        ("rustfs-appauth", "Application authentication and authorization"),
        ("rustfs-audit", "Audit target management system"),
        ("rustfs-checksums", "Client checksums"),
        ("rustfs-common", "Shared utilities and data structures"),
        ("rustfs-config", "Configuration management"),
        ("rustfs-credentials", "Credential management system"),
        ("rustfs-crypto", "Cryptography and security features"),
        ("rustfs-ecstore", "Erasure coding storage implementation"),
        ("rustfs-filemeta", "File metadata management"),
        ("rustfs-heal", "Erasure set and object healing"),
        ("rustfs-iam", "Identity and Access Management"),
        ("rustfs-keystone", "OpenStack Keystone integration"),
        ("rustfs-kms", "Key Management Service"),
        ("rustfs-lock", "Distributed locking implementation"),
        ("rustfs-madmin", "Management dashboard and admin API"),
        ("rustfs-mcp", "MCP server for S3 operations"),
        ("rustfs-metrics", "Metrics collection and reporting"),
        ("rustfs-notify", "Notification system for events"),
        ("rustfs-obs", "Observability utilities"),
        ("rustfs-policy", "Policy management"),
        ("rustfs-protocols", "Protocol implementations (FTPS, SFTP, etc.)"),
        ("rustfs-protos", "Protocol buffer definitions"),
        ("rustfs-rio", "Rust I/O utilities and abstractions"),
        ("rustfs-s3-common", "Common utilities for S3 compatibility"),
        ("rustfs-s3select-api", "S3 Select API interface"),
        ("rustfs-s3select-query", "S3 Select query engine"),
        ("rustfs-scanner", "Scanner for data integrity checks"),
        ("rustfs-signer", "Client signer"),
        ("rustfs-targets", "Target-specific configurations"),
        ("rustfs-trusted-proxies", "Trusted proxies management"),
        ("rustfs-utils", "Utility functions and helpers"),
        ("rustfs-workers", "Worker thread pools and task scheduling"),
        ("rustfs-zip", "ZIP file handling"),
    ];

    let mut output = String::from("=== Internal Crates Information ===\n");
    output.push_str(&format!("Total Internal Crates: {}\n\n", internal_crates.len()));

    for (name, description) in internal_crates {
        output.push_str(&format!("  {} - {}\n", name, description));
    }

    output.push_str("\n--- External Dependencies (Key) ---\n");
    output.push_str("  axum - Web framework\n");
    output.push_str("  tokio - Async runtime\n");
    output.push_str("  hyper - HTTP library\n");
    output.push_str("  serde - Serialization framework\n");
    output.push_str("  clap - Command line parser\n");
    output.push_str("  sysinfo - System information\n");
    output.push_str("  shadow-rs - Build-time information\n");
    output.push_str("  datafusion - SQL query engine\n");
    output.push_str("  tonic - gRPC framework\n");
    output.push_str("  tower - Service abstraction\n");

    output
}

/// Execute the info command
pub fn execute_info(opts: &InfoOpts) {
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
