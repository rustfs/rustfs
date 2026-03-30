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

//! CLI definitions and parsing logic.
//!
//! This module contains the command-line interface definitions including:
//! - `Cli`: Main CLI parser
//! - `Commands`: Subcommands (Server, Info)
//! - `ServerOpts`: Server subcommand options
//! - `InfoOpts`: Info subcommand options
//! - `InfoType`: Information type enum
//! - `CommandResult`: Result of parsing command line arguments

use crate::version::build;
use clap::builder::NonEmptyStringValueParser;
use clap::{Args, Parser, Subcommand, ValueEnum};
use const_str::concat;
use rustfs_config::{DEFAULT_ADDRESS, DEFAULT_CONSOLE_ADDRESS, DEFAULT_CONSOLE_ENABLE, DEFAULT_OBS_ENDPOINT, ENV_RUSTFS_VOLUMES};
use std::path::PathBuf;
// build module is re-exported from crate::build

#[allow(clippy::const_is_empty)]
pub(super) const SHORT_VERSION: &str = {
    if !build::TAG.is_empty() {
        build::TAG
    } else if !build::SHORT_COMMIT.is_empty() {
        concat!("@", build::SHORT_COMMIT)
    } else {
        build::PKG_VERSION
    }
};

pub(super) const LONG_VERSION: &str = concat!(
    concat!(SHORT_VERSION, "\n"),
    concat!("build time   : ", build::BUILD_TIME, "\n"),
    concat!("build profile: ", build::BUILD_RUST_CHANNEL, "\n"),
    concat!("build os     : ", build::BUILD_OS, "\n"),
    concat!("rust version : ", build::RUST_VERSION, "\n"),
    concat!("rust channel : ", build::RUST_CHANNEL, "\n"),
    concat!("git branch   : ", build::BRANCH, "\n"),
    concat!("git commit   : ", build::COMMIT_HASH, "\n"),
    concat!("git tag      : ", build::TAG, "\n"),
    concat!("git status   :\n", build::GIT_STATUS_FILE),
);

/// Known subcommands. When the first arg matches one of these, it is treated as a subcommand.
pub const KNOWN_SUBCOMMANDS: &[&str] = &["server", "info"];

/// Preprocess argv for legacy compatibility: `rustfs <volume>` and `rustfs --address ...` are
/// treated as `rustfs server <volume>` and `rustfs server --address ...` respectively.
/// Also: `rustfs` with no args becomes `rustfs server` (volumes from env), and `rustfs --info`
/// is treated as `rustfs info`.
pub fn preprocess_args_for_legacy(args: Vec<String>) -> Vec<String> {
    if args.len() < 2 {
        // rustfs -> rustfs server (volumes from RUSTFS_VOLUMES env)
        return vec![args[0].clone(), "server".to_string()];
    }
    let first = &args[1];
    // If first arg looks like a subcommand, do nothing
    if KNOWN_SUBCOMMANDS.contains(&first.as_str()) {
        return args;
    }
    // If first arg is --info, treat it as info subcommand
    if first == "--info" {
        let mut out = vec![args[0].clone(), "info".to_string()];
        out.extend(args[2..].iter().cloned());
        return out;
    }
    // If first arg is a global flag (--help, --version), do nothing
    if first == "--help" || first == "-h" || first == "--version" || first == "-V" {
        return args;
    }
    // Legacy: rustfs <volume> or rustfs --address ... -> rustfs server <volume|--address ...>
    let mut out = vec![args[0].clone(), "server".to_string()];
    out.extend(args[1..].iter().cloned());
    out
}

/// Main CLI parser
#[derive(Parser, Clone)]
#[command(name = "rustfs", version = SHORT_VERSION, long_version = LONG_VERSION)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Option<Commands>,
}

/// Available subcommands
#[derive(Subcommand, Clone)]
pub enum Commands {
    /// Start the object storage server (default when no subcommand is given)
    Server(Box<ServerOpts>),
    /// Display system information
    Info(InfoOpts),
}

/// Information type to display
#[derive(ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
pub enum InfoType {
    /// System basic information (OS, architecture, hostname, etc.)
    System,
    /// Runtime information (PID, memory, CPU, threads, etc.)
    Runtime,
    /// Build information (version, build time, git info, etc.)
    Build,
    /// Current configuration information
    Config,
    /// Dependency library versions
    Deps,
}

/// Info subcommand options
#[derive(Args, Clone)]
pub struct InfoOpts {
    /// Display all information types
    #[arg(long, conflicts_with = "info_type")]
    pub all: bool,

    /// Output in JSON format (default: markdown table)
    #[arg(long)]
    pub json: bool,

    /// Type of information to display
    #[arg(value_enum, conflicts_with = "all")]
    pub info_type: Option<InfoType>,
}

/// Server subcommand options
#[derive(Args, Clone)]
pub struct ServerOpts {
    /// DIR points to a directory on a filesystem.
    #[arg(
        required = true,
        env = "RUSTFS_VOLUMES",
        value_delimiter = ' ',
        value_parser = NonEmptyStringValueParser::new()
    )]
    pub volumes: Vec<String>,

    /// bind to a specific ADDRESS:PORT, ADDRESS can be an IP or hostname
    #[arg(
        long,
        default_value_t = rustfs_config::DEFAULT_ADDRESS.to_string(),
        env = "RUSTFS_ADDRESS"
    )]
    pub address: String,

    /// Domain name used for virtual-hosted-style requests.
    #[arg(
        long,
        env = "RUSTFS_SERVER_DOMAINS",
        value_delimiter = ',',
        value_parser = NonEmptyStringValueParser::new()
    )]
    pub server_domains: Vec<String>,

    /// Access key used for authentication.
    #[arg(long, env = "RUSTFS_ACCESS_KEY", group = "access-key")]
    pub access_key: Option<String>,

    /// Access key stored in a file used for authentication.
    #[arg(long, env = "RUSTFS_ACCESS_KEY_FILE", group = "access-key")]
    pub access_key_file: Option<PathBuf>,

    /// Secret key used for authentication.
    #[arg(long, env = "RUSTFS_SECRET_KEY", group = "secret-key")]
    pub secret_key: Option<String>,

    /// Secret key stored in a file used for authentication.
    #[arg(long, env = "RUSTFS_SECRET_KEY_FILE", group = "secret-key")]
    pub secret_key_file: Option<PathBuf>,

    /// Enable console server
    #[arg(
        long,
        default_value_t = rustfs_config::DEFAULT_CONSOLE_ENABLE,
        env = "RUSTFS_CONSOLE_ENABLE"
    )]
    pub console_enable: bool,

    /// Console server bind address
    #[arg(
        long,
        default_value_t = rustfs_config::DEFAULT_CONSOLE_ADDRESS.to_string(),
        env = "RUSTFS_CONSOLE_ADDRESS"
    )]
    pub console_address: String,

    /// Observability endpoint for trace, metrics and logs,only support grpc mode.
    #[arg(
        long,
        default_value_t = rustfs_config::DEFAULT_OBS_ENDPOINT.to_string(),
        env = "RUSTFS_OBS_ENDPOINT"
    )]
    pub obs_endpoint: String,

    /// tls path for rustfs API and console.
    #[arg(long, env = "RUSTFS_TLS_PATH")]
    pub tls_path: Option<String>,

    #[arg(long, env = "RUSTFS_LICENSE")]
    pub license: Option<String>,

    #[arg(long, env = "RUSTFS_REGION")]
    pub region: Option<String>,

    /// Enable KMS encryption for server-side encryption
    #[arg(long, default_value_t = false, env = "RUSTFS_KMS_ENABLE")]
    pub kms_enable: bool,

    /// KMS backend type (local or vault)
    #[arg(long, default_value_t = rustfs_config::DEFAULT_KMS_BACKEND.to_string(), env = "RUSTFS_KMS_BACKEND")]
    pub kms_backend: String,

    /// KMS key directory for local backend
    #[arg(long, env = "RUSTFS_KMS_KEY_DIR")]
    pub kms_key_dir: Option<String>,

    /// Vault address for vault backend
    #[arg(long, env = "RUSTFS_KMS_VAULT_ADDRESS")]
    pub kms_vault_address: Option<String>,

    /// Vault token for vault backend
    #[arg(long, env = "RUSTFS_KMS_VAULT_TOKEN")]
    pub kms_vault_token: Option<String>,

    /// Vault mount path for vault or vault-transit backend
    #[arg(long, env = "RUSTFS_KMS_VAULT_MOUNT_PATH")]
    pub kms_vault_mount_path: Option<String>,

    /// Default KMS key ID for encryption
    #[arg(long, env = "RUSTFS_KMS_DEFAULT_KEY_ID")]
    pub kms_default_key_id: Option<String>,

    /// Disable adaptive buffer sizing with workload profiles
    /// Set this flag to use legacy fixed-size buffer behavior from PR #869
    #[arg(long, default_value_t = false, env = "RUSTFS_BUFFER_PROFILE_DISABLE")]
    pub buffer_profile_disable: bool,

    /// Workload profile for adaptive buffer sizing
    /// Options: GeneralPurpose, AiTraining, DataAnalytics, WebWorkload, IndustrialIoT, SecureStorage
    #[arg(long, default_value_t = rustfs_config::DEFAULT_BUFFER_PROFILE.to_string(), env = "RUSTFS_BUFFER_PROFILE")]
    pub buffer_profile: String,
}

/// Result of parsing command line arguments
#[derive(Clone)]
pub enum CommandResult {
    /// Server command with configuration
    Server(Box<super::Config>),
    /// Info command with options
    Info(InfoOpts),
}

/// Create default ServerOpts from environment variables
pub fn default_server_opts() -> ServerOpts {
    ServerOpts {
        volumes: std::env::var(ENV_RUSTFS_VOLUMES)
            .unwrap_or_default()
            .split(' ')
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .collect(),
        address: DEFAULT_ADDRESS.to_string(),
        server_domains: vec![],
        access_key: None,
        access_key_file: None,
        secret_key: None,
        secret_key_file: None,
        console_enable: DEFAULT_CONSOLE_ENABLE,
        console_address: DEFAULT_CONSOLE_ADDRESS.to_string(),
        obs_endpoint: DEFAULT_OBS_ENDPOINT.to_string(),
        tls_path: None,
        license: None,
        region: None,
        kms_enable: false,
        kms_backend: "local".to_string(),
        kms_key_dir: None,
        kms_vault_address: None,
        kms_vault_token: None,
        kms_vault_mount_path: None,
        kms_default_key_id: None,
        buffer_profile_disable: false,
        buffer_profile: "GeneralPurpose".to_string(),
    }
}
