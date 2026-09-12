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
//! - `Commands`: Top-level server and diagnostic subcommands
//! - `ServerOpts`: Server subcommand options
//! - `InfoOpts`: Info subcommand options
//! - `TlsOpts`: TLS diagnostic subcommand options
//! - `InfoType`: Information type enum
//! - `CommandResult`: Result of parsing command line arguments

use crate::version::{self, build};
use clap::builder::NonEmptyStringValueParser;
use clap::{Args, Parser, Subcommand, ValueEnum};
use const_str::concat;
use rustfs_config::{DEFAULT_ADDRESS, DEFAULT_CONSOLE_ADDRESS, DEFAULT_CONSOLE_ENABLE, DEFAULT_OBS_ENDPOINT, ENV_RUSTFS_VOLUMES};
use std::path::PathBuf;
// build module is re-exported from crate::build

pub(super) const SHORT_VERSION: &str = version::DISPLAY_VERSION;

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
pub const KNOWN_SUBCOMMANDS: &[&str] = &["server", "info", "tls", "diagnose", "inspect", "connect"];

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
    // Preserve the traditional `rustfs help` entry point without exposing
    // Clap's generated `help` subcommand in the top-level command list.
    if first == "help" {
        let mut out = vec![args[0].clone()];
        out.extend(args[2..].iter().cloned());
        out.push("--help".to_string());
        return out;
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
#[command(disable_help_subcommand = true)]
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
    /// Inspect TLS certificate directory layout and parsing status
    Tls(TlsOpts),
    /// Analyze RustFS log files and report probable failure causes
    Diagnose(DiagnoseOpts),
    /// Offline, read-only inspection of on-disk data (no server required)
    Inspect(InspectOpts),
    /// Configure outbound RustFS Connect integration
    Connect(ConnectOpts),
}

/// RustFS Connect subcommand options
#[derive(Args, Clone)]
pub struct ConnectOpts {
    #[command(subcommand)]
    pub command: ConnectCommands,
}

/// Allow-listed RustFS Connect operations
#[derive(Subcommand, Clone)]
pub enum ConnectCommands {
    /// Exchange a protected one-time token for a durable device credential (Unix only)
    Register(ConnectRegisterOpts),
    /// Import, verify, or inspect a signed Connect service license
    License(ConnectLicenseOpts),
    /// Capture a consent-bound local profile and write a signed export
    Profile(ConnectProfileOpts),
    /// Capture allow-listed local log events and write a signed export
    Logs(ConnectLogsOpts),
}

/// `connect logs` options.
#[derive(Args, Clone)]
pub struct ConnectLogsOpts {
    /// Directory containing an enrolled Connect device identity
    #[arg(long = "state-dir")]
    pub state_dir: PathBuf,

    /// New local archive path; an existing file is never replaced
    #[arg(long)]
    pub output: PathBuf,

    /// Capture the recent configured log window or tail new events
    #[arg(long, value_enum, default_value = "batch")]
    pub mode: ConnectLogsMode,

    /// Negotiated producer schema version
    #[arg(long = "schema-version", default_value_t = 1)]
    pub schema_version: u16,

    /// Negotiated producer capability
    #[arg(long, default_value = "logs.capture@1")]
    pub capability: String,

    /// Organization resource name bound to the export
    #[arg(long, value_parser = NonEmptyStringValueParser::new())]
    pub organization: String,

    /// Cluster resource name bound to the export
    #[arg(long, value_parser = NonEmptyStringValueParser::new())]
    pub cluster: String,

    /// Cluster-device resource name bound to the export
    #[arg(long, value_parser = NonEmptyStringValueParser::new())]
    pub device: String,

    /// UUIDv7 diagnostic run identifier issued by Connect
    #[arg(long = "run-uid", value_parser = NonEmptyStringValueParser::new())]
    pub run_uid: String,

    /// UUIDv7 artifact identifier issued by Connect
    #[arg(long = "artifact-uid", value_parser = NonEmptyStringValueParser::new())]
    pub artifact_uid: String,

    /// UUIDv7 consent identifier issued by Connect
    #[arg(long = "consent-uid", value_parser = NonEmptyStringValueParser::new())]
    pub consent_uid: String,

    /// Consent policy revision bound to this capture
    #[arg(long = "policy-revision")]
    pub policy_revision: u64,

    /// Consent expiry as UTC Unix seconds
    #[arg(long = "consent-expires-at")]
    pub consent_expires_at_unix: i64,

    /// Artifact expiry as UTC Unix seconds
    #[arg(long = "expires-at")]
    pub expires_at_unix: i64,

    /// Batch lookback or live capture duration in milliseconds
    #[arg(long = "duration-millis")]
    pub duration_millis: u64,

    /// Maximum exported event count
    #[arg(long = "max-events", default_value_t = 1_024)]
    pub max_events: usize,

    /// Confirm this explicit local L3 log capture
    #[arg(long = "acknowledge-l3", required = true, action = clap::ArgAction::SetTrue)]
    pub acknowledge_l3: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub enum ConnectLogsMode {
    Batch,
    Live,
}

/// `connect profile` options.
#[derive(Args, Clone)]
pub struct ConnectProfileOpts {
    /// Directory containing an enrolled Connect device identity
    #[arg(long = "state-dir")]
    pub state_dir: PathBuf,

    /// New local archive path; an existing file is never replaced
    #[arg(long)]
    pub output: PathBuf,

    /// Profile producer to run
    #[arg(long, value_enum)]
    pub tool: ConnectProfileTool,

    /// Thread source required by the threads producer
    #[arg(long = "thread-scope", value_enum)]
    pub thread_scope: Option<ConnectThreadProfileScope>,

    /// Negotiated producer schema version
    #[arg(long = "schema-version", default_value_t = 1)]
    pub schema_version: u16,

    /// Negotiated producer capability, such as profile.memory@1
    #[arg(long, value_parser = NonEmptyStringValueParser::new())]
    pub capability: String,

    /// Organization resource name bound to the export
    #[arg(long, value_parser = NonEmptyStringValueParser::new())]
    pub organization: String,

    /// Cluster resource name bound to the export
    #[arg(long, value_parser = NonEmptyStringValueParser::new())]
    pub cluster: String,

    /// Cluster-device resource name bound to the export
    #[arg(long, value_parser = NonEmptyStringValueParser::new())]
    pub device: String,

    /// UUIDv7 diagnostic run identifier issued by Connect
    #[arg(long = "run-uid", value_parser = NonEmptyStringValueParser::new())]
    pub run_uid: String,

    /// UUIDv7 artifact identifier issued by Connect
    #[arg(long = "artifact-uid", value_parser = NonEmptyStringValueParser::new())]
    pub artifact_uid: String,

    /// UUIDv7 consent identifier issued by Connect
    #[arg(long = "consent-uid", value_parser = NonEmptyStringValueParser::new())]
    pub consent_uid: String,

    /// Consent policy revision bound to this capture
    #[arg(long = "policy-revision")]
    pub policy_revision: u64,

    /// Consent expiry as UTC Unix seconds
    #[arg(long = "consent-expires-at")]
    pub consent_expires_at_unix: i64,

    /// Artifact expiry as UTC Unix seconds
    #[arg(long = "expires-at")]
    pub expires_at_unix: i64,

    /// Maximum capture duration in milliseconds
    #[arg(long = "duration-millis")]
    pub duration_millis: u64,

    /// Sampling interval in microseconds
    #[arg(long = "sample-period-micros")]
    pub sample_period_micros: u64,

    /// Confirm this explicit local L3 profile capture
    #[arg(long = "acknowledge-l3", required = true, action = clap::ArgAction::SetTrue)]
    pub acknowledge_l3: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub enum ConnectProfileTool {
    Cpu,
    Memory,
    Threads,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub enum ConnectThreadProfileScope {
    TokioRuntime,
    NativeThreads,
}

/// `connect register` options
#[derive(Args, Clone)]
pub struct ConnectRegisterOpts {
    /// Connect agent API HTTPS base URL
    #[arg(long, value_parser = NonEmptyStringValueParser::new())]
    pub endpoint: String,

    /// PEM root CA file used only for this Connect endpoint
    #[arg(long = "ca-file")]
    pub ca_file: PathBuf,

    /// Explicit directory shared with the Connect heartbeat runtime
    #[arg(long = "state-dir")]
    pub state_dir: PathBuf,

    /// Owner-only regular token file; omit to read the token from stdin
    #[arg(long = "token-file")]
    pub token_file: Option<PathBuf>,
}

/// Signed Connect service-license operations.
#[derive(Args, Clone)]
pub struct ConnectLicenseOpts {
    #[command(subcommand)]
    pub command: ConnectLicenseCommands,
}

/// Local service-license operations.
#[derive(Subcommand, Clone)]
pub enum ConnectLicenseCommands {
    /// Verify and atomically install a downloaded or hand-carried license file
    Import(ConnectLicenseArtifactOpts),
    /// Verify a license file without changing installed state
    Verify(ConnectLicenseArtifactOpts),
    /// Verify and display the installed license for one deployment and service
    Show(ConnectLicenseScopeOpts),
}

/// Trust and scope pins shared by service-license commands.
#[derive(Args, Clone)]
pub struct ConnectLicenseScopeOpts {
    /// Directory containing local service-license state
    #[arg(long = "state-dir")]
    pub state_dir: PathBuf,

    /// File containing the pinned Ed25519 public key as canonical base64url
    #[arg(long = "public-key-file")]
    pub public_key_file: PathBuf,

    /// SHA-256 key ID for the pinned public key
    #[arg(long = "key-id", value_parser = NonEmptyStringValueParser::new())]
    pub key_id: String,

    /// Expected Connect license issuer
    #[arg(long, value_parser = NonEmptyStringValueParser::new())]
    pub issuer: String,

    /// Expected RustFS license audience
    #[arg(long, value_parser = NonEmptyStringValueParser::new())]
    pub audience: String,

    /// Expected organization resource name
    #[arg(long, value_parser = NonEmptyStringValueParser::new())]
    pub organization: String,

    /// Expected deployment resource name
    #[arg(long, value_parser = NonEmptyStringValueParser::new())]
    pub deployment: String,

    /// Expected service code
    #[arg(long = "service-code", value_parser = NonEmptyStringValueParser::new())]
    pub service_code: String,
}

/// A service-license file plus its local trust and scope pins.
#[derive(Args, Clone)]
pub struct ConnectLicenseArtifactOpts {
    /// Downloaded or hand-carried signed license artifact
    #[arg(long)]
    pub artifact: PathBuf,

    #[command(flatten)]
    pub scope: ConnectLicenseScopeOpts,
}

/// Offline inspection subcommand options
#[derive(Args, Clone)]
pub struct InspectOpts {
    #[command(subcommand)]
    pub command: InspectCommands,
}

/// Offline inspection subcommands
#[derive(Subcommand, Clone)]
pub enum InspectCommands {
    /// Export a bucket's persisted configuration bytes straight from drive roots
    /// (works even when the config XML no longer parses)
    BucketMeta(InspectBucketMetaOpts),
}

/// `inspect bucket-meta` options
#[derive(Args, Clone)]
#[command(
    after_help = "IMPORTANT: Mount every source drive read-only for forensic use. Read-only application calls cannot prevent filesystem atime updates or path replacement races on writable mounts."
)]
pub struct InspectBucketMetaOpts {
    /// Drive root path(s). Repeat for multi-drive nodes: erasure-coded metadata
    /// needs enough drives for write quorum. Source media must be mounted
    /// read-only for strict forensic use.
    #[arg(long = "path", required = true, value_parser = NonEmptyStringValueParser::new())]
    pub paths: Vec<String>,

    /// Bucket whose metadata to inspect
    #[arg(long, value_parser = NonEmptyStringValueParser::new())]
    pub bucket: String,

    /// Write the raw `.metadata.bin` blob and each stored config's exact bytes to --out
    #[arg(long)]
    pub raw: bool,

    /// New output directory for --raw (default: ./bucket-meta-<bucket>)
    #[arg(long, requires = "raw")]
    pub out: Option<std::path::PathBuf>,
}

/// Diagnose report output format
#[derive(ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
pub enum DiagnoseFormat {
    /// Terminal text report
    Text,
    /// Machine-readable JSON (stable schema_version)
    Json,
    /// Markdown, pasteable into a support ticket
    Md,
}

/// Diagnose subcommand options
#[derive(Args, Clone)]
pub struct DiagnoseOpts {
    /// Log inputs: files, directories, archives (.zip/.tar/.tar.gz/.zst/.gz), or "-" for stdin
    #[arg(required = true)]
    pub paths: Vec<String>,

    /// Output format
    #[arg(long, value_enum, default_value_t = DiagnoseFormat::Text)]
    pub format: DiagnoseFormat,

    /// Only consider events at/after this time (RFC-3339, or relative like "30m", "24h", "7d")
    #[arg(long)]
    pub since: Option<String>,

    /// Only consider events at/before this time (same syntax as --since)
    #[arg(long)]
    pub until: Option<String>,

    /// Minimum level to analyze (trace|debug|info|warn|error)
    #[arg(long)]
    pub min_level: Option<String>,

    /// Hash bucket/object/key/IP values in the report (safe to forward)
    #[arg(long)]
    pub redact: bool,

    /// Extra rules file (JSON; same-id rules override built-ins)
    #[arg(long)]
    pub rules: Option<std::path::PathBuf>,

    /// Max unmatched error patterns to list
    #[arg(long, default_value_t = 20)]
    pub top: usize,

    /// Max sample lines per finding
    #[arg(long, default_value_t = 3)]
    pub samples: usize,
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

/// TLS diagnostic subcommand options
#[derive(Args, Clone)]
pub struct TlsOpts {
    #[command(subcommand)]
    pub command: TlsCommands,
}

/// TLS diagnostic subcommands
#[derive(Subcommand, Clone)]
pub enum TlsCommands {
    /// Inspect a TLS certificate directory
    Inspect(TlsInspectOpts),
}

/// TLS inspect options
#[derive(Args, Clone)]
pub struct TlsInspectOpts {
    /// TLS directory to inspect
    #[arg(long = "path", alias = "tls-path", value_parser = NonEmptyStringValueParser::new())]
    pub path: String,
}

/// Server subcommand options
#[derive(Args, Clone)]
#[command(after_help = "Allocator reclaim environment:
  RUSTFS_ALLOCATOR_RECLAIM_ENABLED=true|false  Enable allocator page reclaim after idle samples (default: true)
  RUSTFS_ALLOCATOR_RECLAIM_INTERVAL_SECS=30    Sampling interval in seconds
  RUSTFS_ALLOCATOR_RECLAIM_FORCE=true|false    Request forceful collection when supported
  RUSTFS_ALLOCATOR_RECLAIM_IDLE_INTERVALS=3    Consecutive idle samples required before reclaim")]
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

    /// Domain name(s) for virtual-hosted-style S3 requests (comma-separated).
    ///
    /// Required for clients that default to virtual-hosted-style addressing
    /// (AWS SDK, Terraform/Pulumi, etc.), e.g. `RUSTFS_SERVER_DOMAINS=s3.example.com`
    /// so that `bucket.s3.example.com` is routed to bucket `bucket`. When unset, only
    /// path-style addressing is supported (configure clients with
    /// `s3_use_path_style = true` / `force_path_style=true`).
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

    /// Root OTLP endpoint for traces, metrics, and logs.
    /// For the current observability pipeline this should be an OTLP/HTTP base
    /// URL such as `http://otel-collector:4318` or
    /// `http://host.docker.internal:4318`.
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

    /// KMS backend type: local (development/testing only), vault or vault-kv2 (plain Vault KV v2 storage), vault-transit, static (development/testing only), aws
    #[arg(long, default_value_t = rustfs_config::DEFAULT_KMS_BACKEND.to_string(), env = "RUSTFS_KMS_BACKEND")]
    pub kms_backend: String,

    /// KMS key directory for local backend
    #[arg(long, env = "RUSTFS_KMS_KEY_DIR")]
    pub kms_key_dir: Option<String>,

    /// Master key for local KMS key-file encryption
    #[arg(long, env = "RUSTFS_KMS_LOCAL_MASTER_KEY")]
    pub kms_local_master_key: Option<String>,

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

    /// Allow development-only insecure KMS defaults
    #[arg(long, default_value_t = false, env = "RUSTFS_KMS_ALLOW_INSECURE_DEV_DEFAULTS")]
    pub kms_allow_insecure_dev_defaults: bool,

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
    /// TLS command with options
    Tls(TlsOpts),
    /// Diagnose command with options
    Diagnose(DiagnoseOpts),
    /// Inspect command with options
    Inspect(InspectOpts),
    /// One-time Connect registration command
    ConnectRegister(ConnectRegisterOpts),
    /// Local Connect service-license command
    ConnectLicense(ConnectLicenseCommands),
    /// Consent-bound local Connect profile export
    ConnectProfile(ConnectProfileOpts),
    /// Consent-bound local Connect log export
    ConnectLogs(ConnectLogsOpts),
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
        kms_local_master_key: None,
        kms_vault_address: None,
        kms_vault_token: None,
        kms_vault_mount_path: None,
        kms_default_key_id: None,
        kms_allow_insecure_dev_defaults: false,
        buffer_profile_disable: false,
        buffer_profile: "GeneralPurpose".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::{Cli, Commands, ConnectCommands, InspectCommands, preprocess_args_for_legacy};
    use crate::version;
    use clap::error::ErrorKind;
    use clap::{CommandFactory, Parser};

    #[test]
    fn preprocess_help_command_displays_top_level_help() {
        let args = preprocess_args_for_legacy(vec!["rustfs".to_string(), "help".to_string()]);

        assert_eq!(args, vec!["rustfs".to_string(), "--help".to_string()]);

        let err = match Cli::try_parse_from(args) {
            Ok(_) => panic!("rustfs help should display help"),
            Err(err) => err,
        };
        assert_eq!(err.kind(), ErrorKind::DisplayHelp);
    }

    #[test]
    fn version_flags_use_display_version() {
        let command = Cli::command();

        let short = command.render_version();
        assert!(short.contains(version::DISPLAY_VERSION));
        assert!(!short.contains("build time"));

        let long = command.render_long_version();
        assert!(long.starts_with(&format!("rustfs {}\n", version::DISPLAY_VERSION)));
        assert!(long.contains("build time"));
    }

    #[test]
    fn inspect_bucket_meta_parses_repeated_drive_paths() {
        let cli = Cli::try_parse_from([
            "rustfs",
            "inspect",
            "bucket-meta",
            "--path",
            "/data/drive-1",
            "--path",
            "/data/drive-2",
            "--bucket",
            "example-bucket",
            "--raw",
            "--out",
            "/tmp/export",
        ])
        .expect("inspect arguments should parse");

        let Some(Commands::Inspect(inspect)) = cli.command else {
            panic!("inspect command expected");
        };
        let InspectCommands::BucketMeta(opts) = inspect.command;
        assert_eq!(opts.paths, ["/data/drive-1", "/data/drive-2"]);
        assert_eq!(opts.bucket, "example-bucket");
        assert!(opts.raw);
        assert_eq!(opts.out.as_deref(), Some(std::path::Path::new("/tmp/export")));
    }

    #[test]
    fn inspect_bucket_meta_rejects_out_without_raw() {
        let err = match Cli::try_parse_from([
            "rustfs",
            "inspect",
            "bucket-meta",
            "--path",
            "/data/drive-1",
            "--bucket",
            "example-bucket",
            "--out",
            "/tmp/export",
        ]) {
            Ok(_) => panic!("--out without --raw must be rejected"),
            Err(err) => err,
        };

        assert_eq!(err.kind(), ErrorKind::MissingRequiredArgument);
    }

    #[test]
    fn connect_register_accepts_only_paths_and_endpoint_configuration() {
        let cli = Cli::try_parse_from([
            "rustfs",
            "connect",
            "register",
            "--endpoint",
            "https://connect.example/agent/",
            "--ca-file",
            "/etc/rustfs/connect-ca.pem",
            "--state-dir",
            "/var/lib/rustfs/connect",
        ])
        .expect("connect register arguments should parse");

        let Some(Commands::Connect(connect)) = cli.command else {
            panic!("connect command expected");
        };
        let ConnectCommands::Register(register) = connect.command else {
            panic!("connect register command expected");
        };
        assert_eq!(register.endpoint, "https://connect.example/agent/");
        assert_eq!(register.ca_file, std::path::Path::new("/etc/rustfs/connect-ca.pem"));
        assert_eq!(register.state_dir, std::path::Path::new("/var/lib/rustfs/connect"));
        assert!(register.token_file.is_none());
    }

    #[test]
    fn connect_register_has_no_token_value_or_environment_option() {
        for forbidden in ["--token", "--registration-token", "--token-env"] {
            let result = Cli::try_parse_from([
                "rustfs",
                "connect",
                "register",
                "--endpoint",
                "https://connect.example/agent/",
                "--ca-file",
                "/etc/rustfs/connect-ca.pem",
                "--state-dir",
                "/var/lib/rustfs/connect",
                forbidden,
                "secret",
            ]);
            let Err(error) = result else {
                panic!("secret-bearing command-line options must be rejected");
            };
            assert_eq!(error.kind(), ErrorKind::UnknownArgument);
        }
    }

    #[test]
    fn connect_register_help_states_the_unix_only_security_scope() {
        let result = Cli::try_parse_from(["rustfs", "connect", "register", "--help"]);
        let Err(help) = result else {
            panic!("help exits without running registration");
        };

        assert_eq!(help.kind(), ErrorKind::DisplayHelp);
        assert!(help.to_string().contains("Unix only"));
    }

    #[test]
    fn connect_profile_requires_explicit_l3_acknowledgement() {
        let arguments = [
            "rustfs",
            "connect",
            "profile",
            "--state-dir",
            "/var/lib/rustfs/connect",
            "--output",
            "/tmp/profile.zip",
            "--tool",
            "memory",
            "--capability",
            "profile.memory@1",
            "--organization",
            "organizations/019e3ae0-0000-7000-8000-000000000001",
            "--cluster",
            "organizations/019e3ae0-0000-7000-8000-000000000001/clusters/019e3ae0-0000-7000-8000-000000000002",
            "--device",
            "organizations/019e3ae0-0000-7000-8000-000000000001/clusters/019e3ae0-0000-7000-8000-000000000002/clusterDevices/019e3ae0-0000-7000-8000-000000000003",
            "--run-uid",
            "019e3ae0-0000-7000-8000-000000000004",
            "--artifact-uid",
            "019e3ae0-0000-7000-8000-000000000005",
            "--consent-uid",
            "019e3ae0-0000-7000-8000-000000000006",
            "--policy-revision",
            "1",
            "--consent-expires-at",
            "4102444800",
            "--expires-at",
            "4102444700",
            "--duration-millis",
            "10",
            "--sample-period-micros",
            "1000",
        ];
        let error = Cli::try_parse_from(arguments).expect_err("an incomplete unacknowledged profile must fail");
        assert_eq!(error.kind(), ErrorKind::MissingRequiredArgument);
        assert!(error.to_string().contains("--acknowledge-l3"));
    }

    #[test]
    fn connect_logs_requires_explicit_l3_acknowledgement() {
        let arguments = [
            "rustfs",
            "connect",
            "logs",
            "--state-dir",
            "/var/lib/rustfs/connect",
            "--output",
            "/tmp/logs.zip",
            "--organization",
            "organizations/019e3ae0-0000-7000-8000-000000000001",
            "--cluster",
            "organizations/019e3ae0-0000-7000-8000-000000000001/clusters/019e3ae0-0000-7000-8000-000000000002",
            "--device",
            "organizations/019e3ae0-0000-7000-8000-000000000001/clusters/019e3ae0-0000-7000-8000-000000000002/clusterDevices/019e3ae0-0000-7000-8000-000000000003",
            "--run-uid",
            "019e3ae0-0000-7000-8000-000000000004",
            "--artifact-uid",
            "019e3ae0-0000-7000-8000-000000000005",
            "--consent-uid",
            "019e3ae0-0000-7000-8000-000000000006",
            "--policy-revision",
            "1",
            "--consent-expires-at",
            "4102444800",
            "--expires-at",
            "4102444700",
            "--duration-millis",
            "1000",
        ];
        let error = Cli::try_parse_from(arguments).expect_err("unacknowledged log capture must fail");
        assert_eq!(error.kind(), ErrorKind::MissingRequiredArgument);
        assert!(error.to_string().contains("--acknowledge-l3"));
    }

    #[test]
    fn server_help_lists_allocator_reclaim_environment() {
        let result = Cli::try_parse_from(["rustfs", "server", "--help"]);
        let Err(help) = result else {
            panic!("help exits without parsing server options");
        };

        assert_eq!(help.kind(), ErrorKind::DisplayHelp);
        let help = help.to_string();
        for env in [
            "RUSTFS_ALLOCATOR_RECLAIM_ENABLED",
            "RUSTFS_ALLOCATOR_RECLAIM_INTERVAL_SECS",
            "RUSTFS_ALLOCATOR_RECLAIM_FORCE",
            "RUSTFS_ALLOCATOR_RECLAIM_IDLE_INTERVALS",
        ] {
            assert!(help.contains(env), "server help should mention {env}");
        }
    }
}
