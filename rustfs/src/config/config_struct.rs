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

//! Server configuration.
//!
//! This module contains the `Config` struct which holds the final server configuration
//! after processing command line arguments, environment variables, and files.

use super::Opt;
use crate::apply_external_env_compat;
use rustfs_config::{
    DEFAULT_CONSOLE_ADDRESS, DEFAULT_CONSOLE_ENABLE, ENV_RUSTFS_ACCESS_KEY, ENV_RUSTFS_SECRET_KEY, RUSTFS_REGION,
};
use rustfs_credentials::{DEFAULT_ACCESS_KEY, DEFAULT_SECRET_KEY, Masked};
use std::collections::HashSet;
use std::sync::{Mutex, OnceLock};

pub(crate) const LEGACY_ENV_RUSTFS_ROOT_USER: &str = "RUSTFS_ROOT_USER";
pub(crate) const LEGACY_ENV_RUSTFS_ROOT_PASSWORD: &str = "RUSTFS_ROOT_PASSWORD";
static LEGACY_CREDENTIAL_WARNED_KEYS: OnceLock<Mutex<HashSet<String>>> = OnceLock::new();

fn warn_legacy_credential_env_once(legacy_key: &str, canonical_key: &str) {
    let warned = LEGACY_CREDENTIAL_WARNED_KEYS.get_or_init(|| Mutex::new(HashSet::new()));
    let mut warned = match warned.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    if warned.insert(legacy_key.to_string()) {
        tracing::warn!(
            "Environment variable {} is deprecated and will be removed at GA; use {} instead",
            legacy_key,
            canonical_key
        );
    }
}

/// Helper function to resolve credentials from multiple sources with precedence:
/// 1. Inline value (if provided)
/// 2. File value (if provided, read the content of the file)
/// 3. Canonical environment variable (if set)
/// 4. Legacy environment aliases (if set)
/// 5. Default value (if none of the above are provided)
pub(crate) fn resolve_credential<T: AsRef<std::path::Path>>(
    inline_value: Option<String>,
    file_value: Option<T>,
    env_key: &str,
    legacy_env_keys: &[&str],
    default_value: &str,
) -> std::io::Result<String> {
    let value = if let Some(value) = inline_value {
        value
    } else if let Some(path) = file_value {
        std::fs::read_to_string(path)?
    } else if let Some(value) = rustfs_utils::get_env_opt_str(env_key) {
        value
    } else if let Some((legacy_key, value)) = legacy_env_keys
        .iter()
        .find_map(|legacy_key| rustfs_utils::get_env_opt_str(legacy_key).map(|value| (*legacy_key, value)))
    {
        warn_legacy_credential_env_once(legacy_key, env_key);
        value
    } else {
        default_value.to_string()
    };

    Ok(value.trim().to_string())
}

/// Server configuration.
///
/// This struct holds all configuration values needed to run the server.
/// It is created from `Opt` which is parsed from command line arguments.
#[derive(Clone)]
pub struct Config {
    /// DIR points to a directory on a filesystem.
    pub volumes: Vec<String>,

    /// bind to a specific ADDRESS:PORT, ADDRESS can be an IP or hostname
    pub address: String,

    /// Domain name used for virtual-hosted-style requests.
    pub server_domains: Vec<String>,

    /// Access key used for authentication.
    pub access_key: String,

    /// Secret key used for authentication.
    pub secret_key: String,

    /// Enable console server
    pub console_enable: bool,

    /// Console server bind address
    pub console_address: String,

    /// Observability endpoint for trace, metrics and logs,only support grpc mode.
    pub obs_endpoint: String,

    /// tls path for rustfs API and console.
    pub tls_path: Option<String>,

    /// License key for enterprise features
    pub license: Option<String>,

    /// Region for the server, used for signing and other region-specific behavior
    pub region: Option<String>,

    /// Enable KMS encryption for server-side encryption
    pub kms_enable: bool,

    /// KMS backend type (local or vault)
    pub kms_backend: String,

    /// KMS key directory for local backend
    pub kms_key_dir: Option<String>,

    /// Vault address for vault backend
    pub kms_vault_address: Option<String>,

    /// Vault token for vault backend
    pub kms_vault_token: Option<String>,

    /// Vault mount path for vault or vault-transit backend
    pub kms_vault_mount_path: Option<String>,

    /// Default KMS key ID for encryption
    pub kms_default_key_id: Option<String>,

    /// Disable adaptive buffer sizing with workload profiles
    pub buffer_profile_disable: bool,

    /// Workload profile for adaptive buffer sizing
    pub buffer_profile: String,
}

impl Config {
    /// Create a `Config` with sensible defaults for the given volumes and address.
    ///
    /// This is the programmatic alternative to [`Opt::parse_command`] which reads
    /// from the CLI / environment. Useful for embedded / integration-test usage.
    pub fn new(address: impl Into<String>, volumes: Vec<String>) -> Self {
        Config {
            volumes,
            address: address.into(),
            server_domains: Vec::new(),
            access_key: DEFAULT_ACCESS_KEY.to_string(),
            secret_key: DEFAULT_SECRET_KEY.to_string(),
            console_enable: DEFAULT_CONSOLE_ENABLE,
            console_address: DEFAULT_CONSOLE_ADDRESS.to_string(),
            obs_endpoint: rustfs_config::DEFAULT_OBS_ENDPOINT.to_string(),
            tls_path: None,
            license: None,
            region: Some(RUSTFS_REGION.to_string()),
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

    /// Create Config from Opt
    pub(super) fn from_opt(opt: Opt) -> std::io::Result<Self> {
        let Opt {
            volumes,
            address,
            server_domains,
            access_key,
            access_key_file,
            secret_key,
            secret_key_file,
            console_enable,
            console_address,
            obs_endpoint,
            tls_path,
            license,
            region,
            kms_enable,
            kms_backend,
            kms_key_dir,
            kms_vault_address,
            kms_vault_token,
            kms_vault_mount_path,
            kms_default_key_id,
            buffer_profile_disable,
            buffer_profile,
        } = opt;

        let access_key = resolve_credential(
            access_key,
            access_key_file.as_ref(),
            ENV_RUSTFS_ACCESS_KEY,
            &[LEGACY_ENV_RUSTFS_ROOT_USER],
            DEFAULT_ACCESS_KEY,
        )?;
        let secret_key = resolve_credential(
            secret_key,
            secret_key_file.as_ref(),
            ENV_RUSTFS_SECRET_KEY,
            &[LEGACY_ENV_RUSTFS_ROOT_PASSWORD],
            DEFAULT_SECRET_KEY,
        )?;

        // Region is optional, but if not set, we should default to "us-east-1" for signing compatibility with AWS S3 clients
        let region = region.or_else(|| Some(RUSTFS_REGION.to_string()));

        Ok(Config {
            volumes,
            address,
            server_domains,
            access_key,
            secret_key,
            console_enable,
            console_address,
            obs_endpoint,
            tls_path,
            license,
            region,
            kms_enable,
            kms_backend,
            kms_key_dir,
            kms_vault_address,
            kms_vault_token,
            kms_vault_mount_path,
            kms_default_key_id,
            buffer_profile_disable,
            buffer_profile,
        })
    }

    /// Parse the command line arguments and environment arguments from [`Opt`] and convert them
    /// into a ready to use [`Config`].
    ///
    /// Supports both `rustfs <volume>` (legacy) and `rustfs server <volume>`.
    ///
    /// This includes some intermediate checks for mutually exclusive options.
    #[allow(dead_code)] // used in config_test
    pub fn parse() -> std::io::Result<Self> {
        let _ = apply_external_env_compat();
        let args: Vec<String> = std::env::args().collect();
        let opt = Opt::parse_from(args);
        Self::from_opt(opt)
    }
}

impl std::fmt::Debug for Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Config")
            .field("volumes", &self.volumes)
            .field("address", &self.address)
            .field("server_domains", &self.server_domains)
            .field("access_key", &self.access_key)
            .field("secret_key", &Masked(Some(&self.secret_key))) // Hide sensitive values
            .field("console_enable", &self.console_enable)
            .field("console_address", &self.console_address)
            .field("obs_endpoint", &self.obs_endpoint)
            .field("tls_path", &self.tls_path)
            .field("license", &Masked(self.license.as_deref()))
            .field("region", &self.region)
            .field("kms_enable", &self.kms_enable)
            .field("kms_backend", &self.kms_backend)
            .field("kms_key_dir", &self.kms_key_dir)
            .field("kms_vault_address", &self.kms_vault_address)
            .field("kms_vault_token", &Masked(self.kms_vault_token.as_deref()))
            .field("kms_vault_mount_path", &self.kms_vault_mount_path)
            .field("kms_default_key_id", &self.kms_default_key_id)
            .field("buffer_profile_disable", &self.buffer_profile_disable)
            .field("buffer_profile", &self.buffer_profile)
            .finish()
    }
}
