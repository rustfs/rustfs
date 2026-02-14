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

use clap::Parser;
use clap::builder::NonEmptyStringValueParser;
use const_str::concat;
use std::path::PathBuf;
use std::string::ToString;
shadow_rs::shadow!(build);

pub mod workload_profiles;

#[cfg(test)]
mod config_test;

#[allow(clippy::const_is_empty)]
const SHORT_VERSION: &str = {
    if !build::TAG.is_empty() {
        build::TAG
    } else if !build::SHORT_COMMIT.is_empty() {
        concat!("@", build::SHORT_COMMIT)
    } else {
        build::PKG_VERSION
    }
};

const LONG_VERSION: &str = concat!(
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

#[derive(Parser, Clone)]
#[command(version = SHORT_VERSION, long_version = LONG_VERSION)]
pub struct Opt {
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

    /// Secret key used for authentication.
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
    #[arg(long, default_value_t = String::from("local"), env = "RUSTFS_KMS_BACKEND")]
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

    /// Default KMS key ID for encryption
    #[arg(long, env = "RUSTFS_KMS_DEFAULT_KEY_ID")]
    pub kms_default_key_id: Option<String>,

    /// Disable adaptive buffer sizing with workload profiles
    /// Set this flag to use legacy fixed-size buffer behavior from PR #869
    #[arg(long, default_value_t = false, env = "RUSTFS_BUFFER_PROFILE_DISABLE")]
    pub buffer_profile_disable: bool,

    /// Workload profile for adaptive buffer sizing
    /// Options: GeneralPurpose, AiTraining, DataAnalytics, WebWorkload, IndustrialIoT, SecureStorage
    #[arg(long, default_value_t = String::from("GeneralPurpose"), env = "RUSTFS_BUFFER_PROFILE")]
    pub buffer_profile: String,
}

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

    pub license: Option<String>,

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

    /// Default KMS key ID for encryption
    pub kms_default_key_id: Option<String>,

    /// Disable adaptive buffer sizing with workload profiles
    /// Set this flag to use legacy fixed-size buffer behavior from PR #869
    pub buffer_profile_disable: bool,

    /// Workload profile for adaptive buffer sizing
    /// Options: GeneralPurpose, AiTraining, DataAnalytics, WebWorkload, IndustrialIoT, SecureStorage
    pub buffer_profile: String,
}

impl Config {
    /// parse the command line arguments and environment arguments from [`Opt`] and convert them
    /// into a ready to use [`Config`]
    ///
    /// This includes some intermediate checks for mutual exclusive options
    pub fn parse() -> Self {
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
            kms_default_key_id,
            buffer_profile_disable,
            buffer_profile,
        } = Opt::parse();

        let access_key = access_key
            .or_else(|| {
                let path = access_key_file.as_ref()?;
                std::fs::read_to_string(path).ok()
            })
            .unwrap_or_else(|| {
                // neither argument was specified ... using default
                rustfs_credentials::DEFAULT_ACCESS_KEY.to_string()
            });

        let secret_key = secret_key
            .or_else(|| {
                let path = secret_key_file.as_ref()?;
                std::fs::read_to_string(path).ok()
            })
            .unwrap_or_else(|| {
                // neither argument was specified ... using default
                rustfs_credentials::DEFAULT_SECRET_KEY.to_string()
            });

        Config {
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
            kms_default_key_id,
            buffer_profile_disable,
            buffer_profile,
        }
    }
}

impl std::fmt::Debug for Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Config")
            .field("volumes", &self.volumes)
            .field("address", &self.address)
            .field("server_domains", &self.server_domains)
            .field("access_key", &self.access_key)
            .field("secret_key", &rustfs_credentials::Masked(Some(&self.secret_key))) // Hide sensitive values
            .field("console_enable", &self.console_enable)
            .field("console_address", &self.console_address)
            .field("obs_endpoint", &self.obs_endpoint)
            .field("tls_path", &self.tls_path)
            .field("license", &rustfs_credentials::Masked(self.license.as_deref()))
            .field("region", &self.region)
            .field("kms_enable", &self.kms_enable)
            .field("kms_backend", &self.kms_backend)
            .field("kms_key_dir", &self.kms_key_dir)
            .field("kms_vault_address", &self.kms_vault_address)
            .field("kms_vault_token", &rustfs_credentials::Masked(self.kms_vault_token.as_deref()))
            .field("kms_default_key_id", &self.kms_default_key_id)
            .field("buffer_profile_disable", &self.buffer_profile_disable)
            .field("buffer_profile", &self.buffer_profile)
            .finish()
    }
}

// lazy_static::lazy_static! {
//     pub(crate)  static ref OPT: OnceLock<Opt> = OnceLock::new();
// }

// pub fn init_config(opt: Opt) {
//     OPT.set(opt).expect("Failed to set global config");
// }

// pub fn get_config() -> &'static Opt {
//     OPT.get().expect("Global config not initialized")
// }
