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
use const_str::concat;
use std::string::ToString;
shadow_rs::shadow!(build);

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

#[derive(Debug, Parser, Clone)]
#[command(version = SHORT_VERSION, long_version = LONG_VERSION)]
pub struct Opt {
    /// DIR points to a directory on a filesystem.
    #[arg(required = true, env = "RUSTFS_VOLUMES")]
    pub volumes: Vec<String>,

    /// bind to a specific ADDRESS:PORT, ADDRESS can be an IP or hostname
    #[arg(long, default_value_t = rustfs_config::DEFAULT_ADDRESS.to_string(), env = "RUSTFS_ADDRESS")]
    pub address: String,

    /// Domain name used for virtual-hosted-style requests.
    #[arg(long, env = "RUSTFS_SERVER_DOMAINS")]
    pub server_domains: Vec<String>,

    /// Access key used for authentication.
    #[arg(long, default_value_t = rustfs_config::DEFAULT_ACCESS_KEY.to_string(), env = "RUSTFS_ACCESS_KEY")]
    pub access_key: String,

    /// Secret key used for authentication.
    #[arg(long, default_value_t = rustfs_config::DEFAULT_SECRET_KEY.to_string(), env = "RUSTFS_SECRET_KEY")]
    pub secret_key: String,

    /// Enable console server
    #[arg(long, default_value_t = rustfs_config::DEFAULT_CONSOLE_ENABLE, env = "RUSTFS_CONSOLE_ENABLE")]
    pub console_enable: bool,

    /// Console server bind address
    #[arg(long, default_value_t = rustfs_config::DEFAULT_CONSOLE_ADDRESS.to_string(), env = "RUSTFS_CONSOLE_ADDRESS")]
    pub console_address: String,

    /// External address for console to access endpoint (used in Docker deployments)
    /// This should match the mapped host port when using Docker port mapping
    /// Example: ":9020" when mapping host port 9020 to container port 9000
    #[arg(long, default_value_t = rustfs_config::DEFAULT_ADDRESS.to_string(), env = "RUSTFS_EXTERNAL_ADDRESS")]
    pub external_address: String,

    /// Observability endpoint for trace, metrics and logs,only support grpc mode.
    #[arg(long, default_value_t = rustfs_config::DEFAULT_OBS_ENDPOINT.to_string(), env = "RUSTFS_OBS_ENDPOINT")]
    pub obs_endpoint: String,

    /// tls path for rustfs API and console.
    #[arg(long, env = "RUSTFS_TLS_PATH")]
    pub tls_path: Option<String>,

    /// Enable rate limiting for console
    #[arg(long, default_value_t = rustfs_config::DEFAULT_CONSOLE_RATE_LIMIT_ENABLE, env = "RUSTFS_CONSOLE_RATE_LIMIT_ENABLE")]
    pub console_rate_limit_enable: bool,

    /// Console rate limit: requests per minute
    #[arg(long, default_value_t = rustfs_config::DEFAULT_CONSOLE_RATE_LIMIT_RPM, env = "RUSTFS_CONSOLE_RATE_LIMIT_RPM")]
    pub console_rate_limit_rpm: u32,

    /// Console authentication timeout in seconds
    #[arg(long, default_value_t = rustfs_config::DEFAULT_CONSOLE_AUTH_TIMEOUT, env = "RUSTFS_CONSOLE_AUTH_TIMEOUT")]
    pub console_auth_timeout: u64,

    #[arg(long, env = "RUSTFS_LICENSE")]
    pub license: Option<String>,

    #[arg(long, env = "RUSTFS_REGION")]
    pub region: Option<String>,
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
