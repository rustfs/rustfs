use clap::Parser;
use const_str::concat;
use ecstore::global::DEFAULT_PORT;
use std::string::ToString;

shadow_rs::shadow!(build);

/// Default Access Key
/// Default value: rustfsadmin
/// Environment variable: RUSTFS_ACCESS_KEY
/// Command line argument: --access-key
/// Example: RUSTFS_ACCESS_KEY=rustfsadmin
/// Example: --access-key rustfsadmin
pub const DEFAULT_ACCESS_KEY: &str = "rustfsadmin";
/// Default Secret Key
/// Default value: rustfsadmin
/// Environment variable: RUSTFS_SECRET_KEY
/// Command line argument: --secret-key
/// Example: RUSTFS_SECRET_KEY=rustfsadmin
/// Example: --secret-key rustfsadmin
pub const DEFAULT_SECRET_KEY: &str = "rustfsadmin";
/// Default configuration file for observability
/// Default value: config/obs.toml
/// Environment variable: RUSTFS_OBS_CONFIG
/// Command line argument: --obs-config
/// Example: RUSTFS_OBS_CONFIG=config/obs.toml
/// Example: --obs-config config/obs.toml
/// Example: --obs-config /etc/rustfs/obs.toml
pub const DEFAULT_OBS_CONFIG: &str = "config/obs.toml";

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
    #[arg(long, default_value_t = format!("0.0.0.0:{}", DEFAULT_PORT), env = "RUSTFS_ADDRESS")]
    pub address: String,

    #[arg(long, env = "RUSTFS_SERVER_DOMAINS")]
    pub server_domains: Vec<String>,

    /// Access key used for authentication.
    #[arg(long, default_value_t = DEFAULT_ACCESS_KEY.to_string(), env = "RUSTFS_ACCESS_KEY")]
    pub access_key: String,

    /// Secret key used for authentication.
    #[arg(long, default_value_t = DEFAULT_SECRET_KEY.to_string(), env = "RUSTFS_SECRET_KEY")]
    pub secret_key: String,

    /// Domain name used for virtual-hosted-style requests.
    #[arg(long, env = "RUSTFS_DOMAIN_NAME")]
    pub domain_name: Option<String>,

    #[arg(long, default_value_t = false, env = "RUSTFS_CONSOLE_ENABLE")]
    pub console_enable: bool,

    #[arg(long, default_value_t = format!("127.0.0.1:{}", 9002), env = "RUSTFS_CONSOLE_ADDRESS")]
    pub console_address: String,

    /// Observability configuration file
    /// Default value: config/obs.toml
    #[arg(long, default_value_t = DEFAULT_OBS_CONFIG.to_string(), env = "RUSTFS_OBS_CONFIG")]
    pub obs_config: String,
}
