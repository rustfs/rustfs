use clap::Parser;
use const_str::concat;
use ecstore::global::DEFAULT_PORT;

shadow_rs::shadow!(build);

/// Default port that a rustfs server listens on.
///
/// Used if no port is specified.

pub const DEFAULT_ACCESS_KEY: &str = "rustfsadmin";
pub const DEFAULT_SECRET_KEY: &str = "rustfsadmin";

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

#[derive(Debug, Parser)]
#[command(version = SHORT_VERSION, long_version = LONG_VERSION)]
pub struct Opt {
    /// DIR points to a directory on a filesystem.
    #[arg(required = true)]
    pub volumes: Vec<String>,

    /// bind to a specific ADDRESS:PORT, ADDRESS can be an IP or hostname
    #[arg(long, default_value_t = format!("0.0.0.0:{}", DEFAULT_PORT))]
    pub address: String,

    /// Access key used for authentication.
    #[arg(long)]
    pub access_key: Option<String>,

    /// Secret key used for authentication.
    #[arg(long)]
    pub secret_key: Option<String>,

    /// Domain name used for virtual-hosted-style requests.
    #[arg(long)]
    pub domain_name: Option<String>,
}
