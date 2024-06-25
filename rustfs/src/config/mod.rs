use clap::Parser;

/// Default port that a rustfs server listens on.
///
/// Used if no port is specified.
pub const DEFAULT_PORT: u16 = 9000;

#[derive(Debug, Parser)]
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
