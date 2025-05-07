use const_str::concat;

/// Application name
/// Default value: RustFs
/// Environment variable: RUSTFS_APP_NAME
pub const APP_NAME: &str = "RustFs";
/// Application version
/// Default value: 1.0.0
/// Environment variable: RUSTFS_VERSION
pub const VERSION: &str = "0.0.1";

/// Default configuration logger level
/// Default value: info
/// Environment variable: RUSTFS_LOG_LEVEL
pub const DEFAULT_LOG_LEVEL: &str = "info";

/// maximum number of connections
/// This is the maximum number of connections that the server will accept.
/// This is used to limit the number of connections to the server.
pub const MAX_CONNECTIONS: usize = 100;
/// timeout for connections
/// This is the timeout for connections to the server.
/// This is used to limit the time that a connection can be open.
pub const DEFAULT_TIMEOUT_MS: u64 = 3000;

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

/// Default TLS key for rustfs
/// This is the default key for TLS.
pub const RUSTFS_TLS_KEY: &str = "rustfs_key.pem";

/// Default TLS cert for rustfs
/// This is the default cert for TLS.
pub const RUSTFS_TLS_CERT: &str = "rustfs_cert.pem";

/// Default port for rustfs
/// This is the default port for rustfs.
/// This is used to bind the server to a specific port.
pub const DEFAULT_PORT: u16 = 9000;

/// Default address for rustfs
/// This is the default address for rustfs.
pub const DEFAULT_ADDRESS: &str = concat!(":", DEFAULT_PORT);

/// Default port for rustfs console
/// This is the default port for rustfs console.
pub const DEFAULT_CONSOLE_PORT: u16 = 9002;

/// Default address for rustfs console
/// This is the default address for rustfs console.
pub const DEFAULT_CONSOLE_ADDRESS: &str = concat!(":", DEFAULT_CONSOLE_PORT);
