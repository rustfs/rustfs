use config::{Config, File, FileFormat};
use serde::Deserialize;
use std::env;

/// OpenTelemetry Configuration
/// Add service name, service version, deployment environment
/// Add interval time for metric collection
/// Add sample ratio for trace sampling
/// Add endpoint for metric collection
/// Add use_stdout for output to stdout
#[derive(Debug, Deserialize, Clone, Default)]
pub struct OtelConfig {
    pub endpoint: String,
    pub use_stdout: bool,
    pub sample_ratio: f64,
    pub meter_interval: u64,
    pub service_name: String,
    pub service_version: String,
    pub deployment_environment: String,
}

/// Kafka Sink Configuration - Add batch parameters
#[derive(Debug, Deserialize, Clone, Default)]
pub struct KafkaSinkConfig {
    pub enabled: bool,
    pub bootstrap_servers: String,
    pub topic: String,
    pub batch_size: Option<usize>,     // Batch size, default 100
    pub batch_timeout_ms: Option<u64>, // Batch timeout time, default 1000ms
}

/// Webhook Sink Configuration - Add Retry Parameters
#[derive(Debug, Deserialize, Clone, Default)]
pub struct WebhookSinkConfig {
    pub enabled: bool,
    pub url: String,
    pub max_retries: Option<usize>,  // Maximum number of retry times, default 3
    pub retry_delay_ms: Option<u64>, // Retry the delay cardinality, default 100ms
}

/// File Sink Configuration - Add buffering parameters
#[derive(Debug, Deserialize, Clone, Default)]
pub struct FileSinkConfig {
    pub enabled: bool,
    pub path: String,
    pub buffer_size: Option<usize>,     // Write buffer size, default 8192
    pub flush_interval_ms: Option<u64>, // Refresh interval time, default 1000ms
    pub flush_threshold: Option<usize>, // Refresh threshold, default 100 logs
}

/// Sink configuration collection
#[derive(Debug, Deserialize, Clone, Default)]
pub struct SinkConfig {
    pub kafka: KafkaSinkConfig,
    pub webhook: WebhookSinkConfig,
    pub file: FileSinkConfig,
}

///Logger Configuration
#[derive(Debug, Deserialize, Clone, Default)]
pub struct LoggerConfig {
    pub queue_capacity: Option<usize>,
}

/// Overall application configuration
/// Add observability, sinks, and logger configuration
///
/// Observability: OpenTelemetry configuration
/// Sinks: Kafka, Webhook, File sink configuration
/// Logger: Logger configuration
///
/// # Example
/// ```
/// use rustfs_obs::AppConfig;
/// use rustfs_obs::load_config;
///
/// let config = load_config(None);
/// ```
#[derive(Debug, Deserialize, Clone, Default)]
pub struct AppConfig {
    pub observability: OtelConfig,
    pub sinks: SinkConfig,
    pub logger: LoggerConfig,
}

const DEFAULT_CONFIG_FILE: &str = "obs";

/// Loading the configuration file
/// Supports TOML, YAML and .env formats, read in order by priority
///
/// # Parameters
/// - `config_dir`: Configuration file path
///
/// # Returns
/// Configuration information
///
/// # Example
/// ```
/// use rustfs_obs::AppConfig;
/// use rustfs_obs::load_config;
///
/// let config = load_config(None);
/// ```
pub fn load_config(config_dir: Option<String>) -> AppConfig {
    let config_dir = if let Some(path) = config_dir {
        // Use the provided path
        let path = std::path::Path::new(&path);
        if path.extension().is_some() {
            // If path has extension, use it as is (extension will be added by Config::builder)
            path.with_extension("").to_string_lossy().into_owned()
        } else {
            // If path is a directory, append the default config file name
            path.to_string_lossy().into_owned()
        }
    } else {
        // If no path provided, use current directory + default config file
        match env::current_dir() {
            Ok(dir) => dir.join(DEFAULT_CONFIG_FILE).to_string_lossy().into_owned(),
            Err(_) => {
                eprintln!("Warning: Failed to get current directory, using default config file");
                DEFAULT_CONFIG_FILE.to_string()
            }
        }
    };

    // Log using proper logging instead of println when possible
    println!("Using config file base: {}", config_dir);

    let config = Config::builder()
        .add_source(File::with_name(config_dir.as_str()).format(FileFormat::Toml))
        .add_source(File::with_name(config_dir.as_str()).format(FileFormat::Yaml).required(false))
        .add_source(config::Environment::with_prefix(""))
        .build()
        .unwrap();

    config.try_deserialize().unwrap()
}
