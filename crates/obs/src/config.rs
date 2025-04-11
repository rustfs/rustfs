use crate::global::{ENVIRONMENT, LOGGER_LEVEL, METER_INTERVAL, SAMPLE_RATIO, SERVICE_NAME, SERVICE_VERSION, USE_STDOUT};
use config::{Config, Environment, File, FileFormat};
use serde::Deserialize;
use std::env;

/// OpenTelemetry Configuration
/// Add service name, service version, environment
/// Add interval time for metric collection
/// Add sample ratio for trace sampling
/// Add endpoint for metric collection
/// Add use_stdout for output to stdout
/// Add logger level for log level
#[derive(Debug, Deserialize, Clone)]
pub struct OtelConfig {
    pub endpoint: String,
    pub use_stdout: Option<bool>,
    pub sample_ratio: Option<f64>,
    pub meter_interval: Option<u64>,
    pub service_name: Option<String>,
    pub service_version: Option<String>,
    pub environment: Option<String>,
    pub logger_level: Option<String>,
}

// 辅助函数：从环境变量中提取可观测性配置
fn extract_otel_config_from_env() -> OtelConfig {
    OtelConfig {
        endpoint: env::var("RUSTFS_OBSERVABILITY_ENDPOINT").unwrap_or_else(|_| "".to_string()),
        use_stdout: env::var("RUSTFS_OBSERVABILITY_USE_STDOUT")
            .ok()
            .and_then(|v| v.parse().ok())
            .or(Some(USE_STDOUT)),
        sample_ratio: env::var("RUSTFS_OBSERVABILITY_SAMPLE_RATIO")
            .ok()
            .and_then(|v| v.parse().ok())
            .or(Some(SAMPLE_RATIO)),
        meter_interval: env::var("RUSTFS_OBSERVABILITY_METER_INTERVAL")
            .ok()
            .and_then(|v| v.parse().ok())
            .or(Some(METER_INTERVAL)),
        service_name: env::var("RUSTFS_OBSERVABILITY_SERVICE_NAME")
            .ok()
            .and_then(|v| v.parse().ok())
            .or(Some(SERVICE_NAME.to_string())),
        service_version: env::var("RUSTFS_OBSERVABILITY_SERVICE_VERSION")
            .ok()
            .and_then(|v| v.parse().ok())
            .or(Some(SERVICE_VERSION.to_string())),
        environment: env::var("RUSTFS_OBSERVABILITY_ENVIRONMENT")
            .ok()
            .and_then(|v| v.parse().ok())
            .or(Some(ENVIRONMENT.to_string())),
        logger_level: env::var("RUSTFS_OBSERVABILITY_LOGGER_LEVEL")
            .ok()
            .and_then(|v| v.parse().ok())
            .or(Some(LOGGER_LEVEL.to_string())),
    }
}

impl Default for OtelConfig {
    fn default() -> Self {
        extract_otel_config_from_env()
    }
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
    pub endpoint: String,
    pub auth_token: String,
    pub max_retries: Option<usize>,  // Maximum number of retry times, default 3
    pub retry_delay_ms: Option<u64>, // Retry the delay cardinality, default 100ms
}

/// File Sink Configuration - Add buffering parameters
#[derive(Debug, Deserialize, Clone)]
pub struct FileSinkConfig {
    pub enabled: bool,
    pub path: String,
    pub buffer_size: Option<usize>,     // Write buffer size, default 8192
    pub flush_interval_ms: Option<u64>, // Refresh interval time, default 1000ms
    pub flush_threshold: Option<usize>, // Refresh threshold, default 100 logs
}

impl FileSinkConfig {
    pub fn get_default_log_path() -> String {
        let temp_dir = env::temp_dir().join("rustfs");

        if let Err(e) = std::fs::create_dir_all(&temp_dir) {
            eprintln!("Failed to create log directory: {}", e);
            return "rustfs/rustfs.log".to_string();
        }

        temp_dir
            .join("rustfs.log")
            .to_str()
            .unwrap_or("rustfs/rustfs.log")
            .to_string()
    }
}

impl Default for FileSinkConfig {
    fn default() -> Self {
        FileSinkConfig {
            enabled: true,
            path: env::var("RUSTFS_SINKS_FILE_PATH")
                .ok()
                .filter(|s| !s.trim().is_empty())
                .unwrap_or_else(Self::get_default_log_path),
            buffer_size: Some(8192),
            flush_interval_ms: Some(1000),
            flush_threshold: Some(100),
        }
    }
}

/// Sink configuration collection
#[derive(Debug, Deserialize, Clone)]
pub struct SinkConfig {
    pub kafka: Option<KafkaSinkConfig>,
    pub webhook: Option<WebhookSinkConfig>,
    pub file: Option<FileSinkConfig>,
}

impl Default for SinkConfig {
    fn default() -> Self {
        SinkConfig {
            kafka: None,
            webhook: None,
            file: Some(FileSinkConfig::default()),
        }
    }
}

///Logger Configuration
#[derive(Debug, Deserialize, Clone)]
pub struct LoggerConfig {
    pub queue_capacity: Option<usize>,
}

impl Default for LoggerConfig {
    fn default() -> Self {
        LoggerConfig {
            queue_capacity: Some(10000),
        }
    }
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
#[derive(Debug, Deserialize, Clone)]
pub struct AppConfig {
    pub observability: OtelConfig,
    pub sinks: SinkConfig,
    pub logger: Option<LoggerConfig>,
}

// 为 AppConfig 实现 Default
impl AppConfig {
    pub fn new() -> Self {
        Self {
            observability: OtelConfig::default(),
            sinks: SinkConfig::default(),
            logger: Some(LoggerConfig::default()),
        }
    }
}

impl Default for AppConfig {
    fn default() -> Self {
        Self::new()
    }
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
        // If a path is provided, check if it's empty
        if path.is_empty() {
            // If empty, use the default config file name
            DEFAULT_CONFIG_FILE.to_string()
        } else {
            // Use the provided path
            let path = std::path::Path::new(&path);
            if path.extension().is_some() {
                // If path has extension, use it as is (extension will be added by Config::builder)
                path.with_extension("").to_string_lossy().into_owned()
            } else {
                // If path is a directory, append the default config file name
                path.to_string_lossy().into_owned()
            }
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

    let app_config = Config::builder()
        .add_source(File::with_name(config_dir.as_str()).format(FileFormat::Toml).required(false))
        .add_source(File::with_name(config_dir.as_str()).format(FileFormat::Yaml).required(false))
        .add_source(
            Environment::default()
                .prefix("RUSTFS")
                .prefix_separator("__")
                .separator("__")
                .with_list_parse_key("volumes")
                .try_parsing(true),
        )
        .build()
        .unwrap_or_default();

    match app_config.try_deserialize::<AppConfig>() {
        Ok(app_config) => {
            println!("Parsed AppConfig: {:?}", app_config);
            app_config
        }
        Err(e) => {
            println!("Failed to deserialize config: {}", e);
            AppConfig::default()
        }
    }
}
