use config::{Config, File, FileFormat};
use rustfs_config::{APP_NAME, DEFAULT_LOG_LEVEL, ENVIRONMENT, METER_INTERVAL, SAMPLE_RATIO, SERVICE_VERSION, USE_STDOUT};
use serde::{Deserialize, Serialize};
use std::env;

/// OpenTelemetry Configuration
/// Add service name, service version, environment
/// Add interval time for metric collection
/// Add sample ratio for trace sampling
/// Add endpoint for metric collection
/// Add use_stdout for output to stdout
/// Add logger level for log level
/// Add local_logging_enabled for local logging enabled
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct OtelConfig {
    pub endpoint: String,                    // Endpoint for metric collection
    pub use_stdout: Option<bool>,            // Output to stdout
    pub sample_ratio: Option<f64>,           // Trace sampling ratio
    pub meter_interval: Option<u64>,         // Metric collection interval
    pub service_name: Option<String>,        // Service name
    pub service_version: Option<String>,     // Service version
    pub environment: Option<String>,         // Environment
    pub logger_level: Option<String>,        // Logger level
    pub local_logging_enabled: Option<bool>, // Local logging enabled
}

/// Helper function: Extract observable configuration from environment variables
fn extract_otel_config_from_env() -> OtelConfig {
    let endpoint = env::var("RUSTFS_OBSERVABILITY_ENDPOINT").unwrap_or_else(|_| "".to_string());
    let mut use_stdout = env::var("RUSTFS_OBSERVABILITY_USE_STDOUT")
        .ok()
        .and_then(|v| v.parse().ok())
        .or(Some(USE_STDOUT));
    if endpoint.is_empty() {
        use_stdout = Some(true);
    }

    OtelConfig {
        endpoint,
        use_stdout,
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
            .or(Some(APP_NAME.to_string())),
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
            .or(Some(DEFAULT_LOG_LEVEL.to_string())),
        local_logging_enabled: env::var("RUSTFS_OBSERVABILITY_LOCAL_LOGGING_ENABLED")
            .ok()
            .and_then(|v| v.parse().ok())
            .or(Some(false)),
    }
}

impl OtelConfig {
    /// Create a new instance of OtelConfig with default values
    ///
    /// # Returns
    /// A new instance of OtelConfig
    pub fn new() -> Self {
        extract_otel_config_from_env()
    }
}

impl Default for OtelConfig {
    fn default() -> Self {
        Self::new()
    }
}

/// Kafka Sink Configuration - Add batch parameters
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct KafkaSinkConfig {
    pub brokers: String,
    pub topic: String,
    pub batch_size: Option<usize>,     // Batch size, default 100
    pub batch_timeout_ms: Option<u64>, // Batch timeout time, default 1000ms
}

impl KafkaSinkConfig {
    pub fn new() -> Self {
        Self {
            brokers: env::var("RUSTFS_SINKS_KAFKA_BROKERS")
                .ok()
                .filter(|s| !s.trim().is_empty())
                .unwrap_or_else(|| "localhost:9092".to_string()),
            topic: env::var("RUSTFS_SINKS_KAFKA_TOPIC")
                .ok()
                .filter(|s| !s.trim().is_empty())
                .unwrap_or_else(|| "default_topic".to_string()),
            batch_size: Some(100),
            batch_timeout_ms: Some(1000),
        }
    }
}

impl Default for KafkaSinkConfig {
    fn default() -> Self {
        Self::new()
    }
}

/// Webhook Sink Configuration - Add Retry Parameters
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct WebhookSinkConfig {
    pub endpoint: String,
    pub auth_token: String,
    pub max_retries: Option<usize>,  // Maximum number of retry times, default 3
    pub retry_delay_ms: Option<u64>, // Retry the delay cardinality, default 100ms
}

impl WebhookSinkConfig {
    pub fn new() -> Self {
        Self {
            endpoint: env::var("RUSTFS_SINKS_WEBHOOK_ENDPOINT")
                .ok()
                .filter(|s| !s.trim().is_empty())
                .unwrap_or_else(|| "http://localhost:8080".to_string()),
            auth_token: env::var("RUSTFS_SINKS_WEBHOOK_AUTH_TOKEN")
                .ok()
                .filter(|s| !s.trim().is_empty())
                .unwrap_or_else(|| "default_token".to_string()),
            max_retries: Some(3),
            retry_delay_ms: Some(100),
        }
    }
}

impl Default for WebhookSinkConfig {
    fn default() -> Self {
        Self::new()
    }
}

/// File Sink Configuration - Add buffering parameters
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct FileSinkConfig {
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
        println!("Using log directory: {:?}", temp_dir);
        temp_dir
            .join("rustfs.log")
            .to_str()
            .unwrap_or("rustfs/rustfs.log")
            .to_string()
    }
    pub fn new() -> Self {
        Self {
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

impl Default for FileSinkConfig {
    fn default() -> Self {
        Self::new()
    }
}

/// Sink configuration collection
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum SinkConfig {
    File(FileSinkConfig),
    Kafka(KafkaSinkConfig),
    Webhook(WebhookSinkConfig),
}

impl SinkConfig {
    pub fn new() -> Self {
        Self::File(FileSinkConfig::new())
    }
}

impl Default for SinkConfig {
    fn default() -> Self {
        Self::new()
    }
}

///Logger Configuration
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct LoggerConfig {
    pub queue_capacity: Option<usize>,
}

impl LoggerConfig {
    pub fn new() -> Self {
        Self {
            queue_capacity: Some(10000),
        }
    }
}

impl Default for LoggerConfig {
    fn default() -> Self {
        Self::new()
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
    pub sinks: Vec<SinkConfig>,
    pub logger: Option<LoggerConfig>,
}

impl AppConfig {
    /// Create a new instance of AppConfig with default values
    ///
    /// # Returns
    /// A new instance of AppConfig
    pub fn new() -> Self {
        Self {
            observability: OtelConfig::default(),
            sinks: vec![SinkConfig::default()],
            logger: Some(LoggerConfig::default()),
        }
    }
}

// implement default for AppConfig
impl Default for AppConfig {
    fn default() -> Self {
        Self::new()
    }
}

/// Default configuration file name
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
            Ok(dir) => {
                println!("Current directory: {:?}", dir);
                dir.join(DEFAULT_CONFIG_FILE).to_string_lossy().into_owned()
            }
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
