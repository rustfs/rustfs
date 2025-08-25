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

use rustfs_config::observability::{
    DEFAULT_AUDIT_LOGGER_QUEUE_CAPACITY, DEFAULT_SINKS_FILE_BUFFER_SIZE, DEFAULT_SINKS_FILE_FLUSH_INTERVAL_MS,
    DEFAULT_SINKS_FILE_FLUSH_THRESHOLD, DEFAULT_SINKS_KAFKA_BATCH_SIZE, DEFAULT_SINKS_KAFKA_BATCH_TIMEOUT_MS,
    DEFAULT_SINKS_KAFKA_BROKERS, DEFAULT_SINKS_KAFKA_TOPIC, DEFAULT_SINKS_WEBHOOK_AUTH_TOKEN, DEFAULT_SINKS_WEBHOOK_ENDPOINT,
    DEFAULT_SINKS_WEBHOOK_MAX_RETRIES, DEFAULT_SINKS_WEBHOOK_RETRY_DELAY_MS, ENV_AUDIT_LOGGER_QUEUE_CAPACITY, ENV_OBS_ENDPOINT,
    ENV_OBS_ENVIRONMENT, ENV_OBS_LOCAL_LOGGING_ENABLED, ENV_OBS_LOG_FILENAME, ENV_OBS_LOG_KEEP_FILES,
    ENV_OBS_LOG_ROTATION_SIZE_MB, ENV_OBS_LOG_ROTATION_TIME, ENV_OBS_LOGGER_LEVEL, ENV_OBS_METER_INTERVAL, ENV_OBS_SAMPLE_RATIO,
    ENV_OBS_SERVICE_NAME, ENV_OBS_SERVICE_VERSION, ENV_SINKS_FILE_BUFFER_SIZE, ENV_SINKS_FILE_FLUSH_INTERVAL_MS,
    ENV_SINKS_FILE_FLUSH_THRESHOLD, ENV_SINKS_FILE_PATH, ENV_SINKS_KAFKA_BATCH_SIZE, ENV_SINKS_KAFKA_BATCH_TIMEOUT_MS,
    ENV_SINKS_KAFKA_BROKERS, ENV_SINKS_KAFKA_TOPIC, ENV_SINKS_WEBHOOK_AUTH_TOKEN, ENV_SINKS_WEBHOOK_ENDPOINT,
    ENV_SINKS_WEBHOOK_MAX_RETRIES, ENV_SINKS_WEBHOOK_RETRY_DELAY_MS,
};
use rustfs_config::observability::{ENV_OBS_LOG_DIRECTORY, ENV_OBS_USE_STDOUT};
use rustfs_config::{
    APP_NAME, DEFAULT_LOG_KEEP_FILES, DEFAULT_LOG_LEVEL, DEFAULT_LOG_ROTATION_SIZE_MB, DEFAULT_LOG_ROTATION_TIME,
    DEFAULT_OBS_LOG_FILENAME, ENVIRONMENT, METER_INTERVAL, SAMPLE_RATIO, SERVICE_VERSION, USE_STDOUT,
};
use rustfs_utils::dirs::get_log_directory_to_string;
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
    // Added flexi_logger related configurations
    pub log_directory: Option<String>,     // LOG FILE DIRECTORY
    pub log_filename: Option<String>,      // The name of the log file
    pub log_rotation_size_mb: Option<u64>, // Log file size cut threshold (MB)
    pub log_rotation_time: Option<String>, // Logs are cut by time (Hour， Day，Minute， Second)
    pub log_keep_files: Option<u16>,       // Number of log files to be retained
}

impl OtelConfig {
    /// Helper function: Extract observable configuration from environment variables
    pub fn extract_otel_config_from_env(endpoint: Option<String>) -> OtelConfig {
        let endpoint = if let Some(endpoint) = endpoint {
            if endpoint.is_empty() {
                env::var(ENV_OBS_ENDPOINT).unwrap_or_else(|_| "".to_string())
            } else {
                endpoint
            }
        } else {
            env::var(ENV_OBS_ENDPOINT).unwrap_or_else(|_| "".to_string())
        };
        let mut use_stdout = env::var(ENV_OBS_USE_STDOUT)
            .ok()
            .and_then(|v| v.parse().ok())
            .or(Some(USE_STDOUT));
        if endpoint.is_empty() {
            use_stdout = Some(true);
        }

        OtelConfig {
            endpoint,
            use_stdout,
            sample_ratio: env::var(ENV_OBS_SAMPLE_RATIO)
                .ok()
                .and_then(|v| v.parse().ok())
                .or(Some(SAMPLE_RATIO)),
            meter_interval: env::var(ENV_OBS_METER_INTERVAL)
                .ok()
                .and_then(|v| v.parse().ok())
                .or(Some(METER_INTERVAL)),
            service_name: env::var(ENV_OBS_SERVICE_NAME)
                .ok()
                .and_then(|v| v.parse().ok())
                .or(Some(APP_NAME.to_string())),
            service_version: env::var(ENV_OBS_SERVICE_VERSION)
                .ok()
                .and_then(|v| v.parse().ok())
                .or(Some(SERVICE_VERSION.to_string())),
            environment: env::var(ENV_OBS_ENVIRONMENT)
                .ok()
                .and_then(|v| v.parse().ok())
                .or(Some(ENVIRONMENT.to_string())),
            logger_level: env::var(ENV_OBS_LOGGER_LEVEL)
                .ok()
                .and_then(|v| v.parse().ok())
                .or(Some(DEFAULT_LOG_LEVEL.to_string())),
            local_logging_enabled: env::var(ENV_OBS_LOCAL_LOGGING_ENABLED)
                .ok()
                .and_then(|v| v.parse().ok())
                .or(Some(false)),
            log_directory: Some(get_log_directory_to_string(ENV_OBS_LOG_DIRECTORY)),
            log_filename: env::var(ENV_OBS_LOG_FILENAME)
                .ok()
                .and_then(|v| v.parse().ok())
                .or(Some(DEFAULT_OBS_LOG_FILENAME.to_string())),
            log_rotation_size_mb: env::var(ENV_OBS_LOG_ROTATION_SIZE_MB)
                .ok()
                .and_then(|v| v.parse().ok())
                .or(Some(DEFAULT_LOG_ROTATION_SIZE_MB)), // Default to 100 MB
            log_rotation_time: env::var(ENV_OBS_LOG_ROTATION_TIME)
                .ok()
                .and_then(|v| v.parse().ok())
                .or(Some(DEFAULT_LOG_ROTATION_TIME.to_string())), // Default to "Day"
            log_keep_files: env::var(ENV_OBS_LOG_KEEP_FILES)
                .ok()
                .and_then(|v| v.parse().ok())
                .or(Some(DEFAULT_LOG_KEEP_FILES)), // Default to keeping 30 log files
        }
    }

    /// Create a new instance of OtelConfig with default values
    ///
    /// # Returns
    /// A new instance of OtelConfig
    pub fn new() -> Self {
        Self::extract_otel_config_from_env(None)
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
        Self::default()
    }
}

impl Default for KafkaSinkConfig {
    fn default() -> Self {
        Self {
            brokers: env::var(ENV_SINKS_KAFKA_BROKERS)
                .ok()
                .filter(|s| !s.trim().is_empty())
                .unwrap_or_else(|| DEFAULT_SINKS_KAFKA_BROKERS.to_string()),
            topic: env::var(ENV_SINKS_KAFKA_TOPIC)
                .ok()
                .filter(|s| !s.trim().is_empty())
                .unwrap_or_else(|| DEFAULT_SINKS_KAFKA_TOPIC.to_string()),
            batch_size: env::var(ENV_SINKS_KAFKA_BATCH_SIZE)
                .ok()
                .and_then(|v| v.parse().ok())
                .or(Some(DEFAULT_SINKS_KAFKA_BATCH_SIZE)),
            batch_timeout_ms: env::var(ENV_SINKS_KAFKA_BATCH_TIMEOUT_MS)
                .ok()
                .and_then(|v| v.parse().ok())
                .or(Some(DEFAULT_SINKS_KAFKA_BATCH_TIMEOUT_MS)),
        }
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
        Self::default()
    }
}

impl Default for WebhookSinkConfig {
    fn default() -> Self {
        Self {
            endpoint: env::var(ENV_SINKS_WEBHOOK_ENDPOINT)
                .ok()
                .filter(|s| !s.trim().is_empty())
                .unwrap_or_else(|| DEFAULT_SINKS_WEBHOOK_ENDPOINT.to_string()),
            auth_token: env::var(ENV_SINKS_WEBHOOK_AUTH_TOKEN)
                .ok()
                .filter(|s| !s.trim().is_empty())
                .unwrap_or_else(|| DEFAULT_SINKS_WEBHOOK_AUTH_TOKEN.to_string()),
            max_retries: env::var(ENV_SINKS_WEBHOOK_MAX_RETRIES)
                .ok()
                .and_then(|v| v.parse().ok())
                .or(Some(DEFAULT_SINKS_WEBHOOK_MAX_RETRIES)),
            retry_delay_ms: env::var(ENV_SINKS_WEBHOOK_RETRY_DELAY_MS)
                .ok()
                .and_then(|v| v.parse().ok())
                .or(Some(DEFAULT_SINKS_WEBHOOK_RETRY_DELAY_MS)),
        }
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
    pub fn new() -> Self {
        Self::default()
    }
}

impl Default for FileSinkConfig {
    fn default() -> Self {
        Self {
            path: get_log_directory_to_string(ENV_SINKS_FILE_PATH),
            buffer_size: env::var(ENV_SINKS_FILE_BUFFER_SIZE)
                .ok()
                .and_then(|v| v.parse().ok())
                .or(Some(DEFAULT_SINKS_FILE_BUFFER_SIZE)),
            flush_interval_ms: env::var(ENV_SINKS_FILE_FLUSH_INTERVAL_MS)
                .ok()
                .and_then(|v| v.parse().ok())
                .or(Some(DEFAULT_SINKS_FILE_FLUSH_INTERVAL_MS)),
            flush_threshold: env::var(ENV_SINKS_FILE_FLUSH_THRESHOLD)
                .ok()
                .and_then(|v| v.parse().ok())
                .or(Some(DEFAULT_SINKS_FILE_FLUSH_THRESHOLD)),
        }
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
            queue_capacity: env::var(ENV_AUDIT_LOGGER_QUEUE_CAPACITY)
                .ok()
                .and_then(|v| v.parse().ok())
                .or(Some(DEFAULT_AUDIT_LOGGER_QUEUE_CAPACITY)),
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
///
/// let config = AppConfig::new_with_endpoint(None);
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

    pub fn new_with_endpoint(endpoint: Option<String>) -> Self {
        Self {
            observability: OtelConfig::extract_otel_config_from_env(endpoint),
            sinks: vec![SinkConfig::new()],
            logger: Some(LoggerConfig::new()),
        }
    }
}

// implement default for AppConfig
impl Default for AppConfig {
    fn default() -> Self {
        Self::new()
    }
}
