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
    ENV_OBS_ENDPOINT, ENV_OBS_ENVIRONMENT, ENV_OBS_LOCAL_LOGGING_ENABLED, ENV_OBS_LOG_DIRECTORY, ENV_OBS_LOG_FILENAME,
    ENV_OBS_LOG_KEEP_FILES, ENV_OBS_LOG_ROTATION_SIZE_MB, ENV_OBS_LOG_ROTATION_TIME, ENV_OBS_LOGGER_LEVEL,
    ENV_OBS_METER_INTERVAL, ENV_OBS_SAMPLE_RATIO, ENV_OBS_SERVICE_NAME, ENV_OBS_SERVICE_VERSION, ENV_OBS_USE_STDOUT,
};
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

/// Overall application configuration
/// Add observability configuration
///
/// Observability: OpenTelemetry configuration
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
}

impl AppConfig {
    /// Create a new instance of AppConfig with default values
    ///
    /// # Returns
    /// A new instance of AppConfig
    pub fn new() -> Self {
        Self {
            observability: OtelConfig::default(),
        }
    }

    pub fn new_with_endpoint(endpoint: Option<String>) -> Self {
        Self {
            observability: OtelConfig::extract_otel_config_from_env(endpoint),
        }
    }
}

// implement default for AppConfig
impl Default for AppConfig {
    fn default() -> Self {
        Self::new()
    }
}
