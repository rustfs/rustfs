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

use crate::constants::app::{ENVIRONMENT, METER_INTERVAL, SAMPLE_RATIO, SERVICE_VERSION, USE_STDOUT};
use crate::{APP_NAME, DEFAULT_LOG_LEVEL};
use serde::{Deserialize, Serialize};
use std::env;

/// OpenTelemetry configuration
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

impl OtelConfig {
    pub fn new() -> Self {
        extract_otel_config_from_env()
    }
}

impl Default for OtelConfig {
    fn default() -> Self {
        Self::new()
    }
}

// Helper function: Extract observable configuration from environment variables
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
