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

//! Configuration error types for the trusted proxy system.

use std::net::AddrParseError;

/// Errors related to application configuration.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    /// Required environment variable is missing.
    #[error("Missing environment variable: {0}")]
    MissingEnvVar(String),

    /// Environment variable exists but could not be parsed.
    #[error("Failed to parse environment variable {0}: {1}")]
    EnvParseError(String, String),

    /// A configuration value is logically invalid.
    #[error("Invalid configuration value for {0}: {1}")]
    InvalidValue(String, String),

    /// An IP address or CIDR range is malformed.
    #[error("Invalid IP address or network: {0}")]
    InvalidIp(String),

    /// Configuration failed overall validation.
    #[error("Configuration validation failed: {0}")]
    ValidationFailed(String),

    /// Two or more configuration settings are in conflict.
    #[error("Configuration conflict: {0}")]
    Conflict(String),

    /// Error reading or parsing a configuration file.
    #[error("Config file error: {0}")]
    FileError(String),

    /// General invalid configuration error.
    #[error("Invalid config: {0}")]
    InvalidConfig(String),
}

impl From<AddrParseError> for ConfigError {
    fn from(err: AddrParseError) -> Self {
        Self::InvalidIp(err.to_string())
    }
}

impl From<ipnetwork::IpNetworkError> for ConfigError {
    fn from(err: ipnetwork::IpNetworkError) -> Self {
        Self::InvalidIp(err.to_string())
    }
}

impl ConfigError {
    /// Creates a `MissingEnvVar` error.
    pub fn missing_env_var(key: &str) -> Self {
        Self::MissingEnvVar(key.to_string())
    }

    /// Creates an `EnvParseError`.
    pub fn env_parse(key: &str, value: &str) -> Self {
        Self::EnvParseError(key.to_string(), value.to_string())
    }

    /// Creates an `InvalidValue` error.
    pub fn invalid_value(field: &str, value: &str) -> Self {
        Self::InvalidValue(field.to_string(), value.to_string())
    }
}
