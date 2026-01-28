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

use crate::constants::ftps;
use std::fmt::Debug;
use std::net::SocketAddr;
use thiserror::Error;

/// FTPS server initialization error
#[derive(Debug, Error)]
pub enum FtpsInitError {
    #[error("failed to bind address {0}")]
    Bind(#[from] std::io::Error),
    #[error("server error: {0}")]
    Server(#[from] libunftp::ServerError),
    #[error("invalid FTPS configuration: {0}")]
    InvalidConfig(String),
}

/// FTPS server configuration
#[derive(Debug, Clone)]
pub struct FtpsConfig {
    /// Server bind address
    pub bind_addr: SocketAddr,
    /// Passive port range (e.g., "40000-50000")
    pub passive_ports: Option<String>,
    /// External IP address for passive mode
    pub external_ip: Option<String>,
    /// Whether FTPS is required
    pub ftps_required: bool,
    /// Whether TLS is enabled (default: true)
    pub tls_enabled: bool,
    /// Certificate directory path (supports multiple certificates)
    pub cert_dir: Option<String>,
    /// CA certificate file path for client certificate verification
    pub ca_file: Option<String>,
}

impl FtpsConfig {
    /// Validates the configuration
    pub async fn validate(&self) -> Result<(), FtpsInitError> {
        if self.ftps_required && self.cert_dir.is_none() {
            return Err(FtpsInitError::InvalidConfig(
                "FTPS is required but certificate directory is missing".to_string(),
            ));
        }

        if let Some(path) = &self.cert_dir
            && !tokio::fs::try_exists(path).await.unwrap_or(false)
        {
            return Err(FtpsInitError::InvalidConfig(format!("Certificate directory not found: {}", path)));
        }

        // Validate CA file exists if specified
        if let Some(path) = &self.ca_file
            && !tokio::fs::try_exists(path).await.unwrap_or(false)
        {
            return Err(FtpsInitError::InvalidConfig(format!("CA file not found: {}", path)));
        }

        // Validate passive ports format
        if self.passive_ports.is_some() {
            self.parse_passive_ports()?;
        }

        Ok(())
    }

    /// Parse passive ports range from string format "start-end"
    pub fn parse_passive_ports(&self) -> Result<std::ops::RangeInclusive<u16>, FtpsInitError> {
        match &self.passive_ports {
            Some(ports) => {
                let parts: Vec<&str> = ports.split(ftps::PORT_RANGE_SEPARATOR).collect();
                if parts.len() != ftps::PASSIVE_PORTS_PART_COUNT {
                    return Err(FtpsInitError::InvalidConfig(format!(
                        "Invalid passive ports format: {}, expected 'start-end'",
                        ports
                    )));
                }

                let start = parts[0]
                    .parse::<u16>()
                    .map_err(|e| FtpsInitError::InvalidConfig(format!("Invalid start port: {}", e)))?;
                let end = parts[1]
                    .parse::<u16>()
                    .map_err(|e| FtpsInitError::InvalidConfig(format!("Invalid end port: {}", e)))?;
                if start > end {
                    return Err(FtpsInitError::InvalidConfig("Start port cannot be greater than end port".to_string()));
                }

                Ok(start..=end)
            }
            None => Err(FtpsInitError::InvalidConfig("No passive ports configured".to_string())),
        }
    }
}

impl Default for FtpsConfig {
    fn default() -> Self {
        Self {
            bind_addr: crate::constants::defaults::DEFAULT_FTPS_ADDRESS.parse().unwrap(),
            passive_ports: Some(crate::constants::defaults::DEFAULT_FTPS_PASSIVE_PORTS.to_string()),
            external_ip: None,
            ftps_required: false,
            tls_enabled: true,
            cert_dir: None,
            ca_file: None,
        }
    }
}
