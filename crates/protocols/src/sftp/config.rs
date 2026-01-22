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

use std::fmt::Debug;
use std::net::SocketAddr;
use thiserror::Error;

/// SFTP server initialization error
#[derive(Debug, Error)]
pub enum SftpInitError {
    #[error("failed to bind address {0}")]
    Bind(#[from] std::io::Error),
    #[error("invalid SFTP configuration: {0}")]
    InvalidConfig(String),
}

/// SFTP server configuration
#[derive(Debug, Clone)]
pub struct SftpConfig {
    /// Server bind address
    pub bind_addr: SocketAddr,
    /// Whether key authentication is required
    pub require_key_auth: bool,
    /// Certificate file path
    pub cert_file: Option<String>,
    /// Private key file path
    pub key_file: Option<String>,
    /// Authorized keys file path
    pub authorized_keys_file: Option<String>,
}

impl SftpConfig {
    /// Validates the configuration
    pub async fn validate(&self) -> Result<(), SftpInitError> {
        // Validate private key file exists if specified
        if let Some(path) = &self.key_file
            && !tokio::fs::try_exists(path).await.unwrap_or(false)
        {
            return Err(SftpInitError::InvalidConfig(format!("Private key file not found: {}", path)));
        }

        // Validate certificate file exists if specified
        if let Some(path) = &self.cert_file
            && !tokio::fs::try_exists(path).await.unwrap_or(false)
        {
            return Err(SftpInitError::InvalidConfig(format!("Certificate file not found: {}", path)));
        }

        // Validate authorized keys file exists if key auth is required
        if self.require_key_auth {
            if let Some(path) = &self.authorized_keys_file {
                if !tokio::fs::try_exists(path).await.unwrap_or(false) {
                    return Err(SftpInitError::InvalidConfig(format!("Authorized keys file not found: {}", path)));
                }
            } else {
                return Err(SftpInitError::InvalidConfig(
                    "Key authentication is required but no authorized keys file specified".to_string(),
                ));
            }
        }

        Ok(())
    }
}

impl Default for SftpConfig {
    fn default() -> Self {
        Self {
            bind_addr: crate::constants::defaults::DEFAULT_SFTP_ADDRESS.parse().unwrap(),
            require_key_auth: true,
            cert_file: None,
            key_file: None,
            authorized_keys_file: None,
        }
    }
}
