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
    /// Private key directory path (supports multiple host keys)
    pub key_dir: Option<String>,
    /// Authorized keys file path
    pub authorized_keys_file: Option<String>,
}

impl SftpConfig {
    /// Validates the configuration
    pub async fn validate(&self) -> Result<(), SftpInitError> {
        // Host key configuration (either single file or directory)
        let has_single_key = self.key_file.is_some();
        let has_key_dir = self.key_dir.is_some();

        if !has_single_key && !has_key_dir {
            return Err(SftpInitError::InvalidConfig("Either key_file or key_dir must be provided for SFTP server".to_string()));
        }

        if let Some(path) = &self.key_file {
            if !std::path::Path::new(path).exists() {
                return Err(SftpInitError::InvalidConfig(format!("Private key file not found: {}", path)));
            }
        }

        if let Some(path) = &self.key_dir {
            if !std::path::Path::new(path).exists() {
                return Err(SftpInitError::InvalidConfig(format!("Private key directory not found: {}", path)));
            }
        }

        // Validate certificate file exists if specified
        if let Some(path) = &self.cert_file
            && !std::path::Path::new(path).exists()
        {
            return Err(SftpInitError::InvalidConfig(format!("Certificate file not found: {}", path)));
        }

        // Validate authorized keys file exists if provided
        if let Some(path) = &self.authorized_keys_file
            && !std::path::Path::new(path).exists()
        {
            return Err(SftpInitError::InvalidConfig(format!("Authorized keys file not found: {}", path)));
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
            key_dir: None,
            authorized_keys_file: None,
        }
    }
}
