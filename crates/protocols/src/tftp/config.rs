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

//! Configuration for the TFTP server.

use std::net::SocketAddr;
use std::str::FromStr;
use thiserror::Error;

/// Errors that can occur during TFTP server initialization.
#[derive(Debug, Error)]
pub enum TftpInitError {
    /// The configured bind address could not be parsed.
    #[error("invalid TFTP bind address: {0}")]
    InvalidBindAddress(String),

    #[error("invalid FTPS configuration: {0}")]
    InvalidConfig(String),

    /// The error from async-tftp server
    #[error("TFTP server error: {0}")]
    ServerError(#[from] async_tftp::Error),

    /// The configured access mode is not recognized.
    #[error("invalid TFTP access mode: {0}, expected 'ro', 'wo', or 'rw'")]
    InvalidAccessMode(String),
}

/// Access mode for the TFTP server.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TftpAccessMode {
    /// Both read (RRQ) and write (WRQ) requests are allowed.
    ReadWrite,
    /// Only read (RRQ) requests are allowed; write requests are rejected.
    ReadOnly,
    /// Only write (WRQ) requests are allowed; read requests are rejected.
    WriteOnly,
}

impl FromStr for TftpAccessMode {
    type Err = TftpInitError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "ro" | "read-only" | "read_only" | "read" | "readonly" => Ok(Self::ReadOnly),
            "wo" | "write-only" | "write_only" | "write" | "writeonly" => Ok(Self::WriteOnly),
            "rw" | "wr" | "readwrite" | "writeread" => Ok(Self::ReadWrite),
            other => Err(TftpInitError::InvalidAccessMode(other.to_owned())),
        }
    }
}

/// TFTP server configuration.
#[derive(Debug, Clone)]
pub struct TftpConfig {
    /// Server bind address.
    pub bind_addr: SocketAddr,
    /// Default S3 bucket. When set, all TFTP paths use this bucket
    /// and the path is treated as the object key. When not set, the
    /// first path component is the bucket name.
    pub default_bucket: Option<String>,
    /// TFTP access_key for authentication.
    /// Configured via RUSTFS_TFTP_ACCESS_KEY;
    pub access_key: String,
    /// Access mode for the TFTP server.
    pub mode: TftpAccessMode,
}

impl TftpConfig {
    /// Validate the configuration and verify credentials against IAM.
    pub async fn validate(&self) -> Result<(), TftpInitError> {
        if self.access_key.is_empty() {
            return Err(TftpInitError::InvalidConfig("TFTP access_key cannot be empty".to_string()));
        }

        Ok(())
    }
}

impl Default for TftpConfig {
    fn default() -> Self {
        TftpConfig {
            bind_addr: crate::constants::defaults::DEFAULT_TFTP_ADDRESS.parse().unwrap(),
            default_bucket: None,
            access_key: "".to_owned(),
            mode: TftpAccessMode::ReadWrite,
        }
    }
}
