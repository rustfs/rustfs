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

//! TFTP server configuration.

use super::constants::{
    BACKEND_OP_TIMEOUT_MAX_SECS, BACKEND_OP_TIMEOUT_MIN_SECS, MAX_BLOCK_SIZE_MAX, MAX_BLOCK_SIZE_MIN,
    MAX_CONCURRENT_TRANSFERS_MAX, MAX_CONCURRENT_TRANSFERS_MIN, MAX_SEND_RETRIES_MAX, MAX_SEND_RETRIES_MIN,
    MAX_TRANSFER_BYTES_MAX, MAX_TRANSFER_BYTES_MIN, MAX_WINDOW_SIZE_MAX, MAX_WINDOW_SIZE_MIN, READ_FETCH_BYTES_MAX,
    READ_FETCH_BYTES_MIN,
};
use rustfs_config::{
    DEFAULT_TFTP_ACCESS_MODE, DEFAULT_TFTP_BACKEND_OP_TIMEOUT_SECS, DEFAULT_TFTP_MAX_BLOCK_SIZE,
    DEFAULT_TFTP_MAX_CONCURRENT_TRANSFERS, DEFAULT_TFTP_MAX_SEND_RETRIES, DEFAULT_TFTP_MAX_TRANSFER_BYTES,
    DEFAULT_TFTP_MAX_WINDOW_SIZE, DEFAULT_TFTP_READ_FETCH_BYTES,
};
use std::net::SocketAddr;
use thiserror::Error;

/// TFTP transfer direction policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TftpAccessMode {
    ReadOnly,
    WriteOnly,
    ReadWrite,
}

impl TftpAccessMode {
    pub fn parse(raw: &str) -> Result<Self, TftpInitError> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "ro" | "read-only" | "readonly" => Ok(Self::ReadOnly),
            "wo" | "write-only" | "writeonly" => Ok(Self::WriteOnly),
            "rw" | "readwrite" | "read-write" => Ok(Self::ReadWrite),
            other => Err(TftpInitError::InvalidConfig(format!(
                "invalid RUSTFS_TFTP_ACCESS_MODE '{other}': expected ro, wo, or rw"
            ))),
        }
    }

    pub fn allows_read(self) -> bool {
        matches!(self, Self::ReadOnly | Self::ReadWrite)
    }

    pub fn allows_write(self) -> bool {
        matches!(self, Self::WriteOnly | Self::ReadWrite)
    }
}

/// Runtime configuration for the TFTP server.
#[derive(Debug, Clone)]
pub struct TftpConfig {
    pub bind_addr: SocketAddr,
    pub default_bucket: Option<String>,
    pub access_mode: TftpAccessMode,
    pub max_block_size: u16,
    pub max_window_size: u16,
    pub max_concurrent_transfers: usize,
    pub max_transfer_bytes: u64,
    pub backend_op_timeout_secs: u64,
    pub read_fetch_bytes: u64,
    pub max_send_retries: u32,
}

/// Errors during TFTP initialization.
#[derive(Debug, Error)]
pub enum TftpInitError {
    #[error("invalid TFTP configuration: {0}")]
    InvalidConfig(String),

    #[error("TFTP credentials rejected: {0}")]
    CredentialsRejected(String),
}

impl TftpConfig {
    pub fn resolve_max_block_size(raw: Option<u16>) -> u16 {
        raw.filter(|&v| (MAX_BLOCK_SIZE_MIN..=MAX_BLOCK_SIZE_MAX).contains(&v))
            .unwrap_or(DEFAULT_TFTP_MAX_BLOCK_SIZE)
    }

    pub fn resolve_max_window_size(raw: Option<u16>) -> u16 {
        raw.filter(|&v| (MAX_WINDOW_SIZE_MIN..=MAX_WINDOW_SIZE_MAX).contains(&v))
            .unwrap_or(DEFAULT_TFTP_MAX_WINDOW_SIZE)
    }

    pub fn resolve_max_concurrent_transfers(raw: Option<usize>) -> usize {
        raw.filter(|&v| (MAX_CONCURRENT_TRANSFERS_MIN..=MAX_CONCURRENT_TRANSFERS_MAX).contains(&v))
            .unwrap_or(DEFAULT_TFTP_MAX_CONCURRENT_TRANSFERS)
    }

    pub fn resolve_max_transfer_bytes(raw: Option<u64>) -> u64 {
        raw.filter(|&v| (MAX_TRANSFER_BYTES_MIN..=MAX_TRANSFER_BYTES_MAX).contains(&v))
            .unwrap_or(DEFAULT_TFTP_MAX_TRANSFER_BYTES)
    }

    pub fn resolve_backend_op_timeout_secs(raw: Option<u64>) -> u64 {
        raw.filter(|&v| (BACKEND_OP_TIMEOUT_MIN_SECS..=BACKEND_OP_TIMEOUT_MAX_SECS).contains(&v))
            .unwrap_or(DEFAULT_TFTP_BACKEND_OP_TIMEOUT_SECS)
    }

    pub fn resolve_read_fetch_bytes(raw: Option<u64>) -> u64 {
        raw.filter(|&v| (READ_FETCH_BYTES_MIN..=READ_FETCH_BYTES_MAX).contains(&v))
            .unwrap_or(DEFAULT_TFTP_READ_FETCH_BYTES)
    }

    pub fn resolve_max_send_retries(raw: Option<u32>) -> u32 {
        raw.filter(|&v| (MAX_SEND_RETRIES_MIN..=MAX_SEND_RETRIES_MAX).contains(&v))
            .unwrap_or(DEFAULT_TFTP_MAX_SEND_RETRIES)
    }

    pub fn resolve_access_mode(raw: Option<&str>) -> Result<TftpAccessMode, TftpInitError> {
        TftpAccessMode::parse(raw.unwrap_or(DEFAULT_TFTP_ACCESS_MODE))
    }
}
