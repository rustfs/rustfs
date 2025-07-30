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

use crate::arn::TargetID;
use std::io;
use thiserror::Error;

/// Error types for the store
#[derive(Debug, Error)]
pub enum StoreError {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("Deserialization error: {0}")]
    Deserialization(String),

    #[error("Compression error: {0}")]
    Compression(String),

    #[error("Entry limit exceeded")]
    LimitExceeded,

    #[error("Entry not found")]
    NotFound,

    #[error("Invalid entry: {0}")]
    Internal(String), // Added internal error type
}

/// Error types for targets
#[derive(Debug, Error)]
pub enum TargetError {
    #[error("Storage error: {0}")]
    Storage(String),

    #[error("Network error: {0}")]
    Network(String),

    #[error("Request error: {0}")]
    Request(String),

    #[error("Timeout error: {0}")]
    Timeout(String),

    #[error("Authentication error: {0}")]
    Authentication(String),

    #[error("Configuration error: {0}")]
    Configuration(String),

    #[error("Encoding error: {0}")]
    Encoding(String),

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("Target not connected")]
    NotConnected,

    #[error("Target initialization failed: {0}")]
    Initialization(String),

    #[error("Invalid ARN: {0}")]
    InvalidARN(String),

    #[error("Unknown error: {0}")]
    Unknown(String),

    #[error("Target is disabled")]
    Disabled,

    #[error("Configuration parsing error: {0}")]
    ParseError(String),

    #[error("Failed to save configuration: {0}")]
    SaveConfig(String),

    #[error("Server not initialized: {0}")]
    ServerNotInitialized(String),
}

/// Error types for the notification system
#[derive(Debug, Error)]
pub enum NotificationError {
    #[error("Target error: {0}")]
    Target(#[from] TargetError),

    #[error("Configuration error: {0}")]
    Configuration(String),

    #[error("ARN not found: {0}")]
    ARNNotFound(String),

    #[error("Invalid ARN: {0}")]
    InvalidARN(String),

    #[error("Bucket notification error: {0}")]
    BucketNotification(String),

    #[error("Rule configuration error: {0}")]
    RuleConfiguration(String),

    #[error("System initialization error: {0}")]
    Initialization(String),

    #[error("Notification system has already been initialized")]
    AlreadyInitialized,

    #[error("I/O error: {0}")]
    Io(io::Error),

    #[error("Failed to read configuration: {0}")]
    ReadConfig(String),

    #[error("Failed to save configuration: {0}")]
    SaveConfig(String),

    #[error("Target '{0}' not found")]
    TargetNotFound(TargetID),

    #[error("Server not initialized")]
    ServerNotInitialized,
}

impl From<url::ParseError> for TargetError {
    fn from(err: url::ParseError) -> Self {
        TargetError::Configuration(format!("URL parse error: {err}"))
    }
}
