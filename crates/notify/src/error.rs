//  Copyright 2024 RustFS Team
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use rustfs_targets::TargetError;
use rustfs_targets::arn::TargetID;
use std::io;
use thiserror::Error;

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
