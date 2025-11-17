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

use thiserror::Error;

/// Custom error type for AHM operations
/// This enum defines various error variants that can occur during
/// the execution of AHM-related tasks, such as I/O errors, storage errors,
/// configuration errors, and specific errors related to healing operations.
#[derive(Debug, Error)]
pub enum Error {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Storage error: {0}")]
    Storage(#[from] rustfs_ecstore::error::Error),

    #[error("Disk error: {0}")]
    Disk(#[from] rustfs_ecstore::disk::error::DiskError),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Heal configuration error: {message}")]
    ConfigurationError { message: String },

    #[error("Other error: {0}")]
    Other(String),

    #[error(transparent)]
    Anyhow(#[from] anyhow::Error),

    // Scanner
    #[error("Scanner error: {0}")]
    Scanner(String),

    #[error("Metrics error: {0}")]
    Metrics(String),

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("IO error: {0}")]
    IO(String),

    #[error("Not found: {0}")]
    NotFound(String),

    #[error("Invalid checkpoint: {0}")]
    InvalidCheckpoint(String),

    // Heal
    #[error("Heal task not found: {task_id}")]
    TaskNotFound { task_id: String },

    #[error("Heal task already exists: {task_id}")]
    TaskAlreadyExists { task_id: String },

    #[error("Heal manager is not running")]
    ManagerNotRunning,

    #[error("Heal task execution failed: {message}")]
    TaskExecutionFailed { message: String },

    #[error("Invalid heal type: {heal_type}")]
    InvalidHealType { heal_type: String },

    #[error("Heal task cancelled")]
    TaskCancelled,

    #[error("Heal task timeout")]
    TaskTimeout,

    #[error("Heal event processing failed: {message}")]
    EventProcessingFailed { message: String },

    #[error("Heal progress tracking failed: {message}")]
    ProgressTrackingFailed { message: String },
}

/// A specialized Result type for AHM operations
///This type is a convenient alias for results returned by functions in the AHM crate,
/// using the custom Error type defined above.
pub type Result<T, E = Error> = std::result::Result<T, E>;

impl Error {
    /// Create an Other error from any error type
    pub fn other<E>(error: E) -> Self
    where
        E: Into<Box<dyn std::error::Error + Send + Sync>>,
    {
        Error::Other(error.into().to_string())
    }
}

impl From<Error> for std::io::Error {
    fn from(err: Error) -> Self {
        std::io::Error::other(err)
    }
}
