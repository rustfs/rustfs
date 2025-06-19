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
    Internal(String), // 新增内部错误类型
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
}

impl From<url::ParseError> for TargetError {
    fn from(err: url::ParseError) -> Self {
        TargetError::Configuration(format!("URL parse error: {}", err))
    }
}
