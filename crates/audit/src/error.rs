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

//! Error types for the audit system

use thiserror::Error;

/// Main error type for audit operations
#[derive(Debug, Error)]
pub enum AuditError {
    #[error("Target error: {0}")]
    Target(#[from] TargetError),
    
    #[error("Target not found: {id}")]
    TargetNotFound { id: String },
    
    #[error("System not initialized")]
    SystemNotInitialized,
    
    #[error("Configuration error: {message}")]
    Configuration { message: String },
    
    #[error("Persistence error: {message}")]
    Persistence { message: String },
    
    #[error("Serialization error: {source}")]
    Serialization {
        #[from]
        source: serde_json::Error,
    },
    
    #[error("Channel error: {message}")]
    Channel { message: String },
    
    #[error("Shutdown error: {message}")]
    Shutdown { message: String },
    
    #[error("Timeout error: operation timed out after {timeout_ms}ms")]
    Timeout { timeout_ms: u64 },
    
    #[error("Rate limit exceeded: {message}")]
    RateLimit { message: String },
    
    #[error("Validation error: {field} - {message}")]
    Validation { field: String, message: String },
}

/// Target-specific error types
#[derive(Debug, Error)]
pub enum TargetError {
    #[error("Unsupported target type: {target_type}")]
    UnsupportedType { target_type: String },
    
    #[error("Invalid configuration: {message}")]
    InvalidConfig { message: String },
    
    #[error("Target initialization failed: {message}")]
    InitializationFailed { message: String },
    
    #[error("Target operation failed: {message}")]
    OperationFailed { message: String },
    
    #[error("Network error: {message}")]
    Network { message: String },
    
    #[error("Authentication error: {message}")]
    Authentication { message: String },
    
    #[error("Connection error: {message}")]
    Connection { message: String },
    
    #[error("Protocol error: {message}")]
    Protocol { message: String },
    
    #[error("Serialization error: {message}")]
    Serialization { message: String },
    
    #[error("Target is disabled")]
    Disabled,
    
    #[error("Target is shutting down")]
    ShuttingDown,
    
    #[error("Queue full: {current_size}/{max_size}")]
    QueueFull { current_size: usize, max_size: usize },
    
    #[error("Retry limit exceeded: {attempts}/{max_attempts}")]
    RetryLimitExceeded { attempts: u32, max_attempts: u32 },
}

/// Result types for convenience
pub type AuditResult<T> = Result<T, AuditError>;
pub type TargetResult<T> = Result<T, TargetError>;

impl AuditError {
    /// Create a configuration error
    pub fn config<S: Into<String>>(message: S) -> Self {
        AuditError::Configuration {
            message: message.into(),
        }
    }
    
    /// Create a persistence error
    pub fn persistence<S: Into<String>>(message: S) -> Self {
        AuditError::Persistence {
            message: message.into(),
        }
    }
    
    /// Create a channel error
    pub fn channel<S: Into<String>>(message: S) -> Self {
        AuditError::Channel {
            message: message.into(),
        }
    }
    
    /// Create a validation error
    pub fn validation<S: Into<String>>(field: S, message: S) -> Self {
        AuditError::Validation {
            field: field.into(),
            message: message.into(),
        }
    }
    
    /// Create a timeout error
    pub fn timeout(timeout_ms: u64) -> Self {
        AuditError::Timeout { timeout_ms }
    }
    
    /// Check if error is recoverable (can retry)
    pub fn is_recoverable(&self) -> bool {
        match self {
            AuditError::Target(TargetError::Network { .. }) => true,
            AuditError::Target(TargetError::Connection { .. }) => true,
            AuditError::Target(TargetError::QueueFull { .. }) => true,
            AuditError::Timeout { .. } => true,
            AuditError::Channel { .. } => true,
            _ => false,
        }
    }
    
    /// Check if error is due to configuration issues
    pub fn is_config_error(&self) -> bool {
        matches!(
            self,
            AuditError::Configuration { .. }
                | AuditError::Target(TargetError::InvalidConfig { .. })
                | AuditError::Target(TargetError::UnsupportedType { .. })
                | AuditError::Validation { .. }
        )
    }
}

impl TargetError {
    /// Create an invalid config error
    pub fn invalid_config<S: Into<String>>(message: S) -> Self {
        TargetError::InvalidConfig {
            message: message.into(),
        }
    }
    
    /// Create a network error
    pub fn network<S: Into<String>>(message: S) -> Self {
        TargetError::Network {
            message: message.into(),
        }
    }
    
    /// Create an operation failed error
    pub fn operation_failed<S: Into<String>>(message: S) -> Self {
        TargetError::OperationFailed {
            message: message.into(),
        }
    }
    
    /// Create an initialization failed error
    pub fn initialization_failed<S: Into<String>>(message: S) -> Self {
        TargetError::InitializationFailed {
            message: message.into(),
        }
    }
    
    /// Check if the error indicates the target should be disabled
    pub fn should_disable_target(&self) -> bool {
        match self {
            TargetError::InvalidConfig { .. } => true,
            TargetError::UnsupportedType { .. } => true,
            TargetError::Authentication { .. } => true,
            TargetError::InitializationFailed { .. } => true,
            _ => false,
        }
    }
    
    /// Check if error is temporary and retryable
    pub fn is_retryable(&self) -> bool {
        match self {
            TargetError::Network { .. } => true,
            TargetError::Connection { .. } => true,
            TargetError::QueueFull { .. } => true,
            TargetError::OperationFailed { .. } => true,
            _ => false,
        }
    }
}

/// Convert common error types
impl From<tokio::sync::mpsc::error::SendError<crate::entity::AuditEntry>> for AuditError {
    fn from(err: tokio::sync::mpsc::error::SendError<crate::entity::AuditEntry>) -> Self {
        AuditError::channel(format!("Failed to send audit entry: {}", err))
    }
}

impl From<tokio::sync::oneshot::error::RecvError> for AuditError {
    fn from(err: tokio::sync::oneshot::error::RecvError) -> Self {
        AuditError::channel(format!("Failed to receive response: {}", err))
    }
}

impl From<url::ParseError> for TargetError {
    fn from(err: url::ParseError) -> Self {
        TargetError::invalid_config(format!("Invalid URL: {}", err))
    }
}

impl From<reqwest::Error> for TargetError {
    fn from(err: reqwest::Error) -> Self {
        if err.is_timeout() {
            TargetError::Network {
                message: "Request timeout".to_string(),
            }
        } else if err.is_connect() {
            TargetError::Connection {
                message: format!("Connection failed: {}", err),
            }
        } else {
            TargetError::Network {
                message: err.to_string(),
            }
        }
    }
}

impl From<rumqttc::ClientError> for TargetError {
    fn from(err: rumqttc::ClientError) -> Self {
        match err {
            rumqttc::ClientError::Io(io_err) => TargetError::Connection {
                message: format!("MQTT I/O error: {}", io_err),
            },
            rumqttc::ClientError::Tls(tls_err) => TargetError::Connection {
                message: format!("MQTT TLS error: {}", tls_err),
            },
            _ => TargetError::Protocol {
                message: format!("MQTT protocol error: {}", err),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_error_creation() {
        let config_err = AuditError::config("Invalid target ID");
        assert!(config_err.is_config_error());
        assert!(!config_err.is_recoverable());
        
        let network_err = AuditError::Target(TargetError::network("Connection refused"));
        assert!(network_err.is_recoverable());
        assert!(!network_err.is_config_error());
    }
    
    #[test]
    fn test_target_error_classification() {
        let invalid_config = TargetError::invalid_config("Missing endpoint");
        assert!(invalid_config.should_disable_target());
        assert!(!invalid_config.is_retryable());
        
        let network_error = TargetError::network("Timeout");
        assert!(!network_error.should_disable_target());
        assert!(network_error.is_retryable());
    }
    
    #[test]
    fn test_error_display() {
        let err = AuditError::TargetNotFound {
            id: "webhook-1".to_string(),
        };
        assert_eq!(err.to_string(), "Target not found: webhook-1");
        
        let target_err = TargetError::QueueFull {
            current_size: 1000,
            max_size: 1000,
        };
        assert_eq!(target_err.to_string(), "Queue full: 1000/1000");
    }
}