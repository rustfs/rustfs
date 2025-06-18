use thiserror::Error;
use tokio::sync::mpsc::error;
use tokio::task::JoinError;

/// The `Error` enum represents all possible errors that can occur in the application.
/// It implements the `std::error::Error` trait and provides a way to convert various error types into a single error type.
#[derive(Error, Debug)]
pub enum Error {
    #[error("Join error: {0}")]
    JoinError(#[from] JoinError),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Serialization error: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("Channel send error: {0}")]
    ChannelSend(#[from] Box<error::SendError<crate::event::Event>>),
    #[error("Feature disabled: {0}")]
    FeatureDisabled(&'static str),
    #[error("Event bus already started")]
    EventBusStarted,
    #[error("necessary fields are missing:{0}")]
    MissingField(&'static str),
    #[error("field verification failed:{0}")]
    ValidationError(&'static str),
    #[error("Custom error: {0}")]
    Custom(String),
    #[error("Configuration error: {0}")]
    ConfigError(String),
    #[error("create adapter failed error: {0}")]
    AdapterCreationFailed(String),
}

impl Error {
    pub fn custom(msg: &str) -> Error {
        Self::Custom(msg.to_string())
    }
}
