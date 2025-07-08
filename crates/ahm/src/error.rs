use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Storage error: {0}")]
    Storage(#[from] rustfs_ecstore::error::Error),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Task scheduling error: {0}")]
    Scheduling(String),

    #[error("Metrics collection error: {0}")]
    Metrics(String),

    #[error("API error: {0}")]
    Api(String),

    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

pub type Result<T, E = Error> = std::result::Result<T, E>; 