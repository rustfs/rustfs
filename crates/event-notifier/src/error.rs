// src/error.rs
use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("target not found: {0}")]
    TargetNotFound(String),

    #[error("send failed: {0}")]
    SendError(String),

    #[error("target error: {0}")]
    TargetError(String),

    #[error("invalid configuration: {0}")]
    ConfigError(String),

    #[error("store error: {0}")]
    StoreError(String),

    #[error("invalid event name: {0}")]
    InvalidEventName(String), // 添加此变体
}

pub type Result<T> = std::result::Result<T, Error>;
