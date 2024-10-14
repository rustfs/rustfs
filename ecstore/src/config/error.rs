use crate::{disk, error::Error};

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("config not found")]
    NotFound,
}

impl ConfigError {
    /// Returns `true` if the config error is [`NotFound`].
    ///
    /// [`NotFound`]: ConfigError::NotFound
    #[must_use]
    pub fn is_not_found(&self) -> bool {
        matches!(self, Self::NotFound)
    }
}
pub fn is_not_found(err: &Error) -> bool {
    if let Some(e) = err.downcast_ref::<ConfigError>() {
        ConfigError::is_not_found(e)
    } else if let Some(e) = err.downcast_ref::<disk::error::DiskError>() {
        matches!(e, disk::error::DiskError::FileNotFound)
    } else {
        false
    }
}
