use crate::{disk, error::Error, store_err::is_err_object_not_found};

#[derive(Debug, PartialEq, thiserror::Error)]
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

impl ConfigError {
    pub fn to_u32(&self) -> u32 {
        match self {
            ConfigError::NotFound => 0x01,
        }
    }

    pub fn from_u32(error: u32) -> Option<Self> {
        match error {
            0x01 => Some(Self::NotFound),
            _ => None,
        }
    }
}

pub fn is_not_found(err: &Error) -> bool {
    if let Some(e) = err.downcast_ref::<ConfigError>() {
        ConfigError::is_not_found(e)
    } else if let Some(e) = err.downcast_ref::<disk::error::DiskError>() {
        matches!(e, disk::error::DiskError::FileNotFound)
    } else if is_err_object_not_found(err) {
        return true;
    } else {
        false
    }
}
