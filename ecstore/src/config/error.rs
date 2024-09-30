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
