use crate::event::config::NotifierConfig;
use crate::ObservabilityConfig;

/// RustFs configuration
pub struct RustFsConfig {
    pub observability: ObservabilityConfig,
    pub event: NotifierConfig,
}

impl RustFsConfig {
    pub fn new() -> Self {
        Self {
            observability: ObservabilityConfig::new(),
            event: NotifierConfig::new(),
        }
    }
}

impl Default for RustFsConfig {
    fn default() -> Self {
        Self::new()
    }
}
