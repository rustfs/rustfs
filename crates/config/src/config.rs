use crate::event::config::EventConfig;
use crate::ObservabilityConfig;

/// RustFs configuration
pub struct RustFsConfig {
    pub observability: ObservabilityConfig,
    pub event: EventConfig,
}

impl RustFsConfig {
    pub fn new() -> Self {
        Self {
            observability: ObservabilityConfig::new(),
            event: EventConfig::new(),
        }
    }
}

impl Default for RustFsConfig {
    fn default() -> Self {
        Self::new()
    }
}
