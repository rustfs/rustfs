use crate::observability::logger::LoggerConfig;
use crate::observability::otel::OtelConfig;
use crate::observability::sink::SinkConfig;
use serde::Deserialize;

/// Observability configuration
#[derive(Debug, Deserialize, Clone)]
pub struct ObservabilityConfig {
    pub otel: OtelConfig,
    pub sinks: SinkConfig,
    pub logger: Option<LoggerConfig>,
}

impl ObservabilityConfig {
    pub fn new() -> Self {
        Self {
            otel: OtelConfig::new(),
            sinks: SinkConfig::new(),
            logger: Some(LoggerConfig::new()),
        }
    }
}

impl Default for ObservabilityConfig {
    fn default() -> Self {
        Self::new()
    }
}
