use crate::observability::logger::LoggerConfig;
use crate::observability::otel::OtelConfig;
use crate::observability::sink::SinkConfig;
use serde::{Deserialize, Serialize};

/// Observability configuration
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ObservabilityConfig {
    pub otel: OtelConfig,
    pub sinks: Vec<SinkConfig>,
    pub logger: Option<LoggerConfig>,
}

impl ObservabilityConfig {
    pub fn new() -> Self {
        Self {
            otel: OtelConfig::new(),
            sinks: vec![SinkConfig::new()],
            logger: Some(LoggerConfig::new()),
        }
    }
}

impl Default for ObservabilityConfig {
    fn default() -> Self {
        Self::new()
    }
}
