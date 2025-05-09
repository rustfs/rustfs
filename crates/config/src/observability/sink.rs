use crate::observability::file_sink::FileSinkConfig;
use crate::observability::kafka_sink::KafkaSinkConfig;
use crate::observability::webhook_sink::WebhookSinkConfig;
use serde::Deserialize;

/// Sink configuration
#[derive(Debug, Deserialize, Clone)]
pub struct SinkConfig {
    pub kafka: Option<KafkaSinkConfig>,
    pub webhook: Option<WebhookSinkConfig>,
    pub file: Option<FileSinkConfig>,
}

impl SinkConfig {
    pub fn new() -> Self {
        Self {
            kafka: None,
            webhook: None,
            file: Some(FileSinkConfig::new()),
        }
    }
}

impl Default for SinkConfig {
    fn default() -> Self {
        Self::new()
    }
}
