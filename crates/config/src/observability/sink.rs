use crate::observability::file::FileSink;
use crate::observability::kafka::KafkaSink;
use crate::observability::webhook::WebhookSink;
use serde::{Deserialize, Serialize};

/// Sink configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum SinkConfig {
    Kafka(KafkaSink),
    Webhook(WebhookSink),
    File(FileSink),
}

impl SinkConfig {
    pub fn new() -> Self {
        Self::File(FileSink::new())
    }
}

impl Default for SinkConfig {
    fn default() -> Self {
        Self::new()
    }
}
