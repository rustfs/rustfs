use crate::event::kafka::KafkaAdapter;
use crate::event::mqtt::MqttAdapter;
use crate::event::webhook::WebhookAdapter;
use serde::{Deserialize, Serialize};

/// Configuration for the notification system.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum AdapterConfig {
    Webhook(WebhookAdapter),
    Kafka(KafkaAdapter),
    Mqtt(MqttAdapter),
}

impl AdapterConfig {
    /// create a new configuration with default values
    pub fn new() -> Self {
        Self::Webhook(WebhookAdapter::new())
    }
}

impl Default for AdapterConfig {
    /// create a new configuration with default values
    fn default() -> Self {
        Self::new()
    }
}
