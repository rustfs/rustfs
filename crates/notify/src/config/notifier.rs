use crate::config::{adapter::AdapterConfig, kafka::KafkaConfig, mqtt::MqttConfig, webhook::WebhookConfig};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Event Notifier Configuration
/// This struct contains the configuration for the event notifier system,
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct EventNotifierConfig {
    /// A collection of webhook configurations, with the key being a unique identifier
    #[serde(default)]
    pub webhook: HashMap<String, WebhookConfig>,
    /// A collection of Kafka configurations, with the key being a unique identifier
    #[serde(default)]
    pub kafka: HashMap<String, KafkaConfig>,
    ///MQTT configuration collection, with the key being a unique identifier
    #[serde(default)]
    pub mqtt: HashMap<String, MqttConfig>,
}

impl EventNotifierConfig {
    /// Create a new default configuration
    pub fn new() -> Self {
        Self::default()
    }

    /// Load the configuration from the file
    pub fn event_load_config(_config_dir: Option<String>) -> EventNotifierConfig {
        // The existing implementation remains the same, but returns EventNotifierConfig
        // ...

        Self::default()
    }

    /// Deserialization configuration
    pub fn unmarshal(data: &[u8]) -> common::error::Result<EventNotifierConfig> {
        let m: EventNotifierConfig = serde_json::from_slice(data)?;
        Ok(m)
    }

    /// Serialization configuration
    pub fn marshal(&self) -> common::error::Result<Vec<u8>> {
        let data = serde_json::to_vec(&self)?;
        Ok(data)
    }

    /// Convert this configuration to a list of adapter configurations
    pub fn to_adapter_configs(&self) -> Vec<AdapterConfig> {
        let mut adapters = Vec::new();

        // Add all enabled webhook configurations
        for webhook in self.webhook.values() {
            if webhook.common.enable {
                adapters.push(AdapterConfig::Webhook(webhook.clone()));
            }
        }

        // Add all enabled Kafka configurations
        for kafka in self.kafka.values() {
            if kafka.common.enable {
                adapters.push(AdapterConfig::Kafka(kafka.clone()));
            }
        }

        // Add all enabled MQTT configurations
        for mqtt in self.mqtt.values() {
            if mqtt.common.enable {
                adapters.push(AdapterConfig::Mqtt(mqtt.clone()));
            }
        }

        adapters
    }
}
