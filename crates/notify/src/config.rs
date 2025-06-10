use rustfs_config::notify::mqtt::MQTTArgs;
use rustfs_config::notify::webhook::WebhookArgs;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use std::env;

/// The default configuration file name
const DEFAULT_CONFIG_FILE: &str = "notify";

/// The prefix for the configuration file
pub const STORE_PREFIX: &str = "rustfs";

/// The default retry interval for the webhook adapter
pub const DEFAULT_RETRY_INTERVAL: u64 = 3;

/// The default maximum retry count for the webhook adapter
pub const DEFAULT_MAX_RETRIES: u32 = 3;

/// The default notification queue limit
pub const DEFAULT_NOTIFY_QUEUE_LIMIT: u64 = 10000;

/// Provide temporary directories as default storage paths
pub(crate) fn default_queue_dir() -> String {
    env::var("EVENT_QUEUE_DIR").unwrap_or_else(|e| {
        tracing::info!("Failed to get `EVENT_QUEUE_DIR` failed err: {}", e.to_string());
        env::temp_dir().join(DEFAULT_CONFIG_FILE).to_string_lossy().to_string()
    })
}

/// Provides the recommended default channel capacity for high concurrency systems
pub(crate) fn default_queue_limit() -> u64 {
    env::var("EVENT_CHANNEL_CAPACITY")
        .unwrap_or_else(|_| DEFAULT_NOTIFY_QUEUE_LIMIT.to_string())
        .parse()
        .unwrap_or(DEFAULT_NOTIFY_QUEUE_LIMIT) // Default to 10000 if parsing fails
}

/// Configuration for the adapter.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum AdapterConfig {
    Webhook(WebhookArgs),
    Mqtt(MQTTArgs),
}

/// Event Notifier Configuration
/// This struct contains the configuration for the event notifier system,
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct EventNotifierConfig {
    /// A collection of webhook configurations, with the key being a unique identifier
    #[serde(default)]
    pub webhook: HashMap<String, WebhookArgs>,
    ///MQTT configuration collection, with the key being a unique identifier
    #[serde(default)]
    pub mqtt: HashMap<String, MQTTArgs>,
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
            if webhook.enable {
                adapters.push(AdapterConfig::Webhook(webhook.clone()));
            }
        }

        // Add all enabled MQTT configurations
        for mqtt in self.mqtt.values() {
            if mqtt.enable {
                adapters.push(AdapterConfig::Mqtt(mqtt.clone()));
            }
        }

        adapters
    }
}
