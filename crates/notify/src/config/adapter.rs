use crate::config::kafka::KafkaConfig;
use crate::config::mqtt::MqttConfig;
use crate::config::webhook::WebhookConfig;
use crate::config::{default_queue_dir, default_queue_limit};
use serde::{Deserialize, Serialize};

/// Add a common field for the adapter configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdapterCommon {
    /// Adapter identifier for unique identification
    pub identifier: String,
    /// Adapter description information
    pub comment: String,
    /// Whether to enable this adapter
    #[serde(default)]
    pub enable: bool,
    #[serde(default = "default_queue_dir")]
    pub queue_dir: String,
    #[serde(default = "default_queue_limit")]
    pub queue_limit: u64,
}

impl Default for AdapterCommon {
    fn default() -> Self {
        Self {
            identifier: String::new(),
            comment: String::new(),
            enable: false,
            queue_dir: default_queue_dir(),
            queue_limit: default_queue_limit(),
        }
    }
}

/// Configuration for the adapter.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum AdapterConfig {
    Webhook(WebhookConfig),
    Kafka(KafkaConfig),
    Mqtt(MqttConfig),
}
