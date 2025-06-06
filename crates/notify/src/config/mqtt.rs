use crate::config::adapter::AdapterCommon;
use crate::config::{default_queue_dir, default_queue_limit};
use serde::{Deserialize, Serialize};

/// Configuration for the MQTT adapter.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MqttConfig {
    #[serde(flatten)]
    pub common: AdapterCommon,
    pub broker: String,
    pub port: u16,
    pub client_id: String,
    pub topic: String,
    pub max_retries: u32,
}

impl Default for MqttConfig {
    fn default() -> Self {
        Self {
            common: AdapterCommon::default(),
            broker: String::new(),
            port: 1883,
            client_id: String::new(),
            topic: String::new(),
            max_retries: 3,
        }
    }
}

impl MqttConfig {
    /// Create a new MQTT configuration
    pub fn new(identifier: impl Into<String>, broker: impl Into<String>, topic: impl Into<String>) -> Self {
        Self {
            common: AdapterCommon {
                identifier: identifier.into(),
                comment: String::new(),
                enable: true,
                queue_dir: default_queue_dir(),
                queue_limit: default_queue_limit(),
            },
            broker: broker.into(),
            topic: topic.into(),
            ..Default::default()
        }
    }
}
