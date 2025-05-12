use serde::{Deserialize, Serialize};

/// Configuration for the MQTT adapter.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MqttAdapter {
    pub broker: String,
    pub port: u16,
    pub client_id: String,
    pub topic: String,
    pub max_retries: u32,
}

impl MqttAdapter {
    /// create a new configuration with default values
    pub fn new() -> Self {
        Self {
            broker: "localhost".to_string(),
            port: 1883,
            client_id: "mqtt_client".to_string(),
            topic: "mqtt_topic".to_string(),
            max_retries: 3,
        }
    }
}

impl Default for MqttAdapter {
    /// create a new configuration with default values
    fn default() -> Self {
        Self::new()
    }
}
