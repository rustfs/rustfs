use serde::{Deserialize, Serialize};

/// Configuration for the Kafka adapter.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaAdapter {
    pub brokers: String,
    pub topic: String,
    pub max_retries: u32,
    pub timeout: u64,
}

impl KafkaAdapter {
    /// create a new configuration with default values
    pub fn new() -> Self {
        Self {
            brokers: "localhost:9092".to_string(),
            topic: "kafka_topic".to_string(),
            max_retries: 3,
            timeout: 1000,
        }
    }
}

impl Default for KafkaAdapter {
    /// create a new configuration with default values
    fn default() -> Self {
        Self::new()
    }
}
