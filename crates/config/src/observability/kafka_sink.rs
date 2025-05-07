use serde::Deserialize;

/// Kafka sink configuration
#[derive(Debug, Deserialize, Clone)]
pub struct KafkaSinkConfig {
    pub brokers: Vec<String>,
    pub topic: String,
}

impl KafkaSinkConfig {
    pub fn new() -> Self {
        Self {
            brokers: vec!["localhost:9092".to_string()],
            topic: "rustfs".to_string(),
        }
    }
}

impl Default for KafkaSinkConfig {
    fn default() -> Self {
        Self::new()
    }
}
