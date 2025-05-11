use serde::{Deserialize, Serialize};

/// Kafka sink configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaSink {
    pub brokers: String,
    pub topic: String,
    #[serde(default = "default_batch_size")]
    pub batch_size: Option<usize>,
    #[serde(default = "default_batch_timeout_ms")]
    pub batch_timeout_ms: Option<u64>,
}

impl KafkaSink {
    pub fn new() -> Self {
        Self {
            brokers: "localhost:9092".to_string(),
            topic: "rustfs".to_string(),
            batch_size: default_batch_size(),
            batch_timeout_ms: default_batch_timeout_ms(),
        }
    }
}

impl Default for KafkaSink {
    fn default() -> Self {
        Self::new()
    }
}

fn default_batch_size() -> Option<usize> {
    Some(100)
}
fn default_batch_timeout_ms() -> Option<u64> {
    Some(1000)
}
