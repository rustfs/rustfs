use crate::config::adapter::AdapterCommon;
use crate::config::{default_queue_dir, default_queue_limit};
use serde::{Deserialize, Serialize};

/// Configuration for the Kafka adapter.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaConfig {
    #[serde(flatten)]
    pub common: AdapterCommon,
    pub brokers: String,
    pub topic: String,
    pub max_retries: u32,
    pub timeout: u64,
}

impl Default for KafkaConfig {
    fn default() -> Self {
        Self {
            common: AdapterCommon::default(),
            brokers: String::new(),
            topic: String::new(),
            max_retries: 3,
            timeout: 5000,
        }
    }
}

impl KafkaConfig {
    /// Create a new Kafka configuration
    pub fn new(identifier: impl Into<String>, brokers: impl Into<String>, topic: impl Into<String>) -> Self {
        Self {
            common: AdapterCommon {
                identifier: identifier.into(),
                comment: String::new(),
                enable: true,
                queue_dir: default_queue_dir(),
                queue_limit: default_queue_limit(),
            },
            brokers: brokers.into(),
            topic: topic.into(),
            ..Default::default()
        }
    }
}
