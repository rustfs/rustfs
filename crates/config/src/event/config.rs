use crate::event::adapters::AdapterConfig;
use serde::{Deserialize, Serialize};
use std::env;

#[allow(dead_code)]
const DEFAULT_CONFIG_FILE: &str = "event";

/// Configuration for the notification system.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NotifierConfig {
    #[serde(default = "default_store_path")]
    pub store_path: String,
    #[serde(default = "default_channel_capacity")]
    pub channel_capacity: usize,
    pub adapters: Vec<AdapterConfig>,
}

impl Default for NotifierConfig {
    fn default() -> Self {
        Self::new()
    }
}

impl NotifierConfig {
    /// create a new configuration with default values
    pub fn new() -> Self {
        Self {
            store_path: default_store_path(),
            channel_capacity: default_channel_capacity(),
            adapters: vec![AdapterConfig::new()],
        }
    }
}

/// Provide temporary directories as default storage paths
fn default_store_path() -> String {
    env::temp_dir().join("event-notification").to_string_lossy().to_string()
}

/// Provides the recommended default channel capacity for high concurrency systems
fn default_channel_capacity() -> usize {
    10000 // Reasonable default values for high concurrency systems
}
