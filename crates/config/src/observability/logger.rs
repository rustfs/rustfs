use serde::{Deserialize, Serialize};

/// Logger configuration
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct LoggerConfig {
    pub queue_capacity: Option<usize>,
}

impl LoggerConfig {
    pub fn new() -> Self {
        Self {
            queue_capacity: Some(10000),
        }
    }
}

impl Default for LoggerConfig {
    fn default() -> Self {
        Self::new()
    }
}
