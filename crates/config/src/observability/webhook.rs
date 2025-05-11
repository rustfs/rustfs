use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Webhook sink configuration
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct WebhookSink {
    pub endpoint: String,
    pub auth_token: String,
    pub headers: Option<HashMap<String, String>>,
    #[serde(default = "default_max_retries")]
    pub max_retries: Option<usize>,
    #[serde(default = "default_retry_delay_ms")]
    pub retry_delay_ms: Option<u64>,
}

impl WebhookSink {
    pub fn new() -> Self {
        Self {
            endpoint: "".to_string(),
            auth_token: "".to_string(),
            headers: Some(HashMap::new()),
            max_retries: default_max_retries(),
            retry_delay_ms: default_retry_delay_ms(),
        }
    }
}

impl Default for WebhookSink {
    fn default() -> Self {
        Self::new()
    }
}

fn default_max_retries() -> Option<usize> {
    Some(3)
}
fn default_retry_delay_ms() -> Option<u64> {
    Some(100)
}
