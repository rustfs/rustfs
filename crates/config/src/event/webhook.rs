use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Configuration for the notification system.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebhookAdapter {
    pub endpoint: String,
    pub auth_token: Option<String>,
    pub custom_headers: Option<HashMap<String, String>>,
    pub max_retries: u32,
    pub timeout: u64,
}

impl WebhookAdapter {
    /// verify that the configuration is valid
    pub fn validate(&self) -> Result<(), String> {
        // verify that endpoint cannot be empty
        if self.endpoint.trim().is_empty() {
            return Err("Webhook endpoint cannot be empty".to_string());
        }

        // verification timeout must be reasonable
        if self.timeout == 0 {
            return Err("Webhook timeout must be greater than 0".to_string());
        }

        // Verify that the maximum number of retry is reasonable
        if self.max_retries > 10 {
            return Err("Maximum retry count cannot exceed 10".to_string());
        }

        Ok(())
    }

    /// Get the default configuration
    pub fn new() -> Self {
        Self {
            endpoint: "".to_string(),
            auth_token: None,
            custom_headers: Some(HashMap::new()),
            max_retries: 3,
            timeout: 1000,
        }
    }
}

impl Default for WebhookAdapter {
    fn default() -> Self {
        Self::new()
    }
}
