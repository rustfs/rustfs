use crate::config::adapter::AdapterCommon;
use crate::config::{default_queue_dir, default_queue_limit};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;
use tracing::info;

/// Configuration for the webhook adapter.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct WebhookConfig {
    #[serde(flatten)]
    pub common: AdapterCommon,
    pub endpoint: String,
    pub auth_token: Option<String>,
    pub custom_headers: Option<HashMap<String, String>>,
    pub max_retries: u32,
    pub retry_interval: Option<u64>,
    pub timeout: Option<u64>,
    #[serde(default)]
    pub client_cert: Option<String>,
    #[serde(default)]
    pub client_key: Option<String>,
}

impl WebhookConfig {
    /// validate the configuration for the webhook adapter
    ///
    /// # Returns
    ///
    /// - `Result<(), String>`: Ok if the configuration is valid, Err with a message if invalid.
    ///
    /// # Errors
    /// - Returns an error if the configuration is invalid, such as empty endpoint, unreasonable timeout, or mismatched certificate and key.
    pub fn validate(&self) -> Result<(), String> {
        // If not enabled, the other fields are not validated
        if !self.common.enable {
            return Ok(());
        }

        // verify that endpoint cannot be empty
        if self.endpoint.trim().is_empty() {
            return Err("Webhook endpoint cannot be empty".to_string());
        }

        // verification timeout must be reasonable
        if self.timeout.is_some() {
            match self.timeout {
                Some(timeout) if timeout > 0 => {
                    info!("Webhook timeout is set to {}", timeout);
                }
                _ => return Err("Webhook timeout must be greater than 0".to_string()),
            }
        }

        // Verify that the maximum number of retry is reasonable
        if self.max_retries > 10 {
            return Err("Maximum retry count cannot exceed 10".to_string());
        }

        // Verify the queue directory path
        if !self.common.queue_dir.is_empty() && !Path::new(&self.common.queue_dir).is_absolute() {
            return Err("Queue directory path should be absolute".to_string());
        }

        // The authentication certificate and key must appear in pairs
        if (self.client_cert.is_some() && self.client_key.is_none()) || (self.client_cert.is_none() && self.client_key.is_some())
        {
            return Err("Certificate and key must be specified as a pair".to_string());
        }

        Ok(())
    }

    /// Create a new webhook configuration
    pub fn new(identifier: impl Into<String>, endpoint: impl Into<String>) -> Self {
        Self {
            common: AdapterCommon {
                identifier: identifier.into(),
                comment: String::new(),
                enable: true,
                queue_dir: default_queue_dir(),
                queue_limit: default_queue_limit(),
            },
            endpoint: endpoint.into(),
            ..Default::default()
        }
    }
}
