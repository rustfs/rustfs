use serde::{Deserialize, Serialize};

/// WebhookArgs - Webhook target arguments.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebhookArgs {
    pub enable: bool,
    pub endpoint: String,
    pub auth_token: String,
    #[serde(skip)]
    pub queue_dir: String,
    pub queue_limit: u64,
    pub client_cert: String,
    pub client_key: String,
}

impl WebhookArgs {
    /// Create a new configuration with default values.
    pub fn new() -> Self {
        Self {
            enable: false,
            endpoint: "".to_string(),
            auth_token: "".to_string(),
            queue_dir: "".to_string(),
            queue_limit: 0,
            client_cert: "".to_string(),
            client_key: "".to_string(),
        }
    }

    /// Validate WebhookArgs fields
    pub fn validate(&self) -> Result<(), String> {
        if !self.enable {
            return Ok(());
        }
        if self.endpoint.trim().is_empty() {
            return Err("endpoint empty".to_string());
        }
        if self.queue_dir != "" && !self.queue_dir.starts_with('/') {
            return Err("queueDir path should be absolute".to_string());
        }
        if (self.client_cert != "" && self.client_key == "") || (self.client_cert == "" && self.client_key != "") {
            return Err("cert and key must be specified as a pair".to_string());
        }
        Ok(())
    }
}

impl Default for WebhookArgs {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_webhook_args_new() {
        let args = WebhookArgs::new();
        assert_eq!(args.endpoint, "");
        assert_eq!(args.auth_token, "");
        assert_eq!(args.queue_dir, "");
        assert_eq!(args.queue_limit, 0);
        assert_eq!(args.client_cert, "");
        assert_eq!(args.client_key, "");
        assert!(!args.enable);
    }

    #[test]
    fn test_webhook_args_validate() {
        let mut args = WebhookArgs::new();
        assert!(args.validate().is_err());
        args.endpoint = "http://example.com".to_string();
        assert!(args.validate().is_ok());
    }
}
