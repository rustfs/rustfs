use serde::Deserialize;

/// Webhook sink configuration
#[derive(Debug, Deserialize, Clone)]
pub struct WebhookSinkConfig {
    pub url: String,
    pub method: String,
    pub headers: Vec<(String, String)>,
}

impl WebhookSinkConfig {
    pub fn new() -> Self {
        Self {
            url: "http://localhost:8080/webhook".to_string(),
            method: "POST".to_string(),
            headers: vec![],
        }
    }
}

impl Default for WebhookSinkConfig {
    fn default() -> Self {
        Self::new()
    }
}
