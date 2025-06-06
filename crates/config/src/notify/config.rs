use crate::notify::mqtt::MQTTArgs;
use crate::notify::webhook::WebhookArgs;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Config - notification target configuration structure, holds
/// information about various notification targets.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NotifyConfig {
    pub mqtt: HashMap<String, MQTTArgs>,
    pub webhook: HashMap<String, WebhookArgs>,
}

impl NotifyConfig {
    /// Create a new configuration with default values.
    pub fn new() -> Self {
        let mut config = NotifyConfig {
            webhook: HashMap::new(),
            mqtt: HashMap::new(),
        };
        // Insert default target for each backend
        config.webhook.insert("1".to_string(), WebhookArgs::new());
        config.mqtt.insert("1".to_string(), MQTTArgs::new());
        config
    }
}

impl Default for NotifyConfig {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use crate::notify::config::NotifyConfig;

    #[test]
    fn test_notify_config_new() {
        let config = NotifyConfig::new();
        assert_eq!(config.webhook.len(), 1);
        assert_eq!(config.mqtt.len(), 1);
        assert!(config.webhook.contains_key("1"));
        assert!(config.mqtt.contains_key("1"));
    }

    #[test]
    fn test_notify_config_default() {
        let config = NotifyConfig::default();
        assert_eq!(config.webhook.len(), 1);
        assert_eq!(config.mqtt.len(), 1);
    }
}
