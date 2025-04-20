use crate::Error;
use figment::providers::Format;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Configuration for the notification system.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebhookConfig {
    pub endpoint: String,
    pub auth_token: Option<String>,
    pub custom_headers: Option<HashMap<String, String>>,
    pub max_retries: u32,
    pub timeout: u64,
}

impl WebhookConfig {
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
}

/// Configuration for the Kafka adapter.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaConfig {
    pub brokers: String,
    pub topic: String,
    pub max_retries: u32,
    pub timeout: u64,
}

/// Configuration for the MQTT adapter.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MqttConfig {
    pub broker: String,
    pub port: u16,
    pub client_id: String,
    pub topic: String,
    pub max_retries: u32,
}

/// Configuration for the notification system.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum AdapterConfig {
    Webhook(WebhookConfig),
    Kafka(KafkaConfig),
    Mqtt(MqttConfig),
}

/// http producer configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HttpProducerConfig {
    #[serde(default = "default_http_port")]
    pub port: u16,
}

impl Default for HttpProducerConfig {
    fn default() -> Self {
        Self {
            port: default_http_port(),
        }
    }
}

fn default_http_port() -> u16 {
    3000
}

/// Configuration for the notification system.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NotificationConfig {
    #[serde(default = "default_store_path")]
    pub store_path: String,
    #[serde(default = "default_channel_capacity")]
    pub channel_capacity: usize,
    pub adapters: Vec<AdapterConfig>,
    #[serde(default)]
    pub http: HttpProducerConfig,
}

impl Default for NotificationConfig {
    fn default() -> Self {
        Self {
            store_path: default_store_path(),
            channel_capacity: default_channel_capacity(),
            adapters: Vec::new(),
            http: HttpProducerConfig::default(),
        }
    }
}

impl NotificationConfig {
    /// create a new configuration with default values
    pub fn new() -> Self {
        Self::default()
    }

    /// create a configuration from a configuration file
    pub fn from_file(path: &str) -> Result<Self, Error> {
        let config = figment::Figment::new()
            .merge(figment::providers::Toml::file(path))
            .extract()?;

        Ok(config)
    }

    /// Read configuration from multiple sources (support TOML, YAML, .env)
    pub fn load() -> Result<Self, Error> {
        let figment = figment::Figment::new()
            // First try to read the config.toml of the current directory
            .merge(figment::providers::Toml::file("event.toml"))
            // Then try to read the config.yaml of the current directory
            .merge(figment::providers::Yaml::file("event.yaml"))
            // Finally read the environment variable and overwrite the previous value
            .merge(figment::providers::Env::prefixed("EVENT_NOTIF_"));

        Ok(figment.extract()?)
    }

    /// loading configuration from env file
    pub fn from_env_file(path: &str) -> Result<Self, Error> {
        // loading env files
        dotenv::from_path(path)
            .map_err(|e| Error::ConfigError(format!("unable to load env file: {}", e)))?;

        // Extract configuration from environment variables using figurement
        let figment =
            figment::Figment::new().merge(figment::providers::Env::prefixed("EVENT_NOTIF_"));

        Ok(figment.extract()?)
    }
}

/// Provide temporary directories as default storage paths
fn default_store_path() -> String {
    std::env::temp_dir()
        .join("event-notification")
        .to_string_lossy()
        .to_string()
}

/// Provides the recommended default channel capacity for high concurrency systems
fn default_channel_capacity() -> usize {
    10000 // Reasonable default values for high concurrency systems
}
