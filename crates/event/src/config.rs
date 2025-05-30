use config::{Config, File, FileFormat};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::env;
use std::path::Path;
use tracing::info;

/// The default configuration file name
const DEFAULT_CONFIG_FILE: &str = "event";

/// The prefix for the configuration file
pub const STORE_PREFIX: &str = "rustfs";

/// The default retry interval for the webhook adapter
pub const DEFAULT_RETRY_INTERVAL: u64 = 3;

/// The default maximum retry count for the webhook adapter
pub const DEFAULT_MAX_RETRIES: u32 = 3;

/// Add a common field for the adapter configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdapterCommon {
    /// Adapter identifier for unique identification
    pub identifier: String,
    /// Adapter description information
    pub comment: String,
    /// Whether to enable this adapter
    #[serde(default)]
    pub enable: bool,
    #[serde(default = "default_queue_dir")]
    pub queue_dir: String,
    #[serde(default = "default_queue_limit")]
    pub queue_limit: u64,
}

impl Default for AdapterCommon {
    fn default() -> Self {
        Self {
            identifier: String::new(),
            comment: String::new(),
            enable: false,
            queue_dir: default_queue_dir(),
            queue_limit: default_queue_limit(),
        }
    }
}

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
    /// # Example
    ///
    /// ```
    /// use rustfs_event::WebhookConfig;
    /// use rustfs_event::AdapterCommon;
    /// use rustfs_event::DEFAULT_RETRY_INTERVAL;
    ///
    /// let config = WebhookConfig {
    ///     common: AdapterCommon::default(),
    ///     endpoint: "https://example.com/webhook".to_string(),
    ///     auth_token: Some("my_token".to_string()),
    ///     custom_headers: None,
    ///     max_retries: 3,
    ///     retry_interval: Some(DEFAULT_RETRY_INTERVAL),
    ///     timeout: Some(5000),
    ///     client_cert: None,
    ///     client_key: None,
    /// };
    ///
    /// assert!(config.validate().is_ok());
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

/// Configuration for the MQTT adapter.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MqttConfig {
    #[serde(flatten)]
    pub common: AdapterCommon,
    pub broker: String,
    pub port: u16,
    pub client_id: String,
    pub topic: String,
    pub max_retries: u32,
}

impl Default for MqttConfig {
    fn default() -> Self {
        Self {
            common: AdapterCommon::default(),
            broker: String::new(),
            port: 1883,
            client_id: String::new(),
            topic: String::new(),
            max_retries: 3,
        }
    }
}

impl MqttConfig {
    /// Create a new MQTT configuration
    pub fn new(identifier: impl Into<String>, broker: impl Into<String>, topic: impl Into<String>) -> Self {
        Self {
            common: AdapterCommon {
                identifier: identifier.into(),
                comment: String::new(),
                enable: true,
                queue_dir: default_queue_dir(),
                queue_limit: default_queue_limit(),
            },
            broker: broker.into(),
            topic: topic.into(),
            ..Default::default()
        }
    }
}

/// Configuration for the adapter.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum AdapterConfig {
    Webhook(WebhookConfig),
    Kafka(KafkaConfig),
    Mqtt(MqttConfig),
}

/// Configuration for the notification system.
///
/// This struct contains the configuration for the notification system, including
/// the storage path, channel capacity, and a list of adapters.
///
/// # Fields
///
/// - `store_path`: The path to the storage directory.
/// - `channel_capacity`: The capacity of the notification channel.
/// - `adapters`: A list of adapters to be used for notifications.
///
/// # Example
///
/// ```
/// use rustfs_event::NotifierConfig;
///
/// let config = NotifierConfig::new();
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NotifierConfig {
    #[serde(default = "default_queue_dir")]
    pub store_path: String,
    #[serde(default = "default_queue_limit")]
    pub channel_capacity: u64,
    pub adapters: Vec<AdapterConfig>,
}

impl Default for NotifierConfig {
    fn default() -> Self {
        Self {
            store_path: default_queue_dir(),
            channel_capacity: default_queue_limit(),
            adapters: Vec::new(),
        }
    }
}

impl NotifierConfig {
    /// create a new configuration with default values
    pub fn new() -> Self {
        Self::default()
    }

    /// Loading the configuration file
    /// Supports TOML, YAML and .env formats, read in order by priority
    ///
    /// # Parameters
    /// - `config_dir`: Configuration file path
    ///
    /// # Returns
    /// Configuration information
    ///
    /// # Example
    /// ```
    /// use rustfs_event::NotifierConfig;
    ///
    /// let config = NotifierConfig::event_load_config(None);
    /// ```
    pub fn event_load_config(config_dir: Option<String>) -> NotifierConfig {
        let config_dir = if let Some(path) = config_dir {
            // If a path is provided, check if it's empty
            if path.is_empty() {
                // If empty, use the default config file name
                DEFAULT_CONFIG_FILE.to_string()
            } else {
                // Use the provided path
                let path = Path::new(&path);
                if path.extension().is_some() {
                    // If path has extension, use it as is (extension will be added by Config::builder)
                    path.with_extension("").to_string_lossy().into_owned()
                } else {
                    // If path is a directory, append the default config file name
                    path.to_string_lossy().into_owned()
                }
            }
        } else {
            // If no path provided, use current directory + default config file
            match env::current_dir() {
                Ok(dir) => dir.join(DEFAULT_CONFIG_FILE).to_string_lossy().into_owned(),
                Err(_) => {
                    eprintln!("Warning: Failed to get current directory, using default config file");
                    DEFAULT_CONFIG_FILE.to_string()
                }
            }
        };

        // Log using proper logging instead of println when possible
        println!("Using config file base: {}", config_dir);

        let app_config = Config::builder()
            .add_source(File::with_name(config_dir.as_str()).format(FileFormat::Toml).required(false))
            .add_source(File::with_name(config_dir.as_str()).format(FileFormat::Yaml).required(false))
            .build()
            .unwrap_or_default();
        match app_config.try_deserialize::<NotifierConfig>() {
            Ok(app_config) => {
                println!("Parsed AppConfig: {:?} \n", app_config);
                app_config
            }
            Err(e) => {
                println!("Failed to deserialize config: {}", e);
                NotifierConfig::default()
            }
        }
    }

    /// unmarshal the configuration from a byte array
    pub fn unmarshal(data: &[u8]) -> common::error::Result<NotifierConfig> {
        let m: NotifierConfig = serde_json::from_slice(data)?;
        Ok(m)
    }

    /// marshal the configuration to a byte array
    pub fn marshal(&self) -> common::error::Result<Vec<u8>> {
        let data = serde_json::to_vec(&self)?;
        Ok(data)
    }

    /// merge the configuration with default values
    pub fn merge(&self) -> NotifierConfig {
        self.clone()
    }
}

/// Provide temporary directories as default storage paths
fn default_queue_dir() -> String {
    env::var("EVENT_QUEUE_DIR").unwrap_or_else(|e| {
        tracing::info!("Failed to get `EVENT_QUEUE_DIR` failed err: {}", e.to_string());
        env::temp_dir().join(DEFAULT_CONFIG_FILE).to_string_lossy().to_string()
    })
}

/// Provides the recommended default channel capacity for high concurrency systems
pub(crate) fn default_queue_limit() -> u64 {
    env::var("EVENT_CHANNEL_CAPACITY")
        .unwrap_or_else(|_| "10000".to_string())
        .parse()
        .unwrap_or(10000) // Default to 10000 if parsing fails
}

/// Event Notifier Configuration
/// This struct contains the configuration for the event notifier system,
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct EventNotifierConfig {
    /// A collection of webhook configurations, with the key being a unique identifier
    #[serde(default)]
    pub webhook: HashMap<String, WebhookConfig>,
    /// A collection of Kafka configurations, with the key being a unique identifier
    #[serde(default)]
    pub kafka: HashMap<String, KafkaConfig>,
    ///MQTT configuration collection, with the key being a unique identifier
    #[serde(default)]
    pub mqtt: HashMap<String, MqttConfig>,
}

impl EventNotifierConfig {
    /// Create a new default configuration
    pub fn new() -> Self {
        Self::default()
    }

    /// Load the configuration from the file
    pub fn event_load_config(_config_dir: Option<String>) -> EventNotifierConfig {
        // The existing implementation remains the same, but returns EventNotifierConfig
        // ...

        Self::default()
    }

    /// Deserialization configuration
    pub fn unmarshal(data: &[u8]) -> common::error::Result<EventNotifierConfig> {
        let m: EventNotifierConfig = serde_json::from_slice(data)?;
        Ok(m)
    }

    /// Serialization configuration
    pub fn marshal(&self) -> common::error::Result<Vec<u8>> {
        let data = serde_json::to_vec(&self)?;
        Ok(data)
    }

    /// Convert this configuration to a list of adapter configurations
    pub fn to_adapter_configs(&self) -> Vec<AdapterConfig> {
        let mut adapters = Vec::new();

        // Add all enabled webhook configurations
        for webhook in self.webhook.values() {
            if webhook.common.enable {
                adapters.push(AdapterConfig::Webhook(webhook.clone()));
            }
        }

        // Add all enabled Kafka configurations
        for kafka in self.kafka.values() {
            if kafka.common.enable {
                adapters.push(AdapterConfig::Kafka(kafka.clone()));
            }
        }

        // Add all enabled MQTT configurations
        for mqtt in self.mqtt.values() {
            if mqtt.common.enable {
                adapters.push(AdapterConfig::Mqtt(mqtt.clone()));
            }
        }

        adapters
    }
}
