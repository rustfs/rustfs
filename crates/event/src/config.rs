use config::{Config, File, FileFormat};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::env;

const DEFAULT_CONFIG_FILE: &str = "event";

/// Configuration for the webhook adapter.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebhookConfig {
    pub endpoint: String,
    pub auth_token: Option<String>,
    pub custom_headers: Option<HashMap<String, String>>,
    pub max_retries: u32,
    pub timeout: u64,
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
    ///
    /// let config = WebhookConfig {
    ///     endpoint: "http://example.com/webhook".to_string(),
    ///     auth_token: Some("my_token".to_string()),
    ///     custom_headers: None,
    ///     max_retries: 3,
    ///     timeout: 5000,
    /// };
    ///
    /// assert!(config.validate().is_ok());
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
    #[serde(default = "default_store_path")]
    pub store_path: String,
    #[serde(default = "default_channel_capacity")]
    pub channel_capacity: usize,
    pub adapters: Vec<AdapterConfig>,
}

impl Default for NotifierConfig {
    fn default() -> Self {
        Self {
            store_path: default_store_path(),
            channel_capacity: default_channel_capacity(),
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
                let path = std::path::Path::new(&path);
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
fn default_store_path() -> String {
    env::var("EVENT_STORE_PATH").unwrap_or_else(|e| {
        tracing::info!("Failed to get `EVENT_STORE_PATH` failed err: {}", e.to_string());
        env::temp_dir().join(DEFAULT_CONFIG_FILE).to_string_lossy().to_string()
    })
}

/// Provides the recommended default channel capacity for high concurrency systems
fn default_channel_capacity() -> usize {
    env::var("EVENT_CHANNEL_CAPACITY")
        .unwrap_or_else(|_| "10000".to_string())
        .parse()
        .unwrap_or(10000) // Default to 10000 if parsing fails
}
