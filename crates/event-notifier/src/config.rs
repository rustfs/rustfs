use config::{Config, Environment, File, FileFormat};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::env;

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

/// Configuration for the notification system.
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
    /// use rustfs_event_notifier::NotifierConfig;
    ///
    /// let config = NotifierConfig::load_config(None);
    /// ```
    pub fn load_config(config_dir: Option<String>) -> NotifierConfig {
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
            .add_source(
                Environment::default()
                    .prefix("NOTIFIER")
                    .prefix_separator("__")
                    .separator("__")
                    .list_separator("_")
                    .with_list_parse_key("adapters")
                    .try_parsing(true),
            )
            .build()
            .unwrap_or_default();
        println!("Loaded config: {:?}", app_config);
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
}

const DEFAULT_CONFIG_FILE: &str = "obs";

/// Provide temporary directories as default storage paths
fn default_store_path() -> String {
    std::env::temp_dir().join("event-notification").to_string_lossy().to_string()
}

/// Provides the recommended default channel capacity for high concurrency systems
fn default_channel_capacity() -> usize {
    10000 // Reasonable default values for high concurrency systems
}
