use keyring::Entry;
use serde::{Deserialize, Serialize};
use std::error::Error;

/// Configuration for the RustFS service
///
/// # Fields
/// * `address` - The address of the RustFS service
/// * `host` - The host of the RustFS service
/// * `port` - The port of the RustFS service
/// * `access_key` - The access key of the RustFS service
/// * `secret_key` - The secret key of the RustFS service
/// * `domain_name` - The domain name of the RustFS service
/// * `volume_name` - The volume name of the RustFS service
/// * `console_address` - The console address of the RustFS service
///
/// # Example
/// ```
/// let config = RustFSConfig {
///    address: "127.0.0.1:9000".to_string(),
///    host: "127.0.0.1".to_string(),
///    port: "9000".to_string(),
///    access_key: "rustfsadmin".to_string(),
///    secret_key: "rustfsadmin".to_string(),
///    domain_name: "demo.rustfs.com".to_string(),
///    volume_name: "data".to_string(),
///    console_address: "127.0.0.1:9001".to_string(),
/// };
/// println!("{:?}", config);
/// assert_eq!(config.address, "127.0.0.1:9000");
/// ```
#[derive(Debug, Clone, Default, Deserialize, Serialize, Ord, PartialOrd, Eq, PartialEq)]
pub struct RustFSConfig {
    pub address: String,
    pub host: String,
    pub port: String,
    pub access_key: String,
    pub secret_key: String,
    pub domain_name: String,
    pub volume_name: String,
    pub console_address: String,
}

impl RustFSConfig {
    /// keyring the name of the service
    const SERVICE_NAME: &'static str = "rustfs-service";
    /// keyring the key of the service
    const SERVICE_KEY: &'static str = "rustfs_key";
    /// default domain name
    const DEFAULT_DOMAIN_NAME_VALUE: &'static str = "demo.rustfs.com";
    /// default address value
    const DEFAULT_ADDRESS_VALUE: &'static str = "127.0.0.1:9000";
    /// default port value
    const DEFAULT_PORT_VALUE: &'static str = "9000";
    /// default host value
    const DEFAULT_HOST_VALUE: &'static str = "127.0.0.1";
    /// default access key value
    const DEFAULT_ACCESS_KEY_VALUE: &'static str = "rustfsadmin";
    /// default secret key value
    const DEFAULT_SECRET_KEY_VALUE: &'static str = "rustfsadmin";
    /// default console address value
    const DEFAULT_CONSOLE_ADDRESS_VALUE: &'static str = "127.0.0.1:9001";

    /// get the default volume_name
    ///
    /// # Returns
    /// * The default volume name
    ///
    /// # Example
    /// ```
    /// let volume_name = RustFSConfig::default_volume_name();
    /// ```
    pub fn default_volume_name() -> String {
        dirs::home_dir()
            .map(|home| home.join("rustfs").join("data"))
            .and_then(|path| path.to_str().map(String::from))
            .unwrap_or_else(|| "data".to_string())
    }

    /// create a default configuration
    ///
    /// # Returns
    /// * The default configuration
    ///
    /// # Example
    /// ```
    /// let config = RustFSConfig::default_config();
    /// println!("{:?}", config);
    /// assert_eq!(config.address, "127.0.0.1:9000");
    /// ```
    pub fn default_config() -> Self {
        Self {
            address: Self::DEFAULT_ADDRESS_VALUE.to_string(),
            host: Self::DEFAULT_HOST_VALUE.to_string(),
            port: Self::DEFAULT_PORT_VALUE.to_string(),
            access_key: Self::DEFAULT_ACCESS_KEY_VALUE.to_string(),
            secret_key: Self::DEFAULT_SECRET_KEY_VALUE.to_string(),
            domain_name: Self::DEFAULT_DOMAIN_NAME_VALUE.to_string(),
            volume_name: Self::default_volume_name(),
            console_address: Self::DEFAULT_CONSOLE_ADDRESS_VALUE.to_string(),
        }
    }

    /// Load the configuration from the keyring
    ///
    /// # Errors
    /// * If the configuration cannot be loaded from the keyring
    /// * If the configuration cannot be deserialized
    /// * If the address cannot be extracted from the configuration
    ///
    /// # Example
    /// ```
    /// let config = RustFSConfig::load().unwrap();
    /// println!("{:?}", config);
    /// assert_eq!(config.address, "127.0.0.1:9000");
    /// ```
    pub fn load() -> Result<Self, Box<dyn Error>> {
        let mut config = Self::default_config();

        // Try to get the configuration of the storage from the keyring
        let entry = Entry::new(Self::SERVICE_NAME, Self::SERVICE_KEY)?;
        if let Ok(stored_json) = entry.get_password() {
            if let Ok(stored_config) = serde_json::from_str::<RustFSConfig>(&stored_json) {
                // update fields that are not empty and non default
                if !stored_config.address.is_empty() && stored_config.address != Self::DEFAULT_ADDRESS_VALUE {
                    config.address = stored_config.address;
                    let (host, port) = Self::extract_host_port(config.address.as_str())
                        .ok_or_else(|| format!("无法从地址 '{}' 中提取主机和端口", config.address))?;
                    config.host = host.to_string();
                    config.port = port.to_string();
                }
                if !stored_config.access_key.is_empty() && stored_config.access_key != Self::DEFAULT_ACCESS_KEY_VALUE {
                    config.access_key = stored_config.access_key;
                }
                if !stored_config.secret_key.is_empty() && stored_config.secret_key != Self::DEFAULT_SECRET_KEY_VALUE {
                    config.secret_key = stored_config.secret_key;
                }
                if !stored_config.domain_name.is_empty() && stored_config.domain_name != Self::DEFAULT_DOMAIN_NAME_VALUE {
                    config.domain_name = stored_config.domain_name;
                }
                // The stored volume_name is updated only if it is not empty and different from the default
                if !stored_config.volume_name.is_empty() && stored_config.volume_name != Self::default_volume_name() {
                    config.volume_name = stored_config.volume_name;
                }
                if !stored_config.console_address.is_empty()
                    && stored_config.console_address != Self::DEFAULT_CONSOLE_ADDRESS_VALUE
                {
                    config.console_address = stored_config.console_address;
                }
            }
        }

        Ok(config)
    }

    /// Auxiliary method: Extract the host and port from the address string
    /// # Arguments
    /// * `address` - The address string
    ///
    /// # Returns
    /// * `Some((host, port))` - The host and port
    ///
    /// # Errors
    /// * If the address is not in the form 'host:port'
    /// * If the port is not a valid u16
    ///
    /// # Example
    /// ```
    /// let (host, port) = RustFSConfig::extract_host_port("127.0.0.1:9000").unwrap();
    /// assert_eq!(host, "127.0.0.1");
    /// assert_eq!(port, 9000);
    /// ```
    pub fn extract_host_port(address: &str) -> Option<(&str, u16)> {
        let parts: Vec<&str> = address.split(':').collect();
        if parts.len() == 2 {
            if let Ok(port) = parts[1].parse::<u16>() {
                return Some((parts[0], port));
            }
        }
        None
    }

    /// save the configuration to keyring
    ///
    /// # Errors
    /// * If the configuration cannot be serialized
    /// * If the configuration cannot be saved to the keyring
    ///
    /// # Example
    /// ```
    /// let config = RustFSConfig::default_config();
    /// config.save().unwrap();
    /// ```
    pub fn save(&self) -> Result<(), Box<dyn Error>> {
        let entry = Entry::new(Self::SERVICE_NAME, Self::SERVICE_KEY)?;
        let json = serde_json::to_string(self)?;
        entry.set_password(&json)?;
        Ok(())
    }

    /// delete the stored configuration
    ///
    /// # Errors
    /// * If the configuration cannot be deleted from the keyring
    ///
    /// # Example
    /// ```
    /// RustFSConfig::clear().unwrap();
    /// ```
    #[allow(dead_code)]
    pub fn clear() -> Result<(), Box<dyn Error>> {
        let entry = Entry::new(Self::SERVICE_NAME, Self::SERVICE_KEY)?;
        entry.delete_credential()?;
        Ok(())
    }
}
