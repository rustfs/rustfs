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

    /// Clear the stored configuration from the system keyring
    ///
    /// # Returns
    /// `Ok(())` if the configuration was successfully cleared, or an error if the operation failed.
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rustfs_config_default() {
        let config = RustFSConfig::default();
        assert!(config.address.is_empty());
        assert!(config.host.is_empty());
        assert!(config.port.is_empty());
        assert!(config.access_key.is_empty());
        assert!(config.secret_key.is_empty());
        assert!(config.domain_name.is_empty());
        assert!(config.volume_name.is_empty());
        assert!(config.console_address.is_empty());
    }

    #[test]
    fn test_rustfs_config_creation() {
        let config = RustFSConfig {
            address: "192.168.1.100:9000".to_string(),
            host: "192.168.1.100".to_string(),
            port: "9000".to_string(),
            access_key: "testuser".to_string(),
            secret_key: "testpass".to_string(),
            domain_name: "test.rustfs.com".to_string(),
            volume_name: "/data/rustfs".to_string(),
            console_address: "192.168.1.100:9001".to_string(),
        };

        assert_eq!(config.address, "192.168.1.100:9000");
        assert_eq!(config.host, "192.168.1.100");
        assert_eq!(config.port, "9000");
        assert_eq!(config.access_key, "testuser");
        assert_eq!(config.secret_key, "testpass");
        assert_eq!(config.domain_name, "test.rustfs.com");
        assert_eq!(config.volume_name, "/data/rustfs");
        assert_eq!(config.console_address, "192.168.1.100:9001");
    }

    #[test]
    fn test_default_volume_name() {
        let volume_name = RustFSConfig::default_volume_name();
        assert!(!volume_name.is_empty());
        // Should either be the home directory path or fallback to "data"
        assert!(volume_name.contains("rustfs") || volume_name == "data");
    }

    #[test]
    fn test_default_config() {
        let config = RustFSConfig::default_config();
        assert_eq!(config.address, RustFSConfig::DEFAULT_ADDRESS_VALUE);
        assert_eq!(config.host, RustFSConfig::DEFAULT_HOST_VALUE);
        assert_eq!(config.port, RustFSConfig::DEFAULT_PORT_VALUE);
        assert_eq!(config.access_key, RustFSConfig::DEFAULT_ACCESS_KEY_VALUE);
        assert_eq!(config.secret_key, RustFSConfig::DEFAULT_SECRET_KEY_VALUE);
        assert_eq!(config.domain_name, RustFSConfig::DEFAULT_DOMAIN_NAME_VALUE);
        assert_eq!(config.console_address, RustFSConfig::DEFAULT_CONSOLE_ADDRESS_VALUE);
        assert!(!config.volume_name.is_empty());
    }

    #[test]
    fn test_extract_host_port_valid() {
        let test_cases = vec![
            ("127.0.0.1:9000", Some(("127.0.0.1", 9000))),
            ("localhost:8080", Some(("localhost", 8080))),
            ("192.168.1.100:3000", Some(("192.168.1.100", 3000))),
            ("0.0.0.0:80", Some(("0.0.0.0", 80))),
            ("example.com:443", Some(("example.com", 443))),
        ];

        for (input, expected) in test_cases {
            let result = RustFSConfig::extract_host_port(input);
            assert_eq!(result, expected, "Failed for input: {input}");
        }
    }

    #[test]
    fn test_extract_host_port_invalid() {
        let invalid_cases = vec![
            "127.0.0.1",            // Missing port
            "127.0.0.1:",           // Empty port
            "127.0.0.1:abc",        // Invalid port
            "127.0.0.1:99999",      // Port out of range
            "",                     // Empty string
            "127.0.0.1:9000:extra", // Too many parts
            "invalid",              // No colon
        ];

        for input in invalid_cases {
            let result = RustFSConfig::extract_host_port(input);
            assert_eq!(result, None, "Should be None for input: {input}");
        }

        // Special case: empty host but valid port should still work
        let result = RustFSConfig::extract_host_port(":9000");
        assert_eq!(result, Some(("", 9000)));
    }

    #[test]
    fn test_extract_host_port_edge_cases() {
        // Test edge cases for port numbers
        assert_eq!(RustFSConfig::extract_host_port("host:0"), Some(("host", 0)));
        assert_eq!(RustFSConfig::extract_host_port("host:65535"), Some(("host", 65535)));
        assert_eq!(RustFSConfig::extract_host_port("host:65536"), None); // Out of range
    }

    #[test]
    fn test_serialization() {
        let config = RustFSConfig {
            address: "127.0.0.1:9000".to_string(),
            host: "127.0.0.1".to_string(),
            port: "9000".to_string(),
            access_key: "admin".to_string(),
            secret_key: "password".to_string(),
            domain_name: "test.com".to_string(),
            volume_name: "/data".to_string(),
            console_address: "127.0.0.1:9001".to_string(),
        };

        let json = serde_json::to_string(&config).unwrap();
        assert!(json.contains("127.0.0.1:9000"));
        assert!(json.contains("admin"));
        assert!(json.contains("test.com"));
    }

    #[test]
    fn test_deserialization() {
        let json = r#"{
            "address": "192.168.1.100:9000",
            "host": "192.168.1.100",
            "port": "9000",
            "access_key": "testuser",
            "secret_key": "testpass",
            "domain_name": "example.com",
            "volume_name": "/opt/data",
            "console_address": "192.168.1.100:9001"
        }"#;

        let config: RustFSConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.address, "192.168.1.100:9000");
        assert_eq!(config.host, "192.168.1.100");
        assert_eq!(config.port, "9000");
        assert_eq!(config.access_key, "testuser");
        assert_eq!(config.secret_key, "testpass");
        assert_eq!(config.domain_name, "example.com");
        assert_eq!(config.volume_name, "/opt/data");
        assert_eq!(config.console_address, "192.168.1.100:9001");
    }

    #[test]
    fn test_serialization_deserialization_roundtrip() {
        let original_config = RustFSConfig {
            address: "10.0.0.1:8080".to_string(),
            host: "10.0.0.1".to_string(),
            port: "8080".to_string(),
            access_key: "roundtrip_user".to_string(),
            secret_key: "roundtrip_pass".to_string(),
            domain_name: "roundtrip.test".to_string(),
            volume_name: "/tmp/roundtrip".to_string(),
            console_address: "10.0.0.1:8081".to_string(),
        };

        let json = serde_json::to_string(&original_config).unwrap();
        let deserialized_config: RustFSConfig = serde_json::from_str(&json).unwrap();

        assert_eq!(original_config, deserialized_config);
    }

    #[test]
    fn test_config_ordering() {
        let config1 = RustFSConfig {
            address: "127.0.0.1:9000".to_string(),
            host: "127.0.0.1".to_string(),
            port: "9000".to_string(),
            access_key: "admin".to_string(),
            secret_key: "password".to_string(),
            domain_name: "test.com".to_string(),
            volume_name: "/data".to_string(),
            console_address: "127.0.0.1:9001".to_string(),
        };

        let config2 = RustFSConfig {
            address: "127.0.0.1:9000".to_string(),
            host: "127.0.0.1".to_string(),
            port: "9000".to_string(),
            access_key: "admin".to_string(),
            secret_key: "password".to_string(),
            domain_name: "test.com".to_string(),
            volume_name: "/data".to_string(),
            console_address: "127.0.0.1:9001".to_string(),
        };

        let config3 = RustFSConfig {
            address: "127.0.0.1:9001".to_string(), // Different port
            host: "127.0.0.1".to_string(),
            port: "9001".to_string(),
            access_key: "admin".to_string(),
            secret_key: "password".to_string(),
            domain_name: "test.com".to_string(),
            volume_name: "/data".to_string(),
            console_address: "127.0.0.1:9002".to_string(),
        };

        assert_eq!(config1, config2);
        assert_ne!(config1, config3);
        assert!(config1 < config3); // Lexicographic ordering
    }

    #[test]
    fn test_clone() {
        let original = RustFSConfig::default_config();
        let cloned = original.clone();

        assert_eq!(original, cloned);
        assert_eq!(original.address, cloned.address);
        assert_eq!(original.access_key, cloned.access_key);
    }

    #[test]
    fn test_debug_format() {
        let config = RustFSConfig::default_config();
        let debug_str = format!("{config:?}");

        assert!(debug_str.contains("RustFSConfig"));
        assert!(debug_str.contains("address"));
        assert!(debug_str.contains("127.0.0.1:9000"));
    }

    #[test]
    fn test_constants() {
        assert_eq!(RustFSConfig::SERVICE_NAME, "rustfs-service");
        assert_eq!(RustFSConfig::SERVICE_KEY, "rustfs_key");
        assert_eq!(RustFSConfig::DEFAULT_DOMAIN_NAME_VALUE, "demo.rustfs.com");
        assert_eq!(RustFSConfig::DEFAULT_ADDRESS_VALUE, "127.0.0.1:9000");
        assert_eq!(RustFSConfig::DEFAULT_PORT_VALUE, "9000");
        assert_eq!(RustFSConfig::DEFAULT_HOST_VALUE, "127.0.0.1");
        assert_eq!(RustFSConfig::DEFAULT_ACCESS_KEY_VALUE, "rustfsadmin");
        assert_eq!(RustFSConfig::DEFAULT_SECRET_KEY_VALUE, "rustfsadmin");
        assert_eq!(RustFSConfig::DEFAULT_CONSOLE_ADDRESS_VALUE, "127.0.0.1:9001");
    }

    #[test]
    fn test_empty_strings() {
        let config = RustFSConfig {
            address: "".to_string(),
            host: "".to_string(),
            port: "".to_string(),
            access_key: "".to_string(),
            secret_key: "".to_string(),
            domain_name: "".to_string(),
            volume_name: "".to_string(),
            console_address: "".to_string(),
        };

        assert!(config.address.is_empty());
        assert!(config.host.is_empty());
        assert!(config.port.is_empty());
        assert!(config.access_key.is_empty());
        assert!(config.secret_key.is_empty());
        assert!(config.domain_name.is_empty());
        assert!(config.volume_name.is_empty());
        assert!(config.console_address.is_empty());
    }

    #[test]
    fn test_very_long_strings() {
        let long_string = "a".repeat(1000);
        let config = RustFSConfig {
            address: format!("{long_string}:9000"),
            host: long_string.clone(),
            port: "9000".to_string(),
            access_key: long_string.clone(),
            secret_key: long_string.clone(),
            domain_name: format!("{long_string}.com"),
            volume_name: format!("/data/{long_string}"),
            console_address: format!("{long_string}:9001"),
        };

        assert_eq!(config.host.len(), 1000);
        assert_eq!(config.access_key.len(), 1000);
        assert_eq!(config.secret_key.len(), 1000);
    }

    #[test]
    fn test_special_characters() {
        let config = RustFSConfig {
            address: "127.0.0.1:9000".to_string(),
            host: "127.0.0.1".to_string(),
            port: "9000".to_string(),
            access_key: "user@domain.com".to_string(),
            secret_key: "p@ssw0rd!#$%".to_string(),
            domain_name: "test-domain.example.com".to_string(),
            volume_name: "/data/rust-fs/storage".to_string(),
            console_address: "127.0.0.1:9001".to_string(),
        };

        assert!(config.access_key.contains("@"));
        assert!(config.secret_key.contains("!#$%"));
        assert!(config.domain_name.contains("-"));
        assert!(config.volume_name.contains("/"));
    }

    #[test]
    fn test_unicode_strings() {
        let config = RustFSConfig {
            address: "127.0.0.1:9000".to_string(),
            host: "127.0.0.1".to_string(),
            port: "9000".to_string(),
            access_key: "用户名".to_string(),
            secret_key: "密码 123".to_string(),
            domain_name: "测试.com".to_string(),
            volume_name: "/数据/存储".to_string(),
            console_address: "127.0.0.1:9001".to_string(),
        };

        assert_eq!(config.access_key, "用户名");
        assert_eq!(config.secret_key, "密码 123");
        assert_eq!(config.domain_name, "测试.com");
        assert_eq!(config.volume_name, "/数据/存储");
    }

    #[test]
    fn test_memory_efficiency() {
        // Test that the structure doesn't use excessive memory
        assert!(std::mem::size_of::<RustFSConfig>() < 1000);
    }

    // Note: Keyring-related tests (load, save, clear) are not included here
    // because they require actual keyring access and would be integration tests
    // rather than unit tests. They should be tested separately in an integration
    // test environment where keyring access can be properly mocked or controlled.
}
