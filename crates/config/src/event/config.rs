use crate::event::adapters::AdapterConfig;
use serde::{Deserialize, Serialize};
use std::env;

#[allow(dead_code)]
const DEFAULT_CONFIG_FILE: &str = "event";

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
        Self::new()
    }
}

impl NotifierConfig {
    /// create a new configuration with default values
    pub fn new() -> Self {
        Self {
            store_path: default_store_path(),
            channel_capacity: default_channel_capacity(),
            adapters: vec![AdapterConfig::new()],
        }
    }
}

/// Provide temporary directories as default storage paths
fn default_store_path() -> String {
    env::temp_dir().join("event-notification").to_string_lossy().to_string()
}

/// Provides the recommended default channel capacity for high concurrency systems
fn default_channel_capacity() -> usize {
    10000 // Reasonable default values for high concurrency systems
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn test_notifier_config_new() {
        let config = NotifierConfig::new();

        // Verify store path is set
        assert!(!config.store_path.is_empty(), "Store path should not be empty");
        assert!(
            config.store_path.contains("event-notification"),
            "Store path should contain event-notification"
        );

        // Verify channel capacity is reasonable
        assert_eq!(config.channel_capacity, 10000, "Channel capacity should be 10000");
        assert!(config.channel_capacity > 0, "Channel capacity should be positive");

        // Verify adapters are initialized
        assert!(!config.adapters.is_empty(), "Adapters should not be empty");
        assert_eq!(config.adapters.len(), 1, "Should have exactly one default adapter");
    }

    #[test]
    fn test_notifier_config_default() {
        let config = NotifierConfig::default();
        let new_config = NotifierConfig::new();

        // Default should be equivalent to new()
        assert_eq!(config.store_path, new_config.store_path);
        assert_eq!(config.channel_capacity, new_config.channel_capacity);
        assert_eq!(config.adapters.len(), new_config.adapters.len());
    }

    #[test]
    fn test_default_store_path() {
        let store_path = default_store_path();

        // Verify store path properties
        assert!(!store_path.is_empty(), "Store path should not be empty");
        assert!(store_path.contains("event-notification"), "Store path should contain event-notification");

        // Verify it's a valid path format
        let path = Path::new(&store_path);
        assert!(path.is_absolute() || path.is_relative(), "Store path should be a valid path");

        // Verify it doesn't contain invalid characters
        assert!(!store_path.contains('\0'), "Store path should not contain null characters");

        // Verify it's based on temp directory
        let temp_dir = env::temp_dir();
        let expected_path = temp_dir.join("event-notification");
        assert_eq!(store_path, expected_path.to_string_lossy().to_string());
    }

    #[test]
    fn test_default_channel_capacity() {
        let capacity = default_channel_capacity();

        // Verify capacity is reasonable
        assert_eq!(capacity, 10000, "Default capacity should be 10000");
        assert!(capacity > 0, "Capacity should be positive");
        assert!(capacity >= 1000, "Capacity should be at least 1000 for production use");
        assert!(capacity <= 1_000_000, "Capacity should not be excessively large");
    }

    #[test]
    fn test_notifier_config_serialization() {
        let config = NotifierConfig::new();

        // Test serialization to JSON
        let json_result = serde_json::to_string(&config);
        assert!(json_result.is_ok(), "Config should be serializable to JSON");

        let json_str = json_result.unwrap();
        assert!(!json_str.is_empty(), "Serialized JSON should not be empty");
        assert!(json_str.contains("store_path"), "JSON should contain store_path");
        assert!(json_str.contains("channel_capacity"), "JSON should contain channel_capacity");
        assert!(json_str.contains("adapters"), "JSON should contain adapters");

        // Test deserialization from JSON
        let deserialized_result: Result<NotifierConfig, _> = serde_json::from_str(&json_str);
        assert!(deserialized_result.is_ok(), "Config should be deserializable from JSON");

        let deserialized_config = deserialized_result.unwrap();
        assert_eq!(deserialized_config.store_path, config.store_path);
        assert_eq!(deserialized_config.channel_capacity, config.channel_capacity);
        assert_eq!(deserialized_config.adapters.len(), config.adapters.len());
    }

    #[test]
    fn test_notifier_config_serialization_with_defaults() {
        // Test serialization with minimal JSON (using serde defaults)
        let minimal_json = r#"{"adapters": []}"#;

        let deserialized_result: Result<NotifierConfig, _> = serde_json::from_str(minimal_json);
        assert!(deserialized_result.is_ok(), "Config should deserialize with defaults");

        let config = deserialized_result.unwrap();
        assert_eq!(config.store_path, default_store_path(), "Should use default store path");
        assert_eq!(config.channel_capacity, default_channel_capacity(), "Should use default channel capacity");
        assert!(config.adapters.is_empty(), "Should have empty adapters as specified");
    }

    #[test]
    fn test_notifier_config_debug_format() {
        let config = NotifierConfig::new();

        let debug_str = format!("{:?}", config);
        assert!(!debug_str.is_empty(), "Debug output should not be empty");
        assert!(debug_str.contains("NotifierConfig"), "Debug output should contain struct name");
        assert!(debug_str.contains("store_path"), "Debug output should contain store_path field");
        assert!(
            debug_str.contains("channel_capacity"),
            "Debug output should contain channel_capacity field"
        );
        assert!(debug_str.contains("adapters"), "Debug output should contain adapters field");
    }

    #[test]
    fn test_notifier_config_clone() {
        let config = NotifierConfig::new();
        let cloned_config = config.clone();

        // Test that clone creates an independent copy
        assert_eq!(cloned_config.store_path, config.store_path);
        assert_eq!(cloned_config.channel_capacity, config.channel_capacity);
        assert_eq!(cloned_config.adapters.len(), config.adapters.len());

        // Verify they are independent (modifying one doesn't affect the other)
        let mut modified_config = config.clone();
        modified_config.channel_capacity = 5000;
        assert_ne!(modified_config.channel_capacity, config.channel_capacity);
        assert_eq!(cloned_config.channel_capacity, config.channel_capacity);
    }

    #[test]
    fn test_notifier_config_modification() {
        let mut config = NotifierConfig::new();

        // Test modifying store path
        let original_store_path = config.store_path.clone();
        config.store_path = "/custom/path".to_string();
        assert_ne!(config.store_path, original_store_path);
        assert_eq!(config.store_path, "/custom/path");

        // Test modifying channel capacity
        let original_capacity = config.channel_capacity;
        config.channel_capacity = 5000;
        assert_ne!(config.channel_capacity, original_capacity);
        assert_eq!(config.channel_capacity, 5000);

        // Test modifying adapters
        let original_adapters_len = config.adapters.len();
        config.adapters.push(AdapterConfig::new());
        assert_eq!(config.adapters.len(), original_adapters_len + 1);

        // Test clearing adapters
        config.adapters.clear();
        assert!(config.adapters.is_empty());
    }

    #[test]
    fn test_notifier_config_adapters() {
        let config = NotifierConfig::new();

        // Test default adapter configuration
        assert_eq!(config.adapters.len(), 1, "Should have exactly one default adapter");

        // Test that we can add more adapters
        let mut config_mut = config.clone();
        config_mut.adapters.push(AdapterConfig::new());
        assert_eq!(config_mut.adapters.len(), 2, "Should be able to add more adapters");

        // Test adapter types
        for adapter in &config.adapters {
            match adapter {
                AdapterConfig::Webhook(_) => {
                    // Webhook adapter should be properly configured
                }
                AdapterConfig::Kafka(_) => {
                    // Kafka adapter should be properly configured
                }
                AdapterConfig::Mqtt(_) => {
                    // MQTT adapter should be properly configured
                }
            }
        }
    }

    #[test]
    fn test_notifier_config_edge_cases() {
        // Test with empty adapters
        let mut config = NotifierConfig::new();
        config.adapters.clear();
        assert!(config.adapters.is_empty(), "Adapters should be empty after clearing");

        // Test serialization with empty adapters
        let json_result = serde_json::to_string(&config);
        assert!(json_result.is_ok(), "Config with empty adapters should be serializable");

        // Test with very large channel capacity
        config.channel_capacity = 1_000_000;
        assert_eq!(config.channel_capacity, 1_000_000);

        // Test with minimum channel capacity
        config.channel_capacity = 1;
        assert_eq!(config.channel_capacity, 1);

        // Test with empty store path
        config.store_path = String::new();
        assert!(config.store_path.is_empty());
    }

    #[test]
    fn test_notifier_config_memory_efficiency() {
        let config = NotifierConfig::new();

        // Test that config doesn't use excessive memory
        let config_size = std::mem::size_of_val(&config);
        assert!(config_size < 5000, "Config should not use excessive memory");

        // Test that store path is not excessively long
        assert!(config.store_path.len() < 1000, "Store path should not be excessively long");

        // Test that adapters collection is reasonably sized
        assert!(config.adapters.len() < 100, "Adapters collection should be reasonably sized");
    }

    #[test]
    fn test_notifier_config_consistency() {
        // Create multiple configs and ensure they're consistent
        let config1 = NotifierConfig::new();
        let config2 = NotifierConfig::new();

        // Both configs should have the same default values
        assert_eq!(config1.store_path, config2.store_path);
        assert_eq!(config1.channel_capacity, config2.channel_capacity);
        assert_eq!(config1.adapters.len(), config2.adapters.len());
    }

    #[test]
    fn test_notifier_config_path_validation() {
        let config = NotifierConfig::new();

        // Test that store path is a valid path
        let path = Path::new(&config.store_path);

        // Path should be valid
        assert!(path.components().count() > 0, "Path should have components");

        // Path should not contain invalid characters for most filesystems
        assert!(!config.store_path.contains('\0'), "Path should not contain null characters");
        assert!(!config.store_path.contains('\x01'), "Path should not contain control characters");

        // Path should be reasonable length
        assert!(config.store_path.len() < 260, "Path should be shorter than Windows MAX_PATH");
    }

    #[test]
    fn test_notifier_config_production_readiness() {
        let config = NotifierConfig::new();

        // Test production readiness criteria
        assert!(config.channel_capacity >= 1000, "Channel capacity should be sufficient for production");
        assert!(!config.store_path.is_empty(), "Store path should be configured");
        assert!(!config.adapters.is_empty(), "At least one adapter should be configured");

        // Test that configuration is reasonable for high-load scenarios
        assert!(config.channel_capacity <= 10_000_000, "Channel capacity should not be excessive");

        // Test that store path is in a reasonable location (temp directory)
        assert!(config.store_path.contains("event-notification"), "Store path should be identifiable");
    }

    #[test]
    fn test_default_config_file_constant() {
        // Test that the constant is properly defined
        assert_eq!(DEFAULT_CONFIG_FILE, "event");
        // DEFAULT_CONFIG_FILE is a const, so is_empty() check is redundant
        // assert!(!DEFAULT_CONFIG_FILE.is_empty(), "Config file name should not be empty");
        assert!(!DEFAULT_CONFIG_FILE.contains('/'), "Config file name should not contain path separators");
        assert!(
            !DEFAULT_CONFIG_FILE.contains('\\'),
            "Config file name should not contain Windows path separators"
        );
    }
}
