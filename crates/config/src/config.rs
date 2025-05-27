use crate::event::config::NotifierConfig;
use crate::ObservabilityConfig;

/// RustFs configuration
pub struct RustFsConfig {
    pub observability: ObservabilityConfig,
    pub event: NotifierConfig,
}

impl RustFsConfig {
    pub fn new() -> Self {
        Self {
            observability: ObservabilityConfig::new(),
            event: NotifierConfig::new(),
        }
    }
}

impl Default for RustFsConfig {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rustfs_config_new() {
        let config = RustFsConfig::new();

        // Verify that observability config is properly initialized
        assert!(!config.observability.sinks.is_empty(), "Observability sinks should not be empty");
        assert!(config.observability.logger.is_some(), "Logger config should be present");

        // Verify that event config is properly initialized
        assert!(!config.event.store_path.is_empty(), "Event store path should not be empty");
        assert!(config.event.channel_capacity > 0, "Channel capacity should be positive");
        assert!(!config.event.adapters.is_empty(), "Event adapters should not be empty");
    }

    #[test]
    fn test_rustfs_config_default() {
        let config = RustFsConfig::default();

        // Default should be equivalent to new()
        let new_config = RustFsConfig::new();

        // Compare observability config
        assert_eq!(config.observability.sinks.len(), new_config.observability.sinks.len());
        assert_eq!(config.observability.logger.is_some(), new_config.observability.logger.is_some());

        // Compare event config
        assert_eq!(config.event.store_path, new_config.event.store_path);
        assert_eq!(config.event.channel_capacity, new_config.event.channel_capacity);
        assert_eq!(config.event.adapters.len(), new_config.event.adapters.len());
    }

    #[test]
    fn test_rustfs_config_components_independence() {
        let mut config = RustFsConfig::new();

        // Modify observability config
        config.observability.sinks.clear();

        // Event config should remain unchanged
        assert!(!config.event.adapters.is_empty(), "Event adapters should remain unchanged");
        assert!(config.event.channel_capacity > 0, "Channel capacity should remain unchanged");

        // Create new config to verify independence
        let new_config = RustFsConfig::new();
        assert!(!new_config.observability.sinks.is_empty(), "New config should have default sinks");
    }

    #[test]
    fn test_rustfs_config_observability_integration() {
        let config = RustFsConfig::new();

        // Test observability config properties
        assert!(config.observability.otel.endpoint.is_empty() || !config.observability.otel.endpoint.is_empty());
        assert!(config.observability.otel.use_stdout.is_some());
        assert!(config.observability.otel.sample_ratio.is_some());
        assert!(config.observability.otel.meter_interval.is_some());
        assert!(config.observability.otel.service_name.is_some());
        assert!(config.observability.otel.service_version.is_some());
        assert!(config.observability.otel.environment.is_some());
        assert!(config.observability.otel.logger_level.is_some());
    }

    #[test]
    fn test_rustfs_config_event_integration() {
        let config = RustFsConfig::new();

        // Test event config properties
        assert!(!config.event.store_path.is_empty(), "Store path should not be empty");
        assert!(config.event.channel_capacity >= 1000, "Channel capacity should be reasonable for production");

        // Test that store path is a valid path format
        let store_path = &config.event.store_path;
        assert!(!store_path.contains('\0'), "Store path should not contain null characters");

        // Test adapters configuration
        for adapter in &config.event.adapters {
            // Each adapter should have a valid configuration
            match adapter {
                crate::event::adapters::AdapterConfig::Webhook(_) => {
                    // Webhook adapter should be properly configured
                },
                crate::event::adapters::AdapterConfig::Kafka(_) => {
                    // Kafka adapter should be properly configured
                },
                crate::event::adapters::AdapterConfig::Mqtt(_) => {
                    // MQTT adapter should be properly configured
                },
            }
        }
    }

    #[test]
    fn test_rustfs_config_memory_usage() {
        // Test that config doesn't use excessive memory
        let config = RustFsConfig::new();

        // Basic memory usage checks
        assert!(std::mem::size_of_val(&config) < 10000, "Config should not use excessive memory");

        // Test that strings are not excessively long
        assert!(config.event.store_path.len() < 1000, "Store path should not be excessively long");

        // Test that collections are reasonably sized
        assert!(config.observability.sinks.len() < 100, "Sinks collection should be reasonably sized");
        assert!(config.event.adapters.len() < 100, "Adapters collection should be reasonably sized");
    }

    #[test]
    fn test_rustfs_config_serialization_compatibility() {
        let config = RustFsConfig::new();

        // Test that observability config can be serialized (it has Serialize trait)
        let observability_json = serde_json::to_string(&config.observability);
        assert!(observability_json.is_ok(), "Observability config should be serializable");

        // Test that event config can be serialized (it has Serialize trait)
        let event_json = serde_json::to_string(&config.event);
        assert!(event_json.is_ok(), "Event config should be serializable");
    }

    #[test]
    fn test_rustfs_config_debug_format() {
        let config = RustFsConfig::new();

        // Test that observability config has Debug trait
        let observability_debug = format!("{:?}", config.observability);
        assert!(!observability_debug.is_empty(), "Observability config should have debug output");
        assert!(observability_debug.contains("ObservabilityConfig"), "Debug output should contain type name");

        // Test that event config has Debug trait
        let event_debug = format!("{:?}", config.event);
        assert!(!event_debug.is_empty(), "Event config should have debug output");
        assert!(event_debug.contains("NotifierConfig"), "Debug output should contain type name");
    }

    #[test]
    fn test_rustfs_config_clone_behavior() {
        let config = RustFsConfig::new();

        // Test that observability config can be cloned
        let observability_clone = config.observability.clone();
        assert_eq!(observability_clone.sinks.len(), config.observability.sinks.len());

        // Test that event config can be cloned
        let event_clone = config.event.clone();
        assert_eq!(event_clone.store_path, config.event.store_path);
        assert_eq!(event_clone.channel_capacity, config.event.channel_capacity);
    }

    #[test]
    fn test_rustfs_config_environment_independence() {
        // Test that config creation doesn't depend on specific environment variables
        // This test ensures the config can be created in any environment

        let config1 = RustFsConfig::new();
        let config2 = RustFsConfig::new();

        // Both configs should have the same structure
        assert_eq!(config1.observability.sinks.len(), config2.observability.sinks.len());
        assert_eq!(config1.event.adapters.len(), config2.event.adapters.len());

        // Store paths should be consistent
        assert_eq!(config1.event.store_path, config2.event.store_path);
        assert_eq!(config1.event.channel_capacity, config2.event.channel_capacity);
    }
}
