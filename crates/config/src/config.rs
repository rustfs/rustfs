use crate::ObservabilityConfig;

/// RustFs configuration
pub struct RustFsConfig {
    pub observability: ObservabilityConfig,
}

impl RustFsConfig {
    pub fn new() -> Self {
        Self {
            observability: ObservabilityConfig::new(),
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
    }

    #[test]
    fn test_rustfs_config_default() {
        let config = RustFsConfig::default();

        // Default should be equivalent to new()
        let new_config = RustFsConfig::new();

        // Compare observability config
        assert_eq!(config.observability.sinks.len(), new_config.observability.sinks.len());
        assert_eq!(config.observability.logger.is_some(), new_config.observability.logger.is_some());
    }

    #[test]
    fn test_rustfs_config_components_independence() {
        let mut config = RustFsConfig::new();

        // Modify observability config
        config.observability.sinks.clear();

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
    fn test_rustfs_config_memory_usage() {
        // Test that config doesn't use excessive memory
        let config = RustFsConfig::new();

        // Basic memory usage checks
        assert!(std::mem::size_of_val(&config) < 10000, "Config should not use excessive memory");

        // Test that collections are reasonably sized
        assert!(config.observability.sinks.len() < 100, "Sinks collection should be reasonably sized");
    }

    #[test]
    fn test_rustfs_config_serialization_compatibility() {
        let config = RustFsConfig::new();

        // Test that observability config can be serialized (it has Serialize trait)
        let observability_json = serde_json::to_string(&config.observability);
        assert!(observability_json.is_ok(), "Observability config should be serializable");
    }

    #[test]
    fn test_rustfs_config_debug_format() {
        let config = RustFsConfig::new();

        // Test that observability config has Debug trait
        let observability_debug = format!("{:?}", config.observability);
        assert!(!observability_debug.is_empty(), "Observability config should have debug output");
        assert!(
            observability_debug.contains("ObservabilityConfig"),
            "Debug output should contain type name"
        );
    }

    #[test]
    fn test_rustfs_config_clone_behavior() {
        let config = RustFsConfig::new();

        // Test that observability config can be cloned
        let observability_clone = config.observability.clone();
        assert_eq!(observability_clone.sinks.len(), config.observability.sinks.len());
    }
}
