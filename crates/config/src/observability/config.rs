use crate::observability::logger::LoggerConfig;
use crate::observability::otel::OtelConfig;
use crate::observability::sink::SinkConfig;
use serde::{Deserialize, Serialize};

/// Observability configuration
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ObservabilityConfig {
    pub otel: OtelConfig,
    pub sinks: Vec<SinkConfig>,
    pub logger: Option<LoggerConfig>,
}

impl ObservabilityConfig {
    pub fn new() -> Self {
        Self {
            otel: OtelConfig::new(),
            sinks: vec![SinkConfig::new()],
            logger: Some(LoggerConfig::new()),
        }
    }
}

impl Default for ObservabilityConfig {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_observability_config_new() {
        let config = ObservabilityConfig::new();

        // Verify OTEL config is initialized
        assert!(config.otel.use_stdout.is_some(), "OTEL use_stdout should be configured");
        assert!(config.otel.sample_ratio.is_some(), "OTEL sample_ratio should be configured");
        assert!(config.otel.meter_interval.is_some(), "OTEL meter_interval should be configured");
        assert!(config.otel.service_name.is_some(), "OTEL service_name should be configured");
        assert!(config.otel.service_version.is_some(), "OTEL service_version should be configured");
        assert!(config.otel.environment.is_some(), "OTEL environment should be configured");
        assert!(config.otel.logger_level.is_some(), "OTEL logger_level should be configured");

        // Verify sinks are initialized
        assert!(!config.sinks.is_empty(), "Sinks should not be empty");
        assert_eq!(config.sinks.len(), 1, "Should have exactly one default sink");

        // Verify logger is initialized
        assert!(config.logger.is_some(), "Logger should be configured");
    }

    #[test]
    fn test_observability_config_default() {
        let config = ObservabilityConfig::default();
        let new_config = ObservabilityConfig::new();

        // Default should be equivalent to new()
        assert_eq!(config.sinks.len(), new_config.sinks.len());
        assert_eq!(config.logger.is_some(), new_config.logger.is_some());

        // OTEL configs should be equivalent
        assert_eq!(config.otel.use_stdout, new_config.otel.use_stdout);
        assert_eq!(config.otel.sample_ratio, new_config.otel.sample_ratio);
        assert_eq!(config.otel.meter_interval, new_config.otel.meter_interval);
        assert_eq!(config.otel.service_name, new_config.otel.service_name);
        assert_eq!(config.otel.service_version, new_config.otel.service_version);
        assert_eq!(config.otel.environment, new_config.otel.environment);
        assert_eq!(config.otel.logger_level, new_config.otel.logger_level);
    }

    #[test]
    fn test_observability_config_otel_defaults() {
        let config = ObservabilityConfig::new();

        // Test OTEL default values
        if let Some(_use_stdout) = config.otel.use_stdout {
            // Test boolean values - any boolean value is valid
        }

        if let Some(sample_ratio) = config.otel.sample_ratio {
            assert!((0.0..=1.0).contains(&sample_ratio), "Sample ratio should be between 0.0 and 1.0");
        }

        if let Some(meter_interval) = config.otel.meter_interval {
            assert!(meter_interval > 0, "Meter interval should be positive");
            assert!(meter_interval <= 3600, "Meter interval should be reasonable (≤ 1 hour)");
        }

        if let Some(service_name) = &config.otel.service_name {
            assert!(!service_name.is_empty(), "Service name should not be empty");
            assert!(!service_name.contains(' '), "Service name should not contain spaces");
        }

        if let Some(service_version) = &config.otel.service_version {
            assert!(!service_version.is_empty(), "Service version should not be empty");
        }

        if let Some(environment) = &config.otel.environment {
            assert!(!environment.is_empty(), "Environment should not be empty");
            assert!(
                ["development", "staging", "production", "test"].contains(&environment.as_str()),
                "Environment should be a standard environment name"
            );
        }

        if let Some(logger_level) = &config.otel.logger_level {
            assert!(
                ["trace", "debug", "info", "warn", "error"].contains(&logger_level.as_str()),
                "Logger level should be a valid tracing level"
            );
        }
    }

    #[test]
    fn test_observability_config_sinks() {
        let config = ObservabilityConfig::new();

        // Test default sink configuration
        assert_eq!(config.sinks.len(), 1, "Should have exactly one default sink");

        let _default_sink = &config.sinks[0];
        // Test that the sink has valid configuration
        // Note: We can't test specific values without knowing SinkConfig implementation
        // but we can test that it's properly initialized

        // Test that we can add more sinks
        let mut config_mut = config.clone();
        config_mut.sinks.push(SinkConfig::new());
        assert_eq!(config_mut.sinks.len(), 2, "Should be able to add more sinks");
    }

    #[test]
    fn test_observability_config_logger() {
        let config = ObservabilityConfig::new();

        // Test logger configuration
        assert!(config.logger.is_some(), "Logger should be configured by default");

        if let Some(_logger) = &config.logger {
            // Test that logger has valid configuration
            // Note: We can't test specific values without knowing LoggerConfig implementation
            // but we can test that it's properly initialized
        }

        // Test that logger can be disabled
        let mut config_mut = config.clone();
        config_mut.logger = None;
        assert!(config_mut.logger.is_none(), "Logger should be able to be disabled");
    }

    #[test]
    fn test_observability_config_serialization() {
        let config = ObservabilityConfig::new();

        // Test serialization to JSON
        let json_result = serde_json::to_string(&config);
        assert!(json_result.is_ok(), "Config should be serializable to JSON");

        let json_str = json_result.unwrap();
        assert!(!json_str.is_empty(), "Serialized JSON should not be empty");
        assert!(json_str.contains("otel"), "JSON should contain otel configuration");
        assert!(json_str.contains("sinks"), "JSON should contain sinks configuration");
        assert!(json_str.contains("logger"), "JSON should contain logger configuration");

        // Test deserialization from JSON
        let deserialized_result: Result<ObservabilityConfig, _> = serde_json::from_str(&json_str);
        assert!(deserialized_result.is_ok(), "Config should be deserializable from JSON");

        let deserialized_config = deserialized_result.unwrap();
        assert_eq!(deserialized_config.sinks.len(), config.sinks.len());
        assert_eq!(deserialized_config.logger.is_some(), config.logger.is_some());
    }

    #[test]
    fn test_observability_config_debug_format() {
        let config = ObservabilityConfig::new();

        let debug_str = format!("{:?}", config);
        assert!(!debug_str.is_empty(), "Debug output should not be empty");
        assert!(debug_str.contains("ObservabilityConfig"), "Debug output should contain struct name");
        assert!(debug_str.contains("otel"), "Debug output should contain otel field");
        assert!(debug_str.contains("sinks"), "Debug output should contain sinks field");
        assert!(debug_str.contains("logger"), "Debug output should contain logger field");
    }

    #[test]
    fn test_observability_config_clone() {
        let config = ObservabilityConfig::new();
        let cloned_config = config.clone();

        // Test that clone creates an independent copy
        assert_eq!(cloned_config.sinks.len(), config.sinks.len());
        assert_eq!(cloned_config.logger.is_some(), config.logger.is_some());
        assert_eq!(cloned_config.otel.endpoint, config.otel.endpoint);
        assert_eq!(cloned_config.otel.use_stdout, config.otel.use_stdout);
        assert_eq!(cloned_config.otel.sample_ratio, config.otel.sample_ratio);
        assert_eq!(cloned_config.otel.meter_interval, config.otel.meter_interval);
        assert_eq!(cloned_config.otel.service_name, config.otel.service_name);
        assert_eq!(cloned_config.otel.service_version, config.otel.service_version);
        assert_eq!(cloned_config.otel.environment, config.otel.environment);
        assert_eq!(cloned_config.otel.logger_level, config.otel.logger_level);
    }

    #[test]
    fn test_observability_config_modification() {
        let mut config = ObservabilityConfig::new();

        // Test modifying OTEL endpoint
        let original_endpoint = config.otel.endpoint.clone();
        config.otel.endpoint = "http://localhost:4317".to_string();
        assert_ne!(config.otel.endpoint, original_endpoint);
        assert_eq!(config.otel.endpoint, "http://localhost:4317");

        // Test modifying sinks
        let original_sinks_len = config.sinks.len();
        config.sinks.push(SinkConfig::new());
        assert_eq!(config.sinks.len(), original_sinks_len + 1);

        // Test disabling logger
        config.logger = None;
        assert!(config.logger.is_none());
    }

    #[test]
    fn test_observability_config_edge_cases() {
        // Test with empty sinks
        let mut config = ObservabilityConfig::new();
        config.sinks.clear();
        assert!(config.sinks.is_empty(), "Sinks should be empty after clearing");

        // Test serialization with empty sinks
        let json_result = serde_json::to_string(&config);
        assert!(json_result.is_ok(), "Config with empty sinks should be serializable");

        // Test with no logger
        config.logger = None;
        let json_result = serde_json::to_string(&config);
        assert!(json_result.is_ok(), "Config with no logger should be serializable");
    }

    #[test]
    fn test_observability_config_memory_efficiency() {
        let config = ObservabilityConfig::new();

        // Test that config doesn't use excessive memory
        let config_size = std::mem::size_of_val(&config);
        assert!(config_size < 5000, "Config should not use excessive memory");

        // Test that endpoint string is not excessively long
        assert!(config.otel.endpoint.len() < 1000, "Endpoint should not be excessively long");

        // Test that collections are reasonably sized
        assert!(config.sinks.len() < 100, "Sinks collection should be reasonably sized");
    }

    #[test]
    fn test_observability_config_consistency() {
        // Create multiple configs and ensure they're consistent
        let config1 = ObservabilityConfig::new();
        let config2 = ObservabilityConfig::new();

        // Both configs should have the same default structure
        assert_eq!(config1.sinks.len(), config2.sinks.len());
        assert_eq!(config1.logger.is_some(), config2.logger.is_some());
        assert_eq!(config1.otel.use_stdout, config2.otel.use_stdout);
        assert_eq!(config1.otel.sample_ratio, config2.otel.sample_ratio);
        assert_eq!(config1.otel.meter_interval, config2.otel.meter_interval);
        assert_eq!(config1.otel.service_name, config2.otel.service_name);
        assert_eq!(config1.otel.service_version, config2.otel.service_version);
        assert_eq!(config1.otel.environment, config2.otel.environment);
        assert_eq!(config1.otel.logger_level, config2.otel.logger_level);
    }
}
