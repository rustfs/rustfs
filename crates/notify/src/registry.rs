use crate::target::ChannelTargetType;
use crate::{
    error::TargetError,
    factory::{MQTTTargetFactory, TargetFactory, WebhookTargetFactory},
    target::Target,
};
use ecstore::config::{Config, KVS};
use std::collections::HashMap;
use tracing::{error, info};

/// Registry for managing target factories
pub struct TargetRegistry {
    factories: HashMap<String, Box<dyn TargetFactory>>,
}

impl Default for TargetRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl TargetRegistry {
    /// Creates a new TargetRegistry with built-in factories
    pub fn new() -> Self {
        let mut registry = TargetRegistry {
            factories: HashMap::new(),
        };

        // Register built-in factories
        registry.register(ChannelTargetType::Webhook.as_str(), Box::new(WebhookTargetFactory));
        registry.register(ChannelTargetType::Mqtt.as_str(), Box::new(MQTTTargetFactory));

        registry
    }

    /// Registers a new factory for a target type
    pub fn register(&mut self, target_type: &str, factory: Box<dyn TargetFactory>) {
        self.factories.insert(target_type.to_string(), factory);
    }

    /// Creates a target from configuration
    pub async fn create_target(
        &self,
        target_type: &str,
        id: String,
        config: &KVS,
    ) -> Result<Box<dyn Target + Send + Sync>, TargetError> {
        let factory = self
            .factories
            .get(target_type)
            .ok_or_else(|| TargetError::Configuration(format!("Unknown target type: {}", target_type)))?;

        // Validate configuration before creating target
        factory.validate_config(&id, config)?;

        // Create target
        factory.create_target(id, config).await
    }

    /// Creates all targets from a configuration
    pub async fn create_targets_from_config(&self, config: &Config) -> Result<Vec<Box<dyn Target + Send + Sync>>, TargetError> {
        let mut targets: Vec<Box<dyn Target + Send + Sync>> = Vec::new();

        // Iterate through configuration sections
        for (section, subsections) in &config.0 {
            // Only process notification sections
            if !section.starts_with("notify_") {
                continue;
            }

            // Extract target type from section name
            let target_type = section.trim_start_matches("notify_");

            // Iterate through subsections (each representing a target instance)
            for (target_id, target_config) in subsections {
                // Skip disabled targets
                if target_config.lookup("enable").unwrap_or_else(|| "off".to_string()) != "on" {
                    continue;
                }

                // Create target
                match self.create_target(target_type, target_id.clone(), target_config).await {
                    Ok(target) => {
                        info!("Created target: {}/{}", target_type, target_id);
                        targets.push(target);
                    }
                    Err(e) => {
                        error!("Failed to create target {}/{}: {}", target_type, target_id, e);
                    }
                }
            }
        }

        Ok(targets)
    }
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn test_target_registry() {}
}
