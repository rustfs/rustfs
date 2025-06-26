use crate::target::ChannelTargetType;
use crate::{
    error::TargetError,
    factory::{MQTTTargetFactory, TargetFactory, WebhookTargetFactory},
    target::Target,
};
use ecstore::config::{Config, ENABLE_KEY, ENABLE_OFF, ENABLE_ON, KVS};
use rustfs_config::notify::NOTIFY_ROUTE_PREFIX;
use rustfs_config::{DEFAULT_DELIMITER, ENV_PREFIX};
use std::collections::HashMap;
use tracing::{debug, error, info};

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
            if !section.starts_with(NOTIFY_ROUTE_PREFIX) {
                continue;
            }

            // Extract target type from section name
            let target_type = section.trim_start_matches(NOTIFY_ROUTE_PREFIX);

            // Iterate through subsections (each representing a target instance)
            for (target_id, target_config) in subsections {
                // Skip disabled targets

                let enable_from_config = target_config.lookup(ENABLE_KEY).unwrap_or_else(|| ENABLE_OFF.to_string());
                debug!("Target enablement from config: {}/{}: {}", target_type, target_id, enable_from_config);
                // Check environment variable for target enablement example: RUSTFS_NOTIFY_WEBHOOK_ENABLE|RUSTFS_NOTIFY_WEBHOOK_ENABLE_[TARGET_ID]
                let env_key = if target_id == DEFAULT_DELIMITER {
                    // If no specific target ID, use the base target type, example: RUSTFS_NOTIFY_WEBHOOK_ENABLE
                    format!(
                        "{}{}{}{}{}",
                        ENV_PREFIX,
                        NOTIFY_ROUTE_PREFIX,
                        target_type.to_uppercase(),
                        DEFAULT_DELIMITER,
                        ENABLE_KEY
                    )
                } else {
                    // If specific target ID, append it to the key, example: RUSTFS_NOTIFY_WEBHOOK_ENABLE_[TARGET_ID]
                    format!(
                        "{}{}{}{}{}{}{}",
                        ENV_PREFIX,
                        NOTIFY_ROUTE_PREFIX,
                        target_type.to_uppercase(),
                        DEFAULT_DELIMITER,
                        ENABLE_KEY,
                        DEFAULT_DELIMITER,
                        target_id.to_uppercase()
                    )
                }
                .to_uppercase();
                debug!("Target env key: {},Target id: {}", env_key, target_id);
                let enable_from_env = std::env::var(&env_key)
                    .map(|v| v.eq_ignore_ascii_case(ENABLE_ON) || v.eq_ignore_ascii_case("true"))
                    .unwrap_or(false);
                debug!("Target env value: {},key: {},Target id: {}", enable_from_env, env_key, target_id);
                debug!(
                    "Target enablement from env: {}/{}: result: {}",
                    target_type, target_id, enable_from_config
                );
                if enable_from_config != ENABLE_ON && !enable_from_env {
                    info!("Skipping disabled target: {}/{}", target_type, target_id);
                    continue;
                }
                debug!("create target: {}/{} start", target_type, target_id);
                // Create target
                match self.create_target(target_type, target_id.clone(), target_config).await {
                    Ok(target) => {
                        info!("Created target: {}/{}", target_type, target_id);
                        targets.push(target);
                    }
                    Err(e) => {
                        error!("Failed to create target {}/{}: reason: {}", target_type, target_id, e);
                    }
                }
            }
        }

        Ok(targets)
    }
}
