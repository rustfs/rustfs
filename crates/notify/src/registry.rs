// Copyright 2024 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::Event;
use crate::factory::{MQTTTargetFactory, TargetFactory, WebhookTargetFactory};
use futures::stream::{FuturesUnordered, StreamExt};
use rustfs_config::notify::NOTIFY_ROUTE_PREFIX;
use rustfs_config::{DEFAULT_DELIMITER, ENABLE_KEY, ENV_PREFIX};
use rustfs_ecstore::config::{Config, KVS};
use rustfs_targets::Target;
use rustfs_targets::TargetError;
use rustfs_targets::target::ChannelTargetType;
use std::collections::{HashMap, HashSet};
use tracing::{debug, error, info, warn};

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
    ) -> Result<Box<dyn Target<Event> + Send + Sync>, TargetError> {
        let factory = self
            .factories
            .get(target_type)
            .ok_or_else(|| TargetError::Configuration(format!("Unknown target type: {target_type}")))?;

        // Validate configuration before creating target
        factory.validate_config(&id, config)?;

        // Create target
        factory.create_target(id, config).await
    }

    /// Creates all targets from a configuration
    /// Create all notification targets from system configuration and environment variables.
    /// This method processes the creation of each target concurrently as follows:
    /// 1. Iterate through all registered target types (e.g. webhooks, mqtt).
    /// 2. For each type, resolve its configuration in the configuration file and environment variables.
    /// 3. Identify all target instance IDs that need to be created.
    /// 4. Combine the default configuration, file configuration, and environment variable configuration for each instance.
    /// 5. If the instance is enabled, create an asynchronous task for it to instantiate.
    /// 6. Concurrency executes all creation tasks and collects results.
    pub async fn create_targets_from_config(
        &self,
        config: &Config,
    ) -> Result<Vec<Box<dyn Target<Event> + Send + Sync>>, TargetError> {
        // Collect only environment variables with the relevant prefix to reduce memory usage
        let all_env: Vec<(String, String)> = std::env::vars().filter(|(key, _)| key.starts_with(ENV_PREFIX)).collect();
        // A collection of asynchronous tasks for concurrently executing target creation
        let mut tasks = FuturesUnordered::new();
        let mut final_config = config.clone(); // Clone a configuration for aggregating the final result
        // 1. Traverse all registered plants and process them by target type
        for (target_type, factory) in &self.factories {
            tracing::Span::current().record("target_type", target_type.as_str());
            info!("Start working on target types...");

            // 2. Prepare the configuration source
            // 2.1. Get the configuration segment in the file, e.g. 'notify_webhook'
            let section_name = format!("{NOTIFY_ROUTE_PREFIX}{target_type}");
            let file_configs = config.0.get(&section_name).cloned().unwrap_or_default();
            // 2.2. Get the default configuration for that type
            let default_cfg = file_configs.get(DEFAULT_DELIMITER).cloned().unwrap_or_default();
            debug!(?default_cfg, "Get the default configuration");

            // *** Optimization point 1: Get all legitimate fields of the current target type ***
            let valid_fields = factory.get_valid_fields();
            debug!(?valid_fields, "Get the legitimate configuration fields");

            // 3. Resolve instance IDs and configuration overrides from environment variables
            let mut instance_ids_from_env = HashSet::new();
            // 3.1. Instance discovery: Based on the '..._ENABLE_INSTANCEID' format
            let enable_prefix = format!("{ENV_PREFIX}{NOTIFY_ROUTE_PREFIX}{target_type}_{ENABLE_KEY}_").to_uppercase();
            for (key, value) in &all_env {
                if value.eq_ignore_ascii_case(rustfs_config::EnableState::One.as_str())
                    || value.eq_ignore_ascii_case(rustfs_config::EnableState::On.as_str())
                    || value.eq_ignore_ascii_case(rustfs_config::EnableState::True.as_str())
                    || value.eq_ignore_ascii_case(rustfs_config::EnableState::Yes.as_str())
                {
                    if let Some(id) = key.strip_prefix(&enable_prefix) {
                        if !id.is_empty() {
                            instance_ids_from_env.insert(id.to_lowercase());
                        }
                    }
                }
            }

            // 3.2. Parse all relevant environment variable configurations
            // 3.2.1. Build environment variable prefixes such as 'RUSTFS_NOTIFY_WEBHOOK_'
            let env_prefix = format!("{ENV_PREFIX}{NOTIFY_ROUTE_PREFIX}{target_type}_").to_uppercase();
            // 3.2.2. 'env_overrides' is used to store configurations parsed from environment variables in the format: {instance id -> {field -> value}}
            let mut env_overrides: HashMap<String, HashMap<String, String>> = HashMap::new();
            for (key, value) in &all_env {
                if let Some(rest) = key.strip_prefix(&env_prefix) {
                    // Use rsplitn to split from the right side to properly extract the INSTANCE_ID at the end
                    // Format: <FIELD_NAME>_<INSTANCE_ID> or <FIELD_NAME>
                    let mut parts = rest.rsplitn(2, '_');

                    // The first part from the right is INSTANCE_ID
                    let instance_id_part = parts.next().unwrap_or(DEFAULT_DELIMITER);
                    // The remaining part is FIELD_NAME
                    let field_name_part = parts.next();

                    let (field_name, instance_id) = match field_name_part {
                        // Case 1: The format is <FIELD_NAME>_<INSTANCE_ID>
                        // e.g., rest = "ENDPOINT_PRIMARY" -> field_name="ENDPOINT", instance_id="PRIMARY"
                        Some(field) => (field.to_lowercase(), instance_id_part.to_lowercase()),
                        // Case 2: The format is <FIELD_NAME> (无 INSTANCE_ID)
                        // e.g., rest = "ENABLE" -> field_name="ENABLE", instance_id="" (Universal configuration `_ DEFAULT_DELIMITER`)
                        None => (instance_id_part.to_lowercase(), DEFAULT_DELIMITER.to_string()),
                    };

                    // *** Optimization point 2: Verify whether the parsed field_name is legal ***
                    if !field_name.is_empty() && valid_fields.contains(&field_name) {
                        debug!(
                            instance_id = %if instance_id.is_empty() { DEFAULT_DELIMITER } else { &instance_id },
                            %field_name,
                            %value,
                            "Parsing to environment variables"
                        );
                        env_overrides
                            .entry(instance_id)
                            .or_default()
                            .insert(field_name, value.clone());
                    } else {
                        // Ignore illegal field names
                        warn!(
                            field_name = %field_name,
                            "Ignore environment variable fields, not found in the list of valid fields for target type {}",
                            target_type
                        );
                    }
                }
            }
            debug!(?env_overrides, "Complete the environment variable analysis");

            // 4. Determine all instance IDs that need to be processed
            let mut all_instance_ids: HashSet<String> =
                file_configs.keys().filter(|k| *k != DEFAULT_DELIMITER).cloned().collect();
            all_instance_ids.extend(instance_ids_from_env);
            debug!(?all_instance_ids, "Determine all instance IDs");

            // 5. Merge configurations and create tasks for each instance
            for id in all_instance_ids {
                // 5.1. Merge configuration, priority: Environment variables > File instance configuration > File default configuration
                let mut merged_config = default_cfg.clone();
                // Instance-specific configuration in application files
                if let Some(file_instance_cfg) = file_configs.get(&id) {
                    merged_config.extend(file_instance_cfg.clone());
                }
                // Application instance-specific environment variable configuration
                if let Some(env_instance_cfg) = env_overrides.get(&id) {
                    // Convert HashMap<String, String> to KVS
                    let mut kvs_from_env = KVS::new();
                    for (k, v) in env_instance_cfg {
                        kvs_from_env.insert(k.clone(), v.clone());
                    }
                    merged_config.extend(kvs_from_env);
                }
                debug!(instance_id = %id, ?merged_config, "Complete configuration merge");

                // 5.2. Check if the instance is enabled
                let enabled = merged_config
                    .lookup(ENABLE_KEY)
                    .map(|v| {
                        v.eq_ignore_ascii_case(rustfs_config::EnableState::One.as_str())
                            || v.eq_ignore_ascii_case(rustfs_config::EnableState::On.as_str())
                            || v.eq_ignore_ascii_case(rustfs_config::EnableState::True.as_str())
                            || v.eq_ignore_ascii_case(rustfs_config::EnableState::Yes.as_str())
                    })
                    .unwrap_or(false);

                if enabled {
                    info!(instance_id = %id, "Target is enabled, ready to create a task");
                    // 5.3. Create asynchronous tasks for enabled instances
                    let target_type_clone = target_type.clone();
                    let tid = id.clone();
                    let merged_config_arc = std::sync::Arc::new(merged_config);
                    tasks.push(async move {
                        let result = factory.create_target(tid.clone(), &merged_config_arc).await;
                        (target_type_clone, tid, result, std::sync::Arc::clone(&merged_config_arc))
                    });
                } else {
                    info!(instance_id = %id, "Skip the disabled target and will be removed from the final configuration");
                    // Remove disabled target from final configuration
                    final_config.0.entry(section_name.clone()).or_default().remove(&id);
                }
            }
        }

        // 6. Concurrently execute all creation tasks and collect results
        let mut successful_targets = Vec::new();
        let mut successful_configs = Vec::new();
        while let Some((target_type, id, result, final_config)) = tasks.next().await {
            match result {
                Ok(target) => {
                    info!(target_type = %target_type, instance_id = %id, "Create a target successfully");
                    successful_targets.push(target);
                    successful_configs.push((target_type, id, final_config));
                }
                Err(e) => {
                    error!(target_type = %target_type, instance_id = %id, error = %e, "Failed to create a target");
                }
            }
        }

        // 7. Aggregate new configuration and write back to system configuration
        if !successful_configs.is_empty() {
            info!(
                "Prepare to update {} successfully created target configurations to the system configuration...",
                successful_configs.len()
            );
            let mut new_config = config.clone();
            for (target_type, id, kvs) in successful_configs {
                let section_name = format!("{NOTIFY_ROUTE_PREFIX}{target_type}").to_lowercase();
                new_config.0.entry(section_name).or_default().insert(id, (*kvs).clone());
            }

            let Some(store) = rustfs_ecstore::global::new_object_layer_fn() else {
                return Err(TargetError::ServerNotInitialized(
                    "Failed to save target configuration: server storage not initialized".to_string(),
                ));
            };

            match rustfs_ecstore::config::com::save_server_config(store, &new_config).await {
                Ok(_) => {
                    info!("The new configuration was saved to the system successfully.")
                }
                Err(e) => {
                    error!("Failed to save the new configuration: {}", e);
                    return Err(TargetError::SaveConfig(e.to_string()));
                }
            }
        }

        info!(count = successful_targets.len(), "All target processing completed");
        Ok(successful_targets)
    }
}
