//  Copyright 2024 RustFS Team
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use crate::{
    AuditEntry, AuditError, AuditResult,
    factory::{KafkaTargetFactory, MQTTTargetFactory, NATSTargetFactory, PulsarTargetFactory, TargetFactory, WebhookTargetFactory},
};
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use hashbrown::HashMap;
use rustfs_config::audit::AUDIT_ROUTE_PREFIX;
use rustfs_ecstore::config::{Config, KVS};
use rustfs_targets::arn::TargetID;
use rustfs_targets::{Target, TargetError, config::collect_target_configs, target::ChannelTargetType};
use std::sync::Arc;
use tracing::{error, info};

/// Registry for managing audit targets
pub struct AuditRegistry {
    /// Storage for created targets
    targets: HashMap<String, Box<dyn Target<AuditEntry> + Send + Sync>>,
    /// Factories for creating targets
    factories: HashMap<String, Box<dyn TargetFactory>>,
}

impl Default for AuditRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl AuditRegistry {
    /// Creates a new AuditRegistry
    pub fn new() -> Self {
        let mut registry = AuditRegistry {
            factories: HashMap::new(),
            targets: HashMap::new(),
        };

        // Register built-in factories
        registry.register(ChannelTargetType::Webhook.as_str(), Box::new(WebhookTargetFactory));
        registry.register(ChannelTargetType::Mqtt.as_str(), Box::new(MQTTTargetFactory));
        registry.register(ChannelTargetType::Nats.as_str(), Box::new(NATSTargetFactory));
        registry.register(ChannelTargetType::Pulsar.as_str(), Box::new(PulsarTargetFactory));
        registry.register(ChannelTargetType::Kafka.as_str(), Box::new(KafkaTargetFactory));

        registry
    }

    /// Registers a new factory for a target type
    ///
    /// # Arguments
    /// * `target_type` - The type of the target (e.g., "webhook", "mqtt").
    /// * `factory` - The factory instance to create targets of this type.
    pub fn register(&mut self, target_type: &str, factory: Box<dyn TargetFactory>) {
        self.factories.insert(target_type.to_string(), factory);
    }

    /// Creates a target of the specified type with the given ID and configuration
    ///
    /// # Arguments
    /// * `target_type` - The type of the target (e.g., "webhook", "mqtt").
    /// * `id` - The identifier for the target instance.
    /// * `config` - The configuration key-value store for the target.
    ///
    /// # Returns
    /// * `Result<Box<dyn Target<AuditEntry> + Send + Sync>, TargetError>` - The created target or an error.
    pub async fn create_target(
        &self,
        target_type: &str,
        id: String,
        config: &KVS,
    ) -> Result<Box<dyn Target<AuditEntry> + Send + Sync>, TargetError> {
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
    pub async fn create_audit_targets_from_config(
        &self,
        config: &Config,
    ) -> AuditResult<Vec<Box<dyn Target<AuditEntry> + Send + Sync>>> {
        let mut tasks = FuturesUnordered::new();
        for (target_type, factory) in &self.factories {
            tracing::Span::current().record("target_type", target_type.as_str());
            info!("Start working on target types...");
            let valid_fields = factory.get_valid_fields();
            for (id, merged_config) in collect_target_configs(config, AUDIT_ROUTE_PREFIX, target_type, &valid_fields) {
                info!(instance_id = %id, "Target is enabled, ready to create a task");
                let tid = id.clone();
                let merged_config_arc = Arc::new(merged_config);
                tasks.push(async move {
                    let result = factory.create_target(tid.clone(), &merged_config_arc).await;
                    (tid, result)
                });
            }
        }

        let mut successful_targets = Vec::new();
        while let Some((id, result)) = tasks.next().await {
            match result {
                Ok(target) => {
                    info!(target_type = %target.id().name, instance_id = %id, "Create a target successfully");
                    successful_targets.push(target);
                }
                Err(e) => {
                    error!(instance_id = %id, error = %e, "Failed to create a target");
                }
            }
        }

        info!(count = successful_targets.len(), "All target processing completed");
        Ok(successful_targets)
    }

    /// Adds a target to the registry
    ///
    /// # Arguments
    /// * `id` - The identifier for the target.
    /// * `target` - The target instance to be added.
    pub fn add_target(&mut self, id: String, target: Box<dyn Target<AuditEntry> + Send + Sync>) {
        self.targets.insert(id, target);
    }

    /// Removes a target from the registry
    ///
    /// # Arguments
    /// * `id` - The identifier for the target to be removed.
    ///
    /// # Returns
    /// * `Option<Box<dyn Target<AuditEntry> + Send + Sync>>` - The removed target if it existed.
    pub fn remove_target(&mut self, id: &str) -> Option<Box<dyn Target<AuditEntry> + Send + Sync>> {
        self.targets.remove(id)
    }

    /// Gets a target from the registry
    ///
    /// # Arguments
    /// * `id` - The identifier for the target to be retrieved.
    ///
    /// # Returns
    /// * `Option<&(dyn Target<AuditEntry> + Send + Sync)>` - The target if it exists.
    pub fn get_target(&self, id: &str) -> Option<&(dyn Target<AuditEntry> + Send + Sync)> {
        self.targets.get(id).map(|t| t.as_ref())
    }

    /// Lists cloned target values for runtime inspection without exposing mutable registry access.
    pub fn list_target_values(&self) -> Vec<Box<dyn Target<AuditEntry> + Send + Sync>> {
        self.targets.values().map(|target| target.clone_dyn()).collect()
    }

    /// Lists all target IDs
    ///
    /// # Returns
    /// * `Vec<String>` - A vector of all target IDs in the registry.
    pub fn list_targets(&self) -> Vec<String> {
        self.targets.keys().cloned().collect()
    }

    /// Closes all targets and clears the registry
    ///
    /// # Returns
    /// * `AuditResult<()>` - Result indicating success or failure.
    pub async fn close_all(&mut self) -> AuditResult<()> {
        let mut errors = Vec::new();

        for (id, target) in self.targets.drain() {
            if let Err(e) = target.close().await {
                error!(target_id = %id, error = %e, "Failed to close audit target");
                errors.push(e);
            }
        }

        if !errors.is_empty() {
            return Err(AuditError::Target(errors.into_iter().next().unwrap()));
        }

        Ok(())
    }

    /// Creates a unique key for a target based on its type and ID
    ///
    /// # Arguments
    /// * `target_type` - The type of the target (e.g., "webhook", "mqtt").
    /// * `target_id` - The identifier for the target instance.
    ///
    /// # Returns
    /// * `String` - The unique key for the target.
    pub fn create_key(&self, target_type: &str, target_id: &str) -> String {
        let key = TargetID::new(target_id.to_string(), target_type.to_string());
        info!(target_type = %target_type, "Create key for {}", key);
        key.to_string()
    }

    /// Enables a target (placeholder, assumes target exists)
    ///
    /// # Arguments
    /// * `target_type` - The type of the target (e.g., "webhook", "mqtt").
    /// * `target_id` - The identifier for the target instance.
    ///
    /// # Returns
    /// * `AuditResult<()>` - Result indicating success or failure.
    pub fn enable_target(&self, target_type: &str, target_id: &str) -> AuditResult<()> {
        let key = self.create_key(target_type, target_id);
        if self.get_target(&key).is_some() {
            info!("Target {}-{} enabled", target_type, target_id);
            Ok(())
        } else {
            Err(AuditError::Configuration(
                format!("Target not found: {}-{}", target_type, target_id),
                None,
            ))
        }
    }

    /// Disables a target (placeholder, assumes target exists)
    ///
    /// # Arguments
    /// * `target_type` - The type of the target (e.g., "webhook", "mqtt").
    /// * `target_id` - The identifier for the target instance.
    ///
    /// # Returns
    /// * `AuditResult<()>` - Result indicating success or failure.
    pub fn disable_target(&self, target_type: &str, target_id: &str) -> AuditResult<()> {
        let key = self.create_key(target_type, target_id);
        if self.get_target(&key).is_some() {
            info!("Target {}-{} disabled", target_type, target_id);
            Ok(())
        } else {
            Err(AuditError::Configuration(
                format!("Target not found: {}-{}", target_type, target_id),
                None,
            ))
        }
    }

    /// Upserts a target into the registry
    ///
    /// # Arguments
    /// * `target_type` - The type of the target (e.g., "webhook", "mqtt").
    /// * `target_id` - The identifier for the target instance.
    /// * `target` - The target instance to be upserted.
    ///
    /// # Returns
    /// * `AuditResult<()>` - Result indicating success or failure.
    pub fn upsert_target(
        &mut self,
        target_type: &str,
        target_id: &str,
        target: Box<dyn Target<AuditEntry> + Send + Sync>,
    ) -> AuditResult<()> {
        let key = self.create_key(target_type, target_id);
        self.targets.insert(key, target);
        Ok(())
    }
}
