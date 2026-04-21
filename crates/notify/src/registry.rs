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
use crate::factory::{KafkaTargetFactory, MQTTTargetFactory, NATSTargetFactory, PulsarTargetFactory, TargetFactory, WebhookTargetFactory};
use futures::stream::{FuturesUnordered, StreamExt};
use hashbrown::HashMap;
use rustfs_config::notify::NOTIFY_ROUTE_PREFIX;
use rustfs_ecstore::config::{Config, KVS};
use rustfs_targets::{Target, TargetError, config::collect_target_configs, target::ChannelTargetType};
use std::sync::Arc;
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
        registry.register(ChannelTargetType::Nats.as_str(), Box::new(NATSTargetFactory));
        registry.register(ChannelTargetType::Pulsar.as_str(), Box::new(PulsarTargetFactory));
        registry.register(ChannelTargetType::Kafka.as_str(), Box::new(KafkaTargetFactory));

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
        let mut tasks = FuturesUnordered::new();
        for (target_type, factory) in &self.factories {
            tracing::Span::current().record("target_type", target_type.as_str());
            info!("Start working on target types...");
            let valid_fields = factory.get_valid_fields();
            for (id, merged_config) in collect_target_configs(config, NOTIFY_ROUTE_PREFIX, target_type, &valid_fields) {
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
                    info!(instance_id = %id, "Create target successfully");
                    successful_targets.push(target);
                }
                Err(e) => {
                    error!(instance_id = %id, error = %e, "Failed to create target");
                }
            }
        }

        info!(count = successful_targets.len(), "All target processing completed");
        Ok(successful_targets)
    }
}
