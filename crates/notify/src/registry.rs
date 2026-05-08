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
use crate::factory::builtin_target_plugins;
use rustfs_config::notify::NOTIFY_ROUTE_PREFIX;
use rustfs_ecstore::config::{Config, KVS};
use rustfs_targets::{Target, TargetError, TargetPluginRegistry};

/// Registry for managing target factories
pub struct TargetRegistry {
    plugins: TargetPluginRegistry<Event>,
}

impl Default for TargetRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl TargetRegistry {
    /// Creates a new TargetRegistry with built-in factories
    pub fn new() -> Self {
        let mut plugins = TargetPluginRegistry::new();
        plugins.register_all(builtin_target_plugins());

        TargetRegistry { plugins }
    }

    pub fn supports_target_type(&self, target_type: &str) -> bool {
        self.plugins.supports_target_type(target_type)
    }

    /// Creates a target from configuration
    pub async fn create_target(
        &self,
        target_type: &str,
        id: String,
        config: &KVS,
    ) -> Result<Box<dyn Target<Event> + Send + Sync>, TargetError> {
        self.plugins.create_target(target_type, id, config)
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
        self.plugins.create_targets_from_config(config, NOTIFY_ROUTE_PREFIX).await
    }
}

#[cfg(test)]
mod tests {
    use super::TargetRegistry;
    use rustfs_targets::target::ChannelTargetType;

    #[test]
    fn registry_registers_amqp_factory() {
        let registry = TargetRegistry::new();

        assert!(registry.supports_target_type(ChannelTargetType::Amqp.as_str()));
    }
}
