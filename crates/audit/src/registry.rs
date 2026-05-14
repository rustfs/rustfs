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

use crate::{AuditEntry, AuditError, AuditResult, factory::builtin_target_plugins};
use rustfs_config::audit::AUDIT_ROUTE_PREFIX;
use rustfs_ecstore::config::{Config, KVS};
use rustfs_targets::arn::TargetID;
use rustfs_targets::{SharedTarget, Target, TargetError, TargetPluginRegistry, TargetRuntimeManager};
use tracing::info;

/// Registry for managing audit targets
pub struct AuditRegistry {
    /// Storage for created targets
    targets: TargetRuntimeManager<AuditEntry>,
    /// Registered plugins for creating targets
    plugins: TargetPluginRegistry<AuditEntry>,
}

impl Default for AuditRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl AuditRegistry {
    /// Creates a new AuditRegistry
    pub fn new() -> Self {
        let mut plugins = TargetPluginRegistry::new();
        plugins.register_all(builtin_target_plugins());

        AuditRegistry {
            targets: TargetRuntimeManager::new(),
            plugins,
        }
    }

    pub fn supports_target_type(&self, target_type: &str) -> bool {
        self.plugins.supports_target_type(target_type)
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
    pub async fn create_audit_targets_from_config(
        &self,
        config: &Config,
    ) -> AuditResult<Vec<Box<dyn Target<AuditEntry> + Send + Sync>>> {
        self.plugins
            .create_targets_from_config(config, AUDIT_ROUTE_PREFIX)
            .await
            .map_err(AuditError::from)
    }

    /// Adds a target to the registry
    ///
    /// # Arguments
    /// * `id` - The identifier for the target.
    /// * `target` - The target instance to be added.
    pub fn add_target(&mut self, _id: String, target: Box<dyn Target<AuditEntry> + Send + Sync>) {
        debug_assert_eq!(_id, target.id().to_string());
        self.targets.add_boxed(target);
    }

    pub fn add_shared_target(&mut self, _id: String, target: SharedTarget<AuditEntry>) {
        debug_assert_eq!(_id, target.id().to_string());
        self.targets.add_arc(target);
    }

    /// Removes a target from the registry
    ///
    /// # Arguments
    /// * `id` - The identifier for the target to be removed.
    ///
    /// # Returns
    /// * `Option<Box<dyn Target<AuditEntry> + Send + Sync>>` - The removed target if it existed.
    pub async fn remove_target(&mut self, id: &str) -> Option<rustfs_targets::SharedTarget<AuditEntry>> {
        self.targets.remove_and_close(id).await
    }

    /// Gets a target from the registry
    ///
    /// # Arguments
    /// * `id` - The identifier for the target to be retrieved.
    ///
    /// # Returns
    /// * `Option<&(dyn Target<AuditEntry> + Send + Sync)>` - The target if it exists.
    pub fn get_target(&self, id: &str) -> Option<rustfs_targets::SharedTarget<AuditEntry>> {
        self.targets.get(id)
    }

    /// Lists cloned target values for runtime inspection without exposing mutable registry access.
    pub fn list_target_values(&self) -> Vec<rustfs_targets::SharedTarget<AuditEntry>> {
        self.targets.values()
    }

    pub fn runtime_manager(&self) -> &TargetRuntimeManager<AuditEntry> {
        &self.targets
    }

    pub fn runtime_manager_mut(&mut self) -> &mut TargetRuntimeManager<AuditEntry> {
        &mut self.targets
    }

    /// Lists all target IDs
    ///
    /// # Returns
    /// * `Vec<String>` - A vector of all target IDs in the registry.
    pub fn list_targets(&self) -> Vec<String> {
        self.targets.keys()
    }

    /// Closes all targets and clears the registry
    ///
    /// # Returns
    /// * `AuditResult<()>` - Result indicating success or failure.
    pub async fn close_all(&mut self) -> AuditResult<()> {
        let mut first_error = None;

        for target_id in self.targets.keys() {
            if let Some(target) = self.targets.remove(&target_id)
                && let Err(err) = target.close().await {
                    tracing::error!(target_id = %target_id, error = %err, "Failed to close target during shutdown");
                    if first_error.is_none() {
                        first_error = Some(err);
                    }
                }
        }

        match first_error {
            Some(err) => Err(AuditError::Target(err)),
            None => Ok(()),
        }
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
        debug_assert_eq!(key, target.id().to_string());
        self.targets.add_boxed(target);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::AuditRegistry;
    use crate::{AuditEntry, AuditError};
    use rustfs_targets::arn::TargetID;
    use rustfs_targets::store::{Key, Store};
    use rustfs_targets::target::{ChannelTargetType, EntityTarget, QueuedPayload, QueuedPayloadMeta};
    use rustfs_targets::{StoreError, Target, TargetError};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Clone)]
    struct CloseTestTarget {
        id: TargetID,
        close_calls: Arc<AtomicUsize>,
        fail_on_close: bool,
    }

    impl CloseTestTarget {
        fn new(id: TargetID, close_calls: Arc<AtomicUsize>, fail_on_close: bool) -> Self {
            Self {
                id,
                close_calls,
                fail_on_close,
            }
        }
    }

    #[async_trait::async_trait]
    impl Target<AuditEntry> for CloseTestTarget {
        fn id(&self) -> TargetID {
            self.id.clone()
        }

        async fn is_active(&self) -> Result<bool, TargetError> {
            Ok(true)
        }

        async fn save(&self, _event: Arc<EntityTarget<AuditEntry>>) -> Result<(), TargetError> {
            Ok(())
        }

        async fn send_raw_from_store(&self, _key: Key, _body: Vec<u8>, _meta: QueuedPayloadMeta) -> Result<(), TargetError> {
            Ok(())
        }

        async fn close(&self) -> Result<(), TargetError> {
            self.close_calls.fetch_add(1, Ordering::SeqCst);
            if self.fail_on_close {
                Err(TargetError::Unknown("close failed".to_string()))
            } else {
                Ok(())
            }
        }

        fn store(&self) -> Option<&(dyn Store<QueuedPayload, Error = StoreError, Key = Key> + Send + Sync)> {
            None
        }

        fn clone_dyn(&self) -> Box<dyn Target<AuditEntry> + Send + Sync> {
            Box::new(self.clone())
        }

        fn is_enabled(&self) -> bool {
            true
        }
    }

    #[test]
    fn registry_registers_amqp_factory() {
        let registry = AuditRegistry::new();

        assert!(registry.supports_target_type(ChannelTargetType::Amqp.as_str()));
    }

    #[tokio::test]
    async fn close_all_returns_first_error_and_clears_targets() {
        let mut registry = AuditRegistry::new();
        let ok_calls = Arc::new(AtomicUsize::new(0));
        let fail_calls = Arc::new(AtomicUsize::new(0));

        let ok_id = TargetID::new("ok".to_string(), "webhook".to_string());
        let fail_id = TargetID::new("fail".to_string(), "webhook".to_string());

        registry.add_target(ok_id.to_string(), Box::new(CloseTestTarget::new(ok_id, Arc::clone(&ok_calls), false)));
        registry.add_target(
            fail_id.to_string(),
            Box::new(CloseTestTarget::new(fail_id, Arc::clone(&fail_calls), true)),
        );

        let result = registry.close_all().await;

        assert!(matches!(result, Err(AuditError::Target(TargetError::Unknown(_)))));
        assert_eq!(ok_calls.load(Ordering::SeqCst), 1);
        assert_eq!(fail_calls.load(Ordering::SeqCst), 1);
        assert!(registry.list_targets().is_empty());
    }
}
