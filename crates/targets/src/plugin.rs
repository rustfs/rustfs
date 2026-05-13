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

use crate::{
    PluginRuntimeAdapter, RuntimeActivation, Target, TargetError, catalog::builtin_target_manifest,
    config::collect_target_configs, manifest::TargetPluginManifest,
};
use hashbrown::HashMap;
use rustfs_ecstore::config::{Config, KVS};
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::collections::HashSet;
use std::sync::Arc;
use tracing::{error, info};

type BoxedTarget<E> = Box<dyn Target<E> + Send + Sync>;
type TargetCreateFn<E> = Arc<dyn Fn(String, &KVS) -> Result<BoxedTarget<E>, TargetError> + Send + Sync>;
type TargetValidateFn = Arc<dyn Fn(&KVS) -> Result<(), TargetError> + Send + Sync>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TargetRequestValidator {
    Webhook,
    Mqtt,
    Amqp(crate::target::TargetType),
    Kafka(crate::target::TargetType),
    MySql(crate::target::TargetType),
    Nats(crate::target::TargetType),
    Postgres(crate::target::TargetType),
    Pulsar(crate::target::TargetType),
    Redis {
        default_channel: &'static str,
        target_type: crate::target::TargetType,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TargetAdminMetadata {
    subsystem: &'static str,
    request_validator: TargetRequestValidator,
}

impl TargetAdminMetadata {
    pub fn new(subsystem: &'static str, request_validator: TargetRequestValidator) -> Self {
        Self {
            subsystem,
            request_validator,
        }
    }

    #[inline]
    pub fn subsystem(&self) -> &'static str {
        self.subsystem
    }

    #[inline]
    pub fn request_validator(&self) -> TargetRequestValidator {
        self.request_validator
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BuiltinTargetAdminDescriptor {
    manifest: TargetPluginManifest,
    valid_fields: &'static [&'static str],
    admin: TargetAdminMetadata,
}

impl BuiltinTargetAdminDescriptor {
    pub fn new(manifest: TargetPluginManifest, valid_fields: &'static [&'static str], admin: TargetAdminMetadata) -> Self {
        Self {
            manifest,
            valid_fields,
            admin,
        }
    }

    #[inline]
    pub fn manifest(&self) -> &TargetPluginManifest {
        &self.manifest
    }

    #[inline]
    pub fn valid_fields(&self) -> &'static [&'static str] {
        self.valid_fields
    }

    #[inline]
    pub fn admin_metadata(&self) -> TargetAdminMetadata {
        self.admin
    }
}

#[derive(Clone)]
pub struct TargetPluginDescriptor<E>
where
    E: Send + Sync + 'static + Clone + Serialize + DeserializeOwned,
{
    create_target: TargetCreateFn<E>,
    manifest: TargetPluginManifest,
    target_type: &'static str,
    valid_fields: &'static [&'static str],
    valid_fields_set: Arc<HashSet<String>>,
    validate_config: TargetValidateFn,
}

impl<E> TargetPluginDescriptor<E>
where
    E: Send + Sync + 'static + Clone + Serialize + DeserializeOwned,
{
    pub fn new<Create, Validate>(
        target_type: &'static str,
        valid_fields: &'static [&'static str],
        validate_config: Validate,
        create_target: Create,
    ) -> Self
    where
        Create: Fn(String, &KVS) -> Result<BoxedTarget<E>, TargetError> + Send + Sync + 'static,
        Validate: Fn(&KVS) -> Result<(), TargetError> + Send + Sync + 'static,
    {
        Self::with_manifest(builtin_target_manifest(target_type), valid_fields, validate_config, create_target)
    }

    pub fn with_manifest<Create, Validate>(
        manifest: TargetPluginManifest,
        valid_fields: &'static [&'static str],
        validate_config: Validate,
        create_target: Create,
    ) -> Self
    where
        Create: Fn(String, &KVS) -> Result<BoxedTarget<E>, TargetError> + Send + Sync + 'static,
        Validate: Fn(&KVS) -> Result<(), TargetError> + Send + Sync + 'static,
    {
        Self {
            create_target: Arc::new(create_target),
            manifest,
            target_type: manifest.target_type,
            valid_fields,
            valid_fields_set: Arc::new(valid_fields.iter().map(|field| (*field).to_string()).collect()),
            validate_config: Arc::new(validate_config),
        }
    }

    #[inline]
    pub fn target_type(&self) -> &'static str {
        self.target_type
    }

    #[inline]
    pub fn manifest(&self) -> &TargetPluginManifest {
        &self.manifest
    }

    #[inline]
    pub fn valid_fields(&self) -> &'static [&'static str] {
        self.valid_fields
    }

    #[inline]
    pub fn valid_fields_set(&self) -> &HashSet<String> {
        self.valid_fields_set.as_ref()
    }

    #[inline]
    pub fn validate_config(&self, config: &KVS) -> Result<(), TargetError> {
        (self.validate_config)(config)
    }

    #[inline]
    pub fn create_target(&self, id: String, config: &KVS) -> Result<BoxedTarget<E>, TargetError> {
        (self.create_target)(id, config)
    }
}

#[derive(Clone)]
pub struct BuiltinTargetDescriptor<E>
where
    E: Send + Sync + 'static + Clone + Serialize + DeserializeOwned,
{
    plugin: TargetPluginDescriptor<E>,
    admin: TargetAdminMetadata,
}

impl<E> BuiltinTargetDescriptor<E>
where
    E: Send + Sync + 'static + Clone + Serialize + DeserializeOwned,
{
    pub fn new(subsystem: &'static str, request_validator: TargetRequestValidator, plugin: TargetPluginDescriptor<E>) -> Self {
        Self {
            plugin,
            admin: TargetAdminMetadata::new(subsystem, request_validator),
        }
    }

    #[inline]
    pub fn plugin(&self) -> &TargetPluginDescriptor<E> {
        &self.plugin
    }

    #[inline]
    pub fn admin_metadata(&self) -> TargetAdminMetadata {
        self.admin
    }

    #[inline]
    pub fn request_validator(&self) -> TargetRequestValidator {
        self.admin.request_validator()
    }

    #[inline]
    pub fn subsystem(&self) -> &'static str {
        self.admin.subsystem()
    }
}

impl<E> From<BuiltinTargetDescriptor<E>> for BuiltinTargetAdminDescriptor
where
    E: Send + Sync + 'static + Clone + Serialize + DeserializeOwned,
{
    fn from(descriptor: BuiltinTargetDescriptor<E>) -> Self {
        Self::new(
            *descriptor.plugin().manifest(),
            descriptor.plugin().valid_fields(),
            descriptor.admin_metadata(),
        )
    }
}

pub struct TargetPluginRegistry<E>
where
    E: Send + Sync + 'static + Clone + Serialize + DeserializeOwned,
{
    plugins: HashMap<String, TargetPluginDescriptor<E>>,
}

impl<E> Default for TargetPluginRegistry<E>
where
    E: Send + Sync + 'static + Clone + Serialize + DeserializeOwned,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<E> TargetPluginRegistry<E>
where
    E: Send + Sync + 'static + Clone + Serialize + DeserializeOwned,
{
    pub fn new() -> Self {
        Self { plugins: HashMap::new() }
    }

    pub fn register(&mut self, plugin: TargetPluginDescriptor<E>) -> Option<TargetPluginDescriptor<E>> {
        self.plugins.insert(plugin.target_type().to_string(), plugin)
    }

    pub fn register_all<I>(&mut self, plugins: I)
    where
        I: IntoIterator<Item = TargetPluginDescriptor<E>>,
    {
        for plugin in plugins {
            self.register(plugin);
        }
    }

    pub fn supports_target_type(&self, target_type: &str) -> bool {
        self.plugins.contains_key(target_type)
    }

    pub fn registered_target_types(&self) -> Vec<String> {
        self.plugins.keys().cloned().collect()
    }

    pub fn create_target(&self, target_type: &str, id: String, config: &KVS) -> Result<BoxedTarget<E>, TargetError> {
        let plugin = self
            .plugins
            .get(target_type)
            .ok_or_else(|| TargetError::Configuration(format!("Unknown target type: {target_type}")))?;
        plugin.validate_config(config)?;
        plugin.create_target(id, config)
    }

    pub async fn create_targets_from_config(
        &self,
        config: &Config,
        route_prefix: &str,
    ) -> Result<Vec<BoxedTarget<E>>, TargetError> {
        let mut successful_targets = Vec::new();

        for (target_type, plugin) in &self.plugins {
            info!(target_type = %target_type, "Start working on target type");
            for (id, merged_config) in collect_target_configs(config, route_prefix, target_type, plugin.valid_fields_set()) {
                info!(target_type = %target_type, instance_id = %id, "Target is enabled, ready to create");
                match self.create_target(target_type, id.clone(), &merged_config) {
                    Ok(target) => {
                        info!(target_type = %target.id().name, instance_id = %id, "Create target successfully");
                        successful_targets.push(target);
                    }
                    Err(err) => {
                        error!(target_type = %target_type, instance_id = %id, error = %err, "Failed to create target");
                    }
                }
            }
        }

        info!(count = successful_targets.len(), "All target processing completed");
        Ok(successful_targets)
    }

    pub async fn create_activation_from_config<A>(
        &self,
        config: &Config,
        route_prefix: &str,
        adapter: &A,
    ) -> Result<RuntimeActivation<E>, TargetError>
    where
        A: PluginRuntimeAdapter<E> + ?Sized,
    {
        let targets = self.create_targets_from_config(config, route_prefix).await?;
        Ok(adapter.activate_with_replay(targets).await)
    }
}

pub fn boxed_target<E, T>(target: T) -> BoxedTarget<E>
where
    E: Send + Sync + 'static + Clone + Serialize + DeserializeOwned,
    T: Target<E> + Send + Sync + 'static,
{
    Box::new(target)
}

#[cfg(test)]
mod tests {
    use super::{TargetPluginDescriptor, TargetPluginRegistry};
    use crate::runtime::adapter::BuiltinPluginRuntimeAdapter;
    use crate::store::{Key, Store};
    use crate::target::{EntityTarget, QueuedPayload, QueuedPayloadMeta};
    use crate::{StoreError, Target, TargetError};
    use async_trait::async_trait;
    use rustfs_config::ENABLE_KEY;
    use rustfs_ecstore::config::{Config, KVS};
    use serde::{Serialize, de::DeserializeOwned};
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Duration;

    #[derive(Clone)]
    struct TestTarget {
        id: crate::arn::TargetID,
    }

    #[async_trait]
    impl<E> Target<E> for TestTarget
    where
        E: Send + Sync + 'static + Clone + Serialize + DeserializeOwned,
    {
        fn id(&self) -> crate::arn::TargetID {
            self.id.clone()
        }

        async fn is_active(&self) -> Result<bool, TargetError> {
            Ok(true)
        }

        async fn save(&self, _event: Arc<EntityTarget<E>>) -> Result<(), TargetError> {
            Ok(())
        }

        async fn send_raw_from_store(&self, _key: Key, _body: Vec<u8>, _meta: QueuedPayloadMeta) -> Result<(), TargetError> {
            Ok(())
        }

        async fn close(&self) -> Result<(), TargetError> {
            Ok(())
        }

        fn store(&self) -> Option<&(dyn Store<QueuedPayload, Error = StoreError, Key = Key> + Send + Sync)> {
            None
        }

        fn clone_dyn(&self) -> Box<dyn Target<E> + Send + Sync> {
            Box::new(self.clone())
        }

        fn is_enabled(&self) -> bool {
            true
        }
    }

    fn builtin_adapter() -> BuiltinPluginRuntimeAdapter<String> {
        BuiltinPluginRuntimeAdapter::new(
            Arc::new(|_event| Box::pin(async {})),
            Arc::new(|_target_id, _has_replay| {}),
            None,
            Duration::from_millis(10),
            Duration::from_millis(10),
            "stopping plugin registry test replay worker",
        )
    }

    #[tokio::test]
    async fn registry_creates_activation_from_config_via_runtime_adapter() {
        let mut registry = TargetPluginRegistry::new();
        registry.register(TargetPluginDescriptor::new(
            "test",
            &[ENABLE_KEY, "endpoint"],
            |_config| Ok(()),
            |id, _config| {
                Ok(Box::new(TestTarget {
                    id: crate::arn::TargetID::new(id, "test".to_string()),
                }))
            },
        ));

        let mut cfg = Config(HashMap::new());
        let mut section = HashMap::new();
        let mut primary = KVS::new();
        primary.insert(ENABLE_KEY.to_string(), "on".to_string());
        primary.insert("endpoint".to_string(), "https://example.com/hook".to_string());
        section.insert("primary".to_string(), primary);
        cfg.0.insert("notify_test".to_string(), section);

        let adapter = builtin_adapter();
        let activation = registry
            .create_activation_from_config(&cfg, "notify_", &adapter)
            .await
            .expect("activation should be created through runtime adapter");

        assert_eq!(activation.targets.len(), 1);
        assert_eq!(activation.targets[0].id().to_string(), "primary:test");
        assert!(activation.replay_workers.is_empty());
    }
}
