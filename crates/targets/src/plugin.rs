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
    PluginRuntimeAdapter, RuntimeActivation, Target, TargetError,
    config::collect_target_config_results,
    manifest::{TargetPluginManifest, builtin_target_manifest},
    target::with_deferred_queue_store_open,
};
use hashbrown::HashMap;
use rustfs_config::server_config::{Config, KVS};
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::collections::HashSet;
use std::sync::Arc;
use tracing::{error, info, warn};

type BoxedTarget<E> = Box<dyn Target<E> + Send + Sync>;
type TargetCreateFn<E> = Arc<dyn Fn(String, &KVS) -> Result<BoxedTarget<E>, TargetError> + Send + Sync>;
type TargetValidateFn = Arc<dyn Fn(&KVS) -> Result<(), TargetError> + Send + Sync>;

/// Event payload contract shared by all target plugin machinery.
///
/// Blanket-implemented for every type meeting the bounds; it exists solely to
/// keep this composite bound spelled in one place instead of on every generic.
pub trait PluginEvent: Send + Sync + Clone + Serialize + DeserializeOwned + 'static {}

impl<T> PluginEvent for T where T: Send + Sync + Clone + Serialize + DeserializeOwned + 'static {}

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
    E: PluginEvent,
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
    E: PluginEvent,
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
    E: PluginEvent,
{
    plugin: TargetPluginDescriptor<E>,
    admin: TargetAdminMetadata,
}

impl<E> BuiltinTargetDescriptor<E>
where
    E: PluginEvent,
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
    E: PluginEvent,
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
    E: PluginEvent,
{
    plugins: HashMap<String, TargetPluginDescriptor<E>>,
}

impl<E> Default for TargetPluginRegistry<E>
where
    E: PluginEvent,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<E> TargetPluginRegistry<E>
where
    E: PluginEvent,
{
    pub fn new() -> Self {
        Self { plugins: HashMap::new() }
    }

    pub fn register(&mut self, plugin: TargetPluginDescriptor<E>) -> Option<TargetPluginDescriptor<E>> {
        let replaced = self.plugins.insert(plugin.target_type().to_string(), plugin);
        if let Some(previous) = &replaced {
            warn!(
                target_type = %previous.target_type(),
                plugin_id = %previous.manifest().plugin_id,
                "replacing previously registered target plugin descriptor"
            );
        }
        replaced
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

    /// Creates every enabled target instance found in `config`.
    ///
    /// Creation is fault-isolated per instance: one broken target must not
    /// prevent the remaining targets from activating, so failures are logged
    /// and summarized instead of aborting the whole activation.
    pub async fn create_targets_from_config(
        &self,
        config: &Config,
        route_prefix: &str,
    ) -> Result<Vec<BoxedTarget<E>>, TargetError> {
        self.create_targets_from_config_with_store_mode(config, route_prefix, false)
            .await
            .map(|(targets, _)| targets)
    }

    /// Creates targets while deferring queue-store open until runtime handoff.
    /// Unlike the compatibility activation API, lifecycle preparation reports
    /// any invalid or unconstructable configured instance so the originating
    /// Admin request cannot report a false success.
    pub async fn create_dormant_targets_from_config(
        &self,
        config: &Config,
        route_prefix: &str,
    ) -> Result<(Vec<BoxedTarget<E>>, Vec<String>), TargetError> {
        self.create_targets_from_config_with_store_mode(config, route_prefix, true)
            .await
    }

    async fn create_targets_from_config_with_store_mode(
        &self,
        config: &Config,
        route_prefix: &str,
        defer_store_open: bool,
    ) -> Result<(Vec<BoxedTarget<E>>, Vec<String>), TargetError> {
        let mut successful_targets = Vec::new();
        let mut failures = Vec::new();

        for (target_type, plugin) in &self.plugins {
            info!(target_type = %target_type, "Start working on target type");
            // Per-instance fault isolation: an invalid instance (e.g. an
            // unparseable `enable` value) is recorded as a failure and skipped,
            // never aborting the remaining instances or other target types.
            let (collected, invalid_instances) =
                collect_target_config_results(config, route_prefix, target_type, plugin.valid_fields_set());
            for detail in invalid_instances {
                error!(target_type = %target_type, reason = "invalid_config", detail = %detail, "Skipping target instance with invalid configuration");
                failures.push(detail);
            }
            for (id, merged_config) in collected {
                info!(target_type = %target_type, instance_id = %id, "Target is enabled, ready to create");
                let created = if defer_store_open {
                    with_deferred_queue_store_open(|| self.create_target(target_type, id.clone(), &merged_config))
                } else {
                    self.create_target(target_type, id.clone(), &merged_config)
                };
                match created {
                    Ok(target) => {
                        info!(target_type = %target.id().name, instance_id = %id, "Create target successfully");
                        successful_targets.push(target);
                    }
                    Err(_) => {
                        failures.push(format!("{target_type}/{id}: target construction failed"));
                        error!(target_type = %target_type, instance_id = %id, reason = "construction_failed", "Failed to create target");
                    }
                }
            }
        }

        if !failures.is_empty() {
            warn!(
                created = successful_targets.len(),
                failed = failures.len(),
                "Some configured targets failed to create and were skipped"
            );
        }
        info!(
            count = successful_targets.len(),
            failed = failures.len(),
            "All target processing completed"
        );
        Ok((successful_targets, failures))
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
    E: PluginEvent,
    T: Target<E> + Send + Sync + 'static,
{
    Box::new(target)
}

#[cfg(test)]
mod tests {
    use super::{TargetPluginDescriptor, TargetPluginRegistry};
    use crate::PluginEvent;
    use crate::runtime::adapter::BuiltinPluginRuntimeAdapter;
    use crate::store::{Key, Store};
    use crate::target::{EntityTarget, QueuedPayload, QueuedPayloadMeta};
    use crate::{StoreError, Target, TargetError};
    use async_trait::async_trait;
    use rustfs_config::ENABLE_KEY;
    use rustfs_config::server_config::{Config, KVS};
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
        E: PluginEvent,
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

    // Regression: a single instance with a malformed `enable` value must not
    // abort the remaining instances or unrelated target types. Before this fix
    // the collector short-circuited the whole create path, so one typo took
    // down every notify/audit target.
    #[tokio::test]
    async fn create_dormant_isolates_invalid_enable_and_still_loads_other_targets() {
        let mut registry = TargetPluginRegistry::<String>::new();
        for target_type in ["alpha", "beta"] {
            registry.register(TargetPluginDescriptor::new(
                target_type,
                &[ENABLE_KEY, "endpoint"],
                |_config| Ok(()),
                move |id, _config| {
                    Ok(Box::new(TestTarget {
                        id: crate::arn::TargetID::new(id, target_type.to_string()),
                    }))
                },
            ));
        }

        let mut cfg = Config(HashMap::new());

        // alpha: one healthy instance plus one with a malformed `enable` value
        // ("enable" is a typo -- EnableState accepts "enabled"/"on", not "enable").
        let mut alpha = HashMap::new();
        let mut alpha_good = KVS::new();
        alpha_good.insert(ENABLE_KEY.to_string(), "on".to_string());
        alpha_good.insert("endpoint".to_string(), "https://example.com/alpha".to_string());
        alpha.insert("good".to_string(), alpha_good);
        let mut alpha_bad = KVS::new();
        alpha_bad.insert(ENABLE_KEY.to_string(), "enable".to_string());
        alpha.insert("bad".to_string(), alpha_bad);
        cfg.0.insert("notify_alpha".to_string(), alpha);

        // beta: a healthy instance in a different target type must survive.
        let mut beta = HashMap::new();
        let mut beta_primary = KVS::new();
        beta_primary.insert(ENABLE_KEY.to_string(), "on".to_string());
        beta_primary.insert("endpoint".to_string(), "https://example.com/beta".to_string());
        beta.insert("primary".to_string(), beta_primary);
        cfg.0.insert("notify_beta".to_string(), beta);

        let (targets, failures) = registry
            .create_dormant_targets_from_config(&cfg, "notify_")
            .await
            .expect("a malformed instance must not abort target creation");

        let mut created: Vec<String> = targets.iter().map(|target| target.id().to_string()).collect();
        created.sort();
        assert_eq!(created, vec!["good:alpha".to_string(), "primary:beta".to_string()]);

        // The malformed instance is surfaced (so an Admin write can't report a
        // false success) rather than silently dropped or fatally aborting.
        assert_eq!(failures.len(), 1);
        assert!(failures[0].contains("alpha/bad"), "unexpected failure summary: {}", failures[0]);
    }
}
