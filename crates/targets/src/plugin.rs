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

use crate::{Target, TargetError, config::collect_target_configs};
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
    MySql,
    Nats(crate::target::TargetType),
    Postgres(crate::target::TargetType),
    Pulsar(crate::target::TargetType),
    Redis {
        default_channel: &'static str,
        target_type: crate::target::TargetType,
    },
}

#[derive(Clone)]
pub struct TargetPluginDescriptor<E>
where
    E: Send + Sync + 'static + Clone + Serialize + DeserializeOwned,
{
    create_target: TargetCreateFn<E>,
    target_type: &'static str,
    valid_fields: &'static [&'static str],
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
        Self {
            create_target: Arc::new(create_target),
            target_type,
            valid_fields,
            validate_config: Arc::new(validate_config),
        }
    }

    #[inline]
    pub fn target_type(&self) -> &'static str {
        self.target_type
    }

    #[inline]
    pub fn valid_fields(&self) -> &'static [&'static str] {
        self.valid_fields
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
    request_validator: TargetRequestValidator,
    subsystem: &'static str,
}

impl<E> BuiltinTargetDescriptor<E>
where
    E: Send + Sync + 'static + Clone + Serialize + DeserializeOwned,
{
    pub fn new(subsystem: &'static str, request_validator: TargetRequestValidator, plugin: TargetPluginDescriptor<E>) -> Self {
        Self {
            plugin,
            request_validator,
            subsystem,
        }
    }

    #[inline]
    pub fn plugin(&self) -> &TargetPluginDescriptor<E> {
        &self.plugin
    }

    #[inline]
    pub fn request_validator(&self) -> TargetRequestValidator {
        self.request_validator
    }

    #[inline]
    pub fn subsystem(&self) -> &'static str {
        self.subsystem
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
            let valid_fields = plugin
                .valid_fields()
                .iter()
                .map(|field| (*field).to_string())
                .collect::<HashSet<_>>();

            info!(target_type = %target_type, "Start working on target type");
            for (id, merged_config) in collect_target_configs(config, route_prefix, target_type, &valid_fields) {
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
}

pub fn boxed_target<E, T>(target: T) -> BoxedTarget<E>
where
    E: Send + Sync + 'static + Clone + Serialize + DeserializeOwned,
    T: Target<E> + Send + Sync + 'static,
{
    Box::new(target)
}
