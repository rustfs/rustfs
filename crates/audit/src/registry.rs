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

use crate::entity::AuditEntry;
use async_trait::async_trait;
use rustfs_targets::TargetError;
use rustfs_targets::arn::TargetID;
use rustfs_targets::target::mqtt::{MQTTArgs, MQTTTarget};
use rustfs_targets::target::webhook::{WebhookArgs, WebhookTarget};
use rustfs_targets::target::{EntityTarget, Target, TargetType};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

/// Trait for the plant used to create the audit objective.
///
/// This trait abstracts the creation process of the 'Target' instance, allowing the 'TargetRegistry'
/// Create different types of targets based on the parameters provided without knowing the specific implementation details.
#[async_trait]
pub trait AuditTargetFactory: Send + Sync {
    /// Create a new audit objective based on the parameters provided.
    ///
    /// # Arguments
    ///
    /// * `id` - A unique identifier for the target.
    /// * `args` - Target-specific configuration parameters。
    ///
    /// # Returns
    ///
    /// Returns a 'Result' containing a boxed 'Target' trait object, or a 'TargetError'.
    async fn create(&self, id: String, args: TargetArgs) -> Result<Box<dyn Target<AuditEntry> + Send + Sync>, TargetError>;
}

/// `AuditTargetFactory` default implementation。
#[allow(dead_code)]
pub struct DefaultAuditTargetFactory;

#[async_trait]
impl AuditTargetFactory for DefaultAuditTargetFactory {
    async fn create(&self, id: String, args: TargetArgs) -> Result<Box<dyn Target<AuditEntry> + Send + Sync>, TargetError> {
        info!(target_id = %id, "Creating new audit target");
        match args {
            TargetArgs::Mqtt(mqtt_args) => {
                // Make sure the target type is correct
                if !matches!(mqtt_args.target_type, TargetType::AuditLog) {
                    return Err(TargetError::Configuration("MQTTArgs provided for a non-audit target type".to_string()));
                }
                let target = MQTTTarget::<AuditEntry>::new(id, mqtt_args)?;
                Ok(Box::new(target))
            } // Here you can add more branches for other target types like webhooks, kafka, etc
            TargetArgs::Webhook(webhook_args) => {
                if !matches!(webhook_args.target_type, TargetType::AuditLog) {
                    return Err(TargetError::Configuration("WebhookArgs provided for a non-audit target type".to_string()));
                }
                let target = WebhookTarget::<AuditEntry>::new(id, webhook_args)?;
                Ok(Box::new(target))
            }
        }
    }
}

/// Enumeration of configuration parameters for different target types.
///
/// This allows the 'AuditTargetFactory' to handle different types of target configurations.
#[derive(Debug, Clone)]
pub enum TargetArgs {
    Mqtt(MQTTArgs),
    // You can add variations for other target types, such as: Webhook, Kafka, etc.
    Webhook(WebhookArgs),
}

type AuditTargetMap = Arc<RwLock<HashMap<TargetID, Arc<dyn Target<AuditEntry> + Send + Sync>>>>;

/// Registry for managing audit objectives。
///
/// 'TargetRegistry' is responsible for maintaining a collection of active audit objectives. It provides the ability to add,
/// A way to retrieve and dispatch events to these destinations.
#[derive(Clone)]
#[allow(dead_code)]
pub struct TargetRegistry {
    targets: AuditTargetMap,
    factory: Arc<dyn AuditTargetFactory>,
}

impl TargetRegistry {
    /// Create a new instance of 'TargetRegistry'.
    ///
    /// # Arguments
    ///
    /// * 'factory' - An implementation of 'AuditTargetFactory' to create a target instance.
    pub fn new(factory: Arc<dyn AuditTargetFactory>) -> Self {
        Self {
            targets: Arc::new(RwLock::new(HashMap::new())),
            factory,
        }
    }

    /// Use the factory to create a new target and add it to the registry.
    ///
    /// # Arguments
    ///
    /// * 'id' - The unique identifier of the target.
    /// * 'args' - The configuration parameter used to create the target.
    pub async fn add_target(&self, id: String, args: TargetArgs) -> Result<(), TargetError> {
        let target = self.factory.create(id, args).await?;
        let target_id = target.id();

        info!(target_id = %target_id, "Initializing and adding target to registry");
        target.init().await?;

        let mut targets = self.targets.write().await;
        if targets.insert(target_id.clone(), Arc::from(target)).is_some() {
            warn!(target_id = %target_id, "An existing target with the same ID was replaced.");
        } else {
            debug!(target_id = %target_id, "Target successfully added to registry.");
        }

        Ok(())
    }

    /// Dispatch audit events to all registered and enabled targets。
    ///
    /// This method sends event entries to each target asynchronously. It records any errors that occur during the dispatch process,
    /// But it doesn't stop dispatching to other targets because of the failure of a single target.
    ///
    /// # Arguments
    ///
    ///* 'entry' - The audit event entry to be logged.
    pub async fn dispatch(&self, entry: Arc<EntityTarget<AuditEntry>>) {
        let targets = self.targets.read().await;
        if targets.is_empty() {
            debug!("No audit targets registered, skipping dispatch.");
            return;
        }

        debug!(event_name = %entry.event_name, "Dispatching audit entry to {} targets", targets.len());

        for (id, target) in targets.iter() {
            if !target.is_enabled() {
                debug!(target_id = %id, "Skipping disabled target");
                continue;
            }

            let entry_clone = entry.clone();
            let target_clone = target.clone();
            let id_clone = id.clone();

            tokio::spawn(async move {
                if let Err(e) = target_clone.save(entry_clone).await {
                    error!(target_id = %id_clone, error = %e, "Failed to save audit entry to target");
                }
            });
        }
    }

    /// Close and clean up all registered targets。
    ///
    /// This method iterates over all targets and calls their 'close' method to free up resources.
    pub async fn close(&self) {
        info!("Closing all registered audit targets.");
        let mut targets = self.targets.write().await;
        for (id, target) in targets.iter() {
            if let Err(e) = target.close().await {
                error!(target_id = %id, error = %e, "Error closing target");
            }
        }
        targets.clear();
        info!("All audit targets have been closed.");
    }
}
