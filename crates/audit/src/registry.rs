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
use rustfs_targets::arn::TargetID;
use rustfs_targets::target::mqtt::{MQTTArgs, MQTTTarget};
use rustfs_targets::target::webhook::{WebhookArgs, WebhookTarget};
use rustfs_targets::target::{EntityTarget, Target, TargetType};
use rustfs_targets::TargetError;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

/// 用于创建审计目标的工厂的 Trait。
///
/// 这个 trait 抽象了 `Target` 实例的创建过程，允许 `TargetRegistry`
/// 在不知道具体实现细节的情况下，根据提供的参数创建不同类型的目标。
#[async_trait]
pub trait AuditTargetFactory: Send + Sync {
    /// 根据提供的参数创建一个新的审计目标。
    ///
    /// # Arguments
    ///
    /// * `id` - 目标的唯一标识符。
    /// * `args` - 特定于目标的配置参数。
    ///
    /// # Returns
    ///
    /// 返回一个 `Result`，其中包含一个装箱的 `Target` trait 对象，或者一个 `TargetError`。
    async fn create(&self, id: String, args: TargetArgs) -> Result<Box<dyn Target<AuditEntry> + Send + Sync>, TargetError>;
}

/// `AuditTargetFactory` 的默认实现。
pub struct DefaultAuditTargetFactory;

#[async_trait]
impl AuditTargetFactory for DefaultAuditTargetFactory {
    async fn create(&self, id: String, args: TargetArgs) -> Result<Box<dyn Target<AuditEntry> + Send + Sync>, TargetError> {
        info!(target_id = %id, "Creating new audit target");
        match args {
            TargetArgs::Mqtt(mqtt_args) => {
                // 确保目标类型正确
                if !matches!(mqtt_args.target_type, TargetType::AuditLog) {
                    return Err(TargetError::Configuration("MQTTArgs provided for a non-audit target type".to_string()));
                }
                let target = MQTTTarget::<AuditEntry>::new(id, mqtt_args)?;
                Ok(Box::new(target))
            } // 在这里可以为其他目标类型（如 Webhook, Kafka 等）添加更多的分支
        }
    }
}

/// 用于不同目标类型的配置参数的枚举。
///
/// 这允许 `AuditTargetFactory` 处理不同类型的目标配置。
#[derive(Debug, Clone)]
pub enum TargetArgs {
    Mqtt(MQTTArgs),
    // 可以为其他目标类型添加变体，例如：
    Webhook(WebhookArgs),
}

/// 管理审计目标的注册表。
///
/// `TargetRegistry` 负责维护一个活动审计目标的集合。它提供了添加、
/// 检索和分派事件到这些目标的方法。
#[derive(Clone)]
pub struct TargetRegistry {
    targets: Arc<RwLock<HashMap<TargetID, Arc<dyn Target<AuditEntry> + Send + Sync>>>>,
    factory: Arc<dyn AuditTargetFactory>,
}

impl TargetRegistry {
    /// 创建一个新的 `TargetRegistry` 实例。
    ///
    /// # Arguments
    ///
    /// * `factory` - 一个 `AuditTargetFactory` 的实现，用于创建目标实例。
    pub fn new(factory: Arc<dyn AuditTargetFactory>) -> Self {
        Self {
            targets: Arc::new(RwLock::new(HashMap::new())),
            factory,
        }
    }

    /// 使用工厂创建一个新的目标并将其添加到注册表中。
    ///
    /// # Arguments
    ///
    /// * `id` - 目标的唯一标识符。
    /// * `args` - 用于创建目标的配置参数。
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

    /// 将审计事件分派到所有已注册且启用的目标。
    ///
    /// 此方法会异步地将事件条目发送到每个目标。它会记录任何在分派过程中发生的错误，
    /// 但不会因为单个目标的失败而停止向其他目标分派。
    ///
    /// # Arguments
    ///
    /// * `entry` - 要记录的审计事件条目。
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

    /// 关闭并清理所有已注册的目标。
    ///
    /// 此方法会迭代所有目标并调用它们的 `close` 方法，以释放资源。
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
