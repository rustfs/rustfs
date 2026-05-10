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

use crate::{Event, NotificationError, notifier::EventNotifier, registry::TargetRegistry, runtime_facade::NotifyRuntimeFacade};
use rustfs_config::notify::{
    NOTIFY_AMQP_SUB_SYS, NOTIFY_KAFKA_SUB_SYS, NOTIFY_MQTT_SUB_SYS, NOTIFY_MYSQL_SUB_SYS, NOTIFY_NATS_SUB_SYS,
    NOTIFY_POSTGRES_SUB_SYS, NOTIFY_PULSAR_SUB_SYS, NOTIFY_REDIS_SUB_SYS, NOTIFY_WEBHOOK_SUB_SYS,
};
use rustfs_ecstore::config::{Config, KVS};
use rustfs_targets::{Target, arn::TargetID};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

pub(crate) fn notify_configuration_hint() -> String {
    let webhook_enable_primary = format!("{}_PRIMARY", rustfs_config::notify::ENV_NOTIFY_WEBHOOK_ENABLE);
    let webhook_endpoint_primary = format!("{}_PRIMARY", rustfs_config::notify::ENV_NOTIFY_WEBHOOK_ENDPOINT);
    format!(
        "No notify targets configured. Check {}=true and instance-scoped target env vars (for example {webhook_enable_primary} + {webhook_endpoint_primary} for arn:rustfs:sqs::primary:webhook). If using default queue_dir, ensure {} is writable.",
        rustfs_config::ENV_NOTIFY_ENABLE,
        rustfs_config::EVENT_DEFAULT_DIR,
    )
}

fn subsystem_target_type(target_type: &str) -> &str {
    match target_type {
        NOTIFY_AMQP_SUB_SYS => "amqp",
        NOTIFY_WEBHOOK_SUB_SYS => "webhook",
        NOTIFY_KAFKA_SUB_SYS => "kafka",
        NOTIFY_MQTT_SUB_SYS => "mqtt",
        NOTIFY_MYSQL_SUB_SYS => "mysql",
        NOTIFY_NATS_SUB_SYS => "nats",
        NOTIFY_POSTGRES_SUB_SYS => "postgres",
        NOTIFY_PULSAR_SUB_SYS => "pulsar",
        NOTIFY_REDIS_SUB_SYS => "redis",
        _ => target_type,
    }
}

pub fn runtime_target_id_for_subsystem(target_type: &str, target_name: &str) -> TargetID {
    TargetID {
        id: target_name.to_lowercase(),
        name: subsystem_target_type(target_type).to_string(),
    }
}

#[derive(Clone)]
pub struct NotifyConfigManager {
    config: Arc<RwLock<Config>>,
    registry: Arc<TargetRegistry>,
    notifier: Arc<EventNotifier>,
    runtime_facade: NotifyRuntimeFacade,
}

impl NotifyConfigManager {
    pub fn new(
        config: Arc<RwLock<Config>>,
        registry: Arc<TargetRegistry>,
        notifier: Arc<EventNotifier>,
        runtime_facade: NotifyRuntimeFacade,
    ) -> Self {
        Self {
            config,
            registry,
            notifier,
            runtime_facade,
        }
    }

    pub async fn init(&self) -> Result<(), NotificationError> {
        info!("Initialize notification system...");

        let config = {
            let guard = self.config.read().await;
            debug!(
                subsystem_count = guard.0.len(),
                "Initializing notification system with configuration summary"
            );
            guard.clone()
        };

        let targets: Vec<Box<dyn Target<Event> + Send + Sync>> = self.registry.create_targets_from_config(&config).await?;

        info!("{} notification targets were created", targets.len());
        if targets.is_empty() {
            warn!("{}", notify_configuration_hint());
        }

        let activation = self.runtime_facade.activate_targets_with_replay(targets).await;
        self.runtime_facade.replace_targets(activation).await?;
        info!("Notification system initialized");
        Ok(())
    }

    pub async fn remove_target(&self, target_id: &TargetID, target_type: &str) -> Result<(), NotificationError> {
        info!("Attempting to remove target: {}", target_id);

        let ttype = target_type.to_lowercase();
        let tname = target_id.id.to_lowercase();

        self.update_config_and_reload(|config| {
            let mut changed = false;
            if let Some(targets_of_type) = config.0.get_mut(&ttype) {
                if targets_of_type.remove(&tname).is_some() {
                    info!("Removed target {} from configuration", target_id);
                    changed = true;
                }
                if targets_of_type.is_empty() {
                    config.0.remove(&ttype);
                }
            }
            if !changed {
                warn!("Target {} not found in configuration", target_id);
            }
            changed
        })
        .await
    }

    pub async fn set_target_config(&self, target_type: &str, target_name: &str, kvs: KVS) -> Result<(), NotificationError> {
        info!("Setting config for target {} of type {}", target_name, target_type);
        let ttype = target_type.to_lowercase();
        let tname = target_name.to_lowercase();
        self.update_config_and_reload(|config| {
            config.0.entry(ttype.clone()).or_default().insert(tname.clone(), kvs.clone());
            true
        })
        .await
    }

    pub async fn remove_target_config(&self, target_type: &str, target_name: &str) -> Result<(), NotificationError> {
        info!("Removing config for target {} of type {}", target_name, target_type);

        let ttype = target_type.to_lowercase();
        let tname = target_name.to_lowercase();
        let target_id = runtime_target_id_for_subsystem(&ttype, &tname);

        if self.notifier.is_target_bound_to_any_bucket(&target_id).await {
            return Err(NotificationError::Configuration(format!(
                "Target is still bound to bucket rules and deletion is prohibited: type={} name={}",
                ttype, tname
            )));
        }

        let config_result = self
            .update_config_and_reload(|config| {
                let mut changed = false;
                if let Some(targets) = config.0.get_mut(&ttype) {
                    if targets.remove(&tname).is_some() {
                        changed = true;
                    }
                    if targets.is_empty() {
                        config.0.remove(&ttype);
                    }
                }
                if !changed {
                    info!("Target {} of type {} not found, no changes made.", target_name, target_type);
                }
                debug!(
                    subsystem_count = config.0.len(),
                    "Target config removal processed and configuration summary updated"
                );
                changed
            })
            .await;

        if config_result.is_ok() {
            let target_list = self.notifier.target_list();
            let mut target_list_guard = target_list.write().await;
            let _ = target_list_guard.remove_target_only(&target_id).await;
        }

        config_result
    }

    pub async fn reload_config(&self, new_config: Config) -> Result<(), NotificationError> {
        info!("Reload notification configuration starts");

        self.update_config(new_config.clone()).await;

        let targets: Vec<Box<dyn Target<Event> + Send + Sync>> = self
            .registry
            .create_targets_from_config(&new_config)
            .await
            .map_err(NotificationError::Target)?;

        info!("{} notification targets were created from the new configuration", targets.len());
        if targets.is_empty() {
            warn!("{}", notify_configuration_hint());
        }

        let activation = self.runtime_facade.activate_targets_with_replay(targets).await;
        self.runtime_facade.replace_targets(activation).await?;
        info!("Configuration reloaded end");
        Ok(())
    }

    async fn update_config(&self, new_config: Config) {
        let mut config = self.config.write().await;
        *config = new_config;
    }

    async fn update_config_and_reload<F>(&self, mut modifier: F) -> Result<(), NotificationError>
    where
        F: FnMut(&mut Config) -> bool,
    {
        let Some(store) = rustfs_ecstore::global::new_object_layer_fn() else {
            return Err(NotificationError::StorageNotAvailable(
                "Failed to save target configuration: server storage not initialized".to_string(),
            ));
        };

        let mut new_config = rustfs_ecstore::config::com::read_config_without_migrate(store.clone())
            .await
            .map_err(|e| NotificationError::ReadConfig(e.to_string()))?;

        if !modifier(&mut new_config) {
            info!("Configuration not changed, skipping save and reload.");
            return Ok(());
        }

        rustfs_ecstore::config::com::save_server_config(store, &new_config)
            .await
            .map_err(|e| NotificationError::SaveConfig(e.to_string()))?;

        info!("Configuration updated. Reloading system...");
        self.reload_config(new_config).await
    }
}

#[cfg(test)]
mod tests {
    use super::{NotifyConfigManager, runtime_target_id_for_subsystem};
    use crate::{
        integration::NotificationMetrics, notifier::EventNotifier, registry::TargetRegistry, runtime_facade::NotifyRuntimeFacade,
    };
    use rustfs_config::notify::{
        NOTIFY_AMQP_SUB_SYS, NOTIFY_KAFKA_SUB_SYS, NOTIFY_MQTT_SUB_SYS, NOTIFY_NATS_SUB_SYS, NOTIFY_POSTGRES_SUB_SYS,
        NOTIFY_PULSAR_SUB_SYS, NOTIFY_REDIS_SUB_SYS, NOTIFY_WEBHOOK_SUB_SYS,
    };
    use rustfs_ecstore::config::Config;
    use rustfs_targets::ReplayWorkerManager;
    use std::sync::Arc;
    use tokio::sync::{RwLock, Semaphore};

    fn build_manager() -> NotifyConfigManager {
        let config = Arc::new(RwLock::new(Config::default()));
        let registry = Arc::new(TargetRegistry::new());
        let metrics = Arc::new(NotificationMetrics::new());
        let notifier = Arc::new(EventNotifier::new(metrics.clone()));
        let runtime_facade = NotifyRuntimeFacade::new(
            notifier.clone(),
            Arc::new(RwLock::new(ReplayWorkerManager::new())),
            Arc::new(Semaphore::new(4)),
            metrics,
        );

        NotifyConfigManager::new(config, registry, notifier, runtime_facade)
    }

    #[tokio::test]
    async fn config_manager_init_accepts_empty_target_set() {
        let manager = build_manager();
        manager.init().await.expect("init should succeed for empty targets");
    }

    #[tokio::test]
    async fn config_manager_reload_accepts_empty_target_set() {
        let manager = build_manager();
        manager
            .reload_config(Config::default())
            .await
            .expect("reload_config should succeed for empty targets");
    }

    #[test]
    fn runtime_target_id_for_subsystem_maps_notify_webhook_to_runtime_type() {
        let target_id = runtime_target_id_for_subsystem(NOTIFY_WEBHOOK_SUB_SYS, "Primary");
        assert_eq!(target_id.id, "primary");
        assert_eq!(target_id.name, "webhook");
    }

    #[test]
    fn runtime_target_id_for_subsystem_maps_notify_amqp_to_runtime_type() {
        let target_id = runtime_target_id_for_subsystem(NOTIFY_AMQP_SUB_SYS, "Primary");
        assert_eq!(target_id.id, "primary");
        assert_eq!(target_id.name, "amqp");
    }

    #[test]
    fn runtime_target_id_for_subsystem_maps_notify_mqtt_to_runtime_type() {
        let target_id = runtime_target_id_for_subsystem(NOTIFY_MQTT_SUB_SYS, "Analytics");
        assert_eq!(target_id.id, "analytics");
        assert_eq!(target_id.name, "mqtt");
    }

    #[test]
    fn runtime_target_id_for_subsystem_maps_notify_kafka_to_runtime_type() {
        let target_id = runtime_target_id_for_subsystem(NOTIFY_KAFKA_SUB_SYS, "EventBus");
        assert_eq!(target_id.id, "eventbus");
        assert_eq!(target_id.name, "kafka");
    }

    #[test]
    fn runtime_target_id_for_subsystem_maps_notify_nats_to_runtime_type() {
        let target_id = runtime_target_id_for_subsystem(NOTIFY_NATS_SUB_SYS, "Bus");
        assert_eq!(target_id.id, "bus");
        assert_eq!(target_id.name, "nats");
    }

    #[test]
    fn runtime_target_id_for_subsystem_maps_notify_pulsar_to_runtime_type() {
        let target_id = runtime_target_id_for_subsystem(NOTIFY_PULSAR_SUB_SYS, "Ledger");
        assert_eq!(target_id.id, "ledger");
        assert_eq!(target_id.name, "pulsar");
    }

    #[test]
    fn runtime_target_id_for_subsystem_maps_notify_redis_to_runtime_type() {
        let target_id = runtime_target_id_for_subsystem(NOTIFY_REDIS_SUB_SYS, "Primary");
        assert_eq!(target_id.id, "primary");
        assert_eq!(target_id.name, "redis");
    }

    #[test]
    fn runtime_target_id_for_subsystem_maps_notify_postgres_to_runtime_type() {
        let target_id = runtime_target_id_for_subsystem(NOTIFY_POSTGRES_SUB_SYS, "AuditTrail");
        assert_eq!(target_id.id, "audittrail");
        assert_eq!(target_id.name, "postgres");
    }
}
