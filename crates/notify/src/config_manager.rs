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
    Event, NotificationError, registry::TargetRegistry, resolve_notify_object_store_handle, rule_engine::NotifyRuleEngine,
    runtime_facade::NotifyRuntimeFacade,
};
use rustfs_config::notify::{
    NOTIFY_AMQP_SUB_SYS, NOTIFY_KAFKA_SUB_SYS, NOTIFY_MQTT_SUB_SYS, NOTIFY_MYSQL_SUB_SYS, NOTIFY_NATS_SUB_SYS,
    NOTIFY_POSTGRES_SUB_SYS, NOTIFY_PULSAR_SUB_SYS, NOTIFY_REDIS_SUB_SYS, NOTIFY_WEBHOOK_SUB_SYS,
};
use rustfs_config::server_config::{Config, KVS};
use rustfs_targets::{Target, arn::TargetID};
use std::sync::{Arc, LazyLock};
use tokio::sync::{Mutex, RwLock};
use tracing::{debug, info};

/// Serializes the read-modify-write sequence over the persisted notify server
/// config. The persisted config is a single process-global resource (there is
/// only one backing object store), so without this guard two concurrent updates
/// can both read the same base config, apply disjoint changes, and race their
/// full-config writes — the later write silently overwrites the earlier one,
/// losing updates. Holding this mutex across the whole read→modify→write makes
/// concurrent updates apply serially so every change is preserved.
///
/// The lock is only ever acquired inside `update_server_config`; it never nests
/// with the per-manager `config` RwLock (the in-memory reload runs after this
/// guard is released), so it introduces no lock-ordering risk.
static NOTIFY_CONFIG_RMW_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

const LOG_COMPONENT_NOTIFY: &str = "notify";
const LOG_SUBSYSTEM_CONFIG: &str = "config";
const EVENT_NOTIFY_RUNTIME_LIFECYCLE: &str = "notify_runtime_lifecycle";
const EVENT_NOTIFY_CONFIG_UPDATE: &str = "notify_config_update";

#[derive(Debug)]
enum NotifyConfigStoreError {
    StorageNotAvailable,
    Read(String),
    Save(String),
}

async fn update_server_config<F>(modifier: F) -> Result<Option<Config>, NotifyConfigStoreError>
where
    F: FnMut(&mut Config) -> bool,
{
    let Some(store) = resolve_notify_object_store_handle() else {
        return Err(NotifyConfigStoreError::StorageNotAvailable);
    };

    let store_for_save = store.clone();
    serialized_read_modify_write(
        modifier,
        move || async move {
            crate::read_notify_server_config_without_migrate(store)
                .await
                .map_err(NotifyConfigStoreError::Read)
        },
        move |config| async move {
            crate::save_notify_server_config(store_for_save, &config)
                .await
                .map_err(NotifyConfigStoreError::Save)
        },
    )
    .await
}

/// Runs a `read → modify → write` over the persisted notify config while holding
/// [`NOTIFY_CONFIG_RMW_LOCK`], so concurrent updates serialize and cannot clobber
/// each other's changes (backlog#968). `read`/`save` are injected so the exact
/// production serialization path can be exercised in tests without a live store.
async fn serialized_read_modify_write<F, R, RFut, S, SFut>(
    mut modifier: F,
    read: R,
    save: S,
) -> Result<Option<Config>, NotifyConfigStoreError>
where
    F: FnMut(&mut Config) -> bool,
    R: FnOnce() -> RFut,
    RFut: std::future::Future<Output = Result<Config, NotifyConfigStoreError>>,
    S: FnOnce(Config) -> SFut,
    SFut: std::future::Future<Output = Result<(), NotifyConfigStoreError>>,
{
    // Hold the RMW lock across the entire read→modify→write so concurrent
    // updates serialize and cannot clobber each other's changes (backlog#968).
    let _rmw_guard = NOTIFY_CONFIG_RMW_LOCK.lock().await;

    let mut new_config = read().await?;

    if !modifier(&mut new_config) {
        return Ok(None);
    }

    save(new_config.clone()).await?;

    Ok(Some(new_config))
}

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
    rule_engine: NotifyRuleEngine,
    runtime_facade: NotifyRuntimeFacade,
}

impl NotifyConfigManager {
    pub fn new(
        config: Arc<RwLock<Config>>,
        registry: Arc<TargetRegistry>,
        rule_engine: NotifyRuleEngine,
        runtime_facade: NotifyRuntimeFacade,
    ) -> Self {
        Self {
            config,
            registry,
            rule_engine,
            runtime_facade,
        }
    }

    pub async fn init(&self) -> Result<(), NotificationError> {
        info!(
            event = EVENT_NOTIFY_RUNTIME_LIFECYCLE,
            component = LOG_COMPONENT_NOTIFY,
            subsystem = LOG_SUBSYSTEM_CONFIG,
            state = "initializing",
            "notify runtime lifecycle"
        );

        let config = {
            let guard = self.config.read().await;
            debug!(
                subsystem_count = guard.0.len(),
                "Initializing notification system with configuration summary"
            );
            guard.clone()
        };

        let targets: Vec<Box<dyn Target<Event> + Send + Sync>> = self.registry.create_targets_from_config(&config).await?;

        info!(
            event = EVENT_NOTIFY_RUNTIME_LIFECYCLE,
            component = LOG_COMPONENT_NOTIFY,
            subsystem = LOG_SUBSYSTEM_CONFIG,
            state = "targets_created",
            target_count = targets.len(),
            "notify runtime lifecycle"
        );
        if targets.is_empty() {
            debug!(
                event = EVENT_NOTIFY_RUNTIME_LIFECYCLE,
                component = LOG_COMPONENT_NOTIFY,
                subsystem = LOG_SUBSYSTEM_CONFIG,
                state = "idle",
                reason = "no_targets_configured",
                hint = %notify_configuration_hint(),
                "notify runtime lifecycle"
            );
        }

        let activation = self.runtime_facade.activate_targets_with_replay(targets).await;
        self.runtime_facade.replace_targets(activation).await?;
        info!(
            event = EVENT_NOTIFY_RUNTIME_LIFECYCLE,
            component = LOG_COMPONENT_NOTIFY,
            subsystem = LOG_SUBSYSTEM_CONFIG,
            state = "initialized",
            "notify runtime lifecycle"
        );
        Ok(())
    }

    pub async fn remove_target(&self, target_id: &TargetID, target_type: &str) -> Result<(), NotificationError> {
        debug!(
            event = EVENT_NOTIFY_CONFIG_UPDATE,
            component = LOG_COMPONENT_NOTIFY,
            subsystem = LOG_SUBSYSTEM_CONFIG,
            action = "remove_target",
            target_id = %target_id,
            target_type,
            "Attempting to remove notification target"
        );

        let ttype = target_type.to_lowercase();
        let tname = target_id.id.to_lowercase();

        self.update_config_and_reload(|config| {
            let mut changed = false;
            if let Some(targets_of_type) = config.0.get_mut(&ttype) {
                if targets_of_type.remove(&tname).is_some() {
                    info!(
                            event = EVENT_NOTIFY_CONFIG_UPDATE,
                            component = LOG_COMPONENT_NOTIFY,
                        subsystem = LOG_SUBSYSTEM_CONFIG,
                        action = "remove_target",
                        target_id = %target_id,
                        result = "removed",
                        "notify config update"
                    );
                    changed = true;
                }
                if targets_of_type.is_empty() {
                    config.0.remove(&ttype);
                }
            }
            if !changed {
                debug!(
                    event = EVENT_NOTIFY_CONFIG_UPDATE,
                    component = LOG_COMPONENT_NOTIFY,
                    subsystem = LOG_SUBSYSTEM_CONFIG,
                    action = "remove_target",
                    target_id = %target_id,
                    result = "not_found",
                    "notify config update"
                );
            }
            changed
        })
        .await
    }

    pub async fn set_target_config(&self, target_type: &str, target_name: &str, kvs: KVS) -> Result<(), NotificationError> {
        debug!(
            event = EVENT_NOTIFY_CONFIG_UPDATE,
            component = LOG_COMPONENT_NOTIFY,
            subsystem = LOG_SUBSYSTEM_CONFIG,
            action = "set_target_config",
            target_type,
            target_name,
            "Setting notification target configuration"
        );
        let ttype = target_type.to_lowercase();
        let tname = target_name.to_lowercase();
        self.update_config_and_reload(|config| {
            config.0.entry(ttype.clone()).or_default().insert(tname.clone(), kvs.clone());
            true
        })
        .await
    }

    pub async fn remove_target_config(&self, target_type: &str, target_name: &str) -> Result<(), NotificationError> {
        debug!(
            event = EVENT_NOTIFY_CONFIG_UPDATE,
            component = LOG_COMPONENT_NOTIFY,
            subsystem = LOG_SUBSYSTEM_CONFIG,
            action = "remove_target_config",
            target_type,
            target_name,
            "Removing notification target configuration"
        );

        let ttype = target_type.to_lowercase();
        let tname = target_name.to_lowercase();
        let target_id = runtime_target_id_for_subsystem(&ttype, &tname);

        if self.rule_engine.is_target_bound_to_any_bucket(&target_id).await {
            return Err(NotificationError::Configuration(format!(
                "Target is still bound to bucket rules and deletion is prohibited: type={} name={}",
                ttype, tname
            )));
        }

        self.update_config_and_reload(|config| {
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
                debug!(
                    event = EVENT_NOTIFY_CONFIG_UPDATE,
                    component = LOG_COMPONENT_NOTIFY,
                    subsystem = LOG_SUBSYSTEM_CONFIG,
                    action = "remove_target_config",
                    target_type = %target_type,
                    target_name = %target_name,
                    result = "not_found",
                    "notify config update"
                );
            }
            debug!(
                subsystem_count = config.0.len(),
                "Target config removal processed and configuration summary updated"
            );
            changed
        })
        .await
    }

    pub async fn reload_config(&self, new_config: Config) -> Result<(), NotificationError> {
        info!(
            event = EVENT_NOTIFY_RUNTIME_LIFECYCLE,
            component = LOG_COMPONENT_NOTIFY,
            subsystem = LOG_SUBSYSTEM_CONFIG,
            state = "reloading",
            "notify runtime lifecycle"
        );

        self.update_config(new_config.clone()).await;

        let targets: Vec<Box<dyn Target<Event> + Send + Sync>> = self
            .registry
            .create_targets_from_config(&new_config)
            .await
            .map_err(NotificationError::Target)?;

        info!(
            event = EVENT_NOTIFY_RUNTIME_LIFECYCLE,
            component = LOG_COMPONENT_NOTIFY,
            subsystem = LOG_SUBSYSTEM_CONFIG,
            state = "targets_created",
            target_count = targets.len(),
            "notify runtime lifecycle"
        );
        if targets.is_empty() {
            debug!(
                event = EVENT_NOTIFY_RUNTIME_LIFECYCLE,
                component = LOG_COMPONENT_NOTIFY,
                subsystem = LOG_SUBSYSTEM_CONFIG,
                state = "idle",
                reason = "no_targets_configured",
                hint = %notify_configuration_hint(),
                "notify runtime lifecycle"
            );
        }

        let activation = self.runtime_facade.activate_targets_with_replay(targets).await;
        self.runtime_facade.replace_targets(activation).await?;
        info!(
            event = EVENT_NOTIFY_RUNTIME_LIFECYCLE,
            component = LOG_COMPONENT_NOTIFY,
            subsystem = LOG_SUBSYSTEM_CONFIG,
            state = "reloaded",
            "notify runtime lifecycle"
        );
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
        let Some(new_config) = update_server_config(&mut modifier).await.map_err(|err| match err {
            NotifyConfigStoreError::StorageNotAvailable => NotificationError::StorageNotAvailable(
                "Failed to save target configuration: server storage not initialized".to_string(),
            ),
            NotifyConfigStoreError::Read(err) => NotificationError::ReadConfig(err),
            NotifyConfigStoreError::Save(err) => NotificationError::SaveConfig(err),
        })?
        else {
            debug!(
                event = EVENT_NOTIFY_CONFIG_UPDATE,
                component = LOG_COMPONENT_NOTIFY,
                subsystem = LOG_SUBSYSTEM_CONFIG,
                action = "reload_if_changed",
                result = "unchanged",
                "notify config update"
            );
            return Ok(());
        };

        info!(
            event = EVENT_NOTIFY_CONFIG_UPDATE,
            component = LOG_COMPONENT_NOTIFY,
            subsystem = LOG_SUBSYSTEM_CONFIG,
            action = "reload_if_changed",
            result = "updated",
            "notify config update"
        );
        self.reload_config(new_config).await
    }
}

#[cfg(test)]
mod tests {
    use super::{NotifyConfigManager, NotifyConfigStoreError, runtime_target_id_for_subsystem, serialized_read_modify_write};
    use crate::{
        integration::NotificationMetrics, notifier::EventNotifier, registry::TargetRegistry, rule_engine::NotifyRuleEngine,
        runtime_facade::NotifyRuntimeFacade,
    };
    use rustfs_config::notify::{
        NOTIFY_AMQP_SUB_SYS, NOTIFY_KAFKA_SUB_SYS, NOTIFY_MQTT_SUB_SYS, NOTIFY_NATS_SUB_SYS, NOTIFY_POSTGRES_SUB_SYS,
        NOTIFY_PULSAR_SUB_SYS, NOTIFY_REDIS_SUB_SYS, NOTIFY_WEBHOOK_SUB_SYS,
    };
    use rustfs_config::server_config::{Config, KVS};
    use rustfs_targets::ReplayWorkerManager;
    use std::sync::Arc;
    use tokio::sync::{RwLock, Semaphore};

    fn build_manager() -> NotifyConfigManager {
        let config = Arc::new(RwLock::new(Config::default()));
        let registry = Arc::new(TargetRegistry::new());
        let metrics = Arc::new(NotificationMetrics::new());
        let rule_engine = NotifyRuleEngine::new();
        let notifier = Arc::new(EventNotifier::new(metrics.clone(), rule_engine.clone()));
        let target_list = notifier.target_list();
        let runtime_facade = NotifyRuntimeFacade::new(
            target_list,
            Arc::new(RwLock::new(ReplayWorkerManager::new())),
            Arc::new(Semaphore::new(4)),
            metrics,
        );

        NotifyConfigManager::new(config, registry, rule_engine, runtime_facade)
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

    // Regression test for backlog#968: the read-modify-write over the persisted
    // notify config must be serialized. Many tasks concurrently add a distinct
    // target through the same production RMW path (`serialized_read_modify_write`,
    // which holds the global RMW lock across read→modify→write) against a shared
    // in-memory backend. Every update must survive — no lost updates. Without the
    // lock, concurrent tasks would read the same base config and clobber each
    // other's writes, leaving only a subset of targets.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_config_updates_preserve_all_targets() {
        // Shared in-memory stand-in for the persisted config backend.
        let backend = Arc::new(RwLock::new(Config::default()));
        const TASKS: usize = 32;

        let mut handles = Vec::with_capacity(TASKS);
        for idx in 0..TASKS {
            let backend = backend.clone();
            handles.push(tokio::spawn(async move {
                let ttype = NOTIFY_WEBHOOK_SUB_SYS.to_lowercase();
                let tname = format!("target-{idx}");

                let read_backend = backend.clone();
                let save_backend = backend.clone();

                let result = serialized_read_modify_write(
                    |config: &mut Config| {
                        config
                            .0
                            .entry(ttype.clone())
                            .or_default()
                            .insert(tname.clone(), KVS::default());
                        true
                    },
                    move || async move {
                        let snapshot = read_backend.read().await.clone();
                        // Yield inside the critical section to widen the race window:
                        // if the RMW were not serialized, other tasks would read this
                        // same base snapshot and their writes would clobber ours.
                        tokio::task::yield_now().await;
                        Ok::<_, NotifyConfigStoreError>(snapshot)
                    },
                    move |config: Config| async move {
                        *save_backend.write().await = config;
                        Ok::<_, NotifyConfigStoreError>(())
                    },
                )
                .await
                .expect("serialized RMW should succeed");
                assert!(result.is_some(), "modifier reported a change, expected Some(config)");
            }));
        }

        for handle in handles {
            handle.await.expect("update task should not panic");
        }

        let final_config = backend.read().await;
        let webhook_targets = final_config
            .0
            .get(&NOTIFY_WEBHOOK_SUB_SYS.to_lowercase())
            .expect("webhook subsystem should exist after updates");

        assert_eq!(
            webhook_targets.len(),
            TASKS,
            "all concurrent target additions must be preserved (no lost updates)"
        );
        for idx in 0..TASKS {
            let tname = format!("target-{idx}");
            assert!(webhook_targets.contains_key(&tname), "missing target {tname}: concurrent update was lost");
        }
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
