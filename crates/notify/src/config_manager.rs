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
    NotificationError,
    lifecycle::{NotificationRuntimeState, NotifyLifecycleCoordinator},
    registry::TargetRegistry,
    resolve_notify_object_store_handle,
    rule_engine::NotifyRuleEngine,
    runtime_facade::NotifyRuntimeFacade,
    with_notify_server_config_read_lock,
};
use rustfs_config::notify::{
    NOTIFY_AMQP_SUB_SYS, NOTIFY_KAFKA_SUB_SYS, NOTIFY_MQTT_SUB_SYS, NOTIFY_MYSQL_SUB_SYS, NOTIFY_NATS_SUB_SYS,
    NOTIFY_POSTGRES_SUB_SYS, NOTIFY_PULSAR_SUB_SYS, NOTIFY_REDIS_SUB_SYS, NOTIFY_WEBHOOK_SUB_SYS,
};
use rustfs_config::server_config::{Config, KVS};
use rustfs_targets::arn::TargetID;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info};

const LOG_COMPONENT_NOTIFY: &str = "notify";
const LOG_SUBSYSTEM_CONFIG: &str = "config";
const EVENT_NOTIFY_CONFIG_UPDATE: &str = "notify_config_update";

#[derive(Debug)]
enum NotifyConfigStoreError {
    Lock(String),
    StorageNotAvailable,
    Read(String),
    Save(String),
    Converge { persisted: bool, error: String },
    Conflict,
}

fn notification_convergence_error(persisted: bool, error: impl std::fmt::Display) -> NotificationError {
    NotificationError::ConfigurationConvergence {
        persisted,
        detail: error.to_string(),
    }
}

async fn supervise_config_update<T>(
    mutation: impl std::future::Future<Output = Result<T, NotifyConfigStoreError>> + Send + 'static,
) -> Result<T, NotifyConfigStoreError>
where
    T: Send + 'static,
{
    tokio::spawn(mutation).await.map_err(|error| {
        let outcome = if error.is_cancelled() { "cancelled" } else { "panicked" };
        NotifyConfigStoreError::Lock(format!("notify config update task {outcome}"))
    })?
}

fn configs_equal(first: Option<&KVS>, second: Option<&KVS>) -> bool {
    match (first, second) {
        (None, None) => true,
        (Some(first), Some(second)) => {
            if first == second {
                return true;
            }
            let mut first: Vec<_> = first.0.iter().collect();
            let mut second: Vec<_> = second.0.iter().collect();
            // Lookup uses the first occurrence of a key, so preserve duplicate-key order.
            first.sort_by(|a, b| a.key.cmp(&b.key));
            second.sort_by(|a, b| a.key.cmp(&b.key));
            first == second
        }
        _ => false,
    }
}

fn replace_target_if_unchanged(
    config: &mut Config,
    target_type: &str,
    target_name: &str,
    expected: Option<&KVS>,
    expected_default: Option<&KVS>,
    kvs: &KVS,
) -> Result<bool, NotifyConfigStoreError> {
    let targets = config.0.get(target_type);
    let mut matching = targets
        .into_iter()
        .flat_map(|targets| targets.iter())
        .filter(|(name, _)| name.eq_ignore_ascii_case(target_name));
    let current = matching.next();
    if matching.next().is_some() {
        return Err(NotifyConfigStoreError::Conflict);
    }
    let stored_name = current
        .map(|(name, _)| name.clone())
        .unwrap_or_else(|| target_name.to_owned());
    let current_default = targets.and_then(|targets| targets.get(rustfs_config::DEFAULT_DELIMITER));
    if !configs_equal(current.map(|(_, kvs)| kvs), expected) || !configs_equal(current_default, expected_default) {
        return Err(NotifyConfigStoreError::Conflict);
    }
    config
        .0
        .entry(target_type.to_owned())
        .or_default()
        .insert(stored_name, kvs.clone());
    Ok(true)
}

async fn update_server_config<F>(
    mut modifier: F,
    lifecycle: NotifyLifecycleCoordinator,
) -> Result<Option<(crate::lifecycle::NotificationLifecycleTransition, bool)>, NotifyConfigStoreError>
where
    F: FnMut(&mut Config, Option<String>) -> Result<bool, NotifyConfigStoreError> + Send + 'static,
{
    let Some(store) = resolve_notify_object_store_handle() else {
        return Err(NotifyConfigStoreError::StorageNotAvailable);
    };

    supervise_config_update(async move {
        let snapshot = crate::read_notify_server_config_snapshot(store.clone())
            .await
            .map_err(NotifyConfigStoreError::Read)?;
        let mut config = snapshot.config.clone();
        let generation = snapshot.generation().map(|value| value.to_string());
        if !modifier(&mut config, generation)? {
            return Ok(None);
        }

        let persisted = crate::save_notify_server_config_snapshot(store.clone(), &config, &snapshot)
            .await
            .map_err(NotifyConfigStoreError::Save)?;
        drop(snapshot);

        let read_store = store.clone();
        with_notify_server_config_read_lock(store, move || async move {
            let latest = crate::read_existing_notify_server_config_no_lock(read_store)
                .await
                .map_err(|error| NotifyConfigStoreError::Converge { persisted, error })?;
            Ok::<_, NotifyConfigStoreError>(Some((lifecycle.update_config(latest), persisted)))
        })
        .await
        .map_err(|error| NotifyConfigStoreError::Converge { persisted, error })?
    })
    .await
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
    lifecycle: NotifyLifecycleCoordinator,
    rule_engine: NotifyRuleEngine,
}

impl NotifyConfigManager {
    pub fn new(
        config: Arc<RwLock<Config>>,
        registry: Arc<TargetRegistry>,
        rule_engine: NotifyRuleEngine,
        runtime_facade: NotifyRuntimeFacade,
    ) -> Self {
        let lifecycle = NotifyLifecycleCoordinator::new(config.clone(), registry, runtime_facade);
        Self {
            config,
            lifecycle,
            rule_engine,
        }
    }

    pub(crate) fn lifecycle(&self) -> NotifyLifecycleCoordinator {
        self.lifecycle.clone()
    }

    pub async fn init(&self) -> Result<(), NotificationError> {
        let config = self.config.read().await.clone();
        self.lifecycle.set_mode(true, Some(config)).wait().await
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
        let log_target_id = target_id.clone();

        // Guard against orphaning bucket notification rules (backlog#979). Removing a
        // target while a bucket rule still references it would leave a dangling
        // binding whose events can never be delivered. This mirrors the symmetric
        // guard already applied in `remove_target_config`: refuse the removal while
        // the target is still bound so the caller unbinds the bucket rules first.
        let bound_target_id = runtime_target_id_for_subsystem(&ttype, &tname);
        if self.rule_engine.is_target_bound_to_any_bucket(&bound_target_id).await {
            return Err(NotificationError::Configuration(format!(
                "Target is still bound to bucket rules and deletion is prohibited: type={ttype} name={tname}"
            )));
        }

        self.update_config_and_reload(move |config| {
            let mut changed = false;
            if let Some(targets_of_type) = config.0.get_mut(&ttype) {
                if targets_of_type.remove(&tname).is_some() {
                    info!(
                            event = EVENT_NOTIFY_CONFIG_UPDATE,
                            component = LOG_COMPONENT_NOTIFY,
                        subsystem = LOG_SUBSYSTEM_CONFIG,
                        action = "remove_target",
                        target_id = %log_target_id,
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
                    target_id = %log_target_id,
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
        self.update_config_and_reload(move |config| {
            config.0.entry(ttype.clone()).or_default().insert(tname.clone(), kvs.clone());
            true
        })
        .await
    }

    /// Replace a target only when its persisted configuration is unchanged since validation.
    pub async fn set_target_config_if_unchanged(
        &self,
        target_type: &str,
        target_name: &str,
        expected: Option<KVS>,
        expected_default: Option<KVS>,
        expected_generation: Option<String>,
        kvs: KVS,
    ) -> Result<(), NotificationError> {
        let ttype = target_type.to_lowercase();
        let tname = target_name.to_lowercase();
        self.update_config_and_reload_checked(move |config, generation| {
            if expected_generation
                .as_ref()
                .is_some_and(|expected| generation.as_ref() != Some(expected))
            {
                return Err(NotifyConfigStoreError::Conflict);
            }
            replace_target_if_unchanged(config, &ttype, &tname, expected.as_ref(), expected_default.as_ref(), &kvs)
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

        self.update_config_and_reload(move |config| {
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
                    target_type = %ttype,
                    target_name = %tname,
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
        self.lifecycle.set_mode(true, Some(new_config)).wait().await
    }

    pub async fn reload_persisted_config(&self) -> Result<(), NotificationError> {
        let Some(store) = resolve_notify_object_store_handle() else {
            return Err(NotificationError::StorageNotAvailable(
                "Failed to load target configuration: server storage not initialized".to_string(),
            ));
        };
        self.reload_persisted_config_from_store(store).await
    }

    pub async fn reload_persisted_config_from_store(&self, store: Arc<crate::NotifyStore>) -> Result<(), NotificationError> {
        if self.lifecycle.state() == NotificationRuntimeState::Terminated {
            return Err(NotificationError::Initialization("Notification runtime has terminated".to_string()));
        }

        let read_store = store.clone();
        let config_cache = self.config.clone();
        let lifecycle = self.lifecycle.clone();
        let transition = with_notify_server_config_read_lock(store, move || async move {
            let config = crate::read_existing_notify_server_config_no_lock(read_store)
                .await
                .map_err(NotificationError::ReadConfig)?;
            Ok::<_, NotificationError>(if *config_cache.read().await == config && lifecycle.is_converged() {
                None
            } else {
                Some(lifecycle.update_config(config))
            })
        })
        .await
        .map_err(NotificationError::StorageNotAvailable)??;

        if let Some(transition) = transition {
            transition.wait().await?;
        }
        Ok(())
    }

    async fn update_config_and_reload<F>(&self, mut modifier: F) -> Result<(), NotificationError>
    where
        F: FnMut(&mut Config) -> bool + Send + 'static,
    {
        self.update_config_and_reload_checked(move |config, _generation| Ok(modifier(config)))
            .await
    }

    async fn update_config_and_reload_checked<F>(&self, modifier: F) -> Result<(), NotificationError>
    where
        F: FnMut(&mut Config, Option<String>) -> Result<bool, NotifyConfigStoreError> + Send + 'static,
    {
        if self.lifecycle.state() == NotificationRuntimeState::Terminated {
            return Err(NotificationError::Initialization("Notification runtime has terminated".to_string()));
        }
        let Some((transition, persisted)) =
            update_server_config(modifier, self.lifecycle.clone())
                .await
                .map_err(|err| match err {
                    NotifyConfigStoreError::Lock(err) => NotificationError::StorageNotAvailable(err),
                    NotifyConfigStoreError::StorageNotAvailable => NotificationError::StorageNotAvailable(
                        "Failed to save target configuration: server storage not initialized".to_string(),
                    ),
                    NotifyConfigStoreError::Read(err) => NotificationError::ReadConfig(err),
                    NotifyConfigStoreError::Save(err) => NotificationError::SaveConfig(err),
                    NotifyConfigStoreError::Converge { persisted, error } => notification_convergence_error(persisted, error),
                    NotifyConfigStoreError::Conflict => NotificationError::ConfigurationConflict,
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

        let result = if persisted { "updated" } else { "unchanged" };
        info!(
            event = EVENT_NOTIFY_CONFIG_UPDATE,
            component = LOG_COMPONENT_NOTIFY,
            subsystem = LOG_SUBSYSTEM_CONFIG,
            action = "reload_if_changed",
            result,
            "notify config update"
        );
        transition
            .wait()
            .await
            .map_err(|err| notification_convergence_error(persisted, err))
    }
}

#[cfg(test)]
mod tests {
    use super::{
        NotifyConfigManager, NotifyConfigStoreError, notification_convergence_error, runtime_target_id_for_subsystem,
        supervise_config_update,
    };
    use crate::rules::RulesMap;
    use crate::{NotificationError, NotificationRuntimeState};

    #[test]
    fn config_comparison_preserves_duplicates_and_hidden_flags() {
        use rustfs_config::server_config::{KV, KVS};
        let field = |key: &str, value: &str, hidden_if_empty: bool| KV {
            key: key.into(),
            value: value.into(),
            hidden_if_empty,
        };
        let original = KVS(vec![
            field("password", "secret", false),
            field("token", "", true),
            field("password", "secret", false),
        ]);
        let reordered = KVS(vec![
            field("password", "secret", false),
            field("password", "secret", false),
            field("token", "", true),
        ]);
        let missing_duplicate = KVS(vec![field("password", "secret", false), field("token", "", true)]);
        let changed_flag = KVS(vec![
            field("password", "secret", false),
            field("password", "secret", false),
            field("token", "", false),
        ]);
        assert!(super::configs_equal(Some(&original), Some(&reordered)));
        assert!(!super::configs_equal(Some(&original), Some(&missing_duplicate)));
        assert!(!super::configs_equal(Some(&original), Some(&changed_flag)));
        assert!(!super::configs_equal(Some(&original), None));
        let first_value = KVS(vec![
            field("password", "old", false),
            field("token", "", true),
            field("password", "new", false),
        ]);
        let reordered_keys = KVS(vec![
            field("token", "", true),
            field("password", "old", false),
            field("password", "new", false),
        ]);
        let reordered_duplicates = KVS(vec![
            field("token", "", true),
            field("password", "new", false),
            field("password", "old", false),
        ]);
        assert!(super::configs_equal(Some(&first_value), Some(&reordered_keys)));
        assert!(!super::configs_equal(Some(&first_value), Some(&reordered_duplicates)));
    }

    #[test]
    fn conditional_target_update_rejects_rotation_and_deletion() {
        use rustfs_config::server_config::{Config, KVS};
        let mut old = KVS::new();
        old.insert("password".into(), "old".into());
        let mut rotated = KVS::new();
        rotated.insert("password".into(), "new".into());
        let mut config = Config::new();
        config
            .0
            .entry("notify_nats".into())
            .or_default()
            .insert("primary".into(), rotated.clone());
        let result = super::replace_target_if_unchanged(&mut config, "notify_nats", "primary", Some(&old), None, &old);
        assert!(matches!(result, Err(super::NotifyConfigStoreError::Conflict)));
        assert_eq!(config.0["notify_nats"]["primary"], rotated);
        config.0.get_mut("notify_nats").expect("subsystem exists").remove("primary");
        assert!(matches!(
            super::replace_target_if_unchanged(&mut config, "notify_nats", "primary", Some(&old), None, &old),
            Err(super::NotifyConfigStoreError::Conflict)
        ));
        assert!(
            super::replace_target_if_unchanged(&mut config, "notify_nats", "primary", None, None, &old)
                .expect("create unchanged target")
        );
        assert!(
            super::replace_target_if_unchanged(&mut config, "notify_nats", "primary", Some(&old), None, &rotated)
                .expect("replace unchanged target")
        );
        assert_eq!(config.0["notify_nats"]["primary"], rotated);

        let mut first = KVS::new();
        first.insert("url".into(), "nats://example.com".into());
        first.insert("token".into(), "secret".into());
        let mut reordered = KVS::new();
        reordered.insert("token".into(), "secret".into());
        reordered.insert("url".into(), "nats://example.com".into());
        config
            .0
            .get_mut("notify_nats")
            .expect("subsystem")
            .insert("primary".into(), first);
        assert!(
            super::replace_target_if_unchanged(&mut config, "notify_nats", "primary", Some(&reordered), None, &rotated)
                .expect("key order does not signal a change")
        );
        let targets = config.0.get_mut("notify_nats").expect("subsystem");
        targets.remove("primary");
        targets.insert("Primary".into(), rotated.clone());
        assert!(
            super::replace_target_if_unchanged(&mut config, "notify_nats", "primary", Some(&rotated), None, &old)
                .expect("case-insensitive match")
        );
        let targets = &config.0["notify_nats"];
        assert!(targets.contains_key("Primary"));
        assert!(!targets.contains_key("primary"));
        let mut old_default = KVS::new();
        old_default.insert("password".into(), "old-default".into());
        let mut new_default = KVS::new();
        new_default.insert("password".into(), "new-default".into());
        config
            .0
            .get_mut("notify_nats")
            .expect("subsystem")
            .insert("_".into(), new_default.clone());
        assert!(matches!(
            super::replace_target_if_unchanged(&mut config, "notify_nats", "primary", Some(&old), Some(&old_default), &rotated),
            Err(super::NotifyConfigStoreError::Conflict)
        ));
        assert_eq!(config.0["notify_nats"]["Primary"], old);
        assert_eq!(config.0["notify_nats"]["_"], new_default);
    }

    #[tokio::test]
    async fn supervised_config_update_survives_waiter_cancellation() {
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let (completed_tx, completed_rx) = tokio::sync::oneshot::channel();

        let waiter = tokio::spawn(async move {
            supervise_config_update(async move {
                let _ = started_tx.send(());
                let _ = release_rx.await;
                let _ = completed_tx.send(());
                Ok::<_, NotifyConfigStoreError>(())
            })
            .await
        });

        started_rx.await.expect("config update should start");
        waiter.abort();
        release_tx.send(()).expect("config update should be released");
        let completed = tokio::time::timeout(std::time::Duration::from_secs(30), completed_rx).await;
        assert!(matches!(completed, Ok(Ok(()))), "detached config update should complete");
    }
    use crate::{
        integration::NotificationMetrics, notifier::EventNotifier, registry::TargetRegistry, rule_engine::NotifyRuleEngine,
        runtime_facade::NotifyRuntimeFacade,
    };
    use rustfs_config::notify::{
        NOTIFY_AMQP_SUB_SYS, NOTIFY_KAFKA_SUB_SYS, NOTIFY_MQTT_SUB_SYS, NOTIFY_NATS_SUB_SYS, NOTIFY_POSTGRES_SUB_SYS,
        NOTIFY_PULSAR_SUB_SYS, NOTIFY_REDIS_SUB_SYS, NOTIFY_WEBHOOK_SUB_SYS,
    };
    use rustfs_config::server_config::Config;
    use rustfs_s3_types::EventName;
    use rustfs_targets::ReplayWorkerManager;
    use rustfs_targets::arn::TargetID;
    use std::sync::Arc;
    use tokio::sync::{RwLock, Semaphore};

    fn build_manager() -> NotifyConfigManager {
        let config = Arc::new(RwLock::new(Config::default()));
        let registry = Arc::new(TargetRegistry::new());
        let metrics = Arc::new(NotificationMetrics::new());
        let rule_engine = NotifyRuleEngine::new();
        let notifier = Arc::new(EventNotifier::new(metrics.clone(), rule_engine.clone()));
        let target_list = notifier.target_list();
        let runtime_facade = NotifyRuntimeFacade::new_with_dispatch_gate(
            target_list,
            Arc::new(RwLock::new(ReplayWorkerManager::new())),
            notifier.dispatch_gate(),
            Arc::new(Semaphore::new(4)),
            metrics,
        );

        NotifyConfigManager::new(config, registry, rule_engine, runtime_facade)
    }

    // Regression test for backlog#979 (b): `remove_target` must refuse to remove a
    // target that is still referenced by a bucket notification rule, otherwise the
    // rule is left orphaned (pointing at a target that no longer exists). This
    // mirrors the guard `remove_target_config` already enforces. The guard runs
    // before the persisted config read-modify-write, so it returns the refusal
    // without needing a live object store.
    #[tokio::test]
    async fn remove_target_refuses_when_bound_to_bucket_rules() {
        let manager = build_manager();

        // Bind the runtime target id (webhook/primary) that `remove_target` derives
        // from (`NOTIFY_WEBHOOK_SUB_SYS`, id="primary") to a bucket rule.
        let bound_id = TargetID::new("primary".to_string(), "webhook".to_string());
        let mut rules_map = RulesMap::new();
        rules_map.add_rule_config(&[EventName::ObjectCreatedPut], "*".to_string(), bound_id.clone());
        manager.rule_engine.set_bucket_rules("bucket", rules_map).await;

        let err = manager
            .remove_target(&bound_id, NOTIFY_WEBHOOK_SUB_SYS)
            .await
            .expect_err("removing a bound target must be refused to avoid orphaning bucket rules");
        assert!(
            matches!(err, NotificationError::Configuration(_)),
            "expected a Configuration refusal, got {err:?}"
        );
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
        assert!(matches!(manager.lifecycle().state(), NotificationRuntimeState::TargetsEnabled { .. }));
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

    #[test]
    fn convergence_error_reports_durable_write_state() {
        for persisted in [true, false] {
            let error = notification_convergence_error(persisted, "injected failure");
            assert!(matches!(error, NotificationError::ConfigurationConvergence { persisted: value, .. } if value == persisted));
        }
    }
}
