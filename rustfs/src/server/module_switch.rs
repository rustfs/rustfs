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

use super::runtime_sources;
use crate::storage_api::server::module_switch::{
    Error as StorageError, read_config, read_config_no_lock, save_config_no_lock, with_config_object_read_lock,
    with_config_object_write_lock,
};
use crate::storage_api::server::runtime_sources::ECStore;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::sync::LazyLock;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::Mutex;

const MODULE_SWITCH_CONFIG_PATH: &str = "config/module_switches.json";
pub(crate) const MODULE_SWITCHES_SIGNAL_SUBSYSTEM: &str = "module_switches";

// Keep a cheap in-process snapshot so hot-path checks do not need to read
// cluster metadata after startup or console-triggered refresh.
static PERSISTED_NOTIFY_MODULE_ENABLED: AtomicBool = AtomicBool::new(rustfs_config::DEFAULT_NOTIFY_ENABLE);
static PERSISTED_AUDIT_MODULE_ENABLED: AtomicBool = AtomicBool::new(rustfs_config::DEFAULT_AUDIT_ENABLE);
static PERSISTED_MODULE_SWITCH_CONFIGURED: AtomicBool = AtomicBool::new(false);
static MODULE_SWITCH_RMW_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

async fn serialize_module_switch_rmw<F, Fut, T>(operation: F) -> T
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = T>,
{
    let _rmw_guard = MODULE_SWITCH_RMW_LOCK.lock().await;
    operation().await
}

#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq, Serialize)]
pub(crate) struct PersistedModuleSwitches {
    pub(crate) notify_enabled: bool,
    pub(crate) audit_enabled: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum ModuleSwitchSource {
    Env,
    Console,
    Default,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ModuleSwitchResolution {
    pub(crate) enabled: bool,
    pub(crate) source: ModuleSwitchSource,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct ModuleSwitchSnapshot {
    pub(crate) notify_enabled: bool,
    pub(crate) audit_enabled: bool,
    pub(crate) persisted_notify_enabled: bool,
    pub(crate) persisted_audit_enabled: bool,
    pub(crate) notify_source: ModuleSwitchSource,
    pub(crate) audit_source: ModuleSwitchSource,
}

pub(crate) fn current_persisted_module_switches() -> PersistedModuleSwitches {
    PersistedModuleSwitches {
        notify_enabled: PERSISTED_NOTIFY_MODULE_ENABLED.load(Ordering::Relaxed),
        audit_enabled: PERSISTED_AUDIT_MODULE_ENABLED.load(Ordering::Relaxed),
    }
}

fn persisted_module_switches_configured() -> bool {
    PERSISTED_MODULE_SWITCH_CONFIGURED.load(Ordering::Relaxed)
}

pub(crate) fn set_persisted_module_switches(config: PersistedModuleSwitches, configured: bool) {
    PERSISTED_NOTIFY_MODULE_ENABLED.store(config.notify_enabled, Ordering::Relaxed);
    PERSISTED_AUDIT_MODULE_ENABLED.store(config.audit_enabled, Ordering::Relaxed);
    PERSISTED_MODULE_SWITCH_CONFIGURED.store(configured, Ordering::Relaxed);
}

fn env_override_exists(key: &str) -> bool {
    std::env::var_os(key).is_some()
}

fn env_override_value(key: &str) -> Option<bool> {
    rustfs_utils::get_env_opt_bool(key)
}

pub(crate) fn validate_notify_module_env() -> Result<(), String> {
    let key = rustfs_config::ENV_NOTIFY_ENABLE;
    if env_override_exists(key) && env_override_value(key).is_none() {
        return Err(format!("{key} is not a valid boolean"));
    }
    Ok(())
}

fn effective_module_switch_state(env_key: &str, persisted_enabled: bool, default_enabled: bool) -> ModuleSwitchResolution {
    // Explicit env remains the highest-priority source so process-level bootstrap
    // cannot be silently overridden by a later console write.
    if let Some(env_enabled) = env_override_value(env_key) {
        return ModuleSwitchResolution {
            enabled: env_enabled,
            source: ModuleSwitchSource::Env,
        };
    }

    if persisted_module_switches_configured() {
        return ModuleSwitchResolution {
            enabled: persisted_enabled,
            source: ModuleSwitchSource::Console,
        };
    }

    ModuleSwitchResolution {
        enabled: default_enabled,
        source: ModuleSwitchSource::Default,
    }
}

pub(crate) fn resolve_notify_module_state() -> ModuleSwitchResolution {
    effective_module_switch_state(
        rustfs_config::ENV_NOTIFY_ENABLE,
        PERSISTED_NOTIFY_MODULE_ENABLED.load(Ordering::Relaxed),
        rustfs_config::DEFAULT_NOTIFY_ENABLE,
    )
}

pub(crate) fn resolve_audit_module_state() -> ModuleSwitchResolution {
    effective_module_switch_state(
        rustfs_config::ENV_AUDIT_ENABLE,
        PERSISTED_AUDIT_MODULE_ENABLED.load(Ordering::Relaxed),
        rustfs_config::DEFAULT_AUDIT_ENABLE,
    )
}

pub(crate) fn current_module_switch_snapshot() -> ModuleSwitchSnapshot {
    let persisted = current_persisted_module_switches();
    let notify = resolve_notify_module_state();
    let audit = resolve_audit_module_state();

    ModuleSwitchSnapshot {
        notify_enabled: notify.enabled,
        audit_enabled: audit.enabled,
        persisted_notify_enabled: persisted.notify_enabled,
        persisted_audit_enabled: persisted.audit_enabled,
        notify_source: notify.source,
        audit_source: audit.source,
    }
}

fn validate_env_override_for_request(env_key: &str, requested: bool, label: &str) -> Result<(), String> {
    if !env_override_exists(env_key) {
        return Ok(());
    }

    match env_override_value(env_key) {
        // Matching values are safe: we still persist the console value, but the
        // effective runtime source remains env until the operator changes it.
        Some(value) if value == requested => Ok(()),
        Some(value) => Err(format!(
            "{label} is managed by environment variable {env_key}={value}; update the environment value first, then use the console to refresh the module switch state"
        )),
        None => Err(format!(
            "{label} is managed by environment variable {env_key}, but its value is not a valid boolean; fix the environment value first, then use the console to refresh the module switch state"
        )),
    }
}

pub(crate) fn validate_module_switch_update(requested: PersistedModuleSwitches) -> Result<(), String> {
    validate_env_override_for_request(rustfs_config::ENV_NOTIFY_ENABLE, requested.notify_enabled, "notify module")?;
    validate_env_override_for_request(rustfs_config::ENV_AUDIT_ENABLE, requested.audit_enabled, "audit module")?;
    Ok(())
}

pub(crate) async fn refresh_persisted_module_switches_from_store() -> Result<PersistedModuleSwitches, String> {
    let Some(store) = runtime_sources::current_object_store_handle() else {
        return Err("storage layer not initialized".to_string());
    };
    refresh_persisted_module_switches_from(store).await
}

pub(crate) async fn refresh_persisted_module_switches_from(store: Arc<ECStore>) -> Result<PersistedModuleSwitches, String> {
    refresh_persisted_module_switches_with(|| async move {
        match read_config(store, MODULE_SWITCH_CONFIG_PATH).await {
            Ok(data) => Ok(Some(data)),
            Err(StorageError::ConfigNotFound) => Ok(None),
            Err(err) => Err(format!("failed to load module switch config: {err}")),
        }
    })
    .await
}

pub(crate) async fn with_refreshed_notify_module_state_from<T, Publish, PublishFuture>(
    store: Arc<ECStore>,
    publish: Publish,
) -> Result<T, String>
where
    Publish: FnOnce(ModuleSwitchResolution) -> PublishFuture + Send + 'static,
    PublishFuture: std::future::Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    // Lock order: local module RMW -> distributed module-object read -> the
    // callback's server-config read lock. Server-config writers never acquire
    // the module-object lock, and module writers release any server-config
    // read before taking the module-object write lock.
    serialize_module_switch_rmw(|| async move {
        let read_store = store.clone();
        with_config_object_read_lock(store, MODULE_SWITCH_CONFIG_PATH.to_string(), move || async move {
            let persisted = match read_config_no_lock(read_store, MODULE_SWITCH_CONFIG_PATH).await {
                Ok(data) => Some(data),
                Err(StorageError::ConfigNotFound) => None,
                Err(err) => return Err(format!("failed to load module switch config: {err}")),
            };
            let config = decode_persisted_module_switches(persisted)?;
            set_persisted_module_switches(config.0, config.1);
            Ok(publish(resolve_notify_module_state()).await)
        })
        .await
        .map_err(|err| format!("failed to lock module switch refresh: {err}"))?
    })
    .await
}

fn decode_persisted_module_switches(data: Option<Vec<u8>>) -> Result<(PersistedModuleSwitches, bool), String> {
    match data {
        Some(data) => Ok((
            serde_json::from_slice::<PersistedModuleSwitches>(&data)
                .map_err(|e| format!("failed to deserialize module switch config: {e}"))?,
            true,
        )),
        None => Ok((PersistedModuleSwitches::default(), false)),
    }
}

async fn refresh_persisted_module_switches_with<F, Fut>(read: F) -> Result<PersistedModuleSwitches, String>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = Result<Option<Vec<u8>>, String>>,
{
    serialize_module_switch_rmw(|| async move {
        let (config, configured) = decode_persisted_module_switches(read().await?)?;

        // Track whether the persisted file exists so the effective state can
        // distinguish "console configured false" from "never configured, use default".
        set_persisted_module_switches(config, configured);
        Ok(config)
    })
    .await
}

pub(crate) async fn save_persisted_module_switches_to<T, F>(
    store: Arc<ECStore>,
    config: PersistedModuleSwitches,
    publish: F,
) -> Result<T, String>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    // Lock order matches refresh_persisted_module_switches_from: local RMW
    // guard before the namespace lock. Taking these in the opposite order lets
    // a reader hold the local guard while waiting for a writer that is itself
    // waiting for the local guard.
    serialize_module_switch_rmw(|| async move {
        let save_store = store.clone();
        with_config_object_write_lock(store, MODULE_SWITCH_CONFIG_PATH.to_string(), move || async move {
            save_persisted_module_switches_inner(
                config,
                move |data| async move {
                    save_config_no_lock(save_store, MODULE_SWITCH_CONFIG_PATH, data)
                        .await
                        .map_err(|e| format!("failed to save module switch config: {e}"))
                },
                publish,
            )
            .await
        })
        .await
        .map_err(|err| format!("failed to lock module switch update: {err}"))?
    })
    .await
}

#[cfg(test)]
async fn save_persisted_module_switches_with<T, F, Save, SaveFuture>(
    config: PersistedModuleSwitches,
    save: Save,
    publish: F,
) -> Result<T, String>
where
    F: FnOnce() -> T,
    Save: FnOnce(Vec<u8>) -> SaveFuture,
    SaveFuture: std::future::Future<Output = Result<(), String>>,
{
    serialize_module_switch_rmw(|| save_persisted_module_switches_inner(config, save, publish)).await
}

async fn save_persisted_module_switches_inner<T, F, Save, SaveFuture>(
    config: PersistedModuleSwitches,
    save: Save,
    publish: F,
) -> Result<T, String>
where
    F: FnOnce() -> T,
    Save: FnOnce(Vec<u8>) -> SaveFuture,
    SaveFuture: std::future::Future<Output = Result<(), String>>,
{
    let data = serde_json::to_vec(&config).map_err(|e| format!("failed to serialize module switch config: {e}"))?;
    save(data).await?;

    set_persisted_module_switches(config, true);
    Ok(publish())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage_api::startup::storage::{EndpointServerPools, init_local_disks_with_instance_ctx, new_instance_ctx};
    use serial_test::serial;
    use std::future::{Future, poll_fn};
    use std::sync::{
        Arc, Mutex as StdMutex,
        atomic::{AtomicBool, Ordering as AtomicOrdering},
    };
    use std::task::Poll;
    use temp_env::{with_var, with_vars};
    use tempfile::TempDir;
    use tokio::sync::{Notify, oneshot};
    use tokio_util::sync::CancellationToken;

    #[test]
    #[serial]
    fn resolve_module_switch_state_prefers_env_override() {
        set_persisted_module_switches(
            PersistedModuleSwitches {
                notify_enabled: false,
                audit_enabled: false,
            },
            true,
        );

        with_vars(
            [
                (rustfs_config::ENV_NOTIFY_ENABLE, Some("true")),
                (rustfs_config::ENV_AUDIT_ENABLE, Some("false")),
            ],
            || {
                let notify = resolve_notify_module_state();
                let audit = resolve_audit_module_state();

                assert!(notify.enabled);
                assert_eq!(notify.source, ModuleSwitchSource::Env);
                assert!(!audit.enabled);
                assert_eq!(audit.source, ModuleSwitchSource::Env);
            },
        );
    }

    #[test]
    #[serial]
    fn resolve_module_switch_state_falls_back_to_console_value() {
        set_persisted_module_switches(
            PersistedModuleSwitches {
                notify_enabled: true,
                audit_enabled: false,
            },
            true,
        );

        with_vars(
            [
                (rustfs_config::ENV_NOTIFY_ENABLE, None::<&str>),
                (rustfs_config::ENV_AUDIT_ENABLE, None::<&str>),
            ],
            || {
                let notify = resolve_notify_module_state();
                let audit = resolve_audit_module_state();

                assert!(notify.enabled);
                assert_eq!(notify.source, ModuleSwitchSource::Console);
                assert!(!audit.enabled);
                assert_eq!(audit.source, ModuleSwitchSource::Console);
            },
        );
    }

    #[test]
    #[serial]
    fn current_module_switch_snapshot_uses_defaults_when_persisted_file_is_absent() {
        set_persisted_module_switches(
            PersistedModuleSwitches {
                notify_enabled: false,
                audit_enabled: false,
            },
            false,
        );

        with_vars(
            [
                (rustfs_config::ENV_NOTIFY_ENABLE, None::<&str>),
                (rustfs_config::ENV_AUDIT_ENABLE, None::<&str>),
            ],
            || {
                let snapshot = current_module_switch_snapshot();

                assert_eq!(snapshot.notify_enabled, rustfs_config::DEFAULT_NOTIFY_ENABLE);
                assert_eq!(snapshot.audit_enabled, rustfs_config::DEFAULT_AUDIT_ENABLE);
                assert!(!snapshot.persisted_notify_enabled);
                assert!(!snapshot.persisted_audit_enabled);
                assert_eq!(snapshot.notify_source, ModuleSwitchSource::Default);
                assert_eq!(snapshot.audit_source, ModuleSwitchSource::Default);
            },
        );
    }

    #[test]
    #[serial]
    fn validate_module_switch_update_rejects_env_conflict() {
        with_var(rustfs_config::ENV_NOTIFY_ENABLE, Some("true"), || {
            let err = validate_module_switch_update(PersistedModuleSwitches {
                notify_enabled: false,
                audit_enabled: false,
            })
            .unwrap_err();

            assert!(err.contains(rustfs_config::ENV_NOTIFY_ENABLE));
            assert!(err.contains("update the environment value first"));
        });
    }

    #[test]
    #[serial]
    fn invalid_notify_env_is_rejected_before_runtime_reconcile() {
        with_var(rustfs_config::ENV_NOTIFY_ENABLE, Some("invalid"), || {
            let err = validate_notify_module_env().expect_err("invalid notify env must fail closed");
            assert!(err.contains(rustfs_config::ENV_NOTIFY_ENABLE));
        });
    }

    #[test]
    #[serial]
    fn validate_module_switch_update_allows_matching_env_override() {
        with_vars(
            [
                (rustfs_config::ENV_NOTIFY_ENABLE, Some("true")),
                (rustfs_config::ENV_AUDIT_ENABLE, Some("false")),
            ],
            || {
                validate_module_switch_update(PersistedModuleSwitches {
                    notify_enabled: true,
                    audit_enabled: false,
                })
                .expect("matching env override should be accepted");
            },
        );
    }

    #[test]
    #[serial]
    fn validate_module_switch_update_rejects_invalid_env_override() {
        with_var(rustfs_config::ENV_AUDIT_ENABLE, Some("invalid"), || {
            let err = validate_module_switch_update(PersistedModuleSwitches {
                notify_enabled: false,
                audit_enabled: true,
            })
            .unwrap_err();

            assert!(err.contains("not a valid boolean"));
        });
    }

    #[tokio::test]
    #[serial]
    async fn save_publish_and_refresh_share_the_rmw_lock() {
        let previous = current_persisted_module_switches();
        let previous_configured = persisted_module_switches_configured();

        save_persisted_module_switches_with(
            PersistedModuleSwitches {
                notify_enabled: false,
                audit_enabled: true,
            },
            |_| async { Ok(()) },
            || {
                assert!(MODULE_SWITCH_RMW_LOCK.try_lock().is_err(), "publish must run while the RMW lock is held");
            },
        )
        .await
        .expect("save and publish should succeed");

        let events = Arc::new(StdMutex::new(Vec::new()));
        let release_save = Arc::new(Notify::new());
        let (save_started_tx, save_started_rx) = oneshot::channel();

        let save_events = events.clone();
        let publish_events = events.clone();
        let save_release = release_save.clone();
        let save_task = tokio::spawn(async move {
            save_persisted_module_switches_with(
                PersistedModuleSwitches {
                    notify_enabled: true,
                    audit_enabled: false,
                },
                move |_| async move {
                    save_events.lock().expect("lock event log").push("save");
                    save_started_tx.send(()).expect("signal save start");
                    save_release.notified().await;
                    Ok(())
                },
                move || {
                    publish_events.lock().expect("lock event log").push("publish");
                },
            )
            .await
        });

        save_started_rx.await.expect("save should reach its persistence step");

        let refresh_events = events.clone();
        let (refresh_waiting_tx, refresh_waiting_rx) = oneshot::channel();
        let refresh_task = tokio::spawn(async move {
            let mut refresh = Box::pin(refresh_persisted_module_switches_with(move || async move {
                refresh_events.lock().expect("lock event log").push("refresh");
                Ok(None)
            }));

            poll_fn(|cx| match refresh.as_mut().poll(cx) {
                Poll::Pending => Poll::Ready(()),
                Poll::Ready(_) => panic!("refresh must wait for an in-flight save"),
            })
            .await;
            refresh_waiting_tx.send(()).expect("signal refresh wait");
            refresh.await
        });

        refresh_waiting_rx.await.expect("refresh should be waiting on the RMW lock");
        release_save.notify_one();

        save_task.await.expect("join save task").expect("save should succeed");
        refresh_task
            .await
            .expect("join refresh task")
            .expect("refresh should succeed");

        assert_eq!(*events.lock().expect("lock event log"), ["save", "publish", "refresh"]);
        set_persisted_module_switches(previous, previous_configured);
    }

    #[tokio::test]
    #[serial]
    async fn distributed_refresh_orders_old_true_before_new_false_publish() {
        temp_env::async_with_vars([(rustfs_config::ENV_NOTIFY_ENABLE, None::<&str>)], async {
            let previous = current_persisted_module_switches();
            let previous_configured = persisted_module_switches_configured();
            let temp_dir = TempDir::new().expect("module switch ordering temp dir");
            let volume = temp_dir.path().join("disk");
            tokio::fs::create_dir_all(&volume)
                .await
                .expect("create module switch test disk");
            let (endpoint_pools, _) =
                EndpointServerPools::from_volumes("127.0.0.1:29131", vec![volume.to_string_lossy().into_owned()])
                    .await
                    .expect("create module switch test endpoints");
            let instance_ctx = new_instance_ctx();
            init_local_disks_with_instance_ctx(&instance_ctx, endpoint_pools.clone())
                .await
                .expect("initialize module switch test disk");
            let shutdown = CancellationToken::new();
            let store = ECStore::new_with_instance_ctx(
                "127.0.0.1:29131".parse().expect("module switch test address"),
                endpoint_pools,
                shutdown.clone(),
                instance_ctx,
            )
            .await
            .expect("create module switch test store");

            save_persisted_module_switches_to(
                store.clone(),
                PersistedModuleSwitches {
                    notify_enabled: true,
                    audit_enabled: false,
                },
                || (),
            )
            .await
            .expect("persist initial enabled module state");

            let publish_order = Arc::new(StdMutex::new(Vec::new()));
            let runtime_enabled = Arc::new(AtomicBool::new(false));
            let reader_entered = Arc::new(Notify::new());
            let release_reader = Arc::new(Notify::new());
            let reader = tokio::spawn({
                let store = store.clone();
                let publish_order = publish_order.clone();
                let runtime_enabled = runtime_enabled.clone();
                let reader_entered = reader_entered.clone();
                let release_reader = release_reader.clone();
                async move {
                    with_refreshed_notify_module_state_from(store, move |resolution| async move {
                        assert!(resolution.enabled, "the old persisted snapshot should be enabled");
                        reader_entered.notify_one();
                        release_reader.notified().await;
                        publish_order.lock().expect("module publish order lock").push("old-true");
                        runtime_enabled.store(true, AtomicOrdering::SeqCst);
                    })
                    .await
                }
            });
            reader_entered.notified().await;

            let writer_entered = Arc::new(AtomicBool::new(false));
            let (writer_started_tx, writer_started_rx) = oneshot::channel();
            let writer = tokio::spawn({
                let store = store.clone();
                let publish_order = publish_order.clone();
                let runtime_enabled = runtime_enabled.clone();
                let writer_entered = writer_entered.clone();
                async move {
                    writer_started_tx.send(()).expect("signal remote writer start");
                    let save_store = store.clone();
                    with_config_object_write_lock(store, MODULE_SWITCH_CONFIG_PATH.to_string(), move || async move {
                        writer_entered.store(true, AtomicOrdering::SeqCst);
                        let data = serde_json::to_vec(&PersistedModuleSwitches {
                            notify_enabled: false,
                            audit_enabled: false,
                        })
                        .expect("serialize disabled module state");
                        save_config_no_lock(save_store, MODULE_SWITCH_CONFIG_PATH, data)
                            .await
                            .map_err(|err| err.to_string())?;
                        publish_order.lock().expect("module publish order lock").push("new-false");
                        runtime_enabled.store(false, AtomicOrdering::SeqCst);
                        Ok::<(), String>(())
                    })
                    .await
                    .expect("remote writer should acquire module object lock")
                    .expect("remote writer should persist disabled state");
                }
            });
            writer_started_rx.await.expect("remote writer should start");
            for _ in 0..10 {
                tokio::task::yield_now().await;
            }
            assert!(
                !writer_entered.load(AtomicOrdering::SeqCst),
                "the remote writer must wait until the old read has synchronously published"
            );

            release_reader.notify_one();
            reader
                .await
                .expect("join module reader")
                .expect("module reader should succeed");
            writer.await.expect("join remote module writer");

            assert_eq!(
                *publish_order.lock().expect("module publish order result lock"),
                ["old-true", "new-false"]
            );
            assert!(!runtime_enabled.load(AtomicOrdering::SeqCst), "the latest persisted false state must win");
            shutdown.cancel();
            set_persisted_module_switches(previous, previous_configured);
        })
        .await;
    }
}
