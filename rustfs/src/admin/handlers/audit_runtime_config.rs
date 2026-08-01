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

use crate::admin::handlers::supervise_admin_mutation;
use crate::admin::handlers::target_descriptor::AdminTargetSpec;
use crate::admin::runtime_sources::{AppContext, current_app_context, current_object_store_handle_for_context};
use crate::admin::service::config::with_runtime_config_reload_lock;
use crate::admin::storage_api::config::{
    read_admin_config_without_migrate, read_admin_server_config_snapshot, read_existing_admin_server_config_no_lock,
    save_admin_server_config_snapshot, with_admin_server_config_read_lock,
};
use rustfs_audit::{audit_system, start_audit_system as start_global_audit_system, system::AuditSystemState};
use rustfs_config::DEFAULT_DELIMITER;
use rustfs_config::server_config::Config;
use s3s::{S3Error, S3Result, s3_error};
use tracing::warn;

pub(crate) async fn load_server_config_from_store_for_context(context: Option<&AppContext>) -> S3Result<Config> {
    let Some(store) = current_object_store_handle_for_context(context) else {
        return Ok(Config::new());
    };

    read_admin_config_without_migrate(store)
        .await
        .map_err(|e| s3_error!(InternalError, "failed to read server config: {}", e))
}

pub(crate) async fn load_server_config_from_store() -> S3Result<Config> {
    let context = current_app_context();
    load_server_config_from_store_for_context(context.as_deref()).await
}

fn has_any_audit_targets(specs: &[AdminTargetSpec], config: &Config) -> bool {
    specs.iter().any(|spec| {
        config
            .0
            .get(spec.subsystem)
            .is_some_and(|targets| targets.keys().any(|key| key != DEFAULT_DELIMITER))
    })
}

fn audit_config_convergence_error(persisted: bool, error: impl std::fmt::Display) -> S3Error {
    if persisted {
        s3_error!(InternalError, "audit config persisted but runtime convergence failed: {}", error)
    } else {
        s3_error!(InternalError, "audit config unchanged but runtime convergence failed: {}", error)
    }
}

pub(crate) async fn apply_audit_runtime_config(specs: &[AdminTargetSpec], config: Config) -> S3Result<()> {
    let has_targets = has_any_audit_targets(specs, &config);

    if let Some(system) = audit_system() {
        match system.get_state().await {
            AuditSystemState::Running | AuditSystemState::Paused | AuditSystemState::Starting => {
                if has_targets {
                    system.reload_config(config).await.map_err(|_| {
                        warn!(reason = "reload_failed", "Failed to reload local audit runtime");
                        s3_error!(InternalError, "failed to reload audit config")
                    })?;
                } else {
                    system.close().await.map_err(|_| {
                        warn!(reason = "stop_failed", "Failed to stop local audit runtime");
                        s3_error!(InternalError, "failed to stop audit system")
                    })?;
                }
            }
            AuditSystemState::Stopped | AuditSystemState::Stopping => {
                if has_targets {
                    system.start(config).await.map_err(|_| {
                        warn!(reason = "start_failed", "Failed to start local audit runtime");
                        s3_error!(InternalError, "failed to start audit system")
                    })?;
                }
            }
        }
    } else if has_targets {
        start_global_audit_system(config).await.map_err(|_| {
            warn!(reason = "start_failed", "Failed to start global audit runtime");
            s3_error!(InternalError, "failed to start audit system")
        })?;
    }

    Ok(())
}

async fn update_audit_config_and_reload_for_context<F>(
    context: Option<&AppContext>,
    specs: &[AdminTargetSpec],
    mut modifier: F,
) -> S3Result<()>
where
    F: FnMut(&mut Config) -> bool + Send + 'static,
{
    let Some(store) = current_object_store_handle_for_context(context) else {
        return Err(s3_error!(InternalError, "server storage not initialized"));
    };
    let specs = specs.to_vec();
    supervise_admin_mutation("audit config update", async move {
        let snapshot = read_admin_server_config_snapshot(store.clone())
            .await
            .map_err(|e| s3_error!(InternalError, "failed to read server config: {}", e))?;
        let mut config = snapshot.config.clone();

        if !modifier(&mut config) {
            return Ok(());
        }

        let persisted = save_admin_server_config_snapshot(store.clone(), &config, &snapshot)
            .await
            .map_err(|e| s3_error!(InternalError, "failed to save audit config: {}", e))?
            .persisted();
        drop(snapshot);

        let read_store = store.clone();
        with_runtime_config_reload_lock(async move {
            let latest = with_admin_server_config_read_lock(store, move || read_existing_admin_server_config_no_lock(read_store))
                .await
                .map_err(|e| s3_error!(InternalError, "failed to lock server config for audit reload: {}", e))?
                .map_err(|e| s3_error!(InternalError, "failed to read latest server config for audit reload: {}", e))?;

            apply_audit_runtime_config(&specs, latest).await
        })
        .await
        .map_err(|e| audit_config_convergence_error(persisted, e))
    })
    .await
}

pub(crate) async fn update_audit_config_and_reload<F>(specs: &[AdminTargetSpec], modifier: F) -> S3Result<()>
where
    F: FnMut(&mut Config) -> bool + Send + 'static,
{
    let context = current_app_context();
    update_audit_config_and_reload_for_context(context.as_deref(), specs, modifier).await
}

pub(crate) async fn set_audit_target_config(
    specs: &[AdminTargetSpec],
    subsystem: &str,
    target_name: &str,
    kvs: rustfs_config::server_config::KVS,
) -> S3Result<()> {
    let subsystem = subsystem.to_lowercase();
    let target_name = target_name.to_lowercase();
    update_audit_config_and_reload(specs, move |config| {
        config
            .0
            .entry(subsystem.clone())
            .or_default()
            .insert(target_name.clone(), kvs.clone());
        true
    })
    .await
}

pub(crate) async fn remove_audit_target_config(specs: &[AdminTargetSpec], subsystem: &str, target_name: &str) -> S3Result<()> {
    let subsystem = subsystem.to_lowercase();
    let target_name = target_name.to_lowercase();
    update_audit_config_and_reload(specs, move |config| {
        let mut changed = false;
        if let Some(targets) = config.0.get_mut(&subsystem) {
            if targets.remove(&target_name).is_some() {
                changed = true;
            }
            if targets.is_empty() {
                config.0.remove(&subsystem);
            }
        }
        changed
    })
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::admin::handlers::target_descriptor::admin_target_spec_from_builtin;
    use crate::admin::runtime_sources::{IamInterface, KmsInterface};
    use crate::admin::storage_api::config::save_admin_server_config;
    use rustfs_config::audit::AUDIT_WEBHOOK_SUB_SYS;
    use rustfs_config::server_config::KVS;
    use rustfs_config::{ENABLE_KEY, EnableState, SCANNER_CYCLE, SCANNER_SUB_SYS, WEBHOOK_ENDPOINT, WEBHOOK_QUEUE_DIR};
    use rustfs_iam::{store::object::ObjectStore, sys::IamSys};
    use rustfs_kms::KmsServiceManager;
    use rustfs_targets::catalog::builtin::builtin_audit_target_admin_descriptors;
    use std::sync::Arc;
    use std::time::Duration;
    use tempfile::TempDir;

    struct TestIam;

    impl IamInterface for TestIam {
        fn handle(&self) -> Arc<IamSys<ObjectStore>> {
            unreachable!("audit config tests do not use IAM")
        }

        fn is_ready(&self) -> bool {
            false
        }
    }

    struct TestKms;

    impl KmsInterface for TestKms {
        fn handle(&self) -> Arc<KmsServiceManager> {
            Arc::new(KmsServiceManager::new())
        }
    }

    fn audit_specs() -> Vec<AdminTargetSpec> {
        builtin_audit_target_admin_descriptors()
            .into_iter()
            .map(|descriptor| admin_target_spec_from_builtin(&descriptor))
            .collect()
    }

    async fn wait_for_persisted_target(store: Arc<crate::admin::storage_api::runtime::ECStore>, subsystem: &str, target: &str) {
        tokio::time::timeout(Duration::from_secs(30), async {
            let mut poll = tokio::time::interval(Duration::from_millis(10));
            loop {
                poll.tick().await;
                let config = read_admin_config_without_migrate(store.clone())
                    .await
                    .expect("read persisted server config");
                if config.0.get(subsystem).is_some_and(|targets| targets.contains_key(target)) {
                    return;
                }
            }
        })
        .await
        .expect("config mutation should become durable");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[serial_test::serial]
    async fn audit_reload_reads_latest_durable_config_after_releasing_write_snapshot() {
        let temp_dir = TempDir::new().expect("audit config temp dir");
        let env = rustfs_test_utils::TestECStoreEnv::builder()
            .base_dir(temp_dir.path())
            .disk_count(1)
            .init_bucket_metadata(false)
            .build()
            .await;
        save_admin_server_config(env.ecstore.clone(), &Config::new())
            .await
            .expect("persist baseline server config");
        let context = Arc::new(AppContext::new(env.ecstore.clone(), Arc::new(TestIam), Arc::new(TestKms)));

        let (locked_tx, locked_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let blocker = tokio::spawn(async move {
            with_runtime_config_reload_lock(async move {
                locked_tx.send(()).expect("signal runtime reload lock acquisition");
                release_rx.await.expect("release runtime reload lock");
                Ok(())
            })
            .await
            .expect("runtime reload lock blocker");
        });
        locked_rx.await.expect("runtime reload lock should be held");

        let older_context = context.clone();
        let older = tokio::spawn(async move {
            let specs = audit_specs();
            update_audit_config_and_reload_for_context(Some(older_context.as_ref()), &specs, |config| {
                config
                    .0
                    .entry(SCANNER_SUB_SYS.to_string())
                    .or_default()
                    .entry(DEFAULT_DELIMITER.to_string())
                    .or_insert_with(KVS::new)
                    .insert(SCANNER_CYCLE.to_string(), "15s".to_string());
                true
            })
            .await
        });
        wait_for_persisted_target(env.ecstore.clone(), SCANNER_SUB_SYS, DEFAULT_DELIMITER).await;
        assert!(!older.is_finished(), "audit runtime publication must wait for the shared reload lock");

        let snapshot = read_admin_server_config_snapshot(env.ecstore.clone())
            .await
            .expect("read newer server config snapshot");
        let mut latest = snapshot.config.clone();
        let mut latest_target = KVS::new();
        latest_target.insert(ENABLE_KEY.to_string(), EnableState::On.to_string());
        latest_target.insert(WEBHOOK_ENDPOINT.to_string(), "https://audit.invalid/hook".to_string());
        latest_target.insert(
            WEBHOOK_QUEUE_DIR.to_string(),
            temp_dir.path().join("audit-queue").to_string_lossy().into_owned(),
        );
        latest
            .0
            .entry(AUDIT_WEBHOOK_SUB_SYS.to_string())
            .or_default()
            .insert("latest".to_string(), latest_target);
        save_admin_server_config_snapshot(env.ecstore.clone(), &latest, &snapshot)
            .await
            .expect("persist newer audit config");
        drop(snapshot);

        release_tx.send(()).expect("release runtime reload blocker");
        blocker.await.expect("runtime reload blocker task");
        tokio::time::timeout(Duration::from_secs(30), older)
            .await
            .expect("older audit update should complete")
            .expect("older audit update task should not panic")
            .expect("older audit update should converge from the latest durable config");

        let system = audit_system().expect("latest durable audit target should start the audit system");
        let targets = system.list_targets().await;
        assert!(targets.iter().any(|target| target.contains("latest")), "active targets: {targets:?}");
        system.close().await.expect("audit system should stop after the test");
    }

    #[test]
    fn audit_convergence_error_reports_durable_write_state() {
        for (persisted, expected) in [(true, "audit config persisted"), (false, "audit config unchanged")] {
            let error = audit_config_convergence_error(persisted, "injected failure");
            assert!(error.to_string().contains(expected), "unexpected convergence error: {error}");
            assert!(error.to_string().contains("injected failure"));
        }
    }
}
