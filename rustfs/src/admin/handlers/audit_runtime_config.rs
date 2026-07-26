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
use crate::admin::storage_api::config::{
    read_admin_config_without_migrate, read_admin_server_config_snapshot, save_admin_server_config_snapshot,
};
use rustfs_audit::{audit_system, start_audit_system as start_global_audit_system, system::AuditSystemState};
use rustfs_config::DEFAULT_DELIMITER;
use rustfs_config::server_config::Config;
use s3s::{S3Result, s3_error};
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

        save_admin_server_config_snapshot(store, &config, &snapshot)
            .await
            .map(|_| ())
            .map_err(|e| s3_error!(InternalError, "failed to save audit config: {}", e))?;

        // Keep persistence and runtime publication in one detached, serialized
        // mutation. Otherwise a cancelled caller or two concurrent updates can
        // leave the persisted config and active audit generation disagreeing.
        apply_audit_runtime_config(&specs, config).await
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
