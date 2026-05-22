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

use crate::admin::handlers::target_descriptor::AdminTargetSpec;
use rustfs_audit::{audit_system, start_audit_system as start_global_audit_system, system::AuditSystemState};
use rustfs_config::DEFAULT_DELIMITER;
use rustfs_ecstore::config::Config;
use s3s::{S3Result, s3_error};

pub(crate) async fn load_server_config_from_store() -> S3Result<Config> {
    let Some(store) = rustfs_ecstore::global::new_object_layer_fn() else {
        return Ok(Config::new());
    };

    rustfs_ecstore::config::com::read_config_without_migrate(store)
        .await
        .map_err(|e| s3_error!(InternalError, "failed to read server config: {}", e))
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
                    system
                        .reload_config(config)
                        .await
                        .map_err(|e| s3_error!(InternalError, "failed to reload audit config: {}", e))?;
                } else {
                    system
                        .close()
                        .await
                        .map_err(|e| s3_error!(InternalError, "failed to stop audit system: {}", e))?;
                }
            }
            AuditSystemState::Stopped | AuditSystemState::Stopping => {
                if has_targets {
                    system
                        .start(config)
                        .await
                        .map_err(|e| s3_error!(InternalError, "failed to start audit system: {}", e))?;
                }
            }
        }
    } else if has_targets {
        start_global_audit_system(config)
            .await
            .map_err(|e| s3_error!(InternalError, "failed to start audit system: {}", e))?;
    }

    Ok(())
}

pub(crate) async fn update_audit_config_and_reload<F>(specs: &[AdminTargetSpec], mut modifier: F) -> S3Result<()>
where
    F: FnMut(&mut Config) -> bool,
{
    let Some(store) = rustfs_ecstore::global::new_object_layer_fn() else {
        return Err(s3_error!(InternalError, "server storage not initialized"));
    };

    let mut config = rustfs_ecstore::config::com::read_config_without_migrate(store.clone())
        .await
        .map_err(|e| s3_error!(InternalError, "failed to read server config: {}", e))?;

    if !modifier(&mut config) {
        return Ok(());
    }

    rustfs_ecstore::config::com::save_server_config(store, &config)
        .await
        .map_err(|e| s3_error!(InternalError, "failed to save audit config: {}", e))?;

    apply_audit_runtime_config(specs, config).await
}

pub(crate) async fn set_audit_target_config(
    specs: &[AdminTargetSpec],
    subsystem: &str,
    target_name: &str,
    kvs: rustfs_ecstore::config::KVS,
) -> S3Result<()> {
    update_audit_config_and_reload(specs, |config| {
        config
            .0
            .entry(subsystem.to_lowercase())
            .or_default()
            .insert(target_name.to_lowercase(), kvs.clone());
        true
    })
    .await
}

pub(crate) async fn remove_audit_target_config(specs: &[AdminTargetSpec], subsystem: &str, target_name: &str) -> S3Result<()> {
    update_audit_config_and_reload(specs, |config| {
        let mut changed = false;
        if let Some(targets) = config.0.get_mut(&subsystem.to_lowercase()) {
            if targets.remove(&target_name.to_lowercase()).is_some() {
                changed = true;
            }
            if targets.is_empty() {
                config.0.remove(&subsystem.to_lowercase());
            }
        }
        changed
    })
    .await
}
