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

use rustfs_audit::reload_audit_config;
use rustfs_config::DEFAULT_DELIMITER;
use rustfs_config::audit::{AUDIT_MQTT_SUB_SYS, AUDIT_WEBHOOK_SUB_SYS};
use rustfs_ecstore::StorageAPI;
use rustfs_ecstore::config::com::{STORAGE_CLASS_SUB_SYS, read_config_without_migrate};
use rustfs_ecstore::config::storageclass;
use rustfs_ecstore::config::{Config as ServerConfig, KVS, set_global_storage_class, update_global_server_config_subsystem};
use rustfs_ecstore::new_object_layer_fn;
use rustfs_ecstore::notification_sys::get_global_notification_sys;
use s3s::{S3Error, S3ErrorCode, S3Result};
use tracing::warn;

pub use rustfs_ecstore::rpc::SERVICE_SIGNAL_RELOAD_DYNAMIC;

pub fn is_dynamic_config_subsystem(sub_system: &str) -> bool {
    matches!(sub_system, STORAGE_CLASS_SUB_SYS | AUDIT_WEBHOOK_SUB_SYS | AUDIT_MQTT_SUB_SYS)
}

fn internal_error(message: impl Into<String>) -> S3Error {
    S3Error::with_message(S3ErrorCode::InternalError, message.into())
}

fn active_subsystem_values(config: &ServerConfig, sub_system: &str) -> Option<std::collections::HashMap<String, KVS>> {
    config.0.get(sub_system).cloned()
}

fn sync_runtime_server_config(config: &ServerConfig, sub_system: &str) {
    update_global_server_config_subsystem(sub_system, active_subsystem_values(config, sub_system));
}

async fn apply_storage_class_runtime_config(config: &ServerConfig) -> S3Result<()> {
    let Some(store) = new_object_layer_fn() else {
        return Err(internal_error("storage layer not initialized"));
    };

    let kvs = config.get_value(STORAGE_CLASS_SUB_SYS, DEFAULT_DELIMITER).unwrap_or_default();
    let set_drive_count = store.set_drive_counts().into_iter().next().unwrap_or(1);
    let parsed = storageclass::lookup_config(&kvs, set_drive_count)
        .map_err(|err| internal_error(format!("failed to apply storage class config: {err}")))?;
    set_global_storage_class(parsed);
    Ok(())
}

pub async fn apply_dynamic_config_for_subsystem(config: &ServerConfig, sub_system: &str) -> S3Result<bool> {
    if !is_dynamic_config_subsystem(sub_system) {
        return Ok(false);
    }

    match sub_system {
        STORAGE_CLASS_SUB_SYS => apply_storage_class_runtime_config(config).await?,
        AUDIT_WEBHOOK_SUB_SYS | AUDIT_MQTT_SUB_SYS => reload_audit_config(config.clone())
            .await
            .map_err(|err| internal_error(format!("failed to reload audit config: {err}")))?,
        _ => return Ok(false),
    }

    sync_runtime_server_config(config, sub_system);
    Ok(true)
}

pub async fn reload_dynamic_config_runtime_state(sub_system: &str) -> S3Result<()> {
    if !is_dynamic_config_subsystem(sub_system) {
        return Err(internal_error(format!("unsupported dynamic config subsystem: {sub_system}")));
    }

    let Some(store) = new_object_layer_fn() else {
        return Err(internal_error("storage layer not initialized"));
    };

    let config = read_config_without_migrate(store)
        .await
        .map_err(|err| internal_error(format!("failed to load server config: {err}")))?;
    let _ = apply_dynamic_config_for_subsystem(&config, sub_system).await?;
    Ok(())
}

pub async fn signal_dynamic_config_reload(sub_system: &str) {
    if !is_dynamic_config_subsystem(sub_system) {
        return;
    }

    let Some(notification_sys) = get_global_notification_sys() else {
        return;
    };

    for failure in notification_sys.reload_dynamic_config(sub_system).await {
        if let Some(err) = failure.err {
            warn!("peer {} dynamic config reload for {} failed: {}", failure.host, sub_system, err);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dynamic_config_subsystems_match_runtime_apply_support() {
        assert!(is_dynamic_config_subsystem(STORAGE_CLASS_SUB_SYS));
        assert!(is_dynamic_config_subsystem(AUDIT_WEBHOOK_SUB_SYS));
        assert!(is_dynamic_config_subsystem(AUDIT_MQTT_SUB_SYS));
        assert!(!is_dynamic_config_subsystem("identity_openid"));
        assert!(!is_dynamic_config_subsystem("notify_webhook"));
    }
}
