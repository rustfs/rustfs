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

use rustfs_audit::factory::{
    MQTTTargetFactory as AuditMQTTTargetFactory, TargetFactory as AuditTargetFactory,
    WebhookTargetFactory as AuditWebhookTargetFactory,
};
use rustfs_audit::reload_audit_config;
use rustfs_config::audit::{AUDIT_MQTT_SUB_SYS, AUDIT_WEBHOOK_SUB_SYS};
use rustfs_config::notify::{NOTIFY_MQTT_SUB_SYS, NOTIFY_WEBHOOK_SUB_SYS};
use rustfs_config::{DEFAULT_DELIMITER, ENABLE_KEY, EnableState};
use rustfs_ecstore::StorageAPI;
use rustfs_ecstore::config::com::{STORAGE_CLASS_SUB_SYS, read_config_without_migrate};
use rustfs_ecstore::config::storageclass;
use rustfs_ecstore::config::{Config as ServerConfig, KVS, set_global_storage_class, update_global_server_config_subsystem};
use rustfs_ecstore::new_object_layer_fn;
use rustfs_ecstore::notification_sys::get_global_notification_sys;
use rustfs_notify::factory::{
    MQTTTargetFactory as NotifyMQTTTargetFactory, TargetFactory as NotifyTargetFactory,
    WebhookTargetFactory as NotifyWebhookTargetFactory,
};
use s3s::{S3Error, S3ErrorCode, S3Result};
use tracing::warn;

pub use rustfs_ecstore::rpc::SERVICE_SIGNAL_RELOAD_DYNAMIC;

pub fn is_dynamic_config_subsystem(sub_system: &str) -> bool {
    matches!(sub_system, STORAGE_CLASS_SUB_SYS | AUDIT_WEBHOOK_SUB_SYS | AUDIT_MQTT_SUB_SYS)
}

fn internal_error(message: impl Into<String>) -> S3Error {
    S3Error::with_message(S3ErrorCode::InternalError, message.into())
}

fn invalid_request(message: impl Into<String>) -> S3Error {
    S3Error::with_message(S3ErrorCode::InvalidRequest, message.into())
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

fn validate_storage_class_kvs(kvs: &KVS, set_drive_counts: &[usize]) -> S3Result<()> {
    for count in set_drive_counts {
        storageclass::lookup_config(kvs, *count)
            .map_err(|err| invalid_request(format!("invalid storage class config: {err}")))?;
    }

    Ok(())
}

async fn validate_storage_class_config(config: &ServerConfig) -> S3Result<()> {
    let Some(store) = new_object_layer_fn() else {
        return Err(internal_error("storage layer not initialized"));
    };

    let kvs = config.get_value(STORAGE_CLASS_SUB_SYS, DEFAULT_DELIMITER).unwrap_or_default();
    let set_drive_counts = store.set_drive_counts();
    if set_drive_counts.is_empty() {
        return validate_storage_class_kvs(&kvs, &[1]);
    }

    validate_storage_class_kvs(&kvs, &set_drive_counts)
}

fn target_enabled(kvs: &KVS) -> bool {
    kvs.lookup(ENABLE_KEY)
        .and_then(|value| value.parse::<EnableState>().ok())
        .is_some_and(|state| state.is_enabled())
}

fn target_kvs(config: &ServerConfig, sub_system: &str, target: &str) -> KVS {
    let mut kvs = config.get_value(sub_system, DEFAULT_DELIMITER).unwrap_or_default();
    if target != DEFAULT_DELIMITER
        && let Some(target_kvs) = config.0.get(sub_system).and_then(|targets| targets.get(target))
    {
        kvs.extend(target_kvs.clone());
    }

    kvs
}

fn validate_notify_subsystem_config(config: &ServerConfig, sub_system: &str) -> S3Result<()> {
    let Some(targets) = config.0.get(sub_system) else {
        return Ok(());
    };

    for target in targets.keys() {
        let kvs = target_kvs(config, sub_system, target);
        if !target_enabled(&kvs) {
            continue;
        }

        let result = match sub_system {
            NOTIFY_WEBHOOK_SUB_SYS => NotifyWebhookTargetFactory.validate_config(target, &kvs),
            NOTIFY_MQTT_SUB_SYS => NotifyMQTTTargetFactory.validate_config(target, &kvs),
            _ => return Ok(()),
        };

        result.map_err(|err| invalid_request(format!("invalid {sub_system} config for target '{target}': {err}")))?;
    }

    Ok(())
}

fn validate_audit_subsystem_config(config: &ServerConfig, sub_system: &str) -> S3Result<()> {
    let Some(targets) = config.0.get(sub_system) else {
        return Ok(());
    };

    for target in targets.keys() {
        let kvs = target_kvs(config, sub_system, target);
        if !target_enabled(&kvs) {
            continue;
        }

        let result = match sub_system {
            AUDIT_WEBHOOK_SUB_SYS => AuditWebhookTargetFactory.validate_config(target, &kvs),
            AUDIT_MQTT_SUB_SYS => AuditMQTTTargetFactory.validate_config(target, &kvs),
            _ => return Ok(()),
        };

        result.map_err(|err| invalid_request(format!("invalid {sub_system} config for target '{target}': {err}")))?;
    }

    Ok(())
}

pub async fn validate_server_config(config: &ServerConfig, sub_system: Option<&str>) -> S3Result<()> {
    match sub_system {
        Some(STORAGE_CLASS_SUB_SYS) => validate_storage_class_config(config).await,
        Some(NOTIFY_WEBHOOK_SUB_SYS) => validate_notify_subsystem_config(config, NOTIFY_WEBHOOK_SUB_SYS),
        Some(NOTIFY_MQTT_SUB_SYS) => validate_notify_subsystem_config(config, NOTIFY_MQTT_SUB_SYS),
        Some(AUDIT_WEBHOOK_SUB_SYS) => validate_audit_subsystem_config(config, AUDIT_WEBHOOK_SUB_SYS),
        Some(AUDIT_MQTT_SUB_SYS) => validate_audit_subsystem_config(config, AUDIT_MQTT_SUB_SYS),
        Some(_) => Ok(()),
        None => {
            validate_storage_class_config(config).await?;
            validate_notify_subsystem_config(config, NOTIFY_WEBHOOK_SUB_SYS)?;
            validate_notify_subsystem_config(config, NOTIFY_MQTT_SUB_SYS)?;
            validate_audit_subsystem_config(config, AUDIT_WEBHOOK_SUB_SYS)?;
            validate_audit_subsystem_config(config, AUDIT_MQTT_SUB_SYS)?;
            Ok(())
        }
    }
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
    use rustfs_config::notify::NOTIFY_WEBHOOK_SUB_SYS;
    use rustfs_config::{MQTT_BROKER, MQTT_QUEUE_DIR, MQTT_TOPIC, WEBHOOK_ENDPOINT, WEBHOOK_QUEUE_DIR};

    #[test]
    fn dynamic_config_subsystems_match_runtime_apply_support() {
        assert!(is_dynamic_config_subsystem(STORAGE_CLASS_SUB_SYS));
        assert!(is_dynamic_config_subsystem(AUDIT_WEBHOOK_SUB_SYS));
        assert!(is_dynamic_config_subsystem(AUDIT_MQTT_SUB_SYS));
        assert!(!is_dynamic_config_subsystem("identity_openid"));
        assert!(!is_dynamic_config_subsystem("notify_webhook"));
    }

    #[test]
    fn validate_storage_class_kvs_rejects_invalid_parity() {
        let mut kvs = KVS::new();
        kvs.insert("standard".to_string(), "EC:5".to_string());

        let err = validate_storage_class_kvs(&kvs, &[4]).expect_err("invalid parity should fail");
        assert_eq!(err.code(), &S3ErrorCode::InvalidRequest);
    }

    #[test]
    fn validate_notify_subsystem_config_rejects_invalid_webhook_endpoint() {
        let mut config = ServerConfig::new();
        let targets = config.0.get_mut(NOTIFY_WEBHOOK_SUB_SYS).expect("notify webhook defaults");
        let kvs = targets.get_mut(DEFAULT_DELIMITER).expect("default target");
        kvs.insert(ENABLE_KEY.to_string(), EnableState::On.to_string());
        kvs.insert(WEBHOOK_ENDPOINT.to_string(), "not-a-url".to_string());
        kvs.insert(WEBHOOK_QUEUE_DIR.to_string(), "/tmp/rustfs-notify".to_string());

        let err = validate_notify_subsystem_config(&config, NOTIFY_WEBHOOK_SUB_SYS).expect_err("invalid endpoint should fail");
        assert_eq!(err.code(), &S3ErrorCode::InvalidRequest);
    }

    #[test]
    fn validate_audit_subsystem_config_rejects_relative_queue_dir() {
        let mut config = ServerConfig::new();
        let targets = config.0.get_mut(AUDIT_MQTT_SUB_SYS).expect("audit mqtt defaults");
        let kvs = targets.get_mut(DEFAULT_DELIMITER).expect("default target");
        kvs.insert(ENABLE_KEY.to_string(), EnableState::On.to_string());
        kvs.insert(MQTT_BROKER.to_string(), "mqtt://localhost:1883".to_string());
        kvs.insert(MQTT_TOPIC.to_string(), "audit-events".to_string());
        kvs.insert(MQTT_QUEUE_DIR.to_string(), "relative/dir".to_string());

        let err = validate_audit_subsystem_config(&config, AUDIT_MQTT_SUB_SYS).expect_err("relative queue dir should fail");
        assert_eq!(err.code(), &S3ErrorCode::InvalidRequest);
    }
}
