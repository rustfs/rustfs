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

use crate::admin::runtime_sources::{
    AppContext, current_app_context, current_notification_system_for_context, current_object_store_handle_for_context,
    publish_server_config, publish_storage_class_config,
};
use crate::admin::storage_api::config::{STORAGE_CLASS_SUB_SYS, read_admin_config_without_migrate, storageclass};
use crate::admin::storage_api::contract::admin::StorageAdminApi;
use crate::admin::storage_api::runtime::ECStore;
use rustfs_audit::reload_audit_config;
use rustfs_config::audit::{AUDIT_MQTT_SUB_SYS, AUDIT_REDIS_DEFAULT_CHANNEL, AUDIT_WEBHOOK_SUB_SYS};
use rustfs_config::notify::{NOTIFY_MQTT_SUB_SYS, NOTIFY_REDIS_DEFAULT_CHANNEL, NOTIFY_WEBHOOK_SUB_SYS};
use rustfs_config::oidc::IDENTITY_OPENID_SUB_SYS;
use rustfs_config::server_config::{Config as ServerConfig, KVS};
use rustfs_config::{AUDIT_DEFAULT_DIR, EVENT_DEFAULT_DIR};
use rustfs_config::{DEFAULT_DELIMITER, ENABLE_KEY, EnableState};
use rustfs_config::{HEAL_SUB_SYS, SCANNER_SUB_SYS};
use rustfs_iam::oidc::load_oidc_provider_configs_from_server_config;
use rustfs_targets::config::{
    validate_amqp_config, validate_kafka_config, validate_mqtt_config, validate_mysql_config, validate_nats_config,
    validate_postgres_config, validate_pulsar_config, validate_redis_config, validate_webhook_config,
};
use s3s::{S3Error, S3ErrorCode, S3Result};
use std::future::Future;
use tracing::warn;
use url::Url;

pub fn is_dynamic_config_subsystem(sub_system: &str) -> bool {
    matches!(
        sub_system,
        STORAGE_CLASS_SUB_SYS | AUDIT_WEBHOOK_SUB_SYS | AUDIT_MQTT_SUB_SYS | SCANNER_SUB_SYS | HEAL_SUB_SYS
    )
}

pub(crate) const FULL_CONFIG_WORKER_SUBSYSTEMS: [&str; 2] = [AUDIT_WEBHOOK_SUB_SYS, SCANNER_SUB_SYS];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DynamicConfigWorkerMutation {
    None,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DynamicConfigReloadPlan {
    worker_mutation: DynamicConfigWorkerMutation,
}

fn dynamic_config_reload_plan(sub_system: &str) -> Option<DynamicConfigReloadPlan> {
    is_dynamic_config_subsystem(sub_system).then_some(DynamicConfigReloadPlan {
        worker_mutation: DynamicConfigWorkerMutation::None,
    })
}

fn internal_error(message: impl Into<String>) -> S3Error {
    S3Error::with_message(S3ErrorCode::InternalError, message.into())
}

fn invalid_request(message: impl Into<String>) -> S3Error {
    S3Error::with_message(S3ErrorCode::InvalidRequest, message.into())
}

fn resolve_runtime_config_store_for_context(context: Option<&AppContext>) -> S3Result<std::sync::Arc<ECStore>> {
    current_object_store_handle_for_context(context).ok_or_else(|| internal_error("storage layer not initialized"))
}

#[derive(Debug, Default)]
pub(crate) struct PreparedRuntimeConfig {
    storage_class: Option<storageclass::Config>,
}

impl PreparedRuntimeConfig {
    fn publish_storage_class_with(self, publish: impl FnOnce(storageclass::Config)) -> bool {
        let Some(config) = self.storage_class else {
            return false;
        };

        publish(config);
        true
    }

    fn publish_storage_class_for_context_with(
        self,
        context: Option<&AppContext>,
        publish_fallback: impl FnOnce(storageclass::Config),
    ) -> S3Result<()> {
        if self.publish_storage_class_with(|config| {
            if let Some(context) = context {
                context.storage_class().set(config);
            } else {
                publish_fallback(config);
            }
        }) {
            Ok(())
        } else {
            Err(internal_error("prepared storage class candidate is missing"))
        }
    }

    fn publish_storage_class_for_context(self, context: Option<&AppContext>) -> S3Result<()> {
        self.publish_storage_class_for_context_with(context, publish_storage_class_config)
    }

    pub(crate) fn publish_storage_class(self) -> S3Result<()> {
        let context = current_app_context();
        self.publish_storage_class_for_context(context.as_deref())
    }
}

fn publish_server_config_for_context(context: Option<&AppContext>, config: ServerConfig) {
    if let Some(context) = context {
        context.server_config().set(config);
    } else {
        publish_server_config(config);
    }
}

fn prepare_storage_class_kvs(kvs: &KVS, set_drive_counts: &[usize]) -> S3Result<storageclass::Config> {
    storageclass::lookup_config_for_pools(kvs, set_drive_counts)
        .map_err(|err| invalid_request(format!("invalid storage class config: {err}")))
}

async fn prepare_storage_class_runtime_config_for_context(
    context: Option<&AppContext>,
    config: &ServerConfig,
) -> S3Result<storageclass::Config> {
    let store = resolve_runtime_config_store_for_context(context)?;
    let kvs = config.get_value(STORAGE_CLASS_SUB_SYS, DEFAULT_DELIMITER).unwrap_or_default();
    let set_drive_counts = StorageAdminApi::set_drive_counts(store.as_ref());

    prepare_storage_class_kvs(&kvs, &set_drive_counts)
}

async fn apply_storage_class_runtime_config_for_context(context: Option<&AppContext>, config: &ServerConfig) -> S3Result<()> {
    let parsed = prepare_storage_class_runtime_config_for_context(context, config)
        .await
        .map_err(|err| internal_error(format!("failed to apply storage class config: {err}")))?;
    PreparedRuntimeConfig {
        storage_class: Some(parsed),
    }
    .publish_storage_class_for_context(context)?;
    Ok(())
}

#[cfg(test)]
fn validate_storage_class_kvs(kvs: &KVS, set_drive_counts: &[usize]) -> S3Result<()> {
    prepare_storage_class_kvs(kvs, set_drive_counts).map(|_| ())
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
            "notify_webhook" => validate_webhook_config(&kvs, EVENT_DEFAULT_DIR),
            "notify_amqp" => validate_amqp_config(&kvs, EVENT_DEFAULT_DIR),
            "notify_kafka" => validate_kafka_config(&kvs, EVENT_DEFAULT_DIR),
            "notify_mqtt" => validate_mqtt_config(&kvs),
            "notify_mysql" => validate_mysql_config(&kvs, EVENT_DEFAULT_DIR),
            "notify_nats" => validate_nats_config(&kvs, EVENT_DEFAULT_DIR),
            "notify_postgres" => validate_postgres_config(&kvs, EVENT_DEFAULT_DIR),
            "notify_pulsar" => validate_pulsar_config(&kvs, EVENT_DEFAULT_DIR),
            "notify_redis" => validate_redis_config(&kvs, EVENT_DEFAULT_DIR, NOTIFY_REDIS_DEFAULT_CHANNEL),
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
            "audit_webhook" => validate_webhook_config(&kvs, AUDIT_DEFAULT_DIR),
            "audit_amqp" => validate_amqp_config(&kvs, AUDIT_DEFAULT_DIR),
            "audit_kafka" => validate_kafka_config(&kvs, AUDIT_DEFAULT_DIR),
            "audit_mqtt" => validate_mqtt_config(&kvs),
            "audit_mysql" => validate_mysql_config(&kvs, AUDIT_DEFAULT_DIR),
            "audit_nats" => validate_nats_config(&kvs, AUDIT_DEFAULT_DIR),
            "audit_postgres" => validate_postgres_config(&kvs, AUDIT_DEFAULT_DIR),
            "audit_pulsar" => validate_pulsar_config(&kvs, AUDIT_DEFAULT_DIR),
            "audit_redis" => validate_redis_config(&kvs, AUDIT_DEFAULT_DIR, AUDIT_REDIS_DEFAULT_CHANNEL),
            _ => return Ok(()),
        };

        result.map_err(|err| invalid_request(format!("invalid {sub_system} config for target '{target}': {err}")))?;
    }

    Ok(())
}

fn is_valid_provider_id(id: &str) -> bool {
    !id.is_empty() && id.chars().all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
}

fn is_valid_scheme(scheme: &str) -> bool {
    scheme == "http" || scheme == "https"
}

fn validate_absolute_http_url(value: &str, field_name: &str) -> S3Result<()> {
    let parsed = Url::parse(value).map_err(|_| invalid_request(format!("{field_name} must be an absolute http/https URL")))?;
    if !is_valid_scheme(parsed.scheme()) || parsed.host_str().is_none() {
        return Err(invalid_request(format!("{field_name} must be an absolute http/https URL")));
    }

    Ok(())
}

fn validate_identity_openid_config(config: &ServerConfig) -> S3Result<()> {
    if let Some(targets) = config.0.get(IDENTITY_OPENID_SUB_SYS) {
        for target in targets.keys() {
            if target == DEFAULT_DELIMITER {
                continue;
            }
            if !is_valid_provider_id(target) {
                return Err(invalid_request(format!("invalid provider_id '{target}'")));
            }
        }
    }

    for provider in load_oidc_provider_configs_from_server_config(config) {
        if !is_valid_provider_id(&provider.id) {
            return Err(invalid_request(format!("invalid provider_id '{}'", provider.id)));
        }
        if provider.config_url.trim().is_empty() {
            return Err(invalid_request(format!("identity_openid provider '{}' requires config_url", provider.id)));
        }
        validate_absolute_http_url(&provider.config_url, "config_url")?;

        if provider.client_id.trim().is_empty() {
            return Err(invalid_request(format!("identity_openid provider '{}' requires client_id", provider.id)));
        }

        if !provider.redirect_uri_dynamic {
            let Some(redirect_uri) = provider.redirect_uri.as_deref() else {
                return Err(invalid_request(format!(
                    "identity_openid provider '{}' requires redirect_uri when redirect_uri_dynamic is off",
                    provider.id
                )));
            };
            validate_absolute_http_url(redirect_uri, "redirect_uri")?;
        } else if let Some(redirect_uri) = provider.redirect_uri.as_deref() {
            validate_absolute_http_url(redirect_uri, "redirect_uri")?;
        }

        if !provider.scopes.iter().any(|scope| scope == "openid") {
            return Err(invalid_request(format!(
                "identity_openid provider '{}' scopes must include openid",
                provider.id
            )));
        }
    }

    Ok(())
}

pub(crate) async fn prepare_server_config_for_context(
    context: Option<&AppContext>,
    config: &ServerConfig,
    sub_system: Option<&str>,
) -> S3Result<PreparedRuntimeConfig> {
    let mut prepared = PreparedRuntimeConfig::default();

    match sub_system {
        Some(STORAGE_CLASS_SUB_SYS) => {
            prepared.storage_class = Some(prepare_storage_class_runtime_config_for_context(context, config).await?);
        }
        Some(NOTIFY_WEBHOOK_SUB_SYS) => validate_notify_subsystem_config(config, NOTIFY_WEBHOOK_SUB_SYS)?,
        Some(NOTIFY_MQTT_SUB_SYS) => validate_notify_subsystem_config(config, NOTIFY_MQTT_SUB_SYS)?,
        Some(AUDIT_WEBHOOK_SUB_SYS) => validate_audit_subsystem_config(config, AUDIT_WEBHOOK_SUB_SYS)?,
        Some(AUDIT_MQTT_SUB_SYS) => validate_audit_subsystem_config(config, AUDIT_MQTT_SUB_SYS)?,
        Some(IDENTITY_OPENID_SUB_SYS) => validate_identity_openid_config(config)?,
        Some(SCANNER_SUB_SYS | HEAL_SUB_SYS) => rustfs_scanner::validate_scanner_runtime_config(config)
            .map_err(|err| invalid_request(format!("invalid scanner config: {err}")))?,
        Some(_) => {}
        None => {
            prepared.storage_class = Some(prepare_storage_class_runtime_config_for_context(context, config).await?);
            validate_notify_subsystem_config(config, NOTIFY_WEBHOOK_SUB_SYS)?;
            validate_notify_subsystem_config(config, NOTIFY_MQTT_SUB_SYS)?;
            validate_audit_subsystem_config(config, AUDIT_WEBHOOK_SUB_SYS)?;
            validate_audit_subsystem_config(config, AUDIT_MQTT_SUB_SYS)?;
            validate_identity_openid_config(config)?;
            rustfs_scanner::validate_scanner_runtime_config(config)
                .map_err(|err| invalid_request(format!("invalid scanner config: {err}")))?;
        }
    }

    Ok(prepared)
}

pub async fn validate_server_config_for_context(
    context: Option<&AppContext>,
    config: &ServerConfig,
    sub_system: Option<&str>,
) -> S3Result<()> {
    prepare_server_config_for_context(context, config, sub_system)
        .await
        .map(|_| ())
}

pub(crate) async fn prepare_server_config(config: &ServerConfig, sub_system: Option<&str>) -> S3Result<PreparedRuntimeConfig> {
    let context = current_app_context();
    prepare_server_config_for_context(context.as_deref(), config, sub_system).await
}

pub async fn validate_server_config(config: &ServerConfig, sub_system: Option<&str>) -> S3Result<()> {
    let context = current_app_context();
    validate_server_config_for_context(context.as_deref(), config, sub_system).await
}

pub async fn apply_dynamic_config_for_subsystem_for_context(
    context: Option<&AppContext>,
    config: &ServerConfig,
    sub_system: &str,
) -> S3Result<bool> {
    if dynamic_config_reload_plan(sub_system).is_none() {
        return Ok(false);
    }

    match sub_system {
        STORAGE_CLASS_SUB_SYS => apply_storage_class_runtime_config_for_context(context, config).await?,
        AUDIT_WEBHOOK_SUB_SYS | AUDIT_MQTT_SUB_SYS => reload_audit_config(config.clone())
            .await
            .map_err(|err| internal_error(format!("failed to reload audit config: {err}")))?,
        SCANNER_SUB_SYS => rustfs_scanner::apply_scanner_runtime_config(config)
            .map_err(|err| internal_error(format!("failed to reload scanner config: {err}")))?,
        HEAL_SUB_SYS => rustfs_scanner::apply_scanner_runtime_config(config)
            .map_err(|err| internal_error(format!("failed to reload heal scanner controls: {err}")))?,
        _ => return Ok(false),
    }

    Ok(true)
}

pub async fn apply_dynamic_config_for_subsystem(config: &ServerConfig, sub_system: &str) -> S3Result<bool> {
    let context = current_app_context();
    apply_dynamic_config_for_subsystem_for_context(context.as_deref(), config, sub_system).await
}

async fn reload_dynamic_config_with<ReadFuture, Apply, ApplyFuture>(read: ReadFuture, apply: Apply) -> S3Result<()>
where
    ReadFuture: Future<Output = S3Result<ServerConfig>>,
    Apply: FnOnce(ServerConfig) -> ApplyFuture,
    ApplyFuture: Future<Output = S3Result<()>>,
{
    let config = read.await?;
    apply(config).await
}

pub async fn reload_dynamic_config_runtime_state_for_context(context: Option<&AppContext>, sub_system: &str) -> S3Result<()> {
    if !is_dynamic_config_subsystem(sub_system) {
        return Err(internal_error(format!("unsupported dynamic config subsystem: {sub_system}")));
    }

    let store = resolve_runtime_config_store_for_context(context)?;

    reload_dynamic_config_with(
        async move {
            read_admin_config_without_migrate(store).await.map_err(|err| {
                warn!("peer reload_dynamic_config: failed to load server config for {sub_system}: {err}");
                internal_error(format!("failed to load server config: {err}"))
            })
        },
        |config| async move {
            apply_dynamic_config_for_subsystem_for_context(context, &config, sub_system)
                .await
                .map_err(|err| {
                    warn!("peer reload_dynamic_config: failed to apply {sub_system}: {err}");
                    err
                })?;
            Ok(())
        },
    )
    .await
}

pub async fn reload_dynamic_config_runtime_state(sub_system: &str) -> S3Result<()> {
    let context = current_app_context();
    reload_dynamic_config_runtime_state_for_context(context.as_deref(), sub_system).await
}

async fn reload_runtime_config_snapshot_with<ReadFuture, Prepare, PrepareFuture, Publish, ApplyWorkers, ApplyWorkersFuture>(
    read: ReadFuture,
    prepare: Prepare,
    publish: Publish,
    apply_workers: ApplyWorkers,
) -> S3Result<()>
where
    ReadFuture: Future<Output = S3Result<ServerConfig>>,
    Prepare: FnOnce(ServerConfig) -> PrepareFuture,
    PrepareFuture: Future<Output = S3Result<(ServerConfig, PreparedRuntimeConfig)>>,
    Publish: FnOnce(&ServerConfig, PreparedRuntimeConfig) -> S3Result<()>,
    ApplyWorkers: FnOnce(ServerConfig) -> ApplyWorkersFuture,
    ApplyWorkersFuture: Future<Output = S3Result<()>>,
{
    let config = read.await?;
    let (config, prepared) = prepare(config).await?;
    publish(&config, prepared)?;

    // Worker reloads mutate live state and have no rollback contract. They are
    // therefore best-effort after the validated storage/server snapshots are
    // published; a transient worker failure must not leave this peer on stale
    // erasure geometry.
    if let Err(err) = apply_workers(config).await {
        warn!(
            event = "config_worker_reload_failed",
            component = "admin",
            subsystem = "config",
            state = "best_effort",
            error = ?err,
            "Runtime config snapshot was published but a worker reload failed"
        );
    }
    Ok(())
}

pub async fn reload_runtime_config_snapshot_for_context(context: Option<&AppContext>) -> S3Result<()> {
    let store = resolve_runtime_config_store_for_context(context)?;

    reload_runtime_config_snapshot_with(
        async move {
            read_admin_config_without_migrate(store).await.map_err(|err| {
                warn!("peer reload_runtime_config_snapshot: failed to load server config: {err}");
                internal_error(format!("failed to load server config: {err}"))
            })
        },
        |config| async move {
            let prepared = prepare_server_config_for_context(context, &config, None).await.map_err(|_| {
                warn!("peer reload_runtime_config_snapshot: failed to prepare server config");
                internal_error("failed to prepare server config")
            })?;
            Ok((config, prepared))
        },
        |config, prepared| {
            prepared.publish_storage_class_for_context(context)?;
            publish_server_config_for_context(context, config.clone());
            Ok(())
        },
        |config| async move {
            for sub_system in FULL_CONFIG_WORKER_SUBSYSTEMS {
                if let Err(err) = apply_dynamic_config_for_subsystem_for_context(context, &config, sub_system).await {
                    warn!(
                        event = "config_worker_reload_failed",
                        component = "admin",
                        subsystem = "config",
                        config_subsystem = sub_system,
                        state = "best_effort",
                        error = ?err,
                        "Peer runtime config snapshot was published but a subsystem worker reload failed"
                    );
                }
            }
            Ok(())
        },
    )
    .await
}

pub async fn reload_runtime_config_snapshot() -> S3Result<()> {
    let context = current_app_context();
    reload_runtime_config_snapshot_for_context(context.as_deref()).await
}

pub async fn signal_dynamic_config_reload_for_context(context: Option<&AppContext>, sub_system: &str) {
    if !is_dynamic_config_subsystem(sub_system) {
        return;
    }

    let Some(notification_sys) = current_notification_system_for_context(context) else {
        return;
    };

    for failure in notification_sys.reload_dynamic_config(sub_system).await {
        if let Some(err) = failure.err {
            tracing::warn!("peer {} dynamic config reload for {} failed: {}", failure.host, sub_system, err);
        }
    }
}

pub async fn signal_dynamic_config_reload(sub_system: &str) {
    let context = current_app_context();
    signal_dynamic_config_reload_for_context(context.as_deref(), sub_system).await;
}

pub async fn signal_config_snapshot_reload_for_context(context: Option<&AppContext>) {
    let Some(notification_sys) = current_notification_system_for_context(context) else {
        return;
    };

    for failure in notification_sys.refresh_config_snapshot().await {
        if let Some(err) = failure.err {
            tracing::warn!("peer config snapshot refresh failed for {}: {}", failure.host, err);
        }
    }
}

pub async fn signal_config_snapshot_reload() {
    let context = current_app_context();
    signal_config_snapshot_reload_for_context(context.as_deref()).await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::admin::runtime_sources::{IamInterface, KmsInterface, ServerConfigInterface, StorageClassInterface};
    use crate::admin::storage_api::bucket::metadata::{BUCKET_LIFECYCLE_CONFIG, BUCKET_REPLICATION_CONFIG};
    use crate::admin::storage_api::config::save_admin_server_config;
    use crate::storage_api::cluster::{Endpoint, EndpointServerPools, Endpoints, PoolEndpoints};
    use crate::storage_api::startup::storage::{init_local_disks_with_instance_ctx, new_instance_ctx};
    use rustfs_config::notify::NOTIFY_WEBHOOK_SUB_SYS;
    use rustfs_config::oidc::{OIDC_CLIENT_ID, OIDC_CONFIG_URL, OIDC_SCOPES};
    use rustfs_config::{HEAL_SUB_SYS, SCANNER_SUB_SYS};
    use rustfs_config::{MQTT_BROKER, MQTT_QUEUE_DIR, MQTT_TOPIC, WEBHOOK_ENDPOINT, WEBHOOK_QUEUE_DIR};
    use rustfs_iam::{store::object::ObjectStore, sys::IamSys};
    use rustfs_kms::KmsServiceManager;
    use std::collections::HashMap;
    use std::path::Path;
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    };
    use tempfile::TempDir;
    use tokio_util::sync::CancellationToken;

    const LIFECYCLE_RELOAD_LABEL: &str = "lifecycle";
    const REPLICATION_RELOAD_LABEL: &str = "replication";

    fn without_storage_class_env<R>(f: impl FnOnce() -> R) -> R {
        temp_env::with_vars_unset(
            [
                storageclass::STANDARD_ENV,
                storageclass::RRS_ENV,
                storageclass::OPTIMIZE_ENV,
                storageclass::INLINE_BLOCK_ENV,
            ],
            f,
        )
    }

    struct TestIamInterface;

    impl IamInterface for TestIamInterface {
        fn handle(&self) -> Arc<IamSys<ObjectStore>> {
            unreachable!("runtime config reload tests do not use IAM")
        }

        fn is_ready(&self) -> bool {
            false
        }
    }

    struct TestKmsInterface {
        manager: Arc<KmsServiceManager>,
    }

    impl KmsInterface for TestKmsInterface {
        fn handle(&self) -> Arc<KmsServiceManager> {
            self.manager.clone()
        }
    }

    struct TestServerConfigInterface {
        snapshot: Arc<Mutex<Option<ServerConfig>>>,
        set_calls: Arc<AtomicUsize>,
    }

    impl ServerConfigInterface for TestServerConfigInterface {
        fn get(&self) -> Option<ServerConfig> {
            self.snapshot.lock().expect("server config snapshot lock").clone()
        }

        fn set(&self, config: ServerConfig) {
            self.set_calls.fetch_add(1, Ordering::SeqCst);
            *self.snapshot.lock().expect("server config snapshot lock") = Some(config);
        }
    }

    struct TestStorageClassInterface {
        snapshot: Arc<Mutex<Arc<storageclass::Config>>>,
        set_calls: Arc<AtomicUsize>,
    }

    impl StorageClassInterface for TestStorageClassInterface {
        fn set(&self, config: storageclass::Config) {
            self.set_calls.fetch_add(1, Ordering::SeqCst);
            *self.snapshot.lock().expect("storage class snapshot lock") = Arc::new(config);
        }
    }

    struct RuntimeConfigReloadFixture {
        _temp_dir: TempDir,
        context: AppContext,
        baseline_server: ServerConfig,
        baseline_storage_class: Arc<storageclass::Config>,
        server_snapshot: Arc<Mutex<Option<ServerConfig>>>,
        storage_class_snapshot: Arc<Mutex<Arc<storageclass::Config>>>,
        server_set_calls: Arc<AtomicUsize>,
        storage_class_set_calls: Arc<AtomicUsize>,
    }

    impl RuntimeConfigReloadFixture {
        fn assert_snapshots_unchanged(&self) {
            assert_eq!(self.server_set_calls.load(Ordering::SeqCst), 0);
            assert_eq!(self.storage_class_set_calls.load(Ordering::SeqCst), 0);
            assert_eq!(
                *self.server_snapshot.lock().expect("server config result lock"),
                Some(self.baseline_server.clone())
            );
            let storage_class_snapshot = self.storage_class_snapshot.lock().expect("storage class result lock");
            assert!(Arc::ptr_eq(&*storage_class_snapshot, &self.baseline_storage_class));
            for storage_class in [storageclass::STANDARD, storageclass::RRS] {
                assert_eq!(
                    storage_class_snapshot.parities_for_sc(storage_class),
                    self.baseline_storage_class.parities_for_sc(storage_class),
                    "{storage_class} snapshot changed"
                );
            }
        }
    }

    fn storage_class_server_config(standard: &str) -> ServerConfig {
        let mut config = ServerConfig::new();
        let mut kvs = KVS::new();
        kvs.insert(storageclass::CLASS_STANDARD.to_string(), standard.to_string());
        config
            .0
            .insert(STORAGE_CLASS_SUB_SYS.to_string(), HashMap::from([(DEFAULT_DELIMITER.to_string(), kvs)]));
        config
    }

    async fn build_isolated_heterogeneous_store(temp_dir: &Path) -> Arc<ECStore> {
        let mut pools = Vec::new();
        for (pool_index, drives_per_set) in [4, 2].into_iter().enumerate() {
            let mut endpoints = Vec::new();
            for disk_index in 0..drives_per_set {
                let disk_path = temp_dir.join(format!("pool{pool_index}/disk{disk_index}"));
                tokio::fs::create_dir_all(&disk_path)
                    .await
                    .expect("create test disk directory");
                let mut endpoint = Endpoint::try_from(disk_path.to_str().expect("utf-8 test disk path")).expect("local endpoint");
                endpoint.set_pool_index(pool_index);
                endpoint.set_set_index(0);
                endpoint.set_disk_index(disk_index);
                endpoints.push(endpoint);
            }
            pools.push(PoolEndpoints {
                legacy: false,
                set_count: 1,
                drives_per_set,
                endpoints: Endpoints::from(endpoints),
                cmd_line: format!("runtime-config-pool-{pool_index}"),
                platform: "test".to_string(),
            });
        }

        let endpoint_pools = EndpointServerPools::from(pools);
        let instance_ctx = new_instance_ctx();
        init_local_disks_with_instance_ctx(&instance_ctx, endpoint_pools.clone())
            .await
            .expect("register isolated test disks");
        ECStore::new_with_instance_ctx(
            "127.0.0.1:0".parse().expect("test address"),
            endpoint_pools,
            CancellationToken::new(),
            instance_ctx,
        )
        .await
        .expect("build isolated heterogeneous store")
    }

    async fn runtime_config_reload_fixture() -> RuntimeConfigReloadFixture {
        let temp_dir = TempDir::new().expect("runtime config reload temp dir");
        let store = build_isolated_heterogeneous_store(temp_dir.path()).await;
        assert_eq!(StorageAdminApi::set_drive_counts(store.as_ref()), vec![4, 2]);

        let rejected_server = storage_class_server_config("EC:2");
        save_admin_server_config(store.clone(), &rejected_server)
            .await
            .expect("persist rejected storage class config");

        let baseline_server = storage_class_server_config("EC:1");
        let baseline_kvs = baseline_server
            .get_value(STORAGE_CLASS_SUB_SYS, DEFAULT_DELIMITER)
            .expect("baseline storage class KVS");
        let baseline_storage_class = Arc::new(prepare_storage_class_kvs(&baseline_kvs, &[4, 2]).expect("baseline storage class"));
        let server_snapshot = Arc::new(Mutex::new(Some(baseline_server.clone())));
        let storage_class_snapshot = Arc::new(Mutex::new(baseline_storage_class.clone()));
        let server_set_calls = Arc::new(AtomicUsize::new(0));
        let storage_class_set_calls = Arc::new(AtomicUsize::new(0));
        let context = AppContext::new(
            store,
            Arc::new(TestIamInterface),
            Arc::new(TestKmsInterface {
                manager: Arc::new(KmsServiceManager::new()),
            }),
        )
        .with_test_runtime_config_interfaces(
            Arc::new(TestServerConfigInterface {
                snapshot: server_snapshot.clone(),
                set_calls: server_set_calls.clone(),
            }),
            Arc::new(TestStorageClassInterface {
                snapshot: storage_class_snapshot.clone(),
                set_calls: storage_class_set_calls.clone(),
            }),
        );

        RuntimeConfigReloadFixture {
            _temp_dir: temp_dir,
            context,
            baseline_server,
            baseline_storage_class,
            server_snapshot,
            storage_class_snapshot,
            server_set_calls,
            storage_class_set_calls,
        }
    }

    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn peer_dynamic_reload_rejects_later_pool_without_publishing() {
        temp_env::async_with_vars(
            [
                (storageclass::STANDARD_ENV, None::<&str>),
                (storageclass::RRS_ENV, None::<&str>),
                (storageclass::OPTIMIZE_ENV, None::<&str>),
                (storageclass::INLINE_BLOCK_ENV, None::<&str>),
            ],
            async {
                let fixture = runtime_config_reload_fixture().await;
                let err = reload_dynamic_config_runtime_state_for_context(Some(&fixture.context), STORAGE_CLASS_SUB_SYS)
                    .await
                    .expect_err("later pool parity must reject peer dynamic reload");

                assert!(
                    err.message()
                        .is_some_and(|message| message.contains("storage class validation failed for pool 1")),
                    "unexpected dynamic reload error: {err:?}"
                );
                fixture.assert_snapshots_unchanged();
            },
        )
        .await;
    }

    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn peer_dynamic_reload_publishes_valid_per_pool_storage_snapshot() {
        temp_env::async_with_vars(
            [
                (storageclass::STANDARD_ENV, None::<&str>),
                (storageclass::RRS_ENV, None::<&str>),
                (storageclass::OPTIMIZE_ENV, None::<&str>),
                (storageclass::INLINE_BLOCK_ENV, None::<&str>),
            ],
            async {
                let fixture = runtime_config_reload_fixture().await;
                let candidate = storage_class_server_config("");
                save_admin_server_config(fixture.context.object_store(), &candidate)
                    .await
                    .expect("persist valid automatic storage class config");

                reload_dynamic_config_runtime_state_for_context(Some(&fixture.context), STORAGE_CLASS_SUB_SYS)
                    .await
                    .expect("valid peer dynamic reload must publish its prepared storage snapshot");

                assert_eq!(fixture.server_set_calls.load(Ordering::SeqCst), 0);
                assert_eq!(fixture.storage_class_set_calls.load(Ordering::SeqCst), 1);
                assert_eq!(
                    *fixture.server_snapshot.lock().expect("server config result lock"),
                    Some(fixture.baseline_server.clone()),
                    "dynamic reload must not replace the server-config snapshot"
                );
                let storage_class = fixture.storage_class_snapshot.lock().expect("storage class result lock");
                assert_eq!(storage_class.parities_for_sc(storageclass::STANDARD), Some(vec![2, 1]));
                assert_eq!(storage_class.parities_for_sc(storageclass::RRS), Some(vec![1, 1]));
            },
        )
        .await;
    }

    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn peer_full_reload_rejects_later_pool_without_publishing() {
        temp_env::async_with_vars(
            [
                (storageclass::STANDARD_ENV, None::<&str>),
                (storageclass::RRS_ENV, None::<&str>),
                (storageclass::OPTIMIZE_ENV, None::<&str>),
                (storageclass::INLINE_BLOCK_ENV, None::<&str>),
            ],
            async {
                let fixture = runtime_config_reload_fixture().await;
                let err = reload_runtime_config_snapshot_for_context(Some(&fixture.context))
                    .await
                    .expect_err("later pool parity must reject peer full reload");

                assert_eq!(err.message(), Some("failed to prepare server config"));
                fixture.assert_snapshots_unchanged();
            },
        )
        .await;
    }

    #[tokio::test]
    async fn full_reload_publishes_snapshots_before_best_effort_worker_failure() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let publish_events = events.clone();
        let worker_events = events.clone();

        reload_runtime_config_snapshot_with(
            async { Ok(ServerConfig::new()) },
            |config| async { Ok((config, PreparedRuntimeConfig::default())) },
            move |_config, _prepared| {
                publish_events.lock().expect("reload event lock").push("publish");
                Ok(())
            },
            move |_config| async move {
                let mut events = worker_events.lock().expect("reload event lock");
                events.push("worker-1-applied");
                events.push("worker-2-failed");
                Err(internal_error("injected worker reload failure"))
            },
        )
        .await
        .expect("worker failure must not roll back validated storage/server snapshots");

        assert_eq!(
            *events.lock().expect("reload result lock"),
            ["publish", "worker-1-applied", "worker-2-failed"]
        );
    }

    #[test]
    fn dynamic_config_subsystems_match_runtime_apply_support() {
        assert!(is_dynamic_config_subsystem(AUDIT_WEBHOOK_SUB_SYS));
        assert!(is_dynamic_config_subsystem(AUDIT_MQTT_SUB_SYS));
        assert!(is_dynamic_config_subsystem(SCANNER_SUB_SYS));
        assert!(is_dynamic_config_subsystem(HEAL_SUB_SYS));
        assert!(is_dynamic_config_subsystem(STORAGE_CLASS_SUB_SYS));
        assert!(!is_dynamic_config_subsystem("identity_openid"));
        assert!(!is_dynamic_config_subsystem("notify_webhook"));
    }

    #[test]
    fn full_config_worker_reload_uses_one_representative_per_worker_family() {
        assert_eq!(FULL_CONFIG_WORKER_SUBSYSTEMS, [AUDIT_WEBHOOK_SUB_SYS, SCANNER_SUB_SYS]);
    }

    #[test]
    fn background_config_reload_plan_never_mutates_workers() {
        for sub_system in [
            STORAGE_CLASS_SUB_SYS,
            AUDIT_WEBHOOK_SUB_SYS,
            AUDIT_MQTT_SUB_SYS,
            SCANNER_SUB_SYS,
            HEAL_SUB_SYS,
        ] {
            assert_eq!(
                dynamic_config_reload_plan(sub_system).map(|plan| plan.worker_mutation),
                Some(DynamicConfigWorkerMutation::None)
            );
        }

        for bucket_config in [
            BUCKET_LIFECYCLE_CONFIG,
            BUCKET_REPLICATION_CONFIG,
            LIFECYCLE_RELOAD_LABEL,
            REPLICATION_RELOAD_LABEL,
        ] {
            assert_eq!(dynamic_config_reload_plan(bucket_config), None);
        }
    }

    #[test]
    #[serial_test::serial(storage_class_env)]
    fn validate_storage_class_kvs_rejects_invalid_parity() {
        let mut kvs = KVS::new();
        kvs.insert("standard".to_string(), "EC:5".to_string());

        without_storage_class_env(|| {
            let err = validate_storage_class_kvs(&kvs, &[4]).expect_err("invalid parity should fail");
            assert_eq!(err.code(), &S3ErrorCode::InvalidRequest);
        });
    }

    #[test]
    #[serial_test::serial(storage_class_env)]
    fn prepare_storage_class_kvs_keeps_all_pool_geometry() {
        without_storage_class_env(|| {
            let prepared = prepare_storage_class_kvs(&KVS::new(), &[4, 2]).expect("prepare heterogeneous pools");

            assert_eq!(prepared.parities_for_sc(storageclass::STANDARD), Some(vec![2, 1]));
        });
    }

    #[test]
    #[serial_test::serial(storage_class_env)]
    fn prepare_storage_class_kvs_rejects_explicit_parity_for_later_pool() {
        let mut kvs = KVS::new();
        kvs.insert(storageclass::CLASS_STANDARD.to_string(), "EC:2".to_string());

        without_storage_class_env(|| {
            let err = prepare_storage_class_kvs(&kvs, &[4, 2]).expect_err("second pool must reject parity equal to its width");

            assert_eq!(err.code(), &S3ErrorCode::InvalidRequest);
            assert!(err.message().unwrap_or_default().contains("pool 1 (2 drives)"));
        });
    }

    #[test]
    #[serial_test::serial(storage_class_env)]
    fn prepare_storage_class_kvs_rejects_empty_topology() {
        without_storage_class_env(|| {
            let err = prepare_storage_class_kvs(&KVS::new(), &[]).expect_err("empty topology must fail closed");

            assert_eq!(err.code(), &S3ErrorCode::InvalidRequest);
            assert!(err.message().unwrap_or_default().contains("at least one pool"));
        });
    }

    #[test]
    fn missing_prepared_storage_class_fails_without_publishing() {
        let publish_calls = std::cell::Cell::new(0);

        let err = PreparedRuntimeConfig::default()
            .publish_storage_class_for_context(None)
            .expect_err("production publisher must reject a missing candidate");
        let injected_err = PreparedRuntimeConfig::default()
            .publish_storage_class_for_context_with(None, |_| publish_calls.set(publish_calls.get() + 1))
            .expect_err("missing candidate must fail closed");

        assert_eq!(err.code(), &S3ErrorCode::InternalError);
        assert_eq!(injected_err.code(), &S3ErrorCode::InternalError);
        assert_eq!(publish_calls.get(), 0);
    }

    #[test]
    #[serial_test::serial(storage_class_env)]
    fn prepared_storage_class_publishes_exact_candidate_without_reparse() {
        let candidate = without_storage_class_env(|| prepare_storage_class_kvs(&KVS::new(), &[4, 2]).expect("prepare candidate"));
        let mut published = None;

        temp_env::with_vars([(storageclass::STANDARD_ENV, Some("EC:2"))], || {
            PreparedRuntimeConfig {
                storage_class: Some(candidate),
            }
            .publish_storage_class_with(|storage_class| published = Some(storage_class));
        });

        assert_eq!(
            published.and_then(|storage_class| storage_class.parities_for_sc(storageclass::STANDARD)),
            Some(vec![2, 1])
        );
    }

    #[test]
    #[serial_test::serial(storage_class_env)]
    fn rejected_storage_class_candidate_does_not_publish() {
        without_storage_class_env(|| {
            let mut published = prepare_storage_class_kvs(&KVS::new(), &[4, 4]).expect("prepare baseline");
            let mut invalid_kvs = KVS::new();
            invalid_kvs.insert(storageclass::CLASS_STANDARD.to_string(), "EC:2".to_string());

            let rejected = match prepare_storage_class_kvs(&invalid_kvs, &[4, 2]) {
                Ok(storage_class) => {
                    PreparedRuntimeConfig {
                        storage_class: Some(storage_class),
                    }
                    .publish_storage_class_with(|storage_class| published = storage_class);
                    false
                }
                Err(err) => {
                    assert_eq!(err.code(), &S3ErrorCode::InvalidRequest);
                    true
                }
            };

            assert!(rejected);
            assert_eq!(published.parities_for_sc(storageclass::STANDARD), Some(vec![2, 2]));
        });
    }

    #[test]
    fn validate_notify_subsystem_config_rejects_invalid_webhook_endpoint() {
        crate::admin::storage_api::config::init_admin_config_defaults();
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
        crate::admin::storage_api::config::init_admin_config_defaults();
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

    #[test]
    fn validate_identity_openid_config_rejects_missing_openid_scope() {
        crate::admin::storage_api::config::init_admin_config_defaults();
        let mut config = ServerConfig::new();
        let targets = config.0.get_mut(IDENTITY_OPENID_SUB_SYS).expect("openid defaults");
        let kvs = targets.get_mut(DEFAULT_DELIMITER).expect("default target");
        kvs.insert(
            OIDC_CONFIG_URL.to_string(),
            "https://issuer.example/.well-known/openid-configuration".to_string(),
        );
        kvs.insert(OIDC_CLIENT_ID.to_string(), "console".to_string());
        kvs.insert(OIDC_SCOPES.to_string(), "profile,email".to_string());

        let err = validate_identity_openid_config(&config).expect_err("openid scope should be required");
        assert_eq!(err.code(), &S3ErrorCode::InvalidRequest);
    }

    #[test]
    fn validate_identity_openid_config_rejects_invalid_named_provider_id() {
        crate::admin::storage_api::config::init_admin_config_defaults();
        let mut config = ServerConfig::new();
        let targets = config.0.get_mut(IDENTITY_OPENID_SUB_SYS).expect("openid defaults");
        let default_kvs = targets.get(DEFAULT_DELIMITER).cloned().expect("default target");
        let mut named_kvs = default_kvs;
        named_kvs.insert(
            OIDC_CONFIG_URL.to_string(),
            "https://issuer.example/.well-known/openid-configuration".to_string(),
        );
        named_kvs.insert(OIDC_CLIENT_ID.to_string(), "console".to_string());
        targets.insert("bad$id".to_string(), named_kvs);

        let err = validate_identity_openid_config(&config).expect_err("provider id should be validated");
        assert_eq!(err.code(), &S3ErrorCode::InvalidRequest);
    }
}
