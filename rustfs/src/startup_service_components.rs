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

use crate::storage_compat::{
    ECStore, EndpointServerPools, get_global_replication_pool, init_bucket_metadata_sys, new_global_notification_sys,
    try_migrate_bucket_metadata, try_migrate_iam_config,
};
use crate::{
    config::Config,
    init::{
        add_bucket_notification_configuration, init_auto_tuner, init_buffer_profile_system, init_kms_system, init_update_check,
        print_server_info,
    },
    server::{ServiceStateManager, init_event_notifier, start_audit_system},
    startup_iam::{IamBootstrapDisposition, bootstrap_or_defer_iam_init_with_startup_kms},
};
use rustfs_audit::AuditResult;
use rustfs_common::GlobalReadiness;
use rustfs_heal::{create_ahm_services_cancel_token, heal::storage::ECStoreHealStorage, init_heal_manager};
use rustfs_iam::init_oidc_sys;
use rustfs_obs::init_metrics_runtime;
use rustfs_storage_api::{BucketOperations, BucketOptions};
use rustfs_utils::get_env_bool_with_aliases;
use std::{
    future::Future,
    io::{Error, Result},
    sync::Arc,
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

const ENV_SCANNER_ENABLED: &str = "RUSTFS_SCANNER_ENABLED";
const ENV_SCANNER_ENABLED_DEPRECATED: &str = "RUSTFS_ENABLE_SCANNER";
const ENV_HEAL_ENABLED: &str = "RUSTFS_HEAL_ENABLED";
const ENV_HEAL_ENABLED_DEPRECATED: &str = "RUSTFS_ENABLE_HEAL";
const LOG_COMPONENT_MAIN: &str = "main";
const LOG_COMPONENT_EMBEDDED: &str = "embedded";
const LOG_SUBSYSTEM_STARTUP: &str = "startup";
const LOG_SUBSYSTEM_AUTH: &str = "auth";
const LOG_SUBSYSTEM_EMBEDDED: &str = "embedded";
const EVENT_AUDIT_SYSTEM_STATE: &str = "audit_system_state";
const EVENT_DEADLOCK_DETECTOR_STATE: &str = "deadlock_detector_state";
const EVENT_EMBEDDED_OPTIONAL_SERVICE_SKIPPED: &str = "embedded_optional_service_skipped";
const EVENT_KEYSTONE_AUTH_INITIALIZED: &str = "keystone_auth_initialized";
const EVENT_KEYSTONE_AUTH_INITIALIZATION_FAILED: &str = "keystone_auth_initialization_failed";
const EVENT_OIDC_INITIALIZATION_FAILED: &str = "oidc_initialization_failed";
const EVENT_NOTIFICATION_SYSTEM_INITIALIZATION_FAILED: &str = "notification_system_initialization_failed";
const EVENT_BACKGROUND_SERVICES_CONFIGURED: &str = "background_services_configured";

pub(crate) async fn init_audit_runtime() {
    match init_event_notifier_and_audit().await {
        Ok(()) => info!(
            target: "rustfs::main::run",
            event = EVENT_AUDIT_SYSTEM_STATE,
            component = LOG_COMPONENT_MAIN,
            subsystem = LOG_SUBSYSTEM_STARTUP,
            state = "started",
            "Audit runtime started"
        ),
        Err(e) => error!(
            target: "rustfs::main::run",
            event = EVENT_AUDIT_SYSTEM_STATE,
            component = LOG_COMPONENT_MAIN,
            subsystem = LOG_SUBSYSTEM_STARTUP,
            state = "start_failed",
            error = %e,
            "Audit runtime failed to start"
        ),
    }
}

pub(crate) async fn init_embedded_optional_service_runtime(config: &Config) {
    if let Err(err) = init_kms_system(config).await {
        log_embedded_optional_service_skipped("kms", err);
    }

    init_buffer_profile_system(config);

    if let Err(err) = init_event_notifier_and_audit().await {
        log_embedded_optional_service_skipped("audit", err);
    }
}

pub(crate) fn init_deadlock_detector_runtime() {
    let detector = crate::storage::deadlock_detector::get_deadlock_detector();
    if detector.is_enabled() {
        detector.start();
        info!(
            target: "rustfs::main::run",
            event = EVENT_DEADLOCK_DETECTOR_STATE,
            component = LOG_COMPONENT_MAIN,
            subsystem = LOG_SUBSYSTEM_STARTUP,
            state = "started",
            "Deadlock detector started"
        );
    } else {
        info!(
            target: "rustfs::main::run",
            event = EVENT_DEADLOCK_DETECTOR_STATE,
            component = LOG_COMPONENT_MAIN,
            subsystem = LOG_SUBSYSTEM_STARTUP,
            state = "disabled",
            "Deadlock detector disabled"
        );
    }
}

pub(crate) async fn init_embedded_bucket_metadata_runtime(store: Arc<ECStore>) -> Result<Vec<String>> {
    let buckets_list = store
        .list_bucket(&BucketOptions {
            no_metadata: true,
            ..Default::default()
        })
        .await
        .map_err(|err| Error::other(format!("list_bucket: {err}")))?;

    let buckets: Vec<String> = buckets_list.into_iter().map(|v| v.name).collect();

    try_migrate_bucket_metadata(store.clone()).await;
    init_bucket_metadata_sys(store.clone(), buckets.clone()).await;
    try_migrate_iam_config(store).await;

    Ok(buckets)
}

pub(crate) async fn init_bucket_metadata_runtime(store: Arc<ECStore>, ctx: CancellationToken) -> Result<Vec<String>> {
    let buckets_list = store
        .list_bucket(&BucketOptions {
            no_metadata: true,
            ..Default::default()
        })
        .await
        .map_err(Error::other)?;

    let buckets: Vec<String> = buckets_list.into_iter().map(|v| v.name).collect();

    try_migrate_bucket_metadata(store.clone()).await;

    if let Some(pool) = get_global_replication_pool() {
        pool.init_resync(ctx, buckets.clone()).await?;
    }

    try_migrate_iam_config(store.clone()).await;
    init_bucket_metadata_sys(store, buckets.clone()).await;

    Ok(buckets)
}

pub(crate) async fn init_embedded_iam_runtime(
    store: Arc<ECStore>,
    ctx: CancellationToken,
    readiness: Arc<GlobalReadiness>,
) -> Result<IamBootstrapDisposition> {
    bootstrap_or_defer_iam_init_with_startup_kms(store, readiness, None, Some(ctx)).await
}

pub(crate) async fn init_iam_runtime(
    store: Arc<ECStore>,
    ctx: CancellationToken,
    readiness: Arc<GlobalReadiness>,
    state_manager: Arc<ServiceStateManager>,
) -> Result<IamBootstrapDisposition> {
    bootstrap_or_defer_iam_init_with_startup_kms(store, readiness, Some(state_manager), Some(ctx)).await
}

pub(crate) async fn init_embedded_notification_runtime(endpoint_pools: EndpointServerPools, buckets: Vec<String>) {
    add_bucket_notification_configuration(buckets).await;

    if let Err(err) = init_notification_system(endpoint_pools).await {
        log_embedded_optional_service_skipped("notification", err);
    }
}

pub(crate) async fn init_auth_integrations() -> Result<()> {
    let keystone_config = rustfs_keystone::KeystoneConfig::from_env().map_err(Error::other)?;
    if keystone_config.enable {
        match crate::auth_keystone::init_keystone_auth(keystone_config).await {
            Ok(_) => info!(
                event = EVENT_KEYSTONE_AUTH_INITIALIZED,
                component = LOG_COMPONENT_MAIN,
                subsystem = LOG_SUBSYSTEM_AUTH,
                "Initialized Keystone authentication"
            ),
            Err(e) => {
                error!(
                    event = EVENT_KEYSTONE_AUTH_INITIALIZATION_FAILED,
                    component = LOG_COMPONENT_MAIN,
                    subsystem = LOG_SUBSYSTEM_AUTH,
                    error = %e,
                    "Failed to initialize Keystone authentication"
                );
            }
        }
    }

    if let Err(e) = init_oidc_sys().await {
        warn!(
            event = EVENT_OIDC_INITIALIZATION_FAILED,
            component = LOG_COMPONENT_MAIN,
            subsystem = LOG_SUBSYSTEM_AUTH,
            error = %e,
            "OIDC initialization failed; continuing without OIDC providers"
        );
    }

    Ok(())
}

pub(crate) async fn init_notification_runtime(endpoint_pools: EndpointServerPools, buckets: Vec<String>) -> Result<()> {
    add_bucket_notification_configuration(buckets).await;

    init_notification_system(endpoint_pools).await.map_err(|err| {
        error!(
            event = EVENT_NOTIFICATION_SYSTEM_INITIALIZATION_FAILED,
            component = LOG_COMPONENT_MAIN,
            subsystem = LOG_SUBSYSTEM_STARTUP,
            error = ?err,
            "Failed to initialize notification system"
        );
        Error::other(err)
    })
}

pub(crate) async fn init_background_service_runtime(store: Arc<ECStore>) -> Result<bool> {
    let _ = create_ahm_services_cancel_token();

    let enable_scanner = get_env_bool_with_aliases(ENV_SCANNER_ENABLED, &[ENV_SCANNER_ENABLED_DEPRECATED], true);
    let enable_heal = get_env_bool_with_aliases(ENV_HEAL_ENABLED, &[ENV_HEAL_ENABLED_DEPRECATED], true);

    info!(
        target: "rustfs::main::run",
        event = EVENT_BACKGROUND_SERVICES_CONFIGURED,
        component = LOG_COMPONENT_MAIN,
        subsystem = LOG_SUBSYSTEM_STARTUP,
        enable_scanner = enable_scanner,
        enable_heal = enable_heal,
        "Background services configured"
    );

    if enable_heal || enable_scanner {
        let heal_storage = Arc::new(ECStoreHealStorage::new(store));
        init_heal_manager(heal_storage, None).await?;
    }

    if !enable_heal && !enable_scanner {
        debug!(
            target: "rustfs::main::run",
            event = EVENT_BACKGROUND_SERVICES_CONFIGURED,
            component = LOG_COMPONENT_MAIN,
            subsystem = LOG_SUBSYSTEM_STARTUP,
            enable_scanner = false,
            enable_heal = false,
            ahm_state = "skipped",
            reason = "disabled",
            "Background services disabled"
        );
    }

    Ok(enable_scanner)
}

pub(crate) async fn init_observability_runtime(ctx: CancellationToken) {
    print_server_info();
    init_update_check();
    crate::allocator_reclaim::init_allocator_reclaim(ctx.clone());

    if rustfs_obs::observability_metric_enabled() {
        rustfs_io_metrics::set_put_stage_metrics_enabled(true);
        init_metrics_runtime(ctx.clone());
        crate::memory_observability::init_memory_observability(ctx.clone());
        init_auto_tuner(ctx).await;
    }
}

pub(crate) async fn init_event_notifier_and_audit() -> AuditResult<()> {
    init_event_notifier_and_audit_with(init_event_notifier, start_audit_system).await
}

async fn init_event_notifier_and_audit_with<NotifyFn, NotifyFuture, AuditFn, AuditFuture>(
    notify: NotifyFn,
    start_audit: AuditFn,
) -> AuditResult<()>
where
    NotifyFn: FnOnce() -> NotifyFuture,
    NotifyFuture: Future<Output = ()>,
    AuditFn: FnOnce() -> AuditFuture,
    AuditFuture: Future<Output = AuditResult<()>>,
{
    notify().await;
    start_audit().await
}

pub(crate) async fn init_notification_system(endpoint_pools: EndpointServerPools) -> crate::storage_compat::EcstoreResult<()> {
    init_notification_system_with(|| new_global_notification_sys(endpoint_pools)).await
}

async fn init_notification_system_with<InitFn, InitFuture>(init_notification: InitFn) -> crate::storage_compat::EcstoreResult<()>
where
    InitFn: FnOnce() -> InitFuture,
    InitFuture: Future<Output = crate::storage_compat::EcstoreResult<()>>,
{
    init_notification().await
}

fn log_embedded_optional_service_skipped(service: &str, err: impl std::fmt::Display) {
    warn!(
        component = LOG_COMPONENT_EMBEDDED,
        subsystem = LOG_SUBSYSTEM_EMBEDDED,
        event = EVENT_EMBEDDED_OPTIONAL_SERVICE_SKIPPED,
        service,
        error = %err,
        "Embedded optional service initialization skipped"
    );
}

#[cfg(test)]
mod tests {
    use super::{init_event_notifier_and_audit_with, init_notification_system_with};
    use rustfs_audit::AuditError;
    use std::sync::{Arc, Mutex};

    #[tokio::test]
    async fn event_notifier_runs_before_successful_audit_start() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let notify_events = events.clone();
        let audit_events = events.clone();
        let result = init_event_notifier_and_audit_with(
            move || async move {
                notify_events.lock().unwrap_or_else(|err| err.into_inner()).push("notify");
            },
            move || async move {
                audit_events.lock().unwrap_or_else(|err| err.into_inner()).push("audit");
                Ok(())
            },
        )
        .await;

        assert!(result.is_ok());
        let events = events.lock().unwrap_or_else(|err| err.into_inner()).clone();
        assert_eq!(events, ["notify", "audit"]);
    }

    #[tokio::test]
    async fn event_notifier_runs_before_failed_audit_result() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let notify_events = events.clone();
        let audit_events = events.clone();

        let result = init_event_notifier_and_audit_with(
            move || async move {
                notify_events.lock().unwrap_or_else(|err| err.into_inner()).push("notify");
            },
            move || async move {
                audit_events.lock().unwrap_or_else(|err| err.into_inner()).push("audit");
                Err(AuditError::ConfigNotLoaded)
            },
        )
        .await;

        assert!(result.is_err());
        let events = events.lock().unwrap_or_else(|err| err.into_inner()).clone();
        assert_eq!(events, ["notify", "audit"]);
    }

    #[tokio::test]
    async fn notification_system_returns_source_error() {
        let result = init_notification_system_with(|| async { Err(crate::storage_compat::EcstoreError::FaultyDisk) }).await;

        assert!(result.is_err());
    }
}
