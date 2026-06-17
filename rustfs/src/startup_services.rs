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
    config::Config,
    init::{
        add_bucket_notification_configuration, init_auto_tuner, init_buffer_profile_system, init_kms_system, init_update_check,
        print_server_info,
    },
    server::{
        ServiceState, ServiceStateManager, ShutdownHandle, ShutdownSignal, init_event_notifier, shutdown_event_notifier,
        start_audit_system, stop_audit_system, wait_for_shutdown,
    },
    startup_iam::{IamBootstrapDisposition, bootstrap_or_defer_iam_init, publish_ready_for_iam_bootstrap},
    startup_protocols::{ProtocolShutdownSenders, init_protocol_shutdown_senders},
};
use futures_util::future::join_all;
use rustfs_audit::AuditResult;
use rustfs_common::GlobalReadiness;
use rustfs_ecstore::{
    bucket::{
        metadata_sys::init_bucket_metadata_sys,
        migration::{try_migrate_bucket_metadata, try_migrate_iam_config},
        replication::get_global_replication_pool,
    },
    endpoints::EndpointServerPools,
    global::shutdown_background_services,
    notification_sys::new_global_notification_sys,
    store::ECStore,
};
use rustfs_heal::{
    create_ahm_services_cancel_token, heal::storage::ECStoreHealStorage, init_heal_manager, shutdown_ahm_services,
};
use rustfs_iam::init_oidc_sys;
use rustfs_obs::init_metrics_runtime;
use rustfs_scanner::init_data_scanner;
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
const LOG_SUBSYSTEM_STARTUP: &str = "startup";
const LOG_SUBSYSTEM_AUTH: &str = "auth";
const EVENT_AUDIT_SYSTEM_STATE: &str = "audit_system_state";
const EVENT_PROTOCOL_SYSTEM_STATE: &str = "protocol_system_state";
const EVENT_DEADLOCK_DETECTOR_STATE: &str = "deadlock_detector_state";
const EVENT_KEYSTONE_AUTH_INITIALIZED: &str = "keystone_auth_initialized";
const EVENT_KEYSTONE_AUTH_INITIALIZATION_FAILED: &str = "keystone_auth_initialization_failed";
const EVENT_OIDC_INITIALIZATION_FAILED: &str = "oidc_initialization_failed";
const EVENT_NOTIFICATION_SYSTEM_INITIALIZATION_FAILED: &str = "notification_system_initialization_failed";
const EVENT_BACKGROUND_SERVICES_CONFIGURED: &str = "background_services_configured";
const EVENT_SERVER_READY: &str = "server_ready";
const EVENT_SHUTDOWN_SIGNAL_RECEIVED: &str = "shutdown_signal_received";
const EVENT_BACKGROUND_SERVICE_SHUTDOWN: &str = "background_service_shutdown";
const EVENT_EVENT_NOTIFIER_SHUTDOWN: &str = "event_notifier_shutdown";
const EVENT_PROFILING_SHUTDOWN: &str = "profiling_shutdown";
const EVENT_SERVER_SHUTDOWN_STATE: &str = "server_shutdown_state";

pub struct StartupServiceRuntime {
    pub protocol_shutdowns: ProtocolShutdownSenders,
    pub iam_bootstrap: IamBootstrapDisposition,
    pub enable_scanner: bool,
}

pub struct StartupRuntimeLifecycle {
    pub server_address: String,
    pub state_manager: Arc<ServiceStateManager>,
    pub s3_shutdown_tx: Option<ShutdownHandle>,
    pub console_shutdown_tx: Option<ShutdownHandle>,
    pub service_runtime: StartupServiceRuntime,
    pub store: Arc<ECStore>,
    pub shutdown_token: CancellationToken,
    pub readiness: Arc<GlobalReadiness>,
}

pub async fn init_startup_runtime_services(
    config: &Config,
    endpoint_pools: EndpointServerPools,
    store: Arc<ECStore>,
    ctx: CancellationToken,
    readiness: Arc<GlobalReadiness>,
    state_manager: Arc<ServiceStateManager>,
) -> Result<StartupServiceRuntime> {
    init_kms_system(config).await?;

    let protocol_shutdowns = init_protocol_shutdown_senders().await?;

    init_buffer_profile_system(config);
    init_audit_runtime().await;
    init_deadlock_detector_runtime();

    let buckets = init_bucket_metadata_runtime(store.clone(), ctx.clone()).await?;
    let iam_bootstrap = init_iam_runtime(store.clone(), ctx.clone(), readiness, state_manager).await?;
    init_auth_integrations().await?;
    init_notification_runtime(endpoint_pools, buckets).await?;
    let enable_scanner = init_background_service_runtime(store.clone()).await?;
    init_observability_runtime(ctx.clone()).await;

    Ok(StartupServiceRuntime {
        protocol_shutdowns,
        iam_bootstrap,
        enable_scanner,
    })
}

pub async fn run_startup_runtime_lifecycle(lifecycle: StartupRuntimeLifecycle) -> Result<()> {
    let StartupRuntimeLifecycle {
        server_address,
        state_manager,
        s3_shutdown_tx,
        console_shutdown_tx,
        service_runtime,
        store,
        shutdown_token,
        readiness,
    } = lifecycle;
    let StartupServiceRuntime {
        protocol_shutdowns,
        iam_bootstrap,
        enable_scanner,
    } = service_runtime;

    info!(
        target: "rustfs::main::run",
        event = EVENT_SERVER_READY,
        component = LOG_COMPONENT_MAIN,
        subsystem = LOG_SUBSYSTEM_STARTUP,
        version = %crate::version::get_version(),
        server_address = %server_address,
        started_at = %jiff::Zoned::now(),
        iam_bootstrap = ?iam_bootstrap,
        "RustFS server ready"
    );
    publish_ready_for_iam_bootstrap(iam_bootstrap, readiness.as_ref(), Some(state_manager.as_ref())).await?;
    rustfs_common::set_global_init_time_now().await;

    if enable_scanner {
        init_data_scanner(shutdown_token.clone(), store).await;
    }

    let shutdown_signal = wait_for_shutdown().await;
    handle_shutdown(
        &state_manager,
        shutdown_signal,
        s3_shutdown_tx,
        console_shutdown_tx,
        protocol_shutdowns,
        shutdown_token,
    )
    .await;

    info!(
        target: "rustfs::main::run",
        event = EVENT_SERVER_SHUTDOWN_STATE,
        component = LOG_COMPONENT_MAIN,
        subsystem = LOG_SUBSYSTEM_STARTUP,
        state = ?state_manager.current_state(),
        result = "stopped",
        "RustFS server stopped"
    );
    Ok(())
}

async fn init_audit_runtime() {
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

fn init_deadlock_detector_runtime() {
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

async fn init_bucket_metadata_runtime(store: Arc<ECStore>, ctx: CancellationToken) -> Result<Vec<String>> {
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

async fn init_iam_runtime(
    store: Arc<ECStore>,
    ctx: CancellationToken,
    readiness: Arc<GlobalReadiness>,
    state_manager: Arc<ServiceStateManager>,
) -> Result<IamBootstrapDisposition> {
    let kms_interface = rustfs_kms::get_global_kms_service_manager().unwrap_or_else(rustfs_kms::init_global_kms_service_manager);
    bootstrap_or_defer_iam_init(store, kms_interface, readiness, Some(state_manager), Some(ctx)).await
}

async fn init_auth_integrations() -> Result<()> {
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

async fn init_notification_runtime(endpoint_pools: EndpointServerPools, buckets: Vec<String>) -> Result<()> {
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

async fn init_background_service_runtime(store: Arc<ECStore>) -> Result<bool> {
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

async fn init_observability_runtime(ctx: CancellationToken) {
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BackgroundShutdownStep {
    DataScanner,
    Ahm,
}

fn background_shutdown_steps(enable_scanner: bool, enable_heal: bool) -> Vec<BackgroundShutdownStep> {
    let mut steps = Vec::with_capacity(2);
    if enable_scanner {
        steps.push(BackgroundShutdownStep::DataScanner);
    }
    if enable_heal || enable_scanner {
        steps.push(BackgroundShutdownStep::Ahm);
    }
    steps
}

async fn handle_shutdown(
    state_manager: &ServiceStateManager,
    shutdown_signal: ShutdownSignal,
    s3_shutdown_handle: Option<ShutdownHandle>,
    console_shutdown_handle: Option<ShutdownHandle>,
    protocols: ProtocolShutdownSenders,
    ctx: CancellationToken,
) {
    let ProtocolShutdownSenders {
        ftp: ftp_shutdown_tx,
        ftps: ftps_shutdown_tx,
        webdav: webdav_shutdown_tx,
        sftp: sftp_shutdown_tx,
    } = protocols;
    ctx.cancel();

    info!(
        target: "rustfs::main::handle_shutdown",
        event = EVENT_SHUTDOWN_SIGNAL_RECEIVED,
        component = LOG_COMPONENT_MAIN,
        subsystem = LOG_SUBSYSTEM_STARTUP,
        signal = shutdown_signal.log_label(),
        "Shutdown signal received"
    );
    state_manager.update(ServiceState::Stopping);

    let enable_scanner = get_env_bool_with_aliases(ENV_SCANNER_ENABLED, &[ENV_SCANNER_ENABLED_DEPRECATED], true);
    let enable_heal = get_env_bool_with_aliases(ENV_HEAL_ENABLED, &[ENV_HEAL_ENABLED_DEPRECATED], true);

    let background_steps = background_shutdown_steps(enable_scanner, enable_heal);
    for step in &background_steps {
        match step {
            BackgroundShutdownStep::DataScanner => {
                info!(
                    target: "rustfs::main::handle_shutdown",
                    event = EVENT_BACKGROUND_SERVICE_SHUTDOWN,
                    component = LOG_COMPONENT_MAIN,
                    subsystem = LOG_SUBSYSTEM_STARTUP,
                    service = "data_scanner",
                    state = "stopping",
                    "Background service shutdown started"
                );
                shutdown_background_services();
            }
            BackgroundShutdownStep::Ahm => {
                info!(
                    target: "rustfs::main::handle_shutdown",
                    event = EVENT_BACKGROUND_SERVICE_SHUTDOWN,
                    component = LOG_COMPONENT_MAIN,
                    subsystem = LOG_SUBSYSTEM_STARTUP,
                    service = "ahm",
                    state = "stopping",
                    "Background service shutdown started"
                );
                shutdown_ahm_services();
            }
        }
    }

    if background_steps.is_empty() {
        info!(
            target: "rustfs::main::handle_shutdown",
            event = EVENT_BACKGROUND_SERVICE_SHUTDOWN,
            component = LOG_COMPONENT_MAIN,
            subsystem = LOG_SUBSYSTEM_STARTUP,
            service = "ahm",
            state = "skipped",
            reason = "disabled",
            "Background service shutdown skipped"
        );
    }

    let mut protocol_shutdowns = Vec::new();
    if let Some(ftp_shutdown_tx) = ftp_shutdown_tx {
        info!(
            target: "rustfs::main::handle_shutdown",
            event = EVENT_PROTOCOL_SYSTEM_STATE,
            component = LOG_COMPONENT_MAIN,
            subsystem = LOG_SUBSYSTEM_STARTUP,
            protocol = "ftp",
            state = "stopping",
            "Protocol runtime stopping"
        );
        protocol_shutdowns.push(ftp_shutdown_tx.shutdown());
    }

    if let Some(ftps_shutdown_tx) = ftps_shutdown_tx {
        info!(
            target: "rustfs::main::handle_shutdown",
            event = EVENT_PROTOCOL_SYSTEM_STATE,
            component = LOG_COMPONENT_MAIN,
            subsystem = LOG_SUBSYSTEM_STARTUP,
            protocol = "ftps",
            state = "stopping",
            "Protocol runtime stopping"
        );
        protocol_shutdowns.push(ftps_shutdown_tx.shutdown());
    }

    if let Some(webdav_shutdown_tx) = webdav_shutdown_tx {
        info!(
            target: "rustfs::main::handle_shutdown",
            event = EVENT_PROTOCOL_SYSTEM_STATE,
            component = LOG_COMPONENT_MAIN,
            subsystem = LOG_SUBSYSTEM_STARTUP,
            protocol = "webdav",
            state = "stopping",
            "Protocol runtime stopping"
        );
        protocol_shutdowns.push(webdav_shutdown_tx.shutdown());
    }

    if let Some(sftp_shutdown_tx) = sftp_shutdown_tx {
        info!(
            target: "rustfs::main::handle_shutdown",
            event = EVENT_PROTOCOL_SYSTEM_STATE,
            component = LOG_COMPONENT_MAIN,
            subsystem = LOG_SUBSYSTEM_STARTUP,
            protocol = "sftp",
            state = "stopping",
            "Protocol runtime stopping"
        );
        protocol_shutdowns.push(sftp_shutdown_tx.shutdown());
    }

    info!(
        target: "rustfs::main::handle_shutdown",
        event = EVENT_EVENT_NOTIFIER_SHUTDOWN,
        component = LOG_COMPONENT_MAIN,
        subsystem = LOG_SUBSYSTEM_STARTUP,
        state = "stopping",
        "Event notifier shutdown started"
    );
    shutdown_event_notifier().await;

    info!(
        target: "rustfs::main::handle_shutdown",
        event = EVENT_AUDIT_SYSTEM_STATE,
        component = LOG_COMPONENT_MAIN,
        subsystem = LOG_SUBSYSTEM_STARTUP,
        state = "stopping",
        "Audit runtime stopping"
    );
    match stop_audit_system().await {
        Ok(_) => info!(
            target: "rustfs::main::handle_shutdown",
            event = EVENT_AUDIT_SYSTEM_STATE,
            component = LOG_COMPONENT_MAIN,
            subsystem = LOG_SUBSYSTEM_STARTUP,
            state = "stopped",
            "Audit runtime stopped"
        ),
        Err(e) => error!(
            target: "rustfs::main::handle_shutdown",
            event = EVENT_AUDIT_SYSTEM_STATE,
            component = LOG_COMPONENT_MAIN,
            subsystem = LOG_SUBSYSTEM_STARTUP,
            state = "stop_failed",
            error = %e,
            "Audit runtime failed to stop"
        ),
    }

    info!(
        target: "rustfs::main::handle_shutdown",
        event = EVENT_PROFILING_SHUTDOWN,
        component = LOG_COMPONENT_MAIN,
        subsystem = LOG_SUBSYSTEM_STARTUP,
        state = "stopping",
        "Profiling shutdown started"
    );
    crate::profiling::shutdown_profiling();

    info!(
        target: "rustfs::main::handle_shutdown",
        event = EVENT_SERVER_SHUTDOWN_STATE,
        component = LOG_COMPONENT_MAIN,
        subsystem = LOG_SUBSYSTEM_STARTUP,
        state = "stopping",
        "RustFS server stopping"
    );
    if let Some(s3_shutdown_handle) = s3_shutdown_handle {
        s3_shutdown_handle.shutdown().await;
    }
    if let Some(console_shutdown_handle) = console_shutdown_handle {
        console_shutdown_handle.shutdown().await;
    }
    join_all(protocol_shutdowns).await;

    state_manager.update(ServiceState::Stopped);
    info!(
        target: "rustfs::main::handle_shutdown",
        event = EVENT_SERVER_SHUTDOWN_STATE,
        component = LOG_COMPONENT_MAIN,
        subsystem = LOG_SUBSYSTEM_STARTUP,
        state = "stopped",
        "RustFS server stopped"
    );
}

pub async fn init_event_notifier_and_audit() -> AuditResult<()> {
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

pub async fn init_notification_system(endpoint_pools: EndpointServerPools) -> rustfs_ecstore::error::Result<()> {
    init_notification_system_with(|| new_global_notification_sys(endpoint_pools)).await
}

async fn init_notification_system_with<InitFn, InitFuture>(init_notification: InitFn) -> rustfs_ecstore::error::Result<()>
where
    InitFn: FnOnce() -> InitFuture,
    InitFuture: Future<Output = rustfs_ecstore::error::Result<()>>,
{
    init_notification().await
}

#[cfg(test)]
mod tests {
    use super::{
        BackgroundShutdownStep, background_shutdown_steps, init_event_notifier_and_audit_with, init_notification_system_with,
    };
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
        let result = init_notification_system_with(|| async { Err(rustfs_ecstore::error::Error::FaultyDisk) }).await;

        assert!(result.is_err());
    }

    #[test]
    fn background_shutdown_plan_keeps_scanner_before_ahm() {
        assert_eq!(
            background_shutdown_steps(true, true),
            vec![BackgroundShutdownStep::DataScanner, BackgroundShutdownStep::Ahm]
        );
        assert_eq!(
            background_shutdown_steps(true, false),
            vec![BackgroundShutdownStep::DataScanner, BackgroundShutdownStep::Ahm]
        );
        assert_eq!(background_shutdown_steps(false, true), vec![BackgroundShutdownStep::Ahm]);
        assert!(background_shutdown_steps(false, false).is_empty());
    }
}
