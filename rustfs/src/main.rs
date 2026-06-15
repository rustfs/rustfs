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

// Ensure the correct path for parse_license is imported
use rustfs::init::{
    add_bucket_notification_configuration, init_buffer_profile_system, init_kms_system, init_update_check, print_server_info,
};

use futures_util::future::join_all;
use rustfs::server::{
    ServiceState, ServiceStateManager, ShutdownHandle, ShutdownSignal, shutdown_event_notifier, stop_audit_system,
    wait_for_shutdown,
};
use rustfs::startup_fs_guard::enforce_unsupported_fs_policy;
use rustfs::startup_iam::{bootstrap_or_defer_iam_init, publish_ready_for_iam_bootstrap};
use rustfs::startup_preflight::{StartupServerPreflightError, bootstrap_external_prefix_compat, init_startup_server_preflight};
use rustfs::startup_protocols::{ProtocolShutdownSenders, init_protocol_shutdown_senders};
use rustfs::startup_server::{StartupHttpServers, StartupListenContext, init_startup_http_servers, init_startup_listen_context};
use rustfs_common::SystemStage;
use rustfs_ecstore::store::init_lock_clients;
use rustfs_ecstore::{
    bucket::metadata_sys::init_bucket_metadata_sys,
    bucket::migration::{try_migrate_bucket_metadata, try_migrate_iam_config},
    bucket::replication::{get_global_replication_pool, init_background_replication},
    config as ecconfig,
    endpoints::EndpointServerPools,
    global::shutdown_background_services,
    set_global_endpoints,
    store::ECStore,
    store::init_local_disks,
    store::prewarm_local_disk_id_map,
    store_api::BucketOperations,
    update_erasure_type,
};
use rustfs_heal::{
    create_ahm_services_cancel_token, heal::storage::ECStoreHealStorage, init_heal_manager, shutdown_ahm_services,
};
use rustfs_iam::init_oidc_sys;
use rustfs_obs::init_metrics_runtime;
use rustfs_scanner::init_data_scanner;
use rustfs_storage_api::BucketOptions;
use rustfs_utils::get_env_bool_with_aliases;
use std::io::{Error, Result};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, instrument, warn};

const ENV_SCANNER_ENABLED: &str = "RUSTFS_SCANNER_ENABLED";
const ENV_SCANNER_ENABLED_DEPRECATED: &str = "RUSTFS_ENABLE_SCANNER";
const ENV_HEAL_ENABLED: &str = "RUSTFS_HEAL_ENABLED";
const ENV_HEAL_ENABLED_DEPRECATED: &str = "RUSTFS_ENABLE_HEAL";
const LOG_COMPONENT_MAIN: &str = "main";
const LOG_SUBSYSTEM_STARTUP: &str = "startup";
const LOG_SUBSYSTEM_AUTH: &str = "auth";
const LOG_SUBSYSTEM_STORAGE: &str = "storage";
const EVENT_SERVER_RUNTIME_FAILED: &str = "server_runtime_failed";
const EVENT_ENDPOINT_PARSING_STARTED: &str = "endpoint_parsing_started";
const EVENT_STARTUP_STORAGE_STAGE: &str = "startup_storage_stage";
const EVENT_STORAGE_POOL_FORMATTING: &str = "storage_pool_formatting";
const EVENT_STORAGE_POOL_HOST_RISK: &str = "storage_pool_host_risk";
const EVENT_PROTOCOL_SYSTEM_STATE: &str = "protocol_system_state";
const EVENT_AUDIT_SYSTEM_STATE: &str = "audit_system_state";
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
const OBSERVABILITY_INIT_FATAL_ALREADY_REPORTED: &str = "observability initialization failure already reported";
#[cfg(all(target_os = "linux", target_env = "gnu", target_arch = "x86_64"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[cfg(not(all(target_os = "linux", target_env = "gnu", target_arch = "x86_64")))]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn format_fatal_stderr_message(context: &str, error: impl std::fmt::Display) -> String {
    format!("[FATAL] {context}: {error}")
}

fn emit_fatal_stderr(context: &str, error: impl std::fmt::Display) {
    eprintln!("{}", format_fatal_stderr_message(context, error));
}

fn main() {
    // Build Tokio runtime with optional dial9 telemetry support
    let runtime = rustfs::server::build_tokio_runtime().expect("Failed to build Tokio runtime");
    let result = runtime.block_on(async_main());
    if let Err(ref e) = result {
        if e.to_string() != OBSERVABILITY_INIT_FATAL_ALREADY_REPORTED {
            // Tracing may not be initialized when startup fails this early.
            emit_fatal_stderr("Server runtime failed", e);
        }
        std::process::exit(1);
    }
}

async fn async_main() -> Result<()> {
    let env_compat_report = bootstrap_external_prefix_compat()?;

    // Parse command line arguments
    let args: Vec<String> = std::env::args().collect();
    let command_result = match rustfs::config::Opt::parse_command(args) {
        Ok(result) => result,
        Err(e) => {
            emit_fatal_stderr("Command parse failed", e);
            std::process::exit(1);
        }
    };

    // Execute subcommand, or prepare config for `server` subcommand
    let config = match command_result {
        rustfs::config::CommandResult::Info(opts) => {
            rustfs::config::execute_info(&opts);
            return Ok(());
        }
        rustfs::config::CommandResult::Tls(opts) => return rustfs::tls::execute_tls(&opts),
        rustfs::config::CommandResult::Server(config) => config,
    };

    match init_startup_server_preflight(&config, &env_compat_report).await {
        Ok(()) => {}
        Err(StartupServerPreflightError::ObservabilityInit(err)) => {
            // Structured logging is unavailable until observability initializes.
            emit_fatal_stderr("Observability initialization failed", err);
            return Err(Error::other(OBSERVABILITY_INIT_FATAL_ALREADY_REPORTED));
        }
        Err(StartupServerPreflightError::Other(err)) => return Err(err),
    }

    // Run parameters
    match run(*config).await {
        Ok(_) => Ok(()),
        Err(e) => {
            error!(
                target: "rustfs::main",
                event = EVENT_SERVER_RUNTIME_FAILED,
                component = LOG_COMPONENT_MAIN,
                subsystem = LOG_SUBSYSTEM_STARTUP,
                error = %e,
                "Server runtime failed"
            );
            Err(e)
        }
    }
}

#[instrument(skip(config))]
async fn run(config: rustfs::config::Config) -> Result<()> {
    let StartupListenContext {
        readiness,
        server_addr,
        server_address,
    } = init_startup_listen_context(&config).await?;

    // For RPC
    info!(
        target: "rustfs::main::run",
        event = EVENT_ENDPOINT_PARSING_STARTED,
        component = LOG_COMPONENT_MAIN,
        subsystem = LOG_SUBSYSTEM_STORAGE,
        server_address = %server_address,
        volume_count = config.volumes.len(),
        "Starting endpoint parsing"
    );
    let (endpoint_pools, setup_type) = EndpointServerPools::from_volumes(server_address.clone().as_str(), config.volumes.clone())
        .await
        .inspect_err(|err| {
            error!(
                target: "rustfs::main::run",
                event = EVENT_STARTUP_STORAGE_STAGE,
                component = LOG_COMPONENT_MAIN,
                subsystem = LOG_SUBSYSTEM_STORAGE,
                stage = "endpoint_parsing",
                state = "failed",
                error = ?err,
                "Endpoint parsing failed"
            );
        })
        .map_err(Error::other)?;
    enforce_unsupported_fs_policy(&endpoint_pools)?;

    set_global_endpoints(endpoint_pools.as_ref().clone());
    update_erasure_type(setup_type).await;

    // Initialize the local disk
    debug!(
        target: "rustfs::main::run",
        event = EVENT_STARTUP_STORAGE_STAGE,
        component = LOG_COMPONENT_MAIN,
        subsystem = LOG_SUBSYSTEM_STORAGE,
        stage = "local_disk_initialization",
        state = "starting",
        "starting local disk initialization"
    );
    init_local_disks(endpoint_pools.clone())
        .await
        .inspect_err(|err| {
            error!(
                target: "rustfs::main::run",
                event = EVENT_STARTUP_STORAGE_STAGE,
                component = LOG_COMPONENT_MAIN,
                subsystem = LOG_SUBSYSTEM_STORAGE,
                stage = "local_disk_initialization",
                state = "failed",
                error = ?err,
                "Local disk initialization failed"
            );
        })
        .map_err(Error::other)?;
    prewarm_local_disk_id_map().await;
    // Initialize the lock clients

    init_lock_clients(endpoint_pools.clone());

    for (i, eps) in endpoint_pools.as_ref().iter().enumerate() {
        info!(
            target: "rustfs::main::run",
            event = EVENT_STORAGE_POOL_FORMATTING,
            component = LOG_COMPONENT_MAIN,
            subsystem = LOG_SUBSYSTEM_STORAGE,
            pool_id = i + 1,
            set_count = eps.set_count,
            drives_per_set = eps.drives_per_set,
            "Formatting storage pool"
        );

        if eps.drives_per_set > 1 {
            warn!(
                target: "rustfs::main::run",
                event = EVENT_STORAGE_POOL_HOST_RISK,
                component = LOG_COMPONENT_MAIN,
                subsystem = LOG_SUBSYSTEM_STORAGE,
                pool_id = i + 1,
                drives_per_set = eps.drives_per_set,
                risk = "host_failure_data_unavailable",
                "Detected multi-drive local set host failure risk"
            );
        }
    }

    for (i, eps) in endpoint_pools.as_ref().iter().enumerate() {
        debug!(
            target: "rustfs::main::run",
            id = i,
            set_count = eps.set_count,
            drives_per_set = eps.drives_per_set,
            cmd = ?eps.cmd_line,
            "created endpoints {}, set_count:{}, drives_per_set: {}, cmd: {:?}",
            i, eps.set_count, eps.drives_per_set, eps.cmd_line
        );

        for ep in eps.endpoints.as_ref().iter() {
            debug!(
                target: "rustfs::main::run",
                "  - endpoint: {}", ep
            );
        }
    }
    let StartupHttpServers {
        state_manager,
        s3_shutdown_tx,
        console_shutdown_tx,
    } = init_startup_http_servers(&config, readiness.clone()).await?;

    let ctx = CancellationToken::new();

    // init store
    // 2. Start Storage Engine (ECStore)
    debug!(
        target: "rustfs::main::run",
        event = EVENT_STARTUP_STORAGE_STAGE,
        component = LOG_COMPONENT_MAIN,
        subsystem = LOG_SUBSYSTEM_STORAGE,
        stage = "ecstore_initialization",
        state = "starting",
        "starting ECStore initialization"
    );
    let store = ECStore::new(server_addr, endpoint_pools.clone(), ctx.clone())
        .await
        .inspect_err(|err| {
            error!(
                target: "rustfs::main::run",
                event = EVENT_STARTUP_STORAGE_STAGE,
                component = LOG_COMPONENT_MAIN,
                subsystem = LOG_SUBSYSTEM_STORAGE,
                stage = "ecstore_initialization",
                state = "failed",
                error = ?err,
                "ECStore initialization failed"
            );
        })?;

    ecconfig::init();
    ecconfig::try_migrate_server_config(store.clone()).await;

    // // Initialize global configuration system
    let mut retry_count = 0;
    while let Err(e) = ecconfig::init_global_config_sys(store.clone()).await {
        error!(
            target: "rustfs::main::run",
            event = EVENT_STARTUP_STORAGE_STAGE,
            component = LOG_COMPONENT_MAIN,
            subsystem = LOG_SUBSYSTEM_STORAGE,
            stage = "global_config_initialization",
            state = "retrying",
            retry_count = retry_count + 1,
            error = ?e,
            "Global config initialization retry failed"
        );
        // TODO: check error type
        retry_count += 1;
        if retry_count > 15 {
            return Err(Error::other("ecconfig::init_global_config_sys failed"));
        }
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    }
    readiness.mark_stage(SystemStage::StorageReady);
    // init replication_pool
    init_background_replication(store.clone()).await;
    // Initialize KMS system if enabled
    init_kms_system(&config).await?;

    let protocol_shutdowns = init_protocol_shutdown_senders().await?;

    // Initialize buffer profiling system
    init_buffer_profile_system(&config);

    match rustfs::startup_services::init_event_notifier_and_audit().await {
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

    // Initialize deadlock detector if enabled
    let detector = rustfs::storage::deadlock_detector::get_deadlock_detector();
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

    let buckets_list = store
        .list_bucket(&BucketOptions {
            no_metadata: true,
            ..Default::default()
        })
        .await
        .map_err(Error::other)?;

    // Collect bucket names into a vector
    let buckets: Vec<String> = buckets_list.into_iter().map(|v| v.name).collect();

    try_migrate_bucket_metadata(store.clone()).await;

    if let Some(pool) = get_global_replication_pool() {
        pool.init_resync(ctx.clone(), buckets.clone()).await?;
    }

    try_migrate_iam_config(store.clone()).await;
    init_bucket_metadata_sys(store.clone(), buckets.clone()).await;

    // 3. Initialize IAM System (Blocking load)
    // This ensures data is in memory before moving forward
    let kms_interface = rustfs_kms::get_global_kms_service_manager().unwrap_or_else(rustfs_kms::init_global_kms_service_manager);
    let iam_bootstrap = bootstrap_or_defer_iam_init(
        store.clone(),
        kms_interface,
        readiness.clone(),
        Some(state_manager.clone()),
        Some(ctx.clone()),
    )
    .await?;

    // 3a. Initialize Keystone authentication if enabled
    let keystone_config = rustfs_keystone::KeystoneConfig::from_env().map_err(Error::other)?;
    if keystone_config.enable {
        match rustfs::auth_keystone::init_keystone_auth(keystone_config).await {
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
                // Continue without Keystone - fall back to standard auth
            }
        }
    }

    // 3b. Initialize OIDC System (non-fatal if no providers configured)
    if let Err(e) = init_oidc_sys().await {
        warn!(
            event = EVENT_OIDC_INITIALIZATION_FAILED,
            component = LOG_COMPONENT_MAIN,
            subsystem = LOG_SUBSYSTEM_AUTH,
            error = %e,
            "OIDC initialization failed; continuing without OIDC providers"
        );
    }

    add_bucket_notification_configuration(buckets.clone()).await;

    rustfs::startup_services::init_notification_system(endpoint_pools.clone())
        .await
        .map_err(|err| {
            error!(
                event = EVENT_NOTIFICATION_SYSTEM_INITIALIZATION_FAILED,
                component = LOG_COMPONENT_MAIN,
                subsystem = LOG_SUBSYSTEM_STARTUP,
                error = ?err,
                "Failed to initialize notification system"
            );
            Error::other(err)
        })?;

    // Create a cancellation token for AHM services
    let _ = create_ahm_services_cancel_token();

    // Check environment variables to determine if scanner and heal should be enabled
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

    // Scanner depends on the heal channel/manager, so scanner implies heal.
    if enable_heal || enable_scanner {
        let heal_storage = Arc::new(ECStoreHealStorage::new(store.clone()));
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

    // print server info
    print_server_info();

    init_update_check();
    rustfs::allocator_reclaim::init_allocator_reclaim(ctx.clone());

    if rustfs_obs::observability_metric_enabled() {
        // Initialize metrics system
        init_metrics_runtime(ctx.clone());
        rustfs::memory_observability::init_memory_observability(ctx.clone());

        // Initialize auto-tuner for performance optimization (optional)
        rustfs::init::init_auto_tuner(ctx.clone()).await;
    }

    info!(
        target: "rustfs::main::run",
        event = EVENT_SERVER_READY,
        component = LOG_COMPONENT_MAIN,
        subsystem = LOG_SUBSYSTEM_STARTUP,
        version = %rustfs::version::get_version(),
        server_address = %server_address,
        started_at = %jiff::Zoned::now(),
        iam_bootstrap = ?iam_bootstrap,
        "RustFS server ready"
    );
    publish_ready_for_iam_bootstrap(iam_bootstrap, readiness.as_ref(), Some(state_manager.as_ref())).await?;
    // Set the global RustFS initialization time to now
    rustfs_common::set_global_init_time_now().await;

    if enable_scanner {
        init_data_scanner(ctx.clone(), store.clone()).await;
    }

    // listen to the shutdown signal
    let shutdown_signal = wait_for_shutdown().await;
    handle_shutdown(
        &state_manager,
        shutdown_signal,
        s3_shutdown_tx,
        console_shutdown_tx,
        protocol_shutdowns,
        ctx.clone(),
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

/// Handles the shutdown process of the server
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
    // update the status to stopping first
    state_manager.update(ServiceState::Stopping);

    // Check environment variables to determine what services need to be stopped
    let enable_scanner = get_env_bool_with_aliases(ENV_SCANNER_ENABLED, &[ENV_SCANNER_ENABLED_DEPRECATED], true);
    let enable_heal = get_env_bool_with_aliases(ENV_HEAL_ENABLED, &[ENV_HEAL_ENABLED_DEPRECATED], true);

    // Stop background services based on what was enabled.
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

    // Shutdown FTP and FTPS servers
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

    // Shutdown WebDAV server
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

    // Shutdown SFTP server
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

    // Stop the notification system
    info!(
        target: "rustfs::main::handle_shutdown",
        event = EVENT_EVENT_NOTIFIER_SHUTDOWN,
        component = LOG_COMPONENT_MAIN,
        subsystem = LOG_SUBSYSTEM_STARTUP,
        state = "stopping",
        "Event notifier shutdown started"
    );
    shutdown_event_notifier().await;

    // Stop the audit system
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

    // Stop profiling tasks
    info!(
        target: "rustfs::main::handle_shutdown",
        event = EVENT_PROFILING_SHUTDOWN,
        component = LOG_COMPONENT_MAIN,
        subsystem = LOG_SUBSYSTEM_STARTUP,
        state = "stopping",
        "Profiling shutdown started"
    );
    rustfs::profiling::shutdown_profiling();

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

    // the last updated status is stopped
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fatal_stderr_message_uses_consistent_prefix_and_context() {
        assert_eq!(
            format_fatal_stderr_message("Observability initialization failed", "collector unavailable"),
            "[FATAL] Observability initialization failed: collector unavailable"
        );
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
