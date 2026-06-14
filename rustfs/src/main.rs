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

#[cfg(feature = "ftps")]
use rustfs::init::{init_ftp_system, init_ftps_system};

#[cfg(feature = "webdav")]
use rustfs::init::init_webdav_system;

#[cfg(feature = "sftp")]
use rustfs::init::init_sftp_system;

use futures_util::future::join_all;
use rustfs::capacity::capacity_integration::init_capacity_management;
use rustfs::license::{init_license, license_status};
use rustfs::server::{
    ServiceState, ServiceStateManager, ShutdownHandle, ShutdownSignal, init_event_notifier, shutdown_event_notifier,
    start_audit_system, start_http_server, stop_audit_system, wait_for_shutdown,
};
use rustfs::startup_fs_guard::enforce_unsupported_fs_policy;
use rustfs::startup_iam::{IamBootstrapDisposition, bootstrap_or_defer_iam_init};
use rustfs_common::{GlobalReadiness, SystemStage, set_global_addr};
use rustfs_credentials::init_global_action_credentials;
use rustfs_ecstore::store::init_lock_clients;
use rustfs_ecstore::{
    bucket::metadata_sys::init_bucket_metadata_sys,
    bucket::migration::{try_migrate_bucket_metadata, try_migrate_iam_config},
    bucket::replication::{get_global_replication_pool, init_background_replication},
    config as ecconfig,
    endpoints::EndpointServerPools,
    global::{set_global_rustfs_port, shutdown_background_services},
    notification_sys::new_global_notification_sys,
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
use rustfs_obs::{init_metrics_runtime, init_obs, set_global_guard};
use rustfs_scanner::init_data_scanner;
use rustfs_storage_api::BucketOptions;
use rustfs_utils::{
    ExternalEnvCompatReport, apply_external_env_compat, get_env_bool_with_aliases, net::parse_and_resolve_address,
};
use rustls::crypto::aws_lc_rs::default_provider;
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
const LOG_SUBSYSTEM_LICENSE: &str = "license";
const LOG_SUBSYSTEM_AUTH: &str = "auth";
const LOG_SUBSYSTEM_STORAGE: &str = "storage";
const EVENT_EXTERNAL_ENV_COMPAT_CONFLICT: &str = "external_env_compat_conflict";
const EVENT_EXTERNAL_ENV_COMPAT_APPLIED: &str = "external_env_compat_applied";
const EVENT_DIAL9_RUNTIME_STATUS: &str = "dial9_runtime_status";
const EVENT_RUNTIME_LICENSE_STATUS: &str = "runtime_license_status";
const EVENT_OBSERVABILITY_GUARD_SET_FAILED: &str = "observability_guard_set_failed";
const EVENT_TLS_OUTBOUND_INITIALIZED: &str = "tls_outbound_initialized";
const EVENT_TLS_OUTBOUND_INITIALIZATION_FAILED: &str = "tls_outbound_initialization_failed";
const EVENT_DEFAULT_CREDENTIALS_DETECTED: &str = "default_credentials_detected";
const EVENT_SERVER_CONFIG_SANITIZED: &str = "server_config_sanitized";
const EVENT_SERVER_STARTING: &str = "server_starting";
const EVENT_SERVER_RUNTIME_FAILED: &str = "server_runtime_failed";
const EVENT_ACTION_CREDENTIALS_INITIALIZED: &str = "action_credentials_initialized";
const EVENT_ACTION_CREDENTIALS_INITIALIZATION_FAILED: &str = "action_credentials_initialization_failed";
const EVENT_OBSERVABILITY_GUARD_SET: &str = "observability_guard_set";
const EVENT_CRYPTO_PROVIDER_STATE: &str = "crypto_provider_state";
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

fn bootstrap_external_prefix_compat() -> Result<ExternalEnvCompatReport> {
    let env_compat_report = apply_external_env_compat();
    Ok(env_compat_report)
}

fn format_external_prefix_mappings(report: &ExternalEnvCompatReport) -> String {
    report
        .mapped_pairs
        .iter()
        .map(|(source_key, rustfs_key)| format!("{source_key}->{rustfs_key}"))
        .collect::<Vec<_>>()
        .join(", ")
}

fn is_using_default_credentials(config: &rustfs::config::Config) -> bool {
    config.is_using_default_credentials()
}

const DEFAULT_CREDENTIALS_WARNING_MESSAGE: &str = "Detected default root credentials; set RUSTFS_ACCESS_KEY and RUSTFS_SECRET_KEY to non-default values for production deployments";

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

    // Initialize the global config snapshot for info command
    rustfs::config::init_config_snapshot(&config);

    // Initialize the configuration
    init_license(config.license.clone());

    // Initialize Observability
    let guard = match init_obs(Some(config.clone().obs_endpoint)).await {
        Ok(g) => g,
        Err(e) => {
            // Structured logging is unavailable until observability initializes.
            emit_fatal_stderr("Observability initialization failed", &e);
            return Err(Error::other(OBSERVABILITY_INIT_FATAL_ALREADY_REPORTED));
        }
    };

    // Store in global storage
    match set_global_guard(guard).map_err(Error::other) {
        Ok(_) => {
            debug!(
                target: "rustfs::main",
                event = EVENT_OBSERVABILITY_GUARD_SET,
                component = LOG_COMPONENT_MAIN,
                subsystem = LOG_SUBSYSTEM_STARTUP,
                result = "ok",
                "Stored global observability guard"
            );
        }
        Err(e) => {
            error!(
                target: "rustfs::main",
                event = EVENT_OBSERVABILITY_GUARD_SET_FAILED,
                component = LOG_COMPONENT_MAIN,
                subsystem = LOG_SUBSYSTEM_STARTUP,
                error = %e,
                "Failed to store global observability guard"
            );
            return Err(e);
        }
    }

    log_external_prefix_compat_report(&env_compat_report);

    // Check dial9 Tokio runtime telemetry status
    // Note: The actual telemetry session is created in build_tokio_runtime()
    // which stores the TelemetryGuard globally for the program duration.
    if rustfs_obs::dial9::is_enabled() {
        info!(
            target: "rustfs::main",
            event = EVENT_DIAL9_RUNTIME_STATUS,
            component = LOG_COMPONENT_MAIN,
            subsystem = LOG_SUBSYSTEM_STARTUP,
            enabled = true,
            "Dial9 Tokio runtime telemetry is enabled"
        );
    } else {
        debug!(
            target: "rustfs::main",
            event = EVENT_DIAL9_RUNTIME_STATUS,
            component = LOG_COMPONENT_MAIN,
            subsystem = LOG_SUBSYSTEM_STARTUP,
            enabled = false,
            "Dial9 Tokio runtime telemetry is disabled"
        );
    }

    info!(
        target: "rustfs::main",
        event = EVENT_RUNTIME_LICENSE_STATUS,
        component = LOG_COMPONENT_MAIN,
        subsystem = LOG_SUBSYSTEM_LICENSE,
        license_status = %license_status(),
        "Initialized runtime license state"
    );

    // print startup logo
    debug!("{}", rustfs::server::LOGO);

    // Initialize performance profiling if enabled
    rustfs::profiling::init_from_env().await;

    // Initialize trusted proxies system
    rustfs_trusted_proxies::init();

    // Make sure to use a modern encryption suite
    if default_provider().install_default().is_err() {
        // A crypto provider is already installed (e.g. by the host process); this is fine.
        debug!(
            target: "rustfs::main",
            event = EVENT_CRYPTO_PROVIDER_STATE,
            component = LOG_COMPONENT_MAIN,
            subsystem = LOG_SUBSYSTEM_STARTUP,
            provider = "aws_lc_rs",
            state = "already_installed",
            "Rustls crypto provider state checked"
        );
    }
    // Initialize TLS outbound material (root CAs, mTLS identity) if configured.
    // Server-side TLS acceptor is built separately inside start_http_server().
    // Single load via tls-runtime; outbound is enriched with platform CAs and
    // published to the global state before any HTTP listener starts.
    if let Some(tls_path) = config.tls_path.as_deref().map(str::trim).filter(|path| !path.is_empty()) {
        match rustfs::server::tls_material::load_tls_material(tls_path).await {
            Ok(snapshot) => {
                use rustfs_tls_runtime::{TlsGeneration, publish_global_outbound_tls_state, record_tls_generation};
                let generation = TlsGeneration(rustfs_common::get_global_outbound_tls_generation().saturating_add(1));
                publish_global_outbound_tls_state(generation, &snapshot.outbound).await;
                record_tls_generation("rustfs_server_startup", generation.0);
                info!(
                    target: "rustfs::main",
                    event = EVENT_TLS_OUTBOUND_INITIALIZED,
                    component = LOG_COMPONENT_MAIN,
                    subsystem = LOG_SUBSYSTEM_STARTUP,
                    tls_path,
                    generation = generation.0,
                    "Initialized TLS outbound material"
                );
            }
            Err(e) => {
                error!(
                    target: "rustfs::main",
                    event = EVENT_TLS_OUTBOUND_INITIALIZATION_FAILED,
                    component = LOG_COMPONENT_MAIN,
                    subsystem = LOG_SUBSYSTEM_STARTUP,
                    tls_path,
                    error = %e,
                    "Failed to initialize TLS outbound material"
                );
                return Err(Error::other(e.to_string()));
            }
        }
        if rustfs_obs::observability_metric_enabled() {
            rustfs_tls_runtime::init_tls_metrics();
        }
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
    debug!(
        target: "rustfs::main::run",
        event = EVENT_SERVER_CONFIG_SANITIZED,
        component = LOG_COMPONENT_MAIN,
        subsystem = LOG_SUBSYSTEM_STARTUP,
        address = %config.address,
        volume_count = config.volumes.len(),
        server_domain_count = config.server_domains.len(),
        console_enable = config.console_enable,
        console_address = %config.console_address,
        tls_enabled = config.tls_path.as_deref().is_some_and(|value| !value.trim().is_empty()),
        kms_enable = config.kms_enable,
        kms_backend = %config.kms_backend,
        region = config.region.as_deref().unwrap_or_default(),
        buffer_profile = %config.buffer_profile,
        "Loaded sanitized server configuration"
    );
    // 1. Initialize global readiness tracker
    let readiness = Arc::new(GlobalReadiness::new());

    if let Some(region_str) = &config.region {
        region_str
            .parse::<s3s::region::Region>()
            .map(rustfs_ecstore::global::set_global_region)
            .map_err(|e| Error::other(format!("invalid region '{}': {}", region_str, e)))?;
    }

    let server_addr = parse_and_resolve_address(config.address.as_str()).map_err(Error::other)?;
    let server_port = server_addr.port();
    let server_address = server_addr.to_string();

    if is_using_default_credentials(&config) {
        warn!(
            target: "rustfs::main::run",
            event = EVENT_DEFAULT_CREDENTIALS_DETECTED,
            component = LOG_COMPONENT_MAIN,
            subsystem = LOG_SUBSYSTEM_AUTH,
            warning = DEFAULT_CREDENTIALS_WARNING_MESSAGE,
            "{DEFAULT_CREDENTIALS_WARNING_MESSAGE}"
        );
    }

    info!(
        target: "rustfs::main::run",
        event = EVENT_SERVER_STARTING,
        component = LOG_COMPONENT_MAIN,
        subsystem = LOG_SUBSYSTEM_STARTUP,
        server_address = %server_address,
        ip = %server_addr.ip(),
        port = %server_port,
        version = %rustfs::version::get_version(),
        "Starting RustFS server"
    );

    // Set up AK and SK
    match init_global_action_credentials(Some(config.access_key.clone()), Some(config.secret_key.clone())) {
        Ok(_) => {
            debug!(
                target: "rustfs::main::run",
                event = EVENT_ACTION_CREDENTIALS_INITIALIZED,
                component = LOG_COMPONENT_MAIN,
                subsystem = LOG_SUBSYSTEM_AUTH,
                result = "ok",
                "Initialized global action credentials"
            );
        }
        Err(e) => {
            let msg = format!("init global action credentials failed: {e:?}");
            error!(
                target: "rustfs::main::run",
                event = EVENT_ACTION_CREDENTIALS_INITIALIZATION_FAILED,
                component = LOG_COMPONENT_MAIN,
                subsystem = LOG_SUBSYSTEM_AUTH,
                error = %e,
                "Failed to initialize global action credentials"
            );
            return Err(Error::other(msg));
        }
    };

    set_global_rustfs_port(server_port);

    set_global_addr(&config.address).await;

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
    // Initialize capacity management system
    init_capacity_management().await;
    let state_manager = Arc::new(ServiceStateManager::new());
    // Update service status to Starting
    state_manager.update(ServiceState::Starting);

    let s3_shutdown_tx = {
        let mut s3_config = config.clone();
        s3_config.console_enable = false;
        let (s3_shutdown_tx, _) = start_http_server(&s3_config, readiness.clone()).await?;
        Some(s3_shutdown_tx)
    };

    let console_shutdown_tx = if config.console_enable && !config.console_address.is_empty() {
        let mut console_config = config.clone();
        console_config.address = console_config.console_address.clone();
        let (console_shutdown_tx, _) = start_http_server(&console_config, readiness.clone()).await?;
        Some(console_shutdown_tx)
    } else {
        None
    };

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

    // Initialize FTP system if enabled
    #[cfg(feature = "ftps")]
    let ftp_shutdown_tx = match init_ftp_system().await {
        Ok(Some(tx)) => {
            debug!(
                event = EVENT_PROTOCOL_SYSTEM_STATE,
                component = LOG_COMPONENT_MAIN,
                subsystem = LOG_SUBSYSTEM_STARTUP,
                protocol = "ftp",
                state = "started",
                "Protocol runtime started"
            );
            Some(tx)
        }
        Ok(None) => {
            info!(
                event = EVENT_PROTOCOL_SYSTEM_STATE,
                component = LOG_COMPONENT_MAIN,
                subsystem = LOG_SUBSYSTEM_STARTUP,
                protocol = "ftp",
                state = "disabled",
                "Protocol runtime disabled"
            );
            None
        }
        Err(e) => {
            error!(
                event = EVENT_PROTOCOL_SYSTEM_STATE,
                component = LOG_COMPONENT_MAIN,
                subsystem = LOG_SUBSYSTEM_STARTUP,
                protocol = "ftp",
                state = "initialization_failed",
                error = %e,
                "Protocol runtime initialization failed"
            );
            return Err(Error::other(e));
        }
    };

    #[cfg(not(feature = "ftps"))]
    let ftp_shutdown_tx: Option<ShutdownHandle> = None;

    // Initialize FTPS system if enabled
    #[cfg(feature = "ftps")]
    let ftps_shutdown_tx = match init_ftps_system().await {
        Ok(Some(tx)) => {
            debug!(
                event = EVENT_PROTOCOL_SYSTEM_STATE,
                component = LOG_COMPONENT_MAIN,
                subsystem = LOG_SUBSYSTEM_STARTUP,
                protocol = "ftps",
                state = "started",
                "Protocol runtime started"
            );
            Some(tx)
        }
        Ok(None) => {
            info!(
                event = EVENT_PROTOCOL_SYSTEM_STATE,
                component = LOG_COMPONENT_MAIN,
                subsystem = LOG_SUBSYSTEM_STARTUP,
                protocol = "ftps",
                state = "disabled",
                "Protocol runtime disabled"
            );
            None
        }
        Err(e) => {
            error!(
                event = EVENT_PROTOCOL_SYSTEM_STATE,
                component = LOG_COMPONENT_MAIN,
                subsystem = LOG_SUBSYSTEM_STARTUP,
                protocol = "ftps",
                state = "initialization_failed",
                error = %e,
                "Protocol runtime initialization failed"
            );
            return Err(Error::other(e));
        }
    };

    #[cfg(not(feature = "ftps"))]
    let ftps_shutdown_tx: Option<ShutdownHandle> = None;

    // Initialize WebDAV system if enabled
    #[cfg(feature = "webdav")]
    let webdav_shutdown_tx = match init_webdav_system().await {
        Ok(Some(tx)) => {
            debug!(
                event = EVENT_PROTOCOL_SYSTEM_STATE,
                component = LOG_COMPONENT_MAIN,
                subsystem = LOG_SUBSYSTEM_STARTUP,
                protocol = "webdav",
                state = "started",
                "Protocol runtime started"
            );
            Some(tx)
        }
        Ok(None) => {
            info!(
                event = EVENT_PROTOCOL_SYSTEM_STATE,
                component = LOG_COMPONENT_MAIN,
                subsystem = LOG_SUBSYSTEM_STARTUP,
                protocol = "webdav",
                state = "disabled",
                "Protocol runtime disabled"
            );
            None
        }
        Err(e) => {
            error!(
                event = EVENT_PROTOCOL_SYSTEM_STATE,
                component = LOG_COMPONENT_MAIN,
                subsystem = LOG_SUBSYSTEM_STARTUP,
                protocol = "webdav",
                state = "initialization_failed",
                error = %e,
                "Protocol runtime initialization failed"
            );
            return Err(Error::other(e));
        }
    };

    #[cfg(not(feature = "webdav"))]
    let webdav_shutdown_tx: Option<ShutdownHandle> = None;

    // Initialize SFTP system if enabled
    #[cfg(feature = "sftp")]
    let sftp_shutdown_tx = match init_sftp_system().await {
        Ok(Some(tx)) => {
            debug!(
                event = EVENT_PROTOCOL_SYSTEM_STATE,
                component = LOG_COMPONENT_MAIN,
                subsystem = LOG_SUBSYSTEM_STARTUP,
                protocol = "sftp",
                state = "started",
                "Protocol runtime started"
            );
            Some(tx)
        }
        Ok(None) => {
            info!(
                event = EVENT_PROTOCOL_SYSTEM_STATE,
                component = LOG_COMPONENT_MAIN,
                subsystem = LOG_SUBSYSTEM_STARTUP,
                protocol = "sftp",
                state = "disabled",
                "Protocol runtime disabled"
            );
            None
        }
        Err(e) => {
            error!(
                event = EVENT_PROTOCOL_SYSTEM_STATE,
                component = LOG_COMPONENT_MAIN,
                subsystem = LOG_SUBSYSTEM_STARTUP,
                protocol = "sftp",
                state = "initialization_failed",
                error = %e,
                "Protocol runtime initialization failed"
            );
            return Err(Error::other(e));
        }
    };

    #[cfg(not(feature = "sftp"))]
    let sftp_shutdown_tx: Option<ShutdownHandle> = None;

    // Initialize buffer profiling system
    init_buffer_profile_system(&config);

    // Initialize event notifier
    init_event_notifier().await;

    // Start the audit system
    match start_audit_system().await {
        Ok(_) => info!(
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

    // Initialize the global notification system
    new_global_notification_sys(endpoint_pools.clone()).await.map_err(|err| {
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
    if iam_bootstrap == IamBootstrapDisposition::ReadyInline {
        rustfs::server::publish_ready_when_runtime_ready(readiness.as_ref(), Some(state_manager.as_ref())).await?;
    }
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
        ProtocolShutdownSenders {
            ftp: ftp_shutdown_tx,
            ftps: ftps_shutdown_tx,
            webdav: webdav_shutdown_tx,
            sftp: sftp_shutdown_tx,
        },
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

fn log_external_prefix_compat_report(report: &ExternalEnvCompatReport) {
    if report.conflict_count() > 0 {
        warn!(
            target: "rustfs::main",
            event = EVENT_EXTERNAL_ENV_COMPAT_CONFLICT,
            component = LOG_COMPONENT_MAIN,
            subsystem = LOG_SUBSYSTEM_STARTUP,
            conflict_count = report.conflict_count(),
            conflict_keys = %report.conflict_keys.join(", "),
            "Detected external-prefix compatibility conflicts; keeping RUSTFS_ values"
        );
    }

    if report.mapped_count() > 0 {
        info!(
            target: "rustfs::main",
            event = EVENT_EXTERNAL_ENV_COMPAT_APPLIED,
            component = LOG_COMPONENT_MAIN,
            subsystem = LOG_SUBSYSTEM_STARTUP,
            mapped_count = report.mapped_count(),
            mapped_pairs = %format_external_prefix_mappings(report),
            "Applied external-prefix compatibility mappings"
        );
    }
}

/// Shutdown channels for every protocol server. None means the protocol was
/// disabled at startup.
struct ProtocolShutdownSenders {
    ftp: Option<ShutdownHandle>,
    ftps: Option<ShutdownHandle>,
    webdav: Option<ShutdownHandle>,
    sftp: Option<ShutdownHandle>,
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
    fn format_external_prefix_mappings_lists_mapped_pairs() {
        let report = ExternalEnvCompatReport {
            mapped_pairs: vec![
                ("MINIO_ROOT_USER".to_string(), "RUSTFS_ROOT_USER".to_string()),
                (
                    "MINIO_NOTIFY_WEBHOOK_ENABLE_PRIMARY".to_string(),
                    "RUSTFS_NOTIFY_WEBHOOK_ENABLE_PRIMARY".to_string(),
                ),
            ],
            conflict_keys: Vec::new(),
        };

        let formatted = format_external_prefix_mappings(&report);

        assert_eq!(
            formatted,
            "MINIO_ROOT_USER->RUSTFS_ROOT_USER, MINIO_NOTIFY_WEBHOOK_ENABLE_PRIMARY->RUSTFS_NOTIFY_WEBHOOK_ENABLE_PRIMARY"
        );
    }

    #[test]
    fn fatal_stderr_message_uses_consistent_prefix_and_context() {
        assert_eq!(
            format_fatal_stderr_message("Observability initialization failed", "collector unavailable"),
            "[FATAL] Observability initialization failed: collector unavailable"
        );
    }

    #[test]
    fn is_using_default_credentials_returns_true_for_default_keys() {
        let mut config = rustfs::config::Config::new("127.0.0.1:9000", Vec::new());
        config.console_enable = true;
        config.console_address = "127.0.0.1:9001".to_string();

        assert!(is_using_default_credentials(&config));
    }

    #[test]
    fn is_using_default_credentials_returns_false_for_custom_keys() {
        let mut config = rustfs::config::Config::new("127.0.0.1:9000", Vec::new());
        config.access_key = "custom-access-key".to_string();
        config.secret_key = "custom-secret-key".to_string();

        assert!(!is_using_default_credentials(&config));
    }

    #[test]
    fn default_credentials_messages_are_actionable_without_exposing_values() {
        assert!(DEFAULT_CREDENTIALS_WARNING_MESSAGE.contains(rustfs_config::ENV_RUSTFS_ACCESS_KEY));
        assert!(DEFAULT_CREDENTIALS_WARNING_MESSAGE.contains(rustfs_config::ENV_RUSTFS_SECRET_KEY));
        assert!(!DEFAULT_CREDENTIALS_WARNING_MESSAGE.contains(rustfs_credentials::DEFAULT_ACCESS_KEY));
        assert!(!DEFAULT_CREDENTIALS_WARNING_MESSAGE.contains(rustfs_credentials::DEFAULT_SECRET_KEY));
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
