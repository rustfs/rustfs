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
use rustfs::app::context::{AppContext, init_global_app_context};
use rustfs::init::{
    add_bucket_notification_configuration, init_buffer_profile_system, init_kms_system, init_update_check, print_server_info,
};

#[cfg(feature = "ftps")]
use rustfs::init::{init_ftp_system, init_ftps_system};

#[cfg(feature = "webdav")]
use rustfs::init::init_webdav_system;

#[cfg(feature = "sftp")]
use rustfs::init::init_sftp_system;

use rustfs::capacity::capacity_integration::init_capacity_management;
use rustfs::license::{current_license, init_license, license_status};
use rustfs::server::{
    SHUTDOWN_TIMEOUT, ServiceState, ServiceStateManager, ShutdownSignal, init_event_notifier, shutdown_event_notifier,
    start_audit_system, start_http_server, stop_audit_system, wait_for_shutdown,
};
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
    store_api::BucketOptions,
    update_erasure_type,
};
use rustfs_heal::{
    create_ahm_services_cancel_token, heal::storage::ECStoreHealStorage, init_heal_manager, shutdown_ahm_services,
};
use rustfs_iam::{init_iam_sys, init_oidc_sys};
use rustfs_obs::{init_metrics_runtime, init_obs, set_global_guard};
use rustfs_scanner::init_data_scanner;
use rustfs_utils::{
    ExternalEnvCompatReport, apply_external_env_compat, get_env_bool, get_env_bool_with_aliases, net::parse_and_resolve_address,
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

#[cfg(all(target_os = "linux", target_env = "gnu", target_arch = "x86_64"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[cfg(not(all(target_os = "linux", target_env = "gnu", target_arch = "x86_64")))]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn main() {
    if let Err(err) = bootstrap_external_prefix_compat() {
        eprintln!("[WARN] Failed to bootstrap external-prefix compatibility: {err}");
    }

    // Build Tokio runtime with optional dial9 telemetry support
    let runtime = rustfs::server::build_tokio_runtime().expect("Failed to build Tokio runtime");
    let result = runtime.block_on(async_main());
    if let Err(ref e) = result {
        // Use eprintln as tracing may not be initialized at this point
        eprintln!("[FATAL] Server encountered an error and is shutting down: {e}");
        std::process::exit(1);
    }
}

fn bootstrap_external_prefix_compat() -> Result<()> {
    let env_compat_report = apply_external_env_compat();
    if env_compat_report.conflict_count() > 0 {
        // RUSTFS_* is the canonical namespace in this codebase, so on key conflicts we keep RUSTFS_*
        // to preserve explicit user/operator overrides and avoid changing existing runtime behavior.
        eprintln!(
            "[WARN] Found {} source/RUSTFS_ conflict(s), keeping RUSTFS_ values: {}",
            env_compat_report.conflict_count(),
            env_compat_report.conflict_keys.join(", ")
        );
    }

    if env_compat_report.mapped_count() == 0 {
        return Ok(());
    }

    eprintln!(
        "[INFO] Applying external-prefix compatibility in-process for {} variable(s): {}",
        env_compat_report.mapped_count(),
        format_external_prefix_mappings(&env_compat_report)
    );

    Ok(())
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

const DEFAULT_CREDENTIALS_WARNING_MESSAGE: &str = "Detected default root credentials; set RUSTFS_ACCESS_KEY and RUSTFS_SECRET_KEY to non-default values, or use RUSTFS_ALLOW_INSECURE_DEFAULT_CREDENTIALS=true only for local development";
const DEFAULT_CREDENTIALS_ERROR_MESSAGE: &str = "Default root credentials are not allowed on non-loopback listeners; set RUSTFS_ACCESS_KEY and RUSTFS_SECRET_KEY to non-default values, bind to loopback, or set RUSTFS_ALLOW_INSECURE_DEFAULT_CREDENTIALS=true for local development only";

fn allow_insecure_default_credentials() -> bool {
    get_env_bool(rustfs_config::ENV_RUSTFS_ALLOW_INSECURE_DEFAULT_CREDENTIALS, false)
}

async fn async_main() -> Result<()> {
    // Parse command line arguments
    let args: Vec<String> = std::env::args().collect();
    let command_result = match rustfs::config::Opt::parse_command(args) {
        Ok(result) => result,
        Err(e) => {
            eprintln!("Command parse failed, error: {}", e);
            std::process::exit(1);
        }
    };

    // Handle info command
    if let rustfs::config::CommandResult::Info(opts) = command_result {
        rustfs::config::execute_info(&opts);
        return Ok(());
    }

    // Get config for server command
    let config = match command_result {
        rustfs::config::CommandResult::Server(cfg) => cfg,
        rustfs::config::CommandResult::Info(_) => unreachable!(),
    };

    // Initialize the global config snapshot for info command
    rustfs::config::init_config_snapshot(&config);

    // Initialize the configuration
    init_license(config.license.clone());

    // Initialize Observability
    let guard = match init_obs(Some(config.clone().obs_endpoint)).await {
        Ok(g) => g,
        Err(e) => {
            // Use eprintln as tracing is not yet initialized
            eprintln!("[FATAL] Failed to initialize observability: {e}");
            return Err(Error::other(e));
        }
    };

    // Store in global storage
    match set_global_guard(guard).map_err(Error::other) {
        Ok(_) => {
            info!(target: "rustfs::main", "Global observability guard set successfully.");
        }
        Err(e) => {
            error!("Failed to set global observability guard: {}", e);
            return Err(e);
        }
    }

    // Check dial9 Tokio runtime telemetry status
    // Note: The actual telemetry session is created in build_tokio_runtime()
    // which stores the TelemetryGuard globally for the program duration.
    if rustfs_obs::dial9::is_enabled() {
        info!(target: "rustfs::main", "Dial9 Tokio telemetry is configured as enabled; runtime guard was installed during startup.");
    } else {
        info!(target: "rustfs::main", "Dial9 Tokio telemetry is not configured (set RUSTFS_RUNTIME_DIAL9_ENABLED=true to enable).");
    }

    info!("license status: {}", license_status());
    if let Some(token) = current_license() {
        info!("runtime license loaded: {}", token.name);
    }

    // print startup logo
    info!("{}", rustfs::server::LOGO);

    // Initialize performance profiling if enabled
    rustfs::profiling::init_from_env().await;

    // Initialize trusted proxies system
    rustfs_trusted_proxies::init();

    // Make sure to use a modern encryption suite
    if default_provider().install_default().is_err() {
        // A crypto provider is already installed (e.g. by the host process); this is fine.
        debug!("rustls crypto provider already installed, skipping aws-lc-rs default install");
    }
    // Initialize TLS outbound material (root CAs, mTLS identity) if configured.
    // Server-side TLS acceptor is built separately inside start_http_server()
    // using the same TlsMaterialSnapshot loading logic.
    if let Some(tls_path) = config.tls_path.as_deref().map(str::trim).filter(|path| !path.is_empty()) {
        match rustfs::server::tls_material::TlsMaterialSnapshot::load(tls_path).await {
            Ok(snapshot) => {
                snapshot.apply_outbound().await;
                info!(target: "rustfs::main", "TLS outbound material initialized from {}", tls_path);
            }
            Err(e) => {
                error!("Failed to initialize TLS from {}: {}", tls_path, e);
                return Err(Error::other(e.to_string()));
            }
        }
    }

    // Run parameters
    match run(*config).await {
        Ok(_) => Ok(()),
        Err(e) => {
            error!("Server encountered an error and is shutting down: {}", e);
            Err(e)
        }
    }
}

#[instrument(skip(config))]
async fn run(config: rustfs::config::Config) -> Result<()> {
    debug!("config: {:?}", &config);
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

    if !config.default_credentials_allowed_for_addr(server_addr, allow_insecure_default_credentials()) {
        error!("{DEFAULT_CREDENTIALS_ERROR_MESSAGE}");
        return Err(Error::other(DEFAULT_CREDENTIALS_ERROR_MESSAGE));
    }

    if is_using_default_credentials(&config) {
        warn!("{}", DEFAULT_CREDENTIALS_WARNING_MESSAGE);
    }

    info!(
        target: "rustfs::main::run",
        server_address = %server_address,
        ip = %server_addr.ip(),
        port = %server_port,
        version = %rustfs::version::get_version(),
        "Starting RustFS server at {}",
        &server_address
    );

    // Set up AK and SK
    match init_global_action_credentials(Some(config.access_key.clone()), Some(config.secret_key.clone())) {
        Ok(_) => {
            info!(target: "rustfs::main::run", "Global action credentials initialized successfully.");
        }
        Err(e) => {
            let msg = format!("init global action credentials failed: {e:?}");
            error!(target: "rustfs::main::run","{msg}");
            return Err(Error::other(msg));
        }
    };

    set_global_rustfs_port(server_port);

    set_global_addr(&config.address).await;

    // For RPC
    let (endpoint_pools, setup_type) = EndpointServerPools::from_volumes(server_address.clone().as_str(), config.volumes.clone())
        .await
        .map_err(Error::other)?;

    set_global_endpoints(endpoint_pools.as_ref().clone());
    update_erasure_type(setup_type).await;

    // Initialize the local disk
    init_local_disks(endpoint_pools.clone()).await.map_err(Error::other)?;
    prewarm_local_disk_id_map().await;
    // Initialize the lock clients

    init_lock_clients(endpoint_pools.clone());

    for (i, eps) in endpoint_pools.as_ref().iter().enumerate() {
        info!(
            target: "rustfs::main::run",
            "Formatting {}st pool, {} set(s), {} drives per set.",
            i + 1,
            eps.set_count,
            eps.drives_per_set
        );

        if eps.drives_per_set > 1 {
            warn!(target: "rustfs::main::run","WARNING: Host local has more than 0 drives of set. A host failure will result in data becoming unavailable.");
        }
    }

    for (i, eps) in endpoint_pools.as_ref().iter().enumerate() {
        info!(
            target: "rustfs::main::run",
            id = i,
            set_count = eps.set_count,
            drives_per_set = eps.drives_per_set,
            cmd = ?eps.cmd_line,
            "created endpoints {}, set_count:{}, drives_per_set: {}, cmd: {:?}",
            i, eps.set_count, eps.drives_per_set, eps.cmd_line
        );

        for ep in eps.endpoints.as_ref().iter() {
            info!(
                target: "rustfs::main::run",
                "  - endpoint: {}", ep
            );
        }
    }
    // Initialize capacity management system
    init_capacity_management().await;
    let state_manager = ServiceStateManager::new();
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
    let store = ECStore::new(server_addr, endpoint_pools.clone(), ctx.clone())
        .await
        .inspect_err(|err| {
            error!("ECStore::new {:?}", err);
        })?;

    ecconfig::init();
    ecconfig::try_migrate_server_config(store.clone()).await;

    // // Initialize global configuration system
    let mut retry_count = 0;
    while let Err(e) = ecconfig::init_global_config_sys(store.clone()).await {
        error!("ecstore config::init_global_config_sys failed {:?}", e);
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
            info!("FTP system initialized successfully");
            Some(tx)
        }
        Ok(None) => {
            info!("FTP system disabled");
            None
        }
        Err(e) => {
            error!("Failed to initialize FTP system: {}", e);
            return Err(Error::other(e));
        }
    };

    #[cfg(not(feature = "ftps"))]
    let ftp_shutdown_tx: Option<tokio::sync::broadcast::Sender<()>> = None;

    // Initialize FTPS system if enabled
    #[cfg(feature = "ftps")]
    let ftps_shutdown_tx = match init_ftps_system().await {
        Ok(Some(tx)) => {
            info!("FTPS system initialized successfully");
            Some(tx)
        }
        Ok(None) => {
            info!("FTPS system disabled");
            None
        }
        Err(e) => {
            error!("Failed to initialize FTPS system: {}", e);
            return Err(Error::other(e));
        }
    };

    #[cfg(not(feature = "ftps"))]
    let ftps_shutdown_tx: Option<tokio::sync::broadcast::Sender<()>> = None;

    // Initialize WebDAV system if enabled
    #[cfg(feature = "webdav")]
    let webdav_shutdown_tx = match init_webdav_system().await {
        Ok(Some(tx)) => {
            info!("WebDAV system initialized successfully");
            Some(tx)
        }
        Ok(None) => {
            info!("WebDAV system disabled");
            None
        }
        Err(e) => {
            error!("Failed to initialize WebDAV system: {}", e);
            return Err(Error::other(e));
        }
    };

    #[cfg(not(feature = "webdav"))]
    let webdav_shutdown_tx: Option<tokio::sync::broadcast::Sender<()>> = None;

    // Initialize SFTP system if enabled
    #[cfg(feature = "sftp")]
    let sftp_shutdown_tx = match init_sftp_system().await {
        Ok(Some(tx)) => {
            info!("SFTP system initialized successfully");
            Some(tx)
        }
        Ok(None) => {
            info!("SFTP system disabled");
            None
        }
        Err(e) => {
            error!("Failed to initialize SFTP system: {}", e);
            return Err(Error::other(e));
        }
    };

    #[cfg(not(feature = "sftp"))]
    let sftp_shutdown_tx: Option<tokio::sync::broadcast::Sender<()>> = None;

    // Initialize buffer profiling system
    init_buffer_profile_system(&config);

    // Initialize event notifier
    init_event_notifier().await;

    // Start the audit system
    match start_audit_system().await {
        Ok(_) => info!(target: "rustfs::main::run","Audit system started successfully."),
        Err(e) => error!(target: "rustfs::main::run","Failed to start audit system: {}", e),
    }

    // Initialize deadlock detector if enabled
    let detector = rustfs::storage::deadlock_detector::get_deadlock_detector();
    if detector.is_enabled() {
        detector.start();
        info!(target: "rustfs::main::run","Deadlock detector started successfully.");
    } else {
        info!(target: "rustfs::main::run","Deadlock detector disabled.");
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
    init_iam_sys(store.clone()).await.map_err(Error::other)?;
    readiness.mark_stage(SystemStage::IamReady);

    // 3a. Initialize Keystone authentication if enabled
    let keystone_config = rustfs_keystone::KeystoneConfig::from_env().map_err(Error::other)?;
    if keystone_config.enable {
        match rustfs::auth_keystone::init_keystone_auth(keystone_config).await {
            Ok(_) => info!("Keystone authentication initialized successfully"),
            Err(e) => {
                error!("Failed to initialize Keystone authentication: {}", e);
                // Continue without Keystone - fall back to standard auth
            }
        }
    }

    // 3b. Initialize OIDC System (non-fatal if no providers configured)
    if let Err(e) = init_oidc_sys().await {
        warn!("OIDC initialization failed (non-fatal): {}", e);
    }

    let iam_interface =
        rustfs_iam::get().map_err(|e| Error::other(format!("initialize app context IAM dependency failed: {e}")))?;
    let kms_interface = rustfs_kms::get_global_kms_service_manager().unwrap_or_else(rustfs_kms::init_global_kms_service_manager);
    let _app_context = init_global_app_context(AppContext::with_default_interfaces(store.clone(), iam_interface, kms_interface));

    add_bucket_notification_configuration(buckets.clone()).await;

    // Initialize the global notification system
    new_global_notification_sys(endpoint_pools.clone()).await.map_err(|err| {
        error!("new_global_notification_sys failed {:?}", &err);
        Error::other(err)
    })?;

    // Create a cancellation token for AHM services
    let _ = create_ahm_services_cancel_token();

    // Check environment variables to determine if scanner and heal should be enabled
    let enable_scanner = get_env_bool_with_aliases(ENV_SCANNER_ENABLED, &[ENV_SCANNER_ENABLED_DEPRECATED], true);
    let enable_heal = get_env_bool_with_aliases(ENV_HEAL_ENABLED, &[ENV_HEAL_ENABLED_DEPRECATED], true);

    info!(
        target: "rustfs::main::run",
        enable_scanner = enable_scanner,
        enable_heal = enable_heal,
        "Background services configuration: scanner={}, heal={}", enable_scanner, enable_heal
    );

    // Scanner depends on the heal channel/manager, so scanner implies heal.
    if enable_heal || enable_scanner {
        let heal_storage = Arc::new(ECStoreHealStorage::new(store.clone()));
        init_heal_manager(heal_storage, None).await?;
    }

    if enable_scanner {
        init_data_scanner(ctx.clone(), store.clone()).await;
    }

    if !enable_heal && !enable_scanner {
        info!(target: "rustfs::main::run","Both scanner and heal are disabled, skipping AHM service initialization");
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
        "RustFS server version: {} started successfully at {}, current time: {}",
        rustfs::version::get_version(),
        &server_address,
        jiff::Zoned::now()
    );
    // 4. Mark as Full Ready now that critical components are warm
    readiness.mark_stage(SystemStage::FullReady);

    // Set the global RustFS initialization time to now
    rustfs_common::set_global_init_time_now().await;
    // Publish ready only after all critical bootstrap metadata is in place
    state_manager.update(ServiceState::Ready);

    // Perform hibernation for 1 second
    tokio::time::sleep(SHUTDOWN_TIMEOUT).await;
    // listen to the shutdown signal
    match wait_for_shutdown().await {
        #[cfg(unix)]
        ShutdownSignal::CtrlC | ShutdownSignal::Sigint | ShutdownSignal::Sigterm => {
            handle_shutdown(
                &state_manager,
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
        }
        #[cfg(not(unix))]
        ShutdownSignal::CtrlC => {
            handle_shutdown(
                &state_manager,
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
        }
    }

    info!(target: "rustfs::main::run","server is stopped state: {:?}", state_manager.current_state());
    Ok(())
}

/// Shutdown channels for every protocol server. None means the protocol was
/// disabled at startup.
struct ProtocolShutdownSenders {
    ftp: Option<tokio::sync::broadcast::Sender<()>>,
    ftps: Option<tokio::sync::broadcast::Sender<()>>,
    webdav: Option<tokio::sync::broadcast::Sender<()>>,
    sftp: Option<tokio::sync::broadcast::Sender<()>>,
}

/// Handles the shutdown process of the server
async fn handle_shutdown(
    state_manager: &ServiceStateManager,
    s3_shutdown_tx: Option<tokio::sync::broadcast::Sender<()>>,
    console_shutdown_tx: Option<tokio::sync::broadcast::Sender<()>>,
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
        "Shutdown signal received in main thread"
    );
    // update the status to stopping first
    state_manager.update(ServiceState::Stopping);

    // Check environment variables to determine what services need to be stopped
    let enable_scanner = get_env_bool_with_aliases(ENV_SCANNER_ENABLED, &[ENV_SCANNER_ENABLED_DEPRECATED], true);
    let enable_heal = get_env_bool_with_aliases(ENV_HEAL_ENABLED, &[ENV_HEAL_ENABLED_DEPRECATED], true);

    // Stop background services based on what was enabled.
    if enable_scanner {
        info!(
            target: "rustfs::main::handle_shutdown",
            "Stopping background services (data scanner)..."
        );
        shutdown_background_services();
    }

    if enable_heal || enable_scanner {
        info!(
            target: "rustfs::main::handle_shutdown",
            "Stopping AHM services..."
        );
        shutdown_ahm_services();
    }

    if !enable_scanner && !enable_heal {
        info!(
            target: "rustfs::main::handle_shutdown",
            "Background services were disabled, skipping AHM shutdown"
        );
    }

    // Shutdown FTP and FTPS servers
    if let Some(ftp_shutdown_tx) = ftp_shutdown_tx {
        info!(
            target: "rustfs::main::handle_shutdown",
            "Shutting down FTP server..."
        );
        let _ = ftp_shutdown_tx.send(());
    }

    if let Some(ftps_shutdown_tx) = ftps_shutdown_tx {
        info!(
            target: "rustfs::main::handle_shutdown",
            "Shutting down FTPS server..."
        );
        let _ = ftps_shutdown_tx.send(());
    }

    // Shutdown WebDAV server
    if let Some(webdav_shutdown_tx) = webdav_shutdown_tx {
        info!(
            target: "rustfs::main::handle_shutdown",
            "Shutting down WebDAV server..."
        );
        let _ = webdav_shutdown_tx.send(());
    }

    // Shutdown SFTP server
    if let Some(sftp_shutdown_tx) = sftp_shutdown_tx {
        info!(
            target: "rustfs::main::handle_shutdown",
            "Shutting down SFTP server..."
        );
        let _ = sftp_shutdown_tx.send(());
    }

    // Stop the notification system
    info!(
        target: "rustfs::main::handle_shutdown",
        "Shutting down event notifier system..."
    );
    shutdown_event_notifier().await;

    // Stop the audit system
    info!(
        target: "rustfs::main::handle_shutdown",
        "Stopping audit system..."
    );
    match stop_audit_system().await {
        Ok(_) => info!("Audit system stopped successfully."),
        Err(e) => error!("Failed to stop audit system: {}", e),
    }

    // Stop profiling tasks
    info!(
        target: "rustfs::main::handle_shutdown",
        "Stopping profiling tasks..."
    );
    rustfs::profiling::shutdown_profiling();

    info!(
        target: "rustfs::main::handle_shutdown",
        "Server is stopping..."
    );
    if let Some(s3_shutdown_tx) = s3_shutdown_tx {
        let _ = s3_shutdown_tx.send(());
    }
    if let Some(console_shutdown_tx) = console_shutdown_tx {
        let _ = console_shutdown_tx.send(());
    }

    // Wait for the worker thread to complete the cleaning work
    tokio::time::sleep(SHUTDOWN_TIMEOUT).await;

    // the last updated status is stopped
    state_manager.update(ServiceState::Stopped);
    info!(target: "rustfs::main::handle_shutdown", "Server stopped successfully.");
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
        for message in [DEFAULT_CREDENTIALS_WARNING_MESSAGE, DEFAULT_CREDENTIALS_ERROR_MESSAGE] {
            assert!(message.contains(rustfs_config::ENV_RUSTFS_ACCESS_KEY));
            assert!(message.contains(rustfs_config::ENV_RUSTFS_SECRET_KEY));
            assert!(message.contains(rustfs_config::ENV_RUSTFS_ALLOW_INSECURE_DEFAULT_CREDENTIALS));
            assert!(!message.contains(rustfs_credentials::DEFAULT_ACCESS_KEY));
            assert!(!message.contains(rustfs_credentials::DEFAULT_SECRET_KEY));
        }
    }
}
