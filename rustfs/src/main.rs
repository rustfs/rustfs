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

mod admin;
mod auth;
mod config;
mod error;
mod init;
mod license;
mod profiling;
mod protocols;
mod server;
mod storage;
mod update;
mod version;

// Ensure the correct path for parse_license is imported
use crate::init::{
    add_bucket_notification_configuration, init_buffer_profile_system, init_ftp_system, init_kms_system, init_sftp_system,
    init_update_check, print_server_info,
};
use crate::server::{
    SHUTDOWN_TIMEOUT, ServiceState, ServiceStateManager, ShutdownSignal, init_cert, init_event_notifier, shutdown_event_notifier,
    start_audit_system, start_http_server, stop_audit_system, wait_for_shutdown,
};
use clap::Parser;
use license::init_license;
use rustfs_ahm::{
    Scanner, create_ahm_services_cancel_token, heal::storage::ECStoreHealStorage, init_heal_manager,
    scanner::data_scanner::ScannerConfig, shutdown_ahm_services,
};
use rustfs_common::{GlobalReadiness, SystemStage, set_global_addr};
use rustfs_credentials::init_global_action_credentials;
use rustfs_ecstore::{
    StorageAPI,
    bucket::metadata_sys::init_bucket_metadata_sys,
    bucket::replication::{GLOBAL_REPLICATION_POOL, init_background_replication},
    config as ecconfig,
    config::GLOBAL_CONFIG_SYS,
    endpoints::EndpointServerPools,
    global::{set_global_rustfs_port, shutdown_background_services},
    notification_sys::new_global_notification_sys,
    set_global_endpoints,
    store::ECStore,
    store::init_local_disks,
    store_api::BucketOptions,
    update_erasure_type,
};
use rustfs_iam::init_iam_sys;
use rustfs_obs::{init_obs, set_global_guard};
use rustfs_utils::net::parse_and_resolve_address;
use std::io::{Error, Result};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, instrument, warn};

#[cfg(all(target_os = "linux", target_env = "gnu", target_arch = "x86_64"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[cfg(not(all(target_os = "linux", target_env = "gnu", target_arch = "x86_64")))]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn main() -> Result<()> {
    let runtime = server::get_tokio_runtime_builder()
        .build()
        .expect("Failed to build Tokio runtime");
    runtime.block_on(async_main())
}
async fn async_main() -> Result<()> {
    // Parse the obtained parameters
    let opt = config::Opt::parse();

    // Initialize the configuration
    init_license(opt.license.clone());

    // Initialize Observability
    let guard = match init_obs(Some(opt.clone().obs_endpoint)).await {
        Ok(g) => g,
        Err(e) => {
            println!("Failed to initialize observability: {e}");
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

    // print startup logo
    info!("{}", server::LOGO);

    // Initialize performance profiling if enabled
    profiling::init_from_env().await;

    // Initialize TLS if a certificate path is provided
    if let Some(tls_path) = &opt.tls_path {
        match init_cert(tls_path).await {
            Ok(_) => {
                info!(target: "rustfs::main", "TLS initialized successfully with certs from {}", tls_path);
            }
            Err(e) => {
                error!("Failed to initialize TLS from {}: {}", tls_path, e);
                return Err(Error::other(e));
            }
        }
    }

    // Run parameters
    match run(opt).await {
        Ok(_) => Ok(()),
        Err(e) => {
            error!("Server encountered an error and is shutting down: {}", e);
            Err(e)
        }
    }
}

#[instrument(skip(opt))]
async fn run(opt: config::Opt) -> Result<()> {
    debug!("opt: {:?}", &opt);
    // 1. Initialize global readiness tracker
    let readiness = Arc::new(GlobalReadiness::new());

    if let Some(region) = &opt.region {
        rustfs_ecstore::global::set_global_region(region.clone());
    }

    let server_addr = parse_and_resolve_address(opt.address.as_str()).map_err(Error::other)?;
    let server_port = server_addr.port();
    let server_address = server_addr.to_string();

    info!(
        target: "rustfs::main::run",
        server_address = %server_address,
        ip = %server_addr.ip(),
        port = %server_port,
        version = %version::get_version(),
        "Starting RustFS server at {}",
        &server_address
    );

    // Set up AK and SK
    match init_global_action_credentials(Some(opt.access_key.clone()), Some(opt.secret_key.clone())) {
        Ok(_) => {
            info!(target: "rustfs::main::run", "Global action credentials initialized successfully.");
        }
        Err(e) => {
            let msg = format!("init_global_action_credentials failed: {e:?}");
            error!("{msg}");
            return Err(Error::other(msg));
        }
    };

    set_global_rustfs_port(server_port);

    set_global_addr(&opt.address).await;

    // For RPC
    let (endpoint_pools, setup_type) = EndpointServerPools::from_volumes(server_address.clone().as_str(), opt.volumes.clone())
        .await
        .map_err(Error::other)?;

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

    let state_manager = ServiceStateManager::new();
    // Update service status to Starting
    state_manager.update(ServiceState::Starting);

    let s3_shutdown_tx = {
        let mut s3_opt = opt.clone();
        s3_opt.console_enable = false;
        let s3_shutdown_tx = start_http_server(&s3_opt, state_manager.clone(), readiness.clone()).await?;
        Some(s3_shutdown_tx)
    };

    let console_shutdown_tx = if opt.console_enable && !opt.console_address.is_empty() {
        let mut console_opt = opt.clone();
        console_opt.address = console_opt.console_address.clone();
        let console_shutdown_tx = start_http_server(&console_opt, state_manager.clone(), readiness.clone()).await?;
        Some(console_shutdown_tx)
    } else {
        None
    };

    set_global_endpoints(endpoint_pools.as_ref().clone());
    update_erasure_type(setup_type).await;

    // Initialize the local disk
    init_local_disks(endpoint_pools.clone()).await.map_err(Error::other)?;

    let ctx = CancellationToken::new();

    // init store
    // 2. Start Storage Engine (ECStore)
    let store = ECStore::new(server_addr, endpoint_pools.clone(), ctx.clone())
        .await
        .inspect_err(|err| {
            error!("ECStore::new {:?}", err);
        })?;

    ecconfig::init();

    // // Initialize global configuration system
    let mut retry_count = 0;
    while let Err(e) = GLOBAL_CONFIG_SYS.init(store.clone()).await {
        error!("GLOBAL_CONFIG_SYS.init failed {:?}", e);
        // TODO: check error type
        retry_count += 1;
        if retry_count > 15 {
            return Err(Error::other("GLOBAL_CONFIG_SYS.init failed"));
        }
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    }
    readiness.mark_stage(SystemStage::StorageReady);
    // init replication_pool
    init_background_replication(store.clone()).await;
    // Initialize KMS system if enabled
    init_kms_system(&opt).await?;

    // Create a shutdown channel for FTP/SFTP services
    let (ftp_sftp_shutdown_tx, _) = tokio::sync::broadcast::channel(1);

    // Initialize FTP system if enabled
    init_ftp_system(&opt, ftp_sftp_shutdown_tx.clone())
        .await
        .map_err(Error::other)?;

    // Initialize SFTP system if enabled
    init_sftp_system(&opt, ftp_sftp_shutdown_tx.clone())
        .await
        .map_err(Error::other)?;

    // Initialize buffer profiling system
    init_buffer_profile_system(&opt);

    // Initialize event notifier
    init_event_notifier().await;
    // Start the audit system
    match start_audit_system().await {
        Ok(_) => info!(target: "rustfs::main::run","Audit system started successfully."),
        Err(e) => error!(target: "rustfs::main::run","Failed to start audit system: {}", e),
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

    if let Some(pool) = GLOBAL_REPLICATION_POOL.get() {
        pool.clone().init_resync(ctx.clone(), buckets.clone()).await?;
    }

    init_bucket_metadata_sys(store.clone(), buckets.clone()).await;

    // 3. Initialize IAM System (Blocking load)
    // This ensures data is in memory before moving forward
    init_iam_sys(store.clone()).await.map_err(Error::other)?;
    readiness.mark_stage(SystemStage::IamReady);

    add_bucket_notification_configuration(buckets.clone()).await;

    // Initialize the global notification system
    new_global_notification_sys(endpoint_pools.clone()).await.map_err(|err| {
        error!("new_global_notification_sys failed {:?}", &err);
        Error::other(err)
    })?;

    // Create a cancellation token for AHM services
    let _ = create_ahm_services_cancel_token();

    // Check environment variables to determine if scanner and heal should be enabled
    let enable_scanner = rustfs_utils::get_env_bool("RUSTFS_ENABLE_SCANNER", true);
    let enable_heal = rustfs_utils::get_env_bool("RUSTFS_ENABLE_HEAL", true);

    info!(
        target: "rustfs::main::run",
        enable_scanner = enable_scanner,
        enable_heal = enable_heal,
        "Background services configuration: scanner={}, heal={}", enable_scanner, enable_heal
    );

    // Initialize heal manager and scanner based on environment variables
    if enable_heal || enable_scanner {
        if enable_heal {
            // Initialize heal manager with channel processor
            let heal_storage = Arc::new(ECStoreHealStorage::new(store.clone()));
            let heal_manager = init_heal_manager(heal_storage, None).await?;

            if enable_scanner {
                info!(target: "rustfs::main::run","Starting scanner with heal manager...");
                let scanner = Scanner::new(Some(ScannerConfig::default()), Some(heal_manager));
                scanner.start().await?;
            } else {
                info!(target: "rustfs::main::run","Scanner disabled, but heal manager is initialized and available");
            }
        } else if enable_scanner {
            info!("Starting scanner without heal manager...");
            let scanner = Scanner::new(Some(ScannerConfig::default()), None);
            scanner.start().await?;
        }
    } else {
        info!(target: "rustfs::main::run","Both scanner and heal are disabled, skipping AHM service initialization");
    }

    // print server info
    print_server_info();

    init_update_check();

    println!(
        "RustFS server started successfully at {}, current time: {}",
        &server_address,
        chrono::offset::Utc::now().to_string()
    );
    info!(target: "rustfs::main::run","server started successfully at {}", &server_address);
    // 4. Mark as Full Ready now that critical components are warm
    readiness.mark_stage(SystemStage::FullReady);

    // Perform hibernation for 1 second
    tokio::time::sleep(SHUTDOWN_TIMEOUT).await;
    // listen to the shutdown signal
    match wait_for_shutdown().await {
        #[cfg(unix)]
        ShutdownSignal::CtrlC | ShutdownSignal::Sigint | ShutdownSignal::Sigterm => {
            handle_shutdown(&state_manager, s3_shutdown_tx, console_shutdown_tx, ftp_sftp_shutdown_tx, ctx.clone()).await;
        }
        #[cfg(not(unix))]
        ShutdownSignal::CtrlC => {
            handle_shutdown(&state_manager, s3_shutdown_tx, console_shutdown_tx, ftp_sftp_shutdown_tx, ctx.clone()).await;
        }
    }

    info!(target: "rustfs::main::run","server is stopped state: {:?}", state_manager.current_state());
    Ok(())
}

/// Handles the shutdown process of the server
async fn handle_shutdown(
    state_manager: &ServiceStateManager,
    s3_shutdown_tx: Option<tokio::sync::broadcast::Sender<()>>,
    console_shutdown_tx: Option<tokio::sync::broadcast::Sender<()>>,
    ftp_sftp_shutdown_tx: tokio::sync::broadcast::Sender<()>,
    ctx: CancellationToken,
) {
    ctx.cancel();

    info!(
        target: "rustfs::main::handle_shutdown",
        "Shutdown signal received in main thread"
    );
    // update the status to stopping first
    state_manager.update(ServiceState::Stopping);

    // Check environment variables to determine what services need to be stopped
    let enable_scanner = rustfs_utils::get_env_bool("RUSTFS_ENABLE_SCANNER", true);
    let enable_heal = rustfs_utils::get_env_bool("RUSTFS_ENABLE_HEAL", true);

    // Stop background services based on what was enabled
    if enable_scanner || enable_heal {
        info!(
            target: "rustfs::main::handle_shutdown",
            "Stopping background services (data scanner and auto heal)..."
        );
        shutdown_background_services();

        info!(
            target: "rustfs::main::handle_shutdown",
            "Stopping AHM services..."
        );
        shutdown_ahm_services();
    } else {
        info!(
            target: "rustfs::main::handle_shutdown",
            "Background services were disabled, skipping AHM shutdown"
        );
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

    // Send shutdown signal to FTP/SFTP services
    let _ = ftp_sftp_shutdown_tx.send(());

    // the last updated status is stopped
    state_manager.update(ServiceState::Stopped);
    info!(
        target: "rustfs::main::handle_shutdown",
        "Server stopped current "
    );
    println!("Server stopped successfully.");
}
