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
// mod grpc;
pub mod license;
mod server;
mod storage;
mod update;
mod version;

// Ensure the correct path for parse_license is imported
use crate::admin::handlers::profile::start_profilers;
use crate::server::{start_http_server, wait_for_shutdown, ServiceState, ServiceStateManager, ShutdownSignal, SHUTDOWN_TIMEOUT};
use chrono::Datelike;
use clap::Parser;
use license::init_license;
use rustfs_ahm::scanner::data_scanner::ScannerConfig;
use rustfs_ahm::{
    create_ahm_services_cancel_token, heal::storage::ECStoreHealStorage, init_heal_manager, shutdown_ahm_services, Scanner,
};
use rustfs_common::globals::set_global_addr;
use rustfs_config::DEFAULT_DELIMITER;
use rustfs_ecstore::bucket::metadata_sys::init_bucket_metadata_sys;
use rustfs_ecstore::cmd::bucket_replication::init_bucket_replication_pool;
use rustfs_ecstore::config as ecconfig;
use rustfs_ecstore::config::GLOBAL_CONFIG_SYS;
use rustfs_ecstore::config::GLOBAL_SERVER_CONFIG;
use rustfs_ecstore::store_api::BucketOptions;
use rustfs_ecstore::{
    endpoints::EndpointServerPools,
    global::{set_global_rustfs_port, shutdown_background_services},
    notification_sys::new_global_notification_sys,
    set_global_endpoints,
    store::init_local_disks,
    store::ECStore,
    update_erasure_type,
    StorageAPI,
};
use rustfs_iam::init_iam_sys;
use rustfs_obs::{init_obs, set_global_guard};
use rustfs_utils::net::parse_and_resolve_address;
use std::io::{Error, Result};
use std::sync::Arc;
use tracing::{debug, error, info, instrument, warn};

/// Configure Jemalloc memory analysis parameters
// #[allow(non_upper_case_globals, unsafe_code)]
// #[unsafe(export_name = "malloc_conf")]
// pub static malloc_conf: &[u8] = b"prof:true,prof_active:true,lg_prof_sample:16,log:true,narenas:2,lg_chunk:21,background_thread:true,dirty_decay_ms:1000,muzzy_decay_ms:1000\0";

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
pub async fn check_jemalloc_profiling() {
    use jemalloc_pprof;
    use std::env;
    use tikv_jemalloc_ctl::{config, epoch, stats};

    // Refresh Jemalloc's statistics
    epoch::advance().expect("Failed to advance epoch");

    // 1. Check malloc_conf
    match config::malloc_conf::read() {
        Ok(conf_val) => println!("Jemalloc malloc_conf: {}", conf_val),
        Err(e) => println!("Failed to read Jemalloc malloc_conf: {}", e),
    }

    // 2. Check environment variable MALLOC_CONF
    match env::var("MALLOC_CONF") {
        Ok(val) => println!("MALLOC_CONF: {}", val),
        Err(_) => println!("MALLOC_CONF is not set"),
    }

    // 3. Check if the profiling controller is available
    if let Some(prof_ctl) = jemalloc_pprof::PROF_CTL.as_ref() {
        println!("Jemalloc profiling controller is available");
        let mut prof_ctl = prof_ctl.lock().await;
        // 4.Check whether profiling is activated
        if prof_ctl.activated() {
            println!("Jemalloc profiling is activated");
            if let Ok(data) = prof_ctl.dump_pprof() {
                std::fs::write("memory_profile.pb", data).expect("Failed to write a Profiling file");
                println!("Memory Profiling Data has been generated: memory_profile.pb");
            }
        } else {
            println!("Jemalloc profiling is NOT activated");
        }
    } else {
        println!("Jemalloc profiling controller is NOT available");
    }

    // Get the memory usage statistics of Jemalloc
    match stats::allocated::read() {
        Ok(allocated) => println!("Currently allocated memory: {} bytes", allocated),
        Err(e) => println!("Failed to read allocated memory stats: {}", e),
    }

    match stats::resident::read() {
        Ok(resident) => println!("Currently resident memory: {} bytes", resident),
        Err(e) => println!("Failed to read resident memory stats: {}", e),
    }
    match stats::mapped::read() {
        Ok(mapped) => println!("Currently mapped memory: {} bytes", mapped),
        Err(e) => println!("Failed to read mapped memory stats: {}", e),
    }
    match stats::metadata::read() {
        Ok(metadata) => println!("Currently metadata memory: {} bytes", metadata),
        Err(e) => println!("Failed to read metadata memory stats: {}", e),
    }
    match stats::active::read() {
        Ok(active) => println!("Currently active memory: {} bytes", active),
        Err(e) => println!("Failed to read active memory stats: {}", e),
    }
}

#[instrument]
fn print_server_info() {
    let current_year = chrono::Utc::now().year();

    // Use custom macros to print server information
    info!("RustFS Object Storage Server");
    info!("Copyright: 2024-{} RustFS, Inc", current_year);
    info!("License: Apache-2.0 https://www.apache.org/licenses/LICENSE-2.0");
    info!("Version: {}", version::get_version());
    info!("Docs: https://rustfs.com/docs/");
}

#[tokio::main]
async fn main() -> Result<()> {
    #[cfg(not(target_env = "msvc"))]
    {
        check_jemalloc_profiling().await;
        start_profilers();
    }
    // Parse the obtained parameters
    let opt = config::Opt::parse();

    // Initialize the configuration
    init_license(opt.license.clone());

    // Initialize Observability
    let (_logger, guard) = init_obs(Some(opt.clone().obs_endpoint)).await;

    // Store in global storage
    set_global_guard(guard).map_err(Error::other)?;

    // Run parameters
    run(opt).await
}

#[instrument(skip(opt))]
async fn run(opt: config::Opt) -> Result<()> {
    debug!("opt: {:?}", &opt);

    if let Some(region) = &opt.region {
        rustfs_ecstore::global::set_global_region(region.clone());
    }

    let server_addr = parse_and_resolve_address(opt.address.as_str()).map_err(Error::other)?;
    let server_port = server_addr.port();
    let server_address = server_addr.to_string();

    debug!("server_address {}", &server_address);

    // Set up AK and SK
    rustfs_ecstore::global::init_global_action_cred(Some(opt.access_key.clone()), Some(opt.secret_key.clone()));

    set_global_rustfs_port(server_port);

    set_global_addr(&opt.address).await;

    // For RPC
    let (endpoint_pools, setup_type) =
        EndpointServerPools::from_volumes(server_address.clone().as_str(), opt.volumes.clone()).map_err(Error::other)?;

    for (i, eps) in endpoint_pools.as_ref().iter().enumerate() {
        info!(
            "Formatting {}st pool, {} set(s), {} drives per set.",
            i + 1,
            eps.set_count,
            eps.drives_per_set
        );

        if eps.drives_per_set > 1 {
            warn!("WARNING: Host local has more than 0 drives of set. A host failure will result in data becoming unavailable.");
        }
    }

    for (i, eps) in endpoint_pools.as_ref().iter().enumerate() {
        info!(
            "created endpoints {}, set_count:{}, drives_per_set: {}, cmd: {:?}",
            i, eps.set_count, eps.drives_per_set, eps.cmd_line
        );

        for ep in eps.endpoints.as_ref().iter() {
            info!("  - {}", ep);
        }
    }

    let state_manager = ServiceStateManager::new();
    // Update service status to Starting
    state_manager.update(ServiceState::Starting);

    let shutdown_tx = start_http_server(&opt, state_manager.clone()).await?;

    set_global_endpoints(endpoint_pools.as_ref().clone());
    update_erasure_type(setup_type).await;

    // Initialize the local disk
    init_local_disks(endpoint_pools.clone()).await.map_err(Error::other)?;

    // init store
    let store = ECStore::new(server_addr, endpoint_pools.clone()).await.inspect_err(|err| {
        error!("ECStore::new {:?}", err);
    })?;

    ecconfig::init();
    // config system configuration
    GLOBAL_CONFIG_SYS.init(store.clone()).await?;

    // Initialize event notifier
    init_event_notifier().await;

    let buckets_list = store
        .list_bucket(&BucketOptions {
            no_metadata: true,
            ..Default::default()
        })
        .await
        .map_err(Error::other)?;

    let buckets = buckets_list.into_iter().map(|v| v.name).collect();

    init_bucket_metadata_sys(store.clone(), buckets).await;

    init_iam_sys(store.clone()).await?;

    new_global_notification_sys(endpoint_pools.clone()).await.map_err(|err| {
        error!("new_global_notification_sys failed {:?}", &err);
        Error::other(err)
    })?;

    let _ = create_ahm_services_cancel_token();

    // Initialize heal manager with channel processor
    let heal_storage = Arc::new(ECStoreHealStorage::new(store.clone()));
    let heal_manager = init_heal_manager(heal_storage, None).await?;

    let scanner = Scanner::new(Some(ScannerConfig::default()), Some(heal_manager));
    scanner.start().await?;
    print_server_info();
    init_bucket_replication_pool().await;

    // Async update check (optional)
    tokio::spawn(async {
        use crate::update::{check_updates, UpdateCheckError};

        match check_updates().await {
            Ok(result) => {
                if result.update_available {
                    if let Some(latest) = &result.latest_version {
                        info!(
                            "🚀 Version check: New version available: {} -> {} (current: {})",
                            result.current_version, latest.version, result.current_version
                        );
                        if let Some(notes) = &latest.release_notes {
                            info!("📝 Release notes: {}", notes);
                        }
                        if let Some(url) = &latest.download_url {
                            info!("🔗 Download URL: {}", url);
                        }
                    }
                } else {
                    debug!("✅ Version check: Current version is up to date: {}", result.current_version);
                }
            }
            Err(UpdateCheckError::HttpError(e)) => {
                debug!("Version check: network error (this is normal): {}", e);
            }
            Err(e) => {
                debug!("Version check: failed (this is normal): {}", e);
            }
        }
    });

    // Perform hibernation for 1 second
    tokio::time::sleep(SHUTDOWN_TIMEOUT).await;
    // listen to the shutdown signal
    match wait_for_shutdown().await {
        #[cfg(unix)]
        ShutdownSignal::CtrlC | ShutdownSignal::Sigint | ShutdownSignal::Sigterm => {
            handle_shutdown(&state_manager, &shutdown_tx).await;
        }
        #[cfg(not(unix))]
        ShutdownSignal::CtrlC => {
            handle_shutdown(&state_manager, &shutdown_tx).await;
        }
    }

    info!("server is stopped state: {:?}", state_manager.current_state());
    Ok(())
}

/// Handles the shutdown process of the server
async fn handle_shutdown(state_manager: &ServiceStateManager, shutdown_tx: &tokio::sync::broadcast::Sender<()>) {
    info!("Shutdown signal received in main thread");
    // update the status to stopping first
    state_manager.update(ServiceState::Stopping);

    // Stop background services (data scanner and auto heal) gracefully
    info!("Stopping background services (data scanner and auto heal)...");
    shutdown_background_services();

    // Stop AHM services gracefully
    info!("Stopping AHM services...");
    shutdown_ahm_services();

    // Stop the notification system
    shutdown_event_notifier().await;

    info!("Server is stopping...");
    let _ = shutdown_tx.send(());

    // Wait for the worker thread to complete the cleaning work
    tokio::time::sleep(SHUTDOWN_TIMEOUT).await;

    // the last updated status is stopped
    state_manager.update(ServiceState::Stopped);
    info!("Server stopped current ");
}

#[instrument]
pub(crate) async fn init_event_notifier() {
    info!("Initializing event notifier...");

    // 1. Get the global configuration loaded by ecstore
    let server_config = match GLOBAL_SERVER_CONFIG.get() {
        Some(config) => config.clone(), // Clone the config to pass ownership
        None => {
            error!("Event notifier initialization failed: Global server config not loaded.");
            return;
        }
    };

    info!("Global server configuration loaded successfully. config: {:?}", server_config);
    // 2. Check if the notify subsystem exists in the configuration, and skip initialization if it doesn't
    if server_config
        .get_value(rustfs_config::notify::NOTIFY_MQTT_SUB_SYS, DEFAULT_DELIMITER)
        .is_none()
        || server_config
            .get_value(rustfs_config::notify::NOTIFY_WEBHOOK_SUB_SYS, DEFAULT_DELIMITER)
            .is_none()
    {
        info!("'notify' subsystem not configured, skipping event notifier initialization.");
        return;
    }

    info!("Event notifier configuration found, proceeding with initialization.");

    // 3. Initialize the notification system asynchronously with a global configuration
    // Put it into a separate task to avoid blocking the main initialization process
    tokio::spawn(async move {
        if let Err(e) = rustfs_notify::initialize(server_config).await {
            error!("Failed to initialize event notifier system: {}", e);
        } else {
            info!("Event notifier system initialized successfully.");
        }
    });
}

/// Shuts down the event notifier system gracefully
pub async fn shutdown_event_notifier() {
    info!("Shutting down event notifier system...");

    if !rustfs_notify::is_notification_system_initialized() {
        info!("Event notifier system is not initialized, nothing to shut down.");
        return;
    }

    let system = match rustfs_notify::notification_system() {
        Some(sys) => sys,
        None => {
            error!("Event notifier system is not initialized.");
            return;
        }
    };

    // Call the shutdown function from the rustfs_notify module
    system.shutdown().await;
    info!("Event notifier system shut down successfully.");
}
