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
#[cfg(not(target_os = "windows"))]
mod profiling;
mod server;
mod storage;
mod update;
mod version;

// Ensure the correct path for parse_license is imported
use crate::admin::console::init_console_cfg;
use crate::server::{
    SHUTDOWN_TIMEOUT, ServiceState, ServiceStateManager, ShutdownSignal, start_console_server, start_http_server,
    wait_for_shutdown,
};
use crate::storage::ecfs::{process_lambda_configurations, process_queue_configurations, process_topic_configurations};
use chrono::Datelike;
use clap::Parser;
use license::init_license;
use rustfs_ahm::scanner::data_scanner::ScannerConfig;
use rustfs_ahm::{
    Scanner, create_ahm_services_cancel_token, heal::storage::ECStoreHealStorage, init_heal_manager, shutdown_ahm_services,
};
use rustfs_common::globals::set_global_addr;
use rustfs_config::DEFAULT_DELIMITER;
use rustfs_ecstore::bucket::metadata_sys;
use rustfs_ecstore::bucket::metadata_sys::init_bucket_metadata_sys;
use rustfs_ecstore::cmd::bucket_replication::init_bucket_replication_pool;
use rustfs_ecstore::config as ecconfig;
use rustfs_ecstore::config::GLOBAL_CONFIG_SYS;
use rustfs_ecstore::config::GLOBAL_SERVER_CONFIG;
use rustfs_ecstore::store_api::BucketOptions;
use rustfs_ecstore::{
    StorageAPI,
    endpoints::EndpointServerPools,
    global::{set_global_rustfs_port, shutdown_background_services},
    notification_sys::new_global_notification_sys,
    set_global_endpoints,
    store::ECStore,
    store::init_local_disks,
    update_erasure_type,
};
use rustfs_iam::init_iam_sys;
use rustfs_notify::global::notifier_instance;
use rustfs_obs::{init_obs, set_global_guard};
use rustfs_targets::arn::TargetID;
use rustfs_utils::dns_resolver::init_global_dns_resolver;
use rustfs_utils::net::parse_and_resolve_address;
use s3s::s3_error;
use std::io::{Error, Result};
use std::str::FromStr;
use std::sync::Arc;
use tracing::{debug, error, info, instrument, warn};

#[cfg(all(target_os = "linux", target_env = "gnu"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

const LOGO: &str = r#"

░█▀▄░█░█░█▀▀░▀█▀░█▀▀░█▀▀
░█▀▄░█░█░▀▀█░░█░░█▀▀░▀▀█
░▀░▀░▀▀▀░▀▀▀░░▀░░▀░░░▀▀▀

"#;

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
    // Parse the obtained parameters
    let opt = config::Opt::parse();

    // Initialize the configuration
    init_license(opt.license.clone());

    // Initialize Observability
    let (_logger, guard) = init_obs(Some(opt.clone().obs_endpoint)).await;

    // print startup logo
    info!("{}", LOGO);

    // Store in global storage
    set_global_guard(guard).map_err(Error::other)?;

    // Initialize performance profiling if enabled
    #[cfg(not(target_os = "windows"))]
    profiling::start_profiling_if_enabled();

    // Run parameters
    run(opt).await
}

#[instrument(skip(opt))]
async fn run(opt: config::Opt) -> Result<()> {
    debug!("opt: {:?}", &opt);

    // Initialize global DNS resolver early for enhanced DNS resolution (concurrent)
    let dns_init = tokio::spawn(async {
        if let Err(e) = init_global_dns_resolver().await {
            warn!("Failed to initialize global DNS resolver: {}. Using standard DNS resolution.", e);
        }
    });

    if let Some(region) = &opt.region {
        rustfs_ecstore::global::set_global_region(region.clone());
    }

    let server_addr = parse_and_resolve_address(opt.address.as_str()).map_err(Error::other)?;
    let server_port = server_addr.port();
    let server_address = server_addr.to_string();

    info!("server_address {}, ip:{}", &server_address, server_addr.ip());

    // Set up AK and SK
    rustfs_ecstore::global::init_global_action_cred(Some(opt.access_key.clone()), Some(opt.secret_key.clone()));

    set_global_rustfs_port(server_port);

    set_global_addr(&opt.address).await;

    // Wait for DNS initialization to complete before network-heavy operations
    dns_init.await.map_err(Error::other)?;

    // For RPC
    let (endpoint_pools, setup_type) = EndpointServerPools::from_volumes(server_address.clone().as_str(), opt.volumes.clone())
        .await
        .map_err(Error::other)?;

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
    // Start console server if enabled
    let console_shutdown_tx = shutdown_tx.clone();
    if opt.console_enable && !opt.console_address.is_empty() {
        // Deal with port mapping issues for virtual machines like docker
        let (external_addr, external_port) = if !opt.external_address.is_empty() {
            let external_addr = parse_and_resolve_address(opt.external_address.as_str()).map_err(Error::other)?;
            let external_port = external_addr.port();
            if external_port != server_port {
                warn!(
                    "External port {} is different from server port {}, ensure your firewall allows access to the external port if needed.",
                    external_port, server_port
                );
            }
            info!("Using external address {} for endpoint access", external_addr);
            rustfs_ecstore::global::set_global_rustfs_external_port(external_port);
            set_global_addr(&opt.external_address).await;
            (external_addr.ip(), external_port)
        } else {
            (server_addr.ip(), server_port)
        };
        warn!("Starting console server on address: '{}', port: '{}'", external_addr, external_port);
        // init console configuration
        init_console_cfg(external_addr, external_port);

        let opt_clone = opt.clone();
        tokio::spawn(async move {
            let console_shutdown_rx = console_shutdown_tx.subscribe();
            if let Err(e) = start_console_server(&opt_clone, console_shutdown_rx).await {
                error!("Console server failed to start: {}", e);
            }
        });
    } else {
        info!("Console server is disabled.");
        info!("You can access the RustFS API at {}", &opt.address);
        info!("For more information, visit https://rustfs.com/docs/");
        info!("To enable the console, restart the server with --console-enable and a valid --console-address.");
        info!(
            "Current console address is set to: '{}' ,console enable is set to: '{}'",
            &opt.console_address, &opt.console_enable
        );
    }

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

    // Collect bucket names into a vector
    let buckets: Vec<String> = buckets_list.into_iter().map(|v| v.name).collect();

    // Parallelize initialization tasks for better network performance
    let bucket_metadata_task = tokio::spawn({
        let store = store.clone();
        let buckets = buckets.clone();
        async move {
            init_bucket_metadata_sys(store, buckets).await;
        }
    });

    let iam_init_task = tokio::spawn({
        let store = store.clone();
        async move { init_iam_sys(store).await }
    });

    let notification_config_task = tokio::spawn({
        let buckets = buckets.clone();
        async move {
            add_bucket_notification_configuration(buckets).await;
        }
    });

    // Wait for all parallel initialization tasks to complete
    bucket_metadata_task.await.map_err(Error::other)?;
    iam_init_task.await.map_err(Error::other)??;
    notification_config_task.await.map_err(Error::other)?;

    // Initialize the global notification system
    new_global_notification_sys(endpoint_pools.clone()).await.map_err(|err| {
        error!("new_global_notification_sys failed {:?}", &err);
        Error::other(err)
    })?;

    // Create a cancellation token for AHM services
    let _ = create_ahm_services_cancel_token();

    // Check environment variables to determine if scanner and heal should be enabled
    let enable_scanner = parse_bool_env_var("RUSTFS_ENABLE_SCANNER", true);
    let enable_heal = parse_bool_env_var("RUSTFS_ENABLE_HEAL", true);

    info!("Background services configuration: scanner={}, heal={}", enable_scanner, enable_heal);

    // Initialize heal manager and scanner based on environment variables
    if enable_heal || enable_scanner {
        if enable_heal {
            // Initialize heal manager with channel processor
            let heal_storage = Arc::new(ECStoreHealStorage::new(store.clone()));
            let heal_manager = init_heal_manager(heal_storage, None).await?;

            if enable_scanner {
                info!("Starting scanner with heal manager...");
                let scanner = Scanner::new(Some(ScannerConfig::default()), Some(heal_manager));
                scanner.start().await?;
            } else {
                info!("Scanner disabled, but heal manager is initialized and available");
            }
        } else if enable_scanner {
            info!("Starting scanner without heal manager...");
            let scanner = Scanner::new(Some(ScannerConfig::default()), None);
            scanner.start().await?;
        }
    } else {
        info!("Both scanner and heal are disabled, skipping AHM service initialization");
    }

    // print server info
    print_server_info();
    // initialize bucket replication pool
    init_bucket_replication_pool().await;

    // Async update check with timeout (optional)
    tokio::spawn(async {
        use crate::update::{UpdateCheckError, check_updates};

        // Add timeout to prevent hanging network calls
        match tokio::time::timeout(std::time::Duration::from_secs(30), check_updates()).await {
            Ok(Ok(result)) => {
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
            Ok(Err(UpdateCheckError::HttpError(e))) => {
                debug!("Version check: network error (this is normal): {}", e);
            }
            Ok(Err(e)) => {
                debug!("Version check: failed (this is normal): {}", e);
            }
            Err(_) => {
                debug!("Version check: timeout after 30 seconds (this is normal)");
            }
        }
    });

    // if opt.console_enable {
    //     debug!("console is enabled");
    //     let console_address = opt.console_address.clone();
    //     let tls_path = opt.tls_path.clone();
    //
    //     if console_address.is_empty() {
    //         error!("console_address is empty");
    //         return Err(Error::other("console_address is empty".to_string()));
    //     }
    //
    //     tokio::spawn(async move {
    //         console::start_static_file_server(&console_address, tls_path).await;
    //     });
    // }

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

/// Parse a boolean environment variable with default value
///
/// Returns true if the environment variable is not set or set to true/1/yes/on/enabled,
/// false if set to false/0/no/off/disabled
fn parse_bool_env_var(var_name: &str, default: bool) -> bool {
    std::env::var(var_name)
        .unwrap_or_else(|_| default.to_string())
        .parse::<bool>()
        .unwrap_or(default)
}

/// Handles the shutdown process of the server
async fn handle_shutdown(state_manager: &ServiceStateManager, shutdown_tx: &tokio::sync::broadcast::Sender<()>) {
    info!("Shutdown signal received in main thread");
    // update the status to stopping first
    state_manager.update(ServiceState::Stopping);

    // Check environment variables to determine what services need to be stopped
    let enable_scanner = parse_bool_env_var("RUSTFS_ENABLE_SCANNER", true);
    let enable_heal = parse_bool_env_var("RUSTFS_ENABLE_HEAL", true);

    // Stop background services based on what was enabled
    if enable_scanner || enable_heal {
        info!("Stopping background services (data scanner and auto heal)...");
        shutdown_background_services();

        info!("Stopping AHM services...");
        shutdown_ahm_services();
    } else {
        info!("Background services were disabled, skipping AHM shutdown");
    }

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
async fn init_event_notifier() {
    info!("Initializing event notifier...");

    // 1. Get the global configuration loaded by ecstore
    let server_config = match GLOBAL_SERVER_CONFIG.get() {
        Some(config) => config.clone(), // Clone the config to pass ownership
        None => {
            error!("Event notifier initialization failed: Global server config not loaded.");
            return;
        }
    };

    info!("Global server configuration loaded successfully");
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
    // Use direct await for better error handling and faster initialization
    if let Err(e) = rustfs_notify::initialize(server_config).await {
        error!("Failed to initialize event notifier system: {}", e);
    } else {
        info!("Event notifier system initialized successfully.");
    }
}

#[instrument(skip_all)]
async fn add_bucket_notification_configuration(buckets: Vec<String>) {
    let region_opt = rustfs_ecstore::global::get_global_region();
    let region = match region_opt {
        Some(ref r) if !r.is_empty() => r,
        _ => {
            warn!("Global region is not set; attempting notification configuration for all buckets with an empty region.");
            ""
        }
    };
    for bucket in buckets.iter() {
        let has_notification_config = metadata_sys::get_notification_config(bucket).await.unwrap_or_else(|err| {
            warn!("get_notification_config err {:?}", err);
            None
        });

        match has_notification_config {
            Some(cfg) => {
                info!("Bucket '{}' has existing notification configuration: {:?}", bucket, cfg);

                let mut event_rules = Vec::new();
                process_queue_configurations(&mut event_rules, cfg.queue_configurations.clone(), TargetID::from_str);
                process_topic_configurations(&mut event_rules, cfg.topic_configurations.clone(), TargetID::from_str);
                process_lambda_configurations(&mut event_rules, cfg.lambda_function_configurations.clone(), TargetID::from_str);

                if let Err(e) = notifier_instance()
                    .add_event_specific_rules(bucket, region, &event_rules)
                    .await
                    .map_err(|e| s3_error!(InternalError, "Failed to add rules: {e}"))
                {
                    error!("Failed to add rules for bucket '{}': {:?}", bucket, e);
                }
            }
            None => {
                info!("Bucket '{}' has no existing notification configuration.", bucket);
            }
        }
    }
}

/// Shuts down the event notifier system gracefully
async fn shutdown_event_notifier() {
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
