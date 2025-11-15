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
use crate::server::{
    SHUTDOWN_TIMEOUT, ServiceState, ServiceStateManager, ShutdownSignal, init_event_notifier, shutdown_event_notifier,
    start_audit_system, start_http_server, stop_audit_system, wait_for_shutdown,
};
use crate::storage::ecfs::{process_lambda_configurations, process_queue_configurations, process_topic_configurations};
use chrono::Datelike;
use clap::Parser;
use license::init_license;
use rustfs_ahm::{
    Scanner, create_ahm_services_cancel_token, heal::storage::ECStoreHealStorage, init_heal_manager,
    scanner::data_scanner::ScannerConfig, shutdown_ahm_services,
};
use rustfs_common::globals::set_global_addr;
use rustfs_config::DEFAULT_UPDATE_CHECK;
use rustfs_config::ENV_UPDATE_CHECK;
use rustfs_ecstore::bucket::metadata_sys;
use rustfs_ecstore::bucket::metadata_sys::init_bucket_metadata_sys;
use rustfs_ecstore::bucket::replication::{GLOBAL_REPLICATION_POOL, init_background_replication};
use rustfs_ecstore::config as ecconfig;
use rustfs_ecstore::config::GLOBAL_CONFIG_SYS;
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
use rustfs_notify::notifier_global;
use rustfs_obs::{init_obs, set_global_guard};
use rustfs_targets::arn::TargetID;
use rustfs_utils::net::parse_and_resolve_address;
use s3s::s3_error;
use std::env;
use std::io::{Error, Result};
use std::str::FromStr;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, instrument, warn};

#[cfg(all(target_os = "linux", target_env = "gnu"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[cfg(all(target_os = "linux", target_env = "musl"))]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

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
            println!("Failed to initialize observability: {}", e);
            return Err(Error::other(e));
        }
    };

    // Store in global storage
    match set_global_guard(guard).map_err(Error::other) {
        Ok(_) => (),
        Err(e) => {
            error!("Failed to set global observability guard: {}", e);
            return Err(e);
        }
    }

    // print startup logo
    info!("{}", LOGO);

    // Initialize performance profiling if enabled
    #[cfg(not(target_os = "windows"))]
    profiling::init_from_env().await;

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
    rustfs_ecstore::global::init_global_action_credentials(Some(opt.access_key.clone()), Some(opt.secret_key.clone()));

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
        let s3_shutdown_tx = start_http_server(&s3_opt, state_manager.clone()).await?;
        Some(s3_shutdown_tx)
    };

    let console_shutdown_tx = if opt.console_enable && !opt.console_address.is_empty() {
        let mut console_opt = opt.clone();
        console_opt.address = console_opt.console_address.clone();
        let console_shutdown_tx = start_http_server(&console_opt, state_manager.clone()).await?;
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
    let store = ECStore::new(server_addr, endpoint_pools.clone(), ctx.clone())
        .await
        .inspect_err(|err| {
            error!("ECStore::new {:?}", err);
        })?;

    ecconfig::init();
    // config system configuration
    GLOBAL_CONFIG_SYS.init(store.clone()).await?;

    // init  replication_pool
    init_background_replication(store.clone()).await;
    // Initialize KMS system if enabled
    init_kms_system(&opt).await?;

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

    init_iam_sys(store.clone()).await.map_err(Error::other)?;

    add_bucket_notification_configuration(buckets.clone()).await;

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

    // Perform hibernation for 1 second
    tokio::time::sleep(SHUTDOWN_TIMEOUT).await;
    // listen to the shutdown signal
    match wait_for_shutdown().await {
        #[cfg(unix)]
        ShutdownSignal::CtrlC | ShutdownSignal::Sigint | ShutdownSignal::Sigterm => {
            handle_shutdown(&state_manager, s3_shutdown_tx, console_shutdown_tx, ctx.clone()).await;
        }
        #[cfg(not(unix))]
        ShutdownSignal::CtrlC => {
            handle_shutdown(&state_manager, s3_shutdown_tx, console_shutdown_tx, ctx.clone()).await;
        }
    }

    info!(target: "rustfs::main::run","server is stopped state: {:?}", state_manager.current_state());
    Ok(())
}

/// Parse a boolean environment variable with default value
///
/// Returns true if the environment variable is not set or set to true/1/yes/on/enabled,
/// false if set to false/0/no/off/disabled
fn parse_bool_env_var(var_name: &str, default: bool) -> bool {
    env::var(var_name)
        .unwrap_or_else(|_| default.to_string())
        .parse::<bool>()
        .unwrap_or(default)
}

/// Handles the shutdown process of the server
async fn handle_shutdown(
    state_manager: &ServiceStateManager,
    s3_shutdown_tx: Option<tokio::sync::broadcast::Sender<()>>,
    console_shutdown_tx: Option<tokio::sync::broadcast::Sender<()>>,
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
    let enable_scanner = parse_bool_env_var("RUSTFS_ENABLE_SCANNER", true);
    let enable_heal = parse_bool_env_var("RUSTFS_ENABLE_HEAL", true);

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

    // the last updated status is stopped
    state_manager.update(ServiceState::Stopped);
    info!(
        target: "rustfs::main::handle_shutdown",
        "Server stopped current "
    );
    println!("Server stopped successfully.");
}

fn init_update_check() {
    let update_check_enable = env::var(ENV_UPDATE_CHECK)
        .unwrap_or_else(|_| DEFAULT_UPDATE_CHECK.to_string())
        .parse::<bool>()
        .unwrap_or(DEFAULT_UPDATE_CHECK);

    if !update_check_enable {
        return;
    }

    // Async update check with timeout
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
                info!(
                    target: "rustfs::main::add_bucket_notification_configuration",
                    bucket = %bucket,
                    "Bucket '{}' has existing notification configuration: {:?}", bucket, cfg);

                let mut event_rules = Vec::new();
                process_queue_configurations(&mut event_rules, cfg.queue_configurations.clone(), TargetID::from_str);
                process_topic_configurations(&mut event_rules, cfg.topic_configurations.clone(), TargetID::from_str);
                process_lambda_configurations(&mut event_rules, cfg.lambda_function_configurations.clone(), TargetID::from_str);

                if let Err(e) = notifier_global::add_event_specific_rules(bucket, region, &event_rules)
                    .await
                    .map_err(|e| s3_error!(InternalError, "Failed to add rules: {e}"))
                {
                    error!("Failed to add rules for bucket '{}': {:?}", bucket, e);
                }
            }
            None => {
                info!(
                    target: "rustfs::main::add_bucket_notification_configuration",
                    bucket = %bucket,
                    "Bucket '{}' has no existing notification configuration.", bucket);
            }
        }
    }
}

/// Initialize KMS system and configure if enabled
#[instrument(skip(opt))]
async fn init_kms_system(opt: &config::Opt) -> Result<()> {
    println!("CLAUDE DEBUG: init_kms_system called!");
    info!("CLAUDE DEBUG: init_kms_system called!");
    info!("Initializing KMS service manager...");
    info!(
        "CLAUDE DEBUG: KMS configuration - kms_enable: {}, kms_backend: {}, kms_key_dir: {:?}, kms_default_key_id: {:?}",
        opt.kms_enable, opt.kms_backend, opt.kms_key_dir, opt.kms_default_key_id
    );

    // Initialize global KMS service manager (starts in NotConfigured state)
    let service_manager = rustfs_kms::init_global_kms_service_manager();

    // If KMS is enabled in configuration, configure and start the service
    if opt.kms_enable {
        info!("KMS is enabled via command line, configuring and starting service...");

        // Create KMS configuration from command line options
        let kms_config = match opt.kms_backend.as_str() {
            "local" => {
                let key_dir = opt
                    .kms_key_dir
                    .as_ref()
                    .ok_or_else(|| Error::other("KMS key directory is required for local backend"))?;

                rustfs_kms::config::KmsConfig {
                    backend: rustfs_kms::config::KmsBackend::Local,
                    backend_config: rustfs_kms::config::BackendConfig::Local(rustfs_kms::config::LocalConfig {
                        key_dir: std::path::PathBuf::from(key_dir),
                        master_key: None,
                        file_permissions: Some(0o600),
                    }),
                    default_key_id: opt.kms_default_key_id.clone(),
                    timeout: std::time::Duration::from_secs(30),
                    retry_attempts: 3,
                    enable_cache: true,
                    cache_config: rustfs_kms::config::CacheConfig::default(),
                }
            }
            "vault" => {
                let vault_address = opt
                    .kms_vault_address
                    .as_ref()
                    .ok_or_else(|| Error::other("Vault address is required for vault backend"))?;
                let vault_token = opt
                    .kms_vault_token
                    .as_ref()
                    .ok_or_else(|| Error::other("Vault token is required for vault backend"))?;

                rustfs_kms::config::KmsConfig {
                    backend: rustfs_kms::config::KmsBackend::Vault,
                    backend_config: rustfs_kms::config::BackendConfig::Vault(rustfs_kms::config::VaultConfig {
                        address: vault_address.clone(),
                        auth_method: rustfs_kms::config::VaultAuthMethod::Token {
                            token: vault_token.clone(),
                        },
                        namespace: None,
                        mount_path: "transit".to_string(),
                        kv_mount: "secret".to_string(),
                        key_path_prefix: "rustfs/kms/keys".to_string(),
                        tls: None,
                    }),
                    default_key_id: opt.kms_default_key_id.clone(),
                    timeout: std::time::Duration::from_secs(30),
                    retry_attempts: 3,
                    enable_cache: true,
                    cache_config: rustfs_kms::config::CacheConfig::default(),
                }
            }
            _ => return Err(Error::other(format!("Unsupported KMS backend: {}", opt.kms_backend))),
        };

        // Configure the KMS service
        service_manager
            .configure(kms_config)
            .await
            .map_err(|e| Error::other(format!("Failed to configure KMS: {e}")))?;

        // Start the KMS service
        service_manager
            .start()
            .await
            .map_err(|e| Error::other(format!("Failed to start KMS: {e}")))?;

        info!("KMS service configured and started successfully from command line options");
    } else {
        // Try to load persisted KMS configuration from cluster storage
        info!("Attempting to load persisted KMS configuration from cluster storage...");

        if let Some(persisted_config) = admin::handlers::kms_dynamic::load_kms_config().await {
            info!("Found persisted KMS configuration, attempting to configure and start service...");

            // Configure the KMS service with persisted config
            match service_manager.configure(persisted_config).await {
                Ok(()) => {
                    // Start the KMS service
                    match service_manager.start().await {
                        Ok(()) => {
                            info!("KMS service configured and started successfully from persisted configuration");
                        }
                        Err(e) => {
                            warn!("Failed to start KMS with persisted configuration: {}", e);
                        }
                    }
                }
                Err(e) => {
                    warn!("Failed to configure KMS with persisted configuration: {}", e);
                }
            }
        } else {
            info!("No persisted KMS configuration found. KMS is ready for dynamic configuration via API.");
        }
    }

    Ok(())
}
