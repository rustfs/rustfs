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

use crate::runtime_sources::{resolve_notify_interface, resolve_region};
use crate::server::ShutdownHandle;
use crate::storage::{
    get_bucket_notification_config, process_lambda_configurations, process_queue_configurations, process_topic_configurations,
};
use crate::{admin, config, startup_runtime_sources, version};
use rustfs_config::{
    DEFAULT_BUFFER_MAX_SIZE, DEFAULT_BUFFER_MIN_SIZE, DEFAULT_BUFFER_PROFILE, DEFAULT_BUFFER_UNKNOWN_SIZE, DEFAULT_UPDATE_CHECK,
    ENV_RUSTFS_BUFFER_DEFAULT_SIZE, ENV_RUSTFS_BUFFER_MAX_SIZE, ENV_RUSTFS_BUFFER_MIN_SIZE, ENV_UPDATE_CHECK, RUSTFS_REGION,
};
use rustfs_targets::arn::{ARN, TargetIDError};
use rustfs_utils::get_env_usize;
use s3s::s3_error;
use std::env;
use std::io::Error;
use tracing::{debug, error, info, instrument, warn};

const LOG_COMPONENT_INIT: &str = "init";
const LOG_SUBSYSTEM_STARTUP: &str = "startup";
const LOG_SUBSYSTEM_UPDATE: &str = "update_check";
const LOG_SUBSYSTEM_NOTIFICATION: &str = "notification";
const LOG_SUBSYSTEM_KMS: &str = "kms";
const LOG_SUBSYSTEM_BUFFER: &str = "buffer_profile";
const LOG_SUBSYSTEM_AUTOTUNER: &str = "autotuner";
const LOG_SUBSYSTEM_PROTOCOL: &str = "protocol";
const EVENT_PROTOCOL_RUNTIME_STATE: &str = "protocol_runtime_state";
const EVENT_PROTOCOL_SERVER_STATE: &str = "protocol_server_state";

#[instrument]
pub fn print_server_info() {
    let current_year = jiff::Zoned::now().year();
    info!(
        target: "rustfs::init",
        event = "server_identity",
        component = LOG_COMPONENT_INIT,
        subsystem = LOG_SUBSYSTEM_STARTUP,
        product = "RustFS Object Storage Server",
        version = %version::get_version(),
        copyright_year = current_year,
        license = "Apache-2.0",
        docs_url = "https://rustfs.com/docs/",
        "Server identity loaded"
    );
}

/// Initialize the asynchronous update check system.
/// This function checks if update checking is enabled via
/// environment variable or default configuration. If enabled,
/// it spawns an asynchronous task to check for updates with a timeout.
pub fn init_update_check() {
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
                            target: "rustfs::init",
                            event = "update_check_result",
                            component = LOG_COMPONENT_INIT,
                            subsystem = LOG_SUBSYSTEM_UPDATE,
                            result = "update_available",
                            current_version = %result.current_version,
                            latest_version = %latest.version,
                            has_release_notes = latest.release_notes.is_some(),
                            download_url = latest.download_url.as_deref().unwrap_or_default(),
                            "Update check completed"
                        );
                    }
                } else {
                    debug!(
                        target: "rustfs::init",
                        event = "update_check_result",
                        component = LOG_COMPONENT_INIT,
                        subsystem = LOG_SUBSYSTEM_UPDATE,
                        result = "up_to_date",
                        current_version = %result.current_version,
                        "Update check completed"
                    );
                }
            }
            Ok(Err(UpdateCheckError::HttpError(e))) => {
                debug!(
                    target: "rustfs::init",
                    event = "update_check_result",
                    component = LOG_COMPONENT_INIT,
                    subsystem = LOG_SUBSYSTEM_UPDATE,
                    result = "http_error",
                    error = %e,
                    "Update check skipped"
                );
            }
            Ok(Err(e)) => {
                debug!(
                    target: "rustfs::init",
                    event = "update_check_result",
                    component = LOG_COMPONENT_INIT,
                    subsystem = LOG_SUBSYSTEM_UPDATE,
                    result = "failed",
                    error = %e,
                    "Update check failed"
                );
            }
            Err(_) => {
                debug!(
                    target: "rustfs::init",
                    event = "update_check_result",
                    component = LOG_COMPONENT_INIT,
                    subsystem = LOG_SUBSYSTEM_UPDATE,
                    result = "timeout",
                    timeout_secs = 30,
                    "Update check timed out"
                );
            }
        }
    });
}

/// Helper function to parse ARN string to target ID
/// Converts an ARN string to a target ID, or returns an error if parsing fails
fn arn_to_target_id(arn_str: &str) -> Result<rustfs_targets::arn::TargetID, TargetIDError> {
    ARN::parse(arn_str)
        .map(|arn| arn.target_id)
        .map_err(|e| TargetIDError::InvalidFormat(e.to_string()))
}

/// Add existing bucket notification configurations to the global notifier system.
/// This function retrieves notification configurations for each bucket
/// and registers the corresponding event rules with the notifier system.
///  It processes queue, topic, and lambda configurations and maps them to event rules.
///  # Arguments
/// * `buckets` - A vector of bucket names to process
#[instrument(skip_all)]
pub async fn add_bucket_notification_configuration(buckets: Vec<String>) {
    let global_region = resolve_region();
    let region = global_region
        .as_ref()
        .filter(|r| !r.as_str().is_empty())
        .map(|r| r.as_str())
        .unwrap_or_else(|| {
            warn!(
                target: "rustfs::init",
                event = "notification_region_fallback",
                component = LOG_COMPONENT_INIT,
                subsystem = LOG_SUBSYSTEM_NOTIFICATION,
                fallback_region = RUSTFS_REGION,
                "Notification configuration falling back to default region"
            );
            RUSTFS_REGION
        });
    for bucket in buckets.iter() {
        let has_notification_config = get_bucket_notification_config(bucket).await.unwrap_or_else(|err| {
            warn!(
                target: "rustfs::init",
                event = "notification_config_load_failed",
                component = LOG_COMPONENT_INIT,
                subsystem = LOG_SUBSYSTEM_NOTIFICATION,
                bucket = %bucket,
                error = ?err,
                "Failed to load bucket notification configuration"
            );
            None
        });

        match has_notification_config {
            Some(cfg) => {
                info!(
                    target: "rustfs::init",
                    event = "notification_config_loaded",
                    component = LOG_COMPONENT_INIT,
                    subsystem = LOG_SUBSYSTEM_NOTIFICATION,
                    bucket = %bucket,
                    queue_configuration_count = cfg.queue_configurations.as_ref().map_or(0, Vec::len),
                    topic_configuration_count = cfg.topic_configurations.as_ref().map_or(0, Vec::len),
                    lambda_configuration_count = cfg.lambda_function_configurations.as_ref().map_or(0, Vec::len),
                    "Loaded bucket notification configuration"
                );

                let mut event_rules = Vec::new();
                if let Err(e) = process_queue_configurations(&mut event_rules, cfg.queue_configurations.clone(), arn_to_target_id)
                {
                    error!(
                        target: "rustfs::init",
                        event = "notification_config_parse_failed",
                        component = LOG_COMPONENT_INIT,
                        subsystem = LOG_SUBSYSTEM_NOTIFICATION,
                        bucket = %bucket,
                        config_type = "queue",
                        error = ?e,
                        "Failed to parse bucket notification configuration"
                    );
                }
                if let Err(e) = process_topic_configurations(&mut event_rules, cfg.topic_configurations.clone(), arn_to_target_id)
                {
                    error!(
                        target: "rustfs::init",
                        event = "notification_config_parse_failed",
                        component = LOG_COMPONENT_INIT,
                        subsystem = LOG_SUBSYSTEM_NOTIFICATION,
                        bucket = %bucket,
                        config_type = "topic",
                        error = ?e,
                        "Failed to parse bucket notification configuration"
                    );
                }
                if let Err(e) =
                    process_lambda_configurations(&mut event_rules, cfg.lambda_function_configurations.clone(), arn_to_target_id)
                {
                    error!(
                        target: "rustfs::init",
                        event = "notification_config_parse_failed",
                        component = LOG_COMPONENT_INIT,
                        subsystem = LOG_SUBSYSTEM_NOTIFICATION,
                        bucket = %bucket,
                        config_type = "lambda",
                        error = ?e,
                        "Failed to parse bucket notification configuration"
                    );
                }

                if let Err(e) = resolve_notify_interface()
                    .add_event_specific_rules(bucket, region, &event_rules)
                    .await
                    .map_err(|e| s3_error!(InternalError, "Failed to add rules: {e}"))
                {
                    error!(
                        target: "rustfs::init",
                        event = "notification_rules_registration_failed",
                        component = LOG_COMPONENT_INIT,
                        subsystem = LOG_SUBSYSTEM_NOTIFICATION,
                        bucket = %bucket,
                        region,
                        error = ?e,
                        "Failed to register bucket notification rules"
                    );
                }
            }
            None => {
                info!(
                    target: "rustfs::init",
                    event = "notification_config_missing",
                    component = LOG_COMPONENT_INIT,
                    subsystem = LOG_SUBSYSTEM_NOTIFICATION,
                    bucket = %bucket,
                    "Bucket notification configuration not found"
                );
            }
        }
    }
}

/// Build KMS configuration for local backend
fn build_local_kms_config(cfg: &config::Config) -> std::io::Result<rustfs_kms::config::KmsConfig> {
    let key_dir = cfg
        .kms_key_dir
        .as_ref()
        .ok_or_else(|| Error::other("KMS key directory is required for local backend"))?;

    Ok(rustfs_kms::config::KmsConfig {
        backend: rustfs_kms::config::KmsBackend::Local,
        backend_config: rustfs_kms::config::BackendConfig::Local(rustfs_kms::config::LocalConfig {
            key_dir: std::path::PathBuf::from(key_dir),
            master_key: cfg.kms_local_master_key.clone(),
            file_permissions: Some(0o600),
        }),
        allow_insecure_dev_defaults: cfg.kms_allow_insecure_dev_defaults,
        default_key_id: cfg.kms_default_key_id.clone(),
        timeout: std::time::Duration::from_secs(30),
        retry_attempts: 3,
        enable_cache: true,
        cache_config: rustfs_kms::config::CacheConfig::default(),
    })
}

/// Build KMS configuration for Vault backend
fn build_vault_kms_config(cfg: &config::Config) -> std::io::Result<rustfs_kms::config::KmsConfig> {
    let vault_address = cfg
        .kms_vault_address
        .as_ref()
        .ok_or_else(|| Error::other("Vault address is required for vault backend"))?;
    let vault_token = cfg
        .kms_vault_token
        .as_ref()
        .ok_or_else(|| Error::other("Vault token is required for vault backend"))?;

    Ok(rustfs_kms::config::KmsConfig {
        backend: rustfs_kms::config::KmsBackend::VaultKv2,
        backend_config: rustfs_kms::config::BackendConfig::VaultKv2(Box::new(rustfs_kms::config::VaultConfig {
            address: vault_address.clone(),
            auth_method: rustfs_kms::config::VaultAuthMethod::Token {
                token: vault_token.clone(),
            },
            namespace: None,
            mount_path: cfg.kms_vault_mount_path.clone().unwrap_or_else(|| "transit".to_string()),
            kv_mount: "secret".to_string(),
            key_path_prefix: "rustfs/kms/keys".to_string(),
            tls: None,
        })),
        allow_insecure_dev_defaults: cfg.kms_allow_insecure_dev_defaults,
        default_key_id: cfg.kms_default_key_id.clone(),
        timeout: std::time::Duration::from_secs(30),
        retry_attempts: 3,
        enable_cache: true,
        cache_config: rustfs_kms::config::CacheConfig::default(),
    })
}

/// Build KMS configuration for Vault Transit backend
fn build_vault_transit_kms_config(cfg: &config::Config) -> std::io::Result<rustfs_kms::config::KmsConfig> {
    let vault_address = cfg
        .kms_vault_address
        .as_ref()
        .ok_or_else(|| Error::other("Vault address is required for vault-transit backend"))?;
    let vault_token = cfg
        .kms_vault_token
        .as_ref()
        .ok_or_else(|| Error::other("Vault token is required for vault-transit backend"))?;

    Ok(rustfs_kms::config::KmsConfig {
        backend: rustfs_kms::config::KmsBackend::VaultTransit,
        backend_config: rustfs_kms::config::BackendConfig::VaultTransit(Box::new(rustfs_kms::config::VaultTransitConfig {
            address: vault_address.clone(),
            auth_method: rustfs_kms::config::VaultAuthMethod::Token {
                token: vault_token.clone(),
            },
            namespace: None,
            mount_path: cfg.kms_vault_mount_path.clone().unwrap_or_else(|| "transit".to_string()),
            tls: None,
        })),
        allow_insecure_dev_defaults: cfg.kms_allow_insecure_dev_defaults,
        default_key_id: cfg.kms_default_key_id.clone(),
        timeout: std::time::Duration::from_secs(30),
        retry_attempts: 3,
        enable_cache: true,
        cache_config: rustfs_kms::config::CacheConfig::default(),
    })
}

/// Configure and start KMS service
async fn configure_and_start_kms(
    service_manager: &std::sync::Arc<rustfs_kms::KmsServiceManager>,
    kms_config: rustfs_kms::config::KmsConfig,
    config_source: &str,
) -> std::io::Result<()> {
    service_manager
        .configure(kms_config)
        .await
        .map_err(|e| Error::other(format!("Failed to configure KMS: {e}")))?;

    service_manager
        .start()
        .await
        .map_err(|e| Error::other(format!("Failed to start KMS: {e}")))?;

    info!(
        target: "rustfs::init",
        event = "kms_service_state",
        component = LOG_COMPONENT_INIT,
        subsystem = LOG_SUBSYSTEM_KMS,
        state = "started",
        config_source,
        "KMS service state changed"
    );
    Ok(())
}

/// Initialize KMS system and configure if enabled
///
/// This function initializes the global KMS service manager. If KMS is enabled
/// via command line options, it configures and starts the service accordingly.
/// If not enabled, it attempts to load any persisted KMS configuration from
/// cluster storage and starts the service if found.
/// # Arguments
/// * `config` - The application configuration options
///
/// Returns `std::io::Result<()>` indicating success or failure
#[instrument(skip(config))]
pub async fn init_kms_system(config: &config::Config) -> std::io::Result<()> {
    // Initialize global KMS service manager (starts in NotConfigured state)
    let service_manager = startup_runtime_sources::init_kms_service_manager();

    // If KMS is enabled in configuration, configure and start the service
    if config.kms_enable {
        info!(
            target: "rustfs::init",
            event = "kms_service_state",
            component = LOG_COMPONENT_INIT,
            subsystem = LOG_SUBSYSTEM_KMS,
            state = "configuring",
            config_source = "command_line",
            "KMS service state changed"
        );

        // Create KMS configuration from command line options
        let kms_config = match config.kms_backend.as_str() {
            "local" => build_local_kms_config(config)?,
            "vault" | "vault-kv2" | "vault_kv2" => build_vault_kms_config(config)?,
            "vault-transit" | "vault_transit" => build_vault_transit_kms_config(config)?,
            _ => return Err(Error::other(format!("Unsupported KMS backend: {}", config.kms_backend))),
        };

        configure_and_start_kms(&service_manager, kms_config, "command line options").await?;
    } else {
        // Try to load persisted KMS configuration from cluster storage
        info!(
            target: "rustfs::init",
            event = "kms_persisted_config_lookup",
            component = LOG_COMPONENT_INIT,
            subsystem = LOG_SUBSYSTEM_KMS,
            state = "loading",
            "Loading persisted KMS configuration"
        );

        if let Some(persisted_config) = admin::handlers::kms_dynamic::load_kms_config().await {
            info!(
                target: "rustfs::init",
                event = "kms_persisted_config_lookup",
                component = LOG_COMPONENT_INIT,
                subsystem = LOG_SUBSYSTEM_KMS,
                state = "found",
                "Loaded persisted KMS configuration"
            );

            // Configure the KMS service with persisted config
            match configure_and_start_kms(&service_manager, persisted_config, "persisted configuration").await {
                Ok(()) => {}
                Err(e) => {
                    warn!(
                        target: "rustfs::init",
                        event = "kms_service_state",
                        component = LOG_COMPONENT_INIT,
                        subsystem = LOG_SUBSYSTEM_KMS,
                        state = "persisted_config_failed",
                        error = %e,
                        "KMS service state changed"
                    );
                }
            }
        } else {
            info!(
                target: "rustfs::init",
                event = "kms_persisted_config_lookup",
                component = LOG_COMPONENT_INIT,
                subsystem = LOG_SUBSYSTEM_KMS,
                state = "not_found",
                "No persisted KMS configuration found"
            );
        }
    }

    Ok(())
}

/// Initialize the adaptive buffer sizing system with workload profile configuration.
///
/// This system provides intelligent buffer size selection based on file size and workload type.
/// Workload-aware buffer sizing is enabled by default with the GeneralPurpose profile,
/// which provides the same buffer sizes as the original implementation for compatibility.
///
/// # Configuration
/// - Default: Enabled with GeneralPurpose profile
/// - Opt-out: Use `--buffer-profile-disable` flag
/// - Custom profile: Set via `--buffer-profile` or `RUSTFS_BUFFER_PROFILE` environment variable
///
/// # Arguments
/// * `config` - The application configuration options
pub fn init_buffer_profile_system(config: &config::Config) {
    use crate::config::WorkloadProfile;

    // Whether buffer profiling is disabled or not, it is enabled by default, unless the user explicitly sets '--buffer-profile-disable' or 'RUSTFS_BUFFER_PROFILE_DISABLE=true'
    if config.buffer_profile_disable {
        // User explicitly disabled buffer profiling - use GeneralPurpose profile in disabled mode
        info!(
            target: "rustfs::init",
            event = "buffer_profile_state",
            component = LOG_COMPONENT_INIT,
            subsystem = LOG_SUBSYSTEM_BUFFER,
            state = "disabled",
            profile = "GeneralPurpose",
            reason = "flag_override",
            "Buffer profile state changed"
        );
        startup_runtime_sources::set_buffer_profile_enabled(false);
    } else {
        // Enabled by default: use configured workload profile
        info!(
            target: "rustfs::init",
            event = "buffer_profile_state",
            component = LOG_COMPONENT_INIT,
            subsystem = LOG_SUBSYSTEM_BUFFER,
            state = "configuring",
            profile = %config.buffer_profile,
            "Buffer profile state changed"
        );

        // Parse the workload profile from configuration string
        // Support a custom profile when buffer_profile is set to "custom";
        // its sizes are controlled via RUSTFS_BUFFER_MIN_SIZE, RUSTFS_BUFFER_MAX_SIZE,
        // and RUSTFS_BUFFER_DEFAULT_SIZE environment variables.
        let profile = if config.buffer_profile.eq_ignore_ascii_case("custom") {
            // Try to create custom profile from environment variables
            let min_size = get_env_usize(ENV_RUSTFS_BUFFER_MIN_SIZE, DEFAULT_BUFFER_MIN_SIZE);
            let max_size = get_env_usize(ENV_RUSTFS_BUFFER_MAX_SIZE, DEFAULT_BUFFER_MAX_SIZE);
            let default_unknown = get_env_usize(ENV_RUSTFS_BUFFER_DEFAULT_SIZE, DEFAULT_BUFFER_UNKNOWN_SIZE);

            info!(
                target: "rustfs::init",
                event = "buffer_profile_custom",
                component = LOG_COMPONENT_INIT,
                subsystem = LOG_SUBSYSTEM_BUFFER,
                min_size,
                max_size,
                default_size = default_unknown,
                "Creating custom buffer profile"
            );
            WorkloadProfile::custom(
                min_size,
                max_size,
                default_unknown,
                vec![
                    (1024 * 1024, 64 * 1024),        // < 1MB: 64KB
                    (100 * 1024 * 1024, 256 * 1024), // 1MB-100MB: 256KB
                    (i64::MAX, 1024 * 1024),         // >= 100MB: 1MB
                ],
            )
        } else {
            WorkloadProfile::from_name(&config.buffer_profile)
        };

        // Log the selected profile for operational visibility
        info!(
            target: "rustfs::init",
            event = "buffer_profile_selected",
            component = LOG_COMPONENT_INIT,
            subsystem = LOG_SUBSYSTEM_BUFFER,
            profile = ?profile,
            "Selected buffer profile"
        );

        let fallback_profile = WorkloadProfile::from_name(DEFAULT_BUFFER_PROFILE);
        let Some(buffer_config) = resolve_buffer_profile_config(profile, fallback_profile) else {
            warn!(
                target: "rustfs::init",
                event = "buffer_profile_validation_failed",
                component = LOG_COMPONENT_INIT,
                subsystem = LOG_SUBSYSTEM_BUFFER,
                error = "all buffer profile configurations rejected",
                fallback_profile = DEFAULT_BUFFER_PROFILE,
                "Buffer profile initialization disabled after validation failures"
            );
            startup_runtime_sources::set_buffer_profile_enabled(false);
            return;
        };

        // Log the workload profile name
        let workload_name = buffer_config.workload_name();
        info!(
            target: "rustfs::init",
            event = "buffer_profile_workload",
            component = LOG_COMPONENT_INIT,
            subsystem = LOG_SUBSYSTEM_BUFFER,
            workload = %workload_name,
            "Buffer profile workload selected"
        );

        // Initialize the global buffer configuration
        startup_runtime_sources::init_buffer_config(buffer_config);

        // Enable buffer profiling globally
        startup_runtime_sources::set_buffer_profile_enabled(true);

        info!(
            target: "rustfs::init",
            event = "buffer_profile_state",
            component = LOG_COMPONENT_INIT,
            subsystem = LOG_SUBSYSTEM_BUFFER,
            state = "initialized",
            workload = %workload_name,
            "Buffer profile state changed"
        );
    }
}

fn resolve_buffer_profile_config(
    profile: crate::config::WorkloadProfile,
    fallback_profile: crate::config::WorkloadProfile,
) -> Option<crate::config::RustFSBufferConfig> {
    use crate::config::RustFSBufferConfig;

    let buffer_config = RustFSBufferConfig::new(profile);
    if let Err(err) = buffer_config.validate() {
        warn!(
            target: "rustfs::init",
            event = "buffer_profile_validation_failed",
            component = LOG_COMPONENT_INIT,
            subsystem = LOG_SUBSYSTEM_BUFFER,
            error = %err,
            fallback_profile = DEFAULT_BUFFER_PROFILE,
            "Buffer profile validation failed"
        );

        info!(
            target: "rustfs::init",
            event = "buffer_profile_fallback",
            component = LOG_COMPONENT_INIT,
            subsystem = LOG_SUBSYSTEM_BUFFER,
            profile = ?fallback_profile,
            "Using fallback buffer profile"
        );

        let fallback_config = RustFSBufferConfig::new(fallback_profile);
        if let Err(fallback_err) = fallback_config.validate() {
            error!(
                target: "rustfs::init",
                event = "buffer_profile_validation_failed",
                component = LOG_COMPONENT_INIT,
                subsystem = LOG_SUBSYSTEM_BUFFER,
                error = %fallback_err,
                fallback_profile = DEFAULT_BUFFER_PROFILE,
                "Fallback buffer profile validation failed"
            );
            return None;
        }

        Some(fallback_config)
    } else {
        Some(buffer_config)
    }
}

/// Parse and normalize server address for FTP/FTPS
/// Forces IPv4 binding to avoid libunftp IPv6 compatibility issues
#[allow(dead_code)]
async fn parse_and_normalize_server_address(
    address_str: &str,
) -> Result<std::net::SocketAddr, Box<dyn std::error::Error + Send + Sync>> {
    let addr = rustfs_utils::net::parse_and_resolve_address(address_str)
        .map_err(|e| format!("Invalid server address '{address_str}': {e}"))?;

    // Force IPv4 binding to avoid libunftp IPv6 compatibility issues
    let normalized_addr = if addr.is_ipv6() {
        std::net::SocketAddr::new(std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED), addr.port())
    } else {
        addr
    };

    Ok(normalized_addr)
}

/// Start FTP/FTPS server in background with shutdown support
/// # Arguments
/// * `server` - The FTP/FTPS server instance
/// * `protocol_name` - Name of the protocol (e.g., "FTP", "FTPS")
#[allow(dead_code)]
fn spawn_server<S>(server: S, protocol_name: &'static str) -> tokio::sync::broadcast::Sender<()>
where
    S: std::future::Future<Output = Result<(), Box<dyn std::error::Error>>> + Send + 'static,
{
    let (shutdown_tx, _) = tokio::sync::broadcast::channel(1);

    tokio::spawn(async move {
        if let Err(e) = server.await {
            error!(
                target: "rustfs::init",
                event = "protocol_server_state",
                component = LOG_COMPONENT_INIT,
                subsystem = LOG_SUBSYSTEM_PROTOCOL,
                protocol = protocol_name,
                state = "runtime_failed",
                error = %e,
                "Protocol server failed"
            );
        }
        info!(
            target: "rustfs::init",
            event = "protocol_server_state",
            component = LOG_COMPONENT_INIT,
            subsystem = LOG_SUBSYSTEM_PROTOCOL,
            protocol = protocol_name,
            state = "stopped",
            "Protocol server stopped"
        );
    });

    shutdown_tx
}

/// Starts the auto-tuner for performance optimization if enabled via environment variable.
///
/// The auto-tuner reads `RUSTFS_AUTOTUNER_ENABLED` to decide whether to run.
/// When enabled, it spawns a background task that tunes concurrency settings
/// every 60 seconds.
pub async fn init_auto_tuner(ctx: tokio_util::sync::CancellationToken) {
    use crate::storage::concurrency::get_concurrency_manager;
    use rustfs_io_metrics::AutoTuner;
    use rustfs_io_metrics::TunerConfig;
    use tracing::{debug, error, info};

    let autotuner_enabled = rustfs_utils::get_env_bool("RUSTFS_AUTOTUNER_ENABLED", false);

    if autotuner_enabled {
        info!(
            target: "rustfs::init",
            event = "autotuner_state",
            component = LOG_COMPONENT_INIT,
            subsystem = LOG_SUBSYSTEM_AUTOTUNER,
            state = "starting",
            "Auto-tuner state changed"
        );

        let config = TunerConfig::default();
        let manager = get_concurrency_manager();
        let performance_metrics = manager.performance_metrics();

        tokio::spawn(async move {
            let mut tuner = AutoTuner::with_config(config).with_metrics(performance_metrics);

            loop {
                tokio::select! {
                    _ = ctx.cancelled() => {
                        info!(
                            target: "rustfs::init",
                            event = "autotuner_state",
                            component = LOG_COMPONENT_INIT,
                            subsystem = LOG_SUBSYSTEM_AUTOTUNER,
                            state = "stopping",
                            "Auto-tuner state changed"
                        );
                        break;
                    }
                    _ = tokio::time::sleep(tokio::time::Duration::from_secs(60)) => {
                        if let Err(e) = tuner.tune().await {
                            error!(
                                target: "rustfs::init",
                                event = "autotuner_iteration",
                                component = LOG_COMPONENT_INIT,
                                subsystem = LOG_SUBSYSTEM_AUTOTUNER,
                                result = "failed",
                                error = %e,
                                "Auto-tuner iteration completed"
                            );
                        } else {
                            debug!(
                                target: "rustfs::init",
                                event = "autotuner_iteration",
                                component = LOG_COMPONENT_INIT,
                                subsystem = LOG_SUBSYSTEM_AUTOTUNER,
                                result = "ok",
                                "Auto-tuner iteration completed"
                            );
                        }
                    }
                }
            }
        });

        info!(
            target: "rustfs::init",
            event = "autotuner_state",
            component = LOG_COMPONENT_INIT,
            subsystem = LOG_SUBSYSTEM_AUTOTUNER,
            state = "started",
            "Auto-tuner state changed"
        );
    } else {
        info!(
            target: "rustfs::init",
            event = "autotuner_state",
            component = LOG_COMPONENT_INIT,
            subsystem = LOG_SUBSYSTEM_AUTOTUNER,
            state = "disabled",
            "Auto-tuner state changed"
        );
    }
}

/// Initialize the FTP system
///
/// This function initializes the FTP server (non-encrypted) if enabled in the configuration.
#[cfg(feature = "ftps")]
#[instrument(skip_all)]
pub async fn init_ftp_system() -> Result<Option<ShutdownHandle>, Box<dyn std::error::Error + Send + Sync>> {
    {
        use crate::protocols::ProtocolStorageClient;
        use rustfs_config::{DEFAULT_FTP_ADDRESS, ENV_FTP_ADDRESS, ENV_FTP_ENABLE, ENV_FTP_EXTERNAL_IP, ENV_FTP_PASSIVE_PORTS};
        use rustfs_protocols::constants::defaults::DEFAULT_FTPS_PASSIVE_PORTS;
        use rustfs_protocols::{FtpsConfig, FtpsServer};
        // Check if FTP is enabled
        let ftp_enable = rustfs_utils::get_env_bool(ENV_FTP_ENABLE, false);
        if !ftp_enable {
            debug!(
                target: "rustfs::init",
                event = EVENT_PROTOCOL_RUNTIME_STATE,
                component = LOG_COMPONENT_INIT,
                subsystem = LOG_SUBSYSTEM_PROTOCOL,
                protocol = "ftp",
                state = "disabled",
                "Protocol runtime disabled"
            );
            return Ok(None);
        }

        // Parse FTP address - force IPv4 for libunftp compatibility
        let ftp_address_str = rustfs_utils::get_env_str(ENV_FTP_ADDRESS, DEFAULT_FTP_ADDRESS);
        let addr = parse_and_normalize_server_address(&ftp_address_str).await?;

        // Get FTP configuration from environment variables
        let passive_ports =
            rustfs_utils::get_env_opt_str(ENV_FTP_PASSIVE_PORTS).or_else(|| Some(DEFAULT_FTPS_PASSIVE_PORTS.to_string())); // Default passive ports range
        let external_ip = rustfs_utils::get_env_opt_str(ENV_FTP_EXTERNAL_IP);

        // Create FTP configuration (TLS disabled, FTPS not required)
        let config = FtpsConfig {
            bind_addr: addr,
            passive_ports,
            external_ip,
            ftps_required: false,
            tls_enabled: false,
            cert_dir: None,
            ca_file: None,
        };

        // Validate FTP configuration
        config.validate().await?;

        // Create FTP server with protocol storage client
        let fs = crate::storage::ecfs::FS::new();
        let storage_client = ProtocolStorageClient::new(fs);
        let server: FtpsServer<ProtocolStorageClient> = FtpsServer::new(config, storage_client).await?;
        let bind_addr = server.config().bind_addr;
        let passive_ports = server.config().passive_ports.clone();

        // Log server configuration
        debug!(
            target: "rustfs::init",
            event = EVENT_PROTOCOL_RUNTIME_STATE,
            component = LOG_COMPONENT_INIT,
            subsystem = LOG_SUBSYSTEM_PROTOCOL,
            protocol = "ftp",
            state = "configured",
            bind_addr = %bind_addr,
            passive_ports = ?passive_ports,
            tls_enabled = false,
            "Protocol runtime configured"
        );

        // Start FTP server in background task with proper shutdown support
        let (shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel(1);

        let task_handle = tokio::spawn(async move {
            if let Err(e) = server.start(shutdown_rx).await {
                error!(
                    target: "rustfs::init",
                    event = EVENT_PROTOCOL_SERVER_STATE,
                    component = LOG_COMPONENT_INIT,
                    subsystem = LOG_SUBSYSTEM_PROTOCOL,
                    protocol = "ftp",
                    state = "runtime_failed",
                    error = %e,
                    "Protocol server failed"
                );
            }
            info!(
                target: "rustfs::init",
                event = EVENT_PROTOCOL_SERVER_STATE,
                component = LOG_COMPONENT_INIT,
                subsystem = LOG_SUBSYSTEM_PROTOCOL,
                protocol = "ftp",
                state = "stopped",
                "Protocol server stopped"
            );
        });

        info!(
            target: "rustfs::init",
            event = EVENT_PROTOCOL_RUNTIME_STATE,
            component = LOG_COMPONENT_INIT,
            subsystem = LOG_SUBSYSTEM_PROTOCOL,
            protocol = "ftp",
            state = "started",
            bind_addr = %bind_addr,
            tls_enabled = false,
            "Protocol runtime started"
        );
        Ok(Some(ShutdownHandle::new(shutdown_tx, task_handle)))
    }
}

/// Initialize the FTPS system
///
/// This function initializes the FTPS server if enabled in the configuration.
/// It sets up the FTPS server with the appropriate configuration and starts
/// the server in a background task.
#[cfg(feature = "ftps")]
#[instrument(skip_all)]
pub async fn init_ftps_system() -> Result<Option<ShutdownHandle>, Box<dyn std::error::Error + Send + Sync>> {
    {
        use crate::protocols::ProtocolStorageClient;
        use rustfs_config::{
            DEFAULT_FTPS_ADDRESS, ENV_FTPS_ADDRESS, ENV_FTPS_CA_FILE, ENV_FTPS_CERTS_DIR, ENV_FTPS_ENABLE, ENV_FTPS_EXTERNAL_IP,
            ENV_FTPS_PASSIVE_PORTS, ENV_FTPS_TLS_ENABLED,
        };
        use rustfs_protocols::constants::defaults::DEFAULT_FTPS_PASSIVE_PORTS;
        use rustfs_protocols::{FtpsConfig, FtpsServer};
        // Check if FTPS is enabled
        let ftps_enable = rustfs_utils::get_env_bool(ENV_FTPS_ENABLE, false);
        if !ftps_enable {
            debug!(
                target: "rustfs::init",
                event = EVENT_PROTOCOL_RUNTIME_STATE,
                component = LOG_COMPONENT_INIT,
                subsystem = LOG_SUBSYSTEM_PROTOCOL,
                protocol = "ftps",
                state = "disabled",
                "Protocol runtime disabled"
            );
            return Ok(None);
        }

        // Parse FTPS address - force IPv4 for libunftp compatibility
        let ftps_address_str = rustfs_utils::get_env_str(ENV_FTPS_ADDRESS, DEFAULT_FTPS_ADDRESS);
        let addr = parse_and_normalize_server_address(&ftps_address_str).await?;

        // Get FTPS configuration from environment variables
        let tls_enabled = rustfs_utils::get_env_bool(ENV_FTPS_TLS_ENABLED, true);
        let cert_dir = rustfs_utils::get_env_opt_str(ENV_FTPS_CERTS_DIR);
        let ca_file = rustfs_utils::get_env_opt_str(ENV_FTPS_CA_FILE);
        let passive_ports =
            rustfs_utils::get_env_opt_str(ENV_FTPS_PASSIVE_PORTS).or_else(|| Some(DEFAULT_FTPS_PASSIVE_PORTS.to_string())); // Default passive ports range
        let external_ip = rustfs_utils::get_env_opt_str(ENV_FTPS_EXTERNAL_IP);

        // Create FTPS configuration
        let config = FtpsConfig {
            bind_addr: addr,
            passive_ports,
            external_ip,
            ftps_required: true,
            tls_enabled,
            cert_dir,
            ca_file,
        };

        // Validate FTPS configuration
        config.validate().await?;

        // Create FTPS server with protocol storage client
        let fs = crate::storage::ecfs::FS::new();
        let storage_client = ProtocolStorageClient::new(fs);
        let server: FtpsServer<ProtocolStorageClient> = FtpsServer::new(config, storage_client).await?;
        let bind_addr = server.config().bind_addr;
        let passive_ports = server.config().passive_ports.clone();
        let tls_enabled = server.config().tls_enabled;

        // Log server configuration
        debug!(
            target: "rustfs::init",
            event = EVENT_PROTOCOL_RUNTIME_STATE,
            component = LOG_COMPONENT_INIT,
            subsystem = LOG_SUBSYSTEM_PROTOCOL,
            protocol = "ftps",
            state = "configured",
            bind_addr = %bind_addr,
            passive_ports = ?passive_ports,
            tls_enabled,
            "Protocol runtime configured"
        );

        // Start FTPS server in background task with proper shutdown support
        let (shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel(1);

        let task_handle = tokio::spawn(async move {
            if let Err(e) = server.start(shutdown_rx).await {
                error!(
                    target: "rustfs::init",
                    event = EVENT_PROTOCOL_SERVER_STATE,
                    component = LOG_COMPONENT_INIT,
                    subsystem = LOG_SUBSYSTEM_PROTOCOL,
                    protocol = "ftps",
                    state = "runtime_failed",
                    error = %e,
                    "Protocol server failed"
                );
            }
            info!(
                target: "rustfs::init",
                event = EVENT_PROTOCOL_SERVER_STATE,
                component = LOG_COMPONENT_INIT,
                subsystem = LOG_SUBSYSTEM_PROTOCOL,
                protocol = "ftps",
                state = "stopped",
                "Protocol server stopped"
            );
        });

        info!(
            target: "rustfs::init",
            event = EVENT_PROTOCOL_RUNTIME_STATE,
            component = LOG_COMPONENT_INIT,
            subsystem = LOG_SUBSYSTEM_PROTOCOL,
            protocol = "ftps",
            state = "started",
            bind_addr = %bind_addr,
            tls_enabled,
            "Protocol runtime started"
        );
        Ok(Some(ShutdownHandle::new(shutdown_tx, task_handle)))
    }
}

/// Initialize the WebDAV system
///
/// This function initializes the WebDAV server if enabled in the configuration.
/// It sets up the WebDAV server with the appropriate configuration and starts
/// the server in a background task.
#[cfg(feature = "webdav")]
#[instrument(skip_all)]
pub async fn init_webdav_system() -> Result<Option<ShutdownHandle>, Box<dyn std::error::Error + Send + Sync>> {
    {
        use crate::protocols::ProtocolStorageClient;
        use rustfs_config::{
            DEFAULT_WEBDAV_ADDRESS, ENV_WEBDAV_ADDRESS, ENV_WEBDAV_CA_FILE, ENV_WEBDAV_CERTS_DIR, ENV_WEBDAV_ENABLE,
            ENV_WEBDAV_MAX_BODY_SIZE, ENV_WEBDAV_REQUEST_TIMEOUT, ENV_WEBDAV_TLS_ENABLED,
        };
        use rustfs_protocols::{WebDavConfig, WebDavServer};

        // Check if WebDAV is enabled
        let webdav_enable = rustfs_utils::get_env_bool(ENV_WEBDAV_ENABLE, false);
        if !webdav_enable {
            debug!(
                target: "rustfs::init",
                event = EVENT_PROTOCOL_RUNTIME_STATE,
                component = LOG_COMPONENT_INIT,
                subsystem = LOG_SUBSYSTEM_PROTOCOL,
                protocol = "webdav",
                state = "disabled",
                "Protocol runtime disabled"
            );
            return Ok(None);
        }

        // Parse WebDAV address
        let webdav_address_str = rustfs_utils::get_env_str(ENV_WEBDAV_ADDRESS, DEFAULT_WEBDAV_ADDRESS);
        let addr = rustfs_utils::net::parse_and_resolve_address(&webdav_address_str)
            .map_err(|e| format!("Invalid WebDAV address '{webdav_address_str}': {e}"))?;

        // Get WebDAV configuration from environment variables
        let tls_enabled = rustfs_utils::get_env_bool(ENV_WEBDAV_TLS_ENABLED, true);
        let cert_dir = rustfs_utils::get_env_opt_str(ENV_WEBDAV_CERTS_DIR);
        let ca_file = rustfs_utils::get_env_opt_str(ENV_WEBDAV_CA_FILE);
        let max_body_size = rustfs_utils::get_env_u64(ENV_WEBDAV_MAX_BODY_SIZE, WebDavConfig::DEFAULT_MAX_BODY_SIZE);
        let request_timeout_secs =
            rustfs_utils::get_env_u64(ENV_WEBDAV_REQUEST_TIMEOUT, WebDavConfig::DEFAULT_REQUEST_TIMEOUT_SECS);

        // Create WebDAV configuration
        let config = WebDavConfig {
            bind_addr: addr,
            tls_enabled,
            cert_dir,
            ca_file,
            max_body_size,
            request_timeout_secs,
        };

        // Create WebDAV server with protocol storage client
        let fs = crate::storage::ecfs::FS::new();
        let storage_client = ProtocolStorageClient::new(fs);
        let server: WebDavServer<crate::protocols::ProtocolStorageClient> = WebDavServer::new(config, storage_client).await?;
        let bind_addr = server.config().bind_addr;
        let tls_enabled = server.config().tls_enabled;
        let max_body_size = server.config().max_body_size;
        let request_timeout_secs = server.config().request_timeout_secs;

        // Log server configuration
        debug!(
            target: "rustfs::init",
            event = EVENT_PROTOCOL_RUNTIME_STATE,
            component = LOG_COMPONENT_INIT,
            subsystem = LOG_SUBSYSTEM_PROTOCOL,
            protocol = "webdav",
            state = "configured",
            bind_addr = %bind_addr,
            tls_enabled,
            max_body_size,
            request_timeout_secs,
            "Protocol runtime configured"
        );

        // Start WebDAV server in background task with proper shutdown support
        let (shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel(1);

        let task_handle = tokio::spawn(async move {
            if let Err(e) = server.start(shutdown_rx).await {
                error!(
                    target: "rustfs::init",
                    event = EVENT_PROTOCOL_SERVER_STATE,
                    component = LOG_COMPONENT_INIT,
                    subsystem = LOG_SUBSYSTEM_PROTOCOL,
                    protocol = "webdav",
                    state = "runtime_failed",
                    error = %e,
                    "Protocol server failed"
                );
            }
            info!(
                target: "rustfs::init",
                event = EVENT_PROTOCOL_SERVER_STATE,
                component = LOG_COMPONENT_INIT,
                subsystem = LOG_SUBSYSTEM_PROTOCOL,
                protocol = "webdav",
                state = "stopped",
                "Protocol server stopped"
            );
        });

        info!(
            target: "rustfs::init",
            event = EVENT_PROTOCOL_RUNTIME_STATE,
            component = LOG_COMPONENT_INIT,
            subsystem = LOG_SUBSYSTEM_PROTOCOL,
            protocol = "webdav",
            state = "started",
            bind_addr = %bind_addr,
            tls_enabled,
            "Protocol runtime started"
        );
        Ok(Some(ShutdownHandle::new(shutdown_tx, task_handle)))
    }
}

/// Start the SFTP server when RUSTFS_SFTP_ENABLE is set. Loads host
/// keys from the configured directory, validates the SSH configuration,
/// and spawns the listener task.
#[cfg(feature = "sftp")]
#[instrument(skip_all)]
pub async fn init_sftp_system() -> Result<Option<ShutdownHandle>, Box<dyn std::error::Error + Send + Sync>> {
    {
        use crate::protocols::ProtocolStorageClient;
        use rustfs_config::{
            DEFAULT_SFTP_ADDRESS, DEFAULT_SFTP_BANNER, DEFAULT_SFTP_IDLE_TIMEOUT, DEFAULT_SFTP_PART_SIZE, DEFAULT_SFTP_READ_ONLY,
            ENV_SFTP_ADDRESS, ENV_SFTP_BACKEND_OP_TIMEOUT_SECS, ENV_SFTP_BANNER, ENV_SFTP_ENABLE, ENV_SFTP_HANDLES_PER_SESSION,
            ENV_SFTP_HOST_KEY_DIR, ENV_SFTP_IDLE_TIMEOUT, ENV_SFTP_PART_SIZE, ENV_SFTP_READ_CACHE_TOTAL_MEM_BYTES,
            ENV_SFTP_READ_CACHE_WINDOW_BYTES, ENV_SFTP_READ_ONLY,
        };
        use rustfs_protocols::{SftpConfig, SftpServer};

        let enabled = rustfs_utils::get_env_bool(ENV_SFTP_ENABLE, false);
        if !enabled {
            debug!(
                target: "rustfs::init",
                event = EVENT_PROTOCOL_RUNTIME_STATE,
                component = LOG_COMPONENT_INIT,
                subsystem = LOG_SUBSYSTEM_PROTOCOL,
                protocol = "sftp",
                state = "disabled",
                "Protocol runtime disabled"
            );
            return Ok(None);
        }

        let addr_str = rustfs_utils::get_env_str(ENV_SFTP_ADDRESS, DEFAULT_SFTP_ADDRESS);
        let addr = rustfs_utils::net::parse_and_resolve_address(&addr_str)
            .map_err(|e| format!("Invalid SFTP address '{}': {}", addr_str, e))?;

        let host_key_dir = rustfs_utils::get_env_opt_str(ENV_SFTP_HOST_KEY_DIR)
            .ok_or("RUSTFS_SFTP_HOST_KEY_DIR is required when SFTP is enabled")?;

        let idle_timeout = rustfs_utils::get_env_u64(ENV_SFTP_IDLE_TIMEOUT, DEFAULT_SFTP_IDLE_TIMEOUT);
        let part_size = rustfs_utils::get_env_u64(ENV_SFTP_PART_SIZE, DEFAULT_SFTP_PART_SIZE);
        let handles_per_session =
            SftpConfig::resolve_handles_per_session(rustfs_utils::get_env_opt_usize(ENV_SFTP_HANDLES_PER_SESSION));
        let backend_op_timeout_secs =
            SftpConfig::resolve_backend_op_timeout_secs(rustfs_utils::get_env_opt_u64(ENV_SFTP_BACKEND_OP_TIMEOUT_SECS));
        let read_cache_window_bytes =
            SftpConfig::resolve_read_cache_window_bytes(rustfs_utils::get_env_opt_u64(ENV_SFTP_READ_CACHE_WINDOW_BYTES));
        let read_cache_total_mem_bytes =
            SftpConfig::resolve_read_cache_total_mem_bytes(rustfs_utils::get_env_opt_u64(ENV_SFTP_READ_CACHE_TOTAL_MEM_BYTES));
        let read_only = rustfs_utils::get_env_bool(ENV_SFTP_READ_ONLY, DEFAULT_SFTP_READ_ONLY);
        let banner = rustfs_utils::get_env_str(ENV_SFTP_BANNER, DEFAULT_SFTP_BANNER);

        let config = SftpConfig {
            bind_addr: addr,
            host_key_dir: std::path::PathBuf::from(&host_key_dir),
            idle_timeout_secs: idle_timeout,
            part_size,
            handles_per_session,
            backend_op_timeout_secs,
            read_cache_window_bytes,
            read_cache_total_mem_bytes,
            read_only,
            banner,
        };

        config.validate().await?;

        // Load and validate host keys. Fails if zero found or any key
        // file has insecure permissions.
        let host_keys = SftpConfig::load_host_keys(&config.host_key_dir).await?;

        let fs = crate::storage::ecfs::FS::new();
        let storage_client = ProtocolStorageClient::new(fs);

        let server = SftpServer::new(config.clone(), storage_client, host_keys)?;

        debug!(
            target: "rustfs::init",
            event = EVENT_PROTOCOL_RUNTIME_STATE,
            component = LOG_COMPONENT_INIT,
            subsystem = LOG_SUBSYSTEM_PROTOCOL,
            protocol = "sftp",
            state = "configured",
            bind_addr = %config.bind_addr,
            read_only = config.read_only,
            host_key_dir = %config.host_key_dir.display(),
            "Protocol runtime configured"
        );

        // Hook into shutdown support
        let (shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel(1);

        // Start SFTP server in background task
        let task_handle = tokio::spawn(async move {
            if let Err(e) = server.start(shutdown_rx).await {
                error!(
                    target: "rustfs::init",
                    event = EVENT_PROTOCOL_SERVER_STATE,
                    component = LOG_COMPONENT_INIT,
                    subsystem = LOG_SUBSYSTEM_PROTOCOL,
                    protocol = "sftp",
                    state = "runtime_failed",
                    error = %e,
                    "Protocol server failed"
                );
            }
            info!(
                target: "rustfs::init",
                event = EVENT_PROTOCOL_SERVER_STATE,
                component = LOG_COMPONENT_INIT,
                subsystem = LOG_SUBSYSTEM_PROTOCOL,
                protocol = "sftp",
                state = "stopped",
                "Protocol server stopped"
            );
        });

        info!(
            target: "rustfs::init",
            event = EVENT_PROTOCOL_RUNTIME_STATE,
            component = LOG_COMPONENT_INIT,
            subsystem = LOG_SUBSYSTEM_PROTOCOL,
            protocol = "sftp",
            state = "started",
            bind_addr = %config.bind_addr,
            read_only = config.read_only,
            "Protocol runtime started"
        );
        Ok(Some(ShutdownHandle::new(shutdown_tx, task_handle)))
    }
}

#[cfg(test)]
mod tests {
    use super::resolve_buffer_profile_config;
    use crate::config::{BufferConfig, WorkloadProfile};
    use rustfs_config::KI_B;

    #[test]
    fn resolve_buffer_profile_config_returns_fallback_when_primary_is_invalid() {
        let invalid_primary = WorkloadProfile::Custom(BufferConfig {
            min_size: 64 * KI_B,
            max_size: 1024,
            default_unknown: 64 * KI_B,
            thresholds: vec![(1024, 64 * KI_B)],
        });

        let resolved = resolve_buffer_profile_config(invalid_primary, WorkloadProfile::GeneralPurpose)
            .expect("fallback profile should be accepted");

        assert_eq!(resolved.workload, WorkloadProfile::GeneralPurpose);
    }

    #[test]
    fn resolve_buffer_profile_config_returns_none_when_primary_and_fallback_are_invalid() {
        let invalid = WorkloadProfile::Custom(BufferConfig {
            min_size: 64 * KI_B,
            max_size: 1024,
            default_unknown: 64 * KI_B,
            thresholds: vec![(1024, 64 * KI_B)],
        });

        let resolved = resolve_buffer_profile_config(invalid.clone(), invalid);

        assert!(resolved.is_none());
    }
}
