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

use crate::runtime_sources::current_region;
use crate::server::ShutdownHandle;
use crate::server::runtime_sources::current_notify_interface;
use crate::storage_api::startup::bucket_metadata::contract::bucket::{BucketOperations, BucketOptions};
use crate::storage_api::startup::init::{
    get_bucket_notification_config, process_lambda_configurations, process_queue_configurations, process_topic_configurations,
};
use crate::storage_api::startup::sse::log_sse_kms_key_policy_mode;
use crate::{admin, config, startup_runtime_sources, version};
use rustfs_config::{
    DEFAULT_BUFFER_MAX_SIZE, DEFAULT_BUFFER_MIN_SIZE, DEFAULT_BUFFER_PROFILE, DEFAULT_BUFFER_UNKNOWN_SIZE, DEFAULT_UPDATE_CHECK,
    ENV_RUSTFS_BUFFER_DEFAULT_SIZE, ENV_RUSTFS_BUFFER_MAX_SIZE, ENV_RUSTFS_BUFFER_MIN_SIZE, ENV_UPDATE_CHECK, RUSTFS_REGION,
};
use rustfs_notify::NotificationError;
use rustfs_s3_types::EventName;
use rustfs_targets::arn::{ARN, TargetID, TargetIDError};
use rustfs_utils::get_env_usize;
use s3s::s3_error;
use std::env;
use std::io::Error;
use std::sync::Arc;
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

type NotificationEventRule = (Vec<EventName>, String, String, Vec<TargetID>);

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

fn notification_config_to_event_rules(
    cfg: &s3s::dto::NotificationConfiguration,
) -> Result<Vec<NotificationEventRule>, TargetIDError> {
    let mut event_rules = Vec::new();
    process_queue_configurations(&mut event_rules, cfg.queue_configurations.clone(), arn_to_target_id)?;
    process_topic_configurations(&mut event_rules, cfg.topic_configurations.clone(), arn_to_target_id)?;
    process_lambda_configurations(&mut event_rules, cfg.lambda_function_configurations.clone(), arn_to_target_id)?;
    Ok(event_rules)
}

async fn apply_bucket_notification_configuration(bucket: &str, region: &str) -> Result<bool, NotificationError> {
    let has_notification_config = get_bucket_notification_config(bucket)
        .await
        .map_err(|err| NotificationError::StorageNotAvailable(format!("load bucket notification config for {bucket}: {err}")))?;

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

            let event_rules =
                notification_config_to_event_rules(&cfg).map_err(|err| NotificationError::BucketNotification(err.to_string()))?;
            current_notify_interface()
                .add_event_specific_rules(bucket, region, &event_rules)
                .await?;
            Ok(true)
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
            current_notify_interface().clear_bucket_notification_rules(bucket).await?;
            Ok(false)
        }
    }
}

/// Add existing bucket notification configurations to the global notifier system.
/// This function retrieves notification configurations for each bucket
/// and registers the corresponding event rules with the notifier system.
///  It processes queue, topic, and lambda configurations and maps them to event rules.
///  # Arguments
/// * `buckets` - A vector of bucket names to process
#[instrument(skip_all)]
pub async fn add_bucket_notification_configuration(buckets: Vec<String>) {
    let region = notification_region();
    for bucket in buckets.iter() {
        if let Err(err) = apply_bucket_notification_configuration(bucket, region.as_str()).await {
            let err = s3_error!(InternalError, "Failed to add rules: {err}");
            error!(
                target: "rustfs::init",
                event = "notification_rules_registration_failed",
                component = LOG_COMPONENT_INIT,
                subsystem = LOG_SUBSYSTEM_NOTIFICATION,
                bucket = %bucket,
                region,
                error = ?err,
                "Failed to register bucket notification rules"
            );
        }
    }
}

pub(crate) async fn reconcile_persisted_bucket_notification_configurations(
    store: Arc<rustfs_notify::NotifyStore>,
) -> Result<usize, NotificationError> {
    let bucket_infos = store
        .list_bucket(&BucketOptions {
            no_metadata: true,
            ..Default::default()
        })
        .await
        .map_err(|err| NotificationError::StorageNotAvailable(format!("list buckets for notification reconciliation: {err}")))?;
    let region = notification_region();
    let mut configured_bucket_count = 0;

    for bucket in bucket_infos {
        if apply_bucket_notification_configuration(&bucket.name, region.as_str()).await? {
            configured_bucket_count += 1;
        }
    }

    Ok(configured_bucket_count)
}

fn notification_region() -> String {
    let global_region = current_region();
    global_region
        .as_ref()
        .filter(|r| !r.as_str().is_empty())
        .map(|r| r.to_string())
        .unwrap_or_else(|| {
            warn!(
                target: "rustfs::init",
                event = "notification_region_fallback",
                component = LOG_COMPONENT_INIT,
                subsystem = LOG_SUBSYSTEM_NOTIFICATION,
                fallback_region = RUSTFS_REGION,
                "Notification configuration falling back to default region"
            );
            RUSTFS_REGION.to_string()
        })
}

/// Build KMS configuration for local backend
fn build_local_kms_config(cfg: &config::Config) -> std::io::Result<rustfs_kms::config::KmsConfig> {
    let key_dir = cfg
        .kms_key_dir
        .as_ref()
        .ok_or_else(|| Error::other("KMS key directory is required for local backend"))?;

    let kms_config = rustfs_kms::config::KmsConfig {
        backend: rustfs_kms::config::KmsBackend::Local,
        backend_config: rustfs_kms::config::BackendConfig::Local(rustfs_kms::config::LocalConfig {
            key_dir: std::path::PathBuf::from(key_dir),
            master_key: cfg.kms_local_master_key.clone(),
            file_permissions: Some(0o600),
        }),
        allow_insecure_dev_defaults: cfg.kms_allow_insecure_dev_defaults,
        allow_immediate_deletion: rustfs_kms::config::allow_immediate_deletion_from_env(),
        default_key_id: cfg.kms_default_key_id.clone(),
        timeout: std::time::Duration::from_secs(30),
        retry_attempts: 3,
        enable_cache: true,
        cache_config: rustfs_kms::config::CacheConfig::default(),
    };
    kms_config
        .validate()
        .map_err(|e| Error::other(format!("Local KMS configuration validation failed: {e}")))?;
    Ok(kms_config)
}

/// Collect the Vault settings the command line owns.
///
/// Everything else — auth method, namespace, TLS, KV mount and metadata paths —
/// is resolved from the environment by the KMS crate, so this path and
/// [`rustfs_kms::config::KmsConfig::from_env`] cannot drift apart. The address
/// stays required here so a missing one is still named instead of silently
/// falling back to the crate's localhost default.
fn vault_cli_overrides<'a>(
    cfg: &'a config::Config,
    backend_name: &str,
) -> std::io::Result<rustfs_kms::config::VaultCliOverrides<'a>> {
    let address = cfg
        .kms_vault_address
        .as_deref()
        .ok_or_else(|| Error::other(format!("Vault address is required for {backend_name} backend")))?;

    Ok(rustfs_kms::config::VaultCliOverrides {
        address: Some(address),
        token: cfg.kms_vault_token.as_deref(),
        mount_path: cfg.kms_vault_mount_path.as_deref(),
    })
}

/// Build KMS configuration for Vault backend
fn build_vault_kms_config(cfg: &config::Config) -> std::io::Result<rustfs_kms::config::KmsConfig> {
    let backend_config = rustfs_kms::config::vault_kv2_config_from_env(vault_cli_overrides(cfg, "vault")?)
        .map_err(|e| Error::other(format!("Vault KMS configuration failed: {e}")))?;

    let kms_config = rustfs_kms::config::KmsConfig {
        backend: rustfs_kms::config::KmsBackend::VaultKv2,
        backend_config: rustfs_kms::config::BackendConfig::VaultKv2(Box::new(backend_config)),
        allow_insecure_dev_defaults: cfg.kms_allow_insecure_dev_defaults,
        allow_immediate_deletion: rustfs_kms::config::allow_immediate_deletion_from_env(),
        default_key_id: cfg.kms_default_key_id.clone(),
        timeout: std::time::Duration::from_secs(30),
        retry_attempts: 3,
        enable_cache: true,
        cache_config: rustfs_kms::config::CacheConfig::default(),
    };
    kms_config
        .validate()
        .map_err(|e| Error::other(format!("Vault KMS configuration validation failed: {e}")))?;
    Ok(kms_config)
}

/// Build KMS configuration for Vault Transit backend
fn build_vault_transit_kms_config(cfg: &config::Config) -> std::io::Result<rustfs_kms::config::KmsConfig> {
    let backend_config = rustfs_kms::config::vault_transit_config_from_env(vault_cli_overrides(cfg, "vault-transit")?)
        .map_err(|e| Error::other(format!("Vault Transit KMS configuration failed: {e}")))?;

    let kms_config = rustfs_kms::config::KmsConfig {
        backend: rustfs_kms::config::KmsBackend::VaultTransit,
        backend_config: rustfs_kms::config::BackendConfig::VaultTransit(Box::new(backend_config)),
        allow_insecure_dev_defaults: cfg.kms_allow_insecure_dev_defaults,
        allow_immediate_deletion: rustfs_kms::config::allow_immediate_deletion_from_env(),
        default_key_id: cfg.kms_default_key_id.clone(),
        timeout: std::time::Duration::from_secs(30),
        retry_attempts: 3,
        enable_cache: true,
        cache_config: rustfs_kms::config::CacheConfig::default(),
    };
    kms_config
        .validate()
        .map_err(|e| Error::other(format!("Vault Transit KMS configuration validation failed: {e}")))?;
    Ok(kms_config)
}

/// Build KMS configuration for static single-key backend
fn build_static_kms_config(cfg: &config::Config) -> std::io::Result<rustfs_kms::config::KmsConfig> {
    use rustfs_kms::config::{ENV_KMS_STATIC_SECRET_KEY, ENV_KMS_STATIC_SECRET_KEY_FILE, StaticConfig};

    // Read secret from file first, then fall back to env var
    let secret_str = if let Some(file_path) = rustfs_utils::get_env_opt_str(ENV_KMS_STATIC_SECRET_KEY_FILE) {
        std::fs::read_to_string(&file_path)
            .map_err(|e| Error::other(format!("Failed to read static KMS secret key file {file_path}: {e}")))?
    } else {
        rustfs_utils::get_env_str(ENV_KMS_STATIC_SECRET_KEY, "")
    };

    let secret_str = secret_str.trim();
    if secret_str.is_empty() {
        return Err(Error::other(format!(
            "Static KMS requires {ENV_KMS_STATIC_SECRET_KEY} or {ENV_KMS_STATIC_SECRET_KEY_FILE} to be set"
        )));
    }

    // Do not include the value in the error: a malformed value is likely the raw
    // secret key itself, and this message ends up in startup logs.
    let colon_pos = secret_str
        .find(':')
        .ok_or_else(|| Error::other("Static KMS secret key must be in format <key-name>:<base64-key>"))?;
    let key_id = secret_str[..colon_pos].to_string();
    let secret_key = secret_str[colon_pos + 1..].to_string();

    if key_id.is_empty() || secret_key.is_empty() {
        return Err(Error::other("Static KMS secret key must be in format <key-name>:<base64-key>"));
    }

    // Base64 decoding and 32-byte key length are validated by KmsConfig::validate() below
    let static_config = StaticConfig {
        key_id: key_id.clone(),
        secret_key,
    };

    let kms_config = rustfs_kms::config::KmsConfig {
        backend: rustfs_kms::config::KmsBackend::Static,
        default_key_id: cfg.kms_default_key_id.clone().or(Some(key_id)),
        backend_config: rustfs_kms::config::BackendConfig::Static(static_config),
        allow_insecure_dev_defaults: cfg.kms_allow_insecure_dev_defaults,
        allow_immediate_deletion: rustfs_kms::config::allow_immediate_deletion_from_env(),
        ..Default::default()
    };

    kms_config
        .validate()
        .map_err(|e| Error::other(format!("Static KMS configuration validation failed: {e}")))?;
    Ok(kms_config)
}

/// Build KMS configuration for the AWS KMS backend
///
/// No credential material is read here: AWS credentials are resolved by the
/// standard `aws-config` provider chain (environment, shared profile,
/// container/IMDS role), so only the two non-credential settings are taken
/// from the environment. An unresolvable region fails the backend closed when
/// the service starts.
fn build_aws_kms_config(cfg: &config::Config) -> std::io::Result<rustfs_kms::config::KmsConfig> {
    use rustfs_kms::config::{AwsKmsConfig, ENV_KMS_AWS_ENDPOINT_URL, ENV_KMS_AWS_REGION};

    let kms_config = rustfs_kms::config::KmsConfig {
        backend: rustfs_kms::config::KmsBackend::Aws,
        backend_config: rustfs_kms::config::BackendConfig::Aws(Box::new(AwsKmsConfig {
            region: rustfs_utils::get_env_opt_str(ENV_KMS_AWS_REGION),
            endpoint_url: rustfs_utils::get_env_opt_str(ENV_KMS_AWS_ENDPOINT_URL),
        })),
        allow_insecure_dev_defaults: cfg.kms_allow_insecure_dev_defaults,
        allow_immediate_deletion: rustfs_kms::config::allow_immediate_deletion_from_env(),
        // Keys are never auto-created on this backend: it refuses
        // caller-named creation because AWS assigns identifiers, so the
        // default key must already exist in AWS and be named by key id or ARN.
        default_key_id: cfg.kms_default_key_id.clone(),
        ..Default::default()
    };

    kms_config
        .validate()
        .map_err(|e| Error::other(format!("AWS KMS configuration validation failed: {e}")))?;
    Ok(kms_config)
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

    log_sse_kms_key_policy_mode();

    // A key referenced by any bucket's encryption configuration must never be
    // deleted. Register the gate before the service can start so every
    // deletion-worker spawn observes it; the gate fails closed while the
    // object store is not ready.
    service_manager
        .set_deletion_reference_checker(std::sync::Arc::new(crate::kms_deletion_gate::BucketEncryptionReferenceChecker));

    // Route KMS management records into the server's audit pipeline. Installed
    // before the service can start so every service version built afterwards
    // carries it; with no audit target configured the records are dropped and
    // KMS operations are unaffected.
    service_manager.set_audit_sink(std::sync::Arc::new(crate::admin::handlers::kms_audit::KmsAdminAuditSink));

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
            "static" => build_static_kms_config(config)?,
            "aws" | "aws-kms" | "aws_kms" => build_aws_kms_config(config)?,
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
/// Starts the auto-tuner for performance optimization if enabled via environment variable.
///
/// The auto-tuner reads `RUSTFS_AUTOTUNER_ENABLED` to decide whether to run.
/// When enabled, it spawns a background task that tunes concurrency settings
/// every 60 seconds.
pub async fn init_auto_tuner(ctx: tokio_util::sync::CancellationToken) {
    use crate::storage_api::startup::init::concurrency::get_concurrency_manager;
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
        let fs = crate::storage_api::startup::init::ecfs::FS::new();
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
        let fs = crate::storage_api::startup::init::ecfs::FS::new();
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
            ENV_WEBDAV_MAX_BODY_SIZE, ENV_WEBDAV_MAX_CONNECTIONS, ENV_WEBDAV_REQUEST_TIMEOUT, ENV_WEBDAV_TLS_ENABLED,
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
        let max_connections = rustfs_utils::get_env_usize(ENV_WEBDAV_MAX_CONNECTIONS, WebDavConfig::DEFAULT_MAX_CONNECTIONS);

        // Create WebDAV configuration
        let config = WebDavConfig {
            bind_addr: addr,
            tls_enabled,
            cert_dir,
            ca_file,
            max_body_size,
            request_timeout_secs,
            max_connections,
        };

        // Create WebDAV server with protocol storage client
        let fs = crate::storage_api::startup::init::ecfs::FS::new();
        let storage_client = ProtocolStorageClient::new(fs);
        let server: WebDavServer<crate::protocols::ProtocolStorageClient> = WebDavServer::new(config, storage_client).await?;
        let bind_addr = server.config().bind_addr;
        let tls_enabled = server.config().tls_enabled;
        let max_body_size = server.config().max_body_size;
        let request_timeout_secs = server.config().request_timeout_secs;
        let max_connections = server.config().max_connections;

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
            max_connections,
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

        let fs = crate::storage_api::startup::init::ecfs::FS::new();
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
    use super::{
        build_aws_kms_config, build_vault_kms_config, build_vault_transit_kms_config, notification_config_to_event_rules,
        resolve_buffer_profile_config,
    };
    use crate::config::{BufferConfig, WorkloadProfile};
    use rustfs_config::KI_B;
    use rustfs_s3_types::EventName;
    use s3s::dto::{
        FilterRule, FilterRuleName, NotificationConfiguration, NotificationConfigurationFilter, QueueConfiguration, S3KeyFilter,
    };

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

    #[test]
    fn notification_config_to_event_rules_preserves_target_and_filters() {
        let cfg = NotificationConfiguration {
            queue_configurations: Some(vec![QueueConfiguration {
                events: vec!["s3:ObjectCreated:Put".to_string().into()],
                queue_arn: "arn:rustfs:sqs:us-east-1:rustfs_to_activemq:mqtt".to_string(),
                filter: Some(NotificationConfigurationFilter {
                    key: Some(S3KeyFilter {
                        filter_rules: Some(vec![
                            FilterRule {
                                name: Some(FilterRuleName::from_static(FilterRuleName::PREFIX)),
                                value: Some("uploads/".to_string()),
                            },
                            FilterRule {
                                name: Some(FilterRuleName::from_static(FilterRuleName::SUFFIX)),
                                value: Some(".json".to_string()),
                            },
                        ]),
                    }),
                }),
                id: Some("primary".to_string()),
            }]),
            topic_configurations: None,
            lambda_function_configurations: None,
            event_bridge_configuration: None,
        };

        let rules = notification_config_to_event_rules(&cfg).expect("valid notification config should map to event rules");

        assert_eq!(rules.len(), 1);
        assert_eq!(rules[0].0, vec![EventName::ObjectCreatedPut]);
        assert_eq!(rules[0].1, "uploads/");
        assert_eq!(rules[0].2, ".json");
        assert_eq!(rules[0].3.len(), 1);
        assert_eq!(rules[0].3[0].id, "rustfs_to_activemq");
        assert_eq!(rules[0].3[0].name, "mqtt");
    }

    #[test]
    fn notification_config_to_event_rules_rejects_invalid_arn() {
        let cfg = NotificationConfiguration {
            queue_configurations: Some(vec![QueueConfiguration {
                events: vec!["s3:ObjectCreated:Put".to_string().into()],
                queue_arn: "arn:aws:sqs:us-east-1:rustfs_to_activemq:mqtt".to_string(),
                filter: None,
                id: None,
            }]),
            topic_configurations: None,
            lambda_function_configurations: None,
            event_bridge_configuration: None,
        };

        let err = notification_config_to_event_rules(&cfg).expect_err("invalid ARN partition must fail");

        assert!(err.to_string().contains("Invalid ARN"), "unexpected error: {err}");
    }

    fn vault_kms_test_config(backend: &str) -> crate::config::Config {
        let mut config = crate::config::Config::new("127.0.0.1:9000", vec!["/tmp/rustfs-vault-kms".to_string()]);
        config.kms_enable = true;
        config.kms_backend = backend.to_string();
        config.kms_vault_address = Some("https://vault.example.com:8200".to_string());
        config
    }

    /// The Vault auth method and the settings the CLI has no flag for come from
    /// the environment, so startup and `KmsConfig::from_env` cannot disagree.
    /// Regression: startup used to hardcode token auth and require a token,
    /// which made every non-token method unreachable through `RUSTFS_KMS_ENABLE`.
    #[test]
    fn build_vault_transit_kms_config_resolves_auth_and_mounts_from_env() {
        let config = temp_env::with_vars(
            [
                ("RUSTFS_KMS_VAULT_TOKEN", None),
                ("RUSTFS_KMS_VAULT_TOKEN_FILE", None),
                ("RUSTFS_KMS_VAULT_KUBERNETES_ROLE", None),
                ("RUSTFS_KMS_VAULT_APPROLE_ROLE_ID", Some("env-role-id")),
                ("RUSTFS_KMS_VAULT_APPROLE_SECRET_ID", Some("env-secret-id")),
                ("RUSTFS_KMS_VAULT_APPROLE_SECRET_ID_FILE", None),
                ("RUSTFS_KMS_VAULT_NAMESPACE", Some("team-a")),
                ("RUSTFS_KMS_VAULT_TRANSIT_METADATA_KV_MOUNT", Some("rustfs-kv")),
            ],
            || {
                build_vault_transit_kms_config(&vault_kms_test_config("vault-transit"))
                    .expect("vault transit KMS configuration should build")
            },
        );

        let vault = config.vault_transit_config().expect("vault transit backend config");
        let rustfs_kms::config::VaultAuthMethod::AppRole { role_id, secret_id, .. } = &vault.auth_method else {
            panic!("approle in the environment must select AppRole auth, got {:?}", vault.auth_method);
        };
        assert_eq!(role_id, "env-role-id");
        assert_eq!(secret_id, "env-secret-id");
        assert_eq!(vault.namespace.as_deref(), Some("team-a"));
        assert_eq!(vault.metadata_kv_mount, "rustfs-kv");
    }

    /// Kubernetes auth needs no credential in the environment at all: the role
    /// selects it and the pod's projected ServiceAccount token supplies the rest.
    #[test]
    fn build_vault_transit_kms_config_selects_kubernetes_auth() {
        let config = temp_env::with_vars(
            [
                ("RUSTFS_KMS_VAULT_TOKEN", None),
                ("RUSTFS_KMS_VAULT_TOKEN_FILE", None),
                ("RUSTFS_KMS_VAULT_APPROLE_ROLE_ID", None),
                ("RUSTFS_KMS_VAULT_KUBERNETES_ROLE", Some("rustfs")),
                ("RUSTFS_KMS_VAULT_KUBERNETES_MOUNT", None),
                ("RUSTFS_KMS_VAULT_KUBERNETES_JWT_PATH", None),
            ],
            || {
                build_vault_transit_kms_config(&vault_kms_test_config("vault-transit"))
                    .expect("vault transit KMS configuration should build")
            },
        );

        let vault = config.vault_transit_config().expect("vault transit backend config");
        let rustfs_kms::config::VaultAuthMethod::Kubernetes {
            role, mount, jwt_path, ..
        } = &vault.auth_method
        else {
            panic!(
                "a kubernetes role in the environment must select Kubernetes auth, got {:?}",
                vault.auth_method
            );
        };
        assert_eq!(role, "rustfs");
        assert_eq!(mount, rustfs_kms::config::DEFAULT_VAULT_KUBERNETES_MOUNT);
        assert_eq!(jwt_path, std::path::Path::new(rustfs_kms::config::DEFAULT_VAULT_KUBERNETES_JWT_PATH));
    }

    /// Two credential sources leave the effective identity ambiguous, so
    /// startup refuses rather than picking one.
    #[test]
    fn build_vault_kms_config_refuses_two_auth_methods() {
        temp_env::with_vars(
            [
                ("RUSTFS_KMS_VAULT_TOKEN", None),
                ("RUSTFS_KMS_VAULT_TOKEN_FILE", Some("/run/vault-agent/token")),
                ("RUSTFS_KMS_VAULT_APPROLE_ROLE_ID", None),
                ("RUSTFS_KMS_VAULT_KUBERNETES_ROLE", Some("rustfs")),
            ],
            || {
                let error = build_vault_kms_config(&vault_kms_test_config("vault"))
                    .expect_err("two Vault auth methods must not start the server");
                assert!(error.to_string().contains("exactly one"), "unexpected error: {error}");
            },
        );
    }

    /// The KV2 backend has its own builder, so the key-location settings have
    /// to be proven separately from the Transit one: pointing at the wrong KV
    /// mount or prefix makes existing keys look absent.
    #[test]
    fn build_vault_kms_config_resolves_kv_mount_and_prefix_from_env() {
        let config = temp_env::with_vars(
            [
                ("RUSTFS_KMS_VAULT_TOKEN", Some("a-real-token")),
                ("RUSTFS_KMS_VAULT_TOKEN_FILE", None),
                ("RUSTFS_KMS_VAULT_APPROLE_ROLE_ID", None),
                ("RUSTFS_KMS_VAULT_KUBERNETES_ROLE", None),
                ("RUSTFS_KMS_VAULT_KV_MOUNT", Some("rustfs-kv")),
                ("RUSTFS_KMS_VAULT_KEY_PREFIX", Some("tenant/keys")),
            ],
            || build_vault_kms_config(&vault_kms_test_config("vault")).expect("vault KV2 KMS configuration should build"),
        );

        let vault = config.vault_config().expect("vault kv2 backend config");
        assert_eq!(vault.kv_mount, "rustfs-kv");
        assert_eq!(vault.key_path_prefix, "tenant/keys");
    }

    /// Skipping TLS verification was silently dropped on this path before, so
    /// an operator who asked for it still got a verified connection. Now that it
    /// is honoured it must fail closed without the development opt-in, rather
    /// than quietly downgrading the Vault connection.
    #[test]
    fn build_vault_transit_kms_config_refuses_skip_tls_verify_without_opt_in() {
        let vars = [
            ("RUSTFS_KMS_VAULT_TOKEN", Some("a-real-token")),
            ("RUSTFS_KMS_VAULT_TOKEN_FILE", None),
            ("RUSTFS_KMS_VAULT_APPROLE_ROLE_ID", None),
            ("RUSTFS_KMS_VAULT_KUBERNETES_ROLE", None),
            ("RUSTFS_KMS_VAULT_SKIP_TLS_VERIFY", Some("true")),
        ];

        temp_env::with_vars(vars, || {
            let error = build_vault_transit_kms_config(&vault_kms_test_config("vault-transit"))
                .expect_err("skipping TLS verification must not start the server");
            assert!(error.to_string().contains("TLS"), "unexpected error: {error}");
        });

        temp_env::with_vars(vars, || {
            let mut cfg = vault_kms_test_config("vault-transit");
            cfg.kms_allow_insecure_dev_defaults = true;
            let config = build_vault_transit_kms_config(&cfg).expect("the development opt-in should accept skip-verify");
            let vault = config.vault_transit_config().expect("vault transit backend config");
            assert!(vault.tls.as_ref().is_some_and(|tls| tls.skip_verify));
        });
    }

    fn aws_kms_test_config() -> crate::config::Config {
        let mut config = crate::config::Config::new("127.0.0.1:9000", vec!["/tmp/rustfs-aws-kms".to_string()]);
        config.kms_enable = true;
        config.kms_backend = "aws".to_string();
        config.kms_default_key_id = Some("arn:aws:kms:us-east-1:111122223333:key/1234abcd".to_string());
        config
    }

    /// Startup takes only the two non-credential AWS settings from the
    /// environment; credentials stay with the `aws-config` provider chain.
    #[test]
    fn build_aws_kms_config_reads_only_non_credential_settings() {
        let config = temp_env::with_vars(
            [
                ("RUSTFS_KMS_AWS_REGION", Some("eu-central-1")),
                ("RUSTFS_KMS_AWS_ENDPOINT_URL", None),
            ],
            || build_aws_kms_config(&aws_kms_test_config()).expect("aws KMS configuration should build"),
        );

        assert_eq!(config.backend, rustfs_kms::config::KmsBackend::Aws);
        let aws = config.aws_kms_config().expect("aws backend config");
        assert_eq!(aws.region.as_deref(), Some("eu-central-1"));
        assert_eq!(aws.endpoint_url, None);
        assert_eq!(config.default_key_id.as_deref(), Some("arn:aws:kms:us-east-1:111122223333:key/1234abcd"));
    }

    /// A plaintext endpoint override exposes every KMS request, plaintext data
    /// keys included, so startup refuses it without the development opt-in.
    #[test]
    fn build_aws_kms_config_refuses_a_plaintext_endpoint_without_opt_in() {
        let vars = [
            ("RUSTFS_KMS_AWS_REGION", Some("us-east-1")),
            ("RUSTFS_KMS_AWS_ENDPOINT_URL", Some("http://localhost:4566")),
        ];

        temp_env::with_vars(vars, || {
            let error =
                build_aws_kms_config(&aws_kms_test_config()).expect_err("a plaintext AWS endpoint must not start the server");
            assert!(error.to_string().contains("https"), "unexpected error: {error}");
        });

        temp_env::with_vars(vars, || {
            let mut config = aws_kms_test_config();
            config.kms_allow_insecure_dev_defaults = true;
            let config = build_aws_kms_config(&config).expect("the development opt-in should accept a plaintext endpoint");
            assert_eq!(
                config.aws_kms_config().expect("aws backend config").endpoint_url.as_deref(),
                Some("http://localhost:4566")
            );
        });
    }
}
