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

use crate::storage::ecfs::{process_lambda_configurations, process_queue_configurations, process_topic_configurations};
use crate::{admin, config};
use rustfs_config::{DEFAULT_UPDATE_CHECK, ENV_UPDATE_CHECK};
use rustfs_ecstore::bucket::metadata_sys;
use rustfs_notify::notifier_global;
use rustfs_targets::arn::{ARN, TargetIDError};
use s3s::s3_error;
use std::env;
use std::io::Error;
use tracing::{debug, error, info, instrument, warn};

pub(crate) fn init_update_check() {
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
pub(crate) async fn add_bucket_notification_configuration(buckets: Vec<String>) {
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
                process_queue_configurations(&mut event_rules, cfg.queue_configurations.clone(), |arn_str| {
                    ARN::parse(arn_str)
                        .map(|arn| arn.target_id)
                        .map_err(|e| TargetIDError::InvalidFormat(e.to_string()))
                });
                process_topic_configurations(&mut event_rules, cfg.topic_configurations.clone(), |arn_str| {
                    ARN::parse(arn_str)
                        .map(|arn| arn.target_id)
                        .map_err(|e| TargetIDError::InvalidFormat(e.to_string()))
                });
                process_lambda_configurations(&mut event_rules, cfg.lambda_function_configurations.clone(), |arn_str| {
                    ARN::parse(arn_str)
                        .map(|arn| arn.target_id)
                        .map_err(|e| TargetIDError::InvalidFormat(e.to_string()))
                });

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
pub(crate) async fn init_kms_system(opt: &config::Opt) -> std::io::Result<()> {
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
/// * `opt` - The application configuration options
pub(crate) fn init_buffer_profile_system(opt: &config::Opt) {
    use crate::config::workload_profiles::{
        RustFSBufferConfig, WorkloadProfile, init_global_buffer_config, set_buffer_profile_enabled,
    };

    if opt.buffer_profile_disable {
        // User explicitly disabled buffer profiling - use GeneralPurpose profile in disabled mode
        info!("Buffer profiling disabled via --buffer-profile-disable, using GeneralPurpose profile");
        set_buffer_profile_enabled(false);
    } else {
        // Enabled by default: use configured workload profile
        info!("Buffer profiling enabled with profile: {}", opt.buffer_profile);

        // Parse the workload profile from configuration string
        let profile = WorkloadProfile::from_name(&opt.buffer_profile);

        // Log the selected profile for operational visibility
        info!("Active buffer profile: {:?}", profile);

        // Initialize the global buffer configuration
        init_global_buffer_config(RustFSBufferConfig::new(profile));

        // Enable buffer profiling globally
        set_buffer_profile_enabled(true);

        info!("Buffer profiling system initialized successfully");
    }
}
