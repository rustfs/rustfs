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

//! KMS service manager for dynamic configuration and runtime management

use crate::backends::{KmsBackend, local::LocalKmsBackend};
use crate::config::{BackendConfig, KmsConfig};
use crate::error::{KmsError, Result};
use crate::manager::KmsManager;
use crate::service::ObjectEncryptionService;
use arc_swap::ArcSwap;
use sha2::{Digest, Sha256};
use std::sync::{
    Arc, OnceLock,
    atomic::{AtomicU64, Ordering},
};
use subtle::ConstantTimeEq;
use tokio::sync::{Mutex, RwLock};
use tracing::{debug, error, info, warn};

const LOG_COMPONENT_KMS: &str = "kms";
const LOG_SUBSYSTEM_SERVICE: &str = "service";
const EVENT_KMS_SERVICE_STATE: &str = "kms_service_state";

fn local_master_key_fingerprint(master_key: Option<&str>) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update([u8::from(master_key.is_some())]);
    if let Some(master_key) = master_key {
        digest.update(master_key.as_bytes());
    }
    digest.finalize().into()
}

fn validate_local_transition(current: Option<&KmsConfig>, new: &KmsConfig) -> Result<()> {
    let Some(current) = current else {
        return Ok(());
    };
    let BackendConfig::Local(current_local) = &current.backend_config else {
        return Ok(());
    };
    let BackendConfig::Local(new_local) = &new.backend_config else {
        return Err(KmsError::configuration_error("Local KMS backend cannot be changed after configuration"));
    };

    if current_local.key_dir != new_local.key_dir {
        return Err(KmsError::configuration_error(
            "Local KMS key directory cannot be changed after configuration",
        ));
    }
    if current_local.file_permissions != new_local.file_permissions {
        return Err(KmsError::configuration_error(
            "Local KMS file permissions cannot be changed after configuration",
        ));
    }
    if current.allow_insecure_dev_defaults != new.allow_insecure_dev_defaults {
        return Err(KmsError::configuration_error(
            "Local KMS development mode cannot be changed after configuration",
        ));
    }

    let current_master_key = local_master_key_fingerprint(current_local.master_key.as_deref());
    let new_master_key = local_master_key_fingerprint(new_local.master_key.as_deref());
    if !bool::from(current_master_key.ct_eq(&new_master_key)) {
        return Err(KmsError::configuration_error(
            "Local KMS master key cannot be changed after configuration",
        ));
    }

    Ok(())
}

/// KMS service status
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum KmsServiceStatus {
    /// KMS is not configured
    NotConfigured,
    /// KMS is configured but not running
    Configured,
    /// KMS is running
    Running,
    /// KMS encountered an error
    Error(String),
}

/// Service version information for zero-downtime reconfiguration
#[derive(Clone)]
struct ServiceVersion {
    /// Service version number (monotonically increasing)
    version: u64,
    /// The encryption service instance
    service: Arc<ObjectEncryptionService>,
    /// The KMS manager instance
    manager: Arc<KmsManager>,
}

/// Dynamic KMS service manager with versioned services for zero-downtime reconfiguration
pub struct KmsServiceManager {
    /// Current service version (if running)
    /// Uses ArcSwap for atomic, lock-free service switching
    /// This allows instant atomic updates without blocking readers
    current_service: ArcSwap<Option<ServiceVersion>>,
    /// Current configuration
    config: Arc<RwLock<Option<KmsConfig>>>,
    /// Current status
    status: Arc<RwLock<KmsServiceStatus>>,
    /// Version counter (monotonically increasing)
    version_counter: Arc<AtomicU64>,
    /// Mutex to protect lifecycle operations (start, stop, reconfigure)
    /// This ensures only one lifecycle operation happens at a time
    lifecycle_mutex: Arc<Mutex<()>>,
}

impl KmsServiceManager {
    /// Create a new KMS service manager (not configured)
    pub fn new() -> Self {
        Self {
            current_service: ArcSwap::from_pointee(None),
            config: Arc::new(RwLock::new(None)),
            status: Arc::new(RwLock::new(KmsServiceStatus::NotConfigured)),
            version_counter: Arc::new(AtomicU64::new(0)),
            lifecycle_mutex: Arc::new(Mutex::new(())),
        }
    }

    /// Get current service status
    pub async fn get_status(&self) -> KmsServiceStatus {
        self.status.read().await.clone()
    }

    /// Get current configuration (if any)
    pub async fn get_config(&self) -> Option<KmsConfig> {
        self.config.read().await.clone()
    }

    /// Get configuration for status and management responses without static key material.
    pub async fn get_redacted_config(&self) -> Option<KmsConfig> {
        let mut config = self.config.read().await.clone()?;
        if let BackendConfig::Static(static_config) = &mut config.backend_config {
            use zeroize::Zeroize;
            static_config.secret_key.zeroize();
        }
        Some(config)
    }

    /// Configure KMS with new configuration
    pub async fn configure(&self, new_config: KmsConfig) -> Result<()> {
        let _guard = self.lifecycle_mutex.lock().await;
        new_config.validate()?;
        {
            let config = self.config.read().await;
            validate_local_transition(config.as_ref(), &new_config)?;
        }

        // Update configuration
        {
            let mut config = self.config.write().await;
            *config = Some(new_config.clone());
        }

        // Update status
        {
            let mut status = self.status.write().await;
            *status = KmsServiceStatus::Configured;
        }

        debug!(
            event = EVENT_KMS_SERVICE_STATE,
            component = LOG_COMPONENT_KMS,
            subsystem = LOG_SUBSYSTEM_SERVICE,
            state = "configured",
            "KMS service configured"
        );
        Ok(())
    }

    /// Start KMS service with current configuration
    pub async fn start(&self) -> Result<()> {
        let _guard = self.lifecycle_mutex.lock().await;
        self.start_internal().await
    }

    /// Internal start implementation (called within lifecycle mutex)
    async fn start_internal(&self) -> Result<()> {
        let config = {
            let config_guard = self.config.read().await;
            match config_guard.as_ref() {
                Some(config) => config.clone(),
                None => {
                    let err_msg = "Cannot start KMS: no configuration provided";
                    error!("{}", err_msg);
                    let mut status = self.status.write().await;
                    *status = KmsServiceStatus::Error(err_msg.to_string());
                    return Err(KmsError::configuration_error(err_msg));
                }
            }
        };

        info!(
            event = EVENT_KMS_SERVICE_STATE,
            component = LOG_COMPONENT_KMS,
            subsystem = LOG_SUBSYSTEM_SERVICE,
            backend = ?config.backend,
            state = "starting",
            "KMS service starting"
        );

        match self.create_service_version(&config).await {
            Ok(service_version) => {
                // Atomically update to new service version (lock-free, instant)
                // ArcSwap::store() is a true atomic operation using CAS
                self.current_service.store(Arc::new(Some(service_version)));

                // Update status
                {
                    let mut status = self.status.write().await;
                    *status = KmsServiceStatus::Running;
                }

                debug!(
                    event = EVENT_KMS_SERVICE_STATE,
                    component = LOG_COMPONENT_KMS,
                    subsystem = LOG_SUBSYSTEM_SERVICE,
                    state = "running",
                    "KMS service running"
                );
                Ok(())
            }
            Err(e) => {
                let err_msg = format!("Failed to create KMS backend: {e}");
                error!("{}", err_msg);
                let mut status = self.status.write().await;
                *status = KmsServiceStatus::Error(err_msg.clone());
                Err(KmsError::backend_error(&err_msg))
            }
        }
    }

    /// Stop KMS service
    ///
    /// Note: This stops accepting new operations, but existing operations using
    /// the service will continue until they complete (due to Arc reference counting).
    pub async fn stop(&self) -> Result<()> {
        let _guard = self.lifecycle_mutex.lock().await;
        self.stop_internal().await
    }

    /// Internal stop implementation (called within lifecycle mutex)
    async fn stop_internal(&self) -> Result<()> {
        debug!(
            event = EVENT_KMS_SERVICE_STATE,
            component = LOG_COMPONENT_KMS,
            subsystem = LOG_SUBSYSTEM_SERVICE,
            state = "stopping",
            "KMS service stopping"
        );

        // Atomically clear current service version (lock-free, instant)
        // Note: Existing Arc references will keep the service alive until operations complete
        self.current_service.store(Arc::new(None));

        // Update status (keep configuration)
        {
            let mut status = self.status.write().await;
            if !matches!(*status, KmsServiceStatus::NotConfigured) {
                *status = KmsServiceStatus::Configured;
            }
        }

        debug!(
            event = EVENT_KMS_SERVICE_STATE,
            component = LOG_COMPONENT_KMS,
            subsystem = LOG_SUBSYSTEM_SERVICE,
            state = "configured",
            "KMS service stopped"
        );
        Ok(())
    }

    /// Reconfigure and restart KMS service with zero-downtime
    ///
    /// This method implements versioned service switching:
    /// 1. Creates a new service version without stopping the old one
    /// 2. Atomically switches to the new version
    /// 3. Old operations continue using the old service (via Arc reference counting)
    /// 4. New operations automatically use the new service
    ///
    /// This ensures zero downtime during reconfiguration, even for long-running
    /// operations like encrypting large files.
    pub async fn reconfigure(&self, new_config: KmsConfig) -> Result<()> {
        let _guard = self.lifecycle_mutex.lock().await;

        debug!(
            event = EVENT_KMS_SERVICE_STATE,
            component = LOG_COMPONENT_KMS,
            subsystem = LOG_SUBSYSTEM_SERVICE,
            state = "reconfiguring",
            "KMS service reconfiguring"
        );
        new_config.validate()?;
        {
            let config = self.config.read().await;
            validate_local_transition(config.as_ref(), &new_config)?;
        }

        // Create new service version without stopping old one
        // This allows existing operations to continue while new operations use new service
        match self.create_service_version(&new_config).await {
            Ok(new_service_version) => {
                // Get old version for logging (lock-free read)
                let old_version = self.current_service.load().as_ref().as_ref().map(|sv| sv.version);

                {
                    let mut config = self.config.write().await;
                    *config = Some(new_config);
                }

                // Atomically switch to new service version (lock-free, instant CAS operation)
                // This is a true atomic operation - no waiting for locks, instant switch
                // Old service will be dropped when no more Arc references exist
                self.current_service.store(Arc::new(Some(new_service_version.clone())));

                // Update status
                {
                    let mut status = self.status.write().await;
                    *status = KmsServiceStatus::Running;
                }

                if let Some(old_ver) = old_version {
                    info!(
                        event = EVENT_KMS_SERVICE_STATE,
                        component = LOG_COMPONENT_KMS,
                        subsystem = LOG_SUBSYSTEM_SERVICE,
                        old_version = old_ver,
                        new_version = new_service_version.version,
                        state = "running",
                        "KMS service reconfigured"
                    );
                } else {
                    info!(
                        event = EVENT_KMS_SERVICE_STATE,
                        component = LOG_COMPONENT_KMS,
                        subsystem = LOG_SUBSYSTEM_SERVICE,
                        new_version = new_service_version.version,
                        state = "running",
                        "KMS service started from reconfigure"
                    );
                }
                Ok(())
            }
            Err(e) => {
                let err_msg = format!("Failed to reconfigure KMS: {e}");
                error!("{}", err_msg);
                Err(KmsError::backend_error(&err_msg))
            }
        }
    }

    /// Get KMS manager (if running)
    ///
    /// Returns the manager from the current service version.
    /// Uses lock-free atomic load for optimal performance.
    pub async fn get_manager(&self) -> Option<Arc<KmsManager>> {
        self.current_service.load().as_ref().as_ref().map(|sv| sv.manager.clone())
    }

    /// Get encryption service (if running)
    ///
    /// Returns the service from the current service version.
    /// Uses lock-free atomic load - no blocking, instant access.
    /// This ensures new operations always use the latest service version,
    /// while existing operations continue using their Arc references.
    pub async fn get_encryption_service(&self) -> Option<Arc<ObjectEncryptionService>> {
        self.current_service.load().as_ref().as_ref().map(|sv| sv.service.clone())
    }

    /// Get current service version number
    ///
    /// Useful for monitoring and debugging.
    /// Uses lock-free atomic load.
    pub async fn get_service_version(&self) -> Option<u64> {
        self.current_service.load().as_ref().as_ref().map(|sv| sv.version)
    }

    /// Health check for the KMS service
    pub async fn health_check(&self) -> Result<bool> {
        let manager = self.get_manager().await;
        match manager {
            Some(manager) => {
                // Perform health check on the backend
                match manager.health_check().await {
                    Ok(healthy) => {
                        if !healthy {
                            warn!("KMS backend health check failed");
                        }
                        Ok(healthy)
                    }
                    Err(e) => {
                        error!("KMS health check error: {}", e);
                        // Update status to error
                        let mut status = self.status.write().await;
                        *status = KmsServiceStatus::Error(format!("Health check failed: {e}"));
                        Err(e)
                    }
                }
            }
            None => {
                warn!("Cannot perform health check: KMS service not running");
                Ok(false)
            }
        }
    }

    /// Create a new service version from configuration
    ///
    /// This creates a new backend, manager, and service, and assigns it a new version number.
    async fn create_service_version(&self, config: &KmsConfig) -> Result<ServiceVersion> {
        config.validate()?;

        // Increment version counter
        let version = self.version_counter.fetch_add(1, Ordering::Relaxed) + 1;

        info!("Creating KMS service version {} with backend: {:?}", version, config.backend);

        // Create backend
        let backend = match &config.backend_config {
            BackendConfig::Local(_) => {
                info!("Creating Local KMS backend for version {}", version);
                let backend = LocalKmsBackend::new(config.clone()).await?;
                Arc::new(backend) as Arc<dyn KmsBackend>
            }
            BackendConfig::VaultKv2(_) => {
                info!("Creating Vault KV2 KMS backend for version {}", version);
                let backend = crate::backends::vault::VaultKmsBackend::new(config.clone()).await?;
                Arc::new(backend) as Arc<dyn KmsBackend>
            }
            BackendConfig::VaultTransit(_) => {
                info!("Creating Vault Transit KMS backend for version {}", version);
                let backend = crate::backends::vault_transit::VaultTransitKmsBackend::new(config.clone()).await?;
                Arc::new(backend) as Arc<dyn KmsBackend>
            }
            BackendConfig::Static(_) => {
                info!("Creating Static KMS backend for version {}", version);
                let backend = crate::backends::static_kms::StaticKmsBackend::new(config.clone()).await?;
                Arc::new(backend) as Arc<dyn KmsBackend>
            }
        };

        // Create KMS manager
        let kms_manager = Arc::new(KmsManager::new(backend, config.clone()));

        // Create encryption service
        let encryption_service = Arc::new(ObjectEncryptionService::new((*kms_manager).clone()));

        Ok(ServiceVersion {
            version,
            service: encryption_service,
            manager: kms_manager,
        })
    }
}

impl Default for KmsServiceManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Global KMS service manager instance
static GLOBAL_KMS_SERVICE_MANAGER: OnceLock<Arc<KmsServiceManager>> = OnceLock::new();

/// Initialize global KMS service manager
pub fn init_global_kms_service_manager() -> Arc<KmsServiceManager> {
    GLOBAL_KMS_SERVICE_MANAGER
        .get_or_init(|| Arc::new(KmsServiceManager::new()))
        .clone()
}

/// Get global KMS service manager
pub fn get_global_kms_service_manager() -> Option<Arc<KmsServiceManager>> {
    GLOBAL_KMS_SERVICE_MANAGER.get().cloned()
}

/// Get global encryption service (if KMS is running)
pub async fn get_global_encryption_service() -> Option<Arc<ObjectEncryptionService>> {
    let manager = get_global_kms_service_manager().unwrap_or_else(init_global_kms_service_manager);
    manager.get_encryption_service().await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn configure_rejects_insecure_development_defaults_before_state_update() {
        let manager = KmsServiceManager::new();

        let error = manager
            .configure(KmsConfig::default())
            .await
            .expect_err("unsafe local defaults should fail validation");

        assert!(error.to_string().contains(crate::config::ENV_KMS_ALLOW_INSECURE_DEV_DEFAULTS));
        assert_eq!(manager.get_status().await, KmsServiceStatus::NotConfigured);
        assert!(manager.get_config().await.is_none());
    }

    #[tokio::test]
    async fn redacted_config_omits_static_key_material() {
        use base64::Engine as _;

        let manager = KmsServiceManager::new();
        let encoded_key = base64::engine::general_purpose::STANDARD.encode([0x5au8; 32]);
        manager
            .configure(KmsConfig::static_kms("static-key".to_string(), encoded_key))
            .await
            .expect("configure static KMS");

        let config = manager.get_redacted_config().await.expect("redacted config");
        let BackendConfig::Static(static_config) = config.backend_config else {
            panic!("expected static config");
        };
        assert!(static_config.secret_key.is_empty());
    }

    #[tokio::test]
    async fn forbidden_local_master_key_change_preserves_running_config_and_service() {
        use crate::types::{CreateKeyRequest, KeyUsage};
        use std::collections::HashMap;
        use tempfile::TempDir;

        let key_dir = TempDir::new().expect("create local KMS directory");
        let config = |master_key: &str| {
            let mut config = KmsConfig::local(key_dir.path().to_path_buf());
            let BackendConfig::Local(local) = &mut config.backend_config else {
                panic!("local constructor must create local backend config");
            };
            local.master_key = Some(master_key.to_string());
            config.allow_insecure_dev_defaults = true;
            config
        };
        let manager = KmsServiceManager::new();
        manager
            .configure(config("working-master-key"))
            .await
            .expect("configure local KMS");
        manager.start().await.expect("start local KMS");
        manager
            .get_manager()
            .await
            .expect("running KMS manager")
            .create_key(CreateKeyRequest {
                key_name: Some("existing-key".to_string()),
                key_usage: KeyUsage::EncryptDecrypt,
                description: None,
                policy: None,
                tags: HashMap::new(),
                origin: None,
            })
            .await
            .expect("create encrypted key");
        let service_version = manager.get_service_version().await;

        let error = manager
            .reconfigure(config("wrong-master-key"))
            .await
            .expect_err("local master key change must be rejected");

        assert!(error.to_string().contains("master key cannot be changed"));
        assert_eq!(manager.get_status().await, KmsServiceStatus::Running);
        assert_eq!(manager.get_service_version().await, service_version);
        let current = manager.get_config().await.expect("working config must remain");
        let BackendConfig::Local(local) = current.backend_config else {
            panic!("working config must remain local");
        };
        assert_eq!(local.master_key.as_deref(), Some("working-master-key"));
    }

    #[tokio::test]
    async fn configure_cannot_replace_existing_local_backend() {
        use base64::Engine as _;
        use tempfile::TempDir;

        let key_dir = TempDir::new().expect("create local KMS directory");
        let mut local = KmsConfig::local(key_dir.path().to_path_buf());
        local.allow_insecure_dev_defaults = true;
        let manager = KmsServiceManager::new();
        manager.configure(local.clone()).await.expect("configure local KMS");

        let encoded_key = base64::engine::general_purpose::STANDARD.encode([0x5au8; 32]);
        let error = manager
            .configure(KmsConfig::static_kms("static-key".to_string(), encoded_key))
            .await
            .expect_err("existing local backend must be immutable");

        assert!(error.to_string().contains("backend cannot be changed"));
        let current = manager.get_config().await.expect("local config must remain");
        assert!(matches!(current.backend_config, BackendConfig::Local(_)));
    }

    #[tokio::test]
    async fn reconfigure_allows_safe_local_runtime_settings_only() {
        use tempfile::TempDir;

        let key_dir = TempDir::new().expect("create local KMS directory");
        let mut initial = KmsConfig::local(key_dir.path().to_path_buf());
        initial.allow_insecure_dev_defaults = true;
        let manager = KmsServiceManager::new();
        manager.configure(initial.clone()).await.expect("configure local KMS");
        manager.start().await.expect("start local KMS");

        let mut updated = initial;
        updated.default_key_id = Some("evaluation-key".to_string());
        updated.timeout = std::time::Duration::from_secs(45);
        updated.enable_cache = false;
        manager
            .reconfigure(updated.clone())
            .await
            .expect("update safe local settings");

        let current = manager.get_config().await.expect("updated config");
        assert_eq!(current.default_key_id, updated.default_key_id);
        assert_eq!(current.timeout, updated.timeout);
        assert!(!current.enable_cache);
    }
}
