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
use std::sync::{
    Arc, OnceLock,
    atomic::{AtomicU64, Ordering},
};
use tokio::sync::{Mutex, RwLock};
use tracing::{error, info, warn};

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

    /// Configure KMS with new configuration
    pub async fn configure(&self, new_config: KmsConfig) -> Result<()> {
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

        info!("KMS configuration updated successfully");
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

        info!("Starting KMS service with backend: {:?}", config.backend);

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

                info!("KMS service started successfully");
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
        info!("Stopping KMS service");

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

        info!("KMS service stopped successfully (existing operations may continue)");
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

        info!("Reconfiguring KMS service (zero-downtime)");

        // Configure with new config
        {
            let mut config = self.config.write().await;
            *config = Some(new_config.clone());
        }

        // Create new service version without stopping old one
        // This allows existing operations to continue while new operations use new service
        match self.create_service_version(&new_config).await {
            Ok(new_service_version) => {
                // Get old version for logging (lock-free read)
                let old_version = self.current_service.load().as_ref().as_ref().map(|sv| sv.version);

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
                        "KMS service reconfigured successfully: version {} -> {} (old service will be cleaned up when operations complete)",
                        old_ver, new_service_version.version
                    );
                } else {
                    info!(
                        "KMS service reconfigured successfully: version {} (service started)",
                        new_service_version.version
                    );
                }
                Ok(())
            }
            Err(e) => {
                let err_msg = format!("Failed to reconfigure KMS: {e}");
                error!("{}", err_msg);
                let mut status = self.status.write().await;
                *status = KmsServiceStatus::Error(err_msg.clone());
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
            BackendConfig::Vault(_) => {
                info!("Creating Vault KMS backend for version {}", version);
                let backend = crate::backends::vault::VaultKmsBackend::new(config.clone()).await?;
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
