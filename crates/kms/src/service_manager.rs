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
use crate::encryption::service::ObjectEncryptionService;
use crate::error::{KmsError, Result};
use crate::manager::KmsManager;
use std::sync::{Arc, OnceLock};
use tokio::sync::RwLock;
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

/// Dynamic KMS service manager
pub struct KmsServiceManager {
    /// Current KMS manager (if running)
    manager: Arc<RwLock<Option<Arc<KmsManager>>>>,
    /// Current encryption service (if running)
    encryption_service: Arc<RwLock<Option<Arc<ObjectEncryptionService>>>>,
    /// Current configuration
    config: Arc<RwLock<Option<KmsConfig>>>,
    /// Current status
    status: Arc<RwLock<KmsServiceStatus>>,
}

impl KmsServiceManager {
    /// Create a new KMS service manager (not configured)
    pub fn new() -> Self {
        Self {
            manager: Arc::new(RwLock::new(None)),
            encryption_service: Arc::new(RwLock::new(None)),
            config: Arc::new(RwLock::new(None)),
            status: Arc::new(RwLock::new(KmsServiceStatus::NotConfigured)),
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

        match self.create_backend(&config).await {
            Ok(backend) => {
                // Create KMS manager
                let kms_manager = Arc::new(KmsManager::new(backend, config));

                // Create encryption service
                let encryption_service = Arc::new(ObjectEncryptionService::new((*kms_manager).clone()));

                // Update manager and service
                {
                    let mut manager = self.manager.write().await;
                    *manager = Some(kms_manager);
                }
                {
                    let mut service = self.encryption_service.write().await;
                    *service = Some(encryption_service);
                }

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
    pub async fn stop(&self) -> Result<()> {
        info!("Stopping KMS service");

        // Clear manager and service
        {
            let mut manager = self.manager.write().await;
            *manager = None;
        }
        {
            let mut service = self.encryption_service.write().await;
            *service = None;
        }

        // Update status (keep configuration)
        {
            let mut status = self.status.write().await;
            if !matches!(*status, KmsServiceStatus::NotConfigured) {
                *status = KmsServiceStatus::Configured;
            }
        }

        info!("KMS service stopped successfully");
        Ok(())
    }

    /// Reconfigure and restart KMS service
    pub async fn reconfigure(&self, new_config: KmsConfig) -> Result<()> {
        info!("Reconfiguring KMS service");

        // Stop current service if running
        if matches!(self.get_status().await, KmsServiceStatus::Running) {
            self.stop().await?;
        }

        // Configure with new config
        self.configure(new_config).await?;

        // Start with new configuration
        self.start().await?;

        info!("KMS service reconfigured successfully");
        Ok(())
    }

    /// Get KMS manager (if running)
    pub async fn get_manager(&self) -> Option<Arc<KmsManager>> {
        self.manager.read().await.clone()
    }

    /// Get encryption service (if running)  
    pub async fn get_encryption_service(&self) -> Option<Arc<ObjectEncryptionService>> {
        self.encryption_service.read().await.clone()
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

    /// Create backend from configuration
    async fn create_backend(&self, config: &KmsConfig) -> Result<Arc<dyn KmsBackend>> {
        match &config.backend_config {
            BackendConfig::Local(_) => {
                info!("Creating Local KMS backend");
                let backend = LocalKmsBackend::new(config.clone()).await?;
                Ok(Arc::new(backend))
            }
            BackendConfig::Vault(_) => {
                info!("Creating Vault KMS backend");
                let backend = crate::backends::vault::VaultKmsBackend::new(config.clone()).await?;
                Ok(Arc::new(backend))
            }
        }
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
