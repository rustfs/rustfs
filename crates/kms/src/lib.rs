#![deny(clippy::unwrap_used)]
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

//! # RustFS Key Management Service (KMS)
//!
//! This crate provides a Key Management Service (KMS) abstraction for RustFS,
//! supporting multiple backend implementations including HashiCorp Vault through rusty_vault.

mod bucket_encryption;
mod bucket_encryption_manager;
mod cache;
mod cipher;
mod config;
mod error;
mod local_client;
pub mod manager;
mod monitoring;
mod object_encryption;
mod object_encryption_service;
mod parallel;
mod security;
mod types;

#[cfg(test)]
mod tests;

// Global KMS instance management
use once_cell::sync::OnceCell;
use std::sync::{Arc, RwLock};

/// Global KMS (Key Management Service) manager instance
///
/// This is initialized during application startup and provides
/// key management services throughout the application lifecycle.
/// Uses RwLock to allow dynamic reconfiguration.
static GLOBAL_KMS: OnceCell<RwLock<Option<Arc<KmsManager>>>> = OnceCell::new();

/// Global bucket encryption manager instance
///
/// This provides bucket-level encryption configuration management
/// throughout the application lifecycle.
static GLOBAL_BUCKET_ENCRYPTION_MANAGER: OnceCell<RwLock<Option<Arc<BucketEncryptionManager>>>> = OnceCell::new();

/// Global object encryption service instance
///
/// This provides object-level encryption and decryption services
/// throughout the application lifecycle.
static GLOBAL_ENCRYPTION_SERVICE: OnceCell<RwLock<Option<Arc<ObjectEncryptionService>>>> = OnceCell::new();

/// Initialize the global KMS manager
///
/// This function should be called during application startup to initialize
/// the KMS with the provided configuration.
///
/// # Arguments
///
/// * `kms_manager` - The configured KMS manager instance
///
/// # Returns
///
/// Returns `Ok(())` if initialization is successful, or an error if the global
/// KMS has already been initialized.
///
/// # Example
///
/// ```rust,ignore
/// use rustfs_kms::{KmsConfig, KmsManager};
///
/// async fn init_app() -> Result<(), Box<dyn std::error::Error>> {
///     let kms_config = KmsConfig::from_env()?;
///     let kms_manager = KmsManager::new(kms_config).await?;
///     
///     rustfs_kms::init_global_kms(Arc::new(kms_manager))?;
///     
///     Ok(())
/// }
/// ```
pub fn init_global_kms(kms_manager: Arc<KmsManager>) -> Result<()> {
    let kms_lock = GLOBAL_KMS.get_or_init(|| RwLock::new(None));
    let mut kms = kms_lock.write().map_err(|_| KmsError::InternalError {
        message: "Failed to acquire write lock".to_string(),
    })?;
    *kms = Some(kms_manager);
    Ok(())
}

/// Get a reference to the global KMS manager
///
/// Returns `None` if the KMS has not been initialized yet.
///
/// # Example
///
/// ```rust,ignore
/// use rustfs_kms::ListKeysRequest;
///
/// if let Some(kms) = rustfs_kms::get_global_kms() {
///     let keys = kms.list_keys(&ListKeysRequest::default(), None).await?;
///     println!("Found {} keys", keys.keys.len());
/// } else {
///     println!("KMS not initialized");
/// }
/// ```
pub fn get_global_kms() -> Option<Arc<KmsManager>> {
    let kms_lock = GLOBAL_KMS.get()?;
    let kms = kms_lock.read().ok()?;
    kms.clone()
}

/// Configure or reconfigure the global KMS manager
///
/// This function allows dynamic configuration of the KMS manager at runtime.
/// It can be used to set up the KMS for the first time or to reconfigure
/// it with new settings.
///
/// # Arguments
///
/// * `kms_manager` - The new KMS manager instance to use
///
/// # Returns
///
/// Returns `Ok(())` if configuration is successful, or an error if the
/// configuration fails.
///
/// # Example
///
/// ```rust,ignore
/// use rustfs_kms::{KmsConfig, KmsManager};
///
/// async fn reconfigure_kms() -> Result<(), Box<dyn std::error::Error>> {
///     let new_config = KmsConfig::from_env()?;
///     let new_manager = KmsManager::new(new_config).await?;
///     
///     rustfs_kms::configure_global_kms(Arc::new(new_manager))?;
///     
///     Ok(())
/// }
/// ```
pub fn configure_global_kms(kms_manager: Arc<KmsManager>) -> Result<()> {
    let kms_lock = GLOBAL_KMS.get_or_init(|| RwLock::new(None));
    let mut kms = kms_lock.write().map_err(|_| KmsError::InternalError {
        message: "Failed to acquire write lock".to_string(),
    })?;
    *kms = Some(kms_manager);
    Ok(())
}

/// Check if the global KMS is initialized and healthy
///
/// This performs a health check on the KMS to ensure it's ready for use.
///
/// # Returns
///
/// Returns `true` if the KMS is initialized and healthy, `false` otherwise.
///
/// # Example
///
/// ```rust,ignore
/// if rustfs_kms::is_kms_healthy().await {
///     println!("KMS is ready to use");
/// } else {
///     println!("KMS is not available");
/// }
/// ```
pub async fn is_kms_healthy() -> bool {
    match get_global_kms() {
        Some(kms) => (kms.health_check().await).is_ok(),
        None => false,
    }
}

/// Get detailed health status of the global KMS manager
///
/// This function performs an enhanced health check that includes encryption
/// capability testing and provides detailed status information.
///
/// # Returns
///
/// Returns `Some(HealthStatus)` if the KMS is initialized, `None` otherwise.
///
/// # Example
///
/// ```rust,ignore
/// if let Some(status) = rustfs_kms::get_kms_health_status().await {
///     println!("KMS Health: {}, Encryption Working: {}",
///              status.kms_healthy, status.encryption_working);
/// }
/// ```
pub async fn get_kms_health_status() -> Option<HealthStatus> {
    match get_global_kms() {
        Some(kms) => kms.health_check_with_encryption_status().await.ok(),
        None => None,
    }
}

/// Shutdown the global KMS manager
///
/// This function should be called during application shutdown to properly
/// clean up KMS resources.
///
/// # Example
///
/// ```rust,ignore
/// // During application shutdown
/// rustfs_kms::shutdown_global_kms().await;
/// ```
pub fn shutdown_global_kms() {
    if let Some(kms) = GLOBAL_KMS.get() {
        // Perform any necessary cleanup
        // The KMS manager will be dropped when the application exits
        tracing::info!("Shutting down global KMS manager");
        if let Ok(mut kms) = kms.write() {
            *kms = None;
        }
    }
}

/// Initialize the global bucket encryption manager
///
/// This function should be called during application startup to initialize
/// the bucket encryption manager.
///
/// # Arguments
///
/// * `manager` - The configured bucket encryption manager instance
///
/// # Returns
///
/// Returns `Ok(())` if initialization is successful, or an error if already initialized.
pub fn init_global_bucket_encryption_manager(manager: Arc<BucketEncryptionManager>) -> Result<()> {
    let manager_lock = GLOBAL_BUCKET_ENCRYPTION_MANAGER.get_or_init(|| RwLock::new(None));
    let mut mgr = manager_lock.write().map_err(|_| KmsError::InternalError {
        message: "Failed to acquire write lock".to_string(),
    })?;
    *mgr = Some(manager);
    Ok(())
}

/// Get a reference to the global bucket encryption manager
///
/// Returns `None` if the manager has not been initialized yet.
pub fn get_global_bucket_encryption_manager() -> Option<Arc<BucketEncryptionManager>> {
    let manager_lock = GLOBAL_BUCKET_ENCRYPTION_MANAGER.get()?;
    let mgr = manager_lock.read().ok()?;
    mgr.clone()
}

/// Initialize the global object encryption service
///
/// This function should be called during application startup to initialize
/// the object encryption service.
///
/// # Arguments
///
/// * `service` - The configured object encryption service instance
///
/// # Returns
///
/// Returns `Ok(())` if initialization is successful, or an error if already initialized.
pub fn init_global_encryption_service(service: Arc<ObjectEncryptionService>) -> Result<()> {
    let service_lock = GLOBAL_ENCRYPTION_SERVICE.get_or_init(|| RwLock::new(None));
    let mut svc = service_lock.write().map_err(|_| KmsError::InternalError {
        message: "Failed to acquire write lock".to_string(),
    })?;
    *svc = Some(service);
    Ok(())
}

/// Get a reference to the global object encryption service
///
/// Returns `None` if the service has not been initialized yet.
pub fn get_global_encryption_service() -> Option<Arc<ObjectEncryptionService>> {
    let service_lock = GLOBAL_ENCRYPTION_SERVICE.get()?;
    let svc = service_lock.read().ok()?;
    svc.clone()
}

/// Initialize all global encryption services
///
/// This convenience function initializes all encryption-related global variables
/// with their respective services.
///
/// # Arguments
///
/// * `kms_manager` - The KMS manager instance
/// * `bucket_manager` - The bucket encryption manager instance
/// * `encryption_service` - The object encryption service instance
///
/// # Returns
///
/// Returns `Ok(())` if all initializations are successful.
pub fn init_all_encryption_services(
    kms_manager: Arc<KmsManager>,
    bucket_manager: Arc<BucketEncryptionManager>,
    encryption_service: Arc<ObjectEncryptionService>,
) -> Result<()> {
    init_global_kms(kms_manager)?;
    init_global_bucket_encryption_manager(bucket_manager)?;
    init_global_encryption_service(encryption_service)?;
    Ok(())
}

/// Shutdown all global encryption services
///
/// This function should be called during application shutdown to properly
/// clean up all encryption-related resources.
pub fn shutdown_all_encryption_services() {
    shutdown_global_kms();

    if let Some(manager_lock) = GLOBAL_BUCKET_ENCRYPTION_MANAGER.get() {
        if let Ok(mut mgr) = manager_lock.write() {
            *mgr = None;
        }
    }

    if let Some(service_lock) = GLOBAL_ENCRYPTION_SERVICE.get() {
        if let Ok(mut svc) = service_lock.write() {
            *svc = None;
        }
    }
}

#[cfg(feature = "vault")]
mod vault_client;

pub use bucket_encryption::{BucketEncryptionAlgorithm, BucketEncryptionConfig};
pub use bucket_encryption_manager::BucketEncryptionManager;
pub use cache::{CacheConfig, CacheStats, CachedBucketConfig, CachedDataKey, KmsCacheManager};
pub use cipher::{AesGcmCipher, ChaCha20Poly1305Cipher, ObjectCipher, StreamingCipher};
pub use config::{BackendConfig, KmsConfig, KmsType, LocalConfig, VaultAuthMethod, VaultConfig};
pub use error::{KmsError, Result};
pub use local_client::LocalKmsClient;
pub use manager::KmsManager;
pub use monitoring::{
    AuditLogEntry, KmsMonitor, KmsOperation, MonitoringConfig, MonitoringReport, OperationMetrics, OperationStatus,
    OperationTimer,
};
pub use object_encryption::{EncryptedObjectData, EncryptionAlgorithm, ObjectEncryptionConfig};
pub use object_encryption_service::ObjectEncryptionService;
pub use parallel::{AsyncIoOptimizer, ConnectionPool, ParallelConfig, ParallelProcessor, PooledConnection};
pub use security::{SecretKey, SecretVec};

// Global KMS functions are already defined in this module and exported automatically
pub use types::{
    DataKey, DecryptRequest, DecryptionInput, EncryptRequest, EncryptResponse, EncryptedObjectMetadata, EncryptionMetadata,
    EncryptionResult, GenerateKeyRequest, HealthStatus, KeyInfo, KeyStatus, ListKeysRequest, ListKeysResponse, MasterKey,
    ObjectDataKeyRequest, ObjectEncryptionContext, ObjectMetadataRequest,
};

#[cfg(feature = "vault")]
pub use vault_client::VaultKmsClient;
