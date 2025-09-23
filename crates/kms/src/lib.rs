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
//! This crate provides a comprehensive Key Management Service (KMS) for RustFS,
//! supporting secure key generation, storage, and object encryption capabilities.
//!
//! ## Features
//!
//! - **Multiple Backends**: Local file storage and Vault (optional)
//! - **Object Encryption**: Transparent S3-compatible object encryption
//! - **Streaming Encryption**: Memory-efficient encryption for large files
//! - **Key Management**: Full lifecycle management of encryption keys
//! - **S3 Compatibility**: SSE-S3, SSE-KMS, and SSE-C encryption modes
//!
//! ## Architecture
//!
//! The KMS follows a three-layer key hierarchy:
//! - **Master Keys**: Managed by KMS backends (Local/Vault)
//! - **Data Encryption Keys (DEK)**: Generated per object, encrypted by master keys
//! - **Object Data**: Encrypted using DEKs with AES-256-GCM or ChaCha20-Poly1305
//!
//! ## Example
//!
//! ```rust,no_run
//! use rustfs_kms::{KmsConfig, init_global_kms_service_manager};
//! use std::path::PathBuf;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // Initialize global KMS service manager
//!     let service_manager = init_global_kms_service_manager();
//!
//!     // Configure with local backend
//!     let config = KmsConfig::local(PathBuf::from("./kms_keys"));
//!     service_manager.configure(config).await?;
//!
//!     // Start the KMS service
//!     service_manager.start().await?;
//!
//!     Ok(())
//! }
//! ```

// Core modules
pub mod api_types;
pub mod backends;
mod cache;
pub mod config;
mod encryption;
mod error;
pub mod manager;
pub mod service_manager;
pub mod types;

// Re-export public API
pub use api_types::{
    CacheSummary, ConfigureKmsRequest, ConfigureKmsResponse, ConfigureLocalKmsRequest, ConfigureVaultKmsRequest,
    KmsConfigSummary, KmsStatusResponse, StartKmsRequest, StartKmsResponse, StopKmsResponse, TagKeyRequest, TagKeyResponse,
    UntagKeyRequest, UntagKeyResponse, UpdateKeyDescriptionRequest, UpdateKeyDescriptionResponse,
};
pub use config::*;
pub use encryption::ObjectEncryptionService;
pub use encryption::service::DataKey;
pub use error::{KmsError, Result};
pub use manager::KmsManager;
pub use service_manager::{
    KmsServiceManager, KmsServiceStatus, get_global_encryption_service, get_global_kms_service_manager,
    init_global_kms_service_manager,
};
pub use types::*;

// For backward compatibility - these functions now delegate to the service manager

/// Initialize global encryption service (backward compatibility)
///
/// This function is now deprecated. Use `init_global_kms_service_manager` and configure via API instead.
#[deprecated(note = "Use dynamic KMS configuration via service manager instead")]
pub async fn init_global_services(_service: ObjectEncryptionService) -> Result<()> {
    // For backward compatibility only - not recommended for new code
    Ok(())
}

/// Check if the global encryption service is initialized and healthy
pub async fn is_encryption_service_healthy() -> bool {
    match get_global_encryption_service().await {
        Some(service) => service.health_check().await.is_ok(),
        None => false,
    }
}

/// Shutdown the global encryption service (backward compatibility)
#[deprecated(note = "Use service manager shutdown instead")]
pub fn shutdown_global_services() {
    // For backward compatibility only - service manager handles shutdown now
    tracing::info!("KMS global services shutdown requested (deprecated)");
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_global_service_lifecycle() {
        // Test service manager initialization
        let manager = init_global_kms_service_manager();

        // Test initial status
        let status = manager.get_status().await;
        assert_eq!(status, KmsServiceStatus::NotConfigured);

        // Test configuration and start
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let config = KmsConfig::local(temp_dir.path().to_path_buf());

        manager.configure(config).await.expect("Configuration should succeed");
        manager.start().await.expect("Start should succeed");

        // Test that encryption service is now available
        assert!(get_global_encryption_service().await.is_some());

        // Test health check
        assert!(is_encryption_service_healthy().await);

        // Test stop
        manager.stop().await.expect("Stop should succeed");
    }
}
