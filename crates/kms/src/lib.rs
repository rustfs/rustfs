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
//! - **Multiple Backends**: Local file storage, Vault KV2 (plain KV storage), and Vault Transit (optional)
//! - **Object Encryption**: Transparent S3-compatible object encryption
//! - **Streaming Encryption**: Memory-efficient encryption for large files
//! - **Key Management**: Full lifecycle management of encryption keys
//! - **S3 Compatibility**: SSE-S3, SSE-KMS, and SSE-C encryption modes
//!
//! ## Architecture
//!
//! The KMS follows a three-layer key hierarchy:
//! - **Master Keys**: Managed by KMS backends (Local / Vault KV2 / Vault Transit)
//! - **Data Encryption Keys (DEK)**: Generated per object, encrypted by master keys
//! - **Object Data**: Encrypted using DEKs with AES-256-GCM or ChaCha20-Poly1305
//!
//! ## Caching Discipline
//!
//! KMS may cache stable master-key metadata, but it must not cache or reuse generated
//! data encryption keys by master key id alone. A generated DEK and its encrypted
//! ciphertext can be bound to the object encryption context, such as the bucket and
//! object path. Reusing it for another object can break context validation and would
//! also violate the expected per-object DEK model for SSE-S3 and SSE-KMS.
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
//!     let config = KmsConfig::local(PathBuf::from("./kms_keys")).with_insecure_development_defaults();
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
pub mod audit;
pub mod backends;
pub mod backup;
mod cache;
pub mod config;
pub mod deletion_worker;
mod encryption;
mod error;
pub mod key_impact;
pub mod manager;
mod persisted_observability;
mod policy;
pub mod probe;
pub mod service;
pub mod service_manager;
mod time_serde;
pub mod types;

#[cfg(test)]
pub(crate) mod test_support {
    use crate::persisted_observability::UNKNOWN_FIELDS_METRIC;
    use metrics_util::MetricKind;
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};
    use serde::Deserializer;
    use serde::de::value::MapDeserializer;
    use serde::de::{self, DeserializeOwned, IntoDeserializer, Visitor};
    use std::io::{self, Write};
    use std::sync::{Arc, Mutex};

    #[derive(Clone, Default)]
    pub(crate) struct CapturedLogs {
        output: Arc<Mutex<Vec<u8>>>,
    }

    pub(crate) struct CapturedWriter(CapturedLogs);

    impl Write for CapturedWriter {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.0.output.lock().expect("log buffer lock poisoned").extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for CapturedLogs {
        type Writer = CapturedWriter;

        fn make_writer(&'a self) -> Self::Writer {
            CapturedWriter(self.clone())
        }
    }

    impl CapturedLogs {
        pub(crate) fn output(&self) -> String {
            String::from_utf8(self.output.lock().expect("log buffer lock poisoned").clone())
                .expect("captured logs should be UTF-8")
        }
    }

    pub(crate) fn unknown_field_metric(recorder: &DebuggingRecorder, record_kind: &str) -> u64 {
        recorder
            .snapshotter()
            .snapshot()
            .into_vec()
            .into_iter()
            .filter_map(|(composite, _unit, _description, value)| {
                let matches = composite.kind() == MetricKind::Counter
                    && composite.key().name() == UNKNOWN_FIELDS_METRIC
                    && composite
                        .key()
                        .labels()
                        .any(|label| label.key() == "record_kind" && label.value() == record_kind);
                match (matches, value) {
                    (true, DebugValue::Counter(count)) => Some(count),
                    _ => None,
                }
            })
            .sum()
    }

    enum IgnoredOnlyValue {
        Json(serde_json::Value),
        Unknown,
    }

    impl<'de> IntoDeserializer<'de, serde_json::Error> for IgnoredOnlyValue {
        type Deserializer = Self;

        fn into_deserializer(self) -> Self::Deserializer {
            self
        }
    }

    impl<'de> Deserializer<'de> for IgnoredOnlyValue {
        type Error = serde_json::Error;

        fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
        where
            V: Visitor<'de>,
        {
            match self {
                Self::Json(value) => value.deserialize_any(visitor),
                Self::Unknown => Err(de::Error::custom("unknown value was materialized")),
            }
        }

        fn deserialize_option<V>(self, visitor: V) -> Result<V::Value, Self::Error>
        where
            V: Visitor<'de>,
        {
            match self {
                Self::Json(value) => value.deserialize_option(visitor),
                Self::Unknown => Err(de::Error::custom("unknown value was materialized")),
            }
        }

        fn deserialize_newtype_struct<V>(self, name: &'static str, visitor: V) -> Result<V::Value, Self::Error>
        where
            V: Visitor<'de>,
        {
            match self {
                Self::Json(value) => value.deserialize_newtype_struct(name, visitor),
                Self::Unknown => Err(de::Error::custom("unknown value was materialized")),
            }
        }

        fn deserialize_enum<V>(
            self,
            name: &'static str,
            variants: &'static [&'static str],
            visitor: V,
        ) -> Result<V::Value, Self::Error>
        where
            V: Visitor<'de>,
        {
            match self {
                Self::Json(value) => value.deserialize_enum(name, variants, visitor),
                Self::Unknown => Err(de::Error::custom("unknown value was materialized")),
            }
        }

        fn deserialize_ignored_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
        where
            V: Visitor<'de>,
        {
            match self {
                Self::Json(value) => value.deserialize_ignored_any(visitor),
                Self::Unknown => visitor.visit_unit(),
            }
        }

        serde::forward_to_deserialize_any! {
            bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string
            bytes byte_buf unit unit_struct seq tuple tuple_struct map struct identifier
        }
    }

    pub(crate) fn deserialize_with_ignored_only_unknown<T>(
        record: serde_json::Value,
        unknown_field: &str,
    ) -> Result<T, serde_json::Error>
    where
        T: DeserializeOwned,
    {
        let object = record
            .as_object()
            .ok_or_else(|| de::Error::custom("test record must be an object"))?;
        let entries = object
            .iter()
            .map(|(key, value)| (key.clone(), IgnoredOnlyValue::Json(value.clone())))
            .chain([(unknown_field.to_owned(), IgnoredOnlyValue::Unknown)]);
        T::deserialize(MapDeserializer::<_, serde_json::Error>::new(entries))
    }
}

// Re-export public API
pub use api_types::{
    CacheSummary, ConfigureAwsKmsRequest, ConfigureKmsRequest, ConfigureKmsResponse, ConfigureLocalKmsRequest,
    ConfigureStaticKmsRequest, ConfigureVaultKmsRequest, ConfigureVaultTransitKmsRequest, KmsConfigSummary, KmsStatusResponse,
    StartKmsRequest, StartKmsResponse, StopKmsResponse, TagKeyRequest, TagKeyResponse, UntagKeyRequest, UntagKeyResponse,
    UpdateKeyDescriptionRequest, UpdateKeyDescriptionResponse,
};
pub use audit::{KmsAuditOperation, KmsAuditOutcome, KmsAuditRecord, KmsAuditSink, redact_encryption_context};
pub use cache::KmsCacheStats;
pub use config::*;
pub use deletion_worker::DeletionReferenceChecker;
pub use encryption::is_data_key_envelope;
// Re-exported so the object layer binds encryption context exactly the way the
// KMS backends do. A second canonicalization is how the object layer once
// serialized a HashMap directly while the Static backend already sorted keys.
pub use encryption::context_aad;
pub use error::{KmsError, KmsUnavailableError, Result};
pub use key_impact::{KeyImpactReport, KeyReference, KeyReferenceKind, ReferenceCompleteness, ReferenceCoverage, ReferenceScope};
pub use manager::KmsManager;
pub use probe::{ProbeFailureKind, ProbeResult, ProbeStatus};
pub use service::{DataKey, ObjectEncryptionService};
pub use service_manager::{
    KmsServiceManager, KmsServiceStatus, KmsStartOutcome, get_global_encryption_service, get_global_kms_service_manager,
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
    use std::sync::Arc;
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
        let config = KmsConfig::local(temp_dir.path().to_path_buf()).with_insecure_development_defaults();

        manager.configure(config).await.expect("Configuration should succeed");
        manager.start().await.expect("Start should succeed");

        // Test that encryption service is now available
        assert!(get_global_encryption_service().await.is_some());

        // Test health check
        assert!(is_encryption_service_healthy().await);

        // Test stop
        manager.stop().await.expect("Stop should succeed");
    }

    #[tokio::test]
    async fn test_versioned_service_reconfiguration() {
        // Test versioned service reconfiguration for zero-downtime
        let manager = KmsServiceManager::new();

        // Initial state: no version
        assert!(manager.get_service_version().await.is_none());

        // Start first service
        let temp_dir1 = TempDir::new().expect("Failed to create temp dir");
        let config1 = KmsConfig::local(temp_dir1.path().to_path_buf()).with_insecure_development_defaults();
        manager
            .configure(config1.clone())
            .await
            .expect("Configuration should succeed");
        manager.start().await.expect("Start should succeed");

        // Verify version 1
        let version1 = manager.get_service_version().await.expect("Service should have version");
        assert_eq!(version1, 1);

        // Get service reference (simulating ongoing operation)
        let service1 = manager.get_encryption_service().await.expect("Service should be available");

        // Reconfigure to new service (zero-downtime)
        let mut config2 = config1;
        config2.timeout = std::time::Duration::from_secs(45);
        manager.reconfigure(config2).await.expect("Reconfiguration should succeed");

        // Verify version 2
        let version2 = manager.get_service_version().await.expect("Service should have version");
        assert_eq!(version2, 2);

        // Old service reference should still be valid (Arc keeps it alive)
        // New requests should get version 2
        let service2 = manager.get_encryption_service().await.expect("Service should be available");

        // Verify they are different instances
        assert!(!Arc::ptr_eq(&service1, &service2));

        // Old service should still work (simulating long-running operation)
        // This demonstrates zero-downtime: old operations continue, new operations use new service
        assert!(service1.health_check().await.is_ok());
        assert!(service2.health_check().await.is_ok());
    }

    #[tokio::test]
    async fn test_concurrent_reconfiguration() {
        // Test that concurrent reconfiguration requests are serialized
        let manager = Arc::new(KmsServiceManager::new());

        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let base_path = temp_dir.path().to_path_buf();

        // Initial configuration
        let config1 = KmsConfig::local(base_path.clone()).with_insecure_development_defaults();
        manager.configure(config1).await.expect("Configuration should succeed");
        manager.start().await.expect("Start should succeed");

        // Spawn multiple concurrent reconfiguration requests
        let mut handles = Vec::new();
        for _i in 0..5 {
            let manager_clone = manager.clone();
            let path = base_path.clone();
            let handle = tokio::spawn(async move {
                let config = KmsConfig::local(path).with_insecure_development_defaults();
                manager_clone.reconfigure(config).await
            });
            handles.push(handle);
        }

        // Wait for all reconfigurations to complete
        let mut results = Vec::new();
        for handle in handles {
            results.push(handle.await);
        }

        // All should succeed (serialized by mutex)
        for result in results {
            assert!(result.expect("Task should complete").is_ok());
        }

        // Final version should be 6 (1 initial + 5 reconfigurations)
        let final_version = manager.get_service_version().await.expect("Service should have version");
        assert_eq!(final_version, 6);
    }
}
