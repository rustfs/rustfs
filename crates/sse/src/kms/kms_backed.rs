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

//! The provider that wraps and unwraps data keys through the KMS service.

use crate::error::{SseError, kms_operation_error};
use crate::host::SseHost;
use crate::kms::provider::SseDekProvider;
use async_trait::async_trait;
use rustfs_kms::types::ObjectEncryptionContext;
use rustfs_kms::{DataKey, KmsUnavailableError};
use rustfs_s3_types::S3ErrorCode;
use std::sync::Arc;

/// Production KMS-backed DEK provider
/// Resolves the latest ObjectEncryptionService on each call.
pub(crate) struct KmsSseDekProvider<'a> {
    service_source: KmsServiceSource<'a>,
}

/// Where a [`KmsSseDekProvider`] finds the service that is serving right now.
enum KmsServiceSource<'a> {
    Host(&'a dyn SseHost),
    #[cfg(any(test, feature = "test-util"))]
    Manager(Arc<rustfs_kms::KmsServiceManager>),
}

/// Classification for a failed data-key unwrap.
///
/// An AEAD failure stays `500` (the envelope is an integrity fault, not a
/// request a retry or a different header can fix), but the generic internal
/// error text hides the one diagnosis an operator needs: the configured
/// backend holds different key material under this key id than the one that
/// wrapped the object, typically after re-creating a key of the same name or
/// switching backends.
pub(crate) fn kms_unwrap_error(error: rustfs_kms::KmsError) -> SseError {
    let unwrap_rejected = matches!(error, rustfs_kms::KmsError::CryptographicError { .. });
    let mut api_error = kms_operation_error(error);
    if unwrap_rejected && api_error.code == S3ErrorCode::InternalError {
        api_error.message = "The object's data key envelope could not be unwrapped by the configured KMS backend: the \
                             key material under this key id differs from the one that wrapped it, or the envelope is damaged"
            .to_string();
    }
    api_error
}

impl<'a> KmsSseDekProvider<'a> {
    /// Create a new KMS-backed provider
    pub async fn new(host: &'a dyn SseHost) -> Result<Self, SseError> {
        let provider = Self {
            service_source: KmsServiceSource::Host(host),
        };
        provider
            .current_service()
            .await
            .ok_or_else(|| SseError::internal(KmsUnavailableError))?;
        Ok(provider)
    }

    #[cfg(any(test, feature = "test-util"))]
    pub(crate) async fn new_with_service_manager(service_manager: Arc<rustfs_kms::KmsServiceManager>) -> Result<Self, SseError> {
        let provider = Self {
            service_source: KmsServiceSource::Manager(service_manager),
        };
        provider
            .current_service()
            .await
            .ok_or_else(|| SseError::internal(KmsUnavailableError))?;
        Ok(provider)
    }

    async fn current_service(&self) -> Option<Arc<rustfs_kms::service::ObjectEncryptionService>> {
        match &self.service_source {
            KmsServiceSource::Host(host) => host.encryption_service().await,
            #[cfg(any(test, feature = "test-util"))]
            KmsServiceSource::Manager(service_manager) => service_manager.get_encryption_service().await,
        }
    }
}

#[async_trait]
impl SseDekProvider for KmsSseDekProvider<'_> {
    async fn generate_sse_dek(
        &self,
        context: &ObjectEncryptionContext,
        kms_key_id: &str,
    ) -> Result<(DataKey, Vec<u8>), SseError> {
        let kms_key_option = Some(kms_key_id.to_string());
        let service = self
            .current_service()
            .await
            .ok_or_else(|| SseError::internal(KmsUnavailableError))?;
        let (data_key, encrypted_data_key) = service
            .create_data_key(&kms_key_option, context)
            .await
            .map_err(kms_operation_error)?;

        Ok((data_key, encrypted_data_key))
    }

    async fn reads_bare_data_key_ciphertext(&self) -> bool {
        self.current_service()
            .await
            .is_some_and(|service| service.reads_bare_data_key_ciphertext())
    }

    async fn rewrap_sse_dek(
        &self,
        encrypted_dek: &[u8],
        context: &ObjectEncryptionContext,
    ) -> Result<rustfs_kms::types::RewrapDataKeyResponse, SseError> {
        let service = self
            .current_service()
            .await
            .ok_or_else(|| SseError::internal(KmsUnavailableError))?;
        service
            .rewrap_data_key(encrypted_dek, context)
            .await
            .map_err(kms_operation_error)
    }

    async fn decrypt_sse_dek(
        &self,
        encrypted_dek: &[u8],
        _kms_key_id: &str,
        context: &ObjectEncryptionContext,
    ) -> Result<[u8; 32], SseError> {
        let service = self
            .current_service()
            .await
            .ok_or_else(|| SseError::internal(KmsUnavailableError))?;
        let data_key = service
            .decrypt_data_key(encrypted_dek, context)
            .await
            .map_err(kms_unwrap_error)?;

        Ok(data_key.plaintext_key)
    }

    #[cfg(feature = "rio-v2")]
    async fn decrypt_legacy_sse_dek(
        &self,
        encrypted_dek: &[u8],
        _kms_key_id: &str,
        _context: &ObjectEncryptionContext,
    ) -> Result<[u8; 32], SseError> {
        let service = self
            .current_service()
            .await
            .ok_or_else(|| SseError::internal(KmsUnavailableError))?;
        let data_key = service
            .decrypt_legacy_data_key(encrypted_dek)
            .await
            .map_err(kms_unwrap_error)?;

        Ok(data_key.plaintext_key)
    }
}
