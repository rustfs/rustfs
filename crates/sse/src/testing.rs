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

//! Fixtures for tests of SSE call sites, in this crate and in its dependents.

use crate::{KmsKeyAuthorizer, SseError, SseKmsPrincipal};
use async_trait::async_trait;
use base64_simd::STANDARD as BASE64_STANDARD;
use rustfs_policy::policy::action::KmsAction;
use rustfs_utils::http::headers::AMZ_ENCRYPTION_KMS;
use rustfs_utils::http::object_encryption_keys::{
    INTERNAL_ENCRYPTION_ALGORITHM_HEADER, INTERNAL_ENCRYPTION_IV_HEADER, INTERNAL_ENCRYPTION_KEY_HEADER,
    INTERNAL_ENCRYPTION_KEY_ID_HEADER,
};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};

/// An authorizer that answers every request the same way and records what it was asked.
pub struct RecordingKmsKeyAuthorizer {
    allowed: bool,
    calls: Mutex<Vec<(KmsAction, String)>>,
}

impl RecordingKmsKeyAuthorizer {
    pub fn new(allowed: bool) -> Arc<Self> {
        Arc::new(Self {
            allowed,
            calls: Mutex::new(Vec::new()),
        })
    }

    /// The `(action, key id)` pairs this authorizer was asked about, in order.
    pub fn calls(&self) -> Vec<(KmsAction, String)> {
        self.calls.lock().unwrap_or_else(PoisonError::into_inner).clone()
    }
}

#[async_trait]
impl KmsKeyAuthorizer for RecordingKmsKeyAuthorizer {
    async fn is_allowed(&self, _principal: &SseKmsPrincipal, action: KmsAction, key_id: &str) -> Result<bool, SseError> {
        self.calls
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push((action, key_id.to_string()));
        Ok(self.allowed)
    }
}

/// Stored metadata of an SSE-KMS object wrapped under the key `finance-key`.
pub fn sse_kms_object_metadata() -> HashMap<String, String> {
    HashMap::from([
        ("x-amz-server-side-encryption".to_string(), AMZ_ENCRYPTION_KMS.to_string()),
        ("x-amz-server-side-encryption-aws-kms-key-id".to_string(), "finance-key".to_string()),
        (INTERNAL_ENCRYPTION_KEY_ID_HEADER.to_string(), "finance-key".to_string()),
        (INTERNAL_ENCRYPTION_KEY_HEADER.to_string(), BASE64_STANDARD.encode_to_string([7u8; 48])),
        (INTERNAL_ENCRYPTION_IV_HEADER.to_string(), BASE64_STANDARD.encode_to_string([9u8; 12])),
        (INTERNAL_ENCRYPTION_ALGORITHM_HEADER.to_string(), "aws:kms".to_string()),
    ])
}

/// Build a real KMS-backed provider from the manager owned by a test fixture.
///
/// The provider resolves the manager's current encryption service on each operation.
///
/// # Errors
///
/// Returns an error when the manager has no active encryption service.
pub async fn kms_sse_dek_provider_for_test(
    manager: Arc<rustfs_kms::KmsServiceManager>,
) -> Result<Arc<dyn crate::SseDekProvider>, SseError> {
    Ok(Arc::new(
        crate::kms::kms_backed::KmsSseDekProvider::new_with_service_manager(manager).await?,
    ))
}
