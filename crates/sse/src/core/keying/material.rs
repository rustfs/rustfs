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

//! The key material an encrypted write or read runs on.

#[cfg(feature = "rio-v2")]
use crate::core::keying::object::{SEALED_KEY_IV_SIZE, SEALED_KEY_SIZE};
use rustfs_utils::http::object_encryption_keys::SSEType;
use std::collections::HashMap;

/// Encryption material returned by [`sse_encryption`] / [`sse_prepare_encryption`].
pub struct EncryptionMaterial {
    #[allow(unused)]
    pub sse_type: SSEType,
    pub server_side_encryption: String,
    pub kms_key_id: Option<String>,

    #[allow(unused)]
    pub algorithm: String,

    /// Encryption key bytes
    pub key_bytes: [u8; 32],
    /// Base nonce/IV used by rio to derive block/part nonces.
    pub base_nonce: [u8; 12],
    /// Encrypted DEK for managed SSE. Absent for SSE-C.
    pub encrypted_data_key: Option<Vec<u8>>,
    /// SSE-C key MD5 if customer-managed encryption is in use.
    pub customer_key_md5: Option<String>,
    /// Original plaintext size when it should be persisted alongside metadata.
    pub original_size: Option<i64>,
    /// Indicates whether `key_bytes` is a direct stream key or a MinIO-style object key.
    pub key_kind: EncryptionKeyKind,
    /// Original client-provided SSE-KMS context, stored in MinIO-compatible metadata.
    pub managed_kms_context: Option<HashMap<String, String>>,
    /// MinIO-compatible sealed object-key metadata for managed SSE object-key mode.
    #[cfg_attr(not(feature = "rio-v2"), allow(dead_code))]
    pub managed_sealed_key: Option<ManagedSealedKey>,
}

/// Decryption material returned by [`sse_decryption`].
pub struct DecryptionMaterial {
    #[allow(unused)]
    pub sse_type: SSEType,
    pub server_side_encryption: String,
    pub kms_key_id: Option<String>,
    #[allow(unused)]
    pub algorithm: String,
    pub customer_key_md5: Option<String>, // if use SSE-C, check key md5

    /// Decryption key bytes
    pub key_bytes: [u8; 32],
    /// Base nonce/IV used by rio to derive block/part nonces.
    pub base_nonce: [u8; 12],
    /// Indicates whether `key_bytes` is a direct stream key or a MinIO-style object key.
    pub key_kind: EncryptionKeyKind,
}

impl std::fmt::Debug for EncryptionMaterial {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EncryptionMaterial")
            .field("sse_type", &self.sse_type)
            .field("server_side_encryption", &self.server_side_encryption)
            .field("original_size", &self.original_size)
            .finish_non_exhaustive()
    }
}

impl std::fmt::Debug for DecryptionMaterial {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DecryptionMaterial")
            .field("sse_type", &self.sse_type)
            .field("server_side_encryption", &self.server_side_encryption)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EncryptionKeyKind {
    Direct,
    Object,
}

#[derive(Debug, Clone)]
pub struct ManagedSealedKey {
    #[cfg(feature = "rio-v2")]
    pub iv: [u8; SEALED_KEY_IV_SIZE],
    #[cfg(feature = "rio-v2")]
    pub sealed_key: [u8; SEALED_KEY_SIZE],
}

impl EncryptionMaterial {
    /// The KMS key id a write response may advertise.
    ///
    /// `kms_key_id` is always set for managed SSE because SSE-S3 also wraps
    /// its data key under the service default key, but that key is an
    /// internal detail of an `AES256` object: only an `aws:kms` object names
    /// a key the caller can act on, and `x-amz-server-side-encryption-aws-kms-key-id`
    /// is defined only for that scheme.
    pub fn response_kms_key_id(&self) -> Option<String> {
        matches!(self.sse_type, SSEType::SseKms)
            .then(|| self.kms_key_id.clone())
            .flatten()
    }
}
