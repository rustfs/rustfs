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

//! The provider that wraps data keys with a node-local master key when no KMS is configured.

use crate::error::SseError;
#[cfg(feature = "rio-v2")]
use crate::kms::minio::{decrypt_minio_kms_data_key, minio_kms_associated_data};
use crate::kms::provider::SseDekProvider;
use aes_gcm::aead::{Aead, KeyInit};
use aes_gcm::{Aes256Gcm, Key, Nonce};
use async_trait::async_trait;
use base64_simd::STANDARD as BASE64_STANDARD;
use rand::Rng;
use rustfs_kms::DataKey;
use rustfs_kms::types::ObjectEncryptionContext;
use rustfs_s3_types::S3ErrorCode;
use rustfs_utils::get_env_opt_str;
use serde::{Deserialize, Serialize};

/// Local SSE DEK provider for deployments without a KMS.
///
/// Uses `RUSTFS_SSE_S3_MASTER_KEY` (base64-encoded 32-byte key) to wrap
/// data-encryption keys with AES-256-GCM.  This is the production fallback
/// when no KMS service is configured.
///
/// # Environment Variable
///
/// ```text
/// RUSTFS_SSE_S3_MASTER_KEY=<base64_encoded_32_byte_key>
/// ```
pub struct LocalSseDekProvider {
    master_key: [u8; 32],
}

pub(crate) const LOCAL_SSE_DEK_FORMAT_VERSION: u8 = 1;

/// Environment variable that carries the local SSE-S3 master key.
pub(crate) const ENV_SSE_S3_MASTER_KEY: &str = "RUSTFS_SSE_S3_MASTER_KEY";

/// The configured local master key, still encoded; `None` when unset or blank.
fn local_master_key_env_value() -> Option<String> {
    get_env_opt_str(ENV_SSE_S3_MASTER_KEY).filter(|value| !value.trim().is_empty())
}

/// Whether the operator configured a local SSE-S3 master key at all.
///
/// Says nothing about whether the value is usable; loading it decides that.
pub(crate) fn local_master_key_configured() -> bool {
    local_master_key_env_value().is_some()
}

#[cfg(feature = "rio-v2")]
/// Returns true when a managed-SSE data key is one RustFS itself wrote.
///
/// Both RustFS envelope shapes are strict JSON — the KMS envelope
/// ([`rustfs_kms::is_data_key_envelope`]) and the local provider's
/// [`LocalSseDekEnvelope`], whose `deny_unknown_fields` keeps it from accepting
/// anything else. Recognition is deliberately positive: an unrecognized payload
/// is left to MinIO's decoder rather than guessed at, and neither decoder is
/// ever handed the other's format.
pub(crate) fn is_rustfs_managed_data_key(encrypted_dek: &[u8]) -> bool {
    if rustfs_kms::is_data_key_envelope(encrypted_dek) {
        return true;
    }
    std::str::from_utf8(encrypted_dek)
        .ok()
        .is_some_and(|text| serde_json::from_str::<LocalSseDekEnvelope<'_>>(text).is_ok())
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct LocalSseDekEnvelope<'a> {
    pub(crate) version: u8,
    pub(crate) nonce: &'a str,
    ciphertext: &'a str,
}

/// Whether [`LocalSseDekProvider`] could have written these bytes.
///
/// Covers both shapes [`LocalSseDekProvider::decrypt_dek`] accepts — anything
/// presented as a JSON object, so a damaged local envelope keeps its own
/// diagnosis, and the deprecated `nonce:ciphertext` base64 pair. Everything
/// else is bytes the local provider can only reject.
pub(crate) fn is_local_sse_dek(encrypted_dek: &[u8]) -> bool {
    let Ok(text) = std::str::from_utf8(encrypted_dek) else {
        return false;
    };
    if text.trim_start().starts_with('{') {
        return true;
    }
    let is_base64 = |part: &str| BASE64_STANDARD.decode_to_vec(part).is_ok();
    text.split_once(':')
        .is_some_and(|(nonce, ciphertext)| is_base64(nonce) && is_base64(ciphertext))
}

/// Test-only alias so existing test code that references `TestSseDekProvider`
/// continues to compile without changes.
#[cfg(any(test, feature = "test-util"))]
#[allow(non_camel_case_types)]
pub type TestSseDekProvider = LocalSseDekProvider;

/// Parse the base64-encoded 32-byte master key from `__RUSTFS_SSE_SIMPLE_CMK`.
///
/// Returns an error (never crashes) for a missing, non-base64, wrong-length, or
/// all-zero key so callers on the request path can fail the request instead of
/// taking the whole server down (backlog#806).
#[cfg(any(test, feature = "test-util"))]
pub(crate) fn parse_simple_sse_cmk(cmk_value: &str) -> Result<[u8; 32], SseError> {
    let trimmed = cmk_value.trim();
    if trimmed.is_empty() {
        return Err(SseError::internal(
            "SSE simple mode requires __RUSTFS_SSE_SIMPLE_CMK to be set to a base64-encoded 32-byte key",
        ));
    }
    let decoded = BASE64_STANDARD
        .decode_to_vec(trimmed)
        .map_err(|e| SseError::internal(format!("__RUSTFS_SSE_SIMPLE_CMK must be valid base64: {e}")))?;
    let master_key: [u8; 32] = decoded.try_into().map_err(|v: Vec<u8>| {
        SseError::internal(format!("__RUSTFS_SSE_SIMPLE_CMK must decode to exactly 32 bytes, got {} bytes", v.len()))
    })?;
    if master_key == [0u8; 32] {
        return Err(SseError::internal("__RUSTFS_SSE_SIMPLE_CMK must not be an all-zero key"));
    }
    Ok(master_key)
}

impl LocalSseDekProvider {
    /// Create a LocalSseDekProvider with a predefined key (for testing)
    #[cfg(any(test, feature = "test-util"))]
    pub fn new_with_key(master_key: [u8; 32]) -> Self {
        Self { master_key }
    }

    /// Create a TestSseDekProvider from `__RUSTFS_SSE_SIMPLE_CMK` (test-only).
    #[cfg(any(test, feature = "test-util"))]
    pub fn new() -> Result<Self, SseError> {
        let cmk_value = std::env::var("__RUSTFS_SSE_SIMPLE_CMK").unwrap_or_default();
        // A missing/invalid key must surface as a request error, never crash the
        // whole server: `LocalSseDekProvider::new` is reached from the SSE request
        // path (get_sse_dek_provider), so `process::exit(1)` here turned a bad
        // `__RUSTFS_SSE_SIMPLE_CMK` into a process crash-loop DoS (backlog#806).
        let master_key = parse_simple_sse_cmk(&cmk_value)?;
        tracing::info!("Successfully loaded SSE master key (32 bytes) from __RUSTFS_SSE_SIMPLE_CMK");
        Ok(Self { master_key })
    }

    /// Create a local SSE DEK provider for SSE-S3 when KMS is not configured.
    /// Requires RUSTFS_SSE_S3_MASTER_KEY to be a valid base64-encoded 32-byte key.
    ///
    /// The failures here are server configuration problems, not internal
    /// faults: surface them as `InvalidRequest` (HTTP 400) so a managed-SSE
    /// request against an unconfigured server does not report 500 (rustfs#4844).
    pub fn new_from_env() -> Result<Self, SseError> {
        fn sse_not_configured(message: impl Into<String>) -> SseError {
            SseError {
                code: S3ErrorCode::InvalidRequest,
                message: message.into(),
                source: None,
            }
        }

        let Some(raw_value) = local_master_key_env_value() else {
            return Err(sse_not_configured(
                "SSE-S3 requires RUSTFS_SSE_S3_MASTER_KEY to be set to a base64-encoded 32-byte key when KMS is not configured",
            ));
        };

        let decoded = BASE64_STANDARD.decode_to_vec(raw_value.trim()).map_err(|err| {
            sse_not_configured(format!(
                "RUSTFS_SSE_S3_MASTER_KEY must be valid base64 for SSE-S3 when KMS is not configured: {err}"
            ))
        })?;
        let master_key: [u8; 32] = decoded.try_into().map_err(|_| {
            sse_not_configured("RUSTFS_SSE_S3_MASTER_KEY must decode to exactly 32 bytes for SSE-S3 when KMS is not configured")
        })?;

        Ok(Self { master_key })
    }

    // Simple encryption of DEK
    pub fn encrypt_dek(dek: [u8; 32], cmk_value: [u8; 32]) -> Result<String, SseError> {
        // Use AES-256-GCM to encrypt DEK
        let key = Key::<Aes256Gcm>::from(cmk_value);

        let cipher = Aes256Gcm::new(&key);
        let mut nonce_bytes = [0u8; 12];
        rand::rng().fill_bytes(&mut nonce_bytes);
        let nonce = Nonce::from(nonce_bytes);
        let ciphertext = cipher
            .encrypt(&nonce, dek.as_slice())
            .map_err(|_| SseError::internal("Failed to encrypt DEK"))?;

        let nonce = BASE64_STANDARD.encode_to_string(nonce);
        let ciphertext = BASE64_STANDARD.encode_to_string(ciphertext);
        serde_json::to_string(&LocalSseDekEnvelope {
            version: LOCAL_SSE_DEK_FORMAT_VERSION,
            nonce: &nonce,
            ciphertext: &ciphertext,
        })
        .map_err(|e| SseError::internal(format!("Failed to serialize encrypted DEK: {e}")))
    }

    // Simple decryption of DEK
    pub fn decrypt_dek(encrypted_dek: &str, cmk_value: [u8; 32]) -> Result<[u8; 32], SseError> {
        let envelope = serde_json::from_str::<LocalSseDekEnvelope<'_>>(encrypted_dek);
        let (nonce, ciphertext) = match envelope {
            Ok(envelope) => {
                if envelope.version != LOCAL_SSE_DEK_FORMAT_VERSION {
                    return Err(SseError::internal(format!(
                        "Unsupported encrypted DEK format version: {}",
                        envelope.version
                    )));
                }
                (envelope.nonce, envelope.ciphertext)
            }
            Err(json_error) if encrypted_dek.trim_start().starts_with('{') => {
                return Err(SseError::internal(format!("Invalid encrypted DEK JSON format: {json_error}")));
            }
            Err(_) => {
                // DEPRECATED: read-only compatibility for persisted colon-delimited DEKs.
                // RUSTFS_COMPAT_TODO(sse-local-dek-json-v1): Remove after all supported upgrades have rewritten legacy DEKs.
                let Some((nonce, ciphertext)) = encrypted_dek.split_once(':') else {
                    return Err(SseError::internal("Invalid encrypted DEK format"));
                };
                if ciphertext.contains(':') {
                    return Err(SseError::internal("Invalid encrypted DEK format"));
                }
                (nonce, ciphertext)
            }
        };
        let nonce_vec = BASE64_STANDARD
            .decode_to_vec(nonce)
            .map_err(|_| SseError::internal("Invalid nonce format"))?;
        let ciphertext = BASE64_STANDARD
            .decode_to_vec(ciphertext)
            .map_err(|_| SseError::internal("Invalid ciphertext format"))?;

        let key = Key::<Aes256Gcm>::from(cmk_value);
        let cipher = Aes256Gcm::new(&key);

        let nonce_array: [u8; 12] = nonce_vec.try_into().map_err(|_| SseError::internal("Invalid nonce length"))?;
        let nonce = Nonce::from(nonce_array);

        let plaintext = cipher
            .decrypt(&nonce, ciphertext.as_slice())
            .map_err(|e| SseError::internal(format!("Failed to decrypt DEK: {e}")))?;

        let dek: [u8; 32] = plaintext
            .try_into()
            .map_err(|_| SseError::internal("Decrypted DEK has invalid length"))?;

        Ok(dek)
    }
}

#[async_trait]
impl SseDekProvider for LocalSseDekProvider {
    fn wraps_dek_with_local_master_key(&self) -> bool {
        true
    }

    async fn generate_sse_dek(
        &self,
        _context: &ObjectEncryptionContext,
        _kms_key_id: &str,
    ) -> Result<(DataKey, Vec<u8>), SseError> {
        // Generate a 32-byte array as data key
        let mut dek = [0u8; 32];
        rand::rng().fill_bytes(&mut dek);

        // Generate a 12-byte array as IV
        let mut nonce = [0u8; 12];
        rand::rng().fill_bytes(&mut nonce);

        // Encrypt data key with master key
        let encrypted_dek = Self::encrypt_dek(dek, self.master_key)?;

        // Return data key and IV
        Ok((
            DataKey {
                plaintext_key: dek,
                nonce,
            },
            encrypted_dek.into_bytes(),
        ))
    }

    async fn decrypt_sse_dek(
        &self,
        encrypted_dek: &[u8],
        _kms_key_id: &str,
        _context: &ObjectEncryptionContext,
    ) -> Result<[u8; 32], SseError> {
        // Decrypt data key with master key
        let encrypted_dek_str =
            std::str::from_utf8(encrypted_dek).map_err(|_| SseError::internal("Invalid UTF-8 in encrypted DEK"))?;
        let dek = Self::decrypt_dek(encrypted_dek_str, self.master_key)?;
        Ok(dek)
    }

    #[cfg(feature = "rio-v2")]
    async fn decrypt_minio_sse_dek(
        &self,
        encrypted_dek: &[u8],
        _kms_key_id: &str,
        context: &ObjectEncryptionContext,
    ) -> Result<[u8; 32], SseError> {
        let aad = minio_kms_associated_data(context)?;
        decrypt_minio_kms_data_key(encrypted_dek, &self.master_key, &aad)
    }
}
