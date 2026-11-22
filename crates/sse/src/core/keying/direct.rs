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

//! Direct keying: the data key encrypts the object itself, under a stored base nonce.

#[cfg(not(feature = "rio-v2"))]
use crate::core::keying::NewObjectKey;
#[cfg(not(feature = "rio-v2"))]
use crate::core::keying::material::EncryptionKeyKind;
use crate::core::s3::ssec::md5_bytes;
#[cfg(not(feature = "rio-v2"))]
use crate::error::SseError;
use base64_simd::STANDARD as BASE64_STANDARD;
#[cfg(not(feature = "rio-v2"))]
use rand::Rng;
#[cfg(not(feature = "rio-v2"))]
use rustfs_kms::DataKey;
#[cfg(not(feature = "rio-v2"))]
use rustfs_utils::http::object_encryption_keys::SSEType;
use rustfs_utils::http::object_encryption_keys::{INTERNAL_ENCRYPTION_IV_HEADER, MINIO_INTERNAL_ENCRYPTION_IV_HEADER};
use std::collections::HashMap;

/// Resolve the SSE-C Direct base nonce for decryption.
///
/// New objects persist a random nonce at encryption time (see
/// `encryption_material_to_metadata`) under `INTERNAL_ENCRYPTION_IV_HEADER` (preferred) or,
/// for MinIO interop, `MINIO_INTERNAL_ENCRYPTION_IV_HEADER`. Legacy objects written before
/// random nonces were persisted carry no stored IV; for those we fall back to the
/// deterministic `generate_ssec_nonce(bucket, key)` value they were originally encrypted
/// with, so previously stored objects still decrypt correctly.
pub(crate) fn read_stored_ssec_nonce(metadata: &HashMap<String, String>, bucket: &str, key: &str) -> [u8; 12] {
    metadata
        .get(INTERNAL_ENCRYPTION_IV_HEADER)
        .or_else(|| metadata.get(MINIO_INTERNAL_ENCRYPTION_IV_HEADER))
        .and_then(|encoded| BASE64_STANDARD.decode_to_vec(encoded).ok())
        .and_then(|bytes| <[u8; 12]>::try_from(bytes.as_slice()).ok())
        .unwrap_or_else(|| generate_ssec_nonce(bucket, key))
}

/// Generate deterministic nonce for SSE-C encryption
///
/// The nonce is derived from the bucket and key to ensure:
/// 1. Same object always gets the same nonce (required for SSE-C)
/// 2. Different objects get different nonces
pub(crate) fn generate_ssec_nonce(bucket: &str, key: &str) -> [u8; 12] {
    let nonce_source = format!("{bucket}-{key}");
    let nonce_hash = md5_bytes(nonce_source.as_bytes());
    let mut nonce = [0u8; 12];
    nonce.copy_from_slice(&nonce_hash[..12]);
    nonce
}

/// Whether a multipart session needs the customer key when it is created.
///
/// Direct mode records only a nonce for the session; the customer key is checked
/// per part, against what the session recorded.
#[cfg(not(feature = "rio-v2"))]
pub(crate) const SESSION_KEY_NEEDS_CUSTOMER_KEY: bool = false;

#[cfg(not(feature = "rio-v2"))]
fn random_base_nonce() -> [u8; 12] {
    let mut base_nonce = [0u8; 12];
    rand::rng().fill_bytes(&mut base_nonce);
    base_nonce
}

/// SSE-C object: the customer key encrypts the object itself.
///
/// Uses a fresh random nonce per encryption. A deterministic nonce derived from
/// (bucket, key) would repeat whenever the same object is overwritten under the same
/// SSE-C key, reusing an identical (key, nonce) pair and catastrophically breaking
/// AES-256-GCM. The nonce is persisted (see `encryption_material_to_metadata`) and read
/// back on decrypt (see [`read_stored_ssec_nonce`]).
#[cfg(not(feature = "rio-v2"))]
pub(crate) fn new_key_under_customer_key(customer_key: [u8; 32], _bucket: &str, _key: &str) -> Result<NewObjectKey, SseError> {
    Ok(NewObjectKey {
        key_bytes: customer_key,
        base_nonce: random_base_nonce(),
        key_kind: EncryptionKeyKind::Direct,
        sealed_key: None,
    })
}

/// SSE-C multipart session: a nonce every part of the upload shares.
///
/// Generates a real random nonce for the session and persists it (see
/// `encryption_material_to_metadata`). Every part of this upload reads the same
/// persisted nonce back, so all parts share one nonce without reintroducing the
/// deterministic (bucket, key) reuse hazard. `key_bytes` stays a placeholder: it is
/// neither used for encryption nor persisted; the real customer key is validated per
/// part on upload.
#[cfg(not(feature = "rio-v2"))]
pub(crate) fn new_session_key_under_customer_key(
    _customer_key: Option<[u8; 32]>,
    _bucket: &str,
    _key: &str,
) -> Result<NewObjectKey, SseError> {
    Ok(NewObjectKey {
        key_bytes: [0; 32],
        base_nonce: random_base_nonce(),
        key_kind: EncryptionKeyKind::Direct,
        sealed_key: None,
    })
}

/// Managed SSE: the data key encrypts the object itself, under the nonce issued with it.
#[cfg(not(feature = "rio-v2"))]
pub(crate) fn new_key_under_data_key(
    data_key: &DataKey,
    _sse_type: SSEType,
    _bucket: &str,
    _key: &str,
) -> Result<NewObjectKey, SseError> {
    Ok(NewObjectKey {
        key_bytes: data_key.plaintext_key,
        base_nonce: data_key.nonce,
        key_kind: EncryptionKeyKind::Direct,
        sealed_key: None,
    })
}
