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

//! Object keying: a derived object key, sealed in the MinIO-compatible format.

use crate::core::keying::material::{EncryptionKeyKind, EncryptionMaterial, ManagedSealedKey};
use crate::core::keying::{NewObjectKey, SealedEtag, StoredDataKey, StoredKeyMode};
use crate::error::SseError;
use aes_gcm::aead::{Aead, KeyInit, Payload};
use aes_gcm::{Aes256Gcm, Nonce};
use base64_simd::STANDARD as BASE64_STANDARD;
use chacha20poly1305::ChaCha20Poly1305;
use hmac::{Hmac, Mac};
use rand::RngExt;
use rustfs_kms::DataKey;
use rustfs_utils::http::object_encryption_keys::{
    INTERNAL_ENCRYPTION_ALGORITHM_HEADER, INTERNAL_ENCRYPTION_IV_HEADER, INTERNAL_ENCRYPTION_KEY_HEADER,
    MINIO_INTERNAL_ENCRYPTION_ALGORITHM_HEADER, MINIO_INTERNAL_ENCRYPTION_IV_HEADER,
    MINIO_INTERNAL_ENCRYPTION_KMS_DATA_KEY_HEADER, MINIO_INTERNAL_ENCRYPTION_KMS_KEY_ID_HEADER,
    MINIO_INTERNAL_ENCRYPTION_KMS_SEALED_KEY_HEADER, MINIO_INTERNAL_ENCRYPTION_MULTIPART_HEADER,
    MINIO_INTERNAL_ENCRYPTION_S3_SEALED_KEY_HEADER, MINIO_INTERNAL_ENCRYPTION_SSEC_SEALED_KEY_HEADER, SSEType,
};
use sha2::Sha256;
use std::collections::HashMap;

pub(crate) use rustfs_utils::http::object_encryption_keys::MINIO_INTERNAL_ENCRYPTION_SEAL_ALGORITHM;

const DARE_VERSION_20: u8 = 0x20;

pub(crate) const DARE_CIPHER_AES_256_GCM: u8 = 0x00;

pub(crate) const DARE_CIPHER_CHACHA20_POLY1305: u8 = 0x01;

const DARE_HEADER_SIZE: usize = 16;

const DARE_TAG_SIZE: usize = 16;

pub(crate) const SEALED_KEY_IV_SIZE: usize = 32;

pub(crate) const SEALED_KEY_SIZE: usize = DARE_HEADER_SIZE + 32 + DARE_TAG_SIZE;

const OBJECT_KEY_DERIVATION_CONTEXT: &[u8] = b"object-encryption-key generation";

pub(crate) type HmacSha256 = Hmac<Sha256>;

fn managed_sse_domain(sse_type: SSEType) -> &'static str {
    match sse_type {
        SSEType::SseS3 => "SSE-S3",
        SSEType::SseKms => "SSE-KMS",
        SSEType::SseC => "SSE-C",
    }
}

pub(crate) fn is_supported_sealed_object_key_cipher(cipher: u8) -> bool {
    matches!(cipher, DARE_CIPHER_AES_256_GCM | DARE_CIPHER_CHACHA20_POLY1305)
}

fn decrypt_sealed_object_key_payload(sealing_key: [u8; 32], header: &[u8], sealed_key: &[u8]) -> Result<Vec<u8>, SseError> {
    let nonce = &header[4..16];
    let ciphertext = &sealed_key[DARE_HEADER_SIZE..];
    let aad = &header[..4];
    match header[1] {
        DARE_CIPHER_AES_256_GCM => {
            let cipher = Aes256Gcm::new_from_slice(&sealing_key)
                .map_err(|err| SseError::internal(format!("Invalid AES-GCM sealing key: {err}")))?;
            let nonce = Nonce::try_from(nonce).map_err(|_| SseError::internal("Invalid sealed object-key package nonce"))?;
            cipher.decrypt(&nonce, Payload { msg: ciphertext, aad })
        }
        DARE_CIPHER_CHACHA20_POLY1305 => {
            let cipher = ChaCha20Poly1305::new_from_slice(&sealing_key)
                .map_err(|err| SseError::internal(format!("Invalid ChaCha20-Poly1305 sealing key: {err}")))?;
            let nonce = chacha20poly1305::Nonce::try_from(nonce)
                .map_err(|_| SseError::internal("Invalid sealed object-key package nonce"))?;
            cipher.decrypt(&nonce, Payload { msg: ciphertext, aad })
        }
        _ => return Err(SseError::internal("Unsupported sealed object-key DARE header")),
    }
    .map_err(|err| SseError::internal(format!("Failed to unseal object key: {err}")))
}

fn canonical_sse_path(bucket: &str, object: &str) -> String {
    let bucket = bucket.trim_matches('/');
    let object = object.trim_matches('/');
    if object.is_empty() {
        bucket.to_string()
    } else if bucket.is_empty() {
        object.to_string()
    } else {
        format!("{bucket}/{object}")
    }
}

pub(crate) fn derive_object_key(external_key: [u8; 32]) -> Result<[u8; 32], SseError> {
    let mut random = [0u8; 32];
    rand::rng().fill(&mut random);

    let mut mac = HmacSha256::new_from_slice(&external_key)
        .map_err(|err| SseError::internal(format!("Invalid HMAC key for object-key derivation: {err}")))?;
    mac.update(OBJECT_KEY_DERIVATION_CONTEXT);
    mac.update(&random);

    let mut object_key = [0u8; 32];
    object_key.copy_from_slice(mac.finalize().into_bytes().as_slice());
    Ok(object_key)
}

fn derive_sealing_key(
    external_key: [u8; 32],
    iv: [u8; SEALED_KEY_IV_SIZE],
    domain: &str,
    bucket: &str,
    object: &str,
) -> Result<[u8; 32], SseError> {
    let mut mac = HmacSha256::new_from_slice(&external_key)
        .map_err(|err| SseError::internal(format!("Invalid HMAC key for sealing-key derivation: {err}")))?;
    mac.update(&iv);
    mac.update(domain.as_bytes());
    mac.update(MINIO_INTERNAL_ENCRYPTION_SEAL_ALGORITHM.as_bytes());
    mac.update(canonical_sse_path(bucket, object).as_bytes());

    let mut sealing_key = [0u8; 32];
    sealing_key.copy_from_slice(mac.finalize().into_bytes().as_slice());
    Ok(sealing_key)
}

pub(crate) fn seal_object_key(
    object_key: [u8; 32],
    external_key: [u8; 32],
    sse_type: SSEType,
    bucket: &str,
    object: &str,
) -> Result<ManagedSealedKey, SseError> {
    let mut iv = [0u8; SEALED_KEY_IV_SIZE];
    rand::rng().fill(&mut iv);
    let sealing_key = derive_sealing_key(external_key, iv, managed_sse_domain(sse_type), bucket, object)?;

    let mut header = [0u8; DARE_HEADER_SIZE];
    header[0] = DARE_VERSION_20;
    header[1] = DARE_CIPHER_AES_256_GCM;
    header[2..4].copy_from_slice(&(31u16).to_le_bytes());
    let mut stream_nonce = [0u8; 12];
    rand::rng().fill(&mut stream_nonce);
    stream_nonce[0] |= 0x80;
    header[4..16].copy_from_slice(&stream_nonce);

    let cipher =
        Aes256Gcm::new_from_slice(&sealing_key).map_err(|err| SseError::internal(format!("Invalid sealing key: {err}")))?;
    let nonce =
        Nonce::try_from(stream_nonce.as_slice()).map_err(|_| SseError::internal("Invalid sealed object-key stream nonce"))?;
    let ciphertext = cipher
        .encrypt(
            &nonce,
            aes_gcm::aead::Payload {
                msg: &object_key,
                aad: &header[..4],
            },
        )
        .map_err(|err| SseError::internal(format!("Failed to seal object key: {err}")))?;

    let mut sealed_key = [0u8; SEALED_KEY_SIZE];
    sealed_key[..DARE_HEADER_SIZE].copy_from_slice(&header);
    sealed_key[DARE_HEADER_SIZE..].copy_from_slice(&ciphertext);

    Ok(ManagedSealedKey { iv, sealed_key })
}

pub(crate) fn unseal_object_key(
    sealed: &ManagedSealedKey,
    external_key: [u8; 32],
    sse_type: SSEType,
    bucket: &str,
    object: &str,
) -> Result<[u8; 32], SseError> {
    let header = &sealed.sealed_key[..DARE_HEADER_SIZE];
    if header[0] != DARE_VERSION_20 || !is_supported_sealed_object_key_cipher(header[1]) {
        return Err(SseError::internal("Unsupported sealed object-key DARE header"));
    }
    if u16::from_le_bytes([header[2], header[3]]) != 31 || header[4] & 0x80 == 0 {
        return Err(SseError::internal("Invalid sealed object-key payload header"));
    }

    let sealing_key = derive_sealing_key(external_key, sealed.iv, managed_sse_domain(sse_type), bucket, object)?;
    let plaintext = decrypt_sealed_object_key_payload(sealing_key, header, &sealed.sealed_key)?;

    let object_key: [u8; 32] = plaintext
        .as_slice()
        .try_into()
        .map_err(|_| SseError::internal("Sealed object key must decrypt to 32 bytes"))?;
    Ok(object_key)
}

/// Domain string MinIO derives the ETag sealing key under (`internal/crypto/key.go`).
const ETAG_SEALING_KEY_CONTEXT: &[u8] = b"SSE-etag";

/// Shortest stored ETag MinIO treats as sealed rather than as a bare digest
/// (`internal/etag`, `ETag.IsEncrypted`).
pub(crate) const SEALED_ETAG_MIN_SIZE: usize = 32;

/// Recover the content MD5 MinIO sealed as the stored ETag of a single-part SSE object.
///
/// The sealed ETag is one final DARE 2.0 package keyed by
/// `HMAC-SHA256(object_key, "SSE-etag")`.
///
/// # Errors
///
/// When `sealed` is not exactly one final package, or does not authenticate
/// under `object_key`.
pub(crate) fn unseal_etag(object_key: [u8; 32], sealed: &[u8]) -> Result<Vec<u8>, SseError> {
    let Some((header, ciphertext)) = sealed.split_at_checked(DARE_HEADER_SIZE) else {
        return Err(SseError::internal("Sealed ETag is shorter than a DARE header"));
    };
    if header[0] != DARE_VERSION_20 || !is_supported_sealed_object_key_cipher(header[1]) {
        return Err(SseError::internal("Unsupported sealed ETag DARE header"));
    }
    let payload_len = usize::from(u16::from_le_bytes([header[2], header[3]])) + 1;
    if ciphertext.len() != payload_len + DARE_TAG_SIZE || header[4] & 0x80 == 0 {
        return Err(SseError::internal("Sealed ETag is not a single final DARE package"));
    }

    let mut mac = HmacSha256::new_from_slice(&object_key)
        .map_err(|err| SseError::internal(format!("Invalid HMAC key for ETag sealing-key derivation: {err}")))?;
    mac.update(ETAG_SEALING_KEY_CONTEXT);
    let sealing_key: [u8; 32] = mac.finalize().into_bytes().into();

    decrypt_sealed_object_key_payload(sealing_key, header, sealed)
}

/// Whether a stored ETag has the form of a sealed one, whatever the object
/// it belongs to: hex, and at least [`SEALED_ETAG_MIN_SIZE`] bytes of it. A
/// bare digest is shorter and a multipart ETag is not hex.
pub(crate) fn has_sealed_etag_form(stored_etag: &str) -> bool {
    stored_etag.len() >= SEALED_ETAG_MIN_SIZE * 2
        && stored_etag.len().is_multiple_of(2)
        && stored_etag.bytes().all(|b| b.is_ascii_hexdigit())
}

/// The sealed form of a stored ETag, when the object carries a sealed object
/// key and its ETag is sealed.
///
/// `None` means the stored ETag is what a response shows: the object is a
/// multipart upload (its ETag is stored as shown), the ETag is not sealed, or
/// the object carries no sealed object key. A customer-key or KMS object is
/// shown the last 32 hex characters of the sealed value, which needs no key.
pub(crate) fn sealed_etag(metadata: &HashMap<String, String>, stored_etag: &str) -> Option<SealedEtag> {
    if metadata.contains_key(MINIO_INTERNAL_ENCRYPTION_MULTIPART_HEADER) {
        return None;
    }
    // A multipart ETag carries a `-<parts>` suffix and is not hex, so it
    // stops here together with every unsealed digest.
    let sealed = hex_simd::decode_to_vec(stored_etag)
        .ok()
        .filter(|sealed| sealed.len() >= SEALED_ETAG_MIN_SIZE)?;

    if metadata.contains_key(MINIO_INTERNAL_ENCRYPTION_SSEC_SEALED_KEY_HEADER)
        || metadata.contains_key(MINIO_INTERNAL_ENCRYPTION_KMS_SEALED_KEY_HEADER)
    {
        return stored_etag
            .get(stored_etag.len() - 32..)
            .map(|shown| SealedEtag::Shown(shown.to_owned()));
    }
    metadata
        .contains_key(MINIO_INTERNAL_ENCRYPTION_S3_SEALED_KEY_HEADER)
        .then_some(SealedEtag::UnderObjectKey(sealed))
}

/// The content MD5 a sealed ETag holds, as lowercase hex.
///
/// `None` when `key_bytes` is not an object key, or the
/// sealed bytes do not authenticate under it.
pub(crate) fn open_sealed_etag(key_kind: EncryptionKeyKind, key_bytes: [u8; 32], sealed: &[u8]) -> Option<String> {
    if key_kind != EncryptionKeyKind::Object {
        return None;
    }
    let etag = unseal_etag(key_bytes, sealed).ok()?;
    Some(hex_simd::encode_to_string(etag, hex_simd::AsciiCase::Lower))
}

fn try_decode_minio_sealed_key(bytes: &str) -> Result<Option<[u8; SEALED_KEY_SIZE]>, SseError> {
    let decoded = BASE64_STANDARD
        .decode_to_vec(bytes)
        .map_err(|e| SseError::internal(format!("Failed to decode sealed object key: {e}")))?;
    match decoded.as_slice().try_into() {
        Ok(sealed_key) => Ok(Some(sealed_key)),
        Err(_) => Ok(None),
    }
}

fn try_decode_minio_sealing_iv(bytes: &str) -> Result<Option<[u8; SEALED_KEY_IV_SIZE]>, SseError> {
    let decoded = BASE64_STANDARD
        .decode_to_vec(bytes)
        .map_err(|e| SseError::internal(format!("Failed to decode sealing IV: {e}")))?;
    match decoded.as_slice().try_into() {
        Ok(iv) => Ok(Some(iv)),
        Err(_) => Ok(None),
    }
}

pub(crate) fn is_legacy_rustfs_managed_metadata(metadata: &HashMap<String, String>) -> bool {
    metadata.contains_key(INTERNAL_ENCRYPTION_KEY_HEADER)
        && metadata.contains_key(INTERNAL_ENCRYPTION_IV_HEADER)
        && !metadata.contains_key(MINIO_INTERNAL_ENCRYPTION_S3_SEALED_KEY_HEADER)
        && !metadata.contains_key(MINIO_INTERNAL_ENCRYPTION_KMS_SEALED_KEY_HEADER)
}

/// Infer the managed SSE scheme from the MinIO sealed-key slot that is present.
///
/// Returns `None` when no managed MinIO slot is present, which keeps callers on
/// their fail-closed path. SSE-C is not a managed scheme and is handled by the
/// SSE-C read path, so its slot is not considered here.
pub(crate) fn infer_minio_managed_sse_type(metadata: &HashMap<String, String>) -> Option<SSEType> {
    if metadata.contains_key(MINIO_INTERNAL_ENCRYPTION_S3_SEALED_KEY_HEADER) {
        Some(SSEType::SseS3)
    } else if metadata.contains_key(MINIO_INTERNAL_ENCRYPTION_KMS_SEALED_KEY_HEADER) {
        Some(SSEType::SseKms)
    } else {
        None
    }
}

pub(crate) fn parse_minio_managed_sealed_key(
    metadata: &HashMap<String, String>,
    sse_type: SSEType,
) -> Result<Option<ManagedSealedKey>, SseError> {
    let algorithm = match metadata.get(MINIO_INTERNAL_ENCRYPTION_ALGORITHM_HEADER) {
        Some(algorithm) => algorithm,
        None => return Ok(None),
    };
    if algorithm != MINIO_INTERNAL_ENCRYPTION_SEAL_ALGORITHM {
        return Ok(None);
    }

    let iv = match metadata.get(MINIO_INTERNAL_ENCRYPTION_IV_HEADER) {
        Some(iv) => match try_decode_minio_sealing_iv(iv)? {
            Some(iv) => iv,
            None => return Ok(None),
        },
        None => return Ok(None),
    };

    let header_key = match sse_type {
        SSEType::SseS3 => MINIO_INTERNAL_ENCRYPTION_S3_SEALED_KEY_HEADER,
        SSEType::SseKms => MINIO_INTERNAL_ENCRYPTION_KMS_SEALED_KEY_HEADER,
        SSEType::SseC => MINIO_INTERNAL_ENCRYPTION_SSEC_SEALED_KEY_HEADER,
    };
    let sealed_key = match metadata.get(header_key) {
        Some(value) => match try_decode_minio_sealed_key(value)? {
            Some(sealed_key) => sealed_key,
            None => return Ok(None),
        },
        None => return Ok(None),
    };

    Ok(Some(ManagedSealedKey { iv, sealed_key }))
}

/// SSE-C: record the object key sealed under the customer key.
pub(crate) fn record_sealed_customer_key(metadata: &mut HashMap<String, String>, sealed: &ManagedSealedKey) {
    metadata.insert(
        MINIO_INTERNAL_ENCRYPTION_IV_HEADER.to_string(),
        BASE64_STANDARD.encode_to_string(sealed.iv),
    );
    metadata.insert(
        MINIO_INTERNAL_ENCRYPTION_ALGORITHM_HEADER.to_string(),
        MINIO_INTERNAL_ENCRYPTION_SEAL_ALGORITHM.to_string(),
    );
    metadata.insert(
        MINIO_INTERNAL_ENCRYPTION_SSEC_SEALED_KEY_HEADER.to_string(),
        BASE64_STANDARD.encode_to_string(sealed.sealed_key),
    );
}

/// Managed SSE: record the wrapped data key in MinIO's slots.
///
/// With a sealed object key the slots hold what MinIO itself writes. Without one
/// the key was formed in Direct mode, and the slots mirror its wrapped data key and
/// nonce: [`parse_minio_managed_sealed_key`] turns that shape down by its length, so
/// the object is read back in Direct mode.
pub(crate) fn record_data_key_in_minio_slots(
    metadata: &mut HashMap<String, String>,
    material: &EncryptionMaterial,
    wrapped_data_key: &[u8],
) {
    metadata.insert(
        MINIO_INTERNAL_ENCRYPTION_ALGORITHM_HEADER.to_string(),
        MINIO_INTERNAL_ENCRYPTION_SEAL_ALGORITHM.to_string(),
    );
    if let Some(kms_key_id) = &material.kms_key_id {
        metadata.insert(MINIO_INTERNAL_ENCRYPTION_KMS_KEY_ID_HEADER.to_string(), kms_key_id.to_string());
    }

    let encoded_data_key = BASE64_STANDARD.encode_to_string(wrapped_data_key);
    match &material.managed_sealed_key {
        Some(sealed) => {
            metadata.insert(
                MINIO_INTERNAL_ENCRYPTION_IV_HEADER.to_string(),
                BASE64_STANDARD.encode_to_string(sealed.iv),
            );
            match material.sse_type {
                SSEType::SseS3 => {
                    metadata.insert(
                        MINIO_INTERNAL_ENCRYPTION_S3_SEALED_KEY_HEADER.to_string(),
                        BASE64_STANDARD.encode_to_string(sealed.sealed_key),
                    );
                }
                SSEType::SseKms => {
                    metadata.insert(
                        MINIO_INTERNAL_ENCRYPTION_KMS_SEALED_KEY_HEADER.to_string(),
                        BASE64_STANDARD.encode_to_string(sealed.sealed_key),
                    );
                }
                SSEType::SseC => {}
            }
            metadata.insert(MINIO_INTERNAL_ENCRYPTION_KMS_DATA_KEY_HEADER.to_string(), encoded_data_key);
        }
        None => {
            metadata.insert(
                MINIO_INTERNAL_ENCRYPTION_IV_HEADER.to_string(),
                BASE64_STANDARD.encode_to_string(material.base_nonce),
            );
            match material.sse_type {
                SSEType::SseS3 => {
                    metadata.insert(MINIO_INTERNAL_ENCRYPTION_S3_SEALED_KEY_HEADER.to_string(), encoded_data_key);
                }
                SSEType::SseKms => {
                    metadata.insert(MINIO_INTERNAL_ENCRYPTION_KMS_SEALED_KEY_HEADER.to_string(), encoded_data_key.clone());
                    metadata.insert(MINIO_INTERNAL_ENCRYPTION_KMS_DATA_KEY_HEADER.to_string(), encoded_data_key);
                }
                SSEType::SseC => {}
            }
        }
    }
}

/// Managed SSE: read back the wrapped data key that seals `sealed_key`.
///
/// `normalized_metadata` is `metadata` with the managed keys normalized.
pub(crate) fn read_sealed_data_key(
    metadata: &HashMap<String, String>,
    normalized_metadata: &HashMap<String, String>,
    sealed_key: ManagedSealedKey,
) -> Result<StoredDataKey, SseError> {
    let encrypted_key_b64 = normalized_metadata
        .get(INTERNAL_ENCRYPTION_KEY_HEADER)
        .or_else(|| metadata.get(MINIO_INTERNAL_ENCRYPTION_KMS_DATA_KEY_HEADER))
        .ok_or_else(|| SseError::internal("Missing encrypted key in metadata"))?;
    let wrapped = BASE64_STANDARD
        .decode_to_vec(encrypted_key_b64)
        .map_err(|e| SseError::internal(format!("Failed to decode encrypted key: {e}")))?;

    Ok(StoredDataKey {
        wrapped,
        algorithm: normalized_metadata
            .get(INTERNAL_ENCRYPTION_ALGORITHM_HEADER)
            .cloned()
            .unwrap_or_else(|| "AES256".to_string()),
        mode: StoredKeyMode::Object { sealed_key },
    })
}

/// Whether a multipart session needs the customer key when it is created.
///
/// Object mode seals the session's object key under the customer key, so the key
/// has to be present, and valid, at that point.
pub(crate) const SESSION_KEY_NEEDS_CUSTOMER_KEY: bool = true;

/// Derive a fresh object key from `external_key` and seal it under the same key.
fn new_sealed_object_key(external_key: [u8; 32], sse_type: SSEType, bucket: &str, key: &str) -> Result<NewObjectKey, SseError> {
    let object_key = derive_object_key(external_key)?;
    let sealed_key = seal_object_key(object_key, external_key, sse_type, bucket, key)?;
    Ok(NewObjectKey {
        key_bytes: object_key,
        base_nonce: [0; 12],
        key_kind: EncryptionKeyKind::Object,
        sealed_key: Some(sealed_key),
    })
}

/// SSE-C object: an object key sealed under the customer key.
pub(crate) fn new_key_under_customer_key(customer_key: [u8; 32], bucket: &str, key: &str) -> Result<NewObjectKey, SseError> {
    new_sealed_object_key(customer_key, SSEType::SseC, bucket, key)
}

/// SSE-C multipart session: an object key sealed under the customer key.
///
/// A session created without its customer key has nothing to seal and records a
/// placeholder instead.
pub(crate) fn new_session_key_under_customer_key(
    customer_key: Option<[u8; 32]>,
    bucket: &str,
    key: &str,
) -> Result<NewObjectKey, SseError> {
    match customer_key {
        Some(customer_key) => new_sealed_object_key(customer_key, SSEType::SseC, bucket, key),
        None => Ok(NewObjectKey {
            key_bytes: [0; 32],
            base_nonce: [0; 12],
            key_kind: EncryptionKeyKind::Direct,
            sealed_key: None,
        }),
    }
}

/// Managed SSE: an object key sealed under the data key.
pub(crate) fn new_key_under_data_key(
    data_key: &DataKey,
    sse_type: SSEType,
    bucket: &str,
    key: &str,
) -> Result<NewObjectKey, SseError> {
    new_sealed_object_key(data_key.plaintext_key, sse_type, bucket, key)
}
