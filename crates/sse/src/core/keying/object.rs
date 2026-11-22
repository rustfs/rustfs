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

use crate::core::keying::material::ManagedSealedKey;
use crate::error::SseError;
use aes_gcm::aead::{Aead, KeyInit, Payload};
use aes_gcm::{Aes256Gcm, Nonce};
use base64_simd::STANDARD as BASE64_STANDARD;
use chacha20poly1305::ChaCha20Poly1305;
use hmac::{Hmac, Mac};
use rand::RngExt;
use rustfs_utils::http::object_encryption_keys::{
    INTERNAL_ENCRYPTION_IV_HEADER, INTERNAL_ENCRYPTION_KEY_HEADER, MINIO_INTERNAL_ENCRYPTION_ALGORITHM_HEADER,
    MINIO_INTERNAL_ENCRYPTION_IV_HEADER, MINIO_INTERNAL_ENCRYPTION_KMS_SEALED_KEY_HEADER,
    MINIO_INTERNAL_ENCRYPTION_S3_SEALED_KEY_HEADER, MINIO_INTERNAL_ENCRYPTION_SSEC_SEALED_KEY_HEADER, SSEType,
};
use sha2::Sha256;
use std::collections::HashMap;

pub(crate) const MINIO_INTERNAL_ENCRYPTION_SEAL_ALGORITHM: &str = "DAREv2-HMAC-SHA256";

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
