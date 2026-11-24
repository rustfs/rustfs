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

//! Reading data keys sealed by MinIO's builtin KMS.

use crate::core::keying::context::canonical_kms_bucket_path;
use crate::core::keying::object::HmacSha256;
use crate::error::SseError;
use base64_simd::STANDARD as BASE64_STANDARD;
use hmac::Mac;
use rustfs_kms::types::ObjectEncryptionContext;
use serde::Deserialize;

/// Associated data MinIO binds when sealing a data key.
///
/// MinIO passes the object's encryption context as the AEAD's associated data,
/// serialized as canonical JSON with sorted keys — the same canonicalization
/// [`rustfs_kms::context_aad`] performs, which is why the context RustFS
/// already rebuilds for the read can be reused verbatim. For SSE-S3 that
/// context is `{bucket: "bucket/object"}`; for SSE-KMS it is whatever the
/// request supplied, recovered from the stored MinIO context header.
pub(crate) fn minio_kms_associated_data(context: &ObjectEncryptionContext) -> Result<Vec<u8>, SseError> {
    let mut ctx = context.encryption_context.clone();
    ctx.entry(context.bucket.clone())
        .or_insert_with(|| canonical_kms_bucket_path(&context.bucket, &context.object_key));
    rustfs_kms::context_aad(&ctx).map_err(|e| SseError::internal(format!("Failed to canonicalize MinIO KMS context: {e}")))
}

/// MinIO's builtin-KMS ciphertext in its JSON encoding.
///
/// Deliberately its own type rather than a relaxation of
/// [`LocalSseDekEnvelope`]: widening that envelope's `deny_unknown_fields`
/// to admit this shape would also admit malformed RustFS envelopes, which
/// backlog#1567 requires to keep failing closed.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct MinioKmsCiphertextJson {
    aead: String,
    #[allow(
        dead_code,
        reason = "present in MinIO's encoding; the key is identified by metadata instead"
    )]
    #[serde(default)]
    id: String,
    iv: String,
    nonce: String,
    bytes: String,
}

/// Bytes of trailing randomness every MinIO builtin-KMS ciphertext carries:
/// a 16-byte IV followed by a 12-byte nonce, *after* the sealed bytes.
const MINIO_KMS_RANDOM_LEN: usize = 28;

const MINIO_KMS_IV_LEN: usize = 16;

const MINIO_KMS_AEAD_AES_GCM: &str = "AES-256-GCM-HMAC-SHA-256";

const MINIO_KMS_AEAD_CHACHA20: &str = "ChaCha20Poly1305";

/// Unwrap a data key sealed by MinIO's builtin (static-secret) KMS.
///
/// The wire format is `sealed_bytes || iv[16] || nonce[12]` — the randomness
/// trails the ciphertext rather than leading it, and MinIO's own decoder
/// normalizes its legacy JSON encoding into exactly that byte order before
/// opening it (`internal/kms/secret-key.go`, `parseCiphertext`). A raw
/// (non-JSON) ciphertext is AES-256-GCM by definition there; the JSON form
/// names its algorithm.
///
/// The sealing key is derived per ciphertext rather than being the master key:
/// `HMAC-SHA256(master, iv)` for AES-256-GCM, `HChaCha20(master, iv)` for
/// ChaCha20-Poly1305. The encryption context is bound as associated data.
pub(crate) fn decrypt_minio_kms_data_key(encrypted_dek: &[u8], master_key: &[u8; 32], aad: &[u8]) -> Result<[u8; 32], SseError> {
    let (body, algorithm) = match std::str::from_utf8(encrypted_dek) {
        // MinIO only treats a payload as JSON when it both starts and ends like
        // an object, and falls back to the raw layout when it does not parse —
        // mirrored here so a ciphertext that merely looks like JSON is not
        // rejected outright.
        Ok(text)
            if text.starts_with('{')
                && text.ends_with('}')
                && let Ok(json) = serde_json::from_str::<MinioKmsCiphertextJson>(text) =>
        {
            let decode = |what: &str, value: &str| -> Result<Vec<u8>, SseError> {
                BASE64_STANDARD
                    .decode_to_vec(value)
                    .map_err(|e| SseError::internal(format!("Invalid MinIO KMS {what}: {e}")))
            };
            let mut body = decode("ciphertext", &json.bytes)?;
            body.extend_from_slice(&decode("iv", &json.iv)?);
            body.extend_from_slice(&decode("nonce", &json.nonce)?);
            (body, json.aead)
        }
        _ => (encrypted_dek.to_vec(), MINIO_KMS_AEAD_AES_GCM.to_string()),
    };

    if body.len() <= MINIO_KMS_RANDOM_LEN {
        return Err(SseError::internal("MinIO KMS ciphertext is too short to carry its IV and nonce"));
    }
    let (sealed, random) = body.split_at(body.len() - MINIO_KMS_RANDOM_LEN);
    let (iv, nonce) = random.split_at(MINIO_KMS_IV_LEN);

    let plaintext = match algorithm.as_str() {
        MINIO_KMS_AEAD_AES_GCM => {
            use aes_gcm::{Aes256Gcm, KeyInit, aead::Aead};
            let mut mac = HmacSha256::new_from_slice(master_key)
                .map_err(|_| SseError::internal("MinIO KMS sealing key derivation failed"))?;
            mac.update(iv);
            let sealing_key: [u8; 32] = mac.finalize().into_bytes().into();
            let cipher = Aes256Gcm::new_from_slice(&sealing_key)
                .map_err(|_| SseError::internal("MinIO KMS sealing key is not a valid AES-256 key"))?;
            let nonce = aes_gcm::Nonce::try_from(nonce).map_err(|_| SseError::internal("MinIO KMS nonce is not 12 bytes"))?;
            cipher.decrypt(&nonce, aes_gcm::aead::Payload { msg: sealed, aad })
        }
        MINIO_KMS_AEAD_CHACHA20 => {
            use chacha20poly1305::{KeyInit, XChaCha20Poly1305, aead::Aead};
            // MinIO derives this branch's key with HChaCha20 over the 16-byte
            // IV, which is exactly XChaCha20-Poly1305's own construction, so the
            // extended-nonce cipher does the derivation rather than hand-rolling it.
            let mut extended = Vec::with_capacity(MINIO_KMS_IV_LEN + nonce.len());
            extended.extend_from_slice(iv);
            extended.extend_from_slice(nonce);
            let cipher = XChaCha20Poly1305::new_from_slice(master_key)
                .map_err(|_| SseError::internal("MinIO KMS master key is not a valid ChaCha20 key"))?;
            let nonce = chacha20poly1305::XNonce::try_from(extended.as_slice())
                .map_err(|_| SseError::internal("MinIO KMS extended nonce is not 24 bytes"))?;
            cipher.decrypt(&nonce, chacha20poly1305::aead::Payload { msg: sealed, aad })
        }
        other => {
            return Err(SseError::internal(format!("Unsupported MinIO KMS AEAD algorithm: {other}")));
        }
    }
    // An AEAD failure here is authentication, not a decode slip: a wrong master
    // key, a tampered ciphertext, and an encryption context that does not match
    // what sealed it all land here and must all fail closed.
    .map_err(|_| SseError::internal("MinIO KMS data key failed authentication"))?;

    plaintext
        .try_into()
        .map_err(|value: Vec<u8>| SseError::internal(format!("MinIO KMS data key must be 32 bytes, got {}", value.len())))
}
