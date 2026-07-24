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

use aes_gcm::{
    Aes256Gcm, Nonce,
    aead::{Aead, KeyInit, Payload},
};
use base64::{Engine, engine::general_purpose::STANDARD as BASE64_STANDARD};
use chacha20poly1305::ChaCha20Poly1305;
use hmac::{Hmac, Mac};
use rustfs_utils::http::{AMZ_SERVER_SIDE_ENCRYPTION, get_consistent_metadata_value};
use serde::Deserialize;
use sha2::Sha256;
use std::collections::HashMap;
#[cfg(not(any(test, debug_assertions)))]
use std::sync::OnceLock;
use thiserror::Error;
use zeroize::Zeroizing;

pub(crate) const DEFAULT_SSE_ALGORITHM: &str = "AES256";
const SSE_KMS_ALGORITHM: &str = "aws:kms";
pub(crate) const INTERNAL_ENCRYPTION_KEY_ID_HEADER: &str = "x-rustfs-encryption-key-id";
pub(crate) const INTERNAL_ENCRYPTION_KEY_HEADER: &str = "x-rustfs-encryption-key";
pub(crate) const INTERNAL_ENCRYPTION_IV_HEADER: &str = "x-rustfs-encryption-iv";
pub(crate) const AMZ_SERVER_SIDE_ENCRYPTION_KMS_KEY_ID: &str = "x-amz-server-side-encryption-aws-kms-key-id";
pub(crate) const MINIO_INTERNAL_ENCRYPTION_IV_HEADER: &str = "X-Minio-Internal-Server-Side-Encryption-Iv";
pub(crate) const MINIO_INTERNAL_ENCRYPTION_ALGORITHM_HEADER: &str = "X-Minio-Internal-Server-Side-Encryption-Seal-Algorithm";
pub(crate) const MINIO_INTERNAL_ENCRYPTION_S3_SEALED_KEY_HEADER: &str = "X-Minio-Internal-Server-Side-Encryption-S3-Sealed-Key";
pub(crate) const MINIO_INTERNAL_ENCRYPTION_KMS_SEALED_KEY_HEADER: &str = "X-Minio-Internal-Server-Side-Encryption-Kms-Sealed-Key";
pub(crate) const MINIO_INTERNAL_ENCRYPTION_KMS_DATA_KEY_HEADER: &str =
    "X-Minio-Internal-Server-Side-Encryption-S3-Kms-Sealed-Key";
pub(crate) const MINIO_INTERNAL_ENCRYPTION_KMS_KEY_ID_HEADER: &str = "X-Minio-Internal-Server-Side-Encryption-S3-Kms-Key-Id";
pub(crate) const MINIO_INTERNAL_ENCRYPTION_SEAL_ALGORITHM: &str = "DAREv2-HMAC-SHA256";
const MINIO_STATIC_KMS_KEY_ENV: &str = "RUSTFS_MINIO_STATIC_KMS_KEY";
const MINIO_STATIC_KMS_RANDOM_SIZE: usize = 28;
const MINIO_STATIC_KMS_IV_SIZE: usize = 16;
const MINIO_STATIC_KMS_NONCE_SIZE: usize = 12;

type HmacSha256 = Hmac<Sha256>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ManagedSseScheme {
    SseS3,
    SseKms,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ManagedDekProvider {
    LocalSseS3,
    MinioKeyValue,
    Kms,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PersistedManagedEncryption {
    LegacySseS3Local,
    LegacySseS3Kms,
    LegacySseKms,
    MinioSseS3KeyValue,
    MinioSseS3Kms,
    MinioSseKmsKms,
}

impl PersistedManagedEncryption {
    pub fn scheme(self) -> ManagedSseScheme {
        match self {
            Self::LegacySseS3Local | Self::LegacySseS3Kms | Self::MinioSseS3KeyValue | Self::MinioSseS3Kms => {
                ManagedSseScheme::SseS3
            }
            Self::LegacySseKms | Self::MinioSseKmsKms => ManagedSseScheme::SseKms,
        }
    }

    pub fn provider(self) -> ManagedDekProvider {
        match self {
            Self::LegacySseS3Local => ManagedDekProvider::LocalSseS3,
            Self::MinioSseS3KeyValue => ManagedDekProvider::MinioKeyValue,
            Self::LegacySseS3Kms | Self::LegacySseKms | Self::MinioSseS3Kms | Self::MinioSseKmsKms => ManagedDekProvider::Kms,
        }
    }

    pub fn uses_object_key(self) -> bool {
        matches!(self, Self::MinioSseS3KeyValue | Self::MinioSseS3Kms | Self::MinioSseKmsKms)
    }
}

#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum PersistedEncryptionError {
    #[error("conflicting values for encryption metadata {field}")]
    ConflictingValue { field: &'static str },
    #[error("conflicting MinIO managed SSE format markers")]
    ConflictingMinioMarkers,
    #[error("incomplete managed SSE metadata: missing {field}")]
    MissingField { field: &'static str },
    #[error("unsupported stored server-side encryption {algorithm}")]
    UnsupportedSseAlgorithm { algorithm: String },
    #[error("unsupported MinIO seal algorithm {algorithm}")]
    UnsupportedSealAlgorithm { algorithm: String },
    #[error("stored SSE algorithm conflicts with the persisted encryption format")]
    SchemeConflict,
    #[error("conflicting encrypted data-key metadata")]
    ConflictingEncryptedDataKey,
    #[error("conflicting KMS key-id metadata")]
    ConflictingKmsKeyId,
    #[error("invalid MinIO static KMS configuration: {reason}")]
    InvalidMinioStaticKmsConfiguration { reason: String },
    #[error("invalid MinIO static KMS ciphertext: {reason}")]
    InvalidMinioStaticKmsCiphertext { reason: String },
    #[error("unrecognized legacy managed SSE encrypted data-key format")]
    UnknownLegacyEnvelope,
}

pub fn classify_persisted_managed_encryption(
    metadata: &HashMap<String, String>,
    encrypted_dek: &[u8],
) -> Result<PersistedManagedEncryption, PersistedEncryptionError> {
    let public_algorithm = consistent(metadata, AMZ_SERVER_SIDE_ENCRYPTION)?;
    if let Some(algorithm) = public_algorithm
        && !matches!(algorithm, DEFAULT_SSE_ALGORITHM | SSE_KMS_ALGORITHM)
    {
        return Err(PersistedEncryptionError::UnsupportedSseAlgorithm {
            algorithm: algorithm.to_string(),
        });
    }

    let s3_sealed_key = consistent(metadata, MINIO_INTERNAL_ENCRYPTION_S3_SEALED_KEY_HEADER)?;
    let kms_sealed_key = consistent(metadata, MINIO_INTERNAL_ENCRYPTION_KMS_SEALED_KEY_HEADER)?;
    if s3_sealed_key.is_some() && kms_sealed_key.is_some() {
        return Err(PersistedEncryptionError::ConflictingMinioMarkers);
    }

    if s3_sealed_key.is_some() || kms_sealed_key.is_some() {
        if s3_sealed_key.is_some() {
            require_non_empty(s3_sealed_key, MINIO_INTERNAL_ENCRYPTION_S3_SEALED_KEY_HEADER)?;
        }
        if kms_sealed_key.is_some() {
            require_non_empty(kms_sealed_key, MINIO_INTERNAL_ENCRYPTION_KMS_SEALED_KEY_HEADER)?;
        }
        require_non_empty(
            consistent(metadata, MINIO_INTERNAL_ENCRYPTION_IV_HEADER)?,
            MINIO_INTERNAL_ENCRYPTION_IV_HEADER,
        )?;
        let seal_algorithm = require_non_empty(
            consistent(metadata, MINIO_INTERNAL_ENCRYPTION_ALGORITHM_HEADER)?,
            MINIO_INTERNAL_ENCRYPTION_ALGORITHM_HEADER,
        )?;
        if seal_algorithm != MINIO_INTERNAL_ENCRYPTION_SEAL_ALGORITHM {
            return Err(PersistedEncryptionError::UnsupportedSealAlgorithm {
                algorithm: seal_algorithm.to_string(),
            });
        }
        let kms_key_id = consistent(metadata, MINIO_INTERNAL_ENCRYPTION_KMS_KEY_ID_HEADER)?;
        let encrypted_data_key = consistent(metadata, MINIO_INTERNAL_ENCRYPTION_KMS_DATA_KEY_HEADER)?;
        let has_kms_pair = match (kms_key_id, encrypted_data_key) {
            (Some(key_id), Some(data_key)) => {
                require_non_empty(Some(key_id), MINIO_INTERNAL_ENCRYPTION_KMS_KEY_ID_HEADER)?;
                require_non_empty(Some(data_key), MINIO_INTERNAL_ENCRYPTION_KMS_DATA_KEY_HEADER)?;
                true
            }
            (None, None) => false,
            (None, Some(_)) => {
                return Err(PersistedEncryptionError::MissingField {
                    field: MINIO_INTERNAL_ENCRYPTION_KMS_KEY_ID_HEADER,
                });
            }
            (Some(_), None) => {
                return Err(PersistedEncryptionError::MissingField {
                    field: MINIO_INTERNAL_ENCRYPTION_KMS_DATA_KEY_HEADER,
                });
            }
        };
        validate_encrypted_key_alias(metadata)?;
        validate_kms_key_id_alias(metadata)?;

        return match (s3_sealed_key, kms_sealed_key, public_algorithm) {
            (Some(_), None, Some(SSE_KMS_ALGORITHM)) | (None, Some(_), Some(DEFAULT_SSE_ALGORITHM)) => {
                Err(PersistedEncryptionError::SchemeConflict)
            }
            (Some(_), None, _) if !has_kms_pair => Ok(PersistedManagedEncryption::MinioSseS3KeyValue),
            (Some(_), None, _) => Ok(PersistedManagedEncryption::MinioSseS3Kms),
            (None, Some(_), _) if !has_kms_pair => Err(PersistedEncryptionError::MissingField {
                field: MINIO_INTERNAL_ENCRYPTION_KMS_KEY_ID_HEADER,
            }),
            (None, Some(_), _) => Ok(PersistedManagedEncryption::MinioSseKmsKms),
            _ => Err(PersistedEncryptionError::ConflictingMinioMarkers),
        };
    }

    if consistent(metadata, MINIO_INTERNAL_ENCRYPTION_ALGORITHM_HEADER)?.is_some()
        || consistent(metadata, MINIO_INTERNAL_ENCRYPTION_KMS_DATA_KEY_HEADER)?.is_some()
    {
        return Err(PersistedEncryptionError::MissingField {
            field: MINIO_INTERNAL_ENCRYPTION_S3_SEALED_KEY_HEADER,
        });
    }

    require_non_empty(consistent(metadata, INTERNAL_ENCRYPTION_KEY_HEADER)?, INTERNAL_ENCRYPTION_KEY_HEADER)?;
    require_non_empty(consistent(metadata, INTERNAL_ENCRYPTION_IV_HEADER)?, INTERNAL_ENCRYPTION_IV_HEADER)?;

    match public_algorithm {
        Some(SSE_KMS_ALGORITHM) if is_local_sse_s3_envelope(encrypted_dek) => Err(PersistedEncryptionError::SchemeConflict),
        Some(SSE_KMS_ALGORITHM) => Ok(PersistedManagedEncryption::LegacySseKms),
        Some(DEFAULT_SSE_ALGORITHM) | None if rustfs_kms::is_data_key_envelope(encrypted_dek) => {
            // RUSTFS_COMPAT_TODO(rustfs-5063): older SSE-S3 objects may contain KMS-wrapped DEKs. Remove after every referenced legacy DEK has been rewrapped with the local SSE-S3 provider.
            Ok(PersistedManagedEncryption::LegacySseS3Kms)
        }
        Some(DEFAULT_SSE_ALGORITHM) | None if is_local_sse_s3_envelope(encrypted_dek) => {
            Ok(PersistedManagedEncryption::LegacySseS3Local)
        }
        Some(DEFAULT_SSE_ALGORITHM) | None => Err(PersistedEncryptionError::UnknownLegacyEnvelope),
        Some(_) => unreachable!("unsupported algorithms return before format classification"),
    }
}

fn consistent<'a>(
    metadata: &'a HashMap<String, String>,
    field: &'static str,
) -> Result<Option<&'a str>, PersistedEncryptionError> {
    get_consistent_metadata_value(metadata, field).map_err(|_| PersistedEncryptionError::ConflictingValue { field })
}

fn require_non_empty<'a>(value: Option<&'a str>, field: &'static str) -> Result<&'a str, PersistedEncryptionError> {
    value
        .filter(|value| !value.is_empty())
        .ok_or(PersistedEncryptionError::MissingField { field })
}

fn validate_encrypted_key_alias(metadata: &HashMap<String, String>) -> Result<(), PersistedEncryptionError> {
    let Some(rustfs_key) = consistent(metadata, INTERNAL_ENCRYPTION_KEY_HEADER)? else {
        return Ok(());
    };
    let minio_key = require_non_empty(
        consistent(metadata, MINIO_INTERNAL_ENCRYPTION_KMS_DATA_KEY_HEADER)?,
        MINIO_INTERNAL_ENCRYPTION_KMS_DATA_KEY_HEADER,
    )?;
    if rustfs_key != minio_key {
        return Err(PersistedEncryptionError::ConflictingEncryptedDataKey);
    }
    Ok(())
}

fn validate_kms_key_id_alias(metadata: &HashMap<String, String>) -> Result<(), PersistedEncryptionError> {
    let values = [
        consistent(metadata, INTERNAL_ENCRYPTION_KEY_ID_HEADER)?,
        consistent(metadata, MINIO_INTERNAL_ENCRYPTION_KMS_KEY_ID_HEADER)?,
        consistent(metadata, AMZ_SERVER_SIDE_ENCRYPTION_KMS_KEY_ID)?,
    ];
    let mut present = values.into_iter().flatten().filter(|value| !value.is_empty());
    let Some(first) = present.next() else {
        return Ok(());
    };
    if present.any(|value| value != first) {
        return Err(PersistedEncryptionError::ConflictingKmsKeyId);
    }
    Ok(())
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct MinioStaticKmsJsonCiphertext {
    #[serde(rename = "aead")]
    algorithm: String,
    id: Option<String>,
    iv: String,
    nonce: String,
    bytes: String,
}

#[derive(Clone, Copy)]
enum MinioStaticKmsAlgorithm {
    Aes256,
    ChaCha20,
}

#[derive(Clone)]
struct ConfiguredMinioStaticKmsKey {
    id: String,
    material: Zeroizing<[u8; 32]>,
}

struct ParsedMinioStaticKmsCiphertext {
    ciphertext: Vec<u8>,
    iv: [u8; 16],
    nonce: [u8; 12],
    algorithm: MinioStaticKmsAlgorithm,
}

pub fn decrypt_minio_static_kms_dek(
    kms_key_id: &str,
    encrypted_dek: &[u8],
    context: &HashMap<String, String>,
) -> Result<Option<[u8; 32]>, PersistedEncryptionError> {
    let Some(configured_key) = minio_static_kms_key()? else {
        return Ok(None);
    };
    if configured_key.id != kms_key_id {
        return Ok(None);
    }

    let parsed = parse_minio_static_kms_ciphertext(encrypted_dek)?;
    let ParsedMinioStaticKmsCiphertext {
        ciphertext,
        iv,
        nonce,
        algorithm,
    } = parsed;
    let master_key = configured_key.material;
    let sealing_key: Zeroizing<[u8; 32]> = Zeroizing::new(match algorithm {
        MinioStaticKmsAlgorithm::Aes256 => {
            let mut mac = HmacSha256::new_from_slice(master_key.as_slice()).map_err(|err| {
                PersistedEncryptionError::InvalidMinioStaticKmsCiphertext {
                    reason: format!("invalid HMAC key: {err}"),
                }
            })?;
            mac.update(&iv);
            mac.finalize().into_bytes().into()
        }
        MinioStaticKmsAlgorithm::ChaCha20 => chacha20::hchacha::<chacha20::R20>((&*master_key).into(), (&iv).into()).into(),
    });
    let associated_data = marshal_minio_kms_context(context);
    let plaintext = Zeroizing::new(
        match algorithm {
            MinioStaticKmsAlgorithm::Aes256 => Aes256Gcm::new_from_slice(sealing_key.as_slice())
                .map_err(|err| PersistedEncryptionError::InvalidMinioStaticKmsCiphertext {
                    reason: format!("invalid AES sealing key: {err}"),
                })?
                .decrypt(
                    &Nonce::from(nonce),
                    Payload {
                        msg: &ciphertext,
                        aad: &associated_data,
                    },
                ),
            MinioStaticKmsAlgorithm::ChaCha20 => ChaCha20Poly1305::new_from_slice(sealing_key.as_slice())
                .map_err(|err| PersistedEncryptionError::InvalidMinioStaticKmsCiphertext {
                    reason: format!("invalid ChaCha20 sealing key: {err}"),
                })?
                .decrypt(
                    &chacha20poly1305::Nonce::from(nonce),
                    Payload {
                        msg: &ciphertext,
                        aad: &associated_data,
                    },
                ),
        }
        .map_err(|_| PersistedEncryptionError::InvalidMinioStaticKmsCiphertext {
            reason: "AEAD authentication failed".to_string(),
        })?,
    );
    plaintext
        .as_slice()
        .try_into()
        .map(Some)
        .map_err(|_| PersistedEncryptionError::InvalidMinioStaticKmsCiphertext {
            reason: "plaintext data key must be 32 bytes".to_string(),
        })
}

fn minio_static_kms_key() -> Result<Option<ConfiguredMinioStaticKmsKey>, PersistedEncryptionError> {
    #[cfg(not(any(test, debug_assertions)))]
    {
        static CONFIG: OnceLock<Result<Option<ConfiguredMinioStaticKmsKey>, PersistedEncryptionError>> = OnceLock::new();
        return CONFIG.get_or_init(parse_minio_static_kms_key).clone();
    }

    #[cfg(any(test, debug_assertions))]
    parse_minio_static_kms_key()
}

fn parse_minio_static_kms_key() -> Result<Option<ConfiguredMinioStaticKmsKey>, PersistedEncryptionError> {
    let Some(value) = std::env::var_os(MINIO_STATIC_KMS_KEY_ENV) else {
        return Ok(None);
    };
    let value =
        Zeroizing::new(
            value
                .into_string()
                .map_err(|_| PersistedEncryptionError::InvalidMinioStaticKmsConfiguration {
                    reason: format!("{MINIO_STATIC_KMS_KEY_ENV} must be valid UTF-8"),
                })?,
        );
    let (key_id, encoded_key) =
        value
            .split_once(':')
            .ok_or_else(|| PersistedEncryptionError::InvalidMinioStaticKmsConfiguration {
                reason: format!("{MINIO_STATIC_KMS_KEY_ENV} must use <key-id>:<base64-key>"),
            })?;
    if key_id.is_empty() {
        return Err(PersistedEncryptionError::InvalidMinioStaticKmsConfiguration {
            reason: "key ID must not be empty".to_string(),
        });
    }
    let decoded_key = Zeroizing::new(BASE64_STANDARD.decode(encoded_key).map_err(|_| {
        PersistedEncryptionError::InvalidMinioStaticKmsConfiguration {
            reason: "key material must be valid Base64".to_string(),
        }
    })?);
    let key: [u8; 32] =
        decoded_key
            .as_slice()
            .try_into()
            .map_err(|_| PersistedEncryptionError::InvalidMinioStaticKmsConfiguration {
                reason: "key material must decode to exactly 32 bytes".to_string(),
            })?;
    if key == [0u8; 32] {
        return Err(PersistedEncryptionError::InvalidMinioStaticKmsConfiguration {
            reason: "key material must not be all zero".to_string(),
        });
    }
    Ok(Some(ConfiguredMinioStaticKmsKey {
        id: key_id.to_string(),
        material: Zeroizing::new(key),
    }))
}

fn parse_minio_static_kms_ciphertext(encrypted_dek: &[u8]) -> Result<ParsedMinioStaticKmsCiphertext, PersistedEncryptionError> {
    if encrypted_dek.first() == Some(&b'{') && encrypted_dek.last() == Some(&b'}') {
        let envelope: MinioStaticKmsJsonCiphertext =
            serde_json::from_slice(encrypted_dek).map_err(|err| PersistedEncryptionError::InvalidMinioStaticKmsCiphertext {
                reason: format!("invalid legacy JSON: {err}"),
            })?;
        let _ = envelope.id;
        let algorithm = match envelope.algorithm.as_str() {
            "AES-256-GCM-HMAC-SHA-256" => MinioStaticKmsAlgorithm::Aes256,
            "ChaCha20Poly1305" => MinioStaticKmsAlgorithm::ChaCha20,
            algorithm => {
                return Err(PersistedEncryptionError::InvalidMinioStaticKmsCiphertext {
                    reason: format!("unsupported algorithm {algorithm}"),
                });
            }
        };
        return Ok(ParsedMinioStaticKmsCiphertext {
            ciphertext: decode_minio_static_kms_field("bytes", &envelope.bytes)?,
            iv: decode_minio_static_kms_array("iv", &envelope.iv)?,
            nonce: decode_minio_static_kms_array("nonce", &envelope.nonce)?,
            algorithm,
        });
    }

    if encrypted_dek.len() <= MINIO_STATIC_KMS_RANDOM_SIZE {
        return Err(PersistedEncryptionError::InvalidMinioStaticKmsCiphertext {
            reason: "binary ciphertext is too short".to_string(),
        });
    }
    let split_at = encrypted_dek.len() - MINIO_STATIC_KMS_RANDOM_SIZE;
    let (ciphertext, random) = encrypted_dek.split_at(split_at);
    Ok(ParsedMinioStaticKmsCiphertext {
        ciphertext: ciphertext.to_vec(),
        iv: random[..MINIO_STATIC_KMS_IV_SIZE].try_into().map_err(|_| {
            PersistedEncryptionError::InvalidMinioStaticKmsCiphertext {
                reason: "invalid binary IV length".to_string(),
            }
        })?,
        nonce: random[MINIO_STATIC_KMS_IV_SIZE..MINIO_STATIC_KMS_IV_SIZE + MINIO_STATIC_KMS_NONCE_SIZE]
            .try_into()
            .map_err(|_| PersistedEncryptionError::InvalidMinioStaticKmsCiphertext {
                reason: "invalid binary nonce length".to_string(),
            })?,
        algorithm: MinioStaticKmsAlgorithm::Aes256,
    })
}

fn decode_minio_static_kms_field(field: &'static str, value: &str) -> Result<Vec<u8>, PersistedEncryptionError> {
    BASE64_STANDARD
        .decode(value)
        .map_err(|_| PersistedEncryptionError::InvalidMinioStaticKmsCiphertext {
            reason: format!("{field} must be valid Base64"),
        })
}

fn decode_minio_static_kms_array<const N: usize>(field: &'static str, value: &str) -> Result<[u8; N], PersistedEncryptionError> {
    decode_minio_static_kms_field(field, value)?.try_into().map_err(|_| {
        PersistedEncryptionError::InvalidMinioStaticKmsCiphertext {
            reason: format!("{field} must decode to exactly {N} bytes"),
        }
    })
}

fn marshal_minio_kms_context(context: &HashMap<String, String>) -> Vec<u8> {
    let mut entries: Vec<_> = context.iter().collect();
    entries.sort_by_key(|(key, _)| *key);
    let mut json = String::from("{");
    for (index, (key, value)) in entries.into_iter().enumerate() {
        if index > 0 {
            json.push(',');
        }
        push_minio_json_string(&mut json, key);
        json.push(':');
        push_minio_json_string(&mut json, value);
    }
    json.push('}');
    json.into_bytes()
}

fn push_minio_json_string(output: &mut String, value: &str) {
    output.push('"');
    for character in value.chars() {
        match character {
            '"' => output.push_str("\\\""),
            '\\' => output.push_str("\\\\"),
            '\n' => output.push_str("\\n"),
            '\r' => output.push_str("\\r"),
            '\t' => output.push_str("\\t"),
            '<' => output.push_str("\\u003c"),
            '>' => output.push_str("\\u003e"),
            '&' => output.push_str("\\u0026"),
            '\u{2028}' => output.push_str("\\u2028"),
            '\u{2029}' => output.push_str("\\u2029"),
            character if character <= '\u{1f}' => {
                const HEX: &[u8; 16] = b"0123456789abcdef";
                let byte = character as u8;
                output.push_str("\\u00");
                output.push(HEX[(byte >> 4) as usize] as char);
                output.push(HEX[(byte & 0x0f) as usize] as char);
            }
            character => output.push(character),
        }
    }
    output.push('"');
}

fn is_local_sse_s3_envelope(encrypted_dek: &[u8]) -> bool {
    let Ok(encoded) = std::str::from_utf8(encrypted_dek) else {
        return false;
    };
    let Some((nonce, ciphertext)) = encoded.split_once(':') else {
        return false;
    };
    use base64::{Engine, engine::general_purpose::STANDARD as BASE64_STANDARD};
    BASE64_STANDARD.decode(nonce).is_ok_and(|nonce| nonce.len() == 12)
        && BASE64_STANDARD
            .decode(ciphertext)
            .is_ok_and(|ciphertext| ciphertext.len() == 48)
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::{Engine, engine::general_purpose::STANDARD as BASE64_STANDARD};

    fn local_envelope() -> Vec<u8> {
        format!("{}:{}", BASE64_STANDARD.encode([1u8; 12]), BASE64_STANDARD.encode([2u8; 48])).into_bytes()
    }

    fn kms_envelope() -> Vec<u8> {
        serde_json::to_vec(&serde_json::json!({
            "key_id": "data-key",
            "master_key_id": "master-key",
            "key_spec": "AES_256",
            "encrypted_key": [1, 2, 3, 4],
            "nonce": [5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
            "encryption_context": {},
            "created_at": "2024-01-01T00:00:00+00:00"
        }))
        .expect("serialize KMS envelope fixture")
    }

    fn legacy_metadata(algorithm: Option<&str>, encrypted_dek: &[u8]) -> HashMap<String, String> {
        let mut metadata = HashMap::from([
            (INTERNAL_ENCRYPTION_KEY_HEADER.to_string(), BASE64_STANDARD.encode(encrypted_dek)),
            (INTERNAL_ENCRYPTION_IV_HEADER.to_string(), BASE64_STANDARD.encode([3u8; 12])),
        ]);
        if let Some(algorithm) = algorithm {
            metadata.insert(AMZ_SERVER_SIDE_ENCRYPTION.to_string(), algorithm.to_string());
        }
        metadata
    }

    fn minio_metadata(kms: bool, encrypted_dek: &[u8]) -> HashMap<String, String> {
        let object_key_header = if kms {
            MINIO_INTERNAL_ENCRYPTION_KMS_SEALED_KEY_HEADER
        } else {
            MINIO_INTERNAL_ENCRYPTION_S3_SEALED_KEY_HEADER
        };
        let mut metadata = HashMap::from([
            (object_key_header.to_string(), BASE64_STANDARD.encode([4u8; 64])),
            (
                MINIO_INTERNAL_ENCRYPTION_KMS_DATA_KEY_HEADER.to_string(),
                BASE64_STANDARD.encode(encrypted_dek),
            ),
            (MINIO_INTERNAL_ENCRYPTION_KMS_KEY_ID_HEADER.to_string(), "default".to_string()),
            (MINIO_INTERNAL_ENCRYPTION_IV_HEADER.to_string(), BASE64_STANDARD.encode([5u8; 32])),
            (
                MINIO_INTERNAL_ENCRYPTION_ALGORITHM_HEADER.to_string(),
                MINIO_INTERNAL_ENCRYPTION_SEAL_ALGORITHM.to_string(),
            ),
        ]);
        metadata.insert(
            AMZ_SERVER_SIDE_ENCRYPTION.to_string(),
            if kms { SSE_KMS_ALGORITHM } else { DEFAULT_SSE_ALGORITHM }.to_string(),
        );
        metadata
    }

    #[test]
    fn classifies_released_legacy_formats() {
        let local = local_envelope();
        let kms = kms_envelope();

        assert_eq!(
            classify_persisted_managed_encryption(&legacy_metadata(Some(DEFAULT_SSE_ALGORITHM), &local), &local)
                .expect("classify local Direct format"),
            PersistedManagedEncryption::LegacySseS3Local
        );
        assert_eq!(
            classify_persisted_managed_encryption(&legacy_metadata(Some(DEFAULT_SSE_ALGORITHM), &kms), &kms)
                .expect("classify legacy SSE-S3 KMS envelope"),
            PersistedManagedEncryption::LegacySseS3Kms
        );
        assert_eq!(
            classify_persisted_managed_encryption(
                &legacy_metadata(Some(SSE_KMS_ALGORITHM), b"opaque-kms-key"),
                b"opaque-kms-key"
            )
            .expect("classify opaque SSE-KMS envelope"),
            PersistedManagedEncryption::LegacySseKms
        );
    }

    #[test]
    fn minio_markers_override_envelope_shape() {
        let local = local_envelope();
        let kms = kms_envelope();

        assert_eq!(
            classify_persisted_managed_encryption(&minio_metadata(false, &kms), &kms)
                .expect("SSE-S3 marker selects MinIO SSE-S3"),
            PersistedManagedEncryption::MinioSseS3Kms
        );
        assert_eq!(
            classify_persisted_managed_encryption(&minio_metadata(true, &local), &local)
                .expect("SSE-KMS marker selects MinIO SSE-KMS"),
            PersistedManagedEncryption::MinioSseKmsKms
        );
    }

    #[test]
    fn accepts_minio_sse_s3_key_value_only_when_kms_pair_is_absent() {
        let local = local_envelope();
        let mut metadata = minio_metadata(false, &local);
        metadata.remove(MINIO_INTERNAL_ENCRYPTION_KMS_KEY_ID_HEADER);
        metadata.remove(MINIO_INTERNAL_ENCRYPTION_KMS_DATA_KEY_HEADER);

        assert_eq!(
            classify_persisted_managed_encryption(&metadata, &[]).expect("classify MinIO SSE-S3 K/V metadata"),
            PersistedManagedEncryption::MinioSseS3KeyValue
        );

        metadata.insert(MINIO_INTERNAL_ENCRYPTION_KMS_KEY_ID_HEADER.to_string(), String::new());
        metadata.insert(MINIO_INTERNAL_ENCRYPTION_KMS_DATA_KEY_HEADER.to_string(), String::new());
        assert_eq!(
            classify_persisted_managed_encryption(&metadata, &[]),
            Err(PersistedEncryptionError::MissingField {
                field: MINIO_INTERNAL_ENCRYPTION_KMS_KEY_ID_HEADER
            })
        );
    }

    #[test]
    fn rejects_minio_sse_kms_without_kms_pair() {
        let local = local_envelope();
        let mut metadata = minio_metadata(true, &local);
        metadata.remove(MINIO_INTERNAL_ENCRYPTION_KMS_KEY_ID_HEADER);
        metadata.remove(MINIO_INTERNAL_ENCRYPTION_KMS_DATA_KEY_HEADER);

        assert_eq!(
            classify_persisted_managed_encryption(&metadata, &[]),
            Err(PersistedEncryptionError::MissingField {
                field: MINIO_INTERNAL_ENCRYPTION_KMS_KEY_ID_HEADER
            })
        );
    }

    #[test]
    fn persisted_minio_provider_does_not_depend_on_runtime_kms_availability() {
        let static_kms = br#"{"aead":"AES-256-GCM-HMAC-SHA-256","iv":"AQEBAQEBAQEBAQEBAQEBAQ==","nonce":"AgICAgICAgICAgIC","bytes":"AwMDAw=="}"#;
        let opaque_kms = b"opaque-external-kms-ciphertext";

        assert_eq!(
            classify_persisted_managed_encryption(&minio_metadata(false, static_kms), static_kms)
                .expect("classify MinIO static KMS envelope")
                .provider(),
            ManagedDekProvider::Kms
        );
        assert_eq!(
            classify_persisted_managed_encryption(&minio_metadata(true, opaque_kms), opaque_kms)
                .expect("classify external KMS ciphertext")
                .provider(),
            ManagedDekProvider::Kms
        );

        let mut key_value = minio_metadata(false, opaque_kms);
        key_value.remove(MINIO_INTERNAL_ENCRYPTION_KMS_KEY_ID_HEADER);
        key_value.remove(MINIO_INTERNAL_ENCRYPTION_KMS_DATA_KEY_HEADER);
        assert_eq!(
            classify_persisted_managed_encryption(&key_value, &[])
                .expect("classify historical K/V provider")
                .provider(),
            ManagedDekProvider::MinioKeyValue
        );
    }

    #[test]
    fn decrypts_minio_static_kms_binary_aes_ciphertext() {
        let master_key = [0x31; 32];
        let plaintext_key = [0x52; 32];
        let iv = [0x63; 16];
        let nonce = [0x74; 12];
        let context = HashMap::from([("bucket".to_string(), "bucket/object".to_string())]);
        let mut mac = HmacSha256::new_from_slice(&master_key).expect("valid master key");
        mac.update(&iv);
        let sealing_key = mac.finalize().into_bytes();
        let ciphertext = Aes256Gcm::new_from_slice(sealing_key.as_slice())
            .expect("valid sealing key")
            .encrypt(
                &Nonce::from(nonce),
                Payload {
                    msg: &plaintext_key,
                    aad: &marshal_minio_kms_context(&context),
                },
            )
            .expect("encrypt fixture");
        let mut envelope = ciphertext;
        envelope.extend_from_slice(&iv);
        envelope.extend_from_slice(&nonce);
        let configured_key = format!("minio-key:{}", BASE64_STANDARD.encode(master_key));

        temp_env::with_var(MINIO_STATIC_KMS_KEY_ENV, Some(configured_key), || {
            assert_eq!(
                decrypt_minio_static_kms_dek("minio-key", &envelope, &context).expect("decrypt binary ciphertext"),
                Some(plaintext_key)
            );
            assert_eq!(
                decrypt_minio_static_kms_dek("external-key", &envelope, &context)
                    .expect("different key ID must remain external KMS"),
                None
            );
        });
    }

    #[test]
    fn decrypts_official_minio_legacy_chacha20_ciphertext() {
        let configured_key = "my-key:eEm+JI9/q4JhH8QwKvf3LKo4DEBl6QbfvAl1CAbMIv8=";
        let ciphertext = br#"{"aead":"ChaCha20Poly1305","iv":"JbI+vwvYww1lCb5VpkAFuQ==","nonce":"ARjIjJxBSD541Gz8","bytes":"KCbEc2sA0TLvA7aWTWa23AdccVfJMpOxwgG8hm+4PaNrxYfy1xFWZg2gEenVrOgv"}"#;
        let expected: [u8; 32] = BASE64_STANDARD
            .decode("zmS7NrG765UZ0ZN85oPjybelxqVvpz01vxsSpOISy2M=")
            .expect("decode official plaintext")
            .try_into()
            .expect("official plaintext is 32 bytes");

        temp_env::with_var(MINIO_STATIC_KMS_KEY_ENV, Some(configured_key), || {
            assert_eq!(
                decrypt_minio_static_kms_dek("my-key", ciphertext, &HashMap::new()).expect("decrypt official MinIO ciphertext"),
                Some(expected)
            );
        });
    }

    #[test]
    fn rejects_malformed_config_and_unknown_legacy_fields() {
        temp_env::with_var(MINIO_STATIC_KMS_KEY_ENV, Some("missing-separator"), || {
            assert!(matches!(
                decrypt_minio_static_kms_dek("my-key", b"ciphertext", &HashMap::new()),
                Err(PersistedEncryptionError::InvalidMinioStaticKmsConfiguration { .. })
            ));
        });

        let configured_key = format!("my-key:{}", BASE64_STANDARD.encode([0x31; 32]));
        let ciphertext =
            br#"{"aead":"AES-256-GCM-HMAC-SHA-256","iv":"Y2NjY2NjY2NjY2NjY2NjYw==","nonce":"dHR0dHR0dHR0dHR0","bytes":"AA==","extra":true}"#;
        temp_env::with_var(MINIO_STATIC_KMS_KEY_ENV, Some(configured_key), || {
            assert!(matches!(
                decrypt_minio_static_kms_dek("my-key", ciphertext, &HashMap::new()),
                Err(PersistedEncryptionError::InvalidMinioStaticKmsCiphertext { .. })
            ));
        });
    }

    #[test]
    fn rejects_all_zero_minio_static_kms_key() {
        let configured_key = format!("my-key:{}", BASE64_STANDARD.encode([0u8; 32]));
        temp_env::with_var(MINIO_STATIC_KMS_KEY_ENV, Some(configured_key), || {
            assert!(matches!(
                decrypt_minio_static_kms_dek("my-key", b"ciphertext", &HashMap::new()),
                Err(PersistedEncryptionError::InvalidMinioStaticKmsConfiguration { .. })
            ));
        });
    }

    #[test]
    fn marshals_minio_kms_context_with_go_json_escaping() {
        let context = HashMap::from([
            ("z".to_string(), "line\n".to_string()),
            ("<\u{8}".to_string(), "&\u{2028}".to_string()),
        ]);

        assert_eq!(marshal_minio_kms_context(&context), br#"{"\u003c\u0008":"\u0026\u2028","z":"line\n"}"#);
    }

    #[test]
    fn rejects_conflicting_or_partial_minio_metadata() {
        let local = local_envelope();
        let mut both = minio_metadata(false, &local);
        both.insert(
            MINIO_INTERNAL_ENCRYPTION_KMS_SEALED_KEY_HEADER.to_string(),
            BASE64_STANDARD.encode([6u8; 64]),
        );
        assert_eq!(
            classify_persisted_managed_encryption(&both, &local),
            Err(PersistedEncryptionError::ConflictingMinioMarkers)
        );

        let mut wrong_scheme = minio_metadata(true, &local);
        wrong_scheme.insert(AMZ_SERVER_SIDE_ENCRYPTION.to_string(), DEFAULT_SSE_ALGORITHM.to_string());
        assert_eq!(
            classify_persisted_managed_encryption(&wrong_scheme, &local),
            Err(PersistedEncryptionError::SchemeConflict)
        );

        let mut partial = minio_metadata(false, &local);
        partial.remove(MINIO_INTERNAL_ENCRYPTION_IV_HEADER);
        assert_eq!(
            classify_persisted_managed_encryption(&partial, &local),
            Err(PersistedEncryptionError::MissingField {
                field: MINIO_INTERNAL_ENCRYPTION_IV_HEADER
            })
        );

        let mut missing_key_id = minio_metadata(false, &local);
        missing_key_id.remove(MINIO_INTERNAL_ENCRYPTION_KMS_KEY_ID_HEADER);
        assert_eq!(
            classify_persisted_managed_encryption(&missing_key_id, &local),
            Err(PersistedEncryptionError::MissingField {
                field: MINIO_INTERNAL_ENCRYPTION_KMS_KEY_ID_HEADER
            })
        );

        let mut missing_data_key = minio_metadata(false, &local);
        missing_data_key.remove(MINIO_INTERNAL_ENCRYPTION_KMS_DATA_KEY_HEADER);
        assert_eq!(
            classify_persisted_managed_encryption(&missing_data_key, &local),
            Err(PersistedEncryptionError::MissingField {
                field: MINIO_INTERNAL_ENCRYPTION_KMS_DATA_KEY_HEADER
            })
        );

        let mut unknown_algorithm = minio_metadata(false, &local);
        unknown_algorithm.insert(
            MINIO_INTERNAL_ENCRYPTION_ALGORITHM_HEADER.to_string(),
            "future-seal-algorithm".to_string(),
        );
        assert_eq!(
            classify_persisted_managed_encryption(&unknown_algorithm, &local),
            Err(PersistedEncryptionError::UnsupportedSealAlgorithm {
                algorithm: "future-seal-algorithm".to_string()
            })
        );

        for field in [
            MINIO_INTERNAL_ENCRYPTION_S3_SEALED_KEY_HEADER,
            MINIO_INTERNAL_ENCRYPTION_KMS_DATA_KEY_HEADER,
        ] {
            let mut empty = minio_metadata(false, &local);
            empty.insert(field.to_string(), String::new());
            assert_eq!(
                classify_persisted_managed_encryption(&empty, &local),
                Err(PersistedEncryptionError::MissingField { field })
            );
        }

        for field in [
            MINIO_INTERNAL_ENCRYPTION_ALGORITHM_HEADER,
            MINIO_INTERNAL_ENCRYPTION_KMS_DATA_KEY_HEADER,
        ] {
            let metadata = HashMap::from([(field.to_string(), "orphaned".to_string())]);
            assert_eq!(
                classify_persisted_managed_encryption(&metadata, &local),
                Err(PersistedEncryptionError::MissingField {
                    field: MINIO_INTERNAL_ENCRYPTION_S3_SEALED_KEY_HEADER
                })
            );
        }
    }

    #[test]
    fn rejects_unknown_legacy_envelope_and_conflicting_alias() {
        let unknown = b"not-an-envelope";
        assert_eq!(
            classify_persisted_managed_encryption(&legacy_metadata(Some(DEFAULT_SSE_ALGORITHM), unknown), unknown),
            Err(PersistedEncryptionError::UnknownLegacyEnvelope)
        );

        let local = local_envelope();
        let mut conflicting = minio_metadata(false, &local);
        conflicting.insert(INTERNAL_ENCRYPTION_KEY_HEADER.to_string(), BASE64_STANDARD.encode(b"different"));
        assert_eq!(
            classify_persisted_managed_encryption(&conflicting, &local),
            Err(PersistedEncryptionError::ConflictingEncryptedDataKey)
        );

        let mut conflicting_key_id = minio_metadata(false, &local);
        conflicting_key_id.insert(INTERNAL_ENCRYPTION_KEY_ID_HEADER.to_string(), "different-key".to_string());
        assert_eq!(
            classify_persisted_managed_encryption(&conflicting_key_id, &local),
            Err(PersistedEncryptionError::ConflictingKmsKeyId)
        );

        assert_eq!(
            classify_persisted_managed_encryption(&legacy_metadata(Some(SSE_KMS_ALGORITHM), &local), &local),
            Err(PersistedEncryptionError::SchemeConflict)
        );
    }

    #[test]
    fn accepts_case_insensitive_minio_markers_and_rejects_conflicting_duplicates() {
        let local = local_envelope();
        let mut metadata = minio_metadata(false, &local);
        let sealed_key = metadata
            .remove(MINIO_INTERNAL_ENCRYPTION_S3_SEALED_KEY_HEADER)
            .expect("MinIO fixture contains the sealed-key marker");
        metadata.insert(MINIO_INTERNAL_ENCRYPTION_S3_SEALED_KEY_HEADER.to_ascii_lowercase(), sealed_key);

        assert_eq!(
            classify_persisted_managed_encryption(&metadata, &local).expect("classify mixed-case MinIO metadata"),
            PersistedManagedEncryption::MinioSseS3Kms
        );

        metadata.insert(
            MINIO_INTERNAL_ENCRYPTION_S3_SEALED_KEY_HEADER.to_string(),
            BASE64_STANDARD.encode([0x77u8; 64]),
        );
        assert_eq!(
            classify_persisted_managed_encryption(&metadata, &local),
            Err(PersistedEncryptionError::ConflictingValue {
                field: MINIO_INTERNAL_ENCRYPTION_S3_SEALED_KEY_HEADER
            })
        );
    }
}
