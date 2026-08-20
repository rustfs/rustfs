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

//! Data Encryption Key (DEK) encryption interface and implementations
//!
//! This module provides a unified interface for encrypting and decrypting
//! data encryption keys using master keys. It abstracts the encryption
//! operations so that different backends can share the same encryption logic.

use crate::error::{KmsError, Result};
use crate::persisted_observability::{BoundedUnknownFieldName, UnknownFieldSummary};
use async_trait::async_trait;
use jiff::Zoned;
use rand::Rng;
use serde::de::{self, IgnoredAny, MapAccess, Visitor};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};

impl UnknownFieldSummary {
    fn record_for_data_key_envelope(&self) {
        let Some((field, field_name_truncated, field_count)) = self.record("data-key-envelope") else {
            return;
        };

        static RECORDS_WITH_UNKNOWN_FIELDS: AtomicU64 = AtomicU64::new(0);
        let observed_records = RECORDS_WITH_UNKNOWN_FIELDS.fetch_add(1, Ordering::Relaxed).saturating_add(1);
        if observed_records.is_power_of_two() {
            tracing::warn!(
                field = ?field,
                field_name_truncated,
                field_count,
                observed_records,
                "KMS data-key envelope contains unknown fields"
            );
        }
    }
}

/// Data key envelope for encrypting/decrypting data keys
///
/// This structure stores the encrypted DEK along with metadata needed for decryption.
/// The `master_key_version` field records which version of the KEK (Key Encryption Key)
/// wrapped this DEK so rotation-aware backends can load the matching historical
/// material. Envelopes written before versioning carry `None`; backends must resolve
/// `None` to a deterministic baseline version recorded in key metadata, never
/// implicitly to whatever version is current.
#[derive(Debug, Clone, Serialize)]
pub struct DataKeyEnvelope {
    pub key_id: String,
    pub master_key_id: String,
    pub key_spec: String,
    pub encrypted_key: Vec<u8>,
    pub nonce: Vec<u8>,
    pub encryption_context: HashMap<String, String>,
    #[serde(with = "crate::time_serde::zoned")]
    pub created_at: Zoned,
    /// KEK version that wrapped `encrypted_key`; `None` on pre-versioning envelopes.
    ///
    /// Optional and omitted when `None` so envelopes from non-rotating backends stay
    /// byte-identical to the historical seven-field JSON shape.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub master_key_version: Option<u32>,
}

impl<'de> Deserialize<'de> for DataKeyEnvelope {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        enum Field {
            KeyId,
            MasterKeyId,
            KeySpec,
            EncryptedKey,
            Nonce,
            EncryptionContext,
            CreatedAt,
            MasterKeyVersion,
            Unknown(BoundedUnknownFieldName),
        }

        impl<'de> Deserialize<'de> for Field {
            fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct FieldVisitor;

                impl Visitor<'_> for FieldVisitor {
                    type Value = Field;

                    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                        formatter.write_str("a KMS data-key envelope field name")
                    }

                    fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
                    where
                        E: de::Error,
                    {
                        Ok(match value {
                            "key_id" => Field::KeyId,
                            "master_key_id" => Field::MasterKeyId,
                            "key_spec" => Field::KeySpec,
                            "encrypted_key" => Field::EncryptedKey,
                            "nonce" => Field::Nonce,
                            "encryption_context" => Field::EncryptionContext,
                            "created_at" => Field::CreatedAt,
                            "master_key_version" => Field::MasterKeyVersion,
                            _ => Field::Unknown(BoundedUnknownFieldName::new(value)),
                        })
                    }
                }

                deserializer.deserialize_identifier(FieldVisitor)
            }
        }

        #[derive(Deserialize)]
        struct ZonedValue(#[serde(with = "crate::time_serde::zoned")] Zoned);

        struct DataKeyEnvelopeVisitor;

        impl<'de> Visitor<'de> for DataKeyEnvelopeVisitor {
            type Value = DataKeyEnvelope;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a KMS data-key envelope")
            }

            fn visit_map<A>(self, mut map: A) -> std::result::Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                macro_rules! read_field {
                    ($slot:ident, $name:literal) => {{
                        if $slot.is_some() {
                            return Err(de::Error::duplicate_field($name));
                        }
                        $slot = Some(map.next_value()?);
                    }};
                }

                let mut key_id = None;
                let mut master_key_id = None;
                let mut key_spec = None;
                let mut encrypted_key = None;
                let mut nonce = None;
                let mut encryption_context = None;
                let mut created_at: Option<ZonedValue> = None;
                let mut master_key_version = None;
                let mut unknown_fields = UnknownFieldSummary::default();

                while let Some(field) = map.next_key()? {
                    match field {
                        Field::KeyId => read_field!(key_id, "key_id"),
                        Field::MasterKeyId => read_field!(master_key_id, "master_key_id"),
                        Field::KeySpec => read_field!(key_spec, "key_spec"),
                        Field::EncryptedKey => read_field!(encrypted_key, "encrypted_key"),
                        Field::Nonce => read_field!(nonce, "nonce"),
                        Field::EncryptionContext => read_field!(encryption_context, "encryption_context"),
                        Field::CreatedAt => read_field!(created_at, "created_at"),
                        Field::MasterKeyVersion => read_field!(master_key_version, "master_key_version"),
                        Field::Unknown(field) => {
                            let _: IgnoredAny = map.next_value()?;
                            unknown_fields.observe(field);
                        }
                    }
                }

                let envelope = DataKeyEnvelope {
                    key_id: key_id.ok_or_else(|| de::Error::missing_field("key_id"))?,
                    master_key_id: master_key_id.ok_or_else(|| de::Error::missing_field("master_key_id"))?,
                    key_spec: key_spec.ok_or_else(|| de::Error::missing_field("key_spec"))?,
                    encrypted_key: encrypted_key.ok_or_else(|| de::Error::missing_field("encrypted_key"))?,
                    nonce: nonce.ok_or_else(|| de::Error::missing_field("nonce"))?,
                    encryption_context: encryption_context.ok_or_else(|| de::Error::missing_field("encryption_context"))?,
                    created_at: created_at.ok_or_else(|| de::Error::missing_field("created_at"))?.0,
                    master_key_version: master_key_version.unwrap_or(None),
                };
                unknown_fields.record_for_data_key_envelope();
                Ok(envelope)
            }
        }

        const FIELDS: &[&str] = &[
            "key_id",
            "master_key_id",
            "key_spec",
            "encrypted_key",
            "nonce",
            "encryption_context",
            "created_at",
            "master_key_version",
        ];
        deserializer.deserialize_struct("DataKeyEnvelope", FIELDS, DataKeyEnvelopeVisitor)
    }
}

#[derive(Deserialize)]
struct DataKeyEnvelopeMarker {
    #[serde(rename = "key_id")]
    _key_id: IgnoredAny,
    #[serde(rename = "master_key_id")]
    _master_key_id: IgnoredAny,
    #[serde(rename = "key_spec")]
    _key_spec: IgnoredAny,
    #[serde(rename = "encrypted_key")]
    _encrypted_key: IgnoredAny,
    #[serde(rename = "nonce")]
    _nonce: IgnoredAny,
    #[serde(rename = "encryption_context")]
    _encryption_context: IgnoredAny,
    #[serde(rename = "created_at")]
    _created_at: IgnoredAny,
}

/// Serialize an encryption context into deterministic AAD bytes.
///
/// The AAD has to be reproducible byte-for-byte at decrypt time. A `HashMap`
/// serializes in its own iteration order, which differs between instances — so
/// a context rebuilt from storage (or from headers) would produce different
/// bytes than the one used to seal, and the sealed data would never open
/// again. Ordering by key removes that dependency.
///
/// Shared by every layer that binds a context as additional data. It lives
/// here rather than beside one caller because a second, subtly different copy
/// is exactly how the object layer ended up serializing a `HashMap` directly
/// while the Static backend was already canonicalizing.
pub fn context_aad(context: &HashMap<String, String>) -> Result<Vec<u8>> {
    let canonical: BTreeMap<&str, &str> = context.iter().map(|(key, value)| (key.as_str(), value.as_str())).collect();
    serde_json::to_vec(&canonical).map_err(Into::into)
}

/// Returns whether ciphertext is a RustFS KMS data-key envelope.
pub fn is_data_key_envelope(ciphertext: &[u8]) -> bool {
    ciphertext.iter().copied().find(|byte| !byte.is_ascii_whitespace()) == Some(b'{')
        && serde_json::from_slice::<DataKeyEnvelopeMarker>(ciphertext).is_ok()
}

/// Trait for encrypting and decrypting data encryption keys (DEK)
///
/// This trait abstracts the encryption operations used to protect
/// data encryption keys with master keys. Different implementations
/// can use different encryption algorithms (e.g., AES-256-GCM).
#[async_trait]
pub trait DekCrypto: Send + Sync {
    /// Encrypt plaintext data using a master key material
    ///
    /// # Arguments
    /// * `key_material` - The master key material (raw bytes)
    /// * `plaintext` - The data to encrypt
    ///
    /// # Returns
    /// A tuple of (ciphertext, nonce) where:
    /// - `ciphertext` - The encrypted data
    /// - `nonce` - The nonce used for encryption (should be stored with ciphertext)
    async fn encrypt(&self, key_material: &[u8], plaintext: &[u8]) -> Result<(Vec<u8>, Vec<u8>)>;

    /// Decrypt ciphertext data using a master key material
    ///
    /// # Arguments
    /// * `key_material` - The master key material (raw bytes)
    /// * `ciphertext` - The encrypted data
    /// * `nonce` - The nonce used for encryption
    ///
    /// # Returns
    /// The decrypted plaintext data
    async fn decrypt(&self, key_material: &[u8], ciphertext: &[u8], nonce: &[u8]) -> Result<Vec<u8>>;

    /// Get the algorithm name used by this implementation
    #[allow(dead_code)] // May be used by implementations or for debugging
    fn algorithm(&self) -> &'static str;

    /// Get the required key material size in bytes
    #[allow(dead_code)] // May be used by implementations or for debugging
    fn key_size(&self) -> usize;
}

/// AES-256-GCM implementation of DEK encryption
pub struct AesDekCrypto;

impl AesDekCrypto {
    /// Create a new AES-256-GCM DEK crypto instance
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl DekCrypto for AesDekCrypto {
    async fn encrypt(&self, key_material: &[u8], plaintext: &[u8]) -> Result<(Vec<u8>, Vec<u8>)> {
        use aes_gcm::{
            Aes256Gcm, Key, Nonce,
            aead::{Aead, KeyInit},
        };

        // Validate key material length
        if key_material.len() != 32 {
            return Err(KmsError::cryptographic_error(
                "key",
                format!("Invalid key length: expected 32 bytes, got {}", key_material.len()),
            ));
        }

        // Create cipher from key material
        let key =
            Key::<Aes256Gcm>::try_from(key_material).map_err(|_| KmsError::cryptographic_error("key", "Invalid key length"))?;
        let cipher = Aes256Gcm::new(&key);

        // Generate random nonce (12 bytes for GCM)
        let mut nonce_bytes = [0u8; 12];
        rand::rng().fill_bytes(&mut nonce_bytes);
        let nonce = Nonce::from(nonce_bytes);

        // Encrypt plaintext
        let ciphertext = cipher
            .encrypt(&nonce, plaintext)
            .map_err(|e| KmsError::cryptographic_error("encrypt", e.to_string()))?;

        Ok((ciphertext, nonce_bytes.to_vec()))
    }

    async fn decrypt(&self, key_material: &[u8], ciphertext: &[u8], nonce: &[u8]) -> Result<Vec<u8>> {
        use aes_gcm::{
            Aes256Gcm, Key, Nonce,
            aead::{Aead, KeyInit},
        };

        // Validate nonce length
        if nonce.len() != 12 {
            return Err(KmsError::cryptographic_error("nonce", "Invalid nonce length: expected 12 bytes"));
        }

        // Validate key material length
        if key_material.len() != 32 {
            return Err(KmsError::cryptographic_error(
                "key",
                format!("Invalid key length: expected 32 bytes, got {}", key_material.len()),
            ));
        }

        // Create cipher from key material
        let key =
            Key::<Aes256Gcm>::try_from(key_material).map_err(|_| KmsError::cryptographic_error("key", "Invalid key length"))?;
        let cipher = Aes256Gcm::new(&key);

        // Convert nonce
        let mut nonce_array = [0u8; 12];
        nonce_array.copy_from_slice(nonce);
        let nonce_ref = Nonce::from(nonce_array);

        // Decrypt ciphertext
        let plaintext = cipher
            .decrypt(&nonce_ref, ciphertext)
            .map_err(|e| KmsError::cryptographic_error("decrypt", e.to_string()))?;

        Ok(plaintext)
    }

    #[allow(dead_code)] // Trait method, may be used by implementations
    fn algorithm(&self) -> &'static str {
        "AES-256-GCM"
    }

    #[allow(dead_code)] // Trait method, may be used by implementations
    fn key_size(&self) -> usize {
        32 // 256 bits
    }
}

impl Default for AesDekCrypto {
    fn default() -> Self {
        Self::new()
    }
}

/// Generate random key material for the given algorithm.
///
/// The lengths must track [`crate::types::KeySpec::key_size`].
///
/// # Arguments
/// * `algorithm` - The key algorithm (e.g., "AES_256", "AES_128", "ChaCha20")
///
/// # Returns
/// A vector containing the generated key material
pub fn generate_key_material(algorithm: &str) -> Result<Vec<u8>> {
    let key_size = match algorithm {
        "AES_256" | "ChaCha20" => 32,
        "AES_128" => 16,
        _ => return Err(KmsError::unsupported_algorithm(algorithm)),
    };

    let mut key_material = vec![0u8; key_size];
    rand::rng().fill_bytes(&mut key_material);
    Ok(key_material)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{deserialize_with_ignored_only_unknown, unknown_field_metric};
    use metrics_util::debugging::DebuggingRecorder;

    #[tokio::test]
    async fn test_aes_dek_crypto_encrypt_decrypt() {
        let crypto = AesDekCrypto::new();

        // Generate test key material
        let key_material = generate_key_material("AES_256").expect("Failed to generate key material");
        let plaintext = b"Hello, World! This is a test message.";

        // Test encryption
        let (ciphertext, nonce) = crypto
            .encrypt(&key_material, plaintext)
            .await
            .expect("Encryption should succeed");

        assert!(!ciphertext.is_empty());
        assert_eq!(nonce.len(), 12);
        assert_ne!(ciphertext, plaintext);

        // Test decryption
        let decrypted = crypto
            .decrypt(&key_material, &ciphertext, &nonce)
            .await
            .expect("Decryption should succeed");

        assert_eq!(decrypted, plaintext);
    }

    #[tokio::test]
    async fn test_aes_dek_crypto_invalid_key_size() {
        let crypto = AesDekCrypto::new();
        let invalid_key = vec![0u8; 16]; // Too short
        let plaintext = b"test";

        let result = crypto.encrypt(&invalid_key, plaintext).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_aes_dek_crypto_invalid_nonce() {
        let crypto = AesDekCrypto::new();
        let key_material = generate_key_material("AES_256").expect("Failed to generate key material");
        let ciphertext = vec![0u8; 16];
        let invalid_nonce = vec![0u8; 8]; // Too short

        let result = crypto.decrypt(&key_material, &ciphertext, &invalid_nonce).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_generate_key_material() {
        let key_256 = generate_key_material("AES_256").expect("Should generate AES_256 key");
        assert_eq!(key_256.len(), 32);

        let key_128 = generate_key_material("AES_128").expect("Should generate AES_128 key");
        assert_eq!(key_128.len(), 16);

        // Keys should be different
        let key_256_2 = generate_key_material("AES_256").expect("Should generate AES_256 key");
        assert_ne!(key_256, key_256_2);

        // Invalid algorithm
        assert!(generate_key_material("INVALID").is_err());
    }

    #[tokio::test]
    async fn test_data_key_envelope_serialization() {
        let envelope = DataKeyEnvelope {
            key_id: "test-key-id".to_string(),
            master_key_id: "master-key-id".to_string(),
            key_spec: "AES_256".to_string(),
            encrypted_key: vec![1, 2, 3, 4],
            nonce: vec![5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
            encryption_context: {
                let mut map = HashMap::new();
                map.insert("bucket".to_string(), "test-bucket".to_string());
                map
            },
            created_at: Zoned::now(),
            master_key_version: None,
        };

        // Test serialization
        let serialized = serde_json::to_vec(&envelope).expect("Serialization should succeed");
        assert!(!serialized.is_empty());

        // Test deserialization
        let deserialized: DataKeyEnvelope = serde_json::from_slice(&serialized).expect("Deserialization should succeed");
        assert_eq!(deserialized.key_id, envelope.key_id);
        assert_eq!(deserialized.master_key_id, envelope.master_key_id);
        assert_eq!(deserialized.encrypted_key, envelope.encrypted_key);
    }

    #[tokio::test]
    async fn test_data_key_envelope_backward_compatibility() {
        // Test deserialization with current Zoned format (with timezone annotation)
        let envelope_json = r#"{
            "key_id": "test-key-id",
            "master_key_id": "master-key-id",
            "key_spec": "AES_256",
            "encrypted_key": [1, 2, 3, 4],
            "nonce": [5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
            "encryption_context": {"bucket": "test-bucket"},
            "created_at": "2024-01-01T00:00:00+00:00[UTC]"
        }"#;

        let deserialized: DataKeyEnvelope = serde_json::from_str(envelope_json).expect("Should deserialize current format");
        assert_eq!(deserialized.key_id, "test-key-id");
        assert_eq!(deserialized.master_key_id, "master-key-id");
        // Envelopes persisted before versioning must parse with no master key version.
        assert_eq!(deserialized.master_key_version, None);
    }

    #[tokio::test]
    async fn test_data_key_envelope_accepts_legacy_rfc3339_timestamp() {
        let envelope_json = r#"{
            "key_id": "test-key-id",
            "master_key_id": "master-key-id",
            "key_spec": "AES_256",
            "encrypted_key": [1, 2, 3, 4],
            "nonce": [5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
            "encryption_context": {"bucket": "test-bucket"},
            "created_at": "2024-01-01T00:00:00+00:00"
        }"#;

        let deserialized: DataKeyEnvelope = serde_json::from_str(envelope_json).expect("Should deserialize legacy format");
        assert_eq!(deserialized.key_id, "test-key-id");
        assert_eq!(deserialized.master_key_id, "master-key-id");
        assert_eq!(deserialized.master_key_version, None);
    }

    #[test]
    fn test_data_key_envelope_unknown_fields_remain_readable() {
        const UNKNOWN_FIELD_VALUE: &str = "field value must not be logged";
        let envelope = serde_json::json!({
            "key_id": "test-key-id",
            "master_key_id": "master-key-id",
            "key_spec": "AES_256",
            "encrypted_key": [1, 2, 3, 4],
            "nonce": [5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
            "encryption_context": {"bucket": "test-bucket"},
            "created_at": "2024-01-01T00:00:00+00:00[UTC]"
        });
        let long_field = format!("{}界", "a".repeat(126));
        let long_prefix = "a".repeat(126);
        let injection_field = "b\n\u{1b}[31m";

        let record_with_unknown = |field: &str| {
            let mut record = envelope.clone();
            let object = record.as_object_mut().expect("envelope is an object");
            object.insert(field.to_owned(), serde_json::json!(UNKNOWN_FIELD_VALUE));
            object.insert("zeta_extension".to_owned(), serde_json::json!("another value must not be logged"));
            serde_json::to_vec(&record).expect("encode envelope with unknown fields")
        };
        let long_record = record_with_unknown(&long_field);
        let injection_record = record_with_unknown(injection_field);
        let logs = crate::test_support::CapturedLogs::default();
        let subscriber = tracing_subscriber::fmt()
            .with_ansi(false)
            .with_max_level(tracing::Level::WARN)
            .with_writer(logs.clone())
            .finish();
        let dispatch = tracing::Dispatch::new(subscriber);
        let parse = |record: &[u8]| {
            let recorder = DebuggingRecorder::new();
            let envelope = metrics::with_local_recorder(&recorder, || {
                tracing::dispatcher::with_default(&dispatch, || {
                    serde_json::from_slice(record).expect("unknown fields must remain readable")
                })
            });
            assert_eq!(unknown_field_metric(&recorder, "data-key-envelope"), 2);
            envelope
        };
        let deserialized: DataKeyEnvelope = parse(&long_record);
        let _: DataKeyEnvelope = parse(&long_record);
        let _: DataKeyEnvelope = parse(&injection_record);
        let _: DataKeyEnvelope = parse(&injection_record);
        assert_eq!(deserialized.key_id, "test-key-id");
        assert_eq!(deserialized.master_key_version, None);

        let output = logs.output();
        assert!(output.contains("WARN"));
        assert_eq!(output.matches("KMS data-key envelope contains unknown fields").count(), 3);
        assert!(output.contains(&long_prefix));
        assert!(!output.contains(&long_field));
        assert!(output.contains("field_name_truncated=true"));
        assert!(output.contains(r#"\n\u{1b}[31m"#));
        assert!(!output.contains("zeta_extension"));
        assert!(output.contains("field_count=2"));
        for observed_records in [1, 2, 4] {
            assert!(output.contains(&format!("observed_records={observed_records}")));
        }
        assert!(!output.contains("observed_records=3"));
        assert!(!output.contains(UNKNOWN_FIELD_VALUE));
        assert!(!output.contains("another value must not be logged"));

        let streamed: DataKeyEnvelope = deserialize_with_ignored_only_unknown(envelope, "stream_only_extension")
            .expect("unknown values must be consumed through deserialize_ignored_any");
        assert_eq!(streamed.key_id, "test-key-id");
    }

    #[test]
    fn test_data_key_envelope_none_version_serializes_without_field() {
        // A `None` version must keep the serialized envelope on the historical
        // seven-field JSON shape so non-rotating backends emit byte-compatible
        // envelopes that older readers accept unchanged.
        let envelope = DataKeyEnvelope {
            key_id: "test-key-id".to_string(),
            master_key_id: "master-key-id".to_string(),
            key_spec: "AES_256".to_string(),
            encrypted_key: vec![1, 2, 3, 4],
            nonce: vec![5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
            encryption_context: HashMap::new(),
            created_at: Zoned::now(),
            master_key_version: None,
        };

        let value = serde_json::to_value(&envelope).expect("serialize envelope");
        let object = value.as_object().expect("envelope serializes to an object");
        assert!(!object.contains_key("master_key_version"));
        assert_eq!(object.len(), 7, "None version must not change the seven-field JSON shape");
    }

    #[test]
    fn test_data_key_envelope_version_round_trip() {
        let envelope = DataKeyEnvelope {
            key_id: "test-key-id".to_string(),
            master_key_id: "master-key-id".to_string(),
            key_spec: "AES_256".to_string(),
            encrypted_key: vec![1, 2, 3, 4],
            nonce: vec![5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
            encryption_context: HashMap::new(),
            created_at: Zoned::now(),
            master_key_version: Some(7),
        };

        let serialized = serde_json::to_vec(&envelope).expect("serialize envelope");
        let value: serde_json::Value = serde_json::from_slice(&serialized).expect("parse serialized envelope");
        assert_eq!(value.get("master_key_version"), Some(&serde_json::json!(7)));

        let deserialized: DataKeyEnvelope = serde_json::from_slice(&serialized).expect("deserialize envelope");
        assert_eq!(deserialized.master_key_version, Some(7));
    }

    #[test]
    fn test_data_key_envelope_discriminator_rejects_local_formats() {
        let kms_envelope = br#"{
            "key_id": "test-key-id",
            "master_key_id": "master-key-id",
            "key_spec": "AES_256",
            "encrypted_key": [1, 2, 3, 4],
            "nonce": [5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
            "encryption_context": {},
            "created_at": "2024-01-01T00:00:00+00:00"
        }"#;
        let minio_legacy = br#"{"aead":"AES-256-GCM-HMAC-SHA-256","iv":[1],"nonce":[2],"bytes":[3]}"#;
        let duplicate_key_id = [b"{\"key_id\":\"duplicate\",".as_slice(), &kms_envelope[1..]].concat();
        // Rotation-aware envelope: the optional master_key_version field must not
        // change how mixed batches of old and new envelopes are routed.
        let versioned_envelope = {
            let mut value: serde_json::Value = serde_json::from_slice(kms_envelope).expect("parse KMS envelope fixture");
            value
                .as_object_mut()
                .expect("KMS envelope fixture is an object")
                .insert("master_key_version".to_string(), serde_json::json!(2));
            serde_json::to_vec(&value).expect("serialize versioned envelope")
        };

        assert!(is_data_key_envelope(kms_envelope));
        assert!(is_data_key_envelope(&versioned_envelope));
        assert!(is_data_key_envelope(&[b" \n".as_slice(), kms_envelope].concat()));
        assert!(!is_data_key_envelope(&duplicate_key_id));
        assert!(!is_data_key_envelope(b"bm9uY2U=:Y2lwaGVydGV4dA=="));
        assert!(!is_data_key_envelope(minio_legacy));

        let envelope_value: serde_json::Value = serde_json::from_slice(kms_envelope).expect("parse KMS envelope fixture");
        for required_field in [
            "key_id",
            "master_key_id",
            "key_spec",
            "encrypted_key",
            "nonce",
            "encryption_context",
            "created_at",
        ] {
            let mut incomplete = envelope_value.clone();
            incomplete
                .as_object_mut()
                .expect("KMS envelope fixture is an object")
                .remove(required_field);
            assert!(
                !is_data_key_envelope(&serde_json::to_vec(&incomplete).expect("serialize incomplete envelope")),
                "missing {required_field} must not classify as a KMS envelope"
            );
        }
    }
}
