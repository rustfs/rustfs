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

//! Core type definitions for KMS operations

use jiff::Zoned;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;
use zeroize::Zeroize;

/// Data encryption key (DEK) used for encrypting object data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataKeyInfo {
    /// Key identifier
    pub key_id: String,
    /// Key version
    pub version: u32,
    /// Plaintext key material (only available during generation)
    /// SECURITY: This field is manually zeroed when dropped
    pub plaintext: Option<Vec<u8>>,
    /// Encrypted key material (ciphertext)
    pub ciphertext: Vec<u8>,
    /// Key algorithm specification
    pub key_spec: String,
    /// Associated metadata
    pub metadata: HashMap<String, String>,
    /// Key creation timestamp
    pub created_at: Zoned,
}

impl DataKeyInfo {
    /// Create a new data key
    ///
    /// # Arguments
    /// * `key_id` - Unique identifier for the key
    /// * `version` - Key version number
    /// * `plaintext` - Optional plaintext key material
    /// * `ciphertext` - Encrypted key material
    /// * `key_spec` - Key specification (e.g., "AES_256")
    ///
    /// # Returns
    /// A new `DataKey` instance
    ///
    pub fn new(key_id: String, version: u32, plaintext: Option<Vec<u8>>, ciphertext: Vec<u8>, key_spec: String) -> Self {
        Self {
            key_id,
            version,
            plaintext,
            ciphertext,
            key_spec,
            metadata: HashMap::new(),
            created_at: Zoned::now(),
        }
    }

    /// Clear the plaintext key material from memory for security
    ///
    /// # Security
    /// This method zeroes out the plaintext key material before dropping it
    /// to prevent sensitive data from lingering in memory.
    ///
    pub fn clear_plaintext(&mut self) {
        if let Some(ref mut plaintext) = self.plaintext {
            // Zero out the memory before dropping
            plaintext.zeroize();
        }
        self.plaintext = None;
    }

    /// Add metadata to the data key
    ///
    /// # Arguments
    /// * `key` - Metadata key
    /// * `value` - Metadata value
    ///
    /// # Returns
    /// Updated `DataKey` instance with added metadata
    ///
    pub fn with_metadata(mut self, key: String, value: String) -> Self {
        self.metadata.insert(key, value);
        self
    }
}

/// Master key stored in KMS backend
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MasterKeyInfo {
    /// Unique key identifier
    pub key_id: String,
    /// Key version
    pub version: u32,
    /// Key algorithm (e.g., "AES-256")
    pub algorithm: String,
    /// Key usage type
    pub usage: KeyUsage,
    /// Key status
    pub status: KeyStatus,
    /// Key description
    pub description: Option<String>,
    /// Associated metadata
    pub metadata: HashMap<String, String>,
    /// Key creation timestamp
    pub created_at: Zoned,
    /// Key last rotation timestamp
    pub rotated_at: Option<Zoned>,
    /// Key creator/owner
    pub created_by: Option<String>,
    /// Scheduled deletion deadline while the key is pending deletion
    #[serde(default)]
    pub deletion_date: Option<Zoned>,
}

impl MasterKeyInfo {
    /// Create a new master key
    ///
    /// # Arguments
    /// * `key_id` - Unique identifier for the key
    /// * `algorithm` - Key algorithm (e.g., "AES-256")
    /// * `created_by` - Optional creator/owner of the key
    ///
    /// # Returns
    /// A new `MasterKey` instance
    ///
    pub fn new(key_id: String, algorithm: String, created_by: Option<String>) -> Self {
        Self {
            key_id,
            version: 1,
            algorithm,
            usage: KeyUsage::EncryptDecrypt,
            status: KeyStatus::Active,
            description: None,
            metadata: HashMap::new(),
            created_at: Zoned::now(),
            rotated_at: None,
            created_by,
            deletion_date: None,
        }
    }

    /// Create a new master key with description
    ///
    /// # Arguments
    /// * `key_id` - Unique identifier for the key
    /// * `algorithm` - Key algorithm (e.g., "AES-256")
    /// * `created_by` - Optional creator/owner of the key
    /// * `description` - Optional key description
    ///
    /// # Returns
    /// A new `MasterKey` instance with description
    ///
    pub fn new_with_description(
        key_id: String,
        algorithm: String,
        created_by: Option<String>,
        description: Option<String>,
    ) -> Self {
        Self {
            key_id,
            version: 1,
            algorithm,
            usage: KeyUsage::EncryptDecrypt,
            status: KeyStatus::Active,
            description,
            metadata: HashMap::new(),
            created_at: Zoned::now(),
            rotated_at: None,
            created_by,
            deletion_date: None,
        }
    }
}

/// Key usage enumeration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum KeyUsage {
    /// For encrypting and decrypting data
    EncryptDecrypt,
    /// For signing and verifying data
    SignVerify,
}

/// Key status
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum KeyStatus {
    /// Key is active and can be used
    Active,
    /// Key is disabled and cannot be used for new operations
    Disabled,
    /// Key is pending deletion
    PendingDeletion,
    /// Key has been deleted
    Deleted,
}

/// Why a key carries the rotation-readiness verdict it does.
///
/// Reported alongside [`KeyInfo::rotation_due`] so an operator can tell an
/// overdue key from one the server cannot judge at all; new variants may be
/// added, so consumers must treat an unknown value as "no verdict".
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RotationDueReason {
    /// The key was last rotated longer ago than the configured maximum age.
    Age,
    /// The key has never been rotated and has existed longer than the
    /// configured maximum age.
    NeverRotated,
    /// The key has wrapped more data keys than the configured maximum.
    ///
    /// Counted per key-material version, so a rotation restarts the budget.
    /// The count is an over-estimate by construction (see the backend's
    /// reservation accounting), so this verdict errs toward rotating early.
    Wraps,
    /// The backend cannot rotate keys at all, so no age makes one due.
    Unsupported,
}

/// Information about a key
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeyInfo {
    /// Key identifier
    pub key_id: String,
    /// Key description
    pub description: Option<String>,
    /// Key algorithm
    pub algorithm: String,
    /// Key usage
    pub usage: KeyUsage,
    /// Key status
    pub status: KeyStatus,
    /// Key version
    pub version: u32,
    /// Associated metadata
    pub metadata: HashMap<String, String>,
    /// Key tags
    pub tags: HashMap<String, String>,
    /// Key creation timestamp
    pub created_at: Zoned,
    /// Key last rotation timestamp
    pub rotated_at: Option<Zoned>,
    /// Key creator
    pub created_by: Option<String>,
    /// Whether the key has outlived the configured rotation age.
    ///
    /// Advisory: nothing consults it before encrypting or decrypting, and a key
    /// reported as due keeps serving traffic unchanged. Backends leave it at
    /// its default — the verdict is filled in by the manager, which is the only
    /// place that knows both the configured age and whether the backend can
    /// rotate at all.
    #[serde(default)]
    pub rotation_due: bool,
    /// Why [`KeyInfo::rotation_due`] holds its value, absent when there is no
    /// verdict to explain.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rotation_due_reason: Option<RotationDueReason>,
    /// Wrap operations reserved against the key's current material, reported
    /// only by backends that count wraps (the Vault KV2 backend today). An
    /// approximate value that by design overestimates the wraps actually
    /// performed. In-process transport for the deletion worker's aggregate
    /// wrap gauge, deliberately kept off the serialized admin surface: per-key
    /// exposure would need its own contract decision and snapshot pin.
    #[serde(skip)]
    pub wrap_budget_reserved: Option<u64>,
}

impl From<MasterKeyInfo> for KeyInfo {
    fn from(master_key: MasterKeyInfo) -> Self {
        Self {
            key_id: master_key.key_id,
            description: master_key.description,
            algorithm: master_key.algorithm,
            usage: master_key.usage,
            status: master_key.status,
            version: master_key.version,
            metadata: master_key.metadata.clone(),
            tags: master_key.metadata,
            created_at: master_key.created_at,
            rotated_at: master_key.rotated_at,
            created_by: master_key.created_by,
            rotation_due: false,
            rotation_due_reason: None,
            wrap_budget_reserved: None,
        }
    }
}

/// Request to generate a new data key
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GenerateKeyRequest {
    /// Master key ID to use for encryption
    pub master_key_id: String,
    /// Key specification (e.g., "AES_256")
    pub key_spec: String,
    /// Number of bytes for the key (optional, derived from key_spec)
    pub key_length: Option<u32>,
    /// Encryption context for additional authenticated data
    pub encryption_context: HashMap<String, String>,
    /// Grant tokens for authorization (future use)
    pub grant_tokens: Vec<String>,
}

impl GenerateKeyRequest {
    /// Create a new generate key request
    ///
    /// # Arguments
    /// * `master_key_id` - Master key ID to use for encryption
    /// * `key_spec` - Key specification (e.g., "AES_256")
    ///
    /// # Returns
    /// A new `GenerateKeyRequest` instance
    ///
    pub fn new(master_key_id: String, key_spec: String) -> Self {
        Self {
            master_key_id,
            key_spec,
            key_length: None,
            encryption_context: HashMap::new(),
            grant_tokens: Vec::new(),
        }
    }

    /// Add encryption context
    ///
    /// # Arguments
    /// * `key` - Context key
    /// * `value` - Context value
    ///
    /// # Returns
    /// Updated `GenerateKeyRequest` instance with added context
    ///
    pub fn with_context(mut self, key: String, value: String) -> Self {
        self.encryption_context.insert(key, value);
        self
    }

    /// Set key length explicitly
    ///
    /// # Arguments
    /// * `length` - Key length in bytes
    ///
    /// # Returns
    /// Updated `GenerateKeyRequest` instance with specified key length
    ///
    pub fn with_length(mut self, length: u32) -> Self {
        self.key_length = Some(length);
        self
    }
}

/// Request to encrypt data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EncryptRequest {
    /// Key ID to use for encryption
    pub key_id: String,
    /// Plaintext data to encrypt
    pub plaintext: Vec<u8>,
    /// Encryption context
    pub encryption_context: HashMap<String, String>,
    /// Grant tokens for authorization
    pub grant_tokens: Vec<String>,
}

impl EncryptRequest {
    /// Create a new encrypt request
    ///
    /// # Arguments
    /// * `key_id` - Key ID to use for encryption
    /// * `plaintext` - Plaintext data to encrypt
    ///
    /// # Returns
    /// A new `EncryptRequest` instance
    ///
    pub fn new(key_id: String, plaintext: Vec<u8>) -> Self {
        Self {
            key_id,
            plaintext,
            encryption_context: HashMap::new(),
            grant_tokens: Vec::new(),
        }
    }

    /// Add encryption context
    ///
    /// # Arguments
    /// * `key` - Context key
    /// * `value` - Context value
    ///
    /// # Returns
    /// Updated `EncryptRequest` instance with added context
    ///
    pub fn with_context(mut self, key: String, value: String) -> Self {
        self.encryption_context.insert(key, value);
        self
    }
}

/// Response from encrypt operation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EncryptResponse {
    /// Encrypted data
    pub ciphertext: Vec<u8>,
    /// Key ID used for encryption
    pub key_id: String,
    /// Key version used
    pub key_version: u32,
    /// Encryption algorithm used
    pub algorithm: String,
}

/// Request to decrypt data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DecryptRequest {
    /// Ciphertext to decrypt
    pub ciphertext: Vec<u8>,
    /// Encryption context (must match the context used during encryption)
    pub encryption_context: HashMap<String, String>,
    /// Grant tokens for authorization
    pub grant_tokens: Vec<String>,
}

impl DecryptRequest {
    /// Create a new decrypt request
    ///
    /// # Arguments
    /// * `ciphertext` - Ciphertext to decrypt
    ///
    /// # Returns
    /// A new `DecryptRequest` instance
    ///
    pub fn new(ciphertext: Vec<u8>) -> Self {
        Self {
            ciphertext,
            encryption_context: HashMap::new(),
            grant_tokens: Vec::new(),
        }
    }

    /// Add encryption context
    ///
    /// # Arguments
    /// * `key` - Context key
    /// * `value` - Context value
    ///
    /// # Returns
    /// Updated `DecryptRequest` instance with added context
    ///
    pub fn with_context(mut self, key: String, value: String) -> Self {
        self.encryption_context.insert(key, value);
        self
    }
}

/// Request to list keys
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListKeysRequest {
    /// Maximum number of keys to return
    pub limit: Option<u32>,
    /// Pagination marker
    pub marker: Option<String>,
    /// Filter by key usage
    pub usage_filter: Option<KeyUsage>,
    /// Filter by key status
    pub status_filter: Option<KeyStatus>,
}

impl Default for ListKeysRequest {
    fn default() -> Self {
        Self {
            limit: Some(100),
            marker: None,
            usage_filter: None,
            status_filter: None,
        }
    }
}

/// Response from list keys operation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListKeysResponse {
    /// List of keys
    pub keys: Vec<KeyInfo>,
    /// Pagination marker for next page
    pub next_marker: Option<String>,
    /// Whether there are more keys available
    pub truncated: bool,
    /// Identifiers that are present in the key store but that this build could
    /// not describe, in listing order.
    ///
    /// A key whose record cannot be interpreted must not simply be missing from
    /// `keys`: an inventory that silently omits it reads as "you do not have
    /// this key", and the deletion sweep's census would be taken over a key set
    /// it never fully saw. Reporting the identifier separately keeps the page
    /// honest while still letting the caller page past the damage. Errors that
    /// say nothing about a specific key — timeouts, 5xx, auth failures — still
    /// fail the whole listing rather than landing here.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub unreadable_key_ids: Vec<String>,
}

/// Operation context for auditing and access control
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperationContext {
    /// Operation ID for tracking
    pub operation_id: Uuid,
    /// User or service performing the operation
    pub principal: String,
    /// Source IP address
    pub source_ip: Option<String>,
    /// User agent
    pub user_agent: Option<String>,
    /// Additional context information
    pub additional_context: HashMap<String, String>,
}

impl OperationContext {
    /// Create a new operation context
    ///
    /// # Arguments
    /// * `principal` - User or service performing the operation
    ///
    /// # Returns
    /// A new `OperationContext` instance
    ///
    pub fn new(principal: String) -> Self {
        Self {
            operation_id: Uuid::new_v4(),
            principal,
            source_ip: None,
            user_agent: None,
            additional_context: HashMap::new(),
        }
    }

    /// Principal recorded for work the server starts on its own behalf.
    ///
    /// Namespaced so it can never collide with an access key or an IAM ARN,
    /// which keeps "no human did this" distinguishable from "we lost track of
    /// who did this" in the audit trail.
    pub const INTERNAL_PRINCIPAL: &'static str = "rustfs:internal";

    /// Context for an operation with no authenticated caller, such as
    /// background maintenance or a call made while serving a data-plane
    /// request that carries its own audit entry.
    pub fn internal() -> Self {
        Self::new(Self::INTERNAL_PRINCIPAL.to_string())
    }

    /// Add additional context
    ///
    /// # Arguments
    /// * `key` - Context key
    /// * `value` - Context value
    ///
    /// # Returns
    /// Updated `OperationContext` instance with added context
    ///
    pub fn with_context(mut self, key: String, value: String) -> Self {
        self.additional_context.insert(key, value);
        self
    }

    /// Set source IP
    ///
    /// # Arguments
    /// * `ip` - Source IP address
    ///
    /// # Returns
    /// Updated `OperationContext` instance with source IP
    ///
    pub fn with_source_ip(mut self, ip: String) -> Self {
        self.source_ip = Some(ip);
        self
    }

    /// Set user agent
    ///
    /// # Arguments
    /// * `agent` - User agent string
    ///
    /// # Returns
    /// Updated `OperationContext` instance with user agent
    ///
    pub fn with_user_agent(mut self, agent: String) -> Self {
        self.user_agent = Some(agent);
        self
    }
}

/// Object encryption context
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectEncryptionContext {
    /// Bucket name
    pub bucket: String,
    /// Object key
    pub object_key: String,
    /// Content type
    pub content_type: Option<String>,
    /// Object size in bytes
    pub size: Option<u64>,
    /// Additional encryption context
    pub encryption_context: HashMap<String, String>,
}

impl ObjectEncryptionContext {
    /// Create a new object encryption context
    ///
    /// # Arguments
    /// * `bucket` - Bucket name
    /// * `object_key` - Object key
    ///
    /// # Returns
    /// A new `ObjectEncryptionContext` instance
    ///
    pub fn new(bucket: String, object_key: String) -> Self {
        Self {
            bucket,
            object_key,
            content_type: None,
            size: None,
            encryption_context: HashMap::new(),
        }
    }

    /// Set content type
    ///
    /// # Arguments
    /// * `content_type` - Content type string
    ///
    /// # Returns
    /// Updated `ObjectEncryptionContext` instance with content type
    ///
    pub fn with_content_type(mut self, content_type: String) -> Self {
        self.content_type = Some(content_type);
        self
    }

    /// Set object size
    ///
    /// # Arguments
    /// * `size` - Object size in bytes
    ///
    /// # Returns
    /// Updated `ObjectEncryptionContext` instance with size
    ///
    pub fn with_size(mut self, size: u64) -> Self {
        self.size = Some(size);
        self
    }

    /// Add encryption context
    ///
    /// # Arguments
    /// * `key` - Context key
    /// * `value` - Context value
    ///
    /// # Returns
    /// Updated `ObjectEncryptionContext` instance with added context
    ///
    pub fn with_encryption_context(mut self, key: String, value: String) -> Self {
        self.encryption_context.insert(key, value);
        self
    }
}

/// Encryption metadata stored with encrypted objects
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EncryptionMetadata {
    /// Encryption algorithm used
    pub algorithm: String,
    /// Key ID used for encryption
    pub key_id: String,
    /// Key version
    pub key_version: u32,
    /// Initialization vector
    pub iv: Vec<u8>,
    /// Authentication tag (for AEAD ciphers)
    pub tag: Option<Vec<u8>>,
    /// Encryption context
    pub encryption_context: HashMap<String, String>,
    /// Timestamp when encrypted
    pub encrypted_at: Zoned,
    /// Size of original data
    pub original_size: u64,
    /// Encrypted data key
    pub encrypted_data_key: Vec<u8>,
    /// The exact AAD bytes this object was sealed under.
    ///
    /// The AAD is the serialized encryption context, and the serialization is
    /// what must be reproduced byte-for-byte — not the map. Objects written
    /// before the context was canonicalized carry whichever `HashMap` order
    /// happened to be in effect when they were sealed, and
    /// `x-rustfs-encryption-context` preserves that exact byte sequence. It is
    /// therefore recoverable, but only while it is never round-tripped through
    /// a `HashMap` and re-serialized.
    ///
    /// `None` means "derive it from `encryption_context`", which is correct
    /// only when no stored serialization exists to defer to.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_aad: Option<Vec<u8>>,
}

/// Health status information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthStatus {
    /// Whether the KMS backend is healthy
    pub kms_healthy: bool,
    /// Whether encryption/decryption operations are working
    pub encryption_working: bool,
    /// Backend type (e.g., "local", "vault-kv2", "vault-transit")
    pub backend_type: String,
    /// Additional health details
    pub details: HashMap<String, String>,
}

/// Supported encryption algorithms
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum EncryptionAlgorithm {
    /// AES-256-GCM
    #[serde(rename = "AES256")]
    Aes256,
    /// ChaCha20-Poly1305
    #[serde(rename = "ChaCha20Poly1305")]
    ChaCha20Poly1305,
    /// AWS KMS managed encryption
    #[serde(rename = "aws:kms")]
    AwsKms,
}

/// Key specification for data keys
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum KeySpec {
    /// AES-256 key (32 bytes)
    Aes256,
    /// AES-128 key (16 bytes)
    Aes128,
    /// ChaCha20 key (32 bytes)
    ChaCha20,
}

impl KeySpec {
    /// Get the key size in bytes
    ///
    /// # Returns
    /// Key size in bytes
    ///
    pub fn key_size(&self) -> usize {
        match self {
            Self::Aes256 => 32,
            Self::Aes128 => 16,
            Self::ChaCha20 => 32,
        }
    }

    /// Get the string representation for backends
    ///
    /// # Returns
    /// Key specification as a string
    ///
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Aes256 => "AES_256",
            Self::Aes128 => "AES_128",
            Self::ChaCha20 => "ChaCha20",
        }
    }
}

/// Key metadata information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeyMetadata {
    /// Key identifier
    pub key_id: String,
    /// Key state
    pub key_state: KeyState,
    /// Key usage type
    pub key_usage: KeyUsage,
    /// Key description
    pub description: Option<String>,
    /// Key creation timestamp
    pub creation_date: Zoned,
    /// Key deletion timestamp
    pub deletion_date: Option<Zoned>,
    /// Key origin
    pub origin: String,
    /// Key manager
    pub key_manager: String,
    /// Key tags
    pub tags: HashMap<String, String>,
}

/// Key state enumeration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum KeyState {
    /// Key is enabled and can be used
    Enabled,
    /// Key is disabled
    Disabled,
    /// Key is pending deletion
    PendingDeletion,
    /// Key is pending import
    PendingImport,
    /// Key is unavailable
    Unavailable,
}

/// Request to create a new key
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateKeyRequest {
    /// Custom key name (optional, will auto-generate UUID if not provided)
    pub key_name: Option<String>,
    /// Key usage type
    pub key_usage: KeyUsage,
    /// Key description
    pub description: Option<String>,
    /// Key policy
    pub policy: Option<String>,
    /// Tags for the key
    pub tags: HashMap<String, String>,
    /// Origin of the key
    pub origin: Option<String>,
}

impl Default for CreateKeyRequest {
    fn default() -> Self {
        Self {
            key_name: None,
            key_usage: KeyUsage::EncryptDecrypt,
            description: None,
            policy: None,
            tags: HashMap::new(),
            origin: None,
        }
    }
}

/// Response from create key operation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateKeyResponse {
    /// Created key ID
    pub key_id: String,
    /// Key metadata
    pub key_metadata: KeyMetadata,
}

/// Response from decrypt operation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DecryptResponse {
    /// Decrypted plaintext
    pub plaintext: Vec<u8>,
    /// Key ID used for decryption
    pub key_id: String,
    /// Encryption algorithm used
    pub encryption_algorithm: Option<String>,
}

/// Request to describe a key
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DescribeKeyRequest {
    /// Key ID to describe
    pub key_id: String,
}

/// Response from describe key operation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DescribeKeyResponse {
    /// Key metadata
    pub key_metadata: KeyMetadata,
}

/// Request to generate a data key
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GenerateDataKeyRequest {
    /// Key ID to use for encryption
    pub key_id: String,
    /// Key specification
    pub key_spec: KeySpec,
    /// Encryption context
    pub encryption_context: HashMap<String, String>,
}

impl GenerateDataKeyRequest {
    /// Create a new generate data key request
    ///
    /// # Arguments
    /// * `key_id` - Key ID to use for encryption
    /// * `key_spec` - Key specification
    ///
    /// # Returns
    /// A new `GenerateDataKeyRequest` instance
    ///
    pub fn new(key_id: String, key_spec: KeySpec) -> Self {
        Self {
            key_id,
            key_spec,
            encryption_context: HashMap::new(),
        }
    }
}

/// Response from generate data key operation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GenerateDataKeyResponse {
    /// Key ID used
    pub key_id: String,
    /// Plaintext data key
    pub plaintext_key: Vec<u8>,
    /// Encrypted data key
    pub ciphertext_blob: Vec<u8>,
}

impl EncryptionAlgorithm {
    /// Get the algorithm name as a string
    ///
    /// # Returns
    /// Algorithm name as a string
    ///
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Aes256 => "AES256",
            Self::ChaCha20Poly1305 => "ChaCha20Poly1305",
            Self::AwsKms => "aws:kms",
        }
    }

    /// Get the key size in bytes for this algorithm
    pub fn key_size(&self) -> usize {
        match self {
            Self::Aes256 => 32,           // 256 bits
            Self::ChaCha20Poly1305 => 32, // 256 bits
            Self::AwsKms => 32,           // 256 bits (uses AES-256 internally)
        }
    }

    /// Get the IV size in bytes for this algorithm
    pub fn iv_size(&self) -> usize {
        match self {
            Self::Aes256 => 12,           // 96 bits for GCM
            Self::ChaCha20Poly1305 => 12, // 96 bits
            Self::AwsKms => 12,           // 96 bits (uses AES-256-GCM internally)
        }
    }
}

impl std::str::FromStr for EncryptionAlgorithm {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "AES256" => Ok(Self::Aes256),
            "ChaCha20Poly1305" => Ok(Self::ChaCha20Poly1305),
            "aws:kms" => Ok(Self::AwsKms),
            _ => Err(()),
        }
    }
}

/// Shortest pending-deletion waiting window a caller may ask for, in days.
///
/// The window is enforced once, in [`crate::manager::KmsManager::delete_key`];
/// backends keep the same bound as a defensive check for direct callers.
pub const MIN_PENDING_DELETION_WINDOW_DAYS: u32 = 7;

/// Longest pending-deletion waiting window a caller may ask for, in days.
pub const MAX_PENDING_DELETION_WINDOW_DAYS: u32 = 30;

/// Waiting window applied when a delete request does not name one.
pub const DEFAULT_PENDING_DELETION_WINDOW_DAYS: u32 = MAX_PENDING_DELETION_WINDOW_DAYS;

/// Request to delete a key
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DeleteKeyRequest {
    /// Key ID to delete
    pub key_id: String,
    /// Number of days to wait before deletion (7-30 days, optional, defaults to 30)
    pub pending_window_in_days: Option<u32>,
    /// Destroy the key material right away instead of scheduling it.
    ///
    /// Refused unless the server enables `allow_immediate_deletion`; see
    /// [`crate::manager::KmsManager::delete_key`] for the gate.
    pub force_immediate: Option<bool>,
    /// Key id echoed back by the caller to confirm an immediate deletion.
    ///
    /// Must equal `key_id` exactly whenever `force_immediate` is set. Optional
    /// with a serde default because this type is part of the admin API
    /// contract: clients that never ask for immediate deletion are unaffected.
    #[serde(default)]
    pub confirm_key_id: Option<String>,
}

/// Response from delete key operation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteKeyResponse {
    /// Key ID that was deleted or scheduled for deletion
    pub key_id: String,
    /// Deletion date (if scheduled)
    pub deletion_date: Option<String>,
    /// Key metadata
    pub key_metadata: KeyMetadata,
}

/// Request to cancel key deletion
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CancelKeyDeletionRequest {
    /// Key ID to cancel deletion for
    pub key_id: String,
}

/// Response from cancel key deletion operation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CancelKeyDeletionResponse {
    /// Key ID
    pub key_id: String,
    /// Key metadata
    pub key_metadata: KeyMetadata,
}

// SECURITY: Implement Drop to automatically zero sensitive data when DataKey is dropped
impl Drop for DataKeyInfo {
    fn drop(&mut self) {
        self.clear_plaintext();
    }
}

/// Request to rewrap an existing data key envelope under the current version of
/// the master key that already wraps it.
///
/// Rewrap changes only the wrapping. The data key inside is never replaced, so
/// object ciphertext, IV derivation and ETags are untouched — which is what
/// makes it possible to retire an old master key version without rewriting a
/// single object body.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RewrapDataKeyRequest {
    /// The existing wrapped data key, exactly as it was persisted
    pub ciphertext: Vec<u8>,
    /// Encryption context the envelope was created with; the backend rejects
    /// the request when the envelope's own context is not reproduced here, so a
    /// caller cannot rewrap an envelope it could not have decrypted
    pub encryption_context: HashMap<String, String>,
}

/// Request to report where an existing data key envelope sits relative to its
/// master key's current version.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DescribeDataKeyWrappingRequest {
    /// The existing wrapped data key, exactly as it was persisted
    pub ciphertext: Vec<u8>,
    /// Encryption context the envelope was created with, checked exactly as
    /// [`RewrapDataKeyRequest`] checks it
    pub encryption_context: HashMap<String, String>,
}

/// Where an existing wrapped data key sits relative to its master key's current
/// version.
///
/// This is the only supported way to ask that question. The answer lives in a
/// different place for every backend that rotates — a JSON field on the envelope
/// for Vault KV2, the `vault:vN:` prefix of the ciphertext for Vault Transit,
/// nowhere at all for backends that do not rotate — and a caller that reached
/// into the envelope itself would be re-implementing that table, on the wrong
/// side of the KMS boundary, once per call site.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DescribeDataKeyWrappingResponse {
    /// Master key wrapping the data key
    pub key_id: String,
    /// Version that wrapped the data key, when the backend can name it
    pub key_version: Option<u32>,
    /// The master key's current version, when the backend can name it
    pub current_key_version: Option<u32>,
    /// Whether the envelope is already wrapped by the current version, which is
    /// exactly the condition under which a rewrap would be a no-op.
    ///
    /// Callers must read this rather than compare the two version fields. A
    /// backend may leave either version unset while still knowing the answer,
    /// and the two must never disagree: this flag is what a retirement scan
    /// counts, and a rewrap sweep that rewrote envelopes the scan already
    /// considered done would never converge.
    pub is_current: bool,
}

/// Result of [`RewrapDataKeyRequest`].
///
/// The plaintext data key is deliberately absent: rewrap exists so that the
/// data key can be re-wrapped without the caller ever holding it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RewrapDataKeyResponse {
    /// The wrapped data key to persist in place of the request's ciphertext.
    /// Byte-identical to the request when `rewrapped` is false.
    pub ciphertext: Vec<u8>,
    /// Master key that wraps the data key; unchanged by a rewrap
    pub key_id: String,
    /// Master key version that wrapped the input, when the backend can name it
    pub source_key_version: Option<u32>,
    /// Master key version that wraps `ciphertext`, when the backend can name it
    pub destination_key_version: Option<u32>,
    /// Whether the wrapping actually changed. False means the input was already
    /// wrapped by the current version and callers must not persist anything —
    /// re-running a rewrap sweep has to converge without further writes.
    pub rewrapped: bool,
}
