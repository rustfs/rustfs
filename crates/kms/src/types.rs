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

//! KMS type definitions

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::SystemTime;
use uuid::Uuid;

/// A data encryption key (DEK) used for encrypting data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataKey {
    /// Key identifier
    pub key_id: String,
    /// Key version
    pub version: u32,
    /// Plaintext key material (only available when generating)
    pub plaintext: Option<Vec<u8>>,
    /// Encrypted key material
    pub ciphertext: Vec<u8>,
    /// Associated metadata
    pub metadata: HashMap<String, String>,
    /// Key creation time
    pub created_at: SystemTime,
}

impl DataKey {
    /// Create a new data key
    pub fn new(key_id: String, version: u32, plaintext: Option<Vec<u8>>, ciphertext: Vec<u8>) -> Self {
        Self {
            key_id,
            version,
            plaintext,
            ciphertext,
            metadata: HashMap::new(),
            created_at: SystemTime::now(),
        }
    }

    /// Clear the plaintext key material from memory
    pub fn clear_plaintext(&mut self) {
        if let Some(ref mut pt) = self.plaintext {
            // Zero out the memory
            pt.fill(0);
        }
        self.plaintext = None;
    }
}

/// A master key used for encrypting data keys
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MasterKey {
    /// Unique key identifier
    pub key_id: String,
    /// Key version
    pub version: u32,
    /// Key algorithm (e.g., "AES-256", "RSA-2048")
    pub algorithm: String,
    /// Key usage type
    pub usage: KeyUsage,
    /// Key status
    pub status: KeyStatus,
    /// Associated metadata
    pub metadata: HashMap<String, String>,
    /// Key creation time
    pub created_at: SystemTime,
    /// Key last rotation time
    pub rotated_at: Option<SystemTime>,
}

/// Key usage types
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum KeyUsage {
    /// For encrypting data
    Encrypt,
    /// For signing data
    Sign,
    /// For both encryption and signing
    EncryptSign,
}

/// Key status
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum KeyStatus {
    /// Key is active and can be used
    Active,
    /// Key is disabled and cannot be used
    Disabled,
    /// Key is pending deletion
    PendingDeletion,
    /// Key has been deleted
    Deleted,
}

/// Information about a key
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeyInfo {
    /// Key identifier
    pub key_id: String,
    /// Key name
    pub name: String,
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
    /// Key creation time
    pub created_at: SystemTime,
    /// Key last rotation time
    pub rotated_at: Option<SystemTime>,
    /// Key creator
    pub created_by: Option<String>,
}

/// Request to generate a new data key
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GenerateKeyRequest {
    /// Master key ID to use for encryption
    pub master_key_id: String,
    /// Key specification (e.g., "AES_256")
    pub key_spec: String,
    /// Number of bytes for the key
    pub key_length: Option<u32>,
    /// Encryption context
    pub encryption_context: HashMap<String, String>,
    /// Grant tokens for authorization
    pub grant_tokens: Vec<String>,
}

impl GenerateKeyRequest {
    /// Create a new generate key request
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
    pub fn with_context(mut self, key: String, value: String) -> Self {
        self.encryption_context.insert(key, value);
        self
    }

    /// Set key length
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
    pub fn new(key_id: String, plaintext: Vec<u8>) -> Self {
        Self {
            key_id,
            plaintext,
            encryption_context: HashMap::new(),
            grant_tokens: Vec::new(),
        }
    }

    /// Add encryption context
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
    /// Encryption context (must match encryption context)
    pub encryption_context: HashMap<String, String>,
    /// Grant tokens for authorization
    pub grant_tokens: Vec<String>,
}

impl DecryptRequest {
    /// Create a new decrypt request
    pub fn new(ciphertext: Vec<u8>) -> Self {
        Self {
            ciphertext,
            encryption_context: HashMap::new(),
            grant_tokens: Vec::new(),
        }
    }

    /// Add encryption context
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
    /// Whether there are more keys
    pub truncated: bool,
}

/// Key operation context for auditing and access control
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
    /// Additional context
    pub additional_context: HashMap<String, String>,
}

impl OperationContext {
    /// Create a new operation context
    pub fn new(principal: String) -> Self {
        Self {
            operation_id: Uuid::new_v4(),
            principal,
            source_ip: None,
            user_agent: None,
            additional_context: HashMap::new(),
        }
    }

    /// Add additional context
    pub fn with_context(mut self, key: String, value: String) -> Self {
        self.additional_context.insert(key, value);
        self
    }

    /// Set source IP
    pub fn with_source_ip(mut self, ip: String) -> Self {
        self.source_ip = Some(ip);
        self
    }

    /// Set user agent
    pub fn with_user_agent(mut self, agent: String) -> Self {
        self.user_agent = Some(agent);
        self
    }
}
