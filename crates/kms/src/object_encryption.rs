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

//! Object encryption service for RustFS
//!
//! This module provides high-level object encryption and decryption services
//! that integrate with the KMS for key management.

use crate::types::EncryptedObjectMetadata;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Object encryption algorithms
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum EncryptionAlgorithm {
    /// AES-256-GCM encryption
    Aes256Gcm,
    /// ChaCha20-Poly1305 encryption
    ChaCha20Poly1305,
}

impl Default for EncryptionAlgorithm {
    fn default() -> Self {
        Self::Aes256Gcm
    }
}

impl std::fmt::Display for EncryptionAlgorithm {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Aes256Gcm => write!(f, "AES-256-GCM"),
            Self::ChaCha20Poly1305 => write!(f, "ChaCha20-Poly1305"),
        }
    }
}

/// Object encryption configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectEncryptionConfig {
    /// Default master key ID
    pub default_master_key_id: String,
    /// Default encryption algorithm
    pub default_algorithm: EncryptionAlgorithm,
    /// Whether to encrypt object metadata
    pub encrypt_metadata: bool,
    /// Maximum object size for encryption (in bytes)
    pub max_object_size: Option<u64>,
    /// Chunk size for streaming encryption (in bytes)
    pub chunk_size: usize,
}

impl Default for ObjectEncryptionConfig {
    fn default() -> Self {
        Self {
            default_master_key_id: "default".to_string(),
            default_algorithm: EncryptionAlgorithm::default(),
            encrypt_metadata: true,
            max_object_size: None,
            chunk_size: 64 * 1024, // 64KB chunks
        }
    }
}

/// Encrypted object data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EncryptedObjectData {
    /// Encrypted data
    pub ciphertext: Vec<u8>,
    /// Encrypted data key
    pub encrypted_data_key: Vec<u8>,
    /// Encryption algorithm used
    pub algorithm: EncryptionAlgorithm,
    /// Initialization vector
    pub iv: Vec<u8>,
    /// Authentication tag
    pub tag: Vec<u8>,
    /// Master key ID used
    pub master_key_id: String,
    /// Encryption context
    pub encryption_context: HashMap<String, String>,
    /// Object metadata (if encrypted)
    pub encrypted_metadata: Option<EncryptedObjectMetadata>,
}
