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

//! Bucket-level encryption configuration management

use crate::error::{KmsError, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Supported encryption algorithms for bucket-level encryption
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum BucketEncryptionAlgorithm {
    /// AES-256 with Galois/Counter Mode
    #[serde(rename = "AES256")]
    Aes256,
    /// ChaCha20-Poly1305
    #[serde(rename = "CHACHA20_POLY1305")]
    ChaCha20Poly1305,
    /// AWS KMS encryption
    #[serde(rename = "aws:kms")]
    AwsKms,
}

impl Default for BucketEncryptionAlgorithm {
    fn default() -> Self {
        Self::Aes256
    }
}

impl std::fmt::Display for BucketEncryptionAlgorithm {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Aes256 => write!(f, "AES256"),
            Self::ChaCha20Poly1305 => write!(f, "CHACHA20_POLY1305"),
            Self::AwsKms => write!(f, "aws:kms"),
        }
    }
}

/// Bucket encryption configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BucketEncryptionConfig {
    /// Whether encryption is enabled for this bucket
    pub enabled: bool,
    /// Encryption algorithm to use
    pub algorithm: BucketEncryptionAlgorithm,
    /// KMS key ID for encrypting data keys
    pub kms_key_id: String,
    /// Whether to encrypt object metadata
    pub encrypt_metadata: bool,
    /// Additional encryption context
    pub encryption_context: HashMap<String, String>,
    /// Configuration creation timestamp
    pub created_at: chrono::DateTime<chrono::Utc>,
    /// Configuration last update timestamp
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

impl BucketEncryptionConfig {
    /// Create a new bucket encryption configuration
    pub fn new(algorithm: BucketEncryptionAlgorithm, kms_key_id: String, encrypt_metadata: bool) -> Self {
        let now = chrono::Utc::now();
        Self {
            enabled: true,
            algorithm,
            kms_key_id,
            encrypt_metadata,
            encryption_context: HashMap::new(),
            created_at: now,
            updated_at: now,
        }
    }

    /// Add encryption context
    pub fn with_context(mut self, key: String, value: String) -> Self {
        self.encryption_context.insert(key, value);
        self
    }

    /// Update the configuration
    pub fn update(&mut self) {
        self.updated_at = chrono::Utc::now();
    }

    /// Disable encryption for this bucket
    pub fn disable(&mut self) {
        self.enabled = false;
        self.update();
    }

    /// Enable encryption for this bucket
    pub fn enable(&mut self) {
        self.enabled = true;
        self.update();
    }

    /// Validate the configuration
    pub fn validate(&self) -> Result<()> {
        if self.enabled && self.kms_key_id.is_empty() {
            return Err(KmsError::ConfigurationError {
                message: "KMS key ID is required when encryption is enabled".to_string(),
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_bucket_encryption_config() {
        let config = BucketEncryptionConfig::new(BucketEncryptionAlgorithm::Aes256, "test-key-id".to_string(), true);

        assert!(config.enabled);
        assert_eq!(config.algorithm, BucketEncryptionAlgorithm::Aes256);
        assert_eq!(config.kms_key_id, "test-key-id");
        assert!(config.encrypt_metadata);
        assert!(config.validate().is_ok());
    }
}
