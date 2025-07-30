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

//! AES-GCM encryption and decryption utilities

use crate::error::Error as CryptoError;
use aes_gcm::{
    Aes256Gcm as AesGcmCipher, Key, Nonce,
    aead::{Aead, AeadCore, KeyInit, OsRng},
};

// Re-export for external use
pub use aes_gcm::Aes256Gcm as AesGcm;

/// AES-256-GCM cipher wrapper
pub struct Aes256Gcm {
    cipher: AesGcmCipher,
}

impl Aes256Gcm {
    /// Create a new AES-256-GCM cipher with the given key
    pub fn new(key: &[u8]) -> Result<Self, CryptoError> {
        if key.len() != 32 {
            return Err(CryptoError::InvalidKeyLength {
                expected: 32,
                actual: key.len(),
            });
        }

        let key = Key::<AesGcmCipher>::from_slice(key);
        let cipher = AesGcmCipher::new(key);

        Ok(Self { cipher })
    }

    /// Generate a random nonce
    pub fn generate_nonce(&self) -> Result<Vec<u8>, CryptoError> {
        let nonce = AesGcmCipher::generate_nonce(&mut OsRng);
        Ok(nonce.to_vec())
    }

    /// Encrypt data with additional authenticated data (AAD)
    pub fn encrypt_with_aad(&self, plaintext: &[u8], nonce: &[u8], aad: &[u8]) -> Result<(Vec<u8>, Vec<u8>), CryptoError> {
        if nonce.len() != 12 {
            return Err(CryptoError::InvalidNonceLength {
                expected: 12,
                actual: nonce.len(),
            });
        }

        let nonce = Nonce::from_slice(nonce);
        let payload = aes_gcm::aead::Payload { msg: plaintext, aad };

        let ciphertext = self
            .cipher
            .encrypt(nonce, payload)
            .map_err(|e| CryptoError::EncryptionFailed(e.to_string()))?;

        // Split ciphertext and tag (last 16 bytes)
        if ciphertext.len() < 16 {
            return Err(CryptoError::EncryptionFailed("Ciphertext too short".to_string()));
        }

        let (data, tag) = ciphertext.split_at(ciphertext.len() - 16);
        Ok((data.to_vec(), tag.to_vec()))
    }

    /// Encrypt data without AAD
    pub fn encrypt(&self, plaintext: &[u8], nonce: &[u8]) -> Result<(Vec<u8>, Vec<u8>), CryptoError> {
        self.encrypt_with_aad(plaintext, nonce, &[])
    }

    /// Decrypt data with additional authenticated data (AAD)
    pub fn decrypt_with_aad(&self, ciphertext: &[u8], nonce: &[u8], tag: &[u8], aad: &[u8]) -> Result<Vec<u8>, CryptoError> {
        if nonce.len() != 12 {
            return Err(CryptoError::InvalidNonceLength {
                expected: 12,
                actual: nonce.len(),
            });
        }

        if tag.len() != 16 {
            return Err(CryptoError::InvalidTagLength {
                expected: 16,
                actual: tag.len(),
            });
        }

        let nonce = Nonce::from_slice(nonce);

        // Combine ciphertext and tag
        let mut combined = Vec::with_capacity(ciphertext.len() + tag.len());
        combined.extend_from_slice(ciphertext);
        combined.extend_from_slice(tag);

        let payload = aes_gcm::aead::Payload { msg: &combined, aad };

        let plaintext = self
            .cipher
            .decrypt(nonce, payload)
            .map_err(|e| CryptoError::DecryptionFailed(e.to_string()))?;

        Ok(plaintext)
    }

    /// Decrypt data without AAD
    pub fn decrypt(&self, ciphertext: &[u8], nonce: &[u8], tag: &[u8]) -> Result<Vec<u8>, CryptoError> {
        self.decrypt_with_aad(ciphertext, nonce, tag, &[])
    }
}

/// Generate a random 256-bit AES key
pub fn generate_aes256_key() -> [u8; 32] {
    use aes_gcm::aead::rand_core::RngCore;
    let mut key = [0u8; 32];
    OsRng.fill_bytes(&mut key);
    key
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_aes256_gcm_encrypt_decrypt() {
        let key = generate_aes256_key();
        let cipher = Aes256Gcm::new(&key).expect("Failed to create cipher");

        let plaintext = b"Hello, World!";
        let nonce = cipher.generate_nonce().expect("Failed to generate nonce");
        let aad = b"additional data";

        // Test encryption
        let (ciphertext, tag) = cipher.encrypt_with_aad(plaintext, &nonce, aad).expect("Encryption failed");

        assert_ne!(ciphertext, plaintext);
        assert_eq!(tag.len(), 16);

        // Test decryption
        let decrypted = cipher
            .decrypt_with_aad(&ciphertext, &nonce, &tag, aad)
            .expect("Decryption failed");

        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn test_aes256_gcm_without_aad() {
        let key = generate_aes256_key();
        let cipher = Aes256Gcm::new(&key).expect("Failed to create cipher");

        let plaintext = b"Hello, World!";
        let nonce = cipher.generate_nonce().expect("Failed to generate nonce");

        // Test encryption without AAD
        let (ciphertext, tag) = cipher.encrypt(plaintext, &nonce).expect("Encryption failed");

        // Test decryption without AAD
        let decrypted = cipher.decrypt(&ciphertext, &nonce, &tag).expect("Decryption failed");

        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn test_invalid_key_length() {
        let key = vec![0u8; 16]; // Wrong key length
        let result = Aes256Gcm::new(&key);
        assert!(result.is_err());
    }

    #[test]
    fn test_invalid_nonce_length() {
        let key = generate_aes256_key();
        let cipher = Aes256Gcm::new(&key).expect("Failed to create cipher");

        let plaintext = b"Hello, World!";
        let nonce = vec![0u8; 8]; // Wrong nonce length

        let result = cipher.encrypt(plaintext, &nonce);
        assert!(result.is_err());
    }
}
