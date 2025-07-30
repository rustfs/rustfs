// Copyright 2024 RustFS
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

//! Object encryption cipher implementations

use crate::error::{EncryptionError, EncryptionResult};
use crate::security::SecretKey;
use rustfs_crypto;

/// Trait for object encryption ciphers
pub trait ObjectCipher: Send + Sync {
    /// Encrypt data with additional authenticated data
    fn encrypt(&self, plaintext: &[u8], iv: &[u8], _aad: &[u8]) -> EncryptionResult<(Vec<u8>, Vec<u8>)>;

    /// Decrypt data with additional authenticated data
    fn decrypt(&self, ciphertext: &[u8], iv: &[u8], tag: &[u8], aad: &[u8]) -> EncryptionResult<Vec<u8>>;

    /// Get the algorithm name
    fn algorithm(&self) -> &str;

    /// Get the key size in bytes
    fn key_size(&self) -> usize;

    /// Get the IV size in bytes
    fn iv_size(&self) -> usize;
}

/// AES-256-GCM cipher implementation
pub struct AesGcmCipher {
    key: SecretKey,
}

impl AesGcmCipher {
    /// Create a new AES-GCM cipher
    pub fn new(key: &[u8]) -> EncryptionResult<Self> {
        if key.len() != 32 {
            return Err(EncryptionError::InvalidKeySize {
                expected: 32,
                actual: key.len(),
            });
        }

        Ok(Self { key: SecretKey::from_slice(key) })
    }
}

impl Default for AesGcmCipher {
    fn default() -> Self {
        let key = vec![0u8; 32];
        Self::new(&key).expect("Failed to create default AES-GCM cipher")
    }
}

impl ObjectCipher for AesGcmCipher {
    fn encrypt(&self, plaintext: &[u8], iv: &[u8], _aad: &[u8]) -> EncryptionResult<(Vec<u8>, Vec<u8>)> {
        if iv.len() != 12 {
            return Err(EncryptionError::InvalidIvSize {
                expected: 12,
                actual: iv.len(),
            });
        }

        // Encrypt the data
        let ciphertext = rustfs_crypto::encrypt_data(plaintext, self.key.expose_secret()).map_err(|e| EncryptionError::cipher_error(
             "encrypt",
             format!("Encryption failed: {:?}", e),
         ))?;

        // For AEAD ciphers, the tag is typically appended to the ciphertext
        // Here we assume the last 16 bytes are the authentication tag
        let (encrypted_data, tag) = if ciphertext.len() >= 16 {
            let split_point = ciphertext.len() - 16;
            (ciphertext[..split_point].to_vec(), ciphertext[split_point..].to_vec())
        } else {
            return Err(EncryptionError::cipher_error(
                 "encrypt",
                 "Invalid ciphertext length",
             ));
        };

        Ok((encrypted_data, tag))
    }

    fn decrypt(&self, ciphertext: &[u8], iv: &[u8], tag: &[u8], _aad: &[u8]) -> EncryptionResult<Vec<u8>> {
        if iv.len() != 12 {
            return Err(EncryptionError::InvalidIvSize {
                expected: 12,
                actual: iv.len(),
            });
        }

        // Combine ciphertext and tag for decryption
        let mut combined = ciphertext.to_vec();
        combined.extend_from_slice(tag);

        // Decrypt the data
        let plaintext = rustfs_crypto::decrypt_data(&combined, self.key.expose_secret()).map_err(|e| EncryptionError::cipher_error(
             "decrypt",
             format!("Decryption failed: {:?}", e),
         ))?;

        Ok(plaintext)
    }

    fn algorithm(&self) -> &str {
        "AES-256-GCM"
    }

    fn key_size(&self) -> usize {
        32 // 256 bits
    }

    fn iv_size(&self) -> usize {
        12 // 96 bits
    }
}

/// ChaCha20-Poly1305 cipher implementation
pub struct ChaCha20Poly1305Cipher {
    key: SecretKey,
}

impl ChaCha20Poly1305Cipher {
    /// Create a new ChaCha20-Poly1305 cipher with the given key
    pub fn new(key: &[u8]) -> EncryptionResult<Self> {
        if key.len() != 32 {
            return Err(EncryptionError::InvalidKeySize {
                expected: 32,
                actual: key.len(),
            });
        }
        Ok(Self { key: SecretKey::from_slice(key) })
    }
}

impl ObjectCipher for ChaCha20Poly1305Cipher {
    fn encrypt(&self, plaintext: &[u8], iv: &[u8], _aad: &[u8]) -> EncryptionResult<(Vec<u8>, Vec<u8>)> {
        if iv.len() != 12 {
            return Err(EncryptionError::InvalidIvSize {
                expected: 12,
                actual: iv.len(),
            });
        }

        let cipher = rustfs_crypto::ChaCha20Poly1305::new(self.key.expose_secret())
             .map_err(|e| EncryptionError::cipher_error(
                 "encrypt",
                 format!("ChaCha20 cipher creation failed: {}", e),
             ))?;
         let (ciphertext, tag) = cipher
             .encrypt(plaintext, iv)
             .map_err(|e| EncryptionError::cipher_error(
                 "encrypt",
                 format!("ChaCha20 encryption failed: {}", e),
             ))?;

        Ok((ciphertext, tag))
    }

    fn decrypt(&self, ciphertext: &[u8], iv: &[u8], tag: &[u8], _aad: &[u8]) -> EncryptionResult<Vec<u8>> {
        if iv.len() != 12 {
            return Err(EncryptionError::InvalidIvSize {
                expected: 12,
                actual: iv.len(),
            });
        }

        let cipher = rustfs_crypto::ChaCha20Poly1305::new(self.key.expose_secret())
             .map_err(|e| EncryptionError::cipher_error(
                 "decrypt",
                 format!("ChaCha20 cipher creation failed: {}", e),
             ))?;
         let plaintext = cipher
             .decrypt(ciphertext, iv, tag)
             .map_err(|e| EncryptionError::cipher_error(
                 "decrypt",
                 format!("ChaCha20 decryption failed: {}", e),
             ))?;

        Ok(plaintext)
    }

    fn algorithm(&self) -> &str {
        "ChaCha20-Poly1305"
    }

    fn key_size(&self) -> usize {
        32 // 256 bits
    }

    fn iv_size(&self) -> usize {
        12 // 96 bits
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_aes_gcm_cipher() {
        let key = vec![0u8; 32]; // 256-bit key
        let cipher = AesGcmCipher::new(&key).unwrap();
        let plaintext = b"Hello, World!";
        let iv = vec![0u8; 12];
        let aad = b"additional data";

        // Test encryption
        let (ciphertext, tag) = cipher.encrypt(plaintext, &iv, aad).unwrap();
        assert!(!ciphertext.is_empty());
        assert_eq!(tag.len(), 16);

        // Test decryption
        let decrypted = cipher.decrypt(&ciphertext, &iv, &tag, aad).unwrap();
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn test_aes_gcm_cipher_properties() {
        let key = vec![0u8; 32];
        let cipher = AesGcmCipher::new(&key).unwrap();
        assert_eq!(cipher.algorithm(), "AES-256-GCM");
        assert_eq!(cipher.key_size(), 32);
        assert_eq!(cipher.iv_size(), 12);
    }

    #[test]
    fn test_aes_gcm_invalid_key_size() {
        let key = vec![0u8; 16]; // Invalid key size
        let result = AesGcmCipher::new(&key);
        assert!(result.is_err());
    }

    #[test]
    fn test_chacha20_cipher_properties() {
        let key = vec![0u8; 32];
        let cipher = ChaCha20Poly1305Cipher::new(&key).unwrap();
        assert_eq!(cipher.algorithm(), "ChaCha20-Poly1305");
        assert_eq!(cipher.key_size(), 32);
        assert_eq!(cipher.iv_size(), 12);
    }

    #[test]
    fn test_chacha20_cipher() {
        let key = vec![0u8; 32]; // 256-bit key
        let cipher = ChaCha20Poly1305Cipher::new(&key).unwrap();
        let plaintext = b"Hello, ChaCha20!";
        let nonce = vec![0u8; 12];
        let aad = b"additional data";

        // Test encryption
        let (ciphertext, tag) = cipher.encrypt(plaintext, &nonce, aad).unwrap();
        assert!(!ciphertext.is_empty());
        assert_eq!(tag.len(), 16);

        // Test decryption
        let decrypted = cipher.decrypt(&ciphertext, &nonce, &tag, aad).unwrap();
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn test_chacha20_invalid_key_size() {
        let key = vec![0u8; 16]; // Invalid key size
        let result = ChaCha20Poly1305Cipher::new(&key);
        assert!(result.is_err());
    }
}
