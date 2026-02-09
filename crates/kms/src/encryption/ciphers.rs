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

//! Cipher implementations for object encryption

use crate::error::{KmsError, Result};
use crate::types::EncryptionAlgorithm;
use aes_gcm::{
    Aes256Gcm, Key, Nonce,
    aead::{Aead, KeyInit},
};
use chacha20poly1305::ChaCha20Poly1305;
use rand::RngExt;

/// Trait for object encryption ciphers
#[cfg_attr(not(test), allow(dead_code))]
pub trait ObjectCipher: Send + Sync {
    /// Encrypt data with the given IV and AAD
    fn encrypt(&self, plaintext: &[u8], iv: &[u8], aad: &[u8]) -> Result<(Vec<u8>, Vec<u8>)>;

    /// Decrypt data with the given IV, tag, and AAD
    fn decrypt(&self, ciphertext: &[u8], iv: &[u8], tag: &[u8], aad: &[u8]) -> Result<Vec<u8>>;

    /// Get the algorithm name
    fn algorithm(&self) -> &'static str;

    /// Get the required key size in bytes
    fn key_size(&self) -> usize;

    /// Get the required IV size in bytes
    fn iv_size(&self) -> usize;

    /// Get the tag size in bytes
    fn tag_size(&self) -> usize;
}

/// AES-256-GCM cipher implementation
pub struct AesCipher {
    cipher: Aes256Gcm,
}

impl AesCipher {
    /// Create a new AES cipher with the given key
    ///
    /// #Arguments
    /// * `key` - A byte slice representing the AES-256 key (32 bytes)
    ///
    /// #Errors
    /// Returns `KmsError` if the key size is invalid
    ///
    /// #Returns
    /// A Result containing the AesCipher instance
    ///
    pub fn new(key: &[u8]) -> Result<Self> {
        if key.len() != 32 {
            return Err(KmsError::invalid_key_size(32, key.len()));
        }

        let key = Key::<Aes256Gcm>::try_from(key).map_err(|_| KmsError::cryptographic_error("key", "Invalid key length"))?;
        let cipher = Aes256Gcm::new(&key);

        Ok(Self { cipher })
    }
}

impl ObjectCipher for AesCipher {
    fn encrypt(&self, plaintext: &[u8], iv: &[u8], aad: &[u8]) -> Result<(Vec<u8>, Vec<u8>)> {
        if iv.len() != 12 {
            return Err(KmsError::invalid_key_size(12, iv.len()));
        }

        let nonce = Nonce::try_from(iv).map_err(|_| KmsError::cryptographic_error("nonce", "Invalid nonce length"))?;

        // AES-GCM includes the tag in the ciphertext
        let ciphertext_with_tag = self
            .cipher
            .encrypt(&nonce, aes_gcm::aead::Payload { msg: plaintext, aad })
            .map_err(KmsError::from_aes_gcm_error)?;

        // Split ciphertext and tag
        let tag_size = self.tag_size();
        if ciphertext_with_tag.len() < tag_size {
            return Err(KmsError::cryptographic_error("AES-GCM encrypt", "Ciphertext too short for tag"));
        }

        let (ciphertext, tag) = ciphertext_with_tag.split_at(ciphertext_with_tag.len() - tag_size);

        Ok((ciphertext.to_vec(), tag.to_vec()))
    }

    fn decrypt(&self, ciphertext: &[u8], iv: &[u8], tag: &[u8], aad: &[u8]) -> Result<Vec<u8>> {
        if iv.len() != 12 {
            return Err(KmsError::invalid_key_size(12, iv.len()));
        }

        if tag.len() != self.tag_size() {
            return Err(KmsError::invalid_key_size(self.tag_size(), tag.len()));
        }

        let nonce = Nonce::try_from(iv).map_err(|_| KmsError::cryptographic_error("nonce", "Invalid nonce length"))?;

        // Combine ciphertext and tag for AES-GCM
        let mut ciphertext_with_tag = ciphertext.to_vec();
        ciphertext_with_tag.extend_from_slice(tag);

        let plaintext = self
            .cipher
            .decrypt(
                &nonce,
                aes_gcm::aead::Payload {
                    msg: &ciphertext_with_tag,
                    aad,
                },
            )
            .map_err(KmsError::from_aes_gcm_error)?;

        Ok(plaintext)
    }

    fn algorithm(&self) -> &'static str {
        "AES-256-GCM"
    }

    fn key_size(&self) -> usize {
        32 // 256 bits
    }

    fn iv_size(&self) -> usize {
        12 // 96 bits for GCM
    }

    fn tag_size(&self) -> usize {
        16 // 128 bits
    }
}

/// ChaCha20-Poly1305 cipher implementation
pub struct ChaCha20Cipher {
    cipher: ChaCha20Poly1305,
}

impl ChaCha20Cipher {
    /// Create a new ChaCha20 cipher with the given key
    ///
    /// #Arguments
    /// * `key` - A byte slice representing the ChaCha20-Poly1305 key (32 bytes)
    ///
    /// #Errors
    /// Returns `KmsError` if the key size is invalid
    ///
    /// #Returns
    /// A Result containing the ChaCha20Cipher instance
    ///
    pub fn new(key: &[u8]) -> Result<Self> {
        if key.len() != 32 {
            return Err(KmsError::invalid_key_size(32, key.len()));
        }

        let key = chacha20poly1305::Key::try_from(key).map_err(|_| KmsError::cryptographic_error("key", "Invalid key length"))?;
        let cipher = ChaCha20Poly1305::new(&key);

        Ok(Self { cipher })
    }
}

impl ObjectCipher for ChaCha20Cipher {
    fn encrypt(&self, plaintext: &[u8], iv: &[u8], aad: &[u8]) -> Result<(Vec<u8>, Vec<u8>)> {
        if iv.len() != 12 {
            return Err(KmsError::invalid_key_size(12, iv.len()));
        }

        let nonce =
            chacha20poly1305::Nonce::try_from(iv).map_err(|_| KmsError::cryptographic_error("nonce", "Invalid nonce length"))?;

        // ChaCha20-Poly1305 includes the tag in the ciphertext
        let ciphertext_with_tag = self
            .cipher
            .encrypt(&nonce, chacha20poly1305::aead::Payload { msg: plaintext, aad })
            .map_err(KmsError::from_chacha20_error)?;

        // Split ciphertext and tag
        let tag_size = self.tag_size();
        if ciphertext_with_tag.len() < tag_size {
            return Err(KmsError::cryptographic_error("ChaCha20-Poly1305 encrypt", "Ciphertext too short for tag"));
        }

        let (ciphertext, tag) = ciphertext_with_tag.split_at(ciphertext_with_tag.len() - tag_size);

        Ok((ciphertext.to_vec(), tag.to_vec()))
    }

    fn decrypt(&self, ciphertext: &[u8], iv: &[u8], tag: &[u8], aad: &[u8]) -> Result<Vec<u8>> {
        if iv.len() != 12 {
            return Err(KmsError::invalid_key_size(12, iv.len()));
        }

        if tag.len() != self.tag_size() {
            return Err(KmsError::invalid_key_size(self.tag_size(), tag.len()));
        }

        let nonce =
            chacha20poly1305::Nonce::try_from(iv).map_err(|_| KmsError::cryptographic_error("nonce", "Invalid nonce length"))?;

        // Combine ciphertext and tag for ChaCha20-Poly1305
        let mut ciphertext_with_tag = ciphertext.to_vec();
        ciphertext_with_tag.extend_from_slice(tag);

        let plaintext = self
            .cipher
            .decrypt(
                &nonce,
                chacha20poly1305::aead::Payload {
                    msg: &ciphertext_with_tag,
                    aad,
                },
            )
            .map_err(KmsError::from_chacha20_error)?;

        Ok(plaintext)
    }

    fn algorithm(&self) -> &'static str {
        "ChaCha20-Poly1305"
    }

    fn key_size(&self) -> usize {
        32 // 256 bits
    }

    fn iv_size(&self) -> usize {
        12 // 96 bits
    }

    fn tag_size(&self) -> usize {
        16 // 128 bits
    }
}

/// Create a cipher instance for the given algorithm and key
///
/// #Arguments
/// * `algorithm` - The encryption algorithm to use
/// * `key` - A byte slice representing the encryption key
///
/// #Returns
/// A Result containing a boxed ObjectCipher instance
///
pub fn create_cipher(algorithm: &EncryptionAlgorithm, key: &[u8]) -> Result<Box<dyn ObjectCipher>> {
    match algorithm {
        EncryptionAlgorithm::Aes256 | EncryptionAlgorithm::AwsKms => Ok(Box::new(AesCipher::new(key)?)),
        EncryptionAlgorithm::ChaCha20Poly1305 => Ok(Box::new(ChaCha20Cipher::new(key)?)),
    }
}

/// Generate a random IV for the given algorithm
///
/// #Arguments
/// * `algorithm` - The encryption algorithm for which to generate the IV
///
/// #Returns
/// A vector containing the generated IV bytes
///
pub fn generate_iv(algorithm: &EncryptionAlgorithm) -> Vec<u8> {
    let iv_size = match algorithm {
        EncryptionAlgorithm::Aes256 | EncryptionAlgorithm::AwsKms => 12,
        EncryptionAlgorithm::ChaCha20Poly1305 => 12,
    };

    let mut iv = vec![0u8; iv_size];
    rand::rng().fill(&mut iv[..]);
    iv
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_aes_cipher() {
        let key = [0u8; 32]; // 256-bit key
        let cipher = AesCipher::new(&key).expect("Failed to create AES cipher");

        let plaintext = b"Hello, World!";
        let iv = [0u8; 12]; // 96-bit IV
        let aad = b"additional data";

        // Test encryption
        let (ciphertext, tag) = cipher.encrypt(plaintext, &iv, aad).expect("Encryption failed");
        assert!(!ciphertext.is_empty());
        assert_eq!(tag.len(), 16); // 128-bit tag

        // Test decryption
        let decrypted = cipher.decrypt(&ciphertext, &iv, &tag, aad).expect("Decryption failed");
        assert_eq!(decrypted, plaintext);

        // Test properties
        assert_eq!(cipher.algorithm(), "AES-256-GCM");
        assert_eq!(cipher.key_size(), 32);
        assert_eq!(cipher.iv_size(), 12);
        assert_eq!(cipher.tag_size(), 16);
    }

    #[test]
    fn test_chacha20_cipher() {
        let key = [0u8; 32]; // 256-bit key
        let cipher = ChaCha20Cipher::new(&key).expect("Failed to create ChaCha20 cipher");

        let plaintext = b"Hello, ChaCha20!";
        let iv = [0u8; 12]; // 96-bit IV
        let aad = b"additional data";

        // Test encryption
        let (ciphertext, tag) = cipher.encrypt(plaintext, &iv, aad).expect("Encryption failed");
        assert!(!ciphertext.is_empty());
        assert_eq!(tag.len(), 16); // 128-bit tag

        // Test decryption
        let decrypted = cipher.decrypt(&ciphertext, &iv, &tag, aad).expect("Decryption failed");
        assert_eq!(decrypted, plaintext);

        // Test properties
        assert_eq!(cipher.algorithm(), "ChaCha20-Poly1305");
        assert_eq!(cipher.key_size(), 32);
        assert_eq!(cipher.iv_size(), 12);
        assert_eq!(cipher.tag_size(), 16);
    }

    #[test]
    fn test_create_cipher() {
        let key = [0u8; 32];

        // Test AES creation
        let aes_cipher = create_cipher(&EncryptionAlgorithm::Aes256, &key).expect("Failed to create AES cipher");
        assert_eq!(aes_cipher.algorithm(), "AES-256-GCM");

        // Test ChaCha20 creation
        let chacha_cipher =
            create_cipher(&EncryptionAlgorithm::ChaCha20Poly1305, &key).expect("Failed to create ChaCha20 cipher");
        assert_eq!(chacha_cipher.algorithm(), "ChaCha20-Poly1305");
    }

    #[test]
    fn test_generate_iv() {
        let aes_iv = generate_iv(&EncryptionAlgorithm::Aes256);
        assert_eq!(aes_iv.len(), 12);

        let chacha_iv = generate_iv(&EncryptionAlgorithm::ChaCha20Poly1305);
        assert_eq!(chacha_iv.len(), 12);

        // IVs should be different
        let another_aes_iv = generate_iv(&EncryptionAlgorithm::Aes256);
        assert_ne!(aes_iv, another_aes_iv);
    }

    #[test]
    fn test_invalid_key_size() {
        let short_key = [0u8; 16]; // Too short

        assert!(AesCipher::new(&short_key).is_err());
        assert!(ChaCha20Cipher::new(&short_key).is_err());
    }

    #[test]
    fn test_invalid_iv_size() {
        let key = [0u8; 32];
        let cipher = AesCipher::new(&key).expect("Failed to create cipher");

        let plaintext = b"test";
        let short_iv = [0u8; 8]; // Too short
        let aad = b"";

        assert!(cipher.encrypt(plaintext, &short_iv, aad).is_err());
    }
}
