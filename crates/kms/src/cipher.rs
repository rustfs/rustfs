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
use std::io::{Result as IoResult};
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, ReadBuf};

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

/// Streaming cipher for encrypting data streams
pub struct StreamingCipher<R> {
    reader: R,
    cipher: Box<dyn ObjectCipher>,
    iv: Vec<u8>,
    buffer: Vec<u8>,
    encrypted_buffer: Vec<u8>,
    buffer_pos: usize,
    chunk_size: usize,
    finished: bool,
}

impl<R> StreamingCipher<R>
where
    R: AsyncRead + Unpin,
{
    /// Create a new streaming cipher
    pub fn new(reader: R, cipher: Box<dyn ObjectCipher>, iv: Vec<u8>) -> Self {
        Self {
            reader,
            cipher,
            iv,
            buffer: Vec::new(),
            encrypted_buffer: Vec::new(),
            buffer_pos: 0,
            chunk_size: 8192, // 8KB chunks
            finished: false,
        }
    }

    /// Set the chunk size for streaming encryption
    pub fn with_chunk_size(mut self, chunk_size: usize) -> Self {
        self.chunk_size = chunk_size;
        self
    }
}

impl<R> AsyncRead for StreamingCipher<R>
where
    R: AsyncRead + Unpin,
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<IoResult<()>> {
        let this = self.get_mut();
        
        // If we have encrypted data in buffer, return it first
        if this.buffer_pos < this.encrypted_buffer.len() {
            let remaining = this.encrypted_buffer.len() - this.buffer_pos;
            let to_copy = std::cmp::min(remaining, buf.remaining());
            
            buf.put_slice(&this.encrypted_buffer[this.buffer_pos..this.buffer_pos + to_copy]);
            this.buffer_pos += to_copy;
            
            return Poll::Ready(Ok(()));
        }

        // If we're finished and no more data in buffer, return EOF
        if this.finished {
            return Poll::Ready(Ok(()));
        }

        // Read more data from the underlying reader
        this.buffer.clear();
        let chunk_size = this.chunk_size;
        this.buffer.resize(chunk_size, 0);
        
        let mut read_buf = ReadBuf::new(&mut this.buffer);
        
        match Pin::new(&mut this.reader).poll_read(cx, &mut read_buf) {
            Poll::Ready(Ok(())) => {
                let bytes_read = read_buf.filled().len();
                
                if bytes_read == 0 {
                    this.finished = true;
                    return Poll::Ready(Ok(()));
                }

                // Encrypt the chunk
                this.buffer.truncate(bytes_read);
                
                match this.cipher.encrypt(&this.buffer, &this.iv, &[]) {
                    Ok((ciphertext, tag)) => {
                        this.encrypted_buffer.clear();
                        this.encrypted_buffer.extend_from_slice(&ciphertext);
                        this.encrypted_buffer.extend_from_slice(&tag);
                        this.buffer_pos = 0;
                        
                        // Return encrypted data
                        let to_copy = std::cmp::min(this.encrypted_buffer.len(), buf.remaining());
                        buf.put_slice(&this.encrypted_buffer[..to_copy]);
                        this.buffer_pos = to_copy;
                        
                        Poll::Ready(Ok(()))
                    }
                    Err(_) => {
                        Poll::Ready(Err(std::io::Error::new(
                            std::io::ErrorKind::Other,
                            "Encryption failed",
                        )))
                    }
                }
            }
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            Poll::Pending => Poll::Pending,
        }
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

    #[tokio::test]
    async fn test_streaming_cipher() {
        use tokio::io::AsyncReadExt;
        
        let key = vec![0u8; 32];
        let cipher = Box::new(AesGcmCipher::new(&key).unwrap()) as Box<dyn ObjectCipher>;
        let iv = vec![0u8; 12];
        let data = b"Hello, streaming encryption!";
        
        let cursor = std::io::Cursor::new(data.to_vec());
        let mut streaming_cipher = StreamingCipher::new(cursor, cipher, iv).with_chunk_size(10);
        
        let mut encrypted_data = Vec::new();
        streaming_cipher.read_to_end(&mut encrypted_data).await.unwrap();
        
        // The encrypted data should be different from the original
        assert_ne!(encrypted_data, data);
        assert!(!encrypted_data.is_empty());
    }

    #[test]
    fn test_streaming_cipher_creation() {
        let key = vec![0u8; 32];
        let cipher = Box::new(AesGcmCipher::new(&key).unwrap()) as Box<dyn ObjectCipher>;
        let iv = vec![0u8; 12];
        let data = b"test data";
        
        let cursor = std::io::Cursor::new(data.to_vec());
        let streaming_cipher = StreamingCipher::new(cursor, cipher, iv).with_chunk_size(1024);
        
        // Just test that we can create the streaming cipher
        assert_eq!(streaming_cipher.chunk_size, 1024);
    }
}
