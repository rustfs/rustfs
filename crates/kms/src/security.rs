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

//! Security utilities for memory-safe key handling

use secrecy::{ExposeSecret, Secret};
use zeroize::ZeroizeOnDrop;

/// A secure wrapper for cryptographic keys that automatically zeroizes memory on drop
#[derive(ZeroizeOnDrop)]
pub struct SecretKey {
    #[zeroize(skip)]
    inner: Secret<Vec<u8>>,
}

impl SecretKey {
    /// Create a new SecretKey from raw bytes
    pub fn new(key: Vec<u8>) -> Self {
        Self { inner: Secret::new(key) }
    }

    /// Create a SecretKey from a slice
    pub fn from_slice(key: &[u8]) -> Self {
        Self::new(key.to_vec())
    }

    /// Expose the secret key for cryptographic operations
    ///
    /// # Security
    /// The exposed reference should be used immediately and not stored
    pub fn expose_secret(&self) -> &[u8] {
        self.inner.expose_secret()
    }

    /// Get the length of the key
    pub fn len(&self) -> usize {
        self.inner.expose_secret().len()
    }

    /// Check if the key is empty
    pub fn is_empty(&self) -> bool {
        self.inner.expose_secret().is_empty()
    }
}

impl Clone for SecretKey {
    fn clone(&self) -> Self {
        Self::new(self.inner.expose_secret().to_vec())
    }
}

impl std::fmt::Debug for SecretKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SecretKey").field("len", &self.len()).finish()
    }
}

/// A secure vector that zeroizes its contents on drop
#[derive(ZeroizeOnDrop)]
pub struct SecretVec {
    data: Vec<u8>,
}

impl SecretVec {
    /// Create a new SecretVec
    pub fn new(data: Vec<u8>) -> Self {
        Self { data }
    }

    /// Create a SecretVec with the specified capacity
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            data: Vec::with_capacity(capacity),
        }
    }

    /// Get a reference to the data
    pub fn as_slice(&self) -> &[u8] {
        &self.data
    }

    /// Get a mutable reference to the data
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        &mut self.data
    }

    /// Get the length of the data
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Check if the vector is empty
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Push a byte to the vector
    pub fn push(&mut self, value: u8) {
        self.data.push(value);
    }

    /// Extend the vector with data from a slice
    pub fn extend_from_slice(&mut self, other: &[u8]) {
        self.data.extend_from_slice(other);
    }
}

impl std::fmt::Debug for SecretVec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SecretVec").field("len", &self.len()).finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_secret_key_creation() {
        let key_data = vec![1, 2, 3, 4, 5];
        let secret_key = SecretKey::new(key_data.clone());

        assert_eq!(secret_key.len(), 5);
        assert_eq!(secret_key.expose_secret(), &key_data);
        assert!(!secret_key.is_empty());
    }

    #[test]
    fn test_secret_key_from_slice() {
        let key_data = [1, 2, 3, 4, 5];
        let secret_key = SecretKey::from_slice(&key_data);

        assert_eq!(secret_key.len(), 5);
        assert_eq!(secret_key.expose_secret(), &key_data);
    }

    #[test]
    fn test_secret_key_clone() {
        let key_data = vec![1, 2, 3, 4, 5];
        let secret_key = SecretKey::new(key_data.clone());
        let cloned_key = secret_key.clone();

        assert_eq!(secret_key.expose_secret(), cloned_key.expose_secret());
    }

    #[test]
    fn test_secret_vec() {
        let mut secret_vec = SecretVec::with_capacity(10);

        assert!(secret_vec.is_empty());
        assert_eq!(secret_vec.len(), 0);

        secret_vec.push(42);
        assert_eq!(secret_vec.len(), 1);
        assert_eq!(secret_vec.as_slice()[0], 42);

        secret_vec.extend_from_slice(&[1, 2, 3]);
        assert_eq!(secret_vec.len(), 4);
        assert_eq!(secret_vec.as_slice(), &[42, 1, 2, 3]);
    }

    #[test]
    fn test_debug_formatting() {
        let secret_key = SecretKey::new(vec![1, 2, 3, 4, 5]);
        let debug_str = format!("{secret_key:?}");

        // Should not contain the actual key data
        assert!(!debug_str.contains("1"));
        assert!(!debug_str.contains("2"));
        assert!(debug_str.contains("len"));
        assert!(debug_str.contains("5"));
    }
}
