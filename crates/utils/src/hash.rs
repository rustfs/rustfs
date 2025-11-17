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

use highway::{HighwayHash, HighwayHasher, Key};
use md5::{Digest, Md5};
use serde::{Deserialize, Serialize};
use sha2::Sha256;

/// The fixed key for HighwayHash256. DO NOT change for compatibility.
const HIGHWAY_HASH256_KEY: [u64; 4] = [3, 4, 2, 1];

#[derive(Serialize, Deserialize, Debug, PartialEq, Default, Clone, Eq, Hash)]
/// Supported hash algorithms for bitrot protection.
pub enum HashAlgorithm {
    // SHA256 represents the SHA-256 hash function
    SHA256,
    // HighwayHash256 represents the HighwayHash-256 hash function
    HighwayHash256,
    // HighwayHash256S represents the Streaming HighwayHash-256 hash function
    #[default]
    HighwayHash256S,
    // BLAKE2b512 represents the BLAKE2b-512 hash function
    BLAKE2b512,
    /// MD5 (128-bit)
    Md5,
    /// No hash (for testing or unprotected data)
    None,
}

enum HashEncoded {
    Md5([u8; 16]),
    Sha256([u8; 32]),
    HighwayHash256([u8; 32]),
    HighwayHash256S([u8; 32]),
    Blake2b512(blake3::Hash),
    None,
}

impl AsRef<[u8]> for HashEncoded {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        match self {
            HashEncoded::Md5(hash) => hash.as_ref(),
            HashEncoded::Sha256(hash) => hash.as_ref(),
            HashEncoded::HighwayHash256(hash) => hash.as_ref(),
            HashEncoded::HighwayHash256S(hash) => hash.as_ref(),
            HashEncoded::Blake2b512(hash) => hash.as_bytes(),
            HashEncoded::None => &[],
        }
    }
}

#[inline]
fn u8x32_from_u64x4(input: [u64; 4]) -> [u8; 32] {
    let mut output = [0u8; 32];
    for (i, &n) in input.iter().enumerate() {
        output[i * 8..(i + 1) * 8].copy_from_slice(&n.to_le_bytes());
    }
    output
}

impl HashAlgorithm {
    /// Hash the input data and return the hash result as Vec<u8>.
    ///
    /// # Arguments
    /// * `data` - A byte slice representing the data to be hashed
    ///
    /// # Returns
    /// A byte slice containing the hash of the input data
    ///
    pub fn hash_encode(&self, data: &[u8]) -> impl AsRef<[u8]> {
        match self {
            HashAlgorithm::Md5 => HashEncoded::Md5(Md5::digest(data).into()),
            HashAlgorithm::HighwayHash256 => {
                let mut hasher = HighwayHasher::new(Key(HIGHWAY_HASH256_KEY));
                hasher.append(data);
                HashEncoded::HighwayHash256(u8x32_from_u64x4(hasher.finalize256()))
            }
            HashAlgorithm::SHA256 => HashEncoded::Sha256(Sha256::digest(data).into()),
            HashAlgorithm::HighwayHash256S => {
                let mut hasher = HighwayHasher::new(Key(HIGHWAY_HASH256_KEY));
                hasher.append(data);
                HashEncoded::HighwayHash256S(u8x32_from_u64x4(hasher.finalize256()))
            }
            HashAlgorithm::BLAKE2b512 => HashEncoded::Blake2b512(blake3::hash(data)),
            HashAlgorithm::None => HashEncoded::None,
        }
    }

    /// Return the output size in bytes for the hash algorithm.
    ///
    /// # Returns
    /// The size in bytes of the hash output
    ///
    pub fn size(&self) -> usize {
        match self {
            HashAlgorithm::SHA256 => 32,
            HashAlgorithm::HighwayHash256 => 32,
            HashAlgorithm::HighwayHash256S => 32,
            HashAlgorithm::BLAKE2b512 => 32, // blake3 outputs 32 bytes by default
            HashAlgorithm::Md5 => 16,
            HashAlgorithm::None => 0,
        }
    }
}

use siphasher::sip::SipHasher;

pub const EMPTY_STRING_SHA256_HASH: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

pub const DEFAULT_SIP_HASH_KEY: [u8; 16] = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];

/// SipHash function to hash a string key into a bucket index.
///
/// # Arguments
/// * `key` - The input string to be hashed
/// * `cardinality` - The number of buckets
/// * `id` - A 16-byte array used as the SipHash key
///
/// # Returns
/// A usize representing the bucket index
///
pub fn sip_hash(key: &str, cardinality: usize, id: &[u8; 16]) -> usize {
    // Your key, must be 16 bytes

    // Calculate SipHash value of the string
    let result = SipHasher::new_with_key(id).hash(key.as_bytes());

    (result as usize) % cardinality
}

/// CRC32 hash function to hash a string key into a bucket index.
///
/// # Arguments
/// * `key` - The input string to be hashed
/// * `cardinality` - The number of buckets
///
/// # Returns
/// A usize representing the bucket index
///
pub fn crc_hash(key: &str, cardinality: usize) -> usize {
    let mut hasher = crc_fast::Digest::new(crc_fast::CrcAlgorithm::Crc32IsoHdlc);
    hasher.update(key.as_bytes());
    let checksum = hasher.finalize() as u32;

    checksum as usize % cardinality
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hash_algorithm_sizes() {
        assert_eq!(HashAlgorithm::Md5.size(), 16);
        assert_eq!(HashAlgorithm::HighwayHash256.size(), 32);
        assert_eq!(HashAlgorithm::HighwayHash256S.size(), 32);
        assert_eq!(HashAlgorithm::SHA256.size(), 32);
        assert_eq!(HashAlgorithm::BLAKE2b512.size(), 32);
        assert_eq!(HashAlgorithm::None.size(), 0);
    }

    #[test]
    fn test_hash_encode_none() {
        let data = b"test data";
        let hash = HashAlgorithm::None.hash_encode(data);
        let hash = hash.as_ref();
        assert_eq!(hash.len(), 0);
    }

    #[test]
    fn test_hash_encode_md5() {
        let data = b"test data";
        let hash = HashAlgorithm::Md5.hash_encode(data);
        let hash = hash.as_ref();
        assert_eq!(hash.len(), 16);
        // MD5 should be deterministic
        let hash2 = HashAlgorithm::Md5.hash_encode(data);
        let hash2 = hash2.as_ref();
        assert_eq!(hash, hash2);
    }

    #[test]
    fn test_hash_encode_highway() {
        let data = b"test data";
        let hash = HashAlgorithm::HighwayHash256.hash_encode(data);
        let hash = hash.as_ref();
        assert_eq!(hash.len(), 32);
        // HighwayHash should be deterministic
        let hash2 = HashAlgorithm::HighwayHash256.hash_encode(data);
        let hash2 = hash2.as_ref();
        assert_eq!(hash, hash2);
    }

    #[test]
    fn test_hash_encode_sha256() {
        let data = b"test data";
        let hash = HashAlgorithm::SHA256.hash_encode(data);
        let hash = hash.as_ref();
        assert_eq!(hash.len(), 32);
        // SHA256 should be deterministic
        let hash2 = HashAlgorithm::SHA256.hash_encode(data);
        let hash2 = hash2.as_ref();
        assert_eq!(hash, hash2);
    }

    #[test]
    fn test_hash_encode_blake2b512() {
        let data = b"test data";
        let hash = HashAlgorithm::BLAKE2b512.hash_encode(data);
        let hash = hash.as_ref();
        assert_eq!(hash.len(), 32); // blake3 outputs 32 bytes by default
        // BLAKE2b512 should be deterministic
        let hash2 = HashAlgorithm::BLAKE2b512.hash_encode(data);
        let hash2 = hash2.as_ref();
        assert_eq!(hash, hash2);
    }

    #[test]
    fn test_different_data_different_hashes() {
        let data1 = b"test data 1";
        let data2 = b"test data 2";

        let md5_hash1 = HashAlgorithm::Md5.hash_encode(data1);
        let md5_hash2 = HashAlgorithm::Md5.hash_encode(data2);
        assert_ne!(md5_hash1.as_ref(), md5_hash2.as_ref());

        let highway_hash1 = HashAlgorithm::HighwayHash256.hash_encode(data1);
        let highway_hash2 = HashAlgorithm::HighwayHash256.hash_encode(data2);
        assert_ne!(highway_hash1.as_ref(), highway_hash2.as_ref());

        let sha256_hash1 = HashAlgorithm::SHA256.hash_encode(data1);
        let sha256_hash2 = HashAlgorithm::SHA256.hash_encode(data2);
        assert_ne!(sha256_hash1.as_ref(), sha256_hash2.as_ref());

        let blake_hash1 = HashAlgorithm::BLAKE2b512.hash_encode(data1);
        let blake_hash2 = HashAlgorithm::BLAKE2b512.hash_encode(data2);
        assert_ne!(blake_hash1.as_ref(), blake_hash2.as_ref());
    }
}
