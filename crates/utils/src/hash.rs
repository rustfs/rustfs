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

use blake2::{Blake2b512, Digest as Blake2Digest};
use highway::{HighwayHash, HighwayHasher, Key};
use md5::Md5;
use serde::{Deserialize, Serialize};
use sha2::Sha256;

/// Magic HH-256 key: HH-256 hash of first 100 decimals of π as utf-8 with zero key.
const MAGIC_HIGHWAY_HASH256_KEY: [u8; 32] = [
    0x4b, 0xe7, 0x34, 0xfa, 0x8e, 0x23, 0x8a, 0xcd, 0x26, 0x3e, 0x83, 0xe6, 0xbb, 0x96, 0x85, 0x52, 0x04, 0x0f, 0x93, 0x5d, 0xa3,
    0x9f, 0x44, 0x14, 0x97, 0xe0, 0x9d, 0x13, 0x22, 0xde, 0x36, 0xa0,
];

/// Legacy HH-256 key (main branch): fixed [3,4,2,1] as u64 LE.
const LEGACY_HIGHWAY_HASH256_KEY: [u8; 32] = [
    3, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0,
];

fn highway_key_from_bytes(bytes: &[u8; 32]) -> [u64; 4] {
    let mut key = [0u64; 4];
    for (i, chunk) in bytes.chunks_exact(8).enumerate() {
        key[i] = u64::from_le_bytes(chunk.try_into().unwrap());
    }
    key
}

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
    /// Legacy HighwayHash256S (main branch) with fixed key [3,4,2,1]
    HighwayHash256SLegacy,
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
    HighwayHash256SLegacy([u8; 32]),
    Blake2b512([u8; 64]),
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
            HashEncoded::HighwayHash256SLegacy(hash) => hash.as_ref(),
            HashEncoded::Blake2b512(hash) => hash.as_ref(),
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
                let key = Key(highway_key_from_bytes(&MAGIC_HIGHWAY_HASH256_KEY));
                let mut hasher = HighwayHasher::new(key);
                hasher.append(data);
                HashEncoded::HighwayHash256(u8x32_from_u64x4(hasher.finalize256()))
            }
            HashAlgorithm::SHA256 => HashEncoded::Sha256(Sha256::digest(data).into()),
            HashAlgorithm::HighwayHash256S => {
                let key = Key(highway_key_from_bytes(&MAGIC_HIGHWAY_HASH256_KEY));
                let mut hasher = HighwayHasher::new(key);
                hasher.append(data);
                HashEncoded::HighwayHash256S(u8x32_from_u64x4(hasher.finalize256()))
            }
            HashAlgorithm::HighwayHash256SLegacy => {
                let key = Key(highway_key_from_bytes(&LEGACY_HIGHWAY_HASH256_KEY));
                let mut hasher = HighwayHasher::new(key);
                hasher.append(data);
                HashEncoded::HighwayHash256SLegacy(u8x32_from_u64x4(hasher.finalize256()))
            }
            HashAlgorithm::BLAKE2b512 => {
                let hash = Blake2b512::digest(data);
                let mut out = [0u8; 64];
                out.copy_from_slice(hash.as_ref());
                HashEncoded::Blake2b512(out)
            }
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
            HashAlgorithm::HighwayHash256SLegacy => 32,
            HashAlgorithm::BLAKE2b512 => 64,
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
        assert_eq!(HashAlgorithm::BLAKE2b512.size(), 64);
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
        assert_eq!(hash.len(), 64);
        // BLAKE2b512 should be deterministic
        let hash2 = HashAlgorithm::BLAKE2b512.hash_encode(data);
        let hash2 = hash2.as_ref();
        assert_eq!(hash, hash2);
    }

    #[test]
    fn test_bitrot_selftest() {
        let checksums: [(HashAlgorithm, &str); 5] = [
            (HashAlgorithm::SHA256, "a7677ff19e0182e4d52e3a3db727804abc82a5818749336369552e54b838b004"),
            (
                HashAlgorithm::BLAKE2b512,
                "e519b7d84b1c3c917985f544773a35cf265dcab10948be3550320d156bab612124a5ae2ae5a8c73c0eea360f68b0e28136f26e858756dbfe7375a7389f26c669",
            ),
            (
                HashAlgorithm::HighwayHash256,
                "39c0407ed3f01b18d22c85db4aeff11e060ca5f43131b0126731ca197cd42313",
            ),
            (
                HashAlgorithm::HighwayHash256S,
                "39c0407ed3f01b18d22c85db4aeff11e060ca5f43131b0126731ca197cd42313",
            ),
            (
                HashAlgorithm::HighwayHash256SLegacy,
                "a5592a831588836b0f61bff43da4bd957c376d9b6412a9ecbbd144a3ecf34649",
            ),
        ];
        for (algo, expected_hex) in checksums {
            let block_size = match algo {
                HashAlgorithm::SHA256 => 64,
                HashAlgorithm::BLAKE2b512 => 128,
                HashAlgorithm::HighwayHash256 | HashAlgorithm::HighwayHash256S | HashAlgorithm::HighwayHash256SLegacy => 32,
                _ => continue,
            };
            let mut msg = Vec::new();
            let mut sum = Vec::new();
            for _ in 0..block_size {
                sum = algo.hash_encode(&msg).as_ref().to_vec();
                msg.extend_from_slice(&sum);
            }
            let got = hex_simd::encode_to_string(&sum, hex_simd::AsciiCase::Lower);
            assert_eq!(got, expected_hex, "{:?} selftest mismatch: got {} want {}", algo, got, expected_hex);
        }
    }

    /// Generates 7557 bytes
    /// Pattern: (i*7+13)%256 for each byte.
    fn generate_compat_test_data(size: usize) -> Vec<u8> {
        (0..size).map(|i| ((i * 7 + 13) % 256) as u8).collect()
    }

    /// Run: cargo test -p rustfs-utils test_highwayhash_compat
    #[test]
    fn test_highwayhash_compat() {
        let data = generate_compat_test_data(7557);
        let hash = HashAlgorithm::HighwayHash256S.hash_encode(&data);
        let got = hex_simd::encode_to_string(hash.as_ref(), hex_simd::AsciiCase::Lower);
        let expected = "06543bf1c637e67386922a43b71cca08e5faa0f9131105a2bf96dec880529551";
        assert_eq!(got, expected, "HighwayHash256S must match: got {} want {}", got, expected);
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
