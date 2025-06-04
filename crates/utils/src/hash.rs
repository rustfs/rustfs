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

impl HashAlgorithm {
    /// Hash the input data and return the hash result as Vec<u8>.
    pub fn hash_encode(&self, data: &[u8]) -> Vec<u8> {
        match self {
            HashAlgorithm::Md5 => Md5::digest(data).to_vec(),
            HashAlgorithm::HighwayHash256 => {
                let mut hasher = HighwayHasher::new(Key(HIGHWAY_HASH256_KEY));
                hasher.append(data);
                hasher.finalize256().iter().flat_map(|&n| n.to_le_bytes()).collect()
            }
            HashAlgorithm::SHA256 => Sha256::digest(data).to_vec(),
            HashAlgorithm::HighwayHash256S => {
                let mut hasher = HighwayHasher::new(Key(HIGHWAY_HASH256_KEY));
                hasher.append(data);
                hasher.finalize256().iter().flat_map(|&n| n.to_le_bytes()).collect()
            }
            HashAlgorithm::BLAKE2b512 => blake3::hash(data).as_bytes().to_vec(),
            HashAlgorithm::None => Vec::new(),
        }
    }

    /// Return the output size in bytes for the hash algorithm.
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
        assert_eq!(hash.len(), 0);
    }

    #[test]
    fn test_hash_encode_md5() {
        let data = b"test data";
        let hash = HashAlgorithm::Md5.hash_encode(data);
        assert_eq!(hash.len(), 16);
        // MD5 should be deterministic
        let hash2 = HashAlgorithm::Md5.hash_encode(data);
        assert_eq!(hash, hash2);
    }

    #[test]
    fn test_hash_encode_highway() {
        let data = b"test data";
        let hash = HashAlgorithm::HighwayHash256.hash_encode(data);
        assert_eq!(hash.len(), 32);
        // HighwayHash should be deterministic
        let hash2 = HashAlgorithm::HighwayHash256.hash_encode(data);
        assert_eq!(hash, hash2);
    }

    #[test]
    fn test_hash_encode_sha256() {
        let data = b"test data";
        let hash = HashAlgorithm::SHA256.hash_encode(data);
        assert_eq!(hash.len(), 32);
        // SHA256 should be deterministic
        let hash2 = HashAlgorithm::SHA256.hash_encode(data);
        assert_eq!(hash, hash2);
    }

    #[test]
    fn test_hash_encode_blake2b512() {
        let data = b"test data";
        let hash = HashAlgorithm::BLAKE2b512.hash_encode(data);
        assert_eq!(hash.len(), 32); // blake3 outputs 32 bytes by default
                                    // BLAKE2b512 should be deterministic
        let hash2 = HashAlgorithm::BLAKE2b512.hash_encode(data);
        assert_eq!(hash, hash2);
    }

    #[test]
    fn test_different_data_different_hashes() {
        let data1 = b"test data 1";
        let data2 = b"test data 2";

        let md5_hash1 = HashAlgorithm::Md5.hash_encode(data1);
        let md5_hash2 = HashAlgorithm::Md5.hash_encode(data2);
        assert_ne!(md5_hash1, md5_hash2);

        let highway_hash1 = HashAlgorithm::HighwayHash256.hash_encode(data1);
        let highway_hash2 = HashAlgorithm::HighwayHash256.hash_encode(data2);
        assert_ne!(highway_hash1, highway_hash2);

        let sha256_hash1 = HashAlgorithm::SHA256.hash_encode(data1);
        let sha256_hash2 = HashAlgorithm::SHA256.hash_encode(data2);
        assert_ne!(sha256_hash1, sha256_hash2);

        let blake_hash1 = HashAlgorithm::BLAKE2b512.hash_encode(data1);
        let blake_hash2 = HashAlgorithm::BLAKE2b512.hash_encode(data2);
        assert_ne!(blake_hash1, blake_hash2);
    }
}
