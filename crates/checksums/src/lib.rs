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

#![cfg_attr(docsrs, feature(doc_auto_cfg))]
#![allow(clippy::derive_partial_eq_without_eq)]
#![warn(
    // missing_docs,
    rustdoc::missing_crate_level_docs,
    unreachable_pub,
    rust_2018_idioms
)]

use crate::error::UnknownChecksumAlgorithmError;

use bytes::Bytes;
use std::{fmt::Debug, str::FromStr};

mod base64;
pub mod error;
pub mod http;

pub const CRC_32_NAME: &str = "crc32";
pub const CRC_32_C_NAME: &str = "crc32c";
pub const CRC_64_NVME_NAME: &str = "crc64nvme";
pub const SHA_1_NAME: &str = "sha1";
pub const SHA_256_NAME: &str = "sha256";
pub const SHA_512_NAME: &str = "sha512";
pub const XXHASH_3_NAME: &str = "xxhash3";
pub const XXHASH_64_NAME: &str = "xxhash64";
pub const XXHASH_128_NAME: &str = "xxhash128";
pub const MD5_NAME: &str = "md5";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum ChecksumAlgorithm {
    #[default]
    Crc32,
    Crc32c,
    Sha1,
    Sha256,
    Crc64Nvme,
    Sha512,
    Xxhash3,
    Xxhash64,
    Xxhash128,
}

impl FromStr for ChecksumAlgorithm {
    type Err = UnknownChecksumAlgorithmError;

    fn from_str(checksum_algorithm: &str) -> Result<Self, Self::Err> {
        if checksum_algorithm.eq_ignore_ascii_case(CRC_32_NAME) {
            Ok(Self::Crc32)
        } else if checksum_algorithm.eq_ignore_ascii_case(CRC_32_C_NAME) {
            Ok(Self::Crc32c)
        } else if checksum_algorithm.eq_ignore_ascii_case(SHA_1_NAME) {
            Ok(Self::Sha1)
        } else if checksum_algorithm.eq_ignore_ascii_case(SHA_256_NAME) {
            Ok(Self::Sha256)
        } else if checksum_algorithm.eq_ignore_ascii_case(CRC_64_NVME_NAME) {
            Ok(Self::Crc64Nvme)
        } else if checksum_algorithm.eq_ignore_ascii_case(SHA_512_NAME) {
            Ok(Self::Sha512)
        } else if checksum_algorithm.eq_ignore_ascii_case(XXHASH_3_NAME) {
            Ok(Self::Xxhash3)
        } else if checksum_algorithm.eq_ignore_ascii_case(XXHASH_64_NAME) {
            Ok(Self::Xxhash64)
        } else if checksum_algorithm.eq_ignore_ascii_case(XXHASH_128_NAME) {
            Ok(Self::Xxhash128)
        } else {
            Err(UnknownChecksumAlgorithmError::new(checksum_algorithm))
        }
    }
}

impl ChecksumAlgorithm {
    pub fn into_impl(self) -> Box<dyn http::HttpChecksum> {
        match self {
            Self::Crc32 => Box::<Crc32>::default(),
            Self::Crc32c => Box::<Crc32c>::default(),
            Self::Crc64Nvme => Box::<Crc64Nvme>::default(),
            Self::Sha1 => Box::<Sha1>::default(),
            Self::Sha256 => Box::<Sha256>::default(),
            Self::Sha512 => Box::<Sha512>::default(),
            Self::Xxhash3 => Box::<Xxhash3>::default(),
            Self::Xxhash64 => Box::<Xxhash64>::default(),
            Self::Xxhash128 => Box::<Xxhash128>::default(),
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Crc32 => CRC_32_NAME,
            Self::Crc32c => CRC_32_C_NAME,
            Self::Crc64Nvme => CRC_64_NVME_NAME,
            Self::Sha1 => SHA_1_NAME,
            Self::Sha256 => SHA_256_NAME,
            Self::Sha512 => SHA_512_NAME,
            Self::Xxhash3 => XXHASH_3_NAME,
            Self::Xxhash64 => XXHASH_64_NAME,
            Self::Xxhash128 => XXHASH_128_NAME,
        }
    }
}

pub trait Checksum: Send + Sync {
    fn update(&mut self, bytes: &[u8]);
    fn finalize(self: Box<Self>) -> Bytes;
    fn size(&self) -> u64;
}

#[derive(Debug)]
struct Crc32 {
    hasher: crc_fast::Digest,
}

impl Default for Crc32 {
    fn default() -> Self {
        Self {
            hasher: crc_fast::Digest::new(crc_fast::CrcAlgorithm::Crc32IsoHdlc),
        }
    }
}

impl Crc32 {
    fn update(&mut self, bytes: &[u8]) {
        self.hasher.update(bytes);
    }

    fn finalize(self) -> Bytes {
        let checksum = self.hasher.finalize() as u32;

        Bytes::copy_from_slice(checksum.to_be_bytes().as_slice())
    }

    fn size() -> u64 {
        4
    }
}

impl Checksum for Crc32 {
    fn update(&mut self, bytes: &[u8]) {
        Self::update(self, bytes)
    }
    fn finalize(self: Box<Self>) -> Bytes {
        Self::finalize(*self)
    }
    fn size(&self) -> u64 {
        Self::size()
    }
}

#[derive(Debug)]
struct Crc32c {
    hasher: crc_fast::Digest,
}

impl Default for Crc32c {
    fn default() -> Self {
        Self {
            hasher: crc_fast::Digest::new(crc_fast::CrcAlgorithm::Crc32Iscsi),
        }
    }
}

impl Crc32c {
    fn update(&mut self, bytes: &[u8]) {
        self.hasher.update(bytes);
    }

    fn finalize(self) -> Bytes {
        let checksum = self.hasher.finalize() as u32;

        Bytes::copy_from_slice(checksum.to_be_bytes().as_slice())
    }

    fn size() -> u64 {
        4
    }
}

impl Checksum for Crc32c {
    fn update(&mut self, bytes: &[u8]) {
        Self::update(self, bytes)
    }
    fn finalize(self: Box<Self>) -> Bytes {
        Self::finalize(*self)
    }
    fn size(&self) -> u64 {
        Self::size()
    }
}

#[derive(Debug)]
struct Crc64Nvme {
    hasher: crc_fast::Digest,
}

impl Default for Crc64Nvme {
    fn default() -> Self {
        Self {
            hasher: crc_fast::Digest::new(crc_fast::CrcAlgorithm::Crc64Nvme),
        }
    }
}

impl Crc64Nvme {
    fn update(&mut self, bytes: &[u8]) {
        self.hasher.update(bytes);
    }

    fn finalize(self) -> Bytes {
        Bytes::copy_from_slice(self.hasher.finalize().to_be_bytes().as_slice())
    }

    fn size() -> u64 {
        8
    }
}

impl Checksum for Crc64Nvme {
    fn update(&mut self, bytes: &[u8]) {
        Self::update(self, bytes)
    }
    fn finalize(self: Box<Self>) -> Bytes {
        Self::finalize(*self)
    }
    fn size(&self) -> u64 {
        Self::size()
    }
}

#[derive(Debug, Default)]
struct Sha1 {
    hasher: sha1::Sha1,
}

impl Sha1 {
    fn update(&mut self, bytes: &[u8]) {
        use sha1::Digest;
        self.hasher.update(bytes);
    }

    fn finalize(self) -> Bytes {
        use sha1::Digest;
        Bytes::copy_from_slice(self.hasher.finalize().as_slice())
    }

    fn size() -> u64 {
        use sha1::Digest;
        sha1::Sha1::output_size() as u64
    }
}

impl Checksum for Sha1 {
    fn update(&mut self, bytes: &[u8]) {
        Self::update(self, bytes)
    }

    fn finalize(self: Box<Self>) -> Bytes {
        Self::finalize(*self)
    }
    fn size(&self) -> u64 {
        Self::size()
    }
}

#[derive(Debug, Default)]
struct Sha256 {
    hasher: sha2::Sha256,
}

impl Sha256 {
    fn update(&mut self, bytes: &[u8]) {
        use sha2::Digest;
        self.hasher.update(bytes);
    }

    fn finalize(self) -> Bytes {
        use sha2::Digest;
        Bytes::copy_from_slice(self.hasher.finalize().as_slice())
    }

    fn size() -> u64 {
        use sha2::Digest;
        sha2::Sha256::output_size() as u64
    }
}

impl Checksum for Sha256 {
    fn update(&mut self, bytes: &[u8]) {
        Self::update(self, bytes);
    }
    fn finalize(self: Box<Self>) -> Bytes {
        Self::finalize(*self)
    }
    fn size(&self) -> u64 {
        Self::size()
    }
}

#[derive(Debug, Default)]
struct Sha512 {
    hasher: sha2::Sha512,
}

impl Sha512 {
    fn update(&mut self, bytes: &[u8]) {
        use sha2::Digest;
        self.hasher.update(bytes);
    }

    fn finalize(self) -> Bytes {
        use sha2::Digest;
        Bytes::copy_from_slice(self.hasher.finalize().as_slice())
    }

    fn size() -> u64 {
        use sha2::Digest;
        sha2::Sha512::output_size() as u64
    }
}

impl Checksum for Sha512 {
    fn update(&mut self, bytes: &[u8]) {
        Self::update(self, bytes);
    }
    fn finalize(self: Box<Self>) -> Bytes {
        Self::finalize(*self)
    }
    fn size(&self) -> u64 {
        Self::size()
    }
}

/// XXH3 (64-bit) hasher with the canonical seed of 0.
///
/// The raw digest is a `u64` serialized as 8 big-endian bytes so that the value
/// matches the server-side (`rustfs-rio`) computation for the same algorithm.
struct Xxhash3 {
    hasher: xxhash_rust::xxh3::Xxh3,
}

impl Default for Xxhash3 {
    fn default() -> Self {
        Self {
            hasher: xxhash_rust::xxh3::Xxh3::new(),
        }
    }
}

impl Xxhash3 {
    fn update(&mut self, bytes: &[u8]) {
        self.hasher.update(bytes);
    }

    fn finalize(self) -> Bytes {
        Bytes::copy_from_slice(self.hasher.digest().to_be_bytes().as_slice())
    }

    fn size() -> u64 {
        8
    }
}

impl Checksum for Xxhash3 {
    fn update(&mut self, bytes: &[u8]) {
        Self::update(self, bytes)
    }
    fn finalize(self: Box<Self>) -> Bytes {
        Self::finalize(*self)
    }
    fn size(&self) -> u64 {
        Self::size()
    }
}

/// XXH3 (128-bit) hasher with the canonical seed of 0.
///
/// The raw digest is a `u128` serialized as 16 big-endian bytes.
struct Xxhash128 {
    hasher: xxhash_rust::xxh3::Xxh3,
}

impl Default for Xxhash128 {
    fn default() -> Self {
        Self {
            hasher: xxhash_rust::xxh3::Xxh3::new(),
        }
    }
}

impl Xxhash128 {
    fn update(&mut self, bytes: &[u8]) {
        self.hasher.update(bytes);
    }

    fn finalize(self) -> Bytes {
        Bytes::copy_from_slice(self.hasher.digest128().to_be_bytes().as_slice())
    }

    fn size() -> u64 {
        16
    }
}

impl Checksum for Xxhash128 {
    fn update(&mut self, bytes: &[u8]) {
        Self::update(self, bytes)
    }
    fn finalize(self: Box<Self>) -> Bytes {
        Self::finalize(*self)
    }
    fn size(&self) -> u64 {
        Self::size()
    }
}

/// XXH64 hasher with the canonical seed of 0.
///
/// The raw digest is a `u64` serialized as 8 big-endian bytes.
struct Xxhash64 {
    hasher: xxhash_rust::xxh64::Xxh64,
}

impl Default for Xxhash64 {
    fn default() -> Self {
        Self {
            hasher: xxhash_rust::xxh64::Xxh64::new(0),
        }
    }
}

impl Xxhash64 {
    fn update(&mut self, bytes: &[u8]) {
        self.hasher.update(bytes);
    }

    fn finalize(self) -> Bytes {
        Bytes::copy_from_slice(self.hasher.digest().to_be_bytes().as_slice())
    }

    fn size() -> u64 {
        8
    }
}

impl Checksum for Xxhash64 {
    fn update(&mut self, bytes: &[u8]) {
        Self::update(self, bytes)
    }
    fn finalize(self: Box<Self>) -> Bytes {
        Self::finalize(*self)
    }
    fn size(&self) -> u64 {
        Self::size()
    }
}

#[allow(dead_code)]
#[derive(Debug, Default)]
struct Md5 {
    hasher: md5::Md5,
}

#[allow(dead_code)]
impl Md5 {
    fn update(&mut self, bytes: &[u8]) {
        use md5::Digest;
        self.hasher.update(bytes);
    }

    fn finalize(self) -> Bytes {
        use md5::Digest;
        Bytes::copy_from_slice(self.hasher.finalize().as_slice())
    }

    fn size() -> u64 {
        use md5::Digest;
        md5::Md5::output_size() as u64
    }
}

impl Checksum for Md5 {
    fn update(&mut self, bytes: &[u8]) {
        Self::update(self, bytes)
    }
    fn finalize(self: Box<Self>) -> Bytes {
        Self::finalize(*self)
    }
    fn size(&self) -> u64 {
        Self::size()
    }
}

#[cfg(test)]
mod tests {
    use super::{
        Crc32, Crc32c, Md5, Sha1, Sha256,
        http::{CRC_32_C_HEADER_NAME, CRC_32_HEADER_NAME, MD5_HEADER_NAME, SHA_1_HEADER_NAME, SHA_256_HEADER_NAME},
    };

    use crate::ChecksumAlgorithm;
    use crate::http::HttpChecksum;
    use base64_simd::STANDARD;
    use http::HeaderValue;
    use pretty_assertions::assert_eq;
    use std::fmt::Write;

    const TEST_DATA: &str = r#"test data"#;

    fn base64_encoded_checksum_to_hex_string(header_value: &HeaderValue) -> String {
        let decoded_checksum = STANDARD
            .decode_to_vec(header_value.to_str().expect("checksum header value should be ASCII"))
            .expect("checksum header value should be valid base64");
        let decoded_checksum = decoded_checksum.into_iter().fold(String::new(), |mut acc, byte| {
            write!(acc, "{byte:02X?}").expect("string will always be writable");
            acc
        });

        format!("0x{decoded_checksum}")
    }

    #[test]
    fn test_crc32_checksum() {
        let mut checksum = Crc32::default();
        checksum.update(TEST_DATA.as_bytes());
        let checksum_result = Box::new(checksum).headers();
        let encoded_checksum = checksum_result.get(CRC_32_HEADER_NAME).unwrap();
        let decoded_checksum = base64_encoded_checksum_to_hex_string(encoded_checksum);

        let expected_checksum = "0xD308AEB2";

        assert_eq!(decoded_checksum, expected_checksum);
    }

    #[cfg(not(any(target_arch = "powerpc", target_arch = "powerpc64")))]
    #[test]
    fn test_crc32c_checksum() {
        let mut checksum = Crc32c::default();
        checksum.update(TEST_DATA.as_bytes());
        let checksum_result = Box::new(checksum).headers();
        let encoded_checksum = checksum_result.get(CRC_32_C_HEADER_NAME).unwrap();
        let decoded_checksum = base64_encoded_checksum_to_hex_string(encoded_checksum);

        let expected_checksum = "0x3379B4CA";

        assert_eq!(decoded_checksum, expected_checksum);
    }

    #[test]
    fn test_crc64nvme_checksum() {
        use crate::{Crc64Nvme, http::CRC_64_NVME_HEADER_NAME};
        let mut checksum = Crc64Nvme::default();
        checksum.update(TEST_DATA.as_bytes());
        let checksum_result = Box::new(checksum).headers();
        let encoded_checksum = checksum_result.get(CRC_64_NVME_HEADER_NAME).unwrap();
        let decoded_checksum = base64_encoded_checksum_to_hex_string(encoded_checksum);

        let expected_checksum = "0xAECAF3AF9C98A855";

        assert_eq!(decoded_checksum, expected_checksum);
    }

    #[test]
    fn test_sha1_checksum() {
        let mut checksum = Sha1::default();
        checksum.update(TEST_DATA.as_bytes());
        let checksum_result = Box::new(checksum).headers();
        let encoded_checksum = checksum_result.get(SHA_1_HEADER_NAME).unwrap();
        let decoded_checksum = base64_encoded_checksum_to_hex_string(encoded_checksum);

        let expected_checksum = "0xF48DD853820860816C75D54D0F584DC863327A7C";

        assert_eq!(decoded_checksum, expected_checksum);
    }

    #[test]
    fn test_sha256_checksum() {
        let mut checksum = Sha256::default();
        checksum.update(TEST_DATA.as_bytes());
        let checksum_result = Box::new(checksum).headers();
        let encoded_checksum = checksum_result.get(SHA_256_HEADER_NAME).unwrap();
        let decoded_checksum = base64_encoded_checksum_to_hex_string(encoded_checksum);

        let expected_checksum = "0x916F0027A575074CE72A331777C3478D6513F786A591BD892DA1A577BF2335F9";

        assert_eq!(decoded_checksum, expected_checksum);
    }

    #[test]
    fn test_md5_checksum() {
        let mut checksum = Md5::default();
        checksum.update(TEST_DATA.as_bytes());
        let checksum_result = Box::new(checksum).headers();
        let encoded_checksum = checksum_result.get(MD5_HEADER_NAME).unwrap();
        let decoded_checksum = base64_encoded_checksum_to_hex_string(encoded_checksum);

        let expected_checksum = "0xEB733A00C0C9D336E65691A37AB54293";

        assert_eq!(decoded_checksum, expected_checksum);
    }

    #[test]
    fn test_checksum_algorithm_returns_error_for_unknown() {
        let error = "some invalid checksum algorithm"
            .parse::<ChecksumAlgorithm>()
            .expect_err("it should error");
        assert_eq!("some invalid checksum algorithm", error.checksum_algorithm());
    }

    #[test]
    fn test_unknown_algorithm_error_message_lists_supported_algorithms() {
        let error = "nope".parse::<ChecksumAlgorithm>().expect_err("it should error");
        let message = error.to_string();
        assert!(message.contains("crc64nvme"), "message should advertise crc64nvme: {message}");
        assert!(!message.contains("md5"), "message should not advertise the unsupported md5: {message}");
    }

    #[test]
    fn test_md5_is_not_a_supported_checksum_algorithm() {
        // MD5 is not an accepted S3 checksum algorithm here: parsing it must fail
        // loudly rather than silently substituting a CRC32 hasher.
        let error = "md5".parse::<ChecksumAlgorithm>().expect_err("md5 should not parse");
        assert_eq!("md5", error.checksum_algorithm());

        let error = "MD5".parse::<ChecksumAlgorithm>().expect_err("md5 should not parse");
        assert_eq!("MD5", error.checksum_algorithm());
    }

    #[test]
    fn test_additional_algorithms_parse_and_round_trip() {
        // The AWS 2026-04 additional checksum algorithms must be recognised
        // (case-insensitively) and round-trip through as_str().
        for (name, expected) in [
            ("sha512", ChecksumAlgorithm::Sha512),
            ("SHA512", ChecksumAlgorithm::Sha512),
            ("xxhash3", ChecksumAlgorithm::Xxhash3),
            ("XXHASH3", ChecksumAlgorithm::Xxhash3),
            ("xxhash64", ChecksumAlgorithm::Xxhash64),
            ("xxhash128", ChecksumAlgorithm::Xxhash128),
        ] {
            let parsed = name.parse::<ChecksumAlgorithm>().expect("algorithm should parse");
            assert_eq!(parsed, expected);
            assert_eq!(expected.as_str().parse::<ChecksumAlgorithm>().unwrap(), expected);
        }
    }

    #[test]
    fn test_unknown_algorithm_never_panics_and_fails_closed() {
        // Fail-closed contract: an unknown or garbage algorithm name must return
        // an error instead of panicking or silently substituting another hasher.
        for name in ["", "xxhash", "sha3", "crc16", "not-a-real-algo", "🦀"] {
            assert!(name.parse::<ChecksumAlgorithm>().is_err(), "unknown algorithm {name:?} must fail closed");
        }
    }

    #[test]
    fn test_sha512_matches_direct_computation() {
        use crate::Sha512;
        use crate::http::SHA_512_HEADER_NAME;
        use sha2::{Digest, Sha512 as Sha512Ref};

        let mut checksum = Sha512::default();
        checksum.update(TEST_DATA.as_bytes());
        let header = Box::new(checksum).headers();
        let encoded = header.get(SHA_512_HEADER_NAME).expect("sha512 header present");
        let got = base64_encoded_checksum_to_hex_string(encoded);

        let mut reference = Sha512Ref::new();
        reference.update(TEST_DATA.as_bytes());
        let expected = reference.finalize().iter().fold(String::from("0x"), |mut acc, b| {
            write!(acc, "{b:02X?}").unwrap();
            acc
        });
        assert_eq!(got, expected);
    }

    #[test]
    fn test_xxhash3_matches_direct_computation_big_endian_seed0() {
        use crate::Xxhash3;
        use xxhash_rust::xxh3::Xxh3;

        let mut checksum = Xxhash3::default();
        checksum.update(TEST_DATA.as_bytes());
        let raw = Box::new(checksum).finalize();

        let mut reference = Xxh3::new();
        reference.update(TEST_DATA.as_bytes());
        assert_eq!(raw.len(), 8);
        assert_eq!(&raw[..], reference.digest().to_be_bytes().as_slice());
    }

    #[test]
    fn test_xxhash128_matches_direct_computation_big_endian_seed0() {
        use crate::Xxhash128;
        use xxhash_rust::xxh3::Xxh3;

        let mut checksum = Xxhash128::default();
        checksum.update(TEST_DATA.as_bytes());
        let raw = Box::new(checksum).finalize();

        let mut reference = Xxh3::new();
        reference.update(TEST_DATA.as_bytes());
        assert_eq!(raw.len(), 16);
        assert_eq!(&raw[..], reference.digest128().to_be_bytes().as_slice());
    }

    #[test]
    fn test_xxhash64_matches_direct_computation_big_endian_seed0() {
        use crate::Xxhash64;
        use xxhash_rust::xxh64::Xxh64;

        let mut checksum = Xxhash64::default();
        checksum.update(TEST_DATA.as_bytes());
        let raw = Box::new(checksum).finalize();

        let mut reference = Xxh64::new(0);
        reference.update(TEST_DATA.as_bytes());
        assert_eq!(raw.len(), 8);
        assert_eq!(&raw[..], reference.digest().to_be_bytes().as_slice());
    }
}
