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
pub const MD5_NAME: &str = "md5";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum ChecksumAlgorithm {
    #[default]
    Crc32,
    Crc32c,
    #[deprecated]
    Md5,
    Sha1,
    Sha256,
    Crc64Nvme,
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
        } else if checksum_algorithm.eq_ignore_ascii_case(MD5_NAME) {
            // MD5 is now an alias for the default Crc32 since it is deprecated
            Ok(Self::Crc32)
        } else if checksum_algorithm.eq_ignore_ascii_case(CRC_64_NVME_NAME) {
            Ok(Self::Crc64Nvme)
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
            #[allow(deprecated)]
            Self::Md5 => Box::<Crc32>::default(),
            Self::Sha1 => Box::<Sha1>::default(),
            Self::Sha256 => Box::<Sha256>::default(),
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Crc32 => CRC_32_NAME,
            Self::Crc32c => CRC_32_C_NAME,
            Self::Crc64Nvme => CRC_64_NVME_NAME,
            #[allow(deprecated)]
            Self::Md5 => MD5_NAME,
            Self::Sha1 => SHA_1_NAME,
            Self::Sha256 => SHA_256_NAME,
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
struct Md5 {
    hasher: md5::Md5,
}

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

    use crate::base64;
    use http::HeaderValue;
    use pretty_assertions::assert_eq;
    use std::fmt::Write;

    const TEST_DATA: &str = r#"test data"#;

    fn base64_encoded_checksum_to_hex_string(header_value: &HeaderValue) -> String {
        let decoded_checksum = base64::decode(header_value.to_str().unwrap()).unwrap();
        let decoded_checksum = decoded_checksum.into_iter().fold(String::new(), |mut acc, byte| {
            write!(acc, "{byte:02X?}").expect("string will always be writeable");
            acc
        });

        format!("0x{}", decoded_checksum)
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
}
