//! Checksum and hash primitives used by S3 signature verification and checksum computation.
//!
//! This module defines the [`Checksum`] trait and provides concrete implementations
//! for every checksum and hash algorithm supported by Amazon S3:
//! non-cryptographic checksums: [`Crc32`], [`Crc32c`], [`Crc64Nvme`];
//! cryptographic hash functions: [`Sha1`], [`Sha256`], and [`Md5`].

use numeric_cast::TruncatingCast;

pub trait Checksum {
    type Output: AsRef<[u8]>;

    #[must_use]
    fn new() -> Self;

    fn update(&mut self, data: &[u8]);

    #[must_use]
    fn finalize(self) -> Self::Output;

    #[must_use]
    fn checksum(data: &[u8]) -> Self::Output
    where
        Self: Sized,
    {
        let mut hasher = Self::new();
        hasher.update(data);
        hasher.finalize()
    }
}

pub struct Crc32(crc_fast::Digest);

impl Default for Crc32 {
    fn default() -> Self {
        Self(crc_fast::Digest::new(crc_fast::CrcAlgorithm::Crc32IsoHdlc))
    }
}

impl Crc32 {
    #[must_use]
    pub fn checksum_u32(data: &[u8]) -> u32 {
        let mut hasher = Self::new();
        hasher.update(data);
        hasher.0.finalize().truncating_cast::<u32>()
    }
}

impl Checksum for Crc32 {
    type Output = [u8; 4];

    fn new() -> Self {
        Self::default()
    }

    fn update(&mut self, data: &[u8]) {
        self.0.update(data);
    }

    fn finalize(self) -> Self::Output {
        self.0.finalize().truncating_cast::<u32>().to_be_bytes()
    }
}

pub struct Crc32c(crc_fast::Digest);

impl Default for Crc32c {
    fn default() -> Self {
        Self(crc_fast::Digest::new(crc_fast::CrcAlgorithm::Crc32Iscsi))
    }
}

impl Checksum for Crc32c {
    type Output = [u8; 4];

    fn new() -> Self {
        Self::default()
    }

    fn update(&mut self, data: &[u8]) {
        self.0.update(data);
    }

    fn finalize(self) -> Self::Output {
        self.0.finalize().truncating_cast::<u32>().to_be_bytes()
    }
}

pub struct Crc64Nvme(crc_fast::Digest);

impl Default for Crc64Nvme {
    fn default() -> Self {
        Self(crc_fast::Digest::new(crc_fast::CrcAlgorithm::Crc64Nvme))
    }
}

impl Checksum for Crc64Nvme {
    type Output = [u8; 8];

    fn new() -> Self {
        Self::default()
    }

    fn update(&mut self, data: &[u8]) {
        self.0.update(data);
    }

    fn finalize(self) -> Self::Output {
        self.0.finalize().to_be_bytes()
    }
}

#[derive(Default)]
pub struct Sha1(sha1::Sha1);

impl Checksum for Sha1 {
    type Output = [u8; 20];

    fn new() -> Self {
        Self::default()
    }

    fn update(&mut self, data: &[u8]) {
        use sha1::Digest as _;
        self.0.update(data);
    }

    fn finalize(self) -> Self::Output {
        use sha1::Digest as _;
        self.0.finalize().into()
    }
}

#[derive(Default)]
pub struct Sha256(sha2::Sha256);

impl Checksum for Sha256 {
    type Output = [u8; 32];

    fn new() -> Self {
        Self::default()
    }

    fn update(&mut self, data: &[u8]) {
        use sha2::Digest as _;
        self.0.update(data);
    }

    fn finalize(self) -> Self::Output {
        use sha2::Digest as _;
        self.0.finalize().into()
    }
}

#[derive(Default)]
pub struct Md5(md5::Md5);

impl Checksum for Md5 {
    type Output = [u8; 16];

    fn new() -> Self {
        Self::default()
    }

    fn update(&mut self, data: &[u8]) {
        use md5::Digest as _;
        self.0.update(data);
    }

    fn finalize(self) -> Self::Output {
        use md5::Digest as _;
        self.0.finalize().into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crc32_checksum() {
        let output = Crc32::checksum(b"hello");
        assert_eq!(output.len(), 4);
        assert_eq!(output, Crc32::checksum(b"hello"));
    }

    #[test]
    fn crc32_incremental() {
        let mut h = Crc32::new();
        h.update(b"hel");
        h.update(b"lo");
        let incremental = h.finalize();
        assert_eq!(incremental, Crc32::checksum(b"hello"));
    }

    #[test]
    fn crc32_checksum_u32() {
        let val = Crc32::checksum_u32(b"hello");
        let bytes = val.to_be_bytes();
        assert_eq!(bytes, Crc32::checksum(b"hello"));
    }

    #[test]
    fn crc32_empty() {
        let output = Crc32::checksum(b"");
        assert_eq!(output.len(), 4);
    }

    #[test]
    fn crc32c_checksum() {
        let output = Crc32c::checksum(b"hello");
        assert_eq!(output.len(), 4);
    }

    #[test]
    fn crc32c_incremental() {
        let mut h = Crc32c::new();
        h.update(b"hel");
        h.update(b"lo");
        assert_eq!(h.finalize(), Crc32c::checksum(b"hello"));
    }

    #[test]
    fn crc64nvme_checksum() {
        let output = Crc64Nvme::checksum(b"hello");
        assert_eq!(output.len(), 8);
    }

    #[test]
    fn crc64nvme_incremental() {
        let mut h = Crc64Nvme::new();
        h.update(b"hel");
        h.update(b"lo");
        assert_eq!(h.finalize(), Crc64Nvme::checksum(b"hello"));
    }

    #[test]
    fn sha1_checksum() {
        let output = Sha1::checksum(b"hello");
        assert_eq!(output.len(), 20);
    }

    #[test]
    fn sha1_incremental() {
        let mut h = Sha1::new();
        h.update(b"hel");
        h.update(b"lo");
        assert_eq!(h.finalize(), Sha1::checksum(b"hello"));
    }

    #[test]
    fn sha256_checksum() {
        let output = Sha256::checksum(b"hello");
        assert_eq!(output.len(), 32);
    }

    #[test]
    fn sha256_incremental() {
        let mut h = Sha256::new();
        h.update(b"hel");
        h.update(b"lo");
        assert_eq!(h.finalize(), Sha256::checksum(b"hello"));
    }

    #[test]
    fn md5_checksum() {
        let output = Md5::checksum(b"hello");
        assert_eq!(output.len(), 16);
    }

    #[test]
    fn md5_incremental() {
        let mut h = Md5::new();
        h.update(b"hel");
        h.update(b"lo");
        assert_eq!(h.finalize(), Md5::checksum(b"hello"));
    }

    #[test]
    fn sha256_known_value() {
        // SHA-256 of empty string is well-known
        let mut expected = [0u8; 32];
        hex_simd::decode(
            b"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            hex_simd::Out::from_slice(&mut expected),
        )
        .unwrap();
        let output = Sha256::checksum(b"");
        assert_eq!(output, expected);
    }
}
