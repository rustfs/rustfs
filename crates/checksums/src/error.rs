use std::error::Error;
use std::fmt;

#[derive(Debug)]
pub struct UnknownChecksumAlgorithmError {
    checksum_algorithm: String,
}

impl UnknownChecksumAlgorithmError {
    pub(crate) fn new(checksum_algorithm: impl Into<String>) -> Self {
        Self {
            checksum_algorithm: checksum_algorithm.into(),
        }
    }

    pub fn checksum_algorithm(&self) -> &str {
        &self.checksum_algorithm
    }
}

impl fmt::Display for UnknownChecksumAlgorithmError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            r#"unknown checksum algorithm "{}", please pass a known algorithm name ("crc32", "crc32c", "sha1", "sha256", "md5")"#,
            self.checksum_algorithm
        )
    }
}

impl Error for UnknownChecksumAlgorithmError {}
