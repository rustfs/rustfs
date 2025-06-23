#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ReaderError {
    #[error("stream input error {0}")]
    StreamInput(String),
    //
    #[error("etag: expected ETag {0} does not match computed ETag {1}")]
    VerifyError(String, String),
    #[error("Bad checksum: Want {0} does not match calculated {1}")]
    ChecksumMismatch(String, String),
    #[error("Bad sha256: Expected {0} does not match calculated {1}")]
    SHA256Mismatch(String, String),
}
