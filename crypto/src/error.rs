#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("unexpected header")]
    ErrUnexpectedHeader,

    #[error("invalid encryption algorithm ID: {0}")]
    ErrInvalidAlgID(u8),

    #[cfg(any(test, feature = "crypto"))]
    #[error("{0}")]
    ErrInvalidLength(#[from] sha2::digest::InvalidLength),

    #[cfg(any(test, feature = "crypto"))]
    #[error("encrypt failed")]
    ErrEncryptFailed(aes_gcm::aead::Error),

    #[cfg(any(test, feature = "crypto"))]
    #[error("decrypt failed")]
    ErrDecryptFailed(aes_gcm::aead::Error),

    #[cfg(any(test, feature = "crypto"))]
    #[error("argon2 err: {0}")]
    ErrArgon2(#[from] argon2::Error),

    #[error("jwt err: {0}")]
    ErrJwt(#[from] jsonwebtoken::errors::Error),
    
    // SSE related errors
    #[error("invalid SSE algorithm")]
    ErrInvalidSSEAlgorithm,

    #[error("missing SSE encryption key")]
    ErrMissingSSEKey,

    #[error("invalid SSE customer key")]
    ErrInvalidSSECustomerKey,

    #[error("SSE key MD5 mismatch")]
    ErrSSEKeyMD5Mismatch,

    #[error("invalid SSE encryption metadata")]
    ErrInvalidEncryptionMetadata,

    #[error("missing KMS configuration")]
    ErrMissingKMSConfig,

    #[error("KMS key ID configuration error: {0}")]
    ErrKMSKeyConfiguration(String),

    #[error("KMS error: {0}")]
    ErrKMS(String),

    #[error("invalid encrypted data format")]
    ErrInvalidEncryptedDataFormat,

    #[error("encrypted object key missing")]
    ErrEncryptedObjectKeyMissing,
    
    // Base64 decoding error
    #[error("base64 decode error: {0}")]
    ErrBase64DecodeError(#[from] base64::DecodeError),
    
    // Feature not supported error
    #[error("feature not supported: {0}")]
    ErrNotSupported(String),

    #[error("Missing encrypted key")]
    ErrMissingEncryptedKey,
    
    #[error("Invalid data format: {0}")]
    ErrInvalidDataFormat(String),
}
