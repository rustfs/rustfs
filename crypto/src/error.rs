use sha2::digest::InvalidLength;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("unexpected header")]
    ErrUnexpectedHeader,

    #[error("invalid encryption algorithm ID: {0}")]
    ErrInvalidAlgID(u8),

    #[error("{0}")]
    ErrInvalidLength(#[from] InvalidLength),

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
}
