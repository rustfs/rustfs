#![deny(clippy::unwrap_used)]

mod encdec;
mod error;
mod jwt;
mod metadata;
pub mod rusty_vault_client;  // 改为public
mod sse;
mod sse_c;
mod sse_s3;
pub mod sse_kms;             // 改为public
#[cfg(test)]
mod tests;

pub use encdec::decrypt::decrypt_data;
pub use encdec::encrypt::encrypt_data;
pub use error::Error;
pub use jwt::decode::decode as jwt_decode;
pub use jwt::encode::encode as jwt_encode;

// 导出SSE功能
pub use sse::{SSE, Algorithm, SSEOptions, Encryptable, get_default_kms_config, DefaultKMSConfig};
pub use sse::{init_kms, is_kms_initialized, get_kms_init_error};
#[cfg(feature = "kms")]
pub use sse::ensure_kms_client;
pub use metadata::{EncryptionInfo, extract_encryption_metadata, remove_encryption_metadata};
pub use sse_c::SSECEncryption;
pub use sse_s3::{SSES3Encryption, init_master_key};

// KMS 功能导出
#[cfg(feature = "kms")]
pub use sse_kms::{KMSClient, SSEKMSEncryption, RustyVaultClient};

/// Encryption factory: Create appropriate encryptor based on encryption type
pub struct CryptoFactory;

impl CryptoFactory {
    /// Create encryptor
    #[cfg(not(feature = "kms"))]
    pub fn create_encryptor(sse_type: Option<SSE>) -> Box<dyn Encryptable> {
        match sse_type {
            Some(SSE::SSEC) => Box::new(SSECEncryption::new()),
            Some(SSE::SSES3) => Box::new(SSES3Encryption::new()),
            Some(SSE::SSEKMS) => Box::new(SSES3Encryption::new()), // Fall back to SSE-S3 when KMS is not enabled
            None => Box::new(SSES3Encryption::new()), // Default to SSE-S3
        }
    }

    #[cfg(feature = "kms")]
    pub fn create_encryptor(sse_type: Option<SSE>) -> Result<Box<dyn Encryptable>, Error> {
        match sse_type {
            Some(SSE::SSEC) => Ok(Box::new(SSECEncryption::new())),
            Some(SSE::SSES3) => Ok(Box::new(SSES3Encryption::new())),
            Some(SSE::SSEKMS) => {
                let kms = SSEKMSEncryption::new()?;
                Ok(Box::new(kms))
            },
            None => Ok(Box::new(SSES3Encryption::new())), // Default to SSE-S3
        }
    }
}
