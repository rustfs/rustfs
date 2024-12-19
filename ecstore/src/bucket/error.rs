use crate::error::Error;

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum BucketMetadataError {
    #[error("tagging not found")]
    TaggingNotFound,
    #[error("bucket policy not found")]
    BucketPolicyNotFound,
    #[error("bucket object lock not found")]
    BucketObjectLockConfigNotFound,
    #[error("bucket lifecycle not found")]
    BucketLifecycleNotFound,
    #[error("bucket SSE config not found")]
    BucketSSEConfigNotFound,
    #[error("bucket quota config not found")]
    BucketQuotaConfigNotFound,
    #[error("bucket replication config not found")]
    BucketReplicationConfigNotFound,
    #[error("bucket remote target not found")]
    BucketRemoteTargetNotFound,
}

impl BucketMetadataError {
    pub fn is(&self, err: &Error) -> bool {
        if let Some(e) = err.downcast_ref::<BucketMetadataError>() {
            e == self
        } else {
            false
        }
    }
}

impl BucketMetadataError {
    pub fn to_u32(&self) -> u32 {
        match self {
            BucketMetadataError::TaggingNotFound => 0x01,
            BucketMetadataError::BucketPolicyNotFound => 0x02,
            BucketMetadataError::BucketObjectLockConfigNotFound => 0x03,
            BucketMetadataError::BucketLifecycleNotFound => 0x04,
            BucketMetadataError::BucketSSEConfigNotFound => 0x05,
            BucketMetadataError::BucketQuotaConfigNotFound => 0x06,
            BucketMetadataError::BucketReplicationConfigNotFound => 0x07,
            BucketMetadataError::BucketRemoteTargetNotFound => 0x08,
        }
    }

    pub fn from_u32(error: u32) -> Option<Self> {
        match error {
            0x01 => Some(BucketMetadataError::TaggingNotFound),
            0x02 => Some(BucketMetadataError::BucketPolicyNotFound),
            0x03 => Some(BucketMetadataError::BucketObjectLockConfigNotFound),
            0x04 => Some(BucketMetadataError::BucketLifecycleNotFound),
            0x05 => Some(BucketMetadataError::BucketSSEConfigNotFound),
            0x06 => Some(BucketMetadataError::BucketQuotaConfigNotFound),
            0x07 => Some(BucketMetadataError::BucketReplicationConfigNotFound),
            0x08 => Some(BucketMetadataError::BucketRemoteTargetNotFound),
            _ => None,
        }
    }
}
