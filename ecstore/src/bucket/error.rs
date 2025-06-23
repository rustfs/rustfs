use crate::error::Error;

#[derive(Debug, thiserror::Error)]
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

    #[error("Io error: {0}")]
    Io(std::io::Error),
}

impl BucketMetadataError {
    pub fn other<E>(error: E) -> Self
    where
        E: Into<Box<dyn std::error::Error + Send + Sync>>,
    {
        BucketMetadataError::Io(std::io::Error::other(error))
    }
}

impl From<Error> for BucketMetadataError {
    fn from(e: Error) -> Self {
        match e {
            Error::Io(e) => e.into(),
            _ => BucketMetadataError::other(e),
        }
    }
}

impl From<std::io::Error> for BucketMetadataError {
    fn from(e: std::io::Error) -> Self {
        e.downcast::<BucketMetadataError>().unwrap_or_else(BucketMetadataError::other)
    }
}

impl PartialEq for BucketMetadataError {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (BucketMetadataError::Io(e1), BucketMetadataError::Io(e2)) => {
                e1.kind() == e2.kind() && e1.to_string() == e2.to_string()
            }
            (e1, e2) => e1.to_u32() == e2.to_u32(),
        }
    }
}

impl Eq for BucketMetadataError {}

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
            BucketMetadataError::Io(_) => 0x09,
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
            0x09 => Some(BucketMetadataError::Io(std::io::Error::other("Io error"))),
            _ => None,
        }
    }
}
