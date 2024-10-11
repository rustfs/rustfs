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
