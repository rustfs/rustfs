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
}
