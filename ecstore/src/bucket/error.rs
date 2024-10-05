#[derive(Debug, thiserror::Error)]
pub enum BucketMetadataError {
    #[error("tagging not found")]
    TaggingNotFound,
}
