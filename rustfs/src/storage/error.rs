use ecstore::{disk::error::is_err_file_not_found, error::Error, store_err::StorageError};
use s3s::{s3_error, S3Error, S3ErrorCode};

pub fn to_s3_error(err: Error) -> S3Error {
    if let Some(storage_err) = err.downcast_ref::<StorageError>() {
        return match storage_err {
            StorageError::NotImplemented => s3_error!(NotImplemented),
            StorageError::InvalidArgument(bucket, object, version_id) => {
                s3_error!(InvalidArgument, "Invalid arguments provided for {}/{}-{}", bucket, object, version_id)
            }
            StorageError::MethodNotAllowed => s3_error!(MethodNotAllowed),
            StorageError::BucketNotFound(bucket) => {
                s3_error!(NoSuchBucket, "bucket not found {}", bucket)
            }
            StorageError::BucketNotEmpty(bucket) => s3_error!(BucketNotEmpty, "bucket not empty {}", bucket),
            StorageError::BucketNameInvalid(bucket) => s3_error!(InvalidBucketName, "invalid bucket name {}", bucket),
            StorageError::ObjectNameInvalid(bucket, object) => {
                s3_error!(InvalidArgument, "invalid object name {}/{}", bucket, object)
            }
            StorageError::BucketExists(bucket) => s3_error!(BucketAlreadyExists, "{}", bucket),
            StorageError::StorageFull => s3_error!(ServiceUnavailable, "Storage reached its minimum free drive threshold."),
            StorageError::SlowDown => s3_error!(SlowDown, "Please reduce your request rate"),
            StorageError::PrefixAccessDenied(bucket, object) => {
                s3_error!(AccessDenied, "PrefixAccessDenied {}/{}", bucket, object)
            }
            StorageError::InvalidUploadIDKeyCombination(bucket, object) => {
                s3_error!(InvalidArgument, "Invalid UploadID KeyCombination:  {}/{}", bucket, object)
            }
            StorageError::MalformedUploadID(bucket) => s3_error!(InvalidArgument, "Malformed UploadID: {}", bucket),
            StorageError::ObjectNameTooLong(bucket, object) => {
                s3_error!(InvalidArgument, "Object name too long: {}/{}", bucket, object)
            }
            StorageError::ObjectNamePrefixAsSlash(bucket, object) => {
                s3_error!(InvalidArgument, "Object name contains forward slash as prefix: {}/{}", bucket, object)
            }
            StorageError::ObjectNotFound(bucket, object) => s3_error!(NoSuchKey, "{}/{}", bucket, object),
            StorageError::VersionNotFound(bucket, object, version_id) => {
                s3_error!(NoSuchVersion, "{}/{}/{}", bucket, object, version_id)
            }
            StorageError::InvalidUploadID(bucket, object, version_id) => {
                s3_error!(InvalidPart, "Invalid upload id:  {}/{}-{}", bucket, object, version_id)
            }
            StorageError::InvalidVersionID(bucket, object, version_id) => {
                s3_error!(InvalidArgument, "Invalid version id: {}/{}-{}", bucket, object, version_id)
            }
            // extended
            StorageError::DataMovementOverwriteErr(bucket, object, version_id) => s3_error!(
                InvalidArgument,
                "invalid data movement operation, source and destination pool are the same for : {}/{}-{}",
                bucket,
                object,
                version_id
            ),
            // extended
            StorageError::ObjectExistsAsDirectory(bucket, object) => {
                s3_error!(InvalidArgument, "Object exists on :{} as directory {}", bucket, object)
            }
            StorageError::InsufficientReadQuorum => {
                s3_error!(SlowDown, "Storage resources are insufficient for the read operation")
            }
            StorageError::InsufficientWriteQuorum => {
                s3_error!(SlowDown, "Storage resources are insufficient for the write operation")
            }
            StorageError::DecommissionNotStarted => s3_error!(InvalidArgument, "Decommission Not Started"),
            StorageError::VolumeNotFound(bucket) => {
                s3_error!(NoSuchBucket, "bucket not found {}", bucket)
            }
        };
    }

    if is_err_file_not_found(&err) {
        return S3Error::with_message(S3ErrorCode::NoSuchKey, format!(" ec err {}", err));
    }

    S3Error::with_message(S3ErrorCode::InternalError, format!(" ec err {}", err))
}
