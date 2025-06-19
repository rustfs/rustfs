use common::error::Error;
use ecstore::error::StorageError;
use s3s::{S3Error, S3ErrorCode, s3_error};
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
            StorageError::InvalidPart(bucket, object, version_id) => {
                s3_error!(
                    InvalidPart,
                    "Specified part could not be found. PartNumber {}, Expected {}, got {}",
                    bucket,
                    object,
                    version_id
                )
            }
            StorageError::DoneForNow => s3_error!(InternalError, "DoneForNow"),
        };
    }

    if is_err_file_not_found(&err) {
        return S3Error::with_message(S3ErrorCode::NoSuchKey, format!(" ec err {}", err));
    }

    S3Error::with_message(S3ErrorCode::InternalError, format!(" ec err {}", err))
}

#[cfg(test)]
mod tests {
    use super::*;
    use s3s::S3ErrorCode;

    #[test]
    fn test_to_s3_error_not_implemented() {
        let storage_err = StorageError::NotImplemented;
        let err = Error::new(storage_err);
        let s3_err = to_s3_error(err);

        assert_eq!(*s3_err.code(), S3ErrorCode::NotImplemented);
    }

    #[test]
    fn test_to_s3_error_invalid_argument() {
        let storage_err =
            StorageError::InvalidArgument("test-bucket".to_string(), "test-object".to_string(), "test-version".to_string());
        let err = Error::new(storage_err);
        let s3_err = to_s3_error(err);

        assert_eq!(*s3_err.code(), S3ErrorCode::InvalidArgument);
        assert!(s3_err.message().unwrap().contains("Invalid arguments provided"));
        assert!(s3_err.message().unwrap().contains("test-bucket"));
        assert!(s3_err.message().unwrap().contains("test-object"));
        assert!(s3_err.message().unwrap().contains("test-version"));
    }

    #[test]
    fn test_to_s3_error_method_not_allowed() {
        let storage_err = StorageError::MethodNotAllowed;
        let err = Error::new(storage_err);
        let s3_err = to_s3_error(err);

        assert_eq!(*s3_err.code(), S3ErrorCode::MethodNotAllowed);
    }

    #[test]
    fn test_to_s3_error_bucket_not_found() {
        let storage_err = StorageError::BucketNotFound("test-bucket".to_string());
        let err = Error::new(storage_err);
        let s3_err = to_s3_error(err);

        assert_eq!(*s3_err.code(), S3ErrorCode::NoSuchBucket);
        assert!(s3_err.message().unwrap().contains("bucket not found"));
        assert!(s3_err.message().unwrap().contains("test-bucket"));
    }

    #[test]
    fn test_to_s3_error_bucket_not_empty() {
        let storage_err = StorageError::BucketNotEmpty("test-bucket".to_string());
        let err = Error::new(storage_err);
        let s3_err = to_s3_error(err);

        assert_eq!(*s3_err.code(), S3ErrorCode::BucketNotEmpty);
        assert!(s3_err.message().unwrap().contains("bucket not empty"));
        assert!(s3_err.message().unwrap().contains("test-bucket"));
    }

    #[test]
    fn test_to_s3_error_bucket_name_invalid() {
        let storage_err = StorageError::BucketNameInvalid("invalid-bucket-name".to_string());
        let err = Error::new(storage_err);
        let s3_err = to_s3_error(err);

        assert_eq!(*s3_err.code(), S3ErrorCode::InvalidBucketName);
        assert!(s3_err.message().unwrap().contains("invalid bucket name"));
        assert!(s3_err.message().unwrap().contains("invalid-bucket-name"));
    }

    #[test]
    fn test_to_s3_error_object_name_invalid() {
        let storage_err = StorageError::ObjectNameInvalid("test-bucket".to_string(), "invalid-object".to_string());
        let err = Error::new(storage_err);
        let s3_err = to_s3_error(err);

        assert_eq!(*s3_err.code(), S3ErrorCode::InvalidArgument);
        assert!(s3_err.message().unwrap().contains("invalid object name"));
        assert!(s3_err.message().unwrap().contains("test-bucket"));
        assert!(s3_err.message().unwrap().contains("invalid-object"));
    }

    #[test]
    fn test_to_s3_error_bucket_exists() {
        let storage_err = StorageError::BucketExists("existing-bucket".to_string());
        let err = Error::new(storage_err);
        let s3_err = to_s3_error(err);

        assert_eq!(*s3_err.code(), S3ErrorCode::BucketAlreadyExists);
        assert!(s3_err.message().unwrap().contains("existing-bucket"));
    }

    #[test]
    fn test_to_s3_error_storage_full() {
        let storage_err = StorageError::StorageFull;
        let err = Error::new(storage_err);
        let s3_err = to_s3_error(err);

        assert_eq!(*s3_err.code(), S3ErrorCode::ServiceUnavailable);
        assert!(
            s3_err
                .message()
                .unwrap()
                .contains("Storage reached its minimum free drive threshold")
        );
    }

    #[test]
    fn test_to_s3_error_slow_down() {
        let storage_err = StorageError::SlowDown;
        let err = Error::new(storage_err);
        let s3_err = to_s3_error(err);

        assert_eq!(*s3_err.code(), S3ErrorCode::SlowDown);
        assert!(s3_err.message().unwrap().contains("Please reduce your request rate"));
    }

    #[test]
    fn test_to_s3_error_prefix_access_denied() {
        let storage_err = StorageError::PrefixAccessDenied("test-bucket".to_string(), "test-prefix".to_string());
        let err = Error::new(storage_err);
        let s3_err = to_s3_error(err);

        assert_eq!(*s3_err.code(), S3ErrorCode::AccessDenied);
        assert!(s3_err.message().unwrap().contains("PrefixAccessDenied"));
        assert!(s3_err.message().unwrap().contains("test-bucket"));
        assert!(s3_err.message().unwrap().contains("test-prefix"));
    }

    #[test]
    fn test_to_s3_error_invalid_upload_id_key_combination() {
        let storage_err = StorageError::InvalidUploadIDKeyCombination("test-bucket".to_string(), "test-object".to_string());
        let err = Error::new(storage_err);
        let s3_err = to_s3_error(err);

        assert_eq!(*s3_err.code(), S3ErrorCode::InvalidArgument);
        assert!(s3_err.message().unwrap().contains("Invalid UploadID KeyCombination"));
        assert!(s3_err.message().unwrap().contains("test-bucket"));
        assert!(s3_err.message().unwrap().contains("test-object"));
    }

    #[test]
    fn test_to_s3_error_malformed_upload_id() {
        let storage_err = StorageError::MalformedUploadID("malformed-id".to_string());
        let err = Error::new(storage_err);
        let s3_err = to_s3_error(err);

        assert_eq!(*s3_err.code(), S3ErrorCode::InvalidArgument);
        assert!(s3_err.message().unwrap().contains("Malformed UploadID"));
        assert!(s3_err.message().unwrap().contains("malformed-id"));
    }

    #[test]
    fn test_to_s3_error_object_name_too_long() {
        let storage_err = StorageError::ObjectNameTooLong("test-bucket".to_string(), "very-long-object-name".to_string());
        let err = Error::new(storage_err);
        let s3_err = to_s3_error(err);

        assert_eq!(*s3_err.code(), S3ErrorCode::InvalidArgument);
        assert!(s3_err.message().unwrap().contains("Object name too long"));
        assert!(s3_err.message().unwrap().contains("test-bucket"));
        assert!(s3_err.message().unwrap().contains("very-long-object-name"));
    }

    #[test]
    fn test_to_s3_error_object_name_prefix_as_slash() {
        let storage_err = StorageError::ObjectNamePrefixAsSlash("test-bucket".to_string(), "/invalid-object".to_string());
        let err = Error::new(storage_err);
        let s3_err = to_s3_error(err);

        assert_eq!(*s3_err.code(), S3ErrorCode::InvalidArgument);
        assert!(
            s3_err
                .message()
                .unwrap()
                .contains("Object name contains forward slash as prefix")
        );
        assert!(s3_err.message().unwrap().contains("test-bucket"));
        assert!(s3_err.message().unwrap().contains("/invalid-object"));
    }

    #[test]
    fn test_to_s3_error_object_not_found() {
        let storage_err = StorageError::ObjectNotFound("test-bucket".to_string(), "missing-object".to_string());
        let err = Error::new(storage_err);
        let s3_err = to_s3_error(err);

        assert_eq!(*s3_err.code(), S3ErrorCode::NoSuchKey);
        assert!(s3_err.message().unwrap().contains("test-bucket"));
        assert!(s3_err.message().unwrap().contains("missing-object"));
    }

    #[test]
    fn test_to_s3_error_version_not_found() {
        let storage_err =
            StorageError::VersionNotFound("test-bucket".to_string(), "test-object".to_string(), "missing-version".to_string());
        let err = Error::new(storage_err);
        let s3_err = to_s3_error(err);

        assert_eq!(*s3_err.code(), S3ErrorCode::NoSuchVersion);
        assert!(s3_err.message().unwrap().contains("test-bucket"));
        assert!(s3_err.message().unwrap().contains("test-object"));
        assert!(s3_err.message().unwrap().contains("missing-version"));
    }

    #[test]
    fn test_to_s3_error_invalid_upload_id() {
        let storage_err =
            StorageError::InvalidUploadID("test-bucket".to_string(), "test-object".to_string(), "invalid-upload-id".to_string());
        let err = Error::new(storage_err);
        let s3_err = to_s3_error(err);

        assert_eq!(*s3_err.code(), S3ErrorCode::InvalidPart);
        assert!(s3_err.message().unwrap().contains("Invalid upload id"));
        assert!(s3_err.message().unwrap().contains("test-bucket"));
        assert!(s3_err.message().unwrap().contains("test-object"));
        assert!(s3_err.message().unwrap().contains("invalid-upload-id"));
    }

    #[test]
    fn test_to_s3_error_invalid_version_id() {
        let storage_err = StorageError::InvalidVersionID(
            "test-bucket".to_string(),
            "test-object".to_string(),
            "invalid-version-id".to_string(),
        );
        let err = Error::new(storage_err);
        let s3_err = to_s3_error(err);

        assert_eq!(*s3_err.code(), S3ErrorCode::InvalidArgument);
        assert!(s3_err.message().unwrap().contains("Invalid version id"));
        assert!(s3_err.message().unwrap().contains("test-bucket"));
        assert!(s3_err.message().unwrap().contains("test-object"));
        assert!(s3_err.message().unwrap().contains("invalid-version-id"));
    }

    #[test]
    fn test_to_s3_error_data_movement_overwrite_err() {
        let storage_err = StorageError::DataMovementOverwriteErr(
            "test-bucket".to_string(),
            "test-object".to_string(),
            "test-version".to_string(),
        );
        let err = Error::new(storage_err);
        let s3_err = to_s3_error(err);

        assert_eq!(*s3_err.code(), S3ErrorCode::InvalidArgument);
        assert!(s3_err.message().unwrap().contains("invalid data movement operation"));
        assert!(s3_err.message().unwrap().contains("source and destination pool are the same"));
        assert!(s3_err.message().unwrap().contains("test-bucket"));
        assert!(s3_err.message().unwrap().contains("test-object"));
        assert!(s3_err.message().unwrap().contains("test-version"));
    }

    #[test]
    fn test_to_s3_error_object_exists_as_directory() {
        let storage_err = StorageError::ObjectExistsAsDirectory("test-bucket".to_string(), "directory-object".to_string());
        let err = Error::new(storage_err);
        let s3_err = to_s3_error(err);

        assert_eq!(*s3_err.code(), S3ErrorCode::InvalidArgument);
        assert!(s3_err.message().unwrap().contains("Object exists on"));
        assert!(s3_err.message().unwrap().contains("as directory"));
        assert!(s3_err.message().unwrap().contains("test-bucket"));
        assert!(s3_err.message().unwrap().contains("directory-object"));
    }

    #[test]
    fn test_to_s3_error_insufficient_read_quorum() {
        let storage_err = StorageError::InsufficientReadQuorum;
        let err = Error::new(storage_err);
        let s3_err = to_s3_error(err);

        assert_eq!(*s3_err.code(), S3ErrorCode::SlowDown);
        assert!(
            s3_err
                .message()
                .unwrap()
                .contains("Storage resources are insufficient for the read operation")
        );
    }

    #[test]
    fn test_to_s3_error_insufficient_write_quorum() {
        let storage_err = StorageError::InsufficientWriteQuorum;
        let err = Error::new(storage_err);
        let s3_err = to_s3_error(err);

        assert_eq!(*s3_err.code(), S3ErrorCode::SlowDown);
        assert!(
            s3_err
                .message()
                .unwrap()
                .contains("Storage resources are insufficient for the write operation")
        );
    }

    #[test]
    fn test_to_s3_error_decommission_not_started() {
        let storage_err = StorageError::DecommissionNotStarted;
        let err = Error::new(storage_err);
        let s3_err = to_s3_error(err);

        assert_eq!(*s3_err.code(), S3ErrorCode::InvalidArgument);
        assert!(s3_err.message().unwrap().contains("Decommission Not Started"));
    }

    #[test]
    fn test_to_s3_error_decommission_already_running() {
        let storage_err = StorageError::DecommissionAlreadyRunning;
        let err = Error::new(storage_err);
        let s3_err = to_s3_error(err);

        assert_eq!(*s3_err.code(), S3ErrorCode::InternalError);
        assert!(s3_err.message().unwrap().contains("Decommission already running"));
    }

    #[test]
    fn test_to_s3_error_volume_not_found() {
        let storage_err = StorageError::VolumeNotFound("test-volume".to_string());
        let err = Error::new(storage_err);
        let s3_err = to_s3_error(err);

        assert_eq!(*s3_err.code(), S3ErrorCode::NoSuchBucket);
        assert!(s3_err.message().unwrap().contains("bucket not found"));
        assert!(s3_err.message().unwrap().contains("test-volume"));
    }

    #[test]
    fn test_to_s3_error_invalid_part() {
        let storage_err = StorageError::InvalidPart(1, "expected-part".to_string(), "got-part".to_string());
        let err = Error::new(storage_err);
        let s3_err = to_s3_error(err);

        assert_eq!(*s3_err.code(), S3ErrorCode::InvalidPart);
        assert!(s3_err.message().unwrap().contains("Specified part could not be found"));
        assert!(s3_err.message().unwrap().contains("PartNumber"));
        assert!(s3_err.message().unwrap().contains("expected-part"));
        assert!(s3_err.message().unwrap().contains("got-part"));
    }

    #[test]
    fn test_to_s3_error_done_for_now() {
        let storage_err = StorageError::DoneForNow;
        let err = Error::new(storage_err);
        let s3_err = to_s3_error(err);

        assert_eq!(*s3_err.code(), S3ErrorCode::InternalError);
        assert!(s3_err.message().unwrap().contains("DoneForNow"));
    }

    #[test]
    fn test_to_s3_error_non_storage_error() {
        // Test with a non-StorageError
        let err = Error::from_string("Generic error message".to_string());
        let s3_err = to_s3_error(err);

        assert_eq!(*s3_err.code(), S3ErrorCode::InternalError);
        assert!(s3_err.message().unwrap().contains("ec err"));
        assert!(s3_err.message().unwrap().contains("Generic error message"));
    }

    #[test]
    fn test_to_s3_error_with_unicode_strings() {
        let storage_err = StorageError::BucketNotFound("测试桶".to_string());
        let err = Error::new(storage_err);
        let s3_err = to_s3_error(err);

        assert_eq!(*s3_err.code(), S3ErrorCode::NoSuchBucket);
        assert!(s3_err.message().unwrap().contains("bucket not found"));
        assert!(s3_err.message().unwrap().contains("测试桶"));
    }

    #[test]
    fn test_to_s3_error_with_special_characters() {
        let storage_err = StorageError::ObjectNameInvalid("bucket-with-@#$%".to_string(), "object-with-!@#$%^&*()".to_string());
        let err = Error::new(storage_err);
        let s3_err = to_s3_error(err);

        assert_eq!(*s3_err.code(), S3ErrorCode::InvalidArgument);
        assert!(s3_err.message().unwrap().contains("invalid object name"));
        assert!(s3_err.message().unwrap().contains("bucket-with-@#$%"));
        assert!(s3_err.message().unwrap().contains("object-with-!@#$%^&*()"));
    }

    #[test]
    fn test_to_s3_error_with_empty_strings() {
        let storage_err = StorageError::BucketNotFound("".to_string());
        let err = Error::new(storage_err);
        let s3_err = to_s3_error(err);

        assert_eq!(*s3_err.code(), S3ErrorCode::NoSuchBucket);
        assert!(s3_err.message().unwrap().contains("bucket not found"));
    }

    #[test]
    fn test_to_s3_error_with_very_long_strings() {
        let long_bucket_name = "a".repeat(1000);
        let storage_err = StorageError::BucketNotFound(long_bucket_name.clone());
        let err = Error::new(storage_err);
        let s3_err = to_s3_error(err);

        assert_eq!(*s3_err.code(), S3ErrorCode::NoSuchBucket);
        assert!(s3_err.message().unwrap().contains("bucket not found"));
        assert!(s3_err.message().unwrap().contains(&long_bucket_name));
    }
}
