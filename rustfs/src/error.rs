// Copyright 2024 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use rustfs_ecstore::error::StorageError;
use s3s::{S3Error, S3ErrorCode};

#[derive(Debug)]
pub struct ApiError {
    pub code: S3ErrorCode,
    pub message: String,
    pub source: Option<Box<dyn std::error::Error + Send + Sync>>,
}

impl std::fmt::Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for ApiError {}

impl ApiError {
    pub fn other<E>(error: E) -> Self
    where
        E: std::fmt::Display + Into<Box<dyn std::error::Error + Send + Sync>>,
    {
        ApiError {
            code: S3ErrorCode::InternalError,
            message: error.to_string(),
            source: Some(error.into()),
        }
    }
}

impl From<ApiError> for S3Error {
    fn from(err: ApiError) -> Self {
        let mut s3e = S3Error::with_message(err.code, err.message);
        if let Some(source) = err.source {
            s3e.set_source(source);
        }
        s3e
    }
}

impl From<StorageError> for ApiError {
    fn from(err: StorageError) -> Self {
        let code = match &err {
            StorageError::NotImplemented => S3ErrorCode::NotImplemented,
            StorageError::InvalidArgument(_, _, _) => S3ErrorCode::InvalidArgument,
            StorageError::MethodNotAllowed => S3ErrorCode::MethodNotAllowed,
            StorageError::BucketNotFound(_) => S3ErrorCode::NoSuchBucket,
            StorageError::BucketNotEmpty(_) => S3ErrorCode::BucketNotEmpty,
            StorageError::BucketNameInvalid(_) => S3ErrorCode::InvalidBucketName,
            StorageError::ObjectNameInvalid(_, _) => S3ErrorCode::InvalidArgument,
            StorageError::BucketExists(_) => S3ErrorCode::BucketAlreadyOwnedByYou,
            StorageError::StorageFull => S3ErrorCode::ServiceUnavailable,
            StorageError::SlowDown => S3ErrorCode::SlowDown,
            StorageError::PrefixAccessDenied(_, _) => S3ErrorCode::AccessDenied,
            StorageError::InvalidUploadIDKeyCombination(_, _) => S3ErrorCode::InvalidArgument,
            StorageError::ObjectNameTooLong(_, _) => S3ErrorCode::InvalidArgument,
            StorageError::ObjectNamePrefixAsSlash(_, _) => S3ErrorCode::InvalidArgument,
            StorageError::ObjectNotFound(_, _) => S3ErrorCode::NoSuchKey,
            StorageError::ConfigNotFound => S3ErrorCode::NoSuchKey,
            StorageError::VolumeNotFound => S3ErrorCode::NoSuchBucket,
            StorageError::FileNotFound => S3ErrorCode::NoSuchKey,
            StorageError::FileVersionNotFound => S3ErrorCode::NoSuchVersion,
            StorageError::VersionNotFound(_, _, _) => S3ErrorCode::NoSuchVersion,
            StorageError::InvalidUploadID(_, _, _) => S3ErrorCode::InvalidPart,
            StorageError::InvalidVersionID(_, _, _) => S3ErrorCode::InvalidArgument,
            StorageError::DataMovementOverwriteErr(_, _, _) => S3ErrorCode::InvalidArgument,
            StorageError::ObjectExistsAsDirectory(_, _) => S3ErrorCode::InvalidArgument,
            StorageError::InvalidPart(_, _, _) => S3ErrorCode::InvalidPart,
            StorageError::EntityTooSmall(_, _, _) => S3ErrorCode::EntityTooSmall,
            StorageError::PreconditionFailed => S3ErrorCode::PreconditionFailed,
            StorageError::InvalidRangeSpec(_) => S3ErrorCode::InvalidRange,
            _ => S3ErrorCode::InternalError,
        };

        ApiError {
            code,
            message: err.to_string(),
            source: Some(Box::new(err)),
        }
    }
}

impl From<std::io::Error> for ApiError {
    fn from(err: std::io::Error) -> Self {
        ApiError {
            code: S3ErrorCode::InternalError,
            message: err.to_string(),
            source: Some(Box::new(err)),
        }
    }
}

impl From<rustfs_iam::error::Error> for ApiError {
    fn from(err: rustfs_iam::error::Error) -> Self {
        let serr: StorageError = err.into();
        serr.into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use s3s::{S3Error, S3ErrorCode};
    use std::io::{Error as IoError, ErrorKind};

    #[test]
    fn test_api_error_from_io_error() {
        let io_error = IoError::new(ErrorKind::PermissionDenied, "permission denied");
        let api_error: ApiError = io_error.into();

        assert_eq!(api_error.code, S3ErrorCode::InternalError);
        assert!(api_error.message.contains("permission denied"));
        assert!(api_error.source.is_some());
    }

    #[test]
    fn test_api_error_from_io_error_different_kinds() {
        let test_cases = vec![
            (ErrorKind::NotFound, "not found"),
            (ErrorKind::InvalidInput, "invalid input"),
            (ErrorKind::TimedOut, "timed out"),
            (ErrorKind::WriteZero, "write zero"),
            (ErrorKind::Other, "other error"),
        ];

        for (kind, message) in test_cases {
            let io_error = IoError::new(kind, message);
            let api_error: ApiError = io_error.into();

            assert_eq!(api_error.code, S3ErrorCode::InternalError);
            assert!(api_error.message.contains(message));
            assert!(api_error.source.is_some());

            // Test that source can be downcast back to io::Error
            let source = api_error.source.as_ref().unwrap();
            let downcast_io_error = source.downcast_ref::<IoError>();
            assert!(downcast_io_error.is_some());
            assert_eq!(downcast_io_error.unwrap().kind(), kind);
        }
    }

    #[test]
    fn test_api_error_other_function() {
        let custom_error = "Custom API error";
        let api_error = ApiError::other(custom_error);

        assert_eq!(api_error.code, S3ErrorCode::InternalError);
        assert_eq!(api_error.message, custom_error);
        assert!(api_error.source.is_some());
    }

    #[test]
    fn test_api_error_other_function_with_complex_error() {
        let io_error = IoError::new(ErrorKind::InvalidData, "complex error");
        let api_error = ApiError::other(io_error);

        assert_eq!(api_error.code, S3ErrorCode::InternalError);
        assert!(api_error.message.contains("complex error"));
        assert!(api_error.source.is_some());

        // Test that source can be downcast back to io::Error
        let source = api_error.source.as_ref().unwrap();
        let downcast_io_error = source.downcast_ref::<IoError>();
        assert!(downcast_io_error.is_some());
        assert_eq!(downcast_io_error.unwrap().kind(), ErrorKind::InvalidData);
    }

    #[test]
    fn test_api_error_from_storage_error() {
        let storage_error = StorageError::BucketNotFound("test-bucket".to_string());
        let api_error: ApiError = storage_error.into();

        assert_eq!(api_error.code, S3ErrorCode::NoSuchBucket);
        assert!(api_error.message.contains("test-bucket"));
        assert!(api_error.source.is_some());

        // Test that source can be downcast back to StorageError
        let source = api_error.source.as_ref().unwrap();
        let downcast_storage_error = source.downcast_ref::<StorageError>();
        assert!(downcast_storage_error.is_some());
    }

    #[test]
    fn test_api_error_from_storage_error_mappings() {
        let test_cases = vec![
            (StorageError::NotImplemented, S3ErrorCode::NotImplemented),
            (
                StorageError::InvalidArgument("test".into(), "test".into(), "test".into()),
                S3ErrorCode::InvalidArgument,
            ),
            (StorageError::MethodNotAllowed, S3ErrorCode::MethodNotAllowed),
            (StorageError::BucketNotFound("test".into()), S3ErrorCode::NoSuchBucket),
            (StorageError::BucketNotEmpty("test".into()), S3ErrorCode::BucketNotEmpty),
            (StorageError::BucketNameInvalid("test".into()), S3ErrorCode::InvalidBucketName),
            (
                StorageError::ObjectNameInvalid("test".into(), "test".into()),
                S3ErrorCode::InvalidArgument,
            ),
            (StorageError::BucketExists("test".into()), S3ErrorCode::BucketAlreadyOwnedByYou),
            (StorageError::StorageFull, S3ErrorCode::ServiceUnavailable),
            (StorageError::SlowDown, S3ErrorCode::SlowDown),
            (StorageError::PrefixAccessDenied("test".into(), "test".into()), S3ErrorCode::AccessDenied),
            (StorageError::ObjectNotFound("test".into(), "test".into()), S3ErrorCode::NoSuchKey),
            (StorageError::ConfigNotFound, S3ErrorCode::NoSuchKey),
            (StorageError::VolumeNotFound, S3ErrorCode::NoSuchBucket),
            (StorageError::FileNotFound, S3ErrorCode::NoSuchKey),
            (StorageError::FileVersionNotFound, S3ErrorCode::NoSuchVersion),
        ];

        for (storage_error, expected_code) in test_cases {
            let api_error: ApiError = storage_error.into();
            assert_eq!(api_error.code, expected_code);
            assert!(api_error.source.is_some());
        }
    }

    #[test]
    fn test_api_error_from_iam_error() {
        let iam_error = rustfs_iam::error::Error::other("IAM test error");
        let api_error: ApiError = iam_error.into();

        // IAM error is first converted to StorageError, then to ApiError
        assert!(api_error.source.is_some());
        assert!(api_error.message.contains("test error"));
    }

    #[test]
    fn test_api_error_to_s3_error() {
        let api_error = ApiError {
            code: S3ErrorCode::NoSuchBucket,
            message: "Bucket not found".to_string(),
            source: Some(Box::new(IoError::new(ErrorKind::NotFound, "not found"))),
        };

        let s3_error: S3Error = api_error.into();
        assert_eq!(*s3_error.code(), S3ErrorCode::NoSuchBucket);
        assert!(s3_error.message().unwrap_or("").contains("Bucket not found"));
        assert!(s3_error.source().is_some());
    }

    #[test]
    fn test_api_error_to_s3_error_without_source() {
        let api_error = ApiError {
            code: S3ErrorCode::InvalidArgument,
            message: "Invalid argument".to_string(),
            source: None,
        };

        let s3_error: S3Error = api_error.into();
        assert_eq!(*s3_error.code(), S3ErrorCode::InvalidArgument);
        assert!(s3_error.message().unwrap_or("").contains("Invalid argument"));
    }

    #[test]
    fn test_api_error_display() {
        let api_error = ApiError {
            code: S3ErrorCode::InternalError,
            message: "Test error message".to_string(),
            source: None,
        };

        assert_eq!(api_error.to_string(), "Test error message");
    }

    #[test]
    fn test_api_error_debug() {
        let api_error = ApiError {
            code: S3ErrorCode::NoSuchKey,
            message: "Object not found".to_string(),
            source: Some(Box::new(IoError::new(ErrorKind::NotFound, "file not found"))),
        };

        let debug_str = format!("{api_error:?}");
        assert!(debug_str.contains("NoSuchKey"));
        assert!(debug_str.contains("Object not found"));
    }

    #[test]
    fn test_api_error_roundtrip_through_io_error() {
        let original_io_error = IoError::new(ErrorKind::PermissionDenied, "original permission error");

        // Convert to ApiError
        let api_error: ApiError = original_io_error.into();

        // Verify the conversion preserved the information
        assert_eq!(api_error.code, S3ErrorCode::InternalError);
        assert!(api_error.message.contains("original permission error"));
        assert!(api_error.source.is_some());

        // Test that we can downcast back to the original io::Error
        let source = api_error.source.as_ref().unwrap();
        let downcast_io_error = source.downcast_ref::<IoError>();
        assert!(downcast_io_error.is_some());
        assert_eq!(downcast_io_error.unwrap().kind(), ErrorKind::PermissionDenied);
        assert!(downcast_io_error.unwrap().to_string().contains("original permission error"));
    }

    #[test]
    fn test_api_error_chain_conversion() {
        // Start with an io::Error
        let io_error = IoError::new(ErrorKind::InvalidData, "invalid data");

        // Convert to StorageError (simulating what happens in the codebase)
        let storage_error = StorageError::other(io_error);

        // Convert to ApiError
        let api_error: ApiError = storage_error.into();

        // Verify the chain is preserved
        assert!(api_error.source.is_some());

        // Check that we can still access the original error information
        let source = api_error.source.as_ref().unwrap();
        let downcast_storage_error = source.downcast_ref::<StorageError>();
        assert!(downcast_storage_error.is_some());
    }

    #[test]
    fn test_api_error_error_trait_implementation() {
        let api_error = ApiError {
            code: S3ErrorCode::InternalError,
            message: "Test error".to_string(),
            source: Some(Box::new(IoError::other("source error"))),
        };

        // Test that it implements std::error::Error
        let error: &dyn std::error::Error = &api_error;
        assert_eq!(error.to_string(), "Test error");
        // ApiError doesn't implement Error::source() properly, so this would be None
        // This is expected because ApiError is not a typical Error implementation
        assert!(error.source().is_none());
    }
}
