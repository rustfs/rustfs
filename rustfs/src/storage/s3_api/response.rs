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

use crate::error::ApiError;
use rustfs_ecstore::error::StorageError;
use s3s::{S3Error, S3ErrorCode, S3Response};

pub(crate) fn s3_response<T>(output: T) -> S3Response<T> {
    S3Response::new(output)
}

pub(crate) fn not_initialized_error() -> S3Error {
    S3Error::with_message(S3ErrorCode::InternalError, "Not init")
}

pub(crate) fn access_denied_error() -> S3Error {
    S3Error::with_message(S3ErrorCode::AccessDenied, "Access Denied")
}

pub(crate) fn map_abort_multipart_upload_error(err: StorageError) -> S3Error {
    // For abort multipart upload, malformed upload IDs should be hidden as NoSuchUpload
    // to match S3 API compatibility expectations.
    if matches!(err, StorageError::MalformedUploadID(_)) {
        return S3Error::new(S3ErrorCode::NoSuchUpload);
    }

    ApiError::from(err).into()
}

#[cfg(test)]
mod tests {
    use super::{access_denied_error, map_abort_multipart_upload_error, not_initialized_error, s3_response};
    use rustfs_ecstore::error::StorageError;
    use s3s::{S3ErrorCode, S3Response};

    #[test]
    fn test_s3_response_wraps_output() {
        let response: S3Response<i32> = s3_response(7);
        assert_eq!(response.output, 7);
    }

    #[test]
    fn test_not_initialized_error_shape() {
        let err = not_initialized_error();
        assert_eq!(*err.code(), S3ErrorCode::InternalError);
        assert_eq!(err.message(), Some("Not init"));
    }

    #[test]
    fn test_access_denied_error_shape() {
        let err = access_denied_error();
        assert_eq!(*err.code(), S3ErrorCode::AccessDenied);
        assert_eq!(err.message(), Some("Access Denied"));
    }

    #[test]
    fn test_map_abort_multipart_upload_error_for_malformed_id() {
        let err = map_abort_multipart_upload_error(StorageError::MalformedUploadID("bad-id".to_string()));
        assert_eq!(*err.code(), S3ErrorCode::NoSuchUpload);
    }

    #[test]
    fn test_map_abort_multipart_upload_error_for_unexpected_error() {
        let err = map_abort_multipart_upload_error(StorageError::Unexpected);
        assert_eq!(*err.code(), S3ErrorCode::InternalError);
    }
}
