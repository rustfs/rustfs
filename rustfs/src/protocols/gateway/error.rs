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

// FTP error code constants
pub mod ftp_errors {
    pub const FILE_NOT_FOUND: &str = "550 File not found";
    pub const DIRECTORY_NOT_FOUND: &str = "550 Directory not found";
    pub const PERMISSION_DENIED: &str = "550 Permission denied";
    pub const DIRECTORY_NOT_EMPTY: &str = "550 Directory not empty";
    pub const DIRECTORY_ALREADY_EXISTS: &str = "550 Directory already exists";
    pub const INVALID_DIRECTORY_NAME: &str = "553 Invalid directory name";
    pub const INVALID_FILE_NAME: &str = "553 Invalid file name";
    pub const INVALID_REQUEST: &str = "501 Invalid request";
    pub const INTERNAL_SERVER_ERROR: &str = "421 Internal server error";
}

// FTP error messages mapping
pub fn map_s3_error_to_ftp_string(s3_error: &s3s::S3Error) -> String {
    match s3_error.code() {
        s3s::S3ErrorCode::NoSuchKey => ftp_errors::FILE_NOT_FOUND.to_string(),
        s3s::S3ErrorCode::NoSuchBucket => ftp_errors::DIRECTORY_NOT_FOUND.to_string(),
        s3s::S3ErrorCode::AccessDenied => ftp_errors::PERMISSION_DENIED.to_string(),
        s3s::S3ErrorCode::BucketNotEmpty => ftp_errors::DIRECTORY_NOT_EMPTY.to_string(),
        s3s::S3ErrorCode::BucketAlreadyExists => ftp_errors::DIRECTORY_ALREADY_EXISTS.to_string(),
        s3s::S3ErrorCode::InvalidBucketName => ftp_errors::INVALID_DIRECTORY_NAME.to_string(),
        s3s::S3ErrorCode::InvalidObjectState => ftp_errors::INVALID_FILE_NAME.to_string(),
        s3s::S3ErrorCode::InvalidRequest => ftp_errors::INVALID_REQUEST.to_string(),
        s3s::S3ErrorCode::InternalError => ftp_errors::INTERNAL_SERVER_ERROR.to_string(),
        _ => ftp_errors::INTERNAL_SERVER_ERROR.to_string(),
    }
}

/// Map S3Error to FTPS libunftp Error
pub fn map_s3_error_to_ftps(s3_error: &s3s::S3Error) -> libunftp::storage::Error {
    use libunftp::storage::{Error, ErrorKind};

    match s3_error.code() {
        s3s::S3ErrorCode::NoSuchKey | s3s::S3ErrorCode::NoSuchBucket => {
            Error::new(ErrorKind::PermanentFileNotAvailable, map_s3_error_to_ftp_string(s3_error))
        }
        s3s::S3ErrorCode::AccessDenied => Error::new(ErrorKind::PermissionDenied, map_s3_error_to_ftp_string(s3_error)),
        s3s::S3ErrorCode::InvalidRequest | s3s::S3ErrorCode::InvalidBucketName | s3s::S3ErrorCode::InvalidObjectState => {
            Error::new(ErrorKind::PermanentFileNotAvailable, map_s3_error_to_ftp_string(s3_error))
        }
        _ => Error::new(ErrorKind::PermanentFileNotAvailable, map_s3_error_to_ftp_string(s3_error)),
    }
}

/// Map S3Error directly to SFTP StatusCode
pub fn map_s3_error_to_sftp_status(s3_error: &s3s::S3Error) -> russh_sftp::protocol::StatusCode {
    use russh_sftp::protocol::StatusCode;

    match s3_error.code() {
        s3s::S3ErrorCode::NoSuchKey => StatusCode::NoSuchFile, // SSH_FX_NO_SUCH_FILE (2)
        s3s::S3ErrorCode::NoSuchBucket => StatusCode::NoSuchFile, // SSH_FX_NO_SUCH_FILE (2)
        s3s::S3ErrorCode::AccessDenied => StatusCode::PermissionDenied, // SSH_FX_PERMISSION_DENIED (3)
        s3s::S3ErrorCode::BucketNotEmpty => StatusCode::Failure, // SSH_FX_DIR_NOT_EMPTY (21)
        s3s::S3ErrorCode::BucketAlreadyExists => StatusCode::Failure, // SSH_FX_FILE_ALREADY_EXISTS (17)
        s3s::S3ErrorCode::InvalidBucketName => StatusCode::Failure, // SSH_FX_INVALID_FILENAME (22)
        s3s::S3ErrorCode::InvalidObjectState => StatusCode::Failure, // SSH_FX_INVALID_FILENAME (22)
        s3s::S3ErrorCode::InvalidRequest => StatusCode::OpUnsupported, // SSH_FX_OP_UNSUPPORTED (5)
        s3s::S3ErrorCode::InternalError => StatusCode::Failure, // SSH_FX_FAILURE (4)
        _ => StatusCode::Failure,                              // SSH_FX_FAILURE as default
    }
}
