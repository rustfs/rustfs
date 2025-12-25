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

use s3s::S3Error;

/// S3 error codes
#[derive(Debug, thiserror::Error)]
pub enum S3ErrorCode {
    #[error("AccessDenied")]
    AccessDenied,
    #[error("NoSuchKey")]
    NoSuchKey,
    #[error("NoSuchBucket")]
    NoSuchBucket,
    #[error("BucketNotEmpty")]
    BucketNotEmpty,
    #[error("BucketAlreadyExists")]
    BucketAlreadyExists,
    #[error("InvalidBucketName")]
    InvalidBucketName,
    #[error("InvalidObjectName")]
    InvalidObjectName,
    #[error("InvalidRequest")]
    InvalidRequest,
    #[error("InternalError")]
    InternalError,
}

impl S3ErrorCode {
    /// Convert to S3Error
    pub fn to_s3_error(&self) -> S3Error {
        match self {
            S3ErrorCode::AccessDenied => S3Error::new(s3s::S3ErrorCode::AccessDenied),
            S3ErrorCode::NoSuchKey => S3Error::new(s3s::S3ErrorCode::NoSuchKey),
            S3ErrorCode::NoSuchBucket => S3Error::new(s3s::S3ErrorCode::NoSuchBucket),
            S3ErrorCode::BucketNotEmpty => S3Error::new(s3s::S3ErrorCode::BucketNotEmpty),
            S3ErrorCode::BucketAlreadyExists => S3Error::new(s3s::S3ErrorCode::BucketAlreadyExists),
            S3ErrorCode::InvalidBucketName => S3Error::new(s3s::S3ErrorCode::InvalidBucketName),
            S3ErrorCode::InvalidObjectName => S3Error::new(s3s::S3ErrorCode::InvalidObjectState),
            S3ErrorCode::InvalidRequest => S3Error::new(s3s::S3ErrorCode::InvalidRequest),
            S3ErrorCode::InternalError => S3Error::new(s3s::S3ErrorCode::InternalError),
        }
    }

    /// Convert protocol errors to S3 errors
    pub fn from_protocol_error(protocol: &str, error: &str) -> Self {
        match (protocol, error) {
            // FTP errors
            ("ftp", "550") => S3ErrorCode::NoSuchKey, // File not found
            ("ftp", "550 Permission denied") => S3ErrorCode::AccessDenied,
            ("ftp", "550 Directory not empty") => S3ErrorCode::BucketNotEmpty,
            ("ftp", "550 File exists") => S3ErrorCode::BucketAlreadyExists,

            // SFTP errors
            ("sftp", "NoSuchFile") => S3ErrorCode::NoSuchKey,
            ("sftp", "PermissionDenied") => S3ErrorCode::AccessDenied,
            ("sftp", "Failure") => S3ErrorCode::InternalError,

            // Default
            _ => S3ErrorCode::InternalError,
        }
    }
}

/// Map S3 errors to protocol-specific errors
pub fn map_s3_error_to_protocol(protocol: &str, s3_error: &S3ErrorCode) -> String {
    match (protocol, s3_error) {
        // FTP error codes
        ("ftp", S3ErrorCode::NoSuchKey) => "550 File not found".to_string(),
        ("ftp", S3ErrorCode::AccessDenied) => "550 Permission denied".to_string(),
        ("ftp", S3ErrorCode::NoSuchBucket) => "550 Directory not found".to_string(),
        ("ftp", S3ErrorCode::BucketNotEmpty) => "550 Directory not empty".to_string(),
        ("ftp", S3ErrorCode::BucketAlreadyExists) => "550 Directory already exists".to_string(),

        // SFTP error codes
        ("sftp", S3ErrorCode::NoSuchKey) => "NoSuchFile".to_string(),
        ("sftp", S3ErrorCode::AccessDenied) => "PermissionDenied".to_string(),
        ("sftp", S3ErrorCode::NoSuchBucket) => "NoSuchFile".to_string(), // Directory not found
        ("sftp", S3ErrorCode::BucketNotEmpty) => "Failure".to_string(), // Directory not empty
        ("sftp", S3ErrorCode::BucketAlreadyExists) => "Failure".to_string(), // Directory exists

        // Default
        (_, _) => "Failure".to_string(),
    }
}