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

//! Map backend and authorization failures to TFTP wire error codes.

use async_tftp::packet::Error as TftpPacketError;
use s3s::{S3Error, S3ErrorCode};
use std::any::Any;
use std::fmt::Display;
use tracing::warn;

const LOG_COMPONENT_PROTOCOLS: &str = "protocols";
const LOG_SUBSYSTEM_TFTP_ERRORS: &str = "tftp_errors";
const EVENT_TFTP_ERROR_MAPPING: &str = "tftp_error_mapping";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BackendErrorKind {
    NotFound,
    PermissionDenied,
    NoSuchUpload,
    Other,
}

fn classify_s3_code(code: &S3ErrorCode) -> BackendErrorKind {
    match code {
        S3ErrorCode::NoSuchKey | S3ErrorCode::NoSuchBucket => BackendErrorKind::NotFound,
        S3ErrorCode::AccessDenied => BackendErrorKind::PermissionDenied,
        S3ErrorCode::NoSuchUpload => BackendErrorKind::NoSuchUpload,
        _ => BackendErrorKind::Other,
    }
}

fn classify_backend_error<E: Display + 'static>(err: &E) -> BackendErrorKind {
    let any = err as &dyn Any;
    if let Some(err) = any.downcast_ref::<S3Error>() {
        return classify_s3_code(err.code());
    }
    BackendErrorKind::Other
}

/// Returns true when AbortMultipartUpload reports an already-missing upload.
pub fn is_no_such_upload_backend_error<E: Display + 'static>(err: &E) -> bool {
    classify_backend_error(err) == BackendErrorKind::NoSuchUpload
}

pub fn backend_error_to_tftp<E: Display + 'static>(op: &str, err: E) -> TftpPacketError {
    let msg = err.to_string();
    let code = match classify_backend_error(&err) {
        BackendErrorKind::NotFound => TftpPacketError::FileNotFound,
        BackendErrorKind::PermissionDenied => TftpPacketError::PermissionDenied,
        BackendErrorKind::NoSuchUpload | BackendErrorKind::Other => TftpPacketError::UnknownError,
    };
    warn!(
        event = EVENT_TFTP_ERROR_MAPPING,
        component = LOG_COMPONENT_PROTOCOLS,
        subsystem = LOG_SUBSYSTEM_TFTP_ERRORS,
        op = op,
        err = %msg,
        mapped = ?code,
        "tftp error mapping"
    );
    code
}
