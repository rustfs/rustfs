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

//! SftpError type and the helpers that convert backend errors and
//! authorization failures into SftpError, plus the success Status
//! payload constructor.

use super::constants::{http_error_codes, s3_error_codes};
use russh_sftp::protocol::{Status, StatusCode};
use s3s::{S3Error, S3ErrorCode};
use std::{any::Any, fmt::Display};

/// Error type for SFTP operations. Converts to StatusCode for the wire.
#[derive(Debug)]
pub struct SftpError(pub(super) StatusCode);

impl From<SftpError> for StatusCode {
    fn from(err: SftpError) -> Self {
        err.0
    }
}

impl SftpError {
    pub(super) fn code(code: StatusCode) -> Self {
        Self(code)
    }
}

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
        _ => match code.as_str() {
            s3_error_codes::NO_SUCH_KEY
            | s3_error_codes::NO_SUCH_BUCKET
            | s3_error_codes::NOT_FOUND
            | http_error_codes::NOT_FOUND => BackendErrorKind::NotFound,
            s3_error_codes::ACCESS_DENIED | s3_error_codes::FORBIDDEN | http_error_codes::FORBIDDEN => {
                BackendErrorKind::PermissionDenied
            }
            s3_error_codes::NO_SUCH_UPLOAD => BackendErrorKind::NoSuchUpload,
            _ => BackendErrorKind::Other,
        },
    }
}

#[cfg(test)]
fn classify_dummy_error(err: &crate::common::dummy_storage::DummyError) -> BackendErrorKind {
    match err {
        crate::common::dummy_storage::DummyError::NoSuchKey(_) | crate::common::dummy_storage::DummyError::NoSuchBucket(_) => {
            BackendErrorKind::NotFound
        }
        crate::common::dummy_storage::DummyError::AccessDenied(_) => BackendErrorKind::PermissionDenied,
        crate::common::dummy_storage::DummyError::NoSuchUpload(_) => BackendErrorKind::NoSuchUpload,
        crate::common::dummy_storage::DummyError::Injected(_) | crate::common::dummy_storage::DummyError::Unconfigured(_) => {
            BackendErrorKind::Other
        }
    }
}

fn classify_backend_error<E: Display + 'static>(err: &E) -> BackendErrorKind {
    let any = err as &dyn Any;
    if let Some(err) = any.downcast_ref::<S3Error>() {
        return classify_s3_code(err.code());
    }

    #[cfg(test)]
    if let Some(err) = any.downcast_ref::<crate::common::dummy_storage::DummyError>() {
        return classify_dummy_error(err);
    }

    BackendErrorKind::Other
}

/// Map an S3 backend error into an SFTP status code and log the underlying
/// detail server-side. The wire response only carries the status code. The
/// full error is written to the server log for operator diagnosis. Typed
/// backend errors are mapped to the matching SFTP status. Everything else is
/// Failure.
pub(super) fn s3_error_to_sftp<E: Display + 'static>(op: &str, err: E) -> SftpError {
    let msg = err.to_string();
    let code = match classify_backend_error(&err) {
        BackendErrorKind::NotFound => StatusCode::NoSuchFile,
        BackendErrorKind::PermissionDenied => StatusCode::PermissionDenied,
        BackendErrorKind::NoSuchUpload | BackendErrorKind::Other => StatusCode::Failure,
    };
    tracing::warn!(op = %op, err = %msg, "SFTP backend error");
    SftpError::code(code)
}

/// Returns SftpError(PermissionDenied), the status used when
/// authorize_operation rejects an operation with AccessDenied.
pub(super) fn auth_err() -> SftpError {
    SftpError::code(StatusCode::PermissionDenied)
}

/// Returns SftpError(Failure) when the IAM layer is unreachable.
/// SFTPv3 has no service-unavailable status, so Failure is the
/// closest fit. The warn log includes the operation and target so an
/// IAM outage produces a distinct server-side signal from a policy
/// deny.
pub(super) fn auth_err_unreachable(op: &str, bucket: &str, key: Option<&str>) -> SftpError {
    tracing::warn!(
        op = op,
        bucket = %bucket,
        key = key.unwrap_or("-"),
        "SFTP authorisation rejected because the IAM system was unreachable"
    );
    SftpError::code(StatusCode::Failure)
}

/// Build the SSH_FX_OK Status payload returned by write operation
/// handlers on success (CLOSE, REMOVE, MKDIR, RMDIR, RENAME, SETSTAT,
/// FSETSTAT).
pub(super) fn ok_status(id: u32) -> Status {
    Status {
        id,
        status_code: StatusCode::Ok,
        error_message: String::new(),
        language_tag: "en".to_string(),
    }
}

/// Classify a backend error as the not-found category that
/// distinguishes the EXCLUDE create accept path (object does not exist)
/// from a backend failure that needs propagating. Mirrors the typed set
/// recognised by s3_error_to_sftp.
pub(super) fn is_not_found_error<E: Display + 'static>(err: &E) -> bool {
    classify_backend_error(err) == BackendErrorKind::NotFound
}

/// Returns true when AbortMultipartUpload reports an already-missing upload.
pub(super) fn is_no_such_upload_error<E: Display + 'static>(err: &E) -> bool {
    classify_backend_error(err) == BackendErrorKind::NoSuchUpload
}

#[cfg(test)]
mod tests {
    use crate::common::dummy_storage::DummyError;

    use super::*;

    #[test]
    fn ok_status_has_ok_code_and_empty_message() {
        let status = ok_status(17);
        assert_eq!(status.id, 17);
        assert!(matches!(status.status_code, StatusCode::Ok));
        assert!(status.error_message.is_empty());
        assert_eq!(status.language_tag, "en");
    }

    #[test]
    fn is_not_found_uses_s3_error_code_not_message_text() {
        let err = S3Error::with_message(S3ErrorCode::NoSuchKey, "object missing");
        assert!(is_not_found_error(&err));

        let err = S3Error::with_message(S3ErrorCode::NoSuchBucket, "bucket missing");
        assert!(is_not_found_error(&err));

        let err = S3Error::with_message(S3ErrorCode::AccessDenied, "not found text in deny message");
        assert!(!is_not_found_error(&err));
    }

    #[test]
    fn s3_error_to_sftp_uses_s3_error_code_not_message_text() {
        let check = |code, msg| -> StatusCode { StatusCode::from(s3_error_to_sftp("test", S3Error::with_message(code, msg))) };
        assert!(matches!(check(S3ErrorCode::AccessDenied, "policy denied"), StatusCode::PermissionDenied));
        assert!(matches!(check(S3ErrorCode::NoSuchKey, "object missing"), StatusCode::NoSuchFile));
        assert!(matches!(
            check(S3ErrorCode::InternalError, "AccessDenied appears only in message"),
            StatusCode::Failure
        ));
    }

    #[test]
    fn unknown_errors_do_not_classify_by_display_substrings() {
        struct E(&'static str);
        impl std::fmt::Display for E {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(self.0)
            }
        }
        let check = |msg: &'static str| -> StatusCode { StatusCode::from(s3_error_to_sftp("test", E(msg))) };
        assert!(matches!(check("AccessDenied"), StatusCode::Failure));
        assert!(matches!(check("Forbidden"), StatusCode::Failure));
        assert!(matches!(check("403"), StatusCode::Failure));
        assert!(matches!(check("NoSuchKey"), StatusCode::Failure));
        assert!(matches!(check("something unexpected"), StatusCode::Failure));
    }

    #[test]
    fn no_such_upload_uses_s3_error_code() {
        let err = S3Error::with_message(S3ErrorCode::NoSuchUpload, "upload is already gone");
        assert!(is_no_such_upload_error(&err));

        let err = S3Error::with_message(S3ErrorCode::InternalError, "NoSuchUpload appears only in message");
        assert!(!is_no_such_upload_error(&err));

        let err = DummyError::NoSuchUpload("upload is already gone".to_string());
        assert!(is_no_such_upload_error(&err));
    }
}
