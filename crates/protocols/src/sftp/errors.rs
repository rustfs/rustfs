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
use std::fmt::Display;

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

/// Map an S3 backend error into an SFTP status code and log the underlying
/// detail server-side. The wire response only carries the status code. The
/// full error is written to the server log for operator diagnosis. Error
/// strings that mention the common "not found" or "access denied" patterns
/// are mapped to the matching SFTP status. Everything else is Failure.
pub(super) fn s3_error_to_sftp<E: Display>(op: &str, err: E) -> SftpError {
    let msg = err.to_string();
    let code = if msg.contains(s3_error_codes::NO_SUCH_KEY)
        || msg.contains(s3_error_codes::NO_SUCH_BUCKET)
        || msg.contains(s3_error_codes::NOT_FOUND)
        || msg.contains(http_error_codes::NOT_FOUND)
    {
        StatusCode::NoSuchFile
    } else if msg.contains(s3_error_codes::ACCESS_DENIED)
        || msg.contains(s3_error_codes::FORBIDDEN)
        || msg.contains(http_error_codes::FORBIDDEN)
    {
        StatusCode::PermissionDenied
    } else {
        StatusCode::Failure
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

/// Classify an S3 backend error string as the not-found category that
/// distinguishes the EXCLUDE create accept path (object does not exist)
/// from a backend failure that needs propagating. Mirrors the prefix set
/// recognised by s3_error_to_sftp.
pub(super) fn is_not_found_error<E: Display>(err: &E) -> bool {
    let msg = err.to_string();
    msg.contains(s3_error_codes::NO_SUCH_KEY)
        || msg.contains(s3_error_codes::NO_SUCH_BUCKET)
        || msg.contains(s3_error_codes::NOT_FOUND)
        || msg.contains(http_error_codes::NOT_FOUND)
}

#[cfg(test)]
mod tests {
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
    fn is_not_found_recognises_standard_error_patterns() {
        struct E(&'static str);
        impl std::fmt::Display for E {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(self.0)
            }
        }
        assert!(is_not_found_error(&E("S3Error: NoSuchKey")));
        assert!(is_not_found_error(&E("backend returned NoSuchBucket")));
        assert!(is_not_found_error(&E("NotFound (404)")));
        assert!(is_not_found_error(&E("response status 404")));
        assert!(!is_not_found_error(&E("AccessDenied")));
        assert!(!is_not_found_error(&E("generic backend failure")));
    }

    #[test]
    fn s3_error_to_sftp_maps_access_denied_to_permission_denied() {
        struct E(&'static str);
        impl std::fmt::Display for E {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(self.0)
            }
        }
        let check = |msg: &'static str| -> StatusCode { StatusCode::from(s3_error_to_sftp("test", E(msg))) };
        assert!(matches!(check("AccessDenied"), StatusCode::PermissionDenied));
        assert!(matches!(check("Forbidden"), StatusCode::PermissionDenied));
        assert!(matches!(check("403"), StatusCode::PermissionDenied));
        assert!(matches!(check("NoSuchKey"), StatusCode::NoSuchFile));
        assert!(matches!(check("something unexpected"), StatusCode::Failure));
    }
}
