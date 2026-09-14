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
    Other,
}

fn classify_s3_code(code: &S3ErrorCode) -> BackendErrorKind {
    match code {
        S3ErrorCode::NoSuchKey | S3ErrorCode::NoSuchBucket => BackendErrorKind::NotFound,
        S3ErrorCode::AccessDenied => BackendErrorKind::PermissionDenied,
        _ => BackendErrorKind::Other,
    }
}

#[cfg(test)]
fn classify_dummy_error(err: &crate::common::dummy_storage::DummyError) -> BackendErrorKind {
    match err {
        crate::common::dummy_storage::DummyError::NoSuchKey(_) | crate::common::dummy_storage::DummyError::NoSuchBucket(_) => {
            BackendErrorKind::NotFound
        }
        crate::common::dummy_storage::DummyError::AccessDenied(_) => BackendErrorKind::PermissionDenied,
        crate::common::dummy_storage::DummyError::Injected(_) | crate::common::dummy_storage::DummyError::Unconfigured(_) => {
            BackendErrorKind::Other
        }
        crate::common::dummy_storage::DummyError::NoSuchUpload(_) => BackendErrorKind::Other,
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

pub fn backend_error_to_tftp<E: Display + 'static>(op: &str, err: E) -> TftpPacketError {
    let msg = err.to_string();
    let code = match classify_backend_error(&err) {
        BackendErrorKind::NotFound => TftpPacketError::FileNotFound,
        BackendErrorKind::PermissionDenied => TftpPacketError::PermissionDenied,
        BackendErrorKind::Other => TftpPacketError::UnknownError,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::dummy_storage::DummyError;

    #[test]
    fn no_such_key_maps_to_file_not_found() {
        let err = backend_error_to_tftp("head_object", S3Error::with_message(S3ErrorCode::NoSuchKey, "missing"));
        assert!(matches!(err, TftpPacketError::FileNotFound));
    }

    #[test]
    fn access_denied_maps_to_permission_denied() {
        let err = backend_error_to_tftp("head_object", S3Error::with_message(S3ErrorCode::AccessDenied, "denied"));
        assert!(matches!(err, TftpPacketError::PermissionDenied));
    }

    #[test]
    fn unknown_errors_map_to_unknown_error() {
        struct E(&'static str);
        impl std::fmt::Display for E {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(self.0)
            }
        }

        let err = backend_error_to_tftp("get_object_range", E("something unexpected"));
        assert!(matches!(err, TftpPacketError::UnknownError));

        let err = backend_error_to_tftp("head_object", DummyError::Injected("backend exploded".into()));
        assert!(matches!(err, TftpPacketError::UnknownError));
    }
}
