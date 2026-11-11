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
//! The one conversion of a RustFS error into the gateway's handler error.
//!
//! Responsible for: [`handler_error`], called by every gateway handler and by nothing else.
//! NOT responsible for: choosing an error (the use case's), or writing it (the gateway renders the
//! document, the status and the request identifiers).
//! Upstream: `rustfs_s3_types::S3Error`. Downstream: `rustfs_gateway::HandlerError`.
//!
//! The gateway's error carries a code with its status, a message, and response headers from a
//! closed set. An error this function cannot carry whole is answered `500 InternalError` rather
//! than sent with a member missing: a `416` without its `Content-Range`, or a throttle without
//! its `Retry-After`, tells a client something RustFS did not say. Each operation slice that
//! raises a header not listed in [`error_header`] adds it there, with its test.

use http::{HeaderName, HeaderValue, StatusCode};
use rustfs_gateway::{ErrorHeader, HandlerError};
use rustfs_gateway_types::ErrorCode;
use rustfs_s3_types::S3Error;
use std::borrow::Cow;

/// Answered when an error names a code without a status: the raiser of a custom code must set one.
const MISSING_STATUS: &str = "the error carries a code without a status";
/// Answered when an error carries a response header this edge cannot write.
const UNWRITABLE_HEADER: &str = "the error carries a response header the gateway cannot write";

/// Converts a use-case error into the error the gateway answers with: the same code, status and
/// message, and every response header it carries.
pub(crate) fn handler_error(error: S3Error) -> HandlerError {
    let parts = error.into_parts();
    let Some(status) = parts.status.or_else(|| parts.code.status_code()) else {
        return HandlerError::internal_error(MISSING_STATUS);
    };
    let mut mapped = HandlerError::new(error_code(parts.code.as_str(), status), parts.message.unwrap_or(Cow::Borrowed("")));
    for (name, value) in parts.headers.iter().flatten() {
        let Some(header) = error_header(name, value) else {
            return HandlerError::internal_error(UNWRITABLE_HEADER);
        };
        mapped = mapped.with_header(header);
    }
    mapped
}

/// The gateway's code for `name` answered with `status`: its own declared code when both agree,
/// otherwise a code with exactly the spelling and status RustFS chose.
fn error_code(name: &str, status: StatusCode) -> ErrorCode {
    match ErrorCode::known(name) {
        Some(declared) if declared.default_status() == status => declared,
        _ => ErrorCode::custom(name.to_owned(), status),
    }
}

/// The gateway's spelling of one error response header, or `None` for a header it cannot write.
fn error_header(name: &HeaderName, value: &HeaderValue) -> Option<ErrorHeader> {
    if name == http::header::RETRY_AFTER {
        let seconds = value.to_str().ok()?.parse().ok()?;
        return Some(ErrorHeader::RetryAfter { seconds });
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::HeaderMap;
    use rustfs_s3_types::S3ErrorCode;

    #[test]
    fn a_declared_code_keeps_its_spelling_status_and_message() {
        let mapped = handler_error(S3Error::with_message(S3ErrorCode::AccessDenied, "Access Denied"));

        assert_eq!(mapped.code().as_str(), "AccessDenied");
        assert_eq!(mapped.code().default_status(), StatusCode::FORBIDDEN);
        assert_eq!(mapped.message(), "Access Denied");
        assert!(mapped.headers().is_empty());
    }

    #[test]
    fn an_internal_error_stays_a_server_fault() {
        let mapped = handler_error(S3Error::with_message(S3ErrorCode::InternalError, "Not init"));

        assert_eq!(mapped.code().as_str(), "InternalError");
        assert_eq!(mapped.code().default_status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(mapped.message(), "Not init");
    }

    #[test]
    fn an_explicit_status_overrides_the_declared_one() {
        let mut error = S3Error::with_message(S3ErrorCode::AccessDenied, "locked");
        error.set_status_code(StatusCode::CONFLICT);

        let mapped = handler_error(error);

        assert_eq!(mapped.code().as_str(), "AccessDenied");
        assert_eq!(mapped.code().default_status(), StatusCode::CONFLICT);
    }

    #[test]
    fn a_custom_code_keeps_the_status_its_raiser_set() {
        let mut error = S3Error::with_message(S3ErrorCode::Custom("RustFsOnly".into()), "custom");
        error.set_status_code(StatusCode::SERVICE_UNAVAILABLE);

        let mapped = handler_error(error);

        assert_eq!(mapped.code().as_str(), "RustFsOnly");
        assert_eq!(mapped.code().default_status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(mapped.message(), "custom");
    }

    #[test]
    fn a_custom_code_without_a_status_is_a_server_fault_not_a_guess() {
        let mapped = handler_error(S3Error::with_message(S3ErrorCode::Custom("RustFsOnly".into()), "custom"));

        assert_eq!(mapped.code().as_str(), "InternalError");
        assert_eq!(mapped.code().default_status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(mapped.message(), MISSING_STATUS);
    }

    #[test]
    fn a_missing_message_is_not_invented() {
        let mapped = handler_error(S3Error::new(S3ErrorCode::AccessDenied));

        assert_eq!(mapped.code().as_str(), "AccessDenied");
        assert_eq!(mapped.message(), "");
    }

    #[test]
    fn a_retry_after_header_is_carried() {
        let mut headers = HeaderMap::new();
        headers.insert(http::header::RETRY_AFTER, HeaderValue::from_static("7"));
        let mut error = S3Error::with_message(S3ErrorCode::SlowDown, "busy");
        error.set_headers(headers);

        let mapped = handler_error(error);

        assert_eq!(mapped.code().as_str(), "SlowDown");
        assert_eq!(mapped.headers(), [ErrorHeader::RetryAfter { seconds: 7 }]);
    }

    #[test]
    fn a_header_the_gateway_cannot_write_fails_the_whole_error() {
        let mut headers = HeaderMap::new();
        headers.insert("x-rustfs-unlisted", HeaderValue::from_static("value"));
        let mut error = S3Error::with_message(S3ErrorCode::AccessDenied, "denied");
        error.set_headers(headers);

        let mapped = handler_error(error);

        assert_eq!(mapped.code().as_str(), "InternalError");
        assert_eq!(mapped.message(), UNWRITABLE_HEADER);
        assert!(mapped.headers().is_empty());
    }

    #[test]
    fn an_unreadable_retry_after_fails_the_whole_error() {
        let mut headers = HeaderMap::new();
        headers.insert(http::header::RETRY_AFTER, HeaderValue::from_static("soon"));
        let mut error = S3Error::with_message(S3ErrorCode::SlowDown, "busy");
        error.set_headers(headers);

        assert_eq!(handler_error(error).message(), UNWRITABLE_HEADER);
    }
}
