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
//! Responsible for: [`handler_error`], called by the gateway backend's one handler and nothing
//! else.
//! NOT responsible for: choosing an error (the use case's), or writing it (the gateway renders the
//! document, the status and the request identifiers).
//! Upstream: `rustfs_s3_types::S3Error`. Downstream: `rustfs_gateway::HandlerError`.
//!
//! The gateway's error carries a code with its status, a message, and response headers from the
//! closed set of [`ErrorHeader`], every member of which is read here. An error this function cannot
//! carry whole is answered `500 InternalError` rather than sent with a member missing: a `416`
//! without its `Content-Range`, or a throttle without its `Retry-After`, tells a client something
//! RustFS did not say.

use http::{HeaderName, HeaderValue};
use rustfs_gateway::{ErrorHeader, HandlerError, HttpDate, RedirectTarget, RegionLabel, VersionIdLabel};
use rustfs_gateway_types::{ErrorCode, Timestamp, TimestampFormat};
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
    // A declared code compares equal to the custom code of the same spelling and status.
    let code = ErrorCode::custom(parts.code.as_str().to_owned(), status);
    let mut mapped = HandlerError::new(code, parts.message.unwrap_or(Cow::Borrowed("")));
    for (name, value) in parts.headers.iter().flatten() {
        let Some(header) = error_header(name, value) else {
            return HandlerError::internal_error(UNWRITABLE_HEADER);
        };
        mapped = mapped.with_header(header);
    }
    mapped
}

/// The gateway's fact for one error response header, or `None` for a header it cannot write.
fn error_header(name: &HeaderName, value: &HeaderValue) -> Option<ErrorHeader> {
    let value = value.to_str().ok()?;
    match name.as_str() {
        "content-range" => {
            let complete_length = value.strip_prefix("bytes */")?.parse().ok()?;
            Some(ErrorHeader::UnsatisfiedRange { complete_length })
        }
        "retry-after" => Some(ErrorHeader::RetryAfter {
            seconds: value.parse().ok()?,
        }),
        "x-amz-bucket-region" => Some(ErrorHeader::BucketRegion {
            region: RegionLabel::new(value).ok()?,
        }),
        "location" => Some(ErrorHeader::RedirectLocation {
            target: RedirectTarget::new(value).ok()?,
        }),
        "x-amz-delete-marker" => (value == "true").then_some(ErrorHeader::DeleteMarker),
        "x-amz-version-id" => Some(ErrorHeader::VersionId {
            version_id: VersionIdLabel::new(value).ok()?,
        }),
        "last-modified" => {
            let seconds = Timestamp::parse(value, TimestampFormat::HttpDate).ok()?.secs();
            Some(ErrorHeader::LastModified {
                at: HttpDate::from_unix_seconds(seconds).ok()?,
            })
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::{HeaderMap, StatusCode, header};
    use rustfs_s3_types::S3ErrorCode;

    fn with_headers(code: S3ErrorCode, headers: &[(HeaderName, &'static str)]) -> HandlerError {
        let mut map = HeaderMap::new();
        for (name, value) in headers {
            map.append(name.clone(), HeaderValue::from_static(value));
        }
        let mut error = S3Error::with_message(code, "refused");
        error.set_headers(map);
        handler_error(error)
    }

    #[test]
    fn a_declared_code_keeps_its_spelling_status_and_message() {
        let mapped = handler_error(S3Error::with_message(S3ErrorCode::AccessDenied, "Access Denied"));

        assert_eq!(mapped.code(), &ErrorCode::ACCESS_DENIED);
        assert_eq!(mapped.message(), "Access Denied");
        assert!(mapped.headers().is_empty());
    }

    #[test]
    fn an_internal_error_stays_a_server_fault() {
        let mapped = handler_error(S3Error::with_message(S3ErrorCode::InternalError, "Not init"));

        assert_eq!(mapped.code(), &ErrorCode::INTERNAL_ERROR);
        assert_eq!(mapped.message(), "Not init");
    }

    #[test]
    fn an_explicit_status_overrides_the_declared_one() {
        let mut error = S3Error::with_message(S3ErrorCode::AccessDenied, "locked");
        error.set_status_code(StatusCode::CONFLICT);

        let mapped = handler_error(error);

        assert_eq!(mapped.code().as_str(), "AccessDenied");
        assert_eq!(mapped.code().default_status(), StatusCode::CONFLICT);
        assert_ne!(mapped.code(), &ErrorCode::ACCESS_DENIED, "a different status is a different code");
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

        assert_eq!(mapped.code(), &ErrorCode::INTERNAL_ERROR);
        assert_eq!(mapped.message(), MISSING_STATUS);
    }

    #[test]
    fn a_missing_message_is_not_invented() {
        let mapped = handler_error(S3Error::new(S3ErrorCode::AccessDenied));

        assert_eq!(mapped.code().as_str(), "AccessDenied");
        assert_eq!(mapped.message(), "");
    }

    #[test]
    fn every_header_of_the_closed_set_is_carried() {
        let mapped = with_headers(
            S3ErrorCode::InvalidRange,
            &[
                (header::CONTENT_RANGE, "bytes */42"),
                (header::RETRY_AFTER, "7"),
                (HeaderName::from_static("x-amz-bucket-region"), "eu-west-1"),
                (header::LOCATION, "https://bucket.s3.eu-west-1.example.test/"),
                (HeaderName::from_static("x-amz-delete-marker"), "true"),
                (HeaderName::from_static("x-amz-version-id"), "null"),
                (header::LAST_MODIFIED, "Tue, 14 Nov 2023 22:13:20 GMT"),
            ],
        );

        let carried: Vec<HeaderName> = mapped.headers().iter().map(ErrorHeader::name).collect();
        assert_eq!(
            carried,
            [
                header::CONTENT_RANGE,
                header::RETRY_AFTER,
                HeaderName::from_static("x-amz-bucket-region"),
                header::LOCATION,
                HeaderName::from_static("x-amz-delete-marker"),
                HeaderName::from_static("x-amz-version-id"),
                header::LAST_MODIFIED,
            ]
        );
        assert!(
            mapped
                .headers()
                .contains(&ErrorHeader::UnsatisfiedRange { complete_length: 42 })
        );
        assert!(mapped.headers().contains(&ErrorHeader::RetryAfter { seconds: 7 }));
        let rendered: Vec<String> = mapped.headers().iter().map(ErrorHeader::value).collect();
        assert_eq!(rendered[6], "Tue, 14 Nov 2023 22:13:20 GMT");
    }

    #[test]
    fn a_header_outside_the_closed_set_fails_the_whole_error() {
        let mapped = with_headers(S3ErrorCode::AccessDenied, &[(HeaderName::from_static("x-rustfs-unlisted"), "value")]);

        assert_eq!(mapped.code(), &ErrorCode::INTERNAL_ERROR);
        assert_eq!(mapped.message(), UNWRITABLE_HEADER);
        assert!(mapped.headers().is_empty());
    }

    #[test]
    fn an_unreadable_value_of_a_known_header_fails_the_whole_error() {
        for (name, value) in [
            (header::RETRY_AFTER, "soon"),
            (header::CONTENT_RANGE, "bytes 0-1/42"),
            (HeaderName::from_static("x-amz-delete-marker"), "false"),
            (HeaderName::from_static("x-amz-bucket-region"), ""),
            (header::LAST_MODIFIED, "yesterday"),
        ] {
            let mapped = with_headers(S3ErrorCode::SlowDown, &[(name.clone(), value)]);

            assert_eq!(mapped.message(), UNWRITABLE_HEADER, "{name}: {value}");
        }
    }
}
