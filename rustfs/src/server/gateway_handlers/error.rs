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
//!
//! The codes the gateway answers only from a context — a missing key, version or bucket, a `304`,
//! a delete marker, a bucket the caller already owns, a redirect — and the `416` it answers only
//! with facts become its legacy RustFS refusal ([`LegacyRustfsRefusal`]), stating whatever facts
//! the error carries. The gateway states no redirect that way, so a redirect is a `500`; RustFS
//! raises none.

use http::header::{CACHE_CONTROL, CONTENT_LOCATION, CONTENT_RANGE, CONTENT_TYPE, ETAG, EXPIRES, LAST_MODIFIED, VARY};
use http::{HeaderMap, HeaderName, HeaderValue};
use rustfs_gateway::{
    ErrorHeader, HandlerError, HandlerErrorContext, HttpDate, LegacyRustfsFacts, LegacyRustfsRefusal, RedirectTarget,
    RegionLabel, StoredHeaderValue, VersionIdLabel,
};
use rustfs_gateway_types::{ETag, ErrorCode, Timestamp, TimestampFormat};
use rustfs_s3_types::S3Error;
use std::borrow::Cow;

/// Answered when an error names a code without a status: the raiser of a custom code must set one.
const MISSING_STATUS: &str = "the error carries a code without a status";
/// Answered when an error carries a response header this edge cannot write.
const UNWRITABLE_HEADER: &str = "the error carries a response header the gateway cannot write";
/// Answered when the gateway refuses a legacy refusal as stated.
const UNSTATABLE_REFUSAL: &str = "the error is not a refusal the gateway can state as legacy RustFS writes it";

/// The codes the gateway refuses to answer without a context (its `is_contextual` set, pinned by
/// a test against the gateway itself), and `InvalidRange`, which it answers only with facts.
const LEGACY_SHAPED: [&str; 11] = [
    "NoSuchKey",
    "NoSuchVersion",
    "NoSuchBucket",
    "PermanentRedirect",
    "TemporaryRedirect",
    "NotModified",
    "AuthorizationHeaderMalformed",
    "MethodNotAllowed",
    "BucketAlreadyOwnedByYou",
    "AccessForbidden",
    "InvalidRange",
];
const DELETE_MARKER: &str = "x-amz-delete-marker";
const VERSION_ID: &str = "x-amz-version-id";

/// Converts a use-case error into the error the gateway answers with: the same code, status and
/// message, and every response header it carries.
pub(crate) fn handler_error(error: S3Error) -> HandlerError {
    let parts = error.into_parts();
    let Some(status) = parts.status.or_else(|| parts.code.status_code()) else {
        return HandlerError::internal_error(MISSING_STATUS);
    };
    // A declared code compares equal to the custom code of the same spelling and status.
    let code = ErrorCode::custom(parts.code.as_str().to_owned(), status);
    let headers = parts.headers.unwrap_or_default();
    if LEGACY_SHAPED.contains(&code.as_str()) {
        return legacy_refusal(code, parts.message, &headers).unwrap_or_else(HandlerError::internal_error);
    }
    let mut mapped = HandlerError::new(code, parts.message.unwrap_or(Cow::Borrowed("")));
    for (name, value) in &headers {
        let Some(header) = error_header(name, value) else {
            return HandlerError::internal_error(UNWRITABLE_HEADER);
        };
        mapped = mapped.with_header(header);
    }
    mapped
}

/// The refusal in its legacy RustFS form, with the facts its code may state: a `304`'s tag,
/// instant, evaluated version and the stored headers it repeats; a `416`'s complete length; a
/// delete marker's version id and instant. The error's `Content-Type` is the gateway's to write;
/// any other header fails the whole error.
fn legacy_refusal(
    code: ErrorCode,
    message: Option<Cow<'static, str>>,
    headers: &HeaderMap,
) -> Result<HandlerError, &'static str> {
    let marker = headers.contains_key(DELETE_MARKER);
    let stated: &[HeaderName] = match code.as_str() {
        "NotModified" => &[
            ETAG,
            LAST_MODIFIED,
            CACHE_CONTROL,
            CONTENT_LOCATION,
            EXPIRES,
            VARY,
            HeaderName::from_static(VERSION_ID),
        ],
        "InvalidRange" => &[CONTENT_RANGE],
        "NoSuchKey" | "MethodNotAllowed" if marker => &[
            HeaderName::from_static(DELETE_MARKER),
            HeaderName::from_static(VERSION_ID),
            LAST_MODIFIED,
        ],
        _ => &[],
    };
    if headers.keys().any(|name| name != CONTENT_TYPE && !stated.contains(name)) {
        return Err(UNWRITABLE_HEADER);
    }
    let version_id = single(headers, VERSION_ID)?;
    let delete_marker = match marker {
        true if single(headers, DELETE_MARKER)? == Some("true") => Some(version_id.ok_or(UNWRITABLE_HEADER)?),
        true => return Err(UNWRITABLE_HEADER),
        false => None,
    };
    let stored = |name: HeaderName| single(headers, name.as_str()).map(|value| value.map(str::to_owned));
    let facts = LegacyRustfsFacts {
        etag: single(headers, ETAG.as_str())?
            .map(|value| {
                ETag::parse_http_header(value)
                    .ok()
                    .filter(|etag| !etag.is_any())
                    .ok_or(UNWRITABLE_HEADER)
            })
            .transpose()?,
        last_modified: single(headers, LAST_MODIFIED.as_str())?
            .map(|value| Timestamp::parse(value, TimestampFormat::HttpDate).map(|at| at.secs()))
            .transpose()
            .map_err(|_| UNWRITABLE_HEADER)?,
        delete_marker: delete_marker.map(str::to_owned),
        complete_length: single(headers, CONTENT_RANGE.as_str())?
            .map(|value| value.strip_prefix("bytes */").and_then(|length| length.parse().ok()))
            .map(|length| length.ok_or(UNWRITABLE_HEADER))
            .transpose()?,
        cache_control: stored(CACHE_CONTROL)?,
        content_location: stored(CONTENT_LOCATION)?,
        expires: stored(EXPIRES)?,
        vary: stored(VARY)?,
        version_id: version_id.filter(|_| !marker).map(str::to_owned),
    };
    let refusal = LegacyRustfsRefusal::new(code, message.map(Cow::into_owned), facts).map_err(|_| UNSTATABLE_REFUSAL)?;
    Ok(HandlerErrorContext::legacy_rustfs(refusal).into())
}

/// The one value of `name`, `None` when absent; a repeated or non-text value is unwritable.
fn single<'a>(headers: &'a HeaderMap, name: &str) -> Result<Option<&'a str>, &'static str> {
    let mut values = headers.get_all(name).iter();
    match (values.next(), values.next()) {
        (None, _) => Ok(None),
        (Some(value), None) => value.to_str().map(Some).map_err(|_| UNWRITABLE_HEADER),
        (Some(_), Some(_)) => Err(UNWRITABLE_HEADER),
    }
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
        "cache-control" => Some(ErrorHeader::CacheControl {
            value: StoredHeaderValue::new(value).ok()?,
        }),
        "content-location" => Some(ErrorHeader::ContentLocation {
            value: StoredHeaderValue::new(value).ok()?,
        }),
        "expires" => Some(ErrorHeader::Expires {
            value: StoredHeaderValue::new(value).ok()?,
        }),
        "vary" => Some(ErrorHeader::Vary {
            value: StoredHeaderValue::new(value).ok()?,
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
    use rustfs_gateway::{ErrorContext, ErrorResolution, InvalidErrorContext, ResponseKind, resolve};
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
            S3ErrorCode::SlowDown,
            &[
                (header::CONTENT_RANGE, "bytes */42"),
                (header::RETRY_AFTER, "7"),
                (HeaderName::from_static("x-amz-bucket-region"), "eu-west-1"),
                (header::LOCATION, "https://bucket.s3.eu-west-1.example.test/"),
                (HeaderName::from_static("x-amz-delete-marker"), "true"),
                (HeaderName::from_static("x-amz-version-id"), "null"),
                (header::LAST_MODIFIED, "Tue, 14 Nov 2023 22:13:20 GMT"),
                (header::CACHE_CONTROL, "max-age=60"),
                (header::CONTENT_LOCATION, "/bucket/key"),
                (header::EXPIRES, "Wed, 15 Nov 2023 22:13:20 GMT"),
                (header::VARY, "Origin"),
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
                header::CACHE_CONTROL,
                header::CONTENT_LOCATION,
                header::EXPIRES,
                header::VARY,
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
            (header::VARY, " Origin"),
        ] {
            let mapped = with_headers(S3ErrorCode::SlowDown, &[(name.clone(), value)]);

            assert_eq!(mapped.message(), UNWRITABLE_HEADER, "{name}: {value}");
        }
    }

    /// What the gateway answers for `error`, read through its own resolution.
    fn resolved(code: S3ErrorCode, headers: &[(&'static str, &'static str)]) -> ErrorResolution {
        let named: Vec<(HeaderName, &'static str)> = headers
            .iter()
            .map(|(name, value)| (HeaderName::from_static(name), *value))
            .collect();
        let context = ErrorContext::ordinary(with_headers(code, &named)).expect("an admitted refusal");
        resolve(context, ResponseKind::Other)
    }

    const AT: &str = "Tue, 14 Nov 2023 22:13:20 GMT";
    const VERSION: &str = "3f2c1a4e-0b7d-4c9e-8a61-5d2f7e9b1c03";

    #[test]
    fn a_not_modified_states_its_tag_and_instant_without_a_document() {
        let answer = resolved(S3ErrorCode::NotModified, &[("etag", "\"abc\""), ("last-modified", AT)]);

        assert_eq!(answer.status(), StatusCode::NOT_MODIFIED);
        assert_eq!(answer.etag().map(|etag| etag.opaque_tag()), Some("abc"));
        let at = HttpDate::from_unix_seconds(1_700_000_000).expect("an HTTP date");
        assert_eq!(answer.headers(), [ErrorHeader::LastModified { at }]);
        assert_eq!(answer.message(), None);
    }

    #[test]
    fn a_not_modified_without_facts_states_none() {
        let answer = resolved(S3ErrorCode::NotModified, &[]);

        assert_eq!(answer.status(), StatusCode::NOT_MODIFIED);
        assert_eq!(answer.etag(), None);
        assert!(answer.headers().is_empty());
    }

    #[test]
    fn a_not_modified_on_a_versioned_object_repeats_its_stored_headers_and_evaluated_version() {
        let answer = resolved(
            S3ErrorCode::NotModified,
            &[
                ("etag", "\"abc\""),
                ("last-modified", AT),
                ("cache-control", "max-age=60"),
                ("content-location", "/bucket/key"),
                ("expires", "Wed, 15 Nov 2023 22:13:20 GMT"),
                ("vary", "Origin"),
                ("x-amz-version-id", VERSION),
            ],
        );

        let rendered: Vec<(HeaderName, String)> = answer
            .headers()
            .iter()
            .map(|header| (header.name(), header.value()))
            .collect();
        for (name, value) in [
            ("cache-control", "max-age=60"),
            ("content-location", "/bucket/key"),
            ("expires", "Wed, 15 Nov 2023 22:13:20 GMT"),
            ("vary", "Origin"),
            ("x-amz-version-id", VERSION),
            ("last-modified", AT),
        ] {
            assert!(
                rendered.contains(&(HeaderName::from_static(name), value.to_owned())),
                "{name} in {rendered:?}"
            );
        }
        assert!(
            !rendered.iter().any(|(name, _)| name == "x-amz-delete-marker"),
            "an evaluated version is not a delete marker"
        );
        assert_eq!(answer.status(), StatusCode::NOT_MODIFIED);
    }

    #[test]
    fn an_unsatisfied_range_states_its_complete_length_only_when_raised_with_it() {
        let stated = resolved(S3ErrorCode::InvalidRange, &[("content-range", "bytes */42")]);
        let bare = resolved(S3ErrorCode::InvalidRange, &[]);

        assert_eq!(stated.status(), StatusCode::RANGE_NOT_SATISFIABLE);
        assert_eq!(stated.headers(), [ErrorHeader::UnsatisfiedRange { complete_length: 42 }]);
        assert_eq!(bare.status(), StatusCode::RANGE_NOT_SATISFIABLE);
        assert!(bare.headers().is_empty(), "no Content-Range RustFS did not write");
    }

    #[test]
    fn a_delete_marker_states_its_flag_and_version_and_its_instant_when_raised_with_it() {
        let current = resolved(
            S3ErrorCode::NoSuchKey,
            &[
                ("x-amz-delete-marker", "true"),
                ("x-amz-version-id", VERSION),
                ("content-type", "application/xml"),
            ],
        );
        let named = resolved(
            S3ErrorCode::MethodNotAllowed,
            &[
                ("x-amz-delete-marker", "true"),
                ("x-amz-version-id", VERSION),
                ("last-modified", AT),
            ],
        );

        let version_id = VersionIdLabel::new(VERSION).expect("a version label");
        assert_eq!(current.status(), StatusCode::NOT_FOUND);
        assert_eq!(
            current.headers(),
            [
                ErrorHeader::DeleteMarker,
                ErrorHeader::VersionId {
                    version_id: version_id.clone()
                }
            ]
        );
        assert_eq!(named.status(), StatusCode::METHOD_NOT_ALLOWED);
        let at = HttpDate::from_unix_seconds(1_700_000_000).expect("an HTTP date");
        assert_eq!(
            named.headers(),
            [
                ErrorHeader::DeleteMarker,
                ErrorHeader::VersionId { version_id },
                ErrorHeader::LastModified { at }
            ]
        );
    }

    /// What the gateway answers for `error`, without a header.
    fn answered(error: S3Error) -> Result<ErrorResolution, InvalidErrorContext> {
        ErrorContext::ordinary(handler_error(error)).map(|context| resolve(context, ResponseKind::Other))
    }

    /// `code`, spelled as RustFS raises it: a named code, or a custom one with its status.
    fn raised(name: &'static str, status: StatusCode) -> S3Error {
        let code = S3ErrorCode::NAMED
            .iter()
            .find(|code| code.as_str() == name)
            .cloned()
            .unwrap_or(S3ErrorCode::Custom(name.into()));
        let mut error = S3Error::with_message(code, "refused");
        error.set_status_code(status);
        error
    }

    #[test]
    fn every_code_the_gateway_answers_only_from_a_context_keeps_its_code_status_and_message() {
        for (name, status) in [
            ("NoSuchKey", StatusCode::NOT_FOUND),
            ("NoSuchVersion", StatusCode::NOT_FOUND),
            ("NoSuchBucket", StatusCode::NOT_FOUND),
            ("AuthorizationHeaderMalformed", StatusCode::BAD_REQUEST),
            ("MethodNotAllowed", StatusCode::METHOD_NOT_ALLOWED),
            ("BucketAlreadyOwnedByYou", StatusCode::CONFLICT),
            ("AccessForbidden", StatusCode::FORBIDDEN),
        ] {
            let answer = answered(raised(name, status)).expect(name);

            assert_eq!(answer.status(), status, "{name}");
            assert_eq!(answer.code().map(ErrorCode::as_str), Some(name), "{name}");
            assert_eq!(answer.message(), Some("refused"), "{name}");
            assert!(answer.headers().is_empty(), "{name}");
        }
    }

    #[test]
    fn a_missing_key_without_a_message_is_answered_without_one() {
        let answer = answered(S3Error::new(S3ErrorCode::NoSuchKey)).expect("admitted");

        assert_eq!(answer.status(), StatusCode::NOT_FOUND);
        assert_eq!(answer.code().map(ErrorCode::as_str), Some("NoSuchKey"));
        assert_eq!(answer.message(), None);
    }

    #[test]
    fn a_redirect_is_a_server_fault_because_the_legacy_refusal_cannot_state_one() {
        for (name, status) in [
            ("PermanentRedirect", StatusCode::MOVED_PERMANENTLY),
            ("TemporaryRedirect", StatusCode::TEMPORARY_REDIRECT),
        ] {
            let mapped = handler_error(raised(name, status));

            assert_eq!(mapped.code(), &ErrorCode::INTERNAL_ERROR, "{name}");
            assert_eq!(mapped.message(), UNSTATABLE_REFUSAL, "{name}");
        }
    }

    /// The list is the gateway's own: every code it refuses without a context is in it, and every
    /// code in it but `InvalidRange` is one the gateway refuses without a context.
    #[test]
    fn the_legacy_shaped_codes_are_exactly_the_codes_the_gateway_requires_a_context_for() {
        let requires_context = |name: &str, status: StatusCode| {
            let bare = HandlerError::new(ErrorCode::custom(name.to_owned(), status), "refused");
            matches!(ErrorContext::ordinary(bare), Err(InvalidErrorContext::ContextRequired))
        };
        for code in S3ErrorCode::NAMED {
            let Some(status) = code.status_code() else { continue };
            let name = code.as_str();
            assert_eq!(
                requires_context(name, status),
                LEGACY_SHAPED.contains(&name) && name != "InvalidRange",
                "{name}"
            );
        }
        for name in LEGACY_SHAPED.into_iter().filter(|name| *name != "InvalidRange") {
            let status = ErrorCode::known(name).expect(name).default_status();
            assert!(requires_context(name, status), "{name}");
        }
    }

    #[test]
    fn a_missing_key_is_hidden_from_a_caller_the_gateway_does_not_let_see_it() {
        let hidden = with_headers(S3ErrorCode::NoSuchKey, &[]).hide_missing_object();

        assert_eq!(hidden.code().as_str(), "AccessDenied");
    }

    #[test]
    fn a_fact_its_code_may_not_state_fails_the_whole_error() {
        for (code, name, value) in [
            (S3ErrorCode::NoSuchKey, "etag", "\"abc\""),
            (S3ErrorCode::NoSuchKey, "last-modified", AT),
            (S3ErrorCode::NotModified, "content-range", "bytes */42"),
            (S3ErrorCode::NoSuchKey, "cache-control", "no-cache"),
            (S3ErrorCode::InvalidRange, "x-amz-version-id", VERSION),
            (S3ErrorCode::InvalidRange, "etag", "\"abc\""),
            (S3ErrorCode::NoSuchBucket, "x-amz-delete-marker", "true"),
            (S3ErrorCode::NoSuchVersion, "x-amz-delete-marker", "true"),
        ] {
            let mapped = with_headers(code.clone(), &[(HeaderName::from_static(name), value)]);

            assert_eq!(mapped.code(), &ErrorCode::INTERNAL_ERROR, "{code:?} {name}");
            assert_eq!(mapped.message(), UNWRITABLE_HEADER, "{code:?} {name}");
        }
    }

    #[test]
    fn an_unreadable_or_incomplete_fact_fails_the_whole_error() {
        let cases: [(S3ErrorCode, &[(&'static str, &'static str)]); 7] = [
            (S3ErrorCode::NotModified, &[("etag", "*")]),
            (S3ErrorCode::NotModified, &[("last-modified", "yesterday")]),
            (S3ErrorCode::InvalidRange, &[("content-range", "bytes 0-1/42")]),
            (S3ErrorCode::InvalidRange, &[("content-range", "bytes */-1")]),
            (S3ErrorCode::NoSuchKey, &[("x-amz-delete-marker", "false"), ("x-amz-version-id", VERSION)]),
            (S3ErrorCode::NoSuchKey, &[("x-amz-delete-marker", "true")]),
            (S3ErrorCode::NotModified, &[("etag", "\"abc\""), ("etag", "\"def\"")]),
        ];
        for (code, headers) in cases {
            let named: Vec<(HeaderName, &'static str)> = headers
                .iter()
                .map(|(name, value)| (HeaderName::from_static(name), *value))
                .collect();
            let mapped = with_headers(code.clone(), &named);

            assert_eq!(mapped.code(), &ErrorCode::INTERNAL_ERROR, "{code:?} {headers:?}");
        }
    }
}
