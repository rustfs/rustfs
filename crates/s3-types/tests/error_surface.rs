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

//! Public-surface tests for the S3 error types: the three macro call forms, the
//! `S3Error` accessor contract, and `S3ErrorCode` parsing and status lookup.
//! Not responsible for: s3s parity, which `src/compat_s3s.rs` owns.
//! Upstream: the crate's public API only. Downstream: none.

use http::{HeaderMap, HeaderValue, StatusCode};
// Imported under an alias on purpose: the s3s footprint ratchet still counts
// `s3_error` invocation lines as s3s usage, and tests of the RustFS-owned macro
// must not move that counter.
use rustfs_s3_types::s3_error as s3err;
use rustfs_s3_types::{S3Error, S3ErrorCode, S3Result, StdError};
use std::convert::Infallible;
use std::io;

const CODES: &str = include_str!("../src/codes.txt");

fn listed_names() -> Vec<&'static str> {
    CODES
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#') && *line != "Custom")
        .collect()
}

fn cause_text(err: &S3Error) -> Option<String> {
    err.source().map(ToString::to_string)
}

// ---- the three macro call forms ----

#[test]
fn macro_code_only_form() {
    let err = s3err!(NoSuchKey);
    assert_eq!(err.code(), &S3ErrorCode::NoSuchKey);
    assert_eq!(err.message(), None);
    assert_eq!(err.status_code(), Some(StatusCode::NOT_FOUND));
}

#[test]
fn macro_literal_message_form() {
    let err = s3err!(InvalidRequest, "bucket name is empty");
    assert_eq!(err.code(), &S3ErrorCode::InvalidRequest);
    assert_eq!(err.message(), Some("bucket name is empty"));
}

#[test]
fn macro_format_form_with_positional_and_captured_arguments() {
    let bucket = "b1";
    let err = s3err!(InvalidArgument, "bucket {bucket} part {}", 7);
    assert_eq!(err.code(), &S3ErrorCode::InvalidArgument);
    assert_eq!(err.message(), Some("bucket b1 part 7"));
}

// The multi-line call shape is the subject of this test; rustfmt would fold it.
#[rustfmt::skip]
#[test]
fn macro_multi_line_and_trailing_comma_forms_match_the_single_line_form() {
    let multi = s3err!(
        InternalError,
        "disk {} offline",
        3,
    );
    let single = s3err!(InternalError, "disk {} offline", 3);
    assert_eq!(multi.code(), single.code());
    assert_eq!(multi.message(), single.message());
    let bare = s3err!(InternalError,);
    assert_eq!(bare.message(), None);
}

#[test]
fn macro_literal_form_does_not_treat_braces_as_a_message_to_format_later() {
    let err = s3err!(InvalidRequest, "{{literal braces}}");
    assert_eq!(err.message(), Some("{literal braces}"));
}

#[test]
fn macro_errors_propagate_with_the_question_mark_operator() {
    fn inner() -> S3Result<()> {
        Err(s3err!(AccessDenied))
    }
    fn outer() -> S3Result<u8> {
        inner()?;
        Ok(1)
    }
    assert_eq!(outer().unwrap_err().code(), &S3ErrorCode::AccessDenied);
}

// ---- S3Error contract ----

#[test]
fn new_sets_nothing_but_the_code() {
    let err = S3Error::new(S3ErrorCode::NoSuchBucket);
    assert_eq!(err.code(), &S3ErrorCode::NoSuchBucket);
    assert_eq!(err.message(), None);
    assert_eq!(err.request_id(), None);
    assert!(err.source().is_none());
    assert!(err.headers().is_none());
}

#[test]
fn status_code_falls_back_to_the_code_default() {
    assert_eq!(S3Error::new(S3ErrorCode::NoSuchKey).status_code(), Some(StatusCode::NOT_FOUND));
    assert_eq!(
        S3Error::new(S3ErrorCode::InternalError).status_code(),
        Some(StatusCode::INTERNAL_SERVER_ERROR)
    );
}

#[test]
fn status_code_prefers_an_explicit_status_over_the_code_default() {
    let mut err = S3Error::new(S3ErrorCode::NoSuchKey);
    err.set_status_code(StatusCode::FORBIDDEN);
    assert_eq!(err.status_code(), Some(StatusCode::FORBIDDEN));
}

#[test]
fn custom_code_without_an_explicit_status_has_no_status() {
    let err = S3Error::new(S3ErrorCode::Custom("TierNotFound".into()));
    assert_eq!(err.status_code(), None);
}

#[test]
fn set_code_moves_the_default_status_with_the_code() {
    let mut err = S3Error::new(S3ErrorCode::NoSuchKey);
    err.set_code(S3ErrorCode::ServiceUnavailable);
    assert_eq!(err.code(), &S3ErrorCode::ServiceUnavailable);
    assert_eq!(err.status_code(), Some(StatusCode::SERVICE_UNAVAILABLE));
    err.set_code(S3ErrorCode::Custom("X".into()));
    assert_eq!(err.status_code(), None);
}

#[test]
fn set_code_does_not_disturb_an_explicit_status() {
    let mut err = S3Error::new(S3ErrorCode::NoSuchKey);
    err.set_status_code(StatusCode::GONE);
    err.set_code(S3ErrorCode::InternalError);
    assert_eq!(err.status_code(), Some(StatusCode::GONE));
}

#[test]
fn with_message_accepts_static_and_owned_strings() {
    let fixed = S3Error::with_message(S3ErrorCode::InvalidPart, "static");
    let owned = S3Error::with_message(S3ErrorCode::InvalidPart, String::from("owned"));
    assert_eq!(fixed.message(), Some("static"));
    assert_eq!(owned.message(), Some("owned"));
}

#[test]
fn set_message_replaces_rather_than_appends() {
    let mut err = S3Error::with_message(S3ErrorCode::InvalidPart, "first");
    err.set_message("second");
    assert_eq!(err.message(), Some("second"));
}

#[test]
fn with_source_and_internal_error_expose_the_cause_both_ways() {
    let err = S3Error::internal_error(io::Error::other("disk gone"));
    assert_eq!(err.code(), &S3ErrorCode::InternalError);
    assert_eq!(cause_text(&err).as_deref(), Some("disk gone"));
    assert_eq!(std::error::Error::source(&err).map(ToString::to_string).as_deref(), Some("disk gone"));

    let boxed: StdError = Box::new(io::Error::other("link reset"));
    let err = S3Error::with_source(S3ErrorCode::ServiceUnavailable, boxed);
    assert_eq!(err.code(), &S3ErrorCode::ServiceUnavailable);
    assert_eq!(err.message(), None);
    assert_eq!(cause_text(&err).as_deref(), Some("link reset"));
}

#[test]
fn set_source_replaces_the_previous_cause() {
    let mut err = S3Error::internal_error(io::Error::other("first"));
    err.set_source(Box::new(io::Error::other("second")));
    assert_eq!(cause_text(&err).as_deref(), Some("second"));
}

#[test]
fn headers_and_request_id_are_absent_until_set() {
    let mut err = S3Error::new(S3ErrorCode::PreconditionFailed);
    assert!(err.headers().is_none());
    assert_eq!(err.request_id(), None);
    let mut headers = HeaderMap::new();
    headers.insert("x-amz-expiration", HeaderValue::from_static("soon"));
    err.set_headers(headers);
    err.set_request_id("req-1");
    assert_eq!(err.headers().map(|h| h["x-amz-expiration"].as_bytes()), Some(&b"soon"[..]));
    assert_eq!(err.request_id(), Some("req-1"));
}

#[test]
fn into_parts_hands_back_every_field_owned() {
    let mut err = S3Error::with_message(S3ErrorCode::InvalidRange, "bad range");
    err.set_request_id("req-9");
    err.set_status_code(StatusCode::RANGE_NOT_SATISFIABLE);
    err.set_source(Box::new(io::Error::other("cause")));
    let mut headers = HeaderMap::new();
    headers.insert("content-range", HeaderValue::from_static("bytes */10"));
    err.set_headers(headers);

    let parts = err.into_parts();
    assert_eq!(parts.code, S3ErrorCode::InvalidRange);
    assert_eq!(parts.message.as_deref(), Some("bad range"));
    assert_eq!(parts.request_id.as_deref(), Some("req-9"));
    assert_eq!(parts.status, Some(StatusCode::RANGE_NOT_SATISFIABLE));
    assert_eq!(parts.source.map(|s| s.to_string()).as_deref(), Some("cause"));
    assert_eq!(parts.headers.map(|h| h.len()), Some(1));
}

#[test]
fn into_parts_reports_the_explicit_status_only() {
    let parts = S3Error::new(S3ErrorCode::NoSuchKey).into_parts();
    assert_eq!(parts.status, None, "the code default is not an explicit status");
    assert_eq!(parts.message, None);
    assert_eq!(parts.request_id, None);
    assert!(parts.source.is_none());
    assert!(parts.headers.is_none());
}

#[test]
fn display_names_the_code_and_the_present_fields_only() {
    let err = S3Error::with_message(S3ErrorCode::NoSuchKey, "gone");
    let text = err.to_string();
    assert!(text.contains("NoSuchKey") && text.contains("gone"), "{text}");
    let bare = S3Error::new(S3ErrorCode::NoSuchKey).to_string();
    assert!(bare.contains("NoSuchKey"), "{bare}");
    assert!(!bare.contains("message") && !bare.contains("request_id"), "{bare}");
}

#[test]
fn from_code_and_from_infallible_conversions_exist() {
    let err: S3Error = S3ErrorCode::SlowDown.into();
    assert_eq!(err.code(), &S3ErrorCode::SlowDown);
    fn never(value: Result<(), Infallible>) -> S3Result<()> {
        value?;
        Ok(())
    }
    assert!(never(Ok(())).is_ok());
}

#[test]
fn s3_error_is_send_sync_and_keeps_results_pointer_sized() {
    fn assert_send_sync<T: Send + Sync + 'static>() {}
    assert_send_sync::<S3Error>();
    assert_eq!(std::mem::size_of::<S3Result<()>>(), std::mem::size_of::<usize>());
}

// ---- S3ErrorCode: generated table, statuses, parsing ----

#[test]
fn named_table_matches_the_code_list_exactly() {
    let listed = listed_names();
    let generated: Vec<&str> = S3ErrorCode::NAMED.iter().map(S3ErrorCode::as_str).collect();
    assert_eq!(generated, listed);
    assert!(!S3ErrorCode::NAMED.iter().any(|code| matches!(code, S3ErrorCode::Custom(_))));
}

#[test]
fn code_list_is_byte_sorted_and_unique() {
    let listed = listed_names();
    let mut sorted = listed.clone();
    sorted.sort_unstable();
    sorted.dedup();
    assert_eq!(listed, sorted);
}

#[test]
fn every_named_code_has_an_error_or_not_modified_status() {
    for code in S3ErrorCode::NAMED {
        let status = code.status_code().unwrap_or_else(|| panic!("{code} has no status"));
        assert!(
            status == StatusCode::NOT_MODIFIED || status.is_client_error() || status.is_server_error(),
            "{code}: {status}"
        );
    }
}

#[test]
fn as_str_and_display_are_the_wire_name() {
    assert_eq!(S3ErrorCode::NoSuchKey.as_str(), "NoSuchKey");
    assert_eq!(S3ErrorCode::NoSuchKey.to_string(), "NoSuchKey");
    assert_eq!(S3ErrorCode::Custom("TierNotFound".into()).as_str(), "TierNotFound");
    assert_eq!(S3ErrorCode::Custom("TierNotFound".into()).to_string(), "TierNotFound");
}

#[test]
fn parsing_recovers_every_named_code_from_its_exact_name() {
    for code in S3ErrorCode::NAMED {
        assert_eq!(S3ErrorCode::from_bytes(code.as_str().as_bytes()).as_ref(), Some(code));
        assert_eq!(code.as_str().parse::<S3ErrorCode>().as_ref(), Ok(code));
    }
}

#[test]
fn parsing_is_ascii_case_insensitive() {
    assert_eq!(S3ErrorCode::from_bytes(b"nosuchkey"), Some(S3ErrorCode::NoSuchKey));
    assert_eq!(S3ErrorCode::from_bytes(b"NOSUCHKEY"), Some(S3ErrorCode::NoSuchKey));
    assert_eq!("noSuchKEY".parse(), Ok(S3ErrorCode::NoSuchKey));
}

#[test]
fn parsing_rejects_invalid_utf8() {
    assert_eq!(S3ErrorCode::from_bytes(b"NoSuch\xffKey"), None);
    assert_eq!(S3ErrorCode::from_bytes(&[0xC3]), None);
}

#[test]
fn parsing_never_promotes_an_unknown_name_to_a_named_variant() {
    for input in [
        "NoSuchKeyX",
        "NoSuchKe",
        " NoSuchKey",
        "NoSuchKey ",
        "No Such Key",
        "TierNotFound",
        "",
    ] {
        let parsed = S3ErrorCode::from_bytes(input.as_bytes()).unwrap_or_else(|| panic!("{input:?} is valid UTF-8"));
        assert_eq!(parsed, S3ErrorCode::Custom(input.to_owned().into()), "{input:?}");
        assert_eq!(input.parse::<S3ErrorCode>(), Ok(parsed), "{input:?}");
    }
}

#[test]
fn parsing_keeps_the_original_spelling_of_an_unknown_name() {
    assert_eq!(
        S3ErrorCode::from_bytes(b"tiernotfound")
            .map(|c| c.as_str().to_owned())
            .as_deref(),
        Some("tiernotfound")
    );
}

#[test]
fn a_custom_code_never_equals_the_named_code_with_the_same_text() {
    let custom = S3ErrorCode::Custom("NoSuchKey".into());
    assert_ne!(custom, S3ErrorCode::NoSuchKey);
    assert_eq!(custom.as_str(), S3ErrorCode::NoSuchKey.as_str());
    assert_eq!(custom.status_code(), None);
}

#[test]
fn custom_codes_compare_by_text_not_by_ownership() {
    assert_eq!(S3ErrorCode::Custom("X".into()), S3ErrorCode::Custom(String::from("X").into()));
    assert_ne!(S3ErrorCode::Custom("X".into()), S3ErrorCode::Custom("x".into()));
}
