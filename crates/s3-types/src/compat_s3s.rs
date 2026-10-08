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

//! The legacy edge between RustFS's S3 error and body types and the s3s ones.
//!
//! Responsible for: the `From` conversions in both directions (errors field by
//! field; bodies frame by frame, keeping errors and the size hint), and the
//! parity tests that pin this crate's statuses, parsing and body length
//! reporting to the pinned s3s revision, which the tests use as an oracle.
//! Not responsible for: anything the s3s-facing `impl S3` does with the result,
//! the default messages s3s attaches to a bare code (restored by s3s's own
//! constructor on the way out, never stored here), or read limits: a limit
//! armed on an s3s body (`set_limit`) keeps applying inside the wrapped body,
//! and our `Body` carries none, so none is armed on the way back.
//! Upstream: `legacy_s3s`, the s3s package under the name the edge knows it
//! by. Downstream: `rustfs` with the `compat-s3s` feature.
//! DELETE BY T4.2 (rustfs/backlog#2784).
//!
//! Footprint note: `scripts/check_s3s_footprint.sh` counts files whose Rust
//! paths name the s3s crate directly. This file reaches s3s through the
//! renamed dependency, so that counter does not see it. The rename is stated here and
//! in the PR rather than hidden; the edge whitelist of T0.6
//! (rustfs/backlog#2741) is expected to list this file explicitly.

use crate::{Body, S3Error, S3ErrorCode};
use std::borrow::Cow;
use std::fmt;

// Bodies cross the legacy edge as opaque frame streams: nothing is buffered,
// and errors and the size hint pass through untouched. DELETE BY T4.2
// (rustfs/backlog#2784) with the rest of this file.
impl From<legacy_s3s::Body> for Body {
    fn from(body: legacy_s3s::Body) -> Self {
        Self::from_http_body(body)
    }
}

impl From<Body> for legacy_s3s::Body {
    fn from(body: Body) -> Self {
        Self::http_body(body)
    }
}

impl From<S3ErrorCode> for legacy_s3s::S3ErrorCode {
    fn from(code: S3ErrorCode) -> Self {
        match code {
            S3ErrorCode::Custom(Cow::Borrowed(text)) => Self::Custom(text.into()),
            S3ErrorCode::Custom(Cow::Owned(text)) => Self::Custom(text.into()),
            // s3s parses every valid UTF-8 name, as its own named variant when it
            // knows the name and as `Custom` otherwise, so a code s3s lacks still
            // reaches the wire verbatim. Today every name is known to s3s; the
            // oracle test pins that.
            named => {
                let Ok(code) = named.as_str().parse::<Self>();
                code
            }
        }
    }
}

impl From<legacy_s3s::S3ErrorCode> for S3ErrorCode {
    fn from(code: legacy_s3s::S3ErrorCode) -> Self {
        match code {
            legacy_s3s::S3ErrorCode::Custom(text) => Self::Custom(Cow::Owned(String::from(&*text))),
            named => {
                let Ok(code) = named.as_str().parse::<Self>();
                code
            }
        }
    }
}

impl From<S3Error> for legacy_s3s::S3Error {
    fn from(error: S3Error) -> Self {
        let parts = error.into_parts();
        let mut out = Self::new(parts.code.into());
        if let Some(message) = parts.message {
            out.set_message(message);
        }
        if let Some(request_id) = parts.request_id {
            out.set_request_id(request_id);
        }
        if let Some(status) = parts.status {
            out.set_status_code(status);
        }
        if let Some(source) = parts.source {
            out.set_source(source);
        }
        if let Some(headers) = parts.headers {
            out.set_headers(headers);
        }
        out
    }
}

impl From<legacy_s3s::S3Error> for S3Error {
    fn from(error: legacy_s3s::S3Error) -> Self {
        let code: S3ErrorCode = error.code().clone().into();
        let mut out = Self::new(code.clone());
        if let Some(message) = error.message() {
            out.set_message(message.to_owned());
        }
        if let Some(request_id) = error.request_id() {
            out.set_request_id(request_id);
        }
        // s3s folds the code's own status into `status_code()`, so only a
        // status that differs from the code's own was set explicitly.
        if let Some(status) = error.status_code()
            && Some(status) != code.status_code()
        {
            out.set_status_code(status);
        }
        if let Some(headers) = error.headers() {
            out.set_headers(headers.clone());
        }
        if error.source().is_some() {
            out.set_source(Box::new(LegacyCause(error)));
        }
        out
    }
}

/// Carries an s3s error's cause across the edge. s3s lends its cause by
/// reference only, so the whole error travels inside and this wrapper presents
/// the cause's own text and chain, as if the cause itself had been moved.
#[derive(Debug)]
struct LegacyCause(legacy_s3s::S3Error);

impl LegacyCause {
    fn cause(&self) -> Option<&(dyn std::error::Error + Send + Sync + 'static)> {
        self.0.source()
    }
}

impl fmt::Display for LegacyCause {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.cause() {
            Some(cause) => fmt::Display::fmt(cause, f),
            None => fmt::Display::fmt(&self.0, f),
        }
    }
}

impl std::error::Error for LegacyCause {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.cause().and_then(std::error::Error::source)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::{HeaderMap, HeaderValue, StatusCode};
    use std::io;

    fn oracle(name: &str) -> legacy_s3s::S3ErrorCode {
        let Ok(code) = name.parse();
        code
    }

    fn is_custom(code: &legacy_s3s::S3ErrorCode) -> bool {
        matches!(code, legacy_s3s::S3ErrorCode::Custom(_))
    }

    fn full_error() -> S3Error {
        let mut error = S3Error::with_message(S3ErrorCode::NoSuchKey, "object is gone");
        error.set_request_id("req-42");
        error.set_status_code(StatusCode::FORBIDDEN);
        error.set_source(Box::new(io::Error::other("disk cause")));
        let mut headers = HeaderMap::new();
        headers.insert("x-amz-delete-marker", HeaderValue::from_static("true"));
        headers.insert("x-amz-version-id", HeaderValue::from_static("v1"));
        error.set_headers(headers);
        error
    }

    // ---- oracle: statuses and parsing ----

    #[test]
    fn every_code_matches_the_s3s_status() {
        let mut mismatches = Vec::new();
        for code in S3ErrorCode::NAMED {
            let name = code.as_str();
            let legacy = oracle(name);
            if is_custom(&legacy) {
                mismatches.push(format!("{name}: unknown to s3s"));
                continue;
            }
            if legacy.as_str() != name {
                mismatches.push(format!("{name}: s3s spells it {}", legacy.as_str()));
            }
            if legacy.status_code() != code.status_code() {
                mismatches.push(format!("{name}: s3s {:?}, ours {:?}", legacy.status_code(), code.status_code()));
            }
        }
        assert!(mismatches.is_empty(), "{} mismatches:\n{}", mismatches.len(), mismatches.join("\n"));
        assert_eq!(oracle("TierNotFound").status_code(), None);
        assert_eq!(S3ErrorCode::Custom("TierNotFound".into()).status_code(), None);
    }

    #[test]
    fn parsing_matches_s3s_for_exact_case_folded_unknown_and_invalid_input() {
        let mut inputs: Vec<Vec<u8>> = Vec::new();
        for code in S3ErrorCode::NAMED {
            let name = code.as_str();
            inputs.push(name.into());
            inputs.push(name.to_ascii_lowercase().into());
            inputs.push(name.to_ascii_uppercase().into());
        }
        for unknown in ["TierNotFound", "", "NoSuchKeyX", " NoSuchKey", "nosuchkey "] {
            inputs.push(unknown.into());
        }
        inputs.push(b"NoSuch\xffKey".to_vec());
        for input in &inputs {
            let ours = S3ErrorCode::from_bytes(input);
            let theirs = legacy_s3s::S3ErrorCode::from_bytes(input);
            assert_eq!(
                ours.as_ref().map(S3ErrorCode::as_str),
                theirs.as_ref().map(legacy_s3s::S3ErrorCode::as_str),
                "{}",
                String::from_utf8_lossy(input)
            );
            assert_eq!(
                ours.as_ref().map(|code| matches!(code, S3ErrorCode::Custom(_))),
                theirs.as_ref().map(is_custom),
                "{}",
                String::from_utf8_lossy(input)
            );
        }
        assert_eq!(S3ErrorCode::from_bytes(b"NoSuch\xffKey"), None);
    }

    // ---- codes across the edge ----

    #[test]
    fn named_codes_round_trip_as_named_s3s_codes() {
        for code in S3ErrorCode::NAMED {
            let legacy: legacy_s3s::S3ErrorCode = code.clone().into();
            assert!(!is_custom(&legacy), "{code} crossed the edge as Custom");
            assert_eq!(legacy.as_str(), code.as_str());
            let back: S3ErrorCode = legacy.into();
            assert_eq!(&back, code);
        }
    }

    #[test]
    fn custom_code_round_trips_verbatim() {
        for code in [
            S3ErrorCode::Custom("TierNotFound".into()),
            S3ErrorCode::Custom(String::from("XMinioAdminTierNotFound").into()),
        ] {
            let legacy: legacy_s3s::S3ErrorCode = code.clone().into();
            assert!(is_custom(&legacy), "{code}");
            assert_eq!(legacy.as_str(), code.as_str());
            let back: S3ErrorCode = legacy.into();
            assert_eq!(back, code);
        }
    }

    #[test]
    fn a_custom_code_spelled_like_a_named_one_stays_custom_in_both_directions() {
        let code = S3ErrorCode::Custom("NoSuchKey".into());
        let legacy: legacy_s3s::S3ErrorCode = code.clone().into();
        assert!(is_custom(&legacy));
        assert_eq!(legacy.status_code(), None);
        let back: S3ErrorCode = legacy.into();
        assert_eq!(back, code);
        assert_ne!(back, S3ErrorCode::NoSuchKey);

        let legacy = legacy_s3s::S3ErrorCode::Custom("NoSuchKey".into());
        let ours: S3ErrorCode = legacy.into();
        assert_eq!(ours, S3ErrorCode::Custom("NoSuchKey".into()));
    }

    // ---- errors across the edge ----

    #[test]
    fn error_round_trip_preserves_message_request_id_status_headers_and_cause() {
        let legacy: legacy_s3s::S3Error = full_error().into();
        assert_eq!(legacy.code().as_str(), "NoSuchKey");
        assert_eq!(legacy.message(), Some("object is gone"));
        assert_eq!(legacy.request_id(), Some("req-42"));
        assert_eq!(legacy.status_code(), Some(StatusCode::FORBIDDEN));
        assert_eq!(legacy.source().map(ToString::to_string).as_deref(), Some("disk cause"));
        let headers = legacy.headers().expect("headers crossed the edge");
        assert_eq!(headers["x-amz-delete-marker"], "true");
        assert_eq!(headers["x-amz-version-id"], "v1");

        let back: S3Error = legacy.into();
        assert_eq!(back.code(), &S3ErrorCode::NoSuchKey);
        assert_eq!(back.message(), Some("object is gone"));
        assert_eq!(back.request_id(), Some("req-42"));
        assert_eq!(back.status_code(), Some(StatusCode::FORBIDDEN));
        assert_eq!(back.source().map(ToString::to_string).as_deref(), Some("disk cause"));
        let headers = back.headers().expect("headers crossed back");
        assert_eq!(headers.len(), 2);
        assert_eq!(headers["x-amz-delete-marker"], "true");
        assert_eq!(headers["x-amz-version-id"], "v1");
        assert_eq!(back.into_parts().status, Some(StatusCode::FORBIDDEN));
    }

    #[test]
    fn a_bare_code_renders_the_same_message_s3s_gives_a_bare_code_today() {
        for code in S3ErrorCode::NAMED {
            let crossed: legacy_s3s::S3Error = S3Error::new(code.clone()).into();
            let native = legacy_s3s::S3Error::new(code.clone().into());
            assert_eq!(crossed.message(), native.message(), "{code}");
            assert_eq!(crossed.status_code(), native.status_code(), "{code}");
        }
    }

    #[test]
    fn absent_fields_stay_absent_from_ours_to_s3s() {
        let legacy: legacy_s3s::S3Error = S3Error::new(S3ErrorCode::Custom("TierNotFound".into())).into();
        assert_eq!(legacy.message(), None);
        assert_eq!(legacy.request_id(), None);
        assert_eq!(legacy.status_code(), None);
        assert!(legacy.source().is_none());
        assert!(legacy.headers().is_none());
    }

    #[test]
    fn absent_fields_stay_absent_from_s3s_to_ours() {
        let legacy = legacy_s3s::S3Error::new(legacy_s3s::S3ErrorCode::Custom("TierNotFound".into()));
        let ours: S3Error = legacy.into();
        assert_eq!(ours.message(), None);
        assert_eq!(ours.request_id(), None);
        assert_eq!(ours.status_code(), None);
        assert!(ours.source().is_none());
        assert!(ours.headers().is_none());
    }

    #[test]
    fn the_s3s_default_status_does_not_become_an_explicit_status_on_our_side() {
        let legacy = legacy_s3s::S3Error::new(legacy_s3s::S3ErrorCode::NoSuchKey);
        assert_eq!(legacy.status_code(), Some(StatusCode::NOT_FOUND));
        let ours: S3Error = legacy.into();
        assert_eq!(ours.status_code(), Some(StatusCode::NOT_FOUND));
        assert_eq!(ours.into_parts().status, None);
    }

    #[test]
    fn an_explicit_status_on_a_custom_code_survives_both_directions() {
        let mut ours = S3Error::new(S3ErrorCode::Custom("XMinioKmsKeyNotFound".into()));
        ours.set_status_code(StatusCode::BAD_REQUEST);
        let legacy: legacy_s3s::S3Error = ours.into();
        assert_eq!(legacy.status_code(), Some(StatusCode::BAD_REQUEST));
        let back: S3Error = legacy.into();
        assert_eq!(back.into_parts().status, Some(StatusCode::BAD_REQUEST));
    }

    #[test]
    fn an_s3s_error_without_a_cause_converts_without_inventing_one() {
        let mut legacy = legacy_s3s::S3Error::new(legacy_s3s::S3ErrorCode::InternalError);
        legacy.set_message("no cause attached");
        let ours: S3Error = legacy.into();
        assert!(ours.source().is_none());
        assert!(std::error::Error::source(&ours).is_none());
        assert_eq!(ours.message(), Some("no cause attached"));
    }

    #[test]
    fn a_cause_brought_back_from_s3s_keeps_its_text_and_its_chain() {
        let mut legacy = legacy_s3s::S3Error::new(legacy_s3s::S3ErrorCode::InternalError);
        legacy.set_source(Box::new(io::Error::new(io::ErrorKind::TimedOut, "peer timed out")));
        let ours: S3Error = legacy.into();
        let cause = ours.source().expect("cause crossed back");
        assert_eq!(cause.to_string(), "peer timed out");
        assert!(std::error::Error::source(cause).is_none(), "an io::Error has no further cause");
        assert_eq!(
            std::error::Error::source(&ours).map(ToString::to_string).as_deref(),
            Some("peer timed out")
        );
    }

    #[test]
    fn headers_do_not_appear_when_none_were_set() {
        let legacy: legacy_s3s::S3Error = S3Error::with_message(S3ErrorCode::NoSuchKey, "m").into();
        assert!(legacy.headers().is_none());
        let back: S3Error = legacy.into();
        assert!(back.headers().is_none());
    }

    // ---- bodies across the edge ----

    use crate::body::test_support::{Scripted, as_io, drain, io_error};
    use bytes::Bytes;
    use http_body::{Body as _, SizeHint};

    fn hint_of(body: &impl http_body::Body) -> (u64, Option<u64>) {
        let hint = body.size_hint();
        (hint.lower(), hint.upper())
    }

    fn ranged(lower: u64, upper: u64) -> SizeHint {
        let mut hint = SizeHint::new();
        hint.set_lower(lower);
        hint.set_upper(upper);
        hint
    }

    #[test]
    fn s3s_bodies_cross_into_ours_with_their_bytes_and_length() {
        let cases: [(&str, legacy_s3s::Body, &[u8]); 4] = [
            ("empty", legacy_s3s::Body::empty(), b""),
            ("small", legacy_s3s::Body::from(Bytes::from_static(b"hello")), b"hello"),
            (
                "multi-chunk",
                legacy_s3s::Body::http_body(Scripted::chunks(&[b"ab", b"cde", b"f"])),
                b"abcdef",
            ),
            ("empty buffer", legacy_s3s::Body::from(Vec::new()), b""),
        ];
        for (name, legacy, expected) in cases {
            let legacy_hint = hint_of(&legacy);
            let legacy_end = legacy.is_end_stream();
            let ours = Body::from(legacy);
            assert_eq!(hint_of(&ours), legacy_hint, "{name}: size hint");
            assert_eq!(ours.is_end_stream(), legacy_end, "{name}: end of stream");
            let drained = drain(ours);
            assert_eq!(drained.bytes(), expected, "{name}");
            assert!(drained.error.is_none(), "{name}");
        }
    }

    #[test]
    fn our_bodies_cross_into_s3s_with_their_bytes_and_length() {
        let cases: [(&str, Body, &[u8]); 4] = [
            ("empty", Body::empty(), b""),
            ("small", Body::from("hello"), b"hello"),
            ("multi-chunk", Body::from_http_body(Scripted::chunks(&[b"ab", b"cde", b"f"])), b"abcdef"),
            ("empty buffer", Body::from(String::new()), b""),
        ];
        for (name, ours, expected) in cases {
            let our_hint = hint_of(&ours);
            let our_end = ours.is_end_stream();
            let legacy = legacy_s3s::Body::from(ours);
            assert_eq!(hint_of(&legacy), our_hint, "{name}: size hint");
            assert_eq!(legacy.is_end_stream(), our_end, "{name}: end of stream");
            let drained = drain(legacy);
            assert_eq!(drained.bytes(), expected, "{name}");
            assert!(drained.error.is_none(), "{name}");
        }
    }

    #[test]
    fn a_ranged_or_unknown_size_hint_survives_both_directions() {
        for hint in [ranged(2, 9), SizeHint::new()] {
            let expected = (hint.lower(), hint.upper());
            let ours = Body::from(legacy_s3s::Body::http_body(Scripted::new(Vec::new(), hint)));
            assert_eq!(hint_of(&ours), expected, "s3s to ours");
            let legacy = legacy_s3s::Body::from(Body::from_http_body(Scripted::new(Vec::new(), hint)));
            assert_eq!(hint_of(&legacy), expected, "ours to s3s");
        }
    }

    #[test]
    fn an_s3s_body_error_crosses_into_ours_after_the_data_before_it() {
        let legacy = legacy_s3s::Body::http_body(Scripted::new(
            vec![
                Ok(Bytes::from_static(b"head")),
                Err(io_error(io::ErrorKind::TimedOut, "remote stalled")),
            ],
            SizeHint::new(),
        ));
        let drained = drain(Body::from(legacy));
        assert_eq!(drained.bytes(), b"head");
        let error = drained.error.expect("the s3s body error must cross the edge");
        assert_eq!(as_io(&error).map(io::Error::kind), Some(io::ErrorKind::TimedOut));
        assert_eq!(error.to_string(), "remote stalled");
    }

    #[test]
    fn our_body_error_crosses_into_s3s_after_the_data_before_it() {
        let ours = Body::from_http_body(Scripted::new(
            vec![
                Ok(Bytes::from_static(b"head")),
                Err(io_error(io::ErrorKind::BrokenPipe, "local abort")),
            ],
            SizeHint::new(),
        ));
        let drained = drain(legacy_s3s::Body::from(ours));
        assert_eq!(drained.bytes(), b"head");
        let error = drained.error.expect("our body error must cross the edge");
        assert_eq!(as_io(&error).map(io::Error::kind), Some(io::ErrorKind::BrokenPipe));
        assert_eq!(error.to_string(), "local abort");
    }

    #[test]
    fn an_error_before_any_data_crosses_both_directions() {
        let legacy = legacy_s3s::Body::http_body(Scripted::new(
            vec![Err(io_error(io::ErrorKind::ConnectionReset, "reset"))],
            SizeHint::with_exact(8),
        ));
        let drained = drain(Body::from(legacy));
        assert!(drained.chunks.is_empty());
        assert_eq!(
            drained.error.as_ref().and_then(as_io).map(io::Error::kind),
            Some(io::ErrorKind::ConnectionReset)
        );

        let ours = Body::from_http_body(Scripted::new(
            vec![Err(io_error(io::ErrorKind::ConnectionReset, "reset"))],
            SizeHint::with_exact(8),
        ));
        let drained = drain(legacy_s3s::Body::from(ours));
        assert!(drained.chunks.is_empty());
        assert_eq!(
            drained.error.as_ref().and_then(as_io).map(io::Error::kind),
            Some(io::ErrorKind::ConnectionReset)
        );
    }

    #[test]
    fn a_read_limit_armed_on_an_s3s_body_keeps_applying_after_the_crossing() {
        let mut legacy = legacy_s3s::Body::from(Bytes::from_static(b"hello"));
        legacy.set_limit(Some(3));
        let drained = drain(Body::from(legacy));
        assert!(drained.chunks.is_empty(), "no byte past the limit may be yielded");
        let error = drained.error.expect("the s3s limit must still fail the read");
        // s3s recognises this exact type to answer with its size-limit error.
        assert!(error.is::<legacy_s3s::BodySizeLimitExceeded>(), "unexpected error: {error}");
    }

    #[test]
    fn in_memory_constructors_report_length_and_end_of_stream_exactly_as_s3s_does() {
        let inputs: [&[u8]; 3] = [b"", b"x", b"hello world"];
        for input in inputs {
            let pairs = [
                (
                    "Bytes",
                    Body::from(Bytes::copy_from_slice(input)),
                    legacy_s3s::Body::from(Bytes::copy_from_slice(input)),
                ),
                ("Vec<u8>", Body::from(input.to_vec()), legacy_s3s::Body::from(input.to_vec())),
                (
                    "String",
                    Body::from(String::from_utf8(input.to_vec()).expect("ASCII input")),
                    legacy_s3s::Body::from(String::from_utf8(input.to_vec()).expect("ASCII input")),
                ),
            ];
            for (name, ours, legacy) in pairs {
                assert_eq!(hint_of(&ours), hint_of(&legacy), "{name} {input:?}: size hint");
                assert_eq!(ours.is_end_stream(), legacy.is_end_stream(), "{name} {input:?}: end of stream");
                assert_eq!(drain(ours).chunks, drain(legacy).chunks, "{name} {input:?}: frames");
            }
        }
        let (ours, legacy) = (Body::empty(), legacy_s3s::Body::empty());
        assert_eq!(hint_of(&ours), hint_of(&legacy), "empty: size hint");
        assert_eq!(ours.is_end_stream(), legacy.is_end_stream(), "empty: end of stream");
    }
}

#[cfg(test)]
mod call_site_codes;
