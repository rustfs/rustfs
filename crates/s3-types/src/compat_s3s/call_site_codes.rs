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

//! Parity of every `S3ErrorCode` variant a call site moved off s3s names.
//!
//! Responsible for: the table of variant names used by the call sites that the
//! T1.2 codemod (`scripts/codemods/s3_error_to_s3_types.py`,
//! rustfs/backlog#2743) moved onto this crate, and for each of them the wire
//! code string, the HTTP status, and the s3s error the legacy edge renders for
//! a bare code (the macro given only a code, or `S3Error::new`), all compared
//! with the pinned s3s.
//! Not responsible for: the full code set (`every_code_matches_the_s3s_status`
//! in the parent module covers every named code), parsing, or conversions of
//! messages, causes and headers (the parent module's round-trip tests).
//! Upstream: `crate::compat_s3s` and `legacy_s3s` as the oracle. Downstream:
//! none; a batch that moves more call sites adds their variant names here.

use crate::{S3Error, S3ErrorCode};

/// Builds one row per variant: its name, our code and the s3s code, both
/// spelled as a path so a variant missing on either side fails to compile.
macro_rules! rows {
    ($($code:ident),+ $(,)?) => {
        [$((stringify!($code), S3ErrorCode::$code, legacy_s3s::S3ErrorCode::$code)),+]
    };
}

#[test]
fn every_variant_a_moved_call_site_names_keeps_its_s3s_code_status_and_message() {
    let rows = rows![
        // Batch 1, crates/ecstore.
        InvalidRange,
        // Batch 3, crates/s3select-api and crates/protocols.
        AccessDenied,
        InternalError,
    ];
    let mut mismatches = Vec::new();
    for (name, ours, theirs) in rows {
        if ours.as_str() != name || theirs.as_str() != name {
            mismatches.push(format!("{name}: ours spells {}, s3s spells {}", ours.as_str(), theirs.as_str()));
        }
        if ours.status_code().is_none() || ours.status_code() != theirs.status_code() {
            mismatches.push(format!("{name}: ours {:?}, s3s {:?}", ours.status_code(), theirs.status_code()));
        }
        // The legacy edge converts with `From`; a bare code must come out as
        // the error s3s itself builds for that code, default message included.
        let crossed = legacy_s3s::S3Error::from(S3Error::new(ours));
        let native = legacy_s3s::S3Error::new(theirs);
        if crossed.code() != native.code()
            || crossed.status_code() != native.status_code()
            || crossed.message() != native.message()
        {
            mismatches.push(format!(
                "{name}: crossed ({:?}, {:?}, {:?}), s3s ({:?}, {:?}, {:?})",
                crossed.code(),
                crossed.status_code(),
                crossed.message(),
                native.code(),
                native.status_code(),
                native.message()
            ));
        }
    }
    assert!(mismatches.is_empty(), "{} mismatches:\n{}", mismatches.len(), mismatches.join("\n"));
}
