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

//! Path manipulation helpers used across the SFTP driver. Pure
//! functions: no driver state, no async, no backend calls.

use super::errors::SftpError;
use russh_sftp::protocol::StatusCode;
use rustfs_utils::path;

/// Prefix the input with "/" if it is empty or relative. SFTP paths are
/// addressed as absolute against the server root. Clients may submit a
/// relative form (e.g. "." or "foo/bar"). Both forms normalize to the
/// same absolute starting point before any cleaning or splitting runs.
pub(super) fn ensure_absolute(path: &str) -> String {
    if path.is_empty() || !path.starts_with('/') {
        format!("/{path}")
    } else {
        path.to_string()
    }
}

/// Return the last path component of a slash-separated string, stripping
/// any trailing slash. Returns None when the input has no usable component
/// (empty input, or a string consisting solely of slashes).
pub(super) fn last_path_component(s: &str) -> Option<&str> {
    let trimmed = s.trim_end_matches('/');
    if trimmed.is_empty() {
        return None;
    }
    Some(trimmed.rsplit('/').next().unwrap_or(trimmed))
}

/// Extract the single filename component of full_key relative to prefix.
/// Returns None when full_key does not start with prefix, when the
/// residual is empty (key equaled prefix exactly), or when the residual
/// contains a slash (entry belongs under a sub-prefix and should have
/// appeared via common_prefixes under delimiter="/").
pub(super) fn relative_filename<'a>(full_key: &'a str, prefix: &str) -> Option<&'a str> {
    let residual = full_key.strip_prefix(prefix)?;
    if residual.is_empty() || residual.contains('/') {
        return None;
    }
    Some(residual)
}

/// Canonicalize an incoming SFTP path and split it into an optional bucket
/// and object key.
///
/// An empty input is treated as root ("/"). An input that does not start
/// with "/" is prefixed with one and then addressed as an absolute path.
/// The result is passed through rustfs_utils::path::clean, which collapses
/// "." and ".." segments. Rooted ".." past the top is dropped by clean,
/// so no resulting path can escape the storage root. Keys containing the
/// reserved GLOBAL_DIR_SUFFIX marker ("__XLDIR__") are rejected because
/// that marker is the backend's internal encoding for directory objects
/// and is not part of the client-visible namespace.
///
/// Returns Ok((bucket, None)) for the root, Ok(("bucket", None)) for a
/// bucket-level directory, and Ok(("bucket", Some("key"))) otherwise.
/// Returns Err(BadMessage) for reserved or malformed inputs, including
/// any input containing an embedded NUL, CR, or LF byte. NUL is never
/// legitimate in a POSIX path component or an S3 key. CR and LF are
/// rejected at this boundary so a path emitted on a tracing field
/// cannot inject a line into the operator log; downstream warn paths
/// (skip-abort, stat fallback, REMOVE refusal) emit the bucket and key
/// without further sanitization.
pub(super) fn parse_s3_path(input: &str) -> Result<(String, Option<String>), SftpError> {
    if input.contains(['\0', '\r', '\n']) {
        return Err(SftpError::code(StatusCode::BadMessage));
    }

    let cleaned = path::clean(&ensure_absolute(input));

    // clean may return ".", "/", or a rooted path. It never returns a path
    // that escapes above the root when the input is rooted, but reject any
    // lingering ".." defensively in case the path::clean contract changes
    // or has an edge case the canonicalisation misses.
    if cleaned == "." || cleaned == ".." || cleaned.starts_with("../") {
        return Ok((String::new(), None));
    }

    let (bucket, object) = path::path_to_bucket_object(&cleaned);

    if object.contains(path::GLOBAL_DIR_SUFFIX) {
        return Err(SftpError::code(StatusCode::BadMessage));
    }

    let key = if object.is_empty() { None } else { Some(object) };
    Ok((bucket, key))
}

/// Replace C0 control bytes (other than tab) with the literal byte 0x3F
/// ("?"). POSIX filenames and S3 keys permit CR, LF, BEL, ESC, and the
/// other low-ASCII control bytes, but echoing them verbatim into the
/// SSH_FXP_NAME longname field or into a tracing emit lets a hostile key
/// inject a forged second entry or split a log line. Tab (0x09) is
/// kept because it is the column separator inside the longname format.
/// NUL is rejected at the parse boundary.
pub(super) fn sanitise_control_bytes(input: &str) -> std::borrow::Cow<'_, str> {
    let needs_sanitise = input.bytes().any(|b| b < 0x20 && b != b'\t');
    if !needs_sanitise {
        return std::borrow::Cow::Borrowed(input);
    }
    let mut out = String::with_capacity(input.len());
    for ch in input.chars() {
        if (ch as u32) < 0x20 && ch != '\t' {
            out.push('?');
        } else {
            out.push(ch);
        }
    }
    std::borrow::Cow::Owned(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use russh_sftp::protocol::StatusCode;

    #[test]
    fn parse_s3_path_root() {
        let (bucket, key) = parse_s3_path("/").unwrap();
        assert!(bucket.is_empty());
        assert!(key.is_none());

        let (bucket, key) = parse_s3_path("").unwrap();
        assert!(bucket.is_empty());
        assert!(key.is_none());
    }

    #[test]
    fn parse_s3_path_bucket_only() {
        let (bucket, key) = parse_s3_path("/mybucket").unwrap();
        assert_eq!(bucket, "mybucket");
        assert!(key.is_none());
    }

    #[test]
    fn parse_s3_path_bucket_and_key() {
        let (bucket, key) = parse_s3_path("/mybucket/path/to/file.txt").unwrap();
        assert_eq!(bucket, "mybucket");
        assert_eq!(key.as_deref(), Some("path/to/file.txt"));
    }

    #[test]
    fn parse_s3_path_rejects_embedded_nul_byte() {
        let err = parse_s3_path("/bucket/key\0withnul").expect_err("NUL must be rejected");
        assert!(matches!(StatusCode::from(err), StatusCode::BadMessage));

        let err = parse_s3_path("\0").expect_err("NUL-only input must be rejected");
        assert!(matches!(StatusCode::from(err), StatusCode::BadMessage));
    }

    #[test]
    fn parse_s3_path_rejects_carriage_return() {
        let err = parse_s3_path("/bucket/line\r/inject").expect_err("CR must be rejected");
        assert!(matches!(StatusCode::from(err), StatusCode::BadMessage));
    }

    #[test]
    fn parse_s3_path_rejects_line_feed() {
        let err = parse_s3_path("/bucket/line\n/inject").expect_err("LF must be rejected");
        assert!(matches!(StatusCode::from(err), StatusCode::BadMessage));
    }

    #[test]
    fn parse_s3_path_rejects_xldir_marker() {
        let err = parse_s3_path("/bucket/__XLDIR__").expect_err("__XLDIR__ must be rejected");
        assert!(matches!(StatusCode::from(err), StatusCode::BadMessage));
    }

    #[test]
    fn parse_s3_path_collapses_dotdot_without_escaping_root() {
        let (bucket, key) = parse_s3_path("/../../bucket/key").unwrap();
        assert_eq!(bucket, "bucket");
        assert_eq!(key.as_deref(), Some("key"));
    }

    #[test]
    fn parse_s3_path_cleans_dotdot_between_segments() {
        let (bucket, key) = parse_s3_path("/bucket/sub/../file").unwrap();
        assert_eq!(bucket, "bucket");
        assert_eq!(key.as_deref(), Some("file"));
    }

    #[test]
    fn parse_s3_path_strips_trailing_slash_on_subdir_path() {
        let (bucket, key) = parse_s3_path("/bucket/subdir/").unwrap();
        assert_eq!(bucket, "bucket");
        assert_eq!(key.as_deref(), Some("subdir"));
    }

    #[test]
    fn parse_s3_path_strips_trailing_slash_on_nested_subdir_path() {
        let (bucket, key) = parse_s3_path("/bucket/a/b/c/").unwrap();
        assert_eq!(bucket, "bucket");
        assert_eq!(key.as_deref(), Some("a/b/c"));
    }

    #[test]
    fn parse_s3_path_collapses_bucket_trailing_slash_to_no_key() {
        let (bucket, key) = parse_s3_path("/bucket/").unwrap();
        assert_eq!(bucket, "bucket");
        assert!(key.is_none());
    }

    #[test]
    fn sanitise_control_bytes_passes_plain_ascii_unchanged() {
        let input = "weekly-report-Q1.pdf";
        let out = sanitise_control_bytes(input);
        assert_eq!(out.as_ref(), input);
        assert!(matches!(out, std::borrow::Cow::Borrowed(_)));
    }

    #[test]
    fn sanitise_control_bytes_replaces_lf() {
        assert_eq!(sanitise_control_bytes("weekly\nreport.pdf").as_ref(), "weekly?report.pdf");
    }

    #[test]
    fn sanitise_control_bytes_replaces_cr() {
        assert_eq!(sanitise_control_bytes("report\rpdf").as_ref(), "report?pdf");
    }

    #[test]
    fn sanitise_control_bytes_replaces_crlf() {
        assert_eq!(sanitise_control_bytes("a\r\nb").as_ref(), "a??b");
    }

    #[test]
    fn sanitise_control_bytes_preserves_tab() {
        let input = "col1\tcol2";
        let out = sanitise_control_bytes(input);
        assert_eq!(out.as_ref(), input);
        assert!(matches!(out, std::borrow::Cow::Borrowed(_)));
    }

    #[test]
    fn sanitise_control_bytes_replaces_other_c0_controls() {
        assert_eq!(sanitise_control_bytes("alarm\x07bell\x1bescape").as_ref(), "alarm?bell?escape");
    }

    #[test]
    fn sanitise_control_bytes_preserves_unicode_above_c0() {
        let input = "report-Q1-é-中文.pdf";
        let out = sanitise_control_bytes(input);
        assert_eq!(out.as_ref(), input);
        assert!(matches!(out, std::borrow::Cow::Borrowed(_)));
    }

    #[test]
    fn ensure_absolute_prefixes_relative_input() {
        assert_eq!(ensure_absolute("foo/bar"), "/foo/bar");
        assert_eq!(ensure_absolute(""), "/");
        assert_eq!(ensure_absolute("."), "/.");
    }

    #[test]
    fn ensure_absolute_passes_through_absolute_input() {
        assert_eq!(ensure_absolute("/"), "/");
        assert_eq!(ensure_absolute("/foo"), "/foo");
        assert_eq!(ensure_absolute("/a/b/c"), "/a/b/c");
    }

    #[test]
    fn last_path_component_extracts_final_segment() {
        assert_eq!(last_path_component("foo/bar/baz"), Some("baz"));
        assert_eq!(last_path_component("foo/bar/baz/"), Some("baz"));
        assert_eq!(last_path_component("singleton"), Some("singleton"));
        assert_eq!(last_path_component("singleton/"), Some("singleton"));
    }

    #[test]
    fn last_path_component_returns_none_for_empty_or_slashes_only() {
        assert_eq!(last_path_component(""), None);
        assert_eq!(last_path_component("/"), None);
        assert_eq!(last_path_component("///"), None);
    }

    #[test]
    fn relative_filename_returns_single_component_residual() {
        assert_eq!(relative_filename("foo/bar.txt", "foo/"), Some("bar.txt"));
        assert_eq!(relative_filename("file.txt", ""), Some("file.txt"));
    }

    #[test]
    fn relative_filename_rejects_non_matching_prefix() {
        assert_eq!(relative_filename("other/bar.txt", "foo/"), None);
    }

    #[test]
    fn relative_filename_rejects_residual_with_slash() {
        assert_eq!(relative_filename("foo/sub/bar.txt", "foo/"), None);
    }

    #[test]
    fn relative_filename_rejects_empty_residual() {
        assert_eq!(relative_filename("foo/", "foo/"), None);
    }

    proptest::proptest! {
        #![proptest_config(proptest::prelude::ProptestConfig {
            cases: 10_000,
            .. proptest::prelude::ProptestConfig::default()
        })]

        #[test]
        fn parse_s3_path_never_leaks_control_bytes_or_traversal_in_ok_output(
            input in proptest::prelude::any::<String>(),
        ) {
            match parse_s3_path(&input) {
                Err(err) => {
                    proptest::prop_assert!(
                        matches!(StatusCode::from(err), StatusCode::BadMessage),
                        "parse_s3_path rejected input with an unexpected status",
                    );
                }
                Ok((bucket, key)) => {
                    proptest::prop_assert!(!bucket.contains('/'));
                    proptest::prop_assert!(!bucket.contains(['\0', '\r', '\n']));
                    if let Some(k) = key.as_deref() {
                        proptest::prop_assert!(!k.contains(['\0', '\r', '\n']));
                        proptest::prop_assert!(!k.split('/').any(|seg| seg == ".."));
                        proptest::prop_assert!(!k.starts_with('/'));
                    }
                }
            }
        }
    }
}
